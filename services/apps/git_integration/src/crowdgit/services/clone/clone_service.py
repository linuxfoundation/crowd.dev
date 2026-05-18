import fcntl
import os
import shutil
import tempfile
import time
from collections.abc import AsyncIterator
from decimal import Decimal

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
)

from crowdgit.database.crud import save_service_execution
from crowdgit.enums import ErrorCode, ExecutionStatus, OperationType
from crowdgit.errors import (
    CommandExecutionError,
    CommandTimeoutError,
    CrowdGitError,
    DiskSpaceError,
    NetworkError,
    RateLimitError,
    RemoteServerError,
    RepoLockingError,
)
from crowdgit.models import CloneBatchInfo, Repository, ServiceExecution
from crowdgit.services.base.base_service import BaseService
from crowdgit.services.utils import (
    get_default_branch,
    get_remote_default_branch,
    get_repo_name,
    run_shell_command,
)
from crowdgit.settings import REPO_STORAGE_ROOT

DEFAULT_STORAGE_OPTIMIZATION_THRESHOLD_MB = 10000

RETRYABLE_CLONE_ERRORS = (RateLimitError, NetworkError, RemoteServerError)

retry_on_clone_error = retry(
    retry=retry_if_exception_type(RETRYABLE_CLONE_ERRORS),
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=5, min=5, max=120),
    reraise=True,
)


GERRIT_PATTERNS = {"gerrit.", "review.", "/gerrit/"}


class CloneService(BaseService):
    """Service for cloning repositories"""

    def __init__(self):
        super().__init__()

    @staticmethod
    def _is_gerrit_remote(remote: str) -> bool:
        return any(pattern in remote for pattern in GERRIT_PATTERNS)

    async def _get_repo_size_mb(self, repo_path: str) -> float:
        try:
            result = await run_shell_command(["du", "-sm", repo_path], cwd=repo_path)
            size_mb = float(result.strip().split()[0])
            return size_mb
        except Exception as e:
            self.logger.warning(f"Failed to get repo size: {e}")
            return 0.0

    async def _optimize_repository_storage(
        self, repo_path: str, threshold_mb: float = DEFAULT_STORAGE_OPTIMIZATION_THRESHOLD_MB
    ) -> None:
        """
        Optimize repository storage if size exceeds threshold.
        This is infrequently performed because 'git gc' is costly (CPU/IO intensive and time-consuming)
        only runs for repositories (>2GB) that grow due to incremental fetching creating
        inefficient pack files.
        Uses git gc to compact repository and logs timing, size before and after operation.
        """
        try:
            size_before = await self._get_repo_size_mb(repo_path)

            if size_before > threshold_mb:
                self.logger.info(
                    f"Repository size {size_before:.1f}MB > {threshold_mb:.0f}MB threshold, running git gc"
                )

                start_time = time.time()
                await run_shell_command(
                    ["git", "gc", "--keep-largest-pack", "--quiet"], cwd=repo_path
                )
                gc_duration = time.time() - start_time

                size_after = await self._get_repo_size_mb(repo_path)
                reduction_pct = (
                    ((size_before - size_after) / size_before) * 100 if size_before > 0 else 0
                )

                self.logger.info(
                    f"Git gc completed in {gc_duration:.1f}s: {size_before:.1f}MB → {size_after:.1f}MB ({reduction_pct:.1f}% reduction)"
                )
            else:
                self.logger.debug(
                    f"Repository size {size_before:.1f}MB is below threshold {threshold_mb:.0f}MB, skipping storage optimization (normal - most repositories stay small)"
                )

        except Exception as e:
            self.logger.error(f"Failed to perform git gc: {repr(e)}")
            # Don't raise - gc failure shouldn't stop processing

    async def _update_batch_info(self, batch_info: CloneBatchInfo, repo_path: str) -> None:
        batch_info.repo_path = repo_path
        batch_info.is_final_batch = True

        if batch_info.is_first_batch:
            latest_commit_output = await run_shell_command(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_path,
            )
            batch_info.latest_commit_in_repo = latest_commit_output.strip()

    async def _cleanup_temp_directory(self, temp_repo_path: str, repo_id: str) -> None:
        """
        Clean up temporary directory with retries and error handling.
        If cleanup fails after all retries, log the failure to service execution.
        """
        try:
            await self._cleanup_temp_directory_with_retries(temp_repo_path)
            self.logger.info(f"successfully cleaned temp dir {temp_repo_path}")
        except Exception as e:
            error_message = (
                f"Failed to cleanup temp directory {temp_repo_path} after retries: {repr(e)}"
            )
            self.logger.error(error_message)

            # Save cleanup failure to service execution (only after all retries failed)
            try:
                service_execution = ServiceExecution(
                    repo_id=repo_id,
                    operation_type=OperationType.CLONE,
                    status=ExecutionStatus.FAILURE,
                    error_code=ErrorCode.CLEANUP_FAILED.value,
                    error_message=error_message,
                    execution_time_sec=Decimal("0.0"),
                )
                await save_service_execution(service_execution)
            except Exception as save_error:
                self.logger.error(f"Failed to save cleanup failure: {repr(save_error)}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        reraise=True,
    )
    async def _cleanup_temp_directory_with_retries(self, temp_repo_path: str) -> None:
        """
        Actual cleanup implementation with retries.
        Raises exceptions so @retry can handle them.
        """
        self.logger.info(f"cleaning temp dir {temp_repo_path}")
        shutil.rmtree(temp_repo_path)

    @retry_on_clone_error
    async def _perform_full_clone(self, repo_path: str, remote: str):
        """Perform full repository clone"""
        self.logger.info(f"Performing full clone for repo {remote}...")
        await run_shell_command(
            ["git", "clone", "--no-tags", "--single-branch", remote, "."], cwd=repo_path
        )
        self.logger.info(f"Successfully completed full clone of repository: {remote}")

    def _stable_repo_path(self, repo_id: str) -> str | None:
        """Returns a stable on-disk path for the repo when REPO_STORAGE_ROOT is set, else None."""
        if REPO_STORAGE_ROOT is None:
            return None
        return os.path.join(REPO_STORAGE_ROOT, str(repo_id))

    async def _is_repo_valid(self, repo_path: str) -> bool:
        """Return True if the local clone is usable: HEAD resolves and a remote tracking branch exists."""
        try:
            await run_shell_command(["git", "rev-parse", "HEAD"], cwd=repo_path)
            branch = await get_default_branch(repo_path)
            return branch != "*"
        except Exception:
            return False

    @retry_on_clone_error
    async def _incremental_fetch(self, repo_path: str, remote: str) -> None:
        """Fetch latest commits into a persistent clone and update the working tree."""
        # Remove stale git lockfiles left by a previous crash before touching the repo.
        # Target known locations only — os.walk(.git) on large repos (e.g. linux kernel) blocks
        # the event loop for seconds scanning hundreds of thousands of pack/ref files.
        git_dir = os.path.join(repo_path, ".git")
        _KNOWN_LOCKFILES = [
            os.path.join(git_dir, "index.lock"),
            os.path.join(git_dir, "HEAD.lock"),
            os.path.join(git_dir, "packed-refs.lock"),
        ]
        for lockpath in _KNOWN_LOCKFILES:
            if os.path.exists(lockpath):
                try:
                    os.remove(lockpath)
                    self.logger.warning(f"Removed stale lockfile: {lockpath}")
                except OSError as e:
                    self.logger.warning(f"Failed to remove stale lockfile {lockpath}: {e}")

        default_branch = await get_default_branch(repo_path)
        if default_branch == "*":
            raise CommandExecutionError(
                f"Cannot fetch {remote}: no remote tracking branch found (detached HEAD)",
                returncode=1,
            )
        self.logger.info(f"Fetching {default_branch} from {remote}")
        await run_shell_command(
            ["git", "fetch", "--no-tags", "origin", default_branch], cwd=repo_path
        )
        # Update working tree so file-scanning services (licensee, rg, vuln scanner) see
        # current file content, not the state from the previous run.
        await run_shell_command(
            ["git", "reset", "--hard", f"origin/{default_branch}"], cwd=repo_path
        )
        await self._optimize_repository_storage(repo_path)

    async def _wipe_and_reclone(self, repo_path: str, remote: str) -> None:
        """Wipe an existing persistent clone and replace it with a fresh full clone."""
        self.logger.info(f"Wiping and re-cloning {remote}")
        shutil.rmtree(repo_path)
        os.makedirs(repo_path)
        try:
            await self._perform_full_clone(repo_path, remote)
        except Exception:
            # Clone failed — wipe the partial .git so the next run starts fresh rather than
            # attempting an incremental fetch against a half-written clone.
            shutil.rmtree(repo_path, ignore_errors=True)
            raise

    async def has_default_branch_changed(self, remote: str, saved_branch: str | None) -> bool:
        """Check if the default branch has changed compared to the saved branch
        Args:
            remote: The remote repository URL
            saved_branch: The branch currently saved in the database (can be None)
        Returns:
            True if default branch has changed and requires re-cloning, False otherwise
        """
        try:
            remote_default_branch = await get_remote_default_branch(remote)

            if remote_default_branch is None:
                self.logger.warning(f"Could not determine default branch for {remote}")
                return False

            if saved_branch is None:
                self.logger.info(f"No saved branch for {remote} assuming it's not changed")
                return False

            if saved_branch != remote_default_branch:
                self.logger.info(
                    f"Branch changed for {remote}: saved='{saved_branch}' vs remote='{remote_default_branch}'"
                )
                return True

            self.logger.debug(f"Branch unchanged for {remote}: {saved_branch}")
            return False

        except Exception as e:
            self.logger.error(f"Error validating branch for {remote}: {e}")
            # On error, assume no change to avoid unnecessary re-cloning
            return False

    async def clone_batches_generator(
        self,
        repository: Repository,
    ) -> AsyncIterator[CloneBatchInfo]:
        """
        Async generator that yields a single CloneBatchInfo per call.

        When REPO_STORAGE_ROOT is set (staging/prod k8s): uses a stable on-disk path per repo.
        First run performs a full clone; subsequent runs do an incremental git fetch + working
        tree reset. No temp dir cleanup — the clone persists across runs.

        When REPO_STORAGE_ROOT is unset (local dev / Docker Compose): falls back to the legacy
        ephemeral behaviour — tempfile.mkdtemp + full clone + cleanup in finally. Behaviour is
        identical to before this change.
        """
        repo_path = None
        stable_path = self._stable_repo_path(repository.id)
        execution_status = ExecutionStatus.SUCCESS
        error_code = None
        error_message = None
        total_execution_time = 0.0
        remote = repository.url.removesuffix(".git")

        batch_info = CloneBatchInfo(
            repo_path=repo_path,
            remote=remote,
            is_final_batch=False,
            is_first_batch=True,
        )
        try:
            batch_start_time = time.time()

            if stable_path is not None:
                # Persistent path — used when REPO_STORAGE_ROOT env var is set (k8s).
                repo_path = stable_path
                os.makedirs(repo_path, exist_ok=True)

                # OS-level flock guards against the DB lock expiry window: if a pod crashes and
                # another acquires the DB lock after STUCK_REPO_TIMEOUT_HOURS, this flock ensures
                # the new pod does not mutate the directory while the crashed pod's git subprocess
                # is still running. Non-blocking: raises OSError(EWOULDBLOCK) if held elsewhere.
                #
                # Lock file lives OUTSIDE repo_path so _wipe_and_reclone (rmtree + makedirs) does
                # not unlink the inode that the kernel's flock is attached to — if it did, a second
                # opener would get a fresh inode and a fresh lock, silently bypassing exclusion.
                # "a" mode avoids truncating the file on concurrent open() calls (harmless, but
                # cleaner than "w").
                flock_path = os.path.join(REPO_STORAGE_ROOT, f"{repository.id}.lock")
                flock_fd = open(flock_path, "a")
                try:
                    fcntl.flock(flock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    flock_fd.close()
                    # RepoLockingError is in _TRANSIENT_ERRORS so the outer except will NOT wipe
                    # the persistent clone that the other process is actively using.
                    raise RepoLockingError(
                        f"OS flock busy for {repo_path} — another process still holds the repo"
                    ) from None

                try:
                    if os.path.isdir(os.path.join(repo_path, ".git")):
                        if not await self._is_repo_valid(repo_path):
                            self.logger.warning(
                                f"Repo at {repo_path} is in an invalid state, wiping and re-cloning"
                            )
                            await self._wipe_and_reclone(repo_path, remote)
                            batch_info.branch_changed = True
                        else:
                            default_branch_changed = await self.has_default_branch_changed(
                                remote, repository.branch
                            )
                            if default_branch_changed:
                                await self._wipe_and_reclone(repo_path, remote)
                                batch_info.branch_changed = True
                            else:
                                await self._incremental_fetch(repo_path, remote)
                    else:
                        # Defensive: wipe in case a prior crash left checkout artifacts with no .git.
                        shutil.rmtree(repo_path, ignore_errors=True)
                        os.makedirs(repo_path, exist_ok=True)
                        await self._perform_full_clone(repo_path, remote)
                except Exception as inner_exc:
                    # Wipe while flock is still held so no other pod can grab the flock and start
                    # operating on a directory we are about to delete. Releasing the flock first
                    # (in finally below) and wiping in the outer except creates a race window.
                    _TRANSIENT = (
                        RateLimitError,
                        NetworkError,
                        RemoteServerError,
                        CommandTimeoutError,
                        RepoLockingError,
                        # DiskSpaceError intentionally excluded: a full-disk clone leaves a
                        # partial .git that _is_repo_valid may accept on the next run, causing
                        # _incremental_fetch to run against a corrupt local clone. Wipe instead.
                    )
                    if not isinstance(inner_exc, _TRANSIENT) and os.path.isdir(
                        os.path.join(stable_path, ".git")
                    ):
                        self.logger.warning(
                            f"Wiping persistent clone at {stable_path} after non-transient failure"
                        )
                        shutil.rmtree(stable_path, ignore_errors=True)
                    raise
                finally:
                    fcntl.flock(flock_fd, fcntl.LOCK_UN)
                    flock_fd.close()
            else:
                # Ephemeral path — legacy behaviour for local dev / Docker Compose.
                repo_path = tempfile.mkdtemp(prefix=f"{get_repo_name(remote)}_")
                await self._perform_full_clone(repo_path, remote)

            await self._update_batch_info(batch_info, repo_path)
            total_execution_time += round(time.time() - batch_start_time, 2)

            yield batch_info

        except Exception as e:
            execution_status = ExecutionStatus.FAILURE
            error_message = e.error_message if isinstance(e, CrowdGitError) else repr(e)
            error_code = (
                e.error_code.value if isinstance(e, CrowdGitError) else ErrorCode.UNKNOWN.value
            )
            self.logger.error(f"Cloning failed: {error_message}")
            # Wipe-on-failure is handled inside the flock-held inner except block above,
            # so the rmtree always happens before the flock is released (no race window).
            raise
        finally:
            # Only clean up ephemeral temp dirs. Persistent clones are intentionally kept.
            if stable_path is None and repo_path and os.path.exists(repo_path):
                await self._cleanup_temp_directory(repo_path, repository.id)

            service_execution = ServiceExecution(
                repo_id=repository.id,
                operation_type=OperationType.CLONE,
                status=execution_status,
                error_code=error_code,
                error_message=error_message,
                execution_time_sec=Decimal(str(round(total_execution_time, 2))),
            )
            await save_service_execution(service_execution)
