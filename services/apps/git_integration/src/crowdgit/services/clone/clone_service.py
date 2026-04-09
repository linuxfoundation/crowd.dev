import os
import shutil
import tempfile
import time
from collections.abc import AsyncIterator
from decimal import Decimal

import aiofiles
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
    CrowdGitError,
    NetworkError,
    RateLimitError,
    RemoteServerError,
)
from crowdgit.models import CloneBatchInfo, Repository, ServiceExecution
from crowdgit.services.base.base_service import BaseService
from crowdgit.services.utils import (
    get_default_branch,
    get_remote_default_branch,
    get_repo_name,
    run_shell_command,
)

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

    async def _check_if_final_batch(self, path: str, target_commit_hash: str | None) -> bool:
        """
        Final batch is determined if:
        - full history is cloned (no longer shallow_clone)
        - target commit reached
        """
        is_shallow_clone = await run_shell_command(
            ["git", "rev-parse", "--is-shallow-repository"], cwd=path
        )
        if "false" in is_shallow_clone:
            return True
        if not target_commit_hash:
            return False
        try:
            await run_shell_command(
                ["git", "rev-parse", "--verify", f"{target_commit_hash}^{{commit}}"], cwd=path
            )
            self.logger.info(f"Target commit {target_commit_hash} reached")
            return True
        except CommandExecutionError:
            return False

    @retry_on_clone_error
    async def _perform_minimal_clone(self, path: str, remote: str) -> None:
        """
        Perform minimal clone of depth=1
        """
        # increasing post buffer to avoid RPC failed error
        await run_shell_command(
            ["git", "config", "--global", "http.postBuffer", "524288000"], cwd=path
        )
        self.logger.info("Initializing minimal clone")
        await run_shell_command(
            ["git", "clone", "--depth=1", "--no-tags", "--single-branch", remote, "."], cwd=path
        )
        self.logger.info("Minimal clone initialized successfully")

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

    @retry_on_clone_error
    async def _clone_next_batch(self, repo_path: str, batch_depth: int, remote: str):
        default_branch = await get_default_branch(repo_path)
        self.logger.info(
            f"Fetching an additional {batch_depth} commits from {default_branch} branch"
        )
        try:
            await run_shell_command(
                ["git", "fetch", "origin", default_branch, f"--deepen={batch_depth}"],
                cwd=repo_path,
            )
        except RemoteServerError:
            if self._is_gerrit_remote(remote):
                self.logger.warning(
                    "Gerrit server error on --deepen, falling back to unshallow fetch"
                )
                await run_shell_command(["git", "fetch", "--unshallow"], cwd=repo_path)
            else:
                raise
        # Optimize repository storage using git garbage collection
        await self._optimize_repository_storage(repo_path)

    async def _update_batch_info(
        self,
        batch_info: CloneBatchInfo,
        repo_path: str,
        target_commit_hash: str | None,
        clone_with_batches: bool,
    ) -> None:
        """Update batch info with repo path and final batch status.

        For full clones (clone_with_batches=False): Marks as final batch immediately.
        For batched clones (clone_with_batches=True): Checks if target commit reached or full history fetched.
        """
        batch_info.repo_path = repo_path
        batch_info.clone_with_batches = clone_with_batches

        if batch_info.is_first_batch:
            # Set latest commit only from first batch
            latest_commit_output = await run_shell_command(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_path,
            )
            batch_info.latest_commit_in_repo = latest_commit_output.strip()
        if not clone_with_batches:
            # Full clone: always final batch since entire repository history is available
            batch_info.is_final_batch = True
            return

        batch_info.is_final_batch = await self._check_if_final_batch(repo_path, target_commit_hash)
        batch_info.edge_commit = await self._get_edge_commit(repo_path)

    async def _get_edge_commit(self, repo_path: str):
        """
        Returns the edge commit of a shallow clone by reading the .git/shallow file,
        which contains the boundary commit(s) when history is truncated.

        If the full history has been cloned, the .git/shallow file does not exist.
        """
        shallow_file = os.path.join(repo_path, ".git", "shallow")
        try:
            async with aiofiles.open(shallow_file, "r", encoding="utf-8") as f:
                oldest_commit = (await f.readline()).strip()
            self.logger.info(f"Edge commit: {oldest_commit}")
            return oldest_commit
        except FileNotFoundError:
            return None

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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        reraise=False,
    )
    async def _cleanup_working_directory(self, repo_path: str) -> None:
        """
        Remove all files and directories from the repository except the .git directory.
        This helps reduce disk usage while preserving git history for commit processing.
        """
        self.logger.info(f"Cleaning working directory: {repo_path}")

        # Use find command to remove everything except .git directory
        await run_shell_command(
            [
                "find",
                ".",
                "-mindepth",
                "1",
                "-maxdepth",
                "1",
                "!",
                "-name",
                ".git",
                "-exec",
                "rm",
                "-rf",
                "{}",
                "+",
            ],
            cwd=repo_path,
        )

        self.logger.info("Working directory cleanup completed")

    async def _calculate_batch_depth(self, repo_path: str, remote: str) -> int:
        calculated_depth = None
        total_branches_tags = await run_shell_command(
            ["git", "ls-remote", "--heads", "--tags", remote], cwd=repo_path
        )
        total_branches_tags = len(total_branches_tags.splitlines())
        if total_branches_tags <= 200:
            # Small repo, get a decent amount of history
            calculated_depth = 100
        elif total_branches_tags <= 1000:
            # Medium repo, get a moderate amount of history
            calculated_depth = 50
        else:
            # Large repo, get less history
            calculated_depth = 5
        self.logger.info(
            f"total_branches_tags={total_branches_tags}, calculated_depth={calculated_depth}"
        )
        return calculated_depth

    @retry_on_clone_error
    async def _perform_full_clone(self, repo_path: str, remote: str):
        """Perform full repository clone"""
        self.logger.info(f"Performing full clone for repo {remote}...")
        await run_shell_command(
            ["git", "clone", "--no-tags", "--single-branch", remote, "."], cwd=repo_path
        )
        self.logger.info(f"Successfully completed full clone of repository: {remote}")

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

    async def determine_clone_strategy(
        self, repo_path: str, remote: str, branch: str | None, last_processed_commit: str | None
    ) -> bool:
        """Determine whether to use full clone or minimal clone strategy based on repository state.

        Args:
            repo_path: Local path where repository will be cloned
            remote: Remote repository URL (e.g., 'https://github.com/user/repo')
            branch: Current saved branch name or None for new repositories
            last_processed_commit: Last processed commit hash or None for new repositories

        Returns: (clone_with_batches)
            bool: False for full clone (clone_with_batches=False), True for minimal clone (clone_with_batches=True)

        Strategy:
            - Full clone: New repositories (last_processed_commit=None) or branch changed
            - Minimal clone: Existing repositories with unchanged branch for incremental processing
        """

        self.logger.info(
            f"Starting clone decision for {remote} (branch: {branch}, last_commit: {last_processed_commit})"
        )

        default_branch_changed = await self.has_default_branch_changed(remote, branch)

        if not last_processed_commit or default_branch_changed:
            reason = "new repository" if not last_processed_commit else "branch changed"
            self.logger.info(f"Performing full clone for {remote} - reason: {reason}")
            await self._perform_full_clone(repo_path, remote)
            return False

        self.logger.info(
            f"Performing minimal clone for {remote} - existing repository with unchanged branch"
        )
        await self._perform_minimal_clone(repo_path, remote)
        return True

    async def clone_batches_generator(
        self,
        repository: Repository,
        working_dir_cleanup: bool | None = False,
    ) -> AsyncIterator[CloneBatchInfo]:
        """
        Async generator that yields CloneBatchInfo for repository cloning.

        For new repositories (clone_with_batches=False): Performs full clone to avoid inefficient batching (stacked git objects).

        For existing repositories (clone_with_batches=True): Uses incremental batched
        processing to fetch only new commits since last processing.
        """
        temp_repo_path = None
        execution_status = ExecutionStatus.SUCCESS
        error_code = None
        error_message = None
        total_execution_time = 0.0
        remote = repository.url.removesuffix(".git")

        batch_info = CloneBatchInfo(
            repo_path=temp_repo_path,
            remote=remote,
            is_final_batch=False,
            is_first_batch=True,
        )
        try:
            temp_repo_path = tempfile.mkdtemp(prefix=f"{get_repo_name(remote)}_")
            batch_start_time = time.time()

            clone_with_batches = await self.determine_clone_strategy(
                temp_repo_path, remote, repository.branch, repository.last_processed_commit
            )
            if clone_with_batches:
                batch_depth = await self._calculate_batch_depth(temp_repo_path, remote)
            await self._update_batch_info(
                batch_info, temp_repo_path, repository.last_processed_commit, clone_with_batches
            )
            batch_end_time = time.time()
            total_execution_time += round(batch_end_time - batch_start_time, 2)

            yield batch_info
            if working_dir_cleanup:
                await self._cleanup_working_directory(temp_repo_path)

            batch_info.is_first_batch = False
            while not batch_info.is_final_batch:
                batch_start_time = time.time()
                batch_info.prev_batch_edge_commit = await self._get_edge_commit(temp_repo_path)
                await self._clone_next_batch(temp_repo_path, batch_depth, remote)
                await self._update_batch_info(
                    batch_info,
                    temp_repo_path,
                    repository.last_processed_commit,
                    clone_with_batches,
                )
                batch_end_time = time.time()
                total_execution_time += round(batch_end_time - batch_start_time, 2)

                yield batch_info

        except Exception as e:
            # Handle both CrowdGitError and generic Exception
            execution_status = ExecutionStatus.FAILURE
            error_message = e.error_message if isinstance(e, CrowdGitError) else repr(e)
            error_code = (
                e.error_code.value if isinstance(e, CrowdGitError) else ErrorCode.UNKNOWN.value
            )
            self.logger.error(f"Cloning failed: {error_message}")
            raise
        finally:
            if temp_repo_path and os.path.exists(temp_repo_path):
                await self._cleanup_temp_directory(temp_repo_path, repository.id)

            # Save metrics
            service_execution = ServiceExecution(
                repo_id=repository.id,
                operation_type=OperationType.CLONE,
                status=execution_status,
                error_code=error_code,
                error_message=error_message,
                execution_time_sec=Decimal(str(round(total_execution_time, 2))),
            )
            await save_service_execution(service_execution)
