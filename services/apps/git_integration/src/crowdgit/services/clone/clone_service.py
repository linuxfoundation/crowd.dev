import os
import shutil
import tempfile
import time
from collections.abc import AsyncIterator
from datetime import datetime, timezone
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
    ReOnboardingRequiredError,
)
from crowdgit.models import CloneBatchInfo, Repository, ServiceExecution
from crowdgit.services.base.base_service import BaseService
from crowdgit.services.utils import (
    get_default_branch,
    get_remote_default_branch,
    get_repo_name,
    run_shell_command,
)
from crowdgit.settings import (
    STUCK_ONBOARDING_REPO_TIMEOUT_HOURS,
    STUCK_RECURRENT_REPO_TIMEOUT_HOURS,
)

DEFAULT_STORAGE_OPTIMIZATION_THRESHOLD_MB = 10000
INITIAL_BATCH_DEEPEN = 50
MAX_BATCH_DEEPEN = 10000
BATCH_DEEPEN_MULTIPLIER = 2

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

    async def _configure_global_git_client(self, path: str) -> None:
        # Increase post buffer to reduce RPC failures on large repos
        await run_shell_command(
            ["git", "config", "--global", "http.postBuffer", "524288000"], cwd=path
        )
        # Disable recursive submodule fetches
        await run_shell_command(
            ["git", "config", "--global", "fetch.recurseSubmodules", "false"], cwd=path
        )
        # Ensure abbreviated commits are disabled
        await run_shell_command(
            ["git", "-C", path, "config", "core.abbrevCommit", "false"], cwd=path
        )

    async def _is_shallow_clone(self, path: str) -> bool:
        result = await run_shell_command(["git", "rev-parse", "--is-shallow-repository"], cwd=path)
        return "true" in result

    def _is_timeout_reached(self, repository: Repository) -> bool:
        processing_duration_hours = (
            datetime.now(timezone.utc) - repository.locked_at.astimezone(timezone.utc)
        ).total_seconds() / 3600
        is_onboarding = repository.last_processed_commit is None
        timeout_hours = (
            STUCK_ONBOARDING_REPO_TIMEOUT_HOURS
            if is_onboarding
            else STUCK_RECURRENT_REPO_TIMEOUT_HOURS
        )
        if processing_duration_hours >= timeout_hours:
            self.logger.warning(
                f"Repo {repository.url} is stuck for {processing_duration_hours:.1f} hours — queuing for re-onboarding"
            )
            return True
        return False

    async def _check_if_final_batch(self, path: str, repository: Repository) -> bool:
        """
        Check whether the current batch is the final one.

        Returns True when:
        - full history fetched (onboarding — no last_processed_commit to look for), or
        - all commits between last_processed_commit and HEAD are available (no shallow boundary in range).

        Raises ReOnboardingRequiredError when:
        - force push detected (full clone but last_processed_commit missing), or
        - processing timeout exceeded while still deepening.
        """
        is_shallow_clone = await self._is_shallow_clone(path)
        is_full_clone = not is_shallow_clone

        target_commit_hash = repository.last_processed_commit
        if is_full_clone and not target_commit_hash:
            # fully cloned at once
            return True

        reached_target_commit = await self._has_required_commit_range(path, target_commit_hash)
        if reached_target_commit:
            return True

        timeout_reached = self._is_timeout_reached(repository)
        force_push_detected = is_full_clone and not reached_target_commit
        if timeout_reached or force_push_detected:
            raise ReOnboardingRequiredError()

        return False

    async def _has_required_commit_range(self, path: str, target_commit_hash: str) -> bool:
        try:
            await run_shell_command(
                ["git", "rev-parse", "--verify", f"{target_commit_hash}^{{commit}}"], cwd=path
            )
            has_boundary_in_required_range = await self._has_boundary_in_required_range(
                path, target_commit_hash
            )
            if has_boundary_in_required_range:
                self.logger.info(
                    f"Target commit {target_commit_hash} reached, but shallow boundary still intersects required range. Continuing deepen."
                )
                return False
            self.logger.info(f"Target commit {target_commit_hash} reached")
            return True
        except CommandExecutionError:
            return False

    async def _read_shallow_file(self, repo_path: str) -> list[str]:
        """
        Read commit hashes from .git/shallow.
        Returns an empty list when the file does not exist (full clone).
        """
        shallow_file = os.path.join(repo_path, ".git", "shallow")
        try:
            async with aiofiles.open(shallow_file, "r", encoding="utf-8") as f:
                return [line.strip() for line in await f.readlines() if line.strip()]
        except FileNotFoundError:
            return []

    async def _get_shallow_boundary_commits(self, repo_path: str) -> set[str]:
        """
        Return all shallow boundary commits from .git/shallow.
        """
        return set(await self._read_shallow_file(repo_path))

    async def _has_boundary_in_required_range(
        self, repo_path: str, target_commit_hash: str
    ) -> bool:
        """
        Check if any shallow boundary commit is inside the current required range.

        If true, the required range is still truncated and the clone should be deepened.
        """
        shallow_boundaries = await self._get_shallow_boundary_commits(repo_path)
        if not shallow_boundaries:
            return False

        required_commits_output = await run_shell_command(
            ["git", "rev-list", f"{target_commit_hash}..HEAD"],
            cwd=repo_path,
        )
        required_commits = {line.strip() for line in required_commits_output.splitlines() if line}
        problematic_boundaries = (required_commits & shallow_boundaries) - {target_commit_hash}
        return bool(problematic_boundaries)

    @retry_on_clone_error
    async def _perform_minimal_clone(self, path: str, remote: str) -> None:
        """
        Perform minimal clone of depth=1
        """
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
        repository: Repository,
        clone_with_batches: bool,
    ) -> None:
        """Update batch info with repo path and final batch status.

        For full clones (clone_with_batches=False): Marks as final batch immediately.
        For batched clones (clone_with_batches=True): Delegates to _check_if_final_batch
        to determine if required commit range is available or timeout/force-push occurred.
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

        batch_info.is_final_batch = await self._check_if_final_batch(repo_path, repository)

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
        await self._configure_global_git_client(repo_path)

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
        batch_depth = INITIAL_BATCH_DEEPEN
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
            await self._update_batch_info(
                batch_info, temp_repo_path, repository, clone_with_batches
            )
            total_execution_time += round(time.time() - batch_start_time, 2)

            yield batch_info
            if working_dir_cleanup:
                await self._cleanup_working_directory(temp_repo_path)

            batch_info.is_first_batch = False
            while not batch_info.is_final_batch:
                batch_start_time = time.time()
                await self._clone_next_batch(temp_repo_path, batch_depth, remote)
                batch_info.is_final_batch = await self._check_if_final_batch(
                    temp_repo_path, repository
                )
                total_execution_time += round(time.time() - batch_start_time, 2)

                if not batch_info.is_final_batch:
                    # exponential deepen increment to speed up fetching
                    batch_depth = min(
                        batch_depth * BATCH_DEEPEN_MULTIPLIER,
                        MAX_BATCH_DEEPEN,
                    )

            if clone_with_batches:
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
