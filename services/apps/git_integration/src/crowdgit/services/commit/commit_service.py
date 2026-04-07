import asyncio
import datetime
import gc
import hashlib
import re
import time
import uuid
from decimal import Decimal
from typing import Any

import orjson
from loguru import logger
from pydantic import validate_email
from tenacity import retry, stop_after_attempt, wait_fixed

from crowdgit.database.crud import (
    batch_check_parent_activities,
    batch_insert_activities,
    save_service_execution,
)
from crowdgit.enums import (
    DataSinkWorkerQueueMessageType,
    ErrorCode,
    ExecutionStatus,
    IntegrationResultState,
    IntegrationResultType,
    OperationType,
)
from crowdgit.errors import CrowdGitError
from crowdgit.models import CloneBatchInfo, Repository, ServiceExecution
from crowdgit.services.base.base_service import BaseService
from crowdgit.services.commit.activitymap import ActivityMap
from crowdgit.services.queue.queue_service import QueueService
from crowdgit.services.utils import get_default_branch, run_shell_command
from crowdgit.settings import DEFAULT_TENANT_ID


class CommitService(BaseService):
    """Service for processing repository commits"""

    COMMIT_START_SPLITTER = "---CROWD_COMMIT_START---"
    NUMSTAT_SPLITTER = "---CROWD_NUMSTAT_START---"
    MIN_COMMIT_FIELDS = 8
    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
    FUTURE_DATE_THRESHOLD_DAYS = 1

    # Pre-compiled regex patterns for better performance
    _COMMIT_HASH_PATTERN = re.compile(r"^[0-9a-f]{40}$")
    _ACTIVITY_PATTERN = re.compile(r"([^:]*):\s*(.*?)\s+<{1,2}([^>]+)>+$")

    # Common strings to avoid repeated string operations
    _GIT_PLATFORM = "git"
    _USERNAME_TYPE = "username"
    _EMAIL_TYPE = "email"

    MAX_CHUNK_SIZE = 250

    def __init__(self, queue_service: QueueService):
        super().__init__()
        self.queue_service = queue_service
        # Metrics tracking for current repository
        self._metrics_context = None

    @property
    def git_log_format(self) -> str:
        """Git log format string with commit splitter"""
        return f"{self.COMMIT_START_SPLITTER}%n%H%n%aI%n%an%n%ae%n%cI%n%cn%n%ce%n%P%n%d%n%B%n{self.NUMSTAT_SPLITTER}"

    def is_valid_commit_hash(self, commit_hash: str) -> bool:
        """Check if the given commit hash is valid.

        :param commit_hash: The commit hash.
        :return: True if the hash is valid, False otherwise.

        >>> is_valid_commit_hash("7d2fd738dbbca7af5d0f9a7c942a51fd0f7c5c5f")
        True
        >>> is_valid_commit_hash('not so')
        False
        """
        return self._COMMIT_HASH_PATTERN.match(commit_hash) is not None

    def is_valid_datetime(self, commit_datetime: str) -> bool:
        """Check if the given datetime string is valid.

        :param commit_datetime: The datetime string.
        :return: True if the datetime string is valid, False otherwise.

        >>> is_valid_datetime("2021-09-01T10:20:30+00:00")
        True
        >>> is_valid_datetime("2021-09-01 10:20:30+00:00")
        False
        """
        try:
            time.strptime(commit_datetime, self.DATETIME_FORMAT)
            return True
        except ValueError:
            return False

    async def process_single_batch_commits(
        self,
        repository: Repository,
        batch_info: CloneBatchInfo,
    ) -> None:
        """
        Process commits from a cloned batch.

        Args:
            repository: Repository object containing segment and integration info
            batch_info: Clone batch information with paths and commit boundaries
        """
        # Initialize metrics context on first call
        if self._metrics_context is None:
            self._metrics_context = {
                "start_time": time.time(),
                "total_execution_time": 0.0,
                "execution_status": ExecutionStatus.SUCCESS,
                "error_code": None,
                "error_message": None,
                "total_commits": 0,
                "processed_commits": 0,
                "bad_commits": 0,
                "skipped_activities": 0,
                "total_activities": 0,
            }

        batch_start_time = time.time()

        try:
            self.logger.info(
                f"Starting commits processing for new batch having commits older than {batch_info.prev_batch_edge_commit}"
            )
            raw_commits = await self._execute_git_log(
                batch_info.repo_path,
                batch_info.clone_with_batches,
                batch_info.prev_batch_edge_commit,
                batch_info.edge_commit,
                repository.last_processed_commit,
            )

            await self._process_activities_from_commits(raw_commits, batch_info, repository)

            batch_end_time = time.time()
            batch_time = round(batch_end_time - batch_start_time, 2)
            self._metrics_context["total_execution_time"] += batch_time

            self.logger.info(
                f"Batch activity processed from {batch_info.remote} in {batch_time}sec"
            )

            # Save metrics if this is the final batch
            if batch_info.is_final_batch:
                service_execution = ServiceExecution(
                    repo_id=repository.id,
                    operation_type=OperationType.COMMIT,
                    status=self._metrics_context["execution_status"],
                    error_code=self._metrics_context["error_code"],
                    error_message=self._metrics_context["error_message"],
                    execution_time_sec=Decimal(
                        str(round(self._metrics_context["total_execution_time"], 2))
                    ),
                    metrics={
                        "total_commits": self._metrics_context["total_commits"],
                        "processed_commits": self._metrics_context["processed_commits"],
                        "bad_commits": self._metrics_context["bad_commits"],
                        "skipped_activities": self._metrics_context["skipped_activities"],
                        "total_activities": self._metrics_context["total_activities"],
                    },
                )
                await save_service_execution(service_execution)
                # Reset metrics context after saving
                self._metrics_context = None

        except Exception as e:
            # Update metrics context with error info
            batch_end_time = time.time()
            batch_time = round(batch_end_time - batch_start_time, 2)
            self._metrics_context["total_execution_time"] += batch_time
            self._metrics_context["execution_status"] = ExecutionStatus.FAILURE

            error_message = e.error_message if isinstance(e, CrowdGitError) else repr(e)
            self._metrics_context["error_code"] = (
                e.error_code.value if isinstance(e, CrowdGitError) else ErrorCode.UNKNOWN.value
            )
            self._metrics_context["error_message"] = error_message

            self.logger.error(f"Commit processing failed: {error_message}")

            # Save metrics on error
            service_execution = ServiceExecution(
                repo_id=repository.id,
                operation_type=OperationType.COMMIT,
                status=self._metrics_context["execution_status"],
                error_code=self._metrics_context["error_code"],
                error_message=self._metrics_context["error_message"],
                execution_time_sec=Decimal(
                    str(round(self._metrics_context["total_execution_time"], 2))
                ),
                metrics={
                    "total_commits": self._metrics_context["total_commits"],
                    "processed_commits": self._metrics_context["processed_commits"],
                    "bad_commits": self._metrics_context["bad_commits"],
                    "skipped_activities": self._metrics_context["skipped_activities"],
                    "total_activities": self._metrics_context["total_activities"],
                },
            )
            await save_service_execution(service_execution)
            # Reset metrics context after saving
            self._metrics_context = None
            raise

    async def _get_commit_reference(self, repo_path: str) -> str:
        """Get the commit reference for git log command."""
        default_branch = await get_default_branch(repo_path)
        if default_branch == "*":  # detached HEAD
            return "HEAD"
        return f"origin/{default_branch}"

    def _build_git_log_command(self, repo_path: str, commit_range: str) -> list[str]:
        """Build git log commands for commits and numstats."""
        return [
            "git",
            "-C",
            repo_path,
            "log",
            commit_range,
            "--cc",
            "--numstat",
            f"--pretty=format:{self.git_log_format}",
        ]

    def _parse_numstats(self, raw_numstats: str) -> tuple[int, int]:
        """
        Parse raw numstats into -> (insertions, deletions).

        Args:
            raw_numstats: Raw output from git log --numstat --pretty=format:%H

        Returns:
            (insertions, deletions) tuple
        """
        insertions_deletions = raw_numstats.strip().splitlines()
        insertions = 0
        deletions = 0

        for line in insertions_deletions:
            match = re.match(r"^(\d+)\s+(\d+)", line)
            if match:
                insertions += int(match.group(1))
                deletions += int(match.group(2))

        return (insertions, deletions)

    async def _get_optimized_commit_range(
        self,
        repo_path: str,
        edge_commit: str,
        prev_batch_edge_commit: str,
        last_processed_commit: str | None,
    ) -> str:
        """
        Optimize commit range by using last_processed_commit if available in current batch.

        For middle batches, returns the slice of history between the last batch's edge and this one's.
        If last_processed_commit exists in the current batch and is not the shallow boundary,
        uses it as the range start to skip already-processed commits.

        Args:
            repo_path: Local repository path
            edge_commit: Current batch's edge commit (shallow boundary)
            prev_batch_edge_commit: Previous batch's edge commit (upper bound of range)
            last_processed_commit: Last commit that was successfully processed (if any)

        Returns:
            Git commit range string (e.g., "commit_a..commit_b")
        """

        default_commit_range = f"{edge_commit}..{prev_batch_edge_commit}"

        if last_processed_commit and last_processed_commit != edge_commit:
            try:
                self.logger.info("Checking last processed commit existence in current batch")
                await run_shell_command(
                    ["git", "cat-file", "-e", last_processed_commit], cwd=repo_path
                )
                self.logger.info("Found! using optimized range")
                default_commit_range = f"{last_processed_commit}..{prev_batch_edge_commit}"
            except Exception:
                self.logger.info("last processed commit not found in range")
        return default_commit_range

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(1),
        reraise=True,
    )
    async def _execute_git_log(
        self,
        repo_path: str,
        clone_with_batches: bool,
        prev_batch_edge_commit: str | None = None,
        edge_commit: str | None = None,
        last_processed_commit: str | None = None,
    ) -> str:
        """Execute git log command and return raw output."""
        # Ensure abbreviated commits are disabled
        await run_shell_command(
            ["git", "-C", repo_path, "config", "core.abbrevCommit", "false"], cwd=repo_path
        )

        self.logger.info("Running git log commands...")

        if not clone_with_batches:
            commit_reference = await self._get_commit_reference(repo_path)
            self.logger.info(
                f"Full repo cloned in single batch, getting all commits in {commit_reference}"
            )
            raw_commits_cmd = self._build_git_log_command(repo_path, commit_reference)
            return await run_shell_command(raw_commits_cmd)

        if not prev_batch_edge_commit:
            return ""

        if edge_commit:
            commit_range = await self._get_optimized_commit_range(
                repo_path, edge_commit, prev_batch_edge_commit, last_processed_commit
            )
            self.logger.info(f"Processing middle batch: {commit_range}")
        else:
            # Final batch: Get all commits from the last known edge to the root.
            commit_range = prev_batch_edge_commit
            self.logger.info(f"Processing final batch from: {prev_batch_edge_commit} to root")

        raw_commits_cmd = self._build_git_log_command(repo_path, commit_range)

        self.logger.info(f"Executing git log for range: {commit_range}")
        return await run_shell_command(raw_commits_cmd)

    def should_skip_commit(self, raw_commit: str | None, edge_commit: str | None) -> bool:
        """Check if commit should be skipped based on edge commit comparison."""
        # Only skip the boundary commit of the current shallow clone.
        return not raw_commit or (edge_commit and raw_commit.startswith(edge_commit))

    def clean_up_username(self, name: str):
        name = re.sub(r"(?i)Reviewed[- ]by:", "", name)
        name = re.sub(r"(?i)from:", "", name)
        name = re.sub(r"(?i)cc:.*", "", name).strip()
        return name.strip()

    def create_activity(
        self,
        remote: str,
        commit: dict,
        activity_type: str,
        member: dict,
        source_id: str,
        segment_id: str,
        re_onboarding_count: int,
        source_parent_id: str = "",
    ) -> dict:
        """
        Create an activity with improved efficiency.

        Args:
            remote: The remote repository URL
            commit: The commit dictionary
            activity_type: Type of activity
            member: Member information dictionary
            source_id: Source ID for the activity
            segment_id: Segment identifier
            re_onboarding_count: Number of times the repository has been re-onboarded.
                Used to set activity.attributes.cycle when > 0.
            source_parent_id: Parent source ID (optional)

        Returns:
            Activity dictionary
        """
        # Pre-calculate timestamp to avoid repeated lookups
        timestamp = (
            commit["author_datetime"] if source_parent_id == "" else commit["committer_datetime"]
        )
        dt = datetime.datetime.fromisoformat(timestamp)

        # Pre-calculate common values
        emails = member["emails"]
        primary_email = emails[0]

        # Optimize member processing by creating a new dict instead of modifying the input
        processed_member = {}

        # Handle username and displayName with fallbacks
        display_name = member.get("displayName") or primary_email.split("@")[0]

        # Clean up names once
        processed_member["displayName"] = self.clean_up_username(display_name)

        # Build identities list more efficiently
        identities = [
            {
                "platform": self._GIT_PLATFORM,
                "value": primary_email,
                "type": self._USERNAME_TYPE,
                "verified": True,
            }
        ]

        # Add email identities
        for email in emails:
            identities.append(
                {
                    "platform": self._GIT_PLATFORM,
                    "value": email,
                    "type": self._EMAIL_TYPE,
                    "verified": False,
                }
            )

        processed_member["identities"] = identities

        # Pre-calculate commit attributes to avoid repeated lookups
        insertions = commit.get("insertions", 0)
        deletions = commit.get("deletions", 0)
        activity = {
            "type": activity_type,
            "timestamp": timestamp,
            "sourceId": source_id,
            "sourceParentId": source_parent_id,
            "platform": self._GIT_PLATFORM,
            "channel": remote,
            "body": "\n".join(commit["message"]),
            "username": primary_email,
            "attributes": {
                "insertions": insertions,
                "timezone": dt.tzname(),
                "deletions": deletions,
                "lines": insertions - deletions,
                "isMerge": commit["is_merge_commit"],
                "isMainBranch": True,
            },
            "url": remote,
            "member": processed_member,
            "segmentId": segment_id,
        }
        if re_onboarding_count > 0:
            activity["attributes"]["cycle"] = f"onboarding-{re_onboarding_count}"
        return activity

    def extract_activities(self, commit_message: list[str]) -> list[dict[str, dict[str, str]]]:
        """
        Extract activities from the commit message and return a list of activities.
        Each activity in the list includes the activity and the person who made it,
        which in turn includes the name and the email.

        :param commit_message: A list of strings, where each string is a line of the commit message.
        :return: A list of dictionaries containing activities and the person who made them.

        >>> extract_activities([
        ...     "Signed-off-by: Arnd Bergmann <arnd@arndb.de>",
        ...     "reported-by: Guenter Roeck <linux@roeck-us.net>"
        ... ]) == [{'Signed-off-by': {'email': 'arnd@arndb.de', 'name': 'Arnd Bergmann'}},
        ...        {'Reported-by': {'email': 'linux@roeck-us.net', 'name': 'Guenter Roeck'}}]
        True
        """
        activities = []

        for line in commit_message:
            match = self._ACTIVITY_PATTERN.match(line)
            if match:
                activity_name, name, email = match.groups()
                activity_name = activity_name.strip().lower()
                if activity_name in ActivityMap:
                    name = name.strip()
                    email = email.strip()
                    for activity in ActivityMap[activity_name]:
                        activities.append({activity: {"name": name, "email": email}})
        return activities

    def prepare_activity_for_db_and_queue(
        self, activity: dict, segment_id: str, integration_id: str
    ) -> tuple[tuple, dict]:
        activity["segmentId"] = segment_id
        result_id = str(uuid.uuid1())

        data_dict = {
            "type": IntegrationResultType.ACTIVITY,
            "data": activity,
        }
        activity_db = (
            result_id,
            IntegrationResultState.PENDING,
            orjson.dumps(data_dict).decode(),
            DEFAULT_TENANT_ID,
            integration_id,
        )
        operation = "upsert_activities_with_members"
        activity_kafka = {
            "message_id": f"{DEFAULT_TENANT_ID}-{operation}-{self._GIT_PLATFORM}-{result_id}",
            "payload": orjson.dumps(
                {
                    "type": DataSinkWorkerQueueMessageType.PROCESS_INTEGRATION_RESULT,
                    "tenantId": DEFAULT_TENANT_ID,
                    "segmentId": segment_id,
                    "integrationId": integration_id,
                    "resultId": result_id,
                }
            ).decode(),
        }
        return activity_db, activity_kafka

    def create_activities_from_commit(
        self,
        remote: str,
        commit: dict[str, Any],
        segment_id: str,
        integration_id: str,
        re_onboarding_count: int,
    ) -> tuple[list[tuple], list[dict[str, Any]]]:
        """
        Create activities from a commit with improved efficiency.

        Args:
            remote: The remote repository URL
            commit: The commit dictionary containing commit data
            segment_id: Segment identifier
            integration_id: Integration identifier
            re_onboarding_count: Number of times the repository has been re-onboarded.
                Used to set activity.attributes.cycle when > 0.

        Returns:
            Tuple of (activities_db, activities_queue) lists
        """
        activities_db = []
        activities_queue = []
        commit_hash = commit["hash"]

        # Pre-calculate common values to avoid repeated lookups
        author_name = commit["author_name"]
        author_email = commit["author_email"]
        committer_name = commit["committer_name"]
        committer_email = commit["committer_email"]

        # Create author activity
        author = {
            "username": author_email,
            "displayName": author_name,
            "emails": [author_email],
        }
        activity = self.create_activity(
            remote=remote,
            commit=commit,
            activity_type="authored-commit",
            member=author,
            source_id=commit_hash,
            segment_id=segment_id,
            re_onboarding_count=re_onboarding_count,
        )
        activity_db, activity_kafka = self.prepare_activity_for_db_and_queue(
            activity, segment_id, integration_id
        )
        activities_db.append(activity_db)
        activities_queue.append(activity_kafka)

        # Only create committer activity if author and committer are different
        if author_name != committer_name or author_email != committer_email:
            # IMPORTANT: hash_input has a typo in "commited" instead of "committed"
            # however fixing it requires recalculating sourceId/parentSourceId for ALL git activities in db
            # so far the typo doesn't have any major effect, since the activity type "committed-commit" is correct
            hash_input = f"{commit_hash}commited-commit{committer_email}"
            committer_source_id = hashlib.sha1(hash_input.encode("utf-8")).hexdigest()

            committer = {
                "username": committer_email,
                "displayName": committer_name,
                "emails": [committer_email],
            }
            activity = self.create_activity(
                remote=remote,
                commit=commit,
                activity_type="committed-commit",
                member=committer,
                source_id=committer_source_id,
                source_parent_id=commit_hash,
                segment_id=segment_id,
                re_onboarding_count=re_onboarding_count,
            )
            activity_db, activity_kafka = self.prepare_activity_for_db_and_queue(
                activity, segment_id, integration_id
            )
            activities_db.append(activity_db)
            activities_queue.append(activity_kafka)

        # Process extracted activities from commit message
        extracted_activities = self.extract_activities(commit["message"])
        for extracted_activity in extracted_activities:
            activity_type, member_data = list(extracted_activity.items())[0]

            # Convert activity type to lowercase and add "-commit" suffix
            # This matches the legacy behavior: "signed-off-by" -> "signed-off-commit"
            activity_type = activity_type.lower().replace("-by", "") + "-commit"

            member = {
                "displayName": member_data["name"],
                "emails": [member_data["email"]],
            }

            # Generate unique source ID for extracted activity
            source_id = hashlib.sha1(
                (commit_hash + activity_type + member_data["email"]).encode("utf-8")
            ).hexdigest()
            activity = self.create_activity(
                remote=remote,
                commit=commit,
                activity_type=activity_type,
                member=member,
                source_id=source_id,
                source_parent_id=commit_hash,
                segment_id=segment_id,
                re_onboarding_count=re_onboarding_count,
            )
            activity_db, activity_kafka = self.prepare_activity_for_db_and_queue(
                activity, segment_id, integration_id
            )
            activities_db.append(activity_db)
            activities_queue.append(activity_kafka)

        return activities_db, activities_queue

    async def _filter_existing_activities(
        self,
        activities_db: list[tuple],
        activities_queue: list[dict],
        source_repo: Repository,
    ) -> tuple[list[tuple], list[dict], int]:
        """
        Filter out activities that exist in specific repo, used for both forked and frequently reonboarded repos.
        Done in post-processing phase using batch lookup to avoid N+1 queries.

        Returns: (filtered_activities_db, filtered_activities_queue, skipped_activities_count)
        """
        if not activities_db:
            return activities_db, activities_queue, 0

        activity_keys = []
        for act in activities_db:
            data = orjson.loads(act[2])["data"]
            activity_keys.append((data["timestamp"], data["type"], data["sourceId"]))

        # Batch check which activities exist in parent repo
        parent_source_ids = await batch_check_parent_activities(
            activity_keys,
            source_repo.url,
            source_repo.segment_id,
        )

        if not parent_source_ids:
            return activities_db, activities_queue, 0

        filtered_activities_db = []
        filtered_activities_queue = []
        skipped_activities_count = 0

        for i, activity_tuple in enumerate(activities_db):
            activity_data = orjson.loads(activity_tuple[2])
            source_id = activity_data["data"]["sourceId"]

            if source_id not in parent_source_ids:
                # Activity doesn't exist in parent repo, keep it
                filtered_activities_db.append(activity_tuple)
                filtered_activities_queue.append(activities_queue[i])
            else:
                # Activity exists in parent repo, skip it
                skipped_activities_count += 1

        if skipped_activities_count > 0:
            self.logger.info(
                f"Filtered out {skipped_activities_count} existing activity from {source_repo.url}"
            )

        return filtered_activities_db, filtered_activities_queue, skipped_activities_count

    async def process_commits_chunk(
        self,
        commit_texts_chunk: list[str | None],
        batch_info: CloneBatchInfo,
        repository: Repository,
    ) -> None:
        """
        Process a chunk of raw commit texts into activities and write them to DB and Kafka.

        Args:
            commit_texts_chunk: List of commit text strings to process
            repo_path: Path to the repository
            edge_commit_hash: Edge commit hash for filtering
            remote: Remote repository URL
            segment_id: Segment identifier
            integration_id: Integration identifier
        """
        activities_db = []
        activities_queue = []
        bad_commits = 0
        processed_commits = 0
        commit = None

        for full_commit_text in commit_texts_chunk:
            if self.should_skip_commit(full_commit_text, batch_info.edge_commit):
                continue
            commit_text, numstats_text = full_commit_text.split(self.NUMSTAT_SPLITTER)
            commit_lines = commit_text.strip().splitlines()
            del full_commit_text
            del commit_text
            if not self._validate_commit_structure(commit_lines):
                self.logger.warning(
                    f"Invalid commit structure in {batch_info.repo_path}: {len(commit_lines)} fields"
                )
                bad_commits += 1
                del commit_lines
                del numstats_text
                continue

            try:
                commit = self._construct_commit_dict(commit_lines, numstats_text)
                if self._validate_commit_data(commit):
                    activity_db_records, activity_kafka = self.create_activities_from_commit(
                        batch_info.remote,
                        commit,
                        repository.segment_id,
                        repository.integration_id,
                        repository.re_onboarding_count,
                    )
                    activities_db.extend(activity_db_records)
                    activities_queue.extend(activity_kafka)
                    del activity_db_records
                    del activity_kafka
                    processed_commits += 1
                else:
                    bad_commits += 1

            except Exception as e:
                self.logger.warning(f"Failed to parse commit in {batch_info.repo_path}: {e}")
                bad_commits += 1
                continue
            finally:
                del commit
                del commit_lines
                del numstats_text

        # Filter out activities from parent repo (for forks)
        skipped_activities = 0
        if repository.parent_repo:
            (
                activities_db,
                activities_queue,
                skipped_activities,
            ) = await self._filter_existing_activities(
                activities_db, activities_queue, repository.parent_repo
            )

        self.logger.info(
            f"Processed {processed_commits} commits, skipped {bad_commits} invalid commits, filtered {skipped_activities} activities from parent repo in {batch_info.repo_path}"
        )
        # Update metrics context
        if self._metrics_context:
            self._metrics_context["processed_commits"] += processed_commits
            self._metrics_context["bad_commits"] += bad_commits
            self._metrics_context["total_activities"] += len(activities_db)
            self._metrics_context["skipped_activities"] += skipped_activities

        # Write activities to database and queue
        if activities_db:
            await asyncio.gather(
                batch_insert_activities(activities_db),
                self.queue_service.send_batch_activities(activities_queue),
            )

        del activities_db, activities_queue

    async def _process_activities_from_commits(
        self, raw_commits: str, batch_info: CloneBatchInfo, repository: Repository
    ):
        """
        Parse raw git log output, process commits into activities, and save to database.
        """
        commit_texts = [
            c.strip() for c in raw_commits.split(self.COMMIT_START_SPLITTER) if c.strip()
        ]
        del raw_commits
        gc.collect()

        logger.info(f"Actual number of commits to be processed: {len(commit_texts)}")

        # Update total_commits metric
        if self._metrics_context:
            self._metrics_context["total_commits"] += len(commit_texts)

        if len(commit_texts) == 0:
            self.logger.info("No commits to be processed")
            return

        chunk_size = self.MAX_CHUNK_SIZE
        total_chunks = (len(commit_texts) + chunk_size - 1) // chunk_size
        self.logger.info(
            f"Total commits {len(commit_texts)} - Total chunks: {total_chunks} - Chunk size: {chunk_size}"
        )

        max_concurrent = 2
        semaphore = asyncio.Semaphore(max_concurrent)
        self.logger.info(f"Processing with max_concurrent={max_concurrent}")

        completed_chunks = 0

        async def process_single_chunk(chunk_start_idx: int, chunk_end_idx: int):
            nonlocal completed_chunks, total_chunks

            async with semaphore:
                chunk = commit_texts[chunk_start_idx:chunk_end_idx]  # noqa: F821
                try:
                    # Process chunk and write to DB/Kafka
                    await self.process_commits_chunk(
                        chunk,
                        batch_info,
                        repository,
                    )
                    completed_chunks += 1
                    self.logger.info(f"Progress: {completed_chunks}/{total_chunks} chunks")
                    del chunk
                except Exception as e:
                    self.logger.error(f"Error processing chunk: {repr(e)}")
                    raise

        tasks = [
            process_single_chunk(i, min(i + chunk_size, len(commit_texts)))
            for i in range(0, len(commit_texts), chunk_size)
        ]

        self.logger.info(
            f"Created {len(tasks)} tasks. Processing with max {max_concurrent} concurrent tasks"
        )

        try:
            await asyncio.gather(*tasks)

            self.logger.info(f"All {total_chunks} chunks processed successfully.")

        except Exception as e:
            self.logger.error(
                f"Error during chunk processing at chunk {completed_chunks}/{total_chunks}: {e}"
            )
            raise

        finally:
            del commit_texts
            gc.collect()
            self.logger.info("Memory cleanup completed")

    def _construct_commit_dict(
        self, commit_metadata_lines: list[str], numstats_text: str
    ) -> dict[str, Any]:
        """Create commit dictionary from parsed lines."""
        commit_hash = commit_metadata_lines[0]
        author_datetime = commit_metadata_lines[1]
        author_name = commit_metadata_lines[2]
        author_email = commit_metadata_lines[3]
        commit_datetime = commit_metadata_lines[4]
        committer_name = commit_metadata_lines[5]
        committer_email = commit_metadata_lines[6]
        parent_hashes = commit_metadata_lines[7].split()

        # Use name as email if email is empty and name is a valid email
        author_email = (
            author_name
            if (not author_email or not author_email.strip()) and self._is_valid_email(author_name)
            else author_email
        )
        committer_email = (
            committer_name
            if (not committer_email or not committer_email.strip())
            and self._is_valid_email(committer_name)
            else committer_email
        )

        # Handle optional fields safely
        commit_message = commit_metadata_lines[9:] if len(commit_metadata_lines) > 9 else []

        # Validate and adjust commit datetime if it's in the future
        adjusted_commit_datetime = self._validate_and_adjust_datetime(
            commit_datetime, author_datetime
        )

        # Parse numstats to get insertions/deletions
        insertions, deletions = self._parse_numstats(numstats_text)
        # release memory
        del numstats_text
        del commit_metadata_lines

        return {
            "hash": commit_hash,
            "author_datetime": author_datetime,
            "author_name": author_name,
            "author_email": author_email,
            "committer_datetime": adjusted_commit_datetime,
            "committer_name": committer_name,
            "committer_email": committer_email,
            "is_main_branch": True,
            "is_merge_commit": len(parent_hashes) > 1,
            "message": commit_message,
            "insertions": insertions,
            "deletions": deletions,
        }

    def _validate_commit_structure(self, commit_lines: list[str]) -> bool:
        """Validate that commit has minimum required fields."""
        return len(commit_lines) >= self.MIN_COMMIT_FIELDS

    def _validate_commit_data(self, commit_dict: dict[str, Any]) -> bool:
        """Validate commit data content."""
        # Check required fields
        if not commit_dict.get("author_email", "").strip():
            self.logger.info(f"Commit without author_email: {commit_dict.get('hash', 'unknown')}")
            return False

        # Validate commit hash format
        if not self.is_valid_commit_hash(commit_dict.get("hash", "")):
            self.logger.error(f"Invalid commit hash: {commit_dict.get('hash', 'unknown')}")
            return False

        # Validate datetime format
        if not self.is_valid_datetime(commit_dict.get("committer_datetime", "")):
            self.logger.error(
                f"Invalid commit datetime: {commit_dict.get('committer_datetime', 'unknown')}"
            )
            return False

        return True

    def _is_valid_email(self, email: str) -> bool:
        """Check if a string is a valid email format using Pydantic validation."""
        try:
            validate_email(email.strip())
            return True
        except Exception:
            return False

    def _validate_and_adjust_datetime(self, commit_datetime: str, author_datetime: str) -> str:
        """Validate commit datetime and adjust if it's in the future."""
        try:
            commit_datetime_obj = datetime.datetime.strptime(commit_datetime, self.DATETIME_FORMAT)
            future_threshold = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
                days=self.FUTURE_DATE_THRESHOLD_DAYS
            )

            if commit_datetime_obj > future_threshold:
                self.logger.warning(
                    f"Commit datetime in future, using author datetime instead: {commit_datetime}"
                )
                return author_datetime

            return commit_datetime

        except ValueError:
            self.logger.warning(
                f"Invalid commit datetime format: {commit_datetime}, using author datetime"
            )
            return author_datetime
