from datetime import datetime, timezone

from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from crowdgit.enums import RepositoryPriority, RepositoryState
from crowdgit.errors import RepoLockingError
from crowdgit.models.repository import Repository
from crowdgit.models.service_execution import ServiceExecution
from crowdgit.settings import (
    MAX_CONCURRENT_ONBOARDINGS,
    MAX_INTEGRATION_RESULTS,
    REPOSITORY_UPDATE_INTERVAL_HOURS,
)

from .connection import get_db_connection
from .registry import execute, executemany, fetchrow, fetchval, query

# Common SELECT columns joining public.repositories + git.repositoryProcessing with aliases for backwards compatibility
REPO_SELECT_COLUMNS = """
    r.id,
    r.url,
    r."segmentId",
    r."gitIntegrationId",
    r."forkedFrom",
    rp.state,
    rp.priority,
    rp."lockedAt",
    rp."lastProcessedAt",
    rp."lastProcessedCommit",
    rp.branch,
    rp."maintainerFile",
    rp."lastMaintainerRunAt",
    rp."stuckRequiresReOnboard",
    rp."reOnboardingCount"
"""


async def get_recently_processed_repository_by_url(url: str) -> Repository | None:
    """
    Get repository by URL that was processed within the configured update interval.

    Returns the repository only if it was last processed within REPOSITORY_UPDATE_INTERVAL_HOURS
    and has a COMPLETED state.
    Used to check if a repository needs reprocessing based on the update interval.
    """
    sql_query = f"""
    SELECT {REPO_SELECT_COLUMNS}
    FROM public.repositories r
    JOIN git."repositoryProcessing" rp ON rp."repositoryId" = r.id
    WHERE r.url = $1
        AND r."deletedAt" IS NULL
        AND rp."lastProcessedAt" > NOW() - INTERVAL '1 hour' * $2
        AND rp.state = $3
    """
    result = await fetchrow(
        sql_query, (url, REPOSITORY_UPDATE_INTERVAL_HOURS, RepositoryState.COMPLETED)
    )
    return Repository.from_db(dict(result)) if result else None


async def acquire_onboarding_repo() -> Repository | None:
    onboarding_repo_sql_query = f"""
    WITH current_onboarding_count AS (
        SELECT COUNT(*) as count
        FROM git."repositoryProcessing" rp
        JOIN public.repositories r ON r.id = rp."repositoryId"
        WHERE rp.state = $1
            AND rp."lastProcessedCommit" IS NULL
            AND r."deletedAt" IS NULL
    ),
    selected_repo AS (
        SELECT r.id
        FROM public.repositories r
        JOIN git."repositoryProcessing" rp ON rp."repositoryId" = r.id
        CROSS JOIN current_onboarding_count c
        WHERE rp.state = $2
            AND rp."lockedAt" IS NULL
            AND r."deletedAt" IS NULL
            AND c.count < $3
        ORDER BY rp.priority ASC, rp."createdAt" ASC
        LIMIT 1
        FOR UPDATE OF rp SKIP LOCKED
    )
    UPDATE git."repositoryProcessing" rp
    SET "lockedAt" = NOW(),
        state = $1,
        "updatedAt" = NOW()
    FROM public.repositories r
    CROSS JOIN selected_repo
    WHERE rp."repositoryId" = r.id
        AND rp."repositoryId" = selected_repo.id
    RETURNING {REPO_SELECT_COLUMNS}
    """
    return await acquire_repository(
        onboarding_repo_sql_query,
        (RepositoryState.PROCESSING, RepositoryState.PENDING, MAX_CONCURRENT_ONBOARDINGS),
    )


@retry(
    retry=retry_if_exception_type(RepoLockingError),
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    reraise=True,
)
async def acquire_repository(query: str, params: tuple = None) -> Repository | None:
    async with get_db_connection() as conn:
        try:
            async with conn.transaction():
                result = await conn.fetchrow(query, *params)
                if result:
                    repo = Repository.from_db(dict(result))
                    logger.info(f"Acquired repository: {repo.url}")
                    return repo
                # logger.info(
                #     "No repository is available for processing based on the filtering rules"
                # )
                return None
        except Exception as e:
            logger.error(f"failed to acquire repository with error: {e}. Retrying...")
            raise RepoLockingError() from e


async def acquire_recurrent_repo() -> Repository | None:
    """Acquire a regular (non-onboarding) repository, that were not processed in the last x hours (REPOSITORY_UPDATE_INTERVAL_HOURS)"""
    recurrent_repo_sql_query = f"""
    WITH selected_repo AS (
        SELECT r.id
        FROM public.repositories r
        JOIN git."repositoryProcessing" rp ON rp."repositoryId" = r.id
        WHERE NOT (rp.state = ANY($2))
            AND rp."lockedAt" IS NULL
            AND r."deletedAt" IS NULL
            AND rp."lastProcessedAt" < NOW() - INTERVAL '1 hour' * $3
        ORDER BY rp.priority ASC, rp."lastProcessedAt" ASC
        LIMIT 1
        FOR UPDATE OF rp SKIP LOCKED
    )
    UPDATE git."repositoryProcessing" rp
    SET "lockedAt" = NOW(),
        state = $1,
        "updatedAt" = NOW()
    FROM public.repositories r
    CROSS JOIN selected_repo
    WHERE rp."repositoryId" = r.id
        AND rp."repositoryId" = selected_repo.id
    RETURNING {REPO_SELECT_COLUMNS}
    """
    states_to_exclude = (
        RepositoryState.PENDING,
        RepositoryState.PROCESSING,
        RepositoryState.STUCK,
        RepositoryState.PENDING_REONBOARD,
    )
    return await acquire_repository(
        recurrent_repo_sql_query,
        (RepositoryState.PROCESSING, states_to_exclude, REPOSITORY_UPDATE_INTERVAL_HOURS),
    )


async def can_onboard_more():
    """
    Check if system can handle more repository onboarding based on activity load.

    Returns False if integration.results count exceeds MAX_INTEGRATION_RESULTS
    or if the query fails (indicating high database load).
    """
    try:
        integration_results_count = await fetchval("SELECT COUNT(*) FROM integration.results")
        return integration_results_count < MAX_INTEGRATION_RESULTS
    except Exception as e:
        logger.warning(f"Failed to get integration.results count with error: {repr(e)}")
        return False  # if query failed mostly due to timeout then db is already under high load


async def acquire_pending_reonboard_repo() -> Repository | None:
    """Acquire a pending_reonboard repo for re-onboarding (only called on weekends)."""
    pending_reonboard_sql_query = f"""
    WITH selected_repo AS (
        SELECT r.id
        FROM public.repositories r
        JOIN git."repositoryProcessing" rp ON rp."repositoryId" = r.id
        WHERE rp.state = $1
            AND rp."lockedAt" IS NULL
            AND r."deletedAt" IS NULL
        ORDER BY rp.priority ASC, rp."lastProcessedAt" ASC
        LIMIT 1
        FOR UPDATE OF rp SKIP LOCKED
    )
    UPDATE git."repositoryProcessing" rp
    SET "lockedAt" = NOW(),
        state = $2,
        "lastProcessedCommit" = NULL,
        branch = NULL,
        "reOnboardingCount" = rp."reOnboardingCount" + 1,
        "updatedAt" = NOW()
    FROM public.repositories r
    CROSS JOIN selected_repo
    WHERE rp."repositoryId" = r.id
        AND rp."repositoryId" = selected_repo.id
    RETURNING {REPO_SELECT_COLUMNS}
    """
    return await acquire_repository(
        pending_reonboard_sql_query,
        (RepositoryState.PENDING_REONBOARD, RepositoryState.PROCESSING),
    )


async def acquire_repo_for_processing() -> Repository | None:
    """
    Acquire the next repository to process based on priority and system load.

    Priority logic:
    1. Onboarding repos (PENDING state) - only if system load allows and
       current onboarding count is below MAX_CONCURRENT_ONBOARDINGS
    2. Recurrent repos (non-PENDING/non-PROCESSING) - fallback when onboarding
       is unavailable or skipped due to high load
    3. Pending reonboard repos (PENDING_REONBOARD state) - weekend-only, lowest priority.
       These are repos needing re-onboarding that were deferred until the weekend.

    Onboarding is delayed when integration.results exceeds MAX_INTEGRATION_RESULTS
    to prevent overloading the system during high activity periods.
    """
    repo_to_process = None
    if await can_onboard_more():
        repo_to_process = await acquire_onboarding_repo()
    else:
        logger.info("Skipping onboarding due to high load on integration.results")

    if not repo_to_process:
        is_weekend = datetime.now(timezone.utc).weekday() >= 5
        if is_weekend:
            repo_to_process = await acquire_pending_reonboard_repo()

    if not repo_to_process:
        repo_to_process = await acquire_recurrent_repo()

    return repo_to_process


async def release_repo(repo_id: str):
    """
    Release repository lock (lockedAt) after processing
    """
    sql_query = """
    UPDATE git."repositoryProcessing"
        SET "lockedAt" = NULL,
        "updatedAt" = NOW()
    WHERE "repositoryId" = $1
    """
    result = await execute(sql_query, (repo_id,))
    return str(result)


async def update_last_processed_commit(repo_id: str, commit_hash: str, branch: str | None = None):
    """
    Update last processed commit and optionally the branch after processing
    """
    sql_query = """
    UPDATE git."repositoryProcessing"
        SET "lastProcessedCommit" = $1,
        "branch" = $2,
        "updatedAt" = NOW()
    WHERE "repositoryId" = $3
    """
    result = await execute(sql_query, (commit_hash, branch, repo_id))
    return str(result)


async def mark_repo_as_processed(repo_id: str, repo_state: RepositoryState):
    sql_query = """
    UPDATE git."repositoryProcessing"
        SET "state" = $2,
        "lastProcessedAt" = NOW(),
        "updatedAt" = NOW(),
        "priority" = $3
    WHERE "repositoryId" = $1
    """
    result = await execute(sql_query, (repo_id, repo_state, RepositoryPriority.NORMAL))
    return str(result)


async def batch_insert_activities(records: list[tuple], batch_size=100):
    sql_query = """
    INSERT INTO integration.results(id, state, data, "tenantId", "integrationId")
    values($1, $2, $3, $4, $5)
    """
    logger.info(f"Saving {len(records)} activity into integration.results")
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        await executemany(sql_query, batch)
    logger.info("activities saved into integration.results")


async def find_github_identity(github_username: str):
    sql_query = """
    SELECT id
        FROM "memberIdentities"
    WHERE
        platform = 'github'
        AND value = $1
        AND "deletedAt" is null
    LIMIT 1
    """
    result = await fetchval(
        sql_query,
        (github_username,),
    )
    return result


async def find_maintainer_identity_by_email(email: str):
    sql_query = """
    SELECT id
        FROM "memberIdentities"
    WHERE platform IN ('github', 'git', 'gitlab')
        AND "verified" = TRUE
        AND value = $1
        AND "deletedAt" is null
    LIMIT 1
    """
    result = await fetchval(
        sql_query,
        (email,),
    )
    return result


async def upsert_maintainer(
    repo_id: str,
    identity_id: str,
    repo_url: str,
    role: str,
    original_role: str,
    start_date: datetime | None = None,
):
    sql_query = """
        INSERT INTO "maintainersInternal"
        ("role", "originalRole", "repoUrl", "repoId", "identityId", "startDate", "endDate")
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT ("repoId", "identityId", "startDate", "endDate") DO UPDATE
        SET role = EXCLUDED.role, "originalRole" = EXCLUDED."originalRole", "updatedAt" = NOW()
    """
    await execute(
        sql_query,
        (role, original_role, repo_url, repo_id, identity_id, start_date, None),
    )


async def update_maintainer_run(repo_id: str, maintainer_file: str):
    """
    Update maintainer file info after processing.
    Updates git.repositoryProcessing table only.
    """
    sql_query = """
        UPDATE git."repositoryProcessing"
            SET "maintainerFile" = $1,
                "lastMaintainerRunAt" = NOW(),
                "updatedAt" = NOW()
        WHERE "repositoryId" = $2
    """
    await execute(
        sql_query,
        (maintainer_file, repo_id),
    )


async def get_maintainers_for_repo(repo_id: str):
    maintainers_sql_query = """
        SELECT mi.role, mi."originalRole", mi."repoUrl", mi."repoId", mi."identityId", mem.value as github_username
            FROM "maintainersInternal" mi
            JOIN "memberIdentities" mem ON mi."identityId" = mem.id
        WHERE mi."repoId" = $1 AND mem.platform = 'github' AND mem.type = 'username' and mem.verified = True AND mem."deletedAt" is null
        """
    return await query(
        maintainers_sql_query,
        (repo_id,),
    )


async def set_maintainer_end_date(
    repo_id: str, identity_id: str, role: str, change_date: datetime
):
    update_end_date_query = """
        UPDATE "maintainersInternal"
        SET "endDate" = $1,
            "updatedAt" = NOW()
        WHERE "repoId" = $2 AND "identityId" = $3 AND role = $4
    """
    await execute(
        update_end_date_query,
        (
            change_date,
            repo_id,
            identity_id,
            role,
        ),
    )


async def batch_check_parent_activities(
    activity_keys: list[tuple[str, str, str]],
    parent_channel: str,
    parent_segment_id: str,
) -> set[str]:
    """
    Batch check which activities exist in parent repo using full dedup key.

    Args:
        activity_keys: List of (timestamp, type, sourceId) tuples
        parent_channel: Parent repository URL
        parent_segment_id: Parent repository segment ID

    Returns:
        Set of sourceIds that exist in parent repo
    """
    if not activity_keys:
        return set()

    # Use dedup index with ALL fields for optimal performance
    # Index: (timestamp, platform, type, sourceId, channel, segmentId)
    # Build OR conditions for each (timestamp, type, sourceId) combination
    conditions = []
    params = ["git", parent_channel, parent_segment_id]
    param_idx = 4

    for timestamp_str, activity_type, source_id in activity_keys:
        conditions.append(
            f'("timestamp" = ${param_idx} AND "type" = ${param_idx + 1} AND "sourceId" = ${param_idx + 2})'
        )
        timestamp = datetime.fromisoformat(timestamp_str)
        params.append(timestamp)
        params.append(activity_type)
        params.append(source_id)
        param_idx += 3

    sql_query = f"""
    SELECT DISTINCT "sourceId"
    FROM "activityRelations"
    WHERE "platform" = $1
        AND "channel" = $2
        AND "segmentId" = $3
        AND ({" OR ".join(conditions)})
    """

    result = await query(sql_query, tuple(params))

    return {row["sourceId"] for row in result}


async def save_service_execution(service_execution: ServiceExecution) -> None:
    """
    Save service execution record to database.
    """
    try:
        sql_query = """
        INSERT INTO git."serviceExecutions" (
            "repoId", "operationType", "status", "errorCode",
            "errorMessage", "executionTimeSec", "metrics"
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """

        db_data = service_execution.to_db_dict()
        await execute(
            sql_query,
            (
                db_data["repoId"],
                db_data["operationType"],
                db_data["status"],
                db_data["errorCode"],
                db_data["errorMessage"],
                db_data["executionTimeSec"],
                db_data["metrics"],
            ),
        )
        logger.debug(
            f"Successfully saved service execution: {service_execution.operation_type} for repo {service_execution.repo_id}"
        )

    except Exception as e:
        logger.error(
            f"Failed to save service execution record: operation={service_execution.operation_type}, "
            f"repo_id={service_execution.repo_id}, status={service_execution.status.value}, "
            f"error: {e}"
        )
        # Do not re-raise - we don't want metrics saving to disrupt main operations
