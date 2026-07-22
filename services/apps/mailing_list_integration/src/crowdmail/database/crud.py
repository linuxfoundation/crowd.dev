from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from crowdmail.enums import ListPriority, ListState
from crowdmail.errors import ListLockingError
from crowdmail.models.list import MailingList
from crowdmail.settings import (
    FAILED_RETRY_INTERVAL_HOURS,
    LIST_UPDATE_INTERVAL_HOURS,
    MAX_CONCURRENT_ONBOARDINGS,
    STUCK_ONBOARDING_LIST_TIMEOUT_HOURS,
    STUCK_RECURRENT_LIST_TIMEOUT_HOURS,
)

from .connection import get_db_connection
from .registry import execute, executemany, fetchrow

# Common SELECT columns joining mailinglist.lists + mailinglist.listProcessing
LIST_SELECT_COLUMNS = """
    l.id,
    l.name,
    l."sourceUrl",
    l."segmentId",
    l."integrationId",
    lp.state,
    lp.priority,
    lp."lockedAt",
    lp."lastProcessedAt",
    lp."lastProcessedHeads"
"""


@retry(
    retry=retry_if_exception_type(ListLockingError),
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    reraise=True,
)
async def acquire_list(sql_query: str, params: tuple = None) -> MailingList | None:
    async with get_db_connection() as conn:
        try:
            async with conn.transaction():
                result = await conn.fetchrow(sql_query, *(params or ()))
                if result:
                    mailing_list = MailingList.from_db(dict(result))
                    logger.info(f"Acquired mailing list: {mailing_list.source_url}")
                    return mailing_list
                return None
        except Exception as e:
            logger.error(f"Failed to acquire mailing list with error: {e}. Retrying...")
            raise ListLockingError() from e


async def acquire_onboarding_list() -> MailingList | None:
    """Acquire a list that has never been processed (onboarding), bounded by
    MAX_CONCURRENT_ONBOARDINGS. Also reclaims onboarding lists stuck in PROCESSING
    past STUCK_ONBOARDING_LIST_TIMEOUT_HOURS (e.g. worker died mid-clone) — such
    rows don't count against the concurrency cap either, so a fully-stuck queue
    can't deadlock the cap forever.
    """
    sql_query = f"""
    WITH current_onboarding_count AS (
        SELECT COUNT(*) as count
        FROM mailinglist."listProcessing" lp
        WHERE lp.state = $1
            AND lp."lastProcessedAt" IS NULL
            AND lp."lockedAt" >= NOW() - INTERVAL '1 hour' * $4::numeric
    ),
    selected_list AS (
        SELECT l.id
        FROM mailinglist.lists l
        JOIN mailinglist."listProcessing" lp ON lp."listId" = l.id
        CROSS JOIN current_onboarding_count c
        WHERE (
            (lp.state = $2 AND lp."lockedAt" IS NULL)
            OR (
                lp.state = $1
                AND lp."lastProcessedAt" IS NULL
                AND lp."lockedAt" < NOW() - INTERVAL '1 hour' * $4::numeric
            )
        )
            AND l."deletedAt" IS NULL
            AND c.count < $3
        ORDER BY lp.priority ASC, lp."createdAt" ASC
        LIMIT 1
        FOR UPDATE OF lp SKIP LOCKED
    )
    UPDATE mailinglist."listProcessing" lp
    SET "lockedAt" = NOW(),
        state = $1,
        "updatedAt" = NOW()
    FROM mailinglist.lists l
    CROSS JOIN selected_list
    WHERE lp."listId" = l.id
        AND lp."listId" = selected_list.id
    RETURNING {LIST_SELECT_COLUMNS}
    """
    return await acquire_list(
        sql_query,
        (
            ListState.PROCESSING,
            ListState.PENDING,
            MAX_CONCURRENT_ONBOARDINGS,
            STUCK_ONBOARDING_LIST_TIMEOUT_HOURS,
        ),
    )


async def acquire_recurrent_list() -> MailingList | None:
    """Acquire a previously-processed list that is due for reprocessing. Also
    reclaims recurrent lists stuck in PROCESSING past
    STUCK_RECURRENT_LIST_TIMEOUT_HOURS (e.g. worker died mid-fetch) regardless of
    their normal reprocessing schedule, since a stuck row is by definition overdue.
    """
    sql_query = f"""
    WITH selected_list AS (
        SELECT l.id
        FROM mailinglist.lists l
        JOIN mailinglist."listProcessing" lp ON lp."listId" = l.id
        WHERE (
            (
                NOT (lp.state = ANY($2))
                AND lp."lastProcessedAt" < NOW() - INTERVAL '1 hour' * (
                    CASE WHEN lp.state = $4 THEN $5::numeric ELSE $3::numeric END
                )
            )
            OR (
                lp.state = $1
                AND lp."lastProcessedAt" IS NOT NULL
                AND lp."lockedAt" < NOW() - INTERVAL '1 hour' * $6::numeric
            )
        )
            AND l."deletedAt" IS NULL
        ORDER BY lp.priority ASC, lp."lastProcessedAt" ASC
        LIMIT 1
        FOR UPDATE OF lp SKIP LOCKED
    )
    UPDATE mailinglist."listProcessing" lp
    SET "lockedAt" = NOW(),
        state = $1,
        "updatedAt" = NOW()
    FROM mailinglist.lists l
    CROSS JOIN selected_list
    WHERE lp."listId" = l.id
        AND lp."listId" = selected_list.id
    RETURNING {LIST_SELECT_COLUMNS}
    """
    states_to_exclude = (ListState.PENDING, ListState.PROCESSING, ListState.PENDING_REONBOARD)
    return await acquire_list(
        sql_query,
        (
            ListState.PROCESSING,
            states_to_exclude,
            LIST_UPDATE_INTERVAL_HOURS,
            ListState.FAILED,
            FAILED_RETRY_INTERVAL_HOURS,
            STUCK_RECURRENT_LIST_TIMEOUT_HOURS,
        ),
    )


async def acquire_list_for_processing() -> MailingList | None:
    """Acquire the next list to process: onboarding lists first, then due recurrent lists."""
    mailing_list = await acquire_onboarding_list()
    if not mailing_list:
        mailing_list = await acquire_recurrent_list()
    return mailing_list


async def get_list_integration_info(list_id: str) -> dict | None:
    sql_query = """
    SELECT l."segmentId", l."integrationId"
    FROM mailinglist.lists l
    WHERE l.id = $1
        AND l."deletedAt" IS NULL
    """
    return await fetchrow(sql_query, (list_id,))


async def update_processed_heads(list_id: str, heads: dict) -> None:
    sql_query = """
    UPDATE mailinglist."listProcessing"
        SET "lastProcessedHeads" = $1::jsonb,
        "updatedAt" = NOW()
    WHERE "listId" = $2
    """
    await execute(sql_query, (heads, list_id))


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    reraise=True,
)
async def mark_list_processed(list_id: str, list_state: ListState) -> None:
    # Retried on any failure: if this write never lands, the list stays stuck
    # in its acquired state (e.g. PROCESSING) forever, since acquire_onboarding_list
    # only picks PENDING and acquire_recurrent_list excludes PROCESSING.
    sql_query = """
    UPDATE mailinglist."listProcessing"
        SET "state" = $2,
        "lastProcessedAt" = NOW(),
        "updatedAt" = NOW(),
        "priority" = $3
    WHERE "listId" = $1
    """
    await execute(sql_query, (list_id, list_state, ListPriority.NORMAL))


async def release_list(list_id: str) -> None:
    """Release list lock (lockedAt) after processing"""
    sql_query = """
    UPDATE mailinglist."listProcessing"
        SET "lockedAt" = NULL,
        "updatedAt" = NOW()
    WHERE "listId" = $1
    """
    await execute(sql_query, (list_id,))


async def batch_insert_activities(records: list[tuple], batch_size=100) -> None:
    sql_query = """
    INSERT INTO integration.results(id, state, data, "tenantId", "integrationId")
    values($1, $2, $3, $4, $5)
    """
    logger.info(f"Saving {len(records)} activities into integration.results")
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        await executemany(sql_query, batch)
    logger.info("Activities saved into integration.results")
