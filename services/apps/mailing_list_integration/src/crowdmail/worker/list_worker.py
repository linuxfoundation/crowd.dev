import asyncio
import uuid

from crowdmail.database.crud import (
    acquire_list_for_processing,
    batch_insert_activities,
    mark_list_processed,
    release_list,
    update_processed_heads,
)
from crowdmail.enums import IntegrationResultState, IntegrationResultType, ListState
from crowdmail.logger import logger
from crowdmail.models.list import MailingList
from crowdmail.services.mirror.mirror_service import (
    discover_shards,
    ensure_mirror,
    new_commits,
    read_email,
    shard_index,
)
from crowdmail.services.parse.noteren import parse_email
from crowdmail.services.queue import queue_service
from crowdmail.settings import (
    DEFAULT_TENANT_ID,
    WORKER_ERROR_BACKOFF_SEC,
    WORKER_POLLING_INTERVAL_SEC,
)


class ListWorker:
    """Worker that mirrors, parses and emits activities for mailing lists"""

    def __init__(self):
        self._shutdown = False

    async def run(self):
        logger.info("Starting mailing list worker")
        try:
            await self._run()
            logger.info("Worker _run() method completed")
        finally:
            logger.info("Worker run() method exiting")

    async def _run(self):
        try:
            logger.info("Starting list worker loop...")
            while not self._shutdown:
                try:
                    await self._process_lists()
                    await asyncio.sleep(WORKER_POLLING_INTERVAL_SEC)
                except Exception as e:
                    logger.error("Worker error: {}", e)
                    await asyncio.sleep(WORKER_ERROR_BACKOFF_SEC)
            logger.info("Worker loop completed")
        finally:
            await queue_service.shutdown()
            logger.info("Worker processing loop completed")

    async def shutdown(self):
        logger.info("Shutting down list worker")
        self._shutdown = True

    async def _process_lists(self):
        mailing_list = None
        try:
            mailing_list = await acquire_list_for_processing()
            if not mailing_list:
                logger.debug("No mailing lists to process")
                return
            await self._process_single_list(mailing_list)
        except Exception as e:
            logger.error(f"Failed to process mailing list {mailing_list} with error {e}")
        finally:
            if mailing_list:
                logger.info(f"releasing list: {mailing_list.source_url}")
                await release_list(mailing_list.id)
                logger.info(f"List {mailing_list.source_url} released!")

    async def _process_single_list(self, mailing_list: MailingList):
        logger.info("Processing mailing list: {}", mailing_list.source_url)
        state = ListState.FAILED

        try:
            list_dir = await ensure_mirror(mailing_list.name, mailing_list.source_url)
            heads = dict(mailing_list.last_processed_heads)
            activities_db = []
            activities_kafka = []

            for shard_path in discover_shards(list_dir):
                shard = shard_index(shard_path)
                commit_ids = await new_commits(shard_path, heads.get(shard))
                for git_id in commit_ids:
                    message, blob_id = read_email(shard_path, git_id)
                    parsed = parse_email(
                        message,
                        mailing_list.source_url,
                        mailing_list.name,
                        git_id,
                        blob_id,
                        mailing_list.segment_id,
                        mailing_list.integration_id,
                    )
                    activity_data = parsed["activityData"]
                    if not activity_data["timestamp"]:
                        logger.warning(
                            "Skipping message {} with no parseable Date header",
                            activity_data["sourceId"],
                        )
                        continue
                    activity_data["segmentId"] = mailing_list.segment_id

                    result_id = str(uuid.uuid1())
                    data_dict = {"type": IntegrationResultType.ACTIVITY, "data": activity_data}
                    activities_db.append(
                        (
                            result_id,
                            IntegrationResultState.PENDING,
                            data_dict,
                            DEFAULT_TENANT_ID,
                            mailing_list.integration_id,
                        )
                    )
                    activities_kafka.append(
                        queue_service.build_activity_kafka_message(
                            mailing_list.segment_id, mailing_list.integration_id, result_id
                        )
                    )
                if commit_ids:
                    heads[shard] = commit_ids[-1]

            if activities_db:
                await batch_insert_activities(activities_db)
                await queue_service.send_batch_activities(activities_kafka)

            await update_processed_heads(mailing_list.id, heads)
            state = ListState.COMPLETED
        except Exception as e:
            logger.error(f"Processing failed for list {mailing_list.source_url}: {repr(e)}")
            state = ListState.FAILED
        finally:
            logger.info(f"Updating list {mailing_list.source_url} state to {state}")
            await mark_list_processed(mailing_list.id, state)

        logger.info("Completed processing mailing list: {}", mailing_list.source_url)
