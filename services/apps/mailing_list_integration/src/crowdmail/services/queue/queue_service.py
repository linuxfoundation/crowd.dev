"""Kafka producer for emitting parsed activities to the data-sink-worker queue."""

import asyncio
import json
import ssl

import orjson
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from crowdmail.enums import DataSinkWorkerQueueMessageType
from crowdmail.errors import QueueConnectionError, QueueMessageProduceError
from crowdmail.logger import logger
from crowdmail.settings import (
    CROWD_KAFKA_BROKERS,
    CROWD_KAFKA_EXTRA,
    CROWD_KAFKA_TOPIC,
    DEFAULT_TENANT_ID,
)

_CLIENT_ID = "mailing-list-integration"
_PLATFORM = "mailinglist"
_OPERATION = "upsert_activities_with_members"

# Global producer state
_producer: AIOKafkaProducer | None = None
_connected = False


def _build_kafka_config() -> dict:
    config = {
        "bootstrap_servers": CROWD_KAFKA_BROKERS,
        "client_id": _CLIENT_ID,
        "acks": "all",
    }
    if not CROWD_KAFKA_EXTRA:
        return config
    # Parse extra configuration from kafkajs config
    extra_config = json.loads(CROWD_KAFKA_EXTRA)

    if extra_config.get("ssl"):
        config["security_protocol"] = "SASL_SSL"
        config["ssl_context"] = ssl.create_default_context()
    else:
        config["security_protocol"] = "SASL_PLAINTEXT"

    sasl_config = extra_config["sasl"]
    config["sasl_mechanism"] = sasl_config["mechanism"].upper()
    config["sasl_plain_username"] = sasl_config["username"]
    config["sasl_plain_password"] = sasl_config["password"]

    return config


async def _is_connection_healthy() -> bool:
    try:
        if _producer is None or _producer._closed:
            return False
        sender_task = _producer._sender.sender_task
        return sender_task is not None and not sender_task.done()
    except Exception:
        return False


async def connect():
    global _producer, _connected
    if _connected:
        return
    logger.info("Connecting to kafka...")
    try:
        _producer = AIOKafkaProducer(**_build_kafka_config())
        await _producer.start()
        logger.info("Connected to kafka")
        _connected = True
    except Exception as e:
        logger.error(f"Queue connection failed: {e}")
        await disconnect()
        raise QueueConnectionError() from e


async def disconnect():
    global _producer, _connected
    if _producer is None or _producer._closed:
        logger.debug("Producer already closed, skipping")
        _producer = None
        _connected = False
        return

    try:
        await _producer.stop()
        logger.info("Disconnected from kafka")
    except Exception as e:
        logger.error(f"Error during disconnect: {e}")
    finally:
        _producer = None
        _connected = False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(QueueConnectionError),
    reraise=True,
)
async def _connect_with_retry():
    await connect()


async def ensure_connected():
    """Ensure connection is established and healthy"""
    if not _connected or not await _is_connection_healthy():
        if _connected:
            logger.info("Connection unhealthy, reconnecting...")
            await disconnect()
        await _connect_with_retry()


async def shutdown():
    """
    Flush pending messages, stop the producer, and release the connection.
    Never raises - safe to call during application shutdown.
    """
    logger.info("Shutting down queue service...")
    try:
        if _connected:
            try:
                await _producer.flush()
                logger.info("Flushed pending messages")
            except Exception as e:
                logger.warning(f"Error flushing messages during shutdown: {e}")

        await disconnect()
        logger.info("Queue service shutdown complete")
    except Exception as e:
        logger.error(f"Failed to shutdown queue service: {repr(e)}")
        # Don't raise - allow application to continue shutdown


_SEND_CHUNK_SIZE = 500


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(KafkaError),
    reraise=True,
)
async def _emit_batch(activities_kafka: list[dict[str, str]]):
    """Emit a batch in bounded-size chunks, rebuilding the producer and retrying on Kafka errors."""
    await ensure_connected()
    try:
        for i in range(0, len(activities_kafka), _SEND_CHUNK_SIZE):
            chunk = activities_kafka[i : i + _SEND_CHUNK_SIZE]
            futures = [
                _producer.send(
                    topic=CROWD_KAFKA_TOPIC,
                    key=activity["message_id"].encode("utf-8", errors="replace"),
                    value=activity["payload"].encode("utf-8", errors="replace"),
                )
                for activity in chunk
            ]
            await asyncio.gather(*futures, return_exceptions=False)
    except KafkaError:
        logger.warning("Kafka send failed, reconnecting before retry...")
        await disconnect()
        raise


def build_activity_kafka_message(segment_id: str, integration_id: str, result_id: str) -> dict:
    """Build the {message_id, payload} pair for one integration.results row."""
    return {
        "message_id": f"{DEFAULT_TENANT_ID}-{_OPERATION}-{_PLATFORM}-{result_id}",
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


async def send_batch_activities(activities_kafka: list[dict[str, str]]):
    """
    Send multiple pre-prepared activities to Kafka in a non-blocking way.
    Args:
        activities_kafka: List of dicts with 'message_id' and 'payload' keys,
                          each payload a process_integration_result message.
    """
    if not activities_kafka:
        return

    logger.info(f"Emitting {len(activities_kafka)} activities to kafka queue...")

    try:
        await _emit_batch(activities_kafka)
        logger.info(f"Successfully emitted {len(activities_kafka)} activities to queue")
    except Exception as e:
        logger.error(f"Failed to emit batch to queue with error: {repr(e)}")
        raise QueueMessageProduceError(
            f"Failed to send {len(activities_kafka)} messages to Kafka"
        ) from e
