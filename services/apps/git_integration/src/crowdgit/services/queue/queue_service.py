import asyncio
import json
import ssl

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from crowdgit.errors import QueueConnectionError, QueueMessageProduceError
from crowdgit.services.base.base_service import BaseService
from crowdgit.settings import (
    CROWD_KAFKA_BROKERS,
    CROWD_KAFKA_EXTRA,
    CROWD_KAFKA_TOPIC,
)


class QueueService(BaseService):
    _CLIENT_ID = "git-integration-v2"

    def __init__(self):
        super().__init__()
        self.kafka_topic = CROWD_KAFKA_TOPIC
        self.kafka_producer: AIOKafkaProducer | None = None
        self._connected = False

    def _build_kafka_config(self):
        config = {
            "bootstrap_servers": CROWD_KAFKA_BROKERS,
            "client_id": self._CLIENT_ID,
            "acks": "all",
        }
        if not CROWD_KAFKA_EXTRA:
            return config
        # Parse extra configuration from kafkajs config
        extra_config = json.loads(CROWD_KAFKA_EXTRA)

        if extra_config.get("ssl"):
            config["security_protocol"] = "SASL_SSL"
            ssl_context = ssl.create_default_context()
            config["ssl_context"] = ssl_context
        else:
            config["security_protocol"] = "SASL_PLAINTEXT"

        sasl_config = extra_config["sasl"]
        config["sasl_mechanism"] = sasl_config["mechanism"].upper()
        config["sasl_plain_username"] = sasl_config["username"]
        config["sasl_plain_password"] = sasl_config["password"]

        return config

    async def ensure_connected(self):
        """Ensure connection is established and healthy"""
        if not self._connected or not await self._is_connection_healthy():
            if self._connected:
                self.logger.info("Connection unhealthy, reconnecting...")
                await self.disconnect()
            await self._connect_with_retry()

    async def _is_connection_healthy(self) -> bool:
        """Check if the current connection is healthy"""
        try:
            producer = self.kafka_producer
            if producer is None or producer._closed:
                return False
            sender_task = producer._sender.sender_task
            return sender_task is not None and not sender_task.done()
        except Exception:
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(QueueConnectionError),
        reraise=True,
    )
    async def _connect_with_retry(self):
        """
        Connect to Kafka with automatic retries.
        """
        await self.connect()

    async def connect(self):
        if self._connected:
            return
        self.logger.info("Connecting to kafka...")
        try:
            self.kafka_producer = AIOKafkaProducer(**self._build_kafka_config())
            await self.kafka_producer.start()
            self.logger.info("Connected to kafka")
            self._connected = True
        except Exception as e:
            self.logger.error(f"Queue connection failed: {e}")
            await self.disconnect()
            raise QueueConnectionError() from e

    async def disconnect(self):
        """
        Disconnect from Kafka and close producer
        """
        if self.kafka_producer is None or self.kafka_producer._closed:
            self.logger.debug("Producer already closed, skipping")
            self.kafka_producer = None
            self._connected = False
            return

        try:
            await self.kafka_producer.stop()
            self.logger.info("Disconnected from kafka")
        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")
        finally:
            self.kafka_producer = None
            self._connected = False

    async def shutdown(self):
        """
        Shutdown the queue service and cleanup resources .
        Ensures:
        - All pending messages are flushed
        - Producer is properly stopped
        - Connections are closed
        - No resource leaks
        """
        self.logger.info("Shutting down queue service...")

        try:
            if self._connected:
                # Flush any pending messages before stopping
                try:
                    await self.kafka_producer.flush()
                    self.logger.info("Flushed pending messages")
                except Exception as e:
                    self.logger.warning(f"Error flushing messages during shutdown: {e}")

            await self.disconnect()

            self.logger.info("Queue service shutdown complete")
        except Exception as e:
            self.logger.error(f"Failed to shutdown queue service: {repr(e)}")
            # Don't raise - allow application to continue shutdown

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(KafkaError),
        reraise=True,
    )
    async def _emit_batch(self, activities_kafka: list[dict[str, str]]):
        """Emit a single batch, rebuilding the producer and retrying on Kafka errors."""
        await self.ensure_connected()
        try:
            futures = [
                self.kafka_producer.send(
                    topic=self.kafka_topic,
                    key=activity["message_id"].encode("utf-8", errors="replace"),
                    value=activity["payload"].encode("utf-8", errors="replace"),
                )
                for activity in activities_kafka
            ]
            # Wait for all messages to be sent
            await asyncio.gather(*futures, return_exceptions=False)
        except KafkaError:
            self.logger.warning("Kafka send failed, reconnecting before retry...")
            await self.disconnect()
            raise

    async def send_batch_activities(self, activities_kafka: list[dict[str, str]]):
        """
        Send multiple pre-prepared activities to Kafka in a non-blocking way.
        Args:
            activities_kafka: List of dicts with 'message_id' and 'payload' keys
                             (prepared by CommitService.prepare_activity_for_db_and_queue)
        """
        if not activities_kafka:
            return

        self.logger.info(f"Emitting {len(activities_kafka)} activities to kafka queue...")

        try:
            await self._emit_batch(activities_kafka)
            self.logger.info(f"Successfully emitted {len(activities_kafka)} activities to queue")
        except Exception as e:
            self.logger.error(f"Failed to emit batch to queue with error: {repr(e)}")
            raise QueueMessageProduceError(
                f"Failed to send {len(activities_kafka)} messages to Kafka"
            ) from e
