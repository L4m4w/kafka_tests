import pika
import uuid
import json
from pika.adapters.blocking_connection import BlockingChannel

from frame.internal.singleton import Singleton


class RMQPublisher(Singleton):
    def __init__(self, url: str = "amqp://guest:guest@185.185.143.231:5672"):
        self._url = url
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    def _start(self):
        self._connection = pika.BlockingConnection(
            pika.URLParameters(self._url)
        )
        self._channel = self._connection.channel()

    def _stop(self):
        if self._channel is not None:
            self._channel.close()

        if self._connection is not None:
            self._connection.close()

    def __enter__(self) -> "RMQPublisher":
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop()

    def publish(
            self,
            exchange: str,
            message: dict,
            routing_key: str = "",
            properties: pika.BasicProperties | None = None
    ):
        if properties is None:
            properties = pika.BasicProperties(
                content_type="application/json",
                correlation_id=str(uuid.uuid4()),
            )
        message = json.dumps(message).encode("utf-8")
        self._channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=properties
        )
