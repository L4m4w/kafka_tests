import json
import threading
from types import TracebackType

from kafka import KafkaProducer

from frame.internal.singleton import Singleton


class KafkaProducerApi(Singleton):
    def __init__(self, base_url: list[str] = ["185.185.143.231:9092"]) -> None:
        self.base_url = base_url
        self.producer: KafkaProducer | None = None
        self.lock: threading.Lock = threading.Lock()

    def start(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=self.base_url,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            acks="all",
            retries=5,
            retry_backoff_ms=5000,
            request_timeout_ms=5000,
            connections_max_idle_ms=65000,
            reconnect_backoff_ms=5000,
            reconnect_backoff_max_ms=10000,
        )

    def stop(self) -> None:
        if self.producer:
            self.producer.close()
            self.producer = None

    def send(self, topic: str, value: dict) -> None:
        if not self.producer:
            raise RuntimeError("Producer is not started")

        try:
            with self.lock:
                future = self.producer.send(topic, value)
                record_metadata = future.get(timeout=10)
                return record_metadata

        except Exception as e:
            raise RuntimeError(f"Failed to send message to kafka: {e}")

    def __enter__(self) -> "KafkaProducerApi":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()

