import json
import time

from frame.internal.rmq.consumer import RMQConsumerApi


class DmMailSending(RMQConsumerApi):
    exchange = "dm.mail.sending"
    routing_key = "#"

    def find_message(self, login: str, timeout: float = 10.0) -> None:
        start_time = time.time()

        while time.time() - start_time < timeout:
            message = self.get_message(timeout=timeout)
            if json.loads(message["body"])["Login"] == login:
                break
        else:
            raise AssertionError(f"Message from rmq: {self.exchange} not found")