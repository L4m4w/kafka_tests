import time

from frame.internal.kafka.subscriber import Subscriber


class RegisterEventsSubscriber(Subscriber):
    topic: str = "register-events"

    def find_message(self, login: str, timeout: float = 10.0) -> None:
        start_time = time.time()

        while time.time() - start_time < timeout:
            message = self.get_message(timeout=timeout)
            if message.value["login"] == login:
                break
        else:
            raise AssertionError(f"Message from topic: {self.topic} not found")