import time

from frame.internal.kafka.subscriber import Subscriber


class RegisterEventsErrorsSubscriber(Subscriber):
    topic: str = "register-events-errors"

    def find_message(
            self,
            login: str,
            timeout: float = 10.0
    ):
        start_time = time.time()

        while time.time() - start_time < timeout:
            message = self.get_message(timeout=timeout)
            if message.value["input_data"]["login"] == login:
                break
        else:
            raise AssertionError(f"Message from topic: {self.topic} not found")

    def find_message_by_error_type(
            self,
            login: str,
            error_type: str,
            timeout: float = 10.0,
    ):
        start_time = time.time()

        while time.time() - start_time < timeout:
            message = self.get_message(timeout=timeout)
            if message.value["input_data"]["login"] == login and message.value["error_type"] == error_type:
                break

        else:
            raise AssertionError(
                f"Message from topic: {self.topic} with login={login} and error_type={error_type} not found"
            )