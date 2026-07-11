import json
import time
import uuid

import pytest

from frame.internal.http.mail import MailApi


@pytest.fixture()
def registration_message() -> dict:
    base = uuid.uuid4().hex
    expected_mail = f"{base}ng@mail.ru"
    message = {
        "login": f"{base}",
        "email": expected_mail,
        "password":"string"
    }
    return message

@pytest.fixture()
def mail_message() -> dict:
    base = uuid.uuid4().hex
    expected_mail = f"{base}ng@mail.ru"
    message = {
        "address": expected_mail,
        "subject": "Publish message",
        "body": "Publish message",
    }
    return message

@pytest.fixture()
def register_events_error_message():
    def _register_events_error_message(base_login_message: dict) -> dict:
        message = {
            "input_data": {
                **base_login_message
            },
            "error_message": {
                "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
                "title": "Validation failed",
                "status": 400,
                "traceId": "00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01",
                "errors": {
                    "Email": [
                        "Invalid"
                    ]
                }
            },
            "error_type": "unknown"
            }
        return message
    return _register_events_error_message

@pytest.fixture()
def get_token_from_message():
    def _get_token_from_message(message) -> str:
        message_activation_str = message.json()['items'][0]['Content']['Body']
        message_activation_body = json.loads(message_activation_str)
        activation_token = message_activation_body["ConfirmationLinkUrl"].split("/")[-1]

        return activation_token

    return _get_token_from_message

@pytest.fixture()
def wait_for_mail(mail: MailApi):
    def _wait_for_mail(email: str, attempts: int = 10, interval: int = 1):
        for _ in range(attempts):
            response = mail.search_mail(query=email, timeout=2)
            if response.json()["total"] > 0:
                return response
            time.sleep(interval)

        raise AssertionError(f"Email not found: {email}")

    return _wait_for_mail
