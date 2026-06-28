import uuid

import pytest

from frame.internal.http.account import AccountApi
from frame.internal.http.mail import MailApi
from frame.internal.kafka.kafka_base import KafkaProducerApi


@pytest.fixture(scope="session")
def account() -> AccountApi:
    return AccountApi()

@pytest.fixture(scope="session")
def mail() -> MailApi:
    return MailApi()

@pytest.fixture(scope="session")
def kafka_producer() -> KafkaProducerApi:
    with KafkaProducerApi() as producer:
        yield producer

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