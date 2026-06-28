import time
import uuid

import pytest
from tomlkit import value

from frame.internal.http.account import AccountApi
from frame.internal.http.mail import MailApi
from frame.internal.kafka.kafka_base import KafkaProducerApi


def test_failed_registration(account: AccountApi, mail: MailApi):
    expectrd_mail = "string@mail.ru"
    account.register_user(login="string", email=expectrd_mail, password="string")

    for _ in range(10):
        responce = mail.search_mail(query=expectrd_mail)
        if responce.json()["total"] > 0:
            raise AssertionError("Email not found")
        time.sleep(1)



def test_success_registration(account: AccountApi, mail: MailApi, registration_message: dict):
    account.register_user(
        **registration_message
    )

    for _ in range(10):
        responce = mail.search_mail(query=registration_message.get("email"))
        if responce.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")

def test_success_registration_with_kafka(kafka_producer: KafkaProducerApi, registration_message: dict, mail: MailApi):

    kafka_producer.send(topic="register-events", value=registration_message)

    for _ in range(10):
        responce = mail.search_mail(query=registration_message.get("email"))
        if responce.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")
