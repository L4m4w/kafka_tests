import time

from frame.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from frame.internal.http.account import AccountApi
from frame.internal.http.mail import MailApi
from frame.internal.kafka.producer import KafkaProducerApi


def test_failed_registration(account: AccountApi, mail: MailApi):
    expectrd_mail = "string@mail.ru"
    account.register_user(login="string", email=expectrd_mail, password="string")

    for _ in range(10):
        responce = mail.search_mail(query=expectrd_mail)
        if responce.json()["total"] > 0:
            raise AssertionError("Email was found")
        time.sleep(1)


def test_success_registration(account: AccountApi, mail: MailApi, registration_message: dict, wait_for_mail):
    account.register_user(
        **registration_message
    )

    mail_response = wait_for_mail(registration_message["email"])
    assert mail_response.json()["total"] == 1

def test_success_registration_with_kafka(
        kafka_producer: KafkaProducerApi,
        registration_message: dict,
        mail: MailApi,
        wait_for_mail
):
    kafka_producer.send(topic="register-events", value=registration_message)

    mail_response = wait_for_mail(registration_message["email"])
    assert mail_response.json()["total"] == 1


def test_register_events_error_consumer(
        kafka_producer: KafkaProducerApi,
        registration_message: dict,
        register_events_error_message,
        mail: MailApi,
        get_token_from_message,
        account: AccountApi,
        wait_for_mail,
):
    kafka_producer.send(
        topic="register-events-errors",
        value=register_events_error_message(registration_message)
    )

    mail_response = wait_for_mail(registration_message["email"])
    activation_token = get_token_from_message(mail_response)

    activation_response = account.activate_user(activation_token)
    assert activation_response.status_code == 200

def test_success_registration_with_kafka_consumer(
        register_events_subscriber: RegisterEventsSubscriber,
        kafka_producer: KafkaProducerApi,
        registration_message: dict,
        mail: MailApi,
        wait_for_mail
):
    kafka_producer.send(topic="register-events", value=registration_message)

    register_events_subscriber.find_message(login=registration_message["login"])
