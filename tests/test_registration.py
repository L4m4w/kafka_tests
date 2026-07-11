import time
import uuid

from frame.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from frame.helpers.kafka.consumers.register_events_errors import RegisterEventsErrorsSubscriber
from frame.internal.http.account import AccountApi
from frame.internal.http.mail import MailApi
from frame.internal.kafka.producer import KafkaProducerApi
from frame.helpers.consts.messages.kafka_error_message import KafkaRegisterEventsErrorsMessage
from frame.internal.rmq.publisher import RMQPublisher


def test_failed_registration(account: AccountApi, mail: MailApi):
    expected_mail = "string@mail.ru"
    account.register_user(login="string", email=expected_mail, password="string")

    for _ in range(10):
        response = mail.search_mail(query=expected_mail)
        if response.json()["total"] > 0:
            raise AssertionError("Email was found")
        time.sleep(1)


def test_success_registration(
        register_events_subscriber: RegisterEventsSubscriber,
        account: AccountApi,
        mail: MailApi,
        registration_message: dict,
        wait_for_mail
):
    login = registration_message["login"]
    account.register_user(
        **registration_message
    )
    register_events_subscriber.find_message(login = login)

    mail_response = wait_for_mail(login)
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

def test_failed_registration_with_kafka(
        register_events_subscriber: RegisterEventsSubscriber,
        register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
        account: AccountApi,
):
    expected_mail = "string@mail.ru"
    account.register_user(login="string", email=expected_mail, password="string")

    register_events_subscriber.find_message(login="string")
    register_events_errors_subscriber.find_message_by_error_type(
        login="string",
        error_type="validation"
    )
def test_push_and_read_kafka_message(
        kafka_producer: KafkaProducerApi,
        register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
):
    kafka_producer.send(
        topic="register-events-errors",
        value=KafkaRegisterEventsErrorsMessage.REGISTER_EVENTS_ERRORS_UNKNOWN_MESSAGE
    )
    register_events_errors_subscriber.find_message_by_error_type(
        login="string",
        error_type="validation"
    )

def test_rmq(rmq_publisher: RMQPublisher):
    address = f"{uuid.uuid4().hex}@mail.com"
    message = {
        "address": address,
        "subject": "Publish message",
        "body": "Publish message",
    }
    rmq_publisher.publish("dm.mail.sending", message)