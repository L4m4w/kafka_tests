import pytest

from frame.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from frame.helpers.kafka.consumers.register_events_errors import RegisterEventsErrorsSubscriber
from frame.helpers.rmq.consumers.dm_mail_sending import DmMailSending
from frame.internal.http.account import AccountApi
from frame.internal.http.mail import MailApi
from frame.internal.kafka.consumer import KafkaConsumerApi
from frame.internal.kafka.producer import KafkaProducerApi
from frame.internal.rmq.publisher import RMQPublisher

pytest_plugins = [
    "tests.fixtures.data_fixtures"
]

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

@pytest.fixture(scope="session")
def register_events_subscriber() -> RegisterEventsSubscriber:
        return RegisterEventsSubscriber()

@pytest.fixture(scope="session")
def register_events_errors_subscriber() -> RegisterEventsErrorsSubscriber:
        return RegisterEventsErrorsSubscriber()

@pytest.fixture(scope="session", autouse=True)
def kafka_consumer(
        register_events_subscriber: RegisterEventsSubscriber,
        register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
) -> KafkaConsumerApi:
    with (KafkaConsumerApi(subscribers=[register_events_subscriber, register_events_errors_subscriber]) as consumer):
        yield consumer

@pytest.fixture(scope="session", autouse=True)
def rmq_dm_mail_sending_consumer() -> DmMailSending:
    with DmMailSending() as consumer:
        yield consumer

@pytest.fixture(scope="session")
def rmq_publisher() -> RMQPublisher:
    with RMQPublisher() as publisher:
        yield publisher
