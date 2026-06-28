import pytest

from frame.internal.http.account import AccountApi
from frame.internal.http.mail import MailApi

@pytest.fixture(scope="session")
def account() -> AccountApi:
    return AccountApi()

@pytest.fixture(scope="session")
def mail() -> MailApi:
    return MailApi()