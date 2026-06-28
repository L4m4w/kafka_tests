import time
import uuid

import pytest

from frame.internal.http.account import AccountApi
from frame.internal.http.mail import MailApi


def test_failed_registration(account: AccountApi, mail: MailApi):
    expectrd_mail = "string@mail.ru"
    account.register_user(login="string", email=expectrd_mail, password="string")

    for _ in range(10):
        responce = mail.search_mail(query=expectrd_mail)
        if responce.json()["total"] > 0:
            raise AssertionError("Email not found")
        time.sleep(1)



def test_success_registration(account: AccountApi, mail: MailApi):
    base = uuid.uuid4().hex
    expectrd_mail = f"{base}ng@mail.ru"
    account.register_user(login=f"{base}", email=expectrd_mail, password="string")

    for _ in range(10):
        responce = mail.search_mail(query=expectrd_mail)
        if responce.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")
