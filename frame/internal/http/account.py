import httpx

class AccountApi:
    def __init__(self, base_url: str = "http://185.185.143.231:8085") -> None:
        self.base_url = base_url
        self._client = httpx.Client(base_url=self.base_url)

    def register_user(self, login: str, email: str, password: str):
        data = {
            "login": login,
            "email": email,
            "password": password
        }
        return self._client.post("/register/user/async-register", json=data)

    def activate_user(self, token: str):
        params = {
            "token": token,
        }
        return self._client.put("/register/user/activate", params=params)
