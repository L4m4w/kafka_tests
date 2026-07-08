
class KafkaRegisterEventsErrorsMessage:
    REGISTER_EVENTS_ERRORS_UNKNOWN_MESSAGE = {
        "input_data": {
            "login": "string",
            "email": "string@mail.ru",
            "password": "string"
        },
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-d957971fc5a90855accb02b59e879f3c-fa1d384d99ca048d-01",
            "errors": {
                "Email": [
                    "Taken"
                ]
            }
        },
        "error_type": "unknown"
}