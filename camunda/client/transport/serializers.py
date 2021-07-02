
from http import HTTPStatus


class RequestsJSONSerializer:

    def deserialize(self, response):
        return response.json()


class RequestsBooleanSerializer:

    def deserialize(self, response):
        return response.status_code == HTTPStatus.NO_CONTENT


class RequestsRawSerializer:

    def deserialize(self, response):
        return response.content
