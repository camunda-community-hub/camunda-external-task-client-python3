from http import HTTPStatus
from unittest import TestCase

import requests

from camunda.utils.response_utils import raise_exception_if_not_ok, get_response_error_message


class TestRaiseExceptionIfResponseNotOk(TestCase):

    def test_does_not_raise_exception_if_response_is_ok(self):
        try:
            raise_exception_if_not_ok(self.__mock_response(HTTPStatus.OK, {}))
        except Exception:
            self.fail("raise_exception_if_not_ok() should not raise Exception when response is ok")

    def test_raise_exception_if_response_is_not_ok(self):
        data = {'type': "SomeExceptionClass", "message": "a detailed message"}
        with self.assertRaises(Exception) as context:
            raise_exception_if_not_ok(self.__mock_response(HTTPStatus.BAD_REQUEST, data))

        self.assertEqual("received 400 : SomeExceptionClass : a detailed message", str(context.exception))

    def __mock_response(self, status_code, data):
        response = requests.Response()
        response.status_code = status_code
        response.json = lambda: data
        return response

    def test_get_response_error_message_no_error_type_no_message(self):
        data = {}
        error_msg = get_response_error_message(HTTPStatus.BAD_REQUEST, data)
        self.assertEqual("received 400", error_msg)

    def test_get_response_error_message_only_type_no_msg(self):
        data = {'type': "InvalidRequestType", "message": ""}
        error_msg = get_response_error_message(HTTPStatus.BAD_REQUEST, data)
        self.assertEqual("received 400 : InvalidRequestType", error_msg)

    def test_get_response_error_message_only_msg_no_type(self):
        data = {"message": "a detailed message"}
        error_msg = get_response_error_message(HTTPStatus.BAD_REQUEST, data)
        self.assertEqual("received 400 : a detailed message", error_msg)
