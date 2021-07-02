
import json
from http import HTTPStatus
from unittest import TestCase
from unittest.mock import patch

import requests
import responses
from camunda.client.transport.requests import RequestsTransport
from camunda.client.transport.serializers import (RequestsBooleanSerializer,
                                                  RequestsRawSerializer)


class RequestsTransportTestCase(TestCase):

    @responses.activate
    def test_get_json(self):
        URL = "http://test-url.com/"
        BODY = {"var1": "value1"}
        responses.add(responses.GET, URL, body=json.dumps(BODY))
        transport = RequestsTransport()
        resp = transport.get(URL)
        self.assertEqual(resp, BODY)

    @responses.activate
    def test_post_json(self):
        URL = "http://test-url.com/"
        BODY = {"var1": "value1"}
        responses.add(responses.POST, URL, body=json.dumps(BODY))
        transport = RequestsTransport()
        resp = transport.post(URL)
        self.assertEqual(resp, BODY)

    @responses.activate
    def test_put_json(self):
        URL = "http://test-url.com/"
        BODY = {"var1": "value1"}
        responses.add(responses.PUT, URL, body=json.dumps(BODY))
        transport = RequestsTransport()
        resp = transport.put(URL)
        self.assertEqual(resp, BODY)

    @responses.activate
    def test_delete_json(self):
        URL = "http://test-url.com/"
        BODY = {"var1": "value1"}
        responses.add(responses.DELETE, URL, body=json.dumps(BODY))
        transport = RequestsTransport()
        resp = transport.delete(URL)
        self.assertEqual(resp, BODY)

    @responses.activate
    def test_get_bool(self):
        URL = "http://test-url.com/"
        responses.add(responses.GET, URL, status=HTTPStatus.NO_CONTENT)
        transport = RequestsTransport()
        resp = transport.get(URL, serializer_class=RequestsBooleanSerializer)
        self.assertTrue(resp)

    @responses.activate
    def test_post_bool(self):
        URL = "http://test-url.com/"
        responses.add(responses.GET, URL, status=HTTPStatus.NO_CONTENT)
        transport = RequestsTransport()
        resp = transport.get(URL, serializer_class=RequestsBooleanSerializer)
        self.assertTrue(resp)

    @responses.activate
    def test_put_bool(self):
        URL = "http://test-url.com/"
        responses.add(responses.PUT, URL, status=HTTPStatus.NO_CONTENT)
        transport = RequestsTransport()
        resp = transport.put(URL, serializer_class=RequestsBooleanSerializer)
        self.assertTrue(resp)

    @responses.activate
    def test_delete_bool(self):
        URL = "http://test-url.de/"
        responses.add(responses.DELETE, URL, status=HTTPStatus.NO_CONTENT)
        transport = RequestsTransport()
        resp = transport.delete(URL, serializer_class=RequestsBooleanSerializer)
        self.assertTrue(resp)

    @responses.activate
    def test_get_bool(self):
        URL = "http://test-url.de/"
        responses.add(responses.GET, URL, status=HTTPStatus.NO_CONTENT)
        transport = RequestsTransport()
        resp = transport.get(URL, serializer_class=RequestsBooleanSerializer)
        self.assertTrue(resp)

    @responses.activate
    def test_post_bool(self):
        URL = "http://test-url.de/"
        BODY = b"content"
        responses.add(responses.GET, URL, body=BODY)
        transport = RequestsTransport()
        resp = transport.get(URL, serializer_class=RequestsRawSerializer)
        self.assertEqual(resp, BODY)

    @responses.activate
    def test_put_bool(self):
        URL = "http://test-url.com/"
        BODY = b"content"
        responses.add(responses.PUT, URL, body=BODY)
        transport = RequestsTransport()
        resp = transport.put(URL, serializer_class=RequestsRawSerializer)
        self.assertEqual(resp, BODY)

    @responses.activate
    def test_delete_bool(self):
        URL = "http://test-url.com/"
        BODY = b"content"
        responses.add(responses.DELETE, URL, body=BODY)
        transport = RequestsTransport()
        resp = transport.delete(URL, serializer_class=RequestsRawSerializer)
        self.assertEqual(resp, BODY)

    @patch("requests.request")
    def test_request_defaults(self, mock_request):
        URL = "http://test-url.com/"
        DEFAULT_PARAMS = {"param": "value"}
        DEFAULT_HEADERS = {"header": "value"}
        DEFAULT_AUTH = ("user", "password")

        class ResponseSub:
            ok = True
            def json(self):
                return {}

        mock_request.return_value = ResponseSub()
        transport = RequestsTransport(
            default_params=DEFAULT_PARAMS,
            default_headers=DEFAULT_HEADERS,
            default_auth=DEFAULT_AUTH,
        )
        transport.get(URL)
        mock_request.assert_called_with(
            responses.GET, URL,
            headers=DEFAULT_HEADERS,
            params=DEFAULT_PARAMS,
            auth=DEFAULT_AUTH
        )
