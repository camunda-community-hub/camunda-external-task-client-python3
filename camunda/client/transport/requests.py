
import requests

from camunda.client.transport.serializers import RequestsJSONSerializer
from camunda.utils.response_utils import raise_exception_if_not_ok


class RequestsTransport:

    def __init__(self, **kwargs):
        # leave this default header for backwards compatibility
        self.default_headers = kwargs.pop("default_headers", {"Content-Type": "application/json"})
        self.default_params = kwargs.pop("default_params", None)
        self.default_auth = kwargs.pop("default_auth", None)
        self.default_flags = kwargs.pop("default_flags", {})
        self.default_serializer_class = kwargs.pop("default_serializer_class", RequestsJSONSerializer)

    def request(self, method, *args, serializer_class=None, **kwargs):

        # merge defaults flags with default values and update
        # thas with the given parameters. tuple iteration
        # to avoid even adding defaults to function call.
        kwargs = {
            **self.default_flags,
            **{k: v for k, v in (
                ("headers", self.default_headers),
                ("params", self.default_params),
                ("auth", self.default_auth),
                ) if v is not None},
            **kwargs
        }

        response = requests.request(method, *args, **kwargs)
        raise_exception_if_not_ok(response)

        serializer_class = serializer_class or self.default_serializer_class
        return serializer_class().deserialize(response)

    def get(self, *args, **kwargs):
        return self.request("GET", *args, **kwargs)

    def post(self, *args, **kwargs):
        return self.request("POST", *args, **kwargs)

    def put(self, *args, **kwargs):
        return self.request("PUT", *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.request("DELETE", *args, **kwargs)
