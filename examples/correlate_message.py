from camunda.client.engine_client import EngineClient


def main():
    client = EngineClient()
    resp_json = client.correlate_message("CANCEL_MESSAGE", business_key="b4a6f392-12ab-11eb-80ef-acde48001122")
    print(resp_json)


if __name__ == '__main__':
    main()
