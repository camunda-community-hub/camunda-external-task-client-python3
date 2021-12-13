import uuid

from camunda.client.engine_client import EngineClient


def main():
    client = EngineClient(config={"auth_basic": {"username": "demo", "password": "demo"}})
    resp_json = client.start_process(
        process_key="PARALLEL_STEPS_EXAMPLE", variables={"intVar": "1", "strVar": "hello"},
        tenant_id="6172cdf0-7b32-4460-9da0-ded5107aa977", business_key=str(uuid.uuid1()))
    print(resp_json)


if __name__ == '__main__':
    main()
