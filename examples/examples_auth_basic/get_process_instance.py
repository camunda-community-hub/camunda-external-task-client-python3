from camunda.client.engine_client import EngineClient


def main():
    client = EngineClient(config={"auth_basic": {"username": "demo", "password": "demo"}})
    resp_json = client.get_process_instance("PARALLEL_STEPS_EXAMPLE", ["intVar_eq_1", "strVar_eq_hello"],
                                            ["6172cdf0-7b32-4460-9da0-ded5107aa977"])
    print(resp_json)


if __name__ == '__main__':
    main()
