import asyncio

from camunda.client.engine_client import EngineClient


async def main():
    client = EngineClient()
    resp_json = await client.get_process_instance("PARALLEL_STEPS_EXAMPLE", ["intVar_eq_2", "strVar_eq_world"],
                                                  ["6172cdf0-7b32-4460-9da0-ded5107aa977"])
    print(resp_json)


if __name__ == '__main__':
    asyncio.run(main())
