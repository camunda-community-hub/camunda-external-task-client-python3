import asyncio

from camunda.client.engine_client import EngineClient


async def main():
    client = EngineClient()
    resp_json = await client.start_process("PARALLEL_STEPS_EXAMPLE", {"int_var": 1, "str_var": "hello"})
    print(resp_json)


if __name__ == '__main__':
    asyncio.run(main())
