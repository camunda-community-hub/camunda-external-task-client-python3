import asyncio


async def start(num):
    while True:
        print("in start: ", num)
        await asyncio.sleep(3)


def main():
    loop = asyncio.get_event_loop()

    loop.create_task(start(1))
    loop.create_task(start(2))
    loop.run_forever()

main()