import asyncio
from time import sleep


async def async_generator():
    for i in range(5):
        await asyncio.sleep(1)
        try:
            yield i * i
        except Exception as e:
            print(f"yield error: {e}")
        print(f"yield complete {i * i}")


async def main():
    async for i in async_generator():
        print(i)
        await asyncio.sleep(2)
        if i == 4:
            raise ValueError("error")
        print(f"{i} handled")


loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(main())
finally:
    loop.run_until_complete(
        loop.shutdown_asyncgens()
    )  # see: https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.shutdown_asyncgens
    loop.close()
