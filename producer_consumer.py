import asyncio
import random


async def produce(queue, n):
    for x in range(1, n+1):
        # producing an item
        print(f"producing {x}/{n}")
        # simulate io operation using sleep
        await asyncio.sleep(random.random())
        item = str(x)
        # put the item in the queue
        await queue.put(item)

    # indicate prodcucer is done
    await queue.put(None)


async def consume(queue):
    while True:
        # wait for an item from the producer
        item = await queue.get()
        if item is None:
            # the producer emit None to indicate it is done.
            break

        # process the item
        print(f"consuming item {item}")
        # simulate io operation using sleep
        await asyncio.sleep(random.random())


loop = asyncio.get_event_loop()

queue = asyncio.Queue(loop=loop)
producer_coro = produce(queue, 10)
consumer_coro = consume(queue)

loop.run_until_complete(asyncio.gather(producer_coro, consumer_coro))
loop.close()
