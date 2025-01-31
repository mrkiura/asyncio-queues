import asyncio
import random


async def produce(queue, n):
    for x in range(n):
        print(f"producing {x}/{n}")
        # simulate io operation using sleep
        await asyncio.sleep(random.random())
        item = str(x)
        # put the item in the queue
        await queue.put(item)


async def consume(queue):
    while True:
        # wait for an item from the producer
        item = await queue.get()

        # process the item
        print(f"consuming item {item}....")
        # simulate io operation
        await asyncio.sleep(random.random())

        # notify queue item is processed
        queue.task_done()


async def run(n):
    queue = asyncio.Queue()
    # schedule the consumer
    consumer = asyncio.ensure_future(consume(queue))
    # run the producer and wait for completion
    await produce(queue, n)
    # wait until the consumer has processed all items
    await queue.join()

    consumer.cancel()


loop = asyncio.get_event_loop()
loop.run_until_complete(run(10))
loop.close()
