import asyncio
import random
import time


async def worker(name, queue):
    while True:
        # Get a work item from the queue
        print(f"Queue size is {queue.qsize()}")
        print("Retrieving message from the queue....")
        message = await queue.get()
        print(f"Queue size is {queue.qsize()}")
        sleep_for = message["sleep_for"]
        content = message["content"]
        sender = message["sender"]
        recipient = message["recipient"]

        # sleep for the sleep_for seconds
        await asyncio.sleep(sleep_for)
        print(f"Message: {content}\nFrom: {sender}\nRecipient: {recipient}\n")
        # notify the queue that the work item has been processed
        queue.task_done()


async def main():
    # create a queue that we'll use to store our workload
    queue = asyncio.Queue()

    # Generate random timings and put them into the queue.
    total_sleep_time = 0
    for index, num in enumerate(range(20,0, -1)):
        sleep_for = random.uniform(0.05, 1.0)
        total_sleep_time += sleep_for
        message = {
            "content": f"Hello user{index}",
            "sleep_for": sleep_for,
            "recipient": f"user{index}",
            "sender": f"user{num}"
        }
        queue.put_nowait(message)

    print(f"Queue has {queue.qsize()} messages.")
    # create 3 worker tasks to process queue concurrently
    tasks = []
    for i in range(3):
        task = asyncio.create_task(worker(f"worker-{i}", queue))
        tasks.append(task)

    # wait till queue is fully processed
    started_at = time.monotonic()
    await queue.join()
    total_slept_for = time.monotonic() - started_at

    # cancel our worker tasks
    for task in tasks:
        task.cancel()

    # wait until all worker tasks are canceled
    await asyncio.gather(*tasks, return_exceptions=True)

    print("====")
    print(f"total expected sleep time: {total_sleep_time:.2f} seconds.")


asyncio.run(main())
