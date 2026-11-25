# Intermediate programming practice: async and sockets.
#
# For EACH problem, work out your problem-solving OUTLINE first, then implement it (you
# may use an AI assistant to help - see the README). The async parts must actually run
# their work CONCURRENTLY (that's the whole point), not one after another.
#
# Do not rename the functions - the tests use these names.

import asyncio
import socket


async def gather_doubled(numbers):
    # Outline (write your plan here first):
    #   ...
    #   1. Make an async helper that doubles one number.
    #   2. Use asyncio.gather to run the helpers concurrently.
    #   3. Return their answers as a list in the original order.
    # TODO (Part 1, async): run one async task per number that returns n * 2, all
    #   CONCURRENTLY with asyncio.gather, and return the results as a list in the same
    #   order as `numbers`.

    # An async helper is a small coroutine that handles one number.
    async def double(number):
        # A zero-second sleep lets other ready tasks run before this one continues.
        # The tasks share the event loop; they do not need separate threads.
        await asyncio.sleep(0)
        # Send the doubled value back to whoever is waiting for this helper.
        return number * 2

    # Make one coroutine for every number. Calling double here does not run it yet.
    tasks = [double(number) for number in numbers]
    # The * unpacks the list into separate arguments. gather schedules them all,
    # waits for every answer, and keeps the input order, even if tasks finish out of order.
    # An empty input gives an empty result list.
    results = await asyncio.gather(*tasks)
    return results


async def fastest(jobs):
    # Outline:
    #   ...
    #   1. Make an async helper that waits for a job's delay and returns its label.
    #   2. Start every helper as a task before waiting for an answer.
    #   3. Use asyncio.as_completed to get the first finished job's label.
    #   4. Cancel and clean up the remaining tasks because their answers are not needed.
    # TODO (Part 2, async): `jobs` is a list of (label, delay) pairs. Start them all
    #   concurrently - each one just does `await asyncio.sleep(delay)` - and return the
    #   label of the FIRST job to finish. (Hint: asyncio.as_completed.)

    # This helper represents one job: wait for its delay, then return its name.
    async def run_job(label, delay):
        # Unlike time.sleep, asyncio.sleep allows other async tasks to keep running.
        await asyncio.sleep(delay)
        return label

    # create_task schedules each job on the event loop. All jobs are scheduled
    # before we await anything, so their waiting periods can overlap.
    tasks = [asyncio.create_task(run_job(label, delay)) for label, delay in jobs]

    # There is no winning label when the list contains no jobs.
    if not tasks:
        return None

    # finally runs even when we return early or a job raises an error.
    try:
        # as_completed gives us results in finishing order, rather than input order.
        for completed in asyncio.as_completed(tasks):
            # Wait for the next finished job, then immediately return its label.
            return await completed
    finally:
        # Ask any jobs that are still waiting to stop.
        for task in tasks:
            if not task.done():
                task.cancel()

        # Wait for cancellation to finish so no background jobs are left behind.
        # return_exceptions=True collects cancellation errors instead of raising them.
        await asyncio.gather(*tasks, return_exceptions=True)


def echo_once(host, port, message):
    # Outline:
    #   ...
    #   1. Create a TCP socket and connect to the server's host and port.
    #   2. Encode the message as bytes and send all of it.
    #   3. Receive the reply, decode it into text, and return it.
    #   4. Close the socket automatically when leaving the with block.
    # TODO (Part 3, sockets): open a TCP socket and connect to (host, port). Send
    #   `message` (encode it to bytes first), receive the server's reply, and return it
    #   as a string. (Hint: socket.socket, .connect, .sendall, .recv, .decode.)

    # AF_INET uses IPv4 addresses; SOCK_STREAM creates a TCP socket.
    # The with block closes the socket automatically, including if an error occurs.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        # connect expects one tuple containing the server address and port number.
        client.connect((host, port))
        # Sockets send bytes, so convert the message from text using UTF-8.
        # sendall keeps sending until all message bytes have been sent or an error occurs.
        client.sendall(message.encode("utf-8"))
        # Receive up to 1024 bytes in this simple, single-receive exercise.
        # TCP can split longer replies into chunks; this is not a general message reader.
        reply = client.recv(1024)
        # Convert the received bytes back into text and return that text.
        return reply.decode("utf-8")


def main():
    # Optional scratch space - try the async parts here, e.g.:
    # print(asyncio.run(gather_doubled([1, 2, 3, 4])))   # [2, 4, 6, 8]
    pass


if __name__ == "__main__":
    main()
