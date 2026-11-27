
import asyncio

from meshweaver.task_network import (
    start_task_server,
    submit_task
)


def calculate_sum(a, b):
    return a + b


async def main():

    print("Starting MeshWeaver TCP task server...")

    # Start the server in the background.
    server = await asyncio.start_server(
        handle_peer,
        "127.0.0.1",
        9999
    )

    print("TCP task server running on port 9999")

    try:
        print("\nTEST 1: Successful task transfer")

        result = await submit_task(
            "127.0.0.1",
            9999,
            calculate_sum,
            100,
            200
        )

        print("Result:", result)

        if result == 300:
            print("PASS: Task executed successfully.")
        else:
            print("FAIL: Incorrect task result.")

        print("\nTEST 2: Unavailable peer")

        try:
            await submit_task(
                "127.0.0.1",
                9998,
                calculate_sum,
                10,
                20
            )

            print("FAIL: Unavailable peer was not detected.")

        except ConnectionRefusedError:
            print("PASS: Unavailable peer detected.")

        except OSError as error:
            print("PASS: Connection error detected:", error)

    finally:
        server.close()
        await server.wait_closed()


async def handle_peer(reader, writer):
    from meshweaver.task_network import handle_peer as process_task

    await process_task(reader, writer)


if __name__ == "__main__":
    asyncio.run(main())
