
import asyncio
from meshweaver.task_network import submit_task


def calculate_sum(a, b):
    return a + b


async def main():

    print("TEST 1: Successful task transfer")

    try:
        result = await submit_task(
            "127.0.0.1",
            9999,
            calculate_sum,
            100,
            200
        )

        print("Result:", result)

    except Exception as error:
        print("Task transfer failed:", error)

    print("\nTEST 2: Unavailable peer")

    try:
        await submit_task(
            "127.0.0.1",
            9998,
            calculate_sum,
            10,
            20
        )

    except ConnectionRefusedError:
        print("PASS: Unavailable peer detected.")

    except OSError as error:
        print("Connection error detected:", error)


if __name__ == "__main__":
    asyncio.run(main())
