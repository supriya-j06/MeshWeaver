
import asyncio

from meshweaver.task_network import submit_task


def calculate_sum(a, b):
    return a + b


async def main():
    result = await submit_task(
        "127.0.0.1",
        9999,
        calculate_sum,
        100,
        200
    )

    print("\nResult received from remote peer:", result)


if __name__ == "__main__":
    asyncio.run(main())
