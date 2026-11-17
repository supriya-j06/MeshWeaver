
import asyncio
import struct
import cloudpickle

MAX_MESSAGE_SIZE = 1024 * 1024

CONNECTION_TIMEOUT = 5
TASK_TIMEOUT = 10


async def receive_data(reader):
    header = await reader.readexactly(4)
    size = struct.unpack("!I", header)[0]

    if size > MAX_MESSAGE_SIZE:
        raise ValueError("Task message is too large")

    return await reader.readexactly(size)


async def send_data(writer, data):
    if len(data) > MAX_MESSAGE_SIZE:
        raise ValueError("Task message is too large")

    writer.write(struct.pack("!I", len(data)) + data)
    await writer.drain()


async def handle_peer(reader, writer):
    try:
        print("Peer connected.")

        task_data = await asyncio.wait_for(
            receive_data(reader),
            timeout=TASK_TIMEOUT
        )

        # Only accept trusted local test data.
        task = cloudpickle.loads(task_data)

        function = task["function"]
        args = task["args"]

        result = function(*args)

        response = cloudpickle.dumps(result)

        await asyncio.wait_for(
            send_data(writer, response),
            timeout=TASK_TIMEOUT
        )

        print("Task executed successfully.")
        print("Result:", result)

    except asyncio.TimeoutError:
        print("Task error: Peer communication timed out.")

    except Exception as error:
        print("Task error:", error)

    finally:
        writer.close()

        try:
            await writer.wait_closed()
        except OSError:
            pass


async def start_task_server():
    server = await asyncio.start_server(
        handle_peer,
        "127.0.0.1",
        9999
    )

    print("MeshWeaver task server running on port 9999")

    async with server:
        await server.serve_forever()


async def submit_task(host, port, function, *args):
    writer = None

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=CONNECTION_TIMEOUT
        )

        task = {
            "function": function,
            "args": args
        }

        serialized_task = cloudpickle.dumps(task)

        await asyncio.wait_for(
            send_data(writer, serialized_task),
            timeout=TASK_TIMEOUT
        )

        response = await asyncio.wait_for(
            receive_data(reader),
            timeout=TASK_TIMEOUT
        )

        result = cloudpickle.loads(response)

        return result

    except asyncio.TimeoutError as error:
        raise TimeoutError(
            "Connection or task communication timed out."
        ) from error

    finally:
        if writer is not None:
            writer.close()

            try:
                await writer.wait_closed()
            except OSError:
                pass
