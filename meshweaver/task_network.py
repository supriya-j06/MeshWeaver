
import asyncio
import struct
import cloudpickle

MAX_MESSAGE_SIZE = 1024 * 1024


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

        # Only accept trusted local test data.
        task_data = await receive_data(reader)
        task = cloudpickle.loads(task_data)

        function = task["function"]
        args = task["args"]

        result = function(*args)

        response = cloudpickle.dumps(result)
        await send_data(writer, response)

        print("Task executed successfully.")
        print("Result:", result)

    except Exception as error:
        print("Task error:", error)

    finally:
        writer.close()
        await writer.wait_closed()


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
    reader, writer = await asyncio.open_connection(host, port)

    try:
        task = {
            "function": function,
            "args": args
        }

        serialized_task = cloudpickle.dumps(task)

        await send_data(writer, serialized_task)

        response = await receive_data(reader)
        result = cloudpickle.loads(response)

        return result

    finally:
        writer.close()
        await writer.wait_closed()
