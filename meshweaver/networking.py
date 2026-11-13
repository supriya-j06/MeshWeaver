
import asyncio


class MeshProtocol(asyncio.DatagramProtocol):
    """Handles incoming UDP messages from other peers."""

    def __init__(self):
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        print("UDP node is ready to receive messages.")

    def datagram_received(self, data, addr):
        message = data.decode("utf-8")

        print(f"Received '{message}' from {addr}")

        if message == "PING":
            self.transport.sendto(b"PONG", addr)
            print(f"Sent PONG to {addr}")

    def error_received(self, exc):
        print(f"UDP error: {exc}")

    def connection_lost(self, exc):
        print("UDP connection closed.")


class PingClientProtocol(asyncio.DatagramProtocol):

    def __init__(self, loop):
        self.loop = loop
        self.response = loop.create_future()

    def datagram_received(self, data, addr):
        message = data.decode("utf-8")

        print(f"Received '{message}' from {addr}")

        if not self.response.done():
            self.response.set_result(message)


async def start_udp_server(host="127.0.0.1", port=8888):
    """Starts an asynchronous UDP server."""

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        MeshProtocol,
        local_addr=(host, port)
    )

    return transport


async def ping_peer(host="127.0.0.1", port=8888):
    """Sends a PING message to another peer."""

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: PingClientProtocol(loop),
        local_addr=("127.0.0.1", 0)
    )

    try:
        transport.sendto(b"PING", (host, port))

        print(f"PING sent to {host}:{port}")

        response = await asyncio.wait_for(
            protocol.response,
            timeout=5
        )

        return response

    finally:
        transport.close()
