
import asyncio
import unittest

from meshweaver.networking import (
    start_udp_server,
    ping_peer
)


class TestUDPCommunication(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.server = await start_udp_server(
            host="127.0.0.1",
            port=8889
        )

    async def asyncTearDown(self):
        self.server.close()

    async def test_ping_pong_communication(self):
        response = await ping_peer(
            "127.0.0.1",
            8889
        )

        self.assertEqual(response, "PONG")


if __name__ == "__main__":
    unittest.main()
