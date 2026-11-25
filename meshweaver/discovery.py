
import asyncio
import json

from meshweaver.peer import Peer


class PeerDiscoveryProtocol(asyncio.DatagramProtocol):
    """Handles incoming peer discovery requests."""

    def __init__(self, local_peer):
        self.local_peer = local_peer
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        print("Peer discovery service started.")

    def datagram_received(self, data, addr):
        try:
            message = json.loads(data.decode("utf-8"))

            if message.get("type") == "DISCOVER":
                response = {
                    "type": "PEERS",
                    "peers": [self.local_peer.get_info()]
                }

                self.transport.sendto(
                    json.dumps(response).encode("utf-8"),
                    addr
                )

                print(f"Sent peer information to {addr}")

        except (json.JSONDecodeError, UnicodeDecodeError):
            print("Invalid discovery message.")


class DiscoveryClientProtocol(asyncio.DatagramProtocol):
    """Receives peer discovery responses."""

    def __init__(self, routing_table, loop):
        self.routing_table = routing_table
        self.response = loop.create_future()

    def datagram_received(self, data, addr):
        try:
            message = json.loads(data.decode("utf-8"))

            if message.get("type") != "PEERS":
                return

            discovered_count = 0

            for info in message.get("peers", []):
                peer = Peer(
                    info["host"],
                    int(info["port"]),
                    info["node_id"]
                )

                if self.routing_table.add_peer(peer):
                    discovered_count += 1
                    print(f"Discovered peer: {peer.node_id}")

            if not self.response.done():
                self.response.set_result(discovered_count)

        except (json.JSONDecodeError, UnicodeDecodeError,
                KeyError, TypeError, ValueError):
            print("Invalid peer information.")


async def start_discovery_server(local_peer):
    """Start the UDP peer discovery service."""

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: PeerDiscoveryProtocol(local_peer),
        local_addr=(local_peer.host, local_peer.port)
    )

    return transport


async def discover_peers(host, port, routing_table):
    """Request and store peer information from another node."""

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: DiscoveryClientProtocol(routing_table, loop),
        local_addr=("127.0.0.1", 0)
    )

    try:
        request = {"type": "DISCOVER"}

        transport.sendto(
            json.dumps(request).encode("utf-8"),
            (host, port)
        )

        print(f"Discovery request sent to {host}:{port}")

        return await asyncio.wait_for(
            protocol.response,
            timeout=5
        )

    finally:
        transport.close()
