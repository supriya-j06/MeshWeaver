
import asyncio
import json

from meshweaver.peer import Peer


class PeerDiscoveryProtocol(asyncio.DatagramProtocol):
    """Handles incoming peer discovery requests."""

    def __init__(self, local_peer, routing_table=None):
        self.local_peer = local_peer
        self.routing_table = routing_table
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        print(
            f"Discovery service started: "
            f"{self.local_peer.host}:{self.local_peer.port}"
        )

    def datagram_received(self, data, addr):
        try:
            message = json.loads(data.decode("utf-8"))

            if message.get("type") != "DISCOVER":
                return

            # Register the node requesting discovery.
            requester = message.get("requester")

            if requester and self.routing_table:
                peer = Peer(
                    requester["host"],
                    int(requester["port"]),
                    requester["node_id"]
                )

                if peer.node_id != self.local_peer.node_id:
                    self.routing_table.add_peer(peer)

            # Share our identity and all known peers.
            peers = [self.local_peer.get_info()]

            if self.routing_table:
                for bucket in self.routing_table.buckets:
                    for peer in bucket:
                        if peer.node_id != self.local_peer.node_id:
                            peers.append(peer.get_info())

            # Remove duplicate node IDs.
            unique_peers = {
                peer["node_id"]: peer for peer in peers
            }

            response = {
                "type": "PEERS",
                "peers": list(unique_peers.values())
            }

            self.transport.sendto(
                json.dumps(response).encode("utf-8"),
                addr
            )

            print(f"Shared {len(unique_peers)} peer(s) with {addr}")

        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            KeyError,
            TypeError,
            ValueError
        ):
            print("Invalid discovery message.")


class DiscoveryClientProtocol(asyncio.DatagramProtocol):
    """Receives peer discovery responses."""

    def __init__(self, routing_table, loop, local_node_id=None):
        self.routing_table = routing_table
        self.local_node_id = local_node_id
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

                # Never add this node to its own routing table.
                if peer.node_id == self.local_node_id:
                    continue

                if self.routing_table.add_peer(peer):
                    discovered_count += 1
                    print(f"Discovered peer: {peer.node_id}")

            if not self.response.done():
                self.response.set_result(discovered_count)

        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            KeyError,
            TypeError,
            ValueError
        ):
            print("Invalid peer information.")


async def start_discovery_server(local_peer, routing_table=None):
    """Start the UDP peer discovery service."""

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: PeerDiscoveryProtocol(local_peer, routing_table),
        local_addr=(local_peer.host, local_peer.port)
    )

    return transport


async def discover_peers(
    host,
    port,
    routing_table,
    local_peer=None
):
    """Request peer information from another node."""

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: DiscoveryClientProtocol(
            routing_table,
            loop,
            local_peer.node_id if local_peer else None
        ),
        local_addr=("127.0.0.1", 0)
    )

    try:
        request = {"type": "DISCOVER"}

        if local_peer:
            request["requester"] = local_peer.get_info()

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


async def iterative_discovery(
    local_peer,
    bootstrap_host,
    bootstrap_port,
    routing_table,
    max_queries=20
):
    """
    Discover peers iteratively through known peers.

    Each newly discovered peer can provide information
    about additional peers.
    """

    queue = [(bootstrap_host, bootstrap_port)]
    queried = set()
    total_discovered = 0
    queries = 0

    while queue and queries < max_queries:
        host, port = queue.pop(0)
        endpoint = (host, port)

        if endpoint in queried:
            continue

        queried.add(endpoint)
        queries += 1

        try:
            discovered = await discover_peers(
                host,
                port,
                routing_table,
                local_peer
            )

            total_discovered += discovered

            # Query newly known peers in the next iterations.
            closest = routing_table.get_closest_peers(
                local_peer.node_id,
                count=20
            )

            for peer in closest:
                peer_endpoint = (peer.host, peer.port)

                if peer_endpoint not in queried:
                    queue.append(peer_endpoint)

        except asyncio.TimeoutError:
            print(f"Peer did not respond: {host}:{port}")

    print(f"Discovery queries completed: {queries}")
    print(f"Total newly discovered peers: {total_discovered}")

    return total_discovered
