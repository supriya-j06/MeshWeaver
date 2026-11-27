
import asyncio
import json
import time

from meshweaver.monitor import get_resource_usage


class GossipProtocol(asyncio.DatagramProtocol):
    """Receives resource information from neighboring peers."""

    def __init__(self, resource_table):
        self.resource_table = resource_table
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        print("Gossip service started.")

    def datagram_received(self, data, addr):
        try:
            message = json.loads(data.decode("utf-8"))

            if message.get("type") != "RESOURCE_UPDATE":
                return

            node_id = message["node_id"]

            self.resource_table[node_id] = {
                "node_id": node_id,
                "host": message["host"],
                "port": int(message["port"]),
                "cpu_percent": float(message["cpu_percent"]),
                "ram_percent": float(message["ram_percent"]),
                "ram_available_mb": float(
                    message["ram_available_mb"]
                ),
                "ram_total_mb": float(message["ram_total_mb"]),
                "timestamp": float(message["timestamp"])
            }

            print(
                f"Received resource update from {node_id[:8]}: "
                f"CPU {message['cpu_percent']}%, "
                f"RAM {message['ram_percent']}%"
            )

        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            KeyError,
            TypeError,
            ValueError
        ):
            print("Invalid gossip message.")


class GossipService:
    """Manages periodic resource sharing with known peers."""

    def __init__(
        self,
        local_peer,
        routing_table,
        transport,
        resource_table,
        interval
    ):
        self.local_peer = local_peer
        self.routing_table = routing_table
        self.transport = transport
        self.resource_table = resource_table
        self.interval = interval
        self.running = True

        self.task = asyncio.create_task(self._gossip_loop())

    async def _gossip_loop(self):
        """Send resource information every configured interval."""

        loop = asyncio.get_running_loop()
        next_update = loop.time()

        try:
            while self.running:

                # Collect CPU and RAM without blocking the event loop.
                usage = await asyncio.to_thread(
                    get_resource_usage
                )

                message = {
                    "type": "RESOURCE_UPDATE",
                    "node_id": self.local_peer.node_id,
                    "host": self.local_peer.host,
                    "port": self.local_peer.port,
                    "cpu_percent": usage["cpu_percent"],
                    "ram_percent": usage["ram_percent"],
                    "ram_available_mb": usage["ram_available_mb"],
                    "ram_total_mb": usage["ram_total_mb"],
                    "timestamp": time.time()
                }

                data = json.dumps(message).encode("utf-8")

                # Get known neighboring peers.
                peers = self.routing_table.get_closest_peers(
                    self.local_peer.node_id,
                    count=20
                )

                # Send resource information to each neighbor.
                for peer in peers:
                    self.transport.sendto(
                        data,
                        (peer.host, peer.port + 1000)
                    )

                print(
                    f"[{time.strftime('%H:%M:%S')}] "
                    f"Shared resource information with "
                    f"{len(peers)} peer(s)."
                )

                # Schedule the next update five seconds later.
                next_update += self.interval

                delay = max(
                    0,
                    next_update - loop.time()
                )

                await asyncio.sleep(delay)

        except asyncio.CancelledError:
            pass

    def close(self):
        """Stop the gossip service."""

        self.running = False
        self.task.cancel()
        self.transport.close()


async def start_gossip_service(
    local_peer,
    routing_table,
    interval=5
):
    """Start a background resource-sharing service."""

    loop = asyncio.get_running_loop()
    resource_table = {}

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: GossipProtocol(resource_table),
        local_addr=(
            local_peer.host,
            local_peer.port + 1000
        )
    )

    service = GossipService(
        local_peer,
        routing_table,
        transport,
        resource_table,
        interval
    )

    return service
