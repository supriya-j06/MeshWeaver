
import asyncio

from meshweaver.peer import Peer
from meshweaver.routing_table import RoutingTable
from meshweaver.discovery import (
    start_discovery_server,
    discover_peers
)


async def main():

    # Create the existing peer (Peer A).
    peer_a = Peer("127.0.0.1", 8888)

    # Create a new peer (Peer B).
    peer_b = Peer("127.0.0.1", 8889)

    # Create Peer B's routing table.
    routing_table_b = RoutingTable(peer_b.node_id)

    print("Starting Peer A...")

    server = await start_discovery_server(peer_a)

    try:
        print("\nPeer B is joining the network...")

        discovered = await discover_peers(
            peer_a.host,
            peer_a.port,
            routing_table_b
        )

        print("\n--- Discovery Results ---")
        print("Discovered peers:", discovered)
        print("Peers in routing table:", routing_table_b.get_peer_count())

        if discovered == 1:
            print("SUCCESS: Peer B discovered Peer A!")

        else:
            print("Peer discovery test failed.")

    finally:
        server.close()


if __name__ == "__main__":
    asyncio.run(main())
