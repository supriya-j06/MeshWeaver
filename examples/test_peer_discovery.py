
import asyncio

from meshweaver.peer import Peer
from meshweaver.routing_table import RoutingTable
from meshweaver.discovery import (
    start_discovery_server,
    iterative_discovery
)


async def main():

    # Create three independent nodes.
    peer_a = Peer("127.0.0.1", 8888)
    peer_b = Peer("127.0.0.1", 8889)
    peer_c = Peer("127.0.0.1", 8890)

    # Create individual routing tables.
    table_a = RoutingTable(peer_a.node_id)
    table_b = RoutingTable(peer_b.node_id)
    table_c = RoutingTable(peer_c.node_id)

    servers = []

    try:
        print("Starting three MeshWeaver nodes...")

        servers.append(
            await start_discovery_server(peer_a, table_a)
        )

        servers.append(
            await start_discovery_server(peer_b, table_b)
        )

        servers.append(
            await start_discovery_server(peer_c, table_c)
        )

        await asyncio.sleep(0.5)

        print("\nPeer B joining through Peer A...")

        await iterative_discovery(
            peer_b,
            peer_a.host,
            peer_a.port,
            table_b
        )

        print("\nPeer C joining through Peer A...")

        await iterative_discovery(
            peer_c,
            peer_a.host,
            peer_a.port,
            table_c
        )

        print("\n--- FINAL NETWORK RESULTS ---")

        print("Peer A knows:", table_a.get_peer_count())
        print("Peer B knows:", table_b.get_peer_count())
        print("Peer C knows:", table_c.get_peer_count())

        for name, table in [
            ("Peer A", table_a),
            ("Peer B", table_b),
            ("Peer C", table_c)
        ]:
            print(f"\n{name} routing table:")

            for peer in table.get_closest_peers(
                table.local_node_id.to_bytes(20, "big").hex(),
                count=20
            ):
                print(f"  {peer.host}:{peer.port} - {peer.node_id}")

        if (
            table_a.get_peer_count() >= 2
            and table_b.get_peer_count() >= 1
            and table_c.get_peer_count() >= 1
        ):
            print("\nSUCCESS: Multiple peer discovery verified!")

        else:
            print("\nDiscovery test incomplete.")

    finally:
        for server in servers:
            server.close()


if __name__ == "__main__":
    asyncio.run(main())
