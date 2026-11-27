
import asyncio

from meshweaver.peer import Peer
from meshweaver.routing_table import RoutingTable
from meshweaver.discovery import (
    start_discovery_server,
    iterative_discovery
)
from meshweaver.gossip import start_gossip_service


async def main():

    # Create three nodes.
    peer_a = Peer("127.0.0.1", 8888)
    peer_b = Peer("127.0.0.1", 8889)
    peer_c = Peer("127.0.0.1", 8890)

    # Create routing tables.
    table_a = RoutingTable(peer_a.node_id)
    table_b = RoutingTable(peer_b.node_id)
    table_c = RoutingTable(peer_c.node_id)

    discovery_servers = []
    gossip_services = []

    try:
        print("Starting discovery servers...")

        for peer, table in [
            (peer_a, table_a),
            (peer_b, table_b),
            (peer_c, table_c)
        ]:
            server = await start_discovery_server(peer, table)
            discovery_servers.append(server)

        # Discover the other nodes.
        print("\nPerforming peer discovery...")

        await iterative_discovery(
            peer_b, peer_a.host, peer_a.port, table_b
        )

        await iterative_discovery(
            peer_c, peer_a.host, peer_a.port, table_c
        )

        await iterative_discovery(
            peer_a, peer_b.host, peer_b.port, table_a
        )

        print("\nStarting gossip services...")

        for peer, table in [
            (peer_a, table_a),
            (peer_b, table_b),
            (peer_c, table_c)
        ]:
            service = await start_gossip_service(
                peer,
                table,
                interval=5
            )
            gossip_services.append(service)

        # Allow at least one resource exchange to complete.
        print("\nWaiting for gossip messages...")
        await asyncio.sleep(12)

        print("\n--- GOSSIP RESULTS ---")

        all_tables = [
            ("Peer A", gossip_services[0].resource_table),
            ("Peer B", gossip_services[1].resource_table),
            ("Peer C", gossip_services[2].resource_table)
        ]

        for name, resources in all_tables:
            print(f"\n{name} received {len(resources)} peer update(s)")

            for node_id, usage in resources.items():
                print(
                    f"Node: {node_id[:8]} | "
                    f"CPU: {usage['cpu_percent']}% | "
                    f"RAM: {usage['ram_percent']}%"
                )

        if all(
            len(resources) >= 2
            for _, resources in all_tables
        ):
            print("\nSUCCESS: Three-node resource gossip verified!")

        else:
            print("\nGossip test incomplete.")

    finally:
        for service in gossip_services:
            service.close()

        for server in discovery_servers:
            server.close()

        await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main())
