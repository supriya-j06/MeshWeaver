Week 2 – Implementation Plan

Objective

Implement dynamic peer discovery and resource-sharing capabilities in MeshWeaver.

1. Kademlia DHT Peer Discovery

- Implement a lightweight Kademlia node discovery protocol.
- Allow nodes to join the mesh dynamically.
- Enable nodes to discover other peers without relying on a central database.

2. Gossip Protocol

- Monitor CPU and RAM usage of each node.
- Share resource usage information with neighbouring peers.
- Update resource information every 5 seconds.

Implementation Order

1. Design the peer and routing-table structure.
2. Implement peer discovery.
3. Test dynamic node joining and peer lookup.
4. Implement CPU and RAM monitoring.
5. Implement periodic gossip updates.
6. Test communication between multiple local nodes.

Expected Outcome

MeshWeaver nodes should discover each other dynamically and share their CPU/RAM resource information with neighbouring peers.
