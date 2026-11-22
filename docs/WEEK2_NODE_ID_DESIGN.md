Week 2 – Kademlia Node Identity Design

Objective

Define how each MeshWeaver node will be uniquely identified in the peer-to-peer network.

Node Identity

- Each node will have a unique Node ID.
- Node IDs will be generated using a consistent hashing approach.
- Each node will maintain its own identity when joining the mesh.

Peer Information

Each discovered peer should maintain:

- Node ID
- IP address
- Port number
- Last-seen timestamp

Purpose

Unique node identities will help MeshWeaver distinguish peers, maintain routing information, and support dynamic peer discovery.

Development Status

Design documentation prepared. Implementation and testing are pending.
