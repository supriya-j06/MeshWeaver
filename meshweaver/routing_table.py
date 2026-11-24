class RoutingTable:
    """Kademlia routing table for storing peer information."""

    def __init__(self, local_node_id, bucket_size=20):
        self.local_node_id = int(local_node_id, 16)
        self.bucket_size = bucket_size

        # Kademlia uses 160 distance buckets.
        self.buckets = [[] for _ in range(160)]

    def calculate_distance(self, node_id):
        """Calculate XOR distance between local node and another node."""

        peer_id = int(node_id, 16)
        return self.local_node_id ^ peer_id

    def get_bucket_index(self, node_id):
        """Find the correct bucket using XOR distance."""

        distance = self.calculate_distance(node_id)

        if distance == 0:
            return None

        return distance.bit_length() - 1

    def add_peer(self, peer):
        """Add a peer to the appropriate routing bucket."""

        bucket_index = self.get_bucket_index(peer.node_id)

        if bucket_index is None:
            return False

        bucket = self.buckets[bucket_index]

        # Avoid adding duplicate peers.
        for existing_peer in bucket:
            if existing_peer.node_id == peer.node_id:
                return False

        # Keep the bucket within its maximum size.
        if len(bucket) >= self.bucket_size:
            return False

        bucket.append(peer)
        return True

    def get_closest_peers(self, target_node_id, count=20):
        """Return peers closest to a target node ID."""

        all_peers = []

        for bucket in self.buckets:
            all_peers.extend(bucket)

        all_peers.sort(
            key=lambda peer: int(peer.node_id, 16) ^ int(target_node_id, 16)
        )

        return all_peers[:count]

    def get_peer_count(self):
        """Return the total number of stored peers."""

        return sum(len(bucket) for bucket in self.buckets)
