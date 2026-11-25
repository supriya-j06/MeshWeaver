
import hashlib
import uuid


def generate_node_id():
    """Generate a unique ID for a MeshWeaver node."""

    unique_value = str(uuid.uuid4())

    node_id = hashlib.sha1(
        unique_value.encode("utf-8")
    ).hexdigest()

    return node_id


class Peer:
    """Stores information about a MeshWeaver peer."""

    def __init__(self, host, port, node_id=None):
        self.node_id = node_id or generate_node_id()
        self.host = host
        self.port = port

    def get_info(self):
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port
        }
