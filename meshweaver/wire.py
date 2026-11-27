"""Length-prefixed JSON frames used on the TCP/TLS task channel."""
import json

MAX_FRAME = 64 * 1024 * 1024


def pack_frame(obj) -> bytes:
    raw = json.dumps(obj, separators=(",", ":")).encode()
    return len(raw).to_bytes(4, "big") + raw


async def read_frame(reader):
    n = int.from_bytes(await reader.readexactly(4), "big")
    if n > MAX_FRAME:
        raise ConnectionError("frame too large")
    return json.loads(await reader.readexactly(n))


async def send_frame(writer, obj):
    writer.write(pack_frame(obj))
    await writer.drain()
