"""
UDP endpoint helper.

Windows quirk: when a datagram is sent to a closed port (our localhost discovery scan does that on
purpose), Windows reports ICMP "port unreachable" as ConnectionResetError on the *next receive*, and
asyncio's Proactor datagram transport then stops receiving for good. So on Windows we use a plain
socket read by a small thread that simply ignores those resets. Set MESHWEAVER_UDP_THREAD=1 to force
this mode on any OS (useful for testing).
"""
from __future__ import annotations

import os
import socket
import sys
import threading


def use_threaded() -> bool:
    return sys.platform == "win32" or os.environ.get("MESHWEAVER_UDP_THREAD") == "1"


class ThreadedUDP:
    """Minimal DatagramTransport look-alike: sendto() / close()."""

    def __init__(self, loop, protocol, sock):
        self.loop, self.protocol, self.sock = loop, protocol, sock
        self._closed = False
        self._thread = threading.Thread(target=self._reader, name="mw-udp", daemon=True)

    def start(self):
        self._thread.start()

    def _reader(self):
        while not self._closed:
            try:
                data, addr = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:                  # e.g. WSAECONNRESET from ICMP -> ignore and keep reading
                if self._closed:
                    break
                continue
            try:
                self.loop.call_soon_threadsafe(self.protocol.datagram_received, data, addr)
            except RuntimeError:             # event loop already closed
                break

    def sendto(self, data, addr):
        self.sock.sendto(data, addr)

    def close(self):
        self._closed = True
        try:
            self.sock.close()
        except OSError:
            pass


async def open_udp(loop, protocol, host, port):
    if not use_threaded():
        transport, _ = await loop.create_datagram_endpoint(
            lambda: protocol, local_addr=(host, port), allow_broadcast=True)
        return transport
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind((host, port))
    sock.settimeout(0.5)
    t = ThreadedUDP(loop, protocol, sock)
    protocol.connection_made(t)
    t.start()
    return t
