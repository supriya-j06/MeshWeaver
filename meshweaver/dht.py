"""
Kademlia-style DHT over UDP.
  * 160-bit node ids, XOR distance, k-buckets (K=8)
  * RPCs: ping, find_node, find_value, store (+ one-way gossip)
  * iterative lookup with parallelism ALPHA=3
Every datagram is HMAC-authenticated (see security.mac_pack).
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import time

from . import security

log = logging.getLogger("meshweaver.dht")

K = 8
ALPHA = 3
ID_BITS = 160
MAX_VALUE = 8192


def new_node_id() -> int:
    return int.from_bytes(os.urandom(20), "big")


def hex_id(i: int) -> str:
    return f"{i:040x}"


def key_to_id(key: bytes) -> int:
    return int.from_bytes(hashlib.sha1(key).digest(), "big")


class Contact:
    __slots__ = ("id", "host", "port", "fails", "last_seen")

    def __init__(self, id, host, port):
        self.id, self.host, self.port = id, host, port
        self.fails = 0
        self.last_seen = time.time()

    @property
    def addr(self):
        return (self.host, self.port)

    @property
    def hex(self):
        return hex_id(self.id)

    def to_wire(self):
        return {"id": self.hex, "host": self.host, "port": self.port}

    @classmethod
    def from_wire(cls, d):
        return cls(int(d["id"], 16), str(d["host"]), int(d["port"]))


class RoutingTable:
    def __init__(self, own_id: int, k: int = K):
        self.own_id, self.k = own_id, k
        self.buckets = [[] for _ in range(ID_BITS)]
        self.cache = [[] for _ in range(ID_BITS)]   # replacement candidates

    def _idx(self, nid: int) -> int:
        return (self.own_id ^ nid).bit_length() - 1

    def get(self, nid: int):
        if nid == self.own_id:
            return None
        for c in self.buckets[self._idx(nid)]:
            if c.id == nid:
                return c
        return None

    def add(self, c: Contact):
        if c.id == self.own_id:
            return
        i = self._idx(c.id)
        b = self.buckets[i]
        for j, x in enumerate(b):
            if x.id == c.id:                      # seen again -> move to tail (most recent)
                b.pop(j)
                x.host, x.port, x.fails, x.last_seen = c.host, c.port, 0, time.time()
                b.append(x)
                return
        if len(b) < self.k:
            b.append(c)
            return
        rc = self.cache[i]
        rc[:] = [x for x in rc if x.id != c.id]
        rc.append(c)
        del rc[:-self.k]

    def remove(self, nid: int):
        if nid == self.own_id:
            return
        i = self._idx(nid)
        b = self.buckets[i]
        for j, x in enumerate(b):
            if x.id == nid:
                b.pop(j)
                if self.cache[i]:
                    b.append(self.cache[i].pop())
                return

    def all(self):
        return [c for b in self.buckets for c in b]

    def closest(self, target: int, n: int = K, exclude=()):
        cs = [c for c in self.all() if c.id not in exclude]
        cs.sort(key=lambda c: c.id ^ target)
        return cs[:n]

    def __len__(self):
        return sum(len(b) for b in self.buckets)


class DHT(asyncio.DatagramProtocol):
    def __init__(self, node_id: int, port: int, secret: bytes, gossip_handler=None):
        self.node_id, self.port, self.secret = node_id, port, secret
        self.table = RoutingTable(node_id)
        self.storage: dict[str, tuple[str, float, float]] = {}   # key -> (value, expires, version)
        self.pending: dict[str, asyncio.Future] = {}
        self.transport = None
        self.gossip_handler = gossip_handler

    # ------------------------------------------------------------ transport
    def connection_made(self, transport):
        self.transport = transport

    def error_received(self, exc):      # ICMP "port unreachable" while scanning -- harmless
        pass

    def _send(self, obj, addr):
        if self.transport is None:
            return
        try:
            self.transport.sendto(security.mac_pack(self.secret, obj), addr)
        except (OSError, ValueError):
            pass

    def _envelope(self, t, d, rid, is_resp):
        return {"t": t, "rid": rid, "r": is_resp, "id": hex_id(self.node_id), "port": self.port, "d": d or {}}

    def datagram_received(self, data, addr):
        try:
            msg = security.mac_unpack(self.secret, data)
            sender = int(msg["id"], 16)
            port = int(msg["port"])
        except (security.SecurityError, KeyError, ValueError, TypeError):
            return                                   # not from our mesh -> drop silently
        if sender == self.node_id:
            return
        contact = Contact(sender, addr[0], port)
        self.table.add(contact)
        if msg.get("r"):
            fut = self.pending.get(msg.get("rid"))
            if fut and not fut.done():
                fut.set_result(msg.get("d", {}))
            return
        try:
            self._handle_request(msg, contact, addr)
        except Exception:
            log.exception("bad request from %s", addr)

    # ------------------------------------------------------------ server side
    def _handle_request(self, msg, contact, addr):
        t, d, rid = msg.get("t"), msg.get("d", {}), msg.get("rid")

        def reply(data):
            self._send(self._envelope(t, data, rid, True), addr)

        if t == "ping":
            reply({})
        elif t == "find_node":
            target = int(d["target"], 16)
            reply({"nodes": [c.to_wire() for c in self.table.closest(target, K, exclude={contact.id})]})
        elif t == "find_value":
            v = self._local_get(d["key"])
            if v is not None:
                reply({"value": v})
            else:
                target = int(d["key"], 16)
                reply({"nodes": [c.to_wire() for c in self.table.closest(target, K, exclude={contact.id})]})
        elif t == "store":
            value = d.get("value", "")
            if isinstance(value, str) and len(value) <= MAX_VALUE:
                self._local_put(d["key"], value, float(d.get("ttl", 3600)), float(d.get("ver", time.time())))
                reply({"ok": True})
            else:
                reply({"ok": False})
        elif t == "gossip":
            if self.gossip_handler:
                self.gossip_handler(contact, d)

    # ------------------------------------------------------------ local storage
    def _local_put(self, key_hex, value, ttl, ver):
        cur = self.storage.get(key_hex)
        if cur and cur[2] > ver and cur[1] > time.time():
            return                                  # we already hold a newer version -> newest wins
        self.storage[key_hex] = (value, time.time() + ttl, ver)

    def _local_get(self, key_hex):
        item = self.storage.get(key_hex)
        if item and item[1] > time.time():
            return item[0]
        return None

    def expire(self):
        now = time.time()
        for k in [k for k, (_, exp, _v) in self.storage.items() if exp < now]:
            del self.storage[k]

    # ------------------------------------------------------------ client side
    def send_oneway(self, addr, t, d):
        self._send(self._envelope(t, d, "", False), addr)

    async def rpc(self, addr, t, d=None, timeout=1.5):
        rid = os.urandom(4).hex()
        fut = asyncio.get_running_loop().create_future()
        self.pending[rid] = fut
        self._send(self._envelope(t, d, rid, False), addr)
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self.pending.pop(rid, None)

    async def call(self, c: Contact, t, d=None, timeout=1.5):
        """RPC to a contact; evicts it from the routing table after 2 consecutive failures."""
        r = await self.rpc(c.addr, t, d, timeout)
        tc = self.table.get(c.id) or c
        if r is None:
            tc.fails += 1
            if tc.fails >= 2:
                self.table.remove(c.id)
        else:
            tc.fails = 0
        return r

    async def lookup(self, target: int, key: str | None = None):
        """Iterative Kademlia lookup. Returns (closest_contacts, value_or_None)."""
        shortlist = {c.id: c for c in self.table.closest(target, K)}
        queried, responded = set(), {}
        rpc_type = "find_value" if key else "find_node"
        payload = {"target": hex_id(target), "key": key}
        for _ in range(12):
            closest = sorted(shortlist.values(), key=lambda c: c.id ^ target)[:K]
            todo = [c for c in closest if c.id not in queried][:ALPHA]
            if not todo:
                break
            queried.update(c.id for c in todo)
            results = await asyncio.gather(*(self.call(c, rpc_type, payload) for c in todo))
            for c, r in zip(todo, results):
                if r is None:
                    shortlist.pop(c.id, None)
                    continue
                responded[c.id] = c
                if key and "value" in r:
                    return [], r["value"]
                for w in r.get("nodes", []):
                    try:
                        nc = Contact.from_wire(w)
                    except (KeyError, ValueError, TypeError):
                        continue
                    if nc.id != self.node_id and nc.id not in shortlist:
                        shortlist[nc.id] = nc
        return sorted(responded.values(), key=lambda c: c.id ^ target)[:K], None

    async def bootstrap(self, addrs) -> int:
        """Ping candidate addresses, then look up our own id to fill the routing table."""
        addrs = list(addrs)
        res = await asyncio.gather(*(self.rpc(a, "ping", timeout=1.0) for a in addrs))
        if len(self.table):
            await self.lookup(self.node_id)
        return sum(1 for r in res if r is not None)

    async def store(self, key: bytes, value: str, ttl: float = 3600):
        ver = time.time()                           # stamp BEFORE the (possibly slow) lookup
        kid = key_to_id(key)
        khex = hex_id(kid)
        self._local_put(khex, value, ttl, ver)
        contacts, _ = await self.lookup(kid)
        await asyncio.gather(*(self.call(c, "store", {"key": khex, "value": value, "ttl": ttl, "ver": ver})
                               for c in contacts))

    async def get(self, key: bytes):
        kid = key_to_id(key)
        khex = hex_id(kid)
        v = self._local_get(khex)
        if v is not None:
            return v
        _, v = await self.lookup(kid, key=khex)
        return v

    async def maintain(self, rediscover):
        """Background upkeep: liveness pings, bucket refresh, re-bootstrap if isolated."""
        n = 0
        while True:
            await asyncio.sleep(5)
            n += 1
            try:
                contacts = self.table.all()
                if not contacts:
                    await rediscover()
                    continue
                for c in random.sample(contacts, min(3, len(contacts))):
                    await self.call(c, "ping")
                if n % 4 == 0:
                    await self.lookup(new_node_id())
                self.expire()
            except Exception:
                log.exception("maintenance error")
