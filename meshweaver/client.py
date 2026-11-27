"""Client API: sign + submit Python functions to any node of the mesh."""
from __future__ import annotations

import asyncio
import uuid

import cloudpickle

from . import security, serializer
from .wire import read_frame, send_frame


class TaskFailed(Exception):
    pass


class Client:
    def __init__(self, cluster_dir, gateway: str | None = None, key_name="submitter",
                 scan_base=9000, scan_range=30):
        self.files = security.ClusterFiles(cluster_dir)
        self.priv = security.load_signing_key(self.files, key_name)
        self.kid = key_name
        self.ssl = security.client_ssl_context(self.files)
        self.gateway = self._parse(gateway) if gateway else None
        self.scan_base, self.scan_range = scan_base, scan_range

    @staticmethod
    def _parse(s):
        host, _, port = s.rpartition(":")
        return (host or "127.0.0.1", int(port))

    async def _open(self, addr):
        return await asyncio.wait_for(asyncio.open_connection(addr[0], addr[1], ssl=self.ssl), 3)

    async def find_gateway(self):
        if self.gateway:
            return self.gateway
        for p in range(self.scan_base, self.scan_base + self.scan_range):
            try:
                _, w = await self._open(("127.0.0.1", p))
                w.close()
                self.gateway = ("127.0.0.1", p)
                return self.gateway
            except Exception:
                continue
        raise ConnectionError("no MeshWeaver node found on localhost -- pass --node host:port")

    async def _simple(self, op, addr=None, **fields):
        addr = addr or await self.find_gateway()
        reader, writer = await self._open(addr)
        try:
            await send_frame(writer, {"type": op, **security.sign_request(self.priv, self.kid, op, **fields)})
            frame = await asyncio.wait_for(read_frame(reader), 10)
        finally:
            writer.close()
        if frame.get("type") == "error":
            raise TaskFailed(frame["error"])
        return frame["data"]

    async def status(self, addr=None):
        return await self._simple("status", addr)

    async def task_info(self, task_id, addr=None):
        return await self._simple("taskinfo", addr, task_id=task_id)

    async def submit_detailed(self, func, *args, kwargs=None, timeout=300, on_event=None, node=None):
        """Submit and return the raw 'done' frame (includes executor, attempts, duration)."""
        addr = self._parse(node) if node else await self.find_gateway()
        payload = serializer.pack_task(func, args, kwargs)
        task_id = uuid.uuid4().hex[:12]
        req = security.sign_request(self.priv, self.kid, "submit", task_id=task_id,
                                    name=serializer.func_name(func), payload=security.b64(payload),
                                    timeout=timeout)
        reader, writer = await self._open(addr)
        try:
            await send_frame(writer, {"type": "submit", **req})
            while True:
                frame = await asyncio.wait_for(read_frame(reader), 20)   # gateway sends hb every 1s
                t = frame.get("type")
                if t == "hb":
                    continue
                if t == "event":
                    if on_event:
                        on_event(frame)
                    continue
                if t == "error":
                    raise TaskFailed(frame["error"])
                if t == "done":
                    frame["task_id"] = task_id
                    return frame
        finally:
            writer.close()

    async def submit(self, func, *args, kwargs=None, timeout=300, on_event=None, node=None):
        """Run func(*args, **kwargs) somewhere on the mesh and return its result."""
        res = await self.submit_detailed(func, *args, kwargs=kwargs, timeout=timeout,
                                         on_event=on_event, node=node)
        if not res.get("ok"):
            raise TaskFailed(res.get("error", "task failed"))
        return cloudpickle.loads(security.unb64(res["result_b64"]))
