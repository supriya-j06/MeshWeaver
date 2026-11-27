"""
MeshWeaver node = one peer of the mesh. Every node can submit, route and execute tasks.

 UDP  port N : Kademlia DHT + gossip (HMAC-authenticated)
 TCP  port N : TLS task channel (Ed25519-signed task requests)

Task life-cycle (coordinator side):
  queued -> assigned -> running -> done | failed
  if the executor dies / goes silent: failed -> re-routed to the next best node (max_attempts)
"""
from __future__ import annotations

import asyncio
import base64
import collections
import json
import logging
import os
import random
import ssl
import sys
import time
from pathlib import Path

from . import dht as dhtmod
from . import gossip, security, udp
from .wire import read_frame, send_frame

log = logging.getLogger("meshweaver.node")


class NodeFailure(Exception):
    """The executing node died, vanished or went silent (task is re-routable)."""


class MeshNode:
    def __init__(self, cluster_dir, port=9000, host="0.0.0.0", bootstrap=None,
                 gossip_interval=5.0, max_tasks=None, scan_base=9000, scan_range=30,
                 hb_interval=1.0, hb_timeout=5.0, task_timeout=300.0, max_attempts=3):
        self.files = security.ClusterFiles(cluster_dir)
        self.secret = security.load_mesh_secret(self.files)
        self.trusted = security.load_trusted_keys(self.files)
        self.server_ssl = security.server_ssl_context(self.files)
        self.client_ssl = security.client_ssl_context(self.files)

        self.node_id = dhtmod.new_node_id()
        self.id = dhtmod.hex_id(self.node_id)
        self.short = self.id[:8]
        self.port, self.host = port, host
        self.bootstrap_addrs = list(bootstrap or [])
        self.scan_base, self.scan_range = scan_base, scan_range

        self.gossip_interval = gossip_interval
        self.dead_after = 4 * gossip_interval + 2
        self.hb_interval, self.hb_timeout = hb_interval, hb_timeout
        self.task_timeout, self.max_attempts = task_timeout, max_attempts
        self.max_tasks = max_tasks or max(1, os.cpu_count() or 1)

        self.dht: dhtmod.DHT | None = None
        self.loads = gossip.LoadTable()
        self.tasks: collections.OrderedDict = collections.OrderedDict()   # (task_id, role) -> record
        self.suspect: dict[str, float] = {}
        self.inflight: collections.Counter = collections.Counter()
        self.replay: dict[str, float] = {}
        self.probed: dict[str, float] = {}
        self.load_tasks = 0
        self.cpu, self.ram, self.seq = 0.0, 0.0, 0
        self.started = time.time()
        self._bg: set = set()
        self._jobs: list = []
        self._server = None
        self._transport = None
        self._sem: asyncio.Semaphore | None = None

    # ================================================================ lifecycle
    async def start(self):
        loop = asyncio.get_running_loop()
        self._sem = asyncio.Semaphore(self.max_tasks)
        gossip.prime_cpu()
        self.dht = dhtmod.DHT(self.node_id, self.port, self.secret, gossip_handler=self._on_gossip)
        self._server = await asyncio.start_server(
            self._on_tcp, self.host, self.port, ssl=self.server_ssl, ssl_handshake_timeout=5)
        self._transport = await udp.open_udp(loop, self.dht, self.host, self.port)
        log.info("node %s up on %s:%d (udp+tcp) max_tasks=%d", self.short, self.host, self.port, self.max_tasks)
        await self._join()
        self._jobs = [asyncio.create_task(self._sampler()),
                      asyncio.create_task(self._gossip_loop()),
                      asyncio.create_task(self.dht.maintain(self._join))]

    async def run_forever(self):
        await self.start()
        try:
            await asyncio.Event().wait()
        finally:
            await self.stop()

    async def stop(self):
        for j in self._jobs:
            j.cancel()
        if self._server:
            self._server.close()
        if self._transport:
            self._transport.close()

    def _spawn(self, coro):
        t = asyncio.create_task(coro)
        self._bg.add(t)
        t.add_done_callback(self._bg.discard)
        return t

    # ================================================================ discovery (Kademlia join)
    def _candidates(self):
        if self.bootstrap_addrs:
            return list(self.bootstrap_addrs)
        c = [("127.0.0.1", p) for p in range(self.scan_base, self.scan_base + self.scan_range)
             if p != self.port]
        c.append(("255.255.255.255", self.scan_base))      # LAN broadcast (best effort)
        return c

    async def _join(self):
        found = await self.dht.bootstrap(self._candidates())
        log.info("join: %d peers answered, routing table has %d contacts", found, len(self.dht.table))

    # ================================================================ gossip (load + heartbeat)
    async def _sampler(self):
        while True:
            self.cpu, self.ram = gossip.sample_load()
            self.loads.prune(time.time(), dead_after=self.dead_after)
            await asyncio.sleep(1.0)

    def _own_entry(self):
        return {"id": self.id, "host": None, "port": self.port, "cpu": self.cpu,
                "ram": self.ram, "tasks": self.load_tasks, "seq": self.seq, "age": 0.0}

    def _fresh_entries(self, limit=12):
        now = time.time()
        es = sorted(self.loads.alive(now, self.dead_after), key=lambda e: -e["last_seen"])[:limit]
        return [{"id": e["id"], "host": e["host"], "port": e["port"], "cpu": e["cpu"], "ram": e["ram"],
                 "tasks": e["tasks"], "seq": e["seq"], "age": round(now - e["last_seen"], 2)} for e in es]

    def _gossip_targets(self):
        contacts = self.dht.table.all()
        contacts.sort(key=lambda c: c.id ^ self.node_id)
        nearest, rest = contacts[:3], contacts[3:]
        return nearest + random.sample(rest, min(2, len(rest)))

    async def _gossip_loop(self):
        while True:
            self.seq += 1
            payload = {"entries": [self._own_entry()] + self._fresh_entries()}
            for c in self._gossip_targets():
                self.dht.send_oneway(c.addr, "gossip", payload)
            await asyncio.sleep(self.gossip_interval)

    def _on_gossip(self, contact, d):
        now = time.time()
        for e in d.get("entries", []):
            try:
                nid = e["id"]
                if nid == self.id:
                    continue
                host = e.get("host") or (contact.host if nid == contact.hex else None)
                age = float(e.get("age", 0))
                if host is None or age > self.dead_after:
                    continue
                entry = {"id": nid, "host": host, "port": int(e["port"]), "cpu": float(e["cpu"]),
                         "ram": float(e["ram"]), "tasks": int(e["tasks"]), "seq": int(e["seq"])}
            except (KeyError, TypeError, ValueError):
                continue
            if self.loads.merge(entry, now - age):
                self.suspect.pop(nid, None)
                self._maybe_probe(entry)

    def _maybe_probe(self, e):
        """Learned of a node via gossip but not in our routing table -> ping it directly."""
        known = {c.hex for c in self.dht.table.all()}
        if e["id"] in known or time.time() - self.probed.get(e["id"], 0) < 30:
            return
        self.probed[e["id"]] = time.time()
        self._spawn(self.dht.rpc((e["host"], e["port"]), "ping", timeout=1.0))

    # ================================================================ routing
    def pick(self, exclude):
        """Choose the live node with the lowest load score (CPU first, queued tasks as penalty)."""
        now = time.time()
        cands = []
        if self.id not in exclude:
            cands.append(self._own_entry())
        for e in self.loads.alive(now, self.dead_after):
            if e["id"] in exclude or self.suspect.get(e["id"], 0) > now:
                continue
            cands.append(e)
        if not cands:
            return None

        def score(e):
            pend = self.inflight[e["id"]] if e["id"] != self.id else 0
            return e["cpu"] + 10.0 * (e["tasks"] + pend) + random.random() * 0.01
        return min(cands, key=score)

    # ================================================================ task records
    def _rec(self, task_id, role, **kw):
        key = (task_id, role)
        rec = self.tasks.get(key)
        if rec is None:
            rec = {"id": task_id, "role": role, "name": "?", "state": "queued", "node": None,
                   "attempts": 0, "created": time.time(), "history": [], "error": None, "duration": None}
            self.tasks[key] = rec
            while len(self.tasks) > 300:
                self.tasks.popitem(last=False)
        if "state" in kw and kw["state"] != rec["state"]:
            rec["history"].append([round(time.time(), 2), kw["state"], kw.get("node", rec["node"])])
        rec.update(kw)
        rec["updated"] = time.time()
        return rec

    def _publish(self, rec):
        """Persist task state in the DHT so any node can look it up (no central DB)."""
        value = json.dumps({k: rec[k] for k in ("id", "name", "state", "node", "attempts", "error", "duration")})

        async def go():
            try:
                await self.dht.store(b"task:" + rec["id"].encode(), value)
            except Exception:
                log.debug("dht publish failed", exc_info=True)
        self._spawn(go())

    # ================================================================ TCP server
    async def _on_tcp(self, reader, writer):
        try:
            msg = await asyncio.wait_for(read_frame(reader), 10)
            t = msg.get("type")
            handler = {"submit": self._h_submit, "run": self._h_run,
                       "status": self._h_status, "taskinfo": self._h_taskinfo}.get(t)
            if handler is None:
                await send_frame(writer, {"type": "error", "error": f"unknown type {t!r}"})
            else:
                await handler(msg, writer)
        except security.SecurityError as e:
            log.warning("REJECTED request: %s", e)
            try:
                await send_frame(writer, {"type": "error", "error": f"rejected: {e}"})
            except Exception:
                pass
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError, ssl.SSLError, OSError):
            pass
        except Exception:
            log.exception("tcp handler error")
        finally:
            try:
                writer.close()
            except Exception:
                pass

    # ---- client -> gateway: submit
    async def _h_submit(self, msg, writer):
        body = security.verify_request(msg, self.trusted, replay=self.replay)
        if body.get("op") != "submit":
            raise security.SecurityError("op mismatch")

        async def emit(ev):
            try:
                await send_frame(writer, {"type": "event", **ev})
            except Exception:
                pass

        async def beat():
            while True:
                await asyncio.sleep(1.0)
                try:
                    await send_frame(writer, {"type": "hb"})
                except Exception:
                    return
        hb = asyncio.create_task(beat())
        try:
            res = await self.coordinate(body, msg, emit)
        finally:
            hb.cancel()
        await send_frame(writer, {"type": "done", **res})

    # ---- coordinator -> executor: run
    async def _h_run(self, msg, writer):
        body = security.verify_request(msg, self.trusted)       # signature + freshness
        if body.get("op") != "submit":
            raise security.SecurityError("op mismatch")
        key = (body["task_id"], "executor")
        old = self.tasks.get(key)
        if old and old["state"] in ("queued", "running"):
            await send_frame(writer, {"type": "error", "error": "duplicate task"})
            return
        job = asyncio.create_task(self._execute(body))
        try:
            while not job.done():
                await asyncio.wait({job}, timeout=self.hb_interval)
                if not job.done():
                    await send_frame(writer, {"type": "hb"})     # "I'm alive and working"
            await send_frame(writer, job.result())
        except (ConnectionError, OSError, ssl.SSLError):
            job.cancel()                                          # coordinator vanished
        except asyncio.CancelledError:
            job.cancel()
            raise

    async def _h_status(self, msg, writer):
        body = security.verify_request(msg, self.trusted, replay=self.replay)
        if body.get("op") != "status":
            raise security.SecurityError("op mismatch")
        await send_frame(writer, {"type": "status", "data": self.snapshot()})

    async def _h_taskinfo(self, msg, writer):
        body = security.verify_request(msg, self.trusted, replay=self.replay)
        if body.get("op") != "taskinfo":
            raise security.SecurityError("op mismatch")
        v = await self.dht.get(b"task:" + str(body["task_id"]).encode())
        await send_frame(writer, {"type": "taskinfo", "data": json.loads(v) if v else None})

    def snapshot(self):
        now = time.time()
        return {
            "id": self.id, "port": self.port, "uptime": round(now - self.started, 1),
            "self": {**self._own_entry(), "host": "self"},
            "loads": self.loads.view(now, self.dead_after),
            "contacts": [c.to_wire() for c in self.dht.table.all()],
            "tasks": [dict(r) for r in list(self.tasks.values())[-100:]],
            "dht_keys": len(self.dht.storage),
        }

    # ================================================================ coordinator
    async def coordinate(self, body, msg, emit):
        task_id, name = body["task_id"], body.get("name", "?")
        rec = self._rec(task_id, "coordinator", name=name, state="queued")
        exclude: set = set()
        last_error = "no live node available"
        for attempt in range(1, self.max_attempts + 1):
            target = self.pick(exclude)
            if target is None:
                await asyncio.sleep(1.0)                 # give gossip a moment, then retry once
                target = self.pick(exclude)
                if target is None:
                    break
            tid = target["id"]
            self._rec(task_id, "coordinator", state="assigned", node=tid[:8], attempts=attempt)
            await emit({"state": "assigned", "node": tid[:8], "port": target["port"], "attempt": attempt})
            log.info("task %s (%s) -> node %s [attempt %d]", task_id, name, tid[:8], attempt)
            self.inflight[tid] += 1
            try:
                res = await self._run_on(target, msg, body, rec)
            except NodeFailure as e:
                last_error = f"node {tid[:8]} failed: {e}"
                self.suspect[tid] = time.time() + 30
                self.dht.table.remove(int(tid, 16))          # proven dead -> evict from routing table now
                self._rec(task_id, "coordinator", state="failed", node=tid[:8], error=last_error)
                self._publish(rec)
                log.warning("task %s: %s -> re-routing", task_id, last_error)
                await emit({"state": "failed", "node": tid[:8], "reason": str(e), "attempt": attempt})
                exclude.add(tid)
                continue
            finally:
                self.inflight[tid] -= 1
            ok = res.get("ok", False)
            self._rec(task_id, "coordinator", state="done" if ok else "failed",
                      error=res.get("error"), duration=res.get("duration"))
            self._publish(rec)
            return {**{k: v for k, v in res.items() if k != "type"},
                    "task_id": task_id, "executor": tid[:8], "attempts": attempt}
        self._rec(task_id, "coordinator", state="failed", error=last_error)
        self._publish(rec)
        return {"ok": False, "kind": "no_node", "error": last_error, "task_id": task_id,
                "attempts": self.max_attempts}

    async def _run_on(self, target, msg, body, rec):
        if target["id"] == self.id:
            self._rec(body["task_id"], "coordinator", state="running")
            return await self._execute(body)
        writer = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(target["host"], target["port"], ssl=self.client_ssl), 3)
            await send_frame(writer, {"type": "run", "env": msg["env"], "sig": msg["sig"]})
            self._rec(body["task_id"], "coordinator", state="running")
            while True:
                frame = await asyncio.wait_for(read_frame(reader), self.hb_timeout)
                t = frame.get("type")
                if t == "hb":
                    continue
                if t == "result":
                    return frame
                raise NodeFailure(frame.get("error", f"unexpected frame {t!r}"))
        except asyncio.TimeoutError:
            raise NodeFailure("no heartbeat (timeout)")
        except asyncio.IncompleteReadError:
            raise NodeFailure("connection lost mid-task")
        except (OSError, ssl.SSLError, ConnectionError) as e:
            raise NodeFailure(f"{type(e).__name__}: {e}")
        finally:
            if writer:
                writer.close()

    # ================================================================ executor
    def _worker_env(self):
        env = dict(os.environ)
        pkg_parent = str(Path(__file__).resolve().parent.parent)
        env["PYTHONPATH"] = pkg_parent + os.pathsep + env.get("PYTHONPATH", "")
        return env

    async def _execute(self, body):
        task_id = body["task_id"]
        payload = base64.b64decode(body["payload"])
        timeout = float(body.get("timeout") or self.task_timeout)
        self._rec(task_id, "executor", name=body.get("name", "?"), state="queued", node=self.short)
        self.load_tasks += 1
        t0 = time.time()
        result = None
        try:
            async with self._sem:
                self._rec(task_id, "executor", state="running")
                proc = await asyncio.create_subprocess_exec(
                    sys.executable, "-m", "meshweaver.worker", env=self._worker_env(),
                    stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE)
                try:
                    out, err = await asyncio.wait_for(proc.communicate(payload), timeout)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
                    result = {"ok": False, "kind": "timeout", "error": f"task exceeded {timeout:.0f}s"}
                except asyncio.CancelledError:
                    proc.kill()
                    raise
                else:
                    if out[:1] == b"O":
                        result = {"ok": True, "result_b64": base64.b64encode(out[1:]).decode()}
                    elif out[:1] == b"E":
                        result = {"ok": False, "kind": "task_error", "error": out[1:].decode(errors="replace")}
                    else:
                        result = {"ok": False, "kind": "task_error",
                                  "error": f"worker exited {proc.returncode}: {err[-500:].decode(errors='replace')}"}
        finally:
            self.load_tasks -= 1
        dur = round(time.time() - t0, 3)
        self._rec(task_id, "executor", state="done" if result["ok"] else "failed",
                  error=result.get("error"), duration=dur)
        return {"type": "result", "duration": dur, **result}
