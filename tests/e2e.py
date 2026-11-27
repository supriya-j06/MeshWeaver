#!/usr/bin/env python3
"""
End-to-end proof for the review demos. Run from the project root:
    python tests/e2e.py
Starts 10 real node processes (NO bootstrap addresses), then checks:
  1. discovery   : every node finds every other node via Kademlia + gossip
  2. serialization: a lambda, a math function and an ML training job run remotely
  3. routing     : concurrent tasks spread across nodes
  4. fault tol.  : a node is SIGKILLed mid-task -> task marked failed -> re-routed -> still succeeds
  5. security    : a request signed by an untrusted key is rejected
  6. DHT         : task state is stored in the DHT and readable from another node
"""
import asyncio
import os
import shutil
import subprocess
import sys
import tempfile
import time
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from meshweaver import security                       # noqa: E402
from meshweaver.client import Client, TaskFailed      # noqa: E402
from meshweaver.wire import read_frame, send_frame    # noqa: E402
import importlib.util                                  # noqa: E402

BASE, N = 9400, 10
spec = importlib.util.spec_from_file_location("ex_tasks", os.path.join(ROOT, "examples", "tasks.py"))
ex = importlib.util.module_from_spec(spec)
sys.modules["ex_tasks"] = ex
spec.loader.exec_module(ex)
import cloudpickle                                     # noqa: E402
cloudpickle.register_pickle_by_value(ex)

ok_count = 0


def check(cond, msg):
    global ok_count
    print(("  PASS  " if cond else "  FAIL  ") + msg)
    if not cond:
        raise SystemExit(1)
    ok_count += 1


async def main():
    tmp = tempfile.mkdtemp(prefix="mw_e2e_")
    cdir = os.path.join(tmp, "cluster")
    security.init_cluster(cdir)
    procs = {}
    env = dict(os.environ, PYTHONPATH=ROOT)
    try:
        for i in range(N):
            port = BASE + i
            procs[port] = subprocess.Popen(
                [sys.executable, "-m", "meshweaver", "node", "--dir", cdir, "--port", str(port),
                 "--scan-base", str(BASE), "--gossip-interval", "1", "--hb-timeout", "3"],
                stdout=open(os.path.join(tmp, f"n{port}.log"), "w"), stderr=subprocess.STDOUT, env=env)
            await asyncio.sleep(0.3)
        c = Client(cdir, scan_base=BASE)

        print("\n[1] discovery without hardcoded IPs")
        for _ in range(80):
            await asyncio.sleep(0.5)
            try:
                views = [len([e for e in (await c.status(("127.0.0.1", p)))["loads"] if e["state"] == "alive"]) + 1
                         for p in procs]
            except Exception:
                continue
            if all(v == N for v in views):
                break
        check(all(v == N for v in views), f"all {N} nodes see all {N} nodes alive: {views}")
        st = await c.status(("127.0.0.1", BASE))
        check(len(st["contacts"]) >= 4, f"Kademlia routing table populated ({len(st['contacts'])} contacts)")

        print("\n[2] serialization of Python functions (cloudpickle)")
        check(await c.submit(lambda x, y: x * y + 1, 6, 7) == 43, "lambda defined in this script ran remotely")
        det = await c.submit(ex.matrix_determinant, 30)
        check(isinstance(det, float), f"matrix determinant computed remotely ({det:.3e})")
        ml = await c.submit(ex.train_logistic_regression, 300, 80)
        check(ml["accuracy"] > 0.9, f"ML training job ran remotely, accuracy={ml['accuracy']:.2f}")

        print("\n[3] load-aware routing")
        res = await asyncio.gather(*(c.submit_detailed(ex.slow_task, 1.0) for _ in range(10)))
        used = Counter(r["executor"] for r in res)
        check(all(r["ok"] for r in res), "10 concurrent tasks succeeded")
        check(len(used) >= 5, f"tasks spread over {len(used)} different nodes: {dict(used)}")

        print("\n[4] fault tolerance: kill the executing node mid-task")
        gw_port = c.gateway[1]
        for _ in range(8):                               # avoid killing the gateway (= coordinator)
            events, victim = [], {}

            def on_event(ev, events=events, victim=victim):
                events.append(ev)
                if ev["state"] == "assigned" and "port" not in victim:
                    victim["port"] = ev["port"]

            job = asyncio.create_task(c.submit_detailed(ex.slow_task, 6, on_event=on_event))
            for _ in range(40):
                await asyncio.sleep(0.1)
                if victim:
                    break
            if victim["port"] != gw_port:
                break
            await job                                    # landed on the gateway itself; try again
        await asyncio.sleep(2.0)                         # task is now running on the victim
        procs[victim["port"]].kill()                     # SIGKILL = sudden power loss
        t0 = time.time()
        res = await job
        states = [e["state"] for e in events]
        check("failed" in states, f"node {victim['port']} killed -> task marked 'failed' (events: {states})")
        check(res["ok"] and res["attempts"] >= 2, f"task re-routed and finished on node {res['executor']} "
                                                  f"after {res['attempts']} attempts")
        print(f"        (recovered {time.time() - t0:.1f}s after the kill)")

        print("\n[5] task errors are reported, not retried")
        r = await c.submit_detailed(ex.always_fails)
        check(not r["ok"] and r["attempts"] == 1 and "ValueError" in r["error"], "ValueError surfaced, 1 attempt")

        print("\n[6] security")
        from cryptography.hazmat.primitives.asymmetric import ed25519
        evil = ed25519.Ed25519PrivateKey.generate()
        req = security.sign_request(evil, "submitter", "submit", task_id="x", name="evil",
                                    payload=security.b64(b"junk"), timeout=5)
        alive_port = next(p for p, pr in procs.items() if pr.poll() is None)
        reader, writer = await asyncio.open_connection("127.0.0.1", alive_port, ssl=c.ssl)
        await send_frame(writer, {"type": "submit", **req})
        frame = await read_frame(reader)
        writer.close()
        check(frame["type"] == "error" and "bad signature" in frame["error"], f"forged signature rejected: {frame['error']}")
        try:
            r = await asyncio.open_connection("127.0.0.1", alive_port)   # plain TCP, no TLS
            r[1].write(b"\x00\x00\x00\x02{}")
            await asyncio.sleep(0.3)
            data = await asyncio.wait_for(r[0].read(10), 2)
            check(not data.startswith(b"\x00"), "plain-text (non-TLS) connection gets no task service")
        except (ConnectionError, asyncio.TimeoutError, OSError):
            check(True, "plain-text (non-TLS) connection refused")

        print("\n[7] DHT stores task state")
        other = next(p for p, pr in procs.items() if pr.poll() is None and p != BASE)
        for _ in range(25):                              # DHT is eventually consistent: poll up to ~12s
            info = await c.task_info(res["task_id"], ("127.0.0.1", other))
            if info and info["state"] == "done":
                break
            await asyncio.sleep(0.5)
        check(info is not None and info["state"] == "done", f"task record readable from another node: {info}")
        print(f"\nALL {ok_count} CHECKS PASSED")
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.kill()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
