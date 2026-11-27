"""Command line: init | keygen | node | launch | submit | status | task | dashboard"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.util
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path


def _addr(s):
    host, _, port = s.rpartition(":")
    return (host or "127.0.0.1", int(port))


def load_callable(spec: str):
    """'examples/tasks.py:monte_carlo_pi' or 'package.module:func' -> callable (pickled by value)."""
    import cloudpickle
    target, _, fn = spec.rpartition(":")
    if not target:
        raise SystemExit("target must look like  file.py:function  or  module:function")
    if target.endswith(".py"):
        name = "mw_user_" + Path(target).stem
        s = importlib.util.spec_from_file_location(name, target)
        mod = importlib.util.module_from_spec(s)
        sys.modules[name] = mod
        s.loader.exec_module(mod)
    else:
        sys.path.insert(0, os.getcwd())
        mod = importlib.import_module(target)
    cloudpickle.register_pickle_by_value(mod)      # ship the code itself, not just a module name
    return getattr(mod, fn)


def cmd_init(a):
    from . import security
    f = security.init_cluster(a.dir)
    print(f"cluster files ready in {f.root}/")
    print("  mesh.secret          shared UDP HMAC secret      (nodes only)")
    print("  tls/cert.pem,key.pem pinned TLS certificate       (nodes; cert.pem also for clients)")
    print("  keys/submitter.key   task-signing private key     (submitters ONLY)")
    print("  keys/trusted/*.pub   public keys nodes accept     (nodes)")


def cmd_keygen(a):
    from . import security
    security.new_submitter(security.ClusterFiles(a.dir), a.name)
    print(f"created keys/{a.name}.key and trusted/{a.name}.pub")


def cmd_node(a):
    from .node import MeshNode
    node = MeshNode(a.dir, port=a.port, host=a.host, bootstrap=[_addr(b) for b in a.bootstrap],
                    gossip_interval=a.gossip_interval, max_tasks=a.max_tasks, scan_base=a.scan_base,
                    scan_range=a.scan_range, hb_timeout=a.hb_timeout)
    try:
        asyncio.run(node.run_forever())
    except KeyboardInterrupt:
        pass


def cmd_launch(a):
    Path(a.logs).mkdir(parents=True, exist_ok=True)
    procs = []
    try:
        for i in range(a.count):
            port = a.base_port + i
            cmd = [sys.executable, "-m", "meshweaver", "node", "--dir", a.dir, "--port", str(port),
                   "--scan-base", str(a.base_port), "--gossip-interval", str(a.gossip_interval)]
            log = open(Path(a.logs) / f"node-{port}.log", "w")
            procs.append(subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT))
            print(f"started node {i + 1}/{a.count} on port {port}  (log: {a.logs}/node-{port}.log)")
            time.sleep(0.3)
        print(f"\n{a.count} nodes running, NO bootstrap addresses given -> they found each other.\n"
              f"Ctrl+C here stops them all.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs:
            p.terminate()
        print("\nall nodes stopped")


def _client(a):
    from .client import Client
    return Client(a.dir, gateway=a.node, scan_base=a.scan_base)


def cmd_submit(a):
    from .client import TaskFailed
    import cloudpickle
    func = load_callable(a.target)
    args = json.loads(a.args)
    kwargs = json.loads(a.kwargs)
    c = _client(a)

    def show(ev):
        if ev["state"] == "assigned":
            print(f"  -> assigned to node {ev['node']} (port {ev['port']}), attempt {ev['attempt']}")
        elif ev["state"] == "failed":
            print(f"  !! node {ev['node']} failed ({ev['reason']}) -- re-routing")

    async def go():
        res = await c.submit_detailed(func, *args, kwargs=kwargs, timeout=a.timeout, on_event=show)
        if res["ok"]:
            from . import security
            value = cloudpickle.loads(security.unb64(res["result_b64"]))
            print(f"task {res['task_id']} done on node {res['executor']} "
                  f"in {res['duration']}s after {res['attempts']} attempt(s)")
            print("result:", json.dumps(value, indent=2, default=repr))
        else:
            print(f"task {res['task_id']} FAILED: {res.get('error')}")
            sys.exit(1)
    try:
        asyncio.run(go())
    except (TaskFailed, ConnectionError, OSError, asyncio.TimeoutError) as e:
        sys.exit(f"error: {e}")


def cmd_status(a):
    st = asyncio.run(_client(a).status())
    print(f"gateway node {st['id'][:8]} port {st['port']} uptime {st['uptime']}s, "
          f"routing contacts: {len(st['contacts'])}, DHT keys: {st['dht_keys']}")
    print(f"{'NODE':<10}{'ADDRESS':<22}{'STATE':<8}{'CPU%':>6}{'RAM%':>6}{'TASKS':>6}")
    me = st["self"]
    print(f"{st['id'][:8]:<10}{'(gateway)':<22}{'alive':<8}{me['cpu']:>6.0f}{me['ram']:>6.0f}{me['tasks']:>6}")
    for e in sorted(st["loads"], key=lambda e: e["port"]):
        print(f"{e['id'][:8]:<10}{e['host'] + ':' + str(e['port']):<22}{e['state']:<8}"
              f"{e['cpu']:>6.0f}{e['ram']:>6.0f}{e['tasks']:>6}")


def cmd_task(a):
    print(json.dumps(asyncio.run(_client(a).task_info(a.task_id)), indent=2))


def cmd_dashboard(a):
    try:
        import rich  # noqa: F401
    except ImportError:
        sys.exit("dashboard needs rich:  pip install rich")
    from .dashboard import run_dashboard
    try:
        asyncio.run(run_dashboard(_client(a), a.node and _addr(a.node)))
    except KeyboardInterrupt:
        pass


def main(argv=None):
    ap = argparse.ArgumentParser(prog="meshweaver", description="Decentralised P2P async task broker")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, node=True):
        p.add_argument("--dir", default="cluster", help="cluster folder created by 'init'")
        if node:
            p.add_argument("--node", help="gateway host:port (default: auto-find on localhost)")
            p.add_argument("--scan-base", type=int, default=9000)

    p = sub.add_parser("init", help="create secret, TLS cert and signing keys"); common(p, False); p.set_defaults(f=cmd_init)
    p = sub.add_parser("keygen", help="add another trusted submitter key"); common(p, False)
    p.add_argument("name"); p.set_defaults(f=cmd_keygen)

    p = sub.add_parser("node", help="run one mesh node"); common(p, False)
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--bootstrap", action="append", default=[], help="host:port of a known node (optional)")
    p.add_argument("--gossip-interval", type=float, default=5.0)
    p.add_argument("--hb-timeout", type=float, default=5.0)
    p.add_argument("--max-tasks", type=int, default=None)
    p.add_argument("--scan-base", type=int, default=9000)
    p.add_argument("--scan-range", type=int, default=30)
    p.set_defaults(f=cmd_node)

    p = sub.add_parser("launch", help="start N nodes locally (for demos/tests)"); common(p, False)
    p.add_argument("--count", type=int, default=10)
    p.add_argument("--base-port", type=int, default=9000)
    p.add_argument("--gossip-interval", type=float, default=2.0)
    p.add_argument("--logs", default="logs")
    p.set_defaults(f=cmd_launch)

    p = sub.add_parser("submit", help="run a function on the mesh"); common(p)
    p.add_argument("target", help="file.py:function  or  module:function")
    p.add_argument("--args", default="[]", help="JSON list of positional args")
    p.add_argument("--kwargs", default="{}", help="JSON object of keyword args")
    p.add_argument("--timeout", type=float, default=300)
    p.set_defaults(f=cmd_submit)

    p = sub.add_parser("status", help="print the mesh as seen by a node"); common(p); p.set_defaults(f=cmd_status)
    p = sub.add_parser("task", help="look up a task record stored in the DHT"); common(p)
    p.add_argument("task_id"); p.set_defaults(f=cmd_task)
    p = sub.add_parser("dashboard", help="live rich dashboard"); common(p); p.set_defaults(f=cmd_dashboard)

    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    a.f(a)
