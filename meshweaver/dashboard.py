"""Live terminal dashboard (rich): mesh topology + task execution state."""
from __future__ import annotations

import asyncio
import time

from .client import Client


async def collect(client: Client, gateway=None) -> dict:
    """Ask the gateway for its view, then ask every live node for its tasks + neighbours."""
    st = await client.status(gateway)
    nodes = {}
    own = st["self"]
    nodes[st["id"]] = {"id": st["id"], "host": "gateway", "port": st["port"], "cpu": own["cpu"],
                       "ram": own["ram"], "tasks": own["tasks"], "age": 0.0, "state": "alive"}
    for e in st["loads"]:
        nodes[e["id"]] = {k: e[k] for k in ("id", "host", "port", "cpu", "ram", "tasks", "age", "state")}
    details = {st["id"]: st}

    async def fetch(n):
        try:
            details[n["id"]] = await asyncio.wait_for(client.status((n["host"], n["port"])), 3)
        except Exception:
            n["state"] = "dead" if n["state"] == "alive" else n["state"]

    await asyncio.gather(*(fetch(n) for n in nodes.values()
                           if n["state"] == "alive" and n["id"] != st["id"]))
    tasks = {}
    for nid, d in details.items():
        for r in d["tasks"]:
            cur = tasks.get(r["id"])
            if cur is None or (r["role"] == "coordinator" and cur["role"] != "coordinator"):
                tasks[r["id"]] = r
    topology = {nid: [c["id"][:8] for c in d["contacts"]] for nid, d in details.items()}
    return {"nodes": sorted(nodes.values(), key=lambda n: n["port"]),
            "tasks": sorted(tasks.values(), key=lambda r: r.get("created", 0), reverse=True)[:15],
            "topology": topology, "ts": time.time()}


def _bar(pct, width=10):
    n = int(round(pct / 100 * width))
    colour = "green" if pct < 50 else "yellow" if pct < 80 else "red"
    return f"[{colour}]{'█' * n}[/]{'░' * (width - n)} {pct:4.0f}%"


STATE_COLOUR = {"done": "green", "running": "cyan", "assigned": "blue", "queued": "white",
                "failed": "red", "alive": "green", "dead": "red"}


def render(data: dict):
    from rich.console import Group
    from rich.panel import Panel
    from rich.table import Table

    alive = sum(1 for n in data["nodes"] if n["state"] == "alive")
    head = Panel(f"[bold]MeshWeaver[/]  nodes alive: [green]{alive}[/]/{len(data['nodes'])}   "
                 f"tasks tracked: {len(data['tasks'])}   (Ctrl+C to quit)", border_style="cyan")

    nt = Table(title="Mesh nodes (gossip view)", expand=True)
    for col in ("Node", "Address", "State", "CPU", "RAM", "Tasks", "Seen"):
        nt.add_column(col)
    for n in data["nodes"]:
        c = STATE_COLOUR.get(n["state"], "white")
        nt.add_row(n["id"][:8], f"{n['host']}:{n['port']}", f"[{c}]{n['state'].upper()}[/]",
                   _bar(n["cpu"]), f"{n['ram']:.0f}%", str(n["tasks"]), f"{n['age']:.0f}s ago")

    short = {n["id"]: n for n in data["nodes"]}
    tt = Table(title="Topology (Kademlia routing-table neighbours)", expand=True)
    tt.add_column("Node")
    tt.add_column("Port")
    tt.add_column("Connected to")
    for nid, peers in data["topology"].items():
        tt.add_row(nid[:8], str(short.get(nid, {}).get("port", "?")),
                   " ".join(peers) if peers else "[dim]-[/]")

    kt = Table(title="Task execution", expand=True)
    for col in ("Task", "Function", "State", "Executor", "Attempts", "Time", "Error"):
        kt.add_column(col)
    now = time.time()
    for r in data["tasks"]:
        c = STATE_COLOUR.get(r["state"], "white")
        t = f"{r['duration']:.1f}s" if r.get("duration") else f"{now - r['created']:.0f}s"
        err = (r.get("error") or "").splitlines()[-1][:40] if r.get("error") else ""
        kt.add_row(r["id"], r["name"][:24], f"[{c}]{r['state']}[/]", r.get("node") or "-",
                   str(r["attempts"]), t, err)
    return Group(head, nt, tt, kt)


async def run_dashboard(client: Client, gateway=None, refresh=1.0):
    from rich.console import Console
    from rich.live import Live
    console = Console()
    data = await collect(client, gateway)
    with Live(render(data), console=console, refresh_per_second=4, screen=False) as live:
        while True:
            await asyncio.sleep(refresh)
            try:
                data = await collect(client, gateway)
            except Exception as e:
                live.update(f"[red]gateway unreachable: {e!r}  (retrying)[/]")
                continue
            live.update(render(data))
