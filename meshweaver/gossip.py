"""Gossip state: who is alive and how loaded they are (CPU %, RAM %, running tasks)."""
from __future__ import annotations

import time

import psutil


def prime_cpu():
    psutil.cpu_percent(None)


def sample_load():
    return psutil.cpu_percent(None), psutil.virtual_memory().percent


class LoadTable:
    """id -> latest load entry. last_seen is when we last got *fresher* data about that node."""

    def __init__(self):
        self.entries: dict[str, dict] = {}

    def merge(self, entry: dict, last_seen: float) -> bool:
        cur = self.entries.get(entry["id"])
        if cur and entry["seq"] <= cur["seq"]:
            return False
        self.entries[entry["id"]] = {**entry, "last_seen": last_seen}
        return True

    def alive(self, now: float, dead_after: float):
        return [e for e in self.entries.values() if now - e["last_seen"] <= dead_after]

    def view(self, now: float, dead_after: float):
        out = []
        for e in self.entries.values():
            age = now - e["last_seen"]
            out.append({**e, "age": round(age, 1), "state": "alive" if age <= dead_after else "dead"})
        return out

    def prune(self, now: float, keep_dead_for: float = 120.0, dead_after: float = 10.0):
        for nid in [n for n, e in self.entries.items() if now - e["last_seen"] > dead_after + keep_dead_for]:
            del self.entries[nid]
