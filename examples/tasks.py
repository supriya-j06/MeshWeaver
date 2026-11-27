"""Example tasks you can send to the mesh:  python -m meshweaver submit examples/tasks.py:<name>"""
import math
import os
import random
import socket
import time


def whoami():
    return {"host": socket.gethostname(), "pid": os.getpid()}


def slow_task(seconds=5):
    time.sleep(seconds)
    return {"slept": seconds, "pid": os.getpid(), "host": socket.gethostname()}


def fib(n=30):
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


def monte_carlo_pi(samples=500_000, seed=1):
    rnd = random.Random(seed)
    inside = sum(1 for _ in range(samples) if rnd.random() ** 2 + rnd.random() ** 2 <= 1)
    return {"pi_estimate": 4 * inside / samples, "samples": samples}


def matrix_determinant(n=60, seed=7):
    """Gaussian elimination on a random n x n matrix (pure Python)."""
    rnd = random.Random(seed)
    m = [[rnd.uniform(-1, 1) for _ in range(n)] for _ in range(n)]
    det = 1.0
    for i in range(n):
        p = max(range(i, n), key=lambda r: abs(m[r][i]))
        if abs(m[p][i]) < 1e-12:
            return 0.0
        if p != i:
            m[i], m[p] = m[p], m[i]
            det = -det
        det *= m[i][i]
        for r in range(i + 1, n):
            f = m[r][i] / m[i][i]
            for c in range(i, n):
                m[r][c] -= f * m[i][c]
    return det


def train_logistic_regression(samples=400, epochs=150, lr=0.3, seed=3):
    """A tiny ML job: logistic regression trained by gradient descent on two gaussian blobs."""
    rnd = random.Random(seed)
    data = []
    for _ in range(samples):
        y = rnd.random() < 0.5
        cx = 1.5 if y else -1.5
        data.append(((rnd.gauss(cx, 1.0), rnd.gauss(cx, 1.0)), 1 if y else 0))
    w, b = [0.0, 0.0], 0.0
    for _ in range(epochs):
        gw, gb = [0.0, 0.0], 0.0
        for (x1, x2), y in data:
            p = 1 / (1 + math.exp(-(w[0] * x1 + w[1] * x2 + b)))
            e = p - y
            gw[0] += e * x1
            gw[1] += e * x2
            gb += e
        w[0] -= lr * gw[0] / samples
        w[1] -= lr * gw[1] / samples
        b -= lr * gb / samples
    correct = sum(1 for (x1, x2), y in data if (w[0] * x1 + w[1] * x2 + b > 0) == bool(y))
    return {"accuracy": correct / samples, "weights": w, "bias": b}


def always_fails():
    raise ValueError("this task fails on purpose")
