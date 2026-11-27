# MeshWeaver: Zero-central-server P2P async task broker

Pure Python. No Redis, no RabbitMQ, no central broker. Every node is identical.

## Install
    python -m venv venv
    venv\Scripts\activate        (Windows)      source venv/bin/activate   (Linux/macOS)
    pip install -r requirements.txt

## Run (5 steps)
    python -m meshweaver init --dir cluster                      # 1. keys + TLS cert (once)
    python -m meshweaver launch --dir cluster --count 10         # 2. terminal A: 10 nodes, no IPs given
    python -m meshweaver dashboard --dir cluster                 # 3. terminal B: live dashboard
    python -m meshweaver submit --dir cluster examples/tasks.py:train_logistic_regression   # 4. terminal C
    python -m meshweaver submit --dir cluster examples/tasks.py:slow_task --args "[20]"     # 5. then kill a node
    python tests/e2e.py                                          # automatic proof of everything

## Python API
    from meshweaver.client import Client
    import asyncio
    c = Client("cluster")
    print(asyncio.run(c.submit(lambda x: x ** 2, 12)))           # runs on the least-loaded node

## Real machines (edge devices)
Copy the whole `cluster/` folder to each machine (delete `keys/submitter.key` from edge nodes;
keep it only on machines that may submit tasks, plus `tls/cert.pem`). Run on every machine:
    python -m meshweaver node --dir cluster --port 9000
Nodes on one LAN find each other by UDP broadcast to port 9000. Across networks add
`--bootstrap <ip>:9000` once. Open TCP+UDP port 9000 in the firewall.

## Security model
* UDP discovery/gossip: HMAC-SHA256 with `mesh.secret`
* Task channel: TLS 1.2+, certificate pinned to the cluster cert
* Task execution: only functions signed (Ed25519) by a key in `keys/trusted/` are executed.
  Tasks run in a separate worker process with a timeout.
  Treat signing keys like SSH keys: whoever holds one can run arbitrary Python on every node.

## Known limits
* If the gateway node (the one the client talks to) dies, the client connection drops; the
  task state is still in the DHT (`python -m meshweaver task <id>`), but results are not.
* DHT is eventually consistent; records expire after 1 hour (no republish).
* Only task-function exceptions are final; node failures are retried (max 3 attempts), so
  tasks should be idempotent (at-least-once).
