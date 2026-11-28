# MeshWeaver

**A Peer-to-Peer Asynchronous Task Broker built with Python.**

MeshWeaver is a decentralized task execution project that enables connected peer nodes to discover each other, exchange resource information, transfer and execute tasks without depending on centralized message brokers such as Redis or RabbitMQ.

## Overview

The project uses asynchronous networking to establish peer communication, discover nodes dynamically, transfer serialized Python functions, execute tasks on receiving peers and return results to the requester.

It also incorporates Kademlia-inspired routing and a Gossip Protocol to exchange CPU and RAM utilization information between neighboring peers.

## Features

### Week 1 – Asynchronous Communication and Task Transfer

- UDP-based peer communication using PING/PONG messages.
- Asynchronous networking using Python `asyncio`.
- Function serialization using `cloudpickle`.
- TCP-based task transfer between peer processes.
- Remote task execution and result transfer.
- Connection timeout and unavailable-peer handling.

### Week 2 – Peer Discovery and Resource Sharing

- Unique peer identity generation using UUID and SHA-1 hashing.
- Kademlia-inspired routing table with 160 buckets.
- XOR distance calculation for peer selection.
- UDP-based dynamic peer discovery.
- Multi-peer discovery and iterative peer lookup.
- CPU and RAM resource monitoring using `psutil`.
- Background Gossip Protocol for exchanging resource utilization information.
- Resource updates shared between neighboring nodes at five-second intervals.

## Tech Stack

| Technology | Purpose |
|---|---|
| Python | Core programming language |
| asyncio | Asynchronous networking |
| UDP | Peer communication and discovery |
| TCP | Task transfer |
| cloudpickle | Function serialization |
| psutil | CPU and RAM monitoring |
| hashlib and uuid | Peer identity generation |
| JSON | Resource information exchange |
| Git & GitHub | Version control |

## Architecture

```text
                 MeshWeaver Network
                         |
             +-----------+-----------+
             |                       |
          Peer A                  Peer B
             |                       |
       Peer Discovery          Peer Discovery
             |                       |
       Routing Table            Routing Table
             |                       |
             +------ Gossip ---------+
             |                       |
        CPU/RAM Data             CPU/RAM Data

             Client Peer
                  |
             Serialize Task
                  |
             TCP Connection
                  |
             Receiving Peer
                  |
              Execute Task
                  |
             Return Result
                  |
              Client Peer
```

## Project Structure

```text
MeshWeaver/
│
├── meshweaver/
│   ├── __init__.py
│   ├── node.py
│   ├── networking.py
│   ├── peer.py
│   ├── task.py
│   ├── task_network.py
│   ├── routing_table.py
│   ├── discovery.py
│   ├── monitor.py
│   └── gossip.py
│
├── examples/
│   ├── test_serialization.py
│   ├── test_task_transfer.py
│   ├── test_peer_discovery.py
│   └── test_gossip.py
│
├── tests/
├── README.md
├── requirements.txt
└── .gitignore
```

## Getting Started

### Prerequisites

- Python 3.10 or later
- Git

### Installation

**1. Clone the repository**

```bash
git clone https://github.com/supriya-j06/MeshWeaver.git
cd MeshWeaver
```

**2. Create a virtual environment**

```bash
python -m venv venv
```

**3. Activate the virtual environment**

Windows PowerShell:

```powershell
.\venv\Scripts\Activate.ps1
```

**4. Install dependencies**

```bash
python -m pip install -r requirements.txt
```

## Running the Project

### 1. UDP Peer Communication

Start the server:

```bash
python -m meshweaver.node server
```

Run the client in another terminal:

```bash
python -m meshweaver.node client
```

The client sends a PING message and receives a PONG response.

### 2. Function Serialization Test

```bash
python -m examples.test_serialization
```

### 3. TCP Task Transfer Test

Start the task server:

```bash
python -c "import asyncio; from meshweaver.task_network import start_task_server; asyncio.run(start_task_server())"
```

Run the test in another terminal:

```bash
python -m examples.test_task_transfer
```

### 4. Multi-Peer Discovery Test

```bash
python -m examples.test_peer_discovery
```

This test verifies peer discovery and exchange of peer information using three local nodes.

### 5. CPU and RAM Monitoring

```bash
python -m meshweaver.monitor
```

This displays the current CPU and memory utilization of the local system.

### 6. Gossip Protocol Test

```bash
python -m examples.test_gossip
```

This test starts three local peers and verifies resource information exchange between neighboring nodes at five-second intervals.

## Development Status

### Completed

- Asynchronous UDP peer communication.
- Function serialization and deserialization using cloudpickle.
- TCP-based task transfer and remote execution.
- Result transfer between peer processes.
- Connection timeout and unavailable-peer handling.
- Unique peer node identity generation.
- Kademlia-inspired routing table using XOR distance.
- UDP-based multi-peer discovery.
- Iterative peer discovery.
- CPU and RAM monitoring using psutil.
- Background Gossip Protocol for neighboring peer resource exchange.
- Three-node discovery and resource exchange testing.
- Regression testing of serialization and TCP task transfer.

### Upcoming Development

- Validation of a ten-node peer network.
- Execution and demonstration of complex mathematical or machine-learning functions across peers.
- Intelligent task scheduling based on CPU utilization.
- Routing tasks to the peer with the lowest reported CPU load.
- Heartbeat monitoring and node failure detection.
- Automatic task failure handling and rerouting.
- TLS/SSL-based secure peer communication.
- Cryptographic signatures for task execution requests.
- Live command-line monitoring dashboard using Rich.

## Security Note

MeshWeaver uses `cloudpickle` to deserialize transferred tasks. Deserializing untrusted data can execute arbitrary code. Therefore, the current implementation should only be used with trusted peers in a controlled environment.

Secure peer communication and cryptographic verification are planned for future development.

## Contributors

**Developed by:** Supriya

**Repository:** [MeshWeaver on GitHub](https://github.com/supriya-j06/MeshWeaver)

---

*Building decentralized task execution through asynchronous peer communication, dynamic discovery and resource sharing.*
