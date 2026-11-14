# MeshWeaver

**A Zero-Dependency Peer-to-Peer Asynchronous Task Broker built with Python.**

MeshWeaver is a decentralized task execution project that enables connected peer nodes to exchange and execute tasks without depending on centralized message brokers such as Redis or RabbitMQ.

## Overview

The project uses asynchronous networking to establish peer communication, transfer serialized Python functions, execute tasks on a receiving peer, and return results to the requester.

## Features

- UDP-based peer communication using PING/PONG messages.
- Asynchronous networking using Python `asyncio`.
- Function serialization using `cloudpickle`.
- TCP-based task transfer between peer processes.
- Remote task execution and result transfer.
- Basic detection of unavailable peers.

## Tech Stack

| Technology | Purpose |
|---|---|
| Python | Core programming language |
| asyncio | Asynchronous networking |
| UDP | Peer communication |
| TCP | Task transfer |
| cloudpickle | Function serialization |
| Git & GitHub | Version control |

## Architecture

```text
       Client Peer
            |
            | Serialize Task
            v
       TCP Connection
            |
            v
       Receiving Peer
            |
            | Execute Task
            v
       Return Result
            |
            v
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
│   └── task_network.py
│
├── examples/
│   ├── test_serialization.py
│   └── test_task_transfer.py
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
python examples/test_serialization.py
```

### 3. TCP Task Transfer Test

Start the task server:

```bash
python -c "import asyncio; from meshweaver.task_network import start_task_server; asyncio.run(start_task_server())"
```

Run the test in another terminal:

```bash
python examples/test_task_transfer.py
```

## Development Status

**Completed:**
- Basic UDP peer communication.
- Function serialization and execution.
- TCP task transfer.
- Result transfer between peer processes.
- Basic unavailable-peer handling.

**Planned:**
- Kademlia-based peer discovery.
- CPU and RAM resource sharing.
- Intelligent task assignment.
- Heartbeat monitoring.
- Automatic task reassignment.
- Secure peer communication.
- Command-line monitoring dashboard.

## Security Note

MeshWeaver uses `cloudpickle` to deserialize transferred tasks. Deserializing untrusted data can execute arbitrary code. Use this project only with trusted peers in a controlled environment.

## Contributors

**Developed by:** Supriya

**Repository:** [MeshWeaver on GitHub](https://github.com/supriya-j06/MeshWeaver)

---

*Building decentralized task execution through asynchronous peer-to-peer communication.*