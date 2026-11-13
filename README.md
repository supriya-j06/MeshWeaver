# MeshWeaver
A decentralized peer-to-peer asynchronous task broker built using Python, asyncio and Kademlia DHT.



# MeshWeaver
### Zero-Dependency P2P Async Task Broker

**Domain:** Distributed Systems & Edge Computing

MeshWeaver is a decentralized peer-to-peer asynchronous task broker developed using Python. It aims to enable multiple peer nodes to communicate, exchange tasks and execute workloads without depending on a centralized message broker.

The project explores asynchronous networking, distributed task execution, task serialization, peer discovery and fault tolerance.

## Project Objectives

- Establish communication between independent peer nodes.
- Implement asynchronous networking using Python asyncio.
- Transfer Python tasks between peers.
- Execute received tasks remotely.
- Build a decentralized task distribution system.
- Explore fault tolerance and secure peer communication.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Core development |
| asyncio | Asynchronous programming |
| UDP / TCP Sockets | Peer communication |
| cloudpickle | Task serialization |
| Rich | Terminal dashboard (planned) |
| psutil | CPU/RAM monitoring (planned) |
| Kademlia DHT | Peer discovery (planned) |

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
│
├── venv/
│
├── main.py
├── requirements.txt
├── README.md
└── .gitignore
```

## Implemented Features – Week 1

### 1. Asynchronous UDP Networking

- Implemented an asynchronous UDP server using asyncio.
- Developed a UDP client for peer communication.
- Implemented PING and PONG message exchange.
- Successfully tested communication between two Python processes.

### 2. Task Serialization

- Implemented Python function serialization using cloudpickle.
- Serialized functions along with their arguments.
- Implemented deserialization and execution of serialized tasks.
- Tested arithmetic operations, string functions and multiple arguments.

### 3. Remote Task Execution

- Implemented TCP socket communication between peers.
- Transferred serialized tasks between sender and receiver.
- Executed received tasks on the receiving peer.
- Returned execution results to the requesting peer.

### 4. Basic Connection Error Handling

- Tested successful task transfer.
- Tested communication with an unavailable peer.
- Verified connection failure detection.

## Installation and Setup

### Prerequisites

- Python 3.10 or later
- Git
- Visual Studio Code

### Step 1: Clone the repository

```bash
git clone https://github.com/supriya-j06/MeshWeaver.git
cd MeshWeaver
```

### Step 2: Switch to the development branch

```bash
git switch supriya-dev
```

### Step 3: Create a virtual environment

```bash
python -m venv venv
```

### Step 4: Activate the environment

Windows PowerShell:

```powershell
.\venv\Scripts\Activate.ps1
```

### Step 5: Install dependencies

```bash
pip install -r requirements.txt
```

## Running the Project

### UDP Server

```bash
python -m meshweaver.node server
```

### UDP Client

Open another terminal:

```bash
python -m meshweaver.node client
```

### Test Task Serialization

```bash
python -m examples.test_serialization
```

### Test Remote Task Execution

Start the task server:

```bash
python -c "import asyncio; from meshweaver.task_network import start_task_server; asyncio.run(start_task_server())"
```

In another terminal:

```bash
python -m examples.test_task_transfer
```

## Week 1 Testing Results

| Test Case | Expected Result | Status |
|---|---|---|
| UDP server startup | Server starts successfully | Passed |
| UDP PING/PONG | PONG received | Passed |
| Function serialization | Function serialized | Passed |
| Arithmetic execution | Correct result returned | Passed |
| String function execution | Correct output returned | Passed |
| Remote task transfer | Result 300 received | Passed |
| Unavailable peer | Connection failure detected | Passed |

## Future Development

- Kademlia Distributed Hash Table (DHT)
- Peer discovery and network formation
- Gossip protocol for CPU/RAM sharing
- Intelligent task routing
- Heartbeat monitoring
- Fault tolerance and task reassignment
- TLS/SSL communication and cryptographic signatures
- Live terminal dashboard using Rich

## Security Note

The current task-transfer prototype uses cloudpickle for trusted local testing. Deserializing untrusted data can execute arbitrary code. Authentication, authorization and secure execution controls are required before deployment across untrusted networks.

## Project Status

**Current Phase:** Week 1 – Core Networking and Task Serialization

MeshWeaver is under active development as part of the Advanced Python Engineering internship project at Infotact Solutions.

---

**Developed by:** Supriya and Team