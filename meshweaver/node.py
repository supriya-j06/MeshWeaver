
import asyncio
import argparse

from meshweaver.networking import start_udp_server, ping_peer


async def run_server(host, port):
    """Start a MeshWeaver peer node."""

    transport = await start_udp_server(host, port)

    print(f"\nMeshWeaver node running at {host}:{port}")
    print("Waiting for incoming peer connections...")
    print("Press CTRL+C to stop.\n")

    try:
        await asyncio.Event().wait()
    finally:
        transport.close()


async def run_client(host, port):
    """Connect to another peer and test communication."""

    print(f"\nConnecting to peer {host}:{port}")

    try:
        response = await ping_peer(host, port)

        if response == "PONG":
            print("\nSUCCESS: Peer communication established!")
        else:
            print("\nUnexpected response:", response)

    except asyncio.TimeoutError:
        print("\nERROR: Peer did not respond within 5 seconds.")


def main():
    parser = argparse.ArgumentParser(
        description="MeshWeaver P2P Task Broker"
    )

    subparsers = parser.add_subparsers(
        dest="mode",
        required=True
    )

    server_parser = subparsers.add_parser("server")
    server_parser.add_argument("--host", default="127.0.0.1")
    server_parser.add_argument("--port", type=int, default=8888)

    client_parser = subparsers.add_parser("client")
    client_parser.add_argument("--host", default="127.0.0.1")
    client_parser.add_argument("--port", type=int, default=8888)

    args = parser.parse_args()

    if args.mode == "server":
        asyncio.run(run_server(args.host, args.port))

    elif args.mode == "client":
        asyncio.run(run_client(args.host, args.port))


if __name__ == "__main__":
    main()
