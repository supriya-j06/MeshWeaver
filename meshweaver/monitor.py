
import psutil


def get_resource_usage():
    """Collect CPU and RAM usage of the current node."""

    cpu_percent = psutil.cpu_percent(interval=1)

    memory = psutil.virtual_memory()

    return {
        "cpu_percent": cpu_percent,
        "ram_percent": memory.percent,
        "ram_available_mb": round(
            memory.available / (1024 * 1024), 2
        ),
        "ram_total_mb": round(
            memory.total / (1024 * 1024), 2
        )
    }


if __name__ == "__main__":
    usage = get_resource_usage()

    print("MeshWeaver Resource Monitoring")
    print("--------------------------------")
    print(f"CPU Usage: {usage['cpu_percent']}%")
    print(f"RAM Usage: {usage['ram_percent']}%")
    print(f"Available RAM: {usage['ram_available_mb']} MB")
    print(f"Total RAM: {usage['ram_total_mb']} MB")
