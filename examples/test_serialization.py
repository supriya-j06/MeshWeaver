
from meshweaver.task import (
    serialize_task,
    execute_serialized_task
)


def calculate_sum(a, b):
    return a + b


# Serialize the function and its arguments
serialized_task = serialize_task(
    calculate_sum,
    10,
    20
)

print("Serialized data size:", len(serialized_task), "bytes")


# Execute the serialized task
result = execute_serialized_task(serialized_task)

print("Task Result:", result)
