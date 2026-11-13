
from meshweaver.task import (
    serialize_task,
    execute_serialized_task
)


def calculate_sum(a, b):
    return a + b


def multiply_numbers(a, b):
    return a * b


def greet_user(name):
    return f"Hello, {name}! Welcome to MeshWeaver."


def calculate_average(a, b, c):
    return (a + b + c) / 3


# Test 1: Addition
task1 = serialize_task(calculate_sum, 10, 20)
print("Addition:", execute_serialized_task(task1))


# Test 2: Multiplication
task2 = serialize_task(multiply_numbers, 5, 6)
print("Multiplication:", execute_serialized_task(task2))


# Test 3: String function
task3 = serialize_task(greet_user, "Supriya")
print("Greeting:", execute_serialized_task(task3))


# Test 4: Average
task4 = serialize_task(calculate_average, 10, 20, 30)
print("Average:", execute_serialized_task(task4))
