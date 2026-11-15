
import unittest

from meshweaver.task import serialize_task, execute_serialized_task


def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


def greet(name):
    return f"Hello, {name}!"


class TestTaskSerialization(unittest.TestCase):

    def test_addition(self):
        data = serialize_task(add, 10, 20)
        result = execute_serialized_task(data)
        self.assertEqual(result, 30)

    def test_multiplication(self):
        data = serialize_task(multiply, 5, 6)
        result = execute_serialized_task(data)
        self.assertEqual(result, 30)

    def test_greeting(self):
        data = serialize_task(greet, "MeshWeaver")
        result = execute_serialized_task(data)
        self.assertEqual(result, "Hello, MeshWeaver!")


if __name__ == "__main__":
    unittest.main()
