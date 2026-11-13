
import cloudpickle


def serialize_task(function, *args, **kwargs):
    """
    Serialize a Python function and its arguments
    so they can be transferred to another peer.
    """

    task_data = {
        "function": function,
        "args": args,
        "kwargs": kwargs
    }

    serialized_data = cloudpickle.dumps(task_data)

    print("Task serialized successfully.")

    return serialized_data


def execute_serialized_task(serialized_data):
    """
    Deserialize and execute a received task.
    """

    task_data = cloudpickle.loads(serialized_data)

    function = task_data["function"]
    args = task_data["args"]
    kwargs = task_data["kwargs"]

    if not callable(function):
        raise TypeError("Received task is not callable.")

    result = function(*args, **kwargs)

    print("Task executed successfully.")

    return result
