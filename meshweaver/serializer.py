"""Task serializer: cloudpickle turns a Python function + arguments into bytes."""
import cloudpickle


def pack_task(func, args, kwargs) -> bytes:
    return cloudpickle.dumps((func, tuple(args), dict(kwargs or {})), protocol=4)


def func_name(func) -> str:
    return getattr(func, "__qualname__", None) or getattr(func, "__name__", None) or repr(func)


def unpack_result(data: bytes):
    return cloudpickle.loads(data)
