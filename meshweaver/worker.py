"""
Sandbox-ish worker process: python -m meshweaver.worker
stdin  : cloudpickle bytes of (func, args, kwargs)
stdout : b"O" + cloudpickle(result)   or   b"E" + utf-8 traceback
User prints are redirected to stderr so they cannot corrupt the result channel.
"""
import os
import sys
import traceback


def main():
    out = os.fdopen(os.dup(1), "wb")
    os.dup2(2, 1)
    sys.stdout = sys.stderr
    data = sys.stdin.buffer.read()
    try:
        import cloudpickle
        func, args, kwargs = cloudpickle.loads(data)
        result = func(*args, **kwargs)
        try:
            payload = b"O" + cloudpickle.dumps(result, protocol=4)
        except Exception as e:  # result not picklable
            payload = b"E" + f"result is not picklable: {e!r}".encode()
    except BaseException:
        payload = b"E" + traceback.format_exc().encode()
    out.write(payload)
    out.flush()


if __name__ == "__main__":
    main()
