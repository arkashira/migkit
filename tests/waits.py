"""One way for a test to wait for something that happens on its own time.

144 bare `time.sleep` calls waited a fixed time for a server, a tail or a
broker instead of for its sign (audit T9): on a loaded machine a flake, on
an idle one a waste. `until` polls the sign itself, from a short pause that
doubles up to a ceiling, and when time runs out says what never came and
the last error the sign raised on the way.
"""
import socket
import time


class NeverCame(AssertionError):
    """What a test waited for did not come in its time. An AssertionError,
    so a wait in vain fails the test rather than erroring or skipping it."""


def until(condition, timeout, what, every=0.1, most=2.0):
    """Call `condition()` until it returns something truthy and return that.

    An exception from `condition` counts as "not yet" (a server still
    starting refuses connections); the last one is named if the time runs
    out. The pause starts at `every` and doubles up to `most`, so a sign
    that comes at once is seen at once and a slow one costs few polls.
    """
    end = time.monotonic() + timeout
    pause, last = every, None
    while True:
        try:
            got = condition()
        except Exception as e:  # noqa: BLE001 - not there yet
            got, last = None, e
        if got:
            return got
        left = end - time.monotonic()
        if left <= 0:
            why = f" (last: {type(last).__name__}: {last})" if last else ""
            raise NeverCame(f"{what}: not within {timeout:g}s{why}")
        time.sleep(min(pause, left))
        pause = min(pause * 2, most)


def listening(port, host="127.0.0.1"):
    """Whether something accepts a connection on `port`. Only a first
    sign: a forwarded port accepts before the server behind it answers,
    so a server is ready by its own handshake, never by this alone."""
    with socket.socket() as s:
        s.settimeout(1)
        return s.connect_ex((host, port)) == 0
