"""A link with a round trip, for the tests that count what one costs.

A TCP proxy on 127.0.0.1 that holds every chunk `delay` seconds each way
before passing it on, in order - a round trip of twice that, and no limit
on bandwidth. Used where the machine gives no other way to add one
(`tc` and `pfctl` need root).
"""
import heapq
import socket
import threading
import time


class SlowLink:
    def __init__(self, port, delay, host="127.0.0.1"):
        self.upstream, self.delay = (host, port), delay
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(64)
        self.port = self.listener.getsockname()[1]
        self.socks, self.closed = [], False
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while not self.closed:
            try:
                near, _ = self.listener.accept()
            except OSError:
                return
            far = socket.create_connection(self.upstream)
            for s in (near, far):
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.socks += [near, far]
            self._pump(near, far)
            self._pump(far, near)

    def _pump(self, src, dst):
        held, cv, seq = [], threading.Condition(), [0]

        def read():
            while True:
                try:
                    data = src.recv(1 << 16)
                except OSError:
                    data = b""
                with cv:
                    seq[0] += 1
                    heapq.heappush(held, (time.monotonic() + self.delay,
                                          seq[0], data))
                    cv.notify()
                if not data:
                    return

        def write():
            while True:
                with cv:
                    while not held:
                        cv.wait()
                    due, _, data = held[0]
                    wait = due - time.monotonic()
                    if wait > 0:
                        cv.wait(wait)
                        continue
                    heapq.heappop(held)
                if not data:
                    try:
                        dst.shutdown(socket.SHUT_WR)
                    except OSError:
                        pass
                    return
                try:
                    dst.sendall(data)
                except OSError:
                    return
        threading.Thread(target=read, daemon=True).start()
        threading.Thread(target=write, daemon=True).start()

    def close(self):
        self.closed = True
        for s in [self.listener] + self.socks:
            try:
                s.close()
            except OSError:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
