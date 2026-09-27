"""A path to a server that cannot be reached directly, opened by migkit.

A database behind a bastion, inside a private network the cloud only
forwards into (an SSM session, an IAP tunnel, a SQL proxy), or in a
cluster (a port-forward): each ends in a port on this machine, and every
connection and program of the run is pointed at it. An endpoint says how in
its `tunnel` option:

    target:
      host: 10.0.0.12            # as the far side of the tunnel sees it
      port: 5432
      tunnel: {ssh: bastion.example.com, user: ec2-user, key: ~/.ssh/id_ed25519}

    source:
      host: db.internal
      port: 3306
      tunnel:
        command: >-
          aws ssm start-session --target i-0abc --document-name
          AWS-StartPortForwardingSessionToRemoteHost --parameters
          host={host},portNumber={port},localPortNumber={local_port}

`ssh` runs the machine's own ssh, so its configuration - jump hosts, keys
in an agent, certificates - applies (`jump` adds hops); keys only, never a
password. `command` runs any forwarder with `{local_port}`, `{host}` and
`{port}` filled in. The tunnel is up only once its port answers; a tunnel
that dies is started again on the same port, and the connections it
carried are lost - the copy goes on a range at a time as after any lost
connection. Everything opened is closed when the run ends.

An SSH connection carries its channels through one window of about 2 MB,
so one tunnel moves at most that much a round trip: 20 MB/s at 100 ms, for
every worker of the run together. Where the bastion is far, several ssh
connections are opened and a splitter on this machine's end hands each new
connection to the next one - as many as the round trip to the bastion,
measured, says it takes to carry `AIM_MB_S`, never more than the run's
workers, and one where the bastion is near.
"""
import atexit
import os
import shlex
import signal
import socket
import subprocess
import tempfile
import threading
import time

_OPEN = {}
_LOCK = threading.Lock()


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _answers(port, wait):
    end = time.time() + wait
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(1)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.2)
    return False


#: what the legs of a far tunnel together should carry, MB/s
AIM_MB_S = 100
#: what one SSH connection's window carries a round trip, MB
WINDOW_MB = 2.0


def _round_trip(host, port, tries=3):
    """Seconds a TCP connection to (host, port) takes to open, the least of
    a few: one round trip on the path the tunnel will take. None where it
    does not open."""
    best = None
    for _ in range(tries):
        began = time.monotonic()
        try:
            with socket.create_connection((host, int(port)), timeout=5):
                took = time.monotonic() - began
        except OSError:
            continue
        best = took if best is None else min(best, took)
    return best


def legs_for(rtt, workers):
    """How many ssh connections carry a tunnel whose bastion is `rtt`
    seconds away: enough that their windows together carry `AIM_MB_S`,
    never more than the workers that will use them."""
    import math
    if not rtt or rtt <= 0:
        return 1
    each = WINDOW_MB / rtt
    return max(1, min(int(workers or 1), math.ceil(AIM_MB_S / each)))


class _Splitter:
    """This machine's end of a tunnel of several legs: each connection
    made to it goes on through the next leg, and bytes are passed both ways
    until either side ends."""

    def __init__(self, port, legs):
        self.legs, self.next, self.carried = legs, 0, [0] * len(legs)
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", port))
        self.sock.listen(256)
        self._lock = threading.Lock()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            with self._lock:
                at = self.next % len(self.legs)
                self.next += 1
                self.carried[at] += 1
            try:
                up = socket.create_connection(("127.0.0.1", self.legs[at]))
            except OSError:
                conn.close()
                continue
            for a, b in ((conn, up), (up, conn)):
                threading.Thread(target=_pump, args=(a, b),
                                 daemon=True).start()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def _pump(a, b):
    try:
        while True:
            data = a.recv(1 << 16)
            if not data:
                break
            b.sendall(data)
    except OSError:
        pass
    finally:
        try:
            b.shutdown(socket.SHUT_WR)
        except OSError:
            pass


class Tunnel:
    """One endpoint's tunnel: `open()` points the endpoint at this
    machine's end of it."""

    #: seconds a new tunnel has to start answering
    READY = 30

    def __init__(self, ep, spec, side, workers=1):
        self.ep, self.spec, self.side = ep, spec, side
        self.far = (ep.host, int(ep.port))
        # the port this process forwards from, and the port the endpoint is
        # pointed at: the same, but behind a splitter
        self.port = self.forward = _free_port()
        self.proc, self.said = None, None
        self.starts = 0
        self.workers = workers
        self.more = []
        self.splitter = None
        self._stop = threading.Event()

    def argv(self):
        spec = self.spec
        if isinstance(spec, str):
            spec = ({"command": spec} if not spec.startswith("ssh://")
                    else _ssh_url(spec))
        host, port = self.far
        if spec.get("command"):
            return shlex.split(str(spec["command"]).format(
                local_port=self.forward, host=host, port=port))
        if spec.get("ssh"):
            argv = ["ssh", "-N", "-L",
                    f"127.0.0.1:{self.forward}:{host}:{port}",
                    "-o", "ExitOnForwardFailure=yes",
                    "-o", "ServerAliveInterval=15",
                    "-o", "ServerAliveCountMax=3", "-o", "BatchMode=yes",
                    "-o", "StrictHostKeyChecking="
                    + str(spec.get("host_keys", "accept-new"))]
            if spec.get("known_hosts"):
                argv += ["-o", "UserKnownHostsFile="
                         + os.path.expanduser(str(spec["known_hosts"]))]
            if spec.get("key"):
                argv += ["-i", os.path.expanduser(str(spec["key"]))]
            if spec.get("port"):
                argv += ["-p", str(spec["port"])]
            jumps = spec.get("jump")
            if jumps:
                argv += ["-J", ",".join(jumps) if isinstance(jumps, list)
                         else str(jumps)]
            who = (f"{spec['user']}@" if spec.get("user") else "")
            return argv + [who + str(spec["ssh"])]
        raise SystemExit(f"{self.side}: a tunnel says `ssh` or `command`;"
                         f" this one says neither ({sorted(spec)})")

    def _start(self):
        self.said = tempfile.TemporaryFile()
        self.proc = subprocess.Popen(self.argv(), stdin=subprocess.DEVNULL,
                                     stdout=self.said, stderr=self.said,
                                     start_new_session=True)
        self.starts += 1

    def _why(self):
        try:
            self.said.seek(0)
            last = self.said.read().decode(errors="replace").strip()
        except Exception:  # noqa: BLE001
            last = ""
        return last.splitlines()[-1] if last else (
            f"exit code {self.proc.returncode}")

    def _ssh_prefix(self):
        """The ssh command line to the tunnel's own machine, the options
        the tunnel was opened with - None where it is not ssh."""
        spec = self.spec
        if isinstance(spec, str):
            spec = ({"command": spec} if not spec.startswith("ssh://")
                    else _ssh_url(spec))
        if not spec.get("ssh"):
            return None
        argv = ["ssh", "-o", "BatchMode=yes", "-o",
                "StrictHostKeyChecking="
                + str(spec.get("host_keys", "accept-new"))]
        if spec.get("known_hosts"):
            argv += ["-o", "UserKnownHostsFile="
                     + os.path.expanduser(str(spec["known_hosts"]))]
        if spec.get("key"):
            argv += ["-i", os.path.expanduser(str(spec["key"]))]
        if spec.get("port"):
            argv += ["-p", str(spec["port"])]
        if spec.get("jump"):
            jumps = spec["jump"]
            argv += ["-J", ",".join(jumps) if isinstance(jumps, list)
                     else str(jumps)]
        who = (f"{spec['user']}@" if spec.get("user") else "")
        return argv + [who + str(spec["ssh"])]

    def has(self, *programs):
        """Whether the tunnel's own machine - next to the database - has
        these programs, asked once."""
        known = self.__dict__.setdefault("_has", {})
        if programs not in known:
            prefix = self._ssh_prefix()
            ok = False
            if prefix:
                try:
                    got = subprocess.run(
                        prefix + ["--", "command -v " + " ".join(
                            shlex.quote(p) for p in programs)
                            + " >/dev/null 2>&1 && echo migkit-has"],
                        capture_output=True, text=True, timeout=30,
                        stdin=subprocess.DEVNULL)
                    ok = "migkit-has" in got.stdout
                except (OSError, subprocess.SubprocessError):
                    ok = False
            known[programs] = ok
        return known[programs]

    def there(self, command):
        """The argv that runs `command` (one shell line) on the tunnel's own
        machine, over ssh - None where the tunnel is not ssh."""
        prefix = self._ssh_prefix()
        return prefix + ["--", command] if prefix else None

    def _ssh_spec(self):
        spec = self.spec
        if isinstance(spec, str):
            spec = ({"command": spec} if not spec.startswith("ssh://")
                    else _ssh_url(spec))
        return spec if spec.get("ssh") and not spec.get("jump") else None

    def open(self):
        # far enough that one window caps the run: more ssh legs, each on a
        # port of its own, behind a splitter on the endpoint's port
        ssh = self._ssh_spec()
        n = legs_for(_round_trip(ssh["ssh"], ssh.get("port") or 22),
                     self.workers) if ssh else 1
        if n > 1:
            self.forward = _free_port()
        self._start()
        if not _answers(self.forward, self.READY) \
                or self.proc.poll() is not None:
            why = self._why() if self.proc.poll() is not None else \
                f"its port did not answer in {self.READY}s"
            self.close()
            raise SystemExit(
                f"the tunnel to the {self.side} ({self.far[0]}:"
                f"{self.far[1]}) did not come up: {why}")
        if n > 1:
            for _ in range(n - 1):
                leg = Tunnel(self.ep, self.spec, self.side)
                leg._start()
                if _answers(leg.forward, self.READY) \
                        and leg.proc.poll() is None:
                    threading.Thread(target=leg._keep, daemon=True).start()
                    self.more.append(leg)
                else:
                    leg.close()
            self.splitter = _Splitter(self.port, [self.forward] + [
                leg.forward for leg in self.more])
        self.ep.host, self.ep.port = "127.0.0.1", self.port
        # said as open: a range copied in a process of its own is handed the
        # endpoint as it is now, and must not open a tunnel from there
        self.options = self.ep.options
        self.ep.options = {**self.ep.options, "tunnel": None,
                           "tunnel_to": f"{self.far[0]}:{self.far[1]}"}
        threading.Thread(target=self._keep, daemon=True).start()
        return self

    def _keep(self):
        """Started again when it dies, on the same port, waiting longer
        each time it keeps dying."""
        wait = 1.0
        while not self._stop.wait(2.0):
            if self.proc.poll() is None:
                wait = 1.0
                continue
            if self._stop.wait(wait):
                return
            wait = min(60.0, wait * 2)
            try:
                self._start()
            except OSError:
                continue

    def close(self):
        self._stop.set()
        if self.splitter is not None:
            self.splitter.close()
        for leg in self.more:
            leg.close()
        if self.proc is not None and self.proc.poll() is None:
            try:
                os.killpg(self.proc.pid, signal.SIGTERM)
                self.proc.wait(timeout=10)
            except Exception:  # noqa: BLE001 - it is going anyway
                try:
                    self.proc.kill()
                except Exception:  # noqa: BLE001
                    pass
        self.ep.host, self.ep.port = self.far
        if getattr(self, "options", None) is not None:
            self.ep.options = self.options


def _ssh_url(url):
    """`ssh://user@host:port` as the parts of an `ssh` tunnel."""
    rest = url[len("ssh://"):]
    user, _, hostport = rest.rpartition("@")
    host, _, port = hostport.partition(":")
    out = {"ssh": host}
    if user:
        out["user"] = user
    if port:
        out["port"] = int(port)
    return out


def through(hop):
    """Every endpoint of the hop that names a tunnel, pointed at its open
    end; opened once a run, however many engines are made for the hop."""
    for side, ep in (("source", hop.source), ("target", hop.target)):
        spec = (getattr(ep, "options", None) or {}).get("tunnel")
        if not spec:
            continue
        with _LOCK:
            if id(ep) in _OPEN:
                continue
            _OPEN[id(ep)] = Tunnel(ep, spec, side,
                                   int(getattr(hop, "workers", 1) or 1)
                                   ).open()


def of(ep):
    """The open tunnel an endpoint goes through, or None."""
    with _LOCK:
        for t in _OPEN.values():
            if t.ep is ep:
                return t
    return None


def close_all():
    with _LOCK:
        for t in list(_OPEN.values()):
            t.close()
        _OPEN.clear()


atexit.register(close_all)
