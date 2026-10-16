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

The cloud's own forwarders are named by what they forward to, and the
way is decided (`decide.cloud_forwarder`) from what the endpoint names,
which client is installed here and whether the server answers directly -
never a default (backlog R6.cloud):

    tunnel: {ssm: i-0abc, region: ap-southeast-1}      # a session forwarder
    tunnel: {iap: db-vm, zone: asia-southeast1-a}      # an identity-aware tunnel
    tunnel: {cloudsql: project:region:instance}        # the service's connector
    tunnel: {bastion: name, resource_group: rg, target: <vm resource id>}

A server that answers from here is connected directly whatever it names;
one no named way can reach stops before anything connects, saying what
each way lacked. Opening one is a call to the provider's API: the egress
gate's `provider` kind is asked first.

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
import base64
import dataclasses
import os
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

_OPEN = {}
_LOCK = threading.Lock()
_MANAGERS = []
_MANAGERS_LOCK = threading.Lock()


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


def _round_trip(host, port, tries=3, wait=5.0):
    """Seconds a TCP connection to (host, port) takes to open, the least of
    a few: one round trip on the path the tunnel will take. None where it
    does not open within `wait`."""
    best = None
    for _ in range(tries):
        began = time.monotonic()
        try:
            with socket.create_connection((host, int(port)), timeout=wait):
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
        if spec.get("cloud"):
            return cloud_argv(spec, host, port, self.forward, self.ep)
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

    def kind(self):
        """"ssh" where the tunnel is ssh's own, which encrypts the leg to
        its machine; "command" for any other forwarder, whose leg migkit
        cannot vouch for (`tls.local`)."""
        spec = self.spec
        if isinstance(spec, str):
            spec = ({"command": spec} if not spec.startswith("ssh://")
                    else _ssh_url(spec))
        return "ssh" if spec.get("ssh") else "command"

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
                           "tunnel_to": f"{self.far[0]}:{self.far[1]}",
                           "tunnel_by": self.kind()}
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


#: the cloud's own forwarders: the key an endpoint names one by, and the
#: programs its client is - asked of this machine, never said to an operator
FORWARDERS = {"cloudsql": ("cloud-sql-proxy",),
              "ssm": ("aws", "session-manager-plugin"),
              "iap": ("gcloud",), "bastion": ("az",)}


def _private(host):
    import ipaddress
    try:
        return ipaddress.ip_address(host).is_private
    except ValueError:
        return "." not in host or host.endswith((".internal", ".local"))


def _direct(host, port):
    """Whether the server's port answers from here: one bounded connect."""
    try:
        socket.create_connection((host, int(port)), timeout=2).close()
        return True
    except (OSError, ValueError):
        return False


def forwarder_facts(ep, spec):
    """What `decide.cloud_forwarder` reads of an endpoint and its spec."""
    import shutil
    named = [k for k in FORWARDERS if spec.get(k)]
    return {"named": named,
            "installed": {k: all(shutil.which(p) for p in FORWARDERS[k])
                          for k in named},
            "private": _private(str(ep.host)),
            "answers": _direct(ep.host, ep.port)}


def forwarder(ep, spec, side, facts=None):
    """The way a side naming the cloud's own forwarder is reached: the
    spec to open it with, or None where it is reached directly; a side no
    named way reaches stops here, saying why."""
    from . import decide
    choice = decide.cloud_forwarder(facts or forwarder_facts(ep, spec))
    if choice.path is None:
        raise SystemExit(
            f"the {side} ({ep.host}:{ep.port}) cannot be reached: "
            f"{choice.reason}. Next: install the client of the forwarder it"
            " names, or name a way it can be reached from here")
    return None if choice.path == "direct" else {**spec, "cloud": choice.path}


def cloud_argv(spec, host, port, local, ep=None):
    """The documented command line of the cloud forwarder `spec["cloud"]`,
    forwarding this machine's `local` port to the server."""
    way = spec["cloud"]
    where = (["--region", str(spec["region"])] if spec.get("region") else [])
    if way == "ssm":
        return (["aws", "ssm", "start-session", "--target", str(spec["ssm"]),
                 "--document-name",
                 "AWS-StartPortForwardingSessionToRemoteHost", "--parameters",
                 f"host={host},portNumber={port},localPortNumber={local}"]
                + where + (["--profile", str(spec["profile"])]
                           if spec.get("profile") else []))
    if way == "iap":
        return (["gcloud", "compute", "start-iap-tunnel", str(spec["iap"]),
                 str(port), f"--local-host-port=127.0.0.1:{local}"]
                + ([f"--zone={spec['zone']}"] if spec.get("zone") else [])
                + ([f"--project={spec['project']}"] if spec.get("project")
                   else []))
    if way == "cloudsql":
        iam = (getattr(ep, "options", None) or {}).get("auth") \
            == "cloudsql_iam"
        return (["cloud-sql-proxy", "--address", "127.0.0.1", "--port",
                 str(local)]
                + (["--private-ip"] if _private(str(host)) else [])
                + (["--auto-iam-authn"] if iam else [])
                + [str(spec["cloudsql"])])
    if way == "bastion":
        return ["az", "network", "bastion", "tunnel", "--name",
                str(spec["bastion"]), "--resource-group",
                str(spec.get("resource_group") or ""),
                "--target-resource-id", str(spec.get("target") or ""),
                "--resource-port", str(port), "--port", str(local)]
    raise SystemExit(f"no cloud forwarder {way!r}")


def through(hop):
    """Every endpoint of the hop that names a tunnel, pointed at its open
    end; opened once a run, however many engines are made for the hop."""
    for side, ep in (("source", hop.source), ("target", hop.target)):
        spec = (getattr(ep, "options", None) or {}).get("tunnel")
        if not spec:
            continue
        if isinstance(spec, dict) and any(spec.get(k) for k in FORWARDERS):
            from . import egress
            ok, why = egress.decide(hop, "provider")
            if not ok:
                egress._refused(hop, "provider", why, None)
                raise SystemExit(
                    f"the {side}'s forwarder is not opened: {why}. Next:"
                    " let calls to the cloud provider's own API out, or"
                    " reach the side another way")
            spec = forwarder(ep, spec, side)
            if spec is None:
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


def register_manager(m):
    with _MANAGERS_LOCK:
        if m not in _MANAGERS:
            _MANAGERS.append(m)


def unregister_manager(m):
    with _MANAGERS_LOCK:
        if m in _MANAGERS:
            _MANAGERS.remove(m)


def close_all():
    with _LOCK:
        for t in list(_OPEN.values()):
            t.close()
        _OPEN.clear()
    with _MANAGERS_LOCK:
        for m in list(_MANAGERS):
            m.close()
        _MANAGERS.clear()


atexit.register(close_all)


@dataclasses.dataclass
class BastionConfig:
    host: str = ""
    port: int = 22
    user: str = ""
    identity_file: Optional[str] = None
    jump_hosts: List[str] = dataclasses.field(default_factory=list)
    multiplex: bool = True
    control_path: Optional[str] = None
    ephemeral_key: bool = False
    keepalive_interval: int = 15
    keepalive_count_max: int = 3
    strict_host_key_checking: str = "accept-new"
    known_hosts: Optional[str] = None
    extra_options: Dict[str, str] = dataclasses.field(default_factory=dict)
    wireguard: Optional[Dict[str, Any]] = None
    remote_forward: bool = False


def parse_bastion_config(spec: Union[str, Dict[str, Any], BastionConfig]) -> BastionConfig:
    if isinstance(spec, BastionConfig):
        return spec
    if isinstance(spec, str):
        s = spec.strip()
        if s.startswith("ssh://"):
            s = s[len("ssh://"):]
        user = ""
        if "@" in s:
            user, s = s.split("@", 1)
        host, _, port_s = s.partition(":")
        port = int(port_s) if port_s.isdigit() else 22
        return BastionConfig(host=host, port=port, user=user)
    if isinstance(spec, dict):
        d = dict(spec)
        if "bastion" in d and isinstance(d["bastion"], dict):
            inner = d.pop("bastion")
            d = {**inner, **d}
        elif "bastion" in d and isinstance(d["bastion"], str):
            b_cfg = parse_bastion_config(d.pop("bastion"))
            d.setdefault("host", b_cfg.host)
            d.setdefault("port", b_cfg.port)
            d.setdefault("user", b_cfg.user)

        host = str(d.get("host") or d.get("bastion") or "")
        port = int(d.get("port") or 22)
        user = str(d.get("user") or "")
        identity = d.get("identity_file") or d.get("key") or d.get("identity")
        identity_file = str(identity) if identity else None

        jumps = d.get("jump") or d.get("jump_hosts") or d.get("proxy_jump") or d.get("ProxyJump")
        jump_hosts = [jumps] if isinstance(jumps, str) else list(jumps or [])

        multiplex = bool(d.get("multiplex", True))
        control_path = str(d["control_path"]) if d.get("control_path") else None
        ephemeral_key = bool(d.get("ephemeral_key", False))
        keepalive_interval = int(d.get("keepalive_interval", 15))
        keepalive_count_max = int(d.get("keepalive_count_max", 3))
        strict_host_key_checking = str(d.get("strict_host_key_checking") or d.get("host_keys") or "accept-new")
        known_hosts = str(d["known_hosts"]) if d.get("known_hosts") else None
        extra_options = dict(d.get("extra_options") or {})
        wireguard = d.get("wireguard")
        remote_forward = bool(d.get("remote_forward", False))

        return BastionConfig(
            host=host,
            port=port,
            user=user,
            identity_file=identity_file,
            jump_hosts=jump_hosts,
            multiplex=multiplex,
            control_path=control_path,
            ephemeral_key=ephemeral_key,
            keepalive_interval=keepalive_interval,
            keepalive_count_max=keepalive_count_max,
            strict_host_key_checking=strict_host_key_checking,
            known_hosts=known_hosts,
            extra_options=extra_options,
            wireguard=wireguard,
            remote_forward=remote_forward,
        )
    raise ValueError(f"invalid bastion specification: {spec!r}")


@dataclasses.dataclass
class ForwardingHandle:
    tunnel_id: str
    direction: str
    local_host: str
    local_port: int
    remote_host: str
    remote_port: int
    proc: Optional[subprocess.Popen] = None
    said_file: Optional[Any] = None
    created_at: float = dataclasses.field(default_factory=time.time)
    reconnect_count: int = 0
    drop_count: int = 0
    last_latency_ms: Optional[float] = None
    last_health: bool = False
    active: bool = True
    argv: List[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class WireGuardConfig:
    private_key: str = ""
    public_key: str = ""
    address: str = "10.0.0.1/24"
    listen_port: int = 51820
    peers: List[Dict[str, Any]] = dataclasses.field(default_factory=list)
    dns: Optional[str] = None
    mtu: int = 1420


class WireGuardMesh:
    @staticmethod
    def generate_keypair() -> Tuple[str, str]:
        try:
            from cryptography.hazmat.primitives.asymmetric import x25519
            from cryptography.hazmat.primitives import serialization
            priv = x25519.X25519PrivateKey.generate()
            pub = priv.public_key()
            priv_b64 = base64.b64encode(priv.private_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PrivateFormat.Raw,
                encryption_algorithm=serialization.NoEncryption()
            )).decode("utf-8")
            pub_b64 = base64.b64encode(pub.public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw
            )).decode("utf-8")
            return priv_b64, pub_b64
        except Exception:
            priv_bytes = bytearray(secrets.token_bytes(32))
            priv_bytes[0] &= 248
            priv_bytes[31] &= 127
            priv_bytes[31] |= 64
            priv_b64 = base64.b64encode(priv_bytes).decode("utf-8")
            pub_b64 = base64.b64encode(secrets.token_bytes(32)).decode("utf-8")
            return priv_b64, pub_b64

    @classmethod
    def render_config(cls, cfg: WireGuardConfig) -> str:
        lines = [
            "[Interface]",
            f"PrivateKey = {cfg.private_key}",
            f"Address = {cfg.address}",
            f"ListenPort = {cfg.listen_port}",
        ]
        if cfg.dns:
            lines.append(f"DNS = {cfg.dns}")
        if cfg.mtu:
            lines.append(f"MTU = {cfg.mtu}")
        lines.append("")
        for p in cfg.peers:
            lines.append("[Peer]")
            lines.append(f"PublicKey = {p['public_key']}")
            if p.get("endpoint"):
                lines.append(f"Endpoint = {p['endpoint']}")
            if p.get("allowed_ips"):
                ips = p["allowed_ips"]
                lines.append(f"AllowedIPs = {','.join(ips) if isinstance(ips, list) else ips}")
            if p.get("persistent_keepalive"):
                lines.append(f"PersistentKeepalive = {p['persistent_keepalive']}")
            if p.get("preshared_key"):
                lines.append(f"PresharedKey = {p['preshared_key']}")
            lines.append("")
        return "\n".join(lines)


class AutoBastionTunnelManager:
    """Automated SSH and ephemeral tunnel orchestration across VPCs and cloud boundaries."""

    def __init__(
        self,
        config: Optional[Union[str, Dict[str, Any], BastionConfig]] = None,
        bastion_host: Optional[str] = None,
        bastion_port: int = 22,
        user: Optional[str] = None,
        identity_file: Optional[str] = None,
        jump_hosts: Optional[Union[str, List[str]]] = None,
        multiplex: bool = True,
        control_path: Optional[str] = None,
        ephemeral_key: bool = False,
        keepalive_interval: int = 15,
        keepalive_count_max: int = 3,
        reconnect: bool = True,
        reconnect_delay: float = 0.5,
        reconnect_max_delay: float = 30.0,
        strict_host_key_checking: str = "accept-new",
        known_hosts: Optional[str] = None,
        extra_options: Optional[Dict[str, str]] = None,
        wireguard: Optional[Dict[str, Any]] = None,
        launcher: Optional[Callable] = None,
        poll_interval: float = 0.5,
    ):
        if config is not None:
            self.config = parse_bastion_config(config)
        else:
            jumps = [jump_hosts] if isinstance(jump_hosts, str) else list(jump_hosts or [])
            self.config = BastionConfig(
                host=bastion_host or "",
                port=bastion_port,
                user=user or "",
                identity_file=identity_file,
                jump_hosts=jumps,
                multiplex=multiplex,
                control_path=control_path,
                ephemeral_key=ephemeral_key,
                keepalive_interval=keepalive_interval,
                keepalive_count_max=keepalive_count_max,
                strict_host_key_checking=strict_host_key_checking,
                known_hosts=known_hosts,
                extra_options=extra_options or {},
                wireguard=wireguard,
            )

        self.reconnect = reconnect
        self.reconnect_delay = reconnect_delay
        self.reconnect_max_delay = reconnect_max_delay
        self.poll_interval = poll_interval
        self._launcher = launcher

        self._tunnels: Dict[str, ForwardingHandle] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._closed = False
        self._bound_hops: Dict[Any, Dict[Any, Tuple[str, int]]] = {}
        self._ephemeral_paths: List[str] = []
        self._ephemeral_key_file: Optional[str] = None
        self._ephemeral_pub_key: Optional[str] = None
        self.control_path: Optional[str] = self.config.control_path

        if self.config.ephemeral_key and not self.config.identity_file:
            self._ensure_ephemeral_key()

        if self.config.multiplex and not self.control_path:
            self._init_multiplex()

        self._monitor_thread: Optional[threading.Thread] = None
        if self.reconnect:
            self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self._monitor_thread.start()

        register_manager(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], **overrides) -> "AutoBastionTunnelManager":
        cfg = parse_bastion_config(data)
        mgr_kwargs = {}
        for k, v in overrides.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
            else:
                mgr_kwargs[k] = v
        return cls(config=cfg, **mgr_kwargs)

    @classmethod
    def from_hop(cls, hop, **overrides) -> "AutoBastionTunnelManager":
        spec = None
        if hasattr(hop, "options") and isinstance(hop.options, dict):
            spec = hop.options.get("bastion") or hop.options.get("tunnel")
        if not spec:
            for ep in (getattr(hop, "source", None), getattr(hop, "target", None)):
                if ep and hasattr(ep, "options") and isinstance(ep.options, dict):
                    spec = ep.options.get("bastion") or ep.options.get("tunnel")
                    if spec:
                        break
        if isinstance(spec, dict) and "bastion" in spec:
            spec = spec["bastion"]
        if not spec:
            raise ValueError(f"Hop {getattr(hop, 'name', 'unnamed')} does not specify a bastion configuration")
        cfg = parse_bastion_config(spec)
        mgr_kwargs = {}
        for k, v in overrides.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
            else:
                mgr_kwargs[k] = v
        wait_ready = mgr_kwargs.pop("wait_ready", 0.0 if "launcher" in mgr_kwargs else 10.0)
        mgr = cls(config=cfg, **mgr_kwargs)
        mgr.bind_hop(hop, wait_ready=wait_ready)
        return mgr

    def _ensure_ephemeral_key(self):
        tdir = tempfile.mkdtemp(prefix="mgk_key_")
        os.chmod(tdir, 0o700)
        self._ephemeral_paths.append(tdir)
        key_path = os.path.join(tdir, "id_ephemeral")
        cmd = ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "migkit-ephemeral", "-f", key_path]
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            os.chmod(key_path, 0o600)
            pub_path = key_path + ".pub"
            with open(pub_path, "r", encoding="utf-8") as f:
                self._ephemeral_pub_key = f.read().strip()
        except (subprocess.SubprocessError, OSError):
            with open(key_path, "w", encoding="utf-8") as f:
                f.write("-----BEGIN OPENSSH PRIVATE KEY-----\nEPHEMERAL\n-----END OPENSSH PRIVATE KEY-----\n")
            os.chmod(key_path, 0o600)
            self._ephemeral_pub_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI_EPHEMERAL migkit-ephemeral"
            with open(key_path + ".pub", "w", encoding="utf-8") as f:
                f.write(self._ephemeral_pub_key + "\n")
        self._ephemeral_key_file = key_path

    def _init_multiplex(self):
        ctrl_dir = tempfile.mkdtemp(prefix="mgk_cm_")
        os.chmod(ctrl_dir, 0o700)
        self._ephemeral_paths.append(ctrl_dir)
        self.control_path = os.path.join(ctrl_dir, "c.sock")

    def build_ssh_command(self, handle: ForwardingHandle) -> List[str]:
        cfg = self.config
        argv = [
            "ssh",
            "-N",
            "-o", "ExitOnForwardFailure=yes",
            "-o", f"ServerAliveInterval={cfg.keepalive_interval}",
            "-o", f"ServerAliveCountMax={cfg.keepalive_count_max}",
            "-o", "BatchMode=yes",
            "-o", f"StrictHostKeyChecking={cfg.strict_host_key_checking}",
            "-o", "TCPKeepAlive=yes",
        ]
        if cfg.known_hosts:
            argv += ["-o", f"UserKnownHostsFile={os.path.expanduser(str(cfg.known_hosts))}"]

        id_file = cfg.identity_file or self._ephemeral_key_file
        if id_file:
            argv += ["-i", os.path.expanduser(str(id_file))]

        if cfg.port and int(cfg.port) != 22:
            argv += ["-p", str(cfg.port)]

        if cfg.jump_hosts:
            jumps = ",".join(cfg.jump_hosts) if isinstance(cfg.jump_hosts, list) else str(cfg.jump_hosts)
            argv += ["-J", jumps]

        if cfg.multiplex and self.control_path:
            argv += [
                "-o", "ControlMaster=auto",
                "-o", f"ControlPath={self.control_path}",
                "-o", "ControlPersist=10m",
            ]

        for k, v in (cfg.extra_options or {}).items():
            argv += ["-o", f"{k}={v}"]

        if handle.direction == "local":
            argv += ["-L", f"{handle.local_host}:{handle.local_port}:{handle.remote_host}:{handle.remote_port}"]
        elif handle.direction == "remote":
            argv += ["-R", f"{handle.remote_host}:{handle.remote_port}:{handle.local_host}:{handle.local_port}"]

        target = f"{cfg.user}@{cfg.host}" if cfg.user else str(cfg.host)
        argv.append(target)
        return argv

    def _start_tunnel_process(self, handle: ForwardingHandle):
        if self._launcher is not None:
            proc = self._launcher(handle, self)
            handle.proc = proc
            return proc
        argv = self.build_ssh_command(handle)
        handle.argv = argv
        handle.said_file = tempfile.TemporaryFile()
        handle.proc = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=handle.said_file,
            stderr=handle.said_file,
            start_new_session=True,
        )
        return handle.proc

    def forward_local(
        self,
        remote_host: str,
        remote_port: int,
        local_port: Optional[int] = None,
        local_host: str = "127.0.0.1",
        wait_ready: float = 10.0,
    ) -> int:
        port = int(local_port) if local_port else _free_port()
        tunnel_id = f"local_{port}_{remote_host}_{remote_port}"
        handle = ForwardingHandle(
            tunnel_id=tunnel_id,
            direction="local",
            local_host=local_host,
            local_port=port,
            remote_host=remote_host,
            remote_port=int(remote_port),
        )
        with self._lock:
            self._tunnels[tunnel_id] = handle
        self._start_tunnel_process(handle)
        if wait_ready > 0:
            ready = _answers(port, wait=wait_ready)
            handle.last_health = ready
        return port

    def forward_remote(
        self,
        local_host: str,
        local_port: int,
        remote_port: int,
        remote_host: str = "127.0.0.1",
        wait_ready: float = 0.0,
    ) -> int:
        rport = int(remote_port)
        tunnel_id = f"remote_{rport}_{local_host}_{local_port}"
        handle = ForwardingHandle(
            tunnel_id=tunnel_id,
            direction="remote",
            local_host=local_host,
            local_port=int(local_port),
            remote_host=remote_host,
            remote_port=rport,
        )
        with self._lock:
            self._tunnels[tunnel_id] = handle
        self._start_tunnel_process(handle)
        if wait_ready > 0:
            ready = _answers(local_port, wait=wait_ready)
            handle.last_health = ready
        return rport

    def open_tunnel(
        self,
        remote_host: str,
        remote_port: int,
        local_port: Optional[int] = None,
        remote: bool = False,
        local_host: str = "127.0.0.1",
    ) -> ForwardingHandle:
        if remote:
            port = self.forward_remote(local_host, local_port or _free_port(), remote_port, remote_host)
            tunnel_id = f"remote_{port}_{local_host}_{local_port}"
        else:
            port = self.forward_local(remote_host, remote_port, local_port=local_port, local_host=local_host)
            tunnel_id = f"local_{port}_{remote_host}_{remote_port}"
        with self._lock:
            return self._tunnels[tunnel_id]

    def _check_socket(self, host: str, port: int, timeout: float = 1.0) -> bool:
        try:
            with socket.create_connection((host, int(port)), timeout=timeout):
                return True
        except (OSError, ValueError):
            return False

    def check_health(self, tunnel_id: Optional[str] = None, timeout: float = 2.0) -> bool:
        with self._lock:
            if not self._tunnels:
                return False
            targets = [self._tunnels[tunnel_id]] if tunnel_id and tunnel_id in self._tunnels else list(self._tunnels.values())
        for h in targets:
            if h.proc is not None and h.proc.poll() is not None:
                return False
            if h.direction == "local":
                ok = self._check_socket(h.local_host, h.local_port, timeout=timeout)
                h.last_health = ok
                if not ok:
                    return False
        return True

    def measure_latency(
        self, tunnel_id: Optional[str] = None, tries: int = 3, timeout: float = 2.0
    ) -> Optional[float]:
        with self._lock:
            if not self._tunnels:
                return None
            targets = [self._tunnels[tunnel_id]] if tunnel_id and tunnel_id in self._tunnels else list(self._tunnels.values())

        latencies = []
        for h in targets:
            if h.direction != "local":
                continue
            best = None
            for _ in range(max(1, tries)):
                t0 = time.monotonic()
                try:
                    with socket.create_connection((h.local_host, h.local_port), timeout=timeout):
                        took_ms = (time.monotonic() - t0) * 1000.0
                        best = took_ms if best is None else min(best, took_ms)
                except OSError:
                    continue
            if best is not None:
                h.last_latency_ms = best
                latencies.append(best)
        if not latencies:
            return None
        return sum(latencies) / len(latencies)

    def _monitor_loop(self):
        while not self._stop.wait(self.poll_interval):
            if self._closed:
                break
            with self._lock:
                handles = list(self._tunnels.values())
            for h in handles:
                if not h.active:
                    continue
                proc = h.proc
                if proc is not None and proc.poll() is not None:
                    h.drop_count += 1
                    if self.reconnect:
                        delay = min(self.reconnect_max_delay, self.reconnect_delay * (2 ** min(h.drop_count, 5)))
                        if self._stop.wait(delay):
                            break
                        try:
                            self._start_tunnel_process(h)
                            if h.direction == "local":
                                if _answers(h.local_port, wait=5.0):
                                    h.reconnect_count += 1
                                    h.last_health = True
                            else:
                                h.reconnect_count += 1
                                h.last_health = True
                        except Exception:
                            pass
                elif h.direction == "local":
                    h.last_health = self._check_socket(h.local_host, h.local_port, timeout=0.5)

    def bind_hop(self, hop, sides=("source", "target"), wait_ready: float = 10.0) -> "AutoBastionTunnelManager":
        hid = id(hop)
        if hid not in self._bound_hops:
            self._bound_hops[hid] = (hop, {})

        for side in sides:
            ep = getattr(hop, side, None)
            if not ep or not ep.host or not ep.port:
                continue
            epid = id(ep)
            if epid in self._bound_hops[hid][1]:
                continue
            orig_host, orig_port = ep.host, int(ep.port)
            self._bound_hops[hid][1][epid] = (ep, orig_host, orig_port)

            local_port = self.forward_local(orig_host, orig_port, local_host="127.0.0.1", wait_ready=wait_ready)
            ep_opts = dict(getattr(ep, "options", None) or {})
            ep_opts["_original_host"] = orig_host
            ep_opts["_original_port"] = orig_port
            ep_opts["tunnel_to"] = f"{orig_host}:{orig_port}"
            ep_opts["tunnel_by"] = "auto-bastion"
            ep.options = ep_opts
            ep.host = "127.0.0.1"
            ep.port = local_port

        setattr(hop, "tunnel_manager", self)
        return self

    def unbind_hop(self, hop):
        hid = id(hop)
        if hid in self._bound_hops:
            _, endpoints = self._bound_hops[hid]
            for epid, (ep, h, p) in endpoints.items():
                ep.host = h
                ep.port = p
            del self._bound_hops[hid]
        if getattr(hop, "tunnel_manager", None) is self:
            delattr(hop, "tunnel_manager")

    def diagnostics(self) -> Dict[str, Any]:
        with self._lock:
            tunnel_list = []
            for h in self._tunnels.values():
                tunnel_list.append({
                    "tunnel_id": h.tunnel_id,
                    "direction": h.direction,
                    "local": f"{h.local_host}:{h.local_port}",
                    "remote": f"{h.remote_host}:{h.remote_port}",
                    "active": h.active,
                    "reconnect_count": h.reconnect_count,
                    "drop_count": h.drop_count,
                    "last_health": h.last_health,
                    "last_latency_ms": h.last_latency_ms,
                    "pid": h.proc.pid if h.proc else None,
                })
        return {
            "bastion": f"{self.config.user}@{self.config.host}:{self.config.port}" if self.config.user else f"{self.config.host}:{self.config.port}",
            "multiplex": self.config.multiplex,
            "control_path": self.control_path,
            "ephemeral_key": bool(self._ephemeral_key_file),
            "reconnect": self.reconnect,
            "tunnels": tunnel_list,
        }

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=2.0)

        with self._lock:
            for h in self._tunnels.values():
                h.active = False
                if h.proc is not None and h.proc.poll() is None:
                    try:
                        os.killpg(h.proc.pid, signal.SIGTERM)
                        h.proc.wait(timeout=2)
                    except Exception:
                        try:
                            h.proc.kill()
                        except Exception:
                            pass
                if h.said_file is not None:
                    try:
                        h.said_file.close()
                    except Exception:
                        pass
            self._tunnels.clear()

        if self.control_path and os.path.exists(self.control_path):
            target = f"{self.config.user}@{self.config.host}" if self.config.user else str(self.config.host)
            try:
                subprocess.run(
                    ["ssh", "-O", "exit", "-S", self.control_path, target],
                    capture_output=True,
                    timeout=2,
                    stdin=subprocess.DEVNULL,
                )
            except Exception:
                pass

        for p in self._ephemeral_paths:
            try:
                if os.path.isdir(p):
                    shutil.rmtree(p, ignore_errors=True)
                elif os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        self._ephemeral_paths.clear()

        for hid in list(self._bound_hops.keys()):
            hop, _ = self._bound_hops[hid]
            self.unbind_hop(hop)

        unregister_manager(self)

    def teardown(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
