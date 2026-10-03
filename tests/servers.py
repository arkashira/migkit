"""The test servers: one container per server profile per lane, started the
first time a module asks for it, lent to each module as a space of its own
and put back as it was found when the module ends, removed when the session
ends.

281 modules started their own containers, 137 of them a MySQL whose first
start initialises a data directory, and one final run was ~3.5 h serial
(audit T1). Here a profile - image, server arguments, TLS - is started once
per lane and shared by every module naming it; each module gets a space of
its own (a database, schema, keyspace, topic prefix, bucket or index
prefix: `Space.name`) and the servers are reset to what they were at start
when it is done (T2): other databases, slots, subscriptions, publications,
roles and users, settings changed at run time, binary logs and GTIDs,
leftover sessions. A reset that cannot finish marks the servers broken and
the next module gets fresh ones.

Lanes (T3, T4). `MIGKIT_TEST_LANE` (0-9, set by `tools/final_suite.py`
and by the slots of `tools/with_docker_lock.py`) and the xdist worker
(gw0-gw9) give this process an index; its servers get host ports from its
own block, `BASE + BLOCK * index + offset`, and container names with
`-l<index>` (none on index 0), so lanes on one daemon or on several colima
profiles (whose forwarded ports share the host) never meet. Every mapping
is `127.0.0.1:` (T5). Offsets from `PRIVATE` to a block's end are for a
module's own containers (`private_port`). Index 0 keeps the names and ports the
modules written before this knew (`pg_pair` on 55432/55433): a module that
names them is pinned to lane 0 (`scan(...).pinned`).

Moving a module onto the shared servers (AUD-T2), in this order:

1. Replace its own `docker run` fixture by a module fixture of a profile:
   `pair = servers.fixture("mysql-gtid")` at module level gives a fixture
   `pair` whose value has `.src`, `.dst` (or `.one`, `.primary`/`.standby`)
   servers and `.name`, the module's space. A profile the module needs and
   `PROFILES` lacks is added there, never started by hand.
2. Use `.name` for the database (keyspace, topic prefix...) the hop names,
   `server.port`/`server.endpoint(...)` for addresses - never a literal port
   or container name.
3. SQL through `server.sql(...)` (the driver on the mapped port);
   `server.exec(...)` only for programs that must run in the server's
   container.
4. Fixed waits become `tests.waits.until(sign, timeout, what)`.
5. A test that must own its server (kills it, fails it over, slows its
   link, restarts it with other settings) keeps a private container through
   `servers.private(...)`, with a comment saying why.
6. An image the module builds goes through `servers.image(...)`.
"""
import atexit
import contextlib
import datetime
import fcntl
import hashlib
import ipaddress
import os
import re
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

from tests.waits import NeverCame, until

HOST = "127.0.0.1"
#: lane index 0's first port; each index has `BLOCK` ports. 20000-25999
#: holds no port any test file names (`test_the_suite_runs_in_lanes.py`),
#: and stays under the ephemeral ports Linux (32768 up: the colima VM,
#: whose own connections took 35034 from a test's server, measured) and
#: macOS (49152 up) hand out
BASE, BLOCK, PRIVATE = 20000, 60, 40
LANES, WORKERS = 10, 10
PREFIX = "migkit-test"
LABEL = "migkit.test"
PASSWORD = "test"
#: SQL Server and Oracle refuse a password as plain as the rest's
STRONG = "CHANGE_ME-Test1"
ROOT = Path(__file__).resolve().parent.parent


def lane():
    """This process's index: its lane (`MIGKIT_TEST_LANE`) and its xdist
    worker within the lane."""
    n = int(os.environ.get("MIGKIT_TEST_LANE") or 0)
    w = os.environ.get("PYTEST_XDIST_WORKER") or ""
    k = int(w[2:]) if w[2:].isdigit() else 0
    if not (0 <= n < LANES and 0 <= k < WORKERS):
        raise ValueError(f"lane {n} worker {k}: lanes are 0-{LANES - 1},"
                         f" workers 0-{WORKERS - 1}")
    return n * WORKERS + k


def port(offset, index=None):
    """The host port at `offset` of this lane's block."""
    if not 0 <= offset < BLOCK:
        raise ValueError(f"offset {offset} outside a block of {BLOCK}")
    return BASE + BLOCK * (lane() if index is None else index) + offset


def private_port(k):
    """The `k`-th port a module may give a container of its own."""
    if not 0 <= k < BLOCK - PRIVATE:
        raise ValueError(f"a module has {BLOCK - PRIVATE} private ports")
    return port(PRIVATE + k)


def name(base, index=None):
    """A container name for this lane: `base` on index 0, `base-l<i>`."""
    i = lane() if index is None else index
    return base if i == 0 else f"{base}-l{i}"


# ---- docker -----------------------------------------------------------------

def docker(*argv, timeout=600, input=None):
    return subprocess.run(["docker", *argv], capture_output=True, text=True,
                          timeout=timeout, input=input)


_DAEMON = {}


def daemon():
    """(reachable, architecture) of the docker this process talks to,
    asked once."""
    if "arch" not in _DAEMON:
        try:
            got = docker("info", "--format", "{{.Architecture}}", timeout=20)
            _DAEMON["arch"] = got.stdout.strip() if got.returncode == 0 \
                else None
        except Exception:  # noqa: BLE001 - no docker here
            _DAEMON["arch"] = None
    return _DAEMON["arch"] is not None, _DAEMON["arch"]


def docker_up():
    return daemon()[0]


def labels():
    run = os.environ.get("MIGKIT_TEST_RUN", "")
    return ["--label", f"{LABEL}=1", "--label", f"{LABEL}.lane={lane()}",
            "--label", f"{LABEL}.run={run}"]


def remove(*containers):
    if containers:
        docker("rm", "-f", "-v", *containers, timeout=120)


def logs(container, lines=30):
    got = docker("logs", "--tail", str(lines), container, timeout=30)
    return (got.stdout + got.stderr)[-3000:]


def ip(container):
    return docker("inspect", "-f", "{{range .NetworkSettings.Networks}}"
                  "{{.IPAddress}} {{end}}", container).stdout.split()[0]


_NETWORKS = set()


def network():
    """This lane's docker network, for servers that reach each other by
    name (a standby its primary); removed with the servers."""
    net = name(f"{PREFIX}-net")
    if docker("network", "inspect", net).returncode != 0:
        docker("network", "create", *labels(), net)
    _NETWORKS.add(net)
    return net


# ---- images a test builds (T11) ---------------------------------------------

def image(tag_name, dockerfile, files=None, platform=""):
    """A test's own image, built once and reused while its recipe holds:
    tagged by the hash of the Dockerfile, every input file and the
    platform, so a changed recipe builds again and an unchanged one never
    does. Older tags of the same name are removed after a build."""
    files = files or {}
    h = hashlib.sha256()
    for part in (dockerfile, platform, *sorted(files)):
        h.update(part.encode() + b"\0")
    for k in sorted(files):
        v = files[k]
        h.update(v if isinstance(v, bytes) else str(v).encode())
    repo = f"{PREFIX}-{tag_name}"
    tag = f"{repo}:{h.hexdigest()[:12]}"
    if docker("image", "inspect", tag).returncode == 0:
        return tag
    with _file_lock(f"/tmp/{repo}.image.lock"):
        if docker("image", "inspect", tag).returncode == 0:
            return tag
        with tempfile.TemporaryDirectory(prefix=f"{repo}-") as work:
            (Path(work) / "Dockerfile").write_text(dockerfile)
            for rel, content in files.items():
                p = Path(work) / rel
                p.parent.mkdir(parents=True, exist_ok=True)
                (p.write_bytes if isinstance(content, bytes)
                 else p.write_text)(content)
            got = docker("build", *(["--platform", platform] if platform
                                    else []), "-t", tag, work, timeout=1800)
        if got.returncode != 0:
            raise RuntimeError(f"building {tag}:"
                               f" {(got.stdout + got.stderr)[-3000:]}")
        old = docker("images", repo, "--format", "{{.Repository}}:{{.Tag}}")
        stale = [t for t in old.stdout.split() if t != tag]
        if stale:
            docker("rmi", *stale)
    return tag


@contextlib.contextmanager
def _file_lock(path):
    with open(path, "a+") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


# ---- TLS --------------------------------------------------------------------

_TLS = {}


def certificates():
    """An authority and a certificate it signed for `localhost` and
    127.0.0.1, made once per session: {ca, crt, key} paths."""
    if _TLS:
        return dict(_TLS)
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
    now = datetime.datetime.now(datetime.timezone.utc)
    where = Path(tempfile.mkdtemp(prefix="migkit-test-tls-"))
    ca_key, srv_key = ec.generate_private_key(ec.SECP256R1()), \
        ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,
                                            "migkit test authority")])

    def made(subject, key, extensions):
        b = (x509.CertificateBuilder().subject_name(subject)
             .issuer_name(ca_name).public_key(key.public_key())
             .serial_number(x509.random_serial_number())
             .not_valid_before(now - datetime.timedelta(days=1))
             .not_valid_after(now + datetime.timedelta(days=3)))
        for ext, critical in extensions:
            b = b.add_extension(ext, critical=critical)
        return b.sign(ca_key, hashes.SHA256())
    ca = made(ca_name, ca_key, [(x509.BasicConstraints(ca=True,
                                                        path_length=None),
                                 True)])
    srv = made(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,
                                             "localhost")]), srv_key,
               [(x509.SubjectAlternativeName([
                   x509.DNSName("localhost"),
                   x509.IPAddress(ipaddress.ip_address(HOST))]), False)])
    pem = serialization.Encoding.PEM
    (where / "ca.crt").write_bytes(ca.public_bytes(pem))
    (where / "server.crt").write_bytes(srv.public_bytes(pem))
    (where / "server.key").write_bytes(srv_key.private_bytes(
        pem, serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption()))
    _TLS.update(ca=str(where / "ca.crt"), crt=str(where / "server.crt"),
                key=str(where / "server.key"), dir=str(where))
    return dict(_TLS)


# ---- profiles ---------------------------------------------------------------

@dataclass(frozen=True)
class Profile:
    """A server as the tests start it: image, server arguments, TLS.
    `{port}` in `args`/`env` is the server's host port and `{sid}` its
    place in `roles` from 1 (a server id)."""
    key: str
    engine: str
    image: str
    inner: int
    offset: int
    roles: tuple = ("src", "dst")
    #: what one server holds in memory, measured or as its settings cap it
    mb: int = 300
    args: tuple = ()
    env: tuple = ()
    options: tuple = ()
    #: the server listens on its host port inside the container too: a
    #: broker or replica set member names its own address to clients
    same_port: bool = False
    tls: bool = False
    #: needs the migkit VM (x86 under Rosetta, or more memory than the
    #: default profile has)
    vm: bool = False
    platform: str = ""
    user: str = ""
    password: str = PASSWORD
    #: the most a start may take, in seconds
    start_s: int = 180


_PG_ENV = (("POSTGRES_PASSWORD", PASSWORD),)
_MY_ENV = (("MYSQL_ROOT_PASSWORD", PASSWORD),)
_KAFKA_ENV = (
    ("KAFKA_NODE_ID", "1"), ("KAFKA_PROCESS_ROLES", "broker,controller"),
    ("KAFKA_LISTENERS",
     "PLAINTEXT://0.0.0.0:{port},CONTROLLER://0.0.0.0:9093"),
    ("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://127.0.0.1:{port}"),
    ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
    ("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093"),
    ("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
     "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"),
    ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
    ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"))
#: SQL Server's image: SQL Edge where nothing else runs (arm64), the full
#: 2022 server where `MIGKIT_MSSQL_IMAGE` says (the x86 workflow)
MSSQL_IMAGE = os.environ.get("MIGKIT_MSSQL_IMAGE") or \
    "mcr.microsoft.com/azure-sql-edge:latest"

PROFILES = {p.key: p for p in (
    # what `pg_pair` has always been; lane 0 keeps its names and ports
    Profile("pg", "postgres", "postgres:16", 5432, 0, mb=80,
            args=("-c", "wal_level=logical"), env=_PG_ENV, user="postgres",
            start_s=120),
    Profile("pg-tls", "postgres", "postgres:16", 5432, 2, roles=("one",),
            mb=80, args=("-c", "wal_level=logical"), env=_PG_ENV,
            user="postgres", tls=True, start_s=120),
    Profile("pg-replica", "postgres", "postgres:16", 5432, 3,
            roles=("primary", "standby"), mb=100,
            args=("-c", "wal_level=logical", "-c", "hot_standby=on",
                  "-c", "wal_keep_size=256MB"),
            env=_PG_ENV, user="postgres", start_s=180),
    Profile("mysql", "mysql", "mysql:8.4", 3306, 5, mb=420, env=_MY_ENV,
            user="root", start_s=300),
    Profile("mysql-gtid", "mysql", "mysql:8.4", 3306, 7, mb=420,
            args=("--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                  "--server-id={sid}"), env=_MY_ENV, user="root",
            start_s=300),
    Profile("mysql-rowmeta", "mysql", "mysql:8.4", 3306, 9, mb=420,
            args=("--binlog-row-metadata=FULL", "--server-id={sid}"),
            env=_MY_ENV, user="root", start_s=300),
    Profile("mysql-infile", "mysql", "mysql:8.4", 3306, 11, mb=420,
            args=("--local-infile=1",), env=_MY_ENV, user="root",
            start_s=300),
    Profile("mysql-tls", "mysql", "mysql:8.4", 3306, 13, roles=("one",),
            mb=420, env=_MY_ENV, user="root", tls=True, start_s=300),
    Profile("mariadb", "mysql", "mariadb:11", 3306, 14, mb=200,
            args=("--log-bin=binlog", "--binlog-format=ROW",
                  "--server-id={sid}"),
            env=(("MARIADB_ROOT_PASSWORD", PASSWORD),), user="root",
            start_s=180),
    Profile("mongo", "mongo", "mongo:7", 27017, 16, mb=300,
            args=("--replSet", "rs0", "--bind_ip_all", "--port", "{port}"),
            same_port=True, start_s=180),
    Profile("redis", "redis", "redis:7", 6379, 18, mb=20, start_s=60),
    Profile("redis-tls", "redis", "redis:7", 6379, 20, roles=("one",),
            mb=20, tls=True, start_s=60),
    Profile("kafka", "kafka", "apache/kafka:3.8.0", 9092, 21, mb=600,
            env=_KAFKA_ENV, same_port=True, start_s=180),
    Profile("redpanda", "kafka", "redpandadata/redpanda:latest", 9092, 23,
            mb=450,
            args=("redpanda", "start", "--overprovisioned", "--smp", "1",
                  "--memory", "384M", "--reserve-memory", "0M",
                  "--node-id", "0", "--check=false",
                  "--kafka-addr", "PLAINTEXT://0.0.0.0:{port}",
                  "--advertise-kafka-addr", "PLAINTEXT://127.0.0.1:{port}"),
            same_port=True, start_s=180),
    Profile("cassandra", "cassandra", "cassandra:4.1", 9042, 25, mb=800,
            env=(("MAX_HEAP_SIZE", "384M"), ("HEAP_NEWSIZE", "64M")),
            start_s=300),
    Profile("clickhouse", "clickhouse", "clickhouse/clickhouse-server:24.8",
            8123, 27, mb=350, env=(("CLICKHOUSE_PASSWORD", PASSWORD),),
            options=("--ulimit", "nofile=262144:262144"), user="default",
            start_s=120),
    Profile("opensearch", "opensearch", "opensearchproject/opensearch:2.19.1",
            9200, 29, mb=650,
            env=(("discovery.type", "single-node"),
                 ("DISABLE_SECURITY_PLUGIN", "true"),
                 ("DISABLE_INSTALL_DEMO_CONFIG", "true"),
                 ("OPENSEARCH_JAVA_OPTS", "-Xms256m -Xmx256m")),
            start_s=240),
    Profile("dynamodb", "dynamodb", "amazon/dynamodb-local:latest", 8000, 31,
            roles=("one",), mb=250,
            args=("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"),
            start_s=90),
    Profile("minio", "minio", "bitnamilegacy/minio:latest", 9000, 32,
            roles=("one",), mb=200,
            env=(("MINIO_ROOT_USER", "minioadmin"),
                 ("MINIO_ROOT_PASSWORD", "minioadmin")),
            # the local stand-in's own sign-in
            user="minioadmin", password="minioadmin",  # sample
            start_s=90),
    Profile("sqlserver", "mssql", MSSQL_IMAGE, 1433, 33, roles=("one",),
            mb=1100,
            env=(("ACCEPT_EULA", "Y"), ("MSSQL_SA_PASSWORD", STRONG),
                 ("MSSQL_MEMORY_LIMIT_MB", "1024"),
                 ("MSSQL_TELEMETRY_ENABLED", "false")),
            vm=True, platform="" if "azure-sql-edge" in MSSQL_IMAGE
            else "linux/amd64", user="sa", password=STRONG, start_s=480),
    Profile("oracle", "oracle", "gvenzl/oracle-free:slim-faststart", 1521,
            34, roles=("one",), mb=2000,
            env=(("ORACLE_PASSWORD", STRONG),), vm=True, user="system",
            password=STRONG, start_s=600),
)}

#: lane 0's `pg` servers keep the names and ports every module written
#: before the lanes knew (`tests/conftest.py:pg_pair`)
PINNED = {("pg", "src"): ("migkit-test-pg-src", 55432),
          ("pg", "dst"): ("migkit-test-pg-dst", 55433)}


# ---- servers ----------------------------------------------------------------

@dataclass
class Server:
    profile: Profile
    role: str
    container: str
    port: int
    host: str = HOST
    baseline: dict = field(default_factory=dict)

    @property
    def user(self):
        return self.profile.user

    @property
    def password(self):
        return self.profile.password

    @property
    def ca(self):
        """The authority the server's certificate verifies against."""
        return certificates()["ca"] if self.profile.tls else None

    def endpoint(self, **options):
        from migkit.config import Endpoint
        return Endpoint(host=self.host, port=self.port, user=self.user,
                        password=self.password, options=options)

    def sql(self, statement, db=None):
        """Rows of `statement` (the last result where it holds several),
        through the driver on the mapped port."""
        return _ENGINES[self.profile.engine].sql(self, statement, db)

    def client(self):
        """The engine's own client for the engines that take no SQL (a
        MongoClient, a Redis client, a Kafka admin client, a Cassandra
        session, boto3's client for DynamoDB and object stores)."""
        return _ENGINES[self.profile.engine].client(self)

    def exec(self, *argv, input=None, timeout=600):
        """A program in the server's own container."""
        return docker("exec", *(["-i"] if input is not None else []),
                      self.container, *argv, input=input, timeout=timeout)

    def ip(self):
        return ip(self.container)


_RUNNING = {}     # profile key -> [Server]
_LAST = {}        # profile key -> when a module last took it
_TAKEN = {}       # profile key -> modules holding it now
_BROKEN = set()


def _budget():
    """What this lane's shared servers may hold, in MB; the least recently
    used profile no module holds is stopped to make room."""
    return int(os.environ.get("MIGKIT_TEST_LANE_MB") or 2500)


def running():
    return {k: list(v) for k, v in _RUNNING.items()}


def servers_of(key):
    """The profile's servers, started if they are not."""
    if key not in PROFILES:
        raise KeyError(f"no server profile {key!r}: one of"
                       f" {', '.join(sorted(PROFILES))}")
    if key in _RUNNING and key not in _BROKEN:
        _LAST[key] = time.monotonic()
        return _RUNNING[key]
    stop(key)
    p = PROFILES[key]
    held = sum(PROFILES[k].mb * len(PROFILES[k].roles) for k in _RUNNING)
    for k in sorted(_RUNNING, key=lambda k: _LAST.get(k, 0)):
        if held + p.mb * len(p.roles) <= _budget():
            break
        if not _TAKEN.get(k):
            held -= PROFILES[k].mb * len(PROFILES[k].roles)
            stop(k)
    if not docker_up():
        import pytest
        pytest.skip("docker not available")
    _RUNNING[key] = _start(p)
    _BROKEN.discard(key)
    _LAST[key] = time.monotonic()
    return _RUNNING[key]


def stop(key):
    got = _RUNNING.pop(key, None)
    _BROKEN.discard(key)
    if got:
        remove(*(s.container for s in got))


def stop_all():
    for key in list(_RUNNING):
        stop(key)
    if _NETWORKS:
        docker("network", "rm", *_NETWORKS)
        _NETWORKS.clear()
    if _TLS:
        import shutil
        shutil.rmtree(_TLS["dir"], ignore_errors=True)
        _TLS.clear()


atexit.register(stop_all)


def _placed(p, role, i):
    if lane() == 0 and (p.key, role) in PINNED:
        return PINNED[(p.key, role)]
    return name(f"{PREFIX}-{p.key}-{role}"), port(p.offset + i)


def _fill(values, hostport, sid):
    return [str(v).replace("{port}", str(hostport)).replace("{sid}", str(sid))
            for v in values]


def _start(p):
    made = []
    try:
        if p.key == "pg-replica":
            made = _start_replica(p)
        else:
            for i, role in enumerate(p.roles):
                container, hostport = _placed(p, role, i)
                made.append(Server(p, role, container, hostport))
                _run(p, made[-1], i + 1)
        engine = _ENGINES[p.engine]
        for s in made:
            _ready(s, engine)
            s.baseline = engine.baseline(s)
        return made
    except BaseException:
        remove(*(s.container for s in made))
        raise


def _run(p, s, sid):
    inner = s.port if p.same_port else p.inner
    env = []
    for k, v in p.env:
        env += ["-e", f"{k}={_fill([v], s.port, sid)[0]}"]
    head = ["--name", s.container, *labels(),
            "-p", f"{HOST}:{s.port}:{inner}", *env, *p.options,
            *(["--platform", p.platform] if p.platform else [])]
    remove(s.container)
    args = _fill(p.args, s.port, sid)
    if not p.tls:
        got = docker("run", "-d", *head, p.image, *args)
    else:
        # the certificate goes in before the server starts: created,
        # copied into, then started
        got = docker("create", *head, p.image, *_ENGINES[p.engine].tls(args))
        if got.returncode == 0:
            got = docker("cp", f"{certificates()['dir']}/.",
                         f"{s.container}:/certs")
        if got.returncode == 0:
            got = docker("start", s.container)
    if got.returncode != 0:
        raise RuntimeError(f"starting {s.container} ({p.image}):"
                           f" {got.stderr.strip()[-2000:]}")


def _start_replica(p):
    """A primary and a hot standby streaming from it, on this lane's
    network: the standby is a base backup of the primary, by name."""
    net = network()
    (pc, pport), (sc, sport) = _placed(p, "primary", 0), \
        _placed(p, "standby", 1)
    primary, standby = Server(p, "primary", pc, pport), \
        Server(p, "standby", sc, sport)
    remove(pc, sc)
    try:
        got = docker("run", "-d", "--name", pc, "--network", net, *labels(),
                     "-p", f"{HOST}:{pport}:5432", "-e",
                     f"POSTGRES_PASSWORD={PASSWORD}", p.image, *p.args)
        if got.returncode != 0:
            raise RuntimeError(f"starting {pc}: {got.stderr.strip()[-2000:]}")
        _ready(primary, _ENGINES["postgres"])
        got = primary.exec("sh", "-c", "echo 'host replication all all"
                           " scram-sha-256' >> \"$PGDATA/pg_hba.conf\"")
        if got.returncode != 0:
            raise RuntimeError(f"{pc}: {got.stderr.strip()}")
        primary.sql("select pg_reload_conf()")
        backup = (f"until pg_basebackup -h {pc} -U postgres -D \"$PGDATA\""
                  " -R -X stream -c fast -d 'application_name=standby'; do"
                  " sleep 1; rm -rf \"$PGDATA\"/*; done; chown -R postgres"
                  " \"$PGDATA\"; chmod 700 \"$PGDATA\"; exec gosu postgres"
                  " postgres -c hot_standby=on")
        got = docker("run", "-d", "--name", sc, "--network", net, *labels(),
                     "-p", f"{HOST}:{sport}:5432", "-e",
                     f"PGPASSWORD={PASSWORD}", "--entrypoint", "bash",
                     p.image, "-c", backup)
        if got.returncode != 0:
            raise RuntimeError(f"starting {sc}: {got.stderr.strip()[-2000:]}")
    except BaseException:
        remove(pc, sc)
        raise
    return [primary, standby]


def _ready(s, engine):
    """Ready by the server's own sign, never by an open port: colima's
    forwarded port accepts before the server behind it answers."""
    try:
        until(lambda: engine.ready(s), s.profile.start_s,
              f"{s.container} ({s.profile.image}) to answer")
    except NeverCame as e:
        raise NeverCame(f"{e}\n{logs(s.container)}") from None


# ---- a module's space -------------------------------------------------------

def space_name(module):
    """A module's own name on a shared server: `m_<stem>_<hash>`, short
    enough for every engine's limit (Oracle's 30, Cassandra's 48) and
    made only of what all of them take."""
    stem = module.rsplit(".", 1)[-1]
    stem = stem[5:] if stem.startswith("test_") else stem
    slug = re.sub(r"[^a-z0-9]+", "_", stem.lower()).strip("_")
    return f"m_{slug[:20]}_{hashlib.sha1(stem.encode()).hexdigest()[:6]}"


class Space:
    """A module's part of a profile's servers: its servers by role
    (`.src`, `.dst`, `.one`, `.primary`, `.standby`), and `.name`, the
    database, keyspace, topic or index prefix, or bucket that is the
    module's own. Leaving puts the servers back as they were at start.
    `own=False` makes no database of the module's own: the modules written
    before the spaces list the server's databases and expect only theirs."""

    def __init__(self, key, module, own=True):
        self.profile = PROFILES[key]
        self.module = module
        self.own = own
        self.name = space_name(module)
        self.servers = []

    def __enter__(self):
        self.servers = servers_of(self.profile.key)
        _TAKEN.setdefault(self.profile.key, set()).add(self.module)
        engine = _ENGINES[self.profile.engine]
        try:
            for s in self.servers:
                if s.role != "standby" and self.own:
                    engine.open(s, self.name)
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *exc):
        _TAKEN.get(self.profile.key, set()).discard(self.module)
        reset(self.profile.key)
        return False

    def __getattr__(self, role):
        for s in self.__dict__.get("servers", ()):
            if s.role == role:
                return s
        raise AttributeError(role)

    @property
    def bucket(self):
        return self.name.replace("_", "-")


def reset(key):
    """Put a profile's servers back as they were when they started; a
    reset that cannot finish leaves them broken, to be started afresh."""
    engine = _ENGINES[PROFILES[key].engine]
    try:
        for s in _RUNNING.get(key, ()):
            if s.role != "standby":
                engine.reset(s)
    except Exception:  # noqa: BLE001 - fresh servers next time
        _BROKEN.add(key)


def space(key, module, own=True):
    return Space(key, module, own)


def fixture(key):
    """A module-scoped fixture lending the module a space on profile
    `key`, named by what it is assigned to: `pair = servers.fixture(
    "mysql")` at a test module's top gives the module a fixture `pair`."""
    import pytest
    if key not in PROFILES:
        raise KeyError(f"no server profile {key!r}")

    def lend(request):
        with Space(key, request.module.__name__) as got:
            yield got
    lend.__name__ = f"space_{key.replace('-', '_')}"
    lend.migkit_profile = key
    return pytest.fixture(scope="module")(lend)


@contextlib.contextmanager
def private(module, role, image_name, *args, ports=(), env=None, options=(),
            platform=""):
    """A container of the module's own, for a test that must own its
    server (kills it, fails it over, slows its link): named for the module
    and this lane, its `ports` (inner ports) mapped to private ports of the
    lane in order, removed when the block ends. Yields (container,
    {inner: host port})."""
    container = name(f"{PREFIX}-{space_name(module)[2:]}-{role}".replace(
        "_", "-"))
    mapped = {inner: private_port(k) for k, inner in enumerate(ports)}
    argv = ["run", "-d", "--name", container, *labels(), *options,
            *(["--platform", platform] if platform else [])]
    for inner, host in mapped.items():
        argv += ["-p", f"{HOST}:{host}:{inner}"]
    for k, v in (env or {}).items():
        argv += ["-e", f"{k}={v}"]
    remove(container)
    got = docker(*argv, image_name, *args)
    if got.returncode != 0:
        raise RuntimeError(f"starting {container}: {got.stderr.strip()}")
    try:
        yield container, mapped
    finally:
        remove(container)


# ---- engines: ready, baseline, open, reset, sql, tls ------------------------

class _Postgres:
    @staticmethod
    def client(s, db="postgres"):
        return _Postgres.connect(s, db)

    @staticmethod
    def connect(s, db="postgres"):
        import psycopg2
        c = psycopg2.connect(host=s.host, port=s.port, user=s.user,
                             password=s.password, dbname=db, connect_timeout=5)
        c.autocommit = True
        return c

    def sql(self, s, statement, db=None):
        with contextlib.closing(self.connect(s, db or "postgres")) as c, \
                c.cursor() as cur:
            cur.execute(statement)
            return cur.fetchall() if cur.description else []

    def ready(self, s):
        if s.role == "standby":
            return self.sql(s, "select pg_is_in_recovery()") == [(True,)]
        return self.sql(s, "select 1") == [(1,)]

    def baseline(self, s):
        return {"roles": {r for (r,) in self.sql(
                    s, "select rolname from pg_roles")},
                "slots": {r for (r,) in self.sql(
                    s, "select slot_name from pg_replication_slots")}}

    def open(self, s, db):
        self.sql(s, f'drop database if exists "{db}" with (force)')
        self.sql(s, f'create database "{db}"')

    def reset(self, s):
        """Back to a fresh server: the settings first (a read-only default
        refuses every drop), every other session, subscriptions, prepared
        transactions, slots, every database (`postgres` made again, so its
        schemas, publications, types and large objects go with it), roles,
        tablespaces."""
        keep = s.baseline
        q = lambda sql, db="postgres": self.sql(s, sql, db)  # noqa: E731
        q("alter system reset all")
        q("select pg_reload_conf()")
        q("alter role postgres reset all")
        # the replica profile's standby streams through a walsender of its
        # own, named by its connection
        q("select pg_terminate_backend(pid) from pg_stat_activity"
          " where pid <> pg_backend_pid() and (backend_type ="
          " 'client backend' or backend_type = 'walsender' and"
          " application_name not in ('standby', 'walreceiver'))")
        for sub, db in q("select s.subname, d.datname from pg_subscription s"
                         " join pg_database d on d.oid = s.subdbid"):
            q(f'alter subscription "{sub}" disable', db)
            q(f'alter subscription "{sub}" set (slot_name = none)', db)
            q(f'drop subscription "{sub}"', db)
        for gid, db in q("select gid, database from pg_prepared_xacts"):
            q("rollback prepared '" + gid.replace("'", "''") + "'", db)
        until(lambda: self._slots_dropped(s, keep["slots"]), 30,
              f"{s.container}: its replication slots to be dropped")
        for (db,) in q("select datname from pg_database where datname not in"
                       " ('template0', 'template1')", "template1"):
            q(f'drop database "{db}" with (force)', "template1")
        q("create database postgres", "template1")
        for (role,) in q("select rolname from pg_roles"):
            if role not in keep["roles"]:
                q(f'drop owned by "{role}" cascade', "template1")
                q(f'drop role "{role}"')
        for (sp,) in q("select spcname from pg_tablespace where spcname"
                       " not in ('pg_default', 'pg_global')"):
            q(f'drop tablespace "{sp}"')

    def _slots_dropped(self, s, keep):
        left = [(n, pid) for n, pid in self.sql(
            s, "select slot_name, active_pid from pg_replication_slots")
            if n not in keep]
        for n, pid in left:
            if pid:
                self.sql(s, f"select pg_terminate_backend({int(pid)})")
            else:
                self.sql(s, "select pg_drop_replication_slot('"
                         + n.replace("'", "''") + "')")
        return not left

    @staticmethod
    def tls(args):
        return ["bash", "-c",
                "mkdir -p /tls && cp /certs/* /tls/ && chown postgres /tls/*"
                " && chmod 600 /tls/server.key && exec docker-entrypoint.sh"
                " postgres -c ssl=on -c ssl_cert_file=/tls/server.crt"
                " -c ssl_key_file=/tls/server.key -c ssl_ca_file=/tls/ca.crt "
                + " ".join(args)]


class _MySQL:
    SYSTEM_DBS = ("mysql", "information_schema", "performance_schema", "sys")
    #: globals that move on their own or cannot be set back by a SET
    MOVING = {"gtid_executed", "gtid_purged", "gtid_owned", "gtid_binlog_pos",
              "gtid_binlog_state", "gtid_current_pos", "timestamp",
              "innodb_buffer_pool_size", "server_uuid"}

    @staticmethod
    def client(s, db=None):
        return _MySQL.connect(s, db)

    @staticmethod
    def connect(s, db=None):
        import pymysql
        from pymysql.constants import CLIENT
        kw = {"ssl": {"ca": s.ca}} if s.profile.tls else {}
        return pymysql.connect(host=s.host, port=s.port, user=s.user,
                               password=s.password, database=db,
                               autocommit=True, connect_timeout=5,
                               client_flag=CLIENT.MULTI_STATEMENTS, **kw)

    def sql(self, s, statement, db=None):
        with contextlib.closing(self.connect(s, db)) as c, c.cursor() as cur:
            cur.execute(statement)
            rows = cur.fetchall() if cur.description else ()
            while cur.nextset():
                if cur.description:
                    rows = cur.fetchall()
            return [tuple(r) for r in rows]

    def ready(self, s):
        return self.sql(s, "select 1") == [(1,)]

    def baseline(self, s):
        return {"users": set(self.sql(s, "select user, host from mysql.user")),
                "globals": dict(self.sql(s, "show global variables")),
                "plugins": {p for (p,) in self.sql(
                    s, "select plugin_name from information_schema.plugins")},
                "components": self._components(s)}

    def _components(self, s):
        try:
            return {c for (c,) in self.sql(
                s, "select component_urn from mysql.component")}
        except Exception:  # noqa: BLE001 - MariaDB has none
            return set()

    def open(self, s, db):
        self.sql(s, f"drop database if exists `{db}`; create database `{db}`")

    def reset(self, s):
        """Back to a fresh server: globals changed at run time (read-only
        first, or nothing below could drop), replication, every other
        session, databases, users, plugins, persisted settings, binary
        logs and GTIDs."""
        keep = s.baseline
        with contextlib.closing(self.connect(s)) as c, c.cursor() as cur:
            cur.execute("show global variables")
            now = dict(cur.fetchall())
            order = sorted(now, key=lambda k: (k not in ("super_read_only",
                                                         "read_only",
                                                         "offline_mode"), k))
            for k in order:
                was = keep["globals"].get(k)
                if was is None or now[k] == was or k in self.MOVING:
                    continue
                value = was if re.fullmatch(r"-?\d+|ON|OFF", was) \
                    else c.escape(was)
                try:
                    cur.execute(f"set global {k} = {value}")
                except Exception as e:  # noqa: BLE001
                    if e.args and e.args[0] == 1238:   # read only
                        continue
                    raise
            for stmt in ("stop replica", "reset replica all", "stop slave",
                         "reset slave all"):
                with contextlib.suppress(Exception):
                    cur.execute(stmt)
            cur.execute("select id from information_schema.processlist where"
                        " id <> connection_id() and user not in"
                        " ('event_scheduler', 'system user')")
            for (pid,) in cur.fetchall():
                with contextlib.suppress(Exception):
                    cur.execute(f"kill {int(pid)}")
            cur.execute("show databases")
            for (db,) in cur.fetchall():
                if db not in self.SYSTEM_DBS:
                    cur.execute(f"drop database `{db}`")
            cur.execute("select user, host from mysql.user")
            for user, host in cur.fetchall():
                if (user, host) not in keep["users"]:
                    cur.execute("drop user %s@%s", (user, host))
            cur.execute("select plugin_name from information_schema.plugins")
            for (plugin,) in cur.fetchall():
                if plugin not in keep["plugins"]:
                    with contextlib.suppress(Exception):
                        cur.execute(f"uninstall plugin {plugin}")
            for urn in self._components(s) - keep["components"]:
                cur.execute("uninstall component %s", (urn,))
            with contextlib.suppress(Exception):
                cur.execute("reset persist")
            try:
                cur.execute("reset binary logs and gtids")
            except Exception:  # noqa: BLE001 - before 8.4, and MariaDB
                cur.execute("reset master")

    @staticmethod
    def tls(args):
        return ["bash", "-c",
                "mkdir -p /tls && cp /certs/* /tls/ && chown mysql /tls/*"
                " && chmod 600 /tls/server.key && exec docker-entrypoint.sh"
                " mysqld --ssl-ca=/tls/ca.crt --ssl-cert=/tls/server.crt"
                " --ssl-key=/tls/server.key " + " ".join(args)]


class _Mongo:
    SYSTEM_DBS = ("admin", "config", "local")

    @staticmethod
    def client(s):
        import pymongo
        return pymongo.MongoClient(host=s.host, port=s.port,
                                   directConnection=True,
                                   serverSelectionTimeoutMS=3000)

    def sql(self, s, statement, db=None):
        raise TypeError("MongoDB takes commands: server.client()")

    def ready(self, s):
        with contextlib.closing(self.client(s)) as c:
            hello = c.admin.command("hello")
            if hello.get("isWritablePrimary"):
                return True
            if not hello.get("setName"):
                with contextlib.suppress(Exception):
                    c.admin.command("replSetInitiate", {
                        "_id": "rs0", "members": [
                            {"_id": 0, "host": f"{HOST}:{s.port}"}]})
            return False

    def baseline(self, s):
        with contextlib.closing(self.client(s)) as c:
            return {"users": {u["_id"] for u in c.admin.command(
                "usersInfo")["users"]}}

    def open(self, s, db):
        with contextlib.closing(self.client(s)) as c:
            c.drop_database(db)

    def reset(self, s):
        with contextlib.closing(self.client(s)) as c:
            with contextlib.suppress(Exception):
                c.admin.command("killAllSessions", [])
            for db in c.list_database_names():
                if db not in self.SYSTEM_DBS:
                    c.drop_database(db)
            for u in c.admin.command("usersInfo")["users"]:
                if u["_id"] not in s.baseline["users"]:
                    c[u["db"]].command("dropUser", u["user"])
            for r in c.admin.command("rolesInfo", 1)["roles"]:
                c[r["db"]].command("dropRole", r["role"])


class _Redis:
    @staticmethod
    def client(s):
        import redis
        kw = {"ssl": True, "ssl_ca_certs": s.ca} if s.profile.tls else {}
        return redis.Redis(host=s.host, port=s.port, socket_timeout=5,
                           decode_responses=True, **kw)

    def sql(self, s, statement, db=None):
        return [(self.client(s).execute_command(*statement.split()),)]

    def ready(self, s):
        return self.client(s).ping()

    def baseline(self, s):
        c = self.client(s)
        return {"config": c.config_get("*"), "users": set(c.acl_users())}

    def open(self, s, db):
        pass

    def reset(self, s):
        c = self.client(s)
        c.execute_command("replicaof", "no", "one")
        c.flushall()
        c.script_flush()
        with contextlib.suppress(Exception):
            c.execute_command("function", "flush")
        for u in c.acl_users():
            if u not in s.baseline["users"]:
                c.acl_deluser(u)
        now = c.config_get("*")
        for k, v in s.baseline["config"].items():
            if now.get(k) != v:
                with contextlib.suppress(Exception):
                    c.config_set(k, v)
        c.execute_command("client", "kill", "type", "normal", "skipme", "yes")

    @staticmethod
    def tls(args):
        return ["sh", "-c",
                "mkdir -p /tls && cp /certs/* /tls/ && chmod 644 /tls/*"
                " && exec redis-server --port 0 --tls-port 6379"
                " --tls-cert-file /tls/server.crt --tls-key-file"
                " /tls/server.key --tls-ca-cert-file /tls/ca.crt"
                " --tls-auth-clients no " + " ".join(args)]


class _Kafka:
    @staticmethod
    def client(s):
        from kafka import KafkaAdminClient
        return KafkaAdminClient(bootstrap_servers=f"{s.host}:{s.port}",
                                request_timeout_ms=10000)

    def sql(self, s, statement, db=None):
        raise TypeError("a broker takes no SQL: server.port is its address")

    def ready(self, s):
        a = self.client(s)
        try:
            a.list_topics()
            return True
        finally:
            a.close()

    def baseline(self, s):
        return {}

    def open(self, s, db):
        pass

    def reset(self, s):
        a = self.client(s)
        try:
            # kafka-python 3 renamed the group calls and returns dicts
            listed = a.list_groups() if hasattr(a, "list_groups") \
                else a.list_consumer_groups()
            groups = [g.get("group_id") or g.get("group") if isinstance(
                g, dict) else g[0] for g in listed]
            if groups:
                (a.delete_groups if hasattr(a, "delete_groups")
                 else a.delete_consumer_groups)(groups)
            topics = [t for t in a.list_topics() if not t.startswith("_")]
            if topics:
                a.delete_topics(topics)
        finally:
            a.close()


class _Cassandra:
    @staticmethod
    def client(s):
        """A session; `session.cluster.shutdown()` closes it."""
        from cassandra.cluster import Cluster
        return Cluster([s.host], port=s.port, connect_timeout=10).connect()

    def sql(self, s, statement, db=None):
        session = self.client(s)
        try:
            if db:
                session.set_keyspace(db)
            return [tuple(r) for r in session.execute(statement)]
        finally:
            session.cluster.shutdown()

    def ready(self, s):
        return bool(self.sql(s, "select release_version from system.local"))

    def baseline(self, s):
        return {}

    def open(self, s, db):
        self.sql(s, f"create keyspace if not exists {db} with replication ="
                    " {'class': 'SimpleStrategy', 'replication_factor': 1}")

    def reset(self, s):
        for (ks,) in self.sql(s, "select keyspace_name from"
                                 " system_schema.keyspaces"):
            if not ks.startswith("system"):
                self.sql(s, f'drop keyspace if exists "{ks}"')


class _ClickHouse:
    SYSTEM_DBS = ("system", "information_schema", "INFORMATION_SCHEMA",
                  "default")

    @staticmethod
    def client(s, db=None):
        import clickhouse_connect
        return clickhouse_connect.get_client(host=s.host, port=s.port,
                                             username=s.user,
                                             password=s.password,
                                             database=db or "default")

    def sql(self, s, statement, db=None):
        c = self.client(s, db)
        try:
            return [tuple(r) for r in c.query(statement).result_rows]
        finally:
            c.close()

    def ready(self, s):
        return self.sql(s, "select 1") == [(1,)]

    def baseline(self, s):
        return {}

    def open(self, s, db):
        c = self.client(s)
        try:
            c.command(f"drop database if exists `{db}`")
            c.command(f"create database `{db}`")
        finally:
            c.close()

    def reset(self, s):
        c = self.client(s)
        try:
            for (db,) in c.query("show databases").result_rows:
                if db not in self.SYSTEM_DBS:
                    c.command(f"drop database `{db}` sync")
            for (t,) in c.query("show tables from default").result_rows:
                c.command(f"drop table default.`{t}` sync")
        finally:
            c.close()


class _OpenSearch:
    @staticmethod
    def client(s):
        from opensearchpy import OpenSearch
        return OpenSearch(hosts=[{"host": s.host, "port": s.port}],
                          timeout=30)

    @staticmethod
    def http(s, method, path):
        import json
        import urllib.request
        req = urllib.request.Request(f"http://{s.host}:{s.port}{path}",
                                     method=method)
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
        return json.loads(body) if body else None

    def sql(self, s, statement, db=None):
        raise TypeError("OpenSearch takes HTTP: server.port is its address")

    def ready(self, s):
        return self.http(s, "GET", "/_cluster/health").get("status") in (
            "green", "yellow")

    def baseline(self, s):
        return {}

    def open(self, s, db):
        pass

    def reset(self, s):
        for ds in (self.http(s, "GET", "/_data_stream") or {}).get(
                "data_streams", []):
            self.http(s, "DELETE", f"/_data_stream/{ds['name']}")
        for ix in self.http(s, "GET", "/_cat/indices?format=json"
                            "&expand_wildcards=all") or []:
            if not ix["index"].startswith("."):
                self.http(s, "DELETE", f"/{ix['index']}")
        for repo in self.http(s, "GET", "/_snapshot") or {}:
            self.http(s, "DELETE", f"/_snapshot/{repo}")
        for t in (self.http(s, "GET", "/_index_template") or {}).get(
                "index_templates", []):
            if not t["name"].startswith("."):
                self.http(s, "DELETE", f"/_index_template/{t['name']}")


class _Dynamo:
    @staticmethod
    def client(s):
        import boto3
        # the local stand-in takes any key made of letters and digits
        return boto3.client("dynamodb",
                            endpoint_url=f"http://{s.host}:{s.port}",
                            region_name="us-east-1",
                            aws_access_key_id="migkittest",
                            aws_secret_access_key="migkittest")

    def sql(self, s, statement, db=None):
        raise TypeError("DynamoDB takes calls: its client")

    def ready(self, s):
        self.client(s).list_tables()
        return True

    def baseline(self, s):
        return {}

    def open(self, s, db):
        pass

    def reset(self, s):
        c = self.client(s)
        for t in c.list_tables().get("TableNames", []):
            c.delete_table(TableName=t)


class _Minio:
    @staticmethod
    def client(s):
        import boto3
        return boto3.client("s3", endpoint_url=f"http://{s.host}:{s.port}",
                            region_name="us-east-1",
                            aws_access_key_id=s.user,
                            aws_secret_access_key=s.password)

    def sql(self, s, statement, db=None):
        raise TypeError("an object store takes calls: its client")

    def ready(self, s):
        self.client(s).list_buckets()
        return True

    def baseline(self, s):
        return {}

    def open(self, s, db):
        self.client(s).create_bucket(Bucket=db.replace("_", "-"))

    def reset(self, s):
        c = self.client(s)
        for b in c.list_buckets().get("Buckets", []):
            for page in c.get_paginator("list_object_versions").paginate(
                    Bucket=b["Name"]):
                gone = [{"Key": v["Key"], "VersionId": v["VersionId"]}
                        for v in page.get("Versions", []) +
                        page.get("DeleteMarkers", [])]
                if gone:
                    c.delete_objects(Bucket=b["Name"],
                                     Delete={"Objects": gone})
            c.delete_bucket(Bucket=b["Name"])


class _SqlServer:
    @staticmethod
    def client(s, db=None):
        return _SqlServer.connect(s, db)

    @staticmethod
    def connect(s, db=None):
        import pymssql
        return pymssql.connect(server=s.host, port=s.port, user=s.user,
                               password=s.password, database=db or "master",
                               login_timeout=5, autocommit=True)

    def sql(self, s, statement, db=None):
        with contextlib.closing(self.connect(s, db)) as c:
            cur = c.cursor()
            cur.execute(statement)
            rows = cur.fetchall() if cur.description else []
            while cur.nextset():
                if cur.description:
                    rows = cur.fetchall()
            return [tuple(r) for r in rows]

    def ready(self, s):
        return self.sql(s, "select 1") == [(1,)]

    def baseline(self, s):
        return {"logins": {n for (n,) in self.sql(
            s, "select name from sys.server_principals")}}

    def open(self, s, db):
        self.sql(s, f"if db_id('{db}') is null create database [{db}]")

    def reset(self, s):
        for (db,) in self.sql(s, "select name from sys.databases where"
                                 " database_id > 4"):
            self.sql(s, f"alter database [{db}] set single_user with"
                        f" rollback immediate; drop database [{db}]")
        for (login,) in self.sql(s, "select name from sys.server_principals"
                                    " where type in ('S', 'U', 'G')"):
            if login not in s.baseline["logins"]:
                self.sql(s, f"drop login [{login}]")


class _Oracle:
    SERVICE = "FREEPDB1"

    @staticmethod
    def client(s):
        return _Oracle.connect(s)

    @staticmethod
    def connect(s):
        import oracledb
        return oracledb.connect(user=s.user, password=s.password,
                                dsn=f"{s.host}:{s.port}/{_Oracle.SERVICE}",
                                tcp_connect_timeout=5)

    def sql(self, s, statement, db=None):
        with contextlib.closing(self.connect(s)) as c:
            c.autocommit = True
            cur = c.cursor()
            cur.execute(statement)
            return [tuple(r) for r in cur.fetchall()] if cur.description \
                else []

    def ready(self, s):
        return self.sql(s, "select 1 from dual") == [(1,)]

    def baseline(self, s):
        return {"users": {u for (u,) in self.sql(
            s, "select username from dba_users")}}

    def open(self, s, db):
        self.sql(s, f'create user "{db.upper()}" identified by "{STRONG}"'
                    " quota unlimited on users")
        self.sql(s, f'grant connect, resource to "{db.upper()}"')

    def reset(self, s):
        for (user,) in self.sql(s, "select username from dba_users"):
            if user in s.baseline["users"]:
                continue
            for sid, serial in self.sql(
                    s, "select sid, serial# from v$session where username ="
                       f" '{user}'"):
                with contextlib.suppress(Exception):
                    self.sql(s, f"alter system kill session '{sid},{serial}'"
                                " immediate")
            self.sql(s, f'drop user "{user}" cascade')


_ENGINES = {"postgres": _Postgres(), "mysql": _MySQL(), "mongo": _Mongo(),
            "redis": _Redis(), "kafka": _Kafka(), "cassandra": _Cassandra(),
            "clickhouse": _ClickHouse(), "opensearch": _OpenSearch(),
            "dynamodb": _Dynamo(), "minio": _Minio(), "mssql": _SqlServer(),
            "oracle": _Oracle()}


# ---- what a test file holds (for the lanes and the lock) --------------------

@dataclass
class Holds:
    """What a test file holds of the machine, read from its source and the
    `tests/` helpers it imports: the host ports and container names it
    fixes (two files fixing one cannot run at once), whether it names lane
    0's `pg` pair (runs on lane 0 only), the images it runs, the profiles
    it takes, and roughly how much memory. Whether a test needs the VM is
    `tests/conftest.py`'s, by what the test reaches."""
    ports: set = field(default_factory=set)
    names: set = field(default_factory=set)
    pinned: bool = False
    images: set = field(default_factory=set)
    profiles: set = field(default_factory=set)
    runs: int = 0
    mb: int = 0

    def keys(self):
        return {f"port:{p}" for p in self.ports} | \
            {f"name:{n}" for n in self.names} | \
            ({"lane0"} if self.pinned else set())


#: ports inside a container a file names beside its host ports; never a
#: host port the tests fix
INNER_PORTS = {5432, 3306, 27017, 6379, 9042, 8123, 9000, 9200, 1433, 1521,
               50000, 5000, 9092, 9093, 8000, 8080, 443, 80, 22, 2181, 8081,
               8083, 9300, 11210, 8091, 26257, 6432, 7000, 7001, 9160, 8443,
               5672, 9644, 8082, 33060, 10000, 2379, 8529, 7474, 7687, 8086,
               9001, 4566, 8888, 19092, 10800, 1025}
#: what a test names when it needs the migkit VM: an x86-only image, or
#: one wanting more memory than the default profile has
VM_SIGNS = ("linux/amd64", "gvenzl/oracle", "mssql/server", "azure-sql-edge",
            "db2_community", "datagrip/sybase")
#: what one container of an image holds, MB; the rest ~300
IMAGE_MB = {"cassandra": 800, "scylla": 800, "opensearch": 650,
            "elasticsearch": 800, "kafka": 600, "redpanda": 450,
            "couchbase": 1200, "cosmos": 2000, "mssql": 1100,
            "azure-sql-edge": 1100, "oracle": 2000, "db2": 2500,
            "sybase": 1000, "clickhouse": 350, "mongo": 300, "mysql": 420,
            "mariadb": 200, "postgres": 80, "postgis": 120, "redis": 20,
            "valkey": 20, "minio": 200, "dynamodb": 250, "toxiproxy": 20,
            "alpine": 10, "pgbouncer": 20, "bigquery": 200, "python": 300}
_KNOWN_REPOS = ("postgres", "mysql", "mariadb", "mongo", "redis", "valkey",
                "cassandra", "alpine", "debian", "ubuntu", "python", "nginx",
                "busybox", "node", "couchbase", "memcached", "rabbitmq")
_IMAGE = re.compile(
    r"""["']((?:[a-z0-9.-]+/)*[a-z0-9._-]+:[A-Za-z0-9._-]+)["']""")
_PINNED = re.compile(
    r"migkit-test-pg-(?:src|dst)\b|migkit-test-pg-\{|\b5543[23]\b")
_NAME = re.compile(r"^migkit-[a-z0-9][a-z0-9-]*[a-z0-9]$")
_HOSTPORT = re.compile(
    r"(?:^|[^\d.])(\d{4,5}):\d{2,5}(?!\d)|"
    r"(?:127\.0\.0\.1|localhost|0\.0\.0\.0):(\d{4,5})(?!\d)")
_PROFILE_USE = re.compile(
    r"servers\.(?:fixture|space|servers_of)\(\s*[\"']([a-z0-9-]+)[\"']")
_HELPERS_SKIPPED = {"conftest", "servers", "waits", "__init__"}
_SCANNED = {}


def scan(path):
    """`Holds` of the test file at `path`, with those of the `tests/`
    helper modules it imports."""
    path = Path(path).resolve()
    key = (str(path), path.stat().st_mtime if path.exists() else 0)
    if key not in _SCANNED:
        _SCANNED[key] = _scan(path, set())
    return _SCANNED[key]


def _scan(path, seen):
    import ast
    h = Holds()
    seen.add(path)
    try:
        text = path.read_text()
        tree = ast.parse(text)
    except (OSError, SyntaxError, ValueError):
        return h
    h.pinned = bool(_PINNED.search(text))
    h.runs = len(re.findall(r"[\"']run[\"']", text)) if '"docker"' in text \
        or "'docker'" in text else 0
    h.profiles = set(_PROFILE_USE.findall(text))
    if re.search(r"\bpg_pair\b", text):
        h.profiles.add("pg")
    if re.search(r"\bmy_pair\b", text):
        h.profiles.add("mysql")
    for m in _IMAGE.finditer(text):
        img = m.group(1)
        repo = img.split(":")[0]
        if not repo.startswith("migkit") and (
                "/" in repo or repo in _KNOWN_REPOS):
            h.images.add(img)

    def ints(node):
        return [n.value for n in ast.walk(node) if isinstance(n, ast.Constant)
                and type(n.value) is int and 1024 <= n.value <= 65535]

    def portish(nm):
        return "port" in nm.lower()

    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) \
                else [node.target]
            names = [n.id for t in targets for n in ast.walk(t)
                     if isinstance(n, ast.Name)]
            if node.value is not None and any(portish(n) for n in names):
                h.ports.update(ints(node.value))
        elif isinstance(node, ast.keyword) and node.arg and portish(node.arg):
            h.ports.update(ints(node.value))
        elif isinstance(node, ast.Compare):
            sides = [node.left, *node.comparators]
            if any(isinstance(n, ast.Name) and portish(n.id) for n in sides):
                for n in sides:
                    h.ports.update(ints(n))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            a = node.args
            pos = a.posonlyargs + a.args
            for arg, default in zip(pos[len(pos) - len(a.defaults):],
                                    a.defaults):
                if portish(arg.arg):
                    h.ports.update(ints(default))
            for arg, default in zip(a.kwonlyargs, a.kw_defaults):
                if default is not None and portish(arg.arg):
                    h.ports.update(ints(default))
        elif isinstance(node, ast.Dict):
            for k, v in zip(node.keys, node.values):
                if isinstance(k, ast.Constant) and isinstance(k.value, str) \
                        and portish(k.value):
                    h.ports.update(ints(v))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            s = node.value
            for a, b in _HOSTPORT.findall(s):
                h.ports.add(int(a or b))
            if _NAME.match(s):
                h.names.add(s)
    h.ports = {p for p in h.ports if p not in INNER_PORTS}
    h.mb = sum(next((mb for word, mb in IMAGE_MB.items() if word in img),
                    300) for img in h.images) * 2 if h.runs else 0
    for helper in _helpers(tree):
        p = path.parent / f"{helper}.py"
        if p.exists() and p not in seen:
            sub = _scan(p, seen)
            h.ports |= sub.ports
            h.names |= sub.names
            h.pinned |= sub.pinned
            h.images |= sub.images
            h.profiles |= sub.profiles
            h.runs += sub.runs
            h.mb = max(h.mb, sub.mb)
    return h


def _helpers(tree):
    import ast
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "tests":
                out.update(a.name for a in node.names)
            elif node.module.startswith("tests."):
                out.add(node.module.split(".")[1])
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name.startswith("tests."):
                    out.add(a.name.split(".")[1])
    return out - _HELPERS_SKIPPED
