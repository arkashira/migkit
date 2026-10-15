"""Which engine answers at an address that names none (backlog
P0-MINIMAL-INPUT).

`app@db1.example.com:5432/shop`, `db1.example.com:3306` or a bare host is
an address too: the engine is read off the server's own first words on
its port, never guessed from the port. Each protocol is asked the one
question it answers before anyone signs in - PostgreSQL its request for
TLS, SQL Server its pre-login, MongoDB `hello`, Redis `PING`, Kafka its
API versions, an HTTP server `GET /` - and MySQL is heard, as it speaks
first. The port only orders the asking (its own engine first,
`engines.PORTS`) and is the default of the one question asked where no
answer names an engine. A host with no port is knocked on at every
engine's own port at once.

Which knock is tried and when to fall to the question is a climb of the
decision engine (`decide.climb`): each knock a rung proved by the
server's answer, the question the last rung, held only where there is a
terminal to ask at.
"""
import json
import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

#: seconds a knock waits for the server
WAIT = 1.5


@dataclass(frozen=True)
class Knock:
    family: str
    said: str
    #: the engines that speak this protocol, `engines.PORTS`' names
    engines: tuple
    hello: object
    hear: object
    #: read until the server closes, not until `hear` answers
    whole: bool = False


def _hear_mysql(b):
    if len(b) < 5 or b[3] != 0:
        return None
    size = int.from_bytes(b[:3], "little")
    body = b[4:4 + size]
    if len(body) < size or not body:
        return None
    if body[0] == 10 and b"\0" in body[1:]:
        version = body[1:body.index(b"\0", 1)].decode("latin-1")
        return {"version": version.removeprefix("5.5.5-")}
    if body[0] == 0xff:
        return {"version": "", "note": body[3:].decode("latin-1",
                                                      "replace")[:120]}
    return None


def _hear_postgres(b):
    return {} if b in (b"S", b"N") else None


def _tds_hello(_host, _port):
    opts = bytes([0x00, 0, 11, 0, 6, 0x01, 0, 17, 0, 1, 0xff])
    payload = opts + b"\0" * 6 + b"\x02"
    return struct.pack(">BBHHBB", 0x12, 0x01, 8 + len(payload), 0, 1,
                       0) + payload


def _hear_tds(b):
    if len(b) < 8 or b[0] != 0x04:
        return None
    size = struct.unpack(">H", b[2:4])[0]
    if len(b) < size or size < 8:
        return None
    payload = b[8:size]
    at = 0
    while at + 5 <= len(payload) and payload[at] != 0xff:
        token = payload[at]
        off, n = struct.unpack(">HH", payload[at + 1:at + 5])
        if token == 0x00 and n >= 4 and off + n <= len(payload):
            v = payload[off:off + n]
            return {"version": f"{v[0]}.{v[1]}."
                               f"{int.from_bytes(v[2:4], 'big')}"}
        at += 5
    return None


def _bson(items):
    body = b""
    for k, v in items:
        name = k.encode() + b"\0"
        if isinstance(v, int):
            body += b"\x10" + name + struct.pack("<i", v)
        else:
            s = v.encode() + b"\0"
            body += b"\x02" + name + struct.pack("<i", len(s)) + s
    return struct.pack("<i", len(body) + 5) + body + b"\0"


def _mongo_hello(_host, _port):
    msg = struct.pack("<I", 0) + b"\0" + _bson([("hello", 1),
                                                ("$db", "admin")])
    return struct.pack("<iiii", 16 + len(msg), 7, 0, 2013) + msg


def _hear_mongo(b):
    if len(b) < 16:
        return None
    size, _, to, op = struct.unpack("<iiii", b[:16])
    if to != 7 or op not in (1, 2013) or len(b) < size:
        return None
    mark = b"\x10maxWireVersion\0"
    at = b.find(mark)
    wire = struct.unpack("<i", b[at + len(mark):at + len(mark) + 4])[0] \
        if at >= 0 else None
    return {"note": f"wire version {wire}" if wire is not None else ""}


def _hear_redis(b):
    if b"\r\n" not in b:
        return None
    line = b.split(b"\r\n", 1)[0].lower()
    if line == b"+pong" or (line.startswith(b"-") and any(
            w in line for w in (b"noauth", b"auth", b"denied"))):
        return {}
    return None


def _kafka_hello(_host, _port):
    client = b"migkit"
    body = struct.pack(">hhih", 18, 0, 0x6d6b, len(client)) + client
    return struct.pack(">i", len(body)) + body


def _hear_kafka(b):
    if len(b) < 8:
        return None
    size, corr = struct.unpack(">ii", b[:8])
    if corr != 0x6d6b or size > 1 << 20 or len(b) < 4 + size:
        return None
    return {}


def _http_hello(host, _port):
    return (f"GET / HTTP/1.0\r\nHost: {host}\r\nAccept: */*\r\n"
            "\r\n").encode()


def _hear_http(b):
    if not b.startswith(b"HTTP/1."):
        return None
    head, _, body = b.partition(b"\r\n\r\n")
    heads = head.decode("latin-1").lower()
    if "x-clickhouse-" in heads:
        return {"engine": "clickhouse"}
    if "couchbase" in heads:
        return {"engine": "couchbase"}
    try:
        doc = json.loads(body.decode("utf-8", "replace") or "{}")
    except ValueError:
        doc = {}
    version = doc.get("version") if isinstance(doc, dict) else None
    if isinstance(version, dict) and version.get("number"):
        return {"engine": "opensearch", "version": str(version["number"])}
    if 'realm="security"' in heads:
        return {"engine": "opensearch"}
    return None


KNOCKS = (
    Knock("mysql", "greeting", ("mysql", "tidb", "oceanbase", "polardbx",
                                "dcdb", "lindorm"), None, _hear_mysql),
    Knock("postgres", "answer to a request for TLS",
          ("postgres", "cockroachdb", "redshift", "dsql", "gaussdb"),
          lambda h, p: struct.pack("!ii", 8, 80877103), _hear_postgres),
    Knock("mssql", "pre-login answer", ("mssql",), _tds_hello, _hear_tds),
    Knock("mongodb", "answer to hello", ("mongodb", "cosmosdb",
                                         "geminidb-mongo"),
          _mongo_hello, _hear_mongo),
    Knock("redis", "answer to PING", ("redis", "geminidb-redis"),
          lambda h, p: b"*1\r\n$4\r\nPING\r\n", _hear_redis),
    Knock("kafka", "answer to a request for its API versions", ("kafka",),
          _kafka_hello, _hear_kafka),
    Knock("http", "answer over HTTP", ("clickhouse", "opensearch",
                                       "couchbase"),
          _http_hello, _hear_http, whole=True),
)


def _exchange(host, port, hello, hear, whole=False, wait=WAIT):
    """What the server says to `hello` (nothing sent where None), read
    until `hear` makes sense of it, the server closes or the wait ends."""
    with socket.create_connection((host, int(port)), timeout=wait) as s:
        s.settimeout(wait)
        if hello:
            s.sendall(hello)
        got, end = b"", time.monotonic() + wait
        while len(got) < 1 << 16 and time.monotonic() < end:
            try:
                part = s.recv(4096)
            except OSError:
                break
            if not part:
                break
            got += part
            if not whole and hear(got) is not None:
                break
        return got


def _reachable(host, port, wait=WAIT):
    try:
        socket.create_connection((host, int(port)), timeout=wait).close()
        return True
    except OSError:
        return False


def _ordered(port):
    from .engines import PORTS
    mine = {e for e, p in PORTS.items() if p == port}
    return sorted(KNOCKS, key=lambda k: (not mine & set(k.engines),
                                         KNOCKS.index(k)))


def _default(port):
    from .engines import PORTS
    return next((e for e, p in PORTS.items() if p == port), "")


def identify(host, port, can_ask=False):
    """The `decide.Choice` of how the engine at `host:port` is known and
    what it said: (choice, {"engine", "version", "note"} or None)."""
    from . import decide
    heard = {}

    def prove(k):
        def run(_facts):
            try:
                got = k.hear(_exchange(host, port, k.hello(host, port)
                                       if k.hello else None, k.hear,
                                       k.whole))
            except (OSError, ValueError, struct.error):
                got = None
            if got is None:
                return f"it did not answer as {k.family} does"
            heard[k.family] = {"engine": got.get("engine") or k.family,
                               "version": got.get("version", ""),
                               "note": got.get("note", ""),
                               "said": k.said}
            return None
        return run
    reached = decide.Need("the server reached", lambda f: f["reached"],
                          f"nothing answers at {host}:{port}")
    rungs = [decide.Rung(k.family, f"the server's own {k.said}",
                         needs=(reached,), prove=prove(k))
             for k in _ordered(port)]
    rungs.append(decide.Rung(
        "asked", "the operator's answer",
        needs=(decide.Need("a terminal to ask at",
                           lambda f: f["can_ask"]),)))
    facts = {"reached": _reachable(host, port), "can_ask": can_ask}
    choice = decide.climb(f"the engine at {host}:{port}", rungs, facts)
    return choice, heard.get(choice.path)


def _split(text):
    """(user@ or "", host, port or None, the rest: path and query)."""
    from .addresses import _hosts
    head, slash, path = text.partition("/")
    head, q, query = head.partition("?")
    rest = (slash + path) + (q + query if q else "")
    user, at, hostport = head.rpartition("@")
    (host, port), = _hosts(hostport) or [("", None)]
    if not host:
        raise SystemExit(f"{text!r} names no server")
    return (user + at), host, port, rest


def _open_ports(host):
    from .engines import PORTS
    mine = {e for k in KNOCKS for e in k.engines}
    ports = sorted({p for e, p in PORTS.items() if e in mine})
    with ThreadPoolExecutor(len(ports)) as pool:
        up = list(pool.map(lambda p: _reachable(host, p), ports))
    return [p for p, ok in zip(ports, up) if ok]


def address(text, ask, say):
    """`text` with the engine that answers there as its scheme. `ask` is
    the guide's question, or None where nothing may be asked: then a
    server that does not say what it is stops here with the address to
    write instead."""
    user, host, port, rest = _split(text.strip())
    ports = [port] if port else _open_ports(host)
    if not ports:
        raise SystemExit(f"nothing answers at {host} on any engine's own"
                         " port: write the port, host:port")
    found, last = [], None
    for p in ports:
        choice, heard = identify(host, p, can_ask=ask is not None)
        last = (p, choice)
        if heard:
            found.append((p, heard))
    if len(found) == 1:
        p, heard = found[0]
        engine = heard["engine"]
        version = f" {heard['version']}" if heard["version"] else ""
        say(f"{host}:{p} answers as {engine}{version} (the server's own"
            f" {heard['said']})")
    else:
        p = found[0][0] if found else last[0]
        default = found[0][1]["engine"] if found else _default(p)
        why = ("it answers as " + ", ".join(
            f"{h['engine']} on {q}" for q, h in found) if found
            else last[1].reason)
        text_q = (f"Which engine answers at {host}"
                  + ("" if len(found) > 1 else f":{p}")
                  + f"? migkit could not tell: {why}")
        if ask is None:
            raise SystemExit(f"{text_q}. Write it as the address's scheme:"
                             f" {default or 'postgres'}://{user}{host}:{p}"
                             f"{rest}")
        engine = (ask(text_q, default=default) or default).strip().lower()
        if len(found) > 1:
            p = next((q for q, h in found if h["engine"] == engine), p)
    from .engines import SCHEMES
    if engine not in SCHEMES:
        raise SystemExit(f"{engine!r} is no engine migkit knows")
    return f"{engine}://{user}{host}:{p}{rest}"
