"""Which engine answers at an address that names none (backlog
P0-MINIMAL-INPUT).

`app@db1.example.com:5432/shop`, `db1.example.com:3306` or a bare host is
an address too: the engine is read off the server's own first words on
its port, never guessed from the port. Each protocol is asked the one
question it answers before anyone signs in - PostgreSQL its request for
TLS, SQL Server its pre-login, MongoDB `hello`, Redis `PING`, Kafka its
API versions, an HTTP server `GET /`, Oracle's listener a connect
packet, Cassandra `OPTIONS`, Db2 its attribute exchange, Teradata's
gateway its configuration - and MySQL is heard, as it speaks first. The port only orders the asking (its own engine first,
`engines.PORTS`) and is the default of the one question asked where no
answer names an engine. A host with no port is knocked on at every
engine's own port at once.

Which knock is tried and when to fall to the question is a climb of the
decision engine (`decide.climb`): each knock a rung proved by the
server's answer, the question the last rung, held only where there is a
terminal to ask at.

A wire several servers speak names its family only: MariaDB, TiDB and
OceanBase greet as MySQL does, CockroachDB and Aurora DSQL answer TLS as
PostgreSQL does. Once signed in, the server's own version text says which
one it is (`variants.identify`, the one place a brand is told): a rung
above the family's on the same ladder, proved by what the server said
after sign-in, the family it greeted as standing where it says nothing
more (`rebrand`).
"""
import json
import re
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


#: where a connect packet's descriptor begins, its fixed part before it
_TNS_DATA_AT = 58


def _tns_hello(host, port):
    data = ("(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=)(CID=(PROGRAM=migkit)"
            f"(HOST=)(USER=)))(ADDRESS=(PROTOCOL=TCP)(HOST={host})"
            f"(PORT={port})))").encode()
    fixed = struct.pack(">HHHHHHHHHHIBB", 314, 300, 0, 0x2000, 0xffff,
                        0x7f08, 0, 0x0100, len(data), _TNS_DATA_AT, 0,
                        0x41, 0x41)
    body = fixed + b"\0" * (_TNS_DATA_AT - 8 - len(fixed))
    size = 8 + len(body) + len(data)
    return struct.pack(">HHBBH", size, 0, 1, 0, 0) + body + data


def _hear_tns(b):
    """Oracle's listener answers a connect packet (NSPTCN) with a packet
    of its own kind - accept, refuse, redirect or resend - and a refusal
    carries the listener's version (`VSNNUM`)."""
    if len(b) < 8:
        return None
    size, kind = struct.unpack(">H", b[:2])[0], b[4]
    if kind not in (2, 4, 5, 11) or size < 8 or b[5] != 0 \
            or b[2:4] not in (b"\0\0", b"\xff\xff"):
        return None
    m = re.search(rb"VSNNUM=(\d+)", b)
    if not m:
        return {}
    v = int(m.group(1))
    return {"version": f"{v >> 24}.{(v >> 20) & 0xf}.{(v >> 12) & 0xff}"}


def _cql_hello(_host, _port):
    return bytes([0x04, 0, 0, 0, 0x05]) + struct.pack(">i", 0)


def _hear_cql(b):
    """Cassandra's native protocol answers `OPTIONS` with `SUPPORTED`: the
    versions it speaks, a string multimap holding `CQL_VERSION` - or, a
    server older than the version asked, an error frame in its own."""
    if len(b) < 9 or not b[0] & 0x80 or not 1 <= b[0] & 0x7f <= 6 \
            or b[2:4] != b"\0\0":
        return None
    op, size = b[4], struct.unpack(">i", b[5:9])[0]
    if op not in (0x00, 0x06) or not 0 <= size <= 1 << 16 \
            or len(b) < 9 + size:
        return None
    if op == 0x00:
        return {"note": f"native protocol v{b[0] & 0x7f}"} if size else None
    body, at, got = b[9:9 + size], 2, {}
    try:
        for _ in range(struct.unpack(">H", body[:2])[0]):
            n = struct.unpack(">H", body[at:at + 2])[0]
            key = body[at + 2:at + 2 + n].decode()
            at += 2 + n
            vals, count = [], struct.unpack(">H", body[at:at + 2])[0]
            at += 2
            for _ in range(count):
                m = struct.unpack(">H", body[at:at + 2])[0]
                vals.append(body[at + 2:at + 2 + m].decode())
                at += 2 + m
            got[key] = vals
    except (struct.error, UnicodeDecodeError):
        return None
    if not got.get("CQL_VERSION"):
        return None
    return {"version": got["CQL_VERSION"][-1],
            "note": "CQL " + ", ".join(got["CQL_VERSION"])}


def _ddm(codepoint, data):
    return struct.pack(">HH", 4 + len(data), codepoint) + data


def _drda_hello(_host, _port):
    levels = b"".join(struct.pack(">HH", cp, lvl) for cp, lvl in (
        (0x1403, 7), (0x2407, 7), (0x240f, 7), (0x1440, 7),
        (0x1474, 5)))
    ddm = _ddm(0x1041, _ddm(0x115e, "migkit".encode("cp500"))
               + _ddm(0x1147, "migkit".encode("cp500"))
               + _ddm(0x1404, levels))
    return struct.pack(">HBBH", 6 + len(ddm), 0xd0, 0x01, 1) + ddm


#: a release level's first letters, and the Db2 that answers with them
_DB2_LEVELS = {"SQL": "db2", "DSN": "db2zos", "QSQ": "db2i"}


def _hear_drda(b):
    """Db2 answers the attribute exchange (EXCSAT) with its own
    (EXCSATRD): its server class and release level - `SQL11055` Db2 for
    Linux, UNIX and Windows 11.5.5, `DSN...` Db2 for z/OS, `QSQ...` Db2
    for i. Another server on the same protocol answers too, and is no
    Db2."""
    if len(b) < 10 or b[2] != 0xd0:
        return None
    size = struct.unpack(">H", b[:2])[0]
    if len(b) < size or struct.unpack(">H", b[8:10])[0] != 0x1443:
        return None
    params, at = {}, 10
    while at + 4 <= size:
        n, cp = struct.unpack(">HH", b[at:at + 4])
        if n < 4:
            break
        params[cp] = b[at + 4:at + n]
        at += n
    level = params.get(0x115a, b"").decode("cp500", "replace").strip()
    engine = _DB2_LEVELS.get(level[:3])
    if engine is None:
        return None
    digits = level[3:]
    version = (f"{int(digits[:2])}.{int(digits[2:4])}.{digits[4:]}"
               if len(digits) >= 5 and digits.isdigit() else digits)
    return {"engine": engine, "version": version,
            "note": params.get(0x1147, b"").decode("cp500",
                                                   "replace").strip()}


#: a Teradata gateway message's header, its versions, and the class and
#: kind of the configuration request asked before any session is
#: assigned. Read from the protocol's published layout and not yet heard
#: from a real gateway: unverified against a server
_TD_HEADER = 52
_TD_VERSIONS = (3, 4)
_TD_CONFIG = (1, 13)
#: what the knock heard, said with its doubt: every report of it is one
TD_SAID = ("gateway's answer to a configuration request, read by the"
           " protocol's published layout and not yet verified against a"
           " real gateway")


def _td_hello(_host, _port):
    head = bytearray(_TD_HEADER)
    head[0], head[1], head[2] = _TD_VERSIONS[0], *_TD_CONFIG
    return bytes(head)


def _hear_td(b):
    """The gateway answers the configuration request in a message of its
    own header's shape: a version it speaks, the request's class, and a
    body as long as the header says - never empty, as the request is, so
    a server that sends back what it was sent is no gateway."""
    if len(b) < _TD_HEADER or b[0] not in _TD_VERSIONS \
            or b[1] != _TD_CONFIG[0]:
        return None
    size = (struct.unpack(">H", b[3:5])[0] << 16) \
        | struct.unpack(">H", b[8:10])[0]
    if not 0 < size <= 1 << 20 or len(b) - _TD_HEADER != size:
        return None
    return {"note": f"gateway message version {b[0]}"}


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
    Knock("oracle", "listener's answer to a connect packet",
          ("oracle", "oceanbase-oracle"), _tns_hello, _hear_tns),
    Knock("cassandra", "answer to OPTIONS", ("cassandra", "keyspaces",
                                             "geminidb-cassandra"),
          _cql_hello, _hear_cql),
    Knock("db2", "answer to an attribute exchange", ("db2", "db2zos",
                                                     "db2i"),
          _drda_hello, _hear_drda),
    Knock("teradata", TD_SAID, ("teradata",), _td_hello, _hear_td),
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


def _brands(family):
    """Every brand `variants` tells apart on `family`'s wire that is an
    address's scheme of its own, the family's own software left out."""
    from .engines import SCHEMES
    from .variants import SIGNATURES
    return [name for name, _, _ in SIGNATURES.get(family, ())
            if name != family and name in SCHEMES]


def _said_after_sign_in(family, host, port, user, password, db):
    """What the server says of itself once signed in - its version text,
    and its endpoint's name, which is how Aurora DSQL is told - or why it
    could not be asked."""
    try:
        if family == "mysql":
            import pymysql
            c = pymysql.connect(host=host, port=int(port), user=user,
                                password=password or "", database=db or None,
                                connect_timeout=int(WAIT * 4))
            sql = "select @@version, @@version_comment"
            names = ("version", "version_comment")
        elif family == "postgres":
            import psycopg2
            c = psycopg2.connect(host=host, port=int(port), user=user,
                                 password=password or "",
                                 dbname=db or "postgres",
                                 connect_timeout=int(WAIT * 4))
            sql = "select version(), current_setting('server_version')"
            names = ("version", "server_version")
        else:
            return f"no sign-in on the {family} wire here"
        try:
            cur = c.cursor()
            cur.execute(sql)
            row = cur.fetchone()
        finally:
            c.close()
    except Exception as e:  # noqa: BLE001 - said, the family stands
        return str(e).strip().splitlines()[0][:160] if str(e).strip() \
            else type(e).__name__
    return {**{n: str(v or "") for n, v in zip(names, row)},
            "endpoint": host}


def _ladder(family, hear, sign_in, heard, needs=()):
    """The rungs `family`'s wire climbs: each brand it carries, proved by
    what the server says after sign-in, above the family's own knock.
    `hear(facts)` is the knock's proof; `sign_in()` what the server said
    after sign-in, or why it could not be asked."""
    from . import decide
    from .variants import identify as told
    said = {}

    def after():
        if "got" not in said:
            said["got"] = sign_in()
        return said["got"]

    def brand(name):
        def run(f):
            why = hear(f)
            if why:
                return why
            got = after()
            if isinstance(got, str):
                return f"signing in to read its version failed: {got}"
            b = told(family, got)
            if b.name != name:
                return (f"its version text names {b.name}"
                        if b.name not in ("unknown", family)
                        else f"its version text does not name {name}")
            base = heard.get(family, {})
            heard[f"{family}/{name}"] = {
                "engine": name, "version": b.version or base.get(
                    "version", ""), "note": b.evidence,
                "said": "version text after sign-in"}
            return None
        return run
    signed = decide.Need("a sign-in", lambda f: f["sign_in"],
                         "nobody has signed in yet: the family it greets"
                         " as stands")
    return [decide.Rung(f"{family}/{name}", f"{name}, by its version text"
                                            " after sign-in",
                        needs=tuple(needs) + (signed,), prove=brand(name))
            for name in _brands(family)]


def identify(host, port, can_ask=False, sign_in=None):
    """The `decide.Choice` of how the engine at `host:port` is known and
    what it said: (choice, {"engine", "version", "note"} or None).
    `sign_in(family)` - what the server says of itself once signed in, or
    why it could not be asked - lets a brand on a shared wire rise above
    the family it greeted as."""
    from . import decide
    heard, knocked = {}, {}

    def prove(k):
        def run(_facts):
            if k.family not in knocked:
                try:
                    got = k.hear(_exchange(host, port, k.hello(host, port)
                                           if k.hello else None, k.hear,
                                           k.whole))
                except (OSError, ValueError, struct.error):
                    got = None
                knocked[k.family] = got
                if got is not None:
                    heard[k.family] = {
                        "engine": got.get("engine") or k.family,
                        "version": got.get("version", ""),
                        "note": got.get("note", ""), "said": k.said}
            if knocked[k.family] is None:
                return f"it did not answer as {k.family} does"
            return None
        return run
    reached = decide.Need("the server reached", lambda f: f["reached"],
                          f"nothing answers at {host}:{port}")
    rungs = []
    for k in _ordered(port):
        hear = prove(k)
        rungs += _ladder(k.family, hear, lambda f=k.family: sign_in(f),
                         heard, needs=(reached,))
        rungs.append(decide.Rung(k.family, f"the server's own {k.said}",
                                 needs=(reached,), prove=hear))
    rungs.append(decide.Rung(
        "asked", "the operator's answer",
        needs=(decide.Need("a terminal to ask at",
                           lambda f: f["can_ask"]),)))
    facts = {"reached": _reachable(host, port), "can_ask": can_ask,
             "sign_in": sign_in is not None}
    choice = decide.climb(f"the engine at {host}:{port}", rungs, facts)
    return choice, heard.get(choice.path)


def rebrand(text, password, say):
    """`text`, an address `address` wrote, with the brand the server names
    once signed in as its scheme: `mysql://` a MariaDB, TiDB or OceanBase
    says it is, `postgres://` a CockroachDB or Aurora DSQL. The same
    ladder `identify` climbs, from the family the scheme already holds;
    an address on any other wire, or a server that says nothing more, is
    kept as it is."""
    from . import decide
    family, sep, rest = text.partition("://")
    if not sep or not _brands(family):
        return text
    user, host, port, tail = _split(rest)
    port = port or _default_port(family)
    if not port or not _reachable(host, port):
        return text
    login = user.rstrip("@").partition(":")[0]
    db = tail.lstrip("/").partition("?")[0].partition("/")[0]
    heard = {family: {"engine": family, "version": "", "note": ""}}
    signed = []

    def sign_in():
        signed.append(_said_after_sign_in(family, host, port, login,
                                          password, db))
        return signed[-1]
    rungs = _ladder(family, lambda f: None, sign_in, heard)
    rungs.append(decide.Rung(family, f"the {family} it greeted as"))
    choice = decide.climb(f"the brand at {host}:{port}", rungs,
                          {"sign_in": True})
    got = heard.get(choice.path)
    if choice.path == family or got is None:
        # the operator's sign-in was tried and refused: said, not swallowed
        if signed and isinstance(signed[0], str):
            say(f"{host}:{port}: signing in to read its version failed"
                f" ({signed[0]}); kept as the {family} it greeted as")
        return text
    version = f" {got['version']}" if got["version"] else ""
    say(f"{host}:{port} signed in as {got['engine']}{version} (its own"
        f" version text; it greeted as {family})")
    return f"{got['engine']}://{rest}"


def _default_port(family):
    from .engines import PORTS
    return PORTS.get(family)


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
