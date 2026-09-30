"""The redo-file reader: Oracle's redo read from its files by a program of
its own, run next to them, as the rung above LogMiner (backlog F5).

The program parses the online and archived redo itself - no LogMiner
session on the database, no mining in its memory - so it keeps up where
the redo comes faster than LogMiner reads it back, and it costs the
database nothing. It runs where the redo files are (the database host, or
a host they are mounted on); it is C++ for Linux x86-64, so migkit drives
it as a separate program and speaks its network protocol to it:
* a request is a protobuf `RedoRequest` - INFO, START from an SCN,
  CONTINUE from a confirmed position, CONFIRM what was applied;
* each message, both ways, is its length as four little-endian bytes (or
  0xFFFFFFFF and eight) and then the message;
* the stream it answers with is JSON documents, one a begin, a row change,
  a commit or a checkpoint, each with the position to confirm it by.

The writer holds what it sent until migkit confirms it, so a tail that
stops and comes back is sent again what it had not applied: migkit
confirms a batch at its next read, which is after the batch was applied
and its position saved. What comes again is left out by the same commit
SCN and transactions the LogMiner reader keeps (`oracle_logminer`).

Which reader, migkit decides (`chosen`): the one the operator runs next to
the redo where the hop names where it listens (`redo_reader: host:port`
on the source), or one migkit starts itself where this machine can read
the redo files and has the program; LogMiner otherwise. The program is put
in place by `migkit doctor --install` on a Linux x86-64 machine.
"""
import datetime
import json
import os
import platform
import shutil
import socket
import struct
import subprocess
import time
from decimal import Decimal

from . import oracle_logminer as miner

RUNG = "redo-files"
LOGMINER = "logminer"
#: the program's name, and the schema version of the configuration the
#: build `doctor --install` puts in place reads
PROGRAM = "OpenLogReplicator"
VERSION = "2.0.0"


def home():
    """Where the build `doctor --install` makes is kept: beside the other
    programs migkit builds for itself."""
    from ..tools import tools_home
    return os.path.join(str(tools_home()), "redo-reader")

# the protocol's codes (proto/OraProtoBuf.proto)
INFO, START, CONTINUE, CONFIRM = 0, 1, 2, 3
READY, FAILED_START, STARTING, ALREADY_STARTED, REPLICATE, PAYLOAD, \
    INVALID_DATABASE, INVALID_COMMAND = range(8)
#: "start from now" in the protocol: the largest SCN
NOW = 0xFFFFFFFFFFFFFFFF
#: the source flag that includes checkpoint records in the output
CHECKPOINTS = 0x1000
LONG = 0xFFFFFFFF


# ---- the wire ------------------------------------------------------------

def _varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def request(code, database, scn=None, c_scn=None, c_idx=None, seq=None):
    """A `RedoRequest`, written field by field: code (1), database name
    (2), start SCN (3), sequence (6), confirmed SCN (8) and index (9)."""
    out = bytearray(_varint(1 << 3) + _varint(code))
    name = database.encode()
    out += _varint((2 << 3) | 2) + _varint(len(name)) + name
    for field, value in ((3, scn), (6, seq), (8, c_scn), (9, c_idx)):
        if value is not None:
            out += _varint(field << 3) + _varint(int(value))
    return bytes(out)


def _read_varint(data, at):
    shift = n = 0
    while True:
        b = data[at]
        at += 1
        n |= (b & 0x7F) << shift
        if not b & 0x80:
            return n, at
        shift += 7


def response(data):
    """{field number: value} of a `RedoResponse`: code (1), SCN (2),
    confirmed SCN (10) and index (11); length-delimited fields as bytes."""
    out, at = {}, 0
    while at < len(data):
        head, at = _read_varint(data, at)
        field, kind = head >> 3, head & 7
        if kind == 0:
            value, at = _read_varint(data, at)
        elif kind == 2:
            n, at = _read_varint(data, at)
            value, at = data[at:at + n], at + n
        elif kind == 1:
            value, at = struct.unpack_from("<Q", data, at)[0], at + 8
        elif kind == 5:
            value, at = struct.unpack_from("<I", data, at)[0], at + 4
        else:
            raise ValueError(f"unknown protobuf wire type {kind}")
        out.setdefault(field, value)
    out.setdefault(1, READY)
    return out


def frame(body):
    if len(body) < LONG:
        return struct.pack("<I", len(body)) + body
    return struct.pack("<I", LONG) + struct.pack("<Q", len(body)) + body


class Stream:
    """One connection to the reader's writer."""

    def __init__(self, uri, timeout=15.0):
        host, _, port = uri.rpartition(":")
        self.sock = socket.create_connection((host or "127.0.0.1",
                                              int(port)), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def send(self, body):
        self.sock.sendall(frame(body))

    def _exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            got = self.sock.recv(n - len(buf))
            if not got:
                raise ConnectionError("the redo-file reader closed the"
                                      " connection")
            buf += got
        return bytes(buf)

    def receive(self, wait=None):
        """The next message's bytes, or None where nothing came within
        `wait` seconds."""
        self.sock.settimeout(wait)
        try:
            head = self.sock.recv(4, socket.MSG_PEEK)
        except (socket.timeout, BlockingIOError):
            return None
        finally:
            self.sock.settimeout(None)
        if not head:
            raise ConnectionError("the redo-file reader closed the"
                                  " connection")
        n = struct.unpack("<I", self._exact(4))[0]
        if n == LONG:
            n = struct.unpack("<Q", self._exact(8))[0]
        return self._exact(n)


# ---- what to run, and where ----------------------------------------------

def program():
    """The installed program, or None."""
    for path in (os.path.join(home(), "bin", PROGRAM),
                 shutil.which(PROGRAM)):
        if path and os.access(path, os.X_OK):
            return path
    return None


def platform_runs_it():
    """Linux on x86-64: the platforms the program is built for."""
    return (platform.system() == "Linux"
            and platform.machine().lower() in ("x86_64", "amd64"))


def redo_here(eng, side, db):
    """Whether this machine can read every online redo log of the source:
    what a reader started here needs."""
    try:
        members = [m for (m,) in eng._rows(side, db, "select member from"
                                                     " v$logfile")]
    except Exception:  # noqa: BLE001 - not readable: not here
        return False
    return bool(members) and all(os.access(m, os.R_OK) for m in members)


def chosen(eng, side, db):
    """Which reader follows this source.

    * `MIGKIT_ORACLE_READER=logminer|redo-files` - forced, for a test or a
      diagnosis.
    * the source names where the operator's reader listens
      (`redo_reader: host:port`): it runs next to the redo because the
      operator put it there, and migkit uses it.
    * this machine reads the redo files and has the program on a platform
      it runs on: migkit starts it here.
    * LogMiner otherwise - in every edition, on a managed database, with
      nothing installed; `assess` measures it against the source's redo
      and says when it would fall behind.
    """
    return choice(eng, side, db).path


def readers():
    """The ways a source's redo is read, the first that holds taken."""
    from ..decide import Need, Rung
    return (
        Rung(RUNG, "the redo-file reader, beside the redo",
             needs=(Need("to run beside the source's redo",
                         lambda f: f["listening"] or (
                             f["platform"] and f["program"]
                             and f["redo here"]),
                         "{rung} is not run beside this source's redo: the"
                         " source names no redo_reader, and this machine"
                         " cannot read its redo files with the program"),),
             because="it reads the redo files themselves, so the database"
                     " does no mining for it",
             footprint=1),
        Rung(LOGMINER, "migkit's LogMiner reader",
             because="in every edition, on a managed database, with"
                     " nothing installed"))


def choice(eng, side, db):
    """The climb of `readers` for this source; `MIGKIT_ORACLE_READER`
    puts one first, to diagnose."""
    from ..decide import Facts, climb
    ep = eng._endpoint(side)
    rungs = readers()
    forced = os.environ.get("MIGKIT_ORACLE_READER", "").strip()
    if forced:
        if forced not in (RUNG, LOGMINER):
            raise SystemExit(f"MIGKIT_ORACLE_READER={forced} is not one of"
                             f" {LOGMINER}, {RUNG}")
        if forced == LOGMINER:
            rungs = tuple(r for r in rungs if r.name == LOGMINER)
        return climb(db, rungs, Facts({"listening": True}))
    return climb(db, rungs, Facts(
        {"listening": bool(ep.options.get("redo_reader"))},
        platform=lambda: platform_runs_it(), program=lambda: program(),
        **{"redo here": lambda: redo_here(eng, side, db)}))


def config(eng, side, db, listen, state):
    """The reader's configuration for this source: the database it reads,
    the schema it follows, and the network writer migkit connects to.
    Values are written so each reads back exactly: text as the hex of its
    UTF-8 (so a value the reader could not decode, which it writes as `?`,
    is never a string that holds one), every message with its commit's
    SCN, a time with a zone as nanoseconds since the epoch."""
    ep = eng._endpoint(side)
    name = eng._rows(side, db, "select name from v$database")[0][0]
    return {
        "version": VERSION,
        "log-level": 2,
        "memory": {"min-mb": 64, "max-mb": int(ep.options.get(
            "redo_reader_mb", 1024))},
        "state": {"type": "disk", "path": state},
        "source": [{
            "alias": "S",
            "name": str(name),
            # checkpoint records (off unless asked, `SHOW_CHECKPOINT`):
            # they say every commit before an SCN was sent, which is what
            # the fence waits on
            "flags": CHECKPOINTS,
            "reader": {"type": "online", "user": ep.user,
                       "password": ep.password,
                       # where the reader reaches the database from where
                       # it runs, where that is not how migkit does
                       "server": "//" + (ep.options.get("redo_reader_dsn")
                                         or eng._dsn(ep)).lstrip("/")},
            "format": {"type": "json", "column": 2, "scn-type": 5,
                       "xid": 1, "timestamp": 12, "timestamp-tz": 0,
                       "char": 2, "unknown-type": 1, "interval-dts": 8,
                       "message": 0, "schema": 0},
            "filter": {"table": [{"owner": eng._owner(side, db),
                                  "table": ".*"}]},
        }],
        "target": [{"alias": "T", "source": "S",
                    "writer": {"type": "network", "uri": listen}}],
    }


def _account():
    """What migkit asks of the reader, in the tool-use registry
    (`tooluse`): each key of its configuration (`config`) and each request
    of its protocol, used, topped up or not used, with the reason."""
    from ..tooluse import TOPPED, UNUSED, USED
    conf = {
        "version": (USED, "the configuration schema of the build `doctor"
                          " --install` makes, the one migkit speaks to"),
        "log-level": (USED, "its own log kept short, beside its state, read"
                            " where the reader does not come up"),
        "memory.min-mb": (USED, "the least it starts with, small beside the"
                                " database it runs next to"),
        "memory.max-mb": (USED, "the source's redo_reader_mb, 1024 where it"
                                " says nothing"),
        "state.type": (USED, "disk, under the hop's report directory, so a"
                             " reader started again goes on from its own"
                             " checkpoint"),
        "state.path": (USED, "one directory per source database, beside the"
                             " reader's log"),
        "source.alias": (USED, "the one source, named for its one writer"),
        "source.name": (USED, "the database's own name, read from it, which"
                              " every request of the protocol names too"),
        "source.flags": (TOPPED, "checkpoint records, off unless asked: each"
                                 " says every commit before an SCN was sent,"
                                 " which migkit's fence waits on"),
        "source.reader.type": (USED, "online: it names the redo's tables by"
                                     " the dictionary of the database it"
                                     " follows"),
        "source.reader.user": (USED, "the source's own user, as migkit signs"
                                     " in"),
        "source.reader.password": (USED, "the source's own password, in a"
                                         " file only this user reads"),
        "source.reader.server": (USED, "the source's address, or how the"
                                       " reader reaches it where that is not"
                                       " migkit's (redo_reader_dsn)"),
        "source.format": (USED, "the JSON shape `value_of` reads back"
                                " exactly: text as the hex of its UTF-8,"
                                " every message with its commit's SCN, a"
                                " time with a zone as nanoseconds since the"
                                " epoch"),
        "source.filter.table": (USED, "the schema the hop follows, every"
                                      " table of it; a table the hop leaves"
                                      " out is dropped as its change is"
                                      " read"),
        "target.alias": (USED, "the one writer, named for its one source"),
        "target.source": (USED, "the one source, as its alias names it"),
        "target.writer.type": (USED, "network: migkit reads the stream"
                                     " itself and confirms only what it"
                                     " applied, so the writer holds the"
                                     " rest"),
        "target.writer.uri": (USED, "a port free on this machine where"
                                    " migkit starts the reader, or where the"
                                    " source says one listens"
                                    " (redo_reader)"),
        "target.writer.type file, kafka, zeromq": (
            UNUSED, "a file or a broker between the reader and migkit would"
                    " hold the changes the confirm is for, and migkit"
                    " applies them itself"),
        "source.reader.type batch": (
            UNUSED, "a list of redo files read once is not a follow of the"
                    " redo as it is written"),
        "source.format.type protobuf": (
            UNUSED, "not yet: the JSON documents are read back exactly, and"
                    " nothing was measured to say the other form is"
                    " faster"),
    }
    protocol = {
        "INFO": (USED, "asked first, so a reader that reads another database"
                       " is refused before anything is read"),
        "START": (USED, "from the tail's own SCN where the writer has not"
                        " begun"),
        "CONTINUE": (USED, "from the position last confirmed where it has,"
                           " so nothing it sent is lost to a restart"),
        "CONFIRM": (TOPPED, "sent at the next read, once the batch before it"
                            " was applied and its position saved; what comes"
                            " again is left out by the commit SCN and the"
                            " transactions the tail keeps"),
    }
    return {**{("config", k): v for k, v in conf.items()},
            **{("protocol", k): v for k, v in protocol.items()}}


def _register():
    from .. import tooluse
    tooluse.register(PROGRAM, options=_account(), release=VERSION)


_register()


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def started_here(eng, side, db):
    """The address of a reader migkit started on this machine for this
    source: the one already running, or one started now."""
    work = eng.hop.report_dir(db) / "redo-reader"
    work.mkdir(parents=True, exist_ok=True)
    pidfile = work / "running.json"
    try:
        got = json.loads(pidfile.read_text())
        os.kill(int(got["pid"]), 0)
        return got["uri"]
    except (OSError, ValueError, KeyError):
        pass
    port = _free_port()
    uri = f"127.0.0.1:{port}"
    (work / "checkpoint").mkdir(exist_ok=True)
    path = work / f"{PROGRAM}.json"
    path.write_text(json.dumps(config(eng, side, db, uri,
                                      str(work / "checkpoint")), indent=1))
    os.chmod(path, 0o600)
    with open(work / "reader.log", "ab") as out:
        proc = subprocess.Popen([program(), "-f", str(path)], cwd=str(work),
                                stdout=out, stderr=subprocess.STDOUT,
                                start_new_session=True)
    pidfile.write_text(json.dumps({"pid": proc.pid, "uri": uri}))
    for _ in range(120):
        with socket.socket() as s:
            s.settimeout(1)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return uri
        if proc.poll() is not None:
            break
        time.sleep(0.5)
    raise SystemExit("the redo-file reader started here did not come up;"
                     f" what it said is in {work / 'reader.log'}")


# ---- reading its stream --------------------------------------------------

def value_of(kind, v):
    """The Python value of one column as the reader wrote it, for a column
    of `kind`, as the driver would have handed the column back; raises
    `miner.NotRead` where the table is asked instead."""
    if v is None:
        return None
    if v == "?":
        raise miner.NotRead(v)
    if kind in ("NUMBER", "FLOAT"):
        return v if isinstance(v, Decimal) else Decimal(str(v))
    if kind in ("BINARY_FLOAT", "BINARY_DOUBLE"):
        return float(v)
    if kind in ("VARCHAR2", "NVARCHAR2", "CLOB", "NCLOB"):
        return bytes.fromhex(str(v)).decode("utf-8")
    if kind in ("CHAR", "NCHAR"):
        return bytes.fromhex(str(v)).decode("utf-8").rstrip(" ")
    if kind in ("RAW", "BLOB"):
        return bytes.fromhex(str(v))
    if kind in ("DATE", "TIMESTAMP"):
        # every digit after the second, as a read keeps them
        from .. import nanotime
        return nanotime.parse(str(v).replace("T", " ").rstrip("Z"))
    if kind == "TIMESTAMP WITH TIME ZONE":
        # the instant and the zone it was written in, as a read hands it
        from .. import nanotime
        from .oracle import Zoned
        epoch, _, zone = str(v).partition(",")
        ns = int(epoch)
        at = (datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
              + datetime.timedelta(microseconds=ns // 1000))
        return Zoned.at(nanotime.of(at, ns % 1000), zone.strip().upper()
                        or "UTC")
    raise miner.NotRead(v)


def message(raw):
    return json.loads(raw, parse_float=Decimal)


class RedoFileReader:
    """The tail's reader where the redo-file reader runs. Same token as the
    LogMiner reader's, plus the position the writer was last confirmed
    at (`redo`)."""

    RUNG = RUNG
    #: seconds without a message after which a read hands back what it has
    IDLE = 1.0

    def __init__(self, eng, side, db):
        self.eng, self.side, self.db = eng, side, db
        self.stream = None
        self.pending, self.txn = [], []
        self.shapes = miner.Miner(eng, side, db)

    def close(self):
        if self.stream is not None:
            self.stream.close()
        self.stream = None
        self.shapes.close()

    def _uri(self):
        ep = self.eng._endpoint(self.side)
        return ep.options.get("redo_reader") or started_here(
            self.eng, self.side, self.db)

    def _database(self):
        return str(self.eng._rows(self.side, self.db, "select name from"
                                                      " v$database")[0][0])

    def _connect(self, token):
        """Connected, and the writer streaming from `token`: started from
        its SCN where the writer has not begun, continued from the
        position last confirmed where it has."""
        self.stream = Stream(self._uri())
        name = self._database()
        self.stream.send(request(INFO, name))
        got = response(self.stream.receive(30) or b"")
        code = got.get(1)
        if code == INVALID_DATABASE:
            raise SystemExit(f"the redo-file reader at {self._uri()} reads"
                             f" another database than {name}")
        if code == READY:
            self.stream.send(request(START, name, scn=int(token["scn"])))
        else:
            at = token.get("redo") or {}
            self.stream.send(request(CONTINUE, name,
                                     c_scn=int(at.get("c_scn") or 0),
                                     c_idx=int(at.get("c_idx") or 0)))
        for _ in range(600):
            got = response(self.stream.receive(60) or b"")
            if got.get(1) == REPLICATE:
                return
            if got.get(1) != STARTING:
                raise SystemExit(
                    "the redo-file reader would not stream from SCN"
                    f" {token['scn']} (answer {got.get(1)}): the redo it"
                    " needs may be gone, or the account lacks its grants")
        raise SystemExit("the redo-file reader never began streaming")

    def read(self, token, limit=1000):
        """(changes, token): whole transactions until about `limit`
        changes, or until the writer has been quiet `IDLE` seconds. A
        transaction still open when a read ends is held here, with the
        messages of it already received, and finished by the next."""
        token = dict(token)
        if self.stream is None:
            self.txn = []
            self._connect(token)
        at = token.get("redo") or {}
        if at.get("c_scn"):
            # the batch before this read was applied, and its position
            # saved: the writer may let it go
            self.stream.send(request(CONFIRM, self._database(),
                                     c_scn=int(at["c_scn"]),
                                     c_idx=int(at.get("c_idx") or 0)))
        done_commit = int(token.get("commit") or 0)
        done = set(token.get("xids") or [])
        out, position = [], dict(at)
        while True:
            raw = self.pending.pop(0) if self.pending else \
                self.stream.receive(self.IDLE)
            if raw is None:
                break
            doc = message(raw)
            where = {"c_scn": doc.get("c_scn"), "c_idx": doc.get("c_idx")}
            for p in doc.get("payload") or []:
                op = p.get("op")
                if op == "chkpt":
                    # every commit before this SCN has been sent
                    token["through"] = max(int(token.get("through") or 0),
                                           int(doc.get("scn") or 0) - 1)
                    if not self.txn:
                        position = where
                elif op == "begin":
                    self.txn = []
                elif op in ("c", "u", "d"):
                    self.txn.append((doc, p))
                elif op == "commit":
                    commit = int(doc.get("scn") or 0)
                    xid = doc.get("xid")
                    if not (commit < done_commit or (
                            commit == done_commit and xid in done)):
                        out += self._changes(self.txn, commit)
                        if commit != done_commit:
                            done_commit, done = commit, set()
                        done.add(xid)
                    self.txn = []
                    position = where
            if not self.txn and len(out) >= limit:
                break
        token.update(commit=done_commit, xids=sorted(done), redo=position)
        return out, token

    def _changes(self, txn, commit):
        out, fetch = [], {}
        for doc, p in txn:
            schema = p.get("schema") or {}
            table = self.eng._said(schema.get("table", ""))
            if self.eng.hop.excluded(self.db, table):
                continue
            kinds, key = self.shapes._shape(table)
            after = {self.eng._said(n): v for n, v in
                     (p.get("after") or {}).items()}
            before = {self.eng._said(n): v for n, v in
                      (p.get("before") or {}).items()}
            values, missing = {}, []
            for n, v in after.items():
                try:
                    values[n] = value_of(kinds.get(n, ""), v)
                except miner.NotRead:
                    missing.append(n)
            old, old_missing = {}, []
            for n, v in before.items():
                try:
                    old[n] = value_of(kinds.get(n, ""), v)
                except miner.NotRead:
                    old_missing.append(n)
            change, missing = miner.row_change(
                self.db, table, {"c": "insert", "u": "update",
                                 "d": "delete"}[p["op"]], kinds, key,
                values, missing, old, old_missing, commit)
            out.append(change)
            if missing:
                fetch.setdefault(table, []).append((change, missing))
        for table, wanted in fetch.items():
            self.shapes._as_of(table, commit, wanted)
        return out
