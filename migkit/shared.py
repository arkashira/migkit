"""Where the machines of one hop agree: records each written only over the
version that was read (backlog W1).

A standby that takes a tail over, and machines that share one table's
ranges, decide together through records nobody may overwrite blind: a
write names the version it read and loses where another machine wrote
since - it then reads again and decides again. Three places keep such
records, through the same calls (`read_record`, `write_record`, `get`,
`put`, `delete`) the run state's bucket already answers:

    bucket     the run state's bucket (`state: {backend: s3}`): the
               bucket's own conditional put - If-None-Match to make a
               record, If-Match over the version read
    directory  a directory every machine sees (`state: {shared: <path>}`):
               a file a record, changed under an exclusive lock on the
               file beside it, its version a number kept in it - on one
               machine, or on a shared mount whose locks reach every
               client
    target     a row a record in the target's `migkit_origin`, the table
               the tail's batch marks already keep: a record made where
               its row is already there loses, and an update names the
               version it read (`where n = <version>`)

Which one is not measured machine by machine: every machine of the hop
has to find the same one, or two machines each holding "the" lease in two
places would both write. So the order is the hop's own - the bucket where
its run state is kept in one, the directory where it names one, else the
target (`choose`) - read from the hop file alone. The one chosen is
proved before it is trusted (`prove`): a record made, made again, written
over the version read and then over that version again must lose the
second and the last time; a store that lets either through - an
S3-compatible store that ignores a condition - is said and not used, and
nothing falls through to the next place, since another machine may have
proved the first. The fence the target keeps (`fencing`) names the store
its holder's lease is in, so a machine whose hop file names another place
is refused there too.

Records are small JSON. What a tail cannot go on without that is not text
- the kept proof, a parked table's spool - is kept by its digest in the
bucket or the directory (`put_blob`); the target keeps none, and a hop
that needs them is not given the target (`BLOBS`).
"""
import base64
import contextlib
import hashlib
import json
import os
import threading
import time
from pathlib import Path

from .state import Taken, _seal, _unseal

#: what the machines of a hop need of a store: records every machine
#: reads and writes over one another
ACROSS = "records every machine of the hop reads and writes"
#: and where the tail keeps files that are not text
BLOBS = ("the tail's files that are not text (the kept proof, a parked"
         " table's changes) kept beside its position")


def together(hop):
    """Whether this hop's machines work together: a standby waits to take
    it over, or machines share its tables."""
    opts = hop.options or {}
    return bool(opts.get("standby")) or bool(opts.get("share_tables"))


def _encode(record, sealed):
    body = json.dumps(record, default=str)
    if sealed and os.environ.get("MIGKIT_STATE_KEY"):
        return "sealed:" + base64.b64encode(
            _seal(body.encode(), often=True)).decode()
    return body


def _decode(text, sealed=False):
    if text is None:
        return None
    if text.startswith("sealed:"):
        text = _unseal(base64.b64decode(text[7:])).decode()
    try:
        return json.loads(text)
    except ValueError:
        return None


def _rel(hop, path):
    """A path under the hop's reports, as every machine names it."""
    root = Path(hop.report_dir())
    try:
        return Path(path).relative_to(root).as_posix()
    except ValueError:
        return Path(path).name


#: one lock a file for the threads of this process: a file lock is the
#: process's, and two threads of one process would each be given it
_LOCKS = {}
_LOCKS_GUARD = threading.Lock()


def _thread_lock(path):
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(str(path), threading.Lock())


class Directory:
    """Records as files in a directory every machine of the hop sees,
    each changed under an exclusive lock on the file beside it."""

    kind = "directory"
    blobs = True

    def __init__(self, hop, root):
        self.hop = hop
        self.root = Path(root).expanduser() / hop.name

    def ident(self):
        return f"directory:{self.root}"

    def _file(self, path):
        return self.root / "run" / _rel(self.hop, path)

    @contextlib.contextmanager
    def _locked(self, f):
        import fcntl
        f.parent.mkdir(parents=True, exist_ok=True)
        guard = f.with_name(f.name + ".guard")
        with _thread_lock(guard):
            fd = os.open(guard, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                yield
            finally:
                os.close(fd)

    @staticmethod
    def _read(f):
        try:
            got = json.loads(f.read_text())
        except (OSError, ValueError):
            return None, None
        return got.get("body"), got.get("version")

    @staticmethod
    def _write(f, body, version):
        # whole or not at all, and on the disk before the lock is let go:
        # a record another machine reads next must survive this one's
        # power loss (`durable`)
        from . import durable
        durable.write_json(f, {"version": version, "body": body})

    def read_record(self, path, sealed=False):
        f = self._file(path)
        with self._locked(f):
            body, version = self._read(f)
        if body is None:
            return None, None
        return _decode(body, sealed), version

    def write_record(self, path, record, version, sealed=False):
        f = self._file(path)
        with self._locked(f):
            _, have = self._read(f)
            if have != version:
                raise Taken()
            self._write(f, _encode(record, sealed), int(have or 0) + 1)

    def get(self, path):
        f = self._file(path)
        with self._locked(f):
            body, _ = self._read(f)
        if body is None:
            return None
        return _unseal(base64.b64decode(body)).decode()

    def put(self, path, text):
        f = self._file(path)
        with self._locked(f):
            _, have = self._read(f)
            self._write(f, base64.b64encode(
                _seal(text.encode(), often=True)).decode(),
                int(have or 0) + 1)

    def delete(self, path):
        f = self._file(path)
        with self._locked(f):
            f.unlink(missing_ok=True)

    def put_blob(self, data):
        digest = hashlib.sha256(data).hexdigest()
        f = self.root / "blob" / digest
        if not f.exists():
            from . import durable
            durable.write_atomic(f, _seal(data, often=True))
        return digest

    def get_blob(self, digest):
        return _unseal((self.root / "blob" / digest).read_bytes())


#: the statements of the target store, by the dialect the engine speaks
#: (`Engine.OWN_DIALECT`)
TARGET_SQL = {
    "postgres": {
        "make": "insert into {t} (origin, n, seen) values (%s, 1, %s)"
                " on conflict (origin) do nothing",
        "put": "insert into {t} as o (origin, n, seen) values (%s, 1, %s)"
               " on conflict (origin) do update set n = o.n + 1,"
               " seen = excluded.seen"},
    "mysql": {
        "make": "insert ignore into {t} (origin, n, seen) values"
                " (%s, 1, %s)",
        "put": "insert into {t} (origin, n, seen) values (%s, 1, %s)"
               " on duplicate key update n = n + 1, seen = values(seen)"},
}
#: the longest name a record's row may have: MySQL's key of it is 191
#: characters, the most a utf8mb4 key of 767 bytes holds
NAME_MOST = 191


class Target:
    """Records as rows of `migkit_origin` on the target, in one of the
    hop's databases there: the row's `n` is its version, `seen` its
    body."""

    kind = "target"
    blobs = False

    def __init__(self, hop, engine, db):
        self.hop, self.eng, self.db = hop, engine, db

    def ident(self):
        ep = self.hop.target
        return (f"target:{getattr(ep, 'host', '')}:{getattr(ep, 'port', '')}"
                f"/{self.hop.target_db(self.db)}")

    def origin(self, path):
        name = f"{self.hop.name}#{_rel(self.hop, path)}"
        if len(name) > NAME_MOST:
            name = (f"{self.hop.name[:100]}#"
                    + hashlib.sha256(name.encode()).hexdigest()[:40])
        return name

    def _run(self, fn):
        conn, table = self.eng.own_table("dst", self.db)
        try:
            with contextlib.closing(conn.cursor()) as cur:
                got = fn(cur, table)
            conn.commit()
            return got
        except BaseException:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001 - it is going anyway
                pass
            raise
        finally:
            conn.close()

    def _sql(self, name):
        return TARGET_SQL[self.eng.OWN_DIALECT][name]

    def read_record(self, path, sealed=False):
        def read(cur, t):
            cur.execute(f"select n, seen from {t} where origin = %s",
                        (self.origin(path),))
            return cur.fetchone()
        got = self._run(read)
        if not got or got[1] is None:
            return None, None
        return _decode(got[1], sealed), int(got[0])

    def write_record(self, path, record, version, sealed=False):
        body = _encode(record, sealed)

        def write(cur, t):
            if version is None:
                cur.execute(self._sql("make").format(t=t),
                            (self.origin(path), body))
            else:
                cur.execute(f"update {t} set n = n + 1, seen = %s where"
                            " origin = %s and n = %s",
                            (body, self.origin(path), int(version)))
            return cur.rowcount
        if self._run(write) != 1:
            raise Taken()

    def get(self, path):
        def read(cur, t):
            cur.execute(f"select seen from {t} where origin = %s",
                        (self.origin(path),))
            return cur.fetchone()
        got = self._run(read)
        if not got or got[0] is None:
            return None
        return _unseal(base64.b64decode(got[0])).decode()

    def put(self, path, text):
        body = base64.b64encode(_seal(text.encode(), often=True)).decode()
        self._run(lambda cur, t: cur.execute(
            self._sql("put").format(t=t), (self.origin(path), body)))

    def delete(self, path):
        self._run(lambda cur, t: cur.execute(
            f"delete from {t} where origin = %s", (self.origin(path),)))

    def put_blob(self, data):
        raise SystemExit("the target keeps records of migkit's, not files")

    get_blob = put_blob


def _cfg(hop):
    return dict((hop.options or {}).get("state") or {})


def _target_class(hop):
    """The class of the engine that writes the hop's target, read from
    the hop alone: nothing is connected to."""
    from .engines import ALIASES, _class_for
    opts = hop.options or {}
    name = ((opts.get("target_engine") or "postgres")
            if hop.engine == "hetero" else hop.engine) or ""
    return _class_for(ALIASES.get(name, name))


def _target_of(hop):
    """(the engine that writes the target, the database whose target
    keeps the hop's records): the hop's first database, in its own
    order."""
    from .engines import get_engine
    eng = get_engine(hop)
    dst = getattr(eng, "dst_engine", None) or eng
    dbs = list(hop.databases or []) or list(eng.databases() or [])
    if not dbs:
        raise SystemExit(f"{hop.name}: no database to keep the machines'"
                         " records in on the target")
    return dst, sorted(dbs)[0]


def _rungs():
    from .decide import Need, Rung
    return (
        Rung("bucket", "the run state's bucket, each record written only"
                       " over the version read",
             gives=frozenset({ACROSS, BLOBS}),
             needs=(Need("the hop's run state in a bucket",
                         lambda f: f["backend"] == "s3",
                         "the hop keeps its run state on this machine,"
                         " not in a bucket"),),
             because="the hop keeps its run state there"),
        Rung("directory", "files under a lock in a directory every machine"
                          " of the hop sees",
             gives=frozenset({ACROSS, BLOBS}),
             needs=(Need("a directory every machine sees",
                         lambda f: bool(f["shared"]),
                         "the hop names no directory every machine sees"
                         " (state: {shared: <path>})"),),
             because="the hop names a directory every machine sees"),
        Rung("target", "a table of migkit's own on the target"
                       " (migkit_origin), each row written only over the"
                       " version read",
             gives=frozenset({ACROSS}), footprint=1,
             needs=(Need("the target keeps migkit's table",
                         lambda f: f["target_keeps"],
                         lambda f: "the target keeps no table of migkit's"
                                   f" ({f['target_engine']})"),),
             because="every machine of the hop reaches the target"),
    )


def needs(hop):
    """What this hop's store must give: records every machine writes, and
    - where a standby carries the tail on - the tail's files that are not
    text: the kept proof, and the changes it sets aside for a table that
    joins it or is parked, which a hop may do at any batch (`tailtables`)
    however its `ddl` reads."""
    out = [ACROSS]
    if (hop.options or {}).get("standby"):
        out.append(BLOBS)
    return tuple(out)


def choose(hop):
    """(the store, the `decide.Choice`) for the hop's machines, from the
    hop file alone - never a probe's timing, which differs machine by
    machine (see the module's words)."""
    from . import decide
    cfg = _cfg(hop)
    cls = _target_class(hop)
    facts = decide.Facts(
        {"backend": cfg.get("backend", "local"), "shared": cfg.get("shared"),
         "target_keeps": callable(getattr(cls, "own_table", None)),
         "target_engine": (getattr(cls, "CANON_ENGINE", "") or hop.engine)})
    got = decide.climb(f"where {hop.name}'s machines agree", _rungs(), facts,
                       need=needs(hop))
    if got.rung is None:
        raise SystemExit(f"{hop.name}: its machines have nowhere to agree"
                         f" on who writes - {got.reason}. Keep the hop's"
                         " run state in a bucket (state: {backend: s3}), or"
                         " name a directory every machine sees (state:"
                         " {shared: <path>})")
    if got.rung.name == "bucket":
        from .state import S3RunState
        return S3RunState(hop, cfg), got
    if got.rung.name == "directory":
        return Directory(hop, cfg["shared"]), got
    eng, db = _target_of(hop)
    return Target(hop, eng, db), got


def prove(store, hop):
    """None where `store` refuses what it must - a record made twice, a
    write over a version already gone - else why not. What the proof
    writes is taken away after it."""
    import secrets
    path = Path(hop.report_dir()) / f"probe-{secrets.token_hex(6)}.json"
    try:
        store.write_record(path, {"probe": 1}, None)
        got, version = store.read_record(path)
        if got != {"probe": 1}:
            return "a record written was not read back as it was written"
        try:
            store.write_record(path, {"probe": 2}, None)
            return "a record made where one was already there was not refused"
        except Taken:
            pass
        store.write_record(path, {"probe": 3}, version)
        try:
            store.write_record(path, {"probe": 4}, version)
            return ("a record written over a version another write had"
                    " already replaced was not refused")
        except Taken:
            pass
        got, _ = store.read_record(path)
        if got != {"probe": 3}:
            return "a refused write changed the record all the same"
        return None
    except Taken:
        return "a write over the version just read was refused"
    finally:
        try:
            store.delete(path)
        except Exception:  # noqa: BLE001 - a probe left is harmless
            pass


#: {(hop, where its reports are): store}, chosen and proved once a process
_CHOSEN = {}
_CHOSEN_LOCK = threading.Lock()


def store_for(hop, log=None):
    """The hop's store, chosen and proved (`choose`, `prove`) the first
    time this process asks. A store that does not prove itself stops
    everything that needs one, with the reason."""
    key = (hop.name, str(Path(hop.report_dir()).resolve()))
    with _CHOSEN_LOCK:
        got = _CHOSEN.get(key)
    if got is not None:
        return got
    store, choice = choose(hop)
    began = time.monotonic()
    why = prove(store, hop)
    if why:
        raise SystemExit(
            f"{hop.name}: {choice.rung.said} cannot be trusted to keep one"
            f" writer at a time - {why}. Nothing was written. Keep the"
            " machines' records where writes are conditional (a bucket of"
            " a store that honours If-Match and If-None-Match, a shared"
            " directory whose locks reach every machine)")
    if log:
        log(f"the machines of {hop.name} agree through {choice.rung.said}"
            f" ({choice.reason}; proved in"
            f" {(time.monotonic() - began) * 1000:.0f} ms)")
    with _CHOSEN_LOCK:
        _CHOSEN[key] = store
    return store


def forget():
    """The stores chosen, chosen again at the next ask (tests, and a
    hop file changed while a process runs)."""
    with _CHOSEN_LOCK:
        _CHOSEN.clear()


def run_state(hop):
    """The store of a hop whose machines work together (`together`) and
    that keeps no bucket; None for every other hop, which keeps its run
    state on this machine as before."""
    if not together(hop):
        return None
    return store_for(hop)


# ---- the library's account (`tooluse`, backlog 0f) --------------------------
# what of the S3 client the machines' records use, read off the code that
# writes them; only the reasons for the rest are written

_S3 = {"put_object": "put_object", "get_object": "get_object",
       "delete_object": "delete_object",
       "IfMatch": "IfMatch", "IfNoneMatch": "IfNoneMatch", "ETag": "ETag",
       "ObjectLockMode": "ObjectLockMode", "ChecksumAlgorithm":
       "ChecksumAlgorithm", "ServerSideEncryption": "ServerSideEncryption",
       "copy_object": "copy_object", "head_object": "head_object"}


def _account():
    from . import tooluse
    unused = tooluse.UNUSED
    tooluse.register(
        "boto3-s3-conditional", licence="Apache-2.0", release="1.40",
        declared={"s3": frozenset(_S3)},
        reads=(tuple(f"state.py::{fn}" for fn in (
            "read_record", "write_record", "get", "put", "delete",
            "put_blob", "get_blob")),
            {("s3", o): t for o, t in _S3.items()}),
        options={
            ("s3", "ObjectLockMode"): (
                unused, "a record is rewritten over its version many times"
                        " a minute; an object held unchangeable could not"
                        " be, and the evidence's own digests are what a"
                        " locked bucket keeps"),
            ("s3", "ChecksumAlgorithm"): (
                unused, "a record is a few hundred bytes read back whole"
                        " and parsed; a torn one does not parse, and is"
                        " read again"),
            ("s3", "ServerSideEncryption"): (
                unused, "records are sealed before they leave this machine"
                        " where the hop sets MIGKIT_STATE_KEY; the bucket's"
                        " own default encryption still applies"),
            ("s3", "copy_object"): (
                unused, "nothing is moved inside the bucket: a record is"
                        " written where it is kept"),
            ("s3", "head_object"): (
                unused, "a record is read whole with its version in one"
                        " request; asking for the version alone would be a"
                        " second"),
        })


_account()
