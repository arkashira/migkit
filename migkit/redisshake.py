"""A Redis keyspace copied by the replica protocol's own copy (backlog
F7.rshake).

The source hands over one snapshot of the whole keyspace, made the moment
it forked, as it hands one to a replica; where it refuses a replica, the
keyspace is scanned and each key restored; where the hop names the
source's own backup file, or its append-only file up to a moment, that is
read and the server is left alone. Which reader, migkit climbs per
database on the source's own answers (`READERS`).

What the program is told, migkit decides and writes into its
configuration file - readable by this user only, gone when it ends
(`config`): the reader and its database, the hop's exclusions as the
program's own key filters (`key_filters`, only where each one says
exactly what the hop's pattern says), a cluster target's single database
through the program's script, the commands in flight from migkit's
sizing, the target's own limits read from it, the target's serialised
form refused where the target is older or another server (every key then
written as plain commands), and a status port whose counts become
migkit's progress lines. What it does not do, migkit does around it: the
target's keys the hop does not leave out are removed first
(`RedisEngine.empty_target`), the copy is held to the source after
(`cli._held_to_the_source`), and it is chosen against the other ways on
the hop's measured rate (`bulkways`).

What it cannot do is a need its rung lacks: it checks no server's
certificate (TLS without verification is all it has), and it carries no
key's access history, so a hop that checks certificates, or a target that
evicts by how recently or how often a key was read, takes migkit's own
copier.
"""
import json
import os
import re
import secrets
import shutil
import tempfile
import threading
import time

from . import tooluse
from .tooluse import TOPPED, UNUSED, USED

PROGRAM = "redis-shake"

#: the hosts of managed services that refuse a replica's protocol
#: (`PSYNC`/`SYNC` are not among their commands): ElastiCache, MemoryDB,
#: Azure Cache for Redis, Tencent's and Alibaba's
NO_REPLICA = (".cache.amazonaws.com", ".memorydb.", ".redis.cache.windows.net",
              ".redis.azure.net", ".tencentcloudapi.com", ".rds.aliyuncs.com",
              ".redis.rds.aliyuncs.com")

#: keys a SCAN asks for at once: migkit's own copier's batch
SCAN_COUNT = 1000


def _rungs():
    from .decide import Need, Rung
    return (
        Rung("rdb", "the source's backup file",
             gives=frozenset({"consistent as of"}),
             needs=(Need("a backup file the hop names",
                         lambda f: bool(f["rdb_file"]),
                         "the hop names no backup file of the source"),),
             because="the hop names the source's backup file, so the"
                     " server itself is not read"),
        Rung("aof", "the source's append-only file",
             gives=frozenset({"consistent as of"}),
             needs=(Need("an append-only file the hop names",
                         lambda f: bool(f["aof_file"]),
                         "the hop names no append-only file of the"
                         " source"),),
             because="the hop names the source's append-only file, read up"
                     " to the moment it gives"),
        Rung("sync", "the snapshot the source hands a replica",
             gives=frozenset({"consistent as of"}),
             needs=(Need("a source that hands a replica its snapshot",
                         lambda f: f["replica_refused"] is None,
                         lambda f: f["replica_refused"]),),
             because="one snapshot of the whole keyspace, as of the moment"
                     " the source forked"),
        Rung("scan", "a scan of the keyspace, key by key"),
    )


READERS = _rungs()


def files(hop):
    """(backup file, append-only file) the hop names on its source, each
    "" where it names none."""
    opts = hop.source.options or {}
    return (str(opts.get("rdb_file") or "").strip(),
            str(opts.get("aof_file") or "").strip())


def replica_refused(hop):
    """Why the source will not hand a replica its snapshot, or None: a
    managed service that refuses the protocol by its name, or a server
    whose command table lacks it (asked, `COMMAND INFO PSYNC`, which is a
    read)."""
    host = str(hop.source.host or "").lower()
    if host.endswith(NO_REPLICA) or any(h in host for h in NO_REPLICA):
        return ("the source is a managed service that refuses a replica's"
                " protocol")
    from .engines.redis import RedisEngine
    try:
        got = RedisEngine(hop)._client("src").execute_command(
            "COMMAND", "INFO", "PSYNC")
    except Exception as e:  # noqa: BLE001 - said, as the reason
        return (f"the source would not say whether it serves a replica"
                f" ({type(e).__name__})")
    if not got or got == [None]:
        return "the source's commands do not include a replica's"
    return None


def reader_for(hop, db):
    """The `Choice` of how database `db` of the source is read."""
    from .decide import Facts, climb
    rdb, aof = files(hop)
    facts = Facts({"rdb_file": rdb, "aof_file": aof},
                  replica_refused=lambda: replica_refused(hop))
    return climb(f"db{db}", READERS, facts)


# ---- the hop's exclusions as the program's own filters -------------------

def _glob_re(glob):
    """A shell pattern (`fnmatch`'s) as a regular expression the program's
    Go regular expressions read the same way, anchored at both ends."""
    out, i, n = [], 0, len(glob)
    while i < n:
        c = glob[i]
        i += 1
        if c == "*":
            out.append(".*")
        elif c == "?":
            out.append(".")
        elif c == "[":
            j = i
            if j < n and glob[j] == "!":
                j += 1
            if j < n and glob[j] == "]":
                j += 1
            while j < n and glob[j] != "]":
                j += 1
            if j >= n:
                out.append(re.escape(c))
            else:
                body = glob[i:j]
                if body.startswith("!"):
                    body = "^" + body[1:]
                out.append("[" + body.replace("\\", "\\\\") + "]")
                i = j + 1
        else:
            out.append(re.escape(c))
    return "^" + "".join(out) + "$"


def key_filters(hop, db):
    """(regexes, why): the keys of database `db` the hop leaves out, as
    anchored regular expressions over the key alone - or (None, why)
    where one of its patterns cannot be said that way.

    `Hop.excluded(db, key)` matches a pattern against the key and against
    `<db>.<key>`. The second is said over the key alone only where the
    pattern starts with `<db>.` written out (what follows it is then a
    pattern of the key), or starts with a character no `<db>.` can start
    with (it then never matches that way). A pattern that starts with a
    wildcard, or with part of the database's name, could match across the
    dot, and is not guessed at."""
    out = []
    prefix = f"{db}."
    for pat in hop.exclude or ():
        pat = str(pat)
        out.append(_glob_re(pat))
        if pat.startswith(prefix):
            out.append(_glob_re(pat[len(prefix):]))
            continue
        first = pat[:1]
        if first in ("*", "?", "[") or (first and prefix.startswith(first)):
            return None, (f"the hop leaves out {pat!r}, which could match"
                          f" across database {db}'s name, and the fast"
                          " keyspace copy filters keys by their names alone")
    return sorted(set(out)), ""


# ---- what the program is told --------------------------------------------

def _toml(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_toml(v) for v in value) + "]"
    # a TOML basic string takes JSON's escapes
    return json.dumps(str(value))


def toml_text(sections):
    """{section: {key: value}} as the program's configuration file."""
    lines = []
    for name, keys in sections.items():
        lines.append(f"[{name}]")
        lines += [f"{k} = {_toml(v)}" for k, v in keys.items()]
        lines.append("")
    return "\n".join(lines)


def _address(ep):
    return f"{ep.host}:{int(ep.port or 6379)}"


def _side(ep, cluster):
    tls = ep.redis_tls()
    return {"cluster": bool(cluster), "address": _address(ep),
            "username": ep.user or "", "password": ep.password or "",
            "tls": bool(tls)}


def _limits(eng):
    """The target's own `proto-max-bulk-len` and `client-query-buffer-limit`
    in bytes, read from it (a managed target keeps CONFIG to itself: None
    each), and the version of its bloom-filter module where it has one."""
    out = {"bulk": None, "querybuf": None, "bloom": None}
    try:
        dst = eng._client("dst")
    except Exception:  # noqa: BLE001 - nothing read, the defaults stand
        return out
    for key, name in (("bulk", "proto-max-bulk-len"),
                      ("querybuf", "client-query-buffer-limit")):
        try:
            got = dst.config_get(name).get(name)
            out[key] = int(got) if got not in (None, "") else None
        except Exception:  # noqa: BLE001 - a managed server
            pass
    try:
        for m in dst.module_list() or []:
            name = m.get("name") or m.get(b"name") or ""
            name = name.decode() if isinstance(name, bytes) else str(name)
            if name == "bf":
                out["bloom"] = int(m.get("ver") or m.get(b"ver") or 0) \
                    or None
    except Exception:  # noqa: BLE001 - no modules, or not allowed to ask
        pass
    return out


def _diskless(eng):
    """Whether the source streams its snapshot from the fork without
    writing it to its own disk (`repl-diskless-sync yes`), read."""
    try:
        got = eng._client("src").config_get("repl-diskless-sync")
        return str(got.get("repl-diskless-sync", "")).lower() == "yes"
    except Exception:  # noqa: BLE001 - a managed source keeps it
        return False


def config(hop, db, reader, workdir, status_port, workers, refused=False):
    """{section: {key: value}}: the program's whole configuration for
    database `db` read by `reader` (a `READERS` name). `refused`: the
    target refuses the source's serialised values, so every key is written
    as plain commands (`target_redis_proto_max_bulk_len = 0`)."""
    from .engines.redis import RedisEngine
    eng = RedisEngine(hop)
    src_cluster = reader in ("sync", "scan") and eng._cluster("src")
    dst_cluster = eng._cluster("dst")
    out = {}
    if reader == "sync":
        out["sync_reader"] = {**_side(hop.source, src_cluster),
                              "sync_rdb": True, "sync_aof": False,
                              "prefer_replica": False,
                              "try_diskless": _diskless(eng)}
    elif reader == "scan":
        out["scan_reader"] = {**_side(hop.source, src_cluster),
                              "dbs": [int(db)], "ksn": False,
                              "count": SCAN_COUNT,
                              "skip_unknown_type": False}
    elif reader == "rdb":
        out["rdb_reader"] = {"filepath": files(hop)[0]}
    else:
        stamp = (hop.source.options or {}).get("aof_timestamp")
        out["aof_reader"] = {"filepath": files(hop)[1],
                             "timestamp": int(stamp or 0)}
    out["redis_writer"] = {**_side(hop.target, dst_cluster),
                           "off_reply": False}
    regexes, _ = key_filters(hop, db)
    filt = {"block_keys": ["migkit_origin"],
            "block_key_regex": regexes or []}
    if reader != "scan":
        # the snapshot and the files hold every database; the scan reads
        # only the ones it is given
        filt["allow_db"] = [int(db)]
    if dst_cluster and int(db) != 0:
        # a cluster holds database 0 alone and refuses SELECT: every
        # command goes there, as migkit's own copier writes it
        filt["function"] = "shake.call(0, ARGV)\n"
    out["filter"] = filt
    limits = _limits(eng)
    adv = {"dir": str(workdir), "ncpu": max(1, int(workers)),
           "status_port": int(status_port),
           "log_file": "shake.log", "log_level": "info",
           "rdb_restore_command_behavior": "rewrite",
           "pipeline_count_limit": max(64, 256 * max(1, int(workers))),
           "empty_db_before_sync": False}
    if limits["querybuf"]:
        adv["target_redis_client_max_querybuf_len"] = limits["querybuf"]
    if refused:
        adv["target_redis_proto_max_bulk_len"] = 0
    elif limits["bulk"]:
        adv["target_redis_proto_max_bulk_len"] = limits["bulk"]
    out["advanced"] = adv
    if limits["bloom"]:
        out["module"] = {"target_mbbloom_version": limits["bloom"]}
    return out


def payload_refused(hop, db):
    """Why the target refuses a value as the source serialises it, or
    None: one key of `db` read (`RANDOMKEY`, `DUMP`) and restored on the
    target under a name of migkit's own with a time to live of one
    millisecond, so nothing stays. A target older than the source, or
    another server whose serialised form has moved on, refuses it; a
    source with no key has nothing to refuse."""
    import redis

    from .engines.redis import RedisEngine
    eng = RedisEngine(hop)
    try:
        src = eng._client("src", db, decode=False)
        key = src.randomkey()
        payload = src.dump(key) if key is not None else None
    except redis.RedisError:
        return None
    if payload is None:
        return None
    probe = f"migkit:probe:{secrets.token_hex(6)}"
    try:
        eng._keys("dst", db, decode=False).restore(probe, 1, payload,
                                                   replace=True)
    except redis.ResponseError as e:
        return (f"the target refuses the source's serialised values"
                f" ({str(e)[:80]})")
    except redis.RedisError:
        return None
    return None


#: the serialised form's version each server writes and reads, by what the
#: server is and its version (`variants`): the points both publish in
#: their release notes. A version not here is not guessed at - the copy
#: then says it where the target refuses a key
SERIALISED = {"redis": {(7, 0): 10, (7, 2): 11, (7, 4): 12, (8, 0): 12},
              "valkey": {(7, 2): 11, (8, 0): 11, (8, 1): 11, (9, 0): 80}}


def _serialised(brand):
    import re
    parts = tuple(int(x) for x in re.findall(r"\d+", brand.version or "")[:2])
    return SERIALISED.get(brand.name, {}).get(parts)


def older_target(hop):
    """Why the target refuses the source's serialised values, or None: the
    source writes a later version of the form than the target reads (a
    Redis 7.4 source and a Valkey 8 target; any newer Redis into an older
    one). Read from what each server says it is (`INFO`), never written
    to - a dry run asks it too."""
    from .engines.redis import RedisEngine
    try:
        src, dst = RedisEngine(hop)._brands()
    except Exception:  # noqa: BLE001 - not known: not guessed
        return None
    a, b = _serialised(src), _serialised(dst)
    if a is None or b is None:
        return None
    # the two forms are one up to 11 (Redis 7.2, where Valkey began); past
    # it each server's is its own, and the other refuses it
    if (a <= b) if src.name == dst.name else (a <= 11 or a == b):
        return None
    return (f"the source ({src.name} {src.version}) writes a later"
            f" serialised form than the target ({dst.name} {dst.version})"
            " reads")


def evicts(hop):
    """Whether the target evicts keys by how recently or how often they
    were read: a memory ceiling and a policy other than `noeviction`
    (`INFO memory`, a read)."""
    from .engines.redis import RedisEngine
    try:
        mem = RedisEngine(hop)._client("dst").info("memory")
    except Exception:  # noqa: BLE001 - not asked: it may
        return False
    policy = str(mem.get("maxmemory_policy") or "")
    return int(mem.get("maxmemory") or 0) > 0 and policy not in (
        "", "noeviction") and "ttl" not in policy and "random" not in policy


def checks_certificates(hop):
    """Whether either side's TLS checks the server's certificate."""
    return any(ep.redis_tls().get("ssl_cert_reqs") == "required"
               for ep in (hop.source, hop.target))


def needs():
    """What the fast keyspace copy needs of the hop, beyond being installed
    (`bulkways` climbs it with these)."""
    from .decide import Need
    return (Need("exclusions it can say",
                 lambda f: f["shake_filters"] is None,
                 lambda f: f["shake_filters"]),)


def facts(hop):
    """The readers of what `needs` asks, each asked only once and only
    where a need gets to it."""
    def filters():
        for db in [str(d) for d in hop.databases or ["0"]]:
            got, why = key_filters(hop, db)
            if got is None:
                return why
        return None
    return {"shake_filters": filters}


# ---- running it -----------------------------------------------------------

class _Status:
    """The program's status port, read every few seconds while it runs:
    what it has read and written becomes migkit's progress line, and the
    last reading is kept for the proof after."""

    def __init__(self, port, label, total, log):
        self.port, self.label, self.total, self.log = port, label, total, log
        self.last = {}
        self._stop = threading.Event()
        self._began = time.monotonic()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def read(self):
        import urllib.request
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/",
                                        timeout=3) as r:
                return json.loads(r.read() or b"{}")
        except Exception:  # noqa: BLE001 - not up yet, or gone
            return None

    def _run(self):
        from .wording import progress
        while not self._stop.wait(3):
            got = self.read()
            if not got:
                continue
            self.last = got
            counts = got.get("total_entries_count") or {}
            written = int(counts.get("write_count") or 0)
            if written and self.log:
                self.log(progress(self.label, written, self.total,
                                  self._began, time.monotonic(),
                                  unit="keys"))

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(5)
        return False


def move(hop, db, workers, go, log):
    """Database `db` of the source into the target by the fast keyspace
    copy: the reader climbed for it, the target's keys the hop does not
    leave out removed first, the program run with a configuration file of
    migkit's making and its status read as progress. Raises where it
    fails; the caller holds the result to the source."""
    from . import movers
    from .engines.redis import RedisEngine
    from .util import PrivateFile, which
    from .wording import Step, phase
    reader = reader_for(hop, db)
    step = Step(phase("keyspace", workers=workers),
                [PROGRAM, "<private configuration file>"])
    if not go:
        return [step, f"# read by {reader.said}", "# dry-run, add --go to"
                                                  " execute"]
    if reader.rung is None:
        raise SystemExit(f"db{db}: no way to read the source here -"
                         f" {reader.reason}")
    # what the ladder holds it to (`bulkways`), held again where the way
    # was forced: a key the hop leaves out is never copied, a certificate
    # the hop checks is never taken unchecked
    _, why = key_filters(hop, db)
    if why:
        raise SystemExit(f"db{db}: {why}. Nothing has been copied")
    if checks_certificates(hop):
        raise SystemExit(f"db{db}: the hop checks the servers' certificates,"
                         " and this way of copying cannot. Nothing has been"
                         " copied")
    if not which(PROGRAM):
        raise SystemExit("this bulk path needs a program this machine does"
                         " not have - migkit doctor --install puts it in"
                         " place")
    eng = RedisEngine(hop)
    refused = older_target(hop) or payload_refused(hop, db)
    if refused and log:
        log(f"db{db}: {refused}; every key is written as plain commands")
    gone = eng.empty_target(db)
    if gone and log:
        log(f"db{db}: emptied {gone:,} keys the target held before the copy")
    work = tempfile.mkdtemp(prefix="migkit-keyspace-")
    os.chmod(work, 0o700)
    try:
        port = movers._free_port()
        text = toml_text(config(hop, db, reader.path, work, port, workers,
                                bool(refused)))
        total = None if hop.exclude else eng._dbsize("src", db)
        if log:
            log(step)
            log(f"db{db}: read by {reader.said} - {reader.reason}")
        with PrivateFile(text, ".toml") as path, \
                _Status(port, f"db{db}", total, log):
            movers._sh([PROGRAM, path], None, log)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return [step]


# ---- the account of what is used (backlog 0f) ----------------------------

_SIDE = ("cluster", "address", "username", "password", "tls")
#: the documented surface of the pinned release (4.6.2), section by
#: section, as its sample configuration lists it
SURFACE = {
    "sync_reader": frozenset(_SIDE + ("sync_rdb", "sync_aof",
                                      "prefer_replica", "try_diskless")),
    "scan_reader": frozenset(_SIDE + ("dbs", "ksn", "count",
                                      "skip_unknown_type")),
    "rdb_reader": frozenset({"filepath"}),
    "aof_reader": frozenset({"filepath", "timestamp"}),
    "redis_writer": frozenset(_SIDE + ("sentinel", "master", "off_reply")),
    "file_writer": frozenset({"filepath", "type"}),
    "filter": frozenset({
        "allow_keys", "allow_key_prefix", "allow_key_suffix",
        "allow_key_regex", "block_keys", "block_key_prefix",
        "block_key_suffix", "block_key_regex", "allow_db", "block_db",
        "allow_command", "block_command", "allow_command_group",
        "block_command_group", "function"}),
    "advanced": frozenset({
        "dir", "ncpu", "pprof_port", "status_port", "log_file",
        "log_level", "log_interval", "rdb_restore_command_behavior",
        "pipeline_count_limit", "target_redis_client_max_querybuf_len",
        "target_redis_proto_max_bulk_len", "target_redis_max_qps",
        "aws_psync", "empty_db_before_sync"}),
    "module": frozenset({"target_mbbloom_version"}),
}

_ADDRESSED = "the side's address, sign-in and TLS, from the hop's endpoint"
_CLUSTER = ("asked of the server (`INFO cluster`): a cluster is read and"
            " written node by node")


def _account():
    o = {}

    def put(section, option, state, why):
        o[(section, option)] = (state, why)
    for section in ("sync_reader", "scan_reader", "redis_writer"):
        put(section, "cluster", USED, _CLUSTER)
        for f in ("address", "username", "password"):
            put(section, f, USED, _ADDRESSED + "; the file is readable by"
                " this user only and removed when the copy ends")
        put(section, "tls", USED, "on where the hop's side asks for TLS; it"
            " checks no certificate, so a hop that checks certificates takes"
            " migkit's own copier (`checks_certificates`)")
    put("sync_reader", "sync_rdb", USED, "the snapshot is the copy")
    put("sync_reader", "sync_aof", USED, "off: a copy ends at its"
        " snapshot; the changes after it are followed by the server's own"
        " replication or migkit's tail, which resume where this reader"
        " would resync in full after a dropped link")
    put("sync_reader", "prefer_replica", USED, "off: a replica's snapshot"
        " is behind the primary by its lag, which nothing here measures, so"
        " the primary is read")
    put("sync_reader", "try_diskless", USED, "on where the source already"
        " streams its snapshot without writing it to its disk, read from"
        " its own setting (`repl-diskless-sync`)")
    put("scan_reader", "dbs", USED, "the one database this run copies")
    put("scan_reader", "ksn", USED, "off: a copy ends at its scan;"
        " following keyspace notifications would need the source's"
        " notifications on,"
        " a server setting migkit never changes, and cannot see a flush")
    put("scan_reader", "count", USED, "the keys a scan asks for at once,"
        " the batch migkit's own copier reads by")
    put("scan_reader", "skip_unknown_type", USED, "off: a key of a type it"
        " cannot carry stops the copy, never left behind unsaid")
    put("rdb_reader", "filepath", USED, "the source's backup file the hop"
        " names (`rdb_file`)")
    put("aof_reader", "filepath", USED, "the source's append-only file the"
        " hop names (`aof_file`)")
    put("aof_reader", "timestamp", USED, "the moment the hop names to read"
        " it up to (`aof_timestamp`), all of it otherwise")
    put("redis_writer", "sentinel", UNUSED, "the hop addresses the target's"
        " primary itself; a sentinel's name for it is not needed")
    put("redis_writer", "master", UNUSED, "the hop addresses the target's"
        " primary itself; a sentinel's name for it is not needed")
    put("redis_writer", "off_reply", USED, "off: the target's reply is how"
        " a refused write is seen")
    put("file_writer", "filepath", UNUSED, "the copy goes to the target"
        " server, never to a file of values on this machine")
    put("file_writer", "type", UNUSED, "the copy goes to the target server,"
        " never to a file of values on this machine")
    for f in ("allow_keys", "allow_key_prefix", "allow_key_suffix",
              "allow_key_regex", "block_key_prefix", "block_key_suffix"):
        put("filter", f, UNUSED, "the hop says what it leaves out, as"
            " patterns written as regular expressions (`block_key_regex`)")
    put("filter", "block_keys", USED, "migkit's own marker key, never the"
        " application's")
    put("filter", "block_key_regex", USED, "the hop's exclusions, each"
        " written only where it says exactly what the pattern says"
        " (`key_filters`)")
    put("filter", "allow_db", USED, "the one database this run copies,"
        " out of a snapshot or a file that holds every one")
    put("filter", "block_db", UNUSED, "the one database copied is allowed"
        " by name, so none needs blocking")
    for f in ("allow_command", "block_command", "allow_command_group",
              "block_command_group"):
        put("filter", f, UNUSED, "a copy carries every command the"
            " snapshot holds; one left out is a key left behind")
    put("filter", "function", USED, "a cluster target holds database 0"
        " alone: every command is sent there, as migkit's own copier writes"
        " it")
    put("advanced", "dir", USED, "a directory of this user's only, removed"
        " when the copy ends: the snapshot it receives holds values")
    put("advanced", "ncpu", USED, "from migkit's sizing of this machine and"
        " both servers, never set by hand")
    put("advanced", "pprof_port", UNUSED, "a profiler's port is for the"
        " program's own developers")
    put("advanced", "status_port", USED, "read every few seconds as the"
        " copy's progress")
    put("advanced", "log_file", USED, "kept in its private directory for"
        " the words of a failure")
    put("advanced", "log_level", USED, "info: enough for the words of a"
        " failure, without a line a key")
    put("advanced", "log_interval", UNUSED, "progress is read from the"
        " status port, not from the log")
    put("advanced", "rdb_restore_command_behavior", USED, "rewrite: a key"
        " written to the target while the copy runs is replaced by the"
        " source's, as migkit's own copier replaces it")
    put("advanced", "pipeline_count_limit", USED, "sized from the workers"
        " migkit's sizing gives, never left at its default")
    put("advanced", "target_redis_client_max_querybuf_len", USED, "read"
        " from the target's own setting, where it says")
    put("advanced", "target_redis_proto_max_bulk_len", USED, "read from the"
        " target's own setting; zero - every key written as plain commands"
        " - where the target refuses the source's serialised values"
        " (`older_target`, and `payload_refused` proved on one key)")
    put("advanced", "target_redis_max_qps", UNUSED, "migkit's throttle"
        " follows the servers' health, which a fixed rate cannot; the"
        " target's strain is read before the copy")
    put("advanced", "aws_psync", UNUSED, "not yet: a managed source's"
        " replica protocol behind a support ticket; such a source is"
        " scanned")
    put("advanced", "empty_db_before_sync", TOPPED, "migkit removes only"
        " the target's keys the hop does not leave out"
        " (`RedisEngine.empty_target`), where this empties the whole"
        " database")
    put("module", "target_mbbloom_version", USED, "read from the target's"
        " own module list, where it has the bloom filters")
    return o


tooluse.register(PROGRAM, options=_account(), declared=SURFACE)
