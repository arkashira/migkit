"""A seed shipped on media, where the link is too narrow to copy the data
in time (the owner's scope, 2026-10-01: "data too large for the link").

Hundreds of terabytes over a line of a few hundred megabits take months;
a carrier - a disk, or a provider's appliance (`carriers`) - takes days.
So the move is cut in two halves that run where the servers are, and the
rows travel on the carrier between them:

**The export, beside the source.** The position the change stream will
start from is taken first, before a row is read - the engine's own:
PostgreSQL's slot made with its snapshot, MySQL's binary log position
with nothing before it a read cannot see, Oracle's SCN, a change stream's
resume token (`Engine.copy_point`) - and then every table in scope is
read into parts, each written to the carrier. A part is a range of the
table's key, a stretch of it in key order, or a stretch of one pass over
a table with no key (`CUTS`), in the format the target's fastest exact
loader reads (`FORMATS`): the binary copy format where both sides are
PostgreSQL with the same columns - read under one exported snapshot a
table (`pg_pass.Snapshot`) and digested by the source in the same
transaction; the bulk load's lines where both are MySQL - rendered by the
source under one consistent snapshot a range, its digest line first
(`my_pass`); and the rows as the source's reader hands them over for any
other pair, loaded by the target's own writer, which climbs its own
ladder of ways. Each part is digested with the same digest the check
takes (`canon`: count, and the exact sum of every row's md5 over its
canonical text), compressed, and encrypted to the hop's `at_rest`
recipients, so a carrier lost on the way gives nothing away; the files
are hashed as stored. The manifest - every part with its rows, digest
and hash, the position, the tables' columns and keys - is encrypted the
same way and signed like the evidence export (`export._sign`), and the
seal (`sealed.json`: counts only, and the manifest's hash) is written
last, so a carrier with no seal holds no seed.

**The load, beside the target.** The far end finds the sealed seed where
it reads it - the carrier attached there, or the object store the
provider imported it into (`carriers.for_load`) - checks the manifest's
signature, and loads it part by part: each part's hash checked before a
byte of it is read, its rows folded again and held to the manifest's
digest, then written through the target's loader and proved there - a
range by the target's own digest of it, a stretch in key order by every
row read back by its key (`HeteroEngine._write_checked`), a table as a
whole by the target's digest against the sum of its parts' (a digest is
a sum, so a table's is its parts' added up). The change stream then
starts from the manifest's position, and replays by key what changed
while the parts were read and while the carrier travelled - which
converges, as it does after any copy.

**Resumable both ways.** Each half keeps what it finished beside the
hop's reports; run again, it goes on from the next part. A part the
carrier holds short, or that fails its hash or its digest at the far end,
is read again from the source over the link - the part's own range, or
for a table cut in key order the rest of it from the part's first key,
or a table with no key whole - and proved as any part is. A carrier that
fills stops the export where it is; the next carrier goes on from there.

**The source's log held long enough.** The stream can start from the
position only while the source still keeps its log back to it: a slot
holds it until `max_slot_wal_keep_size`, a binary log until it expires,
an oplog until it wraps. So the room is read once the position is taken
(`Engine.stream_room`) and held against what the operator says the
carrier takes to reach the far end (`shipping:`) plus the export and the
load as measured, and said. Where it runs out anyway, the load does not
ship the seed again: it takes a new position, finds the rows that changed
since by comparing both sides leaf by leaf (the check's own drilldown,
`leaves`) and copies only those (`_delta_by_the_leaves`), then the
stream starts from the new position.

**Chosen, not assumed.** The decision engine weighs the copy over the
link against the seed on media (`choose`): the link's measured rate
(`machine.stages_for`, the resource model's stages where they were
measured) gives the network copy's time; the export and the load, at the
source's and the target's measured rates, and the shipping as the hop
states it give the seed's. Where the network copy would miss the hop's
`deadline:`, the plan offers the seed; where the hop names a carrier,
the faster of the two that meets it is taken; the plan states both
times.

Only the rows travel on the carrier. What reads each side's shape, the
positions, the proofs and the stream go over the link, which is narrow,
not absent.

    options:
      at_rest: {recipients: [ssh-ed25519 AAAA... ops@example.com]}
      evidence: {sign: ~/.ssh/seed_signing_key}
      deadline: 14d
      ship:
        carrier: snowball            # disk | snowball | databox |
                                     # transfer_appliance
        endpoint: https://10.0.0.20:8443
        bucket: migkit-seed
        ca_bundle: /etc/ssl/snowball.pem
        shipping: 7d                 # sealed here to loaded there
        load_from: s3://migkit-seed/  # where the provider imports it
        signers: ~/.ssh/allowed_signers
"""
import contextlib
import hashlib
import io
import json
import math
import os
import pickle
import shutil
import threading
import time
import zoneinfo
from dataclasses import dataclass

from . import canon, carriers, decide

FORMAT = 1
#: bytes of the source's rows a part holds, as the source's catalogue
#: counts them: set, not measured - large enough that a seed of hundreds
#: of terabytes is a few hundred thousand files (every appliance holds
#: that many), small enough that a part read again over the link is
#: minutes, not hours
PART_BYTES = 2 * 2 ** 30
#: rows a part holds where the catalogue says nothing of bytes
PART_ROWS = 2_000_000
#: the parts' compression level: the fast end, as the rows are read
#: faster than a higher level compresses them
ZSTD_LEVEL = 3
#: seconds the source's log is watched to see how fast it grows, where
#: it says only how many bytes it can still keep
ROOM_SAMPLE = 10.0
#: the namespace the manifest is signed in: no evidence signature is a
#: manifest's
NAMESPACE = "migkit-seed"

SEALED = "sealed.json"
MANIFEST = "manifest.json.age"
SIGNATURE = MANIFEST + ".sig"
EXPORT_STATE = "seed-export.json"
LOAD_STATE = "seed-load.json"
SPOOL = "seed-spool"

#: a part's rows, as batches the unpickler reads back: the classes a
#: value of a source's reader may be, and nothing else (`_allowed`)
_ALLOWED = {
    ("builtins", n) for n in ("set", "frozenset", "complex", "bytearray",
                              "bytes")
} | {("decimal", "Decimal"), ("uuid", "UUID"), ("zoneinfo", "ZoneInfo"),
     ("collections", "OrderedDict"), ("re", "RegexFlag"),
     ("migkit.canon", "_Absent")} | {
    ("datetime", n) for n in ("date", "datetime", "time", "timedelta",
                              "timezone")} | {
    ("ipaddress", n) for n in ("IPv4Address", "IPv6Address", "IPv4Network",
                               "IPv6Network", "IPv4Interface",
                               "IPv6Interface")}
#: modules whose classes are values a driver hands over: any class of
#: theirs is taken, never a function
_VALUE_MODULES = ("bson", "cassandra.util", "psycopg2._range",
                  "psycopg.types.range", "migkit.intervals", "migkit.geo")


def settings(hop):
    """The hop's `ship:`, or None where it names no carrier."""
    got = (hop.options or {}).get("ship")
    return got if isinstance(got, dict) and got else None


# ---- the values a part holds ------------------------------------------------

def _allowed(module, name):
    """Whether a part may hold a value of the class `module.name`: one of
    `_ALLOWED`, or a class of a value module, never a function."""
    if (module, name) in _ALLOWED:
        return True
    if not str(module).startswith(_VALUE_MODULES) or name.startswith("_") \
            or "." in name:
        return False
    import importlib
    try:
        obj = getattr(importlib.import_module(module), name)
    except (ImportError, AttributeError):
        return False
    return isinstance(obj, type)


class Unfit(SystemExit):
    """A value no part may hold."""


class _Pickler(pickle.Pickler):
    """The batches of a part, each value checked against what the far end
    will take: a class it would refuse stops the export here, named. A
    time zone goes by its name, so the far end builds it by name; a view
    of bytes (a driver's `bytea`) as the bytes it shows."""

    def reducer_override(self, obj):
        kind = obj if isinstance(obj, type) else type(obj)
        if kind is zoneinfo.ZoneInfo and not isinstance(obj, type) \
                and obj.key:
            return zoneinfo.ZoneInfo, (obj.key,)
        if kind is memoryview:
            return bytes, (obj.tobytes(),)
        if not _allowed(kind.__module__, kind.__qualname__):
            raise Unfit(f"a value of the class {kind.__module__}."
                        f"{kind.__qualname__} is not one a part may hold")
        return NotImplemented


class _Unpickler(pickle.Unpickler):
    """A part's batches read back, building only the classes a part may
    hold (`_allowed`): a part is not a program."""

    def find_class(self, module, name):
        if not _allowed(module, name):
            raise pickle.UnpicklingError(
                f"{module}.{name} is not a class a part may hold")
        return super().find_class(module, name)


def _dumps(value):
    buf = io.BytesIO()
    _Pickler(buf, protocol=5).dump(value)
    return buf.getvalue()


def _loads(data):
    return _Unpickler(io.BytesIO(data)).load()


def _batches_of(reader):
    """Every batch of rows a part's plain stream holds."""
    while True:
        try:
            yield _Unpickler(reader).load()
        except EOFError:
            return


def key_text(value):
    """A key as the manifest keeps it: exactly, whatever its type."""
    import base64
    return None if value is None else base64.b64encode(
        _dumps(value)).decode()


def key_of(text):
    import base64
    return None if text is None else _loads(base64.b64decode(text))


# ---- a part as a file -------------------------------------------------------

class _Pull:
    """Bytes read in pieces from an iterator of pieces."""

    def __init__(self, pieces):
        self._it, self._buf, self.bytes = iter(pieces), bytearray(), 0

    def read(self, n=-1):
        while n is None or n < 0 or len(self._buf) < n:
            nxt = next(self._it, None)
            if nxt is None:
                break
            self._buf += nxt
        if n is None or n < 0 or n >= len(self._buf):
            out = bytes(self._buf)
            self._buf.clear()
        else:
            out = bytes(self._buf[:n])
            del self._buf[:n]
        self.bytes += len(out)
        return out


class _Hashing:
    """A file written through, its bytes counted and hashed."""

    def __init__(self, f):
        self.f, self.sha, self.bytes = f, hashlib.sha256(), 0

    def write(self, data):
        self.f.write(data)
        self.sha.update(data)
        self.bytes += len(data)
        return len(data)

    def flush(self):
        self.f.flush()


def seal_part(pieces, path, recipients):
    """The part at `path`, from an iterator of its plain bytes: compressed
    (its own checksum in the frame), then encrypted to `recipients`, as it
    is read - no plain byte of it touches the disk. Returns (the stored
    file's sha256, its bytes, the plain bytes)."""
    import pyrage
    import zstandard

    from . import evidence
    src = _Pull(pieces)
    cctx = zstandard.ZstdCompressor(level=ZSTD_LEVEL, write_checksum=True)
    with open(path, "wb") as f:
        out = _Hashing(f)
        pyrage.encrypt_io(cctx.stream_reader(src), out,
                          [evidence._recipient(r) for r in recipients])
    return out.sha.hexdigest(), out.bytes, src.bytes


def sha256_of(path):
    """(sha256, bytes) of a file as it is kept."""
    h, n = hashlib.sha256(), 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
            n += len(chunk)
    return h.hexdigest(), n


class BadPart(RuntimeError):
    """A part whose bytes are not the ones the manifest names: read again
    from the source."""


class Different(RuntimeError):
    """A part the target did not take as it was: nothing a second reading
    of the source mends, so the load stops on it."""


@contextlib.contextmanager
def opened(path):
    """A part's plain bytes as a reader: decrypted on a thread of its own
    into a pipe with this machine's identity (`evidence._identity`),
    decompressed as they are read."""
    import pyrage
    import zstandard

    from . import evidence
    rfd, wfd = os.pipe()
    failed = []

    def run():
        try:
            with open(path, "rb") as f, os.fdopen(wfd, "wb") as w:
                pyrage.decrypt_io(f, w, [evidence._identity()])
        except BrokenPipeError:
            pass
        except BaseException as e:  # noqa: BLE001 - raised by the reader
            failed.append(e)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    raw = os.fdopen(rfd, "rb")
    reader = io.BufferedReader(
        zstandard.ZstdDecompressor().stream_reader(raw), 1 << 20)

    def ended():
        raw.close()
        thread.join()
        return failed[0] if failed else None
    try:
        yield reader
    except (zstandard.ZstdError, pickle.UnpicklingError, EOFError,
            ValueError) as e:
        # a read cut short: by the decryption that failed, where it did
        why = ended()
        if why is None or isinstance(why, SystemExit):
            raise why or e
        raise BadPart(f"it did not decrypt: {_first(why)}") from why
    except BaseException:
        # the body's own failure - the target's, a verdict - is its own;
        # the decryption stopped because the reading did
        ended()
        raise
    why = ended()
    if isinstance(why, SystemExit):
        raise why
    if why is not None:
        raise BadPart(f"it did not decrypt: {_first(why)}") from why


def _first(e):
    text = str(e).strip() or type(e).__name__
    return text.splitlines()[-1][:200]


# ---- how a table is cut, and the format its parts are in --------------------

def _need(what, holds, why=None):
    return decide.Need(what, holds, why)


CUTS = (
    decide.Rung("whole", "one part, the whole table",
                needs=(_need("a table no larger than a part",
                             lambda f: f["rows"] is not None
                             and f["rows"] <= f["per"],
                             "the table is larger than a part"),),
                because="the table fits in one part"),
    decide.Rung("ranges", "ranges of its integer key, each read, digested"
                          " and loaded on its own",
                needs=(_need("one integer key",
                             lambda f: f["integer key"],
                             "the table's key is not one integer column"),
                       _need("a source that answers SQL",
                             lambda f: f["sql"],
                             "the source takes no SQL to cut the ranges")),
                because="each range is addressed on both sides, read again"
                        " alone and proved alone"),
    decide.Rung("key order", "parts in the order of its key, each going on"
                             " from the last key of the one before",
                needs=(_need("a key the source reads it on by",
                             lambda f: f["resumes"],
                             "the table has no key the source reads on by"),)),
    decide.Rung("one pass", "one pass over it, cut into parts, loaded again"
                            " whole where it stops"),
)


def _same(f, engine):
    return f["src engine"] == engine and f["dst engine"] == engine


FORMATS = (
    decide.Rung("binary copy", "the PostgreSQL binary copy format, read"
                               " under one snapshot and loaded by the"
                               " target's own copy",
                needs=(_need("PostgreSQL on both sides",
                             lambda f: _same(f, "postgres"),
                             "{rung} is PostgreSQL's own, on both sides"),
                       _need("one part or ranges of its key",
                             lambda f: f["cut"] in ("whole", "ranges"),
                             "{rung} is read by ranges of a key or whole"),
                       _need("every column digested by the server",
                             lambda f: not f["folded here"],
                             "a column is digested only in this process,"
                             " and {rung} passes no row through it"),
                       _need("the same columns of the same types with a"
                             " binary form on both sides",
                             lambda f: f["binary"],
                             "{rung} needs the same columns of the same types"
                             " with a binary form on both sides")),
                because="no row is decoded here, and both servers digest"
                        " every part"),
    decide.Rung("load lines", "the MySQL bulk load's lines, rendered by the"
                              " source under one snapshot a part and loaded"
                              " by the target's own bulk load",
                needs=(_need("MySQL on both sides",
                             lambda f: _same(f, "mysql"),
                             "{rung} is MySQL's own, on both sides"),
                       _need("one part or ranges of its key",
                             lambda f: f["cut"] in ("whole", "ranges"),
                             "{rung} is read by ranges of a key or whole"),
                       _need("the source's own client installed",
                             lambda f: f["client"],
                             "the source's own client is not installed -"
                             " migkit doctor --install"),
                       _need("a target that takes a bulk load",
                             lambda f: f["loads"],
                             "{rung} needs the target to take a bulk load"
                             " (local_infile on)"),
                       _need("columns the load reads back as they were",
                             lambda f: not f["unfit"],
                             lambda f: f["unfit"])),
                because="no row is decoded here, and both servers digest"
                        " every part"),
    decide.Rung("rows", "the rows as the source's reader hands them over,"
                        " loaded by the target's own writer"),
)


# ---- the decision: over the link, or on media -------------------------------

def _took(secs):
    from .machine import _took as took
    return took(secs)


@dataclass
class Times:
    """What each way would take, in seconds; None where nothing measured
    says."""
    size: int = 0
    network: float = None
    link_rate: float = None
    export: float = None
    shipping: float = None
    load: float = None
    deadline: float = None
    measured: str = ""

    @property
    def media(self):
        if self.shipping is None:
            return None
        return (self.export or 0) + self.shipping + (self.load or 0)

    @property
    def media_complete(self):
        return None not in (self.export, self.shipping, self.load)


def times(hop, eng, dbs, via):
    """The `Times` of this move: its bytes (the tables it carries, as the
    source's catalogue counts them), the link's measured rate, the source's
    reads and the target's writes as measured on this hop, the shipping as
    the hop states it, and the deadline."""
    from . import legs, machine
    size = sum(machine.size_of(eng, d, hop) for d in dbs)
    got = Times(size=size, deadline=machine.deadline(hop))
    s = settings(hop) or {}
    got.shipping = machine.duration(s.get("shipping"))
    stages = None
    for d in dbs:
        try:
            stages = machine.stages_for(hop, via, legs.direct(via), d)
        except Exception:  # noqa: BLE001 - nothing measured, then
            stages = None
        if stages is not None:
            break
    if stages is None or not size:
        return got
    servers = stages.servers()
    rate = min(servers.values()) if servers else stages.at_least
    if rate:
        got.link_rate = rate
        got.network = size / rate
    here = stages.per_core * (_cpus() or 1) if stages.per_core else None

    def at(rate_):
        rates = [r for r in (rate_, here) if r]
        return size / min(rates) if rates else None
    # the export reads the source here and the load writes the target
    # there: neither crosses the link
    got.export = at(stages.source or stages.beyond or stages.at_least)
    got.load = at(stages.target or stages.beyond or stages.at_least)
    got.measured = stages.measured
    return got


def _cpus():
    from . import sizing
    return sizing.host().get("cpus")


def _meets(secs, f):
    return f["deadline"] is None or secs is None or secs <= f["deadline"]


SEEDS = {
    "network": decide.Rung(
        "network", "the copy over the link",
        needs=(_need("the copy over the link within the deadline",
                     lambda f: _meets(f["network"], f),
                     lambda f: f"it takes about {_took(f['network'])}, past"
                               f" the deadline of {_took(f['deadline'])}"),),
        because="the link carries it within what the hop asks"),
    "media": decide.Rung(
        "media", "a seed shipped on media",
        needs=(_need("a carrier the hop names",
                     lambda f: f["ship"],
                     "the hop names no carrier (options ship:)"),
               _need("the seed within the deadline",
                     lambda f: _meets(f["media"], f),
                     lambda f: f"it takes about {_took(f['media'])}, past"
                               f" the deadline of {_took(f['deadline'])}")),
        footprint=1,
        because="the hop names a carrier"),
}


def choose(hop, t):
    """The `decide.Choice` between the copy over the link and the seed on
    media for times `t`: each held to the deadline, the one measured faster
    first, and where nothing is measured the carrier the hop names before
    the link."""
    ship = settings(hop) is not None
    rungs = ((SEEDS["media"], SEEDS["network"]) if ship
             else (SEEDS["network"], SEEDS["media"]))
    facts = decide.Facts({"ship": ship, "network": t.network,
                          "media": t.media, "deadline": t.deadline})
    # the seed's time is a floor where the export or the load is not
    # measured: it ranks as a time only where even the floor says it
    media = t.media if t.media is not None and (
        t.media_complete or (t.network is not None
                             and t.media >= t.network)) else None
    costs = {k: v for k, v in (("network", t.network), ("media", media))
             if v is not None}
    return decide.climb("the seed", rungs, facts, costs=costs)


def said(t, choice=None, ship=True):
    """Both times, and the way taken, in a line."""
    from .machine import _mb
    from .wording import human_bytes
    out = []
    if t.network is not None:
        out.append(f"the copy over the link about {_took(t.network)} for"
                   f" {human_bytes(t.size)} at ~{_mb(t.link_rate)} MB/s")
    else:
        out.append("the copy over the link: nothing is measured on this"
                   " path yet to time it")
    parts = []
    if t.export is not None:
        parts.append(f"the export here about {_took(t.export)}")
    if t.shipping is not None:
        parts.append(f"the shipping {_took(t.shipping)} as the hop states"
                     " it")
    if t.load is not None:
        parts.append(f"the load there about {_took(t.load)}")
    if t.media is not None:
        out.append(f"a seed shipped on media about {_took(t.media)}"
                   + ("" if t.media_complete else " at least")
                   + f" ({', '.join(parts)})")
    elif ship:
        out.append("a seed shipped on media: the hop does not say how long"
                   " the shipping takes (ship: shipping:)")
    line = "; ".join(out)
    if t.deadline is not None:
        line += f"; the deadline {_took(t.deadline)}"
    if choice is not None and choice.rung is not None:
        line += f" - {choice.rung.said}: {choice.reason}"
        # a way the list put below the one taken is no reason for it, and
        # still why that way was not taken
        passed = [f"{r.said}: {why}" for r, why in choice.passed
                  if why not in choice.reason]
        if passed:
            line += "; not " + "; not ".join(passed)
    elif choice is not None:
        line += f" - neither meets it: {choice.reason}"
    if t.measured:
        line += f" (going by {t.measured})"
    return line


def plan_line(hop, eng, db, via):
    """The plan's line about the seed on media, or "": said where the hop
    names a carrier or gives a deadline - and where the copy over the
    link would miss that deadline, the seed is offered with what it would
    have to take."""
    from . import machine
    s = settings(hop)
    if s is None and machine.deadline(hop) is None:
        # nothing to weigh: the plan says what it always said
        return ""
    try:
        t = times(hop, eng, [db], via)
    except SystemExit:
        raise
    except Exception:  # noqa: BLE001 - a plan line, not a reason to stop
        return ""
    if s is None and (t.deadline is None or t.network is None
                      or t.network <= t.deadline):
        return ""
    got = choose(hop, t)
    line = said(t, got, s is not None)
    if s is None:
        spare = t.deadline - (t.export or 0) - (t.load or 0)
        line += ("; a seed shipped on media would meet it where the carrier"
                 " reaches the far end within "
                 f"{_took(max(spare, 0))}"
                 + (" of the export and the load measured here" if
                    t.export is not None and t.load is not None else "")
                 + " - options ship: {carrier: disk | snowball | databox |"
                   " transfer_appliance, shipping: ...}")
    return line


# ---- the source's log, held long enough -------------------------------------

def room(src, db, token, needed, sample=None, sleep=time.sleep):
    """(True | False | None, words): whether the source keeps its log back
    to `token` for `needed` seconds more - True, False, or None where it
    does not say - and what that means. Where it says only the bytes it
    can still keep, how fast its log grows is watched for `sample`
    seconds (`ROOM_SAMPLE`)."""
    sample = ROOM_SAMPLE if sample is None else sample
    try:
        got = src.stream_room("src", db, token)
    except Exception as e:  # noqa: BLE001 - said, never a reason to stop
        return None, ("how long the source keeps its log could not be read"
                      f" ({_first(e)})")
    if not got:
        return None, ("the source does not say how long it keeps its log"
                      " back to the seed's position")
    need = (f"the seed needs about {_took(needed)} from its position to the"
            " far end's first change" if needed else
            "the hop does not say how long the carrier takes to reach the"
            " far end (ship: shipping:), so nothing is held against it")
    if got.get("seconds") is not None:
        secs = float(got["seconds"])
        ok = None if needed is None else secs >= needed
        return ok, (f"the source keeps its log back to the position for"
                    f" about {_took(secs)}, and {need}")
    held = got.get("held_bytes")
    grows = None
    if held is not None and sample:
        sleep(sample)
        try:
            later = src.stream_room("src", db, token) or {}
        except Exception:  # noqa: BLE001 - one reading is what there is
            later = {}
        if later.get("held_bytes") is not None:
            grows = max(0.0, (float(later["held_bytes"]) - float(held))
                        / sample)
    if got.get("bytes") is not None:
        left = float(got["bytes"])
        if not grows:
            return None, (f"the source can still keep {_human(left)} of log"
                          " past the position, and wrote none in the"
                          f" {sample:.0f} seconds watched; {need}")
        secs = left / grows
        ok = None if needed is None else secs >= needed
        return ok, (f"the source can still keep {_human(left)} of log past"
                    f" the position, about {_took(secs)} at the"
                    f" {_human(grows)} a second it wrote while watched, and"
                    f" {need}")
    return None, ("nothing caps the log the source keeps for the position;"
                  f" it holds {_human(held or 0)} now"
                  + (f" and grows {_human(grows)} a second, which its disk"
                     " must hold" if grows else "")
                  + f" - {need}")


def _human(n):
    from .wording import human_bytes
    return human_bytes(n)


RUNS_OUT = ("if it runs out, the stream cannot start from the seed's"
            " position: the far end takes a new position, finds the rows"
            " that changed since by comparing both sides leaf by leaf, and"
            " copies only those - the seed is not shipped again")


def lost(src, db, token):
    """Why the source no longer keeps its log back to `token`, or None."""
    try:
        why = src.position_lost("src", db, token)
    except Exception:  # noqa: BLE001 - asked below another way
        why = None
    if why:
        return str(why)
    try:
        got = src.stream_room("src", db, token) or {}
    except Exception:  # noqa: BLE001 - it does not say
        return None
    if got.get("seconds") == 0 or got.get("bytes") == 0:
        return "the source no longer keeps its log back to the position"
    return None


# ---- one database's work ----------------------------------------------------

def _pair(eng):
    from .engines.hetero import HeteroEngine
    return eng if isinstance(eng, HeteroEngine) else eng._as_pair()


def _tailer(eng):
    """What follows the source after the seed, as `move --mode cdc` has
    it."""
    if hasattr(eng, "tail_apply"):
        return eng
    from .cli import _tail_pair
    return _tail_pair(eng)


def _native(eng, name):
    """The hop's own engine where it is `name` on both sides: the native
    formats are its own."""
    from .engines.hetero import HeteroEngine
    if isinstance(eng, HeteroEngine) or eng.CANON_ENGINE != name:
        return None
    return eng


def _stamp():
    from .util import stamp
    return stamp()


class _Work:
    """One database of the hop, as a half of the seed runs it."""

    def __init__(self, hop, eng, db, mode, log):
        self.hop, self.eng, self.db, self.mode = hop, eng, db, mode
        self.log = log
        self.pair = _pair(eng)
        self.src, self.dst = self.pair.src_engine, self.pair.dst_engine
        self.lock = threading.Lock()
        from . import durable
        self.dir = hop.report_dir(db)
        # parts on their way to or from a carrier: this user's only
        self.spool = durable.private_dir(self.dir / SPOOL)
        self._facts = None

    def prefix(self):
        return f"{self.hop.name}/{self.db}/"

    def facts(self):
        if self._facts is None:
            try:
                self._facts = self.src.table_facts("src", self.db) or {}
            except Exception:  # noqa: BLE001 - parts sized by rows then
                self._facts = {}
        return self._facts

    def record(self, entry):
        from . import audit
        audit.append(self.hop.report_dir() / "changelog.jsonl",
                     {"db": self.db, **entry})

    def workers(self, units):
        from . import sizing
        try:
            n = sizing.estimate(self.hop, self.eng, self.db,
                                units=max(1, units)).start
        except Exception:  # noqa: BLE001 - one at a time, then
            n = 1
        return max(1, int(n))


def _tables(w):
    """The source's tables the move carries, as the table copier lists
    them."""
    out = []
    for sch, tbl in w.pair.list_move_tables(w.db):
        t = f"{sch}.{tbl}" if sch else tbl
        if not w.hop.excluded(w.db, *str(t).split(".")):
            out.append(t)
    return out


def _columns(w, src_t):
    """(the columns a part holds, as the copier reads them; the columns
    its digest is taken over, as the check compares them) - from the
    source alone, so the export needs nothing of the target."""
    src = w.src
    declared = dict(src.neutral_columns("src", w.db, src_t))
    types, back = w.pair._mapped_types(w.db, src_t, declared)
    read = [[back.get(n, n), canon.type_class(src.CANON_ENGINE, types[n])]
            for n in sorted(types)]
    canonical = {n: src.canonical_type("src", w.db, t)
                 for n, t in declared.items()}
    ctypes, cback = w.pair._mapped_types(w.db, src_t, canonical)
    got = w.pair._classify_columns(src, ctypes, src, ctypes)
    digest = [[cback.get(n, n), s] for n, s, _ in got["pairs"]]
    return read, digest


def _span(eng, name, after, upto, extra=None):
    """A range of an integer key, in `eng`'s SQL."""
    q = eng._quote_ident(name)
    got = f"{q} > {int(after)} and {q} <= {int(upto)}"
    return f"({extra}) and {got}" if extra else got


def _parts_of(table):
    """(schema, table) as the table copier lists a table."""
    sch, _, tbl = str(table).rpartition(".")
    return sch, tbl


def _tid(table):
    return hashlib.sha1(str(table).encode(),
                        usedforsecurity=False).hexdigest()[:16]


def _part_name(w, entry, n):
    return f"{w.prefix()}parts/{entry['id']}/{int(n):06d}.part"


# ---- the export -------------------------------------------------------------

def _plan_table(w, src_t):
    """How one table is exported: its columns, its key, how it is cut,
    the format of its parts, and for ranges the ranges."""
    from . import ranges
    src, db = w.src, w.db
    read, digest_cols = _columns(w, src_t)
    f = w.facts().get(src_t) or {}
    rows = int(f["rows"]) if f.get("rows") is not None else None
    nbytes = int(f.get("bytes") or 0)
    per = (max(1, PART_BYTES // max(nbytes // rows, 1)) if rows and nbytes
           else PART_ROWS)
    src_where, _ = w.pair._row_scope(db, src_t)
    key = list(src.neutral_key("src", db, src_t) or [])
    names = [n for n, _ in read]
    classes = dict((n, c) for n, c in read)
    resumes = bool(key) and src.RESUMES_BY_KEY and set(key) <= set(names)
    cut = decide.climb(f"how {src_t} is cut", CUTS, decide.Facts({
        "rows": rows, "per": per, "sql": bool(src.SQL_DIALECT),
        "resumes": resumes,
        "integer key": resumes and len(key) == 1
        and classes.get(key[0]) == "integer"}))
    entry = {"table": src_t, "id": _tid(src_t), "cut": cut.path,
             "key": key if resumes else [], "columns": read,
             "digest_columns": digest_cols, "where": src_where,
             "per": per, "rows_est": rows, "parts": {}, "done": False,
             "because": {"cut": cut.reason}}
    if cut.path == "ranges":
        k = key[0]
        q = src._quote_ident(k)
        table = src._qualified("src", db, src_t)
        got = src.run_rule("src", db, f"select min({q}), max({q}) from"
                           f" {table}" + (f" where ({src_where})"
                                          if src_where else ""))
        if not got or got[0][0] is None:
            entry["cut"] = "whole"
            entry["because"]["cut"] = "the table holds no rows"
        else:
            lo, hi = int(got[0][0]), int(got[0][1])
            edges = [r[0] for r in src.run_rule("src", db, ranges.bounds_sql(
                src._quote_ident, table, k, per, src_where))]
            entry["ranges"] = [list(r) for r in ranges.split(lo, hi, edges)]
    fmt = decide.climb(f"the format of {src_t}'s parts", FORMATS,
                       _format_facts(w, src_t, entry))
    entry["format"] = fmt.path
    entry["because"]["format"] = fmt.reason
    if fmt.path == "binary copy":
        sch, tbl = w.eng._split(src_t)
        entry["copy_columns"] = w.eng._copy_cols(db, sch, tbl)
    elif fmt.path == "load lines":
        got = w.eng._pass_columns(db, w.pair._leaf(src_t))
        entry["load"] = {"names": [n for n, _ in got],
                         "kinds": [k for _, k in got]}
    return entry


def _format_facts(w, src_t, entry):
    from .util import which
    pg, my = _native(w.eng, "postgres"), _native(w.eng, "mysql")
    db = w.db
    leaf = w.pair._leaf(src_t)

    def asked(fn, default):
        def ask():
            try:
                return fn()
            except Exception as e:  # noqa: BLE001 - a need that cannot be
                # read does not hold
                return default(e) if callable(default) else default
        return ask
    return decide.Facts(
        {"src engine": w.src.CANON_ENGINE, "dst engine": w.dst.CANON_ENGINE,
         "cut": entry["cut"],
         "folded here": any(canon.folded_here(c)
                            for _, c in entry["digest_columns"])},
        binary=asked(lambda: pg is not None
                     and bool(pg._copy_cols(db, *pg._split(src_t)))
                     and pg.passes_through(db, src_t), False),
        client=lambda: bool(which("mysql")),
        loads=asked(lambda: my is not None and my._loads_here(), False),
        unfit=asked(lambda: my._pass_unfit(db, leaf) if my is not None
                    else "not MySQL",
                    lambda e: f"the columns could not be read: {_first(e)}"))


class _State:
    """A half's own record of what it finished, kept beside the hop's
    reports and written whole or not at all (`durable`): a torn one
    stops, naming the file, since a part read as done that is not would
    be a part never shipped or never loaded."""

    def __init__(self, path):
        from . import durable
        self.path = path
        got = durable.read_state(path, "state")
        self.data = got if isinstance(got, dict) else {}
        self.lock = threading.Lock()

    def save(self):
        from . import durable
        with self.lock:
            durable.write_json(self.path, self.data, indent=1)


class Full(SystemExit):
    """The carrier holds no more: the next one goes on from here."""


def _batches(w, entry, after=None, where=None):
    """(rows, the last key) of a table's rows in the parts' own order: by
    its key from `after` where it has one the source reads on by, else
    one pass."""
    src, db, t = w.src, w.db, entry["table"]
    cols = [tuple(c) for c in entry["columns"]]
    scope = where if where is not None else entry.get("where")
    # a part is cut where a read ends, so no read is larger than a part
    chunk = min(w.pair._read_rows(db, t, 10 ** 9), int(entry["per"]))
    if entry["key"]:
        point = after
        while True:
            rows, last = src.neutral_read("src", db, t, cols, point, chunk,
                                          **({"where": scope} if scope
                                             else {}))
            if not rows:
                return
            yield rows, last
            if last is None:
                return
            point = last
    else:
        for rows in src.neutral_batches("src", db, t, cols, chunk,
                                        **({"where": scope} if scope
                                           else {})):
            yield rows, None


class _Feed:
    """A table's batches, handed out a part at a time."""

    def __init__(self, gen):
        self.gen, self.head, self.done = gen, None, False

    def peek(self):
        if self.head is None and not self.done:
            self.head = next(self.gen, None)
            self.done = self.head is None
        return self.head

    def take(self, per, seen):
        got = 0
        while got < per and self.peek() is not None:
            rows, last = self.head
            self.head = None
            got += len(rows)
            seen["last"] = last
            yield rows


def _rows_pieces(batches, at, classes, stats):
    """The plain bytes of a part of rows: each batch pickled as it comes,
    and folded into the part's digest over the digest's columns."""
    total, n = 0, 0
    for rows in batches:
        _, total = canon.fold_rows(classes, ([r[i] for i in at]
                                             for r in rows), total)
        n += len(rows)
        yield _dumps(rows)
    stats["rows"], stats["digest"] = n, str(total)


def _digest_at(entry):
    names = [n for n, _ in entry["columns"]]
    return ([names.index(n) for n, _ in entry["digest_columns"]],
            [c for _, c in entry["digest_columns"]])


class _Exporter:
    """The export half of one database: its state, its carrier, its
    recipients, and each part written."""

    def __init__(self, w, carrier, recipients):
        self.w, self.carrier, self.recipients = w, carrier, recipients
        self.state = _State(w.dir / EXPORT_STATE)
        self.snaps = {}

    # -- one part --------------------------------------------------------

    def _room(self, stored):
        """The carrier takes `stored` more bytes, or `Full`."""
        c, lim = self.carrier, self.carrier.limits
        if lim.file_bytes is not None and stored > lim.file_bytes:
            raise SystemExit(
                f"a part of {_human(stored)} is past the largest file"
                f" {c.said} takes ({_human(lim.file_bytes)}; {lim.whence})")
        with self.state.lock:
            used = self.state.data.get("carriers", {}).get(c.said, 0)
            count = self.state.data.get("carriers_files", {}).get(c.said, 0)
        # what it holds in all against what is on it, and what is free on
        # it now against this part
        now = c.store.limits()
        if (lim.capacity is not None and used + stored > lim.capacity) or (
                now.capacity is not None and stored > now.capacity) or (
                now.files is not None and now.files < 1):
            raise Full(
                f"{c.said} is full: {_human(used)} written to it, and the"
                f" next part is {_human(stored)}. Put another carrier in its"
                " place (ship: path or endpoint) and run the same command:"
                " the export goes on onto it. At the far end every"
                " carrier's parts are read from one place - where the"
                " provider imports them all (load_from), or copied under"
                " one directory")
        if lim.files is not None and count + 1 > lim.files:
            raise Full(f"{c.said} holds at most {lim.files:,} files;"
                       " the next part is past that")

    def _put(self, entry, n, pieces_of, extra, late=None):
        """One part: its plain bytes from `pieces_of(stats)`, sealed to the
        recipients in the spool, put on the carrier, its size read back;
        `late()` what is known of it only once it is read (its last key),
        kept with it in the same write."""
        w = self.w
        name = _part_name(w, entry, n)
        tmp = w.spool / f"{entry['id']}-{n}.part"
        stats = {}
        began = time.monotonic()
        try:
            sha, stored, plain = seal_part(pieces_of(stats), tmp,
                                           self.recipients)
            self._room(stored)
            self.carrier.store.put(name, tmp)
            there = self.carrier.store.size(name)
            if there != stored:
                raise SystemExit(f"{self.carrier.said} holds {there} bytes"
                                 f" of {name}, where {stored} were written")
        finally:
            tmp.unlink(missing_ok=True)
        part = {"n": n, "name": name, "rows": stats["rows"],
                "digest": stats["digest"], "sha256": sha, "bytes": stored,
                "plain": plain, "carrier": self.carrier.said,
                "seconds": round(time.monotonic() - began, 3),
                "at": _stamp(), **extra,
                **(late() if late else {})}
        with self.state.lock:
            cs = self.state.data.setdefault("carriers", {})
            cs[self.carrier.said] = cs.get(self.carrier.said, 0) + stored
            cf = self.state.data.setdefault("carriers_files", {})
            cf[self.carrier.said] = cf.get(self.carrier.said, 0) + 1
            entry["parts"][str(n)] = part
        self.state.save()
        self._evidence(entry, part)
        return part

    def _evidence(self, entry, part):
        from . import digests
        w = self.w
        w.record({"op": "seed-part-exported", "table": entry["table"],
                  "part": part["n"], "rows": part["rows"],
                  "digest": part["digest"], "sha256": part["sha256"],
                  "bytes": part["bytes"], "carrier": part["carrier"],
                  "format": entry["format"]})
        if part.get("where"):
            digests.record(w.hop, w.db, entry["table"], "src", part["rows"],
                           part["digest"], method=digests.NEUTRAL,
                           columns=entry["digest_columns"],
                           where=part["where"], engine=w.src.CANON_ENGINE)

    # -- the formats -------------------------------------------------------

    def rows_part(self, entry, n, batches, extra, late=None):
        at, classes = _digest_at(entry)
        return self._put(entry, n, lambda stats: _rows_pieces(
            batches, at, classes, stats), extra, late)

    def binary_part(self, entry, n, where, extra):
        """A part in PostgreSQL's binary copy format, read under the
        table's exported snapshot, digested by the source in the same
        transaction."""
        from .engines import pg_pass
        w, pg = self.w, self.w.eng
        with w.lock:
            snap = self.snaps.get(entry["table"])
            if snap is None:
                snap = self.snaps[entry["table"]] = pg_pass.Snapshot(
                    pg, w.db, (w.hop.options or {}).get("snapshot_limit"))
        for attempt in (1, 2):
            export = snap.lease()
            try:
                return self._binary(entry, n, where, export, extra)
            except pg_pass.SnapshotLost:
                snap.lost(export)
                if attempt == 2:
                    raise
            finally:
                snap.release(export)

    def _binary(self, entry, n, where, export, extra):
        from .engines import pg_pass
        w, pg = self.w, self.w.eng
        qt = pg._qualified("src", w.db, entry["table"])
        conn = pg_pass.connect(pg, "src", w.db, pg_pass.DIGEST_OPTIONS)
        try:
            conn.execute("begin transaction isolation level repeatable read"
                         " read only")
            try:
                conn.execute(f"set transaction snapshot '{export.id}'")
            except Exception as e:
                if any(s in str(e) for s in pg_pass._SNAPSHOT_GONE):
                    raise pg_pass.SnapshotLost(str(e)) from e
                raise
            count, digest = _pg_digest(conn, qt, entry["digest_columns"],
                                       where)
            select = pg._copy_select(qt, entry["copy_columns"], where)
            with conn.cursor() as cur:
                with cur.copy(f"copy ({select}) to stdout (format binary)"
                              ) as cp:
                    def pieces(stats):
                        for data in cp:
                            yield bytes(data)
                        stats["rows"], stats["digest"] = count, digest
                    part = self._put(entry, n, pieces,
                                     {**extra, "mark": export.mark})
            conn.execute("commit")
            return part
        finally:
            conn.close()

    def lines_part(self, entry, n, where, extra):
        """A part of the MySQL bulk load's lines, rendered by the source's
        own client under one consistent snapshot, its digest line first
        (`my_pass.TableCopy._source`)."""
        import types

        from .engines import my_pass
        w, my = self.w, self.w.eng
        cols = [tuple(c) for c in entry["digest_columns"]]
        leaf = w.pair._leaf(entry["table"])
        pass_cols = list(zip(entry["load"]["names"], entry["load"]["kinds"]))
        ns = types.SimpleNamespace(eng=my, db=w.db, table=leaf,
                                   columns=pass_cols,
                                   digest_cols=(cols, cols, []))
        proc = my_pass.TableCopy._source(ns, where, entry["key"][0]
                                         if entry["key"] else None)
        try:
            head = proc.stdout.readline().decode(errors="replace")
            head = head.rstrip("\n").split("\t")
            if len(head) != 2 or not head[0].isdigit():
                raise RuntimeError(_client_said(proc)
                                   or "the source's read gave no digest")

            def pieces(stats):
                for chunk in iter(lambda: proc.stdout.read(1 << 16), b""):
                    yield chunk
                stats["rows"], stats["digest"] = int(head[0]), head[1]
            part = self._put(entry, n, pieces, extra)
            proc.wait(timeout=60)
            if proc.returncode:
                raise RuntimeError(_client_said(proc) or "the source's read"
                                   f" ended with {proc.returncode}")
            return part
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()

    # -- a table -----------------------------------------------------------

    def ranged(self, entry, n, after, upto):
        """One range of a table cut by its key, in its format."""
        w = self.w
        where = _span(w.src, entry["key"][0], after, upto, entry["where"])
        extra = {"after": after, "upto": upto, "where": where,
                 "mark": w.src.snapshot_mark("src", w.db)}
        return self._one(entry, n, where, extra)

    def whole(self, entry):
        extra = {"where": entry["where"] or "",
                 "mark": self.w.src.snapshot_mark("src", self.w.db)}
        return self._one(entry, 0, entry["where"], extra)

    def _one(self, entry, n, where, extra):
        if entry["format"] == "binary copy":
            return self.binary_part(entry, n, where, extra)
        if entry["format"] == "load lines":
            return self.lines_part(entry, n, where, extra)
        return self.rows_part(entry, n, (rows for rows, _ in _batches(
            self.w, entry, where=where)), extra)

    def sequential(self, entry, start=0, after=None, each=None):
        """A table cut in key order or in one pass: its parts from `start`,
        read on from `after`, each handed to `each` once it is put."""
        w = self.w
        feed = _Feed(_batches(w, entry, after))
        n = start
        while feed.peek() is not None or n == 0:
            seen = {"last": after}
            mark = w.src.snapshot_mark("src", w.db)
            part = self.rows_part(
                entry, n, feed.take(entry["per"], seen),
                {"after": key_text(after), "mark": mark},
                lambda: {"last": key_text(seen["last"])})
            after = seen["last"]
            if each is not None:
                each(part)
            n += 1
        entry["done"] = True
        self.state.save()


def _pg_digest(conn, qt, columns, where):
    """(rows, digest) of a range on a PostgreSQL side, the neutral digest
    the check takes (`canon.digest_expr`), in the session it is asked on."""
    row = canon.row_expr("postgres", [tuple(c) for c in columns])
    got = conn.execute(
        f"select count(*)::text, ({canon.digest_expr('postgres', row)})::text"
        f" from {qt}" + (f" where {where}" if where else "")).fetchone()
    return int(got[0]), str(got[1])


def _client_said(proc):
    try:
        return (proc.stderr.read() or b"").decode(
            errors="replace").strip()[-300:]
    except Exception:  # noqa: BLE001 - nothing more to say
        return ""


def _take_position(w, lost=False):
    """The position the stream starts from, taken before a row is read:
    the follow's own, made as `move --mode full+cdc` makes it; for a copy
    alone, one only where asking costs the source nothing. `lost`: the
    one before it is gone from the source's log, and what the server kept
    for it is let go first (`Engine.drop_lost_position`)."""
    if w.mode == "full+cdc":
        tailer = _tailer(w.eng)
        if tailer is None:
            raise SystemExit(
                f"{w.db}: nothing here follows a {w.hop.engine} source, so"
                " the seed could not be carried on from its position - move"
                " it with --mode full")
        check = getattr(tailer, "_can_tail", None)
        if check is not None:
            check(w.db)
        src = getattr(tailer, "src_engine", None) or tailer
        if lost:
            src.drop_lost_position("src", w.db)
        return src.copy_point("src", w.db)
    from .cli import _position_before_copy
    return _position_before_copy(w.eng, w.db)


def _needed(t, exported=None):
    """Seconds from the position to the far end's first change: the
    export (as it went, or as measured), the shipping, the load."""
    if t.shipping is None:
        return None
    return ((exported if exported is not None else (t.export or 0))
            + t.shipping + (t.load or 0))


def export(w, carrier, t, go):
    """The export half of one database, onto `carrier`: the position, every
    table's parts, and the seal once every part is on a carrier. Returns
    the `Done`."""
    from . import evidence
    recipients = evidence.recipients_of(w.hop)
    if not recipients:
        raise SystemExit(
            "a carrier travels, and what it holds is sealed to the hop's"
            " at_rest recipients - this hop names none: add"
            " options.at_rest.recipients (an SSH or age public key whose"
            " private half the far end holds)")
    ex = _Exporter(w, carrier, recipients)
    st = ex.state.data
    if st.get("sealed"):
        seal = st["sealed"]
        w.log(f"{w.db}: the seed was sealed at {seal['sealed_at']} -"
              f" {seal['parts']:,} parts, {_human(seal['bytes'])}; ship the"
              " carrier, and run the same command where the target is")
        return Done("export", position=st.get("position"), said="sealed")
    tables = _tables(w)
    if not go:
        _say_export_plan(w, carrier, tables, t)
        return Done("export", said="planned")
    if "position" not in st:
        st["position"] = _take_position(w)
        st["position_at"] = _stamp()
        st["began"] = time.time()
        ex.state.save()
        w.log(f"{w.db}: the stream's position, taken before a row is read:"
              f" {str(st['position'])[:80]}")
    if st["position"] is not None and "room" not in st:
        ok, words = room(w.src, w.db, st["position"], _needed(t))
        st["room"] = {"ok": ok, "said": words, "at": time.time()}
        ex.state.save()
        w.log(f"{w.db}: {words}"
              + (" - it runs short: raise how long the source keeps its log,"
                 " or ship sooner; " + RUNS_OUT if ok is False
                 else "; " + RUNS_OUT))
    plans = st.setdefault("tables", {})
    for src_t in tables:
        if src_t not in plans:
            plans[src_t] = _plan_table(w, src_t)
            e = plans[src_t]
            w.log(f"{src_t}: {e['cut']} ({e['because']['cut']}), as"
                  f" {e['format']} ({e['because']['format']})")
            ex.state.save()
    _check_limits(w, carrier, plans)
    items = []
    for e in plans.values():
        if e["done"]:
            continue
        have = carrier.store.listing(f"{w.prefix()}parts/{e['id']}/") \
            if e["parts"] else {}
        for k, p in list(e["parts"].items()):
            # a part this carrier no longer holds whole is written again;
            # one on a carrier before this one is that carrier's
            if p.get("carrier") == carrier.said \
                    and have.get(p["name"]) != p["bytes"]:
                w.log(f"{e['table']}: part {k} is not whole on"
                      f" {carrier.said}; it is written again")
                del e["parts"][k]
        if e["cut"] == "ranges":
            for i, (a, u) in enumerate(e["ranges"]):
                if str(i) not in e["parts"]:
                    items.append(("range", e, i, a, u))
        elif e["cut"] == "whole":
            if "0" not in e["parts"]:
                items.append(("whole", e, 0, None, None))
        else:
            items.append(("sequential", e, None, None, None))
    from . import ranges

    def run(item):
        kind, e, i, a, u = item
        if kind == "range":
            ex.ranged(e, i, a, u)
        elif kind == "whole":
            ex.whole(e)
        else:
            done = sorted(int(k) for k in e["parts"])
            # the first part missing: the table goes on from the one before
            gap = next((i for i, k in enumerate(done) if k != i), len(done))
            if e["cut"] == "key order" and gap:
                for k in done[gap:]:
                    del e["parts"][str(k)]
                last = e["parts"][str(gap - 1)].get("last")
                ex.sequential(e, gap, key_of(last))
            else:
                # one pass cannot go on from the middle: from its start
                e["parts"].clear()
                ex.sequential(e)
        return 1
    try:
        ranges.Slots(w.workers(len(items))).each(items, run)
    finally:
        for snap in ex.snaps.values():
            snap.close()
    for e in plans.values():
        if e["cut"] == "ranges" and len(e["parts"]) == len(e["ranges"]):
            e["done"] = True
        elif e["cut"] == "whole" and "0" in e["parts"]:
            e["done"] = True
    ex.state.save()
    return _seal(w, ex, t)


def _check_limits(w, carrier, plans):
    """The seed held to the carrier's limits before a part is written:
    how many files it holds, how long a name, and how many carriers of
    this one's size it takes."""
    lim = carrier.limits
    total = sum(int((w.facts().get(t) or {}).get("bytes") or 0)
                for t in plans)
    parts = sum(len(e.get("ranges") or []) or max(1, math.ceil(
        (e.get("rows_est") or 0) / max(e["per"], 1))) for e in plans.values())
    longest = max((len(_part_name(w, e, 999999)) for e in plans.values()),
                  default=0)
    if lim.name_chars is not None and longest > lim.name_chars:
        raise SystemExit(f"a part's name is {longest} characters and"
                         f" {carrier.said} takes {lim.name_chars} at most")
    if lim.files is not None and parts > lim.files:
        raise SystemExit(f"the seed is about {parts:,} parts and"
                         f" {carrier.said} holds {lim.files:,} files at"
                         f" most ({lim.whence})")
    holds = lim if lim.capacity else carrier.store.limits()
    need = carriers.carriers_needed(total, holds)
    if need and need > 1:
        w.log(f"{w.db}: {_human(total)} as the source counts it, and"
              f" {carrier.said} holds {_human(holds.capacity)}"
              f" ({holds.whence}): about {need} carriers before compression"
              " - each goes on from where the last stopped")


def _say_export_plan(w, carrier, tables, t):
    lim = carrier.limits
    w.log(f"{w.db}: would export {len(tables)} tables to {carrier.said},"
          f" taken because {carrier.reason}; it holds"
          f" {_human(lim.capacity) if lim.capacity else 'what it has free'}"
          + (f" ({lim.whence})" if lim.whence else ""))
    w.log(f"{w.db}: the stream's position is taken before a row is read,"
          " and how long the source keeps its log back to it is held"
          f" against the shipping - {RUNS_OUT}")


def _seal(w, ex, t):
    """The manifest, encrypted to the recipients and signed, and the seal
    written last - once every table's parts are on a carrier."""
    from . import evidence, export as evidence_export
    st = ex.state.data
    open_ = [e["table"] for e in st["tables"].values() if not e["done"]]
    if open_:
        raise SystemExit(f"{w.db}: {len(open_)} tables still have parts to"
                         f" write ({', '.join(open_[:4])}); run again")
    took = time.time() - float(st.get("began") or time.time())
    if st.get("position") is not None:
        ok, words = room(w.src, w.db, st["position"], _needed(t, took))
        st["room_at_seal"] = {"ok": ok, "said": words}
        w.log(f"{w.db}: at the seal, {words}"
              + ("; " + RUNS_OUT if ok is False else ""))
    manifest = {
        "format": FORMAT, "hop": w.hop.name, "db": w.db,
        "engines": {"source": w.src.CANON_ENGINE,
                    "target": w.dst.CANON_ENGINE},
        "mode": w.mode, "position": st.get("position"),
        "position_at": st.get("position_at"), "room": st.get("room"),
        "room_at_seal": st.get("room_at_seal"),
        "exported_seconds": round(took, 1),
        "sealed_at": _stamp(),
        # the scope its tables, rows and columns were taken under: a load
        # under another holds every entry to the scope it has then
        # (`_in_scope_now`)
        "scope_fingerprint": w.hop.scope_fingerprint(),
        "tables": [{k: v for k, v in e.items() if k != "because"}
                   for e in st["tables"].values()]}
    body = json.dumps(manifest, sort_keys=True, default=str).encode()
    sealed = evidence._seal(body, ex.recipients)
    local = w.spool / MANIFEST
    local.write_bytes(sealed)
    key = evidence_export._signing_key(w.hop)
    signed = False
    if key:
        try:
            evidence_export._sign(local, key, NAMESPACE)
            signed = True
        except Exception as e:  # noqa: BLE001 - said; the seed stands
            w.log(f"{w.db}: the manifest could not be signed"
                  f" ({_first(e)}); it is written unsigned")
    store = ex.carrier.store
    store.put(w.prefix() + MANIFEST, local)
    if signed:
        store.put(w.prefix() + SIGNATURE, str(local) + ".sig")
        os.unlink(str(local) + ".sig")
    sha = hashlib.sha256(sealed).hexdigest()
    local.unlink()
    parts = sum(len(e["parts"]) for e in st["tables"].values())
    stored = sum(p["bytes"] for e in st["tables"].values()
                 for p in e["parts"].values())
    seal = {"format": FORMAT, "hop": w.hop.name, "db": w.db,
            "parts": parts, "bytes": stored, "manifest_sha256": sha,
            "signed": signed, "sealed_at": manifest["sealed_at"]}
    store.put_bytes(w.prefix() + SEALED, json.dumps(seal).encode())
    st["sealed"] = seal
    ex.state.save()
    w.record({"op": "seed-sealed", "parts": parts, "bytes": stored,
              "manifest_sha256": sha, "signed": signed,
              "carrier": ex.carrier.said})
    return Done("export", position=st.get("position"), said=(
        f"sealed: {parts:,} parts, {_human(stored)}, on {ex.carrier.said}"))


# ---- the load ---------------------------------------------------------------

def seal_at(store, hop, db):
    """The seal of this hop's seed of `db` at `store`, or None."""
    raw = store.get_bytes(f"{hop.name}/{db}/{SEALED}")
    if raw is None:
        return None
    got = json.loads(raw)
    if got.get("hop") != hop.name or got.get("db") != db:
        return None
    return got


def _signers(w):
    """The allowed signers a manifest is held to: the hop's `signers`, or
    the public half of the key it signs its evidence with."""
    from . import export as evidence_export
    s = settings(w.hop) or {}
    if s.get("signers"):
        return os.path.expanduser(str(s["signers"])), True
    key = evidence_export._signing_key(w.hop)
    pub = key + ".pub" if key else None
    if pub and os.path.exists(pub):
        allowed = w.spool / "allowed_signers"
        allowed.write_text(f"migkit {open(pub).read().strip()}\n")
        return str(allowed), False
    return None, False


def read_manifest(w, store, seal):
    """The manifest the seal names: its hash held to the seal's, its
    signature checked, decrypted with this machine's identity."""
    from . import evidence, export as evidence_export
    raw = store.get_bytes(w.prefix() + MANIFEST)
    if raw is None:
        raise SystemExit(f"{w.db}: the seal is at {store.said} and the"
                         " manifest is not")
    if hashlib.sha256(raw).hexdigest() != seal["manifest_sha256"]:
        raise SystemExit(f"{w.db}: the manifest at {store.said} is not the"
                         " one the seal names - nothing is loaded from it")
    allowed, named = _signers(w)
    sig = store.get_bytes(w.prefix() + SIGNATURE)
    if sig is None and (named or seal.get("signed")):
        raise SystemExit(f"{w.db}: the manifest carries no signature, and"
                         + (" the hop names who must sign it" if named
                            else " its seal says it was signed")
                         + " - nothing is loaded from it")
    if sig is not None:
        if allowed is None:
            w.log(f"{w.db}: the manifest is signed, and the hop names no"
                  " signers to check it against (ship: signers:)")
        else:
            local = w.spool / MANIFEST
            local.write_bytes(raw)
            (w.spool / SIGNATURE).write_bytes(sig)
            try:
                who = evidence_export._verify(local, w.spool / SIGNATURE,
                                              allowed, NAMESPACE)
            except Exception as e:  # noqa: BLE001 - said, and stopped
                raise SystemExit(f"{w.db}: the manifest's signature does"
                                 f" not hold ({_first(e)}) - nothing is"
                                 " loaded from it") from e
            finally:
                local.unlink(missing_ok=True)
                (w.spool / SIGNATURE).unlink(missing_ok=True)
            w.log(f"{w.db}: the manifest is signed by {who}")
    got = json.loads(evidence._open(raw))
    if got.get("hop") != w.hop.name or got.get("db") != w.db:
        raise SystemExit(f"{w.db}: the manifest is of {got.get('hop')}"
                         f" {got.get('db')}, not of this hop's {w.db}")
    if got.get("format") != FORMAT:
        raise SystemExit(f"{w.db}: the seed is in format {got.get('format')},"
                         f" and this migkit reads {FORMAT}")
    return got


class _Target:
    """One table as the load writes it: its name there, the columns the
    parts' rows land in, how a batch is read back, and what its digest
    there is taken over."""

    def __init__(self, w, entry, log):
        pair, db, src_t = w.pair, w.db, entry["table"]
        self.entry = entry
        self.dst_t = _target_table(w, src_t, log)
        src_cols, dst_cols, notes = pair._move_columns(src_t, self.dst_t, db)
        for n in notes:
            log(f"{src_t}: {n}")
        names = [n for n, _ in entry["columns"]]
        gone = [n for n, _ in src_cols if n not in names]
        if gone:
            raise SystemExit(f"{src_t}: the source has columns the parts do"
                             f" not ({', '.join(gone[:4])}) - its shape"
                             " changed since the export; move it again")
        self.src_cols, self.dst_cols = src_cols, dst_cols
        self.to_src = [names.index(n) for n, _ in src_cols]
        self.m_at, self.m_classes = _digest_at(entry)
        sc, dc, _ = pair._comparable_columns(db, w.src, src_t, w.dst,
                                             self.dst_t)
        self.sc, self.dc = sc, dc
        self.sc_at = [names.index(n) for n, _ in sc]
        self.same = [list(c) for c in sc] == entry["digest_columns"]
        _, self.dst_where = pair._row_scope(db, src_t)
        key = entry["key"]
        self.key = key
        self.label = pair.move_key(db, "", pair._leaf(src_t))
        plan = (pair._read_back_plan(db, src_t, self.dst_t, src_cols, key)
                if key and (w.hop.options or {}).get("verify_batches", True)
                else None)
        # the batch is folded here, never asked of the source again: the
        # source has moved on since the part was read
        self.plan = plan[:5] if plan else None
        to_dst = dict(zip([n for n, _ in src_cols],
                          [n for n, _ in dst_cols]))
        self.dst_key = [to_dst.get(k, k) for k in key]

    def span(self, w, part):
        """The part's own rows on the target, in its SQL: a range of the
        key, or the whole table under the hop's row filter."""
        if part.get("upto") is not None:
            return _span(w.dst, self.dst_key[0], part["after"],
                         part["upto"], self.dst_where)
        return self.dst_where


def _target_table(w, src_t, log):
    """The table the rows land in: the target's, matched as the copier
    matches it, or made as the copier makes it - its renames read with
    the database (`_renamer`), so a rule naming it is not passed over."""
    pair, db = w.pair, w.db
    leaf = pair._leaf(src_t)
    there = ([] if w.dst.target_missing(db)
             else w.dst.neutral_tables("dst", db))
    pairs, _, _, _ = pair.match_tables(
        w.src.neutral_tables("src", db), there, pair._renamer(db))
    match = [d for s_, d in pairs if pair._leaf(s_) == leaf]
    if match:
        return match[0]
    want = pair._leaf(pair._rename(src_t, db))
    if w.dst.CREATES_ON_WRITE:
        return want
    return pair._create_target(db, src_t, want, log)


class _Loader:
    """The load half of one database: the manifest, its state, and each
    part read, written and proved."""

    def __init__(self, w, store, manifest, seal):
        self.w, self.store, self.manifest = w, store, manifest
        self.state = _State(w.dir / LOAD_STATE)
        if self.state.data.get("seal") != seal["manifest_sha256"]:
            if self.state.data:
                w.log(f"{w.db}: another seed than the one this load began"
                      " on; it is loaded from its start")
            self.state.data = {"seal": seal["manifest_sha256"],
                               "tables": {}}
            self.state.save()
        self.targets = {}

    def tstate(self, table):
        with self.state.lock:
            return self.state.data["tables"].setdefault(
                table, {"parts": {}, "started": False, "done": False})

    # -- a part's file --------------------------------------------------------

    @contextlib.contextmanager
    def file(self, part, store=None):
        """The part's file as kept, its hash and size held to the
        manifest's before a byte of it is read."""
        store = store or self.store
        local = store.local(part["name"])
        fetched = None
        if local is None:
            fetched = self.w.spool / os.path.basename(part["name"])
            try:
                store.fetch(part["name"], fetched)
            except Exception as e:  # noqa: BLE001 - read again then
                fetched.unlink(missing_ok=True)
                raise BadPart(f"it could not be read ({_first(e)})") from e
            local = fetched
        try:
            if not os.path.exists(local):
                raise BadPart("it is not there")
            sha, n = sha256_of(local)
            if (sha, n) != (part["sha256"], part["bytes"]):
                raise BadPart(f"its bytes are not those the manifest names"
                              f" ({n:,} bytes, sha256 {sha[:12]}...)")
            yield local
        finally:
            if fetched is not None:
                fetched.unlink(missing_ok=True)

    # -- the formats ----------------------------------------------------------

    def rows(self, tgt, part, path, clear):
        """A part of rows: folded again and held to the manifest's digest,
        each batch written and read back by its key where it has one."""
        w = self.w
        if clear is not None:
            w.dst.neutral_empty("dst", w.db, tgt.dst_t,
                                **({"where": clear} if clear else {}))
        n, fold_m, fold_s = 0, 0, 0
        with opened(path) as r:
            try:
                for batch in _batches_of(r):
                    _, fold_m = canon.fold_rows(
                        tgt.m_classes, ([row[i] for i in tgt.m_at]
                                        for row in batch), fold_m)
                    if not tgt.same:
                        _, fold_s = canon.fold_rows(
                            [c for _, c in tgt.sc],
                            ([row[i] for i in tgt.sc_at] for row in batch),
                            fold_s)
                    n += len(batch)
                    rows = [[row[i] for i in tgt.to_src] for row in batch]
                    rows, _ = w.pair._flatten_absent(rows)
                    if tgt.plan is not None:
                        w.pair._write_checked(w.db, tgt.label, tgt.dst_t,
                                              tgt.dst_cols, tgt.plan,
                                              tgt.key, rows, w.log)
                    else:
                        w.dst.neutral_write("dst", w.db, tgt.dst_t,
                                            tgt.dst_cols, rows)
            except (pickle.UnpicklingError, EOFError) as e:
                raise BadPart(f"its rows could not be read ({_first(e)})"
                              ) from e
        if (n, str(fold_m)) != (int(part["rows"]), str(part["digest"])):
            raise BadPart(f"it holds {n:,} rows digesting {fold_m}, where"
                          f" the manifest says {int(part['rows']):,} and"
                          f" {part['digest']}")
        return n, str(fold_m if tgt.same else fold_s)

    def binary(self, tgt, part, path, where):
        """A part in PostgreSQL's binary copy format: the range emptied,
        loaded and digested in one transaction, committed only where the
        target's digest is the manifest's."""
        from .engines import pg_pass
        w, pg = self.w, self.w.eng
        qt = pg._qualified("dst", w.db, tgt.dst_t)
        opts = " ".join(x for x in (pg_pass.LOAD_OPTIONS,
                                    pg._load_options(w.db, qt)) if x)
        cols = ", ".join(pg._quote_ident(c) for c in
                         tgt.entry["copy_columns"])
        conn = pg_pass.connect(pg, "dst", w.db, opts)
        try:
            with conn.transaction():
                for k, v in _digest_session():
                    conn.execute(f"set local {k} = '{v}'")
                conn.execute(f"delete from {qt}"
                             + (f" where {where}" if where else ""))
                with conn.cursor() as cur:
                    with cur.copy(f"copy {qt} ({cols}) from stdin (format"
                                  " binary)") as cp:
                        with opened(path) as r:
                            for chunk in iter(lambda: r.read(1 << 16), b""):
                                cp.write(chunk)
                got = _pg_digest(conn, qt, tgt.entry["digest_columns"],
                                 where)
                if got != (int(part["rows"]), str(part["digest"])):
                    raise Different(
                        f"the target holds {got[0]:,} rows digesting"
                        f" {got[1]} after the load, where the part is"
                        f" {int(part['rows']):,} and {part['digest']}")
        finally:
            conn.close()
        return got[0], got[1]

    def lines(self, tgt, part, path, where):
        """A part of the MySQL bulk load's lines: the range emptied and
        loaded in one transaction, committed only where the load raised no
        warning and the target's digest is the manifest's."""
        from .engines.mysql import _load_feed
        w, my = self.w, self.w.eng
        cols = [tuple(c) for c in tgt.entry["digest_columns"]]
        leaf = tgt.dst_t
        conn = my._conn("dst")
        try:
            with conn.cursor() as cur:
                cur.execute("set foreign_key_checks = 0")
                cur.execute("set session time_zone = '+00:00'")
                cur.execute("set session transaction isolation level read"
                            " committed")
                cur.execute(f"delete from `{my._d('dst', w.db)}`.`{leaf}`"
                            + (f" where {where}" if where else ""))
                with opened(path) as r:
                    taken = _load_feed(cur, my._d("dst", w.db), leaf,
                                       tgt.entry["load"]["names"],
                                       tgt.entry["load"]["kinds"], r)
                warned = int(cur.warning_count or 0)
                cur.execute(my._range_digest_sql("dst", w.db, leaf, where,
                                                 (cols, cols, [])))
                n, d = cur.fetchone()
            got = (int(n), str(d))
            if warned or taken != int(part["rows"]) or got != (
                    int(part["rows"]), str(part["digest"])):
                conn.rollback()
                raise Different(
                    f"{taken:,} of {int(part['rows']):,} rows taken,"
                    f" {warned:,} warnings, the target digesting {got[1]}"
                    f" where the part is {part['digest']}")
            conn.commit()
        finally:
            conn.close()
        return got

    # -- a part ---------------------------------------------------------------

    def part(self, tgt, part, store=None, relinked=False):
        """One part read, written and proved; its record kept. Raises
        `BadPart` where its file is not the one the manifest names."""
        from . import digests
        w, entry = self.w, tgt.entry
        where = tgt.span(w, part)
        began = time.monotonic()
        with self.file(part, store) as path:
            if entry["format"] == "binary copy":
                n, d = self.binary(tgt, part, path, where)
            elif entry["format"] == "load lines":
                n, d = self.lines(tgt, part, path, where)
            else:
                # a range, or a table that is one part, is emptied before
                # its rows go in, so a part loaded again is not loaded twice
                clear = (where or "") if entry["cut"] in ("ranges", "whole") \
                    else None
                n, d = self.rows(tgt, part, path, clear)
        columns = tgt.dc if entry["format"] == "rows" else [
            tuple(c) for c in entry["digest_columns"]]
        if entry["format"] != "rows":
            proved = "the target's digest of the part in its transaction"
        elif entry["cut"] == "ranges":
            there = w.dst.neutral_digest("dst", w.db, tgt.dst_t, tgt.dc,
                                         where=where)
            if (int(there[0]), str(there[1])) != (n, d):
                raise Different(
                    f"the target holds {int(there[0]):,} rows digesting"
                    f" {there[1]} in the range, where the part's rows are"
                    f" {n:,} and {d}")
            proved = "the target's digest of the range"
        elif tgt.plan is not None:
            proved = "every row read back by its key"
        else:
            proved = "the table's digest once every part is in"
        if part.get("upto") is not None:
            digests.record(w.hop, w.db, entry["table"], "dst", n, d,
                           method=digests.NEUTRAL, columns=columns,
                           where=where, engine=w.dst.CANON_ENGINE)
        rec = {"rows": n, "digest": d, "relinked": relinked,
               "last": part.get("last"),
               "seconds": round(time.monotonic() - began, 3)}
        ts = self.tstate(entry["table"])
        with self.state.lock:
            ts["parts"][str(part["n"])] = rec
        self.state.save()
        w.record({"op": "seed-part-loaded", "table": entry["table"],
                  "part": part["n"], "rows": n, "digest": d,
                  "manifest_digest": part["digest"], "proved": proved,
                  "relinked": relinked})
        return rec

    # -- read again over the link ---------------------------------------------

    def relink(self, tgt, part, why):
        """A part read again from the source over the link, into this
        machine's spool, and loaded as any part: its own range, or for a
        table in key order the rest of it from the part's first key, or a
        table with no key whole."""
        from . import evidence
        w, entry = self.w, tgt.entry
        w.log(f"{entry['table']}: part {part['n']} {why} - read again from"
              " the source over the link")
        spool = carriers.LocalStore(w.spool / "relinked",
                                    "this machine's spool")
        carrier = carriers.Carrier(decide.Rung("spool", "the spool"), spool,
                                   carriers.Limits(), "read again")
        ex = _Exporter(w, carrier, evidence.recipients_of(w.hop))
        ex.state = _State(w.dir / "seed-relink.json")
        again = json.loads(json.dumps(entry, default=str))
        again["parts"] = {}
        ts = self.tstate(entry["table"])

        def each(new):
            # loaded as it is read, and gone from the spool after
            self.part(tgt, new, spool, relinked=True)
            spool.delete(new["name"])
        try:
            if entry["cut"] == "ranges":
                each(ex.ranged(again, part["n"], part["after"],
                               part["upto"]))
            elif entry["cut"] == "whole":
                each(ex.whole(again))
            else:
                if entry["cut"] == "one pass":
                    w.dst.neutral_empty("dst", w.db, tgt.dst_t,
                                        **({"where": tgt.dst_where}
                                           if tgt.dst_where else {}))
                    with self.state.lock:
                        ts["parts"].clear()
                    start, after = 0, None
                else:
                    start, after = int(part["n"]), key_of(part.get("after"))
                    with self.state.lock:
                        for k in [k for k in ts["parts"] if int(k) >= start]:
                            del ts["parts"][k]
                with self.state.lock:
                    ts["relinked_from"] = start
                self.state.save()
                ex.sequential(again, start, after, each)
        finally:
            for snap in ex.snaps.values():
                snap.close()
            shutil.rmtree(w.spool / "relinked", ignore_errors=True)
        w.record({"op": "seed-part-relinked", "table": entry["table"],
                  "part": part["n"], "why": why})

    # -- a table --------------------------------------------------------------

    def target(self, entry):
        with self.w.lock:
            tgt = self.targets.get(entry["table"])
            if tgt is None:
                tgt = self.targets[entry["table"]] = _Target(
                    self.w, entry, self.w.log)
        return tgt

    def start(self, entry):
        """A table's first part: its values held to what the target takes,
        and what the target held of it before emptied."""
        w = self.w
        ts = self.tstate(entry["table"])
        if ts["started"]:
            return
        w.pair.refuse_unfit(w.db, [entry["table"]])
        tgt = self.target(entry)
        if entry["format"] == "binary copy":
            sch, tbl = w.eng._split(tgt.dst_t)
            why = w.eng._binary_unfit(w.db, sch, tbl,
                                      entry["copy_columns"])
            if why:
                raise SystemExit(f"{entry['table']}: the parts are in the"
                                 " binary copy format, and the target's"
                                 f" table does not take it: {why}")
        elif entry["format"] == "load lines":
            why = w.eng._pass_unfit(w.db, tgt.dst_t)
            if why:
                raise SystemExit(f"{entry['table']}: the parts are the bulk"
                                 " load's lines, and the target's table"
                                 f" does not take them: {why}")
        gone = w.dst.neutral_empty("dst", w.db, tgt.dst_t,
                                   **({"where": tgt.dst_where}
                                      if tgt.dst_where else {}))
        if gone:
            w.log(f"{entry['table']}: emptied {gone:,} rows the target held"
                  " before the seed")
        with self.state.lock:
            ts["started"] = True
        self.state.save()

    def ranged(self, entry, part):
        tgt = self.target(entry)
        if str(part["n"]) in self.tstate(entry["table"])["parts"]:
            return
        try:
            self.part(tgt, part)
        except BadPart as e:
            self.relink(tgt, part, str(e))

    def sequential(self, entry):
        """A table cut in key order or in one pass, its parts in order;
        one read again from its bad part on where it was."""
        tgt = self.target(entry)
        ts = self.tstate(entry["table"])
        parts = [entry["parts"][k] for k in sorted(entry["parts"], key=int)]
        if "relinked_from" in ts:
            # a table being read again over the link when the load stopped:
            # in key order from the last part it loaded, one pass from its
            # start
            loaded = sorted(int(k) for k in ts["parts"])
            if entry["cut"] == "key order" and loaded:
                self.relink(tgt, {"n": loaded[-1] + 1,
                                  "after": ts["parts"][str(loaded[-1])]
                                  .get("last")}, "was being read again")
            else:
                self.relink(tgt, {"n": 0, "after": None},
                            "was being read again")
            return
        if entry["cut"] == "one pass" and len(ts["parts"]) < len(parts):
            # one pass cannot go on from the middle, and a part half
            # written before a stop left rows no key tells apart: from its
            # start
            self.w.dst.neutral_empty("dst", self.w.db, tgt.dst_t,
                                     **({"where": tgt.dst_where}
                                        if tgt.dst_where else {}))
            with self.state.lock:
                ts["parts"].clear()
        for p in parts:
            if str(p["n"]) in ts["parts"]:
                continue
            try:
                self.part(tgt, p)
            except BadPart as e:
                self.relink(tgt, p, str(e))
                return

    def finish(self, entry):
        """A table whose every part is in, held as a whole: the target's
        digest of it against the sum of its parts'."""
        from . import digests
        w = self.w
        ts = self.tstate(entry["table"])
        if ts["done"]:
            return
        tgt = self.target(entry)
        rows = sum(int(p["rows"]) for p in ts["parts"].values())
        total = sum(int(p["digest"]) for p in ts["parts"].values())
        dc = tgt.dc if entry["format"] == "rows" else [
            tuple(c) for c in entry["digest_columns"]]
        there = w.dst.neutral_digest("dst", w.db, tgt.dst_t, dc,
                                     **({"where": tgt.dst_where}
                                        if tgt.dst_where else {}))
        if (int(there[0]), str(there[1])) != (rows, str(total)):
            raise SystemExit(
                f"{entry['table']}: every part is in, and the target holds"
                f" {int(there[0]):,} rows digesting {there[1]}, where the"
                f" parts add up to {rows:,} rows and {total} - something"
                " else writes this table, or the target changes what it is"
                " given")
        digests.record(w.hop, w.db, entry["table"], "src", rows, str(total),
                       method=digests.NEUTRAL, columns=tgt.sc
                       if entry["format"] == "rows" else dc,
                       engine=w.src.CANON_ENGINE)
        digests.record(w.hop, w.db, entry["table"], "dst", there[0],
                       there[1], method=digests.NEUTRAL, columns=dc,
                       engine=w.dst.CANON_ENGINE)
        with self.state.lock:
            ts["done"] = True
        self.state.save()
        w.log(f"{entry['table']}: {rows:,} rows loaded and proved, part by"
              " part and as a whole")


def _digest_session():
    """The session settings a PostgreSQL digest is taken under
    (`pg_pass.DIGEST_OPTIONS`), as `set local` pairs."""
    from .engines import pg_pass
    out = []
    for item in pg_pass.DIGEST_OPTIONS.split("-c ")[1:]:
        k, _, v = item.strip().partition("=")
        if k and k != "statement_timeout":
            out.append((k, v))
    return out


def _in_scope_now(w, manifest, entries):
    """The manifest's tables as the hop takes them at the load. A seed
    exported under another scope (`Hop.scope_changed`) is held, entry by
    entry, to the one the hop has now: a table it has since left out is
    not loaded, and said - the load empties each table it writes, and it
    wrote over a table the target owns. What cannot be filtered here
    refuses the load, naming it: a table whose row filter changed holds
    the rows the other filter took, and a table the hop takes now that
    the seed does not hold would be followed with none of its rows. The
    columns and names are the hop's now in the load itself (`_Target`). A
    manifest from before scopes were kept is loaded as it always was."""
    if not w.hop.scope_changed(manifest):
        return entries
    keep, gone, refused = [], [], []
    for e in entries:
        parts = str(e["table"]).split(".")
        if w.hop.excluded(w.db, *parts):
            gone.append(e["table"])
        elif (e.get("where") or None) != w.hop.row_filter(w.db, *parts):
            refused.append(f"{e['table']} (its row filter changed)")
        else:
            keep.append(e)
    have = {e["table"] for e in entries}
    refused += [f"{t} (not in the seed)" for t in _tables(w)
                if t not in have]
    if refused:
        raise SystemExit(
            f"{w.db}: the seed was exported under another scope than the"
            f" hop's now, and {len(refused)} of its tables cannot be held"
            " to this one at the load: " + ", ".join(refused[:6])
            + (" ..." if len(refused) > 6 else "")
            + ". Export it again under this scope")
    if gone:
        w.log(f"{w.db}: the seed was exported under another scope; the"
              f" {len(gone)} tables of it the hop now leaves out are not"
              " loaded: " + ", ".join(gone[:6])
              + (" ..." if len(gone) > 6 else ""))
    return keep


def load(w, store, seal, go):
    """The load half of one database, from `store`: every part read,
    written and proved, every table held as a whole, and the position the
    stream starts from - a new one where the source no longer keeps its
    log back to the seed's (`_delta_by_the_leaves`). Returns the `Done`."""
    from . import interlock
    manifest = read_manifest(w, store, seal)
    entries = _in_scope_now(w, manifest, manifest["tables"])
    # the rows the target already holds in these tables, as the hop says
    # to treat them (target_rows): refused, replaced, or kept and left out
    kept = interlock.before_copy(
        w.hop, w.eng, w.db, [_parts_of(e["table"]) for e in entries], go,
        w.log)
    entries = [e for e in entries if _parts_of(e["table"]) not in kept]
    if not go:
        parts = sum(len(e["parts"]) for e in entries)
        w.log(f"{w.db}: would load {len(entries)} tables, {parts:,} parts"
              f" of {_human(seal['bytes'])}, from {store.said}; each part's"
              " hash checked, its rows held to its digest and proved on the"
              " target, then the stream from the seed's position")
        return Done("load", position=manifest.get("position"),
                    said="planned")
    ld = _Loader(w, store, manifest, seal)
    if ld.state.data.get("loaded") is None:
        _load_tables(w, ld, entries)
        ld.state.data["loaded"] = {"at": _stamp(),
                                   "position": manifest.get("position")}
        ld.state.save()
        w.record({"op": "seed-loaded", "parts": sum(
            len(e["parts"]) for e in entries), "bytes": seal["bytes"],
            "manifest_sha256": seal["manifest_sha256"],
            "from": store.said})
    else:
        w.log(f"{w.db}: the seed was loaded and proved at"
              f" {ld.state.data['loaded']['at']}")
    position = ld.state.data["loaded"]["position"]
    why = lost(w.src, w.db, position) if position is not None else None
    if why and w.mode == "full+cdc":
        # a new position first, so the stream carries what changes from
        # here; then only what changed since the seed's
        w.log(f"{w.db}: {why} - {RUNS_OUT}")
        position = _take_position(w, lost=True)
        _delta_by_the_leaves(w)
        ld.state.data["loaded"]["position"] = position
        ld.state.save()
        w.record({"op": "seed-delta", "why": why})
    elif why:
        w.log(f"{w.db}: {why}; move --mode full+cdc here takes a new"
              " position and copies only the rows that changed since the"
              " seed, found leaf by leaf")
    return Done("load", position=position,
                said=f"loaded and proved: {len(entries)} tables from"
                     f" {store.said}")


def _load_tables(w, ld, entries):
    """Every table's parts written and proved, each table then held as a
    whole, and what the target builds after the rows."""
    from . import ranges
    # the engine's own tables where it makes them from the source's
    # definition, then the pair's for the rest, as the table copier does
    w.eng.create_missing(w.db, w.log)
    if w.pair is not w.eng:
        w.pair.create_missing(w.db, w.log)
    leaves = {w.pair._leaf(e["table"]) for e in entries}
    for e in entries:
        ld.start(e)
    items = []
    for e in entries:
        if ld.tstate(e["table"])["done"]:
            continue
        if e["cut"] in ("ranges", "whole"):
            items += [("range", e, p) for p in e["parts"].values()]
        else:
            items.append(("sequential", e, None))

    def run(item):
        kind, e, p = item
        if kind == "range":
            ld.ranged(e, p)
        else:
            ld.sequential(e)
        return 1
    n = w.workers(len(items)) if getattr(w.dst, "WRITES_IN_PARALLEL",
                                         True) else 1
    with w.eng.load_window(w.db, w.log, leaves):
        ranges.Slots(n).each(items, run)
    for e in entries:
        ld.finish(e)
    w.eng.finish_created(w.db, w.log)
    if w.pair is not w.eng:
        w.pair.finish_created(w.db, w.log)
    w.eng.carry_database_objects(w.db, w.log)


def _delta_by_the_leaves(w):
    """What changed on the source since the seed's position, carried
    without shipping the seed again: taken after a new position (so the
    stream carries what changes from here), found by the check's own
    drilldown, leaf by leaf, and copied by the repair - then checked
    again. Behind the hop's approvals for a repair."""
    from . import approvals
    from .engines.base import LEFT_OUT
    eng, db = w.eng, w.db
    approvals.require(w.hop, "repair", db, w.log)
    differ = [r for r in eng.check_data(db) if r.status == "diff"]
    if not differ:
        w.log(f"{db}: nothing changed on the source that the seed does not"
              " hold")
        return
    w.log(f"{db}: {len(differ)} tables changed since the seed; copying"
          " what differs, leaf by leaf")
    for action in eng.repair_plan(db, "rows"):
        if action.kind == LEFT_OUT:
            w.log(f"{db}: {action.note}")
            continue
        eng.apply(db, action)
    again = [r for r in eng.check_data(db) if r.status == "diff"]
    if again:
        raise SystemExit(
            f"{db}: after the rows that changed were copied, "
            f"{', '.join(r.scope for r in again[:4])} still differ -"
            " `migkit check` names the rows")


# ---- which half runs here ---------------------------------------------------

@dataclass
class Done:
    """What a half did for one database."""
    half: str
    position: object = None
    said: str = ""


def carry(hop, eng, db, mode, go, log, via=None):
    """The seed on media for this move, where the hop names a carrier:
    {database: `Done`} - the load where a sealed seed is where the far end
    reads it, else the export - or None where the copy over the link is
    the way (measured faster, within the deadline). Said as it goes."""
    s = settings(hop)
    if s is None or mode not in ("full", "full+cdc"):
        return None
    from .engines import ALIASES
    engine = ALIASES.get(hop.engine, hop.engine)
    if via is None:
        from . import movers
        via = movers.fitted(hop, engine, movers.for_hop(hop, engine)[0])[0]
    dbs = [db] if db else eng.databases()
    out, todo = {}, []
    for d in dbs:
        w = _Work(hop, eng, d, mode, log)
        store, why = carriers.for_load(s, lambda st, d=d: seal_at(st, hop,
                                                                   d))
        if store is not None:
            log(f"{d}: the sealed seed is at {store.said} - {why}")
            out[d] = load(w, store, seal_at(store, hop, d), go)
        else:
            todo.append(w)
    if not todo:
        return out
    begun = [w for w in todo if (w.dir / EXPORT_STATE).exists()]
    t = times(hop, eng, [w.db for w in todo], via)
    choice = choose(hop, t)
    log(said(t, choice))
    if not begun and choice.rung is not None \
            and choice.rung.name == "network":
        return out or None
    carrier = carriers.for_export(s, prove=go)
    log(f"the carrier: {carrier.said}, reached as {carrier.rung.said} -"
        f" {carrier.reason}")
    for w in todo:
        out[w.db] = export(w, carrier, t, go)
    return out
