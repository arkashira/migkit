import contextvars
import json
import os
import re
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .. import fencing
from ..config import MIGKIT_OWN, REPORTS
from ..decide import Need, Rung
from ..tableway import Way
from ..util import (CommandFailed, quote_name, run, split_table,
                    tool_env, which)
from .base import Counted as _Counted
from .base import Engine, Enough as _Enough, RepairAction, Result, clauses
from .base import rows_counted


def _ident(name):
    """One name as PostgreSQL's SQL reads it, its quote doubled inside
    (`util.quote_name`). Names used to be written into statements some
    270 ways by hand - `"{sch}"."{tbl}"`, `'{sch}'` - so a name holding
    a quote broke the statement, and a newline in one written into a
    script started a line of its own (audit 2026-10-01, SEC4);
    `tests/test_postgres_names_are_quoted_one_way.py` fails on a name
    put between quotes by hand."""
    return quote_name("postgres", name)


def _literal(text):
    """One text as a string constant, in the form that reads the same
    whatever `standard_conforming_strings` says: a backslash in it
    written as the escape form's `\\\\`."""
    text = str(text)
    if "\\" in text:
        return "E'" + text.replace("\\", "\\\\").replace("'", "''") + "'"
    return "'" + text.replace("'", "''") + "'"


def _file_name(name):
    """A table's name as a file name: its letters, digits, `.`, `_` and
    `-` kept, anything else - a `/`, a newline - a `_`, and a hash of the
    whole where any was changed, so two names never share a file (audit
    2026-10-01, SEC19)."""
    import hashlib
    plain = re.sub(r"[^A-Za-z0-9._-]", "_", str(name)).lstrip(".") or "_"
    if plain == str(name):
        return plain
    return plain[:80] + "-" + hashlib.sha256(
        str(name).encode()).hexdigest()[:8]


def _kept_whole(path, data):
    """`data` at `path`, whole and on the disk (`durable.write_atomic`),
    sealed to the hop's recipients where the hop keeps the values it
    writes encrypted (`evidence`)."""
    from .. import durable, evidence
    who = path._sealing() if isinstance(path, evidence.EvidencePath) \
        else None
    durable.write_atomic(path, evidence._seal(bytes(data), who) if who
                         else bytes(data))


PGDC_ROOT = REPORTS / "pgdc"

#: migkit's own tables on a side (`config.MIGKIT_OWN`), by their exact
#: names, for a catalogue query to leave out: `migkit\\_%` also hid an
#: application's `migkit_audit` (audit 2026-10-01, DU26)
OWN_SQL = "(" + ", ".join(_literal(t) for t in MIGKIT_OWN) + ")"

INVENTORY_SQL = f"""
with ns as (
  select oid, nspname from pg_namespace
  where nspname not in ('pg_catalog','information_schema')
    and nspname not like 'pg\\_%'
    and nspname not like '\\_\\_%'
)
select case c.relkind when 'v' then 'view' when 'm' then 'matview'
    when 'S' then 'sequence'
    when 'i' then case when i.indisvalid then 'index' else 'invalid-index' end
    when 'I' then case when i.indisvalid then 'index' else 'invalid-index' end
    else 'table' end
  ||'|'||ns.nspname||'.'||c.relname
from pg_class c
join ns on ns.oid = c.relnamespace
left join pg_index i on i.indexrelid = c.oid
where c.relkind in ('r','p','v','m','S','i','I')
  and c.relname not in {OWN_SQL}
union all
select case p.prokind when 'p' then 'procedure' else 'function' end
  ||'|'||ns.nspname||'.'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||')'
from pg_proc p join ns on ns.oid = p.pronamespace
where p.prokind in ('f','p')
union all
select 'trigger|'||ns.nspname||'.'||c.relname||'.'||t.tgname
from pg_trigger t
join pg_class c on c.oid = t.tgrelid
join ns on ns.oid = c.relnamespace
where not t.tgisinternal
union all
select case con.contype when 'p' then 'pk' when 'f' then 'fk'
    when 'u' then 'unique' else 'check' end
  ||'|'||ns.nspname||'.'||rel.relname||'.'||con.conname
from pg_constraint con
join pg_class rel on rel.oid = con.conrelid
join ns on ns.oid = con.connamespace
where con.contype in ('p','f','u','c')
union all
select 'extension|'||extname from pg_extension
"""


#: the head-room one run's own counters take (a cutover's `keys: apart`),
#: set for the thread that plans them: it went through the process's
#: environment, which every other thread of the process read too (audit
#: 2026-10-01, K24)
SEQUENCE_MARGIN = contextvars.ContextVar("sequence_margin", default=None)


def _seq_buffer():
    """Head-room to add above the highest known id: the run's own margin
    (`SEQUENCE_MARGIN`), else SEQUENCE_BUFFER.

    Zero (the default) reseeds to exactly the highest id in use, which is
    correct when the source is quiet. Anything above zero trades a gap in the
    id space for tolerance of rows that arrive a moment later.
    """
    own = SEQUENCE_MARGIN.get()
    if own is not None:
        return max(0, int(own))
    try:
        n = int(os.environ.get("SEQUENCE_BUFFER", "0") or 0)
    except ValueError:
        return 0
    return max(0, n)



def _copy_text(rows):
    """Rows as COPY's text format, or None where a value is of a kind this
    does not write exactly - the caller then sends statements. Text is
    escaped as COPY reads it back; bytes as `\\x` hex; an instant with its
    offset."""
    import datetime
    import decimal
    import io
    import math
    import uuid
    out = io.StringIO()
    for r in rows:
        cells = []
        for v in r:
            if v is None:
                cells.append("\\N")
            elif isinstance(v, bool):
                cells.append("t" if v else "f")
            elif isinstance(v, (int, decimal.Decimal, uuid.UUID)):
                cells.append(str(v))
            elif isinstance(v, float):
                cells.append("NaN" if math.isnan(v) else "Infinity"
                             if v == math.inf else "-Infinity"
                             if v == -math.inf else repr(v))
            elif isinstance(v, (bytes, bytearray, memoryview)):
                cells.append("\\\\x" + bytes(v).hex())
            elif isinstance(v, (datetime.datetime, datetime.date,
                                datetime.time)):
                cells.append(v.isoformat())
            elif isinstance(v, str):
                cells.append(v.replace("\\", "\\\\").replace("\t", "\\t")
                             .replace("\n", "\\n").replace("\r", "\\r"))
            else:
                return None
        out.write("\t".join(cells) + "\n")
    out.seek(0)
    return out


def _binary_kinds():
    """{target type: (the type its value is packed as, the Python types
    it takes, a further test or None)} - the columns a run goes to binary
    COPY for, each taking only a value it lands exactly as `_copy_text`
    lands it. Left to text: `real` (a double packed to single precision
    and the double's shortest text read as one can round apart), a time
    or instant of the other kind (text reads an offset into a `timestamp`
    and drops it), `interval`, `money`, arrays, enums, domains, ranges,
    network and geometric types, and every value not of the one Python
    type its column is listed with here. json and jsonb go as their own
    bytes: a string handed to the driver for them is quoted as a JSON
    string."""
    import datetime
    import decimal
    import uuid

    def ints(bits):
        top = 1 << (bits - 1)
        return lambda v: -top <= v < top
    text = ("text", (str,), None)
    return {
        "int2": ("int2", (int,), ints(16)),
        "int4": ("int4", (int,), ints(32)),
        "int8": ("int8", (int,), ints(64)),
        "numeric": ("numeric", (int, decimal.Decimal), None),
        "float8": ("float8", (float,), None),
        "bool": ("bool", (bool,), None),
        "text": text, "varchar": text, "bpchar": text,
        "bytea": ("bytea", (bytes,), None),
        "json": ("bytea", (str,), None), "jsonb": ("bytea", (str,), None),
        "date": ("date", (datetime.date,), None),
        "timestamp": ("timestamp", (datetime.datetime,),
                      lambda v: v.tzinfo is None),
        "timestamptz": ("timestamptz", (datetime.datetime,),
                        lambda v: v.tzinfo is not None),
        "time": ("time", (datetime.time,), lambda v: v.tzinfo is None),
        "uuid": ("uuid", (uuid.UUID,), None),
    }


_BINARY = []


def _copy_binary(conn, types, rows):
    """Rows as binary COPY for columns of `types` (their names), rendered
    whole in memory before the COPY starts, or None where a column or a
    value is not one `_binary_kinds` sends exactly - the caller then sends
    text, and the server has been sent nothing. Each field goes as the
    server stores it, so it reads no text back into a value."""
    import io

    import psycopg
    from psycopg import Copy
    from psycopg.copy import FileWriter
    if not _BINARY:
        _BINARY.append(_binary_kinds())
    kinds = [_BINARY[0].get(t) for t in types]
    if not rows or None in kinds:
        return None
    width = len(kinds)
    ok = [k[1] for k in kinds]
    more = [k[2] for k in kinds]
    for r in rows:
        for i in range(width):
            v = r[i]
            if v is not None and (type(v) not in ok[i]
                                  or more[i] is not None
                                  and not more[i](v)):
                return None
    own = [i for i, t in enumerate(types) if t in ("json", "jsonb")]
    try:
        if own:
            enc = conn.info.encoding
            lead = [b"\x01" if types[i] == "jsonb" else b"" for i in own]
            rows = [list(r) for r in rows]
            for r in rows:
                for i, head in zip(own, lead):
                    if r[i] is not None:
                        r[i] = head + r[i].encode(enc)
        buf = io.BytesIO()
        with conn.cursor() as cur:
            copy = Copy(cur, binary=True, writer=FileWriter(buf))
            copy.set_types([k[0] for k in kinds])
            for r in rows:
                copy.write_row(r)
            copy.finish(None)
    except (UnicodeEncodeError, ValueError, TypeError, OverflowError,
            psycopg.Error):
        return None
    return buf.getvalue()


class _Piped:
    """The applier's statements sent on a connection without waiting for
    each one's answer (pipeline mode), the answers read once, where the
    batch ends - left while a COPY runs, which the protocol does not allow
    inside it, and taken again after."""

    def __init__(self, conn):
        self.conn, self.now = conn, None

    def on(self):
        if self.now is None:
            self.now = self.conn.pipeline()
            self.now.__enter__()

    def off(self, failed=None):
        """Every answer read; the first error among them raised, unless
        the batch is already failing with `failed`."""
        now, self.now = self.now, None
        if now is not None:
            now.__exit__(type(failed) if failed else None, failed,
                         failed.__traceback__ if failed else None)


#: What every session migkit opens on PostgreSQL is pinned to, whatever the
#: server, the database or the role was set to: a double printed with every
#: digit it takes to read back as itself, and bytes as hex. Measured on 16
#: under `alter database ... set extra_float_digits = 0`, the sum 0.1 + 0.2
#: printed `0.3` - equal to a target that holds 0.3 - where pinned it
#: prints `0.30000000000000004`; and a logical decoding session under
#: `bytea_output = escape` prints bytes in a form only the hex reader here
#: did not know. And UTC with ISO dates and the server's own interval
#: style, so an instant reads the same on both sides whatever each is set
#: to - the client program had those and the driver's connections did
#: not, so business rules ran in each server's own zone (audit
#: 2026-10-01, DU12) - and no statement ended by a timeout the server
#: or the role sets. In this order: a pooler that refuses settings it does
#: not track refuses the first, and says which (`wording.pooler_refused`).
SESSION = ("-c statement_timeout=0 -c extra_float_digits=3"
           " -c bytea_output=hex -c TimeZone=UTC -c DateStyle=ISO"
           " -c IntervalStyle=postgres")

#: seconds a connection is given to be made, and a probe's, which asks
#: whether something answers at all
CONNECT_SECONDS, PROBE_SECONDS = 15, 5


def keepalives():
    """libpq's keepalive settings. Left out, the idle time is the
    operating system's (two hours on macOS): a long query over a
    cross-cloud link that loses its tunnel would hang for hours instead
    of failing; probed sooner it is an error the caller can act on."""
    from .. import envkeys
    return {"keepalives": 1,
            "keepalives_idle": envkeys.number("MIGKIT_KEEPALIVE_IDLE"),
            "keepalives_interval": 10, "keepalives_count": 5}


def connect(ep, dbname, driver="psycopg2", options="", timeout=None,
            retry=True, session=True, **extra):
    """A connection to `ep`'s database `dbname`, made the one way every
    connection migkit makes to PostgreSQL is: the side's TLS as it was
    decided (`Endpoint.tls_forms`), a bounded wait to connect, keepalives,
    migkit's name on the session and its settings (`SESSION`, then
    `options`). `driver` is `psycopg2` or `psycopg`. A failure while
    connecting that tends to clear is tried again (`util.with_retry`);
    `retry=False` for a probe whose answer is the failure. `session=False`
    sends none of `SESSION`, only `options`: for a pooler's console,
    which refuses settings, and for a question about the server's own
    zone.

    Connections were opened thirteen ways: three connect timeouts,
    keepalives on four, UTC on one, the TLS on most (audit 2026-10-01,
    DU12)."""
    from ..util import with_retry
    kw = dict(host=ep.host, port=ep.port, user=ep.user,
              password=ep.password, dbname=dbname,
              connect_timeout=timeout or CONNECT_SECONDS,
              application_name="migkit",
              **keepalives(), **ep.tls_forms("postgres")["libpq"])
    options = " ".join(x for x in (SESSION if session else "", options) if x)
    if options:
        kw["options"] = options
    kw.update(extra)
    if driver == "psycopg":
        import psycopg

        def made():
            return psycopg.connect(**kw)
    else:
        import psycopg2

        def made():
            return psycopg2.connect(**kw)
    return with_retry(made, writes=True) if retry else made()


def _conninfo_value(v):
    """One value of a libpq connection string, quoted where it must be."""
    v = str(v)
    if v and not re.search(r"[\s'\\]", v):
        return v
    return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"


class SqlFailed(CommandFailed):
    """A statement the server refused, in its own words - raised where
    the client program's failure was, so a caller that caught one catches
    the other. `sqlstate` is the server's code for it."""

    def __init__(self, err):
        from ..redact import Redactor
        text = str(err).strip() or type(err).__name__
        said = Redactor()(text if text.startswith("ERROR")
                          else f"ERROR:  {text}")
        RuntimeError.__init__(self, said)
        self.returncode, self.stderr, self.argv = 1, said, []
        self.sqlstate = _sqlstate(err)
        self.pgcode = self.sqlstate


#: a script only the client program reads: a meta-command (`\copy`) on a
#: line of its own
_META = re.compile(r"(^|\n)\s*\\")
#: a statement that may leave something in its session - a setting, a
#: temporary table, a lock, a prepared statement, a role - which a
#: session kept for the next statement must not carry into it
_SESSION_WORDS = re.compile(
    r"set_config|advisory|\btemp|\binto\b|nextval|currval|lastval|setval"
    r"|prepare|\blisten\b|replication_origin_session|dblink|\bload\b",
    re.I)
#: a statement that may change the shape of a side's tables: the side's
#: catalogue, kept for its readers (`catalog`), is read again after it
_DDL = re.compile(r"\b(create|alter|drop|comment|import|rename)\b", re.I)
#: a session's own temporary table, which no side's catalogue holds
_TEMP = re.compile(r"\bcreate\s+(temp|temporary)\s+table\b|\bon\s+commit"
                   r"\s+drop\b", re.I)


def _changes_shape(sql):
    return bool(_DDL.search(_TEMP.sub("", sql)))
#: a statement that only reads, which may be asked again on a new
#: connection when the kept one was found gone
_READS = re.compile(r"\s*(select|show|with|table|values)\b", re.I)
_WRITES = re.compile(
    r"\b(insert|update|delete|merge|create|drop|alter|truncate|grant"
    r"|revoke|comment|copy|vacuum|analyze|cluster|reindex|refresh|lock"
    r"|call|do|begin|commit|security|notify)\b|pg_drop|pg_create"
    r"|pg_terminate|pg_cancel|pg_replication|pg_logical|_advance|setval"
    r"|nextval|pg_reload|pg_switch|lo_", re.I)


def _plain_read(sql):
    """Whether `sql` is one statement that reads and leaves nothing in its
    session: asked again where the kept connection was found gone, and no
    session reset after it."""
    body = sql.strip().rstrip(";")
    return (bool(_READS.match(body)) and ";" not in body
            and not _WRITES.search(body) and not _SESSION_WORDS.search(body))


#: seconds a kept connection may sit unused before it is asked whether it
#: is still there, and how many databases a thread keeps one to per side
KEPT_IDLE, KEPT_PER_SIDE = 30, 2


def _tid(page):
    """The first row address of heap page `page`, as SQL."""
    return f"{_literal(f'({int(page)},0)')}::tid"


def _first_line(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[0][:200]


def _sqlstate(e):
    """The server's code for an error, from either driver: the rows are
    written through one (`_conn3`) and everything else read through the
    other (`_conn`)."""
    return getattr(e, "sqlstate", None) or getattr(e, "pgcode", None)


#: an object the statement names is not there; one another session holds
UNDEFINED_OBJECT, OBJECT_IN_USE = "42704", "55006"


#: which relations each list of tables holds (`user_tables_where`): `all`
#: every table a dump sees, a partitioned one and each partition; `logical`
#: the tables as the application names them - a partitioned table, not
#: its partitions; `leaves` every table that holds rows - each partition,
#: not their parent
TABLE_KINDS = {
    "all": "c.relkind in ('r', 'p')",
    "logical": ("c.relkind in ('r', 'p') and not exists (select 1 from"
                " pg_inherits h join pg_class p on p.oid = h.inhparent"
                " where h.inhrelid = c.oid and p.relkind = 'p')"),
    "leaves": "c.relkind = 'r'",
}


def user_tables_where(kinds="logical", own=False):
    """The one rule for which relations of `pg_class c` (in `pg_namespace
    n`) are the application's tables. "A user table" was written seven
    ways: two lists took another session's temporary tables, another
    tool's schemas and migkit's own, two did not, and a partitioned table
    was listed with its partitions by some and without by others - so the
    counts and the fast checksum compared one set, the copy between engines
    another (audit 2026-10-01, DU5). In a schema of the application's: not
    the catalogue's, a session's temporary one or TOAST's (`pg_%`), and -
    unless `own` - not another tool's (`__%`); and not migkit's own table
    (`config.MIGKIT_OWN`), by its exact name, where `migkit\\_%` also hid an
    application's `migkit_audit` (DU26, `OWN_SQL`)."""
    out = (TABLE_KINDS[kinds]
           + " and n.nspname not in ('pg_catalog', 'information_schema')"
           " and n.nspname not like 'pg\\_%'")
    if not own:
        out += (" and n.nspname not like '\\_\\_%' and c.relname not in "
                + OWN_SQL)
    return out


class PostgresEngine(Engine):
    ENGINE_FAMILY = "postgres"
    checks = ("schema", "counts", "autoinc", "data")
    counts_from_data = True
    # pg_settings, read inside the database, carries `alter database set`
    SETTINGS_PER_DATABASE = True
    #: a subscription with copy_data=true copies the tables, then follows
    REPLICA_COPIES = True

    #: every table that holds rows, by the one rule (`user_tables_where`)
    USER_TABLES = ("select n.nspname||'.'||c.relname from pg_class c"
                   " join pg_namespace n on n.oid = c.relnamespace where "
                   + user_tables_where("leaves") + " order by 1")

    def _d(self, side, db):
        """Physical db name for a side: source as given, target through the
        hop's db_map so a migration can land in a differently-named db
        (identity when unmapped). Maintenance db 'postgres' never maps."""
        if side == "dst" and db != "postgres":
            return self.hop.target_db(db)
        return db

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _open(self, side, name, purpose="read", **extra):
        """`_opening`, where a pooler between migkit and the database
        refuses the settings a connection opens with said as that, not
        as the database refusing something (`wording.pooler_refused`)."""
        try:
            return self._opening(side, name, purpose, **extra)
        except Exception as e:  # noqa: BLE001 - said, or raised as it was
            from ..wording import pooler_refused
            pooled = pooler_refused(str(e))
            if pooled and purpose not in ("probe", "ask"):
                where = "source" if side == "src" else "target"
                raise SystemExit(f"the {where}: {pooled}. The connection was"
                                 " refused before it ran anything.") from e
            raise

    def _opening(self, side, name, purpose="read", **extra):
        """A connection to the physical database `name` on one side, for
        `purpose`: `read` (the driver's own types - the rows a check or a
        copy reads), `write` (the driver that sends statements without
        waiting and COPY binary, `_conn3`), `probe` (a short wait to
        connect, never tried again), `ask` (the questions' own, kept by
        `_kept_conn`: every value as the server's text), `slot` (the
        change tail's, kept by `_slot_session`). A target database inside
        a load window is written as a replica (`load_window`) - never on a
        kept connection, which would stay one after the window closed."""
        ep = self._ep(side)
        if purpose in ("ask", "slot"):
            if purpose == "ask":
                extra.setdefault("prepare_threshold", None)
            conn = connect(ep, name, "psycopg" if purpose == "ask"
                           else "psycopg2", client_encoding="UTF8",
                           **extra)
            conn.autocommit = True
            return conn
        options = ""
        if side == "dst" and name in self.__dict__.get("_as_replica", ()):
            options = "-c session_replication_role=replica"
        if purpose == "write":
            import psycopg
            return connect(ep, name, "psycopg", options,
                           cursor_factory=psycopg.ClientCursor,
                           prepare_threshold=None, **extra)
        if purpose == "probe":
            return connect(ep, name, options=options,
                           timeout=extra.pop("timeout", PROBE_SECONDS),
                           retry=False, **extra)
        conn = connect(ep, name, options=options, **extra)
        if purpose == "read":
            self._read_hstore(conn, side, name)
            self._read_json(conn)
            self._read_float4(conn)
        return conn

    def _connect(self, side, db, purpose="read", **extra):
        """`_open` on the database the hop calls `db` - the source's name,
        the target's mapped here, once. Callers mapped it themselves and
        the helper mapped it again at 33 places, so a chained or swapped
        `db_map` read one database for the counts and another for the
        tables (audit 2026-10-01, DU3)."""
        return self._open(side, self._d(side, db), purpose, **extra)

    # ---- one connection kept per side and thread for the questions -------

    def _kept_conn(self, side, name):
        """(connection, whether it was kept from before) this thread asks
        database `name` on one side on. The questions a check and a move
        ask - the catalogue, the fence, a marker - each started the client
        program, a process and a connection of its own: 6.5 ms for the
        process alone and several round trips to sign in, about 0.3 s a
        question over a 50 ms link (audit 2026-10-01, SP2, W15). Kept per
        thread, at most `KEPT_PER_SIDE` databases a side (the maintenance
        database and the one worked on), and asked whether it is still
        there after `KEPT_IDLE` seconds unused."""
        import threading
        lock = self.__dict__.setdefault("_kept_lock", threading.Lock())
        kept = self.__dict__.setdefault("_kept", {})
        me = (os.getpid(), threading.get_ident(), side)
        with lock:
            got = kept.get(me + (name,))
        if got is not None:
            conn, used = got
            if not conn.closed and not conn.broken:
                if time.monotonic() - used < KEPT_IDLE:
                    return conn, True
                try:
                    conn.execute("select 1")
                    return conn, True
                except Exception:  # noqa: BLE001 - opened again below
                    pass
            self._drop_kept(me + (name,))
        conn = self._open(side, name, "ask")
        from psycopg.postgres import types
        from psycopg.types.string import TextLoader
        # every value as the server's own text, as the program printed it
        for t in types:
            conn.adapters.register_loader(t.oid, TextLoader)
            if t.array_oid:
                conn.adapters.register_loader(t.array_oid, TextLoader)
        with lock:
            # a thread that ended keeps nothing; this one keeps the
            # databases it used last
            alive = {t.ident for t in threading.enumerate()}
            gone = [k for k in kept if k[0] == os.getpid()
                    and k[1] not in alive]
            mine = sorted((k for k in kept if k[:3] == me),
                          key=lambda k: kept[k][1])
            gone += mine[:max(0, len(mine) - KEPT_PER_SIDE + 1)]
            for key in gone:
                old = kept.pop(key, None)
                if old is not None:
                    try:
                        old[0].close()
                    except Exception:  # noqa: BLE001 - it is going anyway
                        pass
            kept[me + (name,)] = [conn, time.monotonic()]
        return conn, False

    def _drop_kept(self, key):
        got = self.__dict__.get("_kept", {}).pop(key, None)
        if got is not None:
            try:
                got[0].close()
            except Exception:  # noqa: BLE001 - it is going anyway
                pass

    def release_kept(self, side=None, name=None):
        """The connections this process keeps for questions closed - every
        one, or those to one side's database `name` as it is named there,
        on every thread."""
        for key in [k for k in list(self.__dict__.get("_kept", {}))
                    if k[0] == os.getpid() and side in (None, k[2])
                    and name in (None, k[3])]:
            self._drop_kept(key)

    def _answer(self, conn, sql, statement_timeout):
        """The rows of every statement in `sql`, as the client program
        printed them: a row a line, its values between `|`, NULL as
        nothing. The session is left as a new one would be: a transaction
        the statement opened rolled back, and what it set, made or held
        (`_SESSION_WORDS`, a timeout) dropped."""
        from psycopg.pq import TransactionStatus
        lines = []
        reset = bool(statement_timeout) or not _plain_read(sql)
        try:
            with conn.cursor() as cur:
                if statement_timeout:
                    cur.execute("set statement_timeout = "
                                + _literal(statement_timeout))
                cur.execute(sql)
                while True:
                    if cur.description is not None:
                        lines += ["|".join("" if v is None else v
                                           for v in row)
                                  for row in cur.fetchall()]
                    if not cur.nextset():
                        break
        finally:
            if not conn.closed:
                try:
                    if conn.info.transaction_status != TransactionStatus.IDLE:
                        conn.execute("rollback")
                    if reset:
                        conn.execute("discard all")
                except Exception:  # noqa: BLE001 - dropped below
                    conn.close()
        return "\n".join(lines).rstrip("\n")

    def _psql(self, side, db, sql, statement_timeout=0, physical=None):
        """`sql` on this side's database `db` (the source's name; the
        target's is mapped here), answered as text: a row a line, its values
        between `|` - on a connection kept for this thread
        (`_kept_conn`), the client program only for a script of its own
        meta-commands. A connection found gone is opened again, and the
        statement asked again only where it reads and nothing else
        (`_plain_read`); a statement that writes is never sent twice.
        `physical` names a database of migkit's own as it is (a scratch
        copy), not through the hop."""
        if _META.search(sql):
            return self._psql_program(side, db, sql, statement_timeout,
                                      physical)
        import psycopg
        name = physical or self._d(side, db)
        for again in (False, True):
            try:
                conn, kept = self._kept_conn(side, name)
            except psycopg.Error as e:
                # raised as the program's failure to connect was, for every
                # caller that reads an unreachable side as a RuntimeError
                from ..wording import pooler_refused
                raise SqlFailed(pooler_refused(str(e)) or e) from e
            try:
                got = self._answer(conn, sql, statement_timeout)
            except psycopg.Error as e:
                if not (conn.closed or conn.broken) or again or not kept \
                        or not _plain_read(sql):
                    if conn.closed or conn.broken:
                        self._drop_kept(self._kept_key(side, name))
                    raise SqlFailed(e) from e
                self._drop_kept(self._kept_key(side, name))
                continue
            held = self.__dict__.get("_kept", {}).get(
                self._kept_key(side, name))
            if held is not None:
                held[1] = time.monotonic()
            if _changes_shape(sql):
                self._forget_catalog(side, db)
            return got

    def _kept_key(self, side, name):
        import threading
        return (os.getpid(), threading.get_ident(), side, name)

    def _client(self, side, db, program, *args, physical=None, options="",
                env=None):
        """(argv, env) of a program on libpq for one side's database `db`
        (mapped here; `physical` names the database as it is): the side's
        TLS in the variables libpq reads, a bounded wait to connect,
        keepalives, and the session every connection of migkit's has
        (`SESSION`, then `options`). The programs were started eleven ways,
        some without the TLS the hop asks for (audit 2026-10-01, SEC2)."""
        ep = self._ep(side)
        name = physical or self._d(side, db)
        conninfo = " ".join(f"{k}={_conninfo_value(v)}" for k, v in (
            ("dbname", name), *keepalives().items()))
        argv = [program, "-h", str(ep.host), "-p", str(ep.port), "-U",
                str(ep.user or ""), "-d", conninfo, *args]
        out = {**ep.tls_forms("postgres")["env"],
               "PGPASSWORD": ep.password or "",
               "PGCONNECT_TIMEOUT": str(CONNECT_SECONDS),
               "PGAPPNAME": "migkit",
               "PGOPTIONS": " ".join(x for x in (SESSION, options) if x)}
        out.update(env or {})
        return argv, out

    def _psql_program(self, side, db, sql, statement_timeout=0,
                      physical=None):
        """`sql` through the client program: what only it reads."""
        argv, env = self._client(
            side, db, "psql", "-X", "-At", "-q", "-v", "ON_ERROR_STOP=1",
            "-c", sql, physical=physical,
            options=(f"-c statement_timeout={statement_timeout}"
                     if statement_timeout else ""))
        try:
            return run(argv, env=env).stdout.rstrip("\n")
        finally:
            if _changes_shape(sql):
                self._forget_catalog(side, db)

    CANON_ENGINE = "postgres"
    OWN_PATHS_READ_COLUMN_MAPPING = False

    def _all_tables(self, side, db):
        """Every table on a side as `schema.table`, the excluded ones too.

        The bulk paths need those by name, to tell the dump what to leave
        out; everything else wants `neutral_tables`. By the one rule
        (`user_tables`): another session's temporary table, which
        `pg_tables` listed and no dump carries, is not one.
        """
        return self.user_tables(side, db, "all", own=True)

    def leg_encryption(self, side, db):
        """The server's own word on a connection made as every connection
        migkit makes to it is (`pg_stat_ssl`), and `verified`: whether that
        connection checked the server's certificate, from the `sslmode` it
        was made with - not from what the hop's options ask, which the
        main data paths used to drop while this said verified (audit
        2026-10-01, SEC2). `unverified` names the paths of the hop on
        which the source's certificate is not checked (`far_paths`)."""
        conn = self._connect(side, db, "probe", timeout=CONNECT_SECONDS)
        try:
            mode = (conn.info.dsn_parameters or {}).get("sslmode", "")
            with conn.cursor() as cur:
                cur.execute("select ssl, version, cipher from pg_stat_ssl"
                            " where pid = pg_backend_pid()")
                got = cur.fetchone()
        finally:
            conn.close()
        if not got:
            return None
        ssl, version, cipher = got
        verified = bool(ssl) and mode in ("verify-ca", "verify-full")
        far = self.far_paths() if side == "src" and verified else []
        how = f"{version} {cipher}" if ssl else ""
        if far:
            how += (" - checked on migkit's own connections; not on "
                    + " or ".join(far) + ", which the target makes itself"
                    " and can carry encrypted but not checked")
        return {"encrypted": bool(ssl), "how": how, "verified": verified,
                "unverified": far}

    def far_paths(self):
        """The paths on which the target server dials the source itself,
        and so carries its TLS as far as a server can (`tls_forms`'s
        `far`: `verify-*` as `require`, the authority's file being on
        this machine and not the target's): the pull through a foreign
        server and the temporary subscription (`server_to_server`), and
        the servers' own replication a PostgreSQL target follows the
        source by (unless `MIGKIT_CDC=follow`)."""
        from .. import envkeys, legs
        from . import ALIASES
        if ALIASES.get(self.hop.engine, self.hop.engine) != "postgres":
            return []
        out = []
        if legs.allowed(self.hop):
            out.append("the target pulling the rows from the source")
        if envkeys.text("MIGKIT_CDC").lower() != "follow":
            out.append("the servers' own replication")
        return out

    def _leg_items(self):
        """The base's rows, and where the source's connection is verified
        on migkit's own connections, a row for each path the target makes
        to the source itself (`far_paths`), on which it is not: "verified"
        is said only of what a path checked."""
        out = super()._leg_items()
        verified = any(i.get("item") == "source connection verified"
                       for i in out)
        for path in (self.far_paths() if verified else []):
            out.append({"level": "warn", "scope": "connection",
                        "item": f"source connection NOT verified on {path}",
                        "detail": "the target dials the source itself there,"
                                  " and a server cannot read the authority's"
                                  " file on this machine: encrypted"
                                  " (sslmode require), the certificate not"
                                  " checked"})
        return out

    def run_rule(self, side, db, sql):
        conn = self._connect(side, db)
        try:
            # a rule is the operator's SQL: the session cannot write, so a
            # rule that tries fails instead of changing the source
            conn.set_session(readonly=True)
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
        finally:
            conn.rollback()
            conn.close()

    def stream_writers(self, db):
        """Adds the target's own subscriptions in this database: a logical
        replication apply worker writing beside a repair. The one migkit
        set up for this hop can be paused for it; any other cannot."""
        out = super().stream_writers(db)
        got = self._psql("dst", db,
                         "select s.subname from pg_subscription s"
                         " join pg_database d on d.oid = s.subdbid"
                         " where d.datname = current_database()"
                         " and s.subenabled order by 1")
        return out + [(f"subscription {name}", name == self._repl_name())
                      for name in got.split() if name]

    def canonical_type(self, side, db, declared):
        """An enum is compared as its label, and a domain as the type it
        is built on. Measured across PostgreSQL and MySQL before this: an
        enum and a domain column were left out of the comparison as types
        with no rendering, and a row whose enum differed on the two sides
        was reported equal."""
        cache = self.__dict__.setdefault("_user_types", {})
        key = (side, db)
        if key not in cache:
            kinds = {}
            for line in self._psql(side, db, """
                    select format_type(t.oid, null)||chr(31)||t.typtype::text
                           ||chr(31)||coalesce(format_type(t.typbasetype,
                                                           t.typtypmod), '')
                      from pg_type t
                      join pg_namespace n on n.oid = t.typnamespace
                     where t.typtype in ('e', 'd')
                       and n.nspname not in ('pg_catalog',
                                             'information_schema')"""
                                 ).splitlines():
                name, kind, base = (line.split("\x1f") + ["", ""])[:3]
                kinds[name] = "text" if kind == "e" else base
            cache[key] = kinds
        kinds = cache[key]
        seen = set()
        # a domain can be built on another domain
        while declared in kinds and declared not in seen:
            seen.add(declared)
            declared = kinds[declared]
        return declared

    def target_mark(self, db):
        """The oldest transaction still running on the target as the move
        begins: a row whose own transaction is older was there before it."""
        got = self._psql("dst", db, "select (pg_snapshot_xmin("
                                    "pg_current_snapshot())::text::bigint"
                                    " % 4294967296)::text").strip()
        return {"xid": got} if got else {}

    def who_wrote(self, db, table, keys, began):
        """Measured on PostgreSQL 16: a row the target had before the move,
        left because the target was not emptied, carries a transaction older
        than the mark taken as the move began; one the application wrote
        during the move carries a newer one. Where the target keeps commit
        timestamps, the times come with it."""
        mark = (began or {}).get("xid")
        if not keys:
            return ""
        sch, tbl = self._split(table)
        pkexpr = self._pk_text_expr(self._pk_cols_of(db, table))
        wanted = ", ".join(_literal(k)
                           for k in keys[:1000])
        stamps = self._psql("dst", db, "show track_commit_timestamp") == "on"
        parts = ["count(*)"]
        if mark:
            parts.append(f"count(*) filter (where age(xmin) >"
                         f" age({_literal(int(mark))}::xid))")
        if stamps:
            parts.append("min(pg_xact_commit_timestamp(xmin))::text")
            parts.append("max(pg_xact_commit_timestamp(xmin))::text")
        got = self._psql("dst", db, "select " + "||'|'||".join(
            f"coalesce(({x})::text, '')" for x in parts)
            + f' from {_ident(sch)}.{_ident(tbl)} where {pkexpr} in ({wanted})')
        vals = got.split("|")
        n = int(vals[0] or 0)
        if not n:
            return ""
        said = []
        if mark:
            before = int(vals[1] or 0)
            at = (began or {}).get("at", "")
            said.append(
                f"{before} written before the move of {at} began - the"
                " target was not emptied of them" if before else "")
            if n - before:
                said.append(f"{n - before} written after it began - by the"
                            " load itself, a stream applying twice, or"
                            " something writing to the target")
        if stamps and vals[-2]:
            said.append(f"committed between {vals[-2][:19]} and"
                        f" {vals[-1][:19]}")
        said = [x for x in said if x]
        if not said:
            return (f"{n} rows only on the target; when they were written"
                    " is not known here: no move of migkit's recorded its"
                    " start against this target, and it keeps no commit"
                    " timestamps (track_commit_timestamp)")
        return f"{n} rows only on the target: " + "; ".join(said)

    def pause_writer(self, db, what, window):
        """migkit's own subscription, paused for a repair.

        First it is let catch up with the source as it is now, so that
        nothing the repair is about to write is still on its way. Then it
        is disabled, and the pause holds once its apply worker has gone.
        What the source writes meanwhile waits in the slot and is applied,
        by key, on top of the repair when it is enabled again."""
        if not what.startswith("subscription "):
            return super().pause_writer(db, what, window)
        name = what.split(" ", 1)[1]
        if name != self._repl_name():
            raise ValueError(f"{what} is not migkit's")
        began = time.monotonic()
        if self.fence_wait(db, self.src_lsn(db), timeout=window) is False:
            return False
        self._psql("dst", db, f"alter subscription {name} disable")
        while time.monotonic() - began < window:
            if self._psql("dst", db, "select count(*) from"
                                     " pg_stat_subscription where subname ="
                                     f" {_literal(name)} and pid is not null") == "0":
                return True
            time.sleep(0.5)
        self._psql("dst", db, f"alter subscription {name} enable")
        return False

    def resume_writer(self, db, what):
        if not what.startswith("subscription "):
            return super().resume_writer(db, what)
        name = what.split(" ", 1)[1]
        if name != self._repl_name():
            raise ValueError(f"{what} is not migkit's")
        self._psql("dst", db, f"alter subscription {name} enable")

    def table_facts(self, side, db):
        """Rows as the planner's statistics estimate them (`row_estimate`:
        `None`, not 0, for a table whose count is not known) - and whether
        a primary key exists, and the sizes on disk a plan adds up into
        what the move carries and the room it needs."""
        out = {}
        rows = self.row_estimate(side, db)
        got = self._psql(side, db,
                         "select n.nspname||'.'||c.relname"
                         # as a number: a boolean turned into text is
                         # `true`, not the `t` psql prints for a column
                         "||chr(31)||exists(select 1 from pg_index i"
                         " where i.indrelid = c.oid and i.indisprimary)::int"
                         "||chr(31)||pg_table_size(c.oid)"
                         "||chr(31)||pg_indexes_size(c.oid)"
                         " from pg_class c join pg_namespace n"
                         " on n.oid = c.relnamespace where "
                         + user_tables_where("all", own=True))
        for line in got.splitlines():
            name, key, size, idx = (line.split("\x1f") + ["", "", ""])[:4]
            out[name] = {"rows": rows.get(name),
                         "key": key == "1",
                         "bytes": int(size) if size.isdigit() else None,
                         "index_bytes": int(idx) if idx.isdigit() else None}
        return out

    def row_estimate(self, side, db):
        """{schema.table: rows the statistics estimate, or None where they
        do not know}. Read three ways before, with three answers for
        "unknown" - 0 from `n_live_tup`, 0 from `greatest(reltuples, 0)`,
        None from `reltuples` of -1 - so after a statistics reset or a
        fail-over a table of a billion rows read as empty and was copied
        without checkpoints (audit 2026-10-01, DU18). Unknown is: `reltuples`
        of -1 (never analysed, PostgreSQL 14 and later), or 0 over pages
        that hold something (never analysed before 14, or its statistics
        lost); the statistics collector's live count stands in where it
        has one. A partitioned table's rows are its partitions'."""
        leaves = ("select relid from pg_partition_tree(c.oid) where isleaf"
                  if (self._server_version(side, db) or 0) >= 120000 else
                  "select h.inhrelid from pg_inherits h"
                  " where h.inhparent = c.oid")
        got = self._psql(side, db, f"""
            select n.nspname||'.'||c.relname||chr(31)||coalesce((
              select case when bool_or(x.reltuples < 0
                                  or (x.reltuples = 0 and x.relpages > 0))
                          then null
                          else coalesce(sum(greatest(x.reltuples, 0)), 0)
                     end::bigint::text
                from pg_class x
               where (c.relkind <> 'p' and x.oid = c.oid)
                  or (c.relkind = 'p' and x.oid in ({leaves}))), '')
              ||chr(31)||coalesce((select s.n_live_tup::text
                                     from pg_stat_all_tables s
                                    where s.relid = c.oid
                                      and (s.n_live_tup > 0
                                           or s.last_analyze is not null
                                           or s.last_autoanalyze
                                              is not null)), '')
              from pg_class c join pg_namespace n on n.oid = c.relnamespace
             where """ + user_tables_where("all", own=True))
        out = {}
        for line in got.splitlines():
            name, est, live = (line.split("\x1f") + ["", ""])[:3]
            out[name] = (int(est) if est.isdigit() else
                         int(live) if live.isdigit() else None)
        return out

    def user_tables(self, side, db, kinds="logical", own=False):
        """The application's tables on a side as `schema.table`, by the one
        rule (`user_tables_where`): `logical` the tables as the application
        names them (a partitioned table, not its partitions), `leaves`
        every table that holds rows (each partition, not their parent),
        `all` both. `own` keeps migkit's own tables and other tools'
        schemas, which a dump has to be told to leave out."""
        out = self._psql(side, db,
                         "select n.nspname||'.'||c.relname from pg_class c"
                         " join pg_namespace n on n.oid = c.relnamespace"
                         " where " + user_tables_where(kinds, own)
                         + " order by 1")
        return [line for line in out.splitlines() if line]

    def neutral_tables(self, side, db):
        # without the hop's exclude list, as every other engine's list is:
        # measured, a PostgreSQL-to-MySQL move copied an excluded table
        # into the target that owned it, and stopped only because the
        # target's copy was already there. Nor an online schema change's
        # working tables (`drift.transient`), as on MySQL. The tables as
        # the application names them: a partitioned table and each of its
        # partitions were both listed, so a copy to another engine carried
        # every partition's rows twice (audit 2026-10-01, DU5)
        from .. import drift
        got = self.user_tables(side, db)
        working = drift.transient_among(got)
        return [t for t in got if t not in working and self.in_scope(db, t)]

    def scope_tables(self, side, db):
        """The tables as the application names them and every partition
        (`user_tables` `all`): a move copies each partition as a table of
        its own, so an include or exclude entry naming one takes it or
        leaves it."""
        from .. import drift
        got = self.user_tables(side, db, "all")
        working = drift.transient_among(got)
        return [t for t in got if t not in working
                and self.in_scope(db, t)]

    def column_catalog(self, side, db):
        """{schema.table: [(column, declared type), ...]} of every relation,
        read now in the one catalogue statement (`catalog`) - what a move
        reads before and after itself to notice a DDL on the source, what
        a resumed check and the cutover's gate compare. The type with its
        length and precision (`character varying(50)`, `numeric(12,2)`):
        read from `information_schema.columns`, a column widened or
        narrowed read as no change at all (audit 2026-10-01, SC3)."""
        got = self._shapes(side, db, fresh=True)["catalog"]
        return {t: sorted((c.name, c.declared) for c in cols)
                for t, cols in got.columns.items()}

    # ---- the catalogue, read once a side -----------------------------------

    #: seconds a side's catalogue, read in one statement, is taken as it
    #: stands before it is read again - a check's pass over its tables
    #: reads it once, a loop that runs for hours sees a change within this
    #: long. migkit's own DDL on a side (`_psql`), `forget_shapes` and a
    #: table not found in it read it again at once
    CATALOG_SECONDS = 60

    SHAPE_CACHES = Engine.SHAPE_CACHES + ("_catalogs", "_user_types")

    def _catalog_sql(self, side, db):
        """The statement reading every relation's shape as one JSON object
        a row. `attidentity` (10) and `attgenerated` (12) read as blank on
        a server older than they are."""
        v = self._server_version(side, db) or 0
        ident = "a.attidentity" if v >= 100000 else "''::char"
        gen = "a.attgenerated" if v >= 120000 else "''::char"
        cols = ("(select coalesce(json_agg(a.attname order by k.o),"
                " '[]'::json) from unnest({keys}) with ordinality k(n, o)"
                " join pg_attribute a on a.attrelid = {rel}"
                " and a.attnum = k.n)")
        return f"""
select json_build_object(
  's', n.nspname, 't', c.relname, 'k', c.relkind,
  'part', exists(select 1 from pg_inherits h
                   join pg_class p on p.oid = h.inhparent
                  where h.inhrelid = c.oid and p.relkind = 'p'),
  'cols', coalesce((
    select json_agg(json_build_array(
             a.attname, format_type(a.atttypid, a.atttypmod),
             not a.attnotnull, pg_get_expr(d.adbin, d.adrelid),
             case when a.attcollation <> 0
                   and a.attcollation <> ty.typcollation
                  then co.collname end,
             {ident}, {gen}) order by a.attnum)
      from pg_attribute a
      join pg_type ty on ty.oid = a.atttypid
      left join pg_attrdef d on d.adrelid = a.attrelid
                            and d.adnum = a.attnum
      left join pg_collation co on co.oid = a.attcollation
     where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped),
    '[]'::json),
  'key', (select {cols.format(keys="i.indkey", rel="i.indrelid")}
            from pg_index i where i.indrelid = c.oid and i.indisprimary),
  'ix', coalesce((
    select json_agg(json_build_array(
             ic.relname, i.indisunique,
             {cols.format(keys="i.indkey", rel="i.indrelid")},
             case when i.indexprs is null and i.indpred is null
                   and 0 <> all(i.indkey) then ''
                  else pg_get_indexdef(i.indexrelid) end)
             order by ic.relname)
      from pg_index i join pg_class ic on ic.oid = i.indexrelid
     where i.indrelid = c.oid and not i.indisprimary), '[]'::json),
  'fk', coalesce((
    select json_agg(json_build_array(
             con.conname,
             {cols.format(keys="con.conkey", rel="con.conrelid")},
             pn.nspname || '.' || pc.relname,
             {cols.format(keys="con.confkey", rel="con.confrelid")},
             con.confdeltype, con.confupdtype) order by con.conname)
      from pg_constraint con
      join pg_class pc on pc.oid = con.confrelid
      join pg_namespace pn on pn.oid = pc.relnamespace
     where con.conrelid = c.oid and con.contype = 'f'), '[]'::json),
  'oth', coalesce((
    select json_agg(o.said order by o.said) from (
      select case con.contype when 'c' then 'check ' else 'exclude ' end
             || con.conname || ' ' || pg_get_constraintdef(con.oid) said
        from pg_constraint con
       where con.conrelid = c.oid and con.contype in ('c', 'x')
      union all
      select 'trigger ' || t.tgname || ' ' || md5(pg_get_triggerdef(t.oid)
             || coalesce(p.prosrc, ''))
        from pg_trigger t left join pg_proc p on p.oid = t.tgfoid
       where t.tgrelid = c.oid and not t.tgisinternal) o), '[]'::json)
)::text
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
 where c.relkind in ('r', 'p', 'v', 'm', 'f')
   and n.nspname not in ('pg_catalog', 'information_schema')
   and n.nspname not like 'pg\\_%'"""

    def _shapes(self, side, db, fresh=False):
        """{"at", "catalog", "kind", "part", "identity", "generated"} of
        one side's database, read in one statement (`_catalog_sql`) and
        kept (`CATALOG_SECONDS`)."""
        from .. import catalog as cat
        import threading
        lock = self.__dict__.setdefault("_catalogs_lock", threading.Lock())
        known = self.__dict__.setdefault("_catalogs", {})
        at = (side, self._d(side, db))
        got = known.get(at)
        if not fresh and got is not None and \
                time.monotonic() - got["at"] < self.CATALOG_SECONDS:
            return got
        with lock:
            got = known.get(at)
            if not fresh and got is not None and \
                    time.monotonic() - got["at"] < self.CATALOG_SECONDS:
                return got
            began = time.monotonic()
            columns, keys, indexes, fks, others = {}, {}, {}, {}, {}
            kind, part, identity, generated = {}, {}, {}, {}
            for line in self._psql(side, db,
                                   self._catalog_sql(side, db)).splitlines():
                if not line:
                    continue
                one = json.loads(line)
                t = f"{one['s']}.{one['t']}"
                kind[t], part[t] = one["k"], bool(one["part"])
                columns[t] = [self._column_of(c) for c in one["cols"]]
                others[t] = list(one.get("oth") or [])
                identity[t] = {c[0]: c[5] or "" for c in one["cols"]}
                generated[t] = {c[0]: c[6] or "" for c in one["cols"]}
                if one["key"]:
                    keys[t] = tuple(one["key"])
                indexes[t] = [cat.Index(i[0], tuple(i[2]), bool(i[1]),
                                        i[3] or "") for i in one["ix"]]
                fks[t] = [cat.ForeignKey(f[0], tuple(f[1] or ()), f[2],
                                         tuple(f[3] or ()), f[4], f[5])
                          for f in one["fk"]]
            got = {"at": began,
                   "catalog": cat.Catalog(tables=sorted(columns),
                                          columns=columns, keys=keys,
                                          indexes=indexes, fks=fks,
                                          others=others),
                   "kind": kind, "part": part, "identity": identity,
                   "generated": generated}
            known[at] = got
            return got

    @staticmethod
    def _column_of(c):
        """One column of `_catalog_sql` as `catalog.Column`: what the server
        computes - an identity, a generated expression - in `extra`, in its
        words, where the expression is not a default."""
        from .. import catalog as cat
        name, declared, nullable, default, collation, ident, gen = c[:7]
        extra = ""
        if ident:
            extra = ("identity always" if ident == "a"
                     else "identity by default")
        if gen:
            extra, default = f"generated always as ({default}) stored", None
        return cat.Column(name, declared, "", bool(nullable), default,
                          collation, extra)

    def _shape_of(self, side, db, table):
        """(the side's shapes, the catalogue's name for `table`), or (the
        shapes, None) where it is not there - read again once first, for a
        table made since."""
        sch, tbl = self._split(table)
        name = f"{sch}.{tbl}"
        got = self._shapes(side, db)
        if name not in got["kind"]:
            got = self._shapes(side, db, fresh=True)
        return got, (name if name in got["kind"] else None)

    def _forget_catalog(self, side, db):
        self.__dict__.get("_catalogs", {}).pop((side, self._d(side, db)),
                                               None)

    def catalog(self, side, db):
        """This side's relations as `catalog.Catalog`: each one's columns
        with their declared type, null, default and collation, its key in
        key order, its indexes and foreign keys - read in one statement for
        the whole database and kept (`CATALOG_SECONDS`). The readers of one
        table's columns, key, rules and indexes take from it; each was a
        statement a table, about 16,000 for a check of 2,000 tables (audit
        2026-10-01, SC15, SP2)."""
        return self._shapes(side, db)["catalog"]

    def shape_mark(self, side, db):
        """One digest of everything `shape_catalog` reads, computed by the
        server over the same statement (`_catalog_sql`): a row of 32
        characters where the catalogue is a row a relation. The same
        catalogue gives the same digest, and any change to what drift
        compares gives another - what a tail asks every batch before it
        reads the whole shape again (`hetero._shape_now`, audit SP8)."""
        return self._psql(side, db, (
            "select md5(coalesce(string_agg(r, chr(10) order by r), ''))"
            f" from ({self._catalog_sql(side, db)}) q(r)")).strip() or None

    def shape_catalog(self, side, db):
        """The catalogue of the tables in scope (`neutral_tables`), read
        now: what drift compares from one reading to the next, a table at a
        time by its fingerprint (`catalog.Catalog.shape`), with what
        differs in words (`explain`) - each column's type with its length,
        precision and scale, null, default, collation, identity or
        generated expression, and each table's key, indexes, foreign keys,
        checks and triggers (a trigger by a hash of its definition and its
        function's body). Drift compared a column's name and a type string
        without its numbers (audit 2026-10-01, SC3)."""
        from .. import catalog as cat
        whole = self._shapes(side, db, fresh=True)["catalog"]
        keep = set(self.neutral_tables(side, db))
        return cat.Catalog(
            tables=[t for t in whole.tables if t in keep],
            columns={t: c for t, c in whole.columns.items() if t in keep},
            keys={t: k for t, k in whole.keys.items() if t in keep},
            indexes={t: i for t, i in whole.indexes.items() if t in keep},
            fks={t: f for t, f in whole.fks.items() if t in keep},
            others={t: o for t, o in whole.others.items() if t in keep})

    def neutral_columns(self, side, db, table):
        """[(name, declared type)] with the type's own numbers attached.

        `format_type` rather than `information_schema.data_type`: the latter
        answers `character varying` and keeps the 50 in a separate column, so
        a target built from it came out `varchar(1024)` - wider than the
        source, which loses a limit the application was relying on without
        losing a row to show for it.
        """
        got, name = self._shape_of(side, db, table)
        if name is None:
            return []
        return [(c.name, c.declared)
                for c in got["catalog"].columns.get(name, [])]

    def neutral_key(self, side, db, table, ask=None):
        """The primary key's columns, in its order - the one order every
        reader of a key takes (audit 2026-10-01, DU19). `ask(sql)` as
        `_slot_ready` takes it: asked on that connection, not the
        catalogue."""
        sch, tbl = self._split(table)
        if ask is None:
            got, name = self._shape_of(side, db, table)
            return list(got["catalog"].keys.get(name) or ()) if name else []
        out = ask(f"""
            select a.attname
              from pg_index i
              join pg_class c on c.oid = i.indrelid
              join pg_namespace n on n.oid = c.relnamespace
              join pg_attribute a on a.attrelid = c.oid
                                 and a.attnum = any(i.indkey)
             where i.indisprimary and n.nspname = {_literal(sch)}
               and c.relname = {_literal(tbl)}
             order by array_position(i.indkey, a.attnum)""")
        return [l for l in out.splitlines() if l]

    def _conn(self, side, db):
        """A connection of the driver the rows are read through, to the
        database `db` as it is named on that side (not mapped: other
        modules hand the target's own name). Inside the engine, `_connect`
        takes the hop's name."""
        return self._open(side, db, "read")

    def _conn3(self, side, db):
        """A connection of the driver that sends statements without
        waiting for each one's answer and COPY binary - the one rows are
        written through - to the database `db` as it is named on that
        side. Each value is put into its statement here as a literal, as
        the other driver puts it (its type cast after it where the other
        driver casts), so a column reads what it always read, and a
        statement holds as many values as it has, where values sent apart
        stop at 65,535; nothing is prepared on a server a pooler may hand
        to another client."""
        return self._open(side, db, "write")

    @staticmethod
    def _read_float4(conn):
        """A `real`, and each element of a `real[]`, read as the 32-bit
        float it is. The driver parses the text the server prints - nine
        digits under `extra_float_digits=3`, `0.100000001` - as a double,
        another number from the float the column holds, so an array of
        them moved into a `Float64` array would hold values the source
        never had. (A plain `real` column is read through `::float8`
        already, `canon.read_expr`.)"""
        import struct

        from psycopg2.extensions import new_array_type, new_type, \
            register_type

        def f32(text, cur):
            if text is None:
                return None
            return struct.unpack("<f", struct.pack("<f", float(text)))[0]
        one = new_type((700,), "MIGKIT_FLOAT4", f32)
        register_type(one, conn)
        register_type(new_array_type((1021,), "MIGKIT_FLOAT4_ARRAY", one),
                      conn)

    @staticmethod
    def _read_json(conn):
        """A `json` or `jsonb` value read with its numbers as the decimals
        they are. The driver's own reading makes each a float: measured,
        `{"n": 123456789.123456789}` arrived as 123456789.12345679 and
        `1.10` as 1.1, and a move wrote those - a changed document, which
        the digest then found on the target and not on the way."""
        import json
        from decimal import Decimal

        from psycopg2.extras import register_default_json, \
            register_default_jsonb

        def loads(text):
            return json.loads(text, parse_float=Decimal)
        register_default_json(conn, loads=loads)
        register_default_jsonb(conn, loads=loads)

    #: a database name no server has, for asking a pooler who it is
    NO_SUCH_DATABASE = "migkit_no_such_database"

    def pooler(self, side):
        """PgBouncer, known by its own console, and by its own words.

        Its console is the database `pgbouncer`: an administrator of it is
        let in and told how the hop's database is pooled. Anyone else is
        refused there, as a wrong password is - so a database no server has
        is asked for next, which PgBouncer refuses as `no such database`
        and PostgreSQL as `database ... does not exist`; a user PgBouncer
        signs in through the server is refused there as a wrong password,
        where PostgreSQL would have signed them in first. Measured on
        PgBouncer 1.25 and PostgreSQL 16. A pooler that passes every
        database name on to the server is not told apart from it, and is
        not claimed to be."""
        got = super().pooler(side)
        if got:
            return got
        import psycopg2

        def ask(dbname):
            try:
                # a pooler's console takes no session settings: the bare
                # connection, with the side's TLS and a short wait
                conn = connect(self._ep(side), dbname, retry=False,
                               timeout=PROBE_SECONDS, session=False)
            except psycopg2.OperationalError as e:
                return None, str(e)
            return conn, ""
        conn, said = ask("pgbouncer")
        if conn is None:
            if "does not exist" in said:
                return None
            _, said = ask(self.NO_SUCH_DATABASE)
            if "no such database" in said:
                return {"name": "PgBouncer", "mode": None, "version": None}
            if "authentication failed" not in said:
                return None
            # PostgreSQL signs a user in before it looks for the database;
            # PgBouncer signs a user outside its own list in per database,
            # and cannot for one it does not serve (measured: `SASL
            # authentication failed` there, for a user the hop's database
            # let in)
            own, _ = ask(self._d(side, (self.databases() or ["postgres"])[0]))
            if own is None:
                return None
            own.close()
            return {"name": "a connection pooler", "mode": None,
                    "version": None}
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("show version")
            version = str(cur.fetchone()[0]).replace("PgBouncer", "").strip()
            cur.execute("show databases")
            names = [c[0] for c in cur.description]
            wanted = {self._d(side, d) for d in self.databases()}
            modes = {r[names.index("pool_mode")] for r in cur.fetchall()
                     if r[names.index("name")] in wanted}
            modes.discard(None)
            modes.discard("")
            if not modes:
                cur.execute("show config")
                modes = {r[1] for r in cur.fetchall() if r[0] == "pool_mode"}
        finally:
            conn.close()
        return {"name": "PgBouncer", "version": version or None,
                "mode": "/".join(sorted(modes)) or None}

    def _read_hstore(self, conn, side, db):
        """A key/value set read as the mapping it is, where the database
        has the extension - it came back as its text (`"a"=>"1"`), and
        written into another engine's JSON column that is not JSON at all.
        Asked once per database."""
        known = self.__dict__.setdefault("_hstore", {})
        ident = (side, db)
        if known.get(ident) is False:
            return
        from psycopg2.extras import register_hstore
        try:
            register_hstore(conn)
            known[ident] = True
        except Exception:  # noqa: BLE001 - no extension here is the answer
            conn.rollback()
            known[ident] = False

    #: how rows are written into the target's tables - by a load, a repair
    #: and a rollback alike (`load_window`) - from what the target's user
    #: may do, as the load reads it (`movers._pg_quiet_triggers`)
    WRITE_WAYS = (
        Rung("as a replica", "the target's triggers and foreign keys held"
                             " quiet as a replica's are",
             needs=(Need("the replica role",
                         lambda f: f["quiet"] in ("flag", "session"),
                         "the target's user may not write as a replica"),)),
        Rung("as they are", "the target's tables written as they are",
             needs=(Need("no trigger and no foreign key on the tables",
                         lambda f: f["quiet"] == "none",
                         lambda f: f["write refused"] or "the tables have"
                         " triggers or foreign keys"),)))

    def load_window(self, db, log=None, tables=None):
        """The target's connections as a replica while rows are written
        into it, which a trigger does not fire for - the way the bulk paths
        load, and the way the server's own subscriptions apply.

        Measured before, writing rows and changes from another engine: a
        `BEFORE INSERT OR UPDATE` trigger appending to the value turned `b`
        into `b!!` and wrote four audit rows the source never had. Decided
        once, before anything is written, and refused where the user may
        not and the tables have triggers (`WRITE_WAYS`) - for the load, a
        repair and a rollback's rows put back alike.
        """
        import contextlib

        from .. import decide, movers
        on = self.__dict__.setdefault("_as_replica", set())
        name = self._d("dst", db)
        if name in on:
            return contextlib.nullcontext()

        def quiet():
            try:
                return movers._pg_quiet_triggers(self.hop, db, tables)
            except SystemExit as e:
                facts["write refused"] = str(e)
                return "refused"
        facts = decide.Facts({"write refused": ""}, quiet=quiet)
        way = decide.climb(f"{db}: rows written into the target",
                           self.WRITE_WAYS, facts)
        if way.rung is None:
            raise SystemExit(facts["write refused"] or way.reason)
        if way.path == "as they are":
            return contextlib.nullcontext()

        @contextlib.contextmanager
        def window():
            on.add(name)
            try:
                yield
            finally:
                on.discard(name)
        return window()

    #: every sequence that owns a column - a serial's, an identity's - as
    #: sequence, table and column, each already quoted
    OWNED_SEQUENCES_SQL = """
        select format('%I.%I', sn.nspname, s.relname) || chr(31)
               || format('%I.%I', tn.nspname, t.relname) || chr(31)
               || quote_ident(a.attname) || chr(31) || tn.nspname
               || '.' || t.relname
          from pg_depend d
          join pg_class s on s.oid = d.objid and s.relkind = 'S'
          join pg_namespace sn on sn.oid = s.relnamespace
          join pg_sequence q on q.seqrelid = s.oid and q.seqincrement > 0
          join pg_class t on t.oid = d.refobjid
          join pg_namespace tn on tn.oid = t.relnamespace
          join pg_attribute a on a.attrelid = t.oid
                             and a.attnum = d.refobjsubid
         where d.classid = 'pg_class'::regclass
           and d.refclassid = 'pg_class'::regclass
           and d.deptype in ('a', 'i')"""

    def settle_target(self, db, from_source=True):
        """The target's sequences past its rows, and its tables analysed.

        The table copier writes each row with the source's key, and a
        sequence does not move for a key it did not hand out. Measured,
        through PostgreSQL's own copier and the copier between engines: the
        first insert after the copy failed on `duplicate key value violates
        unique constraint`. The bulk paths already carried the source's
        sequences; now every path does:
        * the source's values, where the source is PostgreSQL, never below
          a row the target holds
        * then, from any engine, each sequence that owns a column is raised
          to that column's largest value - only raised, never lowered
        Then the statistics (`_analyze_in_stages`). After a copy from a
        PostgreSQL source, the rows it wrote whose parent the hop does not
        take are counted and said (`_orphans_left`).
        """
        said = []
        if from_source:
            from .. import movers
            movers._pg_carry_sequences(self.hop, db)
            said += self._orphans_left(db)
        behind = self.sequences_behind(db)
        if behind:
            self._psql("dst", db, "\n".join(self._raise_past(behind)))
            said.append(f"{len(behind)} sequences raised past the rows the"
                        " copy wrote")
        said.append(self._analyze_in_stages(db))
        return "; ".join(said)

    def sequences_behind(self, db):
        """[(sequence, table, column, the column's largest value)] for each
        sequence on the target that owns a column and would hand out a
        value that column already holds. Asked of the target alone, so it
        answers whatever engine the rows came from."""
        out = []
        for line in self._psql("dst", db,
                               self.OWNED_SEQUENCES_SQL).splitlines():
            seq, table, col, name = (line.split("\x1f") + ["", "", ""])[:4]
            if not name or not self.in_scope(db, name):
                continue
            top = self._psql("dst", db,
                f"select m from (select max({col}) m from {table}) x, {seq} s"
                " where m is not null and m >= case when s.is_called"
                " then s.last_value else s.last_value - 1 end").strip()
            if top:
                out.append((seq, table, col, top))
        return out

    @staticmethod
    def _raise_past(behind):
        """The statements that move each sequence to its column's largest
        value, so the next one handed out is past every row."""
        return [f"select setval({_literal(seq)}, {top});"
                for seq, _, _, top in behind]

    def _foreign_keys(self, db):
        """Every foreign key on the target: [name, the table it is on, the
        table it points at, their columns, whether it is validated]."""
        return [l.split("|") for l in self._psql("dst", db, """
            select c.conname
              ||'|'||c.conrelid::regclass||'|'||c.confrelid::regclass
              ||'|'||(select string_agg(quote_ident(a.attname), ','
                                        order by x.ord)
                      from unnest(c.conkey) with ordinality x(attnum, ord)
                      join pg_attribute a on a.attrelid = c.conrelid
                       and a.attnum = x.attnum)
              ||'|'||(select string_agg(quote_ident(a.attname), ','
                                        order by x.ord)
                      from unnest(c.confkey) with ordinality x(attnum, ord)
                      join pg_attribute a on a.attrelid = c.confrelid
                       and a.attnum = x.attnum)
              ||'|'||c.convalidated::text
            from pg_constraint c
            join pg_namespace n on n.oid = c.connamespace
            where c.contype = 'f'
              and n.nspname not like '\\_\\_%'""").splitlines() if l]

    def _pointing_at_nothing(self, db, child, parent, ckeys, pkeys):
        """How many rows of `child` on the target point at no row of
        `parent`, counted within `fk_scan_seconds` (default 300);
        RuntimeError where it ran out or failed."""
        budget = int(self.hop.options.get("fk_scan_seconds", 300)) * 1000
        cc = ", ".join(f"c.{k}" for k in ckeys.split(","))
        pc = ", ".join(f"p.{k}" for k in pkeys.split(","))
        return int(self._psql("dst", db,
                              f"select count(*) from {child} c"
                              f" where ({cc}) is not null and not exists"
                              f" (select 1 from {parent} p"
                              f" where ({pc}) = ({cc}))",
                              statement_timeout=budget) or 0)

    def _fk_orphans(self, db):
        """Rows on the target whose foreign key points at nothing.

        A validated foreign key was taken to mean there could be none. But
        every load migkit makes writes as a replica, or with the triggers
        off, and a foreign key is a trigger: it did not look. Measured, a
        child row whose parent the hop's row filter left out landed behind
        a validated key, and the check read "all fk constraints validated,
        no orphans possible". So every foreign key is scanned, each within
        `fk_scan_seconds` (default 300); one that runs out is said, not
        passed.

        A child pointing into a table the hop does not take is a warn that
        names that table: what the target holds of it is not the move's to
        answer for, and it was a difference on every run. A parent the hop
        takes - its row filter cutting the rows a child points at included
        - is still a difference."""
        fks = self._foreign_keys(db)
        orphans, outside, unscanned, pending = [], [], [], 0
        for name, child, parent, ckeys, pkeys, valid in fks:
            if not self.in_scope(db, child):
                continue
            pending += valid != "true"
            try:
                n = self._pointing_at_nothing(db, child, parent, ckeys,
                                              pkeys)
            except RuntimeError as e:
                unscanned.append(f"{child}.{name}"
                                 f" ({str(e).splitlines()[-1][:60]})")
                continue
            if not n:
                continue
            if not self.in_scope(db, parent):
                outside.append(f"{child}.{name}: {n} rows point at {parent},"
                               " which this hop does not move")
                continue
            orphans.append(f"{child}.{name}: {n} orphan rows"
                           + (" behind a validated key" if valid == "true"
                              else ""))
        aside = (f"; {len(outside)} into tables the hop does not move: "
                 + "; ".join(outside[:3]) if outside else "")
        if orphans:
            return Result("deep", f"{db} fk", "diff",
                          "; ".join(orphans[:5])
                          + (f"; {len(unscanned)} not scanned in time"
                             if unscanned else "") + aside, "",
                          "put the parents in place or delete the orphans,"
                          " then validate the constraints on the target")
        if unscanned:
            return Result("deep", f"{db} fk", "warn",
                          f"{len(unscanned)} foreign keys not scanned within"
                          " fk_scan_seconds, so unknown rather than clean: "
                          + "; ".join(unscanned[:3]) + aside)
        if outside:
            return Result("deep", f"{db} fk", "warn",
                          "; ".join(outside[:5]), "",
                          "the parent is not moved by this hop: take it"
                          " too, or the rows keep pointing at nothing")
        return Result("deep", f"{db} fk", "ok",
                      f"{len(fks)} foreign keys scanned, 0 orphan rows"
                      + (f"; {pending} are NOT VALID - validate them before"
                         " cutover" if pending else ""))

    def _orphans_left(self, db):
        """The rows a load just wrote whose parent the hop does not take,
        counted key by key, said and recorded in the hop's change log (and
        so in its evidence) - never a failure.

        A load writes with the keys off (as a replica, or with the triggers
        off), so a child arrives whether its parent does or not: a parent
        table the hop leaves out, or the rows of one its row filter leaves
        behind. MySQL's move says the rows of a table it leaves out that
        point at a parent it replaced (`movers._my_orphans_left`);
        PostgreSQL's said nothing at all, and the check was the first to
        hear of it. Nothing is read for a hop that takes everything whole.
        Returns the lines said."""
        from .. import audit
        from ..config import scoped
        if not (scoped(self.hop) or (self.hop.mapping or {}).get("where")):
            return []
        try:
            fks = self._foreign_keys(db)
        except RuntimeError as e:
            return ["the rows pointing at parents the move did not carry"
                    f" could not be counted: {str(e).splitlines()[-1][:90]}"]
        found, said = [], []
        for name, child, parent, ckeys, pkeys, _ in fks:
            if not self.in_scope(db, child):
                continue
            where = self._split(parent)
            if self.hop.excluded(db, *where):
                why = "the hop does not take that table"
            elif self.hop.row_filter(db, *where):
                why = "the hop's row filter leaves those rows behind"
            else:
                continue
            try:
                n = self._pointing_at_nothing(db, child, parent, ckeys,
                                              pkeys)
            except RuntimeError as e:
                said.append(f"{child}: the rows pointing at {parent} could"
                            f" not be counted: {str(e).splitlines()[-1][:60]}")
                continue
            if n:
                # no field named for a key: the evidence leaves out what
                # reads as a secret's name (`redact.SECRET_KEY`)
                found.append({"table": child, "constraint": name,
                              "parent": parent, "rows": n, "why": why})
                rows = "row points" if n == 1 else "rows point"
                said.append(f"{child}: {n:,} {rows} at {parent} rows that"
                            f" are not there - {why}; the check names them")
        if found:
            audit.append(self.hop.report_dir() / "changelog.jsonl",
                         {"op": "orphans-left", "db": db, "found": found})
        return said

    def create_missing(self, db, log=None):
        """The bulk paths' own: the tables from the source's definition,
        their keys and constraints once the rows are in."""
        from .. import movers
        movers._pg_create_missing(self.hop, db, log)

    def finish_created(self, db, log=None):
        from .. import movers
        movers._pg_finish_created(self.hop, db, log)

    def position_spans(self, side, db, table, rows_per):
        """Pages of the heap, as `ctid` ranges: a range of pages is read
        by a scan of just those pages (PostgreSQL 14 and later), and the
        last span is open-ended so rows stored past the plan are read too.
        None before 14, where each span would scan the whole table."""
        if (self._server_version(side, db) or 0) < 140000:
            return None
        sch, tbl = self._split(table)
        got = self._psql(side, db, "select c.relpages, c.reltuples::bigint"
                                   " from pg_class c join pg_namespace n on"
                                   " n.oid = c.relnamespace where"
                                   f" n.nspname = {_literal(sch)} and c.relname ="
                                   f" {_literal(tbl)}").strip()
        if not got:
            return None
        pages, rows = (int(x) for x in got.split("|"))
        if pages > 0 and rows <= 0:
            # never analysed: counted, once
            rows = int(self._psql(side, db, f'select count(*) from'
                                            f' {_ident(sch)}.{_ident(tbl)}').strip())
        if pages <= 0 or rows <= 0:
            return [[0, None]]
        step = max(1, int(pages * rows_per / rows))
        out, start = [], 0
        while start < pages:
            end = start + step
            out.append([start, f"ctid >= {_tid(start)} and ctid <"
                               f" {_tid(end)}" if end < pages else
                        f"ctid >= {_tid(start)}"])
            start = end
        return out

    def replica_env(self, db):
        """The environment a target client needs to write as the window's
        connections do."""
        if self._d("dst", db) in self.__dict__.get("_as_replica", ()):
            return {"PGOPTIONS": "-c session_replication_role=replica"}
        return {}

    def _read_query(self, side, db, table, columns, after, limit, where,
                    as_text=()):
        """(sql, args, key, names) for `neutral_read` and
        `neutral_batches`: one statement shape for both. A column named in
        `as_text` is read as the server's text of it."""
        from .. import canon
        sch, tbl = self._split(table)
        names = [n for n, _ in columns]
        cols = ", ".join(f"{_ident(n)}::text" if n in as_text else
                         canon.read_expr("postgres", _ident(n), c)
                         for n, c in columns)
        key = self.neutral_key(side, db, table)
        resume, args = "", []
        if key and after is not None:
            places = ", ".join(["%s"] * len(key))
            keys = ", ".join(_ident(k) for k in key)
            resume = f"({keys}) > ({places})"
            # a `jsonb` key is read back as the document it holds, which
            # the driver will not send: as its JSON
            args = [canon.sql_value(v) for v in after]
        # the driver formats every statement it is given parameters for
        where = self._where(where, resume, True, percent=True)
        order = (" order by " + ", ".join(_ident(k) for k in key)) if key \
            else ""
        cap = f" limit {int(limit)}" if key and limit else ""
        return (f"select {cols} from {_ident(sch)}.{_ident(tbl)}{where}"
                f"{order}{cap}", args, key, names)

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None, as_text=()):
        """The whole table in one pass, `size` rows at a time, for a table
        with no key to resume from - read through a cursor the server keeps,
        so no more than a batch is ever held here."""
        sql, args, _, _ = self._read_query(side, db, table, columns, None,
                                           None, where, as_text)
        with self._connect(side, db) as conn:
            with conn.cursor(name="migkit_batches") as cur:
                cur.itersize = size
                cur.execute(sql, args)
                while True:
                    rows = cur.fetchmany(size)
                    if not rows:
                        break
                    yield [list(r) for r in rows]

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        sql, args, key, names = self._read_query(side, db, table, columns,
                                                 after, limit, where)
        with self._connect(side, db) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, args)
                rows = [list(r) for r in cur.fetchall()]
        if not rows or not key:
            return (rows, None)
        idx = [names.index(k) for k in key if k in names]
        if len(idx) != len(key):
            # the key is not among the columns being moved, so there is
            # nothing to resume from - one pass, and say so by returning None
            return (rows, None)
        return (rows, tuple(rows[-1][i] for i in idx))

    def column_facts(self, side, db, table):
        """Whether each column takes NULL. A table migkit builds here takes
        NULL outside its key."""
        if table is None:
            return {None: {"null": True}}
        got, name = self._shape_of(side, db, table)
        return {c.name: {"null": c.nullable}
                for c in (got["catalog"].columns.get(name) or [])}

    def key_merges(self, side, db, columns, batches, examples=3):
        """Asked of this server's own comparison where it may take two
        strings for one: under a deterministic collation - libc's, and
        ICU's by default - two strings are equal only where their bytes
        are, so nothing merges and nothing is asked. A nondeterministic
        ICU collation (`deterministic = false`: case- or accent-blind)
        takes the keys into a temporary table whose columns compare as
        the target's, and the server groups them, as MySQL's is asked."""
        names = [co for _, co in columns if co]
        if not names:
            return None
        marks = ", ".join(["%s"] * len(names))
        with self._connect(side, db) as conn:
            with conn.cursor() as cur:
                cur.execute("select collname from pg_collation where not"
                            f" collisdeterministic and collname in ({marks})",
                            names)
                folding = {str(r[0]) for r in cur.fetchall()}
                if not folding:
                    return None
                q = self._quote_ident
                defs = ", ".join(
                    f"{q(n)} text" + (f" collate {q(co)}" if co in folding
                                      else "") for n, co in columns)
                cols = ", ".join(q(n) for n, _ in columns)
                cur.execute(f"create temporary table migkit_key_merges"
                            f" ({defs}) on commit drop")
                insert = ("insert into migkit_key_merges values ("
                          + ", ".join(["%s"] * len(columns)) + ")")
                for rows in batches:
                    if rows:
                        cur.executemany(insert, [tuple(
                            None if v is None else str(v) for v in r)
                            for r in rows])
                cur.execute(f"select count(*) from (select 1 from"
                            f" migkit_key_merges group by {cols} having"
                            " count(*) > 1) g")
                groups = int(cur.fetchone()[0])
                shown = []
                if groups:
                    cur.execute(f"select {cols} from migkit_key_merges"
                                f" group by {cols} having count(*) > 1"
                                f" limit {int(examples)}")
                    for rep in cur.fetchall():
                        # every row, not `distinct`: that compares by the
                        # collation too, and gives back one of the group
                        cur.execute(f"select {cols} from migkit_key_merges"
                                    " where " + " and ".join(
                                        f"{q(n)} = %s" for n, _ in columns),
                                    rep)
                        shown.append([tuple(r) for r in cur.fetchall()])
            conn.rollback()
        return groups, shown

    def decimal_extent(self, side, db, table, column, where=None):
        """The server's own `scale()` and the length of the whole part,
        in one scan rather than every value read here. Through `numeric`,
        which a `money` is not."""
        q = self._quote_ident(column)
        c = f"{q}::numeric"
        got = self.run_rule(
            side, db,
            f"select coalesce(max(scale({c})), 0), coalesce(max(case when"
            f" abs({c}) >= 1 then length(trunc(abs({c}))::text) else 0"
            f" end), 0) from {self._qualified(side, db, table)} where"
            f" {q}::text not in ('NaN', 'Infinity', '-Infinity')"
            + (f" and ({where})" if where else ""))
        frac, whole = got[0]
        return int(whole), int(frac)

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        if not key or not keys:
            return {}
        sch, tbl = self._split(table)
        sql, args = self._by_key_query(
            f'{_ident(sch)}.{_ident(tbl)}', columns, key, list(keys), lambda n: f'{_ident(n)}',
            "%s", where.replace("%", "%%") if where else None, "postgres")
        with self._connect(side, db) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, args)
                rows = [list(r) for r in cur.fetchall()]
        return self._by_key_map(columns, key, rows)

    def neutral_rows_matching(self, side, db, table, columns, match, values):
        """Every row whose `match` columns hold one of `values`, asked of
        the server by the select a read by key makes, a thousand values a
        statement."""
        if not match or not values:
            return []
        sch, tbl = self._split(table)
        values, out = list(values), []
        with self._connect(side, db) as conn:
            with conn.cursor() as cur:
                for i in range(0, len(values), 1000):
                    sql, args = self._by_key_query(
                        f'{_ident(sch)}.{_ident(tbl)}', columns, list(match),
                        values[i:i + 1000], lambda n: f'{_ident(n)}', "%s", None,
                        "postgres")
                    cur.execute(sql, args)
                    out += [list(r) for r in cur.fetchall()]
        return out

    def neutral_write(self, side, db, table, columns, rows):
        """Rows in through COPY, and over what the table already holds by
        its key: into a temporary table first, then one statement that
        inserts or replaces. A batch written again replaces itself.

        COPY takes each value by its Python type - bytes as `bytea`, an
        empty string as one, `None` as NULL - where a CSV the copier wrote
        itself had turned bytes into their hex digits as text and every
        empty string into NULL (measured, MySQL to PostgreSQL: 200,000 of
        200,000 rows differed)."""
        if not rows:
            return 0
        from .. import canon
        self._target_only(side, "write rows")
        sch, tbl = self._split(table)
        quoted = f'{_ident(sch)}.{_ident(tbl)}'
        names = [n for n, _ in columns]
        cols = ", ".join(f'{_ident(n)}' for n in names)
        key = self.neutral_key(side, db, table)
        keyed = bool(key) and all(k in names for k in key)
        with self._connect(side, db, "write") as conn:
            with conn.cursor() as cur:
                into = quoted
                if keyed:
                    # the carried columns only: a column the batch does
                    # not carry keeps its own default on the table
                    cur.execute("create temporary table migkit_batch on"
                                f" commit drop as select {cols} from"
                                f" {quoted} with no data")
                    into = "migkit_batch"
                arrays = self._array_columns(side, db, table)
                sparse = self._sparse_columns(side, db, table)
                with cur.copy(f"copy {into} ({cols}) from stdin") as copy:
                    for r in canon.sql_rows(
                            [canon.SPARSE if n in sparse else c
                             for n, c in columns], rows,
                            {i for i, n in enumerate(names) if n in arrays},
                            "postgres"):
                        copy.write_row(r)
                if keyed:
                    sets = ", ".join(f'{_ident(n)} = excluded.{_ident(n)}'
                                     for n in names if n not in key)
                    conflict = ", ".join(f'{_ident(k)}' for k in key)
                    cur.execute(
                        f"insert into {quoted} ({cols}) select {cols} from"
                        f" migkit_batch on conflict ({conflict}) do "
                        + (f"update set {sets}" if sets else "nothing"))
                # last before the commit: a writer that lost its hold is
                # refused here (`fencing`)
                fencing.hold(self, side, db, cur)
        return len(rows)

    def _sparse_columns(self, side, db, table):
        """The `sparsevec` columns of a table (`catalog`): a vector is
        read dense (`canon.read_expr`) and a `sparsevec` takes only its
        own text, `{1:0.5,4:2}/6`."""
        return {n for n, t in self.neutral_columns(side, db, table)
                if str(t).lower().startswith("sparsevec")}

    def _array_columns(self, side, db, table):
        """The columns of a table that are arrays (`catalog`): a list on
        its way into one is the array, where into any other column
        (`jsonb`) it is the JSON array `sql_value` writes. An array is
        read as its list, to be carried to another engine as JSON
        (`canon.array_element`), and written back here as a list."""
        from .. import canon
        return {n for n, t in self.neutral_columns(side, db, table)
                if canon.array_element("postgres", t) is not None}

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        sch, tbl = self._split(table)
        with self._connect(side, db) as conn:
            with conn.cursor() as cur:
                cur.execute(f'delete from {_ident(sch)}.{_ident(tbl)}'
                            + self._where(where))
                gone = cur.rowcount
                fencing.hold(self, side, db, cur)
            conn.commit()
        return gone

    SQL_DIALECT = "postgres"

    def neutral_column_rules(self, side, db, table):
        """{column: {null, default, identity}} of a table's columns the
        server does not compute (`catalog`)."""
        got, name = self._shape_of(side, db, table)
        out = {}
        for c in (got["catalog"].columns.get(name) or []) if name else []:
            if got["generated"][name].get(c.name):
                continue
            default = c.default or ""
            # a serial's default is its own sequence, which is what an
            # identity is on the engines that have one
            serial = default.startswith("nextval(")
            out[c.name] = {"null": c.nullable,
                           "default": None if serial or not default
                           else default,
                           "identity": bool(got["identity"][name].get(
                               c.name)) or serial}
        return out

    def neutral_indexes(self, side, db, table):
        """[(name, unique, [columns], plain)] of a table's indexes but its
        key (`catalog`); `plain` where the columns say it all - no
        expression, no predicate."""
        got, name = self._shape_of(side, db, table)
        return [(i.name, i.unique, list(i.columns), not i.definition)
                for i in (got["catalog"].indexes.get(name) or [])]

    def execute_ddl(self, side, db, sql):
        self._target_only(side, "change a schema")
        self._psql(side, db, sql)
        self._forget_catalog(side, db)

    def neutral_create_index_sql(self, side, db, table, name, unique,
                                 columns):
        sch, tbl = self._split(table)
        return (f'create {"unique " if unique else ""}index {_ident(name)}'
                f' on {_ident(sch)}.{_ident(tbl)} ('
                + ", ".join(f'{_ident(c)}' for c in columns) + ")")

    def default_works(self, side, db, expr, typ=None):
        try:
            self._psql(side, db, f"select ({expr})"
                                 + (f"::{typ}" if typ else ""))
            return True
        except RuntimeError:
            return False

    def neutral_create_sql(self, side, db, table, columns, key=()):
        from .. import canon
        sch, tbl = self._split(table)
        # the type a key column takes, as the target validated it: a
        # MongoDB `_id` of mixed types is `jsonb` in a key and was built
        # `json`, which no primary key takes (audit 2026-10-01, SC6)
        defs = [_ident(col[0]) + " "
                + canon.ddl_type("postgres", col[1], col[2], col[0] in key)
                + self._column_tail(col[3] if len(col) > 3 else None,
                                    "postgres")
                for col in columns]
        if key:
            defs.append("primary key (" + ", ".join(_ident(k) for k in key)
                        + ")")
        return (f"create table {_ident(sch)}.{_ident(tbl)} ("
                + ", ".join(defs) + ")")

    def neutral_create(self, side, db, table, columns, key=()):
        sch, tbl = self._split(table)
        exists = self._psql(side, db,
                            "select count(*) from information_schema.tables"
                            f" where table_schema={_literal(sch)}"
                            f" and table_name={_literal(tbl)}").strip()
        if exists != "0":
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        ddl = self.neutral_create_sql(side, db, table, columns, key)
        self._psql(side, db, ddl)
        self._forget_catalog(side, db)
        return ddl

    def _insert_override(self, side, db, table, names):
        """`GENERATED ALWAYS AS IDENTITY` refuses a value the client chose,
        and a repair has no choice but to choose one - the row it is fixing
        already has a key.

        Measured on PostgreSQL 16 before this existed: migkit's own repair
        statement against such a target answered `cannot insert a
        non-DEFAULT value into column "id"`, and the same statement with
        this clause succeeded.

        Only the `a` (always) form needs it; `d` (by default), which is what
        `serial` behaves like, takes the value as given. Emitting the clause
        unconditionally was measured to be harmless - PostgreSQL accepts it
        on a table with no identity column at all, and a `by default`
        column still stored the explicit value - but it is looked up
        instead, so the statement says what it means and a reader can tell
        which tables needed it. One catalog query per table, cached.

        **`move` is not affected**, which is what keeps this narrow: `COPY`
        into a generated-always column was measured to succeed with no
        clause at all.
        """
        return (" overriding system value"
                if self._write_rules(side, db, table)["always"] & set(names)
                else "")

    def _write_rules(self, side, db, table):
        """Which columns of this table the server insists on owning.

        Two kinds, and they are **not** interchangeable - measured:

            attidentity = 'a'   GENERATED ALWAYS AS IDENTITY
                                writable with OVERRIDING SYSTEM VALUE
            attgenerated <> ''  GENERATED ALWAYS AS (expr) STORED
                                not writable at all; OVERRIDING SYSTEM VALUE
                                was tried and answered the same error

        Both from the side's catalogue (`catalog`), read once for every
        table.
        """
        got, name = self._shape_of(side, db, table)
        cols = got["catalog"].columns.get(name) or [] if name else []
        return {"always": {c.name for c in cols
                           if got["identity"][name].get(c.name) == "a"},
                "generated": {c.name for c in cols
                              if got["generated"][name].get(c.name)}}

    def _unwritable_columns(self, side, db, table):
        """`GENERATED ALWAYS AS (expr) STORED` - the server computes these
        and refuses anybody else's value, so a repair has to leave them out
        and let it. Measured: an insert omitting `total` stored `total=20`
        from `price * qty`, which is the right answer arrived at the only
        way the server allows."""
        return self._write_rules(side, db, table)["generated"]

    #: Rows in one applied statement (`Engine._apply_each`).
    APPLY_ROWS = 1000

    def _keys_off(self, side, db, edges):
        """Whether the applier's sessions write without checking foreign
        keys: they run as a replica (`load_window`), where a key's check is
        a trigger that does not fire, and every parent a table in scope
        points at is in scope too - so a parent and its child need not
        share a lane or keep each other's order (R2.5). The deep check's
        orphan scan finds a row whose parent never came."""
        if self._d(side, db) not in self.__dict__.get("_as_replica", ()):
            return False
        return all(not self.hop.excluded(db, p) for c, p in edges
                   if not self.hop.excluded(db, c))

    def _ordered_tables(self, side, db):
        def read():
            edges = [tuple(line.split("\x1f")) for line in self._psql(
                side, db, "select a.relname || chr(31) || b.relname"
                              " from pg_constraint c"
                              " join pg_class a on a.oid = c.conrelid"
                              " join pg_class b on b.oid = c.confrelid"
                              " where c.contype = 'f'").splitlines()
                     if line]
            if self._keys_off(side, db, edges):
                edges = []
            singles = [line for line in self._psql(
                side, db, "select c.relname from pg_index i"
                              " join pg_class c on c.oid = i.indrelid"
                              " join pg_namespace n"
                              " on n.oid = c.relnamespace"
                              " where i.indisunique and n.nspname not in"
                              " ('pg_catalog', 'information_schema')"
                              " group by c.relname having count(*) > 1"
                              ).splitlines() if line]
            return self._table_groups(edges, singles)
        return self._ordered_cached(side, db, read)

    def _apply_upsert(self, side, db, table, key, values):
        self._apply_upserts(side, db, table, [(key, values)])

    def _apply_upserts(self, side, db, table, rows):
        from .. import canon
        table = self.local_table(table)
        sch, tbl = self._split(table)
        key = rows[0][0]
        full = [{**k, **v} for k, v in rows]
        names = [n for n in sorted(full[0])
                 if n not in self._unwritable_columns(side, db, table)]
        cols = ", ".join(f'{_ident(n)}' for n in names)
        sets = ", ".join(f'{_ident(n)} = excluded.{_ident(n)}'
                         for n in names if n not in key)
        conflict = ", ".join(f'{_ident(k)}' for k in sorted(key))
        tail = (f" on conflict ({conflict}) do update set {sets}" if sets
                else f" on conflict ({conflict}) do nothing")
        into = (f'insert into {_ident(sch)}.{_ident(tbl)} ({cols})'
                + self._insert_override(side, db, table, names))
        arrays = self._array_columns(side, db, table)
        values = [[r[n] if n in arrays else canon.sql_value(r[n])
                   for n in names] for r in full]
        with self._writer(side, db) as conn:
            key = None
            if len(values) >= self.STAGE_FROM:
                # staged or as statements, whichever did a row in less
                # time here: piped, the statements wait for nothing, and
                # on a long link that can outweigh the stage's own waits
                key = self._work_key("upserts", f"{sch}.{tbl}",
                                     len(values))
                began = time.monotonic()
                if self._cheaper(key, self.UPSERT_WAYS) == "staged":
                    if self._staged(conn, sch, tbl, names, values, into,
                                    tail):
                        self._answered()
                        self._took(key, "staged", time.monotonic() - began,
                                   len(values))
                        return
                    key = None
            began = time.monotonic()
            self._in_statements(conn, f"{into} values ", tail, values)
            if key:
                self._answered()
                self._took(key, "statements", time.monotonic() - began,
                           len(values))

    #: rows from which a run of upserts may go by COPY into a staging table
    #: and one `insert ... select ... on conflict` (`_staged`): measured,
    #: 5,000 rows in 24 ms against 51 ms as multi-row statements, 20,000 in
    #: 91 against 166 - and one round trip for the rows, not one per
    #: thousand. Past it, the way measured cheaper here is taken
    STAGE_FROM = 1000

    #: how a run of upserts that long is written (`_apply_upserts`)
    UPSERT_WAYS = (Rung("staged", "the rows copied into a temporary table"
                                  " and moved on in one statement",
                        footprint=1),
                   Rung("statements", "the rows as multi-row upserts, sent"
                                      " without waiting"))
    #: how a staged run is copied (`_staged`)
    STAGE_WAYS = (Rung("binary", "the rows copied as the server stores"
                                 " them"),
                  Rung("text", "the rows copied as text"))

    def _answered(self):
        """The batch's piped statements answered, so a way timed is timed
        to the end of its work and not to the end of its sending."""
        piped = getattr(self._apply_held(), "piped", None)
        if piped is not None:
            piped.off()

    def _piping(self):
        """Pipeline mode entered only when a statement is about to be
        sent: a batch that is one staged run pays no sync for it."""
        piped = getattr(self._apply_held(), "piped", None)
        if piped is not None:
            piped.on()

    def _in_statements(self, conn, head, tail, values):
        """`values` as rows of `head (...), (...) tail`, `APPLY_ROWS` a
        statement. Where the batch's statements are piped (`_apply_net`)
        none of them waits for its answer."""
        mark = "(" + ", ".join(["%s"] * len(values[0])) + ")"
        self._piping()
        with conn.cursor() as cur:
            for at in range(0, len(values), self.APPLY_ROWS):
                part = values[at:at + self.APPLY_ROWS]
                cur.execute(head + ", ".join([mark] * len(part)) + tail,
                            [v for r in part for v in r])

    def _staged(self, conn, sch, tbl, names, values, into, tail):
        """A run into the stage (`_stage`) by one COPY, then onto the
        table - with the stage emptied - by one `insert ... select`. The
        COPY is binary where every column and value is one `_copy_binary`
        sends exactly, else text (`_copy_text`); of the two, the one
        measured cheaper for this table and runs this long (`_cheaper`).
        False, with nothing sent, where neither carries every value
        exactly: the run then goes as statements.

        A batch's statements sent before it are answered first: COPY
        cannot run while statements are piped. Its own statements go
        after it in one round trip, as before, and those after them are
        piped again (`_piping`).

        Measured on postgres:16, 200,000 rows of eight columns (integer,
        numeric, timestamptz, bytea, jsonb, two texts, a boolean) in runs
        of 20,000: 2.26s binary, 2.36s as text and 3.21s as statements
        (before: 2.18s staged as text, 3.60s as statements each waited
        for); with a round trip of 10 ms 4.07s, 4.54s and 4.84s (before
        4.08s and 8.28s); with 50 ms, 60,000 rows, 2.67s, 2.70s and 2.44s
        (before 3.07s and 7.20s) - piped, the statements wait for nothing,
        and on a long link the stage's own round trips are what costs.
        Binary and text are close on these rows; each is timed per table
        and run size, and the cheaper taken (`_cheaper`), as the stage and
        the statements are. (The round trip was added by a proxy that
        holds each packet: `tc` and `pfctl` need root.)"""
        piped = getattr(self._apply_held(), "piped", None)
        if piped is not None:
            piped.off()
        cols = ", ".join(f'{_ident(n)}' for n in names)
        with conn.cursor() as cur:
            stage, types = self._stage(conn, cur, sch, tbl)
            key = self._work_key("stage", f"{sch}.{tbl}", len(values))
            began = time.monotonic()
            data, way, how = None, "text", ""
            if self._cheaper(key, self.STAGE_WAYS) == "binary":
                data = _copy_binary(conn, [types.get(n) for n in names],
                                    values)
                way, how = ("binary", " (format binary)") if data \
                    else ("text", "")
            if data is None:
                text = _copy_text(values)
                if text is None:
                    return False
                data = text.getvalue()
            with cur.copy(f"copy {stage} ({cols}) from stdin{how}") as cp:
                cp.write(data)
            self._took(key, way, time.monotonic() - began, len(values))
            # one round trip: the rows on, and the stage emptied
            cur.execute(f"{into} select {cols} from {stage}{tail};"
                        f" truncate {stage}")
        return True

    def _stage(self, conn, cur, sch, tbl):
        """A temporary table shaped as the target one, made the first time
        a session needs it, emptied after each use; with the type of each
        of its columns, which says what can go into it binary. Kept on the
        connection, so a stage is never taken for made on another."""
        import hashlib
        name = "migkit_stage_" + hashlib.md5(
            f"{sch}.{tbl}".encode(), usedforsecurity=False).hexdigest()[:12]
        made = conn.__dict__.setdefault("migkit_stages", {})
        if name not in made:
            cur.execute(f'create temp table if not exists {_ident(name)} (like'
                        f' {_ident(sch)}.{_ident(tbl)} including defaults);'
                        " select a.attname, t.typname from pg_attribute a"
                        " join pg_type t on t.oid = a.atttypid"
                        " where a.attrelid ="
                        f" {_literal('pg_temp.' + _ident(name))}::regclass"
                        " and a.attnum > 0 and not a.attisdropped"
                        " and t.typtype = 'b'")
            cur.nextset()
            made[name] = {str(n): str(t) for n, t in cur.fetchall()}
        return f'{_ident(name)}', made[name]

    def _added_row(self, side, db, table, key, values):
        from .. import canon
        sch, tbl = self._split(table)
        row = {**key, **values}
        names = [n for n in sorted(row)
                 if n not in self._unwritable_columns(side, db, table)]
        # an array column takes its list as the array, as an upsert writes
        # it (`_apply_upserts`); `sql_value` would make it JSON text, which
        # an array refuses - a two-way row the target kept a column of
        # goes this way, arrays and all
        arrays = self._array_columns(side, db, table)
        made = []
        for n in names:
            v = row[n].to if isinstance(row[n], canon.Added) else row[n]
            made.append(v if n in arrays else canon.sql_value(v))
        sets, by = [], []
        for n in names:
            if n in key or isinstance(row[n], canon.Kept):
                continue
            if isinstance(row[n], canon.Added):
                sets.append(f'{_ident(n)} = t0.{_ident(n)} + %s')
                by.append(canon.sql_value(row[n].by))
            else:
                sets.append(f'{_ident(n)} = excluded.{_ident(n)}')
        conflict = ", ".join(f'{_ident(k)}' for k in sorted(key))
        return (f'insert into {_ident(sch)}.{_ident(tbl)} as t0 ('
                + ", ".join(f'{_ident(n)}' for n in names) + ")"
                + self._insert_override(side, db, table, names)
                + " values (" + ", ".join(["%s"] * len(names)) + ")"
                + f" on conflict ({conflict}) do "
                + (f"update set {', '.join(sets)}" if sets else "nothing"),
                made + by)

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    def _apply_deletes(self, side, db, table, keys):
        from .. import canon
        table = self.local_table(table)
        sch, tbl = self._split(table)
        names = sorted(keys[0])
        cols = ", ".join(f'{_ident(n)}' for n in names)
        # a list of rows rather than `in (values ...)`: those values are
        # typed as text and do not compare with an integer key
        with self._writer(side, db) as conn:
            self._in_statements(conn, f'delete from {_ident(sch)}.{_ident(tbl)} where'
                                      f" ({cols}) in (", ")",
                                [[canon.sql_value(k[n]) for n in names]
                                 for k in keys])

    def _open_writer(self, side, db):
        self._target_only(side, "apply changes")
        return self._connect(side, db, "write")

    def _apply_net(self, side, db, rows):
        """The batch's runs piped (`_Piped`): each statement is sent
        without waiting for the one before it to be answered, and the
        answers are read once, where the batch ends - the first error
        among them fails the batch as a statement's own error did. On a
        link with a round trip of R, a batch of N statements took N x R
        and takes about R. Measured on postgres:16, a parent and its child
        in turn - a statement a change, as a key between them asks: 2,000
        in 0.46s before and 0.04s piped; 400 with a round trip of 10 ms,
        5.83s and 0.14s; with 50 ms, 24.50s and 0.44s."""
        held = self._apply_held()
        conn = getattr(held, "conn", None)
        if conn is None or getattr(held, "piped", None) is not None \
                or not callable(getattr(conn, "pipeline", None)):
            return super()._apply_net(side, db, rows)
        held.piped = piped = _Piped(conn)
        try:
            super()._apply_net(side, db, rows)
            piped.off()
        except BaseException as e:
            piped.off(e)
            raise
        finally:
            held.piped = None

    PLUGIN = "test_decoding"

    READS_ORIGIN_MARK = True
    SHAPE_CACHES = ("_column_rules_of", "_ordered_of", "_write_rules_cache",
                    "_arrays_of", "_sparse_of")

    def origin_mark(self, side, db):
        """The mark of the rung this side stands on (`marks`), written
        first in the transaction being applied: this thread's row of
        `migkit_origin`, a logical message, or the session taken under a
        replication origin of the hop's."""
        from .. import marks, pgslot, twoway
        rung = self.__dict__.get("_mark_rung") or "table"
        if rung == "origin":
            with self._writer(side, db) as conn:
                self._origin_session(conn, side, db, marks.batch_of(self))
            return
        if rung == "message":
            with self._writer(side, db) as conn:
                with conn.cursor() as cur:
                    cur.execute("select pg_logical_emit_message(true, %s,"
                                " %s)", (pgslot.MARK_PREFIX, self.hop.name))
            return
        self._origin_table(side, db)
        with self._writer(side, db) as conn:
            with conn.cursor() as cur:
                cur.execute(f"insert into public.{twoway.TABLE}"
                            " (origin, n, seen) values (%s, 1, %s)"
                            " on conflict (origin) do update set n ="
                            f" public.{twoway.TABLE}.n + 1, seen = coalesce("
                            f"excluded.seen, public.{twoway.TABLE}.seen)",
                            (self._origin_row(),
                             twoway.batch_seen(self)))

    def _origin_table(self, side, db):
        """`migkit_origin`, made once, on a connection of its own, where it
        is not there."""
        import threading

        from .. import twoway
        lock = self.__dict__.setdefault("_origin_lock", threading.Lock())
        made = self.__dict__.setdefault("_origin_made", set())
        with lock:
            if (side, self._d(side, db)) in made:
                return
            conn = self._connect(side, db)
            try:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("create table if not exists"
                                f" public.{twoway.TABLE} (origin text"
                                " primary key, n bigint not null"
                                " default 0, seen text)")
                    cur.execute(f"alter table public.{twoway.TABLE}"
                                " add column if not exists seen text")
            finally:
                conn.close()
            made.add((side, self._d(side, db)))

    #: how `shared` and `fencing` write migkit's own rows here
    OWN_DIALECT = "postgres"

    def own_name(self, side, db):
        """`migkit_origin`'s name in the database of `db` on `side`."""
        from .. import twoway
        return f"public.{twoway.TABLE}"

    def own_table(self, side, db):
        """(a connection of its own to the database of `db` on `side`,
        `migkit_origin`'s name there), the table made where it is not:
        where the machines of a hop keep their records and the numbers
        their writers are held to (`shared`, `fencing`)."""
        self._origin_table(side, db)
        return self._connect(side, db), self.own_name(side, db)

    #: the prefix of the replication origins two-way tails apply under: a
    #: reader leaves out what carries one, and stops where any other
    #: origin applies into the same server
    TWOWAY_ORIGIN = "migkit_twoway_"

    def twoway_origin(self):
        import re
        base = re.sub(r"[^a-z0-9_]", "_", str(self.hop.name).lower())
        return f"{self.TWOWAY_ORIGIN}{base}"[:200]

    def _origin_session(self, conn, side, db, batch):
        """The batch's connection taken under an origin of the hop's: the
        hop's own where batches are numbered - its progress is then the
        batch's number (`pg_replication_origin_xact_setup`), and as one
        session holds an origin at a time, one a connection just closed
        still holds is waited for - otherwise the first of the hop's
        origins no other lane holds, made where there is none yet.
        Measured on PostgreSQL 14 and 16: a second session asking for an
        origin another holds is refused (`already active for PID`)."""
        import time

        from .. import marks
        first = self.twoway_origin()
        names = [first] if batch is not None else [first] + [
            f"{first}_{k}" for k in range(1, max(int(self.hop.workers or 1),
                                                 1) + 1)]
        end = time.time() + 30
        with conn.cursor() as cur:
            while True:
                for name in names:
                    cur.execute("savepoint migkit_origin")
                    try:
                        cur.execute("select"
                                    " pg_replication_origin_session_setup(%s)",
                                    (name,))
                    except Exception as e:  # noqa: BLE001 - by its code
                        code = _sqlstate(e)
                        if code not in (UNDEFINED_OBJECT, OBJECT_IN_USE):
                            raise
                        cur.execute("rollback to savepoint migkit_origin")
                        if code == OBJECT_IN_USE:
                            continue
                        self._origin_named(side, db, name)
                        cur.execute("select"
                                    " pg_replication_origin_session_setup(%s)",
                                    (name,))
                    cur.execute("release savepoint migkit_origin")
                    if batch is not None:
                        cur.execute("select pg_replication_origin_xact_setup"
                                    "(%s::pg_lsn, now())",
                                    (marks.lsn(batch),))
                    return name
                if time.time() > end:
                    raise SystemExit(
                        f"two_way: the replication origin {first} stayed held"
                        " by another session for 30s - is another tail of"
                        " this hop applying?")
                time.sleep(0.05)

    def _origin_named(self, side, db, name):
        """The replication origin `name`, made where it is not there."""
        import psycopg2
        conn = self._connect(side, db)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("select pg_replication_origin_create(%s) where"
                            " not exists (select 1 from pg_replication_origin"
                            " where roname = %s)", (name, name))
        except psycopg2.errors.UniqueViolation:
            pass
        finally:
            conn.close()

    def mark_facts(self, side, db):
        """What decides the rungs this side allows (`marks.choose_rung`),
        in one round trip. The origin functions are asked of by name in
        the catalogue: their arguments are not the same on every
        version."""
        like = self.TWOWAY_ORIGIN.replace("_", "\\_")
        got = self._psql(side, db, (
            "select current_setting('server_version_num') || chr(31) ||"
            " ((select rolsuper from pg_roles where rolname = current_user)"
            "  or coalesce((select bool_and(has_function_privilege(oid,"
            "  'execute')) from pg_proc where proname in"
            "  ('pg_replication_origin_create',"
            "   'pg_replication_origin_session_setup',"
            "   'pg_replication_origin_xact_setup',"
            "   'pg_replication_origin_progress',"
            "   'pg_replication_origin_advance')), false))::text || chr(31)"
            " || coalesce((select string_agg(roname, chr(30)) from"
            "  pg_replication_origin where roname not like"
            f" {_literal(like + '%')}), '')"
            " || chr(31) || (has_schema_privilege('public', 'create') or"
            "  to_regclass('public.migkit_origin') is not null)::text"))
        version, grants, foreign, create = got.split("\x1f")
        return {"family": "postgres", "version": int(version),
                "origin_grants": grants == "true",
                "foreign_origins": [n for n in foreign.split("\x1e") if n],
                "can_create": create == "true"}

    def mark_committed(self, side, db, rung, batch):
        """Whether this side committed batch `batch` under the hop's mark,
        as the side itself keeps it: its origin's progress. Only ever
        equal to the batch asked of - an origin's progress moves only
        forward, and a new run's batches are numbered afresh
        (`mark_prove` sets it back)."""
        from .. import marks
        if rung != "origin":
            raise SystemExit(f"two_way: {rung} keeps no batch here")
        name = self.twoway_origin()
        got = self._psql(side, db,
                         "select pg_replication_origin_progress(roname,"
                         " false)::text from pg_replication_origin where"
                         f" roname = {_literal(name)}").strip()
        return bool(got) and marks.number(got) == int(batch)

    def mark_prove(self, side, db, rung):
        """None where a probe written through `rung` comes back from this
        side's log as migkit's own, read by migkit's own reader
        (`_slot_lines`), else why not. Read through a temporary slot made
        before the probe, which goes with its session: nothing of the
        proof stays but what the rung itself leaves."""
        import secrets

        import psycopg2

        from .. import marks, pgslot
        target = self._d(side, db)
        nonce = f"probe:{secrets.token_hex(8)}"
        slot = f"migkit_probe_{secrets.token_hex(6)}"
        try:
            reader = self._conn(side, target)
        except psycopg2.Error as e:
            return _first_line(e)
        reader.autocommit = True
        try:
            with reader.cursor() as cur:
                cur.execute("set statement_timeout = '60s'")
                try:
                    cur.execute("select pg_create_logical_replication_slot"
                                "(%s, %s, true)", (slot, self.PLUGIN))
                except psycopg2.Error as e:
                    return f"its log cannot be read here: {_first_line(e)}"
            writer = self._conn(side, target)
            try:
                with writer.cursor() as cur:
                    if rung == "origin":
                        name = self.twoway_origin()
                        self._origin_named(side, db, name)
                        # a new run's batches are numbered afresh, and an
                        # origin's progress only moves forward
                        cur.execute("select pg_replication_origin_advance"
                                    "(%s, '0/0')", (name,))
                        cur.execute("select"
                                    " pg_replication_origin_session_setup(%s);"
                                    " select pg_replication_origin_xact_setup"
                                    "('0/1', now())", (name,))
                        cur.execute("select pg_logical_emit_message(true, %s,"
                                    " %s)", (pgslot.MARK_PREFIX + "_probe",
                                             nonce))
                        writer.commit()
                        # given back now, not when the session ends
                        cur.execute("select"
                                    " pg_replication_origin_session_reset()")
                    elif rung == "message":
                        cur.execute("select pg_logical_emit_message(true, %s,"
                                    " %s)", (pgslot.MARK_PREFIX, nonce))
                    elif rung == "table":
                        self._origin_table(side, db)
                        cur.execute("insert into public.migkit_origin"
                                    " (origin, n) values (%s, 1); delete from"
                                    " public.migkit_origin where origin = %s",
                                    (nonce, nonce))
                    else:
                        return f"{rung} is not a rung of PostgreSQL's"
                writer.commit()
            except psycopg2.Error as e:
                writer.rollback()
                return _first_line(e)
            finally:
                writer.close()

            def peek(local):
                with reader.cursor() as cur:
                    cur.execute("select lsn::text, xid::text, data from"
                                " pg_logical_slot_peek_changes(%s, NULL,"
                                " NULL" + (", 'only-local', 'true'"
                                           if local else "") + ")", (slot,))
                    return cur.fetchall()

            def ask(sql):
                with reader.cursor() as cur:
                    cur.execute(sql)
                    return "\n".join(str(r[0]) for r in cur.fetchall())

            def holds(rows):
                return any(nonce in data for _, _, data in rows)
            _, local = self._log_end(ask, target, True)
            rows = peek(local)
            if rung == "origin":
                if not holds(peek(False)):
                    return "the probe did not reach the log"
                if holds(rows):
                    return ("the reader did not leave out what carries the"
                            " origin")
                with reader.cursor() as cur:
                    cur.execute("select pg_replication_origin_progress(%s,"
                                " false)::text", (self.twoway_origin(),))
                    at = cur.fetchone()[0]
                    cur.execute("select pg_replication_origin_advance(%s,"
                                " '0/0')", (self.twoway_origin(),))
                if at is None or marks.number(at) != 1:
                    return ("the origin did not keep the position the probe"
                            f" committed ({at})")
                return None
            # the probe's own transaction: the application's, landing in
            # the same moments, is no part of the proof
            mine = self._transaction_of(rows, nonce)
            if not mine:
                return "the probe did not reach the log"
            self._marks_seen = []
            out, _ = self._slot_lines(side, db, mine, None, True, ask=ask)
            if out or not [s for s in self._marks_seen
                           if s["ident"] == nonce and s["kind"] == rung]:
                return ("the probe came back as the application's change,"
                        " not as migkit's own")
            return None
        finally:
            reader.close()

    @staticmethod
    def _transaction_of(rows, needle):
        """The rows of the one transaction in what a slot gave that holds
        `needle`, BEGIN to COMMIT, or [] where none does."""
        block = []
        for row in rows:
            data = row[2]
            if data.startswith("BEGIN"):
                block = []
            block.append(row)
            if data.startswith("COMMIT") and any(needle in r[2]
                                                 for r in block):
                return block
        return []

    def mark_teardown(self, side, db, rung, go):
        """What the rung left on this side, taken away: the table, or the
        hop's replication origins. The statements, run where `go`."""
        from .. import twoway
        steps = []
        if rung == "table":
            steps.append(f"drop table if exists public.{twoway.TABLE}")
        elif rung == "origin":
            like = self.twoway_origin().replace("_", "\\_")
            steps.append("select pg_replication_origin_drop(roname) from"
                         " pg_replication_origin where roname like"
                         f" {_literal(like + '%')}")
        for sql in steps if go else ():
            self._psql(side, db, sql)
        return steps

    def origin_seen(self, side, db):
        """What the last batch this hop committed here said it was
        (`twoway.batch_seen`), None where there is none."""
        from .. import twoway
        conn = self._connect(side, db)
        try:
            with conn.cursor() as cur:
                cur.execute("select to_regclass(%s)",
                            (f"public.{twoway.TABLE}",))
                if cur.fetchone()[0] is None:
                    return None
                cur.execute(f"select seen from public.{twoway.TABLE}"
                            " where origin = %s",
                            (self._origin_row(),))
                got = cur.fetchone()
        finally:
            conn.close()
        return twoway.seen_of(got[0] if got else None)

    def slot_name(self):
        """The slot this hop reads from.

        Derived from the hop's name so two hops against one server do not
        consume each other's changes, and sanitised because a slot name takes
        only lower-case letters, digits and underscores.
        """
        import re
        base = re.sub(r"[^a-z0-9_]", "_", str(self.hop.name).lower())
        return f"migkit_{base}"[:63]

    def drop_lost_position(self, side, db):
        """This hop's slot, dropped only where the server says its WAL is
        gone or going (`wal_status` lost, or unreserved - past
        `max_slot_wal_keep_size`, removed at the next checkpoint): it hands
        out no change again, and a new position is a new slot, made with
        its snapshot. A slot that still holds its WAL is left as it is."""
        self._psql(side, db,
                   "select pg_drop_replication_slot(slot_name) from"
                   " pg_replication_slots where slot_name ="
                   f" {_literal(self.slot_name())} and wal_status in"
                   " ('lost', 'unreserved')")

    def _slot_ready(self, side, db, resuming_from=None, ask=None,
                    make=None):
        """Make sure the slot exists and is one migkit can read. Returns its
        name. `ask(sql)` answers a statement's text, by default through the
        client program; the tail asks on the connection it keeps
        (`_slot_text`).

        `resuming_from` is a position a tail saved. A slot that has to be
        made while one is held means the old slot is gone - dropped, or the
        server rebuilt - and every change after that position went with it.
        Making a new one there would carry on from now, so this stops.

        Created rather than assumed: a slot that does not exist yet has no
        changes in it, and a tail that silently started from "now" would skip
        everything written between the full load and the first call. The slot
        has to exist **before** the rows are copied, which is why this is
        callable on its own.
        """
        name = self.slot_name()
        if ask is None:
            def ask(sql):
                return self._psql(side, db, sql)
        got = ask("select plugin from pg_replication_slots"
                  f" where slot_name = {_literal(name)}").strip()
        if not got and resuming_from:
            raise SystemExit(
                f"the slot {name} is gone from the source, and the tail had"
                f" read up to {resuming_from} from it. The changes after that"
                " were kept by the slot and nowhere else, so starting again"
                " from now would skip them without a trace. Move again with"
                " --mode full+cdc, which makes the slot before it copies")
        if not got:
            level = ask("show wal_level").strip()
            if level != "logical":
                from .. import sourcechange
                raise SystemExit(
                    f"wal_level is {level} on this server and a logical slot"
                    " needs 'logical'. Nothing migkit does client-side can"
                    " make the WAL carry row images it was not told to"
                    " carry.\n"
                    "    alter system set wal_level = 'logical';"
                    "   -- then restart\n"
                    "    rds.logical_replication = 1"
                    "                -- parameter group, then reboot\n"
                    + sourcechange.consequence("wal_level").capitalize())
            if make is not None:
                make(side, db, name)
                return name
            ask("select pg_create_logical_replication_slot"
                f"({_literal(name)}, {_literal(self.PLUGIN)})")
            return name
        if got != self.PLUGIN:
            raise SystemExit(
                f"the slot {name} was made with the {got} plugin and migkit"
                f" reads {self.PLUGIN}. Reading one plugin's output as"
                " another's does not fail, it mis-parses - drop the slot and"
                " let migkit make it, or point this hop at another name")
        return name

    def stream_room(self, side, db, token):
        """The slot's `safe_wal_size`: what the server can still write
        before the slot loses WAL it holds, and the tail's position with
        it. Where nothing caps that (`max_slot_wal_keep_size` -1, the
        default) the slot keeps everything, and what fills is the source's
        disk: `held_bytes` says how much it is holding."""
        got = self._psql(side, db,
                         "select wal_status || chr(31)"
                         " || coalesce(safe_wal_size::text, '') || chr(31)"
                         " || coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(),"
                         " restart_lsn)::bigint::text, '')"
                         " from pg_replication_slots where slot_name ="
                         f" {_literal(self.slot_name())}")
        if not got:
            return None
        # not stripped: Python counts the separator as white space, and a
        # lost slot's empty fields went with it
        status, safe, held = got.split("\x1f")
        out = {"held_bytes": int(held)} if held else {}
        if status == "lost":
            out["bytes"] = 0
        elif safe:
            out["bytes"] = max(int(safe), 0)
        return out or None

    def change_point(self, side, db):
        """The slot, made now if it is not there, and the position it holds
        from.

        The position is the slot's own rather than the server's current
        one: a slot that already existed keeps changes from before now, and
        replaying those converges where skipping them would not.

        A slot made here is made with its snapshot (`_slot_with_its_snapshot`)
        - a copy reads after this, and nothing before the position may be out
        of its sight.
        """
        name = self._slot_ready(side, db, make=self._slot_with_its_snapshot)
        return self._psql(side, db,
                          "select confirmed_flush_lsn::text from"
                          " pg_replication_slots"
                          f" where slot_name = {_literal(name)}").strip()

    def _slot_session(self, side, db):
        """The connection the tail reads its slot on: opened the first time
        and kept for every batch after, in the session the client program
        was given - UTC, ISO dates, no statement timeout - since the slot's
        text is printed in it. Autocommit: each statement is its own
        transaction, as each program was."""
        held = self.__dict__.setdefault("_slot_held", {})
        where = (side, self._d(side, db))
        conn = held.get(where)
        if conn is not None and not conn.closed:
            return conn
        conn = self._open(side, where[1], "slot")
        held[where] = conn
        return conn

    def _slot_rows(self, side, db, sql, args=None):
        """The rows of one statement on the slot's connection. One kept
        from an earlier batch and gone since - the server restarted, an
        idle timeout or a pooler ended it - is opened again and the
        statement asked once more: each one asked here reads, or moves the
        slot to a position it may already be at, which is the same move."""
        import psycopg2
        held = self.__dict__.get("_slot_held", {}).get(
            (side, self._d(side, db)))
        kept = held is not None and not held.closed
        for again in (False, True):
            conn = self._slot_session(side, db)
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, args)
                    return cur.fetchall() if cur.description else []
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                if not conn.closed:
                    # the server answered: an error of the statement's own
                    raise
                self.__dict__["_slot_held"].pop((side, self._d(side, db)),
                                                None)
                if again or not kept:
                    raise

    def _slot_rows_waited(self, side, db, sql, args=None, wait=30.0):
        """`_slot_rows` for a hop with a standby: a machine that lost the
        tail may read the slot once more before it finds it lost (`ha`),
        and the server refuses a second reader of a slot while one reads
        (`object_in_use`) - a read that takes milliseconds, waited out
        rather than taken for a failure."""
        import time
        end = time.monotonic() + wait
        while True:
            try:
                return self._slot_rows(side, db, sql, args)
            except Exception as e:  # noqa: BLE001 - only a busy slot waits
                if _sqlstate(e) != OBJECT_IN_USE or time.monotonic() > end:
                    raise
                # the slot's connection is in autocommit: nothing to undo
                time.sleep(0.2)

    def release_changes(self):
        for conn in self.__dict__.pop("_slot_held", {}).values():
            conn.close()

    def _slot_text(self, side, db):
        """`ask(sql)` on the slot's connection: the first column of each row
        as text, one a line - what the client program printed for them."""
        def ask(sql):
            return "\n".join("" if r[0] is None else str(r[0])
                             for r in self._slot_rows(side, db, sql))
        return ask

    #: how long a copy waits for commits before its position to become
    #: visible, before it stops rather than read around them
    VISIBLE_WAIT = 600

    def _slot_with_its_snapshot(self, side, db, name, plugin=None):
        """Make the slot a copy follows, and return once every change before
        its position is visible to any read made from now on.

        A position and a snapshot taken apart can miss a commit between
        them: PostgreSQL makes a commit visible when it leaves the
        ProcArray, which is not the order of the log - a snapshot can see a
        later commit and miss an earlier one (the research of 2026-09-28,
        L2). Measured on 16: a commit held by a synchronous standby that
        does not answer is in the log and out of sight, and the slot's
        creation waits for that one, by either way of making it
        (`test_rows_written_around_the_slot_arrive`); what the waiting does
        not promise is a commit that reaches the log while the slot is
        being made. The slot made over the replication protocol hands back
        the snapshot its position is exact against (`EXPORT_SNAPSHOT`).
        What that snapshot holds is compared with a snapshot taken now; a
        transaction it counts as done that a read now would not see is
        waited for. After that, a read sees everything before the position,
        and whatever it sees after it the tail applies again by key, which
        converges.

        A server that refuses a replication connection (a pooler, a
        `pg_hba.conf` without it) gets the slot from the SQL function, and
        the wait is on what the commit log says has committed and a read
        cannot see yet - the same transactions, but for one that is between
        writing its commit and marking it.

        `plugin` is the output plugin the slot decodes with: this engine's
        own reader's by default, `pgoutput` for the slot a subscription
        takes over after a copy of the pages (`physical`).
        """
        import psycopg2
        from psycopg2.extras import LogicalReplicationConnection
        plugin = plugin or self.PLUGIN
        target = self._d(side, db)
        try:
            rep = connect(self._ep(side), target,
                          connection_factory=LogicalReplicationConnection)
        except psycopg2.OperationalError:
            rep = None
        if rep is None:
            self._psql(side, db, "select pg_create_logical_replication_slot"
                                     f"({_literal(name)}, {_literal(plugin)})")
            return self._visible_by_the_commit_log(side, db, name)
        try:
            cur = rep.cursor()
            cur.execute(f'CREATE_REPLICATION_SLOT {_ident(name)} LOGICAL'
                        f" {plugin} EXPORT_SNAPSHOT")
            snapshot = cur.fetchone()[2]
            conn = self._conn(side, target)
            try:
                conn.rollback()
                conn.set_session(isolation_level="REPEATABLE READ",
                                 readonly=True)
                with conn.cursor() as c:
                    c.execute("set transaction snapshot %s", (snapshot,))
                    c.execute("select txid_current_snapshot()::text")
                    exported = c.fetchone()[0]
                conn.rollback()
            finally:
                conn.close()
        finally:
            rep.close()
        xmin, xmax, done_then = self._snapshot_parts(exported)

        def unseen():
            _, _, running = self._snapshot_parts(self._psql(
                side, db, "select txid_current_snapshot()::text"))
            # done in the slot's snapshot, still running for a read now
            return sorted(x for x in running
                          if x < xmax and x not in done_then)
        self._wait_visible(side, db, name, unseen)

    def _visible_by_the_commit_log(self, side, db, name):

        def unseen():
            _, _, running = self._snapshot_parts(self._psql(
                side, db, "select txid_current_snapshot()::text"))
            if not running:
                return []
            listed = ",".join(str(x) for x in running)
            got = self._psql(side, db,
                             "select x from unnest(array[" + listed
                             + "]::bigint[]) x where txid_status(x)"
                             " = 'committed'")
            return sorted(int(x) for x in got.split() if x)
        self._wait_visible(side, db, name, unseen)

    def visible_now(self, side, db):
        """Return once every transaction the commit log says committed and
        a read cannot see yet has shown - those are the only ones a slot
        can have decoded out of a read's sight (a commit a synchronous
        standby holds is in the log and not shown). Only those found now
        are waited for; a later one is after anything already read."""

        def hidden(xids):
            if not xids:
                return set()
            listed = ",".join(str(x) for x in sorted(xids))
            got = self._psql(side, db,
                             "select x from unnest(array[" + listed
                             + "]::bigint[]) x where txid_status(x)"
                             " = 'committed'")
            return {int(x) for x in got.split() if x}
        _, _, running = self._snapshot_parts(self._psql(
            side, db, "select txid_current_snapshot()::text"))
        first = hidden(running)
        began = time.time()
        while first:
            _, _, running = self._snapshot_parts(self._psql(
                side, db, "select txid_current_snapshot()::text"))
            first &= running
            if not first:
                return
            if time.time() - began > self.VISIBLE_WAIT:
                raise SystemExit(
                    f"{len(first)} transactions committed are still out of"
                    f" sight of any read after {self.VISIBLE_WAIT}s"
                    f" (transaction ids {', '.join(map(str, sorted(first)[:5]))})"
                    " - a synchronous standby that does not answer holds a"
                    " commit this way. A table added to the tail is not"
                    " copied around them; make the standby answer")
            time.sleep(0.2)

    @staticmethod
    def _snapshot_parts(text):
        """(xmin, xmax, {in progress}) from `txid_current_snapshot()`."""
        xmin, xmax, xip = (str(text).strip().split(":") + ["", ""])[:3]
        return int(xmin), int(xmax), {int(x) for x in xip.split(",") if x}

    def _wait_visible(self, side, db, name, unseen):
        began = time.time()
        while True:
            held = unseen()
            if not held:
                return
            if time.time() - began > self.VISIBLE_WAIT:
                # the slot was made for this copy, which is not happening
                self._psql(side, db,
                           "select pg_drop_replication_slot(slot_name) from"
                           " pg_replication_slots"
                           f" where slot_name = {_literal(name)}")
                raise SystemExit(
                    f"{len(held)} transactions committed before the change"
                    " position are still out of sight of any read after"
                    f" {self.VISIBLE_WAIT}s (transaction ids"
                    f" {', '.join(str(x) for x in held[:5])}) - a"
                    " synchronous standby that does not answer holds a"
                    " commit this way. A copy read now would miss them and"
                    " the change tail starts after them, so nothing was"
                    " copied. Make the standby answer, or take it out of"
                    " synchronous_standby_names, and move again.")
            time.sleep(0.2)

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Row changes out of a logical slot, as neutral records.

        **Peeked, not consumed.** `pg_logical_slot_get_changes` advances the
        slot as it reads, so a crash between reading and applying loses the
        changes with nothing left to replay. `peek` leaves them, and a restart
        re-reads what it had already applied - which the appliers are
        idempotent for. Duplicated work is visible; a hole is not.

        What consumes them is the token coming back: handing back the token
        from the previous call is how a caller says "everything up to here is
        applied", and only then does the slot move past it. So the slot is
        advanced by evidence of success rather than by the act of looking.

        **On one connection, kept** (`_slot_session`). Each batch started
        the client program three times - the slot moved on, the log's end
        asked, the changes peeked - a process and a connection each, and
        the changes came back as the lines it printed: a text value holding
        a line break arrived as two lines and stopped the tail on half a
        value at every try (`unterminated value`), and a carriage return
        came back a newline. The position and the change are read as the
        two columns they are. Measured, PostgreSQL 16 to PostgreSQL 16 on a
        laptop, 320,000 changes queued (`bench/tail_rates.py`): a hundred
        starts of the program took 4.3s, the server decoding all 320,000
        0.47s and parsing them here 1.7s; the whole tail 16.1s before,
        7.3s on one connection.
        """
        ask = self._slot_text(side, db)
        name = self._slot_ready(side, db, resuming_from=token, ask=ask)
        standby = bool((self.hop.options or {}).get("standby"))
        rows_of = self._slot_rows_waited if standby else self._slot_rows
        if token and standby:
            # a machine that took the tail over goes on from the position
            # the last holder put where it reads, which may be a save
            # behind the one that holder moved the slot to - and the slot
            # moves only past what was applied, so it goes on from there:
            # the server refuses to move a slot back (`ha`)
            rows_of(side, db, "select pg_replication_slot_advance("
                              "slot_name, %s::pg_lsn) from"
                              " pg_replication_slots where slot_name = %s"
                              " and confirmed_flush_lsn < %s::pg_lsn",
                    (token, name, token))
        elif token:
            # the caller applied everything up to this LSN, so the server may
            # stop keeping it
            self._slot_rows(side, db, "select pg_replication_slot_advance("
                                      "%s, %s)", (name, token))
        # a two-way tail's own transactions are left out whole, or they go
        # back where they came from (`twoway`, `marks`)
        two_way = bool((self.hop.options or {}).get("two_way"))
        # up to where the log is now: when that is read whole, the position
        # moves there even with nothing in it for this hop, so a fence
        # waiting for the tail to reach the log's end sees it get there. It
        # stayed at the last change, and on a quiet database never did.
        upto, local = self._log_end(ask, self._d(side, db), two_way)
        # each change with its transaction's xid, for the tail to hold to
        # the marks the table copier kept (`mark_covers`); each commit with
        # the time the source made it, for the tail's beat
        rows = rows_of(side, db, "select lsn::text, xid::text, data"
                                 " from pg_logical_slot_peek_changes("
                                 "%s, %s, %s, 'include-timestamp',"
                                 " 'true'"
                       + (", 'only-local', 'true'" if local else "")
                       + ")", (name, upto, int(limit)))
        self._marks_seen = []
        out, last = self._slot_lines(side, db, rows, token, two_way, name,
                                     ask)
        if len(rows) < int(limit) and upto and (
                last is None or self.position_reached(upto, last)):
            last = upto
        return out, last

    def _log_end(self, ask, target, two_way):
        """(where the log is now, whether the server leaves out what
        carries an origin), asked through `ask(sql)`. A two-way side where
        a tail applies under a replication origin of migkit's (`marks`)
        has what carries one left out by the server before it is decoded
        (`only-local`) - and where another origin applies into the same
        server too, its changes would go with them, so the tail stops and
        says so.

        The *flushed* end: decoding reads no further than that, so a bound
        past it (the written end, `pg_current_wal_lsn()`) was handed back
        as read when the stretch between the two had not been, and the
        next call advanced the slot over it. Measured on 16 under async
        commits: the written end stood past the flushed one in 199 of 200
        samples, by up to 42,688 bytes."""
        if not two_way:
            return ask("select pg_current_wal_flush_lsn()").strip(), False
        got = ask("select pg_current_wal_flush_lsn()::text || chr(31) ||"
                  " coalesce((select string_agg(roname, chr(30)) from"
                  " pg_replication_origin), '')")
        upto, _, names = got.strip().partition("\x1f")
        names = [n for n in names.split("\x1e") if n]
        ours = [n for n in names if n.startswith(self.TWOWAY_ORIGIN)]
        theirs = [n for n in names if not n.startswith(self.TWOWAY_ORIGIN)]
        if ours and theirs:
            raise SystemExit(
                f"two_way: a tail applies into {target} under a replication"
                f" origin of migkit's ({', '.join(ours)}), and"
                f" {', '.join(theirs)} applies into it too. Leaving out what"
                " migkit applied there would leave out what the other"
                " applies, and those changes would never reach the other"
                " side. Tear the two-way tail into this side down (move"
                " --mode cdc --drop), or the other; its next start chooses"
                " another way to mark what it applies")
        return upto, bool(ours)

    def _slot_lines(self, side, db, rows, last, two_way, slot=None,
                    ask=None):
        """(changes, position) out of what a slot gave, (lsn, xid, text) a
        row: the tail's reader and the proof of a rung (`mark_prove`) read
        through this one loop. A transaction that begins with migkit's
        mark - a logical message of its prefix, or a row of
        `migkit_origin` - is left out whole, and noted in `_marks_seen`
        with what identifies it."""
        from .. import canon, pgslot
        out, keys, marked, begun = [], {}, None, 0
        seen = self.__dict__.setdefault("_marks_seen", [])
        # a partition's changes are its root's: the hop takes, filters and
        # renames a partitioned table by the name the application knows
        root_of, leaves_of = self._root_reader(side, db, ask)
        for lsn, xid, data in rows:
            last = lsn or last
            if data.startswith("BEGIN"):
                marked, begun = None, len(out)
                continue
            if data.startswith("COMMIT"):
                # the commit's own position is where a read goes on after
                # it, and its time is the source's (`include-timestamp`)
                canon.ended(out, begun, at=lsn or None,
                            ts=pgslot.commit_time(data))
                continue
            if two_way and marked is None:
                said = pgslot.mark_message(data)
                if said is not None:
                    marked = {"kind": "message", "ident": said}
                    seen.append(marked)
                    continue
            parsed = pgslot.parse_line(data)
            if parsed is None:
                continue
            table = parsed["table"]
            if parsed["op"] == "truncate":
                # at its place among the rows, for the tail to cut the
                # table's changes there (`canon.ddl`)
                if not marked:
                    out += self._truncated(db, parsed["tables"], xid,
                                           root_of, leaves_of)
                continue
            if two_way and str(table).split(".")[-1] == "migkit_origin":
                if marked is None:
                    marked = {"kind": "table", "ident": None}
                    seen.append(marked)
                if marked["ident"] is None:
                    marked["ident"] = next(
                        (v for n, _, v, _ in parsed["new"] or parsed["old"]
                         if n == "origin"), None)
            if marked:
                continue
            leaf, table = table, root_of(table)
            if not self.in_scope(db, table):
                continue
            if table != leaf:
                parsed = dict(parsed, table=table)
            if table not in keys:
                keys[table] = (self.neutral_key(side, db, table, ask=ask)
                               or (self.neutral_key(side, db, leaf, ask=ask)
                                   if leaf != table else []))
                if not keys[table]:
                    raise SystemExit(
                        f"no primary key on {table} - a change to a keyless"
                        " table cannot be addressed on the target, and"
                        " applying it by matching every column would hit"
                        " every duplicate.\n"
                        "    The change is still in the slot and every call"
                        " will stop here again, because peeking never throws"
                        " anything away. Give the table a key or a unique"
                        " REPLICA IDENTITY, or step past it with:\n"
                        f"    select pg_replication_slot_advance({_literal(slot)},"
                        " pg_current_wal_lsn());"
                        "  -- skips everything pending, including this")
            out.append(pgslot.change(parsed, keys[table],
                                     int(xid) if str(xid or "").isdigit()
                                     else None))
        return out, last

    def _truncated(self, db, tables, xid, root_of, leaves_of):
        """[canon.ddl] of one TRUNCATE the slot read, under each table's
        root: `truncate` where the table is emptied whole - a partitioned
        one where every partition it has was - and `reload` where only
        some of its partitions were, which the tail copies again and never
        empties in place (the rest of the table's rows are the source's
        still)."""
        from .. import canon
        txn = int(xid) if str(xid or "").isdigit() else None
        by = {}
        for t in tables:
            by.setdefault(root_of(t), []).append(t)
        out = []
        for root, got in by.items():
            if not self.in_scope(db, root):
                continue
            whole = got == [root] or set(got) >= leaves_of(root)
            out.append(canon.ddl(root, "truncate" if whole else "reload",
                                 txn=txn))
        return out

    #: seconds the partitions' roots are taken as read (`partition_roots`)
    ROOTS_SECONDS = 60

    def partition_roots(self, side, db, ask=None):
        """{table: the table at the top of its partitions} for every table
        of the side, named as the change log names them (`schema.table`,
        each part quoted where it must be) - a table that is no partition
        is its own. The one reading of partitions by their root: a hop
        takes, filters and renames a partition through its root, the tail
        carries a partition's changes under its root's name (`_slot_lines`)
        and the publication publishes the roots (`published_tables`).
        `ask(sql)`, where given, asks on that connection."""
        return self._roots(side, db, ask)[0]

    def _roots(self, side, db, ask=None):
        """({table: root}, {root: its partitions that hold rows}), read
        once a side for `ROOTS_SECONDS`."""
        held = self.__dict__.setdefault("_roots_of", {})
        at = (side, self._d(side, db))
        got = held.get(at)
        if got is not None and time.monotonic() - got[0] < \
                self.ROOTS_SECONDS:
            return got[1]
        name = ("format('%I.%I', {n}.nspname, {c}.relname)")
        if (self._server_version(side, db) or 0) >= 120000:
            sql = ("select " + name.format(n="n", c="c") + " || chr(31) ||"
                   " coalesce((select " + name.format(n="rn", c="r")
                   + " from pg_class r join pg_namespace rn on rn.oid ="
                   " r.relnamespace where r.oid = pg_partition_root(c.oid)),"
                   " " + name.format(n="n", c="c") + ") || chr(31) ||"
                   " c.relkind from pg_class c join pg_namespace n on n.oid"
                   " = c.relnamespace where c.relkind in ('r', 'p') and"
                   " n.nspname not in ('pg_catalog', 'information_schema')")
        else:
            # before 12, the top of each table's partitions by its parents
            sql = ("with recursive up(rel, top) as (select c.oid, c.oid"
                   " from pg_class c where c.relkind in ('r', 'p') union all"
                   " select up.rel, i.inhparent from up join pg_inherits i"
                   " on i.inhrelid = up.top join pg_class p on p.oid ="
                   " i.inhparent and p.relkind = 'p') select "
                   + name.format(n="n", c="c") + " || chr(31) || "
                   + name.format(n="rn", c="r") + " || chr(31) || c.relkind"
                   " from up join pg_class c on c.oid = up.rel join"
                   " pg_namespace n on n.oid = c.relnamespace join pg_class"
                   " r on r.oid = up.top join pg_namespace rn on rn.oid ="
                   " r.relnamespace where not exists (select 1 from"
                   " pg_inherits i join pg_class p on p.oid = i.inhparent"
                   " and p.relkind = 'p' where i.inhrelid = up.top) and"
                   " n.nspname not in ('pg_catalog', 'information_schema')")
        text = ask(sql) if ask is not None else self._psql(side, db, sql)
        roots, leaves = {}, {}
        for line in text.splitlines():
            parts = line.split("\x1f")
            if len(parts) != 3:
                continue
            table, root, kind = parts
            roots[table] = root
            if kind == "r":
                leaves.setdefault(root, set()).add(table)
        held[at] = (time.monotonic(), (roots, leaves))
        return roots, leaves

    def _root_reader(self, side, db, ask=None):
        """(root_of(table), leaves_of(root)) over `_roots`, read again once
        where a table not among them is met - one made since they were
        read - and that table its own root where it is still not."""
        state = {"again": True}

        def got():
            return self._roots(side, db, ask)

        def root_of(table):
            roots = got()[0]
            if table not in roots and state.pop("again", False):
                self.__dict__.get("_roots_of", {}).pop(
                    (side, self._d(side, db)), None)
                roots = got()[0]
            return roots.get(table, table)

        def leaves_of(root):
            return got()[1].get(root, {root})
        return root_of, leaves_of

    #: a table made with its rows in one statement (CREATE TABLE AS,
    #: SELECT INTO) has those rows in no change the slot decodes, and the
    #: log says nothing of the table's making: a table a tail meets that
    #: it did not see made is copied, never taken live (`tailtables`)
    CREATES_UNSEEN = True

    def table_oids(self, side, db):
        """{schema.table: its pg_class oid} of the application's tables, as
        `neutral_tables` names them. The log carries no DDL, and a shape is
        keyed by name: a table swapped for another under its name (two
        renames) keeps its name and columns and changes only this - which
        the tail holds each table to (`tailtables`)."""
        out = self._psql(side, db, "select n.nspname||'.'||c.relname||"
                                   "chr(31)||c.oid from pg_class c join"
                                   " pg_namespace n on n.oid ="
                                   " c.relnamespace where "
                                   + user_tables_where())
        got = {}
        for line in out.splitlines():
            name, _, oid = line.rpartition("\x1f")
            if name:
                got[name] = oid
        return got

    def snapshot_mark(self, side, db):
        """The server's snapshot now, `xmin:xmax:xip,...` (PostgreSQL 13's
        `pg_current_snapshot`, `txid_current_snapshot` before it), after
        the server it was taken on (`_marked_on`): every transaction it
        shows committed is visible to any read begun after it, which is
        what a range copied from here reflects."""
        on = self._marked_on(side, db)
        for fn in ("pg_current_snapshot", "txid_current_snapshot"):
            try:
                got = self.run_rule(side, db, f"select {fn}()::text")
            except Exception:  # noqa: BLE001 - older name, or no answer
                continue
            if got and got[0][0] and on:
                return f"{on}|{got[0][0]}"
        return None

    def marked_rule(self, side, db, sql):
        """The read and its mark in one REPEATABLE READ transaction: the
        snapshot `pg_current_snapshot` names is the one the read is made
        under, so the two marks are one and no change falls between
        them."""
        on = self._marked_on(side, db)
        conn = self._connect(side, db)
        try:
            conn.rollback()
            conn.set_session(isolation_level="REPEATABLE READ",
                             readonly=True)
            with conn.cursor() as cur:
                mark = None
                for fn in ("pg_current_snapshot", "txid_current_snapshot"):
                    try:
                        cur.execute(f"select {fn}()::text")
                        mark = cur.fetchone()[0]
                        break
                    except Exception:  # noqa: BLE001 - the older name
                        conn.rollback()
                cur.execute(sql)
                rows = cur.fetchall()
        finally:
            conn.rollback()
            conn.close()
        mark = f"{on}|{mark}" if mark and on else None
        return mark, rows, mark

    def _marked_on(self, side, db):
        """The server a mark belongs to: its cluster's system identifier,
        or, where this user may not read that, when it started - an xid
        means nothing on another cluster, and a checkpoint kept from a
        copy of another source would otherwise be held to this one's."""
        got = self.__dict__.setdefault("_marks_on", {})
        if side not in got:
            said = self.stream_identity(side, db)
            if said:
                got[side] = f"cluster {said['cluster']}"
            else:
                try:
                    row = self.run_rule(side, db, "select"
                                        " pg_postmaster_start_time()::text")
                    got[side] = f"started {row[0][0]}" if row else None
                except Exception:  # noqa: BLE001 - no mark, then
                    got[side] = None
        return got[side]

    def mark_covers(self, side, db, mark, change):
        """A committed xid is visible to the snapshot when it is below its
        `xmin`, or below its `xmax` and not among those in progress. The
        log names the xid in 32 bits and the snapshot in 64: it is taken
        as the one nearest the snapshot's, which the server keeps within
        2^31 of every xid still in use."""
        xid = change.get("txn")
        if not isinstance(mark, str) or not isinstance(xid, int) \
                or isinstance(xid, bool):
            return None
        on, _, snap = mark.rpartition("|")
        if not on or on != self._marked_on(side, db):
            return None
        try:
            xmin, xmax, xip = snap.split(":")
            xmin, xmax = int(xmin), int(xmax)
            running = {int(x) for x in xip.split(",") if x}
        except ValueError:
            return None
        d = (xid - xmax) & 0xFFFFFFFF
        full = xmax + (d - 2 ** 32 if d >= 2 ** 31 else d)
        if full < xmin:
            return True
        return full < xmax and full not in running

    def log_position(self, side, db):
        if self._in_recovery(side, db):
            return None
        return self.wal_position(side, db)

    #: The position a fence waits for: every commit the server has
    #: acknowledged, which is the WAL *insert* position, not the written
    #: one. With `synchronous_commit = off` a commit is acknowledged once
    #: its record is inserted, and `pg_current_wal_lsn()` - what has been
    #: written out - trails it until the WAL writer's next round. Measured
    #: on PostgreSQL 16, one async insert at a time, each read right after
    #: its commit: at the default `wal_writer_delay` (200ms) 50 of 50
    #: commits sat past the written position, by up to 288 bytes, for a
    #: median 0.14s and at most 0.20s; at `wal_writer_delay = 10s`, 20 of
    #: 20, by up to 2,264 bytes, the last one for 4.7s. A fence at the
    #: written position passed with that commit still to come, and a
    #: stream torn down there never carried it.
    #:
    #: At the start of a page the insert position points past the page's
    #: header (24 bytes, 40 at the start of a segment), where no record
    #: ends and no consumer's flushed position ever stands: on a quiet
    #: server - a frozen source is one - a fence there waits until
    #: something else writes. The header is taken off again.
    POSITION_SQL = ("select pg_current_wal_insert_lsn()::text||' '||"
                    "current_setting('wal_block_size')||' '||"
                    "(select setting from pg_settings"
                    " where name = 'wal_segment_size')")

    def wal_position(self, side, db):
        """The insert position as a fence wants it (`POSITION_SQL`)."""
        got = self._psql(side, db, self.POSITION_SQL).strip()
        return self._past_page_header(got) if got else None

    @staticmethod
    def _past_page_header(got):
        lsn, block, segment = got.split()
        hi, _, lo = lsn.partition("/")
        at = (int(hi, 16) << 32) | int(lo, 16)
        if at % int(segment) == 40:
            at -= 40
        elif at % int(block) == 24:
            at -= 24
        return f"{at >> 32:X}/{at & 0xFFFFFFFF:X}"

    @staticmethod
    def position_reached(have, want):
        """LSNs compare as the 64-bit numbers their two halves make."""
        def at(lsn):
            hi, _, lo = str(lsn).partition("/")
            return (int(hi, 16) << 32) | int(lo, 16)
        try:
            return at(have) >= at(want)
        except (TypeError, ValueError):
            return None

    #: the declared types a JSON column's values are rendered as canon's
    #: JSON text by the server itself (`_json_text_sql`)
    JSON_IN_SQL = ("json", "jsonb", "hstore")
    #: a number of this many digits or more in a document may be one the
    #: fold in this process rounds (`canon.render_value` normalises a
    #: decimal in a context of 28 digits): a document holding one is
    #: folded here, so the digest stays the fold's
    JSON_DIGITS = 29

    @staticmethod
    def _json_text_sql(c):
        """SQL for canon's JSON text of column `c` (`canon.json_text`):
        `jsonb`'s own text - an object's keys by their length and then
        their bytes, the last of a key given twice, `", "` and `": "`
        between, a string escaped as JSON escapes it - with each number
        outside a string written as its value, the zeros ending its
        fraction dropped and a point with nothing after it too (`1.10` ->
        `1.1`, `2.0` -> `2`); `jsonb` never writes an exponent. A string is
        matched whole first, so a number inside one is never touched."""
        string = r'("(?:[^"\\]|\\.)*")'
        both = _literal(r"\1\2")
        t = f"to_jsonb({c})::text"
        t = (f"regexp_replace({t}, "
             f"{_literal(string + r'|([0-9]\.[0-9]*[1-9])0+(?![0-9])')},"
             f" {both}, 'g')")
        return (f"regexp_replace({t}, "
                f"{_literal(string + r'|([0-9])\.0+(?![0-9])')},"
                f" {both}, 'g')")

    def _json_in_sql(self, side, db, table, columns):
        """The JSON columns of `columns` the server can render as canon's
        text itself, or None where it cannot render them all: a database
        not in UTF-8 orders an object's keys by other bytes than canon's."""
        from .. import canon
        wanted = [n for n, c in columns if canon.folded_here(c)]
        if not wanted:
            return []
        if any(c != "json" for _, c in columns if canon.folded_here(c)):
            return None
        declared = dict(self.neutral_columns(side, db, table))
        if any(str(declared.get(n, "")).lower() not in self.JSON_IN_SQL
               for n in wanted):
            return None
        codes = self.__dict__.setdefault("_encodings", {})
        at = (side, self._d(side, db))
        if at not in codes:
            codes[at] = self._psql(side, db, "show server_encoding").strip()
        return wanted if codes[at].upper() in ("UTF8", "UTF-8") else None

    def neutral_digest(self, side, db, table, columns, where=None):
        """(rows, digest) of a table as canon renders its rows, summed by the
        server. A JSON column is rendered as canon's text by the server too
        (`_json_text_sql`) - a document was folded in this process on every
        cross-engine proof, the proof taking three times the copy's time at
        a million rows (backlog P0-DEPTH 7) - and a table the server cannot
        render whole, or holding a number the fold here rounds
        (`JSON_DIGITS`), is folded here: the same digest either way."""
        from .. import canon, rowtext
        jsons = self._json_in_sql(side, db, table, columns)
        if jsons is None:
            return self._folded_digest(side, db, table, columns, where)
        sch, tbl = self._split(table)
        source = f"{_ident(sch)}.{_ident(tbl)}" + self._where(where)
        if jsons:
            inner = ", ".join(
                f"{self._json_text_sql(_ident(n))} as {_ident(n)}"
                if n in jsons else _ident(n) for n, _ in columns)
            source = f"(select {inner} from {source}) s"
        row = rowtext.postgres_row_from(
            [_ident(n) if n in jsons else canon.expr("postgres", n, c)
             for n, c in columns])
        many = _literal("[0-9.]{%d,}" % self.JSON_DIGITS)
        guard = " or ".join(f"{_ident(n)} ~ {many}" for n in jsons) \
            or "false"
        try:
            got = self._psql(side, db,
                             "select count(*)::text||chr(31)||"
                             f"{canon.digest_expr('postgres', row)}::text"
                             f"||chr(31)||coalesce(bool_or({guard}), false)"
                             f"::text from {source}").strip()
        except SqlFailed:
            if not jsons:
                raise
            # a `json` value `jsonb` cannot hold (`\u0000`)
            return self._folded_digest(side, db, table, columns, where)
        n, d, rounded = (got.split("\x1f") + ["", ""])[:3]
        if rounded == "true":
            return self._folded_digest(side, db, table, columns, where)
        return (int(n), d)

    def _folded_digest(self, side, db, table, columns, where):
        """`neutral_digest` folded in this process: a JSON document read as
        its text, which canon parses as JSON - the driver hands a document
        over parsed, where a JSON `null` came as SQL's and a string holding
        `5` as the text canon then read as the number."""
        from .. import canon
        declared = dict(self.neutral_columns(side, db, table))
        text = [n for n, c in columns if c == "json"
                and str(declared.get(n, "")).lower() in ("json", "jsonb")]
        return canon.fold_batches(columns, self.neutral_batches(
            side, db, table, columns, 5000, where=where, as_text=text))

    @staticmethod
    def _quote_ident(name):
        return _ident(name)

    def _quote_literal(self, value):
        return _literal(value)

    def _qualified(self, side, db, table):
        sch, tbl = self._split(table)
        return f"{self._quote_ident(sch)}.{self._quote_ident(tbl)}"

    @staticmethod
    def _split(table):
        """(schema, table) from either form, one way (`util.split_table`):
        a quoted name's dots are its own, and a name with no schema is in
        `public`.

        `"t".partition(".")` answers `('t', '', '')`, so an unqualified name
        silently became schema `t` and table `""` - which PostgreSQL reports
        as `zero-length delimited identifier`. Change records arrive with
        bare table names because a binlog has no schemas, which is where this
        first showed up.
        """
        return split_table("postgres", table, "public")

    def _brand_probes(self):
        """The version banner and the reported server_version, per side.

        Both, in one round trip, because they disagree on everything that is
        not PostgreSQL itself: measured, CockroachDB v23.2.5 answers
        `show server_version` with `13.0.0` while its banner says what it
        really is. The banner is the evidence; the number is what every other
        comparison in migkit is built on, which is the problem.
        """
        def one(side):
            try:
                got = self._psql(side, "postgres",
                                 "select version()||chr(31)"
                                 "||current_setting('server_version')")
            except Exception:
                return {}
            ver, _, sver = got.strip().partition("\x1f")
            out = {"version": ver, "server_version": sver}
            # Citus's own settings exist only where its library is loaded;
            # its version is the extension's (`variants.SIGNATURES`)
            try:
                citus = self._psql(
                    side, "postgres",
                    "select coalesce((select default_version from"
                    " pg_available_extensions where name = 'citus' and"
                    " exists (select 1 from pg_settings where name like"
                    " 'citus.%')), '')").strip()
            except Exception:
                citus = ""
            if citus:
                out["citus"] = citus
            return out
        return (one("src"), one("dst"))

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        out = self._psql("src", "postgres",
                         "select datname from pg_database where not datistemplate"
                         " and datname not in ('postgres','rdsadmin') order by 1")
        return [l for l in out.splitlines() if l and not self.hop.excluded(l)]

    def _left_out_of_schema(self, db, side):
        """{(schema, table)} on this side that the whole-database schema
        comparers leave out.

        The tables the hop excludes, which are the target's own and which
        no other check reads either. And the tables whose columns the hop
        maps: the pair machinery compares those column by column through
        the mapping (`_mapped_schema`), where a comparer of whole databases
        can only call every column the hop drops or renames a
        difference."""
        out = {self._split(t) for t in self._all_tables(side, db)
               if not self.in_scope(db, t)}
        for t in self.mapped_tables(db):
            sch, tbl = self._split(t)
            if side == "dst":
                sch, tbl = split_table("postgres",
                                       self.hop.target_table_in(db, sch,
                                                                tbl), sch)
            out.add((sch, tbl))
        return out

    def _dump_schema_native(self, side, db, physical=None):
        from .. import leftovers as _lo
        from ..movers import _pg_pattern
        # the views, materialized views and sequences the hop leaves out
        # with the tables: the dump's table switch takes every relation
        left = [a for sch, tbl in sorted(self._left_out_of_schema(db, side)
                                         | self._objects_not_taken(side, db))
                for a in ("--exclude-table", _pg_pattern(f"{sch}.{tbl}"))]
        argv, env = self._client(
            side, db, "pg_dump", "--schema-only", "--no-owner",
            "--no-privileges", "--no-security-labels", "--no-tablespaces",
            "--exclude-schema", self.hop.options.get("exclude_schema", "__*"),
            "--exclude-table", "*.migkit_changelog*", *left,
            *_lo.pg_dump_args(), physical=physical)
        p = run(argv, env=env)
        noise = self._noise()
        keep = []
        for l in p.stdout.splitlines():
            if (l.startswith(("--", "SET ", "\\restrict", "\\unrestrict",
                              "SELECT pg_catalog.set_config")) or not l.strip()):
                continue
            if any(f"EVENT TRIGGER {n}" in l or f"PUBLICATION {n}" in l
                   for n in noise):
                continue
            keep.append(l)
        pats = self._ignore_patterns()
        if pats:
            keep = [l for l in keep if not any(pt.search(l) for pt in pats)]
        return "\n".join(keep)

    def check_schema(self, db):
        import difflib
        src = self._dump_schema_native("src", db)
        dst = self._dump_schema_native("dst", db)
        d = self._report(db)
        d.mkdir(parents=True, exist_ok=True)
        (d / "schema-src.sql").write_text(src + "\n")
        (d / "schema-dst.sql").write_text(dst + "\n")
        changed = [l for l in difflib.unified_diff(
                       src.splitlines(), dst.splitlines(), "src", "dst",
                       lineterm="")
                   if l[:1] in "+-" and not l.startswith(("+++", "---"))]
        whole = False
        if changed:
            (d / "schema.diff").write_text("\n".join(changed) + "\n")
            status, line = "diff", f"{len(changed)} changed lines"
            # a table on one side only is never cosmetic, whatever another
            # comparer read
            whole = any(l[1:].lstrip().upper().startswith("CREATE TABLE")
                        for l in changed)
        else:
            (d / "schema.diff").unlink(missing_ok=True)
            status, line = "ok", "schema identical (native pg_dump diff)"
        res = [Result("schema", db, status, line, str(d / "schema.diff"),
                      "review diff, apply missing DDL from schema-src.sql")]
        res[0].whole_table = whole
        res.append(self.check_structural_diff(db))
        res.append(self.check_objects(db))
        # the two optional readings are said where they did not run, and
        # why - they used to vanish from the report on a timeout (audit
        # 2026-10-01, SC16)
        res.append(self.check_liquibase(db))
        res.append(self.check_atlas(db))
        res = self._atlas_authoritative(res)
        # after the demotion, not before: the schema-aware comparison left
        # these tables out, so its reading is no authority over them
        res += self._mapped_schema(db)
        # written for the same reason `data-evidence.txt` is: the schema
        # cross-check must second-guess the verdict this run reached, not
        # re-derive one of its own a few seconds later
        (d / "schema-evidence.txt").write_text(
            "".join(f"{r.scope}: {r.status.upper()}\n" for r in res))
        return res

    def _schema_from_evidence(self, db):
        """migkit's own schema verdict for this database: True clean, False
        differs, None if the schema check has not run."""
        path = self.hop.report_dir(db) / "schema-evidence.txt"
        if not path.exists():
            return None
        states = [ln.rsplit(":", 1)[-1].strip()
                  for ln in path.read_text().splitlines() if ln.strip()]
        if not states:
            return None
        return not any(s == "DIFF" for s in states)

    # Statements that remove something. The differ emits these happily; only
    # the operator can say whether dropping an object the source no longer has
    # is the intent or an accident, so they are counted and surfaced, never
    # silently included in a "just apply this" recommendation.
    _DESTRUCTIVE = re.compile(
        r"^\s*(drop\s|alter\s+table\s+.*\s+drop\s|truncate\s)", re.I | re.M)

    #: what the structural differ holds per object kind; a table's own
    #: objects name it as `table_name`, the table itself as `name`
    _DIFF_KINDS = ("tables", "relations", "selectables", "indexes",
                   "constraints",
                   "triggers", "rlspolicies", "sequences", "privileges")

    def _leave_out_of_diff(self, db, migration, reverse=False):
        """Take the tables the pair compares out of both sides the
        structural differ inspected, with every object that hangs off
        them, and another tool's schemas whole - the schema, its tables and
        what they own (`leftovers.NOT_DATA_SCHEMAS`). `reverse` is the
        differ run the other way round."""
        from ..leftovers import NOT_DATA_SCHEMAS
        sides = (("dst", migration.changes.i_from),
                 ("src", migration.changes.i_target))
        if reverse:
            sides = (("src", migration.changes.i_from),
                     ("dst", migration.changes.i_target))
        for side, inspected in sides:
            for schema in NOT_DATA_SCHEMAS:
                inspected.exclude_schema(schema)
            schemas = getattr(inspected, "schemas", None)
            if hasattr(schemas, "items"):
                for k in [k for k, v in schemas.items()
                          if getattr(v, "schema", k) in NOT_DATA_SCHEMAS
                          or k in NOT_DATA_SCHEMAS]:
                    del schemas[k]
            left = (self._left_out_of_schema(db, side)
                    | self._objects_not_taken(side, db))
            if not left:
                continue
            for kind in self._DIFF_KINDS:
                got = getattr(inspected, kind, None)
                if not hasattr(got, "items"):
                    continue
                for k in [k for k, v in got.items()
                          if (getattr(v, "schema", None),
                              getattr(v, "table_name", None)
                              or getattr(v, "name", None)) in left]:
                    del got[k]

    def check_structural_diff(self, db):
        """Object-by-object schema comparison, in-process.

        This runs as a library call rather than a subprocess on purpose. The
        previous differ was invoked as a program and its output read from
        stdout: when it stopped working on modern Python it produced no output
        and no error, so the check quietly vanished from every report instead
        of failing. An exception here is reported as an error, which is the
        behaviour we want from a thing whose job is to notice problems.
        """
        # escaped whole, with the side's TLS: built by hand, an `@`, `:`
        # or `/` in the password moved the host, and the hop's TLS was
        # dropped (audit 2026-10-01, SC17)
        surl = self.hop.source.url("postgresql", db)
        turl = self.hop.target.url("postgresql", self._d("dst", db))
        d = self._report(db)
        d.mkdir(parents=True, exist_ok=True)
        try:
            import results as _results
            from results.dbdiff import Migration
        except Exception as e:
            return Result("schema", f"{db} (structural)", "error",
                          f"structural differ unavailable: {e}", "",
                          "reinstall migkit; this check must not be skipped")
        try:
            # from target to source: what the target needs in order to match
            sdb, tdb = _results.db(surl), _results.db(turl)
            m = Migration(tdb, sdb)
            self._leave_out_of_diff(db, m)
            m.add_all_changes_ordered(privileges=True)
            sql = m.sql
            meta = m.result_metadata(options={"privileges": True})
            # The same comparison in the opposite direction is the undo of
            # exactly these statements. It has to be taken here, from the same
            # two snapshots: after the fix is applied the target no longer
            # describes where it came from, and the undo is unrecoverable.
            #
            # The same two handles serve both directions - measured, and the
            # reason it matters is that `results` exposes no way to close a
            # connection and does not drop one on garbage collection, so a
            # second pair would double what this check leaves open against a
            # production source every time it runs.
            try:
                r = Migration(sdb, tdb)
                self._leave_out_of_diff(db, r, reverse=True)
                r.add_all_changes_ordered(privileges=True)
                reverse_sql = r.sql
            except Exception:
                reverse_sql = ""
        except Exception as e:
            return Result("schema", f"{db} (structural)", "error",
                          f"structural diff failed: {str(e).splitlines()[0][:160]}",
                          "", "check connectivity and permissions on both sides")
        (d / "structural-diff.json").write_text(json.dumps(meta, indent=1,
                                                           sort_keys=True))
        if not sql.strip():
            # Clear the previous run's evidence, the way the MySQL paths
            # already do. A left-over fix script is a repair for differences
            # that no longer exist, and a left-over revert is worse: an undo
            # for changes nobody made, sitting in the directory an operator
            # opens during a cutover.
            for stale in ("structural-fix.sql", "structural-fix.locks.txt",
                          "structural-fix.revert.sql"):
                (d / stale).unlink(missing_ok=True)
            return Result("schema", f"{db} (structural)", "ok",
                          f"{meta['totals']['added'] + meta['totals']['removed']}"
                          " object differences, none structural"
                          if any(meta["totals"].values()) else
                          "every object identical, both sides",
                          str(d / "structural-diff.json"))
        path = d / "structural-fix.sql"
        path.write_text(sql)
        drops = len(self._DESTRUCTIVE.findall(sql))
        # What it will lock, next to what it will change. The target of a
        # repair is often still serving an application, and handing over DDL
        # without saying what it blocks is how a verification becomes an
        # outage.
        from .. import locks as _locks
        from .. import revert as _revert
        lock_text, lock_counts = _locks.report(sql)
        (d / "structural-fix.locks.txt").write_text(lock_text)
        rev_path = d / "structural-fix.revert.sql"
        rev_body = _revert.script(sql, reverse_sql, "structural-fix.sql")
        if rev_body:
            rev_path.write_text(rev_body)
        else:
            rev_path.unlink(missing_ok=True)
        tot = meta["totals"]
        detail = (f"{tot['added']} to add, {tot['removed']} to remove,"
                  f" {tot['modified']} to change"
                  f" across {len(meta['object_counts'])} object types")
        if drops:
            detail += (f"; {drops} of the generated statements remove an"
                       " object - read those before applying")
        lock_line = _locks.summary(lock_counts)
        if lock_line:
            detail += f"; {lock_line}"
        rev_line = _revert.summary(sql, reverse_sql)
        detail += ("; " + rev_line if rev_line else
                   "; no undo could be generated - take a backup first")
        detail += self._backfill_clause(db, sql)
        return Result("schema", f"{db} (structural)", "diff", detail,
                      str(path),
                      "statements are in dependency order; read"
                      " structural-fix.locks.txt for what each one blocks,"
                      " review the removals, then apply on the target."
                      " structural-fix.revert.sql undoes it")

    def _url_without_password(self, side, db):
        """A side's address with its TLS (`Endpoint.url`) and no password,
        for a program whose arguments any process listing reads: its
        password goes in a password file."""
        from ..config import Endpoint
        ep = self._ep(side)
        return Endpoint(host=ep.host, port=ep.port, user=ep.user,
                        options=dict(ep.options or {})).url(
            "postgresql", self._d(side, db))

    def _schema_urls(self, db):
        """(source, target) as the schema comparison's URLs."""
        return (self.hop.source.url("postgres", db),
                self.hop.target.url("postgres", self._d("dst", db)))

    def _schema_excludes(self, db):
        """What the schema comparison leaves out: migkit's own objects, and
        the tables the pair compares through the mapping."""
        from ..leftovers import NOT_DATA_SCHEMAS
        return ["__*", "*.migkit_changelog", *sorted(NOT_DATA_SCHEMAS)] + [
            f"{sch}.{tbl}" for side in ("src", "dst")
            for sch, tbl in sorted(self._left_out_of_schema(db, side)
                                   | self._objects_not_taken(side, db))]

    def _not_run(self, db, scope, why):
        """A reading of the schema that did not run, said as such - in
        migkit's words, the program's name taken out of what it said."""
        from ..movers import DRIVEN
        from ..wording import without_programs
        return Result("schema", f"{db} {scope}", "skip",
                      f"not run: {without_programs(str(why), DRIVEN)}")

    def _optional_off(self, name):
        """Why an optional reading of the schema is not run here, or ""."""
        if not self.hop.options.get(name, True):
            return f"the hop turns it off ({name}: false)"
        if not which(name):
            return "the program it runs is not installed here"
        return ""

    def _atlas_authoritative(self, res):
        """The schema-aware reading says clean only of what it inspects -
        tables, their columns, keys and indexes - where the dump's text
        also holds triggers, views and routines: it may demote the text's
        difference to cosmetic only where the structural reading, which
        inspects those, found nothing either. A routine's or a trigger's
        body that differed read ok behind it (audit 2026-10-01, SC16)."""
        structural = [r for r in res if r.scope.endswith("(structural)")]
        if not structural or any(r.status != "ok" for r in structural):
            return res
        return super()._atlas_authoritative(res)

    def check_atlas(self, db):
        from ..movers import schema_diff
        scope = self.AUTHORITY_SCOPE
        off = self._optional_off("atlas")
        if off:
            return self._not_run(db, scope, off)
        su, tu = self._schema_urls(db)
        try:
            p = schema_diff(tu, su, self._schema_excludes(db))
        except Exception as e:  # noqa: BLE001 - said, not dropped
            return self._not_run(db, scope, _first_line(e))
        if p.returncode != 0:
            return self._not_run(db, scope, "it failed: " + (
                (p.stderr or "").strip().splitlines() or [""])[-1][:160])
        text = p.stdout.strip()
        if not text or "Schemas are synced" in text:
            return Result("schema", f"{db} {self.AUTHORITY_SCOPE}", "ok",
                          "the two schemas match, compared as schemas rather"
                          " than as text")
        out = self._report(db) / "schema-fix.sql"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n")
        return Result("schema", f"{db} {self.AUTHORITY_SCOPE}", "diff",
                      f"{len(text.splitlines())} lines of DDL would bring the"
                      " target's schema up to the source's",
                      str(out), "review schema-fix.sql, then apply it to the"
                                " target")

    @staticmethod
    def _liquibase_sections(text):
        """[(heading, [object])] from a schema comparison's text report:
        `Missing Column(s):` and the objects indented under it, one per
        line; a heading that ends `NONE` holds nothing."""
        out, cur = [], None
        for line in text.splitlines():
            m = re.match(r"^((?:Missing|Unexpected|Changed) .+?\(s\)):"
                         r"\s*(NONE)?\s*$", line)
            if m:
                cur = None if m.group(2) else (m.group(1), [])
                if cur:
                    out.append(cur)
            elif cur and line.startswith("     ") \
                    and not line.startswith("          ") and line.strip():
                cur[1].append(line.strip())
            elif not line.startswith(" "):
                cur = None
        return out

    @staticmethod
    def _jdbc_url(ep, dbname):
        """A side as a Java client's address, its TLS in the fields that
        client reads (`tls_forms`'s `jdbc`)."""
        from urllib.parse import quote, urlencode
        host = str(ep.host or "")
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        query = urlencode(ep.tls_forms("postgres")["jdbc"])
        return (f"jdbc:postgresql://{host}:{ep.port}/"
                f"{quote(str(dbname), safe='')}" + (f"?{query}" if query
                                                    else ""))

    def check_liquibase(self, db):
        from ..durable import write_atomic
        s, t = self.hop.source, self.hop.target
        scope = self.OBJECT_SCOPE
        off = self._optional_off("liquibase")
        if off:
            return self._not_run(db, scope, off)
        out = self._report(db) / "schema-objects.txt"
        try:
            # the passwords in the environment, which it reads as well as
            # its flags (measured), not on a command line anyone can list
            p = run(["liquibase", "diff",
                     f"--url={self._jdbc_url(t, self._d('dst', db))}",
                     f"--username={t.user}",
                     f"--referenceUrl={self._jdbc_url(s, db)}",
                     f"--referenceUsername={s.user}"],
                    env={"LIQUIBASE_COMMAND_PASSWORD": t.password,
                         "LIQUIBASE_COMMAND_REFERENCE_PASSWORD": s.password},
                    check=False, timeout=180)
        except Exception as e:  # noqa: BLE001 - said, not dropped
            return self._not_run(db, scope, _first_line(e))
        if p.returncode != 0:
            return self._not_run(db, scope, "it failed: " + (
                (p.stderr or "").strip().splitlines() or [""])[-1][:160])
        write_atomic(out, p.stdout)
        noise = self._noise()
        left = {f"{sch}.{tbl}" for side in ("src", "dst")
                for sch, tbl in self._left_out_of_schema(db, side)
                | self._objects_not_taken(side, db)}
        bad = []
        for head, items in self._liquibase_sections(p.stdout):
            # the database's own name is the hop's db_map, not a difference
            if "Catalog" in head:
                continue
            items = [i for i in items
                     if "__" not in i and "migkit_changelog" not in i
                     and not any(n in i for n in noise)
                     and not any(re.search(rf"(^|[\s(]){re.escape(t)}"
                                           rf"($|[.(\[\s])", i)
                                 for t in left)]
            if items:
                bad.append(f"{head}: " + ", ".join(items[:4])
                           + (" ..." if len(items) > 4 else ""))
        if bad:
            return Result("schema", f"{db} {self.OBJECT_SCOPE}", "diff",
                          "; ".join(bad[:6]), str(out),
                          "schema-objects.txt lists every object that"
                          " differs")
        return Result("schema", f"{db} {self.OBJECT_SCOPE}", "ok",
                      "no object differs by name between the two schemas")

    def _objects_left_out_of_schema(self, db, side):
        """The inventory's names for the tables the pair compares and
        everything on them: the table, its constraints and triggers
        (`schema.table.name`), and its indexes, which are named apart."""
        from ..leftovers import NOT_DATA_SCHEMAS
        left = self._left_out_of_schema(db, side)
        if not left:
            # another tool's schema, and everything in it
            return lambda name: str(name).split(".")[0] in NOT_DATA_SCHEMAS
        pairs = ", ".join(f"({_literal(sch)}, {_literal(tbl)})" for sch, tbl in sorted(left))
        idx = set(self._psql(side, db,
            "select n.nspname||'.'||ci.relname from pg_index i"
            " join pg_class ci on ci.oid = i.indexrelid"
            " join pg_class ct on ct.oid = i.indrelid"
            " join pg_namespace n on n.oid = ct.relnamespace"
            f" where (n.nspname::text, ct.relname::text) in ({pairs})"
            ).splitlines())
        whole = {f"{sch}.{tbl}" for sch, tbl in left}
        return lambda name: (name in whole or name in idx
                             or name.rsplit(".", 1)[0] in whole
                             or str(name).split(".")[0] in NOT_DATA_SCHEMAS)

    #: the inventory's kinds that are not on one table, each the hop takes
    #: or leaves by its own name (`_object_left_out`)
    OWN_NAMED = ("view", "matview", "sequence", "function", "procedure")

    def _object_left_out(self, side, db):
        """A predicate on an object of a side - its kind and `schema.name`
        as the inventory names it - true where the hop leaves it out: a
        table, view or materialized view by its own name (`in_scope`), a
        routine by its name without its arguments, a sequence by the table
        whose column it feeds (`_seq_owned`) and else by its own name. The
        inventory, the owners, the grants and the schema fix read this one
        answer: the views, sequences and functions of tables the hop left
        out were compared, and the schema fix made them on the target."""
        from ..config import scoped
        seq_gone = self._seq_gone(side, db) if scoped(self.hop) else None

        def out(kind, name):
            name = str(name)
            if kind.lower() == "sequence" and seq_gone is not None:
                return seq_gone(name)
            if kind.lower() in ("function", "procedure", "routine"):
                name = name.split("(", 1)[0]
            return not self.takes(db, kind, name)
        return out

    def _objects_not_taken(self, side, db):
        """{(schema, name)} of the views, materialized views, sequences
        and routines on a side that the hop leaves out (`_object_left_out`):
        what the schema comparers and the schema fix leave out beside the
        tables, so the fix never makes one on the target. Nothing for a hop
        that takes everything, which is read as it always was."""
        return {self._split(n)
                for kind, names in self.objects_left_out(side, db).items()
                if kind in self.OWN_NAMED for n in names}

    def objects_left_out(self, side, db):
        """{kind: [name]} of a side's objects beside its tables that the
        hop leaves out (`_object_left_out`), as the inventory names them:
        views and materialized views, sequences, routines without their
        arguments, and triggers as `schema.table.trigger`. What the
        comparers and the schema fix leave out (`_objects_not_taken`), and
        what the schema a move makes leaves out with them
        (`movers._pg_schema`). Nothing for a hop that takes everything,
        which is read as it always was."""
        from ..config import scoped
        if not scoped(self.hop):
            return {}
        out = self._object_left_out(side, db)
        got = {}
        for line in self._psql(side, db, INVENTORY_SQL).splitlines():
            kind, _, name = line.partition("|")
            if kind in self.OWN_NAMED + ("trigger",) and out(kind, name):
                if kind in ("function", "procedure"):
                    name = name.split("(", 1)[0]
                got.setdefault(kind, []).append(name)
        return got

    def check_objects(self, db):
        from .. import drift
        sides = {}
        for side in ("src", "dst"):
            m = {}
            left = self._objects_left_out_of_schema(db, side)
            out = self._object_left_out(side, db)
            lines = [l.partition("|")[::2] for l in
                     self._psql(side, db, INVENTORY_SQL).splitlines()]
            # an online schema change's working tables, with what hangs off
            # them, and the triggers it put on the table it copies, go
            # when it finishes (`drift.transient`)
            tables = [n for t, n in lines if t == "table"]
            working = drift.transient_among(tables)
            for t, name in lines:
                if left(name) or (t == "trigger"
                                  and drift.transient_trigger(name)):
                    continue
                if t in self.OWN_NAMED and out(t, name):
                    continue
                if name in working or name.rsplit(".", 1)[0] in working \
                        or (t != "table" and drift.transient(name, tables)):
                    continue
                m.setdefault(t, set()).add(name)
            sides[side] = m
        inv = {}
        for t in sorted(set(sides["src"]) | set(sides["dst"])):
            s = sides["src"].get(t, set())
            d = sides["dst"].get(t, set())
            inv[t] = {"src": len(s), "dst": len(d),
                      "missing": sorted(s - d)[:50],
                      "extra": sorted(d - s)[:50]}
        out = self.hop.report_dir(db) / "objects.json"
        out.write_text(json.dumps(inv, indent=1))
        bad = {t: v for t, v in inv.items()
               if t != "invalid-index" and (v["missing"] or v["extra"])}
        note = ""
        iv = inv.get("invalid-index")
        if iv and iv["src"]:
            note = (f"; note: {iv['src']} invalid index on source"
                    f" ({', '.join(iv['missing'][:3])}), drop and rebuild"
                    " there, pg_dump and most movers skip it")
        if bad:
            parts = []
            for t, v in bad.items():
                p = f"{t} {v['src']}/{v['dst']}"
                if v["missing"]:
                    p += " missing: " + ", ".join(v["missing"][:3])
                if v["extra"]:
                    p += " extra: " + ", ".join(v["extra"][:3])
                parts.append(p)
            return Result("schema", f"{db} objects", "diff",
                          "; ".join(parts) + note, str(out),
                          "create missing objects on target from schema-src.sql")
        total = sum(v["src"] for t, v in inv.items() if t != "invalid-index")
        return Result("schema", f"{db} objects", "ok",
                      f"{total} objects in {len(inv)} types,"
                      f" all present on target{note}")

    PARAM_CRITICAL = ("TimeZone", "client_encoding", "server_encoding",
                      "lc_collate", "lc_ctype", "lc_monetary", "lc_numeric",
                      "lc_time", "DateStyle", "IntervalStyle",
                      "standard_conforming_strings", "bytea_output",
                      "default_transaction_isolation", "extra_float_digits",
                      "wal_level", "integer_datetimes", "backslash_quote",
                      "check_function_bodies", "array_nulls", "search_path",
                      "default_text_search_config")

    def check_params(self, db):
        q = ("select name||chr(31)||coalesce(setting,'') from pg_settings"
             " order by name")

        def pull(side):
            return dict(l.split("\x1f", 1) for l in
                        self._psql(side, db, q).splitlines() if "\x1f" in l)

        return self._param_result(
            db, pull("src"), pull("dst"), self.PARAM_CRITICAL,
            "align the behavior-critical GUCs on the target parameter group"
            " before cutover")

    SEQ_Q = ("select schemaname||'.'||sequencename||'|'||coalesce(last_value,0)"
             " from pg_sequences where schemaname not like '\\_\\_%'"
             f" and sequencename not in {OWN_SQL} order by 1")

    # map every serial/identity sequence to the column it feeds, via the
    # dependency catalog, so we can prove nextval clears that column's max.
    SEQ_OWNED_Q = (
        "select sn.nspname||'.'||s.relname||'|'||ns.nspname||'|'||t.relname"
        "||'|'||a.attname"
        " from pg_class s"
        " join pg_namespace sn on sn.oid = s.relnamespace"
        " join pg_depend d on d.objid = s.oid"
        "  and d.classid = 'pg_class'::regclass"
        "  and d.refclassid = 'pg_class'::regclass and d.deptype in ('a','i')"
        " join pg_class t on t.oid = d.refobjid"
        " join pg_namespace ns on ns.oid = t.relnamespace"
        " join pg_attribute a on a.attrelid = d.refobjid"
        "  and a.attnum = d.refobjsubid"
        " where s.relkind = 'S' and sn.nspname not like '\\_\\_%'"
        f" and s.relname not in {OWN_SQL}")

    def _seq_owned(self, side, db):
        """seqkey (schema.sequence) -> (schema, table, column) it feeds."""
        owned = {}
        for l in self._psql(side, db, self.SEQ_OWNED_Q).splitlines():
            p = l.split("|")
            if len(p) == 4:
                owned[p[0]] = (p[1], p[2], p[3])
        return owned

    # the tables whose column defaults draw from a sequence: one no table
    # owns (shared by several, or a `default nextval(...)` written by hand)
    # goes with the tables that use it
    SEQ_USED_Q = (
        "select distinct sn.nspname||'.'||s.relname||'|'||tn.nspname||'|'"
        "||t.relname"
        " from pg_depend d"
        " join pg_attrdef ad on ad.oid = d.objid"
        " join pg_class t on t.oid = ad.adrelid"
        " join pg_namespace tn on tn.oid = t.relnamespace"
        " join pg_class s on s.oid = d.refobjid and s.relkind = 'S'"
        " join pg_namespace sn on sn.oid = s.relnamespace"
        " where d.classid = 'pg_attrdef'::regclass"
        " and d.refclassid = 'pg_class'::regclass")

    def _seq_left_out(self, side, db, names, owned=None):
        """The sequences among `names` (`schema.sequence`, as `side` has
        them) the hop does not take (`_seq_gone`), so no counter check or
        repair reads or sets them.

        Found by the audit of scope (backlog P0-SCOPE.6a): with `exclude:
        [job_queue]`, a table the target writes itself, its
        sequence was planned from the source's last value - below the keys
        the target had handed out, so the cutover's counter gate refused,
        and set, the target's next insert would have taken a key it holds.
        """
        gone = self._seq_gone(side, db, owned)
        return {n for n in names if gone(n)}

    def _seq_gone(self, side, db, owned=None):
        """A predicate on a sequence of a side (`schema.sequence`), true
        where the hop does not take it - the one reading the counters,
        their repair, the inventory and the schema fix share: one a table
        owns (a serial's, an identity's - `_seq_owned`) goes with that
        table; one no table owns goes with the tables whose defaults draw
        from it, left out only where every one of them is; one nothing
        owns or draws from by its own name - and each only where the hop
        carries sequences at all (`objects`)."""
        owned = self._seq_owned(side, db) if owned is None else owned
        used = None

        def gone(name):
            nonlocal used
            name = str(name)
            if name in owned:
                by = [owned[name][:2]]
            else:
                if used is None:
                    used = {}
                    for l in self._psql(side, db,
                                        self.SEQ_USED_Q).splitlines():
                        p = l.split("|")
                        if len(p) == 3:
                            used.setdefault(p[0], []).append((p[1], p[2]))
                by = used.get(name) or []
            if by:
                return all(not self.takes(db, "sequence", name, f"{s}.{t}")
                           for s, t in by)
            return not self.takes(db, "sequence", name)
        return gone

    def _seq_col_max(self, side, db, owned):
        """seqkey -> max(owned column) on `side`, so we know the floor the
        sequence must clear to avoid a duplicate-key collision on insert."""
        if not owned:
            return {}
        parts = [f"select {_literal(k + '|')}||coalesce(max({_ident(c)}),0)"
                 f" from {_ident(s)}.{_ident(t)}"
                 for k, (s, t, c) in owned.items()]
        out = {}
        for i in range(0, len(parts), 200):
            chunk = " union all ".join(parts[i:i + 200])
            for l in self._psql(side, db, chunk).splitlines():
                if "|" in l:
                    key, _, v = l.rpartition("|")
                    out[key] = int(v)
        return out

    def _seq_next(self, side, db, owned):
        """seqkey -> the value nextval() would return WITHOUT consuming it.
        A never-called sequence returns last_value itself; once called it
        returns last_value + increment. Getting this right is the difference
        between last_value==max (safe, nextval clears it) and a real
        collision, so we read is_called rather than assume."""
        if not owned:
            return {}
        inc = {}
        for l in self._psql(side, db,
                            "select schemaname||'.'||sequencename||'|'"
                            "||increment_by from pg_sequences").splitlines():
            if "|" in l:
                k, _, v = l.rpartition("|")
                inc[k] = int(v)
        parts = []
        for k in owned:
            sch, name = self._split(k)
            parts.append(f"select {_literal(k)}||e'\\t'||last_value||e'\\t'||is_called"
                         f' from {_ident(sch)}.{_ident(name)}')
        out = {}
        for i in range(0, len(parts), 200):
            chunk = " union all ".join(parts[i:i + 200])
            for l in self._psql(side, db, chunk).splitlines():
                p = l.split("\t")
                if len(p) == 3:
                    last, called = int(p[1]), p[2] in ("t", "true")
                    out[p[0]] = last + inc.get(p[0], 1) if called else last
        return out

    def _noise(self):
        """Prefixes of objects the movers create for their own bookkeeping.

        A leg can carry leftovers from more than one mover - DTS writes dts_*,
        DMS writes awsdms_* - so the option is a comma-separated list and every
        caller matches against the whole tuple. Returns () when unset, which
        makes `startswith(())` false and every `any()` below empty.
        """
        from ..hopkeys import noise
        return noise(self.hop)

    def _keep_tbl(self, db, t):
        """False if the 'schema.table' is excluded for this db, so it is
        neither verified nor repaired (protects target-owned tables).

        Tables the mover keeps for itself (noise_prefix) are excluded too,
        by the hop's scope as every engine's are (`Hop.excluded`): they
        exist on one side by nature and would count as a difference every
        time while holding no application data."""
        from .. import drift
        if drift.maybe_transient(t) and t in self._working_tables(db):
            return False
        return self.in_scope(db, t)

    def in_scope(self, db, table):
        """`Engine.in_scope`, a table without its schema read in `public`
        as `_split` reads it: `regclass` names a table on the search path
        without one, and `orders` asked as itself missed an `include` of
        `appdb.public.orders`. A partition is asked with its table's name
        (`_ruled`)."""
        return not self.hop.excluded(db, *self._ruled(db, table))

    def _ruled(self, db, t):
        """(schema, table) the hop's rules are asked about table `t` with:
        its table's where it is a partition (`root_of`), the name
        `exclude`, `include` and `mapping.where` are written in. Asked by
        its own name, a partition of a table the hop leaves out was
        compared with the target's own rows, and one of a filtered table
        read whole. Asked only of a hop with such a rule
        (`movers.rules_by_name`), and by its own name where the source
        cannot be asked, as it always was."""
        from ..movers import rules_by_name
        if not rules_by_name(self.hop):
            return self._split(t)
        try:
            return self.root_of("src", db, t)
        except Exception:  # noqa: BLE001 - by its own name, then
            return self._split(t)

    def root_of(self, side, db, table):
        """(schema, table) of the table at the top of `table`'s partitions
        on a side, its own where it is none (`partition_roots`), however
        either is quoted."""
        roots = self.partition_roots(side, db)
        got = self.__dict__.get("_split_roots")
        if got is None or got[0] is not roots:
            got = (roots, {self._split(a): self._split(b)
                           for a, b in roots.items()})
            self.__dict__["_split_roots"] = got
        key = self._split(table)
        return got[1].get(key, key)

    def _scoped(self, db, lines, sep="|"):
        """The lines of a listing - catalogue rows whose first field, up to
        `sep`, is the table they are about - that are of a table the hop
        takes (`in_scope`). Every deep check, inventory and repair reads
        its tables through it, so none reads one the hop leaves out: a
        keyless or drifted table the move never took made the deep check
        say different (backlog P0-SCOPE.6b)."""
        if isinstance(lines, str):
            lines = lines.splitlines()
        return [l for l in lines
                if l and self.in_scope(db, l.split(sep, 1)[0])]

    def _working_tables(self, db):
        """The tables an online schema change is working in, on either
        side (`drift.transient`): not the application's rows, and on the
        other side by nature. Read once per database."""
        from .. import drift
        known = self.__dict__.setdefault("_working_of", {})
        if db not in known:
            found = set()
            for side in ("src", "dst"):
                try:
                    found |= drift.transient_among(self._all_tables(side, db))
                except Exception:  # noqa: BLE001 - a target not made yet
                    continue
            known[db] = found
        return known[db]

    def check_counts(self, db):
        st = [t for t in self._psql("src", db, self.USER_TABLES).splitlines()
              if t and self._keep_tbl(db, t)]
        dt = set(t for t in self._psql("dst", db, self.USER_TABLES).splitlines()
                 if t and self._keep_tbl(db, t))
        bad = [f"{t} missing on target" for t in st if t not in dt]
        bad += [f"{t} extra on target"
                for t in sorted(dt - set(st))]

        def cnt(side, t):
            sch, tbl = self._split(t)
            return int(self._psql(side, db,
                                  f"select count(*) from {self._scope(db, t)}")
                   or 0)

        common = [t for t in st if t in dt]
        total = 0
        with ThreadPoolExecutor(max_workers=self.hop.workers) as pool:
            futs = {t: (pool.submit(cnt, "src", t), pool.submit(cnt, "dst", t))
                    for t in common}
            for t in common:
                a, b = futs[t][0].result(), futs[t][1].result()
                total += a
                if a != b:
                    bad.append(f"{t} src={a} dst={b}")
                outside = self._outside_filter(db, t)
                if outside:
                    bad.append(f"{t} dst holds {outside} rows the hop's row"
                               " filter excludes - the move does not put"
                               " them there")
        if bad:
            return [Result("counts", db, "diff", clauses(bad), "",
                           "missing rows show up in check data, fix there")]
        return [Result("counts", db, "ok",
                       f"{len(common)} tables, {total:,} rows both sides")]

    def check_autoinc(self, db):
        """Two verdicts per database. USABLE is the one that matters at
        cutover: every serial/identity sequence on the target must sit above
        its column's max, or the first insert duplicate-keys (the single most
        common migration outage). PARITY is the softer 1-to-1 check that the
        target continues from the same value the source stopped at."""
        src = dict(l.rsplit("|", 1) for l in
                   self._psql("src", db, self.SEQ_Q).splitlines() if l)
        dst = dict(l.rsplit("|", 1) for l in
                   self._psql("dst", db, self.SEQ_Q).splitlines() if l)
        # A mover's own bookkeeping tables carry sequences of their own -
        # awsdms_heartbeat_hb_key_seq lives on whichever side that mover writes
        # to and cannot exist on the other. Reporting those as MISSING buries
        # the sequences that do matter under noise nobody can act on.
        noise = self._noise()
        if noise:
            def app(d):
                return {n: v for n, v in d.items()
                        if not n.rsplit(".", 1)[-1].startswith(noise)}
            src, dst = app(src), app(dst)
        # the counters of the tables the hop takes, and only theirs: a table
        # it leaves out keeps the counter its own writer gives it, and
        # compared, it differed on every run, `watch --verify` included
        gone = self._seq_left_out("src", db, list(src))
        src = {n: v for n, v in src.items() if n not in gone}
        dst = {n: v for n, v in dst.items() if n not in gone}
        owned = self._seq_owned("dst", db)
        if noise:
            owned = {k: v for k, v in owned.items()
                     if not k.rsplit(".", 1)[-1].startswith(noise)}
        gone = self._seq_left_out("dst", db, list(owned), owned)
        owned = {k: v for k, v in owned.items() if k not in gone}
        dmax = self._seq_col_max("dst", db, owned)
        dnext = self._seq_next("dst", db, owned)
        collide = []
        for k, (s, t, c) in sorted(owned.items()):
            nxt = dnext.get(k, 0)
            mx = dmax.get(k, 0)
            if mx > 0 and nxt <= mx:
                collide.append(f"{k}: nextval={nxt} <= max({t}.{c})={mx}")
        res = []
        if collide:
            res.append(Result("autoinc", f"{db} usable", "diff",
                              "sequences WILL collide on next insert: "
                              + "; ".join(collide[:8]), "",
                              f"migkit sync {self.hop.name} --db {db} --kind"
                              " sequences --apply  (fix BEFORE cutover)"))
        else:
            res.append(Result("autoinc", f"{db} usable", "ok",
                              f"{len(owned)} owned sequences all clear their"
                              " column max, no collision"
                              if owned else "no owned sequences"))
        parity = [f"{n} src={v} dst={dst.get(n, 'MISSING')}"
                  for n, v in sorted(src.items()) if dst.get(n) != v]
        if parity:
            res.append(Result("autoinc", f"{db} parity", "diff",
                              "; ".join(parity[:8]), "",
                              f"migkit sync {self.hop.name} --db {db} --kind"
                              " sequences --apply"))
        else:
            res.append(Result("autoinc", f"{db} parity", "ok",
                              f"{len(src)} sequences match source last_value"))
        return res

    #: active sessions against the limit; the lag of the replicas this
    #: server feeds; and, where it is itself a standby, its own: nothing
    #: where every byte it received is replayed - `now()` less the last
    #: replayed commit's time grows by itself on a standby whose primary is
    #: idle, and read as lag it braked every table of a check for nothing -
    #: else the time since that commit; and whether it is a standby
    HEALTH_SQL = (
        "select (select count(*) from pg_stat_activity where state='active')"
        "::float / greatest(current_setting('max_connections')::float, 1)"
        " || '|' || coalesce((select max(extract(epoch from replay_lag))"
        " from pg_stat_replication), -1)"
        " || '|' || case when not pg_is_in_recovery() then -1"
        " when pg_last_wal_receive_lsn() is not null and"
        " pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() then 0"
        " else coalesce(extract(epoch from"
        " now() - pg_last_xact_replay_timestamp()), -1) end"
        " || '|' || pg_is_in_recovery()::int")

    def _health(self, side, db):
        """What this side says about its own load, for the throttle.

        Cheap on purpose - two catalog reads, no table access. Anything the
        provider hides comes back as None rather than as "fine": a managed
        database that will not show us its replication state must not be
        mistaken for one that is idle. A standby says so, and its own lag is
        what the throttle's replica brake holds (`throttle.REPLICA_RISE`).
        """
        from ..throttle import Health
        try:
            raw = self._psql(side, db, self.HEALTH_SQL).strip()
            busy, send_lag, replay_lag, standby = (
                float(x) for x in raw.split("|"))
        except Exception:
            return None
        lag = max(send_lag, replay_lag)
        return Health(busy_ratio=busy if busy >= 0 else None,
                      lag_seconds=lag if lag >= 0 else None,
                      replica=bool(standby) and replay_lag >= 0)

    # A table worth checkpointing. Below this a single aggregate finishes in
    # seconds, and splitting it would cost more in round trips than a restart
    # would ever save.
    CHUNK_MIN_ROWS = 5_000_000

    #: the tables whose key is one whole-number column, and that column
    BIG_TABLES_SQL = (
        "select n.nspname||'.'||c.relname||'|'||a.attname"
        " from pg_class c"
        " join pg_namespace n on n.oid = c.relnamespace"
        " join pg_index i on i.indrelid = c.oid and i.indisprimary"
        " join pg_attribute a on a.attrelid = c.oid"
        "   and a.attnum = i.indkey[0]"
        " where c.relkind = 'r' and i.indnatts = 1"
        "   and a.atttypid in ('int2'::regtype,'int4'::regtype,'int8'::regtype)"
        "   and n.nspname not in ('pg_catalog','information_schema')")

    def _chunkable(self, db):
        """table -> (estimated rows, single integer primary-key column).

        Restricted to one-column integer keys on purpose: range predicates on
        those are index-only and cheap to reason about. A composite or text key
        can still be checksummed, just in one pass.

        The rows from `row_estimate`; a table whose count the statistics do
        not know is taken as big, and checked restartably: the live count
        read 0 after a statistics reset or a fail-over, and a table of a
        billion rows was checked in one pass nothing could resume (audit
        2026-10-01, DU18).
        """
        out = {}
        try:
            rows = self.row_estimate("src", db)
            for line in self._psql("src", db,
                                   self.BIG_TABLES_SQL).splitlines():
                name, _, col = line.rpartition("|")
                if not name:
                    continue
                est = rows.get(name)
                if est is None or est >= self.CHUNK_MIN_ROWS:
                    out[name] = (est, col)
        except Exception:
            pass    # no key facts = no chunking, never a failure
        return out

    def _scope(self, db, t, side=None):
        """What a check reads of table `t`, as a FROM item aliased `t`.

        The whole table, or only the rows the hop's row filter selects. The
        filter narrows what the move carries (`mapping.where`), and
        `Hop.row_filter()` - which says it feeds the checksum's WHERE - had
        no caller anywhere: the check compared a filtered target against
        the whole source and called the rows the filter left behind
        missing, for good. Every read a check makes of a table goes through
        here, so counts, checksums, chunks and the drilldown cannot
        disagree about which rows are in scope.

        Rows the filter excludes that are sitting on the target anyway are
        a separate question, answered by `_outside_filter`. A partition's
        filter is its table's (`_ruled`).

        Asked for the target (`side="dst"`), the table under the name the
        hop gives it there (`Hop.target_table_in`): the delta reads both
        sides by key, and a renamed table's target was read under the
        source's name.
        """
        sch, tbl = self._split(t)
        pred = (self.hop.row_filter(db, *self._ruled(db, t))
                if hasattr(self.hop, "row_filter") else None)
        if side == "dst" and hasattr(self.hop, "target_table_in"):
            sch, tbl = split_table("postgres", self.hop.target_table_in(
                db, sch, tbl), sch)
        qt = f'{_ident(sch)}.{_ident(tbl)}'
        return f"(select * from {qt} where {pred}) t" if pred else f"{qt} t"

    def _outside_filter(self, db, t):
        """Target rows the hop's row filter excludes, or None without one.

        Narrowing the comparison to the filter is right for the rows the
        move carries, and blind to everything else: a target row outside
        the filter - left from an earlier load, or written there since - is
        not compared by anything. The move does not put rows there, so any
        count above zero is said. `is not true` rather than `not`, so a row
        where the predicate is NULL is counted too.
        """
        sch, tbl = self._split(t)
        pred = (self.hop.row_filter(db, *self._ruled(db, t))
                if hasattr(self.hop, "row_filter") else None)
        if not pred:
            return None
        return int(self._psql("dst", db,
                              f'select count(*) from {_ident(sch)}.{_ident(tbl)}'
                              f" where ({pred}) is not true") or 0)

    def _digest_where(self, side, db, table, where):
        """The checksum of the rows of `table` a predicate selects, on one
        side: every column of every row, as the whole-table checksum reads
        them."""
        h = self._row_hash_expr("src", db, table)
        return self._psql(side, db, f"select count(*)||'|'||"
                                    f"{self._summed(h)}"
                                    f" from {self._scope(db, table)}"
                                    f" where {where}")

    def group_candidates(self, side, db, table):
        """[(column, neutral class, estimated distinct values)] for one
        table, from the statistics the server keeps - nothing is scanned.
        A negative `n_distinct` is a fraction of the rows (`row_estimate`,
        where the statistics know them). A table never analysed has none,
        and is not narrowed."""
        from .. import canon
        sch, tbl = self._split(table)
        rows = self.row_estimate(side, db).get(f"{sch}.{tbl}")
        out = []
        for line in self._psql(side, db, f"""
                select s.attname||chr(31)||format_type(a.atttypid,
                       a.atttypmod)||chr(31)||s.n_distinct::text
                  from pg_stats s
                  join pg_namespace n on n.nspname = s.schemaname
                  join pg_class c on c.relnamespace = n.oid
                                 and c.relname = s.tablename
                  join pg_attribute a on a.attrelid = c.oid
                                     and a.attname = s.attname
                 where s.schemaname = {_literal(sch)} and s.tablename = {_literal(tbl)}
                   and not s.inherited""").splitlines():
            parts = line.split("\x1f")
            if len(parts) != 3:
                continue
            try:
                distinct = float(parts[2])
            except ValueError:
                continue
            if distinct < 0:
                if rows is None:
                    continue
                distinct = -distinct * rows
            out.append((parts[0], canon.type_class("postgres", parts[1]),
                        int(distinct)))
        return out

    def _data_fast_native(self, db, stream=None, may_skip=True):
        """Per-table checksum on both sides in parallel: commutative
        sum-of-md5 as a Postgres parallel aggregate (no sort, no lock beyond
        a plain SELECT). Emits the same OK/DIFF/ERROR lines the slice-mode
        drilldown and counts merge consume."""
        tables = [t for t in
                  self._psql("src", db, self.USER_TABLES).splitlines()
                  if t and self._keep_tbl(db, t)
                  and not self.through_pair(db, t)]
        w = int(self.hop.options.get("checksum_workers", 8))

        # The key hash rides along in the same scan. The row is already being
        # read, and hashing a primary key is far cheaper than hashing a whole
        # row, so the extra aggregate costs close to nothing - and it is what
        # turns "this table differs" into "rows were modified" or "rows are
        # missing" without a second pass.
        keyexpr = {}

        def _agg(h, kh):
            s = f"count(*)||'|'||{self._summed(h)}"
            if kh:
                s += f"||'|'||{self._summed(kh)}"
            return s

        def csum(side, t):
            sch, tbl = self._split(t)
            # named from the source and run on both sides, the same way
            # `_key_hash_expr` already is: identical columns in identical
            # order, and a column the target is missing fails loudly instead
            # of quietly hashing to something else
            h = self._row_hash_expr("src", db, t)
            if t not in keyexpr:
                keyexpr[t] = self._key_hash_expr("src", db, t)
            sql = f"select {_agg(h, keyexpr[t])} from {self._scope(db, t)}"
            got = self._psql(side, db,
                             f"set max_parallel_workers_per_gather ="
                             f" {self._digest_workers(db, t, w)}; {sql}")
            _kept(side, t, got, sql)
            return got

        def _kept(side, t, got, sql, salt=None, where=None):
            """One side's `rows|sum[|keys]` kept with the statement that
            summed it, for the evidence (`digests`)."""
            parts = str(got).strip().split("|")
            if len(parts) < 2:
                return
            from .. import digests
            digests.record(self.hop, db, t, side, parts[0], parts[1],
                           method=digests.OWN,
                           keys=parts[2] if len(parts) > 2 else None,
                           sql=sql, salt=salt or self.digest_salt(),
                           engine="postgres", where=where)

        # A checksum is only a SELECT, which is why nothing used to stop this
        # loop from saturating a small instance that was serving traffic.
        from ..throttle import Throttle
        from .. import checkpoint as _cp
        gate = Throttle(self.hop.workers,
                        probe=lambda: self._health("src", db))
        big = self._chunkable(db)
        cp = _cp.Checkpoint(str(self.hop.report_dir(db) / "checkpoint.json"))
        # What was proved equal last time, and the marker it carried then.
        # `--consistent` never reaches this function, so the final proof
        # cannot skip anything by construction rather than by a flag.
        from .. import unchanged as _un
        proof = _un.Proof(self.hop.report_dir(db) / "proof.json")
        skipped = []
        # Read once: a source in recovery produces the target-ahead shapes all
        # by itself, and the verdict has to say so. See `migkit.standby`.
        from .. import standby as _sb
        src_note = _sb.note(self._in_recovery("src", db),
                            self._replay_lag("src", db))
        # How many rows one chunk should cover is not a number anyone can
        # supply usefully - it depends on the row width, the indexes and how
        # busy the server is. Measure it instead.
        rate = _cp.Rate()

        def csum_range(side, t, col, lo, hi, salt):
            sch, tbl = self._split(t)
            h = self._row_hash_expr("src", db, t)
            pred = _cp.where(f'{_ident(col)}', lo, hi)
            return self._psql(side, db,
                f"set max_parallel_workers_per_gather ="
                f" {self._digest_workers(db, t, w)};"
                f" select count(*)||'|'||{self._summed(h, salt)}"
                f" from {self._scope(db, t)}"
                + (f" where {pred}" if pred else ""))

        def table_rows(side, t):
            """The table's own row count, for the moment a chunk differs and
            the range's count would otherwise be reported as the table's."""
            sch, tbl = self._split(t)
            return self._psql(side, db,
                              f"select count(*) from {self._scope(db, t)}"
                              ).strip()

        def both(fn, *args):
            """Run the source and target aggregates at the same time.

            They used to run one after the other, so every table and every
            range waited for the source scan before the target scan started -
            two full passes of wall-clock for work that has no ordering
            between the sides.

            This does not add load to the source: a unit still issues exactly
            one source query. The second thread is the target's, and the
            target is the machine nobody is serving from yet. The throttle
            therefore keeps meaning what it meant.

            **The speed-up is not measured.** On the sandbox both servers are
            containers on a two-CPU VM, so they share the cores that do the
            hashing and running them together came out at 0.96x - neutral,
            inside the noise. A real leg has the two servers on two machines,
            where the work genuinely overlaps, but that is reasoning and not a
            number, and there is no honest way to produce the number here. The
            change is kept because removing an artificial ordering between two
            independent queries is right regardless; if anyone measures it on
            separate hosts, put the figure in this docstring.
            """
            with ThreadPoolExecutor(max_workers=2) as two:
                fa = two.submit(fn, "src", *args)
                fb = two.submit(fn, "dst", *args)
                return fa.result(), fb.result()

        def one_chunked(t, col):
            """Same answer as one pass, but restartable.

            The checksum is a commutative sum over numeric, so the per-range
            sums add up to the whole-table value exactly. Completed ranges are
            persisted, and a rerun only pays for what it still owes.
            """
            sch, tbl = self._split(t)
            try:
                bounds = self._psql("src", db,
                    f'select coalesce(min({_ident(col)}),0)||chr(124)||'
                    f'coalesce(max({_ident(col)}),0) from {_ident(sch)}.{_ident(tbl)}').strip()
                lo, hi = (int(x) for x in bounds.split("|"))
            except Exception as e:
                return f"{t}: ERROR {str(e).splitlines()[-1][:80]}"
            chunk = cp.chunk_for(t, rate.chunk_rows())
            ranges = _cp.plan_ranges(lo, hi, chunk)
            expr = self._row_hash_expr("src", db, t)
            # the ranges' sums are of the rows the hop's row filter takes:
            # partials taken under another filter are not added in
            pred = self.hop.row_filter(db, sch, tbl)
            todo = cp.begin(t, expr + (f" where {pred}" if pred else ""),
                            ranges)
            done_before = cp.resumed(t)
            # the salt the stored partials were taken with, so they add up
            salt = cp.salt(t, self.digest_salt())
            for rlo, rhi in todo:
                with gate.unit():
                    started = time.monotonic()
                    try:
                        a, b = both(csum_range, t, col, rlo, rhi, salt)
                    except RuntimeError as e:
                        # partials already recorded survive for the next run
                        return f"{t}: ERROR {str(e).splitlines()[-1][:80]}"
                    rate.observe(int(a.split("|")[0]),
                                 time.monotonic() - started)
                if a != b:
                    cp.clear(t)
                    pred = _cp.where(col, rlo, rhi) or "whole table"
                    # `a` and `b` are this range's `rows|checksum`, and the
                    # row counts ride out of here into the counts check -
                    # so the range's count would be reported as the table's.
                    # Measured on 10,000,000 rows split into five chunks:
                    # `counts postgres: DIFF public.bench_rows src=2000000
                    # dst=1999992` about a table holding ten million. The
                    # checksums stay range-scoped, which is what `where`
                    # says; the counts are the table's, which is what
                    # anybody reading `src=` means by it.
                    rows_a, rows_b = both(table_rows, t)
                    ranged = (f"select count(*)||'|'||"
                              f"{self._summed(expr, salt)}"
                              f" from {self._scope(db, t)}"
                              + (f" where {pred}" if pred != "whole table"
                                 else ""))
                    _kept("src", t, a, ranged, salt, pred)
                    _kept("dst", t, b, ranged, salt, pred)
                    return (f"{t}: DIFF src={rows_a}|{a.split('|', 1)[1]}"
                            f" dst={rows_b}|{b.split('|', 1)[1]}"
                            f" where {pred}")
                cp.record(t, rlo, rhi, *a.split("|", 1))
            rows, total = cp.total(t)
            cp.clear(t)
            # the ranges' sums add up to the whole table's, so the one
            # statement over the whole table answers the same two numbers
            whole = (f"select count(*)||'|'||{self._summed(expr, salt)}"
                     f" from {self._scope(db, t)}")
            for side in ("src", "dst"):
                _kept(side, t, f"{rows}|{total}", whole, salt)
            resumed = (f" resumed={done_before}/{len(ranges)}"
                       if done_before else "")
            return (f"{t}: OK rows={rows} checksum={total}"
                    f" chunks={len(ranges)} chunk_rows={chunk:,}{resumed}")

        def _kind(a, b):
            from ..verdict import difference_kind
            pa, pb = a.split("|"), b.split("|")
            k = difference_kind(pa[0], pa[2] if len(pa) > 2 else None,
                                pb[0], pb[2] if len(pb) > 2 else None)
            if not k:
                return ""
            note = _sb.caveat(k, src_note)
            return f" kind={k}" + (f" -- {note}" if note else "")

        # every table's marker on each side in one statement, read before
        # any table is: a marker is never newer than the evidence it stands
        # for (audit 2026-10-01, SC4)
        try:
            marked = (self._change_markers("src", db),
                      self._change_markers("dst", db))
        except Exception:  # noqa: BLE001 - no marker never skips
            marked = ({}, {})

        def markers(t):
            """(src, dst) change markers, or (None, None) if either is
            untrustworthy - which never skips."""
            sch, tbl = self._split(t)
            name = f"{sch}.{tbl}"
            return marked[0].get(name), marked[1].get(name)

        def one(t):
            src_m, dst_m = markers(t)
            if may_skip and _un.skippable(proof.get(t), src_m, dst_m):
                skipped.append(t)
                # deliberately not "OK": nothing was read this run, and a
                # line that looks like a fresh verdict would be a lie about
                # when the evidence was taken
                return (f"{t}: UNCHANGED since it was last proved equal"
                        f" (no rows read this run)")
            if t in big:
                line = one_chunked(t, big[t][1])
            else:
                with gate.unit():
                    try:
                        a, b = both(csum, t)
                    except RuntimeError as e:
                        return f"{t}: ERROR {str(e).splitlines()[-1][:80]}"
                if a == b:
                    line = (f"{t}: OK rows={a.split('|')[0]}"
                            f" checksum={a.split('|')[1]}")
                else:
                    line = f"{t}: DIFF src={a} dst={b}{_kind(a, b)}"
            if ": OK" in line:
                # the markers are the ones read *before* the scan, so they
                # can never be newer than the evidence they stand for
                proof.set(t, src_m, dst_m)
            else:
                # a marker against a table that does not match is evidence of
                # the wrong thing; keeping it would let the next run skip a
                # table already known to be wrong
                proof.drop(t)
            return line

        lines, rc = [], 0
        with ThreadPoolExecutor(max_workers=self.hop.workers) as pool:
            for line in pool.map(one, tables):
                lines.append(line)
                if stream:
                    stream(line)
                # say what a failure is rather than inferring it from the
                # absence of the word OK: an UNCHANGED line is neither, and
                # treating it as a failure made a fully quiet run exit 1
                if ": DIFF" in line or ": ERROR" in line:
                    rc = 1
        try:
            proof.save()
        except Exception:
            pass                      # a store we cannot write just means
                                      # the next run reads everything again
        if src_note:
            lines.append(f"# source read while {src_note}; a source behind"
                         f" its writer shows fewer rows than the target and"
                         f" reads as rows-extra")
        if skipped:
            lines.append(f"# {len(skipped)} of {len(tables)} tables unchanged"
                         f" since their last proof and not read this run;"
                         f" `check --consistent` reads every table")
        note = gate.line()
        if note:
            lines.append(f"# {note}")
            if stream:
                stream(f"# {note}")
        self._last_throttle = gate.summary()
        return rc, "\n".join(lines)

    @staticmethod
    def _parse_fast(out):
        import re as _re
        rows_src = rows_dst = n = 0
        bad = []
        for line in out.splitlines():
            m = _re.match(r"(\S+): OK rows=(\d+)", line)
            if m:
                n += 1
                rows_src += int(m.group(2))
                rows_dst += int(m.group(2))
                continue
            m = _re.match(r"(\S+): DIFF src=(\d+)\|\S+ dst=(\d+)\|\S+", line)
            if m:
                n += 1
                a, b = int(m.group(2)), int(m.group(3))
                rows_src += a
                rows_dst += b
                if a != b:
                    bad.append(f"{m.group(1)} src={a} dst={b}")
        return n, rows_src, rows_dst, bad

    #: Tables this role reads through a policy. Measured across all four
    #: role shapes rather than assumed from the docs: a superuser and a
    #: BYPASSRLS role always see everything, an owner sees everything until
    #: the table is `FORCE`d and then does not, and any other role is
    #: filtered either way. Checking only `rolsuper or rolbypassrls` - which
    #: is what the deep check used to do - calls every owner filtered and
    #: cries wolf on a healthy database.
    RLS_FILTERED = (
        "select n.nspname||'.'||c.relname"
        " from pg_class c join pg_namespace n on n.oid = c.relnamespace"
        " where c.relkind = 'r' and c.relrowsecurity"
        " and not (select bool_or(rolsuper or rolbypassrls) from pg_roles"
        "          where rolname = current_user)"
        " and (c.relforcerowsecurity"
        "      or not pg_has_role(current_user, c.relowner, 'USAGE'))"
        " and n.nspname not in ('pg_catalog','information_schema')"
        " and n.nspname not like 'pg\\_%'"
        " and n.nspname not like '\\_\\_%'"
        " order by 1")

    #: What `assess` asks before anything moves. Each of these is about
    #: the move that is about to happen rather than the one that did.
    PREFLIGHT = ("_capacity_gaps", "_temporal_meaning", "_time_zone_rules",
                 "_collation_versions", "_mojibake")

    LARGE_OBJECT_COUNT = "select count(*) from pg_largeobject_metadata"

    #: Every user column that could hold a large object reference, and how
    #: it is known for one: by its type - `oid`, or a domain over it such as
    #: the `lo` extension's, what `vacuumlo` looks at - or, an integer
    #: column, by its values (`_by_value`): the type is no signal there, and
    #: `vacuumlo` deletes the objects such a column names (problems D11).
    OID_COLUMNS = (
        "select n.nspname||'.'||c.relname||chr(9)||a.attname||chr(9)||"
        " case when a.atttypid = 'oid'::regtype or t.typbasetype ="
        " 'oid'::regtype then 'oid' else 'integer' end"
        " from pg_attribute a"
        " join pg_class c on c.oid = a.attrelid"
        " join pg_namespace n on n.oid = c.relnamespace"
        " join pg_type t on t.oid = a.atttypid"
        " where (a.atttypid in ('oid'::regtype, 'int2'::regtype,"
        " 'int4'::regtype, 'int8'::regtype)"
        " or t.typbasetype = 'oid'::regtype) and a.attnum > 0"
        " and not a.attisdropped and c.relkind = 'r'"
        " and n.nspname not in ('pg_catalog','information_schema')"
        " and n.nspname not like 'pg\\_%'"
        " and n.nspname not like '\\_\\_%'"
        f" and c.relname not in {OWN_SQL}"
        " order by 1")

    @staticmethod
    def _as_oid(col):
        """A column's value as the oid it would name, NULL where it cannot
        name one: an integer outside an oid's range is no reference, and
        cast as one it would stop the statement."""
        return (f"(case when {col}::bigint between 1 and 4294967295 then"
                f" {col}::bigint end)::oid")

    #: a large object is read and written this much at a time: never
    #: held whole, whatever its size (backlog R10)
    LO_PIECE = 8 * 2 ** 20

    def carry_database_objects(self, db, log=None):
        """Every large object the source holds, onto the target under the
        same oid - so each reference a copied row holds still resolves -
        read and written a piece at a time (`lo_get` and `lo_put` with an
        offset), owner given where the target has the role. One the target
        already holds is compared a piece at a time and written again only
        where a piece differs. The table copier carried the references and
        not the objects: the deep check found every one dangling."""
        import psycopg2
        target = self._d("dst", db)
        src = self._conn("src", db)
        with src.cursor() as cur:
            cur.execute("select oid, pg_get_userbyid(lomowner) from"
                        " pg_largeobject_metadata order by oid")
            rows = cur.fetchall()
        if not rows:
            src.close()
            return None
        dst = self._conn("dst", target)
        dst.autocommit = False
        have = set()
        with dst.cursor() as cur:
            cur.execute("select oid from pg_largeobject_metadata")
            have = {int(r[0]) for r in cur.fetchall()}
            cur.execute("select rolname from pg_roles")
            roles = {r[0] for r in cur.fetchall()}
        made = same = rewritten = 0
        try:
            for oid, owner in rows:
                oid = int(oid)
                with src.cursor() as sc, dst.cursor() as dc:
                    if oid in have:
                        if self._lo_same(sc, dc, oid):
                            same += 1
                            continue
                        dc.execute("select lo_unlink(%s)", (oid,))
                        rewritten += 1
                    else:
                        made += 1
                    dc.execute("select lo_create(%s)", (oid,))
                    off = 0
                    while True:
                        sc.execute("select lo_get(%s, %s, %s)",
                                   (oid, off, self.LO_PIECE))
                        piece = sc.fetchone()[0]
                        piece = bytes(piece) if piece is not None else b""
                        if piece:
                            dc.execute("select lo_put(%s, %s, %s)",
                                       (oid, off, psycopg2.Binary(piece)))
                        if len(piece) < self.LO_PIECE:
                            break
                        off += len(piece)
                    if owner in roles:
                        dc.execute(f'alter large object {oid} owner to'
                                   f' {_ident(owner)}')
                dst.commit()
        finally:
            src.close()
            dst.close()
        if log:
            log(f"large objects: {made} carried, {rewritten} written again,"
                f" {same} already the same")
        return made + rewritten

    def _lo_same(self, sc, dc, oid):
        """Whether one large object is the same on both sides, a piece at
        a time: its digest per piece, never the whole of it."""
        off = 0
        while True:
            q = "select md5(lo_get(%s, %s, %s)), length(lo_get(%s, %s, %s))"
            args = (oid, off, self.LO_PIECE) * 2
            sc.execute(q, args)
            dc.execute(q, args)
            a, b = sc.fetchone(), dc.fetchone()
            if a != b:
                return False
            if (a[1] or 0) < self.LO_PIECE:
                return True
            off += self.LO_PIECE

    def _large_objects(self, db):
        """Compare the objects themselves, not only the integers that name
        them.

        A column is treated as a large object reference only when the
        **source** resolves it. Plenty of `oid` columns hold something else
        - a `regclass`, a type oid - and those resolve on neither side;
        measured, a column holding `'refs'::regclass::oid` resolved 0 rows
        while a real document column resolved 1, which is the whole
        difference between a finding and a false alarm.

        An integer column is one by its values (problems D11): where at
        least half of what it holds names an object on the source. An id
        column counting past the first object's oid names a few of them by
        chance - one in hundreds - and a column of references names nearly
        all; half keeps the two apart. What it names and the target does
        not is counted beside what already named nothing on the source.

        And the objects themselves are compared, every one both sides hold,
        by the digest of each piece (`_lo_contents`): an object the target
        holds under the same oid with other bytes is a document that did
        not arrive.
        """
        dangling, columns, by_value = [], 0, 0
        contents = None
        try:
            src_total = int(self._psql("src", db,
                                       self.LARGE_OBJECT_COUNT).strip() or 0)
            dst_total = int(self._psql("dst", db,
                                       self.LARGE_OBJECT_COUNT).strip() or 0)
            for line in self._psql("src", db, self.OID_COLUMNS).splitlines():
                parts = line.split("\t")
                if len(parts) != 3:
                    continue
                table, col, kind = parts
                if kind == "integer" and not src_total:
                    continue          # no object for an integer to name
                sch, tbl = self._split(table)
                qt, qc = f'{_ident(sch)}.{_ident(tbl)}', self._quote_ident(col)
                ref = f"t.{qc}" if kind == "oid" else self._as_oid(f"t.{qc}")
                names = ("exists (select 1 from pg_largeobject_metadata m"
                         f" where m.oid = {ref})")
                got = self._psql("src", db, (
                    f"select count(t.{qc}), count(*) filter (where {names})"
                    f" from {qt} t")).strip().split("|")
                filled, on_source = int(got[0] or 0), int(got[1] or 0)
                if not on_source:
                    continue          # not a large object column at all
                if kind == "integer" and 2 * on_source < filled:
                    continue          # an id that names a few by chance
                columns += 1
                by_value += kind == "integer"
                broken = (f"select count(*) from {qt} t where t.{qc} is not"
                          f" null and not {names}")
                try:
                    missing = int(self._psql("dst", db,
                                             broken).strip() or 0)
                except Exception:
                    continue          # the table is not on the target; the
                                      # schema check owns that finding
                if kind == "integer":
                    # what named nothing on the source names nothing there
                    missing -= filled - on_source
                if missing > 0:
                    dangling.append((f"{table}.{col}"
                                     + (" (an integer column)"
                                        if kind == "integer" else ""),
                                     missing, on_source))
            if src_total and dst_total:
                contents = self._lo_contents(db)
        except Exception as e:
            return Result("deep", f"{db} large objects", "error",
                          "could not compare the large objects:"
                          f" {str(e).splitlines()[-1][:90]}")
        return self._large_object_result(
            db, src_total, dst_total, dangling, columns,
            "carry them explicitly - pg_dump skips large objects whenever"
            " -s, -n or -t is used, and -b puts them back",
            contents=contents, by_value=by_value)

    def _lo_contents(self, db):
        """(objects compared, [oid whose bytes differ]) over every large
        object both sides hold: each read a piece at a time on each server
        (`LO_PIECE`), its digest and length compared - some hundreds of
        objects a statement, so a database of small documents is asked in
        a few round trips, and a large one is never held whole."""
        src = self._conn("src", db)
        dst = self._connect("dst", db)
        try:
            oids = []
            with src.cursor() as cur:
                cur.execute("select oid::bigint from pg_largeobject_metadata"
                            " order by oid")
                oids = [int(r[0]) for r in cur.fetchall()]
            with dst.cursor() as cur:
                cur.execute("select oid::bigint from pg_largeobject_metadata")
                there = {int(r[0]) for r in cur.fetchall()}
            todo = [o for o in oids if o in there]
            compared, differ, off = len(todo), [], 0
            while todo:
                a = self._lo_pieces(src, todo, off)
                b = self._lo_pieces(dst, todo, off)
                more = []
                for o in todo:
                    if a.get(o) != b.get(o):
                        differ.append(o)
                    elif (a.get(o) or (None, 0))[1] >= self.LO_PIECE:
                        more.append(o)
                todo, off = more, off + self.LO_PIECE
            return compared, differ
        finally:
            src.close()
            dst.close()

    def _lo_pieces(self, conn, oids, off):
        """{oid: (md5, length)} of one piece of each of these objects."""
        out = {}
        with conn.cursor() as cur:
            for i in range(0, len(oids), 500):
                cur.execute(
                    "select o, md5(p), length(p) from (select o,"
                    " lo_get(o::oid, %s, %s) p from unnest(%s::bigint[]) o)"
                    " x", (off, self.LO_PIECE, oids[i:i + 500]))
                out.update({int(o): (h, int(n or 0))
                            for o, h, n in cur.fetchall()})
        conn.rollback()
        return out

    def _content_fingerprint(self, side, db, relation):
        """`count|checksum` for everything in one relation.

        The expression comes from the **source** and is applied to both
        sides, so a target whose columns sit in a different order still
        compares equal - see `_row_hash_expr`. A count alone would miss a
        copy that is the right size and the wrong contents, which is
        exactly the shape a stale materialized view and a half-restored
        extension table both have.
        """
        h = self._row_hash_expr("src", db, relation)
        return self._psql(side, db,
                          f"select count(*)||'|'||{self._summed(h)}"
                          f" from {relation} t")

    #: Tables an extension declared as its own configuration data. PostGIS
    #: registers `spatial_ref_sys` this way, which is why a custom SRID is
    #: dumped at all - and why a target that already has the stock table
    #: silently keeps its own rows instead of the ones being restored.
    EXTENSION_DATA = (
        "select e.extname||chr(9)||n.nspname||'.'||c.relname"
        " from pg_extension e"
        " cross join lateral unnest(e.extconfig) as cfg(oid)"
        " join pg_class c on c.oid = cfg.oid"
        " join pg_namespace n on n.oid = c.relnamespace"
        " order by 1")

    def _extension_data(self, db):
        """Extensions do not only bring functions - some bring rows.

        The extension list and its versions were already compared. What was
        not is the data those extensions own: PostGIS keeps coordinate
        systems in `spatial_ref_sys`, and a custom SRID lives there beside
        the several thousand stock ones. `pg_upgrade` has been reported
        failing with `Cannot find SRID (4283) in spatial_ref_sys` for
        exactly this reason, and a restored row does not overwrite an
        existing one, so the target keeps its stock copy and the geometry
        that needed the custom entry stops working.
        """
        findings, checked = [], 0
        try:
            # an extension's table by its own name, as the hop's scope
            # reads every table
            rows = [l.split("\t") for l in
                    self._psql("src", db, self.EXTENSION_DATA).splitlines()
                    if l.count("\t") == 1
                    and self.in_scope(db, l.split("\t")[1])]
            for ext, table in rows:
                checked += 1
                try:
                    a = self._content_fingerprint("src", db, table)
                    b = self._content_fingerprint("dst", db, table)
                except Exception:
                    findings.append((ext, table, "missing on target"))
                    continue
                if a != b:
                    findings.append((ext, table, f"src={a} dst={b}"))
        except Exception as e:
            return Result("deep", f"{db} extension data", "error",
                          "could not compare the data extensions own:"
                          f" {str(e).splitlines()[-1][:90]}")
        return self._extension_data_result(
            db, findings, checked,
            "copy the rows the extension owns yourself - a restore does not"
            " overwrite the ones the target already has, so the stock table"
            " wins and the custom entries are simply absent")

    def _filtered_tables(self, side, db):
        try:
            out = self._psql(side, db, self.RLS_FILTERED)
        except Exception:
            return None
        return self._scoped(db, out)

    @staticmethod
    def _failed_tables(out):
        """The tables the fast pass could not read at all. One place,
        because `check_data` and the counts derived from the same output
        both have to know - and only one of them used to."""
        return [l.split(":")[0] for l in out.splitlines() if ": ERROR" in l]

    def _counts_from_fast(self, db, out):
        """Counts ride along with the checksum pass rather than scanning
        again, which means they can only speak for the tables that pass
        managed to read.

        Measured with a role holding a column-level grant, so `select *`
        was refused on the only table in the database: the checksum pass
        errored on it, nothing was left to count, and this reported `OK 0
        tables, rows 0==0`. A clean verdict computed over nothing is the
        same shape as a mover reporting success into an empty target, which
        `moved_nothing` already refuses - so it is refused here too.

        An empty database still reports ok, because there genuinely is
        nothing to count and saying otherwise would cry wolf. The two cases
        are told apart by whether any table exists on both sides.
        """
        n, rows_src, rows_dst, bad = self._parse_fast(out)
        # the tables the hop takes, as the pass these counts come from
        # reads them: one it leaves out, on one side only, is the target's
        st = {t for t in self._psql("src", db, self.USER_TABLES).splitlines()
              if t and self._keep_tbl(db, t)}
        dt = {t for t in self._psql("dst", db, self.USER_TABLES).splitlines()
              if t and self._keep_tbl(db, t)}
        bad += [f"{t} missing on target" for t in sorted(st - dt)]
        bad += [f"{t} extra on target" for t in sorted(dt - st)]
        if bad:
            return Result("counts", db, "diff", clauses(bad), "",
                          "missing rows show up in check data, fix there")
        failed = self._failed_tables(out)
        if failed:
            return Result(
                "counts", db, "error",
                f"counted {n} tables; {len(failed)} could not be read by the"
                f" pass these counts come from: {', '.join(failed[:5])}"
                + (" ..." if len(failed) > 5 else "")
                + " - so this is not a count of the database", "",
                "see the data check for the error itself; a column-level"
                " grant is one cause, and it reports the table rather than"
                " the column")
        shared = st & dt
        if not n and shared:
            return Result(
                "counts", db, "error",
                f"{len(shared)} tables exist on both sides and none of them"
                " was counted - the pass these counts come from returned"
                " nothing to add up", "",
                "run the data check: whatever stopped it stopped this too")
        if not shared:
            return Result("counts", db, "ok",
                          "no tables on both sides to count")
        return self._honest_about_filtering(
            Result("counts", db, "ok",
                   f"{n} tables, rows {rows_src:,}=={rows_dst:,}"
                   " (from the checksum pass, no extra scan)"), db)

    def _drilldown_native(self, db, table):
        """Find the differing pks of one table (whole-table for normal
        sizes, PK-index slices for big single-int-pk tables so pgsql_tmp
        never fills). Writes data-<table>.missing/.extra/.changed."""
        cols = self._pk_cols_of(db, table)
        if not cols:
            return None
        sch, tbl = self._split(table)
        scoped = self._scope(db, table)
        pkexpr = self._pk_text_expr(cols)

        def fetch(side, where=""):
            out = {}
            h = self._row_hash_expr("src", db, f"{sch}.{tbl}")
            for l in self._psql(side, db,
                                f"select {pkexpr}||'|'||{h}"
                                f" from {scoped} {where}").splitlines():
                k, _, hsh = l.rpartition("|")
                out[k] = hsh
            return out

        n = int(self._psql("src", db, f"select count(*) from {scoped}") or 0)
        self.__dict__.setdefault("_drilled_rows", {})[(db, table)] = n
        intpk = self._int_pk(db, sch, tbl)

        def whole():
            missing, extra, changed = [], [], []
            if n > self.hop.slice and intpk:
                mm = self._psql("src", db,
                                f'select coalesce(min({_ident(intpk)}),0)||\'|\'||'
                                f'coalesce(max({_ident(intpk)}),0) from {scoped}')
                lo, hi = (int(x) for x in mm.split("|"))
                step = max(1, (hi - lo) // max(1, n // self.hop.slice) + 1)
                for a in range(lo, hi + 1, step):
                    w = f'where {_ident(intpk)} >= {a} and {_ident(intpk)} < {a + step}'
                    s, dd = fetch("src", w), fetch("dst", w)
                    missing += [k for k in s if k not in dd]
                    extra += [k for k in dd if k not in s]
                    changed += [k for k in s if k in dd and s[k] != dd[k]]
                return missing, extra, changed
            s, dd = fetch("src"), fetch("dst")
            return ([k for k in s if k not in dd], [k for k in dd if k not in s],
                    [k for k in s if k in dd and s[k] != dd[k]])

        def walk(src_where, dst_where):
            s, dd = fetch("src", f"where {src_where}"), fetch(
                "dst", f"where {dst_where}")
            return ([k for k in s if k not in dd], [k for k in dd if k not in s],
                    [k for k in s if k in dd and s[k] != dd[k]])

        def same(src_where, dst_where):
            return (self._digest_where("src", db, table, src_where)
                    == self._digest_where("dst", db, table, dst_where))

        # a large table's walk is chosen: its differing groups, or all of it
        missing, extra, changed = (
            self._narrowed_walk(db, table, table, cols, walk, same, whole, n)
            if n > self.hop.slice else whole())
        self._write_pk_files(db, table, sorted(missing), sorted(extra),
                             sorted(changed))
        return len(missing), len(extra), len(changed)

    def check_data(self, db, table=None, stream=None, with_counts=False,
                   consistent=False):
        if table and self.through_pair(db, table):
            return self.columns_pair().check_data(db, table, stream)
        got = self._own_check_data(db, table, stream, with_counts, consistent)
        if table:
            return got
        return got + self._mapped_data(
            db, stream, " - read as it is now, not inside the consistent"
                        " snapshot" if consistent else "")

    def _own_check_data(self, db, table=None, stream=None, with_counts=False,
                        consistent=False):
        if table:
            r = self._drilldown_native(db, table)
            status = "ok" if r and not any(r) else "diff" if r else "error"
            detail = (f"missing={r[0]} extra={r[1]} changed={r[2]}"
                      if r else "no primary key for row-level compare")
            return [Result("data", f"{db} {table}", status, detail,
                           str(self._report(db)),
                           f"migkit sync {self.hop.name} --db {db} --kind rows",
                           table=str(table),
                           rows_apart={str(table): {
                               "missing": r[0], "extra": r[1],
                               "changed": r[2], "exact": True}}
                           if r and any(r) else {})]
        if not consistent:
            # pass D: the leaves the tail keeps, where the hop keeps them
            from .. import kept
            got = kept.proved_db(self, db, stream, with_counts)
            if got:
                return got
        if consistent:
            rc, out = self._fast_consistent(db)
            if stream:
                for line in out.splitlines():
                    stream(line)
        else:
            # when the row counts are merged out of this pass, every table
            # has to be read or the count is short by whatever was skipped
            rc, out = self._data_fast_native(db, stream=stream,
                                             may_skip=not with_counts)
        ev = self.hop.report_dir(db) / "data-evidence.txt"
        ev.write_text(out + "\n")
        pre = [self._counts_from_fast(db, out)] if with_counts else []
        mode = ""
        if consistent:
            import re as _re
            held = _re.search(r"source snapshot held ([\d,]+)s", out)
            mode = (f"consistent snapshot, held on the source"
                    f" {held.group(1)}s, " if held else "consistent snapshot, ")
        outcomes = self._fast_outcomes(out)
        if rc == 0:
            import re as _re
            rows = sum(int(m) for m in _re.findall(r"rows=(\d+)", out))
            n = out.count(": OK")
            unchanged = out.count(": UNCHANGED")
            extra = (f", {unchanged} unchanged since their last proof and"
                     f" not read this run" if unchanged else "")
            return pre + [self._honest_about_filtering(
                Result("data", db, "ok",
                       f"{mode}{n} tables, {rows:,} rows,"
                       f" checksums equal both sides{extra}",
                       str(ev), tables=outcomes), db)]
        bad = [l.split(":")[0] for l in out.splitlines() if ": DIFF" in l]
        err = self._failed_tables(out)
        drilled = {t: self._drilldown_native(db, t) for t in bad}
        if bad:
            still, healed, how = self._resolve_inflight(db, bad, stream)
            outcomes.update({t: "ok" for t in bad if t not in still})
            if not still and not err:
                return pre + [Result("data", db, "ok",
                                     f"{mode}all diffs proven in-flight"
                                     " replication: " + "; ".join(how),
                                     str(ev), tables=outcomes)]
            bad = still
        detail = ""
        if bad:
            detail = f"tables differ: {', '.join(bad)} (pk-level files written)"
            # each table's rows counted key by key, as its files hold them
            # now - after the fence took back what was still arriving: the
            # shape a hop that keeps some changes off the target is held
            # to (`verdict.hold`)
            counted = [f"{t} " + " ".join(
                f"{k}={len(self._drill_keys(db, t, k))}"
                for k in ("missing", "extra", "changed"))
                for t in bad if drilled.get(t) is not None]
            if counted:
                detail += "; rows counted: " + ", ".join(counted)
            fps = []
            for t in bad[:5]:
                cols = self._column_fingerprint(db, t)
                if cols:
                    fps.append(f"{t} -> {', '.join(cols[:6])}")
            if fps:
                detail += "; drift localized to columns: " + "; ".join(fps)
            whose = []
            began = self._move_began(db)
            for t in bad[:5]:
                keys = self._drill_keys(db, t, "extra")
                try:
                    said = self.who_wrote(db, t, keys, began) if keys else ""
                except RuntimeError:
                    said = ""
                if said:
                    whose.append(f"{t}: {said}")
            if whose:
                detail += "; " + "; ".join(whose)
            detail += "".join(self._second_notes(db, t) for t in bad[:5])
        if err:
            detail += f" errors: {', '.join(err)}"
        stopped = [l for l in out.splitlines() if l.startswith("stopped: ")]
        if stopped:
            detail = (detail + " " + stopped[0]).strip()
        apart = {t: {k: len(self._drill_keys(db, t, k))
                     for k in ("missing", "extra", "changed")}
                 for t in bad if drilled.get(t) is not None}
        for got in apart.values():
            got["exact"] = True
        return pre + [Result("data", db, "diff" if bad else "error", detail,
                             str(self._report(db)),
                             f"migkit sync {self.hop.name} --db {db} --kind rows",
                             tables=outcomes, rows_apart=apart)
                      ] + [r for r in (self._second_rows(db, t)
                                       for t in bad[:5]) if r]

    @staticmethod
    def _fast_outcomes(out):
        """{table: ok | diff | error} of the fast pass's lines: a table
        unchanged since it was last proved equal is equal."""
        got = {}
        for line in str(out).splitlines():
            table, sep, rest = line.partition(": ")
            word = rest.split(" ", 1)[0] if sep else ""
            status = {"OK": "ok", "UNCHANGED": "ok", "DIFF": "diff",
                      "ERROR": "error"}.get(word)
            if status and table and " " not in table:
                got[table] = status
        return got

    def _named_rows(self, db, table):
        """The keys the drilldown named for a table, each as the tuple of
        its key's values as text."""
        return [tuple(self._pk_unescape(p) for p in k.split("\t"))
                for kind in ("missing", "extra", "changed")
                for k in self._drill_keys(db, table, kind)]

    def _key_where(self, db, table, keys):
        """A predicate selecting these named rows, in the key's own text."""
        pkexpr = self._pk_text_expr(self._pk_cols_of(db, table))
        wanted = ", ".join(_literal(k) for k in keys)
        return f"{pkexpr} in ({wanted})"

    def _second_notes(self, db, table):
        """What narrowing and the column-by-column explanation found for a
        differing table, as clauses of its finding."""
        said = ""
        note = self._narrowed_note(db, table)
        if note:
            said += f"; {table}: {note}"
        keys = [k for kind in ("missing", "extra", "changed")
                for k in self._drill_keys(db, table, kind)]
        said += self._explained(db, table, self._pk_cols_of(db, table),
                                keys, self._key_where(db, table, keys)
                                if keys else "")
        return said

    def _second_rows(self, db, table):
        """The second reading of a differing table's rows, or None."""
        cols = self._pk_cols_of(db, table)
        if not cols:
            return None
        from .. import canon
        types = dict(self.neutral_columns("src", db, table))
        classes = [canon.type_class("postgres", types.get(c)) for c in cols]
        columns = [(n, canon.type_class("postgres", t)) for n, t in
                   types.items()]
        rows = (self.__dict__.get("_drilled_rows") or {}).get((db, table), 0)
        from .. import second_reader as sr
        fixed = [n for n, t in types.items() if sr.padded(t)]
        return self._second_reading(db, table, table, cols, classes,
                                    self._named_rows(db, table), rows,
                                    columns, fixed)

    def _ignore_patterns(self):
        import re as _re
        from ..config import CONF
        pats = []
        for d in (Path(CONF).parent, PGDC_ROOT / "conf"):
            f = d / f"{self.hop.name}.schema-ignore"
            if f.exists():
                pats += [p for p in f.read_text().splitlines() if p.strip()]
        return [_re.compile(p) for p in pats]

    #: A single field cannot exceed 1 GB in PostgreSQL - bytea, text and
    #: json alike. Measured guidance from the field puts the practical
    #: ceiling lower still, around 500 MB without binary transfer.
    PG_FIELD_LIMIT = 1024 ** 3

    #: Types whose values can be stored out of line, which is what makes
    #: them behave like the LOBs a mover has a mode for.
    LOB_TYPES = ("bytea", "text", "json", "jsonb", "xml",
                 "character varying")

    def _lob_columns(self, db):
        """Every column whose type can hold a value bigger than a mover's
        LOB limit.

        No cleverness in the filter, and that is deliberate. The obvious
        shortcut - only look at tables whose TOAST relation holds data -
        was measured to have a hole in it: a 1,000,000-byte text value
        compressed to 11,452 bytes on the way in, so a filter reading
        stored sizes would have passed over the very value a limited LOB
        mode would truncate. And the shortcut buys nothing:
        `max(octet_length(col))` over two columns of a 2,000,000-row,
        531 MB table answered in 0.25 s.
        """
        rows = self._psql("src", db,
            "select n.nspname||'.'||c.relname||chr(9)||a.attname"
            "||chr(9)||t.typname"
            " from pg_class c"
            " join pg_namespace n on n.oid = c.relnamespace"
            " join pg_attribute a on a.attrelid = c.oid and a.attnum > 0"
            " join pg_type t on t.oid = a.atttypid"
            " where c.relkind = 'r' and not a.attisdropped"
            " and n.nspname not in ('pg_catalog','information_schema')"
            " and n.nspname not like 'pg\\_%'"
            f" and t.typname = any(array{list(self.TOAST_TYPES)}::name[])"
            " order by 1")
        out = []
        for line in self._scoped(db, rows, "\t"):
            parts = line.split("\t")
            if len(parts) == 3:
                out.append((parts[0], parts[1], parts[2]))
        return out

    #: the catalog spells these differently from `information_schema`
    TOAST_TYPES = ("bytea", "text", "json", "jsonb", "xml", "varchar")

    def _lob_sizes(self, db, probes=None):
        """The biggest value per wide column, on each side.

        `octet_length` and not `pg_column_size`: the first is the size of
        the value, which is what a mover's limit is compared against, and
        the second is the size PostgreSQL stored after compressing it -
        measured, 1,000,000 against 11,452 for the same value.

        Each column a read planned (`probes`, `util.Probes`): one that could
        not be read on either side is said, where it was left out and the
        check said ok over the rest; a table the target does not have yet
        is its own answer (audit 2026-10-01, LG2).
        """
        from ..util import Probes
        probes = probes if probes is not None else Probes("columns")
        findings = []
        for table, column, typ in self._lob_columns(db):
            name = f"{table}.{column}"
            probes.plan(name)
            sch, tbl = self._split(table)
            col = _ident(column)
            if typ in ("json", "jsonb", "xml"):
                col += "::text"
            q = (f"select coalesce(max(octet_length({col})), 0)"
                 f" from {_ident(sch)}.{_ident(tbl)}")
            try:
                src = int(self._psql("src", db, q).strip() or 0)
            except Exception as e:  # noqa: BLE001 - said, not dropped
                probes.failed(name, e)
                continue
            dst = None
            if src:
                try:
                    dst = int(self._psql("dst", db, q).strip() or 0)
                except SqlFailed as e:
                    if e.sqlstate != "42P01":
                        probes.failed(name, e)
                        continue
                except Exception as e:  # noqa: BLE001 - said, not dropped
                    probes.failed(name, e)
                    continue
            probes.done(name)
            if src:
                findings.append((table, column, src, dst,
                                 self.PG_FIELD_LIMIT))
        return findings

    def _lob_check(self, db):
        from ..util import Probes
        probes = Probes("columns")
        try:
            findings = self._lob_sizes(db, probes)
        except Exception as e:
            return Result("deep", f"{db} lobs", "error",
                          "could not size the wide columns:"
                          f" {str(e).splitlines()[-1][:90]}")
        got = self._lob_result(
            db, findings, "1 GB PostgreSQL field limit",
            "set the mover's LOB size limit above the biggest value, or move"
            " those tables with a path that does not truncate -"
            " `migkit move --go` does not")
        got.status, got.detail = probes.verdict(got.status, got.detail)
        return got

    def _scalar(self, side, db, sql):
        out = self._psql(side, db, sql).strip()
        return out.splitlines()[0].split("|") if out else None

    def _zone_fingerprints(self, side, db):
        """`at time zone` renders the wall clock that zone shows at each
        probe instant; the md5 of those readings is the zone's behaviour.

        Measured at 78 ms for all the zones this server carries, so there
        is nothing to optimise and nothing to sample."""
        probes = ", ".join(f"(timestamptz {_literal(p + '+00')})"
                           for p in self.TZ_PROBES)
        out = self._psql(side, db,
                         "select n.name||chr(9)||md5(string_agg("
                         "(p.ts at time zone n.name)::text, ',' order by"
                         " p.ts)) from pg_timezone_names n"
                         f" cross join (values {probes}) p(ts)"
                         " group by n.name order by 1")
        got = {}
        for line in out.splitlines():
            parts = line.split("\t")
            if len(parts) == 2:
                got[parts[0]] = parts[1]
        return got

    #: Unique indexes over at least one collatable column - the ones a
    #: change in sort order can break. Partial and expression indexes are
    #: left out and counted, because grouping by their columns is not the
    #: same question they answer.
    UNIQUE_TEXT_INDEXES = (
        "select n.nspname||'.'||t.relname||chr(9)||c.relname||chr(9)"
        "||(select string_agg(quote_ident(a.attname), ',' order by k.ord)"
        "   from unnest(i.indkey) with ordinality k(attnum, ord)"
        "   join pg_attribute a on a.attrelid = i.indrelid"
        "    and a.attnum = k.attnum)"
        "||chr(9)||(i.indpred is not null or i.indexprs is not null)::int"
        "::text"
        " from pg_index i"
        " join pg_class c on c.oid = i.indexrelid"
        " join pg_class t on t.oid = i.indrelid"
        " join pg_namespace n on n.oid = t.relnamespace"
        " where i.indisunique and i.indisvalid and t.relkind = 'r'"
        " and n.nspname not in ('pg_catalog','information_schema')"
        " and n.nspname not like 'pg\\_%'"
        " and n.nspname not like '\\_\\_%'"
        f" and t.relname not in {OWN_SQL}"
        " and exists (select 1 from unnest(i.indkey) kk"
        "   join pg_attribute aa on aa.attrelid = i.indrelid"
        "    and aa.attnum = kk"
        "   where aa.atttypid in ('text'::regtype, 'varchar'::regtype,"
        "                         'bpchar'::regtype))"
        " order by 1")

    #: The planner must not be allowed to answer this question by reading
    #: the index that is under suspicion. Measured: left to itself it picks
    #: an index-only scan and reports no duplicates at all.
    NO_INDEX_PATHS = ("set enable_indexscan = off;"
                      " set enable_bitmapscan = off;"
                      " set enable_indexonlyscan = off; ")

    def _duplicate_keys(self, db, reason, sides=("src", "dst")):
        """Group by the key with the index paths shut off, and see what the
        unique index has been letting through."""
        found, checked, skipped = [], 0, 0
        if not reason:
            # nothing has accused an index, so nothing is read: the cost of
            # this check is a sequential scan per unique index
            return self._duplicate_hunt_result(db, [], 0, 0, "", "")
        try:
            for side in sides:
                label = "source" if side == "src" else "target"
                for line in self._scoped(db, self._psql(
                        side, db, self.UNIQUE_TEXT_INDEXES), "\t"):
                    parts = line.split("\t")
                    if len(parts) != 4:
                        continue
                    table, index, cols, odd = parts
                    if odd == "1":
                        skipped += 1
                        continue
                    checked += 1
                    sch, tbl = self._split(table)
                    out = self._psql(
                        side, db,
                        self.NO_INDEX_PATHS
                        + self._as_json_rows(
                            f"select {cols}, count(*) as migkit_n from"
                            f' {_ident(sch)}.'
                            f'{_ident(tbl)}'
                            f" group by {cols} having count(*) > 1"
                            f" limit {self.DUPLICATE_CAP}"))
                    groups = [l for l in out.splitlines() if l.strip()]
                    if groups:
                        found.append((label, table, index, cols, len(groups),
                                      groups[0]))
        except Exception as e:
            return Result("deep", f"{db} duplicate keys", "error",
                          "could not hunt for duplicate keys:"
                          f" {str(e).splitlines()[-1][:90]}")
        return self._duplicate_hunt_result(
            db, found, checked, skipped, reason,
            "these rows were accepted by a constraint that had stopped"
            " working - decide which copy survives, delete the rest, then"
            " REINDEX so the constraint means something again")

    #: Every text column on a real table, which is where text that was
    #: already broken before the move is hiding.
    @staticmethod
    def _as_json_rows(select):
        """`select` with each row as one JSON object, one per line. The row
        goes by a name no column has: `row_to_json(s)` over a table with a
        column `s` was read as that column, and the check stopped on `No
        function matches` (measured)."""
        return (f"select row_to_json(migkit_row) from ({select})"
                " migkit_row")

    TEXT_COLUMNS = (
        "select c.table_schema||'.'||c.table_name||chr(9)||c.column_name"
        " from information_schema.columns c"
        " join information_schema.tables t"
        " on t.table_schema = c.table_schema and t.table_name = c.table_name"
        " where t.table_type = 'BASE TABLE'"
        " and c.table_schema not in ('pg_catalog','information_schema')"
        " and c.table_schema not like 'pg\\_%'"
        " and c.table_schema not like '\\_\\_%'"
        f" and c.table_name not in {OWN_SQL}"
        " and c.data_type in ('text','character varying','character')"
        " order by 1")

    def _text_columns(self, side, db):
        """{schema.table: [text column]} on a side, of the tables the hop
        takes (`TEXT_COLUMNS`, `_scoped`): what the mojibake check samples
        on the source and its repair rewrites on the target, read one way -
        the repair read every table of the target, and with
        MIGKIT_REPAIR_TEXT updated tables the hop leaves out."""
        cols = {}
        for line in self._scoped(db, self._psql(side, db, self.TEXT_COLUMNS),
                                 "\t"):
            parts = line.split("\t")
            if len(parts) == 2:
                cols.setdefault(parts[0], []).append(parts[1])
        return cols

    def _mojibake(self, db):
        """Sample the non-ASCII rows of every text column on the source.

        `octet_length <> char_length` is exactly "this value has a character
        outside ASCII", it is the same expression on MySQL, and it lets one
        scan per *table* stand in for one per column. The rows come back as
        JSON so a value containing a tab or a newline cannot be read as two
        values - the mistake this project has already made once.
        """
        findings, scanned = [], 0
        try:
            cols = self._text_columns("src", db)
            for table, columns in sorted(cols.items()):
                sch, tbl = self._split(table)
                quoted = [_ident(c) for c in columns]
                where = " or ".join(f"octet_length({q}) <> char_length({q})"
                                    for q in quoted)
                out = self._psql(
                    "src", db, self._as_json_rows(
                        f'select {", ".join(quoted)}'
                        f' from {_ident(sch)}.'
                        f'{_ident(tbl)}'
                        f' where {where} limit {self.MOJIBAKE_SAMPLE}'))
                rows = [[json.loads(line).get(c) for c in columns]
                        for line in out.splitlines() if line.strip()]
                found, seen = self._mojibake_tally(table, columns, rows)
                findings += found
                scanned += seen
        except Exception as e:
            return Result("deep", f"{db} mojibake", "error",
                          "could not sample the source's text columns:"
                          f" {str(e).splitlines()[-1][:90]}")
        return self._mojibake_result(
            db, findings, scanned,
            "convert the column once on the source before moving it, and"
            " re-check afterwards - a second pass over already-repaired text"
            " breaks it again")

    #: A collation nothing uses cannot have broken anything, and after a real
    #: glibc upgrade *every* locale the OS ships has drifted - 873 of them on
    #: the measured image. Restricting to the ones a user column or index
    #: actually references is the difference between a finding and a wall.
    #: Every trigger a user put on the target, with the table it sits on
    #: and whether it is enabled. Internal ones are excluded: a foreign key
    #: is implemented as a pair of constraint triggers, and reporting those
    #: would bury the ones somebody wrote.
    TARGET_TRIGGERS = (
        "select n.nspname||'.'||c.relname||'.'||t.tgname||chr(9)"
        "||n.nspname||'.'||c.relname||chr(9)||t.tgenabled::text"
        " from pg_trigger t"
        " join pg_class c on c.oid = t.tgrelid"
        " join pg_namespace n on n.oid = c.relnamespace"
        " where not t.tgisinternal"
        " and n.nspname not in ('pg_catalog','information_schema')"
        " order by 1")

    COLLATION_IN_USE = (
        " and (exists (select 1 from pg_attribute a"
        "   join pg_class t on t.oid = a.attrelid"
        "   join pg_namespace n on n.oid = t.relnamespace"
        "   where a.attcollation = c.oid and a.attnum > 0"
        "   and not a.attisdropped"
        "   and n.nspname not in ('pg_catalog','information_schema'))"
        " or exists (select 1 from pg_index i"
        "   join pg_class t on t.oid = i.indrelid"
        "   join pg_namespace n on n.oid = t.relnamespace"
        "   where c.oid = any (i.indcollation)"
        "   and n.nspname not in ('pg_catalog','information_schema')))")

    #: Collations in use whose recorded version no longer matches what the
    #: operating system provides. `is distinct from` rather than `<>` so a
    #: locale that has vanished entirely (actual version NULL) is a finding
    #: and not a row that quietly drops out of the result.
    COLLATION_DRIFT = (
        "select c.collname||chr(9)||c.collversion||chr(9)"
        "||coalesce(pg_collation_actual_version(c.oid), '')"
        " from pg_collation c"
        " where c.collversion is not null and c.collversion <> ''"
        " and c.collversion is distinct from"
        " pg_collation_actual_version(c.oid)"
        + COLLATION_IN_USE + " order by 1")

    COLLATION_COUNT = ("select count(*) from pg_collation c"
                       " where c.collversion is not null"
                       " and c.collversion <> ''" + COLLATION_IN_USE)

    #: The database's own default collation, which is the one every text
    #: column without an explicit COLLATE is sorted by. PostgreSQL 15 is
    #: where it started recording this per database.
    DB_COLLATION_DRIFT = (
        "select datname||chr(9)||coalesce(datcollversion, '')||chr(9)"
        "||coalesce(pg_database_collation_actual_version(oid), '')"
        " from pg_database where datname = current_database()")

    #: Each versioned collation with a table that uses it - by a column,
    #: or by an index on the table - so a hop that takes some tables asks
    #: only about the collations those use (`_collations_taken`).
    COLLATION_USERS = (
        "select c.collname||chr(9)||n.nspname||'.'||t.relname"
        " from pg_collation c"
        " join pg_attribute a on a.attcollation = c.oid and a.attnum > 0"
        "  and not a.attisdropped"
        " join pg_class t on t.oid = a.attrelid and t.relkind not in"
        "  ('i', 'I')"
        " join pg_namespace n on n.oid = t.relnamespace"
        " where c.collversion is not null and c.collversion <> ''"
        " and n.nspname not in ('pg_catalog','information_schema')"
        " union"
        " select c.collname||chr(9)||n.nspname||'.'||t.relname"
        " from pg_index i"
        " join pg_class t on t.oid = i.indrelid"
        " join pg_namespace n on n.oid = t.relnamespace"
        " join pg_collation c on c.oid = any (i.indcollation)"
        " where c.collversion is not null and c.collversion <> ''"
        " and n.nspname not in ('pg_catalog','information_schema')")

    def _collations_taken(self, side, db):
        """The versioned collations a table the hop takes uses, on a side,
        or None for a hop that takes everything - which asks of every
        collation in use, as it always did. A collation only a table the
        hop leaves out sorts by is not the move's to report."""
        from ..config import scoped
        if not scoped(self.hop):
            return None
        return {line.split("\t", 1)[0] for line in self._psql(
                    side, db, self.COLLATION_USERS).splitlines()
                if "\t" in line
                and self.in_scope(db, line.split("\t", 1)[1])}

    def _collation_versions(self, db):
        """Both sides, because this is a property of the machine underneath
        each one - and a migration is usually the moment the two machines
        stop being the same machine."""
        drifted, missing, checked = [], [], 0
        for side in ("src", "dst"):
            label = "source" if side == "src" else "target"
            try:
                rows = self._psql(side, db, self.COLLATION_DRIFT)
                taken = self._collations_taken(side, db)
                if taken is None:
                    checked += int(self._psql(
                        side, db, self.COLLATION_COUNT).strip() or 0)
                else:
                    rows = "\n".join(l for l in rows.splitlines()
                                     if l.split("\t", 1)[0] in taken)
                    checked += len(taken)
                if int(self._psql(side, db,
                                  "select current_setting("
                                  "'server_version_num')::int").strip()
                       ) >= 150000:
                    # the database default sorts every text column that has
                    # no explicit COLLATE, so it is the one that matters most
                    default = self._psql(side, db,
                                         self.DB_COLLATION_DRIFT).strip()
                    rows += "\n" + default
                    parts = default.split("\t")
                    # a C-locale database records no version and has none to
                    # drift from, so counting it would inflate the all-clear
                    if len(parts) == 3 and parts[1]:
                        checked += 1
            except Exception as e:
                return Result("deep", f"{db} collation versions", "error",
                              f"could not read the {label}'s collation"
                              f" versions: {str(e).splitlines()[-1][:90]}")
            for line in rows.splitlines():
                parts = line.split("\t")
                if len(parts) != 3:
                    continue
                coll, stored, actual = parts
                if stored and not actual:
                    missing.append((label, coll, stored))
                elif stored != actual:
                    drifted.append((label, coll, stored, actual))
        return self._collation_version_result(
            db, drifted, missing, checked,
            "rebuild first, then refresh: REINDEX every index on a text"
            " column and only then ALTER DATABASE ... REFRESH COLLATION"
            " VERSION - refreshing first silences the warning and leaves the"
            " indexes sorted wrong")

    #: Every index on a user table, with the two flags that decide whether it
    #: is usable and whether it still costs writes. `relkind` separates a
    #: plain index from a partitioned parent, which is allowed to be invalid.
    INDEX_FLAGS = (
        "select n.nspname||'.'||t.relname||chr(9)||c.relname"
        " ||chr(9)||c.relkind::text||chr(9)||i.indisvalid::int::text"
        " ||chr(9)||i.indisready::int::text"
        " from pg_index i"
        " join pg_class c on c.oid = i.indexrelid"
        " join pg_class t on t.oid = i.indrelid"
        " join pg_namespace n on n.oid = t.relnamespace"
        " where n.nspname not in ('pg_catalog','information_schema')"
        " and n.nspname not like 'pg\\_%'"
        " and n.nspname not like '\\_\\_%'"
        f" and t.relname not in {OWN_SQL}"
        " order by 1")

    def _invalid_indexes(self, db):
        """Both sides, because an invalid index is a reason not to migrate
        yet as much as it is a failed rebuild afterwards. On the source it
        reads as an index nobody uses; on the target it is the post-load
        `CREATE INDEX CONCURRENTLY` that died and said so once."""
        broken, partial, total = [], [], {"source": 0, "target": 0}
        for side in ("src", "dst"):
            label = "source" if side == "src" else "target"
            try:
                out = self._psql(side, db, self.INDEX_FLAGS)
            except Exception as e:
                return Result("deep", f"{db} indexes", "error",
                              f"could not read the {label}'s indexes:"
                              f" {str(e).splitlines()[-1][:90]}")
            for line in self._scoped(db, out, "\t"):
                parts = line.split("\t")
                if len(parts) != 5:
                    continue
                table, index, relkind, valid, ready = parts
                if valid == "1":
                    total[label] += 1
                elif relkind == "I":
                    partial.append((label, table, index))
                else:
                    broken.append((label, table, index, ready == "1"))
        return self._invalid_index_result(
            db, broken, partial, total,
            "drop each one and build it again - `DROP INDEX CONCURRENTLY"
            " <name>` then `CREATE INDEX CONCURRENTLY`, with"
            " maintenance_work_mem and max_parallel_maintenance_workers"
            " raised for the rebuild")

    def rows_present(self, db, tables):
        got = set()
        for name in tables:
            sch, tbl = self._split(name)
            q = f"select 1 from {_ident(sch)}.{_ident(tbl)} limit 1"
            try:
                if self._psql("dst", db, q).strip():
                    got.add(name)
            except RuntimeError:
                # a table the target does not have yet cannot have rows in
                # it, and a permission problem is not an answer either way
                continue
        return got

    def moved_nothing(self, db):
        """Which tables the source has rows in and the target does not.

        Each asked as the move carried it: a partition by its table's scope
        and row filter (`_ruled`) - one of a table the hop leaves out, or
        whose filter selects none of its rows, was said to have received
        nothing - and a renamed table, or a partition of one, under the
        name the hop gives it on the target, where it was looked for under
        the name it no longer has."""
        from .. import movers
        try:
            src = set(self._psql("src", db, self.USER_TABLES).splitlines())
            dst = set(self.user_tables("dst", db, "all"))
        except Exception:
            return None
        empty = []
        # every source table, not only the ones both sides have: a table the
        # target does not have at all received nothing either, and asking
        # only about the shared ones passed a target with no tables
        for t in sorted(x for x in src if x):
            if not self.in_scope(db, t):
                continue
            sch, tbl = self._ruled(db, t)
            there = t
            if movers._renamed(self.hop, db, [sch, tbl]):
                there = ".".join(split_table(
                    "postgres", self.hop.target_table_in(db, sch, tbl), sch))
            # through the same scope as every other check read: a table
            # whose row filter selects nothing is correctly empty
            pred = self.hop.row_filter(db, sch, tbl)
            where = f" where {pred}" if pred else ""
            try:
                has_src = bool(self._psql(
                    "src", db, f"select 1 from {self._qualified('src', db, t)}"
                               f"{where} limit 1").strip())
                has_dst = there in dst and bool(self._psql(
                    "dst", db, "select 1 from"
                               f" {self._qualified('dst', db, there)}"
                               f"{where} limit 1").strip())
            except Exception:
                return None
            if has_src and not has_dst:
                empty.append(t)
        return empty

    def _analyze_in_stages(self, db):
        """Analyze the target after a load, in stages.

        `--analyze-in-stages` does three passes of increasing accuracy, so
        the planner has usable statistics within seconds instead of waiting
        for the full pass - which is what PostgreSQL's own documentation
        recommends after a restore, and what autoanalyze will not do for a
        table nobody writes to afterwards.
        """
        from ..movers import DRIVEN
        from ..wording import without_programs
        target = self._d("dst", db)
        if not which("vacuumdb"):
            self._psql("dst", db, "analyze")
            return f"analyzed {target} on the target"
        argv, env = self._client("dst", db, "vacuumdb",
                                 "--analyze-in-stages", "-j",
                                 str(max(1, int(self.hop.workers))))
        p = run(argv, env=env, check=False, timeout=7200)
        if p.returncode:
            said = (p.stderr or "").strip().splitlines() or ["unknown"]
            return (f"could not analyze {target}: "
                    + without_programs(said[-1][:90],
                                       DRIVEN + ("vacuumdb",)))
        return f"analyzed {target} on the target (in stages)"

    @staticmethod
    def _crosscheck_verdicts(text):
        """`{table: same?}` from `pgcopydb compare data` output.

        Its table marks a difference with `!` in the second column, so the
        parse is the marker and not the checksums - two hex strings that
        happen to differ is what the marker already means, and reading
        them twice would be a second opinion about pgcopydb's own answer.
        """
        out = {}
        for line in text.splitlines():
            parts = [c.strip() for c in line.split("|")]
            if len(parts) < 4 or not parts[0] or "." not in parts[0]:
                continue
            if parts[0].startswith("Table Name") or set(parts[0]) <= set("- "):
                continue
            name = parts[0].split(".", 1)[1] if parts[0].count(".") > 1 \
                else parts[0]
            out[name] = parts[1] != "!"
        return out

    def _crosscheck_result(self, db, mine, theirs, ran):
        """Whether a second implementation reaches migkit's verdict.

        Measured on pgcopydb 0.18 against four pairs before this was
        written, and it agreed every time: identical data, `numeric` 1.0
        against 1.00 (both **differ** - equal by `=`, not equal as stored),
        the same columns declared in a different order (both **same** -
        each sorts them), and a table with no primary key and a missing row
        (both **differ**). That agreement is the point: a check nobody has
        ever audited is the one this project exists to argue against.

        A table only one of them looked at is not a disagreement. Saying so
        is what keeps this from crying wolf on every filtered hop.
        """
        if not ran:
            return Result("deep", f"{db} cross-check", "skip",
                          "this machine cannot read the pair a second time"
                          " by an independent route, so these tables were"
                          " compared once")
        shared = sorted(set(mine) & set(theirs))
        clash = [t for t in shared if mine[t] != theirs[t]]
        if clash:
            return Result(
                "deep", f"{db} cross-check", "diff",
                f"{len(clash)} tables read two ways, with two answers:"
                + ", ".join(f" {t} (matching one way,"
                            f" differing the other)"
                            for t in clash[:3])
                + (" ..." if len(clash) > 3 else "")
                + " - one of the two readings is wrong about this table, so"
                  " the verdict above cannot be relied on for it",
                "", "migkit check <hop> --drill --table <name> writes the"
                    " differing rows out; read them before trusting either"
                    " answer")
        if not shared:
            return Result("deep", f"{db} cross-check", "skip",
                          "no table was read by both routes")
        return Result("deep", f"{db} cross-check", "ok",
                      f"{len(shared)} tables read a second time by an"
                      " independent route, same verdict on every one")

    #: What `pgcopydb compare schema` says when it finds something, measured
    #: on 0.18 by making each difference in turn.
    _SCHEMA_FINDINGS = ("has ", "exists on source but not",
                        "Failed to find")

    @staticmethod
    def _schema_verdict(out, rc):
        """(differs, what it named). `differs` is None when it could not be
        asked at all, which is not the same as agreeing."""
        if "schema inspection is successful" in out:
            return False, []
        if "Schemas on source and target database differ" in out:
            found = []
            for ln in out.splitlines():
                body = ln.split("  ", 1)[-1].strip()
                body = body.split("compare.c:")[-1].lstrip("0123456789 ")
                if any(k in body for k in PostgresEngine._SCHEMA_FINDINGS):
                    found.append(body[:120])
            return True, found[:6]
        return None, []

    def _crosscheck_schema_result(self, db, theirs, found, mine):
        """One direction carries information here, and the other does not.

        `pgcopydb compare schema` is not a second opinion the way
        `compare data` is - it is a **narrower** check. Measured on 0.18 by
        introducing one difference at a time:

            target missing a column     differ   (names the column)
            target missing an index     differ
            target missing a table      differ
            varchar(50) -> varchar(200) **successful** - missed

        migkit reports that last one as a difference, deliberately:
        `neutral_columns` reads `format_type` precisely so a target built
        wider than its source is visible, because a widened column loses a
        limit the application relied on without losing a row to show for it.

        So "pgcopydb says same, migkit says differs" is the expected shape
        and not a clash - reporting it as one would cry wolf on every
        widened column. The other direction is the finding: if pgcopydb
        names a difference that migkit's schema check passed over, migkit
        missed something.
        """
        if theirs is None:
            return Result("deep", f"{db} schema cross-check", "skip",
                          "the schemas could not be read a second time by an"
                          " independent route, so the verdict above stands"
                          " on one reading")
        if mine is None:
            return Result("deep", f"{db} schema cross-check", "skip",
                          "the schema check has not run for this database,"
                          " so there is no verdict to hold this against")
        if theirs and mine:
            return Result(
                "deep", f"{db} schema cross-check", "diff",
                "the schemas differ in a way the schema check above did not"
                " report: " + "; ".join(found or ["(unnamed)"])
                + " - treat the target's schema as unverified until this is"
                  " resolved",
                "", "compare schema-src.sql and schema-dst.sql in the report"
                    " directory for this database")
        if theirs and not mine:
            return Result("deep", f"{db} schema cross-check", "ok",
                          "a second, independent reading agrees the schemas"
                          " differ")
        if not mine:
            return Result(
                "deep", f"{db} schema cross-check", "ok",
                "the schema difference reported above is not one the second"
                " reading looks for - it matches tables, columns and indexes"
                " by name and does not compare column types, so a widened"
                " column passes it. Not a disagreement")
        return Result("deep", f"{db} schema cross-check", "ok",
                      "both readings see the schemas as matching")

    def _crosscheck_schema(self, db):
        """The schema half of the cross-check, behind the same switch."""
        from .. import envkeys
        if not envkeys.flag("MIGKIT_CROSSCHECK"):
            return None
        from ..config import scoped
        if scoped(self.hop):
            # it reads the two databases whole and cannot be told what the
            # hop leaves out, whose differences are not the move's
            return Result("deep", f"{db} schema cross-check", "skip",
                          "the second reading compares whole databases and"
                          " this hop takes only some of this one, so the"
                          " schema check above stands on one reading")
        from ..movers import pgcopydb_available
        from ..util import run
        if not pgcopydb_available():
            return self._crosscheck_schema_result(db, None, [], None)
        import tempfile
        # the passwords in a password file, not in the addresses: those
        # are the program's arguments. The side's TLS in them
        src = self._url_without_password("src", db)
        dst = self._url_without_password("dst", db)
        work = tempfile.mkdtemp(prefix="migkit-schemacheck-")
        try:
            from pathlib import Path

            from ..movers import _pgpass
            passfile = Path(work) / ".pgpass"
            _pgpass(passfile, self.hop, db, self._d("dst", db))
            p = run(["pgcopydb", "compare", "schema", "--dir", work,
                     "--source", src, "--target", dst],
                    env={"PGPASSFILE": str(passfile)}, check=False)
        except Exception as e:
            return Result("deep", f"{db} schema cross-check", "error",
                          "the schemas could not be read a second time:"
                          f" {str(e).splitlines()[-1][:80]}")
        theirs, found = self._schema_verdict(p.stdout + p.stderr,
                                             p.returncode)
        return self._crosscheck_schema_result(
            db, theirs, found, self._schema_from_evidence(db))

    def _crosscheck(self, db):
        """Ask pgcopydb the same question and see if it agrees.

        Off unless `MIGKIT_CROSSCHECK` says otherwise: it reads both
        databases in full a second time, which is a real cost to impose on
        every run. An environment variable rather than a flag, the same way
        `MIGKIT_MOVER` is.
        """
        from .. import envkeys
        if not envkeys.flag("MIGKIT_CROSSCHECK"):
            return None
        from ..movers import pgcopydb_available
        from ..util import run
        if not pgcopydb_available():
            return self._crosscheck_result(db, {}, {}, False)
        import tempfile
        # the passwords in a password file, not in the addresses: those
        # are the program's arguments. The side's TLS in them
        src = self._url_without_password("src", db)
        dst = self._url_without_password("dst", db)
        work = tempfile.mkdtemp(prefix="migkit-crosscheck-")
        try:
            from pathlib import Path

            from ..movers import _pgpass
            passfile = Path(work) / ".pgpass"
            _pgpass(passfile, self.hop, db, self._d("dst", db))
            p = run(["pgcopydb", "compare", "data", "--dir", work,
                     "--source", src, "--target", dst],
                    env={"PGPASSFILE": str(passfile)}, check=False)
        except Exception as e:
            return Result("deep", f"{db} cross-check", "error",
                          "the pair could not be read a second time:"
                          f" {str(e).splitlines()[-1][:80]}")
        theirs = self._crosscheck_verdicts(p.stdout + p.stderr)
        mine = self._mine_from_evidence(db)
        if not mine:
            return Result("deep", f"{db} cross-check", "skip",
                          "no data-evidence.txt to compare against - the"
                          " data check has to have run for there to be a"
                          " migkit verdict to second-guess")
        return self._crosscheck_result(db, mine, theirs, True)

    def _mine_from_evidence(self, db):
        """migkit's own per-table verdict, read from the file the data
        check already wrote rather than by running the pass again.

        Asking twice would be two answers taken at two different moments
        about the same question, which is the thing this check exists to
        catch rather than to commit.
        """
        out = {}
        path = self.hop.report_dir(db) / "data-evidence.txt"
        if not path.exists():
            return out
        for line in path.read_text().splitlines():
            name, _, rest = line.partition(":")
            if not rest.strip():
                continue
            out[name.strip()] = rest.strip().startswith("OK")
        return out

    def _planner_stats(self, db):
        """What the target's own catalog says it knows about its tables.

        `reltuples = -1` is PostgreSQL 14+ for "never counted"; before that
        it was 0, which is indistinguishable from an empty table - so the
        two timestamps are what decide it, and reltuples is only read for
        the row count the ratio needs.
        """
        rows = []
        try:
            out = self._psql("dst", db,
                "select n.nspname||'.'||c.relname"
                " ||chr(9)||greatest(c.reltuples, 0)::bigint"
                " ||chr(9)||(s.last_analyze is not null"
                "            or s.last_autoanalyze is not null)::int"
                " ||chr(9)||coalesce(s.n_mod_since_analyze, 0)"
                # a boolean reloption is stored as whatever spelling was
                # written - `false`, `off`, `0` and `no` all occur - so the
                # server casts it rather than this code guessing the list
                " ||chr(9)||coalesce((select option_value from"
                "  pg_options_to_table(c.reloptions)"
                "  where option_name = 'autovacuum_enabled'),"
                "  'true')::boolean::int"
                " from pg_class c"
                " join pg_namespace n on n.oid = c.relnamespace"
                " left join pg_stat_user_tables s on s.relid = c.oid"
                " where c.relkind = 'r'"
                " and n.nspname not in ('pg_catalog','information_schema')"
                " and n.nspname not like 'pg\\_%'"
                " and n.nspname not like '\\_\\_%'"
                f" and c.relname not in {OWN_SQL}"
                " order by 1")
        except Exception as e:
            return Result("deep", f"{db} statistics", "error",
                          f"could not read the target's statistics:"
                          f" {str(e).splitlines()[-1][:90]}")
        # The global switch decides whether the per-table one matters at
        # all. Read once rather than per table, and read as its own
        # statement so a server that refuses `pg_settings` leaves the
        # answer unknown rather than wrong.
        try:
            autovacuum_on = self._psql(
                "dst", db,
                "select setting from pg_settings where name = 'autovacuum'"
            ).strip() == "on"
        except Exception:
            autovacuum_on = False
        reachable = set()
        for line in self._scoped(db, out, "\t"):
            parts = line.split("\t")
            if len(parts) != 5:
                continue
            rows.append((parts[0], int(parts[1]), parts[2] == "1",
                         int(parts[3])))
            # A never-analyzed table holds its whole row count in
            # n_mod_since_analyze, which clears the threshold for anything
            # above roughly fifty rows - and a table smaller than that has
            # no plan worth worrying about either way.
            if autovacuum_on and parts[4] == "1":
                reachable.add(parts[0])
        if not rows:
            return Result("deep", f"{db} statistics", "skip",
                          "no user tables on the target to have statistics"
                          " for")
        return self._planner_stats_result(
            db, rows,
            "run `vacuumdb --analyze-in-stages` against the target (or"
            " `migkit move --go`, which now analyzes what it loads)",
            reachable)


    # --- the source's own reads, planned and timed on both sides (G1) ----

    def workload_reads(self, db, n):
        """[(label, statement, runnable, mean ms, calls)] - the source's
        busiest reads by total time, from `pg_stat_statements` - or why
        there are none. The hop's own user's statements are left out: they
        are migkit's checks, not the application's reads."""
        return self._workload(db, n, "select|with")

    def workload_writes(self, db, n):
        """The source's busiest inserts, updates, deletes and merges, as
        `workload_reads` gives its reads - their mean time the source's own
        statistics' (a write is never run on the source)."""
        return self._workload(db, n, "insert|update|delete|merge")

    def _workload(self, db, n, verbs):
        if self._psql("src", db, "select count(*) from pg_extension where"
                                 " extname = 'pg_stat_statements'") != "1":
            return ("the source keeps no statement statistics here"
                    " (pg_stat_statements is not installed in this"
                    " database)")
        rows = self._psql("src", db, f"""
            select replace(replace(replace(query, chr(10), ' '), chr(31),
                           ' '), chr(30), ' ')
                   || chr(30) || round(mean_exec_time::numeric, 3)
                   || chr(30) || calls
              from pg_stat_statements
             where dbid = (select oid from pg_database
                            where datname = current_database())
               and userid <> (select oid from pg_roles
                               where rolname = current_user)
               and query ~* {_literal('^\\s*(' + verbs + ')\\M')}
               and query !~* '(pg_catalog|information_schema|pg_stat)'
             order by total_exec_time desc limit {int(n)}""")
        import re
        out = []
        for line in (l for l in rows.splitlines() if l.strip()):
            q, _, rest = line.partition("\x1e")
            mean, _, calls = rest.partition("\x1e")
            label = " ".join(q.split())[:70]
            out.append((label, q, not re.search(r"\$\d", q),
                        float(mean) if mean else None,
                        int(calls) if calls.isdigit() else 1))
        return out

    def workload_rows(self, side, db, sql, most):
        """The statement's answer, at most `most` rows, run read-only under
        a time limit."""
        conn = self._connect(side, db)
        try:
            conn.set_session(readonly=True)
            with conn.cursor() as cur:
                cur.execute("set statement_timeout = 5000")
                cur.execute(sql)
                return [tuple(r) for r in cur.fetchmany(int(most))]
        finally:
            conn.rollback()
            conn.close()

    def workload_time_write(self, side, db, sql):
        """A write's execution time in ms on the target, inside a
        transaction that is rolled back; a lock it would wait on for more
        than a fifth of a second stops it (None), so a change tail writing
        the same rows is never held up."""
        import psycopg2
        self._target_only(side, "time a write")
        conn = self._connect(side, db)
        try:
            with conn.cursor() as cur:
                cur.execute("set local statement_timeout = 5000")
                cur.execute("set local lock_timeout = 200")
                try:
                    cur.execute("explain (analyze, timing off, format json)"
                                f" {sql}")
                except psycopg2.errors.LockNotAvailable:
                    return None
                got = cur.fetchone()[0]
        finally:
            conn.rollback()
            conn.close()
        import json as _json
        plan = got if isinstance(got, list) else _json.loads(got)
        return float(plan[0]["Execution Time"])

    #: the settings the planner's choices turn on, compared between two
    #: servers of one engine (`workload.settings_differ`)
    PLANNER_SETTINGS = ("work_mem", "effective_cache_size", "shared_buffers",
                        "random_page_cost", "seq_page_cost",
                        "default_statistics_target",
                        "effective_io_concurrency", "jit",
                        "max_parallel_workers_per_gather",
                        "join_collapse_limit", "from_collapse_limit",
                        "plan_cache_mode", "enable_seqscan",
                        "enable_indexscan", "enable_bitmapscan",
                        "enable_hashjoin", "enable_mergejoin",
                        "enable_nestloop", "enable_partitionwise_join")

    def planner_settings(self, side, db):
        names = ", ".join(f"{_literal(n)}" for n in self.PLANNER_SETTINGS)
        out = {}
        for line in self._psql(side, db, "select name || chr(31) ||"
                                         " setting || coalesce(unit, '')"
                                         f" from pg_settings where name in"
                                         f" ({names})").splitlines():
            name, _, value = line.partition("\x1f")
            if name:
                out[name] = value
        return out

    def _plan_json(self, side, db, sql, analyze=False):
        import json as _json
        conn = self._connect(side, db)
        try:
            conn.set_session(readonly=True)
            with conn.cursor() as cur:
                cur.execute("set statement_timeout = 5000")
                import re
                params = bool(re.search(r"\$\d", sql))
                opts = ("analyze, timing off, format json" if analyze else
                        "generic_plan, format json" if params
                        else "format json")
                cur.execute(f"explain ({opts}) {sql}")
                got = cur.fetchone()[0]
            return got if isinstance(got, list) else _json.loads(got)
        finally:
            conn.rollback()
            conn.close()

    def workload_plan(self, side, db, sql):
        """{table: "index <name>" or None where the plan reads it whole}.
        A statement with parameters left in it is planned generically,
        which is PostgreSQL 16 and later."""
        out = {}

        def under(node):
            """The indexes a bitmap scan's own children read the table by:
            they name no table, and the heap scan above them names no
            index - read apart, a table reached through an index read as
            a table read whole."""
            for child in node.get("Plans") or []:
                if child.get("Index Name"):
                    yield child["Index Name"]
                yield from under(child)

        def walk(node):
            rel = node.get("Relation Name")
            if rel:
                kind = node.get("Node Type", "")
                by = next(under(node), None) \
                    if kind == "Bitmap Heap Scan" else None
                out[rel] = (f"index {node.get('Index Name')}"
                            if "Index" in kind and node.get("Index Name")
                            else f"index {by}" if by
                            else None if kind == "Seq Scan"
                            else out.get(rel))
            for child in node.get("Plans") or []:
                walk(child)
        walk(self._plan_json(side, db, sql)[0]["Plan"])
        return out

    def workload_time(self, side, db, sql):
        """The statement's execution time in ms, run read-only; no rows
        come back."""
        return float(self._plan_json(side, db, sql, analyze=True)[0]
                     ["Execution Time"])

    def stream_identity(self, side, db):
        """The cluster's system identifier, which a physical replica shares
        with its primary and a rebuilt server does not. None where this
        user may not read it."""
        try:
            got = self._psql(side, db, "select system_identifier from"
                                       " pg_control_system()")
        except Exception:  # noqa: BLE001 - not every user may ask
            return None
        return {"cluster": got.strip()} if got.strip() else None

    def server_identity(self, side, db):
        """The system identifier, which a clone restored from the source's
        backup shares with it - so the start of the postmaster beside it,
        which no two servers share - the address it answers on, its
        version and the database's size. Asked once with a short wait,
        and through the maintenance database where the hop's is not there
        yet (a bulk path makes it)."""
        import psycopg2
        want = self._d(side, db) if db else "postgres"
        last = None
        for at in dict.fromkeys((want, "postgres")):
            try:
                conn = self._open(side, at, "probe")
            except psycopg2.Error as e:
                if "does not exist" not in str(e):
                    raise
                last = e
                continue
            try:
                with conn.cursor() as cur:
                    cur.execute("select pg_postmaster_start_time()::text,"
                                " current_setting('server_version'),"
                                " host(inet_server_addr()),"
                                " pg_database_size(current_database())")
                    started, version, addr, size = cur.fetchone()
            finally:
                conn.close()
            out = {"running": started, "version": version,
                   "name": addr or ""}
            if at == want:
                out["bytes"] = int(size)
            else:
                out["absent"] = True
            got = self.stream_identity(side, at if at == "postgres" else db)
            if got:
                out["id"] = got
            return out
        raise last

    #: WAL the target held at its peak during a load, per byte of
    #: `max_wal_size`: measured on 16 at 64MB, loading a 355 MiB table
    #: that wrote 411 MiB of WAL, `pg_wal` peaked at 80 MiB
    WAL_PEAK = 1.25

    def log_kept(self, db, grows):
        """Checkpoints recycle WAL past `max_wal_size`, unless a slot on
        the target still needs it. Measured with one inactive slot on the
        same load: `pg_wal` peaked at 416 MiB, all of it."""
        row = self._psql("dst", db, """
            select (select count(*) from pg_replication_slots)
                   || ',' || pg_size_bytes(current_setting('max_wal_size'))
                   || ',' || pg_size_bytes(current_setting('wal_keep_size'))
            """).split(",")
        slots, most, keep = (int(x) for x in row)
        if slots:
            return (grows, f"all of it: {slots} replication slot(s) on the"
                           " target keep WAL until they have read it")
        bound = int(most * self.WAL_PEAK) + keep
        return (min(grows, bound), "checkpoints recycle it past"
                                   " max_wal_size and wal_keep_size")

    def text_encodings(self, side, db):
        # one encoding for the whole database: every text value is in it
        got = self._psql(side, db,
                         "select pg_encoding_to_char(encoding) from"
                         " pg_database where datname = current_database()")
        return {got.strip(): 1} if got.strip() else None

    def neutral_create_code(self, side, db, statement):
        if side != "dst":
            raise RuntimeError("migkit does not write to the source")
        conn = self._connect(side, db)
        try:
            with conn.cursor() as cur:
                cur.execute(statement)
            conn.commit()
        finally:
            conn.close()

    def neutral_create_codes(self, side, db, todo):
        """The converted statements in one transaction on one connection,
        each under a savepoint so every failure is named, and none of them
        kept where one fails: each on a connection of its own kept half a
        schema."""
        if side != "dst":
            raise RuntimeError("migkit does not write to the source")
        made, failed = [], []
        conn = self._connect(side, db)
        try:
            with conn.cursor() as cur:
                for i, (name, stmt) in enumerate(todo):
                    cur.execute(f"savepoint migkit_convert_{i}")
                    try:
                        cur.execute(stmt)
                        made.append(name)
                    except Exception as e:  # noqa: BLE001 - listed, undone
                        cur.execute(
                            f"rollback to savepoint migkit_convert_{i}")
                        failed.append(f"{name}: " + (
                            str(e).strip().splitlines() or [""])[0])
            if failed:
                conn.rollback()
                made = []
            else:
                conn.commit()
        finally:
            conn.close()
        return made, failed, True

    def neutral_views(self, side, db):
        rows = self._psql(side, db, "select schemaname||'.'||viewname"
                                    "||chr(31)||replace(definition,"
                                    " chr(10), ' ') from pg_views where"
                                    " schemaname not in ('pg_catalog',"
                                    " 'information_schema') order by 1")
        out = []
        for line in rows.splitlines():
            n, _, d = line.partition("\x1f")
            if n and self.in_scope(db, n):
                out.append((n, d))
        return out

    def neutral_functions(self, side, db):
        """Functions and procedures written in the database - not the ones
        an extension brought, which the target gets from the extension.
        A `language sql` function whose body is one `select` of one
        expression and nothing else is carried; the rest are named."""
        import sqlglot
        from sqlglot import exp
        out = []
        for line in self._psql(side, db, """
                select p.proname::text || chr(31) || p.prokind::text || chr(31)
                       || l.lanname::text || chr(31)
                       || pg_get_function_identity_arguments(p.oid)
                       || chr(31) || format_type(p.prorettype, null)
                       || chr(31) || replace(coalesce(p.prosrc, ''),
                                             chr(10), ' ')
                  from pg_proc p
                  join pg_namespace n on n.oid = p.pronamespace
                  join pg_language l on l.oid = p.prolang
                 where n.nspname not in ('pg_catalog', 'information_schema')
                   and n.nspname not like 'pg_toast%'
                   and p.prokind in ('f', 'p')
                   and not exists (select 1 from pg_depend d
                                    where d.classid = 'pg_proc'::regclass
                                      and d.objid = p.oid
                                      and d.deptype = 'e')
                 order by 1""").splitlines():
            parts = line.split("\x1f")
            if len(parts) != 6:
                continue
            fn, kind, lang, args, returns, src = parts
            params = []
            for a in filter(None, (x.strip() for x in args.split(","))):
                pname, _, ptype = a.partition(" ")
                params.append((pname, ptype) if ptype else ("", pname))
            one = None
            if kind == "f" and lang == "sql" and all(p for p, _ in params):
                try:
                    tree = sqlglot.parse_one(src.strip().rstrip(";"),
                                             read="postgres")
                    # one expression and no clause: the translator names
                    # a select's FROM `from_` (30.x), and a body reading
                    # a table was taken for its select list alone
                    if isinstance(tree, exp.Select) and \
                            len(tree.expressions) == 1 and not any(
                                tree.args.get(k) for k in (
                                    "from", "from_", "where", "group",
                                    "having", "order", "limit", "offset",
                                    "joins", "with", "distinct")):
                        one = tree.expressions[0].unalias().sql("postgres")
                except Exception:  # noqa: BLE001 - not one expression
                    one = None
            out.append((fn, params, returns, one))
        return out

    #: the bits of `pg_trigger.tgtype`
    TG_ROW, TG_BEFORE, TG_INSERT, TG_DELETE, TG_UPDATE, TG_TRUNCATE, \
        TG_INSTEAD = 1, 2, 4, 8, 16, 32, 64

    def _catalog(self, side, db, sql):
        conn = self._connect(side, db)
        try:
            conn.set_session(readonly=True)
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
        finally:
            conn.rollback()
            conn.close()

    def code_routines(self, side, db):
        """Every function, procedure and trigger written in the database -
        not an extension's, which the target gets from the extension. A
        function a trigger runs is the trigger's, not a function of its
        own: it cannot be called."""
        from ..storedcode.routines import Param, Routine
        out = []
        for oid, name, kind, lang, setof, ret, names, modes, types, src, \
                made, sch, volatile in self._catalog(side, db, """
                select p.oid, p.proname, p.prokind, l.lanname, p.proretset,
                       format_type(p.prorettype, null),
                       coalesce(p.proargnames, '{}'),
                       coalesce(p.proargmodes::text[], '{}'),
                       array(select format_type(t, null) from unnest(
                             coalesce(p.proallargtypes,
                                      p.proargtypes::oid[])) t),
                       p.prosrc, pg_get_functiondef(p.oid), n.nspname,
                       p.provolatile
                  from pg_proc p
                  join pg_namespace n on n.oid = p.pronamespace
                  join pg_language l on l.oid = p.prolang
                 where n.nspname not in ('pg_catalog', 'information_schema')
                   and n.nspname not like 'pg_toast%'
                   and p.prokind in ('f', 'p')
                   and p.prorettype <> 'trigger'::regtype
                   and not exists (select 1 from pg_depend d
                                    where d.classid = 'pg_proc'::regclass
                                      and d.objid = p.oid
                                      and d.deptype = 'e')
                 order by 2"""):
            if self.hop.excluded(db, sch, name):
                continue
            params = []
            for i, t in enumerate(types):
                mode = {"i": "in", "o": "out", "b": "inout", "v": "in",
                        "t": "table"}.get(modes[i] if i < len(modes)
                                          else "i", "in")
                if mode != "table":
                    params.append(Param(names[i] if i < len(names) else "",
                                        t, mode))
            k = "procedure" if kind == "p" else "function"
            out.append(Routine(
                kind=k, name=str(name), params=params,
                returns=None if k == "procedure" else str(ret),
                returns_set=bool(setof), body=src, definition=made,
                language=str(lang), settings={
                    "schema": sch, "oid": int(oid),
                    "data": {"i": "NO SQL", "s": "READS SQL DATA"}.get(
                        volatile, "MODIFIES SQL DATA")}))
        for name, table, sch, tgtype, fn, made, src, fmade, lang, foid in \
                self._catalog(side, db, """
                select t.tgname, c.relname, n.nspname, t.tgtype, p.proname,
                       pg_get_triggerdef(t.oid), p.prosrc,
                       pg_get_functiondef(p.oid), l.lanname, p.oid
                  from pg_trigger t
                  join pg_class c on c.oid = t.tgrelid
                  join pg_namespace n on n.oid = c.relnamespace
                  join pg_proc p on p.oid = t.tgfoid
                  join pg_language l on l.oid = p.prolang
                 where not t.tgisinternal
                   and n.nspname not in ('pg_catalog', 'information_schema')
                 order by 2, 1"""):
            if self.hop.excluded(db, sch, table):
                continue
            events = tuple(e for bit, e in (
                (self.TG_INSERT, "INSERT"), (self.TG_UPDATE, "UPDATE"),
                (self.TG_DELETE, "DELETE"), (self.TG_TRUNCATE, "TRUNCATE"))
                if tgtype & bit)
            timing = ("INSTEAD OF" if tgtype & self.TG_INSTEAD else
                      "BEFORE" if tgtype & self.TG_BEFORE else "AFTER")
            out.append(Routine(
                kind="trigger", name=str(name), table=f"{sch}.{table}",
                timing=timing, events=events,
                level="row" if tgtype & self.TG_ROW else "statement",
                body=src, definition=f"{fmade.rstrip().rstrip(';')};\n"
                                     f"{made};",
                language=str(lang), settings={"function": str(fn),
                                              "schema": sch,
                                              "oid": int(foid)}))
        return out

    def table_ddl(self, side, db, table):
        """The statements that make this table as it is - columns with
        their declared types, defaults (a sequence's as identity), NOT
        NULL, the key, unique and check constraints and the enum types it
        uses - for a copy on another PostgreSQL (the proof's sandbox).
        Foreign keys are left to `foreign_keys`, added once rows are in."""
        sch, tbl = self._split(table)
        rows = self._catalog(side, db, f"""
            select a.attname, format_type(a.atttypid, a.atttypmod),
                   a.attnotnull, pg_get_expr(d.adbin, d.adrelid),
                   a.attidentity, a.attgenerated, t.typtype,
                   format_type(a.atttypid, null),
                   (select string_agg(quote_literal(e.enumlabel), ', '
                                      order by e.enumsortorder)
                      from pg_enum e where e.enumtypid = a.atttypid)
              from pg_attribute a
              join pg_class c on c.oid = a.attrelid
              join pg_namespace n on n.oid = c.relnamespace
              join pg_type t on t.oid = a.atttypid
              left join pg_attrdef d on d.adrelid = a.attrelid
                                    and d.adnum = a.attnum
             where n.nspname = {self._lit(sch)} and c.relname = {self._lit(tbl)}
               and a.attnum > 0 and not a.attisdropped
             order by a.attnum""")
        before, cols = [], []
        for name, typ, notnull, default, ident, gen, kind, bare, labels in \
                rows:
            if kind == "e" and labels:
                before.append(f"do $$ begin create type {bare} as enum"
                              f" ({labels}); exception when duplicate_object"
                              " then null; end $$")
            one = f"{self._quote_ident(name)} {typ}"
            if gen == "s" and default:
                one += f" generated always as ({default}) stored"
            elif ident or (default and default.startswith("nextval(")):
                one += " generated by default as identity"
            elif default is not None:
                one += f" default {default}"
            if notnull:
                one += " not null"
            cols.append(one)
        for (con,) in self._catalog(side, db, f"""
                select pg_get_constraintdef(o.oid) from pg_constraint o
                  join pg_class c on c.oid = o.conrelid
                  join pg_namespace n on n.oid = c.relnamespace
                 where n.nspname = {self._lit(sch)}
                   and c.relname = {self._lit(tbl)}
                   and o.contype in ('p', 'u', 'c')
                 order by o.contype, o.conname"""):
            cols.append(con)
        made = (f"create table {self._quote_ident(sch)}."
                f"{self._quote_ident(tbl)} ({', '.join(cols)})")
        return ";\n".join(
            [f"create schema if not exists {self._quote_ident(sch)}"]
            + before + [made])

    @staticmethod
    def _lit(text):
        return _literal(text)

    def foreign_keys(self, side, db):
        """[(child, [its columns], parent, [the parent's columns])], each
        table as `neutral_tables` names it."""
        out = []
        for child, parent, cols, pcols in self._catalog(side, db, """
                select cn.nspname || '.' || c.relname,
                       pn.nspname || '.' || p.relname,
                       array(select a.attname from unnest(o.conkey)
                             with ordinality k(n, i) join pg_attribute a
                             on a.attrelid = o.conrelid and a.attnum = k.n
                             order by k.i),
                       array(select a.attname from unnest(o.confkey)
                             with ordinality k(n, i) join pg_attribute a
                             on a.attrelid = o.confrelid and a.attnum = k.n
                             order by k.i)
                  from pg_constraint o
                  join pg_class c on c.oid = o.conrelid
                  join pg_namespace cn on cn.oid = c.relnamespace
                  join pg_class p on p.oid = o.confrelid
                  join pg_namespace pn on pn.oid = p.relnamespace
                 where o.contype = 'f'
                   and cn.nspname not in ('pg_catalog',
                                          'information_schema')"""):
            out.append((child, list(cols), parent, list(pcols)))
        return out

    DEPENDENCY_KINDS = ("view", "routine", "trigger", "sequence",
                        "partition")

    def dependencies(self, side, db, tables=()):
        """What the database's objects need of its tables
        (`Engine.dependencies`), from the catalogue: a view's relations by
        its rewrite rule's own dependencies (`pg_depend` on `pg_rewrite`);
        a function's, a procedure's and a trigger function's tables by
        the text of the body, which no catalogue records; a sequence a
        default draws from by the default's dependency and its owner by
        the sequence's; a partition by `pg_inherits`. Not an extension's
        objects, which the target gets from the extension."""
        out = []
        for view, needs in self._catalog(side, db, """
                select distinct vn.nspname || '.' || v.relname,
                       tn.nspname || '.' || t.relname
                  from pg_rewrite r
                  join pg_class v on v.oid = r.ev_class
                  join pg_namespace vn on vn.oid = v.relnamespace
                  join pg_depend d on d.classid = 'pg_rewrite'::regclass
                                  and d.objid = r.oid
                                  and d.refclassid = 'pg_class'::regclass
                  join pg_class t on t.oid = d.refobjid
                  join pg_namespace tn on tn.oid = t.relnamespace
                 where v.relkind in ('v', 'm') and t.oid <> v.oid
                   and vn.nspname not in ('pg_catalog', 'information_schema')
                   and tn.nspname not in ('pg_catalog',
                                          'information_schema')"""):
            out.append(("view", view, view, needs))
        for name, body in self._catalog(side, db, """
                select n.nspname || '.' || p.proname, coalesce(p.prosrc, '')
                  from pg_proc p
                  join pg_namespace n on n.oid = p.pronamespace
                 where n.nspname not in ('pg_catalog', 'information_schema')
                   and n.nspname not like 'pg_toast%'
                   and p.prokind in ('f', 'p')
                   and p.prorettype <> 'trigger'::regtype
                   and not exists (select 1 from pg_depend d
                                    where d.classid = 'pg_proc'::regclass
                                      and d.objid = p.oid
                                      and d.deptype = 'e')"""):
            out += [("routine", name, name, t)
                    for t in self._tables_named(body, tables)]
        for name, table, body in self._catalog(side, db, """
                select t.tgname, n.nspname || '.' || c.relname,
                       coalesce(p.prosrc, '')
                  from pg_trigger t
                  join pg_class c on c.oid = t.tgrelid
                  join pg_namespace n on n.oid = c.relnamespace
                  join pg_proc p on p.oid = t.tgfoid
                 where not t.tgisinternal
                   and n.nspname not in ('pg_catalog',
                                         'information_schema')"""):
            out += [("trigger", name, table, t)
                    for t in self._tables_named(body, tables, own=table)]
        for table, seq, owner in self._catalog(side, db, """
                select distinct tn.nspname || '.' || t.relname,
                       sn.nspname || '.' || s.relname,
                       wn.nspname || '.' || w.relname
                  from pg_attrdef a
                  join pg_depend d on d.classid = 'pg_attrdef'::regclass
                                  and d.objid = a.oid
                                  and d.refclassid = 'pg_class'::regclass
                  join pg_class s on s.oid = d.refobjid and s.relkind = 'S'
                  join pg_namespace sn on sn.oid = s.relnamespace
                  join pg_class t on t.oid = a.adrelid
                  join pg_namespace tn on tn.oid = t.relnamespace
                  join pg_depend o on o.classid = 'pg_class'::regclass
                                  and o.objid = s.oid
                                  and o.refclassid = 'pg_class'::regclass
                                  and o.deptype in ('a', 'i')
                  join pg_class w on w.oid = o.refobjid
                  join pg_namespace wn on wn.oid = w.relnamespace
                 where w.oid <> t.oid"""):
            out.append(("sequence", seq, table, owner))
        for root, part in self._catalog(side, db, """
                select pn.nspname || '.' || p.relname,
                       cn.nspname || '.' || c.relname
                  from pg_inherits i
                  join pg_class p on p.oid = i.inhparent
                  join pg_namespace pn on pn.oid = p.relnamespace
                  join pg_class c on c.oid = i.inhrelid
                  join pg_namespace cn on cn.oid = c.relnamespace
                 where p.relkind = 'p'"""):
            out.append(("partition", part, root, part))
        return out

    def code_definition(self, side, db, kind, name):
        schema, leaf = self._split(name)
        if kind == "view":
            got = self._psql(side, db, "select pg_get_viewdef(to_regclass("
                             + _literal(_ident(schema) + "."
                                        + _ident(leaf)) + "))")
            return (f"create view {leaf} as {got}" if got else None)
        got = self._psql(side, db, "select string_agg(pg_get_functiondef("
                                   "p.oid), chr(10)) from pg_proc p where"
                                   f" p.proname = {_literal(leaf)}")
        return got or None

    def neutral_function_sql(self, name, params, returns, body):
        args = ", ".join(f'{self._quote_ident(p)} {t}' for p, t in params)
        return (f"create function {self._quote_ident(name)}({args}) returns"
                f" {returns} language sql as $$ select {body} $$")

    def zone_evidence(self, side, db):
        """(the server's UTC offset in minutes, its UTC clock, [(table.column,
        latest value)]) for the `timestamp without time zone` columns an
        index leads - read by the index, so each is one probe (`zones`)."""
        # the one session left in the server's own zone: its offset is
        # what is asked, which a session pinned to UTC answers as 0
        conn = connect(self._ep(side), self._d(side, db), session=False,
                       options=SESSION.replace(" -c TimeZone=UTC", ""))
        try:
            conn.set_session(readonly=True)
            with conn.cursor() as cur:
                cur.execute("select (extract(timezone from now()) / 60)::int,"
                            " now() at time zone 'utc'")
                offset, now = cur.fetchone()
                cur.execute("""
                    select n.nspname, t.relname, a.attname
                      from pg_index x
                      join pg_class t on t.oid = x.indrelid
                      join pg_namespace n on n.oid = t.relnamespace
                      join pg_attribute a on a.attrelid = t.oid
                                         and a.attnum = x.indkey[0]
                     where a.atttypid = 'timestamp'::regtype
                       and n.nspname not in ('pg_catalog',
                                             'information_schema')
                     group by 1, 2, 3 limit 30""")
                cols = cur.fetchall()
                heads = []
                for sch, t, c in cols:
                    if self.hop.excluded(db, sch, t):
                        continue
                    cur.execute(f'select max({_ident(c)}) from {_ident(sch)}.{_ident(t)}')
                    heads.append((f"{sch}.{t}.{c}", cur.fetchone()[0]))
            return int(offset), now, heads
        finally:
            conn.rollback()
            conn.close()

    def check_deep(self, db):
        res = []
        rpt = self.hop.report_dir(db)

        # a table with no pk/unique is the quiet trap: DMS and GoldenGate drop
        # its UPDATE/DELETE during CDC and duplicate it on full+CDC, and it
        # can't be verified or repaired by key. Surface it before it bites.
        nopk = self._scoped(db, self._psql("src", db,
                "select n.nspname||'.'||c.relname from pg_class c"
                " join pg_namespace n on n.oid = c.relnamespace"
                " where c.relkind = 'r'"
                " and n.nspname not in ('pg_catalog','information_schema')"
                " and n.nspname not like 'pg\\_%'"
                " and n.nspname not like '\\_\\_%'"
                f" and c.relname not in {OWN_SQL}"
                " and not exists (select 1 from pg_index i"
                "  where i.indrelid = c.oid"
                "  and (i.indisprimary or i.indisunique))"
                " order by 1"))
        if nopk:
            res.append(Result("deep", f"{db} keys", "diff",
                              f"{len(nopk)} tables have no pk/unique"
                              " (CDC drops their updates/deletes, dups on"
                              f" reload, unverifiable): {', '.join(nopk[:5])}",
                              "", "add a primary key or unique index, or set"
                                  " replica identity full, before migrating"))
        else:
            res.append(Result("deep", f"{db} keys", "ok",
                              "every table on the source has a pk or unique"
                              " index"))

        res.append(self._planner_stats(db))
        from .. import workload, zones
        res.append(workload.compare(self, db))
        res.append(zones.infer(self, db))
        cross = self._crosscheck(db)
        if cross is not None:
            res.append(cross)
        cross_schema = self._crosscheck_schema(db)
        if cross_schema is not None:
            res.append(cross_schema)
        res.append(self._lob_check(db))
        indexes = self._invalid_indexes(db)
        collations = self._collation_versions(db)
        res.append(indexes)
        res.append(collations)
        res.append(self._mojibake(db))
        # the expensive hunt only earns its keep once something else has
        # said an index cannot be trusted
        if collations.status == "diff":
            why = "the sort order an index was built under has changed"
        elif indexes.status == "diff":
            why = "an index exists that answers no query"
        else:
            why = ""
        res.append(self._duplicate_keys(db, why))
        res.append(self._temporal_meaning(db))
        res.append(self._time_zone_rules(db))
        res.append(self._capacity_gaps(db))

        res.append(self._fk_orphans(db))

        # NOT VALID check constraints enforce new writes but never scanned the
        # existing rows, and the planner distrusts them - a load-time speed
        # hack left unfinished. The fk orphan scan above covers foreign keys;
        # check constraints are the blind spot.
        nv = self._unvalidated_checks(db)
        nvc = [f"{tbl}.{name}" for tbl, name, _ in nv]
        if nvc:
            res.append(Result("deep", f"{db} checks", "diff",
                              f"{len(nvc)} check constraints NOT VALIDATED"
                              " (existing rows unchecked, planner distrusts): "
                              + "; ".join(nvc[:5]), "",
                              "migkit sync --kind schema --apply validates"
                              " them; it fails loudly if a row violates one"))
        else:
            res.append(Result("deep", f"{db} checks", "ok",
                              "all check constraints validated"))

        # some tooling drops DEFERRABLE / INITIALLY DEFERRED when copying a
        # schema; code that relies on deferred checks (bulk reorder inside one
        # transaction) then fails with a constraint violation that never
        # happened on the source. Diff the deferral flags per constraint.
        # each line after the table it is on, which decides its scope
        dfq = ("select tn.nspname||'.'||t.relname||chr(31)"
               "||conrelid::regclass::text||'.'||conname||'|'"
               "||condeferrable||'|'||condeferred from pg_constraint c"
               " join pg_namespace n on n.oid = c.connamespace"
               " join pg_class t on t.oid = c.conrelid"
               " join pg_namespace tn on tn.oid = t.relnamespace"
               " where c.contype in ('p','u','f','c') and c.conrelid <> 0"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'")

        def deferral(side):
            lines = [l.split("\x1f", 1)[1] for l in
                     self._scoped(db, self._psql(side, db, dfq), "\x1f")
                     if "\x1f" in l]
            return {l.split("|", 1)[0]: l.split("|", 1)[1] for l in lines
                    if "|" in l}
        sdf, ddf = deferral("src"), deferral("dst")
        defer = [f"{k}: src deferrable/deferred={sdf[k]} dst={ddf[k]}"
                 for k in sorted(sdf) if k in ddf and sdf[k] != ddf[k]]
        if defer:
            res.append(Result("deep", f"{db} deferrable", "diff",
                              f"{len(defer)} constraints changed deferral: "
                              + "; ".join(defer[:5]), "",
                              "alter table ... alter constraint ... deferrable"
                              " initially deferred to match source"))
        else:
            res.append(Result("deep", f"{db} deferrable", "ok",
                              "constraint deferral flags match"))

        # row-level security is a silent-data-loss trap: a non-owner /
        # non-BYPASSRLS role (which a dump or even migkit itself may connect
        # as) sees only policy-permitted rows, so counts and checksums can be
        # a filtered subset with no error. And RLS enabled with zero policies
        # is default-deny - the table looks empty to everyone but the owner.
        rls = [l.split("|") for l in self._scoped(db, self._psql("src", db,
               "select n.nspname||'.'||c.relname||'|'||"
               "(select count(*) from pg_policies p"
               " where p.schemaname = n.nspname and p.tablename = c.relname)"
               " from pg_class c join pg_namespace n on n.oid = c.relnamespace"
               " where c.relkind = 'r' and c.relrowsecurity"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'"))]
        if rls:
            # the same rule the counts and data passes use, asked once:
            # an owner reads its own tables in full unless they are FORCEd,
            # so checking only for superuser/BYPASSRLS called every owner
            # filtered
            filtered = self._filtered_tables("src", db) or []
            deny = [r[0] for r in rls if r[1] == "0"]
            msgs = []
            if deny:
                msgs.append(f"{len(deny)} RLS tables have ZERO policies"
                            " (default-deny, read as empty by non-owners): "
                            + ", ".join(deny[:4]))
            if filtered:
                msgs.append("migkit's source role is subject to RLS on"
                            f" {len(filtered)} tables - counts/checksums"
                            " there may be a filtered subset, not the full"
                            " data: " + ", ".join(filtered[:4]))
            res.append(Result("deep", f"{db} rls", "diff" if msgs else "ok",
                              "; ".join(msgs) if msgs
                              else f"{len(rls)} RLS tables, all have policies"
                                   " and migkit reads with a bypass role", "",
                              "verify RLS tables with an owner/BYPASSRLS role;"
                              " recreate missing policies on target"
                              if msgs else ""))
        else:
            res.append(Result("deep", f"{db} rls", "ok",
                              "no row-level security in use"))

        # a trigger by the table it sits on
        trg = [l.split("\t") for l in
               self._psql("dst", db, self.TARGET_TRIGGERS).splitlines()
               if l.count("\t") == 2 and self.in_scope(db, l.split("\t")[1])]
        dis = [name for name, table, state in trg if state == "D"]
        # only the tables a load would write: one that exists on the target
        # alone is never written, and naming it would be noise
        writable = set(self._scoped(db, self._psql("src", db,
                                                   self.USER_TABLES)))
        quieted = sorted(name for name, table, state in trg
                         if state != "D" and table in writable)
        res.append(self._trigger_result(
            db, sorted(dis), quieted,
            "nothing to do unless one of them does work the migrated rows"
            " need - an audit row, a maintained counter - in which case"
            " that work has to happen another way"))

        # each line after the table it is on, which decides its scope
        colq = ("select table_schema||'.'||table_name||chr(31)"
                "||table_schema||'.'||table_name||'.'||column_name"
                "||'|'||coalesce(data_type,'')||'|'||is_nullable"
                "||'|'||coalesce(column_default,'')"
                "||'|'||coalesce(character_maximum_length::text,'')"
                "||'|'||coalesce(numeric_precision::text,'')"
                "||'|'||coalesce(numeric_scale::text,'')"
                " from information_schema.columns"
                " where table_schema not in ('pg_catalog',"
                "'information_schema')"
                " and table_schema not like 'pg\\_%'"
                " and table_schema not like '\\_\\_%'"
                f" and table_name not in {OWN_SQL} order by 1")

        def columns(side):
            lines = [l.split("\x1f", 1)[1] for l in
                     self._scoped(db, self._psql(side, db, colq), "\x1f")
                     if "\x1f" in l]
            return {l.split("|", 1)[0]: l for l in lines}
        sc, dc = columns("src"), columns("dst")
        ignores = self._ignore_patterns()
        drift = [f"src {sc[k]}\ndst {dc[k]}" for k in sorted(sc)
                 if k in dc and sc[k] != dc[k]
                 and not any(p.search(sc[k]) or p.search(dc[k])
                             for p in ignores)]
        if drift:
            out = rpt / "deep-columns.diff"
            out.write_text("\n\n".join(drift) + "\n")
            heads = [d.splitlines()[0].split("|")[0][4:] for d in drift]
            res.append(Result("deep", f"{db} columns", "diff",
                              f"{len(drift)} columns drift"
                              f" (type/null/default/precision):"
                              f" {', '.join(heads[:4])}", str(out),
                              "see deep-columns.diff, align target DDL"))
        else:
            res.append(Result("deep", f"{db} columns", "ok",
                              f"{len(sc)} columns compared, type/null/"
                              "default/precision identical"))

        # narrowing is the dangerous subset of drift: a target column that
        # holds fewer characters, less numeric scale/precision, or a smaller
        # integer than the source silently truncates, rounds, or overflows
        # values (DMS caps unlimited text at varchar(8000); scale loss eats
        # money). Called out on its own, at higher severity than cosmetic drift.
        # The reasoning is the one every engine's deep check asks
        # (`verdict.narrowing`), over the declared types read here.
        from .. import verdict

        def declared(line):
            # from the right: a column's default may hold the separator
            head, cm, pr, sca = (["", "", ""] + line.rsplit("|", 3))[-4:]
            t = (head.split("|") + [""])[1]
            if cm and t in ("character varying", "character"):
                return f"{t}({cm})"
            if pr and t in ("numeric", "decimal"):
                return f"{t}({pr},{sca or 0})"
            return t
        narrow = []
        for k in sorted(sc):
            if k not in dc:
                continue
            why = verdict.narrowing("postgres", declared(sc[k]),
                                    declared(dc[k]))
            if why:
                narrow.append(f"{k}: {why}")
        res.append(verdict.narrowing_result(db, narrow))

        # a constant, per-row offset on a timestamp column is the fingerprint
        # of a timezone conversion bug (a mover applying a non-UTC session),
        # not random corruption. Both sides are read with TimeZone=UTC pinned,
        # so a correct instant compares as delta 0; a systematic shift shows
        # the same non-zero delta on every sampled row.
        tsq = ("select n.nspname||'.'||c.relname||'|'||a.attname"
               " from pg_attribute a"
               " join pg_class c on c.oid = a.attrelid"
               " join pg_namespace n on n.oid = c.relnamespace"
               " join pg_type ty on ty.oid = a.atttypid"
               " where c.relkind = 'r' and a.attnum > 0"
               " and not a.attisdropped"
               " and ty.typname in ('timestamp','timestamptz')"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'"
               f" and c.relname not in {OWN_SQL} order by 1")
        shifts = []
        for tc in self._scoped(db, self._psql("src", db, tsq))[:10]:
            tbl, col = tc.split("|", 1)
            pks = self._pk_cols_of(db, tbl)
            if len(pks) != 1:
                continue
            sch2, t2 = self._split(tbl)
            pk = pks[0]
            q = (f'select {_ident(pk)}::text||e\'\\t\'||extract(epoch from {_ident(col)})'
                 f' from {_ident(sch2)}.{_ident(t2)} where {_ident(col)} is not null'
                 f' order by {_ident(pk)} limit 200')
            try:
                sm = dict(l.split("\t") for l in
                          self._psql("src", db, q).splitlines() if "\t" in l)
                dm = dict(l.split("\t") for l in
                          self._psql("dst", db, q).splitlines() if "\t" in l)
            except (RuntimeError, ValueError):
                continue
            line = verdict.uniform_shift(f"{tbl}.{col}", sm, dm)
            if line:
                shifts.append(line)
        res.append(verdict.timeshift_result(db, shifts))

        # NULL vs empty-string: Oracle stores '' as NULL, Postgres keeps them
        # distinct, so a migration can silently flip IS NULL semantics and
        # unique behavior. Per text column, compare the null-count and the
        # empty-count on each side; a swap is the fingerprint (and it survives
        # a row checksum only because both are "present").
        txtq = ("select n.nspname||'.'||c.relname||'|'||a.attname"
                " from pg_attribute a"
                " join pg_class c on c.oid = a.attrelid"
                " join pg_namespace n on n.oid = c.relnamespace"
                " join pg_type ty on ty.oid = a.atttypid"
                " where c.relkind = 'r' and a.attnum > 0"
                " and not a.attisdropped"
                " and ty.typname in ('text','varchar','bpchar')"
                " and n.nspname not in ('pg_catalog','information_schema')"
                " and n.nspname not like 'pg\\_%'"
                " and n.nspname not like '\\_\\_%'"
                f" and c.relname not in {OWN_SQL} order by 1")
        neq = []
        for tc in self._scoped(db, self._psql("src", db, txtq))[:30]:
            tbl, col = tc.split("|", 1)
            sch2, t2 = self._split(tbl)
            q = (f'select count(*) filter (where {_ident(col)} is null)||\'|\'||'
                 f'count(*) filter (where {_ident(col)} = \'\') from {_ident(sch2)}.{_ident(t2)}')
            try:
                sv, dv = self._psql("src", db, q), self._psql("dst", db, q)
            except RuntimeError:
                continue
            if sv != dv and "|" in sv and "|" in dv:
                neq.append(f"{tbl}.{col}: src null/empty={sv} dst={dv}")
        if neq:
            res.append(Result("deep", f"{db} nullempty", "diff",
                              f"{len(neq)} text columns differ in NULL vs"
                              " empty-string split (semantic flip): "
                              + "; ".join(neq[:5]), "",
                              "normalize with NULLIF(col,'') / COALESCE per"
                              " column intent; decide which side is canonical"))
        else:
            res.append(Result("deep", f"{db} nullempty", "ok",
                              "NULL vs empty-string consistent on text columns"))

        # charset corruption: a lossy transcode on load leaves U+FFFD
        # replacement characters, so any excess on the target over the source
        # is silent damage; and a SQL_ASCII target validates nothing, letting
        # bad bytes through. (utf8mb4/latin1 traps are mysql-specific.)
        enc = []
        se = self._psql("src", db, "select pg_encoding_to_char(encoding)"
                        " from pg_database where datname = current_database()")
        de = self._psql("dst", db, "select pg_encoding_to_char(encoding)"
                        " from pg_database where datname = current_database()")
        if de == "SQL_ASCII" and se != "SQL_ASCII":
            enc.append(f"target database is SQL_ASCII (no encoding"
                       f" validation); source is {se}")
        elif se != de:
            enc.append(f"server_encoding differs: src={se} dst={de}")
        for tc in self._scoped(db, self._psql("src", db, txtq))[:30]:
            tbl, col = tc.split("|", 1)
            sch2, t2 = self._split(tbl)
            c = _ident(col)
            q = (f"select coalesce(sum(char_length({c}) - char_length("
                 f"replace({c}, U&'\\FFFD', ''))),0) from"
                 f" {_ident(sch2)}.{_ident(t2)}")
            try:
                sv = int(self._psql("src", db, q) or 0)
                dv = int(self._psql("dst", db, q) or 0)
            except (RuntimeError, ValueError):
                continue
            if dv > sv:
                enc.append(f"{tbl}.{col}: {dv - sv} extra U+FFFD replacement"
                           " chars on target")
        if enc:
            res.append(Result("deep", f"{db} encoding", "diff",
                              "; ".join(enc[:6]), "",
                              "re-load affected rows with a non-lossy client"
                              " encoding; never use a SQL_ASCII target"))
        else:
            res.append(Result("deep", f"{db} encoding", "ok",
                              "encodings match, no replacement-char excess"))

        # partitioned tables: a mover can land rows in the DEFAULT catch-all
        # partition, miss a partition bound entirely, or even change the
        # partition key - all of which reroute or strand data silently. And
        # a partition can arrive empty while its parent's total looks
        # plausible, so each is held to its own count and digest.
        res.append(self._deep_partitions(db))

        # generated columns: a bulk load can write a literal into a stored
        # generated column (so it no longer equals its expression), the
        # expression can drift, or a column generated on the source can arrive
        # plain on the target (then a mover happily inserts rotting literals).
        import re as _re
        genq = ("select c.relnamespace::regnamespace||'.'||c.relname"
                "||'|'||a.attname||'|'||a.attgenerated::text"
                "||'|'||coalesce(pg_get_expr(d.adbin, d.adrelid),'')"
                " from pg_attribute a"
                " join pg_class c on c.oid = a.attrelid"
                " left join pg_attrdef d on d.adrelid = a.attrelid"
                "  and d.adnum = a.attnum"
                " where a.attnum > 0 and not a.attisdropped"
                "  and a.attgenerated <> '' and c.relkind = 'r'"
                " and c.relnamespace::regnamespace::text"
                "  not in ('pg_catalog','information_schema')"
                " and c.relnamespace::regnamespace::text not like 'pg\\_%'"
                " and c.relnamespace::regnamespace::text not like '\\_\\_%'")

        def _genmap(side):
            out = {}
            for l in self._scoped(db, self._psql(side, db, genq)):
                p = l.split("|", 3)
                if len(p) == 4:
                    out[p[0] + "|" + p[1]] = (p[2], p[3])
            return out

        def _norm(e):
            return _re.sub(r"\s+", "", e).lower()
        sg, dg = _genmap("src"), _genmap("dst")
        gbad = []
        for k, (gen, expr) in sorted(sg.items()):
            tbl, col = k.split("|")
            if k not in dg:
                gbad.append(f"{tbl}.{col}: generated on source,"
                            " plain/missing on target")
                continue
            dexpr = dg[k][1]
            if _norm(expr) != _norm(dexpr):
                gbad.append(f"{tbl}.{col}: generation expression differs")
                continue
            if gen == "s":
                sch2, t2 = self._split(tbl)
                try:
                    n = self._psql("dst", db, f'select count(*) from'
                                   f' {_ident(sch2)}.{_ident(t2)} where {_ident(col)}'
                                   f" is distinct from ({dexpr})")
                    if n and int(n) > 0:
                        gbad.append(f"{tbl}.{col}: {n} rows where the stored"
                                    " value != its expression")
                except (RuntimeError, ValueError):
                    pass
        for k in sorted(dg):
            if k not in sg:
                tbl, col = k.split("|")
                gbad.append(f"{tbl}.{col}: generated on target but plain on"
                            " source")
        if gbad:
            res.append(Result("deep", f"{db} generated", "diff",
                              "; ".join(gbad[:6]), "",
                              "align the generation expression / storage on"
                              " target and re-derive the column"))
        else:
            res.append(Result("deep", f"{db} generated", "ok",
                              f"{len(sg)} generated columns match"
                              if sg else "no generated columns"))

        # collation: a unique/pk text column whose target collation is
        # case/accent-insensitive (or just different) can COLLAPSE distinct
        # source rows into duplicates on load - silent data loss. And a
        # glibc/ICU version drift silently corrupts existing btree unique
        # indexes (wrong results, duplicate admission).
        uq = ("select c.relnamespace::regnamespace||'.'||c.relname"
              "||'|'||a.attname||'|'||coalesce(co.collname::text,'default')"
              " from pg_constraint con"
              " join pg_class c on c.oid = con.conrelid"
              " join pg_attribute a on a.attrelid = con.conrelid"
              "  and a.attnum = con.conkey[1]"
              " left join pg_collation co on co.oid = a.attcollation"
              " where con.contype in ('p','u')"
              " and array_length(con.conkey,1) = 1"
              " and a.atttypid in ('text'::regtype,'varchar'::regtype,"
              "'bpchar'::regtype)"
              " and c.relnamespace::regnamespace::text"
              "  not in ('pg_catalog','information_schema')"
              " and c.relnamespace::regnamespace::text not like 'pg\\_%'"
              " and c.relnamespace::regnamespace::text not like '\\_\\_%'")

        def _uqmap(side):
            out = {}
            for l in self._scoped(db, self._psql(side, db, uq)):
                p = l.split("|")
                if len(p) == 3:
                    out[p[0] + "|" + p[1]] = p[2]
            return out
        su, du = _uqmap("src"), _uqmap("dst")
        cbad = []
        for k, scoll in sorted(su.items()):
            dcoll = du.get(k)
            if not dcoll or dcoll == scoll:
                continue
            tbl, col = k.split("|")
            sch2, t2 = self._split(tbl)
            detail = (f"{tbl}.{col}: unique-key collation src({scoll})"
                      f" != dst({dcoll})")
            if dcoll != "default":
                try:
                    n = self._psql("src", db,
                                   f'select count(*) from (select 1 from'
                                   f' {_ident(sch2)}.{_ident(t2)} group by {_ident(col)}'
                                   f' collate {_ident(dcoll)} having count(*) > 1) x')
                    if n and int(n) > 0:
                        detail += (f"; {n} source groups COLLAPSE to duplicates"
                                   " under the target collation (data loss)")
                except RuntimeError:
                    detail += "; collapse untestable (collation not on source)"
            cbad.append(detail)
        try:
            v = self._psql("dst", db,
                           "select count(*) from pg_depend d"
                           " where d.refclassid = 'pg_collation'::regclass"
                           " and d.refobjversion <> ''"
                           " and d.refobjversion <>"
                           " pg_collation_actual_version(d.refobjid)")
            if v and int(v) > 0:
                cbad.append(f"{v} target objects built under a stale collation"
                            " version (glibc/ICU drift) - unique indexes may be"
                            " corrupt; reindex then refresh collation version")
        except RuntimeError:
            pass
        if cbad:
            res.append(Result("deep", f"{db} collation", "diff",
                              "; ".join(cbad[:6]), "",
                              "match the unique-key collation to source (or"
                              " dedup first); reindex on version drift"))
        else:
            res.append(Result("deep", f"{db} collation", "ok",
                              "unique-key collations match, no version drift"))

        # float columns: FLOAT/DOUBLE are IEEE approximations, so a mover that
        # changes precision (float4->float8 widening, a text round-trip) drifts
        # them. Compare per-row with a relative tolerance, so genuine drift is
        # caught without false-flagging a bit-identical copy. This is a
        # diagnostic layer; the row checksum stays exact.
        tol = float(self.hop.options.get("float_tolerance", 1e-9))
        fcols = self._scoped(db, self._psql("src", db,
                 "select n.nspname||'.'||c.relname||'|'||a.attname"
                 " from pg_attribute a join pg_class c on c.oid = a.attrelid"
                 " join pg_namespace n on n.oid = c.relnamespace"
                 " join pg_type ty on ty.oid = a.atttypid"
                 " where c.relkind = 'r' and a.attnum > 0"
                 " and not a.attisdropped and ty.typname in ('float4','float8')"
                 " and n.nspname not in ('pg_catalog','information_schema')"
                 " and n.nspname not like 'pg\\_%'"
                 " and n.nspname not like '\\_\\_%'"
                 f" and c.relname not in {OWN_SQL}"))
        fbad = []
        for tc in fcols[:10]:
            tbl, col = tc.split("|", 1)
            pks = self._pk_cols_of(db, tbl)
            if len(pks) != 1:
                continue
            sch2, t2 = self._split(tbl)
            pk = pks[0]
            q = (f'select {_ident(pk)}::text||e\'\\t\'||{_ident(col)} from {_ident(sch2)}.{_ident(t2)}'
                 f' where {_ident(col)} is not null order by {_ident(pk)} limit 500')
            try:
                sm = dict(l.split("\t") for l in
                          self._psql("src", db, q).splitlines() if "\t" in l)
                dm = dict(l.split("\t") for l in
                          self._psql("dst", db, q).splitlines() if "\t" in l)
            except (RuntimeError, ValueError):
                continue
            worst, nd = 0.0, 0
            for k in sm:
                if k not in dm:
                    continue
                try:
                    a, b = float(sm[k]), float(dm[k])
                except ValueError:
                    continue
                d = abs(a - b)
                if d > tol * max(abs(a), abs(b), 1.0):
                    nd += 1
                    worst = max(worst, d)
            if nd:
                fbad.append(f"{tbl}.{col}: {nd} values drift beyond tolerance"
                            f" (max {worst:g})")
        if fbad:
            res.append(Result("deep", f"{db} float", "diff",
                              "; ".join(fbad[:5]), "",
                              "float precision changed on target (widening or"
                              " round-trip); use numeric for exact columns, or"
                              " set options.float_tolerance to accept it"))
        else:
            res.append(Result("deep", f"{db} float", "ok",
                              "float columns within tolerance"
                              if fcols else "no float columns"))

        # exotic types can render differently across builds; sample and
        # compare their actual text so a checksum can't hide it
        exq = ("select n.nspname||'.'||c.relname||'|'||a.attname"
               " from pg_attribute a"
               " join pg_class c on c.oid = a.attrelid"
               " join pg_namespace n on n.oid = c.relnamespace"
               " join pg_type ty on ty.oid = a.atttypid"
               " where c.relkind = 'r' and a.attnum > 0"
               " and not a.attisdropped"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'"
               f" and c.relname not in {OWN_SQL}"
               " and (ty.typtype in ('e','c','d','r','m') or ty.typname in"
               " ('money','point','polygon','path','circle','box','line',"
               "'lseg','tsvector','tsquery','xml','interval','bit','varbit'))")
        exotic = [l.split("|") for l in
                  self._scoped(db, self._psql("src", db, exq))][:20]

        _esc = _literal

        drifts = []
        audited = 0
        for tbl, col in exotic:
            pks = self._pk_cols_of(db, tbl)
            if not pks:
                continue
            sch2, t2 = self._split(tbl)
            pkexpr = self._pk_text_expr(pks)
            q = (f'select {pkexpr}||\'|\'||{_ident(col)}::text'
                 f' from {_ident(sch2)}.{_ident(t2)} where {_ident(col)} is not null limit 5')
            try:
                a = dict(l.split("|", 1) for l in
                         self._psql("src", db, q).splitlines() if "|" in l)
            except RuntimeError:
                continue
            if not a:
                continue
            audited += 1
            if len(pks) == 1:
                where = (f'{_ident(pks[0])}::text in ('
                         + ", ".join(_esc(k) for k in a) + ")")
            else:
                tup = ", ".join(f'{_ident(p)}::text' for p in pks)
                vals = ", ".join(
                    "(" + ", ".join(_esc(x) for x in self._pk_parts(k)) + ")"
                    for k in a)
                where = f"({tup}) in ({vals})"
            qd = (f'select {pkexpr}||\'|\'||{_ident(col)}::text'
                  f' from {_ident(sch2)}.{_ident(t2)} where {where}')
            try:
                b = dict(l.split("|", 1) for l in
                         self._psql("dst", db, qd).splitlines() if "|" in l)
            except RuntimeError:
                drifts.append(f"{tbl}.{col}: unreadable on target")
                continue
            for k, v in a.items():
                if k in b and b[k] != v:
                    drifts.append(f"{tbl}.{col}: renders differently"
                                  f" (src {v[:40]!r} dst {b[k][:40]!r})")
                    break
        res.append(Result("deep", f"{db} render",
                          "diff" if drifts else "ok",
                          "; ".join(drifts[:5]) if drifts
                          else (f"{audited} exotic-typed columns sampled,"
                                " rendering identical" if audited
                                else "no exotic-typed columns"), "",
                          "check type definitions/versions on target;"
                          " consider options.checksum: jsonb" if drifts
                          else ""))

        # a materialized view the hop names in its include or exclude, by
        # its own name
        mvs = self._scoped(db, self._psql("src", db,
               "select n.nspname||'.'||c.relname from pg_class c"
               " join pg_namespace n on n.oid = c.relnamespace"
               " where c.relkind = 'm'"
               " and n.nspname not like '\\_\\_%'"))
        stale = []
        for m in mvs:
            try:
                pop = self._psql("dst", db, "select relispopulated"
                                 f" from pg_class where oid = {_literal(m)}::regclass")
            except RuntimeError:
                stale.append(f"{m}: missing on target")
                continue
            if pop != "t":
                stale.append(f"{m}: not populated on target")
                continue
            # count misses a stale mv with the same size; checksum content
            a = self._content_fingerprint("src", db, m)
            b = self._content_fingerprint("dst", db, m)
            if a != b:
                stale.append(f"{m}: rows|checksum src={a} dst={b},"
                             " stale on target")
        res.append(Result("deep", f"{db} matviews",
                          "diff" if stale else "ok",
                          "; ".join(stale[:5]) if stale
                          else f"{len(mvs)} matviews populated and equal"
                          if mvs else "no matviews", "",
                          "refresh materialized view ... on target"
                          if stale else ""))

        # The same computation feeds the repair, so it lives in one place:
        # two copies of "which grants are missing" would eventually disagree,
        # and the disagreement would be a repair that grants the wrong thing.
        from ..users import NOT_CARRIED, carried
        taken = carried(self.hop)
        gaps = self._grant_gaps(db) if taken else None
        if not taken:
            res += [Result("deep", f"{db} {what}", "skip", NOT_CARRIED)
                    for what in ("grants", "seq-grants")]
        elif gaps is None:
            res.append(Result("deep", f"{db} grants", "warn",
                              "cannot read grants - unknown, not clean"))
        else:
            miss, extra, smiss, seen, sseen = gaps
            if miss or extra:
                res.append(Result("deep", f"{db} grants", "diff",
                                  (f"{len(miss)} grants missing on target"
                                   + (": " + "; ".join(
                                       self._grant_said(g) for g in miss[:3])
                                      if miss else "")
                                   + (f"; {len(extra)} extra: "
                                      + "; ".join(self._grant_said(g)
                                                  for g in extra[:3])
                                      if extra else "")),
                                  "", "migkit sync --kind schema --apply"
                                      " re-grants them, with undo"))
            else:
                res.append(Result("deep", f"{db} grants", "ok",
                                  f"{seen} grants match - tables, columns,"
                                  " schemas, the database, routines and"
                                  " default privileges (roles present both"
                                  " sides)"))
            if smiss:
                res.append(Result("deep", f"{db} seq-grants", "diff",
                                  f"{len(smiss)} sequence grants missing on"
                                  " target (inserts will hit 'permission"
                                  " denied for sequence'): "
                                  + "; ".join(self._grant_said(g)
                                              for g in smiss[:4]), "",
                                  "migkit sync --kind schema --apply grants"
                                  " them, with undo"))
            else:
                res.append(Result("deep", f"{db} seq-grants", "ok",
                                  f"{sseen} sequence grants match"
                                  if sseen else "no explicit sequence grants"))

        # a missing or version-mismatched extension breaks its functions and
        # can fail the restore outright; the mover copies data, not CREATE
        # EXTENSION. Diff the installed set (plpgsql is always present).
        exq = ("select extname||' '||extversion from pg_extension"
               " where extname <> 'plpgsql'")
        se = set(self._psql("src", db, exq).splitlines()) - {""}
        de = set(self._psql("dst", db, exq).splitlines()) - {""}
        dn = {e.split(" ")[0] for e in de}
        miss = sorted(e.split(" ")[0] for e in se if e.split(" ")[0] not in dn)
        vers = sorted(e for e in se if e.split(" ")[0] in dn and e not in de)
        if miss or vers:
            det = []
            if miss:
                det.append(f"{len(miss)} missing on target: "
                           + ", ".join(miss[:5]))
            if vers:
                det.append("version mismatch: " + ", ".join(vers[:3]))
            res.append(Result("deep", f"{db} extensions", "diff",
                              "; ".join(det), "",
                              "create extension ... on target (and install its"
                              " shared library) before the app depends on it"))
        else:
            res.append(Result("deep", f"{db} extensions", "ok",
                              f"{len(se)} extensions match" if se
                              else "no non-default extensions"))

        res.append(self._extension_data(db))
        res.append(self._large_objects(db))
        res += self._deep_ownership(db)

        pkq = ("select n.nspname||'|'||c.relname||'|'||a.attname"
               " from pg_index i"
               " join pg_class c on c.oid = i.indrelid"
               " join pg_namespace n on n.oid = c.relnamespace"
               " join pg_attribute a on a.attrelid = i.indrelid"
               " and a.attnum = i.indkey[0]"
               " where i.indisprimary and i.indnatts = 1"
               " and a.atttypid in ('smallint'::regtype::oid,"
               "'integer'::regtype::oid,'bigint'::regtype::oid)"
               " and c.relkind = 'r'"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'"
               f" and c.relname not in {OWN_SQL} order by 1")
        spk = [l.split("|") for l in
               self._psql("src", db, pkq).splitlines() if l]
        spk = [p for p in spk if self.in_scope(db, f"{p[0]}.{p[1]}")]
        dpk = {tuple(l.split("|")[:2]) for l in
               self._psql("dst", db, pkq).splitlines() if l}
        both = [(s, t, c) for s, t, c in spk if (s, t) in dpk]

        def maxes(side, triples):
            out = {}
            for i in range(0, len(triples), 200):
                parts = [f"select {_literal(f'{s}.{t}|')}"
                         f"||coalesce(max({_ident(c)}), 0)"
                         f" from {_ident(s)}.{_ident(t)}"
                         for s, t, c in triples[i:i + 200]]
                for l in self._psql(side, db,
                                    " union all ".join(parts)).splitlines():
                    k, _, v = l.rpartition("|")
                    out[k] = int(v)
            return out

        if both:
            am, bm = maxes("src", both), maxes("dst", both)
            ahead = [f"{k} src_max={am[k]} dst_max={bm[k]}"
                     for k in sorted(am) if bm.get(k, 0) > am[k]]
            behind = [f"{k} src_max={am[k]} dst_max={bm[k]}"
                      for k in sorted(am) if bm.get(k, 0) < am[k]]
            if ahead:
                res.append(Result("deep", f"{db} boundary", "diff",
                                  f"target max(pk) AHEAD of source on"
                                  f" {len(ahead)}: {'; '.join(ahead[:4])}",
                                  "", "writes landing on target or"
                                      " double-apply, find the writer"
                                      " before cutover"))
            else:
                note = (f"; {len(behind)} behind (replication lag):"
                        f" {'; '.join(behind[:3])}" if behind else "")
                res.append(Result("deep", f"{db} boundary", "ok",
                                  f"max(pk) checked on {len(both)} tables,"
                                  f" none ahead of source{note}"))
        else:
            res.append(Result("deep", f"{db} boundary", "ok",
                              "no single-int-pk tables to boundary-check"))
        return res

    OWNER_SQL = """
        select 'table '||n.nspname||'.'||c.relname||'|'
               ||pg_get_userbyid(c.relowner)
          from pg_class c join pg_namespace n on n.oid = c.relnamespace
         where c.relkind in ('r','p','v','m','S')
           and n.nspname not in ('pg_catalog','information_schema')
           and n.nspname not like 'pg\\_%' and n.nspname not like '\\_\\_%'
        union all
        select 'routine '||n.nspname||'.'||p.proname||'|'
               ||pg_get_userbyid(p.proowner)
          from pg_proc p join pg_namespace n on n.oid = p.pronamespace
         where n.nspname not in ('pg_catalog','information_schema')
           and n.nspname not like 'pg\\_%' and n.nspname not like '\\_\\_%'
        union all
        select 'schema '||n.nspname||'|'||pg_get_userbyid(n.nspowner)
          from pg_namespace n
         where n.nspname not in ('pg_catalog','information_schema')
           and n.nspname not like 'pg\\_%' and n.nspname not like '\\_\\_%'
        order by 1"""

    def _replay_lag(self, side, db):
        """Seconds this side is behind its writer, or None if it will not say.

        None rather than zero: a replica that hides its replay position is a
        replica of unknown freshness, and calling that "caught up" is the
        mistake this exists to prevent.
        """
        try:
            raw = self._psql(side, db,
                             "select coalesce(extract(epoch from"
                             " (now() - pg_last_xact_replay_timestamp()))"
                             ", -1)").strip()
            v = float(raw)
        except Exception:
            return None
        return None if v < 0 else v

    def _change_markers(self, side, db):
        """{table: a cheap proof that it has not moved}, every table of a
        side in one statement; a table missing, or {} where the server is
        not one whose statistics can be trusted.

        The pieces were chosen by measuring what each kind of change actually
        moves, on PostgreSQL 16:

            UPDATE / DELETE   the tuple counters move
            TRUNCATE          **the counters do not move at all** - only
                              relfilenode and the relation size do
            statistics reset  the counters drop to zero
            DROP / RENAME     nothing of those: the table's live columns
            COLUMN            (their count and a hash of each one's name,
                              type, null and collation) are in it for that

        So the counters alone would skip a truncated table and call it equal.
        `relfilenode` is in the marker for exactly that case, and the whole
        thing is compared for equality so a reset reads as "look again". A
        column dropped or renamed on the target rewrites nothing, and the
        check said "UNCHANGED since it was last proved equal" over a table
        that had lost a column's data; and the marker was four statements a
        table a side (audit 2026-10-01, SC4).

        Nothing is returned rather than a guess when the server is too old to
        trust: before 15 the statistics collector used UDP and could drop a
        message, and a dropped UPDATE is the false negative this exists to
        avoid.
        """
        from .. import unchanged as _u
        # Before the version, because a wire-compatible fork answers the
        # version question plausibly and the pieces underneath it are dead.
        # Measured on CockroachDB: empty pg_stat_all_tables, relfilenode 0
        # everywhere, and a reported server_version of 13.0.0 - so the marker
        # would be constant and the only thing refusing it is that number.
        if not _u.usable_brand(self._brands()[0 if side == "src" else 1]):
            return {}
        texts = self.__dict__.setdefault("_version_texts", {})
        try:
            if side not in texts:
                texts[side] = self._psql(side, db, "show server_version")
            if not _u.usable_postgres(texts[side]):
                return {}
            got = self._psql(side, db, """
                select n.nspname||'.'||c.relname||chr(31)
                       ||coalesce(s.n_tup_ins, -1)||chr(31)
                       ||coalesce(s.n_tup_upd, -1)||chr(31)
                       ||coalesce(s.n_tup_del, -1)||chr(31)
                       ||c.relfilenode||chr(31)
                       ||pg_relation_size(c.oid)||chr(31)
                       ||(select count(*)||':'||coalesce(md5(string_agg(
                                 a.attnum||':'||a.attname||':'||a.atttypid
                                 ||':'||a.atttypmod||':'||a.attnotnull::text
                                 ||':'||a.attcollation, ','
                                 order by a.attnum)), '')
                            from pg_attribute a
                           where a.attrelid = c.oid and a.attnum > 0
                             and not a.attisdropped)
                  from pg_class c
                  join pg_namespace n on n.oid = c.relnamespace
                  left join pg_stat_all_tables s on s.relid = c.oid
                 where """ + user_tables_where("all", own=True))
        except RuntimeError:
            return {}
        out = {}
        for line in got.splitlines():
            name, _, rest = line.partition("\x1f")
            if name:
                out[name] = _u.marker(rest.split("\x1f"))
        return out

    def _change_marker(self, side, db, table):
        """One table's marker (`_change_markers`), or None."""
        sch, tbl = self._split(table)
        return self._change_markers(side, db).get(f"{sch}.{tbl}")

    def _unvalidated_checks(self, db, kinds="c", side="dst"):
        """[(table, constraint, definition)] left NOT VALID on the target.

        One computation for the check and the repair. The definition comes
        along because it is the only way back: PostgreSQL has no statement
        that un-validates a constraint, so the undo has to drop it and add it
        again exactly as it was - and `pg_get_constraintdef` includes the
        NOT VALID, so the recreated constraint is the same object in the same
        state.
        """
        want = ", ".join(f"{_literal(k)}" for k in kinds)
        rows = self._psql(side, db, f"""
            select tn.nspname||'.'||t.relname||chr(31)
                   ||conrelid::regclass::text||chr(31)||conname||chr(31)
                   ||pg_get_constraintdef(c.oid)
              from pg_constraint c
              join pg_namespace n on n.oid = c.connamespace
              join pg_class t on t.oid = c.conrelid
              join pg_namespace tn on tn.oid = t.relnamespace
             where c.contype in ({want}) and not c.convalidated
               and n.nspname not like '\\_\\_%'
             order by conrelid::regclass::text, conname""").splitlines()
        # only the tables the hop takes: the repair validates what this
        # returns, and a table the hop leaves out is the target's own
        return [tuple(r.split("\x1f", 3)[1:])
                for r in self._scoped(db, rows, "\x1f")
                if r.count("\x1f") == 3]

    def _constraint_repair(self, db):
        """Finish the validation the load skipped.

        A NOT VALID check constraint enforces new writes and was never checked
        against the rows already there, so the planner will not use it and
        nobody knows whether the existing data satisfies it. Validating scans
        the table under ShareUpdateExclusiveLock - no reads or writes are
        blocked, which `structural-fix.locks.txt` says for itself.

        Measured: if a row violates the constraint, PostgreSQL refuses with
        `check constraint "..." of relation "..." is violated by some row` and
        changes nothing. So this needs no pre-scan of its own - the server
        already performs one, and failing is the correct outcome.
        """
        # foreign keys too, which a load leaves NOT VALID for the same
        # reason; and only where the source's own is validated - one the
        # source keeps NOT VALID on purpose is left as the source has it
        left = {(t, n) for t, n, _ in
                self._unvalidated_checks(db, "cf", side="src")}
        nv = [c for c in self._unvalidated_checks(db, "cf")
              if (c[0], c[1]) not in left]
        if not nv:
            return None
        stmts, undo = [], []
        for tbl, name, definition in nv:
            stmts.append(f'ALTER TABLE {tbl} VALIDATE CONSTRAINT {_ident(name)};')
            # no statement un-validates a constraint, so the way back is to
            # put the original one back exactly as it was
            undo.append(f'ALTER TABLE {tbl} DROP CONSTRAINT {_ident(name)};')
            undo.append(f'ALTER TABLE {tbl} ADD CONSTRAINT {_ident(name)}'
                        f' {definition};')
        return RepairAction(db, "constraints", stmts, undo,
                            f"{len(nv)} check and foreign key constraints"
                            " the load left unvalidated")

    def _grants_sql(self, side, db):
        """Every privilege granted on a side, one statement: tables and
        views, sequences, schemas, the database, functions and procedures,
        columns and the default privileges new objects get - each row the
        grantee (`PUBLIC` for everyone), the kind, the object as SQL names
        it, the column, the privilege and whether it may be granted on,
        split by `chr(31)`. Read raw rather than through
        `information_schema.table_privileges`: that view hides grants whose
        grantee the connected user is not a member of, which reads as
        missing on a target reached with a plain application user.

        What each role may do there: an object nobody granted on has the
        server's defaults (`acldefault`) - a function its EXECUTE for
        everyone, a database its CONNECT - so a source that took that from
        PUBLIC is told from a target that still gives it. The owner's own
        privileges are the owner's, not a grant, and read as `(owner)`:
        who owns what is the ownership check's (`_deep_ownership`)."""
        v = self._server_version(side, db) or 0
        routine = ("case p.prokind when 'p' then 'PROCEDURE' else"
                   " 'FUNCTION' end" if v >= 110000 else "'FUNCTION'")
        from ..users import system_principals_sql
        ignore = [r.strip() for r in os.environ.get(
            "GRANTS_IGNORE_ROLES", "root,rdsadmin").split(",") if r.strip()]
        # the roles a server or its provider keeps for itself, by the one
        # list `users` reads too (audit 2026-10-01, DU13)
        skip = f" and not {system_principals_sql('postgres', 'g.who')}" + (
            f" and g.who not in ({', '.join(_literal(r) for r in ignore)})"
            if ignore else "")
        return f"""
with ns as (
  select oid, nspname from pg_namespace
   where nspname not in ('pg_catalog', 'information_schema')
     and nspname not like 'pg\\_%' and nspname not like '\\_\\_%'),
acl as (
  select case c.relkind when 'S' then 'SEQUENCE' else 'TABLE' end k,
         format('%I.%I', ns.nspname, c.relname) o, '' col,
         ns.nspname s, c.relname n, c.relowner w,
         coalesce(c.relacl, acldefault(case c.relkind when 'S' then 's'
                                       else 'r' end, c.relowner)) a
    from pg_class c join ns on ns.oid = c.relnamespace
   where c.relkind in ('r', 'p', 'v', 'm', 'f', 'S')
     and c.relname not in {OWN_SQL}
  union all
  select 'COLUMN', format('%I.%I', ns.nspname, c.relname),
         quote_ident(t.attname), ns.nspname, c.relname, c.relowner,
         t.attacl
    from pg_attribute t join pg_class c on c.oid = t.attrelid
    join ns on ns.oid = c.relnamespace
   where t.attacl is not null and t.attnum > 0 and not t.attisdropped
     and c.relname not in {OWN_SQL}
  union all
  select 'SCHEMA', quote_ident(ns.nspname), '', ns.nspname, '', n.nspowner,
         coalesce(n.nspacl, acldefault('n', n.nspowner))
    from pg_namespace n join ns on ns.oid = n.oid
  union all
  select 'DATABASE', '', '', '', '', datdba,
         coalesce(datacl, acldefault('d', datdba))
    from pg_database where datname = current_database()
  union all
  select {routine}, p.oid::regprocedure::text, '', ns.nspname, p.proname,
         p.proowner, coalesce(p.proacl, acldefault('f', p.proowner))
    from pg_proc p join ns on ns.oid = p.pronamespace
   where not exists (select 1 from pg_depend d
                      where d.classid = 'pg_proc'::regclass
                        and d.objid = p.oid and d.deptype = 'e')
  union all
  select 'DEFAULT ' || case d.defaclobjtype when 'r' then 'TABLES'
             when 'S' then 'SEQUENCES' when 'f' then 'FUNCTIONS'
             when 'T' then 'TYPES' else 'SCHEMAS' end,
         quote_ident(pg_get_userbyid(d.defaclrole))
           || coalesce(' IN SCHEMA ' || quote_ident(n.nspname), ''),
         '', coalesce(n.nspname, ''), '', d.defaclrole, d.defaclacl
    from pg_default_acl d
    left join pg_namespace n on n.oid = d.defaclnamespace)
select g.who||chr(31)||acl.k||chr(31)||acl.o||chr(31)||acl.col||chr(31)
       ||g.privilege_type||chr(31)||g.is_grantable::text||chr(31)||acl.s
       ||chr(31)||acl.n
  from acl cross join lateral (
       select case when x.grantee = 0 then 'PUBLIC'
                   when x.grantee = acl.w then '(owner)'
                   else pg_get_userbyid(x.grantee) end who,
              x.privilege_type, x.is_grantable
         from aclexplode(acl.a) x) g
 where true{skip}"""

    def _grant_gaps(self, db):
        """(missing, extra, missing_sequence, n_seen, n_seq_seen), or None.

        One computation, used by the deep check and by the repair. When these
        lived apart the repair did not exist at all; the moment it did, two
        copies of "which grants are missing" would have been two chances to
        grant the wrong thing to the wrong role.

        Every kind of privilege, one statement a side (`_grants_sql`): only
        table and sequence grants were compared, so a schema's USAGE, a
        database's CONNECT, a function's EXECUTE, a column's grant, the
        default privileges the application's new tables get after the
        cutover, a grant to PUBLIC and a grant's WITH GRANT OPTION were
        never looked at (audit 2026-10-01, SC11). Each entry is the
        grantee, kind, object, column, privilege and grant option joined by
        `chr(31)`; the tables the hop leaves out, and those the movers keep
        for themselves (`noise_prefix`), are not the application's grants.
        """
        noise = self._noise()

        def read(side):
            out = set()
            left = self._object_left_out(side, db)
            for line in self._psql(side, db,
                                   self._grants_sql(side, db)).splitlines():
                parts = line.split("\x1f")
                if len(parts) != 8:
                    continue
                who, kind, obj, col, priv, grantable, sch, name = parts
                if kind in ("TABLE", "SEQUENCE", "COLUMN") and (
                        noise and name.startswith(noise)):
                    continue
                # a table's, a sequence's, a column's and a routine's grant
                # by the object it is on; the schema's, the database's and
                # the default privileges are of what the hop takes too
                if kind in ("TABLE", "SEQUENCE", "COLUMN", "FUNCTION",
                            "PROCEDURE") and left(kind, f"{sch}.{name}"):
                    continue
                out.add("\x1f".join((who, kind, obj, col, priv, grantable)))
            return out
        try:
            ga, gb = read("src"), read("dst")
            droles = set(self._psql("dst", db, "select rolname from"
                                               " pg_roles").splitlines())
            sroles = set(self._psql("src", db, "select rolname from"
                                               " pg_roles").splitlines())
        except RuntimeError:
            return None
        droles.add("PUBLIC")
        sroles.add("PUBLIC")

        def present(entry, roles):
            # a grant to a role the side does not have is a missing role,
            # which `users` reports; granting to it here would just fail -
            # and a default privilege is given on behalf of its own role
            who, kind, obj = entry.split("\x1f")[:3]
            if who not in roles:
                return False
            return not kind.startswith("DEFAULT ") or \
                obj.split(" IN SCHEMA ")[0].strip('"') in roles
        seq = {g for g in ga | gb if g.split("\x1f")[1] == "SEQUENCE"}
        miss = sorted(g for g in ga - gb - seq if present(g, droles))
        extra = sorted(g for g in gb - ga - seq if present(g, sroles))
        smiss = sorted(g for g in (ga - gb) & seq if present(g, droles))
        return miss, extra, smiss, len(ga - seq), len(ga & seq)

    @staticmethod
    def _grant_said(entry):
        """One `_grant_gaps` entry in words."""
        who, kind, obj, col, priv, grantable = entry.split("\x1f")
        return (f"{who} {priv}"
                + (" (with grant option)" if grantable == "true" else "")
                + f" on {kind.lower()} {obj or 'the database'}"
                + (f" ({col})" if col else ""))

    def _grant_sql(self, entry, revoke=False, db=None):
        """One GRANT (or its REVOKE) from a `_grant_gaps` entry, on the
        target: a table's, a sequence's (its kind spelled out: measured on
        PostgreSQL 16 the bare form is taken for one too, but the kind is
        known here), a column's, a schema's, the target database's, a
        routine's, or a default privilege's; WITH GRANT OPTION where the
        source's grant has it."""
        who, kind, obj, col, priv, grantable = entry.split("\x1f")
        grantee = "PUBLIC" if who == "PUBLIC" else _ident(who)
        opt = " WITH GRANT OPTION" if grantable == "true" else ""
        if kind.startswith("DEFAULT "):
            what = kind[len("DEFAULT "):]
            if revoke:
                return (f"ALTER DEFAULT PRIVILEGES FOR ROLE {obj} REVOKE"
                        f" {priv} ON {what} FROM {grantee};")
            return (f"ALTER DEFAULT PRIVILEGES FOR ROLE {obj} GRANT {priv}"
                    f" ON {what} TO {grantee}{opt};")
        if kind == "DATABASE":
            target = f"DATABASE {_ident(self._d('dst', db))}"
        elif kind == "COLUMN":
            target = f"TABLE {obj}"
            priv = f"{priv} ({col})"
        else:
            target = f"{kind} {obj}"
        if revoke:
            return f"REVOKE {priv} ON {target} FROM {grantee};"
        return f"GRANT {priv} ON {target} TO {grantee}{opt};"

    def _grant_repair(self, db):
        """Re-grant what the mover did not carry, with a REVOKE to undo it.

        Extra grants on the target are reported by the check but not revoked
        here. Removing a privilege somebody may have added on purpose is a
        different decision from restoring one the migration dropped, and doing
        both under one word would hide the second inside the first.

        Nothing on a hop whose move does not carry grants (`users.carried`):
        the check does not compare them, and the repair does not make them.
        """
        from ..users import carried
        gaps = self._grant_gaps(db) if carried(self.hop) else None
        if not gaps:
            return None
        miss, _extra, smiss, _, _ = gaps
        if not (miss or smiss):
            return None
        stmts = [self._grant_sql(g, db=db) for g in miss + smiss]
        undo = [self._grant_sql(g, revoke=True, db=db) for g in miss + smiss]
        note = (f"{len(miss)} grants and {len(smiss)} sequence grants the"
                " mover did not carry")
        return RepairAction(db, "grants", stmts, undo, note)

    #: leaf partitions read in one statement at most: the statement is one
    #: argument to psql, and Linux caps a single argument at 128 KB
    PARTITIONS_PER_READ = 200

    def _deep_partitions(self, db):
        """Every partitioned table's key on both sides, and every leaf
        partition under it held to its own count and digest
        (`Engine._partitions_verdict`)."""
        try:
            return self._partitions_compared(db)
        except Exception as e:  # noqa: BLE001 - said, and the rest runs
            return Result("deep", f"{db} partitions", "error",
                          "could not read the partitions:"
                          f" {str(e).splitlines()[-1][:120]}")

    def _partitions_compared(self, db):
        def keyed(side):
            out = {}
            for line in self._psql(side, db,
                    "select n.nspname||'.'||c.relname"
                    "||chr(31)||pg_get_partkeydef(c.oid)"
                    "||chr(31)||c.relispartition::int"
                    " from pg_partitioned_table p"
                    " join pg_class c on c.oid = p.partrelid"
                    " join pg_namespace n on n.oid = c.relnamespace"
                    " where n.nspname not like 'pg\\_%'"
                    " and n.nspname not like '\\_\\_%'"
                    " and n.nspname <> 'information_schema'").splitlines():
                name, key, sub = (line.split("\x1f") + ["", ""])[:3]
                if key and self._keep_tbl(db, name):
                    out[name] = (key, sub == "1")
            return out
        src, dst = keyed("src"), keyed("dst")
        plain = {side: set(self._psql(side, db, self.USER_TABLES).splitlines())
                 for side in ("src", "dst")}
        scheme, tables, unread = [], {}, []
        for t, (key, sub) in sorted(src.items()):
            if t not in dst:
                scheme.append(f"{t}: not partitioned on target (was {key})"
                              if t in plain["dst"] else
                              f"{t}: missing on target (partitioned by {key}"
                              " on the source)")
                continue
            if dst[t][0] != key:
                scheme.append(f"{t}: partition key differs"
                              f" src({key}) dst({dst[t][0]})")
            if sub:
                # its leaves are read from the table at the top
                continue
            try:
                sc, dc, notes = self._comparable_columns(db, self, t, self, t)
            except Exception as e:  # noqa: BLE001 - counted, then
                sc, dc = [], []
                notes = [str(e).splitlines()[-1][:90]]
            if notes:
                unread.append(f"{t}: {'; '.join(notes)}")
            sch, tbl = self._split(t)
            where = (self.hop.row_filter(db, sch, tbl)
                     if hasattr(self.hop, "row_filter") else None)
            tables[t] = (self._partition_leaves("src", db, t, sc, where),
                         self._partition_leaves("dst", db, t, dc, where))
        for t, (key, _) in sorted(dst.items()):
            if t not in src and t in plain["src"]:
                scheme.append(f"{t}: partitioned on target ({key}), not on"
                              " source")
        return self._partitions_verdict(db, tables, scheme, "DEFAULT", unread)

    def _partition_leaves(self, side, db, parent, columns, where):
        """{leaf partition: {"rows", "digest", "bound", "catchall"}} under
        `parent` on one side - a partition that is itself partitioned holds
        no rows of its own, so its leaves are what is read - with every
        leaf's count and digest in one statement."""
        from .. import canon
        sch, tbl = self._split(parent)
        lit = f"{_ident(sch)}.{_ident(tbl)}"
        got = self._psql(side, db,
            "with recursive tree(relid) as ("
            f" select inhrelid from pg_inherits where inhparent = {_literal(lit)}"
            "::regclass union all select i.inhrelid from pg_inherits i"
            " join tree t on i.inhparent = t.relid)"
            " select n.nspname||chr(31)||c.relname||chr(31)"
            "||coalesce(pg_get_expr(c.relpartbound, c.oid), '')"
            " from tree join pg_class c on c.oid = tree.relid"
            " join pg_namespace n on n.oid = c.relnamespace"
            " where c.relkind <> 'p' order by 1")
        leaves = [l.split("\x1f") for l in got.splitlines()
                  if l.count("\x1f") == 2]
        digest = (canon.digest_expr("postgres",
                                    canon.row_expr("postgres", columns))
                  + "::text" if columns else "''")
        out = {}
        for at in range(0, len(leaves), self.PARTITIONS_PER_READ):
            reads = []
            for lsch, lname, bound in leaves[at:at + self.PARTITIONS_PER_READ]:
                name = f"{lsch}.{lname}"
                out[name] = {"rows": 0, "digest": None, "bound": bound,
                             "catchall": bound == "DEFAULT"}
                reads.append(
                    f"select {_literal(name)}"
                    f"||chr(31)||count(*)::text||chr(31)||{digest}"
                    f" from {self._quote_ident(lsch)}"
                    f".{self._quote_ident(lname)}" + self._where(where))
            for line in self._psql(side, db, " union all ".join(reads)
                                   ).splitlines():
                name, rows, dig = (line.split("\x1f") + ["", ""])[:3]
                if name in out:
                    out[name]["rows"] = int(rows or 0)
                    out[name]["digest"] = dig or None
        return out

    def _deep_ownership(self, db):
        """Object owners, which the structural differ does not compare.

        Tencent's PostgreSQL migration notes say objects created by `postgres`
        on the source change ownership to the migration account on the target.
        Measured: a table owned by `appowner` arrived owned by
        `dts_migration`, and the generated fix contained no `OWNER TO` at all -
        `results` compares definitions, and an owner is not part of one.

        The owner is the role that may ALTER or DROP the object. A target
        where the application role no longer owns its own tables looks correct
        until the first migration the application tries to run on itself.

        A function's SECURITY DEFINER flag is deliberately not checked here:
        it is part of the definition, so the structural diff already catches
        it, and checking it twice is how two copies drift apart.
        """
        from .. import ownership as _own

        def owners(side):
            # a schema's owner stays: it is the schema of what is taken
            out = self._object_left_out(side, db)
            got = {}
            for l in self._psql(side, db, self.OWNER_SQL).splitlines():
                if "|" not in l:
                    continue
                what, owner = l.split("|", 1)
                kind, _, name = what.partition(" ")
                if kind != "schema" and out(kind, name):
                    continue
                got[what] = owner
            return got
        try:
            a, b = owners("src"), owners("dst")
        except RuntimeError as e:
            return [Result("deep", f"{db} ownership", "warn",
                           f"cannot read owners: {str(e).splitlines()[-1][:90]}"
                           " - unknown, not clean")]
        shared = sorted(set(a) & set(b))
        if not shared:
            return [Result("deep", f"{db} ownership", "skip",
                           "no objects present on both sides to compare")]
        changes = _own.group([(n, a[n], b[n]) for n in shared])
        if not changes:
            return [Result("deep", f"{db} ownership", "ok",
                           f"{len(shared)} objects keep their owner")]
        note = ("one substitution across every object - most likely the mover"
                " reassigning what it created"
                if _own.systematic(changes) else
                "objects drifted individually - read each one")
        return [Result("deep", f"{db} ownership", "diff",
                       f"{_own.total(changes)} of {len(shared)}:"
                       f" {_own.describe(changes)}", "",
                       f"{note}; ALTER ... OWNER TO the intended role, or the"
                       " application cannot alter its own objects")]

    def repair_plan(self, db, kind):
        actions = []
        if kind in ("sequences", "all"):
            q = ("select schemaname||'.'||sequencename||'|'||coalesce(last_value,0)"
                 " from pg_sequences"
                 " where schemaname not like '\\_\\_%'"
                 f" and sequencename not in {OWN_SQL}")
            src = dict(l.rsplit("|", 1) for l in
                       self._psql("src", db, q).splitlines() if l)
            dst = dict(l.rsplit("|", 1) for l in
                       self._psql("dst", db, q).splitlines() if l)
            owned = self._seq_owned("dst", db)
            dmax = self._seq_col_max("dst", db, owned)
            # The same column max on the source: a target sitting above its own
            # sequence is only suspicious when the source does not do it too.
            smax = self._seq_col_max("src", db, owned)
            noise = self._noise()
            # a sequence of a table the hop leaves out is that table's
            # writer's: neither read against the source nor set
            gone = self._seq_left_out("src", db, list(src))
            src = {n: v for n, v in src.items() if n not in gone}
            stmts, undo, refuse, ahead = [], [], [], []
            for name, v in sorted(src.items()):
                # the mover's own bookkeeping sequences exist on one side only
                # by design; setting them on the target would fail or create
                # objects the application never asked for
                if noise and name.rpartition(".")[2].startswith(noise):
                    continue
                cur = dst.get(name)
                mx = dmax.get(name, 0)
                sv = int(v or 0)
                # target column already past source last_value = someone
                # wrote to the target; do not paper over it, surface the writer
                # It means nothing, though, when the source column sits just
                # as far past its own sequence: that is an id space the
                # application assigns itself, and refusing there leaves the
                # target sequence unset while the source carries a value - a
                # difference we introduced rather than one we found.
                if name in owned and mx > sv and mx > smax.get(name, 0):
                    refuse.append(f"{name}: target max={mx} > source last={sv}"
                                  f" and > source max={smax.get(name, 0)}"
                                  f" (writes landed on target?)")
                    continue
                # GREATEST(source, target max) can never sit below a live row,
                # so nextval is guaranteed to clear the column with no gap
                target = max(sv, mx)
                # A buffer is head-room for rows that land after this was read:
                # CDC still draining, or a write between the count and the set.
                # It costs a gap in the id space and nothing else, but it does
                # hide the fact that the leg was not actually quiet - so it is
                # off unless asked for, and the reason is written into the
                # statement rather than left for someone to work out later.
                buf = _seq_buffer()
                if target and buf:
                    target += buf
                if target == 0 or (cur is not None and int(cur) == target):
                    continue
                if cur is not None and int(cur) > target:
                    # never lowered: a counter past the value it would be
                    # set to has handed out keys up to where it stands, and
                    # set back, the next insert takes one of them again
                    ahead.append(f"{name}: target {cur} > {target}")
                    continue
                stmts.append(f"select setval({_literal(name)}, {target}, true);"
                             f"  -- src={sv} dstmax={mx}"
                             + (f" +buffer {buf}" if buf else "")
                             + f" dst now {cur if cur is not None else 'MISSING'}")
                if cur == "0":
                    undo.append(f"select setval({_literal(name)}, 1, false);")
                elif cur is not None:
                    undo.append(f"select setval({_literal(name)}, {cur}, true);")
            same = sum(1 for n, v in src.items() if dst.get(n) == v)
            note = f"{len(stmts)} sequences set, {same} already equal"
            if gone:
                note += (f"; {len(gone)} the hop does not take left as they"
                         " are")
            if ahead:
                note += (f"; left alone {len(ahead)} (the target's counter is"
                         " past it already, never lowered): "
                         + "; ".join(ahead[:4]))
            if refuse:
                note += (f"; REFUSED {len(refuse)} (target ahead of source): "
                         + "; ".join(refuse[:4]))
            if stmts or refuse or ahead:
                actions.append(RepairAction(db, "sequences", stmts, undo, note))
        if kind in ("rows", "all"):
            rpt = self._report(db)
            tables = set()
            if rpt.exists():
                for f in rpt.glob("data-*.missing"):
                    tables.add(f.name[len("data-"):-len(".missing")])
                for f in rpt.glob("data-*.extra"):
                    tables.add(f.name[len("data-"):-len(".extra")])
                for f in rpt.glob("data-*.changed"):
                    tables.add(f.name[len("data-"):-len(".changed")])
            # a drilldown of a table the hop has since left out is said,
            # never acted on
            taken, said = self._drills_in_scope(db, sorted(tables))
            if said:
                actions.append(said)
            for t in taken:
                if not self._keep_tbl(db, t):
                    continue
                counts, named = [], {}
                for kind_ in ("missing", "extra", "changed"):
                    f = rpt / f"data-{t}.{kind_}"
                    n = sum(1 for _ in f.open()) if f.exists() else 0
                    named[kind_] = range(n)
                    if n:
                        counts.append(f"{kind_}={n}")
                # the rows the hop keeps by the changes it does not carry
                # are left as they are, and said
                left = self._repairable(db, t, named)
                kept = self._kept_said(named, left)
                if kept and not any(left.values()):
                    continue
                actions.append(RepairAction(
                    db, "rows", [f"resync-rows {t}"], [],
                    f"{t}: {', '.join(counts) or 'no pk files'},"
                    " deleted rows saved to undo before recopy"
                    + (f"; {kept}" if kept else ""), table=t))
            # after the resync, never before it: a recopy takes its rows from
            # the source, which is where the broken text lives, so a text
            # repair applied first would be undone by the action next to it
            text = self._mojibake_repair(db)
            if text:
                actions.append(text)
            actions += self._mapped_repairs(db, kind)
        if kind in ("schema", "all"):
            # GRANT is DDL and belongs with the schema repair, so no new
            # choice is added to --kind: a plain `migkit sync --apply` now
            # restores them along with everything else
            g = self._grant_repair(db)
            if g:
                actions.append(g)
            c = self._constraint_repair(db)
            if c:
                actions.append(c)
            act = self._schema_repair_action(db)
            if act:
                actions.append(act)
        return actions

    def _mojibake_repair(self, db):
        """The row-by-row repair the deep check tells the operator to do.

        That check ends with "repair row by row, matching only the values
        that re-encode to valid UTF-8" - the hardest part of the job, handed
        back to the engineer along with the warning that the obvious
        one-liner destroys the rows that were never broken. This does it.

        It reads the **target**: migkit writes to the source nowhere, and
        the target is the copy it is answerable for. The price is real and
        is written into the note rather than discovered later - a repaired
        target no longer matches the source it came from.

        Written as the load writes (`apply`, `load_window`): as a replica,
        which no trigger fires for, where the target's user may; as the
        tables are only where they have no trigger; refused otherwise. The
        updates used to run with the target's triggers firing - an audit
        trigger's rows, a stamp rewritten - where every other write of
        migkit's into the target was quiet (audit 2026-10-01).
        """
        cols = self._text_columns("dst", db)
        stmts, undo, touched, refused = [], [], set(), []
        repaired = clean = skipped = left = 0
        for table, columns in sorted(cols.items()):
            key = self._pk_cols_of(db, table, "dst")
            if not key:
                refused.append((table, "no primary key, so no row can be"
                                       " addressed"))
                continue
            want = [c for c in columns if c not in key]
            for c in columns:
                if c in key:
                    refused.append((f"{table}.{c}", "the key itself -"
                                    " rewriting it would move rows other"
                                    " tables point at"))
            if not want:
                continue
            sch, tbl = self._split(table)
            quoted = [_ident(c) for c in key + want]
            where = " or ".join(
                f"octet_length({q}) <> char_length({q})"
                for q in quoted[len(key):])
            out = self._psql(
                "dst", db, self._as_json_rows(
                    f'select {", ".join(quoted)}'
                    f' from {_ident(sch)}.'
                    f'{_ident(tbl)}'
                    f' where {where} limit {self.MOJIBAKE_REPAIR_CAP + 1}'))
            rows = []
            for line in out.splitlines():
                if not line.strip():
                    continue
                got = json.loads(line)
                rows.append([got.get(c) for c in key]
                            + [got.get(c) for c in want])
            s, u, t, rep, cl, sk, lf = self._mojibake_updates(
                db, table, key, want, rows)
            stmts += s
            undo += u
            touched |= t
            repaired += rep
            clean += cl
            skipped += sk
            left += lf
        if not repaired:
            return None
        enabled = self._repair_text_enabled()
        note = self._mojibake_repair_note(repaired, touched, clean, skipped,
                                          left, refused, enabled)
        return RepairAction(db, "text", stmts if enabled else [],
                            undo if enabled else [], note)

    def rehearse(self, db, action):
        """Run a schema fix and then its undo on a scratch copy of the
        target's schema, before either goes near the target itself.

        The undo written beside every fix had only ever run in tests. Here
        it runs on the day, on a database made for it on the target's own
        server and dropped afterwards: the target's schema is restored into
        it, the fix applied, the undo applied, and the schema compared with
        what it was. Returns (usable, sentence): usable is False when the
        fix itself fails, which it would on the target too.
        """
        if action.kind != "schema":
            return True, ""
        from .. import movers
        scratch = re.sub(r"[^a-z0-9_]", "_",
                         f"migkit_rehearsal_{self.hop.name}".lower())[:63]
        dump = self.hop.report_dir(db) / "rehearsal.dump"
        try:
            self._psql("dst", db, f'drop database if exists {_ident(scratch)}')
            self._psql("dst", db, f'create database {_ident(scratch)}')
        except RuntimeError as e:
            return True, ("the undo was not rehearsed: a scratch database"
                          " could not be made on the target ("
                          + str(e).strip().splitlines()[-1][:100] + ")")
        try:
            argv, env = self._client("dst", db, "pg_dump", "--schema-only",
                                     "-Fc", "-f", str(dump))
            run(argv, env=env)
            argv, env = self._client("dst", db, "pg_restore", "--no-owner",
                                     "--no-privileges", str(dump),
                                     physical=scratch)
            movers._restore(argv, env, None, "the rehearsal copy",
                            existing_ok=True)
            before = self._dump_schema_native("dst", db, physical=scratch)
            try:
                self._psql("dst", db, "begin;\n"
                           + "\n".join(action.statements) + "\ncommit;",
                           physical=scratch)
            except RuntimeError as e:
                return False, ("the fix fails on a copy of the target's"
                               " schema, so it was not applied: "
                               + str(e).strip().splitlines()[-1][:160])
            if not action.undo:
                return True, ("the fix applies on a copy of the target's"
                              " schema; it has no undo")
            try:
                self._psql("dst", db, "begin;\n"
                           + "\n".join(action.undo) + "\ncommit;",
                           physical=scratch)
            except RuntimeError as e:
                return True, ("the undo FAILS on a copy of the target's"
                              " schema - keep a backup before applying: "
                              + str(e).strip().splitlines()[-1][:160])
            after = self._dump_schema_native("dst", db, physical=scratch)
            import difflib
            changed = [l for l in difflib.unified_diff(
                before.splitlines(), after.splitlines(), lineterm="")
                if l[:1] in "+-" and not l.startswith(("+++", "---"))]
            if changed:
                (self.hop.report_dir(db) / "rehearsal.diff").write_text(
                    "\n".join(changed) + "\n")
                return True, (f"the undo does NOT return the schema exactly"
                              f" ({len(changed)} lines differ, in"
                              " rehearsal.diff) - keep a backup before"
                              " applying")
            return True, ("rehearsed on a copy of the target's schema: the"
                          " fix applies, and the undo returns the schema"
                          " exactly")
        finally:
            dump.unlink(missing_ok=True)
            # a connection kept to the scratch database would keep it from
            # being dropped
            self.release_kept("dst", scratch)
            try:
                self._psql("dst", db,
                           f"drop database if exists {_ident(scratch)}")
            except RuntimeError:
                pass

    def _schema_repair_action(self, db):
        """DDL that makes the target's objects (columns, indexes, PK/FK,
        views, functions, procedures, triggers, sequences-as-objects) match
        the source, generated by atlas - the authoritative schema differ -
        with the reverse DDL captured as undo. Data is untouched; this only
        aligns structure."""
        if not hasattr(self, "migration_pair"):
            return None
        fwd, undo = self.migration_pair(db)
        if not fwd:
            return None
        return RepairAction(db, "schema", fwd.splitlines(),
                            (undo or "").splitlines(),
                            "DDL that aligns the target's objects with the"
                            " source (review before --apply; runs in one"
                            " transaction, reverse DDL saved to undo)")

    def apply(self, db, action):
        if action.by == "pair":
            return self.columns_pair().apply(db, action)
        if action.kind == "sequences":
            # a plan of counters left alone and nothing to set sends nothing
            if action.statements:
                self._psql("dst", db, "\n".join(s.split("  --")[0]
                                                for s in action.statements))
        elif action.kind == "resnapshot":
            self._apply_resnapshot(action)
        elif action.kind == "text":
            # withheld unless MIGKIT_REPAIR_TEXT says otherwise, in which
            # case the note is the whole action - applying it must do
            # nothing rather than send `begin; commit;` and report success
            if action.statements:
                self._write_as_the_load(db, action.statements)
        elif action.kind in ("schema", "grants", "constraints"):
            # one psql call: multi-statement + $$-quoted bodies apply intact,
            # and a grant set lands all-or-nothing
            blob = "begin;\n" + "\n".join(action.statements) + "\ncommit;"
            self._psql("dst", db, blob)
        elif action.kind == "rows":
            for stmt in action.statements:
                self._repair_rows_native(db, stmt.split(" ", 1)[1])
        else:
            # This used to be the row branch's `else`, so a kind nobody had
            # taught it about was read as a table name - the grant repair's
            # first run tried to look up a primary key for a relation called
            # `INSERT ON "public"."t" TO "app";`. An unknown repair must
            # refuse, not guess.
            raise RuntimeError(f"no way to apply a {action.kind!r} repair")

    def _write_as_the_load(self, db, statements):
        """`statements` - each an update of a table of the target - in one
        transaction on the target, written as the load writes rows
        (`load_window`, `WRITE_WAYS`): as a replica where the target's user
        may, as the tables are only where they have no trigger, and
        refused otherwise, before anything is written."""
        tables = set()
        for s in statements:
            got = re.match(r"\s*update\s+(\S+)\s+set\b", s, re.I)
            if got:
                tables.add(".".join(self._split(got.group(1))))
        with self.load_window(db, None, tables or None):
            conn = self._connect("dst", db, "write")
            try:
                with conn.transaction(), conn.cursor() as cur:
                    for s in statements:
                        cur.execute(s)
            finally:
                conn.close()

    def _psql_run(self, side, db, script):
        """Run a multi-statement psql script from stdin so client-side
        \\copy works (needed for the temp-pk join repair). On the target
        inside the load window it writes as the window's connections do
        (`replica_env`)."""
        quiet = self.replica_env(db).get("PGOPTIONS", "") \
            if side == "dst" else ""
        argv, env = self._client(side, db, "psql", "-X", "-q", "-v",
                                 "ON_ERROR_STOP=1", options=quiet)
        p = subprocess.run(argv, input=script, capture_output=True,
                           text=True, env=tool_env(env))
        if _changes_shape(script):
            self._forget_catalog(side, db)
        if p.returncode:
            from ..wording import pooler_refused
            pooled = pooler_refused(p.stderr)
            if pooled:
                where = "source" if side == "src" else "target"
                raise SystemExit(f"the {where}: {pooled}. The connection was"
                                 " refused before it ran anything.")
            raise RuntimeError((p.stdout + p.stderr)[-400:])
        return p.stdout

    def _repair_rows_native(self, db, t):
        """Set-based row repair: pull the rows to (re)copy from source into a
        temp file via a temp-pk join, save the target rows they overwrite as
        undo, delete extra/changed on target, then copy the source rows in.

        Written as the load writes (`load_window`, `WRITE_WAYS`): as a
        replica, which no trigger or foreign key fires for, where the
        target's user may; as the tables are only where this one has no
        trigger and no foreign key; and not at all otherwise. The repair
        used to switch to writing as they are wherever the replica role was
        refused, triggers or not, where the load stopped - a repair whose
        rows the target's triggers rewrote as they landed."""
        d = self._report(db)
        sch, tbl = self._split(t)
        qt = f'{_ident(sch)}.{_ident(tbl)}'
        pks = self._pk_cols_of(db, t)
        if not pks:
            return
        cols_decl = ", ".join(f"c{i + 1} text" for i in range(len(pks)))
        cond = " and ".join(f't.{_ident(pks[i])}::text = p.c{i + 1}'
                            for i in range(len(pks)))
        only = self.__dict__.get("_repair_only")
        canon_of = self._key_canon(db, t, pks) if only is not None else None

        kept = self._kept_by_hop(db, t)

        def read(kind):
            if kind in kept:
                # a row the hop keeps by a change it does not carry: never
                # removed, added or overwritten here
                return ""
            f = d / f"data-{t}.{kind}"
            text = f.read_text() if f.exists() else ""
            if only is None:
                return text
            # the keys a repair by spans leaves to be repaired by key
            # (`repairways`), the rest of the file left as it is
            lines = [line for line in text.splitlines() if line]
            return "".join(line + "\n" for line, k in
                           zip(lines, canon_of(lines)) if tuple(k) in only)

        # on_conflict=keep-target preserves rows the target changed itself:
        # only fix missing (insert) and extra (delete), never overwrite a
        # differing row. source-wins (default) reconciles everything.
        keep = self.hop.options.get("on_conflict") == "keep-target"
        chg = "" if keep else read("changed")
        col = self.newer_wins_column()
        if col and chg.strip():
            chg = self._newer_split(db, t, sch, tbl, pks, col, chg, d)
        copy_pks = read("missing") + chg
        del_pks = read("extra") + chg
        undo = d / "undo"
        undo.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d-%H%M%S")
        rows_file = undo / f"{ts}-{_file_name(t)}.rows"
        # every statement written down before any lands (`repairlog`), and
        # the table's repair one transaction: a stop leaves all of it or
        # none, where the deletes used to land without the copy after them
        record = self._repair_record(db, t, pks, copy_pks, chg,
                                     read("extra"), undo)
        # the rows travel through the drivers' COPY, never a script: names
        # were written into a script of the client program's, where a
        # newline in one began a line of its own, and the undo rows were
        # written by that program to a file of its own, plain, whatever the
        # hop's `at_rest` said (audit 2026-10-01, SEC4, SEC9)
        rows = tempfile.SpooledTemporaryFile(max_size=64 * 2 ** 20)
        held = tempfile.SpooledTemporaryFile(max_size=64 * 2 ** 20)
        try:
            keys = [[self._pk_unescape(v) for v in line.split("\t")]
                    for line in copy_pks.splitlines() if line]
            if keys:
                self._source_rows(db, qt, pks, keys, rows)
            with self.load_window(db, None, {t}):
                conn = self._connect("dst", db, "write")
                try:
                    with conn.transaction(), conn.cursor() as cur:
                        cur.execute(f"create temp table _pk ({cols_decl})"
                                    " on commit drop")
                        with cur.copy("copy _pk from stdin") as cp:
                            cp.write(del_pks)
                        with cur.copy(f"copy (select t.* from {qt} t join"
                                      f" _pk p on {cond}) to stdout") as cp:
                            for chunk in cp:
                                held.write(chunk)
                        # what the rows it replaces were, sealed where the
                        # hop keeps values encrypted and on the disk before
                        # anything is deleted
                        held.seek(0)
                        _kept_whole(rows_file, held.read())
                        cur.execute(f"delete from {qt} t using _pk p"
                                    f" where {cond}")
                        if keys:
                            rows.seek(0)
                            with cur.copy(f"copy {qt} from stdin") as cp:
                                while True:
                                    chunk = rows.read(1 << 16)
                                    if not chunk:
                                        break
                                    cp.write(chunk)
                finally:
                    conn.close()
            record.applied(record.n)
            with (undo / "manifest.txt").open("a") as m:
                m.write(f"{ts} {t}: re-copy {rows_file} to undo\n")
        finally:
            record.close()
            rows.close()
            held.close()

    #: keys asked of the source in one statement of a repair
    REPAIR_KEYS = 5000

    def _source_rows(self, db, qt, pks, keys, out):
        """The source's rows of `keys` (their key columns' text, in key
        order) as COPY text into `out`, read in one read-only snapshot and
        asked by the keys themselves - no temporary table, which a
        read-only source (a replica) refuses."""
        import psycopg
        conn = self._connect("src", db, "write")
        try:
            conn.read_only = True
            conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            with conn.transaction(), conn.cursor() as cur:
                tup = ", ".join(f"t.{_ident(k)}::text" for k in pks)
                for i in range(0, len(keys), self.REPAIR_KEYS):
                    values = ", ".join(
                        "(" + ", ".join(_literal(v) for v in k) + ")"
                        for k in keys[i:i + self.REPAIR_KEYS])
                    with cur.copy(f"copy (select t.* from {qt} t where"
                                  f" ({tup}) in (values {values}))"
                                  " to stdout") as cp:
                        for chunk in cp:
                            out.write(chunk)
        finally:
            conn.close()

    def _repair_record(self, db, t, pks, copy_lines, changed_lines,
                       extra_lines, undo):
        """The statement-level record of one table's repair (`repairlog`),
        written out whole: each key the repair copies is a write - or a
        delete, where the repair removes a changed row the source no longer
        has - and each extra key a delete, with the row the target held
        and the one it will hold, by the key files' lines."""
        from .. import repairlog
        sc, canon_of = self._key_canon(db, t, pks, with_columns=True)

        def keys(lines):
            return canon_of([line for line in lines.splitlines() if line])
        writes = keys(copy_lines)
        return repairlog.planned(
            undo, repairlog.CanonIO(self, "dst", db, t, sc, pks),
            repairlog.CanonIO(self, "src", db, t, sc, pks).rows(writes),
            writes, keys(changed_lines), keys(extra_lines))

    def _key_canon(self, db, t, pks, with_columns=False):
        """A reader of the drilldown's key lines (`_pk_text_expr`'s form,
        tab-joined) as canonical key texts - with the columns both sides
        render, where asked."""
        from .. import repairlog
        sc, _, _ = self._comparable_columns(db, self, t, self, t)
        missing = [k for k in pks if k not in dict(sc)]
        if missing:
            raise SystemExit(
                f"{t}: the key column {', '.join(missing)} has no text both"
                " sides render, so the repair could not be recorded row by"
                " row. Nothing was repaired on this table.")

        def canon_of(lines):
            return repairlog.canon_keys(sc, pks, [
                [self._pk_unescape(p) for p in line.split("\t")]
                for line in lines])
        return (sc, canon_of) if with_columns else canon_of

    def repair_found(self, db, table):
        """The keys the last check named for `table`, from its key files,
        as canonical text; None where its key is not one both sides
        render."""
        pks = self._pk_cols_of(db, table)
        if not pks:
            return None
        try:
            canon_of = self._key_canon(db, table, pks)
        except SystemExit:
            return None
        return self._repairable(db, table, {
            kind: [list(k) for k in canon_of(
                self._drill_keys(db, table, kind))]
            for kind in ("missing", "changed", "extra")})

    #: a span of a table replaced under one transaction (`repair_range`)
    ONE_TRANSACTION_A_RANGE = True

    def repair_range_ready(self, db, sides):
        """None: the table copier's own range copy takes any span of an
        integer key (`_copy_checked`)."""
        return None

    def repair_range(self, db, sides, lo, hi, log):
        """[lo, hi) of the table's integer key replaced by the source's the
        way the table copier copies a range: the span deleted and copied in
        one transaction, then read back and held to what passed
        (`_copy_checked`, the pipe). Returns the rows copied."""
        sch, tbl = self._split(sides.dst_t)
        qt = f'{_ident(sch)}.{_ident(tbl)}'
        (k,) = sides.dkey
        q = self._quote_ident(k)
        pred = f"{q} >= {int(lo)} and {q} < {int(hi)}"
        got = self._copy_checked(
            db, qt, self._copy_cols(db, sch, tbl), pred,
            f"delete from {qt} where {pred}",
            f"{sch}.{tbl} {k} {int(lo):,} to {int(hi) - 1:,}", log)
        return int(getattr(got, "n", got) or 0)

    def _newer_split(self, db, t, sch, tbl, pks, col, lines, d):
        """The changed keys to overwrite under `newer_wins`, as the key
        file's own lines; the ones whose target row is newer are kept and
        written to `data-<table>.kept-newer`."""
        for side in ("src", "dst"):
            if not self._psql(side, db,
                              "select 1 from information_schema.columns"
                              f" where table_schema = {_literal(sch)}"
                              f" and table_name = {_literal(tbl)}"
                              f" and column_name = {_literal(col)}").strip():
                raise SystemExit(
                    f"{t}: newer_wins names {col}, and it is not on both"
                    " sides, so which row is newer cannot be told. Nothing"
                    " was repaired on this table.")
        keys = [line for line in lines.splitlines() if line]
        cond = " and ".join(f't.{_ident(pks[i])}::text = p.c{i + 1}'
                            for i in range(len(pks)))

        def at(side, wanted):
            # the keys as a VALUES list: a read-only session may not create
            # even a temporary table
            conn = self._connect(side, db)
            names = ", ".join(f"c{i + 1}" for i in range(len(pks)))
            got = ", ".join(f"p.c{i + 1}" for i in range(len(pks)))
            out = {}
            try:
                conn.set_session(readonly=True)
                with conn.cursor() as cur:
                    for i in range(0, len(wanted), 1000):
                        rows = [line.split("\t") for line in wanted[i:i + 1000]]
                        vals = ", ".join(
                            "(" + ", ".join(["%s"] * len(pks)) + ")"
                            for _ in rows)
                        cur.execute(
                            f'select {got}, t.{_ident(col)} from {_ident(sch)}.{_ident(tbl)} t'
                            f" join (values {vals}) as p({names}) on {cond}",
                            [v for r in rows for v in r])
                        out.update({"\t".join(r[:-1]): r[-1]
                                    for r in cur.fetchall()})
                return out
            finally:
                conn.rollback()
                conn.close()
        overwrite, kept = self._keep_the_newer(t, keys, at)
        if kept:
            (d / f"data-{t}.kept-newer").write_text(
                "".join(k + "\n" for k in kept))
        return "".join(k + "\n" for k in overwrite)

    def setup_target_plan(self, db):
        """The steps an operator runs by hand, in the order the measurements
        say they should run.

        The ordering is the whole point. A schema restored in one piece puts
        every secondary index on the table *before* the data arrives, so the
        load maintains them row by row. Measured on 1,000,000 rows with
        three secondary indexes:

            indexes already present, then load      4.48 s    275 MB
            load, then build the same indexes       2.53 s    218 MB
                                                  (0.91 + 1.62)

        Not quite twice the time, and - the part that does not go away - the
        table loaded with its indexes in place is **26% larger on disk**,
        because an index maintained one row at a time does not pack the way
        one built in a single pass does. That bloat is permanent.

        `--section=pre-data` and `--section=post-data` split the dump at
        exactly that line: measured, pre-data left the table with **0**
        indexes and post-data brought them back. post-data also carries the
        foreign keys and triggers, which is why this replaces the old advice
        to disable them by hand - they are simply not there during the load.
        """
        s, t = self.hop.source, self.hop.target
        tdb = self._d("dst", db)
        return [
            f"pg_dumpall -h {s.host} -p {s.port} -U {s.user} --globals-only"
            f" > globals.sql   # review roles, then apply on target",
            f"pg_dump -h {s.host} -p {s.port} -U {s.user} -d {db} -Fc"
            f" --schema-only -f {db}.schema.dump",
            f"createdb -h {t.host} -p {t.port} -U {t.user} {tdb}"
            f"   # match encoding/locale with source",
            f"pg_restore -h {t.host} -p {t.port} -U {t.user} -d {tdb}"
            f" --no-owner --section=pre-data {db}.schema.dump"
            f"   # tables only: no indexes, no FKs, no triggers yet",
            f"-- now load the data: migkit move {self.hop.name} --mode full"
            f" --go",
            f"pg_restore -h {t.host} -p {t.port} -U {t.user} -d {tdb}"
            f" --no-owner --section=post-data -j {max(1, int(self.hop.workers))}"
            f" {db}.schema.dump"
            f"   # indexes, FKs and triggers, built once over the loaded data",
            f"-- measured on 1M rows x 3 indexes: building them after the"
            f" load took 2.53 s against 4.48 s, and left the table 218 MB"
            f" instead of 275 MB",
            f"-- keep the pre-data dump: it is your rollback script",
        ]

    def snapshot_state(self, db, state_dir, kind="all"):
        seqs = self._psql("dst", db,
                          "select schemaname||'.'||sequencename||'|'||"
                          "coalesce(last_value,1)||'|'||(last_value is not null)"
                          " from pg_sequences"
                          " where schemaname not like '\\_\\_%'")
        # what a rollback puts back: whole or not at all, and on the disk
        # before the repair it undoes begins (`durable`)
        from ..durable import write_atomic
        write_atomic(state_dir / "dst-sequences.txt",
                     (seqs + "\n") if seqs else "")
        # a sequence-only repair rolls back from the sequence snapshot alone;
        # skip the schema dump, which is slow (and can stall) on big partitioned
        # databases and buys nothing here.
        if kind == "sequences":
            return
        argv, env = self._client("dst", db, "pg_dump", "--schema-only",
                                 "--no-owner", "--no-privileges")
        write_atomic(state_dir / "dst-schema.sql", run(argv, env=env).stdout)

    #: how `assess` reads the login roles of both sides
    ROLE_READS = (
        Rung("hashes", "the roles with their password hashes"),
        Rung("names", "the roles by name alone"))

    def _login_roles(self):
        """({role: password hash} of the source, of the target, [the
        switch said]). pg_authid has the real hash (pg_roles masks it) and
        needs a superuser; where it is refused on either side (a managed
        service) the roles are read by name alone, switched to by itself -
        and the passwords not compared then is said, not left out
        quietly."""
        # the roles a server or its provider keeps for itself, by the one
        # list `users` and the grant check read too (audit 2026-10-01, DU13)
        from ..users import system_principals_sql
        system = system_principals_sql("postgres", "rolname")
        roles_q = ("select rolname||'|'||coalesce(rolpassword,'')"
                   f" from pg_authid where rolcanlogin and not {system}"
                   " order by 1")
        names_q = ("select rolname from pg_roles where rolcanlogin"
                   f" and not {system} order by 1")

        def roles(choice):
            if choice.path == "names":
                return tuple({n: "" for n in self._psql(
                    side, "postgres", names_q).splitlines()}
                    for side in ("src", "dst"))
            return tuple(dict(l.split("|", 1) for l in self._psql(
                side, "postgres", roles_q).splitlines() if l)
                for side in ("src", "dst"))
        switched = []
        src, dst = self._attempt("the login roles", self.ROLE_READS, roles,
                                 log=switched.append)[1]
        return src, dst, switched

    def _assess_roles(self):
        """The application's login roles on the target, as `assess` says
        them: each there, and with the source's password."""
        items = []

        def add(level, scope, item, detail=""):
            items.append({"level": level, "scope": scope,
                          "item": item, "detail": str(detail)})
        src_pairs, dst_pairs, switched = self._login_roles()
        if switched:
            add("warn", "instance",
                "role passwords match source (login will work on target)",
                "not compared: " + "; ".join(switched))
        sa, da = set(src_pairs), set(dst_pairs)
        miss = sorted(sa - da)
        add("pass" if not miss else "warn", "instance",
            "login roles present on target",
            f"{len(sa)} src / {len(da)} dst"
            + (f", missing: {', '.join(miss[:5])}" if miss else ""))
        # role on both sides but hash differs = DTS carried the user, not the
        # password -> apps can't authenticate on target
        drift = sorted(n for n in (sa & da)
                       if src_pairs[n] and dst_pairs[n]
                       and src_pairs[n] != dst_pairs[n])
        if any(src_pairs.values()):
            add("pass" if not drift else "fail", "instance",
                "role passwords match source (login will work on target)",
                "all match" if not drift
                else f"password differs for: {', '.join(drift[:5])}"
                     " -> reset on target or apps cannot log in")
        return items

    def assess(self):
        items = []

        def add(level, scope, item, detail=""):
            items.append({"level": level, "scope": scope,
                          "item": item, "detail": str(detail)})

        items += self._brand_rows()
        src_b, dst_b = self._brands()
        sv = src_b.version or self._psql("src", "postgres",
                                         "show server_version")
        dv = dst_b.version or self._psql("dst", "postgres",
                                         "show server_version")
        items.append(self._version_row(sv, dv))
        items += self._pooler_items()
        items += self._leg_items()

        # read replica (pg_is_in_recovery=t) rejects writes incl SELECT FOR
        # UPDATE (25006): fatal as a target, ok-for-checks as a source
        src_ro = self._in_recovery("src", "postgres")
        dst_ro = self._in_recovery("dst", "postgres")
        add("warn" if src_ro else "pass", "instance",
            "source endpoint role",
            "READ REPLICA (read-only) - it writes nothing, so it is the"
            " only way to verify a frozen source, but a replica that has not"
            " replayed shows fewer rows than the target and reads as"
            " rows-extra. Measured: a paused standby produced"
            " `kind=rows-extra by=5` against a primary five rows ahead. The"
            " data check names the replica when it reports one; point at the"
            " writer/cluster endpoint for replication and the LSN fence"
            if src_ro else "primary / writer (writable)")
        add("fail" if dst_ro else "pass", "instance",
            "target endpoint is writable (not a read replica)",
            "READ REPLICA (read-only) - cannot migrate or repair into it,"
            " and apps get SQLSTATE 25006 on SELECT FOR UPDATE; use the"
            " writer/cluster endpoint" if dst_ro else "primary / writer")

        for db in self.databases():
            add(*self._large_values(db))

        # standbys the target streams to while it is loaded: every row is
        # shipped to each, and a synchronous one holds each commit until
        # it has it - the managed services advise them off until cutover
        try:
            n, sync = self._psql(
                "dst", "postgres", "select count(*) || '|' ||"
                " current_setting('synchronous_standby_names')"
                " from pg_stat_replication where state = 'streaming'"
                ).split("|")
            n = int(n)
            add("warn" if n else "pass", "instance",
                "no standbys stream from the target during the load",
                f"{n} standbys stream from the target"
                + (f" (synchronous: {sync} - every commit waits)" if sync
                   else "")
                + " - every row loaded is shipped to each; take them off"
                " until cutover, or expect a slower load" if n else "none")
        except Exception:  # noqa: BLE001 - not allowed to see them
            pass

        wal = self._psql("src", "postgres", "show wal_level")
        add("pass" if wal == "logical" else "fail", "instance",
            "wal_level=logical on source (required for CDC)", wal)
        slots = self._psql("src", "postgres",
                           "select count(*)||' used / '||"
                           "current_setting('max_replication_slots')||' max'"
                           " from pg_replication_slots")
        add("pass", "instance", "replication slots", slots)

        # a lagging or abandoned slot pins WAL and can silently fill the source
        # disk (source outage mid-migration). wal_status is pg13+, so probe it
        # and fall back cleanly on older majors.
        try:
            rows = [l for l in self._psql("src", "postgres",
                    "select slot_name||'|'||active||'|'"
                    "||coalesce(wal_status,'?')||'|'||coalesce("
                    "pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(),"
                    " restart_lsn)),'?') from pg_replication_slots"
                    ).splitlines() if l]
            for r in rows:
                name, active, status, retained = r.split("|")
                if status == "lost":
                    add("fail", "instance", f"replication slot {_literal(name)}",
                        f"WAL LOST - replication broken, retained {retained}")
                elif active in ("f", "false"):
                    add("warn", "instance", f"replication slot {_literal(name)}",
                        "INACTIVE (no consumer) - pins WAL, retained"
                        f" {retained}; drop it if abandoned")
                elif status in ("unreserved", "extended"):
                    add("warn", "instance", f"replication slot {_literal(name)}",
                        f"wal_status {status}, retained {retained} -"
                        " approaching the retention limit")
            if rows:
                mk = self._psql("src", "postgres",
                                "show max_slot_wal_keep_size")
                if mk.strip() in ("-1", "-1B"):
                    add("warn", "instance", "max_slot_wal_keep_size",
                        "-1 (unbounded): an inactive slot can fill the disk;"
                        " set a limit so a stalled consumer can't take the"
                        " source down")
        except RuntimeError:
            pass
        lrt = self._psql("src", "postgres",
                         "select count(*) from pg_stat_activity where"
                         " xact_start is not null"
                         " and now() - xact_start > interval '10 minutes'")
        add("pass" if lrt == "0" else "warn", "instance",
            "transactions open longer than 10 minutes", lrt)
        held = self.oldest_snapshot("src")
        if held:
            age, secs, who = held
            add("warn" if secs > 600 else "pass", "instance",
                "the oldest snapshot on the source",
                f"{age:,} transactions old, held {secs:,.0f}s by"
                f" {who or 'an unnamed session'} - vacuum can remove nothing"
                " newer than it while it is open")

        from ..users import NOT_CARRIED, carried
        if not carried(self.hop):
            add("info", "instance", "login roles present on target",
                NOT_CARRIED)
        else:
            items.extend(self._assess_roles())

        try:
            avail = set(self._psql("dst", "postgres",
                                   "select name from pg_available_extensions")
                        .splitlines()) - {""}
        except RuntimeError:
            # an empty set here must not read as "nothing is missing"; the
            # inventory turns it into unknown rather than a clean answer
            avail = set()
        for db in self.databases():
            try:
                exts = set(self._psql("src", db,
                                      "select extname from pg_extension")
                           .splitlines())
                add("pass", db, "extensions on source", f"{len(exts)}")
                invalid = self._psql("src", db,
                                     "select count(*) from pg_index"
                                     " where not indisvalid")
                add("pass" if invalid == "0" else "warn", db,
                    "invalid indexes on source", invalid)
                enc_q = ("select pg_encoding_to_char(encoding)||' '||datcollate"
                         f" from pg_database where datname = {_literal(db)}")
                se = self._psql("src", "postgres", enc_q)
                try:
                    de = self._psql("dst", "postgres", enc_q)
                except RuntimeError:
                    de = ""
                if not de:
                    add("warn", db, "database exists on target", "missing")
                else:
                    add("pass" if se == de else "fail", db,
                        "encoding and collation match", f"src {se} / dst {de}")
            except RuntimeError as e:
                add("fail", db, "assess queries", str(e).splitlines()[-1][:120])
        hw = self._handwork(avail)
        items += hw.rows() + hw.summary()
        items += self._mover_leftovers()
        items += self._bulk_path_rows()
        items += self._client_tool_versions(
            ("pg_dump", "pg_restore", "psql"), dv)
        # the deep checks that are about the move ahead rather than the one
        # behind. They live in one implementation and are read from two
        # places; this is the earlier one, which is the one that can still
        # change what an operator does.
        items += self._preflight_items()
        items += self._stream_capacity()
        items += self._decoding_spills()
        items += self._unmatchable_rows()
        items += self._scope_items()
        return items

    #: key-less tables holding a column whose type has no `=`: a replica
    #: that must find an updated or deleted row by comparing every column
    #: cannot compare these (arrays count by their element type)
    UNMATCHABLE_SQL = (
        "select n.nspname||'.'||c.relname||'|'||string_agg(a.attname||' '"
        "||format_type(a.atttypid, a.atttypmod), ', ' order by a.attnum)"
        " from pg_class c join pg_namespace n on n.oid = c.relnamespace"
        " join pg_attribute a on a.attrelid = c.oid and a.attnum > 0"
        " and not a.attisdropped"
        " join pg_type ty on ty.oid = a.atttypid"
        " where c.relkind in ('r', 'p')"
        " and n.nspname not in ('pg_catalog', 'information_schema')"
        " and not exists (select 1 from pg_index i where i.indrelid = c.oid"
        " and (i.indisprimary or (i.indisunique and i.indimmediate)))"
        " and not exists (select 1 from pg_operator o where o.oprname = '='"
        " and o.oprleft = case when ty.typelem <> 0 and ty.typlen = -1"
        " then ty.typelem else ty.oid end"
        " and o.oprright = o.oprleft)"
        " group by n.nspname, c.relname order by 1")

    def _unmatchable_rows(self):
        """Tables whose updates and deletes a replica cannot apply.

        Without a key, logical replication finds the row on the target by
        comparing every column, and a type with no equality operator -
        `json`, `point`, `xml`, arrays of them - cannot be compared. DTS
        fails such tables outright; the fix is a key, or a unique index the
        table can use as its replica identity.
        """
        out = []
        for db in self.databases():
            try:
                got = self._psql("src", db, self.UNMATCHABLE_SQL)
            except RuntimeError as e:
                # said, not skipped: a question that could not be asked is
                # not a clean answer - skipping it hid a broken query here
                out.append({"level": "warn", "scope": db,
                            "item": "key-less tables a replica cannot match",
                            "detail": "could not be read: "
                                      + str(e).strip().splitlines()[-1][:100]})
                continue
            for line in got.splitlines():
                if not line:
                    continue
                table, cols = line.split("|", 1)
                out.append({
                    "level": "fail", "scope": f"{db}.{table}",
                    "item": "a key-less table a replica cannot match rows in",
                    "detail": f"{cols} have no equality - updates and"
                              " deletes will stop the change stream. Add a"
                              " primary key, or a unique index and"
                              f" `alter table {table} replica identity using"
                              " index <it>`"})
        return out

    def _stream_capacity(self):
        """Room on the source for the change streams this hop would open,
        with the value to set rather than only the verdict: one slot and
        one sender per database in scope, on top of what is already used."""
        out = []
        dbs = len(self.databases())
        got = self._psql("src", "postgres",
                         "select (select count(*) from pg_replication_slots)"
                         "||'|'||current_setting('max_replication_slots')"
                         "||'|'||(select count(*) from pg_stat_replication)"
                         "||'|'||current_setting('max_wal_senders')").strip()
        used, slots, senders, max_senders = (int(x) for x in got.split("|"))
        for what, have, busy in (("max_replication_slots", slots, used),
                                 ("max_wal_senders", max_senders, senders)):
            need = busy + dbs
            out.append({
                "level": "pass" if have >= need else "fail",
                "scope": "instance",
                "item": f"{what} leaves room for this hop's streams",
                "detail": f"{have} set, {busy} in use, {dbs} needed" + (
                    "" if have >= need else
                    f" - set {what} = {need} (a restart; on a managed"
                    " server, in its parameter group)")})
        return out

    def _decoding_spills(self):
        """Whether the source's change streams have been decoding
        transactions too large for `logical_decoding_work_mem`, with the
        value that ends it.

        What does not fit is written to disk on the source and read back,
        which slows the stream and costs the source's disk. Measured on 16,
        a 3.3 MB transaction decoded with the setting at 64kB: `spill_txns
        1, spill_bytes 3,460,000`. At 4MB the same transaction spilled
        nothing, and at 2MB it did. So the value named is the next power
        of two at least the average spilled transaction, and never less
        than twice the current setting. The count is the server's own,
        since its statistics were last reset (PostgreSQL 14 and later)."""
        item = "change streams decode within logical_decoding_work_mem"
        try:
            got = self._psql("src", "postgres",
                             "select coalesce(sum(spill_txns), 0)||'|'||"
                             "coalesce(sum(spill_bytes), 0)||'|'||"
                             "coalesce(string_agg(slot_name, ',') filter"
                             " (where spill_txns > 0), '')||'|'||"
                             "pg_size_bytes(current_setting("
                             "'logical_decoding_work_mem'))"
                             " from pg_stat_replication_slots").strip()
            txns, spilled, slots, setting = got.split("|")
            txns, spilled, setting = int(txns), int(spilled), int(setting)
        except (RuntimeError, ValueError):
            return [{"level": "warn", "scope": "instance", "item": item,
                     "detail": "not asked: the server keeps no count of it"
                               " before PostgreSQL 14"}]
        if not txns:
            return [{"level": "pass", "scope": "instance", "item": item,
                     "detail": "no transaction decoded by a stream here has"
                               " spilled to disk"}]
        want = max(spilled // txns, 2 * setting)
        mb = 1
        while mb * 2 ** 20 < want:
            mb *= 2
        return [{"level": "warn", "scope": "instance", "item": item,
                 "detail": f"{txns:,} transactions decoded for {slots}"
                           f" spilled {spilled / 2 ** 20:.1f} MB to the"
                           " source's disk - set logical_decoding_work_mem"
                           f" = {mb}MB (a reload; on a managed server, in"
                           " its parameter group)"}]

    def _mover_leftovers(self):
        """What a mover added to the source and did not take away.

        migkit's own playbook has said for a long time to remove these after
        cutover. Saying it in prose is a note to remember something; this
        counts them. A replication slot in particular is not untidiness - it
        pins WAL, and the source can run out of disk long after everybody has
        moved on.
        """
        from .. import leftovers as _lo
        found, items = [], []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "source leftovers",
                          "item": item, "detail": str(detail)})
        try:
            for name in self._psql("src", "postgres",
                                   "select slot_name from pg_replication_slots"
                                   ).splitlines():
                if name:
                    found.append(("slot", name))
        except RuntimeError as e:
            add("warn", "cannot list replication slots on the source",
                f"{str(e).splitlines()[-1][:90]} - unknown, not clean")
        # the tables and columns a loader or an online schema change adds all
        # begin with an underscore (`_dlt_loads`, `_orders_ghk`,
        # `_airbyte_raw_id`), so only those are listed, not every column
        user = "table_schema not in ('pg_catalog','information_schema')"
        for db in self.databases():
            try:
                for q, kind in (
                        ("select nspname from pg_namespace", "schema"),
                        ("select pubname from pg_publication", "publication"),
                        ("select evtname from pg_event_trigger",
                         "event trigger"),
                        ("select extname from pg_extension", "extension"),
                        ("select table_schema||'.'||table_name from"
                         " information_schema.tables where table_name like"
                         f" '\\_%' and {user}", "table"),
                        ("select table_schema||'.'||table_name||'.'||"
                         "column_name from information_schema.columns where"
                         f" column_name like '\\_%' and {user}", "column")):
                    for name in self._psql("src", db, q).splitlines():
                        if name:
                            found.append((kind, name))
            except RuntimeError as e:
                add("warn", f"cannot list objects in {db}",
                    f"{str(e).splitlines()[-1][:90]} - unknown, not clean")
        by = _lo.group(found)
        if not by:
            add("pass", "no mover artifacts left in the source",
                f"{len(found)} objects examined")
            return items
        for label, why in _lo.urgent(by):
            add("fail", f"{label} is still active on the source", why)
        add("warn", f"{_lo.total(by)} mover artifacts left in the source",
            _lo.describe(by) + ". Drop them once every leg that used them is"
            " finished - they are on the source, so nothing on the target"
            " tells you they are there")
        return items

    def _handwork(self, avail):
        """Inventory the work the move will not do, per `migkit.handwork`.

        Functions, views and constraints are absent on purpose: the structural
        check diffs them and emits DDL for them, so they are carried. What is
        listed here is what no amount of DDL fixes - contents that live outside
        any table, objects the WAL stream never mentions, and things that have
        to exist on the target before anything is loaded into it.
        """
        from .. import handwork
        inv = handwork.Inventory()
        for db in self.databases():
            USER_SCHEMA = ("n.nspname not in"
                           " ('pg_catalog','information_schema')"
                           " and n.nspname not like 'pg\\_%'"
                           " and n.nspname not like '\\_\\_%'")
            try:
                exts = set(self._psql("src", db,
                                      "select extname from pg_extension")
                           .splitlines()) - {""}
                inv.add("target-prereq", db,
                        "extensions not installable on the target",
                        sorted(exts - avail) if avail else [])
                if not avail:
                    inv.unknown("target-prereq", db,
                                "could not read pg_available_extensions on"
                                " the target")

                # No primary key means logical replication has no REPLICA
                # IDENTITY to match an UPDATE or DELETE against, so those
                # changes are dropped - and migkit cannot name a differing
                # row either, only the whole table.
                nopk = [l for l in self._psql("src", db, f"""
                    select n.nspname||'.'||c.relname
                    from pg_class c
                    join pg_namespace n on n.oid = c.relnamespace
                    where c.relkind = 'r' and {USER_SCHEMA}
                      and not exists (select 1 from pg_index i
                        where i.indrelid = c.oid and i.indisprimary)
                      and c.relreplident not in ('f','i')
                    order by 1""").splitlines() if l]
                inv.add("no-row-key", db, "tables", nopk)

                # Unlogged tables produce no WAL at all, so a WAL-based mover
                # cannot see a single row of them.
                unlogged = [l for l in self._psql("src", db, f"""
                    select n.nspname||'.'||c.relname from pg_class c
                    join pg_namespace n on n.oid = c.relnamespace
                    where c.relkind = 'r' and c.relpersistence = 'u'
                      and {USER_SCHEMA} order by 1""").splitlines() if l]
                inv.add("not-carried", db, "unlogged tables", unlogged)

                # A matview's definition is carried by the structural fix; its
                # contents are not, and it is readable-but-stale until
                # someone refreshes it.
                mviews = [l for l in self._psql("src", db, f"""
                    select n.nspname||'.'||c.relname from pg_class c
                    join pg_namespace n on n.oid = c.relnamespace
                    where c.relkind = 'm' and {USER_SCHEMA}
                    order by 1""").splitlines() if l]
                inv.add("not-carried", db,
                        "materialized views needing REFRESH", mviews)

                # Large objects live outside every table, so a mover that
                # works table by table never sees them - which is most
                # managed services. migkit's own pg_dump path is not one of
                # them: measured, `pg_dump -Fd -j --data-only` (the exact
                # flags `pgdump_move` uses) puts them in the dump and
                # pg_restore lands them. So this is a finding about the leg,
                # not about the objects, and the wording says which.
                los = self._psql("src", db,
                                 "select count(*) from"
                                 " pg_largeobject_metadata").strip()
                inv.add("not-carried", db,
                        "large objects (a managed service moving table by"
                        " table leaves them; migkit's own pg_dump path"
                        " carries them)",
                        [f"{los} in pg_largeobject"] if los not in
                        ("0", "") else [])

                # A user mapping's password is not readable by anyone but its
                # owner, so it cannot be copied even in principle.
                fdw = [l for l in self._psql("src", db,
                       "select srvname||' -> '||usename from pg_user_mappings"
                       ).splitlines() if l]
                inv.add("target-prereq", db,
                        "foreign server user mappings (passwords are not"
                        " readable, so they cannot be copied)", fdw)

                # An untrusted language needs its runtime installed on the
                # target host; no DDL can supply it.
                langs = [l for l in self._psql("src", db,
                         "select lanname from pg_language"
                         " where lanispl and not lanpltrusted"
                         ).splitlines() if l]
                inv.add("target-prereq", db,
                        "untrusted procedural languages", langs)
            except RuntimeError as e:
                inv.unknown("no-row-key", db,
                            f"catalogue query failed:"
                            f" {str(e).splitlines()[-1][:100]}")
        return inv

    def _pk_cols_of(self, db, table, side="src"):
        """The key's columns in key order (`neutral_key`)."""
        return self.neutral_key(side, db, table)

    #: The key files are read back with psql's `\copy`, which parses COPY
    #: text: tabs separate columns and a backslash starts an escape. Writing
    #: the raw value meant the reader and the writer disagreed about both.
    #: Measured against PostgreSQL 16 on a text primary key:
    #:
    #:     key `has<TAB>tab`    ERROR: extra data after last expected column
    #:                          COPY _pk, line 2: "has	tab"   - nothing repaired
    #:     key `back\slash`     apply returned with no error at all, the row
    #:                          was not restored, and the next check still
    #:                          said diff. `\s` is not an escape COPY knows,
    #:                          so it read `backslash` and the join matched
    #:                          no row - a repair reported as done that was
    #:                          never performed, which is the worst shape a
    #:                          failure can take here.
    #:
    #: So every key this engine renders is escaped the way COPY would have
    #: written it, and anything reading one back undoes exactly that.
    PK_ESCAPE = (r"replace(replace(replace(replace({0}, E'\\', E'\\\\'),"
                 r" E'\t', E'\\t'), E'\n', E'\\n'), E'\r', E'\\r')")

    @classmethod
    def _pk_text_expr(cls, cols):
        """SQL for one row's key: each column escaped, then tab-joined."""
        return ("concat_ws(e'\\t', " + ", ".join(
            cls.PK_ESCAPE.format(f'{_ident(c)}::text') for c in cols) + ")")

    @staticmethod
    def _pk_unescape(text):
        """One column's value back from the form `_pk_text_expr` wrote.

        A backslash before anything COPY does not recognise is dropped and
        the character kept, which is what PostgreSQL itself does - so a file
        written by an older migkit is read the way that migkit's repair read
        it, rather than differently.
        """
        known = {"t": "\t", "n": "\n", "r": "\r", "\\": "\\", "b": "\b",
                 "f": "\f", "v": "\v"}
        out = []
        i = 0
        while i < len(text):
            if text[i] == "\\" and i + 1 < len(text):
                out.append(known.get(text[i + 1], text[i + 1]))
                i += 2
            else:
                out.append(text[i])
                i += 1
        return "".join(out)

    @classmethod
    def _pk_parts(cls, key):
        """The original column values of one encoded key."""
        return [cls._pk_unescape(p) for p in key.split("\t")]

    def _compare_pks(self, db, table, keys):
        """Row-level compare of the given pk keys (tab-joined when
        composite). Returns (missing, extra, changed) or None when the
        table has no pk.

        Each side read as the check reads it (`_scope`): through the hop's
        row filter, the target under its own name. Read as the raw table,
        an update of a row the filter leaves behind was a row missing on
        the target every cycle - the delta never advanced, the cutover's
        last check stopped on it, and a repair copied it over."""
        cols = self._pk_cols_of(db, table)
        if not cols:
            return None
        sch, tbl = self._split(table)

        esc = _literal

        def fetch(side):
            out = {}
            klist = sorted(keys)
            for i in range(0, len(klist), 500):
                chunk = klist[i:i + 500]
                if len(cols) == 1:
                    inlist = ", ".join(esc(self._pk_unescape(k))
                                       for k in chunk)
                    where = f'{_ident(cols[0])}::text in ({inlist})'
                else:
                    tup = ", ".join(f'{_ident(c)}::text' for c in cols)
                    vals = ", ".join(
                        "(" + ", ".join(esc(p) for p in self._pk_parts(k))
                        + ")" for k in chunk)
                    where = f"({tup}) in ({vals})"
                pk = self._pk_text_expr(cols)
                h = self._row_hash_expr("src", db, f"{sch}.{tbl}")
                q = (f"select {pk}||'|'||{h}"
                     f" from {self._scope(db, f'{sch}.{tbl}', side)}"
                     f" where {where}")
                for line in self._psql(side, db, q).splitlines():
                    k, _, h = line.rpartition("|")
                    out[k] = h
            return out

        src, dst = fetch("src"), fetch("dst")
        missing = sorted(k for k in keys if k in src and k not in dst)
        extra = sorted(k for k in keys if k in dst and k not in src)
        changed = sorted(k for k in keys
                         if k in src and k in dst and src[k] != dst[k])
        return missing, extra, changed

    def _drill_keys(self, db, table, kind):
        f = self._report(db) / f"data-{table}.{kind}"
        try:
            return [k for k in f.read_text().splitlines() if k]
        except OSError:
            return []

    def _write_pk_files(self, db, table, missing, extra, changed):
        """The keys that differ, a file a kind, which the repair reads
        back: each whole and on the disk (`durable`), sealed where the
        hop keeps values encrypted - a file a crash cut repaired some of
        the keys and said nothing of the rest."""
        d = self._report(db)
        d.mkdir(parents=True, exist_ok=True)
        for kind, rows in (("missing", missing), ("extra", extra),
                           ("changed", changed)):
            f = d / f"data-{table}.{kind}"
            if rows:
                _kept_whole(f, ("\n".join(rows) + "\n").encode())
            elif f.exists():
                f.unlink()

    # --- consistency by design: any consumer (incl. DTS) holds a slot on
    # the source, so "target applied past LSN X" is observable there ---

    def _psql_script(self, side, db, sql):
        """(the client program started on a script read from its input,
        the script): the lanes of a digest that share one exported
        snapshot run as programs of their own, side by side."""
        argv, env = self._client(side, db, "psql", "-X", "-At", "-q", "-v",
                                 "ON_ERROR_STOP=1")
        return subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, env=tool_env(env)), sql

    def _summed(self, h, salt=None):
        """The one fold of a row hash over rows: `h`, an `md5(...)`
        expression, taken behind the run's salt (`checkpoint.new_salt`),
        its first 64 bits summed as numeric, so nothing wraps and a row
        there twice counts twice. The salt draws the 2^-64 chance of two
        different tables summing alike again every run, where without it
        a pair of rows that collided once collided on every check."""
        if not h.startswith("md5("):
            raise ValueError(f"not a row hash: {h[:40]}")
        salt = salt or self.digest_salt()
        return (f"coalesce(sum(('x'||substr(md5({_literal(salt)}||{h[4:]},1,16))"
                "::bit(64)::bigint::numeric), 0)")

    def _key_hash_expr(self, side, db, table):
        """Expression hashing only the primary key of one row, or None.

        A single row hash says "this table differs" and nothing else, so
        finding out what kind of difference it is costs another pass. Hashing
        the key separately in the same scan answers that for free:

          keys equal, values differ   -> rows were modified in place
          keys differ, counts equal   -> rows were replaced
          counts differ               -> rows are missing or extra

        Those three lead to completely different remedies, and knowing which
        one it is before drilling down is the point. It does not replace the
        drilldown - that is still how you learn *which* rows - it removes the
        need for one in order to learn *what happened*.
        """
        # in key order, as every reader of a key takes it (`neutral_key`):
        # by column position, two sides whose columns were added in
        # another order hashed one key two ways (audit 2026-10-01, DU19)
        try:
            cols = self.neutral_key(side, db, table)
        except Exception:  # noqa: BLE001 - no key hash, the row hash stands
            return None
        if not cols:
            return None
        # Joined with chr(2) and standing in for NULL with chr(1), this had
        # the same ambiguity the MySQL row hash was proved to have: measured,
        # `coalesce(chr(1), chr(1)) = coalesce(null, chr(1))` is true, so a
        # key holding chr(1) hashed as a NULL. Unlikely in a primary key and
        # exactly as wrong, so it uses the same encoding as everything else.
        from .. import rowtext
        return f"md5({rowtext.postgres_row(cols)})"

    def _row_hash_expr(self, side=None, db=None, table=None):
        """The one expression that hashes one row of `t`. Used everywhere.

        Built from named columns in name-sorted order, never from the
        whole-row cast, for two measured reasons.

        **Column order.** `t::text` renders columns in physical attribute
        order, so a source whose history includes a DROP COLUMN and an ADD
        COLUMN hashes differently from a target created fresh from the same
        logical schema - identical values, different hash, reported as a
        difference nobody can act on. Measured on PostgreSQL 16: the two sides
        gave -3370828463729857743 and -4255070870107345456 for rows that
        compare equal column by column. Naming the columns removes the
        dependence entirely, and it is what the MySQL engine has always done.

        **PostGIS.** `t::text` renders a geometry as hex EWKB, and that
        encoding is not stable across PostGIS patch releases - two servers
        holding the identical shape produce different bytes. Geometry columns
        are rebuilt as canonical text so the shape is compared, not its
        encoding.

        The jsonb variant uses `jsonb_build_object` rather than wrapping a
        `ROW`: `to_jsonb(ROW(a, b))` throws the column names away and yields
        `{"f1": ..., "f2": ...}`, which would make a renamed column hash the
        same as the original - a check that is quieter than the truth.
        `jsonb_build_object` was measured to produce output byte-identical to
        `to_jsonb(t)`, and jsonb sorts its keys, so it is order-independent
        for free.
        """
        from .. import canon
        jsonb = self.hop.options.get("checksum", "text") == "jsonb"
        cols = self._row_cols(side, db, table)
        if not cols:
            # No catalogue to read - the whole-row cast is all that is left.
            # It is order-dependent, which is the defect described above, so
            # every caller that can name a table should pass one.
            return "md5(to_jsonb(t)::text)" if jsonb else "md5(t::text)"

        def val(c, kind):
            if kind == "geometry":
                return f'ST_AsEWKT(t.{_ident(c)})'
            if kind == "geography":
                return f'ST_AsEWKT(t.{_ident(c)}::geometry)'
            if kind == "wide":
                # a value wider than `canon.WIDE` as its length and md5,
                # hashed where it lies: the row's text around its hex had a
                # 160 MB value killed on a 4 GB source (backlog R22). Any
                # other is the text the row renders it as, as before
                q = f't.{_ident(c)}'
                return (f"(case when octet_length({q}) > {canon.WIDE} then"
                        f" {_literal(canon.WIDE_MARK)} || octet_length({q}) || ':'"
                        f" || md5({q}) else {q}::text end)")
            return f't.{_ident(c)}'

        if jsonb:
            args = ", ".join(f"{_literal(c)}, {val(c, kind)}" for c, kind in cols)
            return f"md5(jsonb_build_object({args})::text)"
        return "md5(ROW(" + ", ".join(val(c, kind)
                                      for c, kind in cols) + ")::text)"

    #: the server memory the wide rows of one digest query may take, about:
    #: each worker of a parallel scan holds the row it hashes, so a table
    #: of wide rows is digested by fewer of them (`_digest_workers`). Set,
    #: not measured: an eighth of the 4 GB machine a source died on
    DIGEST_MEMORY = 512 * 2 ** 20

    def _digest_workers(self, db, t, most):
        """How many parallel workers a digest of `t` asks for: `most`, or
        fewer where two copies of its widest row (`row_sizes`, the measure
        the move's model sizes a worker by) a worker, and the leader's,
        would pass `DIGEST_MEMORY` - none where one row alone would."""
        known = self.__dict__.setdefault("_widest_of", {})
        if (db, t) not in known:
            try:
                got = self.row_sizes("src", db, [t]) or {}
                row = got.get(".".join(self._split(t))) or next(
                    iter(got.values()), {})
                known[(db, t)] = int((row or {}).get("largest") or 0)
            except Exception:  # noqa: BLE001 - not sized: as asked
                known[(db, t)] = 0
        widest = known[(db, t)]
        if not widest:
            return most
        return max(0, min(int(most), self.DIGEST_MEMORY // (2 * widest) - 1))

    #: the types whose values a row's digest takes as their length and md5
    #: where they are wider than `canon.WIDE`: `octet_length` reads their
    #: stored length without reading them
    WIDE_TYPES = ("bytea", "text", "varchar", "bpchar")

    def _row_cols(self, side, db, table):
        """[(column, 'geometry'|'geography'|'wide'|None)] sorted by column
        name.

        Sorted by name rather than by attribute number so the expression is
        the same whichever side produced it - the checkpoint keys its stored
        partials on the expression text, so an expression that varied by side
        would throw away resumable work for no reason.
        """
        if not (side and db and table):
            return None
        key = (side, db, table)
        cache = getattr(self, "_col_cache", None)
        if cache is None:
            cache = self._col_cache = {}
        if key in cache:
            return cache[key]
        sch, tbl = self._split(table)
        rows = self._psql(side, db,
            "select a.attname||'|'||coalesce(t.typname,'') "
            "from pg_attribute a "
            "join pg_class c on c.oid = a.attrelid "
            "join pg_namespace n on n.oid = c.relnamespace "
            "left join pg_type t on t.oid = a.atttypid "
            f"where n.nspname = {_literal(sch)} and c.relname = {_literal(tbl)} "
            "and a.attnum > 0 and not a.attisdropped "
            "order by a.attname").splitlines()
        parsed = [(r.split("|")[0], r.split("|")[1]) for r in rows if r]
        # a byte or text column may hold a value too wide to render whole
        # in a digest (`_row_hash_expr`, `canon.WIDE`)
        out = [(c, ty if ty in ("geometry", "geography") else
                "wide" if ty in self.WIDE_TYPES else None)
               for c, ty in parsed]
        cache[key] = out or None
        return cache[key]

    @staticmethod
    def _snapshot_preamble(snapshot, workers):
        """The statements a worker runs before it reads anything.

        `SET TRANSACTION SNAPSHOT` has to be the first statement of a
        transaction that has not read yet, so the order here is not
        cosmetic: begin, adopt the snapshot, then tune. Putting the `set
        local max_parallel_workers_per_gather` first makes PostgreSQL
        refuse the snapshot.
        """
        lines = ["begin transaction isolation level repeatable read"
                 " read only;"]
        if snapshot:
            lines.append(f"set transaction snapshot {_literal(snapshot)};")
        lines.append(f"set local max_parallel_workers_per_gather = {workers};")
        return lines

    @staticmethod
    def _run_lanes(procs, deadline=None):
        """Feed every lane its script at once and wait for all of them:
        [(returncode, stdout, stderr)] in lane order, or None once
        `deadline` has passed, with every lane ended.

        They were fed one after another - each lane's script was written
        only when the one before it had finished - so the lanes that share
        a snapshot to read in parallel read in turn."""
        import threading
        got = [None] * len(procs)

        def run(i, proc, sql):
            left = None if deadline is None else max(
                deadline - time.time(), 0.001)
            try:
                out, err = proc.communicate(sql, timeout=left)
                got[i] = (proc.returncode, out, err)
            except subprocess.TimeoutExpired:
                got[i] = "late"
        threads = [threading.Thread(target=run, args=(i, pr, sql))
                   for i, (pr, sql) in enumerate(procs)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if any(g == "late" for g in got):
            for pr, _ in procs:
                pr.kill()
                try:
                    pr.communicate(timeout=10)
                except Exception:
                    pass
            return None
        return got

    def _export_snapshot(self, side, db):
        """Open a transaction, export its snapshot, and hand back both.

        Returns `(connection, id)` or `(None, None)`. The connection is the
        point: an exported snapshot lives exactly as long as the
        transaction that exported it, so the caller has to hold it open
        while the workers use it and close it after.

        **What it buys, measured.** migkit's fast data pass reads tables
        through a thread pool, one connection each, so table `a` is read at
        one instant and table `b` at another - and a transaction that moves
        a row between them in that gap shows up as a difference in both.
        On a live server, with a writer inserting between the export and
        the reads:

            workers not sharing the snapshot    a=2  b=2
            workers sharing the snapshot        a=1  b=1

        `--consistent` already avoids this by reading every table of a side
        in one transaction, which costs the parallelism. A shared snapshot
        is how both are had at once.
        """
        try:
            conn = self._connect(side, db)
        except Exception:
            # a server that cannot be reached, which is the caller's cue to
            # fall back. Deliberately narrow: this used to wrap the call
            # itself in a bare `except Exception` and a wrong argument list
            # came back as "no snapshot available" instead of a TypeError.
            return None, None
        try:
            conn.autocommit = False
            cur = conn.cursor()
            cur.execute("begin transaction isolation level repeatable read"
                        " read only")
            cur.execute("select pg_export_snapshot()")
            got = cur.fetchone()
            return (conn, got[0]) if got else (conn, None)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return None, None

    def _snapshot_scripts(self, side, db, tables, snapshot, lanes, workers):
        """One script per lane, every lane reading the shared snapshot.

        The LSN is asked for once, by the first lane, because it is the
        fence for convergence proofs and two lanes reporting two positions
        would leave the caller to pick one. Every lane runs the same
        preamble, so the instant is identical whichever lane a table lands
        in - which is the only reason splitting them is safe.
        """
        lanes = max(1, min(lanes, len(tables)))
        buckets = [tables[i::lanes] for i in range(lanes)]
        out = []
        for n, bucket in enumerate(buckets):
            if not bucket:
                continue
            lines = self._snapshot_preamble(snapshot, workers)
            if n == 0:
                lines.append(
                    "select 'LSN|'||case when pg_is_in_recovery() then"
                    " 'standby (read replica, no fence)' else"
                    " pg_current_wal_insert_lsn()::text end;")
            for t in bucket:
                h = self._row_hash_expr("src", db, t)
                lines.append(
                    f"select {_literal(t + '|')}||count(*)||'|'"
                    f"||{self._summed(h)}"
                    f" from {self._scope(db, t)};")
            lines.append("commit;")
            out.append("\n".join(lines))
        return out

    #: how one side of the consistent pass reads its tables at one
    #: instant (`_fast_consistent`)
    CONSISTENT_WAYS = (
        Rung("lanes", "several connections sharing one exported snapshot",
             needs=(Need("more than one lane and one table",
                         lambda f: f["lanes"] > 1 and f["tables"] > 1,
                         "one lane or one table has nothing to share"),)),
        Rung("one", "one transaction reading every table in turn"))

    def _fast_consistent(self, db):
        """Whole-database checksum inside ONE repeatable-read read-only
        transaction per side: no intra-db skew (every table of a side is
        the same instant), and the src LSN captured in-snapshot gives the
        fence for convergence proofs."""
        # the same tables and the same rows as every other pass reads
        # (`_keep_tbl`, `_scope`): this one read every table whole, so a
        # table the hop excludes and a table under a row filter both came
        # out different on a target that was exactly what the hop asked for
        st = [l for l in self._psql("src", db,
                                    self.USER_TABLES).splitlines()
              if l and self._keep_tbl(db, l)
              and not self.through_pair(db, l)]
        dt = set(l for l in self._psql("dst", db,
                                       self.USER_TABLES).splitlines() if l)
        both = [t for t in st if t in dt]
        w = int(self.hop.options.get("checksum_workers", 8))
        def script(side):
            lines = ["begin transaction isolation level repeatable read"
                     " read only;",
                     f"set local max_parallel_workers_per_gather = {w};",
                     "select 'LSN|'||case when pg_is_in_recovery() then"
                     " 'standby (read replica, no fence)' else"
                     " pg_current_wal_insert_lsn()::text end;"]
            for t in both:
                # named from the source for both sides, so the two scripts
                # hash the same columns in the same order whatever order the
                # two servers store them in
                h = self._row_hash_expr("src", db, t)
                lines.append(
                    f"select {_literal(t + '|')}||count(*)||'|'"
                    f"||{self._summed(h)}"
                    f" from {self._scope(db, t)};")
            lines.append("commit;")
            return "\n".join(lines)

        # One transaction per side is what makes this consistent, and it is
        # also what made it serial: every table of a side queued behind the
        # one before it. An exported snapshot lets several connections read
        # that same instant at the same time, so consistency stops costing
        # the parallelism. Failing to export falls back to the single
        # script rather than reading without one - an inconsistent
        # "consistent" pass is the answer this mode exists to avoid.
        lanes = max(1, int(self.hop.workers or 1))
        # the hop's bound on how long a side's snapshot may be held, in
        # seconds; unset, the pass takes as long as it takes
        limit = float(self.hop.options.get("snapshot_limit") or 0)
        outs, held, said = {}, {}, []
        from .. import decide
        for side in ("src", "dst"):
            began = time.time()

            def read(choice, side=side, began=began):
                conn, snap = None, None
                if choice.path == "lanes":
                    conn, snap = self._export_snapshot(side, db)
                try:
                    if choice.path == "lanes" and not snap:
                        # nothing read yet: one transaction reads the
                        # same instant, without the lanes
                        raise decide.Blocked(
                            "the side would not export its snapshot for"
                            " the lanes to share")
                    scripts = ([script(side)] if not snap
                               else self._snapshot_scripts(
                                   side, db, both, snap, lanes, w))
                    return self._run_lanes(
                        [self._psql_script(side, db, sc) for sc in scripts],
                        began + limit if limit else None)
                finally:
                    # released as soon as this side's lanes are done, not
                    # after the other side's: it used to be held through
                    # both
                    if conn is not None:
                        try:
                            conn.rollback()
                            conn.close()
                        except Exception:
                            pass
            got = self._attempt(
                f"the {side}'s consistent pass", self.CONSISTENT_WAYS, read,
                {"lanes": lanes, "tables": len(both)}, log=said.append,
                switch=decide.only_blocked)[1]
            if got is None:
                return 1, (f"stopped: the pass had held the {side}'s snapshot"
                           f" for {limit:,.0f}s, the most `snapshot_limit`"
                           " allows, and had not finished, so it was ended"
                           " and nothing it read is a verdict. Check without"
                           " --consistent, which reads table by table, or"
                           " raise snapshot_limit")
            for rc, stdout, stderr in got:
                if rc:
                    return 1, (f"consistent pass failed on {side}:"
                               f" {stderr[-300:]}")
            outs[side] = "\n".join(stdout for _, stdout, _ in got)
            held[side] = time.time() - began

        def parse(text):
            lsn, rows = "", {}
            for line in text.splitlines():
                name, _, rest = line.partition("|")
                if name == "LSN":
                    lsn = rest
                elif name:
                    rows[name] = rest
            return lsn, rows

        src_lsn, src_rows = parse(outs["src"])
        dst_lsn, dst_rows = parse(outs["dst"])
        out = [f"consistent snapshot: one repeatable-read txn per side,"
               f" src lsn={src_lsn} dst lsn={dst_lsn}"] + said
        # how long this pass kept the source from cleaning up after itself:
        # nothing newer than its snapshot could be vacuumed meanwhile
        out.append(f"source snapshot held {held.get('src', 0):,.0f}s")
        rc = 0
        for t in both:
            a, b = src_rows.get(t, ""), dst_rows.get(t, "")
            if a == b:
                out.append(f"{t}: OK rows={a.split('|')[0]}"
                           f" checksum={a.split('|', 1)[1]}")
            else:
                rc = 1
                out.append(f"{t}: DIFF src={a} dst={b}")
        for t in st:
            if t not in dt:
                rc = 1
                out.append(f"{t}: ERROR missing on target")
        return rc, "\n".join(out)

    def oldest_snapshot(self, side):
        """(age in transactions, seconds held, who) of the session whose
        snapshot is oldest, or None when none is open. That snapshot is the
        horizon: vacuum removes nothing newer while it lasts."""
        got = self._psql(side, "postgres",
                         "select age(backend_xmin)||'|'||extract(epoch from"
                         " now() - xact_start)::bigint||'|'"
                         "||coalesce(nullif(application_name, ''), usename,"
                         " '') from pg_stat_activity"
                         " where backend_xmin is not null"
                         " and pid <> pg_backend_pid()"
                         " order by age(backend_xmin) desc limit 1").strip()
        if not got:
            return None
        age, secs, who = (got.split("|") + ["", ""])[:3]
        return int(age), float(secs or 0), who

    def _in_recovery(self, side, db="postgres"):
        """True if this endpoint is a standby / read replica (read-only).
        Aurora and RDS readers answer pg_is_in_recovery() = t and reject
        every write, including SELECT ... FOR UPDATE (SQLSTATE 25006)."""
        try:
            return self._psql(side, db, "select pg_is_in_recovery()") == "t"
        except RuntimeError:
            return False

    def src_lsn(self, db):
        # WAL funcs are unavailable during recovery; no source LSN, no fence
        if self._in_recovery("src", db):
            return None
        return self.wal_position("src", db)

    def follow_origin(self, db):
        """The replication origin migkit gives a locally-driven CDC leg.

        pgcopydb's default is the bare name `pgcopydb`, which says nothing
        about which hop or which database it belongs to - and origins are
        cluster-wide, so two databases following at once would share one
        name and overwrite each other's position. Naming it per hop and
        database is what lets `applied_lsn` attribute it.
        """
        raw = f"migkit_{self.hop.name}_{db}".replace("-", "_")
        return "".join(c if c.isalnum() or c == "_" else "_" for c in raw)[:63]

    def follow_slot(self, db):
        """The replication slot that goes with `follow_origin`.

        Same name, lower-cased, because the two namespaces do not accept
        the same characters. Asked of PostgreSQL 16 rather than assumed:

            select pg_create_logical_replication_slot('migkit_Prod_EU_db', ...)
            ERROR:  replication slot name "migkit_Prod_EU_db" contains an
                    invalid character
            HINT:  Replication slot names may only contain lower case
                   letters, numbers, and the underscore character.

        A hop named `Prod-EU` would otherwise build a name the server
        refuses, at the moment the CDC leg starts and not before.
        """
        return self.follow_origin(db).lower()

    #: origins on the target that belong to the database being fenced.
    #: `pg_replication_origin_status` is cluster-wide: connected to one
    #: database it also lists every other one's. Measured on PostgreSQL 16 -
    #: from `postgres`, with the only subscription living in `other`:
    #:
    #:     external_id | remote_lsn
    #:     pgcopydb    | 0/15B9810
    #:     pg_16407    | 0/0
    #:
    #: A fence taking `min(remote_lsn)` across that would wait on a
    #: subscription for a database nobody asked about, parked at `0/0`
    #: because it was created a minute ago, and would never pass. A
    #: subscription is attributable through `pg_subscription.subdbid`; an
    #: origin migkit made is attributable because migkit named it.
    _ORIGIN_ROWS = """
          select o.remote_lsn
            from pg_replication_origin_status o
            join pg_subscription s on o.external_id = 'pg_' || s.oid
           where s.subdbid = (select oid from pg_database
                               where datname = current_database())
          union all
          select o.remote_lsn
            from pg_replication_origin_status o
           where o.external_id = {origin}"""

    def _origin_rows(self, db):
        return self._ORIGIN_ROWS.format(origin=_literal(self.follow_origin(db)))

    def applied_lsn(self, db):
        """The source LSN the *target* has actually applied, or None.

        The source's `confirmed_flush_lsn` is what the consumer said; this
        is what the target did. Both native logical replication and
        pgcopydb write `pg_replication_origin_status` inside the applying
        transaction, so for either path it means "committed here", and the
        two numbers disagree in both directions - measured on PostgreSQL 16
        on one pair under the same insert load:

            native subscription   origin *ahead* of the slot by up to 46 KB
                                  (the slot is a delayed echo of the apply)
            pgcopydb follow       slot ahead of the origin while the target
                                  still held 0 rows

        Read `fence_wait` before reaching for this as a fence: it cannot be
        one. The origin stops at the last applied transaction while the
        source's WAL keeps moving, so `origin >= some current LSN` never
        becomes true on a quiet database.

        What it is good for is saying where the target actually is - in a
        report, and to a CDC leg migkit drives itself, which needs a
        position that is not the mover's own opinion of its progress.
        """
        q = ("select coalesce(min(o.remote_lsn)::text, '') from ("
             + self._origin_rows(db) + ") o")
        try:
            got = self._psql("dst", db, q)
        except RuntimeError:
            return None
        got = got.strip()
        # `0/0` is the absence of a position, not a position. A subscription
        # that has copied its tables but not yet applied a *streamed*
        # transaction sits there - measured: `pg_stat_subscription` showed
        # received_lsn 0/19EB3E8 and the rows were on the target, while the
        # origin still read 0/0; one insert on the source moved it to
        # 0/19EB540. Reading that as "applied nothing" would stall the fence
        # on a database whose only fault is being idle, so it hands back to
        # the slot, which does carry a position for that subscription.
        return None if not got or got == "0/0" else got

    def own_slots(self, db):
        """The names of the slots on the source that carry this hop's own
        stream: its subscription's (named after the subscription, which
        the server folds to lower case), the one its CDC leg driven from
        here reads, the one a physical seed made before its copy
        (`physical.seed_slot`) - by name, so it is waited on before a
        subscription takes it and where none does - the slot of any
        subscription on the target itself - whoever made it, it is what
        writes into this target - and any the hop names (`slot:`) for a
        pipeline migkit did not set up."""
        from ..physical import seed_slot
        named = (self.hop.options or {}).get("slot") or []
        if isinstance(named, str):
            named = [named]
        return sorted({self._repl_name().lower(), self.follow_slot(db),
                       seed_slot(self, db), *[str(n) for n in named],
                       *self._target_feeds(db)})

    def _target_feeds(self, db):
        """The slots the target's own subscriptions read, asked once with
        a short wait and no retry: a fence runs inside a cutover's clock,
        and a target that does not answer - measured, a paused one held
        the retried connection 60s - is no reason to wait longer than the
        stream would have."""
        import psycopg2
        try:
            conn = self._connect("dst", db, "probe", timeout=3)
        except psycopg2.Error:
            return []
        try:
            with conn.cursor() as cur:
                cur.execute("set statement_timeout = 3000")
                cur.execute("select subslotname from pg_subscription where"
                            " subdbid = (select oid from pg_database where"
                            " datname = current_database())"
                            " and subslotname is not null")
                return [r[0] for r in cur.fetchall()]
        except psycopg2.Error:
            return []
        finally:
            conn.close()

    def fence_wait(self, db, lsn, timeout=300):
        """Block until this hop's own slots on the source - by name, active
        or not - have confirmed flushing past `lsn`. True = fence passed,
        False = timed out, None = no slot of the hop's / no source LSN
        (cannot fence).

        By name, because the slot that says anything about this target is
        the one its stream reads. This took the smallest position of every
        *active* slot in the database, and measured on PostgreSQL 16 that
        passed while the hop's subscription was disabled and its slot 1 row
        behind: another consumer's slot, active and caught up, was the only
        one it looked at. A slot that is inactive for the moment - a tail
        reconnecting, a subscription disabled - still holds the hop's
        position, and passing without it is a fence that says nothing.

        This reads the *source's* slot and not `applied_lsn`, and that is
        deliberate rather than an oversight. `applied_lsn` is the better
        number in the sense that it means "committed on the target" - but it
        can only ever be the LSN of the last transaction that was applied,
        so on a quiet database it stops and the source's WAL keeps moving.
        Measured on PostgreSQL 16, one healthy subscription, no writes, five
        samples three seconds apart:

            pg_current_wal_lsn   0/19EB660
            confirmed_flush_lsn  0/19EB660   (keepalives carry it to the end)
            origin remote_lsn    0/19EB540   (288 bytes back, and staying)

        A fence of `origin >= lsn`, or of `origin >= slot`, never passes
        there. It would time out on every idle pair and fall through to the
        weaker sleep-settle path without saying so - a fence that quietly
        stops fencing. The gap is also not a fault signal on its own: it
        looks the same as a consumer holding changes it has not applied,
        which is what `check`'s row comparison is for.
        """
        if lsn is None:
            return None
        names = ", ".join(_literal(n)
                          for n in self.own_slots(db))
        mine = (f" from pg_replication_slots where database = {_literal(db)}"
                f" and slot_type = 'logical' and slot_name in ({names})")
        if self._psql("src", db, "select count(*)" + mine) == "0":
            return None
        q = ("select coalesce(min(confirmed_flush_lsn), '0/0'::pg_lsn)"
             f" >= {_literal(lsn)}::pg_lsn" + mine)
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            if self._psql("src", db, q) == "t":
                return True
            # a cutover's clock runs while this waits: asked every 50 ms,
            # each round one statement on the kept connection - it was a
            # program and a connection a round, every 0.25 s (audit
            # 2026-10-01, SP10)
            time.sleep(0.05)
        return False

    def _column_fingerprint(self, db, table):
        """One scan, one aggregate per column: which columns actually
        differ. Localizes drift (e.g. only updated_at differs = timezone
        rendering, not data loss) before any row-level work."""
        sch, tbl = self._split(table)
        try:
            cols = [l for l in self._psql("src", db,
                    "select attname from pg_attribute"
                    " where attrelid ="
                    f" {_literal(_ident(sch) + '.' + _ident(tbl))}::regclass"
                    " and attnum > 0 and not attisdropped"
                    " and attgenerated = '' order by attnum").splitlines() if l]
        except Exception as e:
            return self._fingerprint_failed(e)
        if not cols:
            return self._fingerprint_failed(
                f"the source lists no columns for {table}")
        # One value per hash, so no separator is involved - but chr(1) stood
        # in for NULL, and a column holding chr(1) hashed as a NULL
        from .. import rowtext
        expr = ", ".join(
            self._summed(f"md5({rowtext.postgres_row([c], alias='')})")
            for c in cols)
        # the rows the check compared, not the whole table: under a row
        # filter the whole source differs from the target in every column
        q = f"select {expr} from {self._scope(db, f'{sch}.{tbl}')}"
        try:
            a = self._psql("src", db, q).split("|")
            b = self._psql("dst", db, q).split("|")
        except RuntimeError as e:
            return self._fingerprint_failed(e)
        diff = [c for c, x, y in zip(cols, a, b) if x != y]
        out = self.hop.report_dir(db) / f"data-{table}.columns"
        if diff:
            out.write_text("columns differing between src and dst:\n"
                           + "\n".join(diff) + "\n")
        elif out.exists():
            out.unlink()
        return diff

    # --- delta verify: a source slot records which rows changed; each cycle
    # verifies only those pks and advances only when clean (idempotent) ---

    def _delta_slot(self, db):
        import re as _re
        name = f"migkit_delta_{self.hop.name}_{db}"
        return _re.sub(r"[^a-z0-9_]", "_", name.lower())[:63]

    def delta_setup(self, db):
        slot = self._delta_slot(db)
        have = self._psql("src", db,
                          "select 1 from pg_replication_slots"
                          f" where slot_name = {_literal(slot)}")
        if have:
            return False
        self._psql("src", db,
                   "select pg_create_logical_replication_slot("
                   f"{_literal(slot)}, 'test_decoding')")
        return True

    def delta_teardown(self, db):
        slot = self._delta_slot(db)
        self._psql("src", db,
                   "select pg_drop_replication_slot(slot_name)"
                   " from pg_replication_slots"
                   f" where slot_name = {_literal(slot)}")

    @staticmethod
    def _parse_decoding(lines, pk_of):
        """Parse test_decoding rows into {table: set(pk_key)}.
        pk_of(table) -> ordered pk column list or None."""
        import re as _re
        field_re = _re.compile(r"(\w+)\[[^\]]*\]:('(?:[^']|'')*'|[^ ]+)")
        head_re = _re.compile(
            r"table ([^:]+): (INSERT|UPDATE|DELETE): (.*)")
        touched = {}
        nopk = set()
        last_lsn = ""
        for lsn, data in lines:
            last_lsn = lsn or last_lsn
            m = head_re.match(data)
            if not m:
                continue
            table = m.group(1).replace('"', "")
            pks = pk_of(table)
            if not pks:
                nopk.add(table)
                continue
            vals = {}
            for col, val in field_re.findall(m.group(3)):
                if val.startswith("'"):
                    val = val[1:-1].replace("''", "'")
                vals[col] = val
            if all(p in vals for p in pks):
                touched.setdefault(table, set()).add(
                    "\t".join(vals[p] for p in pks))
        return touched, nopk, last_lsn

    def delta_verify(self, db, limit=20000, log=None):
        slot = self._delta_slot(db)
        if self._in_recovery("src", db):
            return [Result("delta", db, "error",
                           "source is a read replica (pg_is_in_recovery=t):"
                           " logical slots live on the primary. Point the"
                           " hop's source at the writer/cluster endpoint")]
        if self.delta_setup(db):
            return [Result("delta", db, "ok",
                           f"slot {slot} created, changes are tracked"
                           " from this point on")]
        out = self._psql("src", db,
                         "select lsn||' '||data from"
                         f" pg_logical_slot_peek_changes({_literal(slot)},"
                         f" null, {limit})")
        lines = [l.split(" ", 1) for l in out.splitlines() if " " in l]
        pk_cache = {}

        def pk_of(t):
            if t not in pk_cache:
                try:
                    pk_cache[t] = self._pk_cols_of(db, t)
                except RuntimeError:
                    pk_cache[t] = None
            return pk_cache[t]

        touched, nopk, last_lsn = self._parse_decoding(lines, pk_of)
        # what the hop takes: a table it leaves out is passed over below,
        # its changes stepped past with the rest, and not counted or said
        taken = [t for t in touched if self._keep_tbl(db, t)]
        n_changes = sum(len(touched[t]) for t in taken)
        if not touched and not nopk:
            return [Result("delta", db, "ok",
                           "0 changes since last verified point")]
        res = []
        clean = True
        for t, keys in sorted(touched.items()):
            if not self._keep_tbl(db, t):
                # the target's own: nothing else compares it either
                continue
            # a mapped table is compared by the pair, through the mapping,
            # and the pair keeps its own drilldown
            pair = self.through_pair(db, t)
            cmp = self._delta_compare(db, t, keys)
            if cmp is None:
                res.append(Result("delta", f"{db}.{t}", "error",
                                  "pk lookup failed"))
                clean = False
                continue
            missing, extra, changed = cmp
            if missing or extra or changed:
                if not pair:
                    self._write_pk_files(db, t, missing, extra, changed)
                got, held_back = self._delta_held(Result(
                    "delta", f"{db}.{t}", "diff",
                    f"of {len(keys)} touched rows: missing={len(missing)}"
                    f" extra={len(extra)} changed={len(changed)}",
                    str(self._report(db)),
                    f"migkit sync {self.hop.name} --db {db} --kind rows",
                    table=str(t), rows_apart=rows_counted(
                        t, len(missing), len(extra), len(changed))))
                clean = clean and not held_back
                res.append(got)
            else:
                res.append(Result("delta", f"{db}.{t}", "ok",
                                  f"{len(keys)} touched rows verified"
                                  " equal on both sides"))
            if log:
                log(f"{t}: {len(keys)} touched, "
                    + self._delta_said(res[-1]))
        for t in sorted(nopk):
            if not self._keep_tbl(db, t):
                continue
            res.append(Result("delta", f"{db}.{t}", "skip",
                              "no pk, cannot delta-verify"))
        if clean and last_lsn:
            self._psql("src", db,
                       "select pg_replication_slot_advance("
                       f"{_literal(slot)}, {_literal(last_lsn)}::pg_lsn)")
            note = "slot advanced"
        else:
            note = "slot NOT advanced, window replays next cycle"
        if len(lines) >= limit:
            note += f"; window truncated at {limit} changes, more pending"
        res.insert(0, Result("delta", db,
                             "ok" if clean else "diff",
                             f"{n_changes} changed rows across"
                             f" {len(taken)} tables, {note}"))
        return res

    def list_move_tables(self, db):
        out = self._psql("src", db,
            "select n.nspname||'|'||c.relname from pg_class c"
            " join pg_namespace n on n.oid = c.relnamespace"
            " where c.relkind = 'r'"
            " and n.nspname not in ('pg_catalog','information_schema')"
            " and n.nspname not like 'pg\\_%'"
            " and n.nspname not like '\\_\\_%' order by 1")
        # what the hop excludes is the target's, and another tool's
        # bookkeeping nobody's: the copier took both, and wrote over a
        # table the target owns with the source's rows. A partition is
        # asked with its table's name, which the hop's rules are written
        # in (`in_scope`, `_ruled`); one whose table the pair carries - its
        # columns mapped, or renamed (`through_pair`) - goes as that table,
        # once: the pair reads it through its table
        got = {}
        for x in out.splitlines():
            if not x:
                continue
            s, t = x.split("|", 1)
            if not self.in_scope(db, f"{s}.{t}"):
                continue
            root = self._ruled(db, f"{s}.{t}")
            if root != (s, t) and self.through_pair(db, ".".join(root)):
                s, t = root
            got[(s, t)] = True
        got = list(got)
        # an online schema change's working tables are not the
        # application's rows, and go when it swaps them in
        from .. import drift
        working = drift.transient_among(f"{s}.{t}" for s, t in got)
        return [(s, t) for s, t in got if f"{s}.{t}" not in working]

    def _int_pk(self, db, sch, tbl):
        """The key's one column where it is a whole number, or None."""
        key = self.neutral_key("src", db, f"{sch}.{tbl}")
        if len(key) != 1:
            return None
        declared = dict(self.neutral_columns("src", db, f"{sch}.{tbl}"))
        return key[0] if declared.get(key[0]) in (
            "smallint", "integer", "bigint") else None

    def _copy_cols(self, db, sch, tbl):
        """The source's live columns, in its own order, or None.

        None means the catalogue could not be read, and the copy then falls
        back to positional mapping - which is what it always did. Better to
        keep working than to refuse, but the caller logs it.
        """
        try:
            out = self._psql("src", db,
                             "select a.attname from pg_attribute a"
                             " join pg_class c on c.oid = a.attrelid"
                             " join pg_namespace n on n.oid = c.relnamespace"
                             f" where n.nspname = {_literal(sch)}"
                             f" and c.relname = {_literal(tbl)}"
                             " and a.attnum > 0 and not a.attisdropped"
                             " and a.attgenerated = ''"
                             " order by a.attnum")
        except Exception:
            return None
        cols = [l for l in out.splitlines() if l]
        return cols or None

    @staticmethod
    def _copy_select(qt, cols, pred=""):
        """SELECT naming the columns, so both ends of the pipe agree."""
        what = ", ".join(f'{_ident(c)}' for c in cols) if cols else "*"
        sql = f"select {what} from {qt}"
        return f"{sql} where {pred}" if pred else sql

    def _load_options(self, db, qt):
        """The session options a load into `qt` runs under: as a replica,
        which a trigger does not fire for - the way the bulk paths load.
        Measured before: a `BEFORE INSERT` trigger stamping `now()`
        rewrote every row this copied. Asked once per database, and
        refused where the user may not and the table has triggers."""
        got = self.replica_env(db).get("PGOPTIONS", "")
        if got:
            return got
        cache = self.__dict__.setdefault("_quiet_triggers", {})
        name = qt.replace('"', "")
        if (db, name) not in cache:
            from .. import movers
            cache[(db, name)] = movers._pg_quiet_triggers(self.hop, db,
                                                          [name])
        if cache.get((db, name)) in ("flag", "session"):
            return "-c session_replication_role=replica"
        return ""

    def _source_copy(self, db, commands):
        """(process, stream, beside the source) reading what the source's
        client writes for `commands`: beside the source through the relay
        and compressed where there is one (`_relay`), here otherwise."""
        relay = self._relay(None, db, commands)
        if relay is not None:
            proc, stream = relay
            return proc, stream, True
        args, env = self._client("src", db, "psql", "-X", "-q", "-v",
                                 "ON_ERROR_STOP=1",
                                 *[a for c in commands for a in ("-c", c)])
        proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, env=tool_env(env))
        return proc, proc.stdout, False

    def _range_digest_sql(self, db, table, where=""):
        """The one statement both servers answer for a range of a table:
        its rows and the fold of their hashes, summed as the data check
        sums a table (`_row_hash_expr`, `_summed`), so a table's ranges
        fold to the table's own number."""
        sch, tbl = self._split(table)
        h = self._row_hash_expr("src", db, f"{sch}.{tbl}")
        return (f"select count(*)::text || '|' || {self._summed(h)}::text"
                f' from {_ident(sch)}.{_ident(tbl)} t'
                + (f" where {where}" if where else ""))

    def _copy_pipe(self, db, select_sql, qt, pre_sql="", columns=None):
        """Stream rows from source to target through a COPY pipe.

        `columns` names them on the receiving side. Without it COPY maps by
        position, and position is not a property either side agrees on:
        measured, a source holding (id, a='AAA', b='BBB') copied into a target
        whose columns are declared (id, b, a) lands as a='BBB', b='AAA' - no
        error, every value in the wrong column. A source with a DROP COLUMN in
        its history against a target created fresh from today's schema is
        exactly that shape, and migkit has already measured that pairing
        happening in the row hash.

        So the column list is not an optimisation. It is the difference
        between moving the data and moving it into the wrong columns.
        """
        collist = ""
        if columns:
            collist = " (" + ", ".join(_ident(c) for c in columns) + ")"
        relay = self._relay(select_sql, db)
        if relay is not None:
            out, source = relay
        else:
            argv, env = self._client("src", db, "psql", "-X", "-q", "-v",
                                     "ON_ERROR_STOP=1", "-c",
                                     f"\\copy ({select_sql}) to stdout")
            out = subprocess.Popen(argv, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   env=tool_env(env))
            source = out.stdout
        cmds = ["-c", pre_sql] if pre_sql else []
        argv, env = self._client("dst", db, "psql", "-X", "-q", "-v",
                                 "ON_ERROR_STOP=1", "-1", *cmds, "-c",
                                 f"\\copy {qt}{collist} from stdin",
                                 options=self._load_options(db, qt))
        inp = subprocess.Popen(argv, stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, env=tool_env(env))
        # passed on through here, and tallied on the way: what the target
        # is read back against once it has committed (`_copy_checked`)
        from ..stall import Guard
        from ..tally import Tally
        tally = Tally()
        # both programs ended if the link holds and passes nothing, the
        # commit's wait included
        guard = Guard(f"{qt}", (out, inp))
        try:
            while True:
                chunk = source.read(1 << 16)
                guard.moved()
                if not chunk:
                    break
                tally.feed(chunk)
                inp.stdin.write(chunk)
                guard.moved()
        except BrokenPipeError:
            # the target stopped taking rows - it refused the copy, or its
            # link went - and the source's copy has nowhere to go. Left
            # running, it sat on its full pipe for good and this waited on
            # it (measured: a table missing on the target held the move)
            out.kill()
        finally:
            try:
                inp.stdin.close()
            except BrokenPipeError:
                pass
        err_i = inp.stderr.read()
        inp.stdout.read()
        inp.wait()
        err_o = out.stderr.read()
        out.wait()
        guard.stop()
        if relay is not None and b"migkit-read:0" not in err_o:
            # the read's own end, said by the far machine: a pipe through
            # a compressor ends well whether the read did or not
            raise RuntimeError("the read beside the source did not finish: "
                               + err_o.decode(errors="replace")[-300:])
        if out.returncode or inp.returncode:
            raise RuntimeError((err_o + err_i).decode()[-300:])
        return tally.end()

    def _relay(self, select_sql, db, commands=None):
        """(process, stream) reading `select_sql` beside the source: where
        the source is reached through an ssh tunnel whose machine has psql
        and zstd, the rows are read there, next to the database, and cross
        the link once, compressed, inside the ssh connection - instead of
        COPY's text crossing it as it is (R17d). Measured, 200,000 rows
        over a link held to 5 MB/s and 10 ms: 7.1s as text, 4.3s read
        beside the source. None where they cannot. `commands`, where
        given, are what the far client runs instead - the pass-through's
        read under a shared snapshot, in COPY's binary encoding.

        The password is written to the far read's standard input, never on
        its command line; the read's own exit status comes back on its
        error stream, as a pipe's status is the compressor's."""
        import shlex
        import subprocess

        from .. import tunnel
        s = self.hop.source
        t = tunnel.of(s)
        if t is None or not t.has("psql", "zstd"):
            return None
        host, port = t.far
        # the side's TLS as a program on another machine can carry it: an
        # authority's file is on this one (`tls_forms`'s `far`)
        far = {"PG" + k.upper(): v
               for k, v in s.tls_forms("postgres")["far"].items()}
        env = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in
                       {**far, "PGCONNECT_TIMEOUT": str(CONNECT_SECONDS),
                        "PGAPPNAME": "migkit", "PGOPTIONS": SESSION}.items())
        read = " ".join(shlex.quote(a) for a in [
            "psql", "-h", host, "-p", str(port), "-U", s.user, "-d", db,
            "-X", "-q", "-v", "ON_ERROR_STOP=1",
            *[a for c in (commands or [f"\\copy ({select_sql}) to stdout"])
              for a in ("-c", c)]])
        line = ("IFS= read -r PGPASSWORD; export PGPASSWORD; "
                + (f"export {env}; " if env else "")
                + "{ " + read + "; echo \"migkit-read:$?\" >&2; }"
                " | zstd -q -3 -T0 -c")
        proc = subprocess.Popen(t.there(line), stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        proc.stdin.write((s.password or "").encode() + b"\n")
        proc.stdin.close()
        import zstandard
        return proc, zstandard.ZstdDecompressor().stream_reader(
            proc.stdout, read_across_frames=True)

    def _copy_in_spans(self, db, key, qt, cols, rf, st, ck, log):
        """A table with no integer key copied in parts it can go on from
        (`ranges.keyless`): by where its rows are stored (`position_spans`)
        or by buckets of its rows' hash, a part a transaction, each
        checkpointed once it has committed - so a stop costs the part it
        was in, not the table. A span's stop is placed by the target's
        count; a bucket begun and not done is emptied on the target by the
        same hash and copied again. The whole table is then held to what
        passed, and where it does not add up, bucket by bucket, only the
        buckets that differ copied again. Returns whether it was copied
        this way."""
        from .. import decide, ranges
        sc, dc, _ = self._comparable_columns(db, self, key, self, key)
        began = time.monotonic()
        way = ranges.keyless(
            key, st, self, self, db, key, key, sc, dc,
            int(self._rows_of("src", db, key) or 0),
            lambda: self.position_spans("src", db, key, ranges.LEAST),
            self.digest_salt(), decide.HopCosts(self.hop, "keyless/"))
        if way.restart:
            self._psql("dst", db, f"delete from {qt} where {rf}" if rf
                       else f"truncate {qt}")
            log(f"{key}: the copy that stopped went another way; the target"
                " table is emptied and copied again")
        if way.chunk in (None, "one pass"):
            return False
        if way.chunk != "pages":
            log(f"{key}: {way.strategy.said} -"
                f" {way.strategy.reasons.get('chunk', '')}")

        def scoped(where):
            return (f"({where}) and ({rf})" if where and rf else
                    where or rf or "")

        def count(side, where):
            return int(self._psql(side, db, f"select count(*) from {qt}"
                                  + (f" where {scoped(where)}"
                                     if scoped(where) else "")).strip())

        def restart():
            self._psql("dst", db, f"delete from {qt} where {rf}" if rf
                       else f"truncate {qt}")
            ck.save()

        def clear(where_there):
            return int(self._psql("dst", db, (
                f"with gone as (delete from {qt} where {scoped(where_there)}"
                " returning 1) select count(*) from gone")).strip() or 0)
        if "spans" not in st and way.by_bucket:
            # a fresh start: what the target holds of the table is not this
            # copy's (by count, `spans_to_copy` empties it the same way)
            restart()
        for attempt in (1, 2):
            if way.by_bucket:
                todo = ranges.buckets_to_copy(st, way.spans, clear, log, key)
            else:
                todo = [(s_, w, None) for s_, w in ranges.spans_to_copy(
                    st, [(s_, w) for s_, w, _ in way.spans],
                    lambda: count("dst", None),
                    lambda where: count("src", where), restart, log, key)]
            for start, where, _ in todo:
                if way.by_bucket:
                    ranges.bucket_begun(st, start, ck.save)
                sent = self._copy_pipe(db, self._copy_select(qt, cols,
                                                             scoped(where)),
                                       qt, "", columns=cols)
                ranges.span_copied(st, start, sent.n, sent.total, ck.save)
                log(f"{key}: {len(st['spans_done'])} of {len(st['spans'])}"
                    " parts copied (no key: "
                    + ("by where the rows are stored)" if way.chunk == "pages"
                       else "by buckets of the rows' hash)"))
            whole = ranges.spans_total(st)
            if not (self.hop.options or {}).get("verify_batches", True):
                break
            back = self._copy_out_tally("dst", db,
                                        self._copy_select(qt, cols, rf or ""))
            held = (whole is not None and (back.n, back.total) == whole) \
                or self._range_same(db, key, rf)
            if held:
                break
            if not way.by_bucket or attempt == 2:
                raise SystemExit(
                    f"{key}: the target holds {back.n:,} rows where"
                    f" {whole[0] if whole else '?'} were copied, or the same"
                    " number holding different values. The target changes"
                    " what it is given, or another writer is writing this"
                    " table")
            # held bucket by bucket to the source's: only those that differ
            # are emptied there and copied again
            bad = [s_ for s_, w, _ in way.spans
                   if self._range_same(db, key, scoped(w)) is False]
            log(f"{key}: the target does not hold what was copied;"
                f" {len(bad)} of {len(way.spans)} buckets differ, and only"
                " those are copied again")
            st["spans_done"] = [x for x in st["spans_done"] if x not in bad]
            for x in bad:
                st["span_tally"].pop(str(x), None)
                ranges.bucket_begun(st, x, ck.save)
            ck.save()
        costs = decide.HopCosts(self.hop, "keyless/")
        took = time.monotonic() - began
        for part, rung in way.strategy.names.items():
            costs.saw(f"{part}/{rung}", max(int(whole[0]) if whole else 1,
                                            1), took)
        st["done"] = True
        ck.save()
        return True

    def _copy_out_tally(self, side, db, select_sql):
        """The rows `select_sql` answers on `side`, tallied as COPY writes
        them - the read-back half of `_copy_checked`."""
        from ..tally import Tally
        argv, env = self._client(side, db, "psql", "-X", "-q", "-v",
                                 "ON_ERROR_STOP=1", "-c",
                                 f"\\copy ({select_sql}) to stdout")
        got = subprocess.Popen(argv, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, env=tool_env(env))
        tally = Tally()
        while True:
            chunk = got.stdout.read(1 << 16)
            if not chunk:
                break
            tally.feed(chunk)
        _, err = got.communicate()
        if got.returncode:
            raise RuntimeError(err.decode()[-300:])
        return tally.end()

    def _copy_checked(self, db, qt, cols, where, pre_sql, what, log,
                      way=None, proved=None):
        """One range copied and held to what the source holds, then
        `proved()`: here, through the pipe (`_piped`), or beside the next
        range's load where the table's trials sent the range server to
        server (`way`, `_passed`). Returns what it moved - the pipe's
        tally, or the rows the pass-through wrote - for the move's pace."""
        import time
        began = time.monotonic()
        if way is not None and way.passes:
            return self._passed(db, qt, cols, where, pre_sql, what, log, way,
                                proved, began)
        sent = self._piped(db, qt, cols, where, pre_sql, what, log)
        if way is not None:
            way.took(sent.n, time.monotonic() - began)
        if proved is not None:
            proved()
        return sent

    def _passed(self, db, qt, cols, where, pre_sql, what, log, way, proved,
                began):
        """The range through the pass-through (`pg_pass.TableCopy.copy`)
        and checked beside the next range's load: both servers' digests
        read at the same moment, the source's under the snapshot the copy
        read, and where the commit did not wait for the target's disk, the
        target's log flushed past it - then `proved()`. A range emptied
        whole (`truncate`) is loaded `FREEZE`: its rows are written
        already visible to every later reader, so neither the digest right
        after nor the target's first reader writes each page again to mark
        them.

        A range whose digests differ was changed by the target, never by
        the source moving on - the source's digest reads what the copy
        read. It is copied again as text, where `_piped` reads it back and
        stops the copy on it if it still differs, and the pass-through is
        not tried again on the table. A snapshot that ended before a read
        could import it is let go, and the range read under a new one."""
        import time

        from . import pg_pass
        ctx = way.ctx
        try:
            rows, lsn = ctx.copy(
                way.export, where, pre_sql, what,
                freeze=pre_sql.lower().startswith("truncate"),
                unawaited=way.name == "pass-through unawaited")
        except pg_pass.SnapshotLost:
            ctx.snapshot.lost(way.export)
            ctx.snapshot.release(way.export)
            return self._copy_checked(db, qt, cols, where, pre_sql, what,
                                      log, way=way.ways.pick(way.begin),
                                      proved=proved)
        except Exception as e:  # noqa: BLE001 - said, and the pipe goes
            # a server that refused the binary encoding (a type whose
            # form differs between the two), or one that is gone - which
            # the pipe then finds too, and raises
            ctx.snapshot.release(way.export)
            why = str(e).strip().splitlines()[-1][:200] if str(e) else e
            log(f"{what}: could not pass server to server ({why}) -"
                " copying it as text")
            return self._copy_checked(
                db, qt, cols, where, pre_sql, what, log,
                way=way.instead(f"a range could not pass server to server:"
                                f" {why}"), proved=proved)
        except BaseException:
            ctx.snapshot.release(way.export)
            raise
        checked = (self.hop.options or {}).get("verify_batches", True)

        def check():
            why = ("both servers digest it differently: the target changes"
                   " what it is given")
            try:
                same = ctx.same(way.export, where) if checked else True
            except pg_pass.SnapshotLost as e:
                same = False
                why = f"its snapshot ended before it was digested ({e})"
            finally:
                ctx.snapshot.release(way.export)
            if same:
                if lsn is not None:
                    ctx.durable(lsn)
                way.took(rows, time.monotonic() - began)
                if proved is not None:
                    proved()
                return
            log(f"{what}: passed server to server, {why} - copying it again"
                " as text")
            self._copy_checked(db, qt, cols, where, pre_sql, what, log,
                               way=way.instead(
                                   "a range passed server to server was"
                                   f" held different: {why}"),
                               proved=proved)
        ctx.check(check)
        return rows

    def _piped(self, db, qt, cols, where, pre_sql, what, log):
        """One range copied through the pipe, then read back from the
        target and held to what passed: a copy that says it moved a range
        has moved exactly those rows. Where the two differ, the range is
        asked of both servers in the rendering every engine shares - two
        releases can write a value's text differently and hold the same
        value - and copied once more if that differs too. A range that
        still differs stops the copy, named. A source written to while it
        is copied is not a difference: what passed is the reference, not
        what the source holds by the time the target is read."""
        select = self._copy_select(qt, cols, where)
        sent = self._copy_pipe(db, select, qt, pre_sql, columns=cols)
        if not (self.hop.options or {}).get("verify_batches", True):
            return sent
        import time
        table = qt.replace('"', '')
        for attempt in (1, 2):
            # a range is proved either way: both servers digest it and two
            # numbers cross the link, or it is read back and held to what
            # was sent. Which is cheaper is the link's and the servers', not
            # migkit's to assume - measured, 400,000 rows: 1.9s reading back
            # and 2.4s digesting beside the target, 2.5s and 3.2s 10 ms away
            # with the bandwidth unbounded; with each connection held to
            # 20 MB/s, 4.4s reading back and 4.2s choosing. Each is tried
            # once, timed per row, and the cheaper taken from then on
            way = self._verify_way()
            began = time.monotonic()
            if way == "digest":
                same = self._range_same(db, table, where, every=cols)
                self._verify_took("digest", time.monotonic() - began,
                                  sent.n)
                if same:
                    return sent
            there = self._copy_out_tally("dst", db, select)
            if way == "read back":
                self._verify_took("read back", time.monotonic() - began,
                                  sent.n)
            if there == sent:
                return sent
            if way == "read back" and self._range_same(db, table, where):
                return sent
            if attempt == 2:
                break
            log(f"{what}: read back from the target different from what"
                " was copied; copying it again")
            sent = self._copy_pipe(db, select, qt, pre_sql, columns=cols)
        raise SystemExit(
            f"{what}: copied twice, and the target reads back {there.n:,}"
            f" rows where {sent.n:,} were copied, or the same number"
            " holding different values. The target changes what it is"
            " given, or another writer is writing these rows. The copy"
            " stopped here; the ranges before it read back equal")


    def move_key(self, db, sch, tbl):
        return f"{sch or 'public'}.{tbl}"

    #: how one range of a table goes from a PostgreSQL source into a
    #: PostgreSQL target, climbed per range on the table's own costs
    #: (`_copy_ways`, `pg_pass.Ways`)
    COPY_WAYS = (
        Rung("pass-through",
             "the rows passed server to server in COPY's binary encoding,"
             " each range digested by both servers under one snapshot",
             gives=frozenset({"row filter", "resume by chunk",
                              "consistent as of"}),
             needs=(Need("the same columns of the same types on both sides,"
                         " each with a binary encoding",
                         lambda f: not f["binary"],
                         lambda f: f["binary"]),),
             because="no row passes through migkit, and a range is proved"
                     " by two numbers read under the snapshot it was copied"
                     " from"),
        Rung("pass-through unawaited",
             "the rows passed server to server, each range's commit not"
             " waiting for the target's disk - recorded once it is there",
             gives=frozenset({"row filter", "resume by chunk",
                              "consistent as of"}),
             needs=(Need("the same columns of the same types on both sides,"
                         " each with a binary encoding",
                         lambda f: not f["binary"],
                         lambda f: f["binary"]),
                    Need("a target that says how far its log is on disk",
                         lambda f: (f["dst_version"] or 0) >= 100000,
                         "{rung} needs a target of PostgreSQL 10 or later,"
                         " which says how far its log is on disk"))),
        Rung("pipe", "the rows passed through migkit as text and tallied",
             gives=frozenset({"row filter", "resume by chunk"})),
    )

    BINARY_SQL = (
        "select a.attname || chr(31) || format_type(a.atttypid, null)"
        " || chr(31) || (t.typsend::oid <> 0 and t.typreceive::oid <> 0)::int"
        " from pg_attribute a join pg_class c on c.oid = a.attrelid"
        " join pg_namespace n on n.oid = c.relnamespace"
        " join pg_type t on t.oid = a.atttypid"
        " where n.nspname = {sch} and c.relname = {tbl}"
        " and a.attnum > 0 and not a.attisdropped")

    def _binary_columns(self, side, db, sch, tbl):
        """{column: (its type, whether it has a binary form)} on one side."""
        out = {}
        sql = self.BINARY_SQL.format(sch=_literal(sch), tbl=_literal(tbl))
        for line in self._psql(side, db, sql).splitlines():
            name, typ, ok = line.split("\x1f")
            out[name] = (typ, ok == "1")
        return out

    def _binary_unfit(self, db, sch, tbl, cols):
        """Why the columns a copy carries cannot pass in COPY's binary
        encoding, or "": it names no type, so each value is read on the
        target as the target's column type - both sides must hold each
        column as the same type, one with a binary form."""
        try:
            src = self._binary_columns("src", db, sch, tbl)
            dst = self._binary_columns("dst", db, sch, tbl)
        except Exception as e:  # noqa: BLE001 - said as the reason
            return ("the columns' types could not be read: "
                    + str(e).strip().splitlines()[-1][:100])
        # every column of the source's, the ones computed there too: the
        # range's digest reads them all on both sides (`_range_digest_sql`)
        for c in list(cols) + sorted(set(src) - set(cols)):
            if c not in dst:
                return f"the target has no column {c}"
            (ts, bs), (td, bd) = src.get(c, (None, False)), dst[c]
            if ts != td:
                return f"{c} is {ts} on the source and {td} on the target"
            if c in cols and not (bs and bd):
                return f"{c} ({ts}) has no binary encoding"
        return ""

    #: how a whole table reaches a PostgreSQL target from a PostgreSQL
    #: source, where the table copier carries it: one ladder (`tableway`),
    #: each rung costed on the table by the database's cost model; no
    #: benchmark predicts them, so each is tried where the trials allow
    #: (`decide.Trials`) and measured.
    #: The whole-database programs carry the rest of the database, chosen
    #: for it by `movers._ladder` - a database is not moved twice to learn
    #: which is faster - and each range of the copier's climbs its own ways
    #: (`COPY_WAYS`) on the table's own measurements.
    TABLE_WAYS = (
        Way("ranges", "the table copier's ranges",
            short="the table copier's ranges",
            gives=frozenset({"resume by chunk", "row filter"}),
            run="_copy_ranges"),
        Way("streaming", "the streaming copy of the whole table, read"
            " under the table's snapshot and proved by both servers'"
            " digests of it", short="the streaming copy",
            needs=(Need("the streaming copy installed",
                        lambda f: f["streaming"]),
                   Need("a table large enough to repay starting it",
                        lambda f: f["large"],
                        "the table is small enough that starting the"
                        " streaming copy costs more than it could save"),
                   Need("a table no foreign key points at",
                        lambda f: f["truncatable"] is True,
                        lambda f: "whether a foreign key points at the table"
                        " could not be asked yet" if f["truncatable"] is None
                        else "the streaming copy empties the table first,"
                        " which the target refuses while a foreign key points"
                        " at it"),
                   Need("the same columns of the same types on both sides",
                        lambda f: not f["binary"],
                        lambda f: f["binary"])),
            footprint=1, run="_stream_table"),
    )

    def table_way_facts(self, db, via, quick=False, known=None, sizes=None):
        """What `TABLE_WAYS`' needs read of one table: its size, whether
        the streaming copy is installed, whether the target lets it be
        emptied, whether its columns pass in COPY's binary form."""
        from .. import movers
        from ..decide import Facts

        def ask(fn, unasked):
            got = movers._within(fn) if quick else fn()
            return unasked if got is None else got

        def facts_for(table, fact):
            sch, tbl = self._split(table)
            qt = f'{_ident(sch)}.{_ident(tbl)}'
            return Facts(
                {"batch": via in self.TABLE_BATCH, "via": via,
                 **(known or {}), "bytes": (fact or {}).get("bytes"),
                 "large": ((fact or {}).get("bytes") or 0)
                 >= self.STREAM_FROM},
                streaming=lambda: bool(movers.pgcopydb_runner()),
                truncatable=lambda: ask(lambda: self._truncatable(db, qt),
                                        None),
                binary=lambda: ask(lambda: self._binary_unfit(
                    db, sch, tbl, self._copy_cols(db, sch, tbl) or []),
                    "whether its columns pass in COPY's binary form could"
                    " not be asked yet"))
        return facts_for

    def _stream_table(self, job, way):
        """The whole table through the streaming copy
        (`movers.pgcopydb_table`) under a snapshot held here, then both
        servers' digests of the whole table - the source's under that same
        snapshot. Where the copy fails it is given up on for the run; where
        the digests differ, for the table - and the next way carries it
        (`tableway.carry`), each of its ranges proved."""
        from .. import movers
        from ..tableway import Failed
        from . import pg_pass
        said = way.rung.short or way.said
        db, key = job.db, job.key
        snap = pg_pass.Snapshot(self, db)
        try:
            export = snap.lease()
            ctx = pg_pass.TableCopy(self, db, key, job.qt, job.cols or [], "",
                                    1)
            try:
                movers.pgcopydb_table(
                    self.hop, db, key, export.id,
                    max(1, int(getattr(self.hop, "workers", 1) or 1)),
                    job.fact.get("bytes"), job.log)
                same = (ctx.same(export, "") if (self.hop.options or {}).get(
                    "verify_batches", True) else True)
            finally:
                snap.release(export)
                ctx.close()
        except Exception as e:  # noqa: BLE001 - said, and the next way goes
            raise Failed(
                f"{said} did not finish ("
                f"{str(e).strip().splitlines()[-1][:160] if str(e) else e})",
                everywhere=True) from e
        finally:
            snap.close()
        if not same:
            raise Failed(f"{said} left the target holding other rows than"
                         " the source's snapshot")
        job.st["done"] = True
        job.ck.save()
        from .. import decide
        decide.record(self.hop, {"work": key, "way": said}, say=job.log,
                      work=key, way=said, reason="copied whole")
        return True

    def passes_through(self, db, table):
        """Whether the table copier's ranges of `table` pass server to
        server (`COPY_WAYS`' first rungs, `pg_pass`) - the same columns of
        the same types with a binary form on both sides - for the resource
        model (`migkit.model`): no row is tallied here then, and each
        server digests every range. Where the target has no such table
        yet, the source's own columns say: the move makes it with them."""
        sch, tbl = self._split(table)
        cols = self._copy_cols(db, sch, tbl) or []
        why = self._binary_unfit(db, sch, tbl, cols)
        if not why:
            return True
        if not why.startswith("the target has no column"):
            return False
        try:
            there = self._binary_columns("dst", db, sch, tbl)
            src = self._binary_columns("src", db, sch, tbl)
        except Exception:  # noqa: BLE001 - not known: the pipe, then
            return False
        return not there and all(ok for c, (_, ok) in src.items()
                                 if c in cols)

    def _truncatable(self, db, qt):
        """Whether the target lets `qt` be emptied by TRUNCATE: no foreign
        key of any table points at it, which TRUNCATE refuses whatever
        role the session loads as."""
        return self._psql("dst", db, "select count(*) from pg_constraint"
                                     " where contype = 'f' and confrelid ="
                                     f" {_literal(qt)}::regclass"
                          ).strip() == "0"

    def _copy_ways(self, db, key, qt, cols, log):
        """The table's ways (`pg_pass.Ways`): each range climbs `COPY_WAYS`
        on this table's own measurements, so the pass-through is timed
        against the pipe on the table itself and the cheaper per row taken
        from then on; said once a table - the pipe alone where the move's
        joint plan puts the table's rows through migkit
        (`tableway.planned_through`). None where the source's columns
        could not be read: the pipe, as it always was."""
        if not cols:
            return None
        from .. import ranges, tableway
        from ..decide import Facts
        from . import pg_pass
        sch, tbl = self._split(key)
        ctx = pg_pass.TableCopy(self, db, key, qt, cols,
                                self._load_options(db, qt),
                                ranges.active.workers)
        facts = Facts(
            binary=lambda: self._binary_unfit(db, sch, tbl, cols),
            dst_version=lambda: self._server_version("dst", db))
        ways = self.COPY_WAYS
        if tableway.planned_through(self.hop, db, key):
            ways = tuple(r for r in ways if r.name == "pipe") or ways
        return pg_pass.Ways(self, db, key, ways, facts, ctx, log)

    #: how a range the copier wrote is proved (`_copy_checked`)
    VERIFY_WAYS = (Rung("digest", "digesting the range on both servers"),
                   Rung("read back", "reading the range back from the"
                                     " target"))

    def _verify_way(self):
        """"digest" or "read back": each tried once, then the one that
        proved a row in less time - a climb of `VERIFY_WAYS` on this run's
        own costs."""
        from ..decide import Costs, climb
        cost = self.__dict__.setdefault("_verify_cost", Costs())
        return climb("a range", self.VERIFY_WAYS, costs=cost).path

    def _verify_took(self, way, seconds, rows):
        from ..decide import Costs
        self.__dict__.setdefault("_verify_cost", Costs()).saw(way, rows,
                                                              seconds)

    PAYLOAD_SQL = "select repeat(md5(random()::text), {n})"

    def _large_values(self, db):
        """(level, scope, item, detail) sizing what a database holds out of
        its rows' pages: large objects, which only a path that carries them
        moves at all, and values stored out of line (TOAST), which every
        path moves value by value. Counted before, never sized: how long a
        move of them takes was nobody's number."""
        from ..wording import human_bytes
        got = self._psql("src", db, """
            select coalesce(pg_total_relation_size('pg_largeobject'), 0)
                || '|' || (select count(*) from pg_largeobject_metadata)
                || '|' || coalesce((select sum(pg_total_relation_size(
                                        c.reltoastrelid))
                                    from pg_class c
                                    join pg_namespace n
                                      on n.oid = c.relnamespace
                                   where c.reltoastrelid <> 0
                                     and c.relkind in ('r', 'p', 'm')
                                     and n.nspname not in ('pg_catalog',
                                         'information_schema')
                                     and n.nspname not like 'pg\\_%'), 0)
                || '|' || pg_database_size(current_database())""").strip()
        lo, n, toast, whole = (int(x) for x in got.split("|"))
        big = lo + toast
        return ("warn" if whole and big > whole // 2 else "pass", db,
                "values stored outside the rows sized",
                f"{n:,} large objects in {human_bytes(lo)}, and"
                f" {human_bytes(toast)} of values stored out of line - of"
                f" {human_bytes(whole)} in all"
                + (": most of it, so the move's time is the large values'"
                   if whole and big > whole // 2 else ""))

    CAPACITY_SQL = (
        "select current_setting('max_connections')::int"
        " - current_setting('superuser_reserved_connections')::int"
        " - coalesce(nullif(current_setting('reserved_connections', true),"
        " '')::int, 0)"
        " - (select count(*) from pg_stat_activity"
        "    where backend_type = 'client backend')"
        " || '|' || (select count(*) from pg_stat_activity"
        "    where state = 'active' and backend_type = 'client backend'"
        "    and pid <> pg_backend_pid())"
        " || '|' || current_setting('max_worker_processes')"
        " || '|' || pg_is_in_recovery()::int")

    #: the load a side carries, as counters `machine.observe` turns into
    #: rates: transactions, block reads and writes (`pg_stat_io` from 16,
    #: which counts every backend's; before it the databases' reads and
    #: the buffers the writers flushed), sessions on a CPU now - active and
    #: waiting on nothing - connections and their limit, the shared cache
    #: and how often a read found its block there
    LOAD_SQL = (
        "select (select sum(xact_commit + xact_rollback) from"
        " pg_stat_database)"
        " || '|' || {reads} || '|' || {writes}"
        " || '|' || (select count(*) from pg_stat_activity where state ="
        " 'active' and backend_type = 'client backend' and wait_event is"
        " null and pid <> pg_backend_pid())"
        " || '|' || (select count(*) from pg_stat_activity where"
        " backend_type = 'client backend')"
        " || '|' || current_setting('max_connections')"
        " || '|' || pg_size_bytes(current_setting('shared_buffers'))"
        " || '|' || coalesce((select sum(blks_hit)::float8 / nullif("
        "sum(blks_hit + blks_read), 0) from pg_stat_database)::text, '')")
    LOAD_IO = {
        True: ("(select coalesce(sum(reads), 0) from pg_stat_io)",
               "(select coalesce(sum(writes), 0) + coalesce(sum(extends), 0)"
               " from pg_stat_io)"),
        False: ("(select sum(blks_read) from pg_stat_database)",
                "(select buffers_checkpoint + buffers_clean + buffers_backend"
                " from pg_stat_bgwriter)")}

    def load_sample(self, side, db):
        """One reading of the side's load counters (`LOAD_SQL`); the share
        of memory its cache is, as a managed service sets it by default -
        shared buffers a quarter of the class's memory."""
        reads, writes = self.LOAD_IO[
            (self._server_version(side, db) or 0) >= 160000]
        got = self._psql(side, db, self.LOAD_SQL.format(
            reads=reads, writes=writes)).strip().split("|")
        xacts, r, w, running, conns, most, cache, hit = (got + [""] * 8)[:8]
        num = (lambda v: float(v) if v not in ("", None) else None)
        return {"xacts": num(xacts), "reads": num(r), "writes": num(w),
                "running": int(running or 0), "connections": int(conns or 0),
                "max_connections": int(most or 0),
                "cache_bytes": int(cache or 0), "cache_share": 0.25,
                "hit": num(hit)}

    def capacity(self, side, db):
        """PostgreSQL says nothing of its CPUs in SQL. Where the server's
        own files may be read (a superuser on a server it runs itself) they
        are counted from `/proc/cpuinfo`; a managed server's
        `max_worker_processes` is set from them - RDS and Aurora use
        GREATEST(2 x vCPU, 8) - so above 8 it gives them; otherwise they go
        unread."""
        got = self._psql(side, db, self.CAPACITY_SQL).strip()
        free, running, workers, standby = got.split("|")
        out = {"free_connections": max(0, int(free)),
               "running": int(running), "replica": standby == "1"}
        cpus = None
        try:
            n = self._psql(side, db,
                           "select count(*) from regexp_matches("
                           "pg_read_file('/proc/cpuinfo'), '^processor',"
                           " 'gn')").strip()
            cpus = int(n) or None
        except Exception:  # noqa: BLE001 - not allowed to read it here
            cpus = None
        if cpus is None and int(workers) > 8:
            cpus = int(workers) // 2
        if cpus:
            out["cpus"] = cpus
        return out

    # --- what the resource model measures (`migkit.model`) ----------------

    #: a worker of the table copier: two client programs of about 10 MB
    #: each and this process's relay of 64 KB pieces; the program reading
    #: holds a row whole as the server sends it - twice over, the message
    #: as it arrives and the row it hands on - and the row as carried is
    #: its text, a byte value twice its size in hex (`row_sizes` counts it
    #: so). Counted from the code, not yet measured: `CAP_CALIBRATE=1` on
    #: `tests/test_wide_rows_never_run_out_of_memory.py` measures it
    COPY_MEMORY = {"own": 48 * 2 ** 20, "per_row": 2.0,
                   "batch": 64 * 2 ** 10}
    #: server to server (`pg_pass`): the source's client holds a row in
    #: COPY's binary encoding - its bytes, half its text in hex - as it
    #: arrives and as it hands it on, and this process relays 64 KB pieces.
    #: Counted from the code, not yet measured (as `COPY_MEMORY`)
    PASS_MEMORY = {"own": 48 * 2 ** 20, "per_row": 1.0,
                   "batch": 64 * 2 ** 10}
    #: both servers' digests of a range run beside the next range's load
    CHECKS_BESIDE = True
    #: the ranges of a table are threads of this process, each relaying and
    #: tallying its rows here: one CPU's worth for all of them - the rows
    #: themselves read and written by the server's own client programs
    COPIES_IN_PROCESSES = False
    READS_HERE = False

    def _probe_conn(self, side, db, seconds=None):
        """A connection whose statements the server stops after `seconds`
        - a measurement's own, not the table copier's."""
        conn = self._connect(side, db, "probe", timeout=CONNECT_SECONDS)
        if seconds:
            with conn.cursor() as cur:
                cur.execute("set statement_timeout ="
                            f" {int(seconds * 1000)}")
            conn.commit()
        return conn

    def probe_read(self, side, db, table, part=0, parts=1,
                   budget=16 * 2 ** 20, seconds=1.5, keep=0):
        """COPY of one slice of the table's pages - the statement the table
        copier pipes - into a counter, in a read-only transaction that the
        server stops after `seconds`. A slice is pages by their row
        address, so streams side by side read different pages without a
        key; before 14, which cannot scan by address, the first rows only."""
        import psycopg2
        sch, tbl = self._split(table)
        qt = self._qualified(side, db, table)
        cols = self._copy_cols(db, sch, tbl) if side == "src" else None
        conn = self._probe_conn(side, db, seconds * 2 + 5)
        try:
            conn.set_session(readonly=True)
            cur = conn.cursor()
            cur.execute("select current_setting('server_version_num')::int,"
                        " c.relpages, pg_table_size(c.oid) from pg_class c"
                        " where c.oid = %s::regclass", (qt,))
            version, pages, size = cur.fetchone()
            pages = max(int(pages or 0), 1)
            per_page = max(int(size or 0) / pages, 8192)
            want = max(1, min(int(budget / per_page), pages // max(parts, 1)))
            if version >= 140000:
                lo = pages * part // max(parts, 1)
                where = (f"ctid >= {_tid(lo)} and ctid <"
                         f" {_tid(lo + want)}")
                select = self._copy_select(qt, cols, where)
            elif part:
                return None
            else:
                select = (self._copy_select(qt, cols)
                          + f" limit {max(1, int(budget // 64))}")
            sink = _Counted(budget, seconds, keep)
            try:
                cur.copy_expert(f"copy ({select}) to stdout", sink,
                                size=1 << 16)
            except (_Enough, psycopg2.extensions.QueryCanceledError):
                pass
            return sink.said(cols)
        finally:
            conn.close()

    def _probe_shape(self, db, table):
        """(column definitions, key columns) of a source table, for a table
        to write the probe into."""
        qt = self._qualified("src", db, table)
        rows = self.run_rule("src", db,
                             "select a.attname, format_type(a.atttypid,"
                             " a.atttypmod) from pg_attribute a"
                             f" where a.attrelid = {_literal(qt)}::regclass"
                             " and a.attnum > 0 and not a.attisdropped"
                             " and a.attgenerated = '' order by a.attnum")
        key = self._pk_cols_of(db, ".".join(self._split(table)))
        return [(str(n), str(t)) for n, t in rows], key

    #: where `probe_write` makes its table, each tried where the one
    #: before was refused: the table's own schema, then public, typed as
    #: the source's columns and then as text, and last a temporary table
    PROBE_TABLES = (
        Rung("own typed", "a table of the source's column types in the"
                          " table's own schema"),
        Rung("public typed", "a table of the source's column types in"
                             " public"),
        Rung("own text", "a table of text columns in the table's own"
                         " schema"),
        Rung("public text", "a table of text columns in public"),
        Rung("temp typed", "a temporary table of the source's column"
                           " types"),
        Rung("temp text", "a temporary table of text columns"))

    def probe_write(self, db, table, sample, seconds=1.5, stream=0,
                    apply=True):
        """The sample COPYed into a table made for it in one transaction on
        the target, read back, taken again as upserts, and the transaction
        rolled back: the table never existed for anyone else, and what was
        written went through the target's log as a copy's rows do. Where
        the user may not make a table, a temporary one - which writes no
        log, and is said to be one."""
        import io
        import time
        if not isinstance(sample, (bytes, bytearray)) or not sample:
            return None
        defs, key = self._probe_shape(db, table)
        if not defs:
            return None
        sch, _ = self._split(table)
        q = self._quote_ident
        name = f"migkit_probe_{os.getpid()}_{int(stream)}"
        cols = ", ".join(q(n) for n, _ in defs)
        conn = self._probe_conn("dst", db, seconds * 4 + 10)
        try:
            cur = conn.cursor()

            def make(choice):
                place, kind = choice.path.split()
                schema = {"own": sch, "public": "public"}.get(place)
                body = ", ".join(f"{q(n)} {t if kind == 'typed' else 'text'}"
                                 for n, t in defs)
                if key:
                    body += f", primary key ({', '.join(q(k) for k in key)})"
                where = (f"{q(schema)}.{q(name)}" if schema
                         else f"pg_temp.{q(name)}")
                cur.execute("savepoint migkit_probe")
                try:
                    cur.execute(f"create {'temp ' if not schema else ''}"
                                f"table {where} ({body})")
                except Exception:
                    # the next place starts from where this one did
                    cur.execute("rollback to savepoint migkit_probe")
                    raise
                return where, ("a temporary table, which the target writes"
                               " no log for" if not schema else
                               "a table made and rolled back in one"
                               " transaction")
            try:
                made, how = self._attempt(f"the write probe of {table}",
                                          self.PROBE_TABLES, make)[1]
            except Exception:  # noqa: BLE001 - no place took one: no probe
                return None
            began = time.monotonic()
            cur.copy_expert(f"copy {made} ({cols}) from stdin",
                            io.BytesIO(bytes(sample)), size=1 << 16)
            wrote = time.monotonic() - began
            sink = _Counted(len(sample) * 2, seconds * 4, 0)
            began = time.monotonic()
            cur.copy_expert(f"copy (select {cols} from {made}) to stdout",
                            sink, size=1 << 16)
            read = time.monotonic() - began
            applied, took = 0, None
            rest = [n for n, _ in defs if n not in key]
            if apply and key and rest:
                began = time.monotonic()
                cur.execute(f"insert into {made} ({cols}) select {cols} from"
                            f" {made} on conflict ({', '.join(map(q, key))})"
                            " do update set " + ", ".join(
                                f"{q(n)} = excluded.{q(n)}" for n in rest))
                applied, took = cur.rowcount, time.monotonic() - began
            return {"rows": sample.count(b"\n"), "bytes": len(sample),
                    "seconds": wrote, "read_seconds": read,
                    "applied": applied, "apply_seconds": took, "how": how}
        finally:
            try:
                conn.rollback()
            finally:
                conn.close()

    #: the server's own processes that write what its sessions load: a
    #: session that commits without waiting for the disk leaves the wait
    #: to these
    WRITERS = ("walwriter", "checkpointer", "background writer")

    def io_wait(self, side, db):
        """Sessions and the server's own writers waiting on storage
        (`wait_event_type` IO), and those working - the sessions active and
        the writers waiting - other than this one."""
        writers = ", ".join(f"{_literal(w)}" for w in self.WRITERS)
        got = self.run_rule(side, db,
                            "select count(*) filter (where wait_event_type ="
                            " 'IO'), count(*) filter (where (backend_type ="
                            " 'client backend' and state = 'active') or"
                            " wait_event_type = 'IO') from pg_stat_activity"
                            " where (backend_type = 'client backend' or"
                            f" backend_type in ({writers})) and pid <>"
                            " pg_backend_pid()")
        return {"io_wait": int(got[0][0]), "active": int(got[0][1])}

    def changes_made(self, side, db):
        """Rows inserted, updated and deleted in the database since its
        statistics began, and how long ago that was."""
        got = self.run_rule(side, db,
                            "select tup_inserted + tup_updated + tup_deleted,"
                            " extract(epoch from now() - coalesce(stats_reset,"
                            " pg_postmaster_start_time())) from"
                            " pg_stat_database where datname ="
                            " current_database()")
        if not got:
            return None
        return int(got[0][0]), float(got[0][1])

    #: a table whose heap is at most this big is sized row by row; a larger
    #: one from a sample of its pages. A value stored out of line is sized
    #: from its pointer (`octet_length` reads the length there), so the
    #: heap is what is scanned, not the values
    SIZE_EXACT_UP_TO = 256 * 2 ** 20

    def row_sizes(self, side, db, tables=None):
        """Each row's size as COPY carries it - text as its bytes, a byte
        value twice over in hex, anything else as stored - measured without
        reading a value stored out of line. A sampled table's rows are the
        statistics' estimate (`row_estimate`), or where those do not know,
        the sample's count scaled by its share - not 0 (audit 2026-10-01,
        DU18)."""
        estimates = self.row_estimate(side, db)
        cat = self.run_rule(side, db, """
            select n.nspname || '.' || c.relname, c.relpages::bigint * 8192,
                   string_agg(case
                       when a.atttypid = 'bytea'::regtype
                         then '2 * coalesce(octet_length(' ||
                              quote_ident(a.attname) || '), 0) + 3'
                       when a.atttypid in ('text'::regtype,
                         'varchar'::regtype, 'bpchar'::regtype)
                         then 'coalesce(octet_length(' ||
                              quote_ident(a.attname) || '), 0) + 1'
                       else 'coalesce(pg_column_size(' ||
                            quote_ident(a.attname) || '), 0) + 1' end,
                       ' + ' order by a.attnum)
              from pg_class c
              join pg_namespace n on n.oid = c.relnamespace
              join pg_attribute a on a.attrelid = c.oid and a.attnum > 0
                                  and not a.attisdropped
             where c.relkind in ('r', 'p') and n.nspname not in
                   ('pg_catalog', 'information_schema')
               and n.nspname not like 'pg\\_%'
             group by 1, 2""")
        want = None if tables is None else {
            ".".join(self._split(t)) for t in tables}
        out = {}
        for name, heap, expr in cat:
            if want is not None and name not in want:
                continue
            exact = int(heap) <= self.SIZE_EXACT_UP_TO
            pct = (100.0 if exact else
                   max(0.01, min(100.0, 100.0 * self.SIZE_EXACT_UP_TO
                                 / max(int(heap), 1))))
            qt = self._qualified(side, db, name)
            sample = "" if exact else f" tablesample system ({pct:.4f})"
            try:
                got = self.run_rule(
                    side, db,
                    f"select max(s), percentile_disc(0.99) within group"
                    f" (order by s), avg(s), count(*) from (select {expr} as"
                    f" s from {qt}{sample}) x")
            except Exception:  # noqa: BLE001 - that table is not sized
                continue
            most, p99, mean, n = got[0]
            if not n:
                continue
            est = estimates.get(name)
            out[name] = {"largest": int(most), "p99": int(p99),
                         "mean": float(mean),
                         "rows": int(n) if exact else (
                             est if est is not None
                             else int(n * 100.0 / pct)),
                         "exact": exact}
        return out

    def table_index_window(self, db, table, log):
        """Measured, a million rows into a table with three secondary
        indexes through the table copier: 7.46s with them in place, 4.42s
        as a bare copy and a build (one range); 6.52s and 3.82s in four."""
        from .. import movers
        workers = int(getattr(self.hop, "workers", 1) or 1)
        if table is None:
            return movers._IndexWindow(self.hop, db, workers, log,
                                       only_left=True)
        return movers._IndexWindow(self.hop, db, workers, log,
                                   table=self._split(table))

    def move_table(self, db, sch, tbl, chunk, ck, log):
        import contextlib
        with contextlib.ExitStack() as later:
            return self._move_table(db, sch, tbl, chunk, ck, log, later)

    def _move_table(self, db, sch, tbl, chunk, ck, log, later):
        sch = sch or "public"
        key = self.move_key(db, sch, tbl)
        qt = f'{_ident(sch)}.{_ident(tbl)}'
        st = ck.setdefault(key, {})
        # The hop's row filter, applied on both ends: only the rows it
        # selects are read, and only the rows it selects are replaced. A
        # target row outside the filter is not this copy's to delete - the
        # check reports it separately (`_outside_filter`). A partition's is
        # its table's (`_ruled`): asked with its own name, it had none, and
        # its rows were carried unfiltered
        rf = (self.hop.row_filter(db, *self._ruled(db, f"{sch}.{tbl}"))
              if hasattr(self.hop, "row_filter") else None)
        if st.get("done") and self.recheck_done(db, key, st, log, rf):
            ck.save()
            return
        ck.save()
        self.set_aside_indexes(later, self, db, f"{sch}.{tbl}", st,
                               self._rows_of("src", db, key), log)
        pk = self._int_pk(db, sch, tbl)
        st["key"] = pk
        cols = self._copy_cols(db, sch, tbl)
        from .. import tableway
        tableway.carry(self, tableway.job(
            self, db, key, key, st, ck, log, later=later, chunk=chunk, rf=rf,
            qt=qt, pk=pk, cols=cols))

    def _copy_ranges(self, job, way):
        """The table by the copier's own ranges, each climbing `COPY_WAYS`
        on the table's own measurements (`_copy_ways`) and proved as it
        lands; a table without an integer key by where its rows are stored
        (`_copy_in_spans`), or in one."""
        db, key, st, ck, log = job.db, job.key, job.st, job.ck, job.log
        qt, pk, cols, rf, chunk, later = (job.qt, job.pk, job.cols, job.rf,
                                          job.chunk, job.later)
        if not pk:
            if self._copy_in_spans(db, key, qt, cols, rf, st, ck, log):
                return True
            log(f"{key}: no single int pk, single-shot copy")
            ways = self._copy_ways(db, key, qt, cols, log)
            if ways is not None:
                later.callback(ways.close)
            self._copy_checked(db, qt, cols, rf or "",
                               f"delete from {qt} where {rf}" if rf
                               else f"truncate {qt}", key, log,
                               way=(ways.pick(lambda mark: None)
                                    if ways is not None else None))
            if ways is not None:
                ways.wait()
            st["done"] = True
            return True
        mm = self._psql("src", db,
                        f'select coalesce(min({_ident(pk)}), 0)||\'|\'||'
                        f'coalesce(max({_ident(pk)}), 0)||\'|\'||'
                        f'(min({_ident(pk)}) is not null)::int from {qt}'
                        + (f" where {rf}" if rf else ""))
        lo, hi, has = mm.split("|")
        lo, hi = int(lo), int(hi)
        from .. import ranges
        slots = ranges.active

        def edges():
            rows = ((self.table_facts("src", db).get(key) or {}).get("rows")
                    or hi - lo + 1)
            every = ranges.step(rows, chunk, slots.workers)
            if rows <= every:
                return []
            return [r[0] for r in self.run_rule("src", db, ranges.bounds_sql(
                self._quote_ident, qt, pk, every, rf))]
        todo = (ranges.plan(st, lo, hi, edges, ck.save, table=key)
                if has == "1" else [])
        ways = self._copy_ways(db, key, qt, cols, log) if todo else None
        if ways is not None:
            later.callback(ways.close)
        # the table's one range, not begun: the whole table in one
        # transaction, which the pass-through empties and loads FREEZE
        whole = (not rf and len(todo) == 1
                 and len(st.get("ranges") or ()) == 1)

        def sweep():
            # Each chunk replaces its own key range, so a target row whose
            # key lies outside the source's whole range was never in any
            # chunk and stayed - a target carrying an earlier attempt kept
            # its strays.
            outside = (f'not ({_ident(pk)} between {lo} and {hi})' if has == "1"
                       else "true")
            if rf:
                outside = f"({outside}) and ({rf})"
            gone = self._psql("dst", db, f"with d as (delete from {qt} where"
                                         f" {outside} returning 1)"
                                         " select count(*) from d")
            if gone and gone != "0":
                log(f"{key}: removed {int(gone):,} target rows the source"
                    " does not have")

        def one(rng):
            after, upto = rng
            if ways is not None:
                ways.ctx.raise_failed()
            pred = f'{_ident(pk)} > {after} and {_ident(pk)} <= {upto}'
            if rf:
                pred = f"({pred}) and ({rf})"
            what = f"{key} {pk} {after + 1:,} to {upto:,}"

            def proved():
                ranges.finished(st, after, ck.save)
                log(f"{key}: {pk} {after + 1:,} to {upto:,} copied"
                    f" ({len(st['ranges_done'])} of {len(st['ranges'])}"
                    " ranges)")

            def begin(mark):
                ranges.started(st, after, mark, ck.save)
            pre = f"delete from {qt} where {pred}"
            if ways is None:
                begin(self.snapshot_mark("src", db))
                way = None
            else:
                way = ways.pick(begin)
                if whole and way.passes and self._truncatable(db, qt):
                    # what the table held beyond the source's rows is said
                    # before TRUNCATE takes it with the rest
                    sweep()
                    pre = f"truncate {qt}"
            # the rows it moved, for the move's pace
            return self._copy_checked(db, qt, cols, pred, pre, what, log,
                                      way=way, proved=proved)
        failed = None
        try:
            slots.each(todo, one)
        except BaseException as e:  # noqa: BLE001 - raised once all is kept
            failed = e
        if ways is not None:
            # the ranges copied before a failure are kept once proved
            try:
                ways.wait()
            except BaseException as e:  # noqa: BLE001 - the first is raised
                failed = failed or e
        if failed is not None:
            raise failed
        sweep()
        st["done"] = True
        ck.save()
        return True

    def _repl_name(self):
        return "migkit_" + self.hop.name.replace("-", "_")

    def replicate_sql(self, db, copy_data=True, secret=None, copied=None,
                      slot=None, at=None, via=None):
        """The statements that set the server's own replication up and
        take it down. `slot` is a slot made earlier (before a physical
        copy, `physical`), which the subscription takes instead of making
        its own; `at` is the LSN it goes on from - its origin is advanced
        there before it is enabled, so nothing before is applied again;
        `via` is (host, port) of the source as the target reaches it."""
        # the subscription signs in as the hop's own source user, so there
        # is no replication user for `secret` to be the password of. Its
        # connection string is the one every path the target dials by is
        # built with (`legs.conninfo`): the source's TLS as far as a server
        # carries it, each value quoted as libpq reads it - written by hand
        # here it had neither (audit 2026-10-01, SEC13)
        from .. import legs
        s = self.hop.source
        name = self._repl_name()
        host, port = via or (s.host, s.port)
        conn = legs.conninfo(self.hop, db, host, port)
        streaming = self._subscription_streaming(db)
        # a stream the other way exists or will: this one must not send
        # back what that one applied, or a change goes round for ever
        origin = getattr(self, "loop_safe", False)
        note = {}
        if copied:
            # its slot is made now, and the log before now is not in it
            note = {"note": f"{db} was copied on {copied['at']}; a"
                            " subscription made now carries only what the"
                            " source changes from now on, and what it"
                            " changed since that copy is carried by"
                            " nothing - move with --mode full+cdc, which"
                            " copies under the subscription"}
        try:
            tables = self.published_tables(db)
            publish = self._publication_sql(name, tables, self._via_root_of(
                db))
        except RuntimeError as e:
            # a plan shown while the source cannot be reached: the list is
            # read from the source when the plan runs (`LISTED_WHEN_RUN`)
            tables = []
            why = str(e).strip().splitlines()[-1][:120] if str(e) else ""
            publish = (f"{self.LISTED_WHEN_RUN} {name}: the hop's tables, as"
                       " the source lists them when this runs - it could"
                       f" not be asked now ({why})")
        return {**note,
            "src": [publish],
            "dst": [f"create subscription {name} connection {_literal(conn)}"
                    f" publication {name} with (copy_data ="
                    f" {'true' if copy_data else 'false'}"
                    + (f", streaming = {streaming}" if streaming else "")
                    + (", origin = none" if origin else "")
                    + (f", create_slot = false, slot_name = {_literal(slot)}"
                       if slot else "")
                    + (", enabled = false" if at else "")
                    + ");"] + ([
                        "select pg_replication_origin_advance('pg_' || oid,"
                        # the name was folded to lower case, unquoted
                        f" {_literal(at)}) from pg_subscription where subname ="
                        f" {_literal(name.lower())} and subdbid = (select oid from"
                        " pg_database where datname = current_database());",
                        f"alter subscription {name} enable;"] if at else [])
                    + self._refresh_sql(db, name, tables),
            "drop_src": [f"drop publication if exists {name};"],
            "drop_dst": [f"drop subscription if exists {name};"],
            "status": "select subname, received_lsn, latest_end_lsn,"
                      " latest_end_time from pg_stat_subscription",
            **self._armed_sql(name, conn, publish, streaming, origin),
        }

    def _armed_sql(self, name, conn, publish, streaming, origin):
        """The same stream made ahead and left idle, then started - what a
        cutover makes before its freeze and starts at the flip (`cutover`,
        a stream back made ahead): the publication, and a slot of the
        stream's own on the side it reads, made now; the subscription made
        on its own slot, disabled; and at the start, the slot moved on to
        where its side's log is then - what was written there while it
        waited is not the stream's to carry - and the subscription
        enabled. `unarm_src` takes the slot away where the subscription's
        own drop could not."""
        slot = name.lower()
        return {
            "arm_src": [publish,
                        "select pg_create_logical_replication_slot("
                        f"{_literal(slot)}, 'pgoutput') where not exists (select 1"
                        " from pg_replication_slots where slot_name ="
                        f" {_literal(slot)});"],
            "arm_dst": [f"create subscription {name} connection {_literal(conn)}"
                        f" publication {name} with (copy_data = false"
                        + (f", streaming = {streaming}" if streaming else "")
                        + (", origin = none" if origin else "")
                        + f", create_slot = false, slot_name = {_literal(slot)},"
                        " enabled = false);"],
            "fire_src": ["select pg_replication_slot_advance("
                         f"{_literal(slot)}, pg_current_wal_lsn());"],
            "fire_dst": [f"alter subscription {name} enable;"],
            "unarm_src": ["select pg_drop_replication_slot(slot_name) from"
                          " pg_replication_slots where slot_name ="
                          f" {_literal(slot)} and not active;"],
        }

    def armed_status(self, db, sql):
        """"" where the stream made ahead (`_armed_sql`) stands ready to be
        started, or why not: its subscription on the side it applies to
        knows every table the publication lists - made with the connection
        it will stream on, so the sign-in and the publication are proven -
        and its slot is held on the side it reads."""
        name = self._repl_name().lower()
        want = self._psql("src", db, "select count(*) from"
                                     " pg_publication_tables where pubname"
                                     f" = {_literal(name)}").strip()
        have = self._psql("dst", db, "select count(*) from"
                                     " pg_subscription_rel r join"
                                     " pg_subscription s on s.oid ="
                                     f" r.srsubid where s.subname = {_literal(name)}"
                          ).strip()
        slot = self._psql("src", db, "select count(*) from"
                                     " pg_replication_slots where slot_name"
                                     f" = {_literal(name)}").strip()
        if slot != "1":
            return "its slot is not held on the side it reads"
        if have != want:
            return (f"its subscription knows {have} of the"
                    f" {want} tables its publication lists")
        return ""

    def published_tables(self, db):
        """The source's tables the hop moves, as a publication of its
        lists them: every table but the ones the hop excludes.

        A partition is the hop's where its root is (`partition_roots`):
        `include: [public.orders]` took no partition of it, whose changes
        the publication then left out, and `exclude: [public.orders]`
        published every partition. Where the source has partitions, from
        13 on the publication lists the roots and publishes each
        partition's changes under its root's name (`_via_root`), as the
        hop names the table and the target may hold it unpartitioned; a
        publication made earlier without it keeps the partitions it lists,
        and so does a server before 13, which takes no partitioned table
        into a publication."""
        listed = [t for (t,) in self._plan_rows(
            "src", db, "select schemaname||'.'||tablename from pg_tables"
            " where schemaname not in ('pg_catalog','information_schema')"
            " order by 1")]
        via = self.__dict__.setdefault("_via_root", {})
        via[db] = False
        roots = {".".join(self._split(a)): ".".join(self._split(b))
                 for a, b in self.partition_roots("src", db).items()}
        if all(roots.get(t, t) == t for t in listed):
            return [t for t in listed
                    if self.in_scope(db, t)]
        taken = [t for t in listed
                 if self.in_scope(db, roots.get(t, t))]
        if (self._server_version("src", db) or 0) >= 130000 and \
                self._publishes_via_root(db) is not False:
            via[db] = True
            return [t for t in taken if roots.get(t, t) == t]
        parents = {r for t, r in roots.items() if r != t} | {
            t for (t,) in self._plan_rows(
                "src", db, "select n.nspname||'.'||c.relname from pg_class"
                " c join pg_namespace n on n.oid = c.relnamespace where"
                " c.relkind = 'p'")}
        return [t for t in taken if t not in parents]

    def _via_root_of(self, db):
        """Whether the list `published_tables` last made for `db` is of
        roots, published under their own names."""
        return bool(self.__dict__.get("_via_root", {}).get(db))

    def _publishes_via_root(self, db):
        """Whether the hop's publication, where there is one, publishes its
        partitions' changes under their root's name - None where there is
        none yet."""
        got = self._plan_rows("src", db, "select pubviaroot from"
                                         " pg_publication where pubname ="
                                         f" {_literal(self._repl_name().lower())}")
        return bool(got[0][0]) if got else None

    def _publication_sql(self, name, tables, via_root=False):
        """The publication of exactly the hop's tables: `FOR TABLE` every
        table the hop moves, made once and set to that list again on every
        run after.

        It was `FOR ALL TABLES`, which is wrong three ways. It needs a
        superuser, where a list needs only the owner of the tables - the
        role the application's migrations already run as. It publishes
        the tables the hop excludes, which the target owns, and the
        subscription writes the source's rows into them. And it publishes
        tables the target does not have - another tool's bookkeeping
        (`leftovers.bookkeeping`), a table the hop leaves out - and
        `CREATE SUBSCRIPTION` refuses to start over a published table it
        cannot find (`relation ... does not exist`).

        `FOR TABLES IN SCHEMA` (15 and later) would take in tables made
        later, but needs a superuser as well and carries whatever the hop
        excludes in those schemas. And a table made later is not carried
        by any publication alone: a subscription takes on a new published
        table only at `REFRESH PUBLICATION`, and only once the target has
        the table, since the change stream carries no DDL. So a table a
        later run finds - on both sides by then; `check` names it until it
        is - is added here by `SET TABLE` and taken on by the refresh that
        run asks of the subscription (`_refresh_sql`): the same step either
        way, with no superuser and nothing the hop leaves out.

        `via_root` lists partitioned tables by their roots, whose
        partitions' changes are published under the root's name
        (`published_tables`); a publication made so keeps it, and the list
        set again on it is the roots' again."""
        listed = [".".join(self._quote_ident(p) for p in self._split(t))
                  for t in tables]
        made = (f"create publication {name}"
                + (f" for table {', '.join(listed)}" if listed else "")
                + (" with (publish_via_partition_root = true)" if via_root
                   else ""))
        # a second `--mode cdc` found the first one's publication and
        # stopped on `already exists`; now it sets the list again
        again = (f" else alter publication {name} set table"
                 f" {', '.join(listed)};" if listed else "")
        return ("do $$ begin if not exists (select 1 from pg_publication"
                f" where pubname = {_literal(name)}) then {made};{again} end if;"
                " end $$;")

    def _refresh_sql(self, db, name, tables):
        """`REFRESH PUBLICATION` where the hop's subscription is already on
        the target and lacks some of the hop's tables - a run after tables
        were made on both sides - so their rows are copied and their
        changes follow; or holds tables the publication no longer lists -
        a run after tables left the hop's scope - which it lets go of only
        at a refresh, and until then the stream made ahead read as knowing
        tables its publication does not (`armed_status`). Nothing on a
        first run, where the subscription is made with every table the
        publication lists."""
        if not tables:
            return []
        try:
            has = {t for (t,) in self._plan_rows(
                "dst", db, "select n.nspname||'.'||c.relname from"
                " pg_subscription_rel r join pg_subscription s on s.oid ="
                " r.srsubid join pg_class c on c.oid = r.srrelid join"
                " pg_namespace n on n.oid = c.relnamespace where s.subname"
                f" = {_literal(name)}")}
            there = self._plan_rows("dst", db, "select 1 from"
                                               " pg_subscription where"
                                               f" subname = {_literal(name)}")
        except RuntimeError:
            return []
        if not there or set(tables) == has:
            return []
        return [f"alter subscription {name} refresh publication with"
                " (copy_data = true);"]

    #: the first minor release of each branch that decodes a table added
    #: to a publication while it is written without losing its changes
    #: (2025-05-08, "Avoid data loss when DDL operations that don't take a
    #: strong lock affect tables that are being logically replicated": the
    #: walsender decoded on with a stale catalogue and filtered the added
    #: table's changes out). 12 and older were never fixed. The August
    #: set (17.6, 16.10, 15.14, 14.19, 13.22) also stopped the fix
    #: re-distributing invalidations until memory ran out - a failure that
    #: says so, not a loss
    PUBLICATION_ADD_FIXED = {17: 170005, 16: 160009, 15: 150013,
                             14: 140018, 13: 130021}

    def publication_add_risk(self, db):
        """Why taking the hop's tables into its publication here could lose
        changes, or "" where it cannot: the publication is there already,
        some of the hop's tables are not in it - a run after tables were
        added, whose `set table` takes them in while the application
        writes them (`_publication_sql`) - and the source is older than
        the minor release that fixed PostgreSQL losing such a table's
        changes (`PUBLICATION_ADD_FIXED`). A publication made with every
        table from the start is not that. Asked by the ladder of change
        ways (`movers.stream_ways`), which takes migkit's own tail
        instead: it reads every table from the slot and alters no
        publication (research, leapfrog live tail, L8)."""
        version = self._server_version("src", db)
        if not version:
            return ""
        major = version // 10000
        fixed = self.PUBLICATION_ADD_FIXED.get(major)
        if major > max(self.PUBLICATION_ADD_FIXED) or (
                fixed and version >= fixed):
            return ""
        name = self._repl_name()
        try:
            there = self._plan_rows("src", db, "select 1 from pg_publication"
                                               f" where pubname = {_literal(name)}")
            if not there:
                return ""
            has = {t for (t,) in self._plan_rows(
                "src", db, "select schemaname||'.'||tablename from"
                f" pg_publication_tables where pubname = {_literal(name)}")}
            # what the publication would be set to list, as it lists it
            tables = self.published_tables(db)
        except RuntimeError:
            return ""
        new = [t for t in tables if t not in has]
        if not new:
            return ""
        floor = (f"{fixed // 10000}.{fixed % 100}" if fixed
                 else "a supported major version")
        return (f"the publication would take in {len(new)} tables while"
                f" they are written ({', '.join(new[:3])}"
                + (" ..." if len(new) > 3 else "") + f"), and the source"
                f" runs {major}.{version % 100}: before {floor} PostgreSQL"
                " itself can lose the changes of a table added so")

    def loops_prevented(self, db):
        """Why streams both ways would send changes round for ever here,
        or "" where they cannot. A subscription told `origin = none` takes
        only what the other side's own sessions wrote - PostgreSQL 16 on
        both sides."""
        src = self._server_version("src", db)
        dst = self._server_version("dst", db)
        if not src or not dst:
            return "a side's version could not be read"
        if src < 160000 or dst < 160000:
            return ("a subscription tells its own changes from the other"
                    " side's only from PostgreSQL 16, and a change would go"
                    " round for ever")
        return ""

    def _subscription_streaming(self, db):
        """How the subscription takes a transaction too large for the
        source's decoding memory: streamed while it is still running,
        rather than written to the source's disk and sent at commit.

        Measured on 16, 3.3 MB in one transaction, the source's
        `logical_decoding_work_mem` at 64kB: without it the source spilled
        3,460,000 bytes to disk; with `streaming = parallel` it spilled
        nothing and streamed the same 3,460,000 bytes, and all 20,000 rows
        arrived. `parallel` from 16 on the target, `on` from 14; the
        source has to be 14 as well, to send it. None - the server's own
        default - where either side cannot be asked."""
        src = self._server_version("src", db)
        dst = self._server_version("dst", db)
        if not src or not dst or src < 140000 or dst < 140000:
            return None
        return "parallel" if dst >= 160000 else "on"

    def _plan_rows(self, side, db, sql):
        """The rows of one query a plan is made from, in one attempt with a
        short wait, as `_server_version` asks: a plan is shown whether or
        not a server answers, and through the retries every other read
        takes, a source and a target that did not answer held two plans
        for 303 s (measured; 20 s before the tables were listed, 33 s
        this way). A RuntimeError where it cannot be asked."""
        import psycopg2
        try:
            conn = self._connect(side, db, "probe")
        except psycopg2.Error as e:
            raise RuntimeError(str(e).strip()) from None
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
        except psycopg2.Error as e:
            raise RuntimeError(str(e).strip()) from None
        finally:
            conn.close()

    def _server_version(self, side, db):
        """`server_version_num` of one side, or None where it cannot be
        asked - read once a side (a server's version is the same in every
        database), on the database the hop calls `db`. One attempt with a
        short wait: this decides an option, and a plan should not stall a
        minute on a server that is down. Callers mapped the target's name
        themselves, or did not and asked the source's name of the target
        (audit 2026-10-01, DU3); a marker read it a table (SC4)."""
        known = self.__dict__.setdefault("_versions", {})
        if side in known:
            return known[side]
        try:
            conn = self._connect(side, db, "probe")
        except Exception:  # noqa: BLE001 - not known, and said so
            return None
        try:
            with conn.cursor() as cur:
                cur.execute("select current_setting('server_version_num')"
                            "::int")
                known[side] = cur.fetchone()[0]
                return known[side]
        except Exception:  # noqa: BLE001
            return None
        finally:
            conn.close()

    def replication_status(self, db, sql):
        got = self._psql("dst", db, sql).strip()
        return got or ("no subscription on the target: the statements ran"
                       " but nothing is replicating")

    #: `CREATE SUBSCRIPTION` opens a connection to the source while it runs,
    #: and there is no way to bound that from inside the statement. Measured
    #: on PostgreSQL 16 against a target with no route to the source:
    #:
    #:     plain conninfo, connect_timeout=10, through psql     10s
    #:     the same conninfo inside CREATE SUBSCRIPTION        134s
    #:     ... with connect_timeout=10 in it as well           134s
    #:     ... under statement_timeout=15s                      15s
    #:
    #: `connect_timeout` is enforced by libpq's *synchronous* connect path
    #: and the walreceiver does not use it, so the parameter is accepted and
    #: ignored - the worst shape a setting can have. `statement_timeout` is
    #: the one that works, and nothing is created when it fires, so a
    #: bounded attempt costs nothing but the wait it saves.
    DIALS_ACROSS = ("create subscription", "alter subscription")
    SUBSCRIBE_TIMEOUT = 45

    #: how a plan made while the source could not be listed writes its
    #: publication; the statement is made when it runs
    LISTED_WHEN_RUN = "-- create publication"

    def _dial(self, side, db, stmt, secs):
        """A statement that dials the source from the target, and carries
        its credentials: on a session of its own that asks the target not
        to log its statements where it may (a superuser's), so the
        password is not in the target's log under `log_statement = ddl`,
        and bounded by `secs` (audit 2026-10-01, SEC13)."""
        import psycopg2

        from .. import legs
        conn = connect(self._ep(side), self._d(side, db))
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                legs.unlogged(cur)
                cur.execute(f"set statement_timeout = {int(secs) * 1000}")
                cur.execute(stmt)
                return ""
        except psycopg2.Error as e:
            raise SqlFailed(e) from e
        finally:
            conn.close()

    def apply_replication_stmt(self, side, db, stmt):
        from .. import envkeys
        if stmt.startswith(self.LISTED_WHEN_RUN):
            # the list as `published_tables` makes it: the tables the
            # application names were listed, which published every
            # partition of a partitioned one under its own name
            tables = self.published_tables(db)
            stmt = self._publication_sql(self._repl_name(), tables,
                                         self._via_root_of(db))
        head = stmt.strip().lower()
        if not any(head.startswith(p) for p in self.DIALS_ACROSS):
            return self._psql(side, db, stmt)
        got = envkeys.number("MIGKIT_SUBSCRIBE_TIMEOUT")
        secs = self.SUBSCRIBE_TIMEOUT if got is None else got
        if secs <= 0:
            # a typo here would otherwise be ignored and the operator would
            # wait the default while believing they had changed it
            raise SystemExit(
                f"MIGKIT_SUBSCRIBE_TIMEOUT={secs} is not a whole number of"
                " seconds greater than zero")
        try:
            return self._dial(side, db, stmt, secs)
        except RuntimeError as e:
            if "already exists" in str(e):
                # the stream a first run started: making it again stopped
                # on this, and the status printed next says how it is doing
                return ""
            if head.startswith("alter subscription") \
                    and "does not exist" in str(e):
                raise SystemExit(
                    "the source publishes a table of the hop's that the"
                    f" target does not have ({self._worst_line(str(e))}):"
                    " the change stream carries no DDL, so the table is made"
                    " on the target first - migkit check names it, migkit"
                    " sync --kind schema --apply makes it - and this run"
                    " again")
            raise SystemExit(self._subscribe_failed(str(e), secs))

    @staticmethod
    def _worst_line(err):
        """The line that says what went wrong.

        psql's last line is usually the hint - *"Is the server running on
        that host and accepting TCP/IP connections?"* - which is the least
        informative part of the message.
        """
        lines = [ln.strip() for ln in err.splitlines() if ln.strip()]
        for want in ("failed:", "error:", "ERROR:"):
            for ln in lines:
                if want in ln:
                    return ln[:200]
        return lines[-1][:200] if lines else ""

    def _subscribe_failed(self, err, secs):
        """Say what the failure means, and what it left behind.

        Both shapes - a timeout and a refusal - are one fact: the target has
        no route to the source. What the operator hits *next* is the part
        worth spelling out. The publication on the source is created by the
        statement before this one, so it survives, and PostgreSQL has no
        `CREATE PUBLICATION ... IF NOT EXISTS` (16 answers with a syntax
        error), so a second attempt fails on the source with `publication
        "..." already exists` before it ever reaches the target again.
        """
        name = self._repl_name()
        timed_out = "statement timeout" in err.lower()
        why = (f"gave up after {secs}s" if timed_out
               else "could not reach the source")
        detail = "" if timed_out else f" ({self._worst_line(err)})"
        return (
            f"the target {why} while running CREATE SUBSCRIPTION."
            " That statement runs on the target and dials the source, so"
            " this is the route from target to source - not migkit, and not"
            f" the credentials{detail}."
            " No subscription was created."
            f" The publication {name} on the source was, by the statement"
            " before it, and PostgreSQL has no CREATE PUBLICATION IF NOT"
            " EXISTS - so clear it before retrying, or the next run stops"
            f" on `publication {_ident(name)} already exists`:"
            " migkit move <hop> --mode cdc --drop --go."
            " migkit also has a CDC path that needs no route from the"
            " target at all - it runs from here and connects out to both"
            " sides: MIGKIT_CDC=follow migkit move <hop> --mode cdc."
            " To wait longer instead, raise MIGKIT_SUBSCRIBE_TIMEOUT")

    def migration_pair(self, db):
        from ..movers import schema_diff
        if not which("atlas"):
            return None, None
        su, tu = self._schema_urls(db)
        # a mapped table's schema is the mapping's, and aligning it with the
        # source would put back the columns the hop drops
        excludes = self._schema_excludes(db)

        def diff(a, b):
            p = schema_diff(a, b, excludes)
            text = p.stdout.strip()
            if p.returncode or "Schemas are synced" in text:
                return ""
            return text
        return diff(tu, su), diff(su, tu)

    def fetch_sample_df(self, side, db, table, limit, where=None):
        """The rows `--drill` compares, read without being rewritten.

        `text=True` here would decode psql's output with universal
        newlines, which turns a `\\r\\n` inside a value into `\\n` before
        anything compares it. Measured on a pair whose only difference was
        a carriage return in one column:

            check --only data   DIFF, pk-level file names row 5
            check --drill       "Number of rows with some compared columns
                                 unequal: 0"

        Two answers from one run, and the wrong one came from the command
        whose whole job is to show what differs. The bytes psql sends are
        `b'"one\\r\\ntwo"\\n'`; the same call with `text=True` returns
        `'"one\\ntwo"\\n'`, so the CR is gone before pandas is reached. The
        MySQL engine reads through a driver and never had this.
        """
        import io as _io

        import pandas as pd
        sch, tbl = self._split(table)
        argv, env = self._client(
            side, db, "psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-c",
            f"\\copy (select * from {_ident(sch)}.{_ident(tbl)}"
            + (f" where {where}" if where else "")
            + f" limit {int(limit)}) to stdout (format csv, header)")
        p = subprocess.run(argv, capture_output=True, env=tool_env(env))
        if p.returncode:
            raise RuntimeError(p.stderr.decode("utf-8", "replace")[-200:])
        return pd.read_csv(_io.StringIO(p.stdout.decode("utf-8"), newline=""))

    def watch_sample(self, db):
        sample = {"db": db, "ts": time.time()}
        # per table on both sides: the sums are of what the hop takes
        # (`watched`), and the source's name the tables a queue or a
        # worker is still writing to
        q = ("select schemaname||'.'||relname||'|'||n_live_tup"
             " from pg_stat_user_tables"
             " where schemaname not like '\\_\\_%'")

        def live(side):
            return {l.rsplit("|", 1)[0]: int(l.rsplit("|", 1)[1] or 0)
                    for l in self._psql(side, db, q).splitlines()
                    if "|" in l}
        try:
            src, dst = live("src"), live("dst")
        except (RuntimeError, ValueError) as e:
            sample["error"] = str(e).splitlines()[-1]
            return sample
        src, dst, narrowed = self.watched(db, src, dst)
        sample["src_rows"] = sum(src.values())
        sample["dst_rows"] = sum(dst.values())
        sample["src_tables"] = src
        if narrowed:
            sample["narrowed"] = narrowed
        try:
            slots = self._psql("src", db,
                               "select slot_name||' active='||active||' lag='||"
                               "coalesce(pg_size_pretty(pg_wal_lsn_diff("
                               "pg_current_wal_lsn(), confirmed_flush_lsn)),'?')"
                               " from pg_replication_slots")
            sample["replication_slots"] = slots.splitlines() if slots else []
            conns = self._psql("src", db,
                               "select application_name||' '||client_addr||' '||state"
                               " from pg_stat_replication")
            sample["replication_conns"] = conns.splitlines() if conns else []
        except RuntimeError:
            pass
        return sample
