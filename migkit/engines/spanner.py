"""Google Cloud Spanner as a side of a pair (backlog P0-CLOUDS), both of its
dialects - GoogleSQL and PostgreSQL - through its Python SDK.

A hop's database is a Spanner database; the side's host is the project,
its `instance` option the instance (`spanner://project/instance/db`, and
`emulator: host:port` for the emulator). Which dialect a database speaks
is read from it, never asked.

Every table has a primary key, which a read resumes by. Rows are read
through the SDK's streaming queries - at low priority where the hop says
`priority: low`, so a copy yields to the application - and a table's
digest is worked out by the server in GoogleSQL where every column is
rendered in it and the rendering is proved on the table's first rows
(`warehouse_sql`, the dialect BigQuery shares); otherwise it is folded
here over the table's partitions read side by side (a batch snapshot's
partitioned query, on Data Boost's own compute where the hop says
`data_boost: true`, which leaves the instance's load alone). Rows are
written as mutations - insert-or-update, which a batch written again lands
on - in commits held under the service's limit on mutations. A child table
interleaved in its parent, or holding a foreign key, takes rows only after
its parent's: a commit refused for a parent row not there yet is tried
again while the parent's copy runs (`PARENT_WAIT`).

A generated column (a stored expression, a search index's TOKENLIST) is
not read: it is the server's own function of the others, compared through
them. A same-engine move into an empty database is given the source's own
definition first (interleaving, indexes, foreign keys, sequences, row
deletion policies), as `GetDatabaseDdl` gives it, change streams left
out; a target made from another engine is made column by column.

Its changes are its change stream's, read partition by partition by the
change-topic reader (`changetopic`): each partition is a lane, its
children found as it ends, every partition's heartbeats its marks, so the
changes are handed on in commit-timestamp order across all of them - one
clock, TrueTime's. The stream says which keys changed and when; each is
read back from Spanner as it stood at its batch's fence (a stale read at
that timestamp), so a value arrives exactly as a copy reads it, and a key
the stream says was written but is not there stops the tail. Past the
database's version retention (an hour unless set) the rows are read as
they are now, which later changes level again. A copy's reads and the
stream's commits are on the same clock, so a change the copy already saw
is known exactly (`mark_covers`).
"""
import base64
import calendar
import concurrent.futures
import datetime
import json
import threading
import time
from decimal import Decimal

from .base import Engine, NeutralCopier, Result
from .warehouse_sql import DigestsInSQL

#: mutations one commit may hold, well under the service's 80,000 (each
#: column of a row is one, and so is each index entry it writes)
MUTATIONS = 20_000
#: keys a read by key asks for at once
KEYS = 1000
#: seconds a commit refused for a missing parent row is tried again
#: while the parent's own copy runs
PARENT_WAIT = 600
#: heartbeats a partition's query is asked for, in milliseconds
HEARTBEAT_MS = 1000
#: the longest window one partition query reads before it returns
WINDOW_S = 60
SYSTEM_SCHEMAS = ("INFORMATION_SCHEMA", "SPANNER_SYS", "information_schema",
                  "spanner_sys", "pg_catalog")
#: held while a target database is made and given its definition: tables
#: copied side by side each ask for it first
_MAKING = threading.Lock()
DEFAULT_SCHEMAS = ("", "public")


def ns_of(value):
    """A timestamp as nanoseconds since the epoch: a DatetimeWithNanoseconds
    whole, RFC 3339 text, or a migkit value with its `nanosecond`."""
    from google.api_core.datetime_helpers import DatetimeWithNanoseconds
    if isinstance(value, str):
        value = DatetimeWithNanoseconds.from_rfc3339(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    whole = calendar.timegm(value.utctimetuple()) * 10 ** 9
    if isinstance(value, DatetimeWithNanoseconds):
        return whole + int(value.nanosecond)
    return whole + value.microsecond * 1000 + int(
        getattr(value, "nanosecond", 0) or 0)


def at_ns(ns):
    """Nanoseconds since the epoch as the SDK's timestamp, every digit kept
    (proto-plus writes a DatetimeWithNanoseconds's nanoseconds whole)."""
    from google.api_core.datetime_helpers import DatetimeWithNanoseconds
    secs, frac = divmod(int(ns), 10 ** 9)
    base = datetime.datetime.fromtimestamp(secs, datetime.timezone.utc)
    return DatetimeWithNanoseconds(base.year, base.month, base.day, base.hour,
                                   base.minute, base.second,
                                   nanosecond=frac,
                                   tzinfo=datetime.timezone.utc)


def seconds_of(text):
    """Seconds of a duration as Spanner writes one - `1d`, `36h`, `90m`,
    `3600s` - or None where it is not one."""
    import re
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([dhms])\s*", str(text or ""))
    if not m:
        return None
    return float(m.group(1)) * {"d": 86400, "h": 3600, "m": 60,
                                "s": 1}[m.group(2)]


def _plain_json(value):
    """The SDK's JsonObject (or a JSON text) as the Python value it holds,
    numbers with a point as decimals."""
    if value is None:
        return None
    if hasattr(value, "serialize"):
        text = value.serialize()
        return None if text is None else json.loads(text, parse_float=Decimal)
    if isinstance(value, str):
        return json.loads(value, parse_float=Decimal)
    return value


def normal_type(spanner_type):
    """A column's type as migkit reads it: `ARRAY<T>` as `T[]`, a PROTO
    or ENUM without its message's name."""
    t = str(spanner_type or "").strip()
    up = t.upper()
    if up.startswith("ARRAY<") and t.endswith(">"):
        return normal_type(t[6:-1].split("(vector_length")[0].strip()) + "[]"
    if up.startswith("PROTO<"):
        return "PROTO"
    if up.startswith("ENUM<"):
        return "ENUM"
    return t


def _kind(declared):
    """(kind, element kind or None) of a declared type, in one vocabulary
    for both dialects."""
    t = str(declared or "").strip().lower()
    if t.endswith("[]"):
        return "array", _kind(t[:-2])[0]
    base = t.split("(")[0].strip()
    return {"int64": "int", "bigint": "int", "int8": "int", "enum": "int",
            "oid": "int",
            "float64": "float", "double precision": "float",
            "float8": "float", "float32": "float32", "real": "float32",
            "float4": "float32",
            "numeric": "numeric", "decimal": "numeric",
            "string": "string", "character varying": "string",
            "varchar": "string", "text": "string",
            "bytes": "bytes", "bytea": "bytes", "proto": "bytes",
            "bool": "bool", "boolean": "bool", "date": "date",
            "timestamp": "timestamp", "timestamp with time zone": "timestamp",
            "timestamptz": "timestamp",
            "spanner.commit_timestamp": "timestamp",
            "json": "json", "jsonb": "json", "interval": "interval",
            "uuid": "uuid"}.get(base, "string"), None


class _Folded:
    """The digest a proved server rendering falls back to (`DigestsInSQL`
    calls on to this one)."""

    def neutral_digest(self, side, db, table, columns, where=None):
        return self._fold_digest(side, db, table, columns, where)


class SpannerEngine(DigestsInSQL, _Folded, NeutralCopier, Engine):
    checks = ("schema", "counts", "autoinc", "data")
    CANON_ENGINE = "spanner"
    FOLDED_BECAUSE = ("renders its rows in GoogleSQL where proved, and"
                      " folds them here otherwise")
    OVER_NETWORK = True
    RESUMES_BY_KEY = True
    CHANGE_POINT_READS_ONLY = True
    READS_AHEAD = False
    #: GoogleSQL is proved on each table before a digest is trusted to it
    DIGEST_DIALECT = "spanner"

    # ---- where it is ------------------------------------------------------

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    def _client(self, side):
        held = self.__dict__.setdefault("_clients", {})
        if side not in held:
            from google.cloud import spanner

            from . import gcp
            ep = self._ep(side)
            emulator = ep.options.get("emulator")
            kw = {}
            if emulator:
                # anonymous credentials and the emulator's address: the SDK
                # speaks to it in plain text, no environment needed
                from google.auth.credentials import AnonymousCredentials
                kw = {"credentials": AnonymousCredentials(),
                      "client_options": {"api_endpoint": str(emulator)}}
            elif gcp.credentials(ep) is not None:
                kw["credentials"] = gcp.credentials(ep)
            held[side] = spanner.Client(project=gcp.project(ep), **kw)
        return held[side]

    def _instance(self, side):
        ep = self._ep(side)
        name = ep.options.get("instance")
        if not name:
            raise SystemExit("a Spanner side names its instance: options:"
                             " {instance: ...}, or spanner://project/"
                             "instance/database")
        return self._client(side).instance(str(name))

    def _database(self, side, db):
        held = self.__dict__.setdefault("_databases", {})
        at = (side, self._d(side, db))
        if at not in held:
            held[at] = self._instance(side).database(at[1])
        return held[at]

    def _pg(self, side, db):
        """Whether the database speaks the PostgreSQL dialect."""
        held = self.__dict__.setdefault("_dialects", {})
        at = (side, self._d(side, db))
        if at not in held:
            from google.cloud.spanner_admin_database_v1 import DatabaseDialect
            database = self._database(side, db)
            database.reload()
            held[at] = database.database_dialect == DatabaseDialect.POSTGRESQL
        return held[at]

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        return sorted(d.name.rsplit("/", 1)[-1]
                      for d in self._instance("src").list_databases())

    def _request(self, side):
        """A read's options: low priority where the side says so."""
        from google.cloud.spanner_v1 import RequestOptions
        if str(self._ep(side).options.get("priority") or "").lower() == "low":
            return RequestOptions(
                priority=RequestOptions.Priority.PRIORITY_LOW)
        return None

    # ---- SQL in either dialect --------------------------------------------

    def _qd(self, side, db, name):
        if self._pg(side, db):
            return '"' + str(name).replace('"', '""') + '"'
        return "`" + str(name).replace("`", "\\`") + "`"

    def _q(self, name):
        """GoogleSQL's quoting, which the proved digest is written in."""
        return "`" + str(name).replace("`", "\\`") + "`"

    def _qualified(self, side, db, table):
        sch, _, tbl = str(table).rpartition(".")
        return ".".join(self._qd(side, db, p) for p in (sch, tbl) if p)

    def _mark(self, side, db, i):
        return f"${i}" if self._pg(side, db) else f"@p{i}"

    def _param_type(self, side, db, declared):
        from google.cloud.spanner_v1 import param_types as pt
        kind, element = _kind(declared)
        pg = self._pg(side, db)

        def one(k):
            return {"int": pt.INT64, "float": pt.FLOAT64,
                    "float32": pt.FLOAT32,
                    "numeric": pt.PG_NUMERIC if pg else pt.NUMERIC,
                    "string": pt.STRING, "bytes": pt.BYTES, "bool": pt.BOOL,
                    "date": pt.DATE, "timestamp": pt.TIMESTAMP,
                    "json": pt.PG_JSONB if pg else pt.JSON,
                    "interval": pt.INTERVAL, "uuid": pt.UUID}[k]
        return pt.Array(one(element)) if kind == "array" else one(kind)

    def _execute(self, side, db, sql, params=None, types=None, at=None):
        """[[value]] of a query, each value as the neutral contract reads
        it; `at` a read timestamp in nanoseconds."""
        database = self._database(side, db)
        snap = (database.snapshot(read_timestamp=at_ns(at)) if at is not None
                else database.snapshot())
        with snap as s:
            got = s.execute_sql(sql, params=params or None,
                                param_types=types or None,
                                request_options=self._request(side))
            rows = [list(r) for r in got]
            fields = list(got.fields) if rows else []
        return [[self._value(f.type_, v) for f, v in zip(fields, r)]
                for r in rows]

    def _rows(self, side, db, sql, args=None):
        """What the proved digest reads (`DigestsInSQL`)."""
        return self._execute(side, db, sql)

    def _select(self, cols, source, cond, order, limit):
        return (f"select {cols} from {source}{cond}{order}"
                + (f" limit {int(limit)}" if limit else ""))

    def _wide_times(self, side, db, table):
        # a TIMESTAMP comes whole, with its nanoseconds
        return {}

    def _whole_times(self, side, db, table, names, rows):
        return rows

    # ---- values ------------------------------------------------------------

    def _value(self, type_pb, v):
        """A value as the SDK gives it, as the neutral contract reads it:
        bytes decoded from the base64 the SDK leaves them in, a timestamp
        with its nanoseconds as migkit carries them, JSON as its value, an
        interval as its three parts, a struct by its fields' names."""
        from google.cloud.spanner_v1 import TypeCode
        if v is None:
            return None
        code = type_pb.code
        if code == TypeCode.ARRAY:
            return [self._value(type_pb.array_element_type, x) for x in v]
        if code == TypeCode.STRUCT:
            return {f.name: self._value(f.type_, x)
                    for f, x in zip(type_pb.struct_type.fields, v)}
        if code == TypeCode.BYTES:
            return base64.b64decode(v)
        if code == TypeCode.TIMESTAMP:
            return _nanotime(v)
        if code == TypeCode.JSON:
            return _plain_json(v)
        if code == TypeCode.INTERVAL:
            from .. import intervals
            return intervals.Interval(v.months, v.days, v.nanos)
        if code == TypeCode.UUID:
            return str(v)
        return v

    def _to_spanner(self, declared, value):
        """A value on its way into a column of this declared type, as the
        SDK sends one."""
        from .. import intervals
        if value is None:
            return None
        kind, element = _kind(declared)
        if kind == "array":
            if isinstance(value, str):
                value = json.loads(value, parse_float=Decimal)
            return [self._to_spanner(element or "string", x) for x in value]
        if kind == "bytes":
            if isinstance(value, str):
                value = value.encode("utf-8")
            return base64.b64encode(bytes(value))
        if kind == "timestamp":
            if isinstance(value, str):
                return value
            if isinstance(value, datetime.datetime):
                return at_ns(ns_of(value))
            return value
        if kind == "numeric":
            return format(value if isinstance(value, Decimal)
                          else Decimal(str(value)), "f")
        if kind == "json":
            from google.cloud.spanner_v1 import JsonObject

            from .. import canon
            if isinstance(value, str):
                value = json.loads(value, parse_float=Decimal)
            return JsonObject.from_str(canon.json_text(value))
        if kind == "interval":
            from google.cloud.spanner_v1.data_types import Interval
            got = intervals.of(value)
            return Interval(months=got.months, days=got.days,
                            nanos=got.nanos)
        if kind == "uuid":
            import uuid
            return value if isinstance(value, uuid.UUID) \
                else uuid.UUID(str(value))
        if kind == "int":
            return int(value)
        if kind in ("float", "float32"):
            return float(value)
        if kind == "bool":
            return bool(value)
        if kind == "date" and isinstance(value, datetime.datetime):
            return value.date()
        if kind == "string" and not isinstance(value, str):
            from .. import canon
            if isinstance(value, (dict, list)):
                return canon.json_text(value)
            return str(value)
        return value

    # ---- what a table is ---------------------------------------------------

    def _table_rows(self, side, db):
        return self._execute(
            side, db, "select * from information_schema.tables where"
                      " table_type = 'BASE TABLE'")

    def _tables_named(self, side, db):
        """{table: its INFORMATION_SCHEMA.TABLES row, by column name}."""
        database = self._database(side, db)
        with database.snapshot() as s:
            got = s.execute_sql("select * from information_schema.tables"
                                " where table_type = 'BASE TABLE'")
            rows = [list(r) for r in got]
            names = [f.name.lower() for f in got.fields] if rows else []
        out = {}
        for r in rows:
            row = dict(zip(names, r))
            sch = str(row.get("table_schema") or "")
            if sch in SYSTEM_SCHEMAS:
                continue
            name = row["table_name"] if sch in DEFAULT_SCHEMAS \
                else f"{sch}.{row['table_name']}"
            out[name] = row
        return out

    def neutral_tables(self, side, db):
        return sorted(t for t in self._tables_named(side, db)
                      if not self.hop.excluded(db, *str(t).split(".", 1)))

    def target_missing(self, db):
        return not self._database("dst", db).exists()

    def _split(self, side, db, table):
        sch, _, tbl = str(table).rpartition(".")
        return (sch or ("public" if self._pg(side, db) else "")), tbl

    def _columns_of(self, side, db, table):
        """[(name, the type as the catalogue writes it, generated)] in the
        table's order."""
        held = self.__dict__.setdefault("_columns_of", {})
        at = (side, self._d(side, db), str(table))
        if at not in held:
            sch, tbl = self._split(side, db, table)
            m1, m2 = self._mark(side, db, 1), self._mark(side, db, 2)
            database = self._database(side, db)
            with database.snapshot() as s:
                got = s.execute_sql(
                    "select * from information_schema.columns where"
                    f" table_schema = {m1} and table_name = {m2}",
                    params={"p1": sch, "p2": tbl})
                rows = [list(r) for r in got]
                names = [f.name.lower() for f in got.fields] if rows else []
            cols = sorted((dict(zip(names, r)) for r in rows),
                          key=lambda c: int(c.get("ordinal_position") or 0))
            held[at] = [(c["column_name"], str(c.get("spanner_type") or ""),
                         str(c.get("is_generated") or "NEVER").upper()
                         != "NEVER"
                         or str(c.get("spanner_type") or "").upper()
                         .startswith("TOKENLIST")) for c in cols]
        return held[at]

    SHAPE_CACHES = ("_column_rules_of", "_ordered_of", "_columns_of",
                    "_keys_of")

    def neutral_columns(self, side, db, table):
        return [(n, normal_type(t)) for n, t, generated
                in self._columns_of(side, db, table) if not generated]

    def neutral_key(self, side, db, table):
        held = self.__dict__.setdefault("_keys_of", {})
        at = (side, self._d(side, db), str(table))
        if at not in held:
            sch, tbl = self._split(side, db, table)
            m1, m2 = self._mark(side, db, 1), self._mark(side, db, 2)
            database = self._database(side, db)
            with database.snapshot() as s:
                got = [list(r) for r in s.execute_sql(
                    "select column_name, ordinal_position from"
                    " information_schema.index_columns where table_schema ="
                    f" {m1} and table_name = {m2} and index_type ="
                    " 'PRIMARY_KEY'", params={"p1": sch, "p2": tbl})]
            held[at] = [c for c, _ in sorted(got, key=lambda r: int(r[1]))]
        return held[at]

    def _classes(self, side, db, table):
        from .. import canon
        return {n: canon.type_class("spanner", t)
                for n, t in self.neutral_columns(side, db, table)}

    def neutral_column_rules(self, side, db, table):
        sch, tbl = self._split(side, db, table)
        m1, m2 = self._mark(side, db, 1), self._mark(side, db, 2)
        database = self._database(side, db)
        with database.snapshot() as s:
            got = s.execute_sql("select * from information_schema.columns"
                                f" where table_schema = {m1} and table_name"
                                f" = {m2}", params={"p1": sch, "p2": tbl})
            rows = [list(r) for r in got]
            names = [f.name.lower() for f in got.fields] if rows else []
        out = {}
        for r in rows:
            c = dict(zip(names, r))
            out[c["column_name"]] = {
                "null": str(c.get("is_nullable") or "YES").upper() == "YES",
                "default": c.get("column_default"),
                "identity": str(c.get("is_identity") or "NO").upper()
                == "YES"}
        return out

    def neutral_indexes(self, side, db, table):
        sch, tbl = self._split(side, db, table)
        m1, m2 = self._mark(side, db, 1), self._mark(side, db, 2)
        got = self._execute(
            side, db, "select i.index_name, i.is_unique, c.column_name,"
                      " c.ordinal_position from information_schema.indexes i"
                      " join information_schema.index_columns c on"
                      " c.table_schema = i.table_schema and c.table_name ="
                      " i.table_name and c.index_name = i.index_name where"
                      f" i.table_schema = {m1} and i.table_name = {m2} and"
                      " i.index_type = 'INDEX'",
            {"p1": sch, "p2": tbl})
        out = {}
        for name, unique, col, pos in got:
            ix = out.setdefault(name, [bool(unique), []])
            if pos is not None:
                # a STORING column has no place in the index's key
                ix[1].append((int(pos), col))
        return [(n, u, [c for _, c in sorted(cols)], True)
                for n, (u, cols) in sorted(out.items())]

    def neutral_create_index_sql(self, side, db, table, name, unique,
                                 columns):
        return (f"create {'unique ' if unique else ''}index"
                f" {self._qd(side, db, name)} on"
                f" {self._qualified(side, db, table)}"
                f" ({', '.join(self._qd(side, db, c) for c in columns)})")

    def execute_ddl(self, side, db, sql):
        self._target_only(side, "change a schema")
        self._database(side, db).update_ddl([sql]).result(600)
        self.forget_shapes()

    def _interleaved(self, side, db):
        """{child: parent} of the tables interleaved in another."""
        out = {}
        for t, row in self._tables_named(side, db).items():
            parent = row.get("parent_table_name")
            if parent:
                sch = str(row.get("table_schema") or "")
                out[t] = parent if sch in DEFAULT_SCHEMAS \
                    else f"{sch}.{parent}"
        return out

    def foreign_keys(self, side, db):
        """Each interleaved table as a child of its parent by the parent's
        key, and each foreign key as the catalogue declares it."""
        out = []
        for child, parent in self._interleaved(side, db).items():
            pk = self.neutral_key(side, db, parent)
            out.append((child, list(pk), parent, list(pk)))
        got = self._execute(
            side, db, "select k.table_schema, k.table_name, k.column_name,"
                      " k.ordinal_position, p.table_schema, p.table_name,"
                      " p.column_name, k.constraint_name from"
                      " information_schema.referential_constraints r join"
                      " information_schema.key_column_usage k on"
                      " k.constraint_schema = r.constraint_schema and"
                      " k.constraint_name = r.constraint_name join"
                      " information_schema.key_column_usage p on"
                      " p.constraint_schema = r.unique_constraint_schema and"
                      " p.constraint_name = r.unique_constraint_name and"
                      " p.ordinal_position = k.position_in_unique_constraint")
        by = {}
        for ks, kt, kc, pos, ps, pt, pc, name in got:
            child = kt if ks in DEFAULT_SCHEMAS else f"{ks}.{kt}"
            parent = pt if ps in DEFAULT_SCHEMAS else f"{ps}.{pt}"
            by.setdefault((name, child, parent), []).append((pos, kc, pc))
        for (_, child, parent), cols in sorted(by.items()):
            cols.sort()
            out.append((child, [c for _, c, _ in cols], parent,
                        [p for _, _, p in cols]))
        return out

    def table_facts(self, side, db):
        return {t: {"rows": None, "bytes": None, "key": True}
                for t in self.neutral_tables(side, db)}

    # ---- reading -----------------------------------------------------------

    def _read_sql(self, side, db, table, columns, after, limit, where):
        declared = dict(self.neutral_columns(side, db, table))
        key = self.neutral_key(side, db, table)
        names = [n for n, _ in columns]
        params, types, conds = {}, {}, []
        if after is not None:
            ors = []
            for i in range(len(key)):
                ands = []
                for j, k in enumerate(key[:i + 1]):
                    at = len(params) + 1
                    params[f"p{at}"] = self._to_spanner(declared.get(k),
                                                        after[j])
                    types[f"p{at}"] = self._param_type(side, db,
                                                       declared.get(k))
                    op = ">" if j == i else "="
                    ands.append(f"{self._qd(side, db, k)} {op}"
                                f" {self._mark(side, db, at)}")
                ors.append("(" + " and ".join(ands) + ")")
            conds.append("(" + " or ".join(ors) + ")")
        if where:
            conds.append(f"({where})")
        sql = (f"select {', '.join(self._qd(side, db, n) for n in names)}"
               f" from {self._qualified(side, db, table)}"
               + (" where " + " and ".join(conds) if conds else "")
               + " order by " + ", ".join(self._qd(side, db, k) for k in key)
               + (f" limit {int(limit)}" if limit else ""))
        return sql, params, types, key, names

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        sql, params, types, key, names = self._read_sql(
            side, db, table, columns, after, limit, where)
        rows = self._execute(side, db, sql, params, types)
        if not rows or not all(k in names for k in key):
            return rows, None
        return rows, tuple(rows[-1][names.index(k)] for k in key)

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        sql, params, types, _, _ = self._read_sql(side, db, table, columns,
                                                  None, None, where)
        database = self._database(side, db)
        with database.snapshot() as s:
            got = s.execute_sql(sql, params=params or None,
                                param_types=types or None,
                                request_options=self._request(side))
            batch, fields = [], None
            for r in got:
                if fields is None:
                    fields = list(got.fields)
                batch.append([self._value(f.type_, v)
                              for f, v in zip(fields, r)])
                if len(batch) >= size:
                    yield batch
                    batch = []
            if batch:
                yield batch

    def _read_keys(self, side, db, table, names, keys, at=None):
        """Rows of these keys - each a list of values in key order - as
        dicts, read by the SDK's key set, at a timestamp where given."""
        from google.cloud.spanner_v1 import KeySet
        declared = dict(self.neutral_columns(side, db, table))
        key = self.neutral_key(side, db, table)
        database = self._database(side, db)
        out = []
        for i in range(0, len(keys), KEYS):
            part = [[self._to_spanner(declared.get(k), v)
                     for k, v in zip(key, one)] for one in keys[i:i + KEYS]]
            snap = (database.snapshot(read_timestamp=at_ns(at))
                    if at is not None else database.snapshot())
            with snap as s:
                got = s.read(table, list(names), KeySet(keys=part),
                             request_options=self._request(side))
                rows = [list(r) for r in got]
                fields = list(got.fields) if rows else []
            out += [dict(zip(names, (self._value(f.type_, v)
                                     for f, v in zip(fields, r))))
                    for r in rows]
        return out

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        if not key or not keys:
            return {}
        if where or list(key) != self.neutral_key(side, db, table):
            return self._by_key_map(columns, key, self._by_query(
                side, db, table, columns, key, keys, where))
        names = [n for n, _ in columns]
        found = [[r[n] for n in names]
                 for r in self._read_keys(side, db, table, names,
                                          [list(k) for k in keys])]
        return self._by_key_map(columns, key, found)

    def _by_query(self, side, db, table, columns, match, values, where=None):
        """[row] whose `match` columns hold one of `values`, by SQL."""
        declared = dict(self.neutral_columns(side, db, table))
        names = [n for n, _ in columns]
        found = []
        values = list(values)
        for i in range(0, len(values), 200):
            params, types, ors = {}, {}, []
            for one in values[i:i + 200]:
                ands = []
                for k, v in zip(match, one):
                    at = len(params) + 1
                    params[f"p{at}"] = self._to_spanner(declared.get(k), v)
                    types[f"p{at}"] = self._param_type(side, db,
                                                       declared.get(k))
                    ands.append(f"{self._qd(side, db, k)} ="
                                f" {self._mark(side, db, at)}")
                ors.append("(" + " and ".join(ands) + ")")
            cond = " or ".join(ors)
            if where:
                cond = f"({cond}) and ({where})"
            found += self._execute(
                side, db, f"select {', '.join(self._qd(side, db, n) for n in names)}"
                          f" from {self._qualified(side, db, table)} where"
                          f" {cond}", params, types)
        return found

    def neutral_rows_matching(self, side, db, table, columns, match, values):
        if not match or not values:
            return []
        return self._by_query(side, db, table, columns, match, values)

    # ---- the digest folded here, a partition at a time ---------------------

    def _partition_ways(self):
        from ..decide import Rung
        return (Rung("partitions", "the table's partitions read side by"
                     " side"),
                Rung("stream", "the table read in one streaming query"))

    def _fold_digest(self, side, db, table, columns, where=None):
        """Folded here - reached where the GoogleSQL digest is not proved
        (`DigestsInSQL`), or for a database of the PostgreSQL dialect: the
        table's partitions read side by side and summed, whichever
        partition a row came from; one streaming read where the server
        will not partition the query."""
        from .. import canon, decide
        classes = [c for _, c in columns]
        sql, params, types, _, _ = self._read_sql(side, db, table, columns,
                                                  None, None, where)
        sql = sql.split(" order by ")[0]
        database = self._database(side, db)
        boost = bool(self._ep(side).options.get("data_boost"))

        def fold(rows, fields):
            total, n, batch = 0, 0, []
            for r in rows:
                if fields[0] is None:
                    fields[0] = list(rows.fields)
                batch.append([self._value(f.type_, v)
                              for f, v in zip(fields[0], r)])
                if len(batch) >= 5000:
                    k, total = canon.fold_rows(classes, batch, total)
                    n, batch = n + k, []
            if batch:
                k, total = canon.fold_rows(classes, batch, total)
                n += k
            return n, total

        def run(choice):
            if choice.rung.name == "stream":
                with database.snapshot() as s:
                    n, total = fold(s.execute_sql(
                        sql, params=params or None, param_types=types or None,
                        request_options=self._request(side)), [None])
                return n, str(total)
            snap = database.batch_snapshot()
            try:
                parts = list(snap.generate_query_batches(
                    sql, params=params or None, param_types=types or None,
                    data_boost_enabled=boost))

                def one(part):
                    return fold(snap.process_query_batch(part), [None])
                n, total = 0, 0
                with concurrent.futures.ThreadPoolExecutor(
                        max(1, min(8, len(parts)))) as pool:
                    for k, t in pool.map(one, parts):
                        n, total = n + k, total + t
                return n, str(total)
            finally:
                snap.close()
        return self._attempt(f"{table}: its digest", self._partition_ways(),
                             run, decide.Facts())[1]

    def _declared_cols(self, side, db, table, columns):
        """The columns the GoogleSQL digest renders; a database of the
        PostgreSQL dialect gives none, and is folded here."""
        if self._pg(side, db):
            return []
        declared = dict(self.neutral_columns(side, db, table))
        return [(n, c, declared.get(n)) for n, c in columns]

    # ---- writing -----------------------------------------------------------

    def neutral_create_sql(self, side, db, table, columns, key=()):
        from .. import canon
        if not key:
            raise SystemExit(f"{table} has no key, and Spanner keeps every"
                             " row by its primary key: give the table one,"
                             " or leave it out of this hop")
        pg = self._pg(side, db) if not self.target_missing(db) \
            else self._wants_pg()
        style = "spanner_pg" if pg else "spanner"
        defs = []
        for col in columns:
            name, cls, numbers = col[0], col[1], col[2]
            rule = col[3] if len(col) > 3 else None
            typ = canon.ddl_type(style, cls, numbers, name in key)
            null = " not null" if name in key or (
                rule and rule.get("null") is False) else ""
            defs.append(f"{self._q_style(pg, name)} {typ}{null}")
        keys = ", ".join(self._q_style(pg, k) for k in key)
        name = ".".join(self._q_style(pg, p)
                        for p in str(table).split(".", 1))
        if pg:
            return (f"create table {name} ({', '.join(defs)}, primary key"
                    f" ({keys}))")
        return f"create table {name} ({', '.join(defs)}) primary key ({keys})"

    @staticmethod
    def _q_style(pg, name):
        if pg:
            return '"' + str(name).replace('"', '""') + '"'
        return "`" + str(name).replace("`", "\\`") + "`"

    def _wants_pg(self, db=None):
        """The dialect a database migkit makes speaks: the source's on a
        same-engine hop, else the target's `dialect` option."""
        from . import same_engine
        if same_engine(self):
            return self._pg("src", db or self.databases()[0])
        return str(self.hop.target.options.get("dialect") or "").lower() \
            in ("postgresql", "postgres", "pg")

    def neutral_create(self, side, db, table, columns, key=()):
        self._target_only(side, "create a table")
        self._make_database(db)
        if table in self.neutral_tables(side, db):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        ddl = self.neutral_create_sql(side, db, table, columns, key)
        self._database(side, db).update_ddl([ddl]).result(600)
        self.forget_shapes()
        return ddl

    def prepare_target(self, db):
        """The target database, where it is not there yet - in the source's
        dialect on a same-engine hop - and, on a same-engine hop into a
        database that holds no table, the source's own definition of it:
        tables with their interleaving, indexes, foreign keys, sequences,
        views, roles, row deletion policies. Change streams are the
        source's, not the target's, and are left out; so are the database's
        own options, which `check_params` compares, and every table the hop
        excludes with what is made on it."""
        from . import same_engine
        with _MAKING:
            said = [self._make_database(db)]
            if same_engine(self) and not self._tables_named("dst", db):
                carried = self._definition(db)
                if carried:
                    self._database("dst", db).update_ddl(carried).result(1800)
                    self.forget_shapes()
                    said.append(f"made as the source is ({len(carried)}"
                                " statements of its own definition)")
        return "; ".join(s for s in said if s) or None

    def _make_database(self, db):
        """The target database, made where it is not there; what was done,
        or None."""
        from google.cloud.spanner_admin_database_v1 import DatabaseDialect
        name = self._d("dst", db)
        if self._database("dst", db).exists():
            return None
        pg = self._wants_pg(db)
        self._instance("dst").database(
            name, database_dialect=DatabaseDialect.POSTGRESQL if pg
            else DatabaseDialect.GOOGLE_STANDARD_SQL).create().result(600)
        self.__dict__.get("_databases", {}).pop(("dst", name), None)
        self.__dict__.get("_dialects", {}).pop(("dst", name), None)
        return f"created database {name}"

    def _definition(self, db):
        """The source's own statements, as `GetDatabaseDdl` gives them in
        the order they are made in, but for its change streams, its
        database options and every table the hop leaves out - with what is
        made on one (an index, an interleaved child, a foreign key's
        table)."""
        import re
        src = self._database("src", db)
        src.reload()
        out = []
        for stmt in src.ddl_statements:
            head = " ".join(stmt.split()).upper()
            if head.startswith(("CREATE CHANGE STREAM", "ALTER DATABASE")):
                continue
            names = re.findall(r'(?i)(?:create\s+table|\bon|\binterleave\s+in'
                               r'(?:\s+parent)?|alter\s+table|references)'
                               r'\s+[`"]?([\w.]+)[`"]?', stmt)
            if any(self.hop.excluded(db, *n.split(".", 1)) for n in names):
                continue
            out.append(stmt)
        return out

    def _commit(self, side, db, table, fill):
        """One commit of `fill(batch)`'s mutations, tried again while a
        parent row or a referenced row it needs is still on its way."""
        from google.api_core.exceptions import FailedPrecondition, NotFound
        database = self._database(side, db)
        began = time.monotonic()
        while True:
            try:
                with database.batch(
                        request_options=self._request(side)) as b:
                    fill(b)
                return
            except (NotFound, FailedPrecondition) as e:
                text = str(e).lower()
                waiting = ("parent row" in text or "foreign key" in text
                           or "referenced" in text)
                if not waiting or time.monotonic() - began > PARENT_WAIT:
                    raise
                time.sleep(2)

    def _chunks(self, rows, width):
        per = max(1, MUTATIONS // max(1, width))
        for i in range(0, len(rows), per):
            yield rows[i:i + per]

    def neutral_write(self, side, db, table, columns, rows):
        """Rows as insert-or-update mutations, which replace a row with the
        same key: a batch written again lands on itself."""
        self._target_only(side, "write rows")
        if not rows:
            return 0
        declared = dict(self.neutral_columns(side, db, table))
        names = [n for n, _ in columns]
        for part in self._chunks(rows, len(names)):
            values = [[self._to_spanner(declared.get(n), v)
                       for n, v in zip(names, r)] for r in part]
            self._commit(side, db, table, lambda b, values=values:
                         b.insert_or_update(table=table, columns=names,
                                            values=values))
        return len(rows)

    def _apply_upserts(self, side, db, table, rows):
        declared = dict(self.neutral_columns(side, db, table))
        groups = {}
        for key, values in rows:
            row = {**values, **key}
            groups.setdefault(tuple(row), []).append(row)
        for names, got in groups.items():
            for part in self._chunks(got, len(names)):
                values = [[self._to_spanner(declared.get(n), r[n])
                           for n in names] for r in part]
                self._commit(side, db, table, lambda b, n=list(names),
                             v=values: b.insert_or_update(table=table,
                                                          columns=n,
                                                          values=v))

    def _apply_upsert(self, side, db, table, key, values):
        self._apply_upserts(side, db, table, [(key, values)])

    def _apply_deletes(self, side, db, table, keys):
        from google.cloud.spanner_v1 import KeySet
        declared = dict(self.neutral_columns(side, db, table))
        pk = self.neutral_key(side, db, table)
        keys = list(keys)
        for part in self._chunks(keys, 1):
            ks = KeySet(keys=[[self._to_spanner(declared.get(k), key[k])
                               for k in pk] for key in part])
            self._commit(side, db, table, lambda b, ks=ks:
                         b.delete(table, ks))

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    def neutral_empty(self, side, db, table, where=None):
        """Every row removed by partitioned DML, the table kept."""
        self._target_only(side, "empty a table")
        return int(self._database(side, db).execute_partitioned_dml(
            f"delete from {self._qualified(side, db, table)} where "
            + (f"({where})" if where else "true")) or 0)

    # ---- the checks ----------------------------------------------------------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def repair_plan(self, db, kind):
        return self._as_pair().repair_plan(db, kind)

    def apply(self, db, action):
        return self._as_pair().apply(db, action)

    def _sequences(self, side, db):
        """{sequence: its counter} - the internal state a bit-reversed
        sequence hands its next value out of."""
        database = self._database(side, db)
        with database.snapshot() as s:
            got = s.execute_sql("select * from information_schema.sequences")
            rows = [list(r) for r in got]
            names = [f.name.lower() for f in got.fields] if rows else []
        pg = self._pg(side, db)
        out = {}
        for r in rows:
            row = dict(zip(names, r))
            seq = row.get("name") or row.get("sequence_name")
            if not seq:
                continue
            q = (f"select spanner.get_internal_sequence_state("
                 f"'{str(seq).replace(chr(39), chr(39) * 2)}')" if pg else
                 f"select get_internal_sequence_state(sequence"
                 f" {self._q(seq)})")
            out[str(seq)] = self._execute(side, db, q)[0][0]
        return out

    def check_autoinc(self, db):
        """Each sequence's counter on the target at or past the source's:
        a bit-reversed sequence hands out the value its counter maps to,
        so a target counter behind the source's hands out, again, values
        the source's rows already hold."""
        try:
            src, dst = self._sequences("src", db), self._sequences("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("autoinc", db, "error", "could not read the"
                           f" sequences' state: {str(e).splitlines()[0][:90]}")]
        if not src:
            return [Result("autoinc", db, "ok", "no sequence in the source")]
        pg = self._pg("dst", db)
        out = []
        for seq, have in sorted(src.items()):
            got = dst.get(seq)
            if got is None and seq not in dst:
                out.append(Result("autoinc", f"{db}.{seq}", "diff",
                                  "the sequence is not on the target"))
            elif have is None or (got is not None and int(got) >= int(have)):
                out.append(Result("autoinc", f"{db}.{seq}", "ok",
                                  f"the target's counter {got} is at or past"
                                  f" the source's {have}"))
            else:
                fix = (f"alter sequence {self._qd('dst', db, seq)} restart"
                       f" counter with {int(have) + 1}" if pg else
                       f"alter sequence {self._q(seq)} set options"
                       f" (start_with_counter = {int(have) + 1})")
                out.append(Result(
                    "autoinc", f"{db}.{seq}", "diff",
                    f"the target's counter {got} is behind the source's"
                    f" {have}: its next values are ones the source handed"
                    " out already", "", fix))
        return out

    #: what changes where a row lives, how long it stays, and how far back
    #: it can be read
    CRITICAL_PARAMS = ("version_retention_period",)

    def _settings(self, side, db):
        out = {}
        for t, row in self._tables_named(side, db).items():
            for k in ("parent_table_name", "on_delete_action",
                      "row_deletion_policy_expression", "interleave_type"):
                if k in row:
                    out[f"{t} {k}"] = str(row.get(k))
        for name, value in self._execute(
                side, db, "select option_name, option_value from"
                          " information_schema.database_options"):
            out[str(name)] = str(value)
        return out

    def check_params(self, db):
        """Each table's interleaving, what deleting its parent does and its
        row deletion policy, and the database's own options - its version
        retention the follow reads rows back within."""
        try:
            src, dst = self._settings("src", db), self._settings("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("params", db, "error", "could not read the"
                           f" settings: {str(e).splitlines()[0][:90]}")]
        critical = tuple(k for k in set(src) | set(dst)
                         if k.endswith((" parent_table_name",
                                        " on_delete_action",
                                        " row_deletion_policy_expression"))) \
            + self.CRITICAL_PARAMS
        return self._param_result(db, src, dst, critical,
                                  "make the target's tables and options as"
                                  " the source's")

    def check_deep(self, db):
        """The source's secondary indexes on the target, each over the same
        columns and as unique, and none of the target's still being
        built: a query that the source answers from an index scans the
        whole table on a target without it."""
        out = []
        lacking, building = [], []
        for t in self.neutral_tables("src", db):
            have = {(u, tuple(c)) for _, u, c, _ in
                    self.neutral_indexes("dst", db, t)}
            for name, u, c, _ in self.neutral_indexes("src", db, t):
                if (u, tuple(c)) not in have:
                    lacking.append(f"{t}.{name}")
        for name, state in self._execute(
                "dst", db, "select index_name, index_state from"
                           " information_schema.indexes where index_type ="
                           " 'INDEX'"):
            if state and str(state).upper() != "READ_WRITE":
                building.append(f"{name} ({state})")
        out.append(Result(
            "deep", f"{db} indexes", "diff" if lacking or building else "ok",
            (f"{len(lacking)} of the source's indexes are not on the target"
             f" ({', '.join(lacking[:4])})" if lacking else
             f"indexes still being built: {', '.join(building[:4])}"
             if building else "every index of the source's is on the"
             " target, ready"), "",
            "create them on the target before the cutover" if lacking
            else "wait for the backfill" if building else ""))
        return out

    def settle_target(self, db, from_source=True):
        """A new statistics package built from the loaded tables (`ANALYZE`),
        which the optimizer otherwise builds on its own schedule, days
        later."""
        try:
            self._database("dst", db).update_ddl(["ANALYZE"]).result(1800)
        except Exception as e:  # noqa: BLE001 - said, and nothing else
            return ("the target's statistics were not rebuilt:"
                    f" {str(e).splitlines()[0][:100]}")
        return "the target's optimizer statistics rebuilt (ANALYZE)"

    # ---- who it is -----------------------------------------------------------

    def server_identity(self, side, db):
        ep = self._ep(side)
        out = {"name": f"{ep.options.get('project') or ep.host}/"
                       f"{ep.options.get('instance')}"}
        database = self._database(side, db)
        if not database.exists():
            out["absent"] = True
            return out
        database.reload()
        out["id"] = f"{database.name}@{database.create_time}"
        out["version"] = ("PostgreSQL dialect" if self._pg(side, db)
                          else "GoogleSQL")
        return out

    def stream_identity(self, side, db):
        database = self._database(side, db)
        database.reload()
        out = {"database": f"{database.name}@{database.create_time}"}
        try:
            out["stream"] = self._stream(db) if side == "src" else ""
        except SystemExit:
            out["stream"] = ""
        return out

    # ---- the change stream ---------------------------------------------------

    def _now_ns(self, side, db):
        """TrueTime now: the read timestamp of a strong read begun here,
        which every commit a read begun after it sees is at or before."""
        database = self._database(side, db)
        with database.snapshot(multi_use=True) as s:
            s.begin()
            return ns_of(s._transaction_read_timestamp)

    def _stream(self, db):
        """The change stream the tail reads: the source's `change_stream`,
        or one that watches every table in scope."""
        said = self.hop.source.options.get("change_stream")
        if said:
            return str(said)
        scope = set(self.neutral_tables("src", db))
        streams = {}
        for name, every in self._execute(
                "src", db, "select change_stream_name, `all` from"
                           " information_schema.change_streams"
                if not self._pg("src", db) else
                "select change_stream_name, \"all\" from"
                " information_schema.change_streams"):
            streams[name] = set(scope) if str(every).lower() in (
                "true", "yes", "1") else set()
        for name, table in self._execute(
                "src", db, "select change_stream_name, table_name from"
                           " information_schema.change_stream_tables"):
            if name in streams:
                streams[name].add(table)
        for name, covers in sorted(streams.items()):
            if scope <= covers:
                return name
        raise SystemExit(
            f"{db}: no change stream watches every table in scope, and"
            " Spanner hands its changes out only through one. Have the DBA"
            f" make one (`create change stream migkit_{self.hop.name} for"
            " all options (retention_period = '1d')`), or name one in the"
            " source's options (change_stream: <name>)")

    def _retention_s(self, db, stream):
        """Seconds of changes the stream keeps: its `retention_period`
        (`36h`, `7d`), a day where it sets none."""
        for name, value in self._execute(
                "src", db, "select option_name, option_value from"
                           " information_schema.change_stream_options where"
                           " change_stream_name = "
                           + self._mark("src", db, 1), {"p1": stream}):
            if str(name).lower() == "retention_period":
                return seconds_of(value)
        return 86400.0

    def _follower(self, db):
        from .. import changetopic
        held = self.__dict__.setdefault("_followers", {})
        if db not in held:
            stream = self._stream(db)
            held[db] = changetopic.Follower(
                changetopic.SpannerChanges(),
                SpannerLanes(self, db, stream),
                name=lambda row, db=db: self._named(db, row),
                read_back=lambda t, rows, v, db=db: self._read_back(
                    db, t, rows, v))
        return held[db]

    def _named(self, db, row):
        scope = self.__dict__.setdefault("_scope", {})
        if db not in scope or row.table not in scope[db]:
            scope[db] = set(self.neutral_tables("src", db))
        return row.table if row.table in scope[db] else None

    def change_point(self, side, db):
        return self._follower(db).point(fence=lambda: self._now_ns(side, db))

    def copy_point(self, side, db):
        return self.change_point(side, db)

    def log_position(self, side, db):
        return {"fence": self._now_ns(side, db)}

    @staticmethod
    def position_reached(have, want):
        from ..changetopic import Follower
        return Follower.reached(have, want)

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each row the change stream says changed, in commit-timestamp
        order, as Spanner held it at the fence its batch ends on."""
        if token is None:
            return [], self.change_point(side, db)
        return self._follower(db).changes(token, limit)

    def release_changes(self):
        for f in self.__dict__.pop("_followers", {}).values():
            f.close()

    def position_lost(self, side, db, token):
        return self._follower(db).lost(token) if token else None

    def stream_room(self, side, db, token):
        return self._follower(db).room(token)

    def snapshot_mark(self, side, db):
        return str(self._now_ns(side, db))

    @staticmethod
    def mark_covers(side, db, mark, change):
        """A read begun after the mark sees every commit at or below it:
        TrueTime is one clock for both."""
        txn = change.get("txn")
        if mark is None or not isinstance(txn, int):
            return None
        return txn <= int(mark)

    def _read_back(self, db, table, rows, version):
        """[(row, key, values or None)]: each key the stream named, read as
        Spanner held it at `version` - or as it is now, where that is past
        the database's version retention - and, at `version`, held to what
        the stream said of it."""
        from google.api_core.exceptions import FailedPrecondition

        from .. import canon, changetopic
        declared = dict(self.neutral_columns("src", db, table))
        key = self.neutral_key("src", db, table)
        classes = {n: canon.type_class("spanner", t)
                   for n, t in declared.items()}
        names = list(declared)

        def typed(row):
            missing = [k for k in key if k not in row.key]
            if missing:
                raise SystemExit(
                    f"{table}: the change stream names its changes by"
                    f" {', '.join(row.key) or 'no key'}, and its primary key"
                    f" is {', '.join(key)}: the stream's table is not the"
                    " table here")
            return [changetopic.from_text(classes.get(k) or "text",
                                          row.key[k], "base64") for k in key]

        def text(values):
            return tuple(changetopic.key_text(classes.get(k), v)
                         for k, v in zip(key, values))
        wanted = [typed(r) for r in rows]
        at_fence = True
        try:
            got = self._read_keys("src", db, table, names, wanted, at=version)
        except FailedPrecondition:
            # the fence is older than the database keeps versions: the rows
            # as they are now, which later changes level again
            at_fence = False
            got = self._read_keys("src", db, table, names, wanted)
        found = {text([v[k] for k in key]): v for v in got}
        out = []
        for r, values_key in zip(rows, wanted):
            values = found.get(text(values_key))
            if at_fence and (values is None) != (r.op == "delete"):
                raise SystemExit(
                    f"{db}.{table}: the change stream says the row keyed"
                    f" {dict(zip(key, values_key))} was"
                    f" {'deleted' if r.op == 'delete' else 'written'} by"
                    f" {r.version}, and Spanner as it stood then"
                    f" {'holds' if values is not None else 'does not hold'}"
                    " it: the key as the stream names it is not the key"
                    " here. Nothing of this batch was applied")
            out.append((r, dict(zip(key, values_key)) if values is None
                        else {k: values[k] for k in key}, values))
        return out

    # ---- through the pair every engine is carried by -------------------------

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    def tail_apply(self, db, go, token_path, log):
        return self._as_pair().tail_apply(db, go, token_path, log)

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def delta_verify(self, db, limit=20000, log=None):
        return self._as_pair().delta_verify(db, limit, log)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "rows read by key and written as mutations,"
                            " digested by the server in GoogleSQL where"
                            " proved, else by its partitions side by side"}]
        if side != "src":
            return items
        db = (self.databases() or [""])[0]
        try:
            stream = self._stream(db)
        except SystemExit as e:
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes are not followed",
                          "detail": str(e)})
            return items
        from ..changetopic import SpannerChanges
        items.append({"level": "pass", "scope": "reach",
                      "item": "changes followed through its change stream",
                      "detail": f"{stream}, partition by partition in"
                                " commit-timestamp order; it loses"
                                f" {SpannerChanges.LOSES}"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")


def _nanotime(v):
    """A Spanner timestamp as migkit carries one: a UTC datetime with the
    three digits past its microsecond as `nanosecond`."""
    from .. import nanotime
    if not isinstance(v, datetime.datetime):
        return v
    whole = getattr(v, "nanosecond", None)
    ns = int(whole) % 1000 if whole is not None else 0
    plain = datetime.datetime(v.year, v.month, v.day, v.hour, v.minute,
                              v.second, v.microsecond,
                              tzinfo=datetime.timezone.utc)
    return nanotime.of(plain, ns)


class SpannerLanes:
    """A change stream's partitions, each read by its own query from where
    it was, its children added as it ends and it dropped once read to its
    end. A lane's position is the commit timestamp to read on from
    (nanoseconds); the root, `""`, is the query with no partition token,
    which names the partitions of a moment. Beside each partition's
    records go its marks: its heartbeats, the end of a window it was read
    through, and - for a child just found - the moment before it begins."""
    KIND = "spanner"

    def __init__(self, engine, db, stream):
        self.engine, self.db, self.stream = engine, db, stream
        self.live = None

    def lanes(self):
        return sorted(self.live or [""])

    def ends(self):
        now = self.engine._now_ns("src", self.db)
        self.live = {""}
        return {"": {"at": now}}

    def _query(self, token, start, end):
        """(records, heartbeats, children, ended) of one partition from
        `start` to `end`, nanoseconds."""
        from google.cloud.spanner_v1 import param_types as pt
        eng, db = self.engine, self.db
        pg = eng._pg("src", db)
        params = {"p1": at_ns(start), "p2": at_ns(end),
                  "p3": token or None, "p4": HEARTBEAT_MS}
        types = {"p1": pt.TIMESTAMP, "p2": pt.TIMESTAMP, "p3": pt.STRING,
                 "p4": pt.INT64}
        if pg:
            sql = (f'select * from "spanner"."read_json_{self.stream}"'
                   "($1, $2, $3, $4, null)")
        else:
            sql = (f"select ChangeRecord from READ_{self.stream}("
                   "start_timestamp => @p1, end_timestamp => @p2,"
                   " partition_token => @p3, heartbeat_milliseconds => @p4)")
        database = eng._database("src", db)
        records, beats, children, ended = [], [], [], False
        with database.snapshot() as s:
            got = s.execute_sql(sql, params=params, param_types=types)
            fields = None
            for row in got:
                if fields is None:
                    fields = list(got.fields)
                value = eng._value(fields[0].type_, row[0])
                parts = [value] if isinstance(value, dict) else list(
                    value or [])
                for part in parts:
                    if not isinstance(part, dict):
                        continue
                    for rec in _listed(part.get("data_change_record")):
                        records.append(_data_record(rec))
                    for rec in _listed(part.get("heartbeat_record")):
                        beats.append(ns_of(rec["timestamp"]))
                    for rec in _listed(part.get("child_partitions_record")):
                        begin = ns_of(rec["start_timestamp"])
                        for child in rec.get("child_partitions") or []:
                            children.append((child["token"], begin))
                        ended = True
        return records, beats, children, ended

    def read(self, at, most, seconds):
        out, nxt, caught = [], {}, True
        now = self.engine._now_ns("src", self.db)
        todo = []
        for lane, pos in sorted((at or {}).items()):
            start = int(pos["at"])
            end = min(now, start + WINDOW_S * 10 ** 9)
            if end <= start:
                nxt[lane] = pos
                continue
            if end < now:
                caught = False
            todo.append((lane, start, end))

        def one(job):
            lane, start, end = job
            return job, self._query(lane, start, end)
        with concurrent.futures.ThreadPoolExecutor(
                max(1, min(8, len(todo)))) as pool:
            for (lane, start, end), got in pool.map(one, todo):
                records, beats, children, ended = got
                for rec in records:
                    out.append((lane, {"at": rec["commit_ns"]}, None, rec,
                                None))
                for beat in beats:
                    out.append((lane, {"at": beat}, None, {"mark": beat},
                                None))
                for token, begin in children:
                    if token not in nxt and token not in (at or {}):
                        nxt[token] = {"at": begin}
                        out.append((token, {"at": begin}, None,
                                    {"mark": begin - 1}, None))
                if not ended:
                    nxt[lane] = {"at": end}
                    out.append((lane, {"at": end}, None, {"mark": end},
                                None))
        self.live = set(nxt) or {""}
        if not nxt:
            # every partition ended with no child: the stream from now
            nxt = {"": {"at": now}}
        return out, nxt, caught

    def lost(self, at):
        keep = self.engine._retention_s(self.db, self.stream)
        if not keep or not at:
            return None
        oldest = min(int(p["at"]) for p in at.values())
        now = self.engine._now_ns("src", self.db)
        if (now - oldest) / 10 ** 9 > keep:
            return (f"the change stream keeps {keep / 3600:g} hours of"
                    " changes, and the saved position is older: what"
                    " changed then is carried by nothing")
        return None

    def room(self, at):
        keep = self.engine._retention_s(self.db, self.stream)
        if not keep or not at:
            return None
        oldest = min(int(p["at"]) for p in at.values())
        now = self.engine._now_ns("src", self.db)
        return {"seconds": max(0.0, keep - (now - oldest) / 10 ** 9)}

    def close(self):
        return None


def _listed(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _data_record(rec):
    """A data change record as the envelope reads it, from either dialect:
    its commit timestamp in nanoseconds, its table, what it did, and each
    changed row's key as JSON."""
    mods = []
    for mod in rec.get("mods") or []:
        keys = _plain_json(mod.get("keys"))
        mods.append({"keys": dict(keys or {})})
    return {"commit_ns": ns_of(rec["commit_timestamp"]),
            "table_name": rec.get("table_name"),
            "mod_type": str(rec.get("mod_type") or "").upper(),
            "transaction": rec.get("server_transaction_id"),
            "mods": mods}


def _register():
    """What of the SDK is used, read off this file, and why the rest is
    not (`tooluse`)."""
    from .. import tooluse
    unused = tooluse.UNUSED
    tooluse.register("google-cloud-spanner", licence="Apache-2.0", reads=(
        ("engines/spanner.py",),
        {("Snapshot", "execute_sql"): "execute_sql",
         ("Snapshot", "read"): "read",
         ("Snapshot", "read_timestamp"): "read_timestamp",
         ("Snapshot", "multi_use"): "multi_use",
         ("Batch", "insert_or_update"): "insert_or_update",
         ("Batch", "delete"): "delete",
         ("BatchSnapshot", "generate_query_batches"):
             "generate_query_batches",
         ("BatchSnapshot", "process_query_batch"): "process_query_batch",
         ("request", "data_boost_enabled"): "data_boost_enabled",
         ("request", "priority"): "PRIORITY_LOW",
         ("Database", "execute_partitioned_dml"): "execute_partitioned_dml",
         ("Database", "update_ddl"): "update_ddl",
         ("Database", "ddl_statements"): "ddl_statements",
         ("Database", "database_dialect"): "database_dialect",
         ("Database", "create"): "create",
         ("KeySet", "keys"): "KeySet",
         ("change stream", "heartbeat_milliseconds"): "HEARTBEAT_MS",
         ("change stream", "child_partitions"): "child_partitions"}),
        options={
        ("Batch", "insert"): (
            unused, "insert refuses a row that is there, and a batch written"
                    " again has to land on itself"),
        ("Batch", "replace"): (
            unused, "replace clears the columns a write leaves out, which"
                    " insert-or-update of every column already carries"),
        ("Database", "mutation_groups"): (
            unused, "batch writes commit groups apart and out of order,"
                    " where a copy's range is proved as one commit"),
        ("Database", "run_in_transaction"): (
            unused, "a read-write transaction takes locks a copy that only"
                    " reads and upserts does not need"),
        ("dbapi", "connect"): (
            unused, "the DB-API layer turns every write into DML, which is"
                    " slower than mutations and holds the same limit"),
    })


_register()
