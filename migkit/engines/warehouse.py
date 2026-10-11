"""Warehouses as one side of a pair (backlog 33): Amazon Redshift,
Snowflake and Google BigQuery.

A hop's database is a schema (a dataset, in BigQuery); the database to
connect to is the endpoint's `database` option (the project, in BigQuery).
Tables, columns and keys are read from the warehouse's own
`information_schema`, rows through its driver.

**A digest is answered by the warehouse** (`neutral_digest`): each row's
canonical text built in its own SQL (`warehouse_sql`), hashed and summed
there, so two numbers cross the network instead of the table. A table with
a column whose text the warehouse's SQL does not render as migkit does (a
float on Snowflake or Redshift, JSON, a padded CHAR), or whose first rows
render differently there than here, is folded in this process as before,
and the report says which side's rows crossed the network.

**A batch of the copy lands whole, once**: it is staged, then merged into
the table by its key in one statement (inserted, where the table has no
key), so a batch written again after a restart replaces itself. BigQuery
stages it through a load job from Parquet, into a table of its own that
expires in a day - the table the rows are for takes only the MERGE, which
BigQuery never refuses on its daily limit of table operations; past most
of the project's daily load jobs (read from its own job history) the stage
is written through the Storage Write API instead. Snowflake stages a
Parquet file in the table's own stage and copies it in (its load history
keeps a file loaded once for 64 days). Redshift copies Parquet files from
S3, as many as it has slices, where the endpoint names a `stage` and an
`iam_role`, and takes the rows in one insert statement a page where it
does not.

**A tail's batch is applied exactly once**: the tail numbers its batches
for a warehouse (`EXACT_BATCHES`), the batch's changes land in a journal
table beside the one they are for, and one transaction merges them in and
writes the batch's number and position in `_migkit_ledger`, which the tail
asks before it goes on after a stop (`origin_seen`). The journal is where
each warehouse's own exactly-once device is used: BigQuery's committed
write stream refuses rows at an offset it already holds, Snowflake's
channel keeps the last offset token it committed, and Redshift's journal
is a table of the same transaction.

**Redshift and Snowflake are read through their catalogues** (`_CatalogWarehouse`):
the deep checks hold the two sides' keys (and a key more than one row holds,
which a warehouse never refuses), columns, foreign keys, views, materialized
views, table options, row policies, grants and owners to each other, for the
tables and objects the hop takes; `assess` names what the move will not do
(streams, tasks, stages, datashares, routines, tables with no key). Their
statements have not run on an account: `capabilities.UNPROVED` says which.

The tables migkit makes here start with `_migkit_`, are left out of every
listing of the application's tables, and are named by `assess`. Proved on
BigQuery's emulator and on PostgreSQL where Redshift's SQL is PostgreSQL's;
Snowflake, and what each emulator does not implement, only on a real
account - each test says which.
"""
import hashlib
import json

from .. import decide, nanotime, pieces
from . import warehouse_sql
from .base import Engine, Result
from .bigquery_changes import BigQueryChanges
from .bigquery_checks import BigQueryChecks
from .dbapi import MARK, DbapiRows, FoldsToCapitals

#: the declared type from `information_schema.columns`, with the numbers a
#: counterpart needs, in the standard's spelling
DECLARED = ("case when data_type in ('numeric', 'decimal', 'NUMBER')"
            " and numeric_precision is not null then data_type || '('"
            " || numeric_precision || ',' || coalesce(numeric_scale, 0)"
            " || ')'"
            " when character_maximum_length is not null and"
            " character_maximum_length > 0 then data_type || '('"
            " || character_maximum_length || ')'"
            " else data_type end")

#: the prefix of every table migkit makes in a warehouse
OWN = "_migkit_"


def _utc_naive(v):
    """An instant as UTC reads it, without its zone: what a column with
    no zone holds of it, as every engine's reading here renders it."""
    import datetime
    if isinstance(v, datetime.datetime) and v.tzinfo:
        # the digits past the microsecond kept: a new datetime made from
        # one does not carry them (`nanotime`)
        ns = getattr(v, "nanosecond", 0) or 0
        return nanotime.of(v.astimezone(datetime.timezone.utc)
                           .replace(tzinfo=None), ns)
    return v


def stage_text(v):
    """A value as the text a warehouse casts back to it exactly: a decimal
    in full, a float as the shortest text that reads back as the same
    double, bytes in hex, a time in ISO 8601, JSON as its text."""
    import datetime
    import decimal
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, decimal.Decimal):
        return format(v, "f")
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex().upper()
    if isinstance(v, datetime.datetime):
        # an instant as UTC reads it: the session reading it back is UTC's
        return _utc_naive(v).isoformat(sep=" ")
    if isinstance(v, (datetime.date, datetime.time)):
        return v.isoformat()
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, default=str)
    return str(v)


def _typed(typ, v):
    """A value a change log handed over as the text it printed (a
    PostgreSQL slot's times are), as the type the column takes."""
    import datetime
    import decimal

    import pyarrow as pa
    if not isinstance(v, str):
        return v
    if pa.types.is_timestamp(typ):
        return nanotime.parse(v)
    if pa.types.is_date(typ):
        return datetime.date.fromisoformat(v)
    if pa.types.is_time(typ):
        return nanotime.parse(v)
    if pa.types.is_decimal(typ):
        return decimal.Decimal(v)
    if pa.types.is_integer(typ):
        return int(v)
    if pa.types.is_floating(typ):
        return float(v)
    if pa.types.is_boolean(typ):
        return v.lower() in ("t", "true", "1", "y", "yes", "on")
    return v


def arrow_array(typ, values):
    """An Arrow array of `typ` from the values a batch carries."""
    import decimal

    import pyarrow as pa
    values = [_typed(typ, v) for v in values]
    if pa.types.is_timestamp(typ):
        values = [_utc_naive(v) if typ.tz is None else v for v in values]
    elif pa.types.is_decimal(typ):
        step = decimal.Decimal(1).scaleb(-typ.scale)
        values = [None if v is None else decimal.Decimal(v).quantize(step)
                  for v in values]
    elif pa.types.is_string(typ) or pa.types.is_large_string(typ):
        values = [None if v is None else
                  json.dumps(v, sort_keys=True, default=str)
                  if isinstance(v, (dict, list)) else str(v)
                  if not isinstance(v, str) else v for v in values]
    elif pa.types.is_binary(typ) or pa.types.is_large_binary(typ):
        values = [None if v is None else bytes(v) for v in values]
    return pa.array(values, typ)


class _Warehouse(warehouse_sql.DigestsInSQL, DbapiRows, Engine):
    checks = ("schema", "counts", "data")
    SQL_DIALECT = None
    FOLDED_BECAUSE = ("holds a column whose text its SQL does not render as"
                      " migkit does (a float, JSON, a padded CHAR), or"
                      " rendered this table's first rows differently")
    OVER_NETWORK = True
    PARAMSTYLE = "pyformat"
    LIMIT = "limit"
    #: two MERGEs into one table at once wait on each other, or one fails
    #: (BigQuery's concurrent DML, Redshift's serializable isolation)
    WRITES_IN_PARALLEL = False
    #: the tail numbers its batches for this target, and the ledger says
    #: which one it last committed
    EXACT_BATCHES = True
    LEDGER = OWN + "ledger"

    def _endpoint(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _database(self, side):
        name = (self._endpoint(side).options or {}).get("database")
        if not name:
            raise SystemExit(f"the {'source' if side == 'src' else 'target'}"
                             " needs `database`, the database to connect"
                             " to; the hop's databases are its schemas")
        return name

    def databases(self):
        return list(self.hop.databases)

    def _qualified(self, side, db, table):
        return f"{self._q(self._d(side, db))}.{self._q(table)}"

    def _create_name(self, side, db, table):
        return self._qualified(side, db, str(table).split(".")[-1])

    def _catalog(self, side, db, what):
        """`information_schema.<what>`, as this warehouse names it."""
        return f"information_schema.{what}"

    @staticmethod
    def _own(name):
        return str(name).lower().startswith(OWN)

    def _table_names(self, side, db):
        return [t for (t,) in self._rows(
            side, db, "select table_name from"
                      f" {self._catalog(side, db, 'tables')} where"
                      f" table_schema = {MARK} and table_type ="
                      f" {MARK} order by 1",
            (self._d(side, db), self.BASE_TABLE))]

    def neutral_tables(self, side, db):
        return [t for t in self._table_names(side, db)
                if not self._own(t) and self.in_scope(db, t)]

    def _assess_extra(self):
        return self.reach_items("dst")

    def reach_items(self, side):
        """What `assess` says of a warehouse target, alone or as a pair's
        side (`HeteroEngine._assess_extra`): the tables migkit keeps in its
        schemas, which no listing of the application's tables shows."""
        items = []
        if side != "dst":
            return items
        for db in self.databases():
            try:
                own = [t for t in self._table_names(side, db)
                       if self._own(t)]
            except Exception:  # noqa: BLE001 - the other rows say why
                continue
            if own:
                items.append({
                    "level": "info", "scope": "target",
                    "item": f"{db}: migkit's own tables",
                    "detail": f"{len(own)}: {', '.join(own[:6])} - the"
                              " ledger says which tail batch was committed"
                              " last, a journal holds a table's changes"
                              " until they are merged, a stage a batch"
                              " being loaded; drop them after cutover"})
        return items

    #: what `information_schema.tables` calls a table
    BASE_TABLE = "BASE TABLE"

    def neutral_columns(self, side, db, table):
        return [(n, t) for n, t in self._rows(
            side, db, f"select column_name, {DECLARED} from"
                      f" {self._catalog(side, db, 'columns')} where"
                      f" table_schema = {MARK} and table_name = {MARK}"
                      " order by ordinal_position",
            (self._d(side, db), table))]

    def neutral_key(self, side, db, table):
        """The declared primary key. A warehouse does not enforce it, so
        two rows can share it; the pair's check counts rows as well."""
        return [n for (n,) in self._rows(
            side, db, "select k.column_name from"
                      f" {self._catalog(side, db, 'table_constraints')} c"
                      f" join {self._catalog(side, db, 'key_column_usage')}"
                      " k on k.constraint_name = c.constraint_name and"
                      " k.table_schema = c.table_schema and k.table_name ="
                      " c.table_name where c.constraint_type ="
                      f" 'PRIMARY KEY' and c.table_schema = {MARK} and"
                      f" c.table_name = {MARK} order by k.ordinal_position",
            (self._d(side, db), table))]

    @staticmethod
    def _column_tail(rule, style):
        """Only `not null`: a default is an expression of the engine it
        came from, which a warehouse need not parse, and an identity is
        the source's to keep."""
        return " not null" if rule and rule.get("null") is False else ""

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    # ---- a batch of the copy, landed whole ---------------------------------

    @staticmethod
    def _fingerprint(table, names, rows, extra=""):
        """16 hex digits naming a batch by what it holds: the same rows
        give the same name, so a stage, a file or a job made for a batch
        is found again when the batch is written again."""
        h = hashlib.sha256(f"{table}\x1f{','.join(names)}\x1f{extra}"
                           .encode(), usedforsecurity=False)
        for r in rows:
            h.update(repr(tuple(r)).encode())
            h.update(b"\n")
        return h.hexdigest()[:16]

    @staticmethod
    def _leaf(name):
        return "".join(ch if ch.isalnum() or ch == "_" else "_"
                       for ch in str(name).split(".")[-1])[:64]

    def neutral_write(self, side, db, table, columns, rows):
        """The batch staged and merged in by its key, in one statement: a
        batch written again replaces itself. A table with no key has the
        batch inserted: the copier copies such a table from empty, in one
        pass."""
        from .. import canon
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        key = [k for k in self.neutral_key(side, db, table) if k in names]
        # a field that is not there is a NULL here: a column has one state
        # for both
        rows = [[None if v is canon.ABSENT else v for v in r] for r in rows]
        self._load_batch(side, db, table, columns, rows, key)
        return len(rows)

    def _load_batch(self, side, db, table, columns, rows, key):
        raise NotImplementedError

    # ---- a tail's batch, applied exactly once ------------------------------

    def neutral_apply(self, side, db, changes):
        """The batch's rows, each table's changes collapsed to one a key,
        landed in the table's journal and merged in by one transaction
        with the batch's line in the ledger. The batch's number and
        position come from the tail (`twoway.batch_seen`); a batch applied
        without one is merged the same way and not written in the ledger.

        A counter's change (`canon.Added`) is not taken: a warehouse is
        never one side of a two-way hop."""
        from .. import canon, twoway
        self._target_only(side, "apply changes")
        seen = twoway.seen_of(twoway.batch_seen(self))
        by_table = {}
        for table, (kind, key, values) in self._net_rows(side, db, changes):
            if any(isinstance(v, canon.Added) for v in values.values()):
                raise SystemExit(f"{table}: a counter's change adds to the"
                                 " value where it stands, which only a"
                                 " two-way hop does")
            by_table.setdefault(str(self.local_table(table)).split(".")[-1],
                                []).append((kind, dict(key), dict(values)))
        if by_table:
            self._apply_batch(side, db, by_table, seen)
        return len(changes)

    def _journal_rows(self, table, rows, n, fp):
        """[(record, shape)] for a table's net rows: every column the change
        carries, the operation, and which columns it carried - a column a
        change did not carry is left as it is on the target, not set to
        NULL. `_migkit_seq` keeps the batch's order."""
        out = []
        for seq, (kind, key, values) in enumerate(rows):
            row = {**key, **({} if kind == "delete" else values)}
            shape = ",".join(sorted(row))
            out.append(({**row, "_migkit_batch": n, "_migkit_seq": seq,
                         "_migkit_op": kind, "_migkit_fp": fp,
                         "_migkit_cols": shape}, shape))
        return out

    @staticmethod
    def _shapes(journal):
        """{shape: [column]} in the order the batch first had each."""
        out = {}
        for record, shape in journal:
            out.setdefault(shape, shape.split(","))
        return out

    def _journal_name(self, table, columns):
        """The journal of a table, named by its columns as well: a table
        whose columns changed has a journal of its new shape."""
        sig = hashlib.sha256(json.dumps(columns).encode(),
                             usedforsecurity=False).hexdigest()[:8]
        return f"{OWN}journal_{self._leaf(table)}_{sig}"

    def origin_seen(self, side, db):
        """{"token", "batch"} of the last batch the ledger says this target
        committed for the hop, or None - the tail's `committed_ahead` asks
        it before going on after a stop."""
        try:
            got = self._rows(side, db, "select batch, token from"
                                       f" {self._qualified(side, db, self.LEDGER)}"
                                       f" where hop = {MARK} and db = {MARK}",
                             (self.hop.name, str(db)))
        except Exception:  # noqa: BLE001 - no ledger yet: nothing committed
            return None
        if not got or got[0][0] is None:
            return None
        return {"batch": int(got[0][0]), "token": json.loads(got[0][1])}

    def _apply_batch(self, side, db, by_table, seen):
        raise NotImplementedError


class _CatalogWarehouse(_Warehouse):
    """A warehouse read through the standard `information_schema` and its
    own system views - Redshift and Snowflake - held to everything a
    server of its kind is: the checks and repairs through the pair every
    engine compares by, and a deep look at the catalogue of each side.

    A warehouse declares keys and foreign keys and enforces none, keeps no
    secondary index and runs no trigger, so a move is proved by what the
    catalogue says of both sides and by the rows, not by what the server
    refused."""

    def repair_plan(self, db, kind):
        return self._as_pair().repair_plan(db, kind)

    def apply(self, db, action):
        return self._as_pair().apply(db, action)

    def create_missing(self, db, log=None):
        return self._as_pair().create_missing(db, log)

    def _listing(self, side, db, sql, args=None):
        """The rows of a statement as {column, in small letters: value}, for
        a listing whose columns are read by name."""
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            self._run(cur, sql, args)
            names = [str(d[0]).lower() for d in cur.description]
            return [dict(zip(names, r)) for r in cur.fetchall()]
        finally:
            conn.close()

    def _assess_extra(self):
        return self.reach_items("src") + self.reach_items("dst")

    def reach_items(self, side):
        """The tables migkit keeps in a target's schemas
        (`_Warehouse.reach_items`), and for a source what its move will not
        do (`migkit.handwork`)."""
        items = super().reach_items(side)
        if side == "src":
            inv = self._handwork(side)
            items += inv.rows() + inv.summary()
        return items

    # ---- the catalogue of one side, read for the deep checks -----------------
    #
    # A warehouse enforces none of the keys it declares and keeps no index,
    # so what a move leaves behind is told by the catalogue: each reader
    # below answers {table or object: what the catalogue says of it} for the
    # whole schema in one statement, and `_held` holds the two sides' answers
    # to each other. Only a table or an object the hop takes is read
    # (`_scoped_rows`, `takes`).

    @staticmethod
    def _said(name):
        """A name as the catalogue spells it, in the case migkit says it."""
        return name

    def _stored(self, table):
        """A table's name as the catalogue stores it."""
        return str(table)

    def _schema(self, side, db):
        """The schema's name as the catalogue stores it."""
        return self._d(side, db)

    def _named(self, db, rows):
        """The rows of a catalogue listing for the tables the hop takes,
        the first value of each said as migkit says a name."""
        return self._scoped_rows(
            db, [[self._said(r[0])] + list(r[1:]) for r in rows])

    def neutral_tables(self, side, db):
        """The tables of the schema the hop takes, asked of the scope's one
        answer (`Engine.in_scope`) in the name the engine says them."""
        return [t for t in self._table_names(side, db)
                if not self._own(t) and self.in_scope(db, t)]

    def _first_line(self, err):
        return (str(err).strip().splitlines() or [""])[-1][:90]

    def _scoped_names(self, side, db):
        """The tables in scope on `side`, as the catalogue names them
        (`neutral_tables` asks the scope)."""
        return self.neutral_tables(side, db)

    def _held(self, db, noun, mine, theirs, names):
        """(status, detail, {name: "ok" | "diff"}) of what the two sides'
        catalogues say of the `names` the check compares: a name one side
        has no answer for differs."""
        names = sorted(names)
        differ = [n for n in names if mine.get(n) != theirs.get(n)]
        tables = {n: ("diff" if n in differ else "ok") for n in names}
        if not differ:
            return "ok", f"{len(names)} {noun} the same on both sides", tables
        shown = [f"{n} {self._differs(mine.get(n), theirs.get(n))}"
                 for n in differ[:4]]
        return "diff", (f"{len(differ)} of {len(names)} {noun} differ: "
                        + "; ".join(shown)
                        + (f"; and {len(differ) - 4} more"
                           if len(differ) > 4 else "")), tables

    @staticmethod
    def _differs(a, b):
        """Where two answers part, short."""
        if isinstance(a, dict) and isinstance(b, dict):
            names = sorted(str(k) for k in set(a) | set(b)
                           if a.get(k) != b.get(k))
            return "(" + ", ".join(names[:4]) \
                + (" ..." if len(names) > 4 else "") + ")"
        if a is None or b is None:
            return "(only on the " + ("target" if a is None else "source")
        return f"({str(a)[:40]} on the source, {str(b)[:40]} on the target)"

    def _read_error(self, db, scope, category, err):
        return Result("deep", scope, "error",
                      f"could not read the catalogue: {self._first_line(err)}",
                      "", "", category)

    # the readers: {name: facts} for the whole schema

    def _column_facts(self, side, db):
        """{table: {column: (declared type, takes NULL, default)}}."""
        out = {}
        for t, c, typ, null, default in self._named(db, self._rows(
                side, db, "select table_name, column_name,"
                          f" {DECLARED}, is_nullable, column_default from"
                          f" {self._catalog(side, db, 'columns')} where"
                          f" table_schema = {MARK} order by table_name,"
                          " ordinal_position", (self._schema(side, db),))):
            out.setdefault(self._said(t), {})[self._said(c)] = (
                typ, str(null).upper() != "NO",
                self._default_text(side, db, default))
        return out

    def _default_text(self, side, db, default):
        """A column's default as the two sides compare it: what a cluster
        numbers with, and the database it names itself by, are its own."""
        return None if default in (None, "") else str(default)

    def _keys_of(self, side, db):
        """{table: [declared key columns]}."""
        got = {}
        for t, c in self._named(db, self._rows(
                side, db, "select c.table_name, k.column_name from"
                          f" {self._catalog(side, db, 'table_constraints')} c"
                          f" join {self._catalog(side, db, 'key_column_usage')}"
                          " k on k.constraint_name = c.constraint_name and"
                          " k.table_schema = c.table_schema and k.table_name"
                          " = c.table_name where c.constraint_type ="
                          f" 'PRIMARY KEY' and c.table_schema = {MARK}"
                          " order by 1, k.ordinal_position",
                (self._schema(side, db),))):
            got.setdefault(self._said(t), []).append(self._said(c))
        return got

    def _duplicate_keys(self, side, db):
        """{table: how many key values more than one row holds} for the
        tables in scope that declare a key - which the warehouse never
        enforces."""
        keys, out = self._keys_of(side, db), {}
        for t in self._scoped_names(side, db):
            if not keys.get(t):
                continue
            cols = ", ".join(self._q(c) for c in keys[t])
            n = self._rows(side, db, "select count(*) from (select 1 from"
                                     f" {self._qualified(side, db, t)}"
                                     f" group by {cols} having count(*) > 1)"
                                     " d")[0][0]
            if n:
                out[t] = int(n)
        return out

    def _views_of(self, side, db):
        """{view: its definition's select}."""
        return dict(self.neutral_views(side, db))

    def _matviews(self, side, db):
        """{materialized view: its state}."""
        return {}

    def _table_options(self, side, db):
        """{table: the options the table was made with}."""
        return {}

    def _owners(self, side, db):
        """{table: the account that owns it}."""
        return {}

    def _policies(self, side, db):
        """{table: the row policies attached to it}."""
        return {}

    def _grants_of(self, side, db):
        """{table: {(grantee, privilege)}}."""
        got = {}
        for t, who, what in self._named(db, self._rows(
                side, db, "select table_name, grantee, privilege_type from"
                          f" {self._catalog(side, db, 'table_privileges')}"
                          f" where table_schema = {MARK} order by 1, 2, 3",
                (self._schema(side, db),))):
            got.setdefault(self._said(t), set()).add((str(who), str(what)))
        return got

    # the checks: each finding names the tables it compared and how each
    # came out, and the category it is about

    def _both_tables(self, db):
        there = set(self.neutral_tables("dst", db))
        return [t for t in self.neutral_tables("src", db) if t in there]

    def _by_table(self, db, noun, read):
        """(status, detail, {table: how it came out}) of `read(side, db)`
        held to itself on the two sides, for the tables both have."""
        try:
            return self._held(db, noun, read("src", db), read("dst", db),
                              self._both_tables(db))
        except Exception as e:  # noqa: BLE001 - said, as the error
            return ("error", f"could not read the catalogue:"
                             f" {self._first_line(e)}", {})

    def _by_object(self, db, kind, noun, read):
        """The same for the objects of a kind the hop takes, one side
        having an object the other lacks included."""
        try:
            mine, theirs = read("src", db), read("dst", db)
            return self._held(db, noun, mine, theirs, [
                n for n in set(mine) | set(theirs)
                if self.takes(db, kind, n)])
        except Exception as e:  # noqa: BLE001 - said, as the error
            return ("error", f"could not read the catalogue:"
                             f" {self._first_line(e)}", {})

    def _columns_result(self, db):
        status, detail, tables = self._by_table(
            db, "tables' columns", self._column_facts)
        return Result("deep", f"{db} columns", status, detail,
                      fix_hint="" if status == "ok" else
                      "make the target's columns what the source's are,"
                      " then check again",
                      category="structure.columns", tables=tables)

    def _keys_result(self, db):
        try:
            mine, theirs = self._keys_of("src", db), self._keys_of("dst", db)
            status, detail, tables = self._held(
                db, "tables' declared keys",
                {t: tuple(k) for t, k in mine.items()},
                {t: tuple(k) for t, k in theirs.items()},
                self._both_tables(db))
            twice = self._duplicate_keys("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            status, detail, tables, twice = (
                "error", f"could not read the catalogue:"
                         f" {self._first_line(e)}", {}, {})
        if twice:
            # a declared key is not enforced here: rows that share one are
            # on the target and nothing refused them
            tables.update({t: "diff" for t in twice})
            status = "diff"
            detail += (f"; {len(twice)} tables hold a declared key value"
                       " in more than one row ("
                       + ", ".join(f"{t}: {n}" for t, n in
                                   sorted(twice.items())[:4])
                       + (" ..." if len(twice) > 4 else "") + ")")
        return Result("deep", f"{db} keys", status, detail,
                      fix_hint="" if status == "ok" else
                      "a warehouse enforces no key: remove the repeated"
                      " rows, or key the table on columns that never repeat",
                      category="structure.keys", tables=tables)

    def _duplicate_source_keys(self, db):
        """Before the move: a key the source declares and does not enforce
        may be held by more than one row, which a merge by that key either
        refuses or collapses, and a target that enforces it refuses."""
        try:
            twice = self._duplicate_keys("src", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return Result("deep", f"{db} source keys", "error",
                          f"could not read the source: {self._first_line(e)}",
                          category="structure.keys")
        if not twice:
            return Result("deep", f"{db} source keys", "ok",
                          "no declared key of the source is held by more"
                          " than one row", category="structure.keys")
        return Result(
            "deep", f"{db} source keys", "diff",
            f"{len(twice)} tables hold a declared key value in more than"
            " one row (" + ", ".join(f"{t}: {n}" for t, n in
                                     sorted(twice.items())[:4])
            + (" ..." if len(twice) > 4 else "") + ") - a merge by that key"
            " cannot place them both",
            fix_hint="remove the repeated rows at the source, or move the"
                     " table keyless", category="structure.keys",
            tables={t: "diff" for t in twice})

    def _foreign_keys_result(self, db):
        def read(side, db):
            got = {}
            for child, cols, parent, pcols in self.foreign_keys(side, db):
                got.setdefault(child, set()).add(
                    (tuple(cols), parent, tuple(pcols)))
            return got
        status, detail, tables = self._by_table(
            db, "tables' declared foreign keys", read)
        return Result("deep", f"{db} foreign keys", status, detail,
                      fix_hint="" if status == "ok" else
                      "declare the source's foreign keys on the target;"
                      " neither side enforces them",
                      category="structure.foreign-keys", tables=tables)

    def _objects_result(self, db):
        status, detail, tables = self._by_object(
            db, "view", "views", self._views_of)
        return Result("deep", f"{db} objects", status, detail,
                      fix_hint="" if status == "ok" else
                      "make the target's views what the source's are",
                      category="structure.objects", tables=tables)

    def _matviews_result(self, db):
        status, detail, tables = self._by_object(
            db, "materialized view", "materialized views", self._matviews)
        return Result("deep", f"{db} materialized views", status, detail,
                      fix_hint="" if status == "ok" else
                      "make the target's materialized views what the"
                      " source's are, and refresh a stale one",
                      category="structure.materialized-views",
                      tables=tables)

    def _options_result(self, db):
        status, detail, tables = self._by_table(
            db, "tables' options", self._table_options)
        return Result("deep", f"{db} table options", status, detail,
                      fix_hint="" if status == "ok" else
                      "make the target's tables what the source's are in"
                      " these", category="structure.collection-options",
                      tables=tables)

    def _policies_result(self, db):
        status, detail, tables = self._by_table(
            db, "tables' row policies", self._policies)
        return Result("deep", f"{db} row policies", status, detail,
                      fix_hint="" if status == "ok" else
                      "attach the source's row policies to the target's"
                      " tables", category="structure.row-security",
                      tables=tables)

    def _grants_result(self, db):
        status, detail, tables = self._by_table(
            db, "tables' grants", self._grants_of)
        return Result("deep", f"{db} grants", status, detail,
                      fix_hint="" if status == "ok" else
                      "grant on the target what the source's tables grant",
                      category="access.table-grants", tables=tables)

    def _ownership_result(self, db):
        status, detail, tables = self._by_table(
            db, "tables' owners", self._owners)
        return Result("deep", f"{db} ownership", status, detail,
                      fix_hint="" if status == "ok" else
                      "give the target's tables the owners the source's"
                      " have", category="access.object-ownership",
                      tables=tables)

    def _boundary_result(self, db):
        """The largest value of each single whole-number key, held to the
        source's: a target's that is past it holds a row the source never
        had - a writer on the target, or a batch applied twice."""
        from .. import canon
        try:
            keys = self._keys_of("src", db)
            there = {t for t in self.neutral_tables("dst", db)}
            both = []
            for t in self.neutral_tables("src", db):
                key = keys.get(t, [])
                declared = dict(self.neutral_columns("src", db, t))
                if t in there and len(key) == 1 and canon.type_class(
                        self.CANON_ENGINE, declared.get(key[0])) == "integer":
                    both.append((t, key[0]))

            def tops(side):
                return {t: self._rows(
                    side, db, f"select max({self._q(c)}) from"
                              f" {self._qualified(side, db, t)}")[0][0]
                    for t, c in both}
            mine, theirs = tops("src"), tops("dst")
        except Exception as e:  # noqa: BLE001 - said, as the error
            return Result("deep", f"{db} boundary", "error",
                          f"could not read the keys' largest values:"
                          f" {self._first_line(e)}",
                          category="movement.target-ahead")
        ahead = [t for t, _ in both if theirs[t] is not None and (
            mine[t] is None or int(theirs[t]) > int(mine[t]))]
        if ahead:
            return Result(
                "deep", f"{db} boundary", "diff",
                f"the target's largest key is past the source's on"
                f" {len(ahead)} tables: " + "; ".join(
                    f"{t} source {mine[t]}, target {theirs[t]}"
                    for t in ahead[:4]),
                fix_hint="rows landing on the target, or a batch applied"
                         " twice: find the writer before cutover",
                category="movement.target-ahead",
                tables={t: ("diff" if t in ahead else "ok")
                        for t, _ in both})
        return Result("deep", f"{db} boundary", "ok",
                      f"the largest key of {len(both)} tables is not past"
                      " the source's" if both else
                      "no table with one whole-number key to look at",
                      category="movement.target-ahead",
                      tables={t: "ok" for t, _ in both})

    def _deep_extra(self, db):
        """What only one warehouse's catalogue has to say."""
        return []

    def check_deep(self, db):
        """The two sides' catalogues held to each other for the tables and
        objects the hop takes - what a move leaves wrong without an error,
        since a warehouse enforces no key and keeps no index to refuse it:
        declared keys, and a key more than one row holds; columns, their
        types and defaults; foreign keys; views and materialized views;
        the options a table was made with; row policies; grants; owners."""
        return [self._keys_result(db), self._boundary_result(db),
                self._columns_result(db),
                self._foreign_keys_result(db), self._objects_result(db),
                self._matviews_result(db), self._options_result(db),
                self._policies_result(db), self._grants_result(db),
                self._ownership_result(db)] + self._deep_extra(db)

    #: what `assess` asks before anything moves
    PREFLIGHT = ("_duplicate_source_keys",)

    # ---- what the catalogue says of a table, for another engine to be given -

    def foreign_keys(self, side, db):
        """[(child, [columns], parent, [parent's columns])] - declared and
        never enforced, so the copy's order is all they are for."""
        return []

    def neutral_column_rules(self, side, db, table):
        """{column: {"null", "default", "identity"}} from the catalogue.
        A column an identity numbers has its default left to the
        identity."""
        out = {}
        for name, null, default, identity in self._rows(
                side, db, f"select column_name, is_nullable, column_default,"
                          f" {self._identity_column()} from"
                          f" {self._catalog(side, db, 'columns')} where"
                          f" table_schema = {MARK} and table_name = {MARK}"
                          " order by ordinal_position",
                (self._schema(side, db), self._stored(table))):
            numbered = self._numbered(identity, default)
            out[self._said(name)] = {
                "null": str(null).upper() != "NO",
                "default": None if numbered or default in (None, "")
                else str(default),
                "identity": numbered}
        return out

    def _identity_column(self):
        """The expression telling a column an identity numbers."""
        return "null"

    @staticmethod
    def _numbered(identity, default):
        return str(identity or "").upper() in ("YES", "Y", "T", "TRUE")

    #: the dialect sqlglot reads this warehouse's view text as
    VIEW_DIALECT = None

    def _view_select(self, text):
        """A view's definition as its select: the statement the catalogue
        keeps is `create view ... as <select>` on some, the select alone
        on others."""
        import sqlglot
        from sqlglot import exp
        try:
            tree = sqlglot.parse_one(str(text), read=self.VIEW_DIALECT)
            body = tree.expression if isinstance(tree, exp.Create) else tree
            return body.sql(self.VIEW_DIALECT)
        except Exception:  # noqa: BLE001 - handed on as written
            return str(text).strip().rstrip(";")

    def neutral_views(self, side, db):
        """[(name, the view's select)] - the views the hop takes."""
        out = []
        for name, definition in self._view_rows(side, db):
            if self.takes(db, "view", self._said(name)):
                out.append((self._said(name), self._view_select(definition)))
        return out

    def _view_rows(self, side, db):
        return self._rows(
            side, db, "select table_name, view_definition from"
                      f" {self._catalog(side, db, 'views')} where"
                      f" table_schema = {MARK} order by 1",
            (self._schema(side, db),))

    # ---- the servers, for `assess` ---------------------------------------------

    def _banner(self, side, db):
        """What the server says of itself."""
        raise NotImplementedError

    def _release(self, banner):
        """The warehouse's own release number in what it says of itself."""
        return str(banner or "").strip() or None

    def _banners(self):
        """(source's, target's) as each says it, None for a side that would
        not."""
        out = []
        for side in ("src", "dst"):
            try:
                db = (self.databases() or [None])[0]
                out.append(self._banner(side, db))
            except Exception:  # noqa: BLE001 - unknown, said as such
                out.append(None)
        return tuple(out)

    def _server_versions(self):
        return tuple(self._release(b) if b else None for b in self._banners())

    def _brand_probes(self):
        out = []
        for banner in self._banners():
            release = self._release(banner) if banner else None
            out.append({self.BRAND_FIELD: release} if release else {})
        return tuple(out)

    #: the field of what a server says that only this warehouse says
    BRAND_FIELD = "release"

    # ---- what the move will not do ----------------------------------------------

    def _handwork_probes(self):
        """[(kind of work, what, kind of object the hop's `objects` names it
        by, reader)] - each reader gives the names of what the warehouse
        keeps that no copy of its tables carries (`handwork.KINDS`)."""
        return []

    def _handwork(self, side="src"):
        from .. import handwork
        inv = handwork.Inventory()
        for db in self.databases():
            try:
                keys = self._keys_of(side, db)
                inv.add("no-row-key", db, "tables",
                        [t for t in self.neutral_tables(side, db)
                         if not keys.get(t)])
            except Exception as e:  # noqa: BLE001 - unknown, not zero
                inv.unknown("no-row-key", db, "cannot read the declared"
                            f" keys: {self._first_line(e)}")
            for kind, what, taken, read in self._handwork_probes():
                try:
                    inv.add(kind, db, what, [
                        n for n in read(side, db)
                        if taken is None or self.takes(db, taken, n)])
                except Exception as e:  # noqa: BLE001 - unknown, not zero
                    inv.unknown(kind, db, f"cannot list {what}:"
                                          f" {self._first_line(e)}")
        return inv



class RedshiftEngine(_CatalogWarehouse):
    """Amazon Redshift, through the PostgreSQL driver it speaks to, or
    through its own driver where the endpoint signs in by IAM (`iam: true`,
    with `cluster_identifier` or `serverless_work_group`, `region`).

    `stage: s3://bucket/prefix` and `iam_role` (the role Redshift reads the
    bucket as) let batches be copied in from Parquet files; `stage_region`
    and `stage_endpoint_url` where the bucket is not in the machine's own
    region or is S3-compatible storage."""
    CANON_ENGINE = "redshift"
    DIGEST_DIALECT = "redshift"

    def _connect(self, side, db):
        ep = self._endpoint(side)
        opts = ep.options or {}
        if opts.get("iam"):
            import redshift_connector
            kw = {k: opts[k] for k in ("cluster_identifier",
                                        "serverless_work_group", "region",
                                        "profile", "db_user")
                  if opts.get(k)}
            conn = redshift_connector.connect(
                iam=True, host=ep.host or None, port=ep.port or 5439,
                database=self._database(side), **kw)
        else:
            import psycopg2
            conn = psycopg2.connect(host=ep.host, port=ep.port or 5439,
                                    user=ep.user, password=ep.password,
                                    dbname=self._database(side),
                                    connect_timeout=15, **ep.libpq_tls())
        # instants rendered as UTC shows them, as the other engines' are
        cur = conn.cursor()
        cur.execute("set timezone to 'UTC'")
        conn.commit()
        return conn

    def side_database(self, side, db):
        """The database the connections go to, and the hop's schema in
        it: two schemas of one database are two targets."""
        return f"{self._database(side)}.{self._d(side, db)}"

    def server_identity(self, side, db):
        """What the cluster says of itself (`interlock`): its version, the
        schema's size by its own table list, and whether the schema is
        there yet."""
        schema = self._d(side, db)
        version, name = self._rows(side, db,
                                   "select version(), current_database()")[0]
        out = {"version": str(version or ""), "name": str(name or "")}
        out["absent"] = not int(self._rows(
            side, db, f"select count(*) from pg_namespace where nspname ="
                      f" {MARK}", (schema,))[0][0])
        try:
            # a table's size in megabyte blocks, where the cluster keeps it
            out["bytes"] = int(self._rows(
                side, db, "select coalesce(sum(size), 0) * 1048576 from"
                          f" svv_table_info where schema = {MARK}",
                (schema,))[0][0])
        except Exception:  # noqa: BLE001 - not a cluster's catalogue
            pass
        return out

    # ---- the catalogue, as Redshift keeps it ----------------------------------

    BRAND_FAMILY = "redshift"
    BRAND_FIELD = "redshift_release"
    VIEW_DIALECT = "redshift"

    def _banner(self, side, db):
        return self._rows(side, db, "select version()")[0][0]

    def _release(self, banner):
        """`Redshift 1.0.x` in the banner PostgreSQL 8's version string
        carries."""
        import re
        got = re.search(r"Redshift\s+([0-9][0-9.]*)", str(banner or ""))
        return got.group(1) if got else None

    def _default_text(self, side, db, default):
        """An identity's default names the table's object id, which is the
        cluster's own: it is said as the identity it is."""
        if default in (None, ""):
            return None
        return "identity" if str(default).startswith('"identity"') \
            else str(default)

    def _identity_column(self):
        return "coalesce(position('\"identity\"' in column_default), 0) = 1"

    def foreign_keys(self, side, db):
        """From the constraint's own text (`pg_get_constraintdef`):
        `FOREIGN KEY (a, b) REFERENCES parent(x, y)`."""
        import re

        def names(text):
            return [n.strip().strip('"') for n in text.split(",")]
        out = []
        for child, text in self._named(db, self._rows(
                side, db, "select cl.relname, pg_get_constraintdef(c.oid)"
                          " from pg_constraint c join pg_class cl on cl.oid"
                          " = c.conrelid join pg_namespace n on n.oid ="
                          " cl.relnamespace where c.contype = 'f' and"
                          f" n.nspname = {MARK} order by cl.relname,"
                          " c.conname", (self._schema(side, db),))):
            got = re.match(r"\s*FOREIGN KEY\s*\((.*?)\)\s*REFERENCES\s+"
                           r"(\S+?)\s*\((.*?)\)", str(text), re.I | re.S)
            if got:
                out.append((child, names(got.group(1)),
                            names(got.group(2).split(".")[-1])[0],
                            names(got.group(3))))
        return out

    def _view_rows(self, side, db):
        return self._rows(
            side, db, "select viewname, definition from pg_views where"
                      f" schemaname = {MARK} order by 1",
            (self._schema(side, db),))

    def _matviews(self, side, db):
        return {self._said(n): ("stale" if str(stale).lower() in ("t", "true")
                                else "fresh")
                for n, stale in self._rows(
                    side, db, "select name, is_stale from stv_mv_info where"
                              f' "schema" = {MARK}', (self._schema(side, db),))
                if self.takes(db, "materialized view", self._said(n))}

    def _distribution(self, side, db):
        """{table: how the cluster spreads its rows}: `KEY(col)`, `EVEN`,
        `ALL`, or `AUTO` - which the cluster changes as the table grows,
        so it is not held against the other side's."""
        out = {}
        for t, style in self._named(db, self._rows(
                side, db, 'select "table", diststyle from svv_table_info'
                          f' where "schema" = {MARK}',
                (self._schema(side, db),))):
            style = str(style or "")
            out[t] = "AUTO" if style.upper().startswith("AUTO") else style
        return out

    def _table_options(self, side, db):
        """{table: (its sort key, the number of its sort columns, whether
        its columns are encoded)}."""
        return {t: (str(key or ""), int(num or 0), str(enc or ""))
                for t, key, num, enc in self._named(db, self._rows(
                    side, db, 'select "table", sortkey1, sortkey_num, encoded'
                              f' from svv_table_info where "schema" = {MARK}',
                    (self._schema(side, db),)))}

    def _owners(self, side, db):
        return {t: str(o) for t, o in self._named(db, self._rows(
            side, db, "select tablename, tableowner from pg_tables where"
                      f" schemaname = {MARK}", (self._schema(side, db),)))}

    def _policies(self, side, db):
        """{table: {policy}} from the views that list what is attached:
        row-level security and dynamic data masking. The views name their
        columns by version, so the columns are found by name."""
        out, errors = {}, []
        for view in ("svv_rls_attached_policy", "svv_attached_masking_policy"):
            try:
                rows = self._listing(side, db, f"select * from {view}")
            except Exception as e:  # noqa: BLE001 - the other view, then
                errors.append(e)
                continue
            for row in rows:
                table = next((row[k] for k in ("relname", "relation_name",
                                               "table_name") if k in row),
                             None)
                policy = next((row[k] for k in ("policyname", "policy_name")
                               if k in row), None)
                schema = next((row[k] for k in ("relschema", "schema_name",
                                                "schemaname", "table_schema")
                               if k in row), self._schema(side, db))
                if table is None or policy is None:
                    raise ValueError(f"{view} names no table and policy")
                if str(schema) == self._schema(side, db) and self.in_scope(
                        db, self._said(str(table))):
                    out.setdefault(self._said(str(table)), set()).add(
                        (view, str(policy)))
        if len(errors) == 2:
            raise errors[0]
        return out

    def _deep_extra(self, db):
        """How the cluster spreads each table's rows, held to the source's:
        a table spread by a column on one side and evenly on the other is
        read and joined at another cost."""
        status, detail, tables = self._by_table(
            db, "tables' distribution", self._distribution)
        return [Result("deep", f"{db} distribution", status, detail,
                       fix_hint="" if status == "ok" else
                       "alter the target's tables to the source's"
                       " distribution style and key",
                       category="structure.sharding", tables=tables)]

    def free_bytes(self, side, db):
        """What the cluster's disks have free, from its partitions (a
        provisioned cluster's; Serverless keeps no disk to run out of and
        answers nothing)."""
        try:
            got = self._rows(side, db, "select sum(capacity) - sum(used)"
                                       " from stv_partitions where part_begin"
                                       " = 0")
        except Exception:  # noqa: BLE001 - not a cluster's catalogue
            return None
        # the views count in megabyte blocks
        return int(got[0][0]) * 1048576 if got and got[0][0] is not None \
            else None

    def _handwork_probes(self):
        def routines(kind):
            def read(side, db):
                return [self._said(n) for (n,) in self._rows(
                    side, db, "select p.proname from pg_proc_info p join"
                              " pg_namespace n on n.oid = p.pronamespace"
                              f" where n.nspname = {MARK} and p.prokind ="
                              f" {MARK} order by 1",
                    (self._schema(side, db), kind))]
            return read
        return [
            ("server-side-code", "functions", "function", routines("f")),
            ("server-side-code", "procedures", "procedure", routines("p")),
            ("not-carried", "materialized views", "materialized view",
             lambda side, db: list(self._matviews(side, db))),
            ("not-carried", "datashares", None,
             lambda side, db: [n for (n,) in self._rows(
                 side, db, "select share_name from svv_datashares where"
                           " share_type = 'OUTBOUND' order by 1")]),
            ("target-prereq", "external schemas", None,
             lambda side, db: [n for (n,) in self._rows(
                 side, db, "select schemaname from svv_external_schemas"
                           " order by 1")])]

    # ---- the stage --------------------------------------------------------

    def _stage(self, side):
        """(bucket, prefix, role) where the endpoint names an S3 stage the
        cluster reads, else None."""
        opts = self._endpoint(side).options or {}
        url = str(opts.get("stage") or "")
        role = opts.get("iam_role")
        if not url.startswith("s3://") or not role:
            return None
        bucket, _, prefix = url[5:].partition("/")
        return bucket, prefix.strip("/"), str(role)

    def _s3(self, side):
        import boto3
        opts = self._endpoint(side).options or {}
        kw = {}
        if opts.get("stage_region"):
            kw["region_name"] = opts.get("stage_region")
        if opts.get("stage_endpoint_url"):
            kw["endpoint_url"] = opts.get("stage_endpoint_url")
        return boto3.client("s3", **kw)

    def _slices(self, cur):
        """The cluster's slices: a COPY reads one file a slice at once, so
        a batch is cut into that many files."""
        known = self.__dict__.get("_slice_count")
        if known is None:
            cur.execute("select count(*) from stv_slices")
            known = self.__dict__["_slice_count"] = max(
                int(cur.fetchone()[0] or 1), 1)
        return known

    #: Redshift's declared types, as the Parquet a COPY maps onto them
    @staticmethod
    def _arrow_type(declared):
        import pyarrow as pa
        base, nums = warehouse_sql._base(declared)
        base = base.lower()
        if base in ("smallint", "int2"):
            return pa.int16()
        if base in ("integer", "int", "int4"):
            return pa.int32()
        if base in ("bigint", "int8"):
            return pa.int64()
        if base in ("numeric", "decimal") and len(nums) == 2:
            return pa.decimal128(*nums)
        if base in ("real", "float4"):
            return pa.float32()
        if base in ("double precision", "float8", "float"):
            return pa.float64()
        if base in ("boolean", "bool"):
            return pa.bool_()
        if base in ("character varying", "varchar", "character", "char",
                    "text", "bpchar", "nvarchar"):
            return pa.string()
        if base == "date":
            return pa.date32()
        if base in ("timestamp without time zone", "timestamp"):
            return pa.timestamp("us")
        if base in ("timestamp with time zone", "timestamptz"):
            return pa.timestamp("us", tz="UTC")
        if base in ("binary varying", "varbyte", "varbinary"):
            return pa.binary()
        return None

    #: how a batch reaches its temporary stage: a COPY has seconds of fixed
    #: cost and is then the fastest there is; statements cost little to
    #: start. Tried once each for a table's batches of a size, then the one
    #: that did a row in less time (`Engine._cheaper`)
    STAGE_WAYS = (
        decide.Rung("copy", "a COPY from Parquet files in S3, a file a"
                            " slice",
                    # files in the bucket while the batch is loaded
                    footprint=1),
        decide.Rung("insert", "insert statements of a page of rows each"),
    )

    def _copy_unusable(self, side, db, table):
        """Why a COPY cannot stage this table's batches, or None."""
        if self._stage(side) is None:
            return ("the target names no S3 stage and role for a COPY to"
                    " read (stage, iam_role)")
        odd = [f"{n} ({t})" for n, t in self.neutral_columns(side, db, table)
               if self._arrow_type(t) is None]
        if odd:
            return (f"{', '.join(odd[:4])} "
                    + ("has" if len(odd) == 1 else "have")
                    + " no Parquet type a COPY maps onto")
        return None

    def _stage_way(self, side, db, table, rows):
        """("copy" | "insert", the work key it is timed under or None)."""
        if self._copy_unusable(side, db, table):
            return "insert", None
        key = self._work_key("stage", str(table), rows)
        return self._cheaper(key, self.STAGE_WAYS), key

    def _files(self, side, db, table, names, rows, fp, cur, extra=()):
        """The batch as Parquet files in the stage, one a slice, the stage
        table's columns in its order - the target's, then `extra` - as a
        COPY maps Parquet by position: the S3 prefix they are under."""
        import io

        import pyarrow as pa
        import pyarrow.parquet as pq
        bucket, prefix, _ = self._stage(side)
        types = [(n, self._arrow_type(t))
                 for n, t in self.neutral_columns(side, db, table)]
        types += [(n, pa.string()) for n, _ in extra]
        at = {n: i for i, n in enumerate(names)}
        # as many files as slices, of rows that differ by one at most:
        # Redshift gives each slice a file and does not look at their sizes
        k, n = min(self._slices(cur), len(rows)), len(rows)
        where = "/".join(p for p in (prefix, self.hop.name,
                                     self._leaf(table), fp) if p)
        s3 = self._s3(side)
        for i in range(k):
            part = rows[i * n // k:(i + 1) * n // k]
            arrays = [arrow_array(typ, [r[at[c]] if c in at else None
                                        for r in part])
                      for c, typ in types]
            body = io.BytesIO()
            pq.write_table(pa.Table.from_arrays(
                arrays, names=[c for c, _ in types]), body)
            s3.put_object(Bucket=bucket, Key=f"{where}/part-{i:04d}.parquet",
                          Body=body.getvalue())
        return f"s3://{bucket}/{where}/"

    def _drop_files(self, side, url):
        bucket, _, prefix = url[5:].partition("/")
        s3 = self._s3(side)
        got = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for o in got.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=o["Key"])

    def _staged(self, side, db, cur, table, names, rows, fp, way,
                extra=()):
        """A temporary table holding the batch, made in this transaction:
        the target's columns (and `extra`, `(name, type)`), filled by a
        COPY from the stage's files or by one insert statement a page
        (`way`). Returns (stage, the files' prefix or None)."""
        from psycopg2 import extras

        from .. import canon
        stage = self._q(f"{OWN}stage_{self._leaf(table)}_{fp}")
        target = self._qualified(side, db, table)
        # the target's columns in its order, none of them NOT NULL: a
        # deleted row's change carries its key alone
        cur.execute(f"create temp table {stage} as select * from {target}"
                    " where 1 = 0")
        for name, typ in extra:
            cur.execute(f"alter table {stage} add column {self._q(name)}"
                        f" {typ}")
        if way == "copy":
            url = self._files(side, db, table, list(names)
                              + [n for n, _ in extra], rows, fp, cur, extra)
            cur.execute(f"copy {stage} from '{url}' iam_role"
                        f" '{self._stage(side)[2]}' format as parquet")
            return stage, url
        names = list(names) + [n for n, _ in extra]
        declared = dict(self.neutral_columns(side, db, table))

        def slot(name):
            # bytes and JSON as Redshift takes them from text: a VARBYTE
            # from its hex, a SUPER parsed
            base = warehouse_sql._base(declared.get(name))[0].lower()
            if base in ("binary varying", "varbyte", "varbinary"):
                return "to_varbyte(%s, 'hex')"
            if base == "super":
                return "json_parse(%s)"
            return "%s"

        def value(v):
            if isinstance(v, (bytes, bytearray, memoryview)):
                return bytes(v).hex()
            return canon.sql_value(v)
        extras.execute_values(
            cur, f"insert into {stage} ({', '.join(map(self._q, names))})"
                 " values %s", [tuple(value(v) for v in r) for r in rows],
            template="(" + ", ".join(slot(n) for n in names) + ")",
            page_size=1000)
        return stage, None

    def _merge_sql(self, target, stage, key, names):
        """Redshift's MERGE: one WHEN MATCHED, one WHEN NOT MATCHED, and a
        source that is a table. A row that is all key has nothing to
        update, and is inserted where it is not there."""
        on = " and ".join(f"{target}.{self._q(k)} = {stage}.{self._q(k)}"
                          for k in key)
        sets = [n for n in names if n not in key]
        cols = ", ".join(self._q(n) for n in names)
        vals = ", ".join(f"{stage}.{self._q(n)}" for n in names)
        if not sets:
            return (f"insert into {target} ({cols}) select {vals} from"
                    f" {stage} where not exists (select 1 from {target}"
                    f" where {on})")
        return (f"merge into {target} using {stage} on {on}"
                " when matched then update set " + ", ".join(
                    f"{self._q(n)} = {stage}.{self._q(n)}" for n in sets)
                + f" when not matched then insert ({cols}) values ({vals})")

    def _load_batch(self, side, db, table, columns, rows, key):
        """Staged, then merged in by the key (inserted where there is
        none), in one transaction; the stage filled the way that did a row
        in less time for batches of this size (`STAGE_WAYS`)."""
        import time
        names = [n for n, _ in columns]
        fp = self._fingerprint(table, names, rows)
        target = self._qualified(side, db, table)
        way, timed = self._stage_way(side, db, table, len(rows))
        conn = self._connect(side, db)
        url = None
        try:
            cur = conn.cursor()
            began = time.monotonic()
            stage, url = self._staged(side, db, cur, table, names, rows, fp,
                                      way)
            if timed is not None:
                # the stage's filling is what the two ways differ in
                self._took(timed, way, time.monotonic() - began, len(rows))
            cols = ", ".join(map(self._q, names))
            if key:
                cur.execute(self._merge_sql(target, stage, key, names))
            else:
                cur.execute(f"insert into {target} ({cols}) select {cols}"
                            f" from {stage}")
            cur.execute(f"drop table {stage}")
            try:
                conn.commit()
            except Exception:
                if key or not url or not self._loaded(side, db, url):
                    raise
                # the answer to the commit was lost, and the stage's files
                # are in the cluster's load history as committed: the
                # batch is there, and inserting it again would double it
        finally:
            conn.close()
            if url:
                self._drop_files(side, url)

    def _loaded(self, side, db, url):
        """Whether the files under `url` were loaded by a COPY whose
        transaction committed, asked of the cluster's own history:
        `STL_LOAD_COMMITS` where the cluster keeps it, `SYS_LOAD_DETAIL`
        on Serverless, which has no STL views."""
        asks = [
            "select count(*) from stl_load_commits c join stl_query q on"
            " q.query = c.query where trim(c.filename) like %s and"
            " q.aborted = 0 and exists (select 1 from stl_commit_stats s"
            " where s.xid = q.xid)",
            "select count(*) from sys_load_detail d join sys_load_history h"
            " on h.query_id = d.query_id where trim(d.file_name) like %s"
            " and h.status = 'completed'"]
        for sql in asks:
            try:
                got = self._rows(side, db, sql.replace("%s", MARK),
                                 (url + "%",))
            except Exception:  # noqa: BLE001 - the other view, then
                continue
            return bool(got and int(got[0][0]) > 0)
        return False

    # ---- a tail's batch ------------------------------------------------------

    def _ledger_ddl(self, side, db):
        return (f"create table if not exists"
                f" {self._qualified(side, db, self.LEDGER)} (hop varchar(256),"
                " db varchar(256), batch bigint, token varchar(65535),"
                " at timestamp)")

    def _apply_batch(self, side, db, by_table, seen):
        """One transaction: each table's changes in a temporary table a
        shape, the deleted keys taken out, the rest merged in, and the
        ledger's line for the hop written - so the batch and the line
        saying it was applied are there together or not at all."""
        import time
        n = seen["batch"] if seen else None
        conn = self._connect(side, db)
        files = []
        try:
            cur = conn.cursor()
            cur.execute(self._ledger_ddl(side, db))
            for table, rows in by_table.items():
                target = self._qualified(side, db, table)
                key = self.neutral_key(side, db, table)
                if not key:
                    raise SystemExit(f"{table} has no key on the target, and"
                                     " a change is applied by its key")
                fp = self._fingerprint(table, [], rows, str(n))
                journal = self._journal_rows(table, rows, n, fp)
                for i, (shape, names) in enumerate(
                        self._shapes(journal).items()):
                    part = [r for r, s in journal if s == shape]
                    way, timed = self._stage_way(side, db, table, len(part))
                    began = time.monotonic()
                    stage, url = self._staged(
                        side, db, cur, table, names,
                        [[r.get(c) for c in names] + [r["_migkit_op"]]
                         for r in part], f"{fp}_{i}", way,
                        extra=(("_migkit_op", "varchar(8)"),))
                    if timed is not None:
                        self._took(timed, way, time.monotonic() - began,
                                   len(part))
                    if url:
                        files.append(url)
                    on = " and ".join(f"{target}.{self._q(k)} ="
                                      f" {stage}.{self._q(k)}" for k in key)
                    cur.execute(f"delete from {target} using {stage} where"
                                f" {on} and {stage}.{self._q('_migkit_op')}"
                                " = 'delete'")
                    cur.execute(f"delete from {stage} where"
                                f" {self._q('_migkit_op')} = 'delete'")
                    cur.execute(self._merge_sql(target, stage, key, names))
                    cur.execute(f"drop table {stage}")
            if n is not None:
                ledger = self._qualified(side, db, self.LEDGER)
                cur.execute(f"delete from {ledger} where hop = %s and"
                            " db = %s", (self.hop.name, str(db)))
                cur.execute(f"insert into {ledger} (hop, db, batch, token,"
                            " at) values (%s, %s, %s, %s, getdate())",
                            (self.hop.name, str(db), int(n),
                             json.dumps(seen["token"], default=str)))
            conn.commit()
        finally:
            conn.close()
            for url in files:
                self._drop_files(side, url)


class SnowflakeEngine(FoldsToCapitals, _CatalogWarehouse):
    """Snowflake. The endpoint's `account` option is the account
    identifier; `warehouse` and `role`, where given, are used as they
    are; `private_key_file` (and `private_key_passphrase`) signs in by key
    pair, which is also what lets a tail's changes land through a
    streaming channel. Unquoted names are kept in capitals, as Oracle's
    are."""
    CANON_ENGINE = "snowflake"
    DIGEST_DIALECT = "snowflake"
    #: large values - a VARCHAR or VARIANT holds up to 128 MB, a BINARY
    #: 64 MB: sized by the server over a sample of a large table, a wide
    #: text hashed where it lies (Snowflake's MD5 hashes a string, so a
    #: binary is hashed here as it is folded); read whole, a batch cut by
    #: bytes
    LARGE_VALUES = pieces.Sql(
        "sized by the server, wide text hashed where it lies",
        length={**{t: "octet_length({c})" for t in (
                    "varchar", "text", "string", "binary", "varbinary")},
                # a semi-structured value by its JSON text, as it is carried
                **{t: "octet_length(to_json({c}))" for t in (
                    "variant", "object", "array")}},
        types=("varchar", "text", "string", "binary", "varbinary",
               "variant", "object", "array"),
        md5="md5({c})", md5_classes=("text",),
        sample="sample system ({pct})")
    #: a CHAR is a VARCHAR here, kept without padding
    PADDED = ()
    SCALED = ("NUMBER", "DECIMAL", "NUMERIC")
    #: seconds a channel's rows are waited for before the batch stops
    COMMIT_WAIT = 300

    def _nanos_text(self, quoted, declared):
        """A timestamp or a time as Snowflake writes it with all nine of
        its digits - nine unless declared fewer - where the driver hands
        it over as a Python datetime or time, six. An instant
        (`TIMESTAMP_LTZ`, `TIMESTAMP_TZ`) as its UTC reading, as every
        engine here renders one."""
        from .. import canon
        name, nums = canon._split_declared(declared)
        if (nums[0] if nums else 9) <= 6:
            return None
        if name == "time":
            return f"to_varchar({quoted}, 'HH24:MI:SS.FF9')"
        if name == "timestamp_ntz":
            return f"to_varchar({quoted}, 'YYYY-MM-DD HH24:MI:SS.FF9')"
        if name in ("timestamp_ltz", "timestamp_tz"):
            return (f"to_varchar(convert_timezone('UTC', {quoted}),"
                    " 'YYYY-MM-DD HH24:MI:SS.FF9')")
        return None

    def _connect(self, side, db):
        try:
            import snowflake.connector
        except ImportError:
            raise SystemExit("the Snowflake driver is not installed:"
                             " run migkit doctor --install")
        ep = self._endpoint(side)
        opts = ep.options or {}
        if not opts.get("account"):
            raise SystemExit(f"the {'source' if side == 'src' else 'target'}"
                             " needs `account`, the Snowflake account"
                             " identifier")
        kw = {k: opts[k] for k in ("warehouse", "role", "authenticator")
              if opts.get(k)}
        if opts.get("private_key_file"):
            kw["private_key_file"] = opts["private_key_file"]
            if opts.get("private_key_passphrase"):
                kw["private_key_file_pwd"] = ep.option_secret(
                    "private_key_passphrase")
        else:
            kw["password"] = ep.password
        conn = snowflake.connector.connect(
            account=opts["account"], user=ep.user,
            database=self._database(side), login_timeout=15, **kw)
        conn.cursor().execute("alter session set timezone = 'UTC'")
        return conn

    #: how a tail's batch lands in its journal: a channel where the side
    #: signs in by key pair (its offset token is the batch's, and it is the
    #: only exactly-once ingest Snowflake has), a staged file named for the
    #: batch - which Snowflake's load history copies once - where it does
    #: not
    LAND_WAYS = (
        decide.Rung("channel", "a streaming channel whose offset token is"
                               " the batch's",
                    needs=(decide.Need(
                        "key-pair sign-in", lambda f: f["key pair"],
                        "{rung} signs in by key pair alone, and the target"
                        " signs in by password"),),
                    because="the channel's committed token says whether the"
                            " batch landed"),
        decide.Rung("staged file", "a staged file named for the batch,"
                                   " copied once",
                    # the file in the journal's stage until it is copied
                    footprint=1),
    )

    def side_database(self, side, db):
        """The account, the database the connections go to, and the hop's
        schema in it."""
        opts = self._endpoint(side).options or {}
        return (f"{opts.get('account')}/{self._database(side)}."
                f"{self._owner(side, db)}")

    def server_identity(self, side, db):
        """What the account says of itself (`interlock`): its locator and
        region, its version, the schema's size and whether it is there."""
        account, region, version = self._rows(
            side, db, "select current_account(), current_region(),"
                      " current_version()")[0]
        out = {"id": f"{account}@{region}", "version": str(version or "")}
        schema = self._owner(side, db)
        out["absent"] = not int(self._rows(
            side, db, "select count(*) from information_schema.schemata"
                      f" where schema_name = {MARK}", (schema,))[0][0])
        out["bytes"] = int(self._rows(
            side, db, "select coalesce(sum(bytes), 0) from"
                      f" information_schema.tables where table_schema ="
                      f" {MARK}", (schema,))[0][0])
        return out

    def _table_names(self, side, db):
        return [self._said(n) for (n,) in self._rows(
            side, db, "select table_name from information_schema.tables"
                      f" where table_schema = {MARK} and table_type ="
                      f" {MARK} order by 1",
            (self._owner(side, db), self.BASE_TABLE))]

    def neutral_columns(self, side, db, table):
        """`NUMBER` at scale 0 is Snowflake's integer, and its driver
        returns it as one."""
        out = []
        for n, t in self._rows(
                side, db, f"select column_name, {DECLARED} from"
                          " information_schema.columns where table_schema ="
                          f" {MARK} and table_name = {MARK}"
                          " order by ordinal_position",
                (self._owner(side, db), self._q(table)[1:-1])):
            if t.upper().startswith("NUMBER(") and t.endswith(",0)"):
                t = "INTEGER"
            out.append((self._said(n), t))
        return out

    def neutral_key(self, side, db, table):
        """From `show primary keys`: Snowflake's `information_schema` has
        no key columns."""
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute(f"show primary keys in table"
                        f" {self._qualified(side, db, table)}")
            names = [c[0] for c in cur.description]
            rows = cur.fetchall()
        finally:
            conn.close()
        at, seq = names.index("column_name"), names.index("key_sequence")
        return [self._said(r[at]) for r in sorted(rows, key=lambda r: r[seq])]

    def _fix_read(self, side, db, table, columns, rows):
        return self._unpad(side, db, table, columns, rows)

    # ---- the catalogue, as Snowflake keeps it ---------------------------------

    BRAND_FAMILY = "snowflake"
    BRAND_FIELD = "snowflake_release"
    VIEW_DIALECT = "snowflake"

    def _schema(self, side, db):
        return self._owner(side, db)

    def _stored(self, table):
        return self._q(table)[1:-1]

    def _banner(self, side, db):
        return self._rows(side, db, "select current_version()")[0][0]

    def _default_text(self, side, db, default):
        """A default that draws from a sequence names the database and the
        schema the sequence is in: said without them, so a schema moved
        under another name is not a column that differs."""
        import re
        if default in (None, ""):
            return None
        text = str(default)
        for name, word in ((self._database(side), "<database>"),
                           (self._schema(side, db), "<schema>")):
            text = re.sub(re.escape(name), word, text, flags=re.I)
        return text

    def _identity_column(self):
        return "is_identity"

    def _show(self, side, db, what):
        """The rows of `show <what> in schema`, as {column: value}."""
        return self._listing(side, db, f"show {what} in schema"
                                      f" {self._q(self._schema(side, db))}")

    def _keys_of(self, side, db):
        got = {}
        for row in sorted(self._show(side, db, "primary keys"),
                          key=lambda r: (str(r["table_name"]),
                                         int(r["key_sequence"]))):
            t = self._said(row["table_name"])
            if self.in_scope(db, t):
                got.setdefault(t, []).append(self._said(row["column_name"]))
        return got

    def foreign_keys(self, side, db):
        """From `show imported keys`: a row a column of a key, in the
        order of the key."""
        keys = {}
        for row in sorted(self._show(side, db, "imported keys"),
                          key=lambda r: (str(r["fk_table_name"]),
                                         str(r["fk_name"]),
                                         int(r["key_sequence"]))):
            child = self._said(row["fk_table_name"])
            if not self.in_scope(db, child):
                continue
            one = keys.setdefault((child, str(row["fk_name"])),
                                  (self._said(row["pk_table_name"]), [], []))
            one[1].append(self._said(row["fk_column_name"]))
            one[2].append(self._said(row["pk_column_name"]))
        return [(child, cols, parent, pcols)
                for (child, _), (parent, cols, pcols) in keys.items()]

    def _matviews(self, side, db):
        return {self._said(r["name"]): (
            "invalid" if str(r.get("invalid")).lower() == "true" else "valid")
            for r in self._show(side, db, "materialized views")
            if self.takes(db, "materialized view", self._said(r["name"]))}

    def _table_options(self, side, db):
        """{table: (its kind - permanent, transient - its clustering key,
        its time travel in days, whether it tracks changes)}."""
        return {t: (str(kind), str(cluster or ""), str(days),
                    str(tracking or ""))
                for t, kind, cluster, days, tracking in self._named(db, [
                    [r["name"], r.get("kind"), r.get("cluster_by"),
                     r.get("retention_time"), r.get("change_tracking")]
                    for r in self._show(side, db, "tables")])}

    def _owners(self, side, db):
        return {t: str(o) for t, o in self._named(db, self._rows(
            side, db, "select table_name, table_owner from"
                      " information_schema.tables where table_schema ="
                      f" {MARK} and table_type = {MARK}",
            (self._schema(side, db), self.BASE_TABLE)))}

    def _policies(self, side, db):
        """{table: {(kind, policy)}}: the masking and row access policies
        each table is under, asked of the table function that lists what a
        policy is on."""
        out = {}
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            for t in self.neutral_tables(side, db):
                cur.execute(
                    "select policy_kind, policy_name from table("
                    "information_schema.policy_references(ref_entity_name"
                    f" => {MARK}, ref_entity_domain => 'TABLE'))",
                    (f"{self._q(self._database(side))}."
                     f"{self._qualified(side, db, t)}",))
                got = {(str(k), str(n)) for k, n in cur.fetchall()}
                if got:
                    out[t] = got
        finally:
            conn.close()
        return out

    def _sequences(self, side, db):
        """{sequence: (the value it hands out next, its step)}."""
        return {self._said(r["name"]): (int(r["next_value"]),
                                         int(r.get("interval") or 1))
                for r in self._show(side, db, "sequences")}

    def capacity(self, side, db):
        """The queries running now on the warehouse the session uses."""
        name = self._rows(side, db, "select current_warehouse()")[0][0]
        if not name:
            return {}
        got = self._listing(side, db, f"show warehouses like {MARK}",
                            (name,))
        return {"running": int(got[0].get("running") or 0)} if got else {}

    def _sequence_grants(self, side, db):
        """{sequence: {(grantee, privilege)}}."""
        got = {}
        for name, who, what in self._named(db, self._rows(
                side, db, "select object_name, grantee, privilege_type from"
                          " information_schema.object_privileges where"
                          f" object_schema = {MARK} and object_type ="
                          " 'SEQUENCE' order by 1, 2, 3",
                (self._schema(side, db),))):
            if self.takes(db, "sequence", name):
                got.setdefault(name, set()).add((str(who), str(what)))
        return got

    def _sequence_grants_result(self, db):
        try:
            mine, theirs = (self._sequence_grants("src", db),
                            self._sequence_grants("dst", db))
            status, detail, _ = self._held(
                db, "sequences' grants", mine, theirs,
                set(mine) | set(theirs))
        except Exception as e:  # noqa: BLE001 - said, as the error
            status, detail = ("error", f"could not read the catalogue:"
                                       f" {self._first_line(e)}")
        return Result("deep", f"{db} seq-grants", status, detail,
                      fix_hint="" if status == "ok" else
                      "grant on the target's sequences what the source's"
                      " grant", category="access.sequence-grants")

    def _deep_extra(self, db):
        """Each sequence the hop takes, held to the source's: a target's
        that would hand out a value the source's already has gives a key a
        row holds. Raising one is the operator's - Snowflake sets a
        sequence's next value only by making it again."""
        try:
            mine, theirs = (self._sequences("src", db),
                            self._sequences("dst", db))
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} parity", "error",
                           f"could not read the sequences:"
                           f" {self._first_line(e)}",
                           category="identity.sequence-parity")]
        names = sorted(n for n in mine if self.takes(db, "sequence", n))
        behind = []
        for n in names:
            want, step = mine[n]
            got = theirs.get(n)
            if got is None:
                behind.append(f"{n} (not on the target)")
            elif (got[0] - want) * (1 if step >= 0 else -1) < 0:
                behind.append(f"{n} (next {got[0]}, the source's {want})")
        status = "diff" if behind else "ok"
        return [self._sequence_grants_result(db), Result(
            "deep", f"{db} parity", status,
            "; ".join(behind[:8]) if behind else
            f"{len(names)} sequences are at or past the source's",
            fix_hint="make the target's sequences start where the source's"
                     " are before cutover" if behind else "",
            category="identity.sequence-parity")]

    def _handwork_probes(self):
        def listed(what):
            return lambda side, db: [self._said(r["name"]) for r in
                                     self._show(side, db, what)]
        return [
            ("server-side-code", "functions", "function",
             listed("user functions")),
            ("server-side-code", "procedures", "procedure",
             listed("user procedures")),
            ("server-side-code", "tasks", None, listed("tasks")),
            ("not-carried", "streams", None, listed("streams")),
            ("not-carried", "pipes", None, listed("pipes")),
            ("not-carried", "stages", None, listed("stages")),
            ("not-carried", "sequences", "sequence", listed("sequences")),
            ("not-carried", "dynamic tables", None,
             listed("dynamic tables")),
            ("not-carried", "materialized views", "materialized view",
             lambda side, db: list(self._matviews(side, db))),
            ("not-carried", "masking policies", None,
             listed("masking policies")),
            ("not-carried", "row access policies", None,
             listed("row access policies"))]

    # ---- a batch staged in the table's own stage ----------------------------

    def _cast(self, field, declared):
        """The staged text of a column, as its declared type: every value
        is staged as text and cast here, which is exact for every type a
        Snowflake column holds."""
        base = warehouse_sql._base(declared)[0]
        if base in ("BINARY", "VARBINARY"):
            return f"TO_BINARY({field}, 'HEX')"
        if base in ("VARIANT", "OBJECT", "ARRAY"):
            return f"PARSE_JSON({field})"
        return f"{field}::{declared}"

    def _put(self, cur, side, db, table, columns, rows, name):
        """The rows as a Parquet file of text in `table`'s own stage."""
        import os
        import tempfile

        import pyarrow as pa
        import pyarrow.parquet as pq
        names = [n for n, _ in columns]
        folder = tempfile.mkdtemp(prefix="migkit-stage-")
        path = os.path.join(folder, name)
        try:
            pq.write_table(pa.Table.from_arrays(
                [pa.array([stage_text(r[i]) for r in rows], pa.string())
                 for i in range(len(names))],
                names=[self._q(n)[1:-1] for n in names]), path)
            cur.execute(f"put 'file://{path}'"
                        f" @{self._table_stage(side, db, table)}"
                        " auto_compress = false overwrite = true")
        finally:
            try:
                os.remove(path)
                os.rmdir(folder)
            except OSError:
                pass

    def _table_stage(self, side, db, table):
        """The stage every Snowflake table has of its own."""
        return f"{self._q(self._owner(side, db))}.%{self._q(table)}"

    def _copy(self, cur, side, db, into, table, columns, name, declared):
        """COPY one staged file into `into`, each column cast from its
        text. Returns the rows it loaded, 0 where Snowflake's load history
        says the file was loaded into it already."""
        names = [n for n, _ in columns]
        picks = ", ".join(
            self._cast(f'$1:"{self._q(n)[1:-1]}"::STRING', declared.get(n)
                       or "VARCHAR") for n in names)
        cur.execute(f"copy into {into} ({', '.join(map(self._q, names))})"
                    f" from (select {picks} from"
                    f" @{self._table_stage(side, db, table)})"
                    f" files = ('{name}') file_format = (type = parquet)"
                    " purge = true")
        return self._copied(cur)

    @staticmethod
    def _copied(cur):
        """Rows a COPY loaded, from its result: a row a file with its
        status and `rows_loaded`, or one saying no file was new."""
        desc = [c[0].lower() for c in (cur.description or [])]
        rows = cur.fetchall() or []
        if "rows_loaded" not in desc:
            return 0
        at, st = desc.index("rows_loaded"), desc.index("status")
        bad = [r for r in rows if str(r[st]).upper() not in
               ("LOADED", "LOAD_SKIPPED")]
        if bad:
            raise SystemExit(f"the staged file did not load: {bad[0]}")
        return sum(int(r[at] or 0) for r in rows
                   if str(r[st]).upper() == "LOADED")

    def _merge_sql(self, target, stage, key, names, ops=False):
        on = " and ".join(f"t.{self._q(k)} = s.{self._q(k)}" for k in key)
        sets = [n for n in names if n not in key]
        cols = ", ".join(self._q(n) for n in names)
        vals = ", ".join(f"s.{self._q(n)}" for n in names)
        op = self._q("_migkit_op")
        return (f"merge into {target} t using {stage} s on {on}"
                + (f" when matched and s.{op} = 'delete' then delete"
                   if ops else "")
                + (" when matched then update set " + ", ".join(
                    f"t.{self._q(n)} = s.{self._q(n)}" for n in sets)
                   if sets else "")
                + " when not matched"
                + (f" and s.{op} = 'upsert'" if ops else "")
                + f" then insert ({cols}) values ({vals})")

    def _load_batch(self, side, db, table, columns, rows, key):
        """The batch staged as one file in the table's stage and copied
        in: into a temporary table and merged by the key, or, for a table
        with no key, straight into it - where a file copied once is not
        copied again for 64 days (Snowflake's load history), so a batch
        whose commit was not heard is not loaded twice."""
        names = [n for n, _ in columns]
        fp = self._fingerprint(table, names, rows)
        name = f"migkit-{fp}.parquet"
        declared = dict(self.neutral_columns(side, db, table))
        target = self._qualified(side, db, table)
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            self._put(cur, side, db, table, columns, rows, name)
            if not key:
                self._copy(cur, side, db, target, table, columns, name,
                           declared)
                return
            stage = self._q(f"{OWN}stage_{self._leaf(table)}_{fp}")
            cur.execute(f"create temporary table {stage} like {target}")
            got = self._copy(cur, side, db, stage, table, columns, name,
                             declared)
            if got != len(rows):
                raise SystemExit(f"{table}: {got} of {len(rows)} staged rows"
                                 " loaded")
            cur.execute("begin")
            cur.execute(self._merge_sql(target, stage, key, names))
            cur.execute("commit")
            cur.execute(f"drop table if exists {stage}")
        finally:
            conn.close()

    # ---- a tail's batch --------------------------------------------------------

    def _ledger_ddl(self, side, db):
        return (f"create table if not exists"
                f" {self._qualified(side, db, self.LEDGER)} (hop varchar,"
                " db varchar, batch number(38,0), token varchar,"
                " at timestamp_ntz)")

    def _journal(self, cur, side, db, table):
        """The table's journal, made where it is not there yet: its
        columns, and the batch's own."""
        cols = self.neutral_columns(side, db, table)
        name = self._journal_name(table, cols)
        defs = ", ".join(f"{self._q(n)} {t}" for n, t in cols)
        cur.execute(f"create table if not exists"
                    f" {self._qualified(side, db, name)} ({defs},"
                    f" {self._q('_migkit_batch')} number(38,0),"
                    f" {self._q('_migkit_seq')} number(38,0),"
                    f" {self._q('_migkit_op')} varchar,"
                    f" {self._q('_migkit_fp')} varchar,"
                    f" {self._q('_migkit_cols')} varchar)")
        return name, dict(cols)

    def _channel_props(self, side):
        """What a streaming channel signs in with, or None where the
        endpoint signs in by password, which a channel does not take."""
        opts = self._endpoint(side).options or {}
        if not opts.get("private_key_file"):
            return None
        with open(opts["private_key_file"]) as fh:
            key = fh.read()
        props = {"account": opts["account"], "user": self._endpoint(side).user,
                 "private_key": key,
                 "url": opts.get("url") or
                 f"https://{opts['account']}.snowflakecomputing.com"}
        if opts.get("private_key_passphrase"):
            props["private_key_passphrase"] = self._endpoint(
                side).option_secret("private_key_passphrase")
        if opts.get("role"):
            props["role"] = opts["role"]
        return props

    def _land(self, cur, side, db, table, journal, records, n, fp):
        """The batch's journal rows landed once. Through a streaming
        channel named for the hop and table where the endpoint signs in by
        key pair: the channel's last committed offset token is `n:fp`
        when this attempt landed already, and is not appended again. As a
        staged file named for the batch otherwise, which Snowflake's load
        history copies once."""
        props = self._channel_props(side)
        token = f"{n}:{fp}"
        way = decide.climb("land a batch's changes", self.LAND_WAYS,
                           decide.Facts({"key pair": props is not None}))
        if way.path == "staged file":
            columns = [(c, None) for c in records[0]]
            name = f"migkit-{self._leaf(table)}-{n}-{fp}.parquet"
            declared = dict(self.neutral_columns(side, db, journal))
            self._put(cur, side, db, journal, columns,
                      [[r[c] for c, _ in columns] for r in records], name)
            self._copy(cur, side, db, self._qualified(side, db, journal),
                       journal, columns, name, declared)
            return
        from snowflake.ingest.streaming import StreamingIngestClient
        client = self._streaming(StreamingIngestClient, side, db, journal,
                                 props)
        channel, status = client.open_channel(
            f"migkit_{self._leaf(self.hop.name)}_{self._leaf(table)}")
        try:
            if status.latest_committed_offset_token != token:
                channel.append_rows(
                    [{self._q(c)[1:-1]: v for c, v in r.items()}
                     for r in records], start_offset_token=token,
                    end_offset_token=token)
                channel.wait_for_commit(lambda got: got == token,
                                        timeout_seconds=self.COMMIT_WAIT)
        finally:
            channel.close()

    def _streaming(self, cls, side, db, journal, props):
        """A streaming client for the journal, in table mode: its PIPE is
        the one Snowflake makes for the table itself."""
        return cls.from_table(
            f"migkit_{self._leaf(self.hop.name)}",
            self._q(self._database(side))[1:-1],
            self._q(self._owner(side, db))[1:-1],
            self._q(journal)[1:-1], properties=props)

    def _apply_batch(self, side, db, by_table, seen):
        """Each table's changes landed in its journal once, then one
        transaction merges each shape in and writes the ledger's line."""
        n = seen["batch"] if seen else None
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute(self._ledger_ddl(side, db))
            merges = []
            for table, rows in by_table.items():
                key = self.neutral_key(side, db, table)
                if not key:
                    raise SystemExit(f"{table} has no key on the target, and"
                                     " a change is applied by its key")
                journal, _ = self._journal(cur, side, db, table)
                fp = self._fingerprint(table, [], rows, str(n))
                recs = self._journal_rows(table, rows, n, fp)
                cols = [c for c, _ in self.neutral_columns(side, db, table)]
                self._land(cur, side, db, table, journal,
                           [{c: r.get(c) for c in cols
                             + ["_migkit_batch", "_migkit_seq", "_migkit_op",
                                "_migkit_fp", "_migkit_cols"]}
                            for r, _ in recs], n, fp)
                for shape, names in self._shapes(recs).items():
                    batch = ("is null" if n is None else f"= {int(n)}")
                    stage = (f"(select * from"
                             f" {self._qualified(side, db, journal)} where"
                             f" {self._q('_migkit_batch')} {batch} and"
                             f" {self._q('_migkit_fp')} = '{fp}' and"
                             f" {self._q('_migkit_cols')} = %s qualify"
                             " row_number() over (partition by"
                             f" {self._q('_migkit_seq')}) = 1)")
                    merges.append((self._merge_sql(
                        self._qualified(side, db, table), stage, key, names,
                        ops=True), (shape,)))
            cur.execute("begin")
            for sql, args in merges:
                cur.execute(sql, args)
            if n is not None:
                ledger = self._qualified(side, db, self.LEDGER)
                cur.execute(f"delete from {ledger} where hop = %s and"
                            " db = %s", (self.hop.name, str(db)))
                cur.execute(f"insert into {ledger} (hop, db, batch, token,"
                            " at) select %s, %s, %s, %s,"
                            " sysdate()", (self.hop.name, str(db), int(n),
                                           json.dumps(seen["token"],
                                                      default=str)))
            cur.execute("commit")
        finally:
            conn.close()


class BigQueryEngine(BigQueryChanges, BigQueryChecks, _Warehouse):
    """Google BigQuery. The endpoint's `database` option is the project,
    a hop's databases its datasets, and `location` the datasets' region
    where it is not the default; `credentials_file` a service account's
    key, where the machine's own sign-in is not the one to use.
    `api_endpoint` and `write_endpoint` (host:port of the Storage Write
    API) point it at a private endpoint or an emulator, and `anonymous:
    true` signs in as nobody, which only an emulator takes."""
    READS_BILLED = True
    CANON_ENGINE = "bigquery"
    #: the SQL a hop's row filter and defaults are written in and
    #: translated to (sqlglot's name for it)
    SQL_DIALECT = "bigquery"
    DIGEST_DIALECT = "bigquery"
    #: large values - a row holds up to 100 MB: sized from the free listing
    #: of rows (`row_sizes`), never a billed scan; hashed where they lie in
    #: the digest's SQL and in a fold here; read whole, a batch cut by bytes
    LARGE_VALUES = pieces.Sql(
        "sized from the free listing, wide values hashed where they lie",
        length={"string": "byte_length({c})", "bytes": "byte_length({c})",
                # a JSON value by its text, as it is carried
                "json": "byte_length(to_json_string({c}))"},
        types=("string", "bytes", "json"),
        md5="to_hex(md5({c}))", md5_classes=("bytes", "text"))
    #: load jobs a project may run in a day, failed ones counted (BigQuery's
    #: quota), and the share a move leaves for everything else there
    LOAD_JOBS_A_DAY = 100000
    LOAD_JOBS_SPARE = 0.1
    #: bytes one append request of the Storage Write API carries, under its
    #: 10 MB with room for the request's framing
    APPEND_BYTES = 8 * 1024 * 1024
    #: seconds the project's job history is trusted before it is asked again
    JOBS_ASKED_EVERY = 600

    def _client(self, side):
        known = self.__dict__.setdefault("_clients", {})
        if side in known:
            return known[side]
        try:
            from google.cloud import bigquery
        except ImportError:
            raise SystemExit("the BigQuery driver is not installed:"
                             " run migkit doctor --install")
        opts = self._endpoint(side).options or {}
        kw = {"project": self._database(side)}
        if opts.get("location"):
            kw["location"] = opts["location"]
        if opts.get("api_endpoint"):
            from google.api_core.client_options import ClientOptions
            kw["client_options"] = ClientOptions(
                api_endpoint=opts["api_endpoint"])
        kw.update(self._credentials(side))
        known[side] = bigquery.Client(**kw)
        return known[side]

    def _credentials(self, side):
        opts = self._endpoint(side).options or {}
        if opts.get("anonymous"):
            from google.auth.credentials import AnonymousCredentials
            return {"credentials": AnonymousCredentials()}
        if opts.get("credentials_file"):
            from google.oauth2 import service_account
            return {"credentials": service_account.Credentials
                    .from_service_account_file(opts["credentials_file"])}
        return {}

    def _write_client(self, side):
        known = self.__dict__.setdefault("_writers", {})
        if side in known:
            return known[side]
        from google.cloud import bigquery_storage_v1 as bqs
        opts = self._endpoint(side).options or {}
        at = opts.get("write_endpoint")
        if at and opts.get("anonymous"):
            import grpc
            from google.cloud.bigquery_storage_v1.services.big_query_write\
                .transports import BigQueryWriteGrpcTransport
            known[side] = bqs.BigQueryWriteClient(
                transport=BigQueryWriteGrpcTransport(
                    channel=grpc.insecure_channel(str(at))))
        else:
            kw = self._credentials(side)
            if at:
                from google.api_core.client_options import ClientOptions
                kw["client_options"] = ClientOptions(api_endpoint=str(at))
            known[side] = bqs.BigQueryWriteClient(**kw)
        return known[side]

    def _connect(self, side, db):
        from google.cloud.bigquery import dbapi
        return dbapi.connect(client=self._client(side))

    def _q(self, name):
        return "`" + str(name).replace("\\", "\\\\").replace("`", "\\`") + "`"

    def _catalog(self, side, db, what):
        return f"{self._q(self._d(side, db))}.INFORMATION_SCHEMA.{what.upper()}"

    def _ref(self, side, db, table):
        return f"{self._database(side)}.{self._d(side, db)}.{table}"

    @staticmethod
    def _lit(text):
        """A string literal BigQuery reads back as `text`."""
        return "'" + (str(text).replace("\\", "\\\\").replace("'", "\\'")
                      .replace("\n", "\\n").replace("\r", "\\r")) + "'"

    def neutral_create_sql(self, side, db, table, columns, key=()):
        """A key is taken only as one BigQuery does not enforce."""
        sql = super().neutral_create_sql(side, db, table, columns, key)
        return sql[:-1] + " not enforced)" if key else sql

    #: the API's older names of a type, as `information_schema` spells it
    STANDARD = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL",
                "RECORD": "STRUCT"}

    def neutral_columns(self, side, db, table):
        """The table's columns from its own metadata - one call and no
        query job - with their numbers written as `information_schema`
        writes them (`NUMERIC(10, 2)`, `STRING(20)`); none where the table
        is not there."""
        from google.api_core import exceptions
        try:
            fields = self._client(side).get_table(
                self._ref(side, db, table)).schema
        except exceptions.NotFound:
            return []
        out = []
        for f in fields:
            t = str(f.field_type).upper()
            t = self.STANDARD.get(t, t)
            if t in ("NUMERIC", "BIGNUMERIC") and f.precision is not None:
                t += f"({f.precision}, {f.scale or 0})"
            elif t in ("STRING", "BYTES") and f.max_length:
                t += f"({f.max_length})"
            if f.mode == "REPEATED":
                t = f"ARRAY<{t}>"
            out.append((f.name, t))
        return out

    def neutral_key(self, side, db, table):
        """The table's primary key as its own metadata says it - one call
        and no query job - and from `information_schema` where the
        metadata carries none."""
        try:
            got = self._client(side).get_table(
                self._ref(side, db, table)).table_constraints
            pk = got.primary_key if got is not None else None
            if pk is not None and pk.columns:
                return list(pk.columns)
        except Exception:  # noqa: BLE001 - asked of the catalogue, then
            pass
        try:
            return super().neutral_key(side, db, table)
        except Exception:  # noqa: BLE001 - a catalogue without the view
            return []

    def neutral_create(self, side, db, table, columns, key=()):
        """The table made through the API, its key given as one BigQuery
        does not enforce - what `neutral_create_sql` says, without a query
        job to run it. Returns that statement, for the plan."""
        from google.cloud import bigquery
        from google.cloud.bigquery import table as bqt

        from .. import canon
        self._target_only(side, "create a table")
        if self._leaf_name(table) in {self._leaf_name(t) for t in
                                      self.neutral_tables(side, db)}:
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        ddl = self.neutral_create_sql(side, db, table, columns, key)
        fields = []
        for col in columns:
            base, nums = warehouse_sql._base(
                canon.ddl_type("bigquery", col[1], col[2], col[0] in key))
            kw = {}
            if base in ("NUMERIC", "BIGNUMERIC") and len(nums) == 2:
                kw = {"precision": nums[0], "scale": nums[1]}
            elif base in ("STRING", "BYTES") and nums:
                kw = {"max_length": nums[0]}
            rule = (col[3] if len(col) > 3 else None) or {}
            fields.append(bigquery.SchemaField(
                col[0], base, mode="REQUIRED" if rule.get("null") is False
                else "NULLABLE", **kw))
        made = bigquery.Table(self._ref(side, db, str(table).split(".")[-1]),
                              schema=fields)
        if key:
            made.table_constraints = bqt.TableConstraints(
                primary_key=bqt.PrimaryKey(columns=list(key)),
                foreign_keys=[])
        self._client(side).create_table(made)
        return ddl

    def side_database(self, side, db):
        """The project and the hop's dataset in it: one dataset name in two
        projects is two targets."""
        return f"{self._database(side)}.{self._d(side, db)}"

    def server_identity(self, side, db):
        """What the project says of the dataset (`interlock`): whether it
        is there, and its tables' bytes by its own table list."""
        from google.api_core import exceptions
        client = self._client(side)
        out = {"id": self._database(side), "name": self._database(side)}
        try:
            client.get_dataset(f"{self._database(side)}.{self._d(side, db)}")
        except exceptions.NotFound:
            out["absent"] = True
            return out
        out["absent"] = False
        try:
            got = list(client.query(
                "select coalesce(sum(size_bytes), 0) from"
                f" {self._q(self._d(side, db))}.__TABLES__").result())
            out["bytes"] = int(list(got[0].values())[0])
        except Exception:  # noqa: BLE001 - a size is shown, not held
            pass
        return out

    def run_rule(self, side, db, sql):
        """The operator's SQL, asked first as a dry run and run only as the
        SELECT it says it is: BigQuery keeps no transaction to roll a
        write back out of."""
        from google.cloud import bigquery
        client = self._client(side)
        dry = client.query(sql, job_config=bigquery.QueryJobConfig(
            dry_run=True, use_query_cache=False))
        kind = str(dry.statement_type or "").upper()
        if kind != "SELECT":
            raise SystemExit("a rule must only read; this one is"
                             f" {kind or 'not said to be a SELECT'}")
        return [list(r.values()) for r in client.query(sql).result()]

    def neutral_empty(self, side, db, table, where=None):
        """A DELETE needs a WHERE in BigQuery, `true` for every row."""
        self._target_only(side, "empty a table")
        job = self._client(side).query(
            f"delete from {self._qualified(side, db, table)} where"
            f" {f'({where})' if where else 'true'}")
        job.result()
        return int(job.num_dml_affected_rows or 0)

    def _scales(self, side, db, table):
        """{column: scale} of the NUMERICs declared with one, asked once a
        table: each catalogue read is a query job here."""
        known = self.__dict__.setdefault("_scales_of", {})
        at = (side, db, str(table))
        if at not in known:
            known[at] = {n: warehouse_sql._scale(t)
                         for n, t in self.neutral_columns(side, db, table)
                         if warehouse_sql._scale(t) is not None}
        return known[at]

    def _fix_read(self, side, db, table, columns, rows):
        """A NUMERIC(p, s) at its s places: BigQuery hands 1.50 back as
        1.5, where every engine that declared the scale prints 1.50."""
        from decimal import Decimal
        scales = self._scales(side, db, table)
        fix = [(i, Decimal(1).scaleb(-scales[n]))
               for i, (n, c) in enumerate(columns)
               if c == "decimal" and n in scales]
        if fix:
            for r in rows:
                for i, step in fix:
                    if r[i] is not None:
                        r[i] = Decimal(r[i]).quantize(step)
        return rows

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        rows, last = super().neutral_read(side, db, table, columns, after,
                                          limit, where)
        return self._fix_read(side, db, table, columns, rows), last

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        for batch in super().neutral_batches(side, db, table, columns, size,
                                             where):
            yield self._fix_read(side, db, table, columns, batch)

    def row_sizes(self, side, db, tables=None):
        """As every engine sizes its rows (`Engine.row_sizes`), from the
        first `SIZE_SAMPLE` rows of each table - listed, which BigQuery
        does not bill, where a query for them is billed by every byte of
        the columns it reads whatever its LIMIT: the sizing of a dataset's
        tables would cost a scan of each. A JSON value is counted as its
        text, as it is carried. Only the sizes are kept: a row is let go
        once it is measured, so a listing of wide rows holds a page of them
        at most."""
        import json

        from .base import _row_bytes, sizes_of
        client = self._client(side)
        out = {}
        for t in (tables if tables is not None
                  else self.neutral_tables(side, db)):
            try:
                it = client.list_rows(self._ref(side, db, t),
                                      max_results=self.SIZE_SAMPLE)
                lengths = [_row_bytes([json.dumps(v)
                                       if isinstance(v, (dict, list)) else v
                                       for v in r.values()]) for r in it]
            except Exception:  # noqa: BLE001 - that table is not sized
                continue
            total = getattr(it, "total_rows", None)
            sized = sizes_of(lengths,
                             exact=(len(lengths) == total
                                    if total is not None
                                    else len(lengths) < self.SIZE_SAMPLE),
                             rows=total)
            if sized:
                out[t] = sized
        return out

    # ---- the stage ----------------------------------------------------------

    @staticmethod
    def _arrow_type(field):
        """The Arrow type a column of this BigQuery schema field is loaded
        from."""
        import pyarrow as pa
        t = field.field_type.upper()
        if t in ("INTEGER", "INT64"):
            return pa.int64()
        if t in ("FLOAT", "FLOAT64"):
            return pa.float64()
        if t in ("BOOLEAN", "BOOL"):
            return pa.bool_()
        if t in ("NUMERIC", "BIGNUMERIC") and field.precision is not None \
                and field.precision <= 38:
            # the column's own numbers, where it was declared with them
            return pa.decimal128(field.precision, field.scale or 0)
        if t == "NUMERIC":
            return pa.decimal128(38, 9)
        if t == "BIGNUMERIC":
            return pa.decimal256(76, 38)
        if t in ("STRING", "JSON"):
            return pa.string()
        if t == "BYTES":
            return pa.binary()
        if t == "DATE":
            return pa.date32()
        if t == "DATETIME":
            return pa.timestamp("us")
        if t == "TIMESTAMP":
            return pa.timestamp("us", tz="UTC")
        if t == "TIME":
            return pa.time64("us")
        raise SystemExit(f"a {t} column has no Parquet counterpart migkit"
                         " loads; leave the column out with mapping.columns")

    def _arrow(self, fields, names, rows):
        import pyarrow as pa
        at = {f.name: f for f in fields}
        arrays = [arrow_array(self._arrow_type(at[n]), [r[i] for r in rows])
                  for i, n in enumerate(names)]
        return pa.Table.from_arrays(arrays, names=list(names))

    def _loads_left(self, side):
        """Load jobs the project may still run today, by its own job
        history (asked at most every ten minutes, and counted here between)
        less what a move leaves spare; None where the history cannot be
        read, which leaves loads to the quota's own refusal."""
        import time
        seen = self.__dict__.get("_loads_seen")
        if seen is None or time.monotonic() - seen[0] > self.JOBS_ASKED_EVERY:
            opts = self._endpoint(side).options or {}
            region = str(opts.get("location") or "US").lower()
            try:
                got = list(self._client(side).query(
                    f"select count(*) from `region-{region}`"
                    ".INFORMATION_SCHEMA.JOBS_BY_PROJECT where job_type ="
                    " 'LOAD' and creation_time > timestamp_sub("
                    "current_timestamp(), interval 1 day)").result())
                used = int(list(got[0].values())[0])
            except Exception:  # noqa: BLE001 - no history to read
                used = None
            seen = self.__dict__["_loads_seen"] = [time.monotonic(), used, 0]
        if seen[1] is None:
            return None
        spare = int(self.LOAD_JOBS_A_DAY * self.LOAD_JOBS_SPARE)
        return self.LOAD_JOBS_A_DAY - spare - seen[1] - seen[2]

    def _stage_table(self, side, db, table, fields, fp):
        """A table for one batch, with the target's columns, that BigQuery
        removes itself a day later if the move stops before it does."""
        import datetime

        from google.cloud import bigquery
        name = f"{OWN}stage_{self._leaf(table)}_{fp}"
        t = bigquery.Table(self._ref(side, db, name), schema=list(fields))
        t.expires = (datetime.datetime.now(datetime.timezone.utc)
                     + datetime.timedelta(days=1))
        self._client(side).create_table(t, exists_ok=True)
        return name

    def _load_stage(self, side, db, stage, fields, names, rows):
        """The batch into its stage by a load job from Parquet. The job
        replaces what the stage holds, so a load run twice - a retry after
        an answer that was lost - leaves the batch there once."""
        import io

        import pyarrow.parquet as pq
        from google.cloud import bigquery
        client = self._client(side)
        body = io.BytesIO()
        pq.write_table(self._arrow(fields, names, rows), body)
        body.seek(0)
        at = {f.name: f for f in fields}
        client.load_table_from_file(
            body, self._ref(side, db, stage),
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE",
                schema=[at[n] for n in names])).result()
        seen = self.__dict__.get("_loads_seen")
        if seen is not None:
            seen[2] += 1

    def _new_stream(self, writer, side, db, table):
        from google.cloud.bigquery_storage_v1 import types
        return writer.create_write_stream(
            parent=writer.table_path(self._database(side), self._d(side, db),
                                     table),
            write_stream=types.WriteStream(
                type_=types.WriteStream.Type.COMMITTED)).name

    def _write_stage(self, side, db, stage, fields, names, rows):
        """The batch into its stage through the Storage Write API: a
        committed stream of its own, appended from offset 0."""
        writer = self._write_client(side)
        stream = self._new_stream(writer, side, db, stage)
        held = self._append(writer, stream, fields, names, rows, 0)
        if held:
            raise SystemExit(f"{stage}: a new write stream already held rows"
                             " at its first offset")

    def _proto(self, fields, names):
        """(the message type the rows are written in, as the API is given
        it; a function writing one row in it): made from the table's own
        schema, proto2 as the Storage Write API asks. Each value as the API
        takes it for its column: a NUMERIC as its decimal text, a DATE as
        days since 1970, a TIMESTAMP as microseconds since 1970, a DATETIME
        and a TIME as their text."""
        import re

        from google.protobuf import descriptor_pb2, descriptor_pool
        from google.protobuf.message_factory import GetMessageClass
        at = {f.name: f for f in fields}
        fdp = descriptor_pb2.FileDescriptorProto(
            name="migkit_rows.proto", package="migkit", syntax="proto2")
        msg = fdp.message_type.add(name="Row")
        conv = []
        for i, n in enumerate(names, 1):
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", n):
                raise SystemExit(f"{n}: a column of this name is not written"
                                 " through the Storage Write API")
            kind, fn = _PROTO[at[n].field_type.upper()]
            msg.field.add(name=n, number=i, type=kind,
                          label=descriptor_pb2.FieldDescriptorProto
                          .LABEL_OPTIONAL)
            conv.append((n, fn))
        pool = descriptor_pool.DescriptorPool()
        pool.Add(fdp)
        desc = pool.FindMessageTypeByName("migkit.Row")
        cls = GetMessageClass(desc)
        schema = descriptor_pb2.DescriptorProto()
        desc.CopyToProto(schema)

        def one(row):
            m = cls()
            for (n, fn), v in zip(conv, row):
                if v is not None:
                    setattr(m, n, fn(v))
            return m.SerializeToString()
        return schema, one

    def _append(self, writer, stream, fields, names, rows, offset):
        """`rows` appended to a committed `stream` from `offset`, in
        requests under the API's size, each at the offset it must land at.
        Returns how many of its rows the stream already held at those
        offsets: a request sent again is refused there (`ALREADY_EXISTS`)
        rather than written twice, which is what the offsets are for.
        `OUT_OF_RANGE`, an offset past the stream's end, and a stream that
        is gone raise, for the caller to open a new one."""
        from google.api_core import exceptions
        from google.cloud.bigquery_storage_v1 import types
        from google.protobuf import wrappers_pb2
        schema, one = self._proto(fields, names)
        held = 0

        def send(part, at):
            nonlocal held
            req = types.AppendRowsRequest(
                write_stream=stream,
                offset=wrappers_pb2.Int64Value(value=at),
                proto_rows=types.AppendRowsRequest.ProtoData(
                    writer_schema=types.ProtoSchema(proto_descriptor=schema),
                    rows=types.ProtoRows(serialized_rows=part)))
            try:
                for resp in writer.append_rows(iter([req])):
                    code = resp.error.code if "error" in resp else 0
                    if code == 6:  # ALREADY_EXISTS
                        held += len(part)
                    elif code == 11:  # OUT_OF_RANGE
                        raise exceptions.OutOfRange(resp.error.message)
                    elif code:
                        raise SystemExit("the write stream refused a batch:"
                                         f" {resp.error.message}")
                    break
            except exceptions.AlreadyExists:
                held += len(part)
        part, size = [], 0
        for row in rows:
            body = one(row)
            if part and size + len(body) > self.APPEND_BYTES:
                send(part, offset)
                offset += len(part)
                part, size = [], 0
            part.append(body)
            size += len(body)
        if part:
            send(part, offset)
        return held

    def _merge_sql(self, target, source, key, names, ops=False):
        on = " and ".join(f"T.{self._q(k)} = S.{self._q(k)}" for k in key)
        sets = [n for n in names if n not in key]
        cols = ", ".join(self._q(n) for n in names)
        vals = ", ".join(f"S.{self._q(n)}" for n in names)
        return (f"merge {target} T using {source} S on {on}"
                + (" when matched and S._migkit_op = 'delete' then delete"
                   if ops else "")
                + (" when matched then update set " + ", ".join(
                    f"{self._q(n)} = S.{self._q(n)}" for n in sets)
                   if sets else "")
                + " when not matched"
                + (" and S._migkit_op = 'upsert'" if ops else "")
                + f" then insert ({cols}) values ({vals})")

    #: how a batch reaches its stage. Ranked by what each costs in money,
    #: not in time: a load job is free, the Storage Write API is billed by
    #: the byte - so the stream is taken only once the project's load jobs
    #: of the day are used up, all but the share a move leaves to others
    STAGE_WAYS = (
        decide.Rung("load job", "a load job from Parquet",
                    needs=(decide.Need(
                        "load jobs to spare today",
                        lambda f: f["loads left"] is None
                        or f["loads left"] > 0,
                        "the project's load jobs of the last day are used"
                        " up, all but the share a move leaves to others"),),
                    because="a load job costs nothing, and replaces what"
                            " its stage holds",
                    # the stage, dropped after the merge
                    footprint=1),
        decide.Rung("write stream", "the Storage Write API, a committed"
                                    " stream of its own",
                    because="it takes no load job",
                    footprint=1),
    )

    def _load_batch(self, side, db, table, columns, rows, key):
        """Staged in a table of its own - by a load job from Parquet while
        the project has load jobs to spare today, through the Storage
        Write API once it has not (`STAGE_WAYS`) - then merged in by the
        key (inserted where there is none) and the stage dropped."""
        client = self._client(side)
        names = [n for n, _ in columns]
        fields = [f for f in client.get_table(
            self._ref(side, db, table)).schema if f.name in set(names)]
        fp = self._fingerprint(table, names, rows)
        stage = self._stage_table(side, db, table, fields, fp)
        try:
            way = decide.climb("stage a batch", self.STAGE_WAYS, decide.Facts(
                **{"loads left": lambda: self._loads_left(side)}))
            if way.path == "load job":
                self._load_stage(side, db, stage, fields, names, rows)
            else:
                self._write_stage(side, db, stage, fields, names, rows)
            target = self._qualified(side, db, table)
            source = self._qualified(side, db, stage)
            cols = ", ".join(map(self._q, names))
            client.query(self._merge_sql(target, source, key, names)
                         if key else
                         f"insert into {target} ({cols}) select {cols}"
                         f" from {source}").result()
        finally:
            client.delete_table(self._ref(side, db, stage),
                                not_found_ok=True)

    def reach_items(self, side):
        """How a copy into this project is staged, from its own job
        history, and the tables migkit keeps here."""
        items = super().reach_items(side)
        if side != "dst":
            # a source's inventory of what a copy leaves (`_handwork`),
            # and what following it takes
            return items + self._source_items() + self._tail_items()
        try:
            left = self._loads_left(side)
        except Exception:  # noqa: BLE001 - said, not a stop
            left = None
        if left is None:
            items.append({"level": "info", "scope": "target",
                          "item": "BigQuery load jobs today",
                          "detail": "the project's job history could not be"
                                    " read; each batch is staged by a load"
                                    " job into a table of its own, and the"
                                    " table it is for takes only a MERGE"})
        else:
            items.append({"level": "info", "scope": "target",
                          "item": "BigQuery load jobs today",
                          "detail": f"{left:,} of {self.LOAD_JOBS_A_DAY:,}"
                                    " left to a move; each batch is staged by"
                                    " a load job into a table of its own and"
                                    " merged in, never loaded into the table"
                                    " it is for, whose 1,500 operations a day"
                                    " a MERGE is not refused by; past them,"
                                    " stages are written through the Storage"
                                    " Write API"})
        return items

    # ---- a tail's batch ------------------------------------------------------

    def _ledger(self, side, db):
        """The ledger's line for the hop: {"batch", "token", "streams":
        {table: [write stream, next offset]}}, made where it is not
        there."""
        client = self._client(side)
        led = self._qualified(side, db, self.LEDGER)
        client.query(f"create table if not exists {led} (hop STRING,"
                     " db STRING, batch INT64, token STRING, streams STRING,"
                     " at TIMESTAMP)").result()
        got = list(client.query(
            f"select batch, token, streams from {led} where hop ="
            f" {self._lit(self.hop.name)} and db = {self._lit(db)}"
        ).result())
        if not got:
            return {"batch": None, "token": None, "streams": {}}
        r = list(got[0].values())
        return {"batch": r[0], "token": r[1],
                "streams": json.loads(r[2] or "{}")}

    def _journal(self, side, db, table):
        """The table's journal, made where it is not there yet, and its
        schema: the table's columns and the batch's own."""
        from google.cloud import bigquery
        client = self._client(side)
        fields = list(client.get_table(self._ref(side, db, table)).schema)
        name = self._journal_name(table, [(f.name, f.field_type)
                                          for f in fields])
        schema = [bigquery.SchemaField(f.name, f.field_type, mode="NULLABLE")
                  for f in fields] + [
            bigquery.SchemaField("_migkit_batch", "INT64"),
            bigquery.SchemaField("_migkit_seq", "INT64"),
            bigquery.SchemaField("_migkit_op", "STRING"),
            bigquery.SchemaField("_migkit_fp", "STRING"),
            bigquery.SchemaField("_migkit_cols", "STRING")]
        client.create_table(bigquery.Table(self._ref(side, db, name),
                                           schema=schema), exists_ok=True)
        return name, schema

    def _land(self, side, db, table, journal, schema, records, n, fp,
              streams):
        """The batch's journal rows appended to the table's committed write
        stream, at the offset the ledger says follows the last batch it
        merged. Where the stream holds rows there already, they are this
        batch's from an attempt whose merge did not commit - the journal
        is asked - and nothing is appended again. Where they are anything
        else, or the stream is gone, a new stream is opened and the rows
        go there from 0; rows an earlier attempt left are passed over by
        the merge, which reads this attempt's by its fingerprint, each of
        its rows once."""
        from google.api_core import exceptions
        client = self._client(side)
        writer = self._write_client(side)
        names = [f.name for f in schema]
        rows = [[r.get(c) for c in names] for r in records]
        have = streams.get(table)
        for fresh in (have is None, True):
            if fresh:
                have = [self._new_stream(writer, side, db, journal), 0]
            try:
                held = self._append(writer, have[0], schema, names, rows,
                                    have[1])
            except (exceptions.NotFound, exceptions.OutOfRange,
                    exceptions.FailedPrecondition):
                continue
            if not held or self._landed(client, side, db, journal, n,
                                        fp) >= len(records):
                streams[table] = [have[0], have[1] + len(records)]
                return
        raise SystemExit(f"{table}: the batch's changes could not be written"
                         " to its journal")

    def _landed(self, client, side, db, journal, n, fp):
        got = list(client.query(
            f"select count(distinct _migkit_seq) from"
            f" {self._qualified(side, db, journal)} where _migkit_batch"
            f" {'is null' if n is None else f'= {int(n)}'} and"
            f" _migkit_fp = {self._lit(fp)}").result())
        return int(list(got[0].values())[0])

    def _apply_batch(self, side, db, by_table, seen):
        """Each table's changes landed in its journal through the write
        stream, then one transaction takes each shape's rows of this
        attempt out of the journal into a table of the script's own and
        merges it in, and writes the ledger's line with the streams and the
        offsets after this batch. A MERGE's source is a table here: the
        emulator refuses a subquery there, and BigQuery takes both."""
        n = seen["batch"] if seen else None
        client = self._client(side)
        ledger = self._ledger(side, db)
        streams = dict(ledger["streams"])
        script = []
        done = []
        for table, rows in by_table.items():
            key = self.neutral_key(side, db, table)
            if not key:
                raise SystemExit(f"{table} has no key on the target, and a"
                                 " change is applied by its key")
            journal, schema = self._journal(side, db, table)
            fp = self._fingerprint(table, [], rows, str(n))
            recs = self._journal_rows(table, rows, n, fp)
            self._land(side, db, table, journal, schema,
                       [r for r, _ in recs], n, fp, streams)
            done.append((journal, fp))
            for shape, names in self._shapes(recs).items():
                part = f"_migkit_s{len(script)}"
                script.append(
                    f"create temp table {part} as select * from"
                    f" {self._qualified(side, db, journal)} where"
                    f" _migkit_batch"
                    f" {'is null' if n is None else f'= {int(n)}'}"
                    f" and _migkit_fp = {self._lit(fp)} and _migkit_cols ="
                    f" {self._lit(shape)} qualify row_number() over"
                    " (partition by _migkit_seq) = 1")
                script.append(self._merge_sql(
                    self._qualified(side, db, table), part, key, names,
                    ops=True))
        if n is not None:
            led = self._qualified(side, db, self.LEDGER)
            where = (f"hop = {self._lit(self.hop.name)} and db ="
                     f" {self._lit(db)}")
            script.append(f"delete from {led} where {where}")
            script.append(
                f"insert into {led} (hop, db, batch, token, streams, at)"
                f" values ({self._lit(self.hop.name)}, {self._lit(db)},"
                f" {int(n)}, {self._lit(json.dumps(seen['token'], default=str))},"
                f" {self._lit(json.dumps(streams))}, current_timestamp())")
        client.query("begin transaction; " + "; ".join(script)
                     + "; commit transaction;").result()
        # merged, so no longer needed. Rows this recently written may not
        # be deletable yet; they are kept, passed over by every later
        # merge (which reads its own batch's fingerprint), and asked to go
        # again after the next batch
        owed = self.__dict__.setdefault("_journal_owed", [])
        owed += done
        for journal, fp in list(owed):
            try:
                client.query(f"delete from"
                             f" {self._qualified(side, db, journal)} where"
                             f" _migkit_fp = {self._lit(fp)}").result()
                owed.remove((journal, fp))
            except Exception:  # noqa: BLE001 - kept, and asked again
                pass


def _days(v):
    import datetime
    if isinstance(v, str):
        v = datetime.date.fromisoformat(v)
    if isinstance(v, datetime.datetime):
        v = v.date()
    return (v - datetime.date(1970, 1, 1)).days


def _micros(v):
    """An instant as microseconds since 1970; one without a zone is read as
    UTC, as a TIMESTAMP reads it."""
    import datetime
    if isinstance(v, str):
        v = nanotime.parse(v)
    if v.tzinfo is None:
        v = v.replace(tzinfo=datetime.timezone.utc)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (v - epoch) // datetime.timedelta(microseconds=1)


def _civil(v):
    """A DATETIME's text: an instant with a zone as UTC reads it."""
    import datetime
    if isinstance(v, str):
        v = nanotime.parse(v)
    # six digits, which is what a DATETIME holds, whatever the value carries
    return datetime.datetime.isoformat(_utc_naive(v), " ", "microseconds")


def _clock(v):
    import datetime
    if isinstance(v, str):
        v = nanotime.parse(v)
    return datetime.time.isoformat(v, "microseconds")


def _decimal_text(v):
    import decimal
    return format(v if isinstance(v, decimal.Decimal)
                  else decimal.Decimal(str(v)), "f")


def _any_text(v):
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, default=str)
    return v if isinstance(v, str) else str(v)


def _proto_types():
    from google.protobuf.descriptor_pb2 import FieldDescriptorProto as F
    return {
        "INTEGER": (F.TYPE_INT64, int), "INT64": (F.TYPE_INT64, int),
        "FLOAT": (F.TYPE_DOUBLE, float), "FLOAT64": (F.TYPE_DOUBLE, float),
        "BOOLEAN": (F.TYPE_BOOL, bool), "BOOL": (F.TYPE_BOOL, bool),
        "STRING": (F.TYPE_STRING, _any_text),
        "JSON": (F.TYPE_STRING, _any_text),
        "BYTES": (F.TYPE_BYTES, bytes),
        "NUMERIC": (F.TYPE_STRING, _decimal_text),
        "BIGNUMERIC": (F.TYPE_STRING, _decimal_text),
        "DATE": (F.TYPE_INT32, _days),
        "DATETIME": (F.TYPE_STRING, _civil),
        "TIMESTAMP": (F.TYPE_INT64, _micros),
        "TIME": (F.TYPE_STRING, _clock),
    }


class _ProtoTypes(dict):
    """{BigQuery type: (proto field type, the value as the API takes it)},
    made when first asked, so importing this module needs no protobuf."""

    def __missing__(self, key):
        if not self:
            self.update(_proto_types())
            if key in self:
                return self[key]
        raise SystemExit(f"a {key} column is not written through the"
                         " Storage Write API")


_PROTO = _ProtoTypes()


# ---- what of each wrapped library is used, and why the rest is not --------
#
# The shape `tooluse.register` takes (backlog 0f): {(command, option):
# (state, why)} a library. Registered where the registry is there to take
# it; kept here either way, beside the code that uses each.
USED, UNUSED, TOPPED = "used", "unused", "topped up"

WRAPPED = {
    "google-cloud-bigquery": {"licence": "Apache-2.0", "options": {
        ("Client", "load_table_from_file"): (
            USED, "each batch staged by a Parquet load job into a table of"
                  " its own, WRITE_TRUNCATE so a load run twice holds it"
                  " once"),
        ("Client", "load_table_from_json"): (
            UNUSED, "JSON loads turn bytes and times into text; Parquet"
                    " carries every type as it is"),
        ("Client", "insert_rows"): (
            UNUSED, "legacy streaming is at least once, and its buffer"
                    " refuses DML on fresh rows for up to 90 minutes"),
        ("Client", "query MERGE"): (
            USED, "a stage merged into its table by the key, never refused"
                  " by the table's daily operations limit"),
        ("Client", "multi-statement transaction"): (
            USED, "a tail batch's merges and its ledger line commit"
                  " together or not at all"),
        ("Client", "dry_run"): (
            USED, "an operator's rule runs only as the SELECT its dry run"
                  " says it is"),
        ("Client", "INFORMATION_SCHEMA.JOBS_BY_PROJECT"): (
            USED, "the project's load jobs of the last day choose between a"
                  " load job and the write stream"),
        ("Client", "list_rows"): (
            USED, "a table's first rows sized for the memory a copy takes,"
                  " unbilled where a query for them is billed"),
        ("Client", "Table.expires"): (
            USED, "a stage BigQuery removes itself a day later if the move"
                  " stops before dropping it"),
        ("Client", "copy_table"): (
            UNUSED, "a same-warehouse copy by the service itself is not"
                    " built yet, backlog item 33"),
        ("SQL", "BIT_XOR(FARM_FINGERPRINT)"): (
            UNUSED, "BigQuery's own number meets no other engine's; the"
                    " pair's digest is canon's fold in its SQL"),
    }},
    "google-cloud-bigquery-storage": {"licence": "Apache-2.0", "options": {
        ("BigQueryWriteClient", "committed stream offsets"): (
            USED, "a request sent again is refused at the offset it filled,"
                  " so journal rows land once"),
        ("BigQueryWriteClient", "proto rows"): (
            USED, "each row in a message made from the table's own schema,"
                  " sent with each request"),
        ("BigQueryWriteClient", "pending stream"): (
            UNUSED, "the journal is merged in a transaction, which already"
                    " makes the batch visible all at once"),
        ("BigQueryWriteClient", "default stream _CHANGE_TYPE"): (
            UNUSED, "the default stream takes no offsets and applies at the"
                    " table's staleness, not at the batch's commit"),
        ("BigQueryReadClient", "read session"): (
            UNUSED, "reading a BigQuery source by read streams is not built"
                    " yet, backlog item 33"),
        ("BigQueryWriteClient", "Arrow rows"): (
            UNUSED, "the emulator the side is proved on reads rows in"
                    " messages only; the same values go either way"),
    }},
    "snowflake-connector-python": {"licence": "Apache-2.0", "options": {
        ("cursor", "PUT to a table stage"): (
            USED, "a batch staged as one Parquet file of text in the"
                  " table's own stage"),
        ("cursor", "COPY INTO with a transformation"): (
            USED, "each staged text cast to its column's type, and a file"
                  " copied once for 64 days by its load history"),
        ("cursor", "MERGE in a transaction"): (
            USED, "a stage merged by the key, and a tail batch with its"
                  " ledger line, committed together"),
        ("connect", "private_key_file"): (
            USED, "key-pair sign-in, which a streaming channel needs as"
                  " well"),
        ("pandas_tools", "write_pandas"): (
            TOPPED, "it stages and copies but appends; migkit stages text"
                    " cast by type and merges by the key"),
        ("SQL", "HASH_AGG"): (
            UNUSED, "Snowflake's own number meets no other engine's; the"
                    " pair's digest is canon's fold in its SQL"),
    }},
    "snowpipe-streaming": {"licence": "Apache-2.0", "options": {
        ("StreamingIngestClient", "from_table"): (
            USED, "table mode, on the PIPE Snowflake makes for the"
                  " journal itself"),
        ("StreamingIngestChannel", "offset token"): (
            USED, "the batch's number and fingerprint, so an attempt that"
                  " landed is not appended again"),
        ("StreamingIngestChannel", "wait_for_commit"): (
            USED, "the merge reads the journal only after the channel"
                  " committed the batch's token"),
        ("StreamingIngestClient", "get_elastic_channel"): (
            UNUSED, "elastic channels are at least once, with no order and"
                    " no offset token"),
    }},
    "redshift_connector": {"licence": "Apache-2.0", "options": {
        ("connect", "iam"): (
            USED, "IAM sign-in to a cluster or a Serverless workgroup,"
                  " which the PostgreSQL driver has no way to"),
        ("connect", "password"): (
            UNUSED, "a password sign-in goes through the PostgreSQL driver,"
                    " as it did before"),
        ("cursor", "write_dataframe"): (
            UNUSED, "a batch is staged and merged in one transaction, not"
                    " appended"),
        ("SQL", "FNV_HASH"): (
            UNUSED, "Redshift's own number meets no other engine's; the"
                    " pair's digest is canon's fold in its SQL"),
        ("SQL", "STL_LOAD_COMMITS"): (
            USED, "whether a keyless batch whose commit went unanswered was"
                  " loaded, before it is inserted again"),
    }},
}


def _register(wrapped):
    """Into `tooluse`'s registry, which imports this module for it
    (`tooluse.CONTRIBUTORS`)."""
    from .. import tooluse
    for name, spec in wrapped.items():
        tooluse.register(name, licence=spec["licence"],
                         options=spec["options"])


_register(WRAPPED)
