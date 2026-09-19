import datetime
import decimal
import json
import re

from ..util import run, which
from .base import Engine, RepairAction, Result


class GenericEngine(Engine):
    """Any engine reladiff speaks: snowflake, bigquery, redshift, clickhouse,
    oracle, trino, presto, duckdb, vertica and more. Endpoints carry a full
    connection url in options.url, tables listed in hop options."""

    checks = ("counts", "data")

    def _url(self, side):
        ep = self.hop.source if side == "src" else self.hop.target
        url = ep.options.get("url", "")
        if not url:
            raise SystemExit(f"generic engine needs {side}.url in hops.yaml")
        return url

    def databases(self):
        return ["-"]

    def _tables(self):
        tables = self.hop.options.get("tables") or []
        if not tables:
            raise SystemExit("generic engine needs options.tables: [t1, t2]")
        return tables

    def _key(self):
        key = self.hop.options.get("key", "id")
        return [key] if isinstance(key, str) else list(key)

    def _reladiff_cmd(self, table, extra, jobs=None, stats=True):
        """The command, apart from running it.

        `-j` is the number of threads reladiff uses, and measured against a
        200,000-row pair it is a connection count rather than a speed:

            -j 1   2 connections on the source   0.55s
            -j 8   9 connections on the source   0.54s

        So it is the load this engine puts on somebody's database, and it
        was pinned to the hop's worker count with nothing watching. The
        throttle's current allowance is what goes here instead, so a run
        that is slowing down asks for fewer connections rather than only
        waiting longer between tables.
        """
        # `jobs or workers` would read a throttle down to nothing as "no
        # preference" and go back to the full count, which is the one moment
        # it matters most
        want = self.hop.workers if jobs is None else jobs
        cmd = ["reladiff", self._url("src"), table, self._url("dst"), table,
               *(["--stats"] if stats else []), "-j", str(max(1, int(want)))]
        for k in self._key():
            cmd += ["-k", k]
        return cmd + list(extra)

    def _reladiff(self, table, extra, jobs=None, stats=True):
        if not which("reladiff"):
            raise SystemExit("reladiff not found, run bootstrap.sh")
        return run(self._reladiff_cmd(table, extra, jobs, stats), check=False,
                   timeout=3600)

    def _gate(self):
        """One table at a time is not the unit here - reladiff runs a whole
        table per call, and there is no server to ask how it is doing
        through a subprocess. The latency signal carries it, and what the
        gate narrows is passed on as `-j`.
        """
        from ..throttle import Throttle
        return Throttle(self.hop.workers)

    #: the lines `--stats` prints, which is the whole of what reladiff says
    STATS = (("rows_a", r"(\d+) rows in table A"),
             ("rows_b", r"(\d+) rows in table B"),
             ("only_a", r"(\d+) rows exclusive to table A"),
             ("only_b", r"(\d+) rows exclusive to table B"),
             ("updated", r"(\d+) rows updated"))

    def _stats(self, p):
        """The numbers reladiff printed, or {} if it printed something else.

        Read rather than pattern-matched on a phrase. The old check looked
        for `0 rows are different`, which reladiff 0.6.0 does not say in any
        case - measured on two identical tables it prints `0.00% difference
        score`, so every run of `check data` reported a difference that was
        not there.

        The exit code says nothing either: measured, reladiff exits **0**
        whether the tables match, differ, name a table that does not exist,
        or use a scheme it does not support. What separates those is whether
        the stats came out at all.

        Only three of the five numbers are used for a verdict, and the two
        that are not are the reason. Measured on one unchanging pair of
        50-row tables, the same command three times in a row:

            50 rows in table A | 50 rows in table B | ... | 49 unchanged | 2.00%
             0 rows in table A |  1 rows in table B | ... | -1 unchanged | 200.00%
             0 rows in table A |  1 rows in table B | ... | -1 unchanged | 200.00%

        `-1 rows unchanged` is its own proof that the totals are not a
        reading of the tables. The exclusive and updated counts came out the
        same every time and matched the rows that really differed, so those
        are what migkit reports. A table's row count is still answerable from
        them: everything in common cancels, so the difference between the two
        sides is exactly `only_a - only_b`.
        """
        got = {}
        for name, pattern in self.STATS:
            m = re.search(pattern, p.stdout)
            if m:
                got[name] = int(m.group(1))
        return got if len(got) == len(self.STATS) else {}

    def _why_no_stats(self, p):
        lines = [l for l in (p.stderr or "").splitlines() if l.strip()]
        return (lines[-1][-160:] if lines else
                (p.stdout or "").strip()[-160:] or "no output at all")

    #: a column name no table will have, used to make reladiff list the real
    #: ones: `Column 'x' not found in table 1, named 't'. Columns: id, v`
    PROBE_COLUMN = "migkit_probe_no_such_column"
    PROBE_TABLE = "migkit_probe_no_such_table"
    #: how many tables assess will probe before it stops and says so
    ASSESS_TABLES = 10

    def _probe(self, side, table, key=None):
        """One reladiff call that is meant to fail, read for what it says.

        Every question assess wants answered comes back as an error message
        before reladiff compares anything, so none of these probes scan a
        table. Measured, all four exit 0 and differ only in what they print:

            reachable, table absent   Table 'x' does not exist, or has no columns
            wrong key                 Column 'k' not found in table 1, named 't'.
                                      Columns: id, v
            unsupported scheme        Scheme 'sqlite' currently not supported
            nothing listening         Is the server running on that host ...

        The wrong-key one is the useful one twice over: asking for a column
        that cannot exist is how migkit gets the real column list without a
        query of its own.
        """
        if not which("reladiff"):
            return None
        url = self._url(side)
        cmd = ["reladiff", url, table, table, "--stats",
               "-k", key or self.PROBE_COLUMN]
        return run(cmd, check=False, timeout=120)

    @staticmethod
    def _probe_says(p):
        """(kind, detail) for one probe's output."""
        text = ((p.stderr or "") + (p.stdout or "")).strip()
        last = text.splitlines()[-1].strip() if text else ""
        if "currently not supported" in text:
            return "scheme", last
        if "does not exist, or has no columns" in text:
            return "no-table", last
        if "not found in table" in text:
            columns = text.rsplit("Columns:", 1)[-1].strip() if "Columns:" \
                in text else ""
            return "columns", columns
        if not text:
            return "quiet", "reladiff said nothing at all"
        return "unreachable", last[-160:]

    def _assess_extra(self):
        """What has to be true before reladiff is pointed at anything.

        This engine shells out, and the tool it shells out to reports every
        failure the same way: a line on stderr and an exit status of 0. A run
        that never compared a row looks like a run that found no differences
        unless someone asks these questions first.
        """
        items = []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "tool", "item": item,
                          "detail": str(detail)})
        found = which("reladiff")
        if not found:
            add("fail", "reladiff", "not on PATH - the generic engine is a"
                                    " wrapper around it and can do nothing"
                                    " without it")
            return items
        version = run(["reladiff", "--version"], check=False, timeout=60)
        add("pass", "reladiff",
            f"{found} ({(version.stdout or version.stderr).strip()[:60]})")

        try:
            tables = self._tables()
        except SystemExit as e:
            add("fail", "tables to compare", str(e))
            tables = []
        key = self.hop.options.get("key", "id")
        keys = [key] if isinstance(key, str) else list(key)
        add("pass" if self.hop.options.get("key") else "warn",
            "key columns",
            f"{', '.join(keys)}"
            + ("" if self.hop.options.get("key") else
               " - not configured, so the default `id` is being used"))

        usable = []
        for side in ("src", "dst"):
            try:
                self._url(side)
            except SystemExit as e:
                add("fail", f"{side} url", str(e))
                continue
            kind, detail = self._probe_says(self._probe(side,
                                                        self.PROBE_TABLE))
            if kind == "no-table":
                usable.append(side)
                add("pass", f"{side} url",
                    "reachable, and reladiff speaks this scheme")
            elif kind == "scheme":
                add("fail", f"{side} url",
                    f"{detail} - reladiff exits 0 on this, so a check would"
                    " have looked like a clean run")
            else:
                add("fail", f"{side} url", detail)

        # a side that could not be reached at all is not asked about its
        # tables: the answer would be the same sentence again, once per table
        for table in tables[:self.ASSESS_TABLES] if usable else []:
            for side in usable:
                kind, detail = self._probe_says(self._probe(side, table))
                if kind == "columns":
                    have = {c.strip().lower() for c in detail.split(",") if c}
                    missing = [k for k in keys if k.lower() not in have]
                    add("pass" if not missing else "fail",
                        f"{side} {table}",
                        f"{len(have)} columns"
                        if not missing else
                        f"key column(s) {', '.join(missing)} are not there:"
                        f" {detail}")
                elif kind == "no-table":
                    add("fail", f"{side} {table}", "not on this side")
                else:
                    add("warn", f"{side} {table}",
                        f"{detail} - unknown, not clean")
        if len(tables) > self.ASSESS_TABLES:
            add("warn", "tables probed",
                f"{self.ASSESS_TABLES} of {len(tables)} - the rest were not"
                " looked at here")
        return items

    def check_counts(self, db):
        bad = []
        blind = []
        tables = self._tables()
        gate = self._gate()
        for t in tables:
            with gate.unit():
                p = self._reladiff(t, [], jobs=gate.permits)
            got = self._stats(p)
            if not got:
                blind.append(f"{t}: {self._why_no_stats(p)}")
            elif got["only_a"] != got["only_b"]:
                gap = got["only_a"] - got["only_b"]
                bad.append(f"{t} has {abs(gap)} more rows on the"
                           f" {'source' if gap > 0 else 'target'}")
        res = []
        if blind:
            res.append(Result("counts", db, "error",
                              "reladiff did not report on " + "; ".join(
                                  blind[:6])
                              + " - a table nobody could count is not a table"
                                " whose counts match"))
        if bad:
            res.append(Result("counts", db, "diff", "; ".join(bad[:10]), "",
                              "counted from the keys on one side only, which"
                              " is the part of reladiff's output that holds"
                              " still between runs"))
        return res or [Result("counts", db, "ok",
                              f"{len(tables)} tables, the same number of rows"
                              " on both sides")]

    def check_data(self, db, table=None, stream=None):
        res = []
        gate = self._gate()
        for t in ([table] if table else self._tables()):
            with gate.unit():
                p = self._reladiff(t, ["-c", "%"], jobs=gate.permits)
            got = self._stats(p)
            scope = f"{db}.{t}" if db != "-" else t
            if not got:
                if stream:
                    stream(f"{t}: error")
                res.append(Result(
                    "data", scope, "error",
                    f"reladiff did not report: {self._why_no_stats(p)}", "",
                    "it exits 0 whether it compared anything or not, so the"
                    " absence of its numbers is the only thing that says it"
                    " did not"))
                continue
            parts = [f"{got['only_a']} rows only on the source"
                     if got["only_a"] else "",
                     f"{got['only_b']} rows only on the target"
                     if got["only_b"] else "",
                     f"{got['updated']} rows with different values"
                     if got["updated"] else ""]
            parts = [x for x in parts if x]
            status = "diff" if parts else "ok"
            if stream:
                stream(f"{t}: {status}")
            detail = ("; ".join(parts) if parts
                      else "no row is on one side only and no compared"
                           " column differs")
            res.append(Result(
                "data", scope, status,
                detail + self._drill(db, t, bool(parts), gate.permits), "",
                "`migkit sync --db - --kind rows` names them and can put"
                " them right" if parts else ""))
        return res

    # ---- the rows behind the counts ------------------------------------

    def _drill(self, db, table, differs, jobs=None):
        """Which rows differ, written to the estate's files, as a clause.

        The counts say a table is wrong; this says which rows, which is what
        a repair reads. It is a second call because `--stats` and the rows
        do not come out of the same one - measured on reladiff 0.6.0, asking
        for both prints the stats object alone:

            --stats --json   {"rows_A": 0, ... "updated": 2, ...}
            --json           ["-", ["11", "v11"]]
                             ["+", ["3", "CHANGED"]]
                             ["-", ["3", "v3"]]

        `-` is the source's version of a row and `+` the target's, so a key
        on both sides changed, a key on `-` alone is missing from the target
        and a key on `+` alone is extra there.

        Only the key is read off each line. It is first whatever its
        position in the table - measured on `(a text, id bigint primary key,
        z text)`, which came back as `["2", "CHANGED", "a2"]`, and on a
        two-column key, which came back in the order the `-k` flags were
        given. The columns after the key arrive in an order that is neither
        the table's nor alphabetical, so the repair re-reads whole rows from
        the source by key rather than trusting this line to say which value
        belongs to which column.
        """
        if not differs:
            self._write_drill(db, table, missing=[], changed=[], extra=[])
            return ""
        p = self._reladiff(table, ["-c", "%", "--json"], jobs=jobs,
                           stats=False)
        width = len(self._key())
        minus, plus = set(), set()
        for line in (p.stdout or "").splitlines():
            line = line.strip()
            if not line.startswith("["):
                continue
            try:
                sign, values = json.loads(line)
            except ValueError:
                continue
            if not isinstance(values, list) or len(values) < width:
                continue
            (minus if sign == "-" else plus).add(tuple(values[:width]))
        if not minus and not plus:
            # the counts found differences and this found no rows at all, so
            # the last run's list is not to be left behind for `sync` to act
            # on - an empty drilldown would read as nothing to repair
            self._write_drill(db, table, missing=[], changed=[], extra=[])
            return ("; rows not localised: " + self._why_no_stats(p))
        self._write_drill(
            db, table,
            missing=[json.dumps(list(k)) for k in sorted(minus - plus)],
            changed=[json.dumps(list(k)) for k in sorted(minus & plus)],
            extra=[json.dumps(list(k)) for k in sorted(plus - minus)])
        return ""

    # ---- putting them right --------------------------------------------

    def _connect(self, side):
        """reladiff's own connection, which is the only driver here.

        The comparison is a subprocess, but the same library opens a
        connection from python that executes statements - measured against
        PostgreSQL 16, `db.query("update ...", None)` changed the row, and
        `is_autocommit` is False, so nothing lands until `commit()`. That is
        what makes a repair possible at all: migkit has no driver of its own
        for snowflake or clickhouse, and the subprocess cannot write.
        """
        if not hasattr(self, "_conns"):
            self._conns = {}
        if side not in self._conns:
            from reladiff.databases import connect
            try:
                self._conns[side] = connect(self._url(side))
            except Exception as e:
                raise SystemExit(f"cannot open the {side} url to write:"
                                 f" {str(e).splitlines()[-1][:160]}")
        return self._conns[side]

    @staticmethod
    def _path(conn, table):
        """The table as this database names it, qualified.

        `_normalize_table_path` is not it: measured, it answers
        `(None, 'public', 't')`, which the query builder rejects for the
        None. The schema is only added when the hop did not give one.
        """
        parts = tuple(conn.parse_table_name(table))
        if len(parts) == 1 and conn.default_schema:
            return (conn.default_schema, parts[0])
        return parts

    def _schema(self, conn, table):
        """Column names and types, as the classes every dialect shares.

        `_process_table_schema` is the step that turns `bigint` or `NUMBER`
        into `Integer`, so what comes back can be reasoned about without a
        per-database type table - which is the only way one repair can serve
        every engine reladiff speaks.
        """
        path = self._path(conn, table)
        try:
            return conn._process_table_schema(path,
                                              conn.query_table_schema(path))
        except Exception as e:
            raise SystemExit(f"cannot read the columns of {table}:"
                             f" {str(e).splitlines()[-1][:160]}")

    @staticmethod
    def _value(coltype, text, column):
        """A key from the drilldown, back as a value this dialect can match.

        The drilldown holds canonical text so that the same file can be read
        whatever wrote it; a `where` clause needs the value. Only the three
        classes whose text form is unambiguous are converted, and anything
        else says so rather than guessing a format.
        """
        from sqeleton.abcs import Boolean, NumericType, StringType
        if isinstance(coltype, Boolean):
            return text.strip().lower() in ("1", "true", "t", "yes")
        if isinstance(coltype, NumericType):
            return (decimal.Decimal(text) if getattr(coltype, "precision", 0)
                    else int(text))
        if isinstance(coltype, StringType):
            return text
        raise SystemExit(
            f"migkit will not repair by {column}: it is"
            f" {type(coltype).__name__}, and a key of that type has no one"
            " text form to turn back into a value. Key the hop on a column"
            " that is a number, a string or a boolean, or repair those rows"
            " with the tool that owns that database")

    #: a name the writer renders unquoted, which is the one place a column
    #: name has to be ordinary. Measured on sqeleton 0.1.7: an insert quotes
    #: its column list, `UPDATE ... SET v = 'x'` does not, so a column that
    #: needed quoting would be resolved by the server as some other column.
    PLAIN_COLUMN = re.compile(r"^[a-z_][a-z0-9_]*$")

    def repair_plan(self, db, kind):
        if kind not in ("rows", "all"):
            return []
        return self._rows_plan(db, "from the source to the target")

    def _close(self):
        """Give both servers their locks back.

        A read is a transaction too on a connection that does not
        autocommit. Measured on PostgreSQL 16, a connection that had only
        run `select count(*)` sat at `idle in transaction`, and a `drop
        table` from anywhere else waited on it until it closed - so a repair
        that returned without closing would leave a lock on somebody's table
        for as long as the process lived. Nothing here outlives one repair.
        """
        for conn in getattr(self, "_conns", {}).values():
            try:
                conn.close()
            except Exception:
                pass
        self._conns = {}

    def apply(self, db, action):
        """Put the listed rows right over reladiff's connection.

        Writes first and deletions last, so a repair cut off in the middle
        leaves rows that should not be there - which the next check names -
        rather than a hole nothing looks for. A row that both sides have is
        updated rather than deleted and re-inserted, for the same reason:
        the pair has a moment in which the row is on neither side.
        """
        try:
            self._apply_rows_borrowed(db, action)
        finally:
            self._close()

    def _apply_rows_borrowed(self, db, action):
        """The row repair this engine does over the driver it borrows
        from reladiff. Named apart from the base's `_apply_rows`, which
        carries rows between two engines over the neutral contract and
        takes a different set of arguments: one name for two methods on
        the same object is a trap for whoever calls the wrong one.
        """
        from sqeleton.queries import and_, or_, table, this
        name = action.scope.split(".", 1)[1]
        found = self._drill_tables(db).get(name, {})
        if not found:
            return
        src, dst = self._connect("src"), self._connect("dst")
        key = self._key()
        dst_schema, src_schema = self._schema(dst, name), self._schema(src,
                                                                      name)
        columns = sorted(set(dst_schema) & set(src_schema))
        absent = [k for k in key if k not in columns]
        if absent:
            raise SystemExit(
                f"{name} cannot be repaired by {', '.join(absent)}: the"
                " column is not on both sides, so a row listed by the check"
                " cannot be addressed on the target")
        src_t = table(*self._path(src, name))
        dst_t = table(*self._path(dst, name))

        def where(key_text):
            return and_(*[getattr(this, k) == self._value(dst_schema[k], v, k)
                          for k, v in zip(key, key_text)])

        def rows_for(conn, tbl, key_texts):
            # every column expression is built again per query: `this.id` is
            # resolved in place the first time it is compiled, and a list
            # held over from the previous query raises `Already resolved!`
            got = {}
            for at in range(0, len(key_texts), self.REPAIR_BATCH):
                batch = key_texts[at:at + self.REPAIR_BATCH]
                query = tbl.where(or_(*[where(k) for k in batch])).select(
                    *[getattr(this, c) for c in columns])
                for row in conn.query(query, list):
                    got[tuple(self._text(v) for v in
                              [row[columns.index(k)] for k in key])] = row
            return got

        changed = found.get("changed", [])
        drop = found.get("extra", [])
        undo_dir = self.hop.report_dir(db) / "undo"
        undo_dir.mkdir(parents=True, exist_ok=True)
        held = rows_for(dst, dst_t, changed + drop)
        with (undo_dir / f"{name}.rows.jsonl").open("a") as handle:
            for key_text, row in held.items():
                handle.write(json.dumps({
                    "table": name, "key": list(key_text),
                    "row": {c: self._text(v)
                            for c, v in zip(columns, row)}}) + "\n")

        missing = found.get("missing", [])
        rows = rows_for(src, src_t, missing + changed)
        for key_text, row in rows.items():
            for column, value in zip(columns, row):
                self._writable(name, column, value,
                               key_text in set(changed))
        insert = [tuple(rows[k]) for k in missing if k in rows]
        if insert:
            dst.query(dst_t.insert_rows(insert, columns=columns), None)
        for key_text in changed:
            row = rows.get(key_text)
            if row is None:
                continue
            fields = {c: self._settable(v)
                      for c, v in zip(columns, row) if c not in key}
            unusual = sorted(c for c in fields
                             if not self.PLAIN_COLUMN.match(c))
            if unusual:
                raise SystemExit(
                    f"{name} has columns the writer would not quote:"
                    f" {', '.join(unusual)}. An update naming them would be"
                    " read by the server as some other column, so migkit"
                    " will not send it - the rows it had already inserted"
                    " are listed in the report and the check will name the"
                    " rest again")
            if fields:
                dst.query(dst_t.update_fields(where(key_text), **fields),
                          None)
        for key_text in drop:
            dst.query(dst_t.delete_rows(where(key_text)), None)
        if not dst.is_autocommit:
            dst.commit()

    #: how many keys go into one `where` - the drilldown can be long and a
    #: query per row would be a round trip per row
    REPAIR_BATCH = 200

    #: what the borrowed writer's `update` will take, read off `UpdateTable`
    #: and measured: a `Decimal` raises, and so does anything else not here.
    #: An insert takes more than this, which is why the two are checked
    #: apart - a row that can be inserted cannot always be updated.
    SETTABLE = (str, bool, int, float, datetime.datetime, list, dict,
                type(None))
    #: the digits of an exact number, which is all `_settable` will pass
    #: through as an expression rather than as a value
    EXACT_NUMBER = re.compile(r"^-?\d+(\.\d+)?$")

    def _writable(self, table, column, value, updating):
        """Refuse a value the borrowed writer would not carry faithfully.

        Checked over the whole set before anything is written, so a refusal
        leaves the target as the check found it rather than half repaired.
        """
        if isinstance(value, (bytes, bytearray, memoryview)):
            raise SystemExit(
                f"{table}.{column} holds binary, and the writer this engine"
                " borrows renders bytes as the characters they happen to"
                " spell - measured, b'\\x00\\x01' goes in as a"
                " two-character string. Nothing has been written. Move"
                " those rows with the tool that owns the target, or leave"
                f" {column} out of the hop's column list")
        if (updating and not isinstance(value, self.SETTABLE)
                and not isinstance(value, decimal.Decimal)):
            raise SystemExit(
                f"{table}.{column} is {type(value).__name__}, which the"
                " borrowed writer will not put in an update. Nothing has"
                " been written. `migkit move` can re-copy the table, or"
                " repair those rows with the tool that owns the target")

    def _settable(self, value):
        """A value on its way into an update.

        A `Decimal` goes in as its own digits rather than as a value: the
        writer refuses the type outright, and the float it would otherwise
        take rounds the number on the way into the database. The digits are
        what the same writer renders for an insert of the same value, so the
        two paths put the same number in.
        """
        if isinstance(value, decimal.Decimal):
            from sqeleton.queries import code
            text = format(value, "f")
            if not self.EXACT_NUMBER.match(text):
                raise SystemExit(
                    f"a numeric column holds {text!r}, which is not a number"
                    " that can be written literally. Nothing has been"
                    " written; repair that row with the tool that owns the"
                    " target")
            return code(text)
        return value

    @staticmethod
    def _text(value):
        return None if value is None else str(value)
