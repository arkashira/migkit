import datetime
import decimal
import json
import re

from ..util import run, which
from .base import Engine, RepairAction, Result, clauses


class GenericEngine(Engine):
    """Any engine reladiff speaks: snowflake, bigquery, redshift, clickhouse,
    oracle, trino, presto, duckdb, vertica and more. Endpoints carry a full
    connection url in options.url, tables listed in hop options."""

    checks = ("schema", "counts", "data")

    def _url(self, side):
        ep = self.hop.source if side == "src" else self.hop.target
        url = ep.options.get("url", "")
        if not url:
            raise SystemExit(f"generic engine needs {side}.url in hops.yaml")
        return url

    def databases(self):
        return ["-"]

    def _tables(self, db="-"):
        """The tables the hop takes: those it lists, or where it lists none
        every base table the source's catalogue holds (`_catalogue_tables`),
        each asked of the one scope answer (`in_scope`), so a pattern like
        `tmp_*` means here what it means on every other engine."""
        listed = [str(t) for t in self.hop.options.get("tables") or []] \
            or self._catalogue_tables()
        if not listed:
            raise SystemExit(
                "the hop names no tables in options.tables and the source's"
                f" catalogue listed none ({self._unlisted or 'it is empty'}):"
                " name them in options.tables: [t1, t2]")
        tables = [t for t in listed if self.in_scope(db, t)]
        if not tables:
            raise SystemExit("every table the hop lists is also in its"
                             " exclude list, so there is nothing to compare")
        return tables

    #: the schemas a server keeps its own catalogue in, never a hop's tables
    SYSTEM_SCHEMAS = frozenset({"information_schema", "pg_catalog", "sys",
                                "mysql", "performance_schema", "pg_toast",
                                "crdb_internal", "system"})
    #: the standard catalogue's base tables, asked through the driver
    CATALOGUE_TABLES = ("SELECT table_schema, table_name FROM"
                        " information_schema.tables WHERE table_type ="
                        " 'BASE TABLE'")

    def _catalogue_tables(self):
        """`schema.table` of every base table the source's standard
        catalogue lists, probed once; [] where the server keeps no standard
        catalogue, the reason kept (`_unlisted`) for the stop that follows.
        A catalogue the user may not read stops as the grant it needs."""
        if getattr(self, "_listed", None) is not None:
            return self._listed
        self._unlisted, self._listed = None, []
        try:
            rows = self._connect("src").query(self.CATALOGUE_TABLES, list)
        except (Exception, SystemExit) as e:  # noqa: BLE001 - kept, said
            self._grant_refused("src", "listing tables",
                                "information_schema.tables", e)
            self._unlisted = (str(e).splitlines() or [""])[-1][:160] \
                or "no standard catalogue"
            return self._listed
        self._listed = sorted(
            f"{s}.{t}" if s else str(t) for s, t in rows
            if str(s or "").lower() not in self.SYSTEM_SCHEMAS)
        return self._listed

    def _user(self, side):
        from urllib.parse import unquote, urlsplit
        try:
            got = urlsplit(self._url(side)).username
        except (ValueError, SystemExit):
            return None
        return unquote(got) if got else None

    def grant_statement(self, side, privilege, on):
        """The standard statement that gives the side's user `privilege` on
        `on`; None where the address names no user to grant to."""
        user = self._user(side)
        return f"GRANT {privilege} ON {on} TO {user}" if user else None

    #: the privilege each refused action needs, in the standard's words
    PRIVILEGE = {"listing tables": "SELECT", "reading rows": "SELECT",
                 "copying rows in": "INSERT, DELETE, SELECT"}

    def _grant_refused(self, side, action, on, e):
        """Stop on `e` where the server refused `action` on `on` for want
        of a grant, as the grant and the statement that gives it - never an
        empty answer in its place. Returns where `e` is anything else."""
        from . import enterprise_connectors as ec
        if not ec.is_grant_error("generic", e):
            return
        privilege = self.PRIVILEGE[action]
        run = self.grant_statement(side, privilege, on)
        raise ec.MissingGrant(
            "generic", action, f"{privilege} on {on}"
            + (f"; run: {run}" if run else ""), e)

    def _on(self, which, table, db="-"):
        """The table's name on one side: the source's as listed, the
        target's under the hop's name rules (`Hop.target_table_in`)."""
        if which == "src":
            return table
        return str(self.hop.target_table_in(None if db == "-" else db,
                                            *str(table).split(".")))

    def _filter(self, table, db="-"):
        """The hop's row filter of the table (`Engine.row_filter`), read
        alike by the copy and every check, or None."""
        return self.row_filter(None if db == "-" else db,
                               *str(table).split("."))

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
        # the addresses go in the run's configuration file (`_reladiff`),
        # not here: an address can carry its password
        cmd = ["reladiff", "--conf", None,
               *(["--stats"] if stats else []), "-j", str(max(1, int(want)))]
        # sized by migkit where the table's facts say its rows; its own
        # defaults otherwise, spelled out
        from ..verifiers import reladiff_rung
        fact = self._facts_of(table)
        rung = reladiff_rung(fact.get("rows"), fact.get("bytes"), want,
                             ("generic",))
        cmd += ["--bisection-factor", str(rung["bisection_factor"]),
                "--bisection-threshold", str(rung["bisection_threshold"])]
        for k in self._key():
            cmd += ["-k", k]
        pred = self._filter(table)
        return cmd + (["-w", pred] if pred else []) + list(extra)

    def _facts_of(self, table):
        """What the catalogue says of one table's size, or {}: the engine
        reads no catalogue of its own, so this is {} until one does."""
        try:
            got = self.table_facts("src", "")
        except Exception:  # noqa: BLE001 - no facts, the defaults
            return {}
        return got.get(table) or {}

    def _reladiff(self, table, extra, jobs=None, stats=True):
        if not which("reladiff"):
            raise SystemExit(
                "this engine needs a comparison component that is not"
                " installed on this machine: migkit doctor --install"
                " puts it in place, and migkit doctor says what is"
                " missing")
        from ..util import PrivateFile, diff_run_config
        cmd = self._reladiff_cmd(table, extra, jobs, stats)
        with PrivateFile(diff_run_config(self._url("src"), table,
                                         self._url("dst"),
                                         self._on("dst", table)),
                         ".toml") as path:
            cmd[2] = path
            return run(cmd, check=False, timeout=3600)

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

    #: What the catalogue answers for every engine this speaks, and what it
    #: does not. Measured on PostgreSQL 16 with a deliberately mismatched
    #: pair - the five columns the schema query selects are
    #: (name, declared type, datetime precision, numeric precision, scale):
    #:
    #:     source id      ('id', 'bigint', None, 64, 0)
    #:     target id      ('id', 'integer', None, 32, 0)        <- seen
    #:     source amt     ('amt', 'numeric', None, 12, 2)
    #:     target amt     ('amt', 'numeric', None, 12, 4)       <- seen
    #:     source ts      ('ts', 'timestamp with time zone', 6, None, None)
    #:     target ts      ('ts', 'timestamp without time zone', 6, None, None)
    #:                                                          <- seen
    #:     source v       ('v', 'character varying', None, None, None)
    #:     target v       ('v', 'character varying', None, None, None)
    #:                       varchar(50) against varchar(200)   <- NOT seen
    #:     n_null         identical either way, NOT NULL or not <- NOT seen
    #:
    #: Length and nullability are not in what the catalogue is asked for, and
    #: asking for them would mean writing that query once per engine - nine
    #: of them, eight of which cannot be tried here. So the check says what
    #: it did not look at, every time it passes. A clean line that quietly
    #: means "some of the schema" is worse than no line.
    BLIND_SPOTS = "not compared: string lengths, nullability"

    #: the part of a column's catalogue row that describes its type
    def _coltype(self, row):
        return tuple(row[1:5])

    @staticmethod
    def _describe(row):
        """A column's type as a person would write it."""
        _, kind, dt_prec, num_prec, num_scale = row[:5]
        if num_prec is not None and num_scale not in (None, 0):
            return f"{kind}({num_prec},{num_scale})"
        if num_prec is not None:
            return f"{kind}({num_prec})"
        return str(kind)

    @staticmethod
    def _risk(src_row, dst_row):
        """What the difference costs, when it can be said from these five
        numbers. Anything else is reported as a difference without a claim
        about which way the risk runs."""
        s_kind, d_kind = str(src_row[1]), str(dst_row[1])
        if ("with time zone" in s_kind) != ("with time zone" in d_kind):
            if "with time zone" in s_kind:
                return ("the target drops the offset - every value lands as"
                        " whatever the session's zone made of it")
            return "the target keeps an offset the source does not carry"
        s_p, d_p = src_row[3], dst_row[3]
        if s_p is not None and d_p is not None and s_p != d_p:
            return ("narrower on the target - values the source holds will"
                    " not fit" if d_p < s_p else
                    "wider on the target, which holds every source value")
        s_s, d_s = src_row[4], dst_row[4]
        if s_s is not None and d_s is not None and s_s != d_s:
            return ("fewer decimal places on the target - values round on"
                    " the way in" if d_s < s_s else
                    "more decimal places on the target")
        return ""

    def check_schema(self, db):
        """Compare the two sides' columns and their declared types.

        This engine had no schema check at all, so a target built by hand
        with `int` where the source has `bigint` passed the row counts and
        the row comparison, and overflowed later - the class of failure that
        costs nothing to find now and a cutover to find then.
        """
        res = []
        for t in self._tables(db):
            scope = f"{db}.{t}" if db != "-" else t
            try:
                src = self._raw_schema(self._connect("src"), t)
                dst = self._raw_schema(self._connect("dst"),
                                       self._on("dst", t))
            except SystemExit as e:
                res.append(Result("schema", scope, "error",
                                  str(e).split(":", 1)[-1].strip()[:160], "",
                                  "the schema of both sides has to be"
                                  " readable before anything can be compared"))
                continue
            more = (self._lengths_and_nulls(self._connect("src"), t),
                    self._lengths_and_nulls(self._connect("dst"),
                                            self._on("dst", t)))
            res.append(self._schema_result(
                scope, src, dst, more if all(m is not None for m in more)
                else None))
        self._close()
        return res

    #: a column's length and whether it takes NULL, as the catalogues
    #: name them: the standard `information_schema.columns`, and Oracle's
    #: `ALL_TAB_COLUMNS`
    LENGTH_AND_NULLS = (("character_maximum_length", "is_nullable"),
                        ("char_length", "nullable"))

    def _lengths_and_nulls(self, conn, table):
        """{column: (length or None, takes NULL)}, or None where the
        catalogue does not answer.

        The comparison library's own schema query selects five columns and
        not these two, per dialect. Its FROM and WHERE are the part that
        knows each catalogue's table and naming, so this asks the same
        place for the two columns it leaves out, in the standard spelling
        and then Oracle's. Measured on PostgreSQL 16: `varchar(50)` against
        `varchar(200)` and a NOT NULL on one side only, neither of which
        the five columns show. On a catalogue that has neither spelling
        the question fails, and the verdict keeps saying what it did not
        compare."""
        import re as _re
        try:
            base = conn.select_table_schema(self._path(conn, table))
        except Exception:
            return None
        at = _re.search(r"\sFROM\s", base, _re.I)
        if not at:
            return None
        for length, nulls in self.LENGTH_AND_NULLS:
            try:
                rows = conn.query(f"SELECT column_name, {length}, {nulls}"
                                  f"{base[at.start():]}", list)
            except Exception:
                continue
            if rows:
                return {str(r[0]): (None if r[1] is None else int(r[1]),
                                    str(r[2]).strip().upper()
                                    in ("YES", "Y", "TRUE", "1"))
                        for r in rows}
        return None

    def _schema_result(self, scope, src, dst, more=None):
        """`src` and `dst` are {column: catalogue row}, as `_schema` returns
        them; `more` is each side's `_lengths_and_nulls`, or None where
        either catalogue did not answer. Kept apart from the reading so
        every shape can be exercised without two servers."""
        missing = [c for c in src if c not in dst]
        extra = [c for c in dst if c not in src]
        drift = []
        for c in src:
            if c not in dst:
                continue
            if self._coltype(src[c]) != self._coltype(dst[c]):
                why = self._risk(src[c], dst[c])
                drift.append(f"{c} is {self._describe(src[c])} on the source"
                             f" and {self._describe(dst[c])} on the target"
                             + (f" - {why}" if why else ""))
            if more:
                drift += self._length_and_null_drift(c, more[0].get(c),
                                                     more[1].get(c))
        parts = []
        if missing:
            parts.append(f"{len(missing)} columns the target does not have: "
                         + ", ".join(sorted(missing)[:6]))
        if extra:
            parts.append(f"{len(extra)} columns only the target has: "
                         + ", ".join(sorted(extra)[:6]))
        parts += drift[:6]
        blind = (" String lengths and nullability compared too." if more
                 else f" {self.BLIND_SPOTS}")
        if parts:
            return Result("schema", scope, "diff",
                          "; ".join(parts) + "." + blind, "",
                          "align the target's columns with the source's"
                          " before moving data into them")
        return Result("schema", scope, "ok",
                      f"{len(src)} columns, same names and same declared"
                      f" types on both sides." + blind)

    @staticmethod
    def _length_and_null_drift(column, src, dst):
        """What differs in a column's length and nullability, and which way
        the risk runs."""
        if not src or not dst:
            return []
        out = []
        (s_len, s_null), (d_len, d_null) = src, dst
        if s_len is not None and d_len is not None and s_len != d_len:
            out.append(f"{column} holds {s_len} characters on the source and"
                       f" {d_len} on the target - "
                       + ("values longer than that will not fit"
                          if d_len < s_len else
                          "wider on the target, which holds every source"
                          " value"))
        if s_null != d_null:
            out.append(f"{column} " + (
                "takes NULL on the source and not on the target - a NULL the"
                " source holds is refused" if s_null else
                "is NOT NULL on the source and takes NULL on the target"))
        return out

    def moved_nothing(self, db):
        """The tables the source has rows in and the target has none of,
        counted through the driver - a row limit has no one spelling
        across dialects, a count has. None where a side cannot be read."""
        empty = []
        try:
            for t in self._tables(db):
                counts = []
                for side in ("src", "dst"):
                    conn = self._connect(side)
                    counts.append(int(conn.query(
                        f"SELECT COUNT(*) FROM {self._from(conn, t)}", int)
                        or 0))
                if counts[0] and not counts[1]:
                    empty.append(t)
        except (Exception, SystemExit):  # noqa: BLE001 - cannot answer
            return None
        finally:
            self._close()
        return empty

    def check_counts(self, db):
        bad = []
        blind = []
        tables = self._tables(db)
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
                              "no count came back for " + "; ".join(blind[:6])
                              + " - a table nobody could count is not a table"
                                " whose counts match"))
        if bad:
            res.append(Result("counts", db, "diff", clauses(bad), "",
                              "counted from the keys on one side only, which"
                              " is the part of the comparison that holds"
                              " still between runs"))
        return res or [Result("counts", db, "ok",
                              f"{len(tables)} tables, the same number of rows"
                              " on both sides")]

    #: how many offending key values a verdict names before it stops
    DEEP_EXAMPLES = 5

    def check_deep(self, db):
        """Check the assumption every other verdict on this engine rests on.

        The comparison matches rows by key and the repair addresses them by
        key, so a key that is not one row per value makes both of them
        answer about something other than the data. Two ways it breaks, both
        measured on PostgreSQL 16:

        **A row whose key is NULL is invisible.** Source with four rows, one
        of them keyed NULL, against a target with the other three:

            3 rows in table A
            3 rows in table B
            0 rows exclusive to table A (not present in B)
            0.00% difference score

        and migkit's own checks on that same pair: counts **OK** (`the same
        number of rows on both sides`), data **OK** (`no row is on one side
        only`), schema **OK**. A migration that dropped every NULL-keyed row
        would be reported as complete.

        **Duplicate key values make the answer stop being repeatable.**
        Measured across two servers - which is what every real hop is - a
        source holding `4/y` and `4/z` against a target holding `4/q`, four
        runs of the same command on the same unchanged pair:

            run 1   0 exclusive A, 0 exclusive B, 1 updated, 20.00%
            run 2   0 exclusive A, 0 exclusive B, 1 updated, 20.00%
            run 3   ERROR -           (with no message after it)
            run 4   ERROR -

        The truth is two rows on the source alone and one on the target
        alone. Pointing both URLs at the *same* server instead makes it
        refuse outright with `ERROR - Duplicate primary keys` - the safer
        failure, and the one a real migration never sees.
        """
        res = []
        key = self._key()
        for t in self._tables(db):
            scope = f"{db}.{t}" if db != "-" else t
            for side, where in (("source", "src"), ("target", "dst")):
                try:
                    res.append(self._deep_key(scope, side, where, t, key))
                except SystemExit as e:
                    res.append(Result(
                        "deep", f"{scope} key ({side})", "error",
                        str(e).split(":", 1)[-1].strip()[:160], "",
                        "the key has to be readable on both sides before"
                        " any verdict about the rows means anything"))
        self._close()
        return res

    def _deep_key(self, scope, side, which, t, key):
        from sqeleton.queries import code, or_, table, this
        conn = self._connect(which)
        tbl = table(*self._path(conn, t))
        named = ", ".join(key)

        # rebuilt per query on purpose: `this.id` is resolved in place the
        # first time it is compiled, and the same list handed to a second
        # query raises `Already resolved!`. The repair path documents the
        # same trap; it is a property of the builder, not of either caller.
        def cols():
            return [getattr(this, k) for k in key]

        try:
            blank = conn.query(
                tbl.where(or_(*[c == None for c in cols()]))  # noqa: E711
                   .select(code("count(*)")), list)[0][0]
            dups = conn.query(
                tbl.group_by(*cols()).agg(code("count(*)"))
                   .having(code("count(*) > 1")).limit(self.DEEP_EXAMPLES),
                list)
        except Exception as e:
            raise SystemExit(f"cannot read the key of {t}:"
                             f" {str(e).splitlines()[-1][:160]}")
        if blank:
            return Result(
                "deep", f"{scope} key ({side})", "diff",
                f"{blank} rows on the {side} have no value for {named} -"
                " a row with no key cannot be matched against the other"
                " side, so it is left out of the comparison entirely and"
                " every other verdict for this table is reported over a"
                " smaller table than the one you have", "",
                "give those rows a key, or key the hop on a column that is"
                " always filled, then run the check again")
        if dups:
            shown = ", ".join("/".join(str(v) for v in row[:len(key)])
                              for row in dups)
            return Result(
                "deep", f"{scope} key ({side})", "diff",
                f"{named} names more than one row on the {side} ({shown}) -"
                " rows are matched by key, so with a key that names several"
                " the comparison cannot say which it matched and stops"
                " being repeatable. Measured on one unchanged pair, four"
                " runs in a row: two answered `1 row updated` and two"
                " failed with an empty error, where the truth was two rows"
                " on one side and one on the other. No verdict about this"
                " table's rows means anything until the key is one", "",
                "key the hop on a column or set of columns that is unique,"
                " or add a unique index, then run the check again")
        return Result("deep", f"{scope} key ({side})", "ok",
                      f"{named} is filled and unique on the {side}, so every"
                      " row can be matched and addressed")

    # ---- the way one table is compared ----------------------------------

    #: rows sampled to measure a row's width before the whole table is read
    SAMPLE_ROWS = 200
    #: what a row held in this machine costs over its rendered bytes: the
    #: driver's tuple, its values and the digest kept per row
    HELD_FACTOR = 4.0
    #: the share of this machine's free memory one table's read may take
    MEMORY_SHARE = 0.5

    def compare_facts(self, table):
        """What the ways of comparing `table` stand on, each read once and
        only for a rung still in the running (`decide.Facts`)."""
        from .. import decide
        return decide.Facts(
            {"key": tuple(self._key())},
            comparer=lambda: bool(which("reladiff")),
            columns=lambda: self._side_columns("src", table),
            unique=lambda: self._unique(table),
            held=lambda: self._held_bytes(table),
            memory=self._memory)

    @staticmethod
    def _memory():
        from ..sizing import _memory
        return _memory()

    def compare_rungs(self):
        """The ways one table is compared, as the decision engine's rungs.

        Key ranges: both servers fold a range of the key into one checksum
        and only the ranges that differ are split again, so a table too
        large for one read, and a slow link, carry checksums, never rows.
        It stands on one key column that names one row: a key naming
        several makes the answer change between runs (`check_deep`). Row
        digests in this machine: each side's rows read through the driver,
        every value rendered one way (`canon`) and digested, the two
        multisets compared by key where the table has one and by the whole
        row where it has none - any key width or type, a key that is not
        unique, a table with no key at all. It stands on the table fitting
        the memory it may take, measured from a sample of its rows."""
        from .. import decide
        keyed = decide.Need(
            "the hop's key among the table's columns", self._keyed,
            lambda f: "the table has no column named "
                      + ", ".join(self._absent(f) or f["key"] or ("a key",)))
        return (
            decide.Rung(
                "key-ranges", "key ranges checksummed on both servers",
                frozenset({"rows proved", "rows named by key"}),
                needs=(keyed,
                       decide.Need(
                           "one key column", lambda f: len(f["key"]) == 1,
                           lambda f: f"{len(f['key'])} key columns: ranges"
                                     " are split on one column, so a key of"
                                     " several is compared row by row"),
                       decide.Need(
                           "a key the catalogue does not show as repeated",
                           lambda f: f["unique"] is not False,
                           "{rung} needs a key that names one row, and the"
                           " catalogue holds no unique rule on it: rows"
                           " sharing a key make the ranges' answer change"
                           " between runs"),
                       decide.Need(
                           "the comparison component installed",
                           lambda f: f["comparer"],
                           "the comparison component is not installed on"
                           " this machine: migkit doctor --install puts it"
                           " in place", programs=("reladiff",))),
                because="only the checksums of ranges cross the link, and"
                        " only the ranges that differ are read again",
                leaves="nothing", external=True),
            decide.Rung(
                "row-digests", "row digests compared in this machine",
                frozenset({"rows proved", "rows named by key", "keyless"}),
                needs=(decide.Need(
                    "the table's rows within the memory this machine may"
                    " take", self._fits, self._fits_why),),
                because="every row is read and digested here, by key or"
                        " by its whole self, whatever the key's shape",
                leaves="nothing", footprint=1),
        )

    def _keyed(self, f):
        return bool(f["key"]) and not self._absent(f)

    @staticmethod
    def _absent(f):
        """The hop's key columns the table does not have; none where its
        columns were not read."""
        cols = {c.lower() for c in (f["columns"] or ())}
        return [k for k in f["key"] if cols and k.lower() not in cols]

    def _fits(self, f):
        held, room = f["held"], f["memory"]
        return held is not None and room is not None \
            and held <= room * self.MEMORY_SHARE

    def _fits_why(self, f):
        held, room = f["held"], f["memory"]
        if held is None:
            return "the table's rows could not be counted or sampled"
        if room is None:
            return "this machine's free memory could not be read"
        return (f"the table would take {held / 2**20:,.0f} MiB held here"
                f" and {room * self.MEMORY_SHARE / 2**20:,.0f} MiB may be"
                " taken: only key ranges compare it")

    def compare_way(self, table, need=("rows proved",)):
        """(`decide.Choice`, facts): how `table` is compared. A table the
        hop's key does not name is compared only by a way that carries a
        table without one."""
        from .. import decide
        facts = self.compare_facts(table)
        if not self._keyed(facts):
            need = tuple(need) + ("keyless",)
        return decide.climb(f"compare {table}", self.compare_rungs(), facts,
                            need=need), facts

    def _side_columns(self, which, table):
        """The table's column names on one side, or () where the
        catalogue does not answer."""
        try:
            return tuple(self._raw_schema(self._connect(which),
                                          self._on(which, table)))
        except (Exception, SystemExit):  # noqa: BLE001 - unread, said
            return ()

    def _unique(self, table):
        """Whether the key is one column the source's catalogue holds
        unique: True, False, or None where the catalogue does not say -
        the key's own check (`check_deep`) answers that case."""
        key = self._key()
        try:
            conn = self._connect("src")
            got = {c.lower() for c in conn.query_table_unique_columns(
                self._path(conn, table))}
        except (Exception, SystemExit):  # noqa: BLE001 - not said there
            return None
        return len(key) == 1 and key[0].lower() in got

    def _from(self, conn, table):
        return ".".join(conn.dialect.quote(p) for p in self._path(conn, table))

    def _held_bytes(self, table):
        """What the source's rows would take held in this machine: its
        count times the rendered width of a sample of its rows, times what
        a held row costs over its bytes; None where either is unread."""
        try:
            conn = self._connect("src")
            path = self._from(conn, table)
            rows = int(conn.query(f"SELECT count(*) FROM {path}",
                                  list)[0][0])
            sample = conn.query(
                f"SELECT * FROM {path} "
                + conn.dialect.offset_limit(None, self.SAMPLE_ROWS), list)
        except (Exception, SystemExit):  # noqa: BLE001 - unread, said
            return None
        if not rows:
            return 0
        width = (sum(len(self._row_text(r)) for r in sample)
                 / max(1, len(sample)))
        return int(rows * max(width, 1) * self.HELD_FACTOR)

    @staticmethod
    def _cell(value):
        """One value as one text on every engine (`canon.render_value`),
        whatever type the driver hands back."""
        from ..canon import NUMBER, digest_value
        if value is None:
            return None
        if isinstance(value, bool):
            return str(int(value))
        for cls, types in (("integer", (int,)), ("float", (float,)),
                           (NUMBER, (decimal.Decimal,)),
                           ("bytes", (bytes, bytearray, memoryview)),
                           ("text", (str,))):
            if isinstance(value, types):
                return digest_value(cls, value)
        if isinstance(value, (datetime.datetime, datetime.date,
                              datetime.time)):
            return value.isoformat()
        return str(value)

    @classmethod
    def _row_text(cls, row):
        return "\x1f".join("\x00" if v is None else v
                           for v in map(cls._cell, row))

    def _side_rows(self, which, table, columns):
        """Every row of one side, its columns in `columns`' order."""
        conn = self._connect(which)
        cols = ", ".join(conn.dialect.quote(c) for c in columns)
        pred = self._filter(table)
        on = self._on(which, table)
        try:
            return conn.query(f"SELECT {cols} FROM {self._from(conn, on)}"
                              + (f" WHERE {pred}" if pred else ""), list)
        except Exception as e:  # noqa: BLE001 - said as the table's error
            self._grant_refused(which, "reading rows", on, e)
            raise SystemExit(f"cannot read {table} on the"
                             f" {'source' if which == 'src' else 'target'}:"
                             f" {str(e).splitlines()[-1][:160]}")

    def _client_digests(self, table, facts):
        """(rows only on the source, only on the target, changed keys,
        (missing keys, extra keys)): each side's rows digested here and
        the two multisets compared.

        With a key, rows are matched by it, and a key holding other values
        on the two sides is a changed row - a key the table repeats is
        matched as the multiset of its rows, and a row with no key value is
        matched as `[null]` rather than left out. Without one, the whole row
        is its own key: a row on one side more often than on the other is
        counted there, and none of them can be named for a repair."""
        import collections
        import hashlib
        columns = list(facts["columns"])
        if not columns:
            raise SystemExit(f"the columns of {table} could not be read on"
                             " the source")
        lower = {c.lower(): c for c in columns}
        key = ([lower[k.lower()] for k in facts["key"]]
               if self._keyed(facts) else [])
        order = key + sorted((c for c in columns if c not in key),
                             key=str.lower)
        width = len(key)

        def fold(which):
            got = collections.defaultdict(collections.Counter)
            for row in self._side_rows(which, table, order):
                digest = hashlib.blake2b(
                    self._row_text(row).encode("utf-8", "surrogatepass"),
                    digest_size=16).hexdigest()
                name = (json.dumps([self._cell(v) for v in row[:width]])
                        if width else digest)
                got[name][digest] += 1
            return got

        src, dst = fold("src"), fold("dst")
        missing, extra, changed = [], [], []
        only_src = only_dst = 0
        for name in src.keys() | dst.keys():
            a = src.get(name, collections.Counter())
            b = dst.get(name, collections.Counter())
            if a == b:
                continue
            if not width:
                only_src += sum((a - b).values())
                only_dst += sum((b - a).values())
            elif not b:
                missing.append(name)
            elif not a:
                extra.append(name)
            else:
                changed.append(name)
        if width:
            only_src, only_dst = len(missing), len(extra)
        return only_src, only_dst, sorted(changed), (sorted(missing),
                                                     sorted(extra))

    def _digest_result(self, db, scope, table, facts, choice):
        """The data result of a table compared by row digests here."""
        only_a, only_b, changed, (missing, extra) = self._client_digests(
            table, facts)
        keyed = self._keyed(facts)
        self._write_drill(db, table, missing=missing if keyed else [],
                          changed=changed, extra=extra if keyed else [])
        parts = [f"{only_a} rows only on the source" if only_a else "",
                 f"{only_b} rows only on the target" if only_b else "",
                 f"{len(changed)} rows with different values"
                 if changed else ""]
        parts = [x for x in parts if x]
        detail = ("; ".join(parts) if parts else
                  "every row is on both sides as often, value for value")
        fix = ""
        if parts:
            fix = ("`migkit sync --db - --kind rows` names them and can put"
                   " them right" if keyed else
                   "a table without a key cannot have one row addressed:"
                   " key the hop on columns that name a row, then run the"
                   " check again")
        return Result("data", scope, "diff" if parts else "ok",
                      f"{detail}; compared by {choice.said}: {choice.reason}",
                      "", fix)

    def check_data(self, db, table=None, stream=None):
        res = []
        gate = self._gate()
        for t in ([table] if table else self._tables(db)):
            scope = f"{db}.{t}" if db != "-" else t
            try:
                got = self._row_parity(db, scope, t, gate)
            except SystemExit as e:
                got = Result("data", scope, "error", str(e)[:200])
            if stream:
                stream(f"{t}: {got.status}")
            res.append(got)
        return res

    def _row_parity(self, db, scope, t, gate):
        """Table `t`'s data Result, by the way of proving its rows alike
        `decide.row_parity` climbs: the servers folding key ranges into
        checksums and splitting only those that differ (its server digest,
        where the table's facts let the comparison component stand on its
        key - `compare_way`), else every row digested in this machine (its
        row text, where the rows fit the memory it may take). A comparison
        that comes back without its numbers switches to the next way."""
        from .. import decide
        way, facts = self.compare_way(t)
        parity = decide.Facts(
            {"key": None},
            **{"server digest": lambda: way.path == "key-ranges",
               "rows read here": lambda: way.path == "row-digests"
               or self._fits(facts)})

        def run(choice):
            if choice.rung.name == "row text":
                return self._digest_result(db, scope, t, facts, choice)
            return self._ranged_result(db, scope, t, gate)
        try:
            return decide.row_parity(f"{scope}: its rows alike", run,
                                     parity)[1]
        except decide.Exhausted as e:
            why = (str(e).splitlines() or [""])[-1] or way.reason
            return Result("data", scope, "error",
                          f"no way compares it here: {why}"[:300])

    def _ranged_result(self, db, scope, t, gate):
        with gate.unit():
            p = self._reladiff(t, ["-c", "%"], jobs=gate.permits)
        got = self._stats(p)
        if not got:
            # the comparison reports success whether it compared anything
            # or not, so the absence of its numbers is the only thing that
            # says it did not
            raise RuntimeError("no comparison came back: "
                               + self._why_no_stats(p))
        parts = [f"{got['only_a']} rows only on the source"
                 if got["only_a"] else "",
                 f"{got['only_b']} rows only on the target"
                 if got["only_b"] else "",
                 f"{got['updated']} rows with different values"
                 if got["updated"] else ""]
        parts = [x for x in parts if x]
        detail = ("; ".join(parts) if parts
                  else "no row is on one side only and no compared"
                       " column differs")
        return Result(
            "data", scope, "diff" if parts else "ok",
            detail + self._drill(db, t, bool(parts), gate.permits), "",
            "`migkit sync --db - --kind rows` names them and can put"
            " them right" if parts else "")

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

    def _raw_schema(self, conn, table):
        """The catalogue's own answer: {column: (name, declared type,
        datetime precision, numeric precision, numeric scale)}.

        Kept because normalising throws away the differences a migration
        cares about - measured, `bigint` and `integer` both come back as
        `Integer`, so the target that will overflow looks identical to the
        source that will not.
        """
        try:
            return conn.query_table_schema(self._path(conn, table))
        except Exception as e:
            raise SystemExit(f"cannot read the columns of {table}:"
                             f" {str(e).splitlines()[-1][:160]}")

    def _schema(self, conn, table):
        """Column names and types, as the classes every dialect shares.

        `_process_table_schema` is the step that turns `bigint` or `NUMBER`
        into `Integer`, so what comes back can be reasoned about without a
        per-database type table - which is the only way one repair can serve
        every engine reladiff speaks.

        **It refuses an empty table**, and an empty table is the ordinary
        state of a target that has not been loaded yet - which is exactly
        when a repair is asked for. Measured on PostgreSQL 16, same DDL on
        both sides, one row on the source and none on the target:

            src   Integer / String_VaryingAlphanum / Decimal / TimestampTZ
            dst   cannot read the columns of t: Table ('public','t')
                  appears to be empty

        Reading its source is what makes the fallback safe rather than a
        guess: `col_dict` is built from `dialect.parse_type` *before* any
        sampling happens, and the refinement step only sharpens text
        subtypes from the values it finds. With no rows there is nothing to
        sharpen, so the same two calls in the same order give the same
        answer - minus a distinction that does not exist in an empty table
        anyway.
        """
        path = self._path(conn, table)
        raw = self._raw_schema(conn, table)
        try:
            return conn._process_table_schema(path, raw)
        except ValueError as e:
            if "appears to be empty" not in str(e):
                raise SystemExit(f"cannot read the columns of {table}:"
                                 f" {str(e).splitlines()[-1][:160]}")
        except Exception as e:
            raise SystemExit(f"cannot read the columns of {table}:"
                             f" {str(e).splitlines()[-1][:160]}")
        return {row[0]: conn.dialect.parse_type(path, *row)
                for row in raw.values()}

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
        # never the rows the hop keeps by the changes it does not carry
        found = self._repairable(db, name,
                                 self._drill_tables(db).get(name, {}))
        if not any(found.values()):
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
        col = self.newer_wins_column()
        if col and changed:
            # the target's row is kept where the column the hop names is
            # later there (`newer_wins`), as on every other engine
            if col not in columns:
                raise SystemExit(
                    f"{name}: newer_wins names {col}, and it is not on both"
                    " sides, so which row is newer cannot be told. Nothing"
                    " was repaired on this table.")
            at = columns.index(col)
            theirs = rows_for(src, src_t, changed)

            def read(side, keys):
                got = theirs if side == "src" else held
                return {self._key_id(k): got[tuple(k)][at] for k in keys
                        if got.get(tuple(k)) is not None
                        and got[tuple(k)][at] is not None}
            changed, kept = self._keep_the_newer(name, changed, read)
            if kept:
                (self.hop.report_dir(db) / f"data-{name}.kept-newer"
                 ).write_text("".join(json.dumps(list(k)) + "\n"
                                      for k in kept))
                keep = {tuple(k) for k in kept}
                held = {k: r for k, r in held.items() if k not in keep}
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
        # every statement written down before any lands (`repairlog`): each
        # row's columns as text, as the target held it and as it will be.
        # The writer this engine borrows reads values back only by three
        # classes (`_value`), so a record here says what was done and is
        # put back by hand (`repair_io`)
        from .. import repairlog

        def texts(row):
            return None if row is None else [self._text(v) for v in row]
        record = repairlog.Record(undo_dir, name, "text", columns, key)
        for k in missing + changed:
            if k in rows:
                record.planned("write", k, texts(held.get(tuple(k))),
                               texts(rows[k]))
        for k in drop:
            record.planned("delete", k, texts(held.get(tuple(k))), None)
        record.ready()
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
        # one transaction: all of it landed at the commit
        record.applied(record.n)
        record.close()

    def repair_io(self, db, head):
        """The rows of a repair record are put back by hand here: the
        borrowed writer takes a value back from its text only for a number,
        a string or a truth value (`_value`), and a row put back with any
        other column guessed would be a row nobody had."""
        raise SystemExit(
            f"{head.get('table', '?')}: this engine's repairs are recorded"
            " statement by statement with each row as it was, and cannot be"
            " put back by migkit: the record is the undo file named beside"
            " it, to apply by hand")

    #: nor are a span's or a table's replaced whole (`repairways`)
    ROWS_PUT_BACK = False

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

    # ---- copying and following through the driver ------------------------

    #: a statement's n-th placeholder (from 1), by the paramstyle the DB-API
    #: module of the driver declares
    PLACEHOLDERS = {"qmark": "?", "format": "%s", "pyformat": "%s",
                    "numeric": ":{n}", "named": ":p{n}"}
    #: the table migkit owns on both sides: a marker row written on the
    #: source and seen on the target is the fence of a follow
    FENCE_TABLE = "migkit_fence"
    #: a column whose name says when its row last changed
    CHANGED_AT = re.compile(r"(^|_)(updated|modified|changed|last_?update"
                            r"|last_?modified|mtime|row_?version)(_|$)",
                            re.I)
    #: a declared type whose values only grow as rows change
    MONOTONE_TYPE = re.compile(r"timestamp|datetime|rowversion", re.I)
    #: what one batch of a copy holds at most, its rows rendered; the batch
    #: is resized to it from the widths measured on the rows just read
    BATCH_BYTES_MOST = 64 * 1024 * 1024
    #: rows a batch holds at most when the caller gives no chunk
    BATCH_ROWS = 10_000

    def _dbapi(self, side):
        """(call, paramstyle): `call(fn)` runs `fn` on the driver's own
        DB-API connection on the thread that opened it - a threaded driver
        keeps one per worker thread, and a cursor may not cross threads -
        and the placeholder style its module declares."""
        import sys
        conn = self._connect(side)
        if hasattr(conn, "_queue"):
            def call(fn):
                def inside():
                    if conn._init_error:
                        raise conn._init_error
                    return fn(conn.thread_local.conn)
                return conn._queue.submit(inside).result()
            kind = call(type)
        else:
            raw = getattr(conn, "_conn", None)
            if raw is None:
                raise SystemExit(f"the {self._said(side)}'s driver keeps no"
                                 " connection that runs statements")

            def call(fn):
                return fn(raw)
            kind = type(raw)
        parts = kind.__module__.split(".")
        style = None
        for n in range(len(parts), 0, -1):
            style = getattr(sys.modules.get(".".join(parts[:n])),
                            "paramstyle", None)
            if style:
                break
        if style not in self.PLACEHOLDERS:
            raise SystemExit(f"the {self._said(side)}'s driver declares no"
                             " placeholder style a statement can be written"
                             f" in ({style or 'none'}); nothing was written")
        return call, style

    @staticmethod
    def _said(side):
        return "source" if side == "src" else "target"

    def _quote(self, side, name):
        return self._connect(side).dialect.quote(name)

    def _marks(self, style, values, start=1):
        """(placeholders, parameters) for `values`, numbered from `start`."""
        mark = self.PLACEHOLDERS[style]
        marks = [mark.format(n=n) for n in range(start, start + len(values))]
        if style == "named":
            return marks, {f"p{n}": v for n, v in enumerate(values, start)}
        return marks, tuple(values)

    @staticmethod
    def _run(c, sql, params=(), commit=False, fetch=False):
        cur = c.cursor()
        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            got = cur.fetchall() if fetch else None
        finally:
            cur.close()
        if commit:
            c.commit()
        return got

    def _keyed_where(self, side, style, key, values, op="=", start=1):
        """(sql, parameters): rows whose key is `values` (`=`), or past it
        in the key's order (`>`: a many-column key compared column by
        column, written out so every dialect reads it)."""
        q = [self._quote(side, k) for k in key]
        if op == "=":
            marks, params = self._marks(style, list(values), start)
            return (" AND ".join(f"{c} = {m}" for c, m in zip(q, marks)),
                    params)
        flat = []
        for i in range(len(key)):
            flat += list(values[:i + 1])
        marks, params = self._marks(style, flat, start)
        ors, at = [], 0
        for i in range(len(key)):
            terms = [f"{q[j]} = {marks[at + j]}" for j in range(i)]
            terms.append(f"{q[i]} > {marks[at + i]}")
            at += i + 1
            ors.append("(" + " AND ".join(terms) + ")")
        return " OR ".join(ors), params

    @staticmethod
    def _carried(value):
        """A key or position value as JSON holds it, its type kept."""
        if isinstance(value, decimal.Decimal):
            return {"decimal": str(value)}
        if isinstance(value, datetime.datetime):
            return {"datetime": value.isoformat()}
        if isinstance(value, datetime.date):
            return {"date": value.isoformat()}
        if isinstance(value, (bytes, bytearray, memoryview)):
            return {"hex": bytes(value).hex()}
        return value

    @staticmethod
    def _uncarried(value):
        if not isinstance(value, dict):
            return value
        kind, text = next(iter(value.items()))
        return {"decimal": decimal.Decimal,
                "datetime": datetime.datetime.fromisoformat,
                "date": datetime.date.fromisoformat,
                "hex": bytes.fromhex}[kind](text)

    def _batch(self, rows, most):
        """The rows the next batch takes: as many as fit the bytes a batch
        may hold, measured on the rows just read, never more than `most`."""
        if not rows:
            return most
        width = sum(len(self._row_text(r)) for r in rows) / len(rows)
        return max(1, min(most, int(self.BATCH_BYTES_MOST
                                    // max(width, 1.0))))

    def list_move_tables(self, db):
        return [(t.rpartition(".")[0], t.rpartition(".")[2])
                for t in self._tables(db)]

    def copy_rungs(self):
        """The ways a table is copied through the driver, climbed by
        `decide.climb`: in the key's order, resumed after the last key the
        target committed, where the key is on both sides and the source
        holds it unique; whole otherwise, emptied and copied again if cut
        off."""
        from ..decide import Need, Rung
        return (
            Rung("key-order", "rows copied in the key's order, resumed"
                 " after the last key the target committed",
                 frozenset({"copied", "resume by key"}),
                 needs=(Need("the key on both sides",
                             lambda f: not f["key_absent"],
                             lambda f: "the key is not on both sides: "
                             + ", ".join(f["key_absent"])),
                        Need("a key the source holds unique",
                             lambda f: f["unique"] is not False,
                             "the source's catalogue does not hold the key"
                             " unique, so the last key copied does not"
                             " say which rows are done")),
                 because="a cut-off copy goes on from the last committed"
                         " key"),
            Rung("whole-table", "the table copied whole, emptied and"
                 " copied again if cut off", frozenset({"copied"}),
                 because="without a key held unique there is no order to"
                         " resume in"))

    def copy_way(self, table, columns):
        from .. import decide
        absent = [k for k in self._key() if k not in columns]
        facts = decide.Facts({"key_absent": absent,
                              "unique": False if absent
                              else self._unique(table)})
        return decide.climb(f"copy {table}", self.copy_rungs(), facts,
                            need=("copied",))

    def _copy_columns(self, table):
        src = self._side_columns("src", table)
        if not src:
            raise SystemExit(f"{table}: the source's catalogue gave no"
                             " columns; nothing was copied")
        dst = set(self._side_columns("dst", table))
        lost = [c for c in src if c not in dst]
        if lost:
            raise SystemExit(
                f"{table}: {', '.join(lost)} not on the target, so a row"
                " copied would lose them. Nothing was copied: add them to"
                " the target, or leave the table out of the hop")
        return list(src)

    def move_table(self, db, sch, tbl, chunk, ck, log):
        """One table copied through the standard driver: the source read
        by a cursor in batches (`fetchmany`), each batch written to the
        target in one statement (`executemany`, in the placeholder style
        the target's driver declares) and committed before the checkpoint
        moves. The way is climbed (`copy_way`); a copy cut off between a
        commit and its checkpoint finds the rows past the checkpoint on the
        target and removes them before it goes on, so no row lands
        twice."""
        table = f"{sch}.{tbl}" if sch else tbl
        st = ck.setdefault(self.move_key(db, sch, tbl), {})
        if st.get("done"):
            return
        try:
            self._copy_rows(table, chunk, st, ck, log)
        finally:
            self._close()

    def _copy_rows(self, table, chunk, st, ck, log):
        from .. import faults
        columns = self._copy_columns(table)
        choice = self.copy_way(table, columns)
        if choice.rung is None:
            raise SystemExit(f"{table}: {choice.reason}")
        way, key = choice.rung.name, self._key()
        if st.get("way") not in (None, way):
            # the facts changed under a cut-off copy: the rows it wrote are
            # its own, and go before the other way starts
            st.pop("after", None)
        st["way"] = way
        log(f"{table}: {choice.rung.said}")
        src_call, src_style = self._dbapi("src")
        dst_call, dst_style = self._dbapi("dst")
        src_t = self._from(self._connect("src"), table)
        dst_t = self._from(self._connect("dst"), self._on("dst", table))
        after = st.get("after") if way == "key-order" else None
        if after is not None:
            where, params = self._keyed_where(
                "dst", dst_style, key, [self._uncarried(v) for v in after],
                op=">")
            dst_call(lambda c: self._run(
                c, f"DELETE FROM {dst_t} WHERE {where}", params,
                commit=True))
        else:
            held = int(dst_call(lambda c: self._run(
                c, f"SELECT COUNT(*) FROM {dst_t}", fetch=True))[0][0])
            if held and not st.get("began"):
                raise SystemExit(
                    f"{table}: the target already holds {held:,} rows this"
                    " copy did not write, and copying on top would leave"
                    " them or write some twice. Nothing was copied: empty"
                    " the table on the target, or repair it with sync")
            if held:
                dst_call(lambda c: self._run(c, f"DELETE FROM {dst_t}",
                                             commit=True))
            st["moved"] = 0
        st["began"] = True
        ck.save()
        cols = ", ".join(self._quote("src", c) for c in columns)
        sql, params = f"SELECT {cols} FROM {src_t}", ()
        conds = [f"({pred})"] if (pred := self._filter(table)) else []
        if after is not None:
            where, params = self._keyed_where(
                "src", src_style, key, [self._uncarried(v) for v in after],
                op=">")
            conds.append(where)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        if way == "key-order":
            sql += " ORDER BY " + ", ".join(self._quote("src", k)
                                            for k in key)
        marks, _ = self._marks(dst_style, columns)
        insert = (f"INSERT INTO {dst_t} ("
                  + ", ".join(self._quote("dst", c) for c in columns)
                  + f") VALUES ({', '.join(marks)})")
        at = [columns.index(k) for k in key] if way == "key-order" else []
        most = max(1, int(chunk or self.BATCH_ROWS))
        cursor = src_call(lambda c: c.cursor())
        try:
            if params:
                src_call(lambda c: cursor.execute(sql, params))
            else:
                src_call(lambda c: cursor.execute(sql))
        except Exception as e:  # noqa: BLE001 - a refusal said as its grant
            self._grant_refused("src", "reading rows", table, e)
            raise
        size = min(most, 1000)
        try:
            while True:
                rows = src_call(lambda c: cursor.fetchmany(size))
                if not rows:
                    break
                batch = [self._marks(dst_style, list(r))[1] for r in rows]

                def write(c, batch=batch):
                    cur = c.cursor()
                    try:
                        cur.executemany(insert, batch)
                    finally:
                        cur.close()
                    c.commit()

                was = int(st.get("moved") or 0)
                prev = st.get("after") if at else None
                # in key order the batch is the rows past the last key
                # copied, which nothing of this copy wrote yet: counted
                # there, never over the whole target per failed batch
                span, span_args, base = "", (), was
                if prev is not None:
                    span, span_args = self._keyed_where(
                        "dst", dst_style, key,
                        [self._uncarried(v) for v in prev], op=">")
                    span, base = f" WHERE {span}", 0

                def held(span=span, span_args=span_args):
                    return int(dst_call(lambda c: self._run(
                        c, f"SELECT COUNT(*) FROM {dst_t}{span}",
                        span_args, fetch=True))[0][0])
                # a batch whose commit was lost on the way is counted on
                # the target before it is sent again: there whole, it is
                # not written twice; not there, it goes again
                faults.retried(
                    lambda: dst_call(write), side="dst",
                    operation="copy batch", writes=True, log=log,
                    label=table, check=faults.landed(
                        lambda base=base, n=len(batch): base + n, held,
                        before=base),
                    ledger=self.attempts().ledger,
                    owned=faults.Owned("dst", "copy batch", table, str(was)))
                st["moved"] = int(st.get("moved") or 0) + len(rows)
                if at:
                    st["after"] = [self._carried(rows[-1][i]) for i in at]
                ck.save()
                size = self._batch(rows, most)
                log(f"{table}: {st['moved']:,} rows copied")
        finally:
            src_call(lambda c: cursor.close())
        st["done"] = True
        ck.save()

    # ---- following by the column of when a row changed --------------------

    def _declared_types(self, side, table):
        """{column: its declared type} as the side's catalogue says."""
        conn = self._connect(side)
        return {name: str(row[1]) for name, row in
                self._raw_schema(conn, table).items()}

    def follow_rungs(self):
        """The ways a table's changes are found through the driver, climbed
        by `decide.climb`: the column the hop names as when a row changed
        (`newer_wins`), else the one column the catalogue names and types
        as it. A table with neither, or without its key on both sides, is
        not followed this way."""
        from ..decide import Need, Rung, fenced
        keyed = Need("a key on both sides",
                     lambda f: not f["key_absent"],
                     lambda f: "the key is not on both sides ("
                     + ", ".join(f["key_absent"]) + "), so a changed row"
                     " cannot be told from a new one")
        return fenced((
            Rung("named-column", "the column the hop names as when a row"
                 " changed", frozenset({"changes followed"}),
                 needs=(Need("a column the hop names",
                             lambda f: bool(f["named"]),
                             "the hop names no column as when a row"
                             " changed (newer_wins)"),
                        Need("the named column on both sides",
                             lambda f: f["named"] in f["both"],
                             lambda f: f"{f['named']} is not on both"
                                       " sides")),
                 because="the hop says which column it is"),
            Rung("catalogue-column", "the one column the catalogue gives"
                 " as when a row changed", frozenset({"changes followed"}),
                 needs=(Need("one column of when a row changed",
                             lambda f: len(f["found"]) == 1,
                             lambda f: "no column on both sides is named"
                             " and typed as when a row changed"
                             if not f["found"] else "more than one column"
                             " could be when a row changed ("
                             + ", ".join(f["found"]) + "): name one with"
                             " newer_wins"),),
                 because="its name and its type say it")), keyed)

    def follow_way(self, table):
        """(the column a table's changes are found by, or None; the
        choice)."""
        from .. import decide
        a = self._declared_types("src", table)
        b = self._declared_types("dst", table)
        both = [c for c in a if c in b]
        found = [c for c in both if self.CHANGED_AT.search(c)
                 and self.MONOTONE_TYPE.search(a[c])]
        named = self.newer_wins_column()
        facts = decide.Facts({"named": named, "both": both, "found": found,
                              "key_absent": [k for k in self._key()
                                             if k not in both]})
        choice = decide.climb(f"follow {table}", self.follow_rungs(), facts,
                              need=("changes followed",))
        if choice.rung is None:
            return None, choice
        return (named if choice.rung.name == "named-column"
                else found[0]), choice

    def _unfollowable(self):
        """[(table, why)] of the hop's tables no change column finds."""
        out = []
        for t in self._tables():
            column, choice = self.follow_way(t)
            if column is None:
                out.append((t, choice.reason))
        return out

    def _highest(self, side, table, column):
        call, _ = self._dbapi(side)
        got = call(lambda c: self._run(
            c, f"SELECT MAX({self._quote(side, column)}) FROM"
               f" {self._from(self._connect(side), table)}", fetch=True))
        call(lambda c: c.commit())
        return self._carried(got[0][0]) if got else None

    def _ensure_fence(self, side):
        """The fence table on this side, made where it is not there."""
        call, _ = self._dbapi(side)
        t = self._quote(side, self.FENCE_TABLE)
        try:
            call(lambda c: self._run(c, f"SELECT COUNT(*) FROM {t}",
                                     fetch=True))
        except Exception:  # noqa: BLE001 - not there yet, made below
            call(lambda c: c.rollback())
            call(lambda c: self._run(
                c, f"CREATE TABLE {t} (marker VARCHAR(64) NOT NULL PRIMARY"
                   " KEY, seq BIGINT NOT NULL)", commit=True))

    def _follow_point(self, db):
        """Where a follow begins: each table's highest change value and the
        source's last fence marker, read before the copy it follows."""
        refused = self._unfollowable()
        if refused:
            raise SystemExit(
                "; ".join(f"{t}: {why}" for t, why in refused[:6])
                + ". Nothing is followed: a table is followed by its key"
                " and a column of when its rows changed. Name the column"
                " with newer_wins, or move with writes stopped and prove it"
                " with check")
        for side in ("src", "dst"):
            self._ensure_fence(side)
        tables = {}
        for t in self._tables(db):
            column, _ = self.follow_way(t)
            tables[t] = {"column": column,
                         "at": self._highest("src", t, column)}
        call, _ = self._dbapi("src")
        seq = call(lambda c: self._run(
            c, f"SELECT MAX(seq) FROM {self._quote('src', self.FENCE_TABLE)}",
            fetch=True))[0][0]
        return {"tables": tables, "fence": int(seq or 0)}

    def tail_start(self, db, token_path):
        from .. import tailctl
        if token_path.exists():
            return False
        try:
            tailctl.begin(self, db, token_path, self._follow_point(db))
        finally:
            self._close()
        return True

    def _changed_since(self, table, column, at, most):
        """(columns, up to `most` rows at or past `at` in the change
        column's order). Rows at `at` itself are read again: a row
        committed later can carry the same value."""
        call, style = self._dbapi("src")
        columns = self._copy_columns(table)
        cols = ", ".join(self._quote("src", c) for c in columns)
        q = self._quote("src", column)
        sql = f"SELECT {cols} FROM {self._from(self._connect('src'), table)}"
        params = ()
        if at is not None:
            marks, params = self._marks(style, [self._uncarried(at)])
            sql += f" WHERE {q} >= {marks[0]}"
        sql += f" ORDER BY {q}"
        cursor = call(lambda c: c.cursor())
        try:
            if params:
                call(lambda c: cursor.execute(sql, params))
            else:
                call(lambda c: cursor.execute(sql))
            rows = call(lambda c: cursor.fetchmany(most))
        finally:
            call(lambda c: cursor.close())
            call(lambda c: c.commit())
        return columns, rows

    def _upsert(self, table, columns, rows):
        """Each row put on the target by its key: the target's row deleted
        and the source's inserted in one transaction, so no reader sees the
        row on neither side."""
        if not rows:
            return
        call, style = self._dbapi("dst")
        at = [columns.index(k) for k in self._key()]
        dst_t = self._from(self._connect("dst"), self._on("dst", table))
        marks, _ = self._marks(style, columns)
        insert = (f"INSERT INTO {dst_t} ("
                  + ", ".join(self._quote("dst", c) for c in columns)
                  + f") VALUES ({', '.join(marks)})")
        where, _ = self._keyed_where("dst", style, self._key(),
                                     [None] * len(at))
        delete = f"DELETE FROM {dst_t} WHERE {where}"

        def write(c):
            cur = c.cursor()
            try:
                cur.executemany(delete, [self._marks(
                    style, [r[i] for i in at])[1] for r in rows])
                cur.executemany(insert, [self._marks(style, list(r))[1]
                                         for r in rows])
            finally:
                cur.close()
            c.commit()
        call(write)

    def _carry_markers(self, since):
        """The source's fence markers past `since`: (rows, highest seq)."""
        call, _ = self._dbapi("src")
        rows = call(lambda c: self._run(
            c, f"SELECT marker, seq FROM"
               f" {self._quote('src', self.FENCE_TABLE)}", fetch=True))
        call(lambda c: c.commit())
        new = [(m, int(s)) for m, s in rows if int(s) > since]
        return new, max([since] + [s for _, s in new])

    def _follow_round(self, token, log):
        """One look: the fence markers read first, every table's changed
        rows carried, the markers written last - so a marker on the target
        means a whole round read every table after it was written."""
        markers, seq = self._carry_markers(int(token.get("fence") or 0))
        tables, went, settled = {}, 0, True
        for t, held in (token.get("tables") or {}).items():
            column, at = held["column"], held.get("at")
            columns, rows = self._changed_since(t, column, at,
                                                self.BATCH_ROWS)
            c = columns.index(column)
            was = self._uncarried(at)
            went += sum(1 for r in rows if r[c] is not None
                        and (was is None or r[c] > was))
            self._upsert(t, columns, rows)
            top = max((r[c] for r in rows if r[c] is not None),
                      default=None)
            tables[t] = {"column": column,
                         "at": self._carried(top) if top is not None
                         else at}
            if len(rows) >= self.BATCH_ROWS:
                settled = False
                log(f"{t}: more changed rows than one round reads; the"
                    " next round goes on from here")
        if markers:
            call, style = self._dbapi("dst")
            marks, _ = self._marks(style, ["", 0])
            sql = (f"INSERT INTO {self._quote('dst', self.FENCE_TABLE)}"
                   f" (marker, seq) VALUES ({', '.join(marks)})")

            def write(c):
                cur = c.cursor()
                try:
                    cur.executemany(sql, [self._marks(style, [m, s])[1]
                                          for m, s in markers])
                finally:
                    cur.close()
                c.commit()
            call(write)
        return {"tables": tables, "fence": seq}, went, settled

    def tail_apply(self, db, go, token_path, log):
        """Keep the target following the source by each table's column of
        when a row changed (`follow_way`): every round the rows at or past
        the saved value are read and put on the target by key. A row the
        source deletes leaves no value to find, so deletes are not
        followed - the full check finds them, and this says so."""
        from .. import tailctl
        log("rows deleted on the source are not followed by a column of"
            " when rows changed; the check after the move finds them")

        def look(token):
            try:
                return self._follow_round(token or {}, log)
            finally:
                self._close()

        def behind(token):
            ahead = []
            try:
                for t, held in ((token or {}).get("tables") or {}).items():
                    if self._highest("src", t, held["column"]) \
                            != held.get("at"):
                        ahead.append(t)
            finally:
                self._close()
            return (f"{len(ahead)} tables changed on the source since the"
                    " saved position" + "".join(f"; {t}" for t in ahead))
        tailctl.follow(self, db, go, token_path, log, "rows", look, behind)

    def src_lsn(self, db):
        """A fence: a new marker row written to the source's fence table,
        or None where no follow runs to carry it."""
        import uuid

        from .. import tailctl
        if not tailctl.alive(self.hop.report_dir(db), self.hop):
            return None
        call, style = self._dbapi("src")
        t = self._quote("src", self.FENCE_TABLE)
        marker = uuid.uuid4().hex
        try:
            seq = call(lambda c: self._run(c, f"SELECT MAX(seq) FROM {t}",
                                           fetch=True))[0][0]
            marks, params = self._marks(style, [marker, int(seq or 0) + 1])
            call(lambda c: self._run(
                c, f"INSERT INTO {t} (marker, seq) VALUES"
                   f" ({', '.join(marks)})", params, commit=True))
        finally:
            self._close()
        return marker

    def fence_wait(self, db, lsn, timeout=300):
        """Wait for the marker row `lsn` on the target: True once there,
        False on timeout, None where there is nothing to wait on."""
        import time

        from .. import tailctl
        if lsn is None:
            return None
        began = time.monotonic()
        try:
            while time.monotonic() - began < timeout:
                call, style = self._dbapi("dst")
                marks, params = self._marks(style, [lsn])
                got = call(lambda c: self._run(
                    c, f"SELECT COUNT(*) FROM"
                       f" {self._quote('dst', self.FENCE_TABLE)} WHERE"
                       f" marker = {marks[0]}", params, fetch=True))
                call(lambda c: c.commit())
                if int(got[0][0]):
                    return True
                time.sleep(tailctl.POLL)
        finally:
            self._close()
        return False

    # ---- rows compared by key, and only the changed ones -------------------

    def _rows_by_keys(self, side, table, columns, keys):
        """{key text: row} of the rows with these keys, `REPAIR_BATCH` keys
        a statement."""
        call, style = self._dbapi(side)
        key = self._key()
        at = [columns.index(k) for k in key]
        cols = ", ".join(self._quote(side, c) for c in columns)
        src = self._from(self._connect(side), table)
        got = {}
        for i in range(0, len(keys), self.REPAIR_BATCH):
            ors, values = [], []
            for k in keys[i:i + self.REPAIR_BATCH]:
                where, _ = self._keyed_where(side, style, key, list(k),
                                             start=len(values) + 1)
                ors.append(f"({where})")
                values += list(k)
            _, bound = self._marks(style, values)
            for row in call(lambda c: self._run(
                    c, f"SELECT {cols} FROM {src} WHERE " + " OR ".join(ors),
                    bound, fetch=True)):
                got[tuple(self._text(row[j]) for j in at)] = row
        call(lambda c: c.commit())
        return got

    def _differ(self, table, keys):
        """(missing, extra, changed) of `keys` (each a tuple of the key's
        texts), both sides read by key through the driver and compared in
        one rendering."""
        columns = self._copy_columns(table)
        a = self._rows_by_keys("src", table, columns, keys)
        b = self._rows_by_keys("dst", table, columns, keys)
        missing = [k for k in keys if k in a and k not in b]
        extra = [k for k in keys if k in b and k not in a]
        changed = [k for k in keys if k in a and k in b
                   and self._row_text(a[k]) != self._row_text(b[k])]
        return missing, extra, changed

    def _compare_pks(self, db, table, keys):
        """What still differs of the keys the check named, each line in its
        drilldown's own text."""
        lines = {tuple(str(v) for v in json.loads(k)): k for k in keys}
        try:
            got = self._differ(table, list(lines))
        finally:
            self._close()
        return tuple([lines[k] for k in kind] for kind in got)

    def _write_pk_files(self, db, table, missing, extra, changed):
        self._write_drill(db, table, missing=list(missing),
                          extra=list(extra), changed=list(changed))

    def delta_verify(self, db, limit=20000, log=None):
        """Verify only the rows changed since the last clean pass, found by
        each table's column of when a row changed (`follow_way`) and asked
        of both sides by key. A table's position moves only when it is
        clean; a table no column finds is said, never passed as clean. A
        row the source deleted leaves nothing to find: the full check
        finds it."""
        from .. import durable
        state = self.hop.report_dir(db) / "generic-delta.json"
        held = durable.read_state(state, "tables")
        res, now = [], dict(held or {})
        try:
            for t in self._tables(db):
                column, choice = self.follow_way(t)
                if column is None:
                    res.append(Result("delta", f"{db}.{t}", "skip",
                                      "not verified by change: "
                                      + choice.reason))
                elif held is None or t not in held:
                    now[t] = self._highest("src", t, column)
                    res.append(Result("delta", f"{db}.{t}", "ok",
                                      "baseline recorded, changes are"
                                      " verified from this point on"))
                else:
                    res.append(self._delta_table(db, t, column, held[t],
                                                 limit, now))
        finally:
            self._close()
        durable.write_json(state, {"tables": now})
        return res

    def _delta_table(self, db, t, column, at, limit, now):
        columns, rows = self._changed_since(t, column, at, limit + 1)
        c = columns.index(column)
        over = len(rows) > limit
        rows = rows[:limit]
        key_at = [columns.index(k) for k in self._key()]
        keys = list(dict.fromkeys(tuple(self._text(r[j]) for j in key_at)
                                  for r in rows))
        missing, extra, changed = self._differ(t, keys)
        if missing or extra or changed:
            self._write_drill(db, t,
                              missing=[json.dumps(list(k)) for k in missing],
                              extra=[json.dumps(list(k)) for k in extra],
                              changed=[json.dumps(list(k)) for k in changed])
            got, _ = self._delta_held(Result(
                "delta", f"{db}.{t}", "diff",
                f"of {len(keys)} changed rows: missing={len(missing)}"
                f" extra={len(extra)} changed={len(changed)}",
                str(self.hop.report_dir(db)),
                f"migkit sync {self.hop.name} --db {db} --kind rows",
                table=t))
            return got
        top = max((r[c] for r in rows if r[c] is not None), default=None)
        if top is not None:
            now[t] = self._carried(top)
        return Result("delta", f"{db}.{t}", "ok",
                      f"{len(keys)} changed rows match"
                      + ("; more changed than one pass reads, the next"
                         " goes on from here" if over else "")
                      + "; rows deleted on the source are found by the full"
                        " check")
