import re

from ..util import run, which
from .base import Engine, Result


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

    def _reladiff(self, table, extra):
        if not which("reladiff"):
            raise SystemExit("reladiff not found, run bootstrap.sh")
        key = self.hop.options.get("key", "id")
        cmd = ["reladiff", self._url("src"), table, self._url("dst"), table,
               "--stats", "-j", str(self.hop.workers)]
        for k in ([key] if isinstance(key, str) else key):
            cmd += ["-k", k]
        cmd += extra
        return run(cmd, check=False, timeout=3600)

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
        for t in tables:
            p = self._reladiff(t, [])
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
        for t in ([table] if table else self._tables()):
            p = self._reladiff(t, ["-c", "%"])
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
            res.append(Result(
                "data", scope, status,
                "; ".join(parts) if parts
                else "no row is on one side only and no compared column"
                     " differs",
                "", "re-copy those rows and re-run" if parts else ""))
        return res
