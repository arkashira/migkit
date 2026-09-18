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
