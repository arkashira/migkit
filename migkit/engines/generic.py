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

    def check_counts(self, db):
        bad = []
        for t in self._tables():
            p = self._reladiff(t, [])
            m = re.search(r"(\d+) rows in table A.*?(\d+) rows in table B",
                          p.stdout, re.S)
            if not m:
                bad.append(f"{t}: {p.stderr.strip().splitlines()[-1] if p.stderr else 'no output'}")
            elif m.group(1) != m.group(2):
                bad.append(f"{t} src={m.group(1)} dst={m.group(2)}")
        if bad:
            return [Result("counts", db, "diff", "; ".join(bad[:10]))]
        return [Result("counts", db, "ok", f"{len(self._tables())} tables")]

    def check_data(self, db, table=None, stream=None):
        res = []
        for t in ([table] if table else self._tables()):
            p = self._reladiff(t, ["-c", "%"])
            ok = p.returncode == 0 and "0 rows are different" in \
                (p.stdout + p.stderr)
            status = "ok" if ok else "diff" if p.returncode in (0, 1) else "error"
            if stream:
                stream(f"{t}: {status}")
            res.append(Result("data", f"{db}.{t}" if db != "-" else t, status,
                              (p.stdout or p.stderr).strip()[-200:]))
        return res
