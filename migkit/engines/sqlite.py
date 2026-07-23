import difflib
import hashlib
import time

from .base import Engine, RepairAction, Result


class SQLiteEngine(Engine):
    checks = ("schema", "counts", "autoinc", "data")

    def _path(self, side):
        ep = self.hop.source if side == "src" else self.hop.target
        return ep.options.get("path") or ep.host

    def _q(self, side, sql):
        import sqlite3
        conn = sqlite3.connect(f"file:{self._path(side)}?mode=ro", uri=True)
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()

    def databases(self):
        return ["main"]

    def _tables(self, side):
        return [r[0] for r in self._q(side,
                "select name from sqlite_master where type = 'table'"
                " and name not like 'sqlite_%' order by name")]

    def check_schema(self, db):
        def dump(side):
            rows = self._q(side, "select type, name, coalesce(sql, '')"
                                 " from sqlite_master"
                                 " where name not like 'sqlite_%'"
                                 " order by type, name")
            return "\n".join(f"{t} {n}\n{s};" for t, n, s in rows)

        a, b = dump("src"), dump("dst")
        d = self.hop.report_dir(db)
        (d / "schema-src.sql").write_text(a)
        (d / "schema-dst.sql").write_text(b)
        diff = [l for l in difflib.unified_diff(
                    a.splitlines(), b.splitlines(), "src", "dst", lineterm="")
                if l[:1] in "+-" and not l.startswith(("+++", "---"))]
        if not diff:
            return [Result("schema", db, "ok",
                           "tables, indexes, views, triggers identical")]
        (d / "schema.diff").write_text("\n".join(diff))
        return [Result("schema", db, "diff", f"{len(diff)} changed lines",
                       str(d / "schema.diff"),
                       "apply DDL from schema-src.sql on target")]

    def check_counts(self, db):
        bad = []
        ta = tb = 0
        for t in self._tables("src"):
            a = self._q("src", f'select count(*) from "{t}"')[0][0]
            try:
                b = self._q("dst", f'select count(*) from "{t}"')[0][0]
            except Exception:
                bad.append(f"{t} missing on target")
                continue
            ta += a
            tb += b
            if a != b:
                bad.append(f"{t} src={a} dst={b}")
        if bad:
            return [Result("counts", db, "diff", "; ".join(bad[:10]))]
        return [Result("counts", db, "ok", f"rows {ta:,}=={tb:,}")]

    def _seqs(self, side):
        try:
            return dict(self._q(side, "select name, seq from sqlite_sequence"))
        except Exception:
            return {}

    def check_autoinc(self, db):
        a, b = self._seqs("src"), self._seqs("dst")
        bad = [f"{t} src={v} dst={b.get(t)}" for t, v in sorted(a.items())
               if b.get(t) != v]
        if bad:
            return [Result("autoinc", db, "diff", "; ".join(bad), "",
                           f"migkit repair {self.hop.name} --db {db}"
                           " --kind sequences")]
        return [Result("autoinc", db, "ok",
                       f"{len(a)} counters, values match")]

    def _hash(self, side, t):
        h = hashlib.md5()
        n = 0
        try:
            for row in self._q(side, f'select * from "{t}" order by rowid'):
                h.update(repr(row).encode())
                n += 1
        except Exception as e:
            return f"error: {e}", -1
        return h.hexdigest(), n

    def check_data(self, db, table=None, stream=None):
        res = []
        for t in ([table] if table else self._tables("src")):
            ha, na = self._hash("src", t)
            hb, nb = self._hash("dst", t)
            status = "ok" if (ha, na) == (hb, nb) else "diff"
            if stream:
                stream(f"{t}: {status}")
            detail = (f"rows {na:,}=={nb:,}, md5 {ha} both sides"
                      if status == "ok"
                      else f"rows {na} vs {nb}, md5 {ha} vs {hb}")
            res.append(Result("data", f"{db}.{t}", status, detail, "",
                              "" if status == "ok" else
                              "recopy the table, sqlite files are cheap"))
        return res

    def repair_plan(self, db, kind):
        actions = []
        if kind in ("sequences", "all"):
            a, b = self._seqs("src"), self._seqs("dst")
            stmts = [f"update sqlite_sequence set seq = {v}"
                     f" where name = '{t}';"
                     f"  -- dst now {b.get(t, 'MISSING')}"
                     for t, v in sorted(a.items()) if b.get(t) != v]
            undo = [f"update sqlite_sequence set seq = {b[t]}"
                    f" where name = '{t}';"
                    for t in sorted(a) if t in b and b.get(t) != a[t]]
            same = sum(1 for t, v in a.items() if b.get(t) == v)
            if stmts:
                actions.append(RepairAction(
                    db, "sequences", stmts, undo,
                    f"{len(stmts)} counters differ, {same} already equal"))
        return actions

    def apply(self, db, action):
        import sqlite3
        conn = sqlite3.connect(self._path("dst"))
        try:
            for s in action.statements:
                conn.execute(s.split("  --")[0])
            conn.commit()
        finally:
            conn.close()

    def watch_sample(self, db):
        total = {"src": 0, "dst": 0}
        for side in ("src", "dst"):
            for t in self._tables(side):
                total[side] += self._q(side,
                                       f'select count(*) from "{t}"')[0][0]
        return {"db": db, "ts": time.time(),
                "src_rows": total["src"], "dst_rows": total["dst"]}
