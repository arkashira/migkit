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
        self._register(conn)
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()

    @staticmethod
    def _register(conn):
        """The two functions the cross-engine comparison needs.

        SQLite ships with neither. `md5` is simply absent, and the number it
        would have to be folded into does not exist either - measured,
        `sum()` over 60-bit values raises `integer overflow` past 2**63, and
        `total()` returns a real that has already lost the low digits.

        Both are supplied here, from `migkit.canon`, so the rendering and the
        arithmetic are the same ones the SQL engines use rather than a second
        definition that happens to agree today.
        """
        from .. import canon

        def render(value, cls):
            return canon.render_value(cls, value)

        class _Digest:
            def __init__(self):
                self.total = 0

            def step(self, text):
                self.total = canon.digest_step(self.total, text)

            def finalize(self):
                # text, not a number: Python integers have no width and
                # SQLite would hand a large one back as a float
                return str(self.total)

        conn.create_function("migkit_canon", 2, render, deterministic=True)
        conn.create_aggregate("migkit_digest", 1, _Digest)

    CANON_ENGINE = "sqlite"
    OVER_NETWORK = False

    def neutral_tables(self, side, db):
        return self._tables(side)

    def neutral_columns(self, side, db, table):
        return [(r[1], r[2]) for r in
                self._q(side, f'pragma table_info("{table}")')]

    def neutral_key(self, side, db, table):
        return [r[1] for r in
                self._q(side, f'pragma table_info("{table}")') if r[5]]

    def neutral_read(self, side, db, table, columns, after=None, limit=1000):
        names = [n for n, _ in columns]
        cols = ", ".join(f'"{n}"' for n in names)
        key = self.neutral_key(side, db, table)
        where = ""
        if key and after is not None:
            places = ", ".join(repr(v) if not isinstance(v, str)
                               else "'" + v.replace("'", "''") + "'"
                               for v in after)
            keys = ", ".join(f'"{k}"' for k in key)
            where = f" where ({keys}) > ({places})"
        order = (" order by " + ", ".join(f'"{k}"' for k in key)) if key else ""
        cap = f" limit {int(limit)}" if key else ""
        rows = [list(r) for r in
                self._q(side, f'select {cols} from "{table}"'
                              f"{where}{order}{cap}")]
        if not rows or not key:
            return (rows, None)
        idx = [names.index(k) for k in key if k in names]
        if len(idx) != len(key):
            return (rows, None)
        return (rows, tuple(rows[-1][i] for i in idx))

    def neutral_write(self, side, db, table, columns, rows):
        import sqlite3

        from .. import canon
        if not rows:
            return 0
        names = [n for n, _ in columns]
        cols = ", ".join(f'"{n}"' for n in names)
        place = ", ".join(["?"] * len(names))
        key = self.neutral_key(side, db, table)
        if key and all(k in names for k in key):
            sets = ", ".join(f'"{n}" = excluded."{n}"'
                             for n in names if n not in key)
            conflict = ", ".join(f'"{k}"' for k in key)
            tail = (f" on conflict ({conflict}) do update set {sets}"
                    if sets else f" on conflict ({conflict}) do nothing")
        else:
            tail = ""
        conn = sqlite3.connect(self._path(side))
        try:
            conn.executemany(f'insert into "{table}" ({cols})'
                             f" values ({place}){tail}",
                             [tuple(canon.sql_value(v) for v in r)
                              for r in rows])
            conn.commit()
        finally:
            conn.close()
        return len(rows)

    def neutral_create_sql(self, side, db, table, columns, key=()):
        from .. import canon
        defs = [f'"{n}" {canon.ddl_type("sqlite", c, w)}'
                for n, c, w in columns]
        if key:
            defs.append("primary key (" + ", ".join(f'"{k}"' for k in key)
                        + ")")
        return f'create table "{table}" (' + ", ".join(defs) + ")"

    def neutral_create(self, side, db, table, columns, key=()):
        import sqlite3
        if table in self._tables(side):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        ddl = self.neutral_create_sql(side, db, table, columns, key)
        conn = sqlite3.connect(self._path(side))
        try:
            conn.execute(ddl)
            conn.commit()
        finally:
            conn.close()
        return ddl

    def local_table(self, table):
        """The last component: this engine has no schemas to qualify with."""
        return str(table).split(".")[-1]

    def _apply_upsert(self, side, db, table, key, values):
        import sqlite3

        from .. import canon
        table = self.local_table(table)
        row = dict(key)
        row.update(values)
        names = sorted(row)
        cols = ", ".join(f'"{n}"' for n in names)
        marks = ", ".join(["?"] * len(names))
        sets = ", ".join(f'"{n}" = excluded."{n}"'
                         for n in names if n not in key)
        conflict = ", ".join(f'"{k}"' for k in sorted(key))
        tail = (f" on conflict ({conflict}) do update set {sets}" if sets
                else f" on conflict ({conflict}) do nothing")
        conn = sqlite3.connect(self._path(side))
        try:
            conn.execute(f'insert into "{table}" ({cols})'
                         f" values ({marks}){tail}",
                         [canon.sql_value(row[n]) for n in names])
            conn.commit()
        finally:
            conn.close()

    def _apply_delete(self, side, db, table, key):
        import sqlite3

        from .. import canon
        table = self.local_table(table)
        names = sorted(key)
        where = " and ".join(f'"{n}" = ?' for n in names)
        conn = sqlite3.connect(self._path(side))
        try:
            conn.execute(f'delete from "{table}" where {where}',
                         [canon.sql_value(key[n]) for n in names])
            conn.commit()
        finally:
            conn.close()

    def neutral_digest(self, side, db, table, columns):
        from .. import canon
        row = canon.row_expr("sqlite", columns)
        got = self._q(side, f"select count(*), {canon.digest_expr('sqlite', row)}"
                            f' from "{table}"')[0]
        return (int(got[0]), str(got[1]))

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
                           f"migkit sync {self.hop.name} --db {db}"
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
