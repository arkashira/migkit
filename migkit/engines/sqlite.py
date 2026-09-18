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

    CLIENT_TOOLS = ()

    def _server_versions(self):
        def ver(side):
            try:
                return self._q(side, "select sqlite_version()")[0][0]
            except Exception:
                return None
        return (ver("src"), ver("dst"))

    def _assess_extra(self):
        """What has to be true about two files before a migration starts.

        SQLite has no server to ask, so every question here is about the file
        and about settings that live on the connection rather than in the
        database. Two of them are the ones that bite:

        **Foreign keys are off by default.** Measured, `pragma foreign_keys`
        answers 0 on a fresh connection - SQLite parses the constraints and
        does not enforce them unless each connection turns them on. A source
        that has been written to for years with them off can hold rows that a
        target enforcing them will refuse, and the migration is where that is
        discovered.

        **A damaged file raises rather than reporting.** `pragma
        integrity_check` on a corrupted database does not come back with a
        list of problems; it raises `DatabaseError: database disk image is
        malformed`. A check that only read the returned rows would let that
        exception escape and take the whole assess with it, so both are
        handled.
        """
        import os
        items = []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "file", "item": item,
                          "detail": str(detail)})
        for side in ("src", "dst"):
            path = self._path(side)
            if not path:
                add("fail", f"{side} file path", "no path configured")
                continue
            if not os.path.exists(path):
                add("fail" if side == "src" else "warn",
                    f"{side} database file",
                    f"{path} does not exist"
                    + ("" if side == "src" else
                       " - a target file is created by the first write, so"
                       " this is only a problem if you expected it there"))
                continue
            size = os.path.getsize(path)
            add("pass", f"{side} database file", f"{path}, {size:,} bytes")
            try:
                rows = self._q(side, "pragma integrity_check")
                verdict = rows[0][0] if rows else "?"
                add("pass" if verdict == "ok" else "fail",
                    f"{side} integrity_check",
                    verdict if verdict == "ok"
                    else f"{len(rows)} problems, first: {verdict}")
            except Exception as e:
                add("fail", f"{side} integrity_check",
                    f"{str(e)[:90]} - the file cannot be read as a database,"
                    " so nothing below it means anything")
                continue
            try:
                fk = self._q(side, "pragma foreign_keys")[0][0]
                journal = self._q(side, "pragma journal_mode")[0][0]
            except Exception as e:
                add("warn", f"{side} settings",
                    f"{str(e)[:80]} - unknown, not clean")
                continue
            add("warn" if not fk else "pass", f"{side} foreign_keys",
                f"{fk}" + ("" if fk else
                           " - SQLite parses foreign keys and does not"
                           " enforce them unless each connection turns them"
                           " on, so rows here may not satisfy constraints a"
                           " target does enforce"))
            add("pass" if str(journal).lower() == "wal" else "warn",
                f"{side} journal_mode", f"{journal}"
                + ("" if str(journal).lower() == "wal" else
                   " - a reader and a writer block each other outside WAL,"
                   " so a long read holds up whatever writes to this file"))
        try:
            free = os.statvfs(os.path.dirname(
                os.path.abspath(self._path("dst") or ".")) or ".")
            room = free.f_bavail * free.f_frsize
            need = (os.path.getsize(self._path("src"))
                    if self._path("src") and os.path.exists(self._path("src"))
                    else 0)
            add("pass" if room > need * 2 else "warn",
                "room for the target",
                f"{room:,} bytes free where the target lives, source is"
                f" {need:,}")
        except Exception as e:
            add("warn", "room for the target",
                f"{str(e)[:80]} - unknown, not clean")
        return items

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

    def neutral_rows_by_key(self, side, db, table, columns, key, keys):
        import sqlite3
        if not key or not keys:
            return {}
        sql, args = self._by_key_query(f'"{table}"', columns, key, list(keys),
                                       lambda n: f'"{n}"', "?")
        conn = self._reader(side)
        try:
            rows = [list(r) for r in conn.execute(sql, args).fetchall()]
        finally:
            conn.close()
        return self._by_key_map(columns, key, rows)

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
        """Both directions: a table only the target has is a finding too.

        Measured, a target still holding a 40-row table from an earlier load
        came back `ok | rows 5==5` - this walked the source's tables and
        nothing walked the target's, so the leftover was never looked at.
        postgres and mysql both name an extra table right here, and `migkit
        check` is meant to be the same command underneath every engine.
        """
        src_tables = self._tables("src")
        try:
            dst_tables = set(self._tables("dst"))
        except Exception as e:
            return [Result("counts", db, "error",
                           f"target: {e} - nothing could be counted, which is"
                           " not the same as the counts matching")]
        bad = [f"{t} missing on target" for t in src_tables
               if t not in dst_tables]
        bad += [f"{t} extra on target"
                for t in sorted(dst_tables - set(src_tables))]
        common = [t for t in src_tables if t in dst_tables]
        ta = tb = 0
        for t in common:
            a = self._q("src", f'select count(*) from "{t}"')[0][0]
            b = self._q("dst", f'select count(*) from "{t}"')[0][0]
            ta += a
            tb += b
            if a != b:
                bad.append(f"{t} src={a} dst={b}")
        if bad:
            return [Result("counts", db, "diff", "; ".join(bad[:10]))]
        return [Result("counts", db, "ok",
                       f"{len(common)} tables, rows {ta:,}=={tb:,}")]

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

    CHUNK = 2000

    def _reader(self, side):
        """One read-only connection, held open across chunks."""
        import sqlite3
        conn = sqlite3.connect(f"file:{self._path(side)}?mode=ro", uri=True)
        self._register(conn)
        return conn

    def _row_walk(self, side, table):
        """The columns that put this table in a repeatable order.

        `rowid` is not something every table has. A WITHOUT ROWID table has
        none, and asking for one raises `no such column: rowid` - on both
        sides at once, which is how the old read turned two failures into
        agreement and called them equal.

        The declared primary key comes first because it is the same key in
        both files. rowid is only the fallback for a table that declares no
        key at all, and it is insertion order: two files holding the same rows
        loaded in a different order have different rowids, so a difference
        found that way is worth looking at before believing.
        """
        info = self._q(side, f'pragma table_info("{table}")')
        pk = [r[1] for r in sorted(info, key=lambda r: r[5]) if r[5]]
        if pk:
            return pk, False
        try:
            self._q(side, f'select rowid from "{table}" limit 1')
        except Exception:
            return None, False
        return ["rowid"], True

    def _hash(self, side, t):
        """(digest, rows, error) - read one chunk at a time.

        Chunking is not only about the memory a whole table takes. Measured on
        a WAL database with 157 frames waiting to be checkpointed: while a
        single `select` was part-way through, a passive checkpoint moved **0
        of 157** frames, and the moment that statement finished the same
        checkpoint moved all 157. A whole-table read therefore pins the
        write-ahead log for as long as it runs - the same file measured 729KB
        of WAL under a writer alone and 2.1MB with a reader holding a snapshot
        - so whatever else writes to that file keeps growing it and cannot
        reclaim it. One finished chunk at a time bounds that to one chunk, and
        the reader connection can stay open: what releases the snapshot is the
        statement ending, which was measured too.

        An error is returned rather than folded into the digest, because two
        reads that failed identically are not two tables that agree.
        """
        import sqlite3
        try:
            order, use_rowid = self._row_walk(side, t)
        except Exception as e:
            return None, 0, str(e)
        if order is None:
            return None, 0, (f'"{t}" has neither a primary key nor a rowid,'
                             " so there is no repeatable order to read it in")
        cols = ", ".join("rowid" if use_rowid else f'"{c}"' for c in order)
        h = hashlib.md5()
        n = 0
        after = None
        conn = self._reader(side)
        try:
            while True:
                where = ""
                args = ()
                if after is not None:
                    marks = ", ".join(["?"] * len(order))
                    left = cols if len(order) == 1 else f"({cols})"
                    right = marks if len(order) == 1 else f"({marks})"
                    where = f" where {left} > {right}"
                    args = after
                rows = conn.execute(
                    f'select *, {cols} from "{t}"{where}'
                    f" order by {cols} limit {int(self.CHUNK)}",
                    args).fetchall()
                if not rows:
                    break
                for row in rows:
                    h.update(repr(row[:-len(order)]).encode())
                    n += 1
                after = tuple(rows[-1][-len(order):])
                if len(rows) < self.CHUNK:
                    break
        except Exception as e:
            return None, n, str(e)
        finally:
            conn.close()
        return h.hexdigest(), n, ""

    def check_data(self, db, table=None, stream=None):
        res = []
        try:
            present = set(self._tables("dst"))
        except Exception as e:
            return [Result("data", db, "error",
                           f"target: {e} - nothing below this could be"
                           " compared, which is not the same as nothing"
                           " differing")]
        for t in ([table] if table else self._tables("src")):
            if t not in present:
                if stream:
                    stream(f"{t}: diff")
                res.append(Result("data", f"{db}.{t}", "diff",
                                  "missing on target", "",
                                  "create and copy the table, sqlite files"
                                  " are cheap"))
                continue
            ha, na, ea = self._hash("src", t)
            hb, nb, eb = self._hash("dst", t)
            if ea or eb:
                why = "; ".join(f"{name}: {err}" for name, err in
                                (("source", ea), ("target", eb)) if err)
                if stream:
                    stream(f"{t}: error")
                res.append(Result("data", f"{db}.{t}", "error", why, "",
                                  "migkit could not read the table, which is"
                                  " not the same as the two sides agreeing"))
                continue
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
