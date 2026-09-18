import difflib
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from ..util import keepalive as _keepalive, run, which, with_retry
from .base import Engine, RepairAction, Result

SKIP_DBS = {"mysql", "sys", "performance_schema", "information_schema"}


class MySQLEngine(Engine):
    ENGINE_FAMILY = "mysql"
    checks = ("schema", "counts", "autoinc", "data")
    counts_from_data = True

    def _conn(self, side):
        ep = self.hop.source if side == "src" else self.hop.target
        try:
            import pymysql
        except ImportError:
            raise SystemExit("pip install 'migkit[mysql]' for mysql support")
        # read_timeout + TCP keepalive: without them a network blip on a
        # cross-cloud link leaves the socket open forever and a long scan
        # (count on a hundred-GB table) hangs the whole run with no error.
        import os
        rt = int(os.environ.get("MIGKIT_READ_TIMEOUT", "3600"))

        def _open():
            c = pymysql.connect(host=ep.host, port=ep.port, user=ep.user,
                                password=ep.password, charset="utf8mb4",
                                connect_timeout=15,
                                read_timeout=rt, write_timeout=rt)
            _keepalive(getattr(c, "_sock", None))
            return c
        return with_retry(_open, label=f"mysql connect {side}")

    CANON_ENGINE = "mysql"

    def neutral_tables(self, side, db):
        return self._tables(side, db)

    def neutral_columns(self, side, db, table):
        rows = self._q(side, "select column_name, column_type"
                             " from information_schema.columns"
                             " where table_schema=%s and table_name=%s"
                             " order by ordinal_position",
                       (self._d(side, db), table))
        return [(r[0], r[1]) for r in rows]

    def neutral_key(self, side, db, table):
        rows = self._q(side,
                       "select column_name from information_schema"
                       ".key_column_usage where table_schema=%s"
                       " and table_name=%s and constraint_name='PRIMARY'"
                       " order by ordinal_position",
                       (self._d(side, db), table))
        return [r[0] for r in rows]

    def neutral_read(self, side, db, table, columns, after=None, limit=1000):
        names = [n for n, _ in columns]
        cols = ", ".join(f"`{n}`" for n in names)
        key = self.neutral_key(side, db, table)
        where, args = "", []
        if key and after is not None:
            places = ", ".join(["%s"] * len(key))
            keys = ", ".join(f"`{k}`" for k in key)
            where = f" where ({keys}) > ({places})"
            args = list(after)
        order = (" order by " + ", ".join(f"`{k}`" for k in key)) if key else ""
        cap = f" limit {int(limit)}" if key else ""
        rows = [list(r) for r in self._q(
            side, f"select {cols} from `{self._d(side, db)}`.`{table}`"
                  f"{where}{order}{cap}", args or None)]
        if not rows or not key:
            return (rows, None)
        idx = [names.index(k) for k in key if k in names]
        if len(idx) != len(key):
            return (rows, None)
        return (rows, tuple(rows[-1][i] for i in idx))

    def neutral_write(self, side, db, table, columns, rows):
        if not rows:
            return 0
        from .. import canon
        names = [n for n, _ in columns]
        cols = ", ".join(f"`{n}`" for n in names)
        key = set(self.neutral_key(side, db, table))
        sets = ", ".join(f"`{n}` = values(`{n}`)"
                         for n in names if n not in key)
        # `on duplicate key update` rather than `replace into`: REPLACE
        # deletes the old row first, which fires delete triggers and drops
        # any column the incoming row does not carry
        tail = f" on duplicate key update {sets}" if sets else ""
        place = "(" + ", ".join(["%s"] * len(names)) + ")"
        conn = self._conn(side)
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    f"insert into `{self._d(side, db)}`.`{table}` ({cols})"
                    f" values {place}{tail}",
                    [tuple(canon.sql_value(v) for v in r) for r in rows])
            conn.commit()
        finally:
            conn.close()
        return len(rows)

    def neutral_create(self, side, db, table, columns, key=()):
        from .. import canon
        exists = self._q(side, "select count(*) from information_schema"
                               ".tables where table_schema=%s"
                               " and table_name=%s",
                         (self._d(side, db), table))[0][0]
        if exists:
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        defs = [f"`{n}` {canon.ddl_type('mysql', c, w)}"
                for n, c, w in columns]
        if key:
            defs.append("primary key (" + ", ".join(f"`{k}`" for k in key)
                        + ")")
        ddl = (f"create table `{self._d(side, db)}`.`{table}` ("
               + ", ".join(defs) + ")")
        conn = self._conn(side)
        try:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
        finally:
            conn.close()
        return ddl

    def local_table(self, table):
        """The last component: this engine has no schemas to qualify with."""
        return str(table).split(".")[-1]

    def _apply_upsert(self, side, db, table, key, values):
        from .. import canon
        table = self.local_table(table)
        row = dict(key)
        row.update(values)
        names = sorted(row)
        cols = ", ".join(f"`{n}`" for n in names)
        marks = ", ".join(["%s"] * len(names))
        sets = ", ".join(f"`{n}` = values(`{n}`)"
                         for n in names if n not in key)
        tail = f" on duplicate key update {sets}" if sets else ""
        conn = self._conn(side)
        try:
            with conn.cursor() as cur:
                cur.execute(f"insert into `{self._d(side, db)}`.`{table}`"
                            f" ({cols}) values ({marks}){tail}",
                            [canon.sql_value(row[n]) for n in names])
            conn.commit()
        finally:
            conn.close()

    def _apply_delete(self, side, db, table, key):
        from .. import canon
        table = self.local_table(table)
        names = sorted(key)
        where = " and ".join(f"`{n}` = %s" for n in names)
        conn = self._conn(side)
        try:
            with conn.cursor() as cur:
                cur.execute(f"delete from `{self._d(side, db)}`.`{table}`"
                            f" where {where}",
                            [canon.sql_value(key[n]) for n in names])
            conn.commit()
        finally:
            conn.close()

    def binlog_names(self, db, table, values):
        """Binlog row values keyed by column name rather than by position.

        `binlog_row_metadata` defaults to MINIMAL, and at MINIMAL the binlog
        carries no column names at all - the reader hands back
        `UNKNOWN_COL0`, `UNKNOWN_COL1` and so on. Setting it to FULL fixes
        that on a server you control, and a managed MySQL often will not let
        you, so the mapping has to exist either way.

        The index is the column's ordinal position, which is what
        `information_schema` orders by, so the two line up.
        """
        if not any(str(k).startswith("UNKNOWN_COL") for k in values):
            return values
        real = self._cols(db, table)
        out = {}
        for k, v in values.items():
            if str(k).startswith("UNKNOWN_COL"):
                i = int(str(k)[11:])
                if i >= len(real):
                    raise SystemExit(
                        f"{table}: the binlog has more columns than"
                        " information_schema does, which means the table was"
                        " altered after this event was written - replaying it"
                        " would put values in the wrong columns")
                out[real[i]] = v
            else:
                out[k] = v
        return out

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Row changes out of the binlog, as neutral records.

        Non-blocking on purpose: this returns what is there now and a token
        to come back with, rather than holding the connection open. A caller
        that wants to follow calls it again; one that wants a bounded catch-up
        gets a bounded one.

        A table with no primary key is skipped and named. Without a key there
        is no way to address the row on the target, and applying an UPDATE by
        matching every column would hit every duplicate of it.
        """
        from pymysqlreplication import BinLogStreamReader
        from pymysqlreplication.row_event import (DeleteRowsEvent,
                                                  UpdateRowsEvent,
                                                  WriteRowsEvent)

        from .. import canon
        ep = self.hop.source if side == "src" else self.hop.target
        for name, why in (
                ("binlog_row_metadata",
                 "without it the binlog carries no column names and no"
                 " charset per column, so the reader cannot tell a"
                 " varbinary from a varchar - measured, it tries to UTF-8"
                 " decode the binary and raises. The library's way around"
                 " that replaces the bytes it cannot decode, which is worse"
                 " than stopping"),
                ("binlog_row_image",
                 "without it an UPDATE's before image carries only the key,"
                 " so a column that was not part of the change arrives as"
                 " missing rather than unchanged")):
            got = self._q(side, f"select @@{name}")
            value = str(got[0][0]) if got else "?"
            if value.upper() != "FULL":
                raise SystemExit(
                    f"{name} is {value} on this server, and the tail needs"
                    f" FULL: {why}.\n"
                    f"    set global {name} = 'FULL';   -- self-managed\n"
                    f"    {name}=FULL                   -- parameter group")
        token = dict(token or {})
        if not token:
            pos = (self._q(side, "show binary log status")
                   or self._q(side, "show master status"))
            if not pos:
                raise SystemExit(
                    "the binlog is off on this server, so there is no change"
                    " log to read - turn on log_bin, or move without CDC")
            token = {"log_file": pos[0][0], "log_pos": int(pos[0][1])}
        stream = BinLogStreamReader(
            connection_settings={"host": ep.host, "port": ep.port,
                                 "user": ep.user, "passwd": ep.password},
            server_id=int(self.hop.options.get("server_id", 4379)),
            blocking=False, resume_stream=True,
            log_file=token.get("log_file"), log_pos=token.get("log_pos"),
            only_schemas=[self._d(side, db)],
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent])
        out, skipped = [], set()
        try:
            for ev in stream:
                table = ev.table
                keys = self._pk_cols(self._d(side, db), table)
                if not keys:
                    skipped.add(table)
                    continue
                named = self._d(side, db)
                for row in ev.rows:
                    if isinstance(ev, WriteRowsEvent):
                        vals = self.binlog_names(named, table, row["values"])
                        out.append(canon.change(
                            "insert", table,
                            {k: vals[k] for k in keys}, vals))
                    elif isinstance(ev, UpdateRowsEvent):
                        before = self.binlog_names(named, table,
                                                   row["before_values"])
                        after = self.binlog_names(named, table,
                                                  row["after_values"])
                        # the key from the *before* image: an UPDATE that
                        # moved the primary key has to find the old row
                        out.append(canon.change(
                            "update", table,
                            {k: before[k] for k in keys}, after))
                    else:
                        vals = self.binlog_names(named, table, row["values"])
                        out.append(canon.change(
                            "delete", table, {k: vals[k] for k in keys}))
                token = {"log_file": stream.log_file,
                         "log_pos": stream.log_pos}
                if len(out) >= limit:
                    break
        finally:
            stream.close()
        if skipped:
            raise SystemExit(
                f"no primary key on {', '.join(sorted(skipped))} - a change"
                " to a keyless table cannot be addressed on the target, and"
                " applying it by matching every column would hit every"
                " duplicate. Add a key, or exclude the table")
        return out, token

    def neutral_digest(self, side, db, table, columns):
        from .. import canon
        row = canon.row_expr("mysql", columns)
        r = self._q(side, f"select count(*),"
                          f" {canon.digest_expr('mysql', row)}"
                          f" from `{self._d(side, db)}`.`{table}`")
        return (int(r[0][0]), str(r[0][1]))

    def _brand_probes(self):
        """`version()` and `@@version_comment` per side, in one round trip.

        Two fields because the forks split across them: MariaDB puts its name
        in both (measured: `11.8.9-MariaDB-ubu2404` /
        `mariadb.org binary distribution`), TiDB and Vitess put theirs in the
        version string, and Percona only shows up in the comment.
        """
        def one(side):
            try:
                r = self._q(side, "select version(), @@version_comment")
            except Exception:
                return {}
            if not r:
                return {}
            return {"version": r[0][0], "version_comment": r[0][1]}
        return (one("src"), one("dst"))

    def _q(self, side, sql, args=None, fresh=False):
        # retry the connect+query as a unit so a TLS/socket blip clears
        def once():
            conn = self._conn(side)
            try:
                with conn.cursor() as cur:
                    if fresh:
                        try:
                            cur.execute("set session"
                                        " information_schema_stats_expiry = 0")
                        except Exception:
                            pass
                    cur.execute(sql, args)
                    return cur.fetchall()
            finally:
                conn.close()
        return with_retry(once, label=f"mysql query {side}")

    def _rows_utc(self, side, sql):
        """Read with the session time_zone pinned to +00:00 so UNIX_TIMESTAMP
        on a DATETIME is comparable across sides regardless of server tz -
        the mysql equivalent of the postgres UTC pin used for tz-shift audit."""
        def once():
            conn = self._conn(side)
            try:
                with conn.cursor() as cur:
                    try:
                        cur.execute("set time_zone = '+00:00'")
                    except Exception:
                        pass
                    cur.execute(sql)
                    return cur.fetchall()
            finally:
                conn.close()
        return with_retry(once, label=f"mysql utc {side}")

    def _d(self, side, db):
        """Resolve the physical db name for a side. Source keeps the given
        name; target goes through the hop's db_map so a migration can land
        in a differently-named database (identity when unmapped)."""
        return self.hop.target_db(db) if side == "dst" else db

    @staticmethod
    def _canon_ddl(text):
        """Normalize a mysqldump so cosmetic mover artifacts don't read as
        schema drift: strip per-table AUTO_INCREMENT counters and DEFINER
        clauses, and collapse "CHARACTER SET x COLLATE y" to the bare
        "COLLATE y" (a collation already implies its charset, so the two
        forms are identical - DTS emits one, the source the other). Real
        charset/collation differences still surface via the COLLATE name."""
        text = re.sub(r" AUTO_INCREMENT=\d+", "", text)
        text = re.sub(r"DEFINER=`[^`]*`@`[^`]*`", "", text)
        text = re.sub(r"CHARACTER SET \w+ COLLATE", "COLLATE", text)
        # DTS downgrades routines to SQL SECURITY INVOKER; atlas treats this
        # as non-structural, so drop the clause to agree (raw dumps keep it)
        text = re.sub(r"\s*SQL SECURITY (DEFINER|INVOKER)", "", text)
        return text

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        rows = self._q("src", "show databases")
        return sorted(r[0] for r in rows if r[0] not in SKIP_DBS
                      and not r[0].startswith("__")
                      and not self.hop.excluded(r[0]))

    def _dump_schema(self, side, db):
        ep = self.hop.source if side == "src" else self.hop.target
        if not which("mysqldump"):
            raise SystemExit("mysqldump not found, run bootstrap.sh")
        pdb = self._d(side, db)
        p = run(["mysqldump", "-h", ep.host, "-P", str(ep.port), "-u", ep.user,
                 f"-p{ep.password}", "--no-data", "--routines", "--triggers",
                 "--events", "--skip-comments", "--skip-dump-date",
                 "--column-statistics=0",
                 f"--ignore-table={pdb}.migkit_changelog", pdb], check=False)
        if p.returncode != 0:
            raise RuntimeError(p.stderr.strip())
        text = self._canon_ddl(p.stdout)
        lines = [l for l in text.splitlines()
                 if not l.startswith("--")
                 and "GTID_PURGED" not in l
                 and "SQL_LOG_BIN" not in l
                 and l.strip()]
        return "\n".join(lines) + "\n"

    def check_schema(self, db):
        d = self.hop.report_dir(db)
        src = self._dump_schema("src", db)
        dst = self._dump_schema("dst", db)
        (d / "schema-src.sql").write_text(src)
        (d / "schema-dst.sql").write_text(dst)
        diff = list(difflib.unified_diff(src.splitlines(), dst.splitlines(),
                                         "src", "dst", lineterm=""))
        changed = [l for l in diff if l[:1] in "+-"
                   and not l.startswith(("+++", "---"))]
        res = []
        if not changed:
            (d / "schema.diff").unlink(missing_ok=True)  # clear stale evidence
            res.append(Result("schema", db, "ok"))
        else:
            (d / "schema.diff").write_text("\n".join(diff))
            sample = "; ".join(sorted(set(l.strip() for l in changed))[:3])
            res.append(Result("schema", db, "diff",
                              f"{len(changed)} changed lines, e.g. {sample}",
                              str(d / "schema.diff"),
                              "apply missing DDL from schema-src.sql on target"))
        res.append(self.check_objects(db))
        if which("atlas") and self.hop.options.get("atlas", True):
            at = self._atlas(db)
            if at:
                res.append(at)
        return self._atlas_authoritative(res)

    def check_objects(self, db):
        queries = {
            "table": "select table_name from information_schema.tables"
                     " where table_schema=%s and table_type='BASE TABLE'",
            "view": "select table_name from information_schema.tables"
                    " where table_schema=%s and table_type='VIEW'",
            "function": "select routine_name from information_schema.routines"
                        " where routine_schema=%s and routine_type='FUNCTION'",
            "procedure": "select routine_name from information_schema.routines"
                         " where routine_schema=%s and routine_type='PROCEDURE'",
            "trigger": "select trigger_name from information_schema.triggers"
                       " where trigger_schema=%s",
            "event": "select event_name from information_schema.events"
                     " where event_schema=%s",
            "index": "select concat(table_name, '.', index_name)"
                     " from information_schema.statistics"
                     " where table_schema=%s group by table_name, index_name",
        }
        inv = {}
        for typ, sql in queries.items():
            a = {r[0] for r in self._q("src", sql, (db,))}
            b = {r[0] for r in self._q("dst", sql, (self._d("dst", db),))}
            inv[typ] = {"src": len(a), "dst": len(b),
                        "missing": sorted(a - b)[:50],
                        "extra": sorted(b - a)[:50]}
        out = self.hop.report_dir(db) / "objects.json"
        out.write_text(json.dumps(inv, indent=1))
        bad = {t: v for t, v in inv.items() if v["missing"] or v["extra"]}
        if bad:
            parts = []
            for t, v in bad.items():
                m = f"{t} {v['src']}/{v['dst']}"
                if v["missing"]:
                    m += " missing: " + ", ".join(v["missing"][:3])
                if v["extra"]:
                    m += " extra: " + ", ".join(v["extra"][:3])
                parts.append(m)
            return Result("schema", f"{db} objects", "diff", "; ".join(parts),
                          str(out),
                          "create missing objects on target from schema-src.sql")
        total = sum(v["src"] for v in inv.values())
        return Result("schema", f"{db} objects", "ok",
                      f"{total} objects in {len(inv)} types,"
                      " all present on target")

    def snapshot_state(self, db, state_dir, kind="all"):
        q = ("select table_name, auto_increment from information_schema.tables"
             " where table_schema=%s and auto_increment is not null")
        rows = self._q("dst", q, (self._d("dst", db),), fresh=True)
        (state_dir / "dst-autoinc.txt").write_text(
            "".join(f"{t}|{v}\n" for t, v in sorted(rows)))
        # sequence-only repair rolls back from the auto_increment snapshot alone
        if kind == "sequences":
            return
        (state_dir / "dst-schema.sql").write_text(self._dump_schema("dst", db))

    def _atlas(self, db):
        from urllib.parse import quote
        s, t = self.hop.source, self.hop.target
        su = (f"mysql://{s.user}:{quote(s.password, safe='')}"
              f"@{s.host}:{s.port}/{db}")
        tu = (f"mysql://{t.user}:{quote(t.password, safe='')}"
              f"@{t.host}:{t.port}/{self._d('dst', db)}")
        try:
            p = run(["atlas", "schema", "diff", "--from", tu, "--to", su,
                     "--exclude", "migkit_changelog"],
                    check=False, timeout=180)
        except Exception:
            return None
        if p.returncode != 0:
            return None
        text = p.stdout.strip()
        if not text or "Schemas are synced" in text:
            # both files, not just the fix: an undo left behind after the
            # schemas converged is a rollback for changes nobody made
            for stale in ("atlas-fix.sql", "atlas-fix.revert.sql"):
                (self.hop.report_dir(db) / stale).unlink(missing_ok=True)
            return Result("schema", f"{db} (atlas)", "ok", "atlas diff clean")
        out = self.hop.report_dir(db) / "atlas-fix.sql"
        out.write_text(text + "\n")
        detail = f"atlas generated {len(text.splitlines())} lines of fix DDL"
        # The same diff run the other way is the undo of exactly these
        # statements, and it has to be taken now: once the fix is applied the
        # two schemas no longer describe where the target came from.
        from .. import revert as _revert
        rev = self.hop.report_dir(db) / "atlas-fix.revert.sql"
        try:
            rp = run(["atlas", "schema", "diff", "--from", su, "--to", tu,
                      "--exclude", "migkit_changelog"],
                     check=False, timeout=180)
            rtext = rp.stdout.strip() if rp.returncode == 0 else ""
        except Exception:
            rtext = ""
        body = _revert.script(text, rtext, "atlas-fix.sql")
        if body:
            rev.write_text(body)
            detail += "; " + _revert.summary(text, rtext)
        else:
            # no undo is a fact worth stating, not a blank to fill in later
            rev.unlink(missing_ok=True)
            detail += "; no undo could be generated - take a backup first"
        return Result("schema", f"{db} (atlas)", "diff", detail,
                      str(out), "review then apply atlas-fix.sql on target;"
                      " atlas-fix.revert.sql undoes it")

    def _tables(self, side, db):
        rows = self._q(side, "select table_name from information_schema.tables"
                             " where table_schema=%s and table_type='BASE TABLE'"
                             " and table_name not like 'migkit%%'"
                             " order by 1", (self._d(side, db),))
        return [r[0] for r in rows if not self.hop.excluded(db, r[0])]

    def _pk_cols(self, db, t):
        rows = self._q("src",
                       "select column_name from information_schema.key_column_usage"
                       " where table_schema=%s and table_name=%s"
                       " and constraint_name='PRIMARY' order by ordinal_position",
                       (db, t))
        return [r[0] for r in rows]

    def _cols(self, db, t):
        rows = self._q("src", "select column_name from information_schema.columns"
                              " where table_schema=%s and table_name=%s"
                              " order by ordinal_position", (db, t))
        return [r[0] for r in rows]

    def _row_expr(self, db, t):
        """The row as one string, encoded so distinct rows cannot collide.

        This used to be `concat_ws('#', ...)` with `~null~` standing in for
        NULL, and both halves of that were wrong in the same way. Measured on
        MySQL 8: ('x#y','z') and ('x','y#z') both rendered `x#y#z` and hashed
        to CRC32 3898531935, and a NULL and the literal `~null~` did the same.
        A source holding the first of each against a target holding the second
        was reported `rows 2==2, checksum 810ced44==810ced44` - a verifier
        certifying a difference as equality. See `migkit.rowtext`.
        """
        from .. import rowtext
        return rowtext.mysql_row(self._cols(db, t))

    def check_counts(self, db):
        st, dt = set(self._tables("src", db)), set(self._tables("dst", db))
        res = []
        if st - dt:
            res.append(Result("counts", db, "diff",
                              f"missing tables on target: {sorted(st - dt)}"))
        if dt - st:
            res.append(Result("counts", db, "diff",
                              f"extra tables on target: {sorted(dt - st)}"))
        bad = []
        total_a = total_b = 0

        def cnt(side, t):
            return self._q(side,
                           f"select count(*) from `{self._d(side, db)}`.`{t}`")[0][0]

        common = sorted(st & dt)
        with ThreadPoolExecutor(max_workers=max(2, self.hop.workers)) as pool:
            futs = {t: (pool.submit(cnt, "src", t), pool.submit(cnt, "dst", t))
                    for t in common}
            for t in common:
                a, b = futs[t][0].result(), futs[t][1].result()
                total_a += a
                total_b += b
                if a != b:
                    bad.append(f"{t} src={a} dst={b}")
        if bad:
            res.append(Result("counts", db, "diff", "; ".join(bad)))
        return res or [Result("counts", db, "ok",
                              f"{len(common)} tables, rows"
                              f" {total_a:,}=={total_b:,}")]

    PARAM_CRITICAL = ("time_zone", "system_time_zone", "character_set_server",
                      "character_set_database", "collation_server",
                      "collation_database", "collation_connection", "sql_mode",
                      "lower_case_table_names", "default_storage_engine",
                      "transaction_isolation", "explicit_defaults_for_timestamp",
                      "character_set_connection", "character_set_client",
                      "character_set_results", "max_allowed_packet",
                      "group_concat_max_len", "version")

    def check_params(self, db):
        def pull(side):
            return {r[0]: str(r[1]) for r in
                    self._q(side, "show global variables")}
        return self._param_result(
            db, pull("src"), pull("dst"), self.PARAM_CRITICAL,
            "align the behavior-critical variables on the target parameter"
            " group before cutover")

    def check_autoinc(self, db):
        """usable = will AUTO_INCREMENT collide with an existing row on the
        next insert (the DMS trap - InnoDB 8 usually clamps it up, but a
        pre-8.0 restart, tablespace import, or a stray id=0 row can leave it
        behind); parity = does the counter match the source."""
        ddb = self._d("dst", db)
        q = ("select table_name, auto_increment from information_schema.tables"
             " where table_schema=%s and auto_increment is not null")
        src = dict(self._q("src", q, (db,), fresh=True))
        dst = dict(self._q("dst", q, (ddb,), fresh=True))
        acol = dict(self._q("dst",
                            "select table_name, column_name"
                            " from information_schema.columns"
                            " where table_schema=%s and extra like %s",
                            (ddb, "%auto_increment%"), fresh=True))
        collide = []
        for t, nextv in sorted(dst.items()):
            col = acol.get(t)
            if not col or nextv is None:
                continue
            mx = self._q("dst", f"select coalesce(max(`{col}`),0)"
                                f" from `{ddb}`.`{t}`", fresh=True)[0][0]
            if int(mx) > 0 and int(nextv) <= int(mx):
                collide.append(f"{t}: auto_increment={nextv}"
                               f" <= max({col})={mx}")
        res = []
        if collide:
            res.append(Result("autoinc", f"{db} usable", "diff",
                              "AUTO_INCREMENT will collide on next insert: "
                              + "; ".join(collide[:8]), "",
                              f"migkit sync {self.hop.name} --db {db}"
                              " --kind sequences"))
        else:
            res.append(Result("autoinc", f"{db} usable", "ok",
                              f"{len(acol)} auto_increment tables clear their"
                              " column max, no collision" if acol
                              else "no auto_increment tables"))
        parity = [f"{t} src={v} dst={dst.get(t)}"
                  for t, v in sorted(src.items()) if dst.get(t) != v]
        if parity:
            res.append(Result("autoinc", f"{db} parity", "diff",
                              "; ".join(parity[:8]), "",
                              f"migkit sync {self.hop.name} --db {db}"
                              " --kind sequences"))
        else:
            res.append(Result("autoinc", f"{db} parity", "ok",
                              f"{len(src)} counters match source"))
        return res

    def _health(self, side):
        """What this side says about its own load, for the throttle.

        `Threads_running` against `max_connections` is the cheapest honest
        load signal MySQL offers. Anything unreadable comes back as None
        rather than as "fine", so a server that hides a signal is not
        mistaken for an idle one.
        """
        from ..throttle import Health
        try:
            running = float(self._q(
                side, "show global status like 'Threads_running'")[0][1])
            limit = float(self._q(
                side, "show global variables like 'max_connections'")[0][1])
            busy = running / max(limit, 1.0)
        except Exception:
            return None
        # Replication lag needs the column names, which `_q` does not return,
        # and `performance_schema.replication_applier_status_by_worker` is the
        # modern source for it. Left as busy-ratio only rather than reaching
        # for a cursor description that this helper does not expose: a partial
        # signal is fine here, because lag is None on an unreplicated server
        # anyway and the latency signal covers what this misses.
        return Health(busy_ratio=busy)

    def check_data(self, db, table=None, stream=None, with_counts=False):
        st, dt = set(self._tables("src", db)), set(self._tables("dst", db))
        tables = [table] if table else sorted(st & dt)
        res = []
        rows_a = rows_b = 0
        bad_counts = []
        # Same reason as the PostgreSQL path: a checksum is only a SELECT, so
        # nothing else stops this loop from being the heaviest thing on a
        # server that is also serving an application.
        from ..throttle import Throttle
        gate = Throttle(self.hop.workers, probe=lambda: self._health("src"))

        def guarded(d, tbl):
            with gate.unit():
                return self._diff_table(d, tbl)

        with ThreadPoolExecutor(max_workers=self.hop.workers) as pool:
            futs = {pool.submit(guarded, db, t): t for t in tables}
            for fu in as_completed(futs):
                r, ra, rb = fu.result()
                if stream:
                    stream(f"{futs[fu]}: {r.status}")
                res.append(r)
                rows_a += ra
                rows_b += rb
                if ra != rb:
                    bad_counts.append(f"{futs[fu]} src={ra} dst={rb}")
        res = sorted(res, key=lambda r: r.scope)
        self._last_throttle = gate.summary()
        note = gate.line()
        if note and stream:
            stream(f"# {note}")
        if with_counts:
            bad_counts += [f"{t} missing on target" for t in sorted(st - dt)]
            bad_counts += [f"{t} extra on target" for t in sorted(dt - st)]
            if bad_counts:
                cres = Result("counts", db, "diff",
                              "; ".join(bad_counts[:10]))
            else:
                cres = Result("counts", db, "ok",
                              f"{len(tables)} tables, rows"
                              f" {rows_a:,}=={rows_b:,}"
                              " (from the checksum pass, no extra scan)")
            res.insert(0, cres)
        return res

    def _key_expr(self, db, t):
        """Expression hashing only the primary key, or None without one.

        Rides along in the same scan as the row hash: the row is already being
        read and hashing a key costs far less than hashing a whole row, which
        is what makes naming the shape of a difference effectively free.
        """
        pks = self._pk_cols(db, t)
        if not pks:
            return None
        from .. import rowtext
        # `\x02` joined and `\x01` stood in for NULL here, which is the same
        # ambiguity as the row hash had, just with rarer characters
        return rowtext.mysql_row(pks)

    def _checksum(self, side, db, t, expr, where="", key_expr=None):
        cols = ["count(*)", "coalesce(bit_xor(crc32(" + expr + ")), 0)",
                "coalesce(bit_xor(conv(substring(md5(" + expr + "), 1, 8),"
                " 16, 10)), 0)"]
        if key_expr:
            cols.append("coalesce(bit_xor(conv(substring(md5(" + key_expr
                        + "), 1, 8), 16, 10)), 0)")
        q = (f"select {', '.join(cols)}"
             f" from `{self._d(side, db)}`.`{t}` {where}")
        return tuple(self._q(side, q)[0])

    def _reladiff_url(self, side, db):
        from urllib.parse import quote
        ep = self.hop.source if side == "src" else self.hop.target
        return (f"mysql://{ep.user}:{quote(ep.password, safe='')}"
                f"@{ep.host}:{ep.port}/{self._d(side, db)}")

    def _reladiff_table(self, db, t, pks):
        cmd = ["reladiff", self._reladiff_url("src", db), t,
               self._reladiff_url("dst", db), t, "--stats",
               "-j", str(self.hop.workers), "-c", "%"]
        for k in pks:
            cmd += ["-k", k]
        try:
            p = run(cmd, check=False, timeout=3600)
        except Exception:
            return None, "", 0, 0
        text = p.stdout + p.stderr
        if p.returncode != 0 or "ERROR" in text:
            return None, "", 0, 0
        import re as _re
        m = _re.search(r"(\d+) rows in table A.*?(\d+) rows in table B",
                       text, _re.S)
        ra, rb = (int(m.group(1)), int(m.group(2))) if m else (0, 0)
        rows = f"rows {ra}=={rb}, " if m else ""
        if "0 rows are different" in text and "0 rows exclusive" in text:
            return "ok", f"{rows}hashdiff 0 differences (reladiff)", ra, rb
        return "diff", "", ra, rb

    def _diff_table(self, db, t):
        scope = f"{db}.{t}"
        expr = self._row_expr(db, t)
        pks = self._pk_cols(db, t)

        # builtin checksum is the fast default; options.reladiff:true swaps in
        if pks and which("reladiff") and self.hop.options.get("reladiff",
                                                              False):
            verdict, detail, ra, rb = self._reladiff_table(db, t, pks)
            if verdict == "ok":
                return Result("data", scope, "ok", detail), ra, rb
            if verdict == "diff":
                return self._drilldown(db, t, pks, expr, [""])

        # Ranges come from the shared planner rather than being built here, so
        # both engines get the same guarantee: the ranges cover the whole
        # keyspace, open at both ends. The version that used to live here
        # started at min(pk) on the source, which silently skipped any target
        # row with a smaller key - exactly the kind of difference worth
        # catching.
        from .. import checkpoint as _cp
        pk_ranges, col = [(None, None)], None
        if len(pks) == 1:
            n = self._q("src", f"select count(*) from `{db}`.`{t}`")[0][0]
            if n > self.hop.slice:
                mm = self._q("src", f"select min(`{pks[0]}`), max(`{pks[0]}`)"
                                    f" from `{db}`.`{t}`")[0]
                if mm[0] is not None and str(mm[0]).lstrip("-").isdigit():
                    col = pks[0]
                    cp_path = str(self.hop.report_dir(db) / "checkpoint.json")
                    cp = _cp.Checkpoint(cp_path)
                    chunk = cp.chunk_for(scope, max(1, self.hop.slice))
                    pk_ranges = _cp.plan_ranges(int(mm[0]), int(mm[1]), chunk)

        key_expr = self._key_expr(db, t)

        def clause(lo, hi):
            w = _cp.where(f"`{col}`", lo, hi) if col else ""
            return f"where {w}" if w else ""

        ranges = [clause(lo, hi) for lo, hi in pk_ranges]
        cp = (_cp.Checkpoint(str(self.hop.report_dir(db) / "checkpoint.json"))
              if col else _cp.Checkpoint(None))
        todo = cp.begin(scope, expr, pk_ranges) if col else pk_ranges
        done_before = cp.resumed(scope) if col else 0

        bad_ranges, kinds = [], set()
        rows_a = rows_b = xor_a = xor_b = 0
        with ThreadPoolExecutor(max_workers=4) as pool:
            futs = {}
            for lo, hi in todo:
                w = clause(lo, hi)
                fa = pool.submit(self._checksum, "src", db, t, expr, w,
                                 key_expr)
                fb = pool.submit(self._checksum, "dst", db, t, expr, w,
                                 key_expr)
                futs[(lo, hi)] = (fa, fb)
            for (lo, hi), (fa, fb) in futs.items():
                ra, rb = fa.result(), fb.result()
                if ra != rb:
                    bad_ranges.append(clause(lo, hi))
                    if key_expr and len(ra) > 3:
                        from ..verdict import difference_kind
                        k = difference_kind(ra[0], ra[3], rb[0], rb[3])
                        if k:
                            kinds.add(k)
                    if col:
                        cp.clear(scope)
                    continue
                if col:
                    cp.record(scope, lo, hi, ra[0], int(ra[2] or 0))
                else:
                    rows_a += ra[0]
                    rows_b += rb[0]
                    xor_a ^= int(ra[2] or 0)
                    xor_b ^= int(rb[2] or 0)
        if not bad_ranges:
            if col:
                rows_a, xor_s = cp.total(scope, combine="xor")
                rows_b, xor_a = rows_a, int(xor_s)
                xor_b = xor_a
                cp.clear(scope)
            resumed = (f", resumed {done_before}/{len(pk_ranges)}"
                       if done_before else "")
            return Result("data", scope, "ok",
                          f"rows {rows_a:,}=={rows_b:,}, checksum"
                          f" {xor_a:x}=={xor_b:x}"
                          f" ({len(pk_ranges)} chunks{resumed})"), \
                rows_a, rows_b
        if not pks:
            return Result("data", scope, "diff",
                          "checksum differs, no pk for row drilldown", "",
                          "recopy whole table with dump/load"), rows_a, rows_b
        r, ra, rb = self._drilldown(db, t, pks, expr, bad_ranges)
        if kinds and r.status != "ok":
            r.detail += " kind=" + ",".join(sorted(kinds))
        return r, ra, rb

    def _drilldown(self, db, t, pks, expr, ranges):
        scope = f"{db}.{t}"
        from .. import rowtext
        pkexpr = rowtext.mysql_row(pks)
        src, dst = {}, {}
        for w in ranges:
            src.update(dict(self._q("src",
                f"select {pkexpr}, md5({expr}) from `{db}`.`{t}` {w}")))
            dst.update(dict(self._q("dst",
                f"select {pkexpr}, md5({expr})"
                f" from `{self._d('dst', db)}`.`{t}` {w}")))
        missing = sorted(k for k in src if k not in dst)
        extra = sorted(k for k in dst if k not in src)
        changed = sorted(k for k in src if k in dst and src[k] != dst[k])
        d = self.hop.report_dir(db)
        for name, rows in (("missing", missing), ("extra", extra),
                           ("changed", changed)):
            p = d / f"data-{t}.{name}"
            if rows:
                p.write_text("\n".join(rows) + "\n")
            elif p.exists():
                p.unlink()
        if not (missing or extra or changed):
            # checksum differed but per-pk found none = in-flight CDC lag
            return Result("data", scope, "ok",
                          "checksum flicker settled (in-flight replication),"
                          " 0 rows actually differ"), len(src), len(dst)
        detail = (f"missing={len(missing)} extra={len(extra)}"
                  f" changed={len(changed)}")
        fp = self._column_fingerprint(db, t)
        if fp:
            detail += f"; drift localized to columns: {', '.join(fp[:6])}"
        return Result("data", scope, "diff", detail, str(d),
                      f"migkit sync {self.hop.name} --db {db} --kind rows"
                      " --apply"), len(src), len(dst)

    def _column_fingerprint(self, db, t):
        """One scan per side, one aggregate per column: which columns
        actually differ before any row-level work."""
        cols = self._cols(db, t)
        if not cols:
            return []
        # One value per hash, so there is no separator to be confused by -
        # but a NULL and the literal that stood in for it still collided, and
        # the encoding is meant to be the same everywhere
        from .. import rowtext
        expr = ", ".join(
            f"coalesce(bit_xor(conv(substring(md5("
            f"{rowtext.mysql_row([c])}), 1, 8), 16, 10)), 0)"
            for c in cols)
        try:
            a = self._q("src", f"select {expr} from `{db}`.`{t}`")[0]
            b = self._q("dst",
                        f"select {expr} from `{self._d('dst', db)}`.`{t}`")[0]
        except Exception:
            return []
        diff = [c for c, x, y in zip(cols, a, b) if x != y]
        out = self.hop.report_dir(db) / f"data-{t}.columns"
        if diff:
            out.write_text("columns differing between src and dst:\n"
                           + "\n".join(diff) + "\n")
        elif out.exists():
            out.unlink()
        return diff

    def _compare_pks(self, db, t, keys):
        pks = self._pk_cols(db, t)
        if not pks:
            return None
        expr = self._row_expr(db, t)
        from .. import rowtext
        pkexpr = rowtext.mysql_row(pks)
        def fetch(side):
            out = {}
            klist = sorted(keys)
            for i in range(0, len(klist), 500):
                chunk = klist[i:i + 500]
                if len(pks) == 1:
                    where = (f"cast(`{pks[0]}` as char) in ("
                             + ", ".join(["%s"] * len(chunk)) + ")")
                    args = chunk
                else:
                    tup = ("(" + ", ".join(f"cast(`{c}` as char)"
                                           for c in pks) + ")")
                    one = "(" + ", ".join(["%s"] * len(pks)) + ")"
                    where = tup + " in (" + ", ".join([one] * len(chunk)) + ")"
                    # decoded by the same module that encoded it; splitting
                    # on a tab here is what a tab inside a key value broke
                    args = [x for k in chunk for x in rowtext.parse(k)]
                q = (f"select {pkexpr}, md5({expr})"
                     f" from `{self._d(side, db)}`.`{t}` where {where}")
                out.update({r[0]: r[1] for r in self._q(side, q, args)})
            return out
        src, dst = fetch("src"), fetch("dst")
        missing = sorted(k for k in keys if k in src and k not in dst)
        extra = sorted(k for k in keys if k in dst and k not in src)
        changed = sorted(k for k in keys
                         if k in src and k in dst and src[k] != dst[k])
        return missing, extra, changed

    def _write_pk_files(self, db, t, missing, extra, changed):
        d = self.hop.report_dir(db)
        for kind, rows in (("missing", missing), ("extra", extra),
                           ("changed", changed)):
            f = d / f"data-{t}.{kind}"
            if rows:
                f.write_text("\n".join(rows) + "\n")
            elif f.exists():
                f.unlink()

    # delta verify: re-check only pks touched in the binlog since the saved
    # position, which advances only on a clean verify (idempotent)
    def delta_verify(self, db, limit=20000, log=None):
        try:
            from pymysqlreplication import BinLogStreamReader
            from pymysqlreplication.row_event import (DeleteRowsEvent,
                                                      UpdateRowsEvent,
                                                      WriteRowsEvent)
        except ImportError:
            return [Result("delta", db, "error",
                           "pip install mysql-replication for delta verify")]
        state = self.hop.report_dir(db) / "delta-pos.json"
        if not state.exists():
            pos = None
            for q in ("show binary log status",   # mysql 8.4+
                      "show master status"):      # mysql 8.0 / aurora / txsql
                try:
                    pos = self._q("src", q)
                except Exception:
                    pos = None
                if pos:
                    break
            if not pos:
                return [Result("delta", db, "error",
                               "cannot read binlog position on source")]
            state.write_text(json.dumps({"log_file": pos[0][0],
                                         "log_pos": int(pos[0][1])}))
            return [Result("delta", db, "ok",
                           f"baseline {pos[0][0]}:{pos[0][1]} recorded,"
                           " changes are tracked from this point on")]
        ck = json.loads(state.read_text())
        s = self.hop.source
        stream = BinLogStreamReader(
            connection_settings={"host": s.host, "port": s.port,
                                 "user": s.user, "passwd": s.password},
            server_id=int(self.hop.options.get("server_id", 4379)) + 1,
            resume_stream=True, blocking=False,
            log_file=ck.get("log_file"), log_pos=ck.get("log_pos"),
            only_schemas=[db],
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent])
        touched = {}
        nopk = set()
        n = 0
        try:
            for ev in stream:
                t = ev.table
                pks = self._pk_cols(db, t)
                if not pks:
                    nopk.add(t)
                    continue
                real = self._cols(db, t)

                def fix(vals):
                    if not any(k.startswith("UNKNOWN_COL") for k in vals):
                        return vals
                    return {real[int(k[11:])]: v for k, v in vals.items()}

                for row in ev.rows:
                    for vals in filter(None, (row.get("values"),
                                              row.get("after_values"),
                                              row.get("before_values"))):
                        v = fix(vals)
                        if all(p in v for p in pks):
                            touched.setdefault(t, set()).add(
                                "\t".join(str(v[p]) for p in pks))
                    n += 1
                if n >= limit:
                    break
            end = {"log_file": stream.log_file, "log_pos": stream.log_pos}
        finally:
            stream.close()
        if not touched and not nopk:
            state.write_text(json.dumps(end))
            return [Result("delta", db, "ok",
                           "0 changes since last verified position")]
        res = []
        clean = True
        for t, keys in sorted(touched.items()):
            cmp = self._compare_pks(db, t, keys)
            if cmp is None:
                res.append(Result("delta", f"{db}.{t}", "error",
                                  "pk lookup failed"))
                clean = False
                continue
            missing, extra, changed = cmp
            if missing or extra or changed:
                clean = False
                self._write_pk_files(db, t, missing, extra, changed)
                res.append(Result(
                    "delta", f"{db}.{t}", "diff",
                    f"of {len(keys)} touched rows: missing={len(missing)}"
                    f" extra={len(extra)} changed={len(changed)}",
                    str(self.hop.report_dir(db)),
                    f"migkit sync {self.hop.name} --db {db} --kind rows"))
            else:
                res.append(Result("delta", f"{db}.{t}", "ok",
                                  f"{len(keys)} touched rows verified"
                                  " equal on both sides"))
            if log:
                log(f"{t}: {len(keys)} touched, "
                    + ("clean" if not (missing or extra or changed)
                       else "DIFF"))
        for t in sorted(nopk):
            res.append(Result("delta", f"{db}.{t}", "skip",
                              "no pk, cannot delta-verify"))
        if clean:
            state.write_text(json.dumps(end))
            note = "position advanced"
        else:
            note = "position NOT advanced, window replays next cycle"
        if n >= limit:
            note += f"; window truncated at {limit} events, more pending"
        res.insert(0, Result("delta", db, "ok" if clean else "diff",
                             f"{sum(len(v) for v in touched.values())}"
                             f" changed rows across {len(touched)} tables,"
                             f" {note}"))
        return res

    def check_deep(self, db):
        res = []
        ddb = self._d("dst", db)

        # no pk/unique = CDC drops its updates/deletes and it can't be verified
        # or repaired by key (the same trap postgres has, without the InnoDB
        # guardrails). Check the source tables that are about to be migrated.
        nopk = [r[0] for r in self._q("src",
                "select t.table_name from information_schema.tables t"
                " where t.table_schema=%s and t.table_type='BASE TABLE'"
                " and t.table_name not like 'migkit%%'"
                " and not exists (select 1 from"
                " information_schema.table_constraints tc"
                " where tc.table_schema=t.table_schema"
                " and tc.table_name=t.table_name"
                " and tc.constraint_type in ('PRIMARY KEY','UNIQUE'))", (db,))]
        if nopk:
            res.append(Result("deep", f"{db} keys", "diff",
                              f"{len(nopk)} tables have no pk/unique (CDC drops"
                              " their updates/deletes, unverifiable by key): "
                              + ", ".join(nopk[:5]), "",
                              "add a primary key or unique index before"
                              " migrating"))
        else:
            res.append(Result("deep", f"{db} keys", "ok",
                              "every table has a pk or unique index"))

        # loads run with foreign_key_checks=0, so target orphans are possible
        rows = self._q("dst",
                       "select constraint_name, table_name, column_name,"
                       " referenced_table_name, referenced_column_name"
                       " from information_schema.key_column_usage"
                       " where table_schema=%s"
                       " and referenced_table_name is not null"
                       " order by constraint_name, ordinal_position", (ddb,))
        fks = {}
        for con, t, c, rt, rc in rows:
            fk = fks.setdefault((con, t, rt), ([], []))
            fk[0].append(c)
            fk[1].append(rc)
        orphans = []
        for (con, t, rt), (cols, rcols) in sorted(fks.items()):
            nn = " and ".join(f"c.`{c}` is not null" for c in cols)
            join = " and ".join(f"p.`{r}` = c.`{c}`"
                                for c, r in zip(cols, rcols))
            n = self._q("dst", f"select count(*) from `{ddb}`.`{t}` c"
                               f" where {nn} and not exists"
                               f" (select 1 from `{ddb}`.`{rt}` p"
                               f" where {join})")[0][0]
            if n:
                orphans.append(f"{t}.{con}: {n} orphan rows")
        res.append(Result("deep", f"{db} fk", "diff" if orphans else "ok",
                          "; ".join(orphans[:5]) if orphans
                          else f"{len(fks)} fks scanned, 0 orphan rows", "",
                          "reload the child rows or delete orphans"
                          if orphans else ""))

        colq = ("select concat(table_name, '.', column_name), column_type,"
                " is_nullable, coalesce(column_default, ''),"
                " coalesce(character_set_name, ''),"
                " coalesce(collation_name, ''), extra"
                " from information_schema.columns where table_schema=%s"
                " and table_name not like 'migkit%%' order by 1")
        sc = {r[0]: r[1:] for r in self._q("src", colq, (db,))}
        dc = {r[0]: r[1:] for r in self._q("dst", colq, (ddb,))}
        drift = [f"{k}: src={sc[k]} dst={dc[k]}" for k in sorted(sc)
                 if k in dc and sc[k] != dc[k]]
        if drift:
            out = self.hop.report_dir(db) / "deep-columns.diff"
            out.write_text("\n".join(drift) + "\n")
            res.append(Result("deep", f"{db} columns", "diff",
                              f"{len(drift)} columns drift (type/null/"
                              f"default/charset): "
                              + "; ".join(d.split(":")[0] for d in drift[:4]),
                              str(out), "align target DDL, charset drift"
                                        " corrupts comparisons and apps"))
        else:
            res.append(Result("deep", f"{db} columns", "ok",
                              f"{len(sc)} columns compared, type/null/"
                              "default/charset/collation identical"))

        # narrowing = silent truncation/overflow: a target column shorter than
        # the source (fewer chars, less decimal scale/precision, smaller int,
        # or unsigned turned signed) quietly cuts or wraps values.
        CHAR_T = ("char", "varchar", "tinytext", "text", "mediumtext",
                  "longtext")
        INTW = {"tinyint": 1, "smallint": 2, "mediumint": 3, "int": 4,
                "bigint": 8}
        nq = ("select concat(table_name,'.',column_name), data_type,"
              " coalesce(character_maximum_length,0),"
              " coalesce(numeric_precision,0), coalesce(numeric_scale,0),"
              " column_type from information_schema.columns"
              " where table_schema=%s and table_name not like 'migkit%%'")
        scn = {r[0]: r[1:] for r in self._q("src", nq, (db,))}
        dcn = {r[0]: r[1:] for r in self._q("dst", nq, (ddb,))}
        narrow = []
        for k in sorted(scn):
            if k not in dcn:
                continue
            st, scm, spr, ssc, sct = scn[k]
            dt, dcm, dpr, dsc, dct = dcn[k]
            why = None
            if st in CHAR_T and scm and dcm and int(dcm) < int(scm):
                why = f"char {scm} -> {dcm}"
            elif st == "decimal" and ssc and int(dsc) < int(ssc):
                why = f"decimal scale {ssc} -> {dsc} (rounds)"
            elif st == "decimal" and spr and int(dpr) < int(spr):
                why = f"decimal precision {spr} -> {dpr} (overflow)"
            elif INTW.get(st, 0) > INTW.get(dt, 99):
                why = f"{st} -> {dt} (overflow)"
            elif "unsigned" in sct and "unsigned" not in dct \
                    and st in INTW:
                why = f"unsigned -> signed ({sct} -> {dct})"
            if why:
                narrow.append(f"{k}: {why}")
        if narrow:
            res.append(Result("deep", f"{db} narrowing", "diff",
                              f"{len(narrow)} target columns NARROWER than"
                              " source (silent truncation/overflow risk): "
                              + "; ".join(narrow[:6]), "",
                              "widen the target column to match source before"
                              " loading, or values are cut/rounded/overflowed"))
        else:
            res.append(Result("deep", f"{db} narrowing", "ok",
                              "no target column narrower than source"))

        # a constant per-row offset on a datetime/timestamp column is a tz
        # conversion bug (mover applied a non-UTC session), not row corruption.
        # Both sides read with time_zone pinned to +00:00, so a faithful copy
        # compares as delta 0 and a systematic shift shows the same delta.
        tcols = self._q("src",
                        "select table_name, column_name"
                        " from information_schema.columns where table_schema=%s"
                        " and data_type in ('datetime','timestamp')"
                        " and table_name not like 'migkit%%'", (db,))
        shifts = []
        for t, col in tcols[:10]:
            pk = self._pk_cols(db, t)
            if len(pk) != 1:
                continue
            p = pk[0]

            def rows(side, dbn):
                q = (f"select `{p}`, unix_timestamp(`{col}`)"
                     f" from `{dbn}`.`{t}` where `{col}` is not null"
                     f" order by `{p}` limit 200")
                return {str(r[0]): r[1] for r in self._rows_utc(side, q)
                        if r[1] is not None}
            sm, dm = rows("src", db), rows("dst", ddb)
            deltas = [float(dm[k]) - float(sm[k]) for k in sm if k in dm]
            if len(deltas) < 3:
                continue
            avg = sum(deltas) / len(deltas)
            if max(deltas) - min(deltas) < 1 and abs(avg) >= 1:
                secs = round(avg)
                shifts.append(f"{t}.{col}: every row shifted"
                              f" {secs}s (~{secs / 3600:.1f}h)")
        if shifts:
            res.append(Result("deep", f"{db} timeshift", "diff",
                              "uniform timezone offset (systematic, not"
                              " row-level corruption): " + "; ".join(shifts[:5]),
                              "", "target stored a non-UTC wall clock; re-load"
                              " with the source session timezone"))
        else:
            res.append(Result("deep", f"{db} timeshift", "ok",
                              "no uniform timestamp offset detected"))

        # charset corruption: a text column downgraded from utf8mb4 to a
        # narrower charset (utf8mb3/latin1) on the target truncates or drops
        # 4-byte characters (emoji, astral CJK), and a lossy transcode leaves
        # U+FFFD replacement characters - both silent.
        csq = ("select table_name, column_name, character_set_name"
               " from information_schema.columns where table_schema=%s"
               " and character_set_name is not null"
               " and table_name not like 'migkit%%'")
        scs = {(r[0], r[1]): r[2] for r in self._q("src", csq, (db,))}
        dcs = {(r[0], r[1]): r[2] for r in self._q("dst", csq, (ddb,))}
        WIDTH = {"utf8mb4": 4, "utf8mb3": 3, "utf8": 3, "latin1": 1,
                 "latin2": 1, "ascii": 1}
        csbad = []
        for k, s2 in sorted(scs.items()):
            d2 = dcs.get(k)
            if d2 and d2 != s2:
                note = f"{k[0]}.{k[1]}: charset {s2} -> {d2}"
                if WIDTH.get(d2, 9) < WIDTH.get(s2, 0):
                    note += " (DOWNGRADE, drops/truncates multibyte chars)"
                csbad.append(note)
        tcols = self._q("src",
                        "select distinct table_name, column_name"
                        " from information_schema.columns where table_schema=%s"
                        " and data_type in ('char','varchar','text','tinytext',"
                        "'mediumtext','longtext') and table_name not like"
                        " 'migkit%%'", (db,))
        for t, col in tcols[:30]:
            q = ("select coalesce(sum(char_length(convert(`{c}` using utf8mb4))"
                 " - char_length(replace(convert(`{c}` using utf8mb4),"
                 " _utf8mb4 0xEFBFBD, ''))),0) from `{d}`.`{t}`")
            try:
                sv = int(self._q("src", q.format(c=col, d=db, t=t))[0][0])
                dv = int(self._q("dst", q.format(c=col, d=ddb, t=t))[0][0])
            except Exception:
                continue
            if dv > sv:
                csbad.append(f"{t}.{col}: {dv - sv} extra U+FFFD replacement"
                             " chars on target")
        if csbad:
            res.append(Result("deep", f"{db} charset", "diff",
                              "; ".join(csbad[:6]), "",
                              "keep the target column utf8mb4 and re-load the"
                              " affected rows over a utf8mb4 connection"))
        else:
            res.append(Result("deep", f"{db} charset", "ok",
                              "text column charsets match, no replacement-char"
                              " excess"))

        # partitioned tables (mysql): a missing partition sends rows to the
        # MAXVALUE catch-all, a changed method/expression reroutes them.
        def _parts(side, dbn):
            out = {}
            for t, meth, expr, desc in self._q(side,
                    "select table_name, partition_method,"
                    " coalesce(partition_expression,''), partition_description"
                    " from information_schema.partitions where table_schema=%s"
                    " and partition_name is not null", (dbn,)):
                e = out.setdefault(t, {"m": meth, "e": expr, "b": set()})
                e["b"].add(desc)
            return out
        sp, dp = _parts("src", db), _parts("dst", ddb)
        pbad = []
        for t, si in sorted(sp.items()):
            di = dp.get(t)
            if not di:
                pbad.append(f"{t}: partitioned on source, not on target")
                continue
            if (si["m"], si["e"]) != (di["m"], di["e"]):
                pbad.append(f"{t}: partition scheme differs"
                            f" src({si['m']} {si['e']}) dst({di['m']} {di['e']})")
                continue
            miss = si["b"] - di["b"]
            if miss:
                pbad.append(f"{t}: {len(miss)} partition bound(s) missing on"
                            f" target: {', '.join(sorted(miss)[:2])}")
            if "MAXVALUE" in di["b"]:
                mv = self._q("dst", "select partition_name from"
                             " information_schema.partitions where"
                             " table_schema=%s and table_name=%s and"
                             " partition_description='MAXVALUE'", (ddb, t))
                if mv:
                    n = self._q("dst", f"select count(*) from `{ddb}`.`{t}`"
                                f" partition (`{mv[0][0]}`)")[0][0]
                    if n > 0:
                        pbad.append(f"{t}: {n} rows stranded in MAXVALUE"
                                    f" partition ({mv[0][0]})")
        if not sp:
            res.append(Result("deep", f"{db} partitions", "ok",
                              "no partitioned tables"))
        elif pbad:
            res.append(Result("deep", f"{db} partitions", "diff",
                              "; ".join(pbad[:6]), "",
                              "recreate the missing partitions and move rows"
                              " out of MAXVALUE before cutover"))
        else:
            res.append(Result("deep", f"{db} partitions", "ok",
                              f"{len(sp)} partitioned tables, schemes and"
                              " bounds match"))

        # generated/computed columns (mysql)
        genq = ("select table_name, column_name, extra,"
                " coalesce(generation_expression,'')"
                " from information_schema.columns where table_schema=%s"
                " and generation_expression <> '' and generation_expression"
                " is not null and table_name not like 'migkit%%'")
        sg = {(r[0], r[1]): (r[2], r[3]) for r in self._q("src", genq, (db,))}
        dg = {(r[0], r[1]): (r[2], r[3]) for r in self._q("dst", genq, (ddb,))}

        def _n(e):
            return re.sub(r"\s+", "", e).lower()
        gbad = []
        for k, (extra, expr) in sorted(sg.items()):
            t, col = k
            if k not in dg:
                gbad.append(f"{t}.{col}: generated on source,"
                            " plain/missing on target")
                continue
            if _n(expr) != _n(dg[k][1]):
                gbad.append(f"{t}.{col}: generation expression differs")
                continue
            if "STORED" in (extra or "").upper():
                try:
                    n = self._q("dst", f"select count(*) from `{ddb}`.`{t}`"
                                f" where not (`{col}` <=> ({dg[k][1]}))")[0][0]
                    if n > 0:
                        gbad.append(f"{t}.{col}: {n} rows where the stored"
                                    " value != its expression")
                except Exception:
                    pass
        for k in sorted(dg):
            if k not in sg:
                gbad.append(f"{k[0]}.{k[1]}: generated on target but plain"
                            " on source")
        res.append(Result("deep", f"{db} generated",
                          "diff" if gbad else "ok",
                          "; ".join(gbad[:6]) if gbad
                          else (f"{len(sg)} generated columns match"
                                if sg else "no generated columns"), "",
                          "align the generation expression/storage and"
                          " re-derive the column" if gbad else ""))

        # collation unique-collapse (mysql): a unique/pk text column on a
        # case/accent-insensitive target collation collapses distinct source
        # rows into duplicates on load.
        uqq = ("select k.table_name, k.column_name, c.collation_name"
               " from information_schema.table_constraints tc"
               " join information_schema.key_column_usage k"
               "  on k.table_schema=tc.table_schema"
               "  and k.table_name=tc.table_name"
               "  and k.constraint_name=tc.constraint_name"
               " join information_schema.columns c"
               "  on c.table_schema=k.table_schema"
               "  and c.table_name=k.table_name"
               "  and c.column_name=k.column_name"
               " where tc.table_schema=%s and tc.constraint_type"
               "  in ('PRIMARY KEY','UNIQUE')"
               "  and c.data_type in ('char','varchar','text')"
               "  and c.collation_name is not null")
        su = {(r[0], r[1]): r[2] for r in self._q("src", uqq, (db,))}
        dcu = {(r[0], r[1]): r[2] for r in self._q("dst", uqq, (ddb,))}
        cbad = []
        for k, scoll in sorted(su.items()):
            dcoll = dcu.get(k)
            if not dcoll or dcoll == scoll:
                continue
            t, col = k
            detail = f"{t}.{col}: unique-key collation src({scoll}) != dst({dcoll})"
            try:
                n = self._q("src", f"select count(*) from (select 1 from"
                            f" `{db}`.`{t}` group by `{col}` collate {dcoll}"
                            f" having count(*) > 1) x")[0][0]
                if n > 0:
                    detail += (f"; {n} source groups COLLAPSE to duplicates"
                               " under the target collation (data loss)")
            except Exception:
                detail += "; collapse untestable (collation not on source)"
            cbad.append(detail)
        res.append(Result("deep", f"{db} collation",
                          "diff" if cbad else "ok",
                          "; ".join(cbad[:6]) if cbad
                          else "unique-key collations match", "",
                          "match the unique-key collation to source (or dedup"
                          " first)" if cbad else ""))

        pkq = ("select k.table_name, k.column_name"
               " from information_schema.key_column_usage k"
               " join information_schema.columns c"
               " on c.table_schema = k.table_schema"
               " and c.table_name = k.table_name"
               " and c.column_name = k.column_name"
               " where k.table_schema=%s and k.constraint_name='PRIMARY'"
               " and c.data_type in ('tinyint','smallint','mediumint',"
               "'int','bigint')"
               " and 1 = (select count(*)"
               " from information_schema.key_column_usage k2"
               " where k2.table_schema = k.table_schema"
               " and k2.table_name = k.table_name"
               " and k2.constraint_name='PRIMARY')"
               " order by 1")
        spk = self._q("src", pkq, (db,))
        dpk = {r[0] for r in self._q("dst", pkq, (ddb,))}
        both = [(t, c) for t, c in spk if t in dpk]

        def maxes(side):
            out = {}
            sdb = self._d(side, db)
            for i in range(0, len(both), 200):
                q = " union all ".join(
                    f"select '{t}', coalesce(max(`{c}`), 0)"
                    f" from `{sdb}`.`{t}`" for t, c in both[i:i + 200])
                out.update({r[0]: int(r[1]) for r in self._q(side, q)})
            return out

        if both:
            am, bm = maxes("src"), maxes("dst")
            ahead = [f"{k} src_max={am[k]} dst_max={bm[k]}"
                     for k in sorted(am) if bm.get(k, 0) > am[k]]
            behind = [k for k in sorted(am) if bm.get(k, 0) < am[k]]
            if ahead:
                res.append(Result("deep", f"{db} boundary", "diff",
                                  f"target max(pk) AHEAD of source on"
                                  f" {len(ahead)}: {'; '.join(ahead[:4])}",
                                  "", "writes landing on target or"
                                      " double-apply, find the writer"
                                      " before cutover"))
            else:
                note = (f"; {len(behind)} behind (replication lag)"
                        if behind else "")
                res.append(Result("deep", f"{db} boundary", "ok",
                                  f"max(pk) checked on {len(both)} tables,"
                                  f" none ahead of source{note}"))
        else:
            res.append(Result("deep", f"{db} boundary", "ok",
                              "no single-int-pk tables to boundary-check"))

        res += self._deep_nullempty(db, ddb)
        res += self._deep_float(db, ddb)
        res += self._deep_unenforced_checks(db, ddb)
        res += self._check_grants(db)
        res += self._deep_ownership(db, ddb)
        return res

    OWNED = (("views", "table_schema", "table_name"),
             ("routines", "routine_schema", "routine_name"),
             ("triggers", "trigger_schema", "trigger_name"),
             ("events", "event_schema", "event_name"))

    def _deep_ownership(self, db, ddb):
        """DEFINER and SQL SECURITY drift, which the schema diff cannot see.

        `_canon_ddl` strips both before comparing, deliberately: the mover
        rewrites them on every object, and leaving them in would bury every
        real schema finding under cosmetic noise. So they are reported here
        instead, where one line can describe a change made to everything.

        Measured before this existed: a view that was `app@% / DEFINER` on the
        source and `dts_migration@% / INVOKER` on the target passed the schema
        check, the object check and atlas, all three reporting ok.
        """
        from .. import ownership as _own
        rows = []
        for table, sch_col, name_col in self.OWNED:
            has_sec = table in ("views", "routines")
            sec = ", security_type" if has_sec else ""
            q = (f"select {name_col}, definer{sec} from information_schema."
                 f"{table} where {sch_col} = %s")
            try:
                a = {r[0]: "/".join(str(x) for x in r[1:])
                     for r in self._q("src", q, (db,))}
                b = {r[0]: "/".join(str(x) for x in r[1:])
                     for r in self._q("dst", q, (ddb,))}
            except Exception as e:
                return [Result("deep", f"{db} ownership", "warn",
                               f"cannot read {table}: {str(e)[:90]} -"
                               " unknown, not clean")]
            # objects missing on one side are the object check's finding, not
            # this one; saying it twice would let the two drift apart
            rows += [(f"{table[:-1]} {n}", a[n], b[n])
                     for n in sorted(set(a) & set(b))]
        if not rows:
            return [Result("deep", f"{db} ownership", "ok",
                           "no views, routines, triggers or events")]
        changes = _own.group(rows)
        if not changes:
            return [Result("deep", f"{db} ownership", "ok",
                           f"{len(rows)} objects keep their definer"
                           " and security mode")]
        note = ("one substitution across every object - a decision or"
                " something the mover did to all of them"
                if _own.systematic(changes) else
                "objects drifted individually - read each one")
        invoker = sum(len(v) for (a, b), v in changes.items()
                      if "DEFINER" in a and "INVOKER" in b)
        detail = f"{_own.total(changes)} of {len(rows)}: {_own.describe(changes)}"
        if invoker:
            # the privilege change, not just the name change
            detail += (f"; {invoker} dropped from SQL SECURITY DEFINER to"
                       " INVOKER - they now run with the caller's privileges")
        return [Result("deep", f"{db} ownership", "diff", detail, "",
                       f"{note}; recreate with the intended DEFINER before"
                       " the application depends on it")]

    # ---- checks PostgreSQL already had and MySQL did not ----

    TEXTY = ("char", "varchar", "text", "tinytext", "mediumtext", "longtext")
    FLOATY = ("float", "double")

    def _text_columns(self, db, limit=30):
        q = ("select table_name, column_name from information_schema.columns"
             " where table_schema=%s and data_type in ("
             + ",".join(["%s"] * len(self.TEXTY)) + ")"
             " and table_name not like 'migkit%%' order by 1,2")
        return self._q("src", q, (db,) + self.TEXTY)[:limit]

    def _deep_nullempty(self, db, ddb):
        """NULL and '' are different values that a mover happily swaps.

        Both sides still hold "something" in the column, so a row checksum
        can be equal on one engine's reading and not another's - and the
        application's `IS NULL` and `= ''` branches diverge. Comparing the
        two counts per column is what catches it.
        """
        bad = []
        for tbl, col in self._text_columns(db):
            q = (f"select sum(`{col}` is null), sum(`{col}` = '')"
                 f" from `%s`.`{tbl}`")
            try:
                sv = self._q("src", q % db)[0]
                dv = self._q("dst", q % ddb)[0]
            except Exception:
                continue
            s = (int(sv[0] or 0), int(sv[1] or 0))
            d = (int(dv[0] or 0), int(dv[1] or 0))
            if s != d:
                bad.append(f"{tbl}.{col}: src null/empty={s[0]}|{s[1]}"
                           f" dst={d[0]}|{d[1]}")
        if bad:
            return [Result("deep", f"{db} nullempty", "diff",
                           f"{len(bad)} text columns differ in NULL vs"
                           " empty-string split (semantic flip): "
                           + "; ".join(bad[:5]), "",
                           "normalize with NULLIF(col,'') / COALESCE per"
                           " column intent; decide which side is canonical")]
        return [Result("deep", f"{db} nullempty", "ok",
                       "NULL vs empty-string consistent on text columns")]

    def _deep_float(self, db, ddb):
        """FLOAT and DOUBLE are approximations, so a checksum over them is
        the wrong instrument: identical values can hash differently and real
        drift can hash the same. Compare the aggregate with a tolerance
        instead, which is what the value actually supports."""
        q = ("select table_name, column_name from information_schema.columns"
             " where table_schema=%s and data_type in (%s, %s)"
             " and table_name not like 'migkit%%' order by 1,2")
        cols = self._q("src", q, (db,) + self.FLOATY)[:30]
        bad = []
        for tbl, col in cols:
            agg = (f"select count(`{col}`), coalesce(sum(`{col}`),0),"
                   f" coalesce(max(abs(`{col}`)),0) from `%s`.`{tbl}`")
            try:
                sn, ss, smax = self._q("src", agg % db)[0]
                dn, ds, dmax = self._q("dst", agg % ddb)[0]
            except Exception:
                continue
            if int(sn or 0) != int(dn or 0):
                bad.append(f"{tbl}.{col}: non-null count src={sn} dst={dn}")
                continue
            ss, ds = float(ss or 0), float(ds or 0)
            # relative tolerance against the magnitude actually present, so
            # the test is meaningful for both tiny and huge columns
            scale = max(abs(ss), abs(ds), float(smax or 0), float(dmax or 0), 1.0)
            if abs(ss - ds) > scale * 1e-9:
                bad.append(f"{tbl}.{col}: sum src={ss!r} dst={ds!r}")
        if bad:
            return [Result("deep", f"{db} float", "diff",
                           f"{len(bad)} float/double columns drift beyond"
                           " tolerance: " + "; ".join(bad[:5]), "",
                           "a mover that changed the column's precision will"
                           " do this; compare the column definitions first")]
        return [Result("deep", f"{db} float", "ok",
                       f"{len(cols)} float/double columns within tolerance"
                       if cols else "no float/double columns")]

    def _deep_unenforced_checks(self, db, ddb):
        """MySQL's version of PostgreSQL's NOT VALID.

        A CHECK constraint declared `NOT ENFORCED` is in the catalog and in
        every schema diff, and enforces nothing. Counting constraints finds
        both sides equal; only reading `ENFORCED` shows that the target is
        accepting rows the source would reject.
        """
        q = ("select tc.table_name, tc.constraint_name, cc.check_clause,"
             " tc.enforced from information_schema.table_constraints tc"
             " join information_schema.check_constraints cc"
             "   on cc.constraint_schema = tc.constraint_schema"
             "  and cc.constraint_name = tc.constraint_name"
             " where tc.constraint_schema=%s and tc.constraint_type='CHECK'"
             " and tc.table_name not like 'migkit%%'")
        try:
            src = {(r[0], r[1]): (r[2], r[3]) for r in self._q("src", q, (db,))}
            dst = {(r[0], r[1]): (r[2], r[3]) for r in self._q("dst", q, (ddb,))}
        except Exception as e:
            # MySQL 5.7 parses CHECK and discards it; there is no catalog to
            # read, and saying so is more use than a silent pass
            return [Result("deep", f"{db} checks", "skip",
                           f"CHECK constraint catalog unavailable:"
                           f" {str(e).splitlines()[0][:70]}")]
        bad = []
        for k, (clause, enforced) in sorted(src.items()):
            if k not in dst:
                bad.append(f"{k[0]}.{k[1]} missing on target")
                continue
            dclause, denforced = dst[k]
            if str(denforced).upper() == "NO" and str(enforced).upper() == "YES":
                bad.append(f"{k[0]}.{k[1]} NOT ENFORCED on target"
                           " (enforced on source)")
            elif re.sub(r"\s+", "", str(clause or "")) != \
                    re.sub(r"\s+", "", str(dclause or "")):
                bad.append(f"{k[0]}.{k[1]} clause differs")
        unenforced_both = [k for k, (_, e) in src.items()
                           if str(e).upper() == "NO" and k in dst]
        if bad:
            return [Result("deep", f"{db} checks", "diff",
                           f"{len(bad)} CHECK constraints differ: "
                           + "; ".join(bad[:5]), "",
                           "ALTER TABLE ... ALTER CHECK <name> ENFORCED after"
                           " confirming the existing rows satisfy it")]
        note = (f"; {len(unenforced_both)} NOT ENFORCED on both sides"
                if unenforced_both else "")
        return [Result("deep", f"{db} checks", "ok",
                       f"{len(src)} CHECK constraints match{note}")]

    # privileges live in the grant tables, not in the schema, so neither a
    # dump/restore nor a CDC stream carries them: the data arrives and the
    # permissions do not. The symptom is an app that connects fine and then
    # cannot read its own tables, which nothing else here would catch.
    def _check_grants(self, db):
        ddb = self._d("dst", db)
        src, serr = self._grants_for_db("src", db)
        dst, derr = self._grants_for_db("dst", ddb)
        # an account that cannot read the grant tables must say so - reporting
        # a match nobody was able to look at is worse than reporting nothing.
        # warn, not skip: skip counts as ok everywhere downstream, so an
        # unreadable grant table would leave the run green.
        if serr or derr:
            where = "source" if serr else "target"
            return [Result("deep", f"{db} grants", "warn",
                           f"could not read privileges on {where}:"
                           f" {serr or derr}", "",
                           "grant SELECT on mysql.* to the checking account,"
                           " or check privileges by hand")]

        # comparing users that exist on one side only just restates the users
        # check; what is worth knowing here is whether the users that DO exist
        # on both sides carry the same rights on this database
        on_target = getattr(self, "_grant_users", {}).get("dst", set())
        # a user present on the target is comparable even when it holds
        # nothing there yet - that is a total loss of its grants, and it used
        # to be reported as "the user does not exist on target", which was
        # both wrong and pointed at the wrong fix
        shared = sorted(set(src) & on_target)
        miss, extra = [], []
        for who in shared:
            for g in sorted(src[who] - dst.get(who, set())):
                miss.append(f"{who}: {g}")
            for g in sorted(dst.get(who, set()) - src[who]):
                extra.append(f"{who}: {g}")
        only_src = sorted(set(src) - on_target)
        total = sum(len(v) for v in src.values())

        if miss or extra:
            parts = []
            if miss:
                parts.append(f"{len(miss)} grants missing on target: "
                             + "; ".join(miss[:3]))
            if extra:
                parts.append(f"{len(extra)} grants on target that the source"
                             " does not give: " + "; ".join(extra[:3]))
            if only_src:
                parts.append(f"{len(only_src)} users grant on this db at source"
                             " but do not exist on target: "
                             + ", ".join(only_src[:3]))
            return [Result("deep", f"{db} grants", "diff", "; ".join(parts), "",
                           "create the users first - migkit users create"
                           " replays their GRANTs from the source")]
        if only_src:
            return [Result("deep", f"{db} grants", "diff",
                           f"{len(only_src)} users hold grants on this db at"
                           f" source but do not exist on target:"
                           f" {', '.join(only_src)}", "",
                           "migkit users create <hop> --apply")]
        return [Result("deep", f"{db} grants", "ok",
                       f"{total} grants match on {len(shared)} users"
                       if shared else "no non-system user grants this database")]

    def _grants_for_db(self, side, dbn):
        """{user@host -> set(privileges)} granted ON this database.

        Read through SHOW GRANTS because that is what `migkit users create`
        replays when it builds the users, so the check and the repair agree on
        what a grant is. information_schema.*_PRIVILEGES would be one query
        instead of N, but it is filtered to what the connected account happens
        to be able to see - the same trap the postgres side avoids by reading
        relacl rather than information_schema.
        """
        import os
        from ..users import MYSQL_SYS
        # same knob the postgres side reads, so one setting covers both engines
        ign = MYSQL_SYS | {r.strip() for r in os.environ.get(
            "GRANTS_IGNORE_ROLES", "").split(",") if r.strip()}
        try:
            users = [(u, h) for u, h in self._q(
                side, "select user, host from mysql.user")
                if u not in ign and not u.startswith("AWS_")]
        except Exception as e:
            return {}, f"{type(e).__name__}: {str(e)[:70]}"

        # `db`.* and `db`.`tbl`, with or without the backticks MySQL prints
        # depending on version, and never `dbase` when we asked for `db`
        pat = re.compile(r"\sON\s+`?" + re.escape(dbn) + r"`?\s*\.", re.I)
        out = {}
        # every account this side has, whether or not it holds anything on
        # this database. Without it, a user that exists on the target but was
        # granted nothing there is indistinguishable from one that is not
        # there at all - and it used to be reported as the second.
        seen = getattr(self, "_grant_users", None)
        if seen is None:
            seen = self._grant_users = {}
        seen = seen.setdefault(side, set())
        seen.clear()
        store = getattr(self, "_grant_raw", None)
        if store is None:
            store = self._grant_raw = {}
        raw = store.setdefault(side, {})
        for u, h in users:
            try:
                rows = self._q(side, "show grants for %s@%s", (u, h))
            except Exception:
                # a user that was dropped mid-scan, or one this account may not
                # inspect: skip the user, do not fail the whole check
                continue
            keep = [r[0] for r in rows
                    if pat.search(r[0]) and not r[0].startswith("GRANT PROXY")]
            # `keep` already holds the statement text, so the canonical form
            # is taken from the line - indexing it again would take its first
            # character, which for a moment it did: every user's privilege set
            # came out as {'G'}
            who = f"{u}@{h}"
            seen.add(who)
            privs = {self._canon_grant(line, dbn) for line in keep}
            if privs:
                out[who] = privs
                # the server's own text, kept so the repair replays exactly
                # what it printed instead of reassembling a GRANT from the
                # canonical form and getting the quoting subtly wrong
                for line in keep:
                    raw[(who, self._canon_grant(line, dbn))] = line
        return out, ""

    @staticmethod
    def _canon_grant(text, dbn):
        """Same grant, same string on both sides.

        Two servers print the identical privilege set differently: spacing,
        backtick style, and an IDENTIFIED BY tail on older versions. Without
        this every user reads as a diff on a pair of servers that agree.

        The database name is replaced by a marker because a hop may map the
        database to a different name on the target - otherwise every single
        grant would differ for the one reason we already know about.

        Deliberately NOT uppercased: on Linux MySQL table names are
        case-sensitive, so folding case here would let CatalogProduct and
        catalogproduct compare equal. SHOW GRANTS already prints privilege
        keywords in upper case, so there is nothing left to normalise.
        """
        t = re.sub(r"\s+", " ", text.strip()).replace("`", "")
        t = re.sub(r"\s+IDENTIFIED BY.*$", "", t, flags=re.I)
        t = re.sub(r"(\sON\s+)" + re.escape(dbn) + r"(\s*\.)",
                   r"\1<db>\2", t, flags=re.I)
        return t.rstrip(";")

    def _constraint_repair(self, db):
        """Turn NOT ENFORCED check constraints back on.

        MySQL's counterpart to PostgreSQL's NOT VALID, and the inverse is
        exact - `ALTER CHECK ... NOT ENFORCED` puts it back as it was, which
        is more than the PostgreSQL side can say.

        Measured: enforcing a constraint that existing rows violate fails with
        `ERROR 3819 Check constraint '...' is violated` and changes nothing,
        so no pre-scan is needed here either.
        """
        ddb = self._d("dst", db)
        q = ("select tc.table_name, tc.constraint_name, tc.enforced"
             " from information_schema.table_constraints tc"
             " where tc.constraint_schema=%s and tc.constraint_type='CHECK'"
             " and tc.table_name not like 'migkit%%'")
        try:
            src = {(r[0], r[1]): str(r[2]).upper()
                   for r in self._q("src", q, (db,))}
            dst = {(r[0], r[1]): str(r[2]).upper()
                   for r in self._q("dst", q, (ddb,))}
        except Exception:
            # MySQL 5.7 parses CHECK and discards it - no catalog to read
            return None
        turn_on = [k for k, v in sorted(dst.items())
                   if v == "NO" and src.get(k) == "YES"]
        if not turn_on:
            return None
        stmts = [f"ALTER TABLE `{tbl}` ALTER CHECK `{name}` ENFORCED;"
                 for tbl, name in turn_on]
        undo = [f"ALTER TABLE `{tbl}` ALTER CHECK `{name}` NOT ENFORCED;"
                for tbl, name in turn_on]
        return RepairAction(db, "constraints", stmts, undo,
                            f"{len(turn_on)} check constraints the target"
                            " was not enforcing")

    def _grant_repair(self, db):
        """Replay the grants the mover did not carry, with a REVOKE to undo.

        The statement replayed is the one the source server printed, with only
        the database name swapped for the target's. Reassembling a GRANT from
        the canonical comparison form would mean re-deriving the quoting, and
        the quoting is exactly what differs between servers - which is why the
        canonical form exists in the first place.

        Users that exist on one side only are left alone: that is the users
        check's finding, and `migkit users create` replays their whole grant
        set. Granting to an account that does not exist would just fail.
        """
        ddb = self._d("dst", db)
        src, serr = self._grants_for_db("src", db)
        dst, derr = self._grants_for_db("dst", ddb)
        if serr or derr:
            return None
        raw = getattr(self, "_grant_raw", {}).get("src", {})
        # an account that exists on the target holding nothing on this
        # database is exactly the case worth repairing, and `dst` only lists
        # accounts that already hold something - so the target's account list
        # decides, not its grant list
        on_target = getattr(self, "_grant_users", {}).get("dst", set())
        stmts, undo, n = [], [], 0
        for who in sorted(set(src) & on_target):
            for g in sorted(src[who] - dst.get(who, set())):
                line = raw.get((who, g))
                if not line:
                    continue
                fwd = self._retarget_grant(line, db, ddb)
                stmts.append(fwd)
                undo += self._revoke_for(fwd)
                n += 1
        if not stmts:
            return None
        return RepairAction(db, "grants", stmts, undo,
                            f"{n} grants the mover did not carry")

    @staticmethod
    def _retarget_grant(line, src_db, dst_db):
        """The source's own GRANT, pointed at the target's database name."""
        out = re.sub(r"(\sON\s+)`?" + re.escape(src_db) + r"`?(\s*\.)",
                     r"\1`" + dst_db + r"`\2", line, flags=re.I)
        return out.rstrip(";") + ";"

    @staticmethod
    def _revoke_for(grant):
        """The statements that undo one GRANT.

        `WITH GRANT OPTION` is revoked separately because MySQL will not take
        it as part of the privilege list, and leaving it granted would undo
        less than the forward statement did.
        """
        body = grant.rstrip(";")
        extra = []
        if re.search(r"\s+WITH\s+GRANT\s+OPTION\s*$", body, re.I):
            body = re.sub(r"\s+WITH\s+GRANT\s+OPTION\s*$", "", body,
                          flags=re.I)
            extra.append(re.sub(r"^GRANT\s+.*?\s+ON\s", "GRANT OPTION ON ",
                                body, flags=re.I)
                         .replace(" TO ", " FROM ", 1)
                         .replace("GRANT OPTION ON ", "REVOKE GRANT OPTION ON ",
                                  1) + ";")
        rev = re.sub(r"^GRANT\s", "REVOKE ", body, flags=re.I)
        rev = rev.replace(" TO ", " FROM ", 1)
        return [rev + ";"] + extra

    def repair_plan(self, db, kind):
        actions = []
        if kind in ("sequences", "all"):
            q = ("select table_name, auto_increment from information_schema.tables"
                 " where table_schema=%s and auto_increment is not null")
            ddb = self._d("dst", db)
            src = dict(self._q("src", q, (db,), fresh=True))
            dst = dict(self._q("dst", q, (ddb,), fresh=True))
            stmts, undo, refuse = [], [], []
            for tbl, sv in sorted(src.items()):
                dv = dst.get(tbl)
                if dv == sv:
                    continue
                if dv is None:
                    refuse.append(f"{tbl}: not on target")
                    continue
                # only ever raise. Lowering a counter is how the next insert
                # collides with a row that already exists, and a target that is
                # ahead is normal while changes are still being applied.
                if dv > sv:
                    refuse.append(f"{tbl}: target {dv} > source {sv}")
                    continue
                stmts.append(f"alter table `{ddb}`.`{tbl}` auto_increment = {sv};"
                             f"  -- dst now {dv}")
                undo.append(f"alter table `{ddb}`.`{tbl}` auto_increment = {dv};")
            same = sum(1 for tbl, v in src.items() if dst.get(tbl) == v)
            note = f"{len(stmts)} counters raised, {same} already equal"
            if refuse:
                note += (f"; left alone {len(refuse)} (target not behind): "
                         + "; ".join(refuse[:4]))
            if stmts or refuse:
                actions.append(RepairAction(db, "sequences", stmts, undo, note))
        if kind in ("schema", "all"):
            # GRANT is DDL: it goes with the schema repair rather than adding
            # a choice to --kind, so a plain `migkit sync --apply` restores it
            g = self._grant_repair(db)
            if g:
                actions.append(g)
            c = self._constraint_repair(db)
            if c:
                actions.append(c)
        if kind in ("rows", "all"):
            d = self.hop.report_dir(db)
            tables = sorted({f.name.split(".")[0][len("data-"):]
                             for f in d.glob("data-*.missing")}
                            | {f.name.split(".")[0][len("data-"):]
                               for f in d.glob("data-*.extra")}
                            | {f.name.split(".")[0][len("data-"):]
                               for f in d.glob("data-*.changed")})
            for t in tables:
                counts = []
                for k in ("missing", "extra", "changed"):
                    f = d / f"data-{t}.{k}"
                    if f.exists():
                        counts.append(f"{k}={sum(1 for _ in f.open())}")
                stmts = [f"resync pks for {t} ({', '.join(counts)})"]
                if which("pt-table-sync"):
                    s, tg = self.hop.source, self.hop.target
                    p = run(["pt-table-sync", "--print",
                             f"h={s.host},P={s.port},u={s.user},"
                             f"p={s.password},D={db},t={t}",
                             f"h={tg.host},P={tg.port},u={tg.user},"
                             f"p={tg.password},D={self._d('dst', db)}"],
                            check=False, timeout=300)
                    sql = [l for l in p.stdout.splitlines()
                           if l and not l.startswith("#")]
                    if sql:
                        stmts += [f"  {l}" for l in sql[:10]]
                        if len(sql) > 10:
                            stmts.append(f"  ... {len(sql) - 10} more (pt-table-sync)")
                actions.append(RepairAction(
                    db, "rows", stmts,
                    [], f"{t}: delete extra/changed on target (saved to undo"
                        " first), reinsert from source"))
        if kind in ("schema", "all"):
            act = self._schema_repair_action(db)
            if act:
                actions.append(act)
        return actions

    def _schema_repair_action(self, db):
        """atlas-generated DDL to align the target's objects (columns,
        indexes, PK/FK, views, routines, triggers) to the source, reverse
        DDL as undo. Structure only; data untouched."""
        if not which("atlas"):
            return None
        fwd, undo = self.migration_pair(db)
        if not fwd:
            return None
        return RepairAction(db, "schema", fwd.splitlines(),
                            (undo or "").splitlines(),
                            "atlas-generated DDL to align target objects to"
                            " source (review before --apply; reverse DDL"
                            " saved to undo)")

    def apply(self, db, action):
        if action.kind == "sequences":
            conn = self._conn("dst")
            try:
                with conn.cursor() as cur:
                    for s in action.statements:
                        cur.execute(s.split("  --")[0])
                conn.commit()
            finally:
                conn.close()
            return
        if action.kind in ("schema", "grants", "constraints"):
            # mysql CLI handles routine/trigger bodies (DELIMITER) correctly
            tg = self.hop.target
            ddl = "\n".join(action.statements) + "\n"
            if action.kind == "grants":
                ddl += "flush privileges;\n"
            run(["mysql", "-h", tg.host, "-P", str(tg.port), "-u", tg.user,
                 f"-p{tg.password}", self._d("dst", db)], input=ddl)
            return
        if action.kind != "rows":
            # the same trap the postgres side had: an unhandled kind used to
            # fall through to the row path and be read as a table name
            raise RuntimeError(f"no way to apply a {action.kind!r} repair")
        t = action.statements[0].split()[3]
        self._apply_rows(db, t, getattr(self, "_undo_dir", None))

    def _apply_rows(self, db, t, undo_dir=None):
        d = self.hop.report_dir(db)
        ddb = self._d("dst", db)
        pks = self._pk_cols(db, t)
        cols = self._cols(db, t)

        def read(kind):
            f = d / f"data-{t}.{kind}"
            return [l.split("\t") for l in
                    f.read_text().splitlines()] if f.exists() else []

        missing, extra, changed = read("missing"), read("extra"), read("changed")
        # keep-target preserves target-changed rows: fix only missing/extra
        if self.hop.options.get("on_conflict") == "keep-target":
            changed = []
        to_delete = extra + changed
        to_copy = missing + changed
        cond = " and ".join(f"cast(`{c}` as char) = %s" for c in pks)
        collist = ", ".join(f"`{c}`" for c in cols)
        ph = ", ".join(["%s"] * len(cols))

        undo = Path(undo_dir) if undo_dir else d / "undo"
        undo.mkdir(parents=True, exist_ok=True)
        # record every touched pk's pre-repair target row so restore is exact
        sconn, dconn = self._conn("src"), self._conn("dst")
        touched = missing + extra + changed
        entries = []
        try:
            with dconn.cursor() as cur:
                cur.execute("set foreign_key_checks = 0")
                for pk in touched:
                    cur.execute(f"select {collist} from `{ddb}`.`{t}`"
                                f" where {cond}", pk)
                    got = cur.fetchall()
                    entries.append({"pk": pk, "cols": cols,
                                    "old": [list(r) for r in got] or None})
                (undo / f"rows-{t}.jsonl").write_text(
                    "".join(json.dumps(e, default=str) + "\n"
                            for e in entries))
                # pt-table-sync when available (bounded to the verified pks);
                # builtin delete+copy is the fallback
                pt_sql = self._pt_sync_sql(db, t, pks, touched)
                if pt_sql:
                    for stmt in pt_sql:
                        cur.execute(stmt)
                else:
                    for pk in to_delete:
                        cur.execute(f"delete from `{ddb}`.`{t}`"
                                    f" where {cond}", pk)
                    with sconn.cursor() as scur:
                        for pk in to_copy:
                            scur.execute(f"select {collist} from `{db}`.`{t}`"
                                         f" where {cond}", pk)
                            rows = scur.fetchall()
                            if rows:
                                cur.executemany(
                                    f"insert into `{ddb}`.`{t}` ({collist})"
                                    f" values ({ph})", rows)
            dconn.commit()
        finally:
            sconn.close()
            dconn.close()

    def _pt_sync_sql(self, db, t, pks, touched):
        if not which("pt-table-sync") \
                or not self.hop.options.get("pt_apply", True):
            return None
        if len(pks) != 1 or not touched or len(touched) > 1000:
            return None
        vals = ", ".join("'" + str(k[0]).replace("'", "''") + "'"
                         for k in touched)
        s, tg = self.hop.source, self.hop.target
        p = run(["pt-table-sync", "--print",
                 "--where", f"`{pks[0]}` in ({vals})",
                 f"h={s.host},P={s.port},u={s.user},p={s.password},"
                 f"D={db},t={t}",
                 f"h={tg.host},P={tg.port},u={tg.user},p={tg.password},"
                 f"D={self._d('dst', db)}"],
                check=False, timeout=600)
        if p.returncode not in (0, 2):
            return None
        sql = [l for l in p.stdout.splitlines()
               if l and not l.startswith("#")]
        return sql or None

    def restore_rows(self, db, undo_dir):
        """Replay a complete row undo: for each touched pk, delete whatever is
        there now, then re-insert the saved pre-repair row (or leave absent)."""
        undo_dir = Path(undo_dir)
        files = sorted(undo_dir.glob("rows-*.jsonl"))
        if not files:
            return 0
        n = 0
        ddb = self._d("dst", db)
        dconn = self._conn("dst")
        try:
            with dconn.cursor() as cur:
                cur.execute("set foreign_key_checks = 0")
                for f in files:
                    t = f.name[len("rows-"):-len(".jsonl")]
                    pks = self._pk_cols(db, t)
                    for line in f.read_text().splitlines():
                        e = json.loads(line)
                        cols = e["cols"]
                        cond = " and ".join(
                            f"cast(`{c}` as char) = %s" for c in pks)
                        cur.execute(f"delete from `{ddb}`.`{t}` where {cond}",
                                    e["pk"])
                        if e["old"]:
                            collist = ", ".join(f"`{c}`" for c in cols)
                            ph = ", ".join(["%s"] * len(cols))
                            cur.executemany(
                                f"insert into `{ddb}`.`{t}` ({collist})"
                                f" values ({ph})", e["old"])
                        n += 1
            dconn.commit()
        finally:
            dconn.close()
        return n

    def assess(self):
        items = []

        def add(level, scope, item, detail=""):
            items.append({"level": level, "scope": scope,
                          "item": item, "detail": str(detail)})

        def var(side, name):
            r = self._q(side, f"show variables like '{name}'")
            return r[0][1] if r else "?"

        items += self._brand_rows()
        sv = self._q("src", "select version()")[0][0]
        dv = self._q("dst", "select version()")[0][0]
        items.append(self._version_row(sv, dv, parts=2))
        for name, want, lvl in (("log_bin", "ON", "fail"),
                                ("binlog_format", "ROW", "fail"),
                                ("binlog_row_image", "FULL", "warn")):
            v = var("src", name)
            add("pass" if v == want else lvl, "instance",
                f"{name}={want} on source (CDC requirement)", v)
        ret = var("src", "binlog_expire_logs_seconds")
        try:
            ok = int(ret) >= 86400
        except ValueError:
            ok = False
        add("pass" if ok else "warn", "instance",
            "binlog retention at least 24h", ret)

        # a long-running open transaction pins InnoDB purge and stalls CDC
        # (the target lags for as long as it stays open); flag by age
        lrt = self._q("src",
                      "select count(*), coalesce(max(timestampdiff(second,"
                      " trx_started, now())),0)"
                      " from information_schema.innodb_trx")
        n, age = (lrt[0][0], int(lrt[0][1])) if lrt else (0, 0)
        add("pass" if age < 900 else "warn" if age < 3600 else "fail",
            "instance", "long-running transactions blocking CDC/purge",
            f"{n} open, oldest {age}s" if n else "none")
        for db in self.databases():
            cs = self._q("src", "select default_character_set_name,"
                         " default_collation_name from"
                         " information_schema.schemata"
                         " where schema_name = %s", (db,))
            cd = self._q("dst", "select default_character_set_name,"
                         " default_collation_name from"
                         " information_schema.schemata"
                         " where schema_name = %s", (self._d("dst", db),))
            if not cd:
                add("warn", db, "database exists on target", "missing")
            else:
                add("pass" if cs == cd else "warn", db,
                    "charset and collation match",
                    f"src {cs[0]} / dst {cd[0]}")

        try:
            uq = ("select concat(user,'@',host),"
                  " authentication_string from mysql.user"
                  " where user not in ('mysql.sys','mysql.session',"
                  "'mysql.infoschema','root') and user not like 'rds%'")
            su = dict(self._q("src", uq))
            du = dict(self._q("dst", uq))
            miss = sorted(set(su) - set(du))
            add("pass" if not miss else "warn", "instance",
                "user accounts present on target",
                f"{len(su)} src / {len(du)} dst"
                + (f", missing: {', '.join(miss[:5])}" if miss else ""))
            drift = sorted(u for u in (set(su) & set(du))
                           if su[u] and du[u] and su[u] != du[u])
            add("pass" if not drift else "fail", "instance",
                "account passwords match source",
                "all match" if not drift
                else f"password differs for: {', '.join(drift[:5])}"
                     " -> apps cannot log in on target")
        except Exception:
            add("warn", "instance", "cannot read mysql.user",
                "grant select on mysql.user to compare accounts")
        inv = self._handwork()
        items += inv.rows() + inv.summary()
        items += self._mover_leftovers()
        items += self._client_tool_versions(("mysqldump", "mysql"), dv)
        return items

    def _mover_leftovers(self):
        """What a mover added to the source and did not take away.

        MySQL has no replication slot to pin WAL, so nothing here is urgent in
        the way the PostgreSQL side can be - but a leftover database or table
        is still evidence of which mover touched this server, which matters
        when two of them ran and only one is admitted to.
        """
        from .. import leftovers as _lo
        found, items = [], []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "source leftovers",
                          "item": item, "detail": str(detail)})
        try:
            for (name,) in self._q("src", "show databases"):
                found.append(("database", name))
            for (u, h) in self._q("src", "select user, host from mysql.user"):
                found.append(("user", f"{u}@{h}"))
        except Exception as e:
            add("warn", "cannot list databases or users on the source",
                f"{str(e)[:90]} - unknown, not clean")
            return items
        for db in self.databases():
            try:
                for (name,) in self._q(
                        "src", "select table_name from information_schema"
                               ".tables where table_schema = %s", (db,)):
                    found.append(("table", name))
            except Exception as e:
                add("warn", f"cannot list tables in {db}",
                    f"{str(e)[:90]} - unknown, not clean")
        by = _lo.group(found)
        if not by:
            add("pass", "no mover artifacts left in the source",
                f"{len(found)} objects examined")
            return items
        add("warn", f"{_lo.total(by)} mover artifacts left in the source",
            _lo.describe(by) + ". Drop them once every leg that used them is"
            " finished - they are on the source, so nothing on the target"
            " tells you they are there")
        return items

    def _handwork(self):
        """Inventory the work the move will not do, per `migkit.handwork`.

        Only things migkit genuinely leaves behind belong here. Routines,
        triggers and views are absent on purpose: `_dump_schema` dumps them,
        the textual diff compares them, and atlas generates the DDL to create
        a missing one - so they are carried end to end.

        Events are the boundary case, and the boundary is narrower than
        "not dumped". Measured against MySQL 8 with a source holding one event
        and a target holding none: `_dump_schema` does include it (it passes
        `--events`), and the object check names it - `event 1/0 missing: ev`.
        What does not happen is the repair. atlas, which is what writes the
        fix DDL, reported `atlas diff clean` for that same pair: it does not
        model MySQL events at all. So a missing event is found and never
        fixed, and creating it is hands work.
        """
        from .. import handwork
        inv = handwork.Inventory()
        for db in self.databases():
            try:
                # Row-based replication finds the row to update through a
                # primary key or a unique NOT NULL index; with neither it
                # scans the table per row, and migkit has no key to name a
                # differing row by either.
                rows = self._q("src",
                    "select t.table_name from information_schema.tables t"
                    " where t.table_schema = %s"
                    " and t.table_type = 'BASE TABLE'"
                    " and not exists (select 1 from information_schema"
                    ".statistics s where s.table_schema = t.table_schema"
                    " and s.table_name = t.table_name and s.non_unique = 0"
                    " and s.nullable <> 'YES')", (db,))
                inv.add("no-row-key", db, "tables", [r[0] for r in rows])

                eng = self._q("src",
                    "select table_name, engine from information_schema.tables"
                    " where table_schema = %s and table_type = 'BASE TABLE'"
                    " and engine is not null and engine <> 'InnoDB'", (db,))
                # MEMORY empties on restart and MyISAM has no transactions, so
                # neither can be handed over by a consistent snapshot
                inv.add("not-carried", db, "non-InnoDB tables",
                        [f"{r[0]} ({r[1]})" for r in eng])

                ev = self._q("src",
                    "select event_name, status from information_schema.events"
                    " where event_schema = %s", (db,))
                # detected by the object check, never written into the fix
                # DDL - see this method's docstring for the measurement
                inv.add("not-carried", db, "scheduled events (detected, but"
                        " no tool generates the DDL to recreate them)",
                        [r[0] for r in ev])
                if ev:
                    # an event enabled on the target rewrites rows while the
                    # sync is still running - the same hazard as a TTL index
                    inv.add("decide-then-apply", db,
                            "events that must stay disabled until cutover",
                            [r[0] for r in ev if str(r[1]).upper() == "ENABLED"])
            except Exception as e:
                inv.unknown("no-row-key", db, f"catalogue query failed: {e}")
        self._definer_handwork(inv)
        return inv

    def _definer_handwork(self, inv):
        """Definers that will not exist on the target.

        A view or routine keeps the `DEFINER=user@host` it was created with.
        Recreating it against a target where that account is missing succeeds
        - and then fails at the moment something uses it, which is usually
        after cutover.
        """
        for db in self.databases():
            try:
                rows = self._q("src",
                    "select definer, 'routine', routine_name from"
                    " information_schema.routines where routine_schema = %s"
                    " union all select definer, 'view', table_name from"
                    " information_schema.views where table_schema = %s"
                    " union all select definer, 'trigger', trigger_name from"
                    " information_schema.triggers where trigger_schema = %s"
                    " union all select definer, 'event', event_name from"
                    " information_schema.events where event_schema = %s",
                    (db, db, db, db))
            except Exception as e:
                inv.unknown("target-prereq", db,
                            f"cannot read object definers: {e}")
                continue
            if not rows:
                continue
            try:
                have = {f"{u}@{h}" for u, h in
                        self._q("dst", "select user, host from mysql.user")}
            except Exception as e:
                inv.unknown("target-prereq", db,
                            f"cannot read mysql.user on the target to check"
                            f" definers: {e}")
                continue
            missing = sorted({f"{kind} {name} (definer {d})"
                              for d, kind, name in rows
                              if str(d).replace("`", "") not in have})
            inv.add("target-prereq", db,
                    "accounts named as DEFINER but absent on the target",
                    missing)

    def list_move_tables(self, db):
        return [("", t) for t in self._tables("src", db)]

    def move_table(self, db, sch, tbl, chunk, ck, log):
        t = tbl if tbl else sch
        key = f"{db}.{t}"
        ddb = self._d("dst", db)
        st = ck.setdefault(key, {})
        if st.get("done"):
            log(f"{key}: done earlier, skip")
            return
        cols = self._cols(db, t)
        collist = ", ".join(f"`{c}`" for c in cols)
        ph = ", ".join(["%s"] * len(cols))
        pks = self._pk_cols(db, t)
        intpk = None
        if len(pks) == 1:
            r = self._q("src", "select data_type from information_schema.columns"
                        " where table_schema=%s and table_name=%s"
                        " and column_name=%s", (db, t, pks[0]))
            if r and r[0][0] in ("tinyint", "smallint", "mediumint",
                                 "int", "bigint"):
                intpk = pks[0]
        sconn, dconn = self._conn("src"), self._conn("dst")
        try:
            with dconn.cursor() as dcur, sconn.cursor() as scur:
                dcur.execute("set foreign_key_checks = 0")
                if not intpk:
                    log(f"{key}: no single int pk, single-shot copy")
                    dcur.execute(f"truncate `{ddb}`.`{t}`")
                    scur.execute(f"select {collist} from `{db}`.`{t}`")
                    while True:
                        rows = scur.fetchmany(5000)
                        if not rows:
                            break
                        dcur.executemany(
                            f"insert into `{ddb}`.`{t}` ({collist})"
                            f" values ({ph})", rows)
                    dconn.commit()
                    st["done"] = True
                    ck.save()
                    return
                mm = self._q("src", f"select coalesce(min(`{intpk}`), 0),"
                             f" coalesce(max(`{intpk}`), 0)"
                             f" from `{db}`.`{t}`")[0]
                lo, hi = int(mm[0]), int(mm[1])
                last = st.get("last", lo - 1)
                while last < hi:
                    nxt = min(last + chunk, hi)
                    dcur.execute(f"delete from `{ddb}`.`{t}`"
                                 f" where `{intpk}` > %s and `{intpk}` <= %s",
                                 (last, nxt))
                    scur.execute(f"select {collist} from `{db}`.`{t}`"
                                 f" where `{intpk}` > %s and `{intpk}` <= %s",
                                 (last, nxt))
                    while True:
                        rows = scur.fetchmany(5000)
                        if not rows:
                            break
                        dcur.executemany(
                            f"insert into `{ddb}`.`{t}` ({collist})"
                            f" values ({ph})", rows)
                    dconn.commit()
                    last = nxt
                    st["last"] = last
                    ck.save()
                    log(f"{key}: up to {intpk}={last:,} of {hi:,}")
                st["done"] = True
                ck.save()
        finally:
            sconn.close()
            dconn.close()

    def replicate_sql(self, db, copy_data=True):
        s, t = self.hop.source, self.hop.target
        pos = None
        for _q_ in ("show binary log status", "show master status"):
            try:
                pos = self._q("src", _q_)
            except Exception:
                pos = None
            if pos:
                break
        coords = f"file {pos[0][0]} pos {pos[0][1]}" if pos else "unknown"
        gtid = self._q("src", "show variables like 'gtid_mode'")
        gtid_on = gtid and gtid[0][1] == "ON"
        src_cmds = [
            "create user if not exists 'migkit_repl'@'%'"
            " identified by 'CHANGE_ME';",
            "grant replication slave on *.* to 'migkit_repl'@'%';",
        ]
        if "rds.amazonaws.com" in (t.host or ""):
            dst_cmds = [
                f"call mysql.rds_set_external_source ('{s.host}', {s.port},"
                " 'migkit_repl', 'CHANGE_ME',"
                + (f" '{pos[0][0]}', {pos[0][1]}," if pos else " '', 4,")
                + " 0);",
                "call mysql.rds_start_replication;",
            ]
        else:
            auto = "SOURCE_AUTO_POSITION = 1" if gtid_on else                 (f"SOURCE_LOG_FILE = '{pos[0][0]}',"
                 f" SOURCE_LOG_POS = {pos[0][1]}" if pos else "")
            dst_cmds = [
                f"change replication source to SOURCE_HOST = '{s.host}',"
                f" SOURCE_PORT = {s.port}, SOURCE_USER = 'migkit_repl',"
                f" SOURCE_PASSWORD = 'CHANGE_ME',"
                f" GET_SOURCE_PUBLIC_KEY = 1, {auto};",
                "start replica;",
            ]
        return {"src": src_cmds, "dst": dst_cmds,
                "drop_src": ["drop user if exists 'migkit_repl'@'%';"],
                "drop_dst": ["stop replica;", "reset replica all;"],
                "status": "show replica status",
                "note": f"binlog now at {coords},"
                        f" gtid {'ON' if gtid_on else 'OFF'},"
                        " run move first then replicate from these coords"}

    def setup_target_plan(self, db):
        s, t = self.hop.source, self.hop.target
        tdb = self._d("dst", db)
        return [
            f"mysqldump -h {s.host} -u {s.user} -p --no-data --routines --triggers"
            f" --events {db} > {db}.schema.sql",
            f"mysql -h {t.host} -u {t.user} -p -e 'create database `{tdb}`"
            f" character set utf8mb4'  # match source charset/collation",
            f"mysql -h {t.host} -u {t.user} -p {tdb} < {db}.schema.sql",
            "-- set foreign_key_checks=0 on the load session or drop FKs until cutover",
            "-- then start the migration service full load + binlog replication into existing tables",
        ]

    def migration_pair(self, db):
        from urllib.parse import quote
        if not which("atlas"):
            return None, None
        s, t = self.hop.source, self.hop.target
        su = (f"mysql://{s.user}:{quote(s.password, safe='')}"
              f"@{s.host}:{s.port}/{db}")
        tu = (f"mysql://{t.user}:{quote(t.password, safe='')}"
              f"@{t.host}:{t.port}/{self._d('dst', db)}")

        def diff(a, b):
            p = run(["atlas", "schema", "diff", "--from", a, "--to", b,
                     "--exclude", "migkit_changelog"],
                    check=False, timeout=180)
            text = p.stdout.strip()
            if p.returncode or "Schemas are synced" in text:
                return ""
            return text
        return diff(tu, su), diff(su, tu)

    def fetch_sample_df(self, side, db, table, limit):
        import pandas as pd
        t = table.split(".", 1)[-1]
        cols = self._cols(db, t)
        rows = self._q(side,
                       f"select * from `{self._d(side, db)}`.`{t}` limit {limit}")
        return pd.DataFrame(rows, columns=cols)

    def watch_sample(self, db):
        import time
        q = ("select coalesce(sum(table_rows), 0) from information_schema.tables"
             " where table_schema=%s")
        return {"db": db, "ts": time.time(),
                "src_rows": int(self._q("src", q, (db,))[0][0]),
                "dst_rows": int(self._q("dst", q, (self._d("dst", db),))[0][0])}
