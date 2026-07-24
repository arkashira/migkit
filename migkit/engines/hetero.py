import csv
import io
import re
import subprocess

from ..util import run, tool_env, which
from .base import Engine, Result


class HeteroEngine(Engine):
    """Cross-engine hop, mysql source to postgres target first.

    Reuses the native engines for each side: reads through the source
    engine driver, writes through the target engine bulk path. Schema
    crosses via pgloader when installed, else sqlglot transpilation.
    Data validation crosses via reladiff which speaks both dialects."""

    checks = ("counts", "data")
    counts_from_data = True

    def __init__(self, hop):
        super().__init__(hop)
        pair = (hop.options.get("source_engine", "mysql"),
                hop.options.get("target_engine", "postgres"))
        if pair != ("mysql", "postgres"):
            raise SystemExit(f"hetero {pair[0]}->{pair[1]} not built yet,"
                             " mysql->postgres is the first pair")
        from .mysql import MySQLEngine
        from .postgres import PostgresEngine
        self.my = MySQLEngine(hop)
        self.pg = PostgresEngine(hop)

    def databases(self):
        return self.my.databases()

    def _url(self, side, db):
        from urllib.parse import quote
        ep = self.hop.source if side == "src" else self.hop.target
        proto = "mysql" if side == "src" else "postgresql"
        return (f"{proto}://{ep.user}:{quote(ep.password, safe='')}"
                f"@{ep.host}:{ep.port}/{db}")

    def check_counts(self, db):
        bad = []
        total_a = total_b = 0
        for t in self.my._tables("src", db):
            a = self.my._q("src", f"select count(*) from `{db}`.`{t}`")[0][0]
            try:
                b = int(self.pg._psql("dst", db,
                                      f'select count(*) from "{t}"'))
            except RuntimeError:
                bad.append(f"{t} missing on target")
                continue
            total_a += a
            total_b += b
            if a != b:
                bad.append(f"{t} src={a} dst={b}")
        if bad:
            return [Result("counts", db, "diff", "; ".join(bad[:10]))]
        return [Result("counts", db, "ok",
                       f"rows {total_a:,}=={total_b:,} across engines")]

    def check_data(self, db, table=None, stream=None, with_counts=False):
        if not which("reladiff"):
            return [Result("data", db, "error",
                           "reladiff needed for cross-engine data compare,"
                           " run bootstrap.sh")]
        res = []
        tables = [table] if table else self.my._tables("src", db)
        total_a = total_b = 0
        bad_counts = []
        for t in tables:
            pks = self.my._pk_cols(db, t)
            if not pks:
                res.append(Result("data", f"{db}.{t}", "diff",
                                  "no pk, cross-engine compare needs one"))
                if with_counts:
                    a = self.my._q("src",
                                   f"select count(*) from `{db}`.`{t}`")[0][0]
                    try:
                        b = int(self.pg._psql("dst", db,
                                              f'select count(*) from "{t}"'))
                    except RuntimeError:
                        bad_counts.append(f"{t} missing on target")
                        continue
                    total_a += a
                    total_b += b
                    if a != b:
                        bad_counts.append(f"{t} src={a} dst={b}")
                continue
            cmd = ["reladiff", self._url("src", db), t,
                   self._url("dst", db), t, "--stats",
                   "-j", str(self.hop.workers), "-c", "%"]
            for k in pks:
                cmd += ["-k", k]
            p = run(cmd, check=False, timeout=3600)
            text = p.stdout + p.stderr
            m = re.search(r"(\d+) rows in table A.*?(\d+) rows in table B",
                          text, re.S)
            rows = f"rows {m.group(1)}=={m.group(2)}, " if m else ""
            if m:
                a, b = int(m.group(1)), int(m.group(2))
                total_a += a
                total_b += b
                if a != b:
                    bad_counts.append(f"{t} src={a} dst={b}")
            import re as _re2
            nums = [_re2.search(rf"(\d+) rows {k}", text)
                    for k in ("exclusive to table A", "exclusive to table B",
                              "updated")]
            ok = (p.returncode == 0 and all(nums)
                  and all(n.group(1) == "0" for n in nums))
            status = "ok" if ok else "diff" if p.returncode in (0, 1) \
                else "error"
            if stream:
                stream(f"{t}: {status}")
            res.append(Result("data", f"{db}.{t}", status,
                              f"{rows}reladiff cross-engine"
                              + ("" if ok else f": {text.strip()[-160:]}")))
        if with_counts:
            if bad_counts:
                cres = Result("counts", db, "diff",
                              "; ".join(bad_counts[:10]))
            else:
                cres = Result("counts", db, "ok",
                              f"rows {total_a:,}=={total_b:,} across engines"
                              " (from the reladiff pass, no extra scan)")
            res.insert(0, cres)
        return res

    TYPE_FIX = [
        (r"\bAUTO_INCREMENT\b", "GENERATED BY DEFAULT AS IDENTITY"),
        (r"\bDATETIME\b", "TIMESTAMP"),
        (r"\bTINYINT\(1\)", "BOOLEAN"),
        (r"\bDOUBLE\b(?!\s+PRECISION)", "DOUBLE PRECISION"),
        (r"\bLONGTEXT\b|\bMEDIUMTEXT\b", "TEXT"),
        (r"\bLONGBLOB\b|\bMEDIUMBLOB\b|\bBLOB\b", "BYTEA"),
        (r"\bUNSIGNED\b", ""),
        (r"\)\s*ENGINE=[^;]*", ")"),
        (r"\bCHARACTER SET \w+", ""),
        (r"\bCOLLATE[= ]\w+", ""),
    ]

    def convert_ddl(self, db):
        import sqlglot
        out = []
        for t in self.my._tables("src", db):
            ddl = self.my._q("src", f"show create table `{db}`.`{t}`")[0][1]
            try:
                pg_sql = sqlglot.transpile(ddl, read="mysql",
                                           write="postgres")[0]
            except Exception:
                pg_sql = ddl
            for pat, rep in self.TYPE_FIX:
                pg_sql = re.sub(pat, rep, pg_sql, flags=re.I)
            out.append(pg_sql.rstrip(";") + ";")
        return out

    def setup_target_plan(self, db):
        plan = []
        if which("pgloader"):
            plan.append(f"pgloader mysql://user@{self.hop.source.host}/{db}"
                        f" postgresql://user@{self.hop.target.host}/{db}"
                        "  # schema+data+indexes in one shot")
        plan.append(f"migkit convert-schema <hop> --db {db}"
                    "   # sqlglot DDL conversion, review then --apply")
        plan.append(f"migkit move <hop> --db {db} --go"
                    "   # resumable chunked data copy")
        plan.append("cross-engine CDC needs debezium or a managed"
                    " migration service, see migkit advise")
        return plan

    def list_move_tables(self, db):
        return [("", t) for t in self.my._tables("src", db)]

    def move_table(self, db, sch, tbl, chunk, ck, log):
        t = tbl or sch
        key = f"{db}.{t}"
        st = ck.setdefault(key, {})
        if st.get("done"):
            log(f"{key}: done earlier, skip")
            return
        cols = self.my._cols(db, t)
        collist_my = ", ".join(f"`{c}`" for c in cols)
        collist_pg = ", ".join(f'"{c}"' for c in cols)
        pks = self.my._pk_cols(db, t)
        intpk = None
        if len(pks) == 1:
            r = self.my._q("src",
                           "select data_type from information_schema.columns"
                           " where table_schema=%s and table_name=%s"
                           " and column_name=%s", (db, t, pks[0]))
            if r and r[0][0] in ("tinyint", "smallint", "mediumint",
                                 "int", "bigint"):
                intpk = pks[0]

        def push(rows, pred_pg):
            buf = io.StringIO()
            w = csv.writer(buf)
            for row in rows:
                w.writerow(["" if v is None else
                            v.hex() if isinstance(v, (bytes, bytearray))
                            else v for v in row])
            tgt = self.hop.target
            env = tool_env({"PGPASSWORD": tgt.password})
            pre = f'delete from "{t}" where {pred_pg};' if pred_pg \
                else f'truncate "{t}";'
            p = subprocess.run(
                ["psql", "-h", tgt.host, "-p", str(tgt.port),
                 "-U", tgt.user, "-d", db, "-X", "-q",
                 "-v", "ON_ERROR_STOP=1", "-1", "-c", pre,
                 "-c", f"\\copy \"{t}\" ({collist_pg}) from stdin"
                       " (format csv, null '')"],
                input=buf.getvalue(), capture_output=True, text=True, env=env)
            if p.returncode:
                raise RuntimeError(p.stderr[-300:])

        if not intpk:
            log(f"{key}: no single int pk, single-shot copy")
            rows = self.my._q("src", f"select {collist_my} from `{db}`.`{t}`")
            push(rows, "")
            st["done"] = True
            ck.save()
            return
        mm = self.my._q("src", f"select coalesce(min(`{intpk}`), 0),"
                        f" coalesce(max(`{intpk}`), 0) from `{db}`.`{t}`")[0]
        lo, hi = int(mm[0]), int(mm[1])
        last = st.get("last", lo - 1)
        while last < hi:
            nxt = min(last + chunk, hi)
            rows = self.my._q("src",
                              f"select {collist_my} from `{db}`.`{t}`"
                              f" where `{intpk}` > %s and `{intpk}` <= %s",
                              (last, nxt))
            push(rows, f'"{intpk}" > {last} and "{intpk}" <= {nxt}')
            last = nxt
            st["last"] = last
            ck.save()
            log(f"{key}: up to {intpk}={last:,} of {hi:,}")
        st["done"] = True
        ck.save()

    def tail_apply(self, db, go, token_path, log):
        import json as _json
        try:
            from pymysqlreplication import BinLogStreamReader
            from pymysqlreplication.row_event import (DeleteRowsEvent,
                                                      UpdateRowsEvent,
                                                      WriteRowsEvent)
        except ImportError:
            raise SystemExit("pip install mysql-replication for hetero tail")
        s = self.hop.source
        try:
            self.my._q("src", "set global binlog_row_metadata = 'FULL'")
            self.my._q("src", "set global binlog_row_image = 'FULL'")
        except Exception:
            log("note: cannot set binlog_row_metadata=FULL"
                " (managed mysql: set it in the parameter group)")
        ck = {}
        if token_path.exists():
            ck = _json.loads(token_path.read_text())
            log(f"resuming from {ck.get('log_file')}:{ck.get('log_pos')}")
        else:
            pos = self.my._q("src", "show binary log status") or                 self.my._q("src", "show master status")
            if pos:
                ck = {"log_file": pos[0][0], "log_pos": int(pos[0][1])}
                log(f"starting from current position"
                    f" {ck['log_file']}:{ck['log_pos']}")
        stream = BinLogStreamReader(
            connection_settings={"host": s.host, "port": s.port,
                                 "user": s.user, "passwd": s.password},
            server_id=self.hop.options.get("server_id", 4379),
            blocking=True, resume_stream=True,
            log_file=ck.get("log_file"), log_pos=ck.get("log_pos"),
            only_schemas=[db],
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent])

        def esc(v):
            if v is None:
                return "null"
            if isinstance(v, (int, float)):
                return str(v)
            return "'" + str(v).replace("'", "''") + "'"

        n = 0
        log("tailing binlog, ctrl-c to stop"
            + ("" if go else " (count-only, add --go to apply)"))
        try:
            for ev in stream:
                t = ev.table
                pks = self.my._pk_cols(db, t)
                if not pks:
                    continue
                real = self.my._cols(db, t)

                def fix(vals):
                    if not any(k.startswith("UNKNOWN_COL") for k in vals):
                        return vals
                    return {real[int(k[11:])]: v for k, v in vals.items()}

                stmts = []
                for row in ev.rows:
                    if isinstance(ev, WriteRowsEvent):
                        vals = fix(row["values"])
                        cols = list(vals)
                        sets = ", ".join(f'"{c}" = excluded."{c}"'
                                         for c in cols if c not in pks)
                        stmts.append(
                            f'insert into "{t}" ('
                            + ", ".join(f'"{c}"' for c in cols)
                            + ") values ("
                            + ", ".join(esc(vals[c]) for c in cols)
                            + f') on conflict ({", ".join(chr(34)+p+chr(34) for p in pks)})'
                            + (f" do update set {sets}" if sets
                               else " do nothing"))
                    elif isinstance(ev, UpdateRowsEvent):
                        vals = fix(row["after_values"])
                        before = fix(row["before_values"])
                        cols = list(vals)
                        sets = ", ".join(f'"{c}" = {esc(vals[c])}'
                                         for c in cols if c not in pks)
                        cond = " and ".join(
                            f'"{p}" = {esc(before[p])}' for p in pks)
                        stmts.append(f'update "{t}" set {sets} where {cond}')
                    elif isinstance(ev, DeleteRowsEvent):
                        dv = fix(row["values"])
                        cond = " and ".join(
                            f'"{p}" = {esc(dv[p])}' for p in pks)
                        stmts.append(f'delete from "{t}" where {cond}')
                if go and stmts:
                    self.pg._psql("dst", db, ";\n".join(stmts))
                n += len(ev.rows)
                token_path.write_text(_json.dumps(
                    {"log_file": stream.log_file,
                     "log_pos": stream.log_pos}))
                log(f"{n} row events applied,"
                    f" at {stream.log_file}:{stream.log_pos}")
        except KeyboardInterrupt:
            log(f"stopped at {stream.log_file}:{stream.log_pos},"
                " rerun to resume")

    def watch_sample(self, db):
        import time
        a = sum(self.my._q("src", f"select count(*) from `{db}`.`{t}`")[0][0]
                for t in self.my._tables("src", db))
        try:
            b = sum(int(self.pg._psql("dst", db,
                                      f'select count(*) from "{t}"'))
                    for t in self.my._tables("src", db))
        except RuntimeError:
            b = 0
        return {"db": db, "ts": time.time(), "src_rows": a, "dst_rows": b}
