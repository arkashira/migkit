import csv
import io
import re
import subprocess

from ..util import run, tool_env, which
from .base import Engine, Result


class HeteroEngine(Engine):
    """A hop whose two sides are different database engines.

    One driver per side, built from the same registry every other hop uses,
    so the pair is configuration rather than code. What used to be here was a
    single hand-written mysql-to-postgres pipeline that refused every other
    pairing outright; the comparison below is now written once against the
    neutral contract in `migkit.engines.base` and the rendering in
    `migkit.canon`, which is what makes a second pair cost nothing.

    Verification happens **inside each server**. Both sides fold their rows
    into one number using the same function over the same canonical text, and
    only the two numbers meet - so the size of the table has nothing to do
    with the amount of data on the wire. A validator that fetches both sides'
    rows into a middle box to compare them pays for the table twice, and pays
    again every time it re-runs.

    Moving data, converting DDL and tailing changes are still the
    mysql-to-postgres paths they always were, and say so when asked for
    another pair. Comparing correctly for every pair first is deliberate:
    a mover nobody can verify is the thing this tool exists to argue against.
    """

    checks = ("counts", "data")
    counts_from_data = True
    # the in-process binlog tail below needs this driver; without it
    # `move --mode cdc` uses migkit's streaming pipeline instead
    tail_requires = "pymysqlreplication"

    def __init__(self, hop):
        super().__init__(hop)
        self.src_name = hop.options.get("source_engine", "mysql")
        self.dst_name = hop.options.get("target_engine", "postgres")
        for role, name in (("source_engine", self.src_name),
                           ("target_engine", self.dst_name)):
            if name == "hetero":
                raise SystemExit(f"{role} cannot be 'hetero'")
        from . import engine_named
        self.src_engine = engine_named(self.src_name, hop)
        self.dst_engine = engine_named(self.dst_name, hop)
        # the mover, the DDL conversion and the binlog tail below are still
        # written for this one pair; they are reached through these names and
        # refuse anything else rather than pretending
        self.my = self.src_engine if self.src_name == "mysql" else None
        self.pg = self.dst_engine if self.dst_name == "postgres" else None

    def _mysql_to_postgres_only(self, what):
        if self.my is None or self.pg is None:
            raise SystemExit(
                f"hetero {self.src_name}->{self.dst_name}: {what} is still"
                " written for mysql->postgres only. `check` works on this"
                " pair; this does not.")

    def databases(self):
        return self.src_engine.databases()

    # ---- comparing, for any pair ---------------------------------------

    @staticmethod
    def _leaf(identifier):
        """The table's own name, without whatever qualifies it.

        MySQL answers `orders`; PostgreSQL answers `public.orders`. Matching
        on the last component is what lets the two lists meet without either
        engine having to know the other exists.
        """
        return str(identifier).split(".")[-1]

    @staticmethod
    def match_tables(src_ids, dst_ids):
        """(pairs, src_only, dst_only, ambiguous) by unqualified name.

        A name that appears twice on one side - the same table in two schemas -
        is returned as ambiguous rather than resolved. Picking one would
        compare a table against a namesake and report the verdict as if it
        were about the one the operator meant.
        """
        def index(ids):
            out = {}
            for i in ids:
                out.setdefault(HeteroEngine._leaf(i), []).append(i)
            return out
        s, d = index(src_ids), index(dst_ids)
        ambiguous = sorted(
            [v[0] for v in s.values() if len(v) > 1]
            + [v[0] for v in d.values() if len(v) > 1])
        pairs, src_only, dst_only = [], [], []
        for leaf in sorted(set(s) | set(d)):
            sv, dv = s.get(leaf, []), d.get(leaf, [])
            if len(sv) > 1 or len(dv) > 1:
                continue
            if sv and dv:
                pairs.append((sv[0], dv[0]))
            elif sv:
                src_only.append(sv[0])
            else:
                dst_only.append(dv[0])
        return pairs, src_only, dst_only, ambiguous

    def _column_plan(self, src_table, dst_table, db):
        """What to hash on each side, and everything that is not hashable.

        Returns (src_cols, dst_cols, notes). The two column lists are the
        same names in the same order - **sorted by name**, not by the order
        each server happens to report, because the row text is positional and
        two servers agreeing on a set of columns says nothing about the order
        they list them in.
        """
        from .. import canon

        def declared(engine, side, table):
            got = {}
            for name, typ in engine.neutral_columns(side, db, table):
                got[name] = typ
            return got
        src_types = declared(self.src_engine, "src", src_table)
        dst_types = declared(self.dst_engine, "dst", dst_table)
        notes = []
        both = sorted(set(src_types) & set(dst_types))
        only_src = sorted(set(src_types) - set(dst_types))
        only_dst = sorted(set(dst_types) - set(src_types))
        if only_src:
            notes.append(f"columns only on the source, not compared:"
                         f" {', '.join(only_src)}")
        if only_dst:
            notes.append(f"columns only on the target, not compared:"
                         f" {', '.join(only_dst)}")
        src_cols, dst_cols = [], []
        for name in both:
            scls, swhy = canon.comparable(self.src_engine.CANON_ENGINE,
                                          src_types[name])
            dcls, dwhy = canon.comparable(self.dst_engine.CANON_ENGINE,
                                          dst_types[name])
            if not scls or not dcls:
                notes.append(f"{name}: {swhy or dwhy}")
                continue
            src_cols.append((name, scls))
            dst_cols.append((name, dcls))
        return src_cols, dst_cols, notes

    def _neutral_rows(self, db, table=None, stream=None):
        """One (scope, status, detail, src_rows, dst_rows) per table.

        The single computation behind both `check_counts` and `check_data`:
        the digest query returns the count alongside the checksum, so asking
        twice would be two answers taken at two different moments about the
        same question.
        """
        src_ids = self.src_engine.neutral_tables("src", db)
        dst_ids = self.dst_engine.neutral_tables("dst", db)
        pairs, src_only, dst_only, ambiguous = self.match_tables(src_ids,
                                                                 dst_ids)
        if table:
            pairs = [p for p in pairs if self._leaf(p[0]) == self._leaf(table)]
            src_only = [t for t in src_only
                        if self._leaf(t) == self._leaf(table)]
        rows = []
        for t in src_only:
            rows.append((f"{db}.{self._leaf(t)}", "diff",
                         f"{t} is on the source and not on the target",
                         None, None))
        for t in dst_only:
            rows.append((f"{db}.{self._leaf(t)}", "warn",
                         f"{t} is on the target and not on the source",
                         None, None))
        for t in ambiguous:
            rows.append((f"{db}.{self._leaf(t)}", "warn",
                         f"{self._leaf(t)} exists more than once on one side,"
                         " so migkit will not guess which one the other side"
                         " means - name the schema", None, None))
        for src_t, dst_t in pairs:
            scope = f"{db}.{self._leaf(src_t)}"
            try:
                src_cols, dst_cols, notes = self._column_plan(src_t, dst_t, db)
            except Exception as e:
                rows.append((scope, "error",
                             f"cannot read the columns: {str(e)[:120]}",
                             None, None))
                continue
            tail = ("; " + "; ".join(notes)) if notes else ""
            if not src_cols:
                rows.append((scope, "warn",
                             "no column on this table can be compared across"
                             " these two engines" + tail, None, None))
                continue
            try:
                a = self.src_engine.neutral_digest("src", db, src_t, src_cols)
                b = self.dst_engine.neutral_digest("dst", db, dst_t, dst_cols)
            except Exception as e:
                rows.append((scope, "error",
                             str(e).splitlines()[-1][:120], None, None))
                continue
            if stream:
                stream(f"{scope}: {'ok' if a == b else 'diff'}")
            if a == b:
                rows.append((scope, "ok",
                             f"rows {a[0]:,} and every compared column equal"
                             f" across {self.src_name}/{self.dst_name}"
                             f" (digest {a[1]}){self._where_folded()}{tail}",
                             a[0], b[0]))
            elif a[0] != b[0]:
                rows.append((scope, "diff",
                             f"rows src={a[0]:,} dst={b[0]:,}{tail}",
                             a[0], b[0]))
            else:
                rows.append((scope, "diff",
                             f"rows {a[0]:,} match but the contents do not:"
                             f" digest src={a[1]} dst={b[1]}{tail}",
                             a[0], b[0]))
        return rows

    def _neutral_compare(self, db, table=None, stream=None):
        return [Result("data", scope, status, detail)
                for scope, status, detail, _, _
                in self._neutral_rows(db, table, stream)]

    def _where_folded(self):
        """A clause naming any side that had to fold its rows on this machine.

        The digest is the same number either way; what is not the same is
        where the data went to produce it. An engine with no hashing operator
        - MongoDB has none at all - sends its documents here, and a report
        that read identically for both cases would be hiding the one fact an
        operator needs to size the run.
        """
        from .. import canon
        remote, local = [], []
        for name, engine in ((self.src_name, self.src_engine),
                             (self.dst_name, self.dst_engine)):
            if engine.CANON_ENGINE not in canon.IN_PROCESS:
                continue
            (remote if engine.OVER_NETWORK else local).append(name)
        parts = []
        if remote:
            parts.append(f"{', '.join(remote)} has no hashing operator of its"
                         " own, so its rows crossed the network to be folded"
                         " here")
        if local:
            parts.append(f"{', '.join(local)} was folded in this process,"
                         " which is where it runs anyway")
        return (" - " + "; ".join(parts)) if parts else ""

    def _can_move_neutrally(self):
        """Whether both engines implement the read/write contract.

        Answered from the classes, not by calling them: a capability probe
        that opens a connection turns "can this pair move" into "is the
        database up right now", and the two questions have different answers
        and different failure messages.
        """
        return all(
            type(engine).neutral_read is not Engine.neutral_read
            and type(engine).neutral_write is not Engine.neutral_write
            for engine in (self.src_engine, self.dst_engine))

    def _move_columns(self, src_table, dst_table, db):
        """Column names to carry, and what is being left behind.

        Matched by name and **sorted**, so the two sides line up by name
        rather than by the order each server happens to list them in - the
        same rule the comparison uses, and for the same reason.

        Unlike the comparison, a column whose type has no canonical rendering
        is still moved: the rendering exists so two engines can be compared,
        and a value that cannot be compared can still be carried. What is not
        carried is a column the target does not have, and that is named.
        """
        src_types = dict(self.src_engine.neutral_columns("src", db,
                                                         src_table))
        try:
            dst_types = dict(self.dst_engine.neutral_columns("dst", db,
                                                             dst_table))
        except Exception:
            dst_types = {}
        notes = []
        if not dst_types and self.dst_engine.CREATES_ON_WRITE:
            # nothing to intersect with: the collection is made by the write
            # itself, so what it will hold is whatever the source has
            both = sorted(src_types)
            dst_types = dict(src_types)
        else:
            both = sorted(set(src_types) & set(dst_types))
            missing = sorted(set(src_types) - set(dst_types))
            if missing:
                notes.append("not carried, the target has no such column: "
                             + ", ".join(missing))
        from .. import canon
        src_cols, dst_cols = [], []
        for name in both:
            src_cols.append((name, canon.type_class(
                self.src_engine.CANON_ENGINE, src_types[name])))
            dst_cols.append((name, canon.type_class(
                self.dst_engine.CANON_ENGINE, dst_types[name])))
        return src_cols, dst_cols, notes

    def _neutral_move(self, db, sch, tbl, chunk, ck, log):
        """Read from one engine, write to the other, for any pair.

        Resumable through the checkpoint the caller already keeps, by the
        target table's own key. A table with no key is read in one pass and
        the log says so - restarting that one starts it over, which is the
        honest consequence of there being nothing to resume from.
        """
        src_t = f"{sch}.{tbl}" if sch else tbl
        leaf = self._leaf(src_t)
        pairs, _, _, _ = self.match_tables(
            self.src_engine.neutral_tables("src", db),
            self.dst_engine.neutral_tables("dst", db))
        match = [d for s_, d in pairs if self._leaf(s_) == leaf]
        if match:
            dst_t = match[0]
        elif self.dst_engine.CREATES_ON_WRITE:
            dst_t = leaf
            log(f"{leaf}: not on the target yet;"
                f" {self.dst_name} creates it on the first write")
        else:
            raise SystemExit(f"{leaf} is not on the target - create it first,"
                             " migkit does not invent a table it cannot see")
        src_cols, dst_cols, notes = self._move_columns(src_t, dst_t, db)
        for n in notes:
            log(f"{leaf}: {n}")
        key = f"{db}.{leaf}"
        st = ck.setdefault(key, {})
        if st.get("done"):
            log(f"{key}: done earlier, skip")
            return
        after = tuple(st["last"]) if st.get("last") is not None else None
        moved = int(st.get("moved", 0))
        absent = 0
        while True:
            rows, last = self.src_engine.neutral_read(
                "src", db, src_t, src_cols, after, chunk)
            if not rows:
                break
            rows, flattened = self._flatten_absent(rows)
            absent += flattened
            self.dst_engine.neutral_write("dst", db, dst_t, dst_cols, rows)
            moved += len(rows)
            st["moved"] = moved
            if last is None:
                log(f"{key}: {moved:,} rows in one pass (no key to resume"
                    " from, so a restart starts over)")
                break
            after = last
            st["last"] = list(last)
            ck.save()
            log(f"{key}: {moved:,} rows")
        if absent:
            log(f"{key}: {absent:,} values were not there on the source and"
                f" landed as NULL - {self.dst_name} has no way to store"
                " \"this field is not here\" apart from \"this field is"
                " null\", so the distinction ends at this hop")
        st["done"] = True
        ck.save()

    def _flatten_absent(self, rows):
        """Turn `canon.ABSENT` into None when the target cannot hold it.

        Counted rather than quietly converted. MongoDB keeps a missing field
        and a null field apart; a SQL column has one state for both, so the
        move is lossy in a direction nobody notices unless the number is
        printed. When the target *can* express it, nothing is touched.
        """
        from .. import canon
        if self.dst_engine.EXPRESSES_ABSENT:
            return rows, 0
        n = 0
        out = []
        for row in rows:
            if any(v is canon.ABSENT for v in row):
                n += sum(1 for v in row if v is canon.ABSENT)
                row = [None if v is canon.ABSENT else v for v in row]
            out.append(row)
        return out, n

    def _can_compare_neutrally(self):
        return bool(self.src_engine.CANON_ENGINE
                    and self.dst_engine.CANON_ENGINE)

    def _url(self, side, db):
        from urllib.parse import quote
        ep = self.hop.source if side == "src" else self.hop.target
        proto = "mysql" if side == "src" else "postgresql"
        return (f"{proto}://{ep.user}:{quote(ep.password, safe='')}"
                f"@{ep.host}:{ep.port}/{db}")

    def check_counts(self, db):
        if not self._can_compare_neutrally():
            return [Result("counts", db, "error",
                           f"{self.src_name}->{self.dst_name}: one of these"
                           " engines has no canonical rendering yet, so"
                           " migkit will not claim the two sides agree")]
        rows = self._neutral_rows(db)
        if not rows:
            return [Result("counts", db, "warn",
                           "no table on either side, so nothing was compared"
                           " - unknown, not clean")]
        bad = [(scope, status, detail) for scope, status, detail, a, b in rows
               if status != "ok" or a != b]
        if bad:
            return [Result("counts", scope, status, detail)
                    for scope, status, detail in bad[:10]]
        total = sum(a for _, _, _, a, _ in rows if a is not None)
        return [Result("counts", db, "ok",
                       f"rows {total:,}=={total:,} across"
                       f" {self.src_name}/{self.dst_name},"
                       f" {len(rows)} tables")]

    def check_data(self, db, table=None, stream=None, with_counts=False):
        """The digest comparison for any pair, with reladiff for the pair it
        was written for.

        reladiff names the rows that differ, which the digest cannot - it
        answers whether, not which. So it stays where it applies, and the
        digest is what makes every other pairing answerable at all.
        """
        if self._can_compare_neutrally() and not (self.my and self.pg):
            return self._neutral_compare(db, table, stream)
        if not which("reladiff"):
            if self._can_compare_neutrally():
                return self._neutral_compare(db, table, stream)
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
        self._mysql_to_postgres_only("converting DDL")
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
        self._mysql_to_postgres_only("the target setup plan")
        plan = []
        if which("pgloader"):
            plan.append(f"pgloader mysql://user@{self.hop.source.host}/{db}"
                        f" postgresql://user@{self.hop.target.host}/{db}"
                        "  # schema+data+indexes in one shot")
        plan.append(f"migkit convert-schema <hop> --db {db}"
                    "   # sqlglot DDL conversion, review then --apply")
        plan.append(f"migkit move <hop> --db {db} --go"
                    "   # resumable chunked data copy")
        plan.append("cross-engine CDC: migkit move <hop> --mode cdc --go"
                    "   # migkit stands up the streaming pipeline")
        return plan

    def list_move_tables(self, db):
        if not (self.my and self.pg):
            if not self._can_move_neutrally():
                self._mysql_to_postgres_only("listing tables to move")
            pairs, _, _, _ = self.match_tables(
                self.src_engine.neutral_tables("src", db),
                self.dst_engine.neutral_tables("dst", db))
            out = []
            for src_t, _ in pairs:
                sch, _, tbl = src_t.rpartition(".")
                out.append((sch, tbl))
            return out
        return [("", t) for t in self.my._tables("src", db)]

    def move_table(self, db, sch, tbl, chunk, ck, log):
        if not (self.my and self.pg):
            if not self._can_move_neutrally():
                self._mysql_to_postgres_only("moving a table")
            return self._neutral_move(db, sch, tbl, chunk, ck, log)
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
        self._mysql_to_postgres_only("tailing changes")
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
