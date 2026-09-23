import json
import os
import re
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ..config import REPORTS
from ..util import run, tool_env, which
from .base import Engine, RepairAction, Result

PGDC_ROOT = REPORTS / "pgdc"

INVENTORY_SQL = """
with ns as (
  select oid, nspname from pg_namespace
  where nspname not in ('pg_catalog','information_schema')
    and nspname not like 'pg\\_%'
    and nspname not like '\\_\\_%'
)
select case c.relkind when 'v' then 'view' when 'm' then 'matview'
    when 'S' then 'sequence'
    when 'i' then case when i.indisvalid then 'index' else 'invalid-index' end
    when 'I' then case when i.indisvalid then 'index' else 'invalid-index' end
    else 'table' end
  ||'|'||ns.nspname||'.'||c.relname
from pg_class c
join ns on ns.oid = c.relnamespace
left join pg_index i on i.indexrelid = c.oid
where c.relkind in ('r','p','v','m','S','i','I')
  and c.relname not like 'migkit\\_%'
union all
select case p.prokind when 'p' then 'procedure' else 'function' end
  ||'|'||ns.nspname||'.'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||')'
from pg_proc p join ns on ns.oid = p.pronamespace
where p.prokind in ('f','p')
union all
select 'trigger|'||ns.nspname||'.'||c.relname||'.'||t.tgname
from pg_trigger t
join pg_class c on c.oid = t.tgrelid
join ns on ns.oid = c.relnamespace
where not t.tgisinternal
union all
select case con.contype when 'p' then 'pk' when 'f' then 'fk'
    when 'u' then 'unique' else 'check' end
  ||'|'||ns.nspname||'.'||rel.relname||'.'||con.conname
from pg_constraint con
join pg_class rel on rel.oid = con.conrelid
join ns on ns.oid = con.connamespace
where con.contype in ('p','f','u','c')
union all
select 'extension|'||extname from pg_extension
"""


def _seq_buffer():
    """Head-room to add above the highest known id, from SEQUENCE_BUFFER.

    Zero (the default) reseeds to exactly the highest id in use, which is
    correct when the source is quiet. Anything above zero trades a gap in the
    id space for tolerance of rows that arrive a moment later.
    """
    try:
        n = int(os.environ.get("SEQUENCE_BUFFER", "0") or 0)
    except ValueError:
        return 0
    return max(0, n)


class PostgresEngine(Engine):
    ENGINE_FAMILY = "postgres"
    checks = ("schema", "counts", "autoinc", "data")
    counts_from_data = True

    USER_TABLES = ("select n.nspname||'.'||c.relname from pg_class c"
                   " join pg_namespace n on n.oid = c.relnamespace"
                   " where c.relkind = 'r'"
                   " and n.nspname not in ('pg_catalog','information_schema')"
                   " and n.nspname not like 'pg\\_%'"
                   " and n.nspname not like '\\_\\_%'"
                   " and c.relname not like 'migkit\\_%' order by 1")

    def _d(self, side, db):
        """Physical db name for a side: source as given, target through the
        hop's db_map so a migration can land in a differently-named db
        (identity when unmapped). Maintenance db 'postgres' never maps."""
        if side == "dst" and db != "postgres":
            return self.hop.target_db(db)
        return db

    def _dsn(self, side, db):
        """dbname plus keepalive settings.

        libpq leaves the keepalive idle time at the OS default (two hours on
        macOS). A long query over a cross-cloud link that loses its tunnel
        would therefore hang for hours instead of failing. Probing sooner
        turns it into an error the retry can handle."""
        import os
        idle = os.environ.get("MIGKIT_KEEPALIVE_IDLE", "60")
        return (f"dbname={self._d(side, db)} keepalives=1"
                f" keepalives_idle={idle} keepalives_interval=10"
                f" keepalives_count=5")

    def _psql(self, side, db, sql):
        ep = self.hop.source if side == "src" else self.hop.target
        env = {"PGPASSWORD": ep.password, "PGCONNECT_TIMEOUT": "15",
               "PGOPTIONS": "-c TimeZone=UTC -c DateStyle=ISO -c statement_timeout=0"}
        p = run(["psql", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
                 "-d", self._dsn(side, db), "-X", "-At", "-q", "-v",
                 "ON_ERROR_STOP=1", "-c", sql],
                env=env)
        return p.stdout.rstrip("\n")

    CANON_ENGINE = "postgres"

    def neutral_tables(self, side, db):
        out = self._psql(side, self._d(side, db),
                         "select schemaname||'.'||tablename from pg_tables"
                         " where schemaname not in"
                         " ('pg_catalog','information_schema') order by 1")
        return [l for l in out.splitlines() if l]

    def neutral_columns(self, side, db, table):
        """[(name, declared type)] with the type's own numbers attached.

        `format_type` rather than `information_schema.data_type`: the latter
        answers `character varying` and keeps the 50 in a separate column, so
        a target built from it came out `varchar(1024)` - wider than the
        source, which loses a limit the application was relying on without
        losing a row to show for it.
        """
        sch, tbl = self._split(table)
        out = self._psql(side, self._d(side, db), f"""
            select a.attname||chr(31)
                   ||pg_catalog.format_type(a.atttypid, a.atttypmod)
              from pg_attribute a
              join pg_class c on c.oid = a.attrelid
              join pg_namespace n on n.oid = c.relnamespace
             where n.nspname = '{sch}' and c.relname = '{tbl}'
               and a.attnum > 0 and not a.attisdropped
             order by a.attnum""")
        return [tuple(l.split("\x1f", 1)) for l in out.splitlines() if l]

    def neutral_key(self, side, db, table):
        sch, tbl = self._split(table)
        out = self._psql(side, self._d(side, db), f"""
            select a.attname
              from pg_index i
              join pg_class c on c.oid = i.indrelid
              join pg_namespace n on n.oid = c.relnamespace
              join pg_attribute a on a.attrelid = c.oid
                                 and a.attnum = any(i.indkey)
             where i.indisprimary and n.nspname = '{sch}'
               and c.relname = '{tbl}'
             order by array_position(i.indkey, a.attnum)""")
        return [l for l in out.splitlines() if l]

    def _conn(self, side, db):
        import psycopg2
        ep = self.hop.source if side == "src" else self.hop.target
        return psycopg2.connect(host=ep.host, port=ep.port, user=ep.user,
                                password=ep.password, dbname=db,
                                connect_timeout=15)

    def neutral_read(self, side, db, table, columns, after=None, limit=1000):
        sch, tbl = self._split(table)
        names = [n for n, _ in columns]
        cols = ", ".join(f'"{n}"' for n in names)
        key = self.neutral_key(side, db, table)
        where, args = "", []
        if key and after is not None:
            places = ", ".join(["%s"] * len(key))
            keys = ", ".join(f'"{k}"' for k in key)
            where = f" where ({keys}) > ({places})"
            args = list(after)
        order = (" order by " + ", ".join(f'"{k}"' for k in key)) if key else ""
        cap = f" limit {int(limit)}" if key else ""
        with self._conn(side, self._d(side, db)) as conn:
            with conn.cursor() as cur:
                cur.execute(f'select {cols} from "{sch}"."{tbl}"'
                            f"{where}{order}{cap}", args)
                rows = [list(r) for r in cur.fetchall()]
        if not rows or not key:
            return (rows, None)
        idx = [names.index(k) for k in key if k in names]
        if len(idx) != len(key):
            # the key is not among the columns being moved, so there is
            # nothing to resume from - one pass, and say so by returning None
            return (rows, None)
        return (rows, tuple(rows[-1][i] for i in idx))

    def neutral_rows_by_key(self, side, db, table, columns, key, keys):
        if not key or not keys:
            return {}
        sch, tbl = self._split(table)
        sql, args = self._by_key_query(f'"{sch}"."{tbl}"', columns, key,
                                       list(keys), lambda n: f'"{n}"', "%s")
        with self._conn(side, self._d(side, db)) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, args)
                rows = [list(r) for r in cur.fetchall()]
        return self._by_key_map(columns, key, rows)

    def neutral_write(self, side, db, table, columns, rows):
        if not rows:
            return 0
        from psycopg2.extras import execute_values

        from .. import canon
        sch, tbl = self._split(table)
        names = [n for n, _ in columns]
        cols = ", ".join(f'"{n}"' for n in names)
        key = self.neutral_key(side, db, table)
        if key and all(k in names for k in key):
            sets = ", ".join(f'"{n}" = excluded."{n}"'
                             for n in names if n not in key)
            conflict = ", ".join(f'"{k}"' for k in key)
            tail = (f" on conflict ({conflict}) do update set {sets}"
                    if sets else f" on conflict ({conflict}) do nothing")
        else:
            tail = ""
        with self._conn(side, self._d(side, db)) as conn:
            with conn.cursor() as cur:
                execute_values(cur, f'insert into "{sch}"."{tbl}" ({cols})'
                                    f" values %s{tail}",
                               [[canon.sql_value(v) for v in r]
                                for r in rows])
            conn.commit()
        return len(rows)

    def neutral_create_sql(self, side, db, table, columns, key=()):
        from .. import canon
        sch, tbl = self._split(table)
        defs = [f'"{n}" {canon.ddl_type("postgres", c, w)}'
                for n, c, w in columns]
        if key:
            defs.append("primary key (" + ", ".join(f'"{k}"' for k in key)
                        + ")")
        return f'create table "{sch}"."{tbl}" (' + ", ".join(defs) + ")"

    def neutral_create(self, side, db, table, columns, key=()):
        sch, tbl = self._split(table)
        exists = self._psql(side, self._d(side, db),
                            "select count(*) from information_schema.tables"
                            f" where table_schema='{sch}'"
                            f" and table_name='{tbl}'").strip()
        if exists != "0":
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        ddl = self.neutral_create_sql(side, db, table, columns, key)
        self._psql(side, self._d(side, db), ddl)
        return ddl

    def _insert_override(self, side, db, table, names):
        """`GENERATED ALWAYS AS IDENTITY` refuses a value the client chose,
        and a repair has no choice but to choose one - the row it is fixing
        already has a key.

        Measured on PostgreSQL 16 before this existed: migkit's own repair
        statement against such a target answered `cannot insert a
        non-DEFAULT value into column "id"`, and the same statement with
        this clause succeeded.

        Only the `a` (always) form needs it; `d` (by default), which is what
        `serial` behaves like, takes the value as given. Emitting the clause
        unconditionally was measured to be harmless - PostgreSQL accepts it
        on a table with no identity column at all, and a `by default`
        column still stored the explicit value - but it is looked up
        instead, so the statement says what it means and a reader can tell
        which tables needed it. One catalog query per table, cached.

        **`move` is not affected**, which is what keeps this narrow: `COPY`
        into a generated-always column was measured to succeed with no
        clause at all.
        """
        return (" overriding system value"
                if self._write_rules(side, db, table)["always"] & set(names)
                else "")

    def _write_rules(self, side, db, table):
        """Which columns of this table the server insists on owning.

        Two kinds, and they are **not** interchangeable - measured:

            attidentity = 'a'   GENERATED ALWAYS AS IDENTITY
                                writable with OVERRIDING SYSTEM VALUE
            attgenerated <> ''  GENERATED ALWAYS AS (expr) STORED
                                not writable at all; OVERRIDING SYSTEM VALUE
                                was tried and answered the same error

        One catalog query per table, cached, because both answers come from
        the same row of `pg_attribute` and asking twice would be two round
        trips for one fact.
        """
        # the cache lives in __dict__, so its name must not be a method's:
        # `_write_rules` there shadowed `_write_rules` here and every call
        # answered `'dict' object is not callable`
        cache = self.__dict__.setdefault("_write_rules_cache", {})
        target = self._d(side, db)
        ident = (side, target, table)
        if ident not in cache:
            sch, tbl = self._split(table)
            ref = f'"{sch}"."{tbl}"'.replace("'", "''")
            out = self._psql(side, target,
                             "select a.attname||chr(9)||a.attidentity::text"
                             "||chr(9)||a.attgenerated::text"
                             " from pg_attribute a"
                             f" where a.attrelid = to_regclass('{ref}')"
                             " and a.attnum > 0 and not a.attisdropped")
            always, generated = set(), set()
            for line in out.splitlines():
                parts = line.split("\t")
                if len(parts) != 3:
                    continue
                name, identity, gen = parts
                if identity == "a":
                    always.add(name)
                if gen:
                    generated.add(name)
            cache[ident] = {"always": always, "generated": generated}
        return cache[ident]

    def _unwritable_columns(self, side, db, table):
        """`GENERATED ALWAYS AS (expr) STORED` - the server computes these
        and refuses anybody else's value, so a repair has to leave them out
        and let it. Measured: an insert omitting `total` stored `total=20`
        from `price * qty`, which is the right answer arrived at the only
        way the server allows."""
        return self._write_rules(side, db, table)["generated"]

    def _apply_upsert(self, side, db, table, key, values):
        from .. import canon
        table = self.local_table(table)
        sch, tbl = self._split(table)
        row = dict(key)
        row.update(values)
        names = [n for n in sorted(row)
                 if n not in self._unwritable_columns(side, db, table)]
        cols = ", ".join(f'"{n}"' for n in names)
        marks = ", ".join(["%s"] * len(names))
        sets = ", ".join(f'"{n}" = excluded."{n}"'
                         for n in names if n not in key)
        conflict = ", ".join(f'"{k}"' for k in sorted(key))
        tail = (f" on conflict ({conflict}) do update set {sets}" if sets
                else f" on conflict ({conflict}) do nothing")
        override = self._insert_override(side, db, table, names)
        with self._conn(side, self._d(side, db)) as conn:
            with conn.cursor() as cur:
                cur.execute(f'insert into "{sch}"."{tbl}" ({cols})'
                            f"{override} values ({marks}){tail}",
                            [canon.sql_value(row[n]) for n in names])
            conn.commit()

    def _apply_delete(self, side, db, table, key):
        from .. import canon
        table = self.local_table(table)
        sch, tbl = self._split(table)
        names = sorted(key)
        where = " and ".join(f'"{n}" = %s' for n in names)
        with self._conn(side, self._d(side, db)) as conn:
            with conn.cursor() as cur:
                cur.execute(f'delete from "{sch}"."{tbl}" where {where}',
                            [canon.sql_value(key[n]) for n in names])
            conn.commit()

    PLUGIN = "test_decoding"

    def slot_name(self):
        """The slot this hop reads from.

        Derived from the hop's name so two hops against one server do not
        consume each other's changes, and sanitised because a slot name takes
        only lower-case letters, digits and underscores.
        """
        import re
        base = re.sub(r"[^a-z0-9_]", "_", str(self.hop.name).lower())
        return f"migkit_{base}"[:63]

    def _slot_ready(self, side, db):
        """Make sure the slot exists and is one migkit can read. Returns its
        name.

        Created rather than assumed: a slot that does not exist yet has no
        changes in it, and a tail that silently started from "now" would skip
        everything written between the full load and the first call. The slot
        has to exist **before** the rows are copied, which is why this is
        callable on its own.
        """
        name = self.slot_name()
        target = self._d(side, db)
        got = self._psql(side, target,
                         "select plugin from pg_replication_slots"
                         f" where slot_name = '{name}'").strip()
        if not got:
            level = self._psql(side, target, "show wal_level").strip()
            if level != "logical":
                raise SystemExit(
                    f"wal_level is {level} on this server and a logical slot"
                    " needs 'logical'. Nothing migkit does client-side can"
                    " make the WAL carry row images it was not told to"
                    " carry.\n"
                    "    alter system set wal_level = 'logical';"
                    "   -- then restart\n"
                    "    rds.logical_replication = 1"
                    "                -- parameter group, then reboot")
            self._psql(side, target,
                       "select pg_create_logical_replication_slot"
                       f"('{name}', '{self.PLUGIN}')")
            return name
        if got != self.PLUGIN:
            raise SystemExit(
                f"the slot {name} was made with the {got} plugin and migkit"
                f" reads {self.PLUGIN}. Reading one plugin's output as"
                " another's does not fail, it mis-parses - drop the slot and"
                " let migkit make it, or point this hop at another name")
        return name

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Row changes out of a logical slot, as neutral records.

        **Peeked, not consumed.** `pg_logical_slot_get_changes` advances the
        slot as it reads, so a crash between reading and applying loses the
        changes with nothing left to replay. `peek` leaves them, and a restart
        re-reads what it had already applied - which the appliers are
        idempotent for. Duplicated work is visible; a hole is not.

        What consumes them is the token coming back: handing back the token
        from the previous call is how a caller says "everything up to here is
        applied", and only then does the slot move past it. So the slot is
        advanced by evidence of success rather than by the act of looking.
        """
        from .. import pgslot
        name = self._slot_ready(side, db)
        target = self._d(side, db)
        if token:
            # the caller applied everything up to this LSN, so the server may
            # stop keeping it
            self._psql(side, target,
                       f"select pg_replication_slot_advance('{name}',"
                       f" '{token}')")
        rows = self._psql(side, target,
                          "select lsn::text || chr(31) || data from"
                          f" pg_logical_slot_peek_changes('{name}', null,"
                          f" {int(limit)})")
        out, last = [], token
        keys = {}
        for line in rows.splitlines():
            lsn, _, data = line.partition("\x1f")
            parsed = pgslot.parse_line(data)
            last = lsn or last
            if parsed is None:
                continue
            table = parsed["table"]
            if table not in keys:
                keys[table] = self.neutral_key(side, db, table)
                if not keys[table]:
                    raise SystemExit(
                        f"no primary key on {table} - a change to a keyless"
                        " table cannot be addressed on the target, and"
                        " applying it by matching every column would hit"
                        " every duplicate.\n"
                        "    The change is still in the slot and every call"
                        " will stop here again, because peeking never throws"
                        " anything away. Give the table a key or a unique"
                        " REPLICA IDENTITY, or step past it with:\n"
                        f"    select pg_replication_slot_advance('{name}',"
                        " pg_current_wal_lsn());"
                        "  -- skips everything pending, including this")
            out.append(pgslot.change(parsed, keys[table]))
        return out, last

    def neutral_digest(self, side, db, table, columns):
        from .. import canon
        sch, tbl = self._split(table)
        row = canon.row_expr("postgres", columns)
        got = self._psql(side, self._d(side, db),
                         "select count(*)::text||chr(31)||"
                         f"{canon.digest_expr('postgres', row)}::text"
                         f' from "{sch}"."{tbl}"').strip()
        n, _, d = got.partition("\x1f")
        return (int(n), d)

    @staticmethod
    def _split(table):
        """(schema, table) from either form.

        `"t".partition(".")` answers `('t', '', '')`, so an unqualified name
        silently became schema `t` and table `""` - which PostgreSQL reports
        as `zero-length delimited identifier`. Change records arrive with
        bare table names because a binlog has no schemas, which is where this
        first showed up.
        """
        schema, sep, name = str(table).partition(".")
        return (schema, name) if sep else ("public", schema)

    def _brand_probes(self):
        """The version banner and the reported server_version, per side.

        Both, in one round trip, because they disagree on everything that is
        not PostgreSQL itself: measured, CockroachDB v23.2.5 answers
        `show server_version` with `13.0.0` while its banner says what it
        really is. The banner is the evidence; the number is what every other
        comparison in migkit is built on, which is the problem.
        """
        def one(side):
            try:
                got = self._psql(side, "postgres",
                                 "select version()||chr(31)"
                                 "||current_setting('server_version')")
            except Exception:
                return {}
            ver, _, sver = got.strip().partition("\x1f")
            return {"version": ver, "server_version": sver}
        return (one("src"), one("dst"))

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        out = self._psql("src", "postgres",
                         "select datname from pg_database where not datistemplate"
                         " and datname not in ('postgres','rdsadmin') order by 1")
        return [l for l in out.splitlines() if l and not self.hop.excluded(l)]

    def _report(self, db):
        """Where this engine's evidence goes: the same place as every other
        engine's.

        It used to be `reports/pgdc/<hop>/<db>`, from before there was an
        estate to be consistent with, while `hop.report_dir` - which the
        base's own `params.json` for this very hop uses - is
        `reports/<hop>/<db>`. One hop wrote into two trees, and the
        drilldown an operator was told to read was not where every other
        engine puts it. Both the writer and the reader here go through this
        one method, so they move together; a check run by an older migkit
        leaves its files in the old place, and re-running the check writes
        them where `sync` now looks.
        """
        return self.hop.report_dir(db)

    def _dump_schema_native(self, side, db):
        ep = self.hop.source if side == "src" else self.hop.target
        p = run(["pg_dump", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
                 "-d", self._d(side, db), "--schema-only", "--no-owner",
                 "--no-privileges", "--no-security-labels", "--no-tablespaces",
                 "--exclude-schema", self.hop.options.get("exclude_schema", "__*"),
                 "--exclude-table", "*.migkit_changelog*"],
                env={"PGPASSWORD": ep.password, "PGCONNECT_TIMEOUT": "15"})
        noise = self._noise()
        keep = []
        for l in p.stdout.splitlines():
            if (l.startswith(("--", "SET ", "\\restrict", "\\unrestrict",
                              "SELECT pg_catalog.set_config")) or not l.strip()):
                continue
            if any(f"EVENT TRIGGER {n}" in l or f"PUBLICATION {n}" in l
                   for n in noise):
                continue
            keep.append(l)
        pats = self._ignore_patterns()
        if pats:
            keep = [l for l in keep if not any(pt.search(l) for pt in pats)]
        return "\n".join(keep)

    def check_schema(self, db):
        import difflib
        src = self._dump_schema_native("src", db)
        dst = self._dump_schema_native("dst", db)
        d = self._report(db)
        d.mkdir(parents=True, exist_ok=True)
        (d / "schema-src.sql").write_text(src + "\n")
        (d / "schema-dst.sql").write_text(dst + "\n")
        changed = [l for l in difflib.unified_diff(
                       src.splitlines(), dst.splitlines(), "src", "dst",
                       lineterm="")
                   if l[:1] in "+-" and not l.startswith(("+++", "---"))]
        if changed:
            (d / "schema.diff").write_text("\n".join(changed) + "\n")
            status, line = "diff", f"{len(changed)} changed lines"
        else:
            (d / "schema.diff").unlink(missing_ok=True)
            status, line = "ok", "schema identical (native pg_dump diff)"
        res = [Result("schema", db, status, line, str(d / "schema.diff"),
                      "review diff, apply missing DDL from schema-src.sql")]
        res.append(self.check_structural_diff(db))
        res.append(self.check_objects(db))
        if which("liquibase") and self.hop.options.get("liquibase", True):
            lb = self.check_liquibase(db)
            if lb:
                res.append(lb)
        if which("atlas") and self.hop.options.get("atlas", True):
            at = self.check_atlas(db)
            if at:
                res.append(at)
        return self._atlas_authoritative(res)

    # Statements that remove something. The differ emits these happily; only
    # the operator can say whether dropping an object the source no longer has
    # is the intent or an accident, so they are counted and surfaced, never
    # silently included in a "just apply this" recommendation.
    _DESTRUCTIVE = re.compile(
        r"^\s*(drop\s|alter\s+table\s+.*\s+drop\s|truncate\s)", re.I | re.M)

    def check_structural_diff(self, db):
        """Object-by-object schema comparison, in-process.

        This runs as a library call rather than a subprocess on purpose. The
        previous differ was invoked as a program and its output read from
        stdout: when it stopped working on modern Python it produced no output
        and no error, so the check quietly vanished from every report instead
        of failing. An exception here is reported as an error, which is the
        behaviour we want from a thing whose job is to notice problems.
        """
        s, t = self.hop.source, self.hop.target
        surl = f"postgresql://{s.user}:{s.password}@{s.host}:{s.port}/{db}"
        turl = (f"postgresql://{t.user}:{t.password}"
                f"@{t.host}:{t.port}/{self._d('dst', db)}")
        d = self._report(db)
        d.mkdir(parents=True, exist_ok=True)
        try:
            import results as _results
            from results.dbdiff import Migration
        except Exception as e:
            return Result("schema", f"{db} (structural)", "error",
                          f"structural differ unavailable: {e}", "",
                          "reinstall migkit; this check must not be skipped")
        try:
            # from target to source: what the target needs in order to match
            sdb, tdb = _results.db(surl), _results.db(turl)
            m = Migration(tdb, sdb)
            m.add_all_changes_ordered(privileges=True)
            sql = m.sql
            meta = m.result_metadata(options={"privileges": True})
            # The same comparison in the opposite direction is the undo of
            # exactly these statements. It has to be taken here, from the same
            # two snapshots: after the fix is applied the target no longer
            # describes where it came from, and the undo is unrecoverable.
            #
            # The same two handles serve both directions - measured, and the
            # reason it matters is that `results` exposes no way to close a
            # connection and does not drop one on garbage collection, so a
            # second pair would double what this check leaves open against a
            # production source every time it runs.
            try:
                r = Migration(sdb, tdb)
                r.add_all_changes_ordered(privileges=True)
                reverse_sql = r.sql
            except Exception:
                reverse_sql = ""
        except Exception as e:
            return Result("schema", f"{db} (structural)", "error",
                          f"structural diff failed: {str(e).splitlines()[0][:160]}",
                          "", "check connectivity and permissions on both sides")
        (d / "structural-diff.json").write_text(json.dumps(meta, indent=1,
                                                           sort_keys=True))
        if not sql.strip():
            # Clear the previous run's evidence, the way the MySQL paths
            # already do. A left-over fix script is a repair for differences
            # that no longer exist, and a left-over revert is worse: an undo
            # for changes nobody made, sitting in the directory an operator
            # opens during a cutover.
            for stale in ("structural-fix.sql", "structural-fix.locks.txt",
                          "structural-fix.revert.sql"):
                (d / stale).unlink(missing_ok=True)
            return Result("schema", f"{db} (structural)", "ok",
                          f"{meta['totals']['added'] + meta['totals']['removed']}"
                          " object differences, none structural"
                          if any(meta["totals"].values()) else
                          "every object identical, both sides",
                          str(d / "structural-diff.json"))
        path = d / "structural-fix.sql"
        path.write_text(sql)
        drops = len(self._DESTRUCTIVE.findall(sql))
        # What it will lock, next to what it will change. The target of a
        # repair is often still serving an application, and handing over DDL
        # without saying what it blocks is how a verification becomes an
        # outage.
        from .. import locks as _locks
        from .. import revert as _revert
        lock_text, lock_counts = _locks.report(sql)
        (d / "structural-fix.locks.txt").write_text(lock_text)
        rev_path = d / "structural-fix.revert.sql"
        rev_body = _revert.script(sql, reverse_sql, "structural-fix.sql")
        if rev_body:
            rev_path.write_text(rev_body)
        else:
            rev_path.unlink(missing_ok=True)
        tot = meta["totals"]
        detail = (f"{tot['added']} to add, {tot['removed']} to remove,"
                  f" {tot['modified']} to change"
                  f" across {len(meta['object_counts'])} object types")
        if drops:
            detail += (f"; {drops} of the generated statements remove an"
                       " object - read those before applying")
        lock_line = _locks.summary(lock_counts)
        if lock_line:
            detail += f"; {lock_line}"
        rev_line = _revert.summary(sql, reverse_sql)
        detail += ("; " + rev_line if rev_line else
                   "; no undo could be generated - take a backup first")
        return Result("schema", f"{db} (structural)", "diff", detail,
                      str(path),
                      "statements are in dependency order; read"
                      " structural-fix.locks.txt for what each one blocks,"
                      " review the removals, then apply on the target."
                      " structural-fix.revert.sql undoes it")

    def check_atlas(self, db):
        from urllib.parse import quote
        s, t = self.hop.source, self.hop.target
        su = (f"postgres://{s.user}:{quote(s.password, safe='')}"
              f"@{s.host}:{s.port}/{db}?sslmode=prefer")
        tu = (f"postgres://{t.user}:{quote(t.password, safe='')}"
              f"@{t.host}:{t.port}/{self._d('dst', db)}?sslmode=prefer")
        try:
            p = run(["atlas", "schema", "diff", "--from", tu, "--to", su,
                     "--exclude", "__*",
                     "--exclude", "*.migkit_changelog"],
                    check=False, timeout=180)
        except Exception:
            return None
        if p.returncode != 0:
            return None
        text = p.stdout.strip()
        if not text or "Schemas are synced" in text:
            return Result("schema", f"{db} (atlas)", "ok", "atlas diff clean")
        out = self._report(db) / "atlas-fix.sql"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n")
        return Result("schema", f"{db} (atlas)", "diff",
                      f"atlas generated {len(text.splitlines())} lines of fix DDL",
                      str(out), "review then apply atlas-fix.sql on target")

    def check_liquibase(self, db):
        s, t = self.hop.source, self.hop.target
        out = self._report(db) / "liquibase-diff.txt"
        out.parent.mkdir(parents=True, exist_ok=True)
        try:
            p = run(["liquibase", "diff",
                     f"--url=jdbc:postgresql://{t.host}:{t.port}/"
                     f"{self._d('dst', db)}?sslmode=prefer",
                     f"--username={t.user}", f"--password={t.password}",
                     f"--referenceUrl=jdbc:postgresql://{s.host}:{s.port}/{db}"
                     f"?sslmode=prefer",
                     f"--referenceUsername={s.user}",
                     f"--referencePassword={s.password}"],
                    check=False, timeout=180)
        except Exception:
            return None
        if p.returncode != 0:
            return None
        out.write_text(p.stdout)
        noise = self._noise()
        bad = [l.strip() for l in p.stdout.splitlines()
               if (l.startswith("Missing") or l.startswith("Unexpected")
                   or l.startswith("Changed"))
               and not l.rstrip().endswith("NONE")
               and "__" not in l and "migkit_changelog" not in l
               and not any(n in l for n in noise)]
        if bad:
            return Result("schema", f"{db} (liquibase)", "diff",
                          "; ".join(bad[:6]), str(out),
                          "see liquibase-diff.txt for the full object list")
        return Result("schema", f"{db} (liquibase)", "ok",
                      "liquibase diff clean")

    def check_objects(self, db):
        sides = {}
        for side in ("src", "dst"):
            m = {}
            for line in self._psql(side, db, INVENTORY_SQL).splitlines():
                t, _, name = line.partition("|")
                m.setdefault(t, set()).add(name)
            sides[side] = m
        inv = {}
        for t in sorted(set(sides["src"]) | set(sides["dst"])):
            s = sides["src"].get(t, set())
            d = sides["dst"].get(t, set())
            inv[t] = {"src": len(s), "dst": len(d),
                      "missing": sorted(s - d)[:50],
                      "extra": sorted(d - s)[:50]}
        out = self.hop.report_dir(db) / "objects.json"
        out.write_text(json.dumps(inv, indent=1))
        bad = {t: v for t, v in inv.items()
               if t != "invalid-index" and (v["missing"] or v["extra"])}
        note = ""
        iv = inv.get("invalid-index")
        if iv and iv["src"]:
            note = (f"; note: {iv['src']} invalid index on source"
                    f" ({', '.join(iv['missing'][:3])}), drop and rebuild"
                    " there, pg_dump and most movers skip it")
        if bad:
            parts = []
            for t, v in bad.items():
                p = f"{t} {v['src']}/{v['dst']}"
                if v["missing"]:
                    p += " missing: " + ", ".join(v["missing"][:3])
                if v["extra"]:
                    p += " extra: " + ", ".join(v["extra"][:3])
                parts.append(p)
            return Result("schema", f"{db} objects", "diff",
                          "; ".join(parts) + note, str(out),
                          "create missing objects on target from schema-src.sql")
        total = sum(v["src"] for t, v in inv.items() if t != "invalid-index")
        return Result("schema", f"{db} objects", "ok",
                      f"{total} objects in {len(inv)} types,"
                      f" all present on target{note}")

    PARAM_CRITICAL = ("TimeZone", "client_encoding", "server_encoding",
                      "lc_collate", "lc_ctype", "lc_monetary", "lc_numeric",
                      "lc_time", "DateStyle", "IntervalStyle",
                      "standard_conforming_strings", "bytea_output",
                      "default_transaction_isolation", "extra_float_digits",
                      "wal_level", "integer_datetimes", "backslash_quote",
                      "check_function_bodies", "array_nulls", "search_path",
                      "default_text_search_config")

    def check_params(self, db):
        q = ("select name||chr(31)||coalesce(setting,'') from pg_settings"
             " order by name")

        def pull(side):
            return dict(l.split("\x1f", 1) for l in
                        self._psql(side, db, q).splitlines() if "\x1f" in l)

        return self._param_result(
            db, pull("src"), pull("dst"), self.PARAM_CRITICAL,
            "align the behavior-critical GUCs on the target parameter group"
            " before cutover")

    SEQ_Q = ("select schemaname||'.'||sequencename||'|'||coalesce(last_value,0)"
             " from pg_sequences where schemaname not like '\\_\\_%'"
             " and sequencename not like 'migkit\\_%' order by 1")

    # map every serial/identity sequence to the column it feeds, via the
    # dependency catalog, so we can prove nextval clears that column's max.
    SEQ_OWNED_Q = (
        "select sn.nspname||'.'||s.relname||'|'||ns.nspname||'|'||t.relname"
        "||'|'||a.attname"
        " from pg_class s"
        " join pg_namespace sn on sn.oid = s.relnamespace"
        " join pg_depend d on d.objid = s.oid"
        "  and d.classid = 'pg_class'::regclass"
        "  and d.refclassid = 'pg_class'::regclass and d.deptype in ('a','i')"
        " join pg_class t on t.oid = d.refobjid"
        " join pg_namespace ns on ns.oid = t.relnamespace"
        " join pg_attribute a on a.attrelid = d.refobjid"
        "  and a.attnum = d.refobjsubid"
        " where s.relkind = 'S' and sn.nspname not like '\\_\\_%'"
        " and s.relname not like 'migkit\\_%'")

    def _seq_owned(self, side, db):
        """seqkey (schema.sequence) -> (schema, table, column) it feeds."""
        owned = {}
        for l in self._psql(side, db, self.SEQ_OWNED_Q).splitlines():
            p = l.split("|")
            if len(p) == 4:
                owned[p[0]] = (p[1], p[2], p[3])
        return owned

    def _seq_col_max(self, side, db, owned):
        """seqkey -> max(owned column) on `side`, so we know the floor the
        sequence must clear to avoid a duplicate-key collision on insert."""
        if not owned:
            return {}
        parts = [f"select '{k}|'||coalesce(max(\"{c}\"),0)"
                 f' from "{s}"."{t}"' for k, (s, t, c) in owned.items()]
        out = {}
        for i in range(0, len(parts), 200):
            chunk = " union all ".join(parts[i:i + 200])
            for l in self._psql(side, db, chunk).splitlines():
                if "|" in l:
                    key, _, v = l.rpartition("|")
                    out[key] = int(v)
        return out

    def _seq_next(self, side, db, owned):
        """seqkey -> the value nextval() would return WITHOUT consuming it.
        A never-called sequence returns last_value itself; once called it
        returns last_value + increment. Getting this right is the difference
        between last_value==max (safe, nextval clears it) and a real
        collision, so we read is_called rather than assume."""
        if not owned:
            return {}
        inc = {}
        for l in self._psql(side, db,
                            "select schemaname||'.'||sequencename||'|'"
                            "||increment_by from pg_sequences").splitlines():
            if "|" in l:
                k, _, v = l.rpartition("|")
                inc[k] = int(v)
        parts = []
        for k in owned:
            sch, name = k.split(".", 1)
            parts.append(f"select '{k}'||e'\\t'||last_value||e'\\t'||is_called"
                         f' from "{sch}"."{name}"')
        out = {}
        for i in range(0, len(parts), 200):
            chunk = " union all ".join(parts[i:i + 200])
            for l in self._psql(side, db, chunk).splitlines():
                p = l.split("\t")
                if len(p) == 3:
                    last, called = int(p[1]), p[2] in ("t", "true")
                    out[p[0]] = last + inc.get(p[0], 1) if called else last
        return out

    def _noise(self):
        """Prefixes of objects the movers create for their own bookkeeping.

        A leg can carry leftovers from more than one mover - DTS writes dts_*,
        DMS writes awsdms_* - so the option is a comma-separated list and every
        caller matches against the whole tuple. Returns () when unset, which
        makes `startswith(())` false and every `any()` below empty.
        """
        return tuple(p.strip() for p in
                     self.hop.options.get("noise_prefix", "").split(",")
                     if p.strip())

    def _keep_tbl(self, db, t):
        """False if the 'schema.table' is excluded for this db, so it is
        neither verified nor repaired (protects target-owned tables).

        Tables the mover keeps for itself (noise_prefix) are excluded too: they
        exist on one side by nature and would count as a difference every time
        while holding no application data."""
        sch, _, tbl = t.partition(".")
        noise = self._noise()
        if noise and tbl.startswith(noise):
            return False
        return not self.hop.excluded(db, sch, tbl)

    def check_counts(self, db):
        st = [t for t in self._psql("src", db, self.USER_TABLES).splitlines()
              if t and self._keep_tbl(db, t)]
        dt = set(t for t in self._psql("dst", db, self.USER_TABLES).splitlines()
                 if t and self._keep_tbl(db, t))
        bad = [f"{t} missing on target" for t in st if t not in dt]
        bad += [f"{t} extra on target"
                for t in sorted(dt - set(st))]

        def cnt(side, t):
            sch, tbl = t.split(".", 1)
            return int(self._psql(side, db,
                                  f'select count(*) from "{sch}"."{tbl}"') or 0)

        common = [t for t in st if t in dt]
        total = 0
        with ThreadPoolExecutor(max_workers=self.hop.workers) as pool:
            futs = {t: (pool.submit(cnt, "src", t), pool.submit(cnt, "dst", t))
                    for t in common}
            for t in common:
                a, b = futs[t][0].result(), futs[t][1].result()
                total += a
                if a != b:
                    bad.append(f"{t} src={a} dst={b}")
        if bad:
            return [Result("counts", db, "diff", "; ".join(bad[:10]), "",
                           "missing rows show up in check data, fix there")]
        return [Result("counts", db, "ok",
                       f"{len(common)} tables, {total:,} rows both sides")]

    def check_autoinc(self, db):
        """Two verdicts per database. USABLE is the one that matters at
        cutover: every serial/identity sequence on the target must sit above
        its column's max, or the first insert duplicate-keys (the single most
        common migration outage). PARITY is the softer 1-to-1 check that the
        target continues from the same value the source stopped at."""
        src = dict(l.rsplit("|", 1) for l in
                   self._psql("src", db, self.SEQ_Q).splitlines() if l)
        dst = dict(l.rsplit("|", 1) for l in
                   self._psql("dst", db, self.SEQ_Q).splitlines() if l)
        # A mover's own bookkeeping tables carry sequences of their own -
        # awsdms_heartbeat_hb_key_seq lives on whichever side that mover writes
        # to and cannot exist on the other. Reporting those as MISSING buries
        # the sequences that do matter under noise nobody can act on.
        noise = self._noise()
        if noise:
            def app(d):
                return {n: v for n, v in d.items()
                        if not n.rsplit(".", 1)[-1].startswith(noise)}
            src, dst = app(src), app(dst)
        owned = self._seq_owned("dst", db)
        if noise:
            owned = {k: v for k, v in owned.items()
                     if not k.rsplit(".", 1)[-1].startswith(noise)}
        dmax = self._seq_col_max("dst", db, owned)
        dnext = self._seq_next("dst", db, owned)
        collide = []
        for k, (s, t, c) in sorted(owned.items()):
            nxt = dnext.get(k, 0)
            mx = dmax.get(k, 0)
            if mx > 0 and nxt <= mx:
                collide.append(f"{k}: nextval={nxt} <= max({t}.{c})={mx}")
        res = []
        if collide:
            res.append(Result("autoinc", f"{db} usable", "diff",
                              "sequences WILL collide on next insert: "
                              + "; ".join(collide[:8]), "",
                              f"migkit sync {self.hop.name} --db {db} --kind"
                              " sequences --apply  (fix BEFORE cutover)"))
        else:
            res.append(Result("autoinc", f"{db} usable", "ok",
                              f"{len(owned)} owned sequences all clear their"
                              " column max, no collision"
                              if owned else "no owned sequences"))
        parity = [f"{n} src={v} dst={dst.get(n, 'MISSING')}"
                  for n, v in sorted(src.items()) if dst.get(n) != v]
        if parity:
            res.append(Result("autoinc", f"{db} parity", "diff",
                              "; ".join(parity[:8]), "",
                              f"migkit sync {self.hop.name} --db {db} --kind"
                              " sequences --apply"))
        else:
            res.append(Result("autoinc", f"{db} parity", "ok",
                              f"{len(src)} sequences match source last_value"))
        return res

    HEALTH_SQL = (
        "select (select count(*) from pg_stat_activity where state='active')"
        "::float / greatest(current_setting('max_connections')::float, 1)"
        " || '|' || coalesce((select max(extract(epoch from replay_lag))"
        " from pg_stat_replication), -1)"
        " || '|' || coalesce(extract(epoch from"
        " now() - pg_last_xact_replay_timestamp()), -1)")

    def _health(self, side, db):
        """What this side says about its own load, for the throttle.

        Cheap on purpose - two catalog reads, no table access. Anything the
        provider hides comes back as None rather than as "fine": a managed
        database that will not show us its replication state must not be
        mistaken for one that is idle.
        """
        from ..throttle import Health
        try:
            raw = self._psql(side, db, self.HEALTH_SQL).strip()
            busy, send_lag, replay_lag = (float(x) for x in raw.split("|"))
        except Exception:
            return None
        lag = max(send_lag, replay_lag)
        return Health(busy_ratio=busy if busy >= 0 else None,
                      lag_seconds=lag if lag >= 0 else None)

    # A table worth checkpointing. Below this a single aggregate finishes in
    # seconds, and splitting it would cost more in round trips than a restart
    # would ever save.
    CHUNK_MIN_ROWS = 5_000_000

    BIG_TABLES_SQL = (
        "select n.nspname||'.'||c.relname, coalesce(s.n_live_tup, 0), a.attname"
        " from pg_class c"
        " join pg_namespace n on n.oid = c.relnamespace"
        " left join pg_stat_user_tables s on s.relid = c.oid"
        " join pg_index i on i.indrelid = c.oid and i.indisprimary"
        " join pg_attribute a on a.attrelid = c.oid"
        "   and a.attnum = i.indkey[0]"
        " where c.relkind = 'r' and i.indnatts = 1"
        "   and a.atttypid in ('int2'::regtype,'int4'::regtype,'int8'::regtype)"
        "   and n.nspname not in ('pg_catalog','information_schema')")

    def _chunkable(self, db):
        """table -> (estimated rows, single integer primary-key column).

        Restricted to one-column integer keys on purpose: range predicates on
        those are index-only and cheap to reason about. A composite or text key
        can still be checksummed, just in one pass.
        """
        out = {}
        try:
            for line in self._psql("src", db, self.BIG_TABLES_SQL).splitlines():
                if not line:
                    continue
                name, est, col = line.split("|")
                if int(est) >= self.CHUNK_MIN_ROWS:
                    out[name] = (int(est), col)
        except Exception:
            pass    # no estimates = no chunking, never a failure
        return out

    def _data_fast_native(self, db, stream=None, may_skip=True):
        """Per-table checksum on both sides in parallel: commutative
        sum-of-md5 as a Postgres parallel aggregate (no sort, no lock beyond
        a plain SELECT). Emits the same OK/DIFF/ERROR lines the slice-mode
        drilldown and counts merge consume."""
        tables = [t for t in
                  self._psql("src", db, self.USER_TABLES).splitlines()
                  if t and self._keep_tbl(db, t)]
        w = int(self.hop.options.get("checksum_workers", 8))

        # The key hash rides along in the same scan. The row is already being
        # read, and hashing a primary key is far cheaper than hashing a whole
        # row, so the extra aggregate costs close to nothing - and it is what
        # turns "this table differs" into "rows were modified" or "rows are
        # missing" without a second pass.
        keyexpr = {}

        def _agg(h, kh):
            s = (f"count(*)||'|'||coalesce(sum(('x'||substr({h},1,16))"
                 "::bit(64)::bigint::numeric), 0)")
            if kh:
                s += (f"||'|'||coalesce(sum(('x'||substr({kh},1,16))"
                      "::bit(64)::bigint::numeric), 0)")
            return s

        def csum(side, t):
            sch, tbl = t.split(".", 1)
            # named from the source and run on both sides, the same way
            # `_key_hash_expr` already is: identical columns in identical
            # order, and a column the target is missing fails loudly instead
            # of quietly hashing to something else
            h = self._row_hash_expr("src", db, t)
            if t not in keyexpr:
                keyexpr[t] = self._key_hash_expr("src", db, t)
            return self._psql(side, db,
                f"set max_parallel_workers_per_gather = {w};"
                f' select {_agg(h, keyexpr[t])} from "{sch}"."{tbl}" t')

        # A checksum is only a SELECT, which is why nothing used to stop this
        # loop from saturating a small instance that was serving traffic.
        from ..throttle import Throttle
        from .. import checkpoint as _cp
        gate = Throttle(self.hop.workers,
                        probe=lambda: self._health("src", db))
        big = self._chunkable(db)
        cp = _cp.Checkpoint(str(self.hop.report_dir(db) / "checkpoint.json"))
        # What was proved equal last time, and the marker it carried then.
        # `--consistent` never reaches this function, so the final proof
        # cannot skip anything by construction rather than by a flag.
        from .. import unchanged as _un
        proof = _un.Proof(self.hop.report_dir(db) / "proof.json")
        skipped = []
        # Read once: a source in recovery produces the target-ahead shapes all
        # by itself, and the verdict has to say so. See `migkit.standby`.
        from .. import standby as _sb
        src_note = _sb.note(self._in_recovery("src", db),
                            self._replay_lag("src", db))
        # How many rows one chunk should cover is not a number anyone can
        # supply usefully - it depends on the row width, the indexes and how
        # busy the server is. Measure it instead.
        rate = _cp.Rate()

        def csum_range(side, t, col, lo, hi):
            sch, tbl = t.split(".", 1)
            h = self._row_hash_expr("src", db, t)
            pred = _cp.where(f'"{col}"', lo, hi)
            return self._psql(side, db,
                f"set max_parallel_workers_per_gather = {w};"
                f" select count(*)||'|'||coalesce(sum(('x'||substr({h},1,16))"
                f'::bit(64)::bigint::numeric), 0) from "{sch}"."{tbl}" t'
                + (f" where {pred}" if pred else ""))

        def table_rows(side, t):
            """The table's own row count, for the moment a chunk differs and
            the range's count would otherwise be reported as the table's."""
            sch, tbl = t.split(".", 1)
            return self._psql(side, db,
                              f'select count(*) from "{sch}"."{tbl}"').strip()

        def both(fn, *args):
            """Run the source and target aggregates at the same time.

            They used to run one after the other, so every table and every
            range waited for the source scan before the target scan started -
            two full passes of wall-clock for work that has no ordering
            between the sides.

            This does not add load to the source: a unit still issues exactly
            one source query. The second thread is the target's, and the
            target is the machine nobody is serving from yet. The throttle
            therefore keeps meaning what it meant.

            **The speed-up is not measured.** On the sandbox both servers are
            containers on a two-CPU VM, so they share the cores that do the
            hashing and running them together came out at 0.96x - neutral,
            inside the noise. A real leg has the two servers on two machines,
            where the work genuinely overlaps, but that is reasoning and not a
            number, and there is no honest way to produce the number here. The
            change is kept because removing an artificial ordering between two
            independent queries is right regardless; if anyone measures it on
            separate hosts, put the figure in this docstring.
            """
            with ThreadPoolExecutor(max_workers=2) as two:
                fa = two.submit(fn, "src", *args)
                fb = two.submit(fn, "dst", *args)
                return fa.result(), fb.result()

        def one_chunked(t, col):
            """Same answer as one pass, but restartable.

            The checksum is a commutative sum over numeric, so the per-range
            sums add up to the whole-table value exactly. Completed ranges are
            persisted, and a rerun only pays for what it still owes.
            """
            sch, tbl = t.split(".", 1)
            try:
                bounds = self._psql("src", db,
                    f'select coalesce(min("{col}"),0)||chr(124)||'
                    f'coalesce(max("{col}"),0) from "{sch}"."{tbl}"').strip()
                lo, hi = (int(x) for x in bounds.split("|"))
            except Exception as e:
                return f"{t}: ERROR {str(e).splitlines()[-1][:80]}"
            chunk = cp.chunk_for(t, rate.chunk_rows())
            ranges = _cp.plan_ranges(lo, hi, chunk)
            expr = self._row_hash_expr("src", db, t)
            todo = cp.begin(t, expr, ranges)
            done_before = cp.resumed(t)
            for rlo, rhi in todo:
                with gate.unit():
                    started = time.monotonic()
                    try:
                        a, b = both(csum_range, t, col, rlo, rhi)
                    except RuntimeError as e:
                        # partials already recorded survive for the next run
                        return f"{t}: ERROR {str(e).splitlines()[-1][:80]}"
                    rate.observe(int(a.split("|")[0]),
                                 time.monotonic() - started)
                if a != b:
                    cp.clear(t)
                    pred = _cp.where(col, rlo, rhi) or "whole table"
                    # `a` and `b` are this range's `rows|checksum`, and the
                    # row counts ride out of here into the counts check -
                    # so the range's count would be reported as the table's.
                    # Measured on 10,000,000 rows split into five chunks:
                    # `counts postgres: DIFF public.bench_rows src=2000000
                    # dst=1999992` about a table holding ten million. The
                    # checksums stay range-scoped, which is what `where`
                    # says; the counts are the table's, which is what
                    # anybody reading `src=` means by it.
                    rows_a, rows_b = both(table_rows, t)
                    return (f"{t}: DIFF src={rows_a}|{a.split('|', 1)[1]}"
                            f" dst={rows_b}|{b.split('|', 1)[1]}"
                            f" where {pred}")
                cp.record(t, rlo, rhi, *a.split("|", 1))
            rows, total = cp.total(t)
            cp.clear(t)
            resumed = (f" resumed={done_before}/{len(ranges)}"
                       if done_before else "")
            return (f"{t}: OK rows={rows} checksum={total}"
                    f" chunks={len(ranges)} chunk_rows={chunk:,}{resumed}")

        def _kind(a, b):
            from ..verdict import difference_kind
            pa, pb = a.split("|"), b.split("|")
            k = difference_kind(pa[0], pa[2] if len(pa) > 2 else None,
                                pb[0], pb[2] if len(pb) > 2 else None)
            if not k:
                return ""
            note = _sb.caveat(k, src_note)
            return f" kind={k}" + (f" -- {note}" if note else "")

        def markers(t):
            """(src, dst) change markers, or (None, None) if either is
            untrustworthy - which never skips."""
            try:
                return (self._change_marker("src", db, t),
                        self._change_marker("dst", self._d("dst", db), t))
            except Exception:
                return (None, None)

        def one(t):
            src_m, dst_m = markers(t)
            if may_skip and _un.skippable(proof.get(t), src_m, dst_m):
                skipped.append(t)
                # deliberately not "OK": nothing was read this run, and a
                # line that looks like a fresh verdict would be a lie about
                # when the evidence was taken
                return (f"{t}: UNCHANGED since it was last proved equal"
                        f" (no rows read this run)")
            if t in big:
                line = one_chunked(t, big[t][1])
            else:
                with gate.unit():
                    try:
                        a, b = both(csum, t)
                    except RuntimeError as e:
                        return f"{t}: ERROR {str(e).splitlines()[-1][:80]}"
                if a == b:
                    line = (f"{t}: OK rows={a.split('|')[0]}"
                            f" checksum={a.split('|')[1]}")
                else:
                    line = f"{t}: DIFF src={a} dst={b}{_kind(a, b)}"
            if ": OK" in line:
                # the markers are the ones read *before* the scan, so they
                # can never be newer than the evidence they stand for
                proof.set(t, src_m, dst_m)
            else:
                # a marker against a table that does not match is evidence of
                # the wrong thing; keeping it would let the next run skip a
                # table already known to be wrong
                proof.drop(t)
            return line

        lines, rc = [], 0
        with ThreadPoolExecutor(max_workers=self.hop.workers) as pool:
            for line in pool.map(one, tables):
                lines.append(line)
                if stream:
                    stream(line)
                # say what a failure is rather than inferring it from the
                # absence of the word OK: an UNCHANGED line is neither, and
                # treating it as a failure made a fully quiet run exit 1
                if ": DIFF" in line or ": ERROR" in line:
                    rc = 1
        try:
            proof.save()
        except Exception:
            pass                      # a store we cannot write just means
                                      # the next run reads everything again
        if src_note:
            lines.append(f"# source read while {src_note}; a source behind"
                         f" its writer shows fewer rows than the target and"
                         f" reads as rows-extra")
        if skipped:
            lines.append(f"# {len(skipped)} of {len(tables)} tables unchanged"
                         f" since their last proof and not read this run;"
                         f" `check --consistent` reads every table")
        note = gate.line()
        if note:
            lines.append(f"# {note}")
            if stream:
                stream(f"# {note}")
        self._last_throttle = gate.summary()
        return rc, "\n".join(lines)

    @staticmethod
    def _parse_fast(out):
        import re as _re
        rows_src = rows_dst = n = 0
        bad = []
        for line in out.splitlines():
            m = _re.match(r"(\S+): OK rows=(\d+)", line)
            if m:
                n += 1
                rows_src += int(m.group(2))
                rows_dst += int(m.group(2))
                continue
            m = _re.match(r"(\S+): DIFF src=(\d+)\|\S+ dst=(\d+)\|\S+", line)
            if m:
                n += 1
                a, b = int(m.group(2)), int(m.group(3))
                rows_src += a
                rows_dst += b
                if a != b:
                    bad.append(f"{m.group(1)} src={a} dst={b}")
        return n, rows_src, rows_dst, bad

    #: Tables this role reads through a policy. Measured across all four
    #: role shapes rather than assumed from the docs: a superuser and a
    #: BYPASSRLS role always see everything, an owner sees everything until
    #: the table is `FORCE`d and then does not, and any other role is
    #: filtered either way. Checking only `rolsuper or rolbypassrls` - which
    #: is what the deep check used to do - calls every owner filtered and
    #: cries wolf on a healthy database.
    RLS_FILTERED = (
        "select n.nspname||'.'||c.relname"
        " from pg_class c join pg_namespace n on n.oid = c.relnamespace"
        " where c.relkind = 'r' and c.relrowsecurity"
        " and not (select bool_or(rolsuper or rolbypassrls) from pg_roles"
        "          where rolname = current_user)"
        " and (c.relforcerowsecurity"
        "      or not pg_has_role(current_user, c.relowner, 'USAGE'))"
        " and n.nspname not in ('pg_catalog','information_schema')"
        " and n.nspname not like 'pg\\_%'"
        " and n.nspname not like '\\_\\_%'"
        " order by 1")

    #: What `assess` asks before anything moves. Each of these is about
    #: the move that is about to happen rather than the one that did.
    PREFLIGHT = ("_capacity_gaps", "_temporal_meaning", "_time_zone_rules",
                 "_collation_versions", "_mojibake")

    LARGE_OBJECT_COUNT = "select count(*) from pg_largeobject_metadata"

    #: Every user column that could hold a large object reference. The type
    #: is the signal - `vacuumlo` uses the same one, and has the same blind
    #: spot: a reference parked in a `bigint` is invisible to both.
    OID_COLUMNS = (
        "select n.nspname||'.'||c.relname||chr(9)||a.attname"
        " from pg_attribute a"
        " join pg_class c on c.oid = a.attrelid"
        " join pg_namespace n on n.oid = c.relnamespace"
        " where a.atttypid = 'oid'::regtype and a.attnum > 0"
        " and not a.attisdropped and c.relkind = 'r'"
        " and n.nspname not in ('pg_catalog','information_schema')"
        " and n.nspname not like 'pg\\_%'"
        " and n.nspname not like '\\_\\_%'"
        " and c.relname not like 'migkit\\_%'"
        " order by 1")

    def _large_objects(self, db):
        """Compare the objects themselves, not only the integers that name
        them.

        A column is treated as a large object reference only when the
        **source** resolves it. Plenty of `oid` columns hold something else
        - a `regclass`, a type oid - and those resolve on neither side;
        measured, a column holding `'refs'::regclass::oid` resolved 0 rows
        while a real document column resolved 1, which is the whole
        difference between a finding and a false alarm.
        """
        dangling, columns = [], 0
        try:
            src_total = int(self._psql("src", db,
                                       self.LARGE_OBJECT_COUNT).strip() or 0)
            dst_total = int(self._psql("dst", self._d("dst", db),
                                       self.LARGE_OBJECT_COUNT).strip() or 0)
            for line in self._psql("src", db, self.OID_COLUMNS).splitlines():
                parts = line.split("\t")
                if len(parts) != 2:
                    continue
                table, col = parts
                sch, tbl = self._split(table)
                ref = (f'"{sch}"."{tbl}"', self._quote_ident(col))
                resolves = (f"select count(*) from {ref[0]} t"
                            f" where t.{ref[1]} is not null and exists"
                            " (select 1 from pg_largeobject_metadata m"
                            f" where m.oid = t.{ref[1]})")
                on_source = int(self._psql("src", db, resolves).strip() or 0)
                if not on_source:
                    continue          # not a large object column at all
                columns += 1
                broken = (f"select count(*) from {ref[0]} t"
                          f" where t.{ref[1]} is not null and not exists"
                          " (select 1 from pg_largeobject_metadata m"
                          f" where m.oid = t.{ref[1]})")
                try:
                    missing = int(self._psql("dst", self._d("dst", db),
                                             broken).strip() or 0)
                except Exception:
                    continue          # the table is not on the target; the
                                      # schema check owns that finding
                if missing:
                    dangling.append((f"{table}.{col}", missing, on_source))
        except Exception as e:
            return Result("deep", f"{db} large objects", "error",
                          "could not compare the large objects:"
                          f" {str(e).splitlines()[-1][:90]}")
        return self._large_object_result(
            db, src_total, dst_total, dangling, columns,
            "carry them explicitly - pg_dump skips large objects whenever"
            " -s, -n or -t is used, and -b puts them back")

    def _content_fingerprint(self, side, db, relation):
        """`count|checksum` for everything in one relation.

        The expression comes from the **source** and is applied to both
        sides, so a target whose columns sit in a different order still
        compares equal - see `_row_hash_expr`. A count alone would miss a
        copy that is the right size and the wrong contents, which is
        exactly the shape a stale materialized view and a half-restored
        extension table both have.
        """
        h = self._row_hash_expr("src", db, relation)
        return self._psql(side, self._d(side, db),
                          "select count(*)||'|'||coalesce(sum(('x'||substr("
                          f"{h},1,16))::bit(64)::bigint::numeric), 0)"
                          f" from {relation} t")

    #: Tables an extension declared as its own configuration data. PostGIS
    #: registers `spatial_ref_sys` this way, which is why a custom SRID is
    #: dumped at all - and why a target that already has the stock table
    #: silently keeps its own rows instead of the ones being restored.
    EXTENSION_DATA = (
        "select e.extname||chr(9)||n.nspname||'.'||c.relname"
        " from pg_extension e"
        " cross join lateral unnest(e.extconfig) as cfg(oid)"
        " join pg_class c on c.oid = cfg.oid"
        " join pg_namespace n on n.oid = c.relnamespace"
        " order by 1")

    def _extension_data(self, db):
        """Extensions do not only bring functions - some bring rows.

        The extension list and its versions were already compared. What was
        not is the data those extensions own: PostGIS keeps coordinate
        systems in `spatial_ref_sys`, and a custom SRID lives there beside
        the several thousand stock ones. `pg_upgrade` has been reported
        failing with `Cannot find SRID (4283) in spatial_ref_sys` for
        exactly this reason, and a restored row does not overwrite an
        existing one, so the target keeps its stock copy and the geometry
        that needed the custom entry stops working.
        """
        findings, checked = [], 0
        try:
            rows = [l.split("\t") for l in
                    self._psql("src", db, self.EXTENSION_DATA).splitlines()
                    if l]
            for ext, table in rows:
                checked += 1
                try:
                    a = self._content_fingerprint("src", db, table)
                    b = self._content_fingerprint("dst", db, table)
                except Exception:
                    findings.append((ext, table, "missing on target"))
                    continue
                if a != b:
                    findings.append((ext, table, f"src={a} dst={b}"))
        except Exception as e:
            return Result("deep", f"{db} extension data", "error",
                          "could not compare the data extensions own:"
                          f" {str(e).splitlines()[-1][:90]}")
        return self._extension_data_result(
            db, findings, checked,
            "copy the rows the extension owns yourself - a restore does not"
            " overwrite the ones the target already has, so the stock table"
            " wins and the custom entries are simply absent")

    def _filtered_tables(self, side, db):
        try:
            out = self._psql(side, self._d(side, db), self.RLS_FILTERED)
        except Exception:
            return None
        return [line for line in out.splitlines() if line]

    @staticmethod
    def _failed_tables(out):
        """The tables the fast pass could not read at all. One place,
        because `check_data` and the counts derived from the same output
        both have to know - and only one of them used to."""
        return [l.split(":")[0] for l in out.splitlines() if ": ERROR" in l]

    def _counts_from_fast(self, db, out):
        """Counts ride along with the checksum pass rather than scanning
        again, which means they can only speak for the tables that pass
        managed to read.

        Measured with a role holding a column-level grant, so `select *`
        was refused on the only table in the database: the checksum pass
        errored on it, nothing was left to count, and this reported `OK 0
        tables, rows 0==0`. A clean verdict computed over nothing is the
        same shape as a mover reporting success into an empty target, which
        `moved_nothing` already refuses - so it is refused here too.

        An empty database still reports ok, because there genuinely is
        nothing to count and saying otherwise would cry wolf. The two cases
        are told apart by whether any table exists on both sides.
        """
        n, rows_src, rows_dst, bad = self._parse_fast(out)
        st = set(self._psql("src", db, self.USER_TABLES).splitlines())
        dt = set(self._psql("dst", db, self.USER_TABLES).splitlines())
        bad += [f"{t} missing on target" for t in sorted(st - dt)]
        bad += [f"{t} extra on target" for t in sorted(dt - st)]
        if bad:
            return Result("counts", db, "diff", "; ".join(bad[:10]), "",
                          "missing rows show up in check data, fix there")
        failed = self._failed_tables(out)
        if failed:
            return Result(
                "counts", db, "error",
                f"counted {n} tables; {len(failed)} could not be read by the"
                f" pass these counts come from: {', '.join(failed[:5])}"
                + (" ..." if len(failed) > 5 else "")
                + " - so this is not a count of the database", "",
                "see the data check for the error itself; a column-level"
                " grant is one cause, and it reports the table rather than"
                " the column")
        shared = st & dt
        if not n and shared:
            return Result(
                "counts", db, "error",
                f"{len(shared)} tables exist on both sides and none of them"
                " was counted - the pass these counts come from returned"
                " nothing to add up", "",
                "run the data check: whatever stopped it stopped this too")
        if not shared:
            return Result("counts", db, "ok",
                          "no tables on both sides to count")
        return self._honest_about_filtering(
            Result("counts", db, "ok",
                   f"{n} tables, rows {rows_src:,}=={rows_dst:,}"
                   " (from the checksum pass, no extra scan)"), db)

    def _drilldown_native(self, db, table):
        """Find the differing pks of one table (whole-table for normal
        sizes, PK-index slices for big single-int-pk tables so pgsql_tmp
        never fills). Writes data-<table>.missing/.extra/.changed."""
        cols = self._pk_cols_of(db, table)
        if not cols:
            return None
        sch, tbl = table.split(".", 1)
        qt = f'"{sch}"."{tbl}"'
        pkexpr = self._pk_text_expr(cols)

        def fetch(side, where=""):
            out = {}
            h = self._row_hash_expr("src", db, f"{sch}.{tbl}")
            for l in self._psql(side, db,
                                f"select {pkexpr}||'|'||{h}"
                                f" from {qt} t {where}").splitlines():
                k, _, hsh = l.rpartition("|")
                out[k] = hsh
            return out

        n = int(self._psql("src", db, f"select count(*) from {qt}") or 0)
        intpk = self._int_pk(db, sch, tbl)
        missing, extra, changed = [], [], []
        if n > self.hop.slice and intpk:
            mm = self._psql("src", db,
                            f'select coalesce(min("{intpk}"),0)||\'|\'||'
                            f'coalesce(max("{intpk}"),0) from {qt}')
            lo, hi = (int(x) for x in mm.split("|"))
            step = max(1, (hi - lo) // max(1, n // self.hop.slice) + 1)
            for a in range(lo, hi + 1, step):
                w = f'where "{intpk}" >= {a} and "{intpk}" < {a + step}'
                s, dd = fetch("src", w), fetch("dst", w)
                missing += [k for k in s if k not in dd]
                extra += [k for k in dd if k not in s]
                changed += [k for k in s if k in dd and s[k] != dd[k]]
        else:
            s, dd = fetch("src"), fetch("dst")
            missing = [k for k in s if k not in dd]
            extra = [k for k in dd if k not in s]
            changed = [k for k in s if k in dd and s[k] != dd[k]]
        self._write_pk_files(db, table, sorted(missing), sorted(extra),
                             sorted(changed))
        return len(missing), len(extra), len(changed)

    def check_data(self, db, table=None, stream=None, with_counts=False,
                   consistent=False):
        if table:
            r = self._drilldown_native(db, table)
            status = "ok" if r and not any(r) else "diff" if r else "error"
            detail = (f"missing={r[0]} extra={r[1]} changed={r[2]}"
                      if r else "no primary key for row-level compare")
            return [Result("data", f"{db} {table}", status, detail,
                           str(self._report(db)),
                           f"migkit sync {self.hop.name} --db {db} --kind rows")]
        if consistent:
            rc, out = self._fast_consistent(db)
            if stream:
                for line in out.splitlines():
                    stream(line)
        else:
            # when the row counts are merged out of this pass, every table
            # has to be read or the count is short by whatever was skipped
            rc, out = self._data_fast_native(db, stream=stream,
                                             may_skip=not with_counts)
        ev = self.hop.report_dir(db) / "data-evidence.txt"
        ev.write_text(out + "\n")
        pre = [self._counts_from_fast(db, out)] if with_counts else []
        mode = "consistent snapshot, " if consistent else ""
        if rc == 0:
            import re as _re
            rows = sum(int(m) for m in _re.findall(r"rows=(\d+)", out))
            n = out.count(": OK")
            unchanged = out.count(": UNCHANGED")
            extra = (f", {unchanged} unchanged since their last proof and"
                     f" not read this run" if unchanged else "")
            return pre + [self._honest_about_filtering(
                Result("data", db, "ok",
                       f"{mode}{n} tables, {rows:,} rows,"
                       f" checksums equal both sides{extra}",
                       str(ev)), db)]
        bad = [l.split(":")[0] for l in out.splitlines() if ": DIFF" in l]
        err = self._failed_tables(out)
        for t in bad:
            self._drilldown_native(db, t)
        if bad:
            still, healed, how = self._resolve_inflight(db, bad, stream)
            if not still and not err:
                return pre + [Result("data", db, "ok",
                                     f"{mode}all diffs proven in-flight"
                                     " replication: " + "; ".join(how),
                                     str(ev))]
            bad = still
        detail = ""
        if bad:
            detail = f"tables differ: {', '.join(bad)} (pk-level files written)"
            fps = []
            for t in bad[:5]:
                cols = self._column_fingerprint(db, t)
                if cols:
                    fps.append(f"{t} -> {', '.join(cols[:6])}")
            if fps:
                detail += "; drift localized to columns: " + "; ".join(fps)
        if err:
            detail += f" errors: {', '.join(err)}"
        return pre + [Result("data", db, "diff" if bad else "error", detail,
                             str(self._report(db)),
                             f"migkit sync {self.hop.name} --db {db} --kind rows")]

    def _ignore_patterns(self):
        import re as _re
        from ..config import CONF
        pats = []
        for d in (Path(CONF).parent, PGDC_ROOT / "conf"):
            f = d / f"{self.hop.name}.schema-ignore"
            if f.exists():
                pats += [p for p in f.read_text().splitlines() if p.strip()]
        return [_re.compile(p) for p in pats]

    #: A single field cannot exceed 1 GB in PostgreSQL - bytea, text and
    #: json alike. Measured guidance from the field puts the practical
    #: ceiling lower still, around 500 MB without binary transfer.
    PG_FIELD_LIMIT = 1024 ** 3

    #: Types whose values can be stored out of line, which is what makes
    #: them behave like the LOBs a mover has a mode for.
    LOB_TYPES = ("bytea", "text", "json", "jsonb", "xml",
                 "character varying")

    def _lob_columns(self, db):
        """Every column whose type can hold a value bigger than a mover's
        LOB limit.

        No cleverness in the filter, and that is deliberate. The obvious
        shortcut - only look at tables whose TOAST relation holds data -
        was measured to have a hole in it: a 1,000,000-byte text value
        compressed to 11,452 bytes on the way in, so a filter reading
        stored sizes would have passed over the very value a limited LOB
        mode would truncate. And the shortcut buys nothing:
        `max(octet_length(col))` over two columns of a 2,000,000-row,
        531 MB table answered in 0.25 s.
        """
        rows = self._psql("src", db,
            "select n.nspname||'.'||c.relname||chr(9)||a.attname"
            "||chr(9)||t.typname"
            " from pg_class c"
            " join pg_namespace n on n.oid = c.relnamespace"
            " join pg_attribute a on a.attrelid = c.oid and a.attnum > 0"
            " join pg_type t on t.oid = a.atttypid"
            " where c.relkind = 'r' and not a.attisdropped"
            " and n.nspname not in ('pg_catalog','information_schema')"
            " and n.nspname not like 'pg\\_%'"
            f" and t.typname = any(array{list(self.TOAST_TYPES)}::name[])"
            " order by 1")
        out = []
        for line in rows.splitlines():
            parts = line.split("\t")
            if len(parts) == 3:
                out.append((parts[0], parts[1], parts[2]))
        return out

    #: the catalog spells these differently from `information_schema`
    TOAST_TYPES = ("bytea", "text", "json", "jsonb", "xml", "varchar")

    def _lob_sizes(self, db):
        """The biggest value per wide column, on each side.

        `octet_length` and not `pg_column_size`: the first is the size of
        the value, which is what a mover's limit is compared against, and
        the second is the size PostgreSQL stored after compressing it -
        measured, 1,000,000 against 11,452 for the same value.
        """
        findings = []
        for table, column, typ in self._lob_columns(db):
            sch, tbl = table.split(".", 1)
            col = f'"{column}"'
            if typ in ("json", "jsonb", "xml"):
                col += "::text"
            q = (f'select coalesce(max(octet_length({col})), 0)'
                 f' from "{sch}"."{tbl}"')
            try:
                src = int(self._psql("src", db, q).strip() or 0)
            except Exception:
                continue
            if not src:
                continue
            try:
                dst = int(self._psql("dst", self._d("dst", db), q).strip()
                          or 0)
            except Exception:
                dst = None
            findings.append((table, column, src, dst, self.PG_FIELD_LIMIT))
        return findings

    def _lob_check(self, db):
        try:
            findings = self._lob_sizes(db)
        except Exception as e:
            return Result("deep", f"{db} lobs", "error",
                          "could not size the wide columns:"
                          f" {str(e).splitlines()[-1][:90]}")
        return self._lob_result(
            db, findings, "1 GB PostgreSQL field limit",
            "set the mover's LOB size limit above the biggest value, or move"
            " those tables with a path that does not truncate -"
            " `migkit move --go` does not")

    def _scalar(self, side, db, sql):
        out = self._psql(side, self._d(side, db), sql).strip()
        return out.splitlines()[0].split("|") if out else None

    def _zone_fingerprints(self, side, db):
        """`at time zone` renders the wall clock that zone shows at each
        probe instant; the md5 of those readings is the zone's behaviour.

        Measured at 78 ms for all the zones this server carries, so there
        is nothing to optimise and nothing to sample."""
        probes = ", ".join(f"(timestamptz '{p}+00')" for p in self.TZ_PROBES)
        out = self._psql(side, self._d(side, db),
                         "select n.name||chr(9)||md5(string_agg("
                         "(p.ts at time zone n.name)::text, ',' order by"
                         " p.ts)) from pg_timezone_names n"
                         f" cross join (values {probes}) p(ts)"
                         " group by n.name order by 1")
        got = {}
        for line in out.splitlines():
            parts = line.split("\t")
            if len(parts) == 2:
                got[parts[0]] = parts[1]
        return got

    #: Unique indexes over at least one collatable column - the ones a
    #: change in sort order can break. Partial and expression indexes are
    #: left out and counted, because grouping by their columns is not the
    #: same question they answer.
    UNIQUE_TEXT_INDEXES = (
        "select n.nspname||'.'||t.relname||chr(9)||c.relname||chr(9)"
        "||(select string_agg(quote_ident(a.attname), ',' order by k.ord)"
        "   from unnest(i.indkey) with ordinality k(attnum, ord)"
        "   join pg_attribute a on a.attrelid = i.indrelid"
        "    and a.attnum = k.attnum)"
        "||chr(9)||(i.indpred is not null or i.indexprs is not null)::int"
        "::text"
        " from pg_index i"
        " join pg_class c on c.oid = i.indexrelid"
        " join pg_class t on t.oid = i.indrelid"
        " join pg_namespace n on n.oid = t.relnamespace"
        " where i.indisunique and i.indisvalid and t.relkind = 'r'"
        " and n.nspname not in ('pg_catalog','information_schema')"
        " and n.nspname not like 'pg\\_%'"
        " and n.nspname not like '\\_\\_%'"
        " and t.relname not like 'migkit\\_%'"
        " and exists (select 1 from unnest(i.indkey) kk"
        "   join pg_attribute aa on aa.attrelid = i.indrelid"
        "    and aa.attnum = kk"
        "   where aa.atttypid in ('text'::regtype, 'varchar'::regtype,"
        "                         'bpchar'::regtype))"
        " order by 1")

    #: The planner must not be allowed to answer this question by reading
    #: the index that is under suspicion. Measured: left to itself it picks
    #: an index-only scan and reports no duplicates at all.
    NO_INDEX_PATHS = ("set enable_indexscan = off;"
                      " set enable_bitmapscan = off;"
                      " set enable_indexonlyscan = off; ")

    def _duplicate_keys(self, db, reason, sides=("src", "dst")):
        """Group by the key with the index paths shut off, and see what the
        unique index has been letting through."""
        found, checked, skipped = [], 0, 0
        if not reason:
            # nothing has accused an index, so nothing is read: the cost of
            # this check is a sequential scan per unique index
            return self._duplicate_hunt_result(db, [], 0, 0, "", "")
        try:
            for side in sides:
                label = "source" if side == "src" else "target"
                name = db if side == "src" else self._d("dst", db)
                for line in self._psql(side, name,
                                       self.UNIQUE_TEXT_INDEXES).splitlines():
                    parts = line.split("\t")
                    if len(parts) != 4:
                        continue
                    table, index, cols, odd = parts
                    if odd == "1":
                        skipped += 1
                        continue
                    checked += 1
                    sch, tbl = table.split(".", 1)
                    out = self._psql(
                        side, name,
                        self.NO_INDEX_PATHS
                        + f"select row_to_json(s) from (select {cols},"
                          f" count(*) as migkit_n from"
                          f' "{sch.replace(chr(34), chr(34) * 2)}".'
                          f'"{tbl.replace(chr(34), chr(34) * 2)}"'
                          f" group by {cols} having count(*) > 1"
                          f" limit {self.DUPLICATE_CAP}) s")
                    groups = [l for l in out.splitlines() if l.strip()]
                    if groups:
                        found.append((label, table, index, cols, len(groups),
                                      groups[0]))
        except Exception as e:
            return Result("deep", f"{db} duplicate keys", "error",
                          "could not hunt for duplicate keys:"
                          f" {str(e).splitlines()[-1][:90]}")
        return self._duplicate_hunt_result(
            db, found, checked, skipped, reason,
            "these rows were accepted by a constraint that had stopped"
            " working - decide which copy survives, delete the rest, then"
            " REINDEX so the constraint means something again")

    #: Every text column on a real table, which is where text that was
    #: already broken before the move is hiding.
    TEXT_COLUMNS = (
        "select c.table_schema||'.'||c.table_name||chr(9)||c.column_name"
        " from information_schema.columns c"
        " join information_schema.tables t"
        " on t.table_schema = c.table_schema and t.table_name = c.table_name"
        " where t.table_type = 'BASE TABLE'"
        " and c.table_schema not in ('pg_catalog','information_schema')"
        " and c.table_schema not like 'pg\\_%'"
        " and c.table_schema not like '\\_\\_%'"
        " and c.table_name not like 'migkit\\_%'"
        " and c.data_type in ('text','character varying','character')"
        " order by 1")

    def _mojibake(self, db):
        """Sample the non-ASCII rows of every text column on the source.

        `octet_length <> char_length` is exactly "this value has a character
        outside ASCII", it is the same expression on MySQL, and it lets one
        scan per *table* stand in for one per column. The rows come back as
        JSON so a value containing a tab or a newline cannot be read as two
        values - the mistake this project has already made once.
        """
        findings, scanned = [], 0
        try:
            cols = {}
            for line in self._psql("src", db,
                                   self.TEXT_COLUMNS).splitlines():
                parts = line.split("\t")
                if len(parts) == 2:
                    cols.setdefault(parts[0], []).append(parts[1])
            for table, columns in sorted(cols.items()):
                sch, tbl = table.split(".", 1)
                quoted = ['"' + c.replace('"', '""') + '"' for c in columns]
                where = " or ".join(f"octet_length({q}) <> char_length({q})"
                                    for q in quoted)
                out = self._psql(
                    "src", db,
                    f'select row_to_json(s) from (select {", ".join(quoted)}'
                    f' from "{sch.replace(chr(34), chr(34) * 2)}".'
                    f'"{tbl.replace(chr(34), chr(34) * 2)}"'
                    f' where {where} limit {self.MOJIBAKE_SAMPLE}) s')
                rows = [[json.loads(line).get(c) for c in columns]
                        for line in out.splitlines() if line.strip()]
                found, seen = self._mojibake_tally(table, columns, rows)
                findings += found
                scanned += seen
        except Exception as e:
            return Result("deep", f"{db} mojibake", "error",
                          "could not sample the source's text columns:"
                          f" {str(e).splitlines()[-1][:90]}")
        return self._mojibake_result(
            db, findings, scanned,
            "convert the column once on the source before moving it, and"
            " re-check afterwards - a second pass over already-repaired text"
            " breaks it again")

    #: A collation nothing uses cannot have broken anything, and after a real
    #: glibc upgrade *every* locale the OS ships has drifted - 873 of them on
    #: the measured image. Restricting to the ones a user column or index
    #: actually references is the difference between a finding and a wall.
    #: Every trigger a user put on the target, with the table it sits on
    #: and whether it is enabled. Internal ones are excluded: a foreign key
    #: is implemented as a pair of constraint triggers, and reporting those
    #: would bury the ones somebody wrote.
    TARGET_TRIGGERS = (
        "select n.nspname||'.'||c.relname||'.'||t.tgname||chr(9)"
        "||n.nspname||'.'||c.relname||chr(9)||t.tgenabled::text"
        " from pg_trigger t"
        " join pg_class c on c.oid = t.tgrelid"
        " join pg_namespace n on n.oid = c.relnamespace"
        " where not t.tgisinternal"
        " and n.nspname not in ('pg_catalog','information_schema')"
        " order by 1")

    COLLATION_IN_USE = (
        " and (exists (select 1 from pg_attribute a"
        "   join pg_class t on t.oid = a.attrelid"
        "   join pg_namespace n on n.oid = t.relnamespace"
        "   where a.attcollation = c.oid and a.attnum > 0"
        "   and not a.attisdropped"
        "   and n.nspname not in ('pg_catalog','information_schema'))"
        " or exists (select 1 from pg_index i"
        "   join pg_class t on t.oid = i.indrelid"
        "   join pg_namespace n on n.oid = t.relnamespace"
        "   where c.oid = any (i.indcollation)"
        "   and n.nspname not in ('pg_catalog','information_schema')))")

    #: Collations in use whose recorded version no longer matches what the
    #: operating system provides. `is distinct from` rather than `<>` so a
    #: locale that has vanished entirely (actual version NULL) is a finding
    #: and not a row that quietly drops out of the result.
    COLLATION_DRIFT = (
        "select c.collname||chr(9)||c.collversion||chr(9)"
        "||coalesce(pg_collation_actual_version(c.oid), '')"
        " from pg_collation c"
        " where c.collversion is not null and c.collversion <> ''"
        " and c.collversion is distinct from"
        " pg_collation_actual_version(c.oid)"
        + COLLATION_IN_USE + " order by 1")

    COLLATION_COUNT = ("select count(*) from pg_collation c"
                       " where c.collversion is not null"
                       " and c.collversion <> ''" + COLLATION_IN_USE)

    #: The database's own default collation, which is the one every text
    #: column without an explicit COLLATE is sorted by. PostgreSQL 15 is
    #: where it started recording this per database.
    DB_COLLATION_DRIFT = (
        "select datname||chr(9)||coalesce(datcollversion, '')||chr(9)"
        "||coalesce(pg_database_collation_actual_version(oid), '')"
        " from pg_database where datname = current_database()")

    def _collation_versions(self, db):
        """Both sides, because this is a property of the machine underneath
        each one - and a migration is usually the moment the two machines
        stop being the same machine."""
        drifted, missing, checked = [], [], 0
        for side, name in (("src", db), ("dst", self._d("dst", db))):
            label = "source" if side == "src" else "target"
            try:
                rows = self._psql(side, name, self.COLLATION_DRIFT)
                checked += int(self._psql(side, name,
                                          self.COLLATION_COUNT).strip() or 0)
                if int(self._psql(side, name,
                                  "select current_setting("
                                  "'server_version_num')::int").strip()
                       ) >= 150000:
                    # the database default sorts every text column that has
                    # no explicit COLLATE, so it is the one that matters most
                    default = self._psql(side, name,
                                         self.DB_COLLATION_DRIFT).strip()
                    rows += "\n" + default
                    parts = default.split("\t")
                    # a C-locale database records no version and has none to
                    # drift from, so counting it would inflate the all-clear
                    if len(parts) == 3 and parts[1]:
                        checked += 1
            except Exception as e:
                return Result("deep", f"{db} collation versions", "error",
                              f"could not read the {label}'s collation"
                              f" versions: {str(e).splitlines()[-1][:90]}")
            for line in rows.splitlines():
                parts = line.split("\t")
                if len(parts) != 3:
                    continue
                coll, stored, actual = parts
                if stored and not actual:
                    missing.append((label, coll, stored))
                elif stored != actual:
                    drifted.append((label, coll, stored, actual))
        return self._collation_version_result(
            db, drifted, missing, checked,
            "rebuild first, then refresh: REINDEX every index on a text"
            " column and only then ALTER DATABASE ... REFRESH COLLATION"
            " VERSION - refreshing first silences the warning and leaves the"
            " indexes sorted wrong")

    #: Every index on a user table, with the two flags that decide whether it
    #: is usable and whether it still costs writes. `relkind` separates a
    #: plain index from a partitioned parent, which is allowed to be invalid.
    INDEX_FLAGS = (
        "select n.nspname||'.'||t.relname||chr(9)||c.relname"
        " ||chr(9)||c.relkind::text||chr(9)||i.indisvalid::int::text"
        " ||chr(9)||i.indisready::int::text"
        " from pg_index i"
        " join pg_class c on c.oid = i.indexrelid"
        " join pg_class t on t.oid = i.indrelid"
        " join pg_namespace n on n.oid = t.relnamespace"
        " where n.nspname not in ('pg_catalog','information_schema')"
        " and n.nspname not like 'pg\\_%'"
        " and n.nspname not like '\\_\\_%'"
        " and t.relname not like 'migkit\\_%'"
        " order by 1")

    def _invalid_indexes(self, db):
        """Both sides, because an invalid index is a reason not to migrate
        yet as much as it is a failed rebuild afterwards. On the source it
        reads as an index nobody uses; on the target it is the post-load
        `CREATE INDEX CONCURRENTLY` that died and said so once."""
        broken, partial, total = [], [], 0
        for side, name in (("src", db), ("dst", self._d("dst", db))):
            label = "source" if side == "src" else "target"
            try:
                out = self._psql(side, name, self.INDEX_FLAGS)
            except Exception as e:
                return Result("deep", f"{db} indexes", "error",
                              f"could not read the {label}'s indexes:"
                              f" {str(e).splitlines()[-1][:90]}")
            for line in out.splitlines():
                parts = line.split("\t")
                if len(parts) != 5:
                    continue
                table, index, relkind, valid, ready = parts
                if valid == "1":
                    total += 1
                elif relkind == "I":
                    partial.append((label, table, index))
                else:
                    broken.append((label, table, index, ready == "1"))
        return self._invalid_index_result(
            db, broken, partial, total,
            "drop each one and build it again - `DROP INDEX CONCURRENTLY"
            " <name>` then `CREATE INDEX CONCURRENTLY`, with"
            " maintenance_work_mem and max_parallel_maintenance_workers"
            " raised for the rebuild")

    def moved_nothing(self, db):
        """Which tables the source has rows in and the target does not."""
        try:
            src = set(self._psql("src", db, self.USER_TABLES).splitlines())
            dst = set(self._psql("dst", self._d("dst", db),
                                 self.USER_TABLES).splitlines())
        except Exception:
            return None
        empty = []
        for t in sorted(x for x in src & dst if x):
            sch, tbl = t.split(".", 1)
            q = f'select 1 from "{sch}"."{tbl}" limit 1'
            try:
                has_src = bool(self._psql("src", db, q).strip())
                has_dst = bool(self._psql("dst", self._d("dst", db),
                                          q).strip())
            except Exception:
                return None
            if has_src and not has_dst:
                empty.append(t)
        return empty

    def settle_target(self, db):
        """Analyze the target after a load, in stages.

        `--analyze-in-stages` does three passes of increasing accuracy, so
        the planner has usable statistics within seconds instead of waiting
        for the full pass - which is what PostgreSQL's own documentation
        recommends after a restore, and what autoanalyze will not do for a
        table nobody writes to afterwards.
        """
        ep = self.hop.target
        target = self._d("dst", db)
        if not which("vacuumdb"):
            self._psql("dst", target, "analyze")
            return f"analyzed {target} on the target"
        p = run(["vacuumdb", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
                 "-d", target, "--analyze-in-stages",
                 "-j", str(max(1, int(self.hop.workers)))],
                env={"PGPASSWORD": ep.password}, check=False, timeout=7200)
        if p.returncode:
            return (f"could not analyze {target}:"
                    f" {(p.stderr or '').splitlines()[-1][:90] if p.stderr else 'unknown'}")
        return f"analyzed {target} on the target (in stages)"

    @staticmethod
    def _crosscheck_verdicts(text):
        """`{table: same?}` from `pgcopydb compare data` output.

        Its table marks a difference with `!` in the second column, so the
        parse is the marker and not the checksums - two hex strings that
        happen to differ is what the marker already means, and reading
        them twice would be a second opinion about pgcopydb's own answer.
        """
        out = {}
        for line in text.splitlines():
            parts = [c.strip() for c in line.split("|")]
            if len(parts) < 4 or not parts[0] or "." not in parts[0]:
                continue
            if parts[0].startswith("Table Name") or set(parts[0]) <= set("- "):
                continue
            name = parts[0].split(".", 1)[1] if parts[0].count(".") > 1 \
                else parts[0]
            out[name] = parts[1] != "!"
        return out

    def _crosscheck_result(self, db, mine, theirs, ran):
        """Whether a second implementation reaches migkit's verdict.

        Measured on pgcopydb 0.18 against four pairs before this was
        written, and it agreed every time: identical data, `numeric` 1.0
        against 1.00 (both **differ** - equal by `=`, not equal as stored),
        the same columns declared in a different order (both **same** -
        each sorts them), and a table with no primary key and a missing row
        (both **differ**). That agreement is the point: a check nobody has
        ever audited is the one this project exists to argue against.

        A table only one of them looked at is not a disagreement. Saying so
        is what keeps this from crying wolf on every filtered hop.
        """
        if not ran:
            return Result("deep", f"{db} cross-check", "skip",
                          "pgcopydb is not available to give a second"
                          " opinion on this pair")
        shared = sorted(set(mine) & set(theirs))
        clash = [t for t in shared if mine[t] != theirs[t]]
        if clash:
            return Result(
                "deep", f"{db} cross-check", "diff",
                f"{len(clash)} tables where pgcopydb and migkit disagree:"
                + ", ".join(f" {t} (migkit says"
                            f" {'same' if mine[t] else 'differs'},"
                            f" pgcopydb says"
                            f" {'same' if theirs[t] else 'differs'})"
                            for t in clash[:3])
                + (" ..." if len(clash) > 3 else "")
                + " - one of the two verifiers is wrong about this table",
                "", "run `pgcopydb compare data` by hand on the pair and"
                    " read both row sets before trusting either verdict")
        if not shared:
            return Result("deep", f"{db} cross-check", "skip",
                          "no table was examined by both verifiers")
        return Result("deep", f"{db} cross-check", "ok",
                      f"{len(shared)} tables, pgcopydb reaches the same"
                      " verdict as migkit on every one")

    def _crosscheck(self, db):
        """Ask pgcopydb the same question and see if it agrees.

        Off unless `MIGKIT_CROSSCHECK` says otherwise: it reads both
        databases in full a second time, which is a real cost to impose on
        every run. An environment variable rather than a flag, the same way
        `MIGKIT_MOVER` is.
        """
        import os
        if os.environ.get("MIGKIT_CROSSCHECK", "").strip().lower() \
                not in ("1", "true", "yes", "on"):
            return None
        from ..movers import pgcopydb_available
        from ..util import run
        if not pgcopydb_available():
            return self._crosscheck_result(db, {}, {}, False)
        import tempfile
        from urllib.parse import quote
        s_, t_ = self.hop.source, self.hop.target
        src = (f"postgresql://{s_.user}:{quote(s_.password or '', safe='')}"
               f"@{s_.host}:{s_.port}/{db}")
        dst = (f"postgresql://{t_.user}:{quote(t_.password or '', safe='')}"
               f"@{t_.host}:{t_.port}/{self._d('dst', db)}")
        work = tempfile.mkdtemp(prefix="migkit-crosscheck-")
        try:
            p = run(["pgcopydb", "compare", "data", "--dir", work,
                     "--source", src, "--target", dst], check=False)
        except Exception as e:
            return Result("deep", f"{db} cross-check", "error",
                          "could not run pgcopydb compare:"
                          f" {str(e).splitlines()[-1][:80]}")
        theirs = self._crosscheck_verdicts(p.stdout + p.stderr)
        mine = self._mine_from_evidence(db)
        if not mine:
            return Result("deep", f"{db} cross-check", "skip",
                          "no data-evidence.txt to compare against - the"
                          " data check has to have run for there to be a"
                          " migkit verdict to second-guess")
        return self._crosscheck_result(db, mine, theirs, True)

    def _mine_from_evidence(self, db):
        """migkit's own per-table verdict, read from the file the data
        check already wrote rather than by running the pass again.

        Asking twice would be two answers taken at two different moments
        about the same question, which is the thing this check exists to
        catch rather than to commit.
        """
        out = {}
        path = self.hop.report_dir(db) / "data-evidence.txt"
        if not path.exists():
            return out
        for line in path.read_text().splitlines():
            name, _, rest = line.partition(":")
            if not rest.strip():
                continue
            out[name.strip()] = rest.strip().startswith("OK")
        return out

    def _planner_stats(self, db):
        """What the target's own catalog says it knows about its tables.

        `reltuples = -1` is PostgreSQL 14+ for "never counted"; before that
        it was 0, which is indistinguishable from an empty table - so the
        two timestamps are what decide it, and reltuples is only read for
        the row count the ratio needs.
        """
        rows = []
        try:
            out = self._psql("dst", self._d("dst", db),
                "select n.nspname||'.'||c.relname"
                " ||chr(9)||greatest(c.reltuples, 0)::bigint"
                " ||chr(9)||(s.last_analyze is not null"
                "            or s.last_autoanalyze is not null)::int"
                " ||chr(9)||coalesce(s.n_mod_since_analyze, 0)"
                # a boolean reloption is stored as whatever spelling was
                # written - `false`, `off`, `0` and `no` all occur - so the
                # server casts it rather than this code guessing the list
                " ||chr(9)||coalesce((select option_value from"
                "  pg_options_to_table(c.reloptions)"
                "  where option_name = 'autovacuum_enabled'),"
                "  'true')::boolean::int"
                " from pg_class c"
                " join pg_namespace n on n.oid = c.relnamespace"
                " left join pg_stat_user_tables s on s.relid = c.oid"
                " where c.relkind = 'r'"
                " and n.nspname not in ('pg_catalog','information_schema')"
                " and n.nspname not like 'pg\\_%'"
                " and n.nspname not like '\\_\\_%'"
                " and c.relname not like 'migkit\\_%'"
                " order by 1")
        except Exception as e:
            return Result("deep", f"{db} statistics", "error",
                          f"could not read the target's statistics:"
                          f" {str(e).splitlines()[-1][:90]}")
        # The global switch decides whether the per-table one matters at
        # all. Read once rather than per table, and read as its own
        # statement so a server that refuses `pg_settings` leaves the
        # answer unknown rather than wrong.
        try:
            autovacuum_on = self._psql(
                "dst", self._d("dst", db),
                "select setting from pg_settings where name = 'autovacuum'"
            ).strip() == "on"
        except Exception:
            autovacuum_on = False
        reachable = set()
        for line in out.splitlines():
            parts = line.split("\t")
            if len(parts) != 5:
                continue
            rows.append((parts[0], int(parts[1]), parts[2] == "1",
                         int(parts[3])))
            # A never-analyzed table holds its whole row count in
            # n_mod_since_analyze, which clears the threshold for anything
            # above roughly fifty rows - and a table smaller than that has
            # no plan worth worrying about either way.
            if autovacuum_on and parts[4] == "1":
                reachable.add(parts[0])
        if not rows:
            return Result("deep", f"{db} statistics", "skip",
                          "no user tables on the target to have statistics"
                          " for")
        return self._planner_stats_result(
            db, rows,
            "run `vacuumdb --analyze-in-stages` against the target (or"
            " `migkit move --go`, which now analyzes what it loads)",
            reachable)

    def check_deep(self, db):
        res = []
        rpt = self.hop.report_dir(db)

        # a table with no pk/unique is the quiet trap: DMS and GoldenGate drop
        # its UPDATE/DELETE during CDC and duplicate it on full+CDC, and it
        # can't be verified or repaired by key. Surface it before it bites.
        nopk = [l for l in self._psql("src", db,
                "select n.nspname||'.'||c.relname from pg_class c"
                " join pg_namespace n on n.oid = c.relnamespace"
                " where c.relkind = 'r'"
                " and n.nspname not in ('pg_catalog','information_schema')"
                " and n.nspname not like 'pg\\_%'"
                " and n.nspname not like '\\_\\_%'"
                " and c.relname not like 'migkit\\_%'"
                " and not exists (select 1 from pg_index i"
                "  where i.indrelid = c.oid"
                "  and (i.indisprimary or i.indisunique))"
                " order by 1").splitlines() if l]
        if nopk:
            res.append(Result("deep", f"{db} keys", "diff",
                              f"{len(nopk)} tables have no pk/unique"
                              " (CDC drops their updates/deletes, dups on"
                              f" reload, unverifiable): {', '.join(nopk[:5])}",
                              "", "add a primary key or unique index, or set"
                                  " replica identity full, before migrating"))
        else:
            res.append(Result("deep", f"{db} keys", "ok",
                              "every table has a pk or unique index"))

        res.append(self._planner_stats(db))
        cross = self._crosscheck(db)
        if cross is not None:
            res.append(cross)
        res.append(self._lob_check(db))
        indexes = self._invalid_indexes(db)
        collations = self._collation_versions(db)
        res.append(indexes)
        res.append(collations)
        res.append(self._mojibake(db))
        # the expensive hunt only earns its keep once something else has
        # said an index cannot be trusted
        if collations.status == "diff":
            why = "the sort order an index was built under has changed"
        elif indexes.status == "diff":
            why = "an index exists that answers no query"
        else:
            why = ""
        res.append(self._duplicate_keys(db, why))
        res.append(self._temporal_meaning(db))
        res.append(self._time_zone_rules(db))
        res.append(self._capacity_gaps(db))

        # orphans only hide behind NOT VALID fks (pg enforces validated ones)
        fks = [l.split("|") for l in self._psql("dst", db, """
            select c.conname
              ||'|'||c.conrelid::regclass||'|'||c.confrelid::regclass
              ||'|'||(select string_agg(quote_ident(a.attname), ','
                                        order by x.ord)
                      from unnest(c.conkey) with ordinality x(attnum, ord)
                      join pg_attribute a on a.attrelid = c.conrelid
                       and a.attnum = x.attnum)
              ||'|'||(select string_agg(quote_ident(a.attname), ','
                                        order by x.ord)
                      from unnest(c.confkey) with ordinality x(attnum, ord)
                      join pg_attribute a on a.attrelid = c.confrelid
                       and a.attnum = x.attnum)
            from pg_constraint c
            join pg_namespace n on n.oid = c.connamespace
            where c.contype = 'f' and not c.convalidated
              and n.nspname not like '\\_\\_%'""").splitlines() if l]
        orphans = []
        for name, child, parent, ckeys, pkeys in fks:
            cc = ", ".join(f"c.{k}" for k in ckeys.split(","))
            pc = ", ".join(f"p.{k}" for k in pkeys.split(","))
            n = self._psql("dst", db,
                           f"select count(*) from {child} c"
                           f" where ({cc}) is not null and not exists"
                           f" (select 1 from {parent} p where ({pc}) = ({cc}))")
            if n != "0":
                orphans.append(f"{child}.{name}: {n} orphan rows")
        if orphans:
            res.append(Result("deep", f"{db} fk", "diff",
                              "; ".join(orphans[:5]), "",
                              "fix orphans, then alter table ..."
                              " validate constraint on target"))
        else:
            res.append(Result("deep", f"{db} fk", "ok",
                              f"{len(fks)} NOT VALID fks scanned, 0 orphans;"
                              " validate them before cutover" if fks
                              else "all fk constraints validated, no orphans"
                                   " possible"))

        # NOT VALID check constraints enforce new writes but never scanned the
        # existing rows, and the planner distrusts them - a load-time speed
        # hack left unfinished. The fk orphan scan above covers foreign keys;
        # check constraints are the blind spot.
        nv = self._unvalidated_checks(db)
        nvc = [f"{tbl}.{name}" for tbl, name, _ in nv]
        if nvc:
            res.append(Result("deep", f"{db} checks", "diff",
                              f"{len(nvc)} check constraints NOT VALIDATED"
                              " (existing rows unchecked, planner distrusts): "
                              + "; ".join(nvc[:5]), "",
                              "migkit sync --kind schema --apply validates"
                              " them; it fails loudly if a row violates one"))
        else:
            res.append(Result("deep", f"{db} checks", "ok",
                              "all check constraints validated"))

        # some tooling drops DEFERRABLE / INITIALLY DEFERRED when copying a
        # schema; code that relies on deferred checks (bulk reorder inside one
        # transaction) then fails with a constraint violation that never
        # happened on the source. Diff the deferral flags per constraint.
        dfq = ("select conrelid::regclass::text||'.'||conname||'|'"
               "||condeferrable||'|'||condeferred from pg_constraint c"
               " join pg_namespace n on n.oid = c.connamespace"
               " where c.contype in ('p','u','f','c') and c.conrelid <> 0"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'")
        sdf = {l.split("|", 1)[0]: l.split("|", 1)[1] for l in
               self._psql("src", db, dfq).splitlines() if "|" in l}
        ddf = {l.split("|", 1)[0]: l.split("|", 1)[1] for l in
               self._psql("dst", db, dfq).splitlines() if "|" in l}
        defer = [f"{k}: src deferrable/deferred={sdf[k]} dst={ddf[k]}"
                 for k in sorted(sdf) if k in ddf and sdf[k] != ddf[k]]
        if defer:
            res.append(Result("deep", f"{db} deferrable", "diff",
                              f"{len(defer)} constraints changed deferral: "
                              + "; ".join(defer[:5]), "",
                              "alter table ... alter constraint ... deferrable"
                              " initially deferred to match source"))
        else:
            res.append(Result("deep", f"{db} deferrable", "ok",
                              "constraint deferral flags match"))

        # row-level security is a silent-data-loss trap: a non-owner /
        # non-BYPASSRLS role (which a dump or even migkit itself may connect
        # as) sees only policy-permitted rows, so counts and checksums can be
        # a filtered subset with no error. And RLS enabled with zero policies
        # is default-deny - the table looks empty to everyone but the owner.
        rls = [l.split("|") for l in self._psql("src", db,
               "select n.nspname||'.'||c.relname||'|'||"
               "(select count(*) from pg_policies p"
               " where p.schemaname = n.nspname and p.tablename = c.relname)"
               " from pg_class c join pg_namespace n on n.oid = c.relnamespace"
               " where c.relkind = 'r' and c.relrowsecurity"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'").splitlines() if l]
        if rls:
            # the same rule the counts and data passes use, asked once:
            # an owner reads its own tables in full unless they are FORCEd,
            # so checking only for superuser/BYPASSRLS called every owner
            # filtered
            filtered = self._filtered_tables("src", db) or []
            deny = [r[0] for r in rls if r[1] == "0"]
            msgs = []
            if deny:
                msgs.append(f"{len(deny)} RLS tables have ZERO policies"
                            " (default-deny, read as empty by non-owners): "
                            + ", ".join(deny[:4]))
            if filtered:
                msgs.append("migkit's source role is subject to RLS on"
                            f" {len(filtered)} tables - counts/checksums"
                            " there may be a filtered subset, not the full"
                            " data: " + ", ".join(filtered[:4]))
            res.append(Result("deep", f"{db} rls", "diff" if msgs else "ok",
                              "; ".join(msgs) if msgs
                              else f"{len(rls)} RLS tables, all have policies"
                                   " and migkit reads with a bypass role", "",
                              "verify RLS tables with an owner/BYPASSRLS role;"
                              " recreate missing policies on target"
                              if msgs else ""))
        else:
            res.append(Result("deep", f"{db} rls", "ok",
                              "no row-level security in use"))

        trg = [l.split("\t") for l in
               self._psql("dst", db, self.TARGET_TRIGGERS).splitlines() if l]
        dis = [name for name, table, state in trg if state == "D"]
        # only the tables a load would write: one that exists on the target
        # alone is never written, and naming it would be noise
        writable = set(self._psql("src", db, self.USER_TABLES).splitlines())
        quieted = sorted(name for name, table, state in trg
                         if state != "D" and table in writable)
        res.append(self._trigger_result(
            db, sorted(dis), quieted,
            "nothing to do unless one of them does work the migrated rows"
            " need - an audit row, a maintained counter - in which case"
            " that work has to happen another way"))

        colq = ("select table_schema||'.'||table_name||'.'||column_name"
                "||'|'||coalesce(data_type,'')||'|'||is_nullable"
                "||'|'||coalesce(column_default,'')"
                "||'|'||coalesce(character_maximum_length::text,'')"
                "||'|'||coalesce(numeric_precision::text,'')"
                "||'|'||coalesce(numeric_scale::text,'')"
                " from information_schema.columns"
                " where table_schema not in ('pg_catalog',"
                "'information_schema')"
                " and table_schema not like 'pg\\_%'"
                " and table_schema not like '\\_\\_%'"
                " and table_name not like 'migkit\\_%' order by 1")
        sc = {l.split("|", 1)[0]: l for l in
              self._psql("src", db, colq).splitlines() if l}
        dc = {l.split("|", 1)[0]: l for l in
              self._psql("dst", db, colq).splitlines() if l}
        ignores = self._ignore_patterns()
        drift = [f"src {sc[k]}\ndst {dc[k]}" for k in sorted(sc)
                 if k in dc and sc[k] != dc[k]
                 and not any(p.search(sc[k]) or p.search(dc[k])
                             for p in ignores)]
        if drift:
            out = rpt / "deep-columns.diff"
            out.write_text("\n\n".join(drift) + "\n")
            heads = [d.splitlines()[0].split("|")[0][4:] for d in drift]
            res.append(Result("deep", f"{db} columns", "diff",
                              f"{len(drift)} columns drift"
                              f" (type/null/default/precision):"
                              f" {', '.join(heads[:4])}", str(out),
                              "see deep-columns.diff, align target DDL"))
        else:
            res.append(Result("deep", f"{db} columns", "ok",
                              f"{len(sc)} columns compared, type/null/"
                              "default/precision identical"))

        # narrowing is the dangerous subset of drift: a target column that
        # holds fewer characters, less numeric scale/precision, or a smaller
        # integer than the source silently truncates, rounds, or overflows
        # values (DMS caps unlimited text at varchar(8000); scale loss eats
        # money). Called out on its own, at higher severity than cosmetic drift.
        CHAR_T = ("character varying", "character", "text")
        INTW = {"smallint": 2, "integer": 4, "bigint": 8}
        narrow = []
        for k in sorted(sc):
            if k not in dc:
                continue
            sp, dp = sc[k].split("|"), dc[k].split("|")
            if len(sp) < 7 or len(dp) < 7:
                continue
            st, dt = sp[1], dp[1]
            scm, dcm, spr, ssc = sp[4], dp[4], sp[5], sp[6]
            dpr, dsc = dp[5], dp[6]
            why = None
            if st in CHAR_T and dcm and (not scm or int(dcm) < int(scm)):
                why = f"char {scm or 'unlimited'} -> {dcm}"
            elif st in ("numeric", "decimal") and ssc and dsc \
                    and int(dsc) < int(ssc):
                why = f"numeric scale {ssc} -> {dsc} (rounds)"
            elif st in ("numeric", "decimal") and spr and dpr \
                    and int(dpr) < int(spr):
                why = f"numeric precision {spr} -> {dpr} (overflow)"
            elif INTW.get(st, 0) > INTW.get(dt, 99):
                why = f"{st} -> {dt} (overflow)"
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

        # a constant, per-row offset on a timestamp column is the fingerprint
        # of a timezone conversion bug (a mover applying a non-UTC session),
        # not random corruption. Both sides are read with TimeZone=UTC pinned,
        # so a correct instant compares as delta 0; a systematic shift shows
        # the same non-zero delta on every sampled row.
        tsq = ("select n.nspname||'.'||c.relname||'|'||a.attname"
               " from pg_attribute a"
               " join pg_class c on c.oid = a.attrelid"
               " join pg_namespace n on n.oid = c.relnamespace"
               " join pg_type ty on ty.oid = a.atttypid"
               " where c.relkind = 'r' and a.attnum > 0"
               " and not a.attisdropped"
               " and ty.typname in ('timestamp','timestamptz')"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'"
               " and c.relname not like 'migkit\\_%' order by 1")
        shifts = []
        for tc in [l for l in self._psql("src", db, tsq).splitlines() if l][:10]:
            tbl, col = tc.split("|", 1)
            pks = self._pk_cols_of(db, tbl)
            if len(pks) != 1:
                continue
            sch2, t2 = tbl.split(".", 1)
            pk = pks[0]
            q = (f'select "{pk}"::text||e\'\\t\'||extract(epoch from "{col}")'
                 f' from "{sch2}"."{t2}" where "{col}" is not null'
                 f' order by "{pk}" limit 200')
            try:
                sm = dict(l.split("\t") for l in
                          self._psql("src", db, q).splitlines() if "\t" in l)
                dm = dict(l.split("\t") for l in
                          self._psql("dst", db, q).splitlines() if "\t" in l)
            except (RuntimeError, ValueError):
                continue
            deltas = [float(dm[k]) - float(sm[k]) for k in sm if k in dm]
            if len(deltas) < 3:
                continue
            avg = sum(deltas) / len(deltas)
            if max(deltas) - min(deltas) < 1 and abs(avg) >= 1:
                secs = round(avg)
                shifts.append(f"{tbl}.{col}: every row shifted"
                              f" {secs}s (~{secs / 3600:.1f}h)")
        if shifts:
            res.append(Result("deep", f"{db} timeshift", "diff",
                              "uniform timezone offset (systematic, not"
                              " row-level corruption): " + "; ".join(shifts[:5]),
                              "", "target stored a non-UTC wall clock; re-load"
                              " with the source session timezone or convert"
                              " the column"))
        else:
            res.append(Result("deep", f"{db} timeshift", "ok",
                              "no uniform timestamp offset detected"))

        # NULL vs empty-string: Oracle stores '' as NULL, Postgres keeps them
        # distinct, so a migration can silently flip IS NULL semantics and
        # unique behavior. Per text column, compare the null-count and the
        # empty-count on each side; a swap is the fingerprint (and it survives
        # a row checksum only because both are "present").
        txtq = ("select n.nspname||'.'||c.relname||'|'||a.attname"
                " from pg_attribute a"
                " join pg_class c on c.oid = a.attrelid"
                " join pg_namespace n on n.oid = c.relnamespace"
                " join pg_type ty on ty.oid = a.atttypid"
                " where c.relkind = 'r' and a.attnum > 0"
                " and not a.attisdropped"
                " and ty.typname in ('text','varchar','bpchar')"
                " and n.nspname not in ('pg_catalog','information_schema')"
                " and n.nspname not like 'pg\\_%'"
                " and n.nspname not like '\\_\\_%'"
                " and c.relname not like 'migkit\\_%' order by 1")
        neq = []
        for tc in [l for l in self._psql("src", db, txtq).splitlines() if l][:30]:
            tbl, col = tc.split("|", 1)
            sch2, t2 = tbl.split(".", 1)
            q = (f'select count(*) filter (where "{col}" is null)||\'|\'||'
                 f'count(*) filter (where "{col}" = \'\') from "{sch2}"."{t2}"')
            try:
                sv, dv = self._psql("src", db, q), self._psql("dst", db, q)
            except RuntimeError:
                continue
            if sv != dv and "|" in sv and "|" in dv:
                neq.append(f"{tbl}.{col}: src null/empty={sv} dst={dv}")
        if neq:
            res.append(Result("deep", f"{db} nullempty", "diff",
                              f"{len(neq)} text columns differ in NULL vs"
                              " empty-string split (semantic flip): "
                              + "; ".join(neq[:5]), "",
                              "normalize with NULLIF(col,'') / COALESCE per"
                              " column intent; decide which side is canonical"))
        else:
            res.append(Result("deep", f"{db} nullempty", "ok",
                              "NULL vs empty-string consistent on text columns"))

        # charset corruption: a lossy transcode on load leaves U+FFFD
        # replacement characters, so any excess on the target over the source
        # is silent damage; and a SQL_ASCII target validates nothing, letting
        # bad bytes through. (utf8mb4/latin1 traps are mysql-specific.)
        enc = []
        se = self._psql("src", db, "select pg_encoding_to_char(encoding)"
                        " from pg_database where datname = current_database()")
        de = self._psql("dst", db, "select pg_encoding_to_char(encoding)"
                        " from pg_database where datname = current_database()")
        if de == "SQL_ASCII" and se != "SQL_ASCII":
            enc.append(f"target database is SQL_ASCII (no encoding"
                       f" validation); source is {se}")
        elif se != de:
            enc.append(f"server_encoding differs: src={se} dst={de}")
        for tc in [l for l in self._psql("src", db, txtq).splitlines() if l][:30]:
            tbl, col = tc.split("|", 1)
            sch2, t2 = tbl.split(".", 1)
            q = ('select coalesce(sum(char_length("' + col + '")'
                 ' - char_length(replace("' + col + '", U&\'\\FFFD\','
                 " ''))),0) from \"" + sch2 + '"."' + t2 + '"')
            try:
                sv = int(self._psql("src", db, q) or 0)
                dv = int(self._psql("dst", db, q) or 0)
            except (RuntimeError, ValueError):
                continue
            if dv > sv:
                enc.append(f"{tbl}.{col}: {dv - sv} extra U+FFFD replacement"
                           " chars on target")
        if enc:
            res.append(Result("deep", f"{db} encoding", "diff",
                              "; ".join(enc[:6]), "",
                              "re-load affected rows with a non-lossy client"
                              " encoding; never use a SQL_ASCII target"))
        else:
            res.append(Result("deep", f"{db} encoding", "ok",
                              "encodings match, no replacement-char excess"))

        # partitioned tables: a mover can land rows in the DEFAULT catch-all
        # partition, miss a partition bound entirely, or even change the
        # partition key - all of which reroute or strand data silently.
        parts = [l.split("|", 1) for l in self._psql("src", db,
                 "select c.relnamespace::regnamespace||'.'||c.relname"
                 "||'|'||pg_get_partkeydef(c.oid)"
                 " from pg_partitioned_table p"
                 " join pg_class c on c.oid = p.partrelid"
                 " where c.relnamespace::regnamespace::text not like 'pg\\_%'"
                 " and c.relnamespace::regnamespace::text not like"
                 " '\\_\\_%'").splitlines() if "|" in l]
        pbad = []
        for tbl, keydef in parts:
            dkey = self._psql("dst", db,
                              f"select pg_get_partkeydef('{tbl}'::regclass)")
            if not dkey:
                pbad.append(f"{tbl}: not partitioned on target (was {keydef})")
                continue
            if dkey != keydef:
                pbad.append(f"{tbl}: partition key differs"
                            f" src({keydef}) dst({dkey})")
                continue
            bq = ("select coalesce(pg_get_expr(ch.relpartbound, ch.oid),'')"
                  " from pg_inherits i join pg_class ch on ch.oid = i.inhrelid"
                  f" where i.inhparent = '{tbl}'::regclass")
            sb = set(self._psql("src", db, bq).splitlines()) - {""}
            dbnd = set(self._psql("dst", db, bq).splitlines()) - {""}
            miss = sb - dbnd
            if miss:
                pbad.append(f"{tbl}: {len(miss)} partition bound(s) missing on"
                            f" target: {', '.join(sorted(miss)[:2])}")
            dflt = self._psql("dst", db,
                              "select c.relnamespace::regnamespace||'.'"
                              "||c.relname from pg_inherits i"
                              " join pg_class c on c.oid = i.inhrelid"
                              f" where i.inhparent = '{tbl}'::regclass"
                              " and pg_get_expr(c.relpartbound, c.oid)"
                              " = 'DEFAULT'")
            if dflt:
                n = self._psql("dst", db, f"select count(*) from {dflt}")
                if n and int(n) > 0:
                    pbad.append(f"{tbl}: {n} rows stranded in default"
                                f" partition ({dflt})")
        if not parts:
            res.append(Result("deep", f"{db} partitions", "ok",
                              "no partitioned tables"))
        elif pbad:
            res.append(Result("deep", f"{db} partitions", "diff",
                              "; ".join(pbad[:6]), "",
                              "recreate the missing partitions and move rows"
                              " out of the default before cutover"))
        else:
            res.append(Result("deep", f"{db} partitions", "ok",
                              f"{len(parts)} partitioned tables, schemes and"
                              " bounds match, default empty"))

        # generated columns: a bulk load can write a literal into a stored
        # generated column (so it no longer equals its expression), the
        # expression can drift, or a column generated on the source can arrive
        # plain on the target (then a mover happily inserts rotting literals).
        import re as _re
        genq = ("select c.relnamespace::regnamespace||'.'||c.relname"
                "||'|'||a.attname||'|'||a.attgenerated::text"
                "||'|'||coalesce(pg_get_expr(d.adbin, d.adrelid),'')"
                " from pg_attribute a"
                " join pg_class c on c.oid = a.attrelid"
                " left join pg_attrdef d on d.adrelid = a.attrelid"
                "  and d.adnum = a.attnum"
                " where a.attnum > 0 and not a.attisdropped"
                "  and a.attgenerated <> '' and c.relkind = 'r'"
                " and c.relnamespace::regnamespace::text"
                "  not in ('pg_catalog','information_schema')"
                " and c.relnamespace::regnamespace::text not like 'pg\\_%'"
                " and c.relnamespace::regnamespace::text not like '\\_\\_%'")

        def _genmap(side):
            out = {}
            for l in self._psql(side, db, genq).splitlines():
                p = l.split("|", 3)
                if len(p) == 4:
                    out[p[0] + "|" + p[1]] = (p[2], p[3])
            return out

        def _norm(e):
            return _re.sub(r"\s+", "", e).lower()
        sg, dg = _genmap("src"), _genmap("dst")
        gbad = []
        for k, (gen, expr) in sorted(sg.items()):
            tbl, col = k.split("|")
            if k not in dg:
                gbad.append(f"{tbl}.{col}: generated on source,"
                            " plain/missing on target")
                continue
            dexpr = dg[k][1]
            if _norm(expr) != _norm(dexpr):
                gbad.append(f"{tbl}.{col}: generation expression differs")
                continue
            if gen == "s":
                sch2, t2 = tbl.split(".", 1)
                try:
                    n = self._psql("dst", db, f'select count(*) from'
                                   f' "{sch2}"."{t2}" where "{col}"'
                                   f" is distinct from ({dexpr})")
                    if n and int(n) > 0:
                        gbad.append(f"{tbl}.{col}: {n} rows where the stored"
                                    " value != its expression")
                except (RuntimeError, ValueError):
                    pass
        for k in sorted(dg):
            if k not in sg:
                tbl, col = k.split("|")
                gbad.append(f"{tbl}.{col}: generated on target but plain on"
                            " source")
        if gbad:
            res.append(Result("deep", f"{db} generated", "diff",
                              "; ".join(gbad[:6]), "",
                              "align the generation expression / storage on"
                              " target and re-derive the column"))
        else:
            res.append(Result("deep", f"{db} generated", "ok",
                              f"{len(sg)} generated columns match"
                              if sg else "no generated columns"))

        # collation: a unique/pk text column whose target collation is
        # case/accent-insensitive (or just different) can COLLAPSE distinct
        # source rows into duplicates on load - silent data loss. And a
        # glibc/ICU version drift silently corrupts existing btree unique
        # indexes (wrong results, duplicate admission).
        uq = ("select c.relnamespace::regnamespace||'.'||c.relname"
              "||'|'||a.attname||'|'||coalesce(co.collname::text,'default')"
              " from pg_constraint con"
              " join pg_class c on c.oid = con.conrelid"
              " join pg_attribute a on a.attrelid = con.conrelid"
              "  and a.attnum = con.conkey[1]"
              " left join pg_collation co on co.oid = a.attcollation"
              " where con.contype in ('p','u')"
              " and array_length(con.conkey,1) = 1"
              " and a.atttypid in ('text'::regtype,'varchar'::regtype,"
              "'bpchar'::regtype)"
              " and c.relnamespace::regnamespace::text"
              "  not in ('pg_catalog','information_schema')"
              " and c.relnamespace::regnamespace::text not like 'pg\\_%'"
              " and c.relnamespace::regnamespace::text not like '\\_\\_%'")

        def _uqmap(side):
            out = {}
            for l in self._psql(side, db, uq).splitlines():
                p = l.split("|")
                if len(p) == 3:
                    out[p[0] + "|" + p[1]] = p[2]
            return out
        su, du = _uqmap("src"), _uqmap("dst")
        cbad = []
        for k, scoll in sorted(su.items()):
            dcoll = du.get(k)
            if not dcoll or dcoll == scoll:
                continue
            tbl, col = k.split("|")
            sch2, t2 = tbl.split(".", 1)
            detail = (f"{tbl}.{col}: unique-key collation src({scoll})"
                      f" != dst({dcoll})")
            if dcoll != "default":
                try:
                    n = self._psql("src", db,
                                   f'select count(*) from (select 1 from'
                                   f' "{sch2}"."{t2}" group by "{col}"'
                                   f' collate "{dcoll}" having count(*) > 1) x')
                    if n and int(n) > 0:
                        detail += (f"; {n} source groups COLLAPSE to duplicates"
                                   " under the target collation (data loss)")
                except RuntimeError:
                    detail += "; collapse untestable (collation not on source)"
            cbad.append(detail)
        try:
            v = self._psql("dst", db,
                           "select count(*) from pg_depend d"
                           " where d.refclassid = 'pg_collation'::regclass"
                           " and d.refobjversion <> ''"
                           " and d.refobjversion <>"
                           " pg_collation_actual_version(d.refobjid)")
            if v and int(v) > 0:
                cbad.append(f"{v} target objects built under a stale collation"
                            " version (glibc/ICU drift) - unique indexes may be"
                            " corrupt; reindex then refresh collation version")
        except RuntimeError:
            pass
        if cbad:
            res.append(Result("deep", f"{db} collation", "diff",
                              "; ".join(cbad[:6]), "",
                              "match the unique-key collation to source (or"
                              " dedup first); reindex on version drift"))
        else:
            res.append(Result("deep", f"{db} collation", "ok",
                              "unique-key collations match, no version drift"))

        # float columns: FLOAT/DOUBLE are IEEE approximations, so a mover that
        # changes precision (float4->float8 widening, a text round-trip) drifts
        # them. Compare per-row with a relative tolerance, so genuine drift is
        # caught without false-flagging a bit-identical copy. This is a
        # diagnostic layer; the row checksum stays exact.
        tol = float(self.hop.options.get("float_tolerance", 1e-9))
        fcols = [l for l in self._psql("src", db,
                 "select n.nspname||'.'||c.relname||'|'||a.attname"
                 " from pg_attribute a join pg_class c on c.oid = a.attrelid"
                 " join pg_namespace n on n.oid = c.relnamespace"
                 " join pg_type ty on ty.oid = a.atttypid"
                 " where c.relkind = 'r' and a.attnum > 0"
                 " and not a.attisdropped and ty.typname in ('float4','float8')"
                 " and n.nspname not in ('pg_catalog','information_schema')"
                 " and n.nspname not like 'pg\\_%'"
                 " and n.nspname not like '\\_\\_%'"
                 " and c.relname not like 'migkit\\_%'").splitlines() if l]
        fbad = []
        for tc in fcols[:10]:
            tbl, col = tc.split("|", 1)
            pks = self._pk_cols_of(db, tbl)
            if len(pks) != 1:
                continue
            sch2, t2 = tbl.split(".", 1)
            pk = pks[0]
            q = (f'select "{pk}"::text||e\'\\t\'||"{col}" from "{sch2}"."{t2}"'
                 f' where "{col}" is not null order by "{pk}" limit 500')
            try:
                sm = dict(l.split("\t") for l in
                          self._psql("src", db, q).splitlines() if "\t" in l)
                dm = dict(l.split("\t") for l in
                          self._psql("dst", db, q).splitlines() if "\t" in l)
            except (RuntimeError, ValueError):
                continue
            worst, nd = 0.0, 0
            for k in sm:
                if k not in dm:
                    continue
                try:
                    a, b = float(sm[k]), float(dm[k])
                except ValueError:
                    continue
                d = abs(a - b)
                if d > tol * max(abs(a), abs(b), 1.0):
                    nd += 1
                    worst = max(worst, d)
            if nd:
                fbad.append(f"{tbl}.{col}: {nd} values drift beyond tolerance"
                            f" (max {worst:g})")
        if fbad:
            res.append(Result("deep", f"{db} float", "diff",
                              "; ".join(fbad[:5]), "",
                              "float precision changed on target (widening or"
                              " round-trip); use numeric for exact columns, or"
                              " set options.float_tolerance to accept it"))
        else:
            res.append(Result("deep", f"{db} float", "ok",
                              "float columns within tolerance"
                              if fcols else "no float columns"))

        # exotic types can render differently across builds; sample and
        # compare their actual text so a checksum can't hide it
        exq = ("select n.nspname||'.'||c.relname||'|'||a.attname"
               " from pg_attribute a"
               " join pg_class c on c.oid = a.attrelid"
               " join pg_namespace n on n.oid = c.relnamespace"
               " join pg_type ty on ty.oid = a.atttypid"
               " where c.relkind = 'r' and a.attnum > 0"
               " and not a.attisdropped"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'"
               " and c.relname not like 'migkit\\_%'"
               " and (ty.typtype in ('e','c','d','r','m') or ty.typname in"
               " ('money','point','polygon','path','circle','box','line',"
               "'lseg','tsvector','tsquery','xml','interval','bit','varbit'))")
        exotic = [l.split("|") for l in
                  self._psql("src", db, exq).splitlines() if l][:20]

        def _esc(v):
            return "'" + v.replace("'", "''") + "'"

        drifts = []
        audited = 0
        for tbl, col in exotic:
            pks = self._pk_cols_of(db, tbl)
            if not pks:
                continue
            sch2, t2 = tbl.split(".", 1)
            pkexpr = self._pk_text_expr(pks)
            q = (f'select {pkexpr}||\'|\'||"{col}"::text'
                 f' from "{sch2}"."{t2}" where "{col}" is not null limit 5')
            try:
                a = dict(l.split("|", 1) for l in
                         self._psql("src", db, q).splitlines() if "|" in l)
            except RuntimeError:
                continue
            if not a:
                continue
            audited += 1
            if len(pks) == 1:
                where = (f'"{pks[0]}"::text in ('
                         + ", ".join(_esc(k) for k in a) + ")")
            else:
                tup = ", ".join(f'"{p}"::text' for p in pks)
                vals = ", ".join(
                    "(" + ", ".join(_esc(x) for x in self._pk_parts(k)) + ")"
                    for k in a)
                where = f"({tup}) in ({vals})"
            qd = (f'select {pkexpr}||\'|\'||"{col}"::text'
                  f' from "{sch2}"."{t2}" where {where}')
            try:
                b = dict(l.split("|", 1) for l in
                         self._psql("dst", db, qd).splitlines() if "|" in l)
            except RuntimeError:
                drifts.append(f"{tbl}.{col}: unreadable on target")
                continue
            for k, v in a.items():
                if k in b and b[k] != v:
                    drifts.append(f"{tbl}.{col}: renders differently"
                                  f" (src {v[:40]!r} dst {b[k][:40]!r})")
                    break
        res.append(Result("deep", f"{db} render",
                          "diff" if drifts else "ok",
                          "; ".join(drifts[:5]) if drifts
                          else (f"{audited} exotic-typed columns sampled,"
                                " rendering identical" if audited
                                else "no exotic-typed columns"), "",
                          "check type definitions/versions on target;"
                          " consider options.checksum: jsonb" if drifts
                          else ""))

        mvs = [l for l in self._psql("src", db,
               "select n.nspname||'.'||c.relname from pg_class c"
               " join pg_namespace n on n.oid = c.relnamespace"
               " where c.relkind = 'm'"
               " and n.nspname not like '\\_\\_%'").splitlines() if l]
        stale = []
        for m in mvs:
            try:
                pop = self._psql("dst", db, "select relispopulated"
                                 f" from pg_class where oid = '{m}'::regclass")
            except RuntimeError:
                stale.append(f"{m}: missing on target")
                continue
            if pop != "t":
                stale.append(f"{m}: not populated on target")
                continue
            # count misses a stale mv with the same size; checksum content
            a = self._content_fingerprint("src", db, m)
            b = self._content_fingerprint("dst", db, m)
            if a != b:
                stale.append(f"{m}: rows|checksum src={a} dst={b},"
                             " stale on target")
        res.append(Result("deep", f"{db} matviews",
                          "diff" if stale else "ok",
                          "; ".join(stale[:5]) if stale
                          else f"{len(mvs)} matviews populated and equal"
                          if mvs else "no matviews", "",
                          "refresh materialized view ... on target"
                          if stale else ""))

        # The same computation feeds the repair, so it lives in one place:
        # two copies of "which grants are missing" would eventually disagree,
        # and the disagreement would be a repair that grants the wrong thing.
        gaps = self._grant_gaps(db)
        if gaps is None:
            res.append(Result("deep", f"{db} grants", "warn",
                              "cannot read grants - unknown, not clean"))
        else:
            miss, extra, smiss, seen, sseen = gaps
            if miss or extra:
                res.append(Result("deep", f"{db} grants", "diff",
                                  (f"{len(miss)} grants missing on target"
                                   + (": " + "; ".join(
                                       g.replace("|", " ") for g in miss[:3])
                                      if miss else "")
                                   + (f"; {len(extra)} extra" if extra else "")),
                                  "", "migkit sync --kind schema --apply"
                                      " re-grants them, with undo"))
            else:
                res.append(Result("deep", f"{db} grants", "ok",
                                  f"{seen} table grants match"
                                  " (roles present both sides)"))
            if smiss:
                res.append(Result("deep", f"{db} seq-grants", "diff",
                                  f"{len(smiss)} sequence grants missing on"
                                  " target (inserts will hit 'permission"
                                  " denied for sequence'): "
                                  + "; ".join(g.replace("|", " ")
                                              for g in smiss[:4]), "",
                                  "migkit sync --kind schema --apply grants"
                                  " them, with undo"))
            else:
                res.append(Result("deep", f"{db} seq-grants", "ok",
                                  f"{sseen} sequence grants match"
                                  if sseen else "no explicit sequence grants"))

        # a missing or version-mismatched extension breaks its functions and
        # can fail the restore outright; the mover copies data, not CREATE
        # EXTENSION. Diff the installed set (plpgsql is always present).
        exq = ("select extname||' '||extversion from pg_extension"
               " where extname <> 'plpgsql'")
        se = set(self._psql("src", db, exq).splitlines()) - {""}
        de = set(self._psql("dst", db, exq).splitlines()) - {""}
        dn = {e.split(" ")[0] for e in de}
        miss = sorted(e.split(" ")[0] for e in se if e.split(" ")[0] not in dn)
        vers = sorted(e for e in se if e.split(" ")[0] in dn and e not in de)
        if miss or vers:
            det = []
            if miss:
                det.append(f"{len(miss)} missing on target: "
                           + ", ".join(miss[:5]))
            if vers:
                det.append("version mismatch: " + ", ".join(vers[:3]))
            res.append(Result("deep", f"{db} extensions", "diff",
                              "; ".join(det), "",
                              "create extension ... on target (and install its"
                              " shared library) before the app depends on it"))
        else:
            res.append(Result("deep", f"{db} extensions", "ok",
                              f"{len(se)} extensions match" if se
                              else "no non-default extensions"))

        res.append(self._extension_data(db))
        res.append(self._large_objects(db))
        res += self._deep_ownership(db)

        pkq = ("select n.nspname||'|'||c.relname||'|'||a.attname"
               " from pg_index i"
               " join pg_class c on c.oid = i.indrelid"
               " join pg_namespace n on n.oid = c.relnamespace"
               " join pg_attribute a on a.attrelid = i.indrelid"
               " and a.attnum = i.indkey[0]"
               " where i.indisprimary and i.indnatts = 1"
               " and a.atttypid in ('smallint'::regtype::oid,"
               "'integer'::regtype::oid,'bigint'::regtype::oid)"
               " and c.relkind = 'r'"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'"
               " and c.relname not like 'migkit\\_%' order by 1")
        spk = [l.split("|") for l in
               self._psql("src", db, pkq).splitlines() if l]
        dpk = {tuple(l.split("|")[:2]) for l in
               self._psql("dst", db, pkq).splitlines() if l}
        both = [(s, t, c) for s, t, c in spk if (s, t) in dpk]

        def maxes(side, triples):
            out = {}
            for i in range(0, len(triples), 200):
                parts = [f"select '{s}.{t}|'||coalesce(max(\"{c}\"), 0)"
                         f' from "{s}"."{t}"'
                         for s, t, c in triples[i:i + 200]]
                for l in self._psql(side, db,
                                    " union all ".join(parts)).splitlines():
                    k, _, v = l.rpartition("|")
                    out[k] = int(v)
            return out

        if both:
            am, bm = maxes("src", both), maxes("dst", both)
            ahead = [f"{k} src_max={am[k]} dst_max={bm[k]}"
                     for k in sorted(am) if bm.get(k, 0) > am[k]]
            behind = [f"{k} src_max={am[k]} dst_max={bm[k]}"
                      for k in sorted(am) if bm.get(k, 0) < am[k]]
            if ahead:
                res.append(Result("deep", f"{db} boundary", "diff",
                                  f"target max(pk) AHEAD of source on"
                                  f" {len(ahead)}: {'; '.join(ahead[:4])}",
                                  "", "writes landing on target or"
                                      " double-apply, find the writer"
                                      " before cutover"))
            else:
                note = (f"; {len(behind)} behind (replication lag):"
                        f" {'; '.join(behind[:3])}" if behind else "")
                res.append(Result("deep", f"{db} boundary", "ok",
                                  f"max(pk) checked on {len(both)} tables,"
                                  f" none ahead of source{note}"))
        else:
            res.append(Result("deep", f"{db} boundary", "ok",
                              "no single-int-pk tables to boundary-check"))
        return res

    OWNER_SQL = """
        select 'table '||n.nspname||'.'||c.relname||'|'
               ||pg_get_userbyid(c.relowner)
          from pg_class c join pg_namespace n on n.oid = c.relnamespace
         where c.relkind in ('r','p','v','m','S')
           and n.nspname not in ('pg_catalog','information_schema')
           and n.nspname not like 'pg\\_%' and n.nspname not like '\\_\\_%'
        union all
        select 'routine '||n.nspname||'.'||p.proname||'|'
               ||pg_get_userbyid(p.proowner)
          from pg_proc p join pg_namespace n on n.oid = p.pronamespace
         where n.nspname not in ('pg_catalog','information_schema')
           and n.nspname not like 'pg\\_%' and n.nspname not like '\\_\\_%'
        union all
        select 'schema '||n.nspname||'|'||pg_get_userbyid(n.nspowner)
          from pg_namespace n
         where n.nspname not in ('pg_catalog','information_schema')
           and n.nspname not like 'pg\\_%' and n.nspname not like '\\_\\_%'
        order by 1"""

    def _replay_lag(self, side, db):
        """Seconds this side is behind its writer, or None if it will not say.

        None rather than zero: a replica that hides its replay position is a
        replica of unknown freshness, and calling that "caught up" is the
        mistake this exists to prevent.
        """
        try:
            raw = self._psql(side, db,
                             "select coalesce(extract(epoch from"
                             " (now() - pg_last_xact_replay_timestamp()))"
                             ", -1)").strip()
            v = float(raw)
        except Exception:
            return None
        return None if v < 0 else v

    def _change_marker(self, side, db, table):
        """A cheap proof that this table has not moved, or None.

        The pieces were chosen by measuring what each kind of change actually
        moves, on PostgreSQL 16:

            UPDATE / DELETE   the tuple counters move
            TRUNCATE          **the counters do not move at all** - only
                              relfilenode and the relation size do
            statistics reset  the counters drop to zero

        So the counters alone would skip a truncated table and call it equal.
        `relfilenode` is in the marker for exactly that case, and the whole
        thing is compared for equality so a reset reads as "look again".

        None is returned rather than a guess when the server is too old to
        trust: before 15 the statistics collector used UDP and could drop a
        message, and a dropped UPDATE is the false negative this exists to
        avoid.
        """
        from .. import unchanged as _u
        sch, tbl = self._split(table)
        # Before the version, because a wire-compatible fork answers the
        # version question plausibly and the pieces underneath it are dead.
        # Measured on CockroachDB: empty pg_stat_all_tables, relfilenode 0
        # everywhere, and a reported server_version of 13.0.0 - so the marker
        # would be constant and the only thing refusing it is that number.
        if not _u.usable_brand(self._brands()[0 if side == "src" else 1]):
            return None
        try:
            ver = self._psql(side, db, "show server_version")
            if not _u.usable_postgres(ver):
                return None
            row = self._psql(side, db, f"""
                select coalesce(s.n_tup_ins, -1)||chr(31)
                       ||coalesce(s.n_tup_upd, -1)||chr(31)
                       ||coalesce(s.n_tup_del, -1)||chr(31)
                       ||c.relfilenode||chr(31)
                       ||pg_relation_size(c.oid)
                  from pg_class c
                  join pg_namespace n on n.oid = c.relnamespace
                  left join pg_stat_all_tables s on s.relid = c.oid
                 where n.nspname = '{sch}' and c.relname = '{tbl}'""").strip()
        except RuntimeError:
            return None
        return _u.marker(row.split("\x1f")) if row else None

    def _unvalidated_checks(self, db):
        """[(table, constraint, definition)] left NOT VALID on the target.

        One computation for the check and the repair. The definition comes
        along because it is the only way back: PostgreSQL has no statement
        that un-validates a constraint, so the undo has to drop it and add it
        again exactly as it was - and `pg_get_constraintdef` includes the
        NOT VALID, so the recreated constraint is the same object in the same
        state.
        """
        rows = self._psql("dst", self._d("dst", db), """
            select conrelid::regclass::text||chr(31)||conname||chr(31)
                   ||pg_get_constraintdef(c.oid)
              from pg_constraint c
              join pg_namespace n on n.oid = c.connamespace
             where c.contype = 'c' and not c.convalidated
               and n.nspname not like '\\_\\_%'
             order by 1""").splitlines()
        return [tuple(r.split("\x1f", 2)) for r in rows if r.count("\x1f") == 2]

    def _constraint_repair(self, db):
        """Finish the validation the load skipped.

        A NOT VALID check constraint enforces new writes and was never checked
        against the rows already there, so the planner will not use it and
        nobody knows whether the existing data satisfies it. Validating scans
        the table under ShareUpdateExclusiveLock - no reads or writes are
        blocked, which `structural-fix.locks.txt` says for itself.

        Measured: if a row violates the constraint, PostgreSQL refuses with
        `check constraint "..." of relation "..." is violated by some row` and
        changes nothing. So this needs no pre-scan of its own - the server
        already performs one, and failing is the correct outcome.
        """
        nv = self._unvalidated_checks(db)
        if not nv:
            return None
        stmts, undo = [], []
        for tbl, name, definition in nv:
            stmts.append(f'ALTER TABLE {tbl} VALIDATE CONSTRAINT "{name}";')
            # no statement un-validates a constraint, so the way back is to
            # put the original one back exactly as it was
            undo.append(f'ALTER TABLE {tbl} DROP CONSTRAINT "{name}";')
            undo.append(f'ALTER TABLE {tbl} ADD CONSTRAINT "{name}"'
                        f' {definition};')
        return RepairAction(db, "constraints", stmts, undo,
                            f"{len(nv)} check constraints the load left"
                            " unvalidated")

    def _grant_gaps(self, db):
        """(missing, extra, missing_sequence, n_seen, n_seq_seen), or None.

        One computation, used by the deep check and by the repair. When these
        lived apart the repair did not exist at all; the moment it did, two
        copies of "which grants are missing" would have been two chances to
        grant the wrong thing to the wrong role.

        Reads raw `relacl` rather than `information_schema.table_privileges`:
        that view hides grants whose grantee the connected user is not a
        member of, which reads as missing on a target reached with a plain
        application user.
        """
        ign = "','".join(r.strip() for r in os.environ.get(
            "GRANTS_IGNORE_ROLES", "root,rdsadmin").split(",") if r.strip())
        common = (" and n.nspname not in ('pg_catalog','information_schema')"
                  " and n.nspname not like 'pg\\_%'"
                  " and n.nspname not like '\\_\\_%'"
                  " and pg_get_userbyid(a.grantee) <> 'PUBLIC'"
                  " and pg_get_userbyid(a.grantee) not like 'pg\\_%'"
                  " and pg_get_userbyid(a.grantee) not like 'rds%'"
                  " and pg_get_userbyid(a.grantee) not like '%tencent%'"
                  f" and pg_get_userbyid(a.grantee) not in ('{ign}')"
                  " and c.relname not like 'migkit\\_%'")
        pick = ("select pg_get_userbyid(a.grantee)||'|'||n.nspname||'.'"
                "||c.relname||'|'||a.privilege_type from pg_class c"
                " join pg_namespace n on n.oid = c.relnamespace"
                " cross join lateral aclexplode(c.relacl) a"
                " where c.relkind in ({kinds})" + common)
        gq = pick.format(kinds="'r','p','v','m','f'")
        sgq = pick.format(kinds="'S'")
        try:
            ddb = self._d("dst", db)
            ga = set(self._psql("src", db, gq).splitlines()) - {""}
            gb = set(self._psql("dst", ddb, gq).splitlines()) - {""}
            sga = set(self._psql("src", db, sgq).splitlines()) - {""}
            sgb = set(self._psql("dst", ddb, sgq).splitlines()) - {""}
            droles = set(self._psql("dst", ddb,
                                    "select rolname from pg_roles").splitlines())
            sroles = set(self._psql("src", db,
                                    "select rolname from pg_roles").splitlines())
        except RuntimeError:
            return None
        noise = self._noise()

        def _app(rows):
            if not noise:
                return rows
            return {g for g in rows
                    if not g.split("|")[1].partition(".")[2].startswith(noise)}
        ga, gb, sga, sgb = _app(ga), _app(gb), _app(sga), _app(sgb)
        # a grant to a role the target does not have is a missing role, which
        # `users` reports; granting to it here would just fail
        miss = sorted(g for g in ga - gb if g.split("|")[0] in droles)
        extra = sorted(g for g in gb - ga if g.split("|")[0] in sroles)
        smiss = sorted(g for g in sga - sgb if g.split("|")[0] in droles)
        return miss, extra, smiss, len(ga), len(sga)

    @staticmethod
    def _grant_sql(entry, revoke=False, obj_type=""):
        """One GRANT (or REVOKE) from a `role|schema.object|PRIVILEGE` row.

        `obj_type` is spelled out for sequences. Measured on PostgreSQL 16
        that the bare form is accepted for them too - `GRANT USAGE ON
        "public"."t_id_seq"` succeeds - but the object type is known here, so
        saying it costs nothing and removes the dependence on that fallback.
        """
        role, obj, priv = entry.split("|", 2)
        sch, _, name = obj.partition(".")
        kind = f"{obj_type} " if obj_type else ""
        target = f'{kind}"{sch}"."{name}"'
        who = f'"{role}"'
        if revoke:
            return f'REVOKE {priv} ON {target} FROM {who};'
        return f'GRANT {priv} ON {target} TO {who};'

    def _grant_repair(self, db):
        """Re-grant what the mover did not carry, with a REVOKE to undo it.

        Extra grants on the target are reported by the check but not revoked
        here. Removing a privilege somebody may have added on purpose is a
        different decision from restoring one the migration dropped, and doing
        both under one word would hide the second inside the first.
        """
        gaps = self._grant_gaps(db)
        if not gaps:
            return None
        miss, _extra, smiss, _, _ = gaps
        if not (miss or smiss):
            return None
        stmts = ([self._grant_sql(g) for g in miss]
                 + [self._grant_sql(g, obj_type="SEQUENCE") for g in smiss])
        undo = ([self._grant_sql(g, revoke=True) for g in miss]
                + [self._grant_sql(g, revoke=True, obj_type="SEQUENCE")
                   for g in smiss])
        note = (f"{len(miss)} table and {len(smiss)} sequence grants the"
                " mover did not carry")
        return RepairAction(db, "grants", stmts, undo, note)

    def _deep_ownership(self, db):
        """Object owners, which the structural differ does not compare.

        Tencent's PostgreSQL migration notes say objects created by `postgres`
        on the source change ownership to the migration account on the target.
        Measured: a table owned by `appowner` arrived owned by
        `dts_migration`, and the generated fix contained no `OWNER TO` at all -
        `results` compares definitions, and an owner is not part of one.

        The owner is the role that may ALTER or DROP the object. A target
        where the application role no longer owns its own tables looks correct
        until the first migration the application tries to run on itself.

        A function's SECURITY DEFINER flag is deliberately not checked here:
        it is part of the definition, so the structural diff already catches
        it, and checking it twice is how two copies drift apart.
        """
        from .. import ownership as _own
        try:
            a = dict(l.split("|", 1) for l in
                     self._psql("src", db, self.OWNER_SQL).splitlines() if "|" in l)
            b = dict(l.split("|", 1) for l in
                     self._psql("dst", self._d("dst", db),
                                self.OWNER_SQL).splitlines() if "|" in l)
        except RuntimeError as e:
            return [Result("deep", f"{db} ownership", "warn",
                           f"cannot read owners: {str(e).splitlines()[-1][:90]}"
                           " - unknown, not clean")]
        shared = sorted(set(a) & set(b))
        if not shared:
            return [Result("deep", f"{db} ownership", "skip",
                           "no objects present on both sides to compare")]
        changes = _own.group([(n, a[n], b[n]) for n in shared])
        if not changes:
            return [Result("deep", f"{db} ownership", "ok",
                           f"{len(shared)} objects keep their owner")]
        note = ("one substitution across every object - most likely the mover"
                " reassigning what it created"
                if _own.systematic(changes) else
                "objects drifted individually - read each one")
        return [Result("deep", f"{db} ownership", "diff",
                       f"{_own.total(changes)} of {len(shared)}:"
                       f" {_own.describe(changes)}", "",
                       f"{note}; ALTER ... OWNER TO the intended role, or the"
                       " application cannot alter its own objects")]

    def repair_plan(self, db, kind):
        actions = []
        if kind in ("sequences", "all"):
            q = ("select schemaname||'.'||sequencename||'|'||coalesce(last_value,0)"
                 " from pg_sequences"
                 " where schemaname not like '\\_\\_%'"
                 " and sequencename not like 'migkit\\_%'")
            src = dict(l.rsplit("|", 1) for l in
                       self._psql("src", db, q).splitlines() if l)
            dst = dict(l.rsplit("|", 1) for l in
                       self._psql("dst", db, q).splitlines() if l)
            owned = self._seq_owned("dst", db)
            dmax = self._seq_col_max("dst", db, owned)
            # The same column max on the source: a target sitting above its own
            # sequence is only suspicious when the source does not do it too.
            smax = self._seq_col_max("src", db, owned)
            noise = self._noise()
            stmts, undo, refuse = [], [], []
            for name, v in sorted(src.items()):
                # the mover's own bookkeeping sequences exist on one side only
                # by design; setting them on the target would fail or create
                # objects the application never asked for
                if noise and name.rpartition(".")[2].startswith(noise):
                    continue
                cur = dst.get(name)
                mx = dmax.get(name, 0)
                sv = int(v or 0)
                # target column already past source last_value = someone
                # wrote to the target; do not paper over it, surface the writer
                # It means nothing, though, when the source column sits just
                # as far past its own sequence: that is an id space the
                # application assigns itself, and refusing there leaves the
                # target sequence unset while the source carries a value - a
                # difference we introduced rather than one we found.
                if name in owned and mx > sv and mx > smax.get(name, 0):
                    refuse.append(f"{name}: target max={mx} > source last={sv}"
                                  f" and > source max={smax.get(name, 0)}"
                                  f" (writes landed on target?)")
                    continue
                # GREATEST(source, target max) can never sit below a live row,
                # so nextval is guaranteed to clear the column with no gap
                target = max(sv, mx)
                # A buffer is head-room for rows that land after this was read:
                # CDC still draining, or a write between the count and the set.
                # It costs a gap in the id space and nothing else, but it does
                # hide the fact that the leg was not actually quiet - so it is
                # off unless asked for, and the reason is written into the
                # statement rather than left for someone to work out later.
                buf = _seq_buffer()
                if target and buf:
                    target += buf
                if target == 0 or (cur is not None and int(cur) == target):
                    continue
                stmts.append(f"select setval('{name}', {target}, true);"
                             f"  -- src={sv} dstmax={mx}"
                             + (f" +buffer {buf}" if buf else "")
                             + f" dst now {cur if cur is not None else 'MISSING'}")
                if cur == "0":
                    undo.append(f"select setval('{name}', 1, false);")
                elif cur is not None:
                    undo.append(f"select setval('{name}', {cur}, true);")
            same = sum(1 for n, v in src.items() if dst.get(n) == v)
            note = f"{len(stmts)} sequences set, {same} already equal"
            if refuse:
                note += (f"; REFUSED {len(refuse)} (target ahead of source): "
                         + "; ".join(refuse[:4]))
            if stmts or refuse:
                actions.append(RepairAction(db, "sequences", stmts, undo, note))
        if kind in ("rows", "all"):
            rpt = self._report(db)
            tables = set()
            if rpt.exists():
                for f in rpt.glob("data-*.missing"):
                    tables.add(f.name[len("data-"):-len(".missing")])
                for f in rpt.glob("data-*.extra"):
                    tables.add(f.name[len("data-"):-len(".extra")])
                for f in rpt.glob("data-*.changed"):
                    tables.add(f.name[len("data-"):-len(".changed")])
            for t in sorted(tables):
                if not self._keep_tbl(db, t):
                    continue
                counts = []
                for kind_ in ("missing", "extra", "changed"):
                    f = rpt / f"data-{t}.{kind_}"
                    n = sum(1 for _ in f.open()) if f.exists() else 0
                    if n:
                        counts.append(f"{kind_}={n}")
                actions.append(RepairAction(
                    db, "rows", [f"resync-rows {t}"], [],
                    f"{t}: {', '.join(counts) or 'no pk files'},"
                    " deleted rows saved to undo before recopy"))
            # after the resync, never before it: a recopy takes its rows from
            # the source, which is where the broken text lives, so a text
            # repair applied first would be undone by the action next to it
            text = self._mojibake_repair(db)
            if text:
                actions.append(text)
        if kind in ("schema", "all"):
            # GRANT is DDL and belongs with the schema repair, so no new
            # choice is added to --kind: a plain `migkit sync --apply` now
            # restores them along with everything else
            g = self._grant_repair(db)
            if g:
                actions.append(g)
            c = self._constraint_repair(db)
            if c:
                actions.append(c)
            act = self._schema_repair_action(db)
            if act:
                actions.append(act)
        return actions

    def _mojibake_repair(self, db):
        """The row-by-row repair the deep check tells the operator to do.

        That check ends with "repair row by row, matching only the values
        that re-encode to valid UTF-8" - the hardest part of the job, handed
        back to the engineer along with the warning that the obvious
        one-liner destroys the rows that were never broken. This does it.

        It reads the **target**: migkit writes to the source nowhere, and
        the target is the copy it is answerable for. The price is real and
        is written into the note rather than discovered later - a repaired
        target no longer matches the source it came from.

        Target triggers fire for these updates, unlike a `move`, which runs
        with `session_replication_role = replica`. A handful of statements
        is not a reload, and quieting them needs a privilege an application
        role may not have; `check --deep`'s triggers line names the ones
        that would run.
        """
        cols = {}
        for line in self._psql("dst", db, self.TEXT_COLUMNS).splitlines():
            parts = line.split("\t")
            if len(parts) == 2:
                cols.setdefault(parts[0], []).append(parts[1])
        stmts, undo, touched, refused = [], [], set(), []
        repaired = clean = skipped = left = 0
        for table, columns in sorted(cols.items()):
            key = self._pk_cols_of(db, table, "dst")
            if not key:
                refused.append((table, "no primary key, so no row can be"
                                       " addressed"))
                continue
            want = [c for c in columns if c not in key]
            for c in columns:
                if c in key:
                    refused.append((f"{table}.{c}", "the key itself -"
                                    " rewriting it would move rows other"
                                    " tables point at"))
            if not want:
                continue
            sch, tbl = table.split(".", 1)
            quoted = ['"' + c.replace('"', '""') + '"' for c in key + want]
            where = " or ".join(
                f"octet_length({q}) <> char_length({q})"
                for q in quoted[len(key):])
            out = self._psql(
                "dst", db,
                f'select row_to_json(s) from (select {", ".join(quoted)}'
                f' from "{sch.replace(chr(34), chr(34) * 2)}".'
                f'"{tbl.replace(chr(34), chr(34) * 2)}"'
                f' where {where} limit {self.MOJIBAKE_REPAIR_CAP + 1}) s')
            rows = []
            for line in out.splitlines():
                if not line.strip():
                    continue
                got = json.loads(line)
                rows.append([got.get(c) for c in key]
                            + [got.get(c) for c in want])
            s, u, t, rep, cl, sk, lf = self._mojibake_updates(
                db, table, key, want, rows)
            stmts += s
            undo += u
            touched |= t
            repaired += rep
            clean += cl
            skipped += sk
            left += lf
        if not repaired:
            return None
        enabled = self._repair_text_enabled()
        note = self._mojibake_repair_note(repaired, touched, clean, skipped,
                                          left, refused, enabled)
        return RepairAction(db, "text", stmts if enabled else [],
                            undo if enabled else [], note)

    def _schema_repair_action(self, db):
        """DDL that makes the target's objects (columns, indexes, PK/FK,
        views, functions, procedures, triggers, sequences-as-objects) match
        the source, generated by atlas - the authoritative schema differ -
        with the reverse DDL captured as undo. Data is untouched; this only
        aligns structure."""
        if not hasattr(self, "migration_pair"):
            return None
        fwd, undo = self.migration_pair(db)
        if not fwd:
            return None
        return RepairAction(db, "schema", fwd.splitlines(),
                            (undo or "").splitlines(),
                            "atlas-generated DDL to align target objects to"
                            " source (review before --apply; runs in one"
                            " transaction, reverse DDL saved to undo)")

    def apply(self, db, action):
        if action.kind == "sequences":
            self._psql("dst", db,
                       "\n".join(s.split("  --")[0] for s in action.statements))
        elif action.kind == "resnapshot":
            self._apply_resnapshot(action)
        elif action.kind == "text":
            # withheld unless MIGKIT_REPAIR_TEXT says otherwise, in which
            # case the note is the whole action - applying it must do
            # nothing rather than send `begin; commit;` and report success
            if action.statements:
                self._psql("dst", db, "begin;\n"
                           + "\n".join(action.statements) + "\ncommit;")
        elif action.kind in ("schema", "grants", "constraints"):
            # one psql call: multi-statement + $$-quoted bodies apply intact,
            # and a grant set lands all-or-nothing
            blob = "begin;\n" + "\n".join(action.statements) + "\ncommit;"
            self._psql("dst", db, blob)
        elif action.kind == "rows":
            for stmt in action.statements:
                self._repair_rows_native(db, stmt.split(" ", 1)[1])
        else:
            # This used to be the row branch's `else`, so a kind nobody had
            # taught it about was read as a table name - the grant repair's
            # first run tried to look up a primary key for a relation called
            # `INSERT ON "public"."t" TO "app";`. An unknown repair must
            # refuse, not guess.
            raise RuntimeError(f"no way to apply a {action.kind!r} repair")

    def _psql_run(self, side, db, script):
        """Run a multi-statement psql script from stdin so client-side
        \\copy works (needed for the temp-pk join repair)."""
        ep = self.hop.source if side == "src" else self.hop.target
        env = tool_env({"PGPASSWORD": ep.password, "PGCONNECT_TIMEOUT": "15",
                        "PGOPTIONS": "-c statement_timeout=0"})
        p = subprocess.run(
            ["psql", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
             "-d", self._d(side, db), "-X", "-q", "-v", "ON_ERROR_STOP=1"],
            input=script, capture_output=True, text=True, env=env)
        if p.returncode:
            raise RuntimeError((p.stdout + p.stderr)[-400:])
        return p.stdout

    def _repair_rows_native(self, db, t):
        """Set-based row repair: pull the rows to (re)copy from source into a
        temp file via a temp-pk join, save the target rows they overwrite as
        undo, delete extra/changed on target, then copy the source rows in.
        session_replication_role=replica keeps FKs/triggers quiet, with a
        fallback when the target refuses it."""
        d = self._report(db)
        sch, tbl = t.split(".", 1)
        qt = f'"{sch}"."{tbl}"'
        pks = self._pk_cols_of(db, t)
        if not pks:
            return
        cols_decl = ", ".join(f"c{i + 1} text" for i in range(len(pks)))
        cond = " and ".join(f't."{pks[i]}"::text = p.c{i + 1}'
                            for i in range(len(pks)))

        def read(kind):
            f = d / f"data-{t}.{kind}"
            return f.read_text() if f.exists() else ""

        # on_conflict=keep-target preserves rows the target changed itself:
        # only fix missing (insert) and extra (delete), never overwrite a
        # differing row. source-wins (default) reconciles everything.
        keep = self.hop.options.get("on_conflict") == "keep-target"
        chg = "" if keep else read("changed")
        copy_pks = read("missing") + chg
        del_pks = read("extra") + chg
        work = Path(tempfile.mkdtemp())
        undo = d / "undo"
        undo.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d-%H%M%S")
        try:
            (work / "copy.pks").write_text(copy_pks)
            (work / "del.pks").write_text(del_pks)
            if copy_pks.strip():
                self._psql_run("src", db,
                    f"create temp table _pk ({cols_decl});\n"
                    f"\\copy _pk from '{work}/copy.pks'\n"
                    f"\\copy (select t.* from {qt} t join _pk p on {cond})"
                    f" to '{work}/rows.out'\n")
            body = (f"create temp table _pk ({cols_decl});\n"
                    f"\\copy _pk from '{work}/del.pks'\n"
                    f"\\copy (select t.* from {qt} t join _pk p on {cond})"
                    f" to '{undo}/{ts}-{tbl}.rows'\n"
                    f"delete from {qt} t using _pk p where {cond};\n")
            if (work / "rows.out").exists():
                body += f"\\copy {qt} from '{work}/rows.out'\n"
            try:
                self._psql_run("dst", db,
                               "set session_replication_role = replica;\n" + body)
            except RuntimeError as e:
                if "session_replication_role" not in str(e):
                    raise
                self._psql_run("dst", db, body)
            with (undo / "manifest.txt").open("a") as m:
                m.write(f"{ts} {t}: re-copy {undo}/{ts}-{tbl}.rows to undo\n")
        finally:
            import shutil as _sh
            _sh.rmtree(work, ignore_errors=True)

    def setup_target_plan(self, db):
        """The steps an operator runs by hand, in the order the measurements
        say they should run.

        The ordering is the whole point. A schema restored in one piece puts
        every secondary index on the table *before* the data arrives, so the
        load maintains them row by row. Measured on 1,000,000 rows with
        three secondary indexes:

            indexes already present, then load      4.48 s    275 MB
            load, then build the same indexes       2.53 s    218 MB
                                                  (0.91 + 1.62)

        Not quite twice the time, and - the part that does not go away - the
        table loaded with its indexes in place is **26% larger on disk**,
        because an index maintained one row at a time does not pack the way
        one built in a single pass does. That bloat is permanent.

        `--section=pre-data` and `--section=post-data` split the dump at
        exactly that line: measured, pre-data left the table with **0**
        indexes and post-data brought them back. post-data also carries the
        foreign keys and triggers, which is why this replaces the old advice
        to disable them by hand - they are simply not there during the load.
        """
        s, t = self.hop.source, self.hop.target
        tdb = self._d("dst", db)
        return [
            f"pg_dumpall -h {s.host} -p {s.port} -U {s.user} --globals-only"
            f" > globals.sql   # review roles, then apply on target",
            f"pg_dump -h {s.host} -p {s.port} -U {s.user} -d {db} -Fc"
            f" --schema-only -f {db}.schema.dump",
            f"createdb -h {t.host} -p {t.port} -U {t.user} {tdb}"
            f"   # match encoding/locale with source",
            f"pg_restore -h {t.host} -p {t.port} -U {t.user} -d {tdb}"
            f" --no-owner --section=pre-data {db}.schema.dump"
            f"   # tables only: no indexes, no FKs, no triggers yet",
            f"-- now load the data: migkit move {self.hop.name} --mode full"
            f" --go",
            f"pg_restore -h {t.host} -p {t.port} -U {t.user} -d {tdb}"
            f" --no-owner --section=post-data -j {max(1, int(self.hop.workers))}"
            f" {db}.schema.dump"
            f"   # indexes, FKs and triggers, built once over the loaded data",
            f"-- measured on 1M rows x 3 indexes: building them after the"
            f" load took 2.53 s against 4.48 s, and left the table 218 MB"
            f" instead of 275 MB",
            f"-- keep the pre-data dump: it is your rollback script",
        ]

    def snapshot_state(self, db, state_dir, kind="all"):
        seqs = self._psql("dst", db,
                          "select schemaname||'.'||sequencename||'|'||"
                          "coalesce(last_value,1)||'|'||(last_value is not null)"
                          " from pg_sequences"
                          " where schemaname not like '\\_\\_%'")
        (state_dir / "dst-sequences.txt").write_text((seqs + "\n") if seqs else "")
        # a sequence-only repair rolls back from the sequence snapshot alone;
        # skip the schema dump, which is slow (and can stall) on big partitioned
        # databases and buys nothing here.
        if kind == "sequences":
            return
        ep = self.hop.target
        p = run(["pg_dump", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
                 "-d", self._d("dst", db), "--schema-only", "--no-owner",
                 "--no-privileges"],
                env={"PGPASSWORD": ep.password, "PGCONNECT_TIMEOUT": "15"})
        (state_dir / "dst-schema.sql").write_text(p.stdout)

    def assess(self):
        items = []

        def add(level, scope, item, detail=""):
            items.append({"level": level, "scope": scope,
                          "item": item, "detail": str(detail)})

        items += self._brand_rows()
        src_b, dst_b = self._brands()
        sv = src_b.version or self._psql("src", "postgres",
                                         "show server_version")
        dv = dst_b.version or self._psql("dst", "postgres",
                                         "show server_version")
        items.append(self._version_row(sv, dv))

        # read replica (pg_is_in_recovery=t) rejects writes incl SELECT FOR
        # UPDATE (25006): fatal as a target, ok-for-checks as a source
        src_ro = self._in_recovery("src", "postgres")
        dst_ro = self._in_recovery("dst", "postgres")
        add("warn" if src_ro else "pass", "instance",
            "source endpoint role",
            "READ REPLICA (read-only) - it writes nothing, so it is the"
            " only way to verify a frozen source, but a replica that has not"
            " replayed shows fewer rows than the target and reads as"
            " rows-extra. Measured: a paused standby produced"
            " `kind=rows-extra by=5` against a primary five rows ahead. The"
            " data check names the replica when it reports one; point at the"
            " writer/cluster endpoint for replication and the LSN fence"
            if src_ro else "primary / writer (writable)")
        add("fail" if dst_ro else "pass", "instance",
            "target endpoint is writable (not a read replica)",
            "READ REPLICA (read-only) - cannot migrate or repair into it,"
            " and apps get SQLSTATE 25006 on SELECT FOR UPDATE; use the"
            " writer/cluster endpoint" if dst_ro else "primary / writer")

        wal = self._psql("src", "postgres", "show wal_level")
        add("pass" if wal == "logical" else "fail", "instance",
            "wal_level=logical on source (required for CDC)", wal)
        slots = self._psql("src", "postgres",
                           "select count(*)||' used / '||"
                           "current_setting('max_replication_slots')||' max'"
                           " from pg_replication_slots")
        add("pass", "instance", "replication slots", slots)

        # a lagging or abandoned slot pins WAL and can silently fill the source
        # disk (source outage mid-migration). wal_status is pg13+, so probe it
        # and fall back cleanly on older majors.
        try:
            rows = [l for l in self._psql("src", "postgres",
                    "select slot_name||'|'||active||'|'"
                    "||coalesce(wal_status,'?')||'|'||coalesce("
                    "pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(),"
                    " restart_lsn)),'?') from pg_replication_slots"
                    ).splitlines() if l]
            for r in rows:
                name, active, status, retained = r.split("|")
                if status == "lost":
                    add("fail", "instance", f"replication slot '{name}'",
                        f"WAL LOST - replication broken, retained {retained}")
                elif active in ("f", "false"):
                    add("warn", "instance", f"replication slot '{name}'",
                        "INACTIVE (no consumer) - pins WAL, retained"
                        f" {retained}; drop it if abandoned")
                elif status in ("unreserved", "extended"):
                    add("warn", "instance", f"replication slot '{name}'",
                        f"wal_status {status}, retained {retained} -"
                        " approaching the retention limit")
            if rows:
                mk = self._psql("src", "postgres",
                                "show max_slot_wal_keep_size")
                if mk.strip() in ("-1", "-1B"):
                    add("warn", "instance", "max_slot_wal_keep_size",
                        "-1 (unbounded): an inactive slot can fill the disk;"
                        " set a limit so a stalled consumer can't take the"
                        " source down")
        except RuntimeError:
            pass
        lrt = self._psql("src", "postgres",
                         "select count(*) from pg_stat_activity where"
                         " xact_start is not null"
                         " and now() - xact_start > interval '10 minutes'")
        add("pass" if lrt == "0" else "warn", "instance",
            "transactions open longer than 10 minutes", lrt)

        # pg_authid has the real hash (pg_roles masks it); needs superuser,
        # fall back to name-only when blocked
        roles_q = ("select rolname||'|'||coalesce(rolpassword,'')"
                   " from pg_authid"
                   " where rolcanlogin"
                   " and rolname not like 'pg\\_%'"
                   " and rolname not like 'rds%'"
                   " and rolname not like '%tencent%' order by 1")
        try:
            src_pairs = dict(l.split("|", 1) for l in
                             self._psql("src", "postgres", roles_q).splitlines()
                             if l)
            dst_pairs = dict(l.split("|", 1) for l in
                             self._psql("dst", "postgres", roles_q).splitlines()
                             if l)
        except RuntimeError:
            # no pg_authid access (managed service): fall back to name-only
            names_q = roles_q.replace(
                "||'|'||coalesce(rolpassword,'')", "")
            names_q = ("select rolname from pg_roles r"
                       " where rolcanlogin"
                       " and r.rolname not like 'pg\\_%'"
                       " and r.rolname not like 'rds%'"
                       " and r.rolname not like '%tencent%' order by 1")
            src_pairs = {n: "" for n in
                         self._psql("src", "postgres", names_q).splitlines()}
            dst_pairs = {n: "" for n in
                         self._psql("dst", "postgres", names_q).splitlines()}
        sa, da = set(src_pairs), set(dst_pairs)
        miss = sorted(sa - da)
        add("pass" if not miss else "warn", "instance",
            "login roles present on target",
            f"{len(sa)} src / {len(da)} dst"
            + (f", missing: {', '.join(miss[:5])}" if miss else ""))
        # role on both sides but hash differs = DTS carried the user, not the
        # password -> apps can't authenticate on target
        drift = sorted(n for n in (sa & da)
                       if src_pairs[n] and dst_pairs[n]
                       and src_pairs[n] != dst_pairs[n])
        if any(src_pairs.values()):
            add("pass" if not drift else "fail", "instance",
                "role passwords match source (login will work on target)",
                "all match" if not drift
                else f"password differs for: {', '.join(drift[:5])}"
                     " -> reset on target or apps cannot log in")

        try:
            avail = set(self._psql("dst", "postgres",
                                   "select name from pg_available_extensions")
                        .splitlines()) - {""}
        except RuntimeError:
            # an empty set here must not read as "nothing is missing"; the
            # inventory turns it into unknown rather than a clean answer
            avail = set()
        for db in self.databases():
            try:
                exts = set(self._psql("src", db,
                                      "select extname from pg_extension")
                           .splitlines())
                add("pass", db, "extensions on source", f"{len(exts)}")
                invalid = self._psql("src", db,
                                     "select count(*) from pg_index"
                                     " where not indisvalid")
                add("pass" if invalid == "0" else "warn", db,
                    "invalid indexes on source", invalid)
                enc_q = ("select pg_encoding_to_char(encoding)||' '||datcollate"
                         f" from pg_database where datname = '{db}'")
                se = self._psql("src", "postgres", enc_q)
                try:
                    de = self._psql("dst", "postgres", enc_q)
                except RuntimeError:
                    de = ""
                if not de:
                    add("warn", db, "database exists on target", "missing")
                else:
                    add("pass" if se == de else "fail", db,
                        "encoding and collation match", f"src {se} / dst {de}")
            except RuntimeError as e:
                add("fail", db, "assess queries", str(e).splitlines()[-1][:120])
        hw = self._handwork(avail)
        items += hw.rows() + hw.summary()
        items += self._mover_leftovers()
        items += self._client_tool_versions(
            ("pg_dump", "pg_restore", "psql"), dv)
        # the deep checks that are about the move ahead rather than the one
        # behind. They live in one implementation and are read from two
        # places; this is the earlier one, which is the one that can still
        # change what an operator does.
        items += self._preflight_items()
        return items

    def _mover_leftovers(self):
        """What a mover added to the source and did not take away.

        migkit's own playbook has said for a long time to remove these after
        cutover. Saying it in prose is a note to remember something; this
        counts them. A replication slot in particular is not untidiness - it
        pins WAL, and the source can run out of disk long after everybody has
        moved on.
        """
        from .. import leftovers as _lo
        found, items = [], []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "source leftovers",
                          "item": item, "detail": str(detail)})
        try:
            for name in self._psql("src", "postgres",
                                   "select slot_name from pg_replication_slots"
                                   ).splitlines():
                if name:
                    found.append(("slot", name))
        except RuntimeError as e:
            add("warn", "cannot list replication slots on the source",
                f"{str(e).splitlines()[-1][:90]} - unknown, not clean")
        for db in self.databases():
            try:
                for q, kind in (
                        ("select nspname from pg_namespace", "schema"),
                        ("select pubname from pg_publication", "publication"),
                        ("select evtname from pg_event_trigger",
                         "event trigger"),
                        ("select extname from pg_extension", "extension")):
                    for name in self._psql("src", db, q).splitlines():
                        if name:
                            found.append((kind, name))
            except RuntimeError as e:
                add("warn", f"cannot list objects in {db}",
                    f"{str(e).splitlines()[-1][:90]} - unknown, not clean")
        by = _lo.group(found)
        if not by:
            add("pass", "no mover artifacts left in the source",
                f"{len(found)} objects examined")
            return items
        for label, why in _lo.urgent(by):
            add("fail", f"{label} is still active on the source", why)
        add("warn", f"{_lo.total(by)} mover artifacts left in the source",
            _lo.describe(by) + ". Drop them once every leg that used them is"
            " finished - they are on the source, so nothing on the target"
            " tells you they are there")
        return items

    def _handwork(self, avail):
        """Inventory the work the move will not do, per `migkit.handwork`.

        Functions, views and constraints are absent on purpose: the structural
        check diffs them and emits DDL for them, so they are carried. What is
        listed here is what no amount of DDL fixes - contents that live outside
        any table, objects the WAL stream never mentions, and things that have
        to exist on the target before anything is loaded into it.
        """
        from .. import handwork
        inv = handwork.Inventory()
        for db in self.databases():
            USER_SCHEMA = ("n.nspname not in"
                           " ('pg_catalog','information_schema')"
                           " and n.nspname not like 'pg\\_%'"
                           " and n.nspname not like '\\_\\_%'")
            try:
                exts = set(self._psql("src", db,
                                      "select extname from pg_extension")
                           .splitlines()) - {""}
                inv.add("target-prereq", db,
                        "extensions not installable on the target",
                        sorted(exts - avail) if avail else [])
                if not avail:
                    inv.unknown("target-prereq", db,
                                "could not read pg_available_extensions on"
                                " the target")

                # No primary key means logical replication has no REPLICA
                # IDENTITY to match an UPDATE or DELETE against, so those
                # changes are dropped - and migkit cannot name a differing
                # row either, only the whole table.
                nopk = [l for l in self._psql("src", db, f"""
                    select n.nspname||'.'||c.relname
                    from pg_class c
                    join pg_namespace n on n.oid = c.relnamespace
                    where c.relkind = 'r' and {USER_SCHEMA}
                      and not exists (select 1 from pg_index i
                        where i.indrelid = c.oid and i.indisprimary)
                      and c.relreplident not in ('f','i')
                    order by 1""").splitlines() if l]
                inv.add("no-row-key", db, "tables", nopk)

                # Unlogged tables produce no WAL at all, so a WAL-based mover
                # cannot see a single row of them.
                unlogged = [l for l in self._psql("src", db, f"""
                    select n.nspname||'.'||c.relname from pg_class c
                    join pg_namespace n on n.oid = c.relnamespace
                    where c.relkind = 'r' and c.relpersistence = 'u'
                      and {USER_SCHEMA} order by 1""").splitlines() if l]
                inv.add("not-carried", db, "unlogged tables", unlogged)

                # A matview's definition is carried by the structural fix; its
                # contents are not, and it is readable-but-stale until
                # someone refreshes it.
                mviews = [l for l in self._psql("src", db, f"""
                    select n.nspname||'.'||c.relname from pg_class c
                    join pg_namespace n on n.oid = c.relnamespace
                    where c.relkind = 'm' and {USER_SCHEMA}
                    order by 1""").splitlines() if l]
                inv.add("not-carried", db,
                        "materialized views needing REFRESH", mviews)

                # Large objects live outside every table, so a mover that
                # works table by table never sees them - which is most
                # managed services. migkit's own pg_dump path is not one of
                # them: measured, `pg_dump -Fd -j --data-only` (the exact
                # flags `pgdump_move` uses) puts them in the dump and
                # pg_restore lands them. So this is a finding about the leg,
                # not about the objects, and the wording says which.
                los = self._psql("src", db,
                                 "select count(*) from"
                                 " pg_largeobject_metadata").strip()
                inv.add("not-carried", db,
                        "large objects (a managed service moving table by"
                        " table leaves them; migkit's own pg_dump path"
                        " carries them)",
                        [f"{los} in pg_largeobject"] if los not in
                        ("0", "") else [])

                # A user mapping's password is not readable by anyone but its
                # owner, so it cannot be copied even in principle.
                fdw = [l for l in self._psql("src", db,
                       "select srvname||' -> '||usename from pg_user_mappings"
                       ).splitlines() if l]
                inv.add("target-prereq", db,
                        "foreign server user mappings (passwords are not"
                        " readable, so they cannot be copied)", fdw)

                # An untrusted language needs its runtime installed on the
                # target host; no DDL can supply it.
                langs = [l for l in self._psql("src", db,
                         "select lanname from pg_language"
                         " where lanispl and not lanpltrusted"
                         ).splitlines() if l]
                inv.add("target-prereq", db,
                        "untrusted procedural languages", langs)
            except RuntimeError as e:
                inv.unknown("no-row-key", db,
                            f"catalogue query failed:"
                            f" {str(e).splitlines()[-1][:100]}")
        return inv

    def _pk_cols_of(self, db, table, side="src"):
        sch, tbl = table.split(".", 1)
        cols = self._psql(side, db,
            "select a.attname from pg_index i"
            " join pg_attribute a on a.attrelid = i.indrelid"
            " and a.attnum = any(i.indkey)"
            f" where i.indrelid = '\"{sch}\".\"{tbl}\"'::regclass"
            " and i.indisprimary"
            " order by array_position(i.indkey, a.attnum)").splitlines()
        return [c for c in cols if c]

    #: The key files are read back with psql's `\copy`, which parses COPY
    #: text: tabs separate columns and a backslash starts an escape. Writing
    #: the raw value meant the reader and the writer disagreed about both.
    #: Measured against PostgreSQL 16 on a text primary key:
    #:
    #:     key `has<TAB>tab`    ERROR: extra data after last expected column
    #:                          COPY _pk, line 2: "has	tab"   - nothing repaired
    #:     key `back\slash`     apply returned with no error at all, the row
    #:                          was not restored, and the next check still
    #:                          said diff. `\s` is not an escape COPY knows,
    #:                          so it read `backslash` and the join matched
    #:                          no row - a repair reported as done that was
    #:                          never performed, which is the worst shape a
    #:                          failure can take here.
    #:
    #: So every key this engine renders is escaped the way COPY would have
    #: written it, and anything reading one back undoes exactly that.
    PK_ESCAPE = (r"replace(replace(replace(replace({0}, E'\\', E'\\\\'),"
                 r" E'\t', E'\\t'), E'\n', E'\\n'), E'\r', E'\\r')")

    @classmethod
    def _pk_text_expr(cls, cols):
        """SQL for one row's key: each column escaped, then tab-joined."""
        return ("concat_ws(e'\\t', " + ", ".join(
            cls.PK_ESCAPE.format(f'"{c}"::text') for c in cols) + ")")

    @staticmethod
    def _pk_unescape(text):
        """One column's value back from the form `_pk_text_expr` wrote.

        A backslash before anything COPY does not recognise is dropped and
        the character kept, which is what PostgreSQL itself does - so a file
        written by an older migkit is read the way that migkit's repair read
        it, rather than differently.
        """
        known = {"t": "\t", "n": "\n", "r": "\r", "\\": "\\", "b": "\b",
                 "f": "\f", "v": "\v"}
        out = []
        i = 0
        while i < len(text):
            if text[i] == "\\" and i + 1 < len(text):
                out.append(known.get(text[i + 1], text[i + 1]))
                i += 2
            else:
                out.append(text[i])
                i += 1
        return "".join(out)

    @classmethod
    def _pk_parts(cls, key):
        """The original column values of one encoded key."""
        return [cls._pk_unescape(p) for p in key.split("\t")]

    def _compare_pks(self, db, table, keys):
        """Row-level compare of the given pk keys (tab-joined when
        composite). Returns (missing, extra, changed) or None when the
        table has no pk."""
        cols = self._pk_cols_of(db, table)
        if not cols:
            return None
        sch, tbl = table.split(".", 1)

        def esc(v):
            return "'" + v.replace("'", "''") + "'"

        def fetch(side):
            out = {}
            klist = sorted(keys)
            for i in range(0, len(klist), 500):
                chunk = klist[i:i + 500]
                if len(cols) == 1:
                    inlist = ", ".join(esc(self._pk_unescape(k))
                                       for k in chunk)
                    where = f'"{cols[0]}"::text in ({inlist})'
                else:
                    tup = ", ".join(f'"{c}"::text' for c in cols)
                    vals = ", ".join(
                        "(" + ", ".join(esc(p) for p in self._pk_parts(k))
                        + ")" for k in chunk)
                    where = f"({tup}) in ({vals})"
                pk = self._pk_text_expr(cols)
                h = self._row_hash_expr("src", db, f"{sch}.{tbl}")
                q = (f"select {pk}||'|'||{h}"
                     f' from "{sch}"."{tbl}" t where {where}')
                for line in self._psql(side, db, q).splitlines():
                    k, _, h = line.rpartition("|")
                    out[k] = h
            return out

        src, dst = fetch("src"), fetch("dst")
        missing = sorted(k for k in keys if k in src and k not in dst)
        extra = sorted(k for k in keys if k in dst and k not in src)
        changed = sorted(k for k in keys
                         if k in src and k in dst and src[k] != dst[k])
        return missing, extra, changed

    def _write_pk_files(self, db, table, missing, extra, changed):
        d = self._report(db)
        d.mkdir(parents=True, exist_ok=True)
        for kind, rows in (("missing", missing), ("extra", extra),
                           ("changed", changed)):
            f = d / f"data-{table}.{kind}"
            if rows:
                f.write_text("\n".join(rows) + "\n")
            elif f.exists():
                f.unlink()

    def settle_recheck(self, db, table):
        d = self._report(db)
        keys = set()
        for k in ("missing", "extra", "changed"):
            f = d / f"data-{table}.{k}"
            if f.exists():
                keys |= set(f.read_text().splitlines())
        keys.discard("")
        if not keys or len(keys) > 20000:
            return None
        cmp = self._compare_pks(db, table, keys)
        if cmp is None:
            return None
        missing, extra, changed = cmp
        self._write_pk_files(db, table, missing, extra, changed)
        return len(missing), len(extra), len(changed)

    # --- consistency by design: any consumer (incl. DTS) holds a slot on
    # the source, so "target applied past LSN X" is observable there ---

    def _psql_script(self, side, db, sql):
        ep = self.hop.source if side == "src" else self.hop.target
        env = tool_env({"PGPASSWORD": ep.password, "PGCONNECT_TIMEOUT": "15",
                        "PGOPTIONS": "-c TimeZone=UTC -c DateStyle=ISO"
                                     " -c statement_timeout=0"
                                     " -c extra_float_digits=3"})
        return subprocess.Popen(
            ["psql", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
             "-d", self._d(side, db), "-X", "-At", "-q", "-v",
             "ON_ERROR_STOP=1"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, env=env), sql

    PK_COLS_SQL = (
        "select a.attname from pg_index i"
        " join pg_class c on c.oid = i.indrelid"
        " join pg_namespace n on n.oid = c.relnamespace"
        " join pg_attribute a on a.attrelid = c.oid"
        "   and a.attnum = any(i.indkey)"
        " where i.indisprimary and n.nspname = %s and c.relname = %s"
        " order by a.attnum")

    def _key_hash_expr(self, side, db, table):
        """Expression hashing only the primary key of one row, or None.

        A single row hash says "this table differs" and nothing else, so
        finding out what kind of difference it is costs another pass. Hashing
        the key separately in the same scan answers that for free:

          keys equal, values differ   -> rows were modified in place
          keys differ, counts equal   -> rows were replaced
          counts differ               -> rows are missing or extra

        Those three lead to completely different remedies, and knowing which
        one it is before drilling down is the point. It does not replace the
        drilldown - that is still how you learn *which* rows - it removes the
        need for one in order to learn *what happened*.
        """
        sch, tbl = table.split(".", 1)
        try:
            out = self._psql(side, db, self.PK_COLS_SQL.replace("%s", "{}")
                             .format(f"'{sch}'", f"'{tbl}'"))
        except Exception:
            return None
        cols = [c for c in out.splitlines() if c]
        if not cols:
            return None
        # Joined with chr(2) and standing in for NULL with chr(1), this had
        # the same ambiguity the MySQL row hash was proved to have: measured,
        # `coalesce(chr(1), chr(1)) = coalesce(null, chr(1))` is true, so a
        # key holding chr(1) hashed as a NULL. Unlikely in a primary key and
        # exactly as wrong, so it uses the same encoding as everything else.
        from .. import rowtext
        return f"md5({rowtext.postgres_row(cols)})"

    def _row_hash_expr(self, side=None, db=None, table=None):
        """The one expression that hashes one row of `t`. Used everywhere.

        Built from named columns in name-sorted order, never from the
        whole-row cast, for two measured reasons.

        **Column order.** `t::text` renders columns in physical attribute
        order, so a source whose history includes a DROP COLUMN and an ADD
        COLUMN hashes differently from a target created fresh from the same
        logical schema - identical values, different hash, reported as a
        difference nobody can act on. Measured on PostgreSQL 16: the two sides
        gave -3370828463729857743 and -4255070870107345456 for rows that
        compare equal column by column. Naming the columns removes the
        dependence entirely, and it is what the MySQL engine has always done.

        **PostGIS.** `t::text` renders a geometry as hex EWKB, and that
        encoding is not stable across PostGIS patch releases - two servers
        holding the identical shape produce different bytes. Geometry columns
        are rebuilt as canonical text so the shape is compared, not its
        encoding.

        The jsonb variant uses `jsonb_build_object` rather than wrapping a
        `ROW`: `to_jsonb(ROW(a, b))` throws the column names away and yields
        `{"f1": ..., "f2": ...}`, which would make a renamed column hash the
        same as the original - a check that is quieter than the truth.
        `jsonb_build_object` was measured to produce output byte-identical to
        `to_jsonb(t)`, and jsonb sorts its keys, so it is order-independent
        for free.
        """
        jsonb = self.hop.options.get("checksum", "text") == "jsonb"
        cols = self._row_cols(side, db, table)
        if not cols:
            # No catalogue to read - the whole-row cast is all that is left.
            # It is order-dependent, which is the defect described above, so
            # every caller that can name a table should pass one.
            return "md5(to_jsonb(t)::text)" if jsonb else "md5(t::text)"

        def val(c, kind):
            if kind == "geometry":
                return f'ST_AsEWKT(t."{c}")'
            if kind == "geography":
                return f'ST_AsEWKT(t."{c}"::geometry)'
            return f't."{c}"'

        if jsonb:
            args = ", ".join(f"'{c}', {val(c, kind)}" for c, kind in cols)
            return f"md5(jsonb_build_object({args})::text)"
        return "md5(ROW(" + ", ".join(val(c, kind)
                                      for c, kind in cols) + ")::text)"

    def _row_cols(self, side, db, table):
        """[(column, 'geometry'|'geography'|None)] sorted by column name.

        Sorted by name rather than by attribute number so the expression is
        the same whichever side produced it - the checkpoint keys its stored
        partials on the expression text, so an expression that varied by side
        would throw away resumable work for no reason.
        """
        if not (side and db and table):
            return None
        key = (side, db, table)
        cache = getattr(self, "_col_cache", None)
        if cache is None:
            cache = self._col_cache = {}
        if key in cache:
            return cache[key]
        sch, tbl = self._split(table)
        rows = self._psql(side, db,
            "select a.attname||'|'||coalesce(t.typname,'') "
            "from pg_attribute a "
            "join pg_class c on c.oid = a.attrelid "
            "join pg_namespace n on n.oid = c.relnamespace "
            "left join pg_type t on t.oid = a.atttypid "
            f"where n.nspname = '{sch}' and c.relname = '{tbl}' "
            "and a.attnum > 0 and not a.attisdropped "
            "order by a.attname").splitlines()
        parsed = [(r.split("|")[0], r.split("|")[1]) for r in rows if r]
        out = [(c, ty if ty in ("geometry", "geography") else None)
               for c, ty in parsed]
        cache[key] = out or None
        return cache[key]

    @staticmethod
    def _snapshot_preamble(snapshot, workers):
        """The statements a worker runs before it reads anything.

        `SET TRANSACTION SNAPSHOT` has to be the first statement of a
        transaction that has not read yet, so the order here is not
        cosmetic: begin, adopt the snapshot, then tune. Putting the `set
        local max_parallel_workers_per_gather` first makes PostgreSQL
        refuse the snapshot.
        """
        lines = ["begin transaction isolation level repeatable read"
                 " read only;"]
        if snapshot:
            lines.append(f"set transaction snapshot '{snapshot}';")
        lines.append(f"set local max_parallel_workers_per_gather = {workers};")
        return lines

    def _export_snapshot(self, side, db):
        """Open a transaction, export its snapshot, and hand back both.

        Returns `(connection, id)` or `(None, None)`. The connection is the
        point: an exported snapshot lives exactly as long as the
        transaction that exported it, so the caller has to hold it open
        while the workers use it and close it after.

        **What it buys, measured.** migkit's fast data pass reads tables
        through a thread pool, one connection each, so table `a` is read at
        one instant and table `b` at another - and a transaction that moves
        a row between them in that gap shows up as a difference in both.
        On a live server, with a writer inserting between the export and
        the reads:

            workers not sharing the snapshot    a=2  b=2
            workers sharing the snapshot        a=1  b=1

        `--consistent` already avoids this by reading every table of a side
        in one transaction, which costs the parallelism. A shared snapshot
        is how both are had at once.
        """
        try:
            conn = self._conn(side, self._d(side, db))
        except Exception:
            # a server that cannot be reached, which is the caller's cue to
            # fall back. Deliberately narrow: this used to wrap the call
            # itself in a bare `except Exception` and a wrong argument list
            # came back as "no snapshot available" instead of a TypeError.
            return None, None
        try:
            conn.autocommit = False
            cur = conn.cursor()
            cur.execute("begin transaction isolation level repeatable read"
                        " read only")
            cur.execute("select pg_export_snapshot()")
            got = cur.fetchone()
            return (conn, got[0]) if got else (conn, None)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return None, None

    def _snapshot_scripts(self, side, db, tables, snapshot, lanes, workers):
        """One script per lane, every lane reading the shared snapshot.

        The LSN is asked for once, by the first lane, because it is the
        fence for convergence proofs and two lanes reporting two positions
        would leave the caller to pick one. Every lane runs the same
        preamble, so the instant is identical whichever lane a table lands
        in - which is the only reason splitting them is safe.
        """
        lanes = max(1, min(lanes, len(tables)))
        buckets = [tables[i::lanes] for i in range(lanes)]
        out = []
        for n, bucket in enumerate(buckets):
            if not bucket:
                continue
            lines = self._snapshot_preamble(snapshot, workers)
            if n == 0:
                lines.append(
                    "select 'LSN|'||case when pg_is_in_recovery() then"
                    " 'standby (read replica, no fence)' else"
                    " pg_current_wal_lsn()::text end;")
            for t in bucket:
                sch, tbl = t.split(".", 1)
                h = self._row_hash_expr("src", db, t)
                lines.append(
                    f"select '{t}|'||count(*)||'|'||coalesce(sum(('x'||"
                    f"substr({h},1,16))::bit(64)::bigint::numeric), 0)"
                    f' from "{sch}"."{tbl}" t;')
            lines.append("commit;")
            out.append("\n".join(lines))
        return out

    def _fast_consistent(self, db):
        """Whole-database checksum inside ONE repeatable-read read-only
        transaction per side: no intra-db skew (every table of a side is
        the same instant), and the src LSN captured in-snapshot gives the
        fence for convergence proofs."""
        st = [l for l in self._psql("src", db,
                                    self.USER_TABLES).splitlines() if l]
        dt = set(l for l in self._psql("dst", db,
                                       self.USER_TABLES).splitlines() if l)
        both = [t for t in st if t in dt]
        w = int(self.hop.options.get("checksum_workers", 8))
        def script(side):
            lines = ["begin transaction isolation level repeatable read"
                     " read only;",
                     f"set local max_parallel_workers_per_gather = {w};",
                     "select 'LSN|'||case when pg_is_in_recovery() then"
                     " 'standby (read replica, no fence)' else"
                     " pg_current_wal_lsn()::text end;"]
            for t in both:
                sch, tbl = t.split(".", 1)
                # named from the source for both sides, so the two scripts
                # hash the same columns in the same order whatever order the
                # two servers store them in
                h = self._row_hash_expr("src", db, t)
                lines.append(
                    f"select '{t}|'||count(*)||'|'||coalesce(sum(('x'||"
                    f"substr({h},1,16))::bit(64)::bigint::numeric), 0)"
                    f' from "{sch}"."{tbl}" t;')
            lines.append("commit;")
            return "\n".join(lines)

        # One transaction per side is what makes this consistent, and it is
        # also what made it serial: every table of a side queued behind the
        # one before it. An exported snapshot lets several connections read
        # that same instant at the same time, so consistency stops costing
        # the parallelism. Failing to export falls back to the single
        # script rather than reading without one - an inconsistent
        # "consistent" pass is the answer this mode exists to avoid.
        lanes = max(1, int(self.hop.workers or 1))
        outs, holders = {}, []
        try:
            for side in ("src", "dst"):
                conn, snap = (self._export_snapshot(side, db)
                              if lanes > 1 and len(both) > 1 else (None, None))
                if conn is not None:
                    holders.append(conn)
                scripts = ([script(side)] if not snap
                           else self._snapshot_scripts(side, db, both, snap,
                                                       lanes, w))
                procs = [self._psql_script(side, db, sc) for sc in scripts]
                text = []
                for proc, sql in procs:
                    stdout, stderr = proc.communicate(sql)
                    if proc.returncode:
                        return 1, (f"consistent pass failed on {side}:"
                                   f" {stderr[-300:]}")
                    text.append(stdout)
                outs[side] = "\n".join(text)
        finally:
            for conn in holders:
                try:
                    conn.rollback()
                    conn.close()
                except Exception:
                    pass

        def parse(text):
            lsn, rows = "", {}
            for line in text.splitlines():
                name, _, rest = line.partition("|")
                if name == "LSN":
                    lsn = rest
                elif name:
                    rows[name] = rest
            return lsn, rows

        src_lsn, src_rows = parse(outs["src"])
        dst_lsn, dst_rows = parse(outs["dst"])
        out = [f"consistent snapshot: one repeatable-read txn per side,"
               f" src lsn={src_lsn} dst lsn={dst_lsn}"]
        rc = 0
        for t in both:
            a, b = src_rows.get(t, ""), dst_rows.get(t, "")
            if a == b:
                out.append(f"{t}: OK rows={a.split('|')[0]}"
                           f" checksum={a.split('|', 1)[1]}")
            else:
                rc = 1
                out.append(f"{t}: DIFF src={a} dst={b}")
        for t in st:
            if t not in dt:
                rc = 1
                out.append(f"{t}: ERROR missing on target")
        return rc, "\n".join(out)

    def _in_recovery(self, side, db="postgres"):
        """True if this endpoint is a standby / read replica (read-only).
        Aurora and RDS readers answer pg_is_in_recovery() = t and reject
        every write, including SELECT ... FOR UPDATE (SQLSTATE 25006)."""
        try:
            return self._psql(side, db, "select pg_is_in_recovery()") == "t"
        except RuntimeError:
            return False

    def src_lsn(self, db):
        # WAL funcs are unavailable during recovery; no source LSN, no fence
        if self._in_recovery("src", db):
            return None
        return self._psql("src", db, "select pg_current_wal_lsn()")

    def follow_origin(self, db):
        """The replication origin migkit gives a locally-driven CDC leg.

        pgcopydb's default is the bare name `pgcopydb`, which says nothing
        about which hop or which database it belongs to - and origins are
        cluster-wide, so two databases following at once would share one
        name and overwrite each other's position. Naming it per hop and
        database is what lets `applied_lsn` attribute it.
        """
        raw = f"migkit_{self.hop.name}_{db}".replace("-", "_")
        return "".join(c if c.isalnum() or c == "_" else "_" for c in raw)[:63]

    def follow_slot(self, db):
        """The replication slot that goes with `follow_origin`.

        Same name, lower-cased, because the two namespaces do not accept
        the same characters. Asked of PostgreSQL 16 rather than assumed:

            select pg_create_logical_replication_slot('migkit_Prod_EU_db', ...)
            ERROR:  replication slot name "migkit_Prod_EU_db" contains an
                    invalid character
            HINT:  Replication slot names may only contain lower case
                   letters, numbers, and the underscore character.

        A hop named `Prod-EU` would otherwise build a name the server
        refuses, at the moment the CDC leg starts and not before.
        """
        return self.follow_origin(db).lower()

    #: origins on the target that belong to the database being fenced.
    #: `pg_replication_origin_status` is cluster-wide: connected to one
    #: database it also lists every other one's. Measured on PostgreSQL 16 -
    #: from `postgres`, with the only subscription living in `other`:
    #:
    #:     external_id | remote_lsn
    #:     pgcopydb    | 0/15B9810
    #:     pg_16407    | 0/0
    #:
    #: A fence taking `min(remote_lsn)` across that would wait on a
    #: subscription for a database nobody asked about, parked at `0/0`
    #: because it was created a minute ago, and would never pass. A
    #: subscription is attributable through `pg_subscription.subdbid`; an
    #: origin migkit made is attributable because migkit named it.
    _ORIGIN_ROWS = """
          select o.remote_lsn
            from pg_replication_origin_status o
            join pg_subscription s on o.external_id = 'pg_' || s.oid
           where s.subdbid = (select oid from pg_database
                               where datname = current_database())
          union all
          select o.remote_lsn
            from pg_replication_origin_status o
           where o.external_id = '{origin}'"""

    def _origin_rows(self, db):
        return self._ORIGIN_ROWS.format(origin=self.follow_origin(db))

    def applied_lsn(self, db):
        """The source LSN the *target* has actually applied, or None.

        The source's `confirmed_flush_lsn` is what the consumer said; this
        is what the target did. Both native logical replication and
        pgcopydb write `pg_replication_origin_status` inside the applying
        transaction, so for either path it means "committed here", and the
        two numbers disagree in both directions - measured on PostgreSQL 16
        on one pair under the same insert load:

            native subscription   origin *ahead* of the slot by up to 46 KB
                                  (the slot is a delayed echo of the apply)
            pgcopydb follow       slot ahead of the origin while the target
                                  still held 0 rows

        Read `fence_wait` before reaching for this as a fence: it cannot be
        one. The origin stops at the last applied transaction while the
        source's WAL keeps moving, so `origin >= some current LSN` never
        becomes true on a quiet database.

        What it is good for is saying where the target actually is - in a
        report, and to a CDC leg migkit drives itself, which needs a
        position that is not the mover's own opinion of its progress.
        """
        q = ("select coalesce(min(o.remote_lsn)::text, '') from ("
             + self._origin_rows(db) + ") o")
        try:
            got = self._psql("dst", self._d("dst", db), q)
        except RuntimeError:
            return None
        got = got.strip()
        # `0/0` is the absence of a position, not a position. A subscription
        # that has copied its tables but not yet applied a *streamed*
        # transaction sits there - measured: `pg_stat_subscription` showed
        # received_lsn 0/19EB3E8 and the rows were on the target, while the
        # origin still read 0/0; one insert on the source moved it to
        # 0/19EB540. Reading that as "applied nothing" would stall the fence
        # on a database whose only fault is being idle, so it hands back to
        # the slot, which does carry a position for that subscription.
        return None if not got or got == "0/0" else got

    def fence_wait(self, db, lsn, timeout=300):
        """Block until every active replication consumer of this db has
        confirmed flushing past `lsn`. True = fence passed, False = timed
        out, None = no slot visible / no source LSN (cannot fence).

        This reads the *source's* slot and not `applied_lsn`, and that is
        deliberate rather than an oversight. `applied_lsn` is the better
        number in the sense that it means "committed on the target" - but it
        can only ever be the LSN of the last transaction that was applied,
        so on a quiet database it stops and the source's WAL keeps moving.
        Measured on PostgreSQL 16, one healthy subscription, no writes, five
        samples three seconds apart:

            pg_current_wal_lsn   0/19EB660
            confirmed_flush_lsn  0/19EB660   (keepalives carry it to the end)
            origin remote_lsn    0/19EB540   (288 bytes back, and staying)

        A fence of `origin >= lsn`, or of `origin >= slot`, never passes
        there. It would time out on every idle pair and fall through to the
        weaker sleep-settle path without saying so - a fence that quietly
        stops fencing. The gap is also not a fault signal on its own: it
        looks the same as a consumer holding changes it has not applied,
        which is what `check`'s row comparison is for.
        """
        if lsn is None:
            return None
        if self._psql("src", db,
                      "select count(*) from pg_replication_slots"
                      f" where database = '{db}' and active") == "0":
            return None
        q = ("select coalesce(min(confirmed_flush_lsn), '0/0'::pg_lsn)"
             f" >= '{lsn}'::pg_lsn from pg_replication_slots"
             f" where database = '{db}' and active")
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            if self._psql("src", db, q) == "t":
                return True
            time.sleep(2)
        return False

    def fenced_recheck(self, db, table, keys):
        """Convergence proof for suspect rows: capture src LSN, wait for
        the fence, re-compare. Two rounds ride out rows that stay hot;
        what survives is a real diff, not replication in flight.
        Returns (missing, extra, changed, proof) or None if unfenceable."""
        proof = []
        for rnd in (1, 2):
            lsn = self.src_lsn(db)
            ok = self.fence_wait(db, lsn, timeout=int(
                self.hop.options.get("fence_timeout", 300)))
            if ok is None:
                return None
            proof.append(f"round {rnd}: fence lsn={lsn}"
                         f" {'passed' if ok else 'TIMEOUT'}")
            cmp = self._compare_pks(db, table, keys)
            if cmp is None:
                return None
            missing, extra, changed = cmp
            if not (missing or extra or changed):
                self._write_pk_files(db, table, [], [], [])
                return [], [], [], proof
            keys = set(missing) | set(extra) | set(changed)
            if not ok:
                break
        self._write_pk_files(db, table, missing, extra, changed)
        return missing, extra, changed, proof

    def _resolve_inflight(self, db, bad, stream=None):
        """Split DIFF tables into real diffs vs in-flight replication,
        deterministically when a fence is available, by sleep-settle as
        the fallback."""
        still, healed, how = [], [], []
        settle = int(self.hop.options.get("settle", 0))
        slept = False
        for t in bad:
            d = self._report(db)
            keys = set()
            for k in ("missing", "extra", "changed"):
                f = d / f"data-{t}.{k}"
                if f.exists():
                    keys |= set(f.read_text().splitlines())
            keys.discard("")
            if not keys or len(keys) > 20000:
                still.append(t)
                continue
            r = self.fenced_recheck(db, t, keys)
            if r is not None:
                missing, extra, changed, proof = r
                if missing or extra or changed:
                    still.append(t)
                else:
                    healed.append(t)
                    how.append(f"{t}: {'; '.join(proof)}")
                if stream:
                    stream(f"{t}: fence {'REAL DIFF' if t in still else 'converged'}")
                continue
            if not settle:
                still.append(t)
                continue
            if not slept:
                time.sleep(settle)
                slept = True
            s = self.settle_recheck(db, t)
            if s is None or any(s):
                still.append(t)
            else:
                healed.append(t)
                how.append(f"{t}: settled after {settle}s (no fence visible)")
        return still, healed, how

    def _column_fingerprint(self, db, table):
        """One scan, one aggregate per column: which columns actually
        differ. Localizes drift (e.g. only updated_at differs = timezone
        rendering, not data loss) before any row-level work."""
        sch, tbl = table.split(".", 1)
        try:
            cols = [l for l in self._psql("src", db,
                    "select attname from pg_attribute"
                    f""" where attrelid = '"{sch}"."{tbl}"'::regclass"""
                    " and attnum > 0 and not attisdropped"
                    " and attgenerated = '' order by attnum").splitlines() if l]
        except Exception as e:
            return self._fingerprint_failed(e)
        if not cols:
            return self._fingerprint_failed(
                f"the source lists no columns for {table}")
        # One value per hash, so no separator is involved - but chr(1) stood
        # in for NULL, and a column holding chr(1) hashed as a NULL
        from .. import rowtext
        expr = ", ".join(
            f"coalesce(sum(('x'||substr(md5("
            f"{rowtext.postgres_row([c], alias='')}"
            f"),1,16))::bit(64)::bigint::numeric), 0)"
            for c in cols)
        q = f'select {expr} from "{sch}"."{tbl}"'
        try:
            a = self._psql("src", db, q).split("|")
            b = self._psql("dst", db, q).split("|")
        except RuntimeError as e:
            return self._fingerprint_failed(e)
        diff = [c for c, x, y in zip(cols, a, b) if x != y]
        out = self.hop.report_dir(db) / f"data-{table}.columns"
        if diff:
            out.write_text("columns differing between src and dst:\n"
                           + "\n".join(diff) + "\n")
        elif out.exists():
            out.unlink()
        return diff

    # --- delta verify: a source slot records which rows changed; each cycle
    # verifies only those pks and advances only when clean (idempotent) ---

    def _delta_slot(self, db):
        import re as _re
        name = f"migkit_delta_{self.hop.name}_{db}"
        return _re.sub(r"[^a-z0-9_]", "_", name.lower())[:63]

    def delta_setup(self, db):
        slot = self._delta_slot(db)
        have = self._psql("src", db,
                          "select 1 from pg_replication_slots"
                          f" where slot_name = '{slot}'")
        if have:
            return False
        self._psql("src", db,
                   "select pg_create_logical_replication_slot("
                   f"'{slot}', 'test_decoding')")
        return True

    def delta_teardown(self, db):
        slot = self._delta_slot(db)
        self._psql("src", db,
                   "select pg_drop_replication_slot(slot_name)"
                   " from pg_replication_slots"
                   f" where slot_name = '{slot}'")

    @staticmethod
    def _parse_decoding(lines, pk_of):
        """Parse test_decoding rows into {table: set(pk_key)}.
        pk_of(table) -> ordered pk column list or None."""
        import re as _re
        field_re = _re.compile(r"(\w+)\[[^\]]*\]:('(?:[^']|'')*'|[^ ]+)")
        head_re = _re.compile(
            r"table ([^:]+): (INSERT|UPDATE|DELETE): (.*)")
        touched = {}
        nopk = set()
        last_lsn = ""
        for lsn, data in lines:
            last_lsn = lsn or last_lsn
            m = head_re.match(data)
            if not m:
                continue
            table = m.group(1).replace('"', "")
            pks = pk_of(table)
            if not pks:
                nopk.add(table)
                continue
            vals = {}
            for col, val in field_re.findall(m.group(3)):
                if val.startswith("'"):
                    val = val[1:-1].replace("''", "'")
                vals[col] = val
            if all(p in vals for p in pks):
                touched.setdefault(table, set()).add(
                    "\t".join(vals[p] for p in pks))
        return touched, nopk, last_lsn

    def delta_verify(self, db, limit=20000, log=None):
        slot = self._delta_slot(db)
        if self._in_recovery("src", db):
            return [Result("delta", db, "error",
                           "source is a read replica (pg_is_in_recovery=t):"
                           " logical slots live on the primary. Point the"
                           " hop's source at the writer/cluster endpoint")]
        if self.delta_setup(db):
            return [Result("delta", db, "ok",
                           f"slot {slot} created, changes are tracked"
                           " from this point on")]
        out = self._psql("src", db,
                         "select lsn||' '||data from"
                         f" pg_logical_slot_peek_changes('{slot}',"
                         f" null, {limit})")
        lines = [l.split(" ", 1) for l in out.splitlines() if " " in l]
        pk_cache = {}

        def pk_of(t):
            if t not in pk_cache:
                try:
                    pk_cache[t] = self._pk_cols_of(db, t)
                except RuntimeError:
                    pk_cache[t] = None
            return pk_cache[t]

        touched, nopk, last_lsn = self._parse_decoding(lines, pk_of)
        n_changes = sum(len(v) for v in touched.values())
        if not touched and not nopk:
            return [Result("delta", db, "ok",
                           "0 changes since last verified point")]
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
                    str(self._report(db)),
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
        if clean and last_lsn:
            self._psql("src", db,
                       "select pg_replication_slot_advance("
                       f"'{slot}', '{last_lsn}'::pg_lsn)")
            note = "slot advanced"
        else:
            note = "slot NOT advanced, window replays next cycle"
        if len(lines) >= limit:
            note += f"; window truncated at {limit} changes, more pending"
        res.insert(0, Result("delta", db,
                             "ok" if clean else "diff",
                             f"{n_changes} changed rows across"
                             f" {len(touched)} tables, {note}"))
        return res

    def list_move_tables(self, db):
        out = self._psql("src", db,
            "select n.nspname||'|'||c.relname from pg_class c"
            " join pg_namespace n on n.oid = c.relnamespace"
            " where c.relkind = 'r'"
            " and n.nspname not in ('pg_catalog','information_schema')"
            " and n.nspname not like 'pg\\_%'"
            " and n.nspname not like '\\_\\_%' order by 1")
        return [tuple(x.split("|")) for x in out.splitlines() if x]

    def _int_pk(self, db, sch, tbl):
        rows = self._psql("src", db,
            "select a.attname||'|'||a.atttypid::regtype from pg_index i"
            " join pg_attribute a on a.attrelid = i.indrelid"
            " and a.attnum = any(i.indkey)"
            f" where i.indrelid = '\"{sch}\".\"{tbl}\"'::regclass"
            " and i.indisprimary").splitlines()
        if len(rows) == 1:
            col, typ = rows[0].split("|")
            if typ in ("smallint", "integer", "bigint"):
                return col
        return None

    def _copy_cols(self, db, sch, tbl):
        """The source's live columns, in its own order, or None.

        None means the catalogue could not be read, and the copy then falls
        back to positional mapping - which is what it always did. Better to
        keep working than to refuse, but the caller logs it.
        """
        try:
            out = self._psql("src", db,
                             "select a.attname from pg_attribute a"
                             " join pg_class c on c.oid = a.attrelid"
                             " join pg_namespace n on n.oid = c.relnamespace"
                             f" where n.nspname = '{sch}'"
                             f" and c.relname = '{tbl}'"
                             " and a.attnum > 0 and not a.attisdropped"
                             " and a.attgenerated = ''"
                             " order by a.attnum")
        except Exception:
            return None
        cols = [l for l in out.splitlines() if l]
        return cols or None

    @staticmethod
    def _copy_select(qt, cols, pred=""):
        """SELECT naming the columns, so both ends of the pipe agree."""
        what = ", ".join(f'"{c}"' for c in cols) if cols else "*"
        sql = f"select {what} from {qt}"
        return f"{sql} where {pred}" if pred else sql

    def _copy_pipe(self, db, select_sql, qt, pre_sql="", columns=None):
        """Stream rows from source to target through a COPY pipe.

        `columns` names them on the receiving side. Without it COPY maps by
        position, and position is not a property either side agrees on:
        measured, a source holding (id, a='AAA', b='BBB') copied into a target
        whose columns are declared (id, b, a) lands as a='BBB', b='AAA' - no
        error, every value in the wrong column. A source with a DROP COLUMN in
        its history against a target created fresh from today's schema is
        exactly that shape, and migkit has already measured that pairing
        happening in the row hash.

        So the column list is not an optimisation. It is the difference
        between moving the data and moving it into the wrong columns.
        """
        s, t = self.hop.source, self.hop.target
        env_s = tool_env({"PGPASSWORD": s.password, "PGCONNECT_TIMEOUT": "15"})
        env_t = tool_env({"PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"})
        collist = ""
        if columns:
            collist = " (" + ", ".join(f'"{c}"' for c in columns) + ")"
        out = subprocess.Popen(
            ["psql", "-h", s.host, "-p", str(s.port), "-U", s.user, "-d", db,
             "-X", "-q", "-v", "ON_ERROR_STOP=1",
             "-c", f"\\copy ({select_sql}) to stdout"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env_s)
        cmds = ["-c", pre_sql] if pre_sql else []
        inp = subprocess.Popen(
            ["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
             "-d", self._d("dst", db),
             "-X", "-q", "-v", "ON_ERROR_STOP=1", "-1", *cmds,
             "-c", f"\\copy {qt}{collist} from stdin"],
            stdin=out.stdout, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=env_t)
        out.stdout.close()
        _, err_i = inp.communicate()
        _, err_o = out.communicate()
        if out.returncode or inp.returncode:
            raise RuntimeError((err_o + err_i).decode()[-300:])

    def move_table(self, db, sch, tbl, chunk, ck, log):
        sch = sch or "public"
        key = f"{sch}.{tbl}"
        qt = f'"{sch}"."{tbl}"'
        st = ck.setdefault(key, {})
        if st.get("done"):
            log(f"{key}: done earlier, skip")
            return
        pk = self._int_pk(db, sch, tbl)
        if not pk:
            log(f"{key}: no single int pk, single-shot copy")
            cols = self._copy_cols(db, sch, tbl)
            self._copy_pipe(db, self._copy_select(qt, cols), qt,
                            f"truncate {qt}", columns=cols)
            st["done"] = True
            return
        mm = self._psql("src", db,
                        f'select coalesce(min("{pk}"), 0)||\'|\'||'
                        f'coalesce(max("{pk}"), 0) from {qt}')
        lo, hi = (int(x) for x in mm.split("|"))
        last = st.get("last", lo - 1)
        while last < hi:
            nxt = min(last + chunk, hi)
            pred = f'"{pk}" > {last} and "{pk}" <= {nxt}'
            cols = self._copy_cols(db, sch, tbl)
            self._copy_pipe(db, self._copy_select(qt, cols, pred), qt,
                            f"delete from {qt} where {pred}", columns=cols)
            last = nxt
            st["last"] = last
            ck.save()
            log(f"{key}: up to {pk}={last:,} of {hi:,}")
        st["done"] = True
        ck.save()

    def replicate_sql(self, db, copy_data=True):
        s = self.hop.source
        name = "migkit_" + self.hop.name.replace("-", "_")
        conn = (f"host={s.host} port={s.port} dbname={db}"
                f" user={s.user} password={s.password}")
        return {
            "src": [f"create publication {name} for all tables;"],
            "dst": [f"create subscription {name} connection '{conn}'"
                    f" publication {name} with (copy_data ="
                    f" {'true' if copy_data else 'false'});"],
            "drop_src": [f"drop publication if exists {name};"],
            "drop_dst": [f"drop subscription if exists {name};"],
            "status": "select subname, received_lsn, latest_end_lsn,"
                      " latest_end_time from pg_stat_subscription",
        }

    def replication_status(self, db, sql):
        got = self._psql("dst", db, sql).strip()
        return got or ("no subscription on the target: the statements ran"
                       " but nothing is replicating")

    def migration_pair(self, db):
        from urllib.parse import quote
        if not which("atlas"):
            return None, None
        s, t = self.hop.source, self.hop.target
        su = (f"postgres://{s.user}:{quote(s.password, safe='')}"
              f"@{s.host}:{s.port}/{db}?sslmode=prefer")
        tu = (f"postgres://{t.user}:{quote(t.password, safe='')}"
              f"@{t.host}:{t.port}/{self._d('dst', db)}?sslmode=prefer")

        def diff(a, b):
            p = run(["atlas", "schema", "diff", "--from", a, "--to", b,
                     "--exclude", "__*",
                     "--exclude", "*.migkit_changelog"],
                    check=False, timeout=180)
            text = p.stdout.strip()
            if p.returncode or "Schemas are synced" in text:
                return ""
            return text
        return diff(tu, su), diff(su, tu)

    def fetch_sample_df(self, side, db, table, limit):
        """The rows `--drill` compares, read without being rewritten.

        `text=True` here would decode psql's output with universal
        newlines, which turns a `\\r\\n` inside a value into `\\n` before
        anything compares it. Measured on a pair whose only difference was
        a carriage return in one column:

            check --only data   DIFF, pk-level file names row 5
            check --drill       "Number of rows with some compared columns
                                 unequal: 0"

        Two answers from one run, and the wrong one came from the command
        whose whole job is to show what differs. The bytes psql sends are
        `b'"one\\r\\ntwo"\\n'`; the same call with `text=True` returns
        `'"one\\ntwo"\\n'`, so the CR is gone before pandas is reached. The
        MySQL engine reads through a driver and never had this.
        """
        import io as _io

        import pandas as pd
        sch, tbl = table.split(".", 1) if "." in table else ("public", table)
        ep = self.hop.source if side == "src" else self.hop.target
        env = tool_env({"PGPASSWORD": ep.password})
        p = subprocess.run(
            ["psql", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
             "-d", self._d(side, db), "-X", "-q", "-v", "ON_ERROR_STOP=1",
             "-c", f"\\copy (select * from \"{sch}\".\"{tbl}\""
                   f" limit {limit}) to stdout (format csv, header)"],
            capture_output=True, env=env)
        if p.returncode:
            raise RuntimeError(p.stderr.decode("utf-8", "replace")[-200:])
        return pd.read_csv(_io.StringIO(p.stdout.decode("utf-8"), newline=""))

    def watch_sample(self, db):
        sample = {"db": db, "ts": time.time()}
        q = ("select coalesce(sum(n_live_tup),0) from pg_stat_user_tables"
             " where schemaname not like '\\_\\_%'")
        try:
            sample["src_rows"] = int(self._psql("src", db, q) or 0)
            sample["dst_rows"] = int(self._psql("dst", db, q) or 0)
        except RuntimeError as e:
            sample["error"] = str(e).splitlines()[-1]
            return sample
        try:
            # per-table live counts on the source so a cutover verdict can
            # name which tables a queue/worker is still writing to
            rows = self._psql("src", db,
                              "select schemaname||'.'||relname||'|'||n_live_tup"
                              " from pg_stat_user_tables"
                              " where schemaname not like '\\_\\_%'").splitlines()
            sample["src_tables"] = {l.rsplit("|", 1)[0]: int(l.rsplit("|", 1)[1])
                                    for l in rows if "|" in l}
        except (RuntimeError, ValueError):
            pass
        try:
            slots = self._psql("src", db,
                               "select slot_name||' active='||active||' lag='||"
                               "coalesce(pg_size_pretty(pg_wal_lsn_diff("
                               "pg_current_wal_lsn(), confirmed_flush_lsn)),'?')"
                               " from pg_replication_slots")
            sample["replication_slots"] = slots.splitlines() if slots else []
            conns = self._psql("src", db,
                               "select application_name||' '||client_addr||' '||state"
                               " from pg_stat_replication")
            sample["replication_conns"] = conns.splitlines() if conns else []
        except RuntimeError:
            pass
        return sample
