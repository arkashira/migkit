"""Where stored code is run: a scratch server of the source's engine and
one of the target's, holding the same fixture (backlog R11).

Running a procedure writes, and a rolled-back write still moves a
sequence; migkit never writes to a source, and a target may already be
live. So nothing is run on either: the proof runs on copies. The hop
names them (`proof_sandbox: {source: <endpoint>, target: <endpoint>}`),
or says `proof_sandbox: docker` and migkit starts both itself, on
127.0.0.1, at the engines and versions the hop's own servers run, and
removes them when the proof is done. A hop that names neither proves no
procedure, trigger or event, and says so.

The fixture is a sample of the source, closed over its foreign keys and
holding every row a routine's constants name, carried into the source
engine's sandbox by migkit's own move and from there into the target
engine's by the hop's own mapping, then held to migkit's own data check.
A difference found after that is the code's, never the data's.
"""
import re
import secrets
import socket
import subprocess
import time

from ..config import Endpoint, Hop

#: rows of each table in the fixture, before the rows its foreign keys
#: and the routines' constants pull in
SAMPLE_ROWS = 200
#: a table with no key is carried whole up to this many rows
KEYLESS_WHOLE = 2000
#: the image a PostgreSQL sandbox runs: the server with the static check
#: and profiler (`plpgsql_check`) the proof gates and measures with
PG_IMAGE = "migkit-sandbox-postgres"
#: the SQL Server a sandbox runs: SQL Edge, the one build that runs on
#: arm64; on x86 the full server's image runs the same T-SQL
MSSQL_IMAGE = "mcr.microsoft.com/azure-sql-edge:latest"
#: the Oracle a sandbox runs: Oracle Free, native on arm64 from 23.5
ORACLE_IMAGE = "gvenzl/oracle-free:slim-faststart"
PG_DOCKERFILE = ("FROM postgres:{v}\n"
                 "RUN apt-get update && apt-get install -y"
                 " --no-install-recommends postgresql-{v}-plpgsql-check"
                 " postgresql-{v}-orafce && rm -rf /var/lib/apt/lists/*\n")
#: the recipe's revision, in the image's tag: a changed recipe is built
#: again rather than taken from an image made by an older one
PG_RECIPE = 2
#: extensions a target may run that the sandbox makes where the target has
#: them - Oracle's functions, emulated
EMULATIONS = ("orafce",)


def pg_tag(major):
    return f"{PG_IMAGE}:{major}-r{PG_RECIPE}"


def foreign_key_statements(like, side, db, tables):
    """The statements making the foreign keys among `tables` of a hop's
    own server (`like`, `side`) again in its sandbox, as the server keeps
    them - ON DELETE, ON UPDATE and deferral with them. A key the copy
    makes without its action cascades nothing a routine's delete or a
    constraint's probe deletes, and differs where the server would not.
    MySQL's are in the tables' own statements already (`table_ddl`)."""
    engine = like.CANON_ENGINE
    wanted = {str(t).lower() for t in tables}
    out = []
    if engine == "postgres":
        for child, parent, qchild, name, made in like._catalog(side, db, """
                select n.nspname || '.' || c.relname,
                       pn.nspname || '.' || pc.relname,
                       format('%I.%I', n.nspname, c.relname),
                       quote_ident(con.conname), pg_get_constraintdef(con.oid)
                  from pg_constraint con
                  join pg_class c on c.oid = con.conrelid
                  join pg_namespace n on n.oid = c.relnamespace
                  join pg_class pc on pc.oid = con.confrelid
                  join pg_namespace pn on pn.oid = pc.relnamespace
                 where con.contype = 'f'
                 order by 1, con.conname"""):
            if _among(child, wanted) and _among(parent, wanted):
                out.append(f"alter table {qchild} add constraint {name}"
                           f" {made}")
    elif engine == "mssql":
        keys = {}
        for name, child, parent, col, pcol, on_delete, on_update in \
                like._rows(side, db, "select f.name, s.name + '.' +"
                           " object_name(f.parent_object_id), rs.name + '.'"
                           " + object_name(f.referenced_object_id), cp.name,"
                           " cr.name, f.delete_referential_action_desc,"
                           " f.update_referential_action_desc from"
                           " sys.foreign_keys f join sys.foreign_key_columns"
                           " k on k.constraint_object_id = f.object_id join"
                           " sys.columns cp on cp.object_id ="
                           " k.parent_object_id and cp.column_id ="
                           " k.parent_column_id join sys.columns cr on"
                           " cr.object_id = k.referenced_object_id and"
                           " cr.column_id = k.referenced_column_id join"
                           " sys.objects o on o.object_id ="
                           " f.parent_object_id join sys.schemas s on"
                           " s.schema_id = o.schema_id join sys.objects ro"
                           " on ro.object_id = f.referenced_object_id join"
                           " sys.schemas rs on rs.schema_id = ro.schema_id"
                           " order by f.name, k.constraint_column_id"):
            one = keys.setdefault((name, child, parent, on_delete,
                                   on_update), ([], []))
            one[0].append(col)
            one[1].append(pcol)
        for (name, child, parent, on_delete, on_update), (cols, pcols) in \
                keys.items():
            if not (_among(child, wanted) and _among(parent, wanted)):
                continue
            out.append(
                f"alter table {like._qualified(side, db, child)} add"
                f" constraint {like._q(name)} foreign key"
                f" ({', '.join(like._q(c) for c in cols)}) references"
                f" {like._qualified(side, db, parent)}"
                f" ({', '.join(like._q(c) for c in pcols)})"
                f" on delete {str(on_delete).replace('_', ' ').lower()}"
                f" on update {str(on_update).replace('_', ' ').lower()}")
    elif engine == "oracle":
        from ..engines.dbapi import MARK
        owner = like._owner(side, db)
        keys = {}
        for name, child, parent, col, pcol, rule, deferrable, deferred in \
                like._rows(side, db, "select c.constraint_name, c.table_name,"
                           " p.table_name, cc.column_name, pc.column_name,"
                           " c.delete_rule, c.deferrable, c.deferred from"
                           " all_constraints c join all_constraints p on"
                           " p.owner = c.r_owner and p.constraint_name ="
                           " c.r_constraint_name join all_cons_columns cc on"
                           " cc.owner = c.owner and cc.constraint_name ="
                           " c.constraint_name join all_cons_columns pc on"
                           " pc.owner = p.owner and pc.constraint_name ="
                           " p.constraint_name and pc.position ="
                           " cc.position where c.owner = " + MARK +
                           " and c.constraint_type = 'R' order by"
                           " c.constraint_name, cc.position", (owner,)):
            one = keys.setdefault((name, child, parent, rule, deferrable,
                                   deferred), ([], []))
            one[0].append(col)
            one[1].append(pcol)
        for (name, child, parent, rule, deferrable, deferred), \
                (cols, pcols) in keys.items():
            if not (_among(like._said(child), wanted) and
                    _among(like._said(parent), wanted)):
                continue
            q = '"{}"'.format
            made = (f'alter table {q(child)} add constraint {q(name)}'
                    f' foreign key ({", ".join(q(c) for c in cols)})'
                    f' references {q(parent)}'
                    f' ({", ".join(q(c) for c in pcols)})')
            if str(rule).upper() in ("CASCADE", "SET NULL"):
                made += f" on delete {str(rule).lower()}"
            if str(deferrable).upper() == "DEFERRABLE":
                made += " deferrable initially " + (
                    "deferred" if str(deferred).upper() == "DEFERRED"
                    else "immediate")
            out.append(made)
    return out


def _among(table, wanted):
    """Whether a table a catalogue names is one of `wanted` (the names
    the engine's own listing gives, schema and all or without it)."""
    name = str(table).lower()
    return name in wanted or name.split(".")[-1] in {
        w.split(".")[-1] for w in wanted}


class NoSandbox(Exception):
    """Why nothing could be run: said in the verdict, as the reason."""


def spec(hop):
    got = (hop.options or {}).get("proof_sandbox")
    if not got:
        return None
    if isinstance(got, str):
        return {"docker": {}} if got == "docker" else None
    if isinstance(got, dict) and "docker" in got:
        return {"docker": got["docker"] if isinstance(got["docker"], dict)
                else {}}
    return got if isinstance(got, dict) else None


def _endpoint(d):
    d = dict(d or {})
    return Endpoint(host=str(d.pop("host", "")), port=int(d.pop("port", 0)),
                    user=str(d.pop("user", "")),
                    password=str(d.pop("password", "")),
                    options=d.pop("options", {}) or {})


def _ck():
    class Ck(dict):
        def save(self):
            pass
    return Ck()


def _literal(engine, value):
    """A key's value written into a filter, in `engine`'s SQL."""
    import datetime as dt
    from decimal import Decimal
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, Decimal)):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        hexed = bytes(value).hex()
        return {"mysql": f"x'{hexed}'", "mssql": f"0x{hexed}"}.get(
            engine, f"'\\x{hexed}'::bytea")
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        value = value.isoformat(sep=" ") if isinstance(
            value, dt.datetime) else value.isoformat()
    text = str(value).replace("'", "''")
    if engine == "mysql":
        text = text.replace("\\", "\\\\")
    return f"N'{text}'" if engine == "mssql" else f"'{text}'"


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Sandbox:
    """The two scratch servers, the fixture in both, and the engines that
    reach them. `src`/`dst` are engines of the source's and the target's
    kind whose side "dst" is the sandbox server (so they may write it) and
    whose side "src" is the hop's own server, read-only; `twin` is the pair
    of the two sandboxes."""

    def __init__(self, pair, db):
        self.pair = pair
        self.hop = pair.hop
        self.db = db
        self.tdb = pair.hop.target_db(db)
        self.containers = []
        self.notes = []
        self.src = self.dst = self.twin = None
        self.tables = {}              #: source table -> target table
        self.keys = {}                #: source table -> [key columns]

    # ---- the two servers --------------------------------------------------

    def open(self):
        got = spec(self.hop)
        if got is None:
            raise NoSandbox("no sandbox is named for this hop (the hop"
                            " option proof_sandbox names two scratch"
                            " servers, or docker for migkit to start them),"
                            " and nothing is run on the hop's own servers")
        if "docker" in got:
            src_ep, dst_ep = self._docker(got["docker"])
        else:
            src_ep, dst_ep = _endpoint(got.get("source")), \
                _endpoint(got.get("target"))
        for ep in (src_ep, dst_ep):
            if not ep.configured():
                raise NoSandbox("the hop's proof_sandbox names a server"
                                " without a host")
            for own in (self.hop.source, self.hop.target):
                if (ep.host, int(ep.port or 0)) == (own.host,
                                                    int(own.port or 0)):
                    raise NoSandbox("the hop's proof_sandbox names one of"
                                    " the hop's own servers; the proof"
                                    " writes, and runs only on scratch"
                                    " servers")
        from ..engines import engine_named
        from ..engines.hetero import HeteroEngine
        from ..hopkeys import WRITING
        opts = {k: v for k, v in (self.hop.options or {}).items()
                if k not in ("proof_sandbox", "cutover") and k not in WRITING}
        s, t = self.pair.src_name, self.pair.dst_name
        self.src = engine_named(s, Hop(
            name=f"{self.hop.name}-proof-src", engine=s,
            source=self.hop.source, target=src_ep, databases=[self.db],
            options={}))
        self.dst = engine_named(t, Hop(
            name=f"{self.hop.name}-proof-dst", engine=t,
            source=self.hop.target, target=dst_ep, databases=[self.tdb],
            options={}))
        self.twin = HeteroEngine(Hop(
            name=f"{self.hop.name}-proof", engine="hetero", source=src_ep,
            target=dst_ep, databases=[self.db], exclude=self.hop.exclude,
            include=list(getattr(self.hop, "include", None) or []),
            options=opts, db_map=dict(self.hop.db_map or {}),
            mapping=dict(self.hop.mapping or {})))
        self._fresh_databases()
        self._fixture()
        return self

    def close(self):
        for name in self.containers:
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        self.containers = []

    def __enter__(self):
        return self.open()

    def __exit__(self, *exc):
        self.close()

    def _version(self, engine, side_eng, side):
        if engine == "mysql":
            got = str(side_eng._q(side, "select version()")[0][0])
        elif engine == "postgres":
            got = side_eng._psql(side, self.db, "show server_version")
        elif engine == "mssql":
            got = str(side_eng._rows(
                side, self.db, "select cast(serverproperty('ProductVersion')"
                               " as varchar(40))")[0][0])
        elif engine == "oracle":
            got = str(side_eng._rows(side, self.db, "select version_full from"
                                     " product_component_version")[0][0])
        else:
            raise NoSandbox(f"migkit cannot start a {engine} sandbox itself;"
                            " name one in the hop's proof_sandbox")
        return got

    def _docker(self, opts):
        from ..util import which
        if not which("docker"):
            raise NoSandbox("the hop asks for a sandbox migkit starts, and"
                            " docker is not on this machine")
        prefix = str(opts.get("name") or f"migkit-sandbox-{self.hop.name}")
        ports = list(opts.get("ports") or [])
        out = []
        for k, (engine, eng, side) in enumerate((
                (self.pair.src_name, self.pair.src_engine, "src"),
                (self.pair.dst_name, self.pair.dst_engine, "dst"))):
            version = self._version(eng.CANON_ENGINE, eng, side)
            port = int(ports[k]) if k < len(ports) else _free_port()
            name = f"{prefix}-{'src' if k == 0 else 'dst'}"
            out.append(self._start(eng.CANON_ENGINE, version, name, port))
        return out

    def _start(self, engine, version, name, port):
        password = secrets.token_hex(12)
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        if engine == "mysql":
            m = re.match(r"(\d+\.\d+)", version)
            image = ("mariadb:" if "mariadb" in version.lower()
                     else "mysql:") + (m.group(1) if m else "8.4")
            cmd = ["docker", "run", "-d", "--name", name, "-e",
                   "MYSQL_ROOT_PASSWORD", "-p", f"127.0.0.1:{port}:3306",
                   image, "--log-bin-trust-function-creators=1",
                   "--local-infile=1"]
            env_name, user = "MYSQL_ROOT_PASSWORD", "root"
        elif engine == "oracle":
            image = ORACLE_IMAGE
            cmd = ["docker", "run", "-d", "--name", name, "-e",
                   "ORACLE_PASSWORD", "-p", f"127.0.0.1:{port}:1521", image]
            env_name, user = "ORACLE_PASSWORD", "system"
        elif engine == "mssql":
            # the password rules want every kind of character
            password = "Mk1!" + password
            image = MSSQL_IMAGE
            cmd = ["docker", "run", "-d", "--name", name, "-e",
                   "ACCEPT_EULA=Y", "-e", "MSSQL_SA_PASSWORD", "-p",
                   f"127.0.0.1:{port}:1433", image]
            env_name, user = "MSSQL_SA_PASSWORD", "sa"
        else:
            major = re.match(r"(\d+)", version).group(1)
            image = self._pg_image(major)
            cmd = ["docker", "run", "-d", "--name", name, "-e",
                   "POSTGRES_PASSWORD", "-p", f"127.0.0.1:{port}:5432",
                   image]
            env_name, user = "POSTGRES_PASSWORD", "postgres"
        import os
        made = subprocess.run(cmd, capture_output=True, text=True,
                              env={**os.environ, env_name: password})
        if made.returncode != 0:
            last = (made.stderr.strip().splitlines() or [""])[-1][:120]
            raise NoSandbox(f"the {engine} sandbox did not start: {last}")
        self.containers.append(name)
        ep = Endpoint(host="127.0.0.1", port=port, user=user,
                      password=password)
        self._wait(engine, ep)
        return ep

    def _pg_image(self, major):
        """The PostgreSQL sandbox's image, built once on this machine from
        the server's own image and the static check's package; the plain
        server where that cannot be built (then nothing is gated or
        measured, and the verdicts say so)."""
        tag = pg_tag(major)
        if subprocess.run(["docker", "image", "inspect", tag],
                          capture_output=True).returncode == 0:
            return tag
        built = subprocess.run(["docker", "build", "-t", tag, "-"],
                               input=PG_DOCKERFILE.format(v=major),
                               capture_output=True, text=True, timeout=900)
        if built.returncode == 0:
            return tag
        self.notes.append("the PostgreSQL sandbox runs without its static"
                          " check: its image could not be built here")
        return f"postgres:{major}"

    def _wait(self, engine, ep, most=180):
        began = time.time()
        while time.time() - began < most:
            try:
                if engine == "mysql":
                    import pymysql
                    pymysql.connect(host=ep.host, port=ep.port, user=ep.user,
                                    password=ep.password,
                                    connect_timeout=3).close()
                elif engine == "mssql":
                    import pymssql
                    pymssql.connect(server=ep.host, port=str(ep.port),
                                    user=ep.user, password=ep.password,
                                    login_timeout=3).close()
                elif engine == "oracle":
                    import oracledb
                    oracledb.connect(user=ep.user, password=ep.password,
                                     dsn=f"{ep.host}:{ep.port}/FREEPDB1",
                                     tcp_connect_timeout=3).close()
                else:
                    import psycopg2
                    psycopg2.connect(host=ep.host, port=ep.port,
                                     user=ep.user, password=ep.password,
                                     dbname="postgres",
                                     connect_timeout=3).close()
                return
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(2)
        raise NoSandbox(f"the {engine} sandbox did not come up within"
                        f" {most} seconds")

    # ---- the databases, then the fixture ------------------------------------

    def _fresh_databases(self):
        for eng, name, like, side in ((self.src, self.db,
                                       self.pair.src_engine, "src"),
                                      (self.dst, self.tdb,
                                       self.pair.dst_engine, "dst")):
            if eng.CANON_ENGINE == "mysql":
                made = like._q(side, "select default_character_set_name,"
                                     " default_collation_name from"
                                     " information_schema.schemata where"
                                     " schema_name = %s",
                               (like._d(side, self.db),))
                cs = f" character set {made[0][0]} collate {made[0][1]}" \
                    if made else ""
                conn = eng._conn("dst")
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"drop database if exists"
                                    f" {eng._my_ident(name)}")
                        cur.execute(f"create database {eng._my_ident(name)}"
                                    f"{cs}")
                        try:
                            cur.execute("set global"
                                        " log_bin_trust_function_creators"
                                        " = 1")
                        except Exception:  # noqa: BLE001 - said below
                            self.notes.append(
                                "the MySQL sandbox refuses functions that"
                                " do not declare what they read: its user"
                                " cannot set log_bin_trust_function_creators")
                    conn.commit()
                finally:
                    conn.close()
            elif eng.CANON_ENGINE == "postgres":
                conn = eng._conn("dst", "postgres")
                conn.autocommit = True
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"drop database if exists"
                                    f" {eng._quote_ident(name)} with (force)")
                        cur.execute(f"create database"
                                    f" {eng._quote_ident(name)} template"
                                    " template0 encoding 'UTF8'")
                finally:
                    conn.close()
                conn = eng._conn("dst", name)
                try:
                    with conn.cursor() as cur:
                        for ext in sorted(self.emulations(like, side)):
                            try:
                                cur.execute(f"create extension if not exists"
                                            f" {ext}")
                            except Exception:  # noqa: BLE001 - said
                                conn.rollback()
                                self.notes.append(
                                    f"the target runs the {ext} extension"
                                    " and its sandbox cannot")
                        try:
                            cur.execute("create extension if not exists"
                                        " plpgsql_check")
                        except Exception:  # noqa: BLE001 - said below
                            conn.rollback()
                            self.notes.append(
                                "the PostgreSQL sandbox has no static check"
                                " installed: nothing is gated and no"
                                " coverage is measured there")
                    conn.commit()
                finally:
                    conn.close()
            elif eng.CANON_ENGINE == "oracle":
                # a database is a schema: its own user, made afresh, with
                # what routines and tables need
                owner = eng._owner("dst", name)
                conn = eng._connect("dst", name)
                try:
                    cur = conn.cursor()
                    try:
                        cur.execute(f"drop user {eng._q(owner)} cascade")
                    except Exception:  # noqa: BLE001 - not there yet
                        pass
                    cur.execute(f"create user {eng._q(owner)} identified by"
                                f' "{secrets.token_hex(8)}" quota unlimited'
                                " on users")
                    cur.execute("grant create session, create table, create"
                                " procedure, create trigger, create sequence,"
                                f" create view to {eng._q(owner)}")
                finally:
                    conn.close()
            elif eng.CANON_ENGINE == "mssql":
                got = like._rows(side, self.db, "select collation_name from"
                                 " sys.databases where name = db_name()")
                collate = f" collate {got[0][0]}" if got and got[0][0] \
                    else ""
                conn = eng._connect("dst", "master")
                conn.autocommit(True)
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"if db_id('{name}') is not null begin"
                                    f" alter database {eng._q(name)} set"
                                    " single_user with rollback immediate;"
                                    f" drop database {eng._q(name)} end")
                        cur.execute(f"create database {eng._q(name)}"
                                    f"{collate}")
                finally:
                    conn.close()
            else:
                raise NoSandbox(f"migkit cannot make a {eng.CANON_ENGINE}"
                                " sandbox database yet")

    def emulations(self, eng, side):
        """The emulation extensions the hop's own PostgreSQL runs."""
        if eng.CANON_ENGINE != "postgres":
            return set()
        try:
            got = eng._psql(side, self.db, "select extname from pg_extension")
        except Exception:  # noqa: BLE001 - none known
            return set()
        return {x for x in got.split() if x in EMULATIONS}

    def _order(self, tables, fks):
        """Parents before children, as far as the foreign keys allow."""
        left, done, out = list(tables), set(), []
        parents = {}
        for child, _, parent, _ in fks:
            if child != parent:
                parents.setdefault(child, set()).add(parent)
        while left:
            ready = [t for t in left
                     if not (parents.get(t, set()) - done) & set(left)]
            ready = ready or left[:1]
            for t in ready:
                out.append(t)
                done.add(t)
                left.remove(t)
        return out

    def _exec(self, eng, db, statements):
        if eng.CANON_ENGINE == "oracle":
            conn = eng._connect("dst", db)
            try:
                cur = conn.cursor()
                cur.execute("alter session set current_schema ="
                            f" {eng._q(eng._owner('dst', db))}")
                for s in statements:
                    cur.execute(s)
                conn.commit()
            finally:
                conn.close()
            return
        if eng.CANON_ENGINE == "mssql":
            conn = eng._connect("dst", db)
            try:
                with conn.cursor() as cur:
                    for s in statements:
                        cur.execute(s)
                conn.commit()
            finally:
                conn.close()
            return
        if eng.CANON_ENGINE == "mysql":
            conn = eng._conn("dst")
            try:
                with conn.cursor() as cur:
                    cur.execute(f"use {eng._my_ident(eng._d('dst', db))}")
                    cur.execute("set foreign_key_checks = 0")
                    for s in statements:
                        cur.execute(s)
                conn.commit()
            finally:
                conn.close()
        else:
            conn = eng._conn("dst", eng._d("dst", db))
            try:
                with conn.cursor() as cur:
                    for s in statements:
                        cur.execute(s)
                conn.commit()
            finally:
                conn.close()

    @staticmethod
    def _split(ddl):
        return [s for s in (p.strip() for p in str(ddl).split(";\n")) if s]

    def _fixture(self):
        pair, db = self.pair, self.db
        src_tables = pair.src_engine.neutral_tables("src", db)
        src_fks = pair.src_engine.foreign_keys("src", db)
        for t in self._order(src_tables, src_fks):
            self._exec(self.src, db, self._split(
                pair.src_engine.table_ddl("src", db, t)))
        # the engines name the target's database from the source's name,
        # through the hop's mapping, themselves
        dst_tables = pair.dst_engine.neutral_tables("dst", db)
        dst_fks = pair.dst_engine.foreign_keys("dst", db)
        # what the fresh database holds already is an extension's own (the
        # target's emulation makes its tables itself): not made again
        try:
            made = set(self.dst.neutral_tables("dst", self.tdb))
        except Exception:  # noqa: BLE001 - a fresh database holds nothing
            made = set()
        dst_fks = [f for f in dst_fks if f[0] not in made]
        for t in self._order([t for t in dst_tables if t not in made],
                             dst_fks):
            self._exec(self.dst, self.tdb, self._split(
                pair.dst_engine.table_ddl("dst", db, t)))
        pairs, _, _, _ = pair.match_tables(src_tables, dst_tables,
                                           pair._rename)
        self.tables = dict(pairs)
        sample = self._sample(src_tables, src_fks)
        self._carry(src_tables, sample)
        # the foreign keys once the rows are in, as each server keeps them:
        # with what a delete or a key change does to the rows pointing at
        # it, and when they are checked
        for like, side, eng, name, made_here in (
                (pair.src_engine, "src", self.src, db, src_tables),
                (pair.dst_engine, "dst", self.dst, self.tdb,
                 [t for t in dst_tables if t not in made])):
            stmts = foreign_key_statements(like, side, db, made_here)
            if stmts:
                self._exec(eng, name, stmts)
        self.keys = {t: pair.src_engine.neutral_key("src", db, t)
                     for t in src_tables}

    @staticmethod
    def _pg_q(name):
        return '"' + str(name).replace('"', '""') + '"'

    def _pg_name(self, table):
        return ".".join(self._pg_q(p) for p in str(table).split("."))

    #: literals in the routines' bodies whose rows the fixture carries
    constants = ()

    def _sample(self, tables, fks):
        """{table: [key tuples]} - the first rows of each table, the rows
        the routines' constants name, and every parent a sampled row
        points at; None for a table carried whole."""
        from .. import canon
        pair, db = self.pair, self.db
        eng = pair.src_engine
        keys = {t: eng.neutral_key("src", db, t) for t in tables}
        cols = {t: [(n, canon.type_class(eng.CANON_ENGINE, ty))
                    for n, ty in eng.neutral_columns("src", db, t)]
                for t in tables}
        chosen = {}
        for t in tables:
            if not keys[t]:
                chosen[t] = None
                continue
            names = [n for n, _ in cols[t]]
            at = [names.index(k) for k in keys[t]]
            rows, _ = eng.neutral_read("src", db, t, cols[t], None,
                                       SAMPLE_ROWS)
            chosen[t] = {tuple(r[i] for i in at) for r in rows}
            kind = dict(cols[t]).get(keys[t][0])
            named = [c for c in self.constants
                     if (kind == "integer" and isinstance(c, int))
                     or (kind == "text" and isinstance(c, str))]
            if len(keys[t]) == 1 and named:
                # the rows a routine's own constants name, by the key's
                # own kind of value (a word is no integer key)
                try:
                    got = eng.neutral_rows_by_key(
                        "src", db, t, cols[t], keys[t],
                        [(c,) for c in named], None)
                except Exception:  # noqa: BLE001 - the sample without them
                    got = {}
                chosen[t] |= {tuple(r[i] for i in at) for r in got.values()}
        for _ in range(6):
            grew = False
            for child, ccols, parent, pcols in fks:
                if chosen.get(child) is None or chosen.get(parent) is None \
                        or list(pcols) != list(keys.get(parent) or []):
                    continue
                rows = eng.neutral_rows_by_key(
                    "src", db, child, cols[child], keys[child],
                    list(chosen[child]), None)
                names = [n for n, _ in cols[child]]
                at = [names.index(c) for c in ccols]
                wanted = {tuple(r[i] for i in at) for r in rows.values()}
                wanted = {k for k in wanted if None not in k}
                new = wanted - chosen[parent]
                if new:
                    chosen[parent] |= new
                    grew = True
            if not grew:
                break
        return chosen

    def _carry(self, tables, sample):
        """The sample into the source's sandbox by migkit's own move, then
        into the target's by the hop's own mapping; then migkit's own
        check, which must find them equal."""
        from ..engines.hetero import HeteroEngine
        pair, db = self.pair, self.db
        eng = pair.src_engine
        where = {}
        for t in tables:
            chosen = sample.get(t)
            key = eng.neutral_key("src", db, t)
            if chosen is None:
                rows = eng._rows_of("src", db, t) if hasattr(
                    eng, "_rows_of") else None
                if rows is not None and rows > KEYLESS_WHOLE:
                    self.notes.append(f"{t} has no key and {rows:,} rows:"
                                      " not in the fixture")
                    where[t] = "1 = 0"
                continue
            if not chosen:
                where[t] = "1 = 0"
                continue
            # the engine's own quoting of a name, looked up for its engine
            # only (a PostgreSQL engine has no MySQL quoting to ask for)
            q = {"mysql": lambda: eng._my_ident, "mssql": lambda: eng._q,
                 "oracle": lambda: eng._q}.get(
                eng.CANON_ENGINE, lambda: self._pg_q)()
            lit = eng.CANON_ENGINE
            if len(key) == 1:
                where[t] = (f"{q(key[0])} in ("
                            + ", ".join(_literal(lit, k[0]) for k in chosen)
                            + ")")
            else:
                where[t] = (f"({', '.join(q(k) for k in key)}) in ("
                            + ", ".join("(" + ", ".join(
                                _literal(lit, v) for v in k) + ")"
                                for k in chosen) + ")")
        into_src = HeteroEngine(Hop(
            name=f"{self.hop.name}-proof-sample", engine="hetero",
            source=self.hop.source, target=self.src.hop.target,
            databases=[db], exclude=self.hop.exclude,
            include=list(getattr(self.hop, "include", None) or []),
            options={"source_engine": pair.src_name,
                     "target_engine": pair.src_name},
            mapping={"where": where}))
        said = []
        log = said.append
        for t in tables:
            sch, _, tbl = str(t).rpartition(".")
            into_src.move_table(db, sch, tbl, SAMPLE_ROWS * 4, _ck(), log)
        for t in tables:
            if t not in self.tables:
                continue
            sch, _, tbl = str(t).rpartition(".")
            self.twin.move_table(db, sch, tbl, SAMPLE_ROWS * 4, _ck(), log)
        for made in (into_src, self.twin):
            try:
                made.settle_target(db, from_source=False)
            except Exception:  # noqa: BLE001 - counters are set per call
                pass
        # the tables both sides have: one the target does not have yet is
        # said, and the routines that read it fail there as they would
        wrong = [r for t in self.tables for r in self.twin.check_data(
            db, table=t) if r.status not in ("ok", "skip")]
        missing = sorted(set(tables) - set(self.tables))
        if missing:
            self.notes.append("not on the target, so not in its sandbox:"
                              f" {', '.join(missing[:6])}")
        if wrong:
            raise NoSandbox("the fixture did not arrive equal in the two"
                            f" sandboxes ({wrong[0].scope}: "
                            f"{wrong[0].detail[:100]}), so no difference"
                            " found there could be told from the data's")
