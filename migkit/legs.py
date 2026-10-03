"""The fewest legs the rows can take, and where migkit should stand
(backlog R22, item 4).

Every other way a PostgreSQL database moves takes its rows through the
machine migkit runs on: read from the source, written to the target, the
wide network crossed twice where that machine is far from both. Two ways
take none of them through it - the target pulls them from the source
itself - and the hop turns them on with `server_to_server:` (`true`, or
`{source: host:port}` where the target reaches the source at another
address than migkit does), because each leaves objects on the servers
while it runs and the operator says whether they may:

* **the target pulling through a foreign server**: the foreign-data
  extension, a server pointing at the source, a user mapping holding the
  source's credentials and a schema of the source's tables imported as
  foreign tables, all on the target; each range of a table is one
  `INSERT ... SELECT` from its foreign table, several at a time. The
  credentials are dropped the moment the last row is in, the rest right
  after, and the plan says all of it before anything is made;
* **a temporary subscription copying the tables** (the same major version
  on both sides, so the rows go in the servers' own binary form): a
  publication of the tables on the source and the slot the subscription
  makes there, the subscription on the target, all dropped once every
  table is copied. It writes to the source's catalogue, which is why it
  costs more footprint than the pull and why the hop's leave covers it.

MySQL's target cloning itself from the source is the physical rungs'
own (`physical-clone`), and ClickHouse's `INSERT ... SELECT` from
`remote()` is chosen inside that engine's own bulk path, table by table:
`DIRECT` names the rungs whose rows never cross migkit, so the plan and
the placement advice know them, without a second copy of either here.

Measured on the shared PostgreSQL 16 pair, one range of 200,000 rows
(40 MB) each way, the links held up by a proxy adding half the round
trip each way (`test_the_target_pulls_from_the_source.py`, two runs,
seconds):

    round trip          0 ms         10 ms          50 ms
    the pull            0.34, 0.42   0.79, 0.92     0.96, 1.05
      fetching 100      0.42, 0.48   24.69, 25.25
    the subscription    0.42, 0.31   0.84, 0.76     2.1, 2.0
    migkit's own copy,
      beside the target 0.48, 0.51   0.46, 0.41     0.65, 0.77
      beside the source       0.44         0.58           1.16
      far from both     0.45, 0.38   0.52, 0.6      1.04, 1.08

The pull's cursor pays a round trip a fetch - the foreign server's own
100 rows a fetch took 25 seconds over a 10 ms link, so the fetch is sized
from the round trip (5,241 rows there, 26,208 at 50 ms) - where
migkit's copy streams and pays it once a range. So beside the target,
where migkit's writes wait on no wide round trip, its own copy is the
faster by a quarter (0.77 against 1.05 at 50 ms) and the servers do not
pull; where the writes would cross the link - beside the source (1.16)
or far from both (1.04, 1.08) - the pull is as fast or faster (0.96,
1.05), and takes the rows across the link once instead of through a
machine whose own network carries them in and out again. What that saves
where the link's width binds a proxy that only delays does not show, and
is not measured here. The subscription's copy takes several round trips
a table before its first row and is the slower of the two past a few
milliseconds (2.0 against 1.05 at 50 ms): it is the rung below the pull.

Where migkit should run comes from the round trips it measures - to the
source, to the target, and the servers' own where one was asked (the
pull's proof times its round trips from the target) - `placement` says
it in a line: beside the target when the rows go through it, since the
writer's statements each wait on a round trip and the reads stream; the
relay beside the source where a tunnel's machine can read there (R17d);
the servers pulling, offered where migkit is far from the target and the
hop does not allow it yet; and nowhere in particular when the rows go
server to server. Every round trip is a statement's (`round_trips`):
opening a connection is answered by whatever is in front of the server.
"""
import re
import threading
import time

from .util import split_table

#: the ways a whole database's rows go from server to server without
#: crossing migkit's machine; `physical-clone` is the MySQL physical
#: rungs', named here and not built
DIRECT = ("pgpull", "pgsubscribe", "physical-clone")
#: a round trip past which a side is across a wide network, not beside
#: the machine migkit runs on
FAR_MS = 20.0
#: what a range pulled through the foreign server aims to carry a second,
#: which with the round trip sets how many rows each fetch brings
AIM_BYTES_S = 100 * 2 ** 20
#: rows a fetch may bring, and the memory the target may hold for one
FETCH_LEAST, FETCH_MOST, FETCH_BYTES = 1_000, 200_000, 64 * 2 ** 20
#: rows a range of a table pulled in one statement
RANGE_ROWS = 250_000
#: seconds the target is given to reach the source when it first dials
DIAL_SECONDS = 10


def allowed(hop):
    """Whether the hop lets the servers pull from each other."""
    return bool((getattr(hop, "options", None) or {}).get(
        "server_to_server"))


def source_for_target(hop):
    """(host, port) the target dials the source at, or None where only
    this machine's own address for it is known."""
    said = (getattr(hop, "options", None) or {}).get("server_to_server")
    if isinstance(said, dict) and said.get("source"):
        host, _, port = str(said["source"]).rpartition(":")
        if not host or not port.isdigit():
            host, port = str(said["source"]), str(hop.source.port)
        return host, int(port)
    ep = hop.source
    far = (ep.options or {}).get("tunnel_to")
    if far:
        host, _, port = str(far).rpartition(":")
        return host, int(port)
    if str(ep.host or "") in ("127.0.0.1", "localhost", "::1", ""):
        return None
    return str(ep.host), int(ep.port)


def _far_tls(hop):
    """The TLS the target asks of the source when it dials it: the
    source's own, as it was decided (its options, or what asking the
    server found), as far as it can be carried (`tls_forms`'s `far`) -
    `verify-ca` and `verify-full` need the authority's certificate in a
    file on the target's own machine, which migkit cannot put there, so
    they are carried as `require` (encrypted, the certificate not
    checked) and the plan says so."""
    return dict(hop.source.tls_forms("postgres")["far"])


def conninfo(hop, db, host=None, port=None):
    """The source as the target dials it - a subscription's connection
    string - built one way for every path that has the target dial: each
    value quoted as libpq reads it (a quote in a password ended the value
    written as SQL quotes it), the source's TLS as far as a server can
    carry it (`_far_tls`), keepalives and a bounded dial (audit
    2026-10-01, SEC13). `host`/`port` where the caller knows how the
    target reaches the source."""
    from .engines.postgres import _conninfo_value, keepalives
    if host is None:
        host, port = source_for_target(hop) or (hop.source.host,
                                                hop.source.port)
    parts = {"host": host, "port": port, "dbname": db,
             "user": hop.source.user, "password": hop.source.password or "",
             "connect_timeout": DIAL_SECONDS, **keepalives(),
             **_far_tls(hop)}
    return " ".join(f"{k}={_conninfo_value(v)}" for k, v in parts.items())


def unlogged(cur):
    """Ask the server not to log this session's statements, where the
    user may (a superuser's): the next statement holds the source's
    credentials, and under `log_statement = ddl` the server's log keeps
    them. A session that may not is left as it is, and the plan says so
    (`logs_ddl`). On a connection in autocommit, where a refused setting
    leaves nothing open."""
    try:
        cur.execute("set log_statement = 'none'")
    except Exception:  # noqa: BLE001 - not a superuser's session
        pass


def _connect(ep, dbname, autocommit=True):
    """A connection made as every PostgreSQL connection of migkit's is
    (`engines.postgres.connect`)."""
    from .engines.postgres import connect
    conn = connect(ep, dbname)
    conn.autocommit = autocommit
    return conn


def _one(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()


def _words(e):
    """A server's refusal in its own words, one line."""
    from .movers import DRIVEN
    from .wording import without_programs
    text = str(getattr(e, "pgerror", None) or e).strip()
    return without_programs(text.splitlines()[0][:300] if text else
                            type(e).__name__, DRIVEN)


def _first_db(hop):
    return (list(hop.databases or []) or ["postgres"])[0]


#: roles a managed service's administrator holds in place of a superuser
ADMIN_ROLES = ("rds_superuser", "azure_pg_admin", "cloudsqlsuperuser",
               "alloydbsuperuser")

TARGET_FACTS_SQL = """
select exists(select 1 from pg_available_extensions
              where name = 'postgres_fdw'),
       exists(select 1 from pg_extension where extname = 'postgres_fdw'),
       (select rolsuper from pg_roles where rolname = current_user),
       exists(select 1 from pg_auth_members m
                join pg_roles r on r.oid = m.roleid
                join pg_roles u on u.oid = m.member
               where u.rolname = current_user and r.rolname = any(%s)),
       case when exists(select 1 from pg_foreign_data_wrapper
                        where fdwname = 'postgres_fdw')
            then has_foreign_data_wrapper_privilege('postgres_fdw', 'USAGE')
       end,
       has_database_privilege(current_database(), 'CREATE'),
       current_setting('server_version_num')::int,
       exists(select 1 from pg_auth_members m
                join pg_roles r on r.oid = m.roleid
                join pg_roles u on u.oid = m.member
               where u.rolname = current_user
                 and r.rolname = 'pg_create_subscription'),
       current_setting('log_statement')
"""

SOURCE_FACTS_SQL = """
select exists(select 1 from pg_largeobject_metadata),
       current_setting('server_version_num')::int,
       current_setting('wal_level'),
       (select rolsuper or rolreplication from pg_roles
         where rolname = current_user),
       current_setting('max_replication_slots')::int
         - (select count(*) from pg_replication_slots),
       current_setting('max_wal_senders')::int
         - (select count(*) from pg_stat_replication),
       (select rolsuper from pg_roles where rolname = current_user)
         or (has_database_privilege(current_database(), 'CREATE')
             and not exists(
               select 1 from pg_class c
                 join pg_namespace n on n.oid = c.relnamespace
                where c.relkind in ('r', 'p')
                  and n.nspname not in ('pg_catalog', 'information_schema')
                  and n.nspname not like 'pg\\_%'
                  and not pg_has_role(c.relowner, 'USAGE')))
"""


def facts(hop, db=None):
    """What the two server-to-server ways need to know, asked of both
    servers with read-only questions, once. A side that cannot be asked
    answers with `error`, which every need then reads as missing."""
    db = db or _first_db(hop)
    out = {}
    try:
        conn = _connect(hop.target, hop.target_db(db))
        try:
            (avail, installed, sup, admin, usage, create, version, subs,
             logged) = _one(conn, TARGET_FACTS_SQL, (list(ADMIN_ROLES),))
        finally:
            conn.close()
        may = bool(sup or admin or (installed and usage)) and bool(create)
        out["target"] = {"available": avail, "installed": installed,
                         "may_pull": may, "version": version,
                         "may_subscribe": bool(sup or subs),
                         "logs_ddl": logged in ("ddl", "all")}
    except Exception as e:  # noqa: BLE001 - said by the needs
        out["target"] = {"error": _words(e)}
    try:
        conn = _connect(hop.source, db)
        try:
            lobs, version, wal, repl, slots, senders, publish = _one(
                conn, SOURCE_FACTS_SQL)
        finally:
            conn.close()
        out["source"] = {"large_objects": lobs, "version": version,
                         "wal_level": wal, "replication": bool(repl),
                         "free_slots": slots, "free_senders": senders,
                         "may_publish": bool(publish)}
    except Exception as e:  # noqa: BLE001
        out["source"] = {"error": _words(e)}
    return out


def _t(f, key):
    return (f["legs"].get("target") or {}).get(key)


def _s(f, key):
    return (f["legs"].get("source") or {}).get(key)


def _unasked(f):
    for side in ("target", "source"):
        why = (f["legs"].get(side) or {}).get("error")
        if why:
            return f"the {side} could not be asked: {why}"
    return None


def rungs():
    """The two ways a PostgreSQL database's rows go server to server, as
    `movers._ladder` rungs, in the order measured (see the module's
    table): the needs each has of the hop and the servers, and the proof
    the pull gives at the start of a move."""
    from .decide import Need, Rung
    from .movers import _moves
    leave = Need("the hop's leave",
                 lambda f: allowed(f.get("hop")),
                 "{rung} leaves objects on the servers while it runs, and"
                 " the hop does not allow them (hop option"
                 " server_to_server)")
    dial = Need("the source's address as the target sees it",
                lambda f: source_for_target(f["hop"]) is not None,
                "the source is at an address of this machine's own, which"
                " the target cannot dial: server_to_server: {source:"
                " host:port} says where the target reaches it")
    asked = Need("both servers asked", lambda f: _unasked(f) is None,
                 lambda f: _unasked(f))
    far = Need("this machine far from the target",
               lambda f: far_from_target(f["rtt"]), _near)
    no_lobs = Need("no large objects on the source",
                   lambda f: not _s(f, "large_objects"),
                   "the source holds large objects, which {rung} does not"
                   " carry")
    pull = Rung(
        "pgpull", "the target pulling from the source",
        needs=(_moves("pgpull"), leave, dial, far, asked,
               Need("the foreign-data extension on the target",
                    lambda f: _t(f, "available"),
                    "the target does not offer the foreign-data extension"
                    " {rung} goes through"),
               Need("leave to make a foreign server",
                    lambda f: _t(f, "may_pull"),
                    lambda f: f"{f['hop'].target.user} on the target may"
                              " neither make the foreign-data extension nor"
                              " a server with it: a superuser, the managed"
                              " service's admin role, or USAGE on it and"
                              " CREATE on the database"),
               no_lobs),
        prove=lambda f: (prove_pull(f["hop"], f.get("db"))
                         if f.get("go") else None),
        because="the rows cross the network once, from server to server",
        footprint=1)
    subscribe = Rung(
        "pgsubscribe", "a temporary subscription on the target",
        needs=(_moves("pgsubscribe"), leave, dial, far, asked,
               Need("the same major version on both sides",
                    lambda f: (_s(f, "version") or 0) // 10000
                    == (_t(f, "version") or -1) // 10000,
                    lambda f: f"the source is {_s(f, 'version')} and the"
                              f" target {_t(f, 'version')}: a temporary"
                              " subscription copies in the servers' own"
                              " binary form, which only the same version"
                              " reads"),
               Need("the source's log written for logical decoding",
                    lambda f: _s(f, "wal_level") == "logical",
                    lambda f: f"the source's wal_level is"
                              f" {_s(f, 'wal_level')}, and a subscription"
                              " needs logical - migkit changes no server"
                              " setting"),
               Need("a free replication slot and sender on the source",
                    lambda f: (_s(f, "free_slots") or 0) > 0
                    and (_s(f, "free_senders") or 0) > 0,
                    "the source has no replication slot or sender free for"
                    " {rung}"),
               Need("replication leave on the source",
                    lambda f: _s(f, "replication"),
                    lambda f: f"{f['hop'].source.user} on the source may"
                              " not replicate (REPLICATION)"),
               Need("leave to subscribe on the target",
                    lambda f: _t(f, "may_subscribe"),
                    lambda f: f"{f['hop'].target.user} on the target may"
                              " not make a subscription (a superuser, or"
                              " pg_create_subscription from PostgreSQL 16)"),
               no_lobs),
        prove=lambda f: (prove_subscribe(f["hop"], f.get("db"))
                         if f.get("go") else None),
        because="the rows cross the network once, in the servers' own"
                " binary form",
        footprint=2, changes=(PUBLICATION,))
    return (pull, subscribe)


def _publication_stand(f):
    """Where the temporary subscription's change on the source stands, as
    the source answered: migkit's user may make a publication of every
    table and drop it again - the slot the subscription makes with the
    replication right its own need asks for - or only a DBA can."""
    from .decide import BY_DBA, POSSIBLE, Stand
    why = _unasked(f)
    if why:
        return Stand(BY_DBA, why)
    if not _s(f, "may_publish"):
        return Stand(BY_DBA, f"{f['hop'].source.user} on the source may not"
                             " make a publication of the tables (CREATE on"
                             " the database, and every table its own)")
    return Stand(POSSIBLE, "migkit's user may make it and drop it again")


def _copy_name(hop):
    return _names(hop).replace("pull", "copy")


def _publication_ctx(f):
    """What the publication's statements are written with: the database,
    its name, and the tables the copy carries - listed from the source
    where the facts do not hold them."""
    from .sourcechange import Ctx
    hop = f["hop"]
    db = f.get("db") or _first_db(hop)
    tables = f.get("published")
    if tables is None:
        why = _unasked(f) if "legs" in f else None
        try:
            if why:
                raise RuntimeError(why)
            tables = _published(_carried(hop, db)[1])
        except Exception as e:  # noqa: BLE001 - said in the statement
            tables = [f"<the tables the copy carries, which could not be"
                      f" listed: {_words(e)}>"]
    return Ctx(db=db, name=_copy_name(hop), tables=tables)


def _published(carried):
    from .util import qualify
    return [qualify("postgres", *split_table("postgres", t, "public"))
            for t in carried]


#: the temporary subscription's one change on the source: a publication of
#: the tables it copies, and the slot the subscription makes, dropped once
#: every table is copied (`sourcechange`). The hop's `server_to_server` is
#: its answer given ahead: the objects the copy makes on the servers
from . import sourcechange as _sourcechange  # noqa: E402
PUBLICATION = _sourcechange.change("temporary_publication",
                                   _publication_stand, _publication_ctx)


def _near(f):
    """Why a server-to-server way is passed over for a machine beside the
    target: its own copy measured faster there (the module's table)."""
    ms = (f["rtt"] or {}).get("dst")
    if ms is None:
        return ("the round trip to the target could not be measured, and"
                " the servers pulling from each other was measured faster"
                " only for a machine far from the target")
    return (f"this machine is {ms} ms from the target, where its own copy"
            " measured faster than the servers pulling from each other"
            " (0.77 against 1.05 seconds a 40 MB range over a 50 ms link)")


def direct(via):
    """Whether `via` takes the rows from server to server."""
    return via in DIRECT


# --- the target pulling through a foreign server -----------------------------

def _names(hop):
    base = re.sub(r"[^a-z0-9_]", "_", str(hop.name).lower())[:40]
    return f"migkit_pull_{base}"


def _q(name):
    """A name as PostgreSQL reads it, the one way (`postgres._ident`)."""
    from .engines.postgres import _ident
    return _ident(name)


def _lit(text):
    """A text as a string constant, the one way (`postgres._literal`)."""
    from .engines.postgres import _literal
    return _literal(text)


class Footprint:
    """What the pull leaves on the target while it runs, made in one
    place and taken away in one place, whichever way the pull ends: the
    extension (only where migkit made it), the server, the user mapping
    with the source's credentials, and a schema of foreign tables for each
    of the source's schemas.

    The user mapping is made on a session that asks the target not to log
    its statements where it may (`log_statement` for the session - a
    superuser's); where it may not and the target logs DDL, the statement
    holding the credentials is in its log, and the plan says so."""

    def __init__(self, hop, db, log=None):
        self.hop, self.db, self.log = hop, db, log
        self.server = _names(hop)
        self.made_extension = False
        self.schemas = {}
        self.conn = None

    def __enter__(self):
        self.conn = _connect(self.hop.target, self.hop.target_db(self.db))
        try:
            return self._make()
        except BaseException:
            # half made is taken away as a whole one is
            self.__exit__(None, None, None)
            raise

    def _make(self):
        self._clear()
        host, port = source_for_target(self.hop)
        with self.conn.cursor() as cur:
            cur.execute("select exists(select 1 from pg_extension"
                        " where extname = 'postgres_fdw')")
            if not cur.fetchone()[0]:
                cur.execute("create extension postgres_fdw")
                self.made_extension = True
            # the keepalives every connection of migkit's has: a link that
            # goes quiet mid-range ends the fetch in about two minutes
            # instead of holding it for the operating system's two hours
            from .engines.postgres import keepalives
            opts = {"host": host, "port": str(port), "dbname": self.db,
                    "connect_timeout": str(DIAL_SECONDS),
                    "application_name": "migkit",
                    **{k: str(v) for k, v in keepalives().items()},
                    **_far_tls(self.hop)}
            cur.execute(f"create server {_q(self.server)} foreign data"
                        " wrapper postgres_fdw options ("
                        + ", ".join(f"{k} {_lit(v)}" for k, v in
                                    opts.items()) + ")")
            unlogged(cur)
            cur.execute(f"create user mapping for current_user server"
                        f" {_q(self.server)} options (user %s, password %s)",
                        (self.hop.source.user, self.hop.source.password or ""))
        return self

    def schema_for(self, source_schema):
        """The target's schema holding the source schema's foreign tables."""
        if source_schema not in self.schemas:
            local = f"{self.server}_{len(self.schemas)}"
            with self.conn.cursor() as cur:
                cur.execute(f"create schema {_q(local)}")
            self.schemas[source_schema] = local
        return self.schemas[source_schema]

    def import_tables(self, source_schema, tables, fetch=None):
        """The source's `tables` of `source_schema` as foreign tables; a
        fetch size for each where one is given ({table: rows})."""
        local = self.schema_for(source_schema)
        with self.conn.cursor() as cur:
            cur.execute(
                f"import foreign schema {_q(source_schema)} limit to ("
                + ", ".join(_q(t) for t in tables) + ") from server"
                f" {_q(self.server)} into {_q(local)}")
            for t, n in (fetch or {}).items():
                cur.execute(f"alter foreign table {_q(local)}.{_q(t)}"
                            f" options (add fetch_size {_lit(int(n))})")
        return local

    def drop_credentials(self):
        """The user mapping - the source's credentials on the target -
        gone, the moment nothing needs it."""
        if self.conn is None or self.conn.closed:
            return
        with self.conn.cursor() as cur:
            cur.execute(f"drop user mapping if exists for current_user"
                        f" server {_q(self.server)}")

    def _clear(self):
        """What a pull that died left, taken away before a new one is
        made under the same names."""
        mine = re.compile(re.escape(self.server) + r"_\d+$")
        with self.conn.cursor() as cur:
            cur.execute("select nspname from pg_namespace")
            left = [r[0] for r in cur.fetchall() if mine.match(r[0])]
            cur.execute("select 1 from pg_foreign_server where srvname = %s",
                        (self.server,))
            if cur.fetchone():
                cur.execute(f"drop user mapping if exists for current_user"
                            f" server {_q(self.server)}")
                cur.execute(f"drop server {_q(self.server)} cascade")
            for s in left:
                cur.execute(f"drop schema if exists {_q(s)} cascade")

    def __exit__(self, *exc):
        if self.conn is None:
            return False
        try:
            if self.conn.closed:
                self.conn = _connect(self.hop.target,
                                     self.hop.target_db(self.db))
            else:
                # a statement that failed inside a transaction block would
                # leave every statement below refused
                with self.conn.cursor() as cur:
                    try:
                        cur.execute("rollback")
                    except Exception:  # noqa: BLE001 - none was open
                        pass
            self.drop_credentials()
            self._clear()
            if self.made_extension:
                with self.conn.cursor() as cur:
                    try:
                        cur.execute("drop extension if exists postgres_fdw")
                    except Exception:  # noqa: BLE001 - another uses it
                        if self.log:
                            self.log("the foreign-data extension migkit"
                                     " made is left: something else on the"
                                     " target now uses it")
        finally:
            self.conn.close()
        return False


def prove_pull(hop, db=None):
    """None where the target reaches the source through a foreign server
    and reads from it, else why not in the target's own words. The round
    trips the target measures on the way are kept (`servers_seen`): the
    servers' own view of the link, for the placement advice."""
    db = db or _first_db(hop)
    try:
        with Footprint(hop, db) as fp:
            local = fp.schema_for("pg_catalog")
            with fp.conn.cursor() as cur:
                cur.execute(f"create foreign table {_q(local)}.probe"
                            " (datname name) server"
                            f" {_q(fp.server)} options (schema_name"
                            " 'pg_catalog', table_name 'pg_database')")
                cur.execute("begin")
                took = []
                for _ in range(5):
                    began = time.monotonic()
                    cur.execute(f"select 1 from {_q(local)}.probe limit 1")
                    cur.fetchall()
                    took.append(time.monotonic() - began)
                cur.execute("commit")
    except Exception as e:  # noqa: BLE001 - the proof's answer
        return f"the target could not read the source through it: {_words(e)}"
    # the first opened the connection; the rest are one round trip each
    # through the foreign server's cursor (declare, fetch, close)
    _keep_seen(hop, round(min(took[1:]) * 1000 / 3, 2))
    return None


def prove_subscribe(hop, db=None):
    """None where the target reaches the source to subscribe, else why
    not in the target's own words: a subscription made with nothing to
    copy and no slot, which dials the source to read the publication's
    tables and is dropped at once - it leaves nothing on either side."""
    db = db or _first_db(hop)
    name = _names(hop).replace("pull", "probe")
    host, port = source_for_target(hop)
    dials = conninfo(hop, db, host, port)
    try:
        conn = _connect(hop.target, hop.target_db(db))
    except Exception as e:  # noqa: BLE001
        return f"the target could not be reached: {_words(e)}"
    try:
        with conn.cursor() as cur:
            cur.execute("select 1 from pg_subscription where subname = %s",
                        (name,))
            if cur.fetchone():
                cur.execute(f"alter subscription {_q(name)} set (slot_name"
                            " = none)")
                cur.execute(f"drop subscription {_q(name)}")
            cur.execute(f"set statement_timeout = {DIAL_SECONDS * 4000}")
            unlogged(cur)
            try:
                cur.execute(f"create subscription {_q(name)} connection"
                            f" {_lit(dials)} publication {_q(name)} with"
                            " (connect = true, enabled = false, create_slot"
                            " = false, slot_name = none, copy_data = false)")
            except Exception as e:  # noqa: BLE001 - the proof's answer
                return ("the target could not reach the source to subscribe:"
                        f" {_words(e)}")
            cur.execute(f"drop subscription {_q(name)}")
    finally:
        conn.close()
    return None


def _seen_path(hop):
    return hop.report_dir() / "servers-seen.json"


def _keep_seen(hop, ms):
    from .durable import write_json
    from .util import stamp
    write_json(_seen_path(hop), {"target_to_source_ms": ms, "at": stamp()})


def servers_seen(hop):
    """The round trip the target measured to the source, ms, or None - a
    reading that can be taken again, so a file a crash cut reads as none
    (`durable.read_state`)."""
    from .durable import read_state
    try:
        got = read_state(_seen_path(hop), "cache")
    except OSError:
        return None
    return got.get("target_to_source_ms") if isinstance(got, dict) \
        else None


def _fetch_rows(row_bytes, rtt_s):
    """Rows a fetch brings: enough to carry `AIM_BYTES_S` a round trip,
    within what the target may hold for it."""
    row_bytes = max(float(row_bytes or 100), 1.0)
    want = AIM_BYTES_S * max(rtt_s, 0.001) / row_bytes
    most = min(FETCH_MOST, FETCH_BYTES / row_bytes)
    return int(max(1, min(max(FETCH_LEAST, want), most)))


def _ranges(lo, hi, rows):
    """[(from, to)] covering lo..hi in about `RANGE_ROWS` rows each, the
    last one closed."""
    if lo is None or hi is None:
        return [(None, None)]
    n = max(1, min(int((rows or 0) // RANGE_ROWS) + 1, 10_000))
    step = max(1, (int(hi) - int(lo) + n) // n)
    out, at = [], int(lo)
    while at <= int(hi):
        out.append((at, min(at + step, int(hi) + 1)))
        at += step
    return out


def pull_move(hop, db, workers, go, log):
    """The plan's tables pulled by the target from the source, several
    ranges at a time; the rest left to the table copier as every routing
    bulk path leaves them."""
    from . import movers, planner
    from .engines.postgres import PostgresEngine
    from .wording import Step, phase
    eng = PostgresEngine(hop)
    tables = eng._all_tables("src", db)
    try:
        known = eng.table_facts("src", db)
    except Exception:  # noqa: BLE001 - ranges then from the keys alone
        known = {}
    decisions = planner.plan(hop, db, "pgpull", tables, "public", known)
    carried = _leaves(eng, db, [d.table for d in decisions
                                if d.path == planner.BULK])
    routed = [d.table for d in decisions if d.path == planner.COPIER]
    host, port = source_for_target(hop) or ("?", 0)
    steps = [
        f"# on the target, for the length of the pull: the foreign-data"
        f" extension (made now where it is not there, and dropped after),"
        f" a foreign server to the source at {host}:{port}, a user mapping"
        f" holding {hop.source.user}'s credentials - dropped the moment the"
        f" last row is in - and a schema of the source's tables as foreign"
        f" tables",
        Step(phase("pull", tables=len(carried), workers=workers,
                   routed=len(routed)))]
    try:
        logged = facts(hop, db)["target"].get("logs_ddl")
    except Exception:  # noqa: BLE001
        logged = None
    steps[1:1] = _tls_note(hop)
    if logged:
        steps.insert(1, "# the target logs DDL statements, and one of them"
                        " holds the source's password: where migkit's user"
                        " may not stop that for its own session, the"
                        " target's log keeps it - a source user made for"
                        " the move and dropped after it is the safer one")
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    movers._pg_create_missing(hop, db, log)
    movers._pg_truncate_target(hop, db, log)
    quiet = movers._pg_quiet_triggers(hop, db, carried)
    if log:
        log(steps[-1])
    rtt = (servers_seen(hop) or 1.0) / 1000
    by_schema = {}
    for name in carried:
        sch, tbl = eng._split(name)
        by_schema.setdefault(sch, []).append(tbl)
    with Footprint(hop, db, log) as fp:
        work = []
        for sch, tbls in sorted(by_schema.items()):
            fetch = {}
            for t in tbls:
                f = known.get(f"{sch}.{t}") or {}
                rows, size = f.get("rows") or 0, f.get("bytes") or 0
                fetch[t] = _fetch_rows(size / rows if rows else 100, rtt)
            local = fp.import_tables(sch, tbls, fetch)
            for t in tbls:
                work += _table_work(hop, eng, db, sch, t, local, known)
        with movers._IndexWindow(hop, db, workers, log):
            _run_ranges(hop, db, work, workers, quiet, log)
            fp.drop_credentials()
    movers._pg_carry_sequences(hop, db, log)
    movers._pg_finish_created(hop, db, log)
    return steps


def _tls_note(hop):
    """The plan's line where the target's connection to the source is
    checked less than migkit's own: the source's certificate verified by
    migkit - asked for, or found to verify (`tls.auto`) - and carried as
    `require` by the target (`_far_tls`)."""
    mode = hop.source.tls_forms("postgres")["mode"]
    if mode not in ("verify-ca", "verify-full"):
        return []
    return [f"# migkit checks the source's certificate ({mode}); the"
            " target dialing the source encrypts but cannot check it -"
            " the authority's certificate would have to be a file on the"
            " target's own machine"]


def _leaves(eng, db, tables):
    """The tables of `tables` that hold rows: a partitioned table's rows
    are its partitions', and pulling both would bring each row twice."""
    held = {f"{s}.{t}" for s, t in eng.list_move_tables(db)}
    return [t for t in tables if t in held]


def _without_identity(eng, db, tables):
    """The tables of `tables` whose changes the source could not publish:
    REPLICA IDENTITY NOTHING."""
    if not tables:
        return []
    got = eng._psql("src", db, "select n.nspname || '.' || c.relname from"
                               " pg_class c join pg_namespace n on n.oid ="
                               " c.relnamespace where c.relreplident = 'n'")
    none = {x for x in got.splitlines() if x}
    return [t for t in tables if t in none]


def _table_work(hop, eng, db, sch, tbl, local, known):
    """[(label, statement)] for one table: a statement a range where it
    has an integer key, one for the whole table where it has not."""
    cols = eng._copy_cols(db, sch, tbl)
    if not cols:
        raise RuntimeError(f"{sch}.{tbl}: its columns could not be read on"
                           " the source")
    tsch, ttbl = split_table("postgres", hop.target_table_in(db, sch, tbl),
                             sch)
    qt = f"{_q(tsch)}.{_q(ttbl)}"
    names = ", ".join(_q(c) for c in cols)
    override = eng._insert_override("dst", db, f"{tsch}.{ttbl}", cols)
    head = (f"insert into {qt} ({names}){override} select {names} from"
            f" {_q(local)}.{_q(tbl)}")
    key = eng._int_pk(db, sch, tbl)
    if not key:
        return [(f"{sch}.{tbl}", head)]
    got = eng._psql("src", db, f"select min({_q(key)})||'|'||max({_q(key)})"
                              f" from {_q(sch)}.{_q(tbl)}").strip()
    lo, _, hi = got.partition("|")
    if not lo:
        return []
    rows = (known.get(f"{sch}.{tbl}") or {}).get("rows")
    spans = _ranges(int(lo), int(hi), rows)
    return [(f"{sch}.{tbl}", f"{head} where {_q(key)} >= {a}"
                             f" and {_q(key)} < {b}") for a, b in spans]


def _run_ranges(hop, db, work, workers, quiet, log):
    """Each statement on one of `workers` connections to the target, one
    transaction each; the first failure stops the rest and is raised in
    the target's words."""
    import queue
    todo = queue.Queue()
    for w in work:
        todo.put(w)
    done, failed, lock = {}, [], threading.Lock()
    totals = {}
    for label, _ in work:
        totals[label] = totals.get(label, 0) + 1

    def one():
        conn = None
        try:
            conn = _connect(hop.target, hop.target_db(db), autocommit=False)
            with conn.cursor() as cur:
                if quiet in ("flag", "session"):
                    cur.execute("set session_replication_role = replica")
                conn.commit()
                while not failed:
                    try:
                        label, sql = todo.get_nowait()
                    except queue.Empty:
                        return
                    cur.execute(sql)
                    n = cur.rowcount
                    conn.commit()
                    with lock:
                        rows, ranges = done.get(label, (0, 0))
                        done[label] = (rows + max(n, 0), ranges + 1)
                        if done[label][1] == totals[label] and log:
                            log(f"{label}: {done[label][0]:,} rows pulled")
        except Exception as e:  # noqa: BLE001 - raised below, once
            with lock:
                failed.append(e)
        finally:
            if conn is not None:
                conn.close()
    threads = [threading.Thread(target=one, daemon=True)
               for _ in range(max(1, min(int(workers or 1), len(work))))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if failed:
        raise RuntimeError(f"the target's pull stopped: {_words(failed[0])}")
    ran = sum(n for _, n in done.values())
    if ran != len(work):
        # never "complete" over a range nobody ran
        raise RuntimeError(f"the target's pull ran {ran} of its {len(work)}"
                           " ranges")


# --- a temporary subscription ------------------------------------------------

def _carried(hop, db):
    """(engine, the tables the temporary subscription copies, the ones the
    table copier carries): one reading of the plan, for the copy and for
    the statements its change on the source is said with."""
    from . import planner
    from .engines.postgres import PostgresEngine
    eng = PostgresEngine(hop)
    tables = eng._all_tables("src", db)
    decisions = planner.plan(hop, db, "pgsubscribe", tables, "public",
                             eng.table_facts("src", db))
    carried = _leaves(eng, db, [d.table for d in decisions
                                if d.path == planner.BULK])
    return eng, carried, [d.table for d in decisions
                          if d.path == planner.COPIER]


def subscribe_move(hop, db, workers, go, log, poll=0.5):
    """The plan's tables copied by a subscription made for the copy and
    dropped once each is copied: a publication of them on the source, the
    subscription on the target, in the servers' binary form."""
    from . import movers, stall
    from .wording import Step, phase
    eng, carried, routed = _carried(hop, db)
    unmarked = _without_identity(eng, db, carried)
    if unmarked:
        raise SystemExit(
            f"{', '.join(unmarked[:5])}" + (" ..." if len(unmarked) > 5
                                            else "")
            + " have a key and REPLICA IDENTITY NOTHING: in a publication"
            " the source would refuse its application's updates and"
            " deletes on them while the copy runs. Nothing has been made;"
            " move without server_to_server, or give them an identity")
    host, port = source_for_target(hop) or ("?", 0)
    name = _names(hop).replace("pull", "copy")
    steps = [
        f"# on the source, for the length of the copy: a publication of"
        f" the {len(carried)} tables and the replication slot the"
        f" subscription makes; on the target a subscription reaching the"
        f" source at {host}:{port}, holding {hop.source.user}'s credentials"
        f" - all dropped once every table is copied",
        Step(phase("subscribe-copy", tables=len(carried),
                   routed=len(routed)))]
    steps[1:1] = _tls_note(hop)
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    if not carried:
        return steps
    movers._pg_create_missing(hop, db, log)
    # the keys before the copy, not after it: the changes the subscription
    # applies while it catches up find their rows by them
    movers._pg_finish_created(hop, db, log)
    movers._pg_truncate_target(hop, db, log)
    if log:
        log(steps[-1])
    src = _connect(hop.source, db)
    dst = _connect(hop.target, hop.target_db(db))
    dials = conninfo(hop, db, host, port)
    # from 16 the first copy of each table goes in the binary form too;
    # before it only the changes after it would
    binary = _one(dst, "select current_setting('server_version_num')::int"
                  )[0] >= 160000
    from . import sourcechange

    def on_source(statement):
        if not statement.lstrip().startswith("--"):
            with src.cursor() as cur:
                cur.execute(statement)
    try:
        with movers._IndexWindow(hop, db, workers, log):
            try:
                _drop_copy(src, dst, name)
                # the publication made as a change on the source: behind
                # the hop's approvals, recorded before it is made and taken
                # off after, or by the next run where this one is killed
                with sourcechange.making(
                        hop, PUBLICATION, {"hop": hop, "db": db,
                                           "published": _published(carried)},
                        on_source, db, log or (lambda m: None)):
                    try:
                        _subscribe(dst, name, dials, binary)
                        _wait_copied(dst, name, len(carried), log, poll,
                                     stall.seconds())
                    finally:
                        # the slot is the subscription's until it goes
                        _drop_copy(src, dst, name)
            finally:
                # before the indexes are built again: the subscription
                # would apply the source's changes into every build
                _drop_copy(src, dst, name)
    finally:
        src.close()
        dst.close()
    movers._pg_carry_sequences(hop, db, log)
    return steps


def _subscribe(dst, name, dials, binary):
    """The subscription made, bounded in time: the target dials the source
    inside the statement, and only a statement timeout ends a dial that
    hangs (`PostgresEngine.DIALS_ACROSS`)."""
    with dst.cursor() as cur:
        cur.execute(f"set statement_timeout = {DIAL_SECONDS * 4000}")
        unlogged(cur)
        try:
            cur.execute(f"create subscription {_q(name)} connection"
                        f" {_lit(dials)} publication {_q(name)} with"
                        f" (copy_data = true"
                        + (", binary = true" if binary else "") + ")")
        except Exception as e:  # noqa: BLE001 - said in its words
            raise RuntimeError(
                "the target could not subscribe to the source (the target"
                " dials the source for it, so this is the route from target"
                f" to source): {_words(e)}")
        finally:
            cur.execute("set statement_timeout = 0")


def _wait_copied(dst, name, n, log, poll, stalled_after):
    """Until every table of subscription `name` is ready. A table whose
    copy keeps failing, or a copy that moves nothing for `stalled_after`
    seconds, stops it."""
    seen, moved, said = None, time.monotonic(), 0
    while True:
        with dst.cursor() as cur:
            cur.execute(
                "select r.srsubstate, count(*) from pg_subscription_rel r"
                " join pg_subscription s on s.oid = r.srsubid"
                " where s.subname = %s group by 1", (name,))
            states = dict(cur.fetchall())
            cur.execute("select coalesce(sum(bytes_processed), 0) from"
                        " pg_stat_progress_copy")
            copied = int(cur.fetchone()[0])
            errors = 0
            try:
                cur.execute("select sync_error_count from"
                            " pg_stat_subscription_stats where subname = %s",
                            (name,))
                row = cur.fetchone()
                errors = int(row[0]) if row else 0
            except Exception:  # noqa: BLE001 - before PostgreSQL 15
                errors = 0
        ready = states.get("r", 0)
        now = (ready, copied, tuple(sorted(states.items())))
        if now != seen:
            seen, moved = now, time.monotonic()
        if log and ready > said:
            log(f"{ready} of {n} tables copied")
            said = ready
        if ready >= n:
            return
        if errors:
            raise RuntimeError(
                f"the target's copy of a table failed {errors} time(s); the"
                " target's log says why, and the copy is stopped rather"
                " than retried for good")
        if time.monotonic() - moved > stalled_after:
            raise RuntimeError(f"the subscription's copy moved nothing for"
                               f" {stalled_after:.0f}s")
        time.sleep(poll)


def _drop_copy(src, dst, name):
    """The subscription, its slot and the publication, where they are -
    the slot dropped on the source directly where the subscription can no
    longer reach it."""
    with dst.cursor() as cur:
        cur.execute("select 1 from pg_subscription where subname = %s",
                    (name,))
        if cur.fetchone():
            cur.execute("set statement_timeout = '60s'")
            try:
                cur.execute(f"drop subscription {_q(name)}")
            except Exception:  # noqa: BLE001 - the source out of reach
                cur.execute(f"alter subscription {_q(name)} disable")
                cur.execute(f"alter subscription {_q(name)} set (slot_name"
                            " = none)")
                cur.execute(f"drop subscription {_q(name)}")
            cur.execute("set statement_timeout = 0")
    with src.cursor() as cur:
        # a sender still ending holds its slot for a moment after the
        # subscription let go of it
        for _ in range(20):
            cur.execute("select active from pg_replication_slots where"
                        " slot_name = %s", (name,))
            row = cur.fetchone()
            if row is None:
                break
            if not row[0]:
                cur.execute("select pg_drop_replication_slot(%s)", (name,))
                break
            time.sleep(0.5)
        cur.execute(f"drop publication if exists {_q(name)}")


# --- where migkit should run -------------------------------------------------

def placement(src_ms, dst_ms, servers_ms=None, direct_way=False,
              relay=False, may_pull=False):
    """Where migkit should run, from the round trips measured to each
    side (ms, None where not measured), and the servers' own where one was
    asked: a line, or "" where it already stands where it should."""
    if direct_way:
        return ("the rows go from server to server and none cross this"
                " machine, so where migkit runs does not change the speed"
                + (f"; the servers are {servers_ms} ms apart"
                   if servers_ms is not None else ""))
    if src_ms is None or dst_ms is None:
        return ""
    near_src, near_dst = src_ms < FAR_MS, dst_ms < FAR_MS
    out = ""
    if not near_src and not near_dst:
        out = (f"this machine is {src_ms} ms from the source and {dst_ms} ms"
               " from the target, so every row crosses a wide network"
               " twice. Run migkit next to the target - the side written"
               " to, whose statements wait on each round trip - so only the"
               " reads travel")
    elif near_src and not near_dst:
        out = (f"this machine is beside the source and {dst_ms} ms from the"
               " target: every statement the writer sends waits a round"
               " trip across the wide network. Run migkit - or its writer -"
               " next to the target, so the reads travel as one stream and"
               " the writes do not")
    elif not near_src and near_dst and relay:
        out = (f"this machine is beside the target and {src_ms} ms from the"
               " source; the reads are made beside the source, on the"
               " tunnel's machine, and cross once, compressed")
    if not near_dst and not may_pull:
        # where the writes would cross the link, the servers pulling was
        # measured as fast or faster (the module's table)
        more = ("the target pulling the rows from the source itself was"
                " measured as fast or faster for a machine this far from the"
                " target, and takes them across the link once (hop option"
                " server_to_server)")
        if servers_ms is not None and servers_ms < dst_ms:
            more = f"the servers are {servers_ms} ms apart; {more}"
        out = f"{out}; {more}" if out else more
    return out


def round_trips(hop, db=None):
    """{"src": ms, "dst": ms}: the least of a few `select 1`s on a
    connection to each side, None where one cannot be made - the round
    trip a statement pays, through whatever carries the connection (a
    tunnel's included). Opening a connection is not timed: a proxy or a
    pooler in front of a server answers it alone."""
    db = db or _first_db(hop)
    out = {}
    for side, ep, name in (("src", hop.source, db),
                           ("dst", hop.target, hop.target_db(db))):
        try:
            conn = _connect(ep, name)
        except Exception:  # noqa: BLE001 - not measured, not said
            out[side] = None
            continue
        try:
            took = []
            with conn.cursor() as cur:
                for _ in range(4):
                    began = time.monotonic()
                    cur.execute("select 1")
                    cur.fetchall()
                    took.append(time.monotonic() - began)
            out[side] = round(min(took) * 1000, 2)
        except Exception:  # noqa: BLE001
            out[side] = None
        finally:
            conn.close()
    return out


def far_from_target(ms):
    """Whether this machine is across a wide network from the target -
    where its writes would wait on the link, and the servers pulling was
    measured as fast or faster (the module's table)."""
    b = (ms or {}).get("dst")
    return b is not None and b >= FAR_MS


def plan_line(hop, via, db=None):
    """The plan's line about where migkit stands, from the round trips it
    measures now, or ""."""
    ms = round_trips(hop, db)
    relay = False
    try:
        from . import tunnel
        t = tunnel.of(hop.source)
        relay = bool(t and t.has("psql", "zstd"))
    except Exception:  # noqa: BLE001
        relay = False
    return placement(ms.get("src"), ms.get("dst"), servers_seen(hop),
                     direct(via), relay, allowed(hop))
