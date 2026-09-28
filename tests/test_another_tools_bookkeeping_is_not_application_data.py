"""Another tool's bookkeeping is named as its leftovers and never moved or
compared as the application's rows.

A source that a replicator, a loader or an online schema change has
touched carries their state: pglogical's and Spock's schemas, Bucardo's
deltas, pg_repack's log tables, the sentinel a bulk copy steers its change
stream by, a loader's load table, gh-ost's checkpoint. None of it is the
application's. Compared as data it is a difference on every run - the
target never had it - and copied it is somebody else's state landed on a
new server. The leftovers check named none of these.
"""
import subprocess
import time

import pytest

from migkit import leftovers as lo
from migkit.config import Endpoint, Hop


def _hop(engine="postgres", exclude=()):
    return Hop(name="x", engine=engine,
               source=Endpoint(host="10.0.0.1", port=5432, user="u",
                               password="p"),
               target=Endpoint(host="10.0.0.2", port=5432, user="u",
                               password="p"),
               databases=["appdb"], exclude=list(exclude))


NAMED = [
    ("_orders_ghk", "gh-ost"), ("percona", "Percona Toolkit"),
    ("pgcopydb", "a PostgreSQL bulk copy"), ("pglogical", "pglogical"),
    ("pgl_appdb_provider_sub1", "pglogical"), ("spock", "Spock"),
    ("spk_appdb_n1_sub", "Spock"), ("bucardo", "Bucardo"),
    ("repack", "pg_repack"), ("pg_repack", "pg_repack"),
    ("pgstream", "pgstream"), ("pgstream_appdb_slot", "pgstream"),
    ("_dlt_loads", "dlt"), ("_dlt_load_id", "dlt"),
    ("_sling_loaded_at", "Sling"), ("_PEERDB_IS_DELETED", "PeerDB"),
    ("peerflow_slot_orders", "PeerDB"), ("_airbyte_raw_id", "Airbyte"),
    ("airbyte_internal", "Airbyte"),
]


@pytest.mark.parametrize("name,tool", NAMED, ids=[n for n, _ in NAMED])
def test_each_tools_objects_are_named_after_it(name, tool):
    assert lo.whose(name) == tool


def test_a_plain_word_is_matched_whole_not_as_a_prefix():
    """`repack` and `spock` are words an application uses too."""
    for name in ("repackaging", "spockfans", "percona_sales", "pgcopydb2",
                 "orders_ghk", "dlt_loads", "bucardos", "airbyte_internals",
                 "pgstreamline"):
        assert lo.whose(name) == "", name


def test_a_column_is_matched_by_its_own_name():
    by = lo.group([("column", "public.orders._sling_loaded_at"),
                   ("column", "public.orders.total")])
    assert by == {"Sling": ["column public.orders._sling_loaded_at"]}


BOOKKEEPING = [
    (("appdb", "pgcopydb", "sentinel"), "a PostgreSQL bulk copy"),
    (("appdb", "pglogical", "node"), "pglogical"),
    (("appdb", "spock", "subscription"), "Spock"),
    (("appdb", "bucardo", "delta_orders"), "Bucardo"),
    (("appdb", "repack", "log_16390"), "pg_repack"),
    (("appdb", "pgstream", "schema_log"), "pgstream"),
    (("appdb", "airbyte_internal", "appdb_raw__stream_x"), "Airbyte"),
    (("appdb", "public", "_dlt_loads"), "dlt"),
    (("appdb", "public", "_dlt_pipeline_state"), "dlt"),
    (("appdb", "public", "_airbyte_raw_orders"), "Airbyte"),
    (("appdb", "_orders_ghk"), "gh-ost"),
    (("appdb", "_orders_ghc"), "gh-ost"),
    (("percona",), "Percona Toolkit"),
]


@pytest.mark.parametrize("parts,tool", BOOKKEEPING,
                         ids=[".".join(p) for p, _ in BOOKKEEPING])
def test_bookkeeping_is_known_by_where_it_lives(parts, tool):
    assert lo.bookkeeping(*parts) == tool
    assert _hop().excluded(*parts)


APPLICATION = [
    ("appdb", "public", "orders"),
    # gh-ost's ghost and deleted tables hold application rows
    ("appdb", "_orders_gho"), ("appdb", "_orders_del"),
    # a database the hop names is the operator's choice, whatever it is
    # called
    ("percona", "checksums"),
    # a loader's column stays with its table
    ("appdb", "public", "_sling_loaded_at"),
    ("appdb", "public", "repack_jobs"),
]


@pytest.mark.parametrize("parts", APPLICATION,
                         ids=[".".join(p) for p in APPLICATION])
def test_application_data_is_not_taken_for_bookkeeping(parts):
    assert lo.bookkeeping(*parts) == ""
    assert not _hop().excluded(*parts)


def test_a_key_or_a_topic_is_the_applications_whatever_its_name():
    for engine in ("redis", "kafka"):
        hop = _hop(engine)
        assert not hop.excluded("0", "percona")
        assert not hop.excluded("0", "_dlt_loads")


def test_the_hops_own_exclusions_still_apply():
    hop = _hop(exclude=["audit_log"])
    assert hop.excluded("appdb", "public", "audit_log")
    assert not hop.excluded("appdb", "public", "orders")


def test_the_table_copier_is_not_handed_them(monkeypatch):
    """Measured before the fix: the PostgreSQL table copier listed every
    table, the ones the hop excludes too - a target-owned table was
    overwritten with the source's rows."""
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop(exclude=["audit_log"]))
    monkeypatch.setattr(eng, "_psql", lambda side, db, sql: "\n".join((
        "pgcopydb|sentinel", "public|_dlt_loads", "public|audit_log",
        "public|orders", "spock|node")))
    assert eng.list_move_tables("appdb") == [("public", "orders")]


def test_the_bulk_paths_leave_them_out_without_an_exclude_list():
    from migkit import movers
    tables = ["pgcopydb.sentinel", "public._dlt_loads", "public.orders",
              "public._orders_ghk"]
    got = movers.excluded_tables(_hop(), "appdb", tables)
    assert got == ["pgcopydb.sentinel", "public._dlt_loads",
                   "public._orders_ghk"]


# ---- a real source ---------------------------------------------------------

NAME, PORT, TPORT = "migkit-test-out-pg", 16094, 16095


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _psql(port, sql, db="postgres"):
    name = NAME if port == PORT else NAME + "t"
    r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                        "psql", "-U", "postgres", "-d", db, "-At", "-v",
                        "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


@pytest.fixture(scope="module")
def pgs():
    if not _docker():
        pytest.skip("docker not available")
    try:
        for n, port in ((NAME, PORT), (NAME + "t", TPORT)):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                            "POSTGRES_PASSWORD=test", "-p",
                            f"127.0.0.1:{port}:5432", "postgres:16", "-c",
                            "wal_level=logical"], check=True,
                           capture_output=True)
        for n in (NAME, NAME + "t"):
            for _ in range(60):
                r = subprocess.run(["docker", "exec", n, "pg_isready", "-U",
                                    "postgres"], capture_output=True)
                if r.returncode == 0:
                    break
                time.sleep(1)
            time.sleep(1)
        # a table a loader once wrote, with the loader's column in it: the
        # application's, carried and compared whole
        app = ("create table public.orders (id int primary key, total int,"
               " _sling_loaded_at timestamptz);"
               " insert into public.orders values (1, 10, '2026-01-01'),"
               " (2, 20, '2026-01-01');")
        _psql(PORT, app)
        _psql(TPORT, app)
        # what the tools leave on a source, by the names they use
        _psql(PORT, "create schema pgcopydb; create table pgcopydb.sentinel"
                    " (startpos pg_lsn, endpos pg_lsn, apply bool);"
                    " insert into pgcopydb.sentinel values ('0/0', '0/0',"
                    " false);"
                    " create schema spock; create table spock.node (id int);"
                    " insert into spock.node values (1);"
                    " create schema bucardo; create table bucardo.delta_orders"
                    " (id int); insert into bucardo.delta_orders values (2);"
                    " create schema repack; create table repack.log_16390"
                    " (id bigserial);"
                    " create table public._dlt_loads (load_id text);"
                    " insert into public._dlt_loads values ('1695000000.1');"
                    " create table public._orders_ghk (id int);")
        # a slot is made in a transaction of its own
        _psql(PORT, "select pg_create_logical_replication_slot("
                    "'pgl_postgres_provider_sub1', 'pgoutput');")
        yield
    finally:
        for n in (NAME, NAME + "t"):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _pg_hop():
    return Hop(name="bk", engine="postgres",
               source=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                               password="test"),
               target=Endpoint(host="127.0.0.1", port=TPORT, user="postgres",
                               password="test"),
               databases=["postgres"])


@pytest.mark.docker
def test_the_source_check_names_each_tool(pgs, tmp_path):
    from migkit import tls
    from migkit.engines.postgres import PostgresEngine
    # (and the TLS probe, against a server that offers none)
    tls._SEEN.clear()
    assert tls.probe("127.0.0.1", PORT, "postgres")[0] == "none"
    items = PostgresEngine(_pg_hop())._mover_leftovers()
    said = " ".join(f"{i['item']} {i['detail']}" for i in items)
    for tool in ("a PostgreSQL bulk copy", "Spock", "Bucardo", "pg_repack",
                 "dlt", "gh-ost", "pglogical", "Sling"):
        assert f"{tool}:" in said, (tool, said)
    # the slot is the one that costs something now
    assert any(i["level"] == "fail" and "pgl_postgres_provider_sub1"
               in i["item"] for i in items), items


@pytest.mark.docker
def test_the_check_compares_the_application_and_only_it(pgs, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = _pg_hop()
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    eng = PostgresEngine(hop)
    counts = eng.check_counts("postgres")
    assert [r.status for r in counts] == ["ok"] * len(counts), \
        [(r.scope, r.status, r.detail) for r in counts]
    assert not any("sentinel" in f"{r.scope} {r.detail}" or "_dlt_loads"
                   in f"{r.scope} {r.detail}" for r in counts)
    # and it still says "different" where the application differs
    _psql(TPORT, "update public.orders set total = 99 where id = 2")
    try:
        data = eng.check_data("postgres")
        assert any(r.status == "diff" and "orders" in f"{r.scope} {r.detail}"
                   for r in data), [(r.scope, r.status) for r in data]
    finally:
        _psql(TPORT, "update public.orders set total = 20 where id = 2")
    assert ("public", "orders") in eng.list_move_tables("postgres")
    assert not [t for t in eng.list_move_tables("postgres")
                if t[0] != "public" or t[1].startswith("_")]
    # nor is its schema a difference in the schema's comparison
    schema = eng.check_schema("postgres")
    assert [r.status for r in schema] == ["ok"] * len(schema), (
        [(r.scope, r.status, r.detail) for r in schema],
        [f.read_text()[:3000] for f in tmp_path.rglob("structural-fix.sql")])


@pytest.mark.docker
def test_a_whole_database_dump_leaves_their_schemas_behind(pgs, tmp_path):
    from migkit import movers
    hop = _pg_hop()
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    steps = movers.pgdump_move(hop, "postgres", 1, False, None)
    argv = next(s.argv for s in steps if getattr(s, "argv", None)
                and s.argv[0] == "pg_dump")
    for schema in ("pgcopydb", "spock", "bucardo", "repack"):
        assert ["-N", schema] == argv[argv.index(schema) - 1:
                                      argv.index(schema) + 1], argv
