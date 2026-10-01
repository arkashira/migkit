"""The PostgreSQL engine's questions go over one connection it keeps per
side and thread, made the one way every connection of its is made (audit
2026-10-01, DU3, DU12, SP2, W15).

Each question - the catalogue, a fence, a marker - started the client
program: a process and a connection of its own, about 0.3 s a question over
a 50 ms link. Connections were opened thirteen ways, with three connect
timeouts, keepalives on four and UTC on one, so a business rule ran in each
server's own zone. And the target's database was mapped through the hop's
`db_map` by the caller and again by the helper at 33 places: a swapped map
read one database for the counts and another for the tables.

What the connection answers is held to what the client program printed,
row for row, and what one question leaves in its session - a setting, a
temporary table, a lock, a transaction - never reaches the next.
"""
import pytest

from tests import servers
from tests.conftest import needs_docker

pytestmark = needs_docker

pair = servers.fixture("pg")


def _fresh(server, *dbs):
    for db in dbs:
        server.sql(f'drop database if exists "{db}" with (force)')
        server.sql(f'create database "{db}"')


def _engine(pair, tmp_path, dbs=None, db_map=None):
    from migkit.config import Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="kept", engine="postgres", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=list(dbs or [pair.name]),
              db_map=dict(db_map or {}))
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


@pytest.fixture
def eng(pair, tmp_path):
    _fresh(pair.src, pair.name)
    _fresh(pair.dst, pair.name)
    got = _engine(pair, tmp_path)
    yield got
    got.release_kept()


#: one value of each kind whose text a reader depends on
KINDS = ("select true, false, null::int, 1.50::numeric, 0.1::float8,"
         " array[1,2,null], '{\"b\": 1, \"a\": [1.10, \"x\"]}'::jsonb,"
         " '\\x00ff'::bytea, timestamptz '2020-01-02 03:04:05+07',"
         " interval '1 day 2 hours', 'a|b'::text, E'two\\nlines'")


def test_it_answers_as_the_client_program_printed(pair, eng):
    db = pair.name
    got = eng._psql("src", db, KINDS)
    printed = eng._psql_program("src", db, KINDS)
    assert got == printed, (got, printed)
    # and the text is the server's, pinned: UTC, ISO, every digit
    assert "2020-01-01 20:04:05+00" in got and "0.1" in got, got
    # every result of a script of several, as the program prints them
    assert eng._psql("src", db, "select 1; select 2") == "1\n2"
    assert eng._psql("src", db, "create table t (id int)") == ""


def test_questions_share_one_connection_and_start_no_program(pair, eng,
                                                            monkeypatch):
    from migkit.engines import postgres
    started = []
    monkeypatch.setattr(postgres, "run",
                        lambda cmd, *a, **k: started.append(cmd))
    pids = {eng._psql("src", pair.name, "select pg_backend_pid()")
            for _ in range(5)}
    assert len(pids) == 1 and started == [], (pids, started)
    # the maintenance database is kept beside it, not in place of it
    eng._psql("src", "postgres", "select 1")
    assert eng._psql("src", pair.name, "select pg_backend_pid()") in pids


def test_what_a_question_leaves_in_its_session_goes_with_it(pair, eng):
    db = pair.name
    eng._psql("src", db, "set search_path = nowhere")
    assert eng._psql("src", db, "show search_path") != "nowhere"
    eng._psql("src", db, "create temp table scratch (id int)")
    # the same name made again: the first went with its question
    eng._psql("src", db, "create temp table scratch (id int)")
    eng._psql("src", db, "select pg_advisory_lock(42)")
    free = pair.src.sql("select pg_try_advisory_lock(42)", db)
    assert free == [(True,)], free
    # a transaction a failed script left open is not the next one's
    with pytest.raises(RuntimeError, match="division by zero"):
        eng._psql("src", db, "begin; select 1/0; commit;")
    assert eng._psql("src", db, "select 7") == "7"
    # a timeout asked for one question ends with it
    with pytest.raises(RuntimeError, match="statement timeout"):
        eng._psql("src", db, "select pg_sleep(3)", statement_timeout="1s")
    assert eng._psql("src", db, "show statement_timeout") == "0"


def test_a_failure_is_the_servers_words_and_a_runtime_error(pair, eng):
    from migkit.engines.postgres import SqlFailed
    with pytest.raises(RuntimeError) as e:
        eng._psql("src", pair.name, "select * from not_there")
    assert isinstance(e.value, SqlFailed)
    assert e.value.sqlstate == "42P01"
    assert 'relation "not_there" does not exist' in str(e.value)


def test_a_connection_gone_is_opened_again_for_a_read_only(pair, eng):
    db = pair.name
    pid = eng._psql("src", db, "select pg_backend_pid()")
    pair.src.sql(f"select pg_terminate_backend({pid})")
    # a read is asked again on a new connection
    assert eng._psql("src", db, "select pg_backend_pid()") != pid
    eng._psql("src", db, "create table w (id int)")
    pid = eng._psql("src", db, "select pg_backend_pid()")
    pair.src.sql(f"select pg_terminate_backend({pid})")
    # a write is not sent again on a new connection: the one it was sent
    # on was gone, so it fails, and nothing was written
    with pytest.raises(RuntimeError):
        eng._psql("src", db, "insert into w values (1)")
    assert eng._psql("src", db, "select count(*) from w") == "0"


def test_a_side_that_cannot_be_reached_is_a_runtime_error(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=1, user="postgres",
                  password="CHANGE_ME")
    eng = PostgresEngine(Hop(name="none", engine="postgres", source=ep,
                             target=ep, databases=["app"]))
    with pytest.raises(RuntimeError):
        eng._psql("src", "app", "select 1")


def test_every_session_is_pinned_to_utc_but_the_zone_question(pair, eng):
    db = pair.name
    pair.src.sql(f'alter database "{db}" set timezone to \'Asia/Bangkok\'')
    # a business rule ran in the server's own zone before
    assert eng.run_rule("src", db, "show timezone") == [("UTC",)]
    assert eng._psql("src", db, "show timezone") == "UTC"
    conn = eng._connect("src", db)
    try:
        with conn.cursor() as cur:
            cur.execute("show timezone")
            assert cur.fetchone() == ("UTC",)
    finally:
        conn.close()
    # the one question about the server's own zone is asked in it
    offset, _, _ = eng.zone_evidence("src", db)
    assert offset == 420, offset


def test_the_target_is_mapped_once(pair, tmp_path):
    """A swapped map: counts read `<db>_blue` while the tables, columns
    and keys read `<db>` (audit 2026-10-01, DU3)."""
    app, blue = pair.name, pair.name + "_blue"
    _fresh(pair.dst, app, blue)
    pair.dst.sql("create table in_app (id int primary key)", app)
    pair.dst.sql("create table in_blue (id int primary key)", blue)
    eng = _engine(pair, tmp_path, dbs=[app, blue],
                  db_map={app: blue, blue: app})
    try:
        assert eng._psql("dst", app, "select current_database()") == blue
        assert eng.neutral_tables("dst", app) == ["public.in_blue"]
        assert eng.neutral_key("dst", app, "public.in_blue") == ["id"]
        assert eng.neutral_tables("dst", blue) == ["public.in_app"]
        conn = eng._connect("dst", app)
        try:
            with conn.cursor() as cur:
                cur.execute("select current_database()")
                assert cur.fetchone() == (blue,)
        finally:
            conn.close()
    finally:
        eng.release_kept()
