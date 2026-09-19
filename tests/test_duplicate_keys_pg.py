"""Rows a unique index should have refused, and the reason you cannot ask it.

When the sort order underneath a text index changes - glibc 2.28 is the
famous one - the index is still sorted, by rules nothing uses any more. A
lookup walks past the row it wanted, so an INSERT that should have collided
finds nothing in its way and lands. The constraint is still in the catalog,
still marked unique and valid, and has quietly stopped meaning anything.

Reproduced here by taking the index out of maintenance the way a broken one
behaves, letting a duplicate in, and putting the catalog back to claiming it
is fine. Measured on the result, before any of this was written:

    index scan says   1
    seq scan says     2

And the part that decides the whole design. On a 200,001-row table holding
one duplicate its unique index never recorded, the planner chose
`Index Only Scan using big_email` for `group by ... having count(*) > 1` all
by itself, and reported **0 duplicates**. With `enable_indexscan`,
`enable_bitmapscan` and `enable_indexonlyscan` off, the same query on the
same data reported **1**. Hunting duplicates without shutting those paths
off is a false negative dressed as an all-clear.

These tests run on their own containers: they edit `pg_index` on purpose.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 15524, 15525
NAMES = {SRC: "migkit-test-dup-src", DST: "migkit-test-dup-dst"}
WHY = "the sort order an index was built under has changed"


@pytest.fixture(scope="module")
def dup_pair():
    for port, name in NAMES.items():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "POSTGRES_PASSWORD=test", "-p", f"{port}:5432",
                        "postgres:16"], check=True, capture_output=True)
    for port in NAMES:
        end = time.time() + 120
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(1)
        for _ in range(60):
            if subprocess.run(["docker", "exec", NAMES[port], "pg_isready",
                               "-U", "postgres"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"postgres on {port} never answered")
    yield {"src": SRC, "dst": DST}
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def q(port, sql):
    got = subprocess.run(
        ["docker", "exec", NAMES[port], "psql", "-U", "postgres", "-At", "-q",
         "-v", "ON_ERROR_STOP=1", "-c", sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(autouse=True)
def clean(dup_pair):
    yield
    for port in NAMES:
        q(port, "drop table if exists public.people cascade;"
                " drop table if exists public.big cascade;")


def _engine(dup_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="dup", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=dup_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=dup_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _sneak_in_a_duplicate(port, table, index, values):
    """An index out of maintenance accepts what it should have refused, and
    the catalog afterwards says it is perfectly healthy - which is what a
    collation change leaves behind."""
    q(port, f"update pg_index set indisready = false where indexrelid ="
            f" '{index}'::regclass")
    q(port, f"insert into public.{table} values {values}")
    q(port, f"update pg_index set indisready = true, indisvalid = true"
            f" where indexrelid = '{index}'::regclass")


def test_a_duplicate_under_a_unique_index_is_found(dup_pair, tmp_path):
    q(dup_pair["src"], "create table public.people (id int primary key,"
                       " email text);"
                       " insert into public.people values (1,'a@x.com');")
    q(dup_pair["src"], "create unique index people_email on public.people"
                       " (email)")
    _sneak_in_a_duplicate(dup_pair["src"], "people", "people_email",
                          "(2,'a@x.com')")

    # the state really is the one being claimed: unique, valid, and lying
    assert q(dup_pair["src"], "select indisunique::int::text||indisvalid"
                              "::int::text from pg_index where indexrelid ="
                              " 'people_email'::regclass") == "11"

    got = _engine(dup_pair, tmp_path)._duplicate_keys("postgres", WHY)
    assert got.status == "diff", got.detail
    assert "source public.people.people_email" in got.detail, got.detail
    assert "a@x.com" in got.detail, got.detail
    assert "stopped being enforced" in got.detail, got.detail


def test_the_planner_is_not_allowed_to_answer_with_the_broken_index(
        dup_pair, tmp_path):
    """The measurement this check exists for. Big enough that an index-only
    scan is the cheap plan, so the planner reaches for the very index that
    cannot be trusted - and comes back with nothing."""
    port = dup_pair["src"]
    q(port, "create table public.big (id bigint primary key, email text);"
            " insert into public.big select g, 'u'||g||'@x.com' from"
            " generate_series(1,200000) g;")
    q(port, "create unique index big_email on public.big (email)")
    _sneak_in_a_duplicate(port, "big", "big_email", "(999999,'u1@x.com')")
    q(port, "vacuum analyze public.big")

    plan = q(port, "explain (costs off) select email from public.big"
                   " group by email having count(*) > 1")
    assert "Index Only Scan" in plan, plan
    naive = q(port, "select count(*) from (select email from public.big"
                    " group by email having count(*) > 1) s")
    assert naive == "0", f"expected the planner to miss it, got {naive}"

    got = _engine(dup_pair, tmp_path)._duplicate_keys("postgres", WHY)
    assert got.status == "diff", got.detail
    assert "big_email" in got.detail, got.detail
    assert "u1@x.com" in got.detail, got.detail


def test_without_a_reason_it_does_not_read_anything(dup_pair, tmp_path):
    """A sequential scan of every table to prove a negative is not a check,
    it is a bill. The hunt waits until something else has said an index
    cannot be trusted."""
    q(dup_pair["src"], "create table public.people (id int primary key,"
                       " email text unique);"
                       " insert into public.people values (1,'a@x.com');")
    got = _engine(dup_pair, tmp_path)._duplicate_keys("postgres", "")
    assert got.status == "skip", got.detail
    assert "prove a negative" in got.detail, got.detail


def test_a_clean_table_says_what_it_read(dup_pair, tmp_path):
    q(dup_pair["src"], "create table public.people (id int primary key,"
                       " email text unique);"
                       " insert into public.people select g, 'u'||g from"
                       " generate_series(1,50) g;")
    got = _engine(dup_pair, tmp_path)._duplicate_keys("postgres", WHY)
    assert got.status == "ok", got.detail
    assert "1 unique indexes over text were read" in got.detail, got.detail
    assert "without the planner being allowed to consult them" in got.detail


def test_partial_and_expression_indexes_are_counted_not_guessed(dup_pair,
                                                                 tmp_path):
    """Grouping by the columns of a partial index answers a different
    question than the index does, so it is left alone and said out loud."""
    q(dup_pair["src"], "create table public.people (id int primary key,"
                       " email text, live boolean);"
                       " insert into public.people values"
                       " (1,'a@x.com',true),(2,'a@x.com',false);")
    q(dup_pair["src"], "create unique index people_live on public.people"
                       " (email) where live")
    got = _engine(dup_pair, tmp_path)._duplicate_keys("postgres", WHY)
    assert got.status == "skip", got.detail
    assert "1 partial or expression indexes were not checked" in got.detail


def test_the_deep_report_hunts_only_when_something_else_complained(
        dup_pair, tmp_path):
    q(dup_pair["src"], "create table public.people (id int primary key,"
                       " email text);"
                       " insert into public.people values (1,'a@x.com');")
    q(dup_pair["src"], "create unique index people_email on public.people"
                       " (email)")
    _sneak_in_a_duplicate(dup_pair["src"], "people", "people_email",
                          "(2,'a@x.com')")
    eng = _engine(dup_pair, tmp_path)

    quiet = [r for r in eng.check_deep("postgres")
             if r.scope.endswith("duplicate keys")]
    assert len(quiet) == 1, [r.scope for r in quiet]
    assert quiet[0].status == "skip", quiet[0].detail

    # now give it a reason, the way a real glibc upgrade would
    q(dup_pair["src"], "update pg_database set datcollversion = '2.17'"
                       " where datname = current_database()")
    try:
        loud = [r for r in eng.check_deep("postgres")
                if r.scope.endswith("duplicate keys")]
        assert loud[0].status == "diff", loud[0].detail
        assert "a@x.com" in loud[0].detail
    finally:
        q(dup_pair["src"], "alter database postgres refresh collation"
                           " version")


def test_every_catalog_query_the_engine_carries_actually_parses(dup_pair):
    """A guard against a mistake made three times in this repository: a
    query that concatenates its columns into one string, ordered `by 1, 2`.
    PostgreSQL rejects it, the check reports `error`, and the report reads
    as a failure to connect rather than a typo. Running each one against a
    live server costs milliseconds and ends the whole family."""
    from migkit.engines.postgres import PostgresEngine

    queries = {name: value for name, value in
               vars(PostgresEngine).items()
               if name.isupper() and isinstance(value, str)
               and value.lstrip().lower().startswith("select")}
    assert len(queries) >= 5, sorted(queries)
    for name, sql in sorted(queries.items()):
        # a query that takes parameters still has to parse; NULL stands in
        # for the value so the shape of the statement is what is tested
        ready = sql.replace("%%", "%").replace("%s", "null")
        got = subprocess.run(
            ["docker", "exec", NAMES[dup_pair["src"]], "psql", "-U",
             "postgres", "-At", "-q", "-v", "ON_ERROR_STOP=1", "-c", ready],
            capture_output=True, text=True)
        assert got.returncode == 0, f"{name}: {got.stderr}"


def test_the_verdict_needs_no_server(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="d", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    hit = eng._duplicate_hunt_result(
        "x", [("target", "public.t", "t_u", "email", 3, '{"email":"a"}')],
        2, 0, WHY, "hint")
    assert hit.status == "diff" and "3 duplicated values" in hit.detail

    clean = eng._duplicate_hunt_result("x", [], 2, 0, WHY, "hint")
    assert clean.status == "ok" and "2 unique indexes" in clean.detail

    quiet = eng._duplicate_hunt_result("x", [], 0, 0, "", "hint")
    assert quiet.status == "skip" and "prove a negative" in quiet.detail

    # a reason with nothing to hunt through is not an all-clear
    nothing = eng._duplicate_hunt_result("x", [], 0, 1, WHY, "hint")
    assert nothing.status == "skip", nothing.detail
    assert "no unique index over a text column" in nothing.detail
    assert "1 partial or expression" in nothing.detail


def test_mysql_states_the_gap_instead_of_hunting_on_a_guess(tmp_path):
    """Measured on 8.4: `unique_checks = 0` - which mysqldump writes into
    every dump, and which InnoDB is documented as being allowed to honour by
    skipping the check - still rejected the duplicate with error 1062,
    because the index was cached. Unreproduced is not absent, so migkit
    neither hunts on a guess nor implies the database is safe."""
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="d", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    got = MySQLEngine(hop)._duplicate_keys("x")
    assert got.status == "skip", got.detail
    assert "1062" in got.detail, got.detail
    assert "Unreproduced is not absent" in got.detail, got.detail
