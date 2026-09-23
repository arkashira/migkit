"""What the consumer says, and what the target did.

A convergence proof rests on one number, and migkit's has always been
`confirmed_flush_lsn` on the source - what the replication consumer
reported back. That is not the same as what landed on the target, and the
gap runs in both directions. Measured on PostgreSQL 16, one pair, the same
insert load:

    native subscription   origin *ahead* of the slot by up to 46 KB
    pgcopydb follow       slot ahead of the origin while the target held
                          0 rows

`pg_replication_origin_status.remote_lsn` is written inside the applying
transaction by both paths, so it is the honest one: "committed here".

This file pins that primitive - and, just as importantly, pins why it is
**not** used as the fence. The obvious change is to fence on the origin
instead, and it is wrong: the origin stops at the last applied transaction
while the source's WAL keeps moving. Measured on an idle healthy pair,
five samples three seconds apart:

    pg_current_wal_lsn   0/19EB660
    confirmed_flush_lsn  0/19EB660   (keepalives carry it to the end)
    origin remote_lsn    0/19EB540   (288 bytes back, and staying)

`origin >= lsn` never becomes true there. A fence built on it times out on
every quiet pair and falls through to the weaker sleep-settle path without
saying so. That was written, measured, and reverted; the test at the
bottom is what stops it being written again.

The other trap is that origins are cluster-wide. Connected to `postgres`
with the only subscription living in `other`:

    external_id | remote_lsn
    pgcopydb    | 0/15B9810
    pg_16407    | 0/0

Taking `min(remote_lsn)` across that would read a brand-new subscription
for a database nobody asked about and answer `0/0` for ever.
"""
import ast
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

SRC, DST = 15583, 15584
NS, ND = "migkit-test-origin-src", "migkit-test-origin-dst"


def _hop(tmp_path, name="fence-probe", dbs=("postgres",)):
    hop = Hop(name=name, engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST, user="postgres",
                              password="test"),
              databases=list(dbs))
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _engine(tmp_path, **kw):
    from migkit.engines.postgres import PostgresEngine
    return PostgresEngine(_hop(tmp_path, **kw))


def test_the_origin_name_says_which_hop_and_which_database(tmp_path):
    """pgcopydb's default is the bare word `pgcopydb`. Two databases
    following at once would share it, and origins are cluster-wide, so the
    second would overwrite the first's position."""
    eng = _engine(tmp_path)
    assert eng.follow_origin("appdb") == "migkit_fence_probe_appdb"
    assert eng.follow_origin("other") != eng.follow_origin("appdb")


def test_the_name_is_an_identifier_even_when_the_hop_is_not(tmp_path):
    """Hop and database names carry dots and dashes; an origin name is
    matched as a literal, so anything that is not a word character would
    make the comparison miss rather than error - the quiet failure."""
    eng = _engine(tmp_path, name="prod.eu-west/1")
    got = eng.follow_origin("my db")
    assert got.replace("_", "").isalnum(), got
    assert "-" not in got and "." not in got and " " not in got, got


def test_the_name_fits_what_postgres_will_store(tmp_path):
    """`external_id` is NAMEDATALEN-1. A longer name is truncated by the
    server, so migkit would write one name and look for another."""
    eng = _engine(tmp_path, name="h" * 80)
    assert len(eng.follow_origin("d" * 80)) <= 63


@pytest.fixture(scope="module")
def pair():
    for n in (NS, ND):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    for n, p in ((NS, SRC), (ND, DST)):
        subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                        "POSTGRES_PASSWORD=test", "-p", f"{p}:5432",
                        "postgres:16", "-c", "wal_level=logical",
                        "-c", "max_replication_slots=8",
                        "-c", "max_wal_senders=8"],
                       check=True, capture_output=True)
    try:
        for n, p in ((NS, SRC), (ND, DST)):
            end = time.time() + 180
            while time.time() < end:
                with socket.socket() as s:
                    s.settimeout(2)
                    if s.connect_ex(("127.0.0.1", p)) == 0:
                        break
                time.sleep(1)
            for _ in range(90):
                if subprocess.run(["docker", "exec", n, "pg_isready", "-U",
                                   "postgres"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(1)
            else:
                pytest.fail(f"{n} never answered")
        _sql(NS, "create table t (id int primary key, v text)")
        _sql(NS, "insert into t values (1,'a')")
        _sql(NS, "create publication p for all tables")
        _sql(ND, "create table t (id int primary key, v text)")
        # a second database on the target with no subscription of its own -
        # the one whose fence must not be held hostage by this one's origin
        _sql(ND, "create database other")
        ip = subprocess.run(
            ["docker", "inspect", "-f",
             "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", NS],
            capture_output=True, text=True, check=True).stdout.strip()
        _sql(ND, "create subscription s connection 'host=%s port=5432"
                 " dbname=postgres user=postgres password=test'"
                 " publication p" % ip)
        # let the initial COPY finish, but send nothing through the stream
        for _ in range(60):
            if _sql1(ND, "select count(*) from t") == "1":
                break
            time.sleep(1)
        else:
            pytest.fail("the subscription never copied the table")
        yield {"applied_before_any_stream":
               _sql1(ND, "select coalesce(max(remote_lsn)::text,'')"
                         " from pg_replication_origin_status"
                         " where external_id like 'pg\\_%'")}
    finally:
        for n in (NS, ND):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _sql(name, sql, db="postgres"):
    p = subprocess.run(["docker", "exec", name, "psql", "-U", "postgres",
                        "-d", db, "-q", "-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    return p.stdout


def _sql1(name, sql, db="postgres"):
    p = subprocess.run(["docker", "exec", name, "psql", "-U", "postgres",
                        "-d", db, "-tAc", sql], capture_output=True, text=True)
    return p.stdout.strip()


@needs_docker
def test_a_subscription_that_has_only_copied_reads_as_no_position(pair):
    """`0/0` is the absence of a position, not a position. The rows were
    already on the target when this was taken - the origin is only written
    when a *streamed* transaction is applied."""
    assert pair["applied_before_any_stream"] == "0/0", pair


def test_both_shapes_of_no_position_read_as_unknown(tmp_path):
    """"Nowhere yet" arrives in two different shapes, and a caller that
    compared either against a real LSN would conclude the target is
    infinitely behind on a pair that is fine.

    Measured on PostgreSQL 16, and they are genuinely different rows:

        a subscription that has copied but not yet applied a streamed
        transaction   -> present in pg_replication_origin_status as `0/0`
        an origin created and not yet used
                      -> not in that view at all, so the query answers ''
    """
    eng = _engine(tmp_path)
    for raw in ("0/0", "", "  "):
        eng._psql = lambda side, db, sql, _r=raw: _r
        assert eng.applied_lsn("postgres") is None, raw
    eng._psql = lambda side, db, sql: "0/19EB540"
    assert eng.applied_lsn("postgres") == "0/19EB540"


@needs_docker
def test_a_created_origin_is_absent_rather_than_zero(pair, tmp_path):
    """The live half of the pair above: this is where the '' comes from."""
    eng = _engine(tmp_path, dbs=("other",))
    name = eng.follow_origin("other")
    _sql(ND, f"select pg_replication_origin_create('{name}')", db="other")
    try:
        assert _sql1(ND, "select remote_lsn::text from"
                         f" pg_replication_origin_status"
                         f" where external_id = '{name}'", db="other") == ""
        assert eng.applied_lsn("other") is None
    finally:
        _sql(ND, f"select pg_replication_origin_drop('{name}')", db="other")


@needs_docker
def test_applied_lsn_moves_once_a_transaction_is_applied(pair, tmp_path):
    eng = _engine(tmp_path)
    _sql(NS, "insert into t values (2,'b')")
    for _ in range(30):
        got = eng.applied_lsn("postgres")
        if got:
            break
        time.sleep(1)
    assert got and got != "0/0", got
    assert _sql1(ND, "select count(*) from t") == "2"
    # and it is the target's own number, not the source's slot
    assert got == _sql1(ND, "select remote_lsn::text from"
                            " pg_replication_origin_status"
                            " where external_id like 'pg\\_%'")


@needs_docker
def test_an_origin_belonging_to_another_database_is_not_counted(pair,
                                                                tmp_path):
    """`other` has no subscription. The origin for `postgres` is visible
    from it anyway, because origins are cluster-wide."""
    eng = _engine(tmp_path, dbs=("postgres", "other"))
    assert eng.applied_lsn("postgres") is not None
    assert _sql1(ND, "select count(*) from pg_replication_origin_status",
                 db="other") != "0", "the origin really is visible from there"
    assert eng.applied_lsn("other") is None


@needs_docker
def test_an_origin_nobody_can_attribute_is_not_counted(pair, tmp_path):
    """pgcopydb's default origin is the bare name `pgcopydb`, which belongs
    to no database as far as the catalogue is concerned. Counting it would
    let one hop's CDC leg stall another hop's report."""
    _sql(ND, "select pg_replication_origin_create('pgcopydb')", db="other")
    try:
        eng = _engine(tmp_path, dbs=("other",))
        assert eng.applied_lsn("other") is None
    finally:
        _sql(ND, "select pg_replication_origin_drop('pgcopydb')", db="other")


@needs_docker
def test_an_origin_migkit_named_itself_is_counted(pair, tmp_path):
    """The other half, and the reason the naming matters: a CDC leg migkit
    drives has no `pg_subscription` row to be attributed through, so the
    only thing tying its position to a database is the name migkit gave
    it. If that lookup misses, the position silently reads as absent."""
    eng = _engine(tmp_path, dbs=("other",))
    name = eng.follow_origin("other")
    _sql(ND, f"select pg_replication_origin_create('{name}')", db="other")
    try:
        _sql(ND, f"select pg_replication_origin_advance('{name}',"
                 " '0/ABCDEF0')", db="other")
        assert eng.applied_lsn("other") == "0/ABCDEF0"
        # and a hop by another name does not see it
        other_hop = _engine(tmp_path, name="someone-else", dbs=("other",))
        assert other_hop.applied_lsn("other") is None
    finally:
        _sql(ND, f"select pg_replication_origin_drop('{name}')", db="other")


@needs_docker
def test_the_fence_still_passes_on_an_idle_pair(pair, tmp_path):
    """The regression this file exists for.

    Fencing on the origin instead of the slot was written and measured
    before it was reverted. On this exact fixture - healthy, quiet - the
    origin sits at the last applied transaction while the source's WAL has
    moved past it, so the origin can never reach `src_lsn`. The slot does,
    because keepalives carry it. If someone swaps them, this times out.
    """
    eng = _engine(tmp_path)
    # Put WAL on the source that the subscriber will never apply, so the
    # gap is made rather than waited for. `pgoutput` only forwards logical
    # messages when its `messages` option is on, and a plain
    # `CREATE SUBSCRIPTION` does not turn it on - asserted below rather
    # than assumed.
    _sql(NS, "select pg_logical_emit_message(false, 'migkit', 'fence-probe')")
    lsn = eng.src_lsn("postgres")
    t0 = time.monotonic()
    assert eng.fence_wait("postgres", lsn, timeout=45) is True
    assert time.monotonic() - t0 < 30

    # and the thing that makes swapping them tempting is true at the same
    # moment: the target's applied position never reaches that LSN
    time.sleep(3)
    applied = eng.applied_lsn("postgres")
    behind = _sql1(NS, f"select pg_wal_lsn_diff('{lsn}'::pg_lsn,"
                       f" '{applied}'::pg_lsn) > 0")
    assert behind == "t", (lsn, applied)


def test_the_fence_does_not_read_the_target_origin():
    """Stated as code so the docstring cannot drift away from it."""
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "engines" / "postgres.py").read_text()
    fn = next(n for n in ast.walk(ast.parse(src))
              if isinstance(n, ast.FunctionDef) and n.name == "fence_wait")
    body = fn.body[1:] if (fn.body and isinstance(fn.body[0], ast.Expr)
                           and isinstance(fn.body[0].value, ast.Constant)
                           ) else fn.body
    code = "\n".join(ast.dump(s) for s in body)
    assert "confirmed_flush_lsn" in code, "it must still read the slot"
    for forbidden in ("applied_lsn", "remote_lsn", "replication_origin"):
        assert forbidden not in code, forbidden
