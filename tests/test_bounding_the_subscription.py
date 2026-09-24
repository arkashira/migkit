"""The statement that waits two minutes to tell you the network is wrong.

`CREATE SUBSCRIPTION` runs on the target and opens a connection back to the
source while it runs. When that route does not exist, it blocks - and the
parameter that looks like it bounds the wait does not. Measured on
PostgreSQL 16 against an address that drops packets:

    plain conninfo, connect_timeout=10, through psql       10s
    the same conninfo inside CREATE SUBSCRIPTION          135s
    ... with connect_timeout=10 in it as well             135s
    ... under statement_timeout=15s                        15s

`connect_timeout` is enforced by libpq's *synchronous* connect path, and
the walreceiver does not use it, so the parameter is accepted and ignored -
the worst shape a setting can have. `statement_timeout` is the one that
works, and no subscription is created when it fires, so a bounded attempt
costs only the wait it saves. Per database: a hop with five of them spent
eleven minutes silent before the first useful word.

The other half is what the failure leaves behind. The publication on the
source is created by the statement *before* this one and survives, and
PostgreSQL has no `CREATE PUBLICATION ... IF NOT EXISTS` - 16 answers with
a syntax error - so a second attempt stops on `publication "..." already
exists` before it ever reaches the target. Saying "nothing was created"
would be wrong, and would cost the operator a second confusing failure.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

SRC, DST = 15591, 15592
NS, ND = "migkit-test-bound-src", "migkit-test-bound-dst"
NET_A, NET_B = "migkit-test-bound-a", "migkit-test-bound-b"


def _hop(tmp_path, host="127.0.0.1", port=SRC, name="unreach"):
    hop = Hop(name=name, engine="postgres",
              source=Endpoint(host=host, port=port, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST, user="postgres",
                              password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def _engine(tmp_path, **kw):
    from migkit.engines.postgres import PostgresEngine
    return PostgresEngine(_hop(tmp_path, **kw))


def test_an_ordinary_statement_is_still_unbounded(tmp_path):
    """Every existing call runs under `statement_timeout=0`, and a long
    checksum over a large table is exactly the thing that must not acquire
    a deadline because of this."""
    eng = _engine(tmp_path)
    seen = {}
    eng._psql = lambda side, db, sql, statement_timeout=0: seen.setdefault(
        "t", statement_timeout)
    eng.apply_replication_stmt("src", "postgres",
                               "create publication migkit_unreach"
                               " for all tables;")
    assert seen["t"] == 0, seen


def test_the_statement_that_dials_across_is_bounded(tmp_path):
    eng = _engine(tmp_path)
    seen = {}
    eng._psql = lambda side, db, sql, statement_timeout=0: seen.setdefault(
        "t", statement_timeout)
    eng.apply_replication_stmt("dst", "postgres",
                               "create subscription migkit_unreach"
                               " connection '...' publication x;")
    assert seen["t"] == "45s", seen


def test_the_default_of_psql_itself_did_not_change(tmp_path):
    """The parameter is opt-in at the call site; the signature default is
    what every other caller in the codebase relies on."""
    import inspect
    from migkit.engines.postgres import PostgresEngine
    sig = inspect.signature(PostgresEngine._psql)
    assert sig.parameters["statement_timeout"].default == 0


def test_the_wait_can_be_raised(tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_SUBSCRIBE_TIMEOUT", "600")
    eng = _engine(tmp_path)
    seen = {}
    eng._psql = lambda side, db, sql, statement_timeout=0: seen.setdefault(
        "t", statement_timeout)
    eng.apply_replication_stmt("dst", "postgres", "create subscription s ...")
    assert seen["t"] == "600s", seen


@pytest.mark.parametrize("bad", ["soon", "-5", "0", "15s", "1.5"])
def test_a_wait_that_is_not_a_number_is_refused(tmp_path, monkeypatch, bad):
    """Ignoring it would leave the operator waiting the default while
    believing they had changed it."""
    monkeypatch.setenv("MIGKIT_SUBSCRIBE_TIMEOUT", bad)
    eng = _engine(tmp_path)
    eng._psql = lambda *a, **k: ""
    with pytest.raises(SystemExit) as e:
        eng.apply_replication_stmt("dst", "postgres",
                                   "create subscription s ...")
    assert "MIGKIT_SUBSCRIBE_TIMEOUT" in str(e.value)


def test_the_timeout_message_names_the_route_and_the_way_round_it(tmp_path):
    eng = _engine(tmp_path)
    said = eng._subscribe_failed("ERROR:  canceling statement due to"
                                 " statement timeout", 45)
    assert "gave up after 45s" in said, said
    assert "route from target to source" in said, said
    assert "MIGKIT_CDC=follow" in said, said
    assert "MIGKIT_SUBSCRIBE_TIMEOUT" in said, said


def test_the_message_says_what_survived(tmp_path):
    """"Nothing was created" was the first draft and it was false - the
    publication is made by the statement before this one."""
    said = _engine(tmp_path)._subscribe_failed("ERROR:  statement timeout", 45)
    assert "No subscription was created" in said, said
    assert "publication migkit_unreach on the source was" in said, said
    assert "already exists" in said, said
    assert "--drop --go" in said, said


def test_the_line_quoted_back_is_the_one_with_the_reason(tmp_path):
    """psql's last line is the hint, which is the least useful part."""
    from migkit.engines.postgres import PostgresEngine
    err = ('ERROR:  could not connect to the publisher: connection to server'
           ' at "127.0.0.1", port 15589 failed: Connection refused\n'
           '\tIs the server running on that host and accepting TCP/IP'
           ' connections?\n')
    got = PostgresEngine._worst_line(err)
    assert "Connection refused" in got, got
    assert "Is the server running" not in got, got


def test_a_refusal_is_not_described_as_a_timeout(tmp_path):
    said = _engine(tmp_path)._subscribe_failed(
        "ERROR:  could not connect to the publisher: connection to server at"
        ' "127.0.0.1", port 15589 failed: Connection refused', 45)
    assert "could not reach the source" in said, said
    assert "gave up after" not in said, said
    assert "Connection refused" in said, said


def test_every_engine_that_starts_replication_can_run_it():
    """Same contract as `replication_status`: the caller used to pick by
    `hasattr(eng, "_psql")`, which only PostgreSQL has."""
    from migkit.engines import NAMES, _class_for
    from migkit.engines.base import Engine
    offered = [n for n in NAMES
               if _class_for(n) and hasattr(_class_for(n), "replicate_sql")]
    assert set(offered) == {"postgres", "mysql"}, offered
    for n in offered:
        cls = _class_for(n)
        assert cls.apply_replication_stmt is not Engine.apply_replication_stmt


def test_the_shared_path_asks_the_engine():
    """Scoped to `_replicate`, because `hasattr(eng, "_psql")` elsewhere in
    the file is a different thing: the sequences branch is already behind
    `seqf.exists()`, a file only the PostgreSQL path writes."""
    import ast
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "cli.py").read_text()
    fn = next(n for n in ast.walk(ast.parse(src))
              if isinstance(n, ast.FunctionDef) and n.name == "_replicate")
    body = "\n".join(ast.dump(s) for s in fn.body)
    assert "apply_replication_stmt" in body
    assert "_psql" not in body, "the postgres-only method is back"
    assert "_q'" not in body and "'_q'" not in body


def test_the_publication_name_has_one_definition():
    """The message quotes the name a retry will collide on; if that were
    spelled out a second time it could drift from the statement."""
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "engines" / "postgres.py").read_text()
    assert src.count('"migkit_" + self.hop.name.replace("-", "_")') == 1


def _sql(name, sql, db="postgres"):
    p = subprocess.run(["docker", "exec", name, "psql", "-U", "postgres",
                        "-d", db, "-q", "-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    return p.stdout


def _sql1(name, sql, db="postgres"):
    return subprocess.run(["docker", "exec", name, "psql", "-U", "postgres",
                           "-d", db, "-tAc", sql],
                          capture_output=True, text=True).stdout.strip()


@pytest.fixture(scope="module")
def pair():
    """Two servers on networks that cannot route to each other, so the
    target genuinely has no path to the source - the case this is about."""
    for n in (NS, ND):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    for n in (NET_A, NET_B):
        subprocess.run(["docker", "network", "create", n],
                       capture_output=True)
    for n, p, net in ((NS, SRC, NET_A), (ND, DST, NET_B)):
        subprocess.run(["docker", "run", "-d", "--name", n, "--network", net,
                        "-e", "POSTGRES_PASSWORD=test", "-p", f"{p}:5432",
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
        for n in (NS, ND):
            _sql(n, "create table t (id int primary key, v text)")
        ip = subprocess.run(
            ["docker", "inspect", "-f",
             "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", NS],
            capture_output=True, text=True, check=True).stdout.strip()
        # the isolation is the premise, so it is asserted rather than assumed
        probe = subprocess.run(
            ["docker", "exec", ND, "bash", "-c",
             f"timeout 5 bash -c '</dev/tcp/{ip}/5432' && echo OPEN"
             " || echo BLOCKED"], capture_output=True, text=True, timeout=60)
        assert "BLOCKED" in probe.stdout, probe.stdout
        yield ip
    finally:
        for n in (NS, ND):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)
        for n in (NET_A, NET_B):
            subprocess.run(["docker", "network", "rm", n],
                           capture_output=True)


@needs_docker
def test_postgres_really_does_ignore_connect_timeout_here(pair):
    """The premise of the whole file, asserted against the server: the same
    conninfo is bounded through plain libpq and not inside the statement.

    The unbounded arm is checked by showing it is *still running* well past
    the timeout it was given, rather than by waiting out all 135 seconds.
    """
    ip = pair
    conn = (f"host={ip} port=5432 dbname=postgres user=postgres"
            " password=test connect_timeout=5")
    t0 = time.monotonic()
    p = subprocess.run(["docker", "exec", ND, "psql", conn, "-c", "select 1"],
                       capture_output=True, text=True, timeout=60)
    assert p.returncode != 0
    assert time.monotonic() - t0 < 20, "plain libpq honours connect_timeout"

    _sql(NS, "create publication p for all tables")
    stmt = (f"create subscription s connection '{conn}' publication p;")
    proc = subprocess.Popen(["docker", "exec", ND, "psql", "-U", "postgres",
                             "-c", stmt], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            proc.wait(timeout=25)
    finally:
        proc.kill()
        proc.wait(timeout=30)
        _sql(NS, "drop publication if exists p")


@needs_docker
def test_statement_timeout_is_what_bounds_it(pair):
    ip = pair
    _sql(NS, "create publication p for all tables")
    try:
        t0 = time.monotonic()
        p = subprocess.run(
            ["docker", "exec", "-e", "PGOPTIONS=-c statement_timeout=8s", ND,
             "psql", "-U", "postgres", "-c",
             f"create subscription s connection 'host={ip} port=5432"
             " dbname=postgres user=postgres password=test'"
             " publication p;"], capture_output=True, text=True, timeout=120)
        took = time.monotonic() - t0
        assert "statement timeout" in (p.stdout + p.stderr).lower(), p.stdout
        assert took < 30, took
        assert _sql1(ND, "select count(*) from pg_subscription") == "0", \
            "nothing is created when it fires, so bounding costs nothing"
    finally:
        _sql(NS, "drop publication if exists p")


@needs_docker
def test_migkit_bounds_it_and_says_which_side_cannot_reach_which(pair,
                                                                 tmp_path):
    """End to end on the statements migkit generates. The source address is
    one migkit can use and the target cannot - which is the ordinary shape
    of this: a private name that resolves only in the source's network."""
    eng = _engine(tmp_path, host="127.0.0.1", port=SRC)
    sql = eng.replicate_sql("postgres", copy_data=False)
    eng.apply_replication_stmt("src", "postgres", sql["src"][0])
    try:
        t0 = time.monotonic()
        with pytest.raises(SystemExit) as e:
            eng.apply_replication_stmt("dst", "postgres", sql["dst"][0])
        assert time.monotonic() - t0 < 60
        said = str(e.value)
        assert "route from target to source" in said, said
        assert "MIGKIT_CDC=follow" in said, said
        assert _sql1(ND, "select count(*) from pg_subscription") == "0"
        # and the thing the message warns about is real: the publication stayed
        assert _sql1(NS, "select count(*) from pg_publication") == "1"
    finally:
        _sql(NS, "drop publication if exists migkit_unreach")


@needs_docker
def test_a_retry_no_longer_collides_on_the_publication(pair, tmp_path):
    """It did: a retry stopped on `already exists`. PostgreSQL has no
    `CREATE PUBLICATION ... IF NOT EXISTS` - checked, it is a syntax
    error - so the statement makes it only where it is missing."""
    eng = _engine(tmp_path)
    sql = eng.replicate_sql("postgres", copy_data=False)
    eng.apply_replication_stmt("src", "postgres", sql["src"][0])
    try:
        eng.apply_replication_stmt("src", "postgres", sql["src"][0])
        got = subprocess.run(
            ["docker", "exec", NS, "psql", "-U", "postgres", "-At", "-c",
             "select count(*) from pg_publication"
             " where pubname = 'migkit_unreach'"],
            capture_output=True, text=True)
        assert got.stdout.strip() == "1", got
        bad = subprocess.run(
            ["docker", "exec", NS, "psql", "-U", "postgres", "-c",
             "create publication z if not exists for all tables;"],
            capture_output=True, text=True)
        assert bad.returncode != 0 and "syntax" in bad.stderr.lower(), bad
    finally:
        _sql(NS, "drop publication if exists migkit_unreach")


def test_a_second_cdc_run_finds_what_the_first_set_up(pg_pair, tmp_path):
    """Setting up the stream twice stopped on `already exists`: the
    publication is made only where it is missing, and a subscription that
    is already there is left to run."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    from tests.conftest import psql
    hop = Hop(name="twice", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    eng = PostgresEngine(hop)
    name = eng._repl_name()
    sql = eng.replicate_sql("postgres", copy_data=False)
    try:
        for _ in range(2):
            for stmt in sql["src"]:
                eng.apply_replication_stmt("src", "postgres", stmt)
        assert psql(pg_pair["src"], "select count(*) from pg_publication"
                    f" where pubname = '{name}'").stdout.strip() == "1"
        # a subscription already there: nothing is made again, nothing fails
        psql(pg_pair["dst"], f"create subscription {name} connection"
                             " 'host=127.0.0.1 port=1 dbname=x' publication"
                             f" {name} with (connect = false)")
        assert eng.apply_replication_stmt("dst", "postgres",
                                          sql["dst"][0]) == ""
    finally:
        psql(pg_pair["dst"], f"alter subscription {name} set (slot_name ="
                             f" none); drop subscription if exists {name}")
        psql(pg_pair["src"], f"drop publication if exists {name}")
