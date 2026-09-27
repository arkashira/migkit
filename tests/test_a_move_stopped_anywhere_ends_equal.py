"""A move stopped anywhere, run again, leaves the target equal to the
source.

Every place a move writes down how far it got is a failpoint
(`migkit.failpoint`); the move is run as its own process and ended there
as kill -9 would end it - on the first time the place is reached and on a
later one - then run again as it is, and the target compared with the
source row for row. Two paths: PostgreSQL to PostgreSQL through the table
copier (ranges of a keyed table, spans of a table with no key), and MySQL
to PostgreSQL through the copier every pair shares (ranges in processes of
their own, batches read back).
"""
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROOT = Path(__file__).resolve().parent.parent
MY, MY_PORT = "migkit-test-crash-my", 15880


def _migkit(tmp, *args, fail=None):
    env = dict(os.environ, MIGKIT_CONF=str(tmp / "hops.yaml"),
               MIGKIT_REPORTS=str(tmp / "reports"), MIGKIT_MOVER="builtin",
               COLUMNS="200")
    env.pop("MIGKIT_FAILPOINT", None)
    if fail:
        env["MIGKIT_FAILPOINT"] = fail
    return subprocess.run([sys.executable, "-c",
                           "from migkit.cli import main; main()", *args],
                          env=env, cwd=ROOT, capture_output=True, text=True,
                          timeout=900)


def _seed_pg(port):
    got = psql(port, """
        create table public.big (id bigint primary key, payload text);
        insert into public.big select g, 'row-' || g
          from generate_series(1, 120000) g;
        create table public.nokey (at bigint, payload text);
        insert into public.nokey select g % 5000, 'entry-' || g
          from generate_series(1, 60000) g;
        create table public.small (id int primary key, v text);
        insert into public.small select g, 'v' || g
          from generate_series(1, 100) g;
        analyze""")
    assert got.returncode == 0, got.stderr


PG_Q = {"big": "select count(*) || ':' || md5(string_agg(id || '|' ||"
               " payload, ',' order by id)) from public.big",
        "nokey": "select count(*) || ':' || md5(string_agg(at || '|' ||"
                 " payload, ',' order by at, payload)) from public.nokey",
        "small": "select count(*) || ':' || md5(string_agg(id || '|' || v,"
                 " ',' order by id)) from public.small"}


def _pg_same(pg_pair):
    return {t: (psql(pg_pair["src"], q).stdout.strip(),
                psql(pg_pair["dst"], q).stdout.strip())
            for t, q in PG_Q.items()}


def _reset_target(pg_pair, tmp):
    psql(pg_pair["dst"], "drop table if exists public.big, public.nokey,"
                         " public.small cascade")
    subprocess.run(["rm", "-rf", str(tmp / "reports")])


def _pg_hop(tmp, pg_pair):
    (tmp / "hops.yaml").write_text(
        "hops:\n  crash:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n")


PG_POINTS = [("range.committed", 1), ("range.committed", 3),
             ("range.saved", 2), ("checkpoint.written", 2),
             ("checkpoint.written", 6), ("span.committed", 1),
             ("span.committed", 2), ("table.done", 1), ("table.done", 2)]


@pytest.mark.parametrize("point,at", PG_POINTS,
                         ids=[f"{p}-{n}" for p, n in PG_POINTS])
def test_postgres_to_postgres_stopped_anywhere(pg_pair, tmp_path, point, at):
    if not psql(pg_pair["src"], "select to_regclass('public.big')"
                ).stdout.strip():
        _seed_pg(pg_pair["src"])
    _reset_target(pg_pair, tmp_path)
    _pg_hop(tmp_path, pg_pair)
    first = _migkit(tmp_path, "move", "crash", "--go", "--chunk", "20000",
                    fail=f"{point}:{at}:exit")
    assert first.returncode != 0, (
        f"the move was not stopped at {point}:{at}\n{first.stdout[-2000:]}")
    again = _migkit(tmp_path, "move", "crash", "--go", "--chunk", "20000")
    assert again.returncode == 0, again.stdout[-3000:] + again.stderr[-2000:]
    got = _pg_same(pg_pair)
    assert all(a == b for a, b in got.values()), got


def _my(sql, db="appdb"):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B", db], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql_source():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-e", "MYSQL_DATABASE=appdb",
                    "-p", f"{MY_PORT}:3306", "mysql:8.4"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", MY_PORT)) == 0:
                    break
            time.sleep(2)
        else:
            pytest.fail("MySQL never answered")
        _my("create table big (id bigint primary key, payload varchar(40));"
            " set session cte_max_recursion_depth = 1000000; insert into big"
            " with recursive g(n) as (select 1 union all select n + 1 from g"
            " where n < 120000) select n, concat('row-', n) from g;"
            " create table small (id int primary key, v varchar(10));"
            " insert into small select id, concat('v', id) from big where"
            " id <= 100; analyze table big, small")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


MY_POINTS = [("batch.written", 1), ("range.committed", 2),
             ("range.committed", 1), ("range.saved", 2),
             ("checkpoint.written", 3), ("table.done", 1)]


@pytest.mark.parametrize("point,at", MY_POINTS,
                         ids=[f"{p}-{n}" for p, n in MY_POINTS])
def test_mysql_to_postgres_stopped_anywhere(pg_pair, mysql_source, tmp_path,
                                            point, at):
    psql(pg_pair["dst"], "drop table if exists public.big, public.small")
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  crash:\n    engine: hetero\n"
        f"    source: {{host: 127.0.0.1, port: {MY_PORT}, user: root,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [appdb]\n    db_map: {appdb: postgres}\n"
        "    options: {source_engine: mysql, target_engine: postgres}\n")
    first = _migkit(tmp_path, "move", "crash", "--go", "--chunk", "20000",
                    fail=f"{point}:{at}:exit")
    assert first.returncode != 0, (
        f"the move was not stopped at {point}:{at}\n{first.stdout[-2000:]}")
    again = _migkit(tmp_path, "move", "crash", "--go", "--chunk", "20000")
    assert again.returncode == 0, again.stdout[-3000:] + again.stderr[-2000:]
    for t, q in (("big", "id, payload"), ("small", "id, v")):
        src = _my(f"set session group_concat_max_len = 100000000; select"
                  f" concat(count(*), ':', md5(group_concat(concat_ws('|',"
                  f" {q}) order by id separator ','))) from {t}")
        dst = psql(pg_pair["dst"],
                   f"select count(*) || ':' || md5(string_agg(concat_ws('|',"
                   f" {q}), ',' order by id)) from public.{t}").stdout.strip()
        assert src == dst, (t, src, dst)


def test_stopped_where_chance_puts_it_with_the_source_written_between(
        pg_pair, tmp_path):
    """The same claim as a property: a place, a hit and a way of stopping
    drawn at random, and the source written to between the stop and the
    run after - rows added, changed and removed on either side of where
    the copy had got to. The run after leaves the target equal to the
    source as it is then."""
    from hypothesis import HealthCheck, given, settings
    from hypothesis import strategies as st
    if not psql(pg_pair["src"], "select to_regclass('public.big')"
                ).stdout.strip():
        _seed_pg(pg_pair["src"])
    points = [p for p, _ in PG_POINTS]
    seen = []

    @settings(max_examples=8, deadline=None, derandomize=True,
              suppress_health_check=list(HealthCheck))
    @given(point=st.sampled_from(sorted(set(points))),
           at=st.integers(min_value=1, max_value=4),
           how=st.sampled_from(["exit", "raise"]),
           writes=st.lists(st.tuples(st.sampled_from(["add", "change",
                                                      "remove"]),
                                     st.integers(min_value=1,
                                                 max_value=130000)),
                           max_size=6))
    def one(point, at, how, writes):
        run = tmp_path / f"run{len(seen)}"
        run.mkdir()
        _reset_target(pg_pair, run)
        _pg_hop(run, pg_pair)
        first = _migkit(run, "move", "crash", "--go", "--chunk", "20000",
                        fail=f"{point}:{at}:{how}")
        for kind, n in writes:
            sql = {"add": f"insert into public.big values ({200000 + n},"
                          f" 'added-{n}') on conflict do nothing",
                   "change": f"update public.big set payload = 'changed'"
                             f" where id = {n}",
                   "remove": f"delete from public.big where id = {n}"}[kind]
            assert psql(pg_pair["src"], sql).returncode == 0
        again = _migkit(run, "move", "crash", "--go", "--chunk", "20000")
        assert again.returncode == 0, (point, at, how,
                                       again.stdout[-2000:])
        got = _pg_same(pg_pair)
        assert all(a == b for a, b in got.values()), (point, at, how,
                                                       writes, got)
        seen.append((point, at, how, first.returncode))
    one()
    # it was stopped in most of them: a hit past the place's last is a run
    # that finished, which is a case too
    assert sum(1 for *_, rc in seen if rc != 0) >= len(seen) // 2, seen
