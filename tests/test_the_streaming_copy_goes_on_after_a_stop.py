"""The streaming PostgreSQL copy goes on from the tables it finished when
it was stopped part way, instead of emptying the target and starting over.

Measured: stopped while the large table was loading, a small table loaded
beside it was on the target and the large one held nothing (its COPY
rolled back). Run again, it copied what was not finished, left the small
table alone - its key would have refused a second copy - and ended with
every row once. The part copied
after the stop comes from a new snapshot, which is why the move compares
the whole result with the source before calling it complete.
"""
import pathlib
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = "migkit-test-streamresume-src", "migkit-test-streamresume-dst"
SRC_PORT, DST_PORT = 15866, 15867


def _local():
    from migkit import movers
    return movers.pgcopydb_runner() == "local"


def pg(name, sql, db="postgres"):
    got = subprocess.run(["docker", "exec", "-i", name, "psql", "-U",
                          "postgres", "-d", db, "-q", "-tA"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    if not _local():
        pytest.skip("the streaming copier is not installed on this machine")
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"{port}:5432", "-e", "POSTGRES_PASSWORD=test",
                        "postgres:16"], check=True, capture_output=True)
    try:
        for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
            end = time.time() + 90
            while time.time() < end:
                ok = subprocess.run(["docker", "exec", name, "pg_isready",
                                     "-U", "postgres"],
                                    capture_output=True).returncode == 0
                with socket.socket() as s:
                    s.settimeout(1)
                    if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                        break
                time.sleep(1)
        time.sleep(2)
        schema = ("create table big (id bigint primary key, a int, s text);"
                  " create table small1 (id int primary key, v text);"
                  " create table small2 (id int primary key, v text);")
        for name in (SRC, DST):
            pg(name, "create database app")
            pg(name, schema, "app")
        # the large table's load slowed, so there is a moment to stop it
        # in: a trigger the load cannot turn off pauses every 1,000th row
        pg(DST, "create function slow() returns trigger language plpgsql"
                " as $$ begin if new.id % 1000 = 0 then perform"
                " pg_sleep(0.005); end if; return new; end $$; create"
                " trigger slow before insert on big for each row execute"
                " function slow(); alter table big enable always trigger"
                " slow", "app")
        pg(SRC, "insert into big select n, n % 1000, 'name-' || n from"
                " generate_series(1, 1500000) n; insert into small1 select"
                " n, 'v' || n from generate_series(1, 20000) n; insert into"
                " small2 select n, 'w' || n from generate_series(1, 20000)"
                " n; analyze", "app")
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _hop(tmp_path):
    hop = Hop(name="sr", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT,
                              user="postgres", password="test"),
              databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _counts():
    return pg(DST, "select (select count(*) from big), (select count(*)"
                   " from small1), (select count(*) from small2),"
                   " (select count(distinct id) from big)", "app")


def test_a_stopped_copy_goes_on_from_the_tables_it_finished(pair,
                                                            tmp_path):
    from migkit import movers
    stopped = {}

    def stop_while_the_large_table_loads():
        end = time.time() + 120
        while time.time() < end:
            # the target's own count of what a COPY into it has taken
            got = pg(DST, "select coalesce(max(tuples_processed), 0) from"
                          " pg_stat_progress_copy")
            if 0 < int(got or 0) < 1_000_000:
                # its workers too: they name themselves otherwise
                subprocess.run(["pkill", "-9", "-f", "pgcopydb"])
                stopped["yes"] = True
                return
            time.sleep(0.02)
    threading.Thread(target=stop_while_the_large_table_loads,
                     daemon=True).start()
    with pytest.raises(Exception):
        movers.pgcopydb_move(_hop(tmp_path), "app", 2, True, None)
    assert stopped, "the copy finished before it could be stopped"
    work = pathlib.Path(tmp_path) / "streaming-work.begun"
    assert work.exists()
    big, s1, s2, _ = _counts().split("|")
    # the large table's load rolled back with the stop; a small one that
    # was loaded beside it stayed
    assert int(big) < 1_500_000 and "20000" in (s1, s2), (big, s1, s2)

    said = []
    movers.pgcopydb_move(_hop(tmp_path), "app", 2, True, said.append)
    assert any("going on from the tables it finished" in str(m)
               for m in said), said
    # every row once: the small tables were not copied a second time
    assert _counts() == "1500000|20000|20000|1500000"
    assert not work.exists()
    # a copy that finished leaves nothing to go on from: the next one
    # starts over, emptying what is there first
    said = []
    movers.pgcopydb_move(_hop(tmp_path), "app", 2, True, said.append)
    assert not any("going on" in str(m) for m in said), said
    assert _counts() == "1500000|20000|20000|1500000"
