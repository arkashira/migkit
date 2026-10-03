"""Rows of 100 to 110 MB moved by migkit held to a memory limit (a
container of its own, `--memory`, no swap): the move sizes its workers to
what the widest row takes, finishes, is never killed for memory, and cuts
no value short - every value's digest is the same on both sides. Asked for
less memory than one worker needs, it says so before it starts and copies
nothing, rather than being killed halfway.

`CAP_CALIBRATE=1` measures what one worker holds instead (the move held to
one at a time, its container's peak) - the numbers `Engine.COPY_MEMORY`
is set from.
"""
import os
import shutil
from pathlib import Path

import pytest

from tests import cap_pairs
from tests.cap_pairs import MY_DST, MY_SRC, PG_DST, PG_SRC, my, pg

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("move.large-values", ("postgres", "mysql")),
    ("move.sized", ("postgres", "mysql")),
]

pytestmark = pytest.mark.docker

MB = 2 ** 20
#: the values' sizes, MB, per table - no wider: the docker machine these
#: run on has 4 GB for both servers and migkit together, and each server
#: holds a value several times over while it digests it (measured, a
#: PostgreSQL source digesting values of 160 MB beside a migkit held to
#: 2 GB was killed by the kernel and went into recovery)
SIZES = {"w1": [100, 110], "w2": [104]}
CALIBRATE = bool(os.environ.get("CAP_CALIBRATE"))


@pytest.fixture
def work():
    d = Path(os.path.expanduser(f"~/.cache/migkit-cap-{os.getpid()}"))
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def mysql_pair():
    yield from cap_pairs.my_pair("--max-allowed-packet=1073741824",
                                 "--innodb-redo-log-capacity=4294967296")


@pytest.fixture
def pg_pair():
    yield from cap_pairs.pg_pair()


def _my_data():
    for t, sizes in SIZES.items():
        my(MY_SRC, f"create database if not exists wide; create table"
                   f" wide.{t} (id int primary key, v longblob)")
        for i, size in enumerate(sizes, 1):
            # 1 KB of random bytes over and over: every byte value, quotes
            # and backslashes among them
            my(MY_SRC, f"insert into wide.{t} values ({i},"
                       f" repeat(random_bytes(1024), {size * 1024}))")
    my(MY_DST, "create database if not exists wide")


def _pg_data():
    for t, sizes in SIZES.items():
        pg(PG_SRC, f"create table {t} (id int primary key, v bytea)")
        for i, size in enumerate(sizes, 1):
            pg(PG_SRC, f"insert into {t} select {i}, string_agg(b, '') from"
                       f" (select decode(md5(g::text) || md5((g + 1)::text),"
                       f" 'hex') as b from generate_series(1,"
                       f" {size * MB // 32}) g) x")


def _engine(engine):
    """The hop's engine as this machine reaches the pair."""
    from migkit.config import Endpoint, Hop
    from migkit.engines import get_engine
    ports = cap_pairs.MY_PORTS if engine == "mysql" else cap_pairs.PG_PORTS
    user = "root" if engine == "mysql" else "postgres"
    hop = Hop(name="wide", engine=engine,
              source=Endpoint(host="127.0.0.1", port=ports[0], user=user,
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=ports[1], user=user,
                              password="test"),
              databases=["wide" if engine == "mysql" else "postgres"])
    return get_engine(hop)


def _hops(work, engine, src, dst, db, extra=""):
    (work / "hops.yaml").write_text(
        f"hops:\n  wide:\n    engine: {engine}\n"
        f"    source: {{host: {cap_pairs.ip(src)}, port:"
        f" {3306 if engine == 'mysql' else 5432}, user:"
        f" {'root' if engine == 'mysql' else 'postgres'}, password: test}}\n"
        f"    target: {{host: {cap_pairs.ip(dst)}, port:"
        f" {3306 if engine == 'mysql' else 5432}, user:"
        f" {'root' if engine == 'mysql' else 'postgres'}, password: test}}\n"
        f"    databases: [{db}]\n" + extra)


def _digests(run, side, db):
    got = {}
    for t in SIZES:
        q = (f"select id, length(v), md5(v) from {db}.{t} order by id"
             if run is my else
             f"select id, length(v), md5(v) from {t} order by id")
        got[t] = run(side, q)
    return got


def _calibrate(engine, work, src, dst, db):
    """What one worker holds: the move held to one at a time, in a
    container with room to spare, and its peak."""
    _hops(work, engine, src, dst, db, "    workers: 1\n")
    code, out, killed, peak = cap_pairs.run_capped(
        work, "1500m" if engine == "mysql" else "1g", "move", "wide", "--go")
    assert code == 0 and not killed, out[-3000:]
    widest = max(max(v) for v in SIZES.values()) * MB
    print(f"\n{engine}: one worker peaked at {peak / MB:,.0f} MB for a"
          f" widest value of {widest / MB:,.0f} MB")
    print("\n".join(l for l in out.splitlines() if "memory" in l
                    or "at a time" in l))


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_wide_rows_move_within_the_memory_there_is(engine, work, request):
    if engine == "mysql":
        request.getfixturevalue("mysql_pair")
        src, dst, db, run = MY_SRC, MY_DST, "wide", my
        _my_data()
    else:
        request.getfixturevalue("pg_pair")
        src, dst, db, run = PG_SRC, PG_DST, "postgres", pg
        _pg_data()
    if CALIBRATE:
        return _calibrate(engine, work, src, dst, db)
    # what the model says one worker holds, asked from here of the same
    # servers: the widest row and the way it goes
    from migkit import model
    eng = _engine(engine)
    one, widest = model.worker_memory(eng, eng.row_sizes("src", db),
                                      db=db)
    assert widest[1] >= 110 * MB, widest
    # room for one worker and a third of another: two at once would not
    # fit, and without the model's number they would be started together
    from migkit.sizing import RESERVE_MEMORY
    cap = int(one + RESERVE_MEMORY + one // 3)
    _hops(work, engine, src, dst, db)
    code, out, killed, peak = cap_pairs.run_capped(
        work, f"{cap // MB}m", "move", "wide", "--go")
    print(f"\n{engine}: held to {cap / MB:,.0f} MB, peaked at"
          f" {(peak or 0) / MB:,.0f} MB")
    assert not killed, out[-3000:]
    assert code == 0, out[-3000:]
    assert "memory: the widest row is ~" in out, out[-3000:]
    assert _digests(run, src, db) == _digests(run, dst, db)
    # asked for less than one worker needs: said before anything is
    # copied, never killed halfway
    for t in SIZES:
        run(dst, f"drop table if exists {db}.{t}" if run is my
            else f"drop table if exists {t}")
    code, out, killed, peak = cap_pairs.run_capped(
        work, f"{(one // 2) // MB}m", "move", "wide", "--go")
    assert not killed, out[-3000:]
    assert code != 0 and "not started" in out and \
        "nothing is cut short" in out, out[-3000:]
