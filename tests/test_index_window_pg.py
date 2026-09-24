"""Dropping the target's secondary indexes for a load, and putting them back.

The speed is measurable and was measured. What these defend is the window:
between the drop and the rebuild the table has no indexes, and if the process
dies there a unique index that was enforcing something is simply gone.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-ixw-src", "migkit-test-ixw-dst"
SRC_PORT, DST_PORT = 15425, 15426


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


def _wait(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _sql(name, db, sql):
    r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                        "psql", "-U", "postgres", "-d", db, "-At", "-c", sql],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n,
                        "-e", "POSTGRES_PASSWORD=test", "-p", f"{p}:5432",
                        "postgres:16"], check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    for n in (SRC, DST):
        for _ in range(45):
            r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", n,
                                "psql", "-U", "postgres", "-c", "select 1"],
                               capture_output=True)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail(f"{n} never accepted a connection")
        _sql(n, "postgres", "create database app")
        _sql(n, "app", "create table t (id int primary key, a text,"
                       " b text unique, c numeric)")
        _sql(n, "app", "create index ix_a on t (a)")
        _sql(n, "app", "create index ix_ac on t (a, c)")
    _sql(SRC, "app", "insert into t select g, md5(g::text),"
                     " md5((g*7)::text), g * 1.5"
                     " from generate_series(1,5000) g")
    assert _sql(SRC, "app", "select count(*) from t") == "5000"
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _hop(tmp_path):
    from migkit.config import Endpoint, Hop
    hop = Hop(name="w", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT,
                              user="postgres", password="test"),
              db_map={"app": "app"})
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _idx(name):
    return sorted(_sql(name, "app", "select indexname from pg_indexes"
                                    " where tablename='t'").splitlines())


def test_only_the_secondary_indexes_are_dropped_and_all_come_back(pair,
                                                                  tmp_path):
    from migkit import movers
    before = _idx(DST)
    assert "ix_a" in before and "t_pkey" in before, before
    inside = {}
    with movers._IndexWindow(_hop(tmp_path), "app", 2, None):
        inside["names"] = _idx(DST)
    # the constraint-backed ones never left
    assert "t_pkey" in inside["names"], inside["names"]
    assert "t_b_key" in inside["names"], inside["names"]
    # the plain ones did
    assert "ix_a" not in inside["names"], inside["names"]
    assert "ix_ac" not in inside["names"], inside["names"]
    # and everything is back
    assert _idx(DST) == before


def test_the_definitions_reach_disk_before_anything_is_dropped(pair,
                                                               tmp_path):
    """The file is what makes a rebuild possible from another process, an
    hour later, by a person."""
    from migkit import indexes as ix
    from migkit import movers
    with movers._IndexWindow(_hop(tmp_path), "app", 2, None):
        # one file per load, named for its process (`setaside`)
        written = list(tmp_path.glob("dropped-indexes.*.json"))
        assert len(written) == 1, "nothing was written before the drop"
        where = written[0]
        saved = ix.restore_from(where)
        # by schema too: the same name can be an index in each of two
        assert set(saved) == {"public.ix_a", "public.ix_ac"}, saved
        assert all("CREATE INDEX" in v for v in saved.values()), saved


def test_the_rebuild_happens_even_when_the_load_raises(pair, tmp_path):
    """The window is a context manager for this reason: a load that dies
    must not leave the target without its indexes."""
    from migkit import movers
    before = _idx(DST)
    with pytest.raises(RuntimeError, match="the load blew up"):
        with movers._IndexWindow(_hop(tmp_path), "app", 2, None):
            raise RuntimeError("the load blew up")
    assert _idx(DST) == before


def test_nothing_is_dropped_when_the_definitions_cannot_be_saved(pair,
                                                                 tmp_path):
    """Could not save, so do not drop - the load just runs the slower way."""
    from migkit import movers
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory")
    hop = _hop(tmp_path)
    hop.report_dir = lambda db=None: blocked / "nested"
    before = _idx(DST)
    said = []
    with movers._IndexWindow(hop, "app", 2, said.append):
        assert _idx(DST) == before, "indexes were dropped without a saved file"
    assert any("could not save" in s for s in said), said


def test_a_real_move_lands_the_rows_and_leaves_the_indexes_intact(pair,
                                                                  tmp_path):
    from migkit import movers
    before = _idx(DST)
    movers.pgdump_move(_hop(tmp_path), "app", 2, True, None)
    assert _sql(DST, "app", "select count(*) from t") == "5000"
    assert _idx(DST) == before
    # and the rebuilt indexes are usable, not just present
    plan = _sql(DST, "app", "explain (costs off) select * from t"
                            " where a = 'nope'")
    assert "ix_a" in plan or "Index" in plan, plan


def test_the_same_index_name_in_two_schemas_is_two_indexes(pair, tmp_path):
    """The window named each index without its schema. `ix_a` in `public`
    and `ix_a` in `other` were one name: the drop took whichever the search
    path found, the second definition overwrote the first, and one of the
    two indexes did not come back."""
    from migkit import movers
    _sql(DST, "app", "create schema if not exists other;"
                     " create table if not exists other.t (id int primary"
                     " key, a text); create index if not exists ix_a on"
                     " other.t (a)")
    try:
        def both():
            return _sql(DST, "app", "select schemaname||'.'||indexname from"
                                    " pg_indexes where indexname = 'ix_a'"
                                    " order by 1").splitlines()
        assert both() == ["other.ix_a", "public.ix_a"], both()
        inside = {}
        with movers._IndexWindow(_hop(tmp_path), "app", 2, None):
            inside["left"] = both()
        assert inside["left"] == [], inside
        assert both() == ["other.ix_a", "public.ix_a"], both()
    finally:
        _sql(DST, "app", "drop schema other cascade")


def test_what_a_killed_load_dropped_is_rebuilt_by_the_next(pair, tmp_path):
    """A load killed inside the window left the indexes dropped, and the
    next load saved its own definitions over the file that listed them:
    nothing said they had ever been there. Each load now keeps its own
    file, and the next one builds what a dead one's lists and the target
    lacks - on a table with no index left to drop, too."""
    import json
    import sys
    from migkit import movers
    before = _idx(DST)
    # a load that is gone: its process has exited
    dead = subprocess.run([sys.executable, "-c",
                           "import os; print(os.getpid())"],
                          capture_output=True, text=True).stdout.strip()
    import socket
    ddl = _sql(DST, "app", "select pg_get_indexdef('ix_a'::regclass)")
    (tmp_path / f"dropped-indexes.{socket.gethostname()}.{dead}.1.json"
     ).write_text(json.dumps({"public.ix_a": ddl}))
    _sql(DST, "app", "drop index ix_a")
    said = []
    with movers._IndexWindow(_hop(tmp_path), "app", 2, said.append):
        pass
    assert _idx(DST) == before, said
    assert "an earlier load dropped and did not rebuild" in " ".join(said)
    assert not list(tmp_path.glob("dropped-indexes.*")), said
