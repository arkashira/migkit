"""The change leg run from here leaves out what the hop excludes, refuses
what it cannot carry, and says how far behind it is (backlog 0f).

It carried every table the source has: a table the hop excludes because
the target owns it had its source's changes applied over it. It carried a
table the hop filters whole, where the check - reading through the filter
- could not see the rows outside it. And `/metrics` said nothing of it.
"""
import subprocess
import threading
import time

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql


def _hop(tmp_path, port_src=1, port_dst=2, host="10.0.0.1", **kw):
    hop = Hop(name="leg", engine="postgres",
              source=Endpoint(host=host, port=port_src, user="postgres",
                              password="test"),
              target=Endpoint(host=host, port=port_dst, user="postgres",
                              password="test"),
              databases=["postgres"], **kw)
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def test_a_filtered_table_starts_nothing(tmp_path, monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(PostgresEngine, "_all_tables",
                        lambda self, side, db: ["public.t", "public.o"])
    hop = _hop(tmp_path, mapping={"where": {"public.o": "id > 5"}})
    with pytest.raises(SystemExit) as e:
        movers.pgcopydb_follow(hop, "postgres", go=False)
    assert "public.o (its rows are filtered)" in str(e.value)


def test_an_excluded_table_is_filtered_out_of_the_leg(tmp_path, monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(PostgresEngine, "_all_tables",
                        lambda self, side, db: ["public.t", "public.audit"])
    steps = movers.pgcopydb_follow(_hop(tmp_path, exclude=["audit"]),
                                   "postgres", go=False)
    argv = steps[0].argv
    ini = argv[argv.index("--filters") + 1]
    assert open(ini).read() == "[exclude-table]\npublic.audit\n"
    assert any("1 tables the hop excludes are left out" in s
               for s in steps), steps


def test_a_hop_without_exclusions_runs_the_leg_as_before(tmp_path):
    steps = movers.pgcopydb_follow(_hop(tmp_path), "postgres", go=False)
    assert "--filters" not in steps[0].argv


def test_no_leg_running_says_nothing(tmp_path):
    assert movers.follow_lag(_hop(tmp_path), "postgres") is None


def test_the_metrics_carry_the_leg(tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit import ui
    monkeypatch.setattr(ui, "REPORTS", tmp_path)
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    (tmp_path / "leg" / "postgres" / "follow").mkdir(parents=True)
    (tmp_path / "leg" / "other").mkdir(parents=True)
    asked = []

    def lag(hop, db):
        asked.append(db)
        return {"apply": True, "behind": 4096, "held": None}
    monkeypatch.setattr(movers, "follow_lag", lag)
    got = ui._follow_metrics(_hop(tmp_path), tmp_path / "leg", 'hop="leg"')
    assert asked == ["postgres"]
    assert got == ['migkit_follow_applying{hop="leg",db="postgres"} 1',
                   'migkit_follow_behind_bytes{hop="leg",db="postgres"}'
                   ' 4096'], got


needs_pgcopydb = pytest.mark.skipif(
    subprocess.run(["which", "pgcopydb"], capture_output=True).returncode,
    reason="pgcopydb binary not installed")


@needs_docker
@needs_pgcopydb
def test_the_leg_leaves_the_targets_own_table_alone_and_says_its_lag(
        pg_pair, tmp_path):
    """Measured end to end: a table the hop excludes, written on both
    sides, keeps the target's rows; the leg's own reading comes back while
    it runs, with no password anywhere."""
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table t (id int primary key, v text);"
                   " create table audit (id int primary key, who text)")
    psql(pg_pair["dst"], "insert into audit values (1, 'the target''s')")
    hop = _hop(tmp_path, pg_pair["src"], pg_pair["dst"], host="127.0.0.1",
               exclude=["audit"])
    stop, readings = [], []

    def writer():
        i = 0
        while not stop:
            i += 1
            psql(pg_pair["src"], f"insert into t values ({i}, 'w{i}');"
                                 f" insert into audit values ({i + 100},"
                                 " 'the source''s')")
            time.sleep(0.3)

    def watch():
        while not stop:
            got = movers.follow_lag(hop, "postgres")
            if got:
                readings.append(got)
            time.sleep(1)
    threads = [threading.Thread(target=f, daemon=True)
               for f in (writer, watch)]
    for th in threads:
        th.start()
    try:
        time.sleep(2)
        movers.pgcopydb_follow(hop, "postgres", go=True, timeout=300)
    finally:
        stop.append(True)
        for th in threads:
            th.join(timeout=20)
        movers.follow_teardown(hop, "postgres", go=True)
    carried = psql(pg_pair["dst"], "select count(*) from t").stdout.strip()
    assert int(carried) > 0, "the leg applied nothing"
    assert psql(pg_pair["dst"], "select string_agg(who, ',') from audit"
                ).stdout.strip() == "the target's"
    assert readings, "the leg's reading never came back while it ran"
    assert any(r["apply"] for r in readings), readings
    assert all(r["behind"] is not None and r["behind"] >= 0
               for r in readings), readings
