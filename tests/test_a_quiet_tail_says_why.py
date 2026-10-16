"""A tail that carries nothing says why, per database (field run
2026-10-16: "no delta any more" on two databases, and the operator could
not tell a detached reader from a source busy elsewhere): attached at
where, the source at where, what it applied and since when, and what the
quiet is (`decide.tail_state`, `tailctl.quiet_line`)."""
import itertools

from migkit import decide, tailctl


def _run(where_now, rounds=40):
    said, n = [], itertools.count()

    def look(token):
        return token, 0, True
    tailctl.run_loop(None, "payment", look, said.append, "changes",
                     go=False, token={"log_file": "b.000001",
                                      "log_pos": 100},
                     until_="caught_up",
                     reached=lambda t: next(n) >= rounds,
                     idle=(0.001, 0.005), where_now=where_now)
    return said


def test_the_source_writing_elsewhere_is_said_with_the_counts():
    said = _run(lambda token: {"at": "b.000001:100",
                               "server": "b.000001:4200", "moved": 4100,
                               "attached": True})
    quiet = [s for s in said if "quiet" in s]
    assert len(quiet) == 1, said
    assert quiet[0].startswith("payment: attached at b.000001:100, the"
                               " source at b.000001:4200; 0 changes")
    assert "the source wrote 4,100 bytes of its log elsewhere, none on" \
           " your tables" in quiet[0]


def test_a_detached_reader_is_said_with_its_reason_not_silence():
    said = _run(lambda token: {"at": "b.000001:100", "attached": False,
                               "why_detached": "the source lists no reader"
                               " under server_id 2147483700"})
    assert any(s.startswith("payment: the reader is not attached (the"
                            " source lists no reader under server_id"
                            " 2147483700)") for s in said), said


def test_a_reader_that_cannot_be_asked_is_said_detached():
    def boom(token):
        raise ConnectionError("Lost connection to server during query")
    said = _run(boom)
    assert any("not attached (Lost connection" in s for s in said), said


def test_a_still_source_is_quiet_and_a_change_within_the_interval_is_not():
    assert decide.tail_state({"attached": True, "moved": 0, "quiet_for": 9,
                              "interval": 3}).path == "idle"
    assert decide.tail_state({"attached": True, "moved": 900,
                              "quiet_for": 1, "interval": 3}
                             ).path == "following"


def test_the_providers_writes_on_the_target_are_said():
    line = tailctl.quiet_line("shipping", "idle", {
        "at": "b.000002:4", "server": "b.000002:4",
        "sync_back": ["__dst__tencentdb__sync_6fve1pd0"]}, 12, 0.0)
    assert "quiet: the source wrote nothing" in line
    assert "also writes the target: __dst__tencentdb__sync_6fve1pd0" in line


def test_the_events_elsewhere_are_counted_in_the_line():
    line = tailctl.quiet_line("payment", "elsewhere", {
        "at": "b.000002:4", "server": "b.000003:900", "moved": 5200,
        "elsewhere": 3}, 0, 0.0)
    assert "quiet: the source wrote 3 events (5,200 bytes of its log)" \
           " elsewhere, none on your tables" in line


def test_the_log_moved_is_measured_across_its_files():
    from migkit.engines.mysql import MySQLEngine
    logs = [("b.000001", 1000), ("b.000002", 700), ("b.000003", 300)]
    assert MySQLEngine.log_distance(logs, ("b.000001", 400),
                                    ("b.000003", 120)) == 600 + 700 + 120
    assert MySQLEngine.log_distance(logs, ("b.000002", 100),
                                    ("b.000002", 650)) == 550
    assert MySQLEngine.log_distance(logs, ("b.000000", 4),
                                    ("b.000003", 120)) is None


def _engine(tmp_path, pair, db):
    from migkit.config import Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="quiet", engine="mysql", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[db])
    hop.report_dir = lambda x="": tmp_path
    return MySQLEngine(hop)


def test_a_provider_sync_back_table_on_the_target_is_named(tmp_path):
    from types import SimpleNamespace

    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine.__new__(MySQLEngine)
    eng.hop = SimpleNamespace(engine="mysql", options={}, name="quiet",
                              report_dir=lambda x="": tmp_path,
                              source=SimpleNamespace(host="h", port=1))
    eng._binlog_position = lambda side: ("b.000001", 120)
    eng._q = lambda side, q, *a: [("b.000001", 120)] if "logs" in q else []
    eng.neutral_tables = lambda side, db: [
        "orders", "__dst__tencentdb__sync_6fve1pd0"]
    got = eng.tail_where("shipping", {"log_file": "b.000001",
                                      "log_pos": 120})
    assert got["sync_back"] == ["__dst__tencentdb__sync_6fve1pd0"
                                " (Tencent DTS)"]
    assert got["moved"] == 0 and got["elsewhere"] == 0
    line = tailctl.quiet_line("shipping", decide.tail_state(
        {**got, "quiet_for": 9}).path, got, 0, 0.0)
    assert "quiet: the source wrote nothing" in line
    assert "(Tencent DTS)" in line


from tests import servers  # noqa: E402
from tests.conftest import needs_docker  # noqa: E402

pair = servers.fixture("mysql-rowmeta")


@needs_docker
def test_writes_on_another_database_are_said_elsewhere_and_counted(
        pair, tmp_path):
    db, other = pair.name, pair.name + "_else"
    pair.src.sql(f"create database if not exists {other}")
    for d in (db, other):
        pair.src.sql("drop table if exists t; create table t (id int"
                     " primary key)", d)
    eng = _engine(tmp_path, pair, db)
    token = eng.change_point("src", db)

    def state():
        nonlocal token
        out, token = eng.neutral_changes("src", db, token, limit=100)
        got = eng.tail_where(db, token)
        return out, got, decide.tail_state({**got, "quiet_for": 9}).path
    try:
        out, got, path = state()
        assert (out, path, got["elsewhere"]) == ([], "idle", 0), got
        for i in range(3):
            pair.src.sql(f"insert into t values ({i})", other)
        out, got, path = state()
        assert out == [] and path == "elsewhere", got
        assert got["elsewhere"] == 3 and got["moved"] > 0, got
        line = tailctl.quiet_line(db, path, got, 0, 0.0)
        assert "wrote 3 events" in line, line
        pair.src.sql("flush binary logs")
        pair.src.sql("insert into t values (9)", other)
        out, got, path = state()
        assert path == "elsewhere" and got["elsewhere"] == 4, got
        assert got["moved"] > 0, got
        pair.src.sql("insert into t values (1)", db)
        out, got, path = state()
        assert len(out) == 1 and path == "idle", got
        # a new engine on the same run store stands on the mark it kept
        again = _engine(tmp_path, pair, db).tail_where(db, token)
        assert again["moved"] == 0 and again["elsewhere"] == 0, again
    finally:
        eng.release_changes()
        pair.src.sql(f"drop database if exists {other}")
