"""ClickHouse to ClickHouse: the target pulls each of the source's
partitions itself (`remote()`), no row through migkit, and follows the
source by the partitions that change.

Measured before: a ClickHouse move read every row through migkit and
wrote it back with a delete of its key first, and a ClickHouse hop could
not be followed at all. Measured on 24.8 while building this: the virtual
`_partition_id` is seen through `remote()` and filters on the source (696
of 3,000 rows for one month); an insert that spans more than 100 of the
target's partitions is refused (`Too many partitions for single INSERT
block`); a token seen before drops the insert whole, so a partition
dropped and copied again under the same token leaves the target without
it; the source's password shows as `[HIDDEN]` in the target's query log
and in its errors.
"""
import json
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

NET = "migkit-test-eng-net"
SRC, DST = ("migkit-test-eng-ch-src", 16010), ("migkit-test-eng-ch-dst", 16011)
FROM_TARGET = f"{SRC[0]}:9000"
HELD = "each partition's fingerprint equal on both servers"


@pytest.fixture(scope="module")
def servers():
    import clickhouse_connect
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    for name, port in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "--network",
                        NET, "-e", "CLICKHOUSE_PASSWORD=test", "-e",
                        "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1", "-p",
                        f"{port}:8123", "--ulimit", "nofile=262144:262144",
                        "clickhouse/clickhouse-server:24.8"], check=True,
                       capture_output=True)
    try:
        clients = []
        for _, port in (SRC, DST):
            end = time.time() + 90
            while True:
                try:
                    c = clickhouse_connect.get_client(
                        host="127.0.0.1", port=port, username="default",
                        password="test")
                    c.command("select 1")
                    break
                except Exception:
                    if time.time() > end:
                        raise
                    time.sleep(1)
            clients.append(c)
        yield clients
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _engine(tmp_path, db, address=FROM_TARGET):
    from migkit.engines.clickhouse import ClickHouseEngine
    src = Endpoint(host="127.0.0.1", port=SRC[1], user="default",
                   password="test",
                   options={"address_from_target": address})
    dst = Endpoint(host="127.0.0.1", port=DST[1], user="default",
                   password="test")
    hop = Hop(name="chpull", engine="clickhouse", source=src, target=dst,
              databases=[db])
    hop.report_dir = lambda d=None: tmp_path
    return ClickHouseEngine(hop)


def _months(client, db, table="t", rows=3000):
    client.command(f"create database if not exists {db}")
    client.command(f"create table {db}.{table} (id UInt64, at DateTime,"
                   " kind String, note Nullable(String)) engine = MergeTree"
                   " partition by toYYYYMM(at) order by id")
    client.command(f"insert into {db}.{table} select number,"
                   " toDateTime('2024-01-01') + number * 3600,"
                   " toString(number % 5), if(number % 7 = 0, NULL, 'n' ||"
                   f" toString(number)) from numbers({rows})")


def test_a_whole_move_is_pulled_by_the_target(servers, tmp_path,
                                              monkeypatch):
    import migkit.config as cfg
    from click.testing import CliRunner

    from migkit import cli
    from migkit.engines.clickhouse import ClickHouseEngine
    src, dst = servers
    _months(src, "app")
    # two versions of one row, unmerged: a target that merged the block it
    # takes would keep one, and the partition would not be the source's
    src.command("create table app.r (id UInt64, v UInt32, at Date) engine ="
                " ReplacingMergeTree(v) partition by toYYYYMM(at) order by"
                " id")
    src.command("system stop merges app.r")
    src.command("insert into app.r values (1, 1, '2024-01-05')")
    src.command("insert into app.r values (1, 2, '2024-01-06')")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  chpull:\n    engine: clickhouse\n"
        f"    source: {{host: 127.0.0.1, port: {SRC[1]}, user: default,"
        f" password: test, address_from_target: '{FROM_TARGET}'}}\n"
        f"    target: {{host: 127.0.0.1, port: {DST[1]}, user: default,"
        " password: test}\n    databases: [app]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.delenv("MIGKIT_MOVER", raising=False)

    def no_rows(*a, **k):
        raise AssertionError("a row went through migkit")
    monkeypatch.setattr(ClickHouseEngine, "neutral_write", no_rows)
    monkeypatch.setattr(ClickHouseEngine, "_carry_rows", no_rows)
    got = CliRunner().invoke(cli.main, ["move", "chpull", "--mode", "full",
                                        "--go"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code == 0, said
    assert "t: 5 partitions pulled by the target from the source, 3,000" \
        f" rows, {HELD}" in said, said
    assert f"r: 1 partitions pulled by the target from the source, 2 rows," \
        f" {HELD}" in said, said
    assert "'test'" not in said and "remote(" not in said, said
    made = dst.command("show create table app.r")
    assert "ReplacingMergeTree(v)" in made and "toYYYYMM(at)" in made, made
    assert dst.command("select count() from app.r") == 2
    assert dst.command("select count() from app.t") == 3000
    eng = _engine(tmp_path, "app")
    s, d = eng._fingerprints("app", "t")
    assert s == d and len(s) == 5
    got = eng.check_data("app")
    assert [r.status for r in got] == ["ok", "ok"], [r.detail for r in got]
    # the source's password never reaches the target's log in the clear
    dst.command("system flush logs")
    logged = [q for (q,) in dst.query(
        "select query from system.query_log where query ilike"
        " '%remote(%' and type = 'QueryFinish'").result_rows]
    assert logged and all("'test'" not in q and "[HIDDEN]" in q
                          for q in logged), logged[:2]


def test_a_move_run_again_keeps_what_matches_and_mends_what_does_not(
        servers, tmp_path):
    src, dst = servers
    _months(src, "again")
    eng = _engine(tmp_path, "again")
    said = []
    eng.native_bulk("again", ["t"], True, said.append)
    assert said == ["t: 5 partitions pulled by the target from the source,"
                    f" 3,000 rows, {HELD}"], said
    dst.command("alter table again.t update kind = 'x' where id = 1000"
                " settings mutations_sync = 2")
    dst.command("alter table again.t drop partition id '202403'")
    dst.command("insert into again.t values (7, '2025-06-01 00:00:00', 's',"
                " NULL)")
    said = []
    _engine(tmp_path, "again").native_bulk("again", ["t"], True, said.append)
    assert said == ["t: 2 partitions pulled by the target from the source,"
                    f" 1,440 rows, {HELD}; 3 already equal"], said
    s, d = eng._fingerprints("again", "t")
    assert s == d and "202506" not in d, (s, d)


def test_a_partition_copied_again_is_not_dropped_as_a_duplicate(servers,
                                                                 tmp_path):
    """A target that deduplicates drops a block whose token - or, with no
    token, whose content - it has seen. A month of the source spans 29 of
    this target's days; its rows deleted there and copied again under the
    token of the copy before, only some days come back (measured: 624 of
    696), and with no token none do (0 of 696) - shown first, by hand.
    migkit numbers each copy, so the month copied again lands whole."""
    src, dst = servers
    _months(src, "dedup")
    dst.command("create database if not exists dedup")
    for t in ("byhand", "t"):
        dst.command(f"create table dedup.{t} (id UInt64, at DateTime, kind"
                    " String, note Nullable(String)) engine = MergeTree"
                    " partition by toYYYYMMDD(at) order by id settings"
                    " non_replicated_deduplication_window = 100")
    pull = (f"insert into dedup.byhand select * from remote('{FROM_TARGET}',"
            " 'dedup', 't', 'default', 'test') where _partition_id ="
            " '202402'")
    clear = ("alter table dedup.byhand delete where toYYYYMM(at) = 202402"
             " settings mutations_sync = 2")
    dst.command(pull, settings={"insert_deduplication_token": "same"})
    dst.command(clear)
    dst.command(pull, settings={"insert_deduplication_token": "same"})
    assert 0 < dst.command("select count() from dedup.byhand") < 696
    dst.command("truncate table dedup.byhand")
    dst.command(pull)
    dst.command(clear)
    dst.command(pull)
    assert dst.command("select count() from dedup.byhand") == 0
    eng = _engine(tmp_path, "dedup")
    eng.native_bulk("dedup", ["t"], True, [].append)
    dst.command("alter table dedup.t delete where toYYYYMM(at) = 202402"
                " settings mutations_sync = 2")
    said = []
    eng.native_bulk("dedup", ["t"], True, said.append)
    assert said == ["t: 1 partitions pulled by the target from the source,"
                    f" 696 rows, {HELD}; 4 already equal"], said
    assert dst.command("select count() from dedup.t where toYYYYMM(at) ="
                       " 202402") == 696
    kept = json.loads((tmp_path / "pulled-partitions.json").read_text())
    assert kept["t"]["202402"] == 2, kept


def test_a_target_partitioned_by_the_hour_is_filled_in_pieces(servers,
                                                             tmp_path):
    """Each month of the source holds 744 of the target's hours, and one
    insert may write 100 of them."""
    src, dst = servers
    _months(src, "hours")
    dst.command("create database if not exists hours")
    dst.command("create table hours.t (id UInt64, at DateTime, kind String,"
                " note Nullable(String)) engine = MergeTree partition by"
                " toStartOfHour(at) order by id")
    eng = _engine(tmp_path, "hours")
    assert len(eng._pull_wheres("hours", "t", "202401")) == 8
    said = []
    eng.native_bulk("hours", ["t"], True, said.append)
    assert said == ["t: 5 partitions pulled by the target from the source,"
                    f" 3,000 rows, {HELD}"], said
    s, d = eng._fingerprints("hours", "t")
    assert s == d and len(s) == 5


def test_a_target_that_cannot_reach_the_source_is_copied_by_rows(servers,
                                                                 tmp_path):
    src, dst = servers
    _months(src, "far", rows=300)
    itself = _engine(tmp_path, "far", address=f"{DST[0]}:9000")
    steps = itself.native_bulk("far", ["t"], False, [].append)
    assert steps == ["t: copied table by table - the target pulling from"
                     f" the source did not prove itself: {DST[0]}:9000,"
                     " seen from the target, is the target itself"], steps
    nowhere = _engine(tmp_path, "far", address="nohost.invalid:9000")
    steps = nowhere.native_bulk("far", ["t"], True, [].append)
    assert steps[0].startswith("t: copied table by table - the target"
                               " pulling from the source did not prove"
                               " itself: the target cannot reach the source"
                               " at nohost.invalid:9000"), steps
    assert "'test'" not in steps[0], steps
    assert dst.command("select count() from far.t") == 300
    assert [r.status for r in nowhere.check_data("far")] == ["ok"]


def _tail(eng, db, tmp_path):
    ended = {}

    def run():
        try:
            eng.tail_apply(db, True, tmp_path / "tail-token.json",
                           lambda m: None)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, ended


def _stop(thread):
    import ctypes
    if thread.is_alive():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
    thread.join(timeout=30)


def test_the_target_follows_the_partitions_that_change(servers, tmp_path):
    src, dst = servers
    _months(src, "follow")
    eng = _engine(tmp_path, "follow")
    token = tmp_path / "tail-token.json"
    eng.tail_start("follow", token)
    # written after the position and before the copy: the follow carries it
    src.command("insert into follow.t values (5000, '2024-03-02 00:00:00',"
                " 'during', NULL)")
    eng.native_bulk("follow", ["t"], True, [].append)
    thread, ended = _tail(eng, "follow", tmp_path)
    try:
        src.command("insert into follow.t values (6000, '2024-09-01"
                    " 00:00:00', 'new month', 'n')")
        src.command("alter table follow.t update kind = 'edited' where id ="
                    " 10 settings mutations_sync = 2")
        src.command("alter table follow.t drop partition id '202405'")
        at = eng.src_lsn("follow")
        assert at is not None
        assert eng.fence_wait("follow", at, timeout=120) is True
        # a write the follow has not looked at yet is a difference still
        # arriving: the check waits on the follow and compares again
        src.command("insert into follow.t values (6001, '2024-01-03"
                    " 00:00:00', 'late', NULL)")
        got = eng.check_data("follow", table="t")
        assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    finally:
        _stop(thread)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended
    s, d = eng._fingerprints("follow", "t")
    assert s == d, (s, d)
    assert "202405" not in d and "202409" in d
    assert dst.command("select kind from follow.t where id = 10") == "edited"
    assert dst.command("select count() from follow.t where id = 5000") == 1
