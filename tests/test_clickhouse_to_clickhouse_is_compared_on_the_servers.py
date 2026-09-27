"""ClickHouse to ClickHouse: compared by what each server computes, and
the cells every other engine has - settings, accounts, a snapshot, a
verify of only what changed.

Measured before: a ClickHouse hop read every row of both sides through
migkit to compare them, compared no settings, carried no accounts, took no
snapshot before a repair and had no verify of only what changed. Now each
server sums a hash of its rows per partition - the target's grouped by the
source's partition key, however it is partitioned itself - and only a
table whose sums differ is read row by row.
"""
import json
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = ("migkit-test-chch-src", 15923), ("migkit-test-chch-dst", 15924)


@pytest.fixture(scope="module")
def servers():
    import clickhouse_connect
    for name, port in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "CLICKHOUSE_PASSWORD=test", "-e",
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


def _engine(tmp_path, **options):
    from migkit.engines.clickhouse import ClickHouseEngine

    def ep(port):
        return Endpoint(host="127.0.0.1", port=port, user="default",
                        password="test")
    hop = Hop(name="chch", engine="clickhouse", source=ep(SRC[1]),
              target=ep(DST[1]), databases=["app"], options=options)
    hop.report_dir = lambda db=None: tmp_path
    return ClickHouseEngine(hop)


def _table(client, name, partition=True, db="app"):
    client.command(f"create database if not exists {db}")
    client.command(
        f"create table {db}.{name} (id UInt64, at DateTime, kind String,"
        " note Nullable(String)) engine = MergeTree"
        + (" partition by toYYYYMM(at)" if partition else "")
        + " order by id")


def _fill(client, name, rows=3000, db="app"):
    client.command(
        f"insert into {db}.{name} select number, toDateTime('2024-01-01')"
        " + number * 3600, toString(number % 5), if(number % 7 = 0, NULL,"
        " 'n' || toString(number)) from numbers(" + str(rows) + ")")


def test_equal_tables_are_settled_on_the_servers(servers, tmp_path,
                                                 monkeypatch):
    src, dst = servers
    _table(src, "same")
    # the target not partitioned at all: grouped by the source's key
    _table(dst, "same", partition=False)
    _fill(src, "same")
    _fill(dst, "same")
    eng = _engine(tmp_path)
    read = []
    real = type(eng)._as_pair
    monkeypatch.setattr(type(eng), "_as_pair",
                        lambda self, *a: read.append(1) or real(self, *a))
    got = eng.check_data("app", table="same")
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    assert "3,000 rows in 5 partitions, each one's fingerprint equal" \
        in got[0].detail, got[0].detail
    # not one row read through migkit
    assert read == []
    s, d = eng._fingerprints("app", "same")
    assert s == d and len(s) == 5


def test_a_null_and_the_word_null_are_told_apart(servers, tmp_path):
    src, dst = servers
    src.command("create database if not exists app")
    dst.command("create database if not exists app")
    src.command("create table app.words (id UInt64, w String)"
                " engine = MergeTree order by id")
    dst.command("create table app.words (id UInt64, w Nullable(String))"
                " engine = MergeTree order by id")
    src.command("insert into app.words values (1, 'NULL')")
    dst.command("insert into app.words values (1, NULL)")
    eng = _engine(tmp_path)
    s, d = eng._fingerprints("app", "words")
    assert s != d
    got = eng.check_data("app", table="words")
    assert "diff" in [r.status for r in got], [r.detail for r in got]


def test_a_changed_row_falls_back_to_the_rows(servers, tmp_path):
    src, dst = servers
    _table(src, "edited")
    _table(dst, "edited")
    _fill(src, "edited")
    _fill(dst, "edited")
    dst.command("alter table app.edited update kind = 'x' where id = 42"
                " settings mutations_sync = 2")
    got = _engine(tmp_path).check_data("app", table="edited")
    assert "diff" in [r.status for r in got], [r.detail for r in got]


def test_delta_compares_only_the_partitions_that_changed(servers,
                                                         tmp_path):
    src, dst = servers
    # a database of its own: delta covers every table of one
    _table(src, "grows", db="delta")
    _table(dst, "grows", db="delta")
    _fill(src, "grows", db="delta")
    _fill(dst, "grows", db="delta")
    eng = _engine(tmp_path)
    eng.hop.databases = ["delta"]
    first = eng.delta_verify("delta")
    assert first[0].status == "ok", [r.detail for r in first]
    mine = [r for r in first if r.scope == "delta.grows"]
    assert mine and mine[0].detail.startswith("every partition"), mine
    # nothing written since: nothing of this table compared again
    again = eng.delta_verify("delta")
    assert not [r for r in again if r.scope == "delta.grows"], again
    # a row written on the source only, into one month
    src.command("insert into delta.grows values (999999,"
                " toDateTime('2024-02-10 00:00:00'), 'late', NULL)")
    got = eng.delta_verify("delta")
    row = [r for r in got if r.scope == "delta.grows"][0]
    assert row.status == "diff", row.detail
    assert row.detail == "1 changed partition(s): 1 differ (202402)"
    assert got[0].status == "diff" and "NOT advanced" in got[0].detail
    # held: the next run looks at it again, and passes once it is there
    dst.command("insert into delta.grows values (999999,"
                " toDateTime('2024-02-10 00:00:00'), 'late', NULL)")
    got = eng.delta_verify("delta")
    row = [r for r in got if r.scope == "delta.grows"][0]
    assert row.status == "ok", row.detail
    assert got[0].status == "ok" and "advanced" in got[0].detail


def test_a_setting_that_changes_values_is_named(servers, tmp_path):
    src, dst = servers
    for c in (src, dst):
        c.command("drop user if exists reader")
    src.command("create user reader identified with plaintext_password by"
                " 'CHANGE_ME'")
    dst.command("create user reader identified with plaintext_password by"
                " 'CHANGE_ME' settings join_use_nulls = 1")
    for c in (src, dst):
        c.command("grant select on system.* to reader")
    eng = _engine(tmp_path)
    for ep in (eng.hop.source, eng.hop.target):
        ep.user, ep.password = "reader", "CHANGE_ME"
    got = eng.check_params("app")
    assert got[0].status == "diff", got[0].detail
    assert "join_use_nulls src=0 dst=1" in got[0].detail
    saved = json.loads((tmp_path / "params.json").read_text())
    assert "server.timezone" in saved


def test_a_snapshot_freezes_the_target_tables(servers, tmp_path):
    src, dst = servers
    _table(dst, "kept")
    _fill(dst, "kept", 100)
    point = tmp_path / "20260927-0101"
    point.mkdir()
    _engine(tmp_path).snapshot_state("app", point)
    got = json.loads((point / "dst-tables.json").read_text())
    assert got["freeze"] == "migkit-20260927-0101"
    kept = got["tables"]["kept"]
    # 100 hours of rows: one month, one part
    assert kept["rows"] == 100 and kept["frozen"] == ["202401_1_1_0"], kept
    assert "CREATE TABLE app.kept" in kept["create"]


def test_roles_users_and_grants_are_carried_and_taken_back(servers,
                                                           tmp_path):
    from migkit import users
    src, dst = servers
    src.command("create database if not exists app")
    dst.command("create database if not exists app")
    src.command("create role if not exists analyst")
    src.command("grant select on app.* to analyst")
    src.command("create user if not exists ana identified with"
                " sha256_password by 'CHANGE_ME-ana' default role analyst")
    src.command("grant analyst to ana")
    src.command("grant insert on app.* to ana")
    src.command("create user if not exists nopw identified with"
                " sha256_password by 'CHANGE_ME-x'")
    hop = _engine(tmp_path).hop
    out, _ = users.compare(hop, [].append)
    assert {"role:analyst", "user:ana", "user:nopw"} <= \
        set(out["missing_on_target"]), out
    said = []
    users.create(hop, apply=True, passwords={"ana": "CHANGE_ME-ana"},
                 say=said.append)
    assert any("skipped user nopw" in m for m in said), said
    out, _ = users.compare(hop, [].append)
    assert "user:ana" not in out["missing_on_target"]
    assert "role:analyst" not in out["missing_on_target"]
    assert out["grants_differ"] == [], out
    # the password given is the one the target signs ana in with
    import clickhouse_connect
    c = clickhouse_connect.get_client(host="127.0.0.1", port=DST[1],
                                      username="ana",
                                      password="CHANGE_ME-ana")
    assert c.command("select count() from system.one") == 1
    c.close()
    users.rollback(hop, apply=True, say=[].append)
    names = {r[0] for r in dst.query(
        "select name from system.users union all select name from"
        " system.roles").result_rows}
    assert not {"ana", "analyst"} & names, names


def test_a_table_copied_keeps_its_types(servers, tmp_path):
    """Measured before: ClickHouse to ClickHouse through the table copier
    built every column String, and stopped on the first decimal it wrote
    into one. The same engine on both sides takes the
    source's own type; a column outside the key is made Nullable, as
    every table migkit makes here is."""
    from migkit.cli import _Checkpoint
    src, dst = servers
    src.command("create database if not exists typed")
    dst.command("create database if not exists typed")
    src.command("create table typed.t (id UInt32, amount Decimal(12, 3),"
                " at DateTime, kind LowCardinality(String), n Int16)"
                " engine = MergeTree order by id")
    src.command("insert into typed.t select number, number / 8, toDateTime("
                "'2024-01-01 00:00:00') + number, toString(number % 3),"
                " number from numbers(500)")
    eng = _engine(tmp_path)
    eng.hop.databases = ["typed"]
    eng.move_table("typed", "", "t", 100_000,
                   _Checkpoint(tmp_path / "move.json"), [].append)
    got = dict(dst.query("select name, type from system.columns where"
                         " database = 'typed' and table = 't'").result_rows)
    assert got == {"id": "UInt32", "amount": "Nullable(Decimal(12, 3))",
                   "at": "Nullable(DateTime)", "kind": "Nullable(String)",
                   "n": "Nullable(Int16)"}, got
    assert [r.status for r in eng.check_data("typed", table="t")] == ["ok"]
