"""The streaming pipeline's connector is configured from the hop and its
facts, not left at its defaults (backlog 0f).

Tested through the configuration migkit generates and the calls it makes,
without starting the connector runtime: the runtime pulls about a
gigabyte of images, and what these tests hold is what migkit asks of it.
One test, run only where `MIGKIT_LIVE_E2E=1` has the runtime up, asks
the runtime itself which options its build has.
"""
import json
import os
import re

import pytest

from migkit import movers, sizing
from migkit.config import Endpoint, Hop


def _hop(engine="postgres", **kw):
    return Hop(name="cf", engine=engine,
               source=Endpoint(host="10.0.0.1", port=1, user="u",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=2, user="u",
                               password="CHANGE_ME"),
               databases=["appdb"], **kw)


@pytest.fixture
def gen(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setattr(movers, "_gtid_on", lambda hop: False)
    monkeypatch.setattr(movers, "_pg_snapshot_watermarks",
                        lambda hop, db: False)
    facts = {"tables": None, "row": None, "rate": None}

    def fake(hop, dbs, my):
        tables = facts["tables"] or (["orders", "audit"] if my else
                                     ["public.orders", "public.audit"])
        return {d: tables for d in dbs}, facts["row"], facts["rate"]
    monkeypatch.setattr(movers, "_stream_facts", fake)

    def run(hop, engine=None):
        out = movers.stream_codegen(hop, ["appdb"], engine or hop.engine)
        return (json.loads((out / "source-connector.json").read_text())
                ["config"],
                json.loads((out / "sink-connector.json").read_text())
                ["config"])
    run.facts = facts
    return run


def test_transaction_boundaries_are_on_and_the_sink_reads_around_them(gen):
    src, sink = gen(_hop())
    assert src["provide.transaction.metadata"] == "true"
    prefix = src["topic.prefix"]
    txn = f"{prefix}.{src['topic.transaction']}"
    pattern = re.compile(sink["topics.regex"])
    # every table's topic, and not the transactions' one
    assert pattern.fullmatch(f"{prefix}.public.orders")
    assert pattern.fullmatch(f"{prefix}.transaction_log.orders")
    assert not pattern.fullmatch(txn), (sink["topics.regex"], txn)
    # nor the notifications', the signals' or the history's
    for topic in (src["notification.sink.topic.name"],
                  src["signal.kafka.topic"], f"{prefix}-history"):
        assert not pattern.fullmatch(topic), topic


def test_a_truncate_on_the_source_reaches_the_target(gen):
    """The connector's default leaves truncates out; the target then kept
    every row of a table the source had emptied."""
    src, sink = gen(_hop())
    assert src["skipped.operations"] == "none"
    assert sink["truncate.enabled"] == "true"


@pytest.mark.parametrize("engine,time_mode", [
    ("postgres", "adaptive"), ("mysql", "adaptive_time_microseconds")])
def test_values_arrive_in_the_forms_canon_compares(gen, engine, time_mode):
    """Exact decimals, microseconds, bytes as bytes - pinned, so a default
    that changes cannot change what lands. The lossy answers each option
    has are what is held out."""
    from migkit import canon
    src, _ = gen(_hop(engine))
    assert src["decimal.handling.mode"] == "precise"
    assert src["time.precision.mode"] == time_mode
    assert src["binary.handling.mode"] == "bytes"
    assert src["event.processing.failure.handling.mode"] == "fail"
    # the classes those three carry are ones canon compares exactly
    assert canon.type_class("postgres", "numeric(12,2)") == "decimal"
    assert canon.type_class("postgres", "time(6) without time zone") \
        == "time"
    assert canon.type_class("mysql", "varbinary(16)") == "bytes"


def test_the_buffers_follow_the_rows_carried(gen):
    gen.facts.update(row=1000.0, rate=None)
    src, sink = gen(_hop())
    batch = sizing.WORKER_MEMORY // sizing.BATCH_SHARE // 1000
    assert src["max.batch.size"] == str(batch)
    assert src["max.queue.size"] == str(4 * batch)
    assert src["max.queue.size.in.bytes"] == str(sizing.WORKER_MEMORY)
    assert src["poll.interval.ms"] == "500"
    # the sink writes a batch as large as the one it is handed
    assert sink["batch.size"] == str(batch)


def test_the_wait_for_a_batch_follows_the_rate_measured():
    got = sizing.stream_buffers(row_bytes=None, rows_per_second=40960)
    assert got["max.batch.size"] == "2048"
    assert got["poll.interval.ms"] == "50"
    slow = sizing.stream_buffers(row_bytes=None, rows_per_second=100)
    assert slow["poll.interval.ms"] == "500"
    mid = sizing.stream_buffers(row_bytes=None, rows_per_second=10240)
    assert mid["poll.interval.ms"] == "200"


def test_wide_rows_keep_the_connectors_own_floor():
    got = sizing.stream_buffers(row_bytes=10 * 1024 * 1024)
    assert got["max.batch.size"] == "2048"
    assert got["max.queue.size"] == "8192"
    assert got["max.queue.size.in.bytes"] == str(sizing.WORKER_MEMORY)


def test_the_columns_the_hop_keeps_out_are_left_out_at_the_source(
        gen, monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(MySQLEngine, "column_catalog",
                        lambda self, side, db: {
                            "orders": [("id", "int"), ("total", "int"),
                                       ("note", "text")]})
    src, _ = gen(_hop("mysql", mapping={"columns": {
        "orders": {"keep": ["id", "total"]}, "audit": {"drop": ["who"]}}}))
    assert src["column.exclude.list"] == (
        "appdb\\.audit\\.who,appdb\\.orders\\.note"), src


def test_kept_columns_that_cannot_be_matched_start_nothing(gen, monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(PostgresEngine, "column_catalog",
                        lambda self, side, db: {})
    with pytest.raises(SystemExit, match="could not be matched"):
        gen(_hop(mapping={"columns": {"public.orders": {"keep": ["id"]}}}))


@pytest.mark.parametrize("mapping,why", [
    ({"where": {"public.orders": "id > 10"}}, "its rows are filtered"),
    ({"tables": {"public.orders": "public.orders_v2"}},
     "it is renamed on the target"),
    ({"columns": {"public.orders": {"rename": {"a": "b"}}}},
     "its columns are renamed"),
])
def test_what_the_stream_cannot_carry_starts_nothing(gen, mapping, why):
    """It used to carry such a table whole under its own name, and a check
    reading through the hop's filter never saw the rows outside it."""
    with pytest.raises(SystemExit) as e:
        gen(_hop(mapping=mapping))
    assert why in str(e.value) and "public.orders" in str(e.value)
    assert "Nothing has been started" in str(e.value)


def test_a_mapping_that_renames_nothing_is_not_a_rename(gen):
    src, _ = gen(_hop(mapping={"tables": {"orders": "orders"}}))
    assert src["topic.prefix"] == "migkit-cf"


def test_an_excluded_table_is_not_refused_for_its_filter(gen):
    src, _ = gen(_hop(exclude=["audit"],
                      mapping={"where": {"public.audit": "id > 1"}}))
    assert src["table.exclude.list"] == "public\\.audit"


def test_the_signals_are_the_shapes_the_connector_reads():
    topic, key, value = movers.signal_message("h", "pause-snapshot")
    assert (topic, key) == ("migkit-h-signal", "migkit-h")
    assert value == {"type": "pause-snapshot", "data": {}}
    _, _, go = movers.signal_message("h", "resume-snapshot")
    assert go["type"] == "resume-snapshot"
    with pytest.raises(ValueError):
        movers.signal_message("h", "stop-everything")


NOTES = [
    {"aggregate_type": "Initial Snapshot", "type": "STARTED",
     "additional_data": {}},
    {"aggregate_type": "Initial Snapshot", "type": "IN_PROGRESS",
     "additional_data": {"current_collection_in_progress": "public.orders"}},
    {"aggregate_type": "Initial Snapshot", "type": "TABLE_SCAN_COMPLETED",
     "additional_data": {"data_collection": "public.orders",
                         "total_rows_scanned": "1500", "status":
                         "SUCCEEDED"}},
    {"aggregate_type": "Initial Snapshot", "type": "COMPLETED",
     "additional_data": {}},
    {"aggregate_type": "Something Else", "type": "STARTED"},
]


def test_the_connectors_notifications_are_said_in_migkits_words():
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    said = [movers.snapshot_words(n) for n in NOTES]
    assert said == ["the first read of the source started",
                    "the first read of the source: reading public.orders",
                    "the first read of the source: public.orders read,"
                    " 1,500 rows",
                    "the first read of the source finished", None], said
    assert not [t for t in TOOLS for s in said if s and t in s.lower()]


def test_progress_says_only_what_is_new(monkeypatch):
    monkeypatch.setattr(movers, "_notifications", lambda out, h: NOTES)
    lines, seen = movers.stream_progress(None, "h", since=2)
    assert seen == 5
    assert lines == ["the first read of the source: public.orders read,"
                     " 1,500 rows", "the first read of the source finished"]


def test_the_notifications_are_read_through_compose(monkeypatch, tmp_path):
    seen = {}

    class P:
        stdout = json.dumps(NOTES[0]) + "\nnot json\n"

    def fake_run(cmd, **kw):
        seen["cmd"] = cmd
        return P()
    monkeypatch.setattr(movers, "run", fake_run)
    got = movers._notifications(tmp_path, "h")
    assert got == [NOTES[0]]
    cmd = seen["cmd"]
    assert cmd[:3] == ["docker", "compose", "-f"] and "exec" in cmd
    assert "migkit-h-notifications" in cmd
    # to the end of what is there, then out - never waiting for more
    assert cmd[cmd.index("--offset") + 1] == ":end"


INCREMENTAL = [{"aggregate_type": "Incremental Snapshot", "type": t}
               for t in ("STARTED", "IN_PROGRESS")]


@pytest.mark.parametrize("strained,held,notes,sent,after", [
    ("source: 9 sessions running on 2 CPUs", False, INCREMENTAL,
     ["pause-snapshot"], True),
    ("source: 9 sessions running on 2 CPUs", False, NOTES, [], False),
    ("", True, INCREMENTAL, ["resume-snapshot"], False),
    ("source: 9 sessions running on 2 CPUs", True, INCREMENTAL, [], True),
    ("", False, INCREMENTAL, [], False),
])
def test_a_re_read_is_held_while_the_source_is_under_strain(
        monkeypatch, strained, held, notes, sent, after):
    signals = []
    monkeypatch.setattr(movers, "_notifications", lambda out, h: notes)
    monkeypatch.setattr(movers, "send_signal",
                        lambda out, h, s, *a, **k: signals.append(s))
    said = []
    assert movers.stream_hold(None, "h", strained, held, said.append) \
        is after
    assert signals == sent
    assert bool(said) == bool(sent), said


def test_a_finished_re_read_is_not_under_way():
    done = INCREMENTAL + [{"aggregate_type": "Incremental Snapshot",
                           "type": "COMPLETED"}]
    assert movers.rereading(INCREMENTAL)
    assert not movers.rereading(done)
    assert not movers.rereading(NOTES)


def test_a_configuration_the_runtime_refuses_registers_nothing(
        tmp_path, monkeypatch):
    calls = []

    def fake_api(method, path, body=None, port=8083):
        calls.append((method, path))
        if path.endswith("/config/validate"):
            return 200, {"configs": [{"value": {
                "name": "snapshot.locking.mode",
                "errors": ["Value must be one of minimal, extended,"
                           " none (io.debezium.connector.mysql)"]}}]}
        return 201, {}
    monkeypatch.setattr(movers, "_connect_api", fake_api)
    (tmp_path / "source-connector.json").write_text(json.dumps(
        {"name": "s", "config": {"connector.class": "x.MySqlConnector"}}))
    (tmp_path / "sink-connector.json").write_text(json.dumps(
        {"name": "k", "config": {}}))
    with pytest.raises(SystemExit) as e:
        movers.stream_register(tmp_path)
    assert "snapshot.locking.mode" in str(e.value)
    assert "debezium" not in str(e.value).lower()
    assert ("POST", "/connectors") not in calls, calls


def test_watch_holds_and_says(monkeypatch, tmp_path):
    from migkit import cli
    monkeypatch.setattr(movers, "_notifications", lambda out, h: INCREMENTAL)
    signals = []
    monkeypatch.setattr(movers, "send_signal",
                        lambda out, h, s, *a, **k: signals.append(s))
    monkeypatch.setattr(sizing, "strain",
                        lambda eng, db: "target: 5 sessions running on 2"
                                        " CPUs")
    lines, seen, held = cli._watch_stream(_hop(), None, ["appdb"], tmp_path,
                                          0, False)
    assert held is True and signals == ["pause-snapshot"]
    assert seen == 2
    assert any("held" in line for line in lines), lines


@pytest.mark.skipif(os.environ.get("MIGKIT_LIVE_E2E") != "1",
                    reason="set MIGKIT_LIVE_E2E=1 with the pipeline up")
def test_the_running_build_has_what_the_account_says():
    """The account's surface is the pinned release's documentation; this
    holds it against the build the runtime really runs."""
    from migkit import tooluse
    for connector, cls in (("postgres",
                            "io.debezium.connector.postgresql"
                            ".PostgresConnector"),
                           ("mysql", "io.debezium.connector.mysql"
                                     ".MySqlConnector")):
        _, got = movers._connect_api(
            "PUT", f"/connector-plugins/{cls}/config/validate",
            {"connector.class": cls, "topic.prefix": "x"})
        names = {c["definition"]["name"] for c in got["configs"]}
        declared = tooluse.surface("debezium")[connector]
        # measured working and not advertised by the PostgreSQL connector
        # (`_pg_snapshot_watermarks`)
        quiet = {"read.only"} if connector == "postgres" else set()
        missing = ({o for o in declared if not o.endswith(".")} - names
                   - quiet)
        assert not missing, (connector, sorted(missing))
