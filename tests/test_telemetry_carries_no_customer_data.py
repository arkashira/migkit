"""Product telemetry says what migkit did, never what it carried
(backlog P0-PRODUCT-TELEMETRY).

Canaries - a password, a table's and a column's name, a value, a
statement, a DDL line, an address, a path - are planted everywhere a run
can hold them: the work a decision is for, the reasons a way was passed
over, a way's label made at run time, the command's options and the words
of the exception that stopped it. None of them may reach the spool or
what `migkit telemetry show` prints, and a producer handing a free string
or a field outside the schema is refused where the event is made.
"""
import json
import time

import pytest
from click.testing import CliRunner

from migkit import decide, session, telemetry

CANARIES = (
    "hunter2-canary-pw", "canary_orders", "canary_ssn_col",
    "123-45-6789", "SELECT canary_ssn_col FROM canary_orders",
    "CREATE TABLE canary_orders", "db7.canary.example.com",
    "/srv/canary/secrets.env", "postgres://app:hunter2-canary-pw@db7",
)


@pytest.fixture
def spool(tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_TELEMETRY_SPOOL", str(tmp_path / "spool"))
    monkeypatch.setenv("MIGKIT_TELEMETRY_POLICY", str(tmp_path / "none"))
    monkeypatch.delenv("MIGKIT_TELEMETRY", raising=False)
    monkeypatch.delenv("MIGKIT_EGRESS", raising=False)
    monkeypatch.setattr(telemetry, "_DISPOSED", False)
    telemetry.flush()
    return tmp_path / "spool"


def _kept(root):
    assert telemetry.flush(5)
    return b"".join(p.read_bytes() for p in sorted(root.glob("*.jsonl")))


def _no_canary(blob):
    text = blob.decode() if isinstance(blob, bytes) else blob
    for canary in CANARIES:
        assert canary not in text, canary
    for part in ("canary", "hunter2", "6789", "SELECT", "CREATE", "/srv"):
        assert part not in text, part


def _rungs():
    plain = decide.Rung("canary/" + "db7.canary.example.com",
                        "the way named from what the server said")
    keyed = decide.Rung("range", "ranges of the key",
                        gives=frozenset({"resume by key"}))
    return plain, keyed


def test_a_run_full_of_canaries_keeps_only_codes(spool):
    plain, keyed = _rungs()
    params = {"hop_name": None, "table": "canary_orders",
              "where": "canary_ssn_col = '123-45-6789'", "go": True}
    with pytest.raises(RuntimeError):
        with session.step("check", params):
            decide.climb("the rows of canary_orders (canary_ssn_col)",
                         [plain, keyed], need=("resume by key",))
            decide.climb("SELECT canary_ssn_col FROM canary_orders",
                         [keyed, plain])
            raise RuntimeError("CREATE TABLE canary_orders failed at"
                               " /srv/canary/secrets.env for"
                               " postgres://app:hunter2-canary-pw@db7")
    blob = _kept(spool)
    _no_canary(blob)
    events = [json.loads(x) for x in blob.decode().splitlines()]
    kinds = [e["kind"] for e in events]
    assert kinds.count("decision") == 2 and kinds[-1] == "run", kinds
    first = events[0]
    assert "resume by key" in first["reasons"], first
    assert first["passed"] == 1 and first["ranked"] == 1
    run = events[-1]
    assert run["command"] == "check" and run["completion"] == "failed"
    assert run["error"] == "internal" and run["writes"] is True
    assert run["decisions"] == 2
    # the label made at run time is not a strategy the ladders know
    assert {e.get("strategy") for e in events[:2]} <= (
        set(telemetry.strategies().values()) | {"other"})


def test_show_prints_only_what_is_kept_and_no_canary(spool):
    with session.step("check", {"hop_name": None,
                                "table": "canary_orders"}):
        decide.climb("canary_orders", list(_rungs()))
    got = CliRunner().invoke(_main(), ["telemetry", "show", "--schema"])
    assert got.exit_code == 0, got.output
    assert "telemetry: on" in got.output and '"kind": "run"' in got.output
    _no_canary(got.output)


def _main():
    from migkit.cli import main
    return main


def test_a_field_outside_the_schema_or_a_free_string_is_refused():
    with pytest.raises(telemetry.Rejected) as e:
        telemetry.make("run", canary_orders=1)
    assert "canary" not in str(e.value)
    for bad in ({"command": "SELECT 1"}, {"engine": "canary_orders"},
                {"seconds": True}, {"seconds": -1}, {"seconds": 1e9},
                {"decisions": 1.5}, {"writes": 1},
                {"completion": b"done"}, {"seconds": float("nan")}):
        with pytest.raises(telemetry.Rejected) as e:
            telemetry.make("run", **bad)
        _no_canary(str(e.value))
    for bad in ({"reasons": ("canary_orders",)},
                {"reasons": "keyless"},
                {"guarantees": ("correct:everything",)},
                {"reasons": tuple(decide.WORDS)[:17]}):
        with pytest.raises(telemetry.Rejected):
            telemetry.make("decision", **bad)
    with pytest.raises(telemetry.Rejected):
        telemetry.make("dump", detail="x")
    ok = telemetry.make("decision", reasons=["keyless", "keyless"],
                        guarantees=("correct:proved",), ranked=2)
    assert ok["reasons"] == ["keyless"] and ok["ranked"] == 2


def test_every_field_is_a_code_a_bucket_a_bound_number_or_a_flag():
    import dataclasses
    for cls in telemetry.KINDS.values():
        for f in dataclasses.fields(cls):
            assert f.metadata["type"] in ("one of", "some of", "whole",
                                          "number", "flag", "opaque"), \
                f.name
            if f.metadata["type"] in ("one of", "some of"):
                values = f.metadata["values"]()
                assert values and all(type(v) is str for v in values)


def test_an_exception_is_kept_by_its_type_never_its_words():
    from migkit import verdict
    secret = "password=hunter2-canary-pw at /srv/canary"
    assert telemetry.error_category(TimeoutError(secret)) == "timeout"
    assert telemetry.error_category(PermissionError(secret)) == \
        "permission"
    assert telemetry.error_category(ConnectionResetError(secret)) == \
        "connection"
    assert telemetry.error_category(verdict.Refused(secret)) == "refused"
    assert telemetry.error_category(SystemExit(secret)) == "stopped"
    assert telemetry.error_category(SystemExit(1)) == "none"
    assert telemetry.error_category(KeyError(secret)) == "not found"
    assert telemetry.error_category(RuntimeError(secret)) == "internal"
    assert telemetry.completion(1) == "different"
    assert telemetry.completion(130) == "interrupted"
    assert telemetry.completion(0) == "done"
    assert telemetry.completion(None, KeyboardInterrupt()) == "interrupted"
    assert telemetry.completion("words") == "failed"


@pytest.mark.parametrize("how", ["env", "egress", "policy", "unreadable",
                                 "command"])
def test_off_stops_queueing_and_disposes_of_the_spool(spool, tmp_path,
                                                      monkeypatch, how):
    assert telemetry.record("intervention", phase="move", did="paused")
    assert _kept(spool)
    if how == "env":
        monkeypatch.setenv("MIGKIT_TELEMETRY", "off")
    elif how == "egress":
        monkeypatch.setenv("MIGKIT_EGRESS", "notify")
    elif how in ("policy", "unreadable"):
        policy = tmp_path / "policy.yaml"
        policy.write_text("telemetry: off\n" if how == "policy"
                          else ": [unclosed\n")
        monkeypatch.setenv("MIGKIT_TELEMETRY_POLICY", str(policy))
    else:
        got = CliRunner().invoke(_main(), ["telemetry", "off"])
        assert got.exit_code == 0 and "telemetry: off" in got.output
    on, why = telemetry.state()
    assert not on and why
    assert not telemetry.record("intervention", phase="move",
                                did="resumed")
    assert not list(spool.glob("events-*.jsonl"))
    if how == "command":
        CliRunner().invoke(_main(), ["telemetry", "on"])
        assert telemetry.state()[0]


def test_a_policy_saying_nothing_of_telemetry_leaves_it_on(spool, tmp_path,
                                                          monkeypatch):
    policy = tmp_path / "policy.yaml"
    policy.write_text("telemetry: on\nother: x\n")
    monkeypatch.setenv("MIGKIT_TELEMETRY_POLICY", str(policy))
    assert telemetry.state()[0]
    monkeypatch.setenv("MIGKIT_EGRESS", "notify,telemetry")
    assert telemetry.state()[0]


def test_the_store_is_bounded_and_seals_what_fills(spool):
    sink = telemetry.Local(spool)
    line = json.dumps(telemetry.make("progress", phase="move",
                                     rows=telemetry.bucket(10 ** 6)))
    many = (telemetry.FILE_BYTES * (telemetry.FILES + 2)) // len(line)
    sink.put([line] * many)
    held = sink.segments()
    files = [p for _, p in held] + list(spool.glob("events-*.jsonl"))
    assert len(held) == telemetry.FILES - 1 == telemetry.keeps()[0]
    assert [n for n, _ in held] == list(range(held[0][0], held[0][0]
                                              + len(held)))
    assert all(p.stat().st_size <= telemetry.FILE_BYTES for p in files)
    assert sink.bytes() <= telemetry.FILE_BYTES * telemetry.FILES
    assert all(p.stat().st_mode & 0o077 == 0 for p in files)
    assert len(sink.read()) < many and sink.read()[-1]["phase"] == "move"
    assert telemetry.unsent() >= 2     # the oldest went before a push
    assert sink.dispose() == telemetry.FILES
    assert sink.read() == [] and not list(spool.rglob("*.seg"))


def test_a_full_queue_drops_the_event_and_never_waits(spool, monkeypatch):
    import queue
    monkeypatch.setattr(telemetry, "_Q", queue.Queue(maxsize=1))
    monkeypatch.setattr(telemetry, "_started", lambda: None)
    before = telemetry.dropped()
    began = time.monotonic()
    got = [telemetry.record("intervention", phase="move", did="paused")
           for _ in range(50)]
    assert time.monotonic() - began < 1.0
    assert got[0] and not any(got[1:])
    assert telemetry.dropped() - before == 49


def test_nothing_leaves_but_by_the_push_through_the_egress_gate():
    import inspect
    with pytest.raises(TypeError):
        telemetry.Sink().put(["{}"])
    assert [k for k in dir(telemetry) if "send" in k.lower()
            or "upload" in k.lower() or "http" in k.lower()] == []
    # the one call that writes out is the push's conditional put, and both
    # ways out ask the gate before they make a client
    src = inspect.getsource(telemetry)
    assert src.count(".put_object(") == 1
    for way in (telemetry.push, telemetry.receive):
        body = inspect.getsource(way)
        assert body.index("_gate(conf)") < body.index("_client(conf)")
    assert "on-run-end" not in src and "atexit.register(push" not in src


def test_buckets_are_coarse_and_closed():
    assert telemetry.bucket(0) == "0" and telemetry.bucket(7) == "1-9"
    assert telemetry.bucket(12345) == "1e4-1e5"
    assert telemetry.bucket(10 ** 15) == ">=1e12"
    assert telemetry.seconds_bucket(0.2) == "<1s"
    assert telemetry.seconds_bucket(70) == "64s+"
    assert telemetry.coverage(0, 10) == "0"
    assert telemetry.coverage(10, 10) == "100%"
    assert telemetry.coverage(995, 1000) == "<100%"
    assert telemetry.coverage(1, 3) == "<50%"


def test_the_disclosure_is_in_the_manual_and_names_no_program():
    from migkit import export
    for heading, text in telemetry.DISCLOSURE:
        assert export.words(text) == text, heading
    assert all(export.words(s) == s
               for s in telemetry.strategies().values())


# ---- the five other kinds, and crashes, from their owners ----------------------

def _events(root):
    blob = _kept(root)
    _no_canary(blob)
    return [json.loads(x) for x in blob.decode().splitlines()]


class _Pace:
    """What `planner.progress` reads of a move's pace."""
    limit, history = 3, [(3, None, "start")]
    started, recent = 0.0, 50.0

    def __init__(self, total):
        self.total = total

    def took(self):
        return 20.0

    def _clock(self):
        return 10.0

    def held_by(self):
        return None


def test_progress_and_resources_keep_buckets_and_rates_only(spool,
                                                           monkeypatch):
    from migkit import model, planner
    monkeypatch.setattr(telemetry, "_PACE", {})
    where = spool.parent / "canary_orders"
    planner.progress(where, _Pace(500), 1000)
    planner.progress(where, _Pace(1000), 1000, done=True)
    m = model.Model(measured=False, worker_memory=10 ** 8)
    m.observe([{"table": "canary_orders", "work": "canary_ssn_col"}],
              5000, 20.0, 3)
    got = _events(spool)
    kinds = [e["kind"] for e in got]
    assert kinds.count("progress") == 2 and "resources" in kinds, kinds
    last = [e for e in got if e["kind"] == "progress"][-1]
    assert last["done"] == "100%" and last["rows"] == "1e3-1e4"
    assert last["eta_off"] > 0
    res = [e for e in got if e["kind"] == "resources"][0]
    assert res["workers_used"] == 3 and res["memory"] == "1e8-1e9"
    for e in got:
        assert set(e) <= {"schema", "kind", "hour", "phase", "rows", "rate",
                          "took", "done", "eta_off", "bytes", "cpus",
                          "memory", "workers_allocated", "workers_used",
                          "connections"}, e


def test_a_recovery_keeps_the_class_and_the_action_never_the_words(spool):
    from migkit import faults
    seen = []

    def work():
        seen.append(1)
        if len(seen) < 3:
            raise Exception("lost connection to db7.canary.example.com"
                            " while SELECT canary_ssn_col")
        return "canary_orders"
    assert faults.retried(work, wait=lambda s: None) == "canary_orders"

    def never():
        raise Exception("password authentication failed for user"
                        " hunter2-canary-pw")
    with pytest.raises(Exception):
        faults.retried(never, wait=lambda s: None)
    got = [e for e in _events(spool) if e["kind"] == "recovery"]
    assert got[0]["fault"] == "connectivity" and got[0]["path"] == "retry"
    assert got[0]["outcome"] == "recovered" and got[0]["attempts"] == 3
    assert got[1]["outcome"] == "failed" and got[1]["fault"] == "auth"
    assert {e["error"] for e in got} <= set(telemetry.ERRORS)


def test_a_check_keeps_how_much_was_compared_and_the_guarantees(spool):
    from migkit import verdict
    records = [{"scope": "canary_orders", "status": "ok",
                "detail": "canary_ssn_col 123-45-6789"},
               {"scope": "canary_orders_2", "status": "ok"},
               {"scope": "canary_orders_3", "status": "diff",
                "detail": "SELECT canary_ssn_col"}]
    verdict.summarize(None, records)
    verdict.summarize(None, records[:2])
    got = [e for e in _events(spool) if e["kind"] == "coverage"]
    assert got[0]["verified"] == "<90%" and got[0]["completion"] == \
        "different" and "guarantees" not in got[0]
    assert got[1]["verified"] == "100%" and got[1]["units"] == "1-9"
    assert got[1]["completion"] in telemetry.COMPLETIONS


def test_a_question_to_the_operator_is_kept_by_its_key_alone(spool):
    from migkit import autonomous
    with pytest.raises(autonomous.Asked):
        autonomous.ask(None, "target")
    autonomous.ask(lambda q, d: "postgres://app:hunter2-canary-pw@db7",
                   "password")
    got = [e for e in _events(spool) if e["kind"] == "intervention"]
    assert [(e["did"], e["question"]) for e in got] == [
        ("asked", "target"), ("asked", "password"),
        ("answered", "password")]


def test_a_crash_is_a_category_and_a_phase_the_traceback_stays_local(
        spool, tmp_path, monkeypatch):
    import click

    from migkit import cli, config
    monkeypatch.setattr(config, "REPORTS", tmp_path / "reports")
    ctx = click.Context(click.Command("check"), info_name="check")
    code = cli._stopped(RuntimeError("SELECT canary_ssn_col FROM"
                                     " canary_orders hunter2-canary-pw"), ctx)
    assert code != 0
    got = [e for e in _events(spool) if e["kind"] == "crash"]
    assert len(got) == 1 and got[0]["command"] == "check"
    assert got[0]["phase"] == "verify" and got[0]["error"] == "internal"
    assert set(got[0]) == {"schema", "kind", "hour", "command", "phase",
                           "error"}
    local = (tmp_path / "reports" / "commands.log").read_text()
    assert "RuntimeError" in local and "canary_ssn_col" in local


def test_a_support_bundle_adds_only_a_code_to_the_spool(spool):
    assert telemetry.intervened("bundled", phase="report")
    got = _events(spool)
    assert got[-1]["did"] == "bundled" and "path" not in got[-1]


def test_a_producer_cannot_hand_the_new_kinds_free_text():
    for kind, bad in (("crash", {"error": "SELECT canary_ssn_col"}),
                      ("recovery", {"fault": "canary_orders"}),
                      ("recovery", {"path": "retry 3 times"}),
                      ("intervention", {"question": "canary_orders"}),
                      ("coverage", {"verified": "99.5%"}),
                      ("progress", {"rows": "12345"}),
                      ("resources", {"memory": "5 GB"})):
        with pytest.raises(telemetry.Rejected) as e:
            telemetry.make(kind, **bad)
        _no_canary(str(e.value))
    assert telemetry.SCHEMA == 1.1


def test_the_spool_adds_bounded_time_per_thousand_events_and_never_blocks(
        spool):
    began = time.monotonic()
    for _ in range(1000):
        telemetry.record("progress", phase="move", rows="1e3-1e4",
                         rate="1e1-1e2", took="8s+", done="<90%")
    caller = time.monotonic() - began
    assert caller < 2.0, caller
    assert telemetry.flush(30)
    assert telemetry.Local(spool).bytes() <= (telemetry.FILE_BYTES
                                              * telemetry.FILES)


def test_off_in_the_middle_of_a_run_stops_queueing_and_disposes(spool,
                                                                monkeypatch):
    for _ in range(20):
        assert telemetry.record("progress", phase="move", done="<50%")
    assert _kept(spool)
    monkeypatch.setenv("MIGKIT_TELEMETRY", "off")
    began = time.monotonic()
    got = [telemetry.record("progress", phase="move", done="<50%")
           for _ in range(1000)]
    assert not any(got) and time.monotonic() - began < 2.0
    assert not list(spool.glob("events-*.jsonl"))
    assert telemetry.progressed("move", 1, 10, 1.0, 1.0) is False
    assert telemetry.flush(5) and not list(spool.glob("events-*.jsonl"))


def test_each_unit_is_kept_by_an_opaque_id_with_its_own_outcome(spool):
    from migkit import verdict
    records = [{"scope": "canary_orders.public.canary_orders",
                "status": "ok"},
               {"scope": "canary_orders.public.canary_ssn_col",
                "status": "diff", "detail": "123-45-6789",
                "guarantees": ["correct:proved", "SELECT canary_ssn_col"]},
               {"scope": "canary_orders", "status": "ok"},
               {"scope": "canary_orders.t", "status": "ok",
                "range": "123-45-6789..hunter2-canary-pw"}]
    verdict.summarize(None, records)
    blob = _kept(spool)
    _no_canary(blob)
    got = [e for e in _events(spool) if e["kind"] == "unit"]
    assert len(got) == 4
    assert got[0]["outcome"] == "diff" and got[0]["scope"] == "table"
    assert {e["scope"] for e in got} == {"table", "database", "range"}
    ids = [e["unit"] for e in got]
    assert len(set(ids)) == 4
    assert all(len(i) == 12 and set(i) <= set("0123456789abcdef")
               for i in ids)
    assert set(got[0]["guarantees"]) <= set(telemetry._guarantees())
    assert "SELECT" not in str(got)
    assert ids[0] == telemetry.unit_id(
        "table", "canary_orders.public.canary_ssn_col")


def test_an_id_is_the_same_in_every_run_of_one_install_and_not_another(
        spool, tmp_path, monkeypatch):
    monkeypatch.setattr(telemetry, "_SALT", None)
    first = telemetry.unit_id("table", "canary_orders")
    monkeypatch.setattr(telemetry, "_SALT", None)
    assert telemetry.unit_id("table", "canary_orders") == first
    other = tmp_path / "other"
    monkeypatch.setenv("MIGKIT_TELEMETRY_SPOOL", str(other))
    monkeypatch.setattr(telemetry, "_SALT", None)
    assert telemetry.unit_id("table", "canary_orders") != first


def test_an_opaque_field_refuses_a_name_and_an_unknown_one():
    ok = telemetry.make("unit", phase="verify", scope="table",
                        unit="0123456789ab", outcome="ok")
    assert ok["unit"] == "0123456789ab"
    for bad in ("canary_orders", "0123456789AB", "0123456789abc", 12, ""):
        with pytest.raises(telemetry.Rejected) as err:
            telemetry.make("unit", phase="verify", scope="table",
                           unit=bad, outcome="ok")
        assert "canary" not in str(err.value)
    with pytest.raises(telemetry.Rejected):
        telemetry.make("unit", phase="verify", scope="canary_orders",
                       unit="0123456789ab", outcome="ok")


def test_only_the_units_that_did_not_compare_come_first_and_the_rest_are_cut(
        spool, monkeypatch):
    monkeypatch.setattr(telemetry, "UNITS_KEPT", 3)
    records = [{"scope": f"db.t{i}", "status": "ok"} for i in range(5)]
    records.append({"scope": "db.bad", "status": "diff"})
    telemetry.verified(records, verdict_decision())
    got = [e for e in _events(spool) if e["kind"] == "unit"]
    assert len(got) == 3 and got[0]["outcome"] == "diff"


def verdict_decision():
    from migkit import verdict
    return verdict.Decision("different", "", ())


def test_eta_off_goes_for_every_phase_that_had_a_forecast(spool,
                                                          monkeypatch):
    from migkit import planner
    monkeypatch.setattr(telemetry, "_PLANNED", {})
    monkeypatch.setattr(telemetry, "_SETTLED", set())
    plan = [planner.Phase("copy", 100.0), planner.Phase("catch-up", 50.0),
            planner.Phase("check", 20.0),
            planner.Phase("cutover", never=True, detail="canary_orders"),
            planner.Phase("index builds")]
    planner.phase_lines(plan)
    assert telemetry._PLANNED == {"move": 100.0, "follow": 50.0,
                                  "verify": 20.0}
    assert telemetry.settled("follow", 100.0) is True
    assert telemetry.settled("follow", 100.0) is False
    assert telemetry.settled("assess", 5.0) is False
    cmd = telemetry.Command("check", {})
    telemetry.forecast("verify", 20.0)
    time.sleep(0.01)
    cmd.ended(0)
    got = [e for e in _events(spool) if e["kind"] == "progress"]
    by = {e["phase"]: e for e in got}
    assert by["follow"]["eta_off"] == 2.0
    assert by["verify"]["done"] == "100%" and by["verify"]["eta_off"] < 1
    assert set(by) == {"follow", "verify"}


def test_a_phase_that_reported_its_own_pace_is_not_said_twice(spool,
                                                              monkeypatch):
    monkeypatch.setattr(telemetry, "_PACE", {})
    monkeypatch.setattr(telemetry, "_PLANNED", {"move": 10.0})
    monkeypatch.setattr(telemetry, "_SETTLED", set())
    telemetry.progressed("move", 5, 10, 1.0, 5.0, now=0.0)
    telemetry.progressed("move", 10, 10, 1.0, 10.0, done=True, now=1.0)
    assert telemetry.settled("move", 10.0) is False
    got = [e for e in _events(spool) if e["kind"] == "progress"]
    assert len(got) == 2


def test_databases_moving_side_by_side_keep_their_own_estimate(spool,
                                                              monkeypatch):
    monkeypatch.setattr(telemetry, "_PACE", {})
    monkeypatch.setattr(telemetry, "_SETTLED", set())
    a, b = "/reports/canary_orders", "/reports/other"
    telemetry.progressed("move", 100, 1000, 10.0, 10.0, now=0.0, where=a)
    telemetry.progressed("move", 10, 100, 10.0, 1.0, now=0.0, where=b)
    telemetry.progressed("move", 200, 1000, 10.0, 20.0, now=40.0, where=a)
    telemetry.progressed("move", 20, 100, 10.0, 2.0, now=40.0, where=b)
    assert len(telemetry._PACE) == 2
    telemetry.progressed("move", 1000, 1000, 10.0, 100.0, done=True,
                         now=80.0, where=a)
    telemetry.progressed("move", 100, 100, 10.0, 10.0, done=True,
                         now=80.0, where=b)
    done = [e for e in _events(spool) if e["kind"] == "progress"
            and e["done"] == "100%"]
    assert [e["eta_off"] for e in done] == [1.0, 1.0]
    assert telemetry._PACE == {}
    assert "canary" not in _kept(spool).decode()


# ---- the sealed store, the push's gate and the receiver's check -----------------

def _lines(n, phase="move"):
    return [json.dumps(telemetry.make("progress", phase=phase, done="<50%"),
                       sort_keys=True) for _ in range(n)]


def test_a_sealed_segment_is_chained_and_its_manifest_names_it(spool):
    sink = telemetry.Local(spool)
    sink.put(_lines(3))
    first = sink.seal()
    sink.put(_lines(2, "verify"))
    second = sink.seal()
    one, two = sink.manifest(first), sink.manifest(second)
    assert (one["seq"], one["count"], one["prev"]) == (1, 3, sink.seed())
    assert two["prev"] == one["chain"] and two["count"] == 2
    assert one["first"] == one["last"] == telemetry.make(
        "progress", phase="move")["hour"]
    for path in (first, second):
        doc, events, bad = telemetry.segment_checked(path.read_bytes(),
                                                     sink.manifest(path)
                                                     ["digest"])
        assert len(events) == doc["count"] and bad == []
        assert path.stat().st_mode & 0o077 == 0
    assert [e["phase"] for e in sink.read()] == ["move"] * 3 + ["verify"] * 2
    assert sink.seal() is None


def test_a_seal_cut_anywhere_seals_each_event_once(spool, monkeypatch):
    from migkit import durable
    sink = telemetry.Local(spool)
    sink.put(_lines(4))
    real = durable.write_atomic

    def cut(path, data, *a, **k):
        raise OSError("power gone")
    monkeypatch.setattr(durable, "write_atomic", cut)
    with pytest.raises(OSError):
        sink.seal()          # renamed, never written
    assert list(spool.glob("sealing-*.jsonl")) and not sink.segments()
    monkeypatch.setattr(durable, "write_atomic", real)
    sink.put(_lines(1, "verify"))
    sink.seal()
    assert [sink.manifest(p)["count"] for _, p in sink.segments()] == [4, 1]
    # written whole, then cut before the renamed head went: not twice
    sink.put(_lines(2))
    pending = spool / "sealing-00000003.jsonl"
    sink._file(0).replace(pending)
    sink._close(pending)
    pending.write_text("\n".join(_lines(2)) + "\n")
    sink.seal()
    assert [sink.manifest(p)["count"] for _, p in sink.segments()] \
        == [4, 1, 2] and len(sink.read()) == 7


def test_the_policy_says_how_many_segments_and_days_are_kept(spool,
                                                             tmp_path,
                                                             monkeypatch):
    policy = tmp_path / "policy.yaml"
    policy.write_text("telemetry_keep: 2\n")
    monkeypatch.setenv("MIGKIT_TELEMETRY_POLICY", str(policy))
    sink = telemetry.Local(spool)
    for _ in range(4):
        sink.put(_lines(1))
        sink.seal()
    assert [n for n, _ in sink.segments()] == [3, 4]
    old = json.loads(_lines(1)[0])
    old["hour"] = "2001-01-01T00"
    policy.write_text("telemetry_keep: 9\ntelemetry_days: 30\n")
    sink.put([json.dumps(old)])
    sink.seal()
    assert [n for n, _ in sink.segments()] == [3, 4]


def test_off_disposes_the_segments_and_the_pushes_pending(spool):
    sink = telemetry.Local(spool)
    sink.put(_lines(2))
    sink.seal()
    sink.put(_lines(2))
    got = CliRunner().invoke(_main(), ["telemetry", "off"])
    assert got.exit_code == 0 and "telemetry: off" in got.output
    assert not sink.segments() and not list(spool.rglob("*.jsonl"))
    got = CliRunner().invoke(_main(), ["telemetry", "push", "b"])
    assert got.exit_code == 4 and "telemetry is off" in got.output
    CliRunner().invoke(_main(), ["telemetry", "on"])


@pytest.mark.parametrize("how", ["egress", "egress-to", "policy",
                                 "push-off"])
def test_a_push_refused_says_why_and_connects_nowhere(spool, tmp_path,
                                                      monkeypatch, how):
    from migkit import anchor, verdict
    telemetry.Local(spool).put(_lines(2))
    reached = []
    monkeypatch.setattr(anchor, "_client", lambda conf: reached.append(1))
    policy = tmp_path / "policy.yaml"
    if how == "egress":
        monkeypatch.setenv("MIGKIT_EGRESS", "none")
    elif how == "egress-to":
        monkeypatch.setenv("MIGKIT_EGRESS_TO", "10.0.0.9")
    else:
        policy.write_text("telemetry: off\n" if how == "policy"
                          else "telemetry_push: off\n")
        monkeypatch.setenv("MIGKIT_TELEMETRY_POLICY", str(policy))
    with pytest.raises(verdict.Refused) as got:
        telemetry.push("bucket", "http://10.0.0.1:9000")
    said = str(got.value)
    assert {"egress": "MIGKIT_EGRESS does not let",
            "egress-to": "a host the egress does not name",
            "policy": "telemetry is off",
            "push-off": "telemetry_push: off"}[how] in said
    if how != "policy":
        assert telemetry.Local(spool).segments() == []   # nothing sealed
    if how.startswith("egress"):
        with pytest.raises(verdict.Refused):
            telemetry.receive("bucket", "http://10.0.0.1:9000")
    assert reached == []
    _no_canary(said)


def test_the_receiver_runs_the_producers_check_again():
    good = telemetry.make("run", command="move", engine="postgres",
                          completion="done")
    assert telemetry.checked(dict(good)) == good
    assert telemetry.checked({**good, "version": "9.1.0"})["version"]
    for bad, why in (({**good, "canary_orders": 1}, "not in its schema"),
                     ({**good, "engine": "canary_orders"}, "not free text"),
                     ({**good, "hour": "SELECT 1"}, "takes an hour"),
                     ({**good, "schema": 7}, "schema"),
                     ({**good, "kind": "canary"}, "kind"),
                     ({**good, "version": "/srv/canary/secrets.env"},
                      "version"),
                     ({**good, "seconds": "123-45-6789"}, "a number")):
        with pytest.raises(telemetry.Rejected) as got:
            telemetry.checked(bad)
        assert why in str(got.value)
        _no_canary(str(got.value))


def test_a_blocked_writer_drops_and_counts_and_the_step_never_waits(
        spool, monkeypatch):
    import threading
    assert telemetry._Q.maxsize == telemetry.QUEUED
    telemetry.record("progress", phase="move", done="<50%")
    assert telemetry.flush(5)
    gate, real = threading.Event(), telemetry.Local.put
    monkeypatch.setattr(telemetry.Local, "put",
                        lambda self, lines: (gate.wait(10),
                                             real(self, lines)))
    before = telemetry.dropped()
    began = time.monotonic()
    many = telemetry.QUEUED * 4
    got = [telemetry.record("progress", phase="move", done="<50%")
           for _ in range(many)]
    step = time.monotonic() - began
    gate.set()
    assert telemetry.flush(30)
    assert step < 1.0, step                        # the step never waited
    # the queue's cap held: the queue and the one batch the writer took
    assert sum(got) <= 2 * telemetry.QUEUED
    assert telemetry.dropped() - before == many - sum(got)


def test_the_writer_keeps_up_in_synced_batches(spool):
    began, kept = time.monotonic(), 0
    for _ in range(6):
        for _ in range(telemetry.QUEUED - 1):
            kept += telemetry.record("progress", phase="move", done="<90%")
        assert telemetry.flush(30)
    took = time.monotonic() - began
    sink = telemetry.Local(spool)
    rate = sink.bytes() / took
    assert kept == 6 * (telemetry.QUEUED - 1)
    assert rate > 64 * 1024, rate        # bytes a second, synced
    assert sink.segments() and sink.bytes() <= (telemetry.FILE_BYTES
                                                * telemetry.FILES)
