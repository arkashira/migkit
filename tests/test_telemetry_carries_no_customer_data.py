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
        telemetry.make("crash", detail="x")
    ok = telemetry.make("decision", reasons=["keyless", "keyless"],
                        guarantees=("correct:proved",), ranked=2)
    assert ok["reasons"] == ["keyless"] and ok["ranked"] == 2


def test_every_field_is_a_code_a_bucket_a_bound_number_or_a_flag():
    import dataclasses
    for cls in telemetry.KINDS.values():
        for f in dataclasses.fields(cls):
            assert f.metadata["type"] in ("one of", "some of", "whole",
                                          "number", "flag"), f.name
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
    assert telemetry.record("intervention", phase="move", kind="paused")
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
                                kind="resumed")
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


def test_the_spool_is_bounded_and_rotates(tmp_path):
    sink = telemetry.Local(tmp_path)
    line = json.dumps(telemetry.make("progress", phase="move",
                                     rows=telemetry.bucket(10 ** 6)))
    many = (telemetry.FILE_BYTES * (telemetry.FILES + 2)) // len(line)
    sink.put([line] * many)
    files = sorted(tmp_path.glob("events-*.jsonl"))
    assert len(files) == telemetry.FILES
    assert sink.bytes() <= telemetry.FILE_BYTES * telemetry.FILES
    assert all(p.stat().st_mode & 0o077 == 0 for p in files)
    assert len(sink.read()) < many and sink.read()[-1]["phase"] == "move"
    assert sink.dispose() == telemetry.FILES
    assert sink.read() == []


def test_a_full_queue_drops_the_event_and_never_waits(spool, monkeypatch):
    import queue
    monkeypatch.setattr(telemetry, "_Q", queue.Queue(maxsize=1))
    monkeypatch.setattr(telemetry, "_started", lambda: None)
    before = telemetry.dropped()
    began = time.monotonic()
    got = [telemetry.record("intervention", phase="move", kind="paused")
           for _ in range(50)]
    assert time.monotonic() - began < 1.0
    assert got[0] and not any(got[1:])
    assert telemetry.dropped() - before == 49


def test_a_sink_other_than_the_local_one_sends_nothing():
    with pytest.raises(TypeError):
        telemetry.Sink().put(["{}"])
    assert [k for k in dir(telemetry) if "send" in k.lower()
            or "upload" in k.lower() or "http" in k.lower()] == []


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
