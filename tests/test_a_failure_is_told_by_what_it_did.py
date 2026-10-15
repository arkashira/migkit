"""The failure catalogue (backlog P0-RESILIENCE-ALL): each class told by
its engine code and its effect, its recovery chosen by `decide.recovery`,
carried out by `faults.retried`, and injected through `failpoint`."""
import threading

import pytest

from migkit import decide, failpoint, faults, util
from migkit.attempts import Ledger


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    faults.reset()
    failpoint.reset()
    monkeypatch.delenv("MIGKIT_FAILPOINT", raising=False)
    yield
    faults.reset()
    failpoint.reset()


def _inject(monkeypatch, spec):
    failpoint.reset()
    monkeypatch.setenv("MIGKIT_FAILPOINT", spec)


def _run(fn, **kw):
    kw.setdefault("backoff", lambda i: 0.0)
    kw.setdefault("wait", lambda s: None)
    return faults.retried(fn, **kw)


def test_every_class_names_a_test_here_or_is_unproved():
    import pathlib
    here = pathlib.Path(__file__).read_text()
    for f in faults.CATALOGUE.values():
        assert f.admits and set(f.admits) <= set(faults.ACTIONS), f.name
        assert f.proof, f.name
        if not f.proof.startswith("UNPROVED"):
            assert f"def {f.proof.split('::')[1]}(" in here, f.name


def test_a_dropped_connection_is_retried_and_then_proceeds(monkeypatch):
    _inject(monkeypatch, "fault.try:1:fault=connectivity,"
                         "fault.try:2:fault=connectivity")
    calls = []
    assert _run(lambda: calls.append(1) or "rows", tries=4) == "rows"
    assert len(calls) == 1


def test_a_refused_account_stops_at_once(monkeypatch):
    _inject(monkeypatch, "fault.try:1+:fault=auth")
    calls = []
    with pytest.raises(faults.Injected) as got:
        _run(lambda: calls.append(1), tries=4)
    assert got.value.recovery == "stop" and calls == []
    assert failpoint._counts["fault.try"] == 1


def test_a_missing_privilege_replans_and_never_retries(monkeypatch):
    _inject(monkeypatch, "fault.try:1+:fault=permission")
    with pytest.raises(faults.Injected) as got:
        _run(lambda: None, tries=4)
    assert got.value.recovery == "replan"
    assert failpoint._counts["fault.try"] == 1


def test_an_expired_credential_waits_only_where_it_renews(monkeypatch):
    _inject(monkeypatch, "fault.try:1:fault=expired_credential")
    renewed = []
    assert _run(lambda: "ok", renew=lambda: renewed.append(1)) == "ok"
    assert renewed == [1]
    _inject(monkeypatch, "fault.try:1:fault=expired_credential")
    with pytest.raises(faults.Injected) as got:
        _run(lambda: "ok")
    assert got.value.recovery == "stop"


def test_throttling_backs_off_longer_than_a_drop(monkeypatch):
    waits = []
    _inject(monkeypatch, "fault.try:1:fault=quota")
    _run(lambda: 1, backoff=lambda i: 1.0, wait=waits.append)
    _inject(monkeypatch, "fault.try:1:fault=connectivity")
    _run(lambda: 1, backoff=lambda i: 1.0, wait=waits.append)
    assert waits == [2.0, 1.0]


def test_pressure_read_as_a_fact_throttles():
    got = dict((f.name, c.path) for f, c in faults.assess(
        {"source_cpu_busy": True, "workload_slowed": True}))
    assert got == {"cpu_pressure": "throttle", "external_impact": "throttle"}
    assert faults.assess({"source_cpu_busy": False}) == []
    told = faults.classify(MemoryError())
    assert told.fault.name == "memory_pressure"


def test_a_full_disk_stops_and_is_never_retried_blind(monkeypatch):
    _inject(monkeypatch, "fault.try:1+:fault=disk_pressure")
    with pytest.raises(faults.Injected) as got:
        _run(lambda: None)
    assert got.value.recovery == "stop"
    assert [c.path for _, c in faults.assess(
        {"free_bytes": 10, "bytes_needed": 100})] == ["stop"]
    assert faults.assess({"free_bytes": 100, "bytes_needed": 10}) == []


def test_a_deadlock_victim_is_rerun_only_where_it_was_whole(monkeypatch):
    _inject(monkeypatch, "fault.try:1:fault=lock")
    assert _run(lambda: "ok", writes=True, atomic=True) == "ok"
    _inject(monkeypatch, "fault.try:1:fault=lock")
    with pytest.raises(faults.Injected) as got:
        _run(lambda: "ok", writes=True)
    assert got.value.fault == "uncertain_commit"
    assert got.value.recovery == "stop"


def test_unknown_outcome_is_reconciled_before_repeat(monkeypatch):
    sent = []

    def write():
        sent.append(1)
        if len(sent) == 1:
            raise Exception(2013, "Lost connection to server during query")
        return "applied"
    asked = []
    assert _run(write, writes=True,
                check=lambda: asked.append(1) or "nothing") == "applied"
    assert asked == [1] and len(sent) == 2
    sent.clear()
    asked.clear()
    got = _run(write, writes=True,
               check=lambda: asked.append(1) or ("committed", "kept"))
    assert got == "kept" and len(sent) == 1
    states = iter(["running", "running", "committed"])
    sent.clear()
    assert _run(write, writes=True, tries=5,
                check=lambda: next(states)) is None
    assert len(sent) == 1


def test_a_duplicate_isolates_the_unit(monkeypatch):
    for name in ("duplicate", "malformed_data"):
        _inject(monkeypatch, f"fault.try:1+:fault={name}")
        with pytest.raises(faults.Injected) as got:
            _run(lambda: None)
        assert got.value.recovery == "isolate"


def test_a_restart_during_cleanup_reconciles_then_resumes(tmp_path,
                                                         monkeypatch):
    path = tmp_path / "attempts.json"
    owned = faults.Owned("target", "cleanup", "t1", "0")
    _inject(monkeypatch, "fault.try:1:fault=interruption")
    first = Ledger(path)
    with pytest.raises(faults.Injected):
        _run(lambda: None, writes=True, ledger=first, owned=owned)
    again = Ledger(path)
    assert owned.key in again.uncertain()
    with pytest.raises(RuntimeError, match="cannot be read"):
        _run(lambda: "done", writes=True, ledger=again, owned=owned)
    failpoint.reset()
    monkeypatch.delenv("MIGKIT_FAILPOINT")
    done = []
    assert _run(lambda: done.append(1) or "done", writes=True,
                ledger=Ledger(path), owned=owned,
                check=lambda: "nothing") == "done"
    assert done == [1]
    assert Ledger(path).owned(owned.key)["state"] == "done"
    assert Ledger(path).owned(owned.key)["tries"] >= 2


def test_a_failover_replans_and_never_retries_the_reader(monkeypatch):
    _inject(monkeypatch, "fault.try:1+:fault=failover")
    with pytest.raises(faults.Injected) as got:
        _run(lambda: None, writes=True)
    assert got.value.recovery == "replan"
    assert failpoint._counts["fault.try"] == 1


def test_lost_history_replans():
    told = faults.classify(Exception(1236, "binary log purged"))
    assert told.fault.name == "lost_history"
    assert dict((f.name, c.path) for f, c in faults.assess(
        {"checkpoint_behind_retention": True})) == {"stale_checkpoint":
                                                    "replan"}


def test_a_read_retries_a_broken_snapshot(monkeypatch):
    _inject(monkeypatch, "fault.try:1:fault=inconsistent_view")
    assert _run(lambda: "rows") == "rows"


def test_a_schema_change_replans_the_unit(monkeypatch):
    _inject(monkeypatch, "fault.try:1+:fault=schema_change")
    with pytest.raises(faults.Injected) as got:
        _run(lambda: None)
    assert got.value.recovery == "replan"


def test_a_lossy_value_stops_and_is_never_set_aside(monkeypatch):
    _inject(monkeypatch, "fault.try:1+:fault=lossy_conversion")
    with pytest.raises(faults.Injected) as got:
        _run(lambda: None)
    assert got.value.recovery == "stop"


def test_corrupted_evidence_is_probed_never_trusted(monkeypatch):
    _inject(monkeypatch, "fault.try:1+:fault=corrupted_evidence")
    with pytest.raises(faults.Injected) as got:
        _run(lambda: None)
    assert got.value.recovery == "probe"


def test_a_missing_program_replans():
    told = faults.classify(FileNotFoundError(2, "no such file", "prog"))
    assert told.fault.name == "dependency"
    got = decide.recovery({"class": "dependency",
                           "admits": told.fault.admits, "tries_left": 3})
    assert got.path == "replan"


def test_an_unknown_failure_stops_conservatively(monkeypatch):
    _inject(monkeypatch, "fault.try:1+:fault=unknown")
    with pytest.raises(faults.Injected) as got:
        _run(lambda: None)
    assert got.value.recovery == "stop"
    assert failpoint._counts["fault.try"] == 1


def test_a_retry_storm_on_one_side_is_bounded(monkeypatch):
    calls, lock = [], threading.Lock()

    def down():
        with lock:
            calls.append(1)
        raise Exception(2003, "Can't connect to server")
    workers, tries = 8, 4

    def one():
        with pytest.raises(Exception):
            _run(down, tries=tries, side="source",
                 backoff=lambda i: 60.0, wait=lambda s: None)
    ts = [threading.Thread(target=one) for _ in range(workers)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert len(calls) < workers * tries
    assert faults.BREAKER.sides["source"]["opens"] >= 1


def test_a_failing_log_never_changes_what_is_done(monkeypatch):
    def broken(text):
        raise OSError("log disk gone")
    _inject(monkeypatch, "fault.try:1:fault=connectivity")
    assert _run(lambda: "ok", log=broken) == "ok"


def test_with_retry_and_run_go_through_the_one_recovery(monkeypatch):
    monkeypatch.setattr(util, "_backoff", lambda attempt, base=0.8: 0.0)
    _inject(monkeypatch, "fault.try:1:fault=connectivity")
    assert util.with_retry(lambda: "ok") == "ok"
    assert failpoint._counts["fault.try"] == 2
    with pytest.raises(Exception) as got:
        util.with_retry(lambda: (_ for _ in ()).throw(
            Exception(1045, "Access denied")))
    assert got.value.fault == "auth"


def test_recovery_never_repeats_an_unknown_write_without_a_read():
    got = decide.recovery({"class": "uncertain_commit",
                           "admits": ("reconcile", "stop"),
                           "uncertain": True, "can_check": False,
                           "tries_left": 3, "transient": False})
    assert got.path == "stop"
    got = decide.recovery({"class": "connectivity",
                           "admits": ("retry", "pause", "stop"),
                           "uncertain": True, "reconciled": "nothing",
                           "can_check": True, "tries_left": 3,
                           "transient": True})
    assert got.path == "retry"
    got = decide.recovery({"class": "connectivity",
                           "admits": ("retry", "pause", "stop"),
                           "transient": True, "tries_left": 0})
    assert got.path == "stop"
