"""The copiers' writes reconciled, their sizes retuned, their width
throttled, and the ledger of owned work kept through a torn file (backlog
P0-RESILIENCE-ALL, P0-AUTOTUNE-ALL)."""
import json
import threading

import pytest

from migkit import attempts, decide, faults, tuning


class Lost(Exception):
    """A link dropped after the write was sent: MySQL's 2013."""

    def __init__(self):
        super().__init__(2013, "Lost connection to MySQL server during query")


@pytest.fixture(autouse=True)
def _closed():
    faults.reset()
    yield
    faults.reset()


def test_an_uncertain_write_is_reconciled_then_not_repeated(tmp_path):
    ledger = attempts.Ledger(str(tmp_path / "attempts.json"))
    target, writes = [], []

    def write():
        writes.append(1)
        target.extend(range(5))
        raise Lost()

    owned = faults.Owned("dst", "copy batch", "t", "0")
    got = faults.retried(write, writes=True, side="dst", owned=owned,
                         ledger=ledger, wait=lambda s: None,
                         check=faults.landed(lambda: 5, lambda: len(target),
                                             before=0))
    assert got == 5
    assert len(writes) == 1 and len(target) == 5
    assert ledger.owned(owned.key)["state"] == faults.COMMITTED
    # the same work in a later run: reconciled, never written again
    faults.retried(write, writes=True, side="dst", owned=owned,
                   ledger=attempts.Ledger(str(tmp_path / "attempts.json")),
                   wait=lambda s: None,
                   check=faults.landed(lambda: 5, lambda: len(target)))
    assert len(writes) == 1 and len(target) == 5


def test_an_uncertain_write_that_is_not_there_goes_again_once():
    target, writes = [], []

    def write():
        writes.append(1)
        if len(writes) == 1:
            raise Lost()
        target.extend(range(5))
        return "written"

    got = faults.retried(write, writes=True, side="dst", wait=lambda s: None,
                         check=faults.landed(lambda: 5, lambda: len(target),
                                             before=0))
    assert got == "written"
    assert len(writes) == 2 and len(target) == 5


def test_a_partial_uncertain_write_is_stopped_never_run_on_a_guess():
    target = []

    def write():
        target.extend(range(2))
        raise Lost()

    with pytest.raises(Lost):
        faults.retried(write, writes=True, side="dst", wait=lambda s: None,
                       check=faults.landed(lambda: 5, lambda: len(target),
                                           before=0))
    assert len(target) == 2


def test_a_packet_too_large_is_retuned_once_and_completes():
    retune = tuning.Retune("db.t", rows=1000, nbytes=8 * 2 ** 20)
    seen = []

    def write():
        if seen[-1]["bytes"] >= 8 * 2 ** 20:
            raise Exception(1153, "Got a packet bigger than"
                                  " 'max_allowed_packet' bytes")
        return "done"

    got = tuning.sized(retune, "range 1", write, seen.append,
                       sent=lambda: 8 * 2 ** 20, rows=lambda: 1000)
    assert got == "done"
    assert len(seen) == 2
    assert seen[1]["bytes"] < seen[0]["bytes"]
    assert [a.cause.kind for a in retune.history] == [tuning.PACKET]


def test_a_failure_no_size_answers_is_not_retuned():
    retune = tuning.Retune("db.t", rows=1000)
    tried = []

    def write():
        tried.append(1)
        raise Exception(1045, "Access denied for user")

    with pytest.raises(Exception):
        tuning.sized(retune, "range 1", write, lambda s: None)
    assert len(tried) == 1 and retune.history == []


def test_a_statement_timeout_is_a_sizing_cause():
    cause = tuning.cause_of(Exception(3024, "Query execution was"
                                            " interrupted, maximum statement"
                                            " execution time exceeded"))
    assert cause.kind == tuning.TIMEOUT and cause.sizing
    assert tuning.cause_of(Exception("lock wait timeout exceeded")).kind \
        == tuning.LOCK
    assert tuning.cause_of(MemoryError()).kind == tuning.MEMORY
    assert not tuning.cause_of(Exception("syntax error")).sizing
    assert not hasattr(tuning, "SIGNATURES")


class Pace:
    def __init__(self, limit, most):
        self.limit, self.most = limit, most
        self._lock = threading.Lock()
        self.history = []


def test_a_throttle_lowers_and_restores_the_width_without_flip_flop():
    pressed = {"on": True}
    pace = Pace(8, 8)
    t = faults.Throttle(pace, lambda: {"target_cpu_busy":
                                       "busy" if pressed["on"] else ""})
    t.between()
    assert pace.limit == 4
    pressed["on"] = False
    widths = []
    for _ in range(faults.Throttle.STEADY - 1):
        t.between()
        widths.append(pace.limit)
    assert widths == [4] * (faults.Throttle.STEADY - 1)
    t.between()
    assert pace.limit == 6
    # pressure right after a growth: lowered again, and the wait doubles
    pressed["on"] = True
    t.between()
    assert pace.limit == 3
    pressed["on"] = False
    for _ in range(faults.Throttle.STEADY * 2 - 1):
        t.between()
    assert pace.limit == 3
    t.between()
    assert pace.limit > 3
    for _ in range(40):
        t.between()
    assert pace.limit == 8


def test_a_clear_reading_never_widens_past_where_it_was():
    pace = Pace(2, 8)
    t = faults.Throttle(pace, lambda: {})
    for _ in range(10):
        t.between()
    assert pace.limit == 2


def test_a_torn_ledger_is_read_from_its_snapshot(tmp_path):
    path = tmp_path / "attempts.json"
    ledger = attempts.Ledger(str(path))
    ledger.own("op:1", state="began", tries=1)
    assert json.loads((tmp_path / "attempts.json.bak").read_text())[
        "operations"]["op:1"]["state"] == "began"
    path.write_text(path.read_text()[:17])
    back = attempts.Ledger(str(path))
    assert back.read_from == f"{path}.bak"
    assert back.owned("op:1")["state"] == "began"
    assert "op:1" in back.uncertain()


def test_a_ledger_torn_with_its_snapshot_stops(tmp_path):
    from migkit.durable import Torn
    path = tmp_path / "attempts.json"
    attempts.Ledger(str(path)).own("op:1", state="began")
    path.write_text("{")
    (tmp_path / "attempts.json.bak").write_text("{")
    with pytest.raises(Torn):
        attempts.Ledger(str(path))


def test_nothing_answers_a_one_row_packet_on_a_target_without_pieces():
    retune = tuning.Retune("db.t", rows=1, nbytes=1024)

    def write():
        raise Exception(1153, "Got a packet bigger than"
                              " 'max_allowed_packet' bytes")

    with pytest.raises(decide.Blocked):
        tuning.sized(retune, "range 1", write, lambda s: None,
                     sent=lambda: 1024, rows=lambda: 1)
