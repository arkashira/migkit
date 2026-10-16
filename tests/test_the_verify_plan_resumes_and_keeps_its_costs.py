"""The verify plan resumes and keeps its costs (P0-VERIFY-EXECUTION).

A keyed table cut into key ranges for row text keeps one unit a range in
the engine's ledger and each range proved on the disk (`scanplan.RangeTexts`):
a check stopped after two of four ranges reads, when run again, only the two
it owes - never the whole table at once - and still names the first key the
sides hold apart. A table whose every range is proved is forgotten, so the
next check reads it all again. The ways of proving a table's rows alike are
ranked by one cost model for the run (`GenericEngine._parity_costs`): the
second table of a shape starts from what the first measured, without a
trial of its own.
"""
import contextlib

import pytest

from migkit import attempts, decide, scanplan
from migkit.engines.base import Result
from migkit.engines.generic import GenericEngine


class _Eng:
    """An engine's ledger and nothing else: `Engine.work_unit` as the
    engine opens and closes a unit."""

    def __init__(self, path):
        self.ledger = attempts.Ledger(path)

    def work_unit(self, kind, name, level, parent, parts):
        @contextlib.contextmanager
        def held():
            self.ledger.unit_opened(name, kind, level, parent, parts)
            ok, why = False, ""
            try:
                yield
                ok = True
            except BaseException as e:
                why = type(e).__name__
                raise
            finally:
                self.ledger.unit_closed(name, ok, why)
        return held()


ROWS = 2000


def _side(changed=()):
    return {i: (i, f"v{i}" + ("!" if i in changed else ""))
            for i in range(ROWS)}


def _reader(data, preds, reads, stop_after=None):
    edges = [(i * ROWS // 4, (i + 1) * ROWS // 4) for i in range(4)]
    of = dict(zip(preds, edges))

    def rows(side, pred):
        reads.append((side, pred))
        if stop_after is not None and len({p for _, p in reads}) \
                > stop_after:
            raise KeyboardInterrupt("stopped")
        lo, hi = of[pred]
        return [data[side][k] for k in range(lo, hi)]
    return rows


def _preds():
    return [f"id >= {i * ROWS // 4} and id < {(i + 1) * ROWS // 4}"
            for i in range(4)]


def test_a_stopped_row_text_check_replays_only_the_ranges_it_owes(tmp_path):
    preds = _preds()
    led = tmp_path / "attempts.json"
    kept_at = tmp_path / "row-text.json"
    data = {"src": _side(), "dst": _side()}
    reads = []
    with pytest.raises(KeyboardInterrupt):
        scanplan.row_text(_reader(data, preds, reads, stop_after=2), preds,
                          eng=_Eng(led), table="app.t", scope="s",
                          kept=scanplan.RangeTexts(kept_at))
    first = attempts.Ledger(led).records
    assert sorted(r["state"] for r in first.values()) == \
        ["done", "done", "failed"]
    # one row changed on the target in a range not proved yet
    data["dst"] = _side(changed={1777})
    reads.clear()
    eng = _Eng(led)
    got = scanplan.row_text(_reader(data, preds, reads), preds, eng=eng,
                            table="app.t", scope="s",
                            kept=scanplan.RangeTexts(kept_at))
    read = sorted({p for _, p in reads})
    assert read == sorted(preds[2:]), read
    assert "" not in read
    recs = eng.ledger.records
    assert {n: r["state"] for n, r in recs.items()} == {
        f"app.t {i}/4": "done" for i in range(1, 5)}
    assert [recs[f"app.t {i}/4"]["opens"] for i in range(1, 5)] == \
        [1, 1, 2, 1]
    assert all(recs[f"app.t {i}/4"]["parent"] == "app.t" for i in range(1, 5))
    assert got.rows == {"src": ROWS, "dst": ROWS}
    assert got.first[0] == preds[3] and got.first[1] == ("1777",)
    line = scanplan.row_text_line("app.t", got)
    assert "DIFF" in line and "first key=1777" in line
    # the two ranges proved before the stop stand on that earlier read,
    # and the result says so, with when it was
    assert got.earlier == 2 and got.earlier_at
    assert "(2 of 4 ranges from the check stopped at " in line
    assert line.rstrip().endswith("UTC)")
    # every range proved: the next check reads the whole table again
    assert scanplan.RangeTexts(kept_at).of("app.t", "s") == {}


def test_kept_ranges_fold_to_what_one_uninterrupted_read_gives(tmp_path):
    preds = _preds()
    data = {"src": _side(), "dst": _side()}
    whole = scanplan.row_text(_reader(data, preds, []), preds)
    kept_at = tmp_path / "row-text.json"
    with pytest.raises(KeyboardInterrupt):
        scanplan.row_text(_reader(data, preds, [], stop_after=3), preds,
                          eng=_Eng(None), table="app.t", scope="s",
                          kept=scanplan.RangeTexts(kept_at))
    again = scanplan.row_text(_reader(data, preds, []), preds,
                              eng=_Eng(None), table="app.t", scope="s",
                              kept=scanplan.RangeTexts(kept_at))
    assert again.alike and whole.alike
    assert again.digests == whole.digests
    assert scanplan.fold_digests(again, "src") == \
        scanplan.fold_digests(whole, "src")


def test_ranges_kept_under_another_scope_are_read_again(tmp_path):
    preds = _preds()
    data = {"src": _side(), "dst": _side()}
    kept_at = tmp_path / "row-text.json"
    with pytest.raises(KeyboardInterrupt):
        scanplan.row_text(_reader(data, preds, [], stop_after=2), preds,
                          eng=_Eng(None), table="app.t", scope="masked",
                          kept=scanplan.RangeTexts(kept_at))
    reads = []
    scanplan.row_text(_reader(data, preds, reads), preds, eng=_Eng(None),
                      table="app.t", scope="as stored",
                      kept=scanplan.RangeTexts(kept_at))
    assert sorted({p for _, p in reads}) == sorted(preds)


def _generic(tmp_path, monkeypatch):
    eng = GenericEngine.__new__(GenericEngine)
    eng.hop = None

    class _Way:
        path, reason = "row-digests", "rows read here"
    facts = {"rows": 10000, "bytes": 1e6, "held": 1, "memory": 1 << 30}
    monkeypatch.setattr(eng, "compare_way", lambda t: (_Way(), dict(facts)),
                        raising=False)
    monkeypatch.setattr(eng, "_masks", lambda t: False, raising=False)
    monkeypatch.setattr(eng, "_fits", lambda f: True, raising=False)
    monkeypatch.setattr(eng, "_digest_result",
                        lambda db, scope, t, f, c: Result("data", scope,
                                                          "ok", "alike"),
                        raising=False)
    return eng


def test_the_next_table_of_a_shape_starts_from_the_measured_cost(
        tmp_path, monkeypatch):
    eng = _generic(tmp_path, monkeypatch)
    chosen = []
    real = decide.row_parity

    def seen(*a, **k):
        got = real(*a, **k)
        chosen.append(got[0])
        return got
    monkeypatch.setattr(decide, "row_parity", seen)
    one = eng._row_parity("app", "app.a", "a", None)
    costs = eng._parity_costs()
    assert one.status == "ok"
    assert costs.measured("row text")
    two = eng._row_parity("app", "app.b", "b", None)
    assert two.status == "ok"
    assert eng._parity_costs() is costs
    first, second = chosen
    assert second.rung.name == "row text"
    # the second table opened no trial of its own: its way was ranked by
    # what the first measured
    assert second.trial == 0
    est = dict(second.costs).get("row text")
    assert est is not None and getattr(est, "n", 0) >= 1
    assert costs.fit("row text").n == 2


def test_a_check_read_whole_says_no_range_from_an_earlier_one(tmp_path):
    preds = _preds()
    data = {"src": _side(), "dst": _side()}
    got = scanplan.row_text(_reader(data, preds, []), preds,
                            eng=_Eng(tmp_path / "attempts.json"),
                            table="app.t", scope="s",
                            kept=scanplan.RangeTexts(tmp_path / "rt.json"))
    assert got.earlier == 0 and got.earlier_said() == ""
    assert "earlier" not in scanplan.row_text_line("app.t", got)
