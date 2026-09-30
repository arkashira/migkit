from types import SimpleNamespace

import pytest

from migkit import cutover


class _Eng:
    def __init__(self, results):
        self.results = results

    def check_data(self, d):
        return self.results


def _run(results):
    hop = SimpleNamespace(options={}, report_dir=lambda d: None)
    said = []
    cutover._final_check(hop, _Eng(results), "d", None, lambda: 5.0,
                         said.append)
    return said


def _r(status, scope="t"):
    return SimpleNamespace(status=status, scope=scope, detail="")


def test_a_check_that_compared_nothing_never_lets_the_flip_happen():
    """Every answer skipped used to read as a clean final check and the flip
    went ahead on nothing compared; the one verdict calls it incomplete."""
    with pytest.raises(cutover._Undo, match="did not find the two the same"):
        _run([_r("skip", "a"), _r("skip", "b")])
    with pytest.raises(cutover._Undo):
        _run([])


def test_a_difference_or_an_answer_to_read_stops_it():
    with pytest.raises(cutover._Undo):
        _run([_r("ok", "a"), _r("diff", "b")])
    with pytest.raises(cutover._Undo):
        _run([_r("ok", "a"), _r("warn", "b")])


def test_compared_the_same_with_a_skip_named_goes_on():
    said = _run([_r("ok", "a"), _r("skip", "b")])
    assert said and "found the two the same" in said[-1]
    assert "not compared: b" in said[-1]
