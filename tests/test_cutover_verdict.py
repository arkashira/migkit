"""The cutover verdict logic: a source that looks caught up but is still being
written must NOT be greenlit - name the hot tables; a source that has gone
quiet and whose target has caught up is safe to flip after one consistent
check. Pure function, no infra needed."""
from migkit.cli import _cutover_verdict


def test_hot_source_is_named_not_greenlit():
    prev = {"src_rows": 100, "dst_rows": 100, "src_tables": {"public.orders": 100}}
    cur = {"src_rows": 112, "dst_rows": 100, "src_tables": {"public.orders": 112}}
    label, _, detail = _cutover_verdict(prev, cur, 0)
    assert label == "NOT SAFE"
    assert "public.orders" in detail and "+12" in detail


def test_safe_to_cut_over_when_stable_and_caught_up():
    s = {"src_rows": 100, "dst_rows": 100, "src_tables": {"public.orders": 100}}
    label, _, _ = _cutover_verdict(dict(s), dict(s), 2)
    assert label == "SAFE TO CUT OVER"


def test_converging_before_stable_enough():
    s = {"src_rows": 100, "dst_rows": 100, "src_tables": {"public.orders": 100}}
    label, _, _ = _cutover_verdict(dict(s), dict(s), 1)
    assert label == "converging"


def test_no_verdict_while_still_loading():
    prev = {"src_rows": 100, "dst_rows": 40, "src_tables": {}}
    cur = {"src_rows": 100, "dst_rows": 60, "src_tables": {}}
    assert _cutover_verdict(prev, cur, 0) is None
