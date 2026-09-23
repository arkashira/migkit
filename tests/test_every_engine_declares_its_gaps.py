"""Every engine either does a thing or says why it does not.

Owner's rule (2026-09-24): one hop, one command, one result on every
engine. What an engine does is read off the code; what it does not do is
declared in `migkit/capabilities.py`. These tests hold the two together in
both directions, so a gap can neither appear nor close without the
declaration changing with it.
"""
import re
from pathlib import Path

from migkit import capabilities as caps
from migkit.engines import NAMES
from tests.test_the_report_does_not_name_its_tools import TOOLS

BACKLOG = (Path(__file__).parent.parent / "docs" / "backlog.md").read_text()


def test_no_engine_lacks_anything_without_saying_so():
    assert caps.undeclared() == []


def test_no_declared_gap_is_one_the_code_has_closed():
    assert caps.stale() == []


def test_every_engine_and_every_capability_is_in_the_table():
    assert set(caps.GAPS) == set(NAMES)
    for name, gaps in caps.GAPS.items():
        assert set(gaps) <= set(caps.CAPABILITIES), (name, gaps)
    assert set(caps.PROBES) == set(caps.CAPABILITIES)


def test_a_gap_carries_its_reason_or_its_backlog_item():
    for name, gaps in caps.GAPS.items():
        for cap, (state, why) in gaps.items():
            assert state in (caps.NOT_APPLICABLE, caps.NOT_YET), (name, cap)
            if state == caps.NOT_YET:
                assert re.search(rf"^\*\*{re.escape(why)}\. ", BACKLOG,
                                 re.M), (name, cap, why)
            else:
                assert len(why.split()) >= 4, (name, cap, why)


def test_the_words_an_operator_reads_name_no_program():
    said = list(caps.CAPABILITIES.values()) + [
        why for gaps in caps.GAPS.values()
        for state, why in gaps.values() if state == caps.NOT_APPLICABLE]
    for text in said:
        low = text.lower()
        assert not [t for t in TOOLS if t in low], text


def test_removing_a_capability_is_noticed(monkeypatch):
    """The probes have teeth: take PostgreSQL's fence away and the table
    no longer matches the code."""
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.delattr(PostgresEngine, "fence_wait")
    assert ("postgres", "fence") in caps.undeclared()


def test_closing_a_gap_is_noticed(monkeypatch):
    """Give Redis a table copier and its declared gap goes stale."""
    from migkit.engines.redis import RedisEngine
    monkeypatch.setattr(RedisEngine, "move_table",
                        lambda self, *a: None, raising=False)
    assert ("redis", "table-copy") in caps.stale()


def test_the_base_class_placeholder_is_not_a_capability(monkeypatch):
    """`Engine.check_deep` only says there are no deep checks. An engine
    inheriting it has none."""
    from migkit.engines.hetero import HeteroEngine
    from migkit.engines.base import Engine
    assert HeteroEngine.check_deep is Engine.check_deep
    assert not caps.implemented("hetero", "deep")


def test_users_is_read_from_its_dispatch():
    got = caps._users_engines()
    assert {"postgres", "mysql", "mongodb"} <= got, got
    assert "redis" not in got and "kafka" not in got, got
