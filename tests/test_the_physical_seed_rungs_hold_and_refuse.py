"""The two rungs a physical seed is taken by - the copy of PostgreSQL's
pages and MySQL's clone - each held where every need holds and refused
with its own reason where one does not, on fakes (backlog
AUD-Z.rungs-outside-owed): no pages across major versions, and no clone
from a server older than the clone itself."""
import pytest

from migkit import physical
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why
from tests.test_a_physical_seed_is_taken_only_where_it_holds import (
    MY_OPTS, PG_OPTS, _hop, _my_facts, _pg_facts)

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ('physical', '_rungs', 'physical-clone', 'holds'),
    ('physical', '_rungs', 'physical-clone', 'refuses'),
    ('physical', '_rungs', 'physical-pages', 'holds'),
    ('physical', '_rungs', 'physical-pages', 'refuses'),
]


@pytest.fixture(autouse=True)
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _rung(name):
    return next(r for r in physical.rungs() if r.name == name)


def test_the_pages_are_held_on_one_major_version_and_refused_across_two():
    hop = _hop(physical_opts=PG_OPTS)
    pages = _rung("physical-pages")
    with watched() as seen:
        assert pages.refused(_pg_facts(hop)) is None
        got = pages.refused(_pg_facts(hop, src_version=150007))
    assert branch(seen, "physical-pages") == [HOLDS, REFUSES]
    assert got == why(seen, "physical-pages")
    assert "pages are read only by the major version that wrote them" in got


def test_the_clone_is_held_from_mysql_8_0_17_and_refused_before_it():
    hop = _hop("mysql", MY_OPTS)
    clone = _rung("physical-clone")
    old = _my_facts(hop)
    old["src"] = dict(old["src"], version="8.0.16")
    with watched() as seen:
        assert clone.refused(_my_facts(hop)) is None
        got = clone.refused(old)
    assert branch(seen, "physical-clone") == [HOLDS, REFUSES]
    assert got == why(seen, "physical-clone")
    assert "only from MySQL 8.0.17" in got
