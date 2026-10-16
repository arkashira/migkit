"""The rungs a move's own ways are chosen by - the advice, how a batch is
applied, removed, converted and written, how a table's keys are kept
apart, which mover carries a table, how a cutover drains and arms the
stream back, and the bulk way a database falls from - each reached under
`watched()` on both its branches on fakes (backlog
AUD-Z.rungs-outside-owed): a rung whose needs hold is held, one whose
need does not is refused with that need's own words, so no way is taken
on facts it cannot stand on."""
import pytest

from migkit import (advisors, applyway, archive, charsets, cli, consolidate,
                    crossmove, cutover, decide)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("advisors", "pick", "named", "holds"),
    ("advisors", "pick", "named", "refuses"),
    ("advisors", "pick", "native", "holds"),
    ("applyway", "WAYS", "rows", "holds"),
    ("applyway", "WAYS", "columns", "holds"),
    ("applyway", "WAYS", "columns", "refuses"),
    ("archive", "_remove_ways", "batch", "holds"),
    ("archive", "_remove_ways", "rows", "holds"),
    ("charsets", "_convert_ways", "columns", "holds"),
    ("charsets", "_convert_ways", "table", "holds"),
    ("cli", "_bulk_or_copier", "<v>", "holds"),
    ("cli", "_bulk_or_copier", "builtin", "holds"),
    ("consolidate", "key_ways", "tenant", "holds"),
    ("consolidate", "key_ways", "tenant", "refuses"),
    ("consolidate", "key_ways", "offset", "holds"),
    ("consolidate", "key_ways", "offset", "refuses"),
    ("consolidate", "key_ways", "prefix", "holds"),
    ("consolidate", "key_ways", "prefix", "refuses"),
    ("consolidate", "key_ways", "apart", "holds"),
    ("consolidate", "key_ways", "apart", "refuses"),
    ("consolidate", "write_ways", "bulk", "holds"),
    ("consolidate", "write_ways", "plain", "holds"),
    ("crossmove", "ways", "<RENDERED>", "holds"),
    ("crossmove", "ways", "<RENDERED>", "refuses"),
    ("crossmove", "ways", "<COMPILED>", "holds"),
    ("crossmove", "ways", "<COMPILED>", "refuses"),
    ("crossmove", "ways", "<COPIER>", "holds"),
    ("cutover", "_drain_rungs", "position", "holds"),
    ("cutover", "_drain_rungs", "position", "refuses"),
    ("cutover", "_drain_rungs", "mark", "holds"),
    ("cutover", "_drain_rungs", "mark", "refuses"),
    ("cutover", "_back_rungs", "armed", "holds"),
    ("cutover", "_back_rungs", "armed", "refuses"),
    ("cutover", "_back_rungs", "at_flip", "holds"),
]


class _Hop:
    def __init__(self, service=None, name="app"):
        self.service, self.name = service, name


def test_the_named_playbook_is_held_only_where_one_is_kept():
    known = next(iter(advisors.PLAYBOOKS))
    with watched() as seen:
        assert advisors.pick(_Hop(known))[0] == known
        got, reason = advisors.pick(_Hop("no-such-service"))
        none, _ = advisors.pick(_Hop(None))
    assert got == "native" and none == "native"
    assert branch(seen, "named") == [HOLDS, REFUSES]
    assert why(seen, "named") == "the hop names no service"
    assert "no playbook for service 'no-such-service'" in reason
    assert branch(seen, "native") == [HOLDS]


def test_a_batch_is_collapsed_as_columns_only_where_the_rows_fall_behind():
    with watched() as seen:
        kept, _ = decide.prune(applyway.WAYS, {"behind": (True, "")})
        assert [r.name for r in kept] == ["rows", "columns"]
        kept, out = decide.prune(applyway.WAYS, {
            "behind": (False, "3 changes a second, applied at 40")})
    assert [r.name for r in kept] == ["rows"]
    assert out[0][0].name == "columns"
    assert branch(seen, "columns") == [HOLDS, REFUSES]
    assert why(seen, "columns") == \
        "the batches keep up: 3 changes a second, applied at 40"
    assert branch(seen, "rows") == [HOLDS]


@pytest.mark.parametrize("ladder,first,then,gives", [
    (archive._remove_ways, "batch", "rows", "removed"),
    (charsets._convert_ways, "columns", "table", "converted"),
    (consolidate.write_ways, "bulk", "plain", "written"),
])
def test_a_way_that_fails_as_it_runs_falls_to_the_next(ladder, first, then,
                                                       gives):
    tried = []

    def run(choice):
        tried.append(choice.rung.name)
        if choice.rung.name == first:
            raise TimeoutError("lock wait timeout exceeded")
        return choice.rung.name
    with watched() as seen:
        choice, got = decide.attempt(f"the {gives} batch", ladder(), run,
                                     need=(gives,), log=lambda m: None)
    assert tried == [first, then] and got == then
    # held first; once it failed as it ran, given up for the work
    assert branch(seen, first)[0] == HOLDS and branch(seen, then) == [HOLDS]
    # a way that does not give what is asked is out by capability, not by
    # a need of its own
    with watched() as seen:
        kept, out = decide.prune(ladder(), {}, need=("nothing it gives",))
    assert not kept and len(out) == 2
    assert REFUSES not in branch(seen, first)


#: (rule, the table's facts, the way that holds, a refused way and its
#: reason)
KEYS = [
    ("tenant", {}, "tenant", "offset", "the hop's rule shifts no key"),
    ("offset", {"integer": True}, "offset", "tenant",
     "the hop's rule is not tenant"),
    ("remap", {"integer": False}, None, "offset",
     "only a whole number is shifted"),
    ("prefix", {"text": True, "separated": True}, "prefix", "apart",
     "this table's cannot be changed so"),
    ("prefix", {"text": True, "separated": False}, None, "prefix",
     "so two prefixed keys could be one"),
    ("prefix", {"text": False, "separated": True}, None, "prefix",
     "only text is led by a value"),
    ("refuse", {"clash": False}, "apart", "prefix",
     "the hop's rule leads no key with its source"),
    ("refuse", {"clash": True}, None, "apart",
     "sources hold the same keys in it"),
]


@pytest.mark.parametrize("rule,table,held,refused,said", KEYS)
def test_a_tables_keys_are_kept_apart_by_the_way_its_rule_and_key_allow(
        rule, table, held, refused, said):
    facts = dict({"integer": False, "text": False, "separated": True,
                  "clash": False}, rule=rule, **table)
    with watched() as seen:
        kept, out = decide.prune(consolidate.key_ways(), facts)
    assert [r.name for r in kept] == ([held] if held else [])
    assert REFUSES in branch(seen, refused)
    assert said in why(seen, refused), why(seen, refused)
    if held:
        assert branch(seen, held) == [HOLDS]


def _move(**change):
    base = {"held": False, "masked": False, "pair": ("mysql", "postgres"),
            "unproved": [], "rendered": None, "compiled": None,
            "loads": True}
    base.update(change)
    return base


def test_a_table_goes_by_the_fastest_mover_its_pair_and_columns_allow(
        monkeypatch):
    monkeypatch.setattr(crossmove, "missing_extensions", lambda pair: [])
    names = {r.name for r in crossmove.ways()}
    assert names == {crossmove.RENDERED, crossmove.COMPILED,
                     crossmove.COPIER}
    with watched() as seen:
        kept, _ = decide.prune(crossmove.ways(), _move())
    assert [r.name for r in kept] == [crossmove.RENDERED, crossmove.COMPILED,
                                      crossmove.COPIER]
    with watched() as later:
        kept, out = decide.prune(crossmove.ways(), _move(
            pair=("postgres", "mysql"), loads=False))
    assert [r.name for r in kept] == [crossmove.COPIER]
    assert branch(seen + later, crossmove.RENDERED) == [HOLDS, REFUSES]
    assert "local_infile" in why(later, crossmove.RENDERED)
    assert branch(seen + later, crossmove.COMPILED) == [HOLDS, REFUSES]
    assert "does not join these two engines" in why(later,
                                                    crossmove.COMPILED)
    assert branch(seen + later, crossmove.COPIER) == [HOLDS]


@pytest.mark.parametrize("change,said", [
    ({"held": True}, "writes around the number"),
    ({"masked": True}, "the hop masks columns of this table"),
    ({"unproved": ["geom"]}, "geom is not compared on both sides"),
    ({"rendered": "a bit(12) column", "compiled": "a bit(12) column"},
     "a bit(12) column"),
])
def test_a_table_the_fast_movers_cannot_hold_whole_goes_by_the_copier(
        monkeypatch, change, said):
    monkeypatch.setattr(crossmove, "missing_extensions", lambda pair: [])
    with watched() as seen:
        kept, _ = decide.prune(crossmove.ways(), _move(**change))
    assert [r.name for r in kept] == [crossmove.COPIER]
    for name in (crossmove.RENDERED, crossmove.COMPILED):
        assert branch(seen, name) == [REFUSES]
        assert said in why(seen, name), why(seen, name)


def test_a_compiled_mover_not_installed_is_refused(monkeypatch):
    monkeypatch.setattr(crossmove, "missing_extensions",
                        lambda pair: ["mysql_fdw"])
    with watched() as seen:
        decide.prune(crossmove.ways(), _move())
    assert branch(seen, crossmove.COMPILED) == [REFUSES]
    assert "parts for this pair are not installed" in why(
        seen, crossmove.COMPILED)


DRAIN = {"positions": True, "why none": "", "cutover": True,
         "operator": False, "mark": ""}


@pytest.mark.parametrize("change,refused,said", [
    ({"positions": False, "why none": "the stream keeps no position"},
     "position", "the stream keeps no position"),
    ({"cutover": False}, "mark", "without cutover"),
    ({"operator": True}, "mark", "with freeze: operator"),
    ({"mark": "a mark is not written on this pair"}, "mark",
     "a mark is not written on this pair"),
])
def test_a_cutover_drains_by_the_position_or_the_mark_its_stream_allows(
        change, refused, said):
    with watched() as seen:
        kept, _ = decide.prune(cutover._drain_rungs(), DRAIN)
        assert [r.name for r in kept] == ["position", "mark"]
        kept, _ = decide.prune(cutover._drain_rungs(), dict(DRAIN, **change))
    assert refused not in [r.name for r in kept]
    assert branch(seen, refused) == [HOLDS, REFUSES]
    assert said in why(seen, refused)


def test_the_stream_back_is_armed_only_where_its_engine_lets_it_wait():
    with watched() as seen:
        kept, _ = decide.prune(cutover._back_rungs(), {"arms": True})
        assert [r.name for r in kept] == ["armed", "at_flip"]
        kept, _ = decide.prune(cutover._back_rungs(), {"arms": False})
    assert [r.name for r in kept] == ["at_flip"]
    assert branch(seen, "armed") == [HOLDS, REFUSES]
    assert why(seen, "armed") == "this engine starts its stream back only" \
                                 " whole"
    assert branch(seen, "at_flip") == [HOLDS]


class _CliHop:
    def __init__(self, where):
        self.where = where

    def report_dir(self, db):
        return self.where


def test_a_bulk_way_and_the_copier_under_it_are_both_held(monkeypatch,
                                                           tmp_path):
    from migkit import movers
    way = next(r.name for r in movers._ladder() if r.name != "builtin")
    ran = []
    monkeypatch.setattr(cli, "_bulk_db", lambda *a: ran.append("bulk") or 1)
    monkeypatch.setattr(cli, "_move_full", lambda *a: ran.append("copier"))
    monkeypatch.setattr(movers, "programs", lambda v: ())
    with watched() as seen:
        cli._bulk_or_copier(_CliHop(tmp_path), None, "app", way, None, 1000,
                            "mysql", go=False)
    assert ran == ["bulk"]
    assert branch(seen, way) == [HOLDS]
    assert branch(seen, "builtin") == [HOLDS]
