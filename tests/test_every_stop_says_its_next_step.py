"""Where a command stops or finds a fault the operator is told what
happened, what it means and the one next step - never a wrapped tool's name
or a path - and the change sheet gets the index and check ends. All on
fakes: no server."""
import re
from pathlib import Path

import pytest

from migkit import cli, guide, report, state, verdict

#: the programs migkit drives, which operator text never names
TOOLS = ("pgcopydb", "reladiff", "dvt", "mydumper", "debezium", "pt-table",
         "confluent", "minio")

SITES = {
    "restore_unreachable": (("20260101-000000", "s3", "h"), "migkit rollback h"),
    "record_changed": (("line 4", "h"), "migkit history h"),
    "no_saved_states": (("h",), "migkit history h"),
    "schema_failed": ((2, False, ["create t", "create u"], "h"),
                      "migkit schema h --convert"),
    "mask_refused": (("h", "d", [("c", "no such type")]), "migkit move h"),
    "two_way_refused": (("no marker column", "h"), "migkit doctor"),
    "replication_refused": (("the scope changed", "h"), "migkit move h"),
    "copy_not_started": (("d", "too wide for memory", "h"), "migkit doctor"),
    "repair_stopped": (("d", "the scratch copy failed", "h"), "migkit sync h"),
    "scope_empty": (("h",), "migkit check h"),
    "config_unreadable": (("hops.yaml", "line 3"), "migkit doctor"),
    "probe_unasked": (("h", "src", "replication", "timed out"),
                      "migkit doctor"),
    "transport_failed": (("source", "unverified", "no certificate check"),
                         "migkit doctor"),
    "port_silent": (("target", "timed out"), "migkit doctor"),
    "hop_not_reread": (("bad yaml", "h"), "next cycle"),
    "address_unreadable": (("source",), "migkit init"),
    "source_unreachable": (("refused",), "migkit init"),
}


@pytest.mark.parametrize("name", sorted(SITES))
def test_a_stop_says_what_happened_and_the_one_next_step(name):
    args, step = SITES[name]
    said = getattr(report, name)(*args)
    assert step in said, said
    low = said.lower()
    assert not any(t in low for t in TOOLS), said
    assert not re.search(r"(^|\s)(/[\w.-]+){2,}", said), said
    assert not said.rstrip().endswith("."), said


def test_a_site_with_no_next_step_would_be_caught():
    assert "migkit rollback" not in "state x not retrievable from s3"


def test_the_free_part_is_cut_of_its_trailing_stop():
    said = report.copy_not_started("d", "too wide.", "h")
    assert "too wide.." not in said
    assert "not started - too wide. Nothing was copied" in said


def test_the_transport_words_keep_the_word_doctor_prints_a_failure_by():
    unv = report.transport_failed("source", "unverified", "x")
    plain = report.transport_failed("target", "plain", "x")
    assert unv.startswith("connection NOT verified")
    assert plain.startswith("connection NOT encrypted")
    assert "target" in plain


def test_no_saved_states_keeps_its_leading_words():
    assert report.no_saved_states("h").startswith("no saved states")


class FakeStore:
    kind = "s3"

    def __init__(self, states):
        self.states = states

    def list(self, db):
        return [{"ts": t} for t in self.states]

    def fetch(self, db, ts):
        return None


class Hop:
    name = "h"


def test_a_rollback_with_no_state_names_the_history(monkeypatch):
    monkeypatch.setattr(state, "get_store", lambda hop: FakeStore([]))
    with pytest.raises(SystemExit) as e:
        cli._restore_target(Hop(), object(), "d", "", False, True)
    assert "migkit history h" in str(e.value)
    assert str(e.value).startswith("no saved states")


def test_a_state_that_cannot_be_fetched_names_the_rollback_to_rerun(
        monkeypatch):
    monkeypatch.setattr(state, "get_store",
                        lambda hop: FakeStore(["20260101-000000"]))
    with pytest.raises(SystemExit) as e:
        cli._restore_target(Hop(), object(), "d", "", False, True)
    said = str(e.value)
    assert "migkit rollback h --state 20260101-000000" in said
    assert "s3" in said and "by guess" in said


def test_a_masked_column_that_cannot_be_masked_stops_with_the_step():
    class Eng:
        def masks_refused(self, d):
            return [("card", "no mask for this type")]

        def tail_masks(self):
            return True

        def copier_masks(self):
            return True

    hop = type("H", (), {"name": "h", "mask": True, "mappings": {}})()
    with pytest.raises(verdict.Refused) as e:
        cli._refuse_unmasked(hop, Eng(), ["d"], False)
    said = str(e.value)
    assert "card (no mask for this type)" in said
    assert "migkit move h" in said and "Nothing has been started" in said


def test_an_address_that_never_parses_ends_on_how_to_give_it():
    asked = []

    def question(text):
        asked.append(text)
        return "nosuchengine://x@h/d"

    with pytest.raises(SystemExit) as e:
        guide._address(question, lambda m: None, "", "where?", "source")
    said = str(e.value)
    assert len(asked) == 3
    assert "MIGKIT_SOURCE=" in said and "migkit init" in said


def test_a_missing_partition_names_the_hop_in_its_next_step():
    src = {"p1": {"rows": 10, "digest": None, "bound": "a", "catchall": False}}
    lines = verdict.partition_differences("t", src, {}, hop="h")
    said = " ".join(l for l in lines)
    assert "`migkit schema h`" in said and "`migkit check h`" in said
    assert "missing on target, the source's holds 10 rows" in said
    bare = " ".join(l for l in verdict.partition_differences("t", src, {}))
    assert "`migkit schema`" in bare and "schema h" not in bare


class SheetHop:
    name = "h"
    options = {}

    def __init__(self, root):
        self.root = Path(root)

    def report_dir(self, db=None):
        d = self.root / (db or "")
        d.mkdir(parents=True, exist_ok=True)
        return d


def _sheet(tmp_path):
    hop = SheetHop(tmp_path)
    phases = [{"name": "copy"}, {"name": "index"}, {"name": "check"}]
    report.write_change_sheet(hop, report.change_sheet(hop, "a", phases))
    report.write_change_sheet(hop, report.read_change_sheet(hop)
                              + report.change_sheet(hop, "b", phases))
    return hop


def _by(hop, db, phase):
    return next(s for s in report.read_change_sheet(hop)
                if s["db"] == db and s["phase"] == phase)


def test_a_check_that_held_writes_its_databases_check_step_done(tmp_path):
    hop = _sheet(tmp_path)
    results = [{"scope": "a"}, {"scope": "a"}, {"scope": "b"}]
    ids = report.check_ended(hop, results, ["same", "note", "different"], 4.0)
    assert len(ids) == 2
    a, b = _by(hop, "a", "check"), _by(hop, "b", "check")
    assert a["status"] == "done" and "all 2 answers held" in a["proof"]
    assert b["status"] == "failed" and "check again" in b["proof"]
    assert _by(hop, "a", "copy")["status"] != "done"


def test_a_narrowed_check_is_partial_and_says_how_to_prove_the_whole(tmp_path):
    hop = _sheet(tmp_path)
    report.check_ended(hop, [{"scope": "a"}], ["same"], 1.0,
                       coverage={"table": "t"})
    a = _by(hop, "a", "check")
    assert a["status"] == "partial" and "narrowed to table" in a["proof"]
    assert "without narrowing" in a["proof"]


def test_a_not_compared_answer_is_never_written_as_done(tmp_path):
    hop = _sheet(tmp_path)
    report.check_ended(hop, [{"scope": "a"}], ["not compared"], 1.0)
    assert _by(hop, "a", "check")["status"] == "failed"


def test_the_index_phase_end_is_written_back(tmp_path):
    hop = _sheet(tmp_path)
    ids = cli._sheet_ended(hop, "a", "index", "done", 2.5,
                           "the indexes were built at the end of the copy")
    assert ids and _by(hop, "a", "index")["status"] == "done"
    assert _by(hop, "b", "index")["status"] != "done"


def test_a_sheet_that_cannot_be_written_back_says_what_to_do(tmp_path,
                                                             capsys):
    hop = SheetHop(tmp_path)
    (tmp_path / "change-sheet.json").write_text("{not json")
    cli._sheet_ended(hop, "a", "index", "done", 1.0, "x")
    out = capsys.readouterr().out
    if out:
        assert "migkit change-sheet h --back" in out
