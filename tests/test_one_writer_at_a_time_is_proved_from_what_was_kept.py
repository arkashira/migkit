"""The proof that one machine wrote a hop at a time, from the holdings
kept, the number the target holds and its last batch's mark (backlog W1,
`ha.judge`) - and that it says "two writers" wherever what was kept says
so, which is the point of it.
"""
import json

from migkit import ha, shared
from migkit.config import Endpoint, Hop


def _e(fence, first, last, host="m"):
    return {"fence": fence, "by": f"{host}:{fence}", "host": host,
            "pid": fence,
            "first": {"batch": first, "token": f"0/{first}"}
            if first is not None else None,
            "last": {"batch": last, "token": f"0/{last}"}
            if last is not None else None}


def test_holdings_that_follow_each_other_are_one_writer():
    held = [_e(1, 100, 140), _e(2, 140, 190), _e(4, 190, 200)]
    two, said = ha.judge(held, target=4, mark={"batch": 200, "fence": 4})
    assert not two, said


def test_a_holding_that_went_on_from_before_the_last_saved_is_two():
    held = [_e(1, 100, 140), _e(2, 130, 150)]
    two, said = ha.judge(held)
    assert two
    assert "131 to 140 applied twice" in " ".join(said), said


def test_a_holding_that_skipped_what_the_last_saved_is_two():
    held = [_e(1, 100, 140), _e(2, 145, 150)]
    two, said = ha.judge(held)
    assert two and "never saved" in " ".join(said), said


def test_a_number_held_twice_is_two():
    two, said = ha.judge([_e(3, 1, 5), _e(3, 5, 9)])
    assert two and "number 3 was held twice" in said, said


def test_a_target_below_the_newest_holding_is_two():
    two, said = ha.judge([_e(1, 1, 5), _e(2, 5, 9)], target=1)
    assert two and "below the newest" in " ".join(said), said


def test_a_last_batch_under_the_wrong_number_is_two():
    held = [_e(1, 100, 140), _e(2, 140, 190)]
    # batch 170 is the second holding's, and the mark says the first
    # committed it: the first wrote after it lost
    two, said = ha.judge(held, target=2, mark={"batch": 170, "fence": 1})
    assert two and "committed under number 1" in " ".join(said), said


def test_the_batch_the_last_holder_committed_after_it_saved_is_its_own():
    held = [_e(1, 100, 140), _e(2, 140, 140)]
    # the first committed 141 after its last push; the second found it on
    # the target and went on after it (`twoway.committed_ahead`)
    two, said = ha.judge(held, target=2, mark={"batch": 141, "fence": 1})
    assert not two, said
    # but a batch two past it under the old number is not
    two, said = ha.judge(held, target=2, mark={"batch": 142, "fence": 1})
    assert two, said


def test_unnumbered_holdings_are_held_to_their_positions():
    a = {"fence": 1, "first": {"token": "0/A"}, "last": {"token": "0/B"}}
    b = {"fence": 2, "first": {"token": "0/B"}, "last": {"token": "0/C"}}
    assert not ha.judge([a, b])[0]
    c = {"fence": 2, "first": {"token": "0/Z"}, "last": {"token": "0/C"}}
    two, said = ha.judge([a, c])
    assert two and "not from where number 1 saved last" in said[0], said


def _hop(tmp_path):
    hop = Hop(name="ha", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="x",
                              password="x"),
              target=Endpoint(host="127.0.0.1", port=1, user="x",
                              password="x"),
              databases=["app"],
              options={"standby": True,
                       "state": {"shared": str(tmp_path / "shared")}})
    hop.report_dir = lambda db=None: _mk(tmp_path / "reports" / (db or ""))
    return hop


def _mk(p):
    p.mkdir(parents=True, exist_ok=True)
    return p


def test_the_evidence_names_who_wrote_which_batches(tmp_path):
    shared.forget()
    hop = _hop(tmp_path)
    store = shared.store_for(hop)
    assert store.kind == "directory"
    ledger = hop.report_dir("app") / ha.LEDGER
    store.write_record(ledger, {"epochs": [_e(1, 10, 20, "alpha")]}, None,
                       sealed=True)
    bundle = hop.report_dir("app") / ha.BUNDLE
    store.write_record(bundle, {"fence": 2, "files": {},
                                "epoch": _e(2, 20, 33, "beta")}, None,
                       sealed=True)
    got = ha.evidence(hop)
    rows = [r for r in got if r["what"].startswith("batches")]
    assert [(r["by"], r["number"], r["what"]) for r in rows] == [
        ("alpha process 1", 1, "batches 10 to 20"),
        ("beta process 2", 2, "batches 20 to 33")]
    # the target cannot be reached here: judged on what was kept
    verdict = [r for r in got if r["what"] in ("one writer at a time",
                                              "two writers")]
    assert verdict and verdict[0]["what"] == "one writer at a time", got
    # and where the second went on from before the first's last save
    store.write_record(bundle, {"fence": 2, "files": {},
                                "epoch": _e(2, 15, 33, "beta")},
                       store.read_record(bundle)[1], sealed=True)
    verdict = [r for r in ha.evidence(hop)
               if r["what"] in ("one writer at a time", "two writers")]
    assert verdict[0]["what"] == "two writers", verdict
    assert "applied twice" in verdict[0]["to"]
    shared.forget()


def test_a_holding_left_between_its_seal_and_the_ledger_is_not_lost(
        tmp_path):
    """The holdings before a new one ride in its record until the ledger
    has them: `epochs` reads them from there too."""
    shared.forget()
    hop = _hop(tmp_path)
    store = shared.store_for(hop)
    bundle = hop.report_dir("app") / ha.BUNDLE
    store.write_record(bundle, {"fence": 3, "files": {},
                                "previous": [_e(1, 1, 4), _e(2, 4, 9)],
                                "epoch": _e(3, 9, 9)}, None, sealed=True)
    assert [e["fence"] for e in ha.epochs(hop, "app", store)] == [1, 2, 3]
    shared.forget()


def test_health_says_the_machines_and_their_roles(tmp_path):
    shared.forget()
    hop = _hop(tmp_path)
    store = shared.store_for(hop)
    ha.beat(hop, store, "leading", fence=7)
    lines = ha.health(hop, "app", store)
    assert "leading (number 7)" in lines[0], lines
    assert any(line.startswith(("one writer at a time", "two writers"))
               for line in lines), lines
    # a hop whose machines do not work together says nothing
    plain = Hop(name="p", engine="postgres", source=Endpoint(),
                target=Endpoint())
    assert ha.health(plain, "app") == []
    shared.forget()


def test_the_bundle_is_read_as_json_by_any_machine(tmp_path):
    """Sealed or not, a record a machine wrote is what another reads."""
    shared.forget()
    hop = _hop(tmp_path)
    store = shared.store_for(hop)
    path = hop.report_dir("app") / "x.json"
    store.write_record(path, {"a": [1, 2]}, None, sealed=True)
    raw = json.loads((tmp_path / "shared" / "ha" / "run" / "app" / "x.json")
                     .read_text())
    assert raw["version"] == 1
    other = shared.Directory(hop, tmp_path / "shared")
    assert other.read_record(path, sealed=True) == ({"a": [1, 2]}, 1)
    shared.forget()
