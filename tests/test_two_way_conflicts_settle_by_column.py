"""A row both sides of a two-way pair changed, settled column by column
(backlog P0-DEPTH 2, F7.merge).

Before, a conflict was decided whole by one of five policies: two sides
that changed different columns of one row were a conflict under `error`
(the tail stopped), and under `apply_remote` or `keep_local` one side's
column was written over or thrown away. Now only a column both sides
changed is a conflict, settled by the rule the hop gives it - per hop,
per table, per column or per group of columns - and every other column
keeps what its side wrote. Each kind of conflict is settled explicitly,
the pair of hops is checked to settle each one alike from both ends, and
every conflict reaches the record, `/metrics`, the receivers and the
evidence export.

These run `twoway.resolve` against two sides kept in memory
(`tests/twoway_sides.py`), each applying a batch as the engines do.
"""
import hashlib
import json

import pytest

from migkit import canon, hopkeys, twoway
from tests.twoway_sides import Side, mirrored, pair, records, sync

BASE = {1: {"v": "v0", "w": "w0", "ts": 1, "n": 10, "hi": 5,
            "tags": [1, 2]},
        2: {"v": "v2", "w": "w2", "ts": 1, "n": 0, "hi": 0, "tags": []},
        3: {"v": "v3", "w": "w3", "ts": 1, "n": 0, "hi": 0, "tags": []}}


def _sides(tmp_path, two_way, base=BASE, back=None):
    a, b = Side(base), Side(base)
    ab = pair("ab", two_way, b, tmp_path / "ab")
    ba = pair("ba", mirrored(two_way) if back is None else back, a,
              tmp_path / "ba")
    return a, b, ab, ba


def test_two_sides_that_changed_different_columns_both_keep_theirs(
        tmp_path):
    """Under `error`, the strictest rule: no column was changed on both
    sides, so nothing stops and nothing is written over."""
    a, b, ab, ba = _sides(tmp_path, {"on_conflict": "error"})
    ca = [a.update(1, v="from a")]
    cb = [b.update(1, w="from b")]
    at_b = sync(ab, ca)
    sync(ba, cb)
    assert a.rows == b.rows
    assert (a.rows[1]["v"], a.rows[1]["w"]) == ("from a", "from b")
    # the target's own column neither written over nor left out of the
    # upsert (a NOT NULL column would refuse it): kept where it stands
    assert at_b[0]["values"]["w"] == canon.Kept("from b")
    assert at_b[0]["values"]["v"] == "from a"
    got = records(tmp_path / "ab")
    assert [(r["kind"], r["decision"], r["rules"]) for r in got] == [
        ("update_origin_differs", "merged", [])]
    assert (got[0]["result"]["v"], got[0]["result"]["w"]) == ("from a",
                                                              "from b")


def test_the_old_whole_row_policies_merge_what_only_one_side_changed(
        tmp_path):
    """`apply_remote` one way and `keep_local` back, as a pair was set up
    before: a column only one side changed is still that side's."""
    a, b, ab, ba = _sides(tmp_path, {"on_conflict": "apply_remote"})
    assert ba.hop.options["two_way"]["on_conflict"] == "keep_local"
    ca = [a.update(1, v="from a", n=11)]
    cb = [b.update(1, v="from b", w="only b")]
    sync(ab, ca)
    sync(ba, cb)
    assert a.rows == b.rows
    # v both changed: the source of `ab` (A) wins on both sides; w and n
    # only one side changed, and neither is lost
    assert (a.rows[1]["v"], a.rows[1]["w"], a.rows[1]["n"]) == (
        "from a", "only b", 11)


def test_each_column_is_settled_by_its_own_rule(tmp_path):
    """A group settled whole by its timestamp, a counter, a maximum, a set
    and a column with no rule of its own, which takes the table's - and
    the table names none, so the hop's."""
    rules = {"on_conflict": "overwrite",
             "tables": {"t": {"columns": {
                 "v, ts": {"newest": "ts"}, "n": "delta", "hi": "max",
                 "tags": "union"}}}}
    a, b, ab, ba = _sides(tmp_path, rules)
    assert twoway.apart(rules, mirrored(rules)) == []
    ca = [a.update(1, v="va", ts=5, w="wa", n=13, hi=9, tags=[1, 2, 3])]
    cb = [b.update(1, v="vb", ts=6, w="wb", n=12, hi=7, tags=[2, 4])]
    sync(ab, ca)
    sync(ba, cb)
    want = {"id": 1, "v": "vb", "ts": 6, "w": "wa", "n": 15, "hi": 9,
            "tags": [2, 3, 4]}
    assert a.rows[1] == want, a.rows[1]
    assert b.rows[1] == want, b.rows[1]
    took = {tuple(r["columns"]): (r["rule"], r["took"])
            for r in records(tmp_path / "ab")[0]["rules"]}
    assert took == {("v", "ts"): ("newest by ts", "local"),
                    ("w",): ("overwrite", "remote"),
                    ("n",): ("delta", "added"),
                    ("hi",): ("max", "remote"),
                    ("tags",): ("union", "value")}


def test_a_table_rule_stands_before_the_hops(tmp_path):
    rules = {"on_conflict": "ignore", "column": "ts",
             "tables": {"public.t": {"default": "newest"}}}
    tr = twoway.Rules(rules, None).table("t")
    assert tr.default == twoway.Rule("newest", "ts")
    assert twoway.Rules(rules, None).table("other").default == \
        twoway.Rule("ignore")
    a, b, ab, ba = _sides(tmp_path, rules)
    ca = [a.update(1, v="older", ts=3)]
    cb = [b.update(1, v="newer", ts=4)]
    sync(ab, ca)
    sync(ba, cb)
    assert a.rows[1]["v"] == b.rows[1]["v"] == "newer"


def test_a_row_deleted_on_one_side_and_changed_on_the_other_is_kept(
        tmp_path):
    """The stated default where a table's rule settles values: the changed
    row stands, and is made again whole where it was deleted."""
    a, b, ab, ba = _sides(tmp_path, {"column": "ts",
                                     "on_conflict": "newest"})
    ca = [a.delete(2)]
    cb = [b.update(2, v="kept", ts=9)]
    sync(ab, ca)
    sync(ba, cb)
    assert a.rows == b.rows
    assert a.rows[2]["v"] == "kept" and a.rows[2]["w"] == "w2"
    assert [(r["kind"], r["decision"], r["policy"])
            for r in records(tmp_path / "ab")] == [
        ("delete_origin_differs", "keep_local", "update")]
    assert [(r["kind"], r["decision"])
            for r in records(tmp_path / "ba")] == [
        ("update_missing", "apply")]


def test_where_the_hop_says_so_the_delete_wins_on_both_sides(tmp_path):
    a, b, ab, ba = _sides(tmp_path, {"column": "ts",
                                     "on_conflict": "newest",
                                     "delete_vs_update": "delete"})
    ca = [a.delete(2)]
    cb = [b.update(2, v="changed", ts=9)]
    sync(ab, ca)
    sync(ba, cb)
    assert 2 not in a.rows and 2 not in b.rows
    assert a.rows == b.rows


def test_a_row_both_sides_deleted_is_recorded_and_nothing_stops(tmp_path):
    a, b, ab, ba = _sides(tmp_path, {"on_conflict": "error"})
    ca = [a.delete(3), a.update(1, v="goes on")]
    cb = [b.delete(3)]
    sync(ab, ca)
    sync(ba, cb)
    assert 3 not in a.rows and 3 not in b.rows
    assert b.rows[1]["v"] == "goes on"
    for side in ("ab", "ba"):
        assert [(r["kind"], r["decision"])
                for r in records(tmp_path / side)] == [
            ("delete_missing", "ignored")], side


def test_a_row_both_sides_made_is_settled_column_by_column(tmp_path):
    a, b, ab, ba = _sides(tmp_path, {"column": "ts",
                                     "on_conflict": "newest",
                                     "tables": {"t": {"columns": {
                                         "n": "delta", "tags": "union"}}}})
    ca = [a.insert(7, v="a", w="same", ts=3, n=4, tags=[1])]
    cb = [b.insert(7, v="b", w="same", ts=4, n=6, tags=[2])]
    at_b = sync(ab, ca)
    at_a = sync(ba, cb)
    assert a.rows == b.rows
    # made on both: no addition is known, so the counter is the table's
    # (newest: B's), written as a value on both sides, never added
    assert a.rows[7]["v"] == "b" and a.rows[7]["n"] == 6
    assert isinstance(at_b[0]["values"]["n"], canon.Kept)
    assert not isinstance(at_a[0]["values"]["n"], canon.Added)
    # a set both made holds both sides' members
    assert a.rows[7]["tags"] == [1, 2]
    assert records(tmp_path / "ab")[0]["kind"] == "insert_exists"


def test_error_still_stops_the_tail_before_anything_is_applied(tmp_path):
    a, b, ab, _ = _sides(tmp_path, {"on_conflict": "error"})
    ca = [a.update(2, v="goes first"), a.update(1, v="from a")]
    b.update(1, v="from b")
    held = dict(b.rows[2])
    with pytest.raises(SystemExit, match="update_origin_differs on t"):
        sync(ab, ca)
    assert b.rows[2] == held and b.rows[1]["v"] == "from b"
    got = records(tmp_path / "ab")
    assert [(r["decision"], r["local"]["v"], r["remote"]["v"])
            for r in got] == [("stop", "from b", "from a")]


def test_an_update_of_a_row_deleted_on_the_target_stops_under_error(
        tmp_path):
    a, b, ab, _ = _sides(tmp_path, {"on_conflict": "error"})
    ca = [a.update(1, v="x")]
    b.delete(1)
    with pytest.raises(SystemExit, match="update_missing on t .* the rule"
                                         " for delete_vs_update is error"):
        sync(ab, ca)
    assert 1 not in b.rows


def test_a_parked_row_waits_until_decided_and_the_rest_goes_on(
        tmp_path, monkeypatch):
    from migkit import notify
    told = []
    monkeypatch.setattr(notify, "send",
                        lambda hop, text, facts, trouble, *a, **k:
                        told.append((text, facts, trouble)) or 1)
    a, b, ab, _ = _sides(tmp_path, {"on_conflict": "park"})
    ab.hop.options["notify"] = "https://example.com/hook"
    said = []
    b.update(1, v="b1")
    sync(ab, [a.update(1, v="a1"), a.update(2, v="a2")], said)
    assert b.rows[1]["v"] == "b1" and b.rows[2]["v"] == "a2"
    parked = json.loads((tmp_path / "ab" / "d" / twoway.PARKED)
                        .read_text())["rows"]
    assert list(parked) == ["t"] and len(parked["t"]) == 1
    counts = json.loads((tmp_path / "ab" / "d" / twoway.COUNTS)
                        .read_text())
    assert counts["parked"] == {"t": 1}
    assert any("set aside until decided" in m for m in said), said
    # told at once, by kind and count, never a value
    assert told and told[-1][2] is True
    assert "1 parked" in told[-1][0] and "a1" not in told[-1][0] \
        and "b1" not in told[-1][0]
    assert told[-1][1]["parked"] == 1
    # a later change of the row is held too
    sync(ab, [a.update(1, v="a1 again")])
    assert b.rows[1]["v"] == "b1"
    assert records(tmp_path / "ab")[-1]["decision"] == "held"
    # decided on both sides: the target's row made the source's
    b.rows[1] = dict(a.rows[1])
    said = []
    sync(ab, [a.update(2, v="a2 again")], said)
    assert any("its changes go on" in m for m in said), said
    assert json.loads((tmp_path / "ab" / "d" / twoway.PARKED)
                      .read_text())["rows"] == {}
    sync(ab, [a.update(1, v="after")])
    assert b.rows[1]["v"] == "after"


def test_a_key_moved_on_one_side_while_the_other_changed_the_row(
        tmp_path):
    a, b, ab, ba = _sides(tmp_path, {"column": "ts",
                                     "on_conflict": "newest"})
    ca = [a.move(1, 10)]
    cb = [b.update(1, v="changed on b", ts=5)]
    at_b = sync(ab, ca)
    sync(ba, cb)
    # the changed row stays where B has it, and the moved row arrives at
    # its new key on both sides
    assert [(c["op"], c["key"]) for c in at_b] == [("insert", {"id": 10})]
    assert sorted(a.rows) == sorted(b.rows) == [1, 2, 3, 10]
    assert a.rows == b.rows
    assert a.rows[1]["v"] == "changed on b"


def test_a_batch_applied_twice_leaves_the_rows_as_once(tmp_path):
    rules = {"column": "ts", "on_conflict": "overwrite",
             "tables": {"t": {"columns": {"hi": "max", "tags": "union",
                                          "v, w": {"newest": "ts"}}}}}
    a, b, ab, _ = _sides(tmp_path, rules)
    ca = [a.update(1, v="a", hi=8, tags=[1, 5], ts=4),
          a.insert(8, v="made", ts=2)]
    b.update(1, w="b", hi=9, tags=[2, 6], ts=3)
    b.insert(8, v="also made", ts=3)
    sync(ab, ca)
    once = json.dumps(b.rows, sort_keys=True)
    sync(ab, ca)
    assert json.dumps(b.rows, sort_keys=True) == once


@pytest.mark.parametrize("two_way,match", [
    ({"on_conflict": "newst"}, "not one of .*did you mean newest"),
    ({"on_conflict": "last_update_wins"}, "name it as two_way.column"),
    ({"tables": {"t": {"columns": {"a, b": "max"}}}},
     "settled by one column's value"),
    ({"tables": {"t": {"columns": {"a, b": "delta"}}}},
     "delta settles one column"),
    ({"tables": {"t": {"columns": {"a": "max", "a, b": {"min": "b"}}}}},
     "a already has a rule"),
    ({"tables": {"t": {"colums": {}}}}, "not a key of a table's rules"),
    ({"tables": {"t": {"default": "source_priority"}}}, "source_rank and"),
    ({"delete_vs_update": "newest"}, "delete_vs_update: newest is not"),
    ({"delete_missing": "overwrite"}, "delete_missing: overwrite is not"),
])
def test_a_rule_that_cannot_hold_is_refused(two_way, match):
    with pytest.raises(SystemExit, match=match):
        twoway.Rules(two_way, twoway._ranks_of(two_way))


@pytest.mark.parametrize("columns,match", [
    ({"v": "union"}, "union keeps both sides' members, and t.v is text"),
    ({"v": "delta"}, "t.v is text, not a number"),
    ({"v": {"newest": "changed_at"}}, "compares changed_at, which t does"),
])
def test_a_rule_the_table_cannot_hold_is_refused_before_anything(
        tmp_path, columns, match):
    a, b, ab, _ = _sides(tmp_path, {"tables": {"t": {"columns": columns}}})
    ca = [a.update(1, v="x")]
    with pytest.raises(SystemExit, match=match):
        sync(ab, ca)
    assert b.rows[1]["v"] == "v0"
    assert not (tmp_path / "ab" / "d").exists()


def test_the_old_policies_are_the_rules_they_were():
    for old, new in (("error", "error"), ("apply_remote", "overwrite"),
                     ("keep_local", "ignore"),
                     ("last_update_wins", "newest"),
                     ("source_priority", "source_priority")):
        got = twoway.Rules({"on_conflict": old, "column": "ts",
                            "source_rank": 2, "target_rank": 1}, (2, 1))
        assert got.all.default.name == new, old
    # counters named the old way are counters on every table, and make the
    # hop's batches exact
    got = twoway.Rules({"delta": ["n"]}, None)
    assert got.table("t").rule_of("n")[1] == twoway.Rule("delta")
    assert got.adds and not twoway.Rules({}, None).adds


def _hops_file(ab, ba):
    src = {"host": "10.0.0.1", "port": 5432, "user": "u"}
    dst = {"host": "10.0.0.2", "port": 3306, "user": "u"}
    opts = {"source_engine": "postgres", "target_engine": "mysql"}
    back = {"source_engine": "mysql", "target_engine": "postgres"}
    return {"hops": {
        "ab": {"engine": "hetero", "source": src, "target": dst,
               "options": {**opts, "two_way": ab}},
        "ba": {"engine": "hetero", "source": dst, "target": src,
               "options": {**back, "two_way": ba}}}}


def _pair_problems(ab, ba):
    return [p.text for p in hopkeys.check(_hops_file(ab, ba))
            if p.where == "hops.ab and hops.ba"]


def test_a_pair_that_would_settle_a_conflict_apart_is_refused():
    both = _pair_problems({"on_conflict": "overwrite"},
                          {"on_conflict": "overwrite"})
    assert any("overwrite one way and overwrite the way back" in t
               and "needs ignore" in t for t in both), both
    assert _pair_problems({"on_conflict": "overwrite"},
                          {"on_conflict": "ignore"}) == []
    ranked = {"on_conflict": "source_priority", "source_rank": 2,
              "target_rank": 1}
    turned = _pair_problems(ranked, dict(ranked))
    assert any("ranks turned round" in t for t in turned), turned
    assert _pair_problems(ranked, mirrored(ranked)) == []
    grouped = {"column": "ts", "tables": {"t": {"columns": {
        "v, w": {"newest": "ts"}}}}}
    split = {"column": "ts", "tables": {"t": {"columns": {
        "v": {"newest": "ts"}}}}}
    assert any("settled with v, w one way" in t
               for t in _pair_problems(grouped, split))
    assert _pair_problems(grouped, grouped) == []
    # one hop's own rules are checked before anything runs
    alone = [p for p in hopkeys.check(_hops_file({"on_conflict": "newst"},
                                                 {}))
             if p.where == "hops.ab.options.two_way"]
    assert alone and alone[0].level == "error"


def test_every_conflict_reaches_the_record_metrics_and_evidence(
        tmp_path, monkeypatch):
    from migkit import export, ui
    a, b, ab, _ = _sides(tmp_path, {"on_conflict": "overwrite"})
    ca = [a.update(1, v="secret a")]
    b.update(1, v="secret b")
    sync(ab, ca)
    where = tmp_path / "ab"
    text = (where / "d" / twoway.RECORD).read_text()
    line = json.loads(text)
    assert (line["kind"], line["decision"], line["remote_before"]["v"],
            line["remote"]["v"], line["local"]["v"], line["result"]["v"]) \
        == ("update_origin_differs", "apply", "v0", "secret a", "secret b",
            "secret a")
    assert line["rules"] == [{"columns": ["v"], "rule": "overwrite",
                              "took": "remote", "tie": False}]
    entry = [json.loads(x) for x in
             (where / "changelog.jsonl").read_text().splitlines()][-1]
    assert entry["op"] == "two-way-conflicts" and entry["rows"] == 1
    assert entry["sha256"] == hashlib.sha256(text.encode()).hexdigest()
    got = ui._conflict_metrics(where, 'hop="ab"')
    assert ('migkit_two_way_conflicts{hop="ab",db="d",table="t",'
            'kind="update_origin_differs",decision="apply"} 1') in got
    shown = export._conflicts(ab.hop, where, None)
    assert shown["rows"][0]["source"]["v"] == "secret a"
    assert shown["rows"][0]["columns"][0]["rule"] == "overwrite"
    # masked where the hop masks the column, in the export only
    ab.hop.options["mask"] = ["t.v"]
    shown = export._conflicts(ab.hop, where, None)
    for side in ("before", "source", "target", "result"):
        assert str(shown["rows"][0][side]["v"]).startswith("masked:"), side
    assert "secret" not in json.dumps(shown)
    # a run that saw none carries none
    assert export._conflicts(ab.hop, where, "another-run")["rows"] == []
