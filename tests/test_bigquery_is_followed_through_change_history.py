"""BigQuery followed as a source: each window of its change history read
whole, as the net state of every row that changed in it
(`engines/bigquery_changes.py`).

What is held: a window is `(S, E]` with `E` at least ten minutes old and no
more than a day on from `S`; a key changed in it is handed on as the row the
table held at `E`, or as a delete; windows follow one another until the
limit or the newest one allowed; a position inside a window names how far
each table is through it, and reading from it again - in this process or a
new one - gives the same changes; a table with no key or no change history
is refused with the statement that would turn it on; a position older than
the time travel is lost, said, never stepped over.

The client is a stand-in that answers as the documentation says
(`tests/bigquery_fake.py`): no project has answered `CHANGES()`, so none of
the cells this serves is in a `PROVES` (`capabilities.UNPROVED`).
"""
import datetime

import pytest

pytest.importorskip("google.cloud.bigquery")

from tests import bigquery_fake as fake  # noqa: E402
from tests.bigquery_fake import Field, Table  # noqa: E402

UTC = datetime.timezone.utc
T0 = datetime.datetime(2026, 5, 1, 12, 0, tzinfo=UTC)


def _stamp(at):
    return at.isoformat(timespec="microseconds")


def _eng(monkeypatch, tmp_path, now=None, history=True, key=True):
    eng = fake.engine(monkeypatch, report=tmp_path)
    src = eng.clients["src"]
    src.dataset("sales").add(Table(
        "orders", [Field("id", "INTEGER", "REQUIRED"), Field("name", "STRING"),
                   Field("price", "NUMERIC", precision=12, scale=2)],
        constraints=fake.constraints(primary=["id"]) if key else None))
    src.on("information_schema.tables", [["orders"]])
    src.on("table_options", [["orders", "true" if history else "false"]])
    state = {"now": now or T0 + datetime.timedelta(minutes=30), "windows": []}
    src.on("select current_timestamp()", lambda sql: [[state["now"]]])

    def window(sql):
        state["windows"].append(sql)
        return state.get("rows", [])
    src.answers.insert(0, ("changes(table", window, 0))
    eng.state = state
    return eng


def _row(i, name=None, here=True, price=None):
    return {"_migkit_k0": i, "_migkit_here": True if here else None,
            "id": i if here else None, "name": name if here else None,
            "price": price if here else None}


def test_a_window_is_handed_on_as_the_net_state_at_its_end(monkeypatch,
                                                           tmp_path):
    eng = _eng(monkeypatch, tmp_path)
    eng.state["rows"] = [_row(1, "a", price=None), _row(2, here=False)]
    got, token = eng.neutral_changes("src", "sales", {"at": _stamp(T0)})
    end = T0 + datetime.timedelta(minutes=19)
    assert [(c["op"], c["table"], c["key"]) for c in got] == [
        ("update", "orders", {"id": 1}), ("delete", "orders", {"id": 2})]
    assert got[0]["values"] == {"id": 1, "name": "a", "price": None}
    assert got[0]["txn"] == _stamp(end) == got[1]["txn"]
    assert token == {"at": _stamp(end)}
    sql = eng.state["windows"][0]
    # the keys that changed, and the table as it stood at the window's end
    assert "from changes(table `sales`.`orders`, timestamp" in sql
    assert f"timestamp '{_stamp(T0)}', timestamp '{_stamp(end)}'" in sql
    assert f"for system_time as of timestamp '{_stamp(end)}'" in sql
    assert sql.endswith("order by ch.`id`")


def test_the_newest_window_ends_ten_minutes_ago_and_no_later(monkeypatch,
                                                             tmp_path):
    eng = _eng(monkeypatch, tmp_path)
    got, token = eng.neutral_changes("src", "sales", {"at": _stamp(T0)})
    assert got == [] and token == {"at": _stamp(
        eng.state["now"] - datetime.timedelta(minutes=11))}
    # nothing to read before a window is ten minutes old
    eng.state["now"] = T0 + datetime.timedelta(minutes=5)
    before = len(eng.state["windows"])
    got, token = eng.neutral_changes("src", "sales", {"at": _stamp(T0)})
    assert got == [] and token == {"at": _stamp(T0)}
    assert len(eng.state["windows"]) == before


def test_windows_follow_one_another_a_day_at_most(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path,
               now=T0 + datetime.timedelta(days=3, minutes=30))
    got, token = eng.neutral_changes("src", "sales", {"at": _stamp(T0)})
    assert got == []
    # three days and nineteen minutes behind: windows of 23 hours
    assert len(eng.state["windows"]) == 4
    assert token == {"at": _stamp(eng.state["now"] - datetime.timedelta(
        minutes=11))}


def test_a_window_too_big_for_one_read_is_handed_on_in_parts(monkeypatch,
                                                            tmp_path):
    eng = _eng(monkeypatch, tmp_path)
    eng.state["rows"] = [_row(i, f"n{i}") for i in range(1, 6)]
    end = T0 + datetime.timedelta(minutes=19)
    got, token = eng.neutral_changes("src", "sales", {"at": _stamp(T0)}, 2)
    assert [c["key"]["id"] for c in got] == [1, 2]
    assert token == {"at": _stamp(T0), "end": _stamp(end),
                     "tables": {"orders": 2}}
    got, token = eng.neutral_changes("src", "sales", token, 2)
    assert [c["key"]["id"] for c in got] == [3, 4]
    got, token = eng.neutral_changes("src", "sales", token, 2)
    assert [c["key"]["id"] for c in got] == [5]
    assert token == {"at": _stamp(end)}
    # one query for the window, whatever the parts
    assert len(eng.state["windows"]) == 1


def test_a_stopped_read_asked_again_gives_the_same_changes(monkeypatch,
                                                           tmp_path):
    eng = _eng(monkeypatch, tmp_path)
    eng.state["rows"] = [_row(i, f"n{i}") for i in range(1, 6)]
    end = T0 + datetime.timedelta(minutes=19)
    mid = {"at": _stamp(T0), "end": _stamp(end), "tables": {"orders": 3}}
    got, token = eng.neutral_changes("src", "sales", mid, 10)
    assert [c["key"]["id"] for c in got] == [4, 5]
    # the process went away: a new one reads the window again and passes
    # over what the position says was delivered
    eng.release_changes()
    again, _ = eng.neutral_changes("src", "sales", mid, 10)
    assert again == got


def test_a_position_older_than_the_history_is_lost_not_stepped_over(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path,
               now=T0 + datetime.timedelta(days=9))
    with pytest.raises(SystemExit) as e:
        eng.neutral_changes("src", "sales", {"at": _stamp(T0)})
    assert "older than the dataset's time travel (168 hours)" in str(e.value)
    assert eng.stream_room("src", "sales", {"at": _stamp(T0)}) == {
        "seconds": 0}
    fresh = {"at": _stamp(T0 + datetime.timedelta(days=9, hours=-1))}
    assert eng.stream_room("src", "sales", fresh) == {
        "seconds": 168 * 3600 - 3600}


def test_a_table_that_cannot_be_followed_is_refused_with_the_statement(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, history=False, key=False)
    with pytest.raises(SystemExit) as e:
        eng.change_point("src", "sales")
    said = str(e.value)
    assert "orders has no declared key" in said
    assert "alter table `sales`.`orders` set options(enable_change_history" \
        " = true)" in said
    ok = _eng(monkeypatch, tmp_path)
    assert ok.change_point("src", "sales") == {"at": _stamp(ok.state["now"])}


def test_a_key_that_takes_null_cannot_be_followed(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path)
    eng.clients["src"].dataset("sales").tables["orders"].schema[0] = Field(
        "id", "INTEGER", "NULLABLE")
    with pytest.raises(SystemExit) as e:
        eng.change_point("src", "sales")
    assert "its key column id takes NULL" in str(e.value)


def test_the_start_of_a_tail_and_its_hooks(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path)
    got, point = eng.neutral_changes("src", "sales", None)
    assert got == [] and point == {"at": _stamp(eng.state["now"])}
    assert eng.copy_point("src", "sales") == point
    assert eng.stream_identity("src", "sales") == {
        "dataset": "proj-a.sales"}
    later = {"at": _stamp(eng.state["now"] + datetime.timedelta(hours=1))}
    assert eng.position_reached(later, point) is True
    assert eng.position_reached(point, later) is False
    assert eng.position_reached({}, point) is None
    # a change is covered by a mark taken after the window it ended
    mark = eng.snapshot_mark("src", "sales")
    assert eng.mark_covers("src", "sales", mark,
                           {"txn": _stamp(T0)}) is True
    assert eng.mark_covers("src", "sales", mark,
                           {"txn": _stamp(eng.state["now"] + datetime.
                                          timedelta(hours=1))}) is False
    assert eng.mark_covers("src", "sales", mark, {}) is None


def test_a_name_given_to_another_table_is_told_by_when_it_was_made(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path)
    eng.clients["src"].answers.insert(0, (
        "creation_time", [["orders", "2026-01-01 00:00:00+00:00"],
                          ["audit", "x"]], 0))
    eng.hop.exclude = ["audit"]
    assert eng.table_oids("src", "sales") == {
        "orders": "2026-01-01 00:00:00+00:00"}


def test_the_assessment_says_how_far_behind_and_what_cannot_be_followed(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, history=False)
    rows = eng.reach_items("src")
    texts = {r["item"]: r["detail"] for r in rows}
    assert "ten minutes behind" in texts["changes followed through change"
                                         " history"]
    assert "orders keeps no change history" in texts[
        "tables that cannot be followed"]


def test_the_pair_takes_it_for_a_source_it_can_tail(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path)
    pair = eng._as_pair()
    pair._can_tail()
    from migkit.engines.hetero import HeteroEngine
    assert isinstance(pair, HeteroEngine)
    from migkit.engines.base import Engine
    from migkit.engines.warehouse import BigQueryEngine
    for hook in ("tail_apply", "src_lsn", "fence_wait", "delta_verify"):
        assert getattr(BigQueryEngine, hook) is not getattr(Engine, hook,
                                                            None)


def test_the_pairs_tail_reads_a_window_applies_it_and_then_saves_the_end(
        monkeypatch, tmp_path):
    """The whole tail over BigQuery's windows: it starts from the point
    taken before the copy, reads the first window ten minutes old, hands
    the net state to the target's applier, and saves the window's end only
    once the target has it."""
    from migkit import tailctl
    from migkit.engines.warehouse import BigQueryEngine
    eng = _eng(monkeypatch, tmp_path, now=T0)
    for side in ("src", "dst"):
        eng.clients[side].dataset("sales").add(Table(
            "orders", [Field("id", "INTEGER", "REQUIRED"),
                       Field("name", "STRING")],
            constraints=fake.constraints(primary=["id"])))
    eng.clients["src"].answers.insert(0, (
        "creation_time", [["orders", "2026-01-01 00:00:00+00:00"]], 0))
    eng.clients["dst"].on("information_schema.tables", [["orders"]])
    eng.state["rows"] = [_row(1, "a"), _row(2, here=False)]
    pair = eng._as_pair()
    where = tmp_path / "tail"
    where.mkdir()
    path = where / "token.json"
    assert pair.tail_start("sales", path) is True
    assert tailctl.position(path)["token"] == {"at": _stamp(T0)}
    eng.state["now"] = T0 + datetime.timedelta(minutes=30)
    seen = []

    def apply_batch(self, side, db, by_table, ledger):
        seen.append(by_table)
        tailctl.stop(where)
    monkeypatch.setattr(BigQueryEngine, "_apply_batch", apply_batch)
    monkeypatch.setattr(BigQueryEngine, "origin_seen",
                        lambda self, side, db: None)
    pair.tail_apply("sales", True, path, lambda m: None)
    assert seen == [{"orders": [
        ("upsert", {"id": 1}, {"id": 1, "name": "a"}),
        ("delete", {"id": 2}, {})]}]
    assert tailctl.position(path)["token"] == {
        "at": _stamp(T0 + datetime.timedelta(minutes=19))}
