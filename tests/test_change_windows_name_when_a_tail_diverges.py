import copy
from types import SimpleNamespace

import pytest

from migkit import canon, decide, durable, tailctl, tailtables, verdict, verifiers
from migkit.engines.base import Engine
from tests import servers
from tests.conftest import needs_docker

PROVES = [("tail.follows", "postgres")]
pg = servers.fixture("pg")


class Target:
    def __init__(self):
        self.rows = {}

    def neutral_columns(self, side, db, table):
        return [("id", "integer"), ("v", "text")]

    def neutral_rows_by_key(self, side, db, table, cols, key, keys):
        return {tuple(str(v) for v in k): self.rows[table, k[0]]
                for k in keys if (table, k[0]) in self.rows}

    def apply(self, changes):
        for table, (op, key, values) in Engine._collapsed(changes):
            if op == "delete":
                self.rows.pop((table, key["id"]), None)
            else:
                self.rows[table, key["id"]] = (values.get("id", key["id"]),
                                               values.get("v"))
        return len(changes)


def changes(ts=100):
    out = [canon.change("insert", "public.t", {"id": i},
                        {"id": i, "v": str(i)}) for i in range(4)]
    canon.ended(out, 0, at="0/1", ts=ts)
    return out


def hop_at(tmp_path):
    return SimpleNamespace(report_dir=lambda db=None: tmp_path)


def save(store, receipt, clock, gen):
    with store.batch(gen) as st:
        st.put("meta", f"{verifiers.INTERVALS}:{gen}", receipt)
        st.put("meta", verifiers.INTERVALS + "-clock", clock)
    durable.write_json(tailctl_path(store), {"v": 1, "token": str(gen),
                                           "gen": gen})
    store.settled()


def tailctl_path(store):
    from pathlib import Path
    return Path(store.path).parent / tailctl.TOKEN


def test_commit_time_covers_the_whole_transaction():
    batch = changes()
    assert all(c["txn_ts"] == 100 for c in batch)
    assert [c.get("txn_end", False) for c in batch] == [False] * 3 + [True]
    assert decide.change_window(100, 10) == 1
    assert decide.change_window(100, 1) == .1
    assert decide.change_window(0, 1) is None


def test_windows_are_half_open_and_read_back_values_and_deletes(tmp_path):
    store = tailtables.Store.of(tmp_path)
    target = Target()
    batch = changes()
    proof = verifiers.ChangeIntervals(target, "d", store, batch, 4)
    target.apply(batch)
    receipt = proof.observed(len(batch))
    assert proof.clock == {"start": 100, "end": 102}
    assert receipt["windows"][0]["source"][0] == 4
    assert receipt["windows"][0]["status"] == "ok"
    save(store, receipt, proof.clock, 1)
    out = verifiers.change_intervals(hop_at(tmp_path), "d")
    assert out[0]["status"] == "warn"
    later = [canon.change("delete", "public.t", {"id": 0})]
    canon.ended(later, 0, ts=102)
    proof = verifiers.ChangeIntervals(target, "d", store, later, 1)
    target.apply(later)
    save(store, proof.observed(1), proof.clock, 2)
    out = verifiers.change_intervals(hop_at(tmp_path), "d")
    assert out[0]["status"] == "ok"
    assert out[0]["window"]["end"] == out[1]["window"]["start"] == 102
    assert out[1]["status"] == "warn"
    assert ("public.t", 0) not in target.rows


@pytest.mark.parametrize("fault", ["value", "missing", "count", "delete"])
def test_a_successful_apply_does_not_hide_corruption(tmp_path, fault):
    target = Target()
    store = tailtables.Store.of(tmp_path)
    batch = changes()
    if fault == "delete":
        target.apply(batch)
        batch = [canon.change("delete", "public.t", {"id": 0})]
        canon.ended(batch, 0, ts=100)
    proof = verifiers.ChangeIntervals(target, "d", store, batch, 4)
    if fault != "delete":
        target.apply(batch)
    if fault == "value":
        target.rows["public.t", 0] = (0, "wrong")
    elif fault == "missing":
        target.rows.pop(("public.t", 0))
    receipt = proof.observed(0 if fault == "count" else len(batch))
    assert receipt["windows"][0]["status"] == "diff"
    if fault == "value":
        assert receipt["windows"][0]["source"][0] == receipt["windows"][0]["target"][0]
        assert receipt["windows"][0]["source"][1] != receipt["windows"][0]["target"][1]
    save(store, receipt, proof.clock, 1)
    out = verifiers.change_intervals(hop_at(tmp_path), "d")
    assert out[0]["status"] == "diff"
    assert out[0]["window"]["start"] == 100
    assert verdict.decide(out).status == "different"


def test_repeated_keys_and_key_moves_use_the_existing_apply_semantics(tmp_path):
    target = Target()
    batch = changes() + [canon.change("update", "public.t", {"id": 0},
                                     {"id": 8, "v": "moved"})]
    canon.ended(batch, 0, ts=100)
    proof = verifiers.ChangeIntervals(target, "d", tailtables.Store.of(tmp_path),
                                      batch, 5)
    target.apply(batch)
    assert proof.observed(5)["windows"][0]["status"] == "pending"
    assert target.rows["public.t", 8] == (8, "moved")
    assert ("public.t", 0) not in target.rows


def test_uncommitted_receipts_are_undone_and_resume_does_not_double_count(tmp_path):
    store = tailtables.Store.of(tmp_path)
    target = Target()
    batch = changes()
    proof = verifiers.ChangeIntervals(target, "d", store, batch, 4)
    with store.batch(1) as st:
        st.put("meta", verifiers.INTERVALS + ":1", proof.receipt)
        st.put("meta", verifiers.INTERVALS + "-clock", proof.clock)
    assert verifiers.change_intervals(hop_at(tmp_path), "d")[0]["status"] == "warn"
    target.apply(batch)
    with store.batch() as st:
        st.put("meta", verifiers.INTERVALS + ":1", proof.observed(4))
    store.undo()
    assert store.get(verifiers.INTERVALS + ":1") is None
    target.apply(batch)
    receipt = proof.observed(4)
    save(store, receipt, proof.clock, 1)
    tailtables.Store.forget(tmp_path)
    store = tailtables.Store.of(tmp_path)
    assert store.get(verifiers.INTERVALS + ":1")["windows"][0]["source"][0] == 4
    with store.batch(2) as st:
        st.put("meta", verifiers.INTERVALS + ":2", {"windows": [], "unknown": [],
                                                       "through": 102})
    store.undo()
    assert store.get(verifiers.INTERVALS + ":2") is None
    assert verifiers.change_intervals(hop_at(tmp_path), "d")[0]["window"]["source"][0] == 4


def test_untimed_changes_and_unreadable_target_are_never_equal(tmp_path):
    store = tailtables.Store.of(tmp_path)
    target = Target()
    batch = changes()
    for c in batch:
        c.pop("txn_ts")
    proof = verifiers.ChangeIntervals(target, "d", store, batch, 4)
    target.apply(batch)
    save(store, proof.observed(4), proof.clock, 1)
    assert verifiers.change_intervals(hop_at(tmp_path), "d")[0]["status"] == "warn"
    proof = verifiers.ChangeIntervals(target, "d", store, changes(), 4)
    target.neutral_rows_by_key = lambda *a: (_ for _ in ()).throw(RuntimeError())
    assert proof.observed(4)["windows"][0]["status"] == "pending"


def test_a_later_pending_receipt_cannot_hide_an_earlier_difference(tmp_path):
    store = tailtables.Store.of(tmp_path)
    target = Target()
    batch = changes()
    proof = verifiers.ChangeIntervals(target, "d", store, batch, 4)
    target.apply(batch)
    target.rows["public.t", 0] = (0, "bad")
    receipt = proof.observed(4)
    save(store, receipt, proof.clock, 1)
    pending = copy.deepcopy(receipt)
    pending["windows"][0].update(status="pending", target=None)
    save(store, pending, proof.clock, 2)
    assert verifiers.change_intervals(hop_at(tmp_path), "d")[0]["status"] == "diff"


@needs_docker
def test_postgresql_pair_tail_records_and_exposes_a_corrupt_window(pg, tmp_path, monkeypatch):
    from migkit.config import Hop
    from migkit.engines.postgres import PostgresEngine
    db = pg.name
    for server in (pg.src, pg.dst):
        server.sql("create table public.t (id int primary key, v text)", db=db)
    hop = Hop(name="window", engine="postgres", source=pg.src.endpoint(),
              target=pg.dst.endpoint(), databases=[db],
              options={"follow_until": "caught_up"})
    hop.report_dir = lambda db=None: tmp_path
    pair = PostgresEngine(hop)._as_pair()
    path = tmp_path / tailctl.TOKEN
    pair.tail_start(db, path)
    pg.src.sql("insert into public.t values (1, 'one'), (2, 'two')", db=db)
    apply = pair.dst_engine.neutral_apply

    def corrupt(side, database, batch):
        n = apply(side, database, batch)
        pg.dst.sql("update public.t set v = 'bad' where id = 1", db=db)
        return n

    monkeypatch.setattr(pair.dst_engine, "neutral_apply", corrupt)
    pair.tail_apply(db, True, path, lambda line: None)
    out = verifiers.change_intervals(hop, db)
    differences = [r for r in out if r["status"] == "diff"]
    assert differences and differences[0]["table"] == "public.t"
    assert differences[0]["window"]["end"] > differences[0]["window"]["start"]
    assert verdict.decide(out).status == "different"


def test_interval_findings_keep_machine_readable_time_ranges():
    row = {"check": "data", "scope": "d.t [a,b)", "status": "diff",
           "category": "data.intervals", "window": {"start": 1, "end": 2,
                                                       "table": "t"}}
    finding = verdict._classify_finding(row)
    assert finding["window"] == row["window"]
    assert verdict.interval_status([1, 5], [1, 6], True) == "diff"
    assert verdict.interval_status([1, 5], [1, 5], False) == "warn"
    assert verdict.interval_status([1, 5], [1, 5], True) == "ok"


def test_equal_counts_keep_distinct_value_sums_and_name_the_affected_table(tmp_path):
    store = tailtables.Store.of(tmp_path)
    target = Target()
    batch = changes()
    other = changes()
    for c in other:
        c["table"] = "public.other"
    batch += other
    proof = verifiers.ChangeIntervals(target, "d", store, batch, 8)
    original = proof.receipt["windows"][0]["source"]
    changed = copy.deepcopy(batch)
    changed[0]["values"]["v"] = "different"
    different = verifiers.ChangeIntervals(target, "d", store, changed, 8)
    altered = different.receipt["windows"][0]["source"]
    assert original[0] == altered[0] == 4 and original[1] != altered[1]
    target.apply(batch)
    target.rows["public.other", 0] = (0, "bad")
    save(store, proof.observed(8), proof.clock, 1)
    out = verifiers.change_intervals(hop_at(tmp_path), "d")
    assert [r["table"] for r in out if r["status"] == "diff"] == ["public.other"]
