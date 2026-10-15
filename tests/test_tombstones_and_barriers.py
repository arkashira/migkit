import json
import time
import pytest

from migkit import barriers, marks, twoway
from migkit.barriers import (
    BatchSequenceLedger,
    MarkLadder,
    NeutralMark,
    SequenceTag,
    TargetWriteAttribution,
    TombstoneLedger,
)


def test_tombstone_ledger_records_and_prevents_resurrection(tmp_path):
    storage = str(tmp_path / "tombstones.json")
    ledger = TombstoneLedger(storage_path=storage)
    
    key = {"id": 42}
    ledger.record("users", key, version=5, timestamp=100.0, deleted_by="migkit")
    assert ledger.count() == 1
    assert ledger.is_tombstoned("users", key, version=4)
    assert ledger.is_tombstoned("users", key, version=5)
    assert not ledger.is_tombstoned("users", key, version=6)
    
    assert ledger.prevent_resurrection("users", key, "update", incoming_version=4)
    assert ledger.prevent_resurrection("users", key, "insert", incoming_version=5)
    assert not ledger.prevent_resurrection("users", key, "insert", incoming_version=7)
    assert not ledger.is_tombstoned("users", key)
    assert ledger.count() == 0


def test_tombstone_ledger_persistence(tmp_path):
    storage = str(tmp_path / "tombstones.json")
    ledger1 = TombstoneLedger(storage_path=storage)
    ledger1.record("orders", {"order_id": "ORD-1"}, version=10, timestamp=150.0)
    ledger1.record("items", {"item_id": 99}, version=2, timestamp=120.0)
    
    ledger2 = TombstoneLedger(storage_path=storage)
    ledger2.load()
    assert ledger2.count() == 2
    assert ledger2.is_tombstoned("orders", {"order_id": "ORD-1"}, version=9)
    assert ledger2.is_tombstoned("items", {"item_id": 99}, version=2)
    
    pruned = ledger2.prune(before_timestamp=130.0)
    assert pruned == 1
    assert ledger2.count() == 1
    assert not ledger2.is_tombstoned("items", {"item_id": 99})
    assert ledger2.is_tombstoned("orders", {"order_id": "ORD-1"})


def test_tombstone_settle_batch_events():
    ledger = TombstoneLedger()
    key = {"id": 101}
    
    events_del = [(0, "op")]
    changes_del = [{"op": "delete", "table": "acct", "key": key}]
    res_del = ledger.settle_batch_events("acct", key, events_del, changes_del)
    assert res_del["status"] == "deleted"
    assert res_del["tombstoned"]
    assert ledger.is_tombstoned("acct", key)
    
    events_reinsert = [(0, "op"), (1, "came")]
    changes_reinsert = [
        {"op": "delete", "table": "acct", "key": key},
        {"op": "insert", "table": "acct", "key": key, "values": {"id": 101, "balance": 200}},
    ]
    res_re = ledger.settle_batch_events("acct", key, events_reinsert, changes_reinsert)
    assert res_re["status"] == "reinserted"
    assert not res_re["tombstoned"]
    assert res_re["values"]["balance"] == 200
    assert not ledger.is_tombstoned("acct", key)


def test_tombstone_integration_in_twoway_settle():
    ledger = TombstoneLedger()
    table_rules = twoway.TableRules(
        default=twoway.Rule("newest", by="v"),
        delete_vs_update="delete",
        delete_missing="ignore",
    )
    classes = {"id": "integer", "v": "integer", "name": "text"}
    key = {"id": 1}
    ranks = (2, 1)

    ledger.record("users", key, version=10)
    assert ledger.is_tombstoned("users", key, version=8)

    settled_old = twoway.settle(
        table_rules, classes, ranks,
        made=False,
        before={"id": 1, "v": 7, "name": "old"},
        after={"id": 1, "v": 8, "name": "resurrect_attempt"},
        here=None,
        keys={"id"},
        tombstones=ledger,
        table="users",
        key=key,
    )
    assert settled_old.outcome == "keep"
    assert settled_old.decision == "tombstone"
    assert settled_old.kind == "tombstone_suppressed"


def test_neutral_mark_across_engine_atomic_units():
    engines = ["mongodb", "cassandra", "redis", "dynamodb", "kafka", "postgres", "mysql", "mssql"]
    for eng in engines:
        marker = NeutralMark.format_marker(eng, batch=10, token="tok123", run_id="runA")
        assert marker is not None
        
        injected = NeutralMark.inject_mark(eng, ["op1", "op2"], batch=10, token="tok123")
        assert len(injected) >= 2
        
        if eng == "mongodb":
            assert NeutralMark.MARKER_KEY in injected[0]
            assert NeutralMark.is_migkit_write(eng, injected[0])
            assert not NeutralMark.is_migkit_write(eng, {"user": "app", "action": "insert"})
            extracted = NeutralMark.extract_marker(eng, injected)
            assert extracted["batch"] == 10
            assert extracted["token"] == "tok123"
        elif eng == "kafka":
            headers = injected[0][0]
            assert NeutralMark.is_migkit_write(eng, headers)
            extracted = NeutralMark.extract_marker(eng, headers)
            assert extracted["writer"] == "migkit"
        elif eng == "redis":
            assert NeutralMark.is_migkit_write(eng, injected[0])
        elif eng == "cassandra":
            assert NeutralMark.is_migkit_write(eng, injected[0])
        elif eng == "dynamodb":
            assert NeutralMark.is_migkit_write(eng, injected[0])
        elif eng in ("postgres", "mysql", "mssql"):
            assert NeutralMark.is_migkit_write(eng, injected[0])


def test_exactly_once_batch_sequence_ledger_and_replay_rejection(tmp_path):
    storage = str(tmp_path / "batches.json")
    ledger = BatchSequenceLedger(storage_path=storage)
    
    tag1 = SequenceTag(epoch=1, batch_num=1, run_id="run1", token="tok1")
    tag2 = SequenceTag(epoch=1, batch_num=2, run_id="run1", token="tok2")
    
    assert not ledger.is_replay(tag1)
    ledger.commit_batch(tag1)
    assert ledger.last_committed(epoch=1) == 1
    assert ledger.is_replay(tag1)
    
    with pytest.raises(ValueError, match="BAR-2 replay rejected"):
        ledger.commit_batch(tag1)
        
    assert ledger.check_and_advance(tag2, strict_monotonic=True)
    assert ledger.last_committed(epoch=1) == 2
    
    gap_tag = SequenceTag(epoch=1, batch_num=5, run_id="run1")
    with pytest.raises(ValueError, match="BAR-2 sequence gap"):
        ledger.check_and_advance(gap_tag, strict_monotonic=True)
        
    loaded = BatchSequenceLedger(storage_path=storage)
    loaded.load()
    assert loaded.last_committed(epoch=1) == 2
    assert loaded.is_replay(tag1)
    assert loaded.is_replay(tag2)


def test_target_write_attribution_and_protection():
    class DummyHop:
        options = {"protect_target": True}
        
    hop = DummyHop()
    assert TargetWriteAttribution.is_target_protected(hop)
    
    migkit_write_mongo = {"_migkit_marker": {"writer": "migkit", "batch": 5}}
    app_write_mongo = {"account": "user123", "amount": 500}
    
    assert TargetWriteAttribution.attribute_write("mongodb", migkit_write_mongo) == "migkit"
    assert TargetWriteAttribution.attribute_write("mongodb", app_write_mongo) == "application"
    
    audit = TargetWriteAttribution.audit_target_writes(
        "mongodb",
        [migkit_write_mongo, app_write_mongo],
        migkit_account="migkit_user",
        protected=True,
    )
    assert audit["protected"]
    assert audit["drift_detected"]
    assert audit["migkit_count"] == 1
    assert audit["application_count"] == 1
    assert audit["application_writes"] == [app_write_mongo]

    clean_audit = TargetWriteAttribution.audit_target_writes(
        "mongodb",
        [migkit_write_mongo],
        protected=True,
    )
    assert not clean_audit["drift_detected"]
    assert clean_audit["application_count"] == 0


def test_two_way_mark_ladder_mariadb_gtid_domain():
    facts_with_domain = {
        "family": "mysql",
        "mariadb": True,
        "gtid_domain": True,
        "can_create": True,
    }
    ranked, dropped = marks.choose_rung(None, facts_with_domain)
    names = [r.name for r in ranked]
    assert "gtid_domain" in names
    assert names[0] == "gtid_domain"
    assert marks.rung("mysql", "gtid_domain") is not None
    assert marks.rung("mysql", "gtid_domain").gives == frozenset({"apart", "exact"})
    
    facts_exact = dict(facts_with_domain, exact=True)
    ranked_exact, dropped_exact = marks.choose_rung(None, facts_exact)
    exact_names = [r.name for r in ranked_exact]
    assert "gtid_domain" in exact_names
    assert "table" in exact_names
    assert "skip_flag" not in exact_names
    assert any(r.name == "skip_flag" for r, _ in dropped_exact)
    
    facts_no_domain = {
        "family": "mysql",
        "mariadb": True,
        "gtid_domain": False,
        "can_create": True,
    }
    ranked_no_dom, dropped_no_dom = marks.choose_rung(None, facts_no_domain)
    no_dom_names = [r.name for r in ranked_no_dom]
    assert "gtid_domain" not in no_dom_names
    assert any(r.name == "gtid_domain" for r, _ in dropped_no_dom)


def test_two_way_mark_ladder_costs_and_doctor_said():
    cost_dom = MarkLadder.get_cost("mariadb", "gtid_domain")
    cost_skip = MarkLadder.get_cost("mariadb", "skip_flag")
    cost_table = MarkLadder.get_cost("mariadb", "table")
    assert cost_dom == 320.0
    assert cost_skip == 389.0
    assert cost_table == 764.0
    assert cost_dom < cost_skip < cost_table
    
    class DummySideEngine:
        CANON_ENGINE = "mysql"
        def mark_facts(self, side, db):
            return {"family": "mysql", "mariadb": True, "gtid_domain": True, "can_create": True}
            
    doc = MarkLadder.doctor_said(DummySideEngine(), "/nonexistent/token.json", "app")
    assert "gtid_domain" in doc or "GTID domain" in doc


def test_postgres_and_mysql_mark_ladders():
    pg_facts = {"family": "postgres", "origin_grants": True, "version": 160000, "can_create": True}
    pg_ranked, _ = marks.choose_rung(None, pg_facts)
    pg_names = [r.name for r in pg_ranked]
    assert "message" in pg_names
    assert "table" in pg_names
    assert "origin" in pg_names
    
    my_facts = {
        "family": "mysql",
        "mariadb": False,
        "version": (8, 4),
        "gtid_mode": "ON",
        "grants": {"TRANSACTION_GTID_TAG", "SESSION_VARIABLES_ADMIN"},
        "rows_query": True,
        "can_create": True,
    }
    my_ranked, _ = marks.choose_rung(None, my_facts)
    my_names = [r.name for r in my_ranked]
    assert "gtid_tag" in my_names
    assert "comment" in my_names
    assert "table" in my_names


def test_batch_sequence_ledger_multi_epoch():
    ledger = BatchSequenceLedger()
    e1_t1 = SequenceTag(epoch=1, batch_num=10)
    e2_t1 = SequenceTag(epoch=2, batch_num=10)
    
    ledger.commit_batch(e1_t1)
    assert ledger.last_committed(epoch=1) == 10
    assert ledger.last_committed(epoch=2) == -1
    
    assert not ledger.is_replay(e2_t1)
    ledger.commit_batch(e2_t1)
    assert ledger.last_committed(epoch=2) == 10
    assert ledger.is_replay(e2_t1)
