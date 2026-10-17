import sqlite3
from pathlib import Path
import pytest

from migkit.effect_checkpoint import (
    AtomicEffectCheckpointer,
    CheckpointReconstructionError,
    CheckpointState,
    EngineSemantics,
    KeylessEffect,
    MetadataOwnershipContract,
    OwnershipViolationError,
    RetentionContract,
    RetentionExpiredError,
    decide_effect_semantics,
    decide_retention_contract,
    effect_semantics_rungs,
    retention_contract_rungs,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ('effect_checkpoint', 'effect_semantics_rungs', 'transactional_engine_semantics', 'holds'),
    ('effect_checkpoint', 'effect_semantics_rungs', 'transactional_engine_semantics', 'refuses'),
    ('effect_checkpoint', 'effect_semantics_rungs', 'at_least_once_engine_semantics', 'holds'),
    ('effect_checkpoint', 'effect_semantics_rungs', 'at_least_once_engine_semantics', 'refuses'),
    ('effect_checkpoint', 'effect_semantics_rungs', 'best_effort_engine_semantics', 'holds'),
    ('effect_checkpoint', 'retention_contract_rungs', 'configured_retention_bounds', 'holds'),
    ('effect_checkpoint', 'retention_contract_rungs', 'configured_retention_bounds', 'refuses'),
    ('effect_checkpoint', 'retention_contract_rungs', 'default_retention_bounds', 'holds'),
]


def _init_sqlite_db(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE access_log (ip TEXT, path TEXT, status INT)")
    con.commit()
    return con


def test_atomic_binding_keyless_insert_and_dedup(tmp_path):
    db_path = tmp_path / "target.db"
    con = _init_sqlite_db(db_path)
    checkpointer = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con,
        stream_id="stream-1",
        table="access_log",
    )
    checkpointer.ensure_metadata_tables()

    effect = KeylessEffect(
        effect_id="evt-100",
        table="access_log",
        operation="insert",
        row_data=("10.0.0.1", "/api/v1/resource", 200),
        position=100,
    )

    applied = checkpointer.bind_atomic_effect(effect)
    assert applied is True
    assert checkpointer.is_effect_applied("evt-100") is True

    rows = con.execute("SELECT ip, path, status FROM access_log").fetchall()
    assert len(rows) == 1
    assert rows[0] == ("10.0.0.1", "/api/v1/resource", 200)

    applied_again = checkpointer.bind_atomic_effect(effect)
    assert applied_again is False

    rows2 = con.execute("SELECT ip, path, status FROM access_log").fetchall()
    assert len(rows2) == 1
    con.close()


def test_crash_after_target_effect_before_ack(tmp_path):
    db_path = tmp_path / "target.db"
    con = _init_sqlite_db(db_path)
    checkpointer = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con,
        stream_id="stream-1",
        table="access_log",
    )
    checkpointer.ensure_metadata_tables()

    effect = KeylessEffect(
        effect_id="evt-101",
        table="access_log",
        operation="insert",
        row_data=("10.0.0.2", "/login", 401),
        position=101,
    )

    with pytest.raises(RuntimeError, match="Simulated crash before ACK"):
        checkpointer.simulate_crash_before_ack(effect)

    con.close()

    con_resumed = sqlite3.connect(str(db_path))
    resumed_checkpointer = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con_resumed,
        stream_id="stream-1",
        table="access_log",
    )
    resumed_checkpointer.ensure_metadata_tables()

    replayed = resumed_checkpointer.bind_atomic_effect(effect)
    assert replayed is False

    rows = con_resumed.execute("SELECT ip, path, status FROM access_log").fetchall()
    assert len(rows) == 1
    assert rows[0] == ("10.0.0.2", "/login", 401)
    con_resumed.close()


def test_crash_during_transaction_rollback(tmp_path):
    db_path = tmp_path / "target.db"
    con = _init_sqlite_db(db_path)
    checkpointer = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con,
        stream_id="stream-1",
        table="access_log",
    )
    checkpointer.ensure_metadata_tables()

    effect = KeylessEffect(
        effect_id="evt-102",
        table="access_log",
        operation="insert",
        row_data=("10.0.0.3", "/logout", 200),
        position=102,
    )

    with pytest.raises(RuntimeError, match="Simulated crash during write transaction"):
        checkpointer.simulate_crash_during_transaction(effect)

    assert checkpointer.is_effect_applied("evt-102") is False
    rows_after_abort = con.execute("SELECT count(*) FROM access_log").fetchone()[0]
    assert rows_after_abort == 0

    applied = checkpointer.bind_atomic_effect(effect)
    assert applied is True
    assert con.execute("SELECT count(*) FROM access_log").fetchone()[0] == 1
    con.close()


def test_keyless_duplicate_rows_handled_correctly(tmp_path):
    db_path = tmp_path / "target.db"
    con = _init_sqlite_db(db_path)
    checkpointer = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con,
        stream_id="stream-1",
        table="access_log",
    )
    checkpointer.ensure_metadata_tables()

    effect1 = KeylessEffect(
        effect_id="evt-201",
        table="access_log",
        operation="insert",
        row_data=("10.0.0.9", "/ping", 200),
        position=201,
    )
    effect2 = KeylessEffect(
        effect_id="evt-202",
        table="access_log",
        operation="insert",
        row_data=("10.0.0.9", "/ping", 200),
        position=202,
    )
    effect3 = KeylessEffect(
        effect_id="evt-203",
        table="access_log",
        operation="insert",
        row_data=("10.0.0.9", "/ping", 200),
        position=203,
    )

    batch_res = checkpointer.bind_atomic_batch([effect1, effect2, effect3])
    assert batch_res["applied_count"] == 3
    assert batch_res["skipped_count"] == 0

    rows = con.execute("SELECT ip, path, status FROM access_log").fetchall()
    assert len(rows) == 3
    assert all(r == ("10.0.0.9", "/ping", 200) for r in rows)

    replay_res = checkpointer.bind_atomic_batch([effect1, effect2, effect3])
    assert replay_res["applied_count"] == 0
    assert replay_res["skipped_count"] == 3

    rows_after_replay = con.execute("SELECT count(*) FROM access_log").fetchone()[0]
    assert rows_after_replay == 3
    con.close()


def test_crash_at_checkpoint_boundary_for_keyless_duplicate_rows(tmp_path):
    db_path = tmp_path / "target.db"
    con = _init_sqlite_db(db_path)
    checkpointer = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con,
        stream_id="stream-boundary",
        table="access_log",
    )
    checkpointer.ensure_metadata_tables()

    identical_payload = ("10.0.0.42", "/webhook", 204)
    batch_1 = [
        KeylessEffect("b1-1", "access_log", "insert", identical_payload, position=501),
        KeylessEffect("b1-2", "access_log", "insert", identical_payload, position=502),
    ]
    checkpointer.bind_atomic_batch(batch_1, checkpoint_boundary=True)

    state1 = checkpointer.reconstruct_checkpoint()
    assert state1.last_committed_position == "502"
    assert con.execute("SELECT count(*) FROM access_log").fetchone()[0] == 2

    batch_2 = [
        KeylessEffect("b2-1", "access_log", "insert", identical_payload, position=503),
        KeylessEffect("b2-2", "access_log", "insert", identical_payload, position=504),
    ]
    with pytest.raises(RuntimeError, match="Crash at boundary"):
        checkpointer.bind_atomic_batch(batch_2[:1], checkpoint_boundary=False)
        raise RuntimeError("Crash at boundary")

    con.close()

    con_resumed = sqlite3.connect(str(db_path))
    resumed = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con_resumed,
        stream_id="stream-boundary",
        table="access_log",
    )
    resumed.ensure_metadata_tables()

    state_resumed = resumed.reconstruct_checkpoint()
    assert state_resumed.last_committed_position == "502"
    assert con_resumed.execute("SELECT count(*) FROM access_log").fetchone()[0] == 2

    resumed.bind_atomic_batch(batch_2, checkpoint_boundary=True)
    state_final = resumed.reconstruct_checkpoint()
    assert state_final.last_committed_position == "504"
    assert con_resumed.execute("SELECT count(*) FROM access_log").fetchone()[0] == 4

    resumed.bind_atomic_batch(batch_1 + batch_2, checkpoint_boundary=True)
    assert con_resumed.execute("SELECT count(*) FROM access_log").fetchone()[0] == 4
    con_resumed.close()


def test_checkpoint_boundaries_across_batches(tmp_path):
    db_path = tmp_path / "target.db"
    con = _init_sqlite_db(db_path)
    checkpointer = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con,
        stream_id="stream-1",
        table="access_log",
    )
    checkpointer.ensure_metadata_tables()

    batch_a = [
        KeylessEffect("b1-1", "access_log", "insert", ("10.0.0.1", "/a", 200), position=301),
        KeylessEffect("b1-2", "access_log", "insert", ("10.0.0.2", "/b", 200), position=302),
    ]
    checkpointer.bind_atomic_batch(batch_a, checkpoint_boundary=True)

    state = checkpointer.reconstruct_checkpoint()
    assert state.last_committed_position == "302"
    assert state.total_effects_applied == 2

    batch_b = [
        KeylessEffect("b2-1", "access_log", "insert", ("10.0.0.3", "/c", 200), position=303),
        KeylessEffect("b2-2", "access_log", "insert", ("10.0.0.4", "/d", 200), position=304),
    ]
    checkpointer.bind_atomic_batch(batch_b, checkpoint_boundary=True)

    state_after = checkpointer.reconstruct_checkpoint()
    assert state_after.last_committed_position == "304"
    assert state_after.total_effects_applied == 4
    assert con.execute("SELECT count(*) FROM access_log").fetchone()[0] == 4
    con.close()


def test_metadata_ownership_enforcement(tmp_path):
    db_path = tmp_path / "target.db"
    con = _init_sqlite_db(db_path)

    contract = MetadataOwnershipContract(owner="migkit_engine", schema="main", table_name="__migkit_effects")
    checkpointer = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con,
        stream_id="stream-sec",
        table="access_log",
        ownership=contract,
    )
    checkpointer.ensure_metadata_tables()

    tampered_contract = MetadataOwnershipContract(owner="rogue_agent", schema="main", table_name="__migkit_effects")
    tampered_checkpointer = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con,
        stream_id="stream-sec",
        table="access_log",
        ownership=tampered_contract,
    )
    with pytest.raises(OwnershipViolationError, match="Metadata ownership violation"):
        tampered_checkpointer.ensure_metadata_tables()

    con.close()


def test_retention_contract_pruning_and_guard(tmp_path):
    db_path = tmp_path / "target.db"
    con = _init_sqlite_db(db_path)

    retention = RetentionContract(max_retained_entries=5, min_retained_position=100)
    checkpointer = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con,
        stream_id="stream-ret",
        table="access_log",
        retention=retention,
    )
    checkpointer.ensure_metadata_tables()

    effects = [
        KeylessEffect(f"r-{pos}", "access_log", "insert", ("10.0.0.5", f"/{pos}", 200), position=pos)
        for pos in range(100, 110)
    ]
    checkpointer.bind_atomic_batch(effects)

    assert checkpointer.is_effect_applied("r-100") is True

    deleted = checkpointer.trim_retention(watermark_position=105)
    assert deleted >= 5

    expired_effect = KeylessEffect("r-099", "access_log", "insert", ("10.0.0.5", "/99", 200), position=99)
    with pytest.raises(RetentionExpiredError, match="Position 99 is below minimum retained position 100"):
        checkpointer.bind_atomic_effect(expired_effect)

    con.close()


def test_reconstruct_checkpoint_without_treating_absence_as_fresh_start(tmp_path):
    db_path = tmp_path / "target.db"
    con = _init_sqlite_db(db_path)

    con.execute("INSERT INTO access_log VALUES ('10.0.0.99', '/legacy', 200)")
    con.commit()

    checkpointer = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con,
        stream_id="stream-orphan",
        table="access_log",
    )

    with pytest.raises(CheckpointReconstructionError, match="Checkpoint metadata absent but target table contains existing data"):
        checkpointer.reconstruct_checkpoint()

    state = checkpointer.reconstruct_checkpoint(allow_fresh_start=True)
    assert isinstance(state, CheckpointState)
    assert state.last_committed_position is None

    con.close()


def test_allow_fresh_start_when_target_empty(tmp_path):
    db_path = tmp_path / "target.db"
    con = _init_sqlite_db(db_path)

    checkpointer = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con,
        stream_id="stream-fresh",
        table="access_log",
    )
    state = checkpointer.reconstruct_checkpoint()
    assert state.last_committed_position is None
    assert state.total_effects_applied == 0
    con.close()


def test_explicit_weaker_semantics_declaration():
    class DummyTarget:
        def __init__(self):
            self.rows = []

        def execute(self, sql, *args):
            pass

    dummy = DummyTarget()
    checkpointer = AtomicEffectCheckpointer(
        engine="clickhouse",
        connection=dummy,
        stream_id="ch-stream",
        table="access_log",
    )
    assert checkpointer.declared_semantics == EngineSemantics.AT_LEAST_ONCE

    effects = [
        KeylessEffect("ch-1", "access_log", "insert", ("10.0.0.1", "/ch", 200), position=501),
        KeylessEffect("ch-2", "access_log", "insert", ("10.0.0.2", "/ch", 200), position=502),
    ]

    checkpointer.bind_atomic_batch(effects)
    assert checkpointer.is_effect_applied("ch-1") is True

    recon = checkpointer.reconcile_weaker_semantics(
        unacked_effects=effects,
        target_reader=lambda: [("10.0.0.1", "/ch", 200), ("10.0.0.2", "/ch", 200)],
    )
    assert recon["status"] == "reconciled"
    assert recon["semantics"] == EngineSemantics.AT_LEAST_ONCE
    assert recon["discrepancies"] == 0


def test_keyless_effect_dedup_identity_determinism():
    e1 = KeylessEffect(
        effect_id="eff-abc",
        table="access_log",
        operation="INSERT",
        row_data={"status": 200, "ip": "10.0.0.7", "path": "/test"},
        position=999,
    )
    e2 = KeylessEffect(
        effect_id="eff-abc",
        table="access_log",
        operation="insert",
        row_data={"path": "/test", "status": 200, "ip": "10.0.0.7"},
        position=999,
    )
    assert e1.dedup_identity() == e2.dedup_identity()

    e_diff_pos = KeylessEffect(
        effect_id="eff-abc",
        table="access_log",
        operation="insert",
        row_data={"status": 200, "ip": "10.0.0.7", "path": "/test"},
        position=1000,
    )
    assert e1.dedup_identity() != e_diff_pos.dedup_identity()


def test_interleaved_streams_dedup_isolation(tmp_path):
    db_path = tmp_path / "target.db"
    con = _init_sqlite_db(db_path)

    cp_a = AtomicEffectCheckpointer(engine="sqlite", connection=con, stream_id="stream-a", table="access_log")
    cp_b = AtomicEffectCheckpointer(engine="sqlite", connection=con, stream_id="stream-b", table="access_log")
    cp_a.ensure_metadata_tables()
    cp_b.ensure_metadata_tables()

    e_a = KeylessEffect("evt-shared", "access_log", "insert", ("10.0.0.8", "/stream_a", 200), position=10)
    e_b = KeylessEffect("evt-shared", "access_log", "insert", ("10.0.0.8", "/stream_b", 200), position=10)

    applied_a = cp_a.bind_atomic_effect(e_a)
    applied_b = cp_b.bind_atomic_effect(e_b)

    assert applied_a is True
    assert applied_b is True

    rows = con.execute("SELECT path FROM access_log ORDER BY path").fetchall()
    assert [r[0] for r in rows] == ["/stream_a", "/stream_b"]

    assert cp_a.bind_atomic_effect(e_a) is False
    assert cp_b.bind_atomic_effect(e_b) is False
    con.close()


def test_effect_checkpoint_decide_rungs():
    with watched() as seen:
        s1 = decide_effect_semantics("postgres")
        assert s1 == EngineSemantics.ATOMIC_TRANSACTION

        s2 = decide_effect_semantics("clickhouse")
        assert s2 == EngineSemantics.AT_LEAST_ONCE

        s3 = decide_effect_semantics("discard")
        assert s3 == EngineSemantics.BEST_EFFORT

        r1 = decide_retention_contract(options={"effect_retention_max": 50000})
        assert r1.max_retained_entries == 50000

        r2 = decide_retention_contract()
        assert r2.max_retained_entries == 100000

    assert set(branch(seen, "transactional_engine_semantics")) == {HOLDS, REFUSES}
    assert set(branch(seen, "at_least_once_engine_semantics")) == {HOLDS, REFUSES}
    assert branch(seen, "best_effort_engine_semantics") == [HOLDS]
    assert set(branch(seen, "configured_retention_bounds")) == {HOLDS, REFUSES}
    assert branch(seen, "default_retention_bounds") == [HOLDS]


def test_non_dbapi_write_error_unswallowed():
    from migkit.effect_checkpoint import EffectCheckpointError

    class FailingConnection:
        def execute(self, sql, params):
            raise sqlite3.OperationalError("disk I/O error")

    cp = AtomicEffectCheckpointer(
        engine="weaker_custom",
        connection=FailingConnection(),
        declared_semantics=EngineSemantics.AT_LEAST_ONCE,
    )
    effect = KeylessEffect("e1", "tbl", "insert", ("val",), position=1)
    with pytest.raises(EffectCheckpointError, match="disk I/O error"):
        cp.bind_atomic_effect(effect)


def test_tailctl_apply_unit_with_effect_checkpointer(tmp_path):
    from migkit import tailctl
    db_path = tmp_path / "tailctl_keyless.db"
    con = _init_sqlite_db(db_path)
    cp = AtomicEffectCheckpointer(
        engine="sqlite",
        connection=con,
        stream_id="tail_stream",
        table="access_log",
    )
    cp.ensure_metadata_tables()

    class FakeEngine:
        def __init__(self, cp):
            self.effect_checkpointer = cp
            self.name = "sqlite"

        def apply_columns(self, side, db, cols):
            return 1

    eng = FakeEngine(cp)
    changes = [
        {"table": "access_log", "type": "insert", "row": {"ip": "1.2.3.4", "path": "/p", "status": 200}},
    ]
    applied_count = []
    tailctl.apply_unit(eng, "main", 500, changes, lambda c: applied_count.append(len(c)) or len(c))
    assert cp.is_effect_applied("main:access_log:500:0") is True
    con.close()

