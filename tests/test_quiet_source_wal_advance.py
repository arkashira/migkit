import pytest
from migkit import quiet_source


def test_lsn_utilities():
    assert quiet_source.parse_lsn("0/16B374D") == 0x16B374D
    assert quiet_source.parse_lsn("1/0") == 0x100000000
    assert quiet_source.parse_lsn(100) == 100
    assert quiet_source.format_lsn(0x16B374D) == "0/16B374D"
    assert quiet_source.format_lsn(0x100000000) == "1/0"
    assert quiet_source.lsn_diff("0/2000", "0/1000") == 0x1000
    assert quiet_source.lsn_diff("0/1000", "0/2000") == -0x1000


def test_quiet_source_manager_initial_state():
    mgr = quiet_source.QuietSourceManager(
        slot_name="test_slot",
        in_scope_tables=["public.orders", "public.users"],
        start_lsn="0/1000",
    )
    prog = mgr.get_progress()
    assert prog.slot_confirmed_lsn == "0/1000"
    assert prog.safe_retention_lsn == "0/1000"
    assert prog.target_applied_lsn == "0/1000"
    assert prog.last_business_lsn is None
    assert prog.target_applied_business_writes == 0
    assert prog.filtered_tx_count == 0
    assert prog.heartbeat_count == 0
    assert prog.wal_lag_bytes == 0


def test_business_event_progress_tracking():
    mgr = quiet_source.QuietSourceManager(
        slot_name="test_slot",
        in_scope_tables=["public.orders"],
        start_lsn="0/1000",
    )
    ev = mgr.ingest_change(table="public.orders", lsn="0/2000", tx_id="tx_1")
    assert ev.kind == quiet_source.EVENT_BUSINESS
    assert mgr.get_progress().last_business_lsn == "0/2000"
    assert mgr.get_progress().safe_retention_lsn == "0/1000"

    res = mgr.advance_retention()
    assert not res.advanced
    assert mgr.get_progress().slot_confirmed_lsn == "0/1000"

    mgr.record_target_applied(lsn="0/2000", business_writes_count=1)
    prog = mgr.get_progress()
    assert prog.target_applied_lsn == "0/2000"
    assert prog.target_applied_business_writes == 1
    assert prog.safe_retention_lsn == "0/2000"

    res2 = mgr.advance_retention()
    assert res2.advanced
    assert res2.method == quiet_source.METHOD_READ_ONLY_ACK
    assert res2.business_writes_claimed == 0
    assert mgr.get_progress().slot_confirmed_lsn == "0/2000"


def test_filtered_event_advances_retention_without_false_business_claims():
    mgr = quiet_source.QuietSourceManager(
        slot_name="test_slot",
        in_scope_tables=["public.orders"],
        start_lsn="0/1000",
    )
    mgr.set_wal_head("0/5000000")

    ev1 = mgr.ingest_change(table="audit.log", lsn="0/2000000", tx_id="tx_audit_1")
    assert ev1.kind == quiet_source.EVENT_FILTERED
    ev2 = mgr.ingest_change(table="internal.temp", lsn="0/4000000", tx_id="tx_audit_2")
    assert ev2.kind == quiet_source.EVENT_FILTERED

    prog = mgr.get_progress()
    assert prog.filtered_tx_count == 2
    assert prog.target_applied_business_writes == 0
    assert prog.safe_retention_lsn == "0/4000000"
    assert prog.safe_to_advance_bytes == 0x4000000 - 0x1000

    res = mgr.advance_retention()
    assert res.advanced
    assert res.method == quiet_source.METHOD_READ_ONLY_ACK
    assert res.business_writes_claimed == 0
    assert res.lsn_before == "0/1000"
    assert res.lsn_after == "0/4000000"
    assert not res.audited

    prog2 = mgr.get_progress()
    assert prog2.slot_confirmed_lsn == "0/4000000"
    assert prog2.target_applied_business_writes == 0
    assert prog2.wal_lag_bytes == 0x5000000 - 0x4000000


def test_interleaved_business_and_filtered_transactions():
    mgr = quiet_source.QuietSourceManager(
        slot_name="test_slot",
        in_scope_tables=["public.users"],
        start_lsn="0/1000",
    )

    mgr.ingest_change(table="public.users", lsn="0/2000", tx_id="tx_u1")
    mgr.record_target_applied(lsn="0/2000", business_writes_count=1)
    mgr.advance_retention()
    assert mgr.get_progress().slot_confirmed_lsn == "0/2000"

    mgr.ingest_change(table="metrics.hourly", lsn="0/3000", tx_id="tx_m1")
    mgr.ingest_change(table="metrics.hourly", lsn="0/4000", tx_id="tx_m2")
    assert mgr.get_progress().safe_retention_lsn == "0/4000"

    mgr.ingest_change(table="public.users", lsn="0/5000", tx_id="tx_u2")
    assert mgr.get_progress().safe_retention_lsn == "0/4000"

    mgr.ingest_change(table="metrics.hourly", lsn="0/6000", tx_id="tx_m3")
    assert mgr.get_progress().safe_retention_lsn == "0/4000"

    res = mgr.advance_retention()
    assert res.advanced
    assert res.lsn_after == "0/4000"
    assert mgr.get_progress().target_applied_business_writes == 1

    mgr.record_target_applied(lsn="0/5000", business_writes_count=1)
    assert mgr.get_progress().safe_retention_lsn == "0/6000"

    res2 = mgr.advance_retention()
    assert res2.advanced
    assert res2.lsn_after == "0/6000"
    assert mgr.get_progress().target_applied_business_writes == 2


def test_prioritizes_read_only_methods_first():
    mgr = quiet_source.QuietSourceManager(
        slot_name="test_slot",
        in_scope_tables=["public.users"],
        start_lsn="0/1000",
        allow_source_writes=True,
    )
    mgr.set_wal_head("0/8000")

    method, target_lsn, reason = mgr.evaluate_advance_method()
    assert method == quiet_source.METHOD_READ_ONLY_PROBE
    assert target_lsn == "0/8000"
    assert reason == "source_wal_ahead_read_only"

    res = mgr.advance_retention()
    assert res.advanced
    assert res.method == quiet_source.METHOD_READ_ONLY_PROBE
    assert not res.audited
    assert len(mgr.get_audit_records()) == 0


def test_explicit_heartbeat_write_refused_when_writes_not_allowed():
    mgr = quiet_source.QuietSourceManager(
        slot_name="test_slot",
        in_scope_tables=["public.users"],
        start_lsn="0/1000",
        heartbeat_interval_seconds=5.0,
        allow_source_writes=False,
    )
    mgr.set_wal_head("0/1000")

    res = mgr.tick(now=100.0)
    assert res is None

    res = mgr.tick(now=106.0)
    assert res is not None
    assert not res.advanced
    assert res.refused
    assert "source_read_only" in res.refusal_reason
    assert len(mgr.get_audit_records()) == 0


def test_explicit_heartbeat_write_audited_when_allowed():
    executed_sql = []

    def fake_source_exec(sql):
        executed_sql.append(sql)
        return "0/2000"

    mgr = quiet_source.QuietSourceManager(
        slot_name="test_slot",
        in_scope_tables=["public.users"],
        start_lsn="0/1000",
        heartbeat_interval_seconds=5.0,
        allow_source_writes=True,
        source_executor=fake_source_exec,
    )
    mgr.set_wal_head("0/1000")

    res = mgr.tick(now=100.0)
    assert res is None

    res = mgr.tick(now=106.0)
    assert res is not None
    assert res.advanced
    assert res.method == quiet_source.METHOD_HEARTBEAT_WRITE
    assert res.business_writes_claimed == 0
    assert res.audited
    assert res.audit_record is not None
    assert res.lsn_after == "0/2000"
    assert mgr.get_progress().heartbeat_count == 1
    assert len(executed_sql) == 1
    assert "pg_logical_emit_message" in executed_sql[0]

    records = mgr.get_audit_records()
    assert len(records) == 1
    rec = records[0]
    assert rec.action == "heartbeat_write"
    assert rec.lsn_before == "0/1000"
    assert rec.lsn_after == "0/2000"
    assert rec.authorized is True


def test_transaction_boundary_grouping_filtered_and_business():
    mgr = quiet_source.QuietSourceManager(
        slot_name="test_slot",
        in_scope_tables=["public.orders"],
        start_lsn="0/1000",
    )

    mgr.begin_tx(tx_id="tx_filtered", lsn="0/1100")
    mgr.ingest_change(table="audit.history", lsn="0/1200", tx_id="tx_filtered")
    mgr.commit_tx(tx_id="tx_filtered", commit_lsn="0/1300")

    assert mgr.get_progress().safe_retention_lsn == "0/1300"
    assert mgr.get_progress().target_applied_business_writes == 0

    mgr.begin_tx(tx_id="tx_mixed", lsn="0/1400")
    mgr.ingest_change(table="audit.history", lsn="0/1500", tx_id="tx_mixed")
    mgr.ingest_change(table="public.orders", lsn="0/1600", tx_id="tx_mixed")
    mgr.commit_tx(tx_id="tx_mixed", commit_lsn="0/1700")

    assert mgr.get_progress().safe_retention_lsn == "0/1300"

    mgr.record_target_applied(lsn="0/1700", business_writes_count=1)
    assert mgr.get_progress().safe_retention_lsn == "0/1700"
    assert mgr.get_progress().target_applied_business_writes == 1


def test_callback_invocations_on_advance_and_audit():
    advanced_lsns = []
    audit_events = []

    def on_advance(slot, lsn):
        advanced_lsns.append((slot, lsn))

    def on_audit(rec):
        audit_events.append(rec)

    mgr = quiet_source.QuietSourceManager(
        slot_name="slot_cb",
        in_scope_tables=["public.users"],
        start_lsn="0/1000",
        slot_advance_fn=on_advance,
        audit_callback=on_audit,
        allow_source_writes=True,
        source_executor=lambda sql: "0/3000",
    )

    mgr.ingest_change(table="excluded.t", lsn="0/2000")
    mgr.advance_retention()
    assert advanced_lsns == [("slot_cb", "0/2000")]
    assert len(audit_events) == 0

    mgr.tick(now=100.0)
    mgr.tick(now=120.0)
    assert len(audit_events) == 1
    assert audit_events[0].action == "heartbeat_write"
    assert advanced_lsns[-1] == ("slot_cb", "0/3000")


def test_unbounded_wal_growth_prevention_on_noisy_excluded_tables():
    mgr = quiet_source.QuietSourceManager(
        slot_name="pg_slot_heavy",
        in_scope_tables=["public.payments"],
        start_lsn="0/1000",
        max_wal_lag_bytes=100_000,
    )

    cur = 0x1000
    for i in range(100):
        cur += 50_000
        mgr.ingest_change(table="audit.audit_trail", lsn=quiet_source.format_lsn(cur), tx_id=f"tx_{i}")

    prog = mgr.get_progress()
    assert prog.filtered_tx_count == 100
    assert prog.target_applied_business_writes == 0
    assert prog.wal_lag_bytes == 50_000 * 100
    assert prog.safe_to_advance_bytes == 50_000 * 100

    res = mgr.advance_retention()
    assert res.advanced
    assert res.bytes_reclaimed == 50_000 * 100
    assert res.business_writes_claimed == 0

    prog_after = mgr.get_progress()
    assert prog_after.slot_confirmed_lsn == quiet_source.format_lsn(cur)
    assert prog_after.wal_lag_bytes == 0
    assert prog_after.target_applied_business_writes == 0


def test_staged_retention_advancement_with_interleaved_business_events():
    mgr = quiet_source.QuietSourceManager(
        slot_name="staged_slot",
        in_scope_tables=["public.core"],
        start_lsn="0/1000",
    )

    mgr.ingest_change(table="public.core", lsn="0/2000")
    mgr.ingest_change(table="other.noise", lsn="0/3000")
    mgr.ingest_change(table="public.core", lsn="0/4000")
    mgr.ingest_change(table="other.noise", lsn="0/5000")
    mgr.ingest_change(table="other.noise", lsn="0/6000")

    assert mgr.get_progress().safe_retention_lsn == "0/1000"

    mgr.record_target_applied("0/2000", business_writes_count=1)
    assert mgr.get_progress().safe_retention_lsn == "0/3000"

    res = mgr.advance_retention()
    assert res.advanced
    assert res.lsn_after == "0/3000"
    assert res.business_writes_claimed == 0
    assert mgr.get_progress().target_applied_business_writes == 1

    mgr.record_target_applied("0/4000", business_writes_count=1)
    assert mgr.get_progress().safe_retention_lsn == "0/6000"

    res2 = mgr.advance_retention()
    assert res2.advanced
    assert res2.lsn_after == "0/6000"
    assert mgr.get_progress().target_applied_business_writes == 2


def test_mysql_engine_heartbeat_write_generation():
    executed = []

    def fake_mysql_exec(sql):
        executed.append(sql)
        return "0/5000"

    mgr = quiet_source.QuietSourceManager(
        slot_name="mysql_ch",
        engine="mysql",
        in_scope_tables=["app.users"],
        start_lsn="0/1000",
        heartbeat_interval_seconds=1.0,
        allow_source_writes=True,
        source_executor=fake_mysql_exec,
    )
    mgr.set_wal_head("0/1000")

    res = mgr.tick(now=10.0)
    assert res is None
    res = mgr.tick(now=15.0)
    assert res is not None
    assert res.advanced
    assert len(executed) == 1
    assert "REPLACE INTO migkit_heartbeat" in executed[0]
    assert res.audit_record.engine == "mysql"


def test_advance_when_already_caught_up_is_safe_noop():
    mgr = quiet_source.QuietSourceManager(
        slot_name="caught_up_slot",
        in_scope_tables=["public.users"],
        start_lsn="0/1000",
    )
    res = mgr.advance_retention()
    assert not res.advanced
    assert res.bytes_reclaimed == 0
    assert res.business_writes_claimed == 0

