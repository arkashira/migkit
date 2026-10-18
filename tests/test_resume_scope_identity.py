import sqlite3
from pathlib import Path
import pytest

from migkit.resume_scope import (
    IncompatibleResumeScopeError,
    ResumeScope,
    DurableUnitIdentity,
    ScopedCheckpoint,
    ResumeScopeRunner,
    decide_scope_compatibility,
)
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("resume_scope", "resume_scope_compatibility_rungs", "admit_identical_scope", "holds"),
    ("resume_scope", "resume_scope_compatibility_rungs", "admit_identical_scope", "refuses"),
    ("resume_scope", "resume_scope_compatibility_rungs", "refuse_incompatible_scope", "holds"),
]


def test_scope_header_persists_identity_filter_mapping_tenant():
    scope = ResumeScope(
        tables=["orders", "payments"],
        filter_predicate="status = 'active' AND amount > 100",
        column_mapping={"id": "order_id", "amount": "total", "tenant_id": "tenant"},
        tenant_predicate="tenant_id = 'org_42'",
    )
    hdr = scope.header()
    assert hdr["format_version"] == 1
    assert hdr["tables"] == ["orders", "payments"]
    assert hdr["filter_predicate"] == "status = 'active' AND amount > 100"
    assert hdr["column_mapping"] == {"amount": "total", "id": "order_id", "tenant_id": "tenant"}
    assert hdr["tenant_predicate"] == "tenant_id = 'org_42'"
    assert hdr["scope_identity"] == scope.scope_identity
    assert len(scope.scope_identity) >= 16

    unit = scope.unit_identity("orders", lo=1, hi=100)
    assert isinstance(unit, DurableUnitIdentity)
    assert unit.table == "orders"
    assert unit.lo == 1
    assert unit.hi == 100
    assert unit.filter_predicate == scope.filter_predicate
    assert unit.column_mapping == scope.column_mapping
    assert unit.tenant_predicate == scope.tenant_predicate
    assert unit.scope_identity == scope.scope_identity
    assert len(unit.unit_id) > 0


def test_checkpoint_persists_scope_header_and_durable_unit_identity(tmp_path):
    cp_path = tmp_path / "checkpoint.json"
    scope = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'active'",
        column_mapping={"id": "order_id", "tenant_id": "org"},
        tenant_predicate="tenant_id = 't1'",
    )
    cp = ScopedCheckpoint(cp_path, scope)
    ranges = [(None, 50), (50, 100), (100, None)]
    todo = cp.begin("orders", "md5(order_id)", ranges)
    assert len(todo) == 3

    cp.record("orders", None, 50, rows=10, checksum="12345")

    loaded_cp = ScopedCheckpoint(cp_path, scope)
    assert loaded_cp.resumed("orders") == 1
    remaining = loaded_cp.begin("orders", "md5(order_id)", ranges)
    assert len(remaining) == 2
    assert remaining == [(50, 100), (100, None)]

    raw_data = loaded_cp.raw_data()
    assert "scope_header" in raw_data
    hdr = raw_data["scope_header"]
    assert hdr["scope_identity"] == scope.scope_identity
    assert hdr["filter_predicate"] == "status = 'active'"
    assert hdr["tenant_predicate"] == "tenant_id = 't1'"

    units = raw_data.get("units", {})
    assert len(units) == 1
    unit_entry = next(iter(units.values()))
    assert unit_entry["unit"]["table"] == "orders"
    assert unit_entry["unit"]["filter_predicate"] == "status = 'active'"
    assert unit_entry["unit"]["tenant_predicate"] == "tenant_id = 't1'"
    assert unit_entry["unit"]["scope_identity"] == scope.scope_identity


def test_reject_incompatible_checkpoint_reuse_on_filter_change(tmp_path):
    cp_path = tmp_path / "checkpoint.json"
    scope1 = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'active' AND amount > 100",
        column_mapping={"id": "order_id"},
        tenant_predicate="tenant_id = 't1'",
    )
    cp = ScopedCheckpoint(cp_path, scope1)
    cp.begin("orders", "md5(id)", [(1, 100)])
    cp.record("orders", 1, 100, 10, "111")

    scope2 = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'active' AND amount > 200",
        column_mapping={"id": "order_id"},
        tenant_predicate="tenant_id = 't1'",
    )
    with pytest.raises(IncompatibleResumeScopeError) as exc:
        ScopedCheckpoint(cp_path, scope2)
    assert "filter predicate mismatch" in str(exc.value)


def test_reject_incompatible_checkpoint_reuse_on_mapping_change(tmp_path):
    cp_path = tmp_path / "checkpoint.json"
    scope1 = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'active'",
        column_mapping={"id": "order_id", "amt": "total"},
        tenant_predicate="tenant_id = 't1'",
    )
    cp = ScopedCheckpoint(cp_path, scope1)
    cp.begin("orders", "md5(id)", [(1, 100)])
    cp.record("orders", 1, 100, 10, "111")

    scope2 = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'active'",
        column_mapping={"id": "order_id", "amt": "price"},
        tenant_predicate="tenant_id = 't1'",
    )
    with pytest.raises(IncompatibleResumeScopeError) as exc:
        ScopedCheckpoint(cp_path, scope2)
    assert "column mapping mismatch" in str(exc.value)


def test_reject_incompatible_checkpoint_reuse_on_tenant_change(tmp_path):
    cp_path = tmp_path / "checkpoint.json"
    scope1 = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'active'",
        column_mapping={"id": "order_id"},
        tenant_predicate="tenant_id = 'tenant_A'",
    )
    cp = ScopedCheckpoint(cp_path, scope1)
    cp.begin("orders", "md5(id)", [(1, 100)])
    cp.record("orders", 1, 100, 10, "111")

    scope2 = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'active'",
        column_mapping={"id": "order_id"},
        tenant_predicate="tenant_id = 'tenant_B'",
    )
    with pytest.raises(IncompatibleResumeScopeError) as exc:
        ScopedCheckpoint(cp_path, scope2)
    assert "tenant predicate mismatch" in str(exc.value)


def test_reject_incompatible_checkpoint_reuse_on_table_scope_change(tmp_path):
    cp_path = tmp_path / "checkpoint.json"
    scope1 = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'active'",
        column_mapping={"id": "order_id"},
        tenant_predicate="tenant_id = 't1'",
    )
    cp = ScopedCheckpoint(cp_path, scope1)
    cp.begin("orders", "md5(id)", [(1, 100)])
    cp.record("orders", 1, 100, 10, "111")

    scope2 = ResumeScope(
        tables=["orders", "order_items"],
        filter_predicate="status = 'active'",
        column_mapping={"id": "order_id"},
        tenant_predicate="tenant_id = 't1'",
    )
    with pytest.raises(IncompatibleResumeScopeError) as exc:
        ScopedCheckpoint(cp_path, scope2)
    assert "table scope mismatch" in str(exc.value)


def test_unit_identity_deterministic_and_unique():
    scope = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'active'",
        column_mapping={"id": "order_id"},
        tenant_predicate="tenant_id = 't1'",
    )
    u1 = scope.unit_identity("orders", 1, 100)
    u2 = scope.unit_identity("orders", 1, 100)
    u3 = scope.unit_identity("orders", 101, 200)
    assert u1.unit_id == u2.unit_id
    assert u1.unit_id != u3.unit_id

    scope_other = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'pending'",
        column_mapping={"id": "order_id"},
        tenant_predicate="tenant_id = 't1'",
    )
    u_other = scope_other.unit_identity("orders", 1, 100)
    assert u1.unit_id != u_other.unit_id


def test_crash_at_each_chunk_resumes_cleanly_no_out_of_scope_rows(tmp_path):
    raw_rows = []
    expected_in_scope = []
    chunk_count = 5
    rows_per_chunk = 20

    for i in range(1, (chunk_count * rows_per_chunk) + 1):
        is_t1 = (i % 2 != 0)
        is_active = (i % 3 != 0)
        is_high_val = (i > 25)

        tenant = "t1" if is_t1 else "t2"
        status = "active" if is_active else "inactive"
        amount = 150.0 if is_high_val else 50.0

        row = {
            "id": i,
            "tenant_id": tenant,
            "status": status,
            "amount": amount,
            "payload": f"data_{i}",
        }
        raw_rows.append(row)

        if tenant == "t1" and status == "active" and amount > 100.0:
            expected_in_scope.append({
                "order_id": i,
                "tenant": tenant,
                "order_status": status,
                "total": amount,
            })

    ranges = []
    for c in range(chunk_count):
        lo = (c * rows_per_chunk) + 1
        hi = ((c + 1) * rows_per_chunk) + 1
        ranges.append((lo, hi))

    scope = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'active' and amount > 100",
        column_mapping={"id": "order_id", "tenant_id": "tenant", "status": "order_status", "amount": "total"},
        tenant_predicate="tenant_id = 't1'",
    )

    for crash_chunk_idx in range(chunk_count):
        run_dir = tmp_path / f"run_crash_at_{crash_chunk_idx}"
        run_dir.mkdir(parents=True, exist_ok=True)
        cp_path = run_dir / "checkpoint.json"

        cp = ScopedCheckpoint(cp_path, scope)
        runner = ResumeScopeRunner(cp)

        written_rows = []

        def fetch_chunk(tbl, lo, hi):
            return [r for r in raw_rows if lo <= r["id"] < hi]

        def write_chunk(tbl, mapped_rows):
            written_rows.extend(mapped_rows)

        with pytest.raises(RuntimeError) as exc:
            runner.execute_table(
                "orders",
                ranges,
                fetch_chunk,
                write_chunk,
                crash_at_chunk=crash_chunk_idx,
                expr="md5(id)",
            )
        assert f"simulated crash at chunk {crash_chunk_idx}" in str(exc.value)

        assert cp.resumed("orders") == crash_chunk_idx

        for w in written_rows:
            assert w["tenant"] == "t1", f"Out of scope tenant leaked: {w}"
            assert w["order_status"] == "active", f"Out of scope status leaked: {w}"
            assert w["total"] > 100.0, f"Out of scope amount leaked: {w}"

        resumed_cp = ScopedCheckpoint(cp_path, scope)
        pending = resumed_cp.begin("orders", "md5(id)", ranges)
        assert len(pending) == chunk_count - crash_chunk_idx
        assert pending == ranges[crash_chunk_idx:]

        resumed_runner = ResumeScopeRunner(resumed_cp)
        resumed_runner.execute_table(
            "orders",
            ranges,
            fetch_chunk,
            write_chunk,
            crash_at_chunk=None,
        )

        for w in written_rows:
            assert w["tenant"] == "t1"
            assert w["order_status"] == "active"
            assert w["total"] > 100.0

        assert len(written_rows) == len(expected_in_scope)
        assert sorted(written_rows, key=lambda x: x["order_id"]) == sorted(expected_in_scope, key=lambda x: x["order_id"])
        assert resumed_cp.resumed("orders") == chunk_count


def test_progress_journal_streaming_and_recovery(tmp_path):
    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    cp_path = report_dir / "checkpoint.json"
    journal_path = report_dir / "check-progress.jsonl"

    scope = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'paid'",
        column_mapping={"id": "out_id"},
        tenant_predicate="tenant_id = 't1'",
    )
    cp = ScopedCheckpoint(cp_path, scope)
    ranges = [(1, 50), (50, 100), (100, 150)]
    cp.begin("orders", "md5(id)", ranges)

    cp.record("orders", 1, 50, 10, "csum1")
    cp.record_progress_journal(report_dir, "orders", 1, 50, 10, "csum1")

    recovered_cp = ScopedCheckpoint(report_dir / "recovered_cp.json", scope)
    loaded = recovered_cp.load_progress_journal(journal_path)
    assert loaded == 1
    assert recovered_cp.resumed("orders") == 1

    remaining = recovered_cp.begin("orders", "md5(id)", ranges)
    assert remaining == [(50, 100), (100, 150)]


def test_sqlite_crash_and_resume_end_to_end(tmp_path):
    src_db = tmp_path / "src.db"
    dst_db = tmp_path / "dst.db"
    cp_path = tmp_path / "checkpoint.json"

    with sqlite3.connect(src_db) as conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id TEXT, status TEXT, amount REAL)")
        for i in range(1, 101):
            tenant = "t1" if i % 2 == 1 else "t2"
            status = "active" if i % 3 != 0 else "cancelled"
            amount = 120.0 if i > 30 else 40.0
            conn.execute("INSERT INTO orders VALUES (?, ?, ?, ?)", (i, tenant, status, amount))

    with sqlite3.connect(dst_db) as conn:
        conn.execute("CREATE TABLE target_orders (order_id INTEGER PRIMARY KEY, tenant TEXT, order_status TEXT, order_amount REAL)")

    scope = ResumeScope(
        tables=["orders"],
        filter_predicate="status = 'active' AND amount > 100",
        column_mapping={"id": "order_id", "tenant_id": "tenant", "status": "order_status", "amount": "order_amount"},
        tenant_predicate="tenant_id = 't1'",
    )

    ranges = [(1, 26), (26, 51), (51, 76), (76, 101)]

    cp = ScopedCheckpoint(cp_path, scope)
    runner = ResumeScopeRunner(cp)

    with pytest.raises(RuntimeError) as exc:
        runner.run_sqlite(src_db, dst_db, "orders", "target_orders", "id", ranges, crash_at_chunk=2)
    assert "simulated crash at chunk 2" in str(exc.value)

    with sqlite3.connect(dst_db) as conn:
        rows = conn.execute("SELECT * FROM target_orders").fetchall()
        for r in rows:
            assert r[1] == "t1"
            assert r[2] == "active"
            assert r[3] > 100.0

    resumed_cp = ScopedCheckpoint(cp_path, scope)
    assert resumed_cp.resumed("orders") == 2
    resumed_runner = ResumeScopeRunner(resumed_cp)
    resumed_runner.run_sqlite(src_db, dst_db, "orders", "target_orders", "id", ranges, crash_at_chunk=None)

    with sqlite3.connect(src_db) as conn:
        expected_count = conn.execute(
            "SELECT count(*) FROM orders WHERE tenant_id = 't1' AND status = 'active' AND amount > 100"
        ).fetchone()[0]

    with sqlite3.connect(dst_db) as conn:
        actual_rows = conn.execute("SELECT * FROM target_orders").fetchall()
        assert len(actual_rows) == expected_count
        for r in actual_rows:
            assert r[1] == "t1"
            assert r[2] == "active"
            assert r[3] > 100.0


def test_resume_scope_compatibility_rungs_under_watched():
    with watched() as seen:
        s1 = ResumeScope(tables=["orders"], filter_predicate="active = 1", column_mapping={"id": "pk"})

        # 1. admit_identical_scope holds
        choice_match = decide_scope_compatibility(s1.header(), s1)
        assert choice_match.rung.name == "admit_identical_scope"

        # 2. admit_identical_scope refuses, refuse_incompatible_scope holds
        diff_hdr = dict(s1.header())
        diff_hdr["filter_predicate"] = "active = 0"
        choice_diff = decide_scope_compatibility(diff_hdr, s1)
        assert choice_diff.rung.name == "refuse_incompatible_scope"

