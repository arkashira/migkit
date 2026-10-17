"""Tests for warehouse and lakehouse commit semantics (Row 646: RCH38.storage-protocol).

Validates data-file completeness, journal/manifest positions, atomic publish,
partition/schema evolution, tombstones, and downstream visibility across
Iceberg, Delta, Snowflake, BigQuery, and Redshift staging targets.

Proves:
1. Every decide.Rung in `storage_protocol.storage_commit_rungs` is exercised on
   both `holds` and `refuses` under `tests.rung_branches.watched()`, declared in
   `PROVES_RUNGS`.
2. Staged bulk versus streaming versus journal transaction merge is chosen by
   measured billable work and consistent commit behavior.
3. Files present in object storage are NOT committed table state until referenced
   by an atomically published manifest or journal entry.
4. A crash between object upload, metadata commit, and ack yields no orphan
   visibility or duplicated effects, and queryable target coverage matches the
   source fence.
"""

from __future__ import annotations

import datetime
import decimal
import pathlib
import uuid
from typing import Any, Dict, Sequence, Tuple

import pytest

import tests.rung_branches
from migkit import decide, storage_protocol
from migkit.storage_protocol import (
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
     PARQUET_MAGIC,
    AdaptiveStorageBatchController,
    AtomicStorageCatalogCoordinator,
    CareBadge,
    CommitPhaseState,
    CommitProtocolMode,
    CrashInjectionPoint,
    DataFileSpec,
    DuplicateCommitEffectError,
    DurableCommitUnitCheckpoint,
    IncompatibleEvolutionError,
    IncompleteDataFileError,
    LeaseLockError,
    PartitionTransformSpec,
    SchemaEvolutionAction,
    SchemaFieldSpec,
    SourceFenceExpectation,
    StoragePlatform,
    StorageProtocolCheckpointStore,
    StorageProtocolError,
    StorageProtocolLease,
    StorageProtocolToolWrapper,
    TableSchemaVersion,
    TombstoneMechanism,
    TombstoneSpec,
    UnsafeEndpointError,
    choose_storage_commit_protocol,
    compare_lsn,
    compute_rows_checksum,
    evaluate_billable_work,
    storage_commit_rungs,
    validate_safe_endpoint,
    verify_storage_commit_semantics,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("storage_protocol", "storage_commit_rungs", "staged_bulk_manifest", "holds"),
    ("storage_protocol", "storage_commit_rungs", "staged_bulk_manifest", "refuses"),
    ("storage_protocol", "storage_commit_rungs", "streaming_offset_channel", "holds"),
    ("storage_protocol", "storage_commit_rungs", "streaming_offset_channel", "refuses"),
    ("storage_protocol", "storage_commit_rungs", "journal_transaction_merge", "holds"),
    ("storage_protocol", "storage_commit_rungs", "journal_transaction_merge", "refuses"),
]


# -----------------------------------------------------------------------------
# Helpers & Fixtures
# -----------------------------------------------------------------------------

def _base_schema() -> TableSchemaVersion:
    return TableSchemaVersion(
        schema_epoch=1,
        partition_spec_id=1,
        fields=(
            SchemaFieldSpec(field_id=1, name="id", data_type="BIGINT", nullable=False),
            SchemaFieldSpec(field_id=2, name="customer", data_type="VARCHAR(128)", nullable=False),
            SchemaFieldSpec(field_id=3, name="amount", data_type="DECIMAL(12,2)", nullable=False),
        ),
        key_field_ids=(1,),
        partition_transforms=(
            PartitionTransformSpec(source_field_id=1, field_name="id_bucket", transform="bucket[16]"),
        ),
    )


def _make_data_file(
    uri: str,
    batch_id: str,
    rows: Sequence[Dict[str, Any]],
    *,
    schema_epoch: int = 1,
    partition_spec_id: int = 1,
    source_min_lsn: str = "0/1000",
    source_max_lsn: str = "0/1100",
) -> DataFileSpec:
    rows_tuple = tuple(dict(r) for r in rows)
    checksum = compute_rows_checksum(rows_tuple)
    byte_size = max(128, len(rows_tuple) * 256)
    keys = [ (r["id"],) for r in rows_tuple if "id" in r ]
    return DataFileSpec(
        file_uri=uri,
        batch_id=batch_id,
        partition_values={"id_bucket": 0},
        schema_epoch=schema_epoch,
        partition_spec_id=partition_spec_id,
        expected_row_count=len(rows_tuple),
        actual_row_count=len(rows_tuple),
        expected_byte_size=byte_size,
        actual_byte_size=byte_size,
        expected_checksum=checksum,
        actual_checksum=checksum,
        footer_magic=PARQUET_MAGIC,
        min_key=min(keys) if keys else None,
        max_key=max(keys) if keys else None,
        source_min_lsn=source_min_lsn,
        source_max_lsn=source_max_lsn,
        rows_payload=rows_tuple,
    )


# -----------------------------------------------------------------------------
# 1. Decide Rung Ladder Coverage (holds and refuses under watched())
# -----------------------------------------------------------------------------

RUNG_CASES = [
    (
        "staged_bulk_manifest",
        {
            "object_stage": True,
            "atomic_manifest_publish": True,
            "files_verified": True,
            "bulk_within_budget": True,
        },
        {
            "object_stage": False,
            "atomic_manifest_publish": True,
            "files_verified": True,
            "bulk_within_budget": True,
        },
        "lacks an object stage or atomic manifest publish protocol",
    ),
    (
        "streaming_offset_channel",
        {
            "streaming_offset_token": True,
            "requires_partition_evolution": False,
            "stream_keeps_up": True,
            "stream_within_budget": True,
        },
        {
            "streaming_offset_token": False,
            "requires_partition_evolution": False,
            "stream_keeps_up": True,
            "stream_within_budget": True,
        },
        "no streaming channel that keeps a durable offset token",
    ),
    (
        "journal_transaction_merge",
        {
            "transactional_journal_ledger": True,
            "merge_within_budget": True,
        },
        {
            "transactional_journal_ledger": False,
            "merge_within_budget": True,
        },
        "cannot merge journal and _migkit_ledger in one transaction",
    ),
]


def test_proves_rungs_matches_cases() -> None:
    declared_set = set(PROVES_RUNGS)
    expected_set = set()
    for rung_name, _, _, _ in RUNG_CASES:
        expected_set.add(("storage_protocol", "storage_commit_rungs", rung_name, HOLDS))
        expected_set.add(("storage_protocol", "storage_commit_rungs", rung_name, REFUSES))
    assert declared_set == expected_set


@pytest.mark.parametrize("rung_name,ok_facts,bad_facts,expected_why", RUNG_CASES)
def test_storage_commit_rungs_hold_and_refuse_under_watched(
    rung_name: str,
    ok_facts: Dict[str, Any],
    bad_facts: Dict[str, Any],
    expected_why: str,
) -> None:
    rungs = {r.name: r for r in storage_commit_rungs()}
    target_rung = rungs[rung_name]

    with tests.rung_branches.watched() as seen:
        kept, out = decide.prune([target_rung], dict(ok_facts))
        assert kept == [target_rung], out
        kept_bad, out_bad = decide.prune([target_rung], dict(bad_facts))
        assert not kept_bad and out_bad[0][0] is target_rung

    assert branch(seen, rung_name) == [HOLDS, REFUSES]
    assert expected_why in out_bad[0][1]
    assert why(seen, rung_name) == out_bad[0][1]


def test_choose_storage_commit_protocol_by_measured_billable_work() -> None:
    # Case A: Large 500 MB batch where staged bulk manifest has much lower billable credits than streaming
    large_batch_facts = {
        "batch_rows": 2_000_000,
        "batch_bytes": 500 * 1024 * 1024,
        "tail_rate_rows_sec": 500.0,
        "object_stage": True,
        "atomic_manifest_publish": True,
        "streaming_offset_token": True,
        "transactional_journal_ledger": True,
    }
    with watched() as seen:
        choice_bulk, est_bulk = choose_storage_commit_protocol(large_batch_facts)
    assert choice_bulk.path == CommitProtocolMode.STAGED_BULK_MANIFEST.value
    assert est_bulk is not None
    all_ests = evaluate_billable_work(large_batch_facts)
    assert est_bulk.billable_credits < all_ests[CommitProtocolMode.STREAMING_OFFSET_CHANNEL.value].billable_credits
    assert HOLDS in branch(seen, "staged_bulk_manifest")

    # Case B: Small continuous micro-batch (500 rows, 128 KB) where streaming offset channel wins on billable + latency
    micro_batch_facts = {
        "batch_rows": 500,
        "batch_bytes": 128 * 1024,
        "tail_rate_rows_sec": 200.0,
        "object_stage": True,
        "atomic_manifest_publish": True,
        "streaming_offset_token": True,
        "transactional_journal_ledger": True,
    }
    with watched() as seen_micro:
        choice_stream, est_stream = choose_storage_commit_protocol(micro_batch_facts)
    assert choice_stream.path == CommitProtocolMode.STREAMING_OFFSET_CHANNEL.value
    assert est_stream is not None
    assert HOLDS in branch(seen_micro, "streaming_offset_channel")

    # Case C: Partition evolution required mid-flight -> streaming refuses, falls back to staged_bulk_manifest
    part_evol_facts = dict(micro_batch_facts, requires_partition_evolution=True)
    with watched() as seen_evol:
        choice_evol, _ = choose_storage_commit_protocol(part_evol_facts)
    assert choice_evol.path == CommitProtocolMode.STAGED_BULK_MANIFEST.value
    assert REFUSES in branch(seen_evol, "streaming_offset_channel")
    assert "partition spec" in (why(seen_evol, "streaming_offset_channel") or "")


# -----------------------------------------------------------------------------
# 2. Uncommitted Object Files Are NOT Committed Table State
# -----------------------------------------------------------------------------

@pytest.mark.parametrize(
    "platform",
    [
        StoragePlatform.ICEBERG,
        StoragePlatform.DELTA,
        StoragePlatform.SNOWFLAKE,
        StoragePlatform.BIGQUERY,
        StoragePlatform.REDSHIFT,
    ],
)
def test_files_present_in_object_storage_are_not_committed_table_state(
    platform: StoragePlatform,
) -> None:
    coord = AtomicStorageCatalogCoordinator(
        platform=platform,
        hop_name="pg_to_lake",
        initial_schema=_base_schema(),
        endpoint_host="127.0.0.1",
    )

    rows_b1 = [
        {"id": 1, "customer": "acme", "amount": decimal.Decimal("100.00")},
        {"id": 2, "customer": "globex", "amount": decimal.Decimal("250.50")},
    ]
    file_b1 = _make_data_file("s3://stage/table/part-0001.parquet", "batch-1", rows_b1)

    # Upload file to object storage without committing metadata
    coord.stage_data_file(file_b1)
    assert "s3://stage/table/part-0001.parquet" in coord.object_store_files

    # Downstream visibility must show 0 queryable rows and list the file as uncommitted orphan
    snap_before = coord.downstream_snapshot()
    assert snap_before.queryable_row_count == 0
    assert snap_before.queryable_rows == ()
    assert snap_before.committed_file_uris == ()
    assert snap_before.uncommitted_orphan_file_uris == ("s3://stage/table/part-0001.parquet",)

    # Now atomically publish manifest
    receipt = coord.commit_batch(
        batch_id="batch-1",
        idempotency_token="tok-001",
        source_fence_lsn="0/1100",
        file_uris=["s3://stage/table/part-0001.parquet"],
    )
    assert receipt.committed is True
    assert receipt.acknowledged is True
    assert receipt.care_badge == CareBadge.PASS

    snap_after = coord.downstream_snapshot()
    assert snap_after.queryable_row_count == 2
    assert snap_after.committed_file_uris == ("s3://stage/table/part-0001.parquet",)
    assert snap_after.uncommitted_orphan_file_uris == ()


# -----------------------------------------------------------------------------
# 3. Data-File Completeness Verification (Truncated / Checksum / Count Mismatch)
# -----------------------------------------------------------------------------

def test_incomplete_or_corrupted_data_files_refused_before_commit() -> None:
    coord = AtomicStorageCatalogCoordinator(
        platform=StoragePlatform.ICEBERG,
        hop_name="pg_to_iceberg",
        initial_schema=_base_schema(),
    )
    rows = [{"id": 10, "customer": "initech", "amount": decimal.Decimal("42.00")}]
    valid_file = _make_data_file("s3://stage/t/f1.parquet", "batch-10", rows)

    # 1. Partial upload crash leaves truncated footer & half byte size
    with pytest.raises(StorageProtocolError, match="partial object upload"):
        coord.stage_data_file(
            valid_file,
            crash_point=CrashInjectionPoint.DURING_PARTIAL_UPLOAD,
        )

    # Attempting to commit the truncated file must fail completeness check
    with pytest.raises(IncompleteDataFileError, match="footer magic"):
        coord.commit_batch(
            batch_id="batch-10",
            idempotency_token="tok-10",
            source_fence_lsn="0/2000",
            file_uris=["s3://stage/t/f1.parquet"],
        )

    # 2. Checksum mismatch refused
    bad_checksum_file = _make_data_file("s3://stage/t/f2.parquet", "batch-11", rows)
    bad_checksum_file = DataFileSpec(
        **{**bad_checksum_file.__dict__, "actual_checksum": "deadbeef" * 8}
    )
    coord.stage_data_file(bad_checksum_file)
    with pytest.raises(IncompleteDataFileError, match="checksum mismatch"):
        coord.commit_batch(
            batch_id="batch-11",
            idempotency_token="tok-11",
            source_fence_lsn="0/2100",
            file_uris=["s3://stage/t/f2.parquet"],
        )

    # Downstream table remains completely untouched (0 rows)
    assert coord.downstream_snapshot().queryable_row_count == 0


# -----------------------------------------------------------------------------
# 4. Crash Between Object Upload, Metadata Commit, and Ack (Zero Orphans / Duplicates)
# -----------------------------------------------------------------------------

def test_crash_between_upload_commit_and_ack_yields_exact_fence_coverage() -> None:
    coord = AtomicStorageCatalogCoordinator(
        platform=StoragePlatform.DELTA,
        hop_name="mysql_to_delta",
        initial_schema=_base_schema(),
        endpoint_host="10.0.0.15",
    )

    # Batch 1 commits cleanly at fence 0/1000
    rows_1 = [
        {"id": 1, "customer": "alpha", "amount": decimal.Decimal("10.00")},
        {"id": 2, "customer": "beta", "amount": decimal.Decimal("20.00")},
    ]
    f1 = _make_data_file("s3://delta/t/part-1.parquet", "b1", rows_1, source_min_lsn="0/0900", source_max_lsn="0/1000")
    coord.stage_data_file(f1)
    coord.commit_batch(
        batch_id="b1",
        idempotency_token="token-b1",
        source_fence_lsn="0/1000",
        file_uris=[f1.file_uri],
    )

    # Batch 2 uploads file to S3, then CRASHES before metadata commit
    rows_2 = [
        {"id": 3, "customer": "gamma", "amount": decimal.Decimal("30.00")},
    ]
    f2_orphan = _make_data_file("s3://delta/t/part-2-attempt1.parquet", "b2", rows_2, source_min_lsn="0/1001", source_max_lsn="0/2000")
    coord.stage_data_file(f2_orphan)
    with pytest.raises(StorageProtocolError, match="before metadata commit"):
        coord.commit_batch(
            batch_id="b2",
            idempotency_token="token-b2",
            source_fence_lsn="0/2000",
            file_uris=[f2_orphan.file_uri],
            crash_point=CrashInjectionPoint.AFTER_OBJECT_UPLOAD_BEFORE_COMMIT,
        )

    # Verify while b2 is uncommitted: downstream query sees ONLY b1 (fence 0/1000), zero orphan leak
    expect_b1 = SourceFenceExpectation(fence_lsn="0/1000", expected_rows=tuple(rows_1))
    report_mid = verify_storage_commit_semantics(coord, expect_b1, allow_uncollected_staged_files=True)
    assert report_mid.fence_matched is True
    assert report_mid.orphan_visibility_free is True
    assert report_mid.uncommitted_orphan_file_count == 1
    assert report_mid.care_badge == CareBadge.BENIGN

    # If uncollected staged files are flagged for review before cutover:
    report_review = verify_storage_commit_semantics(coord, expect_b1, allow_uncollected_staged_files=False)
    assert report_review.care_badge == CareBadge.REVIEW

    # Garbage collect uncommitted orphan files from failed b2 attempt 1
    gc_removed = coord.garbage_collect_uncommitted_files()
    assert gc_removed == ("s3://delta/t/part-2-attempt1.parquet",)

    # Retry Batch 2: upload f2_retry, commit metadata, then CRASH before ACK!
    f2_retry = _make_data_file("s3://delta/t/part-2-attempt2.parquet", "b2", rows_2, source_min_lsn="0/1001", source_max_lsn="0/2000")
    coord.stage_data_file(f2_retry)
    with pytest.raises(StorageProtocolError, match="after metadata commit before ack"):
        coord.commit_batch(
            batch_id="b2",
            idempotency_token="token-b2",
            source_fence_lsn="0/2000",
            file_uris=[f2_retry.file_uri],
            crash_point=CrashInjectionPoint.AFTER_METADATA_COMMIT_BEFORE_ACK,
        )

    assert coord.phase_by_batch["b2"] == CommitPhaseState.METADATA_COMMITTED_UNACKED

    # Worker restarts and retries Batch 2 with same idempotency_token="token-b2"
    receipt_b2 = coord.commit_batch(
        batch_id="b2",
        idempotency_token="token-b2",
        source_fence_lsn="0/2000",
        file_uris=[f2_retry.file_uri],
    )
    assert receipt_b2.committed is True
    assert receipt_b2.acknowledged is True
    assert receipt_b2.deduplicated_retry is True
    assert receipt_b2.commit_sequence == 2

    # Attempting a conflicting retry with same batch_id but different fence/token is refused
    with pytest.raises(DuplicateCommitEffectError, match="conflicting retry"):
        coord.commit_batch(
            batch_id="b2",
            idempotency_token="token-b2-different",
            source_fence_lsn="0/2500",
            file_uris=[f2_retry.file_uri],
        )

    # Final verification against source fence 0/2000: exact row count (3), zero duplicates, PASS badge
    expect_b2 = SourceFenceExpectation(
        fence_lsn="0/2000",
        expected_rows=tuple(rows_1 + rows_2),
    )
    final_report = verify_storage_commit_semantics(coord, expect_b2, allow_uncollected_staged_files=False)
    assert final_report.care_badge == CareBadge.PASS
    assert final_report.fence_matched is True
    assert final_report.orphan_visibility_free is True
    assert final_report.duplicate_effects_free is True
    assert final_report.completeness_verified is True
    assert final_report.queryable_row_count == 3
    assert final_report.uncommitted_orphan_file_count == 0


# -----------------------------------------------------------------------------
# 5. Tombstones, Upserts, and Schema / Partition Evolution Across Commits
# -----------------------------------------------------------------------------

def test_tombstones_and_schema_partition_evolution_across_snapshots() -> None:
    coord = AtomicStorageCatalogCoordinator(
        platform=StoragePlatform.SNOWFLAKE,
        hop_name="pg_to_snowflake",
        initial_schema=_base_schema(),
    )

    # Commit 1: Insert id=1, id=2, id=3 under schema epoch 1
    rows_c1 = [
        {"id": 1, "customer": "c1", "amount": decimal.Decimal("10.00")},
        {"id": 2, "customer": "c2", "amount": decimal.Decimal("20.00")},
        {"id": 3, "customer": "c3", "amount": decimal.Decimal("30.00")},
    ]
    f1 = _make_data_file("s3://sf/t/c1.parquet", "batch-c1", rows_c1, schema_epoch=1)
    coord.stage_data_file(f1)
    coord.commit_batch(
        batch_id="batch-c1",
        idempotency_token="tok-c1",
        source_fence_lsn="0/1000",
        file_uris=[f1.file_uri],
    )

    # Evolve schema:
    # - Add nullable column 'region' with default 'US' (epoch 2)
    # - Rename column 'customer' (field_id=2) to 'client_name' (epoch 3)
    # - Evolve partition spec to partition by region + id_bucket (epoch 4)
    coord.evolve_schema_or_partition(
        SchemaEvolutionAction.ADD_NULLABLE_COLUMN,
        new_field=SchemaFieldSpec(field_id=4, name="region", data_type="VARCHAR(32)", nullable=True, default_value="US"),
    )
    coord.evolve_schema_or_partition(
        SchemaEvolutionAction.RENAME_COLUMN_BY_FIELD_ID,
        target_field_id=2,
        new_name="client_name",
    )
    evolved_schema = coord.evolve_schema_or_partition(
        SchemaEvolutionAction.EVOLVE_PARTITION_SPEC,
        new_partition_transforms=(
            PartitionTransformSpec(source_field_id=4, field_name="region", transform="identity"),
            PartitionTransformSpec(source_field_id=1, field_name="id_bucket", transform="bucket[32]"),
        ),
    )
    assert evolved_schema.schema_epoch == 4
    assert evolved_schema.partition_spec_id == 2

    # Incompatible evolutions are strictly refused
    with pytest.raises(IncompatibleEvolutionError, match="narrowing"):
        coord.evolve_schema_or_partition(SchemaEvolutionAction.INCOMPATIBLE_NARROWING)
    with pytest.raises(IncompatibleEvolutionError, match="identifier/key"):
        coord.evolve_schema_or_partition(SchemaEvolutionAction.DROP_REQUIRED_KEY_COLUMN)

    # Commit 2 under schema_epoch=4:
    # - Update id=2 amount to 25.00 and region='EU'
    # - Insert id=4
    # - Delete id=1 via equality delete tombstone
    rows_c2 = [
        {"id": 2, "client_name": "c2_updated", "amount": decimal.Decimal("25.00"), "region": "EU"},
        {"id": 4, "client_name": "c4_new", "amount": decimal.Decimal("40.00"), "region": "APAC"},
    ]
    f2 = _make_data_file(
        "s3://sf/t/c2.parquet",
        "batch-c2",
        rows_c2,
        schema_epoch=4,
        partition_spec_id=2,
        source_min_lsn="0/1001",
        source_max_lsn="0/2000",
    )
    tombstone_c2 = TombstoneSpec(
        mechanism=TombstoneMechanism.EQUALITY_DELETE_FILE,
        key_columns=("id",),
        deleted_keys=((1,),),
        sequence_number=2,
        source_lsn="0/2000",
    )
    coord.stage_data_file(f2)
    coord.commit_batch(
        batch_id="batch-c2",
        idempotency_token="tok-c2",
        source_fence_lsn="0/2000",
        file_uris=[f2.file_uri],
        tombstones=[tombstone_c2],
    )

    # Time-travel query as of commit_sequence=1 sees original 3 rows under epoch 1 schema
    snap_seq1 = coord.downstream_snapshot(as_of_sequence=1)
    assert snap_seq1.queryable_row_count == 3
    assert [r["id"] for r in snap_seq1.queryable_rows] == [1, 2, 3]

    # Current query as of commit_sequence=2 sees id=2 (updated), id=3 (projected with client_name & default region='US'), id=4
    # and id=1 removed by tombstone!
    expected_final_rows = (
        {"id": 2, "client_name": "c2_updated", "amount": decimal.Decimal("25.00"), "region": "EU"},
        {"id": 3, "client_name": "c3", "amount": decimal.Decimal("30.00"), "region": "US"},
        {"id": 4, "client_name": "c4_new", "amount": decimal.Decimal("40.00"), "region": "APAC"},
    )
    expect_fence = SourceFenceExpectation(
        fence_lsn="0/2000",
        expected_rows=expected_final_rows,
        key_columns=("id",),
    )
    report = verify_storage_commit_semantics(coord, expect_fence)
    assert report.care_badge == CareBadge.PASS
    assert report.fence_matched is True
    assert report.queryable_row_count == 3


# -----------------------------------------------------------------------------
# 6. Universal Resilience: O(1) Typed Checkpoint, Lease Lock, AIMD, Tool Wrapper
# -----------------------------------------------------------------------------

def test_o1_typed_durable_checkpoint_preserves_native_types(tmp_path: pathlib.Path) -> None:
    cp_path = tmp_path / "storage_checkpoint.json"
    store = StorageProtocolCheckpointStore(cp_path)

    now_utc = datetime.datetime(2026, 10, 17, 12, 30, 0, tzinfo=datetime.timezone.utc)
    min_key = (uuid.UUID("12345678-1234-5678-1234-567812345678"), decimal.Decimal("99.95"))
    max_key = (uuid.UUID("87654321-4321-8765-4321-876543218765"), decimal.Decimal("199.95"))

    unit_cp = DurableCommitUnitCheckpoint(
        unit_id="range-001",
        batch_id="batch-001",
        idempotency_token="tok-001",
        commit_sequence=1,
        snapshot_id="snap-000001",
        source_fence_lsn="0/5000",
        min_cursor_key=min_key,
        max_cursor_key=max_key,
        committed_at=now_utc,
    )
    store.record_completed_unit(unit_cp)
    assert store.is_completed_o1("range-001") is True
    assert store.is_completed_o1("unknown", idempotency_token="tok-001") is True
    assert store.is_completed_o1("unknown", idempotency_token="tok-999") is False

    # Reload from disk and verify exact native types
    reloaded = StorageProtocolCheckpointStore(cp_path)
    assert reloaded.is_completed_o1("range-001") is True
    restored = reloaded.completed_units["range-001"]
    assert restored.min_cursor_key == min_key
    assert isinstance(restored.min_cursor_key[0], uuid.UUID)
    assert isinstance(restored.min_cursor_key[1], decimal.Decimal)
    assert restored.committed_at == now_utc


def test_mutual_exclusion_lease_and_aimd_controller(tmp_path: pathlib.Path) -> None:
    with StorageProtocolLease(tmp_path, owner_id="worker-1", ttl_seconds=60.0) as lease1:
        lease1.heartbeat()
        with pytest.raises(LeaseLockError, match="active storage protocol lease held"):
            StorageProtocolLease(tmp_path, owner_id="worker-2", ttl_seconds=60.0).acquire()

    # After release, worker-2 can acquire
    with StorageProtocolLease(tmp_path, owner_id="worker-2", ttl_seconds=60.0) as lease2:
        assert lease2.acquired is True

    # AIMD controller and byte-weighted cold-start ETA
    ctrl = AdaptiveStorageBatchController(current_batch_rows=5000, additive_increase=2000)
    cold_eta = ctrl.estimate_eta_seconds(50 * 1024 * 1024)
    assert cold_eta == pytest.approx(1.0)

    # Fast commit increases batch size
    n1 = ctrl.record_commit(rows_committed=5000, bytes_committed=10 * 1024 * 1024, latency_ms=500.0)
    assert n1 == 7000
    # CAS conflict or throttle halves batch size
    n2 = ctrl.record_commit(
        rows_committed=7000,
        bytes_committed=10 * 1024 * 1024,
        latency_ms=2000.0,
        cas_conflict_or_throttled=True,
    )
    assert n2 == 3500
    assert ctrl.estimate_eta_seconds(0) == 0.0
    assert ctrl.estimate_eta_seconds(25 * 1024 * 1024) > 0.0


def test_tool_wrapper_and_endpoint_safety() -> None:
    assert validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert validate_safe_endpoint("10.0.0.42") == "10.0.0.42"
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("prod-warehouse.snowflakecomputing.com")

    # Raw S3 drop without manifest/catalog commit is refused with ACTION badge
    raw_res = StorageProtocolToolWrapper.evaluate_adapter("dms_s3_raw")
    assert raw_res.care_badge == CareBadge.ACTION
    assert raw_res.guarantees_atomic_publish is False
    assert "not committed table state" in raw_res.summary

    # Capable atomic adapter succeeds and sanitizes internal exceptions
    ok_res = StorageProtocolToolWrapper.evaluate_adapter("databricks_lakeflow")
    assert ok_res.care_badge == CareBadge.PASS
    assert ok_res.guarantees_atomic_publish is True

    err_res = StorageProtocolToolWrapper.evaluate_adapter(
        "snowflake_openflow",
        runner=lambda: (_ for _ in ()).throw(RuntimeError("internal vendor stack trace secret")),
    )
    assert err_res.care_badge == CareBadge.ACTION
    assert "secret" not in err_res.summary
    assert "RuntimeError" in err_res.summary
