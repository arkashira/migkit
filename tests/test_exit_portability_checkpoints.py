import json
import os
import sqlite3
import tarfile
from pathlib import Path
import pytest
import yaml

from migkit.checkpoint import Checkpoint, serialize_cursor
from migkit.resume_scope import ResumeScope, ScopedCheckpoint
from migkit.exit_portability import (
    NeutralSchema,
    NeutralType,
    PortableManifestExporter,
    PortableStateImporter,
    PortabilityValidationError,
)


def test_neutral_schema_export_types_and_sequence_offsets():
    schema = NeutralSchema("orders", target_name="tbl_orders")
    schema.add_column("id", NeutralType.BIGINT, is_primary_key=True, nullable=False)
    schema.add_column("customer_uuid", NeutralType.UUID, nullable=False)
    schema.add_column("amount", NeutralType.DECIMAL, precision=12, scale=2, nullable=False)
    schema.add_column("created_at", NeutralType.TIMESTAMP, nullable=False)
    schema.add_column("metadata", NeutralType.JSON, nullable=True)
    schema.set_sequence_offset("seq_orders_id", last_value=50420, increment_by=1)

    doc = schema.to_dict()
    assert doc["table"] == "orders"
    assert doc["target_table"] == "tbl_orders"
    assert len(doc["columns"]) == 5
    assert doc["primary_key"] == ["id"]
    assert doc["sequence_offset"]["sequence_name"] == "seq_orders_id"
    assert doc["sequence_offset"]["last_value"] == 50420
    assert doc["sequence_offset"]["increment_by"] == 1

    js = json.dumps(doc)
    assert "_proprietary" not in js
    assert "__vendor_" not in js


def test_collision_aware_column_mappings():
    exporter = PortableManifestExporter(source_engine="postgres", target_engine="snowflake")
    cols = [
        {"name": "user_id", "type": "INTEGER", "primary_key": True},
        {"name": "USER_ID", "type": "INTEGER"},
        {"name": "status", "type": "VARCHAR(20)"},
    ]
    exporter.add_table(
        "accounts",
        columns=cols,
        collision_strategy="rename_numbered",
    )
    pkg = exporter.to_package_dict()
    mapping = pkg["mappings"]["accounts"]
    assert mapping["user_id"] == "user_id"
    assert mapping["USER_ID"] == "user_id_2"
    assert mapping["status"] == "status"


def test_portable_manifest_exporter_json_yaml(tmp_path):
    exporter = PortableManifestExporter(
        source_engine="mysql",
        target_engine="postgres",
        hop_name="hop_finance",
        version="1.0.0",
    )
    exporter.set_tool_config(
        batch_size=5000,
        verify_method="aimd_merkle",
        concurrency=4,
        salt="99aacc0011223344",
        timeout_seconds=30.0,
    )
    exporter.add_table(
        "transactions",
        target_table="txn_records",
        columns=[
            {"name": "txn_id", "type": "BIGINT", "primary_key": True},
            {"name": "account_id", "type": "INTEGER"},
            {"name": "val", "type": "DECIMAL(10,2)"},
        ],
        primary_key=["txn_id"],
        sequence_offsets={"sequence_name": "txn_id_seq", "last_value": 9001},
    )
    exporter.add_manifest(
        "transactions",
        total_rows=10000,
        total_bytes=1048576,
        total_checksum="1234567890",
        parts=[
            {
                "name": "part_001.csv",
                "format": "csv",
                "rows": 5000,
                "bytes": 524288,
                "sha256": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                "range": {"lo": 1, "hi": 5000},
                "checksum": "600000000",
            },
            {
                "name": "part_002.csv",
                "format": "csv",
                "rows": 5000,
                "bytes": 524288,
                "sha256": "123456abcdef123456abcdef123456abcdef123456abcdef123456abcdef1234",
                "range": {"lo": 5001, "hi": 10000},
                "checksum": "634567890",
            },
        ],
    )
    exporter.add_checkpoint(
        "transactions",
        done_ranges={"1..5000": [5000, "600000000"]},
        cursors={"1..5000": {"lo": 1, "hi": 5000, "rows": 5000, "checksum": "600000000"}},
        watermark="0/16B3748",
        sequence_offsets={"last_seqno": 5000},
    )

    out_dir = tmp_path / "exported_pkg"
    exporter.export_to_directory(out_dir, fmt="json")
    assert (out_dir / "package.json").exists()
    assert (out_dir / "schemas.json").exists()
    assert (out_dir / "mappings.json").exists()
    assert (out_dir / "manifests.json").exists()
    assert (out_dir / "checkpoints.json").exists()
    assert (out_dir / "tool_config.json").exists()

    archive_path = tmp_path / "bundle.tar.gz"
    exporter.export_bundle_archive(archive_path, fmt="yaml")
    assert archive_path.exists()
    assert tarfile.is_tarfile(archive_path)


def test_portable_state_importer_ingest_and_validation(tmp_path):
    exporter = PortableManifestExporter(
        source_engine="postgres",
        target_engine="sqlite",
        hop_name="hop_core",
    )
    exporter.add_table(
        "users",
        columns=[
            {"name": "id", "type": "INTEGER", "primary_key": True},
            {"name": "email", "type": "TEXT"},
        ],
        primary_key=["id"],
        sequence_offsets={"last_value": 300},
    )
    exporter.add_checkpoint(
        "users",
        done_ranges={"1..100": [100, "55555"]},
        cursors={"1..100": {"lo": 1, "hi": 100, "rows": 100, "checksum": "55555"}},
        watermark=100,
    )
    out_dir = tmp_path / "core_pkg"
    exporter.export_to_directory(out_dir)

    importer = PortableStateImporter(out_dir)
    assert importer.validate() is True
    assert importer.source_engine == "postgres"
    assert importer.target_engine == "sqlite"

    schema = importer.get_schema("users")
    assert schema["table"] == "users"
    assert schema["primary_key"] == ["id"]
    assert schema["sequence_offset"]["last_value"] == 300

    offsets = importer.resume_offsets("users")
    assert offsets["watermark"] == 100
    assert offsets["sequence_offsets"]["last_value"] == 300


def test_resume_verification_without_indiscriminate_rescan(tmp_path):
    exporter = PortableManifestExporter(source_engine="sqlite", target_engine="sqlite")
    exporter.add_table("items", columns=[{"name": "id", "type": "INTEGER", "primary_key": True}])
    exporter.add_checkpoint(
        "items",
        done_ranges={
            "1..1000": [1000, "111111"],
            "1001..2000": [1000, "222222"],
        },
        cursors={
            "1..1000": {"lo": 1, "hi": 1000, "rows": 1000, "checksum": "111111"},
            "1001..2000": {"lo": 1001, "hi": 2000, "rows": 1000, "checksum": "222222"},
        },
    )
    pkg = exporter.to_package_dict()

    importer = PortableStateImporter(pkg)
    all_ranges = [(1, 1000), (1001, 2000), (2001, 3000), (3001, 4000)]
    assert importer.can_resume_without_rescan("items") is True

    pending = importer.pending_ranges("items", all_ranges)
    assert pending == [(2001, 3000), (3001, 4000)]

    done = importer.completed_ranges("items")
    assert len(done) == 2
    assert done["1..1000"] == (1000, "111111")
    assert done["1001..2000"] == (1000, "222222")

    tot_rows, tot_ck = importer.total_completed("items")
    assert tot_rows == 2000
    assert tot_ck == str(111111 + 222222)


def test_reconstitute_migkit_checkpoint_for_clean_installation(tmp_path):
    exporter = PortableManifestExporter(source_engine="postgres", target_engine="mysql")
    exporter.add_table("metrics", columns=[{"name": "id", "type": "INTEGER", "primary_key": True}])
    exporter.add_checkpoint(
        "metrics",
        done_ranges={"0..500": [500, "987654321"]},
        cursors={"0..500": {"lo": 0, "hi": 500, "rows": 500, "checksum": "987654321"}},
        fingerprint="fp_abc123",
    )
    pkg = exporter.to_package_dict()

    importer = PortableStateImporter(pkg)
    target_cp_path = tmp_path / "reconstituted_checkpoint.json"
    cp = importer.reconstitute_checkpoint(target_cp_path)
    assert isinstance(cp, Checkpoint)
    assert cp.resumed("metrics") == 1
    rows, ck = cp.total("metrics")
    assert rows == 500
    assert ck == "987654321"

    loaded_cp = Checkpoint(target_cp_path)
    assert loaded_cp.resumed("metrics") == 1


def test_reconstitute_scoped_checkpoint(tmp_path):
    scope = ResumeScope(
        tables=["ledger"],
        filter_predicate="active = 1",
        column_mapping={"raw_amt": "amt"},
        tenant_predicate="tenant_id = 't42'",
    )
    scoped_cp_path = tmp_path / "orig_scoped_cp.json"
    scoped_cp = ScopedCheckpoint(scoped_cp_path, scope)
    scoped_cp.record("ledger", 1, 100, rows=50, checksum="4444")

    exporter = PortableManifestExporter()
    exporter.add_scoped_checkpoint(scoped_cp, "ledger")
    pkg = exporter.to_package_dict()

    importer = PortableStateImporter(pkg)
    reconstituted_path = tmp_path / "reconstituted_scoped.json"
    reconstituted = importer.reconstitute_scoped_checkpoint("ledger", reconstituted_path)
    assert isinstance(reconstituted, ScopedCheckpoint)
    assert reconstituted.scope.tables == ("ledger",)
    assert reconstituted.scope.filter_predicate == "active = 1"
    assert reconstituted.scope.tenant_predicate == "tenant_id = 't42'"
    assert reconstituted.resumed("ledger") == 1


def test_external_runner_ingest_archive_tar_gz(tmp_path):
    exporter = PortableManifestExporter(source_engine="sqlite", target_engine="sqlite")
    exporter.add_table(
        "inventory",
        columns=[
            {"name": "sku", "type": "VARCHAR(32)", "primary_key": True},
            {"name": "qty", "type": "INTEGER"},
        ],
        primary_key=["sku"],
    )
    exporter.add_checkpoint(
        "inventory",
        done_ranges={"AAA..MMM": [120, "112233"]},
        cursors={"AAA..MMM": {"lo": "AAA", "hi": "MMM", "rows": 120, "checksum": "112233"}},
    )
    archive_file = tmp_path / "portable_archive.tar.gz"
    exporter.export_bundle_archive(archive_file, fmt="json")

    clean_importer = PortableStateImporter(archive_file)
    assert clean_importer.validate() is True
    assert "inventory" in clean_importer.get_schemas()
    assert clean_importer.can_resume_without_rescan("inventory") is True

    pending = clean_importer.pending_ranges("inventory", [("AAA", "MMM"), ("MMM", "ZZZ")])
    assert pending == [("MMM", "ZZZ")]


def test_portability_validation_error_on_tampered_or_missing_schema(tmp_path):
    invalid_pkg = {
        "format_version": 1,
        "schemas": {"broken": {"columns": "not-a-list"}},
    }
    importer = PortableStateImporter(invalid_pkg)
    with pytest.raises(PortabilityValidationError):
        importer.validate()


def test_safe_allowed_ips_in_tool_config_export(tmp_path):
    exporter = PortableManifestExporter(source_engine="postgres", target_engine="postgres")
    exporter.set_tool_config(
        source_host="10.0.0.1",
        target_host="127.0.0.1",
        source_port=5432,
        target_port=5433,
    )
    pkg = exporter.to_package_dict()
    importer = PortableStateImporter(pkg)
    cfg = importer.get_tool_config()
    assert cfg["source_host"] == "10.0.0.1"
    assert cfg["target_host"] == "127.0.0.1"


def test_composite_cursor_and_typed_boundaries_portability(tmp_path):
    import datetime
    import decimal
    import uuid

    uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    dt = datetime.datetime(2026, 10, 17, 12, 0, 0)
    dec = decimal.Decimal("12345.67")

    exporter = PortableManifestExporter()
    exporter.add_table(
        "composite_events",
        columns=[
            {"name": "event_uuid", "type": "UUID", "primary_key": True},
            {"name": "occurred_at", "type": "TIMESTAMP", "primary_key": True},
            {"name": "score", "type": "DECIMAL(10,2)"},
        ],
        primary_key=["event_uuid", "occurred_at"],
    )
    exporter.add_checkpoint(
        "composite_events",
        done_ranges={"chunk_0": [15, "9999"]},
        cursors={
            "chunk_0": {
                "lo": [str(uid), dt.isoformat()],
                "hi": [str(uid), (dt + datetime.timedelta(hours=1)).isoformat()],
                "rows": 15,
                "checksum": "9999",
            }
        },
    )
    archive_zip = tmp_path / "typed_bundle.zip"
    exporter.export_bundle_archive(archive_zip, fmt="json")

    importer = PortableStateImporter(archive_zip)
    assert importer.validate() is True
    assert importer.can_resume_without_rescan("composite_events") is True
    done = importer.completed_ranges("composite_events")
    assert done["chunk_0"] == (15, "9999")


def test_fail_on_collision_strategy():
    exporter = PortableManifestExporter()
    cols = [
        {"name": "amount", "type": "DECIMAL(10,2)"},
        {"name": "AMOUNT", "type": "DECIMAL(10,2)"},
    ]
    with pytest.raises(PortabilityValidationError) as exc:
        exporter.add_table("billing", columns=cols, collision_strategy="fail_on_collision")
    assert "collision detected" in str(exc.value)


def test_yaml_export_import_roundtrip(tmp_path):
    exporter = PortableManifestExporter(source_engine="mysql", target_engine="postgres")
    exporter.add_table(
        "catalog",
        columns=[{"name": "sku", "type": "VARCHAR(50)", "primary_key": True}],
        primary_key=["sku"],
        sequence_offsets={"last_value": 777},
    )
    exporter.add_checkpoint(
        "catalog",
        done_ranges={"A..M": [50, "1234"]},
        watermark="gtid_mysql_bin:100",
    )
    yaml_str = exporter.export_yaml()
    assert "catalog" in yaml_str
    assert "gtid_mysql_bin:100" in yaml_str

    yaml_file = tmp_path / "package.yaml"
    yaml_file.write_text(yaml_str)

    importer = PortableStateImporter(yaml_file)
    assert importer.validate() is True
    assert importer.get_watermark("catalog") == "gtid_mysql_bin:100"
    offsets = importer.resume_offsets("catalog")
    assert offsets["sequence_offsets"]["last_value"] == 777
    assert importer.can_resume_without_rescan("catalog") is True

