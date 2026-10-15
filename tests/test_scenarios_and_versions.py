"""Unit tests for scenarios (Items 161, 162, 163 and 65)."""
import time
import pytest

from migkit.scenarios import (
    CharsetMigrationHelper,
    StorageEngineTransformationHelper,
    ConsolidationCoordinator,
    SplittingCoordinator,
)


def test_charset_collation_mapping():
    assert CharsetMigrationHelper.map_collation("mysql", "mysql", "latin1_swedish_ci") == "utf8mb4_0900_ai_ci"
    assert CharsetMigrationHelper.map_collation("mysql", "mysql", "latin1_bin") == "utf8mb4_bin"
    assert CharsetMigrationHelper.map_collation("mysql", "mysql", "utf8_general_ci") == "utf8mb4_general_ci"
    assert CharsetMigrationHelper.map_collation("mysql", "mysql", "utf8_unicode_ci") == "utf8mb4_unicode_ci"
    assert CharsetMigrationHelper.map_collation("mysql", "postgres", "utf8mb4_unicode_ci") == "en_US.utf8"
    assert CharsetMigrationHelper.map_collation("mysql", "postgres", "utf8mb4_bin") == "C"
    assert CharsetMigrationHelper.map_collation("postgres", "mysql", "en_US.utf8") == "utf8mb4_unicode_ci"
    assert CharsetMigrationHelper.map_collation("postgres", "mysql", "C") == "utf8mb4_bin"
    assert CharsetMigrationHelper.map_collation("mssql", "mysql", "SQL_Latin1_General_CP1_CI_AS") == "utf8mb4_0900_ai_ci"


def test_charset_invalid_byte_sequences():
    valid = "Hello world, 123!".encode("utf-8")
    assert CharsetMigrationHelper.detect_invalid_byte_sequences(valid, "utf-8") == []

    # Illegal start byte 0xFF
    illegal_start = b"hello \xff world"
    findings = CharsetMigrationHelper.detect_invalid_byte_sequences(illegal_start, "utf-8")
    assert len(findings) == 1
    assert findings[0]["reason"] == "illegal_start_byte"
    assert findings[0]["offset"] == 6

    # Truncated 2-byte sequence
    truncated_2 = b"prefix \xc2"
    findings = CharsetMigrationHelper.detect_invalid_byte_sequences(truncated_2, "utf-8")
    assert len(findings) == 1
    assert findings[0]["reason"] == "truncated_2_byte_sequence"

    # Invalid continuation byte
    invalid_cont = b"\xc2\x20"
    findings = CharsetMigrationHelper.detect_invalid_byte_sequences(invalid_cont, "utf-8")
    assert len(findings) == 1
    assert findings[0]["reason"] == "invalid_continuation_byte"

    # Overlong 3-byte sequence (e.g. 0xE0 0x80 0xAF)
    overlong_3 = b"\xe0\x80\xaf"
    findings = CharsetMigrationHelper.detect_invalid_byte_sequences(overlong_3, "utf-8")
    assert any(f["reason"] == "overlong_3_byte_sequence" for f in findings)


def test_charset_unrepresentable_characters():
    text = "Hello \u20ac Euro and emoji \U0001F600"
    unrep_latin1 = CharsetMigrationHelper.detect_unrepresentable(text, "latin1")
    assert len(unrep_latin1) == 2
    chars = [u["char"] for u in unrep_latin1]
    assert "\u20ac" in chars
    assert "\U0001F600" in chars

    # utf8mb3 rejects code points > 0xFFFF
    unrep_utf8mb3 = CharsetMigrationHelper.detect_unrepresentable(text, "utf8mb3")
    assert len(unrep_utf8mb3) == 1
    assert unrep_utf8mb3[0]["char"] == "\U0001F600"
    assert unrep_utf8mb3[0]["reason"] == "exceeds_bmp_limit"

    # utf8mb4 supports all characters
    unrep_utf8mb4 = CharsetMigrationHelper.detect_unrepresentable(text, "utf8mb4")
    assert unrep_utf8mb4 == []


def test_charset_mojibake_detection_and_repair():
    clean_text = "Café au lait"
    mojibake_text = clean_text.encode("utf-8").decode("latin1")
    assert CharsetMigrationHelper.detect_mojibake(mojibake_text) is True
    assert CharsetMigrationHelper.detect_mojibake(clean_text) is False

    repaired = CharsetMigrationHelper.repair_mojibake(mojibake_text, "latin1", "utf-8")
    assert repaired == clean_text


def test_charset_alter_ddl_and_index_prefix():
    ddl = CharsetMigrationHelper.generate_charset_alter_ddl(
        table="users",
        column="bio",
        data_type="TEXT",
        target_charset="utf8mb4",
        target_collation="utf8mb4_unicode_ci",
        nullable=False,
    )
    assert ddl == "ALTER TABLE `users` MODIFY `bio` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL"

    res = CharsetMigrationHelper.check_column_index_prefix("VARCHAR", 255, "utf8mb4", max_index_bytes=767)
    assert res["fits"] is False
    assert res["recommended_prefix_length"] == 191


def test_storage_engine_transformation_myisam_to_innodb():
    indexes = [
        {"name": "PRIMARY", "primary": True, "columns": ["id"]},
        {"name": "ft_title", "type": "FULLTEXT", "columns": ["title", "body"]},
    ]
    columns = [
        {"name": "id", "type": "INT", "extra": "auto_increment", "nullable": False},
        {"name": "title", "type": "VARCHAR(255)", "nullable": False},
        {"name": "body", "type": "TEXT", "nullable": True},
    ]
    options = {"CHECKSUM": "1", "DELAY_KEY_WRITE": "1"}

    res = StorageEngineTransformationHelper.check_engine_compatibility(
        table_name="articles",
        source_engine="MyISAM",
        target_engine="InnoDB",
        indexes=indexes,
        columns=columns,
        table_options=options,
    )

    assert res.compatible is True
    assert len(res.errors) == 0
    assert any("innodb_ft_min_token_size" in w for w in res.warnings)
    assert len(res.altered_options) == 2
    assert "ALTER TABLE `articles` ENGINE = InnoDB" in res.ddl_statements


def test_storage_engine_incompatibility_checks():
    # MyISAM to MEMORY with FULLTEXT should fail
    indexes = [{"name": "ft_idx", "type": "FULLTEXT", "columns": ["content"]}]
    columns = [{"name": "content", "type": "TEXT"}]
    res = StorageEngineTransformationHelper.check_engine_compatibility(
        table_name="notes",
        source_engine="MyISAM",
        target_engine="MEMORY",
        indexes=indexes,
        columns=columns,
    )
    assert res.compatible is False
    assert any("does not support FULLTEXT" in err for err in res.errors)

    # Composite group auto_increment (MyISAM specific) to InnoDB should fail
    grp_indexes = [{"name": "PRIMARY", "primary": True, "columns": ["tenant_id", "id"]}]
    grp_columns = [
        {"name": "tenant_id", "type": "INT", "extra": ""},
        {"name": "id", "type": "INT", "extra": "auto_increment"},
    ]
    res_grp = StorageEngineTransformationHelper.check_engine_compatibility(
        table_name="orders",
        source_engine="MyISAM",
        target_engine="InnoDB",
        indexes=grp_indexes,
        columns=grp_columns,
    )
    assert res_grp.compatible is False
    assert any("composite per-group AUTO_INCREMENT" in err for err in res_grp.errors)

    # Spatial index on nullable column to InnoDB should fail
    sp_indexes = [{"name": "sp_idx", "type": "SPATIAL", "columns": ["geom"]}]
    sp_columns = [{"name": "geom", "type": "GEOMETRY", "nullable": True}]
    res_sp = StorageEngineTransformationHelper.check_engine_compatibility(
        table_name="points",
        source_engine="MyISAM",
        target_engine="InnoDB",
        indexes=sp_indexes,
        columns=sp_columns,
    )
    assert res_sp.compatible is False
    assert any("SPATIAL index requires NOT NULL" in err for err in res_sp.errors)


def test_consolidation_coordinator_planning_and_transform():
    tenants = [
        {"id": "tenant_a", "db": "tenant_a_db", "offset": 0},
        {"id": "tenant_b", "db": "tenant_b_db", "offset": 1000000},
    ]

    plan = ConsolidationCoordinator.plan_consolidation(tenants, tenant_column="tenant_id", key_strategy="tenant")
    assert plan.tenant_column == "tenant_id"
    assert plan.strategy == "tenant"
    assert len(plan.tenant_configs) == 2

    row = {"id": 42, "user_id": 99, "name": "Alice"}
    transformed = ConsolidationCoordinator.transform_row(row, "tenant_a", tenant_column="tenant_id", strategy="tenant")
    assert transformed["tenant_id"] == "tenant_a"
    assert transformed["id"] == 42

    plan_offset = ConsolidationCoordinator.plan_consolidation(tenants, tenant_column="tenant_id", key_strategy="offset")
    transformed_offset = ConsolidationCoordinator.transform_row(
        row,
        tenant_id="tenant_b",
        tenant_column="tenant_id",
        key_columns=["id"],
        foreign_key_columns=["user_id"],
        offset=1000000,
        strategy="offset",
    )
    assert transformed_offset["id"] == 1000042
    assert transformed_offset["user_id"] == 1000099
    assert transformed_offset["tenant_id"] == "tenant_b"


def test_consolidation_key_collisions():
    tenant_keys = {
        "tenant_a": {1, 2, 3, 4},
        "tenant_b": {3, 4, 5, 6},
        "tenant_c": {4, 7, 8},
    }
    collisions = ConsolidationCoordinator.detect_key_collisions(tenant_keys)
    assert 3 in collisions
    assert set(collisions[3]) == {"tenant_a", "tenant_b"}
    assert 4 in collisions
    assert set(collisions[4]) == {"tenant_a", "tenant_b", "tenant_c"}
    assert 1 not in collisions


def test_schema_splitting_coordinator():
    tables = [
        "auth_users",
        "auth_tokens",
        "billing_invoices",
        "billing_payments",
        "audit_logs",
    ]
    prefix_to_target = {
        "auth_": "auth_db",
        "billing_": "billing_db",
        "*": "common_db",
    }
    fks = [
        {"name": "fk_invoice_user", "child_table": "billing_invoices", "parent_table": "auth_users"},
        {"name": "fk_payment_invoice", "child_table": "billing_payments", "parent_table": "billing_invoices"},
    ]

    plan = SplittingCoordinator.plan_prefix_split(tables, prefix_to_target, foreign_keys=fks)
    assert plan.table_mappings["auth_users"] == "auth_db"
    assert plan.table_mappings["billing_invoices"] == "billing_db"
    assert plan.table_mappings["audit_logs"] == "common_db"
    assert len(plan.cross_references) == 1
    assert plan.cross_references[0]["fk_name"] == "fk_invoice_user"
    assert plan.cross_references[0]["action"] == "sever_physical_fk_and_decouple"
