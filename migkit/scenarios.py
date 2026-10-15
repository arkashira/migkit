"""Migration scenarios helpers (backlog P0-SCENARIOS, F1.dual):
character set and collation conversion, storage engine transformation,
database consolidation and schema splitting, and dual writes drift detection.
"""
from __future__ import annotations

import enum
import hashlib
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


class CharsetMigrationHelper:
    """Character set and collation migration helpers (Item 161)."""

    MYSQL_COLLATION_MAP = {
        "latin1_swedish_ci": "utf8mb4_0900_ai_ci",
        "latin1_general_ci": "utf8mb4_0900_ai_ci",
        "latin1_bin": "utf8mb4_bin",
        "utf8_general_ci": "utf8mb4_general_ci",
        "utf8_unicode_ci": "utf8mb4_unicode_ci",
        "utf8_bin": "utf8mb4_bin",
        "utf8mb3_general_ci": "utf8mb4_general_ci",
        "utf8mb3_unicode_ci": "utf8mb4_unicode_ci",
        "utf8mb3_bin": "utf8mb4_bin",
    }

    CROSS_ENGINE_COLLATION_MAP = {
        ("mysql", "postgres"): {
            "utf8mb4_unicode_ci": "en_US.utf8",
            "utf8mb4_bin": "C",
            "latin1_general_ci": "en_US.iso88591",
            "latin1_bin": "C",
        },
        ("postgres", "mysql"): {
            "en_US.utf8": "utf8mb4_unicode_ci",
            "C.UTF-8": "utf8mb4_bin",
            "C": "utf8mb4_bin",
            "default": "utf8mb4_0900_ai_ci",
        },
        ("mssql", "mysql"): {
            "SQL_Latin1_General_CP1_CI_AS": "utf8mb4_0900_ai_ci",
            "Latin1_General_BIN": "utf8mb4_bin",
            "Latin1_General_CI_AS": "utf8mb4_0900_ai_ci",
        },
    }

    CHARSET_BYTE_WIDTHS = {
        "latin1": 1,
        "cp1252": 1,
        "ascii": 1,
        "utf8": 3,
        "utf8mb3": 3,
        "utf8mb4": 4,
        "utf16": 4,
        "utf32": 4,
        "gbk": 2,
        "gb18030": 4,
        "big5": 2,
        "shift_jis": 2,
    }

    @classmethod
    def map_collation(
        cls,
        source_engine: str,
        target_engine: str,
        source_collation: str,
        target_charset: str = "utf8mb4",
    ) -> str:
        s_eng = source_engine.lower()
        t_eng = target_engine.lower()
        s_coll = (source_collation or "").strip()

        if s_eng == t_eng and s_eng == "mysql":
            if s_coll in cls.MYSQL_COLLATION_MAP:
                return cls.MYSQL_COLLATION_MAP[s_coll]
            if target_charset.lower() == "utf8mb4":
                if s_coll.endswith("_bin"):
                    return "utf8mb4_bin"
                if "unicode" in s_coll:
                    return "utf8mb4_unicode_ci"
                return "utf8mb4_0900_ai_ci"

        cross = cls.CROSS_ENGINE_COLLATION_MAP.get((s_eng, t_eng), {})
        if s_coll in cross:
            return cross[s_coll]

        if t_eng == "postgres":
            return "C" if s_coll.endswith(("_bin", "_cs")) else "en_US.utf8"
        if t_eng == "mysql":
            return "utf8mb4_bin" if s_coll.endswith(("_bin", "_cs", "C")) else "utf8mb4_0900_ai_ci"

        return s_coll

    @staticmethod
    def detect_invalid_byte_sequences(data: bytes, encoding: str = "utf-8") -> List[Dict[str, Any]]:
        findings = []
        i = 0
        n = len(data)

        if encoding.lower() in ("utf-8", "utf8", "utf8mb4"):
            while i < n:
                b1 = data[i]
                if b1 <= 0x7F:
                    i += 1
                elif 0xC2 <= b1 <= 0xDF:
                    if i + 1 >= n:
                        findings.append({"offset": i, "bytes": data[i:i+1].hex(), "reason": "truncated_2_byte_sequence"})
                        break
                    b2 = data[i + 1]
                    if not (0x80 <= b2 <= 0xBF):
                        findings.append({"offset": i, "bytes": data[i:i+2].hex(), "reason": "invalid_continuation_byte"})
                    i += 2
                elif 0xE0 <= b1 <= 0xEF:
                    if i + 2 >= n:
                        findings.append({"offset": i, "bytes": data[i:n].hex(), "reason": "truncated_3_byte_sequence"})
                        break
                    b2, b3 = data[i + 1], data[i + 2]
                    if b1 == 0xE0 and not (0xA0 <= b2 <= 0xBF):
                        findings.append({"offset": i, "bytes": data[i:i+3].hex(), "reason": "overlong_3_byte_sequence"})
                    elif b1 == 0xED and not (0x80 <= b2 <= 0x9F):
                        findings.append({"offset": i, "bytes": data[i:i+3].hex(), "reason": "surrogate_utf8_sequence"})
                    elif not ((0x80 <= b2 <= 0xBF) and (0x80 <= b3 <= 0xBF)):
                        findings.append({"offset": i, "bytes": data[i:i+3].hex(), "reason": "invalid_continuation_byte"})
                    i += 3
                elif 0xF0 <= b1 <= 0xF4:
                    if i + 3 >= n:
                        findings.append({"offset": i, "bytes": data[i:n].hex(), "reason": "truncated_4_byte_sequence"})
                        break
                    b2, b3, b4 = data[i + 1], data[i + 2], data[i + 3]
                    if b1 == 0xF0 and not (0x90 <= b2 <= 0xBF):
                        findings.append({"offset": i, "bytes": data[i:i+4].hex(), "reason": "overlong_4_byte_sequence"})
                    elif b1 == 0xF4 and not (0x80 <= b2 <= 0x8F):
                        findings.append({"offset": i, "bytes": data[i:i+4].hex(), "reason": "out_of_unicode_range"})
                    elif not ((0x80 <= b2 <= 0xBF) and (0x80 <= b3 <= 0xBF) and (0x80 <= b4 <= 0xBF)):
                        findings.append({"offset": i, "bytes": data[i:i+4].hex(), "reason": "invalid_continuation_byte"})
                    i += 4
                else:
                    findings.append({"offset": i, "bytes": bytes([b1]).hex(), "reason": "illegal_start_byte"})
                    i += 1
            return findings

        try:
            data.decode(encoding)
        except UnicodeDecodeError as err:
            findings.append({
                "offset": err.start,
                "bytes": data[err.start:err.end].hex(),
                "reason": f"decode_error: {err.reason}",
            })
        return findings

    @classmethod
    def detect_unrepresentable(cls, text: str, target_charset: str) -> List[Dict[str, Any]]:
        codec = target_charset.lower().replace("-", "_")
        if codec in ("utf8mb4", "utf_8", "utf8"):
            codec = "utf-8"
        elif codec == "utf8mb3":
            return [
                {"char": ch, "code_point": hex(ord(ch)), "reason": "exceeds_bmp_limit"}
                for ch in text if ord(ch) > 0xFFFF
            ]

        unrep = []
        for ch in text:
            try:
                ch.encode(codec)
            except (UnicodeEncodeError, LookupError):
                unrep.append({"char": ch, "code_point": hex(ord(ch)), "reason": "unrepresentable_in_target_encoding"})
        return unrep

    @staticmethod
    def detect_mojibake(text: str) -> bool:
        if not text:
            return False
        common_mojibake = ("Ã©", "Ã¨", "Ã ", "Ã§", "Ã±", "Ã¼", "Ã¶", "Ã¤", "Ã®", "Ã¢", "Ã´", "â€™", "â€œ", "â€“")
        return any(pat in text for pat in common_mojibake)

    @staticmethod
    def repair_mojibake(text: str, source_encoding: str = "latin1", target_encoding: str = "utf-8") -> str:
        if not text:
            return text
        try:
            raw_bytes = text.encode(source_encoding)
            repaired = raw_bytes.decode(target_encoding)
            return repaired
        except (UnicodeEncodeError, UnicodeDecodeError):
            return text

    @classmethod
    def check_column_index_prefix(
        cls,
        data_type: str,
        length: int,
        target_charset: str,
        max_index_bytes: int = 3072,
    ) -> Dict[str, Any]:
        byte_width = cls.CHARSET_BYTE_WIDTHS.get(target_charset.lower(), 4)
        total_index_bytes = length * byte_width
        fits = total_index_bytes <= max_index_bytes
        max_allowed_len = max_index_bytes // byte_width
        return {
            "data_type": data_type,
            "declared_length": length,
            "target_charset": target_charset,
            "byte_width": byte_width,
            "total_bytes": total_index_bytes,
            "max_index_bytes": max_index_bytes,
            "fits": fits,
            "recommended_prefix_length": min(length, max_allowed_len),
        }

    @classmethod
    def generate_charset_alter_ddl(
        cls,
        table: str,
        column: str,
        data_type: str,
        target_charset: str = "utf8mb4",
        target_collation: Optional[str] = None,
        nullable: bool = True,
        default: Optional[str] = None,
    ) -> str:
        coll_clause = f" COLLATE {target_collation}" if target_collation else ""
        null_clause = " NULL" if nullable else " NOT NULL"
        default_clause = f" DEFAULT '{default}'" if default is not None else ""
        return (
            f"ALTER TABLE `{table}` MODIFY `{column}` {data_type} "
            f"CHARACTER SET {target_charset}{coll_clause}{null_clause}{default_clause}"
        )


@dataclass(frozen=True)
class EngineTraits:
    """Storage engine capabilities and constraints (Item 162)."""
    name: str
    transactions: bool
    foreign_keys: bool
    locks: str
    fulltext: bool
    spatial: bool
    crash_safe: bool
    persistent: bool
    auto_increment_group: bool
    max_key_bytes: int


KNOWN_ENGINES: Dict[str, EngineTraits] = {
    "innodb": EngineTraits(
        name="InnoDB",
        transactions=True,
        foreign_keys=True,
        locks="row",
        fulltext=True,
        spatial=True,
        crash_safe=True,
        persistent=True,
        auto_increment_group=False,
        max_key_bytes=3072,
    ),
    "myisam": EngineTraits(
        name="MyISAM",
        transactions=False,
        foreign_keys=False,
        locks="table",
        fulltext=True,
        spatial=True,
        crash_safe=False,
        persistent=True,
        auto_increment_group=True,
        max_key_bytes=1000,
    ),
    "aria": EngineTraits(
        name="Aria",
        transactions=False,
        foreign_keys=False,
        locks="page",
        fulltext=True,
        spatial=True,
        crash_safe=True,
        persistent=True,
        auto_increment_group=True,
        max_key_bytes=2000,
    ),
    "memory": EngineTraits(
        name="MEMORY",
        transactions=False,
        foreign_keys=False,
        locks="table",
        fulltext=False,
        spatial=False,
        crash_safe=False,
        persistent=False,
        auto_increment_group=False,
        max_key_bytes=3072,
    ),
    "archive": EngineTraits(
        name="ARCHIVE",
        transactions=False,
        foreign_keys=False,
        locks="row",
        fulltext=False,
        spatial=False,
        crash_safe=True,
        persistent=True,
        auto_increment_group=False,
        max_key_bytes=0,
    ),
}


@dataclass
class EngineCompatibilityResult:
    compatible: bool
    source_engine: str
    target_engine: str
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    altered_options: List[str] = field(default_factory=list)
    ddl_statements: List[str] = field(default_factory=list)


class StorageEngineTransformationHelper:
    """Storage engine transformation helper (Item 162)."""

    MYISAM_SPECIFIC_OPTIONS = {"PACK_KEYS", "CHECKSUM", "DELAY_KEY_WRITE", "ROW_FORMAT=FIXED"}

    @classmethod
    def check_engine_compatibility(
        cls,
        table_name: str,
        source_engine: str,
        target_engine: str,
        indexes: List[Dict[str, Any]],
        columns: List[Dict[str, Any]],
        table_options: Optional[Dict[str, Any]] = None,
        mysql_version: str = "8.0",
    ) -> EngineCompatibilityResult:
        src = source_engine.lower()
        dst = target_engine.lower()
        src_traits = KNOWN_ENGINES.get(src)
        dst_traits = KNOWN_ENGINES.get(dst)

        errors: List[str] = []
        warnings: List[str] = []
        altered_options: List[str] = []

        if not dst_traits:
            return EngineCompatibilityResult(
                compatible=False,
                source_engine=source_engine,
                target_engine=target_engine,
                errors=[f"Unknown target storage engine '{target_engine}'"],
            )

        if not src_traits:
            warnings.append(f"Source engine '{source_engine}' is not in known registry; assuming generic defaults")

        has_fulltext = any(idx.get("type", "").upper() == "FULLTEXT" for idx in indexes)
        if has_fulltext:
            if not dst_traits.fulltext:
                errors.append(f"Target engine '{dst_traits.name}' does not support FULLTEXT indexes on table '{table_name}'")
            elif src == "myisam" and dst == "innodb":
                warnings.append(
                    "InnoDB full-text search uses innodb_ft_min_token_size (default 3) instead of "
                    "MyISAM's ft_min_word_len (default 4); stopwords and ranking behavior may differ"
                )

        has_spatial = any(idx.get("type", "").upper() == "SPATIAL" for idx in indexes)
        if has_spatial:
            if not dst_traits.spatial:
                errors.append(f"Target engine '{dst_traits.name}' does not support SPATIAL indexes on table '{table_name}'")
            elif dst == "innodb":
                for col in columns:
                    if col.get("type", "").upper() in ("GEOMETRY", "POINT", "POLYGON", "LINESTRING"):
                        if col.get("nullable", True):
                            errors.append(f"InnoDB SPATIAL index requires NOT NULL on geometry column '{col.get('name')}'")

        for idx in indexes:
            if idx.get("primary", False):
                cols = idx.get("columns", [])
                for col in columns:
                    if col.get("extra", "").lower() == "auto_increment":
                        col_name = col.get("name")
                        if col_name in cols and cols.index(col_name) > 0:
                            if not dst_traits.auto_increment_group:
                                errors.append(
                                    f"Target engine '{dst_traits.name}' does not support composite per-group AUTO_INCREMENT "
                                    f"where '{col_name}' is not the first key column in '{table_name}'"
                                )

        if dst == "memory":
            warnings.append(f"MEMORY engine is non-persistent; table '{table_name}' will lose all data across server restarts")

        if table_options:
            for opt, val in table_options.items():
                opt_str = f"{opt}={val}" if val is not None else opt
                if opt.upper() in {"CHECKSUM", "DELAY_KEY_WRITE", "PACK_KEYS"} or opt_str.upper() in cls.MYISAM_SPECIFIC_OPTIONS:
                    altered_options.append(f"Pruned MyISAM option '{opt_str}' for target '{dst_traits.name}'")

        ddls = []
        if not errors:
            ddls.append(f"ALTER TABLE `{table_name}` ENGINE = {dst_traits.name}")

        return EngineCompatibilityResult(
            compatible=len(errors) == 0,
            source_engine=source_engine,
            target_engine=target_engine,
            errors=errors,
            warnings=warnings,
            altered_options=altered_options,
            ddl_statements=ddls,
        )


@dataclass
class ConsolidationPlan:
    tenant_column: str
    strategy: str
    tenant_configs: Dict[str, Dict[str, Any]]
    schema_adjustments: List[str] = field(default_factory=list)
    key_collisions: Dict[str, List[Any]] = field(default_factory=dict)


class ConsolidationCoordinator:
    """Database consolidation coordinator (Item 163)."""

    @classmethod
    def plan_consolidation(
        cls,
        tenants: List[Dict[str, Any]],
        tenant_column: str = "tenant_id",
        key_strategy: str = "tenant",
        base_stride: int = 1_000_000,
    ) -> ConsolidationPlan:
        tenant_configs = {}
        for idx, t in enumerate(tenants):
            t_id = t["id"]
            offset = t.get("offset", idx * base_stride)
            tenant_configs[t_id] = {
                "id": t_id,
                "db": t.get("db", t_id),
                "offset": offset,
            }

        adjustments = []
        if key_strategy == "tenant":
            adjustments.append(f"Add `{tenant_column}` VARCHAR(64) NOT NULL to all consolidated tables")
            adjustments.append(f"Alter PRIMARY KEYs to compound `({tenant_column}, id)`")
        elif key_strategy == "offset":
            adjustments.append(f"Apply ID offset per tenant and cascade to referencing FOREIGN KEY columns")
        elif key_strategy == "remap":
            adjustments.append(f"Generate global surrogate keys with stride {base_stride}")

        return ConsolidationPlan(
            tenant_column=tenant_column,
            strategy=key_strategy,
            tenant_configs=tenant_configs,
            schema_adjustments=adjustments,
        )

    @classmethod
    def detect_key_collisions(cls, tenant_keys: Dict[str, Set[Any]]) -> Dict[Any, List[str]]:
        collisions: Dict[Any, List[str]] = {}
        seen: Dict[Any, str] = {}
        for tenant_id, keys in tenant_keys.items():
            for k in keys:
                if k in seen:
                    if k not in collisions:
                        collisions[k] = [seen[k]]
                    collisions[k].append(tenant_id)
                else:
                    seen[k] = tenant_id
        return collisions

    @classmethod
    def transform_row(
        cls,
        row: Dict[str, Any],
        tenant_id: Any,
        tenant_column: str = "tenant_id",
        key_columns: Optional[List[str]] = None,
        foreign_key_columns: Optional[List[str]] = None,
        offset: int = 0,
        strategy: str = "tenant",
    ) -> Dict[str, Any]:
        out = dict(row)
        if strategy == "tenant":
            out[tenant_column] = tenant_id
        elif strategy in ("offset", "remap"):
            keys = set(key_columns or ["id"])
            fks = set(foreign_key_columns or [])
            for col in keys | fks:
                if col in out and isinstance(out[col], int):
                    out[col] = out[col] + offset
            out[tenant_column] = tenant_id
        return out


@dataclass
class PrefixSplitPlan:
    table_mappings: Dict[str, str]
    target_schemas: Set[str]
    cross_references: List[Dict[str, str]] = field(default_factory=list)


class SplittingCoordinator:
    """Table prefix and schema splitting coordinator (Item 163)."""

    @classmethod
    def plan_prefix_split(
        cls,
        tables: List[str],
        prefix_to_target: Dict[str, str],
        foreign_keys: Optional[List[Dict[str, str]]] = None,
    ) -> PrefixSplitPlan:
        table_mappings = {}
        target_schemas = set()

        for tbl in tables:
            target = None
            for pfx, tgt in prefix_to_target.items():
                if tbl.startswith(pfx):
                    target = tgt
                    break
            if not target:
                target = prefix_to_target.get("*", "default")
            table_mappings[tbl] = target
            target_schemas.add(target)

        cross_refs = []
        if foreign_keys:
            for fk in foreign_keys:
                parent_tbl = fk.get("parent_table", "")
                child_tbl = fk.get("child_table", "")
                parent_tgt = table_mappings.get(parent_tbl)
                child_tgt = table_mappings.get(child_tbl)
                if parent_tgt and child_tgt and parent_tgt != child_tgt:
                    cross_refs.append({
                        "fk_name": fk.get("name", "fk"),
                        "child_table": child_tbl,
                        "child_target": child_tgt,
                        "parent_table": parent_tbl,
                        "parent_target": parent_tgt,
                        "action": "sever_physical_fk_and_decouple",
                    })

        return PrefixSplitPlan(
            table_mappings=table_mappings,
            target_schemas=target_schemas,
            cross_references=cross_refs,
        )
