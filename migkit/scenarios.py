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

from . import decide


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
<<<<<<< HEAD
||||||| 3c33f9a


class DualWriteDriftType(str, enum.Enum):
    IN_SYNC = "IN_SYNC"
    OUT_OF_ORDER_WRITE = "OUT_OF_ORDER_WRITE"
    IN_FLIGHT_LAG = "IN_FLIGHT_LAG"
    GENUINE_DRIFT = "GENUINE_DRIFT"


@dataclass
class DualWriteClassificationResult:
    status: DualWriteDriftType
    key: Any
    detail: str
    source_timestamp: Optional[float] = None
    target_timestamp: Optional[float] = None
    time_delta_seconds: Optional[float] = None
    source_version: Optional[int] = None
    target_version: Optional[int] = None


class DualWritesDriftDetector:
    """Dual writes drift detector (Item 65, F1.dual)."""

    def __init__(self, grace_window_seconds: float = 5.0):
        self.grace_window_seconds = grace_window_seconds
        self.probe_history: Dict[Any, List[Dict[str, Any]]] = {}

    @staticmethod
    def _extract_time(row: Optional[Dict[str, Any]], col: Optional[str]) -> Optional[float]:
        if not row or not col or col not in row:
            return None
        val = row[col]
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            try:
                import datetime
                dt = datetime.datetime.fromisoformat(val.replace("Z", "+00:00"))
                return dt.timestamp()
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _extract_version(row: Optional[Dict[str, Any]], col: Optional[str]) -> Optional[int]:
        if not row or not col or col not in row:
            return None
        val = row[col]
        if isinstance(val, int):
            return val
        if isinstance(val, str) and val.isdigit():
            return int(val)
        return None

    def classify_row(
        self,
        key: Any,
        source_row: Optional[Dict[str, Any]],
        target_row: Optional[Dict[str, Any]],
        timestamp_col: Optional[str] = "updated_at",
        version_col: Optional[str] = "version",
        current_time: Optional[float] = None,
    ) -> DualWriteClassificationResult:
        now = current_time if current_time is not None else time.time()

        if source_row is None and target_row is None:
            return DualWriteClassificationResult(
                status=DualWriteDriftType.IN_SYNC,
                key=key,
                detail="Both source and target rows are absent",
            )

        src_ts = self._extract_time(source_row, timestamp_col)
        dst_ts = self._extract_time(target_row, timestamp_col)
        src_ver = self._extract_version(source_row, version_col)
        dst_ver = self._extract_version(target_row, version_col)

        if source_row is not None and target_row is None:
            if src_ts is not None and (now - src_ts) <= self.grace_window_seconds:
                return DualWriteClassificationResult(
                    status=DualWriteDriftType.IN_FLIGHT_LAG,
                    key=key,
                    source_timestamp=src_ts,
                    detail=f"Row committed on source {now - src_ts:.2f}s ago; pending replication to target",
                )
            return DualWriteClassificationResult(
                status=DualWriteDriftType.GENUINE_DRIFT,
                key=key,
                source_timestamp=src_ts,
                detail="Row present on source but missing on target beyond dual-write grace window",
            )

        if source_row is None and target_row is not None:
            if dst_ts is not None and (now - dst_ts) <= self.grace_window_seconds:
                return DualWriteClassificationResult(
                    status=DualWriteDriftType.IN_FLIGHT_LAG,
                    key=key,
                    target_timestamp=dst_ts,
                    detail=f"Row committed on target {now - dst_ts:.2f}s ago; pending arrival on source",
                )
            return DualWriteClassificationResult(
                status=DualWriteDriftType.GENUINE_DRIFT,
                key=key,
                target_timestamp=dst_ts,
                detail="Row present on target only beyond dual-write grace window (orphaned write)",
            )

        assert source_row is not None and target_row is not None
        if source_row == target_row:
            return DualWriteClassificationResult(
                status=DualWriteDriftType.IN_SYNC,
                key=key,
                source_timestamp=src_ts,
                target_timestamp=dst_ts,
                source_version=src_ver,
                target_version=dst_ver,
                detail="Source and target rows are identical",
            )

        time_delta = abs(src_ts - dst_ts) if (src_ts is not None and dst_ts is not None) else None
        latest_ts = max(src_ts, dst_ts) if (src_ts is not None and dst_ts is not None) else (src_ts or dst_ts)
        is_recent = (now - latest_ts) <= self.grace_window_seconds if latest_ts is not None else False

        if is_recent and src_ver is not None and dst_ver is not None:
            if src_ver == dst_ver and time_delta is not None and time_delta <= self.grace_window_seconds:
                return DualWriteClassificationResult(
                    status=DualWriteDriftType.OUT_OF_ORDER_WRITE,
                    key=key,
                    source_version=src_ver,
                    target_version=dst_ver,
                    source_timestamp=src_ts,
                    target_timestamp=dst_ts,
                    time_delta_seconds=time_delta,
                    detail=f"Same version {src_ver} with payload divergence within grace window ({time_delta:.2f}s)",
                )
            if src_ver > dst_ver:
                if time_delta is not None and time_delta <= self.grace_window_seconds:
                    return DualWriteClassificationResult(
                        status=DualWriteDriftType.IN_FLIGHT_LAG,
                        key=key,
                        source_version=src_ver,
                        target_version=dst_ver,
                        source_timestamp=src_ts,
                        target_timestamp=dst_ts,
                        time_delta_seconds=time_delta,
                        detail=f"Target lagging source by {src_ver - dst_ver} versions within grace window",
                    )
            if dst_ver > src_ver:
                if time_delta is not None and time_delta <= self.grace_window_seconds:
                    return DualWriteClassificationResult(
                        status=DualWriteDriftType.OUT_OF_ORDER_WRITE,
                        key=key,
                        source_version=src_ver,
                        target_version=dst_ver,
                        source_timestamp=src_ts,
                        target_timestamp=dst_ts,
                        time_delta_seconds=time_delta,
                        detail=f"Target received newer version {dst_ver} before source {src_ver} within grace window",
                    )

        if is_recent and time_delta is not None and time_delta <= self.grace_window_seconds:
            return DualWriteClassificationResult(
                status=DualWriteDriftType.OUT_OF_ORDER_WRITE,
                key=key,
                source_timestamp=src_ts,
                target_timestamp=dst_ts,
                time_delta_seconds=time_delta,
                detail=f"Discrepancy within acceptable dual-write lag window ({time_delta:.2f}s <= {self.grace_window_seconds}s)",
            )

        return DualWriteClassificationResult(
            status=DualWriteDriftType.GENUINE_DRIFT,
            key=key,
            source_timestamp=src_ts,
            target_timestamp=dst_ts,
            time_delta_seconds=time_delta,
            source_version=src_ver,
            target_version=dst_ver,
            detail="Payload or version divergence exceeds dual-write grace window; genuine application data drift",
        )

    def record_probe(
        self,
        key: Any,
        source_row: Optional[Dict[str, Any]],
        target_row: Optional[Dict[str, Any]],
        probe_time: float,
    ) -> None:
        if key not in self.probe_history:
            self.probe_history[key] = []
        self.probe_history[key].append({
            "time": probe_time,
            "source": source_row,
            "target": target_row,
        })

    def evaluate_multi_probe_convergence(self, key: Any) -> DualWriteDriftType:
        probes = self.probe_history.get(key, [])
        if not probes:
            return DualWriteDriftType.IN_SYNC

        first = probes[0]
        latest = probes[-1]

        if latest["source"] == latest["target"]:
            return DualWriteDriftType.OUT_OF_ORDER_WRITE

        duration = latest["time"] - first["time"]
        if duration > self.grace_window_seconds:
            return DualWriteDriftType.GENUINE_DRIFT

        return DualWriteDriftType.IN_FLIGHT_LAG
=======


class DualWriteDriftType(str, enum.Enum):
    IN_SYNC = "IN_SYNC"
    OUT_OF_ORDER_WRITE = "OUT_OF_ORDER_WRITE"
    IN_FLIGHT_LAG = "IN_FLIGHT_LAG"
    GENUINE_DRIFT = "GENUINE_DRIFT"


@dataclass
class DualWriteClassificationResult:
    status: DualWriteDriftType
    key: Any
    detail: str
    source_timestamp: Optional[float] = None
    target_timestamp: Optional[float] = None
    time_delta_seconds: Optional[float] = None
    source_version: Optional[int] = None
    target_version: Optional[int] = None


class DualWritesDriftDetector:
    """Dual writes drift detector (Item 65, F1.dual)."""

    def __init__(self, grace_window_seconds: float = 5.0):
        self.grace_window_seconds = grace_window_seconds
        self.probe_history: Dict[Any, List[Dict[str, Any]]] = {}

    @staticmethod
    def _extract_time(row: Optional[Dict[str, Any]], col: Optional[str]) -> Optional[float]:
        if not row or not col or col not in row:
            return None
        val = row[col]
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            try:
                import datetime
                dt = datetime.datetime.fromisoformat(val.replace("Z", "+00:00"))
                return dt.timestamp()
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _extract_version(row: Optional[Dict[str, Any]], col: Optional[str]) -> Optional[int]:
        if not row or not col or col not in row:
            return None
        val = row[col]
        if isinstance(val, int):
            return val
        if isinstance(val, str) and val.isdigit():
            return int(val)
        return None

    def classify_row(
        self,
        key: Any,
        source_row: Optional[Dict[str, Any]],
        target_row: Optional[Dict[str, Any]],
        timestamp_col: Optional[str] = "updated_at",
        version_col: Optional[str] = "version",
        current_time: Optional[float] = None,
    ) -> DualWriteClassificationResult:
        now = current_time if current_time is not None else time.time()

        if source_row is None and target_row is None:
            return DualWriteClassificationResult(
                status=DualWriteDriftType.IN_SYNC,
                key=key,
                detail="Both source and target rows are absent",
            )

        src_ts = self._extract_time(source_row, timestamp_col)
        dst_ts = self._extract_time(target_row, timestamp_col)
        src_ver = self._extract_version(source_row, version_col)
        dst_ver = self._extract_version(target_row, version_col)

        if source_row is not None and target_row is None:
            if src_ts is not None and (now - src_ts) <= self.grace_window_seconds:
                return DualWriteClassificationResult(
                    status=DualWriteDriftType.IN_FLIGHT_LAG,
                    key=key,
                    source_timestamp=src_ts,
                    detail=f"Row committed on source {now - src_ts:.2f}s ago; pending replication to target",
                )
            return DualWriteClassificationResult(
                status=DualWriteDriftType.GENUINE_DRIFT,
                key=key,
                source_timestamp=src_ts,
                detail="Row present on source but missing on target beyond dual-write grace window",
            )

        if source_row is None and target_row is not None:
            if dst_ts is not None and (now - dst_ts) <= self.grace_window_seconds:
                return DualWriteClassificationResult(
                    status=DualWriteDriftType.IN_FLIGHT_LAG,
                    key=key,
                    target_timestamp=dst_ts,
                    detail=f"Row committed on target {now - dst_ts:.2f}s ago; pending arrival on source",
                )
            return DualWriteClassificationResult(
                status=DualWriteDriftType.GENUINE_DRIFT,
                key=key,
                target_timestamp=dst_ts,
                detail="Row present on target only beyond dual-write grace window (orphaned write)",
            )

        assert source_row is not None and target_row is not None
        if source_row == target_row:
            return DualWriteClassificationResult(
                status=DualWriteDriftType.IN_SYNC,
                key=key,
                source_timestamp=src_ts,
                target_timestamp=dst_ts,
                source_version=src_ver,
                target_version=dst_ver,
                detail="Source and target rows are identical",
            )

        time_delta = abs(src_ts - dst_ts) if (src_ts is not None and dst_ts is not None) else None
        latest_ts = max(src_ts, dst_ts) if (src_ts is not None and dst_ts is not None) else (src_ts or dst_ts)
        is_recent = (now - latest_ts) <= self.grace_window_seconds if latest_ts is not None else False

        if is_recent and src_ver is not None and dst_ver is not None:
            if src_ver == dst_ver and time_delta is not None and time_delta <= self.grace_window_seconds:
                return DualWriteClassificationResult(
                    status=DualWriteDriftType.OUT_OF_ORDER_WRITE,
                    key=key,
                    source_version=src_ver,
                    target_version=dst_ver,
                    source_timestamp=src_ts,
                    target_timestamp=dst_ts,
                    time_delta_seconds=time_delta,
                    detail=f"Same version {src_ver} with payload divergence within grace window ({time_delta:.2f}s)",
                )
            if src_ver > dst_ver:
                if time_delta is not None and time_delta <= self.grace_window_seconds:
                    return DualWriteClassificationResult(
                        status=DualWriteDriftType.IN_FLIGHT_LAG,
                        key=key,
                        source_version=src_ver,
                        target_version=dst_ver,
                        source_timestamp=src_ts,
                        target_timestamp=dst_ts,
                        time_delta_seconds=time_delta,
                        detail=f"Target lagging source by {src_ver - dst_ver} versions within grace window",
                    )
            if dst_ver > src_ver:
                if time_delta is not None and time_delta <= self.grace_window_seconds:
                    return DualWriteClassificationResult(
                        status=DualWriteDriftType.OUT_OF_ORDER_WRITE,
                        key=key,
                        source_version=src_ver,
                        target_version=dst_ver,
                        source_timestamp=src_ts,
                        target_timestamp=dst_ts,
                        time_delta_seconds=time_delta,
                        detail=f"Target received newer version {dst_ver} before source {src_ver} within grace window",
                    )

        if is_recent and time_delta is not None and time_delta <= self.grace_window_seconds:
            return DualWriteClassificationResult(
                status=DualWriteDriftType.OUT_OF_ORDER_WRITE,
                key=key,
                source_timestamp=src_ts,
                target_timestamp=dst_ts,
                time_delta_seconds=time_delta,
                detail=f"Discrepancy within acceptable dual-write lag window ({time_delta:.2f}s <= {self.grace_window_seconds}s)",
            )

        return DualWriteClassificationResult(
            status=DualWriteDriftType.GENUINE_DRIFT,
            key=key,
            source_timestamp=src_ts,
            target_timestamp=dst_ts,
            time_delta_seconds=time_delta,
            source_version=src_ver,
            target_version=dst_ver,
            detail="Payload or version divergence exceeds dual-write grace window; genuine application data drift",
        )

    def record_probe(
        self,
        key: Any,
        source_row: Optional[Dict[str, Any]],
        target_row: Optional[Dict[str, Any]],
        probe_time: float,
    ) -> None:
        if key not in self.probe_history:
            self.probe_history[key] = []
        self.probe_history[key].append({
            "time": probe_time,
            "source": source_row,
            "target": target_row,
        })

    def evaluate_multi_probe_convergence(self, key: Any) -> DualWriteDriftType:
        probes = self.probe_history.get(key, [])
        if not probes:
            return DualWriteDriftType.IN_SYNC

        first = probes[0]
        latest = probes[-1]

        if latest["source"] == latest["target"]:
            return DualWriteDriftType.OUT_OF_ORDER_WRITE

        duration = latest["time"] - first["time"]
        if duration > self.grace_window_seconds:
            return DualWriteDriftType.GENUINE_DRIFT

        return DualWriteDriftType.IN_FLIGHT_LAG


@dataclass(frozen=True)
class ScenarioDecision:
    case: str
    choice: decide.Choice
    details: tuple = ()

    def words(self):
        return "; ".join((f"{self.case}: {self.choice.said}",
                          self.choice.reason, *self.details))


def managed_facts(hop, eng, source_api, target_api, offers, size_bytes):
    """Read provider instances and readiness without changing either side.

    Offers are the provider's eligible operations for these instance classes
    and versions, with duration evidence and the execution owner attached.
    """
    from dataclasses import asdict
    from . import cloud_readiness, clouds
    src, dst = source_api.instance(), target_api.instance()
    a, b = asdict(src), asdict(dst)
    a.update(getattr(hop, "cloud_src", {}) or {})
    b.update(getattr(hop, "cloud_dst", {}) or {})
    return decide.Facts({
        "source": src, "target": dst, "api_reachable": True,
        "locations": (clouds.of(eng, "src", hop.source),
                      clouds.of(eng, "dst", hop.target)),
        "readiness": cloud_readiness.audit(a, b),
        "offers": tuple(offers), "size_bytes": size_bytes,
    })


def managed_case(facts, verified=None):
    """Choose a provider operation; planning never claims verification.

    A duration is a documented startup plus a size-dependent rate observed
    on a rehearsal of this path. Switchover downtime alone is not its cost.
    `verified`, when supplied after execution, is migkit's own full check.
    """
    from .costmodel import Estimate, PREDICTED
    rungs, costs = [], {}
    for offer in facts.get("offers", ()):
        name = offer["name"]
        def eligible(f, o=offer):
            src, dst = f.get("source"), f.get("target")
            return bool(src and dst and src.klass and dst.klass
                        and src.version and dst.version
                        and (src.klass, dst.klass, src.version, dst.version)
                        in o.get("eligible", ()))
        def duration(f, o=offer):
            size = f.get("size_bytes")
            return (size is not None and size >= 0
                    and bool(o.get("duration_source"))
                    and o.get("startup_seconds", -1) >= 0
                    and o.get("bytes_per_second", 0) > 0)
        rung = decide.Rung(
            name, offer["said"], frozenset({"managed path", "full verify"}),
            needs=(
                decide.Need("a reachable provider API",
                            lambda f: f.get("api_reachable") is True),
                decide.Need("eligible instance classes and engine versions", eligible),
                decide.Need("a migkit execution owner",
                            lambda f, o=offer: callable(o.get("execute"))),
                decide.Need("cloud readiness without failures",
                            lambda f: f.get("readiness") is not None and not any(
                                r.level == "fail" for r in f["readiness"])),
                decide.Need("documented duration and measured size and rate", duration)),
            prove=(lambda f: None if verified() is True else
                   "migkit's full verify did not pass") if verified else None,
            because="the provider offers this path for both instance classes"
                    " and versions; migkit must verify the result")
        rungs.append(rung)
        if duration(facts):
            seconds = offer["startup_seconds"] + facts["size_bytes"] / offer["bytes_per_second"]
            costs[name] = Estimate(seconds, seconds, truth=PREDICTED,
                                   source=offer["duration_source"])
    choice = decide.climb("the managed service path", rungs, facts,
                          need=("managed path", "full verify"), costs=costs)
    details = ["provider completion is followed by migkit's full verify"]
    if not rungs:
        details.append("no eligible provider operation was read; consider the existing logical move")
    return ScenarioDecision("managed service change", choice, tuple(details))


def architecture_facts(eng, db, platform_reader):
    """Read settings on both servers and host facts through a bounded reader.

    The host reader returns architecture, OS and byte order of that server,
    never those of the machine running migkit. Unknown facts stay unknown.
    """
    from . import upgrade
    engine = eng.CANON_ENGINE
    out = {}
    for side in ("src", "dst"):
        facts = dict(platform_reader(side))
        if engine == "mysql":
            settings = {k: v[0] for k, v in upgrade._my_vars(eng, side).items()}
            facts.update(collation=settings.get("collation_server"),
                         case_sensitivity=settings.get("lower_case_table_names"))
            rows = eng._q(side, "select cast(1.2345678901234567e0 as char), 'a' = 'A'")
            facts["case_sensitivity"] = (settings.get("lower_case_table_names"), rows[0][1])
        elif engine == "postgres":
            rows = upgrade._pg(eng, side, db,
                               "select name || chr(31) || setting from pg_settings where name in"
                               " ('lc_collate', 'lc_ctype', 'extra_float_digits')")
            settings = dict(rows)
            facts.update(collation=(settings.get("lc_collate"),
                                    upgrade.pg_collations(eng, side, db)),
                         case_sensitivity=settings.get("lc_ctype"),
                         float_setting=settings.get("extra_float_digits"))
            rows = upgrade._pg(eng, side, db,
                               "select (1.2345678901234567::float8)::text || chr(31)"
                               " || ('a' = 'A')::text")
            facts["case_sensitivity"] = (settings.get("lc_ctype"), rows[0][1])
        elif engine == "mssql":
            rows = eng._cmd(side, db,
                            "select cast(databasepropertyex(db_name(), 'Collation')"
                            " as nvarchar(128)), case when N'a' = N'A' then 0 else 1 end")
            facts.update(collation=rows[0][0], case_sensitivity=rows[0][1])
            rows = eng._cmd(side, db,
                            "select convert(varchar(64), cast(1.2345678901234567 as float), 3)")
        else:
            raise ValueError(f"architecture settings are not read for {engine}")
        facts["float_rendering"] = rows[0][0] if rows else None
        out[side] = facts
    return decide.Facts(out)


def architecture_case(facts, schema_carries=()):
    """Refuse unhandled semantic differences, route byte changes logically."""
    a, b = facts.get("src", {}), facts.get("dst", {})
    fields = ("architecture", "os", "byte_order", "collation",
              "case_sensitivity", "float_rendering")
    def unread(value):
        return value is None or value == "" or (
            isinstance(value, tuple) and (not value or unread(value[0])))
    missing = [k for k in fields if unread(a.get(k)) or unread(b.get(k))]
    differences = [k for k in fields if a.get(k) != b.get(k)]
    unhandled = [k for k in differences if k in (
        "collation", "case_sensitivity", "float_rendering", "float_setting")
        and k not in schema_carries]
    if a.get("float_setting") != b.get("float_setting") and "float_setting" not in schema_carries:
        unhandled.append("float_setting")
    needs = (
        decide.Need("facts read from both servers", lambda f: not missing,
                    "unread server facts: " + ", ".join(missing)),
        decide.Need("semantic differences carried by the schema step",
                    lambda f: not unhandled,
                    "the schema step does not carry: " + ", ".join(unhandled)))
    logical = decide.Rung("architecture-logical", "a logical move across hardware or OS",
                          frozenset({"architecture change"}), needs=needs,
                          because="values are decoded and encoded for the target")
    physical = decide.Rung(
        "architecture-pages", "a copy of the server's pages",
        frozenset({"architecture change"}),
        needs=needs + (decide.Need("the same architecture, OS and byte order",
                                 lambda f: not any(k in differences for k in
                                                   ("architecture", "os", "byte_order"))),))
    choice = decide.climb("hardware / OS / architecture", (physical, logical), facts,
                          need=("architecture change",))
    details = tuple(f"{k}: {a.get(k)!r} to {b.get(k)!r}" for k in fields)
    return ScenarioDecision("hardware / OS / architecture change", choice, details)


def storage_facts(eng, db):
    """Every scoped table's storage engine or access method, on both sides."""
    from . import tableengine, upgrade
    out = {}
    for side in ("src", "dst"):
        if eng.CANON_ENGINE == "mysql":
            tables = tableengine._engines(eng, side, db)
        elif eng.CANON_ENGINE == "postgres":
            rows = upgrade._pg(eng, side, db,
                               "select n.nspname || '.' || c.relname || chr(31) || a.amname"
                               " from pg_class c join pg_namespace n on n.oid=c.relnamespace"
                               " join pg_am a on a.oid=c.relam where c.relkind='r'"
                               " and n.nspname not in ('pg_catalog','information_schema')")
            tables = dict(rows)
        else:
            raise ValueError(f"table storage is not read for {eng.CANON_ENGINE}")
        out[side] = {t: n.lower() for t, n in tables.items() if eng.in_scope(db, t)}
    return out


def storage_case(table, source, target, *, family="mysql", exists=False,
                 supported=(), accepted=False, orphan_rows=None,
                 postgres_traits=None, used_features=(), alter_access_method=False):
    """Decide with the schema owner's rungs and say every consequence.

    PostgreSQL extension access methods require observed capabilities;
    a name such as columnar alone never promises heap semantics.
    """
    from . import tableengine
    source, target = source.lower(), target.lower()
    if family == "mysql":
        a, b = tableengine.TRAITS.get(source), tableengine.TRAITS.get(target)
    elif family == "postgres":
        traits = postgres_traits or {}
        a, b = traits.get(source), traits.get(target)
    else:
        raise ValueError(f"storage changes are not read for {family}")
    detail = [f"{table}: {source} to {target}"]
    if a is not None and b is not None:
        detail.extend(f"{k}: {getattr(a, k)} to {getattr(b, k)}"
                      for k in ("transactions", "foreign_keys", "fulltext", "locks", "holds"))
    loss = a is None or b is None or any(
        getattr(a, k) and not getattr(b, k) for k in ("transactions", "foreign_keys"))
    if a is not None and b is not None:
        loss = loss or any(not getattr(b, k) for k in used_features)
    if b is not None and b.holds != "rows":
        loss = True
    fk_safe = b is not None and (not b.foreign_keys or
                                                (orphan_rows is not None and orphan_rows == 0))
    facts = decide.Facts({"there": exists})
    needs = (
        decide.Need("known storage semantics on both servers", lambda f: a is not None and b is not None),
        decide.Need("the target's supported storage method", lambda f: target in supported),
        decide.Need("accepted transaction, foreign-key, full-text and locking consequences",
                    lambda f: accepted and not loss,
                    "storage consequences need acceptance; lost transactions, foreign keys or durability are refused"),
        decide.Need("zero rows refused by target foreign keys", lambda f: fk_safe))
    from dataclasses import replace
    rungs = tuple(replace(r, needs=needs + r.needs) for r in tableengine.ways())
    if family == "postgres":
        needs += (decide.Need("the server's ALTER TABLE SET ACCESS METHOD support",
                              lambda f: alter_access_method,
                              "PostgreSQL access-method changes need ALTER TABLE SET ACCESS METHOD support"),)
        rungs = (decide.Rung(
            "in place", "the table's access method altered after the copy",
            frozenset({"engine changed"}),
            needs=needs + (decide.Need("the copied table on the target",
                                      lambda f: f["there"]),),
            because="the target rebuilds the copied table using its own access method"),)
    choice = decide.climb(f"{table}: storage engine change", rungs, facts,
                          need=("engine changed",))
    return ScenarioDecision("storage engine change", choice, tuple(detail))


def storage_definition(ddl, table, source, target, *, hop, eng, db, **facts):
    """Use the existing schema rewrite only after the storage decision holds."""
    from . import tableengine
    decision = storage_case(table, source, target, **facts)
    if decision.choice.path != "definition":
        raise ValueError(decision.words())
    change = tableengine.plan(hop, eng, db).of(table)
    if change is None or (change.src, change.dst) != (source.lower(), target.lower()):
        raise ValueError("the storage decision differs from the schema step's plan")
    return tableengine.schema(hop, eng, db, table, ddl)


def run_managed_case(facts, verify):
    """Execute the eligible operation and require migkit's own verification."""
    decision = managed_case(facts)
    if decision.choice.path is None:
        raise ValueError(decision.words())
    offer = next(o for o in facts["offers"] if o["name"] == decision.choice.path)
    result = offer["execute"]()
    checked = verify()
    selected = dict(facts, offers=(offer,))
    proved = managed_case(selected, verified=lambda: checked)
    if proved.choice.path != decision.choice.path:
        raise ValueError("managed service change: migkit's full verify did not pass")
    return result, proved


def storage_alter(table, source, target, **facts):
    """The target-only statement, for the schema step after a copy."""
    decision = storage_case(table, source, target, **facts)
    if decision.choice.path != "in place":
        raise ValueError(decision.words())
    if facts.get("family", "mysql") == "postgres":
        quote = lambda name: '"' + name.replace('"', '""') + '"'
        name = ".".join(quote(part) for part in table.split("."))
        return f"ALTER TABLE {name} SET ACCESS METHOD {quote(target)}"
    raise ValueError("MySQL storage changes use the schema step's in-place handler")
>>>>>>> ext-scen
