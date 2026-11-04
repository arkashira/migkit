import hashlib
import io
import json
import os
import tarfile
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

from . import checkpoint as cp_mod
from . import decide
from . import durable
from . import envkeys
from .resume_scope import ResumeScope, ScopedCheckpoint

DEFAULT_FORMAT_VERSION = 1
FORMAT_VERSION = DEFAULT_FORMAT_VERSION
SCHEMA_NEUTRAL_VERSION = 1


def get_format_version(override: Optional[int] = None) -> int:
    """Return dynamic format version without fixed hardcoded literal."""
    if override is not None:
        return max(1, int(override))
    return int(envkeys.number("MIGKIT_PORTABLE_FORMAT_VERSION", default=FORMAT_VERSION))


def _prove_standard_neutral_json_manifest(facts: decide.Facts) -> bool:
    return bool(facts.get("has_schema", True) and facts.get("format", "json") in (None, "json", "package") and not facts.get("require_archive") and not facts.get("require_ddl") and not facts.get("require_checkpoint") and not facts.get("refused"))


def _prove_dialect_agnostic_ddl_bundle(facts: decide.Facts) -> bool:
    return bool(facts.get("has_schema", True) and (facts.get("format") == "ddl" or facts.get("require_ddl")) and not facts.get("require_archive") and not facts.get("require_checkpoint") and not facts.get("refused"))


def _prove_scoped_checkpoint_state_export(facts: decide.Facts) -> bool:
    return bool(facts.get("has_schema", True) and (facts.get("format") == "checkpoint" or facts.get("require_checkpoint")) and not facts.get("require_archive") and not facts.get("refused"))


def _prove_portable_archive_bundle(facts: decide.Facts) -> bool:
    return bool(facts.get("has_schema", True) and (facts.get("format") == "archive" or facts.get("require_archive")) and not facts.get("refused"))


def _prove_refuse_proprietary_locked_format(facts: decide.Facts) -> bool:
    return not bool(facts.get("allow_locked_format"))



def exit_portability_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "standard_neutral_json_manifest",
            "standard portable manifest JSON package with neutral schemas and typed checkpoints",
            needs=(
                decide.Need(
                    "standard json manifest applicable",
                    _prove_standard_neutral_json_manifest,
                    "export format is not standard json manifest",
                ),
            ),
        ),
        decide.Rung(
            "dialect_agnostic_ddl_bundle",
            "portable DDL export across SQL dialects without vendor-specific locks",
            needs=(
                decide.Need(
                    "dialect agnostic ddl applicable",
                    _prove_dialect_agnostic_ddl_bundle,
                    "export format is not dialect-agnostic ddl",
                ),
            ),
        ),
        decide.Rung(
            "scoped_checkpoint_state_export",
            "export table-level cursor and range checkpoints into neutral resume format",
            needs=(
                decide.Need(
                    "checkpoint state export applicable",
                    _prove_scoped_checkpoint_state_export,
                    "export format is not scoped checkpoint state",
                ),
            ),
        ),
        decide.Rung(
            "portable_archive_bundle",
            "compressed multi-table archive bundle with metadata and sha256 checksums",
            needs=(
                decide.Need(
                    "portable archive bundle applicable",
                    _prove_portable_archive_bundle,
                    "export format is not portable archive bundle",
                ),
            ),
        ),
        decide.Rung(
            "refuse_proprietary_locked_format",
            "refuse export formats that omit neutral schema or state proofs",
            needs=(
                decide.Need(
                    "proprietary locked format refused",
                    _prove_refuse_proprietary_locked_format,
                    "proprietary locked formats are refused",
                ),
            ),
        ),
    )


def decide_exit_portability(facts: Dict[str, Any]) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("exit portability export package", exit_portability_rungs(), fact_obj)


def _prove_normalize_integral_type(facts: decide.Facts) -> bool:
    t = str(facts.get("raw_type") or "").strip().upper()
    return t.startswith("INT") or t in ("SERIAL", "INTEGER", "BIGINT", "BIGSERIAL", "INT8", "SMALLINT", "SMALLSERIAL", "INT2", "TINYINT")


def _prove_normalize_numeric_type(facts: decide.Facts) -> bool:
    t = str(facts.get("raw_type") or "").strip().upper()
    return "DECIMAL" in t or "NUMERIC" in t or "FLOAT" in t or "REAL" in t or "DOUBLE" in t


def _prove_normalize_temporal_type(facts: decide.Facts) -> bool:
    t = str(facts.get("raw_type") or "").strip().upper()
    return "TIMESTAMP" in t or "DATETIME" in t or t == "DATE" or "TIME" in t


def _prove_normalize_structured_type(facts: decide.Facts) -> bool:
    t = str(facts.get("raw_type") or "").strip().upper()
    return "JSON" in t or "ARRAY" in t or "UUID" in t or "BOOL" in t


def _prove_normalize_text_binary_fallback(facts: decide.Facts) -> bool:
    return not bool(facts.get("refuse_fallback"))



def neutral_type_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "normalize_integral_type",
            "canonical integral neutral type normalization",
            needs=(
                decide.Need(
                    "integral type",
                    _prove_normalize_integral_type,
                    "raw type is not integral",
                ),
            ),
        ),
        decide.Rung(
            "normalize_numeric_type",
            "canonical exact decimal and float numeric type normalization",
            needs=(
                decide.Need(
                    "numeric type",
                    _prove_normalize_numeric_type,
                    "raw type is not numeric",
                ),
            ),
        ),
        decide.Rung(
            "normalize_temporal_type",
            "canonical timestamp, date and time temporal type normalization",
            needs=(
                decide.Need(
                    "temporal type",
                    _prove_normalize_temporal_type,
                    "raw type is not temporal",
                ),
            ),
        ),
        decide.Rung(
            "normalize_structured_type",
            "canonical JSON, array, UUID and boolean structured type normalization",
            needs=(
                decide.Need(
                    "structured type",
                    _prove_normalize_structured_type,
                    "raw type is not structured",
                ),
            ),
        ),
        decide.Rung(
            "normalize_text_binary_fallback",
            "canonical binary byte array and variable string fallback normalization",
            needs=(
                decide.Need(
                    "text or binary fallback",
                    _prove_normalize_text_binary_fallback,
                    "cannot normalize type",
                ),
            ),
        ),
    )



def decide_neutral_type(facts: Dict[str, Any]) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("neutral schema type normalization", neutral_type_rungs(), fact_obj)


class PortabilityValidationError(ValueError):
    pass


class NeutralType:
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    SMALLINT = "SMALLINT"
    DECIMAL = "DECIMAL"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    TIME = "TIME"
    UUID = "UUID"
    BYTES = "BYTES"
    JSON = "JSON"
    ARRAY = "ARRAY"


def normalize_type_name(raw_type: str) -> str:
    choice = decide_neutral_type({"raw_type": raw_type})
    t = str(raw_type).strip().upper()
    rung_name = choice.rung.name

    if rung_name == "normalize_integral_type":
        if t.startswith("BIGINT") or t in ("BIGSERIAL", "INT8"):
            return NeutralType.BIGINT
        if t.startswith("SMALLINT") or t in ("SMALLSERIAL", "INT2"):
            return NeutralType.SMALLINT
        return NeutralType.INTEGER
    elif rung_name == "normalize_numeric_type":
        if "DOUBLE" in t:
            return NeutralType.DOUBLE
        if "FLOAT" in t or "REAL" in t:
            return NeutralType.FLOAT
        return NeutralType.DECIMAL
    elif rung_name == "normalize_temporal_type":
        if t == "DATE":
            return NeutralType.DATE
        if t == "TIME":
            return NeutralType.TIME
        return NeutralType.TIMESTAMP
    elif rung_name == "normalize_structured_type":
        if "BOOL" in t:
            return NeutralType.BOOLEAN
        if "UUID" in t:
            return NeutralType.UUID
        if "ARRAY" in t:
            return NeutralType.ARRAY
        return NeutralType.JSON
    else:
        if "BYTE" in t or "BLOB" in t or "BINARY" in t:
            return NeutralType.BYTES
        if "TEXT" in t or "CLOB" in t:
            return NeutralType.TEXT
        if "VARCHAR" in t or "CHAR" in t or "NVARCHAR" in t:
            return NeutralType.VARCHAR
        return t



class NeutralSchema:
    def __init__(self, table: str, target_name: Optional[str] = None):
        self.table = str(table)
        self.target_name = str(target_name or table)
        self.columns: List[Dict[str, Any]] = []
        self.primary_key: List[str] = []
        self.sequence_offset: Optional[Dict[str, Any]] = None
        self.foreign_keys: List[Dict[str, Any]] = []
        self.indexes: List[Dict[str, Any]] = []

    def add_column(
        self,
        name: str,
        type_name: str,
        target_name: Optional[str] = None,
        is_primary_key: bool = False,
        nullable: bool = True,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
        length: Optional[int] = None,
        default_value: Optional[Any] = None,
        ordinal_position: Optional[int] = None,
    ) -> "NeutralSchema":
        col_def = {
            "name": str(name),
            "target_name": str(target_name or name),
            "neutral_type": normalize_type_name(type_name),
            "raw_type": str(type_name),
            "is_primary_key": bool(is_primary_key),
            "nullable": bool(nullable),
            "precision": precision,
            "scale": scale,
            "length": length,
            "default_value": default_value,
            "ordinal_position": ordinal_position if ordinal_position is not None else len(self.columns) + 1,
        }
        self.columns.append(col_def)
        if is_primary_key and name not in self.primary_key:
            self.primary_key.append(name)
        return self

    def set_sequence_offset(
        self,
        sequence_name: str,
        last_value: int = 0,
        start_value: int = 1,
        increment_by: int = 1,
        min_value: int = 1,
        max_value: Optional[int] = None,
        is_cycled: bool = False,
    ) -> "NeutralSchema":
        self.sequence_offset = {
            "sequence_name": str(sequence_name),
            "last_value": int(last_value),
            "start_value": int(start_value),
            "increment_by": int(increment_by),
            "min_value": int(min_value),
            "max_value": int(max_value) if max_value is not None else None,
            "is_cycled": bool(is_cycled),
        }
        return self

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "target_table": self.target_name,
            "columns": list(self.columns),
            "primary_key": list(self.primary_key),
            "sequence_offset": dict(self.sequence_offset) if self.sequence_offset else None,
            "foreign_keys": list(self.foreign_keys),
            "indexes": list(self.indexes),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NeutralSchema":
        s = cls(data.get("table", ""), target_name=data.get("target_table"))
        s.columns = list(data.get("columns", []))
        s.primary_key = list(data.get("primary_key", []))
        s.sequence_offset = data.get("sequence_offset")
        s.foreign_keys = list(data.get("foreign_keys", []))
        s.indexes = list(data.get("indexes", []))
        return s


class CollisionStrategy:
    RENAME_NUMBERED = "rename_numbered"
    PRESERVE_EXACT = "preserve_exact"
    FAIL_ON_COLLISION = "fail_on_collision"


class PortableManifestExporter:
    def __init__(
        self,
        source_engine: str = "postgres",
        target_engine: str = "postgres",
        hop_name: str = "default",
        version: Optional[str] = None,
        format_version: Optional[int] = None,
    ):
        self.source_engine = str(source_engine)
        self.target_engine = str(target_engine)
        self.hop_name = str(hop_name)
        self.version = str(version or "1.0.0")
        self.format_version = get_format_version(format_version)


        self._schemas: Dict[str, NeutralSchema] = {}
        self._mappings: Dict[str, Dict[str, str]] = {}
        self._manifests: Dict[str, Dict[str, Any]] = {}
        self._checkpoints: Dict[str, Dict[str, Any]] = {}
        self._tool_config: Dict[str, Any] = {
            "source_engine": self.source_engine,
            "target_engine": self.target_engine,
            "batch_size": 1000,
            "chunk_size": 10000,
            "concurrency": 2,
            "verify_method": "aimd_merkle",
        }
        self._watermarks: Dict[str, Any] = {}
        self._scoped_headers: Dict[str, Dict[str, Any]] = {}

    def set_tool_config(self, **kwargs) -> "PortableManifestExporter":
        for k, v in kwargs.items():
            if v is not None:
                self._tool_config[k] = v
        return self

    def add_table(
        self,
        table_name: str,
        target_table: Optional[str] = None,
        columns: Optional[List[Dict[str, Any]]] = None,
        primary_key: Optional[List[str]] = None,
        sequence_offsets: Optional[Dict[str, Any]] = None,
        foreign_keys: Optional[List[Dict[str, Any]]] = None,
        indexes: Optional[List[Dict[str, Any]]] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        collision_strategy: str = CollisionStrategy.RENAME_NUMBERED,
    ) -> "PortableManifestExporter":
        target = target_table or table_name
        schema = NeutralSchema(table_name, target_name=target)
        col_list = columns or []
        mapping: Dict[str, str] = {}
        target_used: Set[str] = set()

        for col in col_list:
            cname = col["name"]
            raw_tname = col.get("target_name") or (column_mapping.get(cname) if column_mapping else cname)
            is_pk = bool(col.get("primary_key") or (primary_key and cname in primary_key))

            tname = raw_tname
            norm_name = raw_tname.lower()
            if collision_strategy == CollisionStrategy.RENAME_NUMBERED:
                if norm_name in target_used:
                    suffix = 2
                    candidate = f"{raw_tname.lower()}_{suffix}"
                    while candidate in target_used:
                        suffix += 1
                        candidate = f"{raw_tname.lower()}_{suffix}"
                    tname = candidate
                target_used.add(tname.lower())
            elif collision_strategy == CollisionStrategy.FAIL_ON_COLLISION:
                if norm_name in target_used:
                    raise PortabilityValidationError(f"column name collision detected for {raw_tname}")
                target_used.add(norm_name)
            else:
                target_used.add(raw_tname)

            mapping[cname] = tname
            schema.add_column(
                name=cname,
                type_name=col.get("type", "TEXT"),
                target_name=tname,
                is_primary_key=is_pk,
                nullable=col.get("nullable", True),
                precision=col.get("precision"),
                scale=col.get("scale"),
                length=col.get("length"),
                default_value=col.get("default"),
                ordinal_position=col.get("ordinal_position"),
            )

        if primary_key:
            schema.primary_key = list(primary_key)

        if sequence_offsets:
            schema.set_sequence_offset(
                sequence_name=sequence_offsets.get("sequence_name", f"seq_{table_name}"),
                last_value=sequence_offsets.get("last_value", 0),
                start_value=sequence_offsets.get("start_value", 1),
                increment_by=sequence_offsets.get("increment_by", 1),
                min_value=sequence_offsets.get("min_value", 1),
                max_value=sequence_offsets.get("max_value"),
                is_cycled=sequence_offsets.get("is_cycled", False),
            )

        if foreign_keys:
            schema.foreign_keys = list(foreign_keys)
        if indexes:
            schema.indexes = list(indexes)

        self._schemas[table_name] = schema
        self._mappings[table_name] = mapping
        return self

    def add_manifest(
        self,
        table_name: str,
        total_rows: int = 0,
        total_bytes: int = 0,
        total_checksum: str = "0",
        parts: Optional[List[Dict[str, Any]]] = None,
    ) -> "PortableManifestExporter":
        manifest_parts = []
        for p in parts or []:
            rng = p.get("range") or {}
            manifest_parts.append({
                "name": p.get("name", ""),
                "format": p.get("format", "csv"),
                "rows": int(p.get("rows", 0)),
                "bytes": int(p.get("bytes", 0)),
                "sha256": str(p.get("sha256", "")),
                "checksum": str(p.get("checksum", "0")),
                "range": {
                    "lo": cp_mod.serialize_cursor(rng.get("lo")),
                    "hi": cp_mod.serialize_cursor(rng.get("hi")),
                },
            })

        self._manifests[table_name] = {
            "table": table_name,
            "total_rows": int(total_rows),
            "total_bytes": int(total_bytes),
            "total_checksum": str(total_checksum),
            "parts": manifest_parts,
        }
        return self

    def add_checkpoint(
        self,
        table_name: str,
        done_ranges: Optional[Dict[str, List[Any]]] = None,
        cursors: Optional[Dict[str, Dict[str, Any]]] = None,
        watermark: Optional[Any] = None,
        fingerprint: str = "",
        sequence_offsets: Optional[Dict[str, Any]] = None,
        salt: Optional[str] = None,
        units: Optional[Dict[str, Any]] = None,
    ) -> "PortableManifestExporter":
        clean_done: Dict[str, List[Any]] = {}
        for k, v in (done_ranges or {}).items():
            clean_done[str(k)] = [int(v[0]), str(v[1])]

        clean_cursors: Dict[str, Dict[str, Any]] = {}
        for k, cur in (cursors or {}).items():
            clean_cursors[str(k)] = {
                "lo": cp_mod.serialize_cursor(cur.get("lo")),
                "hi": cp_mod.serialize_cursor(cur.get("hi")),
                "rows": int(cur.get("rows", 0)),
                "checksum": str(cur.get("checksum", "0")),
            }

        self._checkpoints[table_name] = {
            "table": table_name,
            "fingerprint": str(fingerprint or ""),
            "salt": str(salt or ""),
            "done": clean_done,
            "cursors": clean_cursors,
            "watermark": watermark,
            "sequence_offsets": sequence_offsets,
            "units": units or {},
        }
        if watermark is not None:
            self._watermarks[table_name] = watermark
        return self

    def add_checkpoint_object(
        self,
        cp: cp_mod.Checkpoint,
        table_name: str,
        watermark: Optional[Any] = None,
    ) -> "PortableManifestExporter":
        data = getattr(cp, "_data", {}).get("tables", {}).get(table_name, {})
        done = data.get("done", {})
        cursors = data.get("cursors", {})
        fp = data.get("fingerprint", "")
        salt = data.get("salt", "")
        return self.add_checkpoint(
            table_name=table_name,
            done_ranges=done,
            cursors=cursors,
            watermark=watermark,
            fingerprint=fp,
            salt=salt,
        )

    def add_scoped_checkpoint(
        self,
        scoped_cp: ScopedCheckpoint,
        table_name: str,
        watermark: Optional[Any] = None,
    ) -> "PortableManifestExporter":
        raw = scoped_cp.raw_data()
        tbl_data = raw.get("tables", {}).get(table_name, {})
        hdr = raw.get("scope_header", {})
        self._scoped_headers[table_name] = hdr

        if table_name not in self._schemas:
            cols = [{"name": c, "type": "TEXT"} for c in (hdr.get("column_mapping") or {}).keys()]
            if not cols:
                cols = [{"name": "id", "type": "INTEGER", "primary_key": True}]
            self.add_table(
                table_name=table_name,
                columns=cols,
                column_mapping=hdr.get("column_mapping"),
            )

        return self.add_checkpoint(
            table_name=table_name,
            done_ranges=tbl_data.get("done", {}),
            cursors=tbl_data.get("cursors", {}),
            watermark=watermark,
            fingerprint=tbl_data.get("fingerprint", ""),
            salt=tbl_data.get("salt", ""),
            units=tbl_data.get("units", {}),
        )

    def to_package_dict(self) -> Dict[str, Any]:
        schemas_doc = {t: s.to_dict() for t, s in self._schemas.items()}
        return {
            "format_version": getattr(self, "format_version", FORMAT_VERSION),

            "schema_neutral_version": SCHEMA_NEUTRAL_VERSION,
            "hop": self.hop_name,
            "source_engine": self.source_engine,
            "target_engine": self.target_engine,
            "migkit_version": self.version,
            "tool_config": dict(self._tool_config),
            "schemas": schemas_doc,
            "mappings": dict(self._mappings),
            "manifests": dict(self._manifests),
            "checkpoints": dict(self._checkpoints),
            "watermarks": dict(self._watermarks),
            "scoped_headers": dict(self._scoped_headers),
        }

    def export_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_package_dict(), indent=indent, sort_keys=True)

    def export_yaml(self) -> str:
        if not HAS_YAML:
            raise RuntimeError("yaml module is not available")
        return yaml.safe_dump(self.to_package_dict(), sort_keys=False)

    def export_to_directory(self, target_dir: Union[str, Path], fmt: str = "json") -> Path:
        out = Path(target_dir)
        out.mkdir(parents=True, exist_ok=True)
        pkg = self.to_package_dict()

        ext = ".yaml" if fmt == "yaml" else ".json"
        dump = (lambda d: yaml.safe_dump(d, sort_keys=False)) if fmt == "yaml" else (lambda d: json.dumps(d, indent=2))

        (out / f"package{ext}").write_text(dump(pkg))
        (out / f"schemas{ext}").write_text(dump(pkg["schemas"]))
        (out / f"mappings{ext}").write_text(dump(pkg["mappings"]))
        (out / f"manifests{ext}").write_text(dump(pkg["manifests"]))
        (out / f"checkpoints{ext}").write_text(dump(pkg["checkpoints"]))
        (out / f"tool_config{ext}").write_text(dump(pkg["tool_config"]))
        return out

    def export_bundle_archive(self, archive_path: Union[str, Path], fmt: str = "json") -> Path:
        target = Path(archive_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        pkg = self.to_package_dict()

        ext = ".yaml" if fmt == "yaml" else ".json"
        dump = (lambda d: yaml.safe_dump(d, sort_keys=False).encode("utf-8")) if fmt == "yaml" else (
            lambda d: json.dumps(d, indent=2).encode("utf-8")
        )

        files = {
            f"package{ext}": dump(pkg),
            f"schemas{ext}": dump(pkg["schemas"]),
            f"mappings{ext}": dump(pkg["mappings"]),
            f"manifests{ext}": dump(pkg["manifests"]),
            f"checkpoints{ext}": dump(pkg["checkpoints"]),
            f"tool_config{ext}": dump(pkg["tool_config"]),
        }

        if str(target).endswith(".tar.gz") or str(target).endswith(".tgz"):
            with tarfile.open(target, "w:gz") as tar:
                for fname, data in files.items():
                    ti = tarfile.TarInfo(name=fname)
                    ti.size = len(data)
                    tar.addfile(ti, io.BytesIO(data))
        elif str(target).endswith(".zip"):
            with zipfile.ZipFile(target, "w") as zf:
                for fname, data in files.items():
                    zf.writestr(fname, data)
        else:
            with tarfile.open(target, "w:gz") as tar:
                for fname, data in files.items():
                    ti = tarfile.TarInfo(name=fname)
                    ti.size = len(data)
                    tar.addfile(ti, io.BytesIO(data))

        return target


class PortableStateImporter:
    def __init__(self, source: Union[str, Path, Dict[str, Any]]):
        self._pkg: Dict[str, Any] = self._load(source)

    def _load(self, source: Union[str, Path, Dict[str, Any]]) -> Dict[str, Any]:
        if isinstance(source, dict):
            return dict(source)

        p = Path(source)
        if not p.exists():
            raise PortabilityValidationError(f"source path does not exist: {source}")

        if p.is_dir():
            for fname in ("package.json", "package.yaml"):
                cand = p / fname
                if cand.exists():
                    if cand.suffix in (".yaml", ".yml"):
                        return yaml.safe_load(cand.read_text())
                    return json.loads(cand.read_text())
            raise PortabilityValidationError(f"directory contains no package.json or package.yaml: {source}")

        if str(p).endswith(".tar.gz") or str(p).endswith(".tgz"):
            with tarfile.open(p, "r:gz") as tar:
                for member in tar.getmembers():
                    if member.name in ("package.json", "package.yaml"):
                        f = tar.extractfile(member)
                        if f:
                            raw = f.read().decode("utf-8")
                            if member.name.endswith(".yaml"):
                                return yaml.safe_load(raw)
                            return json.loads(raw)
            raise PortabilityValidationError(f"archive contains no package.json/yaml: {source}")

        if str(p).endswith(".zip"):
            with zipfile.ZipFile(p, "r") as zf:
                for name in ("package.json", "package.yaml"):
                    if name in zf.namelist():
                        raw = zf.read(name).decode("utf-8")
                        if name.endswith(".yaml"):
                            return yaml.safe_load(raw)
                        return json.loads(raw)
            raise PortabilityValidationError(f"zip archive contains no package.json/yaml: {source}")

        raw = p.read_text()
        if p.suffix in (".yaml", ".yml"):
            return yaml.safe_load(raw)
        return json.loads(raw)

    def validate(self) -> bool:
        if not isinstance(self._pkg, dict):
            raise PortabilityValidationError("package payload is not a dictionary")

        if "format_version" not in self._pkg:
            raise PortabilityValidationError("missing format_version in portable package")

        schemas = self._pkg.get("schemas")
        if not isinstance(schemas, dict):
            raise PortabilityValidationError("missing or invalid schemas section")

        for tbl, s in schemas.items():
            if not isinstance(s, dict):
                raise PortabilityValidationError(f"invalid schema for table {tbl}")
            cols = s.get("columns")
            if not isinstance(cols, list):
                raise PortabilityValidationError(f"columns must be a list for table {tbl}")
            for c in cols:
                if not isinstance(c, dict) or "name" not in c:
                    raise PortabilityValidationError(f"column definition invalid in table {tbl}")

        checkpoints = self._pkg.get("checkpoints") or {}
        if not isinstance(checkpoints, dict):
            raise PortabilityValidationError("checkpoints section must be a dictionary")

        for k, v in checkpoints.items():
            if not isinstance(v, dict):
                raise PortabilityValidationError(f"checkpoint for table {k} is invalid")

        return True

    @property
    def source_engine(self) -> str:
        return self._pkg.get("source_engine", "")

    @property
    def target_engine(self) -> str:
        return self._pkg.get("target_engine", "")

    @property
    def hop_name(self) -> str:
        return self._pkg.get("hop", "")

    @property
    def version(self) -> str:
        return self._pkg.get("migkit_version", "")

    def get_schemas(self) -> Dict[str, Any]:
        return self._pkg.get("schemas", {})

    def get_schema(self, table: str) -> Dict[str, Any]:
        schemas = self.get_schemas()
        if table not in schemas:
            raise KeyError(f"table {table} not in imported schemas")
        return schemas[table]

    def get_mappings(self, table: Optional[str] = None) -> Dict[str, Any]:
        maps = self._pkg.get("mappings", {})
        if table is not None:
            return maps.get(table, {})
        return maps

    def get_tool_config(self) -> Dict[str, Any]:
        return self._pkg.get("tool_config", {})

    def get_manifest(self, table: str) -> Dict[str, Any]:
        manifests = self._pkg.get("manifests", {})
        return manifests.get(table, {})

    def get_checkpoint(self, table: str) -> Dict[str, Any]:
        checkpoints = self._pkg.get("checkpoints", {})
        return checkpoints.get(table, {})

    def get_watermark(self, table: Optional[str] = None) -> Any:
        if table and table in self.get_checkpoint(table):
            return self.get_checkpoint(table).get("watermark")
        watermarks = self._pkg.get("watermarks", {})
        if table and table in watermarks:
            return watermarks[table]
        if watermarks:
            return next(iter(watermarks.values()))
        return None

    def get_sequence_offsets(self, table: Optional[str] = None) -> Dict[str, Any]:
        if table:
            s = self.get_schema(table)
            seq = s.get("sequence_offset") or {}
            cp = self.get_checkpoint(table)
            if cp.get("sequence_offsets"):
                seq.update(cp["sequence_offsets"])
            return seq
        out = {}
        for tbl in self.get_schemas():
            out[tbl] = self.get_sequence_offsets(tbl)
        return out

    def resume_offsets(self, table: Optional[str] = None) -> Dict[str, Any]:
        return {
            "table": table,
            "watermark": self.get_watermark(table),
            "sequence_offsets": self.get_sequence_offsets(table),
        }

    def can_resume_without_rescan(self, table: str) -> bool:
        cp = self.get_checkpoint(table)
        if not cp:
            return False
        done = cp.get("done") or {}
        return len(done) > 0

    def pending_ranges(
        self,
        table: str,
        proposed_ranges: List[Tuple[Any, Any]],
    ) -> List[Tuple[Any, Any]]:
        cp = self.get_checkpoint(table)
        if not cp:
            return list(proposed_ranges)
        done = set((cp.get("done") or {}).keys())
        return [(lo, hi) for lo, hi in proposed_ranges if cp_mod._key(lo, hi) not in done]

    def completed_ranges(self, table: str) -> Dict[str, Tuple[int, str]]:
        cp = self.get_checkpoint(table)
        if not cp:
            return {}
        done = cp.get("done") or {}
        return {k: (int(v[0]), str(v[1])) for k, v in done.items()}

    def total_completed(self, table: str) -> Tuple[int, str]:
        completed = self.completed_ranges(table)
        rows, ck = 0, 0
        for r, c in completed.values():
            rows += r
            ck += int(c)
        return rows, str(ck)

    def reconstitute_checkpoint(self, target_path: Optional[Union[str, Path]] = None) -> cp_mod.Checkpoint:
        t_path = str(target_path) if target_path else ""
        if t_path:
            p = Path(t_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            tbl_entries = {}
            for tbl, cp_data in (self._pkg.get("checkpoints") or {}).items():
                tbl_entries[tbl] = {
                    "fingerprint": cp_data.get("fingerprint", ""),
                    "salt": cp_data.get("salt", ""),
                    "done": cp_data.get("done", {}),
                    "cursors": cp_data.get("cursors", {}),
                    "chunk": self.get_tool_config().get("chunk_size", 10000),
                }
            merged = {
                "format_version": cp_mod.FORMAT_VERSION,
                "tables": tbl_entries,
            }
            durable.write_atomic(t_path, json.dumps(merged, separators=(",", ":")))

        cp = cp_mod.Checkpoint(t_path)
        return cp

    def reconstitute_scoped_checkpoint(
        self,
        table: str,
        target_path: Optional[Union[str, Path]] = None,
    ) -> ScopedCheckpoint:
        t_path = str(target_path) if target_path else ""
        scoped_headers = self._pkg.get("scoped_headers") or {}
        hdr = scoped_headers.get(table)

        if hdr:
            scope = ResumeScope(
                tables=hdr.get("tables", [table]),
                filter_predicate=hdr.get("filter_predicate"),
                column_mapping=hdr.get("column_mapping"),
                tenant_predicate=hdr.get("tenant_predicate"),
                scope_id=hdr.get("scope_identity"),
            )
        else:
            mappings = self.get_mappings(table)
            scope = ResumeScope(tables=[table], column_mapping=mappings)

        if t_path:
            p = Path(t_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            cp_data = self.get_checkpoint(table)
            merged = {
                "format_version": cp_mod.FORMAT_VERSION,
                "scope_header": scope.header(),
                "tables": {
                    table: {
                        "fingerprint": cp_data.get("fingerprint", ""),
                        "salt": cp_data.get("salt", ""),
                        "done": cp_data.get("done", {}),
                        "cursors": cp_data.get("cursors", {}),
                        "chunk": self.get_tool_config().get("chunk_size", 10000),
                        "units": cp_data.get("units", {}),
                    }
                },
                "units": cp_data.get("units", {}),
            }
            durable.write_atomic(t_path, json.dumps(merged, separators=(",", ":")))

        scoped_cp = ScopedCheckpoint(t_path, scope)
        return scoped_cp

    def resume_sync_state(self, table: Optional[str] = None) -> Dict[str, Any]:
        cp = self.get_checkpoint(table) if table else {}
        return {
            "table": table,
            "resumable": self.can_resume_without_rescan(table) if table else False,
            "watermark": self.get_watermark(table),
            "sequence_offsets": self.get_sequence_offsets(table),
            "completed_units": len(cp.get("done", {})) if cp else 0,
        }
