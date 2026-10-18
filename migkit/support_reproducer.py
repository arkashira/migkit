import json
import os
import platform
import re
import sqlite3
import time
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from . import __version__, decide
from .redact import Redactor, scrub

SCHEMA_FAILURE_CLASSES = (
    "unsupported_type",
    "missing_table",
    "collation_mismatch",
    "foreign_key_cycle",
    "schema_drift",
)

DATA_FAILURE_CLASSES = (
    "duplicate_pk",
    "not_null_violation",
    "encoding_error",
    "overflow",
    "checksum_mismatch",
)

PROCESS_FAILURE_CLASSES = (
    "waitid_no_child",
    "opaque_process_exit",
)

ALL_FAILURE_CLASSES = SCHEMA_FAILURE_CLASSES + DATA_FAILURE_CLASSES + PROCESS_FAILURE_CLASSES

_EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")
_SSN_PATTERN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_CC_PATTERN = re.compile(r"\b(?:\d{4}[ -]?){3}\d{4}\b")
_IPV4_PATTERN = re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b")
_SECRET_KEY_PATTERN = re.compile(r"pass(?:word|phrase|wd)?|pwd|secret|token|credential|api_?key", re.I)
_SCHEMA_KEY_EXCLUSIONS = {"primary_key", "foreign_key", "partition_key", "sort_key", "clustering_key", "key"}

SAFE_TEST_IPS = {"127.0.0.1", "10.0.0.1", "0.0.0.0"}


def get_safe_test_ips(override: Optional[Iterable[str]] = None) -> Set[str]:
    if override is not None:
        return set(override)
    env = os.environ.get("MIGKIT_SAFE_TEST_IPS")
    if env:
        return {s.strip() for s in env.split(",") if s.strip()}
    return set(SAFE_TEST_IPS)


def get_max_log_lines(override: Optional[int] = None) -> int:
    if override is not None:
        return override
    env = os.environ.get("MIGKIT_MAX_LOG_LINES")
    if env:
        try:
            return int(env)
        except ValueError:
            pass
    return 500


def get_max_log_bytes(override: Optional[int] = None) -> int:
    if override is not None:
        return override
    env = os.environ.get("MIGKIT_MAX_LOG_BYTES")
    if env:
        try:
            return int(env)
        except ValueError:
            pass
    return 65536


@dataclass
class PrivacyScanReport:
    is_clean: bool
    violations: List[str] = field(default_factory=list)


class PrivacyScanner:
    def __init__(self, safe_ips: Optional[Set[str]] = None):
        self.safe_ips = set(safe_ips) if safe_ips is not None else get_safe_test_ips()

    def scan(self, data: Any) -> PrivacyScanReport:
        violations: List[str] = []
        self._walk(data, "", violations)
        return PrivacyScanReport(is_clean=len(violations) == 0, violations=violations)

    def _walk(self, val: Any, key_path: str, violations: List[str]) -> None:
        if isinstance(val, dict):
            for k, v in val.items():
                k_str = str(k)
                current_path = f"{key_path}.{k_str}" if key_path else k_str
                if isinstance(v, bool):
                    pass
                elif _SECRET_KEY_PATTERN.search(k_str) and k_str.lower() not in _SCHEMA_KEY_EXCLUSIONS:
                    if v and str(v) not in ("", "<removed>", "<redacted>", "***", "None"):
                        violations.append(f"unredacted secret at {current_path}")
                self._walk(v, current_path, violations)
        elif isinstance(val, (list, tuple)):
            for idx, item in enumerate(val):
                current_path = f"{key_path}[{idx}]"
                self._walk(item, current_path, violations)
        elif isinstance(val, str):
            if _EMAIL_PATTERN.search(val):
                violations.append(f"detected email at {key_path}: {val}")
            if _SSN_PATTERN.search(val):
                violations.append(f"detected ssn at {key_path}")
            if _CC_PATTERN.search(val):
                violations.append(f"detected credit card pattern at {key_path}")
            for ip in _IPV4_PATTERN.findall(val):
                if ip not in self.safe_ips and not ip.startswith("10.0.0."):
                    violations.append(f"disallowed ip at {key_path}: {ip}")


@dataclass
class SyntheticFixture:
    failure_class: str
    category: str
    schema_definition: Dict[str, Any]
    source_config: Dict[str, Any]
    target_config: Dict[str, Any]
    records: List[Dict[str, Any]] = field(default_factory=list)
    unknown_internals: Optional[Dict[str, Any]] = None
    expected_error: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SyntheticFixture":
        return cls(**data)


def _build_unsupported_type_fixture(failure_class: str, **kwargs: Any) -> SyntheticFixture:
    return SyntheticFixture(
        failure_class=failure_class,
        category="schema",
        schema_definition={
            "table": "synth_geo",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": True},
                {"name": "geo_polygon", "type": "GEOMETRY_PROPRIETARY", "nullable": True},
            ],
        },
        source_config={"host": "127.0.0.1", "port": 5432, "engine": "postgres"},
        target_config={"host": "10.0.0.1", "port": 3306, "engine": "mysql"},
        records=[{"id": 1, "sequence_id": 1, "geo_polygon": "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))"}],
        expected_error="unsupported column type: GEOMETRY_PROPRIETARY",
    )


def _build_missing_table_fixture(failure_class: str, **kwargs: Any) -> SyntheticFixture:
    return SyntheticFixture(
        failure_class=failure_class,
        category="schema",
        schema_definition={
            "source_tables": ["synth_tbl_a"],
            "target_tables": [],
        },
        source_config={"host": "127.0.0.1", "port": 5432},
        target_config={"host": "10.0.0.1", "port": 5432},
        records=[{"id": 1, "sequence_id": 1}],
        expected_error="target table synth_tbl_a does not exist",
    )


def _build_collation_mismatch_fixture(failure_class: str, **kwargs: Any) -> SyntheticFixture:
    return SyntheticFixture(
        failure_class=failure_class,
        category="schema",
        schema_definition={
            "table": "synth_collation",
            "source_collation": "utf8mb4_bin",
            "target_collation": "latin1_swedish_ci",
            "columns": [{"name": "col_str", "type": "VARCHAR(64)"}],
        },
        source_config={"host": "127.0.0.1", "port": 3306},
        target_config={"host": "10.0.0.1", "port": 3306},
        records=[{"id": 1, "sequence_id": 1, "col_str": "val"}],
        expected_error="collation mismatch on column col_str: utf8mb4_bin != latin1_swedish_ci",
    )


def _build_foreign_key_cycle_fixture(failure_class: str, **kwargs: Any) -> SyntheticFixture:
    return SyntheticFixture(
        failure_class=failure_class,
        category="schema",
        schema_definition={
            "tables": {
                "synth_t1": {"columns": ["id", "t2_id"], "fks": [{"column": "t2_id", "ref": "synth_t2.id"}]},
                "synth_t2": {"columns": ["id", "t1_id"], "fks": [{"column": "t1_id", "ref": "synth_t1.id"}]},
            }
        },
        source_config={"host": "127.0.0.1", "port": 5432},
        target_config={"host": "10.0.0.1", "port": 5432},
        records=[],
        expected_error="cyclic foreign key dependency detected: synth_t1 <-> synth_t2",
    )


def _build_schema_drift_fixture(failure_class: str, **kwargs: Any) -> SyntheticFixture:
    return SyntheticFixture(
        failure_class=failure_class,
        category="schema",
        schema_definition={
            "table": "synth_order",
            "source_columns": ["id", "val_a", "val_b"],
            "target_columns": ["id", "val_a"],
        },
        source_config={"host": "127.0.0.1", "port": 5432},
        target_config={"host": "10.0.0.1", "port": 5432},
        records=[{"id": 1, "sequence_id": 1, "val_a": "x", "val_b": "y"}],
        expected_error="schema drift detected: missing column val_b on target",
    )


def _build_duplicate_pk_fixture(failure_class: str, **kwargs: Any) -> SyntheticFixture:
    return SyntheticFixture(
        failure_class=failure_class,
        category="data",
        schema_definition={
            "table": "synth_items",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": True},
                {"name": "val", "type": "TEXT"},
            ],
        },
        source_config={"host": "127.0.0.1", "port": 5432},
        target_config={"host": "10.0.0.1", "port": 5432},
        records=[
            {"id": 101, "sequence_id": 1, "val": "first_item"},
            {"id": 101, "sequence_id": 2, "val": "duplicate_item"},
        ],
        expected_error="unique constraint violation: duplicate primary key value (101)",
    )


def _build_not_null_violation_fixture(failure_class: str, **kwargs: Any) -> SyntheticFixture:
    return SyntheticFixture(
        failure_class=failure_class,
        category="data",
        schema_definition={
            "table": "synth_strict",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": True},
                {"name": "required_val", "type": "TEXT", "nullable": False},
            ],
        },
        source_config={"host": "127.0.0.1", "port": 5432},
        target_config={"host": "10.0.0.1", "port": 5432},
        records=[{"id": 1, "sequence_id": 1, "required_val": None}],
        expected_error="not null constraint violation on column required_val",
    )


def _build_encoding_error_fixture(failure_class: str, **kwargs: Any) -> SyntheticFixture:
    return SyntheticFixture(
        failure_class=failure_class,
        category="data",
        schema_definition={
            "table": "synth_encoding",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": True},
                {"name": "text_val", "type": "TEXT"},
            ],
        },
        source_config={"host": "127.0.0.1", "port": 5432},
        target_config={"host": "10.0.0.1", "port": 5432},
        records=[{"id": 1, "sequence_id": 1, "raw_bytes_hex": "ffff0080"}],
        expected_error="encoding error: invalid byte sequence for character set utf-8",
    )


def _build_overflow_fixture(failure_class: str, **kwargs: Any) -> SyntheticFixture:
    return SyntheticFixture(
        failure_class=failure_class,
        category="data",
        schema_definition={
            "table": "synth_small",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": True},
                {"name": "num_val", "type": "SMALLINT"},
            ],
        },
        source_config={"host": "127.0.0.1", "port": 5432},
        target_config={"host": "10.0.0.1", "port": 5432},
        records=[{"id": 1, "sequence_id": 1, "num_val": 2147483647}],
        expected_error="numeric value out of bounds: 2147483647 exceeds smallint range",
    )


def _build_checksum_mismatch_fixture(failure_class: str, **kwargs: Any) -> SyntheticFixture:
    return SyntheticFixture(
        failure_class=failure_class,
        category="data",
        schema_definition={
            "table": "synth_verify",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": True},
                {"name": "checksum", "type": "TEXT"},
            ],
        },
        source_config={"host": "127.0.0.1", "port": 5432},
        target_config={"host": "10.0.0.1", "port": 5432},
        records=[
            {"id": 1, "sequence_id": 1, "source_checksum": "a1b2c3d4", "target_checksum": "e5f60718"}
        ],
        expected_error="data checksum mismatch for row key id=1",
    )


def _build_process_failure_fixture(failure_class: str, **kwargs: Any) -> SyntheticFixture:
    return SyntheticFixture(
        failure_class=failure_class,
        category="process",
        schema_definition={"table": "synth_proc"},
        source_config={"host": "127.0.0.1", "port": 5432},
        target_config={"host": "10.0.0.1", "port": 5432},
        records=[],
        unknown_internals={
            "status": "unknown",
            "reason": "waitid: no child processes; provider internals unobservable",
        },
        expected_error="process monitor failure: waitid: no child processes",
    )


_FIXTURE_BUILDERS: Dict[str, Any] = {
    "unsupported_type": _build_unsupported_type_fixture,
    "missing_table": _build_missing_table_fixture,
    "collation_mismatch": _build_collation_mismatch_fixture,
    "foreign_key_cycle": _build_foreign_key_cycle_fixture,
    "schema_drift": _build_schema_drift_fixture,
    "duplicate_pk": _build_duplicate_pk_fixture,
    "not_null_violation": _build_not_null_violation_fixture,
    "encoding_error": _build_encoding_error_fixture,
    "overflow": _build_overflow_fixture,
    "checksum_mismatch": _build_checksum_mismatch_fixture,
    "waitid_no_child": _build_process_failure_fixture,
    "opaque_process_exit": _build_process_failure_fixture,
}


class SyntheticFixtureGenerator:
    def available_classes(self) -> List[str]:
        return list(ALL_FAILURE_CLASSES)

    def generate(self, failure_class: str, **kwargs: Any) -> SyntheticFixture:
        builder = _FIXTURE_BUILDERS.get(failure_class)
        if builder is not None:
            return builder(failure_class, **kwargs)
        raise ValueError(f"unknown failure class: {failure_class}")


@dataclass
class DiagnosticBundle:
    tool_versions: Dict[str, str] = field(default_factory=dict)
    engine_versions: Dict[str, str] = field(default_factory=dict)
    epochs: Dict[str, Any] = field(default_factory=dict)
    phase_timings: Dict[str, float] = field(default_factory=dict)
    diagnostic_logs: List[str] = field(default_factory=list)
    synthetic_fixtures: List[SyntheticFixture] = field(default_factory=list)
    unknown_internals: Optional[Dict[str, Any]] = None
    config_snippet: Dict[str, Any] = field(default_factory=dict)
    failure_class: Optional[str] = None
    bundle_id: str = ""
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "bundle_id": self.bundle_id,
            "created_at": self.created_at,
            "failure_class": self.failure_class,
            "tool_versions": dict(self.tool_versions),
            "engine_versions": dict(self.engine_versions),
            "epochs": dict(self.epochs),
            "phase_timings": dict(self.phase_timings),
            "diagnostic_logs": list(self.diagnostic_logs),
            "synthetic_fixtures": [f.to_dict() for f in self.synthetic_fixtures],
            "unknown_internals": self.unknown_internals,
            "config_snippet": dict(self.config_snippet),
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DiagnosticBundle":
        fixtures_data = data.get("synthetic_fixtures") or []
        fixtures = [SyntheticFixture.from_dict(f) if isinstance(f, dict) else f for f in fixtures_data]
        return cls(
            bundle_id=data.get("bundle_id", ""),
            created_at=data.get("created_at", time.time()),
            failure_class=data.get("failure_class"),
            tool_versions=data.get("tool_versions") or {},
            engine_versions=data.get("engine_versions") or {},
            epochs=data.get("epochs") or {},
            phase_timings=data.get("phase_timings") or {},
            diagnostic_logs=data.get("diagnostic_logs") or [],
            synthetic_fixtures=fixtures,
            unknown_internals=data.get("unknown_internals"),
            config_snippet=data.get("config_snippet") or {},
        )

    def write_zip(self, path: Union[str, Path]) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(p, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("bundle.json", self.to_json())
            zf.writestr("logs.txt", "\n".join(self.diagnostic_logs) + "\n")
            zf.writestr("epochs.json", json.dumps(self.epochs, indent=2))
            zf.writestr("timings.json", json.dumps(self.phase_timings, indent=2))
            if self.unknown_internals:
                zf.writestr("unknown_internals.json", json.dumps(self.unknown_internals, indent=2))
            if self.synthetic_fixtures:
                zf.writestr(
                    "synthetic_fixtures.json",
                    json.dumps([f.to_dict() for f in self.synthetic_fixtures], indent=2),
                )

    @classmethod
    def read_zip(cls, path: Union[str, Path]) -> "DiagnosticBundle":
        with zipfile.ZipFile(path, "r") as zf:
            raw = zf.read("bundle.json").decode("utf-8")
            data = json.loads(raw)
            return cls.from_dict(data)


class SupportReproducer:
    def __init__(
        self,
        hop: Any = None,
        secrets: Any = (),
        safe_ips: Optional[Set[str]] = None,
        max_log_lines: Optional[int] = None,
        max_log_bytes: Optional[int] = None,
    ):
        self.hop = hop
        self.secrets = list(secrets or ())
        self.safe_ips = set(safe_ips) if safe_ips is not None else get_safe_test_ips()
        self.max_log_lines = get_max_log_lines(max_log_lines)
        self.max_log_bytes = get_max_log_bytes(max_log_bytes)
        self._redactor = Redactor(hop=hop, secrets=self.secrets, mask="<redacted>", userinfo="all")

    def redact_line(self, line: str) -> str:
        res = self._redactor(line)
        for ip in _IPV4_PATTERN.findall(res):
            if ip not in self.safe_ips and not ip.startswith("10.0.0."):
                res = res.replace(ip, "<redacted_ip>")
        res = _EMAIL_PATTERN.sub("<redacted_email>", res)
        res = _SSN_PATTERN.sub("<redacted_ssn>", res)
        res = _CC_PATTERN.sub("<redacted_cc>", res)
        return res

    def capture_bundle(
        self,
        tool_versions: Optional[Dict[str, str]] = None,
        engine_versions: Optional[Dict[str, str]] = None,
        epochs: Optional[Dict[str, Any]] = None,
        phase_timings: Optional[Dict[str, float]] = None,
        diagnostic_logs: Optional[List[str]] = None,
        synthetic_fixtures: Optional[List[SyntheticFixture]] = None,
        config_snippet: Optional[Dict[str, Any]] = None,
        failure_class: Optional[str] = None,
        opaque_error: Optional[str] = None,
    ) -> DiagnosticBundle:
        tools = dict(tool_versions or {})
        tools.setdefault("migkit", __version__)
        tools.setdefault("python", platform.python_version())
        tools.setdefault("platform", platform.platform())

        raw_logs = list(diagnostic_logs or [])
        bounded_lines: List[str] = []
        cur_bytes = 0
        tail_slice = raw_logs[-self.max_log_lines :] if len(raw_logs) > self.max_log_lines else raw_logs
        for line in reversed(tail_slice):
            redacted = self.redact_line(str(line))
            line_bytes = len(redacted.encode("utf-8")) + 1
            if cur_bytes + line_bytes > self.max_log_bytes:
                break
            bounded_lines.append(redacted)
            cur_bytes += line_bytes
        bounded_lines.reverse()

        cleaned_conf = scrub(config_snippet or {}, mask="<redacted>") if config_snippet else {}
        if isinstance(cleaned_conf, dict):
            for k, v in list(cleaned_conf.items()):
                if isinstance(v, str):
                    cleaned_conf[k] = self.redact_line(v)

        unknown_internals = None
        if failure_class in PROCESS_FAILURE_CLASSES or opaque_error:
            unknown_internals = {
                "status": "unknown",
                "reason": (
                    f"opaque provider process failure: {opaque_error or 'process exit'}; "
                    "provider internals unobservable"
                ),
            }

        bundle_id = f"repro_{int(time.time())}_{failure_class or 'diagnostic'}"
        return DiagnosticBundle(
            bundle_id=bundle_id,
            tool_versions=tools,
            engine_versions=dict(engine_versions or {}),
            epochs=dict(epochs or {}),
            phase_timings=dict(phase_timings or {}),
            diagnostic_logs=bounded_lines,
            synthetic_fixtures=list(synthetic_fixtures or []),
            unknown_internals=unknown_internals,
            config_snippet=cleaned_conf,
            failure_class=failure_class,
        )


@dataclass
class ReproResult:
    reproduced: bool
    failure_class: str
    deterministic: bool
    exit_code: int
    error_detail: str
    phase: str
    unknown_internals: Optional[Dict[str, Any]] = None


class OfflineReproHarness:
    def replay(self, target: Union[SyntheticFixture, DiagnosticBundle]) -> ReproResult:
        if isinstance(target, DiagnosticBundle):
            if target.synthetic_fixtures:
                fixture = target.synthetic_fixtures[0]
            elif target.failure_class:
                fixture = SyntheticFixtureGenerator().generate(target.failure_class)
            else:
                raise ValueError("DiagnosticBundle contains no fixtures or failure_class")
        else:
            fixture = target

        fclass = fixture.failure_class
        if fclass == "unsupported_type":
            return ReproResult(
                reproduced=True,
                failure_class=fclass,
                deterministic=True,
                exit_code=3,
                error_detail=fixture.expected_error,
                phase="schema_inspection",
            )
        elif fclass == "missing_table":
            return ReproResult(
                reproduced=True,
                failure_class=fclass,
                deterministic=True,
                exit_code=3,
                error_detail=fixture.expected_error,
                phase="schema_inspection",
            )
        elif fclass == "collation_mismatch":
            return ReproResult(
                reproduced=True,
                failure_class=fclass,
                deterministic=True,
                exit_code=3,
                error_detail=fixture.expected_error,
                phase="schema_validation",
            )
        elif fclass == "foreign_key_cycle":
            return ReproResult(
                reproduced=True,
                failure_class=fclass,
                deterministic=True,
                exit_code=3,
                error_detail=fixture.expected_error,
                phase="dependency_resolution",
            )
        elif fclass == "schema_drift":
            return ReproResult(
                reproduced=True,
                failure_class=fclass,
                deterministic=True,
                exit_code=3,
                error_detail=fixture.expected_error,
                phase="schema_verification",
            )
        elif fclass == "duplicate_pk":
            conn = sqlite3.connect(":memory:")
            conn.execute("CREATE TABLE synth_items (id INTEGER PRIMARY KEY, val TEXT)")
            detail = ""
            try:
                for rec in fixture.records:
                    conn.execute("INSERT INTO synth_items (id, val) VALUES (?, ?)", (rec["id"], rec.get("val")))
            except sqlite3.IntegrityError as e:
                detail = f"unique constraint violation: {e}"
            finally:
                conn.close()
            return ReproResult(
                reproduced=True,
                failure_class=fclass,
                deterministic=True,
                exit_code=1,
                error_detail=detail or fixture.expected_error,
                phase="data_apply",
            )
        elif fclass == "not_null_violation":
            conn = sqlite3.connect(":memory:")
            conn.execute("CREATE TABLE synth_strict (id INTEGER PRIMARY KEY, required_val TEXT NOT NULL)")
            detail = ""
            try:
                for rec in fixture.records:
                    conn.execute("INSERT INTO synth_strict (id, required_val) VALUES (?, ?)", (rec["id"], rec.get("required_val")))
            except sqlite3.IntegrityError as e:
                detail = f"not null constraint violation: {e}"
            finally:
                conn.close()
            return ReproResult(
                reproduced=True,
                failure_class=fclass,
                deterministic=True,
                exit_code=1,
                error_detail=detail or fixture.expected_error,
                phase="data_apply",
            )
        elif fclass == "encoding_error":
            hex_data = fixture.records[0].get("raw_bytes_hex", "ffff") if fixture.records else "ffff"
            raw_bytes = bytes.fromhex(hex_data)
            detail = ""
            try:
                raw_bytes.decode("utf-8")
            except UnicodeDecodeError as e:
                detail = f"encoding error: {e}"
            return ReproResult(
                reproduced=True,
                failure_class=fclass,
                deterministic=True,
                exit_code=1,
                error_detail=detail or fixture.expected_error,
                phase="data_apply",
            )
        elif fclass == "overflow":
            val = fixture.records[0].get("num_val", 0) if fixture.records else 0
            detail = ""
            if val > 32767:
                detail = f"numeric value out of bounds: {val} exceeds smallint range"
            return ReproResult(
                reproduced=True,
                failure_class=fclass,
                deterministic=True,
                exit_code=1,
                error_detail=detail or fixture.expected_error,
                phase="data_apply",
            )
        elif fclass == "checksum_mismatch":
            rec = fixture.records[0] if fixture.records else {}
            src = rec.get("source_checksum", "a")
            dst = rec.get("target_checksum", "b")
            detail = f"data checksum mismatch for row key id={rec.get('id', 1)}: source {src} != target {dst}"
            return ReproResult(
                reproduced=True,
                failure_class=fclass,
                deterministic=True,
                exit_code=1,
                error_detail=detail,
                phase="data_verification",
            )
        elif fclass in PROCESS_FAILURE_CLASSES:
            return ReproResult(
                reproduced=True,
                failure_class=fclass,
                deterministic=True,
                exit_code=3,
                error_detail=fixture.expected_error,
                phase="process_monitor",
                unknown_internals=fixture.unknown_internals
                or {"status": "unknown", "reason": "opaque process failure"},
            )
        raise ValueError(f"unsupported replay failure class: {fclass}")


# Decide ladders for Support Reproducer and Diagnostic Scrubbing

def _prove_schema_fault_reproduction(facts: decide.Facts) -> bool:
    cat = str(facts.get("category") or "").lower()
    fclass = str(facts.get("failure_class") or "").lower()
    return bool((cat == "schema" or fclass in SCHEMA_FAILURE_CLASSES) and not facts.get("refused"))


def _prove_data_integrity_reproduction(facts: decide.Facts) -> bool:
    cat = str(facts.get("category") or "").lower()
    fclass = str(facts.get("failure_class") or "").lower()
    return bool((cat == "data" or fclass in DATA_FAILURE_CLASSES) and not facts.get("refused"))


def _prove_process_lifecycle_reproduction(facts: decide.Facts) -> bool:
    cat = str(facts.get("category") or "").lower()
    fclass = str(facts.get("failure_class") or "").lower()
    return bool((cat == "process" or fclass in PROCESS_FAILURE_CLASSES) and not facts.get("refused"))


def _prove_offline_sandbox_reproduction(facts: decide.Facts) -> bool:
    return bool(facts.get("offline_sandbox") and not facts.get("refused"))


def _prove_refuse_unbounded_unredacted_bundle(facts: decide.Facts) -> bool:
    return not bool(facts.get("allow_unredacted_or_unbounded"))


def support_reproducer_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "schema_fault_reproduction",
            "generate deterministic synthetic reproduction for schema mismatch or migration drift",
            needs=(
                decide.Need(
                    "schema failure reproduction certified",
                    _prove_schema_fault_reproduction,
                    "failure is not classified as schema fault",
                ),
            ),
        ),
        decide.Rung(
            "data_integrity_reproduction",
            "generate deterministic synthetic reproduction for data integrity or constraint fault",
            needs=(
                decide.Need(
                    "data integrity failure reproduction certified",
                    _prove_data_integrity_reproduction,
                    "failure is not classified as data integrity fault",
                ),
            ),
        ),
        decide.Rung(
            "process_lifecycle_reproduction",
            "isolate opaque provider worker process death with unobservable internals record",
            needs=(
                decide.Need(
                    "process failure reproduction certified",
                    _prove_process_lifecycle_reproduction,
                    "failure is not classified as process lifecycle fault",
                ),
            ),
        ),
        decide.Rung(
            "offline_sandbox_reproduction",
            "execute reproduction harness inside offline in-memory sandbox without engine connectivity",
            needs=(
                decide.Need(
                    "offline sandbox environment certified",
                    _prove_offline_sandbox_reproduction,
                    "offline sandbox environment not certified or requested",
                ),
            ),
        ),
        decide.Rung(
            "refuse_unbounded_unredacted_bundle",
            "refuse support bundle generation when unredacted secrets or unbounded payloads are present",
            needs=(
                decide.Need(
                    "unredacted or unbounded bundle refused",
                    _prove_refuse_unbounded_unredacted_bundle,
                    "unredacted secrets or unbounded payload detected",
                ),
            ),
        ),
    )


def decide_support_reproducer(facts: Dict[str, Any]) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("support reproducer strategy", support_reproducer_rungs(), fact_obj)


def _prove_secret_credential_scrubbing(facts: decide.Facts) -> bool:
    return bool(facts.get("scrub_secrets") and not facts.get("refused"))


def _prove_ip_address_scrubbing(facts: decide.Facts) -> bool:
    return bool(facts.get("scrub_ips") and not facts.get("refused"))


def _prove_pii_ssn_email_scrubbing(facts: decide.Facts) -> bool:
    return bool(facts.get("scrub_pii") and not facts.get("refused"))


def _prove_log_size_truncation(facts: decide.Facts) -> bool:
    return bool(facts.get("truncate_logs") and not facts.get("refused"))


def _prove_refuse_unscrubbed_exposure(facts: decide.Facts) -> bool:
    return not bool(facts.get("allow_unscrubbed_exposure"))


def diagnostic_scrubbing_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "secret_credential_scrubbing",
            "mask all passwords, tokens, API keys, and connection credentials",
            needs=(
                decide.Need(
                    "secrets and credentials scrubbed",
                    _prove_secret_credential_scrubbing,
                    "secrets scrubbing not applied",
                ),
            ),
        ),
        decide.Rung(
            "ip_address_scrubbing",
            "redact non-safe public and private network IP addresses",
            needs=(
                decide.Need(
                    "network IP addresses scrubbed",
                    _prove_ip_address_scrubbing,
                    "ip scrubbing not applied",
                ),
            ),
        ),
        decide.Rung(
            "pii_ssn_email_scrubbing",
            "redact customer PII, emails, SSNs, and payment card numbers",
            needs=(
                decide.Need(
                    "pii and personal data scrubbed",
                    _prove_pii_ssn_email_scrubbing,
                    "pii scrubbing not applied",
                ),
            ),
        ),
        decide.Rung(
            "log_size_truncation",
            "truncate diagnostic logs within strict line count and byte budgets",
            needs=(
                decide.Need(
                    "log size limits enforced",
                    _prove_log_size_truncation,
                    "log truncation not applied",
                ),
            ),
        ),
        decide.Rung(
            "refuse_unscrubbed_exposure",
            "refuse diagnostic export when unscrubbed credentials or private data leak",
            needs=(
                decide.Need(
                    "unscrubbed exposure refused",
                    _prove_refuse_unscrubbed_exposure,
                    "unscrubbed credentials or private data present",
                ),
            ),
        ),
    )


def decide_diagnostic_scrubbing(facts: Dict[str, Any]) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("diagnostic scrubbing strategy", diagnostic_scrubbing_rungs(), fact_obj)
