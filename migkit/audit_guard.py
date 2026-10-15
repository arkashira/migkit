"""Audit hardening and contracts across tail state, startup checks, schema drift,
MySQL unified connections, neutral shape comparison, and tool use (backlog AUD-B,
AUD-D, AUD-F, AUD-G2, AUD-H, 0f-DVT, 0f-REST).
"""
import dataclasses
from dataclasses import dataclass, field
import difflib
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


@dataclass
class Position:
    """Envelope for durable change-follow positions."""
    v: int = 1
    engine: str = ""
    source: str = ""
    position: Any = ""
    batch: int = 0
    rung: str = ""
    next: Any = ""

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: dict) -> "Position":
        return cls(
            v=int(data.get("v", 1)),
            engine=str(data.get("engine", "")),
            source=str(data.get("source", "")),
            position=data.get("position", ""),
            batch=int(data.get("batch", 0)),
            rung=str(data.get("rung", "")),
            next=data.get("next", ""),
        )

    @classmethod
    def from_json(cls, text: str) -> "Position":
        return cls.from_dict(json.loads(text))


def position_path(state_root: str, hop: str, tail_scope: str) -> Path:
    return Path(state_root) / hop / f"tail.{tail_scope}.position.json"


def write_durable_position(path: Path, pos: Position) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    data = pos.to_json().encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(str(tmp), flags, 0o600)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(str(tmp), str(path))


def read_durable_position(path: Path) -> Optional[Position]:
    path = Path(path)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return Position.from_json(f.read())
    except (json.JSONDecodeError, OSError):
        return None


class DurableTailState:
    """Durable SQLite-backed state store for tail transactions."""

    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.execute("PRAGMA user_version = 1")
            self._conn.execute(
                """CREATE TABLE IF NOT EXISTS batches (
                    batch_id INTEGER PRIMARY KEY,
                    position TEXT NOT NULL,
                    count INTEGER NOT NULL,
                    committed_at REAL NOT NULL
                )"""
            )
            self._conn.execute(
                """CREATE TABLE IF NOT EXISTS table_states (
                    table_name TEXT PRIMARY KEY,
                    last_position TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    status TEXT NOT NULL
                )"""
            )
            self._conn.execute(
                """CREATE TABLE IF NOT EXISTS accounts (
                    account_key TEXT PRIMARY KEY,
                    balance_data TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )"""
            )
            self._conn.execute(
                """CREATE TABLE IF NOT EXISTS spool_frames (
                    frame_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    range_id TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL
                )"""
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_spool_range ON spool_frames (range_id)")

    def commit_batch(
        self,
        batch_id: int,
        position: str,
        count: int,
        table_states: Optional[Dict[str, Tuple[str, int, str]]] = None,
        accounts: Optional[Dict[str, str]] = None,
        spool_frames: Optional[List[Tuple[str, bytes, str]]] = None,
    ) -> None:
        now = time.time()
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO batches (batch_id, position, count, committed_at) VALUES (?, ?, ?, ?)",
                (batch_id, str(position), int(count), now),
            )
            if table_states:
                for tbl, (pos, rows, status) in table_states.items():
                    self._conn.execute(
                        "INSERT OR REPLACE INTO table_states (table_name, last_position, row_count, status) "
                        "VALUES (?, ?, ?, ?)",
                        (tbl, str(pos), int(rows), status),
                    )
            if accounts:
                for k, v in accounts.items():
                    self._conn.execute(
                        "INSERT OR REPLACE INTO accounts (account_key, balance_data, updated_at) VALUES (?, ?, ?)",
                        (k, str(v), now),
                    )
            if spool_frames:
                for rng, payload, st in spool_frames:
                    self._conn.execute(
                        "INSERT INTO spool_frames (range_id, payload, status, created_at) VALUES (?, ?, ?, ?)",
                        (rng, payload, st, now),
                    )

    def get_last_batch(self) -> Optional[dict]:
        cur = self._conn.execute("SELECT * FROM batches ORDER BY batch_id DESC LIMIT 1")
        row = cur.fetchone()
        return dict(row) if row else None

    def get_table_state(self, table_name: str) -> Optional[dict]:
        cur = self._conn.execute("SELECT * FROM table_states WHERE table_name = ?", (table_name,))
        row = cur.fetchone()
        return dict(row) if row else None

    def add_spool_frame(self, range_id: str, payload: bytes, status: str = "waiting") -> int:
        now = time.time()
        with self._conn:
            cur = self._conn.execute(
                "INSERT INTO spool_frames (range_id, payload, status, created_at) VALUES (?, ?, ?, ?)",
                (range_id, payload, status, now),
            )
            return cur.lastrowid

    def release_spool_frames(self, range_id: str) -> int:
        with self._conn:
            cur = self._conn.execute("DELETE FROM spool_frames WHERE range_id = ?", (range_id,))
            return cur.rowcount

    def close(self) -> None:
        self._conn.close()


class ConcurrentTailError(RuntimeError):
    pass


class SingleFollowLoopGuard:
    """Ensures only a single active follow loop holds lease over a tail scope."""

    def __init__(self, lease_dir: str, ttl_seconds: float = 30.0):
        self.lease_dir = Path(lease_dir)
        self.lease_dir.mkdir(parents=True, exist_ok=True)
        self.ttl = float(ttl_seconds)

    def _lease_file(self, scope: str) -> Path:
        return self.lease_dir / f"{scope}.lease.json"

    def is_alive(self, record: dict, now: Optional[float] = None) -> bool:
        if not record:
            return False
        now = time.time() if now is None else now
        if now > record.get("expires", 0):
            return False
        pid = record.get("pid")
        host = record.get("host")
        import socket
        my_host = socket.gethostname()
        if host == my_host and pid:
            try:
                os.kill(pid, 0)
                return True
            except OSError:
                return False
        return True

    def acquire(self, scope: str) -> dict:
        import socket
        now = time.time()
        path = self._lease_file(scope)
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    curr = json.load(f)
                if self.is_alive(curr, now=now):
                    raise ConcurrentTailError(
                        f"Scope '{scope}' is already held by host={curr.get('host')} pid={curr.get('pid')}"
                    )
            except (json.JSONDecodeError, OSError):
                pass

        record = {
            "scope": scope,
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "expires": now + self.ttl,
            "acquired_at": now,
        }
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(record, f)
        os.replace(str(tmp), str(path))
        return record

    def renew(self, scope: str) -> None:
        path = self._lease_file(scope)
        if not path.exists():
            raise ConcurrentTailError(f"Cannot renew lease for '{scope}': file does not exist")
        with open(path, "r", encoding="utf-8") as f:
            curr = json.load(f)
        if curr.get("pid") != os.getpid():
            raise ConcurrentTailError(f"Cannot renew lease for '{scope}': held by another pid")
        curr["expires"] = time.time() + self.ttl
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(curr, f)
        os.replace(str(tmp), str(path))

    def release(self, scope: str) -> None:
        path = self._lease_file(scope)
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    curr = json.load(f)
                if curr.get("pid") == os.getpid():
                    path.unlink()
            except OSError:
                pass


@dataclass
class StartupValidationReport:
    ok: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    suggestions: Dict[str, str] = field(default_factory=dict)


def validate_options(
    options: dict,
    allowed_keys: Set[str],
    prefix: str = "hops.default",
) -> StartupValidationReport:
    rep = StartupValidationReport()
    allowed_list = sorted(allowed_keys)
    for k in options:
        if k not in allowed_keys:
            rep.ok = False
            err_name = f"{prefix}.{k}"
            rep.errors.append(f"Unknown key: {err_name}")
            close = difflib.get_close_matches(k, allowed_list, n=1, cutoff=0.5)
            if close:
                rep.suggestions[err_name] = f"{prefix}.{close[0]}"
    return rep


KNOWN_MIGKIT_ENV_PREFIXES = (
    "MIGKIT_READ_TIMEOUT",
    "MIGKIT_WRITE_TIMEOUT",
    "MIGKIT_CONNECT_TIMEOUT",
    "MIGKIT_STATE_HOME",
    "MIGKIT_CONF",
    "MIGKIT_LOG_LEVEL",
    "MIGKIT_DEBUG",
    "MIGKIT_STRICT",
    "MIGKIT_TEST_STRICT",
    "MIGKIT_BUFFER_SIZE",
    "MIGKIT_BATCH_ROWS",
    "MIGKIT_WORKERS",
)


def validate_environment(
    env: Optional[Dict[str, str]] = None,
    declared_keys: Optional[Set[str]] = None,
) -> StartupValidationReport:
    environ = os.environ if env is None else env
    declared = set(KNOWN_MIGKIT_ENV_PREFIXES) if declared_keys is None else declared_keys
    rep = StartupValidationReport()
    for k in environ:
        if k.startswith("MIGKIT_") and k not in declared:
            rep.warnings.append(f"Undeclared environment variable: {k}")
            close = difflib.get_close_matches(k, sorted(declared), n=1, cutoff=0.5)
            if close:
                rep.suggestions[k] = close[0]
    return rep


def validate_address(
    address: str,
    side: str = "src",
    hop: str = "default",
) -> StartupValidationReport:
    rep = StartupValidationReport()
    text = str(address).strip()
    if not text:
        rep.ok = False
        rep.errors.append(f"hops.{hop}.{side}.address is empty")
        return rep

    if "://" not in text and "@" not in text and ":" not in text:
        rep.ok = False
        rep.errors.append(f"hops.{hop}.{side}.address lacks a valid scheme or host delimiter: {text}")
        return rep

    if text.startswith("secret:"):
        sec_ref = text.split("secret:", 1)[1]
        if not sec_ref.startswith(f"hops.{hop}.{side}.") and not sec_ref.startswith("vault:") and "/" not in sec_ref:
            rep.ok = False
            rep.errors.append(
                f"hops.{hop}.{side}.password secret reference must name 'hops.{hop}.{side}.<key>' or valid path"
            )

    return rep


def resolve_state_root(
    env: Optional[Dict[str, str]] = None,
    conf_dir: Optional[str] = None,
) -> Path:
    environ = os.environ if env is None else env
    if "XDG_STATE_HOME" in environ and environ["XDG_STATE_HOME"].strip():
        p = Path(environ["XDG_STATE_HOME"]) / "migkit"
    elif conf_dir:
        p = Path(conf_dir) / "state"
    else:
        p = Path.home() / ".local" / "state" / "migkit"
    return p


def secure_permissions(path: Path, is_dir: bool = False) -> None:
    p = Path(path)
    if is_dir:
        p.mkdir(parents=True, exist_ok=True)
        os.chmod(str(p), 0o700)
    else:
        if p.exists():
            os.chmod(str(p), 0o600)


@dataclass
class SchemaDriftReport:
    created: List[str] = field(default_factory=list)
    dropped: List[str] = field(default_factory=list)
    altered: List[str] = field(default_factory=list)
    details: Dict[str, List[str]] = field(default_factory=dict)
    needs_reload: Set[str] = field(default_factory=set)
    needs_backfill: Set[str] = field(default_factory=set)

    @property
    def has_drift(self) -> bool:
        return bool(self.created or self.dropped or self.altered)


class SingleCatalogueDriftDetector:
    """Detects schema drift between two catalogue snapshots using definition fingerprints."""

    @staticmethod
    def fingerprint_table(table_def: dict) -> str:
        s = json.dumps(table_def, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    def detect_drift(
        self,
        before_catalog: dict,
        after_catalog: dict,
    ) -> SchemaDriftReport:
        report = SchemaDriftReport()
        before_tables = set(before_catalog.keys())
        after_tables = set(after_catalog.keys())

        report.created = sorted(after_tables - before_tables)
        report.dropped = sorted(before_tables - after_tables)

        common = sorted(before_tables & after_tables)
        for t in common:
            b_def = before_catalog[t]
            a_def = after_catalog[t]
            b_fp = self.fingerprint_table(b_def)
            a_fp = self.fingerprint_table(a_def)
            if b_fp != a_fp:
                report.altered.append(t)
                diffs = []
                b_cols = {c["name"]: c for c in b_def.get("columns", [])}
                a_cols = {c["name"]: c for c in a_def.get("columns", [])}

                for col in set(a_cols) - set(b_cols):
                    diffs.append(f"added column {col}")
                    if not a_cols[col].get("nullable", True) and a_cols[col].get("default") is None:
                        report.needs_backfill.add(t)

                for col in set(b_cols) - set(a_cols):
                    diffs.append(f"dropped column {col}")

                for col in set(b_cols) & set(a_cols):
                    if b_cols[col] != a_cols[col]:
                        diffs.append(f"altered column {col}")
                        if str(b_cols[col].get("type")) != str(a_cols[col].get("type")):
                            report.needs_backfill.add(t)

                if b_def.get("partitioning") != a_def.get("partitioning"):
                    diffs.append("partition definition changed")
                    report.needs_reload.add(t)

                if b_def.get("indexes") != a_def.get("indexes"):
                    diffs.append("indexes altered")

                if b_def.get("foreign_keys") != a_def.get("foreign_keys"):
                    diffs.append("foreign keys altered")

                report.details[t] = diffs

        return report


def build_mysql_connect_kwargs(
    endpoint: Any,
    side: str = "src",
    read_timeout: Optional[int] = None,
    connect_timeout: int = 15,
    write_timeout: int = 3600,
    write_mode: bool = False,
    write_sql_mode: str = "NO_AUTO_VALUE_ON_ZERO,STRICT_ALL_TABLES",
) -> dict:
    host = getattr(endpoint, "host", "127.0.0.1")
    port = int(getattr(endpoint, "port", 3306))
    user = getattr(endpoint, "user", "root")
    password = getattr(endpoint, "password", "")

    tls_opts = {}
    if hasattr(endpoint, "mysql_tls") and callable(endpoint.mysql_tls):
        tls_opts = endpoint.mysql_tls()
    elif isinstance(getattr(endpoint, "tls", None), dict):
        tls_opts = endpoint.tls

    rt = int(os.environ.get("MIGKIT_READ_TIMEOUT", str(write_timeout)))
    effective_read_timeout = read_timeout if read_timeout is not None else 15

    init_parts = ["SET session time_zone = '+00:00'"]
    if write_mode and side == "dst":
        init_parts.append(f"sql_mode = '{write_sql_mode}'")
    init_cmd = ", ".join(init_parts)

    kwargs = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "charset": "utf8mb4",
        "connect_timeout": int(connect_timeout),
        "read_timeout": int(effective_read_timeout),
        "write_timeout": rt,
        "init_command": init_cmd,
    }
    kwargs.update(tls_opts)
    return kwargs


def unified_mysql_connect(
    endpoint: Any,
    side: str = "src",
    connect_fn: Optional[Callable] = None,
    read_timeout: Optional[int] = None,
    write_mode: bool = False,
) -> Any:
    kwargs = build_mysql_connect_kwargs(
        endpoint=endpoint,
        side=side,
        read_timeout=read_timeout,
        write_mode=write_mode,
    )
    if connect_fn is None:
        import pymysql
        connect_fn = pymysql.connect

    conn = connect_fn(**kwargs)
    sock = getattr(conn, "_sock", None)
    if sock is not None:
        try:
            import socket
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except (AttributeError, OSError):
            pass
    return conn


@dataclass
class ProbeAccumulator:
    """Accumulates probe results and unreadable metadata to prevent false OK verdicts."""
    probed_tables: Set[str] = field(default_factory=set)
    unreadable: List[Tuple[str, str, str]] = field(default_factory=list)

    def record_probed(self, table: str) -> None:
        self.probed_tables.add(table)

    def record_unreadable(self, table: str, item_type: str, reason: str) -> None:
        self.unreadable.append((table, item_type, reason))

    @property
    def has_unreadable(self) -> bool:
        return len(self.unreadable) > 0


@dataclass
class NeutralColumn:
    name: str
    neutral_type: str
    capacity: Optional[int] = None
    nullable: bool = True
    default: Optional[str] = None
    collation: Optional[str] = None


@dataclass
class NeutralTable:
    name: str
    columns: Dict[str, NeutralColumn]
    primary_key: Tuple[str, ...] = ()
    indexes: List[dict] = field(default_factory=list)
    foreign_keys: List[dict] = field(default_factory=list)


def normalize_type_to_neutral(raw_type: str) -> Tuple[str, Optional[int]]:
    t = raw_type.upper().strip()
    capacity = None
    if "(" in t:
        base, _, rest = t.partition("(")
        base = base.strip()
        args = rest.rstrip(")").strip()
        if args.isdigit():
            capacity = int(args)
        t = base

    type_mapping = {
        "INT": "INT",
        "INTEGER": "INT",
        "INT4": "INT",
        "MEDIUMINT": "INT",
        "BIGINT": "BIGINT",
        "INT8": "BIGINT",
        "SMALLINT": "SMALLINT",
        "INT2": "SMALLINT",
        "TINYINT": "TINYINT",
        "VARCHAR": "VARCHAR",
        "CHARACTER VARYING": "VARCHAR",
        "CHAR": "CHAR",
        "TEXT": "TEXT",
        "CLOB": "TEXT",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "DECIMAL",
        "FLOAT": "FLOAT",
        "DOUBLE": "DOUBLE",
        "DOUBLE PRECISION": "DOUBLE",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMPTZ": "TIMESTAMP_TZ",
        "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ",
        "DATETIME": "DATETIME",
        "DATE": "DATE",
        "TIME": "TIME",
        "BOOLEAN": "BOOLEAN",
        "BOOL": "BOOLEAN",
        "BLOB": "BLOB",
        "BYTEA": "BLOB",
        "BINARY": "BINARY",
        "VARBINARY": "VARBINARY",
        "JSON": "JSON",
        "JSONB": "JSON",
    }
    return (type_mapping.get(t, t), capacity)


@dataclass
class NeutralComparisonResult:
    compatible: bool = True
    differences: List[str] = field(default_factory=list)
    unreadable: List[Tuple[str, str, str]] = field(default_factory=list)


class NeutralShapeSchemaComparer:
    """Neutral shape schema comparer across heterogeneous engine pairs."""

    def compare(
        self,
        src_tables: Dict[str, NeutralTable],
        dst_tables: Dict[str, NeutralTable],
        probes: Optional[ProbeAccumulator] = None,
    ) -> NeutralComparisonResult:
        res = NeutralComparisonResult()
        if probes and probes.has_unreadable:
            res.compatible = False
            res.unreadable = list(probes.unreadable)
            for tbl, item, reason in probes.unreadable:
                res.differences.append(f"Cannot verify {tbl}.{item}: {reason}")

        for tbl_name, src_tbl in src_tables.items():
            if tbl_name not in dst_tables:
                res.compatible = False
                res.differences.append(f"Missing target table: {tbl_name}")
                continue

            dst_tbl = dst_tables[tbl_name]
            for col_name, src_col in src_tbl.columns.items():
                if col_name not in dst_tbl.columns:
                    res.compatible = False
                    res.differences.append(f"{tbl_name}: missing target column {col_name}")
                    continue

                dst_col = dst_tbl.columns[col_name]
                if src_col.neutral_type != dst_col.neutral_type:
                    res.compatible = False
                    res.differences.append(
                        f"{tbl_name}.{col_name}: type mismatch ({src_col.neutral_type} -> {dst_col.neutral_type})"
                    )

                if (
                    src_col.capacity is not None
                    and dst_col.capacity is not None
                    and dst_col.capacity < src_col.capacity
                ):
                    res.compatible = False
                    res.differences.append(
                        f"{tbl_name}.{col_name}: capacity narrowing ({src_col.capacity} -> {dst_col.capacity})"
                    )

                if src_col.nullable and not dst_col.nullable and dst_col.default is None:
                    res.compatible = False
                    res.differences.append(
                        f"{tbl_name}.{col_name}: target is NOT NULL without default, source is nullable"
                    )

            if src_tbl.primary_key != dst_tbl.primary_key:
                res.compatible = False
                res.differences.append(
                    f"{tbl_name}: primary key mismatch ({src_tbl.primary_key} != {dst_tbl.primary_key})"
                )

        return res


DVT_FULL_FEATURE_MATRIX = {
    "row_hash": "Row validation by SHA-256 hash",
    "custom_queries": "Custom query validation for views and derived datasets",
    "threshold": "Tolerance threshold for aggregates that drift",
    "grouped_columns": "Aggregates per group to bisect differing tables",
    "random_row_sampling": "Random row sampling to reduce scan cost on massive tables",
    "filter_status_fail": "Filter status fail to return only divergent rows",
    "yaml_batches": "Execution of table validations in YAML batches",
    "labels": "Label tagging for migration wave observability",
}


class DVTAuditHelper:
    """Audits DVT usage to ensure whole feature utilization and bisected row compare top-up."""

    @staticmethod
    def audit_dvt(configured_options: Set[str]) -> Dict[str, Tuple[str, str]]:
        result = {}
        for feature, desc in DVT_FULL_FEATURE_MATRIX.items():
            if feature in configured_options:
                result[feature] = ("used", f"Enabled in validation batch: {desc}")
            elif feature == "grouped_columns":
                result[feature] = ("topped up", "Bisects differing tables to group, then migkit row compare")
            else:
                result[feature] = ("unused", "Excluded by workload tier or not yet enabled")
        return result

    @staticmethod
    def bisect_and_row_compare(
        table: str,
        group_column: str,
        dvt_group_fn: Callable[[str, str], Dict[str, int]],
        migkit_row_fn: Callable[[str, str], bool],
    ) -> List[str]:
        differing_groups = []
        sums = dvt_group_fn(table, group_column)
        for grp, delta in sums.items():
            if delta != 0:
                differing_groups.append(grp)
                migkit_row_fn(table, grp)
        return differing_groups


REST_TOOL_AUDIT_REGISTRY = {
    "pg_dump": {
        "jobs": ("used", "-j parallel workers sized to vCPU"),
        "section": ("used", "--section=pre-data/data/post-data for staged creation"),
        "blobs": ("unused", "Measured hanging on large tables, streamed via native pipeline"),
    },
    "pgloader": {
        "prefetch_rows": ("used", "Set to 25000 for network saturation"),
        "workers": ("used", "Configured per numa node"),
        "cast_rules": ("topped up", "Migkit schema converter applies strict typing"),
    },
    "mongosync": {
        "build_indexes": ("used", "buildIndexes=true on completion"),
        "reversible": ("used", "reversible=true for two-way sync"),
        "clone": ("unused", "Migkit manages target collection initialization"),
    },
    "redis_shake": {
        "readers": ("used", "Parallel reader threads"),
        "filters": ("used", "Key prefix filtering"),
        "status_port": ("used", "Health and sync metrics polling"),
    },
    "mirrormaker2": {
        "offset_sync": ("used", "Offset translation between source and target clusters"),
        "checkpoint": ("used", "Regular consumer group checkpoint emission"),
        "exactly_once": ("used", "Idempotent producer transactions"),
    },
    "dsbulk": {
        "splits": ("used", "schema.splits for distributed read"),
        "checkpoint": ("used", "Checkpoint and replay on interruption"),
        "count_modes": ("used", "Exact count verification"),
    },
}


class RestToolAuditHelper:
    """Audits wrapped external tools to ensure whole usage with documented reasons and safe fallback."""

    @staticmethod
    def audit_tool(tool_name: str) -> Dict[str, Tuple[str, str]]:
        return REST_TOOL_AUDIT_REGISTRY.get(tool_name, {})

    @staticmethod
    def run_with_audit_fallback(
        tool_name: str,
        primary_tool_fn: Callable[[], Any],
        fallback_fn: Callable[[], Any],
        audit_log: Optional[List[dict]] = None,
    ) -> Any:
        try:
            res = primary_tool_fn()
            if audit_log is not None:
                audit_log.append({"tool": tool_name, "status": "primary_success"})
            return res
        except Exception as exc:
            if audit_log is not None:
                audit_log.append({
                    "tool": tool_name,
                    "status": "fallback_engaged",
                    "reason": str(exc),
                })
            return fallback_fn()
