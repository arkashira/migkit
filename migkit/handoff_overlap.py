import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from migkit.verifiers import gtid_is_subset, lsn_compare


class HandoffError(RuntimeError):
    pass


class HandoffGapError(HandoffError):
    pass


class HandoffFencingError(HandoffError):
    pass


class HandoffContractExpiredError(HandoffError):
    pass


class HandoffContractStateError(HandoffError):
    pass


def parse_position(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        val = val.strip()
        if val.isdigit():
            return int(val)
    return val


def compare_positions(a: Any, b: Any, engine: str = "generic") -> int:
    if a is None and b is None:
        return 0
    if a is None:
        return -1
    if b is None:
        return 1

    engine_norm = str(engine).lower()
    if engine_norm == "postgres" or (isinstance(a, str) and "/" in a) or (isinstance(b, str) and "/" in b):
        return lsn_compare(a, b)

    if engine_norm == "mysql" and isinstance(a, str) and ":" in a:
        if a == b:
            return 0
        a_sub = gtid_is_subset(a, b)
        b_sub = gtid_is_subset(b, a)
        if a_sub and not b_sub:
            return -1
        if b_sub and not a_sub:
            return 1
        return 0

    pa = parse_position(a)
    pb = parse_position(b)
    if isinstance(pa, (int, float)) and isinstance(pb, (int, float)):
        if pa < pb:
            return -1
        if pa > pb:
            return 1
        return 0

    sa, sb = str(a), str(b)
    if sa < sb:
        return -1
    if sa > sb:
        return 1
    return 0


@dataclass
class SnapshotLifetime:
    snapshot_id: str
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    expires_at: Optional[float] = None
    status: str = "pending"

    def is_valid(self, now: Optional[float] = None) -> bool:
        cur = time.time() if now is None else float(now)
        if self.expires_at is not None and cur > self.expires_at:
            return False
        return self.status in ("running", "completed")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "expires_at": self.expires_at,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SnapshotLifetime":
        return cls(
            snapshot_id=d["snapshot_id"],
            created_at=float(d.get("created_at", 0.0)),
            completed_at=d.get("completed_at"),
            expires_at=d.get("expires_at"),
            status=d.get("status", "pending"),
        )


@dataclass
class CaptureFences:
    lower_fence: Any
    upper_fence: Optional[Any] = None
    engine: str = "generic"

    def has_gap(self, stream_start: Any) -> bool:
        return compare_positions(stream_start, self.lower_fence, self.engine) > 0

    def is_in_overlap(self, pos: Any) -> bool:
        if compare_positions(pos, self.lower_fence, self.engine) < 0:
            return False
        if self.upper_fence is not None and compare_positions(pos, self.upper_fence, self.engine) > 0:
            return False
        return True

    def validate(self) -> None:
        if self.upper_fence is not None:
            if compare_positions(self.upper_fence, self.lower_fence, self.engine) < 0:
                raise HandoffFencingError(
                    f"upper fence {self.upper_fence} precedes lower fence {self.lower_fence}"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lower_fence": self.lower_fence,
            "upper_fence": self.upper_fence,
            "engine": self.engine,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CaptureFences":
        return cls(
            lower_fence=d["lower_fence"],
            upper_fence=d.get("upper_fence"),
            engine=d.get("engine", "generic"),
        )


@dataclass
class DedupIdentity:
    identity_keys: List[str] = field(default_factory=list)
    is_keyless: bool = False
    track_multiplicity: bool = True
    strategy: str = "commit_multiplicity"

    def compute_identity(self, row: Dict[str, Any]) -> str:
        if self.is_keyless or not self.identity_keys:
            serialized = json.dumps(row, sort_keys=True, default=str)
            return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        parts = [str(row.get(k)) for k in self.identity_keys]
        return ":".join(parts)

    def extract_key(self, row: Dict[str, Any]) -> Any:
        if self.is_keyless or not self.identity_keys:
            return self.compute_identity(row)
        if len(self.identity_keys) == 1:
            return row.get(self.identity_keys[0])
        return tuple(row.get(k) for k in self.identity_keys)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "identity_keys": list(self.identity_keys),
            "is_keyless": self.is_keyless,
            "track_multiplicity": self.track_multiplicity,
            "strategy": self.strategy,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DedupIdentity":
        return cls(
            identity_keys=list(d.get("identity_keys", [])),
            is_keyless=bool(d.get("is_keyless", False)),
            track_multiplicity=bool(d.get("track_multiplicity", True)),
            strategy=d.get("strategy", "commit_multiplicity"),
        )


@dataclass
class StreamEvent:
    position: Any
    commit_id: str
    op: str
    row: Dict[str, Any]
    multiplicity: int = 1
    table: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "position": self.position,
            "commit_id": self.commit_id,
            "op": self.op,
            "row": self.row,
            "multiplicity": self.multiplicity,
            "table": self.table,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "StreamEvent":
        return cls(
            position=d["position"],
            commit_id=d["commit_id"],
            op=d["op"],
            row=dict(d["row"]),
            multiplicity=int(d.get("multiplicity", 1)),
            table=d.get("table"),
        )


@dataclass
class HandoffContract:
    contract_id: str
    source_db: str
    table: str
    snapshot_lifetime: SnapshotLifetime
    fences: CaptureFences
    dedup_identity: DedupIdentity
    checkpoint_position: Optional[Any] = None
    applied_commits: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def validate(self, now: Optional[float] = None) -> None:
        if not self.snapshot_lifetime.is_valid(now=now):
            raise HandoffContractExpiredError(
                f"snapshot {self.snapshot_lifetime.snapshot_id} is expired or invalid"
            )
        self.fences.validate()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "contract_id": self.contract_id,
            "source_db": self.source_db,
            "table": self.table,
            "snapshot_lifetime": self.snapshot_lifetime.to_dict(),
            "fences": self.fences.to_dict(),
            "dedup_identity": self.dedup_identity.to_dict(),
            "checkpoint_position": self.checkpoint_position,
            "applied_commits": sorted(list(self.applied_commits)),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "HandoffContract":
        return cls(
            contract_id=d["contract_id"],
            source_db=d["source_db"],
            table=d["table"],
            snapshot_lifetime=SnapshotLifetime.from_dict(d["snapshot_lifetime"]),
            fences=CaptureFences.from_dict(d["fences"]),
            dedup_identity=DedupIdentity.from_dict(d["dedup_identity"]),
            checkpoint_position=d.get("checkpoint_position"),
            applied_commits=set(d.get("applied_commits", [])),
            metadata=dict(d.get("metadata", {})),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)

    @classmethod
    def from_json(cls, s: str) -> "HandoffContract":
        return cls.from_dict(json.loads(s))

    def save(self, path: Union[str, os.PathLike]) -> None:
        path_str = str(path)
        dir_name = os.path.dirname(os.path.abspath(path_str))
        os.makedirs(dir_name, exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=dir_name, prefix="handoff_", suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                f.write(self.to_json())
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path_str)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    @classmethod
    def load(cls, path: Union[str, os.PathLike]) -> "HandoffContract":
        with open(path, "r", encoding="utf-8") as f:
            return cls.from_json(f.read())


class HandoffManager:
    def __init__(self, contract: HandoffContract, state_path: Optional[str] = None):
        self.contract = contract
        self.state_path = state_path
        self.target_rows: Dict[Any, Dict[str, Any]] = {}
        self.target_multiplicities: Dict[str, int] = {}
        self.observed_intervals: List[Tuple[Any, Any]] = []

    @classmethod
    def create(
        cls,
        contract_id: str,
        source_db: str,
        table: str,
        lower_fence: Any,
        engine: str = "generic",
        identity_keys: Optional[List[str]] = None,
        is_keyless: bool = False,
        expires_in: Optional[float] = None,
        state_path: Optional[str] = None,
    ) -> "HandoffManager":
        now = time.time()
        expires_at = now + expires_in if expires_in is not None else None
        lifetime = SnapshotLifetime(
            snapshot_id=f"snap_{contract_id}",
            created_at=now,
            status="running",
            expires_at=expires_at,
        )
        fences = CaptureFences(
            lower_fence=lower_fence,
            upper_fence=None,
            engine=engine,
        )
        dedup = DedupIdentity(
            identity_keys=identity_keys or [],
            is_keyless=is_keyless,
            track_multiplicity=True,
            strategy="commit_multiplicity",
        )
        contract = HandoffContract(
            contract_id=contract_id,
            source_db=source_db,
            table=table,
            snapshot_lifetime=lifetime,
            fences=fences,
            dedup_identity=dedup,
        )
        mgr = cls(contract=contract, state_path=state_path)
        if state_path:
            mgr.save_checkpoint()
        return mgr

    def complete_snapshot(self, upper_fence: Any) -> None:
        self.contract.fences.upper_fence = upper_fence
        self.contract.fences.validate()
        self.contract.snapshot_lifetime.status = "completed"
        self.contract.snapshot_lifetime.completed_at = time.time()
        if self.state_path:
            self.save_checkpoint()

    def verify_stream_handoff(self, stream_start: Any) -> None:
        if self.contract.fences.has_gap(stream_start):
            raise HandoffGapError(
                f"copy-to-follow gap detected: stream_start {stream_start} > lower_fence {self.contract.fences.lower_fence}"
            )

    def apply_snapshot_record(self, row: Dict[str, Any], commit_id: Optional[str] = None) -> None:
        row_id = self.contract.dedup_identity.compute_identity(row)
        key = self.contract.dedup_identity.extract_key(row)
        self.target_rows[key] = dict(row)
        self.target_multiplicities[row_id] = self.target_multiplicities.get(row_id, 0) + 1
        if commit_id:
            self.contract.applied_commits.add(commit_id)

    def apply_stream_event(self, event: StreamEvent) -> bool:
        if event.commit_id in self.contract.applied_commits:
            return False

        if compare_positions(event.position, self.contract.fences.lower_fence, self.contract.fences.engine) < 0:
            return False

        row_id = self.contract.dedup_identity.compute_identity(event.row)
        key = self.contract.dedup_identity.extract_key(event.row)
        op_norm = event.op.upper()

        if op_norm == "INSERT":
            self.target_rows[key] = dict(event.row)
            self.target_multiplicities[row_id] = self.target_multiplicities.get(row_id, 0) + event.multiplicity
        elif op_norm == "UPDATE":
            self.target_rows[key] = dict(event.row)
        elif op_norm == "DELETE":
            if key in self.target_rows:
                self.target_rows.pop(key, None)
            cur_mult = self.target_multiplicities.get(row_id, 0)
            new_mult = max(0, cur_mult - event.multiplicity)
            if new_mult == 0:
                self.target_multiplicities.pop(row_id, None)
            else:
                self.target_multiplicities[row_id] = new_mult

        self.contract.applied_commits.add(event.commit_id)
        if self.contract.fences.upper_fence is not None and compare_positions(
            event.position, self.contract.fences.upper_fence, self.contract.fences.engine
        ) >= 0:
            self.contract.checkpoint_position = event.position
        return True

    def save_checkpoint(self, position: Optional[Any] = None) -> None:
        if not self.state_path:
            return
        if position is not None:
            self.contract.checkpoint_position = position
        state_dict = {
            "contract": self.contract.to_dict(),
            "target_rows": list(self.target_rows.values()),
            "target_multiplicities": self.target_multiplicities,
            "observed_intervals": self.observed_intervals,
        }
        dir_name = os.path.dirname(os.path.abspath(self.state_path))
        os.makedirs(dir_name, exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=dir_name, prefix="handoff_mgr_", suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                f.write(json.dumps(state_dict, indent=2, sort_keys=True))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.state_path)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    @classmethod
    def restore(cls, state_path: str) -> "HandoffManager":
        with open(state_path, "r", encoding="utf-8") as f:
            state = json.loads(f.read())
        contract = HandoffContract.from_dict(state["contract"])
        mgr = cls(contract=contract, state_path=state_path)
        for row in state.get("target_rows", []):
            key = contract.dedup_identity.extract_key(row)
            mgr.target_rows[key] = row
        mgr.target_multiplicities = dict(state.get("target_multiplicities", {}))
        mgr.observed_intervals = [tuple(x) for x in state.get("observed_intervals", [])]
        return mgr

    def simulate_crash(self) -> None:
        pass


@dataclass
class HandoffAuditResult:
    status: str
    certified_gap_free: bool
    total_source_commits: int
    applied_source_commits: int
    missing_commits: List[str]
    dropped_intervals: List[Tuple[Any, Any]]
    multiplicity_divergences: Dict[str, Tuple[int, int]]
    defect_class: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


def compare_source_and_target_history(
    source_commits: List[StreamEvent], manager: HandoffManager
) -> HandoffAuditResult:
    applied_set = manager.contract.applied_commits
    missing = []
    dropped_intervals = []
    prev_pos = None

    for ev in source_commits:
        if ev.commit_id not in applied_set:
            missing.append(ev.commit_id)
            if prev_pos is not None:
                dropped_intervals.append((prev_pos, ev.position))
        prev_pos = ev.position

    expected_multiplicities: Dict[str, int] = {}
    for ev in source_commits:
        row_id = manager.contract.dedup_identity.compute_identity(ev.row)
        op_norm = ev.op.upper()
        if op_norm == "INSERT":
            expected_multiplicities[row_id] = expected_multiplicities.get(row_id, 0) + ev.multiplicity
        elif op_norm == "DELETE":
            expected_multiplicities[row_id] = max(0, expected_multiplicities.get(row_id, 0) - ev.multiplicity)

    divergences: Dict[str, Tuple[int, int]] = {}
    all_keys = set(expected_multiplicities.keys()) | set(manager.target_multiplicities.keys())
    for k in all_keys:
        exp = expected_multiplicities.get(k, 0)
        act = manager.target_multiplicities.get(k, 0)
        if exp != act:
            divergences[k] = (exp, act)

    if missing or dropped_intervals:
        status = "defect"
        certified = False
        defect_class = "copy_to_follow_gap" if dropped_intervals else "missing_commits"
    elif divergences:
        status = "defect"
        certified = False
        defect_class = "multiplicity_divergence"
    else:
        status = "ok"
        certified = True
        defect_class = None

    return HandoffAuditResult(
        status=status,
        certified_gap_free=certified,
        total_source_commits=len(source_commits),
        applied_source_commits=len(source_commits) - len(missing),
        missing_commits=missing,
        dropped_intervals=dropped_intervals,
        multiplicity_divergences=divergences,
        defect_class=defect_class,
    )
