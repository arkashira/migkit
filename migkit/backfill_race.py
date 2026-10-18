from dataclasses import dataclass, field
import enum
import json
from pathlib import Path
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from . import decide, durable


def backfill_coverage_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for column backfill coverage preceding schema discovery (Row 657: NEED09)."""
    return (
        decide.Rung(
            "selective_key_range_backfill",
            "perform selective backfill on affected key ranges without full table resnapshot",
            needs=(
                decide.Need(
                    "backfill volume is narrow",
                    lambda f: bool(f.get("narrow_gap", True)),
                    "backfill volume exceeds selective thresholds",
                ),
                decide.Need(
                    "candidate keys or range are available",
                    lambda f: bool(f.get("keys_available", True)),
                    "key ranges cannot be enumerated",
                ),
                decide.Need(
                    "concurrent lock pressure is manageable",
                    lambda f: bool(f.get("low_lock_pressure", True)),
                    "lock contention is prohibitive",
                ),
            ),
            footprint=0,
            leaves="selective key range backfill without re-snapshotting table",
        ),
        decide.Rung(
            "shadow_table_resnapshot",
            "stage shadow copy and swap atomically when backfill is dense or broad",
            needs=(
                decide.Need(
                    "broad or dense backfill volume detected",
                    lambda f: bool(f.get("broad_or_dense_backfill", False)),
                    "backfill is not broad enough to warrant full resnapshot",
                ),
                decide.Need(
                    "shadow storage budget is available",
                    lambda f: bool(f.get("shadow_storage_available", True)),
                    "insufficient storage budget for shadow table",
                ),
            ),
            footprint=1,
            leaves="shadow resnapshot with atomic cutover swap",
        ),
        decide.Rung(
            "dual_read_reconciliation",
            "reconcile historical coverage via dual-read schema epoch verification",
            needs=(
                decide.Need(
                    "epoch discrepancy is resolvable via dual read",
                    lambda f: bool(f.get("epoch_discrepancy_resolvable", False)),
                    "dual read reconciliation not viable",
                ),
            ),
            footprint=2,
            leaves="dual read schema epoch reconciliation",
        ),
        decide.Rung(
            "drain_and_reprobe_schema",
            "quiesce CDC apply and reprobe catalog schema from database dictionary",
            needs=(
                decide.Need(
                    "schema reprobe allowed by pipeline policy",
                    lambda f: bool(f.get("schema_reprobe_allowed", False)),
                    "schema reprobing disallowed",
                ),
            ),
            footprint=3,
            leaves="drain CDC and reprobe catalog schema",
        ),
        decide.Rung(
            "refuse_backfill_race",
            "refuse migration to prevent applying corrupted schema epoch updates",
            footprint=4,
            leaves="refused backfill execution due to unresolvable schema race",
        ),
    )


def decide_backfill_coverage(facts: Dict[str, Any]) -> decide.Choice:
    """Climb the backfill_coverage_rungs ladder."""
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("backfill coverage strategy", backfill_coverage_rungs(), fact_obj)


class CoverageStrategy(str, enum.Enum):
    SELECTIVE_BACKFILL = "selective_backfill"
    SHADOW_RESNAPSHOT = "shadow_resnapshot"


class ObligationStatus(str, enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SATISFIED = "satisfied"
    FAILED = "failed"


def normalize_watermark(val: Any) -> Any:
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        s = val.strip()
        if "/" in s:
            hi, _, lo = s.partition("/")
            try:
                return (int(hi, 16) << 32) | int(lo, 16)
            except ValueError:
                pass
        if s.isdigit():
            return int(s)
        try:
            return float(s)
        except ValueError:
            return s
    return str(val)


def compare_watermarks(w1: Any, w2: Any) -> int:
    n1 = normalize_watermark(w1)
    n2 = normalize_watermark(w2)
    if isinstance(n1, (int, float)) and isinstance(n2, (int, float)):
        if n1 < n2:
            return -1
        if n1 > n2:
            return 1
        return 0
    s1, s2 = str(w1), str(w2)
    if s1 < s2:
        return -1
    if s1 > s2:
        return 1
    return 0


@dataclass
class CDCEvent:
    event_id: str
    relation: str
    op: str
    key: Any
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    watermark: Optional[Any] = None
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "relation": self.relation,
            "op": self.op,
            "key": self.key,
            "before": self.before,
            "after": self.after,
            "watermark": self.watermark,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CDCEvent":
        return cls(
            event_id=d["event_id"],
            relation=d["relation"],
            op=d["op"],
            key=d["key"],
            before=d.get("before"),
            after=d.get("after"),
            watermark=d.get("watermark"),
            timestamp=d.get("timestamp", 0.0),
            metadata=d.get("metadata", {}),
        )


@dataclass
class SchemaEpoch:
    relation: str
    epoch: int
    columns: List[str]
    watermark: Optional[Any] = None
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "relation": self.relation,
            "epoch": self.epoch,
            "columns": list(self.columns),
            "watermark": self.watermark,
            "created_at": self.created_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SchemaEpoch":
        return cls(
            relation=d["relation"],
            epoch=d["epoch"],
            columns=list(d["columns"]),
            watermark=d.get("watermark"),
            created_at=d.get("created_at", 0.0),
            metadata=d.get("metadata", {}),
        )


@dataclass
class ObservedGap:
    gap_id: str
    relation: str
    from_epoch: int
    to_epoch: int
    missing_columns: List[str]
    start_watermark: Optional[Any] = None
    end_watermark: Optional[Any] = None
    observed_at: float = field(default_factory=time.time)
    details: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "gap_id": self.gap_id,
            "relation": self.relation,
            "from_epoch": self.from_epoch,
            "to_epoch": self.to_epoch,
            "missing_columns": list(self.missing_columns),
            "start_watermark": self.start_watermark,
            "end_watermark": self.end_watermark,
            "observed_at": self.observed_at,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ObservedGap":
        return cls(
            gap_id=d["gap_id"],
            relation=d["relation"],
            from_epoch=d["from_epoch"],
            to_epoch=d["to_epoch"],
            missing_columns=list(d["missing_columns"]),
            start_watermark=d.get("start_watermark"),
            end_watermark=d.get("end_watermark"),
            observed_at=d.get("observed_at", 0.0),
            details=d.get("details"),
        )


@dataclass
class HistoricalCoverageObligation:
    obligation_id: str
    relation: str
    columns: List[str]
    epoch: int
    from_epoch: int
    strategy: CoverageStrategy = CoverageStrategy.SELECTIVE_BACKFILL
    status: ObligationStatus = ObligationStatus.PENDING
    key_range: Optional[Tuple[Any, Any]] = None
    target_keys: Optional[List[Any]] = None
    covered_keys: List[Any] = field(default_factory=list)
    rows_covered: int = 0
    watermark_low: Optional[Any] = None
    watermark_high: Optional[Any] = None
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_satisfied(self) -> bool:
        return self.status == ObligationStatus.SATISFIED

    def mark_satisfied(self, rows_covered: Optional[int] = None) -> None:
        self.status = ObligationStatus.SATISFIED
        self.completed_at = time.time()
        if rows_covered is not None:
            self.rows_covered = rows_covered

    def to_dict(self) -> Dict[str, Any]:
        return {
            "obligation_id": self.obligation_id,
            "relation": self.relation,
            "columns": list(self.columns),
            "epoch": self.epoch,
            "from_epoch": self.from_epoch,
            "strategy": str(self.strategy.value if hasattr(self.strategy, "value") else self.strategy),
            "status": str(self.status.value if hasattr(self.status, "value") else self.status),
            "key_range": list(self.key_range) if self.key_range is not None else None,
            "target_keys": list(self.target_keys) if self.target_keys is not None else None,
            "covered_keys": list(self.covered_keys),
            "rows_covered": self.rows_covered,
            "watermark_low": self.watermark_low,
            "watermark_high": self.watermark_high,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "HistoricalCoverageObligation":
        kr = d.get("key_range")
        key_range = tuple(kr) if kr is not None else None
        return cls(
            obligation_id=d["obligation_id"],
            relation=d["relation"],
            columns=list(d["columns"]),
            epoch=d["epoch"],
            from_epoch=d["from_epoch"],
            strategy=CoverageStrategy(d.get("strategy", CoverageStrategy.SELECTIVE_BACKFILL)),
            status=ObligationStatus(d.get("status", ObligationStatus.PENDING)),
            key_range=key_range,
            target_keys=d.get("target_keys"),
            covered_keys=list(d.get("covered_keys", [])),
            rows_covered=d.get("rows_covered", 0),
            watermark_low=d.get("watermark_low"),
            watermark_high=d.get("watermark_high"),
            created_at=d.get("created_at", 0.0),
            completed_at=d.get("completed_at"),
            metadata=d.get("metadata", {}),
        )


@dataclass
class ReconciliationResult:
    relation: str
    strategy_used: CoverageStrategy
    success: bool
    rows_backfilled: int = 0
    events_reconciled: int = 0
    duplicate_updates_prevented: int = 0
    error: Optional[str] = None


def choose_coverage_strategy(
    relation: str,
    obligation: HistoricalCoverageObligation,
    table_stats: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> CoverageStrategy:
    if config and "strategy" in config:
        return CoverageStrategy(config["strategy"])
    total = table_stats.get("total_rows", 0) if table_stats else 0
    affected = table_stats.get("affected_rows", 0) if table_stats else 0
    ratio = (affected / float(total)) if total > 0 else 0.0
    broad = (ratio > 0.25 and affected > 1000) or (total == 0 and affected > 1000)
    facts = {
        "narrow_gap": not broad,
        "keys_available": bool(obligation.target_keys or obligation.key_range or not broad),
        "low_lock_pressure": True,
        "broad_or_dense_backfill": broad,
        "shadow_storage_available": True,
        "epoch_discrepancy_resolvable": False,
        "schema_reprobe_allowed": False,
    }
    choice = decide_backfill_coverage(facts)
    if choice.rung.name == "shadow_table_resnapshot":
        return CoverageStrategy.SHADOW_RESNAPSHOT
    return CoverageStrategy.SELECTIVE_BACKFILL


class SchemaEpochTracker:
    def __init__(self) -> None:
        self.epochs: Dict[str, List[SchemaEpoch]] = {}
        self.gaps: Dict[str, List[ObservedGap]] = {}
        self.obligations: Dict[str, List[HistoricalCoverageObligation]] = {}
        self._seq = 0

    def _next_id(self, prefix: str) -> str:
        self._seq += 1
        return f"{prefix}-{int(time.time())}-{self._seq}"

    def register_schema(
        self,
        relation: str,
        columns: Union[Sequence[str], Dict[str, Any]],
        watermark: Optional[Any] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SchemaEpoch:
        col_list = list(columns.keys()) if isinstance(columns, dict) else list(columns)
        history = self.epochs.setdefault(relation, [])

        if not history:
            ep = SchemaEpoch(
                relation=relation,
                epoch=1,
                columns=col_list,
                watermark=watermark,
                metadata=metadata or {},
            )
            history.append(ep)
            return ep

        prev = history[-1]
        new_cols = [c for c in col_list if c not in prev.columns]
        ep_num = prev.epoch + 1
        ep = SchemaEpoch(
            relation=relation,
            epoch=ep_num,
            columns=col_list,
            watermark=watermark,
            metadata=metadata or {},
        )
        history.append(ep)

        if new_cols:
            ob = HistoricalCoverageObligation(
                obligation_id=self._next_id("ob"),
                relation=relation,
                columns=new_cols,
                epoch=ep_num,
                from_epoch=prev.epoch,
                watermark_low=prev.watermark,
                watermark_high=watermark,
                status=ObligationStatus.PENDING,
            )
            self.obligations.setdefault(relation, []).append(ob)

            gap = ObservedGap(
                gap_id=self._next_id("gap"),
                relation=relation,
                from_epoch=prev.epoch,
                to_epoch=ep_num,
                missing_columns=new_cols,
                start_watermark=prev.watermark,
                end_watermark=watermark,
                details=f"accepted columns {new_cols} at epoch {ep_num}",
            )
            self.gaps.setdefault(relation, []).append(gap)

        return ep

    def observe_event(self, event: CDCEvent) -> Optional[ObservedGap]:
        curr = self.get_current_epoch(event.relation)
        if not curr:
            return None

        event_cols: Set[str] = set()
        if event.after and isinstance(event.after, dict):
            event_cols.update(event.after.keys())
        if event.before and isinstance(event.before, dict):
            event_cols.update(event.before.keys())

        missing = [c for c in event_cols if c not in curr.columns]
        if missing:
            gap = ObservedGap(
                gap_id=self._next_id("gap"),
                relation=event.relation,
                from_epoch=curr.epoch,
                to_epoch=curr.epoch + 1,
                missing_columns=missing,
                start_watermark=curr.watermark,
                end_watermark=event.watermark,
                details=f"unregistered columns {missing} in stream before discovery",
            )
            self.gaps.setdefault(event.relation, []).append(gap)
            return gap
        return None

    def record_gap(
        self,
        relation: str,
        from_epoch: int,
        to_epoch: int,
        missing_columns: Sequence[str],
        start_watermark: Optional[Any] = None,
        end_watermark: Optional[Any] = None,
        details: Optional[str] = None,
    ) -> ObservedGap:
        gap = ObservedGap(
            gap_id=self._next_id("gap"),
            relation=relation,
            from_epoch=from_epoch,
            to_epoch=to_epoch,
            missing_columns=list(missing_columns),
            start_watermark=start_watermark,
            end_watermark=end_watermark,
            details=details,
        )
        self.gaps.setdefault(relation, []).append(gap)
        return gap

    def get_current_epoch(self, relation: str) -> Optional[SchemaEpoch]:
        history = self.epochs.get(relation)
        return history[-1] if history else None

    def get_epoch_history(self, relation: str) -> List[SchemaEpoch]:
        return list(self.epochs.get(relation, []))

    def get_gaps(self, relation: Optional[str] = None) -> List[ObservedGap]:
        if relation:
            return list(self.gaps.get(relation, []))
        out: List[ObservedGap] = []
        for g_list in self.gaps.values():
            out.extend(g_list)
        return out

    def get_obligations(
        self,
        relation: Optional[str] = None,
        status: Optional[Union[str, ObligationStatus]] = None,
    ) -> List[HistoricalCoverageObligation]:
        if relation:
            candidates = self.obligations.get(relation, [])
        else:
            candidates = [ob for o_list in self.obligations.values() for ob in o_list]

        if status is not None:
            s_val = status.value if hasattr(status, "value") else str(status)
            return [ob for ob in candidates if (ob.status.value if hasattr(ob.status, "value") else ob.status) == s_val]
        return list(candidates)

    def get_pending_obligations(self, relation: Optional[str] = None) -> List[HistoricalCoverageObligation]:
        return self.get_obligations(relation=relation, status=ObligationStatus.PENDING)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "seq": self._seq,
            "epochs": {k: [ep.to_dict() for ep in eps] for k, eps in self.epochs.items()},
            "gaps": {k: [g.to_dict() for g in gs] for k, gs in self.gaps.items()},
            "obligations": {k: [ob.to_dict() for ob in obs] for k, obs in self.obligations.items()},
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SchemaEpochTracker":
        tracker = cls()
        tracker._seq = d.get("seq", 0)
        for k, eps in d.get("epochs", {}).items():
            tracker.epochs[k] = [SchemaEpoch.from_dict(ep) for ep in eps]
        for k, gs in d.get("gaps", {}).items():
            tracker.gaps[k] = [ObservedGap.from_dict(g) for g in gs]
        for k, obs in d.get("obligations", {}).items():
            tracker.obligations[k] = [HistoricalCoverageObligation.from_dict(ob) for ob in obs]
        return tracker

    def save(self, path: Union[str, Path]) -> None:
        target = Path(path)
        payload = json.dumps(self.to_dict(), indent=2)
        durable.write_atomic(target, payload)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "SchemaEpochTracker":
        target = Path(path)
        with open(target, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)


class BackfillRaceReconciler:
    def __init__(
        self,
        tracker: SchemaEpochTracker,
        target_store: Optional[Dict[str, Dict[Any, Dict[str, Any]]]] = None,
        state_path: Optional[Union[str, Path]] = None,
    ) -> None:
        self.tracker = tracker
        self.target_store = target_store if target_store is not None else {}
        self.state_path = Path(state_path) if state_path else None
        self.buffered_events: Dict[str, List[CDCEvent]] = {}
        self.applied_event_ids: Set[str] = set()
        self.relation_reload_counts: Dict[str, int] = {}

    def buffer_event(self, event: CDCEvent) -> None:
        self.tracker.observe_event(event)
        self.buffered_events.setdefault(event.relation, []).append(event)

    def get_buffered_events(self, relation: str) -> List[CDCEvent]:
        return list(self.buffered_events.get(relation, []))

    def apply_event(self, event: CDCEvent) -> bool:
        if event.event_id in self.applied_event_ids:
            return False

        table = self.target_store.setdefault(event.relation, {})
        op = event.op.upper()
        key = event.key

        if op == "DELETE":
            if key in table:
                del table[key]
                self.applied_event_ids.add(event.event_id)
                return True
            self.applied_event_ids.add(event.event_id)
            return False

        new_data = event.after or {}
        curr = table.get(key)

        if curr is not None:
            is_identical = True
            for k, v in new_data.items():
                if curr.get(k) != v:
                    is_identical = False
                    break
            if is_identical and len(new_data) <= len(curr):
                self.applied_event_ids.add(event.event_id)
                return False
            updated = dict(curr)
            updated.update(new_data)
            table[key] = updated
        else:
            table[key] = dict(new_data)

        self.applied_event_ids.add(event.event_id)
        return True

    def reconcile_relation(
        self,
        relation: str,
        source_fetcher: Optional[Callable[[str, List[Any]], Dict[Any, Dict[str, Any]]]] = None,
        source_snapshot_reader: Optional[Callable[[str], Dict[Any, Dict[str, Any]]]] = None,
        snapshot_watermark: Optional[Any] = None,
        table_stats: Optional[Dict[str, Any]] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> ReconciliationResult:
        self.relation_reload_counts[relation] = self.relation_reload_counts.get(relation, 0) + 1

        pending_obs = self.tracker.get_pending_obligations(relation)
        if not pending_obs:
            buffered = self.buffered_events.get(relation, [])
            rec_cnt = 0
            dup_cnt = 0
            for ev in list(buffered):
                if self.apply_event(ev):
                    rec_cnt += 1
                else:
                    dup_cnt += 1
            self.buffered_events[relation] = []
            return ReconciliationResult(
                relation=relation,
                strategy_used=CoverageStrategy.SELECTIVE_BACKFILL,
                success=True,
                rows_backfilled=0,
                events_reconciled=rec_cnt,
                duplicate_updates_prevented=dup_cnt,
            )

        obligation = pending_obs[0]
        strategy = obligation.strategy
        if strategy == CoverageStrategy.SELECTIVE_BACKFILL and (table_stats or config):
            strategy = choose_coverage_strategy(relation, obligation, table_stats=table_stats, config=config)
            obligation.strategy = strategy

        obligation.status = ObligationStatus.IN_PROGRESS
        rows_backfilled = 0
        events_reconciled = 0
        duplicates_prevented = 0

        target_table = self.target_store.setdefault(relation, {})

        if strategy == CoverageStrategy.SELECTIVE_BACKFILL:
            keys_to_fetch: Set[Any] = set()
            if obligation.target_keys:
                keys_to_fetch.update(obligation.target_keys)
            else:
                for k, row in target_table.items():
                    for col in obligation.columns:
                        if col not in row:
                            keys_to_fetch.add(k)
                            break
                for ev in self.buffered_events.get(relation, []):
                    keys_to_fetch.add(ev.key)

            if source_fetcher and keys_to_fetch:
                fetched_data = source_fetcher(relation, list(keys_to_fetch))
                for k, src_row in fetched_data.items():
                    curr_row = target_table.setdefault(k, {})
                    updated = False
                    for col in obligation.columns:
                        if col in src_row:
                            curr_row[col] = src_row[col]
                            updated = True
                    if updated:
                        rows_backfilled += 1
                        obligation.covered_keys.append(k)

        elif strategy == CoverageStrategy.SHADOW_RESNAPSHOT:
            if source_snapshot_reader:
                shadow_data = source_snapshot_reader(relation)
                for k, shadow_row in shadow_data.items():
                    curr_row = target_table.setdefault(k, {})
                    for col in obligation.columns:
                        if col in shadow_row:
                            curr_row[col] = shadow_row[col]
                    for ck, cv in shadow_row.items():
                        if ck not in curr_row:
                            curr_row[ck] = cv
                    rows_backfilled += 1
                    obligation.covered_keys.append(k)

        buffered = self.buffered_events.get(relation, [])
        buffered.sort(key=lambda e: (normalize_watermark(e.watermark) if isinstance(normalize_watermark(e.watermark), (int, float)) else 0, e.timestamp))

        for ev in list(buffered):
            if snapshot_watermark is not None and ev.watermark is not None:
                if compare_watermarks(ev.watermark, snapshot_watermark) <= 0:
                    curr = target_table.get(ev.key)
                    if curr is not None and ev.after:
                        matches = all(curr.get(k) == v for k, v in ev.after.items())
                        if matches:
                            duplicates_prevented += 1
                            self.applied_event_ids.add(ev.event_id)
                            continue

            applied = self.apply_event(ev)
            if applied:
                events_reconciled += 1
            else:
                duplicates_prevented += 1

        self.buffered_events[relation] = []
        obligation.mark_satisfied(rows_covered=rows_backfilled)

        if self.state_path:
            self.save()

        return ReconciliationResult(
            relation=relation,
            strategy_used=strategy,
            success=True,
            rows_backfilled=rows_backfilled,
            events_reconciled=events_reconciled,
            duplicate_updates_prevented=duplicates_prevented,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tracker": self.tracker.to_dict(),
            "buffered_events": {
                k: [ev.to_dict() for ev in evs] for k, evs in self.buffered_events.items()
            },
            "applied_event_ids": list(self.applied_event_ids),
            "relation_reload_counts": dict(self.relation_reload_counts),
        }

    @classmethod
    def from_dict(
        cls,
        d: Dict[str, Any],
        target_store: Optional[Dict[str, Dict[Any, Dict[str, Any]]]] = None,
        state_path: Optional[Union[str, Path]] = None,
    ) -> "BackfillRaceReconciler":
        tracker = SchemaEpochTracker.from_dict(d.get("tracker", {}))
        reconciler = cls(tracker=tracker, target_store=target_store, state_path=state_path)
        for k, evs in d.get("buffered_events", {}).items():
            reconciler.buffered_events[k] = [CDCEvent.from_dict(ev) for ev in evs]
        reconciler.applied_event_ids = set(d.get("applied_event_ids", []))
        reconciler.relation_reload_counts = dict(d.get("relation_reload_counts", {}))
        return reconciler

    def save(self, state_path: Optional[Union[str, Path]] = None) -> None:
        target = Path(state_path) if state_path else self.state_path
        if not target:
            raise ValueError("No state path provided for save")
        payload = json.dumps(self.to_dict(), indent=2)
        durable.write_atomic(target, payload)

    @classmethod
    def load(
        cls,
        state_path: Union[str, Path],
        target_store: Optional[Dict[str, Dict[Any, Dict[str, Any]]]] = None,
    ) -> "BackfillRaceReconciler":
        target = Path(state_path)
        with open(target, "r") as f:
            data = json.load(f)
        return cls.from_dict(data, target_store=target_store, state_path=target)
