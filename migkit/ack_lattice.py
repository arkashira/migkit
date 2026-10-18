from dataclasses import dataclass
import enum
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from . import decide
from . import durable


def ack_lattice_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for ACK position lattice progression and cutover authorization (Row 652: NEED04)."""
    return (
        decide.Rung(
            "authorize_cutover_fully_proved",
            "authorize cutover when applied and proved positions meet or exceed required drain position",
            needs=(
                decide.Need(
                    "target applied position meets required cutover watermark",
                    lambda f: bool(f.get("applied_meets_required", True)),
                    "target committed-prefix has not reached required cutover watermark",
                ),
                decide.Need(
                    "target proved position meets required cutover watermark",
                    lambda f: bool(f.get("proved_meets_required", True)),
                    "target proved watermark has not reached required cutover position",
                ),
                decide.Need(
                    "source ack is safely bounded by durably retained position",
                    lambda f: bool(f.get("source_ack_bounded_by_retained", True)),
                    "source ack exceeds durably retained stream position",
                ),
            ),
            footprint=0,
            leaves="authorized cutover with verified applied and proved prefix",
        ),
        decide.Rung(
            "allow_pipeline_drain_progression",
            "allow pipeline stream to continue advancing positions toward cutover requirement",
            needs=(
                decide.Need(
                    "positions advance monotonically without regression",
                    lambda f: bool(f.get("positions_monotonic", True)),
                    "pipeline positions show non-monotonic regression",
                ),
                decide.Need(
                    "source ack is safely bounded by durably retained position",
                    lambda f: bool(f.get("source_ack_bounded_by_retained", True)),
                    "source ack exceeds durably retained stream position",
                ),
            ),
            footprint=1,
            leaves="forward pipeline drain progression permitted",
        ),
        decide.Rung(
            "refuse_cutover_unproved_or_unapplied",
            "refuse cutover when applied or proved watermark has not reached required stream position",
            footprint=2,
            leaves="cutover blocked until applied and proved positions catch up",
        ),
    )


def decide_ack_lattice(facts: Dict[str, Any]) -> decide.Choice:
    """Climb the ack_lattice_rungs ladder."""
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("ack position lattice cutover authorization", ack_lattice_rungs(), fact_obj)


class PipelineStage(enum.IntEnum):
    READ = 10
    DURABLY_RETAINED = 20
    APPLIED = 30
    PROVED = 40

    @classmethod
    def from_string(cls, val: str) -> "PipelineStage":
        key = str(val).strip().upper()
        if hasattr(cls, key):
            return getattr(cls, key)
        raise ValueError(f"unknown pipeline stage: {val}")


READ = PipelineStage.READ
DURABLY_RETAINED = PipelineStage.DURABLY_RETAINED
APPLIED = PipelineStage.APPLIED
PROVED = PipelineStage.PROVED


class MonotoneRegressionError(ValueError):
    pass


class SourceAckRefusedError(RuntimeError):
    pass


class CutoverRefusedError(RuntimeError):
    pass


def normalize_position(pos: Any) -> int:
    if pos is None:
        return 0
    if isinstance(pos, int):
        return pos
    if isinstance(pos, str):
        s = pos.strip()
        if "/" in s:
            hi, _, lo = s.partition("/")
            return (int(hi, 16) << 32) | int(lo, 16)
        if s.isdigit():
            return int(s)
        try:
            return int(s, 16)
        except ValueError:
            pass
    if hasattr(pos, "value"):
        return normalize_position(pos.value)
    return int(pos)


def format_lsn(val: int) -> str:
    return f"{val >> 32:X}/{val & 0xFFFFFFFF:X}"


@dataclass
class CutoverAuthorization:
    authorized: bool
    reason: str
    applied_pos: int
    proved_pos: int
    retained_pos: int
    source_ack_pos: int
    required_pos: int


class PositionLattice:
    def __init__(
        self,
        read_pos: Any = 0,
        retained_pos: Any = 0,
        applied_pos: Any = 0,
        proved_pos: Any = 0,
        source_ack_pos: Any = 0,
        strict: bool = False,
    ):
        self._read_pos = normalize_position(read_pos)
        self._retained_pos = normalize_position(retained_pos)
        self._applied_pos = normalize_position(applied_pos)
        self._proved_pos = normalize_position(proved_pos)
        self._source_ack_pos = normalize_position(source_ack_pos)
        self.strict = strict

    @property
    def read_pos(self) -> int:
        return self._read_pos

    @property
    def retained_pos(self) -> int:
        return self._retained_pos

    @property
    def durably_retained_pos(self) -> int:
        return self._retained_pos

    @property
    def applied_pos(self) -> int:
        return self._applied_pos

    @property
    def proved_pos(self) -> int:
        return self._proved_pos

    @property
    def source_ack_pos(self) -> int:
        return self._source_ack_pos

    def advance_read(self, pos: Any) -> int:
        p = normalize_position(pos)
        if p < self._read_pos:
            if self.strict:
                raise MonotoneRegressionError(
                    f"cannot regress read position from {self._read_pos} to {p}"
                )
            return self._read_pos
        self._read_pos = p
        return self._read_pos

    def advance_retained(self, pos: Any) -> int:
        p = normalize_position(pos)
        if p < self._retained_pos:
            if self.strict:
                raise MonotoneRegressionError(
                    f"cannot regress retained position from {self._retained_pos} to {p}"
                )
            return self._retained_pos
        self._retained_pos = p
        if self._retained_pos > self._read_pos:
            self._read_pos = self._retained_pos
        return self._retained_pos

    advance_durably_retained = advance_retained

    def advance_applied(self, pos: Any) -> int:
        p = normalize_position(pos)
        if p < self._applied_pos:
            if self.strict:
                raise MonotoneRegressionError(
                    f"cannot regress applied position from {self._applied_pos} to {p}"
                )
            return self._applied_pos
        self._applied_pos = p
        if self._applied_pos > self._read_pos:
            self._read_pos = self._applied_pos
        return self._applied_pos

    def advance_proved(self, pos: Any) -> int:
        p = normalize_position(pos)
        if p < self._proved_pos:
            if self.strict:
                raise MonotoneRegressionError(
                    f"cannot regress proved position from {self._proved_pos} to {p}"
                )
            return self._proved_pos
        self._proved_pos = p
        if self._proved_pos > self._applied_pos:
            self._applied_pos = self._proved_pos
        if self._applied_pos > self._read_pos:
            self._read_pos = self._applied_pos
        return self._proved_pos

    def advance(self, stage: Union[PipelineStage, str], pos: Any) -> int:
        s = stage if isinstance(stage, PipelineStage) else PipelineStage.from_string(stage)
        if s == READ:
            return self.advance_read(pos)
        if s == DURABLY_RETAINED:
            return self.advance_retained(pos)
        if s == APPLIED:
            return self.advance_applied(pos)
        if s == PROVED:
            return self.advance_proved(pos)
        raise ValueError(f"unknown stage: {stage}")

    def can_ack_source(self, pos: Any) -> bool:
        p = normalize_position(pos)
        return p <= self._retained_pos

    def ack_source(self, pos: Any) -> int:
        p = normalize_position(pos)
        if not self.can_ack_source(p):
            raise SourceAckRefusedError(
                f"source ack at {p} refused: durably retained only up to {self._retained_pos}"
            )
        if p < self._source_ack_pos:
            if self.strict:
                raise MonotoneRegressionError(
                    f"cannot regress source ack from {self._source_ack_pos} to {p}"
                )
            return self._source_ack_pos
        self._source_ack_pos = p
        return self._source_ack_pos

    def can_authorize_cutover(self, required_pos: Optional[Any] = None) -> Tuple[bool, str]:
        target = (
            normalize_position(required_pos)
            if required_pos is not None
            else self._read_pos
        )
        if self._applied_pos < target:
            return (
                False,
                f"target committed-prefix not reached: applied {self._applied_pos} < required {target} "
                "(source ack alone cannot authorize cutover)",
            )
        if self._proved_pos < target:
            return (
                False,
                f"required proof not reached: proved {self._proved_pos} < required {target}",
            )
        return True, "cutover authorized: target applied and proved prefix satisfied"

    def authorize_cutover(self, required_pos: Optional[Any] = None) -> CutoverAuthorization:
        target = (
            normalize_position(required_pos)
            if required_pos is not None
            else self._read_pos
        )
        can, reason = self.can_authorize_cutover(target)
        facts = {
            "applied_meets_required": self._applied_pos >= target,
            "proved_meets_required": self._proved_pos >= target,
            "source_ack_bounded_by_retained": self._source_ack_pos <= self._retained_pos,
            "positions_monotonic": True,
        }
        choice = decide_ack_lattice(facts)
        if choice.rung.name != "authorize_cutover_fully_proved":
            raise CutoverRefusedError(f"{reason} (rung={choice.rung.name}: {choice.rung.because})")
        return CutoverAuthorization(
            authorized=True,
            reason=reason,
            applied_pos=self._applied_pos,
            proved_pos=self._proved_pos,
            retained_pos=self._retained_pos,
            source_ack_pos=self._source_ack_pos,
            required_pos=target,
        )

    def join(self, other: "PositionLattice") -> "PositionLattice":
        return PositionLattice(
            read_pos=max(self._read_pos, other._read_pos),
            retained_pos=max(self._retained_pos, other._retained_pos),
            applied_pos=max(self._applied_pos, other._applied_pos),
            proved_pos=max(self._proved_pos, other._proved_pos),
            source_ack_pos=max(self._source_ack_pos, other._source_ack_pos),
            strict=self.strict and other.strict,
        )

    def meet(self, other: "PositionLattice") -> "PositionLattice":
        return PositionLattice(
            read_pos=min(self._read_pos, other._read_pos),
            retained_pos=min(self._retained_pos, other._retained_pos),
            applied_pos=min(self._applied_pos, other._applied_pos),
            proved_pos=min(self._proved_pos, other._proved_pos),
            source_ack_pos=min(self._source_ack_pos, other._source_ack_pos),
            strict=self.strict or other.strict,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PositionLattice):
            return False
        return (
            self._read_pos == other._read_pos
            and self._retained_pos == other._retained_pos
            and self._applied_pos == other._applied_pos
            and self._proved_pos == other._proved_pos
            and self._source_ack_pos == other._source_ack_pos
        )

    def __le__(self, other: "PositionLattice") -> bool:
        return (
            self._read_pos <= other._read_pos
            and self._retained_pos <= other._retained_pos
            and self._applied_pos <= other._applied_pos
            and self._proved_pos <= other._proved_pos
            and self._source_ack_pos <= other._source_ack_pos
        )

    def __lt__(self, other: "PositionLattice") -> bool:
        return self <= other and self != other

    def to_dict(self) -> Dict[str, Any]:
        return {
            "read_pos": self._read_pos,
            "retained_pos": self._retained_pos,
            "applied_pos": self._applied_pos,
            "proved_pos": self._proved_pos,
            "source_ack_pos": self._source_ack_pos,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any], strict: bool = False) -> "PositionLattice":
        return cls(
            read_pos=d.get("read_pos", 0),
            retained_pos=d.get("retained_pos", 0),
            applied_pos=d.get("applied_pos", 0),
            proved_pos=d.get("proved_pos", 0),
            source_ack_pos=d.get("source_ack_pos", 0),
            strict=strict,
        )

    def summary(self) -> str:
        return (
            f"read={self._read_pos} retained={self._retained_pos} "
            f"applied={self._applied_pos} proved={self._proved_pos} "
            f"ack={self._source_ack_pos}"
        )


class DurableAckPipeline:
    def __init__(self, spool_dir: Union[str, Path]):
        self.spool_dir = Path(spool_dir)
        durable.private_dir(self.spool_dir)
        self.state_path = self.spool_dir / "lattice_state.json"
        self.journal_path = self.spool_dir / "spool.journal"
        self._unspooled: List[Dict[str, Any]] = []

        if self.state_path.exists():
            st = durable.read_state(self.state_path, "state")
            self.lattice = PositionLattice.from_dict(st or {})
        else:
            self.lattice = PositionLattice()

    def read(self, pos: Any, payload: Any = None) -> int:
        p = normalize_position(pos)
        self._unspooled.append({"pos": p, "payload": payload})
        self.lattice.advance_read(p)
        return p

    def spool(self, pos: Optional[Any] = None) -> int:
        target = normalize_position(pos) if pos is not None else self.lattice.read_pos
        to_flush = [r for r in self._unspooled if r["pos"] <= target]
        self._unspooled = [r for r in self._unspooled if r["pos"] > target]

        if to_flush:
            lines = [json.dumps(r) for r in to_flush]
            durable.append_lines(self.journal_path, lines, sync=True)
            max_flushed = max(r["pos"] for r in to_flush)
            self.lattice.advance_retained(max_flushed)
        elif target <= self.lattice.retained_pos:
            pass
        else:
            self.lattice.advance_retained(target)

        durable.write_json(self.state_path, self.lattice.to_dict())
        return self.lattice.retained_pos

    def ack(self, pos: Any) -> int:
        ack_val = self.lattice.ack_source(pos)
        durable.write_json(self.state_path, self.lattice.to_dict())
        return ack_val

    def apply(self, pos: Any) -> int:
        app_val = self.lattice.advance_applied(pos)
        durable.write_json(self.state_path, self.lattice.to_dict())
        return app_val

    def prove(self, pos: Any) -> int:
        prv_val = self.lattice.advance_proved(pos)
        durable.write_json(self.state_path, self.lattice.to_dict())
        return prv_val

    def reconstruct(self) -> List[Dict[str, Any]]:
        if not self.journal_path.exists():
            return []
        items = durable.read_lines(self.journal_path, "journal")
        records = []
        for item in items:
            rec = item if isinstance(item, dict) else json.loads(item)
            if rec.get("pos", 0) > self.lattice.applied_pos:
                records.append(rec)
        return records

    def crash(self) -> None:
        self._unspooled.clear()
