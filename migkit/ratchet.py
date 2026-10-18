import dataclasses
from typing import Any, Dict, List, Optional, Tuple, Union

from . import decide

HEADROOM_DEFAULT = 10000


def get_default_headroom() -> int:
    if HEADROOM_DEFAULT != 10000:
        return int(HEADROOM_DEFAULT)
    from . import envkeys
    v = envkeys.number("MIGKIT_HEADROOM_RATCHET")
    return int(v) if v is not None else HEADROOM_DEFAULT


class RatchetError(RuntimeError):
    pass


class SlotMissingError(RatchetError):
    pass


class SnapshotAuthorizationError(RatchetError):
    pass


class HeadroomDeficitError(RatchetError):
    pass


@dataclasses.dataclass
class RatchetItem:
    name: str
    source_max: int
    target_max: int
    current_target_seq: int
    ratchet_target_seq: int
    has_adequate_headroom: bool
    forward_ddl: str
    rollback_ddl: str
    schema: Optional[str] = None
    engine: str = "postgres"

    @property
    def greatest_max(self) -> int:
        return max(self.source_max, self.target_max)

    @property
    def required_headroom(self) -> int:
        return self.ratchet_target_seq - self.greatest_max


@dataclasses.dataclass
class HeadroomRatchetPlan:
    engine: str
    db: str
    headroom: int = HEADROOM_DEFAULT
    items: List[RatchetItem] = dataclasses.field(default_factory=list)
    refused: bool = False
    refusal_reasons: List[str] = dataclasses.field(default_factory=list)
    applied: bool = False

    @property
    def forward_ddl(self) -> List[str]:
        return [item.forward_ddl for item in self.items if item.forward_ddl]

    @property
    def rollback_ddl(self) -> List[str]:
        return [item.rollback_ddl for item in reversed(self.items) if item.rollback_ddl]

    def generate_forward_ddl_script(self) -> str:
        if not self.forward_ddl:
            return ""
        lines = [
            f"-- migkit forward ddl: engine={self.engine} db={self.db} headroom={self.headroom}",
            *self.forward_ddl,
        ]
        return "\n".join(lines) + "\n"

    def generate_rollback_ddl_script(self) -> str:
        if not self.rollback_ddl:
            return ""
        lines = [
            f"-- migkit reversible rollback ddl: engine={self.engine} db={self.db}",
            *self.rollback_ddl,
        ]
        return "\n".join(lines) + "\n"

    def apply_forward(self, engine_or_conn: Any = None, side: str = "dst") -> None:
        if not self.forward_ddl:
            return
        target = engine_or_conn
        apply_statements(target, self.db, self.forward_ddl, side=side)
        for item in self.items:
            if not item.has_adequate_headroom:
                item.current_target_seq = item.ratchet_target_seq
                item.has_adequate_headroom = True
        self.applied = True
        self.refused = False
        self.refusal_reasons.clear()

    def apply_rollback(self, engine_or_conn: Any = None, side: str = "dst") -> None:
        if not self.rollback_ddl:
            return
        target = engine_or_conn
        apply_statements(target, self.db, self.rollback_ddl, side=side)
        self.applied = False


def make_postgres_forward_ddl(name: str, target_seq: int) -> str:
    escaped = name.replace("'", "''")
    return f"SELECT setval('{escaped}', {target_seq}, true);"


def make_postgres_rollback_ddl(name: str, prev_seq: int) -> str:
    escaped = name.replace("'", "''")
    if prev_seq <= 0:
        return f"SELECT setval('{escaped}', 1, false);"
    return f"SELECT setval('{escaped}', {prev_seq}, true);"


def make_mysql_forward_ddl(name: str, target_seq: int, db: Optional[str] = None) -> str:
    escaped = name.replace("`", "``")
    if db:
        escaped_db = db.replace("`", "``")
        return f"ALTER TABLE `{escaped_db}`.`{escaped}` AUTO_INCREMENT = {target_seq};"
    return f"ALTER TABLE `{escaped}` AUTO_INCREMENT = {target_seq};"


def make_mysql_rollback_ddl(name: str, prev_seq: int, db: Optional[str] = None) -> str:
    escaped = name.replace("`", "``")
    val = max(1, prev_seq)
    if db:
        escaped_db = db.replace("`", "``")
        return f"ALTER TABLE `{escaped_db}`.`{escaped}` AUTO_INCREMENT = {val};"
    return f"ALTER TABLE `{escaped}` AUTO_INCREMENT = {val};"


def calculate_ratchet(
    name: str,
    source_max: int,
    target_max: int,
    current_target_seq: int,
    headroom: int = HEADROOM_DEFAULT,
    engine: str = "postgres",
    db: Optional[str] = None,
    schema: Optional[str] = None,
) -> RatchetItem:
    s_max = max(0, int(source_max or 0))
    t_max = max(0, int(target_max or 0))
    c_target = max(0, int(current_target_seq or 0))
    greatest = max(s_max, t_max)
    ratchet_target_seq = greatest + headroom
    has_adequate_headroom = c_target >= ratchet_target_seq

    fwd = ""
    rev = ""
    if not has_adequate_headroom:
        if engine == "postgres":
            fwd = make_postgres_forward_ddl(name, ratchet_target_seq)
            rev = make_postgres_rollback_ddl(name, c_target)
        elif engine == "mysql":
            fwd = make_mysql_forward_ddl(name, ratchet_target_seq, db=db)
            rev = make_mysql_rollback_ddl(name, c_target, db=db)
        else:
            fwd = f"-- ratchet: {name} = {ratchet_target_seq};"
            rev = f"-- rollback: {name} = {c_target};"

    return RatchetItem(
        name=name,
        source_max=s_max,
        target_max=t_max,
        current_target_seq=c_target,
        ratchet_target_seq=ratchet_target_seq,
        has_adequate_headroom=has_adequate_headroom,
        forward_ddl=fwd,
        rollback_ddl=rev,
        schema=schema,
        engine=engine,
    )


def plan_ratchet(
    sources: Dict[str, int],
    targets: Dict[str, int],
    current: Dict[str, int],
    engine: str = "postgres",
    db: str = "",
    headroom: int = HEADROOM_DEFAULT,
) -> HeadroomRatchetPlan:
    keys = sorted(set(sources.keys()) | set(targets.keys()) | set(current.keys()))
    items = []
    refusal_reasons = []
    refused = False

    for key in keys:
        s_max = sources.get(key, 0)
        t_max = targets.get(key, 0)
        c_val = current.get(key, 0)
        item = calculate_ratchet(
            name=key,
            source_max=s_max,
            target_max=t_max,
            current_target_seq=c_val,
            headroom=headroom,
            engine=engine,
            db=db,
        )
        items.append(item)
        if not item.has_adequate_headroom:
            refused = True
            refusal_reasons.append(
                f"{key}: target sequence lacks adequate headroom ({item.current_target_seq} < {item.ratchet_target_seq})"
            )

    return HeadroomRatchetPlan(
        engine=engine,
        db=db,
        headroom=headroom,
        items=items,
        refused=refused,
        refusal_reasons=refusal_reasons,
    )


def apply_statements(engine_or_conn: Any, db: str, statements: List[str], side: str = "dst") -> None:
    if not engine_or_conn:
        return
    for stmt in statements:
        if not stmt or stmt.strip().startswith("--"):
            continue
        if hasattr(engine_or_conn, "_psql"):
            engine_or_conn._psql(side, db, stmt)
        elif hasattr(engine_or_conn, "_q"):
            engine_or_conn._q(side, stmt)
        elif hasattr(engine_or_conn, "execute"):
            engine_or_conn.execute(stmt)
        elif hasattr(engine_or_conn, "cursor"):
            with engine_or_conn.cursor() as cur:
                cur.execute(stmt)


def verify_target_replication_slot(
    engine_or_conn: Any,
    db: str,
    slot_name: Optional[str] = None,
    side: str = "dst",
) -> bool:
    target_slot = slot_name
    if not target_slot:
        target_slot = getattr(engine_or_conn, "_repl_name", lambda: "migkit")
        if callable(target_slot):
            target_slot = target_slot()
        target_slot = str(target_slot).lower()

    if hasattr(engine_or_conn, "_check_slot_exists"):
        return bool(engine_or_conn._check_slot_exists(side, db, target_slot))

    query = f"select 1 from pg_replication_slots where slot_name = '{target_slot}'"
    try:
        if hasattr(engine_or_conn, "_psql"):
            res = engine_or_conn._psql(side, db, query)
            if isinstance(res, str):
                return "1" in res.split()
            return bool(res)
        if hasattr(engine_or_conn, "_q"):
            res = engine_or_conn._q(side, query)
            return bool(res and len(res) > 0)
        if hasattr(engine_or_conn, "cursor"):
            with engine_or_conn.cursor() as cur:
                cur.execute(query)
                return bool(cur.fetchone())
        if hasattr(engine_or_conn, "execute"):
            res = engine_or_conn.execute(query)
            if hasattr(res, "fetchone"):
                return bool(res.fetchone())
            return bool(res)
    except Exception:
        return False
    return False


def authorize_snapshot_export(
    engine_or_conn: Any,
    db: str,
    slot_name: Optional[str] = None,
    side: str = "dst",
) -> bool:
    eng_name = getattr(engine_or_conn, "engine", "")
    if hasattr(eng_name, "lower"):
        eng_name = eng_name.lower()

    if eng_name in ("postgres", "postgresql", "gaussdb", "yugabyte") or slot_name:
        if not verify_target_replication_slot(engine_or_conn, db, slot_name=slot_name, side=side):
            actual_slot = slot_name or getattr(engine_or_conn, "_repl_name", lambda: "migkit")
            if callable(actual_slot):
                actual_slot = actual_slot()
            actual_slot = str(actual_slot).lower()
            raise SlotMissingError(
                f"logical replication slot '{actual_slot}' missing on target {db}; snapshot export refused"
            )
    return True


def build_headroom_ratchet_plan(
    engine: Any,
    db: str,
    headroom: int = HEADROOM_DEFAULT,
) -> HeadroomRatchetPlan:
    eng_name = getattr(engine, "engine", "postgres")
    if hasattr(eng_name, "lower"):
        eng_name = eng_name.lower()

    sources: Dict[str, int] = {}
    targets: Dict[str, int] = {}
    current: Dict[str, int] = {}

    if "postgres" in eng_name or "gaussdb" in eng_name or "yugabyte" in eng_name:
        try:
            q = (
                "select schemaname||'.'||sequencename||'|'||coalesce(last_value,0)"
                " from pg_sequences where schemaname not like '\\_\\_%'"
            )
            src_raw = getattr(engine, "_psql", lambda s, d, q: "")("src", db, q)
            dst_raw = getattr(engine, "_psql", lambda s, d, q: "")("dst", db, q)
            src_seq = dict(l.rsplit("|", 1) for l in src_raw.splitlines() if "|" in l)
            dst_seq = dict(l.rsplit("|", 1) for l in dst_raw.splitlines() if "|" in l)

            owned = getattr(engine, "_seq_owned", lambda s, d: {})("dst", db)
            smax = getattr(engine, "_seq_col_max", lambda s, d, o: {})("src", db, owned)
            dmax = getattr(engine, "_seq_col_max", lambda s, d, o: {})("dst", db, owned)

            all_names = sorted(set(src_seq.keys()) | set(dst_seq.keys()) | set(owned.keys()))
            for name in all_names:
                sv = int(src_seq.get(name, 0))
                sm = max(sv, int(smax.get(name, 0)))
                dm = max(0, int(dmax.get(name, 0)))
                cv = int(dst_seq.get(name, 0))
                sources[name] = sm
                targets[name] = dm
                current[name] = cv
        except Exception as e:
            return HeadroomRatchetPlan(
                engine="postgres",
                db=db,
                headroom=headroom,
                refused=True,
                refusal_reasons=[f"failed to read sequence metadata for {db}: {e}"],
            )
        return plan_ratchet(sources, targets, current, engine="postgres", db=db, headroom=headroom)

    if "mysql" in eng_name or "mariadb" in eng_name:
        try:
            counters = getattr(engine, "_counters", None)
            if counters:
                src_c, dst_c = counters(db)
            else:
                src_c, dst_c = {}, {}
            for tbl in sorted(set(src_c.keys()) | set(dst_c.keys())):
                sv = int(src_c.get(tbl, 0))
                dv = int(dst_c.get(tbl, 0))
                sources[tbl] = sv
                targets[tbl] = dv
                current[tbl] = dv
        except Exception as e:
            return HeadroomRatchetPlan(
                engine="mysql",
                db=db,
                headroom=headroom,
                refused=True,
                refusal_reasons=[f"failed to read counter metadata for {db}: {e}"],
            )
        return plan_ratchet(sources, targets, current, engine="mysql", db=db, headroom=headroom)

    return plan_ratchet(sources, targets, current, engine=eng_name, db=db, headroom=headroom)


def execute_headroom_ratchet(
    engine: Any,
    db: str,
    headroom: Optional[int] = None,
    apply: bool = True,
) -> HeadroomRatchetPlan:
    if headroom is None:
        headroom = get_default_headroom()
    plan = build_headroom_ratchet_plan(engine, db, headroom=headroom)
    if apply and plan.forward_ddl:
        plan.apply_forward(engine, side="dst")
    return plan


def pre_syncback_cutover_check(
    engine: Any,
    db: str,
    headroom: Optional[int] = None,
    auto_ratchet: bool = False,
    refuse_on_deficit: bool = True,
) -> HeadroomRatchetPlan:
    if headroom is None:
        headroom = get_default_headroom()
    if auto_ratchet:
        plan = execute_headroom_ratchet(engine, db, headroom=headroom, apply=True)
    else:
        plan = build_headroom_ratchet_plan(engine, db, headroom=headroom)

    if plan.refused and refuse_on_deficit:
        raise HeadroomDeficitError(f"cutover refused: {'; '.join(plan.refusal_reasons)}")

    return plan


def _prove_adequate_headroom(facts: Dict[str, Any]) -> bool:
    if facts.get("deficit_detected"):
        return False
    current = facts.get("current_seq", 0)
    greatest = facts.get("greatest_max", 0)
    headroom = facts.get("headroom", get_default_headroom())
    return int(current) >= int(greatest) + int(headroom)


def _prove_forward_ratchet_applied(facts: Dict[str, Any]) -> bool:
    if facts.get("forward_failed"):
        return False
    return bool(facts.get("forward_applied") or facts.get("applied"))


def _prove_slot_pre_snapshot_verified(facts: Dict[str, Any]) -> bool:
    if facts.get("slot_missing"):
        return False
    return bool(facts.get("slot_verified") or facts.get("slot_exists"))


def _prove_rollback_script_generation(facts: Dict[str, Any]) -> bool:
    if facts.get("rollback_omitted"):
        return False
    return bool(facts.get("rollback_ddl") or facts.get("has_rollback_script"))


def _prove_refuse_unratcheted_deficit(facts: Dict[str, Any]) -> bool:
    if facts.get("unratcheted_refused"):
        return False
    return bool(facts.get("explicit_deficit_override"))


def headroom_ratchet_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "adequate_headroom_verified",
            "verify target sequences possess adequate headroom over greatest source/target max",
            needs=(
                decide.Need(
                    "target sequence value meets or exceeds greatest max plus headroom",
                    _prove_adequate_headroom,
                    "target sequence lacks required headroom buffer",
                ),
            ),
        ),
        decide.Rung(
            "automated_forward_ratchet_applied",
            "apply automated forward ratchet DDL ensuring non-colliding cutover sequence space",
            needs=(
                decide.Need(
                    "forward ratchet DDL applied successfully to target",
                    _prove_forward_ratchet_applied,
                    "forward ratchet DDL unapplied or failed during execution",
                ),
            ),
        ),
        decide.Rung(
            "replication_slot_pre_snapshot_verified",
            "verify logical replication slot existence before authorizing snapshot export",
            needs=(
                decide.Need(
                    "replication slot confirmed present on publisher before snapshot",
                    _prove_slot_pre_snapshot_verified,
                    "replication slot missing on publisher prior to snapshot export",
                ),
            ),
        ),
        decide.Rung(
            "reversible_rollback_script_generation",
            "generate reversible rollback script restoring previous sequence counters",
            needs=(
                decide.Need(
                    "rollback statements generated for all ratcheted items",
                    _prove_rollback_script_generation,
                    "rollback statements incomplete or missing previous values",
                ),
            ),
        ),
        decide.Rung(
            "refuse_unratcheted_cutover_deficit",
            "refuse cutover execution when sequence deficit remains un-ratcheted",
            needs=(
                decide.Need(
                    "all sequence deficits resolved or explicit unrefused override",
                    _prove_refuse_unratcheted_deficit,
                    "cutover refused due to unresolved sequence headroom deficit",
                ),
            ),
        ),
    )


def decide_headroom_ratchet(facts: Dict[str, Any]) -> decide.Choice:
    return decide.climb("headroom ratchet", headroom_ratchet_rungs(), facts=decide.Facts(facts))
