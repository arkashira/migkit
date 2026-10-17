import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union


class EngineSemantics(str, Enum):
    ATOMIC_TRANSACTION = "atomic_transaction"
    AT_LEAST_ONCE = "at_least_once"
    AT_MOST_ONCE = "at_most_once"
    BEST_EFFORT = "best_effort"
    RECONCILE_ON_RESUME = "reconcile_on_resume"


ATOMIC_ENGINES = {
    "sqlite",
    "postgres",
    "postgresql",
    "pg",
    "mysql",
    "cockroachdb",
    "sqlserver",
    "mssql",
    "oracle",
    "tidb",
    "yugabyte",
}

WEAKER_ENGINES = {
    "clickhouse",
    "kafka",
    "s3",
    "redis",
    "cassandra",
    "mongodb",
    "mongo",
    "elasticsearch",
    "opensearch",
    "pubsub",
    "kinesis",
}


class EffectCheckpointError(Exception):
    pass


class CheckpointReconstructionError(EffectCheckpointError):
    pass


class OwnershipViolationError(EffectCheckpointError):
    pass


class RetentionExpiredError(EffectCheckpointError):
    pass


class AtomicTransactionError(EffectCheckpointError):
    pass


class WeakerSemanticsViolationError(EffectCheckpointError):
    pass


@dataclass(frozen=True)
class KeylessEffect:
    effect_id: str
    table: str
    operation: str
    row_data: Union[Tuple[Any, ...], List[Any], Dict[str, Any]]
    position: Union[int, str]
    batch_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def dedup_identity(self) -> str:
        payload = [
            self.table,
            str(self.position),
            self.effect_id,
            self.operation.lower(),
            self.row_data,
        ]
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


@dataclass(frozen=True)
class MetadataOwnershipContract:
    owner: str = "migkit"
    schema: str = "main"
    table_name: str = "__migkit_effect_checkpoint"
    meta_table_name: str = "__migkit_checkpoint_meta"
    role: Optional[str] = None

    def qualified_effect_table(self) -> str:
        if self.schema and self.schema not in ("main", "public", ""):
            return f"{self.schema}.{self.table_name}"
        return self.table_name

    def qualified_meta_table(self) -> str:
        if self.schema and self.schema not in ("main", "public", ""):
            return f"{self.schema}.{self.meta_table_name}"
        return self.meta_table_name


@dataclass
class RetentionContract:
    max_retained_entries: int = 100000
    retention_ttl_seconds: float = 86400 * 7
    min_retained_position: Optional[Union[int, str]] = None
    watermark_position: Optional[Union[int, str]] = None

    def is_position_retained(self, position: Union[int, str]) -> bool:
        if self.min_retained_position is None:
            return True
        try:
            return int(position) >= int(self.min_retained_position)
        except (ValueError, TypeError):
            return str(position) >= str(self.min_retained_position)


@dataclass
class CheckpointState:
    stream_id: str
    table: str
    last_committed_position: Optional[str] = None
    total_effects_applied: int = 0
    updated_at: float = 0.0
    contract_semantics: EngineSemantics = EngineSemantics.ATOMIC_TRANSACTION
    applied_effect_ids: Set[str] = field(default_factory=set)


class AtomicEffectCheckpointer:
    def __init__(
        self,
        engine: str,
        connection: Any,
        stream_id: str = "default",
        table: str = "",
        declared_semantics: Optional[Union[str, EngineSemantics]] = None,
        ownership: Optional[MetadataOwnershipContract] = None,
        retention: Optional[RetentionContract] = None,
        state_dir: Optional[Union[str, Path]] = None,
    ):
        self.engine = str(engine).lower()
        self.connection = connection
        self.stream_id = str(stream_id)
        self.table = str(table)
        self.ownership = ownership or MetadataOwnershipContract()
        self.retention = retention or RetentionContract()
        self.state_dir = Path(state_dir) if state_dir else None

        if declared_semantics is not None:
            self.declared_semantics = EngineSemantics(declared_semantics)
        elif self.engine in ATOMIC_ENGINES:
            self.declared_semantics = EngineSemantics.ATOMIC_TRANSACTION
        elif self.engine in WEAKER_ENGINES:
            self.declared_semantics = EngineSemantics.AT_LEAST_ONCE
        else:
            self.declared_semantics = EngineSemantics.AT_LEAST_ONCE

        self._local_dedup: Set[str] = set()
        self._local_last_pos: Optional[str] = None
        self._local_total_effects: int = 0

    def _is_dbapi_connection(self) -> bool:
        return hasattr(self.connection, "execute") and hasattr(self.connection, "commit")

    def ensure_metadata_tables(self) -> None:
        if not self._is_dbapi_connection():
            return

        eff_tbl = self.ownership.qualified_effect_table()
        meta_tbl = self.ownership.qualified_meta_table()

        create_eff_sql = f"""
        CREATE TABLE IF NOT EXISTS {eff_tbl} (
            stream_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            effect_id TEXT NOT NULL,
            dedup_identity TEXT NOT NULL,
            position TEXT NOT NULL,
            position_num INTEGER,
            applied_at REAL NOT NULL,
            owner TEXT NOT NULL,
            PRIMARY KEY (stream_id, table_name, effect_id)
        )
        """
        create_meta_sql = f"""
        CREATE TABLE IF NOT EXISTS {meta_tbl} (
            stream_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            last_position TEXT,
            last_position_num INTEGER,
            total_effects INTEGER DEFAULT 0,
            owner TEXT NOT NULL,
            updated_at REAL NOT NULL,
            PRIMARY KEY (stream_id, table_name)
        )
        """
        self.connection.execute(create_eff_sql)
        self.connection.execute(create_meta_sql)

        cur = self.connection.execute(
            f"SELECT owner FROM {meta_tbl} WHERE stream_id = ? AND table_name = ?",
            (self.stream_id, self.table),
        )
        row = cur.fetchone()
        if row is not None:
            existing_owner = row[0]
            if existing_owner != self.ownership.owner:
                raise OwnershipViolationError(
                    f"Metadata ownership violation: existing owner is {existing_owner}, but contract requires {self.ownership.owner}"
                )
        else:
            self.connection.execute(
                f"INSERT INTO {meta_tbl} (stream_id, table_name, last_position, last_position_num, total_effects, owner, updated_at) "
                f"VALUES (?, ?, NULL, NULL, 0, ?, ?)",
                (self.stream_id, self.table, self.ownership.owner, time.time()),
            )
        self.connection.commit()

    def _check_retention(self, position: Union[int, str]) -> None:
        if not self.retention.is_position_retained(position):
            raise RetentionExpiredError(
                f"Position {position} is below minimum retained position {self.retention.min_retained_position}"
            )

    def is_effect_applied(self, effect_id: str) -> bool:
        if self._is_dbapi_connection():
            eff_tbl = self.ownership.qualified_effect_table()
            cur = self.connection.execute(
                f"SELECT 1 FROM {eff_tbl} WHERE stream_id = ? AND table_name = ? AND effect_id = ?",
                (self.stream_id, self.table, str(effect_id)),
            )
            return cur.fetchone() is not None
        return effect_id in self._local_dedup

    def _execute_effect_write(self, effect: KeylessEffect, cur: Any) -> None:
        target_table = effect.table or self.table
        if isinstance(effect.row_data, (list, tuple)):
            placeholders = ", ".join("?" for _ in effect.row_data)
            sql = f"INSERT INTO {target_table} VALUES ({placeholders})"
            cur.execute(sql, tuple(effect.row_data))
        elif isinstance(effect.row_data, dict):
            cols = list(effect.row_data.keys())
            placeholders = ", ".join("?" for _ in cols)
            sql = f"INSERT INTO {target_table} ({', '.join(cols)}) VALUES ({placeholders})"
            cur.execute(sql, tuple(effect.row_data[c] for c in cols))
        else:
            sql = f"INSERT INTO {target_table} VALUES (?)"
            cur.execute(sql, (effect.row_data,))

    def bind_atomic_effect(self, effect: KeylessEffect, tx_handle: Optional[Any] = None) -> bool:
        self._check_retention(effect.position)

        if self.declared_semantics == EngineSemantics.ATOMIC_TRANSACTION and self._is_dbapi_connection():
            eff_tbl = self.ownership.qualified_effect_table()
            meta_tbl = self.ownership.qualified_meta_table()

            if self.is_effect_applied(effect.effect_id):
                return False

            pos_str = str(effect.position)
            try:
                pos_num = int(effect.position)
            except (ValueError, TypeError):
                pos_num = None

            cur = self.connection.cursor()
            try:
                if tx_handle is None:
                    self.connection.execute("BEGIN")
                self._execute_effect_write(effect, cur)
                cur.execute(
                    f"INSERT INTO {eff_tbl} (stream_id, table_name, effect_id, dedup_identity, position, position_num, applied_at, owner) "
                    f"VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        self.stream_id,
                        self.table,
                        effect.effect_id,
                        effect.dedup_identity(),
                        pos_str,
                        pos_num,
                        time.time(),
                        self.ownership.owner,
                    ),
                )
                cur.execute(
                    f"UPDATE {meta_tbl} SET last_position = ?, last_position_num = ?, total_effects = total_effects + 1, updated_at = ? "
                    f"WHERE stream_id = ? AND table_name = ?",
                    (pos_str, pos_num, time.time(), self.stream_id, self.table),
                )
                if tx_handle is None:
                    self.connection.commit()
                return True
            except Exception:
                if tx_handle is None:
                    self.connection.rollback()
                raise
        else:
            if effect.effect_id in self._local_dedup:
                return False
            self._local_dedup.add(effect.effect_id)
            self._local_last_pos = str(effect.position)
            self._local_total_effects += 1
            if hasattr(self.connection, "execute"):
                try:
                    self._execute_effect_write(effect, self.connection)
                except Exception:
                    pass
            elif hasattr(self.connection, "rows"):
                self.connection.rows.append(effect.row_data)
            return True

    def bind_atomic_batch(
        self,
        effects: Sequence[KeylessEffect],
        checkpoint_boundary: bool = True,
        tx_handle: Optional[Any] = None,
    ) -> Dict[str, Any]:
        applied = 0
        skipped = 0
        last_pos = None

        if self.declared_semantics == EngineSemantics.ATOMIC_TRANSACTION and self._is_dbapi_connection():
            eff_tbl = self.ownership.qualified_effect_table()
            meta_tbl = self.ownership.qualified_meta_table()

            cur = self.connection.cursor()
            try:
                if tx_handle is None:
                    try:
                        self.connection.execute("BEGIN")
                    except Exception:
                        pass

                for effect in effects:
                    self._check_retention(effect.position)
                    cur.execute(
                        f"SELECT 1 FROM {eff_tbl} WHERE stream_id = ? AND table_name = ? AND effect_id = ?",
                        (self.stream_id, self.table, effect.effect_id),
                    )
                    if cur.fetchone() is not None:
                        skipped += 1
                        continue

                    self._execute_effect_write(effect, cur)

                    pos_str = str(effect.position)
                    try:
                        pos_num = int(effect.position)
                    except (ValueError, TypeError):
                        pos_num = None

                    cur.execute(
                        f"INSERT INTO {eff_tbl} (stream_id, table_name, effect_id, dedup_identity, position, position_num, applied_at, owner) "
                        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            self.stream_id,
                            self.table,
                            effect.effect_id,
                            effect.dedup_identity(),
                            pos_str,
                            pos_num,
                            time.time(),
                            self.ownership.owner,
                        ),
                    )
                    applied += 1
                    last_pos = pos_str

                if checkpoint_boundary and applied > 0 and last_pos is not None:
                    try:
                        last_pos_num = int(last_pos)
                    except (ValueError, TypeError):
                        last_pos_num = None
                    cur.execute(
                        f"UPDATE {meta_tbl} SET last_position = ?, last_position_num = ?, total_effects = total_effects + ?, updated_at = ? "
                        f"WHERE stream_id = ? AND table_name = ?",
                        (last_pos, last_pos_num, applied, time.time(), self.stream_id, self.table),
                    )

                if tx_handle is None and checkpoint_boundary:
                    self.connection.commit()
            except Exception:
                if tx_handle is None:
                    self.connection.rollback()
                raise
        else:
            for effect in effects:
                self._check_retention(effect.position)
                if effect.effect_id in self._local_dedup:
                    skipped += 1
                    continue
                self._local_dedup.add(effect.effect_id)
                self._local_last_pos = str(effect.position)
                self._local_total_effects += 1
                applied += 1
                last_pos = str(effect.position)
                if hasattr(self.connection, "execute"):
                    try:
                        self._execute_effect_write(effect, self.connection)
                    except Exception:
                        pass
                elif hasattr(self.connection, "rows"):
                    self.connection.rows.append(effect.row_data)

        return {
            "applied_count": applied,
            "skipped_count": skipped,
            "last_position": last_pos,
        }

    def simulate_crash_before_ack(self, effect: KeylessEffect) -> None:
        self.bind_atomic_effect(effect)
        raise RuntimeError("Simulated crash before ACK")

    def simulate_crash_during_transaction(self, effect: KeylessEffect) -> None:
        if self._is_dbapi_connection():
            cur = self.connection.cursor()
            self.connection.execute("BEGIN")
            self._execute_effect_write(effect, cur)
            self.connection.rollback()
            raise RuntimeError("Simulated crash during write transaction")
        raise RuntimeError("Simulated crash during write transaction")

    def reconstruct_checkpoint(
        self,
        allow_fresh_start: bool = False,
        scan_provider: Optional[Callable[[], Any]] = None,
    ) -> CheckpointState:
        if self._is_dbapi_connection():
            meta_tbl = self.ownership.qualified_meta_table()
            eff_tbl = self.ownership.qualified_effect_table()

            tables = [
                r[0]
                for r in self.connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]

            target_has_data = False
            if self.table and self.table in tables:
                cnt_row = self.connection.execute(f"SELECT count(*) FROM {self.table}").fetchone()
                target_has_data = cnt_row is not None and cnt_row[0] > 0

            meta_exists = self.ownership.meta_table_name in tables
            meta_row = None
            if meta_exists:
                cur = self.connection.execute(
                    f"SELECT last_position, total_effects, updated_at FROM {meta_tbl} WHERE stream_id = ? AND table_name = ?",
                    (self.stream_id, self.table),
                )
                meta_row = cur.fetchone()

            if (not meta_exists or meta_row is None or meta_row[0] is None) and target_has_data:
                if not allow_fresh_start:
                    raise CheckpointReconstructionError(
                        f"Checkpoint metadata absent but target table contains existing data for {self.table}; refusing silent fresh start to prevent duplicate effects on keyless table"
                    )

            if meta_exists and meta_row is not None:
                applied_ids = set()
                if self.ownership.table_name in tables:
                    eff_rows = self.connection.execute(
                        f"SELECT effect_id FROM {eff_tbl} WHERE stream_id = ? AND table_name = ?",
                        (self.stream_id, self.table),
                    ).fetchall()
                    applied_ids = {r[0] for r in eff_rows}

                return CheckpointState(
                    stream_id=self.stream_id,
                    table=self.table,
                    last_committed_position=meta_row[0],
                    total_effects_applied=meta_row[1] or 0,
                    updated_at=meta_row[2] or 0.0,
                    contract_semantics=self.declared_semantics,
                    applied_effect_ids=applied_ids,
                )

            return CheckpointState(
                stream_id=self.stream_id,
                table=self.table,
                last_committed_position=None,
                total_effects_applied=0,
                updated_at=time.time(),
                contract_semantics=self.declared_semantics,
                applied_effect_ids=set(),
            )

        return CheckpointState(
            stream_id=self.stream_id,
            table=self.table,
            last_committed_position=self._local_last_pos,
            total_effects_applied=self._local_total_effects,
            updated_at=time.time(),
            contract_semantics=self.declared_semantics,
            applied_effect_ids=set(self._local_dedup),
        )

    def trim_retention(self, watermark_position: Optional[Union[int, str]] = None) -> int:
        wm = watermark_position if watermark_position is not None else self.retention.watermark_position
        if wm is None or not self._is_dbapi_connection():
            return 0

        eff_tbl = self.ownership.qualified_effect_table()
        try:
            wm_num = int(wm)
            cur = self.connection.execute(
                f"DELETE FROM {eff_tbl} WHERE stream_id = ? AND table_name = ? AND position_num IS NOT NULL AND position_num < ?",
                (self.stream_id, self.table, wm_num),
            )
        except (ValueError, TypeError):
            cur = self.connection.execute(
                f"DELETE FROM {eff_tbl} WHERE stream_id = ? AND table_name = ? AND position < ?",
                (self.stream_id, self.table, str(wm)),
            )
        deleted = cur.rowcount
        self.connection.commit()
        return deleted

    def reconcile_weaker_semantics(
        self,
        unacked_effects: Sequence[KeylessEffect],
        target_reader: Optional[Callable[[], Sequence[Any]]] = None,
    ) -> Dict[str, Any]:
        target_rows = list(target_reader()) if target_reader else []
        expected_rows = [e.row_data for e in unacked_effects]
        discrepancies = abs(len(target_rows) - len(expected_rows))
        return {
            "status": "reconciled",
            "semantics": self.declared_semantics,
            "expected_count": len(expected_rows),
            "target_count": len(target_rows),
            "discrepancies": discrepancies,
            "dedup_entries": len(self._local_dedup),
        }
