"""Barriers, tombstones, and neutral write markers across engines (BAR-1, BAR-2, BAR-3, P0-DEPTH.tombstones, P0-DE.3)."""
import dataclasses
import json
import time
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclasses.dataclass
class Tombstone:
    table: str
    key: Dict[str, Any]
    version: Optional[Any] = None
    timestamp: float = dataclasses.field(default_factory=time.time)
    deleted_by: str = "migkit"
    metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "key": self.key,
            "version": self.version,
            "timestamp": self.timestamp,
            "deleted_by": self.deleted_by,
            "metadata": self.metadata or {},
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Tombstone":
        return cls(
            table=data["table"],
            key=data["key"],
            version=data.get("version"),
            timestamp=data.get("timestamp", 0.0),
            deleted_by=data.get("deleted_by", "migkit"),
            metadata=data.get("metadata"),
        )


class TombstoneLedger:
    def __init__(self, storage_path: Optional[str] = None):
        self._tombstones: Dict[Tuple[str, str], Tombstone] = {}
        self.storage_path = storage_path

    @staticmethod
    def _key_ident(table: str, key: Dict[str, Any]) -> Tuple[str, str]:
        canonical_key = json.dumps(key, sort_keys=True, default=str)
        return (table, canonical_key)

    def record(self, table: str, key: Dict[str, Any], version: Optional[Any] = None,
               timestamp: Optional[float] = None, deleted_by: str = "migkit",
               metadata: Optional[Dict[str, Any]] = None) -> Tombstone:
        ident = self._key_ident(table, key)
        ts = timestamp if timestamp is not None else time.time()
        tombstone = Tombstone(
            table=table,
            key=dict(key),
            version=version,
            timestamp=ts,
            deleted_by=deleted_by,
            metadata=metadata or {},
        )
        self._tombstones[ident] = tombstone
        if self.storage_path:
            self.save()
        return tombstone

    def is_tombstoned(self, table: str, key: Dict[str, Any],
                      version: Optional[Any] = None,
                      timestamp: Optional[float] = None) -> bool:
        ident = self._key_ident(table, key)
        t = self._tombstones.get(ident)
        if not t:
            return False
        if version is not None and t.version is not None:
            try:
                if version <= t.version:
                    return True
                return False
            except TypeError:
                return str(version) <= str(t.version)
        if timestamp is not None and t.timestamp:
            return timestamp <= t.timestamp
        return True

    def prevent_resurrection(self, table: str, key: Dict[str, Any],
                             incoming_op: str,
                             incoming_version: Optional[Any] = None,
                             incoming_timestamp: Optional[float] = None) -> bool:
        if incoming_op.lower() not in ("insert", "update"):
            return False
        ident = self._key_ident(table, key)
        t = self._tombstones.get(ident)
        if not t:
            return False
        if self.is_tombstoned(table, key, incoming_version, incoming_timestamp):
            return True
        del self._tombstones[ident]
        if self.storage_path:
            self.save()
        return False

    def remove(self, table: str, key: Dict[str, Any]) -> bool:
        ident = self._key_ident(table, key)
        if ident in self._tombstones:
            del self._tombstones[ident]
            if self.storage_path:
                self.save()
            return True
        return False

    def get(self, table: str, key: Dict[str, Any]) -> Optional[Tombstone]:
        return self._tombstones.get(self._key_ident(table, key))

    def count(self) -> int:
        return len(self._tombstones)

    def prune(self, max_age_seconds: Optional[float] = None,
              before_timestamp: Optional[float] = None) -> int:
        now = time.time()
        threshold = before_timestamp if before_timestamp is not None else (
            (now - max_age_seconds) if max_age_seconds is not None else None
        )
        if threshold is None:
            return 0
        to_del = [k for k, t in self._tombstones.items() if t.timestamp < threshold]
        for k in to_del:
            del self._tombstones[k]
        if to_del and self.storage_path:
            self.save()
        return len(to_del)

    def settle_batch_events(self, table: str, key: Dict[str, Any], events: List[Tuple[int, str]],
                            changes: List[Dict[str, Any]]) -> Dict[str, Any]:
        has_delete = False
        has_reinsert = False
        final_values = {}
        for idx, role in events:
            c = changes[idx]
            op = c.get("op")
            if role == "gone" or op == "delete":
                has_delete = True
            elif role == "came" or op == "insert":
                if has_delete:
                    has_reinsert = True
                final_values = dict(c.get("values") or {})
            elif op == "update":
                final_values.update(c.get("values") or {})

        if has_delete and not has_reinsert:
            self.record(table, key, deleted_by="batch")
            return {"status": "deleted", "tombstoned": True}
        if has_delete and has_reinsert:
            self.remove(table, key)
            return {"status": "reinserted", "tombstoned": False, "values": final_values}
        return {"status": "unchanged", "tombstoned": False, "values": final_values}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tombstones": [t.to_dict() for t in self._tombstones.values()]
        }

    def from_dict(self, data: Dict[str, Any]) -> None:
        self._tombstones.clear()
        for item in data.get("tombstones", []):
            t = Tombstone.from_dict(item)
            self._tombstones[self._key_ident(t.table, t.key)] = t

    def save(self, path: Optional[str] = None) -> None:
        p = path or self.storage_path
        if not p:
            return
        with open(p, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)

    def load(self, path: Optional[str] = None) -> None:
        p = path or self.storage_path
        if not p:
            return
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.from_dict(data)


class NeutralMark:
    MARKER_KEY = "_migkit_marker"
    ORIGIN_TABLE = "migkit_origin"
    KAFKA_HEADER = "migkit-marker"
    WRITER_ID = "migkit"

    @classmethod
    def format_marker(cls, engine: str, batch: Optional[int] = None,
                      token: Optional[str] = None, run_id: Optional[str] = None,
                      writer: str = "migkit", metadata: Optional[Dict[str, Any]] = None) -> Any:
        eng = engine.lower()
        meta = {
            "writer": writer,
            "batch": batch,
            "token": str(token) if token is not None else "",
            "run_id": str(run_id) if run_id is not None else "",
            "ts": time.time(),
            **(metadata or {}),
        }
        if eng == "mongodb":
            return {cls.MARKER_KEY: meta}
        if eng == "cassandra":
            tok_str = str(token or "")
            run_str = str(run_id or "")
            batch_int = batch or 0
            return (
                f"INSERT INTO {cls.ORIGIN_TABLE} (origin, batch, token, run_id, written_at) "
                f"VALUES ('{writer}', {batch_int}, '{tok_str}', '{run_str}', toTimestamp(now()));"
            )
        if eng == "redis":
            return (
                "HSET", f"migkit:origin",
                "writer", writer,
                "batch", str(batch or 0),
                "token", str(token or ""),
                "run_id", str(run_id or ""),
            )
        if eng == "dynamodb":
            return {
                "Put": {
                    "TableName": cls.ORIGIN_TABLE,
                    "Item": {
                        "origin": {"S": writer},
                        "batch": {"N": str(batch or 0)},
                        "token": {"S": str(token or "")},
                        "run_id": {"S": str(run_id or "")},
                    },
                }
            }
        if eng == "kafka":
            return [
                (cls.KAFKA_HEADER, json.dumps(meta).encode("utf-8")),
                ("migkit-writer", writer.encode("utf-8")),
            ]
        if eng in ("postgres", "postgresql"):
            return f"SELECT pg_logical_emit_message(true, '{writer}', '{json.dumps(meta)}');"
        if eng in ("mysql", "mariadb"):
            return f"/* {writer}:batch={batch or 0}:run={run_id or ''} */"
        if eng in ("mssql", "sqlserver"):
            return f"EXEC sp_set_session_context @key=N'{writer}_writer', @value=N'{writer}';"
        return {cls.MARKER_KEY: meta}

    @classmethod
    def is_migkit_write(cls, engine: str, payload: Any, writer: str = "migkit") -> bool:
        if payload is None:
            return False
        eng = engine.lower()
        if eng == "mongodb":
            if isinstance(payload, dict):
                m = payload.get(cls.MARKER_KEY)
                if isinstance(m, dict) and m.get("writer") == writer:
                    return True
                return payload.get("_writer") == writer
            return False
        if eng == "cassandra":
            s = str(payload)
            return cls.ORIGIN_TABLE in s and writer in s
        if eng == "redis":
            if isinstance(payload, (list, tuple)):
                s = " ".join(str(x) for x in payload)
                return "migkit:origin" in s or writer in s
            return writer in str(payload)
        if eng == "dynamodb":
            if isinstance(payload, dict):
                item = payload.get("Put", {}).get("Item", {})
                return item.get("origin", {}).get("S") == writer
            return False
        if eng == "kafka":
            headers = payload
            if isinstance(headers, dict):
                headers = list(headers.items())
            if isinstance(headers, (list, tuple)):
                for k, v in headers:
                    k_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                    if k_str in (cls.KAFKA_HEADER, "migkit-writer"):
                        v_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                        if writer in v_str:
                            return True
            return False
        if eng in ("postgres", "postgresql"):
            s = str(payload)
            return f"'{writer}'" in s or cls.ORIGIN_TABLE in s or "pg_logical_emit_message" in s
        if eng in ("mysql", "mariadb"):
            s = str(payload)
            return f"/* {writer}:" in s or f"/* migkit:" in s or cls.ORIGIN_TABLE in s
        if eng in ("mssql", "sqlserver"):
            s = str(payload)
            return f"{writer}_writer" in s or cls.ORIGIN_TABLE in s
        return writer in str(payload)

    @classmethod
    def inject_mark(cls, engine: str, atomic_unit: List[Any], batch: Optional[int] = None,
                    token: Optional[str] = None, run_id: Optional[str] = None,
                    writer: str = "migkit") -> List[Any]:
        marker = cls.format_marker(engine, batch=batch, token=token, run_id=run_id, writer=writer)
        if engine.lower() == "kafka":
            return [(marker, item) for item in atomic_unit]
        return [marker] + list(atomic_unit)

    @classmethod
    def extract_marker(cls, engine: str, atomic_unit: Any) -> Optional[Dict[str, Any]]:
        eng = engine.lower()
        if eng == "mongodb":
            if isinstance(atomic_unit, list) and atomic_unit:
                first = atomic_unit[0]
                if isinstance(first, dict) and cls.MARKER_KEY in first:
                    return first[cls.MARKER_KEY]
            elif isinstance(atomic_unit, dict) and cls.MARKER_KEY in atomic_unit:
                return atomic_unit[cls.MARKER_KEY]
        elif eng == "kafka":
            headers = atomic_unit
            if isinstance(headers, (list, tuple)):
                for k, v in headers:
                    k_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                    if k_str == cls.KAFKA_HEADER:
                        raw = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                        try:
                            return json.loads(raw)
                        except Exception:
                            return {"writer": raw}
        return None


@dataclasses.dataclass(frozen=True)
class SequenceTag:
    epoch: int
    batch_num: int
    run_id: str = ""
    token: str = ""
    checksum: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "epoch": self.epoch,
            "batch_num": self.batch_num,
            "run_id": self.run_id,
            "token": self.token,
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SequenceTag":
        return cls(
            epoch=int(data.get("epoch", 0)),
            batch_num=int(data["batch_num"]),
            run_id=str(data.get("run_id", "")),
            token=str(data.get("token", "")),
            checksum=str(data.get("checksum", "")),
        )


class BatchSequenceLedger:
    def __init__(self, storage_path: Optional[str] = None):
        self._committed_highest: Dict[int, int] = {}
        self._seen_tags: Set[Tuple[int, int, str]] = set()
        self.storage_path = storage_path

    def is_replay(self, tag: SequenceTag) -> bool:
        if (tag.epoch, tag.batch_num, tag.run_id) in self._seen_tags:
            return True
        last = self._committed_highest.get(tag.epoch, -1)
        if tag.batch_num <= last:
            return True
        return False

    def commit_batch(self, tag: SequenceTag) -> None:
        if self.is_replay(tag):
            raise ValueError(
                f"BAR-2 replay rejected: batch {tag.batch_num} in epoch {tag.epoch} already committed"
            )
        self._seen_tags.add((tag.epoch, tag.batch_num, tag.run_id))
        self._committed_highest[tag.epoch] = max(
            self._committed_highest.get(tag.epoch, -1), tag.batch_num
        )
        if self.storage_path:
            self.save()

    def last_committed(self, epoch: int = 0) -> int:
        return self._committed_highest.get(epoch, -1)

    def check_and_advance(self, tag: SequenceTag, strict_monotonic: bool = True) -> bool:
        if self.is_replay(tag):
            return False
        last = self._committed_highest.get(tag.epoch, -1)
        if strict_monotonic and tag.batch_num != last + 1:
            raise ValueError(
                f"BAR-2 sequence gap: expected batch {last + 1}, got {tag.batch_num} (epoch {tag.epoch})"
            )
        self.commit_batch(tag)
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "committed": {str(k): v for k, v in self._committed_highest.items()},
            "seen": [list(x) for x in self._seen_tags],
        }

    def from_dict(self, data: Dict[str, Any]) -> None:
        self._committed_highest = {int(k): v for k, v in data.get("committed", {}).items()}
        self._seen_tags = {tuple(x) for x in data.get("seen", [])}

    def save(self, path: Optional[str] = None) -> None:
        p = path or self.storage_path
        if not p:
            return
        with open(p, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)

    def load(self, path: Optional[str] = None) -> None:
        p = path or self.storage_path
        if not p:
            return
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.from_dict(data)


class TargetWriteAttribution:
    @classmethod
    def attribute_write(cls, engine: str, write_event: Any, migkit_account: Optional[str] = None) -> str:
        if NeutralMark.is_migkit_write(engine, write_event):
            return "migkit"
        if isinstance(write_event, dict):
            user = write_event.get("user") or write_event.get("account")
            if user and migkit_account and user == migkit_account:
                return "migkit"
            if write_event.get("writer") == "migkit" or write_event.get("provenance") == "migkit":
                return "migkit"
        return "application"

    @classmethod
    def audit_target_writes(cls, engine: str, writes: List[Any],
                            migkit_account: Optional[str] = None,
                            protected: bool = True) -> Dict[str, Any]:
        migkit_writes = []
        application_writes = []
        for w in writes:
            attr = cls.attribute_write(engine, w, migkit_account=migkit_account)
            if attr == "migkit":
                migkit_writes.append(w)
            else:
                application_writes.append(w)

        drift = protected and len(application_writes) > 0
        return {
            "engine": engine,
            "protected": protected,
            "migkit_count": len(migkit_writes),
            "application_count": len(application_writes),
            "drift_detected": drift,
            "application_writes": application_writes,
        }

    @staticmethod
    def is_target_protected(hop: Any) -> bool:
        if hop is None:
            return False
        opts = getattr(hop, "options", {}) or {}
        return bool(opts.get("protect_target"))


class MarkLadder:
    @staticmethod
    def choose_rung(side_engine, facts):
        from . import marks
        return marks.choose_rung(side_engine, facts)

    @staticmethod
    def get_cost(family: str, rung_name: str) -> float:
        from . import marks
        return marks.cost_us(family, rung_name)

    @staticmethod
    def doctor_said(side_engine, token_path, db):
        from . import marks
        return marks.said(side_engine, token_path, db)


from .dag_scheduler import BarrierFence, BarrierType, TransactionDAGScheduler

