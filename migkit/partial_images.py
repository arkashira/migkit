from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

from migkit.verifiers import lsn_compare

UNCHANGED_TOAST = "unchanged-toast-datum"
KNOWN_PLACEHOLDERS = {
    UNCHANGED_TOAST,
    "__debezium_unavailable_value",
    "__debezium_unchanged_toast_value",
}


class PartialImageError(RuntimeError):
    pass


class StaleRowVersionError(PartialImageError):
    pass


class MissingBeforeImageError(PartialImageError):
    pass


class PlaceholderLeakError(PartialImageError):
    pass


class IncompleteRowImageError(PartialImageError):
    pass


class FieldState(str, Enum):
    ABSENT = "absent"
    UNCHANGED = "unchanged"
    NULL = "null"
    VALUE = "value"


@dataclass(frozen=True)
class PartialFieldValue:
    state: FieldState
    value: Any = None
    declared_type: Optional[str] = None
    is_toast: bool = False

    @property
    def is_absent(self) -> bool:
        return self.state == FieldState.ABSENT

    @property
    def is_unchanged(self) -> bool:
        return self.state == FieldState.UNCHANGED

    @property
    def is_null(self) -> bool:
        return self.state == FieldState.NULL

    @property
    def has_value(self) -> bool:
        return self.state == FieldState.VALUE

    @property
    def is_concrete(self) -> bool:
        return self.state in (FieldState.VALUE, FieldState.NULL)

    def unwrap(self, default: Any = None) -> Any:
        if self.state == FieldState.VALUE:
            return self.value
        if self.state == FieldState.NULL:
            return None
        return default

    @classmethod
    def absent(cls, declared_type: Optional[str] = None) -> "PartialFieldValue":
        return cls(state=FieldState.ABSENT, value=None, declared_type=declared_type, is_toast=False)

    @classmethod
    def unchanged(cls, declared_type: Optional[str] = None, is_toast: bool = True) -> "PartialFieldValue":
        return cls(state=FieldState.UNCHANGED, value=None, declared_type=declared_type, is_toast=is_toast)

    @classmethod
    def null(cls, declared_type: Optional[str] = None) -> "PartialFieldValue":
        return cls(state=FieldState.NULL, value=None, declared_type=declared_type, is_toast=False)

    @classmethod
    def value(cls, val: Any, declared_type: Optional[str] = None) -> "PartialFieldValue":
        if isinstance(val, str) and val in KNOWN_PLACEHOLDERS:
            raise PlaceholderLeakError(f"refusing placeholder {val!r} as concrete value")
        return cls(state=FieldState.VALUE, value=val, declared_type=declared_type, is_toast=False)

    @classmethod
    def from_raw(
        cls,
        raw: Any,
        quoted: bool = True,
        declared_type: Optional[str] = None,
        placeholders: Optional[Set[str]] = None,
    ) -> "PartialFieldValue":
        if isinstance(raw, PartialFieldValue):
            return raw
        if raw is None or (not quoted and raw == "null"):
            return cls.null(declared_type=declared_type)
        ph = KNOWN_PLACEHOLDERS if placeholders is None else placeholders
        if not quoted and isinstance(raw, str) and raw in ph:
            return cls.unchanged(declared_type=declared_type, is_toast=True)
        return cls.value(raw, declared_type=declared_type)


@dataclass
class PartialRowImage:
    table: str
    key: Dict[str, Any]
    fields: Dict[str, PartialFieldValue] = field(default_factory=dict)
    op: str = "update"
    version: Optional[Any] = None
    old_key: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __getitem__(self, name: str) -> PartialFieldValue:
        return self.fields[name]

    def __setitem__(self, name: str, val: Union[PartialFieldValue, Any]) -> None:
        if isinstance(val, PartialFieldValue):
            self.fields[name] = val
        elif val is None:
            self.fields[name] = PartialFieldValue.null()
        else:
            self.fields[name] = PartialFieldValue.value(val)

    def __contains__(self, name: str) -> bool:
        return name in self.fields

    def get(self, name: str, default: Any = None) -> Any:
        if name not in self.fields:
            return default
        return self.fields[name].unwrap(default=default)

    def get_field(self, name: str) -> Optional[PartialFieldValue]:
        return self.fields.get(name)

    def is_unchanged(self, name: str) -> bool:
        f = self.fields.get(name)
        return bool(f and f.is_unchanged)

    def is_null(self, name: str) -> bool:
        f = self.fields.get(name)
        return bool(f and f.is_null)

    def is_absent(self, name: str) -> bool:
        f = self.fields.get(name)
        return f is None or f.is_absent

    def has_concrete_value(self, name: str) -> bool:
        f = self.fields.get(name)
        return bool(f and f.has_value)

    @property
    def unchanged_columns(self) -> List[str]:
        return sorted([k for k, v in self.fields.items() if v.is_unchanged])

    @property
    def null_columns(self) -> List[str]:
        return sorted([k for k, v in self.fields.items() if v.is_null])

    @property
    def concrete_values(self) -> Dict[str, Any]:
        return {k: v.unwrap() for k, v in self.fields.items() if v.is_concrete}

    def has_partial(self) -> bool:
        return any(v.is_unchanged or v.is_absent for v in self.fields.values())

    def apply_before_image(self, before: Union[Dict[str, Any], "PartialRowImage"]) -> "PartialRowImage":
        before_vals = before.fields if isinstance(before, PartialRowImage) else before
        new_fields = {}
        for k, v in self.fields.items():
            if v.is_unchanged or v.is_absent:
                if k not in before_vals:
                    raise MissingBeforeImageError(f"{self.table}: column {k!r} is unchanged but absent in before-image")
                b_val = before_vals[k]
                if isinstance(b_val, PartialFieldValue):
                    new_fields[k] = b_val
                elif b_val is None:
                    new_fields[k] = PartialFieldValue.null()
                else:
                    new_fields[k] = PartialFieldValue.value(b_val)
            else:
                new_fields[k] = v

        if isinstance(before_vals, dict):
            for bk, bv in before_vals.items():
                if bk not in new_fields:
                    if isinstance(bv, PartialFieldValue):
                        new_fields[bk] = bv
                    elif bv is None:
                        new_fields[bk] = PartialFieldValue.null()
                    else:
                        new_fields[bk] = PartialFieldValue.value(bv)

        return PartialRowImage(
            table=self.table,
            key=dict(self.key),
            fields=new_fields,
            op=self.op,
            version=self.version,
            old_key=dict(self.old_key) if self.old_key else None,
            metadata=dict(self.metadata),
        )

    def to_canon_change(self) -> Dict[str, Any]:
        out = {
            "op": self.op,
            "table": self.table,
            "key": dict(self.key),
            "values": self.concrete_values,
        }
        unchanged = self.unchanged_columns
        if unchanged:
            out["unchanged"] = unchanged
        if self.old_key:
            out["old_key"] = dict(self.old_key)
        return out

    @classmethod
    def from_canon_change(
        cls,
        c: Dict[str, Any],
        all_columns: Optional[Iterable[str]] = None,
        version: Optional[Any] = None,
    ) -> "PartialRowImage":
        table = c["table"]
        key = dict(c.get("key", {}))
        op = c.get("op", "update")
        values = c.get("values", {})
        unchanged = set(c.get("unchanged", []))
        fields = {}

        for k, v in values.items():
            if k in unchanged:
                fields[k] = PartialFieldValue.unchanged()
            elif v is None:
                fields[k] = PartialFieldValue.null()
            elif isinstance(v, str) and v in KNOWN_PLACEHOLDERS:
                fields[k] = PartialFieldValue.unchanged()
            else:
                fields[k] = PartialFieldValue.value(v)

        for k in unchanged:
            if k not in fields:
                fields[k] = PartialFieldValue.unchanged()

        if all_columns:
            for k in all_columns:
                if k not in fields:
                    fields[k] = PartialFieldValue.absent()

        return cls(
            table=table,
            key=key,
            fields=fields,
            op=op,
            version=version or c.get("version") or c.get("txn_at"),
            old_key=c.get("old_key"),
        )


@dataclass
class CachedRowEntry:
    table: str
    key: Dict[str, Any]
    values: Dict[str, Any]
    version: Any
    updated_at: float
    byte_size: int = 0


def _estimate_size(val: Any) -> int:
    if val is None:
        return 0
    if isinstance(val, (bytes, bytearray, memoryview)):
        return len(val)
    if isinstance(val, str):
        return len(val.encode("utf-8"))
    if isinstance(val, (int, float, bool)):
        return 8
    if isinstance(val, dict):
        return sum(len(str(k)) + _estimate_size(v) for k, v in val.items())
    if isinstance(val, (list, tuple, set)):
        return sum(_estimate_size(x) for x in val)
    return 16


def compare_versions(a: Any, b: Any, engine: str = "postgres") -> int:
    if a is None and b is None:
        return 0
    if a is None:
        return -1
    if b is None:
        return 1
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return -1 if a < b else (1 if a > b else 0)
    str_a, str_b = str(a).strip(), str(b).strip()
    if "/" in str_a and "/" in str_b:
        return lsn_compare(str_a, str_b)
    if str_a.isdigit() and str_b.isdigit():
        ia, ib = int(str_a), int(str_b)
        return -1 if ia < ib else (1 if ia > ib else 0)
    if str_a < str_b:
        return -1
    if str_a > str_b:
        return 1
    return 0


class RowVersionCache:
    def __init__(
        self,
        max_size: int = 10000,
        max_bytes: Optional[int] = None,
        max_history_per_key: int = 512,
        refetch_fn: Optional[Callable[[str, Dict[str, Any], Optional[Any]], Optional[Dict[str, Any]]]] = None,
        engine: str = "postgres",
    ):
        self.max_size = max_size
        self.max_bytes = max_bytes
        self.max_history_per_key = max_history_per_key
        self.refetch_fn = refetch_fn
        self.engine = engine
        self._cache: OrderedDict[Tuple[str, Tuple[Tuple[str, Any], ...]], List[CachedRowEntry]] = OrderedDict()
        self._current_bytes = 0
        self._lock = threading.RLock()

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._cache)

    @property
    def bytes_used(self) -> int:
        with self._lock:
            return self._current_bytes

    def compare_versions(self, a: Any, b: Any) -> int:
        return compare_versions(a, b, engine=self.engine)

    def _cache_key(self, table: str, key: Dict[str, Any]) -> Tuple[str, Tuple[Tuple[str, Any], ...]]:
        return (str(table), tuple(sorted(key.items())))

    def _call_refetch(self, table: str, key: Dict[str, Any], version: Optional[Any]) -> Optional[Dict[str, Any]]:
        if self.refetch_fn is None:
            return None
        row = self.refetch_fn(table, key, version)
        if row is None:
            return None
        for col, v in row.items():
            if isinstance(v, str) and v in KNOWN_PLACEHOLDERS:
                raise PlaceholderLeakError(f"Refetched row contains placeholder {v!r} in column {col!r}")
        return dict(row)

    def _put_locked(self, table: str, key: Dict[str, Any], values: Dict[str, Any], version: Any) -> CachedRowEntry:
        ckey = self._cache_key(table, key)
        entry_size = sum(_estimate_size(v) for v in values.values())
        entry = CachedRowEntry(
            table=str(table),
            key=dict(key),
            values=dict(values),
            version=version,
            updated_at=time.time(),
            byte_size=entry_size,
        )

        if ckey not in self._cache:
            self._cache[ckey] = [entry]
            self._current_bytes += entry_size
        else:
            history = self._cache[ckey]
            self._cache.move_to_end(ckey)
            inserted = False
            for idx, existing in enumerate(history):
                cmp = self.compare_versions(version, existing.version)
                if cmp == 0:
                    self._current_bytes -= existing.byte_size
                    history[idx] = entry
                    self._current_bytes += entry_size
                    inserted = True
                    break
                elif cmp < 0:
                    history.insert(idx, entry)
                    self._current_bytes += entry_size
                    inserted = True
                    break
            if not inserted:
                history.append(entry)
                self._current_bytes += entry_size

            while len(history) > self.max_history_per_key:
                oldest = history.pop(0)
                self._current_bytes -= oldest.byte_size

        while self.max_size and len(self._cache) > self.max_size:
            _, evicted_history = self._cache.popitem(last=False)
            for ev in evicted_history:
                self._current_bytes -= ev.byte_size

        while self.max_bytes and self._current_bytes > self.max_bytes and self._cache:
            _, evicted_history = self._cache.popitem(last=False)
            for ev in evicted_history:
                self._current_bytes -= ev.byte_size

        return entry

    def _invalidate_locked(self, table: str, key: Dict[str, Any]) -> None:
        ckey = self._cache_key(table, key)
        if ckey in self._cache:
            evicted_history = self._cache.pop(ckey)
            for ev in evicted_history:
                self._current_bytes -= ev.byte_size

    def put(self, table: str, key: Dict[str, Any], values: Dict[str, Any], version: Any) -> None:
        with self._lock:
            self._put_locked(table, key, values, version)

    def get(self, table: str, key: Dict[str, Any], version: Optional[Any] = None) -> Optional[CachedRowEntry]:
        with self._lock:
            ckey = self._cache_key(table, key)
            if ckey not in self._cache:
                return None
            self._cache.move_to_end(ckey)
            history = self._cache[ckey]
            if not history:
                return None
            if version is None:
                return history[-1]
            for entry in reversed(history):
                if self.compare_versions(entry.version, version) <= 0:
                    return entry
            return None

    def invalidate(self, table: str, key: Dict[str, Any]) -> None:
        with self._lock:
            self._invalidate_locked(table, key)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._current_bytes = 0

    def resolve_insert(self, image: PartialRowImage) -> PartialRowImage:
        with self._lock:
            if image.has_partial():
                raise IncompleteRowImageError(f"{image.table}: INSERT cannot contain unchanged or absent fields")
            vals = image.concrete_values
            self._put_locked(image.table, image.key, vals, image.version)
            return image

    def resolve_delete(self, image: PartialRowImage) -> PartialRowImage:
        with self._lock:
            self._invalidate_locked(image.table, image.key)
            return image

    def resolve_update(self, image: PartialRowImage) -> PartialRowImage:
        with self._lock:
            ckey = self._cache_key(image.table, image.key)
            history = self._cache.get(ckey)

            if not image.has_partial():
                vals = image.concrete_values
                self._put_locked(image.table, image.key, vals, image.version)
                return image

            before_vals: Optional[Dict[str, Any]] = None
            if history:
                self._cache.move_to_end(ckey)
                if image.version is None:
                    before_vals = history[-1].values
                else:
                    candidate = None
                    for entry in reversed(history):
                        if self.compare_versions(entry.version, image.version) <= 0:
                            candidate = entry
                            break
                    if candidate is not None:
                        before_vals = candidate.values
                    else:
                        if self.refetch_fn is not None:
                            refetched = self._call_refetch(image.table, image.key, image.version)
                            if refetched is None:
                                raise StaleRowVersionError(
                                    f"Cached versions ({history[0].version}..{history[-1].version}) are ahead of update {image.version} and refetch returned nothing"
                                )
                            before_vals = refetched
                        else:
                            raise StaleRowVersionError(
                                f"Cached version {history[0].version} is ahead of incoming update version {image.version}"
                            )

            if before_vals is None:
                if self.refetch_fn is not None:
                    refetched = self._call_refetch(image.table, image.key, image.version)
                    if refetched is None:
                        raise MissingBeforeImageError(
                            f"Missing before-image for {image.table} {image.key} and refetch failed"
                        )
                    before_vals = refetched
                else:
                    raise MissingBeforeImageError(
                        f"Missing before-image for {image.table} {image.key} and no refetch handler registered"
                    )

            reconstructed = image.apply_before_image(before_vals)
            new_vals = reconstructed.concrete_values
            self._put_locked(image.table, image.key, new_vals, image.version)
            return reconstructed

    def resolve(self, image: PartialRowImage) -> PartialRowImage:
        if image.op == "delete":
            return self.resolve_delete(image)
        if image.op == "insert":
            return self.resolve_insert(image)
        return self.resolve_update(image)

    def export_state(self) -> List[Dict[str, Any]]:
        with self._lock:
            out = []
            for history in self._cache.values():
                for entry in history:
                    out.append({
                        "table": entry.table,
                        "key": entry.key,
                        "values": entry.values,
                        "version": entry.version,
                        "updated_at": entry.updated_at,
                    })
            return out

    def import_state(self, data: List[Dict[str, Any]]) -> None:
        with self._lock:
            for item in data:
                self._put_locked(
                    table=item["table"],
                    key=item["key"],
                    values=item["values"],
                    version=item.get("version"),
                )
