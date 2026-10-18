import hashlib
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from . import checkpoint as cp_mod
from . import decide
from . import durable
from . import rowfilter


class IncompatibleResumeScopeError(ValueError):
    pass


class DurableUnitIdentity:
    def __init__(
        self,
        table: str,
        lo: Any = None,
        hi: Any = None,
        chunk_id: Optional[Union[str, int]] = None,
        filter_predicate: Optional[str] = None,
        column_mapping: Optional[Dict[str, Any]] = None,
        tenant_predicate: Optional[Union[str, Dict[str, Any]]] = None,
        scope_identity: Optional[str] = None,
    ):
        self.table = str(table)
        self.lo = lo
        self.hi = hi
        self.chunk_id = chunk_id
        self.filter_predicate = str(filter_predicate or "").strip() if filter_predicate else None
        self.column_mapping = {str(k): v for k, v in sorted((column_mapping or {}).items())}
        self.tenant_predicate = tenant_predicate
        self.scope_identity = str(scope_identity or "")

    @property
    def range_key(self) -> str:
        if self.chunk_id is not None:
            return f"chunk:{self.chunk_id}"
        return cp_mod._key(self.lo, self.hi)

    def digest(self) -> str:
        payload = json.dumps(
            [
                self.table,
                self.range_key,
                self.filter_predicate or "",
                self.column_mapping,
                str(self.tenant_predicate or "").strip(),
                self.scope_identity,
            ],
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    @property
    def unit_id(self) -> str:
        return f"{self.table}::{self.range_key}::{self.digest()}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "range_id": self.range_key,
            "cursor": {
                "lo": cp_mod.serialize_cursor(self.lo),
                "hi": cp_mod.serialize_cursor(self.hi),
            },
            "chunk_id": self.chunk_id,
            "filter_predicate": self.filter_predicate,
            "column_mapping": self.column_mapping,
            "tenant_predicate": self.tenant_predicate,
            "scope_identity": self.scope_identity,
            "digest": self.digest(),
            "unit_id": self.unit_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DurableUnitIdentity":
        cur = data.get("cursor") or {}
        return cls(
            table=data.get("table", ""),
            lo=cp_mod.deserialize_cursor(cur.get("lo")),
            hi=cp_mod.deserialize_cursor(cur.get("hi")),
            chunk_id=data.get("chunk_id"),
            filter_predicate=data.get("filter_predicate"),
            column_mapping=data.get("column_mapping"),
            tenant_predicate=data.get("tenant_predicate"),
            scope_identity=data.get("scope_identity"),
        )

    def __str__(self) -> str:
        return self.unit_id

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DurableUnitIdentity):
            return self.unit_id == other.unit_id
        if isinstance(other, str):
            return self.unit_id == other
        return False

    def __hash__(self) -> int:
        return hash(self.unit_id)


def resume_scope_compatibility_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder evaluating resume compatibility against stored checkpoint scope."""
    return (
        decide.Rung(
            "admit_identical_scope",
            "admit resume into checkpoint with matching scope, filter, and mappings",
            needs=(
                decide.Need(
                    "table scope matches stored header",
                    lambda f: bool(f.get("tables_match", True)),
                    lambda f: f"table scope mismatch: expected {sorted(f.get('expected_tables', []))}, found {sorted(f.get('stored_tables', []))}",
                ),
                decide.Need(
                    "filter predicate matches stored header",
                    lambda f: bool(f.get("filter_match", True)),
                    lambda f: f"filter predicate mismatch: expected {f.get('expected_filter')!r}, found {f.get('stored_filter')!r}",
                ),
                decide.Need(
                    "column mappings match stored header",
                    lambda f: bool(f.get("mapping_match", True)),
                    lambda f: f"column mapping mismatch: expected {f.get('expected_mapping')!r}, found {f.get('stored_mapping')!r}",
                ),
                decide.Need(
                    "tenant predicate matches stored header",
                    lambda f: bool(f.get("tenant_match", True)),
                    lambda f: f"tenant predicate mismatch: expected {f.get('expected_tenant')!r}, found {f.get('stored_tenant')!r}",
                ),
                decide.Need(
                    "scope identity matches stored header",
                    lambda f: bool(f.get("identity_match", True)),
                    lambda f: f"scope identity mismatch: expected {f.get('expected_identity')!r}, found {f.get('stored_identity')!r}",
                ),
            ),
            footprint=0,
            leaves="reused partials and durable unit identities",
        ),
        decide.Rung(
            "refuse_incompatible_scope",
            "refuse resume and discard stored partials due to scope or filter drift",
            footprint=1,
            leaves="fresh checkpoint on new scope identity",
        ),
    )


def decide_scope_compatibility(stored_header: Dict[str, Any], current_scope: "ResumeScope") -> decide.Choice:
    """Climb the resume_scope_compatibility_rungs ladder."""
    stored_tables = set(stored_header.get("tables") or [])
    expected_tables = set(current_scope.tables)
    stored_filter = str(stored_header.get("filter_predicate") or "").strip()
    expected_filter = str(current_scope.filter_predicate or "").strip()
    stored_mapping = {str(k): v for k, v in sorted((stored_header.get("column_mapping") or {}).items())}
    expected_mapping = current_scope.column_mapping
    stored_tenant = stored_header.get("tenant_predicate") or ""
    expected_tenant = current_scope.tenant_predicate or ""
    stored_identity = stored_header.get("scope_identity") or ""
    expected_identity = current_scope.scope_identity or ""

    facts = decide.Facts({
        "tables_match": stored_tables == expected_tables,
        "stored_tables": list(stored_tables),
        "expected_tables": list(expected_tables),
        "filter_match": stored_filter == expected_filter,
        "stored_filter": stored_filter,
        "expected_filter": expected_filter,
        "mapping_match": stored_mapping == expected_mapping,
        "stored_mapping": stored_mapping,
        "expected_mapping": expected_mapping,
        "tenant_match": stored_tenant == expected_tenant,
        "stored_tenant": stored_tenant,
        "expected_tenant": expected_tenant,
        "identity_match": not stored_identity or stored_identity == expected_identity,
        "stored_identity": stored_identity,
        "expected_identity": expected_identity,
    })
    return decide.climb("resume scope compatibility", resume_scope_compatibility_rungs(), facts)


class ResumeScope:
    def __init__(
        self,
        tables: Union[str, List[str], Set[str], Tuple[str, ...]],
        filter_predicate: Optional[str] = None,
        column_mapping: Optional[Dict[str, Any]] = None,
        tenant_predicate: Optional[Union[str, Dict[str, Any]]] = None,
        scope_id: Optional[str] = None,
        dialect: str = "sqlite",
    ):
        if isinstance(tables, str):
            self.tables = (tables,)
        else:
            self.tables = tuple(sorted(str(t) for t in tables))

        self.filter_predicate = str(filter_predicate).strip() if filter_predicate else None
        self.column_mapping = {str(k): v for k, v in sorted((column_mapping or {}).items())}
        self.tenant_predicate = tenant_predicate
        self.dialect = dialect
        self._scope_id = str(scope_id) if scope_id else None

    @property
    def scope_identity(self) -> str:
        if self._scope_id:
            return self._scope_id
        payload = json.dumps(
            {
                "tables": list(self.tables),
                "filter_predicate": self.filter_predicate or "",
                "column_mapping": self.column_mapping,
                "tenant_predicate": self.tenant_predicate or "",
            },
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]

    def header(self) -> Dict[str, Any]:
        return {
            "format_version": 1,
            "scope_identity": self.scope_identity,
            "tables": list(self.tables),
            "filter_predicate": self.filter_predicate,
            "column_mapping": self.column_mapping,
            "tenant_predicate": self.tenant_predicate,
        }

    def validate_compatibility(self, header_or_scope: Union[Dict[str, Any], "ResumeScope"]) -> bool:
        hdr = header_or_scope.header() if isinstance(header_or_scope, ResumeScope) else header_or_scope
        if not isinstance(hdr, dict):
            raise IncompatibleResumeScopeError(f"invalid checkpoint scope header: {hdr!r}")

        choice = decide_scope_compatibility(hdr, self)
        if choice.rung.name != "admit_identical_scope":
            raise IncompatibleResumeScopeError(choice.reason or choice.rung.because)

        return True

    def unit_identity(
        self,
        table: str,
        lo: Any = None,
        hi: Any = None,
        chunk_id: Optional[Union[str, int]] = None,
    ) -> DurableUnitIdentity:
        if table not in self.tables:
            raise IncompatibleResumeScopeError(f"table {table!r} not in resume scope {self.tables!r}")
        return DurableUnitIdentity(
            table=table,
            lo=lo,
            hi=hi,
            chunk_id=chunk_id,
            filter_predicate=self.filter_predicate,
            column_mapping=self.column_mapping,
            tenant_predicate=self.tenant_predicate,
            scope_identity=self.scope_identity,
        )

    def is_row_in_scope(self, row: Dict[str, Any]) -> bool:
        if self.tenant_predicate:
            if isinstance(self.tenant_predicate, dict):
                for k, v in self.tenant_predicate.items():
                    if row.get(k) != v:
                        return False
            elif isinstance(self.tenant_predicate, str):
                if not rowfilter.evaluate(self.tenant_predicate, self.dialect, row):
                    return False

        if self.filter_predicate:
            if callable(self.filter_predicate):
                if not self.filter_predicate(row):
                    return False
            elif isinstance(self.filter_predicate, str):
                if not rowfilter.evaluate(self.filter_predicate, self.dialect, row):
                    return False

        return True

    def apply_mapping(self, row: Dict[str, Any]) -> Dict[str, Any]:
        if not self.column_mapping:
            return dict(row)

        if "rename" in self.column_mapping or "keep" in self.column_mapping or "drop" in self.column_mapping:
            renames = self.column_mapping.get("rename") or {}
            keep = set(self.column_mapping.get("keep") or [])
            drop = set(self.column_mapping.get("drop") or [])
            out = {}
            for k, v in row.items():
                if k in drop:
                    continue
                if keep and k not in keep:
                    continue
                out[renames.get(k, k)] = v
            return out

        return {dst: row[src] for src, dst in self.column_mapping.items() if src in row}

    def where_clause(
        self,
        column: Optional[str] = None,
        lo: Any = None,
        hi: Any = None,
        base_where: Optional[str] = None,
    ) -> str:
        clauses = []
        if base_where:
            clauses.append(f"({base_where})")
        if column and (lo is not None or hi is not None):
            clause = cp_mod.where(column, lo, hi)
            if clause:
                clauses.append(f"({clause})")
        if self.tenant_predicate and isinstance(self.tenant_predicate, str):
            clauses.append(f"({self.tenant_predicate})")
        if self.filter_predicate and isinstance(self.filter_predicate, str):
            clauses.append(f"({self.filter_predicate})")
        return " AND ".join(clauses)


class ScopedCheckpoint(cp_mod.Checkpoint):
    def __init__(self, path: Optional[Union[str, Path]], scope: ResumeScope):
        self.scope = scope
        self.path = str(path) if path else None
        self._lock = threading.Lock()
        self._data = {
            "format_version": cp_mod.FORMAT_VERSION,
            "scope_header": scope.header(),
            "tables": {},
            "units": {},
        }
        self._touched = set()

        if self.path:
            disk = durable.read_state(self.path, "cache")
            if isinstance(disk, dict):
                if disk.get("scope_header"):
                    self.scope.validate_compatibility(disk["scope_header"])
                    self._data["scope_header"] = disk["scope_header"]
                else:
                    existing_tables = set((disk.get("tables") or {}).keys())
                    if existing_tables and not existing_tables.issubset(set(self.scope.tables)):
                        raise IncompatibleResumeScopeError(
                            f"table scope mismatch: expected {sorted(self.scope.tables)}, found {sorted(existing_tables)}"
                        )
                if disk.get("format_version") == cp_mod.FORMAT_VERSION:
                    self._data["tables"] = disk.get("tables") or {}
                    self._data["units"] = disk.get("units") or {}
            self._flush()

    def raw_data(self) -> Dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._data))

    def begin(self, table: str, expr: str, ranges: List[Tuple[Any, Any]]) -> List[Tuple[Any, Any]]:
        if table not in self.scope.tables:
            raise IncompatibleResumeScopeError(f"table {table!r} not in scope {self.scope.tables!r}")

        fp = cp_mod.fingerprint(expr, ranges)
        with self._lock:
            self._touched.add(table)
            entry = self._data["tables"].get(table)
            if not entry:
                entry = {
                    "fingerprint": fp,
                    "done": {},
                    "units": {},
                    "chunk": None,
                }
                self._data["tables"][table] = entry
            elif not entry.get("fingerprint"):
                entry["fingerprint"] = fp
            elif entry.get("fingerprint") != fp:
                entry = {
                    "fingerprint": fp,
                    "done": {},
                    "units": {},
                    "chunk": entry.get("chunk"),
                }
                self._data["tables"][table] = entry
            done = entry.get("done") or {}
        return [(lo, hi) for lo, hi in ranges if cp_mod._key(lo, hi) not in done]

    def record(
        self,
        table: str,
        lo: Any,
        hi: Any,
        rows: int,
        checksum: Any,
        cursor: Any = None,
        chunk_id: Optional[Union[str, int]] = None,
    ):
        unit = self.scope.unit_identity(table, lo=lo, hi=hi, chunk_id=chunk_id)
        with self._lock:
            self._touched.add(table)
            entry = self._data["tables"].setdefault(
                table, {"fingerprint": "", "done": {}, "units": {}}
            )
            k = unit.range_key
            entry["done"][k] = [int(rows), str(checksum)]
            entry.setdefault("units", {})[k] = unit.to_dict()
            entry.setdefault("cursors", {})[k] = {
                "lo": cp_mod.serialize_cursor(lo),
                "hi": cp_mod.serialize_cursor(hi),
                "rows": int(rows),
                "checksum": str(checksum),
            }
            self._data.setdefault("units", {})[unit.unit_id] = {
                "unit": unit.to_dict(),
                "rows": int(rows),
                "checksum": str(checksum),
            }
            self._flush()

    def record_progress_journal(
        self,
        report_dir: Union[str, Path],
        table: str,
        lo: Any,
        hi: Any,
        rows: int,
        checksum: Any,
        chunk_id: Optional[Union[str, int]] = None,
    ):
        if not report_dir:
            return
        pf = Path(report_dir) / "check-progress.jsonl"
        pf.parent.mkdir(parents=True, exist_ok=True)
        unit = self.scope.unit_identity(table, lo=lo, hi=hi, chunk_id=chunk_id)
        rec = {
            "kind": "scoped_unit",
            "scope_identity": self.scope.scope_identity,
            "unit_id": unit.unit_id,
            "table": str(table),
            "range_id": unit.range_key,
            "cursor": {
                "lo": cp_mod.serialize_cursor(lo),
                "hi": cp_mod.serialize_cursor(hi),
            },
            "rows": int(rows),
            "checksum": str(checksum),
            "unit": unit.to_dict(),
        }
        with open(pf, "a") as f:
            f.write(json.dumps(rec) + "\n")

    def load_progress_journal(self, jsonl_path: Union[str, Path]) -> int:
        p = Path(jsonl_path)
        if not p.exists():
            return 0
        loaded = 0
        with self._lock:
            for line in p.read_text().splitlines():
                if not line.strip():
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue

                if r.get("scope_identity") and r.get("scope_identity") != self.scope.scope_identity:
                    continue

                tbl = r.get("table")
                rid = r.get("range_id")
                if not tbl or not rid or tbl not in self.scope.tables:
                    continue

                self._touched.add(tbl)
                entry = self._data["tables"].setdefault(
                    tbl, {"fingerprint": "", "done": {}, "units": {}}
                )
                if rid not in entry["done"]:
                    rows = int(r.get("rows", 0))
                    checksum = str(r.get("checksum", "0"))
                    entry["done"][rid] = [rows, checksum]
                    if "unit" in r:
                        entry.setdefault("units", {})[rid] = r["unit"]
                        uid = r.get("unit_id") or rid
                        self._data.setdefault("units", {})[uid] = {
                            "unit": r["unit"],
                            "rows": rows,
                            "checksum": checksum,
                        }
                    loaded += 1
            if loaded:
                self._flush()
        return loaded

    def _flush(self):
        if not self.path:
            return
        with durable.locked(self.path):
            disk = durable.read_state(self.path, "cache")
            merged = {
                "format_version": cp_mod.FORMAT_VERSION,
                "scope_header": self.scope.header(),
                "tables": {},
                "units": {},
            }
            if isinstance(disk, dict) and disk.get("format_version") == cp_mod.FORMAT_VERSION:
                merged["tables"].update(disk.get("tables") or {})
                merged["units"].update(disk.get("units") or {})

            for table in self._touched:
                if table in self._data["tables"]:
                    merged["tables"][table] = self._data["tables"][table]
                else:
                    merged["tables"].pop(table, None)

            merged["units"].update(self._data.get("units") or {})
            durable.write_atomic(
                self.path,
                json.dumps(merged, separators=(",", ":"), default=str),
            )


class ResumeScopeRunner:
    def __init__(self, checkpoint: ScopedCheckpoint):
        self.checkpoint = checkpoint
        self.scope = checkpoint.scope

    def execute_table(
        self,
        table: str,
        ranges: List[Tuple[Any, Any]],
        fetch_chunk_fn: Callable[[str, Any, Any], List[Dict[str, Any]]],
        write_chunk_fn: Callable[[str, List[Dict[str, Any]]], None],
        crash_at_chunk: Optional[int] = None,
        expr: str = "md5(id)",
    ) -> Tuple[int, int]:
        pending = self.checkpoint.begin(table, expr, ranges)
        processed_chunks = 0
        total_rows = 0

        for idx, (lo, hi) in enumerate(ranges):
            if (lo, hi) not in pending:
                continue

            if crash_at_chunk is not None and idx == crash_at_chunk:
                raise RuntimeError(f"simulated crash at chunk {idx}")

            raw_rows = fetch_chunk_fn(table, lo, hi)
            in_scope_rows = [r for r in raw_rows if self.scope.is_row_in_scope(r)]
            mapped_rows = [self.scope.apply_mapping(r) for r in in_scope_rows]

            write_chunk_fn(table, mapped_rows)

            checksum = hashlib.sha256(
                json.dumps(mapped_rows, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()[:16]

            self.checkpoint.record(table, lo, hi, len(mapped_rows), checksum)
            processed_chunks += 1
            total_rows += len(mapped_rows)

        return total_rows, processed_chunks

    def run_sqlite(
        self,
        src_db_path: Union[str, Path],
        dst_db_path: Union[str, Path],
        src_table: str,
        dst_table: str,
        pk_col: str,
        ranges: List[Tuple[Any, Any]],
        crash_at_chunk: Optional[int] = None,
    ) -> Tuple[int, int]:
        pending = self.checkpoint.begin(src_table, f"pk:{pk_col}", ranges)
        processed_chunks = 0
        total_rows = 0

        src_conn = sqlite3.connect(src_db_path)
        src_conn.row_factory = sqlite3.Row
        dst_conn = sqlite3.connect(dst_db_path)

        try:
            for idx, (lo, hi) in enumerate(ranges):
                if (lo, hi) not in pending:
                    continue

                if crash_at_chunk is not None and idx == crash_at_chunk:
                    raise RuntimeError(f"simulated crash at chunk {idx}")

                query_parts = [f"SELECT * FROM {src_table}"]
                where_sql = self.scope.where_clause(column=pk_col, lo=lo, hi=hi)
                if where_sql:
                    query_parts.append(f"WHERE {where_sql}")
                query_sql = " ".join(query_parts)

                cur = src_conn.cursor()
                cur.execute(query_sql)
                rows = [dict(r) for r in cur.fetchall()]

                in_scope_rows = [r for r in rows if self.scope.is_row_in_scope(r)]
                mapped_rows = [self.scope.apply_mapping(r) for r in in_scope_rows]

                if mapped_rows:
                    cols = list(mapped_rows[0].keys())
                    placeholders = ", ".join(["?"] * len(cols))
                    col_names = ", ".join(cols)
                    insert_sql = f"INSERT INTO {dst_table} ({col_names}) VALUES ({placeholders})"

                    with dst_conn:
                        for row in mapped_rows:
                            dst_conn.execute(insert_sql, [row[c] for c in cols])

                checksum = hashlib.sha256(
                    json.dumps(mapped_rows, sort_keys=True, default=str).encode("utf-8")
                ).hexdigest()[:16]

                self.checkpoint.record(src_table, lo, hi, len(mapped_rows), checksum)
                processed_chunks += 1
                total_rows += len(mapped_rows)

        finally:
            src_conn.close()
            dst_conn.close()

        return total_rows, processed_chunks
