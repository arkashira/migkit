"""Composed mapping proof for load and CDC (RCH17.mapping-proof).

Verifies composed mappings (filter, mask, rename, transform, embed)
across initial load snapshots and CDC change streams with explicit
lossiness comparison, multiplicity and identity preservation, and
durable checkpoint restart recovery.
"""
from __future__ import annotations

import copy
import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple


class CareBadge(str, Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class LossinessCategory(str, Enum):
    LOSSLESS = "LOSSLESS"
    LOSSY_BY_DESIGN = "LOSSY_BY_DESIGN"
    LOSSY_COERCION = "LOSSY_COERCION"
    ACCIDENTAL_LOSS = "ACCIDENTAL_LOSS"


class MultiplicityKind(str, Enum):
    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:N"
    MANY_TO_ONE = "N:1"


class ChangeOp(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


@dataclass
class Discrepancy:
    badge: CareBadge
    target_key: str
    field: str
    source_mapped_value: Any
    target_actual_value: Any
    lossiness_category: LossinessCategory
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "badge": self.badge.value,
            "target_key": self.target_key,
            "field": self.field,
            "source_mapped_value": str(self.source_mapped_value),
            "target_actual_value": str(self.target_actual_value),
            "lossiness_category": self.lossiness_category.value,
            "reason": self.reason,
        }


@dataclass
class StageLineage:
    stage_id: str
    stage_type: str
    description: str
    input_fields: List[str]
    output_fields: List[str]
    lossiness: LossinessCategory
    lossiness_detail: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage_id": self.stage_id,
            "stage_type": self.stage_type,
            "description": self.description,
            "input_fields": list(self.input_fields),
            "output_fields": list(self.output_fields),
            "lossiness": self.lossiness.value,
            "lossiness_detail": self.lossiness_detail,
        }


@dataclass
class LineageCoverageReport:
    version: str
    checksum: str
    total_source_fields: int
    mapped_fields: List[str]
    filtered_fields: List[str]
    masked_fields: List[str]
    embedded_fields: List[str]
    unmapped_fields: List[str]
    coverage_pct: float
    is_fully_covered: bool
    lossiness_manifest: Dict[str, str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "checksum": self.checksum,
            "total_source_fields": self.total_source_fields,
            "mapped_fields": list(self.mapped_fields),
            "filtered_fields": list(self.filtered_fields),
            "masked_fields": list(self.masked_fields),
            "embedded_fields": list(self.embedded_fields),
            "unmapped_fields": list(self.unmapped_fields),
            "coverage_pct": self.coverage_pct,
            "is_fully_covered": self.is_fully_covered,
            "lossiness_manifest": dict(self.lossiness_manifest),
        }

    def format_unix_report(self) -> str:
        status_badge = CareBadge.PASS.value if self.is_fully_covered else CareBadge.REVIEW.value
        lines = [
            f"DATAFLOW LINEAGE COVERAGE REPORT: [{status_badge}]",
            f"Version: {self.version} | Checksum: {self.checksum[:16]}",
            f"Coverage: {self.coverage_pct:.1f}% ({len(self.mapped_fields) + len(self.filtered_fields) + len(self.masked_fields) + len(self.embedded_fields)}/{self.total_source_fields} fields accounted for)",
            f"Mapped fields:    {', '.join(sorted(self.mapped_fields)) or '(none)'}",
            f"Masked fields:    {', '.join(sorted(self.masked_fields)) or '(none)'}",
            f"Filtered fields:  {', '.join(sorted(self.filtered_fields)) or '(none)'}",
            f"Embedded fields:  {', '.join(sorted(self.embedded_fields)) or '(none)'}",
            f"Unmapped fields:  {', '.join(sorted(self.unmapped_fields)) or '(none)'}",
            "Lossiness Manifest:",
        ]
        for fld, loss in sorted(self.lossiness_manifest.items()):
            lines.append(f"  - {fld}: {loss}")
        return "\n".join(lines)


class DataflowStage:
    stage_id: str
    stage_type: str

    def get_lineage(self) -> StageLineage:
        raise NotImplementedError


class FilterStage(DataflowStage):
    """Filters records based on predicate."""

    def __init__(
        self,
        stage_id: str,
        predicate: Callable[[Dict[str, Any]], bool],
        fields_involved: Sequence[str],
        description: str = "Row filter predicate",
        sql_expression: str = "",
    ):
        self.stage_id = stage_id
        self.stage_type = "filter"
        self.predicate = predicate
        self.fields_involved = list(fields_involved)
        self.description = description
        self.sql_expression = sql_expression

    def get_lineage(self) -> StageLineage:
        return StageLineage(
            stage_id=self.stage_id,
            stage_type=self.stage_type,
            description=self.description,
            input_fields=self.fields_involved,
            output_fields=[],
            lossiness=LossinessCategory.LOSSY_BY_DESIGN,
            lossiness_detail=f"Excludes rows not satisfying predicate ({self.sql_expression or self.description})",
        )

    def matches(self, row: Dict[str, Any]) -> bool:
        try:
            return bool(self.predicate(row))
        except Exception:
            return False

    def apply(self, rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        passed: List[Dict[str, Any]] = []
        dropped: List[Dict[str, Any]] = []
        for r in rows:
            if self.matches(r):
                passed.append(copy.deepcopy(r))
            else:
                dropped.append(copy.deepcopy(r))
        return passed, dropped


class MaskStage(DataflowStage):
    """Masks sensitive fields."""

    def __init__(
        self,
        stage_id: str,
        column: str,
        method: str = "hash",
        secret_key: Optional[bytes] = None,
        salt: Optional[bytes] = None,
        param: Optional[Any] = None,
        description: str = "",
    ):
        self.stage_id = stage_id
        self.stage_type = "mask"
        self.column = column
        self.method = method.lower()
        self.secret_key = secret_key or b"migkit-default-proof-salt-k102"
        self.salt = salt or b"epoch-01"
        self.param = param
        self.description = description or f"Mask column {column} using {self.method}"

    def get_lineage(self) -> StageLineage:
        lossiness = (
            LossinessCategory.LOSSLESS
            if self.method in ("fpe", "reversible_token")
            else LossinessCategory.LOSSY_BY_DESIGN
        )
        return StageLineage(
            stage_id=self.stage_id,
            stage_type=self.stage_type,
            description=self.description,
            input_fields=[self.column],
            output_fields=[self.column],
            lossiness=lossiness,
            lossiness_detail=f"Masking method: {self.method}",
        )

    def apply_value(self, val: Any) -> Any:
        if val is None:
            return None
        val_str = str(val)
        if self.method == "hash":
            h = hmac.new(self.secret_key, self.salt + val_str.encode("utf-8"), hashlib.sha256)
            return "mask:" + h.hexdigest()[:16]
        elif self.method == "truncate":
            n = int(self.param) if self.param is not None else 4
            return val_str[:n]
        elif self.method == "constant":
            return str(self.param) if self.param is not None else "[REDACTED]"
        elif self.method == "null":
            return None
        elif self.method == "fpe":
            alphabet_mode = str(self.param).lower() if self.param is not None else "digits"
            h = hashlib.sha256(self.secret_key + val_str.encode("utf-8")).hexdigest()
            res = []
            for i, ch in enumerate(val_str):
                offset = int(h[i % len(h)], 16)
                if ch.isdigit():
                    res.append(str((int(ch) + offset) % 10))
                elif alphabet_mode in ("alnum", "alpha") and ch.isupper():
                    res.append(chr(ord('A') + (ord(ch) - ord('A') + offset) % 26))
                elif alphabet_mode in ("alnum", "alpha") and ch.islower():
                    res.append(chr(ord('a') + (ord(ch) - ord('a') + offset) % 26))
                else:
                    res.append(ch)
            return "".join(res)
        return val

    def apply_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        out = copy.deepcopy(row)
        if self.column in out:
            out[self.column] = self.apply_value(out[self.column])
        return out


class RenameStage(DataflowStage):
    """Renames source columns to target columns."""

    def __init__(
        self,
        stage_id: str,
        mapping: Dict[str, str],
        drop_unmapped: bool = False,
        description: str = "",
    ):
        self.stage_id = stage_id
        self.stage_type = "rename"
        self.mapping = dict(mapping)
        self.drop_unmapped = drop_unmapped
        self.description = description or "Column projection and renaming"

    def get_lineage(self) -> StageLineage:
        return StageLineage(
            stage_id=self.stage_id,
            stage_type=self.stage_type,
            description=self.description,
            input_fields=list(self.mapping.keys()),
            output_fields=list(self.mapping.values()),
            lossiness=LossinessCategory.LOSSLESS,
            lossiness_detail="Lossless identity rename and projection",
        )

    def apply_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in row.items():
            if k in self.mapping:
                out[self.mapping[k]] = v
            elif not self.drop_unmapped:
                out[k] = v
        return out


class TransformStage(DataflowStage):
    """Applies computational transformation or coercion to a column."""

    def __init__(
        self,
        stage_id: str,
        column: str,
        transform_fn: Callable[[Any], Any],
        target_column: Optional[str] = None,
        lossiness: LossinessCategory = LossinessCategory.LOSSLESS,
        lossiness_detail: str = "",
        coercion_checker: Optional[Callable[[Any, Any], bool]] = None,
        description: str = "",
    ):
        self.stage_id = stage_id
        self.stage_type = "transform"
        self.column = column
        self.target_column = target_column or column
        self.transform_fn = transform_fn
        self.lossiness = lossiness
        self.lossiness_detail = lossiness_detail or f"Transform on {column}"
        self.coercion_checker = coercion_checker
        self.description = description or f"Transform {column} -> {self.target_column}"

    def get_lineage(self) -> StageLineage:
        return StageLineage(
            stage_id=self.stage_id,
            stage_type=self.stage_type,
            description=self.description,
            input_fields=[self.column],
            output_fields=[self.target_column],
            lossiness=self.lossiness,
            lossiness_detail=self.lossiness_detail,
        )

    def apply_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        out = copy.deepcopy(row)
        if self.column in out:
            val = out.pop(self.column) if self.target_column != self.column else out[self.column]
            out[self.target_column] = self.transform_fn(val)
        return out


class EmbeddingStage(DataflowStage):
    """Relational-to-document parent-child embedding or document unnesting."""

    def __init__(
        self,
        stage_id: str,
        kind: MultiplicityKind,
        parent_entity: str,
        child_entity: str,
        parent_pk: str,
        child_foreign_key: str,
        target_embedded_field: str,
        child_pk: str,
        child_fields: Optional[List[str]] = None,
        description: str = "",
    ):
        self.stage_id = stage_id
        self.stage_type = "embed"
        self.kind = kind
        self.parent_entity = parent_entity
        self.child_entity = child_entity
        self.parent_pk = parent_pk
        self.child_foreign_key = child_foreign_key
        self.target_embedded_field = target_embedded_field
        self.child_pk = child_pk
        self.child_fields = child_fields
        self.description = description or f"Embed {child_entity} into {parent_entity}.{target_embedded_field}"

    def get_lineage(self) -> StageLineage:
        inputs = [f"{self.parent_entity}.{self.parent_pk}", f"{self.child_entity}.{self.child_foreign_key}"]
        if self.child_fields:
            inputs.extend([f"{self.child_entity}.{f}" for f in self.child_fields])
        return StageLineage(
            stage_id=self.stage_id,
            stage_type=self.stage_type,
            description=self.description,
            input_fields=inputs,
            output_fields=[f"{self.parent_entity}.{self.target_embedded_field}"],
            lossiness=LossinessCategory.LOSSLESS,
            lossiness_detail=f"Composed {self.kind.value} document hierarchy",
        )

    def embed(
        self,
        parents: List[Dict[str, Any]],
        children: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        # Index children by foreign key
        child_map: Dict[Any, List[Dict[str, Any]]] = {}
        for c in children:
            fk_val = c.get(self.child_foreign_key)
            if fk_val is not None:
                item = copy.deepcopy(c)
                if self.child_fields:
                    item = {k: item[k] for k in self.child_fields if k in item}
                child_map.setdefault(fk_val, []).append(item)

        out_docs: List[Dict[str, Any]] = []
        for p in parents:
            doc = copy.deepcopy(p)
            pk_val = p.get(self.parent_pk)
            # Embed array of children
            matched_children = child_map.get(pk_val, [])
            # Keep children sorted by child_pk for deterministic verification
            matched_children.sort(key=lambda x: str(x.get(self.child_pk, "")))
            doc[self.target_embedded_field] = matched_children
            out_docs.append(doc)
        return out_docs


class MappingDataflow:
    """A versioned dataflow pipeline of composed stages."""

    def __init__(
        self,
        dataflow_id: str,
        version: str,
        target_entity: str,
        target_key_cols: Sequence[str],
        source_entities: Sequence[str],
    ):
        self.dataflow_id = dataflow_id
        self.version = version
        self.target_entity = target_entity
        self.target_key_cols = list(target_key_cols)
        self.source_entities = list(source_entities)
        self.stages: List[DataflowStage] = []
        self.created_at = time.time()

    def add_stage(self, stage: DataflowStage) -> MappingDataflow:
        self.stages.append(stage)
        return self

    def checksum(self) -> str:
        s = f"{self.dataflow_id}:{self.version}:{self.target_entity}:{','.join(self.target_key_cols)}"
        for st in self.stages:
            lin = st.get_lineage()
            s += f"|{lin.stage_id}:{lin.stage_type}:{','.join(lin.input_fields)}->{','.join(lin.output_fields)}"
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    def get_stages_by_type(self, stage_type: str) -> List[DataflowStage]:
        return [st for st in self.stages if st.stage_type == stage_type]

    def compute_coverage(
        self,
        all_source_columns: Dict[str, Sequence[str]],
    ) -> LineageCoverageReport:
        total_fields_set: Set[str] = set()
        field_to_full: Dict[str, Set[str]] = {}
        for ent, cols in all_source_columns.items():
            for c in cols:
                full_name = f"{ent}.{c}" if len(all_source_columns) > 1 else c
                total_fields_set.add(full_name)
                field_to_full.setdefault(c, set()).add(full_name)

        mapped_fields: Set[str] = set()
        filtered_fields: Set[str] = set()
        masked_fields: Set[str] = set()
        embedded_fields: Set[str] = set()
        lossiness_manifest: Dict[str, str] = {}

        for st in self.stages:
            lin = st.get_lineage()
            target_set = None
            if st.stage_type == "filter":
                target_set = filtered_fields
            elif st.stage_type == "mask":
                target_set = masked_fields
            elif st.stage_type in ("rename", "transform"):
                target_set = mapped_fields
            elif st.stage_type == "embed":
                target_set = embedded_fields

            for f in lin.input_fields:
                detail = f"{lin.lossiness.value} ({lin.lossiness_detail})" if lin.lossiness_detail else lin.lossiness.value
                lossiness_manifest[f] = detail
                matched_fulls = field_to_full.get(f)
                if matched_fulls:
                    for mf in matched_fulls:
                        if target_set is not None:
                            target_set.add(f)
                        lossiness_manifest[mf] = detail
                else:
                    if target_set is not None:
                        target_set.add(f)

        unmapped: List[str] = []
        for full_fld in sorted(total_fields_set):
            unqualified = full_fld.split(".")[-1]
            is_acc = (
                full_fld in lossiness_manifest
                or full_fld in mapped_fields
                or full_fld in filtered_fields
                or full_fld in masked_fields
                or full_fld in embedded_fields
                or unqualified in mapped_fields
                or unqualified in filtered_fields
                or unqualified in masked_fields
                or unqualified in embedded_fields
            )
            if not is_acc:
                unmapped.append(full_fld)

        total_count = len(total_fields_set) or 1
        covered_count = total_count - len(unmapped)
        cov_pct = (covered_count / total_count) * 100.0

        return LineageCoverageReport(
            version=self.version,
            checksum=self.checksum(),
            total_source_fields=total_count,
            mapped_fields=sorted(list(mapped_fields)),
            filtered_fields=sorted(list(filtered_fields)),
            masked_fields=sorted(list(masked_fields)),
            embedded_fields=sorted(list(embedded_fields)),
            unmapped_fields=unmapped,
            coverage_pct=round(cov_pct, 2),
            is_fully_covered=(len(unmapped) == 0),
            lossiness_manifest=lossiness_manifest,
        )

    def execute_load(
        self,
        source_tables: Dict[str, List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        """Executes full composed dataflow on initial load source tables."""
        primary_entity = self.source_entities[0]
        current_rows = copy.deepcopy(source_tables.get(primary_entity, []))

        # Check for embedding stages
        embed_stages = [s for s in self.stages if isinstance(s, EmbeddingStage)]
        if embed_stages:
            for emb in embed_stages:
                child_rows = copy.deepcopy(source_tables.get(emb.child_entity, []))
                current_rows = emb.embed(current_rows, child_rows)

        # Apply sequential row-level stages (filter, mask, rename, transform)
        filtered_rows: List[Dict[str, Any]] = []
        for r in current_rows:
            keep = True
            for st in self.stages:
                if isinstance(st, FilterStage):
                    if not st.matches(r):
                        keep = False
                        break
            if keep:
                filtered_rows.append(r)

        result_rows: List[Dict[str, Any]] = []
        for r in filtered_rows:
            curr = copy.deepcopy(r)
            for st in self.stages:
                if isinstance(st, MaskStage):
                    curr = st.apply_row(curr)
                elif isinstance(st, TransformStage):
                    curr = st.apply_row(curr)
                elif isinstance(st, RenameStage):
                    curr = st.apply_row(curr)
            result_rows.append(curr)

        return result_rows


@dataclass
class CDCChangeEvent:
    sequence_offset: int
    entity: str
    op: ChangeOp
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sequence_offset": self.sequence_offset,
            "entity": self.entity,
            "op": self.op.value,
            "before": self.before,
            "after": self.after,
            "timestamp": self.timestamp,
        }


@dataclass
class CheckpointState:
    last_processed_offset: int
    dataflow_checksum: str
    target_state: Dict[str, Dict[str, Any]]
    source_snapshots: Dict[str, Dict[str, Dict[str, Any]]]
    event_count: int

    def serialize(self) -> str:
        return json.dumps({
            "last_processed_offset": self.last_processed_offset,
            "dataflow_checksum": self.dataflow_checksum,
            "target_state": self.target_state,
            "source_snapshots": self.source_snapshots,
            "event_count": self.event_count,
        })

    @classmethod
    def deserialize(cls, payload: str) -> CheckpointState:
        data = json.loads(payload)
        return cls(
            last_processed_offset=data["last_processed_offset"],
            dataflow_checksum=data["dataflow_checksum"],
            target_state=data["target_state"],
            source_snapshots=data["source_snapshots"],
            event_count=data["event_count"],
        )


class CDCExecutionEngine:
    """Applies CDC change events to track and update composed target state.

    Accurately handles filter boundary transitions (a row updated to fail
    a filter must be DELETED from target; updated to pass must be INSERTED),
    child embeddings in parent documents, and durable restart checkpoints.
    """

    def __init__(
        self,
        dataflow: MappingDataflow,
        initial_sources: Dict[str, List[Dict[str, Any]]],
        primary_entity_pk: str = "id",
        child_entity_pk: str = "item_id",
    ):
        self.dataflow = dataflow
        self.primary_entity = dataflow.source_entities[0]
        self.primary_entity_pk = primary_entity_pk
        self.child_entity_pk = child_entity_pk

        # Internal source state
        self._source_state: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for ent, rows in initial_sources.items():
            pk = self.child_entity_pk if ent != self.primary_entity else self.primary_entity_pk
            self._source_state[ent] = {str(r[pk]): copy.deepcopy(r) for r in rows if pk in r}

        # Initialize current target documents
        initial_target_rows = self.dataflow.execute_load(initial_sources)
        self._target_state: Dict[str, Dict[str, Any]] = {}
        for r in initial_target_rows:
            k = self._make_target_key(r)
            self._target_state[k] = r

        self.last_offset = 0
        self.events_applied = 0

    def _make_target_key(self, row: Dict[str, Any]) -> str:
        parts = [str(row.get(col, "")) for col in self.dataflow.target_key_cols]
        return ":".join(parts)

    def _apply_row_transforms(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Check filter
        for st in self.dataflow.stages:
            if isinstance(st, FilterStage):
                if not st.matches(row):
                    return None
        out = copy.deepcopy(row)
        for st in self.dataflow.stages:
            if isinstance(st, MaskStage):
                out = st.apply_row(out)
            elif isinstance(st, TransformStage):
                out = st.apply_row(out)
            elif isinstance(st, RenameStage):
                out = st.apply_row(out)
        return out

    def _rebuild_parent_doc(self, parent_pk_val: Any) -> Optional[Dict[str, Any]]:
        pk_str = str(parent_pk_val)
        parent_row = self._source_state.get(self.primary_entity, {}).get(pk_str)
        if not parent_row:
            return None

        # Check embedding stages
        embed_stages = [s for s in self.dataflow.stages if isinstance(s, EmbeddingStage)]
        doc = copy.deepcopy(parent_row)
        for emb in embed_stages:
            # find all children matching parent pk
            matching_children = []
            for c in self._source_state.get(emb.child_entity, {}).values():
                if str(c.get(emb.child_foreign_key)) == pk_str:
                    item = copy.deepcopy(c)
                    if emb.child_fields:
                        item = {k: item[k] for k in emb.child_fields if k in item}
                    matching_children.append(item)
            matching_children.sort(key=lambda x: str(x.get(emb.child_pk, "")))
            doc[emb.target_embedded_field] = matching_children

        return self._apply_row_transforms(doc)

    def apply_event(self, event: CDCChangeEvent) -> None:
        """Applies a single CDC change event."""
        if event.sequence_offset <= self.last_offset and self.last_offset > 0:
            # Already processed (idempotent skip)
            return

        ent = event.entity
        pk_field = self.child_entity_pk if ent != self.primary_entity else self.primary_entity_pk

        if ent == self.primary_entity:
            # Event on primary entity
            if event.op == ChangeOp.INSERT:
                row = event.after or {}
                pk_val = row.get(pk_field)
                self._source_state.setdefault(ent, {})[str(pk_val)] = copy.deepcopy(row)
                target_doc = self._rebuild_parent_doc(pk_val)
                if target_doc is not None:
                    t_key = self._make_target_key(target_doc)
                    self._target_state[t_key] = target_doc

            elif event.op == ChangeOp.UPDATE:
                row = event.after or {}
                pk_val = row.get(pk_field)
                self._source_state.setdefault(ent, {})[str(pk_val)] = copy.deepcopy(row)
                target_doc = self._rebuild_parent_doc(pk_val)
                # Form target key for before / after
                old_key = str(pk_val)
                if target_doc is not None:
                    t_key = self._make_target_key(target_doc)
                    self._target_state[t_key] = target_doc
                else:
                    # Filter boundary transition: row updated such that it now fails filter!
                    # Must be removed from target state.
                    self._target_state.pop(old_key, None)

            elif event.op == ChangeOp.DELETE:
                before_row = event.before or {}
                pk_val = before_row.get(pk_field)
                if pk_val is not None:
                    self._source_state.get(ent, {}).pop(str(pk_val), None)
                    t_key = str(pk_val)
                    self._target_state.pop(t_key, None)

        else:
            # Event on child entity (e.g. order_items)
            embed_stage = next(
                (s for s in self.dataflow.stages if isinstance(s, EmbeddingStage) and s.child_entity == ent),
                None,
            )
            if embed_stage:
                fk_field = embed_stage.child_foreign_key
                if event.op in (ChangeOp.INSERT, ChangeOp.UPDATE):
                    row = event.after or {}
                    c_pk = row.get(pk_field)
                    parent_fk = row.get(fk_field)
                    self._source_state.setdefault(ent, {})[str(c_pk)] = copy.deepcopy(row)
                    if parent_fk is not None:
                        updated_parent = self._rebuild_parent_doc(parent_fk)
                        if updated_parent is not None:
                            t_key = self._make_target_key(updated_parent)
                            self._target_state[t_key] = updated_parent

                elif event.op == ChangeOp.DELETE:
                    row = event.before or {}
                    c_pk = row.get(pk_field)
                    parent_fk = row.get(fk_field)
                    if c_pk is not None:
                        self._source_state.get(ent, {}).pop(str(c_pk), None)
                    if parent_fk is not None:
                        updated_parent = self._rebuild_parent_doc(parent_fk)
                        if updated_parent is not None:
                            t_key = self._make_target_key(updated_parent)
                            self._target_state[t_key] = updated_parent

        self.last_offset = event.sequence_offset
        self.events_applied += 1

    def create_checkpoint(self) -> CheckpointState:
        return CheckpointState(
            last_processed_offset=self.last_offset,
            dataflow_checksum=self.dataflow.checksum(),
            target_state=copy.deepcopy(self._target_state),
            source_snapshots=copy.deepcopy(self._source_state),
            event_count=self.events_applied,
        )

    def restore_checkpoint(self, checkpoint: CheckpointState) -> None:
        if checkpoint.dataflow_checksum != self.dataflow.checksum():
            raise ValueError("Checkpoint checksum mismatch with active dataflow pipeline")
        self.last_offset = checkpoint.last_processed_offset
        self._target_state = copy.deepcopy(checkpoint.target_state)
        self._source_state = copy.deepcopy(checkpoint.source_snapshots)
        self.events_applied = checkpoint.event_count

    def get_target_state(self) -> List[Dict[str, Any]]:
        return list(self._target_state.values())


@dataclass
class MappingProofResult:
    badge: CareBadge
    dataflow_version: str
    dataflow_checksum: str
    source_record_count: int
    expected_target_count: int
    actual_target_count: int
    matched_keys_count: int
    discrepancies: List[Discrepancy]
    coverage_report: LineageCoverageReport
    raw_count_matches: bool

    def is_verified(self) -> bool:
        return self.badge == CareBadge.PASS

    def format_unix_report(self) -> str:
        lines = [
            f"MAPPING PROOF VERIFICATION: [{self.badge.value}]",
            f"Dataflow: {self.dataflow_version} (checksum {self.dataflow_checksum[:16]})",
            f"Counts: source={self.source_record_count} | expected_target={self.expected_target_count} | actual_target={self.actual_target_count} | matched_keys={self.matched_keys_count}",
            f"Raw count matches: {'YES' if self.raw_count_matches else 'NO'} (mapped-value proof enforced)",
            f"Lineage coverage: {self.coverage_report.coverage_pct:.1f}%",
        ]
        if self.discrepancies:
            lines.append("Discrepancies:")
            for d in self.discrepancies[:20]:
                lines.append(
                    f"  [{d.badge.value}] key={d.target_key} field={d.field}: {d.reason} "
                    f"(expected={d.source_mapped_value!r}, actual={d.target_actual_value!r}, lossiness={d.lossiness_category.value})"
                )
            if len(self.discrepancies) > 20:
                lines.append(f"  ... and {len(self.discrepancies) - 20} more discrepancies.")
        else:
            lines.append("Zero value/identity/multiplicity discrepancies found.")
        return "\n".join(lines)


class MappingProofEngine:
    """Verifies expected mapped source output against target records."""

    def __init__(self, dataflow: MappingDataflow):
        self.dataflow = dataflow

    def _make_key(self, row: Dict[str, Any]) -> str:
        parts = [str(row.get(col, "")) for col in self.dataflow.target_key_cols]
        return ":".join(parts)

    def _deep_compare(
        self,
        exp_val: Any,
        act_val: Any,
        field_path: str,
        target_key: str,
        lossiness_manifest: Dict[str, str],
        transforms_by_target_col: Dict[str, TransformStage],
    ) -> List[Discrepancy]:
        discrepancies: List[Discrepancy] = []

        if exp_val == act_val:
            return discrepancies

        # Check if field has an explicit coercion checker
        t_stage = transforms_by_target_col.get(field_path)
        if t_stage and t_stage.coercion_checker:
            if t_stage.coercion_checker(exp_val, act_val):
                # Within declared coercion tolerance -> REVIEW (never silently normalized away!)
                discrepancies.append(Discrepancy(
                    badge=CareBadge.REVIEW,
                    target_key=target_key,
                    field=field_path,
                    source_mapped_value=exp_val,
                    target_actual_value=act_val,
                    lossiness_category=LossinessCategory.LOSSY_COERCION,
                    reason=f"Declared lossy coercion accepted: {t_stage.lossiness_detail}",
                ))
                return discrepancies

        # Compare nested lists (e.g. embedded child array)
        if isinstance(exp_val, list) and isinstance(act_val, list):
            if len(exp_val) != len(act_val):
                discrepancies.append(Discrepancy(
                    badge=CareBadge.ACTION,
                    target_key=target_key,
                    field=f"{field_path}.multiplicity",
                    source_mapped_value=len(exp_val),
                    target_actual_value=len(act_val),
                    lossiness_category=LossinessCategory.ACCIDENTAL_LOSS,
                    reason=f"Embedded child array length mismatch (expected {len(exp_val)}, got {len(act_val)})",
                ))
                return discrepancies
            for idx, (exp_item, act_item) in enumerate(zip(exp_val, act_val)):
                sub_discs = self._deep_compare(
                    exp_item, act_item, f"{field_path}[{idx}]", target_key, lossiness_manifest, transforms_by_target_col
                )
                discrepancies.extend(sub_discs)
            return discrepancies

        # Compare nested dicts
        if isinstance(exp_val, dict) and isinstance(act_val, dict):
            all_keys = set(exp_val.keys()) | set(act_val.keys())
            for k in sorted(all_keys):
                sub_discs = self._deep_compare(
                    exp_val.get(k), act_val.get(k), f"{field_path}.{k}" if field_path else k,
                    target_key, lossiness_manifest, transforms_by_target_col
                )
                discrepancies.extend(sub_discs)
            return discrepancies

        # Value mismatch
        loss_cat = LossinessCategory.ACCIDENTAL_LOSS
        loss_reason = f"Value mismatch on field {field_path}"
        badge = CareBadge.ACTION

        # Check if field was declared lossy by design
        declared_loss = lossiness_manifest.get(field_path, "") or lossiness_manifest.get(field_path.split(".")[-1], "")
        if "LOSSY_BY_DESIGN" in declared_loss:
            loss_cat = LossinessCategory.LOSSY_BY_DESIGN
            loss_reason = f"Mismatch on masked/lossy field: expected mapped {exp_val!r}, target had {act_val!r}"
        elif "LOSSY_COERCION" in declared_loss:
            loss_cat = LossinessCategory.LOSSY_COERCION
            loss_reason = f"Undeclared or out-of-tolerance coercion difference on {field_path}"

        discrepancies.append(Discrepancy(
            badge=badge,
            target_key=target_key,
            field=field_path,
            source_mapped_value=exp_val,
            target_actual_value=act_val,
            lossiness_category=loss_cat,
            reason=loss_reason,
        ))
        return discrepancies

    def verify(
        self,
        expected_target_records: List[Dict[str, Any]],
        actual_target_records: List[Dict[str, Any]],
        source_count: int,
        all_source_columns: Optional[Dict[str, Sequence[str]]] = None,
    ) -> MappingProofResult:
        coverage = self.dataflow.compute_coverage(all_source_columns or {})
        transforms_by_target: Dict[str, TransformStage] = {
            st.target_column: st for st in self.dataflow.stages if isinstance(st, TransformStage)
        }

        # Index expected by key
        expected_map: Dict[str, Dict[str, Any]] = {}
        for r in expected_target_records:
            k = self._make_key(r)
            if k in expected_map:
                # Multiplicity violation in expected generation (duplicate key produced)
                pass
            expected_map[k] = r

        # Index actual by key
        actual_map: Dict[str, Dict[str, Any]] = {}
        actual_key_counts: Dict[str, int] = {}
        for r in actual_target_records:
            k = self._make_key(r)
            actual_key_counts[k] = actual_key_counts.get(k, 0) + 1
            actual_map[k] = r

        discrepancies: List[Discrepancy] = []

        # 1. Check for duplicate keys in target (multiplicity defect)
        for k, cnt in actual_key_counts.items():
            if cnt > 1:
                discrepancies.append(Discrepancy(
                    badge=CareBadge.ACTION,
                    target_key=k,
                    field="multiplicity",
                    source_mapped_value=1,
                    target_actual_value=cnt,
                    lossiness_category=LossinessCategory.ACCIDENTAL_LOSS,
                    reason=f"Target contains duplicate records for key '{k}' (multiplicity violation)",
                ))

        # 2. Check for missing keys in target
        for k, exp_row in expected_map.items():
            if k not in actual_map:
                discrepancies.append(Discrepancy(
                    badge=CareBadge.ACTION,
                    target_key=k,
                    field="__identity__",
                    source_mapped_value=exp_row,
                    target_actual_value=None,
                    lossiness_category=LossinessCategory.ACCIDENTAL_LOSS,
                    reason=f"Target missing expected mapped record for key '{k}'",
                ))

        # 3. Check for unexpected extra keys in target
        for k, act_row in actual_map.items():
            if k not in expected_map:
                discrepancies.append(Discrepancy(
                    badge=CareBadge.ACTION,
                    target_key=k,
                    field="__identity__",
                    source_mapped_value=None,
                    target_actual_value=act_row,
                    lossiness_category=LossinessCategory.ACCIDENTAL_LOSS,
                    reason=f"Target contains unexpected record for key '{k}' not produced by mapped source",
                ))

        # 4. Compare fields for matched keys
        matched_keys = set(expected_map.keys()) & set(actual_map.keys())
        for k in sorted(matched_keys):
            exp_row = expected_map[k]
            act_row = actual_map[k]
            diffs = self._deep_compare(
                exp_row, act_row, "", k, coverage.lossiness_manifest, transforms_by_target
            )
            discrepancies.extend(diffs)

        # Determine overall care badge
        raw_count_matches = len(expected_target_records) == len(actual_target_records)
        has_action = any(d.badge == CareBadge.ACTION for d in discrepancies)
        has_review = any(d.badge == CareBadge.REVIEW for d in discrepancies)

        if has_action:
            badge = CareBadge.ACTION
        elif has_review or not coverage.is_fully_covered:
            badge = CareBadge.REVIEW
        else:
            badge = CareBadge.PASS

        return MappingProofResult(
            badge=badge,
            dataflow_version=self.dataflow.version,
            dataflow_checksum=self.dataflow.checksum(),
            source_record_count=source_count,
            expected_target_count=len(expected_target_records),
            actual_target_count=len(actual_target_records),
            matched_keys_count=len(matched_keys),
            discrepancies=discrepancies,
            coverage_report=coverage,
            raw_count_matches=raw_count_matches,
        )

    def verify_load(
        self,
        source_tables: Dict[str, List[Dict[str, Any]]],
        actual_target_records: List[Dict[str, Any]],
    ) -> MappingProofResult:
        """Verifies initial load snapshot."""
        primary_entity = self.dataflow.source_entities[0]
        source_count = len(source_tables.get(primary_entity, []))
        expected_target = self.dataflow.execute_load(source_tables)
        all_source_cols = {ent: list(rows[0].keys()) if rows else [] for ent, rows in source_tables.items()}
        return self.verify(expected_target, actual_target_records, source_count, all_source_cols)

    def verify_cdc(
        self,
        cdc_engine: CDCExecutionEngine,
        actual_target_records: List[Dict[str, Any]],
        all_source_columns: Optional[Dict[str, Sequence[str]]] = None,
    ) -> MappingProofResult:
        """Verifies target state after applying CDC stream events."""
        expected_target = cdc_engine.get_target_state()
        primary_cnt = len(cdc_engine._source_state.get(self.dataflow.source_entities[0], {}))
        return self.verify(expected_target, actual_target_records, primary_cnt, all_source_columns)
