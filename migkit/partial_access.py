import hashlib
import hmac
import ipaddress
import json
import os
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from . import decide, envkeys
from .verdict import VERDICTS


def get_default_allowed_networks(override: Optional[Iterable[str]] = None) -> Set[str]:
    if override is not None:
        return set(override)
    env = envkeys.items("MIGKIT_ALLOWED_NETWORKS")
    if env:
        return set(env)
    return {"127.0.0.1", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "::1"}


class PartialAccessError(Exception):
    pass


class SecurityScopeViolationError(PartialAccessError):
    pass


class SchemaIntegrityError(PartialAccessError):
    pass


class ManifestVersionError(PartialAccessError):
    pass


class ManifestCompletenessError(PartialAccessError):
    pass


class PlaceholderDataProhibitedError(PartialAccessError):
    pass


class HonestCoverageViolationError(PartialAccessError):
    pass


class UnauthorizedCollectorError(PartialAccessError):
    pass


class AbsentReason(str, Enum):
    RESTRICTED_ACCESS_NO_API = "restricted_access_no_api"
    OWNER_ONLY_DATA = "owner_only_data"
    PERMISSION_DENIED = "permission_denied"
    COLUMN_MASKED = "column_masked"
    DYNAMIC_LINEAGE_UNRESOLVED = "dynamic_lineage_unresolved"
    CORRUPTED_EXPORT = "corrupted_export"
    FILTERED_OUT_OF_SCOPE = "filtered_out_of_scope"


@dataclass(frozen=True)
class SourceLineage:
    source_system: str
    export_batch_id: str
    collected_at: float
    collector_id: str
    extraction_method: str
    tenant_id: Optional[str] = None
    provenance_chain: List[Dict[str, Any]] = field(default_factory=list)
    lineage_hash: str = ""

    def __post_init__(self):
        if not self.lineage_hash:
            computed = self._compute_hash()
            object.__setattr__(self, "lineage_hash", computed)

    def _compute_hash(self) -> str:
        body = {
            "source_system": self.source_system,
            "export_batch_id": self.export_batch_id,
            "collected_at": self.collected_at,
            "collector_id": self.collector_id,
            "extraction_method": self.extraction_method,
            "tenant_id": self.tenant_id,
            "provenance_chain": self.provenance_chain,
        }
        serialized = json.dumps(body, sort_keys=True).encode("utf-8")
        return hashlib.sha256(serialized).hexdigest()

    def extend_chain(self, step: str, details: Dict[str, Any]) -> "SourceLineage":
        new_chain = list(self.provenance_chain)
        new_chain.append({
            "step": step,
            "details": details,
            "timestamp": time.time(),
        })
        return SourceLineage(
            source_system=self.source_system,
            export_batch_id=self.export_batch_id,
            collected_at=self.collected_at,
            collector_id=self.collector_id,
            extraction_method=self.extraction_method,
            tenant_id=self.tenant_id,
            provenance_chain=new_chain,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_system": self.source_system,
            "export_batch_id": self.export_batch_id,
            "collected_at": self.collected_at,
            "collector_id": self.collector_id,
            "extraction_method": self.extraction_method,
            "tenant_id": self.tenant_id,
            "provenance_chain": self.provenance_chain,
            "lineage_hash": self.lineage_hash,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SourceLineage":
        return cls(
            source_system=data["source_system"],
            export_batch_id=data["export_batch_id"],
            collected_at=data["collected_at"],
            collector_id=data["collector_id"],
            extraction_method=data["extraction_method"],
            tenant_id=data.get("tenant_id"),
            provenance_chain=data.get("provenance_chain", []),
            lineage_hash=data.get("lineage_hash", ""),
        )


@dataclass
class ExportFilePart:
    part_number: int
    file_path: str
    record_count: int
    sha256_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExportFilePart":
        return cls(
            part_number=data["part_number"],
            file_path=data["file_path"],
            record_count=data["record_count"],
            sha256_hash=data["sha256_hash"],
        )


@dataclass
class ExportObjectManifest:
    object_name: str
    record_count: int
    data_hash: str
    schema_definition: Dict[str, str]
    schema_version: int = 1
    file_parts: List[ExportFilePart] = field(default_factory=list)
    columns: List[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.columns and self.schema_definition:
            self.columns = sorted(list(self.schema_definition.keys()))

    @classmethod
    def from_records(
        cls,
        object_name: str,
        records: Sequence[Dict[str, Any]],
        schema_definition: Dict[str, str],
        columns: Optional[List[str]] = None,
        schema_version: int = 1,
    ) -> "ExportObjectManifest":
        cols = columns if columns is not None else sorted(list(schema_definition.keys()))
        serialized = json.dumps(records, sort_keys=True, default=str).encode("utf-8")
        data_hash = hashlib.sha256(serialized).hexdigest()
        return cls(
            object_name=object_name,
            record_count=len(records),
            data_hash=data_hash,
            schema_definition=schema_definition,
            schema_version=schema_version,
            columns=cols,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "object_name": self.object_name,
            "record_count": self.record_count,
            "data_hash": self.data_hash,
            "schema_definition": self.schema_definition,
            "schema_version": self.schema_version,
            "file_parts": [p.to_dict() for p in self.file_parts],
            "columns": self.columns,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExportObjectManifest":
        parts = [ExportFilePart.from_dict(p) for p in data.get("file_parts", [])]
        return cls(
            object_name=data["object_name"],
            record_count=data["record_count"],
            data_hash=data["data_hash"],
            schema_definition=data["schema_definition"],
            schema_version=data.get("schema_version", 1),
            file_parts=parts,
            columns=data.get("columns", []),
        )


@dataclass
class ApprovedExportManifest:
    manifest_id: str
    collector_id: str
    scope_id: str
    lineage: SourceLineage
    objects: Dict[str, ExportObjectManifest]
    manifest_version: int = 1
    overall_data_hash: str = ""
    manifest_signature: Optional[str] = None
    created_at: float = field(default_factory=time.time)

    def __post_init__(self):
        if not self.overall_data_hash:
            self.overall_data_hash = self._compute_overall_hash()

    def _compute_overall_hash(self) -> str:
        obj_hashes = {k: v.data_hash for k, v in sorted(self.objects.items())}
        raw = json.dumps(obj_hashes, sort_keys=True).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def digest_for_signing(self) -> bytes:
        body = {
            "manifest_id": self.manifest_id,
            "manifest_version": self.manifest_version,
            "collector_id": self.collector_id,
            "scope_id": self.scope_id,
            "overall_data_hash": self.overall_data_hash,
            "lineage_hash": self.lineage.lineage_hash,
        }
        return json.dumps(body, sort_keys=True).encode("utf-8")

    def validate_completeness(self, expected_objects: Iterable[str]) -> Tuple[bool, List[str]]:
        missing = [obj for obj in expected_objects if obj not in self.objects]
        return len(missing) == 0, missing

    def validate_version(self, expected_version: int) -> bool:
        if self.manifest_version != expected_version:
            raise ManifestVersionError(
                f"unsupported manifest version {self.manifest_version}; expected {expected_version}"
            )
        return True

    def validate_schema_integrity(self, expected_schemas: Dict[str, Dict[str, str]]) -> bool:
        for obj_name, exp_schema in expected_schemas.items():
            if obj_name not in self.objects:
                raise SchemaIntegrityError(f"manifest missing schema for object: {obj_name}")
            actual_schema = self.objects[obj_name].schema_definition
            for col_name, col_type in exp_schema.items():
                if col_name not in actual_schema:
                    raise SchemaIntegrityError(
                        f"schema drift on {obj_name}: missing column {col_name}"
                    )
                if actual_schema[col_name].upper() != col_type.upper():
                    raise SchemaIntegrityError(
                        f"type mismatch on {obj_name}.{col_name}: expected {col_type}, got {actual_schema[col_name]}"
                    )
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "manifest_id": self.manifest_id,
            "manifest_version": self.manifest_version,
            "collector_id": self.collector_id,
            "scope_id": self.scope_id,
            "lineage": self.lineage.to_dict(),
            "objects": {k: v.to_dict() for k, v in self.objects.items()},
            "overall_data_hash": self.overall_data_hash,
            "manifest_signature": self.manifest_signature,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ApprovedExportManifest":
        objs = {k: ExportObjectManifest.from_dict(v) for k, v in data.get("objects", {}).items()}
        return cls(
            manifest_id=data["manifest_id"],
            collector_id=data["collector_id"],
            scope_id=data["scope_id"],
            lineage=SourceLineage.from_dict(data["lineage"]),
            objects=objs,
            manifest_version=data.get("manifest_version", 1),
            overall_data_hash=data.get("overall_data_hash", ""),
            manifest_signature=data.get("manifest_signature"),
            created_at=data.get("created_at", 0.0),
        )


@dataclass
class PartialAccessScope:
    scope_id: str
    source_id: str
    accessible_objects: Set[str] = field(default_factory=set)
    inaccessible_objects: Set[str] = field(default_factory=set)
    permitted_columns: Dict[str, Set[str]] = field(default_factory=dict)
    inaccessible_columns: Dict[str, Set[str]] = field(default_factory=dict)
    predicates: Dict[str, str] = field(default_factory=dict)
    allowed_networks: Set[str] = field(default_factory=get_default_allowed_networks)
    allowed_roles: Set[str] = field(default_factory=set)
    is_partial: bool = True

    def is_object_accessible(self, name: str) -> bool:
        return name in self.accessible_objects and name not in self.inaccessible_objects

    def is_column_accessible(self, obj_name: str, col_name: str) -> bool:
        if not self.is_object_accessible(obj_name):
            return False
        if obj_name in self.inaccessible_columns and col_name in self.inaccessible_columns[obj_name]:
            return False
        if obj_name in self.permitted_columns:
            return col_name in self.permitted_columns[obj_name]
        return True

    def validate_network_access(self, client_ip: str) -> None:
        ip = ipaddress.ip_address(client_ip)
        for net_str in self.allowed_networks:
            try:
                net = ipaddress.ip_network(net_str, strict=False)
                if ip in net:
                    return
            except ValueError:
                if str(ip) == net_str:
                    return
        raise SecurityScopeViolationError(
            f"access from {client_ip} rejected: outside allowed networks {sorted(list(self.allowed_networks))}"
        )

    def validate_permission_boundary(self, requested_roles: Iterable[str]) -> None:
        if not self.allowed_roles:
            return
        for r in requested_roles:
            if r not in self.allowed_roles:
                raise SecurityScopeViolationError(
                    f"privilege escalation rejected: role '{r}' not in allowed roles {sorted(list(self.allowed_roles))}"
                )


class AuthorizedCollectorRegistry:
    def __init__(self):
        self._collectors: Dict[str, Dict[str, Any]] = {}

    def register_collector(
        self,
        collector_id: str,
        allowed_scopes: Iterable[str],
        secret_key: bytes,
        allowed_networks: Optional[Iterable[str]] = None,
        roles: Optional[Iterable[str]] = None,
    ) -> None:
        self._collectors[collector_id] = {
            "allowed_scopes": set(allowed_scopes),
            "secret_key": secret_key,
            "allowed_networks": set(allowed_networks if allowed_networks is not None else get_default_allowed_networks()),
            "roles": set(roles or []),
            "revoked": False,
        }

    def revoke_collector(self, collector_id: str) -> None:
        if collector_id in self._collectors:
            self._collectors[collector_id]["revoked"] = True

    def is_collector_authorized(self, collector_id: str, scope_id: str, client_ip: str) -> bool:
        col = self._collectors.get(collector_id)
        if not col or col["revoked"]:
            return False
        if scope_id not in col["allowed_scopes"]:
            return False
        ip = ipaddress.ip_address(client_ip)
        for net_str in col["allowed_networks"]:
            try:
                net = ipaddress.ip_network(net_str, strict=False)
                if ip in net:
                    return True
            except ValueError:
                if str(ip) == net_str:
                    return True
        return False

    def sign_manifest(self, manifest: ApprovedExportManifest, collector_id: str) -> ApprovedExportManifest:
        col = self._collectors.get(collector_id)
        if not col or col["revoked"]:
            raise UnauthorizedCollectorError(f"collector '{collector_id}' is not authorized to sign manifests")
        secret = col["secret_key"]
        digest = manifest.digest_for_signing()
        signature = hmac.new(secret, digest, hashlib.sha256).hexdigest()
        manifest.manifest_signature = signature
        return manifest

    def verify_manifest(self, manifest: ApprovedExportManifest) -> bool:
        if not manifest.manifest_signature:
            return False
        col = self._collectors.get(manifest.collector_id)
        if not col or col["revoked"]:
            return False
        secret = col["secret_key"]
        expected_sig = hmac.new(secret, manifest.digest_for_signing(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(manifest.manifest_signature, expected_sig)


@dataclass
class AbsentSourceEvidence:
    object_name: str
    reason: AbsentReason
    detail: str
    target_record_count: Optional[int] = None
    source_record_count: Optional[int] = None
    lineage_gap: bool = True
    impact: str = "Whole-source equivalence verification impossible due to missing source evidence"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "object_name": self.object_name,
            "reason": self.reason.value if isinstance(self.reason, AbsentReason) else str(self.reason),
            "detail": self.detail,
            "target_record_count": self.target_record_count,
            "source_record_count": self.source_record_count,
            "lineage_gap": self.lineage_gap,
            "impact": self.impact,
        }


@dataclass
class HonestCoverageVerdict:
    verdict: str
    exit_code: int
    is_whole_source_same: bool
    accessible_objects_verified: List[str]
    absent_source_evidence: List[AbsentSourceEvidence]
    total_upstream_objects: int
    object_verdicts: Dict[str, str] = field(default_factory=dict)
    discrepancies: List[Dict[str, Any]] = field(default_factory=list)
    summary: str = ""
    coverage_ratio: float = 0.0

    def __post_init__(self):
        if self.is_whole_source_same:
            raise HonestCoverageViolationError(
                "partial exports can never produce a whole-source same verdict"
            )
        if self.verdict == "same" and self.absent_source_evidence:
            raise HonestCoverageViolationError(
                "partial exports with absent source evidence cannot produce 'same' verdict"
            )
        if self.total_upstream_objects > 0:
            self.coverage_ratio = len(self.accessible_objects_verified) / float(self.total_upstream_objects)
        elif self.accessible_objects_verified or self.absent_source_evidence:
            total = len(self.accessible_objects_verified) + len(self.absent_source_evidence)
            self.coverage_ratio = len(self.accessible_objects_verified) / float(total)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "verdict": self.verdict,
            "exit_code": self.exit_code,
            "is_whole_source_same": self.is_whole_source_same,
            "accessible_objects_verified": self.accessible_objects_verified,
            "absent_source_evidence": [e.to_dict() for e in self.absent_source_evidence],
            "total_upstream_objects": self.total_upstream_objects,
            "coverage_ratio": self.coverage_ratio,
            "object_verdicts": self.object_verdicts,
            "discrepancies": self.discrepancies,
            "summary": self.summary,
        }


@dataclass
class NormalizedEnvelope:
    target_object: str
    records: List[Dict[str, Any]]
    lineage: SourceLineage


class NormalizedTargetMapping:
    def __init__(
        self,
        source_object: str,
        target_object: str,
        column_transforms: Dict[str, str],
        lineage: SourceLineage,
    ):
        self.source_object = source_object
        self.target_object = target_object
        self.column_transforms = column_transforms
        self.lineage = lineage

    @staticmethod
    def detect_and_reject_placeholders(records: Sequence[Dict[str, Any]]) -> None:
        for rec in records:
            if not isinstance(rec, dict):
                continue
            if rec.get("__is_placeholder__") or rec.get("_is_synthetic") or rec.get("__mock__"):
                raise PlaceholderDataProhibitedError("inventing placeholder or synthetic records is prohibited")
            for k, v in rec.items():
                if isinstance(v, str):
                    if v.startswith("[PLACEHOLDER]") or v.startswith("__MOCK_") or v.startswith("__SYNTHETIC_"):
                        raise PlaceholderDataProhibitedError(
                            f"placeholder sentinel value detected in field '{k}': '{v}'"
                        )
                if k == "id" and v == -1 and any("placeholder" in str(x).lower() for x in rec.values()):
                    raise PlaceholderDataProhibitedError("synthetic dummy record detected with negative ID sentinel")

    def transform(self, records: Sequence[Dict[str, Any]]) -> NormalizedEnvelope:
        self.detect_and_reject_placeholders(records)
        mapped_records: List[Dict[str, Any]] = []
        for rec in records:
            mapped = {}
            for src_col, dst_col in self.column_transforms.items():
                if src_col in rec:
                    mapped[dst_col] = rec[src_col]
            mapped_records.append(mapped)

        child_lineage = self.lineage.extend_chain(
            step="normalize_target_mapping",
            details={
                "source_object": self.source_object,
                "target_object": self.target_object,
                "record_count": len(mapped_records),
            },
        )
        return NormalizedEnvelope(
            target_object=self.target_object,
            records=mapped_records,
            lineage=child_lineage,
        )


class PartialAccessReconciler:
    def __init__(
        self,
        scope: PartialAccessScope,
        manifest: ApprovedExportManifest,
        registry: Optional[AuthorizedCollectorRegistry] = None,
        client_ip: Optional[str] = None,
        known_upstream_objects: Optional[Set[str]] = None,
    ):
        self.scope = scope
        self.manifest = manifest
        self.registry = registry
        self.client_ip = client_ip or "127.0.0.1"
        self.known_upstream_objects = known_upstream_objects or (
            scope.accessible_objects | scope.inaccessible_objects
        )

    def reconcile(
        self,
        source_data: Dict[str, Sequence[Dict[str, Any]]],
        target_data: Dict[str, Sequence[Dict[str, Any]]],
    ) -> HonestCoverageVerdict:
        self.scope.validate_network_access(self.client_ip)

        if self.registry:
            if not self.registry.verify_manifest(self.manifest):
                raise UnauthorizedCollectorError("manifest signature verification failed")
            if not self.registry.is_collector_authorized(
                self.manifest.collector_id, self.manifest.scope_id, self.client_ip
            ):
                raise UnauthorizedCollectorError(
                    f"collector '{self.manifest.collector_id}' unauthorized for scope '{self.manifest.scope_id}' from IP '{self.client_ip}'"
                )

        self.manifest.validate_version(1)

        for obj_name, recs in source_data.items():
            NormalizedTargetMapping.detect_and_reject_placeholders(recs)
        for obj_name, recs in target_data.items():
            NormalizedTargetMapping.detect_and_reject_placeholders(recs)

        accessible_verified: List[str] = []
        absent_evidence: List[AbsentSourceEvidence] = []
        object_verdicts: Dict[str, str] = {}
        discrepancies: List[Dict[str, Any]] = []

        for obj_name, obj_manifest in self.manifest.objects.items():
            if not self.scope.is_object_accessible(obj_name):
                continue

            src_recs = source_data.get(obj_name, [])
            tgt_recs = target_data.get(obj_name, [])

            filtered_src = []
            for r in src_recs:
                filtered_src.append({
                    k: v for k, v in r.items()
                    if self.scope.is_column_accessible(obj_name, k)
                })

            filtered_tgt = []
            for r in tgt_recs:
                filtered_tgt.append({
                    k: v for k, v in r.items()
                    if self.scope.is_column_accessible(obj_name, k)
                })

            src_digest = hashlib.sha256(json.dumps(filtered_src, sort_keys=True, default=str).encode("utf-8")).hexdigest()
            tgt_digest = hashlib.sha256(json.dumps(filtered_tgt, sort_keys=True, default=str).encode("utf-8")).hexdigest()

            if src_digest == tgt_digest and len(filtered_src) == len(filtered_tgt):
                object_verdicts[obj_name] = "same"
                accessible_verified.append(obj_name)
            else:
                object_verdicts[obj_name] = "different"
                discrepancies.append({
                    "object": obj_name,
                    "source_count": len(filtered_src),
                    "target_count": len(filtered_tgt),
                    "source_hash": src_digest,
                    "target_hash": tgt_digest,
                })

        for obj in self.scope.inaccessible_objects:
            tgt_count = len(target_data[obj]) if obj in target_data else None
            absent_evidence.append(
                AbsentSourceEvidence(
                    object_name=obj,
                    reason=AbsentReason.OWNER_ONLY_DATA,
                    detail="Source object inaccessible due to owner-only or restricted access constraints",
                    target_record_count=tgt_count,
                    source_record_count=None,
                )
            )
            object_verdicts[obj] = "absent"

        for obj in self.known_upstream_objects:
            if obj not in self.manifest.objects and obj not in self.scope.inaccessible_objects:
                tgt_count = len(target_data[obj]) if obj in target_data else None
                absent_evidence.append(
                    AbsentSourceEvidence(
                        object_name=obj,
                        reason=AbsentReason.RESTRICTED_ACCESS_NO_API,
                        detail="Upstream object absent from authorized export manifest",
                        target_record_count=tgt_count,
                        source_record_count=None,
                    )
                )
                object_verdicts[obj] = "absent"

        total_objects = len(self.known_upstream_objects)
        if len(discrepancies) > 0:
            canonical = VERDICTS["different"]
        else:
            canonical = VERDICTS["incomplete"]
        final_verdict = canonical.word
        exit_code = canonical.exit

        summary = (
            f"Partial export coverage: {len(accessible_verified)}/{total_objects} objects verified; "
            f"{len(absent_evidence)} absent source evidence reported; verdict={final_verdict}"
        )

        return HonestCoverageVerdict(
            verdict=final_verdict,
            exit_code=exit_code,
            is_whole_source_same=False,
            accessible_objects_verified=accessible_verified,
            absent_source_evidence=absent_evidence,
            total_upstream_objects=total_objects,
            object_verdicts=object_verdicts,
            discrepancies=discrepancies,
            summary=summary,
        )


# Decide ladders for Partial Access Scope and Honest Coverage

def _prove_authorized_partial_export_scope(facts: decide.Facts) -> bool:
    return bool(facts.get("authorized_scope") and not facts.get("refused"))


def _prove_signature_verified_manifest_bundle(facts: decide.Facts) -> bool:
    return bool(facts.get("signature_valid") and facts.get("manifest_complete") and not facts.get("refused"))


def _prove_provenance_chain_lineage_proof(facts: decide.Facts) -> bool:
    return bool(facts.get("provenance_verified") and not facts.get("refused"))


def _prove_absent_source_honest_coverage(facts: decide.Facts) -> bool:
    return bool(facts.get("honest_coverage") and not facts.get("refused"))


def _prove_refuse_unauthorized_scope_tampering(facts: decide.Facts) -> bool:
    return not bool(facts.get("allow_unauthorized_or_tampered"))


def partial_access_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "authorized_partial_export_scope",
            "verify partial access scope against authorized tenant boundaries and allowed networks",
            needs=(
                decide.Need(
                    "authorized export scope certified",
                    _prove_authorized_partial_export_scope,
                    "scope authorization check failed",
                ),
            ),
        ),
        decide.Rung(
            "signature_verified_manifest_bundle",
            "authenticate export bundle using HMAC signature and complete object manifest",
            needs=(
                decide.Need(
                    "manifest signature and completeness verified",
                    _prove_signature_verified_manifest_bundle,
                    "manifest signature invalid or parts incomplete",
                ),
            ),
        ),
        decide.Rung(
            "provenance_chain_lineage_proof",
            "verify source lineage, collector identity, and epoch sequence proof",
            needs=(
                decide.Need(
                    "provenance lineage verified",
                    _prove_provenance_chain_lineage_proof,
                    "provenance chain broken or unverified",
                ),
            ),
        ),
        decide.Rung(
            "absent_source_honest_coverage",
            "produce honest coverage verdicts recording absent evidence without false-same assertion",
            needs=(
                decide.Need(
                    "honest coverage certified",
                    _prove_absent_source_honest_coverage,
                    "honest coverage criteria not met",
                ),
            ),
        ),
        decide.Rung(
            "refuse_unauthorized_scope_tampering",
            "refuse partial access operations when scope violation, privilege escalation, or payload tampering is detected",
            needs=(
                decide.Need(
                    "scope tampering or privilege escalation refused",
                    _prove_refuse_unauthorized_scope_tampering,
                    "scope tampering or privilege escalation detected",
                ),
            ),
        ),
    )


def decide_partial_access(facts: Dict[str, Any]) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("partial access scope", partial_access_rungs(), fact_obj)


def _prove_all_accessible_verified(facts: decide.Facts) -> bool:
    return bool(facts.get("all_accessible_ok") and not facts.get("refused"))


def _prove_explicit_absent_evidence(facts: decide.Facts) -> bool:
    return bool(facts.get("has_absent_evidence") and not facts.get("refused"))


def _prove_discrepancy_divergence_review(facts: decide.Facts) -> bool:
    return bool(facts.get("discrepancies_detected") and not facts.get("refused"))


def _prove_partial_scope_certified(facts: decide.Facts) -> bool:
    return bool(facts.get("scope_certified") and not facts.get("refused"))


def _prove_refuse_uncovered_omission(facts: decide.Facts) -> bool:
    return not bool(facts.get("allow_uncovered_omission"))


def honest_coverage_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "all_accessible_verified",
            "all accessible objects in authorized scope match target exactly",
            needs=(
                decide.Need(
                    "all accessible objects match",
                    _prove_all_accessible_verified,
                    "not all accessible objects verified",
                ),
            ),
        ),
        decide.Rung(
            "explicit_absent_evidence",
            "record explicit AbsentSourceEvidence for unexported upstream objects",
            needs=(
                decide.Need(
                    "absent source evidence recorded",
                    _prove_explicit_absent_evidence,
                    "no absent source evidence present",
                ),
            ),
        ),
        decide.Rung(
            "discrepancy_divergence_review",
            "report divergence discrepancy requiring user review or repair before cutover",
            needs=(
                decide.Need(
                    "discrepancy divergence detected",
                    _prove_discrepancy_divergence_review,
                    "no discrepancies detected",
                ),
            ),
        ),
        decide.Rung(
            "partial_scope_certified",
            "certify partial export coverage with explicit boundary disclosure",
            needs=(
                decide.Need(
                    "partial scope certified",
                    _prove_partial_scope_certified,
                    "partial scope not certified",
                ),
            ),
        ),
        decide.Rung(
            "refuse_uncovered_omission",
            "refuse verification when unexported upstream objects lack explicit absent evidence",
            needs=(
                decide.Need(
                    "uncovered omission refused",
                    _prove_refuse_uncovered_omission,
                    "uncovered upstream omissions detected",
                ),
            ),
        ),
    )


def decide_honest_coverage(facts: Dict[str, Any]) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("honest coverage evaluation", honest_coverage_rungs(), fact_obj)

