import datetime
import decimal
import hashlib
import hmac
import os
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

P256 = 115792089237316195423570985008687907853269984665640564039457584007913129639747


class PrivateProofError(Exception):
    pass


class DebtSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    PARTIAL = "partial"
    CRITICAL = "critical"


class DebtCategory(str, Enum):
    UNVERIFIABLE_TRANSFORM = "unverifiable_transform"
    RESTRICTED_ACCESS = "restricted_access"
    LOSSY_MASKING = "lossy_masking"
    MISSING_SALT = "missing_salt"
    UNRESOLVED_DEPENDENCY = "unresolved_dependency"
    CROSS_DOMAIN_LEAK_RISK = "cross_domain_leak_risk"


class VerificationStatus(str, Enum):
    VERIFIED = "verified"
    DISCREPANCY = "discrepancy"
    DEBT_REJECTED = "debt_rejected"
    REFUSED = "refused"


@dataclass(frozen=True)
class SaltEpoch:
    version: int
    salt: bytes
    created_at: float = field(default_factory=time.time)
    retired_at: Optional[float] = None


class SaltRotationManager:
    def __init__(self, initial_salt: Optional[bytes] = None):
        self._epochs: Dict[int, SaltEpoch] = {}
        first_salt = initial_salt if initial_salt is not None else os.urandom(32)
        self._current_version = 1
        self._epochs[1] = SaltEpoch(version=1, salt=first_salt)

    def current_epoch(self) -> SaltEpoch:
        return self._epochs[self._current_version]

    def rotate(self, next_salt: Optional[bytes] = None) -> SaltEpoch:
        now = time.time()
        curr = self._epochs[self._current_version]
        self._epochs[self._current_version] = SaltEpoch(
            version=curr.version,
            salt=curr.salt,
            created_at=curr.created_at,
            retired_at=now,
        )
        self._current_version += 1
        new_salt = next_salt if next_salt is not None else os.urandom(32)
        epoch = SaltEpoch(version=self._current_version, salt=new_salt, created_at=now)
        self._epochs[self._current_version] = epoch
        return epoch

    def get_epoch(self, version: int) -> Optional[SaltEpoch]:
        return self._epochs.get(version)

    def list_epochs(self) -> List[SaltEpoch]:
        return [self._epochs[k] for k in sorted(self._epochs.keys())]


class TypeAwareBlinder:
    def __init__(
        self,
        secret_key: bytes,
        default_salt: Optional[bytes] = None,
        salt_manager: Optional[SaltRotationManager] = None,
    ):
        self.secret_key = secret_key
        self.salt_manager = salt_manager or SaltRotationManager(initial_salt=default_salt)

    def _canonicalize(self, value: Any, col_type: Optional[str] = None) -> bytes:
        if value is None:
            return b"__MIGKIT_NULL_SENTINEL__"

        t = (col_type or "").strip().lower()

        if t in ("int", "integer", "bigint", "smallint", "tinyint") or (isinstance(value, int) and not isinstance(value, bool)):
            return f"int:{int(value)}".encode("utf-8")

        if t in ("decimal", "numeric", "float", "real", "double") or isinstance(value, (decimal.Decimal, float)):
            if isinstance(value, decimal.Decimal):
                normalized = value.normalize()
                if normalized == 0:
                    normalized = decimal.Decimal("0")
                return f"dec:{normalized:f}".encode("utf-8")
            d = decimal.Decimal(str(value)).normalize()
            return f"dec:{d:f}".encode("utf-8")

        if t in ("bool", "boolean") or isinstance(value, bool):
            return b"bool:true" if value else b"bool:false"

        if t in ("uuid",) or isinstance(value, uuid.UUID):
            u = value if isinstance(value, uuid.UUID) else uuid.UUID(str(value).strip())
            return f"uuid:{str(u).lower()}".encode("utf-8")

        if t in ("date", "datetime", "timestamp", "timestamptz") or isinstance(value, (datetime.date, datetime.datetime)):
            if isinstance(value, datetime.datetime):
                if value.tzinfo is None:
                    utc_val = value.replace(tzinfo=datetime.timezone.utc)
                else:
                    utc_val = value.astimezone(datetime.timezone.utc)
                return f"dt:{utc_val.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}".encode("utf-8")
            return f"date:{value.isoformat()}".encode("utf-8")

        if t in ("bytes", "blob", "bytea", "binary") or isinstance(value, (bytes, bytearray)):
            return b"bytes:" + bytes(value)

        text = str(value)
        return b"text:" + text.encode("utf-8", "surrogatepass")

    def derive_key(self, domain: Optional[str] = None, salt: Optional[bytes] = None) -> bytes:
        d_tag = (domain or "default").encode("utf-8")
        s = salt if salt is not None else self.salt_manager.current_epoch().salt
        h = hmac.new(self.secret_key, b"migkit-blind-v1|" + d_tag + b"|" + s, hashlib.sha256)
        return h.digest()

    def blind(
        self,
        value: Any,
        col_type: Optional[str] = None,
        domain: Optional[str] = None,
        salt_version: Optional[int] = None,
        custom_salt: Optional[bytes] = None,
    ) -> str:
        if custom_salt is not None:
            salt = custom_salt
        elif salt_version is not None:
            ep = self.salt_manager.get_epoch(salt_version)
            if ep is None:
                raise ValueError(f"unknown salt epoch version {salt_version}")
            salt = ep.salt
        else:
            salt = self.salt_manager.current_epoch().salt

        key = self.derive_key(domain=domain, salt=salt)
        payload = self._canonicalize(value, col_type=col_type)
        token_digest = hmac.new(key, payload, hashlib.sha256).hexdigest()
        return "blind:" + token_digest[:24]

    def blind_scalar_hash(
        self,
        value: Any,
        col_type: Optional[str] = None,
        domain: Optional[str] = None,
        salt: Optional[bytes] = None,
    ) -> int:
        key = self.derive_key(domain=domain, salt=salt)
        payload = self._canonicalize(value, col_type=col_type)
        digest = hmac.new(key, payload, hashlib.sha256).digest()
        return int.from_bytes(digest, "big") % P256


@dataclass
class FKConstraint:
    parent_table: str
    parent_col: str
    child_table: str
    child_col: str
    name: Optional[str] = None

    def identifier(self) -> str:
        if self.name:
            return self.name
        return f"{self.parent_table}.{self.parent_col}->{self.child_table}.{self.child_col}"

    def domain_tag(self) -> str:
        return f"fk:{self.parent_table}.{self.parent_col}"


@dataclass
class ProofDebt:
    table: str
    column: Optional[str]
    category: DebtCategory
    severity: DebtSeverity
    reason: str
    impact_rows: int = 0
    requires_plaintext_audit: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "column": self.column,
            "category": self.category.value,
            "severity": self.severity.value,
            "reason": self.reason,
            "impact_rows": self.impact_rows,
            "requires_plaintext_audit": self.requires_plaintext_audit,
        }


@dataclass
class ProofDebtLedger:
    items: List[ProofDebt] = field(default_factory=list)

    def add(self, debt: ProofDebt) -> None:
        self.items.append(debt)

    def has_debt(self) -> bool:
        return len(self.items) > 0

    def has_critical_debt(self) -> bool:
        return any(d.severity == DebtSeverity.CRITICAL for d in self.items)

    def summary(self) -> str:
        if not self.items:
            return ""
        parts = [f"[{d.severity.value}:{d.category.value}] {d.table}.{d.column or '*'}: {d.reason}" for d in self.items]
        return "; ".join(parts)

    def to_list(self) -> List[Dict[str, Any]]:
        return [d.to_dict() for d in self.items]


@dataclass
class BlindedCounterexample:
    blinded_id: str
    bucket_index: Optional[int] = None
    issue: str = "discrepancy"
    table: Optional[str] = None

    def __repr__(self) -> str:
        return f"BlindedCounterexample(blinded_id={self.blinded_id!r}, bucket={self.bucket_index}, issue={self.issue!r})"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "blinded_id": self.blinded_id,
            "bucket_index": self.bucket_index,
            "issue": self.issue,
            "table": self.table,
        }


@dataclass
class MultisetHashProof:
    table_name: str
    row_count: int
    multiset_hash: str
    salt_epoch: int
    key_cols: Tuple[str, ...]
    value_cols: Tuple[str, ...]
    bucket_hashes: Dict[int, str] = field(default_factory=dict)
    blinded_key_set: Set[str] = field(default_factory=set)

    @classmethod
    def compute(
        cls,
        rows: Iterable[Dict[str, Any]],
        blinder: TypeAwareBlinder,
        key_cols: Sequence[str],
        value_cols: Sequence[str],
        table_name: str = "",
        salt_epoch: Optional[int] = None,
        domain_map: Optional[Dict[str, str]] = None,
        col_types: Optional[Dict[str, str]] = None,
        num_buckets: int = 16,
    ) -> "MultisetHashProof":
        d_map = domain_map or {}
        c_types = col_types or {}
        k_cols = tuple(key_cols)
        v_cols = tuple(value_cols)
        epoch = salt_epoch or blinder.salt_manager.current_epoch().version
        salt = blinder.salt_manager.get_epoch(epoch).salt if blinder.salt_manager.get_epoch(epoch) else None

        accumulator = 0
        bucket_accs = [0] * num_buckets
        blinded_keys: Set[str] = set()
        count = 0

        for r in rows:
            count += 1
            key_scalars = []
            key_tokens = []
            for kc in k_cols:
                domain = d_map.get(kc, f"{table_name}.{kc}" if table_name else kc)
                ctype = c_types.get(kc)
                val = r.get(kc)
                s_h = blinder.blind_scalar_hash(val, col_type=ctype, domain=domain, salt=salt)
                tok = blinder.blind(val, col_type=ctype, domain=domain, salt_version=epoch)
                key_scalars.append(s_h)
                key_tokens.append(tok)

            composite_key_token = ":".join(key_tokens)
            blinded_keys.add(composite_key_token)

            val_scalars = []
            for vc in v_cols:
                domain = d_map.get(vc, f"{table_name}.{vc}" if table_name else vc)
                ctype = c_types.get(vc)
                val = r.get(vc)
                s_h = blinder.blind_scalar_hash(val, col_type=ctype, domain=domain, salt=salt)
                val_scalars.append(s_h)

            row_term = 1
            for x in key_scalars + val_scalars:
                row_term = (row_term * (x + 3)) % P256

            accumulator = (accumulator + row_term) % P256
            b_idx = (key_scalars[0] if key_scalars else row_term) % num_buckets
            bucket_accs[b_idx] = (bucket_accs[b_idx] + row_term) % P256

        b_hashes = {i: f"{bucket_accs[i]:064x}" for i in range(num_buckets)}
        return cls(
            table_name=table_name,
            row_count=count,
            multiset_hash=f"{accumulator:064x}",
            salt_epoch=epoch,
            key_cols=k_cols,
            value_cols=v_cols,
            bucket_hashes=b_hashes,
            blinded_key_set=blinded_keys,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "row_count": self.row_count,
            "multiset_hash": self.multiset_hash,
            "salt_epoch": self.salt_epoch,
            "key_cols": list(self.key_cols),
            "value_cols": list(self.value_cols),
            "bucket_hashes": {str(k): v for k, v in self.bucket_hashes.items()},
        }


@dataclass
class BlindedDatasetProof:
    table: str
    row_count: int
    multiset_proof: MultisetHashProof
    debt_ledger: ProofDebtLedger = field(default_factory=ProofDebtLedger)
    fk_blinded_sets: Dict[str, Set[str]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "row_count": self.row_count,
            "multiset_proof": self.multiset_proof.to_dict(),
            "debt_ledger": self.debt_ledger.to_list(),
        }


class OwnerExecutedCollector:
    def __init__(
        self,
        table: str,
        secret_key: bytes,
        salt: Optional[bytes] = None,
        key_cols: Optional[Sequence[str]] = None,
        restricted_cols: Optional[Sequence[str]] = None,
        col_types: Optional[Dict[str, str]] = None,
        fk_domains: Optional[Dict[str, str]] = None,
        debt_rules: Optional[Sequence[ProofDebt]] = None,
        salt_manager: Optional[SaltRotationManager] = None,
    ):
        self.table = table
        self.key_cols = list(key_cols or ["id"])
        self.restricted_cols = list(restricted_cols or [])
        self.col_types = dict(col_types or {})
        self.fk_domains = dict(fk_domains or {})
        self.debt_rules = list(debt_rules or [])
        self.blinder = TypeAwareBlinder(
            secret_key=secret_key,
            default_salt=salt,
            salt_manager=salt_manager,
        )

    def collect(self, rows: Iterable[Dict[str, Any]]) -> BlindedDatasetProof:
        debt_ledger = ProofDebtLedger()
        for rule in self.debt_rules:
            debt_ledger.add(rule)

        cached_rows = list(rows)
        domain_map: Dict[str, str] = {}
        for c in self.key_cols + self.restricted_cols:
            if c in self.fk_domains:
                domain_map[c] = self.fk_domains[c]
            else:
                domain_map[c] = f"{self.table}.{c}"

        value_cols = [c for c in self.restricted_cols if c not in self.key_cols]
        ms_proof = MultisetHashProof.compute(
            rows=cached_rows,
            blinder=self.blinder,
            key_cols=self.key_cols,
            value_cols=value_cols,
            table_name=self.table,
            domain_map=domain_map,
            col_types=self.col_types,
        )

        fk_sets: Dict[str, Set[str]] = {}
        for col_name in set(self.key_cols + self.restricted_cols):
            if col_name in self.fk_domains:
                domain = self.fk_domains[col_name]
                col_set: Set[str] = set()
                ctype = self.col_types.get(col_name)
                for r in cached_rows:
                    val = r.get(col_name)
                    if val is not None:
                        b_tok = self.blinder.blind(val, col_type=ctype, domain=domain)
                        col_set.add(b_tok)
                fk_sets[col_name] = col_set

        return BlindedDatasetProof(
            table=self.table,
            row_count=len(cached_rows),
            multiset_proof=ms_proof,
            debt_ledger=debt_ledger,
            fk_blinded_sets=fk_sets,
        )


@dataclass
class VerificationSummary:
    status: VerificationStatus
    is_sound: bool
    row_count_source: int
    row_count_target: int
    count_delta: int
    mismatched_buckets: List[int] = field(default_factory=list)
    counterexamples: List[BlindedCounterexample] = field(default_factory=list)
    debt_ledger: ProofDebtLedger = field(default_factory=ProofDebtLedger)
    debt_summary: str = ""
    orphan_count: int = 0
    is_valid: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "is_sound": self.is_sound,
            "row_count_source": self.row_count_source,
            "row_count_target": self.row_count_target,
            "count_delta": self.count_delta,
            "mismatched_buckets": self.mismatched_buckets,
            "counterexamples": [c.to_dict() for c in self.counterexamples],
            "debt_summary": self.debt_summary,
            "orphan_count": self.orphan_count,
            "is_valid": self.is_valid,
        }

    def __repr__(self) -> str:
        return (
            f"VerificationSummary(status={self.status.value}, is_sound={self.is_sound}, "
            f"delta={self.count_delta}, buckets={self.mismatched_buckets}, "
            f"debt={self.debt_summary!r}, orphans={self.orphan_count})"
        )


class BlindedVerifier:
    def __init__(self, engine: Optional["PrivateProofEngine"] = None):
        self.engine = engine

    def verify_dataset(
        self,
        source_proof: BlindedDatasetProof,
        target_proof: BlindedDatasetProof,
    ) -> VerificationSummary:
        combined_debt = ProofDebtLedger()
        for d in source_proof.debt_ledger.items:
            combined_debt.add(d)
        for d in target_proof.debt_ledger.items:
            if d not in combined_debt.items:
                combined_debt.add(d)

        src_ms = source_proof.multiset_proof
        tgt_ms = target_proof.multiset_proof

        row_delta = tgt_ms.row_count - src_ms.row_count

        if src_ms.salt_epoch != tgt_ms.salt_epoch:
            combined_debt.add(
                ProofDebt(
                    table=source_proof.table,
                    column=None,
                    category=DebtCategory.MISSING_SALT,
                    severity=DebtSeverity.CRITICAL,
                    reason=f"salt epoch mismatch: source has epoch {src_ms.salt_epoch}, target has epoch {tgt_ms.salt_epoch}",
                )
            )

        if combined_debt.has_critical_debt():
            return VerificationSummary(
                status=VerificationStatus.DEBT_REJECTED,
                is_sound=False,
                row_count_source=src_ms.row_count,
                row_count_target=tgt_ms.row_count,
                count_delta=row_delta,
                debt_ledger=combined_debt,
                debt_summary=combined_debt.summary(),
                is_valid=False,
            )

        mismatched_buckets: List[int] = []
        for b_idx, s_hash in src_ms.bucket_hashes.items():
            t_hash = tgt_ms.bucket_hashes.get(b_idx)
            if s_hash != t_hash:
                mismatched_buckets.append(b_idx)

        counterexamples: List[BlindedCounterexample] = []
        if src_ms.multiset_hash != tgt_ms.multiset_hash or row_delta != 0:
            diff_keys = (src_ms.blinded_key_set ^ tgt_ms.blinded_key_set)
            for k in list(diff_keys)[:10]:
                counterexamples.append(
                    BlindedCounterexample(
                        blinded_id=k,
                        table=source_proof.table,
                        issue="multiset_mismatch",
                    )
                )
            if not counterexamples and mismatched_buckets:
                for b in mismatched_buckets[:5]:
                    counterexamples.append(
                        BlindedCounterexample(
                            blinded_id=f"bucket:{b}",
                            bucket_index=b,
                            table=source_proof.table,
                            issue="bucket_hash_divergence",
                        )
                    )

            status = VerificationStatus.DISCREPANCY
            is_sound = False
        else:
            status = VerificationStatus.VERIFIED
            is_sound = not combined_debt.has_critical_debt()

        return VerificationSummary(
            status=status,
            is_sound=is_sound,
            row_count_source=src_ms.row_count,
            row_count_target=tgt_ms.row_count,
            count_delta=row_delta,
            mismatched_buckets=mismatched_buckets,
            counterexamples=counterexamples,
            debt_ledger=combined_debt,
            debt_summary=combined_debt.summary(),
            is_valid=(status == VerificationStatus.VERIFIED),
        )

    def verify_fk_referential_integrity(
        self,
        parent_proof: BlindedDatasetProof,
        child_proof: BlindedDatasetProof,
        fk_name: Optional[str] = None,
        parent_col: Optional[str] = None,
        child_col: Optional[str] = None,
    ) -> VerificationSummary:
        p_col = parent_col
        c_col = child_col
        if not p_col or not c_col:
            if self.engine:
                for fk in self.engine.fk_constraints:
                    if (fk.parent_table == parent_proof.table and fk.child_table == child_proof.table) or (fk.name == fk_name):
                        p_col = fk.parent_col
                        c_col = fk.child_col
                        break

        p_set = parent_proof.fk_blinded_sets.get(p_col or "") if p_col else None
        if p_set is None:
            p_set = parent_proof.multiset_proof.blinded_key_set

        c_set = child_proof.fk_blinded_sets.get(c_col or "") if c_col else None
        if c_set is None:
            c_set = set()

        orphans = c_set - p_set
        orphan_count = len(orphans)

        counterexamples = [
            BlindedCounterexample(
                blinded_id=k,
                table=child_proof.table,
                issue="orphan_foreign_key",
            )
            for k in list(orphans)[:10]
        ]

        if orphan_count > 0:
            status = VerificationStatus.REFUSED
            is_sound = False
            is_valid = False
        else:
            status = VerificationStatus.VERIFIED
            is_sound = True
            is_valid = True

        return VerificationSummary(
            status=status,
            is_sound=is_sound,
            row_count_source=parent_proof.row_count,
            row_count_target=child_proof.row_count,
            count_delta=0,
            counterexamples=counterexamples,
            orphan_count=orphan_count,
            is_valid=is_valid,
        )


class PrivateProofEngine:
    def __init__(
        self,
        secret_key: bytes,
        salt_manager: Optional[SaltRotationManager] = None,
        default_salt: Optional[bytes] = None,
    ):
        self.secret_key = secret_key
        self.salt_manager = salt_manager or SaltRotationManager(initial_salt=default_salt)
        self.fk_constraints: List[FKConstraint] = []
        self.debt_rules: List[ProofDebt] = []
        self.domain_map: Dict[Tuple[str, str], str] = {}

    def register_fk(self, constraint: FKConstraint) -> None:
        self.fk_constraints.append(constraint)
        domain = constraint.domain_tag()
        self.domain_map[(constraint.parent_table, constraint.parent_col)] = domain
        self.domain_map[(constraint.child_table, constraint.child_col)] = domain

    def register_debt_rule(
        self,
        table: str,
        column: Optional[str],
        category: DebtCategory,
        severity: DebtSeverity,
        reason: str,
        impact_rows: int = 0,
        requires_plaintext_audit: bool = False,
    ) -> ProofDebt:
        debt = ProofDebt(
            table=table,
            column=column,
            category=category,
            severity=severity,
            reason=reason,
            impact_rows=impact_rows,
            requires_plaintext_audit=requires_plaintext_audit,
        )
        self.debt_rules.append(debt)
        return debt

    def get_fk_domains_for_table(self, table: str) -> Dict[str, str]:
        res = {}
        for (tbl, col), domain in self.domain_map.items():
            if tbl == table:
                res[col] = domain
        return res

    def get_debt_rules_for_table(self, table: str) -> List[ProofDebt]:
        return [d for d in self.debt_rules if d.table == table]

    def create_collector(
        self,
        table: str,
        key_cols: Optional[Sequence[str]] = None,
        restricted_cols: Optional[Sequence[str]] = None,
        col_types: Optional[Dict[str, str]] = None,
    ) -> OwnerExecutedCollector:
        fk_domains = self.get_fk_domains_for_table(table)
        debt_rules = self.get_debt_rules_for_table(table)
        return OwnerExecutedCollector(
            table=table,
            secret_key=self.secret_key,
            salt_manager=self.salt_manager,
            key_cols=key_cols,
            restricted_cols=restricted_cols,
            col_types=col_types,
            fk_domains=fk_domains,
            debt_rules=debt_rules,
        )

    def create_verifier(self) -> BlindedVerifier:
        return BlindedVerifier(engine=self)
