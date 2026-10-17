import enum
import hashlib
import json
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union


class IdentitySemanticsError(Exception):
    pass


class IdentityContractViolationError(IdentitySemanticsError):
    pass


class NonInjectiveKeyError(IdentitySemanticsError):
    def __init__(
        self,
        table: str,
        columns: Tuple[str, ...],
        key1: Any,
        key2: Any,
        target_collation: Optional[str] = None,
        reason: str = "non_injective_representation",
        details: Optional[Dict[str, Any]] = None,
    ):
        self.table = table
        self.columns = columns
        self.key1 = key1
        self.key2 = key2
        self.target_collation = target_collation
        self.reason = reason
        self.details = details or {}
        msg = (
            f"table '{table}': non-injective target key representation on columns "
            f"{columns} under target collation '{target_collation}'. "
            f"Distinct source keys '{key1}' and '{key2}' collide ({reason})."
        )
        super().__init__(msg)


class KeyStrategy(str, enum.Enum):
    REFUSE = "refuse"
    BINARY_COLLATION = "binary_collation"
    SURROGATE = "surrogate"
    ENCODED_KEY = "encoded_key"
    DIRECT = "direct"


def encode_lossless_key(val: Any) -> Any:
    if isinstance(val, str):
        return f"_k_{val.encode('utf-8').hex()}"
    if isinstance(val, (tuple, list)):
        return tuple(encode_lossless_key(x) for x in val)
    return val


def decode_lossless_key(val: Any) -> Any:
    if isinstance(val, str) and val.startswith("_k_"):
        return bytes.fromhex(val[3:]).decode("utf-8")
    if isinstance(val, (tuple, list)):
        return tuple(decode_lossless_key(x) for x in val)
    return val


@dataclass(frozen=True)
class CollationSemantics:
    name: str
    is_case_sensitive: bool = True
    is_accent_sensitive: bool = True
    is_binary: bool = False
    pad_space: bool = False
    unicode_normal_form: Optional[str] = None

    @classmethod
    def from_name(
        cls,
        name: Optional[str],
        default_case_sensitive: bool = True,
    ) -> "CollationSemantics":
        if not name:
            return cls(name="", is_case_sensitive=default_case_sensitive, is_binary=False)
        lower = name.lower()
        is_bin = any(x in lower for x in ("_bin", "binary", "_bin2", "utf8mb4_bin")) or name in ("C", "POSIX")
        if is_bin:
            return cls(name=name, is_case_sensitive=True, is_accent_sensitive=True, is_binary=True)

        is_cs = "_cs" in lower or "case_sensitive" in lower
        is_ci = "_ci" in lower or "case_insensitive" in lower or "_ai_ci" in lower
        if is_ci:
            case_sens = False
        elif is_cs:
            case_sens = True
        else:
            case_sens = default_case_sensitive

        is_ai = "_ai" in lower or "accent_insensitive" in lower
        accent_sens = not is_ai

        pad = "pad" in lower or "general" in lower or "cp1" in lower

        norm = None
        if "unicode" in lower or "uca" in lower or "0900" in lower:
            norm = "NFC"

        return cls(
            name=name,
            is_case_sensitive=case_sens,
            is_accent_sensitive=accent_sens,
            is_binary=False,
            pad_space=pad,
            unicode_normal_form=norm,
        )

    def fold_value(self, val: Any, unicode_norm: Optional[str] = None) -> Any:
        if isinstance(val, str):
            s = val
            norm = unicode_norm or self.unicode_normal_form
            if norm:
                s = unicodedata.normalize(norm, s)
            if not self.is_accent_sensitive:
                s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
            if not self.is_case_sensitive:
                s = s.casefold()
            if self.pad_space:
                s = s.rstrip(" ")
            return s
        if isinstance(val, (tuple, list)):
            return tuple(self.fold_value(x, unicode_norm=unicode_norm) for x in val)
        return val


def detect_non_injective_representation(
    keys: Sequence[Any],
    target_collation: Optional[Union[str, CollationSemantics]] = None,
    source_collation: Optional[Union[str, CollationSemantics]] = None,
    unicode_normalization: Optional[str] = None,
) -> Optional[Tuple[Any, Any, str]]:
    tgt_sem = (
        target_collation
        if isinstance(target_collation, CollationSemantics)
        else CollationSemantics.from_name(target_collation, default_case_sensitive=False)
    )

    seen: Dict[Any, Any] = {}
    for k in keys:
        folded = tgt_sem.fold_value(k, unicode_norm=unicode_normalization)
        if folded in seen:
            prev_k = seen[folded]
            if prev_k != k:
                reason = _diagnose_collision_reason(prev_k, k)
                return (prev_k, k, reason)
        else:
            seen[folded] = k
    return None


def _diagnose_collision_reason(k1: Any, k2: Any) -> str:
    s1 = str(k1)
    s2 = str(k2)
    if unicodedata.normalize("NFC", s1) == unicodedata.normalize("NFC", s2):
        return "unicode_normalization_collision"
    if unicodedata.normalize("NFKD", s1) == unicodedata.normalize("NFKD", s2):
        return "unicode_normalization_collision"
    if s1.casefold() == s2.casefold():
        return "case_sensitivity_collision"
    if s1.rstrip(" ") == s2.rstrip(" "):
        return "whitespace_padding_collision"
    return "non_injective_representation"


@dataclass
class BulkApplyResult:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    total: int = 0


@dataclass
class IdentityContract:
    table: str
    key_columns: Tuple[str, ...]
    strategy: KeyStrategy = KeyStrategy.REFUSE
    source_collation: Optional[str] = None
    target_collation: Optional[str] = None
    source_case_sensitive: bool = True
    unicode_normalization: Optional[str] = None
    allow_keyless: bool = False

    def __post_init__(self):
        if isinstance(self.key_columns, list):
            self.key_columns = tuple(self.key_columns)
        if isinstance(self.strategy, str):
            self.strategy = KeyStrategy(self.strategy)

    @property
    def target_semantics(self) -> CollationSemantics:
        if self.strategy == KeyStrategy.BINARY_COLLATION:
            return CollationSemantics(name="binary", is_case_sensitive=True, is_binary=True)
        return CollationSemantics.from_name(self.target_collation, default_case_sensitive=False)

    def source_identity(self, row: Dict[str, Any]) -> Tuple[Any, ...]:
        if not self.key_columns:
            if not self.allow_keyless:
                raise IdentityContractViolationError(f"table '{self.table}' has no key columns")
            payload = json.dumps(row, sort_keys=True, default=str)
            return (hashlib.sha256(payload.encode("utf-8")).hexdigest(),)
        return tuple(row.get(col) for col in self.key_columns)

    def target_identity_key(self, row: Dict[str, Any]) -> Tuple[Any, ...]:
        src_id = self.source_identity(row)
        if self.strategy == KeyStrategy.ENCODED_KEY:
            return tuple(encode_lossless_key(v) for v in src_id)
        if self.strategy == KeyStrategy.SURROGATE:
            raw = ":".join(str(v) for v in src_id)
            h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
            return (f"surr_{h[:16]}",)
        return src_id

    def target_equivalence_key(self, row: Dict[str, Any]) -> Tuple[Any, ...]:
        tgt_key = self.target_identity_key(row)
        if self.strategy in (KeyStrategy.ENCODED_KEY, KeyStrategy.SURROGATE):
            return tuple(self.target_semantics.fold_value(v) for v in tgt_key)
        return tuple(
            self.target_semantics.fold_value(v, unicode_norm=self.unicode_normalization)
            for v in tgt_key
        )

    def validate_injectivity(self, rows: Sequence[Dict[str, Any]]) -> None:
        seen_equiv: Dict[Tuple[Any, ...], Tuple[Any, ...]] = {}
        for r in rows:
            src_id = self.source_identity(r)
            equiv = self.target_equivalence_key(r)
            if equiv in seen_equiv:
                prev_src_id = seen_equiv[equiv]
                if prev_src_id != src_id:
                    reason = _diagnose_collision_reason(prev_src_id, src_id)
                    raise NonInjectiveKeyError(
                        table=self.table,
                        columns=self.key_columns,
                        key1=prev_src_id,
                        key2=src_id,
                        target_collation=self.target_collation,
                        reason=reason,
                    )
            else:
                seen_equiv[equiv] = src_id

    def dedup_batch(
        self,
        rows: Sequence[Dict[str, Any]],
        keep: str = "last",
    ) -> List[Dict[str, Any]]:
        dedup_map: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        order: List[Tuple[Any, ...]] = []

        for r in rows:
            src_id = self.source_identity(r)
            if src_id not in dedup_map:
                order.append(src_id)
                dedup_map[src_id] = r
            else:
                if keep == "last":
                    dedup_map[src_id] = r

        deduped = [dedup_map[k] for k in order]
        self.validate_injectivity(deduped)
        return deduped

    def apply_row(
        self,
        state: Dict[Tuple[Any, ...], Dict[str, Any]],
        row: Dict[str, Any],
        op: str = "UPSERT",
    ) -> Dict[str, Any]:
        src_id = self.source_identity(row)
        tgt_key = self.target_identity_key(row)
        equiv = self.target_equivalence_key(row)

        for existing_key, existing_row in state.items():
            existing_equiv = self.target_equivalence_key(existing_row)
            if existing_equiv == equiv:
                existing_src = self.source_identity(existing_row)
                if existing_src != src_id:
                    reason = _diagnose_collision_reason(existing_src, src_id)
                    raise NonInjectiveKeyError(
                        table=self.table,
                        columns=self.key_columns,
                        key1=existing_src,
                        key2=src_id,
                        target_collation=self.target_collation,
                        reason=reason,
                    )

        state[tgt_key] = dict(row)
        return state[tgt_key]

    def apply_bulk(
        self,
        state: Dict[Tuple[Any, ...], Dict[str, Any]],
        rows: Sequence[Dict[str, Any]],
    ) -> BulkApplyResult:
        deduped = self.dedup_batch(rows, keep="last")
        res = BulkApplyResult(total=len(deduped))

        for r in deduped:
            tgt_key = self.target_identity_key(r)
            if tgt_key in state:
                if state[tgt_key] == r:
                    res.unchanged += 1
                else:
                    self.apply_row(state, r)
                    res.updated += 1
            else:
                self.apply_row(state, r)
                res.inserted += 1

        return res

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "key_columns": list(self.key_columns),
            "strategy": self.strategy.value,
            "source_collation": self.source_collation,
            "target_collation": self.target_collation,
            "source_case_sensitive": self.source_case_sensitive,
            "unicode_normalization": self.unicode_normalization,
            "allow_keyless": self.allow_keyless,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "IdentityContract":
        return cls(
            table=d["table"],
            key_columns=tuple(d["key_columns"]),
            strategy=KeyStrategy(d.get("strategy", KeyStrategy.REFUSE.value)),
            source_collation=d.get("source_collation"),
            target_collation=d.get("target_collation"),
            source_case_sensitive=bool(d.get("source_case_sensitive", True)),
            unicode_normalization=d.get("unicode_normalization"),
            allow_keyless=bool(d.get("allow_keyless", False)),
        )


def verify_bulk_and_row_parity(
    rows: Sequence[Dict[str, Any]],
    contract: IdentityContract,
) -> bool:
    bulk_state: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    contract.apply_bulk(bulk_state, rows)

    row_state: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for r in rows:
        contract.apply_row(row_state, r)

    return bulk_state == row_state
