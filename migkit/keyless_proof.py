"""Prove keyless multisets at scale (Row 615: RCH07.keyless-proof).

Provides duplicate-sensitive multiset fingerprints with exact escalation,
external K-way merge-sort spill for billion-row and high-skew tables, and a
deterministic row-identity strategy where CDC or targeted repair needs it.

Avoids XOR-only acceptance (where even-multiplicity duplicates cancel out),
sample-only acceptance (where equal-count non-sampled corruption passes), and
synthetic-key source schema mutation. Enforces canonicalization and injective
length-prefixed row framing contracts via `migkit.canon`, `migkit.render`, and
`migkit.rowtext`.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import collections
import dataclasses
import enum
import hashlib
import heapq
import json
import math
import os
import pathlib
import tempfile
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from . import canon, decide, render, rowtext

# -----------------------------------------------------------------------------
# Care Badges and Core Enums
# -----------------------------------------------------------------------------

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class MultisetProofMode(str, enum.Enum):
    """Strategies for proving keyless multisets at scale."""

    SQL_BUCKET_MULTISET_DIGEST = "sql_bucket_multiset_digest"
    EXTERNAL_SORT_SPILL_MULTISET = "external_sort_spill_multiset"
    IN_MEMORY_CANONICAL_MULTISET = "in_memory_canonical_multiset"


class RowIdentityMode(str, enum.Enum):
    """Deterministic row-identity strategies for keyless CDC and repair."""

    CANONICAL_FULL_ROW_MULTIPLICITY = "canonical_full_row_multiplicity"
    FENCED_PHYSICAL_LOCATOR_VERIFIED = "fenced_physical_locator_verified"
    BUCKET_MULTISET_RECONCILE = "bucket_multiset_reconcile"


class KeylessChangeOp(str, enum.Enum):
    """Row mutation operations on a keyless table."""

    INSERT = "insert"
    DELETE = "delete"
    UPDATE = "update"


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class KeylessProofError(Exception):
    """Base exception for keyless multiset proof and identity failures."""


class KeylessCanonicalizationError(KeylessProofError):
    """Raised when a row violates schema arity, column contract, or comparability."""


class KeylessSpillError(KeylessProofError):
    """Raised when external merge-sort spill encounters corruption or insufficient disk."""


class KeylessIdentityError(KeylessProofError):
    """Raised when deterministic keyless CDC/repair cannot uniquely match expected multiplicity."""


class KeylessLocatorFenceError(KeylessIdentityError):
    """Raised when a physical row locator fails epoch fencing or canonical hash verification."""


class UnsafeEndpointError(KeylessProofError):
    """Raised when an endpoint host is outside loopback or RFC1918 10.0.0.x test bounds."""


# -----------------------------------------------------------------------------
# Host Safety Validation
# -----------------------------------------------------------------------------

def validate_safe_endpoint(host: str) -> str:
    """Ensure endpoint uses safe loopback (127.0.0.1, localhost) or 10.0.0.x."""
    cleaned = (host or "").strip()
    if not cleaned:
        raise UnsafeEndpointError("endpoint host cannot be empty")
    if cleaned in ("127.0.0.1", "localhost") or cleaned.startswith("10.0.0."):
        return cleaned
    raise UnsafeEndpointError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, or 10.0.0.x allowed"
    )


# -----------------------------------------------------------------------------
# Schema & Canonicalization Contract
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class KeylessColumnSpec:
    """Canonical column specification for a keyless table."""

    name: str
    canon_class: str
    nullable: bool = True

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise KeylessCanonicalizationError("column name cannot be empty")
        mapped = canon.NEUTRAL_TO_CANON.get(self.canon_class, self.canon_class)
        if mapped not in canon.CLASSES and mapped not in (
            canon.INTERVAL,
            canon.VECTOR,
            canon.SPARSE,
            canon.GEOMETRY,
            canon.BSON,
            canon.VARIANT,
        ):
            raise KeylessCanonicalizationError(
                f"unsupported canonical class '{self.canon_class}' for column '{self.name}'"
            )


@dataclass(frozen=True)
class KeylessTableContract:
    """Canonicalization and multiplicity contract for a keyless relation."""

    table_name: str
    columns: Tuple[KeylessColumnSpec, ...]
    digest_salt: str = "migkit-keyless-v1"
    reject_uncomparable: bool = True

    def __post_init__(self) -> None:
        if not self.table_name or not self.table_name.strip():
            raise KeylessCanonicalizationError("table_name cannot be empty")
        if not self.columns:
            raise KeylessCanonicalizationError(
                f"keyless table '{self.table_name}' must declare at least one column"
            )
        seen = set()
        for col in self.columns:
            if col.name in seen:
                raise KeylessCanonicalizationError(
                    f"duplicate column '{col.name}' in keyless contract for '{self.table_name}'"
                )
            seen.add(col.name)


class KeylessRowCanonicalizer:
    """Encodes rows into injective length-prefixed canonical text using `migkit.canon`
    and `migkit.rowtext`.

    Guarantees:
    1. SQL NULL (`None`) renders to `None` and frames as `N:` in `rowtext.encode`,
       whereas empty string `""` renders to `""` and frames as `0:`, and literal
       `"N"` frames as `1:N`. They never collide.
    2. Delimiters inside values (`"a|b"`, `"c"` vs `"a"`, `"b|c"`) frame with exact
       character lengths (`3:a|b|1:c` vs `1:a|3:b|c`) and never collide.
    3. Values that `canon.render_value` marks uncomparable (`canon.is_uncomparable`)
       raise `KeylessCanonicalizationError` when `reject_uncomparable=True`.
    """

    def __init__(self, contract: KeylessTableContract) -> None:
        self.contract = contract
        self._renderers: Tuple[Callable[[Any], Optional[str]], ...] = tuple(
            render.digest_renderer(col.canon_class) for col in contract.columns
        )

    def extract_ordered_values(
        self, row: Union[Sequence[Any], Mapping[str, Any]]
    ) -> Tuple[Any, ...]:
        """Extract column values in strict schema order, validating arity and nullability."""
        cols = self.contract.columns
        if isinstance(row, Mapping):
            unknown = set(row.keys()) - {c.name for c in cols}
            if unknown:
                raise KeylessCanonicalizationError(
                    f"row contains undeclared columns {sorted(unknown)} for '{self.contract.table_name}'"
                )
            values: List[Any] = []
            for col in cols:
                if col.name not in row:
                    raise KeylessCanonicalizationError(
                        f"missing required column '{col.name}' in mapping row for '{self.contract.table_name}'"
                    )
                val = row[col.name]
                if val is None and not col.nullable:
                    raise KeylessCanonicalizationError(
                        f"NULL value violates non-nullable column '{col.name}' in '{self.contract.table_name}'"
                    )
                values.append(val)
            return tuple(values)

        if isinstance(row, (str, bytes, bytearray)):
            raise KeylessCanonicalizationError(
                "row must be a sequence of column values or mapping, not a scalar string/bytes"
            )

        seq = tuple(row)
        if len(seq) != len(cols):
            raise KeylessCanonicalizationError(
                f"row arity mismatch for '{self.contract.table_name}': expected {len(cols)} columns, got {len(seq)}"
            )
        for col, val in zip(cols, seq):
            if val is None and not col.nullable:
                raise KeylessCanonicalizationError(
                    f"NULL value violates non-nullable column '{col.name}' in '{self.contract.table_name}'"
                )
        return seq

    def canonical_row_text(
        self, row: Union[Sequence[Any], Mapping[str, Any]]
    ) -> str:
        """Return injective length-prefixed canonical string for `row`."""
        ordered = self.extract_ordered_values(row)
        rendered: List[Optional[str]] = []
        for col, fn, val in zip(self.contract.columns, self._renderers, ordered):
            if val is None:
                rendered.append(None)
                continue
            text = fn(val)
            if text is not None and text.startswith(canon.UNCOMPARABLE):
                if self.contract.reject_uncomparable:
                    raise KeylessCanonicalizationError(
                        f"uncomparable value {text!r} in column '{col.name}' of '{self.contract.table_name}'"
                    )
            rendered.append(text)
        return rowtext.encode(rendered)

    def row_hashes(self, canonical_text: str) -> Tuple[int, int]:
        """Compute two independent 64-bit unsigned integer lanes from salted SHA-256."""
        payload = f"{self.contract.digest_salt}\x00{canonical_text}".encode("utf-8")
        digest = hashlib.sha256(payload).digest()
        h1 = int.from_bytes(digest[0:8], byteorder="big", signed=False)
        h2 = int.from_bytes(digest[8:16], byteorder="big", signed=False)
        return h1, h2

    def bucket_for_hashes(self, h1: int, bucket_bits: int) -> int:
        """Partition a 64-bit row hash into `2**bucket_bits` buckets using top bits."""
        if bucket_bits <= 0:
            return 0
        if bucket_bits > 32:
            raise ValueError(f"bucket_bits={bucket_bits} exceeds 32-bit partition bound")
        return h1 >> (64 - bucket_bits)


# -----------------------------------------------------------------------------
# Duplicate-Sensitive Multiset Fingerprint (Abelian Homomorphic Sum)
# -----------------------------------------------------------------------------

def multiplicity_entry_hash(salt: str, canonical_text: str, count: int) -> int:
    """Compute a 128-bit integer commitment binding a canonical row to its exact multiplicity."""
    if count <= 0:
        return 0
    framed = f"{salt}\x00{len(canonical_text)}:{canonical_text}#{int(count)}".encode("utf-8")
    digest = hashlib.sha256(framed).digest()
    return int.from_bytes(digest[:16], byteorder="big", signed=False)


@dataclass(frozen=True)
class MultisetFingerprint:
    """Duplicate-sensitive commutative multiset fingerprint over arbitrary-precision Z.

    Why this never suffers from XOR cancellation or count-only collisions:
    - Every row with canonical encoding `c` and multiplicity `m >= 1` adds:
        row_count  += m
        sum_h1     += m * h1(c)
        sum_h2     += m * h2(c)
        sum_h1_sq  += m * (h1(c) ** 2)
        sum_h1_cube += m * (h1(c) ** 3)
      over exact Python integers (never wrapped mod 2, never XORed).
    - Reordering rows in any permutation preserves all five sums identically.
    - Even-multiplicity duplicates (e.g. 2 copies of row A vs 2 copies of row B,
      or 2 copies of row A vs 0 copies) multiply `h1(A)` by 2 rather than cancelling
      to 0 as XOR does.
    - Equal-count corruption (e.g. [A, A, B] vs [A, B, B] where both have count=3
      and identical distinct set {A, B}) produces distinct `sum_h1`, `sum_h2`,
      `sum_h1_sq`, and `sum_h1_cube`.
    - When exact multiplicity grouping has been performed (via bucket escalation or
      external merge-sort), `multiplicity_commitment` sums the 128-bit commitment
      of each `(canonical_row, exact_count)` pair and `distinct_row_count` records
      the number of unique canonical rows.
    """

    row_count: int = 0
    sum_h1: int = 0
    sum_h2: int = 0
    sum_h1_sq: int = 0
    sum_h1_cube: int = 0
    distinct_row_count: Optional[int] = None
    multiplicity_commitment: Optional[int] = None

    def add_row_hashes(self, h1: int, h2: int, multiplicity: int = 1) -> MultisetFingerprint:
        """Return a new fingerprint with `multiplicity` copies of `(h1, h2)` added."""
        m = int(multiplicity)
        if m < 0:
            raise ValueError(f"multiplicity must be non-negative, got {m}")
        if m == 0:
            return self
        h1_sq = h1 * h1
        h1_cube = h1_sq * h1
        return MultisetFingerprint(
            row_count=self.row_count + m,
            sum_h1=self.sum_h1 + m * h1,
            sum_h2=self.sum_h2 + m * h2,
            sum_h1_sq=self.sum_h1_sq + m * h1_sq,
            sum_h1_cube=self.sum_h1_cube + m * h1_cube,
            distinct_row_count=None,
            multiplicity_commitment=None,
        )

    def combine(self, other: MultisetFingerprint) -> MultisetFingerprint:
        """Homomorphically combine two disjoint partition/bucket fingerprints."""
        both_have_mult = (
            self.multiplicity_commitment is not None
            and other.multiplicity_commitment is not None
            and self.distinct_row_count is not None
            and other.distinct_row_count is not None
        )
        return MultisetFingerprint(
            row_count=self.row_count + other.row_count,
            sum_h1=self.sum_h1 + other.sum_h1,
            sum_h2=self.sum_h2 + other.sum_h2,
            sum_h1_sq=self.sum_h1_sq + other.sum_h1_sq,
            sum_h1_cube=self.sum_h1_cube + other.sum_h1_cube,
            distinct_row_count=(
                (self.distinct_row_count or 0) + (other.distinct_row_count or 0)
                if both_have_mult
                else None
            ),
            multiplicity_commitment=(
                (self.multiplicity_commitment or 0) + (other.multiplicity_commitment or 0)
                if both_have_mult
                else None
            ),
        )

    def matches(self, other: MultisetFingerprint) -> bool:
        """Compare all moments and (if present on both) exact multiplicity commitments."""
        base_eq = (
            self.row_count == other.row_count
            and self.sum_h1 == other.sum_h1
            and self.sum_h2 == other.sum_h2
            and self.sum_h1_sq == other.sum_h1_sq
            and self.sum_h1_cube == other.sum_h1_cube
        )
        if not base_eq:
            return False
        if (
            self.multiplicity_commitment is not None
            and other.multiplicity_commitment is not None
        ):
            return (
                self.multiplicity_commitment == other.multiplicity_commitment
                and self.distinct_row_count == other.distinct_row_count
            )
        return True

    def delta(self, other: MultisetFingerprint) -> Tuple[int, int, int, int, int]:
        """Return `(d_count, d_h1, d_h2, d_h1_sq, d_h1_cube)` of `self - other`."""
        return (
            self.row_count - other.row_count,
            self.sum_h1 - other.sum_h1,
            self.sum_h2 - other.sum_h2,
            self.sum_h1_sq - other.sum_h1_sq,
            self.sum_h1_cube - other.sum_h1_cube,
        )


@dataclass(frozen=True)
class MultiplicityDiscrepancy:
    """Exact multiplicity mismatch for one canonical row between source and target."""

    canonical_row: str
    decoded_values: Tuple[Optional[str], ...]
    source_count: int
    target_count: int

    @property
    def net_delta(self) -> int:
        """Source multiplicity minus target multiplicity."""
        return self.source_count - self.target_count


# -----------------------------------------------------------------------------
# Decision Engine Ladders (`decide.Rung` / `decide.climb`)
# -----------------------------------------------------------------------------

def derive_scale_facts(raw_facts: Mapping[str, Any]) -> Dict[str, Any]:
    """Derive memory and spill bounds from measured resource facts without magic numbers.

    Reads measured `available_ram_bytes`, `measured_row_bytes`, `estimated_rows`,
    `bucket_count`, `max_bucket_rows`, `spill_free_bytes`, and `spill_amplification_factor`
    from `raw_facts` and populates derived boolean predicates if not already set.
    """
    out = dict(raw_facts)

    row_bytes = int(out.get("measured_row_bytes") or 0)
    avail_ram = int(out.get("available_ram_bytes") or 0)
    est_rows = int(out.get("estimated_rows") or 0)
    bucket_count = int(out.get("bucket_count") or 0)

    if row_bytes > 0 and avail_ram > 0:
        memory_row_limit = avail_ram // row_bytes
        out.setdefault("memory_row_limit", memory_row_limit)
    else:
        memory_row_limit = int(out.get("memory_row_limit") or 0)

    if "max_bucket_rows" not in out and est_rows > 0 and bucket_count > 0:
        skew_factor = float(out.get("measured_bucket_skew_factor") or 1.0)
        out["max_bucket_rows"] = int(math.ceil((est_rows / bucket_count) * skew_factor))

    max_bucket_rows = int(out.get("max_bucket_rows") or 0)
    if "bucket_fits_memory" not in out and memory_row_limit > 0:
        out["bucket_fits_memory"] = max_bucket_rows <= memory_row_limit

    if "table_fits_memory" not in out and memory_row_limit > 0 and est_rows > 0:
        out["table_fits_memory"] = est_rows <= memory_row_limit

    if "required_spill_bytes" not in out and est_rows > 0 and row_bytes > 0:
        amp = float(out.get("spill_amplification_factor") or 1.0)
        out["required_spill_bytes"] = int(math.ceil(est_rows * row_bytes * amp))

    req_spill = int(out.get("required_spill_bytes") or 0)
    free_spill = int(out.get("spill_free_bytes") or 0)
    if "spill_disk_fits" not in out and req_spill > 0:
        out["spill_disk_fits"] = free_spill >= req_spill

    return out


def multiset_proof_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for proving keyless multisets at scale."""
    return (
        decide.Rung(
            "sql_bucket_multiset_digest",
            "server-side bucketed multiset sum digest with exact multiplicity escalation",
            gives=frozenset({"keyless", "byte exact", "consistent as of", "resume by chunk"}),
            needs=(
                decide.Need(
                    "no synthetic-key source mutation",
                    lambda f: not bool(f.get("synthetic_key_source_mutation")),
                    "cannot add a synthetic key or mutate the source table to prove it",
                ),
                decide.Need(
                    "duplicate-sensitive full multiset digest",
                    lambda f: not bool(f.get("xor_only_digest"))
                    and not bool(f.get("sample_only_proof")),
                    "XOR-only or sample-only check cannot prove duplicate multiplicity",
                ),
                decide.Need(
                    "canonical injective row hash in server SQL",
                    lambda f: bool(f.get("server_canonical_hash")),
                    "both sides must evaluate the canonical injective row hash in SQL",
                ),
                decide.Need(
                    "largest hash bucket fits escalation memory budget",
                    lambda f: bool(f.get("bucket_fits_memory")),
                    "the largest hash bucket exceeds measured in-memory escalation budget",
                ),
            ),
            because="hashes buckets on the servers and escalates only differing buckets",
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "external_sort_spill_multiset",
            "external K-way merge-sort spill over canonical row runs with exact multiplicity count",
            gives=frozenset({"keyless", "byte exact", "consistent as of", "resume by chunk"}),
            needs=(
                decide.Need(
                    "no synthetic-key source mutation",
                    lambda f: not bool(f.get("synthetic_key_source_mutation")),
                    "cannot add a synthetic key or mutate the source table to prove it",
                ),
                decide.Need(
                    "duplicate-sensitive full multiset digest",
                    lambda f: not bool(f.get("xor_only_digest"))
                    and not bool(f.get("sample_only_proof")),
                    "XOR-only or sample-only check cannot prove duplicate multiplicity",
                ),
                decide.Need(
                    "measured spill disk headroom for external merge-sort runs",
                    lambda f: bool(f.get("spill_disk_fits")),
                    "spill directory lacks measured disk headroom for external merge-sort runs",
                ),
            ),
            because="bounds process RAM via external merge-sort runs on billion-row or skewed multisets",
            footprint=1,
            leaves="temporary sorted canonical run files removed after merge",
        ),
        decide.Rung(
            "in_memory_canonical_multiset",
            "in-memory canonical multiset counter with injective length-prefixed row encoding",
            gives=frozenset({"keyless", "byte exact", "consistent as of"}),
            needs=(
                decide.Need(
                    "no synthetic-key source mutation",
                    lambda f: not bool(f.get("synthetic_key_source_mutation")),
                    "cannot add a synthetic key or mutate the source table to prove it",
                ),
                decide.Need(
                    "duplicate-sensitive full multiset digest",
                    lambda f: not bool(f.get("xor_only_digest"))
                    and not bool(f.get("sample_only_proof")),
                    "XOR-only or sample-only check cannot prove duplicate multiplicity",
                ),
                decide.Need(
                    "table row count fits measured in-memory budget",
                    lambda f: bool(f.get("table_fits_memory")),
                    "table row count exceeds measured in-memory row budget",
                ),
            ),
            because="counts exact canonical row multiplicities directly in memory when table fits RAM",
            footprint=0,
            leaves="nothing",
        ),
    )


def choose_multiset_proof_strategy(
    facts: Union[Mapping[str, Any], decide.Facts],
    need: Sequence[str] = (),
    costs: Optional[decide.Costs] = None,
) -> decide.Choice:
    """Climb `multiset_proof_rungs` to select a keyless multiset proof strategy."""
    derived = derive_scale_facts(facts)
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(derived)
    for k, v in derived.items():
        if k not in fact_obj:
            fact_obj[k] = v
    return decide.climb(
        "keyless multiset proof strategy",
        multiset_proof_rungs(),
        fact_obj,
        need=tuple(need),
        costs=costs,
    )


def row_identity_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for deterministic row-identity in keyless CDC and repair."""
    return (
        decide.Rung(
            "canonical_full_row_multiplicity",
            "canonical full-row before-image with explicit duplicate ordinal and single-row limit",
            gives=frozenset({"keyless", "exact", "byte exact"}),
            needs=(
                decide.Need(
                    "no synthetic-key source schema mutation",
                    lambda f: not bool(f.get("synthetic_key_source_mutation")),
                    "cannot mutate source schema with a synthetic key column",
                ),
                decide.Need(
                    "full canonical before-image in change stream",
                    lambda f: bool(f.get("full_before_image")),
                    "change stream does not carry the full canonical before-image of the row",
                ),
                decide.Need(
                    "target single-row bounded mutation",
                    lambda f: bool(f.get("supports_single_row_limit")),
                    "target cannot bound keyless UPDATE/DELETE to a single duplicate instance",
                ),
            ),
            because="matches exact canonical before-image and mutates one duplicate instance at a time",
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "fenced_physical_locator_verified",
            "fenced physical tuple locator verified against canonical full-row hash",
            gives=frozenset({"keyless", "exact", "byte exact"}),
            needs=(
                decide.Need(
                    "no synthetic-key source schema mutation",
                    lambda f: not bool(f.get("synthetic_key_source_mutation")),
                    "cannot mutate source schema with a synthetic key column",
                ),
                decide.Need(
                    "stable physical tuple locator within snapshot fence",
                    lambda f: bool(f.get("physical_locator_available"))
                    and not bool(f.get("locator_invalidated_by_movement")),
                    "physical row locator is unavailable or invalidated by compaction or row movement",
                ),
                decide.Need(
                    "canonical row hash verification on physical locator",
                    lambda f: bool(f.get("verify_row_hash_on_locator")),
                    "physical locator apply must verify canonical row hash before mutation",
                ),
            ),
            because="addresses physical tuple slot directly when fenced and hash-verified",
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "bucket_multiset_reconcile",
            "hash-bucket multiset delta reconciliation replacing exact multiplicity count",
            gives=frozenset({"keyless", "exact", "resume by chunk"}),
            needs=(
                decide.Need(
                    "no synthetic-key source schema mutation",
                    lambda f: not bool(f.get("synthetic_key_source_mutation")),
                    "cannot mutate source schema with a synthetic key column",
                ),
                decide.Need(
                    "transactional bucket replacement on target",
                    lambda f: bool(f.get("transactional_bucket_replace")),
                    "target cannot atomically replace a hash bucket's rows in one transaction",
                ),
                decide.Need(
                    "bucket size within reconciliation budget",
                    lambda f: bool(f.get("bucket_reconcile_within_budget")),
                    "hash bucket size exceeds measured transactional reconciliation budget",
                ),
            ),
            because="reconciles exact canonical row multiplicities per hash bucket without single-row limit",
            footprint=1,
            leaves="rewritten rows in the affected hash bucket",
        ),
    )


def choose_row_identity_strategy(
    facts: Union[Mapping[str, Any], decide.Facts],
    need: Sequence[str] = (),
    costs: Optional[decide.Costs] = None,
) -> decide.Choice:
    """Climb `row_identity_rungs` to select a deterministic keyless CDC/repair strategy."""
    raw = dict(facts)
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(raw)
    for k, v in raw.items():
        if k not in fact_obj:
            fact_obj[k] = v
    return decide.climb(
        "keyless deterministic row identity",
        row_identity_rungs(),
        fact_obj,
        need=tuple(need),
        costs=costs,
    )


# -----------------------------------------------------------------------------
# External K-Way Merge-Sort Spill Engine for Billion-Row Keyless Multisets
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class SpillRunManifest:
    """Metadata and integrity proof for one sorted canonical multiset spill run."""

    run_index: int
    file_path: pathlib.Path
    distinct_rows_in_run: int
    total_rows_in_run: int
    byte_size: int
    sha256_hex: str


class ExternalMultisetSpillSorter:
    """Sorts and coalesces canonical `(canonical_row, multiplicity)` runs on disk
    using bounded in-memory buffers and K-way `heapq.merge`.

    Prevents OOM on billion-row keyless tables or skewed buckets where in-memory
    hash tables would exceed available RAM. Every spill file is length-framed and
    checksum-verified on readback so truncated or corrupted spill files raise
    `KeylessSpillError` rather than silently passing.
    """

    def __init__(
        self,
        canonicalizer: KeylessRowCanonicalizer,
        *,
        run_distinct_limit: int,
        spill_dir: Optional[Union[str, pathlib.Path]] = None,
        avail_disk_bytes: Optional[int] = None,
    ) -> None:
        if run_distinct_limit <= 0:
            raise ValueError(
                f"run_distinct_limit must be positive (derived from RAM / row_bytes), got {run_distinct_limit}"
            )
        self.canonicalizer = canonicalizer
        self.run_distinct_limit = int(run_distinct_limit)
        self._explicit_spill_dir = pathlib.Path(spill_dir) if spill_dir is not None else None
        self.avail_disk_bytes = avail_disk_bytes
        self.bytes_spilled: int = 0
        self.runs_written: List[SpillRunManifest] = []

    @staticmethod
    def _encode_run_record(canonical_row: str, count: int) -> bytes:
        body = f"{count}:{len(canonical_row)}:{canonical_row}\n".encode("utf-8")
        return body

    @staticmethod
    def _decode_run_line(line: bytes) -> Tuple[str, int]:
        try:
            text = line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise KeylessSpillError(f"corrupt UTF-8 in spill run record: {exc}") from exc
        if not text.endswith("\n"):
            raise KeylessSpillError("truncated spill run record: missing newline terminator")
        body = text[:-1]
        first_colon = body.find(":")
        if first_colon <= 0:
            raise KeylessSpillError(f"malformed multiplicity prefix in spill record: {body[:40]!r}")
        second_colon = body.find(":", first_colon + 1)
        if second_colon <= first_colon + 1:
            raise KeylessSpillError(f"malformed length prefix in spill record: {body[:40]!r}")
        try:
            count = int(body[:first_colon])
            expected_len = int(body[first_colon + 1 : second_colon])
        except ValueError as exc:
            raise KeylessSpillError(f"invalid integer header in spill record: {exc}") from exc
        payload = body[second_colon + 1 :]
        if count <= 0 or len(payload) != expected_len:
            raise KeylessSpillError(
                f"spill record length mismatch: header declared {expected_len} chars, got {len(payload)}"
            )
        return payload, count

    def _flush_run(
        self,
        buffer: Dict[str, int],
        target_dir: pathlib.Path,
        run_index: int,
    ) -> SpillRunManifest:
        run_path = target_dir / f"multiset_run_{run_index:06d}.spill"
        hasher = hashlib.sha256()
        total_rows = 0
        byte_size = 0

        sorted_items = sorted(buffer.items(), key=lambda kv: kv[0])
        with open(run_path, "wb") as fh:
            for canonical_row, count in sorted_items:
                rec = self._encode_run_record(canonical_row, count)
                if (
                    self.avail_disk_bytes is not None
                    and self.bytes_spilled + byte_size + len(rec) > self.avail_disk_bytes
                ):
                    raise KeylessSpillError(
                        f"spill disk budget exhausted: needed >{self.bytes_spilled + byte_size + len(rec)} bytes, "
                        f"available={self.avail_disk_bytes} bytes"
                    )
                fh.write(rec)
                hasher.update(rec)
                byte_size += len(rec)
                total_rows += count

        self.bytes_spilled += byte_size
        manifest = SpillRunManifest(
            run_index=run_index,
            file_path=run_path,
            distinct_rows_in_run=len(sorted_items),
            total_rows_in_run=total_rows,
            byte_size=byte_size,
            sha256_hex=hasher.hexdigest(),
        )
        self.runs_written.append(manifest)
        return manifest

    @staticmethod
    def _iter_verified_run(manifest: SpillRunManifest) -> Iterator[Tuple[str, int]]:
        if not manifest.file_path.exists():
            raise KeylessSpillError(f"spill run file missing: {manifest.file_path}")
        actual_bytes = manifest.file_path.stat().st_size
        if actual_bytes != manifest.byte_size:
            raise KeylessSpillError(
                f"spill run '{manifest.file_path.name}' byte size mismatch: "
                f"expected {manifest.byte_size}, got {actual_bytes}"
            )
        hasher = hashlib.sha256()
        records_read = 0
        rows_read = 0
        with open(manifest.file_path, "rb") as fh:
            for raw_line in fh:
                hasher.update(raw_line)
                canonical_row, count = ExternalMultisetSpillSorter._decode_run_line(raw_line)
                records_read += 1
                rows_read += count
                yield canonical_row, count

        if hasher.hexdigest() != manifest.sha256_hex:
            raise KeylessSpillError(
                f"spill run '{manifest.file_path.name}' checksum mismatch: "
                f"expected {manifest.sha256_hex}, got {hasher.hexdigest()}"
            )
        if records_read != manifest.distinct_rows_in_run or rows_read != manifest.total_rows_in_run:
            raise KeylessSpillError(
                f"spill run '{manifest.file_path.name}' row count mismatch on readback"
            )

    def spill_and_coalesce(
        self,
        rows: Iterable[Union[Sequence[Any], Mapping[str, Any]]],
        work_dir: pathlib.Path,
    ) -> Tuple[List[SpillRunManifest], Iterator[Tuple[str, int]]]:
        """Consume `rows`, flush bounded sorted runs to `work_dir`, and return a
        coalesced sorted `(canonical_row, total_multiplicity)` iterator."""
        work_dir.mkdir(parents=True, exist_ok=True)
        buffer: Dict[str, int] = {}
        manifests: List[SpillRunManifest] = []

        for row in rows:
            c_text = self.canonicalizer.canonical_row_text(row)
            buffer[c_text] = buffer.get(c_text, 0) + 1
            if len(buffer) >= self.run_distinct_limit:
                manifests.append(self._flush_run(buffer, work_dir, len(manifests)))
                buffer.clear()

        if buffer or not manifests:
            manifests.append(self._flush_run(buffer, work_dir, len(manifests)))
            buffer.clear()

        def _coalesced_stream() -> Iterator[Tuple[str, int]]:
            run_iters = [self._iter_verified_run(m) for m in manifests]
            merged = heapq.merge(*run_iters, key=lambda item: item[0])
            cur_row: Optional[str] = None
            cur_count = 0
            for c_row, cnt in merged:
                if cur_row is None:
                    cur_row = c_row
                    cur_count = cnt
                elif c_row == cur_row:
                    cur_count += cnt
                else:
                    yield cur_row, cur_count
                    cur_row = c_row
                    cur_count = cnt
            if cur_row is not None:
                yield cur_row, cur_count

        return manifests, _coalesced_stream()


def diff_coalesced_multiset_streams(
    source_stream: Iterable[Tuple[str, int]],
    target_stream: Iterable[Tuple[str, int]],
    canonicalizer: KeylessRowCanonicalizer,
) -> Tuple[MultisetFingerprint, MultisetFingerprint, Tuple[MultiplicityDiscrepancy, ...]]:
    """Perform a single linear merge walk over two sorted `(canonical_row, count)` streams.

    Computes exact `MultisetFingerprint` (including `multiplicity_commitment`) for both
    sides and collects every `MultiplicityDiscrepancy` in $O(1)$ working memory per cursor step.
    """
    src_iter = iter(source_stream)
    dst_iter = iter(target_stream)

    src_item = next(src_iter, None)
    dst_item = next(dst_iter, None)

    src_fp = MultisetFingerprint()
    dst_fp = MultisetFingerprint()
    src_distinct = 0
    dst_distinct = 0
    src_mult_commit = 0
    dst_mult_commit = 0
    discrepancies: List[MultiplicityDiscrepancy] = []
    salt = canonicalizer.contract.digest_salt

    def _accumulate_side(
        fp: MultisetFingerprint, c_row: str, count: int
    ) -> Tuple[MultisetFingerprint, int]:
        h1, h2 = canonicalizer.row_hashes(c_row)
        next_fp = fp.add_row_hashes(h1, h2, count)
        m_hash = multiplicity_entry_hash(salt, c_row, count)
        return next_fp, m_hash

    while src_item is not None or dst_item is not None:
        if src_item is not None and (dst_item is None or src_item[0] < dst_item[0]):
            c_row, s_cnt = src_item
            src_fp, m_hash = _accumulate_side(src_fp, c_row, s_cnt)
            src_distinct += 1
            src_mult_commit += m_hash
            discrepancies.append(
                MultiplicityDiscrepancy(
                    canonical_row=c_row,
                    decoded_values=tuple(rowtext.parse(c_row)),
                    source_count=s_cnt,
                    target_count=0,
                )
            )
            src_item = next(src_iter, None)
        elif dst_item is not None and (src_item is None or dst_item[0] < src_item[0]):
            c_row, t_cnt = dst_item
            dst_fp, m_hash = _accumulate_side(dst_fp, c_row, t_cnt)
            dst_distinct += 1
            dst_mult_commit += m_hash
            discrepancies.append(
                MultiplicityDiscrepancy(
                    canonical_row=c_row,
                    decoded_values=tuple(rowtext.parse(c_row)),
                    source_count=0,
                    target_count=t_cnt,
                )
            )
            dst_item = next(dst_iter, None)
        else:
            assert src_item is not None and dst_item is not None
            c_row, s_cnt = src_item
            _, t_cnt = dst_item
            src_fp, s_mhash = _accumulate_side(src_fp, c_row, s_cnt)
            dst_fp, t_mhash = _accumulate_side(dst_fp, c_row, t_cnt)
            src_distinct += 1
            dst_distinct += 1
            src_mult_commit += s_mhash
            dst_mult_commit += t_mhash
            if s_cnt != t_cnt:
                discrepancies.append(
                    MultiplicityDiscrepancy(
                        canonical_row=c_row,
                        decoded_values=tuple(rowtext.parse(c_row)),
                        source_count=s_cnt,
                        target_count=t_cnt,
                    )
                )
            src_item = next(src_iter, None)
            dst_item = next(dst_iter, None)

    final_src_fp = dataclasses.replace(
        src_fp,
        distinct_row_count=src_distinct,
        multiplicity_commitment=src_mult_commit,
    )
    final_dst_fp = dataclasses.replace(
        dst_fp,
        distinct_row_count=dst_distinct,
        multiplicity_commitment=dst_mult_commit,
    )
    return final_src_fp, final_dst_fp, tuple(discrepancies)


# -----------------------------------------------------------------------------
# Keyless Multiset Verifier (Bucket Escalation, External Spill, In-Memory)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class KeylessProofReport:
    """Formal verification verdict for a keyless multiset comparison."""

    table_name: str
    proof_mode: MultisetProofMode
    decision: decide.Choice
    verified_equal: bool
    care_badge: CareBadge
    source_fingerprint: MultisetFingerprint
    target_fingerprint: MultisetFingerprint
    buckets_scanned: int
    buckets_escalated: Tuple[int, ...]
    spill_runs_written: int
    spill_bytes_written: int
    discrepancies: Tuple[MultiplicityDiscrepancy, ...]
    summary: str

    @property
    def missing_from_target_count(self) -> int:
        """Total row instances present on source but missing on target."""
        return sum(max(0, d.source_count - d.target_count) for d in self.discrepancies)

    @property
    def extra_on_target_count(self) -> int:
        """Total row instances present on target beyond source multiplicity."""
        return sum(max(0, d.target_count - d.source_count) for d in self.discrepancies)


class KeylessMultisetVerifier:
    """Proves two keyless multisets equal or pinpoints exact `(row, multiplicity)`
    discrepancies using the strategy chosen by `choose_multiset_proof_strategy`.

    Rejects XOR-only digests, sample-only checks, and synthetic-key source mutations.
    """

    def __init__(
        self,
        contract: KeylessTableContract,
        *,
        endpoint_host: str = "127.0.0.1",
    ) -> None:
        self.endpoint_host = validate_safe_endpoint(endpoint_host)
        self.contract = contract
        self.canonicalizer = KeylessRowCanonicalizer(contract)

    def compute_bucket_fingerprints(
        self,
        rows: Iterable[Union[Sequence[Any], Mapping[str, Any]]],
        bucket_bits: int = 4,
    ) -> Dict[int, MultisetFingerprint]:
        """Scan `rows` once and accumulate `MultisetFingerprint` per hash bucket."""
        buckets: Dict[int, MultisetFingerprint] = {}
        for row in rows:
            c_text = self.canonicalizer.canonical_row_text(row)
            h1, h2 = self.canonicalizer.row_hashes(c_text)
            b = self.canonicalizer.bucket_for_hashes(h1, bucket_bits)
            prev = buckets.get(b, MultisetFingerprint())
            buckets[b] = prev.add_row_hashes(h1, h2, 1)
        return buckets

    def verify(
        self,
        source_rows: Iterable[Union[Sequence[Any], Mapping[str, Any]]],
        target_rows: Iterable[Union[Sequence[Any], Mapping[str, Any]]],
        facts: Union[Mapping[str, Any], decide.Facts],
        *,
        bucket_bits: int = 4,
        spill_dir: Optional[Union[str, pathlib.Path]] = None,
    ) -> KeylessProofReport:
        """Climb `multiset_proof_rungs` and execute the chosen keyless multiset proof."""
        choice = choose_multiset_proof_strategy(facts)
        if choice.rung is None:
            raise KeylessProofError(
                f"no keyless multiset proof rung held for '{self.contract.table_name}': {choice.reason}"
            )

        mode = MultisetProofMode(choice.rung.name)
        derived = derive_scale_facts(facts)

        if mode == MultisetProofMode.IN_MEMORY_CANONICAL_MULTISET:
            return self._verify_in_memory(source_rows, target_rows, choice, derived)
        if mode == MultisetProofMode.SQL_BUCKET_MULTISET_DIGEST:
            return self._verify_bucket_escalated(
                source_rows, target_rows, choice, derived, bucket_bits=bucket_bits
            )
        return self._verify_external_spill(
            source_rows, target_rows, choice, derived, spill_dir=spill_dir
        )

    def _verify_in_memory(
        self,
        source_rows: Iterable[Union[Sequence[Any], Mapping[str, Any]]],
        target_rows: Iterable[Union[Sequence[Any], Mapping[str, Any]]],
        choice: decide.Choice,
        derived: Mapping[str, Any],
    ) -> KeylessProofReport:
        mem_limit = int(derived.get("memory_row_limit") or 0)
        src_counts: Dict[str, int] = collections.Counter()
        dst_counts: Dict[str, int] = collections.Counter()

        src_total = 0
        for r in source_rows:
            src_total += 1
            if mem_limit > 0 and src_total > mem_limit:
                raise KeylessProofError(
                    f"source row count ({src_total}) exceeded measured memory_row_limit ({mem_limit})"
                )
            src_counts[self.canonicalizer.canonical_row_text(r)] += 1

        dst_total = 0
        for r in target_rows:
            dst_total += 1
            if mem_limit > 0 and dst_total > mem_limit:
                raise KeylessProofError(
                    f"target row count ({dst_total}) exceeded measured memory_row_limit ({mem_limit})"
                )
            dst_counts[self.canonicalizer.canonical_row_text(r)] += 1

        src_fp, dst_fp, discrepancies = diff_coalesced_multiset_streams(
            sorted(src_counts.items(), key=lambda kv: kv[0]),
            sorted(dst_counts.items(), key=lambda kv: kv[0]),
            self.canonicalizer,
        )
        equal = src_fp.matches(dst_fp) and len(discrepancies) == 0
        badge = CareBadge.PASS if equal else CareBadge.ACTION
        summary = (
            f"{badge.value}: keyless multiset '{self.contract.table_name}' proved equal in memory "
            f"(rows={src_fp.row_count}, distinct={src_fp.distinct_row_count})"
            if equal
            else f"{badge.value}: keyless multiset '{self.contract.table_name}' differs in "
            f"{len(discrepancies)} canonical row groups (src_rows={src_fp.row_count}, dst_rows={dst_fp.row_count})"
        )
        return KeylessProofReport(
            table_name=self.contract.table_name,
            proof_mode=MultisetProofMode.IN_MEMORY_CANONICAL_MULTISET,
            decision=choice,
            verified_equal=equal,
            care_badge=badge,
            source_fingerprint=src_fp,
            target_fingerprint=dst_fp,
            buckets_scanned=1,
            buckets_escalated=(0,) if not equal else (),
            spill_runs_written=0,
            spill_bytes_written=0,
            discrepancies=discrepancies,
            summary=summary,
        )

    def _verify_bucket_escalated(
        self,
        source_rows: Iterable[Union[Sequence[Any], Mapping[str, Any]]],
        target_rows: Iterable[Union[Sequence[Any], Mapping[str, Any]]],
        choice: decide.Choice,
        derived: Mapping[str, Any],
        *,
        bucket_bits: int,
    ) -> KeylessProofReport:
        src_list = list(source_rows)
        dst_list = list(target_rows)
        bucket_limit = int(derived.get("memory_row_limit") or 0)

        src_buckets: Dict[int, MultisetFingerprint] = {}
        dst_buckets: Dict[int, MultisetFingerprint] = {}
        src_by_bucket: Dict[int, Counter[str]] = collections.defaultdict(collections.Counter)
        dst_by_bucket: Dict[int, Counter[str]] = collections.defaultdict(collections.Counter)
        src_bucket_rows: Dict[int, int] = collections.Counter()
        dst_bucket_rows: Dict[int, int] = collections.Counter()

        for r in src_list:
            c_text = self.canonicalizer.canonical_row_text(r)
            h1, h2 = self.canonicalizer.row_hashes(c_text)
            b = self.canonicalizer.bucket_for_hashes(h1, bucket_bits)
            src_bucket_rows[b] += 1
            if bucket_limit > 0 and src_bucket_rows[b] > bucket_limit:
                raise KeylessProofError(
                    f"source bucket {b} rows ({src_bucket_rows[b]}) exceeded bucket memory limit ({bucket_limit})"
                )
            src_buckets[b] = src_buckets.get(b, MultisetFingerprint()).add_row_hashes(h1, h2, 1)
            src_by_bucket[b][c_text] += 1

        for r in dst_list:
            c_text = self.canonicalizer.canonical_row_text(r)
            h1, h2 = self.canonicalizer.row_hashes(c_text)
            b = self.canonicalizer.bucket_for_hashes(h1, bucket_bits)
            dst_bucket_rows[b] += 1
            if bucket_limit > 0 and dst_bucket_rows[b] > bucket_limit:
                raise KeylessProofError(
                    f"target bucket {b} rows ({dst_bucket_rows[b]}) exceeded bucket memory limit ({bucket_limit})"
                )
            dst_buckets[b] = dst_buckets.get(b, MultisetFingerprint()).add_row_hashes(h1, h2, 1)
            dst_by_bucket[b][c_text] += 1

        all_buckets = sorted(set(src_buckets.keys()) | set(dst_buckets.keys()))
        zero_fp = MultisetFingerprint()

        # Pass 1: Compare bucket homomorphic sum digests; escalate differing buckets
        # AND verify exact multiplicity commitment per bucket so no hash collision passes
        escalated: List[int] = []
        discrepancies: List[MultiplicityDiscrepancy] = []
        src_total_fp = MultisetFingerprint()
        dst_total_fp = MultisetFingerprint()
        src_distinct = 0
        dst_distinct = 0
        src_mult_commit = 0
        dst_mult_commit = 0

        for b in all_buckets:
            s_bfp = src_buckets.get(b, zero_fp)
            t_bfp = dst_buckets.get(b, zero_fp)
            b_src_fp, b_dst_fp, b_diffs = diff_coalesced_multiset_streams(
                sorted(src_by_bucket[b].items(), key=lambda kv: kv[0]),
                sorted(dst_by_bucket[b].items(), key=lambda kv: kv[0]),
                self.canonicalizer,
            )
            if not s_bfp.matches(t_bfp) or b_diffs:
                escalated.append(b)
                discrepancies.extend(b_diffs)
            src_total_fp = src_total_fp.combine(b_src_fp)
            dst_total_fp = dst_total_fp.combine(b_dst_fp)
            src_distinct += b_src_fp.distinct_row_count or 0
            dst_distinct += b_dst_fp.distinct_row_count or 0
            src_mult_commit += b_src_fp.multiplicity_commitment or 0
            dst_mult_commit += b_dst_fp.multiplicity_commitment or 0

        src_total_fp = dataclasses.replace(
            src_total_fp,
            distinct_row_count=src_distinct,
            multiplicity_commitment=src_mult_commit,
        )
        dst_total_fp = dataclasses.replace(
            dst_total_fp,
            distinct_row_count=dst_distinct,
            multiplicity_commitment=dst_mult_commit,
        )
        equal = src_total_fp.matches(dst_total_fp) and len(discrepancies) == 0
        badge = CareBadge.PASS if equal else CareBadge.ACTION
        summary = (
            f"{badge.value}: bucketed multiset digest proved '{self.contract.table_name}' equal "
            f"across {len(all_buckets)} active buckets (rows={src_total_fp.row_count})"
            if equal
            else f"{badge.value}: bucketed multiset digest escalated {len(escalated)} differing buckets "
            f"with {len(discrepancies)} row multiplicity discrepancies"
        )
        return KeylessProofReport(
            table_name=self.contract.table_name,
            proof_mode=MultisetProofMode.SQL_BUCKET_MULTISET_DIGEST,
            decision=choice,
            verified_equal=equal,
            care_badge=badge,
            source_fingerprint=src_total_fp,
            target_fingerprint=dst_total_fp,
            buckets_scanned=max(1, len(all_buckets)),
            buckets_escalated=tuple(escalated),
            spill_runs_written=0,
            spill_bytes_written=0,
            discrepancies=tuple(discrepancies),
            summary=summary,
        )

    def _verify_external_spill(
        self,
        source_rows: Iterable[Union[Sequence[Any], Mapping[str, Any]]],
        target_rows: Iterable[Union[Sequence[Any], Mapping[str, Any]]],
        choice: decide.Choice,
        derived: Mapping[str, Any],
        *,
        spill_dir: Optional[Union[str, pathlib.Path]] = None,
    ) -> KeylessProofReport:
        mem_limit = max(1, int(derived.get("memory_row_limit") or 2))
        avail_disk = derived.get("spill_free_bytes")
        half_disk = int(avail_disk) // 2 if avail_disk is not None else None

        with tempfile.TemporaryDirectory(
            prefix="migkit_keyless_spill_",
            dir=str(spill_dir) if spill_dir is not None else None,
        ) as tmp_root:
            root_path = pathlib.Path(tmp_root)
            src_sorter = ExternalMultisetSpillSorter(
                self.canonicalizer,
                run_distinct_limit=mem_limit,
                spill_dir=root_path / "src",
                avail_disk_bytes=half_disk,
            )
            dst_sorter = ExternalMultisetSpillSorter(
                self.canonicalizer,
                run_distinct_limit=mem_limit,
                spill_dir=root_path / "dst",
                avail_disk_bytes=half_disk,
            )

            src_manifests, src_stream = src_sorter.spill_and_coalesce(
                source_rows, root_path / "src"
            )
            dst_manifests, dst_stream = dst_sorter.spill_and_coalesce(
                target_rows, root_path / "dst"
            )

            src_fp, dst_fp, discrepancies = diff_coalesced_multiset_streams(
                src_stream, dst_stream, self.canonicalizer
            )
            total_runs = len(src_manifests) + len(dst_manifests)
            total_bytes = src_sorter.bytes_spilled + dst_sorter.bytes_spilled

        equal = src_fp.matches(dst_fp) and len(discrepancies) == 0
        badge = CareBadge.PASS if equal else CareBadge.ACTION
        summary = (
            f"{badge.value}: external merge-sort spill proved '{self.contract.table_name}' equal "
            f"(rows={src_fp.row_count}, runs={total_runs}, spill_bytes={total_bytes})"
            if equal
            else f"{badge.value}: external merge-sort spill found {len(discrepancies)} multiplicity "
            f"discrepancies across {total_runs} sorted runs"
        )
        return KeylessProofReport(
            table_name=self.contract.table_name,
            proof_mode=MultisetProofMode.EXTERNAL_SORT_SPILL_MULTISET,
            decision=choice,
            verified_equal=equal,
            care_badge=badge,
            source_fingerprint=src_fp,
            target_fingerprint=dst_fp,
            buckets_scanned=1,
            buckets_escalated=(0,) if not equal else (),
            spill_runs_written=total_runs,
            spill_bytes_written=total_bytes,
            discrepancies=discrepancies,
            summary=summary,
        )


# -----------------------------------------------------------------------------
# Deterministic Keyless CDC & Repair Row-Identity Applier
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class KeylessChangeEvent:
    """One keyless CDC or repair mutation event with explicit multiplicity or locator."""

    op: KeylessChangeOp
    before_row: Optional[Union[Sequence[Any], Mapping[str, Any]]] = None
    after_row: Optional[Union[Sequence[Any], Mapping[str, Any]]] = None
    affected_multiplicity: int = 1
    physical_locator: Optional[str] = None
    fence_epoch: Optional[int] = None

    def __post_init__(self) -> None:
        if self.affected_multiplicity <= 0:
            raise KeylessIdentityError(
                f"affected_multiplicity must be >= 1, got {self.affected_multiplicity}"
            )
        if self.op == KeylessChangeOp.INSERT and self.after_row is None:
            raise KeylessIdentityError("INSERT change event requires after_row")
        if self.op == KeylessChangeOp.DELETE and self.before_row is None:
            raise KeylessIdentityError("DELETE change event requires before_row")
        if self.op == KeylessChangeOp.UPDATE and (
            self.before_row is None or self.after_row is None
        ):
            raise KeylessIdentityError(
                "UPDATE change event on keyless table requires both before_row and after_row"
            )


@dataclass(frozen=True)
class KeylessApplyReceipt:
    """Result of applying a keyless change batch under a chosen row-identity strategy."""

    identity_mode: RowIdentityMode
    decision: decide.Choice
    events_applied: int
    rows_inserted: int
    rows_deleted: int
    rows_updated: int
    resulting_row_count: int
    care_badge: CareBadge
    summary: str


class KeylessTargetTable:
    """Keyless target table storing physical tuple slots `(locator_id, canonical_row, raw_values)`
    with epoch fencing to model keyless CDC and bucket reconciliation without synthetic keys."""

    def __init__(
        self,
        contract: KeylessTableContract,
        initial_rows: Iterable[Union[Sequence[Any], Mapping[str, Any]]] = (),
        *,
        fence_epoch: int = 1,
    ) -> None:
        self.contract = contract
        self.canonicalizer = KeylessRowCanonicalizer(contract)
        self.fence_epoch = int(fence_epoch)
        self._next_slot_id = 1
        self._slots: Dict[str, Tuple[str, Tuple[Any, ...]]] = {}
        for r in initial_rows:
            self.insert_row(r)

    def insert_row(self, row: Union[Sequence[Any], Mapping[str, Any]]) -> str:
        """Insert one row and return its physical slot locator."""
        vals = self.canonicalizer.extract_ordered_values(row)
        c_text = self.canonicalizer.canonical_row_text(vals)
        locator = f"ctid:{self.fence_epoch}:{self._next_slot_id}"
        self._next_slot_id += 1
        self._slots[locator] = (c_text, vals)
        return locator

    def compact_or_move_rows(self) -> int:
        """Simulate VACUUM FULL / table compaction that advances fence_epoch and reassigns locators."""
        self.fence_epoch += 1
        new_slots: Dict[str, Tuple[str, Tuple[Any, ...]]] = {}
        for c_text, vals in self._slots.values():
            loc = f"ctid:{self.fence_epoch}:{self._next_slot_id}"
            self._next_slot_id += 1
            new_slots[loc] = (c_text, vals)
        self._slots = new_slots
        return self.fence_epoch

    def rows(self) -> Tuple[Tuple[Any, ...], ...]:
        """Return all live rows in current physical storage order."""
        return tuple(vals for _, vals in self._slots.values())

    def canonical_multiset(self) -> Dict[str, int]:
        """Return `{canonical_row: multiplicity}` of all live rows."""
        counts: Dict[str, int] = collections.Counter()
        for c_text, _ in self._slots.values():
            counts[c_text] += 1
        return dict(counts)


class KeylessDeterministicApplier:
    """Applies keyless CDC mutations (`INSERT`, `DELETE`, `UPDATE`) or bucket
    reconciliation using the strategy selected by `choose_row_identity_strategy`."""

    def __init__(self, target: KeylessTargetTable) -> None:
        self.target = target
        self.canonicalizer = target.canonicalizer

    def apply_changes(
        self,
        events: Sequence[KeylessChangeEvent],
        facts: Union[Mapping[str, Any], decide.Facts],
        *,
        source_bucket_rows: Optional[Dict[int, Sequence[Union[Sequence[Any], Mapping[str, Any]]]]] = None,
        bucket_bits: int = 4,
    ) -> KeylessApplyReceipt:
        """Climb `row_identity_rungs` and apply `events` to `self.target`."""
        choice = choose_row_identity_strategy(facts)
        if choice.rung is None:
            raise KeylessIdentityError(
                f"no keyless row-identity strategy held for '{self.target.contract.table_name}': {choice.reason}"
            )

        mode = RowIdentityMode(choice.rung.name)
        if mode == RowIdentityMode.CANONICAL_FULL_ROW_MULTIPLICITY:
            return self._apply_by_full_row_multiplicity(events, choice)
        if mode == RowIdentityMode.FENCED_PHYSICAL_LOCATOR_VERIFIED:
            return self._apply_by_fenced_locator(events, choice)
        return self._apply_by_bucket_reconcile(
            events,
            choice,
            source_bucket_rows=source_bucket_rows,
            bucket_bits=bucket_bits,
        )

    def _find_matching_locators(self, canonical_target: str, limit: int) -> List[str]:
        matched: List[str] = []
        for loc, (c_text, _) in self.target._slots.items():
            if c_text == canonical_target:
                matched.append(loc)
                if len(matched) == limit:
                    break
        return matched

    def _apply_by_full_row_multiplicity(
        self,
        events: Sequence[KeylessChangeEvent],
        choice: decide.Choice,
    ) -> KeylessApplyReceipt:
        inserted = 0
        deleted = 0
        updated = 0

        for ev in events:
            m = ev.affected_multiplicity
            if ev.op == KeylessChangeOp.INSERT:
                assert ev.after_row is not None
                for _ in range(m):
                    self.target.insert_row(ev.after_row)
                    inserted += 1
            elif ev.op == KeylessChangeOp.DELETE:
                assert ev.before_row is not None
                c_before = self.canonicalizer.canonical_row_text(ev.before_row)
                locs = self._find_matching_locators(c_before, m)
                if len(locs) < m:
                    raise KeylessIdentityError(
                        f"keyless DELETE expected multiplicity {m} of row {c_before!r}, "
                        f"found only {len(locs)} on target"
                    )
                for loc in locs:
                    del self.target._slots[loc]
                    deleted += 1
            elif ev.op == KeylessChangeOp.UPDATE:
                assert ev.before_row is not None and ev.after_row is not None
                c_before = self.canonicalizer.canonical_row_text(ev.before_row)
                locs = self._find_matching_locators(c_before, m)
                if len(locs) < m:
                    raise KeylessIdentityError(
                        f"keyless UPDATE expected multiplicity {m} of before-image {c_before!r}, "
                        f"found only {len(locs)} on target"
                    )
                after_vals = self.canonicalizer.extract_ordered_values(ev.after_row)
                c_after = self.canonicalizer.canonical_row_text(after_vals)
                for loc in locs:
                    self.target._slots[loc] = (c_after, after_vals)
                    updated += 1

        total = len(self.target._slots)
        return KeylessApplyReceipt(
            identity_mode=RowIdentityMode.CANONICAL_FULL_ROW_MULTIPLICITY,
            decision=choice,
            events_applied=len(events),
            rows_inserted=inserted,
            rows_deleted=deleted,
            rows_updated=updated,
            resulting_row_count=total,
            care_badge=CareBadge.PASS,
            summary=(
                f"{CARE_PASS}: applied {len(events)} keyless events by canonical full-row "
                f"multiplicity (ins={inserted}, del={deleted}, upd={updated}, total={total})"
            ),
        )

    def _apply_by_fenced_locator(
        self,
        events: Sequence[KeylessChangeEvent],
        choice: decide.Choice,
    ) -> KeylessApplyReceipt:
        inserted = 0
        deleted = 0
        updated = 0

        for ev in events:
            if ev.op == KeylessChangeOp.INSERT:
                assert ev.after_row is not None
                for _ in range(ev.affected_multiplicity):
                    self.target.insert_row(ev.after_row)
                    inserted += 1
                continue

            if ev.fence_epoch != self.target.fence_epoch:
                raise KeylessLocatorFenceError(
                    f"locator fence epoch mismatch: event epoch={ev.fence_epoch}, "
                    f"target epoch={self.target.fence_epoch}"
                )
            if not ev.physical_locator or ev.physical_locator not in self.target._slots:
                raise KeylessLocatorFenceError(
                    f"physical locator {ev.physical_locator!r} not found on target"
                )
            assert ev.before_row is not None
            expected_c = self.canonicalizer.canonical_row_text(ev.before_row)
            actual_c, _ = self.target._slots[ev.physical_locator]
            if actual_c != expected_c:
                raise KeylessLocatorFenceError(
                    f"canonical row hash mismatch at locator {ev.physical_locator!r}: "
                    f"expected {expected_c!r}, found {actual_c!r}"
                )

            if ev.op == KeylessChangeOp.DELETE:
                del self.target._slots[ev.physical_locator]
                deleted += 1
            elif ev.op == KeylessChangeOp.UPDATE:
                assert ev.after_row is not None
                after_vals = self.canonicalizer.extract_ordered_values(ev.after_row)
                c_after = self.canonicalizer.canonical_row_text(after_vals)
                self.target._slots[ev.physical_locator] = (c_after, after_vals)
                updated += 1

        total = len(self.target._slots)
        return KeylessApplyReceipt(
            identity_mode=RowIdentityMode.FENCED_PHYSICAL_LOCATOR_VERIFIED,
            decision=choice,
            events_applied=len(events),
            rows_inserted=inserted,
            rows_deleted=deleted,
            rows_updated=updated,
            resulting_row_count=total,
            care_badge=CareBadge.PASS,
            summary=(
                f"{CARE_PASS}: applied {len(events)} keyless events by fenced physical "
                f"locator (ins={inserted}, del={deleted}, upd={updated}, total={total})"
            ),
        )

    def _apply_by_bucket_reconcile(
        self,
        events: Sequence[KeylessChangeEvent],
        choice: decide.Choice,
        *,
        source_bucket_rows: Optional[Dict[int, Sequence[Union[Sequence[Any], Mapping[str, Any]]]]],
        bucket_bits: int,
    ) -> KeylessApplyReceipt:
        if source_bucket_rows is None:
            raise KeylessIdentityError(
                "bucket_multiset_reconcile requires source_bucket_rows for affected hash buckets"
            )
        inserted = 0
        deleted = 0

        for bucket_id, replacement_rows in sorted(source_bucket_rows.items()):
            to_delete: List[str] = []
            for loc, (c_text, _) in self.target._slots.items():
                h1, _ = self.canonicalizer.row_hashes(c_text)
                if self.canonicalizer.bucket_for_hashes(h1, bucket_bits) == bucket_id:
                    to_delete.append(loc)
            for loc in to_delete:
                del self.target._slots[loc]
                deleted += 1
            for row in replacement_rows:
                self.target.insert_row(row)
                inserted += 1

        total = len(self.target._slots)
        return KeylessApplyReceipt(
            identity_mode=RowIdentityMode.BUCKET_MULTISET_RECONCILE,
            decision=choice,
            events_applied=len(events) or len(source_bucket_rows),
            rows_inserted=inserted,
            rows_deleted=deleted,
            rows_updated=0,
            resulting_row_count=total,
            care_badge=CareBadge.PASS,
            summary=(
                f"{CARE_PASS}: reconciled {len(source_bucket_rows)} keyless hash buckets "
                f"(ins={inserted}, del={deleted}, total={total})"
            ),
        )
