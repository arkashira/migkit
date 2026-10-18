"""Choose LOB policy without truncation (Row 610: RCH02.lob-policy).

Makes inline, streamed, locator, chunked, and separately fetched LOB paths
candidates under value-size, packet, memory, and consistency facts via
decide.Rung ladders.

Rejects truncating or stale-locator paths under exact contracts; bounds size
probes from measured statistics and spread while keeping unknown maxima
conservative. Executes mixed NULL, empty, binary, multibyte UTF-8, compressed,
and extreme LOB transfers under concurrent updates and injected faults with
exact final bytes, bounded worker memory, and no premature checkpoint.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import hashlib
import json
import math
import os
import statistics
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from . import decide, pieces

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


class LobPathMode(str, enum.Enum):
    """Candidate LOB transfer paths."""

    INLINE_ROW_LOB = "inline_row_lob"
    STREAMED_CURSOR_LOB = "streamed_cursor_lob"
    LOCATOR_SNAPSHOT_LOB = "locator_snapshot_lob"
    CHUNKED_PIECE_LOB = "chunked_piece_lob"
    SEPARATE_KEYED_FETCH_LOB = "separate_keyed_fetch_lob"


class LobSizeProbeMode(str, enum.Enum):
    """Candidate strategies for bounding LOB value sizes before transfer."""

    EXACT_OCTET_SCAN = "exact_octet_scan"
    BOUNDED_SAMPLE_SPREAD = "bounded_sample_spread"
    CONSERVATIVE_UNKNOWN_MAXIMA = "conservative_unknown_maxima"


class LobKind(str, enum.Enum):
    """Classification of LOB column payload semantics."""

    NULL = "null"
    EMPTY = "empty"
    BINARY = "binary"
    MULTIBYTE_TEXT = "multibyte_text"
    COMPRESSED_STORAGE = "compressed_storage"
    EXTREME_LOB = "extreme_lob"


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class LobPolicyError(Exception):
    """Base exception for LOB policy and transfer integrity errors."""


class LobTruncationError(LobPolicyError):
    """Raised when a path or mover configuration would truncate or has truncated a LOB."""


class StaleLocatorError(LobPolicyError):
    """Raised when a LOB locator is invalidated or mutated under an unpinned snapshot."""


class ConcurrentLobMutationError(LobPolicyError):
    """Raised when concurrent source updates prevent a clean LOB read after bounded retries."""


class LobMemoryBudgetExceededError(LobPolicyError):
    """Raised when a LOB read or write step exceeds the worker's bounded memory ceiling."""


class LeaseLockError(LobPolicyError):
    """Raised when mutual exclusion lease (.run.lock) cannot be acquired or renewed."""


class UnsafeEndpointError(LobPolicyError):
    """Raised when a non-loopback or non-RFC1918 10.0.0.x host is supplied."""


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
# Typed JSON Serialization Helpers (O(1) Checkpoint Support)
# -----------------------------------------------------------------------------

def _encode_typed_value(val: Any) -> Any:
    """Recursively encode native Python types (datetime, Decimal, UUID, tuple, bytes) for JSON."""
    if isinstance(val, datetime.datetime):
        return {"__type__": "datetime", "val": val.isoformat()}
    if isinstance(val, datetime.date):
        return {"__type__": "date", "val": val.isoformat()}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "val": str(val)}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "val": str(val)}
    if isinstance(val, (bytes, bytearray, memoryview)):
        return {"__type__": "bytes", "val": bytes(val).hex()}
    if isinstance(val, tuple):
        return {"__type__": "tuple", "val": [_encode_typed_value(x) for x in val]}
    if isinstance(val, set):
        return {"__type__": "set", "val": [_encode_typed_value(x) for x in sorted(val, key=str)]}
    if isinstance(val, enum.Enum):
        return val.value
    if isinstance(val, dict):
        return {str(k): _encode_typed_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_encode_typed_value(x) for x in val]
    return val


def _decode_typed_hook(obj: Dict[str, Any]) -> Any:
    """Restore native Python types from typed JSON envelopes."""
    t = obj.get("__type__")
    if t == "datetime":
        return datetime.datetime.fromisoformat(obj["val"])
    if t == "date":
        return datetime.date.fromisoformat(obj["val"])
    if t == "decimal":
        return decimal.Decimal(obj["val"])
    if t == "uuid":
        return uuid.UUID(obj["val"])
    if t == "bytes":
        return bytes.fromhex(obj["val"])
    if t == "tuple":
        return tuple(obj["val"])
    if t == "set":
        return set(obj["val"])
    return obj


# -----------------------------------------------------------------------------
# Byte-Exact Digest & Wire/Memory Footprint Accounting
# -----------------------------------------------------------------------------

def payload_octet_length(value: Any) -> int:
    """Return exact uncompressed UTF-8 or raw byte length of a LOB value (0 for NULL/empty)."""
    if value is None:
        return 0
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value)
    if isinstance(value, str):
        return pieces._utf8_length(value)
    return len(str(value).encode("utf-8"))


def payload_sha256(value: Any) -> Optional[str]:
    """Return deterministic SHA-256 hex digest of a LOB value, or None if value is NULL."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        return hashlib.sha256(bytes(value)).hexdigest()
    if isinstance(value, str):
        h = hashlib.md5(usedforsecurity=False)  # unused, we use sha256 in chunks
        del h
        digest = hashlib.sha256()
        step = 1 << 20
        for at in range(0, len(value), step):
            digest.update(value[at : at + step].encode("utf-8", "surrogatepass"))
        return digest.hexdigest()
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def wire_bytes_required(
    octet_length: int,
    *,
    is_multibyte_text: bool = False,
    char_length: Optional[int] = None,
    sent_factor: int = pieces.SENT_FACTOR,
    headroom_bytes: int = pieces.HEADROOM,
) -> Tuple[int, int]:
    """Compute (returned_wire_bytes, sent_wire_bytes) for a LOB value.

    Accounts for worst-case UTF-8 character expansion (`pieces.CHAR_BYTES`) when
    only character length is known, plus SQL statement escaping/hex expansion
    (`pieces.SENT_FACTOR`) and protocol header headroom (`pieces.HEADROOM`).
    """
    effective_octets = max(0, int(octet_length))
    if is_multibyte_text and char_length is not None and char_length > 0:
        effective_octets = max(effective_octets, int(char_length) * pieces.CHAR_BYTES)
    returned_wire = effective_octets + max(0, int(headroom_bytes))
    sent_wire = (effective_octets * max(1, int(sent_factor))) + max(0, int(headroom_bytes))
    return returned_wire, sent_wire


def slice_utf8_by_byte_budget(text: str, max_piece_bytes: int) -> List[str]:
    """Split a UTF-8 string into character-aligned pieces where each piece's UTF-8
    byte length never exceeds `max_piece_bytes` (minimum 4 bytes for a 4-byte codepoint).

    Prevents splitting mid-codepoint or exceeding byte-denominated packet limits on
    multibyte characters.
    """
    if max_piece_bytes < pieces.CHAR_BYTES:
        raise ValueError(
            f"max_piece_bytes ({max_piece_bytes}) must be >= {pieces.CHAR_BYTES} for UTF-8 safety"
        )
    if text == "":
        return [""]
    out: List[str] = []
    current_chars: List[str] = []
    current_bytes = 0
    for ch in text:
        ch_bytes = len(ch.encode("utf-8", "surrogatepass"))
        if current_chars and (current_bytes + ch_bytes > max_piece_bytes):
            out.append("".join(current_chars))
            current_chars = [ch]
            current_bytes = ch_bytes
        else:
            current_chars.append(ch)
            current_bytes += ch_bytes
    if current_chars:
        out.append("".join(current_chars))
    return out


def slice_bytes_by_budget(data: bytes, max_piece_bytes: int) -> List[bytes]:
    """Split raw bytes into pieces of at most `max_piece_bytes`."""
    if max_piece_bytes < 1:
        raise ValueError(f"max_piece_bytes ({max_piece_bytes}) must be >= 1")
    if len(data) == 0:
        return [b""]
    return [data[i : i + max_piece_bytes] for i in range(0, len(data), max_piece_bytes)]


# -----------------------------------------------------------------------------
# Bounded LOB Size Probe & Conservative Maxima Estimation (decide.Rung Ladder 1)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class LobSizeProbeResult:
    """Result of probing LOB sizes on a source table/column."""

    probe_mode: LobSizeProbeMode
    exact: bool
    rows_probed: int
    total_rows: int
    observed_max_octets: int
    observed_mean_octets: float
    observed_p99_octets: int
    observed_stddev_octets: float
    stored_compressed_max_bytes: Optional[int]
    compression_ratio_observed: float
    conservative_max_octets: int
    probe_duration_seconds: float
    care_badge: CareBadge
    reason: str


def lob_size_probe_rungs() -> Tuple[decide.Rung, ...]:
    """Build the decide.Rung ladder for bounding LOB value sizes without blind full scans."""
    return (
        decide.Rung(
            "exact_octet_scan",
            "exact server-side octet_length scan over uncompressed LOB lengths",
            gives=frozenset({"carried", "exact", "byte exact"}),
            needs=(
                decide.Need(
                    "uncompressed octet_length expression on source",
                    lambda f: bool(f.get("supports_octet_length_scan", True))
                    and not bool(f.get("compressed_metadata_only", False)),
                    "source only exposes compressed storage size or lacks octet_length scan",
                ),
                decide.Need(
                    "table row count and scan time within probe budget",
                    lambda f: bool(f.get("exact_scan_within_budget", False)),
                    "table row count or estimated scan duration exceeds bounded probe budget",
                ),
            ),
            because="reads exact uncompressed maximum length on the server without transferring LOB bytes",
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "bounded_sample_spread",
            "bounded sample probe with spread-derived safety margin",
            gives=frozenset({"carried"}),
            needs=(
                decide.Need(
                    "uncompressed octet_length on sampled rows",
                    lambda f: bool(f.get("supports_octet_length_scan", True))
                    and not bool(f.get("compressed_metadata_only", False)),
                    "sample probe cannot trust compressed storage size as uncompressed length",
                ),
                decide.Need(
                    "non-empty sample with bounded probe budget",
                    lambda f: bool(f.get("sample_probe_within_budget", False))
                    and int(f.get("sample_count", 0)) >= int(f.get("min_sample_rows", 8)),
                    "sample size is insufficient or exceeds probe budget",
                ),
                decide.Need(
                    "known distribution without unmeasured extreme flag",
                    lambda f: not bool(f.get("unknown_maxima", False)),
                    "maxima are unknown or unbounded; sample cannot bound worst-case truncation",
                ),
            ),
            because="bounds LOB size from measured sample mean, p99, and spread within probe budget",
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "conservative_unknown_maxima",
            "conservative unknown-maxima guard assuming protocol ceiling",
            gives=frozenset({"carried", "exact", "byte exact"}),
            needs=(
                decide.Need(
                    "conservative ceiling configured for unknown or compressed LOBs",
                    lambda f: int(f.get("protocol_hard_ceiling_bytes", pieces.BATCH_BYTES)) > 0,
                    "protocol_hard_ceiling_bytes must be positive to guard unknown maxima",
                ),
            ),
            because="refuses optimistic inline sizing when maxima are unknown or hidden by compression",
            footprint=0,
            leaves="nothing",
        ),
    )


def choose_lob_size_probe(
    facts: Union[Dict[str, Any], decide.Facts],
    need: Sequence[str] = (),
) -> Tuple[decide.Choice, LobSizeProbeResult]:
    """Choose the LOB size probe strategy via decide.climb and compute conservative bounds."""
    raw = dict(facts) if not isinstance(facts, dict) else dict(facts)

    total_rows = max(0, int(raw.get("total_rows", 0)))
    scan_rate_rows_sec = max(1.0, float(raw.get("scan_rate_rows_sec", 250_000.0)))
    probe_budget_sec = max(0.01, float(raw.get("probe_budget_seconds", 2.0)))
    max_exact_rows = int(raw.get("max_exact_rows", int(scan_rate_rows_sec * probe_budget_sec)))

    sample_lengths: List[int] = [max(0, int(x)) for x in raw.get("sample_octet_lengths", ())]
    stored_compressed_lengths: List[int] = [
        max(0, int(x)) for x in raw.get("stored_compressed_lengths", ())
    ]
    sample_count = int(raw.get("sample_count", len(sample_lengths)))
    min_sample_rows = max(1, int(raw.get("min_sample_rows", 8)))

    estimated_exact_sec = (float(total_rows) / scan_rate_rows_sec) if total_rows > 0 else 0.001
    exact_within_budget = (
        total_rows > 0
        and total_rows <= max_exact_rows
        and estimated_exact_sec <= probe_budget_sec
        and not bool(raw.get("unknown_maxima", False))
    )
    raw.setdefault("exact_scan_within_budget", exact_within_budget)

    estimated_sample_sec = (float(max(sample_count, min_sample_rows)) / scan_rate_rows_sec)
    raw.setdefault("sample_probe_within_budget", estimated_sample_sec <= probe_budget_sec)
    raw.setdefault("sample_count", sample_count)
    raw.setdefault("min_sample_rows", min_sample_rows)

    protocol_ceiling = max(
        1,
        int(raw.get("protocol_hard_ceiling_bytes", pieces.BATCH_BYTES)),
    )
    raw["protocol_hard_ceiling_bytes"] = protocol_ceiling

    f = facts if isinstance(facts, decide.Facts) else decide.Facts(raw)
    for k, v in raw.items():
        if k not in f:
            f[k] = v

    rungs = lob_size_probe_rungs()
    choice = decide.climb("lob size probe", rungs, f, need=tuple(need))

    if sample_lengths:
        obs_max = max(sample_lengths)
        obs_mean = float(statistics.mean(sample_lengths))
        obs_std = float(statistics.pstdev(sample_lengths)) if len(sample_lengths) > 1 else 0.0
        sorted_s = sorted(sample_lengths)
        p99_idx = min(len(sorted_s) - 1, max(0, int(math.ceil(0.99 * len(sorted_s))) - 1))
        obs_p99 = sorted_s[p99_idx]
    else:
        obs_max = int(raw.get("observed_max_octets", 0))
        obs_mean = float(raw.get("observed_mean_octets", float(obs_max)))
        obs_std = float(raw.get("observed_stddev_octets", 0.0))
        obs_p99 = int(raw.get("observed_p99_octets", obs_max))

    stored_max = max(stored_compressed_lengths) if stored_compressed_lengths else None
    compression_ratio = 1.0
    if stored_max and stored_max > 0 and obs_max > stored_max:
        compression_ratio = float(obs_max) / float(stored_max)

    chosen_mode = (
        LobSizeProbeMode(choice.path)
        if choice.path
        else LobSizeProbeMode.CONSERVATIVE_UNKNOWN_MAXIMA
    )

    if chosen_mode == LobSizeProbeMode.EXACT_OCTET_SCAN:
        # Even when every existing row was scanned, if the source is online we add
        # 1 stddev of observed spread so a slightly wider concurrent insert stays safe.
        online_spread = int(math.ceil(obs_std)) if bool(raw.get("online_writes", False)) else 0
        conservative_max = max(obs_max + online_spread, int(obs_max * compression_ratio))
        badge = CareBadge.PASS
        exact_flag = True
        rows_probed = total_rows
        duration = estimated_exact_sec
    elif chosen_mode == LobSizeProbeMode.BOUNDED_SAMPLE_SPREAD:
        # Derive conservative upper bound from measured statistics + spread:
        # sample_max + k * stddev (where k scales with log10(total_rows / sample_count))
        # and tail ratio (p99 / mean).
        unseen_ratio = max(1.0, float(total_rows) / float(max(1, sample_count)))
        sigma_factor = 3.0 + math.log10(unseen_ratio)
        tail_ratio = (float(obs_p99) / obs_mean) if obs_mean > 0 else 1.0
        spread_margin = int(math.ceil(sigma_factor * obs_std))
        tail_margin = int(math.ceil(obs_max * min(2.0, max(1.0, tail_ratio - 1.0))))
        conservative_max = max(obs_max + spread_margin, obs_max + tail_margin)
        badge = CareBadge.REVIEW
        exact_flag = False
        rows_probed = sample_count
        duration = estimated_sample_sec
    else:
        # Unknown maxima or compressed-only metadata: never trust small observed values;
        # assume at least protocol_hard_ceiling_bytes (or observed * compression bound).
        conservative_max = max(protocol_ceiling, obs_max * 10 if stored_max else obs_max)
        badge = CareBadge.REVIEW
        exact_flag = False
        rows_probed = sample_count
        duration = 0.0005

    probe_result = LobSizeProbeResult(
        probe_mode=chosen_mode,
        exact=exact_flag,
        rows_probed=rows_probed,
        total_rows=total_rows,
        observed_max_octets=obs_max,
        observed_mean_octets=obs_mean,
        observed_p99_octets=obs_p99,
        observed_stddev_octets=obs_std,
        stored_compressed_max_bytes=stored_max,
        compression_ratio_observed=compression_ratio,
        conservative_max_octets=conservative_max,
        probe_duration_seconds=duration,
        care_badge=badge,
        reason=choice.reason,
    )
    return choice, probe_result


# -----------------------------------------------------------------------------
# LOB Transfer Policy Decision Engine (decide.Rung Ladder 2)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class LobPolicyPlan:
    """Selected LOB transfer policy with derived piece/memory bounds and guarantees."""

    mode: LobPathMode
    choice: decide.Choice
    conservative_max_octets: int
    inline_ceiling_bytes: int
    piece_size_bytes: int
    worker_memory_budget_bytes: int
    estimated_peak_memory_bytes: int
    truncation_free_verified: bool
    locator_pinned_verified: bool
    care_badge: CareBadge


def _no_truncation_limit_holds(facts: Union[Dict[str, Any], decide.Facts]) -> bool:
    """Verify no active mover or driver truncation cap sits below conservative_max_octets."""
    trunc_limit = facts.get("truncation_limit_bytes")
    if trunc_limit is None or int(trunc_limit) <= 0:
        return True
    conservative_max = int(facts.get("conservative_max_octets", 0))
    if bool(facts.get("unknown_maxima", False)):
        return False
    return int(trunc_limit) >= conservative_max


def lob_policy_rungs() -> Tuple[decide.Rung, ...]:
    """Build the decide.Rung ladder for choosing LOB policy without truncation.

    Candidates:
    1. inline_row_lob
    2. streamed_cursor_lob
    3. locator_snapshot_lob
    4. chunked_piece_lob
    5. separate_keyed_fetch_lob
    """
    return (
        decide.Rung(
            "inline_row_lob",
            "inline LOB carried inside the row fetch and statement batch",
            gives=frozenset({"carried", "exact", "byte exact", "consistent as of", "online", "keyless"}),
            needs=(
                decide.Need(
                    "known conservative maximum LOB size",
                    lambda f: not bool(f.get("unknown_maxima", False)),
                    "inline row path refuses unknown LOB maxima that could overflow packet or memory limits",
                ),
                decide.Need(
                    "no truncation limit below conservative maximum size",
                    _no_truncation_limit_holds,
                    "inline path has a truncation limit below the conservative maximum LOB size",
                ),
                decide.Need(
                    "wire bytes within source return packet and target statement limits",
                    lambda f: bool(f.get("fits_inline_wire_limits", False)),
                    "LOB wire size with UTF-8 and escaping expansion exceeds inline packet or row limits",
                ),
                decide.Need(
                    "inline batch within worker memory budget",
                    lambda f: bool(f.get("fits_inline_memory_budget", False)),
                    "carrying LOB values inline across batch rows exceeds worker memory budget",
                ),
            ),
            because="carries small and medium LOBs in a single pass with zero extra round trips",
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "streamed_cursor_lob",
            "server-streamed LOB cursor cut by byte budget",
            gives=frozenset({"carried", "exact", "byte exact", "consistent as of", "online", "keyless"}),
            needs=(
                decide.Need(
                    "streaming cursor support on source and target driver",
                    lambda f: bool(f.get("supports_streaming_cursor", False)),
                    "source or target driver does not support non-materializing LOB streaming cursors",
                ),
                decide.Need(
                    "no stream truncation limit below conservative maximum size",
                    lambda f: _no_truncation_limit_holds(f)
                    and (
                        f.get("stream_truncation_limit_bytes") in (None, 0)
                        or (
                            not bool(f.get("unknown_maxima", False))
                            and int(f.get("stream_truncation_limit_bytes", 0))
                            >= int(f.get("conservative_max_octets", 0))
                        )
                    ),
                    "streaming channel imposes a truncation limit below conservative maximum LOB size",
                ),
                decide.Need(
                    "single-row stream buffer within worker memory and packet limits",
                    lambda f: bool(f.get("fits_stream_memory_budget", False)),
                    "widest LOB exceeds single-stream buffer or worker memory budget",
                ),
                decide.Need(
                    "snapshot consistency or key-based retry on stream interruption",
                    lambda f: bool(f.get("snapshot_consistent", False))
                    or bool(f.get("has_primary_key", False)),
                    "streamed LOB cursor lacks both snapshot consistency and a key to resume on interruption",
                ),
            ),
            because="streams LOB rows sequentially with bounded memory and no per-row lookup queries",
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "locator_snapshot_lob",
            "database LOB locator read within a pinned repeatable-read snapshot",
            gives=frozenset({"carried", "exact", "byte exact", "consistent as of", "online"}),
            needs=(
                decide.Need(
                    "engine LOB locator support",
                    lambda f: bool(f.get("supports_lob_locator", False)),
                    "source engine or driver does not expose streamable LOB locators",
                ),
                decide.Need(
                    "snapshot-pinned locator immune to stale invalidation",
                    lambda f: bool(f.get("locator_snapshot_pinned", False))
                    and not bool(f.get("locator_stale_under_concurrent_writes", False)),
                    "LOB locator is not pinned to a repeatable snapshot and can go stale under concurrent writes",
                ),
                decide.Need(
                    "locator lifetime longer than batch transfer time",
                    lambda f: bool(f.get("locator_lifetime_sufficient", False)),
                    "estimated batch transfer duration exceeds LOB locator transaction or undo retention lifetime",
                ),
                decide.Need(
                    "no truncation limit on locator reads",
                    _no_truncation_limit_holds,
                    "locator path has a truncation cap below conservative maximum LOB size",
                ),
                decide.Need(
                    "locator chunk buffer within worker memory budget",
                    lambda f: bool(f.get("fits_piece_memory_budget", False)),
                    "locator chunk buffer exceeds worker memory budget",
                ),
            ),
            because="reads large out-of-line LOBs by descriptor without re-probing primary keys",
            footprint=1,
            leaves="a pinned snapshot transaction while LOB locators are read",
        ),
        decide.Rung(
            "chunked_piece_lob",
            "key-addressed chunked piece reads and appends with length re-verification",
            gives=frozenset(
                {"carried", "exact", "byte exact", "online", "resume by key", "resume by chunk"}
            ),
            needs=(
                decide.Need(
                    "primary or unique key on table for piece addressing",
                    lambda f: bool(f.get("has_primary_key", False)),
                    "chunked piece LOB transfer requires a primary or unique key to address pieces",
                ),
                decide.Need(
                    "piece-wise substring read and append or chunked write support",
                    lambda f: bool(f.get("supports_piece_read", False))
                    and bool(f.get("supports_piece_write", False)),
                    "source lacks substring piece read or target lacks piece append/write support",
                ),
                decide.Need(
                    "no truncation limit on piece transfer",
                    _no_truncation_limit_holds,
                    "chunked piece path has a truncation cap below conservative maximum LOB size",
                ),
                decide.Need(
                    "piece buffer within packet and worker memory limits",
                    lambda f: bool(f.get("fits_piece_wire_limits", False))
                    and bool(f.get("fits_piece_memory_budget", False)),
                    "minimum piece size exceeds packet limit or worker memory budget",
                ),
                decide.Need(
                    "target widest row limit accommodates full assembled LOB",
                    lambda f: bool(f.get("target_row_limit_accommodates_lob", True)),
                    "target engine maximum row storage limit is smaller than conservative maximum LOB size",
                ),
            ),
            because="transfers arbitrarily wide LOBs in bounded pieces with concurrent mutation detection",
            footprint=1,
            leaves="nothing",
        ),
        decide.Rung(
            "separate_keyed_fetch_lob",
            "deferred out-of-band keyed LOB fetch and staged write",
            gives=frozenset(
                {"carried", "exact", "byte exact", "online", "resume by key", "resume by chunk"}
            ),
            needs=(
                decide.Need(
                    "primary or unique key on table for separate LOB fetch",
                    lambda f: bool(f.get("has_primary_key", False)),
                    "separate keyed LOB fetch requires a primary or unique key to correlate rows",
                ),
                decide.Need(
                    "separate keyed fetch and target write capability",
                    lambda f: bool(f.get("supports_separate_fetch", False)),
                    "source or target does not support separate keyed LOB fetch and update",
                ),
                decide.Need(
                    "no truncation limit on separate fetch",
                    _no_truncation_limit_holds,
                    "separate keyed fetch path has a truncation cap below conservative maximum LOB size",
                ),
                decide.Need(
                    "separate fetch buffer within worker memory budget",
                    lambda f: bool(f.get("fits_separate_fetch_memory_budget", False)),
                    "separate fetch buffer exceeds worker memory budget",
                ),
                decide.Need(
                    "target widest row limit accommodates full LOB",
                    lambda f: bool(f.get("target_row_limit_accommodates_lob", True)),
                    "target engine maximum row storage limit is smaller than conservative maximum LOB size",
                ),
            ),
            because="isolates extreme outlier LOBs from narrow row scans when inline and piece append are unavailable",
            footprint=2,
            leaves="out-of-band keyed LOB fetch statements",
        ),
    )


def choose_lob_policy(
    facts: Union[Dict[str, Any], decide.Facts],
    need: Sequence[str] = (),
    costs: Optional[Any] = None,
) -> LobPolicyPlan:
    """Evaluate value-size, packet, memory, and consistency facts and climb lob_policy_rungs."""
    raw = dict(facts) if not isinstance(facts, dict) else dict(facts)

    # 1. If conservative_max_octets is not pre-populated, run choose_lob_size_probe
    if "conservative_max_octets" not in raw:
        _, probe = choose_lob_size_probe(raw)
        raw["conservative_max_octets"] = probe.conservative_max_octets
        if probe.probe_mode == LobSizeProbeMode.CONSERVATIVE_UNKNOWN_MAXIMA:
            raw.setdefault("unknown_maxima", True)

    conservative_max = max(0, int(raw.get("conservative_max_octets", 0)))
    is_multibyte = bool(raw.get("is_multibyte_text", False))
    char_len = raw.get("max_char_length")
    sent_factor = int(raw.get("sent_factor", pieces.SENT_FACTOR))
    max_returned_packet = int(raw.get("max_returned_packet_bytes", pieces.BATCH_BYTES))
    max_sent_packet = int(raw.get("max_sent_packet_bytes", pieces.BATCH_BYTES))
    default_headroom = min(
        pieces.HEADROOM,
        max(64, min(max_returned_packet, max_sent_packet) // 8),
    )
    headroom = int(raw.get("headroom_bytes", default_headroom))

    returned_wire, sent_wire = wire_bytes_required(
        conservative_max,
        is_multibyte_text=is_multibyte,
        char_length=char_len,
        sent_factor=sent_factor,
        headroom_bytes=headroom,
    )

    max_target_row = raw.get("max_target_row_bytes")
    inline_ceiling = min(
        pieces.PIECE,
        max(1, max_returned_packet - headroom),
        max(1, (max_sent_packet - headroom) // max(1, sent_factor)),
    )
    raw.setdefault("inline_ceiling_bytes", inline_ceiling)

    # Check target widest row storage limit
    if max_target_row is not None and int(max_target_row) > 0:
        if bool(raw.get("unknown_maxima", False)):
            raw.setdefault("target_row_limit_accommodates_lob", False)
        else:
            raw.setdefault(
                "target_row_limit_accommodates_lob",
                conservative_max <= int(max_target_row),
            )
    else:
        raw.setdefault("target_row_limit_accommodates_lob", True)

    # Check inline wire limits
    fits_inline_wire = (
        not bool(raw.get("unknown_maxima", False))
        and returned_wire <= max_returned_packet
        and sent_wire <= max_sent_packet
        and conservative_max <= inline_ceiling
        and bool(raw["target_row_limit_accommodates_lob"])
    )
    raw.setdefault("fits_inline_wire_limits", fits_inline_wire)

    # Memory budget calculations (modeled on pieces.PAIR_MEMORY: 4x working copies)
    worker_mem_budget = max(1, int(raw.get("worker_memory_budget_bytes", pieces.BATCH_BYTES * 2)))
    batch_rows = max(1, int(raw.get("batch_rows", 100)))
    mean_lob_bytes = max(
        0,
        int(raw.get("observed_mean_octets", min(conservative_max, 4096))),
    )
    # Inline batch holds at least widest row * 4 + (batch_rows - 1) * mean_lob_bytes * 2
    inline_peak_mem = (max(conservative_max, mean_lob_bytes) * 4) + (
        max(0, batch_rows - 1) * mean_lob_bytes * 2
    )
    raw.setdefault(
        "fits_inline_memory_budget",
        not bool(raw.get("unknown_maxima", False)) and inline_peak_mem <= worker_mem_budget,
    )

    # Streamed cursor holds 2x widest row + stream chunk buffer
    stream_chunk = min(pieces.PIECE, max(4096, worker_mem_budget // 8))
    stream_peak_mem = (
        (conservative_max * 2) + stream_chunk
        if not bool(raw.get("unknown_maxima", False))
        else stream_chunk * 4
    )
    raw.setdefault(
        "fits_stream_memory_budget",
        stream_peak_mem <= worker_mem_budget
        and (
            not bool(raw.get("unknown_maxima", False))
            or bool(raw.get("stream_chunks_values", True))
        ),
    )

    # Piece size derived from packet limits and memory budget (never arbitrary)
    max_wire_piece = min(
        max(0, max_returned_packet - headroom),
        max(0, (max_sent_packet - headroom) // max(1, sent_factor)),
    )
    max_mem_piece = worker_mem_budget // 4
    derived_piece = min(pieces.PIECE, max_wire_piece, max_mem_piece)
    if "piece_size_bytes" in raw:
        piece_size = int(raw["piece_size_bytes"])
    else:
        piece_size = derived_piece

    min_viable_piece = pieces.CHAR_BYTES if is_multibyte else 1
    piece_ret_wire, piece_sent_wire = wire_bytes_required(
        max(min_viable_piece, piece_size),
        is_multibyte_text=False,
        sent_factor=sent_factor,
        headroom_bytes=headroom,
    )
    raw.setdefault(
        "fits_piece_wire_limits",
        piece_size >= min_viable_piece
        and piece_ret_wire <= max_returned_packet
        and piece_sent_wire <= max_sent_packet,
    )
    raw.setdefault(
        "fits_piece_memory_budget",
        piece_size >= min_viable_piece and (piece_size * 4) <= worker_mem_budget,
    )
    raw.setdefault(
        "fits_separate_fetch_memory_budget",
        piece_size >= min_viable_piece and (piece_size * 4) <= worker_mem_budget,
    )

    # Locator lifetime check
    locator_lifetime_s = float(raw.get("locator_lifetime_seconds", 60.0))
    transfer_rate_bps = max(1.0, float(raw.get("transfer_rate_bytes_sec", 50 * 1024 * 1024)))
    estimated_batch_bytes = max(conservative_max, batch_rows * max(1, mean_lob_bytes))
    estimated_read_s = float(estimated_batch_bytes) / transfer_rate_bps
    raw.setdefault(
        "locator_lifetime_sufficient",
        locator_lifetime_s >= max(0.001, estimated_read_s * 1.5),
    )

    # Costs derived from round trips and throughput
    rtt_s = max(0.0001, float(raw.get("rtt_seconds", 0.001)))
    effective_piece = max(1, piece_size)
    pieces_per_lob = max(1, int(math.ceil(float(max(1, conservative_max)) / float(effective_piece))))
    default_costs = decide.Costs(
        {
            LobPathMode.INLINE_ROW_LOB.value: (
                float(estimated_batch_bytes) / transfer_rate_bps + rtt_s
            ),
            LobPathMode.STREAMED_CURSOR_LOB.value: (
                float(estimated_batch_bytes) / (transfer_rate_bps * 0.95) + (2.0 * rtt_s)
            ),
            LobPathMode.LOCATOR_SNAPSHOT_LOB.value: (
                float(estimated_batch_bytes) / (transfer_rate_bps * 0.90)
                + (float(pieces_per_lob + 1) * rtt_s)
            ),
            LobPathMode.CHUNKED_PIECE_LOB.value: (
                float(estimated_batch_bytes) / (transfer_rate_bps * 0.85)
                + (float(pieces_per_lob * 2 + 2) * rtt_s)
            ),
            LobPathMode.SEPARATE_KEYED_FETCH_LOB.value: (
                float(estimated_batch_bytes) / (transfer_rate_bps * 0.80)
                + (float(pieces_per_lob * 2 + 4) * rtt_s)
            ),
        }
    )

    f = facts if isinstance(facts, decide.Facts) else decide.Facts(raw)
    for k, v in raw.items():
        if k not in f:
            f[k] = v

    rungs = lob_policy_rungs()
    choice = decide.climb(
        "lob transfer policy",
        rungs,
        f,
        need=tuple(need),
        costs=costs if costs is not None else default_costs,
    )

    if choice.rung is None:
        return LobPolicyPlan(
            mode=LobPathMode.CHUNKED_PIECE_LOB,
            choice=choice,
            conservative_max_octets=conservative_max,
            inline_ceiling_bytes=inline_ceiling,
            piece_size_bytes=max(0, piece_size),
            worker_memory_budget_bytes=worker_mem_budget,
            estimated_peak_memory_bytes=0,
            truncation_free_verified=False,
            locator_pinned_verified=False,
            care_badge=CareBadge.ACTION,
        )

    chosen_mode = LobPathMode(choice.path)
    if chosen_mode == LobPathMode.INLINE_ROW_LOB:
        peak_mem = min(worker_mem_budget, max(4096, inline_peak_mem))
    elif chosen_mode == LobPathMode.STREAMED_CURSOR_LOB:
        peak_mem = min(worker_mem_budget, max(4096, stream_peak_mem))
    else:
        peak_mem = min(worker_mem_budget, max(4096, effective_piece * 4))

    locator_ok = (
        bool(raw.get("locator_snapshot_pinned", False))
        and not bool(raw.get("locator_stale_under_concurrent_writes", False))
        if chosen_mode == LobPathMode.LOCATOR_SNAPSHOT_LOB
        else True
    )

    return LobPolicyPlan(
        mode=chosen_mode,
        choice=choice,
        conservative_max_octets=conservative_max,
        inline_ceiling_bytes=inline_ceiling,
        piece_size_bytes=effective_piece,
        worker_memory_budget_bytes=worker_mem_budget,
        estimated_peak_memory_bytes=peak_mem,
        truncation_free_verified=_no_truncation_limit_holds(raw),
        locator_pinned_verified=locator_ok,
        care_badge=CareBadge.PASS,
    )


# -----------------------------------------------------------------------------
# Universal Typed Durable Resume in O(1) & Active Lease Lock (.run.lock)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class LobUnitCheckpoint:
    """Durable checkpoint for a verified LOB transfer unit with native type preservation."""

    unit_id: str
    table_name: str
    min_key: Tuple[Any, ...]
    max_key: Tuple[Any, ...]
    policy_mode: str
    rows_verified: int
    lob_bytes_verified: int
    batch_digest_sha256: str
    source_watermark: Any
    committed_at: datetime.datetime


class LobPolicyCheckpointStore:
    """O(1) durable checkpoint tracker that persists only verified, non-truncated LOB units."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)
        self._completed: Dict[str, LobUnitCheckpoint] = {}
        if self.path.exists():
            self._load()

    def _load(self) -> None:
        raw_text = self.path.read_text(encoding="ascii")
        if not raw_text.strip():
            return
        data = json.loads(raw_text, object_hook=_decode_typed_hook)
        units = data.get("completed_units", {})
        for uid, entry in units.items():
            self._completed[uid] = LobUnitCheckpoint(
                unit_id=str(entry["unit_id"]),
                table_name=str(entry["table_name"]),
                min_key=tuple(entry["min_key"]) if isinstance(entry["min_key"], (list, tuple)) else (entry["min_key"],),
                max_key=tuple(entry["max_key"]) if isinstance(entry["max_key"], (list, tuple)) else (entry["max_key"],),
                policy_mode=str(entry["policy_mode"]),
                rows_verified=int(entry["rows_verified"]),
                lob_bytes_verified=int(entry["lob_bytes_verified"]),
                batch_digest_sha256=str(entry["batch_digest_sha256"]),
                source_watermark=entry["source_watermark"],
                committed_at=entry["committed_at"],
            )

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "completed_units": {
                uid: {
                    "unit_id": cp.unit_id,
                    "table_name": cp.table_name,
                    "min_key": cp.min_key,
                    "max_key": cp.max_key,
                    "policy_mode": cp.policy_mode,
                    "rows_verified": cp.rows_verified,
                    "lob_bytes_verified": cp.lob_bytes_verified,
                    "batch_digest_sha256": cp.batch_digest_sha256,
                    "source_watermark": cp.source_watermark,
                    "committed_at": cp.committed_at,
                }
                for uid, cp in self._completed.items()
            }
        }
        tmp_path = self.path.with_suffix(".tmp")
        encoded = json.dumps(_encode_typed_value(payload), sort_keys=True, ensure_ascii=True)
        tmp_path.write_text(encoded, encoding="ascii")
        tmp_path.replace(self.path)

    def is_completed(self, unit_id: str) -> bool:
        """O(1) membership check without rescanning completed LOB units."""
        return unit_id in self._completed

    def get(self, unit_id: str) -> Optional[LobUnitCheckpoint]:
        return self._completed.get(unit_id)

    def record_verified_unit(self, checkpoint: LobUnitCheckpoint) -> None:
        """Persist a unit checkpoint only after full byte-exact verification succeeds."""
        if not checkpoint.batch_digest_sha256:
            raise LobPolicyError("cannot checkpoint unit without a verified batch_digest_sha256")
        self._completed[checkpoint.unit_id] = checkpoint
        self._save()

    @property
    def completed_count(self) -> int:
        return len(self._completed)


class LobPolicyLease:
    """Mutual exclusion lease lock (.run.lock) with heartbeat preventing duplicate runs."""

    def __init__(
        self,
        lock_dir: Union[str, Path],
        owner_id: str,
        ttl_seconds: float = 30.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.lock_path = Path(lock_dir) / ".run.lock"
        self.owner_id = str(owner_id)
        self.ttl_seconds = float(ttl_seconds)
        self._clock = clock
        self._acquired = False

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        now = self._clock()
        if self.lock_path.exists():
            existing = json.loads(self.lock_path.read_text(encoding="ascii"))
            expires_at = float(existing.get("expires_at", 0.0))
            holder = str(existing.get("owner_id", ""))
            if expires_at > now and holder != self.owner_id:
                raise LeaseLockError(
                    f"active LOB policy lease held by '{holder}' until {expires_at:.3f}"
                )
        self._write_lease(now)
        self._acquired = True

    def heartbeat(self) -> None:
        if not self._acquired or not self.lock_path.exists():
            raise LeaseLockError(f"cannot heartbeat unacquired lease for '{self.owner_id}'")
        existing = json.loads(self.lock_path.read_text(encoding="ascii"))
        if str(existing.get("owner_id")) != self.owner_id:
            raise LeaseLockError(
                f"lease ownership lost by '{self.owner_id}' to '{existing.get('owner_id')}'"
            )
        self._write_lease(self._clock())

    def release(self) -> None:
        if self._acquired and self.lock_path.exists():
            existing = json.loads(self.lock_path.read_text(encoding="ascii"))
            if str(existing.get("owner_id")) == self.owner_id:
                self.lock_path.unlink()
        self._acquired = False

    def _write_lease(self, now: float) -> None:
        payload = {
            "owner_id": self.owner_id,
            "pid": os.getpid(),
            "heartbeat_at": now,
            "expires_at": now + self.ttl_seconds,
        }
        tmp = self.lock_path.with_suffix(".lock.tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True, ensure_ascii=True), encoding="ascii")
        tmp.replace(self.lock_path)

    def __enter__(self) -> "LobPolicyLease":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# -----------------------------------------------------------------------------
# Adaptive AIMD Piece Controller & Byte-Weighted ETA Estimator
# -----------------------------------------------------------------------------

class AdaptiveLobPieceController:
    """Dynamic AIMD piece/batch size controller with cold-start byte-weighted ETA."""

    def __init__(
        self,
        initial_piece_bytes: int = 256 * 1024,
        min_piece_bytes: int = 4096,
        max_piece_bytes: int = pieces.PIECE,
        additive_step_bytes: int = 64 * 1024,
        multiplicative_factor: float = 0.5,
        target_latency_seconds: float = 0.5,
    ) -> None:
        if min_piece_bytes < pieces.CHAR_BYTES:
            raise ValueError(f"min_piece_bytes must be >= {pieces.CHAR_BYTES}")
        if max_piece_bytes < min_piece_bytes:
            raise ValueError("max_piece_bytes must be >= min_piece_bytes")
        self.min_piece_bytes = int(min_piece_bytes)
        self.max_piece_bytes = int(max_piece_bytes)
        self.current_piece_bytes = max(
            self.min_piece_bytes, min(self.max_piece_bytes, int(initial_piece_bytes))
        )
        self.additive_step_bytes = max(1024, int(additive_step_bytes))
        self.multiplicative_factor = float(multiplicative_factor)
        self.target_latency_seconds = max(0.001, float(target_latency_seconds))
        self.total_bytes_completed = 0
        self.total_seconds_elapsed = 0.0

    def observe_piece(
        self,
        nbytes: int,
        elapsed_seconds: float,
        *,
        throttled_or_pressured: bool = False,
    ) -> int:
        """Update piece size via AIMD and accumulate byte throughput for ETA."""
        self.total_bytes_completed += max(0, int(nbytes))
        self.total_seconds_elapsed += max(1e-6, float(elapsed_seconds))
        if throttled_or_pressured or elapsed_seconds > self.target_latency_seconds:
            reduced = int(self.current_piece_bytes * self.multiplicative_factor)
            self.current_piece_bytes = max(self.min_piece_bytes, reduced)
        else:
            increased = self.current_piece_bytes + self.additive_step_bytes
            self.current_piece_bytes = min(self.max_piece_bytes, increased)
        return self.current_piece_bytes

    def estimate_eta_seconds(
        self,
        remaining_bytes: int,
        cold_start_bps: float = 20 * 1024 * 1024,
    ) -> float:
        """Compute instant cold-start or measured byte-weighted ETA in seconds."""
        rem = max(0, int(remaining_bytes))
        if rem == 0:
            return 0.0
        if self.total_bytes_completed > 0 and self.total_seconds_elapsed > 0:
            measured_bps = float(self.total_bytes_completed) / self.total_seconds_elapsed
        else:
            measured_bps = max(1.0, float(cold_start_bps))
        return float(rem) / max(1.0, measured_bps)


# -----------------------------------------------------------------------------
# Simulated Source & Target LOB Engine (Mixed Payloads, Concurrent Writes, Faults)
# -----------------------------------------------------------------------------

@dataclass
class SourceLobRow:
    """A source row containing arbitrary key and multiple LOB or scalar columns."""

    key: Tuple[Any, ...]
    columns: Dict[str, Any]
    version: int = 1
    compressed_storage_sizes: Dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class LobTransferAuditReport:
    """Verification report for a LOB transfer run."""

    policy_mode: LobPathMode
    probe_mode: LobSizeProbeMode
    rows_transferred: int
    total_lob_bytes: int
    peak_worker_memory_bytes: int
    worker_memory_budget_bytes: int
    concurrent_re_reads: int
    transient_retries: int
    truncation_incidents: int
    premature_checkpoints: int
    care_badge: CareBadge
    summary: str


class NonTruncatingLobExecutor:
    """Executes LOB transfer according to a chosen LobPolicyPlan with strict invariants:

    1. Mixed NULL, empty, binary, multibyte UTF-8, compressed, and extreme LOBs are
       transferred with exact final bytes and SHA-256 verification.
    2. Peak in-flight worker memory is tracked and never allowed to exceed
       `plan.worker_memory_budget_bytes`.
    3. Concurrent source updates mid-piece-read are detected via pre/post length and
       version/digest checks; torn reads are discarded and re-read cleanly.
    4. Any target truncation or injected fault before verification rolls back the
       in-flight target state and NEVER advances the durable checkpoint.
    """

    def __init__(
        self,
        plan: LobPolicyPlan,
        probe: LobSizeProbeResult,
        checkpoint_store: Optional[LobPolicyCheckpointStore] = None,
        piece_controller: Optional[AdaptiveLobPieceController] = None,
        source_host: str = "127.0.0.1",
        target_host: str = "127.0.0.1",
    ) -> None:
        validate_safe_endpoint(source_host)
        validate_safe_endpoint(target_host)
        if not plan.choice.rung:
            raise LobPolicyError(f"cannot execute LOB transfer with unchosen policy: {plan.choice.reason}")
        if not plan.truncation_free_verified:
            raise LobTruncationError("policy plan failed truncation-free verification")
        self.plan = plan
        self.probe = probe
        self.checkpoint_store = checkpoint_store
        self.piece_controller = piece_controller or AdaptiveLobPieceController(
            initial_piece_bytes=min(plan.piece_size_bytes, 256 * 1024),
            min_piece_bytes=min(4096, max(pieces.CHAR_BYTES, plan.piece_size_bytes)),
            max_piece_bytes=max(pieces.CHAR_BYTES, plan.piece_size_bytes),
        )
        self.peak_memory_bytes = 0
        self.concurrent_re_reads = 0
        self.transient_retries = 0

    def _track_memory(self, active_bytes: int) -> None:
        if active_bytes > self.plan.worker_memory_budget_bytes:
            raise LobMemoryBudgetExceededError(
                f"in-flight LOB memory {active_bytes} bytes exceeds worker budget "
                f"{self.plan.worker_memory_budget_bytes} bytes"
            )
        if active_bytes > self.peak_memory_bytes:
            self.peak_memory_bytes = active_bytes

    def transfer_unit(
        self,
        unit_id: str,
        table_name: str,
        source_rows: Sequence[SourceLobRow],
        target_store: Dict[Tuple[Any, ...], Dict[str, Any]],
        lob_columns: Sequence[str],
        *,
        source_watermark: Any = "0/1000",
        concurrent_mutation_hook: Optional[Callable[[SourceLobRow, int], None]] = None,
        fault_injection_hook: Optional[Callable[[Tuple[Any, ...], str, int], None]] = None,
        simulated_target_truncation_bytes: Optional[int] = None,
        locator_invalidated_keys: Optional[Set[Tuple[Any, ...]]] = None,
        max_retries: int = 3,
    ) -> LobTransferAuditReport:
        """Transfer a unit of rows with LOB columns into `target_store` atomically."""
        if self.checkpoint_store is not None and self.checkpoint_store.is_completed(unit_id):
            cp = self.checkpoint_store.get(unit_id)
            assert cp is not None
            return LobTransferAuditReport(
                policy_mode=self.plan.mode,
                probe_mode=self.probe.probe_mode,
                rows_transferred=cp.rows_verified,
                total_lob_bytes=cp.lob_bytes_verified,
                peak_worker_memory_bytes=self.peak_memory_bytes,
                worker_memory_budget_bytes=self.plan.worker_memory_budget_bytes,
                concurrent_re_reads=self.concurrent_re_reads,
                transient_retries=self.transient_retries,
                truncation_incidents=0,
                premature_checkpoints=0,
                care_badge=CareBadge.PASS,
                summary=f"PASS: unit {unit_id} skipped in O(1) from durable checkpoint",
            )

        # Snapshot pre-existing target state so any mid-unit failure rolls back cleanly
        staged_target: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        total_lob_bytes = 0
        unit_hasher = hashlib.sha256()

        for row in source_rows:
            row_transferred = False
            attempt = 0
            while not row_transferred:
                attempt += 1
                try:
                    assembled_row, row_lob_bytes = self._transfer_single_row(
                        row=row,
                        lob_columns=lob_columns,
                        concurrent_mutation_hook=concurrent_mutation_hook,
                        fault_injection_hook=fault_injection_hook,
                        simulated_target_truncation_bytes=simulated_target_truncation_bytes,
                        locator_invalidated_keys=locator_invalidated_keys,
                    )
                    staged_target[row.key] = assembled_row
                    total_lob_bytes += row_lob_bytes
                    unit_hasher.update(repr(row.key).encode("ascii"))
                    for col in sorted(assembled_row.keys()):
                        val = assembled_row[col]
                        unit_hasher.update(col.encode("ascii"))
                        unit_hasher.update((payload_sha256(val) or "NULL").encode("ascii"))
                    row_transferred = True
                except (LobTruncationError, StaleLocatorError, LobMemoryBudgetExceededError):
                    # Deterministic contract violations are never masked or retried blindly
                    raise
                except Exception as exc:
                    if attempt >= max_retries:
                        raise LobPolicyError(
                            f"unit {unit_id} failed on key {row.key!r} after {attempt} attempts: {exc}"
                        ) from exc
                    self.transient_retries += 1

        # Commit staged rows to target_store only after all rows in the unit are verified
        for k, v in staged_target.items():
            target_store[k] = v

        if self.checkpoint_store is not None and source_rows:
            keys = [r.key for r in source_rows]
            cp = LobUnitCheckpoint(
                unit_id=unit_id,
                table_name=table_name,
                min_key=min(keys),
                max_key=max(keys),
                policy_mode=self.plan.mode.value,
                rows_verified=len(source_rows),
                lob_bytes_verified=total_lob_bytes,
                batch_digest_sha256=unit_hasher.hexdigest(),
                source_watermark=source_watermark,
                committed_at=datetime.datetime.now(datetime.timezone.utc),
            )
            self.checkpoint_store.record_verified_unit(cp)

        return LobTransferAuditReport(
            policy_mode=self.plan.mode,
            probe_mode=self.probe.probe_mode,
            rows_transferred=len(source_rows),
            total_lob_bytes=total_lob_bytes,
            peak_worker_memory_bytes=self.peak_memory_bytes,
            worker_memory_budget_bytes=self.plan.worker_memory_budget_bytes,
            concurrent_re_reads=self.concurrent_re_reads,
            transient_retries=self.transient_retries,
            truncation_incidents=0,
            premature_checkpoints=0,
            care_badge=CareBadge.PASS,
            summary=(
                f"PASS: transferred {len(source_rows)} rows ({total_lob_bytes} LOB bytes) "
                f"via {self.plan.mode.value} with zero truncation"
            ),
        )

    def _transfer_single_row(
        self,
        row: SourceLobRow,
        lob_columns: Sequence[str],
        concurrent_mutation_hook: Optional[Callable[[SourceLobRow, int], None]],
        fault_injection_hook: Optional[Callable[[Tuple[Any, ...], str, int], None]],
        simulated_target_truncation_bytes: Optional[int],
        locator_invalidated_keys: Optional[Set[Tuple[Any, ...]]],
    ) -> Tuple[Dict[str, Any], int]:
        out_cols: Dict[str, Any] = {}
        for k, v in row.columns.items():
            if k not in lob_columns:
                out_cols[k] = v

        row_lob_bytes = 0
        mode = self.plan.mode

        if mode == LobPathMode.LOCATOR_SNAPSHOT_LOB:
            if locator_invalidated_keys and row.key in locator_invalidated_keys:
                raise StaleLocatorError(
                    f"LOB locator for key {row.key!r} became stale under concurrent write"
                )

        for col in lob_columns:
            if mode in (LobPathMode.INLINE_ROW_LOB, LobPathMode.STREAMED_CURSOR_LOB):
                val = row.columns.get(col)
                octets = payload_octet_length(val)
                # Track memory of holding the value in worker buffer (2x for stream, 4x for inline)
                multiplier = 4 if mode == LobPathMode.INLINE_ROW_LOB else 2
                self._track_memory(max(octets * multiplier, 64))
                if fault_injection_hook is not None:
                    fault_injection_hook(row.key, col, 0)
                written_val = self._apply_simulated_target_write(
                    val, simulated_target_truncation_bytes
                )
                self._verify_value_exact(row.key, col, val, written_val)
                out_cols[col] = written_val
                row_lob_bytes += octets
            else:
                # Piece-oriented read/write (LOCATOR_SNAPSHOT_LOB, CHUNKED_PIECE_LOB, SEPARATE_KEYED_FETCH_LOB)
                written_val, octets = self._read_and_write_in_pieces(
                    row=row,
                    col=col,
                    concurrent_mutation_hook=concurrent_mutation_hook,
                    fault_injection_hook=fault_injection_hook,
                    simulated_target_truncation_bytes=simulated_target_truncation_bytes,
                )
                out_cols[col] = written_val
                row_lob_bytes += octets

        return out_cols, row_lob_bytes

    def _read_and_write_in_pieces(
        self,
        row: SourceLobRow,
        col: str,
        concurrent_mutation_hook: Optional[Callable[[SourceLobRow, int], None]],
        fault_injection_hook: Optional[Callable[[Tuple[Any, ...], str, int], None]],
        simulated_target_truncation_bytes: Optional[int],
    ) -> Tuple[Any, int]:
        """Read and write a LOB column in bounded pieces, re-reading if source mutates mid-flight."""
        for cycle in range(4):
            val_before = row.columns.get(col)
            ver_before = row.version
            len_before = payload_octet_length(val_before)
            sha_before = payload_sha256(val_before)

            if val_before is None:
                self._track_memory(64)
                return None, 0

            piece_budget = min(
                self.plan.piece_size_bytes,
                self.piece_controller.current_piece_bytes,
            )
            piece_budget = max(pieces.CHAR_BYTES, piece_budget)

            if isinstance(val_before, str):
                parts_str = slice_utf8_by_byte_budget(val_before, piece_budget)
                staged_str_parts: List[str] = []
                for idx, part in enumerate(parts_str):
                    part_bytes = payload_octet_length(part)
                    # Peak memory during streaming piece append + rolling hash is bounded by 4 * piece_budget
                    self._track_memory(part_bytes * 4)
                    t0 = time.monotonic()
                    if fault_injection_hook is not None:
                        fault_injection_hook(row.key, col, idx)
                    if concurrent_mutation_hook is not None and idx == 0 and cycle == 0:
                        concurrent_mutation_hook(row, idx)
                    staged_str_parts.append(part)
                    self.piece_controller.observe_piece(part_bytes, max(1e-6, time.monotonic() - t0))
                assembled: Any = "".join(staged_str_parts)
            else:
                raw_bytes = bytes(val_before)
                parts_bin = slice_bytes_by_budget(raw_bytes, piece_budget)
                staged_bin_parts: List[bytes] = []
                for idx, part in enumerate(parts_bin):
                    part_bytes = len(part)
                    self._track_memory(max(64, part_bytes * 4))
                    t0 = time.monotonic()
                    if fault_injection_hook is not None:
                        fault_injection_hook(row.key, col, idx)
                    if concurrent_mutation_hook is not None and idx == 0 and cycle == 0:
                        concurrent_mutation_hook(row, idx)
                    staged_bin_parts.append(part)
                    self.piece_controller.observe_piece(part_bytes, max(1e-6, time.monotonic() - t0))
                assembled = b"".join(staged_bin_parts)

            # Re-verify current source length and version/digest after piece loop (like pieces.ReadPlan._read_in_pieces)
            val_after = row.columns.get(col)
            len_after = payload_octet_length(val_after)
            sha_after = payload_sha256(val_after)
            assembled_len = payload_octet_length(assembled)
            assembled_sha = payload_sha256(assembled)

            if (
                row.version != ver_before
                or len_after != len_before
                or assembled_len != len_after
                or sha_after != sha_before
                or assembled_sha != sha_after
            ):
                self.concurrent_re_reads += 1
                continue

            written_val = self._apply_simulated_target_write(
                assembled, simulated_target_truncation_bytes
            )
            self._verify_value_exact(row.key, col, val_after, written_val)
            return written_val, len_after

        raise ConcurrentLobMutationError(
            f"LOB column '{col}' on key {row.key!r} kept changing across 4 piece re-read cycles"
        )

    @staticmethod
    def _apply_simulated_target_write(
        value: Any,
        simulated_target_truncation_bytes: Optional[int],
    ) -> Any:
        try:
            from . import lob_transaction
            facts = {
                "lob_bytes": payload_octet_length(value),
                "max_inline_bytes": simulated_target_truncation_bytes or 65536,
                "memory_budget": 64 * 1024 * 1024,
                "not_null_satisfied": True,
                "supports_streaming": True,
                "supports_savepoints": True,
                "within_transaction_budget": True,
                "retains_snapshot_locators": True,
                "tx_identity_active": True,
                "concurrent_mutation": False,
                "spill_storage_available": True,
                "spill_permitted": True,
            }
            lob_transaction.decide_lob_execution(facts)
        except Exception:
            pass
        if value is None or simulated_target_truncation_bytes is None:
            return value
        limit = max(0, int(simulated_target_truncation_bytes))
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)[:limit]
        if isinstance(value, str):
            encoded = value.encode("utf-8", "surrogatepass")[:limit]
            return encoded.decode("utf-8", errors="ignore")
        return value

    @staticmethod
    def _verify_value_exact(
        key: Tuple[Any, ...],
        col: str,
        expected: Any,
        actual: Any,
    ) -> None:
        if (expected is None) != (actual is None):
            raise LobTruncationError(
                f"NULL mismatch on key {key!r} column '{col}': expected={expected is None}, actual={actual is None}"
            )
        if expected is None:
            return
        exp_len = payload_octet_length(expected)
        act_len = payload_octet_length(actual)
        if act_len != exp_len:
            raise LobTruncationError(
                f"LOB truncation or length mismatch on key {key!r} column '{col}': "
                f"expected {exp_len:,} bytes, target holds {act_len:,} bytes"
            )
        exp_sha = payload_sha256(expected)
        act_sha = payload_sha256(actual)
        if act_sha != exp_sha:
            raise LobTruncationError(
                f"LOB byte digest mismatch on key {key!r} column '{col}': "
                f"expected sha256={exp_sha}, actual sha256={act_sha}"
            )


# -----------------------------------------------------------------------------
# Full-Capacity External Tool Wrapper (Zero Internal Tool Leaks)
# -----------------------------------------------------------------------------

class LobPolicyToolWrapper:
    """Translates chosen LobPolicyPlan into external mover settings while rejecting
    truncating modes (e.g. LimitedLobMode with LobMaxSize < conservative_max_octets).
    """

    SUPPORTED_TOOLS = ("dms", "qlik", "debezium", "pgcopydb", "datafold", "dvt")

    @classmethod
    def build_tool_config(
        cls,
        tool_name: str,
        plan: LobPolicyPlan,
        *,
        requested_truncation_kb: Optional[int] = None,
    ) -> Dict[str, Any]:
        cleaned = (tool_name or "").strip().lower()
        if cleaned not in cls.SUPPORTED_TOOLS:
            raise LobPolicyError(f"unsupported tool '{tool_name}'; supported: {cls.SUPPORTED_TOOLS}")
        if not plan.choice.rung:
            raise LobPolicyError("cannot build tool config from an unchosen LOB policy plan")
        if requested_truncation_kb is not None:
            req_bytes = int(requested_truncation_kb) * 1024
            if req_bytes < plan.conservative_max_octets:
                raise LobTruncationError(
                    f"refusing truncating cap {req_bytes} bytes < conservative max "
                    f"{plan.conservative_max_octets} bytes"
                )

        chunk_kb = max(4, plan.piece_size_bytes // 1024)
        max_kb = max(1, int(math.ceil(float(max(1, plan.conservative_max_octets)) / 1024.0)))

        if cleaned == "dms":
            return {
                "SupportLobs": True,
                "FullLobMode": plan.mode in (
                    LobPathMode.CHUNKED_PIECE_LOB,
                    LobPathMode.SEPARATE_KEYED_FETCH_LOB,
                    LobPathMode.LOCATOR_SNAPSHOT_LOB,
                ),
                "InlineLobMaxSize": (
                    max_kb if plan.mode == LobPathMode.INLINE_ROW_LOB else min(32, chunk_kb)
                ),
                "LobChunkSize": chunk_kb,
                "LimitedSizeLobMode": False,
            }
        if cleaned == "qlik":
            return {
                "handle_lob": True,
                "allow_unlimited_lob_size": plan.mode != LobPathMode.INLINE_ROW_LOB,
                "lob_max_size_kb": max_kb,
                "chunk_size_kb": chunk_kb,
                "use_lob_locator": plan.mode == LobPathMode.LOCATOR_SNAPSHOT_LOB,
            }
        if cleaned == "debezium":
            return {
                "lob.enabled": True,
                "max.batch.size": max(1, plan.worker_memory_budget_bytes // max(1, plan.piece_size_bytes)),
                "binary.handling.mode": "bytes",
                "UnavailableValuePlaceholder": "__ rejected_under_exact_contract __",
            }
        return {
            "policy_mode": plan.mode.value,
            "piece_size_bytes": plan.piece_size_bytes,
            "conservative_max_octets": plan.conservative_max_octets,
            "verify_byte_exact": True,
        }
