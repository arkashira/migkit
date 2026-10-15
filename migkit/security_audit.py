"""Security audit, cryptographic integrity, transparent at-rest encryption,
scorecard hardening, format-preserving encryption, and immutable audit anchors
(backlog items 325/R17b.hmac, 326/R17b.prog, 223/F7.sec, 287/W4.b, 301/R5.anchor).
"""
import base64
import dataclasses
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
import struct
import tempfile
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

REPORT_HMAC_ALGORITHM = "HMAC-SHA256"
REPORT_ENVELOPE_FORMAT = "migkit-report-envelope-v1"
CHECKPOINT_ENVELOPE_FORMAT = "migkit-audit-immutable-envelope-v1"
ENC_MAGIC = b"MIGKIT-ENC-REST-V1\x00"

SECRET_KEY_PATTERN = re.compile(
    r"pass|pwd|secret|token|key|credential|notify|auth", re.I
)

BEARER_PATTERN = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9_\-\.=]+")
JWT_PATTERN = re.compile(r"\beyJ[A-Za-z0-9_\-]+\.eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\b")
API_KEY_PATTERN = re.compile(r"\b(sk-[A-Za-z0-9]{20,}|ghp_[A-Za-z0-9]{20,}|glpat-[A-Za-z0-9\-_]{20,})\b")
AWS_KEY_PATTERN = re.compile(r"\bAKIA[0-9A-Z]{16}\b")
URI_CREDENTIAL_PATTERN = re.compile(r"(?P<prefix>\b[A-Za-z][\w+.-]*://)(?P<user>[^\s:/@'\"]*):(?P<password>[^\s'\"]*)@")


def canonical_json_bytes(data: Any) -> bytes:
    """Canonical JSON serialization for deterministic cryptographic hashing."""
    def _default(obj: Any) -> Any:
        if isinstance(obj, (datetime,)):
            return obj.isoformat()
        if isinstance(obj, (Path,)):
            return str(obj)
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return dataclasses.asdict(obj)
        return str(obj)

    return json.dumps(
        data,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=_default,
    ).encode("utf-8")


def derive_report_key(secret_or_hop: Any, salt: Optional[bytes] = None) -> bytes:
    """Derive 32-byte report integrity key from secret or hop configuration."""
    if hasattr(secret_or_hop, "report_dir"):
        from . import masking
        hop_salt = masking._salt(secret_or_hop)
        local_secret = masking._secret()
        return hmac.new(
            local_secret,
            b"migkit-report-integrity-v1|" + hop_salt,
            hashlib.sha256,
        ).digest()
    raw = secret_or_hop if isinstance(secret_or_hop, bytes) else str(secret_or_hop).encode("utf-8")
    salt_bytes = salt or b"migkit-report-salt-v1"
    return hmac.new(raw, b"migkit-report-derived|" + salt_bytes, hashlib.sha256).digest()


def sign_report_data(payload: Any, key: bytes, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Compute keyed HMAC envelope over report data to prove report integrity (Item 325)."""
    payload_bytes = canonical_json_bytes(payload)
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
    ts = datetime.now(timezone.utc).isoformat()
    mac_data = b"|".join([
        REPORT_ENVELOPE_FORMAT.encode("utf-8"),
        REPORT_HMAC_ALGORITHM.encode("utf-8"),
        payload_sha256.encode("utf-8"),
        ts.encode("utf-8"),
        canonical_json_bytes(metadata or {}),
    ])
    signature = hmac.new(key, mac_data, hashlib.sha256).hexdigest()
    return {
        "format": REPORT_ENVELOPE_FORMAT,
        "version": 1,
        "algorithm": REPORT_HMAC_ALGORITHM,
        "timestamp": ts,
        "payload_sha256": payload_sha256,
        "metadata": metadata or {},
        "signature": signature,
        "payload": payload,
    }


def verify_report_data(envelope: Dict[str, Any], key: bytes) -> Tuple[bool, str]:
    """Verify integrity of a signed report data envelope."""
    if not isinstance(envelope, dict):
        return False, "Envelope must be a dict"
    if envelope.get("format") != REPORT_ENVELOPE_FORMAT:
        return False, f"Unexpected format: {envelope.get('format')}"
    if envelope.get("algorithm") != REPORT_HMAC_ALGORITHM:
        return False, f"Unsupported algorithm: {envelope.get('algorithm')}"

    payload = envelope.get("payload")
    claimed_sha = envelope.get("payload_sha256")
    claimed_sig = envelope.get("signature")
    ts = envelope.get("timestamp", "")
    metadata = envelope.get("metadata", {})

    actual_bytes = canonical_json_bytes(payload)
    actual_sha = hashlib.sha256(actual_bytes).hexdigest()
    if not hmac.compare_digest(actual_sha, str(claimed_sha)):
        return False, "Payload digest mismatch: content was altered"

    mac_data = b"|".join([
        REPORT_ENVELOPE_FORMAT.encode("utf-8"),
        REPORT_HMAC_ALGORITHM.encode("utf-8"),
        actual_sha.encode("utf-8"),
        str(ts).encode("utf-8"),
        canonical_json_bytes(metadata),
    ])
    expected_sig = hmac.new(key, mac_data, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_sig, str(claimed_sig)):
        return False, "HMAC signature mismatch"

    return True, "OK"


def sign_report_file(file_path: Union[str, Path], key: bytes, sidecar: bool = True) -> Path:
    """Sign an existing report file with keyed HMAC and save sidecar or sealed envelope."""
    p = Path(file_path)
    data = p.read_bytes()
    file_sha256 = hashlib.sha256(data).hexdigest()
    ts = datetime.now(timezone.utc).isoformat()
    mac_data = b"|".join([
        b"migkit-report-file-v1",
        p.name.encode("utf-8"),
        file_sha256.encode("utf-8"),
        ts.encode("utf-8"),
    ])
    signature = hmac.new(key, mac_data, hashlib.sha256).hexdigest()
    sidecar_dict = {
        "format": "migkit-report-file-hmac-v1",
        "file_name": p.name,
        "file_sha256": file_sha256,
        "file_size": len(data),
        "timestamp": ts,
        "algorithm": REPORT_HMAC_ALGORITHM,
        "signature": signature,
    }
    sidecar_path = p.with_name(p.name + ".hmac")
    fd = os.open(sidecar_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(sidecar_dict, f, indent=2, sort_keys=True)
    return sidecar_path


def verify_report_file(file_path: Union[str, Path], key: bytes, sidecar_path: Optional[Union[str, Path]] = None) -> Tuple[bool, str]:
    """Verify keyed HMAC signature of a report file."""
    p = Path(file_path)
    if not p.is_file():
        return False, f"File not found: {file_path}"
    sc_path = Path(sidecar_path) if sidecar_path else p.with_name(p.name + ".hmac")
    if not sc_path.is_file():
        return False, f"Signature sidecar not found: {sc_path}"

    try:
        sidecar = json.loads(sc_path.read_text(encoding="utf-8"))
    except Exception as e:
        return False, f"Failed reading sidecar: {e}"

    data = p.read_bytes()
    actual_sha = hashlib.sha256(data).hexdigest()
    if not hmac.compare_digest(actual_sha, sidecar.get("file_sha256", "")):
        return False, "File SHA-256 digest mismatch"

    mac_data = b"|".join([
        b"migkit-report-file-v1",
        sidecar.get("file_name", p.name).encode("utf-8"),
        actual_sha.encode("utf-8"),
        str(sidecar.get("timestamp", "")).encode("utf-8"),
    ])
    expected_sig = hmac.new(key, mac_data, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_sig, sidecar.get("signature", "")):
        return False, "File HMAC signature mismatch"

    return True, "OK"


def generate_ephemeral_key() -> bytes:
    """Generate ephemeral 256-bit symmetric key for dump/intermediate storage."""
    return os.urandom(32)


def derive_hop_at_rest_key(hop_name: str, master_secret: bytes, salt: Optional[bytes] = None) -> bytes:
    """Derive deterministic hop encryption key for at-rest scratch storage."""
    salt_bytes = salt or b"migkit-at-rest-salt-v1"
    info = f"migkit-hop-at-rest-v1|{hop_name}".encode("utf-8")
    return hmac.new(master_secret, salt_bytes + b"|" + info, hashlib.sha256).digest()


def encrypt_bytes(data: bytes, key: bytes, associated_data: Optional[bytes] = None) -> bytes:
    """Transparently encrypt payload with AES-256-GCM authenticated cipher."""
    if len(key) != 32:
        key = hashlib.sha256(key).digest()
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    ciphertext = aesgcm.encrypt(nonce, data, associated_data)
    return ENC_MAGIC + nonce + ciphertext


def decrypt_bytes(payload: bytes, key: bytes, associated_data: Optional[bytes] = None) -> bytes:
    """Decrypt AES-256-GCM payload with integrity verification."""
    if len(key) != 32:
        key = hashlib.sha256(key).digest()
    if not payload.startswith(ENC_MAGIC):
        raise ValueError("Invalid ciphertext magic header")
    offset = len(ENC_MAGIC)
    nonce = payload[offset:offset + 12]
    ciphertext = payload[offset + 12:]
    aesgcm = AESGCM(key)
    return aesgcm.decrypt(nonce, ciphertext, associated_data)


def encrypt_file(
    src_path: Union[str, Path],
    dst_path: Optional[Union[str, Path]] = None,
    key: Optional[bytes] = None,
) -> Tuple[Path, bytes]:
    """Encrypt file at rest with ephemeral or specified key, enforcing 0600 mode (Item 326)."""
    src = Path(src_path)
    enc_key = key or generate_ephemeral_key()
    target = Path(dst_path) if dst_path else src.with_suffix(src.suffix + ".enc")
    target.parent.mkdir(parents=True, exist_ok=True)

    plain_data = src.read_bytes()
    encrypted_data = encrypt_bytes(plain_data, enc_key)

    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(encrypted_data)
    os.chmod(target, 0o600)
    return target, enc_key


def decrypt_file(
    src_path: Union[str, Path],
    dst_path: Optional[Union[str, Path]] = None,
    key: bytes = b"",
) -> Path:
    """Decrypt file encrypted at rest, enforcing 0600 permissions."""
    src = Path(src_path)
    target = Path(dst_path) if dst_path else src.with_suffix("")
    if target == src:
        target = src.with_suffix(".dec")
    target.parent.mkdir(parents=True, exist_ok=True)

    encrypted_data = src.read_bytes()
    plain_data = decrypt_bytes(encrypted_data, key)

    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(plain_data)
    os.chmod(target, 0o600)
    return target


class TransparentProgramSpool:
    """Context manager transparently encrypting program dumps & intermediates at rest (Item 326)."""

    def __init__(
        self,
        directory: Union[str, Path],
        key: Optional[bytes] = None,
        hop_name: str = "default",
    ):
        self.directory = Path(directory)
        self.key = key or generate_ephemeral_key()
        self.hop_name = hop_name
        self.sealed_files: Dict[str, Path] = {}

    def __enter__(self) -> "TransparentProgramSpool":
        self.directory.mkdir(parents=True, exist_ok=True)
        os.chmod(self.directory, 0o700)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.cleanup_plaintext()

    def seal_directory(self) -> Dict[str, Path]:
        """Encrypt all unencrypted files in spool directory and securely wipe cleartext."""
        for root, _, files in os.walk(self.directory):
            for fname in files:
                if fname.endswith(".enc"):
                    continue
                full_path = Path(root) / fname
                enc_path, _ = encrypt_file(full_path, full_path.with_suffix(full_path.suffix + ".enc"), key=self.key)
                self.sealed_files[str(full_path.relative_to(self.directory))] = enc_path
                secure_wipe(full_path)
        return self.sealed_files

    def unseal_file(self, rel_path: str, target_path: Optional[Path] = None) -> Path:
        """Decrypt a sealed spool file back to a secure 0600 path."""
        enc_path = self.sealed_files.get(rel_path) or (self.directory / rel_path).with_suffix(Path(rel_path).suffix + ".enc")
        if not enc_path.is_file():
            raise FileNotFoundError(f"Sealed file not found: {enc_path}")
        out = target_path or (self.directory / rel_path)
        return decrypt_file(enc_path, out, key=self.key)

    def read_sealed_bytes(self, rel_path: str) -> bytes:
        """Read decrypted bytes directly from sealed file in memory without writing to disk."""
        enc_path = self.sealed_files.get(rel_path) or (self.directory / rel_path).with_suffix(Path(rel_path).suffix + ".enc")
        encrypted_bytes = enc_path.read_bytes()
        return decrypt_bytes(encrypted_bytes, self.key)

    def cleanup_plaintext(self) -> None:
        """Securely wipe all unencrypted files in the spool."""
        if not self.directory.is_dir():
            return
        for root, _, files in os.walk(self.directory):
            for fname in files:
                if not fname.endswith(".enc"):
                    p = Path(root) / fname
                    try:
                        secure_wipe(p)
                    except OSError:
                        pass


def enforce_file_permissions(path: Union[str, Path], mode: int = 0o600) -> None:
    """Enforce strict permissions on a file (default 0600, Item 223)."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Path does not exist: {p}")
    if not p.is_file():
        raise ValueError(f"Path is not a regular file: {p}")
    os.chmod(p, mode)
    actual = stat.S_IMODE(p.stat().st_mode)
    if actual != mode:
        raise PermissionError(f"Failed enforcing mode {oct(mode)} on {p}, current: {oct(actual)}")


def enforce_directory_permissions(path: Union[str, Path], mode: int = 0o700) -> None:
    """Enforce strict permissions on a directory (default 0700, Item 223)."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Path does not exist: {p}")
    if not p.is_dir():
        raise ValueError(f"Path is not a directory: {p}")
    os.chmod(p, mode)
    actual = stat.S_IMODE(p.stat().st_mode)
    if actual != mode:
        raise PermissionError(f"Failed enforcing mode {oct(mode)} on {p}, current: {oct(actual)}")


def audit_permissions(root_path: Union[str, Path]) -> List[Tuple[Path, int, str]]:
    """Audit permissions recursively: files must be 0600 or 0400, directories 0700."""
    root = Path(root_path)
    violations: List[Tuple[Path, int, str]] = []
    if not root.exists():
        return violations

    for current, dirs, files in os.walk(root):
        cpath = Path(current)
        cmode = stat.S_IMODE(cpath.stat().st_mode)
        if cmode & 0o077 != 0 or (cmode & 0o700 != 0o700 and cmode & 0o500 != 0o500):
            violations.append((cpath, cmode, "Directory permission too open; expected 0700"))

        for f in files:
            fpath = cpath / f
            try:
                fmode = stat.S_IMODE(fpath.stat().st_mode)
                if fmode & 0o077 != 0:
                    violations.append((fpath, fmode, "File accessible by group/others; expected 0600"))
            except OSError:
                pass
    return violations


def harden_directory_tree(root_path: Union[str, Path]) -> int:
    """Recursively enforce 0700 on directories and 0600 on files."""
    root = Path(root_path)
    count = 0
    if not root.exists():
        return 0

    if root.is_dir():
        os.chmod(root, 0o700)
        count += 1

    for current, dirs, files in os.walk(root):
        cpath = Path(current)
        for d in dirs:
            dp = cpath / d
            os.chmod(dp, 0o700)
            count += 1
        for f in files:
            fp = cpath / f
            os.chmod(fp, 0o600)
            count += 1
    return count


def redact_sensitive_tokens(value: Any, mask: str = "***") -> Any:
    """Deep redaction of tokens, bearer headers, passwords, and API keys (Item 223)."""
    if value is None:
        return None

    if isinstance(value, str):
        text = value
        text = URI_CREDENTIAL_PATTERN.sub(lambda m: f"{m.group('prefix')}{m.group('user')}:{mask}@", text)
        text = BEARER_PATTERN.sub(f"Bearer {mask}", text)
        text = JWT_PATTERN.sub(f"eyJ{mask}.eyJ{mask}.{mask}", text)
        text = API_KEY_PATTERN.sub(mask, text)
        text = AWS_KEY_PATTERN.sub(mask, text)
        from . import redact
        return redact.shapes(text, mask=mask)

    if isinstance(value, dict):
        out_dict: Dict[str, Any] = {}
        for k, v in value.items():
            k_str = str(k)
            if isinstance(v, (dict, list, tuple, set)):
                out_dict[k] = redact_sensitive_tokens(v, mask=mask)
            elif SECRET_KEY_PATTERN.search(k_str):
                out_dict[k] = mask if v is not None else None
            else:
                out_dict[k] = redact_sensitive_tokens(v, mask=mask)
        return out_dict

    if isinstance(value, (list, tuple, set)):
        items = [redact_sensitive_tokens(x, mask=mask) for x in value]
        if isinstance(value, tuple):
            return tuple(items)
        if isinstance(value, set):
            return set(items)
        return items

    return value


def secure_wipe(path: Union[str, Path], passes: int = 1) -> None:
    """Securely overwrite file bytes before deletion to prevent recovery."""
    p = Path(path)
    if not p.is_file():
        return
    try:
        length = p.stat().st_size
        if length > 0:
            with open(p, "ba+", buffering=0) as f:
                for _ in range(max(1, passes)):
                    f.seek(0)
                    f.write(os.urandom(length))
                    f.flush()
                    os.fsync(f.fileno())
                f.seek(0)
                f.write(b"\x00" * length)
                f.flush()
                os.fsync(f.fileno())
    except (OSError, PermissionError):
        pass
    try:
        p.unlink(missing_ok=True)
    except OSError:
        pass


def create_secure_temp_file(
    dir: Optional[Union[str, Path]] = None,
    prefix: str = "migkit-sec-",
    suffix: str = ".tmp",
) -> Path:
    """Create a temporary file securely with 0600 mode."""
    target_dir = str(dir) if dir else tempfile.gettempdir()
    os.makedirs(target_dir, exist_ok=True)
    fd, path = tempfile.mkstemp(dir=target_dir, prefix=prefix, suffix=suffix)
    os.close(fd)
    p = Path(path)
    os.chmod(p, 0o600)
    return p


def create_secure_temp_dir(
    dir: Optional[Union[str, Path]] = None,
    prefix: str = "migkit-sec-",
) -> Path:
    """Create a temporary directory securely with 0700 mode."""
    target_dir = str(dir) if dir else tempfile.gettempdir()
    os.makedirs(target_dir, exist_ok=True)
    path = tempfile.mkdtemp(dir=target_dir, prefix=prefix)
    p = Path(path)
    os.chmod(p, 0o700)
    return p


class SecureTempFile:
    """Context manager for temporary file with automatic secure zero-wipe."""

    def __init__(self, dir: Optional[Union[str, Path]] = None, prefix: str = "migkit-sec-"):
        self.path = create_secure_temp_file(dir=dir, prefix=prefix)

    def __enter__(self) -> Path:
        return self.path

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        secure_wipe(self.path)


class SecureTempDir:
    """Context manager for temporary directory with automatic wipe and removal."""

    def __init__(self, dir: Optional[Union[str, Path]] = None, prefix: str = "migkit-sec-"):
        self.path = create_secure_temp_dir(dir=dir, prefix=prefix)

    def __enter__(self) -> Path:
        return self.path

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.path.is_dir():
            for root, _, files in os.walk(self.path):
                for f in files:
                    secure_wipe(Path(root) / f)
            shutil.rmtree(self.path, ignore_errors=True)


class FormatPreservingCipher:
    """Format-preserving encryption (FPE) using Feistel network over arbitrary alphabets (Item 287)."""

    DIGITS = "0123456789"
    ALPHANUMERIC = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    LOWER_ALPHANUM = "0123456789abcdefghijklmnopqrstuvwxyz"

    def __init__(self, alphabet: str = DIGITS, rounds: int = 10):
        if len(alphabet) < 2:
            raise ValueError("Alphabet must have at least 2 characters")
        if len(set(alphabet)) != len(alphabet):
            raise ValueError("Alphabet contains duplicate characters")
        self.alphabet = alphabet
        self.radix = len(alphabet)
        self.char_to_val = {c: i for i, c in enumerate(alphabet)}
        self.val_to_char = {i: c for i, c in enumerate(alphabet)}
        self.rounds = max(4, rounds)

    def _str_to_int(self, s: str) -> int:
        num = 0
        for ch in s:
            num = num * self.radix + self.char_to_val[ch]
        return num

    def _int_to_str(self, num: int, length: int) -> str:
        chars = []
        for _ in range(length):
            num, rem = divmod(num, self.radix)
            chars.append(self.val_to_char[rem])
        return "".join(reversed(chars))

    def _round_func(self, round_idx: int, val: int, val_len: int, key: bytes, tweak: bytes, mod: int) -> int:
        msg = struct.pack(">II", round_idx, val_len) + tweak + str(val).encode("utf-8")
        h = hmac.new(key, msg, hashlib.sha256).digest()
        num = int.from_bytes(h, "big")
        return num % mod

    def _encrypt_clean(self, text: str, key: bytes, tweak: bytes) -> str:
        length = len(text)
        if length == 0:
            return ""
        if length == 1:
            val = self.char_to_val[text]
            shift = int.from_bytes(hmac.new(key, tweak + b"\x01", hashlib.sha256).digest(), "big")
            return self.val_to_char[(val + shift) % self.radix]

        u = length // 2
        v = length - u
        a_str = text[:u]
        b_str = text[u:]
        mod_a = self.radix ** u
        mod_b = self.radix ** v
        a = self._str_to_int(a_str)
        b = self._str_to_int(b_str)

        for i in range(self.rounds):
            if i % 2 == 0:
                f = self._round_func(i, b, v, key, tweak, mod_a)
                a = (a + f) % mod_a
            else:
                f = self._round_func(i, a, u, key, tweak, mod_b)
                b = (b + f) % mod_b

        return self._int_to_str(a, u) + self._int_to_str(b, v)

    def _decrypt_clean(self, text: str, key: bytes, tweak: bytes) -> str:
        length = len(text)
        if length == 0:
            return ""
        if length == 1:
            val = self.char_to_val[text]
            shift = int.from_bytes(hmac.new(key, tweak + b"\x01", hashlib.sha256).digest(), "big")
            return self.val_to_char[(val - shift) % self.radix]

        u = length // 2
        v = length - u
        mod_a = self.radix ** u
        mod_b = self.radix ** v
        a = self._str_to_int(text[:u])
        b = self._str_to_int(text[u:])

        for i in reversed(range(self.rounds)):
            if i % 2 == 0:
                f = self._round_func(i, b, v, key, tweak, mod_a)
                a = (a - f) % mod_a
            else:
                f = self._round_func(i, a, u, key, tweak, mod_b)
                b = (b - f) % mod_b

        return self._int_to_str(a, u) + self._int_to_str(b, v)

    def encrypt(
        self,
        plaintext: str,
        key: bytes,
        tweak: Optional[bytes] = None,
        preserve_prefix: int = 0,
        preserve_suffix: int = 0,
    ) -> str:
        """Encrypt preserving length, alphabet, and non-alphabet formatting."""
        if plaintext is None:
            return None
        text = str(plaintext)
        tweak_bytes = tweak or b"migkit-fpe-default-tweak"

        mask: List[Tuple[int, str]] = []
        clean_chars: List[str] = []
        for i, ch in enumerate(text):
            if ch in self.char_to_val:
                clean_chars.append(ch)
            else:
                mask.append((i, ch))

        clean_len = len(clean_chars)
        prefix_len = min(preserve_prefix, clean_len)
        suffix_len = min(preserve_suffix, clean_len - prefix_len)

        prefix = "".join(clean_chars[:prefix_len])
        suffix = "".join(clean_chars[clean_len - suffix_len:]) if suffix_len > 0 else ""
        middle = "".join(clean_chars[prefix_len:clean_len - suffix_len if suffix_len > 0 else clean_len])

        enc_middle = self._encrypt_clean(middle, key, tweak_bytes)
        combined = list(prefix + enc_middle + suffix)

        for idx, ch in mask:
            combined.insert(idx, ch)
        return "".join(combined)

    def decrypt(
        self,
        ciphertext: str,
        key: bytes,
        tweak: Optional[bytes] = None,
        preserve_prefix: int = 0,
        preserve_suffix: int = 0,
    ) -> str:
        """Decrypt format-preserved ciphertext back to original plaintext."""
        if ciphertext is None:
            return None
        text = str(ciphertext)
        tweak_bytes = tweak or b"migkit-fpe-default-tweak"

        mask: List[Tuple[int, str]] = []
        clean_chars: List[str] = []
        for i, ch in enumerate(text):
            if ch in self.char_to_val:
                clean_chars.append(ch)
            else:
                mask.append((i, ch))

        clean_len = len(clean_chars)
        prefix_len = min(preserve_prefix, clean_len)
        suffix_len = min(preserve_suffix, clean_len - prefix_len)

        prefix = "".join(clean_chars[:prefix_len])
        suffix = "".join(clean_chars[clean_len - suffix_len:]) if suffix_len > 0 else ""
        middle = "".join(clean_chars[prefix_len:clean_len - suffix_len if suffix_len > 0 else clean_len])

        dec_middle = self._decrypt_clean(middle, key, tweak_bytes)
        combined = list(prefix + dec_middle + suffix)

        for idx, ch in mask:
            combined.insert(idx, ch)
        return "".join(combined)


@dataclass
class TokenizeRule:
    mode: str = "fpe"
    alphabet: str = FormatPreservingCipher.DIGITS
    preserve_prefix: int = 0
    preserve_suffix: int = 0
    key: Optional[bytes] = None
    tweak: Optional[bytes] = None


class InFlightColumnTokenizer:
    """Tokenization and FPE helper for sensitive columns during in-flight migration (Item 287)."""

    def __init__(self, master_key: bytes):
        self.master_key = master_key
        self.rules: Dict[str, TokenizeRule] = {}
        self._ciphers: Dict[str, FormatPreservingCipher] = {}

    def _get_cipher(self, alphabet: str) -> FormatPreservingCipher:
        if alphabet not in self._ciphers:
            self._ciphers[alphabet] = FormatPreservingCipher(alphabet=alphabet)
        return self._ciphers[alphabet]

    def register_rule(
        self,
        target: str,
        mode: str = "fpe",
        alphabet: str = FormatPreservingCipher.DIGITS,
        preserve_prefix: int = 0,
        preserve_suffix: int = 0,
        key: Optional[bytes] = None,
        tweak: Optional[bytes] = None,
    ) -> None:
        """Register tokenization rule for column or table.column."""
        k = key or hmac.new(self.master_key, f"col-key|{target.lower()}".encode("utf-8"), hashlib.sha256).digest()
        tw = tweak or f"col-tweak|{target.lower()}".encode("utf-8")
        self.rules[target.lower()] = TokenizeRule(
            mode=mode,
            alphabet=alphabet,
            preserve_prefix=preserve_prefix,
            preserve_suffix=preserve_suffix,
            key=k,
            tweak=tw,
        )

    def _resolve_rule(self, table: str, column: str) -> Optional[TokenizeRule]:
        col_lower = column.lower()
        full = f"{table.lower()}.{col_lower}"
        leaf = f"{table.rpartition('.')[2].lower()}.{col_lower}"
        return self.rules.get(full) or self.rules.get(leaf) or self.rules.get(col_lower)

    def tokenize_value(self, table: str, column: str, value: Any) -> Any:
        """Tokenize or encrypt value in-flight based on registered column rule."""
        if value is None:
            return None
        rule = self._resolve_rule(table, column)
        if not rule:
            return value

        val_str = str(value)
        if rule.mode == "fpe":
            cipher = self._get_cipher(rule.alphabet)
            return cipher.encrypt(
                val_str,
                rule.key,
                tweak=rule.tweak,
                preserve_prefix=rule.preserve_prefix,
                preserve_suffix=rule.preserve_suffix,
            )
        if rule.mode == "hash_token":
            h = hmac.new(rule.key, val_str.encode("utf-8"), hashlib.sha256).hexdigest()
            return f"tok_{h[:16]}"
        return value

    def detokenize_value(self, table: str, column: str, value: Any) -> Any:
        """Reverse format-preserving encryption for target column."""
        if value is None:
            return None
        rule = self._resolve_rule(table, column)
        if not rule or rule.mode != "fpe":
            return value

        cipher = self._get_cipher(rule.alphabet)
        return cipher.decrypt(
            str(value),
            rule.key,
            tweak=rule.tweak,
            preserve_prefix=rule.preserve_prefix,
            preserve_suffix=rule.preserve_suffix,
        )

    def tokenize_row(self, table: str, row: Dict[str, Any]) -> Dict[str, Any]:
        """Transform all matching sensitive columns in a row dictionary."""
        out = {}
        for col, val in row.items():
            out[col] = self.tokenize_value(table, col, val)
        return out


def create_immutable_checkpoint_envelope(
    seq: int,
    head: str,
    signing_key: bytes,
    hop_name: str,
    entries_count: int = 0,
    retention_days: int = 365,
    mode: str = "GOVERNANCE",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create signed audit checkpoint envelope for object-locked/immutable storage (Item 301)."""
    now = datetime.now(timezone.utc)
    retain_until = (now + timedelta(days=retention_days)).isoformat()
    core_payload = {
        "format": CHECKPOINT_ENVELOPE_FORMAT,
        "seq": int(seq),
        "head": str(head),
        "hop": str(hop_name),
        "entries_count": int(entries_count),
        "timestamp": now.isoformat(),
        "retention": {
            "mode": mode.upper(),
            "days": int(retention_days),
            "retain_until": retain_until,
        },
        "metadata": metadata or {},
    }
    payload_bytes = canonical_json_bytes(core_payload)
    payload_digest = hashlib.sha256(payload_bytes).hexdigest()
    signature = hmac.new(signing_key, b"migkit-anchor-checkpoint-v1|" + payload_bytes, hashlib.sha256).hexdigest()

    return {
        "envelope_version": 1,
        "format": CHECKPOINT_ENVELOPE_FORMAT,
        "payload_digest": payload_digest,
        "signature_algorithm": "HMAC-SHA256",
        "signature": signature,
        "immutable": True,
        "checkpoint": core_payload,
    }


def verify_immutable_checkpoint_envelope(envelope: Dict[str, Any], verification_key: bytes) -> Tuple[bool, str]:
    """Verify cryptographic signature and integrity of an immutable audit checkpoint envelope."""
    if not isinstance(envelope, dict):
        return False, "Envelope must be a dict"
    if envelope.get("format") != CHECKPOINT_ENVELOPE_FORMAT:
        return False, f"Invalid envelope format: {envelope.get('format')}"

    core = envelope.get("checkpoint")
    if not isinstance(core, dict):
        return False, "Missing core checkpoint object"

    claimed_digest = envelope.get("payload_digest")
    claimed_sig = envelope.get("signature")

    actual_bytes = canonical_json_bytes(core)
    actual_digest = hashlib.sha256(actual_bytes).hexdigest()
    if not hmac.compare_digest(actual_digest, str(claimed_digest)):
        return False, "Payload digest mismatch"

    expected_sig = hmac.new(verification_key, b"migkit-anchor-checkpoint-v1|" + actual_bytes, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_sig, str(claimed_sig)):
        return False, "Signature mismatch: checkpoint invalid"

    retention = core.get("retention", {})
    if not retention.get("retain_until"):
        return False, "Retention timestamp missing in immutable envelope"

    return True, "OK"


class ImmutableStorageVault:
    """Manages append-only, tamper-evident checkpoint envelopes in object-locked or WORM vaults (Item 301)."""

    def __init__(self, vault_dir: Union[str, Path]):
        self.vault_dir = Path(vault_dir)
        self.vault_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self.vault_dir, 0o700)

    def _object_key(self, envelope: Dict[str, Any]) -> str:
        core = envelope["checkpoint"]
        return f"{core['hop']}/audit/{core['seq']}-{core['head']}.json"

    def store_checkpoint(self, envelope: Dict[str, Any], overwrite_guard: bool = True) -> Path:
        """Store checkpoint envelope in WORM storage, setting read-only mode and preventing overwrite."""
        obj_key = self._object_key(envelope)
        target = self.vault_dir / obj_key
        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists():
            if overwrite_guard:
                raise PermissionError(f"Object {obj_key} already anchored in immutable storage")
            return target

        data = canonical_json_bytes(envelope)
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
        with os.fdopen(fd, "wb") as f:
            f.write(data)

        # Enforce read-only 0400
        os.chmod(target, 0o400)
        return target

    def read_checkpoint(self, hop: str, seq: int, head: str) -> Dict[str, Any]:
        """Read anchored checkpoint from immutable storage."""
        target = self.vault_dir / hop / "audit" / f"{seq}-{head}.json"
        if not target.is_file():
            raise FileNotFoundError(f"Anchored checkpoint not found: {target}")
        return json.loads(target.read_text(encoding="utf-8"))

    def audit_vault_integrity(self, verification_key: bytes) -> List[Tuple[str, bool, str]]:
        """Audit all envelopes in vault to verify cryptographic integrity and retention policies."""
        results = []
        for root, _, files in os.walk(self.vault_dir):
            for fname in files:
                if not fname.endswith(".json"):
                    continue
                p = Path(root) / fname
                try:
                    envelope = json.loads(p.read_text(encoding="utf-8"))
                    ok, reason = verify_immutable_checkpoint_envelope(envelope, verification_key)
                    mode = stat.S_IMODE(p.stat().st_mode)
                    if mode & 0o222 != 0:
                        ok = False
                        reason = f"File is writable ({oct(mode)}), violates WORM immutability"
                    results.append((str(p.relative_to(self.vault_dir)), ok, reason))
                except Exception as e:
                    results.append((str(p.relative_to(self.vault_dir)), False, str(e)))
        return results
