"""Pluggable state backend, terraform-style.

A restore point is a directory of files (sequence values, complete row undo,
schema snapshot) plus a meta.json describing what/when. It is written locally
then committed to a backend so it survives losing this machine:

    state:
      backend: local            # default, kept under reports + ~/.migkit-state
    state:
      backend: s3
      bucket: my-migkit-state
      prefix: migkit/           # optional
      endpoint_url: ...         # optional, for S3-compatible stores (minio)

Config lives per-hop (hop.options["state"]) or falls back to a top-level
`state:` block in hops.yaml.
"""
import io
import json
import os
import shutil
import tarfile
import tempfile
import time
from pathlib import Path

from . import config as _config


def _cfg(hop):
    cfg = dict(hop.options.get("state") or {})
    cfg.setdefault("backend", "local")
    return cfg


def get_store(hop):
    cfg = _cfg(hop)
    backend = cfg.get("backend", "local")
    if backend == "local":
        return LocalStore(hop, cfg)
    if backend == "s3":
        return S3Store(hop, cfg)
    raise SystemExit(f"unknown state backend '{backend}', use local or s3")


class _Point:
    """A staging directory the caller writes undo files into, then commit()s."""

    def __init__(self, store, db, ts, tag):
        self.store = store
        self.db = db
        self.ts = ts + (f"-{tag}" if tag else "")
        self.tag = tag
        self.dir = Path(tempfile.mkdtemp(prefix="migkit-state-"))
        self.meta = {"ts": self.ts, "tag": tag, "created": None,
                     "op": "", "tables": [], "detail": ""}

    def path(self, name):
        return self.dir / name

    def set_meta(self, **kw):
        self.meta.update(kw)

    def commit(self, stamp):
        self.meta["created"] = stamp
        (self.dir / "meta.json").write_text(json.dumps(self.meta, indent=1,
                                                       default=str))
        self.store._commit(self.db, self.ts, self.dir)
        return self.ts


def _tar(src_dir):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for f in sorted(Path(src_dir).iterdir()):
            tf.add(f, arcname=f.name)
    return _seal(buf.getvalue())


def _untar(data, dst_dir):
    with tarfile.open(fileobj=io.BytesIO(_unseal(data)), mode="r:gz") as tf:
        tf.extractall(dst_dir, filter="data")


#: a restore point sealed with `MIGKIT_STATE_KEY` starts with this
SEALED = b"MKS1"


def _key(salt):
    """The key for one restore point, from the passphrase and the point's
    own salt: the same passphrase never gives two points the same key."""
    import base64

    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    phrase = os.environ.get("MIGKIT_STATE_KEY", "")
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                     iterations=390_000)
    return base64.urlsafe_b64encode(kdf.derive(phrase.encode()))


#: (salt, key) this process seals run state with: the key derivation is
#: deliberately slow, and a checkpoint is saved after every chunk
_RUN_KEY = {}


def _seal(data, often=False):
    """A restore point holds whole rows - the undo of a repair - and it is
    kept where the local machine may not be the only reader: a mirror
    directory, a bucket (backlog 41). With `MIGKIT_STATE_KEY` set it is
    encrypted before it leaves the working directory; without it, as
    before. `often` reuses one salt for the process's run state, which is
    written after every chunk; a restore point gets a salt of its own."""
    phrase = os.environ.get("MIGKIT_STATE_KEY")
    if not phrase:
        return data
    from cryptography.fernet import Fernet
    if often:
        if _RUN_KEY.get("phrase") != phrase:
            salt = os.urandom(16)
            _RUN_KEY.update(phrase=phrase, salt=salt, key=_key(salt))
        salt, key = _RUN_KEY["salt"], _RUN_KEY["key"]
    else:
        salt = os.urandom(16)
        key = _key(salt)
    return SEALED + salt + Fernet(key).encrypt(data)


def _unseal(data):
    if not data.startswith(SEALED):
        return data
    if not os.environ.get("MIGKIT_STATE_KEY"):
        raise SystemExit("this restore point is encrypted: set"
                         " MIGKIT_STATE_KEY to the passphrase it was written"
                         " with")
    from cryptography.fernet import Fernet, InvalidToken
    salt, body = data[4:20], data[20:]
    try:
        return Fernet(_key(salt)).decrypt(body)
    except InvalidToken:
        raise SystemExit("MIGKIT_STATE_KEY is not the passphrase this"
                         " restore point was written with")


class LocalStore:
    kind = "local"

    def __init__(self, hop, cfg):
        self.hop = hop
        self.mirror = Path(cfg.get("mirror", str(
            Path.home() / ".migkit-state" / hop.name)))

    def _root(self, db):
        # read when asked: a copy taken at import is where the reports were
        # then, which a test that moves them - or a caller that sets them
        # after importing - does not reach
        d = _config.REPORTS / self.hop.name / db / "state"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def new_point(self, db, tag=""):
        return _Point(self, db, time.strftime("%Y%m%d-%H%M%S"), tag)

    def _commit(self, db, ts, src_dir):
        dst = self._root(db) / ts
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src_dir, dst)
        self.mirror.mkdir(parents=True, exist_ok=True)
        (self.mirror / f"{db}-{ts}.tar.gz").write_bytes(_tar(src_dir))
        shutil.rmtree(src_dir, ignore_errors=True)

    def list(self, db):
        root = self._root(db)
        out = []
        for d in sorted(root.glob("*")):
            if not d.is_dir():
                continue
            meta = {}
            mf = d / "meta.json"
            if mf.exists():
                try:
                    meta = json.loads(mf.read_text())
                except ValueError:
                    pass
            meta.setdefault("ts", d.name)
            out.append(meta)
        return out

    def fetch(self, db, ts):
        d = self._root(db) / ts
        if d.exists():
            return d
        tar = self.mirror / f"{db}-{ts}.tar.gz"
        if tar.exists():
            d.mkdir(parents=True, exist_ok=True)
            _untar(tar.read_bytes(), d)
            return d
        return None


def _bucket(cfg):
    """(client, bucket, prefix) for the s3 backend."""
    try:
        import boto3
    except ImportError:
        raise SystemExit("pip install boto3 for the s3 state backend")
    if not cfg.get("bucket"):
        raise SystemExit("s3 state backend needs 'bucket' in hops.yaml")
    kw = {}
    if cfg.get("endpoint_url"):
        kw["endpoint_url"] = cfg["endpoint_url"]
    if cfg.get("region"):
        kw["region_name"] = cfg["region"]
    return (boto3.client("s3", **kw), cfg["bucket"],
            cfg.get("prefix", "migkit/").rstrip("/") + "/")


class S3Store:
    kind = "s3"

    def __init__(self, hop, cfg):
        self.hop = hop
        self.s3, self.bucket, self.prefix = _bucket(cfg)
        # local staging mirror so an interrupted upload keeps the files
        self.stage = _config.REPORTS / self.hop.name

    def _key(self, db, ts):
        return f"{self.prefix}{self.hop.name}/{db}/{ts}.tar.gz"

    def new_point(self, db, tag=""):
        return _Point(self, db, time.strftime("%Y%m%d-%H%M%S"), tag)

    def _commit(self, db, ts, src_dir):
        # keep a local copy too (fast reads) then push the tarball to s3
        local = self.stage / db / "state" / ts
        local.mkdir(parents=True, exist_ok=True)
        for f in Path(src_dir).iterdir():
            shutil.copy2(f, local / f.name)
        self.s3.put_object(Bucket=self.bucket, Key=self._key(db, ts),
                           Body=_tar(src_dir))
        shutil.rmtree(src_dir, ignore_errors=True)

    def list(self, db):
        prefix = f"{self.prefix}{self.hop.name}/{db}/"
        out = []
        token = None
        while True:
            kw = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kw)
            for obj in resp.get("Contents", []):
                ts = obj["Key"].rsplit("/", 1)[-1].removesuffix(".tar.gz")
                meta = {"ts": ts, "created": str(obj["LastModified"]),
                        "size": obj["Size"], "backend": "s3"}
                out.append(meta)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return sorted(out, key=lambda m: m["ts"])

    def fetch(self, db, ts):
        d = self.stage / db / "state" / ts
        if (d / "meta.json").exists():
            return d
        d.mkdir(parents=True, exist_ok=True)
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=self._key(db, ts))
        except Exception:
            return None
        _untar(obj["Body"].read(), d)
        return d


# ---- a run's own state, where another machine can take it over ------------

def run_state(hop):
    """Where a run keeps its lease, checkpoints and change position besides
    this machine's report directory: the bucket of the s3 backend, or None
    for the local one (backlog 30)."""
    cfg = _cfg(hop)
    return S3RunState(hop, cfg) if cfg.get("backend") == "s3" else None


class Taken(Exception):
    """A conditional write lost to another writer: read again, decide
    again."""


class S3RunState:
    """The run's state in the bucket, under `<prefix><hop>/run/`, beside
    the restore points. The lease is written only if nobody wrote it since
    it was read - the bucket's own conditional write, so two machines
    deciding at once cannot both hold it. Checkpoints and positions are
    sealed as a restore point is, where `MIGKIT_STATE_KEY` is set: a
    checkpoint names the last key copied, which is data."""

    def __init__(self, hop, cfg):
        self.hop = hop
        self.s3, self.bucket, self.prefix = _bucket(cfg)

    def key(self, path):
        root = Path(self.hop.report_dir())
        try:
            rel = Path(path).relative_to(root).as_posix()
        except ValueError:
            rel = Path(path).name
        return f"{self.prefix}{self.hop.name}/run/{rel}"

    def _missing(self, e):
        return getattr(e, "response", {}).get("Error", {}).get("Code") in (
            "NoSuchKey", "404", "NotFound")

    def get(self, path):
        try:
            got = self.s3.get_object(Bucket=self.bucket, Key=self.key(path))
        except Exception as e:  # noqa: BLE001 - only absence is an answer
            if self._missing(e):
                return None
            raise
        return _unseal(got["Body"].read()).decode()

    def put(self, path, text):
        self.s3.put_object(Bucket=self.bucket, Key=self.key(path),
                           Body=_seal(text.encode(), often=True))

    def delete(self, path):
        self.s3.delete_object(Bucket=self.bucket, Key=self.key(path))

    def read_record(self, path, sealed=False):
        """(record, version) of a JSON record, (None, None) if absent."""
        try:
            got = self.s3.get_object(Bucket=self.bucket, Key=self.key(path))
        except Exception as e:  # noqa: BLE001 - only absence is an answer
            if self._missing(e):
                return None, None
            raise
        body = got["Body"].read()
        try:
            return (json.loads(_unseal(body) if sealed else body),
                    got["ETag"])
        except ValueError:
            return None, got["ETag"]

    def write_record(self, path, record, version, sealed=False):
        """Write only over `version` (None: only if nothing is there);
        `Taken` when another writer got there first."""
        kw = {"IfMatch": version} if version else {"IfNoneMatch": "*"}
        body = json.dumps(record).encode()
        try:
            self.s3.put_object(Bucket=self.bucket, Key=self.key(path),
                               Body=_seal(body, often=True) if sealed
                               else body, **kw)
        except Exception as e:  # noqa: BLE001 - a lost race is not an error
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            if code in ("PreconditionFailed", "ConditionalRequestConflict",
                        "412", "409"):
                raise Taken() from None
            raise


class Mirror:
    """Files a long run rewrites in place - a change position - pushed to
    the run state when they change, and fetched from it when this machine
    has none. A position a few seconds old is safe to resume from: the
    tail applies by key, so what it replays converges."""

    def __init__(self, remote, paths, every=2.0):
        import threading
        self.remote, self.paths, self.every = remote, list(paths), every
        self.seen = {}
        self._stop = threading.Event()
        self._thread = None

    def fetch(self):
        for p in self.paths:
            p = Path(p)
            if not p.exists():
                text = self.remote.get(p)
                if text is not None:
                    p.parent.mkdir(parents=True, exist_ok=True)
                    p.write_text(text)
            if p.exists():
                self.seen[p] = p.stat().st_mtime_ns
        return self

    def push(self, force=False):
        for p in self.paths:
            p = Path(p)
            try:
                stamp = p.stat().st_mtime_ns
            except OSError:
                continue
            if force or self.seen.get(p) != stamp:
                self.remote.put(p, p.read_text())
                self.seen[p] = stamp

    def start(self):
        import threading

        def loop():
            while not self._stop.wait(self.every):
                try:
                    self.push()
                except Exception:  # noqa: BLE001 - the next turn retries
                    pass
        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        self.push()
