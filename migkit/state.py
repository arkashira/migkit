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


#: the state backends there are
BACKENDS = ("local", "s3")


def _cfg(hop):
    cfg = dict((hop.options or {}).get("state") or {})
    cfg["backend"] = backend(hop)
    return cfg


def backend(hop):
    """The hop's state backend, read one way for everything that asks - the
    restore points and the lease and positions another machine reads. It
    was read two ways: `backend: S3` refused where a store was needed and
    kept the lease on this machine where it was not, so two machines could
    both write (audit K10)."""
    got = (hop.options or {}).get("state") or {}
    name = str(got.get("backend") or "local").strip().lower() \
        if isinstance(got, dict) else "local"
    if name not in BACKENDS:
        raise SystemExit(f"state: backend {name!r} is not one of"
                         f" {', '.join(BACKENDS)}")
    return name


def get_store(hop):
    cfg = _cfg(hop)
    if cfg["backend"] == "s3":
        return S3Store(hop, cfg)
    return LocalStore(hop, cfg)


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
        # a name of its own first, the one its meta says
        self.ts = self.store._name(self.db, self.ts)
        self.meta.update(created=stamp, ts=self.ts)
        (self.dir / "meta.json").write_text(json.dumps(self.meta, indent=1,
                                                       default=str))
        self.store._commit(self.db, self.ts, self.dir)
        return self.ts


def _tar(src_dir, hop=None):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for f in sorted(Path(src_dir).iterdir()):
            tf.add(f, arcname=f.name)
    return sealed(buf.getvalue(), hop)


def _untar(data, dst_dir):
    with tarfile.open(fileobj=io.BytesIO(_unseal(data)), mode="r:gz") as tf:
        tf.extractall(dst_dir, filter="data")


def sealed(data, hop=None):
    """A restore point as it leaves the working directory - a mirror
    directory, a bucket - sealed by the one policy every file holding
    values is (audit ST15): to the hop's `at_rest` recipients where it
    names them (`evidence`), else with `MIGKIT_STATE_KEY` as before, else
    as it is. Two schemes each covered half: a hop that kept its evidence
    encrypted kept its restore points - whole rows - in the clear."""
    from . import evidence
    who = evidence.recipients_of(hop) if hop is not None else []
    if who:
        return evidence._seal(data, who)
    return _seal(data)


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
    from . import evidence
    if data.startswith(evidence.AGE):
        return evidence._open(data)
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


#: what a point is - its name, when, the operation and its tables - and
#: no value of any row: kept unsealed, so points are listed without a key
META = "meta.json"


def _put_local(root, ts, src_dir, hop):
    """A point's local copy put in place whole as `root/ts` (audit ST22):
    written beside it, then renamed - and each file sealed to the hop's
    `at_rest` recipients where it names them (ST15), `META` apart: the
    local copy held the rows of an undo in the clear while the copy that
    left the machine was sealed."""
    from . import durable, evidence
    who = evidence.recipients_of(hop) if hop is not None else []
    src = Path(src_dir)
    part = root / f".{ts}.partial"
    shutil.rmtree(part, ignore_errors=True)
    durable.private_dir(part)
    for f in sorted(src.rglob("*")):
        rel = f.relative_to(src)
        if f.is_dir():
            durable.private_dir(part / rel)
            continue
        data = f.read_bytes()
        if who and str(rel) != META:
            data = evidence._seal(data, who)
        durable.write_atomic(part / rel, data)
    os.rename(part, root / ts)
    durable.sync_dir(root)


def _opened(d):
    """The point at `d` to read: `d` itself where none of its files is
    sealed, else its files opened into a directory of this run's own,
    only this user's and removed when the run ends (ST15)."""
    import atexit

    from . import durable, evidence
    d = Path(d)
    files = [f for f in sorted(d.rglob("*")) if f.is_file()]

    def sealed_one(f):
        with open(f, "rb") as h:
            return h.read(len(evidence.AGE)) == evidence.AGE
    if not any(sealed_one(f) for f in files if f.name != META):
        return d
    out = Path(tempfile.mkdtemp(prefix="migkit-point-"))
    atexit.register(shutil.rmtree, out, True)
    for f in files:
        rel = f.relative_to(d)
        raw = f.read_bytes()
        if raw.startswith(evidence.AGE):
            raw = evidence._open(raw)
        durable.private_dir((out / rel).parent)
        durable.write_atomic(out / rel, raw)
    return out


def _from_tar(root, ts, data, hop):
    """A point fetched from where it was kept away from this machine, put
    in place here as `_put_local` puts one, and opened to read."""
    got = Path(tempfile.mkdtemp(prefix="migkit-point-"))
    try:
        _untar(data, got)
        _put_local(root, ts, got, hop)
    finally:
        shutil.rmtree(got, ignore_errors=True)
    return _opened(root / ts)


def _unique(root, ts):
    """`ts`, or `ts-2`, `ts-3`... where a point of that name is kept
    already: two points in one second are two."""
    got, n = ts, 1
    while (root / got).exists():
        n += 1
        got = f"{ts}-{n}"
    return got


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
        from . import durable
        return durable.private_dir(_config.REPORTS / self.hop.name / db
                                   / "state")

    def new_point(self, db, tag=""):
        return _Point(self, db, time.strftime("%Y%m%d-%H%M%S"), tag)

    def _name(self, db, ts):
        return _unique(self._root(db), ts)

    def _commit(self, db, ts, src_dir):
        """The point put in place whole under a name of its own (audit
        ST22): copied beside where it goes, then renamed there - it was
        the older point removed first and the copy made after, so a crash
        between lost both, and two points in one second were one."""
        from . import durable
        _put_local(self._root(db), ts, src_dir, self.hop)
        durable.private_dir(self.mirror)
        durable.write_atomic(self.mirror / f"{db}-{ts}.tar.gz",
                             _tar(src_dir, self.hop))
        shutil.rmtree(src_dir, ignore_errors=True)
        return ts

    def list(self, db):
        root = self._root(db)
        out = []
        for d in sorted(root.glob("*")):
            if not d.is_dir() or d.name.startswith("."):
                continue
            meta = {}
            mf = d / META
            if mf.exists():
                try:
                    meta = json.loads(mf.read_text())
                except ValueError:
                    pass
            meta.setdefault("ts", d.name)
            out.append(meta)
        return out

    def fetch(self, db, ts):
        """The point's directory to read (`_opened`), from the mirror
        where this machine has no copy of it; None where neither has."""
        root = self._root(db)
        if (root / ts).exists():
            return _opened(root / ts)
        tar = self.mirror / f"{db}-{ts}.tar.gz"
        if tar.exists():
            return _from_tar(root, ts, tar.read_bytes(), self.hop)
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

    def _name(self, db, ts):
        from . import durable
        return _unique(durable.private_dir(self.stage / db / "state"), ts)

    def _commit(self, db, ts, src_dir):
        # keep a local copy too (fast reads) then push the tarball to s3
        from . import durable
        _put_local(durable.private_dir(self.stage / db / "state"), ts,
                   src_dir, self.hop)
        self.s3.put_object(Bucket=self.bucket, Key=self._key(db, ts),
                           Body=_tar(src_dir, self.hop))
        shutil.rmtree(src_dir, ignore_errors=True)
        return ts

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
        from . import durable
        root = durable.private_dir(self.stage / db / "state")
        if (root / ts / META).exists():
            return _opened(root / ts)
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=self._key(db, ts))
        except Exception:
            return None
        # what an earlier version left half fetched
        shutil.rmtree(root / ts, ignore_errors=True)
        return _from_tar(root, ts, obj["Body"].read(), self.hop)


# ---- a run's own state, where another machine can take it over ------------

def run_state(hop):
    """Where a run keeps its lease, checkpoints and change position besides
    this machine's report directory: the bucket of the s3 backend, or None
    for the local one (backlog 30) - unless the hop's machines work
    together (a standby, one table's ranges shared), which then agree
    through the store `shared` chooses."""
    cfg = _cfg(hop)
    if cfg["backend"] == "s3":
        return S3RunState(hop, cfg)
    from . import shared
    return shared.run_state(hop)


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

    kind = "bucket"
    #: keeps what is not text as well (`put_blob`)
    blobs = True

    def __init__(self, hop, cfg):
        self.hop = hop
        self.s3, self.bucket, self.prefix = _bucket(cfg)

    def ident(self):
        """Which store this is, the same on every machine that names it."""
        return f"bucket:{self.bucket}/{self.prefix}{self.hop.name}"

    def put_blob(self, data):
        """`data` kept under its own digest, sealed as the run state is;
        the digest. Written once: a digest already there holds the same
        bytes."""
        import hashlib
        digest = hashlib.sha256(data).hexdigest()
        key = f"{self.prefix}{self.hop.name}/blob/{digest}"
        try:
            self.s3.put_object(Bucket=self.bucket, Key=key,
                               Body=_seal(data, often=True), IfNoneMatch="*")
        except Exception as e:  # noqa: BLE001 - one already there is no error
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            if code not in ("PreconditionFailed", "ConditionalRequestConflict",
                            "412", "409"):
                raise
        return digest

    def get_blob(self, digest):
        got = self.s3.get_object(
            Bucket=self.bucket,
            Key=f"{self.prefix}{self.hop.name}/blob/{digest}")
        return _unseal(got["Body"].read())

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
    """Files a run rewrites - an interlock's record - pushed to the run
    state when they change, and fetched from it when this machine has
    none. The tail's own files go whole, by `TailMirror`."""

    def __init__(self, remote, paths, every=2.0):
        import threading
        self.remote, self.paths, self.every = remote, list(paths), every
        self.seen = {}
        self._stop = threading.Event()
        self._thread = None

    def fetch(self):
        from . import durable
        for p in self.paths:
            p = Path(p)
            if not p.exists():
                text = self.remote.get(p)
                if text is not None:
                    durable.write_atomic(p, text)
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


class TailMirror(Mirror):
    """The tail's files, whole, in the run state (audit ST5): its
    position, its own state (`tailtables.Store`), the kept proof, the
    copy's position - one record (`ha.bundle`), carrying the position's
    generation, written only over one whose generation is no higher; and
    put in place on this machine where the record's generation is higher
    than this machine's own position's.

    Each file was pushed on its own every two seconds while the tail
    wrote it in place, so a remote position could be torn; the changes a
    tail had set aside and its tables' states were not pushed at all, so
    a machine going on with the tail dropped them; and a stale position
    on this machine beat a newer one kept there. A position a few
    seconds old is still safe to go on from: the tail applies by key,
    and where batches are numbered the target's mark says which it
    committed (`twoway.committed_ahead`)."""

    def __init__(self, remote, paths, every=2.0):
        super().__init__(remote, paths, every)
        self.where = Path(self.paths[0]).parent if self.paths else None
        self.seen = None

    def _record_path(self):
        from . import ha
        return self.where / ha.BUNDLE

    def _gen(self):
        from . import tailctl
        env = tailctl.position(self.where / tailctl.TOKEN)
        return None if env is None else int(env.get("gen") or 0)

    def fetch(self):
        from . import ha
        if self.where is None:
            return self
        have, _ = self.remote.read_record(self._record_path(), sealed=True)
        if isinstance(have, dict) and "files" in have:
            mine = self._gen()
            if mine is None or int(have.get("gen") or 0) > mine:
                ha.place(self.where, have.get("files") or {},
                         have.get("blobs") or {}, self.remote)
        else:
            # kept by an earlier version, a file at a time
            for p in self.paths:
                p = Path(p)
                if not p.exists():
                    text = self.remote.get(p)
                    if text is not None:
                        from . import durable
                        durable.write_atomic(p, text)
        self.seen = self._stamps()
        return self

    def _stamps(self):
        from . import ha
        return tuple((n, ha._stamp(self.where / n))
                     for n in ha._text_files(self.where)
                     + ha._blob_files(self.where))

    def push(self, force=False):
        from . import ha
        if self.where is None:
            return
        now = self._stamps()
        if not force and now == self.seen:
            return
        if not ha.settled(self.where):
            return
        record = ha.bundle(self.where, self.remote)
        mine = record["gen"]
        for _ in range(50):
            have, version = self.remote.read_record(self._record_path(),
                                                    sealed=True)
            if isinstance(have, dict) and int(have.get("gen") or 0) > mine:
                # another machine went further: its record stands
                return
            try:
                self.remote.write_record(self._record_path(), record,
                                         version, sealed=True)
                break
            except Taken:
                time.sleep(0.05)
        self.seen = now

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        self.push(force=True)
