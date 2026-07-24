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

from .config import REPORTS


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
    return buf.getvalue()


def _untar(data, dst_dir):
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
        tf.extractall(dst_dir)


class LocalStore:
    kind = "local"

    def __init__(self, hop, cfg):
        self.hop = hop
        self.mirror = Path(cfg.get("mirror", str(
            Path.home() / ".migkit-state" / hop.name)))

    def _root(self, db):
        d = REPORTS / self.hop.name / db / "state"
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


class S3Store:
    kind = "s3"

    def __init__(self, hop, cfg):
        self.hop = hop
        try:
            import boto3
        except ImportError:
            raise SystemExit("pip install boto3 for the s3 state backend")
        self.bucket = cfg.get("bucket") or self._require("bucket")
        self.prefix = cfg.get("prefix", "migkit/").rstrip("/") + "/"
        kw = {}
        if cfg.get("endpoint_url"):
            kw["endpoint_url"] = cfg["endpoint_url"]
        if cfg.get("region"):
            kw["region_name"] = cfg["region"]
        self.s3 = boto3.client("s3", **kw)
        # local staging mirror so an interrupted upload keeps the files
        self.stage = REPORTS / self.hop.name

    def _require(self, name):
        raise SystemExit(f"s3 state backend needs '{name}' in hops.yaml")

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
