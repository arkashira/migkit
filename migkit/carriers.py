"""Where a seed shipped on media is written, and where the far end reads
it back (`ship`; the owner's scope, 2026-10-01).

A carrier is a rung (`decide.Rung`), climbed for what the hop's `ship:`
names and proved by a file written, read back and taken away again before
a part is trusted to it:

* **a disk** attached to this machine, or any mounted directory;
* **the AWS appliance** through the object endpoint it serves on the local
  network (an S3-compatible endpoint, the device's own certificate and
  keys). AWS no longer takes new customers for it; an account that has one
  still ships it;
* **the Azure appliance** through its file share (SMB or NFS, mounted
  here) or its blob endpoint;
* **the Google appliance** through its NFS share, mounted here.

And the far end reads the parts where they are: the carrier attached
there, or the provider's object store it was imported into (an `s3://`,
`gs://` or `https://` blob location - `load_from`), climbed the same way
(`LOAD_RUNGS`), proved by the sealed seed being there.

**The limits, checked before a part is written.** Each carrier's are its
provider's published ones (the pages below, read 2026-10-01), and
where the carrier is a directory here, what its file system says of
itself (`os.statvfs`: the bytes free, the files it can still hold, the
longest name) is taken over the published number wherever it is less -
measured, not assumed. The hop's `capacity:` stands in for a model's
where it says one.

    carrier                 holds                files       largest file
    AWS appliance           210 TB (its storage  -           5 TB, an S3
                            optimized model)                 object
    Azure appliance         80, 120 or 525 TiB   500 M,      14 TiB through
                                                 500 M, 1 G  the block blob
                                                             share or the
                                                             blob endpoint,
                                                             4 TiB through
                                                             the file share
    Google appliance        40 or 300 TB         -           5 TiB, a Cloud
                                                             Storage object

A part is written under a name of its own, a temporary one first and put
in place whole - both appliances import only files that stand still, and
the Azure one uploads every file of its shares, a page blob only where it
is a whole number of 512 bytes (a part is not, so the page blob share is
refused), and nothing written straight into a share's root (a folder
under it becomes the container).
"""
import hashlib
import math
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

from . import decide, durable

TB, TIB = 10 ** 12, 2 ** 40

#: where each published number comes from
PAGES = {
    "snowball": "docs.aws.amazon.com/snowball/latest/developer-guide"
                " (device-differences.html, limits.html)",
    "databox": "learn.microsoft.com/azure/databox/data-box-limits",
    "transfer_appliance": "docs.cloud.google.com/transfer-appliance/docs/"
                          "4.0/specifications; a Cloud Storage object's"
                          " own limit",
}

#: each carrier's models and what each holds
MODELS = {
    "snowball": {"210": 210 * TB},
    "databox": {"80": 80 * TIB, "120": 120 * TIB, "525": 525 * TIB},
    "transfer_appliance": {"ta40": 40 * TB, "ta300": 300 * TB},
}
#: the model a carrier is taken as where the hop names none: its
#: smallest, so a seed never counts on room it may not have
DEFAULT_MODEL = {"snowball": "210", "databox": "80",
                 "transfer_appliance": "ta40"}
#: files a model holds
FILES = {("databox", "80"): 500_000_000, ("databox", "120"): 500_000_000,
         ("databox", "525"): 1_000_000_000}
#: the largest object an object endpoint or store takes, and the longest
#: name
S3_OBJECT, S3_NAME = 5 * TB, 1024
BLOCK_BLOB, AZURE_FILE, BLOB_NAME = 14 * TIB, 4 * TIB, 1024
GCS_OBJECT, GCS_NAME = 5 * TIB, 1024
#: the AWS appliance takes a part of a multipart upload of 2 GiB at most
SNOWBALL_PART = 2 * 2 ** 30
#: a block blob's own block size and count, an object's parts to compose
AZURE_BLOCK, AZURE_BLOCKS = 4000 * 2 ** 20, 50_000
GCS_COMPOSE, GCS_COMPONENTS = 32, 1024
#: what a multipart upload sends at once: well under that
UPLOAD_PART = 512 * 2 ** 20

KINDS = ("disk", "snowball", "databox", "transfer_appliance")
#: a carrier's name as an operator reads it
SAID = {"disk": "a disk attached here",
        "snowball": "the AWS appliance",
        "databox": "the Azure appliance",
        "transfer_appliance": "the Google appliance"}

#: the name of the file a carrier is proved with
PROBE = ".migkit-probe"


@dataclass(frozen=True)
class Limits:
    """What a carrier takes: its bytes, its largest file, how many files,
    the longest name - None where nothing says - and where the numbers
    come from."""
    capacity: int = None
    file_bytes: int = None
    files: int = None
    name_chars: int = None
    whence: str = ""

    def merged(self, other):
        """The least of each, where either says it."""
        def least(a, b):
            got = [x for x in (a, b) if x is not None]
            return min(got) if got else None
        return Limits(least(self.capacity, other.capacity),
                      least(self.file_bytes, other.file_bytes),
                      least(self.files, other.files),
                      least(self.name_chars, other.name_chars),
                      "; ".join(w for w in (self.whence, other.whence) if w))


def size_of(text):
    """Bytes from `80TB`, `120 TiB`, `500GB` or a number of bytes; None
    where it is none of them."""
    if text is None or text == "":
        return None
    if isinstance(text, (int, float)) and not isinstance(text, bool):
        return int(text)
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kmgtp]?)(i?)b?\s*",
                     str(text).lower())
    if not m:
        return None
    power = "kmgtp".find(m.group(2)) + 1 if m.group(2) else 0
    base = 1024 if m.group(3) else 1000
    return int(float(m.group(1)) * base ** power)


# ---- the stores ------------------------------------------------------------

class Store:
    """One place parts are written to or read from, by name under its
    root. `said` is what an operator reads; no store names a program."""
    said = ""
    #: the `Link` measured to this store, where one was (`move_file`)
    link = None

    def limits(self):
        return Limits()

    def local(self, name):
        """The file itself where it is on this machine's file system, so
        it is read in place; None where it must be fetched."""
        return None


class LocalStore(Store):
    """A directory: a disk attached here, or an appliance's share mounted
    here. Written whole and synced as `durable` writes, but not private
    to this user as its state is: the appliance imports the files, and
    the far end reads them as another user - they are sealed instead."""

    def __init__(self, root, said="a directory here"):
        self.root = Path(root)
        self.said = f"{said} ({self.root})"

    def _at(self, name):
        return self.root / name

    def local(self, name):
        return self._at(name)

    def put(self, name, path):
        dest = self._at(name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".partial")
        shutil.copyfile(path, tmp)
        _synced(tmp)
        os.replace(tmp, dest)
        durable.sync_dir(dest.parent)

    def fetch(self, name, path):
        shutil.copyfile(self._at(name), path)

    def size(self, name):
        try:
            return self._at(name).stat().st_size
        except OSError:
            return None

    def listing(self, prefix=""):
        """{name: bytes} of the files under `prefix`."""
        base = self._at(prefix) if prefix else self.root
        if not base.exists():
            return {}
        return {p.relative_to(self.root).as_posix(): p.stat().st_size
                for p in base.rglob("*")
                if p.is_file() and not p.name.endswith(".partial")}

    def put_bytes(self, name, data):
        dest = self._at(name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".partial")
        tmp.write_bytes(data)
        _synced(tmp)
        os.replace(tmp, dest)
        durable.sync_dir(dest.parent)

    def get_bytes(self, name):
        try:
            return self._at(name).read_bytes()
        except OSError:
            return None

    def delete(self, name):
        self._at(name).unlink(missing_ok=True)

    def limits(self):
        """What the file system under the root says of itself now: the
        bytes free, the files it can still hold, the longest name."""
        at = self.root
        while not at.exists() and at != at.parent:
            at = at.parent
        try:
            st = os.statvfs(at)
        except OSError:
            return Limits()
        return Limits(capacity=st.f_bavail * st.f_frsize,
                      files=st.f_favail or None,
                      name_chars=st.f_namemax or None,
                      whence="what the file system there says of itself")


def _synced(path):
    with open(path, "rb+") as f:
        os.fsync(f.fileno())


class S3Store(Store):
    """A bucket and prefix: the AWS appliance's own object endpoint, or
    the object store a carrier was imported into (`s3://`)."""

    def __init__(self, bucket, prefix="", endpoint=None, region=None,
                 profile=None, ca_bundle=None, said="an object store"):
        self.bucket, self.prefix = bucket, _prefix(prefix)
        self.endpoint, self.region = endpoint, region
        self.profile, self.ca_bundle = profile, ca_bundle
        self.said = f"{said} (s3://{bucket}/{self.prefix})"
        self._client = None

    def _s3(self):
        if self._client is None:
            import boto3
            from botocore.config import Config
            session = boto3.session.Session(profile_name=self.profile or None)
            self._client = session.client(
                "s3", endpoint_url=self.endpoint or None,
                region_name=self.region or None,
                verify=self.ca_bundle if self.ca_bundle else None,
                config=Config(s3={"addressing_style": "path"},
                              retries={"max_attempts": 10,
                                       "mode": "adaptive"}))
        return self._client

    def _key(self, name):
        return self.prefix + name

    def _transfer(self, size=None):
        from boto3.s3.transfer import TransferConfig
        if self.link is not None and size:
            plan = part_plan(size, self.link, most=min(UPLOAD_PART,
                                                       SNOWBALL_PART))
            return TransferConfig(multipart_threshold=plan.part,
                                  multipart_chunksize=plan.part,
                                  max_concurrency=plan.streams)
        return TransferConfig(multipart_threshold=64 * 2 ** 20,
                              multipart_chunksize=min(UPLOAD_PART,
                                                      SNOWBALL_PART),
                              max_concurrency=4)

    def put(self, name, path):
        self._s3().upload_file(str(path), self.bucket, self._key(name),
                               Config=self._transfer(
                                   os.path.getsize(path)))

    def fetch(self, name, path):
        self._s3().download_file(self.bucket, self._key(name), str(path),
                                 Config=self._transfer(self.size(name)))

    def size(self, name):
        from botocore.exceptions import ClientError
        try:
            got = self._s3().head_object(Bucket=self.bucket,
                                         Key=self._key(name))
        except ClientError as e:
            if _missing(e):
                return None
            raise
        return int(got["ContentLength"])

    def listing(self, prefix=""):
        out = {}
        pages = self._s3().get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=self._key(prefix))
        for page in pages:
            for o in page.get("Contents") or []:
                out[o["Key"][len(self.prefix):]] = int(o["Size"])
        return out

    def put_bytes(self, name, data):
        self._s3().put_object(Bucket=self.bucket, Key=self._key(name),
                              Body=bytes(data))

    def get_bytes(self, name):
        from botocore.exceptions import ClientError
        try:
            got = self._s3().get_object(Bucket=self.bucket,
                                        Key=self._key(name))
        except ClientError as e:
            if _missing(e):
                return None
            raise
        return got["Body"].read()

    def delete(self, name):
        self._s3().delete_object(Bucket=self.bucket, Key=self._key(name))

    def limits(self):
        return Limits(file_bytes=S3_OBJECT, name_chars=S3_NAME,
                      whence="an S3 object's own limits")


def _missing(e):
    code = str((getattr(e, "response", None) or {}).get("Error", {})
               .get("Code", ""))
    return code in ("404", "NoSuchKey", "NotFound")


def _prefix(p):
    p = str(p or "").strip("/")
    return p + "/" if p else ""


class AzureBlobStore(Store):
    """A container and prefix: the Azure appliance's blob endpoint, or the
    storage account a carrier was imported into (`https://`)."""

    def __init__(self, account_url, container, prefix="", credential=None,
                 ca_bundle=None, said="a blob store"):
        self.account_url, self.container = account_url, container
        self.prefix = _prefix(prefix)
        self.credential, self.ca_bundle = credential, ca_bundle
        self.said = f"{said} ({account_url}/{container}/{self.prefix})"
        self._client = None

    def _box(self):
        if self._client is None:
            from azure.storage.blob import BlobServiceClient
            kw = {"connection_verify": self.ca_bundle} if self.ca_bundle \
                else {}
            cred = self.credential
            if cred and ("AccountKey=" in cred
                         or "SharedAccessSignature=" in cred):
                # a connection string: the account and its key in one
                service = BlobServiceClient.from_connection_string(cred,
                                                                   **kw)
            else:
                if not cred:
                    from azure.identity import DefaultAzureCredential
                    cred = DefaultAzureCredential()
                elif not str(cred).lstrip("?").startswith("sv="):
                    # an account key; a shared access signature is given
                    # as it is
                    cred = {"account_name": _account_of(self.account_url),
                            "account_key": cred}
                service = BlobServiceClient(self.account_url,
                                            credential=cred, **kw)
            self._client = service.get_container_client(self.container)
        return self._client

    def _key(self, name):
        return self.prefix + name

    def put(self, name, path):
        with open(path, "rb") as f:
            self._box().upload_blob(self._key(name), f, overwrite=True,
                                    max_concurrency=4)

    def fetch(self, name, path):
        with open(path, "wb") as f:
            self._box().download_blob(self._key(name),
                                      max_concurrency=4).readinto(f)

    def size(self, name):
        from azure.core.exceptions import ResourceNotFoundError
        try:
            return int(self._box().get_blob_client(self._key(name))
                       .get_blob_properties().size)
        except ResourceNotFoundError:
            return None

    def listing(self, prefix=""):
        return {b.name[len(self.prefix):]: int(b.size)
                for b in self._box().list_blobs(
                    name_starts_with=self._key(prefix))}

    def put_bytes(self, name, data):
        self._box().upload_blob(self._key(name), bytes(data), overwrite=True)

    def get_bytes(self, name):
        from azure.core.exceptions import ResourceNotFoundError
        try:
            return self._box().download_blob(self._key(name)).readall()
        except ResourceNotFoundError:
            return None

    def delete(self, name):
        self._box().delete_blob(self._key(name))

    def limits(self):
        return Limits(file_bytes=BLOCK_BLOB, name_chars=BLOB_NAME,
                      whence="a block blob's own limits")


def _account_of(url):
    """The storage account an address names: the first label of its host,
    or, where the host is an address (a local emulator), the first part
    of its path."""
    from urllib.parse import urlparse
    u = urlparse(url)
    host = u.hostname or ""
    if host == "localhost" or ":" in host or re.fullmatch(r"[\d.]+", host):
        return u.path.strip("/").split("/")[0]
    return host.split(".")[0]


class GcsStore(Store):
    """A bucket and prefix of Cloud Storage (`gs://`): where the Google
    appliance's contents are put."""

    def __init__(self, bucket, prefix="", said="an object store"):
        self.bucket_name, self.prefix = bucket, _prefix(prefix)
        self.said = f"{said} (gs://{bucket}/{self.prefix})"
        self._bucket = None

    def _gcs(self):
        if self._bucket is None:
            from google.cloud import storage
            client = storage.Client(
                project=os.environ.get("GOOGLE_CLOUD_PROJECT") or "<none>")
            self._bucket = client.bucket(self.bucket_name)
        return self._bucket

    def _key(self, name):
        return self.prefix + name

    def put(self, name, path):
        self._gcs().blob(self._key(name)).upload_from_filename(str(path))

    def fetch(self, name, path):
        self._gcs().blob(self._key(name)).download_to_filename(str(path))

    def size(self, name):
        got = self._gcs().get_blob(self._key(name))
        return None if got is None else int(got.size)

    def listing(self, prefix=""):
        return {b.name[len(self.prefix):]: int(b.size)
                for b in self._gcs().client.list_blobs(
                    self._gcs(), prefix=self._key(prefix))}

    def put_bytes(self, name, data):
        self._gcs().blob(self._key(name)).upload_from_string(bytes(data))

    def get_bytes(self, name):
        got = self._gcs().get_blob(self._key(name))
        return None if got is None else got.download_as_bytes()

    def delete(self, name):
        self._gcs().blob(self._key(name)).delete()

    def limits(self):
        return Limits(file_bytes=GCS_OBJECT, name_chars=GCS_NAME,
                      whence="a Cloud Storage object's own limits")


def at(location, endpoint=None, region=None, profile=None, credential=None,
       ca_bundle=None, said="where the far end reads the seed"):
    """The store a location names: `s3://bucket/prefix`, `gs://...`, an
    `http(s)://` blob address (account, container, prefix), or a
    directory."""
    text = str(location)
    if text.startswith("s3://"):
        bucket, _, prefix = text[5:].partition("/")
        return S3Store(bucket, prefix, endpoint, region, profile, ca_bundle,
                       said)
    if text.startswith("gs://"):
        bucket, _, prefix = text[5:].partition("/")
        return GcsStore(bucket, prefix, said)
    if text.startswith(("https://", "http://")):
        from urllib.parse import urlparse
        u = urlparse(text)
        parts = u.path.strip("/").split("/")
        if _account_of(text) == (parts[0] if parts else ""):
            base, parts = f"{u.scheme}://{u.netloc}/{parts[0]}", parts[1:]
        else:
            base = f"{u.scheme}://{u.netloc}"
        if not parts or not parts[0]:
            raise SystemExit(f"ship: {text} names no container - write it"
                             " as https://<account host>/<container>/<prefix>")
        return AzureBlobStore(base, parts[0], "/".join(parts[1:]),
                              credential, ca_bundle, said)
    return LocalStore(text, said)


# ---- the carriers, as rungs -------------------------------------------------

def _model(kind, settings):
    model = str(settings.get("model") or DEFAULT_MODEL.get(kind) or "")
    return model.lower().removeprefix("databox-").removesuffix("tb")


def published(kind, settings, share=""):
    """The limits the provider publishes for this carrier - the hop's
    `capacity:` over its model's - through the share it is written
    through (`share_kind`)."""
    capacity = size_of(settings.get("capacity"))
    model = _model(kind, settings)
    whence = []
    if capacity is not None:
        whence.append("the capacity the hop gives")
    elif kind in MODELS:
        capacity = MODELS[kind].get(model)
        if capacity is None:
            raise SystemExit(
                f"ship: model {settings.get('model')!r} is not one of"
                f" {SAID[kind]}'s ({', '.join(MODELS[kind])}) - name one,"
                " or give its capacity (capacity: 80TB)")
        whence.append("its model's published capacity")
    file_bytes = name = None
    if kind == "snowball":
        file_bytes, name = S3_OBJECT, S3_NAME
    elif kind == "databox":
        file_bytes = AZURE_FILE if share == "file" else BLOCK_BLOB
        name = BLOB_NAME
    elif kind == "transfer_appliance":
        file_bytes, name = GCS_OBJECT, GCS_NAME
    if kind in PAGES:
        whence.append(f"its published limits ({PAGES[kind]})")
    return Limits(capacity, file_bytes, FILES.get((kind, model)), name,
                  "; ".join(whence))


#: an Azure appliance share's name ends in what it uploads the files as
_SHARES = {"_blockblob": "block", "_pageblob": "page", "_azfile": "file"}


def share_kind(path):
    """("block" | "page" | "file" | "", the share's own folder is the
    path) for a path on an Azure appliance's share: which of its shares
    the path is under, and whether it is that share's root."""
    parts = [p.lower() for p in Path(path).parts]
    for i, p in enumerate(parts):
        for end, kind in _SHARES.items():
            if p.endswith(end):
                return kind, i == len(parts) - 1
    return "", False


def _share_problem(path):
    """Why a path on an Azure appliance's share is not uploaded whole, or
    "": the page blob share, a share's own root, a folder whose name no
    container may have."""
    kind, root = share_kind(path)
    if kind == "page":
        return ("the page blob share takes only files a whole number of 512"
                " bytes long, which a part is not - write to the block blob"
                " share")
    if root:
        return ("a file written straight into the share's own root is not"
                " uploaded - make a folder under it (it becomes the"
                " container) and write there")
    if kind == "block":
        parts = Path(path).parts
        at = next(i for i, p in enumerate(parts)
                  if p.lower().endswith("_blockblob"))
        name = parts[at + 1]
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){2,62}",
                            name):
            return (f"the folder {name!r} becomes a container, whose name"
                    " is 3 to 63 lowercase letters, digits and single"
                    " hyphens - rename it")
    return ""


def _need(what, holds, why=None):
    return decide.Need(what, holds, why)


def _named(kind):
    return _need(f"the hop to name {SAID[kind]}",
                 lambda f, k=kind: f["kind"] == k, "the hop names another"
                 " carrier")


def _prove(f):
    """A probe written, read back and taken away: the carrier takes a
    file and gives it back as it was."""
    store = f["store"]()
    body = hashlib.sha256(os.urandom(32)).hexdigest().encode()
    try:
        store.put_bytes(PROBE, body)
        back = store.get_bytes(PROBE)
        store.delete(PROBE)
    except Exception as e:  # noqa: BLE001 - said as the reason
        return f"a probe file could not be written and read back ({_one(e)})"
    if back != body:
        return "a probe file came back different from what was written"
    return None


def _one(e):
    text = str(e).strip() or type(e).__name__
    return text.splitlines()[-1][:200]


def rungs():
    """The carriers a seed is written to, one rung each way it is
    reached; each proved by a probe file (`_prove`) where it is climbed
    (`for_export`)."""
    path = _need("a path", lambda f: bool(f["path"]),
                 "{rung} is reached through a directory here, and the hop"
                 " gives no path")
    endpoint = _need("an endpoint and a bucket",
                     lambda f: bool(f["endpoint"] and f["bucket"]),
                     "{rung} is reached through its endpoint, and the hop"
                     " gives no endpoint and bucket")
    blob = _need("an endpoint and a container",
                 lambda f: bool(f["endpoint"] and f["container"]),
                 "{rung} is reached through its blob endpoint, and the hop"
                 " gives no endpoint and container")
    share = _need("a folder under a share that uploads it whole",
                  lambda f: not _share_problem(f["path"]),
                  lambda f: _share_problem(f["path"]))
    return (
        decide.Rung("disk", SAID["disk"],
                    needs=(_named("disk"), path)),
        decide.Rung("snowball", f"{SAID['snowball']} through its object"
                                " endpoint",
                    needs=(_named("snowball"), endpoint)),
        decide.Rung("databox share", f"{SAID['databox']} through its file"
                                     " share",
                    needs=(_named("databox"), path, share)),
        decide.Rung("databox blob", f"{SAID['databox']} through its blob"
                                    " endpoint",
                    needs=(_named("databox"), blob)),
        decide.Rung("transfer appliance", f"{SAID['transfer_appliance']}"
                                          " through its NFS share",
                    needs=(_named("transfer_appliance"), path)),
    )


def _store_for(rung_name, s):
    from .config import _secret
    said = SAID[_kind_of(rung_name)]
    if rung_name in ("disk", "databox share", "transfer appliance"):
        return LocalStore(s.get("path"), said)
    if rung_name == "snowball":
        return S3Store(s.get("bucket"), s.get("prefix") or "",
                       s.get("endpoint"), s.get("region") or "snow",
                       s.get("profile"), s.get("ca_bundle"), said)
    return AzureBlobStore(s.get("endpoint"), s.get("container"),
                          s.get("prefix") or "",
                          _secret(s.get("credential")) if s.get("credential")
                          else None, s.get("ca_bundle"), said)


def _kind_of(rung_name):
    return {"disk": "disk", "snowball": "snowball",
            "databox share": "databox", "databox blob": "databox",
            "transfer appliance": "transfer_appliance"}[rung_name]


@dataclass
class Carrier:
    """The carrier a seed is written to: its rung, its store, its limits
    and why it was taken."""
    rung: object
    store: Store
    limits: Limits
    reason: str

    @property
    def kind(self):
        return _kind_of(self.rung.name)

    @property
    def said(self):
        return self.store.said


def for_export(s, prove=True):
    """The `Carrier` the hop's `ship:` names, climbed and proved; stops,
    with every rung's reason, where none holds."""
    kind = str(s.get("carrier") or "").lower()
    if kind not in KINDS:
        raise SystemExit(f"ship: carrier {s.get('carrier')!r} - one of"
                         f" {', '.join(KINDS)}")
    stores = {}

    def store(name):
        if name not in stores:
            stores[name] = _store_for(name, s)
        return stores[name]
    facts = decide.Facts({"kind": kind, "path": s.get("path"),
                          "endpoint": s.get("endpoint"),
                          "bucket": s.get("bucket"),
                          "container": s.get("container")})
    ladder = []
    # the ways the carrier the hop names is reached: another carrier's are
    # no reason for or against it
    for r in [r for r in rungs() if _kind_of(r.name) == kind]:
        def proof(f, r=r):
            return _prove({**f, "store": lambda: store(r.name)})
        ladder.append(decide.Rung(r.name, r.said, needs=r.needs,
                                  prove=proof if prove else None))
    got = decide.climb("the carrier", ladder, facts)
    if got.rung is None:
        raise SystemExit(f"ship: no carrier the hop names can be written"
                         f" here - {got.reason}")
    st = store(got.rung.name)
    share = share_kind(s.get("path") or "")[0] if kind == "databox" else ""
    limits = published(kind, s, share) if kind != "disk" else Limits(
        size_of(s.get("capacity")), whence="the capacity the hop gives"
        if s.get("capacity") else "")
    # what the carrier holds in all, from its model or the hop, stays as
    # it is: what is free on it now is asked part by part (`Store.limits`)
    now = st.limits()
    limits = limits.merged(Limits(file_bytes=now.file_bytes,
                                  name_chars=now.name_chars,
                                  whence=now.whence))
    return Carrier(got.rung, st, limits, got.reason)


def _load_rungs():
    """Where the far end reads the seed: the provider's object store the
    hop names, or the carrier attached there - each only where the sealed
    seed is."""
    def sealed(where):
        return _need("the seed sealed there",
                     lambda f: f["sealed"](where) is not None,
                     lambda f: "no sealed seed is at"
                               f" {f['said'](where)} yet")
    return (
        decide.Rung("object store", "the provider's object store the carrier"
                                    " was imported into",
                    needs=(_need("the hop to say where it was imported",
                                 lambda f: bool(f["load_from"]),
                                 "the hop gives no load_from"),
                           sealed("load_from"))),
        decide.Rung("attached", "the carrier attached here",
                    needs=(_need("the carrier readable here",
                                 lambda f: f["attached"] is not None,
                                 "the carrier is reached by a directory or"
                                 " an endpoint this machine does not have"),
                           sealed("attached"))),
    )


def for_load(s, sealed_at):
    """(the store the far end reads the seed from, why) - the provider's
    object store where the hop names one, else the carrier as attached
    here - or (None, why neither holds a sealed seed). `sealed_at(store)`
    reads the seed's seal there, or None."""
    from .config import _secret
    stores = {}
    if s.get("load_from"):
        stores["load_from"] = at(
            s.get("load_from"), s.get("load_endpoint"), s.get("load_region"),
            s.get("load_profile"),
            _secret(s.get("load_credential")) if s.get("load_credential")
            else None, s.get("ca_bundle"), "the provider's object store")
    name = {"disk": "disk", "snowball": "snowball",
            "transfer_appliance": "transfer appliance",
            "databox": "databox share" if s.get("path") else "databox blob"
            }.get(str(s.get("carrier") or "").lower())
    try:
        stores["attached"] = _store_for(name, s) if name else None
    except Exception:  # noqa: BLE001 - a carrier this machine cannot reach
        stores["attached"] = None
    seals = {}

    def sealed(where):
        if where not in seals:
            st = stores.get(where)
            try:
                seals[where] = sealed_at(st) if st is not None else None
            except Exception:  # noqa: BLE001 - not readable is not there
                seals[where] = None
        return seals[where]
    facts = decide.Facts({"load_from": s.get("load_from"),
                          "attached": stores.get("attached"),
                          "sealed": sealed,
                          "said": lambda w: stores[w].said})
    got = decide.climb("where the seed is read", _load_rungs(), facts)
    if got.rung is None:
        return None, got.reason
    return (stores["load_from" if got.rung.name == "object store"
                   else "attached"], got.reason)


def carriers_needed(total, limits):
    """How many carriers `total` bytes take at this carrier's capacity,
    or None where nothing says what it holds."""
    if not limits.capacity:
        return None
    return max(1, math.ceil(total / limits.capacity))


# ---- files as parallel ranged parts (backlog R20.7) ------------------------

#: an S3 multipart upload's own limits: a part 5 MiB at least (the last
#: excepted), 5 GiB at most, 10,000 parts
S3_PART_LEAST, S3_PART_MOST, S3_PARTS = 5 * 2 ** 20, 5 * 2 ** 30, 10_000
#: a part long enough that its request's round trip is this share of it
TRIP_SHARE = 0.01


@dataclass(frozen=True)
class Link:
    """What a file's way between two stores measured: the bytes a second
    of every stream together, of one stream, and a request's round trip
    in seconds."""
    rate: float
    per_stream: float
    rtt: float
    streams: int = 1
    whence: str = "measured"


@dataclass(frozen=True)
class PartPlan:
    size: int
    part: int
    streams: int
    parts: int
    why: str = ""


def part_plan(size, link, least=S3_PART_LEAST, most=S3_PART_MOST,
              count=S3_PARTS):
    """The part size and how many go at once for a file of `size` bytes
    over `link`: as many streams as one stream's rate fills the link
    with, each part long enough that a round trip is `TRIP_SHARE` of it,
    within the store's own least, most and count of parts."""
    size = int(size or 0)
    streams = max(1, math.ceil(link.rate / link.per_stream)) \
        if link.per_stream else 1
    part = int(link.per_stream * link.rtt / TRIP_SHARE) \
        if link.per_stream and link.rtt else least
    floor = math.ceil(size / count) if count else 0
    part = max(part, least, floor)
    if most:
        part = min(part, most)
    part = max(part, 1)
    parts = max(1, math.ceil(size / part))
    streams = min(streams, parts)
    return PartPlan(size, part, streams, parts,
                    f"{parts} parts of {part} bytes, {streams} at a time:"
                    f" one stream ~{link.per_stream:.0f} B/s of a link"
                    f" ~{link.rate:.0f} B/s, a round trip {link.rtt:.3f}s"
                    f" ({link.whence})")


def measure_link(read_range, size, clock=None, most_streams=None):
    """`Link` of a store's ranged reads (`read_range(start, end)` ->
    bytes) on a file of `size`: a one-byte read times the round trip,
    a range read alone times one stream, and the streams are doubled
    while the bytes a second of all of them still rise by more than a
    tie (`decide.TIE`) - bounded by the file, never by a fixed count."""
    import time
    from concurrent.futures import ThreadPoolExecutor
    clock = clock or time.perf_counter
    if not size:
        return Link(0.0, 0.0, 0.0, 1, "nothing to read")
    t0 = clock()
    read_range(0, 1)
    rtt = clock() - t0
    piece = max(1, min(size, S3_PART_LEAST))
    t0 = clock()
    got = len(read_range(0, piece))
    one = got / max(clock() - t0, 1e-9)
    best, streams = one, 1
    most = most_streams or max(1, size // piece)
    while streams * 2 <= most:
        n = streams * 2
        starts = [(i * piece) % max(size - piece + 1, 1) for i in range(n)]
        t0 = clock()
        with ThreadPoolExecutor(max_workers=n) as pool:
            total = sum(len(b) for b in pool.map(
                lambda s: read_range(s, s + piece), starts))
        rate = total / max(clock() - t0, 1e-9)
        if rate <= best * (1 + decide.TIE):
            break
        best, streams = rate, n
    return Link(best, one, rtt, streams)


_CRC32C = 0x82F63B78


def crc32c(data, start=0):
    """CRC-32C (Castagnoli) of `data`, folded onto `start`: the library
    the Google client already needs where it is there, else a table."""
    try:
        import google_crc32c
        return google_crc32c.extend(start, bytes(data))
    except ImportError:
        pass
    table = _crc_table()
    crc = start ^ 0xFFFFFFFF
    for b in bytes(data):
        crc = table[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def _crc_table():
    out = []
    for n in range(256):
        for _ in range(8):
            n = (n >> 1) ^ _CRC32C if n & 1 else n >> 1
        out.append(n)
    return out


def _gf2_times(mat, vec):
    out, i = 0, 0
    while vec:
        if vec & 1:
            out ^= mat[i]
        vec >>= 1
        i += 1
    return out


def crc32c_combine(first, second, length):
    """The CRC-32C of two pieces joined, from each piece's own and the
    second's length - so parts hashed in parallel give the whole's."""
    if length <= 0:
        return first
    odd = [_CRC32C] + [1 << n for n in range(31)]
    even = [_gf2_times(odd, odd[n]) for n in range(32)]
    odd = [_gf2_times(even, even[n]) for n in range(32)]
    while True:
        even = [_gf2_times(odd, odd[n]) for n in range(32)]
        if length & 1:
            first = _gf2_times(even, first)
        length >>= 1
        if not length:
            break
        odd = [_gf2_times(even, even[n]) for n in range(32)]
        if length & 1:
            first = _gf2_times(odd, first)
        length >>= 1
        if not length:
            break
    return first ^ second


class Streamed:
    """What a file's parts carried, hashed as they streamed: each part's
    MD5 (what an object store says of a part it took) and SHA-256, the
    whole as a SHA-256 of the parts' in order."""

    def __init__(self, crc=False):
        self.parts, self.crcs, self._crc = {}, {}, crc

    def saw(self, i, data):
        self.parts[i] = (hashlib.md5(data, usedforsecurity=False).hexdigest(),
                         hashlib.sha256(data).hexdigest(), len(data))
        if self._crc:
            self.crcs[i] = crc32c(data)

    def crc32c(self):
        """The CRC-32C of the whole, folded from the parts' in order."""
        whole = 0
        for i in sorted(self.parts):
            whole = crc32c_combine(whole, self.crcs[i], self.parts[i][2])
        return whole

    def digest(self):
        h = hashlib.sha256()
        for i in sorted(self.parts):
            h.update(bytes.fromhex(self.parts[i][1]))
        return h.hexdigest()

    def multipart_etag(self):
        """The ETag an object store gives an object of these parts."""
        md5s = b"".join(bytes.fromhex(self.parts[i][0])
                        for i in sorted(self.parts))
        return (hashlib.md5(md5s, usedforsecurity=False).hexdigest()
                + f"-{len(self.parts)}")

    def size(self):
        return sum(p[2] for p in self.parts.values())


class Moved:
    """A file moved: the way, the plan, its digest and what proved it."""

    def __init__(self, way, plan, digest, proved):
        self.way, self.plan, self.digest, self.proved = (way, plan, digest,
                                                         proved)

    def line(self):
        return f"{self.way}: {self.plan.why}; {self.proved}"


def _local_read_range(store, name):
    path = store.local(name)

    def read(start, end):
        with open(path, "rb") as f:
            return os.pread(f.fileno(), end - start, start)
    return read


def _s3_read_range(store, name):
    def read(start, end):
        got = store._s3().get_object(Bucket=store.bucket,
                                     Key=store._key(name),
                                     Range=f"bytes={start}-{end - 1}")
        return got["Body"].read()
    return read


def _azure_read_range(store, name):
    def read(start, end):
        return store._box().download_blob(
            store._key(name), offset=start, length=end - start).readall()
    return read


def _gcs_read_range(store, name):
    def read(start, end):
        return store._gcs().blob(store._key(name)).download_as_bytes(
            start=start, end=end - 1)
    return read


def read_range_of(store, name):
    """`read_range(start, end)` of a file in `store`, or None where the
    store cannot read a range."""
    if isinstance(store, S3Store):
        return _s3_read_range(store, name)
    if isinstance(store, AzureBlobStore):
        return _azure_read_range(store, name)
    if isinstance(store, GcsStore):
        return _gcs_read_range(store, name)
    if store.local(name) is not None:
        return _local_read_range(store, name)
    return None


def _same_s3(a, b):
    return (isinstance(a, S3Store) and isinstance(b, S3Store)
            and (a.endpoint or "") == (b.endpoint or "")
            and (a.region or "") == (b.region or "")
            and (a.profile or "") == (b.profile or ""))


def _ranges(plan):
    return [(i, i * plan.part, min(plan.size, (i + 1) * plan.part))
            for i in range(plan.parts)]


def _parallel(plan, each):
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=plan.streams) as pool:
        return list(pool.map(lambda r: each(*r), _ranges(plan)))


#: what a copy inside a store is held to, best proof first
SOURCE_PROOFS = (
    decide.Rung("source checksum", "the source object's own whole-object"
                " checksum",
                needs=(decide.Need("a whole-object checksum on the source",
                                   lambda f: bool(f["checksum"])),),
                because="the source says what the copy must be, and the"
                " store computes the copy's the same way", leaves="nothing"),
    decide.Rung("source digest", "the source object's own digest",
                needs=(decide.Need("a source object written in one piece",
                                   lambda f: f["plain etag"]),
                       decide.Need("an object a single copy can carry",
                                   lambda f: f["size"] <= S3_COPY_ONE),),
                leaves="nothing"),
    decide.Rung("parts reported", "the parts the store reported",
                because="the source gave nothing to hold the copy to",
                leaves="nothing"),
)
#: the largest object one copy request carries
S3_COPY_ONE = 5 * 2 ** 30
#: the whole-object checksums an object store can give, name -> algorithm
_S3_FULL = (("ChecksumCRC64NVME", "CRC64NVME"), ("ChecksumCRC32C", "CRC32C"),
            ("ChecksumCRC32", "CRC32"))


def _source_says(src, name):
    """(facts for `SOURCE_PROOFS`, the source's head): what the source
    object says of itself - a whole-object checksum, or the digest of an
    object written whole - or nothing where it cannot be asked."""
    s3 = src._s3()
    try:
        head = s3.head_object(Bucket=src.bucket, Key=src._key(name),
                              ChecksumMode="ENABLED")
        etag = str(head.get("ETag") or "").strip('"')
        full = head.get("ChecksumType") == "FULL_OBJECT" or \
            "ChecksumCRC64NVME" in head
        got = next(((k, a, head[k]) for k, a in _S3_FULL
                    if full and head.get(k)), None)
        plain = bool(etag) and "-" not in etag and \
            head.get("ServerSideEncryption") != "aws:kms"
        return {"checksum": got, "plain etag": plain,
                "size": int(head.get("ContentLength") or 0)}, head
    except Exception:  # noqa: BLE001 - said by the rung that holds the copy
        return {"checksum": None, "plain etag": False, "size": 0}, {}


def _server_side(src, name, dst, dst_name, plan):
    says, head = _source_says(src, name)
    says["size"] = says["size"] or plan.size
    proof = decide.climb(f"holding the copy of {name} to the source",
                         SOURCE_PROOFS, facts=says, prove=False)
    s3, key = dst._s3(), dst._key(dst_name)
    source = {"Bucket": src.bucket, "Key": src._key(name)}
    if proof.path == "source digest":
        s3.copy_object(Bucket=dst.bucket, Key=key, CopySource=source)
        got = s3.head_object(Bucket=dst.bucket, Key=key)
        want = str(head.get("ETag") or "").strip('"')
        if got["ETag"].strip('"') != want or \
                int(got["ContentLength"]) != plan.size:
            dst.delete(dst_name)
            raise BadCopy(f"{dst_name}: the copy's digest is not the"
                          " source's - moved again whole")
        return Moved("copied inside the store", plan, want,
                     "the copy's digest equal to the source object's own")
    extra = {}
    if proof.path == "source checksum":
        field, algorithm, _ = says["checksum"]
        extra = {"ChecksumAlgorithm": algorithm, "ChecksumType":
                 "FULL_OBJECT"}
    up = s3.create_multipart_upload(Bucket=dst.bucket, Key=key,
                                    **extra)["UploadId"]

    def each(i, start, end):
        got = s3.upload_part_copy(
            Bucket=dst.bucket, Key=key, UploadId=up, PartNumber=i + 1,
            CopySource=source, CopySourceRange=f"bytes={start}-{end - 1}")
        return {"PartNumber": i + 1,
                "ETag": got["CopyPartResult"]["ETag"]}
    try:
        done = _parallel(plan, each)
        s3.complete_multipart_upload(Bucket=dst.bucket, Key=key,
                                     UploadId=up,
                                     MultipartUpload={"Parts": done})
    except BaseException:
        s3.abort_multipart_upload(Bucket=dst.bucket, Key=key, UploadId=up)
        raise
    md5s = b"".join(bytes.fromhex(p["ETag"].strip('"')) for p in done)
    want = hashlib.md5(md5s, usedforsecurity=False).hexdigest() \
        + f"-{len(done)}"
    if proof.path == "source checksum":
        field, algorithm, value = says["checksum"]
        got = s3.head_object(Bucket=dst.bucket, Key=key,
                             ChecksumMode="ENABLED")
        if got.get(field) != value or int(got["ContentLength"]) != plan.size:
            dst.delete(dst_name)
            raise BadCopy(f"{dst_name}: the copy's {algorithm} checksum is"
                          " not the source's - moved again whole")
        return Moved("copied inside the store", plan, value,
                     f"every part's digest the store returned, and the"
                     f" whole's {algorithm} equal to the source object's own")
    head = s3.head_object(Bucket=dst.bucket, Key=key)
    if head["ETag"].strip('"') != want or \
            int(head["ContentLength"]) != plan.size:
        dst.delete(dst_name)
        raise BadCopy(f"{dst_name}: the store's copy is not the parts it"
                      " said it wrote - moved again whole")
    return Moved("copied inside the store", plan, want,
                 "every part's digest the store returned, and the whole's;"
                 " the source gave no checksum of its own to hold it to")


def _to_s3(read, dst, dst_name, plan):
    import base64
    s3, key = dst._s3(), dst._key(dst_name)
    up = s3.create_multipart_upload(Bucket=dst.bucket, Key=key)["UploadId"]
    seen = Streamed()

    def each(i, start, end):
        data = read(start, end)
        seen.saw(i, data)
        md5 = seen.parts[i][0]
        got = s3.upload_part(Bucket=dst.bucket, Key=key, UploadId=up,
                             PartNumber=i + 1, Body=data,
                             ContentMD5=base64.b64encode(
                                 bytes.fromhex(md5)).decode())
        if got["ETag"].strip('"') != md5:
            raise BadCopy(f"{dst_name}: part {i + 1} arrived other than"
                          " it was sent")
        return {"PartNumber": i + 1, "ETag": got["ETag"]}
    try:
        done = _parallel(plan, each)
        s3.complete_multipart_upload(Bucket=dst.bucket, Key=key,
                                     UploadId=up,
                                     MultipartUpload={"Parts": done})
    except BaseException:
        s3.abort_multipart_upload(Bucket=dst.bucket, Key=key, UploadId=up)
        raise
    head = s3.head_object(Bucket=dst.bucket, Key=key)
    if head["ETag"].strip('"') != seen.multipart_etag() or \
            int(head["ContentLength"]) != plan.size:
        dst.delete(dst_name)
        raise BadCopy(f"{dst_name}: the store holds other bytes than were"
                      " streamed to it")
    return Moved("parallel ranged parts", plan, seen.digest(),
                 "each part checked as the store took it, and the whole")


def _to_local(read, dst, dst_name, plan):
    dest = dst.local(dst_name)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".partial")
    seen = Streamed()
    fd = os.open(tmp, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.ftruncate(fd, plan.size)

        def each(i, start, end):
            data = read(start, end)
            if len(data) != end - start:
                raise BadCopy(f"{dst_name}: part {i + 1} came short")
            seen.saw(i, data)
            os.pwrite(fd, data, start)
        _parallel(plan, each)
        os.fsync(fd)
    except BaseException:
        os.close(fd)
        tmp.unlink(missing_ok=True)
        raise
    os.close(fd)
    if seen.size() != plan.size or tmp.stat().st_size != plan.size:
        tmp.unlink(missing_ok=True)
        raise BadCopy(f"{dst_name}: {seen.size()} bytes of {plan.size}"
                      " arrived")
    os.replace(tmp, dest)
    durable.sync_dir(dest.parent)
    return Moved("parallel ranged parts", plan, seen.digest(),
                 "every part hashed as it streamed, the whole sized")


def _block_id(i):
    import base64
    return base64.b64encode(f"migkit-{i:08d}".encode()).decode()


def _to_azure(read, dst, dst_name, plan):
    from azure.storage.blob import BlobBlock
    blob = dst._box().get_blob_client(dst._key(dst_name))
    seen = Streamed()

    def each(i, start, end):
        data = read(start, end)
        seen.saw(i, data)
        blob.stage_block(_block_id(i), data, length=len(data),
                         validate_content=True)
        return _block_id(i)
    ids = _parallel(plan, each)
    blob.commit_block_list([BlobBlock(block_id=b) for b in ids])
    committed = blob.get_block_list("committed")[0]
    held = [int(b.size) for b in committed]
    sent = [seen.parts[i][2] for i in sorted(seen.parts)]
    if held != sent or int(blob.get_blob_properties().size) != plan.size:
        dst.delete(dst_name)
        raise BadCopy(f"{dst_name}: the blob holds other blocks than were"
                      " streamed to it")
    return Moved("parallel ranged parts", plan, seen.digest(),
                 "each block's digest checked by the store as it took it,"
                 " and the blocks and size it committed")


def _to_gcs(read, dst, dst_name, plan):
    bucket, key = dst._gcs(), dst._key(dst_name)
    seen = Streamed(crc=True)
    made = []

    def temp(tag):
        made.append(f"{key}.migkit-{tag}")
        return bucket.blob(made[-1])

    def each(i, start, end):
        data = read(start, end)
        seen.saw(i, data)
        piece = temp(f"part-{i:06d}")
        piece.upload_from_string(data, checksum="md5")
        return piece
    try:
        level = _parallel(plan, each)
        step = 0
        while len(level) > GCS_COMPOSE:
            level = [_composed(temp, level[k:k + GCS_COMPOSE], step, k)
                     for k in range(0, len(level), GCS_COMPOSE)]
            step += 1
        final = bucket.blob(key)
        final.compose(level)
        final.reload()
    finally:
        for n in made:
            try:
                bucket.blob(n).delete()
            except Exception:  # noqa: BLE001 - a part already gone
                pass
    import base64
    held = int.from_bytes(base64.b64decode(final.crc32c), "big")
    if int(final.size) != plan.size or held != seen.crc32c():
        final.delete()
        raise BadCopy(f"{dst_name}: the object holds other bytes than were"
                      " streamed to it")
    return Moved("parallel ranged parts", plan, seen.digest(),
                 "each part's digest checked by the store as it took it,"
                 " and the composed whole's CRC-32C equal to the parts'")


def _composed(temp, group, step, at):
    out = temp(f"join-{step}-{at:06d}")
    out.compose(group)
    return out


class BadCopy(SystemExit):
    """A moved file whose bytes are not what was sent: taken away again."""


def _one_stream(src, name, dst, dst_name, plan):
    import tempfile
    path = src.local(name)
    if path is None:
        fd, tmp = tempfile.mkstemp(prefix="migkit-part-")
        os.close(fd)
        try:
            src.fetch(name, tmp)
            dst.put(dst_name, tmp)
        finally:
            os.unlink(tmp)
    else:
        dst.put(dst_name, path)
    return Moved("one stream", plan, "", "the store's own transfer")


FILE_WAYS = (
    decide.Rung("server side", "copied inside the store",
                needs=(decide.Need("one object store on both sides",
                                   lambda f: f["same store"]),),
                because="no byte crosses this machine", leaves="nothing"),
    decide.Rung("ranged parts", "parallel ranged parts",
                needs=(decide.Need("a ranged read of the source",
                                   lambda f: f["ranged"]),
                       decide.Need("a store taking parts",
                                   lambda f: f["parts"]),),
                leaves="nothing"),
    decide.Rung("one stream", "the store's own transfer",
                leaves="nothing"),
)


def _write_probe(read, dst, dst_name):
    """(probe, clean): `probe(start, end)` puts that range of the file as
    a scratch object in `dst` and returns the bytes, so `measure_link`
    times the way up; `clean()` takes the scratch objects away."""
    import itertools
    import threading
    count, made, lock = itertools.count(), [], threading.Lock()

    def probe(start, end):
        data = read(start, end)
        name = f"{dst_name}.migkit-probe-{next(count)}"
        with lock:
            made.append(name)
        dst.put_bytes(name, data)
        return data

    def clean():
        for n in made:
            try:
                dst.delete(n)
            except Exception:  # noqa: BLE001 - a scratch object already gone
                pass
    return probe, clean


def _remote(store):
    return isinstance(store, (S3Store, AzureBlobStore, GcsStore))


def _bounds(dst):
    """(least, most, count) of a part in `dst`: the store's own."""
    if isinstance(dst, S3Store):
        return S3_PART_LEAST, S3_PART_MOST, S3_PARTS
    if isinstance(dst, AzureBlobStore):
        return S3_PART_LEAST, AZURE_BLOCK, AZURE_BLOCKS
    if isinstance(dst, GcsStore):
        return S3_PART_LEAST, GCS_OBJECT, GCS_COMPONENTS
    return 1, None, None


def _measured(src, name, dst, dst_name, read, size, clock):
    """The `Link` of the way: the remote end's ranged reads where the
    source is remote; where the file is here and goes to a store, the
    way up, timed by writing scratch objects - a read of this disk says
    nothing of the link."""
    here = src.local(name) is not None
    if here and _remote(dst):
        probe, clean = _write_probe(read, dst, dst_name)
        try:
            from dataclasses import replace
            return replace(measure_link(probe, size, clock),
                           whence="measured by writing scratch objects")
        finally:
            clean()
    return measure_link(read, size, clock)


def move_file(src, name, dst, dst_name=None, link=None, clock=None):
    """`Moved`: a dump file, Parquet or spool file from one store to
    another as the way the stores allow - copied inside one object store
    (held to the source's own checksum where it gives one), else as
    parallel ranged parts (object stores, blob and Cloud Storage
    included) hashed while they stream and checked at the end, else the
    store's own transfer - chosen through `decide`, the parts' size and
    number from the link measured on the way (`measure_link`) where
    `link` is not given and none was kept on the store. Two directories
    here are no link: with none given the file is copied whole."""
    dst_name = dst_name or name
    size = src.size(name)
    if size is None:
        raise SystemExit(f"{name} is not in {src.said}")
    read = read_range_of(src, name)
    both_here = src.local(name) is not None and not _remote(dst)
    ranged = read is not None and size > 0 and \
        not (both_here and link is None and dst.link is None)
    facts = {"same store": _same_s3(src, dst) and size > 0,
             "ranged": ranged,
             "parts": _remote(dst) or dst.local(dst_name) is not None}
    far = dst if _remote(dst) and src.local(name) is not None else src
    link = link or far.link
    if link is None and ranged:
        link = _measured(src, name, dst, dst_name, read, size, clock)
    if link is not None:
        for st in (far, dst):
            if _remote(st):
                st.link = link
    least, most, count = _bounds(dst)
    plan = part_plan(size, link, least, most, count) if link else None
    if plan is None:
        plan = PartPlan(size, size, 1, 1, "one part: nothing to measure on")
    choice = decide.climb(f"moving {name}", FILE_WAYS, facts=facts,
                          prove=False)
    if choice.path == "server side":
        return _server_side(src, name, dst, dst_name, plan)
    if choice.path == "ranged parts":
        if isinstance(dst, S3Store):
            return _to_s3(read, dst, dst_name, plan)
        if isinstance(dst, AzureBlobStore):
            return _to_azure(read, dst, dst_name, plan)
        if isinstance(dst, GcsStore):
            return _to_gcs(read, dst, dst_name, plan)
        return _to_local(read, dst, dst_name, plan)
    return _one_stream(src, name, dst, dst_name, plan)
