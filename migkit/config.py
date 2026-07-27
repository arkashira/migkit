import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

BASE = Path(__file__).resolve().parent.parent
CONF = Path(os.environ.get("MIGKIT_CONF", BASE / "conf" / "hops.yaml"))
REPORTS = Path(os.environ.get("MIGKIT_REPORTS", BASE / "reports"))


@dataclass
class Endpoint:
    host: str = ""
    port: int = 0
    user: str = ""
    password: str = ""
    options: dict = field(default_factory=dict)

    def configured(self):
        return bool(self.host or self.options.get("hosts")
                    or self.options.get("path") or self.options.get("url"))


@dataclass
class Hop:
    name: str
    engine: str
    source: Endpoint
    target: Endpoint
    databases: list = field(default_factory=list)
    exclude: list = field(default_factory=list)
    service: str = ""
    big_rows: int = 5_000_000
    slice: int = 1_000_000
    workers: int = 4
    options: dict = field(default_factory=dict)
    db_map: dict = field(default_factory=dict)

    def report_dir(self, db=""):
        d = REPORTS / self.name / db if db else REPORTS / self.name
        d.mkdir(parents=True, exist_ok=True)
        return d

    def target_db(self, db):
        """Target database name for a source db. Migrations often land in a
        differently-named db (e.g. cart_uat -> cart), so every dst-side
        connection resolves through this map (identity when unmapped).
        Local report paths stay keyed by the source name."""
        return self.db_map.get(db, db)

    def excluded(self, *parts):
        """True if an object matches any `exclude` pattern, so migkit neither
        verifies nor repairs it. Pass the dotted name parts, e.g. (db, schema,
        table) for postgres or (db, table) for mysql/mongo. A pattern matches
        the full dotted id or any right-anchored suffix of it, with shell
        wildcards, so 'pick_dispatch_queue', 'public.pick_dispatch_queue' and
        'oms_mkp_uat.public.pick_dispatch_queue' all exclude the same table,
        and 'oms_mkp_uat.public.*' excludes a whole schema. This protects
        target-owned tables (rows written on the target, not the source) from
        being deleted by a reconcile."""
        from fnmatch import fnmatch
        parts = [str(p) for p in parts if p not in (None, "")]
        cands = {".".join(parts[i:]) for i in range(len(parts))}
        return any(fnmatch(c, str(pat)) for pat in self.exclude for c in cands)


DEFAULT_PORTS = {
    "postgres": 5432, "mysql": 3306, "mssql": 1433,
    "mongodb": 27017, "redis": 6379, "kafka": 9092,
}


def _secret(val):
    """Resolve a secret reference so credentials need not sit in plaintext:
      env:NAME / ${NAME}   -> environment variable
      file:/path           -> file contents (trimmed; Docker/K8s secrets)
      vault:secret/db#key  -> Vault KV via VAULT_ADDR/VAULT_TOKEN (or the
                              vault CLI), read at load time
    A plain string is returned unchanged."""
    if not isinstance(val, str):
        return val
    if val.startswith("env:") or (val.startswith("${") and val.endswith("}")):
        name = val[4:] if val.startswith("env:") else val[2:-1]
        v = os.environ.get(name)
        if v is None:
            raise SystemExit(f"secret env var '{name}' is not set")
        return v
    if val.startswith("file:"):
        p = Path(val[5:]).expanduser()
        if not p.exists():
            raise SystemExit(f"secret file '{p}' not found")
        return p.read_text().strip()
    if val.startswith("vault:"):
        return _vault_read(val[6:])
    return val


def _vault_read(ref):
    path, _, key = ref.partition("#")
    key = key or "value"
    import json as _json
    import shutil
    import subprocess
    if shutil.which("vault"):
        p = subprocess.run(["vault", "kv", "get", "-format=json", path],
                           capture_output=True, text=True)
        if p.returncode == 0:
            data = _json.loads(p.stdout)["data"]["data"]
            if key in data:
                return data[key]
    raise SystemExit(f"could not read vault secret '{ref}'"
                     " (need vault CLI + VAULT_ADDR/VAULT_TOKEN)")


def _endpoint(engine, raw):
    raw = raw or {}
    extra = {k: v for k, v in raw.items()
             if k not in ("host", "port", "user", "password")}
    nested = extra.pop("options", None)
    if isinstance(nested, dict):
        extra.update(nested)
    return Endpoint(
        host=_secret(raw.get("host", "")),
        port=int(raw.get("port") or DEFAULT_PORTS.get(engine, 0)),
        user=_secret(raw.get("user", "")),
        password=str(_secret(raw.get("password", ""))),
        options=extra,
    )


def load_hops(path=None):
    path = Path(path or CONF)
    if not path.exists():
        raise SystemExit(f"missing config {path}, copy conf/hops.example.yaml")
    data = yaml.safe_load(path.read_text()) or {}
    hops = {}
    for name, raw in (data.get("hops") or {}).items():
        engine = raw.get("engine", "postgres")
        hops[name] = Hop(
            name=name,
            engine=engine,
            source=_endpoint(engine, raw.get("source")),
            target=_endpoint(engine, raw.get("target")),
            databases=raw.get("databases") or [],
            exclude=raw.get("exclude") or [],
            service=raw.get("service", ""),
            big_rows=int(raw.get("big_rows", 5_000_000)),
            slice=int(raw.get("slice", 1_000_000)),
            workers=int(raw.get("workers", 4)),
            options=raw.get("options") or {},
            db_map=raw.get("db_map") or {},
        )
    return hops


def get_hop(name):
    hops = load_hops()
    if name not in hops:
        raise SystemExit(f"unknown hop {name}, have: {', '.join(hops) or 'none'}")
    return hops[name]
