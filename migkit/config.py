import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

BASE = Path(__file__).resolve().parent.parent


def _find(env, name, default_dir):
    """Where a runtime file lives, for a copy installed anywhere.

    Resolved against the place the work is happening, not the place the code
    was installed to. Installed with pip, BASE is site-packages, and looking
    for the config there would mean editing files inside an installed package.
    Order: the explicit variable, the working directory, the user's config
    directory, then the source tree for a checkout run in place.
    """
    override = os.environ.get(env)
    if override:
        return Path(override).expanduser()
    here = Path.cwd() / default_dir / name
    if here.exists():
        return here
    user = Path(os.environ.get("XDG_CONFIG_HOME",
                               Path.home() / ".config")) / "migkit" / name
    if user.exists():
        return user
    in_place = BASE / default_dir / name
    if in_place.exists():
        return in_place
    # Nothing yet. Point at the user's config directory rather than at
    # site-packages: an installed copy must never ask anyone to create files
    # inside the installed package.
    return user


def user_config_path(name="hops.yaml"):
    return (Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
            / "migkit" / name)


def _reports_root():
    """Where reports get written.

    An installed copy must not write inside its own package directory, so an
    installed run puts them under the working directory. A source checkout
    keeps them at the repo root, which is where they have always been.
    """
    override = os.environ.get("MIGKIT_REPORTS")
    if override:
        return Path(override).expanduser()
    installed = "site-packages" in BASE.parts or "dist-packages" in BASE.parts
    return (Path.cwd() if installed else BASE) / "reports"


CONF = _find("MIGKIT_CONF", "hops.yaml", "conf")
REPORTS = _reports_root()


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
    mapping: dict = field(default_factory=dict)

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

    @staticmethod
    def _ids(*parts):
        """Every name an object answers to, longest first.

        The same right-anchored suffix rule `excluded` uses, so one idea of
        "which table is this" serves the deny list and the mapping instead
        of two that can disagree.
        """
        parts = [str(p) for p in parts if p not in (None, "")]
        return [".".join(parts[i:]) for i in range(len(parts))]

    def table_map(self):
        """`{source id: target id}` from the hop's `mapping.tables`."""
        return dict((self.mapping or {}).get("tables") or {})

    def target_table(self, *parts):
        """What this source table is called on the target.

        Identity when unmapped, which is the answer for almost every table
        - the same shape as `target_db`, and for the same reason: a rename
        is a fact about the hop, not something each caller should carry.
        """
        rules = self.table_map()
        for ident in self._ids(*parts):
            if ident in rules:
                return str(rules[ident])
        return ".".join(str(p) for p in parts if p not in (None, ""))

    def row_filter(self, *parts):
        """The predicate that decides which rows of this table move, or None.

        Returned as written. It is pushed into the mover's own flag and
        into the checksum's `WHERE`, so a filtered load is compared against
        the same filter rather than against the whole source - which is the
        half DMS leaves out, and the reason a filtered target reads as
        missing rows there.
        """
        rules = (self.mapping or {}).get("where") or {}
        for ident in self._ids(*parts):
            if ident in rules:
                return str(rules[ident])
        return None

    def ambiguous_mapping(self):
        """Renames that would land two source tables on one target.

        Refused rather than resolved: whichever copy ran second would
        overwrite the first, and the verification would then compare one
        source against a target holding the other. `hetero.match_tables`
        already refuses an ambiguous *pair* for the same reason.
        """
        seen = {}
        for src, dst in sorted(self.table_map().items()):
            seen.setdefault(str(dst), []).append(src)
        return {dst: srcs for dst, srcs in seen.items() if len(srcs) > 1}

    def unused_mapping(self, source_ids):
        """Mapping keys that matched nothing on the source.

        A rule that matches nothing is how a table quietly fails to move:
        the operator believes it was renamed or filtered, and it was never
        considered at all. `source_ids` is what the source actually has.
        """
        known = set()
        for ident in source_ids:
            known.update(self._ids(*str(ident).split(".")))
        keys = set(self.table_map()) | set(
            ((self.mapping or {}).get("where") or {}))
        return sorted(k for k in keys if k not in known)


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
        raise SystemExit(
            f"no hop configuration yet ({path} does not exist).\n"
            "  create one:  migkit init\n"
            "  or point at an existing file:  MIGKIT_CONF=/path/to/hops.yaml")
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
            mapping=raw.get("mapping") or {},
        )
    return hops


def get_hop(name):
    hops = load_hops()
    if name not in hops:
        raise SystemExit(f"unknown hop {name}, have: {', '.join(hops) or 'none'}")
    hop = hops[name]
    # MIGKIT_EXCLUDE lets a caller skip databases/tables without editing hops.yaml,
    # so the run's config file stays the single place the operator changes.
    env = os.environ.get("MIGKIT_EXCLUDE", "")
    if env:
        hop.exclude = list(hop.exclude) + [p.strip() for p in env.split(",") if p.strip()]
    return hop
