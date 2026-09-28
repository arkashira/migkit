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


#: tables migkit keeps on a side for itself - a two-way tail's origin
#: marks (`twoway`) - never copied, compared or repaired as the
#: application's
MIGKIT_OWN = ("migkit_origin",)

#: engines whose objects are keys or topics an application names as it
#: likes, not tables and schemas another tool creates for its own state:
#: a key called `percona` is the application's
KEYED_ENGINES = ("redis", "kafka")


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

    #: a PostgreSQL connection's TLS from the endpoint's options, under
    #: libpq's own names: `sslmode` (verify-full checks the certificate and
    #: the name on it), `sslrootcert`, `sslcert`, `sslkey`, `sslcrl`. Left
    #: out, libpq prefers TLS where the server offers it and checks nothing
    #: - unless the server's certificate verifies, when it is checked
    #: (`tls.auto`)
    LIBPQ_TLS = ("sslmode", "sslrootcert", "sslcert", "sslkey", "sslcrl")

    def libpq_tls(self):
        got = {k: str(self.options[k]) for k in self.LIBPQ_TLS
               if self.options.get(k)}
        if "sslmode" not in got:
            from . import tls
            found = tls.auto(self, "postgres", got.get("sslrootcert"))
            if found:
                got.update(sslmode="verify-full",
                           sslrootcert=got.get("sslrootcert") or found)
        return got

    def libpq_env(self):
        """The same, as the environment every program on libpq reads."""
        return {"PG" + k.upper(): v for k, v in self.libpq_tls().items()}

    def mysql_tls(self):
        """A MySQL connection's TLS from the endpoint's options: `ssl_ca`
        (the authority that signed the server's certificate, which is then
        checked, and the name on it unless `ssl_verify_identity: false`),
        `ssl_cert` and `ssl_key` (a client certificate), or `ssl: true` for
        TLS with nothing checked. Left out, the client takes TLS where the
        server offers it and checks nothing (measured on 8.4) - unless the
        server's certificate verifies, when it is checked (`tls.auto`)."""
        o = self.options
        ca = o.get("ssl_ca")
        if not ca and not o.get("ssl"):
            from . import tls
            ca = tls.auto(self, "mysql")
        if ca:
            out = {"ssl_ca": str(ca), "ssl_verify_cert": True,
                   "ssl_verify_identity": bool(o.get("ssl_verify_identity",
                                                     True))}
            out.update({k: str(o[k]) for k in ("ssl_cert", "ssl_key")
                        if o.get(k)})
            return out
        if o.get("ssl"):
            import ssl
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            if o.get("ssl_cert"):
                ctx.load_cert_chain(str(o["ssl_cert"]),
                                    str(o.get("ssl_key") or "") or None)
            return {"ssl": ctx}
        return {}

    # ---- TLS of the engines that had none of their own -----------------
    #: The same names on MongoDB, Redis and Cassandra: `tls: true` (the
    #: certificate checked against the system's authorities),
    #: `tls_ca_file` (against these), `tls_cert_file` and `tls_key_file`
    #: (a client certificate; MongoDB's in one file, `tls_cert_file`),
    #: `tls_crl_file` (MongoDB), `tls_insecure: true` (encrypted, nothing
    #: checked - which `assess` fails on). Left out, as before - unless the
    #: server's certificate verifies, when it is checked (`tls.auto`).

    def _tls_wanted(self, how="direct"):
        """(on, authorities, insecure) from the options, or from asking the
        server where they say nothing."""
        o = self.options
        insecure = bool(o.get("tls_insecure"))
        ca = o.get("tls_ca_file")
        if o.get("tls") or ca or insecure or o.get("tls_cert_file"):
            return True, (str(ca) if ca else None), insecure
        from . import tls
        found = tls.auto(self, how)
        return bool(found), found, False

    def mongo_tls(self):
        """The URI options of a MongoDB connection's TLS, for the driver and
        for the programs that take a URI alike. An operator's own
        `uri_options` that speak of TLS are theirs, and left alone."""
        extra = str(self.options.get("uri_options") or "").lower()
        if "tls=" in extra or "ssl=" in extra:
            return {}
        hosts = self.options.get("hosts")
        if hosts and not self.host:
            # a replica set's first member answers for its certificate
            first = str(hosts).split(",")[0].strip()
            name, sep, port = first.rpartition(":")
            if not sep or not port.isdigit():
                name, port = first, "27017"
            probe = Endpoint(host=name, port=int(port),
                             options=self.options)
            on, ca, insecure = probe._tls_wanted()
        else:
            on, ca, insecure = self._tls_wanted()
        if not on:
            return {}
        out = {"tls": "true"}
        if ca:
            out["tlsCAFile"] = ca
        if self.options.get("tls_cert_file"):
            out["tlsCertificateKeyFile"] = str(self.options["tls_cert_file"])
        if self.options.get("tls_crl_file"):
            out["tlsCRLFile"] = str(self.options["tls_crl_file"])
        if insecure:
            out["tlsInsecure"] = "true"
        return out

    def redis_tls(self):
        """The keyword arguments of a Redis connection's TLS, as redis-py's
        client and its cluster client take them."""
        on, ca, insecure = self._tls_wanted()
        if not on:
            return {}
        out = {"ssl": True, "ssl_cert_reqs": "none" if insecure
               else "required", "ssl_check_hostname": not insecure}
        if ca:
            out["ssl_ca_certs"] = ca
        for k, v in (("tls_cert_file", "ssl_certfile"),
                     ("tls_key_file", "ssl_keyfile")):
            if self.options.get(k):
                out[v] = str(self.options[k])
        return out

    def cassandra_tls(self):
        """The driver's `ssl_context` and `ssl_options` for a Cassandra
        connection's TLS, or {}. The name on the certificate is checked
        with it unless `tls_insecure`."""
        on, ca, insecure = self._tls_wanted()
        if not on:
            return {}
        import ssl
        ctx = ssl.create_default_context(cafile=ca) if ca \
            else ssl.create_default_context()
        if insecure:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        if self.options.get("tls_cert_file"):
            ctx.load_cert_chain(str(self.options["tls_cert_file"]),
                                str(self.options.get("tls_key_file") or "")
                                or None)
        return {"ssl_context": ctx,
                "ssl_options": {"server_hostname": self.host}}

    def mssql_tls(self):
        """A SQL Server connection's encryption, as the driver names it:
        `encrypt: require | request | off`. The driver migkit reads SQL
        Server through encrypts the login and the rows where asked and
        checks no certificate; `assess` says so."""
        raw = self.options.get("encrypt")
        enc = {True: "require", False: "off"}.get(raw) if isinstance(
            raw, bool) else str(raw or "").strip().lower()
        if enc in ("true", "yes", "on", "strict"):
            enc = "require"
        if enc in ("false", "no"):
            enc = "off"
        return {"encryption": enc} if enc in ("require", "request",
                                              "off") else {}


class IamEndpoint(Endpoint):
    """An endpoint that signs in with a token the cloud signs instead of a
    password (backlog 38): `auth: aws_iam` in its options, for RDS and
    Aurora. A token is good for 15 minutes to open connections; it is
    made again when it is 10 minutes old, so a run longer than a token
    does not start failing to connect halfway. The configured password,
    if any, is not used. The region comes from `aws_region`, or from the
    AWS configuration of this machine. `aws_role_arn` signs as a role in
    another account, assumed for each token - the cross-account pattern
    the managed services use."""

    _made = 0.0
    _token = ""

    @property
    def password(self):
        import time
        if not self._token or time.time() - self._made > 600:
            self._token = _aws_client(
                "rds", self.options.get("aws_region"),
                self.options.get("aws_role_arn")).generate_db_auth_token(
                    DBHostname=self.host, Port=int(self.port),
                    DBUsername=self.user)
            self._made = time.time()
        return self._token

    @password.setter
    def password(self, value):
        pass


def _aws_client(service, region=None, role=None):
    try:
        import boto3
    except ImportError:
        raise SystemExit("pip install boto3 for AWS sign-in and secrets")
    where = {"region_name": region} if region else {}
    if not role:
        return boto3.client(service, **where)
    creds = boto3.client("sts", **where).assume_role(
        RoleArn=role, RoleSessionName="migkit")["Credentials"]
    return boto3.client(service, aws_access_key_id=creds["AccessKeyId"],
                        aws_secret_access_key=creds["SecretAccessKey"],
                        aws_session_token=creds["SessionToken"], **where)


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
    #: whether the hop set `workers` itself - then it is a ceiling the
    #: sizing never passes (`sizing.estimate`); otherwise migkit works the
    #: number out. A hop read from the configuration without `workers` is
    #: not set; one made in code with a number is
    workers_set: bool = True
    #: the most the pace of a move may reach, set by `sizing.fit`
    workers_most: int = 0

    def reversed(self):
        """The same hop run the other way: the target as the source, the
        database names mapped back. Its name is its own, so its
        replication objects and reports are not the forward hop's. Used
        only where the hop asks for a stream back (`reverse`,
        `topology`)."""
        import dataclasses
        return dataclasses.replace(
            self, name=f"{self.name}-reverse", source=self.target,
            target=self.source,
            databases=[self.target_db(d) for d in (self.databases or [])],
            db_map={v: k for k, v in (self.db_map or {}).items()},
            options={k: v for k, v in (self.options or {}).items()
                     if k not in ("reverse", "topology", "protect_target")})

    def report_dir(self, db=""):
        d = REPORTS / self.name / db if db else REPORTS / self.name
        d.mkdir(parents=True, exist_ok=True)
        # the files under it that hold values encrypted, where the hop
        # names who may read them (`evidence`)
        from . import evidence
        return evidence.report_path(self, d)

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
        if parts and parts[-1] in MIGKIT_OWN:
            # migkit's own bookkeeping on a side, never the application's
            return True
        if self.engine not in KEYED_ENGINES:
            # another tool's bookkeeping - a schema a replicator keeps on
            # the source, a loader's load table: its state, not the
            # application's rows (`leftovers.bookkeeping`)
            from .leftovers import bookkeeping
            if bookkeeping(*parts):
                return True
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

    def column_rules(self, *parts):
        """What the hop's `mapping.columns` says about this table's columns:
        `keep` (only these), `drop` (all but these) and `rename` (source name
        to target name), matched by the same suffix rule as every other
        name in the hop. Empty when nothing is said."""
        rules = (self.mapping or {}).get("columns") or {}
        for ident in self._ids(*parts):
            if ident in rules:
                got = rules[ident] or {}
                return {"keep": [str(c) for c in got.get("keep") or []],
                        "drop": [str(c) for c in got.get("drop") or []],
                        "rename": {str(a): str(b) for a, b in
                                   (got.get("rename") or {}).items()}}
        return {}

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
      aws-sm:<id>[#key]    -> AWS Secrets Manager; `key` picks a field of a
                              JSON secret (RDS's own secrets are JSON)
      gcp-sm:projects/<p>/secrets/<s>[/versions/<v>]
                           -> Google Secret Manager (latest by default)
      azure-kv:https://<vault>.vault.azure.net/secrets/<name>
                           -> Azure Key Vault
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
    if val.startswith("aws-sm:"):
        return _aws_secret(val[7:])
    if val.startswith("gcp-sm:"):
        return _gcp_secret(val[7:])
    if val.startswith("azure-kv:"):
        return _azure_secret(val[9:])
    return val


def _aws_secret(ref):
    import json as _json
    sid, _, key = ref.partition("#")
    # an ARN names its region; a bare name uses this machine's
    region = sid.split(":")[3] if sid.startswith("arn:") else None
    try:
        got = _aws_client("secretsmanager", region).get_secret_value(
            SecretId=sid)
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001 - said, with the secret's name
        raise SystemExit(f"could not read AWS secret '{sid}':"
                         f" {type(e).__name__}")
    text = got.get("SecretString")
    if text is None:
        raise SystemExit(f"AWS secret '{sid}' holds no text")
    if not key:
        return text
    try:
        return str(_json.loads(text)[key])
    except (ValueError, KeyError, TypeError):
        raise SystemExit(f"AWS secret '{sid}' has no field '{key}'")


def _gcp_secret(ref):
    try:
        from google.cloud import secretmanager
    except ImportError:
        raise SystemExit("pip install google-cloud-secret-manager for"
                         " gcp-sm: secrets")
    name = ref if "/versions/" in ref else ref + "/versions/latest"
    got = secretmanager.SecretManagerServiceClient().access_secret_version(
        name=name)
    return got.payload.data.decode()


def _azure_secret(ref):
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError:
        raise SystemExit("pip install azure-identity azure-keyvault-secrets"
                         " for azure-kv: secrets")
    vault, _, name = ref.partition("/secrets/")
    return SecretClient(vault_url=vault, credential=DefaultAzureCredential()
                        ).get_secret(name.split("/")[0]).value


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
    auth = str(extra.get("auth", "") or "").lower()
    if auth not in ("", "password", "aws_iam"):
        raise SystemExit(f"auth: {auth} - password (the default) or"
                         " aws_iam")
    return (IamEndpoint if auth == "aws_iam" else Endpoint)(
        host=_secret(raw.get("host", "")),
        port=int(raw.get("port") or DEFAULT_PORTS.get(engine, 0)),
        user=_secret(raw.get("user", "")),
        password=("" if auth == "aws_iam"
                  else str(_secret(raw.get("password", "")))),
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
            workers_set="workers" in raw,
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
