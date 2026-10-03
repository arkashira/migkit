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
#: marks (`twoway`), a cutover's drain marks (`drainmark`) - never copied,
#: compared or repaired as the application's
MIGKIT_OWN = ("migkit_origin", "migkit_cutover_mark")

#: engines whose objects are keys or topics an application names as it
#: likes, not tables and schemas another tool creates for its own state:
#: a key called `percona` is the application's
KEYED_ENGINES = ("redis", "kafka")


#: engines whose unquoted names fold to one case - Oracle, Db2 and
#: Snowflake to upper, SQL Server, ASE and Teradata compare without case -
#: so `orders` written in a hop names the table the server calls ORDERS
FOLDS_CASE = ("oracle", "db2", "db2i", "db2zos", "snowflake", "mssql",
              "ase", "teradata", "sapodp")


def name_matches(name, pattern, fold=False):
    """Whether a dotted name is one a hop's pattern names: a shell pattern
    read the same on every machine (`fnmatchcase` - `fnmatch` folds case
    on some systems and not on others, so one hop took a table on one
    machine and left it on another), without case where the engine's
    names have none (`FOLDS_CASE`)."""
    from fnmatch import fnmatchcase
    name, pattern = str(name), str(pattern)
    if fnmatchcase(name, pattern):
        return True
    return fold and fnmatchcase(name.casefold(), pattern.casefold())


#: the changes a tail can carry, in the order they are said
OPS = ("insert", "update", "delete")
_OP_WORDS = {"i": "insert", "c": "insert", "insert": "insert",
             "create": "insert", "u": "update", "update": "update",
             "d": "delete", "delete": "delete"}


def parse_ops(value):
    """`mapping.ops`' value - a list or a comma-separated string of insert,
    update and delete (or i, u, d) - as the set it names; SystemExit for
    a word that is none of them."""
    words = value if isinstance(value, (list, tuple, set, frozenset)) \
        else str(value or "").replace(";", ",").split(",")
    got = set()
    for w in words:
        w = str(w).strip().lower()
        if not w:
            continue
        if w not in _OP_WORDS:
            raise SystemExit(f"{w!r} is not a change a tail carries -"
                             " insert, update or delete")
        got.add(_OP_WORDS[w])
    return frozenset(got)


#: the kinds of object beside the tables a hop's `objects` says it
#: carries, and the words each is written with
OBJECT_KINDS = {"views": ("views", "view", "matviews"),
                "functions": ("functions", "function"),
                "procedures": ("procedures", "procedure"),
                "triggers": ("triggers", "trigger"),
                "events": ("events", "event"),
                "sequences": ("sequences", "sequence"),
                "roles": ("roles", "role", "users", "accounts", "grants")}
#: words that name more than one kind at once
OBJECT_GROUPS = {"routines": ("functions", "procedures"),
                 "routine": ("functions", "procedures")}


def object_kind(word):
    """The kind of `objects` a word names - `view`, `materialized view`,
    `function`, `trigger` as the inventories say them - or None for a
    word that is not one (a table, an index: they follow the table)."""
    w = str(word or "").strip().lower().replace("_", " ")
    if w in ("materialized view", "matview", "mview"):
        return "views"
    for kind, words in OBJECT_KINDS.items():
        if w in words:
            return kind
    return None


def objects_of(hop):
    """{kind: True | False | [names]} of the hop's `objects`, each kind
    under its one name (`OBJECT_KINDS`; `routines` says functions and
    procedures at once); a kind not said is True."""
    raw = getattr(hop, "objects", None) or {}
    words = {w: (k,) for k, ws in OBJECT_KINDS.items() for w in ws}
    words.update(OBJECT_GROUPS)
    out = {}
    for k, v in raw.items():
        kinds = words.get(str(k).lower())
        if kinds is None:
            raise SystemExit(f"objects: {k} is not a kind of object - "
                             + ", ".join(list(OBJECT_KINDS) + ["routines"]))
        if v in (True, None) or str(v).lower() in ("all", "true", "yes"):
            got = True
        elif v is False or str(v).lower() in ("none", "false", "no"):
            got = False
        else:
            got = list(dict.fromkeys(scope_patterns(v)))
        for kind in kinds:
            out[kind] = got
    return out


def scope_patterns(value):
    """A hop's `include` or `exclude` as the dotted patterns every reader
    matches: a list as written, or the nested form that reads like the
    databases it names -

        include:
          billing: all                  # the whole database
          logistics: [orders, parcels]  # some of its tables
          shop:
            public: [orders, "lines_*"] # a schema's tables
            audit: all                  # a whole schema

    one shape for every reader, so the two forms cannot be read apart."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value]
    if not isinstance(value, dict):
        raise SystemExit(f"{value!r}: a list of names, or databases each"
                         " with all, a list of tables, or its schemas")
    out = []

    def walk(prefix, got):
        if got in (True, None, "all", "*"):
            out.append(f"{prefix}.*")
        elif isinstance(got, str):
            out.append(f"{prefix}.{got}")
        elif isinstance(got, (list, tuple)):
            out.extend(f"{prefix}.{t}" for t in got)
        elif isinstance(got, dict):
            for k, v in got.items():
                walk(f"{prefix}.{k}", v)
        else:
            raise SystemExit(f"{prefix}: {got!r} - all, a table, a list of"
                             " tables, or its schemas")
    for k, v in value.items():
        if v is False:
            continue
        walk(str(k), v)
    return out


def scoped(hop):
    """Whether the hop leaves anything out by name - an `exclude`, or an
    `include` that takes less than everything. A way that carries a whole
    database, a whole server or a whole keyspace serves only a hop that
    is not; the one `excluded` answers which objects."""
    if getattr(hop, "exclude", None) or getattr(hop, "include", None):
        return True
    try:
        return any(v is not True for v in objects_of(hop).values())
    except SystemExit:
        return False


@dataclass
class Endpoint:
    host: str = ""
    port: int = 0
    user: str = ""
    password: str = ""
    options: dict = field(default_factory=dict)

    def configured(self):
        return bool(self.host or self.options.get("hosts")
                    or self.options.get("path") or self.options.get("url")
                    or self.options.get("program"))

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
        return self._pg_env(self.libpq_tls())

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

    # ---- the side as an address, and its TLS in every form ---------------
    #: the connections whose TLS a URL of each scheme carries
    URL_FAMILY = {"postgres": "postgres", "postgresql": "postgres",
                  "pgsql": "postgres", "redshift": "postgres",
                  "cockroachdb": "postgres", "dsql": "postgres",
                  "mysql": "mysql", "mariadb": "mysql", "tidb": "mysql",
                  "mongodb": "mongodb"}

    def held_secrets(self):
        """The secrets this side holds now, found without asking anything
        (a keychain, a cloud): what `redact` hides."""
        return [self.password] if self.password else []

    def _netloc(self):
        host = str(self.host or "")
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return host + (f":{self.port}" if self.port else "")

    def url(self, scheme, db="", tls=True):
        """This side as `scheme://user:password@host:port/db?...`, built
        one way for every program and driver that takes an address.

        The user, the password and the database are escaped whole
        (`quote(..., safe="")`): measured with the user `app@corp` and a
        password holding `/:@#`, fifteen builders escaped them four ways,
        and one parsed the host as `corp`. An IPv6 address is bracketed; a
        PostgreSQL socket directory goes in the `host` parameter; a
        MongoDB side's `hosts` and `uri_options` are its own. With `tls`,
        the query carries the side's resolved TLS in the form the scheme's
        own clients read (`tls_forms`) - a scheme that has none refuses
        rather than drop it, since a URL that quietly loses the TLS the
        hop asks for is what `doctor` could not see."""
        from urllib.parse import quote
        family = self.URL_FAMILY.get(scheme)
        if tls and family is None:
            raise ValueError(f"a {scheme} address carries no TLS in its"
                             " query: carry it the way its client takes it")
        query = []
        where = self._netloc()
        if family == "mongodb":
            where = str(self.options.get("hosts") or "") or where
            extra = str(self.options.get("uri_options") or "")
            if extra:
                query.append(extra)
        elif family == "postgres" and str(self.host or "").startswith("/"):
            query.append("host=" + quote(str(self.host), safe=""))
            if self.port:
                query.append(f"port={int(self.port)}")
            where = ""
        if tls:
            got = self.tls_forms(family)["query"]
            if got:
                query.append(got)
        auth = ""
        if self.user or self.password:
            auth = quote(str(self.user or ""), safe="")
            if self.password:
                auth += ":" + quote(str(self.password), safe="")
            auth += "@"
        path = "/" + quote(str(db), safe="") if db else "/"
        return (f"{scheme}://{auth}{where}{path}"
                + ("?" + "&".join(query) if query else ""))

    @staticmethod
    def _pg_env(got):
        return {"PG" + k.upper(): v for k, v in got.items()}

    def tls_forms(self, family):
        """The side's TLS - its options, or where they say nothing, what
        asking the server found (`tls.auto`) - decided once and rendered
        in every form a connection of `family` is made in, so no path
        connects to it another way than the rest.

        The hop's TLS used to reach migkit's own connections and be
        dropped on the main data paths while `doctor` said verified: the
        streaming copy, both native replications, the clone, the page
        copy, the bulk programs and the change pipeline (audit
        2026-10-01, SEC2).

        PostgreSQL: `libpq` (the connection's keywords), `env` (the
        variables every program on libpq reads), `query` (a URL's),
        `far` (what a server that dials this side asks of it - a
        subscription's or a foreign server's connection string: an
        authority's file is on this machine, not that server's, so
        `verify-*` is carried as `require`), `jdbc` and `connector` (a
        Java client's and the change pipeline's fields, the files as
        named here). MySQL: `driver` (the driver's keywords), `client`
        (the command-line clients' flags), `dump` (the dump and load
        programs': they know two modes, and a named authority is held to
        the server's name too), `uri` and `query` (MySQL's URI-like
        address), `source` and `master` (a replica's options for the
        source, in MySQL's own and the older spelling: encrypted where
        the side is, the certificate not checked for the same reason as
        `far`, and the server's public key fetched only where it is
        not), `clone` (a clone's `REQUIRE SSL`), `connector` (the change
        pipeline's mode: it verifies through a Java key store, which an
        authority's file is not, so it is carried as `required`).
        MongoDB, Redis, Cassandra, SQL Server: `driver`, and MongoDB's
        `query`. `mode` in each is the decision in the engine's words."""
        from urllib.parse import urlencode
        if family == "postgres":
            got = self.libpq_tls()
            mode = got.get("sslmode") or "prefer"
            far = ({"sslmode": "require"}
                   if mode in ("verify-ca", "verify-full")
                   else {"sslmode": mode} if got.get("sslmode") else {})
            java = {k: v for k, v in got.items() if k != "sslcrl"}
            return {"mode": mode, "libpq": got, "env": self._pg_env(got),
                    "query": urlencode(got or {"sslmode": "prefer"}),
                    "far": far, "jdbc": java,
                    "connector": {"database." + k: v
                                  for k, v in java.items()}}
        if family == "mysql":
            got = self.mysql_tls()
            ca = got.get("ssl_ca", "")
            mode = ("VERIFY_IDENTITY" if got.get("ssl_verify_identity")
                    else "VERIFY_CA") if ca else ("REQUIRED" if got else "")
            uri, dump = {}, []
            if mode:
                cert = str(self.options.get("ssl_cert") or "")
                key = str(self.options.get("ssl_key") or "")
                uri = {"ssl-mode": mode, "ssl-ca": ca, "ssl-cert": cert,
                       "ssl-key": key}
                uri = {k: v for k, v in uri.items() if v}
                dump = (["--ssl-mode", "VERIFY_IDENTITY", "--ca", ca] if ca
                        else ["--ssl"])
                dump += (["--cert", cert] if cert else []) + (
                    ["--key", key] if key else [])
            return {"mode": mode or "PREFERRED", "driver": got,
                    "client": [f"--{k}={v}" for k, v in uri.items()],
                    "dump": dump, "uri": uri, "query": urlencode(uri),
                    "source": ("SOURCE_SSL = 1" if mode
                               else "GET_SOURCE_PUBLIC_KEY = 1"),
                    "master": "MASTER_SSL = 1" if mode else "",
                    "clone": "REQUIRE SSL" if mode else "",
                    "connector": ({"database.ssl.mode": "required"} if mode
                                  else {})}
        own = {"mongodb": self.mongo_tls, "redis": self.redis_tls,
               "cassandra": self.cassandra_tls, "mssql": self.mssql_tls}
        if family not in own:
            raise ValueError(f"no TLS forms for {family!r}")
        got = own[family]()
        out = {"mode": "on" if got else "", "driver": got}
        if family == "mongodb":
            out["query"] = urlencode(got)
        return out


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

    def held_secrets(self):
        """The token made last, if one has been: a new one is not signed
        just to be hidden."""
        return [self._token] if self._token else []


#: the keychain entries migkit keeps passwords under: service `migkit`,
#: one entry a hop side (`keychain_name`)
KEYCHAIN = "migkit"


def keychain_name(hop, side):
    return f"{hop}.{side}"


def keychain_read(name, strict=True):
    """The password this machine's keychain holds under `name`. `strict`
    for a `keyring:` reference, which names an entry that must be there;
    not for a side given as an address, which may sign in without one."""
    try:
        import keyring
        got = keyring.get_password(KEYCHAIN, name)
    except Exception as e:  # noqa: BLE001 - no keychain on this machine
        if strict:
            raise SystemExit(f"this machine's keychain could not be read"
                             f" for {KEYCHAIN}/{name}: {type(e).__name__}"
                             f" {e}".rstrip())
        return ""
    if got is None and strict:
        raise SystemExit(f"nothing is kept in this machine's keychain as"
                         f" {KEYCHAIN}/{name} - `migkit init` keeps it"
                         " there, or write another reference")
    return got or ""


def keychain_write(name, secret):
    import keyring
    keyring.set_password(KEYCHAIN, name, secret)


class KeychainEndpoint(Endpoint):
    """A side written as an address with no password: its password is the
    one this machine's keychain keeps for the hop and side, read the first
    time a connection needs it - so a hop that never connects never asks
    the keychain, and one whose server wants no password finds none."""

    keychain = ""
    _got = None

    @property
    def password(self):
        if self._got is None:
            self._got = keychain_read(self.keychain, strict=False)
        return self._got

    @password.setter
    def password(self, value):
        pass

    def held_secrets(self):
        """The password once read: the keychain is not asked just for it
        to be hidden."""
        return [self._got] if self._got else []


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
    #: what the hop takes, where it takes less than everything - the same
    #: patterns `exclude` reads; empty takes everything (`included`)
    include: list = field(default_factory=list)
    #: the kinds of object beside the tables it carries (`takes_object`)
    objects: dict = field(default_factory=dict)
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
    #: what the hop is for (`hopkeys.GOALS`), "" where it does not say -
    #: its options are already under the hop's own by the time it is built
    goal: str = ""

    def reversed(self):
        """The same hop run the other way: the target as the source, the
        database names mapped back. Its name is its own, so its
        replication objects and reports are not the forward hop's. Used
        only where the hop asks for a stream back (`reverse`,
        `topology`)."""
        import dataclasses
        mapping = self._mapping_on_target()
        return dataclasses.replace(
            self, name=f"{self.name}-reverse", source=self.target,
            target=self.source,
            databases=[self.target_db(d) for d in (self.databases or [])],
            db_map={v: k for k, v in (self.db_map or {}).items()},
            include=[self._on_target(p) for p in self.include or []],
            exclude=[self._on_target(p) for p in self.exclude or []],
            objects={k: ([self._on_target(p) for p in v]
                         if isinstance(v, list) else v)
                     for k, v in objects_of(self).items()},
            mapping=mapping,
            # never `physical`: the way back would empty the old source's
            # data directory to fill it with the new primary's pages
            options={k: v for k, v in (self.options or {}).items()
                     if k not in ("reverse", "topology", "protect_target",
                                  "physical")})

    def _mapping_on_target(self):
        """The mapping as the way back reads it: every rule keyed by the
        table's name on the target, a rename turned round, a column renamed
        named as the target has it; a column the way there dropped is not
        on the target to carry back, and a row filter is asked of the
        target's rows under their target names."""
        mapping = dict(self.mapping or {})
        renames = {str(k): str(v) for k, v in
                   (mapping.get("tables") or {}).items()}

        def key(k):
            k = str(k)
            if k in renames:
                head = k.split(".")[0] if k.count(".") > \
                    renames[k].count(".") else ""
                got = renames[k]
                return self._on_target(f"{head}.{got}") if head else got
            return self._on_target(k)
        out = {}
        if renames:
            out["tables"] = {key(k): k.split(".", k.count(".") -
                                             renames[k].count("."))[-1]
                             if k.count(".") > renames[k].count(".") else k
                             for k in renames}
        cols = {}
        for k, rule in (mapping.get("columns") or {}).items():
            rule = dict(rule or {})
            ren = {str(a): str(b) for a, b in
                   (rule.get("rename") or {}).items()}
            back = {}
            if rule.get("keep"):
                back["keep"] = [ren.get(str(c), str(c))
                                for c in rule["keep"]]
            if ren:
                back["rename"] = {b: a for a, b in ren.items()}
            if back:
                cols[key(k)] = back
        if cols:
            out["columns"] = cols
        for part in ("where", "tolerance", "ops"):
            if mapping.get(part):
                out[part] = {key(k): v for k, v in mapping[part].items()}
        return out

    def _on_target(self, pattern):
        """A scope pattern as the target names what it names: a leading
        database the hop maps, written as the target's database - so the
        way back takes what the way there took."""
        head, dot, rest = str(pattern).partition(".")
        if dot and head in (self.db_map or {}):
            return f"{self.db_map[head]}.{rest}"
        return str(pattern)

    def scope_fingerprint(self):
        """One short digest of everything that decides what the hop takes
        and how each table is read: its databases and their target names,
        include, exclude and the whole mapping. Kept beside every answer,
        position and checkpoint that is only good for the scope it was
        taken under, so one taken under another scope is never reused as
        if it were this one's (`scope_changed`)."""
        import hashlib
        import json
        got = {"databases": sorted(str(d) for d in self.databases or []),
               "db_map": {str(k): str(v) for k, v in
                          sorted((self.db_map or {}).items())},
               "include": sorted(str(p) for p in self.include or []),
               "exclude": sorted(str(p) for p in self.exclude or []),
               "objects": self.objects or {},
               "mapping": self.mapping or {}}
        text = json.dumps(got, sort_keys=True, default=str)
        return hashlib.sha256(text.encode()).hexdigest()[:16]

    def scope_changed(self, recorded):
        """Whether a record made under `recorded` (a fingerprint, or a dict
        holding one under `scope_fingerprint`) was taken under another
        scope than the hop's now. A record with none predates them and is
        taken as it always was."""
        if isinstance(recorded, dict):
            recorded = recorded.get("scope_fingerprint")
        return bool(recorded) and recorded != self.scope_fingerprint()

    def report_dir(self, db=""):
        d = REPORTS / self.name / db if db else REPORTS / self.name
        d.mkdir(parents=True, exist_ok=True)
        # the files under it that hold values encrypted, where the hop
        # names who may read them (`evidence`)
        from . import evidence
        return evidence.report_path(self, d)

    def target_db(self, db):
        """Target database name for a source db. Migrations often land in a
        differently-named db (e.g. shop_staging -> shop), so every dst-side
        connection resolves through this map (identity when unmapped).
        Local report paths stay keyed by the source name."""
        got = (self.db_map or {}).get(db)
        return got if got is not None else self.named("databases", db)

    def excluded(self, *parts):
        """True if an object matches any `exclude` pattern, so migkit neither
        verifies nor repairs it. Pass the dotted name parts, e.g. (db, schema,
        table) for postgres or (db, table) for mysql/mongo. A pattern matches
        the full dotted id or any right-anchored suffix of it, with shell
        wildcards, so 'job_queue', 'public.job_queue' and
        'orders_db.public.job_queue' all exclude the same table,
        and 'orders_db.public.*' excludes a whole schema. This protects
        target-owned tables (rows written on the target, not the source) from
        being deleted by a reconcile."""
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
        fold = self.folds_case()
        if any(name_matches(c, pat, fold) for pat in self.exclude
               for c in cands):
            return True
        return not self.included(*parts)

    def folds_case(self):
        """Whether the names the hop's patterns are matched against have
        no case (`FOLDS_CASE`)."""
        return self._names_engine() in FOLDS_CASE

    def included(self, *parts):
        """Whether the hop's `include` takes this object - True where it
        has no `include`, which takes everything.

        The same patterns `exclude` reads, matched the same way against
        the object's own name and every name it sits in: `orders`,
        `public.orders`, `appdb.public.orders`, `appdb.public.*` and
        `appdb` each take `appdb.public.orders`. `exclude` wins where both
        name it, so a task that took a schema but none of its log tables
        is said as the two lists it is. Asked through `excluded`, so every
        reader of the hop's scope - the check, the copy, the repair, the
        tail - reads one answer and none can verify a table the move
        left out, or leave out one it moved.

        A single name is a database being listed: it is opened unless
        every entry names another database first - `orders` and
        `public.orders` may be in any of them, `crm.public.orders` and,
        on MySQL, `crm.orders` only in `crm`. Where a name may hold a dot
        of its own (a collection, a topic) every database is opened. A
        database opened for nothing has no table taken, never a table
        too many."""
        if not self.include:
            return True
        names = self._names(*parts)
        if not names:
            return True
        pats = [str(p) for p in self.include if str(p).strip()]
        if not pats:
            return True
        fold = self.folds_case()
        for k in range(1, len(names) + 1):
            for ident in self._ids(*names[:k]):
                if any(name_matches(ident, p, fold) for p in pats):
                    return True
        if len(names) > 1:
            return False
        db = names[0]
        whole = self.whole_name()
        if not whole:
            return True
        for p in pats:
            head, _, rest = p.partition(".")
            if name_matches(db, head, fold) or p.count(".") + 1 < whole:
                return True
        return False

    def _names(self, *parts):
        """An object's name as its parts, split the one way names are
        (`util.split_table`): some callers pass a table as one part
        (`public.orders`), and a bare `include` entry must take it either
        way. A quoted name's dots are its own, and so are an unquoted
        name's on an engine without schemas - a collection `fs.chunks`, a
        topic `orders.created`."""
        from .util import _parts
        eng = self._names_engine()
        return [x for p in parts if p not in (None, "")
                for x in _parts(eng, str(p)) if x]

    def whole_name(self):
        """How many dotted parts name a table from its database down - 3
        where there are schemas, 2 on an engine whose names hold no dot -
        or 0 where a name may hold a dot of its own (a collection
        `fs.chunks`, a topic `orders.created`), so no dotted name is sure
        of the database it names."""
        from .util import NO_SCHEMA
        eng = self._names_engine()
        if eng in ("mysql", "tidb", "clickhouse", "cassandra"):
            return 2
        return 0 if eng in NO_SCHEMA else 3

    def _names_engine(self):
        """The engine whose names the hop's objects carry: the source's,
        a pair's own engine being neither side's."""
        from .util import _canonical
        eng = self.engine
        if eng == "hetero":
            eng = (self.options or {}).get("source_engine") or "mysql"
        return _canonical(eng)

    def unused_include(self, tables):
        """`include` entries that take none of these source tables, each
        `(database, table)` as the engine lists it: an entry that takes
        nothing is a table the operator believes is moved and checked,
        and nobody looks at it."""
        known = set()
        for db, table in tables:
            names = self._names(db, table)
            for k in range(1, len(names) + 1):
                known.update(self._ids(*names[:k]))
        fold = self.folds_case()
        return [str(p) for p in self.include
                if str(p).strip() and not any(name_matches(i, p, fold)
                                              for i in known)]

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
        parts = [str(p) for p in parts if p not in (None, "")]
        if self.name_policy("tables") or self.name_policy("schemas"):
            # the database a name opens with is the target's own
            # (`target_db`); the schema and the table take the hop's rule
            whole = self.whole_name() or 2
            lead = 1 if len(parts) >= whole else 0
            if parts:
                parts[-1] = self.named("tables", parts[-1])
            if len(parts) - lead >= 2:
                parts[-2] = self.named("schemas", parts[-2])
        return ".".join(parts)

    def name_policy(self, level):
        """The hop's rule for every name at one level (`databases`,
        `schemas`, `tables`) on the target, from `mapping.names`: `case`
        (lower or upper), `prefix`, `suffix`, `strip_prefix`,
        `strip_suffix` - as DMS's and the DTS services' name rules
        transform every name of a level at once. A rename the hop names
        itself (`mapping.tables`, `db_map`) is taken as written."""
        rules = ((self.mapping or {}).get("names") or {}).get(level)
        return dict(rules) if isinstance(rules, dict) else {}

    def named(self, level, name):
        """A name at `level` as the hop's rule writes it on the target."""
        rule = self.name_policy(level)
        if not rule:
            return name
        name = str(name)
        for key, cut in (("strip_prefix", lambda n, x: n[len(x):]
                          if n.startswith(x) else n),
                         ("strip_suffix", lambda n, x: n[:-len(x)]
                          if x and n.endswith(x) else n)):
            if rule.get(key):
                name = cut(name, str(rule[key]))
        case = str(rule.get("case") or "").lower()
        if case == "lower":
            name = name.lower()
        elif case == "upper":
            name = name.upper()
        return f"{rule.get('prefix') or ''}{name}{rule.get('suffix') or ''}"

    def target_table_in(self, db, *parts):
        """`target_table` for a table of database `db`, named without it.

        A rule may name the database - the only way to tell two
        databases' `orders` apart - so the database is part of what is
        matched; the answer is the rule's target, or the table's name as
        the caller gave it (under the hop's name rule, `named`) where no
        rule applies.
        """
        own = [str(p) for p in parts if p not in (None, "")]
        rules = self.table_map()
        for ident in self._ids(db, *own):
            if ident in rules:
                return str(rules[ident])
        return ".".join(str(self.target_table(db, *own)).split(".")[1:])

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
        got = self._where_rule(*parts)
        if got is None:
            return None
        return str(got.get("expr")) if isinstance(got, dict) else str(got)

    def row_filter_zone(self, *parts):
        """The time zone the row filter's times are read in - `where:
        {table: {expr: ..., tz: "+07:00"}}` - or None, where each server
        reads them in its own session's zone. A filter on a time column
        read in two zones takes other rows on each side; said once, it is
        read in that one zone by every reader."""
        got = self._where_rule(*parts)
        return str(got["tz"]) if isinstance(got, dict) and got.get("tz") \
            else None

    def _where_rule(self, *parts):
        rules = (self.mapping or {}).get("where") or {}
        for ident in self._ids(*parts):
            if ident in rules:
                return rules[ident]
        return None

    def takes_object(self, kind, *parts):
        """Whether the hop carries this object of a kind beside the tables
        - a view, a routine, a trigger, an event, a sequence, a role - by
        its `objects`: all of a kind (the default), none of it, or the
        ones named, matched as every other name in the hop. A kind the hop
        does not carry is out of its scope as an excluded table is: not
        moved, not compared, not repaired - and said, in the scope the
        evidence carries and before the move, so nobody takes its absence
        on the target for a pass. A table's own objects follow the table
        too (`excluded`)."""
        got = objects_of(self).get(object_kind(kind), True)
        if got is True:
            return True
        if got is False:
            return False
        ids = self._ids(*self._names(*parts))
        fold = self.folds_case()
        return any(name_matches(i, p, fold) for p in got for i in ids)

    def carries(self, *parts):
        """The changes the hop carries for this table: insert, update and
        delete, unless its `mapping.ops` says fewer - a move told to keep
        what the source deletes (an archive kept on the target), as DTS
        jobs and GoldenGate's IGNOREDELETES are. Keys are names or
        patterns (`logistics.*`, `*` for every table); the most particular
        one that names the table wins. The tail applies only these, and
        the check holds the table to what they leave: kept deletes let
        the target hold more rows, never fewer or other ones."""
        rules = (self.mapping or {}).get("ops") or {}
        if not rules:
            return frozenset(OPS)
        ids = self._ids(*parts)
        fold = self.folds_case()
        hits = [k for k in rules
                if any(name_matches(i, k, fold) for i in ids)]
        if not hits:
            return frozenset(OPS)
        best = max(hits, key=lambda k: (sum(c not in "*?[]" for c in
                                            str(k)), len(str(k))))
        return parse_ops(rules[best])

    def tolerance(self, *parts):
        """The percentage this table's aggregates may drift by when read a
        second way, from the hop's `mapping.tolerance`, or None.

        Only where the hop names it: a sum of doubles added in another
        order is not the same number, and a hop moving such a table says
        how far apart is still the same data. Nothing is allowed to drift
        by default, and a drift inside the tolerance is still said.
        """
        rules = (self.mapping or {}).get("tolerance") or {}
        for ident in self._ids(*parts):
            if ident in rules:
                return float(rules[ident])
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
            ((self.mapping or {}).get("where") or {})) | set(
            ((self.mapping or {}).get("tolerance") or {}))
        return sorted(k for k in keys if k not in known)


DEFAULT_PORTS = {
    "postgres": 5432, "mysql": 3306, "mssql": 1433,
    "mongodb": 27017, "redis": 6379, "kafka": 9092,
    "tidb": 4000, "cockroachdb": 26257, "dsql": 5432, "cosmosdb": 443,
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
      keyring:<name>       -> this machine's keychain (`migkit init` keeps
                              passwords there)
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
    if val.startswith("keyring:"):
        return keychain_read(val[8:])
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


def _endpoint(engine, raw, keychain=""):
    """A side from its long-form mapping. `keychain` names the entry a
    side written as an address signs in with where it carries no password
    of its own."""
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
    lazy = bool(keychain) and auth != "aws_iam" and not raw.get("password")
    ep = (IamEndpoint if auth == "aws_iam" else
          KeychainEndpoint if lazy else Endpoint)(
        host=_secret(raw.get("host", "")),
        port=int(raw.get("port") or DEFAULT_PORTS.get(engine, 0)),
        user=_secret(raw.get("user", "")),
        password=("" if auth == "aws_iam" or lazy
                  else str(_secret(raw.get("password", "")))),
        options=extra,
    )
    if lazy:
        ep.keychain = keychain
    return ep


def load_hops(path=None):
    path = Path(path or CONF)
    if not path.exists():
        raise SystemExit(
            f"no hop configuration yet ({path} does not exist).\n"
            "  create one:  migkit init\n"
            "  or point at an existing file:  MIGKIT_CONF=/path/to/hops.yaml")
    data = yaml.safe_load(path.read_text()) or {}
    from . import hopkeys
    errors = [p for p in hopkeys.check(data) if p.level == "error"]
    if errors:
        # before anything runs, and all of them at once: a run stopped on
        # the first only to meet the second is two runs for one fix
        raise SystemExit(
            f"{path} has {len(errors)} problem"
            f"{'s' if len(errors) > 1 else ''}, and nothing was run:\n"
            + "\n".join(f"  {p}" for p in errors))
    return {name: build_hop(name, raw)
            for name, raw in (data.get("hops") or {}).items()}


#: engines whose side signs in with nothing a keychain would hold
_NO_KEYCHAIN = ("sqlite", "duckdb", "parquet", "bigquery", "pubsub",
                "generic", "saas")


def build_hop(name, raw):
    """One hop from its mapping - the long form, two addresses
    (`migkit.addresses`), or a side of each. An address is read into the
    mapping the long form would hold, and built the same way from there."""
    from . import addresses, hopkeys
    raw = dict(raw or {})
    src_e, dst_e, engine = hopkeys.sides(raw)
    options = dict(raw.get("options") or {})
    dbs = list(raw.get("databases") or [])
    db_map = dict(raw.get("db_map") or {})
    ends = {}
    for side in ("source", "target"):
        got = raw.get(side)
        if not addresses.is_address(got):
            ends[side] = _endpoint(engine, got)
            continue
        e, ep_raw, names = addresses.parse(got, side)
        ends[side] = _endpoint(e, ep_raw, "" if e in _NO_KEYCHAIN
                               else keychain_name(name, side))
        if engine == "hetero":
            options.setdefault(f"{side}_engine", e)
        if side == "source" and names:
            if dbs and [str(d) for d in dbs] != names:
                raise SystemExit(
                    f"{name}: the source address names {', '.join(names)}"
                    f" and databases says {', '.join(map(str, dbs))} -"
                    " say it once")
            dbs = names
        if side == "target" and names:
            if not dbs or len(names) != len(dbs):
                raise SystemExit(
                    f"{name}: the target address names"
                    f" {', '.join(names)}, and the source"
                    + (f" {len(dbs)} databases" if dbs else " none")
                    + " - name as many on each, in the same order, and"
                    " each moves into the one in its place")
            for s, t in zip(dbs, names):
                if str(s) != t:
                    if db_map.get(s, t) != t:
                        raise SystemExit(
                            f"{name}: {s} goes to {t} by the target"
                            f" address and to {db_map[s]} by db_map")
                    db_map[s] = t
    hop = Hop(
        name=name,
        engine=engine,
        source=ends["source"],
        target=ends["target"],
        databases=dbs,
        exclude=scope_patterns(raw.get("exclude")),
        include=scope_patterns(raw.get("include")),
        objects=dict(raw.get("objects") or {}),
        service=raw.get("service", ""),
        big_rows=int(raw.get("big_rows", 5_000_000)),
        slice=int(raw.get("slice", 1_000_000)),
        workers=int(raw.get("workers", 4)),
        options=options,
        db_map=db_map,
        mapping=raw.get("mapping") or {},
        workers_set="workers" in raw,
        goal=str(raw.get("goal") or ""),
    )
    hopkeys.apply_goal(hop)
    return hop


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
