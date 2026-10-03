import os
import threading
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from . import envkeys
from .engines import PORTS

BASE = Path(__file__).resolve().parent.parent


def _find(env, name, default_dir):
    """Where a runtime file lives, for a copy installed anywhere.

    Resolved against the place the work is happening, not the place the code
    was installed to. Installed with pip, BASE is site-packages, and looking
    for the config there would mean editing files inside an installed package.
    Order: the explicit variable, the working directory, the user's config
    directory, then the source tree for a checkout run in place.
    """
    override = envkeys.text(env)
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


def state_home():
    """The user's own directory for what a program keeps between runs:
    `$XDG_STATE_HOME/migkit`, `~/.local/state/migkit` where unset."""
    return Path(os.environ.get("XDG_STATE_HOME")
                or Path.home() / ".local" / "state") / "migkit"


def _reports_root(conf=None):
    """Where reports and the state of runs are kept: `MIGKIT_REPORTS`, else
    the repo root for a source checkout, where they have always been.

    An installed copy keeps them beside its configuration where that is a
    project's own (`<dir>/conf/hops.yaml` -> `<dir>/reports`, where an
    installed run from that directory always kept them), else in the
    user's state directory (`state_home`) - never under the working
    directory. They followed it: run from cron's `$HOME` or systemd's `/`,
    the same hop met a fresh directory - no lease, so two writers; no tail
    position, so the tail started from now; no half-done cutover to undo
    (audit 2026-10-01, ST7).
    """
    override = envkeys.text("MIGKIT_REPORTS")
    if override:
        return Path(override).expanduser()
    installed = "site-packages" in BASE.parts or "dist-packages" in BASE.parts
    if not installed:
        return BASE / "reports"
    conf = Path(conf) if conf else None
    if conf is not None and conf.parent.name == "conf":
        return conf.parent.parent / "reports"
    return state_home() / "reports"


CONF = _find("MIGKIT_CONF", "hops.yaml", "conf")
REPORTS = _reports_root(CONF)


def private(path):
    """`path` as a directory only this user may read, made where missing:
    what migkit keeps there holds values and secrets, and was readable by
    every user of the machine (audit 2026-10-01, SEC10)."""
    from . import durable
    return durable.private_dir(path)


#: tables migkit keeps on a side for itself - a two-way tail's origin
#: marks (`twoway`), a cutover's drain marks (`drainmark`) - never copied,
#: compared or repaired as the application's
MIGKIT_OWN = ("migkit_origin", "migkit_cutover_mark")
#: the beginning of the name of every table migkit makes on a side: the
#: one rule for "migkit's own", the underscore taken as itself. It was
#: three - the names above exactly, `migkit\_%` on PostgreSQL and
#: `migkit%%` on MySQL, which also took an application's `migkitchen`
#: (audit 2026-10-01, DU26)
OWN_PREFIX = "migkit_"
#: the same as a LIKE pattern, `\` its escape (both engines' default),
#: for a catalogue query to leave them out; quoted by each engine's own
#: `_quote_literal`
OWN_LIKE = "migkit\\_%"


def own(name, keyed=False):
    """Whether a table (its own name, without schema or database) is one
    migkit made on a side for itself. On an engine whose objects are keys
    or topics an application names as it likes (`KEYED_ENGINES`), only the
    names migkit uses exactly."""
    name = str(name)
    if keyed:
        return name in MIGKIT_OWN
    return name in MIGKIT_OWN or name.startswith(OWN_PREFIX)


def _matches(pattern, names):
    """Whether an `exclude` or `include` pattern matches any of `names`: a
    shell pattern, or after `re:` a regular expression the whole name must
    match - what another tool's include and exclude lists are written in,
    carried into the hop as they were (`imports`)."""
    import re
    from fnmatch import fnmatchcase
    pattern = str(pattern)
    if pattern.startswith("re:"):
        try:
            got = re.compile(pattern[3:])
        except re.error as e:
            raise SystemExit(f"{pattern}: not a regular expression ({e})")
        return any(got.fullmatch(n) for n in names)
    return any(fnmatchcase(n, pattern) for n in names)


def _is_reference(value):
    """Whether a side's field is written as a reference to a secret."""
    from .addresses import REFERENCES
    return isinstance(value, str) and value.startswith(REFERENCES) and (
        not value.startswith("${") or value.endswith("}"))

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
        return ([self.password] if self.password else []) + self._opened()

    def _opened(self):
        """The secret options read so far (`option_secret`)."""
        return [v for v in (self.__dict__.get("_secrets") or {}).values()
                if v]

    def option_secret(self, key):
        """The side's option `key`, a secret (`hopkeys` declares it so),
        read the first time it is asked for and kept: a reference
        resolved, its failure naming `hops.<hop>.<side>.<key>`. Measured
        before: a `mine_password`, a key's passphrase or a registry's
        password written as `env:NAME` - as `doctor` told the operator to
        write it - went out as the text `env:NAME` (audit 2026-10-01,
        K7)."""
        kept = self.__dict__.setdefault("_secrets", {})
        if key not in kept:
            raw = (self.options or {}).get(key)
            where = self.__dict__.get("_where", "")
            kept[key] = secret(raw, f"{where}.{key}" if where else key,
                               self.__dict__.get("_memo"))
        return kept[key]

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


def _side_field(name):
    """A field of a side read from the configuration whose value may be a
    reference: kept as written, resolved the first time it is read
    (`secret`), once."""
    def get(self):
        d = self.__dict__
        refs = d.get("_refs") or {}
        if name in refs:
            with _RESOLVING:
                if name in refs:
                    d["_v_" + name] = secret(
                        refs[name], f"{d.get('_where') or 'the side'}"
                                    f".{name}", d.get("_memo"))
                    del refs[name]
        return d.get("_v_" + name, "")

    def put(self, value):
        d = self.__dict__
        refs = d.setdefault("_refs", {})
        if _is_reference(value):
            refs[name] = value
            d.pop("_v_" + name, None)
        else:
            refs.pop(name, None)
            d["_v_" + name] = value
    return property(get, put)


#: one side's references resolved at a time: two connections opened at
#: once ask a vault or a cloud once, not twice
_RESOLVING = threading.RLock()


class SideEndpoint(Endpoint):
    """A side read from the configuration. A host, user or password
    written as a reference (`env:`, `file:`, `vault:`, `aws-sm:`, ...) is
    read the first time a connection needs it, once, and a failure names
    `hops.<hop>.<side>.<key>`: every hop's secrets were read on every load,
    several loads a command - a vault and a cloud asked each time, and
    one hop's unset variable stopping a command on another, naming
    neither (audit 2026-10-01, K12). `doctor`, `report` and a hop that
    never connects read none."""

    host = _side_field("host")
    user = _side_field("user")
    password = _side_field("password")

    def __repr__(self):
        refs = self.__dict__.get("_refs") or {}
        shown = {k: refs.get(k) or getattr(self, k) if k != "password"
                 else ("***" if refs.get(k) or self.__dict__.get(
                     "_v_password") else "")
                 for k in ("host", "user", "password")}
        return (f"{type(self).__name__}(host={shown['host']!r},"
                f" port={self.port!r}, user={shown['user']!r},"
                f" password={shown['password']!r}, options={self.options!r})")

    def held_secrets(self):
        """The password once read, and the secret options read: nothing
        is asked just for it to be hidden."""
        got = self.__dict__.get("_v_password")
        return ([got] if got else []) + self._opened()


class IamEndpoint(SideEndpoint):
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
        return ([self._token] if self._token else []) + self._opened()


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


class KeychainEndpoint(SideEndpoint):
    """A side written as an address with no password: its password is the
    one this machine's keychain keeps for the hop and side, read the first
    time a connection needs it - so a hop that never connects never asks
    the keychain, and one whose server wants no password finds none. A
    password given it (a copy of the side made with one) is used as it
    is."""

    keychain = ""
    _got = None

    @property
    def password(self):
        given = self.__dict__.get("_v_password")
        if given:
            return given
        if self._got is None:
            self._got = keychain_read(self.keychain, strict=False)
        return self._got

    @password.setter
    def password(self, value):
        if value:
            self.__dict__["_v_password"] = value

    def held_secrets(self):
        """The password once read: the keychain is not asked just for it
        to be hidden."""
        got = self.__dict__.get("_v_password") or self._got
        return ([got] if got else []) + self._opened()


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
    #: what the hop is for (`hopkeys.GOALS`), "" where it does not say -
    #: its options are already under the hop's own by the time it is built
    goal: str = ""
    #: the only objects taken (`excluded`): the same patterns as `exclude`
    include: list = field(default_factory=list)
    #: {side: the name its engine was written as} where that is another
    #: name than the engine's own (`mariadb`, `aurora-postgres`):
    #: `engine` and the side engines are the engine's own name
    #: (`hopkeys.brands`)
    brand: dict = field(default_factory=dict)

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
            # never `physical`: the way back would empty the old source's
            # data directory to fill it with the new primary's pages
            options={k: v for k, v in (self.options or {}).items()
                     if k not in ("reverse", "topology", "protect_target",
                                  "physical")})

    def report_dir(self, db=""):
        # each level only this user's: what is kept under it holds values,
        # undo journals and positions (SEC10)
        private(REPORTS)
        d = private(REPORTS / self.name)
        if db:
            d = private(d / db)
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
        """True if an object is left out, so migkit neither verifies nor
        repairs it. Pass the dotted name parts, e.g. (db, schema, table)
        for postgres or (db, table) for mysql/mongo.

        Left out: migkit's own tables (`own`), another tool's bookkeeping
        (`leftovers.bookkeeping`) and the tables whose names begin with the
        hop's `noise_prefix`; what an `exclude` pattern matches - the full
        dotted id or any right-anchored suffix of it, with shell wildcards,
        so 'pick_dispatch_queue', 'public.pick_dispatch_queue' and
        'oms_mkp_uat.public.pick_dispatch_queue' all exclude the same
        table, 'oms_mkp_uat.public.*' a whole schema, and `re:` before a
        pattern makes it a regular expression over the same names; and,
        where the hop has an `include`, a table no include pattern takes -
        one that matches the table's names or the database or schema it is
        in. This protects target-owned tables (rows written on the target,
        not the source) from being deleted by a reconcile.

        One rule for all of it: `noise_prefix` was read by one engine's
        schema check only, and a list of tables to take, imported from
        another tool, was applied only where the guide reached the source,
        so a hop it could not reach moved every table (audit 2026-10-01,
        DU16, K16)."""
        keyed = self.engine in KEYED_ENGINES
        parts = [str(p) for p in parts if p not in (None, "")]
        if not parts:
            return False
        if own(parts[-1], keyed):
            # migkit's own bookkeeping on a side, never the application's
            return True
        if not keyed:
            # another tool's bookkeeping - a schema a replicator keeps on
            # the source, a loader's load table: its state, not the
            # application's rows (`leftovers.bookkeeping`)
            from .leftovers import bookkeeping
            if bookkeeping(*parts):
                return True
            noise = self._noise()
            if noise and len(parts) > 1 and parts[-1].startswith(noise):
                return True
        cands = self._ids(*parts, split=not keyed)
        if any(_matches(pat, cands) for pat in self.exclude or ()):
            return True
        if self.include:
            names = parts if keyed else [n for p in parts
                                         for n in p.split(".") if n]
            # a database alone is not judged: its tables are
            if len(names) > 1:
                runs = [".".join(names[i:j]) for i in range(len(names))
                        for j in range(i + 1, len(names) + 1)]
                return not any(_matches(pat, runs) for pat in self.include)
        return False

    def _noise(self):
        """The beginnings of names the hop's `noise_prefix` leaves out."""
        from .hopkeys import noise
        return noise(self)

    @staticmethod
    def _ids(*parts, split=True):
        """Every name an object answers to, longest first.

        The same right-anchored suffix rule `excluded` uses, so one idea of
        "which table is this" serves the deny list and the mapping instead
        of two that can disagree. A part holding dots is read as the parts
        it names (`public.orders` as `public`, `orders`), so a caller that
        hands the table over as one part reaches the same rules as one
        that hands over each; not on an engine of keys, whose names are
        the application's own.
        """
        parts = [str(p) for p in parts if p not in (None, "")]
        if split:
            parts = [n for p in parts for n in p.split(".") if n]
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


#: the port a side listens on where it names none: the one table, kept
#: in `engines` beside every engine's other names (`engines.PORTS`)
DEFAULT_PORTS = PORTS


# ---- secrets --------------------------------------------------------------

#: where each kind of reference is kept, as its failures say it
_STORES = (("env:", "the environment"), ("${", "the environment"),
           ("file:", "a file"), ("vault:", "Vault"),
           ("aws-sm:", "AWS Secrets Manager"),
           ("gcp-sm:", "Google Secret Manager"),
           ("azure-kv:", "Azure Key Vault"), ("keyring:", "the keychain"))


def _unread(ref, why):
    """The one way a secret that could not be read is said, whatever store
    keeps it: the reference, where it is kept and why (K22)."""
    store = next((s for p, s in _STORES if str(ref).startswith(p)),
                 "its store")
    return SystemExit(f"the secret {ref} could not be read from {store}:"
                      f" {why}")


def secret(value, where="", memo=None):
    """`value` with a reference read from where it is kept (`_secret`);
    anything else as it is. A failure names `where` - the key it was
    written under, `hops.<hop>.<side>.password`. `memo` keeps what a vault
    or a cloud answered for the configuration it was read from, so one
    file's reference is asked once a process; the environment and files
    are read each time."""
    if not _is_reference(value):
        return value
    if memo is not None and value in memo:
        return memo[value]
    try:
        got = _secret(value)
    except SystemExit as e:
        raise SystemExit(f"{where}: {e}" if where else str(e)) from None
    if memo is not None and not value.startswith(("env:", "${", "file:")):
        memo[value] = got
    return got


def _secret(val):
    """Resolve a secret reference so credentials need not sit in plaintext:
      env:NAME / ${NAME}   -> environment variable
      file:/path           -> file contents (trimmed; Docker/K8s secrets)
      vault:secret/db#key  -> Vault's KV store, version 1 or 2, over its
                              HTTP API (VAULT_ADDR, VAULT_TOKEN or
                              ~/.vault-token, VAULT_NAMESPACE,
                              VAULT_CACERT), or the vault CLI where
                              VAULT_ADDR is unset
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
            raise _unread(val, f"the variable {name} is not set")
        return v
    if val.startswith("file:"):
        p = Path(val[5:]).expanduser()
        try:
            return p.read_text().strip()
        except OSError as e:
            raise _unread(val, f"{p}: {e.strerror or e}") from None
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
        raise _unread(f"aws-sm:{ref}", type(e).__name__) from None
    text = got.get("SecretString")
    if text is None:
        raise _unread(f"aws-sm:{ref}", f"AWS secret '{sid}' holds no text")
    if not key:
        return text
    try:
        return str(_json.loads(text)[key])
    except (ValueError, KeyError, TypeError):
        raise _unread(f"aws-sm:{ref}",
                      f"AWS secret '{sid}' has no field '{key}'") from None


def _gcp_secret(ref):
    try:
        from google.cloud import secretmanager
    except ImportError:
        raise SystemExit("pip install google-cloud-secret-manager for"
                         " gcp-sm: secrets")
    name = ref if "/versions/" in ref else ref + "/versions/latest"
    try:
        got = secretmanager.SecretManagerServiceClient() \
            .access_secret_version(name=name)
    except Exception as e:  # noqa: BLE001 - said, with the secret's name
        raise _unread(f"gcp-sm:{ref}", f"{type(e).__name__}: {e}") from None
    return got.payload.data.decode()


def _azure_secret(ref):
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError:
        raise SystemExit("pip install azure-identity azure-keyvault-secrets"
                         " for azure-kv: secrets")
    vault, _, name = ref.partition("/secrets/")
    try:
        return SecretClient(vault_url=vault,
                            credential=DefaultAzureCredential()).get_secret(
                                name.split("/")[0]).value
    except Exception as e:  # noqa: BLE001 - said, with the secret's name
        raise _unread(f"azure-kv:{ref}",
                      f"{type(e).__name__}: {e}") from None


def _vault_read(ref):
    """One field of a secret Vault's KV store keeps at `path#key` (`value`
    where no key is said). Over Vault's own HTTP API where `VAULT_ADDR`
    says where it is - version 1 and 2 of the store alike, the version
    read off the mount as the vault CLI reads it - else through the CLI.
    It was the CLI only, though the docstring promised `VAULT_ADDR`, read
    a version 2 store's layout only (version 1 ended in `KeyError`), and
    kept what the CLI said to itself (audit 2026-10-01, K22)."""
    import shutil
    path, _, key = ref.partition("#")
    key = key or "value"
    path = path.strip("/")
    addr = os.environ.get("VAULT_ADDR", "").rstrip("/")
    if addr:
        data = _vault_http(addr, path, f"vault:{ref}")
    elif shutil.which("vault"):
        data = _vault_cli(path, f"vault:{ref}")
    else:
        raise _unread(f"vault:{ref}", "VAULT_ADDR is not set, and there is"
                      " no vault program to ask")
    if not isinstance(data, dict) or key not in data:
        raise _unread(f"vault:{ref}", f"{path} holds no field '{key}'")
    return str(data[key])


def _vault_get(addr, api, ref):
    """The JSON Vault answers for `GET /v1/<api>`, None for 404."""
    import json as _json
    import ssl
    import urllib.error
    import urllib.request
    token = os.environ.get("VAULT_TOKEN", "")
    if not token:
        kept = Path.home() / ".vault-token"
        token = kept.read_text().strip() if kept.is_file() else ""
    headers = {"X-Vault-Token": token} if token else {}
    if os.environ.get("VAULT_NAMESPACE"):
        headers["X-Vault-Namespace"] = os.environ["VAULT_NAMESPACE"]
    ctx = None
    if addr.startswith("https"):
        ctx = ssl.create_default_context(
            cafile=os.environ.get("VAULT_CACERT") or None)
    req = urllib.request.Request(f"{addr}/v1/{api}", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
            return _json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        try:
            said = "; ".join(_json.loads(e.read().decode() or "{}")
                             .get("errors") or []) or e.reason
        except ValueError:
            said = e.reason
        raise _unread(ref, f"Vault answered {e.code}: {said}") from None
    except (urllib.error.URLError, OSError, ValueError) as e:
        why = getattr(e, "reason", None) or e
        raise _unread(ref, f"{addr} could not be reached: {why}") from None


def _vault_http(addr, path, ref):
    """The fields kept at `path`, from a KV store of either version."""
    mount, version = "", ""
    try:
        got = _vault_get(addr, f"sys/internal/ui/mounts/{path}", ref)
    except SystemExit:
        got = None
    if got and isinstance(got.get("data"), dict):
        mount = str(got["data"].get("path") or "")
        version = str((got["data"].get("options") or {}).get("version")
                      or "1")
    if not mount:
        mount = path.split("/", 1)[0] + "/"
    rest = path[len(mount):] if path.startswith(mount) else path
    if version in ("2", ""):
        got = _vault_get(addr, f"{mount}data/{rest}", ref)
        data = ((got or {}).get("data") or {}).get("data")
        if isinstance(data, dict) or version == "2":
            return data
    got = _vault_get(addr, path, ref)
    return (got or {}).get("data")


def _vault_cli(path, ref):
    import json as _json
    import subprocess
    p = subprocess.run(["vault", "kv", "get", "-format=json", path],
                       capture_output=True, text=True)
    if p.returncode != 0:
        said = (p.stderr or p.stdout or "").strip().splitlines()
        raise _unread(ref, said[-1] if said else f"exit {p.returncode}")
    try:
        data = _json.loads(p.stdout)["data"]
    except (ValueError, KeyError, TypeError):
        raise _unread(ref, "the vault program's answer was not read")
    # version 2 keeps the fields under data.data, beside its metadata
    if isinstance(data, dict) and isinstance(data.get("data"), dict) and \
            "metadata" in data:
        return data["data"]
    return data


# ---- the configuration file ------------------------------------------------

try:
    _LOADER = yaml.CSafeLoader
except AttributeError:  # a build of the YAML library without its C part
    _LOADER = yaml.SafeLoader


class _OnceEach(_LOADER):
    """YAML's safe loader, refusing a key written twice in one mapping.
    YAML keeps the last and reads the first as nothing - measured: a hop
    with two `options:` blocks loaded as the second, `protect_target` and
    `approvals` gone, and no error (audit 2026-10-01, K2). A key a merge
    (`<<: *anchor`) brings is the anchor's to be overridden, as YAML
    means. The C parser where it is built: 40 hops read in 0.5 ms against
    4.3 ms (measured in the audit)."""

    def construct_mapping(self, node, deep=False):
        if isinstance(node, yaml.MappingNode):
            seen = {}
            for key_node, _ in node.value:
                if key_node.tag == "tag:yaml.org,2002:merge":
                    continue
                key = self.construct_object(key_node, deep=True)
                try:
                    first = seen.get(key)
                except TypeError:
                    continue
                line = key_node.start_mark.line + 1
                if first is not None:
                    raise yaml.constructor.ConstructorError(
                        None, None,
                        f"{key!r} is written twice in one mapping (line"
                        f" {first} and line {line}): YAML keeps the last"
                        " and reads the first as nothing - say it once",
                        key_node.start_mark)
                seen[key] = line
        return super().construct_mapping(node, deep)


def read_yaml(text):
    """A configuration's text as data, a key written twice refused: the
    one reader of a hops file (`load_hops`, `doctor`)."""
    return yaml.load(text, Loader=_OnceEach) or {}


#: {path: (what the file was, its data, the secrets read for it)}: the
#: file read and checked once while it stays as it is (`_checked`)
_FILES = {}
_FILES_LOCK = threading.Lock()


def _checked(path=None):
    """(data, the memo of its secrets) of the hops file, every problem of
    it refused before anything runs - read and checked again only when
    the file changes. A command loaded it two to seven times, the
    dashboard on every poll (K12)."""
    import hashlib
    path = Path(path or CONF)
    try:
        text = path.read_bytes()
        mode = path.stat().st_mode
    except FileNotFoundError:
        raise SystemExit(
            f"no hop configuration yet ({path} does not exist).\n"
            "  create one:  migkit init\n"
            "  or point at an existing file:  MIGKIT_CONF=/path/to/hops.yaml")
    stamp = (hashlib.blake2b(text, digest_size=16).digest(), mode)
    key = str(path)
    with _FILES_LOCK:
        got = _FILES.get(key)
    if got is not None and got[0] == stamp:
        return got[1], got[2]
    try:
        data = read_yaml(text)
    except yaml.YAMLError as e:
        raise SystemExit(f"{path} is not YAML migkit can read, and nothing"
                         f" was run:\n  {e}")
    from . import hopkeys
    errors = [p for p in hopkeys.check(data, path) if p.level == "error"]
    if errors:
        # before anything runs, and all of them at once: a run stopped on
        # the first only to meet the second is two runs for one fix
        raise SystemExit(
            f"{path} has {len(errors)} problem"
            f"{'s' if len(errors) > 1 else ''}, and nothing was run:\n"
            + "\n".join(f"  {p}" for p in errors))
    with _FILES_LOCK:
        _FILES[key] = (stamp, data, {})
    return data, _FILES[key][2]


def load_hops(path=None):
    """Every hop of the hops file, built. Nothing is resolved: a side's
    secrets are read the first time it connects (`SideEndpoint`)."""
    import copy
    data, memo = _checked(path)
    return {name: build_hop(name, copy.deepcopy(raw), memo)
            for name, raw in (data.get("hops") or {}).items()}


#: engines whose side signs in with nothing a keychain would hold
_NO_KEYCHAIN = ("sqlite", "duckdb", "parquet", "bigquery", "pubsub",
                "generic", "saas")


def _endpoint(engine, raw, keychain="", where="", memo=None):
    """A side from its long-form mapping. `keychain` names the entry a
    side written as an address signs in with where it carries no password
    of its own; `where` is the side's place in the file
    (`hops.<hop>.<side>`), named where a secret of it cannot be read."""
    from . import hopkeys
    raw = raw or {}
    extra = {k: v for k, v in raw.items()
             if k not in ("host", "port", "user", "password")}
    nested = extra.pop("options", None)
    if isinstance(nested, dict):
        extra.update(nested)
    extra = hopkeys.normalised(hopkeys.ENDPOINT_OPTIONS, extra)
    auth = str(extra.get("auth", "") or "").lower()
    if auth not in ("", "password", "aws_iam"):
        raise SystemExit(f"auth: {auth} - password (the default) or"
                         " aws_iam")
    lazy = bool(keychain) and auth != "aws_iam" and not raw.get("password")
    password = raw.get("password", "")
    if password is not None and not isinstance(password, str):
        password = str(password)
    ep = (IamEndpoint if auth == "aws_iam" else
          KeychainEndpoint if lazy else SideEndpoint)(
        host=raw.get("host", ""),
        port=int(raw.get("port") or PORTS.get(engine, 0)),
        user=raw.get("user", ""),
        password="" if auth == "aws_iam" or lazy else (password or ""),
        options=extra,
    )
    ep.__dict__.update(_where=where, _memo=memo)
    if lazy:
        ep.keychain = keychain
    return ep


def build_hop(name, raw, memo=None):
    """One hop from its mapping - the long form, two addresses
    (`migkit.addresses`), or a side of each. An address is read into the
    mapping the long form would hold, and built the same way from there.

    Every engine on it is the engine's own name, whatever other name it
    was written as (the name kept apart, `Hop.brand`); every side has its
    engine's port where it names none, a cross-engine hop's long-form
    sides too, which got port 0; and every word an option folds is in its
    own spelling (`hopkeys.normalised`) - so no reader meets another
    spelling than the next (audit 2026-10-01, DU6, K15, K20)."""
    from . import addresses, hopkeys
    raw = dict(raw or {})
    src_e, dst_e, engine = hopkeys.sides(raw)
    options = hopkeys.normalised(hopkeys.HOP_OPTIONS,
                                 raw.get("options") or {})
    if engine == "hetero":
        options.update(source_engine=src_e, target_engine=dst_e)
    dbs = list(raw.get("databases") or [])
    db_map = dict(raw.get("db_map") or {})
    ends = {}
    for side, e in (("source", src_e), ("target", dst_e)):
        got = raw.get(side)
        where = f"hops.{name}.{side}"
        if not addresses.is_address(got):
            ends[side] = _endpoint(e, got, where=where, memo=memo)
            continue
        e, ep_raw, names = addresses.parse(got, side)
        ends[side] = _endpoint(e, ep_raw, "" if e in _NO_KEYCHAIN
                               else keychain_name(name, side), where, memo)
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
    keys = hopkeys.HOP_KEYS
    hop = Hop(
        name=name,
        engine=engine,
        source=ends["source"],
        target=ends["target"],
        databases=dbs,
        exclude=list(raw.get("exclude") or []),
        include=list(raw.get("include") or []),
        service=raw.get("service", ""),
        big_rows=hopkeys.opt(raw, "big_rows", keys),
        slice=hopkeys.opt(raw, "slice", keys),
        workers=hopkeys.opt(raw, "workers", keys),
        options=options,
        db_map=db_map,
        mapping=raw.get("mapping") or {},
        workers_set="workers" in raw,
        goal=str(raw.get("goal") or ""),
        brand=hopkeys.brands(raw),
    )
    hopkeys.apply_goal(hop)
    return hop


def get_hop(name):
    """The hop `name`, and only it, built: another hop's secrets are not
    read, and a name that is not one is said with the nearest that is."""
    import copy
    import difflib
    data, memo = _checked()
    hops = data.get("hops") or {}
    key = next((k for k in hops if str(k) == str(name)), None)
    if key is None:
        names = [str(k) for k in hops]
        near = difflib.get_close_matches(str(name), names, n=1, cutoff=0.6)
        raise SystemExit(f"unknown hop {name}"
                         + (f" - did you mean {near[0]}?" if near else "")
                         + f", have: {', '.join(names) or 'none'}")
    hop = build_hop(key, copy.deepcopy(hops[key]), memo)
    # MIGKIT_EXCLUDE lets a caller skip databases/tables without editing hops.yaml,
    # so the run's config file stays the single place the operator changes.
    extra = envkeys.items("MIGKIT_EXCLUDE")
    if extra:
        hop.exclude = list(hop.exclude) + extra
    return hop
