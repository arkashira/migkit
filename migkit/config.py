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


def _is_reference(value):
    """Whether a side's field is written as a reference to a secret."""
    from .addresses import REFERENCES
    return isinstance(value, str) and value.startswith(REFERENCES) and (
        not value.startswith("${") or value.endswith("}"))

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


def _shell_parts(pattern):
    """A shell pattern as its parts: `*`, `?`, a `[...]` class whole, or
    one plain character."""
    out, i, text = [], 0, str(pattern)
    while i < len(text):
        if text[i] == "[":
            end = text.find("]", i + 2)
            if end > 0:
                out.append(text[i:end + 1])
                i = end + 1
                continue
        out.append(text[i])
        i += 1
    return out


def pattern_after(pattern, prefix):
    """The patterns that take what is left of a name once `prefix` has been
    matched by `pattern`'s start - [] where no start of it matches the
    prefix. What a program that names tables without their database is
    handed of a hop's pattern written with it: `shop.orders` after `shop.`
    is `orders`, `*.orders` is `*.orders` and `orders` (the star took the
    database, or the database and more), `crm.orders` is nothing. A star
    can take the dot as `name_matches` reads it, so each way it can is
    one pattern."""
    parts = _shell_parts(pattern)
    out, seen = [], set()

    def walk(i, j):
        if (i, j) in seen:
            return
        seen.add((i, j))
        if j == len(prefix):
            got = "".join(parts[i:])
            if got not in out:
                out.append(got)
            return
        if i == len(parts):
            return
        p, ch = parts[i], prefix[j]
        if p == "*":
            walk(i + 1, j)
            walk(i, j + 1)
        elif p == "?" or p == ch or (len(p) > 1 and name_matches(ch, p)):
            walk(i + 1, j + 1)
    walk(0, 0)
    return out


def has_wildcard(pattern):
    """Whether a hop's pattern holds a wildcard: one that does not names
    exactly the name it is."""
    return any(c in str(pattern) for c in "*?[")


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


#: each way a hop takes other than every object whole, under its own
#: name, with every change - in the order `narrowed_by` says them
NARROWED_BY = ("include", "exclude", "objects_not_carried", "objects_named",
             "row_filters", "column_rules", "values_masked", "renamed",
             "names_ruled", "databases_renamed", "changes_carried")
#: the parts of it that leave objects out by name
BY_NAME = ("include", "exclude", "objects_not_carried", "objects_named")


def narrowed_by(hop):
    """{part: [what]} of every way the hop takes other than everything
    whole (`NARROWED_BY`): objects left out by name, rows filtered, columns
    chosen or masked, names changed on the target, changes not carried.
    The one reading of it: a way that copies a whole database or server
    (pages, a backup) asks which parts it cannot serve, the report says
    them beside a `same`, and none of them reads the hop's keys apart.
    Empty for a hop that takes everything whole."""
    mapping = getattr(hop, "mapping", None) or {}
    options = getattr(hop, "options", None) or {}
    out = {k: [str(x) for x in getattr(hop, k, None) or []]
           for k in ("include", "exclude")}
    try:
        said = objects_of(hop)
    except SystemExit:
        said = {}
    out["objects_not_carried"] = sorted(k for k, v in said.items()
                                        if v is False)
    out["objects_named"] = [f"{k}: {', '.join(v)}" for k, v in
                            sorted(said.items()) if isinstance(v, list)]
    out["row_filters"] = sorted(str(k) for k in mapping.get("where") or {})
    out["column_rules"] = sorted(str(k) for k in
                                 mapping.get("columns") or {})
    out["values_masked"] = sorted(str(k) for k in
                                  options.get("mask_on_write") or {})
    out["renamed"] = [f"{k} -> {v}" for k, v in
                      sorted((mapping.get("tables") or {}).items(),
                             key=lambda kv: str(kv[0]))]
    out["names_ruled"] = [
        f"{level}: " + ", ".join(f"{k} {v}" for k, v in sorted(
            (rule or {}).items()))
        for level, rule in sorted((mapping.get("names") or {}).items())
        if rule]
    out["databases_renamed"] = [
        f"{k} -> {v}" for k, v in sorted((getattr(hop, "db_map", None)
                                          or {}).items()) if k != v]
    every = frozenset(OPS)
    ops = mapping.get("ops") or {}
    out["changes_carried"] = []
    for k, v in sorted(ops.items(), key=lambda kv: str(kv[0])):
        try:
            fewer = parse_ops(v) != every
        except SystemExit:
            fewer = True
        if fewer:
            out["changes_carried"].append(str(k))
    return {k: out[k] for k in NARROWED_BY if out.get(k)}


def narrowed_said(hop, serves=()):
    """The parts of `narrowed_by` a way does not serve (all but `serves`),
    in the words a report says them - '' where there are none."""
    from .verdict import SCOPE_WORDS
    said = []
    for k, v in narrowed_by(hop).items():
        if k in serves:
            continue
        more = f" and {len(v) - 3} more" if len(v) > 3 else ""
        said.append(f"{SCOPE_WORDS.get(k, k)} {', '.join(v[:3])}{more}")
    return "; ".join(said)


def scoped(hop):
    """Whether the hop leaves anything out by name - an `exclude`, an
    `include` that takes less than everything, objects not carried
    (`narrowed_by`). A way that carries a whole database, a whole server or
    a whole keyspace serves only a hop that is not; the one `excluded`
    answers which objects."""
    got = narrowed_by(hop)
    return any(part in got for part in BY_NAME)


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
    #: {side: the name its engine was written as} where that is another
    #: name than the engine's own (`mariadb`, `aurora-postgres`):
    #: `engine` and the side engines are the engine's own name
    #: (`hopkeys.brands`)
    brand: dict = field(default_factory=dict)

    def reversed(self):
        """The same hop run the other way: the target as the source, every
        name as the target has it - databases mapped back, tables renamed
        back, the name rules turned round. Its name is its own, so its
        replication objects and reports are not the forward hop's. Used
        only where the hop asks for a stream back (`reverse`,
        `topology`). A name rule that cannot be turned round - one that
        strips a part some names never had, or folds case - is refused
        here, before a change is written back under a name the old
        source does not have."""
        import dataclasses
        mapping = self._mapping_on_target()
        return dataclasses.replace(
            self, name=f"{self.name}-reverse", source=self.target,
            target=self.source,
            databases=[self.target_db(d) for d in (self.databases or [])],
            db_map={self.target_db(k): k for k in (self.db_map or {})},
            include=[self._on_target(p) for p in self.include or []],
            exclude=[self._on_target(p) for p in self.exclude or []],
            objects={k: ([self._on_target(p, tables=False) for p in v]
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
        table's name on the target (`_on_target`), a rename turned round,
        a column renamed named as the target has it, the name rules
        turned round (`_names_back`); a column the way there dropped is
        not on the target to carry back, and a row filter is asked of the
        target's rows under their target names."""
        mapping = dict(self.mapping or {})
        out = {}
        renames = {str(k): str(v) for k, v in
                   (mapping.get("tables") or {}).items()}
        if renames:
            back = {}
            for k, v in renames.items():
                head, tail = k.split("."), v.split(".")
                back[self._on_target(k)] = ".".join(
                    head[-len(tail):] if len(head) >= len(tail) else head)
            out["tables"] = back
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
                cols[self._on_target(k)] = back
        if cols:
            out["columns"] = cols
        for part in ("where", "tolerance", "ops"):
            if mapping.get(part):
                out[part] = {self._on_target(k): v
                             for k, v in mapping[part].items()}
        names = self._names_back()
        if names:
            out["names"] = names
        return out

    #: the parts of a name rule the way back turns round: each one's
    #: opposite, written to every name the way there wrote
    _NAMES_TURNED = {"prefix": "strip_prefix", "suffix": "strip_suffix"}

    def _names_back(self):
        """`mapping.names` turned round for the way back: a prefix or a
        suffix the way there added, stripped. SystemExit for a rule that
        cannot be: one that strips (a name that never had the part is
        not told apart from one that did) or folds case."""
        out = {}
        for level, rule in ((self.mapping or {}).get("names") or {}).items():
            back = {}
            for k, v in (rule or {}).items():
                if not v:
                    continue
                if k not in self._NAMES_TURNED:
                    raise SystemExit(
                        f"{self.name}: mapping.names.{level}.{k} cannot be"
                        " turned round for the way back - not every name"
                        " it writes says what it was. Name those tables in"
                        " mapping.tables, which is turned round exactly")
                back[self._NAMES_TURNED[k]] = v
            if back:
                out[level] = back
        return out

    def _on_target(self, pattern, tables=True):
        """A scope pattern as the target names what it names: a leading
        database (one the pattern names whole: as many parts as a table's
        whole name, or a database the hop maps) as the target's database,
        and - `tables` - a table the hop renames by its new name, the
        name rules written on the schema and the table as on every name
        (`target_table`). A rename shorter than the name it renames keeps
        the parts before it. So the way back takes what the way there
        took, under the names it took them to."""
        text = str(pattern)
        parts = text.split(".")
        whole = self.whole_name()
        has_db = (len(parts) >= whole if whole else
                  len(parts) >= 2 and parts[0] in (self.db_map or {}))
        head = []
        if has_db:
            db, parts = parts[0], parts[1:]
            head = [db if has_wildcard(db) else self.target_db(db)]
            if not parts:
                return head[0]
        if not tables:
            return ".".join(head + parts)
        rules = self.table_map()
        for ident in self._ids(*([text.split(".")[0]] if has_db else [])
                               + parts):
            if ident in rules:
                got = str(rules[ident]).split(".")
                kept = parts[:max(len(parts) - len(got), 0)]
                return ".".join(head + kept + got)
        named = list(parts)
        named[-1] = self.named("tables", named[-1])
        if whole == 3 and len(named) >= 2:
            named[-2] = self.named("schemas", named[-2])
        return ".".join(head + named)

    def scope_fingerprint(self):
        """One short digest of everything that decides what the hop takes
        and how each table is read: its databases and their target names,
        include, exclude and the whole mapping. Kept beside every answer,
        position and checkpoint that is only good for the scope it was
        taken under, so one taken under another scope is never reused as
        if it were this one's (`scope_changed`)."""
        import hashlib
        import json
        text = json.dumps(self.scope_record(), sort_keys=True, default=str)
        return hashlib.sha256(text.encode()).hexdigest()[:16]

    def scope_record(self):
        """What `scope_fingerprint` digests, as plain values - kept beside
        a record whose reader must know what that scope took, not only
        that it was another (`under`)."""
        return {"databases": sorted(str(d) for d in self.databases or []),
                "db_map": {str(k): str(v) for k, v in
                           sorted((self.db_map or {}).items())},
                "include": sorted(str(p) for p in self.include or []),
                "exclude": sorted(str(p) for p in self.exclude or []),
                "objects": self.objects or {},
                "mapping": self.mapping or {}}

    def under(self, record):
        """This hop under the scope `record` kept (`scope_record`, or a dict
        holding one under `scope`) - to ask what that scope took. None
        where it kept none."""
        import dataclasses
        if isinstance(record, dict) and isinstance(record.get("scope"),
                                                   dict):
            record = record["scope"]
        if not isinstance(record, dict) or "mapping" not in record:
            return None
        return dataclasses.replace(
            self, databases=list(record.get("databases") or []),
            db_map=dict(record.get("db_map") or {}),
            include=list(record.get("include") or []),
            exclude=list(record.get("exclude") or []),
            objects=dict(record.get("objects") or {}),
            mapping=dict(record.get("mapping") or {}))

    def scope_changed(self, recorded):
        """Whether a record made under `recorded` (a fingerprint, or a dict
        holding one under `scope_fingerprint`) was taken under another
        scope than the hop's now. A record with none predates them and is
        taken as it always was."""
        if isinstance(recorded, dict):
            recorded = recorded.get("scope_fingerprint")
        return bool(recorded) and recorded != self.scope_fingerprint()

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
        differently-named db (e.g. shop_staging -> shop), so every dst-side
        connection resolves through this map (identity when unmapped).
        Local report paths stay keyed by the source name."""
        got = (self.db_map or {}).get(db)
        return got if got is not None else self.named("databases", db)

    def excluded(self, *parts):
        """True if an object is left out, so migkit neither verifies nor
        repairs it. Pass the dotted name parts, e.g. (db, schema, table)
        for postgres or (db, table) for mysql/mongo.

        Left out: migkit's own tables (`own`), another tool's bookkeeping
        (`leftovers.bookkeeping`) and the tables whose names begin with the
        hop's `noise_prefix`; what an `exclude` pattern matches - the full
        dotted id or any right-anchored suffix of it, with shell
        wildcards, so 'job_queue', 'public.job_queue' and
        'orders_db.public.job_queue' all exclude the same table, and
        'orders_db.public.*' a whole schema; and what the hop's `include`
        does not take (`included`). This protects target-owned tables
        (rows written on the target, not the source) from being deleted by
        a reconcile.

        A name handed over as one part (`public.orders`) is read as the
        parts it names, as `included` reads it, so `orders` excludes it
        whichever way a caller passes it. One rule for all of it:
        `noise_prefix` was read by one engine's schema check only, so the
        copy, the tail and every other engine took the tables it named
        (audit 2026-10-01, DU16)."""
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
        names = self._names(*parts) or parts
        cands = {".".join(names[i:]) for i in range(len(names))}
        fold = self.folds_case()
        if any(name_matches(c, pat, fold) for pat in self.exclude or ()
               for c in cands):
            return True
        return not self.included(*parts)

    def _noise(self):
        """The beginnings of names the hop's `noise_prefix` leaves out."""
        from .hopkeys import noise
        return noise(self)

    def takes_table(self, db, table, engine=None):
        """Whether the hop takes a table named as a listing names it
        (`public.orders`, `orders`, `public."v1.orders"`): split the one way
        names are (`util.split_table`, by `engine`'s quoting - the
        source's by default - and a bare name in the engine's default
        schema, `DEFAULT_SCHEMA`), then asked of `excluded`. The one
        question every reader holding a table's listed name asks;
        `Engine.in_scope` asks it with its own engine. Splitting it by
        hand split `public."v1.orders"` at the wrong dot and asked a
        bare PostgreSQL name outside its schema."""
        from .util import DEFAULT_SCHEMA, _canonical, split_table
        eng = _canonical(engine) if engine else self._names_engine()
        schema, name = split_table(eng, str(table),
                                   DEFAULT_SCHEMA.get(eng, ""))
        return not self.excluded(db, *([schema] if schema else []), name)

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

    def source_table_in(self, db, *parts):
        """The parts of the source's name, within `db`, of the table the
        target names `parts` in `db`'s target database: `target_table_in`
        turned round (`reversed` - a rename, the name rules), a rename
        shorter than the name keeping the parts before it. None where the
        hop's name rules cannot be turned round."""
        own = [str(p) for p in parts if p not in (None, "")]
        try:
            back = self.reversed()
        except SystemExit:
            return None
        got = [p for p in back.target_table_in(self.target_db(db), *own)
               .split(".") if p]
        if len(got) < len(own):
            got = own[:len(own) - len(got)] + got
        return tuple(got)

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
        best = self.most_particular(rules, *parts)
        if best is None:
            return frozenset(OPS)
        return parse_ops(rules[best])

    def most_particular(self, rules, *parts):
        """The key of `rules` - names or patterns (`logistics.*`, `*`) -
        that names this table most particularly, or None: the one with the
        most characters that are not wildcards, then the longest. How
        `mapping.ops` and a `ddl` said per table (`tailtables.policy`)
        pick the rule a table goes by."""
        ids = self._ids(*parts)
        fold = self.folds_case()
        hits = [k for k in rules
                if any(name_matches(i, k, fold) for i in ids)]
        if not hits:
            return None
        return max(hits, key=lambda k: (sum(c not in "*?[]" for c in
                                            str(k)), len(str(k))))

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
        exclude=scope_patterns(raw.get("exclude")),
        include=scope_patterns(raw.get("include")),
        objects=dict(raw.get("objects") or {}),
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
