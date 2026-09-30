"""A hop from two addresses (backlog R21).

    hops:
      shop:
        source: postgres://app@db1:5432/shop
        target: mysql://app@db2/shop

is a whole hop. The engine of each side comes from the address's scheme
(`engines.SCHEMES`, the one place a scheme is mapped), the pair - one
engine, or two across (`hetero`) - from the two, the port and the
databases from the address, TLS from its query (`?sslmode=verify-full`,
`?ssl-mode=VERIFY_IDENTITY&ssl-ca=/etc/ca.pem`), and what the address
cannot say is migkit's to decide. What a side is (a managed service, a
replica) is read from the host and the server (`migkit.clouds`), never
asked. The long form - `source: {host: ..., port: ...}` - stays valid,
and an address is read into exactly the mapping the long form would have
been (`parse`), so the two cannot come to mean different things.

A password is never part of an address: one written there is refused,
with the two places it can go instead - the OS keychain (`migkit init`
stores it there, under the hop's name, and a side given as an address
with no password is looked up there) or a reference the address carries
in the password's place (`postgres://app:env:SRC_PW@db1/shop`).

Counted on the files as they were: the example configuration's hop was
18 lines (its name, engine, service, ten endpoint lines, databases,
db_map and three knobs) and the one `init` wrote 16; a hop of two
addresses is 3, and 4 with a goal.
"""
import difflib
import re
from urllib.parse import parse_qsl, quote, unquote, urlencode

#: the references a password may be written as, never the password itself
#: (`config._secret` resolves each)
REFERENCES = ("env:", "${", "file:", "vault:", "aws-sm:", "gcp-sm:",
              "azure-kv:", "keyring:")

#: engines whose address is a file or a place, not a server
PLACES = ("sqlite", "duckdb", "parquet", "copybook", "delta")
#: schemes whose place is an S3 prefix
ON_S3 = ("parquet+s3", "delta+s3")
#: an Iceberg side's catalog: a SQLite file (`iceberg:///c.db`) or a REST
#: catalog (`iceberg+https://host/path`)
ICEBERG_REST = ("iceberg+http", "iceberg+https")
#: engines whose first path segment names what to connect to, and the
#: second the hop's databases (their schemas)
CONNECTS_TO = {"oracle": "service", "db2": "database", "db2zos": "database",
               "redshift": "database", "snowflake": "database",
               # spanner://project/instance/database
               "spanner": "instance"}
#: engines that sign in without a user and password of the address's own
NO_SIGN_IN = ("sqlite", "duckdb", "parquet", "copybook", "bigquery",
              "pubsub", "iceberg", "delta", "saas", "spanner", "bigtable",
              "firestore", "datastore")
#: engines whose address's host is the region a service is in, or an
#: endpoint of its own (`dynamodb://us-east-1/table`)
REGIONAL = ("dynamodb", "kinesis", "timestream")
#: a program source's scheme is its kind, and what follows it the program
#: (`engines.saas`): `singer://tap-github?config=...`
#: an AWS region, as a host naming where a service is rather than a server
REGION = re.compile(r"^[a-z]{2}(-gov|-iso[a-z]*)?-[a-z]+-\d$")

#: MySQL's TLS words (and libpq's, which people write for MySQL too), as
#: the endpoint's own options (`config.Endpoint.mysql_tls`)
MYSQL_MODES = {"disabled": "disabled", "disable": "disabled",
               "preferred": "preferred", "prefer": "preferred",
               "allow": "preferred",
               "required": "required", "require": "required",
               "verify_ca": "verify_ca", "verify-ca": "verify_ca",
               "verify_identity": "verify_identity",
               "verify-full": "verify_identity"}
MYSQL_TLS_KEYS = {"ssl-ca": "ssl_ca", "ssl_ca": "ssl_ca",
                  "sslrootcert": "ssl_ca", "ssl-cert": "ssl_cert",
                  "ssl_cert": "ssl_cert", "sslcert": "ssl_cert",
                  "ssl-key": "ssl_key", "ssl_key": "ssl_key",
                  "sslkey": "ssl_key"}
LIBPQ_MODES = ("disable", "allow", "prefer", "require", "verify-ca",
               "verify-full")


def is_address(value):
    """Whether a hop's side is written as an address."""
    return isinstance(value, str) and "://" in value


def is_reference(text):
    return str(text).startswith(REFERENCES)


def _scheme(text, side):
    from .engines import SCHEMES
    scheme, sep, rest = str(text).strip().partition("://")
    name = scheme.lower()
    if not sep or name not in SCHEMES:
        near = difflib.get_close_matches(name, list(SCHEMES), n=1)
        raise SystemExit(
            f"the {side} address {_shown(text)} names no engine migkit"
            f" knows by {name!r} (it knows"
            f" {', '.join(sorted(set(SCHEMES)))})"
            + (f" - did you mean {near[0]}://" if near else ""))
    return name, SCHEMES[name], rest


def _shown(text):
    """An address as it may be printed: whatever sits in the password's
    place taken out."""
    return re.sub(r"(://[^/@:]*):[^/@]*@", r"\1:...@", str(text))


def _refuse_password(side, text, user):
    raise SystemExit(
        f"the {side} address {_shown(text)} holds a password, and migkit"
        " keeps none in its configuration. Take it out of the address"
        f" and either let `migkit init` keep it in this machine's keychain,"
        " or put it in an environment variable and write the variable in"
        f" its place: {text.split('://', 1)[0]}://{user or 'user'}"
        ":env:NAME@...")


def _value(v):
    low = str(v).lower()
    if low in ("true", "yes", "on"):
        return True
    if low in ("false", "no", "off"):
        return False
    return v


def _hosts(hostlist):
    """[(host, port or None)] from `h1:1,h2:2`, `[::1]:5432`."""
    out = []
    for part in [p for p in hostlist.split(",") if p]:
        if part.startswith("["):
            host, _, rest = part[1:].partition("]")
            port = rest[1:] if rest.startswith(":") else ""
        else:
            host, _, port = part.rpartition(":") if ":" in part else (
                part, "", "")
        if port and not port.isdigit():
            raise SystemExit(f"{part!r}: the port is not a number")
        out.append((unquote(host), int(port) if port else None))
    return out


def parse(text, side="source"):
    """(engine, endpoint, databases) of an address.

    `endpoint` is the mapping the long form would hold for this side -
    `host`, `port`, `user`, `password` (a reference only) and the engine's
    own options - so `config` builds the side from it as it builds any
    other. `databases` are the names in the address's path, in order."""
    scheme, engine, rest = _scheme(text, side)
    rest, _, query = rest.partition("?")
    params = dict(parse_qsl(query, keep_blank_values=True))
    if engine == "generic":
        if re.search(r"://[^/@:]*:[^/@]*@", str(text)) and not re.search(
                r"://[^/@:]*:(" + "|".join(map(re.escape, REFERENCES))
                + r")", str(text)):
            _refuse_password(side, text, "")
        return engine, {"url": str(text).strip()}, []
    if engine == "saas":
        from .engines.saas import database_name
        if not rest:
            raise SystemExit(f"the {side} address {text!r} names no program")
        program = unquote(rest)
        return engine, {"kind": scheme, "program": program, **_options(
            engine, params, side)}, [database_name(scheme, program)]
    if engine == "iceberg":
        if not rest:
            raise SystemExit(f"the {side} address {text!r} names no catalog")
        catalog = (f"{scheme.split('+')[1]}://{rest}"
                   if scheme in ICEBERG_REST else f"sqlite:///{unquote(rest)}")
        return engine, {"catalog": catalog, **_options(
            engine, params, side)}, []
    if engine in PLACES:
        if scheme in ON_S3:
            return engine, {"url": "s3://" + rest, **_options(
                engine, params, side)}, []
        if not rest:
            raise SystemExit(f"the {side} address {text!r} names no file")
        return engine, {"path": unquote(rest), **_options(
            engine, params, side)}, []
    authority, _, path = rest.partition("/")
    userinfo, at, hostlist = authority.rpartition("@")
    user, colon, password = userinfo.partition(":")
    user, password = unquote(user), unquote(password)
    if colon and password and not is_reference(password):
        _refuse_password(side, text, user)
    segments = [unquote(s) for s in path.split("/") if s]
    hosts = _hosts(hostlist)
    endpoint = {}
    if user:
        endpoint["user"] = user
    if password:
        endpoint["password"] = password
    dbs = []
    if engine == "mongodb" and scheme == "mongodb+srv":
        hosts, params = _srv(hostlist, params)
    if engine in REGIONAL:
        (host, port), = hosts or [("", None)]
        if REGION.match(host or ""):
            endpoint["region"] = host
        elif host:
            endpoint["endpoint_url"] = (f"http://{host}"
                                        + (f":{port}" if port else ""))
        dbs = segments[0].split(",") if segments else []
    elif engine == "bigquery":
        (host, _), = hosts or [("", None)]
        endpoint["database"] = host
        dbs = segments[0].split(",") if segments else []
    elif engine == "pubsub":
        (host, port), = hosts or [("", None)]
        if port:
            # an emulator: a host and port, the project in the path
            endpoint.update(host=host, port=port)
            if segments:
                endpoint["project"] = segments[0]
        else:
            endpoint["project"] = host
    else:
        if len(hosts) > 1:
            if engine == "kafka":
                endpoint["hosts"] = [f"{h}:{p or 9092}" for h, p in hosts]
            elif engine == "mongodb":
                endpoint["hosts"] = ",".join(f"{h}:{p or 27017}"
                                             for h, p in hosts)
            else:
                raise SystemExit(
                    f"the {side} address names {len(hosts)} servers, and a"
                    f" {engine} side is one server")
        elif hosts:
            host, port = hosts[0]
            endpoint["host"] = host
            if port:
                endpoint["port"] = port
        if engine in CONNECTS_TO:
            if segments:
                endpoint[CONNECTS_TO[engine]] = segments[0]
            dbs = segments[1].split(",") if len(segments) > 1 else []
        elif engine != "kafka":
            dbs = segments[0].split(",") if segments else []
        if engine == "snowflake" and "host" in endpoint:
            # the account identifier is where a server's name would be
            endpoint["account"] = endpoint.pop("host")
    endpoint.update(_options(engine, params, side))
    if scheme in ("rediss", "couchbases"):
        # Redis's and Couchbase's own word for TLS, the certificate
        # checked (`Endpoint.redis_tls`, `CouchbaseEngine._cluster`)
        endpoint["tls"] = True
    if scheme == "sapodp+http":
        # an SAP service over plain HTTP: a sandbox's, never a system's
        # own unless it says so
        endpoint["tls"] = False
    return engine, endpoint, [d for d in dbs if d]


def _srv(hostlist, params):
    """A `mongodb+srv://` name read as the servers and options its DNS
    records give - what the driver does with one, done once here because
    every connection migkit makes to MongoDB is built from a server list."""
    from pymongo.uri_parser import parse_uri
    try:
        got = parse_uri(f"mongodb+srv://{hostlist}/")
    except Exception as e:  # noqa: BLE001 - said, with the name
        raise SystemExit(f"{hostlist}: its DNS records could not be read"
                         f" ({type(e).__name__}: {e})")
    opts = {}
    for k, v in (got.get("options") or {}).items():
        opts[k] = str(v).lower() if isinstance(v, bool) else str(v)
    opts.update(params)
    return [(h, p) for h, p in got["nodelist"]], opts


def _options(engine, params, side):
    """The address's query as the endpoint's own options, in the names the
    engine reads (`hopkeys.ENDPOINT_OPTIONS`); a word the engine does not
    read is refused here, where it was written, rather than ignored."""
    from . import hopkeys
    from .engines import WIRE
    if not params:
        return {}
    if engine == "mongodb":
        # the driver's own options, handed to it as they were written
        return {"uri_options": urlencode(params)}
    wire = WIRE.get(engine, engine)
    if wire == "mysql":
        params = _mysql_tls(params, side)
    if wire in ("postgres", "redshift"):
        mode = params.get("sslmode")
        if mode is not None and mode not in LIBPQ_MODES:
            raise SystemExit(f"sslmode={mode}: one of"
                             f" {', '.join(LIBPQ_MODES)}")
    out = {}
    known = hopkeys.endpoint_keys_for(engine)
    for k, v in params.items():
        if k not in known:
            near = difflib.get_close_matches(k, sorted(known), n=1)
            raise SystemExit(
                f"the {side} address's {k}={v}: a {engine} side has no"
                f" option {k!r}"
                + (f" - did you mean {near[0]}?" if near else "")
                + (f" It reads: {', '.join(sorted(known))}" if known else
                   " It reads none from an address"))
        out[k] = _value(v) if not isinstance(v, bool) else v
    return out


def _mysql_tls(params, side):
    """MySQL's `ssl-mode` (or libpq's `sslmode`) as the options its
    connection reads: `ssl_ca` to check the server's certificate (and the
    name on it, unless VERIFY_CA), `ssl: true` for TLS unchecked."""
    params = dict(params)
    out = {k: v for k, v in params.items()
           if k not in ("ssl-mode", "ssl_mode", "sslmode")
           and k not in MYSQL_TLS_KEYS}
    for k, v in params.items():
        if k in MYSQL_TLS_KEYS:
            out[MYSQL_TLS_KEYS[k]] = v
    said = next((params[k] for k in ("ssl-mode", "ssl_mode", "sslmode")
                 if k in params), None)
    if said is None:
        return out
    mode = MYSQL_MODES.get(str(said).lower())
    if mode is None:
        raise SystemExit(f"ssl-mode={said}: one of DISABLED, PREFERRED,"
                         " REQUIRED, VERIFY_CA, VERIFY_IDENTITY")
    if mode == "disabled":
        raise SystemExit(
            f"the {side} address asks for no TLS (ssl-mode={said}), and a"
            " MySQL connection migkit opens takes TLS wherever the server"
            " offers it - leave ssl-mode out")
    if mode == "required":
        out["ssl"] = True
    if mode in ("verify_ca", "verify_identity"):
        if not out.get("ssl_ca"):
            raise SystemExit(
                f"ssl-mode={said} checks the server's certificate against"
                " an authority: say which with ssl-ca=<its certificate"
                " file> (a managed service publishes its bundle)")
        if mode == "verify_ca":
            out["ssl_verify_identity"] = False
    return out


# ---- the other way: a side as an address, for the smallest config ------

def render(engine, endpoint, dbs=(), default_port=0):
    """The address that `parse` reads back into `endpoint` and `dbs`, or
    None where the side holds something an address cannot carry (a literal
    password, a nested option, a sign-in by the cloud) - that side stays in
    the long form. `default_port` is the port a side gets when it names
    none: said only when it differs."""
    from .engines import SCHEMES
    ep = dict(endpoint or {})
    password = ep.pop("password", "") or ""
    if password and not is_reference(password):
        return None
    scheme = next((s for s, e in SCHEMES.items() if e == engine
                   and s not in ON_S3 + ICEBERG_REST
                   + ("mongodb+srv", "rediss", "couchbases")), None)
    if engine in ("redis", "couchbase") and ep.get("tls") is True:
        scheme = "rediss" if engine == "redis" else "couchbases"
        ep.pop("tls")
    if engine == "sapodp" and ep.get("tls") is False:
        scheme = "sapodp+http"
        ep.pop("tls")
    if engine == "saas":
        kind, program = ep.pop("kind", ""), ep.pop("program", "")
        if kind not in SCHEMES or SCHEMES[kind] != "saas" or not program \
                or ep.pop("host", "") or ep.pop("user", ""):
            return None
        query = _query(engine, ep)
        return None if query is None else (
            f"{kind}://{quote(str(program), safe='/:.@_-')}{query}")
    if scheme is None or engine == "generic":
        return None
    user = str(ep.pop("user", "") or "")
    host = ep.pop("host", "")
    port = ep.pop("port", 0) or 0
    auth = ""
    if user or password:
        auth = quote(user, safe="") + (
            ":" + quote(password, safe=":${}") if password else "") + "@"
    path = ""
    if engine == "iceberg":
        catalog = str(ep.pop("catalog", "") or "")
        if host or port or auth:
            return None
        if catalog.startswith("sqlite:///"):
            where = f"iceberg://{catalog[len('sqlite:///'):]}"
        elif catalog.startswith(("http://", "https://")):
            where = f"iceberg+{catalog}"
        else:
            return None
        query = _query(engine, ep)
        return None if query is None else where + query
    if engine in PLACES:
        if engine in ("parquet", "delta") and str(
                ep.get("url", "")).startswith("s3://"):
            return (f"{engine}+s3://{ep.pop('url')[5:]}"
                    + _query(engine, ep))
        where = ep.pop("path", "") or host
        if not where or auth:
            return None
        return f"{scheme}://{where}" + _query(engine, ep)
    if engine in REGIONAL:
        region, url = ep.pop("region", ""), ep.pop("endpoint_url", "")
        if host or port or (region and url):
            return None
        if url:
            m = re.match(r"^http://([^/:]+)(?::(\d+))?$", str(url))
            if not m:
                return None
            where = m.group(1) + (f":{m.group(2)}" if m.group(2) else "")
        else:
            where = region
        path = "/" + ",".join(map(str, dbs)) if dbs else ""
        return f"{scheme}://{auth}{where}{path}" + _query(engine, ep)
    if engine == "bigquery":
        if host or port or auth:
            return None
        path = "/" + ",".join(map(str, dbs)) if dbs else ""
        return f"{scheme}://{ep.pop('database', '')}{path}" + _query(
            engine, ep)
    if engine == "pubsub":
        project = ep.pop("project", "")
        if host and port:
            return (f"{scheme}://{auth}{host}:{port}/{project}"
                    + _query(engine, ep))
        if host or port:
            return None
        return f"{scheme}://{project}" + _query(engine, ep)
    if engine == "snowflake":
        host = ep.pop("account", "") or host
    hosts = ep.pop("hosts", None)
    if hosts:
        if host or (port and int(port) != int(default_port or 0)):
            # a port beside a list of servers says nothing an address can
            return None
        where = ",".join(hosts) if isinstance(hosts, (list, tuple)) \
            else str(hosts)
    else:
        if not host:
            return None
        where = f"[{host}]" if ":" in str(host) else str(host)
        if not port and default_port:
            # a side the long form left at port 0 - which an address,
            # given the engine's own port, cannot say
            return None
        if port and int(port) != int(default_port or 0):
            where += f":{int(port)}"
    if engine in CONNECTS_TO:
        first = ep.pop(CONNECTS_TO[engine], "")
        if first:
            path = "/" + quote(str(first), safe="")
            if dbs:
                path += "/" + ",".join(quote(str(d), safe="") for d in dbs)
        elif dbs:
            return None
    elif dbs:
        path = "/" + ",".join(quote(str(d), safe="") for d in dbs)
    query = _query(engine, ep)
    if query is None:
        return None
    return f"{scheme}://{auth}{where}{path}{query}"


def _query(engine, options):
    """The options left as an address's query, or None where one cannot be
    written there."""
    if not options:
        return ""
    if engine == "mongodb":
        if set(options) != {"uri_options"}:
            return None
        return "?" + str(options["uri_options"]) if options[
            "uri_options"] else ""
    out = {}
    for k, v in options.items():
        if isinstance(v, (dict, list, tuple)):
            return None
        out[k] = ("true" if v else "false") if isinstance(v, bool) else v
    if engine == "mysql":
        if "ssl_verify_identity" in out:
            if out.pop("ssl_verify_identity") != "false" or not out.get(
                    "ssl_ca"):
                return None
            out["ssl-mode"] = "VERIFY_CA"
        elif out.get("ssl_ca"):
            out["ssl-mode"] = "VERIFY_IDENTITY"
        if out.pop("ssl", None) == "true":
            if "ssl-mode" in out:
                return None
            out["ssl-mode"] = "REQUIRED"
        for k in ("ssl_ca", "ssl_cert", "ssl_key"):
            if k in out:
                out[k.replace("_", "-")] = out.pop(k)
    from . import hopkeys
    known = hopkeys.endpoint_keys_for(engine)
    for k in out:
        if k not in known and not (engine == "mysql" and k in (
                "ssl-mode", "ssl-ca", "ssl-cert", "ssl-key")):
            return None
    return "?" + urlencode(out)
