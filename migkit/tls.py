"""Whether a connection to a server can be verified, asked of the server.

A connection a hop does not configure went out as it always had: libpq's
`prefer`, MySQL's TLS with nothing checked, MongoDB, Redis and Cassandra
in the clear unless the operator wrote it otherwise. Each of those is
open to anyone on the path who presents a certificate of their own. Most
servers reachable over a network present one that verifies - a public
authority's, or the one the hop names - and then there is no reason not
to check it: so the server is asked, once per process, and where its
certificate verifies against the system's authorities or the hop's own,
every connection to it is made verifying it. Where it does not, the hop
goes on connecting as before, and `assess` and `doctor` say so as a
failure (`posture`) - a hop that worked keeps working, and nobody is left
believing a connection is checked that is not.

Asked the way each protocol asks: PostgreSQL by its `SSLRequest`, MySQL
by the TLS request after the server's greeting, the rest by a handshake
from the first byte.

A connection is "local" only where its rows cross no network migkit does
not encrypt: a server on this machine, or one reached through the hop's
own SSH tunnel. A tunnel the hop opens with a command of its own (a
cloud's port forward, `kubectl port-forward`, socat) encrypts what it
encrypts, which migkit cannot tell: before, every tunnel counted as
local, so a plain forwarder turned the automatic check off and passed
the posture. Every engine migkit reaches over a network is in the
posture, said from how migkit connects to it; what cannot be told is
"unknown", a warning.
"""
import os
import socket
import ssl
import struct
import threading
from urllib.parse import urlsplit

#: what a probe found, per (host, port, how, ca): the state and the file
#: of authorities that verified it
_SEEN = {}
_LOCK = threading.Lock()
#: seconds a probe waits for the server, each step. Short: a server that
#: does not answer in it is connected to as before, which is the safe side
#: of the question, and a run that cannot reach a server does not wait on
#: the probe as well as on the connection
TIMEOUT = 1.5

LOOPBACK = ("127.0.0.1", "::1", "localhost", "")


def tunnel_kind(ep):
    """How the hop's tunnel carries an endpoint: "ssh", "command", or
    None where it has none - read from the tunnel it names (`tunnel`), or
    from the one opened for it (`tunnel_by`, set by `tunnel.Tunnel.open`;
    an opened tunnel that does not say is taken as a command's)."""
    opts = getattr(ep, "options", None) or {}
    if opts.get("tunnel_to"):
        return "ssh" if opts.get("tunnel_by") == "ssh" else "command"
    spec = opts.get("tunnel")
    if not spec:
        return None
    if isinstance(spec, str):
        return "ssh" if spec.startswith("ssh://") else "command"
    return "ssh" if isinstance(spec, dict) and spec.get("ssh") \
        else "command"


def on_this_machine(ep):
    """Whether the server is on this machine itself: a loopback address or
    a socket, and not the near end of a tunnel to another."""
    if tunnel_kind(ep):
        return False
    host = str(getattr(ep, "host", "") or "")
    return host in LOOPBACK or host.startswith("/") \
        or host.startswith("127.")


def local(ep):
    """Whether an endpoint's rows never cross a network migkit does not
    encrypt: a server on this machine, or a leg carried by the hop's own
    SSH tunnel (`tunnel.py`). A command's tunnel is not: what it does
    with the rows is its own."""
    return on_this_machine(ep) or tunnel_kind(ep) == "ssh"


def authorities():
    """The files of authorities a certificate is verified against when
    the hop names none: the system's, as OpenSSL finds it (measured on
    macOS: `/etc/ssl/cert.pem`, 128 authorities), and the bundle Python's
    requests ship (`certifi`) for a machine whose OpenSSL has none at its
    default path. Files, not the store: every driver and every program
    libpq runs takes a file."""
    out = []
    paths = ssl.get_default_verify_paths()
    for f in (paths.cafile, paths.openssl_cafile):
        if f and os.path.isfile(f) and f not in out:
            out.append(f)
    try:
        import certifi
        if os.path.isfile(certifi.where()) and certifi.where() not in out:
            out.append(certifi.where())
    except ImportError:
        pass
    return out


def _open(host, port, how):
    """A socket ready for the TLS handshake, or None where the server
    offers no TLS on it."""
    s = socket.create_connection((host, int(port)), timeout=TIMEOUT)
    s.settimeout(TIMEOUT)
    try:
        if how == "postgres":
            s.sendall(struct.pack("!ii", 8, 80877103))
            if s.recv(1) != b"S":
                s.close()
                return None
        elif how == "mysql":
            head = _recv(s, 4)
            size = int.from_bytes(head[:3], "little")
            greeting = _recv(s, size)
            caps = _mysql_caps(greeting)
            if not caps & 0x0800:
                s.close()
                return None
            # CLIENT_SSL | PROTOCOL_41 | SECURE_CONNECTION | LONG_PASSWORD
            flags = 0x0800 | 0x0200 | 0x8000 | 0x0001
            body = struct.pack("<IIB", flags, 1 << 24, 45) + b"\0" * 23
            s.sendall(len(body).to_bytes(3, "little") + b"\x01" + body)
        return s
    except Exception:
        s.close()
        raise


def _recv(s, n):
    got = b""
    while len(got) < n:
        part = s.recv(n - len(got))
        if not part:
            raise ConnectionError("closed")
        got += part
    return got


def _mysql_caps(greeting):
    """The capability flags of a MySQL greeting (protocol 10)."""
    if not greeting or greeting[0] != 10:
        return 0
    at = greeting.index(b"\0", 1) + 1 + 4 + 8 + 1
    low = int.from_bytes(greeting[at:at + 2], "little")
    high = (int.from_bytes(greeting[at + 5:at + 7], "little")
            if len(greeting) >= at + 7 else 0)
    return low | (high << 16)


def _attempt(host, port, how, ctx):
    """"tls" where the handshake under `ctx` went through, "none" where
    the server offers no TLS, None where it could not be told; a
    certificate that does not verify under `ctx` raises
    `ssl.SSLCertVerificationError`.

    A server reached that answers a handshake with something else, closes
    on it or says nothing to it is one without TLS on that port, where TLS
    starts from the first byte: measured, a plain Redis 7 says nothing -
    it waits for the rest of a line of text that never comes - where a
    TLS server answers within one round trip, far inside the wait. After
    PostgreSQL's or MySQL's own request for TLS, which the server said yes
    to, a handshake that fails is not known to be anything."""
    try:
        s = _open(host, port, how)
    except (OSError, ValueError):
        return None
    if s is None:
        return "none"
    try:
        with ctx.wrap_socket(s, server_hostname=host) as t:
            t.do_handshake()
        return "tls"
    except ssl.SSLCertVerificationError:
        raise
    except (ssl.SSLError, OSError):
        return "none" if how == "direct" else None
    finally:
        s.close()


def probe(host, port, how="direct", ca=None):
    """(state, authorities) of the server's TLS on `host:port`:

    "verified"    its certificate verifies, name and all, against `ca` -
                  or, with none given, the system's authorities; the
                  file that verified it is the second value
    "unverified"  it speaks TLS, with a certificate that does not verify
    "none"        it offers no TLS
    None          it could not be asked (not reached, or not this
                  protocol)

    Once per process for each server: every connection of a run asks for
    its options, and the answer does not change within one."""
    key = (str(host), int(port), how, ca or "")
    with _LOCK:
        if key in _SEEN:
            return _SEEN[key]
    got = _probe(str(host), int(port), how, ca)
    with _LOCK:
        _SEEN[key] = got
    return got


def _probe(host, port, how, ca):
    for f in ([ca] if ca else authorities()):
        try:
            got = _attempt(host, port, how,
                           ssl.create_default_context(cafile=f))
        except ssl.SSLCertVerificationError:
            continue
        return {"tls": ("verified", f), "none": ("none", None)}.get(
            got, (None, None))
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    got = _attempt(host, port, how, ctx)
    return {"tls": ("unverified", None), "none": ("none", None)}.get(
        got, (None, None))


def auto(ep, how, ca=None):
    """The authorities to verify a hop's server against where the hop
    asks nothing of its TLS: the file that verified it, or None - then the
    connection is made as before. Never asked of a local endpoint, nor
    through a tunnel, whose near end is this machine's address and not
    the name on the server's certificate."""
    if local(ep) or tunnel_kind(ep) or not getattr(ep, "port", None):
        return None
    state, found = probe(ep.host, ep.port, how, ca)
    return found if state == "verified" else None


#: set while a command's tunnel is judged: the server is not asked, since
#: what answers on the tunnel's near end is not the server's name
_QUIET = threading.local()


def posture(ep, family):
    """{"state", "detail"} of the connection migkit makes to `ep`, for
    `assess` and `doctor`: "local", "verified", "unverified", "plain" or
    "unknown". "unverified" and "plain" to a host that is not local are
    failures, "unknown" a warning."""
    fn = _POSTURE.get(family)
    if fn is None:
        return None
    if fn is _files or getattr(fn, "service", False):
        # files: on this machine's disks nothing is said, as before; a
        # cloud's service is reached by its SDK, and a hop naming no host
        # for it is not a server on this machine
        return fn(ep)
    kind = tunnel_kind(ep)
    if kind == "ssh":
        return {"state": "local",
                "detail": "carried by the hop's SSH tunnel, which encrypts"
                          " it as far as the tunnel's own machine"}
    if on_this_machine(ep):
        return {"state": "local",
                "detail": "on this machine: no row crosses a network"}
    if kind == "command":
        _QUIET.on = True
        try:
            got = fn(ep)
        finally:
            _QUIET.on = False
        if got is None or got["state"] == "verified":
            return got
        return {"state": "unknown",
                "detail": "carried by the tunnel's own command, which"
                          " encrypts what it encrypts - migkit cannot tell."
                          " Ask the server for TLS and check its"
                          " certificate, or carry it through an SSH tunnel"
                          " (tunnel: {ssh: ...})"}
    return fn(ep)


def _from_probe(ep, how, ca=None, fix=""):
    if getattr(_QUIET, "on", False):
        return {"state": "unknown", "detail": "not asked through a tunnel"}
    state, _ = probe(ep.host, ep.port, how, ca)
    if state == "verified":
        return {"state": "verified",
                "detail": "the server's certificate is verified, its name"
                          " included" + (" (it verifies against the"
                                         " system's authorities, so migkit"
                                         " checks it)" if not ca else "")}
    if state == "unverified":
        return {"state": "unverified",
                "detail": "encrypted, but the server's certificate does not"
                          " verify against the system's authorities, so it"
                          f" is not checked - {fix}"}
    if state == "none":
        return {"state": "plain",
                "detail": "the server offers no TLS: every row crosses the"
                          f" network as it is - {fix}"}
    return {"state": "unknown",
            "detail": "the server could not be asked whether it speaks TLS"}


def _pg(ep):
    o = ep.options or {}
    mode = str(o.get("sslmode") or "")
    fix = ("name the authority that signed it (sslrootcert) and"
           " sslmode: verify-full")
    if mode == "verify-full":
        return {"state": "verified", "detail": "sslmode verify-full"}
    if mode == "verify-ca":
        return {"state": "verified", "detail": "sslmode verify-ca: the"
                " certificate is checked, the name on it is not"}
    if mode == "disable":
        return {"state": "plain", "detail": f"sslmode disable - {fix}"}
    if mode:
        return {"state": "unverified",
                "detail": f"sslmode {mode}: the certificate is not checked"
                          f" - {fix}"}
    return _from_probe(ep, "postgres", o.get("sslrootcert"), fix)


def _my(ep):
    o = ep.options or {}
    fix = "name the authority that signed it (ssl_ca)"
    if o.get("ssl_ca"):
        name = o.get("ssl_verify_identity", True)
        return {"state": "verified",
                "detail": "ssl_ca: the certificate is checked"
                          + ("" if name else ", the name on it is not")}
    if o.get("ssl"):
        return {"state": "unverified",
                "detail": f"ssl: true checks nothing - {fix}"}
    return _from_probe(ep, "mysql", None, fix)


def _direct(option_ca, option_on, option_off, fix):
    def one(ep):
        o = ep.options or {}
        extra = str(o.get("uri_options") or "").lower()
        if o.get(option_off) or "tlsinsecure=true" in extra \
                or "tlsallowinvalidcertificates=true" in extra:
            return {"state": "unverified",
                    "detail": f"{option_off}: the certificate is not"
                              f" checked - {fix}"}
        if o.get(option_ca) or o.get(option_on) or "tls=true" in extra \
                or "ssl=true" in extra:
            return {"state": "verified",
                    "detail": "TLS, the certificate checked"
                              + (f" against {option_ca}"
                                 if o.get(option_ca) else "")}
        return _from_probe(ep, "direct", None, fix)
    return one


def _mssql(ep):
    fix = ("the driver migkit reads SQL Server through checks no"
           " certificate: carry the connection through the hop's SSH tunnel"
           " (tunnel:)")
    enc = (ep.mssql_tls() if hasattr(ep, "mssql_tls") else {}).get(
        "encryption", "")
    if enc == "off":
        return {"state": "plain", "detail": f"encrypt: off - {fix}"}
    return {"state": "unverified",
            "detail": ("encrypted" if enc == "require" else
                       "encrypted where the server asks for it")
                      + f", the certificate not checked - {fix}"}


_TUNNEL_FIX = ("carry it through the hop's SSH tunnel (tunnel: {ssh: ...})"
               " to a machine beside the server")


def _plain_always(what):
    """An engine whose client migkit connects with asks for no TLS."""
    def one(ep):
        return {"state": "plain",
                "detail": f"{what} - every row crosses the network as it"
                          f" is: {_TUNNEL_FIX}"}
    return one


def _by_scheme(url, fix):
    """The state an address's own scheme says, or None where it says
    neither: https is TLS whose certificate the client checks by
    default; http is none."""
    scheme = urlsplit(str(url or "")).scheme.lower()
    if scheme in ("https", "couchbases", "grpcs"):
        return {"state": "verified",
                "detail": f"{scheme}: the certificate checked by the client"}
    if scheme in ("http", "couchbase", "grpc"):
        return {"state": "plain",
                "detail": f"{scheme}: every row crosses the network as it"
                          f" is - {fix}"}
    return None


def _options(on, ca=None, insecure=None, fix=""):
    """An engine whose client speaks TLS when the endpoint says `on`,
    checking the certificate unless it says `insecure`."""
    def one(ep):
        o = ep.options or {}
        if o.get("url"):
            got = _by_scheme(o["url"], fix)
            if got is not None:
                if got["state"] == "verified" and insecure \
                        and o.get(insecure):
                    return {"state": "unverified",
                            "detail": f"{insecure}: the certificate is not"
                                      f" checked - {fix}"}
                return got
        if o.get(on) or (ca and o.get(ca)):
            if insecure and o.get(insecure):
                return {"state": "unverified",
                        "detail": f"{insecure}: the certificate is not"
                                  f" checked - {fix}"}
            return {"state": "verified",
                    "detail": "TLS, the certificate checked"
                              + (f" against {ca}" if ca and o.get(ca)
                                 else "")}
        return {"state": "plain",
                "detail": f"no TLS asked for ({on} is not set): every"
                          f" row crosses the network as it is - {fix}"}
    return one


def _kafka(ep):
    o = ep.options or {}
    protocol = str(o.get("security_protocol") or "").upper()
    fix = "security_protocol: SSL or SASL_SSL (ssl_cafile for the authority)"
    if protocol in ("SSL", "SASL_SSL"):
        return {"state": "verified",
                "detail": f"{protocol}: the brokers' certificates checked"
                          + (" against ssl_cafile" if o.get("ssl_cafile")
                             else ", names included")}
    return {"state": "plain",
            "detail": f"{protocol or 'PLAINTEXT'}: every message - and a"
                      f" SASL password - crosses the network as it is -"
                      f" {fix}"}


def _cloud(option, what):
    """A cloud's service, reached through its SDK over HTTPS with the
    certificate checked, unless the endpoint names another address
    (`option`: an emulator, a private endpoint)."""
    def one(ep):
        o = ep.options or {}
        named = o.get(option)
        if named:
            got = _by_scheme(named if "://" in str(named)
                             else f"https://{named}", _TUNNEL_FIX)
            if got is not None:
                return got
        return {"state": "verified",
                "detail": f"{what} over HTTPS, the certificate checked by"
                          " its client"}
    one.service = True
    return one


def _redshift(ep):
    if (ep.options or {}).get("iam"):
        return {"state": "verified",
                "detail": "signed in by IAM, over TLS the driver checks"
                          " the certificate of (verify-ca: the name on it"
                          " is not)"}
    return _pg(ep)


def _service(fn):
    fn.service = True
    return fn


@_service
def _pubsub(ep):
    if ep.host:
        return {"state": "plain",
                "detail": "an emulator at host and port, spoken to without"
                          " TLS"}
    return {"state": "verified",
            "detail": "Pub/Sub over TLS, the certificate checked by its"
                      " client"}


@_service
def _bigquery(ep):
    o = ep.options or {}
    at = o.get("api_endpoint") or o.get("write_endpoint")
    if at and o.get("anonymous") and "://" not in str(at):
        return {"state": "plain",
                "detail": "an emulator signed into as nobody, spoken to"
                          " without TLS"}
    return _cloud("api_endpoint", "BigQuery")(ep)


def _emulated(what):
    """A Google service reached through its SDK over TLS, the certificate
    checked by its client - or its emulator, which the SDK speaks to in
    plain text."""
    def one(ep):
        if (ep.options or {}).get("emulator"):
            return {"state": "plain",
                    "detail": "an emulator at host and port, spoken to"
                              " without TLS"}
        return {"state": "verified",
                "detail": f"{what} over TLS, the certificate checked by its"
                          " client"}
    one.service = True
    return one


def _neptune(ep):
    """Neptune over wss and https, the certificate checked by the client,
    unless the side says `tls: false` (a Gremlin server reached without
    it)."""
    if (ep.options or {}).get("tls") is False:
        return {"state": "plain",
                "detail": "tls: false - every element crosses the network"
                          f" as it is: {_TUNNEL_FIX}"}
    return {"state": "verified",
            "detail": "wss and https, the certificate checked by the client"}


def _opensearch(ep):
    o = ep.options or {}
    if not o.get("tls"):
        return {"state": "plain",
                "detail": "http: every document and the password cross the"
                          " network as they are - tls: true"}
    if o.get("verify") is False:
        return {"state": "unverified",
                "detail": "verify: false - the certificate is not checked"}
    return {"state": "verified", "detail": "https, the certificate checked"}


def _cosmos(ep):
    o = ep.options or {}
    fix = "leave tls on, and the certificate checked"
    if o.get("url"):
        got = _by_scheme(o["url"], fix)
        if got is not None and got["state"] == "plain":
            return got
    elif o.get("tls") is False:
        return {"state": "plain", "detail": f"tls: false - {fix}"}
    if o.get("tls_insecure"):
        return {"state": "unverified",
                "detail": f"tls_insecure: the certificate is not checked -"
                          f" {fix}"}
    return {"state": "verified", "detail": "https, the certificate checked"}


def _teradata(ep):
    """The driver encrypts the data whatever TLS is taken (`encryptdata`),
    which authenticates no server: only a certificate checked makes it
    verified."""
    mode = str((ep.options or {}).get("sslmode") or "").upper()
    if mode in ("VERIFY-FULL", "VERIFY-CA"):
        return {"state": "verified",
                "detail": f"sslmode {mode.lower()}: the certificate checked"
                          + ("" if mode == "VERIFY-FULL"
                             else ", the name on it not")}
    return {"state": "unverified",
            "detail": "encrypted, the server not checked - name the"
                      " authority that signed it (sslrootcert) and sslmode:"
                      " verify-full"}


@_service
def _sapodp(ep):
    o = ep.options or {}
    fix = "leave tls on, and the certificate checked"
    if o.get("tls", True) is False:
        return {"state": "plain", "detail": f"tls: false - the rows and the"
                                            f" user's password cross as they"
                                            f" are - {fix}"}
    if o.get("tls_insecure"):
        return {"state": "unverified",
                "detail": f"tls_insecure: the certificate is not checked -"
                          f" {fix}"}
    return {"state": "verified",
            "detail": "https, the certificate checked"
                      + (" against tls_ca_file" if o.get("tls_ca_file")
                         else "")}


@_service
def _saas(ep):
    return {"state": "unknown",
            "detail": "the service is read by the program that speaks its"
                      " API, with that program's own client: whether it"
                      " checks the service's certificate is the program's"
                      " to say, not migkit's"}


def _files(ep):
    """Files: on this machine, or in an object store its SDK reaches over
    HTTPS unless `endpoint_url` names plain HTTP."""
    o = ep.options or {}
    where = str(o.get("url") or o.get("path") or ep.host or "")
    scheme = urlsplit(where).scheme.lower()
    if scheme in ("", "file") or len(scheme) == 1:
        # files on this machine's own disks: nothing to say
        return None
    named = o.get("endpoint_url")
    if named:
        got = _by_scheme(named, "an https endpoint_url")
        if got is not None:
            return got
    return {"state": "verified",
            "detail": f"{scheme}: the store's client over HTTPS, the"
                      " certificate checked"}


_POSTURE = {
    "postgres": _pg,
    "mysql": _my,
    "mongodb": _direct("tls_ca_file", "tls", "tls_insecure",
                       "name the authority that signed it (tls_ca_file)"),
    "redis": _direct("tls_ca_file", "tls", "tls_insecure",
                     "name the authority that signed it (tls_ca_file)"),
    "cassandra": _direct("tls_ca_file", "tls", "tls_insecure",
                         "name the authority that signed it"
                         " (tls_ca_file)"),
    "mssql": _mssql,
    "redshift": _redshift,
    "kafka": _kafka,
    "clickhouse": _options("secure", fix="secure: true, to the server's TLS"
                           " port"),
    "opensearch": _opensearch,
    "couchbase": _options("tls", "tls_ca_file", "tls_insecure",
                          fix="tls: true (couchbases://)"),
    "cosmosdb": _cosmos,
    "db2i": _options("tls", "tls_ca_file", "tls_insecure",
                     fix="tls: true (tls_ca_file for the authority)"),
    "oracle": _plain_always("the connection string asks for no TLS (a"
                            " TCPS address is not written by migkit)"),
    "db2": _plain_always("the connection string asks for no TLS"
                         " (SECURITY=SSL is not written by migkit)"),
    "db2zos": _plain_always("the connection string asks for no TLS"
                            " (SECURITY=SSL is not written by migkit)"),
    "ase": _plain_always("the ODBC connection asks for no encryption"),
    "snowflake": _cloud("url", "Snowflake"),
    "bigquery": _bigquery,
    "dynamodb": _cloud("endpoint_url", "DynamoDB"),
    "kinesis": _cloud("endpoint_url", "Kinesis"),
    "pubsub": _pubsub,
    "teradata": _teradata,
    "sapodp": _sapodp,
    "saas": _saas,
    # the cloud-native services (P0-CLOUDS part 2): Google's by their
    # SDKs unless an emulator is named, Neptune over wss and https unless
    # `tls: false`, the MySQL client's TLS where a side speaks through one
    **{name: _emulated(title) for name, title in (
        ("spanner", "Spanner"), ("bigtable", "Bigtable"),
        ("firestore", "Firestore"), ("datastore", "Datastore"))},
    "neptune": _neptune,
    "timestream": _cloud("endpoint_url", "Timestream"),
    "oceanbase-oracle": _my,
    "lindorm": _my,
    **{name: _files for name in ("sqlite", "duckdb", "parquet", "copybook",
                                 "iceberg", "delta")},
}
