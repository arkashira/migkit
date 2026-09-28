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
"""
import os
import socket
import ssl
import struct
import threading

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


def local(ep):
    """Whether an endpoint's rows never cross a network this machine does
    not own: a server on this machine, a socket, or a leg carried by the
    hop's own SSH tunnel (`tunnel.py`), which encrypts it."""
    opts = getattr(ep, "options", None) or {}
    if opts.get("tunnel") or opts.get("tunnel_to"):
        return True
    host = str(getattr(ep, "host", "") or "")
    return host in LOOPBACK or host.startswith("/") \
        or host.startswith("127.")


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
    connection is made as before. Never asked of a local endpoint."""
    if local(ep) or not getattr(ep, "port", None):
        return None
    state, found = probe(ep.host, ep.port, how, ca)
    return found if state == "verified" else None


def posture(ep, family):
    """{"state", "detail"} of the connection migkit makes to `ep`, for
    `assess` and `doctor`: "local", "verified", "unverified", "plain" or
    "unknown". The last three to a non-local host are failures."""
    fn = _POSTURE.get(family)
    if fn is None:
        return None
    if local(ep):
        via = (ep.options or {}).get("tunnel_to") or \
            (ep.options or {}).get("tunnel")
        return {"state": "local",
                "detail": "carried by the hop's SSH tunnel" if via
                else "on this machine: no row crosses a network"}
    return fn(ep)


def _from_probe(ep, how, ca=None, fix=""):
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
}
