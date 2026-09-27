"""A schema registry spoken to over its HTTP API (the one Confluent's
registry defines and Redpanda's, Apicurio's and Karapace's also speak), and
the framing a registry's clients put on every message: a zero byte, the
schema's id in four bytes, then the body.

Only what migkit needs: register a schema under a subject and learn its id,
read a schema by its id, and a schema's fingerprint - the same for the same
schema in any registry, so a message whose id another registry gave it can
be compared with one written through this one.
"""
import base64
import json
import threading
import urllib.error
import urllib.request

MAGIC = b"\x00"


class Refused(Exception):
    """The registry would not take a schema: its compatibility rule says
    the change would break the consumers already reading the subject."""


class Registry:
    def __init__(self, url, user="", password=""):
        self.url = str(url).rstrip("/")
        self._auth = (("Basic " + base64.b64encode(
            f"{user}:{password}".encode()).decode()) if user else None)
        self._ids, self._schemas, self._prints = {}, {}, {}
        self._lock = threading.Lock()

    def _ask(self, method, path, body=None):
        req = urllib.request.Request(
            self.url + path, method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type":
                     "application/vnd.schemaregistry.v1+json",
                     **({"Authorization": self._auth} if self._auth
                        else {})})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode() or "null")
        except urllib.error.HTTPError as e:
            said = e.read().decode(errors="replace")
            if e.code == 409:
                raise Refused(said)
            raise RuntimeError(f"the schema registry answered {e.code}:"
                               f" {said[:300]}")

    def register(self, subject, schema):
        """The id of `schema` (a dict) under `subject`, registering it
        where it is new; `Refused` where the subject's compatibility rule
        does not take it."""
        text = json.dumps(schema, sort_keys=True)
        with self._lock:
            got = self._ids.get((subject, text))
        if got is not None:
            return got
        got = int(self._ask("POST", f"/subjects/{_q(subject)}/versions",
                            {"schema": text})["id"])
        with self._lock:
            self._ids[(subject, text)] = got
            self._schemas[got] = schema
        return got

    def schema(self, schema_id):
        with self._lock:
            got = self._schemas.get(schema_id)
        if got is None:
            got = json.loads(self._ask("GET", f"/schemas/ids/{schema_id}")
                             ["schema"])
            with self._lock:
                self._schemas[schema_id] = got
        return got

    def fingerprint(self, schema_id):
        """The schema's canonical form's fingerprint: the same for the same
        schema whatever registry numbered it."""
        with self._lock:
            got = self._prints.get(schema_id)
        if got is None:
            from fastavro.schema import fingerprint, \
                to_parsing_canonical_form
            got = fingerprint(to_parsing_canonical_form(
                self.schema(schema_id)), "SHA-256")
            with self._lock:
                self._prints[schema_id] = got
        return got


def _q(subject):
    return urllib.request.quote(str(subject), safe="")


def framed(schema_id, body):
    return MAGIC + int(schema_id).to_bytes(4, "big") + body


def frame_of(raw):
    """(schema id, body) of a framed message, or None for one that is not
    (JSON, plain bytes, a tombstone)."""
    if raw and len(raw) >= 5 and raw[:1] == MAGIC:
        return int.from_bytes(raw[1:5], "big"), raw[5:]
    return None


def of(ep):
    """The registry an endpoint names (`schema_registry`), or None."""
    opts = getattr(ep, "options", None) or {}
    url = opts.get("schema_registry")
    if not url:
        return None
    return Registry(url, opts.get("schema_registry_user", ""),
                    opts.get("schema_registry_password", ""))
