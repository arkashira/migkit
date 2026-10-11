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
import hashlib
import json
import threading
import urllib.error
import urllib.request

MAGIC = b"\x00"


class Refused(Exception):
    """The registry would not take a schema: its compatibility rule says
    the change would break the consumers already reading the subject."""


class Missing(RuntimeError):
    """The registry has no schema under the id asked for."""


class Registry:
    def __init__(self, url, user="", password=""):
        self.url = str(url).rstrip("/")
        self._auth = (("Basic " + base64.b64encode(
            f"{user}:{password}".encode()).decode()) if user else None)
        self._ids, self._entries, self._prints = {}, {}, {}
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
            if e.code == 404:
                raise Missing(f"the schema registry answered 404:"
                              f" {said[:300]}")
            raise RuntimeError(f"the schema registry answered {e.code}:"
                               f" {said[:300]}")

    def register(self, subject, schema, kind="AVRO"):
        """The id of `schema` under `subject`, registering it where it is
        new; `Refused` where the subject's compatibility rule does not take
        it. `schema` is a dict (Avro, JSON Schema) or the schema's text
        (any `kind`: AVRO, JSON or PROTOBUF - the registry's own names)."""
        text = (schema if isinstance(schema, str)
                else json.dumps(schema, sort_keys=True))
        with self._lock:
            got = self._ids.get((subject, kind, text))
        if got is not None:
            return got
        body = {"schema": text}
        if kind != "AVRO":
            body["schemaType"] = kind
        got = int(self._ask("POST", f"/subjects/{_q(subject)}/versions",
                            body)["id"])
        with self._lock:
            self._ids[(subject, kind, text)] = got
            self._entries[got] = (kind, text, [])
        return got

    def entry(self, schema_id):
        """(kind, schema text, references) of a schema by its id: the kind
        is the registry's `schemaType` - AVRO where it names none. A
        schema that refers to others (an import, a `$ref` into another
        subject) lists them as references. `Missing` for an id the
        registry does not hold."""
        with self._lock:
            got = self._entries.get(schema_id)
        if got is None:
            said = self._ask("GET", f"/schemas/ids/{schema_id}")
            got = (str(said.get("schemaType") or "AVRO").upper(),
                   said["schema"], list(said.get("references") or []))
            with self._lock:
                self._entries[schema_id] = got
        return got

    def schema(self, schema_id):
        """An Avro schema by its id, parsed; a schema of another kind is
        not one."""
        kind, text, _ = self.entry(schema_id)
        if kind != "AVRO":
            raise ValueError(f"schema {schema_id} is a {kind} schema, not an"
                             " Avro one")
        return json.loads(text)

    def fingerprint(self, schema_id):
        """The schema's canonical form's fingerprint: the same for the same
        schema whatever registry numbered it."""
        with self._lock:
            got = self._prints.get(schema_id)
        if got is None:
            kind, text, refs = self.entry(schema_id)
            if kind == "AVRO":
                from fastavro.schema import fingerprint, \
                    to_parsing_canonical_form
                got = fingerprint(to_parsing_canonical_form(
                    self.schema(schema_id)), "SHA-256")
            else:
                # no canonical form is defined for these here: the text,
                # with a JSON schema's keys put in order, and its
                # references
                if kind == "JSON":
                    try:
                        text = json.dumps(json.loads(text), sort_keys=True)
                    except ValueError:
                        pass
                got = hashlib.sha256(json.dumps(
                    [kind, text, refs], sort_keys=True).encode()
                ).hexdigest()
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
    """The registry an endpoint names (`schema_registry`), or None. Its
    password is read as a secret (`env:NAME`, `vault:...`): it went out as
    the reference's own text (audit 2026-10-01, K7)."""
    opts = getattr(ep, "options", None) or {}
    url = opts.get("schema_registry")
    if not url:
        return None
    return Registry(url, opts.get("schema_registry_user", ""),
                    ep.option_secret("schema_registry_password") or "")
