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

    def latest(self, subject):
        """{"type": "AVRO" | "JSON" | "PROTOBUF", "schema": the schema -
        parsed for Avro and JSON, the text for Protobuf} of a subject's
        newest version, or None where the registry has no such subject."""
        try:
            got = self._ask("GET", f"/subjects/{_q(subject)}/versions/latest")
        except Missing:
            return None
        kind = str(got.get("schemaType") or "AVRO").upper()
        text = got["schema"]
        return {"type": kind,
                "schema": text if kind == "PROTOBUF" else json.loads(text)}

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


def _label(t):
    """An Avro type as a field's type is told apart: `int`, `long`,
    `union<null|string>`, `array<int>`, `record Address`, `decimal` with its
    logical type."""
    if isinstance(t, str):
        return t
    if isinstance(t, list):
        return "union<" + "|".join(_label(x) for x in t) + ">"
    if not isinstance(t, dict):
        return str(t)
    kind = t.get("type")
    logical = t.get("logicalType")
    if kind == "array":
        return f"array<{_label(t.get('items'))}>"
    if kind == "map":
        return f"map<{_label(t.get('values'))}>"
    if kind in ("record", "enum", "fixed"):
        return f"{kind} {t.get('name', '')}".strip()
    base = _label(kind)
    return f"{base}({logical})" if logical else base


def _fields(t, path, out, depth=0):
    """{field path: its type's label} of an Avro type, records inside
    records, arrays, maps and unions followed to their fields."""
    if depth > 24:
        return
    if isinstance(t, list):
        for x in t:
            _fields(x, path, out, depth + 1)
    elif isinstance(t, dict):
        kind = t.get("type")
        if kind == "record":
            for f in t.get("fields") or []:
                name = f"{path}.{f['name']}" if path else str(f["name"])
                out[name] = _label(f.get("type"))
                _fields(f.get("type"), name, out, depth + 1)
        elif kind == "array":
            _fields(t.get("items"), path + "[]", out, depth + 1)
        elif kind == "map":
            _fields(t.get("values"), path + "{}", out, depth + 1)
        elif isinstance(kind, (dict, list)):
            _fields(kind, path, out, depth + 1)


def field_drift(a, b):
    """What differs between two schemas of one subject (`Registry.latest`)
    as clauses: for Avro, each field whose type is one on the first and
    another on the second (`int` against `long`), and each field only one
    has; for a schema kept as text, whether the two differ at all."""
    if a["type"] != b["type"]:
        return [f"is {a['type']} on the source and {b['type']} on the"
                " target"]
    if a["type"] != "AVRO":
        return [] if a["schema"] == b["schema"] else [
            f"differs between the registries ({a['type']}, compared as a"
            " whole)"]
    fa, fb = {}, {}
    _fields(a["schema"], "", fa)
    _fields(b["schema"], "", fb)
    if not fa and not fb and a["schema"] != b["schema"]:
        # not records: a bare type
        return [f"is {_label(a['schema'])} on the source and"
                f" {_label(b['schema'])} on the target"]
    out = [f"field {n} is {fa[n]} on the source and {fb[n]} on the target"
           for n in sorted(fa) if n in fb and fa[n] != fb[n]]
    out += [f"field {n} is only on the {'source' if n in fa else 'target'}"
            for n in sorted(set(fa) ^ set(fb))]
    return out


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
