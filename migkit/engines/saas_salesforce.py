"""Salesforce as a program source (backlog ER7): objects read through its
REST API, and followed through its own change events.

    source: salesforce://login.salesforce.com?config=/etc/migkit/sf.json
    options: {streams: [Account, Contact, Opportunity]}

The config signs in: an `access_token` with its `instance_url`, or a
connected app's `client_id` and `client_secret` (the client-credentials
flow, at the org's own domain) with a `refresh_token` where it has one.
An object's columns are its fields as its description gives them - a
compound field (an address, a location) by its parts, a formula field by
its value - and its key `Id`.

**Read.** An object is read by a Bulk API 2.0 query job, its results a
page of CSV at a time - the application's own bulk read - and where the
job cannot run (an object the bulk API does not serve, a user without
it), by the REST query's pages instead, said (`decide.attempt`). A
record is asked by its key through a query of `Id IN (...)`.

**Follow.** Change Data Capture through the Pub/Sub API: one
subscription a stream (`/data/<Object>ChangeEvent`), each event's Avro
payload read by the schema the API gives for it. The position is each
topic's replay id, kept as the bytes it is, never computed. A follow
starts from the oldest event the API keeps and passes over every event
committed before its copy began, less a minute of clock between the two
(`SKEW`): an event after that is applied in order, and applying one the
copy already holds again leaves the row as the copy has it, since each
event carries the fields it changed. An UPDATE carries the fields its
`changedFields` name, and sets to NULL those its `nulledFields` name; a
field whose event holds a diff (`diffFields`), a GAP event and an
UNDELETE have their records read again as they are now; a GAP_OVERFLOW
reads the object again whole. The API keeps events 72 hours: a follow
stopped longer, or a copy older, cannot go on, and says so. A formula
field changes with what it reads, with no event of its own, so only a
whole read compares it - said with the verdict.

**Proved.** The object's own count (`SELECT COUNT()`), before and after a
whole read, held to the records read (`read_proved`).
"""
import csv
import datetime
import io
import json
import time
from decimal import Decimal

from .saas import Program, Stream, value_of

API = "v62.0"
PUBSUB = "api.pubsub.salesforce.com:7443"
#: the change events the API keeps
RETENTION = datetime.timedelta(hours=72)
#: the clock of this machine and the org's, apart by at most this: an
#: event this much before the copy began is still applied
SKEW = datetime.timedelta(minutes=1)
#: records a key query asks for: its SOQL stays under the API's 100,000
#: characters and the URL under 16 KB
IDS_A_QUERY = 200
#: events asked for in one fetch, and how long a fetch waits for them
EVENTS_A_FETCH = 100
WAIT = 20
#: field types, in the words `canon` maps
WORDS = {"id": "text", "string": "text", "picklist": "text",
         "multipicklist": "text", "reference": "text", "email": "text",
         "phone": "text", "url": "text", "textarea": "text",
         "combobox": "text", "encryptedstring": "text", "anyType": "text",
         "boolean": "boolean", "int": "integer", "long": "integer",
         "double": "decimal", "currency": "decimal", "percent": "decimal",
         "date": "date", "datetime": "timestamptz", "time": "time",
         "complexvalue": "json", "json": "json"}
#: fields a query cannot hand over as values
UNREAD = {"base64": "a file's bytes are fetched one record at a time, not"
                    " by a query",
          "address": "carried as its parts", "location": "carried as its"
                                                           " parts"}


def pubsub_messages():
    """The Pub/Sub API's messages migkit speaks, built here from the field
    numbers its documentation gives - no generated code, no file of the
    API's shipped - in a pool of their own."""
    known = pubsub_messages.__dict__.get("got")
    if known:
        return known
    from google.protobuf import descriptor_pb2, descriptor_pool
    from google.protobuf import message_factory
    f = descriptor_pb2.FieldDescriptorProto
    fdp = descriptor_pb2.FileDescriptorProto(
        name="migkit/eventbus_v1.proto", package="eventbus.v1",
        syntax="proto3")
    enum = fdp.enum_type.add(name="ReplayPreset")
    for name, n in (("LATEST", 0), ("EARLIEST", 1), ("CUSTOM", 2)):
        enum.value.add(name=name, number=n)

    def message(name, fields):
        m = fdp.message_type.add(name=name)
        for fname, number, typ, repeated, type_name in fields:
            m.field.add(name=fname, number=number, type=typ,
                        label=f.LABEL_REPEATED if repeated
                        else f.LABEL_OPTIONAL,
                        **({"type_name": type_name} if type_name else {}))
    s, b, i32, msg = f.TYPE_STRING, f.TYPE_BYTES, f.TYPE_INT32, \
        f.TYPE_MESSAGE
    message("EventHeader", [("key", 1, s, False, None),
                            ("value", 2, b, False, None)])
    message("ProducerEvent", [("id", 1, s, False, None),
                              ("schema_id", 2, s, False, None),
                              ("payload", 3, b, False, None),
                              ("headers", 4, msg, True,
                               ".eventbus.v1.EventHeader")])
    message("ConsumerEvent", [("event", 1, msg, False,
                               ".eventbus.v1.ProducerEvent"),
                              ("replay_id", 2, b, False, None)])
    message("FetchRequest", [("topic_name", 1, s, False, None),
                             ("replay_preset", 2, f.TYPE_ENUM, False,
                              ".eventbus.v1.ReplayPreset"),
                             ("replay_id", 3, b, False, None),
                             ("num_requested", 4, i32, False, None),
                             ("auth_refresh", 5, s, False, None)])
    message("FetchResponse", [("events", 1, msg, True,
                               ".eventbus.v1.ConsumerEvent"),
                              ("latest_replay_id", 2, b, False, None),
                              ("rpc_id", 3, s, False, None),
                              ("pending_num_requested", 4, i32, False,
                               None)])
    message("SchemaRequest", [("schema_id", 1, s, False, None)])
    message("SchemaInfo", [("schema_json", 1, s, False, None),
                           ("schema_id", 2, s, False, None),
                           ("rpc_id", 3, s, False, None)])
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    got = {n: message_factory.GetMessageClass(
        pool.FindMessageTypeByName(f"eventbus.v1.{n}"))
        for n in ("FetchRequest", "FetchResponse", "ConsumerEvent",
                  "ProducerEvent", "SchemaRequest", "SchemaInfo",
                  "EventHeader")}
    got["EARLIEST"], got["CUSTOM"] = 1, 2
    pubsub_messages.got = got
    return got


def topic_of(obj):
    """An object's change event channel: `AccountChangeEvent`, and a
    custom object's `__c` as `__ChangeEvent`."""
    name = obj[:-3] + "__ChangeEvent" if obj.endswith("__c") \
        else f"{obj}ChangeEvent"
    return f"/data/{name}"


def bitmap_fields(bitmaps, fields):
    """[(field, part or None)] a change event's bitmap names: bit n of a
    hexadecimal value the n-th field of the event's schema, from the
    least significant; `<n>-<hex>` the parts of the compound n-th
    field."""
    out = []
    for bm in bitmaps or []:
        bm = str(bm)
        if "-" in bm:
            idx, _, sub = bm.partition("-")
            parent = fields[int(idx)]
            inner = _record_fields(parent["type"])
            bits = int(sub, 16)
            out += [(parent["name"], inner[i]["name"])
                    for i in range(len(inner)) if bits >> i & 1]
        else:
            bits = int(bm, 16)
            out += [(fields[i]["name"], None)
                    for i in range(len(fields)) if bits >> i & 1]
    return out


def _record_fields(typ):
    if isinstance(typ, list):
        for t in typ:
            got = _record_fields(t)
            if got:
                return got
        return []
    if isinstance(typ, dict) and typ.get("type") == "record":
        return typ.get("fields") or []
    return []


def column_of(field, part):
    """The column a compound field's part is: a name's own part, an
    address's part after its prefix (`BillingAddress.City` is
    `BillingCity`); None for another compound's."""
    if part is None:
        return field
    if field == "Name":
        return part
    if field.endswith("Address"):
        return field[:-len("Address")] + part
    return None


def event_value(word, value):
    """A change event's value as its field means it: a datetime its
    milliseconds, a date its days, a time its milliseconds of the day, a
    number the double the event carries, read as its shortest digits."""
    if value is None:
        return None
    base = str(word).split("(")[0]
    if base == "timestamptz" and isinstance(value, int):
        return datetime.datetime.fromtimestamp(value / 1000,
                                               datetime.timezone.utc)
    if base == "date" and isinstance(value, int):
        return datetime.date(1970, 1, 1) + datetime.timedelta(days=value)
    if base == "time" and isinstance(value, int):
        return (datetime.datetime(2000, 1, 1)
                + datetime.timedelta(milliseconds=value)).time()
    if base in ("decimal", "number") and isinstance(value, float):
        return Decimal(repr(value))
    return value_of(word, value)


class Salesforce(Program):
    kind = "salesforce"
    said = "Salesforce"

    # ---- signing in, and the REST API ---------------------------------------

    def _auth(self, again=False):
        """{"instance_url", "access_token", "org"}, asked once, and again
        where a token has lapsed."""
        known = self.engine.__dict__.setdefault("_sf_auth", {})
        if self.side in known and not again:
            return known[self.side]
        import requests
        cfg = self.config()
        if cfg.get("access_token") and not again:
            got = {"instance_url": cfg["instance_url"].rstrip("/"),
                   "access_token": cfg["access_token"],
                   "org": cfg.get("org_id") or ""}
        else:
            host = str(self.options.get("program") or "login.salesforce.com")
            base = host if host.startswith(("http://", "https://")) \
                else f"https://{host}"
            form = {"client_id": cfg.get("client_id"),
                    "client_secret": cfg.get("client_secret")}
            form["grant_type"] = ("refresh_token" if cfg.get("refresh_token")
                                  else "client_credentials")
            if cfg.get("refresh_token"):
                form["refresh_token"] = cfg["refresh_token"]
            r = requests.post(f"{base}/services/oauth2/token", data=form,
                              timeout=60)
            if r.status_code >= 400:
                raise SystemExit("Salesforce refused the sign-in"
                                 f" ({r.status_code}): the config's"
                                 " connected app and its user")
            body = r.json()
            org = str(body.get("id") or "").rstrip("/").split("/")
            got = {"instance_url": body["instance_url"].rstrip("/"),
                   "access_token": body["access_token"],
                   "org": cfg.get("org_id") or (org[-2] if len(org) >= 2
                                                else "")}
        known[self.side] = got
        return got

    def _rest(self, method, path, **kw):
        """An answer of the REST API; a lapsed token signed in again once."""
        import requests
        for again in (False, True):
            auth = self._auth(again)
            url = path if path.startswith("http") else \
                auth["instance_url"] + path
            r = requests.request(method, url, headers={
                "Authorization": f"Bearer {auth['access_token']}",
                **kw.pop("headers", {})}, timeout=300, **kw)
            if r.status_code != 401:
                return r
        return r

    def _json(self, method, path, **kw):
        r = self._rest(method, path, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"Salesforce answered {r.status_code}:"
                               f" {r.text[:200]}")
        return json.loads(r.text, parse_float=Decimal) if r.text else {}

    def _base(self):
        return f"/services/data/{self.options.get('api_version') or API}"

    # ---- the catalogue -----------------------------------------------------------

    def _objects(self):
        only = self.options.get("streams") or self.config().get("objects")
        if not only:
            raise SystemExit("name the Salesforce objects to read with"
                             " `streams` in the side's options")
        return [only] if isinstance(only, str) else list(only)

    def catalog(self):
        out = {}
        for obj in self._objects():
            d = self._json("GET", f"{self._base()}/sobjects/{obj}/describe")
            cols, unread, formula = [], [], []
            for fld in d.get("fields") or []:
                typ = fld.get("type")
                if typ in UNREAD:
                    unread.append((fld["name"], UNREAD[typ]))
                    continue
                word = WORDS.get(typ, "json")
                if word == "text" and fld.get("length"):
                    word = f"text({int(fld['length'])})"
                if word == "decimal" and fld.get("precision"):
                    word = (f"decimal({int(fld['precision'])},"
                            f"{int(fld.get('scale') or 0)})")
                cols.append((fld["name"], word))
                if fld.get("calculated"):
                    formula.append(fld["name"])
            out[obj] = Stream(obj, cols, ["Id"], True,
                              {"formula": formula}, (), unread)
        return out

    def not_compared(self, stream):
        formula = (stream.raw or {}).get("formula") or []
        if not formula:
            return []
        return [f"{', '.join(formula[:4])}: a formula field changes with"
                " what it reads and no event says so - only a whole read"
                " compares it"]

    def count(self, stream):
        got = self._json("GET", f"{self._base()}/query",
                         params={"q": f"SELECT COUNT() FROM {stream.name}"})
        return int(got.get("totalSize") or 0)

    # ---- reading -------------------------------------------------------------------

    def _soql(self, stream, where=""):
        return (f"SELECT {', '.join(n for n, _ in stream.columns)} FROM"
                f" {stream.name}" + (f" WHERE {where}" if where else ""))

    def _bulk(self, soql):
        """The records of a Bulk API 2.0 query job, a page of its CSV
        results at a time."""
        from .. import decide
        r = self._rest("POST", f"{self._base()}/jobs/query",
                       json={"operation": "query", "query": soql,
                             "contentType": "CSV", "columnDelimiter": "COMMA",
                             "lineEnding": "LF"})
        if r.status_code >= 400:
            raise decide.Blocked(f"the bulk query job was refused"
                                 f" ({r.status_code}: {r.text[:120]})")
        job = r.json()["id"]
        while True:
            state = self._json("GET", f"{self._base()}/jobs/query/{job}")
            if state.get("state") == "JobComplete":
                break
            if state.get("state") in ("Failed", "Aborted"):
                raise decide.Blocked(f"the bulk query job ended"
                                     f" {state.get('state')}:"
                                     f" {state.get('errorMessage', '')[:120]}")
            time.sleep(2)
        locator = None
        while True:
            params = {"maxRecords": 50000}
            if locator:
                params["locator"] = locator
            r = self._rest("GET", f"{self._base()}/jobs/query/{job}/results",
                           params=params)
            if r.status_code >= 400:
                raise RuntimeError(f"Salesforce answered {r.status_code}:"
                                   f" {r.text[:200]}")
            for rec in csv.DictReader(io.StringIO(r.text)):
                yield {k: (v if v != "" else None) for k, v in rec.items()}
            locator = r.headers.get("Sforce-Locator")
            if not locator or locator == "null":
                return

    def _query(self, soql):
        """The records of a REST query, page by page."""
        got = self._json("GET", f"{self._base()}/query", params={"q": soql})
        while True:
            for rec in got.get("records") or []:
                rec.pop("attributes", None)
                yield rec
            nxt = got.get("nextRecordsUrl")
            if not nxt or got.get("done"):
                return
            got = self._json("GET", nxt)

    def _records(self, stream):
        """An object's records: by a bulk query job, or by the REST
        query's pages where the job cannot run (`decide.attempt`) - only
        its start switches, since records handed on cannot be read
        twice."""
        from .. import decide
        soql = self._soql(stream)

        def run(choice):
            if choice.rung.name == "bulk":
                it = self._bulk(soql)
                first = next(it, None)
                return first, it
            it = self._query(soql)
            return next(it, None), it
        rungs = (decide.Rung("bulk", "a bulk query job"),
                 decide.Rung("rest", "the REST query's pages"))
        first, rest = self.engine._attempt(f"reading {stream.name}",
                                           rungs, run)[1]
        if first is not None:
            yield first
            yield from rest

    def read(self, streams, state):
        for s in streams:
            for rec in self._records(s):
                yield ("record", s.name, rec)
        yield ("state", state)

    def rows_by_key(self, stream, keys):
        out = []
        ids = [str(k[0]) for k in keys]
        for i in range(0, len(ids), IDS_A_QUERY):
            part = ", ".join("'" + x.replace("'", "\\'") + "'"
                             for x in ids[i:i + IDS_A_QUERY])
            out += list(self._query(self._soql(stream, f"Id IN ({part})")))
        return out

    # ---- following -----------------------------------------------------------------

    def start(self, now):
        """Where a follow of each object starts: its oldest kept event,
        with every one committed before the copy began passed over."""
        return {s: {"replay": None, "since": now} for s in self._objects()}

    def _channel(self):
        import grpc
        where = str(self.options.get("pubsub_endpoint") or PUBSUB)
        if self.options.get("pubsub_tls", True) is False:
            return grpc.insecure_channel(where)
        return grpc.secure_channel(where, grpc.ssl_channel_credentials())

    def _metadata(self):
        auth = self._auth()
        return [("accesstoken", auth["access_token"]),
                ("instanceurl", auth["instance_url"]),
                ("tenantid", auth["org"])]

    def _schema(self, channel, schema_id):
        known = self.engine.__dict__.setdefault("_sf_schemas", {})
        if schema_id not in known:
            import fastavro
            m = pubsub_messages()
            call = channel.unary_unary(
                "/eventbus.v1.PubSub/GetSchema",
                request_serializer=m["SchemaRequest"].SerializeToString,
                response_deserializer=m["SchemaInfo"].FromString)
            got = call(m["SchemaRequest"](schema_id=schema_id),
                       metadata=self._metadata(), timeout=60)
            raw = json.loads(got.schema_json)
            known[schema_id] = (raw, fastavro.parse_schema(raw))
        return known[schema_id]

    def _subscribe(self, channel, topic, replay, n):
        """(events, the latest replay id, whether the topic had nothing
        more to give): one fetch of `n` events after `replay`, or from the
        oldest kept where it is None."""
        import threading

        import grpc
        m = pubsub_messages()
        done = threading.Event()
        ask = m["FetchRequest"](
            topic_name=topic, num_requested=n,
            replay_preset=m["CUSTOM"] if replay else m["EARLIEST"],
            **({"replay_id": bytes.fromhex(replay)} if replay else {}))

        def requests():
            yield ask
            done.wait(WAIT + 5)
        call = channel.stream_stream(
            "/eventbus.v1.PubSub/Subscribe",
            request_serializer=m["FetchRequest"].SerializeToString,
            response_deserializer=m["FetchResponse"].FromString)(
                requests(), metadata=self._metadata(), timeout=WAIT)
        events, latest, drained = [], None, False
        try:
            for resp in call:
                events += list(resp.events)
                if resp.latest_replay_id:
                    latest = bytes(resp.latest_replay_id).hex()
                if resp.pending_num_requested > 0 and not resp.events:
                    drained = True
                    break
                if len(events) >= n:
                    break
                if resp.pending_num_requested > 0:
                    drained = True
                    break
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                drained = True
            elif "replay" in str(e.details() or "").lower():
                raise SystemExit(
                    f"{topic}: the follow's position is past the 72 hours"
                    " Salesforce keeps change events - copy the object and"
                    " follow it again") from None
            else:
                raise RuntimeError(f"the change events could not be read:"
                                   f" {e.code().name}") from None
        finally:
            done.set()
            call.cancel()
        return events, latest, drained

    def changes(self, db, token, limit):
        """The changes of every object's events after the follow's
        replay ids (`_subscribe`), as `canon.change`s in commit order a
        topic."""
        from .. import canon
        began = datetime.datetime.now(datetime.timezone.utc)
        topics = dict(token.get("events") or {})
        streams = self.engine._catalog(self.side)
        out, drained_all = [], True
        channel = self._channel()
        try:
            for obj in sorted(streams):
                pos = dict(topics.get(obj) or {
                    "replay": None, "since": began.isoformat()})
                since = datetime.datetime.fromisoformat(pos["since"])
                if pos.get("replay") is None and began - since > RETENTION:
                    raise SystemExit(
                        f"{obj}: its copy began more than 72 hours ago, and"
                        " Salesforce keeps change events no longer - copy it"
                        " and follow it again")
                room = max(1, min(EVENTS_A_FETCH, limit - len(out)))
                events, latest, drained = self._subscribe(
                    channel, topic_of(obj), pos.get("replay"), room)
                drained_all = drained_all and drained
                for ev in events:
                    out += self._event_changes(channel, streams[obj], ev,
                                               pos, since, canon)
                if latest:
                    pos["replay"] = latest
                topics[obj] = pos
        finally:
            channel.close()
        new = {**token, "events": topics}
        if drained_all:
            new["read_at"] = began.isoformat()
        return out, new

    def _event_changes(self, channel, stream, ev, pos, since, canon):
        import fastavro
        raw, parsed = self._schema(channel, ev.event.schema_id)
        rec = fastavro.schemaless_reader(io.BytesIO(bytes(ev.event.payload)),
                                         parsed)
        head = rec.get("ChangeEventHeader") or {}
        committed = head.get("commitTimestamp")
        if committed is not None:
            when = datetime.datetime.fromtimestamp(committed / 1000,
                                                   datetime.timezone.utc)
            if when < since - SKEW:
                # in the copy already: the copy began after it
                return []
        kind = str(head.get("changeType") or "")
        ids = list(head.get("recordIds") or [])
        name = stream.name
        words = dict(stream.columns)
        if kind == "DELETE":
            return [canon.change("delete", name, {"Id": i}) for i in ids]
        if kind == "GAP_OVERFLOW":
            self.engine._say(f"{name}: more changes in one transaction than"
                             " its events carry; the object is read again"
                             " whole")
            return [canon.change("update", name, {"Id": r["Id"]},
                                 self._values(stream, r))
                    for r in self._records(stream)]
        fields = raw.get("fields") or []
        diff = bitmap_fields(head.get("diffFields"), fields)
        if kind.startswith("GAP_") or kind == "UNDELETE" or diff:
            return self._as_now(stream, ids)
        if kind == "CREATE":
            names = [(f["name"], None) for f in fields
                     if f["name"] != "ChangeEventHeader"]
            names += [(f["name"], p["name"]) for f in fields
                      for p in _record_fields(f["type"])]
            nulled = []
        else:
            names = bitmap_fields(head.get("changedFields"), fields)
            nulled = bitmap_fields(head.get("nulledFields"), fields)
        values = {}
        for field, part in names:
            col = column_of(field, part)
            if col is None or col not in words:
                continue
            got = rec.get(field)
            if part is not None:
                got = (got or {}).get(part) if isinstance(got, dict) \
                    else None
            if isinstance(got, dict) and part is None:
                continue
            values[col] = event_value(words[col], got)
        for field, part in nulled:
            col = column_of(field, part)
            if col is not None and col in words:
                values[col] = None
        values.pop("Id", None)
        return [canon.change("insert" if kind == "CREATE" else "update",
                             name, {"Id": i}, dict(values)) for i in ids]

    def _values(self, stream, rec):
        words = dict(stream.columns)
        return {c: value_of(words[c], rec.get(c)) for c in words
                if c != "Id"}

    def _as_now(self, stream, ids):
        """Each record as it is now: an update carrying it, or a delete
        where it is gone."""
        from .. import canon
        found = {r["Id"]: r for r in self.rows_by_key(stream,
                                                      [(i,) for i in ids])}
        out = []
        for i in ids:
            r = found.get(i)
            out.append(canon.change("delete", stream.name, {"Id": i})
                       if r is None else
                       canon.change("update", stream.name, {"Id": i},
                                    self._values(stream, r)))
        return out


# ---- what migkit asks of Salesforce's APIs (backlog 0f) ---------------------

def _register():
    from .. import tooluse
    unused = "unused"
    surface = {
        "rest": frozenset(("/query", "/queryAll", "/sobjects", "/describe",
                           "/jobs/query", "/results", "/limits",
                           "/composite", "/search", "/tooling",
                           "/services/oauth2/token")),
        "pubsub": frozenset(("Subscribe", "ManagedSubscribe", "GetSchema",
                             "GetTopic", "Publish", "PublishStream")),
        "fetch": frozenset(("topic_name", "replay_preset", "replay_id",
                            "num_requested", "auth_refresh")),
    }
    tooluse.register(
        "salesforce-api", licence="LicenseRef-Salesforce-API",
        terms=tooluse.Terms(open=False, allows="reading the org a hop"
                                               " names, signed in as that"
                                               " hop's own user, under the"
                                               " org's own agreement"),
        release=API, declared=surface,
        reads=(tuple(f"engines/saas_salesforce.py::{fn}" for fn in (
            "_auth", "catalog", "count", "_bulk", "_query", "_subscribe",
            "_schema")),
               {**{("rest", p): p for p in surface["rest"]},
                ("rest", "/sobjects"): "/sobjects/",
                **{("pubsub", r): f"/eventbus.v1.PubSub/{r}"
                   for r in surface["pubsub"]},
                **{("fetch", f): f for f in surface["fetch"]}}),
        options={
            ("rest", "/queryAll"): (unused, "a deleted record is not the"
                                            " application's data: the"
                                            " follow carries its delete"),
            ("rest", "/limits"): (unused, "the API's own answer says when a"
                                          " limit is reached"),
            ("rest", "/composite"): (unused, "each read is one query, of"
                                             " one object"),
            ("rest", "/search"): (unused, "records are read by their"
                                          " object, never searched"),
            ("rest", "/tooling"): (unused, "the org's configuration is not"
                                           " its data"),
            ("pubsub", "ManagedSubscribe"): (unused, "the position is kept"
                                                     " with the follow, as"
                                                     " every other engine's"
                                                     " is; a managed"
                                                     " subscription keeps it"
                                                     " on the org, where"
                                                     " another reader of it"
                                                     " would move it"),
            ("pubsub", "GetTopic"): (unused, "a topic is known by its"
                                             " object's name"),
            ("pubsub", "Publish"): (unused, "a source is read, never"
                                            " written"),
            ("pubsub", "PublishStream"): (unused, "see Publish"),
            ("fetch", "auth_refresh"): (unused, "a fetch is short, and a"
                                                " lapsed token signs in again"
                                                " before the next"),
        })


_register()
