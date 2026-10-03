"""SAP application data through ODP over OData (backlog ER6): the
extractors and CDS views an SAP system (ECC, S/4HANA, BW) publishes for
extraction, read through the OData service generated for them
("ODP-based extraction via OData").

That is the one way a program that is not SAP's may read them: ODP over
RFC is closed to it (SAP Note 3255746), `RFC_READ_TABLE` is not for
customers (Note 382318), and a direct read of the system's database under
a runtime licence is not allowed (Note 581312). So a hop's database is a
Gateway service, a table an entity set of it (`EntityOf...`,
`FactsOf...`, `AttrOf...`, `TextOf...`), its columns and key the entity
type's as the service's metadata gives them - read by `pyodata`, with the
SAP annotations it knows (`sap:display-format`, `sap:filterable`,
`sap:countable`) and the ones it does not read here from the same
document (`sap:semantics="aggregate"`, `sap:aggregation-role`). An SAP
system is only ever read: it is a source of a pair (engine: hetero,
source_engine: sapodp), or both sides of a comparison.

**Read.** An entity set is read page by page (`Prefer:
odata.maxpagesize`, following `__next`), in one pass: the service keeps no
order a read could resume by. Values are the entity type's: pyodata's
reading of a DateTime (an ABAP date where the service marks it as one),
of a DateTimeOffset, a number, a boolean; a decimal read as its digits,
bytes from their base64 and a time from its duration, which pyodata
leaves as text.

**Follow.** The copy a follow starts from is the extractor's delta
initialisation: its first read asks for changes to be tracked (`Prefer:
odata.track-changes`), and the delta token the last page carries - or,
where the page omits it, the one `DeltaLinksOf<entity set>` lists for that
initialisation - is the position, kept per entity set. ODP over OData
cannot start a delta without reading the data once (`$top` passes the
delta queue by, by SAP's own documentation), so no other read starts it:
a check, a comparison, a count read without asking for changes. From
there each delta is read by its token: `ODQ_CHANGEMODE` D a delete by
key, a before image (U with `ODQ_ENTITYCNTR` -1) passed over for the
after image that follows it, anything else the row as it now is. Reading
a token again repeats its delta until the next one is read, so a stopped
follow reads its last delta again and applies it again, which the
appliers are idempotent for. The fence is the queue's own: a delta token
is `D<the UTC time its request was made>_<counter>`, so a follow has read
everything committed before a moment once it holds a token of a request
made after it (`position_reached`); a token of another shape cannot say,
and says so. The subscription is the SAP user's: two hops reading one
extractor as one user would take each other's deltas, which `assess`
says; it stays on the system after the follow ends, to be ended in
ODQMON or by `TerminateDeltasFor<entity set>` - migkit changes nothing
there itself.

**Proved.** A comparison reads the whole entity set again, without
tracking changes, through migkit's own renderer - there is no digest on
the SAP side. Where the service counts an entity set (`$count`), its
count is taken before and after the read and held to the rows read: equal
before and after and different from what was read is a read that missed
or doubled rows; where it changed while it was read, that is said. An
aggregate entity type's measures are summed by the service the same way
and held to the sums of what was read. What cannot be proved is said:
an extractor that delivers additive deltas is not carried by key (its
deltas are amounts, not rows), and the authorisations of SAP users are
the system's, not carried.

Written 2026-10-01 against the OData v2 shapes SAP documents, with a
local service standing in for Gateway
(`tests/test_sap_application_data_through_odp.py`); no SAP system - the
ABAP trial image is amd64 only and 16-32 GB, and whether it serves ODP
over OData is unproved - was reachable here.
"""
import base64
import datetime
import email.utils
import json
import re
from decimal import Decimal

from .base import Engine

#: where Gateway publishes its services
SERVICE_ROOT = "/sap/opu/odata/sap"
#: the fields a delta page adds to each row
CHANGE_MODE, ENTITY_COUNTER = "ODQ_CHANGEMODE", "ODQ_ENTITYCNTR"
#: rows a page of the service holds: what one round trip carries, within
#: Gateway's own default response limits
PAGE_ROWS = 5000
#: keys asked for in one `$filter`: an address past 8 KB is refused by
#: Gateway's web dispatcher
KEYS_A_FILTER = 40
#: the queue's own request numbers: `D` and the UTC time the request was
#: made, then a counter
TOKEN = re.compile(r"^D?(\d{14})_(\d+)$")
_TIME = re.compile(r"^-?PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$")
_DELTA = re.compile(r"!deltatoken='([^']*)'")

#: where the delta initialisation each copy started is kept, beside the
#: hop's reports
INITIAL = "odp-initial-deltas.json"


def token_of(link):
    """The delta token of a `__delta` link, or None."""
    if not link:
        return None
    m = _DELTA.search(str(link))
    return m.group(1) if m else None


def token_order(token):
    """(the request's UTC time, its counter) of a delta token, or None
    where it is not of the queue's shape."""
    m = TOKEN.match(str(token or "").strip())
    return (m.group(1), int(m.group(2))) if m else None


def odata_time(text):
    """An `Edm.Time` (`PT12H30M05.5S`) as a time of day."""
    if text is None:
        return None
    m = _TIME.match(str(text).strip())
    if not m:
        raise ValueError(f"not an Edm.Time: {text!r}")
    h, mi = int(m.group(1) or 0), int(m.group(2) or 0)
    sec = Decimal(m.group(3) or 0)
    whole = int(sec)
    micro = int((sec - whole) * 1_000_000)
    return datetime.time(h, mi, whole, micro)


class Refused(Exception):
    """The service refused a request: its status and what it said."""

    def __init__(self, status, text):
        self.status = status
        super().__init__(f"the service answered {status}: {text[:200]}")


class SapOdpEngine(Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "sapodp"
    SQL_DIALECT = None
    FOLDED_BECAUSE = ("is read through its OData services, which keep no"
                      " digest of their own")
    OVER_NETWORK = True
    #: the service keeps no order a read resumes by
    RESUMES_BY_KEY = False
    #: reading a delta moves the subscription: a delta check would take
    #: the follow's changes
    CHANGE_POINT_READS_ONLY = False

    # ---- reaching it -------------------------------------------------------

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _session(self, side):
        """One HTTP session a side: the SAP user's basic sign-in, the
        system's certificate checked (`tls_ca_file`, or nothing checked
        where `tls_insecure` says so), JSON asked for."""
        known = self.__dict__.setdefault("_sessions", {})
        if side not in known:
            import requests
            ep = self._ep(side)
            o = ep.options or {}
            s = requests.Session()
            s.auth = (str(ep.user or ""), str(ep.password or ""))
            s.verify = (False if o.get("tls_insecure")
                        else o.get("tls_ca_file") or True)
            s.headers.update({"Accept": "application/json"})
            known[side] = s
        return known[side]

    def _root(self, side, db):
        ep = self._ep(side)
        o = ep.options or {}
        tls = o.get("tls", True) is not False
        port = ep.port or (443 if tls else 80)
        path = str(o.get("service_path") or SERVICE_ROOT).rstrip("/")
        return (f"{'https' if tls else 'http'}://{ep.host}:{port}{path}/"
                f"{self._d(side, db)}/")

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    def _params(self, side, extra=None):
        o = self._ep(side).options or {}
        out = {"$format": "json"}
        if o.get("client"):
            out["sap-client"] = str(o["client"])
        if o.get("language"):
            out["sap-language"] = str(o["language"])
        out.update(extra or {})
        return out

    def _get(self, side, db, url, params=None, prefer=None):
        """(the answer's JSON, its headers) for `url` - relative to the
        service root, or a `__next` link whole."""
        from urllib.parse import parse_qs, urlsplit
        full = url if url.startswith(("http://", "https://")) \
            else self._root(side, db) + url
        headers = {"Prefer": prefer} if prefer else {}
        # a link the service gave keeps its own query; what it leaves out
        # of the sign-in's client and language is added, never repeated
        there = parse_qs(urlsplit(full).query)
        ask = {k: v for k, v in self._params(side, params).items()
               if k not in there}
        got = self._session(side).get(full, params=ask, headers=headers,
                                      timeout=300)
        if got.status_code >= 400:
            raise Refused(got.status_code, got.text)
        return (got.json() if got.content else {}), got.headers

    def server_time(self, side, db):
        """The system's clock, as the HTTP Date of its answer, in UTC."""
        got = self._session(side).get(self._root(side, db),
                                      params=self._params(side),
                                      timeout=60)
        if got.status_code >= 400:
            raise Refused(got.status_code, got.text)
        when = got.headers.get("Date")
        if not when:
            return datetime.datetime.now(datetime.timezone.utc)
        return email.utils.parsedate_to_datetime(when).astimezone(
            datetime.timezone.utc)

    # ---- the metadata ------------------------------------------------------------

    def _service(self, side, db):
        """(the pyodata client, the metadata's XML root) of a service,
        read once."""
        known = self.__dict__.setdefault("_services", {})
        at = (side, db)
        if at not in known:
            import pyodata
            from lxml import etree
            from pyodata.v2.model import Config, PolicyIgnore
            got = self._session(side).get(self._root(side, db) + "$metadata",
                                          params={k: v for k, v in
                                                  self._params(side).items()
                                                  if k != "$format"},
                                          timeout=120)
            if got.status_code >= 400:
                raise SystemExit(
                    f"the {'source' if side == 'src' else 'target'} service"
                    f" {self._d(side, db)} refused its metadata"
                    f" ({got.status_code}): the SAP user needs the service"
                    " (S_SERVICE) and ODP extraction of its data sources")
            xml = got.content
            client = pyodata.Client(
                self._root(side, db), self._session(side), metadata=xml,
                config=Config(default_error_policy=PolicyIgnore()))
            known[at] = (client, etree.fromstring(xml))
        return known[at]

    def _sets(self, side, db):
        client, _ = self._service(side, db)
        return {es.name: es for es in client.schema.entity_sets}

    def _set(self, side, db, table):
        got = self._sets(side, db).get(str(table))
        if got is None:
            raise SystemExit(f"{table} is not an entity set of the service"
                             f" {self._d(side, db)}")
        return got

    def _props(self, side, db, table):
        """The entity type's properties, the delta fields left out."""
        return [p for p in self._set(side, db, table).entity_type.proprties()
                if p.name not in (CHANGE_MODE, ENTITY_COUNTER)]

    def _annotations(self, side, db, table):
        """(whether the entity type is an aggregate, its measures): the SAP
        annotations pyodata does not read, from the same metadata."""
        _, root = self._service(side, db)
        typ = self._set(side, db, table).entity_type.name
        sap = "http://www.sap.com/Protocols/SAPData"
        for node in root.iter():
            if str(node.tag).endswith("}EntityType") and \
                    node.get("Name") == typ:
                aggregate = node.get(f"{{{sap}}}semantics") == "aggregate"
                measures = [p.get("Name") for p in node
                            if str(p.tag).endswith("}Property")
                            and p.get(f"{{{sap}}}aggregation-role")
                            == "measure"]
                return aggregate, measures
        return False, []

    def databases(self):
        if not self.hop.databases:
            raise SystemExit("an SAP side names its OData services as the"
                             " hop's databases: sapodp://user@host/SERVICE_SRV")
        return list(self.hop.databases)

    def neutral_tables(self, side, db):
        return [n for n in sorted(self._sets(side, db))
                if not n.startswith("DeltaLinksOf")
                and not self.hop.excluded(db, n)]

    @staticmethod
    def declared_of(prop):
        """A property's declared type, with the numbers a counterpart
        needs: `Edm.String(10)`, `Edm.Decimal(15,2)`, and `Edm.Date` for a
        DateTime its service marks as a date."""
        name = prop.typ.name
        if name == "Edm.DateTime" and prop.date:
            return "Edm.Date"
        if name == "Edm.String" and prop.max_length and \
                int(prop.max_length) > 0:
            return f"Edm.String({int(prop.max_length)})"
        if name == "Edm.Decimal" and prop.precision:
            return f"Edm.Decimal({int(prop.precision)},{int(prop.scale or 0)})"
        return name

    def neutral_columns(self, side, db, table):
        return [(p.name, self.declared_of(p))
                for p in self._props(side, db, table)]

    def neutral_key(self, side, db, table):
        return [p.name for p in
                self._set(side, db, table).entity_type.key_proprties
                if p.name not in (CHANGE_MODE, ENTITY_COUNTER)]

    def neutral_column_rules(self, side, db, table):
        return {p.name: {"null": bool(p.nullable), "default": None,
                         "identity": False}
                for p in self._props(side, db, table)}

    def count(self, side, db, table):
        """The service's own count of an entity set (`$count`), or None
        where it does not count it."""
        es = self._set(side, db, table)
        if not es.countable:
            return None
        client, _ = self._service(side, db)
        request = getattr(client.entity_sets, str(table)).get_entities()
        for k, v in self._params(side).items():
            if k != "$format":
                request = request.custom(k, v)
        return int(request.count().execute())

    def table_facts(self, side, db):
        out = {}
        for t in self.neutral_tables(side, db):
            try:
                rows = self.count(side, db, t)
            except Exception:  # noqa: BLE001 - unknown, never guessed
                rows = None
            out[t] = {"rows": rows, "bytes": None,
                      "key": bool(self.neutral_key(side, db, t))}
        return out

    # ---- reading ---------------------------------------------------------------

    def _value(self, prop, raw):
        """A JSON value as the entity type means it: pyodata's reading
        where it has one that keeps the value, migkit's own for a decimal
        (its digits), bytes (base64) and a time (a duration), which
        pyodata hands back as the text they came in."""
        if raw is None:
            return None
        name = prop.typ.name
        if name == "Edm.Decimal":
            return Decimal(str(raw))
        if name == "Edm.Binary":
            return base64.b64decode(raw)
        if name == "Edm.Time":
            return odata_time(raw)
        if name == "Edm.Guid":
            return str(raw)
        got = prop.typ.traits.from_json(raw)
        if name == "Edm.DateTime":
            # no zone: the ABAP date or timestamp as its digits say
            got = got.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return got.date() if prop.date else got
        if name == "Edm.Int64" and isinstance(got, str):
            return int(got)
        return got

    @staticmethod
    def _results(body):
        d = (body or {}).get("d", body or {})
        if isinstance(d, list):
            return d, None, None
        return (d.get("results", []) if isinstance(d, dict) else [],
                d.get("__next") if isinstance(d, dict) else None,
                d.get("__delta") if isinstance(d, dict) else None)

    def _pages(self, side, db, table, prefer=None, url=None):
        """Every page of `table` (or of `url`, a delta or a `__next`): yields
        (rows as JSON, the next link, the delta link) a page."""
        at = url or str(table)
        preference = ",".join(p for p in (f"odata.maxpagesize={PAGE_ROWS}",
                                          prefer) if p)
        while at:
            body, _ = self._get(side, db, at, prefer=preference)
            rows, nxt, delta = self._results(body)
            yield rows, nxt, delta
            at = nxt

    def _initial(self, db):
        """{entity set: {"asked", "token"}}: the delta initialisation each
        copy asked for, and the token it ended with."""
        path = self.hop.report_dir(db) / INITIAL
        try:
            return json.loads(path.read_text())
        except (OSError, ValueError):
            return {}

    def _keep_initial(self, db, got):
        from .. import durable
        durable.write_json(self.hop.report_dir(db) / INITIAL, got, indent=1)

    def _rows_of(self, side, db, table, columns, track=False):
        """The entity set's rows, `columns` in order, a page at a time:
        with changes tracked where `track` - the delta initialisation - and
        the token it ends with kept (`_initial`)."""
        props = {p.name: p for p in self._props(side, db, table)}
        names = [n for n, _ in columns]
        missing = [n for n in names if n not in props]
        if missing:
            raise SystemExit(f"{table} has no {', '.join(missing[:3])}")
        picked = [props[n] for n in names]
        last = None
        for rows, _, delta in self._pages(
                side, db, table,
                prefer="odata.track-changes" if track else None):
            last = delta or last
            yield [[self._value(p, r.get(p.name)) for p in picked]
                   for r in rows]
        if track:
            token = token_of(last) or self._latest_link(side, db, table,
                                                        initial=True)
            if not token:
                raise SystemExit(
                    f"{table}: the extractor's delta initialisation ended"
                    " with no delta token, and its delta links list none -"
                    " the service does not track changes of it (a simple"
                    " transformation on the entity set turns tokens off,"
                    " SAP Note 1574568)")
            got = self._initial(db)
            entry = got.setdefault(str(table), {})
            if not entry.get("token"):
                # the first initialisation after the follow's point is its
                # start: one after it would pass over what came between
                entry["token"] = token
                entry["at"] = datetime.datetime.now(
                    datetime.timezone.utc).isoformat()
                self._keep_initial(db, got)

    def _starts_follow(self, db, table):
        """Whether this read is the delta initialisation a follow asked for
        and has not had yet."""
        entry = self._initial(db).get(str(table)) or {}
        return bool(entry.get("asked")) and not entry.get("token")

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        """Page by page. The first read of an entity set a follow asked a
        start for (`copy_point`) is its delta initialisation; every other
        read tracks nothing."""
        if where:
            raise SystemExit("a row filter is SQL, and an SAP service takes"
                             " none")
        track = side == "src" and self._starts_follow(db, table)
        for rows in self._rows_of(side, db, table, columns, track):
            for i in range(0, len(rows), max(1, int(size))):
                yield rows[i:i + size]

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """In key order, which the service does not keep: the entity set
        read whole and sorted, for the few callers that ask for a row or
        two - the copier reads it in one pass (`RESUMES_BY_KEY`)."""
        names = [n for n, _ in columns]
        rows = [r for b in self._rows_of(side, db, table, columns)
                for r in b]
        key = [k for k in self.neutral_key(side, db, table) if k in names]
        if not key:
            return rows, None
        at = [names.index(k) for k in key]
        rows.sort(key=lambda r: tuple(str(r[i]) for i in at))
        if after is not None:
            rows = [r for r in rows
                    if tuple(str(r[i]) for i in at) > tuple(map(str, after))]
        rows = rows[:limit] if limit else rows
        return rows, (tuple(rows[-1][i] for i in at) if rows else None)

    def _literal(self, prop, value):
        """A key value as OData writes it in a `$filter`: pyodata's
        literal where it has one; a decimal with its `M`; a date or a
        DateTime as the UTC instant pyodata's literal wants."""
        name = prop.typ.name
        if name == "Edm.Decimal":
            return f"{value}M"
        if name == "Edm.DateTime":
            if isinstance(value, datetime.date) and \
                    not isinstance(value, datetime.datetime):
                value = datetime.datetime(value.year, value.month, value.day)
            value = value.replace(tzinfo=datetime.timezone.utc)
        if name == "Edm.Binary":
            value = base64.b64encode(bytes(value)).decode()
        if name == "Edm.Time":
            return (f"time'PT{value.hour:02d}H{value.minute:02d}M"
                    f"{value.second:02d}S'")
        return prop.to_literal(value)

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        """By a `$filter` on the key, some keys a request, where the
        service filters every key field; a service that refuses the filter
        is read whole and the rows picked (`decide.attempt`)."""
        from .. import decide
        if not key or not keys:
            return {}
        keys = list(keys)
        props = {p.name: p for p in self._props(side, db, table)}
        names = [n for n, _ in columns]
        picked = [props[n] for n in names]
        want = {self._key_of(columns, key, [dict(zip(key, k)).get(n)
                                            for n in names]) for k in keys}

        def run(choice):
            out = []
            if choice.rung.name == "by key":
                for i in range(0, len(keys), KEYS_A_FILTER):
                    part = keys[i:i + KEYS_A_FILTER]
                    cond = " or ".join(
                        "(" + " and ".join(
                            f"{k} eq {self._literal(props[k], v)}"
                            for k, v in zip(key, one)) + ")"
                        for one in part)
                    try:
                        for rows, _, _ in self._pages(
                                side, db, f"{table}?$filter="
                                          + _quote(cond)):
                            out += [[self._value(p, r.get(p.name))
                                     for p in picked] for r in rows]
                    except Refused as e:
                        raise decide.Blocked(str(e)) from e
            else:
                for rows in self._rows_of(side, db, table, columns):
                    out += rows
            return out
        filterable = all(getattr(props.get(k), "filterable", False)
                         for k in key)
        rungs = (decide.Rung("by key", "the key asked of the service",
                             needs=(decide.Need(
                                 "every key field filterable",
                                 lambda f: f["filterable"],
                                 "the service does not filter every key"
                                 " field"),)),
                 decide.Rung("whole", "the entity set read whole and the"
                                      " rows picked"))
        rows = self._attempt(f"reading {table} by its key", rungs, run,
                             decide.Facts({"filterable": filterable}))[1]
        found = self._by_key_map(columns, key, rows)
        return {k: v for k, v in found.items() if k in want}

    def _sums(self, side, db, table, measures):
        """The service's own sums of an aggregate entity type's measures:
        its one row when nothing but the measures is selected."""
        body, _ = self._get(side, db, str(table),
                            params={"$select": ",".join(measures)})
        rows, _, _ = self._results(body)
        if len(rows) != 1:
            return None
        props = {p.name: p for p in self._props(side, db, table)}
        return {m: self._value(props[m], rows[0].get(m)) for m in measures}

    def neutral_digest(self, side, db, table, columns, where=None):
        """Folded here over a whole read that tracks nothing, with the
        service's own count - and an aggregate's sums - asked before and
        after it, for `read_proved` to hold the read to."""
        from .. import canon
        if where:
            raise SystemExit("a row filter is SQL, and an SAP service takes"
                             " none")
        aggregate, measures = self._annotations(side, db, table)
        names = [n for n, _ in columns]
        measures = [m for m in measures if m in names] if aggregate else []

        def own():
            try:
                n = self.count(side, db, table)
            except Exception:  # noqa: BLE001 - not counted by the service
                n = None
            try:
                sums = self._sums(side, db, table, measures) \
                    if measures else None
            except Exception:  # noqa: BLE001 - not summed by the service
                sums = None
            return n, sums
        before = own()
        classes = [c for _, c in columns]
        total, n = 0, 0
        read_sums = {m: Decimal(0) for m in measures}
        at = {m: names.index(m) for m in measures}
        for batch in self._rows_of(side, db, table, columns):
            k, total = canon.fold_rows(classes, batch, total)
            n += k
            for m, i in at.items():
                read_sums[m] += sum((Decimal(str(r[i])) for r in batch
                                     if r[i] is not None), Decimal(0))
        after = own()
        self.__dict__.setdefault("_read_totals", {})[(side, db, str(table))] \
            = {"rows": n, "sums": read_sums, "before": before,
               "after": after}
        return (n, str(total))

    def read_proved(self, side, db, table):
        """(status, words) holding the last whole read of `table` to what
        the service counts and sums of it, or None where it counts
        nothing. The same count before and after the read, and another
        number read, is a read that missed or doubled rows: `diff`."""
        got = (self.__dict__.get("_read_totals") or {}).get(
            (side, db, str(table)))
        if not got:
            return None
        (n0, s0), (n1, s1) = got["before"], got["after"]
        if n0 is None and n1 is None:
            return ("ok", "the service does not count this entity set, so"
                          " only the rows read were compared")
        if n0 != n1 or (s0 or {}) != (s1 or {}):
            return ("warn", "the extractor changed while it was read"
                            f" ({_counted(n0)} rows before, {_counted(n1)}"
                            " after), so its own count could not be held to"
                            " the rows read")
        bad = []
        if n0 != got["rows"]:
            bad.append(f"the service counts {n0:,} rows and {got['rows']:,}"
                       " were read")
        for m, v in (s0 or {}).items():
            if v is not None and Decimal(str(v)) != got["sums"].get(m):
                bad.append(f"it sums {m} to {v} and the rows read sum to"
                           f" {got['sums'].get(m)}")
        if bad:
            return ("diff", "; ".join(bad) + " - the read missed or doubled"
                                             " rows; read it again")
        return ("ok", f"the service's own count ({n0:,})"
                      + (f" and sums of {', '.join(sorted(s0))}" if s0
                         else "") + " agree with the rows read")

    def not_compared(self, db, table):
        """What a comparison of this entity set cannot prove, in words."""
        return ("the SAP system keeps no digest: its rows were read again"
                " through its service and compared here")

    # ---- the delta queue -----------------------------------------------------------

    def _latest_link(self, side, db, table, initial, since=None):
        """The delta token `DeltaLinksOf<entity set>` lists for this SAP
        user - of an initialisation where `initial` - created at or after
        `since`: the earliest such, or the latest where `since` is None."""
        name = f"DeltaLinksOf{table}"
        if name not in self._sets(side, db):
            return None
        body, _ = self._get(side, db, name)
        rows, _, _ = self._results(body)
        prop = next((p for p in self._props(side, db, name)
                     if p.name == "CreatedAt"), None)
        links = []
        for r in rows:
            if bool(r.get("IsInitialLoad")) != bool(initial):
                continue
            when = self._value(prop, r.get("CreatedAt")) if prop else None
            links.append((when, r.get("DeltaToken")))
        if since is not None:
            links = [x for x in links if x[0] is not None and x[0] >= since]
            links.sort(key=lambda x: x[0])
            return links[0][1] if links else None
        links.sort(key=lambda x: (x[0] is not None, x[0]))
        return links[-1][1] if links else None

    def copy_point(self, side, db):
        """Where a follow of this service starts: for each entity set, the
        delta initialisation the next copy of it makes, asked for here and
        kept beside the hop's reports - the copy's first read tracks
        changes, and the token it ends with is the position."""
        now = self.server_time(side, db).replace(tzinfo=None)
        tables = self.neutral_tables(side, db)
        sets = self._sets(side, db)
        untracked = [t for t in tables if f"DeltaLinksOf{t}" not in sets]
        if untracked:
            raise SystemExit(
                f"{', '.join(untracked[:6])} track no changes - the service"
                " lists no delta links for them - so a follow cannot carry"
                " theirs: leave them out of the hop (exclude), or copy them"
                " without a follow")
        got = self._initial(db)
        out = {}
        for t in tables:
            got[t] = {"asked": now.isoformat(), "token": None}
            out[t] = {"init": now.isoformat()}
        self._keep_initial(db, got)
        return out

    def change_point(self, side, db):
        return self.copy_point(side, db)

    def _start(self, side, db, table, pos):
        """The token a follow's start resolves to: the one the copy's
        initialisation ended with, or the first initialisation
        `DeltaLinksOf` lists after the follow's point."""
        entry = self._initial(db).get(str(table)) or {}
        if entry.get("token"):
            return entry["token"]
        since = datetime.datetime.fromisoformat(pos["init"])
        got = self._latest_link(side, db, table, initial=True, since=since)
        if got:
            return got
        raise SystemExit(
            f"{table}: no delta initialisation of it happened after this"
            " follow's point, so there is no position to follow it from -"
            " the copy that starts it has not run. Copy it and follow it"
            " together (migkit move --mode full+cdc)")

    def _change(self, table, key, props, row):
        from .. import canon
        mode = str(row.get(CHANGE_MODE) or "").strip().upper()
        counter = str(row.get(ENTITY_COUNTER) or "").strip()
        values = {p.name: self._value(p, row.get(p.name)) for p in props}
        k = {c: values[c] for c in key}
        if mode == "D":
            return canon.change("delete", table, k)
        if mode == "U" and counter == "-1":
            # a before image: the after image that follows it is the row
            return None
        return canon.change("insert" if mode == "C" else "update", table, k,
                            values)

    def neutral_changes(self, side, db, token=None, limit=1000):
        """The changes of each entity set's deltas after `token`, read by
        the queue's own tokens: a page at a time, the position carrying the
        page to read next until its delta is read whole, then that delta's
        own next token."""
        if token is None:
            return [], self.change_point(side, db)
        out, after = [], {}
        for table in sorted(token):
            pos = token[table]
            if isinstance(pos, dict) and "init" in pos:
                pos = {"at": self._start(side, db, table, pos), "next": None}
            props = self._props(side, db, table)
            key = self.neutral_key(side, db, table)
            at, url = pos["at"], pos.get("next")
            if len(out) >= limit:
                after[table] = pos
                continue
            try:
                pages = self._pages(side, db, table,
                                    url=url or f"{table}?!deltatoken="
                                               f"'{at}'")
                page = next(pages)
            except Refused:
                if not url:
                    raise
                # a page link that no longer answers: the delta is read
                # again from its token, which repeats it
                pages = self._pages(side, db, table,
                                    url=f"{table}?!deltatoken='{at}'")
                page = next(pages)
            while True:
                rows, nxt, delta = page
                for r in rows:
                    got = self._change(table, key, props, r)
                    if got is not None:
                        out.append(got)
                if nxt and len(out) >= limit:
                    after[table] = {"at": at, "next": nxt}
                    break
                if not nxt:
                    new = token_of(delta) or self._latest_link(
                        side, db, table, initial=False)
                    after[table] = {"at": new or at, "next": None}
                    break
                page = next(pages)
        return out, after

    def log_position(self, side, db):
        """Where the queue stands now, as a token of a request made now:
        every change committed before it is in a delta requested after."""
        now = self.server_time(side, db)
        at = f"D{now:%Y%m%d%H%M%S}_000000000"
        return {t: {"at": at, "next": None}
                for t in self.neutral_tables(side, db)}

    @staticmethod
    def position_reached(have, want):
        """Whether the follow holds, for every entity set, a token of a
        request made at or after the one `want` names; None where a token
        is not of the queue's shape and cannot say."""
        if not have or not want:
            return None
        for table, w in want.items():
            h = have.get(table)
            if not isinstance(h, dict) or "at" not in h:
                return False
            a, b = token_order(h["at"]), token_order(w.get("at"))
            if a is None or b is None:
                return None
            if h.get("next") or a[0] < b[0]:
                return False
        return True

    def stream_identity(self, side, db):
        ep = self._ep(side)
        return {"system": str(ep.host),
                "client": str((ep.options or {}).get("client") or ""),
                "service": str(self._d(side, db))}

    def position_lost(self, side, db, token):
        """Why the SAP user's subscription no longer holds a position: the
        service says it is no longer subscribed (`SubscribedTo<entity
        set>`) - ended in ODQMON, by a terminate, or by another
        initialisation under this user."""
        if not token:
            return None
        client, _ = self._service(side, db)
        names = {f.name for f in client.schema.function_imports}
        for table, pos in token.items():
            if isinstance(pos, dict) and "init" in pos:
                continue
            name = f"SubscribedTo{table}"
            if name not in names:
                continue
            body, _ = self._get(side, db, name)
            d = (body or {}).get("d", {})
            got = d.get(name, d) if isinstance(d, dict) else d
            if got is False or str(got).lower() == "false":
                return (f"{table}: this SAP user is no longer subscribed to"
                        " its deltas - the subscription was ended in ODQMON,"
                        " terminated, or replaced by another initialisation"
                        " under this user. Copy it and follow it again")
        return None

    def server_identity(self, side, db):
        ep = self._ep(side)
        o = ep.options or {}
        out = {"id": f"{ep.host}/{o.get('client') or ''}",
               "name": str(ep.host)}
        try:
            self._service(side, db)
        except SystemExit as e:
            out["unread"] = str(e)[:160]
        return out

    # ---- as a hop's engine: two SAP systems compared -------------------------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def reach_items(self, side):
        """What `assess` says of an SAP side: which services answer, which
        entity sets are counted and which track changes, and what is not
        carried."""
        items = []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "reach", "item": item,
                          "detail": str(detail)})
        for db in self.hop.databases or []:
            try:
                sets = self.neutral_tables(side, db)
            except (SystemExit, Exception) as e:  # noqa: BLE001 - said
                add("fail", f"{db}: the service", str(e).splitlines()[0]
                    [:160] if str(e) else type(e).__name__)
                continue
            deltas = [t for t in sets
                      if f"DeltaLinksOf{t}" in self._sets(side, db)]
            add("pass", f"{db}: the service",
                f"{len(sets)} entity sets, {len(deltas)} of them tracking"
                " changes")
        add("info", "the SAP user", "the delta subscriptions are this SAP"
                                    " user's: two hops reading one extractor"
                                    " as one user take each other's deltas;"
                                    " end a subscription in ODQMON after the"
                                    " follow")
        add("info", "users and authorisations", "SAP authorisations are the"
                                                " system's roles (PFCG), not"
                                                " carried")
        return items

    def _assess_extra(self):
        return self.reach_items("src")


def _counted(n):
    return "not counted" if n is None else f"{n:,}"


def _quote(text):
    from urllib.parse import quote
    return quote(text, safe="'()=,")


# ---- what migkit asks of pyodata (backlog 0f) --------------------------------

#: what the library offers a client of a service, as its 1.12 release has it
SURFACE = {
    "client": frozenset(("Client", "metadata", "config", "Config",
                         "PolicyIgnore", "PolicyWarning", "PolicyFatal",
                         "retain_null", "namespaces", "odata_version",
                         "response_hook")),
    "model": frozenset(("entity_sets", "entity_type", "proprties",
                        "key_proprties", "countable", "filterable", "date",
                        "max_length", "precision", "scale", "nullable",
                        "from_json", "to_literal", "function_imports",
                        "sortable", "upper_case", "value_list", "label",
                        "text_proprty", "nav_proprties", "pageable",
                        "topable", "requires_filter")),
    "request": frozenset(("get_entities", "count", "custom", "filter",
                          "select", "expand", "order_by", "skip", "top",
                          "next_url", "add_headers", "execute",
                          "async_execute", "create_batch",
                          "create_changeset", "get_entity",
                          "create_entity", "update_entity",
                          "delete_entity")),
}


def _account():
    unused = "unused"
    writes = "an SAP system is read, never written"
    pages = ("pages are followed through the service's own `__next` and"
             " `__delta` links, which the delta queue needs read as they"
             " come")
    return {
        ("client", "PolicyWarning"): (unused, "a quirk of SAP's metadata"
                                              " in a part migkit does not"
                                              " read is passed over, not"
                                              " logged"),
        ("client", "PolicyFatal"): (unused, "a quirk of SAP's metadata in a"
                                            " part migkit does not read"
                                            " would stop every read"),
        ("client", "retain_null"): (unused, "values are read from the"
                                            " JSON here, where a null is"
                                            " kept as None"),
        ("client", "namespaces"): (unused, "the metadata's own namespaces"
                                           " are the ones SAP uses"),
        ("client", "odata_version"): (unused, "ODP's services are OData"
                                              " version 2, the default"),
        ("client", "response_hook"): (unused, "each answer's status is"
                                              " read where it is asked"),
        ("model", "sortable"): (unused, "no read is ordered: the service"
                                        " keeps no order a read resumes"
                                        " by"),
        ("model", "upper_case"): (unused, "a value is compared as it is"
                                          " held, not as it is shown"),
        ("model", "value_list"): (unused, "value helps are for people"
                                          " entering values"),
        ("model", "label"): (unused, "columns keep their technical names"),
        ("model", "text_proprty"): (unused, "a text is its own column"),
        ("model", "nav_proprties"): (unused, "an extractor's rows are"
                                             " flat"),
        ("model", "pageable"): (unused, pages),
        ("model", "topable"): (unused, "a read with a top passes the delta"
                                       " queue by"),
        ("model", "requires_filter"): (unused, "an entity set that must be"
                                               " filtered is read with the"
                                               " key filter the pair asks"),
        ("request", "filter"): (unused, "the key filter is written here,"
                                        " with each value as its literal"),
        ("request", "select"): (unused, "a row is read whole; an"
                                        " aggregate's sums are asked by the"
                                        " service's own select"),
        ("request", "expand"): (unused, "an extractor's rows are flat"),
        ("request", "order_by"): (unused, "no read is ordered: the service"
                                          " keeps no order a read resumes"
                                          " by"),
        ("request", "skip"): (unused, "a read with a skip passes the delta"
                                      " queue by"),
        ("request", "top"): (unused, "see skip"),
        ("request", "next_url"): (unused, pages),
        ("request", "add_headers"): (unused, pages),
        ("request", "async_execute"): (unused, "one request at a time keeps"
                                               " the delta queue's order"),
        ("request", "create_batch"): (unused, writes),
        ("request", "create_changeset"): (unused, writes),
        ("request", "get_entity"): (unused, "see filter"),
        ("request", "create_entity"): (unused, writes),
        ("request", "update_entity"): (unused, writes),
        ("request", "delete_entity"): (unused, writes),
    }


def _register():
    from .. import tooluse
    tooluse.register(
        "pyodata", licence="Apache-2.0", release="1.12",
        options=_account(), declared=SURFACE,
        reads=(tuple(f"engines/sapodp.py::{fn}" for fn in (
            "_service", "_sets", "_set", "_props", "declared_of",
            "neutral_key", "neutral_column_rules", "count", "_value",
            "_literal", "position_lost", "neutral_rows_by_key")),
               {(c, o): o for c, opts in SURFACE.items() for o in opts}))


_register()
