"""OpenSearch (and Elasticsearch) as one side of a pair (backlog 34).

A database is a prefix on index names (`<db>.` by default, the endpoint's
`index_prefix` otherwise, with `{db}` in it), and a table is an index. A
document's id is the canonical text of the row's key, so a row written
again replaces itself. What each column was is kept in the index
mapping's own `_meta`, and a document's `_source` keeps each value as it
was sent: a decimal is sent as its text and comes back with its scale,
where a JSON number would have lost it. An index migkit did not create is
described by its mapping, and keyed by `_id`.

Documents come back in no order, so an index is read in one pass through
a scroll, and looked up by key through `_mget`. What was written is made
visible to reads before migkit reads the target (a refresh), never on the
source.
"""
import base64
import json
import posixpath
import ssl
import urllib.error
import urllib.request

from .. import pieces
from .base import Engine, NeutralCopier, Result

#: mapping for each class, and the kinds of field a mapping names
MAPPING = {
    "integer": {"type": "long"},
    "float": {"type": "double"},
    "boolean": {"type": "boolean"},
    "text": {"type": "text",
             "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
    "bytes": {"type": "binary"},
    "date": {"type": "date", "format": "yyyy-MM-dd"},
    "timestamp": {"type": "date_nanos",
                  "format": "yyyy-MM-dd HH:mm:ss.SSSSSS"},
    "time": {"type": "keyword"},
}


#: the class a field of an index migkit did not make holds, by its type;
#: a `date` there is a moment, a timestamp
FIELDS = {"long": "integer", "integer": "integer", "short": "integer",
          "byte": "integer", "unsigned_long": "integer",
          "double": "float", "float": "float", "half_float": "float",
          "scaled_float": "float", "keyword": "text", "text": "text",
          "match_only_text": "text", "wildcard": "text",
          "constant_keyword": "text", "boolean": "boolean",
          "date": "timestamp", "date_nanos": "timestamp",
          "binary": "bytes"}


# ---- what two indexes' mappings, settings and values differ in -------------
#
# Pure functions over what the cluster answered (`GET /<index>`), so every
# kind of drift they name can be shown without a server. Each returns
# [(kind, words)], the kinds being the deep check's categories.

#: the integer types by the number of bits each holds
_WIDTH = {"byte": 8, "short": 16, "integer": 32, "long": 64}
#: the floating types by the bits of precision each keeps
_FLOATS = {"half_float": 16, "float": 32, "double": 64}
#: the moment types by the fractional digits of a second each keeps
_MOMENT = {"date": 3, "date_nanos": 9}
#: what makes a field's text compare another way - how it is split,
#: folded and ordered, a search engine's collation
_COLLATES = ("analyzer", "search_analyzer", "search_quote_analyzer",
             "normalizer", "language", "country", "variant", "strength",
             "decomposition", "alternate", "case_level", "case_first",
             "numeric", "variable_top", "rules")
#: what makes a field's value the cluster's doing, not the document's
_COMPUTED = ("copy_to", "script", "on_script_error")
#: a field's mapping keys that hold other fields
_CHILDREN = ("properties", "fields")


def _type_of(field):
    return field.get("type") or "object"


def _type_drift(here, a, b):
    ta, tb = _type_of(a), _type_of(b)
    if ta in _WIDTH and tb in _WIDTH and _WIDTH[tb] < _WIDTH[ta]:
        return [("narrowing", f"{here}: {ta} -> {tb} - a value past what"
                              f" {tb} holds is refused")]
    if ta in _FLOATS and tb in _FLOATS and _FLOATS[tb] < _FLOATS[ta]:
        return [("precision", f"{here}: {ta} -> {tb} - digits past {tb}'s"
                              " precision are lost in the index")]
    if ta in _MOMENT and tb in _MOMENT and _MOMENT[tb] < _MOMENT[ta]:
        return [("precision", f"{here}: {ta} -> {tb} - the time past the"
                              " millisecond is lost in the index")]
    return [("columns", f"{here}: {ta} -> {tb}")]


def _attribute_drift(here, a, b):
    out = []
    for key in sorted((set(a) | set(b)) - set(_CHILDREN) - {"type"}):
        x, y = a.get(key), b.get(key)
        if x == y:
            continue
        if key == "scaling_factor" and x and y and float(y) < float(x):
            out.append(("precision", f"{here}: scaling_factor {x} -> {y} -"
                                     " decimals past the factor are rounded"
                                     " in the index"))
        elif key == "ignore_above" and (y if y is not None else 2 ** 31) < (
                x if x is not None else 2 ** 31):
            out.append(("narrowing", f"{here}: ignore_above {x} -> {y} - a"
                                     " string longer than that is kept but"
                                     " not indexed"))
        elif key in _COLLATES:
            out.append(("collation", f"{here}: {key} {x!r} -> {y!r}"))
        elif key in _COMPUTED:
            out.append(("generated", f"{here}: {key} {x!r} -> {y!r}"))
        else:
            out.append(("columns", f"{here}: {key} {x!r} -> {y!r}"))
    return out


def mapping_drift(src, dst, path=""):
    """[(kind, words)] of two mappings' `properties`, field by field and
    through objects and multi-fields: `columns` (a field missing, extra, of
    another type or with another attribute), `narrowing` (a narrower number
    or a shorter `ignore_above`), `precision` (fewer digits kept),
    `collation` (how text is split and folded), `generated` (a value the
    cluster computes)."""
    out = []
    for name in sorted(set(src) | set(dst)):
        here = f"{path}{name}"
        a, b = src.get(name), dst.get(name)
        if a is None:
            out.append(("columns", f"{here}: only on the target"
                                   f" ({_type_of(b)})"))
            continue
        if b is None:
            out.append(("columns", f"{here}: missing on the target"
                                   f" ({_type_of(a)})"))
            continue
        out += (_type_drift(here, a, b) if _type_of(a) != _type_of(b)
                else _attribute_drift(here, a, b))
        for child in _CHILDREN:
            out += mapping_drift(a.get(child) or {}, b.get(child) or {},
                                 here + ".")
    return out


def _flat(node, prefix=""):
    """A nested settings answer as {dotted key: text}."""
    out = {}
    for k, v in (node or {}).items():
        if isinstance(v, dict):
            out.update(_flat(v, f"{prefix}{k}."))
        else:
            out[f"{prefix}{k}"] = json.dumps(v) if isinstance(
                v, list) else str(v)
    return out


#: the index settings that are choices about how documents are stored and
#: placed, not about the cluster's weather (replicas, refresh) or its own
#: identity
_OPTIONS = ("number_of_shards", "number_of_routing_shards",
            "routing_partition_size", "codec", "sort.", "soft_deletes.",
            "mapping.", "max_", "blocks.", "hidden", "queries.",
            "similarity.", "store.", "translog.durability", "knn")
#: what a mapping says about the index as a whole
_MAPPING_OPTIONS = ("dynamic", "dynamic_templates", "date_detection",
                    "dynamic_date_formats", "numeric_detection", "_source",
                    "_routing", "_size", "_field_names", "enabled")


def _settings_of(described):
    return _flat((described.get("settings") or {}).get("index"))


def options_drift(a, b):
    """[words] of two indexes' options: how many shards, how they are
    sorted and compressed, how strict the mapping is and whether the source
    of a document is kept - what cannot be changed on an index once it has
    documents, and changes what a write or a read does."""
    out = []
    sa, sb = _settings_of(a), _settings_of(b)
    for key in sorted(set(sa) | set(sb)):
        if not key.startswith(_OPTIONS):
            continue
        if sa.get(key) != sb.get(key):
            out.append(f"{key} {sa.get(key)!r} -> {sb.get(key)!r}")
    ma, mb = a.get("mappings") or {}, b.get("mappings") or {}
    for key in _MAPPING_OPTIONS:
        if ma.get(key) != mb.get(key):
            out.append(f"mapping {key} {ma.get(key)!r} -> {mb.get(key)!r}")
    return out


def analysis_drift(a, b):
    """[words] of two indexes' analysis - the analyzers, tokenizers, filters
    and normalizers that decide how text is split, folded and compared."""
    sa = {k: v for k, v in _settings_of(a).items()
          if k.startswith("analysis.")}
    sb = {k: v for k, v in _settings_of(b).items()
          if k.startswith("analysis.")}
    return [f"{k} {sa.get(k)!r} -> {sb.get(k)!r}"
            for k in sorted(set(sa) | set(sb)) if sa.get(k) != sb.get(k)]


def generated_drift(a, b):
    """[words] of what a write to either index makes of a document beyond
    what it was sent: a runtime field's script, and the ingest pipelines
    every document goes through before it is stored."""
    out = []
    ra = (a.get("mappings") or {}).get("runtime") or {}
    rb = (b.get("mappings") or {}).get("runtime") or {}
    for name in sorted(set(ra) | set(rb)):
        if ra.get(name) != rb.get(name):
            out.append(f"runtime field {name}: {ra.get(name)!r} ->"
                       f" {rb.get(name)!r}")
    sa, sb = _settings_of(a), _settings_of(b)
    for key in ("default_pipeline", "final_pipeline"):
        if sa.get(key) != sb.get(key):
            out.append(f"{key} {sa.get(key)!r} -> {sb.get(key)!r} - every"
                       " document is rewritten by it on its way in")
    return out


#: the kinds a value is, for the drift between two sides' documents
TYPE_KINDS = ("boolean", "integer", "float", "string", "object", "array")
EMPTY_KINDS = ("null", "empty", "empty-array", "absent")
#: field paths one index may show before they are not all counted - a
#: mapping explosion, not a schema
MOST_PATHS = 10_000


class TooManyFields(Exception):
    pass


def _kind(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "empty" if value == "" else "string"
    if isinstance(value, dict):
        return "object"
    return "array"


def _walk(value, path, found):
    kind = _kind(value)
    if path:
        found.setdefault(path, set()).add(kind)
    if isinstance(value, dict):
        for k, v in value.items():
            _walk(v, f"{path}.{k}" if path else k, found)
    elif isinstance(value, list):
        if not value:
            found.setdefault(path, set()).add("empty-array")
        for v in value:
            if isinstance(v, list):
                found.setdefault(path, set()).add("array")
            else:
                _walk(v, path, found)


def count_kinds(hits, into):
    """Count, into `into` ({path: {kind: documents}} and `docs`), the kind
    of value each field holds in each document's `_source`: a document
    counts once for a kind at a path however many times an array repeats
    it, and `present` counts those that have the field at all - null
    included - so an absent field is told from a null one."""
    for hit in hits:
        into["docs"] = into.get("docs", 0) + 1
        found = {}
        _walk(hit.get("_source") or {}, "", found)
        for path, kinds in found.items():
            if path not in into["paths"] and len(into["paths"]) >= MOST_PATHS:
                raise TooManyFields(path)
            row = into["paths"].setdefault(path, {})
            row["present"] = row.get("present", 0) + 1
            for k in kinds:
                row[k] = row.get(k, 0) + 1


def kinds_drift(a, b):
    """([type drift], [null/empty drift]) between two sides' counts
    (`count_kinds`), as words by field: a field held as a number on one
    side and a string on the other, or null here and absent or empty
    there."""
    typed, empty = [], []
    for path in sorted(set(a["paths"]) | set(b["paths"])):
        x, y = a["paths"].get(path, {}), b["paths"].get(path, {})
        x = dict(x, absent=a["docs"] - x.get("present", 0))
        y = dict(y, absent=b["docs"] - y.get("present", 0))
        for group, into in ((TYPE_KINDS, typed), (EMPTY_KINDS, empty)):
            apart = [f"{k} {x.get(k, 0)} -> {y.get(k, 0)}" for k in group
                     if x.get(k, 0) != y.get(k, 0)]
            if apart:
                into.append(f"{path}: " + ", ".join(apart))
    return typed, empty


def _last_line(error, most=90):
    """What an error says at its end, cut to `most` characters: the line a
    finding gives for a read that could not be made."""
    return (str(error).strip().splitlines() or [""])[-1][:most]


_UNITS = {"b": 1, "kb": 1 << 10, "mb": 1 << 20, "gb": 1 << 30,
          "tb": 1 << 40, "pb": 1 << 50}


def kept_free(watermark, total):
    """Bytes a disk of `total` bytes keeps free under a disk watermark
    setting: a percentage of the disk used (`85%`, or `0.85`) leaves the
    rest free, a size (`50gb`) is the free space itself. None for a
    setting that is neither."""
    import re
    text = str(watermark).strip().lower()
    if text.endswith("%"):
        try:
            return round(total * (1 - float(text[:-1]) / 100))
        except ValueError:
            return None
    try:
        ratio = float(text)
    except ValueError:
        ratio = None
    if ratio is not None and 0 <= ratio <= 1:
        return round(total * (1 - ratio))
    got = re.fullmatch(r"([\d.]+)\s*([kmgtp]?b)", text)
    return int(float(got.group(1)) * _UNITS[got.group(2)]) if got else None


class OpenSearchEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "opensearch"
    ENGINE_FAMILY = "opensearch"

    def _brand_probes(self):
        """What `GET /` says of each side: OpenSearch names its
        distribution, Elasticsearch its build flavour and its tagline
        (`variants.SIGNATURES`). Everything else migkit does here is the
        same on both - a document written as JSON, a version given as
        `external`, basic authentication - so Elasticsearch is a side as
        OpenSearch is, and what one has of the other's field types is
        asked before a move (`lacks.SEARCH`)."""
        def one(side):
            try:
                got = self._call(side, "GET", "/") or {}
            except Exception:
                return {}
            version = got.get("version") or {}
            out = {"number": version.get("number"),
                   "distribution": version.get("distribution"),
                   "build_flavor": version.get("build_flavor"),
                   "tagline": got.get("tagline")}
            return {k: v for k, v in out.items() if v is not None}
        return (one("src"), one("dst"))

    def _server_versions(self):
        s, d = self._brands()
        return (s.version or None, d.version or None)
    FOLDED_BECAUSE = "has no way to hash documents in the service"
    OVER_NETWORK = True
    RESUMES_BY_KEY = False
    #: large values - a document is as large as a request the cluster takes
    #: (`http.max_content_length`, 100 MB by default): read in scroll pages
    #: of as many as `pieces.BATCH_BYTES` holds at the index's widest
    #: document (`_page`), written in bulk requests cut by bytes
    #: (`BULK_BYTES`, one alone where it is more), hashed here as it passes
    LARGE_VALUES = pieces.Way(
        "documents read in pages sized by the widest, written in bulk"
        " requests cut by bytes")

    def _page(self, side, db, table, most):
        """Documents a scroll page of `table` asks for: `most`, or as many
        as `pieces.BATCH_BYTES` holds at its widest document as sampled
        (`pieces.widest`) - a page is held whole once it arrives."""
        return pieces.rows_for(pieces.widest(self, side, db, table), most)

    # ---- the wire ----

    def _base(self, side):
        ep = self.hop.source if side == "src" else self.hop.target
        return str(ep.options.get("url") or
                   f"{'https' if ep.options.get('tls') else 'http'}://"
                   f"{ep.host}:{ep.port or 9200}").rstrip("/")

    def _call(self, side, method, path, body=None, ndjson=False,
              missing_ok=False):
        ep = self.hop.source if side == "src" else self.hop.target
        base = self._base(side)
        data = None
        if body is not None:
            data = (body if ndjson else json.dumps(body)).encode()
        req = urllib.request.Request(base + path, data=data, method=method)
        req.add_header("Content-Type", "application/x-ndjson" if ndjson
                       else "application/json")
        if ep.user:
            token = base64.b64encode(f"{ep.user}:{ep.password}".encode())
            req.add_header("Authorization", "Basic " + token.decode())
        ctx = None
        if base.startswith("https") and ep.options.get("verify") is False:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        try:
            with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
                raw = r.read()
        except urllib.error.HTTPError as e:
            if missing_ok and e.code == 404:
                return None
            raise RuntimeError(f"{method} {path.split('?')[0]}: {e.code}"
                               f" {e.read()[:300].decode(errors='replace')}")
        return json.loads(raw) if raw else {}

    #: documents and bytes one bulk request carries at most
    BULK_DOCS, BULK_BYTES = 1000, 32 * 2 ** 20
    #: times a document the cluster pushed back (429) is sent again, the
    #: wait doubling from one second up to a minute
    BULK_RETRIES = 8

    def _os(self, side):
        """The cluster's client, one a side, shared by the threads of a
        copy. OpenSearch's own: Elasticsearch's refuses any server that is
        not Elasticsearch."""
        clients = self.__dict__.setdefault("_clients", {})
        if side not in clients:
            from opensearchpy import OpenSearch
            ep = self.hop.source if side == "src" else self.hop.target
            checked = ep.options.get("verify") is not False
            clients[side] = OpenSearch(
                hosts=[self._base(side)],
                http_auth=(ep.user, ep.password) if ep.user else None,
                verify_certs=checked, ssl_assert_hostname=checked,
                ssl_show_warn=False, timeout=120,
                pool_maxsize=max(10, int(self.hop.workers or 1) * 2))
        return clients[side]

    def _bulk(self, side, actions):
        """(written, left because the target holds that version or a newer
        one) for a stream of bulk actions, sent in requests of
        `BULK_DOCS` documents or `BULK_BYTES`, one after another - a
        document the cluster pushes back for being busy is sent again
        after a wait (`BULK_RETRIES`), which parallel sending does not do.
        A delete of a document that is not there is what was wanted; any
        other refusal stops the copy, naming the first.

        Every operation the target numbered is counted as migkit's own
        where the hop names the target's other writes (`freeze.counted`)."""
        from opensearchpy.helpers import streaming_bulk

        from .. import freeze
        written, kept, refused = 0, 0, []
        numbered = {}
        try:
            for ok, item in streaming_bulk(
                    self._os(side), actions, chunk_size=self.BULK_DOCS,
                    max_chunk_bytes=self.BULK_BYTES, raise_on_error=False,
                    raise_on_exception=True, max_retries=self.BULK_RETRIES,
                    initial_backoff=1, max_backoff=60, yield_ok=True):
                op, got = next(iter(item.items()))
                status = int(got.get("status") or 0)
                if int(got.get("_seq_no", -1)) >= 0:
                    numbered[got.get("_index")] = numbered.get(
                        got.get("_index"), 0) + 1
                if ok:
                    written += 1
                elif status == 409:
                    kept += 1
                elif not (op == "delete" and status == 404):
                    refused.append(got)
        finally:
            if side == "dst":
                freeze.counted(self.hop, numbered)
        if refused:
            first = refused[0].get("error") or {}
            raise SystemExit(f"{refused[0].get('_index')}: the target refused"
                             f" {len(refused)} documents:"
                             f" {first.get('type')}:"
                             f" {str(first.get('reason'))[:120]}")
        return written, kept

    @staticmethod
    def _versioned(hit, index):
        """A document read from the source, as the bulk action that writes
        it to `index` with the source's own place in its shard's history
        as its version: its primary term above its sequence number, so a
        later change always carries a larger one.

        `external`: the target keeps a write only when its version is
        larger than the one it holds, so a change carried twice, or an
        older one carried after a newer, is left (a conflict, counted, not
        an error). Not `external_gte`, which also takes an equal version -
        the documentation's own warning is that it can lose data - and not
        `if_seq_no`/`if_primary_term`, which hold a write to the target's
        own sequence numbers, which the copy neither knows nor shares with
        the source."""
        action = {"_op_type": "index", "_index": index, "_id": hit["_id"],
                  "_source": hit.get("_source") or {}}
        if hit.get("_routing") is not None:
            action["routing"] = hit["_routing"]
        version = OpenSearchEngine._version_of(hit)
        if version is not None:
            action["version"] = version
            action["version_type"] = "external"
        return action

    def _prefix(self, side, db):
        ep = self.hop.source if side == "src" else self.hop.target
        name = self.hop.target_db(db) if side == "dst" else db
        return str(ep.options.get("index_prefix") or "{db}.").format(
            db=name).lower()

    def _index(self, side, db, table):
        return self._prefix(side, db) + str(table).lower()

    def databases(self):
        return list(self.hop.databases or ["default"])

    # ---- what an index is ----

    def neutral_tables(self, side, db):
        pre = self._prefix(side, db)
        got = self._call(side, "GET", "/_cat/indices?format=json&h=index")
        return sorted(i["index"][len(pre):] for i in got or []
                      if i["index"].startswith(pre)
                      and self.in_scope(db, i["index"][len(pre):]))

    def _mapping(self, side, db, table):
        index = self._index(side, db, table)
        got = self._call(side, "GET", f"/{index}/_mapping")
        return got[index]["mappings"]

    def _described(self, side, db, table):
        meta = (self._mapping(side, db, table).get("_meta") or {}).get(
            "migkit")
        if meta:
            return [tuple(c) for c in meta["columns"]], list(meta["key"])
        return None

    def neutral_columns(self, side, db, table):
        described = self._described(side, db, table)
        if described is not None:
            return described[0]
        # an index migkit did not make: its mapping, by the class each
        # kind of field holds, and its ids as its key. An object, a nested
        # document or anything else stays unmapped.
        props = self._mapping(side, db, table).get("properties") or {}
        return [("_id", "text")] + sorted(
            (n, FIELDS.get(p.get("type", "object"), p.get("type", "object")))
            for n, p in props.items())

    def neutral_key(self, side, db, table):
        described = self._described(side, db, table)
        return described[1] if described is not None else ["_id"]

    # ---- values ----

    @staticmethod
    def _to_source(declared, value):
        from decimal import Decimal

        from .. import canon
        if value is None:
            return None
        cls = canon.type_class("opensearch", declared)
        if cls == "decimal":
            # the text, scale and all: a JSON number keeps neither
            return format(value if isinstance(value, Decimal)
                          else Decimal(str(value)), "f")
        if cls == "integer":
            return int(value)
        if cls == "float":
            return float(value)
        if cls == "boolean":
            return bool(value)
        if cls == "bytes":
            return base64.b64encode(bytes(value)).decode()
        if cls in ("date", "timestamp", "time"):
            return canon.render_value(cls, value)
        return str(value)

    @staticmethod
    def _from_source(declared, value):
        import datetime
        from decimal import Decimal

        from .. import canon
        if value is None:
            return None
        cls = canon.type_class("opensearch", declared)
        if cls == "decimal":
            return Decimal(str(value))
        if cls == "integer":
            return int(value)
        if cls == "float":
            return float(value)
        if cls == "bytes":
            return base64.b64decode(value)
        if cls == "timestamp" and isinstance(value, str):
            return datetime.datetime.fromisoformat(value.replace("T", " "))
        if cls == "date" and isinstance(value, str):
            return datetime.date.fromisoformat(value[:10])
        if cls == "time" and isinstance(value, str):
            return datetime.time.fromisoformat(value)
        return value

    def _row(self, declared, names, hit):
        src = hit.get("_source") or {}
        return [hit["_id"] if n == "_id" else
                self._from_source(declared.get(n, ""), src.get(n))
                for n in names]

    def _doc_id(self, columns, key, row):
        """A document's id: the canonical text of the row's key, or the id
        itself where the index is keyed by `_id` (an index migkit did not
        make) - the one id `neutral_rows_by_key` looks a row up by."""
        from .. import rowtext
        got = self._key_of(columns, key, row)
        if list(key) == ["_id"]:
            return got[0]
        return rowtext.encode(list(got))

    def _refresh(self, side, db, table):
        if side == "dst":
            self._call(side, "POST",
                       f"/{self._index(side, db, table)}/_refresh")

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        if where:
            raise SystemExit(f"{table} moves under a row filter, which is"
                             " SQL, and OpenSearch takes none")
        self._refresh(side, db, table)
        declared = dict(self.neutral_columns(side, db, table))
        names = [n for n, _ in columns]
        index = self._index(side, db, table)
        got = self._call(side, "POST", f"/{index}/_search?scroll=2m",
                         {"size": self._page(side, db, table, int(size)),
                          "sort": ["_doc"],
                          "_source": [n for n in names if n != "_id"]})
        scroll = got.get("_scroll_id")
        try:
            while True:
                hits = got["hits"]["hits"]
                if not hits:
                    return
                yield [self._row(declared, names, h) for h in hits]
                got = self._call(side, "POST", "/_search/scroll",
                                 {"scroll": "2m", "scroll_id": scroll})
                scroll = got.get("_scroll_id", scroll)
        finally:
            if scroll:
                self._call(side, "DELETE", "/_search/scroll",
                           {"scroll_id": scroll}, missing_ok=True)

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """Documents come back in no order: the copier reads an index in
        one pass (`RESUMES_BY_KEY`). This serves the callers that ask
        whether an index holds anything."""
        rows = []
        for batch in self.neutral_batches(side, db, table, columns,
                                          limit or 1000, where):
            rows += batch
            if limit and len(rows) >= limit:
                return rows[:limit], None
        return rows, None

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        from .. import canon, rowtext
        if not key or not keys:
            return {}
        self._refresh(side, db, table)
        declared = dict(self.neutral_columns(side, db, table))
        cls = dict(columns)
        names = [n for n, _ in columns]
        ids = [kk[0] if key == ["_id"] else rowtext.encode(
            [canon.render_value(cls[k], v) for k, v in zip(key, kk)])
            for kk in keys]
        found = []
        index = self._index(side, db, table)
        for i in range(0, len(ids), 500):
            got = self._call(side, "POST", f"/{index}/_mget",
                             {"ids": ids[i:i + 500]})
            found += [self._row(declared, names, d)
                      for d in got.get("docs", []) if d.get("found")]
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns, 2000,
                                          where):
            k, total = canon.fold_rows(classes, batch, total)
            n += k
        return (n, str(total))

    def table_facts(self, side, db):
        pre = self._prefix(side, db)
        got = self._call(side, "GET", "/_cat/indices?format=json&bytes=b"
                                      "&h=index,docs.count,store.size")
        return {i["index"][len(pre):]: {
                    "rows": int(i.get("docs.count") or 0),
                    "bytes": int(i.get("store.size") or 0), "key": True}
                for i in got or [] if i["index"].startswith(pre)}

    def row_sizes(self, side, db, tables=None):
        """The bytes each document takes as it is carried - its `_source`
        as JSON and its id - read from the index itself, a page at a time
        and only the sizes kept: every document of an index of no more
        than `pieces.EXACT_UP_TO` stored bytes, and the first `SIZE_SAMPLE`
        of a larger one, whose largest is then not an exact bound
        (`exact` False). What one worker holds depends on the widest
        document, not on the index's average."""
        from .base import sizes_of
        try:
            facts = self.table_facts(side, db)
        except Exception:  # noqa: BLE001 - sized without the statistics
            facts = {}
        out = {}
        for t in (tables if tables is not None
                  else self.neutral_tables(side, db)):
            f = facts.get(t) or {}
            exact = int(f.get("bytes") or 0) <= pieces.EXACT_UP_TO
            most = None if exact else self.SIZE_SAMPLE
            lengths = []
            index = self._index(side, db, t)
            try:
                # a few first, then as many to a page as a step of bytes
                # holds at the widest of them: a page of wide documents is
                # never five hundred of them held at once
                first = self._call(side, "POST", f"/{index}/_search",
                                   {"size": 5, "sort": ["_doc"]})
                widest = max([self._carried_bytes(h) for h in
                              (first.get("hits") or {}).get("hits", [])] or
                             [0])
                for hits in self._scroll(
                        side, index,
                        {"size": pieces.rows_for(
                            widest, 500, pieces.SAMPLE_STEP_BYTES),
                         "sort": ["_doc"]}):
                    lengths += [self._carried_bytes(h) for h in hits]
                    if most and len(lengths) >= most:
                        lengths = lengths[:most]
                        break
            except Exception:  # noqa: BLE001 - that index is not sized
                continue
            if lengths:
                out[t] = sizes_of(lengths, exact=exact,
                                  rows=int(f.get("rows") or len(lengths)))
        return out

    @staticmethod
    def _carried_bytes(hit):
        """Bytes one document takes as it is carried: its `_source` as
        JSON, and its id."""
        return (len(json.dumps(hit.get("_source") or {},
                               separators=(",", ":"),
                               ensure_ascii=False).encode())
                + len(str(hit["_id"]).encode()))

    def neutral_column_rules(self, side, db, table):
        """{field: {"null", "default", "identity"}}: what a mapping says of
        a field's rules - nothing. It declares a type, and every field takes
        null and is absent when it is not sent; nothing defaults and
        nothing numbers a document. Only the key is required - it makes
        the document's id - so a SQL table's NOT NULL and defaults are
        what the pair finds this target does not keep."""
        key = set(self.neutral_key(side, db, table))
        return {n: {"null": n not in key, "default": None,
                    "identity": False}
                for n, _ in self.neutral_columns(side, db, table)}

    # ---- writing ----

    def neutral_create_sql(self, side, db, table, columns, key=()):
        if not key:
            raise SystemExit(f"{table} has no key, and a document's id is"
                             " its row's key: without one a row written"
                             " again would be a second document. Give the"
                             " table a key, or leave it out of this hop")
        from .. import canon
        props, cols = {}, []
        for col in columns:
            name, cls, numbers = col[0], col[1], tuple(col[2] or ())
            if cls == canon.OWN:
                # OpenSearch to OpenSearch, row by row: a field as its own
                # type. Measured before: every such index was refused, on
                # its `_id`, the document's own id and no field at all
                own = str(numbers[0]) if numbers else ""
                if name == "_id":
                    cols.append([name, "text"])
                    continue
                if own in FIELDS:
                    props[name] = {"type": own}
                    cols.append([name, FIELDS[own]])
                    continue
                raise SystemExit(
                    f"{name} is a field of type {own or 'object'}, which is"
                    f" not copied row by row; the move's own path carries"
                    f" {table} whole")
            if cls == "decimal":
                scale = numbers[1] if len(numbers) == 2 else None
                props[name] = ({"type": "scaled_float",
                                "scaling_factor": 10 ** scale}
                               if scale is not None else {"type": "double"})
                declared = "decimal" + (f"({numbers[0]},{numbers[1]})"
                                        if scale is not None else "")
            elif cls in MAPPING:
                props[name] = MAPPING[cls]
                declared = cls
            else:
                raise SystemExit(f"a {cls} column ({name}) has no mapping"
                                 " migkit writes; leave it out with"
                                 " mapping.columns")
            cols.append([name, declared])
        return json.dumps({"mappings": {
            "_meta": {"migkit": {"columns": cols, "key": list(key)}},
            "dynamic": "strict", "properties": props}})

    def neutral_create(self, side, db, table, columns, key=()):
        self._target_only(side, "create an index")
        if str(table).lower() in self.neutral_tables(side, db):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace an index that"
                             " is already there")
        body = self.neutral_create_sql(side, db, table, columns, key)
        self._call(side, "PUT", f"/{self._index(side, db, table)}",
                   json.loads(body))
        return body

    def neutral_write(self, side, db, table, columns, rows):
        """One document per row, its id the canonical text of the row's
        key, so a row written again replaces itself."""
        self._target_only(side, "write rows")
        if not rows:
            return 0
        declared = dict(self.neutral_columns(side, db, table))
        key = self.neutral_key(side, db, table)
        names = [n for n, _ in columns]
        index = self._index(side, db, table)
        self._bulk(side, ({
            "_op_type": "index", "_index": index,
            "_id": self._doc_id(columns, key, r),
            "_source": {n: self._to_source(declared.get(n, "text"), v)
                        for n, v in zip(names, r) if n != "_id"}}
            for r in rows))
        return len(rows)

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty an index")
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and OpenSearch"
                             " takes none")
        from .. import freeze
        index = self._index(side, db, table)
        got = self._call(side, "POST", f"/{index}"
                                       "/_delete_by_query?refresh=true",
                         {"query": {"match_all": {}}})
        # a delete each, numbered: migkit's own
        freeze.counted(self.hop, {index: int(got.get("deleted", 0))})
        return int(got.get("deleted", 0))

    # ---- the checks, through the pair every engine compares by ----

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        """The pair's comparison, each finding saying how many rows are
        apart of each kind, as the table's drilldown counted them
        (`Result.rows_apart`), and how its table came out
        (`Result.tables`) - what a hop that keeps some changes off the
        target is held to (`verdict.hold`)."""
        pair = self._as_pair()
        return [self._counted(pair, db, r)
                for r in pair.check_data(db, table, stream)]

    @staticmethod
    def _counted(pair, db, found):
        """`found` with the rows apart of its table counted from the files
        the comparison wrote, and its table's outcome said. The count is
        exact unless the drilldown stopped short of the whole table or
        could not localise the rows."""
        from .base import rows_counted
        if not found.table:
            return found
        found.tables = {found.table: found.status}
        if found.status != "diff":
            return found
        leaf = pair._leaf(found.table)
        counts = {k: len(pair._read_drill(db, leaf, k))
                  for k in ("missing", "extra", "changed")}
        if any(counts.values()):
            whole = not any(w in found.detail for w in (
                "stopped after", "not localised"))
            found.rows_apart = rows_counted(found.table, exact=whole,
                                            **counts)
        return found

    def repair_plan(self, db, kind):
        return self._as_pair().repair_plan(db, kind)

    def apply(self, db, action):
        return self._as_pair().apply(db, action)

    def _apply_deletes(self, side, db, table, keys):
        """These rows' documents taken away - `keys` are {column: value}
        - by the ids their keys give, in bulk requests cut by bytes. One
        that is not there is what was wanted (`_bulk`)."""
        self._target_only(side, "delete documents")
        columns = self.neutral_columns(side, db, table)
        key = self.neutral_key(side, db, table)
        index = self._index(side, db, table)
        names = [n for n, _ in columns]
        self._bulk(side, ({
            "_op_type": "delete", "_index": index,
            "_id": self._doc_id(columns, key,
                                [k.get(n) for n in names])} for k in keys))

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    def check_deep(self, db):
        """The target's indexes' health, and - between two OpenSearch
        clusters, where every index has its twin - how the twins differ:
        their mappings field by field (columns, narrowing, precision,
        collation, what the cluster generates), their options, the kinds of
        value their documents hold (types, null against empty against
        absent). Only the indexes the hop takes (`in_scope`)."""
        out = self._health_findings(db)
        left = self.set_aside(db)
        if left is not None:
            out.append(left)
        if self.hop.engine not in ("opensearch", "elasticsearch"):
            return out
        pairs, unread = self._described_pairs(db)
        return (out + self._mapping_findings(db, pairs, unread)
                + self._value_findings(db, pairs, unread)
                + self._shard_findings(db, pairs, unread)
                + self._boundary_findings(db, pairs, unread)
                + self._object_findings(db, pairs, unread)
                + self._access_findings(db, pairs, unread)
                + self._time_findings(db, pairs, unread))

    def _described_index(self, side, db, table):
        """One index as the cluster describes it: its mappings, its
        settings and its aliases."""
        index = self._index(side, db, table)
        return self._call(side, "GET", f"/{index}")[index]

    def _described_pairs(self, db):
        """([(table, the source's index described, the target's)], {table:
        why it could not be read}) for the indexes both sides hold - an
        index on one side only is the schema check's to name."""
        both = sorted(set(self.neutral_tables("src", db))
                      & set(self.neutral_tables("dst", db)))
        pairs, unread = [], {}
        for t in both:
            try:
                pairs.append((t, self._described_index("src", db, t),
                              self._described_index("dst", db, t)))
            except Exception as e:  # noqa: BLE001 - said, per index
                unread[t] = _last_line(e, 100)
        return pairs, unread

    @staticmethod
    def _judged(asked, found, fine):
        """(status, words) of one kind of difference: what was found, or
        `fine`, and - over indexes that could not all be read - never a
        plain ok (`util.Probes`)."""
        from .base import clauses
        return asked.verdict("diff" if found else "ok",
                             clauses(found) if found else fine)

    def _mapping_findings(self, db, pairs, unread):
        from ..util import Probes
        asked = Probes("indexes", [t for t, _, _ in pairs])
        for t, why in unread.items():
            asked.failed(t, why)
        found = {k: [] for k in ("columns", "narrowing", "precision",
                                 "collation", "generated", "options")}
        for t, a, b in pairs:
            with asked.reading(t):
                for kind, words in mapping_drift(
                        (a.get("mappings") or {}).get("properties") or {},
                        (b.get("mappings") or {}).get("properties") or {}):
                    found[kind].append(f"{t}.{words}")
                found["collation"] += [f"{t}: {w}"
                                       for w in analysis_drift(a, b)]
                found["generated"] += [f"{t}: {w}"
                                       for w in generated_drift(a, b)]
                found["options"] += [f"{t}: {w}"
                                     for w in options_drift(a, b)]
        judged = self._judged
        out = []
        status, detail = judged(
            asked, found["columns"],
            "every field has the same type and attributes on both sides")
        out.append(Result(
            "deep", f"{db} columns", status, detail, "",
            "a field's type cannot change on an index that holds"
            " documents: make the index again with the source's mapping"
            " and reindex" if found["columns"] else "",
            category="structure.columns"))
        status, detail = judged(asked, found["narrowing"],
                                "no field is mapped narrower on the target")
        out.append(Result(
            "deep", f"{db} narrowing", status, detail, "",
            "map the field as wide as the source's, and reindex"
            if found["narrowing"] else "", category="value.narrowing"))
        status, detail = judged(asked, found["precision"],
                                "no field keeps fewer digits on the target")
        out.append(Result(
            "deep", f"{db} precision", status, detail, "",
            "map the field as precisely as the source's, and reindex"
            if found["precision"] else "", category="value.precision"))
        status, detail = judged(
            asked, found["collation"],
            "the analysis of text is the same on both sides")
        out.append(Result(
            "deep", f"{db} collation", status, detail, "",
            "give the target index the source's analyzers, then reindex:"
            " the same text is found by other words under another"
            if found["collation"] else "", category="value.collation"))
        status, detail = judged(
            asked, found["generated"],
            "nothing on either side computes a value the other does not")
        out.append(Result(
            "deep", f"{db} generated", status, detail, "",
            "give both indexes the same ingest pipelines and runtime fields"
            if found["generated"] else "", category="value.generated"))
        status, detail = judged(
            asked, found["options"],
            "the indexes' shards, sort, codec and strictness are the same")
        out.append(Result(
            "deep", f"{db} index options", status, detail, "",
            "make the index again with the source's options: shards and"
            " sort cannot change once it holds documents"
            if found["options"] else "",
            category="structure.collection-options"))
        return out

    def _kinds_of(self, side, db, table):
        """{"docs": n, "paths": {field: {kind: documents}}} of one index:
        every document's `_source` read once and only counted."""
        into = {"docs": 0, "paths": {}}
        for hits in self._scroll(
                side, self._index(side, db, table),
                {"size": self._page(side, db, table, 1000),
                 "sort": ["_doc"]}):
            count_kinds(hits, into)
        return into

    def _value_findings(self, db, pairs, unread):
        """The kinds of value each field holds, counted over every
        document of each twin: a number that became a string, a null that
        became an absent field or an empty one - things a search tells
        apart (`exists`, a term on "") and a count of documents does not.
        Compared only where the twins hold as many documents as each
        other: a count of kinds over different sets differs for that
        reason alone, and the counts check names that."""
        from ..util import Probes
        asked = Probes("indexes", [t for t, _, _ in pairs])
        for t, why in unread.items():
            asked.failed(t, why)
        typed, empty = [], []
        for t, _, _ in pairs:
            with asked.reading(t):
                self._refresh("dst", db, t)
                try:
                    a = self._kinds_of("src", db, t)
                    b = self._kinds_of("dst", db, t)
                except TooManyFields as e:
                    raise RuntimeError(f"more than {MOST_PATHS:,} field"
                                       f" paths ({e})")
                if a["docs"] != b["docs"]:
                    raise RuntimeError(f"{a['docs']:,} documents against"
                                       f" {b['docs']:,}")
                x, y = kinds_drift(a, b)
                typed += [f"{t}.{w}" for w in x]
                empty += [f"{t}.{w}" for w in y]
        status, detail = self._judged(
            asked, typed,
            "every field holds the same kinds of value on both sides")
        out = [Result(
            "deep", f"{db} field types", status, detail, "",
            "copy the documents as they are (the `_source`), not through"
            " a mapping's types" if typed else "",
            category="value.type-drift")]
        status, detail = self._judged(
            asked, empty,
            "null, empty and absent fields count the same on both sides")
        out.append(Result(
            "deep", f"{db} null and empty", status, detail, "",
            "copy the documents as they are: a search tells null, empty"
            " and absent apart" if empty else "",
            category="value.null-empty"))
        return out

    # ---- how the cluster lays the twins out ----

    def _duplicate_ids(self, side, db, table):
        """How many more documents an index holds than ids: a document
        written with a routing of its own lives in the shard the routing
        names, and the same `_id` written without it lives in another - two
        documents the target counts twice and a lookup by id finds one of.
        Every id read, none sampled."""
        seen, total = set(), 0
        for hits in self._scroll(
                side, self._index(side, db, table),
                {"size": 5000, "_source": False, "sort": ["_doc"]}):
            total += len(hits)
            seen.update(h["_id"] for h in hits)
        return total - len(seen)

    def _shard_findings(self, db, pairs, unread):
        """The target's shards held to what makes a count and a lookup
        mean what they say: no shard moving or starting (a count of an
        index whose shards are on the move is a moving target), and no
        `_id` in two shards. The balancer's running and the orphaned
        documents of a sharded database, in a cluster's terms."""
        from ..util import Probes
        asked = Probes("indexes", [t for t, _, _ in pairs])
        for t, why in unread.items():
            asked.failed(t, why)
        moving, twice = [], []
        for t, _, _ in pairs:
            with asked.reading(t):
                index = self._index("dst", db, t)
                got = self._call("dst", "GET", f"/_cluster/health/{index}"
                                               "?level=indices")
                health = (got.get("indices") or {}).get(index) or {}
                busy = (int(health.get("relocating_shards") or 0)
                        + int(health.get("initializing_shards") or 0))
                if busy:
                    moving.append(f"{t}: {busy} shards moving or starting"
                                  " - a count of it is a moving target")
                self._refresh("dst", db, t)
                extra = self._duplicate_ids("dst", db, t)
                if extra:
                    twice.append(f"{t}: {extra:,} documents share an _id"
                                 " with another in a different shard (a"
                                 " routing of their own)")
        found = moving + twice
        status, detail = self._judged(
            asked, found, "no shard of the target is moving, and no _id is"
                          " held in two shards")
        return [Result("deep", f"{db} sharding", status, detail, "",
                       "let the cluster finish moving shards before"
                       " trusting a count; reindex the documents that share"
                       " an _id" if found else "")]

    #: a version from here up was written with the source's own (`_versioned`)
    EXTERNAL = 1 << 43

    @staticmethod
    def _version_of(hit):
        """The version a document is written to the target with: its
        primary term above its sequence number - so a later change always
        carries a larger one - or None where the source gave neither, or a
        term too large to shift."""
        term, seq = hit.get("_primary_term"), hit.get("_seq_no")
        if term is not None and seq is not None and int(term) < 2 ** 20:
            return (int(term) << 43) | int(seq)
        return None

    def _boundary_findings(self, db, pairs, unread):
        """Whether the target is ahead of the source: a document written
        with the source's version (`_versioned`) that the target holds at
        a larger one has been written again on the target by something
        else - a change nothing carried back, or a double-applied write. A
        copy made through rows carries no version and is not judged. The
        same finding follows if the source index was made again since the
        copy: the target then holds what its earlier self wrote."""
        from ..util import Probes
        asked = Probes("indexes", [t for t, _, _ in pairs])
        for t, why in unread.items():
            asked.failed(t, why)
        ahead = []
        for t, _, _ in pairs:
            with asked.reading(t):
                self._refresh("dst", db, t)
                there = {}
                for hits in self._scroll(
                        "dst", self._index("dst", db, t),
                        {"size": 5000, "_source": False, "version": True,
                         "sort": ["_doc"]}):
                    there.update({h["_id"]: int(h.get("_version") or 0)
                                  for h in hits
                                  if int(h.get("_version") or 0)
                                  >= self.EXTERNAL})
                if not there:
                    continue
                here = {}
                for hits in self._scroll(
                        "src", self._index("src", db, t),
                        {"size": 5000, "_source": False,
                         "seq_no_primary_term": True, "sort": ["_doc"]}):
                    here.update({h["_id"]: self._version_of(h)
                                 for h in hits})
                late = sorted(i for i, v in there.items()
                              if here.get(i) is not None and v > here[i])
                if late:
                    ahead.append(f"{t}: {len(late):,} documents newer on"
                                 f" the target ({', '.join(late[:3])}"
                                 + (" ..." if len(late) > 3 else "") + ")")
        status, detail = self._judged(
            asked, ahead, "no document is held at a newer version on the"
                          " target than on the source")
        return [Result("deep", f"{db} boundary", status, detail, "",
                       "find what writes on the target before cutover: a"
                       " change made there is lost to the source's next"
                       " write, or to a rollback" if ahead else "")]

    # ---- what lies beside the indexes ----

    def _optional(self, side, path):
        """What the cluster answers, None where it has no such thing: the
        path does not exist (404) or the plugin that answers it is not
        there (400, 405)."""
        try:
            return self._call(side, "GET", path)
        except RuntimeError as e:
            code = str(e).split(":", 2)[1].strip()[:3] if ":" in str(e) \
                else ""
            if code in ("400", "404", "405"):
                return None
            raise

    @staticmethod
    def _matches(name, pattern):
        """Whether an index name is one a pattern of the security plugin or
        of a template names: shell wildcards, or a regular expression
        between slashes."""
        import fnmatch
        import re
        pattern = str(pattern)
        if len(pattern) > 2 and pattern.startswith("/") \
                and pattern.endswith("/"):
            try:
                return re.fullmatch(pattern[1:-1], name) is not None
            except re.error:
                return False
        return fnmatch.fnmatchcase(name, pattern)

    def _lifecycle(self, side, db, table, described):
        """(policy, its definition) the index is managed by, or None: the
        policy index management (OpenSearch) or lifecycle management
        (Elasticsearch) rolls it over, shrinks and deletes it by. The
        definition is None where the policy is named and not there; what
        changes by itself (versions, dates of change) is left out of it."""
        index = self._index(side, db, table)
        if self._brands()[0 if side == "src" else 1].name == "elasticsearch":
            name = _settings_of(described).get("lifecycle.name")
            if not name:
                return None
            got = (self._optional(side, f"/_ilm/policy/{name}") or {}).get(
                name) or {}
            return name, got.get("policy")
        got = (self._optional(side, f"/_plugins/_ism/explain/{index}")
               or {}).get(index) or {}
        name = got.get("index.plugins.index_state_management.policy_id") \
            or got.get("policy_id")
        if not name:
            return None
        body = (self._optional(side, f"/_plugins/_ism/policies/{name}")
                or {}).get("policy") or {}
        if not body:
            return name, None
        return name, {k: body.get(k) for k in (
            "default_state", "states", "ism_template", "description")}

    def _beside(self, side, db, held):
        """{what: (definition, table)} of the objects beside the indexes
        `held` ([(table, the index described)]) that belong to them: their
        aliases (a filtered one is a view of the index), the templates
        whose patterns name them, the ingest pipelines their settings run
        every document through, and the policy that manages them. Each
        with the table that makes it the hop's: `Engine.takes` asks the
        hop's `objects` of it."""
        out = {}
        names = {self._index(side, db, t): t for t, _ in held}
        for t, d in held:
            for alias, body in sorted((d.get("aliases") or {}).items()):
                kind = "view" if (body or {}).get("filter") else "alias"
                if self.takes(db, kind, alias, by=t):
                    out[f"{kind} {alias} on {t}"] = (body or {}, t)
            settings = _settings_of(d)
            for key in ("default_pipeline", "final_pipeline"):
                name = settings.get(key)
                if name and name != "_none" and self.takes(
                        db, "pipeline", name, by=t):
                    got = (self._optional(side, f"/_ingest/pipeline/{name}")
                           or {}).get(name)
                    if got is not None:
                        out[f"pipeline {name}"] = (got, t)
            managed = self._lifecycle(side, db, t, d)
            if managed and managed[1] is not None and self.takes(
                    db, "policy", managed[0], by=t):
                out[f"policy {managed[0]}"] = (managed[1], t)
        shown = self._optional(side, "/_index_template") or {}
        for entry in shown.get("index_templates") or []:
            body = entry.get("index_template") or {}
            hit = next((names[n] for n in sorted(names) for p in
                        body.get("index_patterns") or []
                        if self._matches(n, p)), None)
            if hit and self.takes(db, "template", entry.get("name"),
                                  by=hit):
                out[f"template {entry.get('name')}"] = (body, hit)
        for name, body in sorted((self._optional(side, "/_template")
                                  or {}).items()):
            hit = next((names[n] for n in sorted(names) for p in
                        (body or {}).get("index_patterns") or []
                        if self._matches(n, p)), None)
            if hit and self.takes(db, "template", name, by=hit):
                out[f"legacy template {name}"] = (body, hit)
        return out

    def _object_findings(self, db, pairs, unread):
        """What lies beside the indexes - aliases, templates, ingest
        pipelines, lifecycle policies - held to the twin's: one missing on
        the target means an application reading by an alias finds nothing,
        a new index made without its template, a write that skips its
        pipeline. The hop's `objects` leaves a kind out of this, and an
        object whose index the hop leaves out."""
        from ..util import Probes
        asked = Probes("sides", ["source", "target"])
        found = []
        mine = [(t, a) for t, a, _ in pairs]
        theirs = [(t, b) for t, _, b in pairs]
        got = {}
        for side, held in (("source", mine), ("target", theirs)):
            with asked.reading(side):
                got[side] = self._beside("src" if side == "source" else "dst",
                                         db, held)
        if len(got) == 2:
            src, dst = got["source"], got["target"]
            prefix = (self._prefix("src", db), self._prefix("dst", db))
            for key in sorted(set(src) | set(dst)):
                if key not in dst:
                    found.append(f"{key}: missing on the target")
                elif key not in src:
                    found.append(f"{key}: only on the target")
                elif self._moved(src[key][0], prefix) != dst[key][0]:
                    found.append(f"{key}: differs")
        status, detail = self._judged(
            asked, found, "the aliases, templates, pipelines and policies"
                          " of the indexes are the same on both sides")
        return [Result("deep", f"{db} objects", status, detail, "",
                       "make them on the target from the source's"
                       " definitions (the same names, the target's index"
                       " names in their patterns)" if found else "")]

    @staticmethod
    def _moved(body, prefix):
        """A definition with the source's index prefix as the target
        calls it, where the hop gives a database another name there."""
        if prefix[0] == prefix[1]:
            return body
        text = json.dumps(body, sort_keys=True)
        return json.loads(text.replace(f'"{prefix[0]}', f'"{prefix[1]}'))

    # ---- who may do what to them ----

    @classmethod
    def _grants_on(cls, role, index):
        """([actions], [(row filter, field filter, masked fields)]) a role
        grants on one index, from the security plugin's shape
        (`index_permissions`) or Elasticsearch's (`indices`)."""
        actions, rows = set(), set()
        for perm in role.get("index_permissions") or []:
            if any(cls._matches(index, p)
                   for p in perm.get("index_patterns") or []):
                actions.update(perm.get("allowed_actions") or [])
                if perm.get("dls") or perm.get("fls") or perm.get(
                        "masked_fields"):
                    rows.add((str(perm.get("dls") or ""),
                              tuple(sorted(perm.get("fls") or [])),
                              tuple(sorted(perm.get("masked_fields")
                                           or []))))
        for perm in role.get("indices") or []:
            if any(cls._matches(index, p) for p in perm.get("names") or []):
                actions.update(perm.get("privileges") or [])
                fields = perm.get("field_security") or {}
                if perm.get("query") or fields:
                    rows.add((str(perm.get("query") or ""),
                              tuple(sorted(fields.get("grant") or [])
                                    + sorted("~" + f for f in
                                             fields.get("except") or [])),
                              ()))
        return sorted(actions), sorted(rows)

    def _access_findings(self, db, pairs, unread):
        """Who may do what to each index - the roles of the security plugin
        that grant on it, with the rows (document-level security) and
        fields they hide - held to the twin's. A role that may read an
        index on the source and not on the target is an application that
        stops working; one that may on the target and not on the source, a
        door opened."""
        from .. import users
        from ..util import Probes
        try:
            s = users._os_security(self.hop, "src")
            t = users._os_security(self.hop, "dst")
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said
            why = str(e).strip().splitlines()[0][:100] if str(e).strip() \
                else type(e).__name__
            return [Result("deep", f"{db} grants", "error",
                           f"the roles could not be read: {why}"),
                    Result("deep", f"{db} rls", "error",
                           f"the roles could not be read: {why}")]
        names = {tbl: self._index("dst", db, tbl) for tbl, _, _ in pairs}
        gap, rows = [], []
        if s is None and t is None:
            fine = ("neither cluster runs the security plugin: no role"
                    " grants or hides anything to differ")
        else:
            fine = None
            s, t = s or {}, t or {}
            if s and not t:
                gap.append("the source's roles grant on these indexes and"
                           " the target runs no security plugin")
            for key in sorted(k for k in set(s) | set(t)
                              if k.startswith("role:")):
                a = users._os_patterns(self.hop, s.get(key) or {})
                b = t.get(key) or {}
                for tbl, index in sorted(names.items()):
                    if not self.takes(db, "role", key[5:], by=tbl):
                        continue
                    (acts_a, rows_a), (acts_b, rows_b) = (
                        self._grants_on(a, index), self._grants_on(b, index))
                    if key not in t and acts_a:
                        gap.append(f"{key[5:]} may {', '.join(acts_a[:3])}"
                                   f" on {tbl} on the source; the role is"
                                   " missing on the target")
                    elif acts_a != acts_b:
                        gap.append(f"{key[5:]} on {tbl}: "
                                   f"{', '.join(acts_a) or 'nothing'} ->"
                                   f" {', '.join(acts_b) or 'nothing'}")
                    if rows_a != rows_b:
                        rows.append(f"{key[5:]} on {tbl}: row, field and"
                                    f" masking rules {rows_a} -> {rows_b}")
        asked = Probes("sides", ["source", "target"])
        for side in ("source", "target"):
            asked.done(side)
        status, detail = self._judged(
            asked, gap, fine or "the roles grant the same on every index")
        out = [Result("deep", f"{db} grants", status, detail, "",
                      "carry the roles with `migkit users`" if gap else "")]
        status, detail = self._judged(
            asked, rows, fine or "the roles hide the same rows and fields"
                                 " of every index")
        out.append(Result("deep", f"{db} rls", status, detail, "",
                          "carry the roles with `migkit users`: a role"
                          " without its row filter shows what it hid"
                          if rows else ""))
        return out

    # ---- when a value was ----

    #: documents of an index whose dates are read from both sides to tell
    #: a whole index shifted in time from rows that differ one by one
    SHIFT_SAMPLE = 200

    @classmethod
    def _date_fields(cls, properties, path=""):
        out = []
        for name, field in sorted((properties or {}).items()):
            here = f"{path}{name}"
            if _type_of(field) in _MOMENT:
                out.append(here)
            out += cls._date_fields(field.get("properties"), here + ".")
        return out

    def _time_findings(self, db, pairs, unread):
        """Whether the dates of an index are held shifted in time on the
        target: the same documents' instants read from both sides
        (`docvalue_fields` in `epoch_millis`, the zone out of it), and a
        single offset on every one is a conversion applied to the whole
        field - a mover's zone - and not rows corrupted one by one. Read
        for the first `SHIFT_SAMPLE` documents of each index with a date
        field: the finding names a pattern, and an ok says none was
        seen."""
        from .. import verdict
        from ..util import Probes
        asked = Probes("indexes", [t for t, _, _ in pairs])
        for t, why in unread.items():
            asked.failed(t, why)
        shifts = []
        for t, a, _ in pairs:
            fields = self._date_fields((a.get("mappings") or {}).get(
                "properties"))
            with asked.reading(t):
                if not fields:
                    continue
                ask = {"size": self.SHIFT_SAMPLE, "sort": ["_doc"],
                       "_source": False,
                       "docvalue_fields": [{"field": f,
                                            "format": "epoch_millis"}
                                           for f in fields]}
                src = self._call("src", "POST", "/" + self._index(
                    "src", db, t) + "/_search", ask)["hits"]["hits"]
                ids = [h["_id"] for h in src]
                if not ids:
                    continue
                self._refresh("dst", db, t)
                dst = self._call("dst", "POST", "/" + self._index(
                    "dst", db, t) + "/_search",
                    dict(ask, size=len(ids),
                         query={"ids": {"values": ids}}))["hits"]["hits"]
                for f in fields:
                    line = verdict.uniform_shift(
                        f"{t}.{f}", self._instants(src, f),
                        self._instants(dst, f))
                    if line:
                        shifts.append(line)
        found = verdict.timeshift_result(db, shifts)
        found.status, found.detail = asked.verdict(found.status,
                                                   found.detail)
        return [found]

    @staticmethod
    def _instants(hits, field):
        """{id: the field's instant in seconds} of hits read with
        `docvalue_fields` in `epoch_millis`."""
        out = {}
        for h in hits:
            got = (h.get("fields") or {}).get(field)
            if got:
                out[h["_id"]] = float(got[0]) / 1000.0
        return out

    def _health_findings(self, db):
        """A red index is missing shards, and what a check reads from it
        is not all there is. A red index of another application's is not
        this move's to report."""
        pre = self._prefix("dst", db)
        try:
            got = self._call("dst", "GET",
                             "/_cat/indices?format=json&h=index,health")
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} target index health", "error",
                           "could not read the target's indexes:"
                           f" {str(e).splitlines()[0][:90]}")]
        red = [i["index"] for i in got or []
               if i["index"].startswith(pre) and i.get("health") == "red"
               and self.in_scope(db, i["index"][len(pre):])]
        if red:
            return [Result("deep", f"{db} target index health", "diff",
                           f"{len(red)} indexes missing shards:"
                           f" {', '.join(red[:6])}", "",
                           "bring the shards back before trusting a check")]
        return [Result("deep", f"{db} target index health", "ok",
                       "no index is missing a shard")]

    # ---- settings ----

    #: settings that change what a search finds or whether a write lands:
    #: an analyzer is how text is split into what is searched, a field
    #: limit refuses a document with one more field
    CRITICAL_PARAMS = ("action.auto_create_index",
                       "index.mapping.total_fields.limit",
                       "index.mapping.depth.limit",
                       "index.mapping.nested_fields.limit",
                       "index.max_result_window",
                       "indices.query.bool.max_clause_count",
                       "search.max_buckets")

    #: per-index settings that are the index's own, not a choice to carry
    OWN_SETTINGS = ("index.uuid", "index.creation_date", "index.version",
                    "index.provided_name", "index.routing",
                    "index.resize", "index.history")

    def _cluster_settings(self, side):
        """The cluster's settings as it runs them: its defaults, under what
        was set for good (`persistent`), under what was set till a restart
        (`transient`)."""
        got = self._call(side, "GET", "/_cluster/settings?include_defaults"
                                      "=true&flat_settings=true")
        out = {}
        for part in ("defaults", "persistent", "transient"):
            out.update({str(k): v for k, v in (got.get(part) or {}).items()})
        return out

    def _settings(self, side, db):
        out = {k: str(v) for k, v in self._cluster_settings(side).items()}
        for t in self.neutral_tables(side, db):
            index = self._index(side, db, t)
            got = self._call(side, "GET", f"/{index}/_settings"
                                          "?flat_settings=true")
            for k, v in ((got.get(index) or {}).get("settings") or {}
                         ).items():
                if not str(k).startswith(self.OWN_SETTINGS):
                    out[f"{t}:{k}"] = str(v)
        return {k: v for k, v in out.items()
                if not any(w in k.lower() for w in ("password", "secret",
                                                    "keystore"))}

    def check_params(self, db):
        """The cluster's settings and each index's own, both sides, through
        the report every engine's settings go through. An index's analysis
        (its analyzers, tokenizers, filters) counts as behaviour: the same
        text is found by other words under another."""
        def pull(side):
            try:
                return self._settings(side, db)
            except Exception as e:  # noqa: BLE001 - said by the report
                return {self.UNREADABLE: str(e).splitlines()[0][:80]}
        src, dst = pull("src"), pull("dst")
        crit = sorted({n for n in set(src) | set(dst)
                       if n.split(":")[-1] in self.CRITICAL_PARAMS
                       or ".analysis." in n})
        return self._param_result(
            db, src, dst, crit,
            "give the target's indexes the source's analysis and limits"
            " before cutover")

    # ---- before the move: what the target has room for, who writes in it ----

    def _allocation(self, side):
        """[(node, free bytes, total bytes)] of the nodes that hold data,
        from the cluster's allocation table."""
        out = []
        for row in self._call(side, "GET",
                              "/_cat/allocation?format=json&bytes=b") or []:
            if row.get("node") == "UNASSIGNED" or \
                    row.get("disk.avail") in (None, ""):
                continue
            out.append((row.get("node"), int(row["disk.avail"]),
                        int(row["disk.total"])))
        return out

    def _room(self, side):
        """(bytes the data nodes will still take, how many nodes): each
        node's free disk less what its low watermark keeps free - past it
        the cluster allocates no new shard to the node, and past its flood
        stage it makes the indexes on it read-only. Where the disk
        threshold is turned off, the free disk itself."""
        settings = self._cluster_settings(side)
        low = settings.get("cluster.routing.allocation.disk.watermark.low",
                           "85%")
        enforced = str(settings.get(
            "cluster.routing.allocation.disk.threshold_enabled",
            "true")).lower() != "false"
        nodes = self._allocation(side)
        room = sum(max(0, free - ((kept_free(low, total) or 0)
                                  if enforced else 0))
                   for _, free, total in nodes)
        return room, len(nodes)

    def free_bytes(self, side, db):
        """The disk the data nodes will still take (`_room`), None where
        the cluster gives no figure."""
        room, nodes = self._room(side)
        return room if nodes else None

    def capacity(self, side, db):
        """What the cluster can give a move, for `sizing`: `cpus`, the
        processors its data nodes were given, and `running`, the threads of
        their write and search pools busy now. A figure the cluster does
        not answer is left out - {} says nothing, never idle."""
        out = {}
        try:
            nodes = (self._call(side, "GET", "/_nodes/os") or {}).get(
                "nodes") or {}
            cpus = sum(int((n.get("os") or {}).get("allocated_processors")
                           or 0) for n in nodes.values()
                       if any(r == "data" or r.startswith("data_")
                              for r in n.get("roles") or ["data"]))
            if cpus:
                out["cpus"] = cpus
        except Exception:  # noqa: BLE001 - not answered, left out
            pass
        try:
            stats = (self._call(side, "GET", "/_nodes/stats/thread_pool")
                     or {}).get("nodes") or {}
            out["running"] = sum(
                int(((n.get("thread_pool") or {}).get(pool) or {}).get(
                    "active") or 0)
                for n in stats.values() for pool in ("write", "search"))
        except Exception:  # noqa: BLE001 - not answered, left out
            pass
        return out

    def _primary_sizes(self, side, db):
        """{table: (bytes of its primary shards, primaries, replicas)} of
        the indexes the hop takes."""
        pre = self._prefix(side, db)
        out = {}
        for row in self._call(side, "GET", "/_cat/indices?format=json"
                                           "&bytes=b&h=index,pri,rep,"
                                           "pri.store.size") or []:
            name = row["index"]
            if name.startswith(pre) and self.in_scope(db, name[len(pre):]):
                out[name[len(pre):]] = (int(row.get("pri.store.size") or 0),
                                        int(row.get("pri") or 1),
                                        int(row.get("rep") or 0))
        return out

    #: share of the room the target has that a move may fill before it is
    #: said to be close
    ROOM_WARN = 0.8

    PREFLIGHT = ("_room_gaps", "_write_blocks")

    def _room_gaps(self, db):
        """Whether the target takes the move, asked before it begins: the
        disk its data nodes will still take (their free disk less the low
        watermark) against the primaries of every index the hop takes, with
        the replicas the index keeps - less what the target already holds
        of the same indexes - and the shards the cluster allows (its
        `max_shards_per_node` for each data node) against the shards the
        move would add. The figures are the source's stored sizes: what
        the target stores them as differs a little with its mapping and
        codec."""
        from ..wording import human_bytes
        scope = f"{db} target room"
        try:
            want = self._primary_sizes("src", db)
            held = self._primary_sizes("dst", db)
            room, nodes = self._room("dst")
            health = self._call("dst", "GET", "/_cluster/health") or {}
            settings = self._cluster_settings("dst")
        except Exception as e:  # noqa: BLE001 - said, as the error
            return Result("deep", scope, "error",
                          "the target's room could not be read: "
                          f"{_last_line(e)}"
                          " - unknown, not clean")
        need = sum(max(0, size - held.get(t, (0, 0, 0))[0]) * (1 + rep)
                   for t, (size, pri, rep) in want.items())
        added = sum(pri * (1 + rep) for t, (size, pri, rep) in want.items()
                    if t not in held)
        data_nodes = int(health.get("number_of_data_nodes") or nodes or 0)
        allowed = int(settings.get("cluster.max_shards_per_node", 1000)) \
            * data_nodes
        open_now = int(health.get("active_shards") or 0) + int(
            health.get("unassigned_shards") or 0)
        lines, status = [], "ok"
        if not nodes:
            status = "warn"
            lines.append("the target gives no disk figures, so its room is"
                         " unknown, not enough")
        elif need > room:
            status = "diff"
            lines.append(f"the move needs about {human_bytes(need)} on the"
                         " target's disks (primaries and replicas) and its"
                         f" {nodes} data nodes will take {human_bytes(room)}"
                         " before their low watermark")
        elif need > room * self.ROOM_WARN:
            status = "warn"
            lines.append(f"the move needs about {human_bytes(need)}, most of"
                         f" the {human_bytes(room)} the target's {nodes}"
                         " data nodes will take before their low watermark")
        else:
            lines.append(f"the move needs about {human_bytes(need)} of the"
                         f" {human_bytes(room)} the target's {nodes} data"
                         " nodes will take")
        if data_nodes and added > allowed - open_now:
            status = "diff"
            lines.append(f"the move adds {added} shards and the target has"
                         f" room for {max(0, allowed - open_now)} more"
                         f" ({open_now} open of {allowed}): it refuses to"
                         " make the index that passes the limit")
        return Result("deep", scope, status, "; ".join(lines), "",
                      "free disk or add data nodes to the target, or raise"
                      " cluster.max_shards_per_node, before the move"
                      if status in ("diff", "warn") else "")

    def _write_blocks(self, db):
        """The indexes of the target the hop takes that refuse writes: a
        block (`index.blocks.write`, `read_only`, and the read-only
        `read_only_allow_delete` a node at its flood stage sets and does
        not lift on its own), or the index closed - a move into them
        stops at the first bulk request."""
        scope = f"{db} target writes"
        blocked = []
        try:
            status = {r["index"]: r.get("status") for r in self._call(
                "dst", "GET", "/_cat/indices?format=json&h=index,status")
                or []}
            for t in self.neutral_tables("dst", db):
                index = self._index("dst", db, t)
                if status.get(index) == "close":
                    blocked.append(f"{t} is closed")
                    continue
                got = self._call("dst", "GET", f"/{index}/_settings"
                                               "?flat_settings=true")
                flat = (got.get(index) or {}).get("settings") or {}
                why = [k.split(".")[-1] for k in (
                    "index.blocks.write", "index.blocks.read_only",
                    "index.blocks.read_only_allow_delete")
                    if str(flat.get(k, "false")).lower() == "true"]
                if why:
                    blocked.append(f"{t} is {' and '.join(why)}")
            cluster = self._cluster_settings("dst")
            if str(cluster.get("cluster.blocks.read_only",
                               "false")).lower() == "true":
                blocked.append("the whole cluster is read-only")
        except Exception as e:  # noqa: BLE001 - said, as the error
            return Result("deep", scope, "error",
                          "the target's blocks could not be read: "
                          f"{_last_line(e)}"
                          " - unknown, not clean")
        if blocked:
            return Result("deep", scope, "diff",
                          "writes into the target are refused: "
                          + "; ".join(blocked), "",
                          "lift the block (a node past its flood stage sets"
                          " read_only_allow_delete; free disk first) or open"
                          " the index")
        return Result("deep", scope, "ok",
                      "no index the hop takes on the target is closed or"
                      " blocked against writes")

    #: seconds the target's indexing counters are watched for a writer that
    #: is not migkit's own
    WRITER_PROBE = 1.0

    def stream_writers(self, db):
        """What writes into the target's indexes besides migkit: a reindex
        or a by-query task running now, an index that follows another
        cluster's (cross-cluster replication), and - watched for
        `WRITER_PROBE` seconds - indexing that no task or follower
        explains: an application, a shipper. None of them can be paused
        from here."""
        import time

        from .. import tailctl
        out = super().stream_writers(db)
        names = [self._index("dst", db, t)
                 for t in self.neutral_tables("dst", db)]
        if not names:
            return out
        tasks = self._optional("dst", "/_tasks?actions=*reindex,*byquery"
                                      "&detailed=true") or {}
        for node in (tasks.get("nodes") or {}).values():
            for task in (node.get("tasks") or {}).values():
                text = str(task.get("description") or "")
                hit = [n for n in names if n in text]
                if hit:
                    kind = "reindex" if "reindex" in str(
                        task.get("action")) else "update or delete by query"
                    out.append((f"a running {kind} into {hit[0]}", False))
        replicated = set(((self._optional(
            "dst", "/_plugins/_replication/follower_stats") or {}).get(
                "index_stats") or {})) | {
            i.get("index") for i in (((self._optional(
                "dst", "/_ccr/stats") or {}).get("follow_stats") or {}).get(
                    "indices") or [])}
        for name in names:
            if name in replicated:
                out.append((f"replication of another cluster's index into"
                            f" {name}", False))
        if not tailctl.alive(self.hop.report_dir(db), self.hop):
            before = self._indexed("dst", db, names)
            time.sleep(self.WRITER_PROBE)
            wrote = self._indexed("dst", db, names) - before
            if wrote > 0:
                out.append((f"something writing into the target's indexes"
                            f" ({wrote:,} documents in"
                            f" {self.WRITER_PROBE:g}s)", False))
        return out

    def _indexed(self, side, db, names):
        """Documents the primary shards of these indexes have indexed so
        far (a counter that only goes up while they are written)."""
        got = self._call(side, "GET", f"/{self._prefix(side, db)}*"
                                      "/_stats/indexing") or {}
        return sum(int(((v.get("primaries") or {}).get("indexing") or {})
                       .get("index_total") or 0)
                   for k, v in (got.get("indices") or {}).items()
                   if k in names)

    def _handwork(self):
        """The work the move will not do, counted and named
        (`migkit.handwork`): what lies beside the source's indexes - its
        aliases, templates, ingest pipelines and the policies that manage
        them - which a copy of the documents does not carry, the data
        streams whose backing indexes no prefix names, and the indexes that
        are closed (unreadable until someone opens them, which is a change
        to the source). A probe that could not run is unknown, never
        none."""
        from .. import handwork
        inv = handwork.Inventory()
        for db in self.databases():
            try:
                held = [(t, self._described_index("src", db, t))
                        for t in self.neutral_tables("src", db)]
                beside = self._beside("src", db, held)
            except Exception as e:  # noqa: BLE001 - said, per database
                inv.unknown("not-carried", db, "what lies beside the"
                            " indexes could not be read: "
                            f"{_last_line(e, 80)}")
                held, beside = [], {}
            for title, words in (("aliases", ("alias ", "view ")),
                                 ("templates", ("template ",
                                                "legacy template ")),
                                 ("ingest pipelines", ("pipeline ",))):
                inv.add("not-carried", db, title,
                        [k for k in sorted(beside) if k.startswith(words)])
            inv.add("decide-then-apply", db,
                    "lifecycle policies that roll over or delete the"
                    " indexes (attached on the target before the move ends,"
                    " they act on the copy)",
                    [k for k in sorted(beside) if k.startswith("policy ")])
            try:
                pre = self._prefix("src", db)
                streams = [s.get("name") for s in (self._optional(
                    "src", "/_data_stream") or {}).get("data_streams") or []
                    if str(s.get("name")).startswith(pre)
                    and self.in_scope(db, str(s["name"])[len(pre):])]
                inv.add("not-carried", db, "data streams", streams)
                closed = [r["index"][len(pre):] for r in self._call(
                    "src", "GET", "/_cat/indices?format=json&h=index,status")
                    or [] if r["index"].startswith(pre)
                    and r.get("status") == "close"
                    and self.in_scope(db, r["index"][len(pre):])]
                inv.add("decide-then-apply", db, "closed indexes", closed)
            except Exception as e:  # noqa: BLE001 - said, per database
                inv.unknown("not-carried", db, "data streams and closed"
                            " indexes could not be listed: "
                            f"{_last_line(e, 80)}")
        return inv

    def _assess_extra(self):
        """The readiness checks of this engine: each side's cluster health
        (a red cluster is missing shards, and what is read from it is not
        all there is) and the work the move leaves to hands
        (`_handwork`)."""
        items = []
        for side, word in (("src", "source"), ("dst", "target")):
            try:
                health = self._call(side, "GET", "/_cluster/health") or {}
            except Exception as e:  # noqa: BLE001 - said, as the error
                items.append({"level": "warn", "scope": f"{word} cluster",
                              "item": "cluster health",
                              "detail": "could not be read: "
                              f"{_last_line(e)}"
                              " - unknown, not clean"})
                continue
            colour = health.get("status")
            items.append({
                "level": {"green": "pass", "yellow": "pass"}.get(
                    colour, "fail" if colour == "red" else "warn"),
                "scope": f"{word} cluster", "item": "cluster health",
                "detail": f"{colour}: "
                          f"{health.get('number_of_data_nodes', '?')} data"
                          f" nodes, {health.get('active_shards', '?')}"
                          " active shards, "
                          f"{health.get('unassigned_shards', 0)} unassigned"
                          + ("; a red cluster is missing primary shards, and"
                             " what is read from it is not all there is"
                             if colour == "red" else "")})
        inv = self._handwork()
        return items + inv.rows() + inv.summary()

    # ---- snapshot ----

    def snapshot_state(self, db, state_dir, kind="all"):
        """The target's indexes snapshotted by the cluster itself into a
        file repository under its own `path.repo`, where it has one; where
        it has none, each index's mapping, settings and count are kept,
        and why no snapshot was taken is said."""
        indexes = [self._index("dst", db, t)
                   for t in self.neutral_tables("dst", db)]
        out = {"indexes": {}}
        for index in indexes:
            got = self._call("dst", "GET", f"/{index}")
            count = self._call("dst", "GET", f"/{index}/_count")
            out["indexes"][index] = {"definition": got.get(index),
                                     "count": count.get("count")}
        nodes = self._call("dst", "GET", "/_nodes/settings")
        repos = sorted({r for n in (nodes.get("nodes") or {}).values()
                        for r in ((n.get("settings") or {}).get("path") or {})
                        .get("repo") or []})
        if not repos:
            out["no_snapshot"] = ("the target cluster has no path.repo, so"
                                  " there is no file repository to take one"
                                  " into")
        elif indexes:
            repo = f"migkit-{self.hop.name}".lower()
            self._call("dst", "PUT", f"/_snapshot/{repo}", {
                "type": "fs", "settings": {
                    "location": posixpath.join(repos[0], repo)}})
            name = f"migkit-{state_dir.name}".lower()
            got = self._call("dst", "PUT",
                             f"/_snapshot/{repo}/{name}"
                             "?wait_for_completion=true",
                             {"indices": ",".join(indexes),
                              "include_global_state": False})
            snap = got.get("snapshot") or {}
            out["snapshot"] = {"repository": repo, "name": name,
                               "state": snap.get("state"),
                               "shards": snap.get("shards")}
        (state_dir / "dst-indexes.json").write_text(
            json.dumps(out, indent=2, sort_keys=True, default=str))

    # ---- verifying only what changed ----

    def _shard_marks(self, side, db, table):
        """{primary shard: (highest sequence number, deleted docs)}: every
        write to a shard takes the next sequence number."""
        index = self._index(side, db, table)
        got = self._call(side, "GET", f"/{index}/_stats/docs"
                                      "?level=shards")
        out = {}
        for n, copies in ((got.get("indices") or {}).get(index, {})
                          .get("shards") or {}).items():
            for c in copies:
                if (c.get("routing") or {}).get("primary"):
                    out[str(n)] = [int((c.get("seq_no") or {})
                                       .get("max_seq_no", -1)),
                                   int((c.get("docs") or {})
                                       .get("deleted", 0))]
        return out

    def _written_since(self, db, table, before, now):
        """The ids of the source's documents written after `before` in each
        shard, asked of that shard by sequence number."""
        index = self._index("src", db, table)
        ids = set()
        for shard, (top, _) in now.items():
            was = int((before.get(shard) or [-1])[0])
            if top <= was:
                continue
            for hits in self._scroll(
                    "src", index, {"size": 1000, "_source": False,
                                   "sort": ["_doc"],
                                   "query": {"range": {"_seq_no": {
                                       "gt": was}}}},
                    f"preference=_shards:{shard}"):
                ids.update(h["_id"] for h in hits)
        return ids

    def _sources(self, side, db, table, ids):
        """{id: the document as sorted JSON} of these ids that are there -
        between two OpenSearch clusters a document is compared as it is
        held, not through a rendering."""
        index, out, ids = self._index(side, db, table), {}, sorted(ids)
        for i in range(0, len(ids), 500):
            got = self._call(side, "POST", f"/{index}/_mget",
                             {"ids": ids[i:i + 500]})
            out.update({d["_id"]: json.dumps(d.get("_source"),
                                             sort_keys=True)
                        for d in got.get("docs", []) if d.get("found")})
        return out

    def _ids(self, side, db, table):
        ids = set()
        for hits in self._scroll(side, self._index(side, db, table),
                                 {"size": 5000, "_source": False,
                                  "sort": ["_doc"]}):
            ids.update(h["_id"] for h in hits)
        return ids

    def delta_verify(self, db, limit=20000, log=None):
        """Only the documents written since the last clean run: each
        primary shard's highest sequence number is kept, and the documents
        above it are asked of that shard. A delete leaves nothing to find
        by number, so an index whose deleted count moved has its ids
        compared whole. The written documents are compared on both sides
        as the check compares them; the marks move only on a clean run."""
        state = self.hop.report_dir(db) / "delta-seqno.json"
        prev = json.loads(state.read_text()) if state.exists() else None
        now = {t: self._shard_marks("src", db, t)
               for t in self.neutral_tables("src", db)}
        if prev is None:
            state.write_text(json.dumps(now))
            return [Result("delta", db, "ok", "baseline recorded, writes"
                           " are tracked from this point on")]
        res, clean, total = [], True, 0
        for t, marks in sorted(now.items()):
            was = prev.get(t, {})
            written = self._written_since(db, t, was, marks)
            deleted = any(marks[s][1] != (was.get(s) or [0, 0])[1]
                          for s in marks)
            gone = set()
            if deleted:
                self._refresh("dst", db, t)
                gone = self._ids("dst", db, t) - self._ids("src", db, t)
            if not written and not gone:
                continue
            total += len(written) + len(gone)
            self._refresh("dst", db, t)
            a = self._sources("src", db, t, written)
            b = self._sources("dst", db, t, written)
            bad = sorted(gone | {i for i in written if a.get(i) != b.get(i)})
            clean = clean and not bad
            res.append(Result(
                "delta", f"{db}.{t}", "diff" if bad else "ok",
                f"{len(written)} written, {len(gone)} deleted on the source"
                f" and still on the target" + (f": {len(bad)} differ" if bad
                                               else ", all the same")))
            if log:
                log(f"{t}: {'DIFF' if bad else 'ok'}")
        if clean:
            state.write_text(json.dumps(now))
        res.insert(0, Result("delta", db, "ok" if clean else "diff",
                             f"{total} documents compared, marks"
                             f" {'advanced' if clean else 'NOT advanced'}"))
        return res

    # ---- documents copied as they are ----

    def _scroll(self, side, index, body, where=""):
        """Pages of hits of one search, read through to the end with a
        scroll that is let go of however the reading stops. `where` is
        more of the request's query string (`preference=_shards:0`)."""
        got = self._call(side, "POST", f"/{index}/_search?scroll=2m"
                                       + (f"&{where}" if where else ""), body)
        scroll = got.get("_scroll_id")
        try:
            while got["hits"]["hits"]:
                yield got["hits"]["hits"]
                got = self._call(side, "POST", "/_search/scroll",
                                 {"scroll": "2m", "scroll_id": scroll})
                scroll = got.get("_scroll_id", scroll)
        finally:
            if scroll:
                self._call(side, "DELETE", "/_search/scroll",
                           {"scroll_id": scroll}, missing_ok=True)

    def _table_ways(self):
        """How an index goes (`tableway.each`): out of the source's own
        snapshot where the hop names one and the reader can carry it
        (`fromsnapshot`), by migkit's sliced scroll always."""
        from .. import fromsnapshot
        from ..decide import Need, Rung
        return (
            Rung("snapshot", "the source's own snapshot, the cluster itself"
                             " left unread",
                 gives=frozenset({"resume by chunk"}),
                 needs=(Need("the snapshot reader built here",
                             lambda f: f["reader"],
                             "the snapshot reader is not built here"),
                        Need("an index it can carry",
                             lambda f: f["unfit"] is None,
                             lambda f: f["unfit"])),
                 prove=fromsnapshot.prove,
                 because="the snapshot's shards read on this machine, no"
                         " search on the source"),
            Rung("scroll", "a sliced scroll of the source",
                 because="each document written with the source's own"
                         " version of it"),
        )

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each index's documents copied as the source holds them - the
        `_source`, the `_id` and the routing unchanged - by the way climbed
        for it (`_table_ways`): out of a snapshot of the source the hop
        names, or by a sliced scroll, a slice per worker, each written
        back through `_bulk` with the source's own version of it
        (`_versioned`). An index the target lacks is made with the
        source's mappings and analysis; while it loads it keeps no replica
        and does not refresh, and gets both back after (`_loading`).

        What the target already holds is not emptied first: a document it
        holds at the source's version or a newer one is left, an older one
        replaced, and - where the index held documents before the copy -
        one the source does not hold is taken away after. Emptied, each
        deleted document's version stays behind for a minute
        (`index.gc_deletes`), and the copy's own write of it, at the same
        version, would be refused and the document lost."""
        steps = [f"{t}: documents copied as they are, by a sliced scroll"
                 for t in tables]
        if not go:
            return steps
        from .. import fromsnapshot, tableway
        from ..decide import Facts
        slices = max(1, int(getattr(self.hop, "workers_most", 0)
                            or self.hop.workers or 1))
        self.create_missing(db, log, list(shape_only) + list(tables))

        def run(way, t):
            dst = self._index("dst", db, t)
            self._refresh("dst", db, t)
            held = int(self._call("dst", "GET", f"/{dst}/_count")
                       .get("count", 0))
            with self._loading(db, dst, log):
                if way == "snapshot":
                    moved = fromsnapshot.copy_index(self, db, t, slices, log)
                    how = "out of the source's snapshot"
                else:
                    moved = self._copy_by_scroll(db, t, slices)
                    how = f"in {slices} slices"
            gone, refused = self._strays(db, t) if held else (0, 0)
            if refused:
                raise SystemExit(
                    f"{t}: {refused:,} documents deleted on the target in"
                    " the last minute refuse the source's version of them"
                    " for a minute more - move again after it")
            if log:
                log(f"{t}: {moved:,} documents copied {how}"
                    + (f", {gone:,} the source does not hold taken from"
                       " the target" if gone else ""))
            return moved
        tableway.each(self, db, tables, self._table_ways(),
                      lambda t: Facts(
                          reader=lambda: fromsnapshot.program() is not None,
                          unfit=lambda: fromsnapshot.unfit(self, db, t)),
                      run, log)
        return steps

    #: the file-name stem of a load window's record on disk (`setaside`)
    WINDOW_KIND = "load-window"

    def _window_record(self, db):
        """This process's record of the load windows it holds open
        (`setaside.SetAside`), and what it holds: {index: the settings to
        give back}, shared by the threads of a load."""
        import threading

        from ..setaside import SetAside
        held = self.__dict__.setdefault("_windows", {})
        if db not in held:
            held[db] = (SetAside(self.hop, db, self.WINDOW_KIND), {},
                        threading.Lock())
        return held[db]

    def _given_back(self, db, log=None):
        """Put back what a load that died left open on the target - its
        replicas and its refresh, which only that load's window could
        give back - and let its record go. Asked of the records whose
        process is gone (`setaside`); a load still running keeps its own
        (a change tail's lasts as long as it runs)."""
        aside = self._window_record(db)[0]
        left, files = aside.left_behind()
        there = set(self.neutral_tables("dst", db))
        pre = self._prefix("dst", db)
        for index, restore in sorted(left.items()):
            if index.startswith(pre) and index[len(pre):] in there:
                self._call("dst", "PUT", f"/{index}/_settings",
                           {"index": restore})
                self._call("dst", "POST", f"/{index}/_refresh")
                if log:
                    log(f"{index}: given back the replicas and refresh a"
                        " load that did not finish had taken away")
        for path in files:
            path.unlink(missing_ok=True)

    def _loading(self, db, index, log=None):
        """While `index` loads on the target: no replica and no refresh,
        each put back as it was after, however the load ends. What is to
        be put back is written to disk first (`setaside`), so a process
        that dies inside the window leaves a record the next load - or
        `check` (`set_aside`) - finds, rather than an index that never
        refreshes again."""
        import contextlib

        @contextlib.contextmanager
        def window():
            aside, held, lock = self._window_record(db)
            self._given_back(db, log)
            kept = self._call("dst", "GET", f"/{index}/_settings")
            index_settings = ((kept.get(index) or {}).get("settings")
                              or {}).get("index", {})
            restore = {"number_of_replicas": index_settings.get(
                           "number_of_replicas", "1"),
                       # None: it was not set, and goes back to the default
                       "refresh_interval": index_settings.get(
                           "refresh_interval")}
            with lock:
                held[index] = restore
                if not aside.save(dict(held)):
                    raise SystemExit(
                        f"{index}: the load's record could not be written"
                        f" under {aside.dir}, so its replicas and refresh"
                        " are not taken away - a load that died inside the"
                        " window could not give them back")
            self._call("dst", "PUT", f"/{index}/_settings",
                       {"index": {"number_of_replicas": 0,
                                  "refresh_interval": "-1"}})
            try:
                yield
            finally:
                self._call("dst", "PUT", f"/{index}/_settings",
                           {"index": restore})
                self._call("dst", "POST", f"/{index}/_refresh")
                with lock:
                    held.pop(index, None)
                    if held:
                        aside.save(dict(held))
                    else:
                        aside.done()
        return window()

    def load_window(self, db, log=None, tables=None):
        """The target's indexes held off from replicating and refreshing
        while a load writes into them (`tables`, or every index the hop
        takes there) and given back after, as the settings they had
        (`_loading`) - the indexes and triggers of a SQL target set aside
        for a load, in a search engine's terms: a replica written for
        every document of a bulk, a refresh for every second of it."""
        import contextlib
        there = set(self.neutral_tables("dst", db))
        names = sorted(str(t).lower() for t in (
            tables if tables is not None else there))

        @contextlib.contextmanager
        def window():
            with contextlib.ExitStack() as stack:
                for t in names:
                    if t in there:
                        stack.enter_context(self._loading(
                            db, self._index("dst", db, t), log))
                yield
        return window()

    def set_aside(self, db):
        """What a load took off the target's indexes - their replicas and
        their refresh - and has not given back, as a check result; None
        where nothing is. Held by a load still running (a change tail,
        for as long as it runs) it is off on purpose and goes back when
        that stops; held by one whose process is gone, the index is left
        unreplicated and invisible to a search, and nothing else in the
        check compares an index's settings to what they were."""
        from ..setaside import SetAside
        held, lost, unread = [], [], []
        for path, alive, defs in SetAside(
                self.hop, db, self.WINDOW_KIND).records():
            if defs is None:
                unread.append(str(path))
            elif alive:
                held += sorted(defs)
            else:
                lost += [(i, str(path)) for i in sorted(defs)]
        scope = f"{db} load windows set aside"
        if lost or unread:
            said = []
            if lost:
                said.append(f"{len(lost)} indexes a load took the replicas"
                            " and the refresh from and never gave back: "
                            + ", ".join(i for i, _ in lost[:6]))
            if unread:
                said.append(f"{len(unread)} records that cannot be read: "
                            + ", ".join(unread[:3]))
            return Result("deep", scope, "diff", "; ".join(said), "",
                          "the next load into the database gives them back;"
                          " or give an index its settings by hand")
        if held:
            return Result("deep", scope, "ok",
                          f"{len(held)} indexes are held off by a load that"
                          " is still running: " + ", ".join(held[:6]))
        return None

    def create_missing(self, db, log=None, tables=None):
        """The indexes the hop takes that the target lacks, made like the
        source's - its mappings, analysis and options - before any is
        filled (`_made_like`): written to, an index not there is made by
        the first document with the types the document's values suggest.
        Returns the names it made."""
        there = set(self.neutral_tables("dst", db))
        made = []
        for t in (tables if tables is not None
                  else self.neutral_tables("src", db)):
            if str(t).lower() not in there:
                self._made_like(db, t)
                made.append(t)
                if log:
                    log(f"{t}: not on the target, made like the source's")
        return made

    def _copy_by_scroll(self, db, t, slices):
        """One index by a sliced scroll of the source, each document
        written with the source's own version of it; the documents
        copied."""
        from concurrent.futures import ThreadPoolExecutor
        src, dst = self._index("src", db, t), self._index("dst", db, t)

        page = self._page("src", db, t, 1000)

        def one(i):
            body = {"size": page, "sort": ["_doc"],
                    "seq_no_primary_term": True}
            if slices > 1:
                body["slice"] = {"id": i, "max": slices}
            moved = 0
            for hits in self._scroll("src", src, body):
                self._bulk("dst", (self._versioned(h, dst) for h in hits))
                moved += len(hits)
            return moved
        with ThreadPoolExecutor(max_workers=slices) as pool:
            return sum(pool.map(one, range(slices)))

    def _strays(self, db, table, deletes=True, inserts=True):
        """(deleted, refused): the target's documents of one index the
        source does not hold, deleted; the source's the target lacks,
        written with their versions - and how many of those the target
        still refused. A document deleted on the target keeps its delete's
        version for a minute (`index.gc_deletes`), and until then refuses
        the source's version of it. Both are found by a search of their ids
        rather than asked by id: a document written with a routing of its
        own lives in the shard its routing names, where a lookup by id
        alone does not look.

        A kind the hop does not carry for the table (`Hop.carries`) is
        left: `deletes` False leaves the target's extra documents, `inserts`
        False leaves the source's the target lacks."""
        self._refresh("dst", db, table)
        there = self._ids("dst", db, table)
        held = self._ids("src", db, table)
        gone = sorted(there - held) if deletes else []
        dst = self._index("dst", db, table)
        from .. import freeze
        for at in range(0, len(gone), 500):
            got = self._call("dst", "POST", f"/{dst}/_delete_by_query"
                                            "?refresh=true&conflicts=proceed",
                             {"query": {"ids": {"values":
                                                gone[at:at + 500]}}})
            # a delete each, numbered: migkit's own
            freeze.counted(self.hop, {dst: int((got or {}).get("deleted",
                                                               0))})
        lacking = sorted(held - there) if inserts else []
        src = self._index("src", db, table)
        refused = 0
        for at in range(0, len(lacking), 500):
            got = self._call("src", "POST", f"/{src}/_search",
                             {"size": 500, "seq_no_primary_term": True,
                              "query": {"ids": {
                                  "values": lacking[at:at + 500]}}})
            refused += self._bulk("dst", (
                self._versioned(h, dst)
                for h in (got.get("hits") or {}).get("hits", [])))[1]
        return len(gone), refused

    def _made_like(self, db, table):
        """The target index made with the source's mappings and analysis,
        its own identity left out."""
        src = self._index("src", db, table)
        got = (self._call("src", "GET", f"/{src}").get(src) or {})
        settings = ((got.get("settings") or {}).get("index") or {})
        keep = {k: v for k, v in settings.items()
                if k in ("number_of_shards", "number_of_replicas",
                         "analysis", "mapping", "max_result_window",
                         "refresh_interval", "codec", "sort")}
        self._call("dst", "PUT", f"/{self._index('dst', db, table)}",
                   {"settings": {"index": keep},
                    "mappings": got.get("mappings") or {}})

    # ---- following the source by its shards' sequence numbers ----

    #: seconds between two looks at the source once caught up
    FOLLOW_EVERY = 1
    CHANGE_POINT_READS_ONLY = True

    def _copies(self, side, index):
        """(the index's id, {primary shard: [global checkpoint, highest
        sequence number]}, {copy of a shard: times it has made writes
        visible to a search}). Every write to a shard takes its next
        sequence number, a delete too; up to the global checkpoint every
        copy of the shard holds it."""
        got = self._call(side, "GET", f"/{index}/_stats/refresh"
                                      "?level=shards")
        mine = (got.get("indices") or {}).get(index) or {}
        marks, seen = {}, {}
        for n, copies in (mine.get("shards") or {}).items():
            for c in copies:
                routing = c.get("routing") or {}
                refresh = c.get("refresh") or {}
                seen[f"{n}/{routing.get('node')}/{routing.get('primary')}"] \
                    = int(refresh.get("external_total",
                                      refresh.get("total", 0)))
                if routing.get("primary"):
                    seq = c.get("seq_no") or {}
                    marks[str(n)] = [int(seq.get("global_checkpoint", -1)),
                                     int(seq.get("max_seq_no", -1))]
        return str(mine.get("uuid") or ""), marks, seen

    def _refresh_every(self, side, index):
        """Seconds between the index's refreshes, None where it refreshes
        only when asked (`-1`)."""
        got = self._call(side, "GET", f"/{index}/_settings?include_defaults"
                                      "=true&flat_settings=true")
        mine = got.get(index) or {}
        text = str((mine.get("settings") or {}).get("index.refresh_interval")
                   or (mine.get("defaults") or {}).get(
                       "index.refresh_interval") or "1s")
        if text.startswith("-"):
            return None
        for unit, scale in (("ms", 0.001), ("s", 1), ("m", 60), ("h", 3600)):
            if text.endswith(unit) and text[:-len(unit)].replace(
                    ".", "", 1).isdigit():
                return float(text[:-len(unit)]) * scale
        return 1.0

    def _visible(self, side, waits):
        """Wait until every write up to the checkpoints read with each
        index's `before` is within a search's reach - {index: (before,
        seconds between its refreshes)}, all waited on together. A copy of
        a shard that has made writes visible twice since - the second time
        begun after the first ended, so after `before` was read - has them.
        One that has not by twice its interval either has nothing left to
        make visible (a copy refreshes on its interval whenever it has), or
        has gone unsearched a while and makes writes visible only when
        searched - and a search waits for it to (measured on 2.19: after
        35 s without a search, a shard had not made a new write visible 3 s
        later; a count on it made it visible at once). So the index is
        searched then, and its wait is over."""
        import time
        began, left = time.monotonic(), dict(waits)
        while left:
            for index, (before, every) in list(left.items()):
                _, _, now = self._copies(side, index)
                if all(now.get(c, 0) >= n + 2 for c, n in before.items()):
                    del left[index]
                elif time.monotonic() - began > 2 * every:
                    self._call(side, "POST", f"/{index}/_count",
                               {"query": {"match_all": {}}})
                    del left[index]
            if left:
                time.sleep(0.2)

    @staticmethod
    def _never_refreshes(index):
        return (f"{index} on the source refreshes only when asked, so what"
                " it takes is seen by no search until then - a copy and a"
                " follow cannot see it either. Give it a refresh interval,"
                " or stop writes and move it")

    def stream_identity(self, side, db):
        return {"cluster": str(self._call(side, "GET", "/")
                               .get("cluster_uuid") or "")}

    def change_point(self, side, db):
        """Now, in each index's shards: the global checkpoint of each,
        taken once every copy has made the writes up to it visible - so a
        copy read after it holds everything up to it, and a follow begun
        from it misses nothing between. It reads only, and waits."""
        out, waits = {}, {}
        for t in self.neutral_tables(side, db):
            index = self._index(side, db, t)
            uuid, marks, seen = self._copies(side, index)
            every = self._refresh_every(side, index)
            if every is None:
                raise SystemExit(self._never_refreshes(index))
            waits[index] = (seen, every)
            out[t] = {"uuid": uuid,
                      "shards": {s: m[0] for s, m in marks.items()}}
        self._visible(side, waits)
        return {"indexes": out}

    def log_position(self, side, db):
        """Each index's shards' highest sequence numbers now: every write
        acknowledged so far is at or under them."""
        out = {}
        for t in self.neutral_tables(side, db):
            uuid, marks, _ = self._copies(side, self._index(side, db, t))
            out[t] = {"uuid": uuid,
                      "shards": {s: m[1] for s, m in marks.items()}}
        return {"indexes": out}

    @staticmethod
    def position_reached(have, want):
        """Whether the follow at `have` has carried, and checked for
        deletes, every write each shard of `want` holds."""
        if not have or not want:
            return None
        for t, w in (want.get("indexes") or {}).items():
            h = (have.get("indexes") or {}).get(t)
            if not h or h.get("uuid") != w.get("uuid"):
                return False
            if any(int((h.get("shards") or {}).get(s, -1)) < int(n)
                   for s, n in (w.get("shards") or {}).items()):
                return False
        return True

    def tail_start(self, db, token_path):
        """The position the follow begins at, taken before the copy it
        follows (`change_point`)."""
        from .. import tailctl
        if token_path.exists():
            return False
        tailctl.begin(self, db, token_path, self.change_point("src", db))
        return True

    def tail_seed(self, db, token_path, point):
        from .. import tailctl
        tailctl.begin(self, db, token_path, point)

    def _follow_index(self, db, table, was, log):
        """(the index's position after one look, documents carried, whether
        it settled) - every write each shard took since the position, read
        as the documents it left and written with their versions; then,
        since a delete leaves nothing to find by its number, the two sides
        counted, and where they differ, their ids compared and the
        target's extra taken away (`_strays`).

        The counts are of both sides as they stand: a delete in this look
        and a document written just after the checkpoint was read can
        cancel out in them for one look, and the next look, whose counts
        differ, takes the document away. Once writes stop, the counts are
        exact - which is when a fence decides a cutover."""
        src, dst = self._index("src", db, table), self._index("dst", db,
                                                              table)
        uuid, marks, seen = self._copies("src", src)
        if was and was.get("uuid") != uuid:
            raise SystemExit(
                f"{src} on the source was made again since the saved"
                " position (another index id): what the position counted is"
                " not in it. Move it again with --mode full+cdc")
        done = {s: int((was or {}).get("shards", {}).get(s, -1))
                for s in marks}
        upto = {s: m[0] for s, m in marks.items()}
        if was and all(upto[s] <= done[s] for s in upto):
            return was, 0, True
        every = self._refresh_every("src", src)
        if every is None:
            raise SystemExit(self._never_refreshes(src))
        self._visible("src", {src: (seen, every)})
        if table.lower() not in self.neutral_tables("dst", db):
            self._made_like(db, table)
        wrote = 0
        page = self._page("src", db, table, 1000)
        # only the changes the hop carries for this table (`mapping.ops`):
        # a document the target holds is an update of it, one it lacks an
        # insert, and a delete is found by the counts below
        carried = self.hop.carries(db, table)
        for s, top in sorted(upto.items()):
            if top <= done[s]:
                continue
            for hits in self._scroll(
                    "src", src, {"size": page, "sort": ["_doc"],
                                 "seq_no_primary_term": True,
                                 "query": {"range": {"_seq_no": {
                                     "gt": done[s], "lte": top}}}},
                    f"preference=_shards:{s}"):
                hits = self._carried_hits(dst, hits, carried)
                wrote += self._bulk("dst", (self._versioned(h, dst)
                                            for h in hits))[0]
        self._refresh("dst", db, table)
        count = [int(self._call(side, "GET", f"/{index}/_count")
                     .get("count", 0)) for side, index in (("src", src),
                                                           ("dst", dst))]
        deletes, inserts = "delete" in carried, "insert" in carried
        # the counts say where a delete is only when every insert is
        # carried: with inserts left out the target is short of the source
        # by the ones it was not given, and a delete can hide in that
        # difference - so the ids are compared at every look
        differ = count[0] != count[1] or (deletes and not inserts)
        gone, refused = (self._strays(db, table, deletes, inserts)
                         if differ and (deletes or inserts) else (0, 0))
        if refused:
            # looked at again from the same position, once the target
            # takes the source's version of them
            return was, wrote + gone, False
        return {"uuid": uuid, "shards": upto}, wrote + gone, True

    def _carried_hits(self, dst, hits, carried):
        """`hits` of the kinds of change the hop carries (`carried`, the
        hop's `mapping.ops` for the table): a document the target holds
        is an update, one it lacks an insert. All of them where both are
        carried, as they were."""
        if {"insert", "update"} <= carried:
            return hits
        if not carried & {"insert", "update"}:
            return []
        have = set()
        ids = [h["_id"] for h in hits]
        for at in range(0, len(ids), 500):
            got = self._call("dst", "POST", f"/{dst}/_mget",
                             {"ids": ids[at:at + 500]})
            have.update(d["_id"] for d in got.get("docs", [])
                        if d.get("found"))
        kind = {i: "update" if i in have else "insert" for i in ids}
        return [h for h in hits if kind[h["_id"]] in carried]

    def tail_apply(self, db, go, token_path, log):
        """Keep the target following the source by each primary shard's
        sequence numbers: each look, the documents written since the saved
        position are read from the shard that took them and written with
        the source's version (`_versioned`), so a look repeated, or a
        change carried after a newer one, is left by the target; deletes
        are found by the counts (`_follow_index`). The position is saved
        after each look; the fence waits on it (`position_reached`)."""
        from .. import tailctl

        def look(token):
            indexes = dict((token or {}).get("indexes") or {})
            went, settled = 0, True
            for t in self.neutral_tables("src", db):
                indexes[t], n, ok = self._follow_index(db, t,
                                                       indexes.get(t), log)
                went += n
                settled = settled and ok
            return {"indexes": indexes}, went, settled

        def behind(token):
            now = self.log_position("src", db)["indexes"]
            have = (token or {}).get("indexes") or {}
            ahead = {t: sum(max(0, int(n) - int((have.get(t) or {}).get(
                "shards", {}).get(s, -1))) for s, n in w["shards"].items())
                for t, w in now.items()}
            return (f"{sum(ahead.values()):,} writes on the source past the"
                    " saved position" + "".join(
                        f"; {t} {n:,}" for t, n in sorted(ahead.items())
                        if n))
        tailctl.follow(self, db, go, token_path, log, "documents", look,
                       behind, self.FOLLOW_EVERY)

    # the fence and the confirm are the pair's, through the position above

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)
