"""Azure Cosmos DB for NoSQL as a side of a pair (backlog ER5), through its
Python SDK. (Its MongoDB and Cassandra APIs are those engines' sides.)

A hop's database is a Cosmos database, and a table a container. An item
is a row: its top-level fields as columns, without what Cosmos DB adds
of its own (`_rid`, `_self`, `_etag`, `_attachments`, `_ts`). A container
migkit did not make is described by the JSON kinds its items hold, read
from all of them, and keyed by its partition key's fields and `id` - an
item is one `id` within one partition key value. One migkit made keeps
each column's class and its key's columns beside the endpoint on this
machine. A key holding a column `id` is kept as Cosmos DB keeps one: that
column the item's id, the rest of the key its partition key - so a
container copied to another container comes back as it was. Any other key
is the item's id (a key of several columns, their values as one JSON
array), partitioned by `/id` for a key of one column and by the first key
column's field for a key of more, so the rows of one value of it are
written together in transactional batches of 100; `/ \\ ? #` - which an
id may not hold - are written `%2F %5C %3F %23`.

Rows are read one feed range at a time, the ranges side by side for a
digest - so a container is verified by its partition key ranges, each
range read once - and written by upsert, a throttled request (429) waited
out as the service says (the SDK's own retry); a read may be asked at low
priority where the account has priority-based execution
(`priority: low`), so a move yields to the application first.

Its changes are its change feed, read by feed range by the change-topic
reader (`changetopic`), each range's continuation the position.
All-versions-and-deletes mode is used where the account has it
(continuous backup and the feature on): every create, replace and delete,
TTL expiries included, from the moment the tail began - it cannot start
earlier, which a tail begun before its copy never needs. Latest-version
mode loses every delete: a follow in it is refused unless the hop says
`change_feed: latest`, and then every delete is found by check instead.
The fence is each range's own position: read to its end, its continuation
at or past the one a "now" read gives.
"""
import base64
import concurrent.futures
import hashlib
import json

from .base import Engine, NeutralCopier, Result, clauses

#: operations in one transactional batch, as the service allows
BATCH_OPS = 100
#: items written side by side where each has a partition key of its own
WRITERS = 8
KEY = "id"
AVAD, LATEST = "AllVersionsAndDeletes", "LatestVersion"
ESCAPES = (("%", "%25"), ("/", "%2F"), ("\\", "%5C"), ("?", "%3F"),
           ("#", "%23"))


class CosmosLanes:
    """A container's feed ranges, each read by its continuation. A range
    that splits goes on through its continuation, which the SDK carries
    across the split."""
    KIND = "cosmos"

    def __init__(self, container, mode, priority=None):
        self.container, self.mode, self.priority = container, mode, priority
        self._ranges = None

    def _all(self):
        if self._ranges is None:
            self._ranges = {json.dumps(r, sort_keys=True): r
                            for r in self.container.read_feed_ranges()}
        return self._ranges

    def lanes(self):
        return sorted(self._all())

    def _kw(self):
        return {"priority": self.priority} if self.priority else {}

    def _last(self):
        return self.container.client_connection.last_response_headers.get(
            "etag")

    def ends(self):
        """Every range's continuation from now."""
        out = {}
        for lane, rng in self._all().items():
            for _ in self.container.query_items_change_feed(
                    feed_range=rng, start_time="Now", mode=self.mode,
                    **self._kw()):
                pass
            out[lane] = self._last()
        return out

    def read(self, at, most, seconds):
        import time
        out, nxt, caught = [], dict(at), True
        stop = time.monotonic() + seconds
        name = self.container.id
        for lane, rng in self._all().items():
            cont = at.get(lane)
            while len(out) < most and time.monotonic() < stop:
                kw = ({"continuation": cont} if cont else
                      {"feed_range": rng, "start_time": "Now",
                       "mode": self.mode})
                pager = self.container.query_items_change_feed(
                    max_item_count=min(1000, most - len(out)), **kw,
                    **self._kw()).by_page()
                page = next(pager, None)
                items = list(page) if page is not None else []
                after = self._last()
                for item in items:
                    out.append((lane, cont, None, item, name))
                if after:
                    nxt[lane] = cont = after
                if not items:
                    break
            else:
                caught = False
        return out, nxt, caught

    def lost(self, at):
        return None

    def room(self, at):
        return None

    def close(self):
        return None


def lsns(continuation):
    """The positions a continuation holds, one a range it spans: a plain
    etag, or the SDK's own token (base64 JSON with a `token` a range)."""
    if not continuation:
        return []
    text = str(continuation).strip()
    if text.strip('"').isdigit():
        return [int(text.strip('"'))]
    try:
        body = json.loads(base64.b64decode(text))
    except ValueError:
        return []
    out = []

    def walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "token" and isinstance(v, str) and \
                        v.strip('"').isdigit():
                    out.append(int(v.strip('"')))
                else:
                    walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)
    walk(body)
    return out


class CosmosEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "cosmosdb"
    FOLDED_BECAUSE = "has no way to hash items in the service"
    OVER_NETWORK = True
    RESUMES_BY_KEY = False
    CHANGE_POINT_READS_ONLY = True
    READS_AHEAD = False

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _client(self, side):
        held = self.__dict__.setdefault("_clients", {})
        if side not in held:
            from azure.cosmos import CosmosClient
            ep = self._ep(side)
            url = ep.options.get("url") or (
                f"{'http' if ep.options.get('tls') is False else 'https'}"
                f"://{ep.host}:{ep.port or 443}/")
            credential = ep.password
            if not credential:
                from azure.identity import DefaultAzureCredential
                credential = DefaultAzureCredential()
            held[side] = CosmosClient(
                url, credential=credential,
                connection_verify=not ep.options.get("tls_insecure"))
        return held[side]

    def _database(self, side, db):
        return self._client(side).get_database_client(self._d(side, db))

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    def _container(self, side, db, table):
        return self._database(side, db).get_container_client(table)

    def _priority(self, side):
        got = str(self._ep(side).options.get("priority") or "").lower()
        return {"low": "Low", "high": "High"}.get(got)

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        return sorted(d["id"] for d in self._client("src").list_databases())

    # ---- what a container is ---------------------------------------------

    def scope_tables(self, side, db):
        """Every container of the database, those the hop leaves out too:
        what its include and exclude entries may name."""
        return sorted(c["id"] for c in self._database(side, db)
                      .list_containers())

    def neutral_tables(self, side, db):
        return [t for t in self.scope_tables(side, db)
                if self.in_scope(db, t)]

    def _pk_fields(self, side, db, table):
        """The fields a container's partition key is read from - a top-level
        field each; a path into a nested object is refused by name."""
        props = self._container(side, db, table).read()
        paths = (props.get("partitionKey") or {}).get("paths") or []
        out = []
        for p in paths:
            parts = [x for x in str(p).split("/") if x]
            if len(parts) != 1:
                raise SystemExit(
                    f"{table} is partitioned by {p}, a field inside an"
                    " object, which no other side has a column for")
            out.append(parts[0])
        return out

    def _kept_here(self, side):
        from pathlib import Path

        from .. import config
        ep = self._ep(side)
        where = str(ep.options.get("url") or f"{ep.host}:{ep.port}")
        d = Path(config.REPORTS) / "_cosmosdb"
        d.mkdir(parents=True, exist_ok=True)
        return d / (hashlib.sha1(where.encode(), usedforsecurity=False)
                    .hexdigest()[:16] + ".json")

    def _described(self, side, db, table):
        try:
            got = json.loads(self._kept_here(side).read_text()).get(
                f"{self._d(side, db)}.{table}")
        except (OSError, ValueError):
            got = None
        if got:
            return [tuple(c) for c in got["columns"]], list(got["key"])
        return None

    def _items(self, side, db, table, rng=None):
        from ..changetopic import COSMOS_SYSTEM
        kw = {"feed_range": rng} if rng is not None else {
            "enable_cross_partition_query": True}
        prio = self._priority(side)
        if prio:
            kw["priority"] = prio
        for item in self._container(side, db, table).query_items(
                "SELECT * FROM c", max_item_count=1000, **kw):
            yield {k: v for k, v in item.items() if k not in COSMOS_SYSTEM}

    def neutral_columns(self, side, db, table):
        described = self._described(side, db, table)
        if described is not None:
            return described[0]
        from .couchbase import document_columns
        return document_columns(self, (side, db, table),
                                lambda: self._items(side, db, table))

    def neutral_key(self, side, db, table):
        described = self._described(side, db, table)
        if described is not None:
            return described[1]
        return [f for f in self._pk_fields(side, db, table)
                if f != KEY] + [KEY]

    def _classes(self, side, db, table):
        from .. import canon
        return {n: canon.type_class("cosmosdb", t) or "json"
                for n, t in self.neutral_columns(side, db, table)}

    # ---- values --------------------------------------------------------------

    @staticmethod
    def _escape(text):
        for a, b in ESCAPES:
            text = text.replace(a, b)
        return text

    @staticmethod
    def _unescape(text):
        for a, b in reversed(ESCAPES):
            text = text.replace(b, a)
        return text

    @staticmethod
    def _text(cls, value):
        from .. import canon
        return value if isinstance(value, str) else \
            canon.render_value(cls or "text", value)

    def _item_id(self, key, classes, values):
        """The id of a row's item: its `id` column's value where its key
        holds one - Cosmos DB's own model, the rest of the key its
        partition key - else the key's value, or several as a JSON
        array."""
        if KEY in key:
            return self._escape(self._text(classes.get(KEY), values[KEY]))
        texts = [self._text(classes.get(k), values[k]) for k in key]
        return self._escape(texts[0] if len(texts) == 1
                            else json.dumps(texts))

    def _key_of_id(self, item_id, key, classes, fields=None):
        """A row's key back from its item's id (`_item_id`), and from
        `fields` - the item, or a delete's partition key - for the rest
        of a key that holds `id`."""
        from .. import changetopic

        def typed(k, t):
            return changetopic.from_text(classes.get(k) or "text", t,
                                         "base64")
        text = self._unescape(item_id)
        if KEY in key:
            return {k: typed(k, text) if k == KEY else
                    self._from_json(classes.get(k), (fields or {}).get(k))
                    for k in key}
        texts = [text] if len(key) == 1 else json.loads(text)
        return {k: typed(k, t) for k, t in zip(key, texts)}

    @staticmethod
    def _from_json(cls, value):
        from .couchbase import CouchbaseEngine
        return CouchbaseEngine._from_json(cls, value)

    def _row(self, names, classes, item, described):
        """An item as a row: in a container migkit made, the id read back
        as its column's value, or left out where it was made from the
        key."""
        from .couchbase import CouchbaseEngine
        if described and KEY not in names:
            item = {n: v for n, v in item.items() if n != KEY}
        out = []
        for n in names:
            v = item.get(n)
            if described and n == KEY and v is not None:
                v = self._key_of_id(v, [KEY], classes)[KEY]
            out.append(CouchbaseEngine._from_json(classes.get(n), v))
        return out

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        if where:
            raise SystemExit(f"{table} moves under a row filter, which is"
                             " SQL, and a container is read whole")
        classes = self._classes(side, db, table)
        described = self._described(side, db, table)
        names = [n for n, _ in columns]
        container = self._container(side, db, table)
        from .. import pieces
        # an item is 2 MB at most: handed on in batches of `size` items or
        # `pieces.BATCH_BYTES`, whichever is reached first
        yield from pieces.by_bytes(
            (self._row(names, classes, item, described)
             for rng in container.read_feed_ranges()
             for item in self._items(side, db, table, rng)),
            most_rows=int(size))

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        rows = []
        for batch in self.neutral_batches(side, db, table, columns,
                                          limit or 1000, where):
            rows += batch
            if limit and len(rows) >= limit:
                return rows[:limit], None
        return rows, None

    def neutral_digest(self, side, db, table, columns, where=None):
        """Folded one feed range at a time, the ranges side by side: the
        digest is a sum, whichever range a row came from."""
        from .. import canon
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a"
                             " container takes none")
        classes = [c for _, c in columns]
        names = [n for n, _ in columns]
        by = self._classes(side, db, table)
        described = self._described(side, db, table)
        ranges = list(self._container(side, db, table).read_feed_ranges())

        def one(rng):
            total, n, batch = 0, 0, []
            for item in self._items(side, db, table, rng):
                batch.append(self._row(names, by, item, described))
                if len(batch) >= 5000:
                    k, total = canon.fold_rows(classes, batch, total)
                    n, batch = n + k, []
            if batch:
                k, total = canon.fold_rows(classes, batch, total)
                n += k
            return n, total
        n, total = 0, 0
        with concurrent.futures.ThreadPoolExecutor(
                max(1, min(8, len(ranges)))) as pool:
            for k, t in pool.map(one, ranges):
                n, total = n + k, total + t
        return (n, str(total))

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        if not key or not keys:
            return {}
        classes = self._classes(side, db, table)
        described = self._described(side, db, table)
        names = [n for n, _ in columns]
        pk = self._pk_fields(side, db, table)
        container = self._container(side, db, table)
        found = []
        from azure.cosmos.exceptions import CosmosResourceNotFoundError
        for k in keys:
            values = dict(zip(key, k))
            item_id = (self._item_id(key, classes, values) if described
                       else str(values[KEY]))
            part = [item_id if f == KEY and described else
                    self._to_json(classes.get(f), values.get(f))
                    for f in pk]
            try:
                item = container.read_item(item_id, part[0] if len(part) == 1
                                           else part)
            except CosmosResourceNotFoundError:
                continue
            found.append(self._row(names, classes, item, described))
        return self._by_key_map(columns, key, found)

    @staticmethod
    def _to_json(cls, value):
        from .couchbase import CouchbaseEngine
        return CouchbaseEngine._to_json(cls, value)

    def table_facts(self, side, db):
        return {t: {"rows": None, "bytes": None, "key": True}
                for t in self.neutral_tables(side, db)}

    def row_sizes(self, side, db, tables=None):
        """Each container's items sized as the service holds them - the
        bytes of the item's JSON, its own fields only: the first
        `SIZE_SAMPLE` items of a container, or every one where it holds no
        more, and `exact` only then (an item is 2 MB at most, which the
        copy's memory is set from where this is not exact)."""
        from .base import sizes_of
        out = {}
        for t in (tables if tables is not None
                  else self.neutral_tables(side, db)):
            lengths, whole = [], True
            try:
                for item in self._items(side, db, t):
                    lengths.append(len(json.dumps(
                        item, separators=(",", ":"),
                        ensure_ascii=False).encode()))
                    if len(lengths) >= self.SIZE_SAMPLE:
                        whole = False
                        break
            except Exception:  # noqa: BLE001 - that container is not sized
                continue
            if lengths:
                out[t] = sizes_of(lengths, exact=whole)
        return out

    # ---- indexes: composite ones, and the unique keys a container is made with

    def neutral_indexes(self, side, db, table):
        """[(name, unique, [fields], plain)]: the container's composite
        indexes and its unique keys. A path inside an object is not a
        field another side has, and is not plain."""
        props = self._container(side, db, table).read()
        out = []
        for i, group in enumerate(self._indexing(props)["composite"]):
            paths = [c["path"] for c in json.loads(group)]
            out.append((f"composite_{i + 1}", False,
                        [p.strip("/") for p in paths],
                        all(p.count("/") == 1 for p in paths)))
        for i, key in enumerate((props.get("uniqueKeyPolicy") or {})
                                .get("uniqueKeys") or []):
            paths = key.get("paths") or []
            out.append((f"unique_{i + 1}", True,
                        [p.strip("/") for p in paths],
                        all(p.count("/") == 1 for p in paths)))
        return out

    def neutral_create_index_sql(self, side, db, table, name, unique,
                                 columns):
        return json.dumps({"leaf": table, "index": name,
                           "unique": bool(unique), "columns": list(columns)})

    def execute_ddl(self, side, db, sql):
        """A composite index made by replacing the container's indexing
        policy with one that holds it (a single field is in the default
        policy already). A unique key is refused: it is set when the
        container is made and cannot be added to one."""
        self._target_only(side, "change a schema")
        spec = json.loads(sql)
        if spec["unique"]:
            raise RuntimeError(f"{spec['index']} is a unique key, which a"
                               " container takes only when it is made")
        from azure.cosmos import PartitionKey
        container = self._container(side, db, spec["leaf"])
        props = container.read()
        policy = dict(props.get("indexingPolicy") or {})
        fields = [f"/{c}" for c in spec["columns"]]
        if len(fields) == 1:
            have = [p.get("path") for p in policy.get("includedPaths") or []]
            if policy.get("indexingMode") == "none" or "/*" in have or \
                    f"{fields[0]}/?" in have or f"{fields[0]}/*" in have:
                return
            policy["includedPaths"] = list(policy.get("includedPaths")
                                           or []) + [{"path": fields[0]
                                                      + "/?"}]
        else:
            want = [{"path": f, "order": "ascending"} for f in fields]
            composite = list(policy.get("compositeIndexes") or [])
            if want in composite:
                return
            policy["compositeIndexes"] = composite + [want]
        key = props["partitionKey"]
        paths = key.get("paths") or []
        self._database(side, db).replace_container(
            container, partition_key=PartitionKey(
                path=paths[0] if len(paths) == 1 else paths,
                kind=key.get("kind") or "Hash"),
            indexing_policy=policy, default_ttl=props.get("defaultTtl"),
            conflict_resolution_policy=props.get("conflictResolutionPolicy"))

    # ---- the assessment ---------------------------------------------------------

    PREFLIGHT = ("_follow_readiness",)

    def _follow_readiness(self, db):
        """Whether the account serves the change feed the hop follows by:
        found before the copy, not at the first read of the tail."""
        said = str(self.hop.source.options.get("change_feed") or "").lower()
        if said in ("latest", "latest_version"):
            return Result(
                "deep", f"{db} change feed", "warn",
                "the hop follows the latest-version change feed, which "
                "carries no delete and no expiry by time to live: each is "
                "found by check, not followed", "",
                "turn on continuous backup and the all-versions-and-deletes "
                "feature to follow deletes")
        bad = []
        for t in self.neutral_tables("src", db):
            try:
                self._mode("src", db, self._container("src", db, t))
            except SystemExit as e:
                bad.append(f"{t}: {str(e)[:100]}")
                break
        if bad:
            return Result("deep", f"{db} change feed", "warn", bad[0], "",
                          "turn on continuous backup and the "
                          "all-versions-and-deletes feature, or say "
                          "change_feed: latest in the source's options")
        return Result("deep", f"{db} change feed", "ok",
                      "the account serves the all-versions-and-deletes "
                      "change feed: creates, replaces, deletes and expiries "
                      "are followed")

    #: the policy a container has when nothing was set
    DEFAULT_INDEXING = {"mode": "consistent", "included": ["/*"],
                        "excluded": [], "composite": [], "spatial": [],
                        "vector": []}

    def _handwork(self):
        """What the move leaves for a person, counted and named
        (`handwork`): what a container is made with and a copy of its items
        does not give the one on the target."""
        from .. import handwork
        inv = handwork.Inventory()
        for db in self.databases():
            try:
                tables = self.neutral_tables("src", db)
                props = {t: self._container("src", db, t).read()
                         for t in tables}
            except Exception as e:  # noqa: BLE001 - unknown, not none
                inv.unknown("not-carried", db, "cannot read the containers:"
                            f" {str(e).splitlines()[0][:90]}")
                continue
            inv.add("target-prereq", db, "unique keys, set only when a"
                    " container is made",
                    [t for t in tables if (props[t].get("uniqueKeyPolicy")
                                           or {}).get("uniqueKeys")])
            inv.add("not-carried", db, "default time to live, which expires"
                    " items",
                    [t for t in tables
                     if props[t].get("defaultTtl") is not None])
            inv.add("not-carried", db, "indexing policies other than the"
                    " default",
                    [t for t in tables if self._indexing(props[t])
                     != self.DEFAULT_INDEXING])
            inv.add("not-carried", db, "computed properties",
                    [t for t in tables if props[t].get("computedProperties")])
            try:
                for t in tables:
                    got = self._scripts("src", db, t)
                    for kind in ("procedures", "functions", "triggers"):
                        inv.add("server-side-code", db, f"{kind} ",
                                [f"{t}.{n}" for n in got[kind]
                                 if self.takes(db, kind, f"{t}.{n}", by=t)])
            except Exception as e:  # noqa: BLE001 - unknown, not none
                inv.unknown("server-side-code", db, "cannot list the"
                            " scripts: " + str(e).splitlines()[0][:90])
        return inv

    def capacity(self, side, db):
        """The request units per second the database's containers provision
        (`request_units_per_second`, the autoscale ceiling where a
        container scales) and how many provision any (`provisioned`): a
        container on shared or serverless throughput has none of its own
        and is left out."""
        total, n = 0, 0
        for t in self.neutral_tables(side, db):
            try:
                got = self._container(side, db, t).get_throughput()
            except Exception:  # noqa: BLE001 - none of its own
                continue
            ru = getattr(got, "offer_throughput", None) or getattr(
                got, "auto_scale_max_throughput", None)
            if ru:
                total, n = total + int(ru), n + 1
        return {"request_units_per_second": total, "provisioned": n} \
            if n else {}

    def stream_writers(self, db):
        """Besides migkit's own tail, what writes into the target's
        containers and cannot be paused: the service expiring items by a
        default time to live, and an account that takes writes in more than
        one region."""
        out = list(super().stream_writers(db))
        for t in self.neutral_tables("dst", db):
            ttl = self._container("dst", db, t).read().get("defaultTtl")
            if ttl is not None:
                out.append((f"the service expiring {t}'s items (default"
                            f" time to live {ttl})", False))
        acct = self._client("dst").get_database_account()
        if getattr(acct, "EnableMultipleWritableLocations", False):
            out.append(("other regions writing to the account (it takes"
                        " writes in more than one)", False))
        return out

    # ---- writing -------------------------------------------------------------

    @staticmethod
    def _declared_of(col):
        """A column's kind as the container's description keeps it: the
        source's own kind where both sides are Cosmos DB (`canon.OWN`), else
        its class, a decimal with its scale."""
        from .. import canon
        if col[1] == canon.OWN and col[2]:
            return str(col[2][0])
        return col[1] + (f"({col[2][0]},{col[2][1]})"
                         if col[1] == "decimal" and len(col[2] or ()) == 2
                         else "")

    def neutral_create_sql(self, side, db, table, columns, key=()):
        if not key:
            raise SystemExit(f"{table} has no key, and an item is kept by"
                             " its id: a table without one has no container"
                             " to go to")
        key = list(key)
        if KEY in [c[0] for c in columns] and KEY not in key:
            raise SystemExit(
                f"{table} has a column named id outside its key, and a"
                " Cosmos DB item's id is made from its key: rename the"
                " column with mapping.columns")
        cols = [[c[0], self._declared_of(c)] for c in columns]
        if KEY in key:
            # Cosmos DB's own model: the rest of the key its partition key
            pk = [f"/{k}" for k in key if k != KEY] or [f"/{KEY}"]
        else:
            pk = [f"/{KEY}"] if len(key) == 1 else [f"/{key[0]}"]
        return json.dumps({"container": table, "partition_key": pk,
                           "key": key, "columns": cols})

    def neutral_create(self, side, db, table, columns, key=()):
        self._target_only(side, "create a container")
        from azure.cosmos import PartitionKey
        if table in self.neutral_tables(side, db):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a container"
                             " that is already there")
        spec = json.loads(self.neutral_create_sql(side, db, table, columns,
                                                  key))
        paths = spec["partition_key"]
        self._database(side, db).create_container(
            id=table, partition_key=PartitionKey(
                path=paths[0] if len(paths) == 1 else paths,
                kind="Hash" if len(paths) == 1 else "MultiHash"))
        path = self._kept_here(side)
        try:
            kept = json.loads(path.read_text())
        except (OSError, ValueError):
            kept = {}
        kept[f"{self._d(side, db)}.{table}"] = {"columns": spec["columns"],
                                                "key": spec["key"]}
        path.write_text(json.dumps(kept, indent=1))
        return json.dumps(spec)

    def _bodies(self, side, db, table, names, rows):
        classes = self._classes(side, db, table)
        described = self._described(side, db, table)
        key = self.neutral_key(side, db, table)
        out = []
        for r in rows:
            values = dict(zip(names, r))
            body = {n: self._to_json(classes.get(n), v)
                    for n, v in values.items()}
            if described:
                body[KEY] = self._item_id(key, classes, values)
            else:
                body[KEY] = str(values[KEY])
            out.append(body)
        return out

    def _upsert(self, side, db, table, bodies):
        """Items of one partition key value in transactional batches of
        100; items with one each, side by side."""
        from .couchbase import forget_columns
        forget_columns(self)
        container = self._container(side, db, table)
        pk = self._pk_fields(side, db, table)
        groups = {}
        for b in bodies:
            value = tuple(json.dumps(b.get(f), sort_keys=True, default=str)
                          for f in pk)
            groups.setdefault(value, []).append(b)
        single = [g[0] for g in groups.values() if len(g) == 1]
        for got in groups.values():
            if len(got) < 2:
                continue
            part = [got[0].get(f) for f in pk]
            for i in range(0, len(got), BATCH_OPS):
                container.execute_item_batch(
                    [("upsert", (b,)) for b in got[i:i + BATCH_OPS]],
                    partition_key=part[0] if len(part) == 1 else part)
        if single:
            with concurrent.futures.ThreadPoolExecutor(WRITERS) as pool:
                list(pool.map(container.upsert_item, single))

    def neutral_write(self, side, db, table, columns, rows):
        """Each item upserted by its id and partition key, which replaces
        one already there: a batch written again lands on itself."""
        self._target_only(side, "write rows")
        if not rows:
            return 0
        self._upsert(side, db, table, self._bodies(
            side, db, table, [n for n, _ in columns], rows))
        return len(rows)

    def _apply_upserts(self, side, db, table, rows):
        bodies = []
        for key, values in rows:
            row = {**key, **values}
            bodies += self._bodies(side, db, table, list(row),
                                   [list(row.values())])
        self._upsert(side, db, table, bodies)

    def _apply_upsert(self, side, db, table, key, values):
        self._apply_upserts(side, db, table, [(key, values)])

    def _apply_deletes(self, side, db, table, keys):
        from azure.cosmos.exceptions import CosmosResourceNotFoundError

        from .couchbase import forget_columns
        classes = self._classes(side, db, table)
        forget_columns(self)
        described = self._described(side, db, table)
        key = self.neutral_key(side, db, table)
        pk = self._pk_fields(side, db, table)
        container = self._container(side, db, table)
        for k in keys:
            item_id = (self._item_id(key, classes, k) if described
                       else str(k[KEY]))
            part = [item_id if f == KEY and described else
                    self._to_json(classes.get(f), k.get(f)) for f in pk]
            try:
                container.delete_item(item_id, part[0] if len(part) == 1
                                      else part)
            except CosmosResourceNotFoundError:
                pass

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    def neutral_empty(self, side, db, table, where=None):
        from .couchbase import forget_columns
        self._target_only(side, "empty a container")
        forget_columns(self)
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a"
                             " container takes none")
        pk = self._pk_fields(side, db, table)
        container = self._container(side, db, table)
        gone = 0
        for item in list(self._items(side, db, table)):
            part = [item.get(f) for f in pk]
            container.delete_item(item[KEY], part[0] if len(part) == 1
                                  else part)
            gone += 1
        return gone

    # ---- the checks ----------------------------------------------------------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self.tallied(db, self._as_pair().check_data(db, table,
                                                           stream))

    def create_missing(self, db, log=None):
        return self._as_pair().create_missing(db, log)

    def finish_created(self, db, log=None):
        return self._as_pair().finish_created(db, log)

    def repair_plan(self, db, kind):
        return self._as_pair().repair_plan(db, kind)

    def apply(self, db, action):
        return self._as_pair().apply(db, action)

    #: what changes where an item goes or how long it stays: another
    #: partition key is another placement, another time to live expires
    #: other items, a missing unique key takes duplicates
    CRITICAL_PARAMS = ("partition key", "default ttl", "unique keys")

    def _settings(self, side, db):
        out = {}
        for t in self.neutral_tables(side, db):
            props = self._container(side, db, t).read()
            pk = props.get("partitionKey") or {}
            out[f"{t} partition key"] = ",".join(pk.get("paths") or [])
            out[f"{t} default ttl"] = str(props.get("defaultTtl"))
            out[f"{t} unique keys"] = json.dumps(sorted(
                ",".join(k.get("paths") or []) for k in
                (props.get("uniqueKeyPolicy") or {}).get("uniqueKeys", [])))
            out[f"{t} indexing"] = str((props.get("indexingPolicy") or {})
                                       .get("indexingMode"))
        return out

    def check_params(self, db):
        """Each container's partition key, time to live, unique keys and
        indexing mode, source against target."""
        try:
            src, dst = self._settings("src", db), self._settings("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("params", db, "error", "could not read the"
                           f" containers' settings: {str(e)[:90]}")]
        # a setting is named for its container: "orders partition key"
        crit = sorted(n for n in set(src) | set(dst)
                      if " ".join(n.split(" ")[-2:]) in self.CRITICAL_PARAMS)
        return self._param_result(db, src, dst, crit,
                                  "set the target container's settings as"
                                  " the source's")

    def check_deep(self, db):
        """Writes in conflict on either side (a multi-region write
        account's conflicts feed): each is an item whose regions disagree,
        and which of its versions a read gives is not settled."""
        out = []
        for side, who in (("src", "source"), ("dst", "target")):
            n = 0
            try:
                for t in self.neutral_tables(side, db):
                    n += sum(1 for _ in self._container(side, db, t)
                             .list_conflicts())
            except Exception as e:  # noqa: BLE001 - said, as the error
                out.append(Result("deep", f"{db} {who} conflicts", "error",
                                  f"could not read the conflicts feed:"
                                  f" {str(e).splitlines()[0][:90]}"))
                continue
            out.append(Result(
                "deep", f"{db} {who} conflicts", "diff" if n else "ok",
                f"{n} writes in conflict between regions" if n
                else "no write in conflict", "",
                "resolve them before the cutover" if n else ""))
        return out + self._deep_items(db)

    # ---- what the comparison of items does not see ----------------------------

    NULL_KIND = "null"
    ITEM_NOUN = "items"

    def _shape_of(self, side, db, table):
        """What a container's items hold, read item by item: how many,
        whether every one was read, and per field how many hold each kind
        of value and how many hold it empty - a JSON null, an empty value
        and an absent field are three things."""
        from collections import Counter

        from .couchbase import _kind
        kinds, empty, n, whole = {}, Counter(), 0, True
        for item in self._items(side, db, table):
            n += 1
            for name, value in item.items():
                kinds.setdefault(name, Counter())[_kind(value)] += 1
                if value in ("", [], {}):
                    empty[name] += 1
            if n >= self.DEEP_MOST:
                whole = False
                break
        return {"items": n, "whole": whole, "kinds": kinds, "empty": empty}

    def _deep_items(self, db):
        """A field that was null on the source and is absent on the target
        (or the other way), a field holding another kind of value, and the
        containers' indexing, options, partition keys, computed fields and
        scripts, both sides."""
        try:
            both = sorted(set(self.neutral_tables("src", db))
                          & set(self.neutral_tables("dst", db)))
            props = {side: {t: self._container(side, db, t).read()
                            for t in both} for side in ("src", "dst")}
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} item shapes", "error",
                           "could not read the containers: "
                           f"{str(e).splitlines()[0][:90]}")]
        return (self._item_shape_results(db, both)
                + self._property_results(db, both, props)
                + self._script_results(db, both))

    @staticmethod
    def _indexing(props):
        """A container's indexing policy as what it indexes: its mode, the
        paths included and excluded (the service's own `_etag` left out),
        and its composite, spatial and vector indexes."""
        pol = props.get("indexingPolicy") or {}

        def paths(key):
            return sorted(p["path"] for p in pol.get(key) or []
                          if p.get("path") and p["path"] != '/"_etag"/?')

        def each(key):
            return sorted(json.dumps(x, sort_keys=True)
                          for x in pol.get(key) or [])
        return {"mode": pol.get("indexingMode"),
                "included": paths("includedPaths"),
                "excluded": paths("excludedPaths"),
                "composite": each("compositeIndexes"),
                "spatial": each("spatialIndexes"),
                "vector": each("vectorIndexes")}

    #: the container properties that say how its items are kept or expire
    OPTIONS = ("defaultTtl", "analyticalStorageTtl", "conflictResolutionPolicy",
               "changeFeedPolicy", "geospatialConfig")

    def _property_results(self, db, both, props):
        """Indexes, options, partition keys and computed fields of the
        containers on both sides, from one reading of their properties."""
        src, dst = props["src"], props["dst"]
        out = []
        idx = []
        for t in both:
            a, b = self._indexing(src[t]), self._indexing(dst[t])
            idx += [f"{t}: {k} {json.dumps(a[k])} on the source,"
                    f" {json.dumps(b[k])} on the target"
                    for k in a if a[k] != b[k]]
        out.append(Result(
            "deep", f"{db} indexes", "diff" if idx else "ok",
            clauses(idx) if idx else
            f"{len(both)} containers, indexing policies equal", "",
            "give the target container the source's indexing policy" if idx
            else ""))
        odd, expires = [], []
        for t in both:
            for opt in self.OPTIONS:
                a, b = src[t].get(opt), dst[t].get(opt)
                if a != b:
                    (expires if opt == "defaultTtl" else odd).append(
                        f"{t}.{opt}: {json.dumps(a)} on the source,"
                        f" {json.dumps(b)} on the target")
        found = expires + odd
        out.append(Result(
            "deep", f"{db} container options",
            "diff" if expires else "warn" if found else "ok",
            clauses(found) if found else
            f"{len(both)} containers, options equal", "",
            "give the target the source's default time to live before"
            " cutover; it expires other items" if expires else "",
            category="structure.collection-options"))
        keys = [f"{t}: {json.dumps(src[t].get('partitionKey'))} on the"
                f" source, {json.dumps(dst[t].get('partitionKey'))} on the"
                " target" for t in both
                if {k: (src[t].get("partitionKey") or {}).get(k)
                    for k in ("paths", "kind")}
                != {k: (dst[t].get("partitionKey") or {}).get(k)
                    for k in ("paths", "kind")}]
        out.append(Result(
            "deep", f"{db} sharding", "diff" if keys else "ok",
            clauses(keys) if keys else
            f"{len(both)} containers, partition keys equal", "",
            "an item is placed by its partition key: make the target"
            " container with the source's" if keys else ""))
        comp = [f"{t}: {json.dumps(src[t].get('computedProperties'))} on the"
                f" source, {json.dumps(dst[t].get('computedProperties'))}"
                " on the target" for t in both
                if sorted(json.dumps(x, sort_keys=True) for x in
                          src[t].get("computedProperties") or [])
                != sorted(json.dumps(x, sort_keys=True) for x in
                          dst[t].get("computedProperties") or [])]
        out.append(Result(
            "deep", f"{db} generated", "diff" if comp else "ok",
            clauses(comp) if comp else
            f"{len(both)} containers, computed properties equal", "",
            "a computed property is a query the container answers with:"
            " define it on the target" if comp else ""))
        return out

    # ---- stored procedures, triggers and functions of a container ---------------

    def _scripts(self, side, db, table):
        """{"procedures": {id: body}, "functions": {id: body}, "triggers":
        {id: (type, operation, body)}} of one container, as its scripts
        collection lists them."""
        scripts = self._container(side, db, table).scripts
        return {
            "procedures": {p["id"]: p.get("body") for p in
                           scripts.list_stored_procedures()},
            "functions": {f["id"]: f.get("body") for f in
                          scripts.list_user_defined_functions()},
            "triggers": {t["id"]: (t.get("triggerType"),
                                   t.get("triggerOperation"), t.get("body"))
                         for t in scripts.list_triggers()}}

    def _script_results(self, db, both):
        """Two findings: the stored procedures and functions, and the
        triggers, each by name and body, of the containers the hop takes
        and the kinds of object it carries (`objects`)."""
        try:
            found = {side: {t: self._scripts(side, db, t) for t in both}
                     for side in ("src", "dst")}
        except Exception as e:  # noqa: BLE001 - said, as the error
            said = f"could not list the scripts: {str(e).splitlines()[0][:90]}"
            return [Result("deep", f"{db} objects", "error", said),
                    Result("deep", f"{db} triggers", "error", said)]
        out = []
        for scope, kinds in ((f"{db} objects", ("procedures", "functions")),
                             (f"{db} triggers", ("triggers",))):
            apart, n = [], 0
            for t in both:
                for kind in kinds:
                    have = {name: body for name, body in
                            found["src"][t][kind].items()
                            if self.takes(db, kind, f"{t}.{name}",
                                          by=t)}
                    got = found["dst"][t][kind]
                    n += len(have)
                    for name, body in sorted(have.items()):
                        if name not in got:
                            apart.append(f"{t}: {kind[:-1]} {name} is not on"
                                         " the target")
                        elif got[name] != body:
                            apart.append(f"{t}: {kind[:-1]} {name} differs on"
                                         " the target")
            if scope.endswith("objects"):
                out.append(Result(
                    "deep", f"{db} objects", "diff" if apart else "ok",
                    clauses(apart) if apart else
                    f"{n} stored procedures and functions, each on the target"
                    " as on the source", "",
                    "a script is not copied with its container: make it on"
                    " the target" if apart else ""))
            else:
                out.append(Result(
                    "deep", f"{db} triggers", "diff" if apart else "ok",
                    clauses(apart) if apart else
                    f"{n} triggers, each on the target as on the source", "",
                    "a trigger is not copied with its container: make it on"
                    " the target" if apart else ""))
        return out

    # ---- the change feed -------------------------------------------------------

    def _mode(self, side, db, container):
        """All versions and deletes where the account has it; latest
        version only where the hop says so (`change_feed: latest`)."""
        said = str(self._ep(side).options.get("change_feed") or "").lower()
        if said in ("latest", "latest_version"):
            return LATEST
        try:
            for _ in container.query_items_change_feed(
                    start_time="Now", mode=AVAD, max_item_count=1):
                break
            return AVAD
        except Exception as e:  # noqa: BLE001 - said, with the way out
            raise SystemExit(
                f"the account does not serve the all-versions-and-deletes"
                f" change feed ({str(e).splitlines()[0][:80]}), and the"
                " latest-version one carries no delete: turn on continuous"
                " backup and the all-versions-and-deletes feature, or say"
                " change_feed: latest in the source's options - every delete"
                " is then found by check instead of followed") from None

    def _follower(self, db):
        """The reader of every container in scope, as they were when it
        was made: a container made on the source since joins at the
        tail's next start."""
        from .. import changetopic
        held = self.__dict__.setdefault("_followers", {})
        at = db
        if at not in held:
            tables = self.neutral_tables("src", db)
            lanes = {}
            env = changetopic.CosmosFeed({t: [f for f in self._pk_fields(
                "src", db, t) if f != KEY] for t in tables})
            for t in tables:
                c = self._container("src", db, t)
                lanes[t] = CosmosLanes(c, self._mode("src", db, c),
                                       self._priority("src"))
            held[at] = changetopic.Follower(
                env, _Containers(lanes),
                name=lambda row: row.table,
                shape=lambda t, row, db=db: self._shape(db, t, row))
        return held[at]

    def _shape(self, db, table, row):
        """(key, values) of a feed item as `neutral_read` gives its row: a
        container migkit made keyed by its key's columns - read back from
        the id where a delete names nothing else - any other by its
        partition key's fields and id."""
        classes = self._classes("src", db, table)
        described = self._described("src", db, table)
        key = self.neutral_key("src", db, table)
        if row.values is None:
            ident = (self._key_of_id(row.key[KEY], key, classes, row.key)
                     if described else {k: row.key.get(k) for k in key})
            return ident, None
        names = [n for n, _ in self.neutral_columns("src", db, table)]
        values = dict(zip(names, self._row(names, classes, row.values,
                                           described)))
        return {k: values.get(k) for k in key}, values

    def change_point(self, side, db):
        return self._follower(db).point()

    def copy_point(self, side, db):
        return self.change_point(side, db)

    def log_position(self, side, db):
        """Every range's position now, as its continuation's."""
        f = self._follower(db)
        return {"at": f.lanes.ends(), "fence": None}

    @staticmethod
    def position_reached(have, want):
        """Each range read to or past where a "now" read put it."""
        if not have or not want:
            return None
        for lane, cont in (want.get("at") or {}).items():
            got = lsns((have.get("at") or {}).get(lane))
            need = lsns(cont)
            if not need:
                continue
            if not got or min(got) < max(need):
                return False
        return True

    #: a feed range's position is a continuation, read again from any
    #: container's feed as often as asked: a container that left a tail
    #: comes back with its changes since
    REREADS_FROM_A_POSITION = True

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each item the change feed says was created, replaced or
        deleted, in its range's order - and a container dropped, or made
        since the tail began (one the saved position has no range of), as
        the DDLs of the feed (`canon.ddl`): the tail follows the containers
        of the database as they are, the reader made again for them."""
        if token is None:
            return [], self.change_point(side, db)
        from .. import canon
        out = []
        held = self.__dict__.get("_followers", {}).get(db)
        if held is not None:
            known = set(held.lanes.by)
            listed = set(self.scope_tables(side, db))
            taken = set(self.neutral_tables(side, db))
            followed = {lane.partition("|")[0]
                        for lane in (token.get("at") or {})}
            out += [canon.ddl(t, "drop") for t in sorted(known - listed)]
            out += [canon.ddl(t, "create")
                    for t in sorted(taken - known - followed)]
            if known != taken:
                # a reader of the containers there are now; its place is
                # the position's, which every range of the old one is in
                self.release_changes()
        got, after = self._follower(db).changes(token, limit)
        return out + got, after

    def position_lost(self, side, db, token):
        """Why a saved position can no longer be read on from, or None:
        a range's continuation the service no longer takes (the feed's
        retention passed, or the container was made again). Every container
        still there is asked, each range for a page after its position."""
        listed = set(self.scope_tables(side, db))
        for lane, cont in sorted((token.get("at") or {}).items()):
            t = lane.partition("|")[0]
            if t not in listed or not cont:
                continue
            try:
                feed = self._container(side, db, t) \
                    .query_items_change_feed(continuation=cont,
                                             max_item_count=1)
                next(iter(feed.by_page()), None)
            except Exception as e:  # noqa: BLE001 - said, as the error
                return (f"the change feed of {t} no longer takes its saved"
                        f" position: {str(e).splitlines()[0][:80]}")
        return None

    def release_changes(self):
        for f in self.__dict__.pop("_followers", {}).values():
            f.close()

    def stream_identity(self, side, db):
        return {"containers": ",".join(
            f"{t}:{self._container(side, db, t).read().get('_rid')}"
            for t in self.neutral_tables(side, db))}

    def server_identity(self, side, db):
        ep = self._ep(side)
        out = {"name": str(ep.options.get("url") or ep.host)}
        try:
            props = self._database(side, db).read()
            out["id"] = str(props.get("_rid"))
        except Exception:  # noqa: BLE001 - a database not there yet
            out["absent"] = True
        return out

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    #: its change tail is the pair's (`Engine.tail_masks`)
    TAILS_THROUGH_PAIR = True

    def tail_apply(self, db, go, token_path, log):
        return self._as_pair().tail_apply(db, go, token_path, log)

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)

    def delta_verify(self, db, limit=20000, log=None):
        return self._as_pair().delta_verify(db, limit, log)

    def reach_items(self, side):
        from ..changetopic import CosmosFeed
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "containers read one feed range at a time,"
                            " items upserted in transactional batches by"
                            " partition key, compared item by item"}]
        if side == "src":
            said = str(self.hop.source.options.get("change_feed") or "")
            items.append({"level": "warn" if said.startswith("latest")
                          else "pass", "scope": "reach",
                          "item": "changes followed through the change feed",
                          "detail": f"it loses {CosmosFeed.LOSES}"})
        return items

    def _assess_extra(self):
        """Where the data is read and written, what the move leaves for a
        person, and the throughput the target containers provision."""
        inv = self._handwork()
        rows = self.reach_items("src") + inv.rows() + inv.summary()
        for db in self.databases():
            try:
                got = self.capacity("dst", db)
            except Exception as e:  # noqa: BLE001 - unknown, not clean
                rows.append({"level": "warn", "scope": db,
                             "item": "the target's request units",
                             "detail": "could not ask: "
                                       f"{str(e).splitlines()[0][:90]}"
                                       " - unknown, not clean"})
                continue
            rows.append({
                "level": "pass" if got else "warn", "scope": db,
                "item": "the target's request units",
                "detail": (f"{got['provisioned']} target containers provision"
                           f" {got['request_units_per_second']:,} request"
                           " units a second between them; a request over it"
                           " is throttled and waited out" if got else
                           "no target container provisions throughput of its"
                           " own (shared, serverless, or not made yet) - the"
                           " rate of the copy is the database's")})
        return rows


def _register():
    """What of the SDK is used, read off this file, and why the rest is
    not (`tooluse`)."""
    from .. import tooluse
    unused = tooluse.UNUSED
    tooluse.register("azure-cosmos", licence="MIT", reads=(
        tuple(f"engines/cosmosdb.py::{fn}" for fn in (
            "_items", "read", "ends", "_mode", "_all", "neutral_digest",
            "_upsert", "neutral_rows_by_key", "_apply_deletes",
            "check_deep", "_kw")),
        {("ContainerProxy", "query_items"): "query_items",
         ("ContainerProxy", "query_items_change_feed"):
             "query_items_change_feed",
         ("ContainerProxy", "read_feed_ranges"): "read_feed_ranges",
         ("ContainerProxy", "execute_item_batch"): "execute_item_batch",
         ("ContainerProxy", "upsert_item"): "upsert_item",
         ("ContainerProxy", "read_item"): "read_item",
         ("ContainerProxy", "delete_item"): "delete_item",
         ("ContainerProxy", "list_conflicts"): "list_conflicts",
         ("change feed", "AllVersionsAndDeletes"): "AVAD",
         ("change feed", "LatestVersion"): "LATEST",
         ("change feed", "continuation"): "continuation",
         ("change feed", "feed_range"): "feed_range",
         ("request", "priority"): "priority"}), options={
        ("ContainerProxy", "read_all_items"): (
            unused, "items are read one feed range at a time, the ranges"
                    " side by side for a digest, which one read of all"
                    " cannot be split into"),
        ("ContainerProxy", "patch_item"): (
            unused, "a change carries its item's whole state, which a"
                    " patch of some fields would not"),
        ("ContainerProxy", "delete_all_items_by_partition_key"): (
            unused, "a preview the accounts migkit meets may not have; a"
                    " container is emptied item by item"),
        ("CosmosClient", "preferred_locations"): (
            unused, "a side is read where its endpoint's address sends"
                    " it, which names a nearer region where one is"
                    " wanted"),
    })


_register()


class _Containers:
    """The lanes of every container of a database, one reader over them:
    a lane is a container's feed range."""
    KIND = "cosmos"

    def __init__(self, by_table):
        self.by = by_table

    def _split(self, at):
        out = {t: {} for t in self.by}
        for lane, pos in (at or {}).items():
            t, _, rest = lane.partition("|")
            if t in out:
                out[t][rest] = pos
        return out

    def lanes(self):
        return [f"{t}|{lane}" for t, lanes in self.by.items()
                for lane in lanes.lanes()]

    def ends(self):
        return {f"{t}|{lane}": pos for t, lanes in self.by.items()
                for lane, pos in lanes.ends().items()}

    def read(self, at, most, seconds):
        out, nxt, caught = [], {}, True
        parts = self._split(at)
        for t, lanes in self.by.items():
            got, after, done = lanes.read(parts[t], most - len(out), seconds)
            out += [(f"{t}|{lane}", pos, k, v, topic)
                    for lane, pos, k, v, topic in got]
            nxt.update({f"{t}|{lane}": pos for lane, pos in after.items()})
            caught = caught and done
            if len(out) >= most:
                caught = False
                break
        for lane, pos in (at or {}).items():
            nxt.setdefault(lane, pos)
        return out, nxt, caught

    def lost(self, at):
        return None

    def room(self, at):
        return None

    def close(self):
        return None
