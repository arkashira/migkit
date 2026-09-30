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

from .base import Engine, NeutralCopier, Result

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

    def neutral_tables(self, side, db):
        return sorted(c["id"] for c in self._database(side, db)
                      .list_containers()
                      if not self.hop.excluded(db, c["id"]))

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
        batch = []
        container = self._container(side, db, table)
        for rng in container.read_feed_ranges():
            for item in self._items(side, db, table, rng):
                batch.append(self._row(names, classes, item, described))
                if len(batch) >= size:
                    yield batch
                    batch = []
        if batch:
            yield batch

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

    # ---- writing -------------------------------------------------------------

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
        cols = [[c[0], c[1] + (f"({c[2][0]},{c[2][1]})"
                               if c[1] == "decimal" and len(c[2] or ()) == 2
                               else "")] for c in columns]
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
        return self._as_pair().check_data(db, table, stream)

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
        return [self._param_result(db, src, dst, self.CRITICAL_PARAMS,
                                   "set the target container's settings as"
                                   " the source's")]

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

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each item the change feed says was created, replaced or
        deleted, in its range's order."""
        if token is None:
            return [], self.change_point(side, db)
        return self._follower(db).changes(token, limit)

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
        return self.reach_items("src")


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
