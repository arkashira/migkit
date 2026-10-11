import re
import time

from .. import pieces
from ..decide import Rung
from .base import Engine, RepairAction, Result, clauses, rows_counted
from ..verdict import difference_kind_from_counts

SKIP_DBS = {"admin", "local", "config"}
# Target documents per comparison range. Ranges keep the memory
# cost proportional to the range rather than to the collection.
DRILL_RANGE_DOCS = 200_000


class MongoEngine(Engine):
    checks = ("schema", "counts", "data")
    counts_from_data = True

    def _client(self, side):
        ep = self.hop.source if side == "src" else self.hop.target
        try:
            from pymongo import MongoClient
        except ImportError:
            raise SystemExit("pip install 'migkit[mongo]' for mongodb support")
        from urllib.parse import quote_plus
        auth = (f"{quote_plus(ep.user)}:{quote_plus(ep.password)}@"
                if ep.user else "")
        hosts = ep.options.get("hosts") or f"{ep.host}:{ep.port}"
        uri = f"mongodb://{auth}{hosts}/"
        extra = ep.options.get("uri_options", "")
        tls = ep.mongo_tls()
        if tls:
            from urllib.parse import urlencode
            extra = "&".join(p for p in (extra, urlencode(tls)) if p)
        # retryReads = the driver re-issues a read after a network blip
        if "retryReads" not in extra:
            extra = (extra + "&retryReads=true") if extra else "retryReads=true"
        uri += "?" + extra
        return MongoClient(uri, serverSelectionTimeoutMS=15000,
                           connectTimeoutMS=15000)

    def _d(self, side, db):
        """Physical db name: source as given, target through the hop's
        db_map (identity when unmapped) so a migration can land in a
        differently-named database."""
        return self.hop.target_db(db) if side == "dst" else db

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        c = self._client("src")
        return sorted(d for d in c.list_database_names()
                      if d not in SKIP_DBS and not self.hop.excluded(d))

    CANON_ENGINE = "mongodb"
    ENGINE_FAMILY = "mongodb"

    def _brand_probes(self):
        """`buildInfo` from each side, which is where MongoDB names itself.

        The alternative this replaced was reading the hostname and calling
        anything with `docdb` in it Amazon DocumentDB. A CNAME, a bastion or
        a connection string with a different label defeats that, and it
        answers about the address rather than about the software.
        """
        def one(side):
            try:
                out = dict(self._client(side).admin.command("buildInfo"))
            except Exception:
                return {}
            # a command only Azure Cosmos DB answers: MongoDB refuses it as
            # one it does not have, and its answer names the brand
            try:
                got = self._client(side).admin.command(
                    "getLastRequestStatistics")
                out["cosmos_request_charge"] = str(got.get("RequestCharge",
                                                           ""))
            except Exception:
                pass
            return out
        return (one("src"), one("dst"))

    def _server_versions(self):
        s, d = self._brands()
        return (s.version or None, d.version or None)


    def neutral_tables(self, side, db):
        c = self._client(side)[self._d(side, db)]
        return sorted(n for n in c.list_collection_names()
                      if not n.startswith("system.")
                      and self.in_scope(db, n))

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a collection")
        if where:
            raise SystemExit("a row filter is SQL, and MongoDB takes none;"
                             " narrow this collection another way")
        return self._client(side)[self._d(side, db)][table].delete_many(
            {}).deleted_count

    def text_encodings(self, side, db):
        # BSON strings are UTF-8 by the format's own definition
        return {"UTF-8": 1}

    def _there(self, db, name):
        """A collection's name on the target: the one the hop's
        `mapping.tables` or name rules give it, the source's own where it
        says nothing (`Hop.target_table_in` - the one reading of it, which
        the copy, the check, the repair and the tail all ask)."""
        return self.hop.target_table_in(db, name)

    def masked_on_the_way(self, db):
        """The collections the hop masks on the way (`mask_on_write`),
        which this engine's copier cannot (`move_table`)."""
        from ..movers import mask_rules
        return set(mask_rules(self.hop, db))

    def _rows_of(self, db, name):
        """The hop's row filter for a collection as a document query
        (`rowfilter.to_mongo`, with SQL's NULL kept), or None where it has
        none: the one filter the copy takes the documents by and the
        counts and the comparison read both sides through."""
        pred = self.hop.row_filter(db, name)
        if not pred:
            return None
        from .. import rowfilter
        return rowfilter.to_mongo(
            pred, rowfilter.dialect_of(self.CANON_ENGINE))

    @staticmethod
    def _within(flt, rows):
        """A query held to a row filter's documents."""
        if not rows:
            return flt
        return {"$and": [flt, rows]} if flt else rows

    def _taken(self, db):
        """([(a collection the hop takes on the source, its name on the
        target)], [the names only the target holds that the hop takes]):
        the collections every check reads, asked of the one scope answer
        (`Engine.in_scope`) and the hop's name rules (`_there`), so a
        collection renamed on the way is compared by its new name and not
        reported missing and extra."""
        s = self._client("src")[db].list_collection_names()
        t = set(self._client("dst")[self._d("dst", db)]
                .list_collection_names())
        mine = [(n, self._there(db, n)) for n in sorted(s)
                if not n.startswith("system.") and self.in_scope(db, n)]
        images = {b for _, b in mine}
        only = sorted(n for n in t if not n.startswith("system.")
                      and n not in images and self.in_scope(db, n))
        return mine, only

    def list_move_tables(self, db):
        """The collections the copy fills: a view is not one - it holds no
        documents, and read through it a copy made a collection of what it
        answers - it is carried as a view, once the collections are in
        (`carry_database_objects`)."""
        views = {v[0] for v in self._views("src", db)}
        return [("", c) for c in self.neutral_tables("src", db)
                if c not in views]

    # ---- indexes -----------------------------------------------------------

    @staticmethod
    def _index_specs(coll):
        """[(name, keys, options)] of a collection's indexes but its `_id`
        one: the keys as `create_index` takes them, the options as the
        server keeps them (`unique`, `sparse`, a TTL's `expireAfterSeconds`,
        a `partialFilterExpression`, a `collation`, a text index's weights
        ...) less the build's own bookkeeping. One reading, for the copy
        that makes the indexes of a collection it made and for what is
        told to another engine."""
        out = []
        for name, spec in coll.index_information().items():
            if name == "_id_":
                continue
            spec = dict(spec)
            keys = spec.pop("key")
            spec.pop("v", None)
            spec.pop("ns", None)
            spec.pop("background", None)
            out.append((name, list(keys), spec))
        return out

    def neutral_indexes(self, side, db, table):
        """[(name, unique, [fields], plain)] of a collection's indexes
        beside `_id`'s. `plain` is False for one another engine cannot be
        given as it is: a TTL, partial, sparse, hidden, collated, text,
        geospatial, hashed or wildcard one, a compound one that mixes
        directions - and the fields of a nested path, which are no
        column. [] for a view, which has none."""
        from pymongo.errors import OperationFailure
        coll = self._client(side)[self._d(side, db)][table]
        try:
            specs = self._index_specs(coll)
        except OperationFailure as e:
            if e.code == 166:
                return []
            raise
        out = []
        for name, keys, spec in specs:
            ascending = [d for _, d in keys]
            plain = (not set(spec) - {"unique"}
                     and all(d in (1, -1) for d in ascending)
                     and (len(set(ascending)) == 1
                          or all(d == 1 for d in ascending))
                     and not any("$" in str(f) for f, _ in keys))
            out.append((name, bool(spec.get("unique")),
                        [str(f) for f, _ in keys], plain))
        return out

    def neutral_create_index_sql(self, side, db, table, name, unique,
                                 columns):
        """The statement that makes one index here, in the shell's own
        words - what `execute_ddl` runs and a person can run by hand."""
        import json
        keys = json.dumps({c: 1 for c in columns})
        opts = {"name": name}
        if unique:
            opts["unique"] = True
        return (f"db.getCollection({json.dumps(str(table))})"
                f".createIndex({keys}, {json.dumps(opts)})")

    #: the one statement `execute_ddl` runs (`neutral_create_index_sql`)
    _CREATE_INDEX = re.compile(r"^db\.getCollection\(")

    def execute_ddl(self, side, db, sql):
        """Run one statement that changes this side's schema: the index
        `neutral_create_index_sql` writes, and nothing else - a document
        store keeps no other schema to change. Read as JSON from end to
        end, never matched by text, so a collection or a field named with
        a brace or a quote is what it is named."""
        import json
        self._target_only(side, "change a schema")
        text = str(sql).strip()
        refuse = ValueError(
            "a document store takes one schema statement, an index made"
            f" with createIndex: {text[:80]}")
        got = self._CREATE_INDEX.match(text)
        if not got:
            raise refuse
        read = json.JSONDecoder().raw_decode
        try:
            coll, at = read(text, got.end())
            glue = ").createIndex("
            if text[at:at + len(glue)] != glue:
                raise refuse
            keys, at = read(text, at + len(glue))
            if text[at:at + 2] != ", ":
                raise refuse
            opts, at = read(text, at + 2)
        except json.JSONDecodeError:
            raise refuse
        if text[at:] != ")" or not isinstance(coll, str) \
                or not isinstance(keys, dict) or not isinstance(opts, dict):
            raise refuse
        self._client(side)[self._d(side, db)][coll].create_index(
            list(keys.items()), **opts)

    # ---- views and stored functions ----------------------------------------

    def _views(self, side, db):
        """[(name, viewOn, pipeline, collation)] of the database's views,
        the hop's scope not asked: an aggregation pipeline over a
        collection (or another view), kept by the server as the view's
        definition."""
        d = self._client(side)[self._d(side, db)]
        out = []
        for c in d.list_collections(filter={"type": "view"}):
            opts = c.get("options") or {}
            out.append((c["name"], opts.get("viewOn"),
                        list(opts.get("pipeline") or []),
                        opts.get("collation")))
        return sorted(out, key=lambda v: v[0])

    @staticmethod
    def _view_text(on, pipeline, collation=None):
        """A view's definition as one text, Extended JSON with its keys in
        order: what two views are held to, and what a person reads."""
        from bson import json_util
        body = {"viewOn": on, "pipeline": pipeline}
        if collation:
            body["collation"] = collation
        return json_util.dumps(body, sort_keys=True)

    def neutral_views(self, side, db):
        """[(name, definition)] of the views the hop takes: each its
        pipeline and the collection it reads, as `_view_text` says it -
        not SQL, which no other engine's converter reads (it asks for an
        SQL dialect first)."""
        return [(n, self._view_text(on, pipe, coll))
                for n, on, pipe, coll in self._views(side, db)
                if self.takes(db, "view", n)]

    def code_routines(self, side, db):
        """The functions stored in the database's `system.js`, the one
        stored code a document store keeps: JavaScript, each by the name
        it is called by in the server's own scripts. A routine of another
        engine's language cannot be made from it - the conversion names
        that - and `carry_database_objects` carries it to a server of this
        engine as it is."""
        from ..storedcode.routines import Routine
        out = []
        for doc in self._client(side)[self._d(side, db)]["system.js"].find(
                sort=[("_id", 1)]):
            name = str(doc["_id"])
            if not self.hop.takes_object("function", db, name):
                continue
            body = doc.get("value")
            out.append(Routine(
                kind="function", name=name, body=str(body),
                definition=self._function_text(doc), language="javascript",
                settings={"kept in": "system.js"}))
        return out

    @staticmethod
    def _function_text(doc):
        from bson import json_util
        return json_util.dumps({"_id": doc["_id"], "value": doc.get("value")},
                               sort_keys=True)

    def carry_database_objects(self, db, log=None):
        """What a database holds outside its collections, made on the
        target once the collections are in: its views - each over the
        collection it reads, a view over a view after the one it reads -
        and its stored functions. One the target already holds the same is
        left; one it holds that differs is the target's own and is said,
        never replaced; a view over a collection the hop leaves out is
        not made, and said."""
        log = log or (lambda m: None)
        self._carry_views(db, log)
        self._carry_functions(db, log)

    def _carry_views(self, db, log):
        from pymongo.errors import PyMongoError
        views = [v for v in self._views("src", db)
                 if self.takes(db, "view", v[0])]
        if not views:
            return
        target = self._client("dst")[self._d("dst", db)]
        have = {n: (on, pipe, coll) for n, on, pipe, coll in
                self._views("dst", db)}
        taken = {v[0] for v in views}
        kept = set(self.neutral_tables("src", db)) - taken
        waiting = list(views)
        while waiting:
            later = []
            for name, on, pipe, coll in waiting:
                there = self.hop.target_table_in(db, name)
                over = self.hop.target_table_in(db, on)
                if any(v[0] == on for v in waiting):
                    later.append((name, on, pipe, coll))
                    continue
                if on not in kept and on not in taken:
                    log(f"view {name}: not made - it reads {on}, which the"
                        " hop leaves out")
                    continue
                if there in have:
                    same = have[there] == (over, pipe, coll)
                    log(f"view {name}: " + (
                        "already on the target" if same else
                        "the target has a view of that name that reads"
                        " otherwise; left as it is"))
                    continue
                try:
                    kw = {"viewOn": over, "pipeline": pipe}
                    if coll:
                        kw["collation"] = coll
                    target.create_collection(there, **kw)
                except PyMongoError as e:
                    log(f"view {name}: not made - "
                        f"{(str(e).splitlines() or [''])[0][:100]}")
                    continue
                have[there] = (over, pipe, coll)
                log(f"view {name}: made over {over}")
            if len(later) == len(waiting):
                for name, on, *_ in later:
                    log(f"view {name}: not made - it reads {on}, a view"
                        " that could not be made")
                return
            waiting = later

    def _carry_functions(self, db, log):
        src = self.code_routines("src", db)
        if not src:
            return
        target = self._client("dst")[self._d("dst", db)]["system.js"]
        read = self._client("src")[self._d("src", db)]["system.js"]
        for r in src:
            doc = read.find_one({"_id": r.name})
            held = target.find_one({"_id": r.name})
            if held is None:
                target.insert_one(doc)
                log(f"function {r.name}: made")
            elif self._function_text(held) == self._function_text(doc):
                log(f"function {r.name}: already on the target")
            else:
                log(f"function {r.name}: the target has one of that name"
                    " that reads otherwise; left as it is")

    #: documents per read of the collection copier, and the bytes of BSON
    #: a read ends at (`_docs_until`)
    COPY_BATCH = 5000
    COPY_BYTES = pieces.BATCH_BYTES
    #: large values - a document is 16 MB at most, BSON's own limit, and a
    #: file kept in GridFS is its chunks, documents of 255 KB each, carried
    #: as the collections they are: every read of the copier, the pair and
    #: the digest ends at `pieces.BATCH_BYTES` of BSON, and a collection is
    #: sized by the server (`row_sizes`)
    LARGE_VALUES = pieces.Way(
        "sized by the server, read and written in batches cut by bytes, a"
        " file in GridFS carried chunk by chunk")

    def _docs_until(self, coll, flt, projection, limit, raw=False):
        """The documents `find` gives in `_id` order, `limit` at most, up to
        the one that brings them to `pieces.BATCH_BYTES` as BSON - measured
        by the bytes each arrived as, and decoded one at a time unless
        `raw`, the collection's own way. The cursor is closed there: the
        next read goes on from the last `_id`. By a count alone, five
        thousand documents a read of up to 16 MB each could be 80 GB."""
        from bson import decode
        from bson.raw_bson import RawBSONDocument
        opts = coll.codec_options
        cursor = coll.with_options(codec_options=opts.with_options(
            document_class=RawBSONDocument)).find(
            flt, projection).sort("_id", 1).limit(int(limit))
        out, held = [], 0
        try:
            for d in cursor:
                held += len(d.raw)
                out.append(d if raw else decode(d.raw, codec_options=opts))
                if held >= pieces.BATCH_BYTES:
                    break
        finally:
            cursor.close()
        return out

    def row_sizes(self, side, db, tables=None):
        """Each document's bytes as BSON, asked of the server
        (`$bsonSize`): the largest and the mean over every document of a
        collection of no more than `pieces.EXACT_UP_TO`, over a sample of a
        larger one (`$sample`), the 99th percentile from its first
        `SIZE_SAMPLE`. Only numbers cross."""
        from .base import sizes_of
        dbh = self._client(side)[self._d(side, db)]
        try:
            facts = self.table_facts(side, db)
        except Exception:  # noqa: BLE001 - sized without the statistics
            facts = {}
        size = {"$bsonSize": "$$ROOT"}
        out = {}
        for t in (tables if tables is not None
                  else self.neutral_tables(side, db)):
            f = facts.get(t) or {}
            exact = int(f.get("bytes") or 0) <= pieces.EXACT_UP_TO
            head = [] if exact else [{"$sample": {"size": self.SIZE_SAMPLE}}]
            try:
                got = list(dbh[t].aggregate(head + [{"$group": {
                    "_id": None, "most": {"$max": size},
                    "mean": {"$avg": size}, "n": {"$sum": 1}}}],
                    allowDiskUse=True))
                first = [int(d["s"]) for d in dbh[t].aggregate([
                    {"$limit": self.SIZE_SAMPLE},
                    {"$project": {"_id": 0, "s": size}}])]
            except Exception:  # noqa: BLE001 - that collection is not sized
                continue
            if not got or not got[0].get("n"):
                continue
            sized = sizes_of(first or [int(got[0]["most"])], exact=exact,
                             rows=int(f.get("rows") or got[0]["n"]))
            sized["largest"] = max(int(got[0]["most"]), sized["largest"])
            sized["mean"] = float(got[0]["mean"])
            out[t] = sized
        return out

    def move_table(self, db, sch, tbl, chunk, ck, log):
        """One collection, document for document as stored: every BSON type,
        nested documents and arrays included, which the classes the other
        engines share cannot carry.

        Resumed by `_id` from the checkpoint. The resume compares through
        `$expr`, which orders across types the way the sort does - a plain
        `$gt` matches only values of the same type, and a collection whose
        `_id`s are numbers and strings would have lost every string after
        the last number. Each batch replaces the documents with the same
        `_id`, so a restart converges. A collection this copy made gets the
        source's indexes once its documents are in."""
        from bson import json_util
        from bson.raw_bson import RawBSONDocument
        from pymongo import ReplaceOne
        from pymongo.errors import BulkWriteError

        from ..wording import progress
        name = tbl or sch
        if name in self.masked_on_the_way(db):
            # the copy carries each document as the bytes the server sent,
            # and a column the hop masks would arrive as it is: no copy
            # that cannot mask is run for a collection that is to be
            raise SystemExit(
                f"{db}.{name}: the hop masks columns of this collection"
                " (mask_on_write), and a MongoDB copy carries each"
                " document as stored, so the masked values would arrive"
                " unmasked. Nothing was copied; leave the collection out"
                " of this hop, or move it by a way that masks")
        key = self.move_key(db, sch, tbl)
        st = ck.setdefault(key, {})
        if st.get("done") and self.recheck_done(db, name, st, log,
                                                label=key):
            ck.save()
            return
        ck.save()
        # each document as the bytes the server sent, never decoded and
        # encoded again, inserted rather than replaced one by one where the
        # collection was emptied, and read past the last `_id` by the index
        # where it can be: measured, 200,000 documents in 0.7-1.0s against
        # 3.4s
        src = self._client("src")[db].get_collection(
            name, codec_options=self._client("src").codec_options
            .with_options(document_class=RawBSONDocument))
        target = self._client("dst")[self._d("dst", db)]
        # under the name the hop gives it there (`mapping.tables`): it was
        # written under the source's, and the one the hop names stayed
        # empty
        there = self._there(db, name)
        dst = target[there]
        # only the documents the hop's row filter takes, if it has one
        rows = self._rows_of(db, name)
        after = json_util.loads(st["last"]) if "last" in st else None
        started_here = after is None
        if started_here:
            st["made"] = there not in target.list_collection_names()
            # what the target already holds is not this copy's
            gone = dst.delete_many({}).deleted_count
            st["moved"] = 0
            if gone:
                log(f"{key}: emptied {gone:,} documents the target held"
                    " before the copy")
        moved = from_docs = int(st.get("moved", 0))
        total = (src.count_documents(rows) if rows else
                 (self.table_facts("src", db).get(name) or {}).get("rows"))
        batch = max(1, min(int(chunk), self.COPY_BATCH))
        began = time.monotonic()
        # `_id`s of one BSON type from the first to the last are all of
        # that type - BSON orders by type first - and then a plain `$gt`
        # is exact and takes the index
        ends = [next(iter(src.find(rows or {}, {"_id": 1}).sort("_id", o)
                          .limit(1)), None) for o in (1, -1)]
        one_type = all(ends) and type(ends[0]["_id"]) is \
            type(ends[1]["_id"])
        while True:
            flt = {} if after is None else (
                {"_id": {"$gt": after}} if one_type else
                {"$expr": {"$gt": ["$_id", {"$literal": after}]}})
            docs = self._docs_until(src, self._within(flt, rows), None,
                                    batch, raw=True)
            if not docs:
                break
            try:
                # into a collection this copy emptied: new documents,
                # nothing to replace - except after a stop, where the batch
                # past the last saved `_id` may be there already
                dst.insert_many(docs, ordered=False)
            except BulkWriteError as e:
                if any(err.get("code") != 11000
                       for err in e.details.get("writeErrors", [])):
                    raise
                dst.bulk_write([ReplaceOne({"_id": d["_id"]}, d,
                                           upsert=True) for d in docs],
                               ordered=False)
            moved += len(docs)
            after = docs[-1]["_id"]
            st["moved"] = moved
            st["last"] = json_util.dumps(after)
            ck.save()
            log(progress(key, moved, total, began, time.monotonic(),
                         unit="documents", since=from_docs))
        if st.get("made"):
            for index_name, keys, spec in self._index_specs(src):
                dst.create_index(keys, name=index_name, **spec)
        st["done"] = True
        ck.save()

    def moved_nothing(self, db):
        """Collections the source has documents in and the target has none.

        The guard the SQL engines already had: a bulk copy can finish
        without an error and leave the target empty, and saying the move
        is complete over that is the one thing a migration tool must not
        do. A collection the target does not have at all counts too - this
        engine creates collections on the first write, so a missing one is
        one nothing reached. Excluded collections are not listed, so a
        collection left out on purpose is not reported. A renamed one is
        looked for under the name the hop gives it on the target.
        """
        try:
            dst = set(self._client("dst")[self._d("dst", db)]
                      .list_collection_names())
            s = self._client("src")[db]
            t = self._client("dst")[self._d("dst", db)]
            empty = []
            for c in self.neutral_tables("src", db):
                # a collection whose filter takes no document has nothing
                # to move
                if s[c].find_one(self._rows_of(db, c) or {},
                                 {"_id": 1}) is None:
                    continue
                there = self._there(db, c)
                if there not in dst or t[there].find_one(
                        {}, {"_id": 1}) is None:
                    empty.append(c)
            return empty
        except Exception:
            return None

    def field_types(self, side, db, collection):
        """{field: (types, present_count, null_count)} over the whole
        collection.

        A full scan rather than `$sample`. Sampling would be cheaper and
        would answer a different question: a field that exists in one
        document out of a million is exactly the one a migration drops, and a
        sample that misses it reports a collection migkit never looked at as
        fully described.

        `missing` and `null` are counted apart because MongoDB keeps them
        apart and the target usually cannot. Both land as NULL in a SQL
        column, so the conversion is one-way and nobody notices unless it is
        said out loud.
        """
        coll = self._client(side)[self._d(side, db)][collection]
        rows = coll.aggregate([
            {"$project": {"kv": {"$objectToArray": "$$ROOT"}}},
            {"$unwind": "$kv"},
            {"$group": {"_id": "$kv.k",
                        "types": {"$addToSet": {"$type": "$kv.v"}},
                        "present": {"$sum": 1},
                        "nulls": {"$sum": {"$cond": [
                            {"$eq": [{"$type": "$kv.v"}, "null"]}, 1, 0]}}}},
            {"$sort": {"_id": 1}},
        ])
        out = {r["_id"]: (sorted(r["types"]), r["present"], r["nulls"])
               for r in rows}
        for field, (types, present, nulls) in out.items():
            if "binData" in types:
                out[field] = (sorted(set(types) - {"binData"}
                                     | self._binary_kinds(coll, field)),
                              present, nulls)
        return out

    @staticmethod
    def _binary_kinds(coll, field):
        """The binary subtypes one field holds, by the names the type map
        knows them by: `binData` for plain bytes (0), `uuid` for a UUID
        (4), `binData3` and on for the rest. `$type` says `binData` for
        every subtype and no aggregation operator reads one before
        MongoDB 8, so the field's binaries are read here - only that
        field, and only where it holds any. Known only as `binData`, a
        UUID (subtype 4) went to another engine as plain bytes and came
        back subtype 0, its sixteen bytes compared equal on both hops."""
        kinds = set()
        for doc in coll.find({field: {"$type": "binData"}},
                             {field: 1, "_id": 0}):
            v = doc.get(field)
            sub = getattr(v, "subtype", 0)
            kinds.add({0: "binData", 4: "uuid"}.get(sub, f"binData{sub}"))
        return kinds or {"binData"}

    def neutral_columns(self, side, db, table):
        """[(field, types joined by '|')] - the set, not a declaration.

        The set is what `canon.type_class` is given: it drops `null` and
        `missing`, and a field left holding two real types comes back
        unmapped rather than resolved to the more popular one.
        """
        return [(name, "|".join(types))
                for name, (types, _, _) in
                sorted(self.field_types(side, db, table).items())]

    EXPRESSES_ABSENT = True
    CREATES_ON_WRITE = True

    def table_facts(self, side, db):
        """{collection: {"rows", "key", "bytes", "index_bytes"}} from each
        collection's own statistics - `size` is the documents as they are,
        before the storage engine compresses them, which is what a copy
        carries."""
        out = {}
        dbh = self._client(side)[self._d(side, db)]
        for name in self.neutral_tables(side, db):
            try:
                st = dbh.command("collStats", name)
            except Exception:  # noqa: BLE001 - a view, or not allowed
                continue
            out[name] = {"rows": int(st.get("count") or 0), "key": True,
                         "bytes": int(st.get("size") or 0),
                         "index_bytes": int(st.get("totalIndexSize") or 0)}
        return out

    def free_bytes(self, side, db):
        """The free space of the disk the database is on: `dbStats`
        reports the filesystem's size and use."""
        try:
            st = self._client(side)[self._d(side, db)].command("dbStats")
        except Exception:  # noqa: BLE001 - not allowed is no answer
            return None
        total, used = st.get("fsTotalSize"), st.get("fsUsedSize")
        if total is None or used is None:
            return None
        return max(int(total) - int(used), 0)

    def neutral_key(self, side, db, table):
        """`_id`, which MongoDB guarantees on every document."""
        return ["_id"]

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """Documents as rows, with a missing field marked rather than nulled.

        `canon.ABSENT` is not None: MongoDB distinguishes a field that is not
        there from one holding null, and reading the first as the second here
        would destroy the distinction before the mover could count what the
        target does to it.
        """
        if where:
            raise SystemExit("a row filter is SQL, and MongoDB takes none;"
                             " narrow this collection another way")
        from .. import canon
        coll = self._client(side)[self._d(side, db)][table]
        names = [n for n, _ in columns]
        projection = {f: 1 for f in names}
        projection.setdefault("_id", 1 if "_id" in names else 0)
        # `$gt` in a query compares within one type only, so resuming from
        # a number never reached a string or an ObjectId: measured, a chunked
        # read of seven documents keyed 1, 2, 2.5, "a", "b" and two
        # ObjectIds stopped after the three numbers, and a copy through it
        # moved three of seven without a word. An expression compares in
        # the order the sort uses, across types.
        flt = ({"$expr": {"$gt": ["$_id", after[0]]}} if after else {})
        rows, last = [], None
        # ending at `pieces.BATCH_BYTES` of BSON, and going on from the last
        # `_id` the next time (`_docs_until`)
        for doc in self._docs_until(coll, flt, projection, limit or 1000):
            rows.append([doc.get(n, canon.ABSENT) if n in doc
                         else canon.ABSENT for n in names])
            last = doc.get("_id")
        if not rows:
            return ([], None)
        # a document can be keyed null, and ending on one is not the end
        return (rows, (last,))

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        """The same lookup, expressed as a filter instead of a select.

        A field that is not there still comes back as `canon.ABSENT` rather
        than None, for the same reason the ordered read does: the two mean
        different things here and only one of them is a value.
        """
        if where:
            raise SystemExit("a row filter is SQL, and MongoDB takes none;"
                             " narrow this collection another way")
        from .. import canon
        if not key or not keys:
            return {}
        names = [n for n, _ in columns]
        projection = {f: 1 for f in names}
        projection.setdefault("_id", 1 if "_id" in names else 0)
        if len(key) == 1:
            flt = {key[0]: {"$in": [k[0] for k in keys]}}
        else:
            flt = {"$or": [dict(zip(key, values)) for values in keys]}
        rows = []
        for doc in self._client(side)[self._d(side, db)][table].find(
                flt, projection):
            rows.append([doc.get(n, canon.ABSENT) if n in doc
                         else canon.ABSENT for n in names])
        return self._by_key_map(columns, key, rows)

    @staticmethod
    def _bind(value, cls=None):
        """One value on its way into a BSON document - by the BSON type
        the column it came from is held as, where the copy says
        (`canon.write_value`, `bsontype`): a `bigint` as a 64-bit
        integer, a `uuid` as a binary of subtype 4, a JSON column's
        Extended JSON as the value it stands for.

        MongoDB has a decimal type and pymongo will not reach it on its own:
        measured, `insert_one({"n": Decimal("1.50")})` raises `InvalidDocument:
        cannot encode object: Decimal('1.50'), of type: <class
        'decimal.Decimal'>`. That is where a PostgreSQL `numeric` column meets
        this side of a move or a repair.

        `Decimal128` is the right home for it rather than a float or a
        string: measured, it comes back holding the scale it was given
        (`1.50` stays `1.50`), which is what makes the two sides render to
        the same text.
        """
        import datetime
        import decimal

        from .. import canon
        if cls is not None and value is not canon.ABSENT:
            value = canon.write_value("mongodb", cls, value)
        if isinstance(value, decimal.Decimal):
            from bson.decimal128 import Decimal128
            return Decimal128(value)
        if type(value) is datetime.date:
            # pymongo takes no date without a time (`cannot encode
            # object`): a BSON date at its midnight, as a `date` column's
            # BSON type says
            return datetime.datetime(value.year, value.month, value.day)
        return value

    def neutral_write(self, side, db, table, columns, rows):
        """Upsert by `_id`, leaving an absent field absent.

        A value that arrived as `canon.ABSENT` is not written at all, so a
        MongoDB-to-MongoDB move keeps the shape of the document it copied. A
        value that arrived as None is written as null, because that is what
        the source said it was.
        """
        from pymongo import UpdateOne

        from .. import canon
        if not rows:
            return 0
        names = [n for n, _ in columns]
        if "_id" not in names:
            raise SystemExit(f"{table}: migkit will not write documents"
                             " without _id - there would be no way to write"
                             " the same row twice without duplicating it")
        at = names.index("_id")
        classes = [c for _, c in columns]
        ops = []
        for row in rows:
            body = {n: self._bind(v, c) for n, c, v in zip(names, classes,
                                                            row)
                    if n != "_id" and v is not canon.ABSENT}
            unset = {n: "" for n, v in zip(names, row)
                     if n != "_id" and v is canon.ABSENT}
            update = {}
            if body:
                update["$set"] = body
            if unset:
                update["$unset"] = unset
            if not update:
                update = {"$setOnInsert": {}}
            ops.append(UpdateOne({"_id": self._bind(row[at], classes[at])},
                                 update, upsert=True))
        coll = self._client(side)[self._d(side, db)][table]
        coll.bulk_write(ops, ordered=False)
        return len(rows)

    def local_table(self, table):
        """The last component: this engine has no schemas to qualify with."""
        return str(table).split(".")[-1]

    def _apply_upsert(self, side, db, table, key, values):
        from .. import canon
        table = self.local_table(table)
        body = {n: self._bind(v) for n, v in values.items()
                if n not in key and v is not canon.ABSENT}
        unset = {n: "" for n, v in values.items()
                 if n not in key and v is canon.ABSENT}
        update = {}
        if body:
            update["$set"] = body
        if unset:
            update["$unset"] = unset
        if not update:
            update = {"$setOnInsert": {}}
        coll = self._client(side)[self._d(side, db)][table]
        coll.update_one(dict(key), update, upsert=True)

    def _apply_delete(self, side, db, table, key):
        table = self.local_table(table)
        coll = self._client(side)[self._d(side, db)][table]
        coll.delete_one(dict(key))

    # A change stream reports these four as row changes. The rest are
    # collection- and database-level events, and none of them is something a
    # row-shaped applier can carry.
    STREAM_OPS = {"insert": "insert", "replace": "insert",
                  "update": "update", "delete": "delete"}

    @staticmethod
    def _not_a_row_change(op, event):
        return SystemExit(
            f"the change stream reported {op!r} on"
            f" {(event.get('ns') or {}).get('coll', '?')}, which is"
            " not a row change - migkit carries inserts,"
            " updates, replaces and deletes, and will not pretend"
            " a dropped or renamed collection is one of them.\n"
            "    Re-run the full load for that collection, then"
            " start the tail again from a token taken after it")

    CHANGE_POINT_READS_ONLY = True
    #: a resume token only says where a stream opens: one of its own reads
    #: from an older one again
    REREADS_FROM_A_POSITION = True
    #: a tail stopped on a collection dropped or renamed before there was a
    #: `ddl` (`_not_a_row_change`), and still does where it says stop
    DDL_STOPS = ("truncate", "drop", "rename", "create", "alter")

    def stream_room(self, side, db, token):
        """Seconds of oplog older than the tail's resume point: the oplog
        drops its oldest entries as it is written, so this is about how
        long the point stays in it at the rate it is written now.

        Measured on 7: a resume token's `_data` opens with 0x82 and the
        cluster time, seconds then increment, both four bytes big-endian -
        the seconds matched the `clusterTime` the server answered with.
        None where the token is not one of those or the oplog cannot be
        read by this user."""
        from pymongo.errors import PyMongoError
        try:
            raw = bytes.fromhex(str(token or ""))
        except ValueError:
            return None
        if len(raw) < 9 or raw[0] != 0x82:
            return None
        at = int.from_bytes(raw[1:5], "big")
        try:
            oldest = next(iter(
                self._client(side).local["oplog.rs"].find({}, {"ts": 1})
                .sort("$natural", 1).limit(1)), None)
        except PyMongoError:
            return None
        if not oldest:
            return None
        return {"seconds": max(at - oldest["ts"].time, 0)}

    def stream_identity(self, side, db):
        """The replica set: its name, and its id where this user may read
        the configuration. A token from another set was measured to be
        accepted without a word."""
        from pymongo.errors import PyMongoError
        admin = self._client(side).admin
        try:
            name = admin.command("hello").get("setName")
        except PyMongoError:
            return None
        if not name:
            return None
        out = {"replica set": name}
        try:
            got = admin.command("replSetGetConfig")["config"]
            out["id"] = str(got.get("settings", {}).get("replicaSetId", ""))
        except PyMongoError:
            pass
        return out

    def server_identity(self, side, db):
        """The replica set (`stream_identity`), the member answering and
        its process - an election moves both - its version, and the
        database's size."""
        from pymongo.errors import PyMongoError
        client = self._client(side)
        hello = client.admin.command("hello")
        out = {"name": str(hello.get("me") or "")}
        try:
            st = client.admin.command("serverStatus")
            out["running"] = f"{st.get('host')} process {st.get('pid')}"
            out["version"] = str(st.get("version") or "")
            out["name"] = out["name"] or str(st.get("host") or "")
        except PyMongoError:
            pass
        name = self._d(side, db)
        if name:
            try:
                if name not in client.list_database_names():
                    out["absent"] = True
                else:
                    out["bytes"] = int(client[name].command("dbStats")
                                       .get("dataSize") or 0)
            except PyMongoError:
                pass
        got = self.stream_identity(side, db)
        if got:
            out["id"] = got
        return out

    def rows_held(self, db, tables):
        """Collections the target holds a document of: its copy empties a
        collection before it starts one afresh (`move_table`)."""
        from pymongo.errors import PyMongoError
        try:
            t = self._client("dst")[self._d("dst", db)]
            have = set(t.list_collection_names())
            return {(s, c) for s, c in tables
                    if c in have and t[c].find_one({}, {"_id": 1}) is not None}
        except PyMongoError:
            return None

    def position_lost(self, side, db, token):
        """The oplog no longer reaching back to the token, or the token
        standing later than the set's own clock - taken on another set.
        Asked before the stream opens: a stream opened on a lost point is
        refused only by some servers, and not by all versions."""
        from pymongo.errors import PyMongoError
        data = token.get("_data") if isinstance(token, dict) else token
        at = self._token_time(data) if data else None
        if at is None:
            return None
        client = self._client(side)
        try:
            oldest = next(iter(client.local["oplog.rs"].find({}, {"ts": 1})
                               .sort("$natural", 1).limit(1)), None)
            now = client.admin.command("hello").get("operationTime")
        except PyMongoError:
            return None
        if oldest and at.time < oldest["ts"].time:
            return (f"the source's oplog now starts at {oldest['ts'].as_datetime():%Y-%m-%d %H:%M:%S} UTC and"
                    f" the saved position is from {at.as_datetime():%Y-%m-%d %H:%M:%S} UTC: the changes"
                    " between them are gone from the source")
        if now is not None and at.time > now.time + 60:
            return (f"the saved position ({at.as_datetime():%Y-%m-%d %H:%M:%S} UTC) is later than"
                    " the source's own clock"
                    f" ({now.as_datetime():%Y-%m-%d %H:%M:%S} UTC): it was taken on another replica"
                    " set")
        return None

    def stream_match(self, db):
        """The change stream's own filter for what the hop takes - [] where
        it takes every collection: the server leaves out the events of a
        collection the hop does not take, and they never cross the wire,
        where every event of the database was read and dropped here. Read
        from the hop's patterns (`movers.scope_alternatives`), so a
        collection made later under a taken pattern is in it; an event
        renaming one into what is taken passes by its new name; one about
        the whole database always passes. Each event is still asked here
        (`Hop.excluded`): the server's filter only saves the reading."""
        from ..movers import NOTHING, scope_alternatives
        out, taken = scope_alternatives(self.hop, db, False)
        if not out and taken is None:
            return []
        rx = ("^" + (f"(?!(?:{'|'.join(out)})$)" if out else "")
              + (".*$" if taken is None else
                 f"(?:{'|'.join(taken) or NOTHING})$"))
        return [{"$match": {"$or": [
            {"ns.coll": {"$regex": rx}},
            {"to.coll": {"$regex": rx}},
            {"ns.coll": {"$exists": False}}]}}]

    def change_point(self, side, db):
        """A resume token for now, read without taking any event off the
        stream: the server answers an empty first batch with the position it
        reached, which is exactly the point a tail started from here would
        begin at."""
        try:
            stream = self._client(side)[self._d(side, db)].watch([])
        except Exception as e:
            if "only supported on replica sets" in str(e):
                raise SystemExit(
                    "this server is a standalone, and a change stream needs"
                    " a replica set - the oplog a stream reads does not"
                    " exist otherwise")
            raise
        try:
            token = (stream.resume_token or {}).get("_data")
        finally:
            stream.close()
        if not token:
            # None would be read as "start from now" later, after the copy
            raise SystemExit(
                "the server opened a change stream without saying where it"
                " stands, so there is no position to start the tail from"
                " before the copy")
        return token

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Row changes out of a change stream, as neutral records.

        Opened over the whole database rather than per collection, so one
        cursor covers every table the hop moves.

        `fullDocument="updateLookup"` is not optional here. Measured on
        MongoDB 7: without it an `update` event carries only
        `updateDescription.updatedFields` - the delta - and nothing else. On
        a target row that already exists that is enough; on one that is
        missing it would insert a row holding the key and the changed field
        and nothing more, which is a row that looks present and is not. The
        cost is that the lookup reads the document as it is *now*, so a value
        newer than the event can arrive early. A tail converges either way;
        a half-written row does not.

        `updateDescription.removedFields` names what `$unset` took away, and
        those come through as `canon.ABSENT` - the same marker the full load
        uses, so a SQL target counts them and a document target keeps them
        missing.
        """
        from .. import canon
        client = self._client(side)
        target = self._d(side, db)
        args = {"full_document": "updateLookup"}
        if token:
            args["resume_after"] = {"_data": token}
        try:
            stream = client[target].watch(self.stream_match(db), **args)
        except Exception as e:
            if "only supported on replica sets" in str(e):
                raise SystemExit(
                    "this server is a standalone, and a change stream needs"
                    " a replica set - the oplog a stream reads does not"
                    " exist otherwise. A single node is enough:\n"
                    "    mongod --replSet rs0\n"
                    "    mongosh --eval 'rs.initiate()'")
            raise
        out = []
        # the transaction the last event read was in, the events of it in
        # `out` from `begun`, and where a read goes on after it: its end
        # is known only at the next event, or not at all where the read
        # stops inside it (`canon.ended`)
        txn, begun, after, ts, event = None, 0, None, None, None
        try:
            while len(out) < limit:
                event = stream.try_next()
                if event is None:
                    break
                op = event.get("operationType")
                table = (event.get("ns") or {}).get("coll")
                now = self._txn_of(event)
                if now != txn or now is None:
                    if txn is not None or after is not None:
                        canon.ended(out, begun, at=after, ts=ts)
                    begun = len(out)
                txn = now
                after = (event.get("_id") or {}).get("_data")
                ts = self._event_time(event)
                # a collection the hop leaves alone, dropped or written,
                # is the target's business and not the tail's - unless it
                # is renamed into the hop's scope, which the tail must
                # meet: dropped here, the collection joined with none of
                # its documents under every `ddl` (as MySQL's `_ddl_of`
                # keeps a rename whose either end is in scope)
                if table and self.hop.excluded(db, table) and not (
                        op == "rename" and self._renamed_in(db, event)):
                    continue
                if op in self.STREAM_DDL:
                    got = self._ddl_event(db, op, event)
                    if got is None:
                        raise self._not_a_row_change(op, event)
                    out += got
                    continue
                if op not in self.STREAM_OPS:
                    raise self._not_a_row_change(op, event)
                key = dict(event.get("documentKey") or {})
                at = event.get("clusterTime")
                if self.STREAM_OPS[op] == "delete":
                    out.append(canon.change("delete", table, key, txn=at))
                elif event.get("fullDocument") is None:
                    # The lookup found nothing, which means the document was
                    # deleted between the change and this read. Whatever
                    # deleted it produced its own event, and that event is
                    # behind this one in the same stream - so the truth about
                    # this key is already on its way and carrying a key with
                    # no values would put an empty row on the target until it
                    # arrives.
                    continue
                else:
                    values = dict(event.get("fullDocument") or {})
                    for gone in ((event.get("updateDescription") or {})
                                 .get("removedFields") or []):
                        values[gone] = canon.ABSENT
                    out.append(canon.change(self.STREAM_OPS[op], table,
                                            key, values, txn=at))
            else:
                # stopped at the limit: the last transaction may go on in
                # the next read, unless it was one event of its own
                if txn is None:
                    canon.ended(out, begun, at=after, ts=ts)
                txn = None
            if txn is not None or event is None:
                # read to the stream's end: the transaction read last is
                # whole, a server's transaction being in the stream at once
                canon.ended(out, begun, at=after, ts=ts)
            last = stream.resume_token
        finally:
            stream.close()
        return out, (last or {}).get("_data") if last else token

    #: a change stream can end a read inside a multi-document transaction,
    #: and a resume token only says where to read from: the tail may end a
    #: batch at the last transaction's end and read the rest again
    #: (`hetero.tail_apply`)
    READ_ENDS_INSIDE_TRANSACTIONS = True

    #: the collection events a change stream reports, as `canon.ddl` kinds
    #: - with `showExpandedEvents` (6.0 and later) the rest of them too;
    #: `dropDatabase` and `invalidate` end the stream and are not one
    #: collection's
    STREAM_DDL = {"drop": "drop", "rename": "rename", "create": "create",
                  "modify": "alter", "createIndexes": "alter",
                  "dropIndexes": "alter", "shardCollection": "alter",
                  "reshardCollection": "alter",
                  "refineCollectionShardKey": "alter"}

    def _renamed_in(self, db, event):
        """Whether a rename event's new name, in this database, is one the
        hop takes."""
        dest = event.get("to") or {}
        return bool(dest.get("coll")) and dest.get("db") == self._d(
            "src", db) and not self.hop.excluded(db, dest["coll"])

    def _ddl_event(self, db, op, event):
        """[canon.ddl] for a collection event, None where it is not one
        collection's."""
        from .. import canon
        table = (event.get("ns") or {}).get("coll")
        if not table:
            return None
        to = None
        if op == "rename":
            dest = event.get("to") or {}
            to = dest.get("coll") if dest.get("db") == self._d(
                "src", db) else None
            if to is None:
                return [canon.ddl(table, "drop",
                                  txn=event.get("clusterTime"))]
        return [canon.ddl(table, self.STREAM_DDL[op], to=to,
                          txn=event.get("clusterTime"))]

    @staticmethod
    def _txn_of(event):
        """(session, transaction number) of a multi-document transaction's
        event, None for one that is its own."""
        if event.get("txnNumber") is None:
            return None
        lsid = event.get("lsid") or {}
        return (str(lsid.get("id")), int(event["txnNumber"]))

    @staticmethod
    def _event_time(event):
        """The source's commit time of an event, in seconds: its wall time
        (6.0 and later), else its cluster time's seconds."""
        wall = event.get("wallTime")
        if wall is not None:
            try:
                import datetime
                if wall.tzinfo is None:
                    wall = wall.replace(tzinfo=datetime.timezone.utc)
                return wall.timestamp()
            except (AttributeError, ValueError):
                pass
        at = event.get("clusterTime")
        return float(at.time) if at is not None else None

    def snapshot_mark(self, side, db):
        """The replica set's majority commit point now, `set|seconds:inc`:
        every write at or before it is committed on a majority and in the
        primary's own view, so a read begun after it sees it. Not the
        cluster time a command answers with - writes still in flight
        around it can show later than one after them (the oplog's
        holes); the majority point has none behind it. None where the
        server says no majority point, a standalone or a sharded
        cluster's router."""
        from pymongo.errors import PyMongoError
        on = (self.stream_identity(side, db) or {}).get("replica set")
        if not on:
            return None
        try:
            got = self._client(side).admin.command("hello")
        except PyMongoError:
            return None
        at = ((got.get("lastWrite") or {}).get("majorityOpTime") or {}) \
            .get("ts")
        if at is None:
            return None
        return f"{on}|{at.time}:{at.inc}"

    def mark_covers(self, side, db, mark, change):
        """Whether the change's cluster time is at or before the mark's
        (`snapshot_mark`), on the same replica set."""
        at = change.get("txn")
        if not isinstance(mark, str) or at is None \
                or not hasattr(at, "time") or not hasattr(at, "inc"):
            return None
        on, _, point = mark.rpartition("|")
        if not on or on != (self.stream_identity(side, db) or {}).get(
                "replica set"):
            return None
        try:
            t, _, i = point.partition(":")
            return (int(at.time), int(at.inc)) <= (int(t), int(i))
        except ValueError:
            return None

    def neutral_digest(self, side, db, table, columns, where=None):
        """(count, digest) folded here rather than in the server.

        MongoDB has no hashing operator at all - measured on 7.0, `$md5`,
        `$sha1`, `$sha256` and `$hash` are each "Unknown expression". The two
        ways to get one are `$toHashedIndexKey`, which is internal and whose
        value no other engine can reproduce, and `$function`, which runs
        server-side JavaScript that would have to carry its own MD5 and that
        several MongoDB-compatible services do not allow at all.

        So the documents are read and folded on this machine, using the same
        rendering and the same arithmetic the SQL engines use in-server. The
        answer is identical; what differs is that the collection crosses the
        network to produce it, and `check` says so rather than letting the
        number imply otherwise.
        """
        if where:
            raise SystemExit("a row filter is SQL, and MongoDB takes none;"
                             " narrow this collection another way")
        from .. import canon
        coll = self._client(side)[self._d(side, db)][table]
        fields = [name for name, _ in columns]
        classes = dict(columns)
        from ..util import with_retry
        total, n = 0, 0
        projection = {f: 1 for f in fields}
        # the key rides along to resume from, whether or not it is compared
        projection["_id"] = 1
        # In chunks, each its own short query resumed from the last key,
        # not one cursor held open for the whole collection: sustained
        # cursors stalled over a tunnel where single reads did not. A chunk
        # that fails on the way is read again rather than ending the pass.
        after, started = None, False
        while True:
            flt = {"$expr": {"$gt": ["$_id", after]}} if started else {}
            # a chunk ends at `pieces.BATCH_BYTES` of BSON as well
            docs = with_retry(lambda: self._docs_until(
                coll, flt, projection, self.DIGEST_CHUNK),
                label="mongo read")
            if not docs:
                break
            k, total = canon.fold_rows(
                [classes[name] for name in fields],
                ([doc.get(name) for name in fields] for doc in docs), total)
            n += k
            after, started = docs[-1]["_id"], True
        return (n, str(total))

    #: documents per read of the digest pass
    DIGEST_CHUNK = 5000

    def _shape(self, side, db, pairs=None):
        """{collection: its indexes, type and options} of the ones the hop
        takes, keyed by the source's name: `pairs` - (the source's name,
        its name on this side) - says which are read and under what name
        they are held here (a collection the hop renames), by default the
        ones this side lists that the hop takes. A view the hop's
        `objects` leaves out is no more read than a collection it
        excludes."""
        d = self._client(side)[self._d(side, db)]
        shape = {}
        listed = set(d.list_collection_names())
        if pairs is None:
            pairs = [(n, n) for n in sorted(listed) if self.in_scope(db, n)]
        # the collections and views the hop takes, with their indexes: one
        # it leaves out, on one side only, was "extra on target" every check
        for source, name in pairs:
            if name not in listed:
                continue
            idx = {}
            opts = d.command("listCollections",
                             filter={"name": name})["cursor"]["firstBatch"][0]
            # a view has no indexes, and the server refuses to list them
            for i in ([] if opts.get("type") == "view"
                      else d[name].list_indexes()):
                spec = {k: (str(v) if k == "key" else v)
                        for k, v in i.items()
                        if k not in ("v", "ns", "background")}
                idx[i["name"]] = repr(sorted(spec.items(), key=str))
            o = {k: v for k, v in opts.get("options", {}).items()
                 if k not in ("storageEngine", "autoIndexId")
                 and not (k == "capped" and not v)}
            kind = opts.get("type", "collection")
            if kind == "view" and not self.takes(db, "view", source):
                continue
            shape[source] = {"indexes": idx, "type": kind,
                             "options": repr(sorted(o.items(), key=str))}
        return shape

    def _shape_diff(self, name, a, b):
        out = []
        if a["type"] != b["type"]:
            out.append(f"{name}: type {a['type']} vs {b['type']}")
        if a["options"] != b["options"]:
            out.append(f"{name}: options src={a['options']}"
                       f" dst={b['options']}")
        for ix in sorted(set(a["indexes"]) | set(b["indexes"])):
            sa, sb = a["indexes"].get(ix), b["indexes"].get(ix)
            if sa is None:
                out.append(f"{name}.{ix}: index missing on target")
            elif sb is None:
                out.append(f"{name}.{ix}: extra index on target")
            elif sa != sb:
                out.append(f"{name}.{ix}: spec src={sa} dst={sb}")
        return out

    def check_schema(self, db):
        mine, only = self._taken(db)
        src = self._shape("src", db, [(a, a) for a, _ in mine])
        dst = self._shape("dst", db, mine + [(b, b) for b in only])
        bad = []
        for name in sorted(set(src) | set(dst)):
            if name not in dst:
                bad.append(f"{name}: missing on target")
            elif name not in src:
                bad.append(f"{name}: extra on target")
            elif src[name] != dst[name]:
                bad.extend(self._shape_diff(name, src[name], dst[name]))
        d = self.hop.report_dir(db)
        (d / "schema-src.txt").write_text(repr(src))
        (d / "schema-dst.txt").write_text(repr(dst))
        import json
        inv = {}
        for typ, pick in (("collection", lambda n, v: v["type"] == "collection"),
                          ("view", lambda n, v: v["type"] == "view")):
            a = {n for n, v in src.items() if pick(n, v)}
            b = {n for n, v in dst.items() if pick(n, v)}
            inv[typ] = {"src": len(a), "dst": len(b),
                        "missing": sorted(a - b)[:50], "extra": sorted(b - a)[:50]}
        ia = {f"{n}.{i}" for n, v in src.items() for i in v["indexes"]}
        ib = {f"{n}.{i}" for n, v in dst.items() for i in v["indexes"]}
        inv["index"] = {"src": len(ia), "dst": len(ib),
                        "missing": sorted(ia - ib)[:50],
                        "extra": sorted(ib - ia)[:50]}
        (d / "objects.json").write_text(json.dumps(inv, indent=1))
        res = []
        if bad:
            res.append(Result("schema", db, "diff", "; ".join(bad[:10]),
                              str(d), "create missing collections/indexes on target"))
        else:
            res.append(Result("schema", db, "ok", f"{len(src)} collections"))
        total = sum(v["src"] for v in inv.values())
        obad = {t: v for t, v in inv.items() if v["missing"] or v["extra"]}
        if obad:
            parts = [f"{t} {v['src']}/{v['dst']}" for t, v in obad.items()]
            res.append(Result("schema", f"{db} objects", "diff",
                              "; ".join(parts), str(d / "objects.json"),
                              "create the missing objects on target"))
        else:
            res.append(Result("schema", f"{db} objects", "ok",
                              f"{total} objects in {len(inv)} types,"
                              " all present on target"))
        return res

    def check_counts(self, db):
        """Counts per collection - and the collections that are on one side
        only, which this used to drop before counting anything.

        Measured: a collection holding 9 documents that existed only on the
        source came back `ok | 1 collections, docs 5==5`. The intersection
        removed it, so the check reported on what the two sides happened to
        share and called that agreement. Every other engine names a missing
        table right here, and `migkit check` is meant to be the same command
        whichever store is underneath.
        """
        s, t = self._client("src")[db], self._client("dst")[self._d("dst", db)]
        # the collections the hop takes, each by the name it has on the
        # target (`_taken`), and counted through the hop's row filter
        mine, only = self._taken(db)
        there = set(t.list_collection_names())
        missing = [a for a, b in mine if b not in there]
        res = []
        if missing:
            res.append(Result("counts", db, "diff",
                              "missing collections on target:"
                              f" {missing}", "",
                              "create and load them before cutover"))
        if only:
            res.append(Result("counts", db, "diff",
                              "extra collections on target:"
                              f" {only}"))
        bad, outcomes = [], {}
        names = [(a, b) for a, b in mine if b in there]
        total_a = total_b = 0
        for name, other in names:
            rows = self._rows_of(db, name) or {}
            a = s[name].count_documents(rows)
            b = t[other].count_documents(rows)
            total_a += a
            total_b += b
            outcomes[name] = "ok" if a == b else "diff"
            if a != b:
                bad.append(f"{name} src={a} dst={b}")
        outcomes.update({a: "diff" for a in missing})
        if bad:
            res.append(Result("counts", db, "diff", "; ".join(bad)))
        res = res or [Result("counts", db, "ok",
                             f"{len(names)} collections, docs"
                             f" {total_a:,}=={total_b:,}")]
        # how each collection came out, said by every finding it is in
        for r in res:
            r.tables = dict(outcomes)
        return res

    def check_params(self, db):
        def pull(side):
            try:
                res = self._client(side).admin.command({"getParameter": "*"})
            except Exception as e:
                return {self.UNREADABLE: str(e).splitlines()[-1][:80]}
            return {k: v for k, v in res.items() if k != "ok"}
        return self._param_result(
            db, pull("src"), pull("dst"), ("featureCompatibilityVersion",),
            "align server parameters / feature compatibility version on target")

    def changes_made(self, side, db):
        """Inserts, updates and deletes the server has run since it started
        (`serverStatus` opcounters, every database), and its uptime."""
        st = self._client(side).admin.command("serverStatus")
        ops = st.get("opcounters") or {}
        return (sum(int(ops.get(k) or 0) for k in ("insert", "update",
                                                    "delete")),
                float(st.get("uptime") or 0))

    def _health(self, side):
        """What this deployment says about its own load, for the throttle.

        `connections.active` against the connection ceiling is the direct
        analogue of active sessions against max_connections. Operations
        waiting in `globalLock.currentQueue` are folded into the same number:
        work piling up *is* the server being at its limit, which is what
        busy_ratio means, and keeping the shared Health contract to two
        fields is worth more than a third signal only one engine reports.
        """
        from ..throttle import BUSY_RATIO, Health
        try:
            st = self._client(side).admin.command("serverStatus")
        except Exception:
            return None
        busy = None
        conn = st.get("connections") or {}
        ceiling = (conn.get("current") or 0) + (conn.get("available") or 0)
        if ceiling:
            busy = (conn.get("active") or 0) / ceiling
        queued = (((st.get("globalLock") or {}).get("currentQueue") or {})
                  .get("total") or 0)
        if queued:
            busy = max(busy or 0.0, BUSY_RATIO)
        lag, replica = None, False
        try:
            rs = self._client(side).admin.command("replSetGetStatus")
            members = rs.get("members") or []
            primary = next((m for m in members if m.get("stateStr") == "PRIMARY"),
                           None)
            me = next((m for m in members if m.get("self")), None)
            if primary and me and me.get("stateStr") == "SECONDARY" \
                    and me.get("optimeDate") and primary.get("optimeDate"):
                # the member read is a secondary: its own distance behind
                # the primary, which its brake holds the check's rise in
                # (`throttle.REPLICA_RISE`)
                replica = True
                lag = max(0.0, (primary["optimeDate"] - me["optimeDate"])
                          .total_seconds())
            elif primary:
                times = [m.get("optimeDate") for m in members
                         if m.get("optimeDate") and m.get("stateStr")
                         in ("PRIMARY", "SECONDARY")]
                if len(times) > 1:
                    lag = (max(times) - min(times)).total_seconds()
        except Exception:
            pass    # standalone, or the command is not permitted: lag unknown
        return Health(busy_ratio=busy, lag_seconds=lag, replica=replica)

    def _db_hashes(self, db, names=None):
        """(source, target) `dbHash` per collection, or (None, None) where
        either side will not give it - a mongos, a user without the
        privilege, a service that leaves the command out."""
        # `names` are the source's; the target is asked for each under the
        # name the hop gives it there, and answers by that name
        cmd = {"dbHash": 1}
        if names:
            cmd["collections"] = list(names)
            there = dict(cmd, collections=[self._there(db, n)
                                           for n in names])
        else:
            there = cmd
        try:
            return (self._client("src")[db].command(cmd)["collections"],
                    self._client("dst")[self._d("dst", db)]
                    .command(there)["collections"])
        except Exception:
            return None, None

    @staticmethod
    def _server_hash(hashes, name):
        """The collection's md5 from `dbHash`, or None where there is none
        to trust: absent, or a note in its place - a collection without an
        `_id` index is answered `no _id _index` on both sides, which read
        as two equal hashes."""
        h = (hashes or {}).get(name)
        return h if isinstance(h, str) and len(h) == 32 and all(
            c in "0123456789abcdef" for c in h) else None

    def check_data(self, db, table=None, stream=None, with_counts=False):
        s, t = self._client("src")[db], self._client("dst")[self._d("dst", db)]
        # the collections the hop takes, each by the name it has on the
        # target (`_taken`)
        mine, only = self._taken(db)
        there = set(t.list_collection_names())
        names = [table] if table else [a for a, b in mine if b in there]
        a, b = self._db_hashes(db, names)
        # dbHash and the id drilldown are both full scans, so the same rule
        # as the other engines applies: stop adding load to a deployment that
        # is already struggling.
        from ..throttle import Throttle
        gate = Throttle(1, probe=lambda: self._health("src"))
        res = []
        told = {}
        for name in names:
            if name.startswith("system."):
                continue
            ha, hb = self._server_hash(a, name), self._server_hash(
                b, self._there(db, name))
            if self._rows_of(db, name):
                # a hash of the whole collection says nothing of the
                # documents the hop's filter takes: they are walked, the
                # filter read as the copy read it
                ha = hb = None
            if ha and ha == hb:
                if stream:
                    stream(f"{name}: ok")
                res.append(Result("data", f"{db}.{name}", "ok",
                                  f"dbHash {ha} both sides"))
                continue
            told[name] = bool(ha and hb)
            with gate.unit():
                r = self._compare(db, name, told[name])
            if stream:
                stream(f"{name}: {r.status}")
            res.append(r)
        bad = [r.scope.split(".", 1)[1] for r in res
               if r.check == "data" and r.status == "diff"]
        if bad:
            # confirm before calling it different: a change the tail has not
            # applied yet is still arriving, not wrong
            _, healed, how = self._resolve_inflight(db, bad, stream)
            proof = dict(h.split(": ", 1) for h in how)
            for i, r in enumerate(res):
                name = r.scope.split(".", 1)[1] if "." in r.scope else ""
                if r.check == "data" and name in healed:
                    again = self._compare(db, name, told.get(name, False))
                    if again.status == "ok":
                        again = Result("data", again.scope, "ok",
                                       f"{again.detail}; the difference was"
                                       " still arriving"
                                       f" ({proof.get(name, 'confirmed')})",
                                       again.report)
                    res[i] = again
        self._last_throttle = gate.summary()
        note = gate.line()
        if note and stream:
            stream(f"# {note}")
        # each finding says how the collection it compared came out
        for r in res:
            if r.check == "data" and "." in r.scope:
                r.tables = {r.scope.split(".", 1)[1]: r.status}
        if with_counts:
            # equal hashes imply equal counts; only diffed colls pay to count
            bad = [f"{a} missing on target" for a, b in mine
                   if b not in there]
            bad += [f"{c} extra on target" for c in only]
            for r in res:
                if r.check == "data" and r.status != "ok":
                    name = r.scope.split(".", 1)[1]
                    rows = self._rows_of(db, name) or {}
                    ca = s[name].count_documents(rows)
                    cb = t[self._there(db, name)].count_documents(rows)
                    if ca != cb:
                        bad.append(f"{name} src={ca} dst={cb}")
            if bad:
                cres = Result("counts", db, "diff", clauses(bad))
            else:
                cres = Result("counts", db, "ok",
                              f"{len(names)} collections, equality implied"
                              " by data hash (no extra count scan)")
            res.insert(0, cres)
        return res

    # --- the confirm pass (base `fenced_recheck`) ---------------------------

    def _compare_pks(self, db, table, keys):
        """(missing, extra, changed) among these `_id`s, as the key files
        write them - read again from both sides, now."""
        from bson.json_util import dumps, loads
        ids = [loads(k) for k in keys]
        src = self._raw(self._client("src")[db][table])
        dst = self._raw(self._client("dst")[self._d("dst", db)]
                        [self._there(db, table)])

        def read(coll):
            out = {}
            for i in range(0, len(ids), 1000):
                for doc in coll.find({"_id": {"$in": ids[i:i + 1000]}}):
                    out[dumps(self._plain_id(doc))] = doc.raw
            return out
        a, b = read(src), read(dst)
        missing = [k for k in a if k not in b]
        extra = [k for k in b if k not in a]
        changed = [k for k in a if k in b and a[k] != b[k]]
        # a key gone from both sides has converged: pgCompare's lesson
        return missing, extra, changed

    def who_wrote(self, db, table, keys, began):
        """An ObjectId carries the second it was made, which is when the
        document was written unless its writer chose its own id. Of the
        documents only the target has, those made after the move began
        were written to the target while it ran."""
        from bson import ObjectId
        from bson.json_util import loads
        ids = []
        for k in keys:
            try:
                ids.append(loads(k))
            except Exception:
                continue
        made = [i.generation_time.timestamp() for i in ids
                if isinstance(i, ObjectId)]
        if not ids:
            return ""
        other = len(ids) - len(made)
        said = []
        since = (began or {}).get("epoch")
        if made and since:
            after = sum(1 for t in made if t >= since)
            if len(made) - after:
                said.append(f"{len(made) - after} with ids made before the"
                            f" move of {began.get('at')} began - there"
                            " before it, or brought from elsewhere")
            if after:
                said.append(f"{after} with ids made after it began - written"
                            " to the target while it ran")
        elif made:
            import datetime
            first = datetime.datetime.fromtimestamp(min(made),
                                                    datetime.timezone.utc)
            last = datetime.datetime.fromtimestamp(max(made),
                                                   datetime.timezone.utc)
            said.append(f"ids made between {first:%Y-%m-%d %H:%M} and"
                        f" {last:%Y-%m-%d %H:%M} UTC")
        if other:
            said.append(f"{other} with ids that carry no time")
        return f"{len(ids)} documents only on the target: " + "; ".join(said)

    def _write_pk_files(self, db, table, missing, extra, changed):
        d = self.hop.report_dir(db)
        for bucket, keys in (("missing", missing), ("extra", extra),
                             ("changed", changed)):
            p = d / f"data-{table}.{bucket}"
            if keys:
                p.write_text("\n".join(keys) + "\n")
            elif p.exists():
                p.unlink()

    def src_lsn(self, db):
        """The source's cluster time now, or None on a server that keeps
        none (a standalone) - there is nothing a tail could have read up
        to there."""
        got = self._client("src").admin.command("hello")
        return got.get("operationTime")

    def log_position(self, side, db):
        """The cluster time now, which a resume token carries too."""
        got = self._client(side).admin.command("hello")
        return got.get("operationTime")

    @classmethod
    def position_reached(cls, have, want):
        at = cls._token_time(have)
        if at is None or want is None:
            return None
        return at >= want

    @staticmethod
    def _token_time(token):
        """The cluster time a resume token stands at. The token's first
        byte marks a timestamp and the next eight are its seconds and
        increment - measured on MongoDB 7 against the times of the events
        the tokens came from."""
        from bson.timestamp import Timestamp
        raw = bytes.fromhex(str(token)[:18])
        if len(raw) < 9 or raw[0] != 0x82:
            return None
        return Timestamp(int.from_bytes(raw[1:5], "big"),
                         int.from_bytes(raw[5:9], "big"))

    def _tail_position(self, db):
        """How far migkit's own tail has applied, from the position it
        saved - either tail's - or None when no tail has saved one."""
        from .. import tailctl
        try:
            token = tailctl.saved(tailctl.token_path(self.hop, db))
        except SystemExit:
            return None
        if isinstance(token, dict):
            token = token.get("_data")
        return self._token_time(token) if isinstance(token, str) else None

    def fence_wait(self, db, at, timeout=300):
        """Wait until migkit's tail has applied the source up to `at`.

        True when it has, False on timeout, None when there is nothing to
        wait on - no cluster time, or no tail of migkit's saving a
        position - so the caller falls back rather than waiting blind.
        """
        from .. import tailctl
        if at is None or self._tail_position(db) is None:
            return None
        end = time.time() + timeout
        while time.time() < end:
            got = self._tail_position(db)
            if got is not None and got >= at:
                return True
            # looked at every 50 ms, not once a second (audit SP19)
            time.sleep(tailctl.POLL)
        return False

    def _id_plan(self, coll, docs):
        """Ranges that split this collection by `_id`, one BSON type at a time.

        The trap here cost a rewrite and is worth stating plainly: MongoDB's
        *sort* order spans BSON types, but its *query* comparison operators
        are type-bracketed. `{_id: {$lt: someObjectId}}` therefore matches no
        integer and no string, however the values sort. A single ordered list
        of boundaries taken across a mixed-type `_id` leaves every document of
        the other types outside every range - and a verifier that quietly
        skips 12% of a collection while reporting "identical" is worse than
        one that refuses to split at all.

        So the keyspace is partitioned per type: the types present are read
        first, and each gets its own boundaries and its own open-ended first
        and last range. `_coverage_ok` then checks the plan actually accounts
        for every document before it is trusted.
        """
        from .. import checkpoint as _cp
        want = max(1, min(512, -(-docs // DRILL_RANGE_DOCS)))
        if want < 2:
            return [(None, None, None)]
        try:
            types = [r["_id"] for r in coll.aggregate(
                [{"$group": {"_id": {"$type": "$_id"}}},
                 {"$sort": {"_id": 1}}], allowDiskUse=True)]
        except Exception:
            return [(None, None, None)]
        if not types:
            return [(None, None, None)]
        plan = []
        for ty in types:
            flt = {"_id": {"$type": ty}}
            try:
                n = coll.count_documents(flt)
                per = max(2, -(-want * n // max(docs, 1))) if n else 0
                if n and per >= 2:
                    out = list(coll.aggregate(
                        [{"$match": flt},
                         {"$bucketAuto": {"groupBy": "$_id",
                                          "buckets": min(per, 512)}}],
                        allowDiskUse=True))
                    bounds = [x["_id"]["min"] for x in out][1:]
                else:
                    bounds = []
            except Exception:
                bounds = []
            # each type carries its own open ends, and the $type match is what
            # keeps one type's boundaries from being asked about another's docs
            plan += [(ty, lo, hi) for lo, hi in _cp.plan_from_boundaries(bounds)]
        return plan

    def _coverage_ok(self, coll, plan, docs, rows=None):
        """Do these ranges actually account for every document - of the
        ones the hop's row filter (`rows`) takes?

        A plan is only worth using if it does. Cheap to verify - one indexed
        count per range - and it turns a silent hole into a fallback.
        """
        # Only the genuinely universal range is trusted without counting.
        # "One range must cover everything" is true for (None, None, None) and
        # false for any single range that carries a type or a bound, which is
        # a shortcut worth not taking.
        if plan == [(None, None, None)]:
            return True
        try:
            seen = sum(coll.count_documents(
                self._within(self._range_filter(ty, lo, hi), rows))
                for ty, lo, hi in plan)
        except Exception:
            return False
        return seen == coll.count_documents(rows or {})

    @staticmethod
    def _range_filter(ty, lo, hi):
        from .. import checkpoint as _cp
        f = dict(_cp.mongo_filter(lo, hi))
        cond = dict(f.get("_id") or {})
        if ty is not None:
            cond["$type"] = ty
        return {"_id": cond} if cond else {}

    def _range_hashes(self, coll, flt):
        """Each document's `$toHashedIndexKey`, worked out by the server:
        what the first walk after a differing `dbHash` ships, eight bytes a
        document. Not a comparison of values: the hasher turns every number
        into a 64-bit integer first, so 2.3 and 2.9 hash alike, and so do
        5, NumberLong(5) and 5.0 - it finds where documents differ in
        anything else, and `_compare` walks again by the bytes when it
        finds nothing."""
        pipe = ([{"$match": flt}] if flt else []) + [
            {"$project": {"h": {"$toHashedIndexKey": "$$ROOT"}}}]
        return {repr(d["_id"]): (d["_id"], d["h"])
                for d in coll.aggregate(pipe, allowDiskUse=True)}

    #: how a collection whose `dbHash` differs is walked first
    #: (`_compare`): by the servers' id hash, which is cheap, and where a
    #: server will not work it out - an older one without
    #: `$toHashedIndexKey`, a user refused the aggregation - by the bytes,
    #: on both sides alike
    WALKS = (Rung("id hash", "each document's hash worked out by the"
                             " servers"),
             Rung("bytes", "each document's stored bytes"))

    def _compare(self, db, name, told):
        """One collection compared document by document, and the answer
        only `ok` when nothing is left unexplained.

        `told` is whether both servers gave a `dbHash` and they differ.
        Then the server-side id hash walks first, being cheap, and what it
        finds is the answer. When it finds nothing the difference is one
        it cannot see - a number changed in its fraction or its type
        (measured on 7.0: 2.3 against 2.9, and NumberLong(5) against 5.0,
        each reported `ok` with `dbHash` different) - so the same ranges
        are walked again by each document's stored bytes, which see
        everything, field order and types included. And when even that
        finds nothing, `dbHash` is asked again: equal now, a write was
        between the reads; still different, the collection differs and
        says it could not say where, rather than ok.

        Without a `dbHash` to go by (a mongos, a user without the right) the
        id hash could only ever say "ok" wrongly or find what the bytes
        find too, so the walk is by the bytes alone: every document
        crosses the network, which is what exactness costs before a server
        can hash a document itself. Measured, 200,000 documents of 232
        bytes a side on MongoDB 7: the id hash walked them in 0.79 to
        1.13 s, the bytes in 1.36 to 2.49 s."""
        by_bytes = False
        if told:
            # a walk by the id hash that cannot run on either side is
            # switched from by itself, to the bytes on both (`_attempt`):
            # one side's hashes are never held to the other side's bytes,
            # which differ for every document. A difference found is the
            # answer, returned as it is
            said = []
            walked, r = self._attempt(
                f"{db}.{name}", self.WALKS,
                lambda c: self._drilldown(db, name,
                                          exact=c.path == "bytes"),
                log=said.append)
            r = self._switch_said(r, said)
            if r.status != "ok":
                return r
            by_bytes = walked.path == "bytes"
        if not by_bytes:
            r = self._drilldown(db, name, exact=True)
        if r.status != "ok" or not told:
            return r
        a, b = self._db_hashes(db, [name])
        ha, hb = self._server_hash(a, name), self._server_hash(
            b, self._there(db, name))
        if ha and ha == hb:
            return Result("data", r.scope, "ok",
                          f"{r.detail}; dbHash, different at first, is"
                          " equal asked again - a write was between the"
                          " reads", r.report)
        return Result("data", r.scope, "diff",
                      "differs, not localized: dbHash differs"
                      + (f" ({ha} against {hb})" if ha and hb else "")
                      + " and no document differs by id or by its stored"
                      f" bytes ({r.detail})", str(self.hop.report_dir(db)),
                      "check again with no writes on either side; if it"
                      " stays, copy the collection again")

    @staticmethod
    def _switch_said(r, said):
        """The walk's result with the switch it took said in it."""
        import dataclasses
        if not said:
            return r
        return dataclasses.replace(r, detail=f"{r.detail}; {'; '.join(said)}")

    def _drilldown(self, db, name, exact=False):
        """Compare every document by id, one `_id` range at a time - by the
        server's id hash, or `exact`ly by each document's stored bytes.

        This used to build a dict of every id and hash for both sides at once,
        which is why it refused to run past five million documents and sent
        the operator to another tool. Working a range at a time makes the
        memory cost proportional to the range instead of the collection, and
        makes the comparison restartable: completed ranges are recorded, so a
        rerun only pays for what it still owes.

        Comparing per range is sound because `_id` is unique - a document
        falls in exactly one range, and both sides are given the same
        boundaries, so nothing can be missing on one side merely for being
        looked at in a different range. The two walks keep their partials
        apart: a range the id hash found equal proves nothing about its
        bytes.
        """
        from bson.json_util import dumps

        from .. import checkpoint as _cp
        scope = f"{db}.{name}"
        mark = f"{scope} by bytes" if exact else scope
        hashes = self._client_hashes if exact else self._range_hashes
        s = self._client("src")[db][name]
        t = self._client("dst")[self._d("dst", db)][self._there(db, name)]
        # both sides read through the hop's row filter, as the copy read
        # the source (`_rows_of`)
        rows = self._rows_of(db, name)
        docs = s.count_documents(rows or {})
        ranges = self._id_plan(s, docs)
        if not self._coverage_ok(s, ranges, docs, rows):
            # better one slow pass than a fast answer about part of the data
            ranges = [(None, None, None)]
        cp = _cp.Checkpoint(str(self.hop.report_dir(db) / "checkpoint.json"))
        # The checkpoint identifies a range by text, and a range here is
        # (type, lo, hi) - so key and value are kept as an explicit pair
        # rather than squeezing three things into two and hoping.
        keyed = [((f"{ty}|{lo}", f"{hi}"), (ty, lo, hi)) for ty, lo, hi in ranges]
        by_key = dict(keyed)
        todo_keys = cp.begin(mark, ("bson md5" if exact
                                    else "toHashedIndexKey")
                             + (repr(rows) if rows else ""),
                             [k for k, _ in keyed])
        todo = [by_key[k] for k in todo_keys]
        done_before = cp.resumed(mark)

        d = self.hop.report_dir(db)
        found = {"missing": [], "extra": [], "changed": []}
        for ty, lo, hi in todo:
            flt = self._within(self._range_filter(ty, lo, hi), rows)
            src = hashes(s, flt)
            dst = hashes(t, flt)
            found["missing"] += [src[k][0] for k in src if k not in dst]
            found["extra"] += [dst[k][0] for k in dst if k not in src]
            found["changed"] += [src[k][0] for k in src
                                 if k in dst and src[k][1] != dst[k][1]]
            if any(found.values()):
                # a difference makes the stored partials useless: the next run
                # must re-read this collection rather than trust a half-total
                cp.clear(mark)
            else:
                cp.record(mark, f"{ty}|{lo}", f"{hi}", len(src), 0)
        checked = cp.total(mark)[0] if not any(found.values()) else 0
        if not any(found.values()):
            cp.clear(mark)
            resumed = (f", resumed {done_before}/{len(ranges)}"
                       if done_before else "")
            how = "by their bytes" if exact else "by id hash"
            return Result("data", scope, "ok",
                          f"docs {checked:,} compared {how},"
                          f" {len(ranges)} ranges{resumed}")
        for bucket, ids in found.items():
            p = d / f"data-{name}.{bucket}"
            if ids:
                p.write_text("\n".join(dumps(i) for i in ids) + "\n")
            elif p.exists():
                p.unlink()
        kind = difference_kind_from_counts(len(found["missing"]),
                                           len(found["extra"]),
                                           len(found["changed"]))
        whose = self.who_wrote(db, name, [dumps(i) for i in found["extra"]],
                               self._move_began(db))
        return Result("data", scope, "diff",
                      f"missing={len(found['missing'])}"
                      f" extra={len(found['extra'])}"
                      f" changed={len(found['changed'])}"
                      f" over {len(ranges)} ranges"
                      + (" by their bytes" if exact else "")
                      + (f" kind={kind}" if kind else "")
                      + (f"; {whose}" if whose else ""), str(d),
                      f"migkit sync {self.hop.name} --db {db} --kind rows --apply",
                      table=str(name), rows_apart=rows_counted(
                          name, len(found["missing"]), len(found["extra"]),
                          len(found["changed"])))

    #: stages that write, which a rule may not hold anywhere in it
    WRITING_STAGES = ("$out", "$merge")

    def run_rule(self, side, db, question):
        """The rows one side answers to a business rule: an aggregation
        pipeline over one collection, `{"collection": ..., "pipeline":
        [...]}`. Each document is a row, its values in the order the
        pipeline wrote them, a grouped `_id` spread into its fields - the
        same rows a SQL `group by` gives on the other side of a pair.

        MongoDB has no transaction that cannot write, so a pipeline holding
        a stage that writes is refused before it runs: a rule never writes,
        and the source is not written to."""
        from bson import json_util
        try:
            spec = json_util.loads(question)
        except ValueError:
            raise RuntimeError("a MongoDB rule is an aggregation pipeline:"
                               " {collection: ..., pipeline: [...]}, not SQL")
        if not isinstance(spec, dict) or "collection" not in spec:
            raise RuntimeError("a MongoDB rule names its collection:"
                               " {collection: ..., pipeline: [...]}")

        def writes(node):
            if isinstance(node, dict):
                return any(k in self.WRITING_STAGES or writes(v)
                           for k, v in node.items())
            if isinstance(node, list):
                return any(writes(v) for v in node)
            return False
        pipeline = spec.get("pipeline") or []
        if writes(pipeline):
            raise RuntimeError("the rule's pipeline writes ($out or $merge),"
                               " and a rule may only read")
        coll = self._client(side)[self._d(side, db)][str(spec["collection"])]
        rows = []
        for doc in coll.aggregate(pipeline, allowDiskUse=True):
            row = []
            for k, v in doc.items():
                if k == "_id" and isinstance(v, dict):
                    row += list(v.values())
                else:
                    row.append(v)
            rows.append(tuple(row))
        return rows

    def _client_hashes(self, coll, flt=None):
        """Each document's md5 over the bytes the server keeps, hashed here:
        exact - a number's type and every digit, field order, everything a
        document is - where the server's id hash is not.

        Honours the same range filter as the server-side path, so the walk
        is restartable and bounded in memory too - a walk that reads the
        whole collection would reintroduce exactly the limit the ranges
        exist to remove. The documents are never decoded: what is hashed is
        what the server sent, as the copy carries it.
        """
        import hashlib
        out = {}
        for doc in self._raw(coll).find(flt or {}, sort=[("_id", 1)]):
            _id = self._plain_id(doc)
            out[repr(_id)] = (_id, hashlib.md5(
                doc.raw, usedforsecurity=False).hexdigest())
        return out

    @staticmethod
    def _raw(coll):
        """The collection, its documents read as the bytes the server sends."""
        from bson.raw_bson import RawBSONDocument
        return coll.with_options(codec_options=coll.codec_options
                                 .with_options(document_class=RawBSONDocument))

    @staticmethod
    def _plain_id(doc):
        """A raw document's `_id` as a decoded one names it, so both walks
        and the key files name a document alike."""
        import bson
        from bson.raw_bson import RawBSONDocument
        _id = doc["_id"]
        return bson.decode(_id.raw) if isinstance(_id, RawBSONDocument) \
            else _id

    def snapshot_state(self, db, state_dir, kind="all"):
        """The target's collections as they stand - type, options, indexes
        - set down whole or not at all before a repair begins, so a
        rollback has what the repair changed to go back to. A document
        store keeps no counters to put back; its rows are put back by the
        repair's own undo (`repair_io`)."""
        from ..durable import write_atomic
        write_atomic(state_dir / "dst-shape.txt",
                     repr(self._shape("dst", db)))

    def check_deep(self, db):
        s, t = self._client("src")[db], self._client("dst")[self._d("dst", db)]
        # the collections the hop takes that both sides hold, each by the
        # name it has on the target (`_taken`)
        mine, _ = self._taken(db)
        held = set(t.list_collection_names())
        tn = {a: b for a, b in mine if b in held}
        names = sorted(tn)

        def newest(coll, rows=None):
            d = coll.find_one(rows or {}, sort=[("_id", -1)],
                              projection={"_id": 1})
            return d["_id"] if d else None

        res = []
        ahead, behind = [], []
        n = 0
        for name in names:
            n += 1
            rows = self._rows_of(db, name)
            na, nb = newest(s[name], rows), newest(t[tn[name]], rows)
            try:
                if nb is not None and (na is None or nb > na):
                    ahead.append(name)
                elif na is not None and (nb is None or nb < na):
                    behind.append(name)
            except TypeError:
                continue
        if ahead:
            res.append(Result("deep", f"{db} boundary", "diff",
                              f"target newest _id AHEAD of source on:"
                              f" {', '.join(ahead[:5])}", "",
                              "writes landing on target or double-apply,"
                              " find the writer before cutover"))
        else:
            note = f"; {len(behind)} behind (replication lag)" if behind else ""
            res.append(Result("deep", f"{db} boundary", "ok",
                              f"newest _id checked on {n} collections,"
                              f" none ahead of source{note}"))

        # BSON type fidelity: a migration through mongoexport/mongoimport or a
        # careless mover silently changes a field's type - Decimal128 -> double
        # loses precision, Int64 -> Int32 overflows, ObjectId -> string breaks
        # _id equality, Date -> Timestamp shifts meaning. Compare the dominant
        # BSON type per field on each side (sampled, top-level fields).
        def field_types(coll):
            pipe = [
                {"$limit": 5000},
                {"$project": {"kv": {"$objectToArray": "$$ROOT"}}},
                {"$unwind": "$kv"},
                {"$group": {"_id": {"f": "$kv.k", "t": {"$type": "$kv.v"}},
                            "n": {"$sum": 1}}},
            ]
            hist = {}
            for row in coll.aggregate(pipe, allowDiskUse=True):
                hist.setdefault(row["_id"]["f"], {})[row["_id"]["t"]] = row["n"]
            return {f: max(d, key=d.get) for f, d in hist.items()}

        bad = []
        for name in names:
            if name.startswith("system."):
                continue
            sf, df = field_types(s[name]), field_types(t[tn[name]])
            for f, sty in sorted(sf.items()):
                dty = df.get(f)
                if dty and dty != sty:
                    bad.append(f"{name}.{f}: {sty} -> {dty}")
        res.append(Result("deep", f"{db} bson-types", "diff" if bad else "ok",
                          "; ".join(bad[:6]) if bad
                          else f"field BSON types match across {n} collections",
                          "", "re-migrate preserving types ($toDecimal/$toLong)"
                          " or add a $jsonSchema validator" if bad else ""))

        # null vs missing: {f: null} and an absent field are distinct in mongo,
        # but a migration can flip one into the other - breaking $exists queries
        # and sparse/partial-index membership. Doc counts stay equal either way,
        # so compare the explicit-null and absent buckets per field.
        nm = []
        for name in names:
            if name.startswith("system."):
                continue
            sc, tc = s[name], t[tn[name]]
            fields = (set(field_types(sc)) | set(field_types(tc))) - {"_id"}
            for f in sorted(fields)[:40]:
                sn = sc.count_documents({f: {"$type": "null"}})
                dn = tc.count_documents({f: {"$type": "null"}})
                sa = sc.count_documents({f: {"$exists": False}})
                da = tc.count_documents({f: {"$exists": False}})
                if sn != dn or sa != da:
                    nm.append(f"{name}.{f}: src null/absent={sn}/{sa}"
                              f" dst={dn}/{da}")
        res.append(Result("deep", f"{db} null-missing", "diff" if nm else "ok",
                          "; ".join(nm[:6]) if nm
                          else "explicit-null vs absent consistent", "",
                          "preserve explicit null vs absent per field on load"
                          if nm else ""))

        # capped collection: a smaller size (or lost capped flag) on the target
        # silently rolls old docs off the head FIFO - history vanishes, no error.
        sopts = {c["name"]: c.get("options", {}) for c in s.list_collections()}
        topts = {c["name"]: c.get("options", {}) for c in t.list_collections()}
        cap = []
        for name in names:
            so, to = sopts.get(name, {}), topts.get(tn[name], {})
            if not so.get("capped") and not to.get("capped"):
                continue
            if bool(so.get("capped")) != bool(to.get("capped")):
                cap.append(f"{name}: capped {bool(so.get('capped'))} ->"
                           f" {bool(to.get('capped'))}")
            elif so.get("size") and to.get("size") and to["size"] < so["size"]:
                cap.append(f"{name}: capped size {so['size']} -> {to['size']}"
                           " (rolls old docs off the head)")
        res.append(Result("deep", f"{db} capped", "diff" if cap else "ok",
                          "; ".join(cap[:5]) if cap
                          else "capped collections match", "",
                          "recreate the capped collection with size >= source"
                          if cap else ""))

        # index parity for the attributes that silently lose or delete data:
        # a dropped `unique` admits duplicates, a changed `expireAfterSeconds`
        # (TTL) either stops expiry or starts deleting, a `partialFilterExpression`
        # change moves what the index covers. And a unique index that lands on a
        # case/accent-insensitive collation on the target COLLAPSES distinct
        # source values into duplicates on load - silent data loss.
        def indexes(coll):
            from pymongo.errors import OperationFailure
            try:
                return {ix["name"]: ix for ix in coll.list_indexes()}
            except OperationFailure as e:
                # a view has no indexes, and the server refuses to list them
                if e.code == 166:
                    return {}
                raise
        ixbad = []
        for name in names:
            if name.startswith("system."):
                continue
            si, di = indexes(s[name]), indexes(t[tn[name]])
            for ixn, spec in si.items():
                if ixn == "_id_":
                    continue
                d = di.get(ixn)
                if not d:
                    ixbad.append(f"{name}.{ixn}: index missing on target")
                    continue
                if bool(spec.get("unique")) != bool(d.get("unique")):
                    ixbad.append(f"{name}.{ixn}: unique"
                                 f" {bool(spec.get('unique'))} ->"
                                 f" {bool(d.get('unique'))}")
                if spec.get("expireAfterSeconds") != d.get("expireAfterSeconds"):
                    ixbad.append(f"{name}.{ixn}: TTL"
                                 f" {spec.get('expireAfterSeconds')} ->"
                                 f" {d.get('expireAfterSeconds')}")
                if spec.get("partialFilterExpression") != \
                        d.get("partialFilterExpression"):
                    ixbad.append(f"{name}.{ixn}: partial filter differs")
                ss = (spec.get("collation") or {}).get("strength")
                ds = (d.get("collation") or {}).get("strength")
                if ss != ds:
                    ixbad.append(f"{name}.{ixn}: collation strength"
                                 f" {ss} -> {ds}")
            # collapse: a unique index on a ci/ai collation on the target
            for ixn, d in di.items():
                if ixn == "_id_" or not d.get("unique"):
                    continue
                strength = (d.get("collation") or {}).get("strength")
                if strength is None or strength > 2:
                    continue
                keys = list((d.get("key") or {}).keys())
                if len(keys) != 1:
                    continue
                f = keys[0]
                try:
                    r = list(s[name].aggregate([
                        {"$group": {"_id": {"$toLower": f"${f}"},
                                    "n": {"$sum": 1}}},
                        {"$match": {"n": {"$gt": 1}}},
                        {"$count": "c"}]))
                    c = r[0]["c"] if r else 0
                except Exception:
                    c = 0
                if c > 0:
                    ixbad.append(f"{name}.{f}: {c} source groups COLLAPSE"
                                 " under the target's case-insensitive unique"
                                 " index (data loss)")
        res.append(Result("deep", f"{db} indexes", "diff" if ixbad else "ok",
                          "; ".join(ixbad[:6]) if ixbad
                          else "index unique/TTL/partial/collation match", "",
                          "recreate the target index with the source's"
                          " options (match collation to avoid collapse)"
                          if ixbad else ""))

        # sharded cluster: an interrupted moveChunk leaves orphaned docs, a
        # merge/reshard with a non-_id shard key can duplicate _id across
        # shards, and a running balancer makes counts a moving target. On a
        # standalone / replica-set this is a clean skip, not a false OK.
        # each read planned and said where it could not be made: a failed
        # "is it sharded?" read as "not sharded", a failed balancer read as
        # "idle", a failed duplicate-_id read as "no duplicates" - each an
        # `ok` over nothing (audit LG2)
        from ..util import Probes
        asked = Probes("sharding reads")
        sharded = {}
        for side in ("src", "dst"):
            with asked.reading(f"{side} topology"):
                sharded[side] = self._client(side).admin.command(
                    "hello").get("msg") == "isdbgrid"
        if not any(sharded.values()):
            status, detail = asked.verdict(
                "ok", "not a sharded cluster (standalone/replica-set)")
            res.append(Result("deep", f"{db} sharding", status, detail))
        else:
            sh = []
            with asked.reading("target balancer"):
                if self._client("dst").admin.command(
                        "balancerStatus").get("inBalancerRound"):
                    sh.append("balancer running on target - counts/orphans are"
                              " a moving target; sh.stopBalancer() before"
                              " verify")
            for name in names:
                if name.startswith("system."):
                    continue
                with asked.reading(f"{name} duplicate _id"):
                    r = list(t[tn[name]].aggregate(
                        [{"$group": {"_id": "$_id", "n": {"$sum": 1}}},
                         {"$match": {"n": {"$gt": 1}}}, {"$count": "c"}],
                        allowDiskUse=True))
                    if r and r[0]["c"] > 0:
                        sh.append(f"{name}: {r[0]['c']} duplicate _id across"
                                  " shards (merge/reshard collision)")
            status, detail = asked.verdict(
                "diff" if sh else "ok",
                "; ".join(sh[:5]) if sh
                else "sharded: no dup _id, balancer idle")
            res.append(Result("deep", f"{db} sharding", status, detail, "",
                              "cleanupOrphaned / dedupe and stop the"
                              " balancer before cutover" if sh else ""))
        res += self._value_checks(db, tn)
        res.append(self._grants(db, tn))
        res += [r for r in (self.set_aside(db),) if r]
        return res

    # --- what a value became, and who may read it ---------------------------

    #: a value of the first BSON type held as the second keeps fewer of its
    #: digits, and what is lost
    LOSES_DIGITS = {
        ("decimal", "double"): "a decimal128 keeps 34 digits, a double 15"
                               " to 17",
        ("decimal", "long"): "the fraction is cut",
        ("decimal", "int"): "the fraction is cut",
        ("double", "long"): "the fraction is cut",
        ("double", "int"): "the fraction is cut",
        ("long", "double"): "past 2^53 a double cannot tell one 64-bit"
                            " integer from the next"}
    #: types whose text is what they are: every other type is rendered
    #: (`_renderings`) by a rule of the driver that reads it
    PLAIN_TYPES = frozenset({"string", "int", "long", "double", "bool",
                             "null", "object", "array", "objectId",
                             "binData"})
    #: fields a deep check samples at most - a bound that is said where
    #: it cuts
    DEEP_FIELDS = 20

    def _value_checks(self, db, tn):
        """The deep checks of what a document's values became on the
        target, each over the types of every field of every collection
        (`field_types` - the whole collection, never a sample, so a field
        in one document of a million is read too): a 64-bit integer held as
        a 32-bit one (narrowing), a decimal128 or a long past 2^53 held as
        a double (precision), a collation changed (collation), the text of
        the types a driver renders by its own rule (rendering), dates moved
        by a zone's offset (time zone) and replacement characters a lossy
        transcode left (character set). A collection whose types could not
        be read is said, never passed."""
        from .. import verdict
        types, unread = {}, []
        for name in sorted(tn):
            try:
                types[name] = (self.field_types("src", db, name),
                               self.field_types("dst", db, tn[name]))
            except Exception as e:  # noqa: BLE001 - a view, or not allowed
                unread.append(f"{name} ({str(e).splitlines()[0][:60]})")
        out = [verdict.narrowing_result(db, self._narrowed(types)),
               self._precision(db, types), self._collation(db, tn),
               self._renderings(db, tn, types),
               verdict.timeshift_result(db, self._shifted(db, tn, types)),
               self._replacement(db, tn, types)]
        if unread:
            out.append(Result("deep", f"{db} narrowing", "warn",
                              "the types of the fields of "
                              + ", ".join(unread[:5]) + " could not be"
                              " read, so narrowing, precision, rendering,"
                              " time shift and replacement characters were"
                              " not judged there"))
        return out

    def _narrowed(self, types):
        """Lines naming a field the source holds as a 64-bit integer that
        the target holds only as a 32-bit one (`verdict.narrowing`, the
        reasoning every engine's check asks, over `canon.INT_RANGES`)."""
        from .. import verdict
        out = []
        for name, (sf, df) in sorted(types.items()):
            for field, (stypes, _, _) in sorted(sf.items()):
                there = set((df.get(field) or ([], 0, 0))[0]) - {"null"}
                for have in sorted(set(stypes) - {"null"} - there):
                    for held in sorted(there):
                        why = verdict.narrowing("mongodb", have, held)
                        if why:
                            out.append(f"{name}.{field}: {why}")
        return out

    def _precision(self, db, types):
        """A field whose values the target holds in a type with fewer
        digits (`LOSES_DIGITS`): a decimal128 as a double, a long as a
        double. The row comparison sees the bytes change; this names the
        field and says what was lost."""
        lost = []
        for name, (sf, df) in sorted(types.items()):
            for field, (stypes, _, _) in sorted(sf.items()):
                there = set((df.get(field) or ([], 0, 0))[0]) - {"null"}
                for have in sorted(set(stypes) - there):
                    for held in sorted(there):
                        why = self.LOSES_DIGITS.get((have, held))
                        if why:
                            lost.append(f"{name}.{field}: {have} -> {held}"
                                        f" ({why})")
        if lost:
            return Result("deep", f"{db} float", "diff", "; ".join(lost[:6]),
                          "", "keep the type: move the field as a decimal128"
                              " or a long, not through a double")
        return Result("deep", f"{db} float", "ok",
                      f"no field holds fewer digits on the target, over"
                      f" the types of {len(types)} collections")

    def _collation(self, db, tn):
        """A collection's default collation, or an index's, that is not the
        source's: it orders, compares and de-duplicates text another way
        (the ICU version a server reports is its own and is not compared)."""
        s, t = self._client("src")[db], self._client("dst")[self._d("dst", db)]
        sopts = {c["name"]: c.get("options", {})
                 for c in s.list_collections()}
        topts = {c["name"]: c.get("options", {})
                 for c in t.list_collections()}

        def spec(got):
            return {k: v for k, v in (got or {}).items() if k != "version"}
        bad = []
        for name in sorted(tn):
            a = spec(sopts.get(name, {}).get("collation"))
            b = spec(topts.get(tn[name], {}).get("collation"))
            if a != b:
                bad.append(f"{name}: default collation src={a or 'binary'}"
                           f" dst={b or 'binary'}")
            si = {ix["name"]: spec(ix.get("collation"))
                  for ix in s[name].list_indexes()}
            di = {ix["name"]: spec(ix.get("collation"))
                  for ix in t[tn[name]].list_indexes()}
            for ixn in sorted(set(si) & set(di)):
                if si[ixn] != di[ixn]:
                    bad.append(f"{name}.{ixn}: index collation"
                               f" src={si[ixn] or 'binary'}"
                               f" dst={di[ixn] or 'binary'}")
        if bad:
            return Result("deep", f"{db} collation", "diff",
                          "; ".join(bad[:6]), "",
                          "create the collection and its indexes on the"
                          " target with the source's collation")
        return Result("deep", f"{db} collation", "ok",
                      f"default and index collations match on {len(tn)}"
                      " collections")

    def _renderings(self, db, tn, types):
        """The text of a few values of each type a driver renders by a rule
        of its own (a decimal128's scale, a date, a UUID's byte order, a
        regular expression, a timestamp), as Extended JSON, asked of both
        sides for the same `_id`s: a value the target would show another
        way is named. A sample of five documents of each of at most
        `DEEP_FIELDS` fields - the row comparison, by the stored bytes, is
        the one that is exact."""
        from bson.json_util import CANONICAL_JSON_OPTIONS, dumps
        s, t = self._client("src")[db], self._client("dst")[self._d("dst", db)]

        def show(value):
            return dumps(value, json_options=CANONICAL_JSON_OPTIONS)
        fields = [(name, field, ty) for name in sorted(types)
                  for field, (stypes, _, _) in sorted(types[name][0].items())
                  for ty in stypes if ty not in self.PLAIN_TYPES]
        drifts, audited = [], 0
        for name, field, ty in fields[:self.DEEP_FIELDS]:
            kind = "binData" if ty == "uuid" or ty.startswith("binData") \
                else ty
            got = {repr(d["_id"]): (d["_id"], d[field])
                   for d in s[name].find({field: {"$type": kind}},
                                         {field: 1}).limit(5)
                   if field in d}
            if not got:
                continue
            audited += 1
            ids = [i for i, _ in got.values()]
            held = {repr(d["_id"]): d[field]
                    for d in t[tn[name]].find({"_id": {"$in": ids}},
                                              {field: 1}) if field in d}
            for key, (_, value) in got.items():
                if key in held and show(held[key]) != show(value):
                    drifts.append(
                        f"{name}.{field}: renders differently (src"
                        f" {show(value)[:40]!r} dst {show(held[key])[:40]!r})")
                    break
        cut = max(len(fields) - self.DEEP_FIELDS, 0)
        if drifts:
            return Result("deep", f"{db} render", "diff",
                          "; ".join(drifts[:5]), "",
                          "check how the target's driver and version read"
                          " and write these types")
        return Result("deep", f"{db} render", "ok",
                      (f"{audited} fields of types a driver renders by its"
                       " own rule sampled, rendering identical"
                       if audited else "no field of a type a driver"
                                       " renders by its own rule")
                      + (f"; {cut} more fields not sampled" if cut else ""))

    @staticmethod
    def _epoch(moment):
        """A BSON date as seconds since 1970: a date is an instant in UTC,
        which the driver reads without a zone."""
        import calendar
        return calendar.timegm(moment.utctimetuple()) \
            + moment.microsecond / 1e6

    def _shifted(self, db, tn, types):
        """Lines naming a date field every document of which the target
        holds shifted by the same number of seconds (or by two, an hour
        apart: a zone with daylight saving) - a mover that wrote a local
        wall clock where a UTC instant belongs (`verdict.uniform_shift`).
        The first 200 documents of at most ten fields are compared by
        `_id`."""
        from .. import verdict
        s, t = self._client("src")[db], self._client("dst")[self._d("dst", db)]
        fields = [(name, field) for name in sorted(types)
                  for field, (stypes, _, _) in sorted(types[name][0].items())
                  if "date" in stypes
                  and "date" in (types[name][1].get(field) or ([], 0, 0))[0]]
        shifts = []
        for name, field in fields[:10]:
            try:
                src = {repr(d["_id"]): (d["_id"], self._epoch(d[field]))
                       for d in s[name].find(
                           {field: {"$type": "date"}}, {field: 1})
                       .sort("_id", 1).limit(200)}
                ids = [i for i, _ in src.values()]
                dst = {repr(d["_id"]): self._epoch(d[field])
                       for d in t[tn[name]].find(
                           {"_id": {"$in": ids}, field: {"$type": "date"}},
                           {field: 1})}
            except (OverflowError, ValueError):
                continue    # a date past what the driver can read
            line = verdict.uniform_shift(
                f"{name}.{field}", {k: v for k, (_, v) in src.items()}, dst)
            if line:
                shifts.append(line)
        return shifts

    def _replacement(self, db, tn, types):
        """U+FFFD, the replacement character a lossy transcode leaves for
        what it could not read: strings are UTF-8 by the format's own
        definition, so there is no character set to differ - an excess of
        the character on the target over the source is the damage. Each
        top-level string field of at most `DEEP_FIELDS * 2` is counted by
        the documents it is in."""
        s, t = self._client("src")[db], self._client("dst")[self._d("dst", db)]
        fields = [(name, field) for name in sorted(types)
                  for field, (stypes, _, _) in sorted(types[name][0].items())
                  if "string" in stypes]
        bad = []
        for name, field in fields[:self.DEEP_FIELDS * 2]:
            query = {field: {"$regex": "�"}}
            was = s[name].count_documents(query)
            now = t[tn[name]].count_documents(query)
            if now > was:
                bad.append(f"{name}.{field}: {now - was} more documents"
                           " with a replacement character on the target")
        cut = max(len(fields) - self.DEEP_FIELDS * 2, 0)
        if bad:
            return Result("deep", f"{db} encoding", "diff",
                          "; ".join(bad[:6]), "",
                          "copy the affected documents again with a reader"
                          " that does not transcode")
        return Result("deep", f"{db} encoding", "ok",
                      f"no more replacement characters on the target, over"
                      f" {min(len(fields), self.DEEP_FIELDS * 2)} top-level"
                      " string fields"
                      + (f"; {cut} more fields not counted" if cut else ""))

    def _role_privileges(self, side, db, tn):
        """{role: {(collection, action)}} the roles made in the database
        hold on its collections the hop takes ("" - the whole database):
        what `rolesInfo` shows of the privileges of the roles that are not
        built in. A collection is named as the source names it."""
        name = self._d(side, db)
        got = self._client(side)[name].command(
            "rolesInfo", 1, showPrivileges=True, showBuiltinRoles=False)
        names = {b: a for a, b in tn.items()} if side == "dst" \
            else {a: a for a in tn}
        out = {}
        for role in got.get("roles", []):
            held = set()
            for priv in role.get("privileges", []):
                resource = priv.get("resource") or {}
                if resource.get("db") != name:
                    continue
                coll = resource.get("collection", "")
                if coll and coll not in names:
                    continue
                held.update((names.get(coll, ""), a)
                            for a in priv.get("actions", []))
            out[role["role"]] = held
        return out

    def _grants(self, db, tn):
        """What may be done with the collections, which MongoDB keeps as
        roles on resources and not as grants on a table: the accounts that
        hold a role on this database (or on every one) and the privileges
        of the roles made in it, source against target. A role or a
        privilege the source has and the target lacks is the gap; one the
        target holds beyond the source is the target's own. Only the
        accounts and roles the hop's `objects` carries (`roles`) are
        compared."""
        from .. import users
        tdb = self._d("dst", db)
        try:
            mine = users._mongo_users(self.hop.source)
            there = users._mongo_users(self.hop.target)
            owned = self._role_privileges("src", db, tn)
            held = self._role_privileges("dst", db, tn)
        except Exception as e:  # noqa: BLE001 - not allowed, or no command
            return Result("deep", f"{db} grants", "skip",
                          "the accounts and roles could not be read: "
                          + str(e).splitlines()[0][:100])
        gaps, compared = [], 0
        for (udb, user), roles in sorted(mine.items()):
            reach = sorted({(r, tdb if d == db else d) for r, d in roles
                            if d in (db, "admin")})
            key = tdb if udb == db else udb
            if not reach or not users.role_taken(self.hop, f"{udb}.{user}"):
                continue
            compared += 1
            if (key, user) not in there:
                gaps.append(f"{user}: holds {[r for r, _ in reach]} on the"
                            " source and is missing on the target")
                continue
            short = users._mongo_role_gap(reach, there[(key, user)])
            if short:
                gaps.append(f"{user}: lacks {sorted(r for r, _ in short)}"
                            " on the target")
        for role, privileges in sorted(owned.items()):
            if not self.hop.takes_object("roles", role):
                continue
            compared += 1
            if role not in held:
                gaps.append(f"role {role}: missing on the target")
                continue
            short = sorted(privileges - held[role])
            if short:
                gaps.append(f"role {role}: lacks "
                            + ", ".join(f"{a} on {c or 'every collection'}"
                                        for c, a in short[:4])
                            + (f" and {len(short) - 4} more"
                               if len(short) > 4 else "") + " on the target")
        if gaps:
            return Result("deep", f"{db} grants", "diff", "; ".join(gaps[:6]),
                          "", "grant the target's accounts and roles what"
                              " the source's hold (migkit users create)")
        return Result("deep", f"{db} grants", "ok",
                      f"{compared} accounts and roles hold on the target"
                      " what they hold on the source over these collections")

    # delta verify: re-check only _ids touched since the saved change-stream
    # token, which advances only on a clean verify (idempotent)
    #: what a standalone server answers when asked for a change stream
    NO_CHANGE_STREAM = "only supported on replica sets"

    def _no_stream_result(self, db, check, error):
        """A standalone MongoDB cannot be tailed, and says so plainly.

        Measured against `mongo:7` started on its own: `$changeStream stage
        is only supported on replica sets`. A delta reads the oplog through a
        change stream, and a standalone keeps no oplog, so there is nothing
        to read - which is a sentence an operator can act on rather than a
        driver exception arriving from four frames down.
        """
        return [Result(check, db, "error",
                       "this MongoDB is a standalone, so it keeps no oplog"
                       " and migkit cannot follow its changes"
                       f" ({str(error).splitlines()[0][:80]})", "",
                       "run `migkit check` for a full comparison, or make it"
                       " a single-node replica set (`--replSet` plus"
                       " `rs.initiate()`) if you want delta and CDC")]

    def delta_verify(self, db, limit=20000, log=None):
        from bson.json_util import dumps, loads
        from pymongo.errors import OperationFailure
        state = self.hop.report_dir(db) / "delta-token.json"
        src = self._client("src")[db]
        dst = self._client("dst")[self._d("dst", db)]
        if not state.exists():
            try:
                with src.watch() as stream:
                    stream.try_next()
                    state.write_text(dumps(stream.resume_token))
            except OperationFailure as e:
                if self.NO_CHANGE_STREAM in str(e):
                    return self._no_stream_result(db, "delta", e)
                raise
            return [Result("delta", db, "ok",
                           "resume token recorded, changes are tracked"
                           " from this point on")]
        token = loads(state.read_text())
        touched = {}
        n = 0
        end_token = token
        # if the saved token has fallen off the oplog window, resuming from a
        # fresh token would silently SKIP every change in the gap and report
        # "in sync". Detect ChangeStreamHistoryLost (code 286), drop the token,
        # and demand a full re-baseline instead of a false green.
        from pymongo.errors import OperationFailure
        try:
            with src.watch(resume_after=token) as stream:
                while n < limit:
                    ev = stream.try_next()
                    if ev is None:
                        break
                    n += 1
                    end_token = ev["_id"]
                    coll = ev.get("ns", {}).get("coll")
                    key = ev.get("documentKey", {}).get("_id")
                    # only the collections the hop takes are compared
                    if coll and key is not None and self.in_scope(db, coll):
                        touched.setdefault(coll, {})[repr(key)] = key
        except OperationFailure as e:
            msg = str(e).lower()
            if self.NO_CHANGE_STREAM in str(e):
                return self._no_stream_result(db, "delta", e)
            if getattr(e, "code", None) == 286 \
                    or "no longer be in the oplog" in msg \
                    or "changestreamhistorylost" in msg:
                state.unlink(missing_ok=True)
                return [Result("delta", db, "diff",
                               "change-stream token expired (oplog window"
                               " exceeded) - the gap since the last verified"
                               " point cannot be replayed, so 'in sync' would"
                               " be a lie", "",
                               "run a full check to re-baseline; grow the"
                               " oplog to cover the migration window")]
            raise
        if not touched:
            state.write_text(dumps(end_token))
            return [Result("delta", db, "ok",
                           "0 changes since last verified token")]
        res = []
        clean = True
        for coll, ids in sorted(touched.items()):
            bad = []
            # each document apart said as missing, only on the target or
            # changed: a hop that keeps what the source deletes is held to
            # which (`verdict.hold`)
            apart = {"missing": [], "extra": [], "changed": []}
            # by the stored bytes: decoded, 5 == 5.0 == NumberLong(5) and
            # 1 == True in Python, and two dicts equal in any field order
            one = self._raw(src[coll])
            two = self._raw(dst[self._there(db, coll)])
            for _id in ids.values():
                a = one.find_one({"_id": _id})
                b = two.find_one({"_id": _id})
                if (a and a.raw) != (b and b.raw):
                    bad.append(_id)
                    apart["missing" if b is None else "extra" if a is None
                          else "changed"].append(_id)
            if bad:
                d = self.hop.report_dir(db)
                for kind, got in apart.items():
                    p = d / f"data-{coll}.{kind}"
                    if got:
                        p.write_text("\n".join(dumps(i) for i in got) + "\n")
                    elif p.exists():
                        p.unlink()
                answer, held_back = self._delta_held(Result(
                    "delta", f"{db}.{coll}", "diff",
                    f"of {len(ids)} touched docs, {len(bad)} differ:"
                    f" missing={len(apart['missing'])}"
                    f" extra={len(apart['extra'])}"
                    f" changed={len(apart['changed'])}",
                    str(d),
                    f"migkit sync {self.hop.name} --db {db} --kind rows",
                    table=str(coll), rows_apart=rows_counted(
                        coll, len(apart["missing"]), len(apart["extra"]),
                        len(apart["changed"]))))
                clean = clean and not held_back
                res.append(answer)
            else:
                res.append(Result("delta", f"{db}.{coll}", "ok",
                                  f"{len(ids)} touched docs verified"
                                  " equal on both sides"))
            if log:
                log(f"{coll}: {len(ids)} touched, "
                    + self._delta_said(res[-1]))
        if clean:
            state.write_text(dumps(end_token))
            note = "token advanced"
        else:
            note = "token NOT advanced, window replays next cycle"
        if n >= limit:
            note += f"; window truncated at {limit} events, more pending"
        res.insert(0, Result("delta", db, "ok" if clean else "diff",
                             f"{sum(len(v) for v in touched.values())}"
                             f" changed docs across {len(touched)}"
                             f" collections, {note}"))
        return res

    def repair_plan(self, db, kind):
        if kind not in ("rows", "all"):
            return []
        d = self.hop.report_dir(db)
        names = sorted({f.name.split(".")[0][len("data-"):]
                        for k in ("missing", "extra", "changed")
                        for f in d.glob(f"data-*.{k}")})
        # a drilldown of a collection the hop has since left out is said,
        # never acted on
        names, said = self._drills_in_scope(db, names)
        actions = [said] if said else []
        for name in names:
            counts, named = [], {}
            for k in ("missing", "extra", "changed"):
                f = d / f"data-{name}.{k}"
                if f.exists():
                    named[k] = range(sum(1 for _ in f.open()))
                    counts.append(f"{k}={len(named[k])}")
            # the documents the hop keeps by the changes it does not carry
            # are left as they are, and said
            left = self._repairable(db, name, named)
            kept = self._kept_said(named, left)
            if kept and not any(left.values()):
                continue
            actions.append(RepairAction(
                db, "rows", [f"resync docs for {name} ({', '.join(counts)})"],
                [], f"{name}: replace/insert from source per _id, deleted and"
                    " overwritten target docs saved to undo first"
                    + (f"; {kept}" if kept else ""),
                table=name))
        return actions

    #: a document is put back as Extended JSON by its own record
    #: (`repair_io`), never as canonical text: repaired by `_id` alone
    ROWS_PUT_BACK = False

    def repair_found(self, db, table):
        """The `_id`s the last check named, each as its Extended JSON -
        never the ones the hop keeps (`_repairable`)."""
        return self._repairable(db, table, {
            kind: [(line,) for line in self._read_drill(db, table, kind)]
            for kind in ("missing", "changed", "extra")})

    def repair_sides(self, db, table):
        """None: a collection's documents are lined up by `_id` alone, the
        rows way (`repairways`)."""
        return None

    def apply(self, db, action):
        from bson.json_util import dumps, loads
        name = action.statements[0].split()[3]
        d = self.hop.report_dir(db)
        s = self._client("src")[db][name]
        t = self._client("dst")[self._d("dst", db)][self._there(db, name)]
        undo = d / "undo"
        undo.mkdir(exist_ok=True)

        kept = self._kept_by_hop(db, name)

        def read(kind):
            # a document the hop keeps by a change it does not carry is
            # never removed, added or overwritten here
            f = d / f"data-{name}.{kind}"
            return [loads(l) for l in f.read_text().splitlines()] \
                if f.exists() and kind not in kept else []

        from .. import repairlog
        extra, missing = read("extra"), read("missing")
        changed = read("changed")
        if changed and self.newer_wins_column():
            changed = self._newer_docs(name, s, t, changed, d)
        # every statement written down before any lands (`repairlog`):
        # the documents by `_id`, as the target holds them and as the
        # source's will be
        io = repairlog.DocumentIO(t, name)
        keys = [(repairlog.doc_text(i),) for i in missing + changed + extra]
        before = io.rows(keys)
        after = repairlog.DocumentIO(s, name).rows(
            keys[:len(missing) + len(changed)])
        with repairlog.Record(undo, name, "document", (), ["_id"]) as rec:
            num = {}
            for k in keys[len(missing) + len(changed):]:
                num[k] = rec.planned("delete", k, before.get(k), None)
            for k in keys[:len(missing) + len(changed)]:
                if k in after:
                    num[k] = rec.planned("write", k, before.get(k), after[k])
            rec.ready()
            with (undo / f"{name}.docs.jsonl").open("a") as uf:
                for _id in extra:
                    doc = t.find_one({"_id": _id})
                    if doc is not None:
                        uf.write(dumps(doc) + "\n")
                    t.delete_one({"_id": _id})
                    rec.applied(num.get((repairlog.doc_text(_id),)))
                for _id in missing + changed:
                    old = t.find_one({"_id": _id})
                    if old is not None:
                        uf.write(dumps(old) + "\n")
                    doc = s.find_one({"_id": _id})
                    if doc is not None:
                        t.replace_one({"_id": _id}, doc, upsert=True)
                        rec.applied(num.get((repairlog.doc_text(_id),)))

    def repair_io(self, db, head):
        """A repair record's documents on the target, by `_id`."""
        from .. import repairlog
        if head.get("form") != "document":
            return super().repair_io(db, head)
        return repairlog.DocumentIO(
            self._client("dst")[self._d("dst", db)]
            [self._there(db, head["table"])], head["table"])

    def _newer_docs(self, name, src, dst, changed, report):
        """The changed `_id`s to overwrite under `newer_wins`: a document
        whose field the hop names is later on the target is kept, and its
        `_id` written to `data-<collection>.kept-newer`."""
        from bson.json_util import dumps
        col = self.newer_wins_column()
        by = {dumps(i): i for i in changed}

        def read(side, keys):
            coll = src if side == "src" else dst
            out = {}
            ids = [by[k] for k in keys]
            for i in range(0, len(ids), 1000):
                for doc in coll.find({"_id": {"$in": ids[i:i + 1000]}},
                                     {col: 1}):
                    if doc.get(col) is not None:
                        out[dumps(doc["_id"])] = doc[col]
            return out
        overwrite, kept = self._keep_the_newer(name, list(by), read)
        if kept:
            (report / f"data-{name}.kept-newer").write_text(
                "".join(k + "\n" for k in kept))
        return [by[k] for k in overwrite]

    def _assess_extra(self):
        """What a document store has to say beside what every engine's
        assessment does (`Engine.assess`: the brands, the two servers'
        versions held to each other, the connections, the pre-flight
        checks, the scope and the writers already on the target): the
        work the move will not do, what a mover left on the source, and
        the programs the move would run."""
        inv = self._handwork()
        return (inv.rows() + inv.summary() + self._mover_leftovers()
                + self._bulk_path_rows())

    #: what `assess` asks before anything moves (`Engine._preflight_items`)
    PREFLIGHT = ("_stream_ready", "_features_the_target_lacks")

    def _stream_ready(self, db):
        """Whether the source can be followed once it is copied: a change
        stream is read from the oplog of a replica set (or the router of a
        sharded cluster), and a standalone server has none. How far back
        the oplog reaches is said with it - the copy must be followed from
        a point it still holds. Asked of the source only, and read-only."""
        from pymongo.errors import PyMongoError
        scope = f"{db} change stream"
        try:
            client = self._client("src")
            hello = client.admin.command("hello")
        except PyMongoError as e:
            return Result("schema", scope, "error",
                          f"could not ask the source: {str(e)[:80]}")
        on = hello.get("setName")
        sharded = hello.get("msg") == "isdbgrid"
        if not on and not sharded:
            return Result(
                "schema", scope, "warn",
                "the source is a standalone server and keeps no oplog, so"
                " its changes cannot be followed after the copy; only a"
                " full copy is possible",
                fix_hint="run the source as a replica set (a single member"
                         " is enough) to follow it online")
        if sharded:
            return Result("schema", scope, "ok",
                          "a sharded cluster: its changes are read through"
                          " its router")
        try:
            oldest = next(iter(client.local["oplog.rs"].find({}, {"ts": 1})
                               .sort("$natural", 1).limit(1)), None)
        except PyMongoError:
            oldest = None
        now = hello.get("operationTime")
        if oldest is None or now is None:
            return Result("schema", scope, "warn",
                          f"replica set {on}: how far back its oplog"
                          " reaches could not be read with this account -"
                          " unknown, not clean")
        window = max(int(now.time) - int(oldest["ts"].time), 0)
        return Result("schema", scope, "ok",
                      f"replica set {on}: its oplog holds the last"
                      f" {window / 3600:.1f} hours of changes; a copy is"
                      " followed from the point taken before it, which has"
                      " to stay inside that")

    #: the first server version that has each feature, from the server's
    #: own documentation: a collection or index that uses one cannot be
    #: made on an older server
    FEATURE_SINCE = {
        "wildcard index": (4, 2), "hidden index": (4, 4),
        "time-series collection": (5, 0), "clustered collection": (5, 3)}

    def _features_the_target_lacks(self, db):
        """The collections and indexes the source has that need a newer
        server than the target runs: made on it, they fail or arrive as
        something else. Read from the source's listing and the two
        servers' versions."""
        from pymongo.errors import PyMongoError
        scope = f"{db} features the target lacks"
        try:
            sv, dv = self._server_versions()
            used = self._features_used(db)
        except PyMongoError as e:
            return Result("schema", scope, "error",
                          f"could not ask: {str(e)[:80]}")
        try:
            have = tuple(int(x) for x in str(dv).split(".")[:2])
        except ValueError:
            have = None
        if have is None:
            return Result("schema", scope, "warn",
                          "the target's version could not be read - whether"
                          " it has what the source uses is unknown, not"
                          " clean")
        short = {f: names for f, names in used.items()
                 if have < self.FEATURE_SINCE[f]}
        if not short:
            return Result("schema", scope, "ok",
                          f"the target ({dv}) has every feature the source"
                          f" ({sv}) uses here")
        return Result(
            "schema", scope, "diff",
            "; ".join(
                f"{f} ({', '.join(sorted(names)[:3])}) needs"
                f" {'.'.join(map(str, self.FEATURE_SINCE[f]))} and the"
                f" target runs {dv}" for f, names in sorted(short.items())),
            fix_hint="move to a target of at least the source's version")

    def _features_used(self, db):
        """{feature (`FEATURE_SINCE`): the source's collections or indexes
        that use it}, of the collections the hop takes."""
        used = {}
        d = self._client("src")[self._d("src", db)]
        for c in d.list_collections():
            name = c["name"]
            if name.startswith("system.") or not self.in_scope(db, name):
                continue
            opts = c.get("options") or {}
            if c.get("type") == "timeseries" or "timeseries" in opts:
                used.setdefault("time-series collection", set()).add(name)
            if "clusteredIndex" in opts:
                used.setdefault("clustered collection", set()).add(name)
            if c.get("type") == "view":
                continue
            for ix in d[name].list_indexes():
                key = dict(ix.get("key") or {})
                if any(str(f).endswith("$**") for f in key):
                    used.setdefault("wildcard index", set()).add(
                        f"{name}.{ix['name']}")
                if ix.get("hidden"):
                    used.setdefault("hidden index", set()).add(
                        f"{name}.{ix['name']}")
        return used

    def stream_writers(self, db):
        """Adds what else writes into the target's database: the operations
        running on it just now, by the client that sent each (`currentOp`),
        and - for a writer between its operations at that instant - the
        writes the server counts on it over a short watch (`top`). None of
        them is migkit's to pause. The tail of migkit's own is the base's,
        and its writes are expected."""
        from pymongo.errors import PyMongoError
        out = super().stream_writers(db)
        blind = self.__dict__.setdefault("_writers_blind", {})
        try:
            who, wrote = self._writers_seen(db)
        except PyMongoError as e:
            # a repair goes on as it did; the assessment says it could not
            # ask (`_writer_items`)
            blind[db] = (str(e).splitlines() or [""])[0][:80]
            return out
        blind.pop(db, None)
        out += [(w, False) for w in who]
        if wrote and not who and not out:
            out.append((f"a writer the server does not name ({wrote} writes"
                        f" in {self.WRITE_WATCH:g}s)", False))
        return out

    def _writer_items(self, db):
        """The base's, but a target that would not say who writes to it is
        unknown and not clean - its `none found` would be a claim."""
        items = super()._writer_items(db)
        why = self.__dict__.get("_writers_blind", {}).get(db)
        if why and items and items[0]["level"] == "pass":
            items = [{**items[0], "level": "warn",
                      "detail": f"could not ask the target ({why}) -"
                                " unknown, not clean"}]
        return items

    #: seconds the target is watched for writes, and the looks at what is
    #: running in them (`stream_writers`)
    WRITE_WATCH, WRITE_LOOKS = 1.0, 5
    #: the commands of a running operation that write
    _WRITING = ("insert", "update", "delete", "findAndModify", "bulkWrite")

    def _writers_seen(self, db):
        """([who writes now], writes counted in the watch) on the target's
        database, from `$currentOp` (every account's operations) and
        `top` before and after. Bounded by `WRITE_WATCH`; a writer that
        is quiet for the whole watch is not seen, so a clear answer is
        about that window and no more."""
        name = self._d("dst", db)
        admin = self._client("dst").admin
        mine = re.compile("^" + re.escape(name) + r"\.")

        def counted():
            totals = admin.command("top").get("totals") or {}
            return sum(int((v.get(k) or {}).get("count") or 0)
                       for ns, v in totals.items() if mine.match(ns)
                       for k in ("insert", "update", "remove"))
        before = counted()
        who = set()
        for _ in range(self.WRITE_LOOKS):
            for op in admin.aggregate([
                    {"$currentOp": {"allUsers": True}},
                    {"$match": {"ns": {"$regex": mine.pattern}}}]):
                command = op.get("command") or {}
                if op.get("op") in ("insert", "update", "remove") or any(
                        k in command for k in self._WRITING):
                    who.add(f"{op.get('appName') or 'a client'} at"
                            f" {str(op.get('client') or '?').split(':')[0]}"
                            f" writing to {op.get('ns')}")
            time.sleep(self.WRITE_WATCH / self.WRITE_LOOKS)
        return sorted(who), max(counted() - before, 0)

    def _mover_leftovers(self):
        """What a mover added to the source and did not take away.

        MongoDB has no replication slot to pin storage the way PostgreSQL
        does, so nothing here is urgent - but a leftover database or
        collection is still evidence of which mover touched this deployment,
        and that matters when two of them ran and only one is admitted to.

        `system.*` is the server's own bookkeeping, never a mover's, so it is
        excluded before anything is matched.
        """
        from .. import leftovers as _lo
        found, items = [], []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "source leftovers",
                          "item": item, "detail": str(detail)})
        src = self._client("src")
        try:
            dbs = [d for d in src.list_database_names() if d not in SKIP_DBS]
        except Exception as e:
            add("warn", "cannot list databases on the source",
                f"{str(e)[:90]} - unknown, not clean")
            return items
        for name in dbs:
            found.append(("database", name))
        for db in dbs:
            try:
                for coll in src[db].list_collection_names():
                    if not coll.startswith("system."):
                        found.append(("collection", coll))
            except Exception as e:
                add("warn", f"cannot list collections in {db}",
                    f"{str(e)[:90]} - unknown, not clean")
        by = _lo.group(found)
        if not by:
            add("pass", "no mover artifacts left in the source",
                f"{len(found)} objects examined")
            return items
        add("warn", f"{_lo.total(by)} mover artifacts left in the source",
            _lo.describe(by) + ". Drop them once every leg that used them is"
            " finished - they are on the source, so nothing on the target"
            " tells you they are there")
        return items

    def _handwork(self):
        """Inventory the work the move will not do, per `migkit.handwork`.

        Views are absent on purpose: `_shape` records a collection's type, so
        the schema check already reports a view that arrived as a collection.
        What is listed here is what an insert-by-insert copy silently gets
        wrong rather than fails on - a capped collection that lands uncapped
        and grows without a bound, a time-series collection that lands as an
        ordinary one holding the same documents.
        """
        from .. import handwork
        inv = handwork.Inventory()
        src = self._client("src")
        try:
            sharded_cluster = src.admin.command("hello").get("msg") == "isdbgrid"
            cluster_known = True
        except Exception as e:
            sharded_cluster, cluster_known = False, False
            reason = str(e)[:100]
        for db in self.databases():
            try:
                d = src[self._d("src", db)]
                colls = d.command("listCollections")["cursor"]["firstBatch"]
            except Exception as e:
                inv.unknown("target-prereq", db,
                            f"cannot list collections: {str(e)[:100]}")
                continue
            # system.views and system.buckets.* are the server's own
            # bookkeeping; naming them would be work nobody can do
            colls = [c for c in colls
                     if not c["name"].startswith("system.")]

            # created capped or not at all: an insert-by-insert copy into a
            # collection that does not exist yet creates an ordinary one, and
            # the size bound is gone without any error
            inv.add("target-prereq", db, "capped collections",
                    [f"{c['name']} ({c['options']['capped'] and 'capped'},"
                     f" size {c['options'].get('size', '?')})"
                     for c in colls if c.get("options", {}).get("capped")])
            # same shape of failure: the timeField is only settable at
            # creation, so the documents land in a plain collection
            inv.add("target-prereq", db, "time-series collections",
                    [f"{c['name']} (timeField"
                     f" {c['options']['timeseries'].get('timeField')})"
                     for c in colls if c.get("type") == "timeseries"])
            # a default collation changes how every index and sort compares
            # strings, and it too is only settable at creation
            inv.add("target-prereq", db,
                    "collections with a non-simple default collation",
                    [f"{c['name']} ({c['options']['collation'].get('locale')})"
                     for c in colls
                     if c.get("options", {}).get("collation")])

            if cluster_known:
                shards = []
                if sharded_cluster:
                    try:
                        ns = src["config"]["collections"].find(
                            {"_id": {"$regex": f"^{db}\\."}, "dropped":
                             {"$ne": True}})
                        shards = [f"{c['_id']} (key {dict(c['key'])})"
                                  for c in ns]
                    except Exception as e:
                        inv.unknown("target-prereq", db,
                                    f"cannot read config.collections:"
                                    f" {str(e)[:100]}")
                        shards = None
                if shards is not None:
                    # the shard key has to be chosen and applied before any
                    # data lands; resharding afterwards is a separate project
                    inv.add("target-prereq", db, "sharded collections",
                            shards)
            else:
                inv.unknown("target-prereq", db,
                            f"cannot tell whether this is a sharded cluster:"
                            f" {reason}")

            # Per collection, not per database: a view has no indexes and
            # raises when asked for them, and one such failure used to turn
            # every TTL index in the database into "unknown" - losing real
            # findings to a collection that never had any.
            ttl = []
            for c in colls:
                if c.get("type") == "view":
                    continue
                try:
                    ttl += [f"{c['name']}.{ix['name']}"
                            for ix in d[c["name"]].list_indexes()
                            if "expireAfterSeconds" in ix]
                except Exception as e:
                    inv.unknown("decide-then-apply", db,
                                f"cannot list indexes on {c['name']}:"
                                f" {str(e)[:80]}")
            # a TTL index on the target deletes documents while the sync is
            # still running - the same hazard as an enabled MySQL event
            inv.add("decide-then-apply", db,
                    "TTL indexes that must stay disabled until cutover", ttl)
        return inv

    def setup_target_plan(self, db):
        return [
            "-- mongo target needs no pre-created schema, but build indexes first:",
            "-- run migkit check --only schema, create missing indexes on target",
            "-- then start mongosync (with embedded verifier),"
            " or mongodump --oplog | mongorestore --oplogReplay",
        ]

    def tail_seed(self, db, token_path, point):
        """Start the tail from `point`, a resume token taken earlier."""
        from .. import tailctl
        tailctl.begin(self, db, token_path, {"_data": point})

    #: events a look at the change stream carries at most before its
    #: position is saved (`tail_apply`)
    TAIL_BATCH = 1000

    def _tail_event(self, db, dst, ev, go):
        """One row change of the stream applied to the target, only if the
        hop carries that kind of change for the collection (`mapping.ops`,
        `Hop.carries`: a move told to keep what the source deletes does not
        delete). A replace is a document written whole - an update of one
        the target holds, an insert of one it does not. A document is
        written to the collection under the name the hop gives it there,
        by `_id`, so the same event twice leaves it as it was. Without
        `go` nothing is written. 1 where the event is one the hop carries,
        0 where it leaves it be - the position moves on either way, and
        the check holds the collection to what is carried."""
        op = ev["operationType"]
        coll = (ev.get("ns") or {}).get("coll")
        carried = self.hop.carries(db, coll)
        key = ev.get("documentKey", {})
        into = dst[self._there(db, coll)]
        if op == "delete":
            if "delete" not in carried:
                return 0
            if go:
                into.delete_one(key)
            return 1
        doc = ev.get("fullDocument")
        # an update writes the document it made; one the target lacks is
        # put there only where inserts are carried too
        kinds = {"insert": {"insert"}, "update": {"update"},
                 "replace": {"update", "insert"}}[op]
        if not kinds & carried:
            return 0
        if go and doc is not None:
            into.replace_one(key, doc, upsert="insert" in carried)
        return 1

    def tail_apply(self, db, go, token_path, log):
        """Same-engine change tail, carrying each document as BSON, round
        the loop every tail goes round (`tailctl.run_loop`): up to
        `TAIL_BATCH` events read off the stream and applied by key, then
        the position after them saved - so a stop, a crash or a lost
        connection reads again from the last position saved, which the
        writes by key converge on. It had no stop, no end once caught up,
        no retry, took SIGTERM as a crash and beat never (audit ST14).

        Only an applying run moves the saved position: a count-only run that
        saved one sent the next `--go` past events nobody applied. The
        position is saved before the first event is read, so a tail stopped
        before anything arrived resumes there rather than at a later "now".
        """
        import json
        from types import SimpleNamespace

        from bson.json_util import dumps, loads

        from .. import tailctl
        src = self._client("src")[db]
        dst = self._client("dst")[self._d("dst", db)]
        env = tailctl.position(token_path)
        resume = None
        if env is not None:
            if env.get("token") is not None:
                resume = loads(json.dumps(env["token"]))
            tailctl.same_source(self, db, token_path, resume)
            log("resuming from saved token")
        elif go:
            env = tailctl.begin(self, db, token_path,
                                {"_data": self.change_point("src", db)})
            resume = env["token"]
        log("tailing change stream, ctrl-c to stop"
            + ("" if go else " (count-only, add --go to apply)"))
        s = SimpleNamespace(stream=None, n=0, env=env)

        def close():
            if s.stream is not None:
                try:
                    s.stream.close()
                except Exception:  # noqa: BLE001 - gone with it
                    pass
                s.stream = None

        def look(at):
            if s.stream is None:
                kwargs = {"full_document": "updateLookup"}
                if at:
                    kwargs["resume_after"] = at
                # the server leaves out what the hop does not take
                # (`stream_match`); each event is still asked below
                s.stream = src.watch(self.stream_match(db), **kwargs)
            read, n, after, settled = 0, 0, at, False
            while read < self.TAIL_BATCH:
                ev = s.stream.try_next()
                if ev is None:
                    # nothing left before this point: the stream's own
                    # position is how far the target is now, and saving
                    # it is what lets `check` fence on this tail
                    settled = True
                    if s.stream.resume_token:
                        after = s.stream.resume_token
                    break
                read += 1
                op = ev["operationType"]
                coll = (ev.get("ns") or {}).get("coll")
                if coll and not self.in_scope(db, coll):
                    after = ev["_id"]
                    continue
                if op not in self.STREAM_OPS:
                    raise self._not_a_row_change(op, ev)
                n += 1
                self._tail_event(db, dst, ev, go)
                after = ev["_id"]
            if n:
                s.n += n
                log(f"{s.n} events, token saved")
            return after, n, settled

        def save(after):
            s.env = tailctl.moved(token_path, s.env,
                                  json.loads(dumps(after)))

        def lost(e, at):
            # the stream opened again from the last position saved
            close()
            got = tailctl.saved(token_path)
            return loads(json.dumps(got)) if got is not None else at
        tailctl.run_loop(token_path.parent, db, look, log, "changes",
                         hop=self.hop, go=go, token=resume, save=save,
                         lost=lost, on_end=close,
                         secrets=(self.hop.source.password,
                                  self.hop.target.password))

    def load_window(self, db, log=None, tables=None):
        """The target's TTL indexes, held from deleting what the load
        writes (`_TtlWindow`). A document store runs no trigger and keeps
        no other index that rewrites a document."""
        return _TtlWindow(self, db, log, tables)

    def set_aside(self, db):
        """The TTL indexes a load held and has not put back. Held by a
        load still running they are held on purpose, and go back when it
        stops. Held by one whose process is gone, they never expire what
        the application writes: the cutover onto this target would run
        without the expiry, and nothing else compares the two."""
        from ..setaside import SetAside
        held, lost, unread = [], [], []
        for path, alive, defs in SetAside(self.hop, db,
                                          "dropped-indexes").records():
            if defs is None:
                unread.append(str(path))
            elif alive:
                held += sorted(defs)
            else:
                lost += [(n, str(path)) for n in sorted(defs)]
        scope = f"{db} ttl set aside"
        if lost or unread:
            said = []
            if lost:
                said.append(f"{len(lost)} TTL indexes a load held from"
                            " deleting and never put back: "
                            + ", ".join(n for n, _ in lost[:5])
                            + " - their values in "
                            + ", ".join(sorted({p for _, p in lost})))
            if unread:
                said.append("a record of TTL indexes held cannot be read: "
                            + ", ".join(unread))
            return Result("deep", scope, "diff", "; ".join(said), "",
                          "the next `migkit move` into this database puts"
                          " them back")
        if held:
            return Result("deep", scope, "ok",
                          f"{len(held)} TTL indexes are held on the target"
                          " while a migkit load runs into it ("
                          + ", ".join(held[:5]) + "); they go back when it"
                          " stops")
        return None

    def watch_sample(self, db):
        a = self._client("src")[db].command("dbStats")
        b = self._client("dst")[self._d("dst", db)].command("dbStats")
        return {"db": db, "ts": time.time(),
                "src_rows": a.get("objects", 0), "dst_rows": b.get("objects", 0)}


class _TtlWindow:
    """The target's TTL indexes, held from deleting for the load.

    A TTL index has the server delete the documents whose date is past its
    expiry, a minute at a time. A load writes the source's documents with
    the dates they have - documents the source's own expiry has not reached
    yet - and the target's expiry, running beside the load, deletes them
    as they land: the move would say complete over a collection short of
    documents the source still holds. So each TTL index of a collection
    the load writes is set, for the load, to the longest expiry the server
    takes, its own value saved to disk first and put back on the way out
    whether the load worked or not (`setaside`, as a trigger or an index
    set aside is in the other engines).

    A load killed in between does not put them back. What it held goes back
    at the end of the next load into the database, and `check` and the
    cutover name it until then (`MongoEngine.set_aside`).
    """

    #: the longest expiry a TTL index takes, in seconds
    MOST = 2_147_483_647

    def __init__(self, eng, db, log=None, tables=None):
        from ..setaside import SetAside
        self.eng, self.db, self.log = eng, db, log or (lambda m: None)
        # the collections the load writes, by the names it is asked with
        # (the source's, from a copy of this engine's own; the target's,
        # from a pair's) and by the names the hop gives them there
        # (`mapping.tables`)
        self.tables = (None if tables is None else
                       set(tables) | {eng.hop.target_table_in(db, t)
                                      for t in tables})
        self.held = {}
        self.record = SetAside(eng.hop, db, "dropped-indexes")

    def _target(self):
        return self.eng._client("dst")[self.eng._d("dst", self.db)]

    def __enter__(self):
        import json
        target = self._target()
        now = {}
        for name in target.list_collection_names():
            if name.startswith("system.") \
                    or not self.eng.in_scope(self.db, name) \
                    or (self.tables is not None and name not in self.tables):
                continue
            try:
                indexes = list(target[name].list_indexes())
            except Exception:  # noqa: BLE001 - a view has none
                continue
            for ix in indexes:
                if "expireAfterSeconds" in ix:
                    now[f"{name}.{ix['name']}"] = {
                        "collection": name, "index": ix["name"],
                        "seconds": int(ix["expireAfterSeconds"])}
        # what a load that died held: put back at the end of this one,
        # whichever collections this one writes. It is held already, so its
        # own value is the saved one, and the value read now is not
        left, stale = self.record.left_behind()
        adopted = {k: json.loads(v) for k, v in left.items()}
        self.held = {**now, **adopted}
        if not self.held:
            for path in stale:
                path.unlink(missing_ok=True)
            return self
        kept = {k: json.dumps(v, sort_keys=True)
                for k, v in self.held.items()}
        if not self.record.save(kept, stale):
            raise SystemExit(
                f"{self.db}: the target's TTL indexes could not be saved"
                " before holding them for the load, so nothing was loaded")
        fresh = [k for k in now if k not in adopted]
        for key in fresh:
            self._set(now[key], self.MOST)
        if adopted:
            self.log(f"{len(adopted)} TTL indexes an earlier load held and"
                     " did not put back go back at the end of this one: "
                     + ", ".join(sorted(adopted)[:5]))
        if fresh:
            self.log(f"{len(fresh)} TTL indexes of the target held from"
                     " deleting for the load; their values saved to"
                     f" {self.record.path}")
        return self

    def _set(self, one, seconds):
        self._target().command(
            "collMod", one["collection"],
            index={"name": one["index"], "expireAfterSeconds": seconds})

    def __exit__(self, exc_type, *exc):
        failed = []
        for key, one in sorted(self.held.items()):
            try:
                self._set(one, one["seconds"])
            except Exception as e:  # noqa: BLE001 - said, and kept
                failed.append(key)
                self.log(f"TTL INDEX NOT PUT BACK: {key}:"
                         f" {(str(e).splitlines() or [''])[0][:100]}")
        if self.held:
            self.log(f"{len(self.held) - len(failed)} of {len(self.held)}"
                     " TTL indexes put back")
        if not failed:
            self.record.done()
        if failed and exc_type is None:
            raise SystemExit(
                f"{self.db}: {len(failed)} TTL indexes were not put back:"
                f" {', '.join(failed)}. Their values are in"
                f" {self.record.path}")
        return False
