import time

from .base import Engine, RepairAction, Result
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
                return dict(self._client(side).admin.command("buildInfo"))
            except Exception:
                return {}
        return (one("src"), one("dst"))

    def _server_versions(self):
        s, d = self._brands()
        return (s.version or None, d.version or None)


    def neutral_tables(self, side, db):
        c = self._client(side)[self._d(side, db)]
        return sorted(n for n in c.list_collection_names()
                      if not n.startswith("system.")
                      and not self.hop.excluded(db, n))

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
        return {r["_id"]: (sorted(r["types"]), r["present"], r["nulls"])
                for r in rows}

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

    def neutral_key(self, side, db, table):
        """`_id`, which MongoDB guarantees on every document."""
        return ["_id"]

    def neutral_read(self, side, db, table, columns, after=None, limit=1000):
        """Documents as rows, with a missing field marked rather than nulled.

        `canon.ABSENT` is not None: MongoDB distinguishes a field that is not
        there from one holding null, and reading the first as the second here
        would destroy the distinction before the mover could count what the
        target does to it.
        """
        from .. import canon
        coll = self._client(side)[self._d(side, db)][table]
        names = [n for n, _ in columns]
        projection = {f: 1 for f in names}
        projection.setdefault("_id", 1 if "_id" in names else 0)
        flt = {"_id": {"$gt": after[0]}} if after else {}
        cursor = coll.find(flt, projection).sort("_id", 1).limit(int(limit))
        rows, last = [], None
        for doc in cursor:
            rows.append([doc.get(n, canon.ABSENT) if n in doc
                         else canon.ABSENT for n in names])
            last = doc.get("_id", last)
        if not rows:
            return ([], None)
        return (rows, (last,) if last is not None else None)

    def neutral_rows_by_key(self, side, db, table, columns, key, keys):
        """The same lookup, expressed as a filter instead of a select.

        A field that is not there still comes back as `canon.ABSENT` rather
        than None, for the same reason the ordered read does: the two mean
        different things here and only one of them is a value.
        """
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
    def _bind(value):
        """One value on its way into a BSON document.

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
        import decimal
        if isinstance(value, decimal.Decimal):
            from bson.decimal128 import Decimal128
            return Decimal128(value)
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
        ops = []
        for row in rows:
            body = {n: self._bind(v) for n, v in zip(names, row)
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
            ops.append(UpdateOne({"_id": row[at]}, update, upsert=True))
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
            stream = client[target].watch([], **args)
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
        try:
            while len(out) < limit:
                event = stream.try_next()
                if event is None:
                    break
                op = event.get("operationType")
                if op not in self.STREAM_OPS:
                    raise SystemExit(
                        f"the change stream reported {op!r} on"
                        f" {event.get('ns', {}).get('coll', '?')}, which is"
                        " not a row change - migkit carries inserts,"
                        " updates, replaces and deletes, and will not pretend"
                        " a dropped or renamed collection is one of them.\n"
                        "    Re-run the full load for that collection, then"
                        " start the tail again from a token taken after it")
                table = event.get("ns", {}).get("coll")
                key = dict(event.get("documentKey") or {})
                if self.STREAM_OPS[op] == "delete":
                    out.append(canon.change("delete", table, key))
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
                                            key, values))
            last = stream.resume_token
        finally:
            stream.close()
        return out, (last or {}).get("_data") if last else token

    def neutral_digest(self, side, db, table, columns):
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
        from .. import canon, rowtext
        coll = self._client(side)[self._d(side, db)][table]
        fields = [name for name, _ in columns]
        classes = dict(columns)
        total, n = 0, 0
        projection = {f: 1 for f in fields}
        projection.setdefault("_id", 1 if "_id" in fields else 0)
        for doc in coll.find({}, projection):
            parts = []
            for name in fields:
                value = doc.get(name)
                text = (None if value is None
                        else canon.render_value(classes[name], value))
                if text is None:
                    parts.append(f"{rowtext.NULL_LEN}:")
                else:
                    parts.append(f"{len(text)}:{text}")
            total = canon.digest_step(total, rowtext.SEP.join(parts))
            n += 1
        return (n, str(total))

    def _shape(self, side, db):
        d = self._client(side)[self._d(side, db)]
        shape = {}
        for name in sorted(d.list_collection_names()):
            idx = {}
            for i in d[name].list_indexes():
                spec = {k: (str(v) if k == "key" else v)
                        for k, v in i.items()
                        if k not in ("v", "ns", "background")}
                idx[i["name"]] = repr(sorted(spec.items(), key=str))
            opts = d.command("listCollections",
                             filter={"name": name})["cursor"]["firstBatch"][0]
            o = {k: v for k, v in opts.get("options", {}).items()
                 if k not in ("storageEngine", "autoIndexId")
                 and not (k == "capped" and not v)}
            shape[name] = {"indexes": idx,
                           "type": opts.get("type", "collection"),
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
        src, dst = self._shape("src", db), self._shape("dst", db)
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

        def collections(side):
            return {c for c in side.list_collection_names()
                    if not c.startswith("system.")
                    and not self.hop.excluded(db, c)}

        src_names, dst_names = collections(s), collections(t)
        res = []
        if src_names - dst_names:
            res.append(Result("counts", db, "diff",
                              "missing collections on target:"
                              f" {sorted(src_names - dst_names)}", "",
                              "create and load them before cutover"))
        if dst_names - src_names:
            res.append(Result("counts", db, "diff",
                              "extra collections on target:"
                              f" {sorted(dst_names - src_names)}"))
        bad = []
        names = sorted(src_names & dst_names)
        total_a = total_b = 0
        for name in names:
            a = s[name].count_documents({})
            b = t[name].count_documents({})
            total_a += a
            total_b += b
            if a != b:
                bad.append(f"{name} src={a} dst={b}")
        if bad:
            res.append(Result("counts", db, "diff", "; ".join(bad)))
        return res or [Result("counts", db, "ok",
                              f"{len(names)} collections, docs"
                              f" {total_a:,}=={total_b:,}")]

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
        lag = None
        try:
            rs = self._client(side).admin.command("replSetGetStatus")
            members = rs.get("members") or []
            primary = next((m for m in members if m.get("stateStr") == "PRIMARY"),
                           None)
            if primary:
                times = [m.get("optimeDate") for m in members
                         if m.get("optimeDate") and m.get("stateStr")
                         in ("PRIMARY", "SECONDARY")]
                if len(times) > 1:
                    lag = (max(times) - min(times)).total_seconds()
        except Exception:
            pass    # standalone, or the command is not permitted: lag unknown
        return Health(busy_ratio=busy, lag_seconds=lag)

    def check_data(self, db, table=None, stream=None, with_counts=False):
        s, t = self._client("src")[db], self._client("dst")[self._d("dst", db)]
        try:
            a = s.command("dbHash")["collections"]
            b = t.command("dbHash")["collections"]
        except Exception:
            a = b = None
        sn = set(s.list_collection_names())
        tn = set(t.list_collection_names())
        names = [table] if table else sorted(c for c in sn & tn
                                             if not self.hop.excluded(db, c))
        # dbHash and the id drilldown are both full scans, so the same rule
        # as the other engines applies: stop adding load to a deployment that
        # is already struggling.
        from ..throttle import Throttle
        gate = Throttle(1, probe=lambda: self._health("src"))
        res = []
        for name in names:
            if name.startswith("system."):
                continue
            if a is not None and a.get(name) == b.get(name) \
                    and a.get(name) is not None:
                if stream:
                    stream(f"{name}: ok")
                res.append(Result("data", f"{db}.{name}", "ok",
                                  f"dbHash {a.get(name)} both sides"))
                continue
            with gate.unit():
                r = self._drilldown(db, name)
            if stream:
                stream(f"{name}: {r.status}")
            res.append(r)
        self._last_throttle = gate.summary()
        note = gate.line()
        if note and stream:
            stream(f"# {note}")
        if with_counts:
            # equal hashes imply equal counts; only diffed colls pay to count
            bad = [f"{c} missing on target" for c in sorted(sn - tn)
                   if not c.startswith("system.")]
            bad += [f"{c} extra on target" for c in sorted(tn - sn)
                    if not c.startswith("system.")]
            for r in res:
                if r.check == "data" and r.status != "ok":
                    name = r.scope.split(".", 1)[1]
                    ca = s[name].count_documents({})
                    cb = t[name].count_documents({})
                    if ca != cb:
                        bad.append(f"{name} src={ca} dst={cb}")
            if bad:
                cres = Result("counts", db, "diff", "; ".join(bad[:10]))
            else:
                cres = Result("counts", db, "ok",
                              f"{len(names)} collections, equality implied"
                              " by data hash (no extra count scan)")
            res.insert(0, cres)
        return res

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

    def _coverage_ok(self, coll, plan, docs):
        """Do these ranges actually account for every document?

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
            seen = sum(coll.count_documents(self._range_filter(ty, lo, hi))
                       for ty, lo, hi in plan)
        except Exception:
            return False
        return seen == coll.count_documents({})

    @staticmethod
    def _range_filter(ty, lo, hi):
        from .. import checkpoint as _cp
        f = dict(_cp.mongo_filter(lo, hi))
        cond = dict(f.get("_id") or {})
        if ty is not None:
            cond["$type"] = ty
        return {"_id": cond} if cond else {}

    def _range_hashes(self, coll, flt):
        pipe = ([{"$match": flt}] if flt else []) + [
            {"$project": {"h": {"$toHashedIndexKey": "$$ROOT"}}}]
        try:
            return {repr(d["_id"]): (d["_id"], d["h"])
                    for d in coll.aggregate(pipe, allowDiskUse=True)}
        except Exception:
            return self._client_hashes(coll, flt)

    def _drilldown(self, db, name):
        """Compare every document by id, one `_id` range at a time.

        This used to build a dict of every id and hash for both sides at once,
        which is why it refused to run past five million documents and sent
        the operator to another tool. Working a range at a time makes the
        memory cost proportional to the range instead of the collection, and
        makes the comparison restartable: completed ranges are recorded, so a
        rerun only pays for what it still owes.

        Comparing per range is sound because `_id` is unique - a document
        falls in exactly one range, and both sides are given the same
        boundaries, so nothing can be missing on one side merely for being
        looked at in a different range.
        """
        from bson.json_util import dumps

        from .. import checkpoint as _cp
        scope = f"{db}.{name}"
        s, t = self._client("src")[db][name], self._client("dst")[self._d("dst", db)][name]
        docs = s.count_documents({})
        ranges = self._id_plan(s, docs)
        if not self._coverage_ok(s, ranges, docs):
            # better one slow pass than a fast answer about part of the data
            ranges = [(None, None, None)]
        cp = _cp.Checkpoint(str(self.hop.report_dir(db) / "checkpoint.json"))
        # The checkpoint identifies a range by text, and a range here is
        # (type, lo, hi) - so key and value are kept as an explicit pair
        # rather than squeezing three things into two and hoping.
        keyed = [((f"{ty}|{lo}", f"{hi}"), (ty, lo, hi)) for ty, lo, hi in ranges]
        by_key = dict(keyed)
        todo_keys = cp.begin(scope, "toHashedIndexKey", [k for k, _ in keyed])
        todo = [by_key[k] for k in todo_keys]
        done_before = cp.resumed(scope)

        d = self.hop.report_dir(db)
        found = {"missing": [], "extra": [], "changed": []}
        for ty, lo, hi in todo:
            flt = self._range_filter(ty, lo, hi)
            src = self._range_hashes(s, flt)
            dst = self._range_hashes(t, flt)
            found["missing"] += [src[k][0] for k in src if k not in dst]
            found["extra"] += [dst[k][0] for k in dst if k not in src]
            found["changed"] += [src[k][0] for k in src
                                 if k in dst and src[k][1] != dst[k][1]]
            if any(found.values()):
                # a difference makes the stored partials useless: the next run
                # must re-read this collection rather than trust a half-total
                cp.clear(scope)
            else:
                cp.record(scope, f"{ty}|{lo}", f"{hi}", len(src), 0)
        checked = cp.total(scope)[0] if not any(found.values()) else 0
        if not any(found.values()):
            cp.clear(scope)
            resumed = (f", resumed {done_before}/{len(ranges)}"
                       if done_before else "")
            return Result("data", scope, "ok",
                          f"docs {checked:,} compared by id hash,"
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
        return Result("data", scope, "diff",
                      f"missing={len(found['missing'])}"
                      f" extra={len(found['extra'])}"
                      f" changed={len(found['changed'])}"
                      f" over {len(ranges)} ranges"
                      + (f" kind={kind}" if kind else ""), str(d),
                      f"migkit sync {self.hop.name} --db {db} --kind rows --apply")

    def _client_hashes(self, coll, flt=None):
        """Hash documents locally when the server has no hashing operator.

        Honours the same range filter as the server-side path, so the
        fallback is restartable and bounded in memory too - a fallback that
        reads the whole collection would reintroduce exactly the limit the
        ranges exist to remove.
        """
        import hashlib

        import bson
        out = {}
        for doc in coll.find(flt or {}, sort=[("_id", 1)]):
            h = hashlib.md5(bson.encode(doc)).hexdigest()
            out[repr(doc["_id"])] = (doc["_id"], h)
        return out

    def snapshot_state(self, db, state_dir, kind="all"):
        (state_dir / "dst-shape.txt").write_text(repr(self._shape("dst", db)))

    def check_deep(self, db):
        s, t = self._client("src")[db], self._client("dst")[self._d("dst", db)]
        names = sorted(set(s.list_collection_names())
                       & set(t.list_collection_names()))

        def newest(coll):
            d = coll.find_one(sort=[("_id", -1)], projection={"_id": 1})
            return d["_id"] if d else None

        res = []
        ahead, behind = [], []
        n = 0
        for name in names:
            if name.startswith("system."):
                continue
            n += 1
            na, nb = newest(s[name]), newest(t[name])
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
            sf, df = field_types(s[name]), field_types(t[name])
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
            sc, tc = s[name], t[name]
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
            so, to = sopts.get(name, {}), topts.get(name, {})
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
            return {ix["name"]: ix for ix in coll.list_indexes()}
        ixbad = []
        for name in names:
            if name.startswith("system."):
                continue
            si, di = indexes(s[name]), indexes(t[name])
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
        def mongos(side):
            try:
                return self._client(side).admin.command(
                    "hello").get("msg") == "isdbgrid"
            except Exception:
                return False
        if not mongos("src") and not mongos("dst"):
            res.append(Result("deep", f"{db} sharding", "ok",
                              "not a sharded cluster (standalone/replica-set)"))
        else:
            sh = []
            try:
                if self._client("dst").admin.command(
                        "balancerStatus").get("inBalancerRound"):
                    sh.append("balancer running on target - counts/orphans are"
                              " a moving target; sh.stopBalancer() before"
                              " verify")
            except Exception:
                pass
            for name in names:
                if name.startswith("system."):
                    continue
                try:
                    r = list(t[name].aggregate(
                        [{"$group": {"_id": "$_id", "n": {"$sum": 1}}},
                         {"$match": {"n": {"$gt": 1}}}, {"$count": "c"}],
                        allowDiskUse=True))
                    if r and r[0]["c"] > 0:
                        sh.append(f"{name}: {r[0]['c']} duplicate _id across"
                                  " shards (merge/reshard collision)")
                except Exception:
                    continue
            res.append(Result("deep", f"{db} sharding",
                              "diff" if sh else "ok",
                              "; ".join(sh[:5]) if sh
                              else "sharded: no dup _id, balancer idle", "",
                              "cleanupOrphaned / dedupe and stop the balancer"
                              " before cutover" if sh else ""))
        return res

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
                    if coll and key is not None:
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
            for _id in ids.values():
                a = src[coll].find_one({"_id": _id})
                b = dst[coll].find_one({"_id": _id})
                if a != b:
                    bad.append(_id)
            if bad:
                clean = False
                d = self.hop.report_dir(db)
                (d / f"data-{coll}.changed").write_text(
                    "\n".join(dumps(i) for i in bad) + "\n")
                res.append(Result(
                    "delta", f"{db}.{coll}", "diff",
                    f"of {len(ids)} touched docs, {len(bad)} differ",
                    str(d),
                    f"migkit sync {self.hop.name} --db {db} --kind rows"))
            else:
                res.append(Result("delta", f"{db}.{coll}", "ok",
                                  f"{len(ids)} touched docs verified"
                                  " equal on both sides"))
            if log:
                log(f"{coll}: {len(ids)} touched, "
                    + ("clean" if not bad else "DIFF"))
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
        actions = []
        for name in names:
            if self.hop.excluded(db, name):
                continue
            counts = []
            for k in ("missing", "extra", "changed"):
                f = d / f"data-{name}.{k}"
                if f.exists():
                    counts.append(f"{k}={sum(1 for _ in f.open())}")
            actions.append(RepairAction(
                db, "rows", [f"resync docs for {name} ({', '.join(counts)})"],
                [], f"{name}: replace/insert from source per _id, deleted and"
                    " overwritten target docs saved to undo first"))
        return actions

    def apply(self, db, action):
        from bson.json_util import dumps, loads
        name = action.statements[0].split()[3]
        d = self.hop.report_dir(db)
        s = self._client("src")[db][name]
        t = self._client("dst")[self._d("dst", db)][name]
        undo = d / "undo"
        undo.mkdir(exist_ok=True)

        def read(kind):
            f = d / f"data-{name}.{kind}"
            return [loads(l) for l in f.read_text().splitlines()] \
                if f.exists() else []

        with (undo / f"{name}.docs.jsonl").open("a") as uf:
            for _id in read("extra"):
                doc = t.find_one({"_id": _id})
                if doc is not None:
                    uf.write(dumps(doc) + "\n")
                t.delete_one({"_id": _id})
            for _id in read("missing") + read("changed"):
                old = t.find_one({"_id": _id})
                if old is not None:
                    uf.write(dumps(old) + "\n")
                doc = s.find_one({"_id": _id})
                if doc is not None:
                    t.replace_one({"_id": _id}, doc, upsert=True)

    def assess(self):
        items = []

        def add(level, scope, item, detail=""):
            items.append({"level": level, "scope": scope,
                          "item": item, "detail": str(detail)})

        items += self._brand_rows()
        sv, dv = self._server_versions()
        items.append(self._version_row(sv, dv))

        inv = self._handwork()
        items += inv.rows() + inv.summary()
        items += self._mover_leftovers()
        return items

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

    def tail_apply(self, db, go, token_path, log):
        import json as _json

        from bson.json_util import dumps, loads
        src = self._client("src")[db]
        dst = self._client("dst")[self._d("dst", db)]
        resume = None
        if token_path.exists():
            resume = loads(token_path.read_text())
            log(f"resuming from saved token")
        kwargs = {"full_document": "updateLookup"}
        if resume:
            kwargs["resume_after"] = resume
        n = 0
        with src.watch(**kwargs) as stream:
            log("tailing change stream, ctrl-c to stop"
                + ("" if go else " (count-only, add --go to apply)"))
            for ev in stream:
                n += 1
                op = ev["operationType"]
                coll = ev["ns"]["coll"]
                key = ev.get("documentKey", {})
                if go:
                    if op in ("insert", "update", "replace"):
                        doc = ev.get("fullDocument")
                        if doc is not None:
                            dst[coll].replace_one(key, doc, upsert=True)
                    elif op == "delete":
                        dst[coll].delete_one(key)
                if n % 100 == 0 or op == "invalidate":
                    token_path.write_text(dumps(ev["_id"]))
                    log(f"{n} events, token saved ({op} {coll})")
        token_path.write_text(dumps(stream.resume_token))

    def watch_sample(self, db):
        a = self._client("src")[db].command("dbStats")
        b = self._client("dst")[self._d("dst", db)].command("dbStats")
        return {"db": db, "ts": time.time(),
                "src_rows": a.get("objects", 0), "dst_rows": b.get("objects", 0)}
