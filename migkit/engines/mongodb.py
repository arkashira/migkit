import time

from .base import Engine, RepairAction, Result

SKIP_DBS = {"admin", "local", "config"}
DRILL_MAX_DOCS = 5_000_000


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
        if extra:
            uri += "?" + extra
        return MongoClient(uri, serverSelectionTimeoutMS=15000)

    def _d(self, side, db):
        """Physical db name: source as given, target through the hop's
        db_map (identity when unmapped) so a migration can land in a
        differently-named database."""
        return self.hop.target_db(db) if side == "dst" else db

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        c = self._client("src")
        return sorted(d for d in c.list_database_names() if d not in SKIP_DBS)

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
        s, t = self._client("src")[db], self._client("dst")[self._d("dst", db)]
        bad = []
        names = sorted(set(s.list_collection_names()) & set(t.list_collection_names()))
        total_a = total_b = 0
        for name in names:
            a = s[name].count_documents({})
            b = t[name].count_documents({})
            total_a += a
            total_b += b
            if a != b:
                bad.append(f"{name} src={a} dst={b}")
        if bad:
            return [Result("counts", db, "diff", "; ".join(bad))]
        return [Result("counts", db, "ok",
                       f"{len(names)} collections, docs"
                       f" {total_a:,}=={total_b:,}")]

    def check_data(self, db, table=None, stream=None, with_counts=False):
        s, t = self._client("src")[db], self._client("dst")[self._d("dst", db)]
        try:
            a = s.command("dbHash")["collections"]
            b = t.command("dbHash")["collections"]
        except Exception:
            a = b = None
        sn = set(s.list_collection_names())
        tn = set(t.list_collection_names())
        names = [table] if table else sorted(sn & tn)
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
            r = self._drilldown(db, name)
            if stream:
                stream(f"{name}: {r.status}")
            res.append(r)
        if with_counts:
            # equal hashes imply equal counts, so only diffed collections
            # pay for an exact count
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

    def _drilldown(self, db, name):
        from bson.json_util import dumps
        scope = f"{db}.{name}"
        s, t = self._client("src")[db][name], self._client("dst")[self._d("dst", db)][name]
        if s.estimated_document_count() > DRILL_MAX_DOCS:
            return Result("data", scope, "diff",
                          "dbHash differs, too large for id drilldown", "",
                          "recopy with mongodump | mongorestore --drop, or"
                          " run mongodb-labs migration-verifier")
        pipe = [{"$project": {"h": {"$toHashedIndexKey": "$$ROOT"}}}]
        try:
            src = {repr(d["_id"]): (d["_id"], d["h"])
                   for d in s.aggregate(pipe, allowDiskUse=True)}
            dst = {repr(d["_id"]): (d["_id"], d["h"])
                   for d in t.aggregate(pipe, allowDiskUse=True)}
        except Exception:
            src = self._client_hashes(s)
            dst = self._client_hashes(t)
        missing = [src[k][0] for k in src if k not in dst]
        extra = [dst[k][0] for k in dst if k not in src]
        changed = [src[k][0] for k in src
                   if k in dst and src[k][1] != dst[k][1]]
        if not (missing or extra or changed):
            return Result("data", scope, "ok",
                          f"docs {len(src):,}=={len(dst):,},"
                          f" per-id hash equal")
        d = self.hop.report_dir(db)
        for kind, ids in (("missing", missing), ("extra", extra),
                          ("changed", changed)):
            p = d / f"data-{name}.{kind}"
            if ids:
                p.write_text("\n".join(dumps(i) for i in ids) + "\n")
            elif p.exists():
                p.unlink()
        return Result("data", scope, "diff",
                      f"missing={len(missing)} extra={len(extra)}"
                      f" changed={len(changed)}", str(d),
                      f"migkit sync {self.hop.name} --db {db} --kind rows --apply")

    def _client_hashes(self, coll):
        import hashlib

        import bson
        out = {}
        for doc in coll.find(sort=[("_id", 1)]):
            h = hashlib.md5(bson.encode(doc)).hexdigest()
            out[repr(doc["_id"])] = (doc["_id"], h)
        return out

    def snapshot_state(self, db, state_dir):
        (state_dir / "dst-shape.txt").write_text(repr(self._shape("dst", db)))

    def check_deep(self, db):
        s, t = self._client("src")[db], self._client("dst")[self._d("dst", db)]
        names = sorted(set(s.list_collection_names())
                       & set(t.list_collection_names()))

        def newest(coll):
            d = coll.find_one(sort=[("_id", -1)], projection={"_id": 1})
            return d["_id"] if d else None

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
            return [Result("deep", f"{db} boundary", "diff",
                           f"target newest _id AHEAD of source on:"
                           f" {', '.join(ahead[:5])}", "",
                           "writes landing on target or double-apply,"
                           " find the writer before cutover")]
        note = f"; {len(behind)} behind (replication lag)" if behind else ""
        return [Result("deep", f"{db} boundary", "ok",
                       f"newest _id checked on {n} collections,"
                       f" none ahead of source{note}")]

    # delta verify: drain the change stream window since the saved resume
    # token and re-verify only the touched _ids. The token advances only
    # after a clean verify, so crashes and diffs replay the same window.
    def delta_verify(self, db, limit=20000, log=None):
        from bson.json_util import dumps, loads
        state = self.hop.report_dir(db) / "delta-token.json"
        src = self._client("src")[db]
        dst = self._client("dst")[self._d("dst", db)]
        if not state.exists():
            with src.watch() as stream:
                stream.try_next()
                state.write_text(dumps(stream.resume_token))
            return [Result("delta", db, "ok",
                           "resume token recorded, changes are tracked"
                           " from this point on")]
        token = loads(state.read_text())
        touched = {}
        n = 0
        end_token = token
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

        sv = self._client("src").server_info().get("version", "?")
        dv = self._client("dst").server_info().get("version", "?")
        add("pass" if sv.split(".")[0] == dv.split(".")[0] else "warn",
            "instance", "server version match", f"src {sv} / dst {dv}")
        if "docdb" in (self.hop.source.host or ""):
            add("warn", "instance", "source is DocumentDB",
                "no dbHash or hashed-index aggregation,"
                " client-side hashing is used (slower)")
        for db in self.databases():
            shape = self._shape("src", db)
            capped = [n for n, v in shape.items() if "capped" in v["options"]]
            add("pass" if not capped else "warn", db,
                "capped collections (size-bound, verify caps match)",
                ", ".join(capped) or "none")
            ttl = [f"{n}.{ix}" for n, v in shape.items()
                   for ix, spec in v["indexes"].items()
                   if "expireAfterSeconds" in spec]
            add("pass" if not ttl else "warn", db,
                "TTL indexes (target TTL deletes docs during sync,"
                " keep disabled until cutover)", ", ".join(ttl) or "none")
        return items

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
