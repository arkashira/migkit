#!/usr/bin/env python3
"""Full source-vs-target comparison for MongoDB (DocumentDB -> Tencent Mongo), {src,dst} JSON.
Per collection: doc count, _id-set checksum (catches missing/extra/dup ids), content checksum
(order-independent XOR of per-doc canonical-json md5), index parity. Read-only. Not part of migkit."""
import os, sys, datetime, yaml, json, hashlib, warnings
from pymongo import MongoClient
from bson import json_util
warnings.filterwarnings("ignore")

OUT = os.environ.get("COMPARE_OUT", "reports")  # relative to migkit/ cwd
os.makedirs(OUT, exist_ok=True)
HOP_NAME = sys.argv[1] if len(sys.argv) > 1 else "mongo-to-tencent"
H = yaml.safe_load(open(os.environ.get("MIGKIT_CONF", "conf/hops.yaml")))["hops"][HOP_NAME]
DB = (H.get("databases") or ["oss"])[0]
CONTENT_CAP = 120_000   # full content hash only up to this size; larger -> count + id-set only (fast, still proves doc set)

def uri(side):
    if side == "src":
        s = H["source"]   # DocDB via SSM tunnel 127.0.0.1:27018 (directConnection; cert is for the cluster endpoint)
        opts = ("tls=true&tlsCAFile=./conf/global-bundle.pem&retryWrites=false"
                "&directConnection=true&tlsAllowInvalidHostnames=true")
        return f"mongodb://{s['user']}:{s['password']}@127.0.0.1:27018/?{opts}"
    t = H["target"]; return f"mongodb://{t['user']}:{t['password']}@{t['hosts']}/?{t['uri_options']}"

def canon(doc):
    # canonical, order-independent per-doc hash (extended-json with sorted keys)
    return hashlib.md5(json_util.dumps(doc, sort_keys=True).encode()).hexdigest()

def scan_ids(coll):                       # count + id-set checksum, _id projection only (light over WARP)
    n = 0; id_x = 0
    for d in coll.find({}, {"_id": 1}).batch_size(3000):
        n += 1
        id_x ^= int(hashlib.md5(json_util.dumps(d.get("_id"), sort_keys=True).encode()).hexdigest(), 16)
    return n, f"{id_x:032x}"

def content_hash(coll):                   # order-independent XOR of per-doc canonical md5 (full docs)
    doc_x = 0
    for d in coll.find({}).batch_size(1000):
        doc_x ^= int(canon(d), 16)
    return f"{doc_x:032x}"

def side(which):
    cl = MongoClient(uri(which), serverSelectionTimeoutMS=8000)
    db = cl[DB]
    info = {"version": cl.server_info().get("version")}
    try:
        r = cl.admin.command({"getParameter": 1, "featureCompatibilityVersion": 1})
        info["fcv"] = r.get("featureCompatibilityVersion", {}).get("version", str(r))
    except Exception as e:
        info["fcv"] = f"n/a ({type(e).__name__})"
    colls = {}
    for name in sorted(db.list_collection_names()):
        c = db[name]
        n, idck = scan_ids(c)
        do_content = n <= CONTENT_CAP
        dock = content_hash(c) if do_content else None
        idx = sorted(c.index_information().keys())
        colls[name] = {"count": n, "id_checksum": idck, "content_checksum": dock,
                       "content_hashed": do_content, "indexes": idx}
        print(f"    [{which}] {name}: {n} docs (content={'yes' if do_content else 'skip'})", flush=True)
    cl.close()
    return info, colls

def kv(sv, dv):
    return {"src": sv, "dst": dv, "match": (sv == dv)}

print(f"[mongo:{DB}] source (DocumentDB)…")
sinfo, scolls = side("src")
print(f"[mongo:{DB}] target (TC mongo)…")
dinfo, dcolls = side("dst")

names = sorted(set(scolls) | set(dcolls))
collections = {}
cnt_diff = idset_diff = content_diff = idx_diff = 0
for nm in names:
    s = scolls.get(nm, {}); d = dcolls.get(nm, {})
    row = {
        "count": kv(s.get("count"), d.get("count")),
        "id_checksum": kv(s.get("id_checksum"), d.get("id_checksum")),
        "content_checksum": kv(s.get("content_checksum"), d.get("content_checksum")),
        "content_hashed": {"src": s.get("content_hashed"), "dst": d.get("content_hashed")},
        "indexes": kv(s.get("indexes"), d.get("indexes")),
        "present": {"src": nm in scolls, "dst": nm in dcolls},
    }
    if row["count"]["match"] is False: cnt_diff += 1
    if row["id_checksum"]["match"] is False: idset_diff += 1
    if row["content_checksum"]["src"] and row["content_checksum"]["match"] is False: content_diff += 1
    if row["indexes"]["match"] is False: idx_diff += 1
    collections[nm] = row

doc = {
    "engine": "mongodb", "database": DB, "generated": datetime.date.today().isoformat(),
    "source": f"AWS DocumentDB {H['source']['host'].split('.')[0]} (v{sinfo['version']}, FCV {sinfo['fcv']})",
    "target": f"Tencent Mongo {H['target']['hosts']} (v{dinfo['version']}, FCV {dinfo['fcv']})",
    "note": "DocDB and Tencent Mongo are different engines; id_checksum proves the same document set "
            "migrated (order-independent). content_checksum is best-effort (extended-JSON canonical); "
            "a content diff can be BSON-serialization, not data loss — confirm a sample if it triggers.",
    "summary": {"collections": len(names), "count_diff": cnt_diff, "idset_diff": idset_diff,
                "content_diff": content_diff, "index_diff": idx_diff},
    "server": {"src": sinfo, "dst": dinfo},
    "collections": collections,
}
path = f"{OUT}/compare-mongo-{DB}.json"
json.dump(doc, open(path, "w"), indent=2, default=str)
print(f"  wrote {path}")
print(f"    {json.dumps(doc['summary'])}")
