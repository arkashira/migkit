"""What the one-scan tree, the sketch and the kept leaves cost against the
verify they replace (backlog R20.8-9, F1).

    python tools/with_docker_lock.py .venv/bin/python bench/proof.py \
        [verify 1000000 5000000] [link 1000000] [cutover 1000000] [tail]

A PostgreSQL 16 pair `migkit-test-proof-src`/`-dst` on 127.0.0.1:16170-16171,
and, for the link, a proxy on 16174-16175 that holds every packet for half
the round trip each way. Removed after. Numbers print as JSON.

* `verify N...`: a table of N rows with 10 rows scattered apart (4
  changed, 3 missing, 3 extra) - the old way (the table digest, then the
  key range halved with a digest of each half on both sides, the walk of
  each small range), the one-scan tree (range leaves by the integer key,
  and hash leaves), the table digest alone, the tree of equal tables, and
  the bisecting program where it runs here.
* `link N`: 10 and 1,000 differences over a link with 50 ms added: the old
  halving, the leaves, the sketch.
* `cutover N`: a tail keeping its proof carries 10,000 changes, the source
  stops, and the final proof from the kept leaves is timed against the
  full re-scan (the table digest) and the one-scan tree.
* `tail`: the tail's rate with and without the kept proof.
"""
import asyncio
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

SRC, DST = ("migkit-test-proof-src", 16170), ("migkit-test-proof-dst", 16171)
PROXY = {16170: 16174, 16171: 16175}
DB = "postgres"


def _sh(argv, **kw):
    return subprocess.run(argv, capture_output=True, text=True, **kw)


def _pg(name, sql):
    got = _sh(["docker", "exec", "-i", "-e", "PGPASSWORD=test", name, "psql",
               "-U", "postgres", "-h", "127.0.0.1", "-v", "ON_ERROR_STOP=1",
               "-At"], input=sql)
    if got.returncode:
        raise SystemExit(got.stderr[-400:])
    return got.stdout.strip()


def _start():
    for name, port in (SRC, DST):
        _sh(["docker", "rm", "-f", "-v", name])
        _sh(["docker", "run", "-d", "--name", name, "-e",
             "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{port}:5432",
             "postgres:16", "-c", "wal_level=logical", "-c",
             "shared_buffers=256MB"], check=True)
    for name, port in (SRC, DST):
        end = time.time() + 120
        while time.time() < end:
            if _sh(["docker", "exec", "-e", "PGPASSWORD=test", name, "psql",
                    "-U", "postgres", "-h", "127.0.0.1", "-c",
                    "select 1"]).returncode == 0:
                break
            time.sleep(1)


def _stop():
    for name, _ in (SRC, DST):
        _sh(["docker", "rm", "-f", "-v", name])


def _table(n, differ=10, scattered=True):
    """t (id, s, v, amount, ts) of n rows on both sides, `differ` rows
    apart: four in ten changed, three missing, three extra."""
    make = (f"drop table if exists public.t; create table public.t"
            " (id bigint primary key, s text, v int, amount numeric(12,2),"
            " ts timestamp);"
            f" insert into public.t select g, md5(g::text), g % 1000,"
            f" g / 7.0, timestamp '2024-01-01' + g * interval '1 s'"
            f" from generate_series(1, {n}) g; vacuum analyze public.t;")
    _pg(SRC[0], make)
    _pg(DST[0], make)
    import random
    rnd = random.Random(differ)
    ids = rnd.sample(range(1, n + 1), differ)
    k = differ * 4 // 10
    m = differ * 3 // 10
    changed, missing = ids[:k], ids[k:k + m]
    extra = [n + 1 + i for i in range(differ - k - m)]
    sql = []
    if changed:
        sql.append("update public.t set v = -1 where id in ("
                   + ",".join(map(str, changed)) + ")")
    if missing:
        sql.append("delete from public.t where id in ("
                   + ",".join(map(str, missing)) + ")")
    if extra:
        sql.append("insert into public.t select g, 'x', 0, 0, null from"
                   " unnest(array[" + ",".join(map(str, extra)) + "]) g")
    if sql:
        _pg(DST[0], "; ".join(sql) + "; vacuum analyze public.t")
    return len(changed), len(missing), len(extra)


def _pair(src_port=SRC[1], dst_port=DST[1], **options):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="proof-bench", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=src_port,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=dst_port,
                              user="postgres", password="test"),
              databases=[DB], options=options)
    where = Path(os.environ.get("TMPDIR", "/tmp")) / "migkit-proof-bench"
    where.mkdir(parents=True, exist_ok=True)
    for f in where.iterdir():
        if f.is_file():
            f.unlink()
    hop.report_dir = lambda db=None: where
    return PostgresEngine(hop)._as_pair()


def old_bisect(pair, db, src_t, dst_t, src_cols, dst_cols, scope,
               leaf_rows=2000, budget=None):
    """The key range halved with a digest of each half on both sides, as
    `hetero._bisect` did until the one-scan tree replaced it (the same
    code, kept here to be measured against)."""
    src, dst = pair.src_engine, pair.dst_engine
    (k,) = list(src.neutral_key("src", db, src_t))
    bounds = []
    for eng, side, table, where in ((src, "src", src_t, scope[0]),
                                    (dst, "dst", dst_t, scope[1])):
        got = eng.run_rule(side, db, (
            f"select min({eng._quote_ident(k)}),"
            f" max({eng._quote_ident(k)}) from"
            f" {eng._qualified(side, db, table)}"))
        bounds += [v for v in got[0] if v is not None]
    asked = [0]
    stop = time.monotonic() + budget if budget else None

    def digest(eng, side, table, cols, lo, hi):
        q = eng._quote_ident(k)
        asked[0] += 1
        if stop and time.monotonic() > stop:
            raise TimeoutError(f"past {budget}s after {asked[0]} digests")
        return eng.neutral_digest(side, db, table, cols,
                                  where=f"{q} >= {int(lo)} and {q} <"
                                        f" {int(hi)}")
    found, todo = [], [(int(min(bounds)), int(max(bounds)) + 1)]
    while todo:
        lo, hi = todo.pop()
        a = digest(src, "src", src_t, src_cols, lo, hi)
        b = digest(dst, "dst", dst_t, dst_cols, lo, hi)
        if a == b:
            continue
        if max(a[0], b[0]) <= leaf_rows or hi - lo <= 1:
            found.append((lo, hi))
            continue
        mid = lo + (hi - lo) // 2
        todo += [(mid, hi), (lo, mid)]
    missing, changed, extra = [], [], []
    for lo, hi in sorted(found):
        if stop and time.monotonic() > stop:
            raise TimeoutError(f"past {budget}s walking the ranges")
        wheres = []
        for eng in (src, dst):
            q = eng._quote_ident(k)
            wheres.append(f"{q} >= {lo} and {q} < {hi}")
        got = pair._localise(db, "t", src, src_t, src_cols, dst, dst_t,
                             dst_cols, tuple(wheres))
        missing += got[0]
        changed += got[1]
        extra += got[2]
    return {"missing": len(missing), "changed": len(changed),
            "extra": len(extra), "digests": asked[0]}


def _timed(fn):
    began = time.monotonic()
    got = fn()
    return round(time.monotonic() - began, 2), got


def _cols(pair):
    sc, dc, _ = pair._column_plan("public.t", "public.t", DB)
    return sc, dc


def verify(n):
    from migkit import leaves
    out = {"rows": n}
    _table(n)
    pair = _pair()
    sc, dc = _cols(pair)
    a, b = leaves.sides(pair, DB, "public.t", "public.t", sc, dc,
                        (None, None))

    def digests():
        return leaves.both(lambda s: s.eng.neutral_digest(
            s.side, DB, s.table, s.cols), a, b)
    out["table digest, both sides"], _ = _timed(digests)
    out["old: halving"], got = _timed(lambda: old_bisect(
        pair, DB, "public.t", "public.t", sc, dc, (None, None)))
    out["old: halving found"] = got

    def tree(kind):
        lohi = leaves.key_range(a, b) if kind == "range" else (None, None)
        spec = leaves.plan(n, kind, *lohi, salt="b3")
        ta, tb = leaves.both(leaves.scan, a, b, spec)
        found = leaves.localize(a, b, spec, ta, tb)
        return {"leaves": len(ta.leaves), "differ": found.leaves,
                "rows read": found.rows, "complete": found.complete,
                "found": [len(found.missing), len(found.changed),
                          len(found.extra)]}
    out["new: one scan, range leaves"], got = _timed(lambda: tree("range"))
    out["new: range found"] = got
    out["new: one scan, hash leaves"], got = _timed(lambda: tree("hash"))
    out["new: hash found"] = got
    out["whole verify old (digest + halving)"] = round(
        out["table digest, both sides"] + out["old: halving"], 2)
    out["whole verify new (digest + range leaves)"] = round(
        out["table digest, both sides"] + out["new: one scan, range leaves"],
        2)
    # equal tables: what the tree costs over the plain digest
    _pg(DST[0], f"delete from public.t where id > {n};"
                " insert into public.t select g, md5(g::text), g % 1000,"
                f" g / 7.0, timestamp '2024-01-01' + g * interval '1 s' from"
                f" generate_series(1, {n}) g on conflict (id) do update set"
                " v = excluded.v")
    spec = leaves.plan(n, "hash", salt="b3")
    out["equal: one-scan tree, both sides"], trees = _timed(
        lambda: leaves.both(leaves.scan, a, b, spec))
    assert trees[0].leaves == trees[1].leaves
    out["equal: table digest, both sides"], _ = _timed(digests)
    _table(n)
    from migkit.util import which
    if which("reladiff"):
        from migkit.util import PrivateFile, diff_run_config, run
        url = "postgresql://postgres:test@127.0.0.1:{}/postgres"
        conf = diff_run_config(url.format(SRC[1]), "t", url.format(DST[1]),
                               "t")

        def bisecting():
            with PrivateFile(conf, ".toml") as path:
                p = run(["reladiff", "--conf", path, "--stats", "-j", "2",
                         "-k", "id", "-c", "%"], check=False, timeout=3600)
            return p.stdout[-200:]
        out["bisecting program"], said = _timed(bisecting)
        out["bisecting program said"] = said.strip().splitlines()[:5]
    return out


def versus(n):
    """The bisecting program over every column against the table digest
    and the range leaves, on a table of n rows with 10 apart."""
    from migkit import leaves
    from migkit.util import PrivateFile, diff_run_config, run, which
    out = {"rows": n}
    _table(n)
    pair = _pair()
    sc, dc = _cols(pair)
    a, b = leaves.sides(pair, DB, "public.t", "public.t", sc, dc,
                        (None, None))

    def ours():
        leaves.both(lambda s: s.eng.neutral_digest(s.side, DB, s.table,
                                                   s.cols), a, b)
        spec = leaves.plan(n, "range", *leaves.key_range(a, b), salt="e6")
        ta, tb = leaves.both(leaves.scan, a, b, spec)
        f = leaves.localize(a, b, spec, ta, tb)
        return [len(f.missing), len(f.changed), len(f.extra), f.complete]
    out["digest + range leaves"], out["found"] = _timed(ours)
    if which("reladiff"):
        url = "postgresql://postgres:test@127.0.0.1:{}/postgres"
        conf = diff_run_config(url.format(SRC[1]), "t", url.format(DST[1]),
                               "t")
        for jobs in ("2", "8"):
            def bisecting():
                with PrivateFile(conf, ".toml") as path:
                    p = run(["reladiff", "--conf", path, "--stats", "-j",
                             jobs, "-k", "id", "-c", "%"], check=False,
                            timeout=3600)
                return [l for l in p.stdout.splitlines() if "rows" in l]
            out[f"bisecting program, every column, -j {jobs}"], \
                out[f"it said (-j {jobs})"] = _timed(bisecting)
    return out


def variants(n):
    """The one-scan statement as written (the inner rows fenced with
    `offset 0`) against the same without the fence, and the plain digest,
    on one PostgreSQL side of n rows: seconds, and whether the plan ran
    in parallel."""
    import psycopg2

    from migkit import leaves
    _table(n, differ=0)
    pair = _pair()
    sc, dc = _cols(pair)
    a, _ = leaves.sides(pair, DB, "public.t", "public.t", sc, dc,
                        (None, None))
    spec = leaves.plan(n, "hash", salt="f7")
    fenced = leaves._scan_sql(a, spec)
    loose = fenced.replace(" offset 0)", ")")
    from migkit import canon
    row = canon.row_expr("postgres", sc)
    plain = (f"select count(*), {canon.digest_expr('postgres', row)}"
             " from public.t")
    conn = psycopg2.connect(host="127.0.0.1", port=SRC[1], user="postgres",
                            password="test", dbname=DB)
    conn.autocommit = True
    out = {"rows": n}
    with conn.cursor() as cur:
        for name, sql in (("plain digest", plain), ("tree, fenced", fenced),
                          ("tree, not fenced", loose)):
            cur.execute("explain " + sql)
            plan = "\n".join(r[0] for r in cur.fetchall())
            began = time.monotonic()
            cur.execute(sql)
            cur.fetchall()
            out[name] = {"seconds": round(time.monotonic() - began, 2),
                         "parallel": "Gather" in plan}
    conn.close()
    return out


class Lag:
    """A TCP proxy holding each chunk for half the round trip each way."""

    def __init__(self, rtt_ms):
        self.half = rtt_ms / 2000.0
        self.loop = asyncio.new_event_loop()
        self.ready = threading.Event()
        threading.Thread(target=self._run, daemon=True).start()
        self.ready.wait(10)

    def _run(self):
        asyncio.set_event_loop(self.loop)
        servers = []

        async def pipe(r, w):
            try:
                while True:
                    data = await r.read(65536)
                    if not data:
                        break
                    await asyncio.sleep(self.half)
                    w.write(data)
                    await w.drain()
            except Exception:  # noqa: BLE001 - one end gone
                pass
            finally:
                w.close()

        def serve(to):
            async def handle(r, w):
                ur, uw = await asyncio.open_connection("127.0.0.1", to)
                await asyncio.gather(pipe(r, uw), pipe(ur, w))
            return handle

        async def start():
            for real, lagged in PROXY.items():
                servers.append(await asyncio.start_server(
                    serve(real), "127.0.0.1", lagged))
            self.ready.set()
        self.loop.run_until_complete(start())
        self.loop.run_forever()


def link(n, rtt=50):
    from migkit import leaves
    Lag(rtt)
    out = {"rows": n, "rtt_ms": rtt}
    for d in (10, 1000):
        _table(n, differ=d)
        pair = _pair(PROXY[SRC[1]], PROXY[DST[1]])
        sc, dc = _cols(pair)
        a, b = leaves.sides(pair, DB, "public.t", "public.t", sc, dc,
                            (None, None))
        row = {}
        row["measured rtt ms"] = round(leaves.rtt_ms(a.eng, "src", DB), 1)
        try:
            row["old: halving"], got = _timed(lambda: old_bisect(
                pair, DB, "public.t", "public.t", sc, dc, (None, None),
                budget=900))
            row["old found"] = got
        except TimeoutError as e:
            row["old: halving"] = f"> 900s ({e})"

        def by_leaves():
            spec = leaves.plan(n, "range", *leaves.key_range(a, b),
                               salt="c4")
            ta, tb = leaves.both(leaves.scan, a, b, spec)
            f = leaves.localize(a, b, spec, ta, tb)
            return [f.complete, len(f.missing), len(f.changed),
                    len(f.extra)]
        row["leaves"], row["leaves found"] = _timed(by_leaves)

        def by_sketch():
            spec = leaves.plan(n, "hash", salt="c4")
            f, why = leaves.by_sketch(a, b, spec, d)
            return why or [f.complete, len(f.missing), len(f.changed),
                           len(f.extra)]
        row["sketch"], row["sketch found"] = _timed(by_sketch)
        out[f"{d} differences"] = row
    return out


def _tail_thread(pair, token_path, lines):
    import ctypes
    done = threading.Event()

    def run():
        try:
            pair.tail_apply(DB, True, token_path, lines.append)
        except BaseException as e:  # noqa: BLE001
            lines.append(f"exit {type(e).__name__} {e}")
        finally:
            done.set()
    t = threading.Thread(target=run, daemon=True)
    t.start()

    def stop():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(t.ident), ctypes.py_object(KeyboardInterrupt))
        done.wait(120)
    return stop, done


def _until(pair, token_path, end, done, timeout=1800):
    began = time.time()
    while time.time() - began < timeout and not done.is_set():
        try:
            at = json.loads(token_path.read_text()).get("token")
        except (OSError, ValueError):
            at = None
        if at and pair.src_engine.position_reached(at, end):
            return time.time() - began
        time.sleep(0.2)
    raise SystemExit("the tail did not catch up")


def cutover(n, changes=10_000, last=100):
    """A tail keeping its proof: `changes` scattered updates, then a pass
    over what they touched (as the background pass makes one every few
    minutes), then the last window - `last` more - and the source stops:
    the final proof from the kept leaves, against the full re-scan."""
    from migkit import kept, leaves
    _table(n, differ=0)
    _pg(SRC[0], "alter table public.t replica identity full;"
                " select pg_drop_replication_slot(slot_name) from"
                " pg_replication_slots")
    pair = _pair(keep_proof=True)
    where = pair.hop.report_dir(DB)
    token_path = where / "tail-token.json"
    pair.tail_start(DB, token_path)
    lines = []
    stop, done = _tail_thread(pair, token_path, lines)
    out = {"rows": n, "changes": changes, "last window": last}

    def scattered(k):
        _pg(SRC[0], f"update public.t set v = v + 1 where id in (select"
                    f" (random() * {n - 1})::bigint + 1 from"
                    f" generate_series(1, {k}))")
        return _until(pair, token_path,
                      _pg(SRC[0], "select pg_current_wal_lsn()"), done)
    try:
        _pg(SRC[0], "update public.t set v = v + 1 where id = 1")
        began = time.time()
        while not (where / kept.FILE).exists() or not kept.Store(
                where / kept.FILE).tables():
            if time.time() - began > 1800 or done.is_set():
                raise SystemExit(lines[-5:])
            time.sleep(0.5)
        out["seed (both sides read once)"] = round(time.time() - began, 1)
        out["tail caught up"] = round(scattered(changes), 1)
        out["a pass over what they touched"], got = _timed(
            lambda: kept.prove(pair, DB))
        out["that pass said"] = got["t"][1][:200] if got else None
        scattered(last)
        out["final proof: kept leaves"], got = _timed(
            lambda: kept.prove(pair, DB))
        out["final proof said"] = got["t"][1][:240] if got else None
        sc, dc = _cols(pair)
        a, b = leaves.sides(pair, DB, "public.t", "public.t", sc, dc,
                            (None, None))
        out["final proof: full re-scan (table digest)"], _ = _timed(
            lambda: leaves.both(lambda s: s.eng.neutral_digest(
                s.side, DB, s.table, s.cols), a, b))
        spec = leaves.plan(n, "range", *leaves.key_range(a, b), salt="d5")
        out["final proof: one-scan tree"], _ = _timed(
            lambda: leaves.both(leaves.scan, a, b, spec))
    finally:
        stop()
        _pg(SRC[0], "select pg_drop_replication_slot(slot_name) from"
                    " pg_replication_slots")
    return out


def tail(rows=100_000):
    """The tail's rate carrying `rows` inserts and as many updates, with
    and without the kept proof."""
    out = {}
    for keep in (False, True):
        _table(1000, differ=0)
        _pg(SRC[0], "alter table public.t replica identity full;"
                    " select pg_drop_replication_slot(slot_name) from"
                    " pg_replication_slots")
        pair = _pair(**({"keep_proof": True} if keep else {}))
        where = pair.hop.report_dir(DB)
        token_path = where / "tail-token.json"
        pair.tail_start(DB, token_path)
        lines = []
        stop, done = _tail_thread(pair, token_path, lines)
        try:
            _pg(SRC[0], "update public.t set v = v + 1 where id = 1")
            time.sleep(3)
            _pg(SRC[0], f"insert into public.t select g, md5(g::text), 0, 1,"
                        f" null from generate_series(2000, {2000 + rows}) g;"
                        " update public.t set v = v + 1 where id >= 2000")
            end = _pg(SRC[0], "select pg_current_wal_lsn()")
            took = _until(pair, token_path, end, done)
            out["kept proof" if keep else "plain"] = {
                "changes": 2 * rows, "seconds": round(took, 1),
                "changes/s": round(2 * rows / took)}
        finally:
            stop()
            _pg(SRC[0], "select pg_drop_replication_slot(slot_name) from"
                        " pg_replication_slots")
    return out


def main(argv):
    todo, i = [], 0
    while i < len(argv):
        what = argv[i]
        i += 1
        sizes = []
        while i < len(argv) and argv[i].isdigit():
            sizes.append(int(argv[i]))
            i += 1
        todo.append((what, sizes))
    _start()
    runs = {"verify": lambda n: verify(n), "link": lambda n: link(n),
            "versus": lambda n: versus(n),
            "variants": lambda n: variants(n),
            "cutover": lambda n: cutover(n), "tail": lambda n: tail()}
    try:
        for what, sizes in todo:
            for n in sizes or [1_000_000]:
                try:
                    print(json.dumps({what: runs[what](n)}), flush=True)
                except BaseException as e:  # noqa: BLE001 - the next one
                    import traceback
                    print(json.dumps({what: {"rows": n, "failed":
                                             traceback.format_exc()[-800:]}}),
                          flush=True)
                    if isinstance(e, KeyboardInterrupt):
                        raise
    finally:
        _stop()


if __name__ == "__main__":
    main(sys.argv[1:] or ["verify", "1000000", "5000000", "link", "1000000",
                          "cutover", "1000000", "tail"])
