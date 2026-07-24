import time

from .base import Engine, Result


class RedisEngine(Engine):
    checks = ("counts", "data")

    def _client(self, side, db=0):
        ep = self.hop.source if side == "src" else self.hop.target
        try:
            import redis
        except ImportError:
            raise SystemExit("pip install 'migkit[redis]' for redis support")
        return redis.Redis(host=ep.host, port=ep.port,
                           password=ep.password or None, db=int(db),
                           socket_timeout=15, decode_responses=True)

    def databases(self):
        if self.hop.databases:
            return [str(d) for d in self.hop.databases]
        info = self._client("src").info("keyspace")
        return sorted(k[2:] for k in info) or ["0"]

    def check_counts(self, db):
        a = self._client("src", db).dbsize()
        b = self._client("dst", db).dbsize()
        if a != b:
            return [Result("counts", f"db{db}", "diff", f"src={a} dst={b}")]
        return [Result("counts", f"db{db}", "ok", f"keys {a:,}=={b:,}")]

    def _batch_compare(self, s, t, keys):
        """Pipelined type-aware compare: two round trips per batch per
        side instead of one per key."""
        bad = []
        ps, pt = s.pipeline(transaction=False), t.pipeline(transaction=False)
        for k in keys:
            ps.type(k)
            pt.type(k)
        stypes, dtypes = ps.execute(), pt.execute()
        ps, pt = s.pipeline(transaction=False), t.pipeline(transaction=False)
        plan = []
        for k, ty, dty in zip(keys, stypes, dtypes):
            if ty != dty:
                bad.append(k)
                continue
            for p in (ps, pt):
                if ty == "string":
                    p.get(k)
                elif ty == "hash":
                    p.hgetall(k)
                elif ty == "set":
                    p.smembers(k)
                elif ty == "zset":
                    p.zrange(k, 0, -1, withscores=True)
                elif ty == "list":
                    p.lrange(k, 0, -1)
                elif ty == "stream":
                    p.xlen(k)
                else:
                    p.exists(k)
            plan.append(k)
        for k, a, b in zip(plan, ps.execute(), pt.execute()):
            if a != b:
                bad.append(k)
        return bad

    def check_data(self, db, table=None, stream=None):
        s = self._client("src", db)
        t = self._client("dst", db)
        sample = int(self.hop.options.get("sample", 5000))
        deep = bool(self.hop.options.get("deep", False))
        checked = 0
        bad = 0
        cursor = 0
        while True:
            cursor, keys = s.scan(cursor, count=1000)
            if not deep and checked + len(keys) > sample:
                keys = keys[:max(0, sample - checked)]
            if keys:
                bad += len(self._batch_compare(s, t, keys))
                checked += len(keys)
            if stream and checked and checked % 20000 < 1000:
                stream(f"db{db}: {checked} keys compared")
            if cursor == 0 or (not deep and checked >= sample):
                break
        mode = "full scan" if deep else f"sample {checked}"
        if bad:
            return [Result("data", f"db{db}", "diff",
                           f"{bad}/{checked} keys differ ({mode})", "",
                           "full sync: use RIOT (riot replicate) or"
                           " redis-shake, both verify and resume")]
        return [Result("data", f"db{db}", "ok",
                       f"{checked} keys value-equal ({mode}, pipelined)")]

    def check_deep(self, db):
        s = self._client("src", db)
        t = self._client("dst", db)
        sample = int(self.hop.options.get("sample", 5000))
        res = []
        # ttl drift: movers frequently drop or reset expirations; a key
        # that outlives its source ttl serves stale data forever
        cursor = 0
        seen = 0
        no_ttl = []
        drifted = []
        big = []
        while seen < sample:
            cursor, keys = s.scan(cursor, count=1000)
            if keys:
                ps = s.pipeline(transaction=False)
                pt = t.pipeline(transaction=False)
                pm = s.pipeline(transaction=False)
                for k in keys:
                    ps.pttl(k)
                    pt.pttl(k)
                    pm.memory_usage(k, samples=0)
                for k, a, b, mem in zip(keys, ps.execute(), pt.execute(),
                                        pm.execute()):
                    seen += 1
                    if mem:
                        big.append((mem, k))
                    if a is None or a < 0:
                        continue  # no ttl on source
                    if b is None or b == -2:
                        continue  # key missing on dst, counts covers it
                    if b == -1:
                        no_ttl.append(k)
                    elif abs(a - b) > max(60000, a * 0.1):
                        drifted.append(f"{k} src={a // 1000}s"
                                       f" dst={b // 1000}s")
            if cursor == 0:
                break
        ttl_bad = ([f"{len(no_ttl)} keys lost their ttl on target:"
                    f" {', '.join(no_ttl[:4])}"] if no_ttl else []) \
            + ([f"{len(drifted)} ttls drifted >10%:"
                f" {', '.join(drifted[:3])}"] if drifted else [])
        res.append(Result("deep", f"db{db} ttl",
                          "diff" if ttl_bad else "ok",
                          "; ".join(ttl_bad) if ttl_bad
                          else f"{seen} keys sampled, ttls match on target",
                          "", "re-set expirations on target before cutover"
                          if ttl_bad else ""))
        big.sort(reverse=True)
        top = big[:5]
        missing_big = []
        for mem, k in top:
            if not t.exists(k):
                missing_big.append(k)
        res.append(Result("deep", f"db{db} bigkeys",
                          "diff" if missing_big else "ok",
                          (f"biggest keys missing on target:"
                           f" {', '.join(missing_big)}") if missing_big
                          else "top keys by memory present on target: "
                          + ", ".join(f"{k} ({mem // 1024}KB)"
                                      for mem, k in top), "",
                          "big keys often exceed proxy/mover limits,"
                          " copy them explicitly" if missing_big else ""))
        return res

    def delta_verify(self, db, limit=20000, log=None):
        # redis has no built-in change log to diff against; honest about it
        # rather than faking a delta from a full scan
        return [Result("delta", f"db{db}", "error",
                       "redis has no native change log for O(changes) delta;"
                       " enable keyspace notifications (config set"
                       " notify-keyspace-events KEA) and consume __keyevent__,"
                       " or use RIOT/redis-shake which stream changes."
                       " Use check --deep for full-scan verification instead")]

    def watch_sample(self, db):
        return {"db": f"db{db}", "ts": time.time(),
                "src_rows": self._client("src", db).dbsize(),
                "dst_rows": self._client("dst", db).dbsize()}
