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

    def check_data(self, db, table=None, stream=None):
        s = self._client("src", db)
        t = self._client("dst", db)
        sample = int(self.hop.options.get("sample", 5000))
        deep = bool(self.hop.options.get("deep", False))
        checked = bad = 0
        cursor = 0
        while True:
            cursor, keys = s.scan(cursor, count=1000)
            for k in keys:
                if not deep and checked >= sample:
                    cursor = 0
                    break
                typ = s.type(k)
                if t.type(k) != typ:
                    bad += 1
                elif typ == "string" and s.get(k) != t.get(k):
                    bad += 1
                elif typ == "hash" and s.hgetall(k) != t.hgetall(k):
                    bad += 1
                elif typ in ("set",) and s.smembers(k) != t.smembers(k):
                    bad += 1
                elif typ == "zset" and s.zrange(k, 0, -1, withscores=True) != \
                        t.zrange(k, 0, -1, withscores=True):
                    bad += 1
                elif typ == "list" and s.lrange(k, 0, -1) != t.lrange(k, 0, -1):
                    bad += 1
                checked += 1
            if cursor == 0:
                break
        mode = "full scan" if deep else f"sample {checked}"
        if bad:
            return [Result("data", f"db{db}", "diff",
                           f"{bad}/{checked} keys differ ({mode})", "",
                           "full sync: use RIOT (riot replicate) or"
                           " redis-shake, both verify and resume")]
        return [Result("data", f"db{db}", "ok", f"{checked} keys checked ({mode})")]

    def watch_sample(self, db):
        return {"db": f"db{db}", "ts": time.time(),
                "src_rows": self._client("src", db).dbsize(),
                "dst_rows": self._client("dst", db).dbsize()}
