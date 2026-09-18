import time

from .base import Engine, RepairAction, Result


class RedisEngine(Engine):
    checks = ("counts", "data")
    ENGINE_FAMILY = "redis"

    def _client(self, side, db=0, decode=True):
        """`decode=False` for DUMP payloads, which are binary: decoding one
        as text is how a repair would corrupt what it copied."""
        ep = self.hop.source if side == "src" else self.hop.target
        try:
            import redis
        except ImportError:
            raise SystemExit("pip install 'migkit[redis]' for redis support")
        return redis.Redis(host=ep.host, port=ep.port,
                           password=ep.password or None, db=int(db),
                           socket_timeout=15, decode_responses=decode)

    def databases(self):
        if self.hop.databases:
            return [str(d) for d in self.hop.databases]
        info = self._client("src").info("keyspace")
        return sorted(k[2:] for k in info) or ["0"]

    def _brand_probes(self):
        """The whole INFO from each side, not just the Server section.

        Whole, because KeyDB publishes nothing named for itself up there -
        measured, its Server section carries `redis_version:6.3.4` and no
        more. What does name it, `server_threads` and `mvcc_depth`, is
        further down in the same reply.
        """
        def info(side):
            try:
                return self._client(side).info()
            except Exception:
                return {}
        return (info("src"), info("dst"))

    def _server_versions(self):
        """Each side's own version, not the protocol number it advertises.

        `redis_version` is a compatibility claim on every fork: measured, a
        Valkey 8.1.10 server reports 7.2.4 and a Dragonfly df-v2.0.0 reports
        7.4.0. Reading it as the version put a Redis source and a Valkey
        target side by side as an exact match.
        """
        s, d = self._brands()
        return (s.version or None, d.version or None)

    def _assess_extra(self):
        """What has to be true before a Redis move is worth starting.

        Persistence is the one that matters and is easy to miss: a target
        with neither RDB snapshots nor AOF keeps everything in memory, so a
        restart between the load and the cutover loses the whole database
        without any error anywhere.
        """
        items = []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "instance",
                          "item": item, "detail": str(detail)})
        try:
            s = self._client("src").info()
            d = self._client("dst").info()
        except Exception as e:
            add("warn", "cannot read INFO from both sides",
                f"{str(e)[:90]} - unknown, not clean")
            return items

        # `rdb_last_save_time` is set at startup whether or not snapshots are
        # configured - measured, it is identical on a default server and on
        # one started with `--save ""`. Reading it as evidence of persistence
        # reported a memory-only target as safe, which is the worst direction
        # for this check to be wrong in. The configuration is the signal.
        try:
            save = str(self._client("dst").config_get("save").get("save", ""))
            aofc = str(self._client("dst").config_get("appendonly")
                       .get("appendonly", "no"))
        except Exception as e:
            add("warn", "cannot read the target's persistence settings",
                f"{str(e)[:90]} - unknown, not clean")
            save = aofc = None
        if save is not None:
            keeps = bool(save.strip()) or aofc.lower() == "yes"
            add("pass" if keeps else "warn",
                "target keeps the data on disk",
                f"save={save.strip() or '(none)'} appendonly={aofc}"
                + ("" if keeps else
                   " - memory only, so a restart between the load and the"
                   " cutover loses everything with no error anywhere"))

        pol = str(d.get("maxmemory_policy", "?"))
        add("pass" if pol in ("noeviction", "?") else "fail",
            "target eviction policy",
            f"{pol}" + ("" if pol in ("noeviction", "?") else
                        " - the target will silently drop keys under memory"
                        " pressure, which reads as a migration that lost data"))

        smax = int(s.get("maxmemory", 0) or 0)
        dmax = int(d.get("maxmemory", 0) or 0)
        used = int(s.get("used_memory", 0) or 0)
        if dmax and used > dmax:
            add("fail", "target memory limit is below the source's usage",
                f"source uses {used:,} bytes, target caps at {dmax:,}")
        else:
            add("pass", "target memory limit",
                f"source uses {used:,} bytes, target caps at"
                f" {dmax or 'unlimited'}"
                + (f" (source caps at {smax:,})" if smax else ""))
        return items

    def _health(self, side):
        """What this server says about its own load, for the throttle.

        `connected_clients` against `maxclients` is the same shape the other
        engines use: work in flight against the ceiling, not throughput.

        Throughput was the obvious choice and it is the wrong one. Measured,
        `instantaneous_ops_per_sec` read **287,248** on a Redis whose own
        latency probe said 0.02 ms - a server doing a great deal of work
        perfectly comfortably - and it still read **287,187** a full two
        seconds after the load stopped, because it is a rolling sample rather
        than a reading of now. A throttle built on it would have backed off
        from a fast healthy server and kept going against a struggling slow
        one, which is the wrong way round.

        A background save or AOF rewrite is folded up to the busy threshold
        the way MongoDB folds its queue: Redis forks for those, and a fork on
        a large keyspace is the one moment when adding a full scan is
        genuinely unkind. Measured, `rdb_bgsave_in_progress` is 1 while it
        runs.
        """
        try:
            info = self._client(side).info()
        except Exception:
            return None
        return self._health_from(info)

    @staticmethod
    def _health_from(info):
        """The reading, taken apart from the fetching.

        One INFO reply in, one Health out, so what the server said and what
        migkit made of it can be judged together. Two separate samples cannot
        be: a background save that ends between them makes the pair disagree
        without either being wrong.
        """
        from ..throttle import BUSY_RATIO, Health
        busy = None
        ceiling = float(info.get("maxclients") or 0)
        if ceiling:
            busy = float(info.get("connected_clients") or 0) / ceiling
        if (info.get("rdb_bgsave_in_progress")
                or info.get("aof_rewrite_in_progress")):
            busy = max(busy or 0.0, BUSY_RATIO)
        if busy is None:
            # the server would not say, which is not the same as idle
            return None
        return Health(busy_ratio=busy,
                      note="rewriting to disk" if busy >= BUSY_RATIO else "")

    def check_counts(self, db):
        a = self._client("src", db).dbsize()
        b = self._client("dst", db).dbsize()
        if a != b:
            return [Result("counts", f"db{db}", "diff", f"src={a} dst={b}")]
        return [Result("counts", f"db{db}", "ok", f"keys {a:,}=={b:,}")]

    def _batch_compare(self, s, t, keys):
        """Pipelined type-aware compare: two round trips per batch per
        side instead of one per key.

        (missing, changed) rather than one list of bad keys, because the
        repair does different things with them and the report says which is
        which. A key the target does not have answers `none` to TYPE, which
        is how the two are told apart without another round trip.
        """
        missing, changed = [], []
        ps, pt = s.pipeline(transaction=False), t.pipeline(transaction=False)
        for k in keys:
            ps.type(k)
            pt.type(k)
        stypes, dtypes = ps.execute(), pt.execute()
        ps, pt = s.pipeline(transaction=False), t.pipeline(transaction=False)
        plan = []
        for k, ty, dty in zip(keys, stypes, dtypes):
            if ty != dty:
                (missing if dty == "none" else changed).append(k)
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
                changed.append(k)
        return missing, changed

    def _scan_batches(self, client, sample, deep):
        """Batches of keys from one side, up to the sample cap."""
        cursor = 0
        seen = 0
        while True:
            cursor, keys = client.scan(cursor, count=1000)
            if not deep and seen + len(keys) > sample:
                keys = keys[:max(0, sample - seen)]
            if keys:
                seen += len(keys)
                yield keys
            if cursor == 0 or (not deep and seen >= sample):
                break

    def check_data(self, db, table=None, stream=None):
        """Both directions, because scanning the source only sees one of
        them.

        A key the target has and the source never did is invisible to a scan
        of the source, and the count check cannot see it either: measured,
        deleting one key on the target and adding one stray key there leaves
        `dbsize` at 20 on both sides, and the old pair of checks reported
        `keys 20==20` and said nothing about the stray. A target still
        carrying keys from an earlier attempt is exactly what that looks
        like, and every other engine names what the target has and the source
        does not.
        """
        from ..throttle import Throttle
        s = self._client("src", db)
        t = self._client("dst", db)
        sample = int(self.hop.options.get("sample", 5000))
        deep = bool(self.hop.options.get("deep", False))
        checked = 0
        bad = 0
        # A SCAN plus a pipeline of reads is the heaviest thing a verifier
        # does to a single-threaded server, and nothing else was slowing it
        # down: this loop ran at whatever speed the network allowed, against
        # a Redis that might also be serving an application. Each side is
        # gated on its own health, since each pass leans on a different one.
        src_gate = Throttle(1, probe=lambda: self._health("src"))
        dst_gate = Throttle(1, probe=lambda: self._health("dst"))
        missing, changed = [], []
        for keys in self._scan_batches(s, sample, deep):
            with src_gate.unit():
                gone, differ = self._batch_compare(s, t, keys)
            missing.extend(gone)
            changed.extend(differ)
            checked += len(keys)
            if stream and checked % 20000 < 1000:
                stream(f"db{db}: {checked} keys compared")
        bad = len(missing) + len(changed)
        extra = []
        seen_dst = 0
        for keys in self._scan_batches(t, sample, deep):
            with dst_gate.unit():
                pipe = s.pipeline(transaction=False)
                for k in keys:
                    pipe.exists(k)
                extra.extend(k for k, there in zip(keys, pipe.execute())
                             if not there)
            seen_dst += len(keys)
        self._write_drilldown(db, missing=missing, changed=changed,
                              extra=extra)
        mode = "full scan" if deep else f"sample {checked}"
        res = []
        if bad:
            parts = []
            if missing:
                parts.append(f"{len(missing)} missing on the target")
            if changed:
                parts.append(f"{len(changed)} with a different value")
            res.append(Result("data", f"db{db}", "diff",
                              f"{bad}/{checked} keys differ ({mode}):"
                              f" {', '.join(parts)}", "",
                              f"migkit sync {self.hop.name} --db {db}"
                              " --kind rows --apply, or a full sync with RIOT"
                              " (riot replicate) or redis-shake"))
        if extra:
            shown = ", ".join(sorted(extra)[:6])
            more = f" (+{len(extra) - 6} more)" if len(extra) > 6 else ""
            res.append(Result(
                "data", f"db{db} extra keys", "diff",
                f"{len(extra)} of {seen_dst} keys on the target are not on"
                f" the source: {shown}{more}", "",
                "a target still holding keys from an earlier attempt:"
                f" migkit sync {self.hop.name} --db {db} --kind rows --apply"
                " removes them, with the old values written to the undo file"
                " first; the key count can match while this is true"))
        return res or [Result(
            "data", f"db{db}", "ok",
            f"{checked} keys value-equal, {seen_dst} target keys all present"
            f" on the source ({mode}, pipelined)")]

    def _drilldown_path(self, db, kind):
        return self.hop.report_dir(db) / f"data-db{db}.{kind}"

    def _write_drilldown(self, db, **kinds):
        """The keys behind the counts, one per line, in the estate's file
        names - which is what makes a repair possible at all.

        A run that finds nothing removes the file rather than leaving the
        previous run's list behind for `sync` to act on.
        """
        for kind, keys in kinds.items():
            path = self._drilldown_path(db, kind)
            if keys:
                path.write_text("\n".join(sorted(keys)) + "\n")
            elif path.exists():
                path.unlink()

    def _read_drilldown(self, db, kind):
        path = self._drilldown_path(db, kind)
        if not path.exists():
            return []
        return [l for l in path.read_text().splitlines() if l]

    def repair_plan(self, db, kind):
        """What `migkit sync --kind rows` would do to this database.

        The keys come from the last check's drilldown files, so the plan
        describes what was actually found rather than re-scanning and acting
        on something the operator never saw.
        """
        if kind not in ("rows", "all"):
            return []
        missing = self._read_drilldown(db, "missing")
        changed = self._read_drilldown(db, "changed")
        extra = self._read_drilldown(db, "extra")
        if not (missing or changed or extra):
            return []
        statements = []
        if missing or changed:
            names = ", ".join((missing + changed)[:6])
            statements.append(
                f"RESTORE {len(missing) + len(changed)} keys from the source,"
                f" type and expiry included: {names}"
                + (" ..." if len(missing) + len(changed) > 6 else ""))
        if extra:
            statements.append(
                f"DEL {len(extra)} keys the source does not have:"
                f" {', '.join(extra[:6])}"
                + (" ..." if len(extra) > 6 else ""))
        return [RepairAction(
            f"db{db}", "rows", statements, [],
            f"{len(missing)} missing, {len(changed)} changed,"
            f" {len(extra)} extra; every key the repair overwrites or removes"
            " is dumped to the undo file before it is touched")]

    def apply(self, db, action):
        """Copy the keys back with DUMP/RESTORE, then remove the strays.

        DUMP carries the type and the expiry with the value, so one path
        covers strings, hashes, lists, sets, sorted sets and streams alike -
        measured across two servers, a hash came back a hash and a key with
        600s left came back with 599992 ms.

        It is also version-bound, and loudly: measured, a payload taken from
        Redis 7.4 and restored into Redis 6.2 answers `DUMP payload version
        or checksum are wrong` rather than writing anything. That is reported
        as it is instead of being worked around, because the way around it -
        re-issuing each value with type-specific commands - quietly changes
        what some types contain.

        Writes come first and deletions last: a repair cut off in the middle
        then leaves keys that should not be there, which the next check
        names, rather than a hole nothing looks for.
        """
        import base64
        import json
        raw_s = self._client("src", db, decode=False)
        raw_t = self._client("dst", db, decode=False)
        undo_dir = self.hop.report_dir(db) / "undo"
        undo_dir.mkdir(parents=True, exist_ok=True)
        undo = undo_dir / f"db{db}.keys.jsonl"
        missing = self._read_drilldown(db, "missing")
        changed = self._read_drilldown(db, "changed")
        extra = self._read_drilldown(db, "extra")

        def remember(client, key, handle):
            payload = client.dump(key.encode())
            if payload is None:
                return
            ttl = client.pttl(key.encode())
            handle.write(json.dumps({
                "key": key, "pttl": ttl,
                "dump": base64.b64encode(payload).decode()}) + "\n")

        with undo.open("a") as handle:
            done = 0
            for key in missing + changed:
                remember(raw_t, key, handle)
                payload = raw_s.dump(key.encode())
                if payload is None:
                    continue    # gone from the source since the check
                ttl = raw_s.pttl(key.encode())
                try:
                    raw_t.restore(key.encode(), ttl if ttl and ttl > 0 else 0,
                                  payload, replace=True)
                except Exception as e:
                    raise SystemExit(
                        f"RESTORE of {key!r} was refused by the target:"
                        f" {str(e)[:90]}. {done} keys were copied before"
                        " this one and nothing has been deleted. A payload"
                        " version error means the target runs an older Redis"
                        " than the source - migrate with RIOT (riot"
                        " replicate) or redis-shake, which re-issue values"
                        " instead of moving RDB payloads")
                done += 1
            for key in extra:
                remember(raw_t, key, handle)
                raw_t.delete(key.encode())

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
                        continue  # missing on the target: the data check
                        # compares every sampled key's type and value, which
                        # is where that is reported. The count check is not
                        # what covers it - it compares dbsize totals, and one
                        # key missing plus one stray key leaves those equal.
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
