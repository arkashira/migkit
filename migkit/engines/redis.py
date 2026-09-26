import time

from .base import Engine, RepairAction, Result


class RedisEngine(Engine):
    checks = ("schema", "counts", "data")
    ENGINE_FAMILY = "redis"

    def _client(self, side, db=0, decode=True):
        """`decode=False` for DUMP payloads, which are binary: decoding one
        as text is how a repair would corrupt what it copied."""
        ep = self.hop.source if side == "src" else self.hop.target
        try:
            import redis
        except ImportError:
            raise SystemExit("pip install 'migkit[redis]' for redis support")
        # a key or a value need not be text: decoded so that every byte
        # comes back as it was sent, where strict decoding stopped the
        # check on the first key that was not UTF-8
        return redis.Redis(host=ep.host, port=ep.port,
                           password=ep.password or None, db=int(db),
                           socket_timeout=15, decode_responses=decode,
                           encoding_errors="surrogateescape")

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

    def _kept(self, db, keys):
        """The keys migkit verifies and repairs: all but the ones the hop
        excludes, matched as `db.key` and as `key`. This engine used to
        ignore `exclude`, so a target-owned key pattern was compared, and
        repaired away, like any other."""
        if not self.hop.exclude:
            return keys
        return [k for k in keys if not self.hop.excluded(str(db), k)]

    def _count(self, side, db):
        client = self._client(side, db)
        if not self.hop.exclude:
            return client.dbsize()
        # the server's own count includes the excluded keys, so the kept
        # ones are counted by walking them
        return sum(len(b) for b in self._scan_batches(client, 0, True, db))

    def list_move_tables(self, db):
        """A keyspace is the unit: it has no tables inside it."""
        return [("", str(db))]

    def move_key(self, db, sch, tbl):
        return f"db{db}"

    #: keys per read of the keyspace copier
    COPY_BATCH = 1000

    def _raw_kept(self, db, keys):
        """`_kept` over keys as the server holds them, which need not be
        text: matched through a decoding that cannot fail, returned as
        they were."""
        if not self.hop.exclude:
            return keys
        text = {k.decode("utf-8", "surrogateescape"): k for k in keys}
        return [text[k] for k in self._kept(db, list(text))]

    def move_table(self, db, sch, tbl, chunk, ck, log):
        """The keyspace, key for key as stored - each value in the
        server's own serialised form with what is left of its time to
        live - through the same read-write the repair uses.

        Resumed from the scan's cursor in the checkpoint. A key written
        while the scan runs may or may not be carried, as the server
        documents for any scan: `check`, and the tail where there is one,
        are what settle it. A fresh start first removes what the target
        holds that the hop does not leave out: it is not this copy's."""
        from ..wording import progress
        key = self.move_key(db, sch, tbl)
        st = ck.setdefault(key, {})
        if st.get("done"):
            # a keyspace has no digest a server answers for it, and the
            # source may have changed since: copied again rather than
            # skipped on the word of an earlier run
            st.clear()
            ck.save()
            log(f"{key}: done earlier; a keyspace cannot be asked whether"
                " it still matches, so it is copied again")
        src = self._client("src", db, decode=False)
        dst = self._client("dst", db, decode=False)
        if "cursor" not in st:
            gone, cursor = 0, 0
            while True:
                cursor, keys = dst.scan(cursor, count=self.COPY_BATCH)
                keys = self._raw_kept(db, keys)
                if keys:
                    gone += dst.unlink(*keys)
                if cursor == 0:
                    break
            st.update(cursor=0, moved=0)
            ck.save()
            if gone:
                log(f"{key}: emptied {gone:,} keys the target held before"
                    " the copy")
        cursor = int(st["cursor"])
        moved = from_keys = int(st.get("moved", 0))
        # the server's count includes what the hop leaves out; walking the
        # keyspace once more for a total would double the reading
        total = None if self.hop.exclude else src.dbsize()
        began = time.monotonic()
        batch = max(1, min(int(chunk), self.COPY_BATCH))
        while True:
            cursor, keys = src.scan(cursor, count=batch)
            keys = self._raw_kept(db, keys)
            if keys:
                read = src.pipeline(transaction=False)
                for k in keys:
                    read.dump(k)
                    read.pttl(k)
                got = read.execute()
                write = dst.pipeline(transaction=False)
                for k, payload, ttl in zip(keys, got[0::2], got[1::2]):
                    if payload is None or ttl == -2:
                        continue    # gone since the scan found it
                    write.restore(k, ttl if ttl and ttl > 0 else 0, payload,
                                  replace=True)
                    moved += 1
                try:
                    write.execute()
                except Exception as e:
                    raise SystemExit(
                        f"{key}: the target refused a key it was handed:"
                        f" {str(e)[:90]}. A payload version error means it"
                        " runs an older Redis than the source, whose values"
                        " do not load into an older version") from None
            st.update(cursor=cursor, moved=moved)
            ck.save()
            if keys:
                log(progress(key, moved, total, began, time.monotonic(),
                             unit="keys", since=from_keys))
            if cursor == 0:
                break
        st["done"] = True
        ck.save()

    def moved_nothing(self, db):
        """The keyspace holds keys on the source and none on the target,
        counting only what the hop does not leave out."""
        try:
            def any_kept(side):
                client = self._client(side, db, decode=False)
                cursor = 0
                while True:
                    cursor, keys = client.scan(cursor, count=self.COPY_BATCH)
                    if self._raw_kept(db, keys):
                        return True
                    if cursor == 0:
                        return False
            return [f"db{db}"] if any_kept("src") and not any_kept("dst") \
                else []
        except Exception:  # noqa: BLE001 - None: cannot be asked
            return None

    def snapshot_state(self, db, state_dir, kind="all"):
        """What the target held before a repair: how many keys of each
        kind, and the modules loaded. The keys a repair replaces are kept
        whole beside it, in the repair's own undo file."""
        client = self._client("dst", db, decode=False)
        kinds, cursor = {}, 0
        while True:
            cursor, keys = client.scan(cursor, count=self.COPY_BATCH)
            read = client.pipeline(transaction=False)
            for k in self._raw_kept(db, keys):
                read.type(k)
            for t in read.execute():
                t = t.decode()
                kinds[t] = kinds.get(t, 0) + 1
            if cursor == 0:
                break
        (state_dir / "dst-shape.txt").write_text(repr(sorted(kinds.items())))

    def check_schema(self, db):
        """What a keyspace has in place of a schema: the modules loaded,
        and the kinds of value it holds.

        A module brings its own value types - a JSON document, a search
        index, a time series - and a target without the module cannot hold
        them at all. And a kind of value present on the source and absent
        on the target (streams, say) is the shape of a copy that dropped
        what it did not understand. Kinds are read from a sample of keys,
        the hop's `sample` option, the whole keyspace when `deep` is set.
        """
        res = []

        def modules(side):
            got = self._client(side, 0).module_list() or []
            return {str(m.get("name", m.get(b"name", ""))):
                    str(m.get("ver", m.get(b"ver", ""))) for m in got}

        try:
            ms, md = modules("src"), modules("dst")
        except Exception as e:
            return [Result("schema", f"db{db}", "error",
                           f"the modules could not be read: "
                           f"{str(e).splitlines()[-1][:80]} - the kinds of"
                           " value each side can hold were not compared")]
        lacking = sorted(m for m in ms if m not in md)
        older = sorted(f"{m} {ms[m]} -> {md[m]}" for m in ms
                       if m in md and md[m] < ms[m])
        if lacking or older:
            res.append(Result(
                "schema", f"db{db} modules", "diff",
                "; ".join(([f"not on the target: {', '.join(lacking)}"]
                           if lacking else [])
                          + ([f"older on the target: {', '.join(older)}"]
                             if older else [])), "",
                "load the same modules on the target before copying: a"
                " value of a module's type cannot land without it"))
        else:
            res.append(Result("schema", f"db{db} modules", "ok",
                              f"{len(ms)} modules, the same on both sides"))

        sample = int(self.hop.options.get("sample", 5000))
        deep = bool(self.hop.options.get("deep", False))

        def kinds(side):
            client = self._client(side, db)
            seen = {}
            for keys in self._scan_batches(client, sample, deep, db):
                pipe = client.pipeline(transaction=False)
                for k in keys:
                    pipe.type(k)
                for t in pipe.execute():
                    seen[t] = seen.get(t, 0) + 1
            return seen

        try:
            ks, kd = kinds("src"), kinds("dst")
        except Exception as e:
            res.append(Result("schema", f"db{db} kinds", "error",
                              f"the keyspace could not be sampled:"
                              f" {str(e).splitlines()[-1][:80]}"))
            return res
        gone = sorted(k for k in ks if k not in kd)
        if gone:
            res.append(Result(
                "schema", f"db{db} kinds", "diff",
                f"kinds of value on the source and none on the target:"
                f" {', '.join(f'{k} ({ks[k]:,})' for k in gone)}", "",
                "a copy that left out values it did not handle: move those"
                " keys again with a path that carries their kind"))
        else:
            res.append(Result("schema", f"db{db} kinds", "ok",
                              "every kind of value on the source is on the"
                              f" target: {', '.join(sorted(ks)) or 'none'}"))
        return res

    #: settings that change what the data means or whether it survives:
    #: an eviction policy decides which keys vanish under memory pressure,
    #: and a different number of databases leaves some with nowhere to go
    CRITICAL_CONFIG = ("maxmemory-policy", "databases", "notify-keyspace-events",
                       "lua-time-limit", "busy-reply-threshold")

    def check_params(self, db):
        """The server's own settings, both sides, through the report every
        engine's settings go through. A target that evicts differently
        loses different keys under the same load."""
        def pull(side):
            try:
                got = self._client(side, 0).config_get("*")
                # the file this lands in is evidence people attach to
                # tickets: nothing that is, or authenticates like, a secret
                return {str(k): str(v) for k, v in got.items()
                        if not any(w in str(k).lower()
                                   for w in ("pass", "auth", "secret",
                                             "key"))}
            except Exception as e:
                return {self.UNREADABLE: str(e).splitlines()[-1][:80]
                        if str(e) else type(e).__name__}
        return self._param_result(
            db, pull("src"), pull("dst"), self.CRITICAL_CONFIG,
            "align the target's settings - eviction first - before cutover")

    def check_counts(self, db):
        a = self._count("src", db)
        b = self._count("dst", db)
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

    def _scan_batches(self, client, sample, deep, db=0):
        """Batches of keys from one side, up to the sample cap."""
        cursor = 0
        seen = 0
        while True:
            cursor, keys = client.scan(cursor, count=1000)
            keys = self._kept(db, keys)
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
        # A SCAN plus a pipeline of reads is the heaviest thing a verifier
        # does to a single-threaded server, and nothing else was slowing it
        # down: this loop ran at whatever speed the network allowed, against
        # a Redis that might also be serving an application. Each side is
        # gated on its own health, since each pass leans on a different one.
        src_gate = Throttle(1, probe=lambda: self._health("src"))
        dst_gate = Throttle(1, probe=lambda: self._health("dst"))
        missing, changed = [], []
        for keys in self._scan_batches(s, sample, deep, db):
            with src_gate.unit():
                gone, differ = self._batch_compare(s, t, keys)
            missing.extend(gone)
            changed.extend(differ)
            checked += len(keys)
            if stream and checked % 20000 < 1000:
                stream(f"db{db}: {checked} keys compared")
        extra = []
        seen_dst = 0
        for keys in self._scan_batches(t, sample, deep, db):
            with dst_gate.unit():
                pipe = s.pipeline(transaction=False)
                for k in keys:
                    pipe.exists(k)
                extra.extend(k for k, there in zip(keys, pipe.execute())
                             if not there)
            seen_dst += len(keys)
        self._write_drilldown(db, missing=missing, changed=changed,
                              extra=extra)
        proof = ""
        suspects = missing + changed + extra
        if suspects and len(suspects) <= self.DRILL_CAP:
            # confirm before calling it different: where the target is a
            # replica of this source, a change it has not applied yet is
            # still arriving, not wrong
            import json
            got = self.fenced_recheck(db, f"db{db}",
                                      {json.dumps(k) for k in suspects})
            if got is not None:
                missing, extra, changed = ([json.loads(k) for k in ks]
                                           for ks in got[:3])
                proof = "; ".join(got[3])
                if stream:
                    stream(f"db{db}: fence {proof}")
        bad = len(missing) + len(changed)
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
                              f" {', '.join(parts)}"
                              + (f"; still so once the replica had caught"
                                 f" up ({proof})" if proof else ""), "",
                              f"migkit sync {self.hop.name} --db {db}"
                              " --kind rows --apply"))
        if extra:
            shown = ", ".join(sorted(extra)[:6])
            more = f" (+{len(extra) - 6} more)" if len(extra) > 6 else ""
            res.append(Result(
                "data", f"db{db} extra keys", "diff",
                f"{len(extra)} of {seen_dst} keys on the target are not on"
                f" the source: {shown}{more}"
                + (f"; still so once the replica had caught up ({proof})"
                   if proof else ""), "",
                "a target still holding keys from an earlier attempt:"
                f" migkit sync {self.hop.name} --db {db} --kind rows --apply"
                " removes them, with the old values written to the undo file"
                " first; the key count can match while this is true"))
        return res or [Result(
            "data", f"db{db}", "ok",
            f"{checked} keys value-equal, {seen_dst} target keys all present"
            f" on the source ({mode}, pipelined)"
            + (f"; the difference was still arriving ({proof})"
               if proof else ""))]

    def _write_drilldown(self, db, **kinds):
        """The keys behind the counts, through the shared writer.

        Written as JSON, which is what the rest of the estate's drilldowns
        hold and what a Redis key needs: keys are binary-safe, so one can
        contain the newline these files are separated by. Measured on Redis
        7 with a key holding a newline, the raw form was written as one line
        and read back as two, and the repair left the target differing while
        reporting that it had put the keys right.
        """
        import json
        self._write_drill(db, f"db{db}",
                          **{kind: [json.dumps(k) for k in sorted(keys)]
                             for kind, keys in kinds.items()})

    def _read_drilldown(self, db, kind):
        import json
        return [json.loads(line) for line in
                self._read_drill(db, f"db{db}", kind)]

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
                        " than the source: its payloads do not load into an"
                        " older version, so these keys need re-writing value"
                        " by value, which migkit does not do yet (backlog"
                        " item 0e)")
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
            keys = self._kept(db, keys)
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

    def watch_sample(self, db):
        return {"db": f"db{db}", "ts": time.time(),
                "src_rows": self._client("src", db).dbsize(),
                "dst_rows": self._client("dst", db).dbsize()}

    # --- the server's own replication (REPLICAOF) ---------------------------

    #: one replica per server: it carries every database the server holds
    REPLICATES_THE_SERVER = True
    #: a replica begins with a copy of the source's whole dataset, so
    #: following and copying-then-following are the same thing
    REPLICA_COPIES = True
    #: the account the replica signs in to the source as
    REPL_USER = "migkit_repl"
    #: seconds the status waits for the replica's link to come up
    REPLICA_SETTLE = 10

    def _keyspace(self, side):
        """The databases holding keys on a side, by number."""
        return {k[2:] for k in self._client(side).info("keyspace")}

    def native_replica_unsafe(self):
        """Why REPLICAOF would carry this hop wrongly, or None.

        A replica's first sync empties the target, every database of it,
        before it loads the source's snapshot - measured on 7.4: a key the
        target had of its own and a key in a database the source did not
        use were both gone once the link came up. And it carries every
        database and every key the source has, with no filter to leave any
        out."""
        if self.hop.exclude:
            return ("the hop excludes keys, and a Redis replica carries"
                    " every key of the source: its first sync empties the"
                    " target, the excluded keys with it")
        for side in ("src", "dst"):
            if self._client(side).info("cluster").get("cluster_enabled"):
                return (f"the {'source' if side == 'src' else 'target'} is"
                        " a Redis Cluster, which takes no REPLICAOF: its"
                        " shards replicate within the cluster")
        covered = set(self.databases())
        carried = sorted(self._keyspace("src") - covered, key=int)
        if carried:
            return (f"the source has keys in db{', db'.join(carried)}, which"
                    " the hop does not name, and a replica carries every"
                    " database of the source")
        erased = sorted(self._keyspace("dst") - covered, key=int)
        if erased:
            return (f"the target has keys in db{', db'.join(erased)}, which"
                    " the hop does not name, and a replica's first sync"
                    " empties every database of the target")
        return self._older_target()

    def _older_target(self):
        """Why the target cannot load the source's snapshot, or None.

        A replica loads the source's snapshot, and a server does not read
        the format of a newer one - measured, a 7.4 source and a 6.2 target:
        `Can't handle RDB format version 12`, the link down for good, and
        the target already emptied. The same software is compared by its
        own version; different software by the Redis version each says it
        is compatible with, which is what decides the format it reads."""
        s, d = self._brands()
        if s.name and s.name == d.name and s.version and d.version:
            have, need = d.version, s.version
        else:
            def claim(side):
                try:
                    return self._client(side).info("server").get(
                        "redis_version")
                except Exception:
                    return None
            have, need = claim("dst"), claim("src")
        if not (have and need):
            return None

        def parts(v):
            import re
            return tuple(int(x) for x in re.findall(r"\d+", str(v))[:2])
        if parts(have) < parts(need):
            return (f"the target runs {have} and the source {need}: a"
                    " replica loads the source's snapshot, which an older"
                    " server does not read, and its first sync has emptied"
                    " the target by then")
        return None

    def _acl(self, side):
        """Whether the side keeps accounts of its own (Redis 6 and later)."""
        try:
            self._client(side).execute_command("ACL", "WHOAMI")
            return True
        except Exception:
            return False

    def replicate_sql(self, db, copy_data=True, secret=None, copied=None):
        """The commands that make the target a replica of the source.

        Where the source asks for a password, the replica signs in as an
        account of its own that may do nothing but replicate, with a
        password drawn for the run - the source's own password is never
        given to the target. `secret` is None for a plan only shown, which
        carries CHANGE_ME for the person who runs it."""
        import shlex
        secret = secret or "CHANGE_ME"
        s = self.hop.source
        src_cmds, dst_cmds, drop_src = [], [], []
        notes = []
        if s.password:
            if self._acl("src"):
                src_cmds = [f"ACL SETUSER {self.REPL_USER} reset on"
                            f" >{secret} +psync +replconf +ping"]
                dst_cmds = [f"CONFIG SET masteruser {self.REPL_USER}",
                            f"CONFIG SET masterauth {secret}"]
                drop_src = [f"ACL DELUSER {self.REPL_USER}"]
            else:
                dst_cmds = ["CONFIG SET masterauth"
                            f" {shlex.quote(s.password)}"]
                notes.append("the source keeps no accounts, so the target"
                             " is given the source's own password")
        dst_cmds.append(f"REPLICAOF {s.host} {s.port}")
        held = {d: self._client("dst", d).dbsize()
                for d in sorted(self._keyspace("dst"), key=int)}
        if any(held.values()):
            notes.append("the first sync replaces what the target holds - "
                         + ", ".join(f"{n:,} key{'s' * (n != 1)} in db{d}"
                                     for d, n in held.items() if n)
                         + " - with the source's")
        if not copy_data or copied:
            notes.append("a replica always begins with a copy of the"
                         " source's whole dataset, whatever was copied"
                         " before")
        notes.append("the target is read-only while it replicates;"
                     " REPLICAOF NO ONE at cutover makes it writable and"
                     " keeps every key")
        return {"src": src_cmds, "dst": dst_cmds,
                "drop_dst": ["REPLICAOF NO ONE", "CONFIG SET masteruser ''",
                             "CONFIG SET masterauth ''"],
                "drop_src": drop_src,
                "status": "INFO replication",
                "note": "; ".join(notes)}

    def apply_replication_stmt(self, side, db, stmt):
        """One command of the plan, as the server's own words.

        REPLICAOF answers OK whether or not the target can reach the
        source; `replication_status` is what says whether it did."""
        import shlex

        import redis
        from ..util import without_secret
        args = shlex.split(stmt)
        try:
            return self._client(side).execute_command(*args)
        except redis.RedisError as e:
            # a password is the value after `>` or after masterauth
            hidden = [w[1:] for w in args if w.startswith(">")]
            if [a.lower() for a in args[:3]] == ["config", "set",
                                                 "masterauth"]:
                hidden += args[3:]
            said = str(e)
            for word in hidden + [self.hop.source.password]:
                said = without_secret(said, word)
            where = "source" if side == "src" else "target"
            hint = (" - a managed service keeps replication to itself"
                    if "unknown command" in said.lower() else "")
            raise SystemExit(f"the {where} refused {' '.join(args[:2])}:"
                             f" {said[:160]}{hint}. What ran before it"
                             " stays as it is.")

    def _replica(self):
        return self._client("dst").info("replication")

    def replication_status(self, db, sql):
        """Whether the target is replicating, read from the target after
        the link has had a moment to come up.

        REPLICAOF says OK and connects behind it. A refused sign-in shows
        on the target only as a link that stays down - the reason goes to
        its log, which migkit cannot read - so the source's own record of
        refused sign-ins (ACL LOG) is asked what happened."""
        end = time.time() + self.REPLICA_SETTLE
        while True:
            r = self._replica()
            if (r.get("role") != "slave"
                    or r.get("master_link_status") == "up"
                    or r.get("master_sync_in_progress")
                    or time.time() > end):
                break
            time.sleep(0.5)
        if r.get("role") != "slave":
            return ("the target is not a replica: the commands ran but"
                    " nothing is replicating, NOT replicating")
        where = f"source {r.get('master_host')}:{r.get('master_port')}"
        if r.get("master_sync_in_progress"):
            total = int(r.get("master_sync_total_bytes") or -1)
            got = int(r.get("master_sync_read_bytes") or 0)
            return (f"{where}, copying the source's dataset ({got:,}"
                    + (f" of {total:,}" if total > 0 else "")
                    + " bytes so far)")
        if r.get("master_link_status") == "up":
            at = int(self._client("src").info("replication")
                     .get("master_repl_offset") or 0)
            behind = max(0, at - int(r.get("master_repl_offset") or 0))
            return f"{where}, link up, {behind:,} bytes behind"
        return f"{where}, link down, NOT replicating: {self._why_down()}"

    def _why_down(self):
        """What the source says about the replica's sign-in."""
        try:
            log = self._client("src").execute_command("ACL", "LOG", "20")
        except Exception:
            log = []
        for entry in log or []:
            e = entry if isinstance(entry, dict) else dict(
                zip(entry[::2], entry[1::2]))
            if (e.get("reason") == "auth"
                    and e.get("username") == self.REPL_USER
                    and float(e.get("age-seconds") or 1e9)
                    < self.REPLICA_SETTLE + 30):
                return "the source refused the replica's sign-in"
        s = self.hop.source
        return (f"the target has not reached the source at {s.host}:"
                f"{s.port} - an address the target cannot route to, or a"
                " source it cannot sign in to")

    def src_lsn(self, db):
        """Where the source's replication stream is now: its replication
        id and offset."""
        r = self._client("src").info("replication")
        return f"{r.get('master_replid')}:{int(r.get('master_repl_offset') or 0)}"

    def fence_wait(self, db, at, timeout=300):
        """Wait until the target's replica has applied the source's stream
        up to `at`. None where the target is not a replica of this source,
        so the caller does not wait on something that cannot arrive."""
        if not at:
            return None
        replid, _, offset = str(at).rpartition(":")
        end = time.time() + timeout
        while time.time() < end:
            r = self._replica()
            if r.get("role") != "slave":
                return None
            if r.get("master_link_status") == "up":
                if replid not in (r.get("master_replid"),
                                  r.get("master_replid2")):
                    return None
                if int(r.get("master_repl_offset") or 0) >= int(offset):
                    return True
            time.sleep(0.2)
        return False

    def stream_writers(self, db):
        """The target's replica, when it is one: it cannot be paused for a
        repair, and a replica takes no writes but its own."""
        out = super().stream_writers(db)
        r = self._replica()
        if r.get("role") == "slave":
            out.append((f"the replica of {r.get('master_host')}:"
                        f"{r.get('master_port')}", False))
        return out

    # --- the confirm pass (base `fenced_recheck`) ---------------------------

    def _compare_pks(self, db, table, keys):
        """(missing, extra, changed) among these keys, as the drilldown
        writes them - read again from both sides, now."""
        import json
        names = [json.loads(k) for k in keys]
        s, t = self._client("src", db), self._client("dst", db)
        ps, pt = s.pipeline(transaction=False), t.pipeline(transaction=False)
        for k in names:
            ps.type(k)
            pt.type(k)
        missing, extra, both = [], [], []
        for k, a, b in zip(names, ps.execute(), pt.execute()):
            if a == "none" and b == "none":
                continue
            if b == "none":
                missing.append(k)
            elif a == "none":
                extra.append(k)
            else:
                both.append(k)
        gone, changed = self._batch_compare(s, t, both) if both else ([], [])

        def enc(ks):
            return [json.dumps(k) for k in ks]
        return enc(missing + gone), enc(extra), enc(changed)

    def _write_pk_files(self, db, table, missing, extra, changed):
        self._write_drill(db, table, missing=missing, extra=extra,
                          changed=changed)
