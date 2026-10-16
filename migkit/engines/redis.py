import os
import struct
import time

from .. import pieces
from ..config import scoped
from ..decide import Need, Rung
from .base import (LEFT_OUT, Engine, RepairAction, Result, clauses,
                   rows_counted)


class RedisEngine(Engine):
    checks = ("schema", "counts", "data")
    ENGINE_FAMILY = "redis"

    def _client(self, side, db=0, decode=True, no_touch=False):
        """`decode=False` for DUMP payloads, which are binary: decoding one
        as text is how a repair would corrupt what it copied. `no_touch`:
        every connection reads without touching keys (`_no_touch`)."""
        ep = self.hop.source if side == "src" else self.hop.target
        try:
            import redis
        except ImportError:
            raise SystemExit("pip install 'migkit[redis]' for redis support")
        # a key or a value need not be text: decoded so that every byte
        # comes back as it was sent, where strict decoding stopped the
        # check on the first key that was not UTF-8
        pool = redis.ConnectionPool(
            connection_class=self._connection_class(side, decode, no_touch),
            host=ep.host, port=ep.port, password=ep.password or None,
            db=int(db), socket_timeout=15, decode_responses=decode,
            encoding_errors="surrogateescape")
        return redis.Redis(connection_pool=pool)

    def _connection_class(self, side, decode, no_touch=False):
        """What every connection to a side is made of, whichever client
        holds it: the side's TLS (`Endpoint.redis_tls`), and a key sent
        back byte for byte - by the client's own packer where hiredis is
        installed, which packs strictly, and a key that was not UTF-8
        stopped the copy with `UnicodeEncodeError` (measured)."""
        import redis
        from redis.connection import Encoder, PythonRespSerializer
        ep = self.hop.source if side == "src" else self.hop.target
        tls = {k: v for k, v in ep.redis_tls().items() if k != "ssl"}
        pack = Encoder("utf-8", "surrogateescape", decode).encode

        class Connection(redis.SSLConnection if tls else redis.Connection):
            def __init__(self, **kw):
                kw.update(tls)
                kw["command_packer"] = PythonRespSerializer(6000, pack)
                super().__init__(**kw)

            def on_connect(self):
                super().on_connect()
                if no_touch:
                    # the source's access history left as its clients made it
                    self.send_command("CLIENT", "NO-TOUCH", "ON")
                    self.read_response()
        return Connection

    def _cluster(self, side):
        """Whether a side is a Redis Cluster, asked once."""
        seen = self.__dict__.setdefault("_is_cluster", {})
        if side not in seen:
            try:
                seen[side] = bool(int(self._client(side).info("cluster")
                                      .get("cluster_enabled") or 0))
            except Exception:  # noqa: BLE001 - a server that will not say
                seen[side] = False
        return seen[side]

    def _keys(self, side, db=0, decode=True, no_touch=False):
        """The client a key is read and written through. On a cluster, one
        that sends each key to the node serving its slot, and a pipeline
        to every node its keys are on: the side's own server answers only
        for its own slots (`MOVED` for the rest). Elsewhere, the side's
        own client."""
        if not self._cluster(side):
            return self._client(side, db, decode, no_touch)
        made = self.__dict__.setdefault("_clusters", {})
        if (side, decode, no_touch) not in made:
            from redis.cluster import RedisCluster
            ep = self.hop.source if side == "src" else self.hop.target
            made[(side, decode, no_touch)] = RedisCluster.from_url(
                f"redis://{ep.host}:{int(ep.port)}",
                password=ep.password or None, socket_timeout=15,
                decode_responses=decode, encoding_errors="surrogateescape",
                connection_class=self._connection_class(side, decode,
                                                        no_touch))
        return made[(side, decode, no_touch)]

    def _nodes(self, side, db=0, decode=True):
        """[(name, client)] of the servers that hold a side's keys: every
        master of a cluster, in the order of their names, so a walk that
        stopped goes on where it stopped; the side's own server
        elsewhere. A SCAN of one node of a cluster sees the keys of its
        own slots and no others."""
        if not self._cluster(side):
            return [("", self._client(side, db, decode))]
        return sorted(((n.name, n.redis_connection)
                       for n in self._keys(side, 0, decode).get_primaries()),
                      key=lambda n: n[0])

    def _dbsize(self, side, db):
        """How many keys a side holds: every master's, on a cluster. The
        cluster client sends `DBSIZE` to one node (measured: 201 of a
        three-master cluster's 603 keys)."""
        return sum(int(c.dbsize()) for _, c in self._nodes(side, db))

    def _walk(self, side, db, decode=True, count=1000, start=None):
        """The keys of a side, a page at a time: (keys, then), `then` where
        the walk goes on from after this page - {"node", "cursor"} - or
        None once every node is done. `start`, a `then` of an earlier walk,
        goes on from there."""
        nodes = self._nodes(side, db, decode)
        names = [n for n, _ in nodes]
        at, cursor = 0, 0
        if start and start.get("node", "") in names:
            at, cursor = names.index(start.get("node", "")), \
                int(start.get("cursor") or 0)
        for j in range(at, len(nodes)):
            name, client = nodes[j]
            while True:
                cursor, keys = client.scan(cursor, count=count)
                then = ({"node": name, "cursor": cursor} if cursor else
                        {"node": names[j + 1], "cursor": 0}
                        if j + 1 < len(nodes) else None)
                yield keys, then
                if not cursor:
                    break

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

        why = self._snapshot_valkey_refuses(*self._brands())
        add("fail" if why else "pass", "target reads the source's snapshot",
            why or "no snapshot format the target refuses is known")
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
        inv = self._handwork()
        items += inv.rows() + inv.summary()
        return items

    def _handwork(self):
        """What a key-by-key copy leaves behind, counted and named
        (`migkit.handwork`): keys in databases the hop does not name, the
        accounts (`migkit users` carries them) and the functions, which
        are code stored in the server and not keys at all. A probe that
        could not run is unknown, never none."""
        from .. import handwork, users
        inv = handwork.Inventory()
        scope = "instance"
        src = self._client("src")
        try:
            named = {str(d) for d in self.databases()}
            held = src.info("keyspace")
            inv.add("not-carried", scope,
                    "keys in databases the hop does not name",
                    [f"{d} ({int(v.get('keys') or 0):,} keys)"
                     for d, v in sorted(held.items())
                     if d[2:] not in named])
        except Exception as e:  # noqa: BLE001 - said, not counted as none
            inv.unknown("not-carried", scope,
                        f"cannot list the source's databases:"
                        f" {str(e)[:80]}")
        try:
            inv.add("not-carried", scope,
                    "accounts, which `migkit users` carries",
                    sorted(users._redis_users(self.hop.source)))
        except Exception as e:  # noqa: BLE001
            inv.unknown("not-carried", scope,
                        f"cannot read the source's accounts: {str(e)[:80]}")
        try:
            # a map of fields, or - on the older protocol - a flat list of
            # name, value, name, value
            libraries = []
            for lib in src.function_list():
                lib = lib if isinstance(lib, dict) else dict(
                    zip(lib[::2], lib[1::2]))
                libraries.append(str(lib.get("library_name",
                                             lib.get(b"library_name"))))
            inv.add("server-side-code", scope,
                    "function libraries, stored in the server and not keys",
                    libraries)
        except Exception as e:  # noqa: BLE001
            if "unknown command" in str(e).lower():
                # before Redis 7 there are no functions to leave behind
                inv.add("server-side-code", scope,
                        "function libraries, stored in the server and not"
                        " keys", [])
            else:
                inv.unknown("server-side-code", scope,
                            f"cannot list the source's functions:"
                            f" {str(e)[:80]}")
        return inv

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

    def capacity(self, side, db):
        """What the server says of its room for a move's connections, from
        the one INFO the throttle reads (`_health_from`): the clients it
        will still take, and whether it is a replica. Commands run one at
        a time, so there is no count of sessions busy or CPUs to give."""
        info = self._client(side).info()
        out = {}
        ceiling = int(info.get("maxclients") or 0)
        if ceiling:
            out["free_connections"] = max(
                0, ceiling - int(info.get("connected_clients") or 0))
        if info.get("role"):
            out["replica"] = info.get("role") == "slave"
        return out

    def free_bytes(self, side, db):
        """What a side has free to hold keys. A Redis keeps its dataset in
        memory, so its room is the memory under `maxmemory` it has not
        used; None where it sets no limit - the machine's memory is shared
        with whatever else runs on it, and a guess would be worse than the
        plan saying nobody knows."""
        info = self._client(side).info()
        limit = int(info.get("maxmemory") or 0)
        if not limit:
            return None
        return max(limit - int(info.get("used_memory") or 0), 0)

    #: what `assess` asks before anything moves (`Engine.PREFLIGHT`)
    PREFLIGHT = ("_database_on_target", "_keys_the_copy_removes")

    def _database_on_target(self, db):
        """Whether the target has the database the keys are going to: a
        server holds `databases` of them (16 by default) and refuses a
        number past that, and a Redis Cluster has database 0 only - the
        copy would stop on its first key."""
        scope = f"db{db} target database"
        try:
            n = int(self.side_database("dst", db))
            if self._cluster("dst"):
                if n:
                    return Result("deep", scope, "diff",
                                  f"the target is a Redis Cluster, which has"
                                  f" database 0 only: db{db} cannot be"
                                  " selected there", "",
                                  "move this database's keys to database 0"
                                  " of the cluster")
                return Result("deep", scope, "ok",
                              "the target is a Redis Cluster; database 0 is"
                              " the one it has")
            have = int(self._client("dst").config_get("databases")
                       .get("databases") or 0)
        except Exception as e:  # noqa: BLE001 - said, never assumed
            return Result("deep", scope, "error",
                          "the target's databases could not be read: "
                          + (str(e).splitlines() or [type(e).__name__])[-1]
                          [:80])
        if have and n >= have:
            return Result("deep", scope, "diff",
                          f"the target has {have} databases (0 to"
                          f" {have - 1}) and the keys are for db{db}", "",
                          f"set `databases` to more than {n} on the target"
                          " before the copy")
        return Result("deep", scope, "ok",
                      f"db{db} is one of the target's {have or '?'}"
                      " databases")

    def _keys_the_copy_removes(self, db):
        """The keys the target holds in this database that the hop takes:
        a copy begins afresh by removing them (`empty_target`), and a
        target filled by mistake is better known before than after."""
        scope = f"db{db} held on target"
        held = self.rows_held(db, [f"db{db}"])
        if held is None:
            return Result("deep", scope, "error",
                          "the target's keys could not be read, so whether"
                          " the copy removes any is not known")
        if held:
            return Result(
                "deep", scope, "warn",
                f"the target already holds keys in db{db} that the hop"
                " takes; the copy removes them before it writes the"
                " source's", "",
                "empty the database yourself if they matter, or leave them"
                " out of the hop with `exclude`")
        return Result("deep", scope, "ok",
                      f"the target holds no key in db{db} that a copy would"
                      " remove")

    #: what a hop may ask of a table's rows and columns that a keyspace
    #: has no meaning for: a key is taken or left by its name (`include`,
    #: `exclude`) and carried as it is
    UNREAD_BY_KEYS = ("row_filters", "column_rules", "values_masked",
                      "renamed", "names_ruled")

    def _refuse_unread_mapping(self):
        """Stop a copy or a check of a hop that asks for a row filter, a
        column rule, a masked value, a renamed table or a rule for names:
        this engine reads none of them, and the keys would be carried and
        compared whole, under their own names, as if the hop had not said
        so."""
        from ..config import narrowed_by
        said = [k.replace("_", " ") for k, v in narrowed_by(self.hop).items()
                if v and k in self.UNREAD_BY_KEYS]
        if said:
            raise SystemExit(
                f"the hop asks for {', '.join(said)}, which this engine does"
                " not read: its keys are taken or left by name (`include`,"
                " `exclude`) and carried as they are. Nothing was read or"
                " written; take the rule out of the hop")

    def _kept(self, db, keys):
        """The keys migkit verifies and repairs: all but the ones the hop
        excludes, matched as `db.key` and as `key`. This engine used to
        ignore `exclude`, so a target-owned key pattern was compared, and
        repaired away, like any other."""
        if not scoped(self.hop):
            return keys
        return [k for k in keys if self.in_scope(db, k)]

    def _count(self, side, db):
        if not scoped(self.hop):
            return self._dbsize(side, db)
        # the server's own count includes the excluded keys, so the kept
        # ones are counted by walking them
        return sum(len(b) for b in self._scan_batches(side, 0, True, db))

    def list_move_tables(self, db):
        """A keyspace is the unit: it has no tables inside it. One the hop
        leaves out whole is none; the keys it leaves out of the others are
        left at the copy (`_raw_kept`)."""
        return [("", str(db))] if self.in_scope(db, "") else []

    def move_key(self, db, sch, tbl):
        return f"db{db}"

    #: keys per read of the keyspace copier, and the bytes a batch of their
    #: serialised values holds at most (`_carry`)
    COPY_BATCH = 1000
    COPY_BYTES = pieces.BATCH_BYTES
    #: large values - a string is up to 512 MB, a collection any size: a key
    #: past a piece is read and written a piece at a time by its type's own
    #: commands (`_carry_in_pieces`), compared a piece at a time
    #: (`_compare_in_pieces`), and sized by the memory it takes on the
    #: server (`row_sizes`); what one worker holds is a piece of it
    LARGE_VALUES = pieces.Way(
        "sized by the server, keys past a piece read, written and compared"
        " a piece at a time")
    CARRIES_IN_PIECES = pieces.PIECE

    def row_sizes(self, side, db, tables=None):
        """{keyspace: sizes} (`Engine.row_sizes`): a key is a keyspace's row,
        sized by the memory it takes on the server (`MEMORY USAGE`, which
        samples a collection's elements) over the first `SIZE_SAMPLE` keys
        shared out among the nodes - only numbers cross."""
        from .base import sizes_of
        lengths, whole = [], True
        client = self._keys(side, db, decode=False)
        for keys in self._scan_batches(side, self.SIZE_SAMPLE, False, db):
            raw = [k.encode("utf-8", "surrogateescape")
                   if isinstance(k, str) else k for k in keys]
            got = self._memory_of(client, raw)
            lengths += [got.get(k, 0) + 1 for k in raw]
        if len(lengths) >= self.SIZE_SAMPLE:
            whole = False
        name = str(db)
        if tables is not None and name not in {str(t) for t in tables}:
            return {}
        return {name: sizes_of(lengths, exact=whole)} if lengths else {}

    def _raw_kept(self, db, keys):
        """`_kept` over keys as the server holds them, which need not be
        text: matched through a decoding that cannot fail, returned as
        they were."""
        if not scoped(self.hop):
            return keys
        text = {k.decode("utf-8", "surrogateescape"): k for k in keys}
        return [text[k] for k in self._kept(db, list(text))]

    def move_table(self, db, sch, tbl, chunk, ck, log):
        """The keyspace, key for key as stored - each value in the
        server's own serialised form, with its expiry and how recently or
        how often it was read (`_carry`) - through the same read-write the
        repair uses. Every master of a cluster, one after another.

        Resumed from the scan's node and cursor in the checkpoint. A key
        written while the scan runs may or may not be carried, as the
        server documents for any scan: `check`, and the tail where there is
        one, are what settle it. A fresh start first removes what the
        target holds that the hop does not leave out: it is not this
        copy's."""
        from ..wording import progress
        self._refuse_unread_mapping()
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
        names = [n for n, _ in self._nodes("src", db, False)]
        if "cursor" in st and st.get("node", "") not in names:
            # the cluster's masters are not the ones the copy stopped on: a
            # cursor is a position on one node, so it starts again
            st.clear()
            log(f"{key}: the source's nodes changed since the copy stopped,"
                " so it starts again")
        src = self._keys("src", db, decode=False)
        dst = self._keys("dst", db, decode=False)
        if "cursor" not in st:
            gone = self.empty_target(db)
            st.update(cursor=0, node=names[0] if names else "", moved=0)
            ck.save()
            if gone:
                log(f"{key}: emptied {gone:,} keys the target held before"
                    " the copy")
        moved = from_keys = int(st.get("moved", 0))
        # the server's count includes what the hop leaves out; walking the
        # keyspace once more for a total would double the reading
        total = None if scoped(self.hop) else self._dbsize("src", db)
        began = time.monotonic()
        batch = max(1, min(int(chunk), self.COPY_BATCH))
        how = self._carry_plan(db)
        for keys, then in self._walk("src", db, False, batch,
                                     start={"node": st.get("node", ""),
                                            "cursor": st["cursor"]}):
            keys = self._raw_kept(db, keys)
            if keys:
                moved += self._carry(src, dst, keys, how, key, log)
            if then is not None:
                st.update(cursor=then["cursor"], node=then["node"],
                          moved=moved)
                ck.save()
            if keys:
                log(progress(key, moved, total, began, time.monotonic(),
                             unit="keys", since=from_keys))
            if then is None:
                break
        st.update(cursor=0, moved=moved, done=True)
        ck.save()

    def empty_target(self, db):
        """Remove what the target holds of this database that the hop does
        not leave out - every copy's fresh start - and return how many.
        One key a command: on a cluster, keys of several slots in one
        UNLINK are refused (`CROSSSLOT`)."""
        dst = self._keys("dst", db, decode=False)
        gone = 0
        for keys, _ in self._walk("dst", db, False, self.COPY_BATCH):
            keys = self._raw_kept(db, keys)
            if keys:
                drop = dst.pipeline(transaction=False)
                for k in keys:
                    drop.unlink(k)
                gone += sum(drop.execute())
        return gone

    # ---- a key as the source holds it ------------------------------------

    #: the source's eviction policies that count reads (LFU): their keys
    #: carry how often they were read, the others how long since
    LFU = ("allkeys-lfu", "volatile-lfu")

    def _carry_plan(self, db):
        """How keys are read and written for this run, asked once: whether
        the source answers `PEXPIRETIME` (7.0 and later; before, the expiry
        is its clock plus `PTTL`), which access history its policy keeps,
        and whether the target takes `ABSTTL`, `IDLETIME` and `FREQ` on a
        `RESTORE` (5.0 and later)."""
        import re

        import redis
        src = self._client("src", db)
        # asked of the command table, not with a key: a key of the probe's
        # was served by another node of a cluster, which answered `MOVED`,
        # and read as a server without the command. A server without it
        # answers `[nil]`, which the client's reader fails on (TypeError)
        try:
            known = src.execute_command("COMMAND", "INFO", "PEXPIRETIME")
            expiry = "pexpiretime" if known else "pttl"
        except Exception:  # noqa: BLE001 - no such command, then
            expiry = "pttl"
        try:
            pol = str(src.config_get("maxmemory-policy").get(
                "maxmemory-policy", ""))
        except redis.ResponseError:
            # a managed server keeps CONFIG to itself, and says it in INFO
            pol = str(src.info("memory").get("maxmemory_policy", ""))
        try:
            claim = str(self._client("dst", db).info("server").get(
                "redis_version", ""))
        except redis.RedisError:
            claim = ""
        v = tuple(int(x) for x in re.findall(r"\d+", claim)[:2])
        return {"expiry": expiry,
                "history": "freq" if pol in self.LFU else "idletime",
                "abs": v >= (5, 0)}

    #: how a restored key's expiry is written (`_carry`): the moment it
    #: falls due, with how long since it was read or how often, on a
    #: target of 5.0 and later; the time left to that moment otherwise,
    #: and where a target that claims 5.0 refuses the first
    EXPIRY_WAYS = (
        Rung("moment", "the moment each key falls due, with its access"
                       " history",
             needs=(Need("a target that takes an absolute expiry",
                         lambda f: f["abs"],
                         "the target is older than 5.0, which takes no"
                         " absolute expiry on a restore"),)),
        Rung("left", "the time left to each key's expiry"))

    def _carry(self, src, dst, keys, how, label="", log=None):
        """Copy `keys` from `src` to `dst` as the source holds them, and
        return how many landed: a key past `pieces.PIECE` a piece at a time
        (`_carry_in_pieces`), the others in their serialised form, in
        batches of `pieces.BATCH_BYTES` by the memory each takes on the
        source (`_carry_dumped`) - a thousand keys a batch were a thousand
        payloads held at once, of whatever size."""
        sizes = self._memory_of(src, keys)
        big = [k for k in keys if (sizes.get(k) or 0) > pieces.PIECE]
        landed = sum(self._carry_in_pieces(src, dst, k, how, label, log)
                     for k in big)
        rest = [k for k in keys if (sizes.get(k) or 0) <= pieces.PIECE]
        for part in pieces.by_bytes(rest, size=lambda k: (sizes.get(k)
                                                          or 0) + 1):
            landed += self._carry_dumped(src, dst, part, how, label, log)
        return landed

    @staticmethod
    def _memory_of(client, keys):
        """{key: bytes it takes on the server} (`MEMORY USAGE`, which
        samples a collection's elements), in one round trip; a key gone, or
        a server that will not say, is left out - carried whole as before."""
        pipe = client.pipeline(transaction=False)
        for k in keys:
            pipe.memory_usage(k)
        try:
            got = pipe.execute(raise_on_error=False)
        except Exception:  # noqa: BLE001 - no sizes: every key whole
            return {}
        return {k: int(n) for k, n in zip(keys, got)
                if isinstance(n, int) and not isinstance(n, bool)}

    #: the types whose values are written piece by piece; a stream, or a
    #: module's own type, is carried in its serialised form whatever its
    #: size
    PIECE_TYPES = ("string", "hash", "set", "zset", "list")

    def _carry_in_pieces(self, src, dst, key, how, label="", log=None):
        """One key past `pieces.PIECE`, carried a piece at a time: its
        type's own reads on the source (GETRANGE, HSCAN, SSCAN, ZSCAN,
        LRANGE), its type's own writes on the target (APPEND, HSET, SADD,
        ZADD, RPUSH), then its moment of expiry - never held whole here,
        and never one payload past what a target takes in one command (a
        RESTORE of a key past 512 MB is refused). Its access history
        starts again on the target: only a RESTORE sets it. A key written on
        the source while it is read, which `WATCH` says, is read again from
        its start, three times at most; on a cluster, where a transaction
        cannot watch across the client, it is read once and `check` holds
        it to the source. Returns 1 where it landed, 0 where it was gone."""
        import redis
        kind = src.type(key)
        kind = kind.decode() if isinstance(kind, bytes) else str(kind)
        if kind == "none":
            return 0
        if kind not in self.PIECE_TYPES:
            return self._carry_dumped(src, dst, [key], how, label, log)
        for _ in range(3):
            try:
                watch = src.pipeline(transaction=True)
                watch.watch(key)
            except Exception:  # noqa: BLE001 - a cluster: read once
                watch = None
            reader = watch if watch is not None else src
            try:
                at = self._due(reader, key, how)
                dst.unlink(key)
                self._pieces_of(reader, dst, key, kind)
                if at:
                    dst.pexpireat(key, at)
                if watch is not None:
                    watch.multi()
                    watch.exists(key)
                    watch.execute()
                if log and not how.get("pieces_said"):
                    how["pieces_said"] = True
                    log(f"{label}: keys past {pieces.PIECE // 2 ** 20} MB"
                        " carried a piece at a time; their access history"
                        " starts again on the target")
                return 1
            except redis.WatchError:
                continue
            finally:
                if watch is not None:
                    watch.reset()
        if log:
            log(f"{label}: a key past a piece kept changing while it was"
                " read; carried as last read, and `check` holds it to the"
                " source")
        return 1

    def _due(self, client, key, how):
        """The moment a key falls due, in milliseconds since the epoch, or
        0 where it does not - read as `_carry_dumped` reads it: by the
        source's own clock where it answers the time left."""
        if how["expiry"] == "pexpiretime":
            got = client.pexpiretime(key)
            return int(got) if got and int(got) > 0 else 0
        left = client.pttl(key)
        if left is None or int(left) < 0:
            return 0
        sec, usec = self._client("src").time()
        return sec * 1000 + usec // 1000 + int(left)

    @staticmethod
    def _pieces_of(src, dst, key, kind):
        """The value of `key` read from `src` and written to `dst` a piece
        at a time, by its type - the target's key emptied before."""
        piece = pieces.PIECE
        if kind == "string":
            size = int(src.strlen(key))
            for at in range(0, size, piece):
                dst.append(key, src.getrange(key, at,
                                             min(at + piece, size) - 1))
            return
        total = int({"hash": src.hlen, "set": src.scard, "zset": src.zcard,
                     "list": src.llen}[kind](key) or 0)
        if not total:
            return
        # elements a piece: as many as a piece holds at the key's own
        # average, its memory over its elements
        each = max(1, int(src.memory_usage(key) or piece) // total)
        step = max(1, min(10_000, piece // each))
        if kind == "list":
            for at in range(0, total, step):
                got = src.lrange(key, at, at + step - 1)
                if got:
                    dst.rpush(key, *got)
            return
        scan = {"hash": src.hscan, "set": src.sscan, "zset": src.zscan}[kind]
        cursor = 0
        while True:
            cursor, got = scan(key, cursor, count=step)
            if got:
                if kind == "hash":
                    dst.hset(key, mapping=got)
                elif kind == "set":
                    dst.sadd(key, *got)
                else:
                    dst.zadd(key, dict(got))
            if int(cursor) == 0:
                return

    def _carry_dumped(self, src, dst, keys, how, label="", log=None):
        """Copy `keys` from `src` to `dst` in the server's own serialised
        form, and return how many landed.

        The expiry is carried as the moment it falls due (`ABSTTL`), read
        once: the relative TTL this sent before was read a pipeline ahead
        of the write, so every key landed living longer than on the source
        by the time between - by as long as a slow target took, on every
        key. Measured on Redis 7 with a target whose writes took 2 s: 200
        keys landed 2,013 to 2,015 ms late before, 0 ms now; a key idle 2 s
        on the source was idle 0 s on the target before, and kept its idle
        time now. How long since each key was read (`IDLETIME`) or how often
        (`FREQ`, under an LFU policy) comes with it, so the target evicts
        the keys the source would have evicted rather than whichever were
        copied last. The history is read before the payload: `DUMP` is a
        read, and counts as one. A key whose moment has passed by the time
        it is written is not created - the server's own rule for `ABSTTL`.
        A target older than 5.0 is sent the time left to that moment,
        worked out at the write."""
        import redis
        hist = how["history"]
        read = src.pipeline(transaction=False)
        for k in keys:
            read.object(hist, k)
            if how["expiry"] == "pexpiretime":
                read.pexpiretime(k)
            else:
                read.pttl(k)
            read.dump(k)
        clock = None
        if how["expiry"] != "pexpiretime":
            sec, usec = self._client("src").time()
            clock = sec * 1000 + usec // 1000
        read_at = time.monotonic()
        got = read.execute(raise_on_error=False)
        rows = []
        for k, h, exp, payload in zip(keys, got[0::3], got[1::3],
                                      got[2::3]):
            if isinstance(exp, Exception) or isinstance(payload, Exception):
                raise SystemExit(f"{label}: the source would not hand over a"
                                 f" key: {str(exp if isinstance(exp, Exception) else payload)[:90]}")
            if payload is None or exp == -2:
                continue    # gone since the scan found it
            at = 0 if exp is None or exp < 0 else \
                exp if clock is None else clock + exp
            rows.append((k, at, payload,
                         None if isinstance(h, Exception) else h))

        def send(abs_ok):
            mark = self.__dict__.get("_mark_rung") == "multi"
            # a tail that marks sends each batch as one MULTI / EXEC whose
            # first command is the mark: the keys of a batch land with it
            # or not at all
            write = dst.pipeline(transaction=mark)
            if mark:
                write.execute_command(*self._mark_command())
            n = 0
            for k, at, payload, h in rows:
                if abs_ok:
                    extra = ({"frequency": h} if hist == "freq" else
                             {"idletime": h}) if h is not None else {}
                    write.restore(k, at, payload, replace=True,
                                  absttl=bool(at), **extra)
                else:
                    left = 0
                    if at:
                        now = (clock + int((time.monotonic() - read_at)
                                           * 1000)) if clock is not None \
                            else int(time.time() * 1000)
                        left = at - now
                        if left <= 0:
                            continue    # fallen due since it was read
                    write.restore(k, left, payload, replace=True)
                n += 1
            write.execute()
            return n
        from .. import decide

        def run(choice):
            try:
                return send(choice.path == "moment")
            except redis.ResponseError as e:
                if choice.path == "moment" and "syntax" in str(e).lower():
                    # a server that claims 5.0 and takes none of the three:
                    # a restore it refuses writes nothing, and one sent
                    # again replaces what the pipeline did write
                    raise decide.Blocked(
                        "the target takes no absolute expiry or access"
                        " history on a restore") from None
                raise SystemExit(
                    f"{label}: the target refused a key it was handed:"
                    f" {str(e)[:90]}. A payload version error means it runs"
                    " an older Redis than the source, whose values do not"
                    " load into an older version") from None

        def undo(rung, error):
            # kept for the rest of the run: the next batch goes on the time
            # left at once
            how["abs"] = False
        return self._attempt(label or "the keys", self.EXPIRY_WAYS, run,
                             how, log=log, undo=undo,
                             switch=decide.only_blocked)[1]

    def moved_nothing(self, db):
        """The keyspace holds keys on the source and none on the target,
        counting only what the hop does not leave out."""
        try:
            return [f"db{db}"] if self._any_kept("src", db) and \
                not self._any_kept("dst", db) else []
        except Exception:  # noqa: BLE001 - None: cannot be asked
            return None

    def _any_kept(self, side, db):
        """Whether the side's keyspace holds a key the hop does not leave
        out, on any node of a cluster."""
        return any(self._raw_kept(db, keys) for keys, _ in
                   self._walk(side, db, False, self.COPY_BATCH))

    def rows_held(self, db, tables):
        """The keyspace, where the target holds a key the hop does not
        leave out: its copy removes those before it starts afresh."""
        try:
            return set(tables) if self._any_kept("dst", db) else set()
        except Exception:  # noqa: BLE001 - None: cannot be asked
            return None

    def side_database(self, side, db):
        """The same keyspace number on both sides: a hop's `db_map` does
        not move a keyspace to another number."""
        return db

    def server_identity(self, side, db):
        """The process answering (`run_id`, new at every start - a server
        says nothing lasting about itself) and its version."""
        info = self._client(side).info("server")
        return {"running": str(info.get("run_id") or ""),
                "version": str(info.get("redis_version") or "")}

    def snapshot_state(self, db, state_dir, kind="all"):
        """What the target held before a repair: how many keys of each
        kind, and the modules loaded. The keys a repair replaces are kept
        whole beside it, in the repair's own undo file."""
        client = self._keys("dst", db, decode=False)
        kinds = {}
        for keys, _ in self._walk("dst", db, False, self.COPY_BATCH):
            read = client.pipeline(transaction=False)
            for k in self._raw_kept(db, keys):
                read.type(k)
            for t in read.execute():
                t = t.decode()
                kinds[t] = kinds.get(t, 0) + 1
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
            client = self._keys(side, db)
            seen = {}
            for keys in self._scan_batches(side, sample, deep, db):
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
        self._refuse_unread_mapping()
        a = self._count("src", db)
        b = self._count("dst", db)
        # a database's keys are the one table a Redis has: named as the
        # copy names it (`move_key`), so a rule and a finding meet on it
        table = f"db{db}"
        if a != b:
            return [Result("counts", table, "diff", f"src={a} dst={b}",
                           table=table, tables={table: "diff"})]
        return [Result("counts", table, "ok", f"keys {a:,}=={b:,}",
                       table=table, tables={table: "ok"})]

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
        # a key past a piece compared a piece at a time; the others read
        # whole, in batches of `pieces.BATCH_BYTES` by the memory each takes
        sizes = self._memory_of(s, keys)
        plan = []
        for k, ty, dty in zip(keys, stypes, dtypes):
            if ty != dty:
                (missing if dty == "none" else changed).append(k)
            elif ty in self.PIECE_TYPES and (sizes.get(k) or 0) \
                    > pieces.PIECE:
                if not self._compare_in_pieces(s, t, k, ty):
                    changed.append(k)
            else:
                plan.append((k, ty))
        for part in pieces.by_bytes(plan, size=lambda kt: (sizes.get(kt[0])
                                                           or 0) + 1):
            ps = s.pipeline(transaction=False)
            pt = t.pipeline(transaction=False)
            for k, ty in part:
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
            for (k, _), a, b in zip(part, ps.execute(), pt.execute()):
                if a != b:
                    changed.append(k)
        return missing, changed

    @staticmethod
    def _compare_in_pieces(s, t, key, kind):
        """Whether one key past `pieces.PIECE` holds the same on both
        sides, read a piece at a time and stopping at the first piece that
        differs: a string and a list by their ranges, a hash, a set and a
        sorted set by the source's scan asked of the target member by
        member - the same number of members, each there with the same
        value, is the same collection."""
        piece = pieces.PIECE
        if kind == "string":
            size = int(s.strlen(key))
            if size != int(t.strlen(key)):
                return False
            return all(s.getrange(key, at, min(at + piece, size) - 1)
                       == t.getrange(key, at, min(at + piece, size) - 1)
                       for at in range(0, size, piece))
        count = {"hash": "hlen", "set": "scard", "zset": "zcard",
                 "list": "llen"}[kind]
        total = int(getattr(s, count)(key) or 0)
        if total != int(getattr(t, count)(key) or 0):
            return False
        if not total:
            return True
        each = max(1, int(s.memory_usage(key) or piece) // total)
        step = max(1, min(10_000, piece // each))
        if kind == "list":
            return all(s.lrange(key, at, at + step - 1)
                       == t.lrange(key, at, at + step - 1)
                       for at in range(0, total, step))
        scan = {"hash": s.hscan, "set": s.sscan, "zset": s.zscan}[kind]
        cursor = 0
        while True:
            cursor, got = scan(key, cursor, count=step)
            if got:
                if kind == "hash":
                    fields = list(got)
                    if t.hmget(key, fields) != [got[f] for f in fields]:
                        return False
                elif kind == "set":
                    pipe = t.pipeline(transaction=False)
                    for m in got:
                        pipe.sismember(key, m)
                    if not all(pipe.execute()):
                        return False
                else:
                    pipe = t.pipeline(transaction=False)
                    for m, _ in got:
                        pipe.zscore(key, m)
                    if pipe.execute() != [score for _, score in got]:
                        return False
            if int(cursor) == 0:
                return True

    def _scan_batches(self, side, sample, deep, db=0):
        """Batches of keys from one side, up to the sample cap - shared
        out among a cluster's masters, so a sample is taken from every
        node and not from the first one's keys alone."""
        nodes = self._nodes(side, db)
        share = -(-int(sample) // len(nodes)) if nodes else 0
        for _, client in nodes:
            cursor = seen = 0
            while True:
                cursor, keys = client.scan(cursor, count=1000)
                keys = self._kept(db, keys)
                if not deep and seen + len(keys) > share:
                    keys = keys[:max(0, share - seen)]
                if keys:
                    seen += len(keys)
                    yield keys
                if cursor == 0 or (not deep and seen >= share):
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
        self._refuse_unread_mapping()
        s = self._keys("src", db)
        t = self._keys("dst", db)
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
        for keys in self._scan_batches("src", sample, deep, db):
            with src_gate.unit():
                gone, differ = self._batch_compare(s, t, keys)
            missing.extend(gone)
            changed.extend(differ)
            checked += len(keys)
            if stream and checked % 20000 < 1000:
                stream(f"db{db}: {checked} keys compared")
        extra = []
        seen_dst = 0
        for keys in self._scan_batches("dst", sample, deep, db):
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
        table = f"db{db}"
        state = {table: "diff" if bad or extra else "ok"}

        def counted(**only):
            """The rows apart this finding is about, counted key by key
            where both sides were read whole, the least there are of a
            sample."""
            got = {"missing": 0, "extra": 0, "changed": 0, **only}
            return rows_counted(table, got["missing"], got["extra"],
                                got["changed"], exact=deep)
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
                              " --kind rows --apply", table=table,
                              rows_apart=counted(missing=len(missing),
                                                 changed=len(changed)),
                              tables=state))
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
                " first; the key count can match while this is true",
                table=table, rows_apart=counted(extra=len(extra)),
                tables=state))
        return res or [Result(
            "data", f"db{db}", "ok",
            f"{checked} keys value-equal, {seen_dst} target keys all present"
            f" on the source ({mode}, pipelined)"
            + (f"; the difference was still arriving ({proof})"
               if proof else ""), table=table, tables=state)]

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

    def in_scope(self, db, table):
        """Whether the hop takes a key of a database, matched as `db.key`
        and as `key` - the one scope answer here, which `_kept` asks of
        every key a check, a copy or a repair reads. A key of "" is the
        keyspace itself: one the hop leaves out whole (`exclude: ["5"]`)."""
        return not scoped(self.hop) or \
            not self.hop.excluded(str(db), table)

    def _read_drilldown(self, db, kind, kept=True):
        """The keys the last check named, of the ones the hop takes
        (`_kept`): a key it named under another scope - an `exclude` added
        since - is the target's own now, and the repair deleted it."""
        import json
        keys = [json.loads(line) for line in
                self._read_drill(db, f"db{db}", kind)]
        return self._kept(db, keys) if kept else keys

    def _drilled(self, db, held=True):
        """{kind: keys} the last check named, of the keys the hop takes -
        and, with `held`, of the kinds it does not keep: where the hop does
        not carry deletes the target's extra keys are its own, where it
        carries no inserts the missing ones stay missing, and no updates,
        the changed ones stay as they are (`_repairable`)."""
        found = {kind: self._read_drilldown(db, kind)
                 for kind in ("missing", "changed", "extra")}
        return self._repairable(db, f"db{db}", found) if held else found

    def repair_plan(self, db, kind):
        """What `migkit sync --kind rows` would do to this database.

        The keys come from the last check's drilldown files, so the plan
        describes what was actually found rather than re-scanning and acting
        on something the operator never saw.
        """
        if kind not in ("rows", "all"):
            return []
        _, said = self._drills_in_scope(db, sorted({
            k for found in ("missing", "changed", "extra")
            for k in self._read_drilldown(db, found, kept=False)}), "keys")
        out = [said] if said else []
        found = self._drilled(db)
        missing, changed, extra = (found["missing"], found["changed"],
                                   found["extra"])
        left = self._kept_said(self._drilled(db, held=False), found)
        if left:
            out.append(RepairAction(f"db{db}", LEFT_OUT, [], [],
                                    f"left as they are, the hop keeps"
                                    f" them: {left}"))
        if not (missing or changed or extra):
            return out
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
        return out + [RepairAction(
            f"db{db}", "rows", statements, [],
            f"{len(missing)} missing, {len(changed)} changed,"
            f" {len(extra)} extra; every key the repair overwrites or removes"
            " is dumped to the undo file before it is touched",
            table=f"db{db}")]

    #: a key is put back from its serialised value by its own record
    #: (`repair_io`), never as canonical text: repaired key by key alone
    ROWS_PUT_BACK = False

    def repair_found(self, db, table):
        """The keys the last check named."""
        return {kind: [(k,) for k in self._read_drilldown(db, kind)]
                for kind in ("missing", "changed", "extra")}

    def repair_sides(self, db, table):
        """None: a database's keys are lined up by name alone, the rows
        way (`repairways`)."""
        return None

    def apply(self, db, action):
        """Copy the keys back with DUMP/RESTORE, then remove the strays.

        DUMP carries the type with the value, so one path covers strings,
        hashes, lists, sets, sorted sets and streams alike - measured
        across two servers, a hash came back a hash - and the expiry and
        access history go with it the way the copier sends them
        (`_carry`).

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
        raw_s = self._keys("src", db, decode=False)
        raw_t = self._keys("dst", db, decode=False)
        how = self._carry_plan(db)
        undo_dir = self.hop.report_dir(db) / "undo"
        undo_dir.mkdir(parents=True, exist_ok=True)
        undo = undo_dir / f"db{db}.keys.jsonl"
        found = self._drilled(db)
        missing, changed, extra = (found["missing"], found["changed"],
                                   found["extra"])
        if changed and self.newer_wins_column():
            changed = self._newer_keys(db, raw_s, raw_t, changed)
        # every statement written down before any lands (`repairlog`): each
        # key's serialised value and expiry as the target holds it, and as
        # the source's will be
        from .. import repairlog
        io = repairlog.DumpIO(raw_t, f"db{db}")
        writes = [(k,) for k in missing + changed]
        before = io.rows(writes + [(k,) for k in extra])
        after = repairlog.DumpIO(raw_s, f"db{db}").rows(writes)
        rec = repairlog.Record(undo_dir, f"db{db}", "dump", (), ["key"])
        num = {}
        for k in writes:
            if k in after:
                num[k] = rec.planned("write", k, before.get(k), after[k])
        for k in extra:
            num[(k,)] = rec.planned("delete", (k,), before.get((k,)), None)
        rec.ready()

        def remember(client, key, handle):
            payload = client.dump(key.encode())
            if payload is None:
                return
            ttl = client.pttl(key.encode())
            handle.write(json.dumps({
                "key": key, "pttl": ttl,
                "dump": base64.b64encode(payload).decode()}) + "\n")

        with undo.open("a") as handle, rec:
            done = 0
            for key in missing + changed:
                remember(raw_t, key, handle)
                try:
                    # gone from the source since the check: nothing lands
                    self._carry(raw_s, raw_t, [key.encode()], how,
                                f"db{db}")
                except SystemExit as e:
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
                rec.applied(num.get((key,)))
            for key in extra:
                remember(raw_t, key, handle)
                raw_t.delete(key.encode())
                rec.applied(num.get((key,)))

    def _newer_keys(self, db, src, dst, changed):
        """The changed keys to overwrite under `newer_wins`: a hash whose
        field the hop names is later on the target is kept, and the key
        written to `data-db<N>.kept-newer`. A key that holds no such field
        on either side - a string, a list, a hash without it - is
        overwritten: the source is the truth where the values cannot say
        otherwise. Compared as numbers where both read as numbers (an epoch,
        a counter), else as text (an ISO timestamp)."""
        field = self.newer_wins_column()

        def read(side, keys):
            client = src if side == "src" else dst
            out = {}
            for k in keys:
                try:
                    got = client.hget(k.encode(), field.encode())
                except Exception:  # noqa: BLE001 - not a hash: no field
                    got = None
                if got is None:
                    continue
                text = got.decode("utf-8", "replace")
                try:
                    out[k] = float(text)
                except ValueError:
                    out[k] = text
            return out
        overwrite, kept = self._keep_the_newer(f"db{db}", changed, read)
        if kept:
            (self.hop.report_dir(db) / f"data-db{db}.kept-newer").write_text(
                "".join(k + "\n" for k in kept))
        return overwrite

    def repair_io(self, db, head):
        """A repair record's keys on the target, as their serialised
        values."""
        from .. import repairlog
        if head.get("form") != "dump":
            return super().repair_io(db, head)
        return repairlog.DumpIO(self._keys("dst", db, decode=False),
                                head["table"])

    def check_deep(self, db):
        s = self._keys("src", db)
        t = self._keys("dst", db)
        sample = int(self.hop.options.get("sample", 5000))
        res = []
        # ttl drift: movers frequently drop or reset expirations; a key
        # that outlives its source ttl serves stale data forever
        seen = 0
        no_ttl = []
        drifted = []
        big = []
        streams = []
        for keys in self._scan_batches("src", sample, False, db):
            if keys:
                ps = s.pipeline(transaction=False)
                pt = t.pipeline(transaction=False)
                pm = s.pipeline(transaction=False)
                py = s.pipeline(transaction=False)
                for k in keys:
                    ps.pttl(k)
                    pt.pttl(k)
                    pm.memory_usage(k, samples=0)
                    py.type(k)
                for k, a, b, mem, kind in zip(
                        keys, ps.execute(), pt.execute(), pm.execute(),
                        py.execute()):
                    seen += 1
                    if kind == "stream":
                        streams.append(k)
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
        res.append(self._boundary(db, s, t, streams))
        res.append(self._key_grants(db))
        return res

    @staticmethod
    def _stream_id(stamp):
        """A stream entry's id (`1700000000000-3`) as the pair it orders
        by, or None where it is none."""
        try:
            ms, _, seq = str(stamp).partition("-")
            return int(ms), int(seq or 0)
        except ValueError:
            return None

    def _boundary(self, db, s, t, streams):
        """Whether the target is ahead of the source. The one thing a
        Redis counts forward on its own is a stream's ids: a target whose
        stream has handed out an id the source's has not was added to
        there - by a writer on the target, or an entry applied twice.
        Read from `XINFO STREAM`, which keeps the last id handed out even
        after its entry is deleted."""
        ahead, behind, read = [], 0, 0
        for k in streams:
            try:
                a = self._stream_id(s.xinfo_stream(k)["last-generated-id"])
                b = self._stream_id(t.xinfo_stream(k)["last-generated-id"])
            except Exception:  # noqa: BLE001 - not on the target, or moved
                continue
            if a is None or b is None:
                continue
            read += 1
            if b > a:
                ahead.append(k)
            elif b < a:
                behind += 1
        if ahead:
            return Result(
                "deep", f"db{db} boundary", "diff",
                f"target stream ids AHEAD of the source on {len(ahead)}"
                f" of {read} streams: {', '.join(sorted(ahead)[:4])}", "",
                "writes landing on the target or entries applied twice,"
                " find the writer before cutover")
        note = f"; {behind} behind (replication lag)" if behind else ""
        return Result("deep", f"db{db} boundary", "ok",
                      f"last stream ids checked on {read} sampled streams,"
                      f" none ahead of the source{note}")

    def _key_grants(self, db):
        """The key patterns each account may read and write, held to the
        target's: an account present on both with other patterns reaches
        other keys there. Read through the one reader of accounts
        (`users`, from `ACL LIST`); the accounts themselves and their
        passwords are `migkit users`'s."""
        from .. import users
        try:
            src = users._redis_users(self.hop.source)
            dst = users._redis_users(self.hop.target)
        except Exception as e:  # noqa: BLE001 - no ACL, or not allowed
            return Result("deep", f"db{db} grants", "warn",
                          "the accounts could not be read, so the key"
                          " patterns they may reach were not compared: "
                          + (str(e).splitlines() or [type(e).__name__])[-1]
                          [:80])

        def reach(user):
            return sorted(r for r in user["rules"]
                          if r.startswith(("~", "%"))
                          or r in ("allkeys", "resetkeys"))
        both = sorted(set(src) & set(dst))
        differ = [f"{u} src({' '.join(reach(src[u])) or 'no keys'})"
                  f" dst({' '.join(reach(dst[u])) or 'no keys'})"
                  for u in both if reach(src[u]) != reach(dst[u])]
        if differ:
            return Result(
                "deep", f"db{db} grants", "diff",
                f"{len(differ)} of {len(both)} accounts reach other keys"
                f" on the target: {clauses(differ, 4)}", "",
                f"migkit users {self.hop.name} test lists the accounts"
                " that differ; set the target's rules as the source's")
        return Result("deep", f"db{db} grants", "ok",
                      f"{len(both)} accounts on both sides reach the same"
                      " key patterns")

    def watch_sample(self, db):
        return {"db": f"db{db}", "ts": time.time(),
                "src_rows": self._dbsize("src", db),
                "dst_rows": self._dbsize("dst", db)}

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
        if scoped(self.hop):
            return ("the hop leaves keys out, and a Redis replica carries"
                    " every key of the source: its first sync empties the"
                    " target, the keys left out with it")
        from ..config import BY_NAME, narrowed_by
        other = sorted(k for k, v in narrowed_by(self.hop).items()
                       if v and k not in BY_NAME)
        if other:
            return ("the hop asks for more than every key, whole, as the"
                    " source has it ("
                    + ", ".join(k.replace("_", " ") for k in other)
                    + "), and a Redis replica applies every change of the"
                    " source as it is")
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

    @staticmethod
    def _snapshot_valkey_refuses(s, d):
        """Why a Valkey target cannot take a Redis source's keys, or None.

        Redis 7.4 and later write a snapshot format (and a DUMP payload)
        that Valkey, which forked from 7.2, does not read: a replica's
        first sync fails with the target already emptied, and a restore of
        a dumped key is refused. Said before the move starts."""
        import re
        if not (s.name == "redis" and d.name == "valkey"):
            return None
        got = tuple(int(x) for x in re.findall(r"\d+", str(s.version))[:2])
        if got >= (7, 4):
            return (f"the source is Redis {s.version}, whose snapshots and"
                    " dumped keys Valkey does not read (they changed after"
                    " 7.2): a replica's first sync would empty the target"
                    " and fail, and a restored key is refused")
        return None

    def _older_target(self):
        """Why the target cannot load the source's snapshot, or None.

        A replica loads the source's snapshot, and a server does not read
        the format of a newer one - measured, a 7.4 source and a 6.2 target:
        `Can't handle RDB format version 12`, the link down for good, and
        the target already emptied. The same software is compared by its
        own version; different software by the Redis version each says it
        is compatible with, which is what decides the format it reads."""
        s, d = self._brands()
        newer = self._snapshot_valkey_refuses(s, d)
        if newer:
            return newer
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

    def log_position(self, side, db):
        """Where a side's replication stream is now - its replication id
        and the offset in it, `id:offset`. After the cutover's freeze this
        is the point the drain waits for the target to reach."""
        r = self._client(side).info("replication")
        offset = int(r.get("master_repl_offset") or 0)
        return f"{r.get('master_replid')}:{offset}"

    @staticmethod
    def position_reached(have, want):
        """Whether a stream at `have` has applied at least as far as
        `want`: offsets compare only within one replication id, and
        another id (a failover, a restart) cannot say."""
        try:
            h_id, _, h_off = str(have).rpartition(":")
            w_id, _, w_off = str(want).rpartition(":")
            if not h_id or h_id != w_id:
                return None
            return int(h_off) >= int(w_off)
        except ValueError:
            return None

    def src_lsn(self, db):
        """Where the source's replication stream is now: its replication
        id and offset."""
        return self.log_position("src", db)

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
        s, t = self._keys("src", db), self._keys("dst", db)
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

    # --- verifying only what changed ---------------------------------------
    #: keys told of in one cycle past which the cycle compares them all
    #: instead: a set that large is the keyspace written wholesale
    DELTA_MOST = 200_000

    def _listening(self, db):
        """The listener of this process for the source's written keys, or
        None where there is none yet."""
        return getattr(self, "_listeners", {}).get(str(db))

    def _listen(self, db):
        """Start being told of every key the source has written from now:
        a connection of migkit's is put in broadcast tracking (`CLIENT
        TRACKING ... BCAST`), redirected to a second one that listens on the
        server's invalidation channel. Nothing of the server's is changed -
        tracking belongs to the connection and ends with it - and no key
        is read to learn it. On a cluster, every master is listened to:
        a node tells of the keys of its own slots only."""
        import threading
        state = {"keys": set(), "whole": False, "lost": None,
                 "since": time.time(), "stop": False,
                 "lock": threading.Lock(), "parts": []}
        try:
            for name, _ in self._nodes("src", db, False):
                host, port = (name.rsplit(":", 1) if name else
                              (self.hop.source.host, self.hop.source.port))
                self._listen_node(host, int(port), db, state)
        except Exception:
            self._stop_parts(state)
            raise
        if not hasattr(self, "_listeners"):
            self._listeners = {}
        self._listeners[str(db)] = state
        return state

    def _listen_node(self, host, port, db, state):
        import threading
        import uuid

        import redis
        ep = self.hop.source
        name = f"migkit-delta-{uuid.uuid4().hex[:12]}"
        # RESP2: over RESP3 (the client's default since 8) the server
        # pushes each invalidation as its own kind of message, which the
        # listening connection never hands over - measured, nothing heard
        made = self._connection_class("src", False)
        on = {} if self._cluster("src") else {"db": int(db)}
        client = redis.Redis(connection_pool=redis.ConnectionPool(
            connection_class=made, host=host, port=port,
            password=ep.password or None, socket_timeout=15,
            client_name=name, protocol=2, **on))
        sub = client.pubsub(ignore_subscribe_messages=True)
        sub.subscribe("__redis__:invalidate")
        ids = [c["id"] for c in client.client_list(_type="pubsub")
               if c.get("name") == name]
        if not ids:
            sub.close()
            raise RuntimeError("the listening connection could not be found"
                               " on the server")
        tracker = made(host=host, port=port, password=ep.password or None,
                       socket_timeout=15, protocol=2)
        tracker.connect()
        tracker.send_command("CLIENT", "TRACKING", "ON", "REDIRECT", ids[0],
                             "BCAST")
        if tracker.read_response() not in (b"OK", "OK"):
            raise RuntimeError("the source would not track its keys")

        def read():
            while not state["stop"]:
                try:
                    msg = sub.get_message(timeout=1.0)
                    # the tracking ends with its connection, and the client
                    # connects the listener again by itself, under another
                    # id the tracking does not send to - measured, a
                    # listener cut off and back went on hearing nothing,
                    # and the server's own tracking info still named the
                    # old one. So the one it sends to is asked for by id
                    tracker.send_command("CLIENT", "LIST", "ID", ids[0])
                    if not tracker.read_response():
                        raise RuntimeError("the listening connection was"
                                           " closed")
                except Exception as e:  # noqa: BLE001 - said by the cycle
                    with state["lock"]:
                        state["lost"] = f"{type(e).__name__}: {str(e)[:80]}"
                    return
                if not msg or msg.get("type") != "message":
                    continue
                keys = msg.get("data")
                with state["lock"]:
                    if keys is None:
                        # the keyspace flushed: every key may have changed
                        state["whole"] = True
                    else:
                        state["keys"].update(
                            k.decode("utf-8", "surrogateescape")
                            if isinstance(k, bytes) else str(k)
                            for k in keys)
                        if len(state["keys"]) > self.DELTA_MOST:
                            state["whole"] = True
        thread = threading.Thread(target=read, daemon=True)
        state["parts"].append({"sub": sub, "tracker": tracker,
                               "client": client, "thread": thread})
        thread.start()

    @staticmethod
    def _stop_parts(state):
        state["stop"] = True
        for part in state.get("parts", []):
            part["thread"].join(timeout=5)
            for what in ("sub", "tracker", "client"):
                try:
                    getattr(part[what], "close",
                            getattr(part[what], "disconnect", None))()
                except Exception:  # noqa: BLE001 - closing, nothing to keep
                    pass

    def delta_teardown(self, db):
        state = getattr(self, "_listeners", {}).pop(str(db), None)
        if state:
            self._stop_parts(state)

    def delta_verify(self, db, limit=20000, log=None):
        """Only the keys written since the last cycle, as the source tells
        them (`_listen`), compared on both sides as `check` compares a key;
        a key that differed is compared again next cycle until it matches.

        The first cycle has nothing to go on: it starts listening and
        compares nothing, as a baseline does. A cycle whose listener lost
        its connection cannot say what was written meanwhile, and says so
        rather than pass; one told the keyspace was flushed, or of more
        keys than a cycle compares, compares the keyspace whole. Both start
        listening again."""
        import json
        state = self._listening(db)
        if state is None:
            self._listen(db)
            return [Result("delta", f"db{db}", "ok",
                           "listening for the keys the source writes from"
                           " now: the next cycle compares them")]
        with state["lock"]:
            keys, whole, lost = state["keys"], state["whole"], state["lost"]
            state["keys"], state["whole"] = set(), False
        keys |= getattr(self, "_still_differ", {}).get(str(db), set())
        if lost:
            self.delta_teardown(db)
            self._listen(db)
            return [Result("delta", f"db{db}", "error",
                           "the listener for written keys lost its"
                           f" connection ({lost}): what was written since"
                           f" {time.strftime('%H:%M:%S', time.localtime(state['since']))}"
                           " was not seen - run check; listening again",
                           "", f"migkit check {self.hop.name}")]
        if whole:
            got = self.check_data(db)
            return [Result("delta", f"db{db}", got[0].status,
                           "the keyspace was flushed or written wholesale,"
                           " so it was compared whole: " + got[0].detail)] \
                + got[1:]
        kept = [k for k in keys if self._kept(db, [k])]
        missing, extra, changed = (
            [json.loads(k) for k in ks]
            for ks in self._compare_pks(db, f"db{db}",
                                        {json.dumps(k) for k in kept})) \
            if kept else ([], [], [])
        bad = set(missing) | set(extra) | set(changed)
        if not hasattr(self, "_still_differ"):
            self._still_differ = {}
        self._still_differ[str(db)] = bad
        if log:
            log(f"{len(kept)} written keys compared, {len(bad)} differ")
        if bad:
            parts = [f"{n} {w}" for n, w in (
                (len(missing), "missing on the target"),
                (len(extra), "on the target only"),
                (len(changed), "with another value")) if n]
            return [Result("delta", f"db{db}", "diff",
                           f"{len(bad)} of {len(kept)} written keys differ:"
                           f" {', '.join(parts)} - compared again next cycle"
                           )]
        return [Result("delta", f"db{db}", "ok",
                       f"{len(kept)} written keys, every one the same on"
                       " the target")]

    def _write_pk_files(self, db, table, missing, extra, changed):
        self._write_drill(db, table, missing=missing, extra=extra,
                          changed=changed)

    # --- the tail by key space, lossless (P0-PARITY.tail, LT.C6) ----------

    def _config_one(self, client, name):
        try:
            got = client.config_get(name)
        except Exception:  # noqa: BLE001 - a managed server hides CONFIG
            return None
        return got.get(name) if isinstance(got, dict) else None

    def _replica_grant(self, client):
        """Whether the account the hop signs in with may replicate: asked
        of the server's own access check where it has one, read off the
        account where it has accounts but no check, and yes where it keeps
        no accounts at all. A managed server that renames or removes the
        command answers no."""
        import redis
        user = getattr(self.hop.source, "user", None) or "default"
        try:
            return client.execute_command("ACL", "DRYRUN", user, "PSYNC",
                                          "?", "-1") in ("OK", b"OK")
        except redis.ResponseError as e:
            text = str(e).lower()
            if "unknown command" in text:
                return True
            if "unknown subcommand" not in text:
                return False
        try:
            rules = client.execute_command("ACL", "GETUSER", user)
        except redis.ResponseError:
            return False
        rules = dict(zip(rules[::2], rules[1::2])) if isinstance(
            rules, list) else rules or {}
        said = str(rules.get("commands") or rules.get(b"commands") or "")
        return "-psync" not in said and any(
            w in said for w in ("+@all", "+psync", "+@admin"))

    def live_facts(self, client=None):
        """What choosing the live way reads off the source (`decide.
        redis_live`): the replica grant, whether it is a cluster, whether
        it keeps idle times, and the memory the replica's buffer may take
        against what is free. The buffer is bounded by its hard limit
        where one is set (the source drops a replica at it, which the
        stream sees); with none, by the write rate measured over the
        source's last timed snapshot. `client` is one shard's server of a
        cluster, asked as one server."""
        src = client or self._client("src")
        mem = src.info("memory")
        policy = str(mem.get("maxmemory_policy")
                     or self._config_one(src, "maxmemory-policy") or "")
        most = int(mem.get("maxmemory") or 0) or int(
            mem.get("total_system_memory") or 0)
        used = int(mem.get("used_memory") or 0)
        backlog = int(self._config_one(src, "repl-backlog-size") or 0)
        need = None
        limits = str(self._config_one(src, "client-output-buffer-limit")
                     or "").split()
        for i, word in enumerate(limits[:-1]):
            if word in ("replica", "slave") and limits[i + 1].isdigit() \
                    and int(limits[i + 1]):
                need = backlog + int(limits[i + 1])
        if need is None:
            took = int(src.info("persistence").get("rdb_last_bgsave_time_sec")
                       or -1)
            if took >= 0:
                a = int(src.info("replication").get("master_repl_offset")
                        or 0)
                t0 = time.monotonic()
                time.sleep(1)
                b = int(src.info("replication").get("master_repl_offset")
                        or 0)
                need = backlog + int((b - a) / (time.monotonic() - t0)
                                     * max(took, 1))
        return {"replica grant": self._replica_grant(src),
                "cluster": False if client is not None
                else self._cluster("src"),
                "idletime": "lfu" not in policy,
                "headroom": max(0, most - used) if most else None,
                "stream needs": need}

    def live_way(self, facts=None, client=None):
        """The `Choice` of the live way, from `live_facts` and anything
        measured since (`facts`)."""
        from .. import decide
        got = self.live_facts(client) if client is not None else \
            self.live_facts()
        got.update(facts or {})
        return decide.redis_live(got)

    def _key_spec(self, client):
        """`spec(argv) -> [keys]` from the source's own command table, each
        command asked once; a command whose keys move with its arguments is
        asked with them (`COMMAND GETKEYS`). `spec.categories(name)` is
        the command's categories, once it was asked."""
        seen, cats = {}, {}

        def spec(argv):
            name = argv[0].lower()
            if name not in seen:
                info = client.execute_command("COMMAND", "INFO", name)
                row = (next(iter(info.values()), None) if isinstance(
                    info, dict) else info[0] if info else None)
                if isinstance(row, dict):
                    row = [name, row.get("arity"), row.get("flags") or (),
                           row.get("first_key_pos"), row.get("last_key_pos"),
                           row.get("step_count"),
                           row.get("acl_categories") or ()]
                if not row:
                    # a write the source cannot place: its keys unknown
                    seen[name] = None
                else:
                    flags = {(f.decode() if isinstance(f, bytes) else str(f))
                             .lower() for f in (row[2] or ())}
                    cats[name] = {(c.decode() if isinstance(c, bytes)
                                   else str(c)).lower()
                                  for c in (row[6] if len(row) > 6 else ())
                                  or ()}
                    seen[name] = (int(row[3] or 0), int(row[4] or 0),
                                  int(row[5] or 0), "movablekeys" in flags)
            if seen[name] is None:
                return None
            first, last, step, movable = seen[name]
            if movable:
                return list(client.execute_command("COMMAND", "GETKEYS",
                                                   *argv))
            if not first or not step:
                return []
            end = len(argv) + last if last < 0 else last
            return list(argv[first:end + 1:step])
        spec.categories = lambda name: cats.get(name, set())
        return spec

    def _refresh_keys(self, db, ops):
        """`ops` {key: what the stream did to it}: each key read again from
        the source and written on the target as it is now, or removed there
        where the source has it no longer - the same whether done once or
        again after a stop, so a replayed round changes nothing. What
        happened is counted against the target as it was."""
        tally = dict.fromkeys(KEY_EVENTS, 0)
        ops = {k.encode("latin-1") if isinstance(k, str) else k: v
               for k, v in ops.items()}
        keys = self._raw_kept(db, list(ops))
        if not keys:
            return tally
        src = self._keys("src", db, decode=False, no_touch=self._no_touch())
        dst = self._keys("dst", db, decode=False)

        def types(client):
            p = client.pipeline(transaction=False)
            for k in keys:
                p.type(k)
            return [t.decode() if isinstance(t, bytes) else t
                    for t in p.execute()]
        now, before = types(src), types(dst)
        # only the changes the hop carries (`mapping.ops`): a kept delete
        # leaves the key on the target, as the copy does
        carried = self.hop.carries(f"db{db}")
        applied = [(k, a, b) for k, a, b in zip(keys, before, now)
                   if ("delete" if b == "none" else "insert" if a == "none"
                       else "update") in carried]
        gone = [k for k, a, b in applied if b == "none"]
        if gone:
            if self.__dict__.get("_mark_rung") == "multi":
                unit = dst.pipeline(transaction=True)
                unit.execute_command(*self._mark_command())
                unit.delete(*gone)
                unit.execute()
            else:
                dst.delete(*gone)
        here = [k for k, a, b in applied if b != "none"]
        if here:
            self._carry(src, dst, here, self._carry_plan(db))
        for k, a, b in applied:
            tally[key_event(ops[k], a, b)] += 1
        return tally

    def follow_live(self, db, state, save, log, stop=lambda: False,
                    rounds=None):
        """Follow the source past the copy by the way `live_way` chose,
        re-chosen where a fact it rested on proves wrong as it runs (the
        source refuses the stream, or cannot continue from the offset
        saved). `state` is the tail's saved state, `save()` writes it; the
        offset or the round's start moves only after the keys it covers
        landed on the target. A cluster is followed one stream a shard
        (`_follow_shards`)."""
        if self._cluster("src"):
            return self._follow_shards(db, state, save, log, stop, rounds)
        return self._follow_one(db, state, save, log, stop, rounds)

    def _follow_one(self, db, state, save, log, stop, rounds, node=None,
                    guard=None):
        client = self._node_client(node[1], node[2]) if node else None
        measured, failed_at = {}, None
        while not stop():
            choice = self.live_way(measured, client=client)
            if choice.rung is None:
                raise SystemExit(f"db{db}: no live way holds - "
                                 f"{choice.reason}")
            log(f"db{db}: {choice.rung.said}")
            if choice.rung.name == "idle-time sweep":
                if node is not None:
                    raise SystemExit(
                        f"the shard from slot {node[0]} can no longer be"
                        f" followed as a replica ({choice.reason}); the"
                        " tail stops with each shard's offset saved, and"
                        " its next start sweeps the cluster whole")
                return self._sweep_rounds(db, state, save, log, stop, rounds)
            try:
                return self._stream_rounds(db, state, save, log, stop,
                                           rounds, node=node, guard=guard)
            except LiveRefused as e:
                at = (state.get("keyspaces") or {}).get("offset")
                if e.facts.get("continued", True) is not False \
                        and failed_at is not None and at == failed_at:
                    raise SystemExit(f"db{db}: the stream fails again at"
                                     f" offset {at} - {e}; the tail stops"
                                     " there, its offset saved") from e
                failed_at = at
                log(f"db{db}: {e}; choosing again")
                measured.update(e.facts)

    def _node_client(self, host, port, decode=True):
        import redis
        ep = self.hop.source
        pool = redis.ConnectionPool(
            connection_class=self._connection_class("src", decode),
            host=host, port=int(port), password=ep.password or None,
            socket_timeout=15, decode_responses=decode,
            encoding_errors="surrogateescape")
        return redis.Redis(connection_pool=pool)

    def _shards(self):
        """[(first slot, host, port, slots)] of the source's primaries. A
        shard is named by the first slot it serves, which a failover keeps:
        the promoted replica continues the stream from the offset saved."""
        own = {}
        cache = self._keys("src", 0).nodes_manager.slots_cache
        for slot, nodes in cache.items():
            if nodes:
                own.setdefault((nodes[0].host, int(nodes[0].port)),
                               set()).add(int(slot))
        return sorted((str(min(s)), h, p, s) for (h, p), s in own.items())

    def _follow_shards(self, db, state, save, log, stop, rounds):
        """A cluster source followed one replica stream a shard, each at
        its own offset in the ledger (`state["shards"]`, by first slot) and
        resumed from it; the streams read side by side, every change to
        the ledger and every save under one lock. Where any shard cannot
        stream (`decide.redis_live` asked of that shard's own server), the
        cluster is swept whole."""
        import threading
        ledger = state.setdefault("shards", {})
        shards = self._shards()
        weak = {}
        for first, host, port, _ in shards:
            choice = self.live_way({}, client=self._node_client(host, port))
            if choice.rung is None or choice.rung.name != "replica stream":
                weak[first] = choice
        if weak:
            for first, c in sorted(weak.items()):
                log(f"the shard from slot {first}: "
                    + (c.reason or c.rung.said))
            log("the cluster is swept whole, since a shard cannot be"
                " followed as a replica")
            return self._sweep_rounds(db, state, save, log, stop, rounds)
        for gone in sorted(set(ledger) - {s[0] for s in shards}):
            log(f"no shard serves from slot {gone} now: its keys' writes"
                " reach the streams of the shards serving them")
            ledger.pop(gone)
        lock = threading.RLock()
        failed = []

        def saved():
            with lock:
                save()

        def one(first, host, port, slots):
            try:
                self._follow_one(db, ledger.setdefault(first, {}), saved,
                                 lambda m: log(f"shard {first}: {m}"), stop,
                                 rounds, node=(first, host, port, slots),
                                 guard=lock)
            except BaseException as e:  # noqa: BLE001 - raised below
                failed.append(e)
        runs = [threading.Thread(target=one, args=s, daemon=True)
                for s in shards]
        for t in runs:
            t.start()
        for t in runs:
            t.join()
        if failed:
            raise failed[0]
        return ledger

    def _copy_again(self, d, log):
        self.move_table(d, "", f"db{d}", None, _NoCheckpoint(), log)

    def _stream_socket(self, host=None, port=None, side="src"):
        import socket
        ep = self.hop.source if side == "src" else self.hop.target
        host, port = host or ep.host, port or ep.port
        sock = socket.create_connection((host, port), timeout=30)
        if getattr(ep, "tls", None):
            import ssl
            sock = ssl.create_default_context().wrap_socket(
                sock, server_hostname=host)
        return sock

    def close_streams(self):
        for stream, _, _ in self.__dict__.pop("_streams", {}).values():
            stream.close()

    def _stream_rounds(self, db, state, save, log, stop, rounds=None,
                       node=None, guard=None):
        """Rounds of the replica stream, over a connection kept open from
        one call to the next while the saved state is the one it read. A
        full start fills the key spaces it copies from the snapshot the
        source sends (`_seed_from`, as `decide.redis_seed` chose), the scan
        where that does not hold. `node` is a cluster's shard (first slot,
        host, port, slots); `guard` the lock its ledger is changed under."""
        import contextlib
        ep = self.hop.source
        guard = guard or contextlib.nullcontext()
        streams = self.__dict__.setdefault("_streams", {})
        where = node[0] if node else ""
        held = streams.pop(where, None)
        if held is not None and held[1].state is state.get("keyspaces"):
            stream, spaces, spec = held
        else:
            if held is not None:
                held[0].close()
            spaces = KeyspaceStates(state.setdefault("keyspaces", {}), log)
            stream = ReplicaStream(
                self._stream_socket(*(node[1:3] if node else ())),
                user=getattr(ep, "user", None), password=ep.password or None)
            spool = self._spool(db, node)
            try:
                got = stream.begin(spaces.replid, spaces.offset,
                                   sink=spool.write if spool else None)
                with guard:
                    self._started(db, state, spaces, got, spool, node, log)
                    save()
            except BaseException:
                stream.close()
                raise
            finally:
                if spool:
                    spool.close()
            spec = self._key_spec(self._client("src", decode=False))
        done = 0
        lands = self._landing_pace(where)
        try:
            groups = OwnGroups(self.MARK_KEY) \
                if self.__dict__.get("_mark_rung") else None
            for _ in stream.commands():
                with guard:
                    for argv in (groups.feed(stream.argv) if groups
                                 else (stream.argv,)):
                        spaces.route(stream.offset, argv, spec)
                    if not spaces.ready() or not lands.due(
                            stream.buffered()):
                        continue
                    batch = spaces.take()
                began = lands.pace.clock()
                for d, ops in batch.items():
                    tally = self._refresh_keys(d, ops)
                    with guard:
                        spaces.landed(d, tally)
                lands.landed(sum(len(ops) for ops in batch.values()), began)
                with guard:
                    for d in spaces.rejoin(stream.offset):
                        self._copy_again(d, log)
                        spaces.copied(d, {})
                    spaces.reached(stream.offset)
                    state["rounds"] = int(state.get("rounds") or 0) + 1
                    save()
                stream.ack(stream.offset)
                done += 1
                if stop() or (rounds is not None and done >= rounds):
                    break
        except BaseException:
            stream.close()
            raise
        streams[where] = (stream, spaces, spec)
        self.__dict__["_drained"] = not stream.buffered()
        return spaces

    def _landing_pace(self, where):
        """The `_LandingPace` of the stream of `where` (a shard's first
        slot, "" for one server), kept from one round to the next."""
        paces = self.__dict__.setdefault("_paces", {})
        if where not in paces:
            paces[where] = _LandingPace()
        return paces[where]

    def _started(self, db, state, spaces, got, spool, node, log):
        """The key spaces at the stream's start: those that join filled
        from the snapshot where it was kept and reads whole, by scan
        otherwise; then what the stream changed meanwhile read again."""
        reseed = spaces.started(got)
        listing = {d: self._dbsize("src", d)
                   for d in sorted(self._keyspace("src"), key=int)
                   if str(d) == str(db) or db is None}
        todo = spaces.boundary(listing, got["offset"]) + reseed
        seeded = self._seed_from(spool, todo, node, log) \
            if todo and spool is not None and got["kind"] == "full" else {}
        copied_at = state.get("copied_at")
        for d in todo:
            if d in seeded:
                # the snapshot is the source at this offset exactly
                spaces.copied(d, seeded[d])
            elif d not in reseed and copied_at and got["kind"] == "full" \
                    and self.live_facts()["idletime"]:
                # the move copied it already: only the keys written since
                # its start, before this offset, are read again
                spaces.copied(d, self._sweep_once(d, int(copied_at), log))
            else:
                self._copy_again(d, log)
                spaces.copied(d, self._refresh_keys(d, spaces.joined(d)))
        for d, ops in spaces.take().items():
            spaces.landed(d, self._refresh_keys(d, ops))
        log(spaces.said())

    def seed_way(self, db, node=None):
        """The `Choice` of how a full start fills the target, from what was
        measured: the source's dataset, the disk free where the snapshot
        is kept, this machine's free memory against the batches written,
        and a snapshot that did not read whole before (`decide.redis_seed`)."""
        import shutil

        from .. import decide
        src = self._node_client(node[1], node[2]) if node else \
            self._client("src")
        mem = src.info("memory")
        where = self.hop.report_dir(db if db is not None else "all")
        where.mkdir(parents=True, exist_ok=True)
        return decide.redis_seed({
            "snapshot bytes": int(mem.get("used_memory_dataset")
                                  or mem.get("used_memory") or 0),
            "local free": shutil.disk_usage(where).free,
            "local memory": local_memory(),
            "memory needs": 2 * pieces.BATCH_BYTES,
            "every change carried": all(
                len(self.hop.carries(f"db{d}")) == 3
                for d in ([db] if db is not None else self.databases())),
            "unread": self.__dict__.get("_unread")}), where

    def _spool(self, db, node):
        choice, where = self.seed_way(db, node)
        if choice.rung is None or choice.rung.name != "snapshot":
            return None
        mem = local_memory()
        return _Spool(where, choice.facts.get("local free"),
                      mem - pieces.BATCH_BYTES if mem else None)

    def _seed_from(self, spool, todo, node, log):
        """{db: what landed} of the key spaces `todo` filled from the kept
        snapshot: the target's keys the hop does not leave out removed
        first (on a shard, only those of its slots the snapshot lacks,
        after), each key restored in the source's own serialised form with
        its expiry and access history, in batches of `pieces.BATCH_BYTES`.
        A snapshot not kept whole, of a kind the reader does not know, or
        whose checksum does not match fills nothing: the scan does, and
        the next full start is decided knowing it."""
        if spool.why:
            self.__dict__["_unread"] = spool.why
            log(f"the source's snapshot was not kept ({spool.why}); the key"
                " spaces are read from the source instead")
            return {}
        want = {str(d) for d in todo}
        out = {d: dict.fromkeys(KEY_EVENTS, 0) for d in want}
        how = self._carry_plan(int(sorted(want)[0]))
        seen = set() if node else None
        if node is None:
            for d in sorted(want):
                self.empty_target(d)
        spool.f.seek(0)
        reader = RdbReader(spool.f, most=spool.memory)
        batch, size, cur = [], 0, None
        try:
            for e in reader.entries():
                d = str(e["db"])
                if d not in want or not self._raw_kept(d, [e["key"]]):
                    continue
                if batch and (d != cur or size >= pieces.BATCH_BYTES):
                    self._land(cur, batch, how, reader.version, out)
                    batch, size = [], 0
                cur = d
                batch.append(e)
                size += len(e["raw"]) + len(e["key"])
                if seen is not None:
                    seen.add(e["key"])
            if batch:
                self._land(cur, batch, how, reader.version, out)
        except RdbUnread as e:
            self.__dict__["_unread"] = str(e)
            log(f"the source's snapshot could not be read whole ({e}); the"
                " key spaces are read from the source instead")
            return {}
        if node is not None:
            for d in sorted(want):
                out[d]["removed"] += self._shard_leftovers(d, seen, node[3])
        log("filled from the source's snapshot: " + "; ".join(
            f"db{d} {out[d]['added'] + out[d]['written']:,} keys"
            for d in sorted(want)))
        return out

    def _land(self, d, batch, how, version, out):
        """One batch of the snapshot's keys on the target. A target that
        refuses the serialised form (another server's, or an older one) is
        given each key by its type's own writes from then on."""
        dst = self._keys("dst", d, decode=False)
        now = int(time.time() * 1000)
        if not self.__dict__.get("_plain_seed"):
            p = dst.pipeline(transaction=False)
            for e in batch:
                exp = e["expire"]
                if exp is not None and exp <= now:
                    p.delete(e["key"])
                    continue
                ttl = exp if exp is not None and how["abs"] else \
                    max(1, exp - now) if exp is not None else 0
                args = [e["key"], ttl, dump_payload(e["raw"], version),
                        "REPLACE"]
                if how["abs"]:
                    args += ["ABSTTL"] if exp is not None else []
                    if e["freq"] is not None:
                        args += ["FREQ", e["freq"]]
                    elif e["idle"] is not None:
                        args += ["IDLETIME", e["idle"]]
                p.execute_command("RESTORE", *args)
            got = p.execute(raise_on_error=False)
            bad = [g for g in got if isinstance(g, Exception)]
            if not bad:
                out[d]["written" if self._cluster("src") else "added"] += \
                    len(batch)
                return
            if not any("payload" in str(g).lower() for g in bad):
                raise SystemExit(f"db{d}: the target refused a key of the"
                                 f" snapshot: {str(bad[0])[:90]}")
            self.__dict__["_plain_seed"] = True
        self._land_plain(dst, batch, now)
        out[d]["written" if self._cluster("src") else "added"] += len(batch)

    @staticmethod
    def _land_plain(dst, batch, now):
        p = dst.pipeline(transaction=False)
        for e in batch:
            k, v, kind = e["key"], e["value"], e["kind"]
            if e["expire"] is not None and e["expire"] <= now:
                p.delete(k)
                continue
            if v is None:
                raise RdbUnread(f"the target takes no serialised form, and"
                                f" a {kind} is written only in it")
            p.delete(k)
            if kind == "string":
                p.set(k, v)
            elif kind == "list":
                for part in pieces.by_bytes(v, size=len):
                    p.rpush(k, *part)
            elif kind == "set":
                for part in pieces.by_bytes(v, size=len):
                    p.sadd(k, *part)
            elif kind == "zset":
                for part in pieces.by_bytes(v, size=lambda m: len(m[0]) + 8):
                    p.zadd(k, dict(part))
            else:
                for part in pieces.by_bytes(
                        v, size=lambda m: len(m[0]) + len(m[1])):
                    p.hset(k, mapping=dict(part))
            if e["expire"] is not None:
                p.pexpireat(k, e["expire"])
        p.execute()

    def _shard_leftovers(self, d, seen, slots):
        """Keys the target holds in a shard's slots that its snapshot did
        not: removed, as `empty_target` removes them on one server."""
        from redis.crc import key_slot
        dst = self._keys("dst", d, decode=False)
        gone = 0
        for keys, _ in self._walk("dst", d, False, self.COPY_BATCH):
            for k in self._raw_kept(d, keys):
                if k not in seen and key_slot(k) in slots:
                    gone += int(dst.delete(k) or 0)
        return gone

    def _no_touch(self, side="src"):
        """Whether a side reads keys without touching them (`CLIENT
        NO-TOUCH`, 7.2 and later), asked once."""
        seen = self.__dict__.setdefault("_untouched", {})
        if side not in seen:
            import redis
            try:
                self._client(side).execute_command("CLIENT", "NO-TOUCH",
                                                   "OFF")
                seen[side] = True
            except redis.RedisError:
                seen[side] = False
        return seen[side]

    def _sweep_rounds(self, db, state, save, log, stop, rounds=None):
        """Rounds of the idle-time sweep: each takes the source's clock,
        reads again every key whose idle time is no older than the last
        round's start (a second more, the clock's grain), and removes on
        the target every key the source no longer has. The round's start
        is saved only after both landed. Its reads leave the source's
        access history alone where the server offers that; where it does
        not, it says so. A key restored with an older idle time is not
        seen by it; the deep check that ends the tail is."""
        st = state.setdefault("sweep", {})
        if not self._no_touch() and not self.__dict__.get("_caveat"):
            self.__dict__["_caveat"] = True
            log(f"db{db}: the source cannot read a key without counting it"
                " as an access (no-touch reads came in 7.2), so each"
                " round's own reads make the next round read those keys"
                " again, and the source's idle times start from them")
        done = 0
        while not stop() and (rounds is None or done < rounds):
            started = int(self._client("src", db).time()[0])
            self._sweep_once(db, int(st.get(str(db), 0)) or None, log)
            st[str(db)] = started
            state["rounds"] = int(state.get("rounds") or 0) + 1
            save()
            done += 1
        return st

    def _sweep_once(self, db, since, log):
        """Every key of `db` the source touched at or after its clock read
        `since` read again (all where None), every key the target holds
        that the source has no longer removed; the tally of what changed.
        The batches are cut by bytes (`pieces.by_bytes`) as the copy's
        are, and read without touching where the source offers it."""
        src = self._keys("src", db, decode=False,
                         no_touch=self._no_touch())
        window = int(self._client("src", db).time()[0]) - since + 1 \
            if since else None
        keys = [k for page, _ in self._walk("src", db, False,
                                            self.COPY_BATCH)
                for k in self._raw_kept(db, page)]
        touched = []
        for batch in pieces.by_bytes(keys, size=lambda k: len(k) + 16):
            p = src.pipeline(transaction=False)
            for k in batch:
                p.object("idletime", k)
            idle = p.execute(raise_on_error=False)
            touched += [k for k, i in zip(batch, idle)
                        if window is None or not isinstance(i, int)
                        or i <= window]
        held = {k for page, _ in self._walk("dst", db, False,
                                            self.COPY_BATCH)
                for k in self._raw_kept(db, page)}
        ops = {k: "write" for k in touched}
        ops.update({k: "write" for k in held - set(keys)})
        tally = self._refresh_keys(db, ops)
        log(f"db{db}: swept {len(keys):,} keys, {len(ops):,} read again"
            f" ({tally['added']:,} added, {tally['removed']:,} removed)")
        return tally

    # --- the tail a run starts (`cli._tail`) -------------------------------

    # ---- the mark migkit's own writes carry (BAR-1, `marks`) ----------

    MARK_KEY = "migkit:origin"

    def _mark_command(self):
        """The command that opens a unit the tail writes, naming the
        batch (`NeutralMark.format_marker`)."""
        from .. import marks
        from ..barriers import NeutralMark
        return NeutralMark.format_marker("redis", batch=marks.tail_batch(self))

    def mark_facts(self, side, db):
        c = self._client(side)
        version = tuple(int(x) for x in
                        c.info("server")["redis_version"].split(".")[:2])
        return {"family": "redis", "version": version,
                "cluster": bool(c.info("cluster").get("cluster_enabled")),
                "can_create": True}

    def mark_unit(self, side, db, rung, batch, ops, run_id=None,
                  token=None):
        """`ops` - ("set", key, value) and ("del", key) - sent as one
        MULTI / EXEC; under `multi` the first command queued is the hash
        `migkit:origin` naming the batch. Nothing is sent until EXEC, so
        a failure before it leaves neither the marker nor the keys."""
        from .. import failpoint
        from ..barriers import NeutralMark
        pipe = self._client(
            side, db=int(db) if str(db).isdigit() else 0).pipeline(
            transaction=True)
        if rung == "multi":
            pipe.execute_command(*NeutralMark.format_marker(
                "redis", batch=batch, token=token, run_id=run_id))
        for kind, key, *rest in ops:
            failpoint.hit("mark.unit")
            if kind == "set":
                pipe.set(key, rest[0])
            else:
                pipe.delete(key)
        failpoint.hit("mark.unit.exec")
        pipe.execute()

    @classmethod
    def mark_groups(cls, lines):
        """[{"own": bool, "commands": [argv, ..]}] from the lines a
        monitor showed, [(client, command text)]: each MULTI .. EXEC of a
        client is one group, own where its first command is the marker
        hash; any other command is a group of its own, not marked."""
        open_, out = {}, []
        for client, line in lines:
            argv = line.split()
            verb = argv[0].upper() if argv else ""
            if verb == "MULTI":
                open_[client] = []
            elif verb == "EXEC" and client in open_:
                cmds = open_.pop(client)
                out.append({"own": bool(cmds) and len(cmds[0]) > 1
                            and cmds[0][0].upper() == "HSET"
                            and cmds[0][1] == cls.MARK_KEY,
                            "commands": cmds})
            elif client in open_:
                open_[client].append(argv)
            else:
                out.append({"own": False, "commands": [argv]})
        return out

    def mark_watch(self, side, work):
        """The lines a monitor of the side shows while `work()` runs,
        [(client, command text)]; ended by a command of its own."""
        import threading
        c, lines = self._client(side), []
        ready = threading.Event()

        def listen():
            with c.monitor() as m:
                ready.set()
                for got in m.listen():
                    if got["command"].startswith("PING migkit-end"):
                        return
                    lines.append((f"{got['client_address']}:"
                                  f"{got['client_port']}", got["command"]))
        t = threading.Thread(target=listen, daemon=True)
        t.start()
        if not ready.wait(15):
            raise SystemExit("the side's monitor did not open")
        time.sleep(0.2)
        try:
            work()
        finally:
            time.sleep(0.2)
            self._client(side).execute_command("PING", "migkit-end")
            t.join(15)
        return lines

    def mark_prove(self, side, db, rung):
        import secrets
        a, b = (f"migkit:probe:{secrets.token_hex(6)}:{x}" for x in "ab")
        try:
            lines = self.mark_watch(side, lambda: (
                self.mark_unit(side, db, rung, -1, [("set", a, "1")]),
                self.mark_unit(side, db, None, -1, [("set", b, "1")])))
            groups = self.mark_groups(lines)
            mine = [g for g in groups
                    if any(a in " ".join(c) for c in g["commands"])]
            theirs = [g for g in groups
                      if any(b in " ".join(c) for c in g["commands"])]
            if not mine or not theirs:
                return "the monitor showed no command of the probe"
            if not all(g["own"] for g in mine):
                return "the probe's unit did not open with its mark"
            if any(g["own"] for g in theirs):
                return "an application's write was shown as migkit's"
            return None
        except Exception as e:  # noqa: BLE001 - said, then the next rung
            return (str(e).strip().splitlines() or [type(e).__name__])[0]
        finally:
            self._client(side).delete(a, b)

    def mark_timed(self, side, db, rung, rounds):
        import secrets
        key, out = f"migkit:probe:{secrets.token_hex(6)}", []
        try:
            for i in range(rounds):
                ops = [("set", f"{key}:{k}", "1") for k in range(3)]
                at = time.perf_counter()
                self.mark_unit(side, db, rung, i, ops)
                out.append(time.perf_counter() - at)
        finally:
            self._client(side).delete(*[f"{key}:{k}" for k in range(3)])
        return out

    def mark_teardown(self, side, db, rung, go):
        """What the marks left on this side, taken away: the marker hash,
        whatever `rung` stands. [what is taken away, done where `go`]"""
        steps = [f"delete the key {self.MARK_KEY}"]
        if go:
            self._client(side).delete(self.MARK_KEY)
        return steps

    def tail_start(self, db, token_path):
        """Before the copy: the source's clock kept as the tail's
        `copied_at`, so a full start where no snapshot is kept reads again
        only the keys touched since."""
        from .. import tailctl
        sec = int(self._client("src").time()[0])
        tailctl.moved(token_path, tailctl.new_stream(self, None),
                      {"copied_at": sec})
        return False

    def tail_apply(self, db, go, token_path, log):
        """Follow the source past the copy (`follow_live`), round the loop
        every tail goes round (`tailctl.run_loop`): each look hands back
        within a beat, the stream's connection kept open between looks,
        the state saved only after what it covers landed; a lost
        connection goes on from the state last saved."""
        import json

        from .. import failpoint, marks, tailctl
        env = [tailctl.position(token_path)]
        state = dict((env[0] or {}).get("token") or {})
        # how what the tail writes is marked, taken once from the target
        # and chosen again only between two looks
        marks.tail_start(self, db, go, token_path, log)
        log("following the source as its replica, ctrl-c to stop"
            + ("" if go else " (count-only, add --go to copy)"))
        if not go:
            choice = self.live_way()
            log(f"db{db}: " + (choice.rung.said if choice.rung
                               else choice.reason))
            return
        last = [json.dumps(state, sort_keys=True, default=str)]

        def save():
            text = json.dumps(state, sort_keys=True, default=str)
            if text == last[0]:
                return
            failpoint.hit("tail.applied")
            env[0] = tailctl.moved(
                token_path, env[0] or tailctl.new_stream(self, None),
                json.loads(text))
            last[0] = text
            failpoint.hit("tail.saved")

        def look(at):
            marks.tail_round(self, db, token_path, log)
            before = _rounds_of(state)
            ends = time.monotonic() + tailctl.BEAT_EVERY
            self.follow_live(db, state, save, log,
                             stop=lambda: time.monotonic() > ends, rounds=1
                             if "sweep" in state else None)
            went = _rounds_of(state) - before
            return state, went, self.__dict__.pop("_drained", True)

        def lost(e, at):
            self.close_streams()
            state.clear()
            state.update((tailctl.position(token_path) or {}).get("token")
                         or {})
            return state
        try:
            tailctl.run_loop(token_path.parent, db, look, log, "rounds",
                             hop=self.hop, go=go, token=state, lost=lost,
                             secrets=(self.hop.source.password,
                                      self.hop.target.password))
        finally:
            self.close_streams()

    # --- two sites both written: each one's changes off its own stream ----

    def site_changes(self, side, stop, log, rounds=None):
        """The writes site `side` takes from now until `stop()` (or
        `rounds` commands), read off its own replica stream continued from
        its current offset, as the change records `twoway.redis_two_site`
        reads: each with the site's clock at its arrival. A value's
        record carries the key as the site holds it at the end, in its
        serialised form; a key gone by then is a removal. Returns
        (records, held: the writes no record can say, e.g. a flush)."""
        client = self._client(side)
        rep = client.info("replication")
        sec, usec = client.time()
        changes = SiteChanges(self._key_spec(self._client(side,
                                                          decode=False)),
                              int(sec) * 1000 + int(usec) // 1000,
                              time.monotonic())
        ep = self.hop.source if side == "src" else self.hop.target
        stream = ReplicaStream(self._stream_socket(side=side),
                               user=getattr(ep, "user", None),
                               password=ep.password or None)
        done = 0
        try:
            stream.begin(rep.get("master_replid"),
                         int(rep.get("master_repl_offset") or 0))
            for argv in stream.commands():
                changes.route(stream.offset, argv)
                if not stream.buffered():
                    stream.ack(stream.offset)
                done += 1
                if stop() or (rounds is not None and done >= rounds):
                    break
        finally:
            stream.close()
        changes.valued(lambda d, keys: self._dumps(side, d, keys))
        for h in changes.held:
            log(f"{side} db{h['db']} at offset {h['offset']:,}: {h['why']};"
                " the window cannot be made alike by key and is held")
        return changes.records, changes.held

    def _dumps(self, side, d, keys):
        c = self._keys(side, d, decode=False, no_touch=self._no_touch(side))
        p = c.pipeline(transaction=False)
        for k in keys:
            p.dump(k)
        return dict(zip(keys, p.execute()))

    def site_skew_ms(self, tries=3):
        """The two sites' clocks apart, with the reading lag, in ms: the
        least of `tries` readings, each the gap between the clocks at the
        midpoint of their reads plus half of both round trips."""
        best = None
        for _ in range(tries):
            seen = []
            for side in ("src", "dst"):
                c = self._client(side)
                a = time.time()
                sec, usec = c.time()
                b = time.time()
                seen.append((int(sec) * 1000 + int(usec) / 1000
                             - (a + b) * 500, (b - a) * 1000))
            (oa, ra), (ob, rb) = seen
            got = abs(oa - ob) + (ra + rb) / 2
            best = got if best is None else min(best, got)
        return int(best) + 1

#: what the tail counts a key's change as (`key_event`)
KEY_EVENTS = ("added", "removed", "renamed", "ttl", "retyped", "written")

#: the commands that change only a key's expiry
_TTL_COMMANDS = frozenset((b"expire", b"pexpire", b"expireat", b"pexpireat",
                           b"persist"))
_RENAMES = frozenset((b"rename", b"renamenx"))


def key_event(op, before, now):
    """What one change was, from the stream's word for it (`op`) and the
    key's type on the target before and on the source after."""
    if before == "none" and now == "none":
        return "written"
    if before == "none":
        return "added"
    if now == "none":
        return "removed"
    if before != now:
        return "retyped"
    return {"rename": "renamed", "ttl": "ttl"}.get(op, "written")


class LiveRefused(Exception):
    """The source would not give the stream as decided; `facts` is what it
    showed, for the decision taken again."""

    def __init__(self, why, **facts):
        super().__init__(why)
        self.facts = facts


class _NoCheckpoint(dict):
    """A key space's copy inside the tail: its seam is the tail's offset,
    so a stop copies it again rather than resuming a cursor."""

    def save(self):
        return None


class ReplicaStream:
    """migkit as a replica of a Redis source, over one connection: the
    handshake a replica makes, the snapshot the source sends where it
    cannot continue, then every write command with the offset it ends at.
    `offset` is the source's offset of the last byte read; the source's
    answer to a saved offset (+CONTINUE) is the proof nothing between was
    lost."""

    def __init__(self, sock, user=None, password=None):
        self.sock, self.user, self.password = sock, user, password
        self.buf = bytearray()
        self.offset = 0
        self.read = 0
        self.argv = []

    def _fill(self):
        try:
            chunk = self.sock.recv(1 << 16)
        except OSError as e:
            raise LiveRefused(f"the stream broke: {e}", continued=True) \
                from e
        if not chunk:
            raise LiveRefused("the source closed the stream", continued=True)
        self.buf += chunk

    def _line(self):
        while True:
            i = self.buf.find(b"\r\n")
            if i >= 0:
                out = bytes(self.buf[:i])
                del self.buf[:i + 2]
                self.read += i + 2
                return out
            self._fill()

    def _exact(self, n):
        while len(self.buf) < n:
            self._fill()
        out = bytes(self.buf[:n])
        del self.buf[:n]
        self.read += n
        return out

    def _send(self, *argv):
        parts = [b"*%d\r\n" % len(argv)]
        for a in argv:
            a = a if isinstance(a, bytes) else str(a).encode()
            parts.append(b"$%d\r\n%s\r\n" % (len(a), a))
        self.sock.sendall(b"".join(parts))

    def _ask(self, *argv):
        self._send(*argv)
        got = self._line().lstrip(b"\n")
        if got.startswith(b"-"):
            text = got[1:].decode("latin-1")
            raise LiveRefused(f"the source refused {argv[0]}: {text}",
                              **{"replica grant": False})
        return got

    def begin(self, replid=None, offset=None, sink=None):
        """The handshake, and {"kind": "continue"|"full", "replid",
        "offset"}: continued from the byte after `offset` of `replid`, or
        a full snapshot (its bytes to `sink`) as of the offset it names."""
        if self.password:
            self._ask(*(("AUTH", self.user, self.password) if self.user
                        else ("AUTH", self.password)))
        self._ask("REPLCONF", "listening-port",
                  self.sock.getsockname()[1])
        self._ask("REPLCONF", "capa", "eof", "capa", "psync2")
        want = (replid, offset + 1) if replid and offset is not None \
            else ("?", -1)
        got = self._ask("PSYNC", *want).decode()
        if got.startswith("+CONTINUE"):
            rest = got.split()
            self.offset = int(offset)
            return {"kind": "continue",
                    "replid": rest[1] if len(rest) > 1 else replid,
                    "offset": self.offset}
        if not got.startswith("+FULLRESYNC"):
            raise LiveRefused(f"the source answered the stream with {got}",
                              **{"replica grant": False})
        _, rid, at = got.split()
        self.offset = int(at)
        self._snapshot(sink or (lambda chunk: None))
        return {"kind": "full", "replid": rid, "offset": self.offset}

    def _snapshot(self, sink):
        head = b""
        while not head:
            head = self._line().strip(b"\n")
        if head.startswith(b"$EOF:"):
            mark = head[5:]
            while True:
                i = self.buf.find(mark)
                if i >= 0:
                    sink(bytes(self.buf[:i]))
                    del self.buf[:i + len(mark)]
                    return
                keep = max(0, len(self.buf) - len(mark))
                sink(bytes(self.buf[:keep]))
                del self.buf[:keep]
                self._fill()
        left = int(head[1:])
        while left:
            if not self.buf:
                self._fill()
            take = min(left, len(self.buf))
            sink(bytes(self.buf[:take]))
            del self.buf[:take]
            left -= take

    def commands(self):
        """Each write command as `argv`, `offset` moved past its bytes; the
        source's request for an acknowledgement answered with the offset
        last saved."""
        while True:
            start = self.read
            head = self._line()
            if not head.startswith(b"*"):
                raise LiveRefused(f"the stream carried {head[:20]!r} where"
                                  " a command was due", continued=False)
            argv = []
            for _ in range(int(head[1:])):
                size = self._line()
                argv.append(self._exact(int(size[1:]) + 2)[:-2])
            self.offset += self.read - start
            self.argv = argv
            if argv[:2] and argv[0].lower() == b"replconf" \
                    and argv[1].lower() == b"getack":
                continue
            yield argv

    def buffered(self):
        return bool(self.buf)

    def ack(self, offset):
        self._send("REPLCONF", "ACK", offset)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


class _LandingPace:
    """When a replica stream's pending keys land on the target. Always at
    a command boundary and outside a transaction (the caller's), and then
    where the stream has nothing more buffered, or where the stream was
    read for as long as the last landing took - the target's round trip
    and its writes, measured - so under writes without pause at most half
    the time goes to landing and a batch holds what arrived meanwhile;
    never longer than `tailctl.IDLE_MOST` (`tailctl.Pace` keeps the clock
    and the bounds). A source written without pause still lands, saves
    its offset and acknowledges it."""

    def __init__(self, clock=None):
        from .. import tailctl
        self.pace = tailctl.Pace(0.0, tailctl.IDLE_MOST, clock=clock)
        self.took = 0.0

    def due(self, buffered):
        if not buffered:
            return True
        read = self.pace.clock() - self.pace.at
        return read >= min(self.pace.most, max(self.took, self.pace.least))

    def landed(self, keys, began):
        """A landing of `keys` begun at `began` ended: timed, and the
        stream's reading counted from now."""
        self.took = max(self.pace.clock() - began, 0.0)
        self.pace.saw(keys)


class KeyspaceStates:
    """A Redis tail's key spaces (its databases), each in a state of its
    own, the way the SQL tail keeps its tables (`tailtables`) and Kafka's
    its topics (`topicstates`):

        joining   added to the running tail: copied, every key the stream
                  changed after its seam (the offset it joined at) read
                  again once the copy is done, live from there
        live      each change read again from the source as it arrives
        left      out of the scope: the keys changed since kept by name, so
                  that its return reads only those again
        parked    flushed or swapped on the source, or the stream could not
                  continue: copied again from the next offset

    `state` is the saved dict; nothing in it moves before the keys it
    covers landed. Each space counts the keys added, removed, renamed,
    retyped and given another expiry while followed."""

    def __init__(self, state, log):
        self.state = state
        self.log = log
        self.spaces = state.setdefault("spaces", {})
        # the database the stream last selected: a stream continued from a
        # saved offset does not say it again
        self.db = state.get("db", "0")
        self.pending = {}
        self.multi = False
        self.held = state.setdefault("held", {})

    @property
    def replid(self):
        return self.state.get("replid")

    @property
    def offset(self):
        return self.state.get("offset")

    def started(self, got):
        """The stream's answer at its start: a continue keeps every space;
        a full snapshot on a saved offset means a write may be lost, so
        every space followed is parked and copied again from the new
        offset. Returns the spaces to copy again."""
        lost = got["kind"] == "full" and self.offset is not None
        self.state.update(replid=got["replid"], offset=got["offset"])
        again = []
        for d, st in sorted(self.spaces.items()):
            if lost and st["state"] in ("live", "joining", "parked"):
                self._enter(d, "joining", got["offset"], "the source could"
                            " not continue from the offset saved")
                again.append(d)
            elif lost and st["state"] == "left":
                st["reseed"] = True
        return again

    def _enter(self, d, state, at, why):
        st = self.spaces.setdefault(d, {"counts": dict.fromkeys(KEY_EVENTS,
                                                                0)})
        st.update(state=state, seam=at, why=why)
        st["touched"] = {} if state in ("joining", "left") else None
        self.log(f"db{d}: {state} at offset {at:,} - {why}")
        return st

    def boundary(self, listing, at):
        """The scope's spaces now (`listing`, {db: keys it holds}) against
        those followed: a new one joins at `at`, one gone from the scope
        leaves, one back with fewer keys changed than it holds reads only
        those again. Returns the spaces to copy."""
        copy = []
        here = {str(d): n for d, n in listing.items()}
        for d, n in sorted(here.items()):
            st = self.spaces.get(d)
            if st is None or st["state"] == "parked":
                self._enter(d, "joining", at, "added to the tail")
                copy.append(d)
            elif st["state"] == "left":
                touched = st.get("touched") or {}
                if st.pop("reseed", False) or len(touched) > n:
                    self._enter(d, "joining", at, "back in the scope; its"
                                " changes since outnumber its keys")
                    copy.append(d)
                else:
                    self.pending.setdefault(d, {}).update(touched)
                    self._enter(d, "live", at, f"back in the scope; the"
                                f" {len(touched):,} keys changed since read"
                                " again")
        for d, st in sorted(self.spaces.items()):
            if d not in here and st["state"] != "left":
                self.pending.pop(d, None)
                self._enter(d, "left", at, "out of the scope")["keys"] = \
                    self.held.get(d, 0)
            if d in here:
                self.held[d] = here[d]
        return copy

    def rejoin(self, at):
        """The parked spaces, joining again at `at`: to be copied."""
        out = self.parked()
        for d in out:
            self._enter(d, "joining", at, "copied again after what parked"
                        " it")
        return out

    def joined(self, d):
        """The keys a joining space's stream changed while it was copied."""
        return dict(self.spaces[d].get("touched") or {})

    def copied(self, d, tally):
        st = self.spaces[d]
        self._count(st, tally)
        self._enter(d, "live", st["seam"], "copied, its changes since read"
                    " again")

    def _count(self, st, tally):
        for k, n in tally.items():
            st["counts"][k] = st["counts"].get(k, 0) + n

    def _touch(self, d, key, op):
        st = self.spaces.get(d)
        if st is None or st["state"] == "parked":
            return
        if st["state"] == "live":
            self.pending.setdefault(d, {})[key] = op
        elif st.get("reseed"):
            return
        else:
            st["touched"][key.decode("latin-1") if isinstance(
                key, bytes) else key] = op
            if st["state"] == "left" and len(st["touched"]) > st.get(
                    "keys", 0):
                # more changed than it held: its return copies it whole
                st["touched"], st["reseed"] = {}, True

    def route(self, at, argv, spec):
        """One write command of the stream, ending at offset `at`."""
        name = argv[0].lower()
        if name == b"select":
            self.db = argv[1].decode()
        elif name == b"multi":
            self.multi = True
        elif name == b"exec":
            self.multi = False
        elif name in (b"flushdb", b"flushall", b"swapdb"):
            hit = ([self.db] if name == b"flushdb" else
                   [a.decode() for a in argv[1:3]] if name == b"swapdb"
                   else list(self.spaces))
            for d in hit:
                if d in self.spaces and self.spaces[d]["state"] != "left":
                    self.pending.pop(d, None)
                    self._enter(d, "parked", at, f"{name.decode().upper()}"
                                " on the source")
                elif d in self.spaces:
                    self.spaces[d]["reseed"] = True
        elif name in (b"move", b"copy"):
            keys = spec(argv)
            to = self.db
            for i, a in enumerate(argv):
                if (name == b"move" and i == 2) or (
                        i and argv[i - 1].lower() == b"db"):
                    to = a.decode()
            for k in keys:
                self._touch(self.db, k, "write")
                self._touch(to, k, "write")
        elif name not in (b"ping", b"replconf"):
            op = ("rename" if name in _RENAMES else
                  "ttl" if name in _TTL_COMMANDS else "write")
            keys = spec(argv)
            if keys is None:
                self._enter(self.db, "parked", at, f"{name.decode().upper()}"
                            " changes keys the source does not name")
                self.pending.pop(self.db, None)
            for k in keys or ():
                self._touch(self.db, k, op)

    def ready(self):
        """Not inside a transaction: its writes land together."""
        return not self.multi

    def take(self):
        out, self.pending = self.pending, {}
        return out

    def landed(self, d, tally):
        self._count(self.spaces[d], tally)

    def reached(self, at):
        self.state["offset"] = at
        self.state["db"] = self.db

    def parked(self):
        return sorted(d for d, st in self.spaces.items()
                      if st["state"] == "parked")

    def said(self):
        return "; ".join(
            f"db{d} {st['state']} from offset {st['seam']:,}: "
            + ", ".join(f"{n:,} {k}" for k, n in sorted(st["counts"].items())
                        if n) for d, st in sorted(self.spaces.items()))


class OwnGroups:
    """A replica stream's commands with the transactions migkit wrote
    left out: each MULTI .. EXEC is held until its EXEC, and let through
    whole (`feed`) unless its first command is the mark, the hash `key`
    (`RedisEngine.mark_unit`). What is outside a transaction passes as it
    is. A transaction the stream ends in the middle of is held until the
    next call that finishes it."""

    def __init__(self, key):
        self.key, self.open, self.opened = key, None, None

    @staticmethod
    def _text(word):
        return word.decode("latin-1") if isinstance(word, bytes) else str(word)

    def feed(self, argv):
        verb = self._text(argv[0]).upper() if argv else ""
        if verb == "MULTI" and self.open is None:
            self.open, self.opened = [], argv
            return []
        if self.open is None:
            return [argv]
        if verb != "EXEC":
            self.open.append(argv)
            return []
        held, self.open = self.open, None
        first = held[0] if held else None
        if first and self._text(first[0]).upper() == "HSET" \
                and len(first) > 1 and self._text(first[1]) == self.key:
            return []
        return [self.opened, *held, argv]


def _rounds_of(state):
    return int(state.get("rounds") or 0) + sum(
        int(s.get("rounds") or 0) for s in (state.get("shards") or {})
        .values())


def local_memory():
    """Bytes of memory free on this machine, or None where it cannot be
    read (a choice resting on it then does not hold)."""
    try:
        import psutil
        return int(psutil.virtual_memory().available)
    except ImportError:
        pass
    try:
        return os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (ValueError, OSError, AttributeError):
        return None


class _Spool:
    """The snapshot the source sends, kept on disk as it arrives, up to the
    room measured (`most`); past it, or on a write that fails, the rest is
    let go and `why` says so - the stream still reads through it."""

    def __init__(self, where, most, memory):
        import tempfile
        self.f = tempfile.TemporaryFile(dir=where)
        self.most, self.memory = most, memory
        self.size, self.why = 0, None

    def write(self, chunk):
        if self.why:
            return
        if self.most is not None and self.size + len(chunk) > self.most:
            self.why = (f"it is past the {self.most:,} bytes free where it"
                        " is kept")
            return
        try:
            self.f.write(chunk)
        except OSError as e:
            self.why = f"keeping it failed: {e}"
            return
        self.size += len(chunk)

    def close(self):
        self.f.close()


class RdbUnread(Exception):
    """A snapshot the reader cannot read whole: the scan takes over."""


def _crc64_table():
    out = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ 0x95AC9329AC4BC9B5 if c & 1 else c >> 1
        out.append(c)
    return out


_CRC64 = _crc64_table()


def crc64(data, crc=0):
    """The checksum Redis ends a snapshot and a serialised key with (Jones,
    reflected, no final xor)."""
    t = _CRC64
    for b in data:
        crc = t[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc


def dump_payload(raw, version):
    """A key's serialised form, as `DUMP` gives it: its type and value as
    the snapshot holds them, the snapshot's version, the checksum."""
    body = raw + int(version).to_bytes(2, "little")
    return body + crc64(body).to_bytes(8, "little")


def lzf_decompress(data, size):
    out = bytearray()
    i, n = 0, len(data)
    while i < n:
        ctrl = data[i]
        i += 1
        if ctrl < 32:
            out += data[i:i + ctrl + 1]
            i += ctrl + 1
            continue
        length = ctrl >> 5
        ref = len(out) - ((ctrl & 0x1F) << 8) - 1
        if length == 7:
            length += data[i]
            i += 1
        ref -= data[i]
        i += 1
        if ref < 0:
            raise RdbUnread("a compressed value points before its start")
        for _ in range(length + 2):
            out.append(out[ref])
            ref += 1
    if len(out) != size:
        raise RdbUnread(f"a compressed value came out {len(out):,} bytes,"
                        f" not {size:,}")
    return bytes(out)


#: the snapshot's value types, by kind (a type not here is read by scan)
_RDB_KINDS = {0: "string", 1: "list", 2: "set", 3: "zset", 4: "hash",
              5: "zset", 7: "module", 10: "list", 11: "set", 12: "zset",
              13: "hash", 14: "list", 15: "stream", 16: "hash", 17: "zset",
              18: "list", 19: "stream", 20: "set", 21: "stream"}


def _signed(v, bits):
    return v - (1 << bits) if v >= 1 << (bits - 1) else v


def _ziplist(blob):
    out, i = [], 10
    while blob[i] != 0xFF:
        i += 5 if blob[i] == 0xFE else 1
        e = blob[i]
        top = e >> 6
        if top < 3:
            if top == 0:
                n, i = e & 0x3F, i + 1
            elif top == 1:
                n, i = ((e & 0x3F) << 8) | blob[i + 1], i + 2
            else:
                n, i = int.from_bytes(blob[i + 1:i + 5], "big"), i + 5
            out.append(bytes(blob[i:i + n]))
            i += n
            continue
        width = {0xC0: 2, 0xD0: 4, 0xE0: 8, 0xF0: 3, 0xFE: 1}.get(e)
        if width is None:
            out.append(str((e & 0x0F) - 1).encode())
            i += 1
            continue
        out.append(str(int.from_bytes(blob[i + 1:i + 1 + width], "little",
                                      signed=True)).encode())
        i += 1 + width
    return out


def _listpack(blob):
    out, i = [], 6
    while blob[i] != 0xFF:
        e, start = blob[i], i
        if e & 0x80 == 0:
            out.append(str(e & 0x7F).encode())
            i += 1
        elif e & 0xC0 == 0x80:
            n = e & 0x3F
            out.append(bytes(blob[i + 1:i + 1 + n]))
            i += 1 + n
        elif e & 0xE0 == 0xC0:
            out.append(str(_signed(((e & 0x1F) << 8) | blob[i + 1], 13))
                       .encode())
            i += 2
        elif e & 0xF0 == 0xE0:
            n = ((e & 0x0F) << 8) | blob[i + 1]
            out.append(bytes(blob[i + 2:i + 2 + n]))
            i += 2 + n
        elif e == 0xF0:
            n = int.from_bytes(blob[i + 1:i + 5], "little")
            out.append(bytes(blob[i + 5:i + 5 + n]))
            i += 5 + n
        elif e in (0xF1, 0xF2, 0xF3, 0xF4):
            w = {0xF1: 2, 0xF2: 3, 0xF3: 4, 0xF4: 8}[e]
            out.append(str(int.from_bytes(blob[i + 1:i + 1 + w], "little",
                                          signed=True)).encode())
            i += 1 + w
        else:
            raise RdbUnread(f"a packed entry of an unknown kind {e:#x}")
        n = i - start
        i += 1 if n < 128 else 2 if n < 16384 else 3 if n < 2097152 \
            else 4 if n < 268435456 else 5
    return out


def _intset(blob):
    w = int.from_bytes(blob[:4], "little")
    n = int.from_bytes(blob[4:8], "little")
    return [str(int.from_bytes(blob[8 + j * w:8 + (j + 1) * w], "little",
                               signed=True)).encode() for j in range(n)]


def _pairs(flat):
    return list(zip(flat[0::2], flat[1::2]))


class RdbReader:
    """The keys of a Redis snapshot, read in one pass from a file: each
    {db, key, kind, expire (ms, or None), idle, freq, value, raw}. `raw`
    is the key's type and value as the snapshot holds them (the body of
    its serialised form, `dump_payload`); `value` the value itself for a
    string, list, set, sorted set and hash ([(member, score)] and [(field,
    value)] for the last two), None for a stream or a module's type,
    which are carried only serialised. The checksum at the end is held to
    what was read; a kind the reader does not know, a value past `most`
    bytes (this machine's memory measured) or a checksum that does not
    match is `RdbUnread`."""

    def __init__(self, f, most=None):
        self.f, self.most = f, most
        self.crc, self.tape, self.version = 0, None, None

    def _take(self, n):
        if self.most is not None and n > self.most:
            raise RdbUnread(f"a value of {n:,} bytes is past the"
                            f" {self.most:,} this machine has free")
        b = self.f.read(n)
        if len(b) != n:
            raise RdbUnread("the snapshot ends inside a value")
        self.crc = crc64(b, self.crc)
        if self.tape is not None:
            self.tape += b
            if self.most is not None and len(self.tape) > self.most:
                raise RdbUnread(f"a value past the {self.most:,} bytes this"
                                " machine has free")
        return b

    def _len(self):
        b = self._take(1)[0]
        kind = b >> 6
        if kind == 0:
            return b & 0x3F, False
        if kind == 1:
            return ((b & 0x3F) << 8) | self._take(1)[0], False
        if kind == 2:
            if b in (0x80, 0x81):
                return int.from_bytes(self._take(4 if b == 0x80 else 8),
                                      "big"), False
            raise RdbUnread(f"a length of an unknown form {b:#x}")
        return b & 0x3F, True

    def length(self):
        n, special = self._len()
        if special:
            raise RdbUnread("an encoded value where a length was due")
        return n

    def string(self):
        n, special = self._len()
        if not special:
            return self._take(n)
        if n in (0, 1, 2):
            return str(int.from_bytes(self._take(1 << n), "little",
                                      signed=True)).encode()
        if n == 3:
            packed, size = self.length(), self.length()
            return lzf_decompress(self._take(packed), size)
        raise RdbUnread(f"a string of an unknown form {n}")

    def _score(self):
        n = self._take(1)[0]
        if n in (253, 254, 255):
            return {253: float("nan"), 254: float("inf"),
                    255: float("-inf")}[n]
        return float(self._take(n))

    def _value(self, t):
        if t == 0:
            return self.string()
        if t in (1, 2):
            return [self.string() for _ in range(self.length())]
        if t == 3:
            return [(self.string(), self._score())
                    for _ in range(self.length())]
        if t == 4:
            return [(self.string(), self.string())
                    for _ in range(self.length())]
        if t == 5:
            out = []
            for _ in range(self.length()):
                m = self.string()
                out.append((m, struct.unpack("<d", self._take(8))[0]))
            return out
        if t == 7:
            self.length()
            self._module_ops()
            return None
        if t in (10, 12, 13):
            flat = _ziplist(self.string())
            return flat if t == 10 else _pairs(flat) if t == 13 else [
                (m, float(s)) for m, s in _pairs(flat)]
        if t == 11:
            return _intset(self.string())
        if t == 14:
            return [x for _ in range(self.length())
                    for x in _ziplist(self.string())]
        if t == 18:
            out = []
            for _ in range(self.length()):
                plain = self.length() == 1
                blob = self.string()
                out += [blob] if plain else _listpack(blob)
            return out
        if t in (16, 17, 20):
            flat = _listpack(self.string())
            return flat if t == 20 else _pairs(flat) if t == 16 else [
                (m, float(s)) for m, s in _pairs(flat)]
        if t in (15, 19, 21):
            self._stream(t)
            return None
        raise RdbUnread(f"a value of type {t}, which this reader does not"
                        " know")

    def _module_ops(self):
        while True:
            op = self.length()
            if op == 0:
                return
            if op in (1, 2):
                self.length()
            elif op in (3, 4):
                self._take(4 if op == 3 else 8)
            elif op == 5:
                self.string()
            else:
                raise RdbUnread(f"a module's value of an unknown part {op}")

    def _stream(self, t):
        for _ in range(self.length()):
            self.string()
            self.string()
        for _ in range(3 + (5 if t >= 19 else 0)):
            self.length()
        for _ in range(self.length()):
            self.string()
            self.length()
            self.length()
            if t >= 19:
                self.length()
            for _ in range(self.length()):
                self._take(24)
                self.length()
            for _ in range(self.length()):
                self.string()
                self._take(16 if t >= 21 else 8)
                for _ in range(self.length()):
                    self._take(16)

    def entries(self):
        head = self._take(9)
        if head[:5] != b"REDIS" or not head[5:].isdigit():
            raise RdbUnread("it does not begin as a snapshot")
        self.version = int(head[5:])
        db, expire, idle, freq = 0, None, None, None
        while True:
            op = self._take(1)[0]
            if op == 0xFF:
                want = self.crc
                got = int.from_bytes(self.f.read(8), "little") \
                    if self.version >= 5 else 0
                if got and got != want:
                    raise RdbUnread("its checksum does not match what was"
                                    " read")
                return
            if op == 0xFE:
                db = self.length()
            elif op == 0xFD:
                expire = int.from_bytes(self._take(4), "little") * 1000
            elif op == 0xFC:
                expire = int.from_bytes(self._take(8), "little")
            elif op == 0xFB:
                self.length()
                self.length()
            elif op == 0xFA:
                self.string()
                self.string()
            elif op == 0xF9:
                freq = self._take(1)[0]
            elif op == 0xF8:
                idle = self.length()
            elif op == 0xF7:
                self.length()
                self.length()
                self.length()
                self._module_ops()
            elif op == 0xF5:
                # the server's functions travel with it, not with keys
                self.string()
            elif op == 0xF4:
                self.length()
                self.length()
                self.length()
            elif op in _RDB_KINDS:
                key = self.string()
                self.tape = bytearray([op])
                try:
                    value = self._value(op)
                    raw = bytes(self.tape)
                finally:
                    self.tape = None
                yield {"db": db, "key": key, "kind": _RDB_KINDS[op],
                       "expire": expire, "idle": idle, "freq": freq,
                       "value": value, "raw": raw}
                expire, idle, freq = None, None, None
            else:
                raise RdbUnread(f"a part of type {op}, which this reader"
                                " does not know")


#: a counter's writes and the sign of their step (`SiteChanges`)
_COUNTER_STEPS = {b"incr": 1, b"decr": -1, b"incrby": 1, b"decrby": -1}


class SiteChanges:
    """One site's writes as the two-site rules read them (`twoway.
    redis_two_site`): a counter's step, a set's members added and removed,
    a removal, a list's or a stream's write (whose order no rule may
    overrule, by the command's own category), any other write a value.
    Each record has the site's offset and its clock at the write's
    arrival (`base_ms` read at `t0`, moved on by this machine's clock). A
    write whose keys the site does not name, or a flush, is `held`."""

    def __init__(self, spec, base_ms, t0):
        self.spec, self.base, self.t0 = spec, base_ms, t0
        self.db, self.records, self.held = "0", [], []

    def clock(self):
        return self.base + int((time.monotonic() - self.t0) * 1000)

    def route(self, at, argv):
        name = argv[0].lower()
        if name == b"select":
            self.db = argv[1].decode()
            return
        if name in (b"multi", b"exec", b"ping", b"replconf"):
            return
        base = {"db": self.db, "offset": at, "time": self.clock()}
        if name in (b"flushdb", b"flushall", b"swapdb"):
            self.held.append(dict(base, why=f"{name.decode().upper()}"
                                  " changes keys it does not name"))
            return
        keys = self.spec(argv)
        if keys is None:
            self.held.append(dict(base, why=f"{name.decode().upper()}"
                                  " changes keys the site does not name"))
            return
        if name in _COUNTER_STEPS and len(keys) == 1:
            try:
                step = int(argv[2]) if len(argv) > 2 else 1
            except ValueError:
                step = None
            if step is not None:
                self.records.append(dict(base, key=keys[0], kind="counter",
                                         delta=_COUNTER_STEPS[name] * step))
                return
        if name in (b"sadd", b"srem") and keys:
            members = set(argv[2:])
            self.records.append(dict(
                base, key=keys[0], kind="set",
                added=members if name == b"sadd" else set(),
                gone=members if name == b"srem" else set()))
            return
        if name in (b"del", b"unlink"):
            self.records += [dict(base, key=k, kind="removed") for k in keys]
            return
        cats = getattr(self.spec, "categories", lambda n: set())(name)
        kind = "list" if "@list" in cats else "stream" \
            if "@stream" in cats else "value"
        self.records += [dict(base, key=k, kind=kind) for k in keys]

    def valued(self, dumps):
        """Each value's record given the key as the site holds it now
        (`dumps(db, keys) -> {key: serialised form or None}`); a key gone
        by then ends with a removal."""
        want = {}
        for r in self.records:
            if r["kind"] in ("value", "list", "stream"):
                want.setdefault(r["db"], set()).add(r["key"])
        for d, keys in sorted(want.items()):
            got = dumps(d, sorted(keys))
            last = {}
            for r in self.records:
                if r["db"] == d and r["key"] in keys:
                    r["value"] = got.get(r["key"])
                    last[r["key"]] = r
            for k, r in sorted(last.items()):
                if got.get(k) is None:
                    self.records.append(dict(db=d, key=k, kind="removed",
                                             offset=r["offset"],
                                             time=r["time"]))
