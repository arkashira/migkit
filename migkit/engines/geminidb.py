"""Huawei GeminiDB as a side of a pair (backlog P0-CLOUDS): its Cassandra,
Redis and MongoDB APIs, each the engine whose API it speaks, with what
GeminiDB does differently.

GeminiDB keeps every API's data in its own storage behind the API: it
answers the API's commands, not its servers' inner workings.

* Cassandra API (`geminidb-cassandra`): read and written as Cassandra is,
  compared by value and by each cell's write time and time to live. It
  keeps no change log a client can read - neither Cassandra's commit log
  nor a CDC table - so it is not followed: moved while nothing writes,
  and proved by check.
* Redis API (`geminidb-redis`): copied key by key with each key's time to
  live, as Redis is when no replica can be made. A GeminiDB instance is
  never a replica, nor serves one - it takes no REPLICAOF and answers no
  PSYNC - so nothing replicates it and it is followed by nothing; each key
  written while the move runs is found by check.
* MongoDB API (`geminidb-mongo`): MongoDB's side whole - reads, writes,
  digests, and its change streams where the instance serves them (the
  follow asks, and says so where it does not).

Which is GeminiDB is read off what it says of itself where it says it;
none of it has been run against a GeminiDB instance here - the wire each
speaks has, through its own engine's tests.
"""
from .base import Engine, NotHere
from .cassandra import CassandraEngine
from .mongodb import MongoEngine
from .redis import RedisEngine


class GeminiCassandraEngine(CassandraEngine):
    #: each API's values are its engine's in every way `canon` reads
    #: them; the pair a same-engine hop is carried by is made of GeminiDB
    #: sides
    PAIR_ENGINE = "geminidb-cassandra"
    _sessions = {}

    def _logged(self, db):
        raise SystemExit(
            f"{db}: GeminiDB's Cassandra API keeps no change log a client"
            " can read, so its changes cannot be followed. Stop writes on"
            " the source and move it, then prove it with migkit check")

    # a Cassandra source is followed through ScyllaDB's change log only:
    # GeminiDB keeps none, and has nothing to follow by
    tail_start = NotHere()
    tail_seed = NotHere()
    tail_apply = NotHere()
    neutral_changes = NotHere()
    fence_wait = NotHere()
    delta_verify = NotHere()

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "rows read by token and compared row by row,"
                            " with each cell's write time and time to live"}]
        if side == "src":
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes are not followed",
                          "detail": "GeminiDB keeps no change log a client"
                                    " can read"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")


class GeminiRedisEngine(RedisEngine):
    PAIR_ENGINE = "geminidb-redis"
    REPLICATES_THE_SERVER = False
    REPLICA_COPIES = False

    # a GeminiDB instance is never a replica and serves none
    replicate_sql = NotHere()
    apply_replication_stmt = NotHere()
    replication_status = NotHere()
    src_lsn = NotHere()
    fence_wait = NotHere()
    native_replica_unsafe = Engine.native_replica_unsafe
    stream_writers = Engine.stream_writers

    def server_identity(self, side, db):
        """What it says it is: GeminiDB answers `INFO` with its own
        fields beside Redis's, and a process id that lasts no longer than
        Redis's."""
        info = self._client(side).info("server")
        return {"running": str(info.get("run_id") or ""),
                "version": str(info.get("redis_version") or ""),
                "name": str(info.get("gemini_version")
                            or info.get("server_name") or "")}

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "keys copied one by one with their times to"
                            " live, compared value by value"}]
        if side == "src":
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes are not followed",
                          "detail": "a GeminiDB instance serves no replica:"
                                    " a key written during the move is found"
                                    " by check"})
        return items


class GeminiMongoEngine(MongoEngine):
    PAIR_ENGINE = "geminidb-mongo"
