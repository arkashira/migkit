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

#: why the ways below are owed a run: the Cassandra, Redis and MongoDB
#: wires' ways as GeminiDB answers them (P0-CLOUDS.native), proved on
#: fakes and on the parent's sandbox only
UNPROVED_ON = "built; not run on a real GeminiDB instance"
#: the ways built here and run against no such server: the whole
#: list, the matrix reads it from this source (`capabilities`)
UNPROVED_ITEMS = (
    "geminidb-redis:tail.quiet "
    "geminidb-mongo:tail.quiet "
    "geminidb-cassandra:operations.fencing geminidb-redis:operations.fencing "
    "geminidb-mongo:operations.fencing "
    "geminidb-cassandra:assess.brands geminidb-cassandra:assess.capacity "
    "geminidb-cassandra:assess.engine-checks "
    "geminidb-cassandra:assess.preflight "
    "geminidb-cassandra:assess.versions geminidb-cassandra:cutover.drain "
    "geminidb-cassandra:cutover.freeze "
    "geminidb-cassandra:cutover.read-only-target "
    "geminidb-cassandra:cutover.rollback "
    "geminidb-cassandra:cutover.two-way geminidb-cassandra:move.bulk "
    "geminidb-cassandra:move.create-missing "
    "geminidb-cassandra:move.load-window geminidb-cassandra:move.sized "
    "geminidb-cassandra:move.table-copy geminidb-cassandra:schema.create "
    "geminidb-cassandra:schema.indexes geminidb-cassandra:scope.copy "
    "geminidb-cassandra:scope.deep geminidb-cassandra:scope.listings "
    "geminidb-cassandra:scope.names geminidb-cassandra:scope.objects "
    "geminidb-cassandra:scope.ops geminidb-cassandra:scope.repair "
    "geminidb-cassandra:scope.row-filter "
    "geminidb-cassandra:tail.table-states "
    "geminidb-cassandra:verify.access.table-grants "
    "geminidb-cassandra:verify.movement.target-ahead "
    "geminidb-cassandra:verify.repair "
    "geminidb-cassandra:verify.repair-undo "
    "geminidb-cassandra:verify.rows-apart "
    "geminidb-cassandra:verify.structure.collection-options "
    "geminidb-cassandra:verify.structure.columns "
    "geminidb-cassandra:verify.structure.indexes "
    "geminidb-cassandra:verify.structure.materialized-views "
    "geminidb-cassandra:verify.structure.objects "
    "geminidb-cassandra:verify.structure.schema-diff "
    "geminidb-cassandra:verify.structure.triggers "
    "geminidb-cassandra:verify.table geminidb-cassandra:verify.tables "
    "geminidb-cassandra:verify.value.charset "
    "geminidb-cassandra:verify.value.narrowing "
    "geminidb-cassandra:verify.value.null-empty "
    "geminidb-cassandra:verify.value.precision "
    "geminidb-cassandra:verify.value.timezone "
    "geminidb-redis:assess.brands geminidb-redis:assess.capacity "
    "geminidb-redis:assess.engine-checks geminidb-redis:assess.handwork "
    "geminidb-redis:assess.preflight geminidb-redis:assess.versions "
    "geminidb-redis:cutover.drain geminidb-redis:cutover.freeze "
    "geminidb-redis:cutover.read-only-target "
    "geminidb-redis:cutover.rollback geminidb-redis:cutover.two-way "
    "geminidb-redis:move.sized geminidb-redis:scope.copy "
    "geminidb-redis:scope.deep geminidb-redis:scope.listings "
    "geminidb-redis:scope.ops geminidb-redis:scope.repair "
    "geminidb-redis:tail.follows geminidb-redis:tail.table-states "
    "geminidb-redis:verify.access.table-grants "
    "geminidb-redis:verify.delta "
    "geminidb-redis:verify.movement.target-ahead "
    "geminidb-redis:verify.repair geminidb-redis:verify.repair-undo "
    "geminidb-redis:verify.rows-apart "
    "geminidb-redis:verify.structure.keys geminidb-redis:verify.table "
    "geminidb-redis:verify.tables geminidb-mongo:assess.brands "
    "geminidb-mongo:assess.capacity geminidb-mongo:assess.engine-checks "
    "geminidb-mongo:assess.handwork geminidb-mongo:assess.preflight "
    "geminidb-mongo:assess.versions geminidb-mongo:assess.writers "
    "geminidb-mongo:cutover.drain geminidb-mongo:cutover.freeze "
    "geminidb-mongo:cutover.read-only-target "
    "geminidb-mongo:cutover.rollback geminidb-mongo:cutover.two-way "
    "geminidb-mongo:move.bulk geminidb-mongo:move.load-window "
    "geminidb-mongo:move.sized geminidb-mongo:move.table-copy "
    "geminidb-mongo:schema.indexes geminidb-mongo:schema.routines "
    "geminidb-mongo:schema.views geminidb-mongo:scope.copy "
    "geminidb-mongo:scope.deep geminidb-mongo:scope.listings "
    "geminidb-mongo:scope.names geminidb-mongo:scope.objects "
    "geminidb-mongo:scope.ops geminidb-mongo:scope.repair "
    "geminidb-mongo:scope.row-filter geminidb-mongo:tail.added "
    "geminidb-mongo:tail.ddl geminidb-mongo:tail.follows "
    "geminidb-mongo:tail.readded geminidb-mongo:tail.renames "
    "geminidb-mongo:tail.table-states "
    "geminidb-mongo:verify.access.table-grants "
    "geminidb-mongo:verify.confirm geminidb-mongo:verify.delta "
    "geminidb-mongo:verify.fence "
    "geminidb-mongo:verify.movement.target-ahead "
    "geminidb-mongo:verify.repair geminidb-mongo:verify.repair-undo "
    "geminidb-mongo:verify.rows-apart "
    "geminidb-mongo:verify.structure.collection-options "
    "geminidb-mongo:verify.structure.indexes "
    "geminidb-mongo:verify.structure.objects "
    "geminidb-mongo:verify.structure.sharding geminidb-mongo:verify.table "
    "geminidb-mongo:verify.tables geminidb-mongo:verify.value.charset "
    "geminidb-mongo:verify.value.collation "
    "geminidb-mongo:verify.value.narrowing "
    "geminidb-mongo:verify.value.null-empty "
    "geminidb-mongo:verify.value.precision "
    "geminidb-mongo:verify.value.rendering "
    "geminidb-mongo:verify.value.timezone "
    "geminidb-mongo:verify.value.type-drift "
    "geminidb-redis:move.large-values "
    "geminidb-mongo:move.large-values")


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
    # GeminiDB keeps none, and has nothing to follow by (its change
    # reader, asked anyway, says so - `_logged`)
    tail_start = NotHere()
    tail_seed = NotHere()
    tail_apply = NotHere()
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
