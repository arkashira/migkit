"""YugabyteDB's YSQL as a side of a pair (backlog P0-CLOUDS.more): a
distributed SQL database on PostgreSQL's wire.

The PostgreSQL wire's engine is its connection, catalogue, reads, writes
and checks (`pgwire.PgWireEngine`); the copier's rows are written by key
in batches, as on PostgreSQL. A source is followed through the logical
replication YugabyteDB serves over PostgreSQL's protocol, from 2024.1.1
and only where its replication commands answer and a slot is free
(`decide.service_follow`, on what the server says); refused with the
reason otherwise. Its LSNs are not byte offsets and the WAL position
functions are refused, so the position a fence waits for is the server's
own hybrid-time LSN (`src_lsn`), never `pg_current_wal_lsn`.
"""
import re

from .. import decide
from .gaussdb import GaussEngine
from .pgwire import PgWireEngine

#: why the ways below are owed a run (P0-CLOUDS.more)
UNPROVED_ON = "built; not run on a real YugabyteDB cluster"
#: the cells built here and run against no such service
UNPROVED_ITEMS = (
    "yugabytedb:tail.quiet "
    "assess.brands assess.engine-checks assess.handwork "
    "assess.preflight assess.versions cutover.counters cutover.freeze "
    "cutover.read-only-target move.create-missing move.large-values "
    "move.partitioned "
    "move.sized move.table-copy operations.fencing schema.create "
    "schema.defaults schema.foreign-keys schema.indexes schema.views "
    "scope.copy scope.counters scope.deep scope.listings scope.names "
    "scope.objects scope.ops scope.repair scope.row-filter security.tls "
    "tail.follows tail.table-states verify.access.object-ownership "
    "verify.access.sequence-grants verify.access.table-grants "
    "verify.confirm verify.delta verify.fence "
    "verify.identity.sequence-collision verify.identity.sequence-parity "
    "verify.movement.target-ahead verify.repair verify.repair-undo "
    "verify.rows-apart verify.structure.columns "
    "verify.structure.indexes verify.structure.keys "
    "verify.structure.objects verify.structure.schema-diff verify.table "
    "verify.users verify.value.charset verify.value.collation "
    "verify.value.generated verify.value.narrowing "
    "verify.value.null-empty verify.value.precision "
    "verify.value.rendering verify.value.timezone")

#: the first release whose logical replication a client reads
LOGICAL_FROM = (2024, 1, 1)


def release(banner):
    """(major, minor, patch) of the `-YB-2024.1.1.0-b0` a version string
    carries, or None."""
    got = re.search(r"YB-(\d+)\.(\d+)\.(\d+)", str(banner or ""))
    return tuple(int(g) for g in got.groups()) if got else None


class YugabyteEngine(PgWireEngine):
    CANON_ENGINE = "yugabytedb"
    SAID = "YugabyteDB"
    DEFAULT_PORT = 5433
    LARGE_VALUES = GaussEngine.LARGE_VALUES
    _deep_partitions = GaussEngine._deep_partitions

    def follow_facts(self, db):
        """What the source says of its logical replication: its release,
        and the slots it has and holds."""
        banner = self._scalar("src", db, "select version()")
        got = release(banner)
        facts = {"engine": self.SAID, "logical": False, "slots": None}
        if got is None:
            facts["logical_why"] = f"its version is not read ({banner})"
            return facts
        if got < LOGICAL_FROM:
            facts["logical_why"] = (
                f"{'.'.join(map(str, got))} serves no logical replication"
                f" to a client before {'.'.join(map(str, LOGICAL_FROM))}")
            return facts
        try:
            most = int(self._scalar("src", db, "select setting from"
                                    " pg_settings where name ="
                                    " 'max_replication_slots'"))
            held = int(self._scalar("src", db, "select count(*) from"
                                    " pg_replication_slots"))
        except Exception as e:  # noqa: BLE001 - said as the reason
            facts["logical_why"] = ("its replication commands do not"
                                    f" answer: {str(e).splitlines()[0]}")
            return facts
        facts.update(logical=True, slots=most - held)
        return facts

    def follow_way(self, db):
        return decide.service_follow(self.follow_facts(db))

    def tail_start(self, db, token_path):
        choice = self.follow_way(db)
        if choice.path is None:
            raise SystemExit(f"{db}: no way to follow {self.SAID} holds:"
                             f" {choice.reason}")
        return super().tail_start(db, token_path)

    def src_lsn(self, db):
        """The source's position now: its hybrid-time LSN, in the
        `hi/lo` form a slot's positions are compared in."""
        value = int(self._scalar("src", db,
                                 "select yb_get_current_hybrid_time_lsn()"))
        return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"
