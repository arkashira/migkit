"""Which brand is actually answering, and what that brand cannot do.

Every engine in migkit asks a server what version it is and believes the
answer. That works for the software the engine was written against and fails
quietly for everything that reimplements its wire protocol - which, by now, is
most of what a migration actually points at.

The failure is not that the fork refuses to answer. It is that the fork
answers with a **compatibility number**, and the number is true about the
protocol and false about the software. Measured here, on containers:

    brand        what it reports as its version      what it is
    -----------  ----------------------------------  ----------------
    Redis 7      redis_version:7.4.11                Redis 7.4.11
    Valkey 8     redis_version:7.2.4                 Valkey 8.1.10
    KeyDB        redis_version:6.3.4                 KeyDB
    Dragonfly    redis_version:7.4.0                 Dragonfly df-v2.0.0
    CockroachDB  server_version:13.0.0               CockroachDB v23.2.5

So a Redis 7.2.4 source and a Valkey 8.1.10 target both say `7.2.4`, and
migkit's version-match row passes them. It is not comparing two versions; it
is comparing two protocol claims, and reporting agreement as a clean bill.
That is a false pass, which is the direction this codebase treats as a bug
rather than a rough edge.

**Detection is on positive evidence only.** A brand is named because it said
something only it says - `server_name:valkey`, `dragonfly_version`,
`CockroachDB` in the version banner - never because an expected field was
missing. A missing field means an older build, a trimmed managed surface, or a
permission, and reading absence as identity would put a brand name on a server
that is not it.

What the detection is *for* is the second half: what migkit must not assume
against that brand. Two examples, both measured rather than read:

- CockroachDB has `pg_stat_all_tables`, and it is **empty** - `select count(*)`
  returns 0 - while `pg_class.relfilenode` is `0` for every relation. The
  change marker in `unchanged.py` is built from exactly those two things, so
  against CockroachDB it would be the same string forever: every table would
  skip its scan, every run, and report as proved equal. The only reason that
  does not happen today is that CockroachDB reports `server_version` as
  `13.0.0` and the marker refuses anything below 15 - the safety is an
  accident of a number the brand is free to change.

- MariaDB 11.8 has no `gtid_mode` variable at all (the query returns zero
  rows, not `OFF`), and `select @@gtid_executed` fails with
  `ERROR 1193 Unknown system variable`. MariaDB's equivalent is
  `gtid_current_pos`. Anything that reads the MySQL name to pin a CDC start
  point gets an error or an empty answer, and an empty answer is the one that
  gets treated as "GTID is off" and quietly moved past.

A brand migkit has not been measured against is reported as identified but
unmeasured. That is deliberately not the same as a pass: knowing the name of
the software is worth saying, and claiming its limits are known is not.
"""
from dataclasses import dataclass, field

# Capabilities migkit asks a server for, named once so a limit recorded in
# this file and a call site that needs it cannot drift apart.
CHANGE_MARKER = "change-marker"
CDC_POSITION = "cdc-position"
REPLICATION_DDL = "replication-ddl"
CONTENT_HASH = "content-hash"
VERSION_IS_REAL = "version-is-real"

CAPABILITY_TEXT = {
    CHANGE_MARKER: "per-table statistics that move when rows change",
    CDC_POSITION: "a position in the change log to start replication from",
    REPLICATION_DDL: "the replication statements migkit generates",
    CONTENT_HASH: "a server-side digest of a whole collection",
    VERSION_IS_REAL: "the version it reports is its own, not a"
                     " compatibility number",
}


@dataclass(frozen=True)
class Brand:
    """What answered, and what migkit knows about it.

    `measured` is whether migkit has run against this brand and recorded what
    it found, not whether the brand is popular or well documented. An
    unmeasured brand carries no limits, and that absence must never be read as
    "no limits".
    """
    name: str
    family: str
    version: str = ""
    evidence: str = ""
    measured: bool = False
    limits: dict = field(default_factory=dict)

    @property
    def identified(self):
        return self.name != "unknown"

    def cannot(self, capability):
        """Why this brand cannot do it, or '' when migkit has no finding.

        '' is not a promise. It means this file records nothing against that
        capability for this brand, which for an unmeasured brand is the
        expected state.
        """
        return self.limits.get(capability, "")

    def label(self):
        return f"{self.name} {self.version}".strip() if self.version \
            else self.name


# A signature is (field, needle): the brand is named when `needle` appears,
# case-insensitively, in that field of what the server reported. Order
# matters and is not alphabetical - YugabyteDB's version banner contains
# "PostgreSQL" too, so it has to be tested before stock PostgreSQL, and every
# family ends with its own software rather than starting with it.
#
# The `version` entry names the field that holds the brand's *own* version,
# which for a fork is usually not the field the engine already reads.
SIGNATURES = {
    "postgres": [
        ("cockroachdb", [("version", "cockroachdb")], "version"),
        ("yugabytedb", [("version", "-yb-"), ("version", "yugabyte")],
         "version"),
        ("greenplum", [("version", "greenplum")], "version"),
        ("redshift", [("version", "redshift")], "version"),
        ("postgres", [("version", "postgresql")], "server_version"),
    ],
    "mysql": [
        ("mariadb", [("version", "mariadb"),
                     ("version_comment", "mariadb")], "version"),
        ("tidb", [("version", "tidb")], "version"),
        ("vitess", [("version", "vitess")], "version"),
        ("oceanbase", [("version", "oceanbase"),
                       ("version_comment", "oceanbase")], "version"),
        ("polardb", [("version_comment", "polardb")], "version"),
        ("tdsql", [("version_comment", "tdsql")], "version"),
        ("percona", [("version_comment", "percona")], "version"),
        ("mysql", [("version_comment", "mysql")], "version"),
    ],
    "redis": [
        # Valkey says both, and says them in the Server section of INFO where
        # the engine is already looking.
        ("valkey", [("server_name", "valkey"),
                    ("valkey_version", "")], "valkey_version"),
        ("dragonfly", [("dragonfly_version", "")], "dragonfly_version"),
        # KeyDB publishes no version field of its own: measured, its INFO
        # carries `redis_version:6.3.4` and nothing named for itself. What it
        # does carry is `server_threads` and an `mvcc_depth` section, neither
        # of which stock Redis has, plus its own binary in `executable`.
        ("keydb", [("server_threads", ""), ("mvcc_depth", ""),
                   ("executable", "keydb")], "redis_version"),
        ("redis", [("redis_version", "")], "redis_version"),
    ],
}

# What migkit measured about a brand, keyed by capability. A brand absent
# here is unmeasured, which `identify` reports as such.
LIMITS = {
    "cockroachdb": {
        VERSION_IS_REAL:
            "reports server_version 13.0.0 whatever release it is, so a"
            " version comparison compares two protocol claims",
        CHANGE_MARKER:
            "pg_stat_all_tables exists and is empty (count 0) and"
            " pg_class.relfilenode is 0 for every relation, so a marker built"
            " from them never changes - every table would skip its scan and"
            " report as proved equal",
    },
    "valkey": {
        VERSION_IS_REAL:
            "redis_version is a compatibility number (measured: 7.2.4 on a"
            " Valkey 8.1.10 server); the real one is valkey_version",
    },
    "keydb": {
        VERSION_IS_REAL:
            "redis_version is a compatibility number and KeyDB publishes no"
            " version field of its own, so its release cannot be read from"
            " INFO at all",
    },
    "dragonfly": {
        VERSION_IS_REAL:
            "redis_version is a compatibility number (measured: 7.4.0 on a"
            " df-v2.0.0 server); the real one is dragonfly_version",
    },
    "mariadb": {
        CDC_POSITION:
            "has no gtid_mode variable at all - the query returns zero rows"
            " rather than OFF, and @@gtid_executed fails with ERROR 1193"
            " Unknown system variable. The MariaDB name is gtid_current_pos",
        REPLICATION_DDL:
            "the CHANGE REPLICATION SOURCE statement migkit generates carries"
            " GET_SOURCE_PUBLIC_KEY, which is MySQL 8 syntax - check the"
            " generated plan before running it here",
    },
}


def _fields(raw):
    """Lower-cased keys and string values, so a signature matches once."""
    out = {}
    for k, v in (raw or {}).items():
        out[str(k).strip().lower()] = "" if v is None else str(v)
    return out


def identify(family, raw):
    """The brand behind a family, from what the server said about itself.

    `raw` is whatever the engine could read - a version banner, an INFO dict,
    a settings row. Nothing is required: a field that is missing simply does
    not match, and a server that said nothing recognisable comes back as
    `unknown` rather than as the family's own software.
    """
    fields = _fields(raw)
    for name, sigs, version_key in SIGNATURES.get(family, []):
        for key, needle in sigs:
            got = fields.get(key)
            if got is None:
                continue
            if needle and needle not in got.lower():
                continue
            limits = LIMITS.get(name)
            return Brand(name=name, family=family,
                         version=fields.get(version_key, "").strip(),
                         evidence=f"{key}={got[:60]}" if got else key,
                         measured=limits is not None,
                         limits=dict(limits or {}))
    return Brand(name="unknown", family=family,
                 version=str(fields.get("version", "")).strip(),
                 evidence="nothing the server reported names a brand")


def mismatch(src, dst):
    """Why a source and target brand pairing is worth stopping over, or ''.

    Two different brands is not by itself an error - moving Redis to Valkey is
    a migration someone chose. What it is, is the case where every version
    number migkit compares afterwards is comparing the wrong thing, and it has
    to be said out loud before those comparisons are read as agreement.
    """
    if not src or not dst:
        return ""
    if src.name == dst.name:
        return ""
    if not src.identified or not dst.identified:
        return ""
    return (f"source is {src.label()}, target is {dst.label()} - different"
            " software behind the same protocol, so the version numbers"
            " below describe compatibility rather than agreement")


def rows(src, dst):
    """assess rows naming each side's brand and anything measured against it.

    One row per side plus, when they differ, the pairing. A brand that was
    identified but never measured is a `warn`: migkit knows what it is talking
    to and has not been run against it, and calling that a pass would be the
    same mistake in a new place.
    """
    out = []

    def add(level, item, detail):
        out.append({"level": level, "scope": "brand", "item": item,
                    "detail": str(detail)})
    for side, b in (("source", src), ("target", dst)):
        if b is None:
            continue
        if not b.identified:
            add("warn", f"{side} brand",
                f"{b.evidence} - unknown, not clean:"
                " migkit cannot say which software is answering")
            continue
        if not b.measured:
            add("warn", f"{side} brand",
                f"{b.label()} ({b.evidence}) - identified, but migkit has"
                " not been measured against it, so its limits are unknown"
                " rather than none")
            continue
        add("pass", f"{side} brand", f"{b.label()} ({b.evidence})")
    why = mismatch(src, dst)
    if why:
        add("warn", "brand pairing", why)
    for side, b in (("source", src), ("target", dst)):
        if b is None:
            continue
        for cap, reason in sorted(b.limits.items()):
            add("warn", f"{side} cannot be relied on for"
                f" {CAPABILITY_TEXT.get(cap, cap)}",
                f"{b.label()}: {reason}")
    return out
