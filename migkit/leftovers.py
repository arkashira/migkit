"""What a mover added to the source and never took away.

Every other check in migkit compares the source with the target. This one
looks at the source on its own and asks a different question: what is in here
that the application did not put there?

Movers are not read-only. They create bookkeeping in the database they are
reading from - a schema to hold checksums, event triggers to capture DDL, a
publication, a replication slot - and several of them do not remove any of it
when the task ends. migkit's own playbook has said so in prose for a long
time: "after cutover remove the leftovers on both sides". Prose is a note to
remember something. This counts it.

Three reasons it matters more than tidiness:

- **A replication slot left behind pins WAL.** The source keeps every segment
  the slot has not consumed, and a source that runs out of disk is an outage
  on the system you already migrated away from.
- **Event triggers fire on DDL.** A trigger the mover installed is still there
  the next time anybody runs a migration on the source, long after the mover
  is gone.
- **It is evidence.** A leftover schema is proof of which mover touched this
  database and when, which matters when two of them ran and only one is
  admitted to.

The patterns are named by the mover that leaves them, so a finding says who
was here rather than just that something is odd. migkit's own artifacts are in
the list too - a tool that reports everyone else's litter and not its own is
not worth reading.
"""

class Named(str):
    """A whole name: the object is called exactly this. For the tools
    whose bookkeeping has a plain word for a name - a schema called
    `repack` - where a prefix or a suffix would also take in an
    application's `repackaging`."""


# (mover, [patterns]) - matched case-insensitively as a prefix or a whole
# name depending on the shape the mover uses. Kept as data so an engine only
# has to say what kind of object it found, never what it means.
SIGNATURES = [
    ("Tencent DTS", ["__tencentdb__", "tencentdb", "dts_"]),
    ("AWS DMS", ["awsdms_", "dms_"]),
    ("Debezium", ["debezium_", "dbz_"]),
    ("Maxwell", ["maxwell"]),
    ("Canal", ["canal_"]),
    # gh-ost and pt-osc wrap the original table name in BOTH a leading
    # underscore and a suffix. Matching the suffix alone would flag an
    # application's own `orders_new` as litter, which is the false positive
    # this module cannot afford. `_ghk` is where a paused gh-ost keeps its
    # checkpoint to resume from (`--checkpoint`)
    ("gh-ost", [("_", "_gho"), ("_", "_ghc"), ("_", "_del"),
                ("_", "_ghk")]),
    ("pt-online-schema-change", [("_", "_new"), "pt_osc_", ("_", "_old")]),
    # its checksums and its schema-change history: `percona.checksums`,
    # `percona.pt_osc_history`
    ("Percona Toolkit", [Named("percona")]),
    # a schema of its own on the source holding the sentinel its change
    # stream is steered by, and a slot of the same name. Named for what it
    # does rather than what it is called: migkit drives it too, and the
    # program is migkit's business (`tests/test_the_report_does_not_name_
    # its_tools.py`); the schema's name is the database's own word
    ("a PostgreSQL bulk copy", [Named("pgcopydb")]),
    # the extension's schema, and its slots `pgl_<db>_<provider>_<sub>`
    ("pglogical", [Named("pglogical"), "pgl_"]),
    # pgEdge's fork of it: `spock` schema and extension, `spk_` slots
    ("Spock", [Named("spock"), "spk_"]),
    # a schema in every database it replicates, holding the deltas its
    # triggers write, and the triggers themselves
    ("Bucardo", [Named("bucardo"), "bucardo_"]),
    # the extension's schema `repack` (a `log_<oid>` table per table being
    # rewritten) and the trigger it puts on that table while it runs
    ("pg_repack", [Named("repack"), Named("pg_repack"),
                   Named("repack_trigger")]),
    # its schema log (`pgstream.schema_log`), its slot and its DDL triggers
    ("pgstream", [Named("pgstream"), "pgstream_"]),
    # the loaders: tables and columns they add to what they load. A source
    # that was once a loader's destination carries them
    ("dlt", ["_dlt_"]),
    ("Sling", ["_sling_"]),
    ("PeerDB", ["_peerdb_", "peerflow_"]),
    ("Airbyte", ["_airbyte_", Named("airbyte_internal")]),
    ("migkit", ["migkit_"]),
]

#: Another tool's state and nothing else - never the application's rows, so
#: never copied, compared or repaired as them (`config.Hop.excluded` asks
#: `bookkeeping`). Stricter than what `whose` names: a finding may be
#: wrong at the cost of a line nobody acts on, an exclusion at the cost of
#: a table nobody compares, which is the worst answer migkit can give. So
#: only whole schema names and the loaders' own tables, and of gh-ost only
#: its changelog and checkpoint - its ghost and deleted tables hold
#: application rows. A column a loader added (`_sling_loaded_at`) is named
#: by `whose` and stays with its table: leaving it out would copy a table
#: into another shape than the source's.
NOT_DATA_SCHEMAS = {"percona": "Percona Toolkit",
                    "pgcopydb": "a PostgreSQL bulk copy",
                    "pglogical": "pglogical", "spock": "Spock",
                    "bucardo": "Bucardo", "repack": "pg_repack",
                    "pgstream": "pgstream",
                    "airbyte_internal": "Airbyte",
                    "_peerdb_internal": "PeerDB"}
NOT_DATA_TABLES = [
    ("gh-ost", ("_", "_ghc")), ("gh-ost", ("_", "_ghk")),
    ("dlt", Named("_dlt_loads")), ("dlt", Named("_dlt_pipeline_state")),
    ("dlt", Named("_dlt_version")),
    ("Airbyte", "_airbyte_raw_"), ("Airbyte", "_airbyte_tmp_"),
    ("PeerDB", "_peerdb_raw_"),
]


def _matches(low, pat):
    if isinstance(pat, Named):
        return low == pat
    if isinstance(pat, tuple):
        # both ends must match, so the original table name is wrapped
        pre, suf = pat
        return (low.startswith(pre) and low.endswith(suf)
                and len(low) > len(pre) + len(suf))
    return low.startswith(pat) or low.endswith(pat) or low == pat


def whose(name):
    """Which mover leaves an object with this name, or '' if none known."""
    low = str(name).lower()
    for mover, pats in SIGNATURES:
        if any(_matches(low, pat) for pat in pats):
            return mover
    return ""


def bookkeeping(*parts):
    """The tool whose own state the object named by `parts` is, or ''.

    `parts` as `Hop.excluded` takes them: a database alone, `(db, table)`,
    `(db, schema, table)`. A schema in the middle is matched whole; so is a
    database alone - the one a hop lists its databases through - but never
    the database a hop names and then asks a table of: the operator chose
    it. The last part of two or more is the table."""
    parts = [str(p).lower() for p in parts if p not in (None, "")]
    if not parts:
        return ""
    for name in (parts if len(parts) == 1 else parts[1:-1]):
        if name in NOT_DATA_SCHEMAS:
            return NOT_DATA_SCHEMAS[name]
    if len(parts) > 1:
        for mover, pat in NOT_DATA_TABLES:
            if _matches(parts[-1], pat):
                return mover
    return ""


def pg_dump_args():
    """The dump program's switches that leave another tool's schemas out
    whole (`-N`; one that matches nothing is no error): a dump of the
    whole database carried them to the target. Its tables elsewhere
    (`_dlt_loads`) are left out with the hop's own exclusions, wherever a
    move lists the source's tables (`movers.excluded_tables`)."""
    return [a for name in sorted(NOT_DATA_SCHEMAS) for a in ("-N", name)]


#: kinds whose name is qualified (`schema.table`, `schema.table.column`)
#: and matched by its last part
QUALIFIED = ("table", "column", "collection")


def group(found):
    """{mover: [labels]} from (kind, name) pairs, keeping only known movers.

    An unrecognised object is not reported. The source belongs to the
    application, and guessing that an unfamiliar schema must be litter would
    turn this from a finding into noise - the one thing a check nobody asked
    for cannot afford to be.
    """
    out = {}
    for kind, name in found:
        mover = whose(str(name).rsplit(".", 1)[-1] if kind in QUALIFIED
                      else name)
        if mover:
            out.setdefault(mover, []).append(f"{kind} {name}")
    return out


def describe(by_mover, show=4):
    parts = []
    for mover, items in sorted(by_mover.items()):
        items = sorted(set(items))
        shown = ", ".join(items[:show])
        if len(items) > show:
            shown += f" and {len(items) - show} more"
        parts.append(f"{mover}: {shown}")
    return "; ".join(parts)


def total(by_mover):
    return sum(len(set(v)) for v in by_mover.values())


# Leftovers that cost something while they sit there, as opposed to merely
# being untidy. Named by object kind, because every engine calls the object
# the same thing even when it spells the query differently.
COSTLY = {
    "slot": "pins WAL on the source until it is dropped - the source can run"
            " out of disk",
    "publication": "keeps the source publishing changes nobody reads",
    "event trigger": "still fires on the next DDL anyone runs on the source",
}


def urgent(by_mover):
    """The subset that is doing harm now, not just sitting there."""
    out = []
    # longest kind first: an object kind can be two words ("event trigger"),
    # and splitting on the first space matched "event" against nothing
    kinds = sorted(COSTLY, key=len, reverse=True)
    for items in by_mover.values():
        for label in set(items):
            for kind in kinds:
                if label.startswith(kind + " "):
                    out.append((label, COSTLY[kind]))
                    break
    return sorted(out)
