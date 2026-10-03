PLAYBOOKS = {
    "aws-dms": {
        "title": "AWS DMS (source/target any supported engine)",
        "phases": [
            ("1. prepare source", [
                "network: DMS replication instance must reach source (SG/VPN/peering)",
                "postgres source: set rds.logical_replication=1 (RDS) or wal_level=logical, restart",
                "mysql source: binlog_format=ROW, binlog_row_image=FULL, retention >= 24h",
                "create a migration user with replication privileges only",
            ]),
            ("2. prepare target (migkit does this part)", [
                "migkit schema <hop>: create db + full schema with the engine's own"
                " definitions (migkit move creates what is missing itself)",
                "never let DMS create tables: it makes bare tables, no secondary index/FK/default",
                "drop or disable FK constraints and triggers on target, keep PKs",
                "migkit check <hop> --only schema: confirm structure identical before load",
            ]),
            ("3. create DMS task", [
                "migration type: full load + CDC (ongoing replication)",
                "target table prep mode: DO_NOTHING (schema already exists), never DROP",
                "LOB mode: full LOB or limited with max size above your biggest value,"
                " limited mode silently truncates",
                "enable validation if both sides supported, enable CloudWatch logs",
                "batch apply off if FK order matters during CDC",
            ]),
            ("4. during load (migkit watch <hop>)", [
                "watch table stats in DMS console: full load rows, CDC latency",
                "migkit watch shows src/dst row estimates, rate, ETA, replication slots",
                "postgres source: watch replication slot size, do not let it bloat disk",
                "no DDL on source while replicating, DMS does not carry DDL for all engines",
            ]),
            ("5. cutover", [
                "stop writes on source, wait CDC latency = 0",
                "migkit sync <hop> --kind sequences --apply  (sequence/identity values)",
                "re-add FK constraints and triggers on target",
                "migkit check <hop>  (full validation, all green required)",
                "switch application, keep DMS task stopped not deleted (rollback path)",
            ]),
            ("rollback", [
                "before cutover: just stop the DMS task, source untouched",
                "after cutover: reverse replication (new DMS task dst->src) is the"
                " standard net, prepare it before cutover day",
            ]),
        ],
    },
    "tencent-dts": {
        "title": "Tencent DTS (postgres/mysql/mongodb)",
        "phases": [
            ("1. prepare source", [
                "allowlist DTS service IPs on the source (SG/firewall)",
                "postgres: DTS installs event triggers + __tencentdb__ schema on source,"
                " account needs create on db, migkit filters these objects in checks",
                "check unlogged tables: no WAL means DTS cannot CDC them",
            ]),
            ("2. prepare target (migkit does this part)", [
                "migkit setup-target <hop>: schema via native dump, match locale/encoding",
                "tencentdb blocks unlogged tables unless"
                " tencentdb_log_unlogged_table=off (session level works)",
                "drop/disable FK + triggers on target, keep PKs",
                "migkit check <hop> --only schema before starting the task",
            ]),
            ("3. create DTS task", [
                "migration type: structure off if schema pre-created, full + incremental on",
                "tables without PK need REPLICA IDENTITY FULL for incremental sync",
                "run the DTS pre-check, fix everything it flags before start",
            ]),
            ("4. during load (migkit watch <hop>)", [
                "DTS console shows phase (structure/full/incremental) and lag",
                "migkit watch: row counts both sides, rate, ETA,"
                " replication slot on source (dts_ slot must stay active)",
                "no DDL on source, DTS DDL sync coverage is partial",
            ]),
            ("5. cutover", [
                "stop writes, wait incremental lag = 0",
                "migkit sync <hop> --kind sequences --apply  (DTS never carries these)",
                "re-add FK/triggers, run migkit check <hop>, all green",
                "after cutover remove DTS leftovers on both sides:"
                " __tencentdb__ schemas, dts_ publication, dts_ event triggers, dts_ slot",
            ]),
            ("rollback", [
                "before cutover: stop DTS task, source untouched",
                "after: reverse sync via DTS dst->src or AWS DMS for the return leg",
            ]),
        ],
    },
    "gcp-dms": {
        "title": "GCP Database Migration Service (postgres/mysql/sqlserver)",
        "phases": [
            ("1. prepare source", [
                "postgres: pglogical extension required on source, wal_level=logical",
                "mysql: ROW binlog, gtid recommended",
                "connectivity: IP allowlist, reverse SSH tunnel, or VPC peering",
            ]),
            ("2. prepare target (migkit does this part)", [
                "gcp dms postgres copies schema itself via pglogical but sequences"
                " still need manual sync at cutover",
                "for heterogeneous targets pre-create schema natively, same as DMS",
            ]),
            ("3. create migration job", [
                "continuous migration type for minimal downtime",
                "run the built-in test before start",
            ]),
            ("4. during (migkit watch <hop>)", [
                "console shows phase + replication delay, watch storage on target",
            ]),
            ("5. cutover", [
                "stop writes, wait delay 0, promote the Cloud SQL replica",
                "migkit sync --kind sequences --apply, re-add FK/triggers",
                "migkit check <hop>, all green, switch app",
            ]),
            ("rollback", [
                "before promote: delete the job, source untouched",
                "after promote: reverse replication job, prepare beforehand",
            ]),
        ],
    },
    "native": {
        "title": "migkit on its own, no managed service",
        "phases": [
            ("copy", [
                "migkit move <hop> --mode full: migkit picks the fastest way"
                " this machine and the two servers allow, measures it, and"
                " holds every table it copies to the source",
                "migkit move <hop> --mode full+cdc: the copy, then the"
                " source's changes followed from a position taken before it,"
                " for near-zero downtime",
            ]),
            ("schema changes on a big live table", [
                "not a plain ALTER TABLE under load: an online schema change"
                " that copies the table in the background and can be"
                " throttled and resumed",
            ]),
            ("verify", [
                "migkit check <hop>: every table compared, exit 0 only when"
                " the two are the same",
                "migkit watch <hop> --verify --delta: only what changed,"
                " verified each cycle, while the stream runs; it says when a"
                " flip is safe",
            ]),
            ("cutover", [
                "migkit move <hop> --mode cdc --drop: the stream torn down as"
                " the hop's cutover says, behind its approvals",
            ]),
        ],
    },
}


def playbook(service):
    pb = PLAYBOOKS.get(service)
    if not pb:
        return None
    lines = [pb["title"], "=" * len(pb["title"])]
    for phase, items in pb["phases"]:
        lines.append("")
        lines.append(phase)
        lines.extend(f"  - {i}" for i in items)
    return "\n".join(lines)
