"""How deeply the changes of a table can be read from its source: the
scale `assess` prints per table, with the statement that would raise it
(backlog LT.R1).

D4 whole before and after images, D3 the key and the changed columns,
D2 which rows changed (values read again), D1 which tables changed (a
ranged digest diff of those), D0 nothing (a digest diff of everything,
exact only with writes stopped). Said from the settings the source
reports and each table's key; what a deeper depth would cost is the
statement's own words. Reading a setting is not choosing a method: the
method is `decide`'s.
"""

SCALE = {
    "D4": "every change with its before and after image",
    "D3": "every change with its key and the columns it changed",
    "D2": "which rows changed, their values read again",
    "D1": "which tables changed, then a digest diff of those",
    "D0": "no change reading; a digest diff of everything",
}


def mysql(log_bin, binlog_format, row_image, row_metadata, has_key):
    """(depth, the statement that raises it or "") of one MySQL table."""
    if str(log_bin).upper() not in ("ON", "1"):
        return "D0", "start the server with log_bin on (a restart)"
    if str(binlog_format).upper() != "ROW":
        return "D1", "set global binlog_format = 'ROW'"
    if str(row_metadata).upper() != "FULL":
        return "D0", "set global binlog_row_metadata = 'FULL'"
    if not has_key:
        return "D1", "give the table a primary key"
    if str(row_image).upper() in ("MINIMAL", "NOBLOB"):
        return "D3", "set global binlog_row_image = 'FULL'"
    return "D4", ""


def postgres(wal_level, replica_identity, has_key):
    """The same for one PostgreSQL table; `replica_identity` is its
    `relreplident` letter (d default, n nothing, f full, i an index)."""
    if str(wal_level).lower() != "logical":
        return "D0", "alter system set wal_level = 'logical' (a restart)"
    ident = str(replica_identity).lower()
    if ident == "f":
        return "D4", ""
    if ident == "n" or (ident == "d" and not has_key):
        return "D1", ("give the table a primary key, or alter table ..."
                      " replica identity full")
    return "D3", "alter table ... replica identity full"


def lines(table_depths, shown=5):
    """Assess rows, one per depth reached, from {table: (depth, raise)}:
    (level, item, detail) - the tables named up to `shown`, and the one
    statement that raises each depth."""
    by = {}
    for table, (depth, fix) in sorted(table_depths.items()):
        by.setdefault((depth, fix), []).append(table)
    out = []
    for (depth, fix), names in sorted(by.items(), reverse=True):
        more = f" and {len(names) - shown} more" if len(names) > shown else ""
        level = "pass" if depth == "D4" else "warn" if depth in (
            "D3", "D2") else "fail" if depth == "D0" else "warn"
        out.append((
            level, f"change reading {depth} ({SCALE[depth]}):"
            f" {len(names)} table{'s' if len(names) != 1 else ''}",
            ", ".join(names[:shown]) + more
            + (f" - to raise it: {fix}" if fix else "")))
    return out
