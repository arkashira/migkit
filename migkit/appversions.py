"""The schema version each side stands on, from the application's own
migration history table (backlog 26.versions).

An application's tool (Flyway, Liquibase, Alembic, Django, Rails, Entity
Framework) records every migration it has run in a table of its own. A
move carries that table with the rest, so after a move the two sides
agree - until a migration is run on one side only, which this finds: the
tool would then apply it twice or never. (A change migkit makes to the
target's schema itself is not in that table, and the tool does not know
of it: not said here yet.)

The history is read through the engine (`history_tables`, `history_rows`),
and what it says is compared here: one place for every engine.
"""

#: tool -> (table spellings, columns that name one applied migration,
#: a condition on the rows that count, if any)
TOOLS = {
    "Flyway": (("flyway_schema_history", "schema_version"),
               ("version", "description"), "success"),
    "Liquibase": (("databasechangelog",), ("id", "author", "filename"), ""),
    "Alembic": (("alembic_version",), ("version_num",), ""),
    "Django": (("django_migrations",), ("app", "name"), ""),
    "Rails": (("schema_migrations",), ("version",), ""),
    "Entity Framework": (("__efmigrationshistory",), ("MigrationId",), ""),
}


def applied(history_tables, history_rows):
    """{tool: set of applied migrations} of one side, for each tool whose
    table the side has. `history_tables()` answers {lower-cased table name:
    how the engine names it}; `history_rows(ref, columns, where)` the rows
    of one, as tuples."""
    have = history_tables()
    out = {}
    for tool, (names, columns, where) in TOOLS.items():
        ref = next((have[n] for n in names if n in have), None)
        if ref is None:
            continue
        out[tool] = {tuple("" if v is None else str(v) for v in row)
                     for row in history_rows(ref, columns, where)}
    return out


def compare(src, dst):
    """[(status, tool, detail)] from the two sides' `applied`: ok when the
    same migrations are recorded, diff when one side has some the other
    lacks, and when the table is on one side only."""
    out = []
    for tool in sorted(set(src) | set(dst)):
        s, t = src.get(tool), dst.get(tool)
        if s is None or t is None:
            where = "source" if t is None else "target"
            out.append(("diff", tool,
                        f"{tool} keeps its history on the {where} only: the"
                        " other side has no record of the migrations it ran,"
                        " so the tool would run them all again"))
            continue
        only_s, only_t = sorted(s - t), sorted(t - s)
        if not only_s and not only_t:
            out.append(("ok", tool,
                        f"{tool}: the same {len(s)} migrations are recorded"
                        " on both sides"))
            continue
        said = []
        if only_s:
            said.append(f"{len(only_s)} recorded on the source only (e.g."
                        f" {_name(only_s[-1])})")
        if only_t:
            said.append(f"{len(only_t)} recorded on the target only (e.g."
                        f" {_name(only_t[-1])})")
        out.append(("diff", tool, f"{tool}: " + "; ".join(said)))
    return out


def _name(row):
    return "/".join(p for p in row if p)[:80]

