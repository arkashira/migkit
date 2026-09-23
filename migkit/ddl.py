"""Statements migkit generates that should not simply be applied.

`check schema` hands the operator DDL that would bring the target's schema
up to the source's, and says "review, then apply". One shape in that DDL
does not survive contact with a target that already has rows, and the two
engines break differently - measured, same statement, same data:

    PostgreSQL 16   alter table t add column note text not null;
                    ERROR:  column "note" of relation "t" contains null
                            values
                    (the same statement on an *empty* table succeeds)

    MySQL 8, with STRICT_TRANS_TABLES set
                    alter table t add column note text not null;
                    Query OK - and every existing row now holds '',
                    length 0, not null

PostgreSQL refuses out loud. MySQL invents a value for every row that was
already there, which is the quieter failure and the worse one: the column
exists, the counts match, nothing errors, and the contents are made up.

migkit generates exactly this shape whenever the source has a `NOT NULL`
column with no default - a column the application fills - and the target
does not have it yet. Verified end to end: pointed at a source with
`note text not null` and a target of three rows without it, the generated
`structural-fix.sql` was

    alter table "public"."t" add column "note" text not null;
    alter table "public"."t" add column "tagged" text not null default
        'x'::text;

and applying the first of those to the target failed. The second, which
carries a default, applied cleanly.

A linter reading the SQL alone can only say *might*. migkit knows whether
the target table has rows, so it can say *will*.
"""
import re

#: `alter table <name> add [column] <name> <the rest of the definition>`.
#: Written against the differ's own output, which quotes both identifiers
#: and always spells out `add column`, but kept tolerant of the shorter
#: form because the same file is read when an operator has edited it.
_ADD_COLUMN = re.compile(
    r"^\s*alter\s+table\s+(?P<table>[^\s(]+)\s+"
    r"add\s+(?:column\s+)?(?P<column>[^\s(]+)\s+(?P<rest>.+?);?\s*$",
    re.I | re.S)


def _bare(name):
    """`"public"."t"` -> `public.t`, so a name can be matched against the
    catalogue's own spelling."""
    return name.replace('"', "").replace("`", "").replace("[", "").replace(
        "]", "")


def needs_backfill(sql):
    """[(table, column, statement)] for statements that add a NOT NULL
    column without saying what the rows that already exist should hold.

    A default answers it, and so does an identity or generated column -
    those supply a value per row. Anything else leaves the question to the
    engine, and neither engine's answer is one migkit should let pass
    without saying so.
    """
    found = []
    for raw in split(sql):
        m = _ADD_COLUMN.match(raw)
        if not m:
            continue
        rest = " ".join(m.group("rest").split()).lower()
        if "not null" not in rest:
            continue
        if any(w in rest for w in ("default", "generated", "identity",
                                   "auto_increment", "serial")):
            continue
        found.append((_bare(m.group("table")), _bare(m.group("column")),
                      " ".join(raw.split())))
    return found


def split(sql):
    """Statements, one per entry. Deliberately the same naive split the
    lock report uses - the input is generated DDL, not arbitrary SQL with
    semicolons inside string literals."""
    return [s for s in (part.strip() for part in sql.split(";")) if s]


#: What each engine does with such a statement when the table has rows.
#: Keyed by `Engine.ENGINE_FAMILY` so a new engine has to say which it is
#: rather than inherit an answer that may not be true of it.
BEHAVIOUR = {
    "postgres": ("refuses a statement like that",
                 "it fails, and nothing after it in the script runs"),
    "mysql": ("accepts it and fills the gap itself",
              "it succeeds, and every row that was already there ends up"
              " holding a value nobody chose - an empty string, a zero, a"
              " zero date"),
}


def backfill_warning(family, findings, non_empty):
    """One sentence for the tables that will actually be hit, or "".

    `non_empty` is the set of tables the target has rows in. A linter
    reading the SQL alone would have to hedge; migkit asked the target, so
    it does not.
    """
    hit = [(t, c) for t, c, _ in findings if t in non_empty]
    if not hit:
        return ""
    verb, consequence = BEHAVIOUR.get(
        family, ("does something migkit has not measured on this engine",
                 "check what it does to the rows that are already there"
                 " before applying"))
    named = ", ".join(f"{t}.{c}" for t, c in hit[:4])
    more = " ..." if len(hit) > 4 else ""
    one = len(hit) == 1
    return (f"{len(hit)} of the generated statements"
            f" {'adds' if one else 'add'} a required column to a table that"
            f" already has rows ({named}{more}) without saying what those"
            f" rows should hold - this database {verb}, so {consequence}")
