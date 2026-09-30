"""What every converter into PostgreSQL's PL/pgSQL shares: names, the
dollar quote, how a routine is wrapped, and the conversion's record.

A MySQL or T-SQL local variable wins over a column of the same name in
the statements around it; PL/pgSQL refuses such a name as ambiguous
unless told otherwise. `#variable_conflict use_variable` is that "told
otherwise", so every converted body starts with it and the source's
names can be kept as they are.
"""
import re
from dataclasses import dataclass, field

#: PostgreSQL's reserved words and PL/pgSQL's own: a variable, parameter
#: or label named one of these is written quoted
RESERVED = frozenset("""
all analyse analyze and any array as asc asymmetric authorization binary
both case cast check collate collation column concurrently constraint create
cross current_catalog current_date current_role current_schema current_time
current_timestamp current_user default deferrable desc distinct do else end
except false fetch for foreign freeze from full grant group having ilike in
initially inner intersect into is isnull join lateral leading left like limit
localtime localtimestamp natural not notnull null offset on only or order
outer overlaps placing primary references returning right select
session_user similar some symmetric system_user table tablesample then to
trailing true union unique user using variadic verbose when where window with
begin by declare execute foreach if loop strict while found exit continue
return raise perform elsif get diagnostics open close exception
""".split())

#: the dollar quote a converted body is written in
QUOTE = "$migkit$"


def ident(name):
    """A name as PL/pgSQL is to read it: bare where it can be, so the
    source's case-blind references to it all fold to the same name."""
    text = str(name)
    low = text.lower()
    if low.isidentifier() and low not in RESERVED and text.isascii():
        return low
    return '"' + text.replace('"', '""') + '"'


def unparsed(statements):
    """Why PostgreSQL's own parser (libpg_query, through pglast) refuses
    one of `statements` - the SQL, and a PL/pgSQL body's statements - or
    None where it takes them all. Syntax only: what the body means is the
    static check's and the proof's to say."""
    import pglast
    from pglast.parser import ParseError
    for text in statements:
        try:
            pglast.parser.parse_sql(text)
            if re.search(r"(?i)\blanguage\s+plpgsql\b", text):
                pglast.parse_plpgsql(text)
        except ParseError as e:
            return (str(e).splitlines() or [""])[0][:120]
        except Exception:  # noqa: BLE001 - the wrapper's own trouble
            # measured: the parse of a trigger function's body comes back
            # as JSON its wrapper cannot read; that says nothing of the
            # body, and the target's own CREATE is the next gate
            continue
    return None


@dataclass
class Converted:
    """One routine in the target's SQL, and what converting it found."""
    statements: list                    #: run in order to make it
    constructs: list = field(default_factory=list)  #: (code, line)
    notes: list = field(default_factory=list)       #: what a person reads
    #: the INOUT refcursor parameters that carry the result sets a MySQL
    #: procedure sends, in the order the source sends them
    result_sets: list = field(default_factory=list)
    #: the routine commits or rolls back itself: it cannot be proved
    #: inside a transaction the proof rolls back
    commits: bool = False
    #: what is left for a person, said once (an event's schedule)
    residue: list = field(default_factory=list)

    @property
    def sql(self):
        return ";\n".join(s.rstrip().rstrip(";") for s in self.statements) \
            + ";"


class Emitter:
    """Lines of PL/pgSQL, indented as they nest."""

    def __init__(self):
        self.lines = []
        self.depth = 0

    def __call__(self, line):
        self.lines.append("  " * self.depth + line)

    def push(self):
        self.depth += 1

    def pop(self):
        self.depth -= 1

    def text(self):
        return "\n".join(self.lines)


#: MySQL's and SQL Server's SQLSTATE classes and error numbers, as the
#: condition PL/pgSQL catches the same failure by. PostgreSQL treats a
#: code ending in 000 as the whole class
MYSQL_ERRNO = {1062: "unique_violation", 1586: "unique_violation",
               1451: "foreign_key_violation", 1452: "foreign_key_violation",
               1216: "foreign_key_violation", 1217: "foreign_key_violation",
               1048: "not_null_violation", 1364: "not_null_violation",
               1406: "string_data_right_truncation",
               1264: "numeric_value_out_of_range",
               1690: "numeric_value_out_of_range",
               1365: "division_by_zero", 1146: "undefined_table",
               1054: "undefined_column", 1213: "deadlock_detected",
               1205: "lock_not_available", 1292: "invalid_datetime_format",
               1366: "invalid_text_representation", 3819: "check_violation"}
MYSQL_SQLSTATE = {"42S02": "undefined_table", "42S22": "undefined_column",
                  "HY000": None}
