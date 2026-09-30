"""The proof kept as tests on the target (backlog R11), so it keeps running
after the source is gone.

A routine that reads no table answers the same wherever it runs, so the
calls the proof made of it - with the source's answers - are written as
plain SQL the target runs by itself (`code-tests.sql` beside the hop's
reports): one block per call, raising where the answer is not the
source's. A routine that reads tables answered over the proof's fixture,
not the target's rows, and is not kept this way. Only PostgreSQL targets
for now, and only answers a cast to text says exactly - numbers and
text; a call the source answered with an error is kept as the error's
SQLSTATE class.
"""
import re

#: calls kept per routine
KEEP = 20
#: the classes whose canonical text PostgreSQL's own text of the value is
EXACT = ("integer", "decimal", "text")


def pure(routine):
    """Whether a MySQL routine reads and writes no table (then its answers
    are the same over any rows)."""
    from . import mysql_pg
    from .translate import Refused
    if routine.kind not in ("function", "procedure") or not routine.body:
        return False
    try:
        tree = mysql_pg.read(routine.body)
    except Refused:
        return False
    for node in tree.walk():
        if node.kind in ("sql", "select_into", "result", "call",
                         "unsupported", "tx"):
            return False
        if node.kind == "block" and node.cursors:
            return False
    return not re.search(r"(?i)\b(from|into)\b", routine.body)


def _literal(value):
    if value is None:
        return "null"
    return "'" + str(value).replace("'", "''") + "'"


def tests(routine, target, calls):
    """SQL blocks, one per kept call: `calls` is [(args, outcome)] with the
    source's outcome already rendered."""
    out = []
    name = '"' + target.name.replace('"', '""') + '"'
    ins = [p for p in target.params if p.mode != "out"]
    for args, outcome in calls[:KEEP]:
        passed = [f"cast({_literal(a)} as {p.type})"
                  for p, a in zip(ins, args)]
        said = ", ".join(repr(a) for a in args).replace("'", "''")
        if outcome.error is not None:
            state = str(outcome.error[0])
            if not re.fullmatch(r"[0-9A-Z]{5}", state):
                continue
            call = f"perform {name}({', '.join(passed)})" \
                if target.kind == "function" else \
                f"call {name}({', '.join(passed)})"
            # the block's own failure has a class no routine raises, so
            # the handler for the expected one cannot swallow it
            out.append(f"do $migkit$ begin {call}; raise exception using"
                       f" errcode = 'MK001', message = 'migkit:"
                       f" {routine.name}({said}) answered where the source"
                       f" raised {state}'; exception when sqlstate"
                       f" '{state[:2]}000' then null; end $migkit$;")
            continue
        if target.kind == "function":
            out.append(
                f"do $migkit$ begin if (select {name}({', '.join(passed)})"
                f")::text is distinct from {_literal(outcome.value)} then"
                f" raise exception 'migkit: {routine.name}({said}) no longer"
                " answers as the source did'; end if; end $migkit$;")
    return out


def write(path, blocks):
    head = ("-- the calls migkit's proof made, with the source's answers:"
            " run on the target, each raises where the answer changed\n")
    path.write_text(head + "\n".join(blocks) + "\n")
