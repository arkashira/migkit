"""Oracle's packages as the routines the proof calls one by one (backlog
W3).

A package is one spec and one body, but what an application calls is
each public subprogram in it: `pkg.member(...)`. So each is a routine of
its own here, named `pkg.member`, carrying its package's spec and body
whole (the sandbox makes the package, not the member) and its own text
from the body (what the converters, the statement-level reduction and
the state reports read). Its parameters come from the catalogue's
arguments; a member the spec declares more than once (an overload) is
left out and said, since one name cannot be called two ways here.

On a PostgreSQL target the package is a schema of the same name and each
member a routine in it (`plsql_pg`); the target's catalogue lists them by
their own names, so they are named back `pkg.member` where the schema is
one of the source's packages (`qualify`) - that is how the two sides
meet.

Read from the catalogue the engine reads its routines from
(`ALL_SOURCE`, `ALL_ARGUMENTS`), read-only, in the hop's schema.
"""
import re

from .routines import Param, Routine


def members(eng, side, db, notes=None):
    """[Routine] of every public subprogram of every package in the
    schema whose spec and body are both there."""
    from ..engines.dbapi import MARK
    from ..engines.oracle import DECLARED
    owner = eng._owner(side, db)
    src = {}
    for name, kind, text in eng._rows(
            side, db, "select name, type, text from all_source where owner ="
                      f" {MARK} and type in ('PACKAGE', 'PACKAGE BODY')"
                      " order by name, type, line", (owner,)):
        src.setdefault((name, kind), []).append(text or "")
    args = {}
    for pkg, obj, overload, arg, declared, pos, mode in eng._rows(
            side, db, "select package_name, object_name, overload,"
                      f" argument_name, {DECLARED}, position, in_out from"
                      f" all_arguments where owner = {MARK} and package_name"
                      " is not null and data_level = 0 order by"
                      " package_name, object_name, overload, position",
            (owner,)):
        args.setdefault((pkg, obj), {}).setdefault(overload, []).append(
            (arg, declared, int(pos), str(mode)))
    out = []
    for (name, kind), lines in sorted(src.items()):
        if kind != "PACKAGE" or (name, "PACKAGE BODY") not in src:
            continue
        spec = "".join(lines)
        body = "".join(src[(name, "PACKAGE BODY")])
        pkg = eng._said(name)
        for member, member_kind in public(spec):
            got = args.get((name, member.upper()), {}) or \
                args.get((name, member), {})
            if len(got) > 1:
                if notes is not None:
                    notes.append(f"{pkg}.{member} is declared more than once"
                                 " (overloaded): left out of the proof")
                continue
            mine = next(iter(got.values()), [])
            text, lines = member_text(body, member) or ("", None)
            r = Routine(kind=member_kind, name=f"{pkg}.{eng._said(member)}",
                        language="plsql",
                        definition=f"create or replace {spec.strip()}\n"
                                   f"create or replace {body.strip()}",
                        body=text,
                        settings={"package": pkg, "spec": spec,
                                  "package body": body, "owner": owner,
                                  "lines": lines,
                                  "made": [f"create or replace"
                                           f" {spec.strip()}",
                                           f"create or replace"
                                           f" {body.strip()}"]})
            r.returns = next((d for a, d, p, _ in mine if p == 0), None) \
                if member_kind == "function" else None
            r.params = [Param(eng._said(a or ""), d, {"IN": "in",
                                                      "OUT": "out"}.get(
                                  m, "inout"))
                        for a, d, p, m in mine if p > 0 and a]
            out.append(r)
    return out


def public(spec):
    """[(member, "function" | "procedure")] a package's spec declares, in
    its order."""
    out = []
    for m in re.finditer(r"(?is)\b(function|procedure)\s+(\"[^\"]+\"|"
                         r"[\w$#]+)", _without_comments(spec)):
        name = m.group(2).strip('"')
        one = (name, m.group(1).lower())
        if one not in out:
            out.append(one)
    return out


def member_text(body, member):
    """(a member's own text in its package's body - `FUNCTION name ...
    END name;` - and its first and last lines there), or None."""
    from .plsql_pg import _Reader
    from .translate import Refused
    try:
        r = _Reader(body)
        r.need("package")
        r.need("body")
        r.dotted()
        r.i += 1
        while not r.at_end() and r.w() not in ("begin", "end"):
            if r.w() in ("function", "procedure"):
                start = r.i
                sub = r.subprogram()
                if sub["name"].lower() == member.lower():
                    first = r.t[start]
                    last = r.t[r.i - 1]
                    return r.src(start, r.i), (first.line, last.line)
                continue
            r.declarations(until=("begin", "end", "function", "procedure"))
    except Refused:
        return None
    return None


def _without_comments(text):
    return re.sub(r"(?s)--[^\n]*|/\*.*?\*/|'(?:[^']|'')*'", " ", text)


def qualify(routines, packages):
    """The target's routines in a schema named for one of the source's
    `packages` named `schema.name`, as the source's members are."""
    wanted = {p.lower() for p in packages}
    if not wanted:
        return routines
    for r in routines:
        schema = str(r.settings.get("schema") or "").lower()
        if r.kind in ("function", "procedure") and schema in wanted and \
                "." not in r.name:
            r.name = f"{schema}.{r.name}"
            r.settings["package"] = schema
    return routines
