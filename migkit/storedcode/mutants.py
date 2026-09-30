"""The proof's own mutation score: can it say "different"? (leapfrog
research P18).

A proof that passes every version it is shown proves nothing. So a
version already proved is changed on purpose, one small change at a time
- the silent differences the rule tables list: a comparison moved across
its boundary, a constant off by one, an `AND` for an `OR`, a `+` for a
`-`, a NULL default dropped, a statement left out - and each changed
version is proved again. A mutant the proof passes is one of two things:
a change that means nothing on any input (the same routine written
another way), or a hole in the proof's inputs; the score is the share it
caught, and the survivors are named so a person can tell which.
"""
import re
from dataclasses import dataclass

#: (what, pattern, replacement): one change each, applied at one place
CHANGES = [
    ("a comparison moved across its boundary", r"(?<![<>!=])>=(?!=)", ">"),
    ("a comparison moved across its boundary", r"(?<![<>!=:-])>(?![=>])",
     ">="),
    ("a comparison moved across its boundary", r"(?<![<>!=])<=(?!=)", "<"),
    ("a comparison moved across its boundary", r"(?<![<>!=])<(?![<=>])",
     "<="),
    ("an equality turned round", r"(?<![<>!=:])=(?![=>])", "<>"),
    ("AND for OR", r"(?i)\band\b", "or"),
    ("OR for AND", r"(?i)\bor\b", "and"),
    ("+ for -", r"(?<=\s)\+(?=\s)", "-"),
    ("- for +", r"(?<=\s)-(?=\s)", "+"),
    ("a NULL default dropped", r"(?i)\bcoalesce\(([^,()]+),[^()]+\)",
     r"(\1)"),
]
_NUMBER = re.compile(r"(?<![\w.$'])(\d+)(?![\w.'])")
#: a number that is a type's length or scale, not a value the code uses
_TYPE_ARGUMENT = re.compile(r"(?i)\b(varchar|char|numeric|decimal|"
                            r"timestamp|time|varbinary|bit)\s*\([\d,\s]*$")


@dataclass
class Mutant:
    what: str
    line: int
    text: str


def _body(statement):
    """(start, end) of the dollar-quoted body in a CREATE statement."""
    m = re.search(r"(\$[A-Za-z_]*\$)", statement)
    if not m:
        return None
    end = statement.find(m.group(1), m.end())
    return (m.end(), end) if end > 0 else None


def mutants(statements, most=40):
    """[Mutant] of `statements` (the text of each a list of statements
    again), each with one small change inside the first dollar-quoted
    body - never in a signature - at most `most`."""
    k = next((i for i, st in enumerate(statements) if _body(st)), None)
    if k is None:
        return []
    stmt = statements[k]
    lo, hi = _body(stmt)
    body = stmt[lo:hi]
    # declarations and directives say what is there, not what is done
    begin = re.search(r"(?im)^\s*begin\b", body)
    start = begin.start() if begin else 0
    made = []

    def keep(what, at, new_body):
        whole = stmt[:lo] + new_body + stmt[hi:]
        made.append(Mutant(what, body[:at].count("\n") + 1,
                           statements[:k] + [whole] + statements[k + 1:]))
    for what, pattern, repl in CHANGES:
        for m in re.finditer(pattern, body[start:]):
            at = start + m.start()
            if _in_string(body, at):
                continue
            keep(what, at, body[:at] + m.expand(repl) + body[start + m.end():])
            break
    for m in list(_NUMBER.finditer(body[start:])):
        at = start + m.start()
        if len([x for x in made if x.what == "a constant off by one"]) >= 4:
            break
        if not _in_string(body, at) and not _TYPE_ARGUMENT.search(
                body[max(0, at - 40):at]):
            keep("a constant off by one", at, body[:at] +
                 str(int(m.group(1)) + 1) + body[start + m.end():])
    lines = body.split("\n")
    for i, ln in enumerate(lines):
        if i and (re.match(r"^\s*[\w\".]+\s*:=.*;\s*$", ln) or re.match(
                r"(?i)^\s*(insert|update|delete|return)\b.*;\s*$", ln)):
            at = len("\n".join(lines[:i]))
            keep("a statement left out", at,
                 "\n".join(lines[:i] + lines[i + 1:]))
            break
    return made[:most]


def _in_string(text, at):
    return text[:at].count("'") % 2 == 1


def score(proof, routine, statements, most=40):
    """(caught, tried, [(what, line) that passed]) of `statements`'
    mutants, each made in the target's sandbox in place of the proved
    version and proved against the source's; the proved version is put
    back after."""
    # the fixed inputs and every one that ever told two versions apart:
    # what a mutant survives is then what those inputs cannot see
    searching, proof.searching = proof.searching, False
    try:
        return _score(proof, routine, statements, most)
    finally:
        proof.searching = searching


def _score(proof, routine, statements, most):
    caught, tried, survived = 0, 0, []
    for m in mutants(statements, most):
        verdict, made = proof.try_version(routine, m.text)
        if verdict.reason.startswith("the target refuses it") or \
                verdict.reason.startswith("the target's static check"):
            continue
        tried += 1
        if verdict.status == "diff":
            caught += 1
        else:
            survived.append((m.what, m.line))
    proof.install(routine, statements)
    return caught, tried, survived
