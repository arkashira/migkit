"""What migkit asks of each program and library it wraps, against what the
build installed offers (backlog 0f).

The owner's rule (2026-09-27): every tool used whole and smarter than on
its own. So each wrapped tool says, option by option, one of three things:
* used - and where the value comes from (facts, sizing, a measurement)
* unused - and why: measured slower, unsafe, broken in the build, not yet
* topped up - migkit does it itself, better, and says with what

Kept like `capabilities.matrix`: a registry every module that wraps a tool
contributes to (`register`), read off the build where the build can be
asked (`installed`) and off the pinned release's documentation where it
cannot (`declared`). `tests/test_every_wrapped_option_is_used_or_explained.py`
fails on an option the build has and nobody wrote about, and on an entry
for an option the build no longer has.

Each tool carries its licence and its terms, for the decision layer to
gate a path by: an open licence allows every task; a closed one says what
it allows, and `allowed` answers for a hop. A program `doctor --install`
puts in place has its licence and terms in one place already
(`tools.PROGRAM_LICENCES`, `tools.TERMS`, where the operator accepts
them) and is read from there; only what migkit runs that is not such a
program - a container image a generated pipeline pulls - declares its
own.

The names here are the tools' own, for migkit's code and tests. None of
them reaches an operator: what the operator reads is the reason, never the
name (`tests/test_the_report_does_not_name_its_tools.py`).
"""
from dataclasses import dataclass, field

USED, UNUSED, TOPPED = "used", "unused", "topped up"
STATES = (USED, UNUSED, TOPPED)

#: modules that register a tool when imported. A module that wraps one
#: adds itself here, so `tools_used` sees every tool without importing
#: all of migkit
CONTRIBUTORS = ["migkit.movers", "migkit.engines.db2i"]


@dataclass(frozen=True)
class Terms:
    """What a tool's terms allow. `open` for an open licence, which allows
    every task. Otherwise `allows` says it in words, and `check(hop)`
    answers "" where this hop's task is allowed and the reason where it
    is not."""
    open: bool = True
    allows: str = "every task"
    check: object = None


OPEN = Terms()


@dataclass
class Tool:
    name: str
    licence: str
    terms: Terms
    #: {(command, option): (state, why)}; a command is "" for a program
    #: without subcommands, a connector's name for a connector
    options: dict = field(default_factory=dict)
    #: () -> {command: frozenset(option)} from the build installed here,
    #: or None where there is no build here to ask
    installed: object = None
    #: {command: frozenset(option)} from the pinned release's
    #: documentation, for a build that cannot be asked where the tests run
    declared: dict = None
    #: option families: a name that starts with one of these is a member
    #: (`column.mask.hash.`, `signal.kafka.`)
    families: tuple = ()


REGISTRY = {}


def register(name, *, options, licence=None, terms=None, installed=None,
             declared=None, families=()):
    """Add a tool's account. `licence` and `terms` are read from `tools`
    for a program it installs, and given here only for one it does not."""
    from . import tools
    if name in tools.PROGRAM_LICENCES:
        if licence is not None or terms is not None:
            raise ValueError(f"{name}: its licence and terms are"
                             " `tools`'s, where the operator accepts them")
        licence = tools.PROGRAM_LICENCES[name]
        closed = tools.TERMS.get(name)
        terms = (OPEN if closed is None else Terms(
            open=False, allows=closed.allows,
            check=lambda hop, n=name: "" if tools.terms_accepted(n)
            else f"its terms are not accepted here: {closed.otherwise}"))
    elif licence is None:
        raise ValueError(f"{name}: no licence given, and `tools` has none")
    terms = OPEN if terms is None else terms
    for key, (state, why) in options.items():
        if state not in STATES:
            raise ValueError(f"{name} {key}: {state!r} is not one of"
                             f" {', '.join(STATES)}")
        if len(str(why).split()) < 4:
            raise ValueError(f"{name} {key}: the reason is not written")
    tool = Tool(name, licence, terms, dict(options), installed,
                declared, tuple(families))
    REGISTRY[name] = tool
    return tool


def _load():
    import importlib
    for module in CONTRIBUTORS:
        importlib.import_module(module)


def surface(name):
    """{command: frozenset(option)} the tool offers: its installed build's
    where it can be asked, else what its pinned release documents; None
    where neither is known."""
    _load()
    tool = REGISTRY[name]
    got = tool.installed() if tool.installed else None
    return got if got is not None else tool.declared


def entry(name, command, option):
    """(state, why) for one option, the family's where it is one."""
    _load()
    tool = REGISTRY[name]
    got = tool.options.get((command, option))
    if got:
        return got
    for fam in tool.families:
        if option.startswith(fam):
            return tool.options.get((command, fam))
    return None


def unexplained(name):
    """[(command, option)] the tool offers and migkit wrote nothing about."""
    have = surface(name) or {}
    return sorted((c, o) for c, opts in have.items() for o in opts
                  if entry(name, c, o) is None)


def stale(name):
    """[(command, option)] written about that the build does not offer: a
    use or a reason for an option that is not there any more. Families
    and commands the build was not asked about are left out."""
    _load()
    have = surface(name)
    if have is None:
        return []
    tool = REGISTRY[name]
    return sorted((c, o) for c, o in tool.options
                  if c in have and o not in tool.families
                  and o not in have[c])


def tools_used():
    """{tool: {"licence", "terms", USED: {..}, UNUSED: {..}, TOPPED: {..}}},
    each option keyed `command option`."""
    _load()
    out = {}
    for name, tool in sorted(REGISTRY.items()):
        row = {"licence": tool.licence,
               "terms": "open" if tool.terms.open else tool.terms.allows,
               USED: {}, UNUSED: {}, TOPPED: {}}
        for (command, option), (state, why) in sorted(tool.options.items()):
            row[state][f"{command} {option}".strip()] = why
        out[name] = row
    return out


def allowed(name, hop):
    """(True, "") where the tool's terms allow this hop's task, else
    (False, why) - the reason in words an operator can act on."""
    _load()
    terms = REGISTRY[name].terms
    if terms.open or terms.check is None:
        return True, ""
    why = terms.check(hop) or ""
    return (not why), why


def lines():
    """The table as the backlog keeps it: one row per option."""
    out = ["| tool | licence | option | state | why |",
           "|---|---|---|---|---|"]
    for name, row in tools_used().items():
        for state in STATES:
            for option, why in row[state].items():
                out.append(f"| {name} | {row['licence']} | `{option}` |"
                           f" {state} | {why} |")
    return out
