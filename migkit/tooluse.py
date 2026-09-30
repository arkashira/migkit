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

Where the code that drives a tool names each option it uses - a flag as a
string, a method or a keyword by name - what is used is not written at
all but read off that code (`reads`), as `capabilities` reads an engine's
abilities: only the reasons for the rest are written, and an option
written as unused that the code has since started using is stale.

Each tool carries its licence and its terms, for the decision layer to
gate a path by: an open licence allows every task; a closed one says what
it allows, and `allowed` answers for a hop. A program `doctor --install`
puts in place, and a container image migkit pulls, has its licence and
terms in one place (`tools.PROGRAM_LICENCES`, `tools.TERMS`, where the
operator accepts them) and is read from there; only a library migkit
imports declares its own.

The names here are the tools' own, for migkit's code and tests. None of
them reaches an operator: what the operator reads is the reason, never the
name (`tests/test_the_report_does_not_name_its_tools.py`).
"""
import ast
from dataclasses import dataclass, field
from pathlib import Path

USED, UNUSED, TOPPED = "used", "unused", "topped up"
STATES = (USED, UNUSED, TOPPED)
#: the reason of a use read off the code
READ_OFF = "read off the code that drives it"

#: modules that register a tool when imported. A module that wraps one
#: adds itself here, so `tools_used` sees every tool without importing
#: all of migkit
CONTRIBUTORS = ["migkit.movers", "migkit.verifiers",
                "migkit.engines.oracle_redo", "migkit.engines.db2i",
                "migkit.engines.warehouse", "migkit.engines.lake",
                "migkit.engines.couchbase", "migkit.engines.cosmosdb",
                "migkit.storedcode.programs", "migkit.notify",
                "migkit.traces", "migkit.redisshake", "migkit.mirror",
                "migkit.dsbulk", "migkit.fromsnapshot", "migkit.ddbexport",
                "migkit.upgrade",
                "migkit.engines.teradata", "migkit.engines.sapodp",
                "migkit.engines.saas", "migkit.engines.saas_salesforce",
                "migkit.shared", "migkit.seedtools",
                "migkit.engines.spanner", "migkit.engines.bigtable",
                "migkit.engines.firestore", "migkit.engines.keyspaces",
                "migkit.engines.neptune", "migkit.engines.timestream"]

_HERE = Path(__file__).parent


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
    #: the release `declared` documents
    release: str = ""
    #: (files, {(command, option): token}): where each use is read off the
    #: code - the option is used where its token is in those files (a
    #: file written `path::function` read only inside that function)
    reads: tuple = None


REGISTRY = {}


def register(name, *, options, licence=None, terms=None, installed=None,
             declared=None, families=(), release="", reads=None):
    """Add a tool's account. `licence` and `terms` are read from `tools`
    for a program it installs or an image it pulls, and given here only
    for a library migkit imports. With `reads`, what is used is read off
    the code, and `options` holds only what is not used and what is
    topped up."""
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
            else ("the terms it runs under are not accepted on this"
                  f" machine, so {tools.TERMS[n].otherwise} - migkit"
                  " doctor --install asks for them")))
    elif licence is None:
        raise ValueError(f"{name}: no licence given, and `tools` has none")
    terms = OPEN if terms is None else terms
    for key, (state, why) in options.items():
        if state not in STATES:
            raise ValueError(f"{name} {key}: {state!r} is not one of"
                             f" {', '.join(STATES)}")
        if reads is not None and state == USED:
            raise ValueError(f"{name} {key}: a use is read off the code"
                             " that drives it, not written")
        if not _written(why, key, options):
            raise ValueError(f"{name} {key}: the reason is not written")
    tool = Tool(name, licence, terms, dict(options), installed,
                declared, tuple(families), release, reads)
    REGISTRY[name] = tool
    return tool


def _written(why, key, options):
    """A reason of three words or more, or `see <option>` naming another
    of the tool's options, whose reason it shares."""
    words = str(why).split()
    if len(words) >= 3:
        return True
    return (len(words) == 2 and words[0] == "see"
            and (key[0], words[1]) in options
            and (key[0], words[1]) != key)


def _load():
    import importlib
    for module in CONTRIBUTORS:
        importlib.import_module(module)


def _tokens(files):
    """Every string constant, name, attribute, keyword and imported name
    in the code that drives a tool - where an option being used shows. A
    file named `path::function` is read only inside that function, for a
    file that drives other programs too. A path is migkit's own, from its
    package directory."""
    out = set()
    for rel in files:
        rel, _, fn = rel.partition("::")
        tree = ast.parse((_HERE / rel).read_text())
        if fn:
            tree = next(n for n in ast.walk(tree)
                        if isinstance(n, ast.FunctionDef) and n.name == fn)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                out.add(node.value)
            elif isinstance(node, ast.Name):
                out.add(node.id)
            elif isinstance(node, ast.Attribute):
                out.add(node.attr)
            elif isinstance(node, ast.keyword) and node.arg:
                out.add(node.arg)
            elif isinstance(node, ast.ImportFrom):
                out.update(a.name for a in node.names)
    return out


def _used_by_code(tool):
    """{(command, option)} the code that drives `tool` uses."""
    files, tokens = tool.reads
    seen = _tokens(files)
    return {key for key, token in tokens.items() if token in seen}


def accounts(name):
    """{(command, option): (state, why)} as the registry answers it now:
    the written account, and for a tool whose uses are read off the code,
    every option the code uses (topped up where that is written) and the
    reasons of the ones it does not."""
    _load()
    tool = REGISTRY[name]
    if tool.reads is None:
        return dict(tool.options)
    now = _used_by_code(tool)
    out = {}
    for key in dict.fromkeys([*tool.reads[1], *tool.options]):
        written = tool.options.get(key)
        if key in now:
            out[key] = (written if written and written[0] == TOPPED
                        else (USED, READ_OFF))
        elif written and written[0] == UNUSED:
            out[key] = written
    return out


def used(name, command=""):
    """The options of `command` the tool is used with, topped up or not."""
    return sorted(o for (c, o), (state, _) in accounts(name).items()
                  if c == command and state in (USED, TOPPED))


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
    have = accounts(name)
    return _entry(REGISTRY[name], have, command, option)


def _entry(tool, have, command, option):
    got = have.get((command, option))
    if got:
        return got
    for fam in tool.families:
        if option.startswith(fam):
            return have.get((command, fam))
    return None


def unexplained(name):
    """[(command, option)] the tool offers and migkit wrote nothing about."""
    have = surface(name) or {}
    acc = accounts(name)
    return sorted((c, o) for c, opts in have.items() for o in opts
                  if _entry(REGISTRY[name], acc, c, o) is None)


def stale(name):
    """[(command, option)] written about and not so: a use or a reason for
    an option the build does not offer any more (families, and commands
    the build was not asked about, left out), and - where the uses are
    read off the code - a reason for an option the code now uses, or a
    top-up of one it does not."""
    _load()
    tool = REGISTRY[name]
    out = set()
    have = surface(name)
    if have is not None:
        out |= {(c, o) for c, o in tool.options
                if c in have and o not in tool.families
                and o not in have[c]}
    if tool.reads is not None:
        now = _used_by_code(tool)
        out |= {k for k, (state, _) in tool.options.items()
                if (state == UNUSED and k in now)
                or (state == TOPPED and k not in now)}
    return sorted(out)


def tools_used():
    """{tool: {"licence", "terms", USED: {..}, UNUSED: {..}, TOPPED: {..}}},
    each option keyed `command option`."""
    _load()
    out = {}
    for name, tool in sorted(REGISTRY.items()):
        row = {"licence": tool.licence,
               "terms": "open" if tool.terms.open else tool.terms.allows,
               USED: {}, UNUSED: {}, TOPPED: {}}
        for (command, option), (state, why) in sorted(
                accounts(name).items()):
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
