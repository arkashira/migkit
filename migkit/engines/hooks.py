"""What an engine asks of the workflow above it, by name.

An engine adapter sits below the workflows (`docs/agents/LAYERS.md`), so it
may not import one. Where it needs a step only a workflow does - the schema
comparison, the change follower, the grants a hop carries - it asks here for
the piece by the workflow's name and the piece's name, and the workflow
fills it when it is imported (`fill`, one line at the end of the workflow
module). `HOOKS` is the whole contract: a name an engine asks that is not
declared, and one the workflow does not hold, both fail at once.

A piece asked before its workflow has filled it first loads that workflow
(a copier or a check can run in a process that never started the command
line, and the step must still be done); a workflow that cannot be loaded,
or one still being imported that has not reached its `fill`, is refused
with the workflow's name, never answered with nothing: the engine would
otherwise carry on as if the step had been done.
"""
import importlib

#: workflow -> the names engines may ask of it (its own, unchanged)
HOOKS = {
    "verifiers": ("compare_schema",),
    "changetopic": (
        "BigtableChanges", "COSMOS_SYSTEM", "CosmosFeed", "CouchbaseDcp",
        "CrdbChangefeed", "DsqlCdc", "Follower", "KafkaLanes",
        "KeyspacesChanges", "KinesisLanes", "NeptuneChanges", "SINCE",
        "SpannerChanges", "envelope", "from_text", "hlc", "hlc_text",
        "key_text"),
    "users": ("CH_SYS", "NOT_CARRIED", "_os_patterns", "_os_security",
              "carried", "system_principal"),
    "tailctl": ("alive", "begin", "follow"),
    "twoway": ("TABLE", "batch_seen", "seen_of"),
    "streamout": ("count_skipped", "encoded", "options"),
    "leaves": ("text_hash",),
    "tableengine": ("ONE_WAY", "Traits"),
    "drainmark": ("TABLE", "able", "make", "put"),
    "drift": ("transient_among",),
    "movers": ("_renamed", "stream_cannot_carry"),
}

_FILLED = {}


class HookNotFilled(RuntimeError):
    """An engine asked for a piece whose workflow has not filled it."""


def fill(workflow, held):
    """Give engines the pieces `HOOKS` declares for `workflow`, taken from
    `held` (the workflow module's own names). A declared piece the
    workflow does not hold fails here, at its import."""
    if workflow not in HOOKS:
        raise KeyError(f"no engine hook is declared for the workflow"
                       f" `{workflow}`")
    missing = [n for n in HOOKS[workflow] if n not in held]
    if missing:
        raise KeyError(f"the workflow `{workflow}` does not hold what its"
                       f" hooks declare: {', '.join(missing)}")
    _FILLED[workflow] = {n: held[n] for n in HOOKS[workflow]}


def filled(workflow):
    """Whether `workflow` has filled its hooks (it has been imported)."""
    return workflow in _FILLED


def ask(workflow, *names):
    """The piece (several: a tuple) the workflow filled under `names`."""
    got = tuple(_piece(workflow, n) for n in names)
    return got[0] if len(got) == 1 else got


def _piece(workflow, name):
    if name not in HOOKS.get(workflow, ()):
        raise KeyError(f"`{name}` is not a declared hook of the workflow"
                       f" `{workflow}` (engines/hooks.py HOOKS)")
    if workflow not in _FILLED:
        _load(workflow, name)
    return _FILLED[workflow][name]


def _load(workflow, name):
    """Import the workflow so that it fills its hooks; refuse, naming it,
    when it cannot be imported or has not filled them once it is."""
    top = __package__.rpartition(".")[0]
    try:
        importlib.import_module(f"{top}.{workflow}")
    except ImportError as why:
        raise HookNotFilled(
            f"the engine asked the workflow `{workflow}` for `{name}`, and"
            f" that workflow could not be loaded: {why}") from why
    if workflow not in _FILLED:
        raise HookNotFilled(
            f"the engine asked the workflow `{workflow}` for `{name}`"
            " before that workflow had filled its hooks (it is still being"
            " imported, or never calls `fill`)")


class _Workflow:
    """A workflow as an engine sees it: only its declared pieces, looked
    up when used (so an engine can name it before it is loaded)."""

    def __init__(self, workflow):
        if workflow not in HOOKS:
            raise KeyError(f"no engine hook is declared for the workflow"
                           f" `{workflow}`")
        self._workflow = workflow

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        return _piece(self._workflow, name)

    def __repr__(self):
        return f"<hooks of the workflow {self._workflow}>"


def workflow(name):
    """`name` as an engine may use it: attribute access returns the
    declared piece, or refuses naming the workflow."""
    return _Workflow(name)
