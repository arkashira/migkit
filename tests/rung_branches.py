"""Which branch of a decide rung a test reached (backlog AUD-Z.rungs):
`watched()` records every rung `decide.prune` was handed while it is open -
held where its needs all held, refused where one of its needs (or a change
on the source) did not, with the reason, and lacking where the climb's
capability ruled it out before any need was asked. A module naming a rung
in `PROVES_RUNGS` imports this, so its declaration stands on needs seen
evaluated, not on a rung imported (`test_every_decide_rung_is_reached`)."""
import contextlib

from migkit import decide

HOLDS, REFUSES, LACKS = "holds", "refuses", "lacks"


@contextlib.contextmanager
def watched():
    """[(rung name, branch, why)] of every rung pruned while open."""
    seen = []
    real = decide.prune

    def prune(rungs, facts, need=()):
        rungs = tuple(rungs)
        kept, out = real(rungs, facts, need)
        for r in kept:
            seen.append((r.name, HOLDS, None))
        for r, why in out:
            seen.append((r.name, LACKS if r.lacks(need) is not None
                         else REFUSES, why))
        return kept, out
    decide.prune = prune
    try:
        yield seen
    finally:
        decide.prune = real


def branch(seen, name):
    """The branches `name` reached, in the order reached, each once."""
    return list(dict.fromkeys(b for n, b, _ in seen if n == name))


def why(seen, name):
    """The reason `name` was last refused, or None."""
    got = [w for n, b, w in seen if n == name and b == REFUSES]
    return got[-1] if got else None
