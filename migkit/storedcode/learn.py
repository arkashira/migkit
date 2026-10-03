"""Rules learned from the fixes that proved (leapfrog research P16).

When migkit's rules made a version of a routine that failed its proof
and another version - a person's file, a model's - proved, the two are
compared. Where they differ only by one function standing for another
(`length(` where `octet_length(` was needed, `now(` where
`clock_timestamp(` was), that substitution is a rule candidate for this
pair. It is not trusted for being the fix of one routine: it is applied
to every routine of the hop whose rules' version calls that function,
each proved again, and adopted only when none that proved before fails
with it (zero regressions) and at least one proves that did not. An
adopted rule is kept per hop, with the routines it was proved on, and
the rules apply it from then on; a candidate that regressed is kept as a
proposal with the routine it broke.
"""
import json
import re

_CALL = re.compile(r"(?i)\b([a-z_][\w.]*)\s*\(")


def substitution(before, after):
    """(function, function) where `after` is `before` with every call of
    one function made a call of another, else None."""
    a = [m.group(1).lower() for m in _CALL.finditer(before)]
    b = [m.group(1).lower() for m in _CALL.finditer(after)]
    if len(a) != len(b):
        return None
    pairs = {(x, y) for x, y in zip(a, b) if x != y}
    if len(pairs) != 1:
        return None
    old, new = pairs.pop()
    swapped = re.sub(rf"(?i)\b{re.escape(old)}\s*\(", f"{new}(", before)
    squash = re.compile(r"\s+")
    if squash.sub(" ", swapped).strip().lower() != \
            squash.sub(" ", after).strip().lower():
        return None
    return old, new


def apply(text, rules):
    """`text` with every adopted substitution made."""
    for rule in rules:
        text = re.sub(rf"(?i)\b{re.escape(rule['from'])}\s*\(",
                      f"{rule['to']}(", text)
    return text


class Learned:
    """The hop's learned rules and proposals, beside its reports."""

    def __init__(self, path):
        self.path = path
        try:
            self.data = json.loads(path.read_text()) if path and \
                path.exists() else {}
        except (OSError, ValueError):
            self.data = {}
        self.data.setdefault("adopted", [])
        self.data.setdefault("proposals", [])

    def adopted(self, pair):
        return [r for r in self.data["adopted"] if r["pair"] == pair]

    def known(self, pair, old, new):
        return any(r["pair"] == pair and r["from"] == old and r["to"] == new
                   for r in self.data["adopted"] + self.data["proposals"])

    def adopt(self, pair, old, new, proved, source):
        self.data["adopted"].append({"pair": pair, "from": old, "to": new,
                                     "proved": sorted(proved),
                                     "learned from": source})

    def propose(self, pair, old, new, broke, source):
        self.data["proposals"].append({"pair": pair, "from": old, "to": new,
                                       "broke": sorted(broke),
                                       "learned from": source})

    def save(self):
        if self.path:
            from .. import durable
            durable.write_atomic(self.path, json.dumps(
                self.data, indent=1, sort_keys=True))
