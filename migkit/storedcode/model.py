"""A model as the last candidate, in a loop the proof closes (backlog R11).

Published loops that feed a translation's failure back to a model gain a
few points and stall after two to five rounds; what they gain comes from
the check the model cannot argue with, not from the model. So the model is
asked last, only for what no rule and no converter produced a proved
version of, and each round it is handed the best version so far with why
it failed - the target's refusal, its static check's finding, or the call
that answered differently - and asked to change as little as it can. The
rounds are capped; every round is gated and proved like any candidate, and
re-run against every input that ever told two versions apart.

Consent is unchanged (`assist`): nothing is sent unless `MIGKIT_AI` names
a provider and `MIGKIT_AI_SHARE` includes `code`. What is sent is code: the
source's definition and the failing version. A failing call's values go
only where `MIGKIT_AI_SHARE` includes `detail` and the hop masks nothing,
since a value may have come from the source's rows. The hop option
`code_model: {provider, url, model}` names another endpoint than the
environment's for this hop (the key stays in `MIGKIT_AI_KEY`), and
`code_model_rounds` how many rounds it gets. What leaves goes through
the one gate (`egress`, by way of `assist.ask`): the egress settings are
asked, the prompt cleaned of the hop's secrets, and an address that is
not `https` - this machine's own aside - is not sent the key.
"""
import re

from .plpgsql import Converted
from .translate import Refused

#: rounds a model gets on one routine unless the hop says otherwise
ROUNDS = 3


def rounds(hop):
    try:
        return max(1, int((hop.options or {}).get("code_model_rounds",
                                                  ROUNDS)))
    except (TypeError, ValueError):
        return ROUNDS


def configured(hop):
    """Whether a model may be asked for this hop's code."""
    from .. import assist
    return bool(_endpoint(hop) or assist.provider()) and \
        assist.shares("code")


def _endpoint(hop):
    got = (hop.options or {}).get("code_model")
    return got if isinstance(got, dict) and got.get("provider") else None


def ask(hop, prompt):
    """The model's answer, through the hop's endpoint where it names one."""
    from .. import assist
    return assist.ask(prompt, _endpoint(hop), hop=hop)


def may_show_values(hop):
    from .. import assist, masking
    return assist.shares("detail") and not masking.active(hop)


def prompt(src, dst, routine, definition, tried, show_values):
    """What the model is asked, from the source's code and the versions
    tried so far with why each failed."""
    lines = [f"Convert this {src} {routine.kind} to {dst}. Keep its name,"
             f" {routine.name}, its parameters' names and modes, and what it"
             " does for every input - its result, its errors, the rows it"
             " writes - nulls included. Answer with the CREATE statement(s)"
             " only, no explanation.", "", definition or ""]
    for text, why, values in tried[-2:]:
        lines += ["", "This version was tried and failed:", "", text, "",
                  f"Why it failed: {why}"]
        if show_values and values:
            lines.append(f"The call that answered differently: {values}")
        lines.append("Change it as little as you can to fix that.")
    return "\n".join(lines)


def answer_statements(text, dst):
    """The statements in a model's answer, or []."""
    if not text or text.startswith("(the "):
        return []
    fenced = re.findall(r"```[a-zA-Z]*\n(.*?)```", text, re.S)
    body = "\n".join(fenced) if fenced else text
    if dst == "postgres":
        from .programs import statements
        return statements(body)
    return [s.strip() for s in re.split(r";\s*\n", body)
            if re.match(r"(?is)\s*create\b", s)]


def propose(hop, src, dst, routine, definition, tried):
    """One round's version, or Refused."""
    from .. import egress
    if not configured(hop):
        raise Refused("no model is configured to be sent code (MIGKIT_AI and"
                      " MIGKIT_AI_SHARE=code)")
    if not egress.allows(hop, "assist"):
        raise Refused("the egress keeps code on this machine: MIGKIT_EGRESS"
                      " or the hop's egress does not let it go to a model")
    got = answer_statements(ask(hop, prompt(src, dst, routine, definition,
                                            tried, may_show_values(hop))),
                            dst)
    if not got:
        raise Refused("the model's answer held no CREATE statement")
    return Converted(got)
