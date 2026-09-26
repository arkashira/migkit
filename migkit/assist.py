"""Help from a language model, from any provider, off unless configured
(backlog 43).

`MIGKIT_AI` names the provider:
* `openai`: any endpoint that speaks the OpenAI chat completions API.
  That includes local models served by Ollama or llama.cpp, at
  `MIGKIT_AI_URL` (`https://api.openai.com/v1` by default).
* `anthropic`: the Messages API (`MIGKIT_AI_URL` overrides the address)
* `google`: the Gemini API (`MIGKIT_AI_URL` overrides the address)

`MIGKIT_AI_KEY` is the key and `MIGKIT_AI_MODEL` the model. Nothing is
sent anywhere unless `MIGKIT_AI` is set.

What it is used for:
* after a `check` that found something, a plain-language account of the
  findings. That account is a proposal, not a verdict. It is printed
  under the verdict and changes nothing.
* a view or function the translator cannot carry to the target's SQL, a
  proposed statement for it, when `MIGKIT_AI_SHARE` includes `code`: the
  object's definition is what is sent, never a row. The proposal is
  marked as one in the converted file, and `check` holds it to the same
  inputs and outputs as the source's, as anything converted is.

What is sent is each finding's check, scope, status and category. The
finding's detail, where keys and values are, is sent only when
`MIGKIT_AI_SHARE=detail` says so, and never for a hop that masks values.
"""
import json
import os
import urllib.error
import urllib.request

TIMEOUT = 60
DEFAULT_MODEL = {"anthropic": "claude-sonnet-5"}
BASE = {"openai": "https://api.openai.com/v1",
        "anthropic": "https://api.anthropic.com/v1",
        "google": "https://generativelanguage.googleapis.com/v1beta"}


def provider():
    got = os.environ.get("MIGKIT_AI", "").strip().lower()
    if got and got not in BASE:
        raise SystemExit(f"MIGKIT_AI={got} is not one of"
                         f" {', '.join(sorted(BASE))}")
    return got or None


def _request(kind, prompt):
    """(url, headers, body) for one prompt to `kind`."""
    base = os.environ.get("MIGKIT_AI_URL", "").rstrip("/") or BASE[kind]
    key = os.environ.get("MIGKIT_AI_KEY", "")
    model = os.environ.get("MIGKIT_AI_MODEL", "") or DEFAULT_MODEL.get(kind)
    if not model:
        raise SystemExit(f"MIGKIT_AI={kind} needs MIGKIT_AI_MODEL: migkit"
                         " does not choose a model for this provider")
    if kind == "anthropic":
        return (f"{base}/messages",
                {"x-api-key": key, "anthropic-version": "2023-06-01"},
                {"model": model, "max_tokens": 1024,
                 "messages": [{"role": "user", "content": prompt}]})
    if kind == "google":
        return (f"{base}/models/{model}:generateContent",
                {"x-goog-api-key": key},
                {"contents": [{"parts": [{"text": prompt}]}]})
    return (f"{base}/chat/completions",
            {"Authorization": f"Bearer {key}"} if key else {},
            {"model": model,
             "messages": [{"role": "user", "content": prompt}]})


def _answer(kind, got):
    if kind == "anthropic":
        return "".join(b.get("text", "") for b in got.get("content") or [])
    if kind == "google":
        parts = ((got.get("candidates") or [{}])[0].get("content") or {}
                 ).get("parts") or []
        return "".join(p.get("text", "") for p in parts)
    return ((got.get("choices") or [{}])[0].get("message") or {}
            ).get("content", "")


def ask(prompt):
    """The provider's answer, or None where none is configured. A provider
    that cannot be reached is said as the answer: this never stops a
    command whose own result is already in."""
    kind = provider()
    if not kind:
        return None
    url, headers, body = _request(kind, prompt)
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **headers},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return _answer(kind, json.loads(resp.read())).strip()
    except urllib.error.HTTPError as e:
        return f"(the {kind} provider answered {e.code})"
    except (urllib.error.URLError, OSError, ValueError) as e:
        why = getattr(e, "reason", None) or e
        return f"(the {kind} provider could not be reached:" \
               f" {type(why).__name__})"


def shares(what):
    """Whether `MIGKIT_AI_SHARE` lets `what` go to the provider."""
    got = {w.strip() for w in
           os.environ.get("MIGKIT_AI_SHARE", "").split(",") if w.strip()}
    return what in got


def propose(src, dst, kind, name, definition):
    """A statement creating `name` in the target's SQL, proposed by the
    provider from the source's definition - or None where none is
    configured, code may not be sent, or the answer holds no statement."""
    import re
    if not provider() or not shares("code") or not definition:
        return None
    answer = ask(
        f"Convert this {src} {kind} to {dst}. Keep its name, {name}, its"
        " arguments' names and what it returns for every input, nulls"
        " included. Answer with the one CREATE statement only, with no"
        f" explanation.\n\n{definition}")
    if not answer or answer.startswith("(the "):
        return None
    fenced = re.search(r"```[a-zA-Z]*\n(.*?)```", answer, re.S)
    text = (fenced.group(1) if fenced else answer).strip()
    return text if re.match(r"(?is)\s*create\b", text) else None


def explain(hop, env):
    """A plain-language account of a verdict's findings, or None."""
    if not provider():
        return None
    from . import masking
    share = shares("detail") and not masking.active(hop)
    keep = ("check", "scope", "status", "category") + \
        (("detail",) if share else ())
    findings = [{k: f.get(k) for k in keep}
                for f in (env.get("findings") or [])[:40]]
    prompt = (
        "You are reading the result of a database migration check. The"
        f" source and target of hop {hop.name!r} ({hop.engine}) were"
        f" compared, and the verdict is {env.get('status')!r}. Explain, in"
        " plain language for the engineer running the migration, what each"
        " finding means and what they would usually do next. Do not invent"
        " facts that are not in the findings. Findings as JSON:\n"
        + json.dumps(findings, indent=1, default=str))
    return ask(prompt)
