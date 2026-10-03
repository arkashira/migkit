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

Every prompt goes through the one gate of what leaves the machine
(`egress`): the environment's `MIGKIT_EGRESS` and the hop's `egress` say
whether a model may be asked at all and at which hosts, and the prompt
is cleaned of every secret the hop holds and of the programs migkit
drives. The key goes only over TLS: an address that is not `https` is
refused - the model is not asked - unless it is this machine's own
(`http://localhost:11434` for a local model), where no network is
crossed.
"""
import json
import os
import urllib.error
import urllib.request
from urllib.parse import urlsplit

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


def _request(kind, prompt, endpoint=None):
    """(url, headers, body) for one prompt to `kind`. `endpoint` names
    another address and model than the environment's (a hop's own); the
    key is always the environment's."""
    endpoint = endpoint or {}
    base = (str(endpoint.get("url") or "") or
            os.environ.get("MIGKIT_AI_URL", "")).rstrip("/") or BASE[kind]
    key = os.environ.get("MIGKIT_AI_KEY", "")
    model = str(endpoint.get("model") or "") or \
        os.environ.get("MIGKIT_AI_MODEL", "") or DEFAULT_MODEL.get(kind)
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


#: hosts an address may name over plain HTTP: the model runs here
LOOPBACK = ("localhost", "127.0.0.1", "::1")


def refused(url):
    """Why the model at `url` is not asked, or None: the key and the
    prompt go in the clear over anything but TLS, so only this machine's
    own address may be plain HTTP."""
    got = urlsplit(url)
    host = (got.hostname or "").lower()
    if got.scheme == "https" or (got.scheme == "http" and (
            host in LOOPBACK or host.startswith("127."))):
        return None
    return (f"the model's address is {got.scheme or 'not'} a TLS address"
            " (https): the key and what is asked would cross the network"
            " readable, so the model was not asked")


def ask(prompt, endpoint=None, hop=None):
    """The provider's answer, or None where none is configured or the
    egress keeps prompts here. A provider that cannot be reached, or an
    address that is not TLS, is said as the answer: this never stops a
    command whose own result is already in. `endpoint` ({provider, url,
    model}) is a hop's own choice over the environment's; `hop` the hop
    the prompt is about, whose secrets it is cleaned of and whose egress
    is asked."""
    from . import egress
    kind = str((endpoint or {}).get("provider") or "").lower() or provider()
    if not kind:
        return None
    if kind not in BASE:
        raise SystemExit(f"the model provider {kind} is not one of"
                         f" {', '.join(sorted(BASE))}")
    if not egress.allows(hop, "assist"):
        return None
    prompt = egress.words(hop, prompt)
    url, headers, body = _request(kind, prompt, endpoint)
    why = refused(url)
    if why:
        return f"(the {kind} provider was not asked: {why})"
    if not egress.permit(hop, "assist", url):
        return None
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


def propose(src, dst, kind, name, definition, hop=None):
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
        f" explanation.\n\n{definition}", hop=hop)
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
    return ask(prompt, hop=hop)
