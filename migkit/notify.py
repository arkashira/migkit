"""Telling someone when a verdict changes or a change tail stops.

Receivers come from two places, and both are used:
* the hop option `notify`: an address, or a list of them
* `MIGKIT_NOTIFY`: addresses separated by commas, for every hop

What each address is sent:
* a Slack incoming webhook: `{"text": ...}`, the only shape it takes
* a Discord webhook: `{"content": ...}`
* a Microsoft Teams workflow: the message as an Adaptive Card
* `pagerduty:<routing key>`: a PagerDuty event, one incident per hop and
  one per change tail. The incident is triggered while the hop is not the
  same, and resolved when it is again.
* any other `http(s)://` address: the message as `text`, with the facts
  beside it (`event`, `hop`, `status`, ...) for anything that reads JSON

A verdict is sent when it moves between `same`, `different` and `error`,
compared with what was last sent (`notified.json`): the first difference,
the error after it, and the return to `same`. A run that says
`incomplete` - narrowed and finding nothing, or overtaken by a schema
change - is no answer either way, and sends nothing.

What is sent never carries a row's values. It carries the check, the
scope and the status of each finding, not its detail. An error that
stopped a tail is named only by its kind where the hop masks values.

The address is never printed or written anywhere, because a webhook's
address is its password. A receiver that cannot be reached is said, and
the run goes on: the run's answer is its verdict, not the delivery of it.
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

PAGERDUTY = "https://events.pagerduty.com/v2/enqueue"
TIMEOUT = 10
#: the verdicts a receiver is told about; `incomplete` is not an answer
TOLD = ("same", "different", "error")


def receivers(hop):
    got = (hop.options or {}).get("notify") if hop else None
    if isinstance(got, str):
        got = [got]
    out = []
    for r in list(got or []) + os.environ.get("MIGKIT_NOTIFY", "").split(","):
        r = str(r).strip()
        if r and r not in out:
            out.append(r)
    return out


def body(receiver, text, facts, trouble, dedup, severity="error"):
    """(url, JSON body) for one receiver, or None for one migkit cannot
    send to."""
    if receiver.startswith("pagerduty:"):
        event = {"routing_key": receiver.split(":", 1)[1],
                 "event_action": "trigger" if trouble else "resolve",
                 "dedup_key": dedup}
        if trouble:
            event["payload"] = {"summary": text[:1024], "source": "migkit",
                                "severity": severity,
                                "custom_details": facts}
        return PAGERDUTY, event
    if not receiver.startswith(("http://", "https://")):
        return None
    host = (urllib.parse.urlsplit(receiver).hostname or "").lower()
    if host == "hooks.slack.com":
        return receiver, {"text": text}
    if host in ("discord.com", "discordapp.com"):
        return receiver, {"content": text[:2000]}
    if host.endswith((".logic.azure.com", ".powerplatform.com",
                      ".webhook.office.com")):
        return receiver, {"type": "message", "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {"type": "AdaptiveCard", "version": "1.4",
                        "$schema": "http://adaptivecards.io/schemas/"
                                   "adaptive-card.json",
                        "body": [{"type": "TextBlock", "text": text,
                                  "wrap": True}]}}]}
    return receiver, {**facts, "text": text}


def send(hop, text, facts, trouble, dedup, log, severity="error"):
    """Sends to every receiver; how many took it."""
    sent = 0
    for i, receiver in enumerate(receivers(hop), 1):
        got = body(receiver, text, facts, trouble, dedup, severity)
        if got is None:
            log(f"notify receiver {i} is not an http(s) address or"
                " pagerduty:<routing key>, so nothing was sent to it")
            continue
        url, payload = got
        req = urllib.request.Request(
            url, data=json.dumps(payload, default=str).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                resp.read()
            sent += 1
        except urllib.error.HTTPError as e:
            log(f"notify receiver {i} answered {e.code}; the run goes on")
        except (urllib.error.URLError, OSError) as e:
            why = getattr(e, "reason", None) or e
            log(f"notify receiver {i} was not reached"
                f" ({type(why).__name__}); the run goes on")
    return sent


def _told(hop):
    return hop.report_dir() / "notified.json"


def verdict(hop, env, log):
    """After a check: sends when the verdict is not what receivers were
    last told. Returns how many receivers took it (0 when nothing was
    due)."""
    now = env.get("status")
    if now not in TOLD or not receivers(hop):
        return 0
    try:
        told = json.loads(_told(hop).read_text()).get("status")
    except (OSError, ValueError):
        told = None
    # nobody told anything yet is as good as told it was the same
    if now == (told or "same"):
        return 0
    findings = env.get("findings") or []
    listed = "; ".join(f"{f.get('status')} {f.get('check')}"
                       f" {f.get('scope')}" for f in findings[:5])
    text = (f"migkit: {hop.name} is {now}"
            + (f", was {told}" if told else "")
            + (f": {listed}" if listed else "")
            + (f" and {len(findings) - 5} more" if len(findings) > 5
               else ""))
    facts = {"event": "verdict", "hop": hop.name, "status": now,
             "was": told, "totals": env.get("totals"),
             "findings": [{k: f.get(k) for k in
                           ("status", "check", "scope", "category")}
                          for f in findings[:20]]}
    sent = send(hop, text, facts, now != "same", f"migkit-{hop.name}", log)
    if sent:
        # only what somebody was told: a receiver that was down hears
        # about it on the next check
        _told(hop).write_text(json.dumps({"status": now,
                                          "at": time.time()}))
    return sent


def tail_stopped(hop, db, stopped, log):
    """`stopped` is what the tail kept (`tailctl.stopped`)."""
    from . import masking
    why = stopped.get("why") or stopped.get("kind") or "an error"
    if masking.active(hop):
        # an error from the target can quote the row it was writing
        why = (f"{stopped.get('kind') or 'an error'}; the hop masks values,"
               " so what it said is only in the tail's own output")
    text = f"migkit: the change tail of {hop.name} {db} stopped: {why}"
    return send(hop, text, {"event": "tail stopped", "hop": hop.name,
                            "db": db, "why": why},
                True, f"migkit-{hop.name}-{db}-tail", log)


def tail_started(hop, db, log):
    """A tail that had stopped with an error is running again."""
    text = f"migkit: the change tail of {hop.name} {db} is running again"
    return send(hop, text, {"event": "tail started", "hop": hop.name,
                            "db": db},
                False, f"migkit-{hop.name}-{db}-tail", log)
