"""A hop set up in the view (backlog R21.8): two addresses with a user and
a password each, and the page does what `migkit init` does - it calls the
guide (`guide.run`) and nothing else writes the hop, so the page and the
terminal cannot disagree on what is found, what is proposed or what is
written.

What the page adds is only what a terminal gives for free:

* **Who may**: a hop does not exist before it is made, so there is no role
  on it yet. A global admin may make any; anyone else must name the hop
  and hold operator on that name (`roles.may_create`), so what they make
  is theirs to run.
* **No password in an address, a URL or a page**: the password has its
  own field, travels in the POST body only, is handed to the guide in its
  environment mapping (never this process's) and kept where `init` keeps
  it, the keychain. An address that carries one (written or as a
  reference to read) is refused: a reference would be read here and sent
  to whatever server the address names.
* **Addresses only**: `init` also reads a task file from a path; a path
  typed in a page would be read on the machine serving it, so only an
  address is taken.
* **One at a time**: the guide reads and rewrites the hops file; two
  pages doing it together would each add to what they read.
* **The words of the console**: what the guide says is shown as it prints
  it, with the passwords hidden and no program named.
"""
import re
import threading
from urllib.parse import quote, unquote

#: a hop's name as the page takes it
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$")
#: the most of an address, a user and a password the page takes
ADDRESS_MOST, USER_MOST, PASSWORD_MOST = 2048, 128, 1024
#: the query words of an address that would hold a password
SECRET_WORDS = ("password", "passwd", "pwd", "secret", "token")
#: the words of the page's own next step
EXAMPLE = "postgres://db1:5432/shop"

_LOCK = threading.Lock()


class Refused(Exception):
    """A hop that is not made, with the words to show."""

#: option flag on the command line -> the name the runner's option has
FLAGS = {"--mode": "mode", "--db": "db", "--table": "table",
         "--only": "only", "--kind": "kind", "--tag": "tag",
         "--state": "state", "--on-conflict": "on_conflict",
         "--deep": "deep", "--apply": "apply", "--go": "go",
         "--drop": "drop"}
#: the runs that take a command line's first word
CANDIDATES = {"assess": ("assess",), "check": ("check",),
              "sync": ("sync",), "rollback": ("rollback",),
              "report": ("report",), "move": ("plan", "move", "cutover")}


def _plain(text):
    """A line the console prints, as plain text: its markup taken out."""
    try:
        from rich.text import Text
        return Text.from_markup(str(text)).plain
    except Exception:  # noqa: BLE001 - a line that is not markup
        return re.sub(r"\[/?[a-z ]+\]", "", str(text))


def _control(text):
    return any(ord(c) < 32 or ord(c) == 127 for c in text)


def _address(text, user, side):
    """(the address with its user, "" or what is wrong with it)."""
    text = str(text or "").strip()
    if not text:
        return "", (f"the {side} address is empty - write it like"
                    f" {EXAMPLE}")
    if len(text) > ADDRESS_MOST or _control(text):
        return "", (f"the {side} address is too long or holds a line break"
                    f" - write it on one line, like {EXAMPLE}")
    if "://" not in text:
        return "", (f"the {side} address is not an address - write it like"
                    f" {EXAMPLE}; a task file is read at a terminal"
                    " (`migkit init`), not from a page")
    scheme, _, rest = text.partition("://")
    head, _, query = rest.partition("?")
    held, _, _ = head.partition("#")
    if "@" in held:
        given = held.rpartition("@")[0]
        if ":" in given:
            return "", (f"the {side} address holds a password; take it out"
                        " and type it in the password field - it is kept in"
                        " this machine's keychain, never in an address")
        if user and unquote(given) != user:
            return "", (f"the {side} user is in the address ({unquote(given)})"
                        f" and in the user field ({user}), and they differ"
                        " - keep one")
    elif user:
        text = f"{scheme}://{quote(user, safe='')}@{rest}"
    for pair in query.split("&"):
        if pair.partition("=")[0].lower() in SECRET_WORDS:
            return "", (f"the {side} address carries a secret in its query;"
                        " take it out - passwords go in the password field")
    from .. import addresses
    try:
        addresses.parse(text, side)
    except SystemExit as e:
        return "", f"{e}; write it like {EXAMPLE}"
    return text, ""


def read(form):
    """(values, problems) of a submitted form: every field checked
    before anything is asked of a server. Each problem says what to do
    next."""
    from .. import hopkeys
    problems = []
    values = {}
    for side in ("source", "target"):
        user = str(form.get(f"{side}_user", "")).strip()
        if user and (len(user) > USER_MOST or _control(user)):
            problems.append(f"the {side} user is too long or holds a line"
                            " break")
            user = ""
        password = str(form.get(f"{side}_password", ""))
        if len(password) > PASSWORD_MOST or "\x00" in password:
            problems.append(f"the {side} password is longer than"
                            f" {PASSWORD_MOST} characters or holds a null"
                            " byte - check what was typed")
            password = ""
        address, why = _address(form.get(side), user, side)
        if why:
            problems.append(why)
        values[side] = address
        values[f"{side}_password"] = password
    name = str(form.get("name", "")).strip()
    if name and not NAME.match(name):
        problems.append(f"the name {name!r} is not one a hop takes - letters,"
                        " digits, - _ . and at most 63 characters, or leave"
                        " it empty and migkit names it")
        name = ""
    goal = str(form.get("goal", "")).strip()
    if goal and goal not in hopkeys.GOALS:
        problems.append(f"the goal {goal!r} is none of"
                        f" {', '.join(hopkeys.GOALS)} - or leave it empty and"
                        " migkit proposes one from what it finds")
        goal = ""
    values.update(name=name, goal=goal)
    return values, problems


def create(values, path):
    """The guide run as `init` runs it, without a terminal: its result,
    and the lines it said. Raises `Refused` with the words to show."""
    from .. import guide
    passwords = [values["source_password"], values["target_password"]]
    env = {"MIGKIT_SOURCE": values["source"],
           "MIGKIT_TARGET": values["target"]}
    if values["name"]:
        env["MIGKIT_HOP"] = values["name"]
    if values["goal"]:
        env["MIGKIT_GOAL"] = values["goal"]
    kept = False
    for side in ("source", "target"):
        if values[f"{side}_password"]:
            env[f"MIGKIT_{side.upper()}_PASSWORD"] = values[
                f"{side}_password"]
            kept = True
    if kept:
        if not guide.keychain_usable():
            raise Refused("this machine has no keychain migkit can use, so"
                          " the page cannot keep a password; run `migkit"
                          " init` at a terminal on it (it can read the"
                          " passwords from environment variables), or leave"
                          " the password fields empty where a server needs"
                          " none")
        env["MIGKIT_SECRETS"] = "keychain"
    said = []
    try:
        with _LOCK:
            got = guide.run(None, said.append, path, env=env)
    except (SystemExit, Exception) as e:  # noqa: BLE001 - init stops
        raise Refused(_words(_stop(e), passwords) + " Nothing was written;"
                      " change what it names and submit again, or run"
                      " `migkit init` at a terminal for the same questions"
                      " one at a time.")
    return got, [_words(line, passwords) for line in said]


def _stop(e):
    if isinstance(e, SystemExit):
        text = str(e.code) if e.code not in (None, 0) else "init stopped"
        return text.replace("MIGKIT_HOP=<another name> adds this one"
                            " beside it", "give the hop another name in"
                            " the Name field to add it beside it")
    return f"init stopped ({type(e).__name__}): {e}"


def _words(text, passwords):
    """Text as it leaves the page: the console's plain words, the
    passwords typed hidden, no program named."""
    from .. import export
    from ..redact import shapes
    text = _plain(text)
    for p in passwords:
        if p:
            text = text.replace(p, "***")
    return export.words(shapes(text))


def next_steps(got):
    """The commands init says come next, as it says them."""
    from .. import hopkeys
    goal = hopkeys.GOALS[got["goal"]]
    return [step.format(hop=got["name"]) for step in goal["next"]]


def offer(step):
    """(action, params, writes) of a next step the runner can start as it
    is written - checked by building the runner's own command line from
    the guess and comparing it to the step, word for word - or None:
    the button runs exactly what is said, never a near thing."""
    from . import runs
    words = step.split()
    if len(words) < 3 or words[0] != "migkit":
        return None
    argv = words[1:]
    params = {}
    n = 2
    while n < len(argv):
        flag = argv[n]
        key = FLAGS.get(flag)
        if key is None:
            return None
        if key in ("deep", "apply", "go", "drop"):
            params[key] = True
            n += 1
        elif n + 1 < len(argv):
            params[key] = argv[n + 1]
            n += 2
        else:
            return None
    for action in CANDIDATES.get(argv[0], ()):
        spec = runs.ACTIONS[action]
        mine = {k: v for k, v in params.items()
                if k in {o.name for o in spec.options}}
        try:
            built, got = spec.argv(argv[1], mine)
        except runs.Refused:
            continue
        if built == argv:
            return action, got, spec.writes(got)
    return None
