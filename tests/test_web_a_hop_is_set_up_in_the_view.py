"""A hop set up from the view (backlog R21.8): two addresses with a user
and a password each, and the page does what `migkit init` does by calling
the guide itself.

Held: the page renders for an operator and an admin and is refused to
anyone else, in words; a form not from the view's page is refused; a
post with bad input is refused before any server is asked, with the
next step said, and nothing typed as a password comes back; a post with
a fake init result writes through the guide's own entry point (no
terminal, the passwords in its mapping and kept in the keychain, never in
an address, a file or this process's environment), shows what init said
and lists the hop; the first command is a button the runner starts as
written; a person whose role does not cover the name cannot make the
hop; and with the real guide, on two files, the hop is made offline.
"""
import html
import os
import re
import sqlite3
import sys

import pytest

from migkit import guide
from migkit.web import newhop, runs
from tests.web_stand_ins import (URL, browser, call, get, place, post,
                                 signed_in, view)
from tests.web_stand_ins import web  # noqa: F401 - the fixture

HEADERS = {"Origin": URL, "Sec-Fetch-Site": "same-origin"}
ROOT = {"id": "root@example.com", "via": "oidc"}
ANN = {"id": "ann@example.com", "via": "oidc"}
CAROL = {"id": "carol@example.com", "via": "oidc"}
PASSWORD = "CHANGE_ME-typed-in-the-box"
GOOD = {"source": "postgres://db1.example.com:5432/shop",
        "target": "mysql://db2.example.com/shop"}


class FakeGuide:
    """`guide.run`, standing in: what it is told is kept, what it does is
    what the real one does to the file and to its caller."""

    def __init__(self, monkeypatch, name="fresh", goal="verify",
                 keychain=True):
        self.calls = []
        self.name, self.goal = name, goal
        monkeypatch.setattr(guide, "run", self.run)
        monkeypatch.setattr(guide, "keychain_usable", lambda: keychain)

    def run(self, ask, say, path, force=False, env=None):
        self.calls.append({"ask": ask, "path": path, "force": force,
                           "env": dict(env)})
        name = env.get("MIGKIT_HOP") or self.name
        say("what is there")
        say("[green]source[/green]: postgres 16, a primary")
        say("target: mysql 8.4, a primary")
        with open(path, "a") as f:
            f.write(f"  {name}:\n    source: {env['MIGKIT_SOURCE']}\n"
                    f"    target: {env['MIGKIT_TARGET']}\n")
        say(f"wrote hop {name} to {path} (3 lines)")
        say(f"  migkit check {name}")
        return {"path": path, "name": name, "questions": [],
                "goal": env.get("MIGKIT_GOAL") or self.goal, "config": {},
                "found": {}, "scripts": []}


class Never:
    """The guide, armed to fail the test if anything reaches it."""

    def __init__(self, monkeypatch):
        self.calls = []

        def run(*args, **kw):
            self.calls.append((args, kw))
            raise AssertionError("the guide was reached")
        monkeypatch.setattr(guide, "run", run)


class Started:
    """Popen, standing in: what the runner would have started."""

    def __init__(self):
        self.calls = []

    def __call__(self, args, **kw):
        self.calls.append((args, kw))

        class P:
            pid = 999_999

            def wait(self):
                return 0
        return P()


def _form(csrf, **over):
    return {"csrf": csrf, **GOOD, **over}


def _post(c, data):
    return post(c, "/new-hop", data=data, headers=HEADERS)


def _errors(response):
    return [html.unescape(m) for m in re.findall(
        r'<p class="note error">([^<]+)</p>',
        response.get_data(as_text=True))]


def _conf():
    return open(os.environ["MIGKIT_CONF"]).read()


def test_the_page_renders_the_form_for_an_admin_and_an_operator(web):
    for who in (ROOT, ANN):
        c = browser(web)
        signed_in(web, c, who)
        r = get(c, "/new-hop")
        body = r.get_data(as_text=True)
        assert r.status_code == 200
        for field in ("source", "source_user", "source_password", "target",
                      "target_user", "target_password", "name", "goal"):
            assert f'name="{field}"' in body
        # a password is typed in a password box with no value of its own
        assert body.count('type="password"') == 2
        assert not re.search(r'type="password"[^>]*value=', body)
        assert 'method="post"' in body and 'name="csrf"' in body
        assert 'href="/new-hop"' in get(c, "/").get_data(as_text=True)
        for goal in ("copy", "cutover", "verify"):
            assert f'<option value="{goal}"' in body


def test_a_person_with_no_operator_role_is_refused_in_words(web,
                                                            monkeypatch):
    c = browser(web)
    csrf = signed_in(web, c, CAROL)
    r = get(c, "/new-hop")
    assert r.status_code == 403
    assert "setting up a hop is for an operator of a hop or a global admin" \
        in html.unescape(r.get_data(as_text=True))
    assert 'href="/new-hop"' not in get(c, "/").get_data(as_text=True)
    never = Never(monkeypatch)
    before = _conf()
    r = _post(c, _form(csrf, name="hcarol"))
    assert r.status_code == 403
    assert never.calls == [] and _conf() == before
    assert '"why": "not an operator"' in web.record_path.read_text()


@pytest.mark.parametrize("over,says,next_step", [
    ({"source": ""}, "the source address is empty", "write it like"),
    ({"target": "   "}, "the target address is empty", "write it like"),
    ({"source": "/etc/hops/task.json"}, "is not an address",
     "a task file is read at a terminal"),
    ({"source": "postgres://app:pw@db1/shop"}, "holds a password",
     "type it in the password field"),
    ({"target": "mysql://app:env:DB_PASSWORD@db2/shop"}, "holds a password",
     "type it in the password field"),
    ({"source": "postgres://db1/shop?password=x"}, "carries a secret",
     "passwords go in the password field"),
    ({"source": "oracle9000://db1/shop"}, "names no engine migkit knows",
     "write it like"),
    ({"source": "postgres://app@db1/shop", "source_user": "someone"},
     "they differ", "keep one"),
    ({"name": "bad name!"}, "is not one a hop takes", "leave it empty"),
    ({"goal": "teleport"}, "is none of", "leave it empty"),
    ({"source": "postgres://db1/shop\nx"}, "holds a line break",
     "write it on one line"),
    ({"source_password": "x" * 2000}, "password is longer", "check what"),
])
def test_bad_input_is_refused_before_any_server_is_asked(
        web, monkeypatch, over, says, next_step):
    c = browser(web)
    csrf = signed_in(web, c, ROOT)
    never = Never(monkeypatch)
    before = _conf()
    r = _post(c, _form(csrf, **{"source_password": PASSWORD,
                                "target_password": PASSWORD, **over}))
    assert r.status_code == 422
    said = " ".join(_errors(r))
    assert says in said, said
    assert next_step in said, said
    assert never.calls == [] and _conf() == before
    # nothing typed as a password comes back to the browser
    assert PASSWORD not in r.get_data(as_text=True)


def test_a_form_not_from_the_views_page_is_refused(web, monkeypatch):
    c = browser(web)
    csrf = signed_in(web, c, ROOT)
    never = Never(monkeypatch)
    r = post(c, "/new-hop", data=_form("not-the-token"), headers=HEADERS)
    assert r.status_code == 403
    r = post(c, "/new-hop", data=_form(csrf),
             headers={"Origin": "http://evil.example",
                      "Sec-Fetch-Site": "cross-site"})
    assert r.status_code == 403
    assert never.calls == []
    # with no session, the sign-in page is where it goes
    r = post(browser(web), "/new-hop", data=_form(csrf), headers=HEADERS)
    assert r.status_code == 303 and "/signin" in r.headers["Location"]


def test_a_fake_init_result_creates_the_hop_and_lists_it(web, monkeypatch):
    fake = FakeGuide(monkeypatch)
    c = browser(web)
    csrf = signed_in(web, c, ROOT)
    r = _post(c, _form(csrf, source_user="app", source_password=PASSWORD,
                       target_user="app target", target_password=PASSWORD))
    body = r.get_data(as_text=True)
    assert r.status_code == 200, _errors(r)
    # the guide's own entry point, as `init` calls it with no terminal
    (got,) = fake.calls
    assert got["ask"] is None and got["force"] is False
    assert str(got["path"]) == os.environ["MIGKIT_CONF"]
    env = got["env"]
    assert env["MIGKIT_SOURCE"] == "postgres://app@db1.example.com:5432/shop"
    assert env["MIGKIT_TARGET"] == \
        "mysql://app%20target@db2.example.com/shop"
    assert env["MIGKIT_SOURCE_PASSWORD"] == PASSWORD
    assert env["MIGKIT_TARGET_PASSWORD"] == PASSWORD
    assert env["MIGKIT_SECRETS"] == "keychain"
    # a password is in the guide's mapping, not this process's, and not in
    # the file, the page or the record
    assert PASSWORD not in "".join(os.environ.values())
    assert PASSWORD not in _conf()
    assert PASSWORD not in body
    assert PASSWORD not in web.record_path.read_text()
    # what init said, as init prints it: its markup taken out
    assert "source: postgres 16, a primary" in body
    assert "[green]" not in body
    assert "Hop fresh is set up" in body
    # and it is a hop of the view now
    assert 'href="/hops/fresh"' in get(c, "/").get_data(as_text=True)
    assert get(c, "/hops/fresh").status_code == 200
    assert '"op": "hop-created"' in web.record_path.read_text()


def test_the_first_command_is_a_button_the_runner_starts_as_written(
        web, monkeypatch):
    FakeGuide(monkeypatch, goal="verify")
    c = browser(web)
    csrf = signed_in(web, c, ROOT)
    body = _post(c, _form(csrf)).get_data(as_text=True)
    assert 'data-api="/api/v1/hops/fresh/runs"' in body
    assert "Run migkit check fresh" in body
    # what the page's script posts: the form's named fields, as JSON
    form = body[body.index('data-api="/api/v1/hops/fresh/runs"'):]
    form = form[:form.index("</form>")]
    posted = dict(re.findall(r'<input type="hidden" name="(\w+)"'
                             r' value="([^"]*)"', form))
    assert posted == {"action": "check"}
    started = Started()
    monkeypatch.setattr(runs.subprocess, "Popen", started)
    r = call(c, "POST", "/api/v1/hops/fresh/runs", csrf, posted)
    assert r.status_code == 202, r.get_data(as_text=True)
    assert r.get_json()["argv"] == ["check", "fresh"]
    (args, _), = started.calls
    assert args[:3] == [sys.executable, "-m", "migkit.web.runner"]


def test_a_writing_first_command_asks_before_it_runs(web, monkeypatch):
    FakeGuide(monkeypatch)
    monkeypatch.setattr(newhop, "next_steps", lambda got: [
        "migkit move fresh --mode full --go"])
    c = browser(web)
    csrf = signed_in(web, c, ROOT)
    body = _post(c, _form(csrf)).get_data(as_text=True)
    assert 'name="action" value="move"' in body
    assert 'data-confirm="Run migkit move fresh --mode full --go? It' \
        ' writes."' in body


@pytest.mark.parametrize("step,want", [
    ("migkit check fresh", ("check", {}, False)),
    ("migkit assess fresh", ("assess", {}, False)),
    ("migkit move fresh --mode full", ("plan", {"mode": "full"}, False)),
    ("migkit move fresh --mode full --go",
     ("move", {"mode": "full"}, True)),
    ("migkit move fresh --mode cdc --drop --go",
     ("cutover", {"go": True}, True)),
    ("migkit sync fresh --mode migrate",
     ("sync", {"mode": "migrate"}, False)),
    ("migkit sync fresh --mode migrate --go",
     ("sync", {"mode": "migrate", "go": True}, True)),
    # not startable as written: a flag the runner does not carry
    ("migkit sync fresh --mode migrate --go --serve", None),
    ("migkit watch fresh", None),
    ("make coffee now", None),
])
def test_a_step_is_offered_only_as_the_runner_would_run_it(step, want):
    got = newhop.offer(step)
    if want is None:
        assert got is None
        return
    action, params, writes = got
    assert (action, writes) == (want[0], want[2])
    for k, v in want[1].items():
        assert params[k] == v
    # the command line the runner builds is the step, word for word
    argv, _ = runs.ACTIONS[action].argv("fresh", params)
    assert argv == step.split()[1:]


def test_a_role_that_does_not_cover_the_name_cannot_make_the_hop(
        web, monkeypatch):
    fake = FakeGuide(monkeypatch)
    c = browser(web)
    csrf = signed_in(web, c, ANN)
    # ann is an operator of hops named "h*" and no others: no name, or a
    # name the file does not give her, is not hers to make
    for name, says in (("", "give the hop a name your role covers"),
                       ("billing", "your role does not cover a hop called")):
        r = _post(c, _form(csrf, name=name))
        assert r.status_code == 422
        assert says in " ".join(_errors(r))
    assert fake.calls == []
    assert "billing" not in _conf()
    assert web.record_path.read_text().count(
        '"why": "no operator role on that name"') == 2
    # the name she holds operator on is hers, and so is the button
    r = _post(c, _form(csrf, name="hnew"))
    body = r.get_data(as_text=True)
    assert r.status_code == 200, _errors(r)
    assert "Hop hnew is set up" in body
    assert "Run migkit check hnew" in body
    assert fake.calls[0]["env"]["MIGKIT_HOP"] == "hnew"
    # an admin needs no name
    c2 = browser(web)
    csrf2 = signed_in(web, c2, ROOT)
    assert _post(c2, _form(csrf2, source="postgres://other.example.com/shop")
                 ).status_code == 200


def test_a_machine_with_no_keychain_keeps_no_password_it_was_not_asked_to(
        web, monkeypatch):
    fake = FakeGuide(monkeypatch, keychain=False)
    c = browser(web)
    csrf = signed_in(web, c, ROOT)
    r = _post(c, _form(csrf, source_password=PASSWORD))
    assert r.status_code == 422
    said = " ".join(_errors(r))
    assert "no keychain migkit can use" in said
    assert "run `migkit init` at a terminal" in said
    assert fake.calls == []
    assert PASSWORD not in r.get_data(as_text=True)
    # with no password to keep there is nothing to refuse
    assert _post(c, _form(csrf)).status_code == 200
    assert "MIGKIT_SECRETS" not in fake.calls[0]["env"]


def test_what_init_refuses_is_said_with_the_next_step(web, monkeypatch):
    def refuses(ask, say, path, force=False, env=None):
        raise SystemExit(f"{path} already has a hop shop going somewhere"
                         " else (MIGKIT_HOP=<another name> adds this one"
                         " beside it)")
    monkeypatch.setattr(guide, "run", refuses)
    c = browser(web)
    csrf = signed_in(web, c, ROOT)
    r = _post(c, _form(csrf))
    assert r.status_code == 422
    said = " ".join(_errors(r))
    assert "already has a hop shop going somewhere else" in said
    assert "Nothing was written" in said
    assert "MIGKIT_HOP" not in said and "the Name field" in said


def test_the_real_guide_makes_a_hop_from_two_files_offline(tmp_path,
                                                           monkeypatch):
    place(tmp_path, monkeypatch, hops="")
    app = view(tmp_path)
    try:
        for n in ("a", "b"):
            db = sqlite3.connect(tmp_path / f"{n}.db")
            db.execute("create table t (id integer primary key, v text)")
            db.execute("insert into t values (1, 'x')")
            db.commit()
            db.close()
        c = browser(app)
        csrf = signed_in(app, c, {"id": "admin", "via": "local"})
        r = _post(c, {"csrf": csrf, "source": f"sqlite:///{tmp_path}/a.db",
                      "target": f"sqlite:///{tmp_path}/b.db",
                      "name": "files"})
        body = r.get_data(as_text=True)
        assert r.status_code == 200, _errors(r)
        assert "Hop files is set up" in body
        assert "what is there" in body and "wrote hop files" in body
        assert "Run migkit" in body
        text = (tmp_path / "hops.yaml").read_text()
        assert "files:" in text and f"{tmp_path}/a.db" in text
        assert 'href="/hops/files"' in get(c, "/").get_data(as_text=True)
        # the same two files the other way round under the same name is
        # another hop of that name: refused, and the file as it was
        again = _post(c, {"csrf": csrf,
                          "source": f"sqlite:///{tmp_path}/b.db",
                          "target": f"sqlite:///{tmp_path}/a.db",
                          "name": "files"})
        assert again.status_code == 422
        assert "Nothing was written" in " ".join(_errors(again))
        assert text == (tmp_path / "hops.yaml").read_text()
    finally:
        app.store.close()
