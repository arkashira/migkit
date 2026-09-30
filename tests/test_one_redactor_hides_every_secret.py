"""One redactor for every secret, wherever migkit writes or says it (audit
2026-10-01, SEC12, DU9).

There were five, each missing something - measured: an address with an
empty user kept its password, `password=` in a connection string or a
JDBC query was left, a password holding `/` or `@` broke the address
pattern, a password URL-encoded into a notification went out as it was.
`redact.Redactor` hides by value, in every form a secret reaches text in,
and by shape, and the debug log, `without_secret`, the notifications, the
diagnostics bundle and a failed command's message all go through it.
"""
import sys

import pytest

from migkit import config, diagnostics, redact, util, wording
from migkit.config import Endpoint, Hop
from migkit.redact import Redactor, shapes


@pytest.mark.parametrize("text,secret", [
    # the audit's measured misses
    ("redis://:s3cret@cache:6379/0", "s3cret"),
    ("host=h port=5432 dbname=d user=u password=CHANGE_ME sslmode=require",
     "CHANGE_ME"),
    ("jdbc:postgresql://h:5432/app?user=app&password=pw-CHANGE_ME&ssl=true",
     "pw-CHANGE_ME"),
    ("mysql://app:pa/ss@db:3306/app", "pa/ss"),
    ("postgres://u:p@ss@h:5432/app", "p@ss"),
    ("mongodb://app:pa/ss@h1:27017,h2:27017/app?replicaSet=rs0", "pa/ss"),
    # statements that carry another account's password
    ("CREATE USER 'app'@'%' IDENTIFIED BY 'se''cret';", "se''cret"),
    ("ALTER USER app IDENTIFIED WITH caching_sha2_password BY 'CHANGE_ME';",
     "CHANGE_ME"),
    ("alter user scott identified by tiger;", "tiger"),
    ("CREATE ROLE app LOGIN PASSWORD 'CHANGE_ME';", "CHANGE_ME"),
    ("create user mapping for current_user server s options (user 'u',"
     " password 'CHANGE_ME')", "CHANGE_ME"),
    ("CONFIG SET masterauth CHANGE_ME", "CHANGE_ME"),
    ("change replication source to SOURCE_HOST = 'h', SOURCE_PASSWORD ="
     " 'CHANGE_ME', GET_SOURCE_PUBLIC_KEY = 1;", "CHANGE_ME"),
    ("Driver={ODBC};Server=h;UID=sa;PWD={CHANGE;ME};", "CHANGE;ME"),
    ('{"password": "CHANGE_ME", "api_token": "tok-CHANGE_ME"}',
     "CHANGE_ME"),
    ("MYSQL_PWD=CHANGE_ME1 PGPASSWORD=CHANGE_ME2 client -h h", "CHANGE_ME"),
])
def test_a_secret_is_found_by_its_shape(text, secret):
    got = shapes(text)
    assert secret not in got, got
    assert "***" in got, got


def test_what_is_not_a_secret_is_left_as_it_was():
    """A redactor that hid everything would pass the test above: the rest
    of each line stands."""
    for text in ("password authentication failed for user app",
                 "host=db port=5432 key=id tokens=5",
                 "the saved token: 8263ab was not found in the log",
                 "https://hooks.example.com/services/T0/B0/X"):
        assert Redactor()(text) == text
    assert shapes("mysql://app:pa/ss@db:3306/app") == \
        "mysql://app:***@db:3306/app"
    assert shapes("jdbc:postgresql://h/app?user=app&password=x&ssl=true") \
        == "jdbc:postgresql://h/app?user=app&password=***&ssl=true"


def test_an_empty_password_closing_its_string_is_not_a_value():
    """A subscription for a hop that signs in without a password: its
    connection string ends `password='`, and the next quote is the slot's
    name - read as a quoted password, the statement would be shredded."""
    stmt = ("create subscription s connection 'host=h user=u password='"
            " publication p with (copy_data = true, slot_name = 'x')")
    assert Redactor()(stmt) == stmt


def test_a_secret_is_found_in_every_form_it_reaches_text_in():
    secret = "it's a/b"
    text = ("sh: 'it'\"'\"'s a/b' sql: 'it''s a/b' url: it%27s%20a%2Fb"
            " plus: it%27s+a%2Fb json: \"it's a/b\" backslashed: it\\'s a/b")
    got = Redactor(secrets=[secret], mask="#")(text)
    for form in ("it's", "it''s", "it%27s", "'\"'\"'s", "it\\'s"):
        assert form not in got, (form, got)
    assert got.startswith("sh: # sql: '#' url: # plus: # json: \"#\""), got


def test_what_the_hop_holds_is_hidden_without_asking_for_it(monkeypatch):
    def asked(*a, **k):
        raise AssertionError("the keychain was asked just to hide a secret")
    monkeypatch.setattr(config, "keychain_read", asked)
    unread = config.KeychainEndpoint(host="10.0.0.5", port=5432, user="app")
    unread.keychain = "hop.source"
    target = Endpoint(host="10.0.0.6", port=5432, user="app",
                      password="tgt-CHANGE_ME",
                      options={"mine_password": "mine-CHANGE_ME",
                               "ssl_key": "/keys/client.key"})
    hop = Hop(name="h", engine="postgres", source=unread, target=target,
              options={"notify": {"to": ["slack://T0/B0/hook-CHANGE_ME"],
                                  "events": ["verdict"]},
                       "tables": {"orders": {"key": "id"}}})
    said = ("tgt-CHANGE_ME mine-CHANGE_ME slack://T0/B0/hook-CHANGE_ME"
            " verdict orders id /keys/client.key")
    got = Redactor(hop)(said)
    assert "CHANGE_ME" not in got, got
    # a table's key column, an event's name and a file's path are not
    # secrets: hidden by value, every `id` would go
    assert got.endswith("verdict orders id /keys/client.key"), got
    assert unread.held_secrets() == []
    iam = config.IamEndpoint(host="10.0.0.5", port=5432, user="app")
    assert iam.held_secrets() == []


def test_the_environments_secrets_are_hidden(monkeypatch):
    monkeypatch.setenv("MIGKIT_AI_KEY", "sk-CHANGE_ME-123")
    monkeypatch.setenv("MIGKIT_AI_MODEL", "some-model")
    got = Redactor()("sent with sk-CHANGE_ME-123 to some-model")
    assert got == "sent with *** to some-model", got


def test_the_debug_log_and_without_secret_are_the_redactor():
    line = wording.redact(["x", "--source", "redis://:s3cret@cache/0"])
    assert line == "x --source redis://:***@cache/0", line
    # a password URL-encoded into an address, found by its value
    said = util.without_secret("postgres://app:p%40ss%2Fw@h/db", "p@ss/w")
    assert "p%40ss" not in said and "****" in said, said


def test_a_notification_hides_the_password_in_every_form():
    """Measured: a password URL-encoded into the text of an error went to
    the channel as it was."""
    from migkit import notify
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="10.0.0.5", port=5432, user="app",
                              password="p@ss/w-CHANGE_ME"),
              target=Endpoint(host="10.0.0.6", port=5432, user="app",
                              password=""))
    # the words go only where the hop's notify asks for them, and then
    # through the gate every send is cleaned at (`egress`)
    from migkit import egress
    hop.options = {"notify": {"detail": True}}
    got = egress.words(hop, notify._clean(
        hop, "could not connect to"
             " postgres://app:p%40ss%2Fw-CHANGE_ME@10.0.0.5/db"))
    assert "CHANGE_ME" not in got and "10.0.0.5" in got, got


def test_the_bundle_takes_whole_credentials_out():
    got = diagnostics.scrub({"a": "mysql://app:pa/ss@db",
                             "b": "postgres://u:p@ss@h", "c": "plain",
                             "password": "x"})
    assert got == {"a": "mysql://<removed>@db", "b": "postgres://<removed>@h",
                   "c": "plain", "password": "<removed>"}, got
    assert redact.SECRET_KEY.search("private_key_passphrase")
    assert not redact.SECRET_KEY.search("port")


def test_a_failed_command_says_why_without_its_command_line():
    """The command line carried hosts, users and the SQL, and a traceback
    printed it (audit 2026-10-01, W3)."""
    script = ("import sys; sys.stderr.write('password=CHANGE_ME_9 was"
              " rejected'); sys.exit(3)")
    argv = [sys.executable, "-c", script, "--flag=ARGV-CHANGE_ME"]
    with pytest.raises(util.CommandFailed) as e:
        util.run(argv, retries=0)
    said = str(e.value)
    assert "exit 3" in said and "was rejected" in said, said
    assert "CHANGE_ME" not in said and sys.executable not in said, said
    assert e.value.argv[-1] == "--flag=ARGV-CHANGE_ME"
    assert isinstance(e.value, RuntimeError)
