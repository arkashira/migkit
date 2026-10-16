"""The cloud's own sign-in and forwarders (backlog 38.real, R6.cloud).

A side naming `auth: rds-iam`, `cloudsql_iam` or `azure_managed_identity`
is given a token minted the way its provider documents - from the rung
`decide.cloud_sign_in` picks on what this machine has - through the egress
gate's `provider` kind, minted again before it ends (`decide.token_renew`),
and the token is in no text migkit writes. A side naming the cloud's own
forwarder is reached by the rung `decide.cloud_forwarder` picks from what
the endpoint names, the clients installed and whether it answers directly.

Stand-ins: the RDS signer runs on this machine from stand-in credentials;
the metadata and identity endpoints answer through `cloudapi._http` in the
providers' documented shapes - Google's `{"access_token", "expires_in",
"token_type"}` under `Metadata-Flavor: Google`, Azure's IMDS (api-version
2018-02-01, `Metadata: true`) and App Service endpoint (api-version
2019-08-01, `X-IDENTITY-HEADER`) with `expires_on` in epoch seconds. No
real account is asked (`cloud_readiness.UNPROVED`).
"""
import pytest

import migkit.config as cfg
from migkit import cloudapi, decide, egress, tunnel
from migkit.redact import Redactor
from tests import servers

CANARY = "ya29.CANARY-token-never-said-0123456789abcdef"


class Clock:
    def __init__(self):
        self.now = 1_800_000_000.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(cloudapi, "_clock", c)
    monkeypatch.setattr(cloudapi, "MINTS", {})
    monkeypatch.delenv("MIGKIT_EGRESS", raising=False)
    monkeypatch.delenv("MIGKIT_EGRESS_TO", raising=False)
    return c


def _hop(tmp_path, monkeypatch, auth, engine="postgres"):
    (tmp_path / "hops.yaml").write_text(
        f"hops:\n  cloud:\n    engine: {engine}\n"
        "    source: {host: db.example.com, port: 5432, user: app,"
        f" password: ignored, options: {{auth: {auth}}}}}\n"
        "    target: {host: 10.0.0.2, user: u, password: CHANGE_ME}\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    return cfg.get_hop("cloud")


def _answers(monkeypatch, *bodies):
    asked = []
    left = list(bodies)

    def http(url, headers=None, timeout=None):
        asked.append((url, dict(headers or {})))
        got = left.pop(0)
        if isinstance(got, Exception):
            raise got
        return got
    monkeypatch.setattr(cloudapi, "_http", http)
    return asked


def _google_here(monkeypatch):
    monkeypatch.setattr(cloudapi, "sign_in_facts", lambda kind: {
        "kind": kind, "sdk": False, "credentials": False, "metadata": True})


def test_rds_iam_is_signed_here_and_signed_again_before_its_end(
        tmp_path, monkeypatch, clock):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "CHANGE_ME-not-a-key")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-1")
    hop = _hop(tmp_path, monkeypatch, "rds-iam")
    assert hop.source.options["auth"] == "aws_iam"
    signed, real = [], cfg._aws_client
    monkeypatch.setattr(cfg, "_aws_client", lambda *a: signed.append(a[0])
                        or real(*a))
    first = hop.source.password
    assert "Action=connect" in first and "X-Amz-Signature=" in first
    assert hop.source.password == first and signed == ["rds"]
    clock.now += 599
    hop.source.password
    assert signed == ["rds"]
    clock.now += 2          # a third of 900 s left
    hop.source.password
    assert signed == ["rds", "rds"]


def test_cloud_sql_iam_takes_the_metadata_token_and_minted_again(
        tmp_path, monkeypatch, clock):
    _google_here(monkeypatch)
    asked = _answers(monkeypatch,
                     {"access_token": CANARY, "expires_in": 3599,
                      "token_type": "Bearer"},
                     {"access_token": CANARY + "-2", "expires_in": 3599,
                      "token_type": "Bearer"})
    hop = _hop(tmp_path, monkeypatch, "cloudsql_iam")
    assert hop.source.password == CANARY
    url, headers = asked[0]
    assert "sqlservice.login" in url and headers == {"Metadata-Flavor":
                                                     "Google"}
    clock.now += 3599 - 3599 / 3 - 1
    assert hop.source.password == CANARY and len(asked) == 1
    clock.now += 2
    assert hop.source.password == CANARY + "-2" and len(asked) == 2


@pytest.mark.parametrize("engine,audience", [
    ("postgres", "ossrdbms-aad.database.windows.net"),
    ("mssql", "database.windows.net%2F")])
def test_a_managed_identity_is_asked_where_the_host_gives_one(
        tmp_path, monkeypatch, clock, engine, audience):
    monkeypatch.setenv("IDENTITY_ENDPOINT", "http://localhost:41000/msi/token")
    monkeypatch.setenv("IDENTITY_HEADER", "CHANGE_ME")
    monkeypatch.setattr(cloudapi, "_installed", lambda m: False)
    asked = _answers(monkeypatch, {
        "access_token": CANARY, "token_type": "Bearer",
        "expires_on": str(int(clock.now) + 86399)})
    hop = _hop(tmp_path, monkeypatch, "azure_managed_identity", engine)
    assert hop.source.password == CANARY
    url, headers = asked[0]
    assert "api-version=2019-08-01" in url and audience in url
    assert headers == {"X-IDENTITY-HEADER": "CHANGE_ME"}
    assert hop.source.__dict__["_tok"].way == "identity endpoint"


def test_a_managed_identity_on_a_virtual_machine_asks_its_metadata(
        tmp_path, monkeypatch, clock):
    for k in ("IDENTITY_ENDPOINT", "IDENTITY_HEADER", "AZURE_CLIENT_ID"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(cloudapi, "_metadata_answers", lambda kind: True)
    asked = _answers(monkeypatch, {
        "access_token": CANARY, "expires_in": "86399",
        "expires_on": str(int(clock.now) + 86399), "token_type": "Bearer"})
    hop = _hop(tmp_path, monkeypatch, "azure_managed_identity")
    assert hop.source.password == CANARY
    assert asked[0][0].startswith(
        "http://169.254.169.254/metadata/identity/oauth2/token?"
        "api-version=2018-02-01")
    assert asked[0][1] == {"Metadata": "true"}


def test_the_token_is_in_no_text_migkit_writes(tmp_path, monkeypatch, clock):
    _google_here(monkeypatch)
    _answers(monkeypatch, {"access_token": CANARY, "expires_in": 3599},
             RuntimeError(f"denied while holding {CANARY}"))
    hop = _hop(tmp_path, monkeypatch, "cloudsql_iam")
    tok = hop.source.password
    url = hop.source.url("postgresql")
    assert tok in url          # the engines connect with it
    for text in (repr(hop.source), str(hop.source.__dict__["_tok"]),
                 Redactor(hop)(f"could not connect: {url} as {tok}"),
                 egress.words(hop, f"login failed: password={tok}")):
        assert CANARY not in text, text
    clock.now += 4000           # ended: the next mint fails
    with pytest.raises(cloudapi.SignInRefused) as e:
        hop.source.password
    assert CANARY not in str(e.value) and "Next:" in str(e.value)


@pytest.mark.parametrize("setting", ["none", "notify,evidence"])
def test_egress_closed_stops_the_mint_and_says_what_to_do(
        tmp_path, monkeypatch, clock, setting):
    _google_here(monkeypatch)
    asked = _answers(monkeypatch)
    monkeypatch.setenv("MIGKIT_EGRESS", setting)
    hop = _hop(tmp_path, monkeypatch, "cloudsql_iam")
    with pytest.raises(cloudapi.SignInRefused) as e:
        hop.source.password
    said = str(e.value)
    assert "cloud provider's own API" in said and "MIGKIT_EGRESS=provider" \
        in said and asked == []
    monkeypatch.setenv("MIGKIT_EGRESS", "provider")
    _answers(monkeypatch, {"access_token": CANARY, "expires_in": 3599})
    assert hop.source.password == CANARY


def test_no_way_to_mint_says_what_each_lacked(tmp_path, monkeypatch, clock):
    monkeypatch.setattr(cloudapi, "sign_in_facts", lambda kind: {
        "kind": kind, "sdk": False, "credentials": False, "metadata": False})
    hop = _hop(tmp_path, monkeypatch, "cloudsql_iam")
    with pytest.raises(cloudapi.SignInRefused) as e:
        hop.source.password
    assert "library is not installed" in str(e.value)
    assert "no metadata endpoint answers" in str(e.value)


def test_a_slow_signer_is_minted_again_earlier():
    assert decide.token_renew(900, [0.2])[0] == 300
    got, choice = decide.token_renew(900, [0.2, 200.0])
    assert got == 400 and choice.path == "measured"


def test_an_unknown_sign_in_is_refused(tmp_path, monkeypatch):
    with pytest.raises(SystemExit):
        _hop(tmp_path, monkeypatch, "kerberos-ticket")


# --- reached through the cloud's own forwarder (R6.cloud) ---------------------

PROGRAMS = ("aws", "ssm", "gcloud", "iap", "cloud-sql", "proxy", "az ",
            "session-manager")


def _facts(named, installed=(), answers=False, private=True):
    return {"named": named, "installed": {k: k in installed for k in named},
            "answers": answers, "private": private}


def test_the_forwarder_is_decided_from_what_is_installed_and_answers():
    assert decide.cloud_forwarder(_facts(["ssm"], ["ssm"])).path == "ssm"
    none = decide.cloud_forwarder(_facts(["ssm"]))
    assert none.path is None and "not installed" in none.reason
    assert decide.cloud_forwarder(_facts(["ssm"], answers=True)).path \
        == "direct"
    both = _facts(["ssm", "cloudsql"], ["ssm", "cloudsql"])
    assert decide.cloud_forwarder(both).path == "cloudsql"
    both["installed"]["cloudsql"] = False
    assert decide.cloud_forwarder(both).path == "ssm"
    public = decide.cloud_forwarder(_facts(["iap"], ["iap"], private=False))
    assert public.path == "iap" and "public" in public.rung.because
    assert "a private network's" in decide.cloud_forwarder(
        _facts(["iap"], ["iap"])).rung.because


def test_the_line_an_operator_reads_names_no_program():
    class Ep:
        host, port, options = "10.0.0.5", servers.private_port(0), {}
    with pytest.raises(SystemExit) as e:
        tunnel.forwarder(Ep(), {"ssm": "i-0abc"}, "source",
                         facts=_facts(["ssm"]))
    said = str(e.value).lower()
    assert "Next:" in str(e.value)
    assert not [p for p in PROGRAMS if p in said], said
    for way in ("cloudsql", "ssm", "iap", "bastion"):
        got = decide.cloud_forwarder(_facts([way], [way]))
        assert not [p for p in PROGRAMS if p in got.said.lower()], got.said
    assert tunnel.forwarder(Ep(), {"ssm": "i-0abc"}, "source",
                            facts=_facts(["ssm"], answers=True)) is None


def test_each_forwarder_runs_its_documented_command_line():
    ssm = tunnel.cloud_argv({"cloud": "ssm", "ssm": "i-0abc",
                             "region": "ap-southeast-1"}, "10.0.0.5", servers.private_port(0),
                            40001)
    assert "AWS-StartPortForwardingSessionToRemoteHost" in ssm
    assert "host=10.0.0.5,portNumber=5432,localPortNumber=40001" in ssm
    iap = tunnel.cloud_argv({"cloud": "iap", "iap": "db-vm", "zone": "z1"},
                            "10.0.0.5", servers.private_port(0), 40001)
    assert "--local-host-port=127.0.0.1:40001" in iap and "--zone=z1" in iap

    class Ep:
        options = {"auth": "cloudsql_iam"}
    sql = tunnel.cloud_argv({"cloud": "cloudsql", "cloudsql": "p:r:i"},
                            "10.0.0.5", servers.private_port(0), 40001, Ep())
    assert sql[-1] == "p:r:i" and "--private-ip" in sql \
        and "--auto-iam-authn" in sql
    assert "--private-ip" not in tunnel.cloud_argv(
        {"cloud": "cloudsql", "cloudsql": "p:r:i"}, "1.2.3.4", servers.private_port(0),
        40001)
    bastion = tunnel.cloud_argv({"cloud": "bastion", "bastion": "b",
                                 "resource_group": "rg", "target": "/vm"},
                                "10.0.0.5", servers.private_port(0), 40001)
    assert bastion[bastion.index("--resource-port") + 1] == "5432"


def test_a_closed_egress_opens_no_forwarder(monkeypatch):
    monkeypatch.setenv("MIGKIT_EGRESS", "none")

    class Ep:
        host, port = "10.0.0.5", servers.private_port(0)
        options = {"tunnel": {"ssm": "i-0abc"}}

    class Hop:
        name, options, workers = "h", {}, 1
        source, target = Ep(), type("T", (), {"options": {}})()
    with pytest.raises(SystemExit) as e:
        tunnel.through(Hop())
    assert "cloud provider's own API" in str(e.value)
