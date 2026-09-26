"""Accounts and clouds without long-lived passwords (backlog 38).

`auth: aws_iam` signs in to RDS and Aurora with a token the cloud signs,
made again before its 15 minutes run out. `aws-sm:`, `gcp-sm:` and
`azure-kv:` read a password from each cloud's secret store at load time.
Nothing here reaches a cloud: the token is signed on this machine from
stand-in credentials, and the secret store answers through the SDK's own
stub.
"""
import json

import pytest

import migkit.config as cfg


@pytest.fixture
def fake_aws(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "CHANGE_ME-not-a-key")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-1")


def _hop(tmp_path, monkeypatch, source):
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  cloud:\n    engine: postgres\n"
        f"    source: {source}\n"
        "    target: {host: 10.0.0.2, user: u, password: CHANGE_ME}\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    return cfg.get_hop("cloud")


def test_an_iam_endpoint_signs_a_token_and_makes_it_again(
        tmp_path, monkeypatch, fake_aws):
    hop = _hop(tmp_path, monkeypatch,
               "{host: db.example.rds.amazonaws.com, port: 5432, user: app,"
               " password: ignored, options: {auth: aws_iam}}")
    signed = []
    real_client = cfg._aws_client

    def counting(service, region=None, role=None):
        signed.append(service)
        return real_client(service, region, role)
    monkeypatch.setattr(cfg, "_aws_client", counting)
    first = hop.source.password
    assert "db.example.rds.amazonaws.com:5432/" in first, first
    assert "Action=connect" in first and "DBUser=app" in first, first
    assert "X-Amz-Signature=" in first and "ignored" not in first
    # the same token while it is young
    assert hop.source.password == first and signed == ["rds"]
    # and signed again once it is ten minutes old (the signature's own
    # clock is the SDK's, so the text can come out the same)
    import time
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 601)
    assert hop.source.password and signed == ["rds", "rds"], signed


def test_an_auth_it_does_not_know_is_refused(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match="password .the default. or aws_iam"):
        _hop(tmp_path, monkeypatch,
             "{host: h, user: u, options: {auth: kerberos}}")


def test_a_password_from_aws_secrets_manager(tmp_path, monkeypatch,
                                             fake_aws):
    import boto3
    from botocore.stub import Stubber
    client = boto3.client("secretsmanager", region_name="ap-southeast-1")
    stub = Stubber(client)
    # an example account, as AWS's own documentation writes one
    arn = ("arn:aws:secretsmanager:ap-southeast-1:123456789012:secret:"  # example
           "rds-app-AbCdEf")
    stub.add_response("get_secret_value",
                      {"SecretString": json.dumps({"username": "app",
                                                   "password": "CHANGE_ME-s1"}),
                       "ARN": arn, "Name": "rds-app"},
                      {"SecretId": arn})
    stub.add_response("get_secret_value",
                      {"SecretString": "CHANGE_ME-plain", "Name": "plain"},
                      {"SecretId": "plain"})
    stub.activate()
    asked = []

    def client_for(service, region=None, role=None):
        asked.append((service, region))
        return client
    monkeypatch.setattr(cfg, "_aws_client", client_for)
    hop = _hop(tmp_path, monkeypatch,
               "{host: 10.0.0.1, user: app, password: 'aws-sm:"
               + arn + "#password'}")
    assert hop.source.password == "CHANGE_ME-s1"
    # the region is the ARN's, not this machine's
    assert asked[-1] == ("secretsmanager", "ap-southeast-1"), asked
    assert cfg._secret("aws-sm:plain") == "CHANGE_ME-plain"
    stub.assert_no_pending_responses()


def test_a_missing_field_is_said(monkeypatch):
    class Client:
        def get_secret_value(self, SecretId):
            return {"SecretString": json.dumps({"user": "x"})}
    monkeypatch.setattr(cfg, "_aws_client", lambda s, r=None, o=None: Client())
    with pytest.raises(SystemExit, match="has no field 'password'"):
        cfg._secret("aws-sm:s#password")


def test_the_other_clouds_say_what_they_need_when_it_is_not_there(
        monkeypatch):
    import builtins
    real = builtins.__import__

    def without_sdks(name, *a, **k):
        if name.startswith(("google.cloud", "azure")):
            raise ImportError(name)
        return real(name, *a, **k)
    monkeypatch.setattr(builtins, "__import__", without_sdks)
    with pytest.raises(SystemExit, match="google-cloud-secret-manager"):
        cfg._secret("gcp-sm:projects/p/secrets/s")
    with pytest.raises(SystemExit, match="azure-keyvault-secrets"):
        cfg._secret("azure-kv:https://v.vault.azure.net/secrets/pw")


def test_a_role_in_another_account_is_assumed_for_the_token(
        tmp_path, monkeypatch, fake_aws):
    import datetime

    import boto3
    from botocore.stub import Stubber
    sts = boto3.client("sts", region_name="ap-southeast-1")
    stub = Stubber(sts)
    role = "arn:aws:iam::210987654321:role/migkit-reader"  # example
    stub.add_response("assume_role", {
        "Credentials": {"AccessKeyId": "ASIAEXAMPLEROLEXYZ",
                        "SecretAccessKey": "CHANGE_ME-role-secret",
                        "SessionToken": "CHANGE_ME-session",
                        "Expiration": datetime.datetime(2030, 1, 1)}},
        {"RoleArn": role, "RoleSessionName": "migkit"})
    stub.activate()
    real = boto3.client

    def client(service, **kw):
        return sts if service == "sts" else real(service, **kw)
    monkeypatch.setattr(boto3, "client", client)
    hop = _hop(tmp_path, monkeypatch,
               "{host: db.example.rds.amazonaws.com, port: 5432, user: app,"
               f" options: {{auth: aws_iam, aws_role_arn: '{role}',"
               " aws_region: ap-southeast-1}}")
    token = hop.source.password
    # signed with the role's credentials, not this machine's
    assert "ASIAEXAMPLEROLEXYZ" in token and "AKIDEXAMPLE" not in token, token
    assert "X-Amz-Security-Token=" in token, token
    stub.assert_no_pending_responses()
