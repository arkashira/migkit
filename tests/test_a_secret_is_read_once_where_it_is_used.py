"""A hop's secrets are read where a connection needs them, once, and a
failure names where it was written (audit 2026-10-01, K7, K12, K22, K24).

Measured before: every hop's secrets were read on every load, several
loads a command, so hop b's unset variable stopped `get_hop("a")` with
"secret env var '...' is not set" - naming neither hop nor side; a
secret kept in a side's options (`mine_password`, a key's passphrase, a
registry's password) went out as the text of its reference; and Vault
was read through its program only, a version 2 store's layout only.
"""
import base64
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from migkit import config, registry
from migkit.config import Endpoint, Hop

TEXT = """hops:
  shop:
    engine: postgres
    source: {host: 10.0.0.1, user: app, password: "env:SHOP_SRC_PW"}
    target: {host: 10.0.0.2, user: app, password: "env:SHOP_DST_PW"}
  crm:
    engine: postgres
    source: {host: 10.0.0.3, user: app, password: "env:CRM_UNSET_PW"}
    target: {host: 10.0.0.4, user: app, password: "env:CRM_UNSET_PW"}
"""


@pytest.fixture
def conf(tmp_path, monkeypatch):
    path = tmp_path / "hops.yaml"
    path.write_text(TEXT)
    os.chmod(path, 0o600)
    monkeypatch.setattr(config, "CONF", path)
    monkeypatch.setenv("SHOP_SRC_PW", "CHANGE_ME-src")
    monkeypatch.setenv("SHOP_DST_PW", "CHANGE_ME-dst")
    monkeypatch.delenv("CRM_UNSET_PW", raising=False)
    return path


def test_one_hops_missing_secret_stops_only_that_hop(conf):
    hop = config.get_hop("shop")
    assert hop.source.password == "CHANGE_ME-src"
    # every hop built, none of its secrets read
    hops = config.load_hops()
    assert set(hops) == {"shop", "crm"}
    crm = config.get_hop("crm")
    with pytest.raises(SystemExit) as e:
        crm.source.password
    said = str(e.value)
    assert said.startswith("hops.crm.source.password: the secret"
                           " env:CRM_UNSET_PW could not be read from the"
                           " environment"), said


def test_a_secret_is_read_the_first_time_it_is_used(conf, monkeypatch):
    hop = config.get_hop("shop")
    # nothing read yet, so nothing held to hide
    assert hop.source.held_secrets() == []
    monkeypatch.setenv("SHOP_SRC_PW", "CHANGE_ME-later")
    assert hop.source.password == "CHANGE_ME-later"
    monkeypatch.setenv("SHOP_SRC_PW", "CHANGE_ME-changed")
    # once read, kept
    assert hop.source.password == "CHANGE_ME-later"
    assert hop.source.held_secrets() == ["CHANGE_ME-later"]
    assert "CHANGE_ME" not in repr(hop.source)


def test_a_copy_of_a_side_reads_its_own_secret(conf):
    """A copy made before either was read shares nothing it can lose:
    the side that reads first leaves the other's reference in place."""
    import copy
    hop = config.get_hop("shop")
    twin = copy.copy(hop.source)
    assert hop.source.password == "CHANGE_ME-src"
    assert twin.password == "CHANGE_ME-src"


def test_a_name_that_is_not_a_hop_is_said_with_the_nearest(conf):
    with pytest.raises(SystemExit) as e:
        config.get_hop("shpo")
    assert "unknown hop shpo - did you mean shop?" in str(e.value)
    with pytest.raises(SystemExit) as e:
        config.get_hop("zzz")
    assert "did you mean" not in str(e.value)
    assert "have: shop, crm" in str(e.value)


def test_the_file_is_read_again_only_when_it_changes(conf):
    first, _ = config._checked()
    again, _ = config._checked()
    assert again is first
    conf.write_text(TEXT.replace("10.0.0.1", "10.0.0.9"))
    changed, _ = config._checked()
    assert changed is not first
    assert config.get_hop("shop").source.host == "10.0.0.9"
    # a hop built is the caller's own: changing it changes no other
    hop = config.get_hop("shop")
    hop.options["on_conflict"] = "keep-target"
    assert "on_conflict" not in config.get_hop("shop").options


def test_a_secret_kept_in_a_sides_options_is_read(monkeypatch):
    monkeypatch.setenv("REG_PW", "CHANGE_ME-reg")
    ep = Endpoint(host="10.0.0.5", port=9092,
                  options={"schema_registry": "http://10.0.0.5:8081",
                           "schema_registry_user": "u",
                           "schema_registry_password": "env:REG_PW"})
    got = registry.of(ep)
    assert base64.b64decode(got._auth.split()[1]).decode() == \
        "u:CHANGE_ME-reg"
    assert "CHANGE_ME-reg" in ep.held_secrets()
    monkeypatch.delenv("REG_PW")
    side = config._endpoint("kafka", {"host": "h", "options": {
        "schema_registry_password": "env:REG_PW"}}, where="hops.k.source")
    with pytest.raises(SystemExit, match="^hops.k.source"
                                         ".schema_registry_password:"):
        side.option_secret("schema_registry_password")


# ---- K22: Vault over its own API, either version of its store -------------

class _Vault(BaseHTTPRequestHandler):
    KEPT = {
        "sys/internal/ui/mounts/secret/app": {
            "data": {"path": "secret/", "options": {"version": "2"}}},
        "secret/data/app": {"data": {"data": {"pw": "CHANGE_ME-v2"},
                                     "metadata": {"version": 3}}},
        "sys/internal/ui/mounts/kv/app": {
            "data": {"path": "kv/", "options": {"version": "1"}}},
        "kv/app": {"data": {"pw": "CHANGE_ME-v1"}},
    }

    def do_GET(self):
        path = self.path[len("/v1/"):]
        if self.headers.get("X-Vault-Token") != "CHANGE_ME-token":
            self._send(403, {"errors": ["permission denied"]})
        elif path in self.KEPT:
            self._send(200, self.KEPT[path])
        else:
            self._send(404, {"errors": []})

    def _send(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):
        pass


@pytest.fixture
def vault(monkeypatch):
    server = HTTPServer(("127.0.0.1", 0), _Vault)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("VAULT_ADDR", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("VAULT_TOKEN", "CHANGE_ME-token")
    monkeypatch.delenv("VAULT_NAMESPACE", raising=False)
    yield server
    server.shutdown()


def test_vault_is_read_over_its_api_in_either_version(vault, monkeypatch):
    assert config._secret("vault:secret/app#pw") == "CHANGE_ME-v2"
    # a version 1 store ended in KeyError
    assert config._secret("vault:kv/app#pw") == "CHANGE_ME-v1"
    with pytest.raises(SystemExit, match="the secret vault:kv/app#user"
                                         " could not be read from Vault:"
                                         " kv/app holds no field 'user'"):
        config._secret("vault:kv/app#user")
    monkeypatch.setenv("VAULT_TOKEN", "CHANGE_ME-wrong")
    with pytest.raises(SystemExit, match="Vault answered 403: permission"
                                         " denied"):
        config._secret("vault:kv/app#pw")


def test_every_store_says_a_failure_one_way(tmp_path, monkeypatch):
    monkeypatch.delenv("NOT_SET_HERE", raising=False)
    with pytest.raises(SystemExit, match="^the secret env:NOT_SET_HERE could"
                                         " not be read from the environment:"
                                         " the variable NOT_SET_HERE is not"
                                         " set$"):
        config._secret("env:NOT_SET_HERE")
    gone = tmp_path / "gone"
    with pytest.raises(SystemExit, match="^the secret file:.* could not be"
                                         " read from a file: "):
        config._secret(f"file:{gone}")
    # where it was written is named in front of it
    with pytest.raises(SystemExit, match="^hops.h.target.password: the"
                                         " secret env:NOT_SET_HERE"):
        config.secret("env:NOT_SET_HERE", "hops.h.target.password")


def test_a_hop_made_in_code_keeps_its_values_as_they_are():
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="10.0.0.1", password="env:NOT_A_REF_HERE"),
              target=Endpoint(host="10.0.0.2"))
    # a hop built in code is not read through references
    assert hop.source.password == "env:NOT_A_REF_HERE"
