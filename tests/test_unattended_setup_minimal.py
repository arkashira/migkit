"""Tests for P0-VALIDATE.unattended.setup (#496) and P0-MINIMAL-INPUT.setup (#462):
Connection-only setup, automatic discovery from URI or env, private credentials.
"""
import os
from migkit.config import get_hop


def test_get_hop_from_connection_uri():
    hop = get_hop("postgres://app:env:MY_PW@db.corp.internal:5432/analytics")
    assert hop.name == "connection-only"
    assert hop.engine == "postgres"
    assert hop.databases == ["analytics"]
    assert hop.source.host == "db.corp.internal"
    assert hop.source.port == 5432
    assert hop.source.user == "app"


def test_get_hop_from_env_vars(monkeypatch):
    monkeypatch.setenv("MIGKIT_SOURCE", "postgres://user:env:PW1@src.internal:5432/db1")
    monkeypatch.setenv("MIGKIT_TARGET", "postgres://user:env:PW2@dst.internal:5432/db1")

    hop = get_hop("adhoc-run")
    assert hop.name == "adhoc-run"
    assert hop.engine == "postgres"
    assert hop.databases == ["db1"]
    assert hop.source.host == "src.internal"
    assert hop.target.host == "dst.internal"
