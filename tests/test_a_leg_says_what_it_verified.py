from types import SimpleNamespace

from migkit import cli, movers, tls


class _Eng:
    ENGINE_FAMILY = "postgres"
    CANON_ENGINE = "postgres"

    def __init__(self, got, mapped):
        self.got, self.mapped, self.asked = got, mapped, []

    def leg_encryption(self, side, db):
        return self.got

    def _d(self, side, db):
        return self.mapped.get(db, db)

    def _psql(self, side, db, sql):
        self.asked.append(db)
        return ""


def _said(got, posture, monkeypatch):
    monkeypatch.setattr(tls, "on_this_machine", lambda ep: False)
    monkeypatch.setattr(tls, "posture", lambda ep, fam: posture)
    ep = SimpleNamespace(options={})
    return cli._leg_said(_Eng(got, {}), "src", "d", ep)


def test_a_connection_the_engine_says_was_not_verified_is_not_called_verified(
        monkeypatch):
    """The hop's settings asked for a checked certificate, the connection
    the engine opened did not check one: doctor used to say verified."""
    said = _said({"encrypted": True, "how": "TLS 1.3", "verified": False,
                  "unverified": []}, {"state": "verified", "detail": "x"},
                 monkeypatch)
    assert "the certificate verified" not in said


def test_paths_left_unverified_are_named(monkeypatch):
    said = _said({"encrypted": True, "how": "TLS 1.3", "verified": True,
                  "unverified": ["the target pulling from the source"]},
                 {"state": "verified", "detail": "x"}, monkeypatch)
    assert "the certificate verified" in said
    assert "not verified on: the target pulling from the source" in said


def test_an_engine_that_cannot_say_falls_back_to_the_hop(monkeypatch):
    said = _said({"encrypted": True, "how": "TLS"},
                 {"state": "verified", "detail": "x"}, monkeypatch)
    assert "the certificate verified" in said


def test_the_origin_is_made_in_the_database_the_engine_maps_once():
    eng = _Eng(None, {"app": "app_target"})
    movers._seed_origin(eng, "app", "leg", "0/0")
    assert eng.asked == ["app", "app"]
