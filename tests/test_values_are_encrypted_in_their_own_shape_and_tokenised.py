"""Format-preserving encryption and tokens as transforms (backlog W4.b).

`fpe` encrypts a value in its own shape and length (FF1, held to the
published test vectors); `token` stands a value in for a token kept in the
user's vault, reversible by the holder of the key; which of mask, fpe and
token a column takes is `decide.hide_column`'s. No server.
"""
import pytest

from migkit import decide, masking, transforms


@pytest.fixture(autouse=True)
def state(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    masking._SECRETS.clear()
    transforms._VAULTS.clear()
    yield tmp_path / "state" / "migkit"
    for db, _ in transforms._VAULTS.values():
        db.close()
    transforms._VAULTS.clear()
    masking._SECRETS.clear()


KEY = bytes.fromhex("2B7E151628AED2A6ABF7158809CF4F3C")


def _ff1(key, tweak, text, alphabet, decrypt=False):
    digits = [alphabet.index(c) for c in text]
    out = transforms._ff1(key, tweak, digits, len(alphabet), decrypt)
    return "".join(alphabet[d] for d in out)


def test_the_cipher_is_ff1_by_the_published_vectors():
    # NIST SP 800-38G sample vectors, AES-128, radix 10 and radix 36
    d10, d36 = "0123456789", "0123456789abcdefghijklmnopqrstuvwxyz"
    assert _ff1(KEY, b"", "0123456789", d10) == "2433477484"
    assert _ff1(KEY, bytes.fromhex("39383736353433323130"),
                "0123456789", d10) == "6124200773"
    assert _ff1(KEY, bytes.fromhex("3737373770717273373737"),
                "0123456789abcdefghi", d36) == "a9tv40mll9kdu509eum"
    assert _ff1(KEY, b"", "2433477484", d10, decrypt=True) == "0123456789"


def test_fpe_round_trips_and_keeps_shape_and_length():
    for rule, value in (("fpe", "4111111111111111"), ("fpe", "0012345678"),
                        ("fpe alnum", "Ab12Cd34Ef"),
                        ("fpe upper", "ABCDEFGHIJ"),
                        ("fpe xyz01", "xyz01xyz01xyz01xyz01")):
        got = transforms.apply(rule, value)
        chars = transforms.alphabet(rule.partition(" ")[2])
        assert len(got) == len(value) and set(got) <= set(chars)
        assert got != value
        assert transforms.reverse(rule, got) == value
        # equal in, equal out: a key stays unique and a join still lines up
        assert transforms.apply(rule, value) == got
    assert transforms.apply("fpe", None) is None


def test_fpe_is_unique_over_a_set_of_keys_and_depends_on_the_key():
    values = [f"{n:08d}" for n in range(0, 3000, 7)]
    a = [transforms.apply("fpe", v) for v in values]
    assert len(set(a)) == len(values)
    other = [transforms.apply("fpe", v, key="a hop's key") for v in values]
    assert other != a
    assert [transforms.apply("fpe", v, key="a hop's key") for v in values] \
        == other
    assert transforms.reverse("fpe", other[3], key="a hop's key") == values[3]
    assert transforms.reverse("fpe", other[3]) != values[3]


def test_a_value_outside_the_alphabet_is_refused_by_name():
    with pytest.raises(SystemExit) as e:
        transforms.apply("fpe", "12a4567890")
    said = str(e.value)
    assert "fpe" in said and "'digits'" in said and "position 3" in said
    assert "12a4567890" not in said  # the value is not repeated
    with pytest.raises(SystemExit, match="'lower'"):
        transforms.apply("fpe lower", "abcDef")
    with pytest.raises(SystemExit, match="neither digits"):
        transforms.parse("fpe aab")


def test_a_value_too_short_to_hold_the_encryption_is_refused():
    with pytest.raises(SystemExit, match="fewer than the 1,000,000"):
        transforms.apply("fpe", "1234")
    assert len(transforms.apply("fpe", "123456")) == 6


def test_a_token_is_stable_and_reversible_only_with_the_key():
    a = transforms.apply("token", "ann@example.com")
    assert a == transforms.apply("token", "ann@example.com")
    assert a.startswith("tok-") and "ann" not in a
    assert transforms.apply("token", "bob@example.com") != a
    assert transforms.apply("token invoices", "ann@example.com") != a
    assert transforms.reverse("token", a) == "ann@example.com"
    with pytest.raises(SystemExit, match="does not open"):
        transforms.reverse("token", a, key="another key")
    with pytest.raises(SystemExit, match="not in the vault"):
        transforms.reverse("token", "tok-" + "0" * 24)
    assert transforms.apply("token", None) is None


def test_the_vault_holds_the_value_sealed_and_stays_private(state):
    import sqlite3
    import stat
    t = transforms.apply("token", "4111-1111-1111-1111")
    raw = (state / "token.vault").read_bytes()
    assert b"4111" not in raw
    assert stat.S_IMODE((state / "token.vault").stat().st_mode) == 0o600
    row = sqlite3.connect(str(state / "token.vault")).execute(
        "select token from vault").fetchall()
    assert row == [(t,)]
    # without the key in the state directory the token cannot be opened
    (state / "transform.key").write_bytes(bytes(32))
    masking._SECRETS.clear()
    with pytest.raises(SystemExit, match="does not open"):
        transforms.reverse("token", t)


def test_the_new_transforms_are_the_owners_and_leave_sql_to_python():
    assert transforms.parse("fpe") == ("fpe", "")
    assert transforms.parse("token pay") == ("token", "pay")
    for rule in ("fpe", "token"):
        assert transforms.sql(rule, '"c"', "postgres") is None
        assert transforms.text_only(rule)
    with pytest.raises(SystemExit, match="fpe \\[alphabet\\]"):
        transforms.parse("scramble")
    with pytest.raises(SystemExit, match="cannot be reversed"):
        transforms.reverse("hash", "abc")


@pytest.mark.parametrize("facts,want", [
    # read by its shape, digits, room to hold: encrypted in its shape
    ({"kind": "digits", "length": 16, "shape": True}, "fpe"),
    # a key others join on, digits: the same value must come out everywhere
    ({"kind": "digits", "length": 10, "unique": True, "referenced": 2},
     "fpe"),
    ({"kind": "alphabet", "alphabet": "alnum", "length": 12,
      "reversible": True}, "fpe"),
    # a text of no known set that must come back or stay joined: a token
    ({"kind": "text", "length": 40, "unique": True}, "token"),
    ({"kind": "text", "length": 40, "referenced": 1}, "token"),
    # digits too few to hold: the token, not a dictionary
    ({"kind": "digits", "length": 4, "unique": True}, "token"),
    # nothing is asked of the value but that it be hidden
    ({"kind": "text", "length": 40}, "hash"),
    ({"kind": "digits", "length": 16}, "hash"),
])
def test_the_choice_between_hash_fpe_and_token_is_a_rung_on_the_facts(
        facts, want):
    got = decide.hide_column(facts)
    assert got.path == want, (got.path, got.reason)
    rule, again = transforms.choose(facts)
    assert again.path == want and rule.split()[0] == want


def test_the_choice_says_why_a_rung_above_was_not_taken():
    got = decide.hide_column({"kind": "digits", "length": 4, "unique": True})
    why = " ".join(str(w) for w in (got.reason, got.said,
                                    getattr(got, "skipped", "")))
    assert "1,000,000" in why or "fewer" in why
    rule, _ = transforms.choose({"kind": "alphabet", "alphabet": "lower",
                                 "length": 9, "shape": True})
    assert rule == "fpe lower"


def test_a_dump_left_behind_is_a_rung_on_its_owner():
    gone = decide.leftover_dump({"owner gone": True})
    live = decide.leftover_dump({"owner gone": False, "age": 1e9,
                                 "longest step": 5})
    old = decide.leftover_dump({"owner gone": None, "age": 900,
                                "longest step": 60})
    new = decide.leftover_dump({"owner gone": None, "age": 30,
                                "longest step": 60})
    assert (gone.path, live.path, old.path, new.path) == (
        "remove", "keep", "remove", "keep")
