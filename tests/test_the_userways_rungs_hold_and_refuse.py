"""The rungs an account's password reaches the target by, each reached on
its branches through `userways.password` on fakes (backlog
AUD-Z.rungs-outside): the source's own hash where it can be carried, the
passwords file's where it gives one, and where neither holds no account is
made and the reason names both."""
from migkit import userways
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("userways", "RUNGS", "<HASH>", "holds"),
    ("userways", "RUNGS", "<HASH>", "refuses"),
    ("userways", "RUNGS", "<FILE>", "holds"),
    ("userways", "RUNGS", "<FILE>", "refuses"),
]

HASH_WHY = "the target does not sign in with the account's plugin"
FILE_WHY = "the passwords file names no password for app"


def test_a_hash_that_can_be_carried_is_carried():
    with watched() as seen:
        got = userways.password("app", hash_ok=True, hash_why=HASH_WHY,
                                secret_ok=False, secret_why=FILE_WHY)
    assert got.path == userways.HASH
    assert branch(seen, userways.HASH) == [HOLDS]
    assert branch(seen, userways.FILE) == [REFUSES]
    assert why(seen, userways.FILE) == FILE_WHY


def test_a_hash_the_target_does_not_take_falls_to_the_file():
    with watched() as seen:
        got = userways.password("app", hash_ok=False, hash_why=HASH_WHY,
                                secret_ok=True, secret_why=FILE_WHY)
    assert got.path == userways.FILE
    assert branch(seen, userways.HASH) == [REFUSES]
    assert branch(seen, userways.FILE) == [HOLDS]
    assert why(seen, userways.HASH) == HASH_WHY and HASH_WHY in got.reason


def test_an_account_with_neither_way_is_not_made_and_says_both():
    with watched() as seen:
        got = userways.password("app", hash_ok=False, hash_why=HASH_WHY,
                                secret_ok=False, secret_why=FILE_WHY)
    assert got.path is None
    assert HASH_WHY in got.reason and FILE_WHY in got.reason
    assert why(seen, userways.HASH) == HASH_WHY
    assert why(seen, userways.FILE) == FILE_WHY
