"""How an account's password reaches the target, chosen by what the source
shows and what the target takes (backlog P0-DECISION-ALL.coverage, the
`users` task).

Each engine's account sync made this choice in a branch of its own: MySQL
carried the source's hash unless the target did not sign in with the
account's plugin, PostgreSQL the same unless the source hid its hashes,
Oracle the same unless the hash was not readable or not in a form it takes.
The three said the same thing three ways and kept no record of why one way
was passed over for the next. It is one ladder now, climbed per account
through the one decision engine (`decide.climb`), each engine giving the
facts it alone knows:

* **the source's own hash** (`HASH`): the account keeps the very password
  it had, and the plaintext is never read or shown. The first way, where
  the source's hash can be read and the target signs in with it.
* **the passwords file's** (`FILE`): the password the operator gave for the
  account, bound where the statement runs and never shown or recorded -
  the operator's choice, not the source's, so it is said to be given.

An account with neither way is not made, and the reason names why each way
was passed over. What this ladder does not decide stays with each engine's
plan, as a guarantee and not a choice: an account with no password that may
sign in is never made - it would let anyone in.
"""
from . import decide

#: the ways, by the names the decision record keeps them under
HASH, FILE = "password hash", "password from the file"

_N = decide.Need

RUNGS = (
    decide.Rung(
        HASH, "the source's own password hash, carried",
        because="the account keeps the very password it had, and the"
                " plaintext is never read",
        needs=(_N("a hash to carry", lambda f: f["hash"],
                  lambda f: f["hash why"]),)),
    decide.Rung(
        FILE, "the password the passwords file gives it", footprint=1,
        because="the passwords file gives the account its password",
        needs=(_N("a password in the passwords file", lambda f: f["secret"],
                  lambda f: f["secret why"]),)),
)


def password(who, *, hash_ok, hash_why, secret_ok, secret_why):
    """The `decide.Choice` of how `who`'s password reaches the target.

    `hash_ok` is whether the source's hash can be carried - read, in a form
    this target takes - and `hash_why` the sentence saying why not;
    `secret_ok` whether the passwords file gives a password this target
    takes, and `secret_why` why not. The path is `HASH`, `FILE`, or None
    where neither holds, and the reason then says both."""
    facts = decide.Facts({"hash": bool(hash_ok), "hash why": hash_why,
                          "secret": bool(secret_ok),
                          "secret why": secret_why})
    return decide.climb(f"{who}: its password", RUNGS, facts)
