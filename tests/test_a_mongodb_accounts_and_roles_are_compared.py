"""MongoDB's accounts and their roles are compared between the two servers,
and the deep check holds the accounts and roles that reach a database to
what the source gave them (P0-PARITY.verify).

MongoDB keeps who may do what as roles on resources - a database, a
collection, the cluster - and not as grants on a table: an account holds
roles, a role made in a database holds privileges (actions on
collections). `users.compare` is the account comparison; the deep check's
`grants` finding is the same question asked of one database's
collections, through the hop's scope (`objects: {roles: ...}`, the
collections it takes, the names it gives them).

Run on servers in memory (`tests/mongo_fake.py`). The accounts are read
by the call that already ran against real servers (`usersInfo`); the
privileges of the roles made in a database by `rolesInfo`, which has not
- so `verify.access.table-grants` is built and its server proof is owed.
"""
from migkit import users
from tests.mongo_fake import engine, no_real_server  # noqa: F401 - autouse

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.users", "mongodb")]


def _account(db, user, *roles):
    return {"user": user, "db": db,
            "roles": [{"role": r, "db": d} for r, d in roles]}


def _hop(tmp_path, **kw):
    eng, src, dst = engine(kw, tmp_path)
    return eng, src, dst


def test_an_account_the_target_lacks_is_missing_on_it(tmp_path):
    eng, src, dst = _hop(tmp_path)
    src.accounts = [_account("shop", "app", ("readWrite", "shop")),
                    _account("shop", "report", ("read", "shop"))]
    dst.accounts = [_account("shop", "app", ("readWrite", "shop"))]
    out, mine = users.compare(eng.hop, say=lambda line: None)
    assert out["missing_on_target"] == ["shop.report"]
    assert out["grants_missing"] == [] and out["result"] == "gap"
    assert sorted(mine) == [("shop", "app"), ("shop", "report")]


def test_a_role_the_target_lacks_is_a_grant_missing(tmp_path):
    eng, src, dst = _hop(tmp_path)
    src.accounts = [_account("shop", "app", ("readWrite", "shop"),
                             ("dbAdmin", "shop"))]
    dst.accounts = [_account("shop", "app", ("readWrite", "shop"))]
    out, _ = users.compare(eng.hop, say=lambda line: None)
    assert out["grants_missing"] == ["shop.app"]
    assert out["result"] == "gap"


def test_roles_beyond_the_sources_are_the_targets_own(tmp_path):
    eng, src, dst = _hop(tmp_path)
    src.accounts = [_account("shop", "app", ("read", "shop"))]
    dst.accounts = [_account("shop", "app", ("read", "shop"),
                             ("readWrite", "shop"))]
    out, _ = users.compare(eng.hop, say=lambda line: None)
    assert out["grants_missing"] == [] and out["result"] == "pass"


def test_passwords_are_said_not_to_be_comparable(tmp_path):
    eng, src, dst = _hop(tmp_path)
    src.accounts = dst.accounts = [_account("shop", "app", ("read", "shop"))]
    said = []
    out, _ = users.compare(eng.hop, say=said.append)
    assert out["password_differs"] is None
    assert any("passwords cannot be copied" in line for line in said)


def test_a_hop_that_names_its_roles_compares_only_those(tmp_path):
    eng, src, dst = _hop(tmp_path, objects={"roles": ["shop.app"]})
    src.accounts = [_account("shop", "app", ("read", "shop")),
                    _account("shop", "other", ("read", "shop"))]
    dst.accounts = [_account("shop", "app", ("read", "shop"))]
    out, _ = users.compare(eng.hop, say=lambda line: None)
    assert out["missing_on_target"] == [] and out["result"] == "pass"


# --- the deep check's finding for one database -----------------------------

def _privilege(db, collection, *actions):
    return {"resource": {"db": db, "collection": collection},
            "actions": list(actions)}


def _grants(eng):
    [got] = [r for r in eng.check_deep("shop") if r.scope == "shop grants"]
    return got


def test_accounts_and_roles_that_hold_the_same_are_ok(tmp_path):
    eng, src, dst = _hop(tmp_path)
    for side in (src, dst):
        side.db("shop").add("orders", [{"_id": 1}])
        side.accounts = [_account("shop", "app", ("readWrite", "shop"))]
        side.roles["shop"] = [{"role": "clerk", "db": "shop", "privileges": [
            _privilege("shop", "orders", "find", "update")]}]
    got = _grants(eng)
    assert got.status == "ok" and got.category == "access.table-grants"
    assert "2 accounts and roles" in got.detail


def test_an_account_missing_or_short_of_a_role_is_the_gap(tmp_path):
    eng, src, dst = _hop(tmp_path)
    for side in (src, dst):
        side.db("shop").add("orders", [{"_id": 1}])
    src.accounts = [_account("shop", "app", ("readWrite", "shop")),
                    _account("shop", "audit", ("read", "shop"))]
    dst.accounts = [_account("shop", "app", ("read", "shop"))]
    got = _grants(eng)
    assert got.status == "diff"
    assert "audit: holds ['read'] on the source and is missing" in got.detail
    assert "app: lacks ['readWrite'] on the target" in got.detail


def test_an_account_on_another_database_is_not_this_ones(tmp_path):
    eng, src, dst = _hop(tmp_path)
    for side in (src, dst):
        side.db("shop").add("orders", [{"_id": 1}])
    src.accounts = [_account("crm", "sales", ("readWrite", "crm"))]
    assert _grants(eng).status == "ok"


def test_a_role_short_of_a_privilege_on_a_collection_is_named(tmp_path):
    eng, src, dst = _hop(tmp_path)
    for side in (src, dst):
        side.db("shop").add("orders", [{"_id": 1}])
    src.roles["shop"] = [{"role": "clerk", "db": "shop", "privileges": [
        _privilege("shop", "orders", "find", "update", "remove")]}]
    dst.roles["shop"] = [{"role": "clerk", "db": "shop", "privileges": [
        _privilege("shop", "orders", "find")]}]
    got = _grants(eng)
    assert got.status == "diff"
    assert "role clerk: lacks remove on orders, update on orders" \
        in got.detail


def test_a_privilege_on_a_collection_the_hop_leaves_out_is_not_compared(
        tmp_path):
    eng, src, dst = _hop(tmp_path, exclude=["shop.scratch"])
    for side in (src, dst):
        side.db("shop").add("orders", [{"_id": 1}])
        side.db("shop").add("scratch", [{"_id": 1}])
    src.roles["shop"] = [{"role": "clerk", "db": "shop", "privileges": [
        _privilege("shop", "scratch", "find")]}]
    dst.roles["shop"] = [{"role": "clerk", "db": "shop", "privileges": []}]
    assert _grants(eng).status == "ok"


def test_a_privilege_is_held_on_the_name_the_collection_has_on_the_target(
        tmp_path):
    eng, src, dst = _hop(
        tmp_path, mapping={"tables": {"shop.orders": "orders_v2"}})
    src.db("shop").add("orders", [{"_id": 1}])
    dst.db("shop").add("orders_v2", [{"_id": 1}])
    src.roles["shop"] = [{"role": "clerk", "db": "shop", "privileges": [
        _privilege("shop", "orders", "find")]}]
    dst.roles["shop"] = [{"role": "clerk", "db": "shop", "privileges": [
        _privilege("shop", "orders_v2", "find")]}]
    assert _grants(eng).status == "ok"


def test_a_server_that_will_not_say_is_a_skip_not_an_ok(tmp_path):
    eng, src, dst = _hop(tmp_path)
    for side in (src, dst):
        side.db("shop").add("orders", [{"_id": 1}])

    def refuse(*a, **kw):
        raise RuntimeError("not authorized on shop to execute rolesInfo")
    dst.db("shop").command = refuse
    got = _grants(eng)
    assert got.status == "skip" and "rolesInfo" in got.detail
