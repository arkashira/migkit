"""The target's foreign keys taken off for a load and put back after
(`indexes.ForeignKeyWindow`), on the one set-aside record
(`setaside.SetAside`), and the partitions read by the engine that owns them.

Without a server: a scripted target keeps its keys in a dict and answers the
catalogue read, the drops, the adds and the validations the window sends;
it refuses the statements a test tells it to. What is held: a key that does
not go back stops the move with the statement said, a key that cannot be
read or dropped leaves nothing off, names are quoted the one way, and a
partition that cannot be read is an error, never an empty scope.
"""
import json
import socket

import pytest

from migkit import decide, indexes
from migkit.engines.postgres import _ident
from migkit.indexes import FKS_SET_ASIDE, ForeignKeyWindow, NotPutBack


class Hop:
    def __init__(self, tmp_path):
        self.path = tmp_path

    def report_dir(self, db=None):
        return self.path


class Target:
    """Keys as {(schema, table, name): [validated, definition, ref schema,
    ref table]}; `refuse` holds words that, found in a statement, make the
    server refuse it."""

    def __init__(self, keys, refuse=()):
        self.keys = {k: list(v) for k, v in keys.items()}
        self.known = {k: list(v) for k, v in keys.items()}
        self.refuse = list(refuse)
        self.sent = []
        self.unreadable = False

    def _key(self, sql):
        """The key a statement names, by the quoting the window must use."""
        for (s, t, n) in self.known:
            on = f"alter table {_ident(s)}.{_ident(t)} "
            for verb in ("drop", "add", "validate"):
                lead = f"{on}{verb} constraint {_ident(n)}"
                if sql == lead or sql.startswith(lead + " "):
                    return (s, t, n), verb, sql[len(lead) + 1:]
        raise AssertionError(f"a statement naming no key: {sql!r}")

    def __call__(self, sql):
        self.sent.append(sql)
        if sql == indexes.PG_FK_SQL:
            if self.unreadable:
                raise RuntimeError("could not connect to server: timeout")
            return chr(30).join(
                chr(31).join([s, t, n, "t" if v else "f", d, rs, rt])
                for (s, t, n), (v, d, rs, rt) in sorted(self.keys.items())
            ) + "\n"
        for word in self.refuse:
            if word in sql:
                raise RuntimeError(
                    "psql: error\nERROR:  insert or update on table violates"
                    f" foreign key constraint ({word})")
        key, verb, rest = self._key(sql)
        if verb == "drop":
            del self.keys[key]
        elif verb == "add":
            valid = not rest.endswith(" not valid")
            definition = rest if valid else rest[:-len(" not valid")]
            self.keys[key] = [valid and not definition.endswith(
                " NOT VALID"), definition] + self.known[key][2:]
        else:
            self.keys[key][0] = True
        return ""


KEYS = {
    ("public", "orders", "orders_cust_fk"):
        [True, "FOREIGN KEY (cust_id) REFERENCES customers(id)",
         "public", "customers"],
    ("public", "items", "items_order_fk"):
        [True, "FOREIGN KEY (order_id) REFERENCES orders(id)",
         "public", "orders"],
    ("audit", "log", "log_user_fk"):
        [True, "FOREIGN KEY (user_id) REFERENCES audit.users(id)",
         "audit", "users"],
}


def _target(keys=None, refuse=()):
    return Target(KEYS if keys is None else keys, refuse)


def _records(tmp_path):
    return list(tmp_path.glob(f"{FKS_SET_ASIDE}.*.json"))


def test_keys_come_off_for_the_load_and_go_back_validated(tmp_path):
    t = _target()
    lines = []
    with ForeignKeyWindow(Hop(tmp_path), "app", lines.append,
                          tables=["public.orders"], run=t) as w:
        # on the table filled and pointing into it; the other schema's kept
        assert sorted(w.dropped) == ['"public"."items"."items_order_fk"',
                                     '"public"."orders"."orders_cust_fk"']
        assert set(t.keys) == {("audit", "log", "log_user_fk")}
        assert set(json.loads(w.record.path.read_text())) == set(w.dropped)
    assert t.keys == KEYS
    # added without the lock that stops writes, then validated
    assert ('alter table "public"."orders" add constraint "orders_cust_fk"'
            " FOREIGN KEY (cust_id) REFERENCES customers(id) not valid"
            in t.sent)
    assert ('alter table "public"."orders" validate constraint'
            ' "orders_cust_fk"' in t.sent)
    assert not _records(tmp_path)
    assert lines[-1] == "2 foreign keys put back"


def test_a_refused_add_constraint_stops_the_move_with_the_statement(
        tmp_path):
    t = _target(refuse=['validate constraint "items_order_fk"'])
    with pytest.raises(NotPutBack) as got:
        with ForeignKeyWindow(Hop(tmp_path), "app", tables=["public.orders"],
                              run=t):
            pass
    said = str(got.value)
    assert ('alter table "public"."items" validate constraint'
            ' "items_order_fk" -> ERROR:  insert or update on table'
            in said)
    assert "orders_cust_fk" not in said
    # a SystemExit: no `except Exception` on the way turns it into a log line
    assert not isinstance(got.value, Exception)
    # the record stays for the next load, or a person
    kept = _records(tmp_path)
    assert len(kept) == 1 and str(kept[0]) in said
    assert '"public"."items"."items_order_fk"' in json.loads(
        kept[0].read_text())


def test_a_refused_add_after_the_load_failed_still_stops(tmp_path):
    t = _target(refuse=['add constraint "orders_cust_fk"'])
    with pytest.raises(NotPutBack) as got:
        with ForeignKeyWindow(Hop(tmp_path), "app", tables=["public.orders"],
                              run=t):
            raise RuntimeError("the load failed")
    assert ('alter table "public"."orders" add constraint "orders_cust_fk"'
            in str(got.value))
    # the load's own failure is kept beneath it
    assert str(got.value.__context__) == "the load failed"
    # the other key went back regardless
    assert t.keys[("public", "items", "items_order_fk")][0] is True
    assert ("public", "orders", "orders_cust_fk") not in t.keys


def test_keys_that_cannot_be_read_leave_nothing_off(tmp_path):
    t = _target()
    t.unreadable = True
    with pytest.raises(decide.Blocked) as got:
        with ForeignKeyWindow(Hop(tmp_path), "app", run=t):
            pytest.fail("the load ran with keys it could not see")
    assert "could not be read, so none were taken off" in str(got.value)
    assert "timeout" in str(got.value)
    assert t.keys == KEYS and not _records(tmp_path)


def test_a_refused_drop_puts_back_what_came_off_first(tmp_path):
    t = _target(refuse=['drop constraint "orders_cust_fk"'])
    with pytest.raises(decide.Blocked) as got:
        with ForeignKeyWindow(Hop(tmp_path), "app", tables=["public.orders"],
                              run=t):
            pytest.fail("the load ran with a key still on")
    assert ('alter table "public"."orders" drop constraint "orders_cust_fk"'
            in str(got.value))
    assert t.keys == KEYS and not _records(tmp_path)


def test_keys_a_dead_load_left_off_go_back_with_this_ones(tmp_path):
    t = _target()
    left = '"public"."items"."items_order_fk"'
    del t.keys[("public", "items", "items_order_fk")]
    dead = tmp_path / f"{FKS_SET_ASIDE}.{socket.gethostname()}.999999.1.json"
    dead.write_text(json.dumps({left: {
        "on": '"public"."items"', "name": '"items_order_fk"',
        "def": "FOREIGN KEY (order_id) REFERENCES orders(id)",
        "valid": True}}))
    lines = []
    with ForeignKeyWindow(Hop(tmp_path), "app", lines.append,
                          tables=["audit.log"], run=t) as w:
        assert left in w.dropped and not dead.exists()
        assert any("an earlier load took off" in x for x in lines)
    assert t.keys == KEYS and not _records(tmp_path)


def test_a_dead_loads_key_the_target_has_back_is_left_alone(tmp_path):
    t = _target()
    dead = tmp_path / f"{FKS_SET_ASIDE}.{socket.gethostname()}.999999.1.json"
    dead.write_text(json.dumps({'"public"."items"."items_order_fk"': {
        "on": '"public"."items"', "name": '"items_order_fk"',
        "def": "FOREIGN KEY (order_id) REFERENCES orders(id)",
        "valid": True}}))
    with ForeignKeyWindow(Hop(tmp_path), "app", tables=["audit.users"],
                          run=t) as w:
        assert list(w.dropped) == ['"audit"."log"."log_user_fk"']
    assert not [s for s in t.sent if "items_order_fk" in s]


def test_a_name_holding_a_quote_is_quoted_the_one_way(tmp_path):
    keys = {('we"ird', "t\nx", 'k"1'):
            [True, "FOREIGN KEY (a) REFERENCES p(id)", 'we"ird', "p"]}
    t = _target(keys)
    with ForeignKeyWindow(Hop(tmp_path), "app", run=t):
        assert not t.keys
    assert 'alter table "we""ird"."t\nx" drop constraint "k""1"' in t.sent
    assert t.keys == keys


def test_a_key_saved_not_valid_goes_back_not_valid():
    fk = {"on": '"public"."a"', "name": '"k"', "valid": False,
          "def": "FOREIGN KEY (b) REFERENCES b(id) NOT VALID"}
    assert indexes.fk_put_back('"public"."a"."k"', fk, {}) == [
        'alter table "public"."a" add constraint "k"'
        " FOREIGN KEY (b) REFERENCES b(id) NOT VALID"]
    # already there and valid: nothing; there but not validated: validated
    fk["valid"] = True
    assert indexes.fk_put_back('"public"."a"."k"', fk,
                               {'"public"."a"."k"': True}) == []
    assert indexes.fk_put_back('"public"."a"."k"', fk,
                               {'"public"."a"."k"': False}) == [
        'alter table "public"."a" validate constraint "k"']


def test_a_partition_read_error_is_an_error_not_an_empty_scope(
        monkeypatch, tmp_path):
    from tests.mssql_stub import Server, engine

    def down(side, sql):
        raise RuntimeError("Login timeout expired")
    eng = engine(monkeypatch, tmp_path,
                 Server().on("sys.partition_schemes", down))
    with pytest.raises(RuntimeError, match="Login timeout expired"):
        eng._deep_partitions("appdb")


def test_no_second_partition_reader_or_fk_window_is_left():
    import importlib.util
    for gone in ("migkit.partitions", "migkit.fk_window"):
        assert importlib.util.find_spec(gone) is None, gone


def test_a_dead_loads_record_with_nothing_left_to_do_goes(tmp_path):
    t = _target()
    dead = tmp_path / f"{FKS_SET_ASIDE}.{socket.gethostname()}.999999.1.json"
    dead.write_text(json.dumps({'"audit"."log"."log_user_fk"': {
        "on": '"audit"."log"', "name": '"log_user_fk"',
        "def": "FOREIGN KEY (user_id) REFERENCES audit.users(id)",
        "valid": True}}))
    with ForeignKeyWindow(Hop(tmp_path), "app", tables=["public.none"],
                          run=t) as w:
        assert not w.dropped
    assert not dead.exists() and t.sent == [indexes.PG_FK_SQL]
