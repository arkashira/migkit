"""A row filter keyed `schema.table` reaches the bulk path.

`mapping.where` keys match tables by suffix, like every name in a hop, so
`public.orders` is PostgreSQL's orders table. The bulk path asked a cheaper
question first, which tables' filters apply to this database, and counted a
two-part key only when its first part was the database's own name.
`public.orders` therefore applied nowhere. The dump neither refused the
filter nor routed the table to the copier that applies it, and every row
was copied. The check, which does match by suffix, then compared a
filtered source against a full target.
"""
import pytest

from migkit.config import Endpoint, Hop


def _hop(where, databases=("appdb",)):
    return Hop(name="f", engine="postgres",
               source=Endpoint(host="10.0.0.1", port=5432, user="u",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=5432, user="u",
                               password="CHANGE_ME"),
               databases=list(databases), mapping={"where": where})


def test_the_filtered_table_is_routed_to_the_copier():
    from migkit import movers
    got = movers.routed_to_copier(
        _hop({"public.orders": "region = 'apac'"}), "appdb", "pgdump",
        ["public.orders", "public.people"])
    assert got == ["public.orders"], got


def test_a_path_that_cannot_filter_refuses_it():
    from migkit import movers
    with pytest.raises(SystemExit) as e:
        movers.refuse_unpushable_filters(
            _hop({"public.orders": "region = 'apac'"}), "appdb", "builtin",
            "hetero")
    assert "public.orders" in str(e.value), e.value


def test_a_key_for_another_database_still_stays_there():
    from migkit import movers
    hop = _hop({"other.orders": "x = 1"}, databases=("appdb", "other"))
    assert movers._filtered_here(hop, "appdb") == []
    assert movers._filtered_here(hop, "other") == ["other.orders"]
