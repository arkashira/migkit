import json
import threading
import time
import pytest

from migkit import decide, indexes, schema
from migkit.indexes import (
    INDEXES_SET_ASIDE,
    DeferredIndexLifecycle,
    ForeignKeyWindow,
    IndexWindow,
    NotPutBack,
    NotRebuilt,
    concurrent_index_sql,
    drop_index_sql,
    index_rows,
    mysql_index_rows,
    plan_secondary_indexes,
    rebuild_indexes_parallel,
    restore_foreign_keys_non_blocking,
    topological_fk_sort,
    topological_validate_fks,
)


class Hop:
    def __init__(self, tmp_path):
        self.path = tmp_path

    def report_dir(self, db=None):
        return self.path


class ScriptedTarget:
    def __init__(self, indexes_raw=None, fks_raw=None, refuse=()):
        self.indexes_raw = indexes_raw or ""
        self.fks_raw = fks_raw or ""
        self.refuse = list(refuse)
        self.sent = []
        self.max_concurrent_workers = 0
        self._active_workers = 0
        self._lock = threading.Lock()
        self.fks_present = True
        self.indexes_present = True

    def __call__(self, sql):
        with self._lock:
            self._active_workers += 1
            if self._active_workers > self.max_concurrent_workers:
                self.max_concurrent_workers = self._active_workers
            self.sent.append(sql)

        try:
            for word in self.refuse:
                if word in sql:
                    raise RuntimeError(f"error executing {sql}: {word}")

            if sql == indexes.PG_INDEX_SQL:
                return self.indexes_raw if self.indexes_present else ""
            if sql == indexes.PG_FK_SQL:
                return self.fks_raw if self.fks_present else ""

            if "drop constraint" in sql.lower():
                self.fks_present = False
            elif "add constraint" in sql.lower():
                self.fks_present = True
            elif "drop index" in sql.lower():
                self.indexes_present = False
            elif "create index" in sql.lower():
                self.indexes_present = True

            time.sleep(0.005)
            return ""
        finally:
            with self._lock:
                self._active_workers -= 1


def test_secondary_non_unique_index_metadata_extraction():
    raw_pg = (
        chr(31).join(["public", "orders", "orders_pkey", "CREATE UNIQUE INDEX orders_pkey ON orders (id)", "t", "t", "t"])
        + chr(30)
        + chr(31).join(["public", "orders", "orders_cust_id_idx", "CREATE INDEX orders_cust_id_idx ON orders (cust_id)", "f", "f", "f"])
        + chr(30)
        + chr(31).join(["public", "orders", "orders_email_uq", "CREATE UNIQUE INDEX orders_email_uq ON orders (email)", "t", "f", "f"])
        + chr(30)
        + chr(31).join(["public", "items", "items_order_id_idx", "CREATE INDEX items_order_id_idx ON items (order_id)", "f", "f", "f"])
    )
    rows = index_rows(raw_pg)
    assert len(rows) == 4

    to_drop, plan = plan_secondary_indexes(rows, tables=[("public", "orders")])
    assert to_drop == ['"public"."orders_cust_id_idx"']
    assert '"public"."orders_cust_id_idx"' in plan
    assert plan['"public"."orders_cust_id_idx"']["def"] == "CREATE INDEX orders_cust_id_idx ON orders (cust_id)"
    assert plan['"public"."orders_cust_id_idx"']["table"] == "orders"

    all_drop, all_plan = plan_secondary_indexes(rows)
    assert all_drop == ['"public"."items_order_id_idx"', '"public"."orders_cust_id_idx"']


def test_mysql_index_metadata_extraction():
    raw_mysql = (
        chr(31).join(["app", "users", "PRIMARY", "t", "t", "id"])
        + chr(30)
        + chr(31).join(["app", "users", "uq_email", "t", "f", "email"])
        + chr(30)
        + chr(31).join(["app", "users", "ix_created", "f", "f", "created_at,status"])
    )
    rows = mysql_index_rows(raw_mysql)
    assert len(rows) == 3

    to_drop, plan = plan_secondary_indexes(rows, engine="mysql")
    assert to_drop == ["`app`.`ix_created`"]
    assert "`app`.`ix_created`" in plan
    assert "CREATE INDEX `ix_created` ON `app`.`users` (`created_at`, `status`)" in plan["`app`.`ix_created`"]["def"]


def test_concurrent_index_sql_postgres_and_mysql():
    pg_simple = "CREATE INDEX idx_a ON public.tbl (a)"
    assert concurrent_index_sql(pg_simple, "postgres") == "CREATE INDEX CONCURRENTLY idx_a ON public.tbl (a)"

    pg_already = "CREATE INDEX CONCURRENTLY idx_a ON public.tbl (a)"
    assert concurrent_index_sql(pg_already, "postgres") == pg_already

    pg_unique = "create unique index idx_u on t (email);"
    assert concurrent_index_sql(pg_unique, "postgres") == "create unique index CONCURRENTLY idx_u on t (email)"

    mysql_create = "CREATE INDEX `idx_a` ON `tbl` (`a`)"
    assert concurrent_index_sql(mysql_create, "mysql") == "CREATE INDEX `idx_a` ON `tbl` (`a`) ALGORITHM=INPLACE LOCK=NONE"

    mysql_alter = "ALTER TABLE `tbl` ADD INDEX `idx_a` (`a`);"
    assert concurrent_index_sql(mysql_alter, "mysql") == "ALTER TABLE `tbl` ADD INDEX `idx_a` (`a`), ALGORITHM=INPLACE, LOCK=NONE"


def test_drop_index_sql_syntax():
    assert drop_index_sql("public", "idx_a", engine="postgres") == 'DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_a"'
    assert drop_index_sql("app", "idx_a", table="orders", engine="mysql") == "ALTER TABLE `app`.`orders` DROP INDEX `idx_a`"


def test_parallel_index_rebuild_workers():
    definitions = {
        f"idx_{i}": f"CREATE INDEX idx_{i} ON t (col_{i})"
        for i in range(8)
    }
    t = ScriptedTarget()
    rebuilt, failed = rebuild_indexes_parallel(definitions, t, max_workers=4, engine="postgres")
    assert len(rebuilt) == 8
    assert failed == []
    assert t.max_concurrent_workers >= 2
    for i in range(8):
        assert f"CREATE INDEX CONCURRENTLY idx_{i} ON t (col_{i})" in t.sent


def test_parallel_index_rebuild_failure_reporting():
    definitions = {
        "idx_good": "CREATE INDEX idx_good ON t (a)",
        "idx_bad": "CREATE INDEX idx_bad ON t (b)",
    }
    t = ScriptedTarget(refuse=["idx_bad"])
    rebuilt, failed = rebuild_indexes_parallel(definitions, t, max_workers=2, engine="postgres")
    assert rebuilt == ["idx_good"]
    assert len(failed) == 1
    assert failed[0][0] == "CREATE INDEX CONCURRENTLY idx_bad ON t (b)"
    assert "idx_bad" in failed[0][1]


def test_index_window_drops_and_parallel_rebuilds(tmp_path):
    raw_pg = (
        chr(31).join(["public", "tbl", "tbl_pkey", "CREATE UNIQUE INDEX tbl_pkey ON tbl (id)", "t", "t", "t"])
        + chr(30)
        + chr(31).join(["public", "tbl", "tbl_idx1", "CREATE INDEX tbl_idx1 ON tbl (col1)", "f", "f", "f"])
        + chr(30)
        + chr(31).join(["public", "tbl", "tbl_idx2", "CREATE INDEX tbl_idx2 ON tbl (col2)", "f", "f", "f"])
    )
    t = ScriptedTarget(indexes_raw=raw_pg)
    hop = Hop(tmp_path)
    lines = []

    with IndexWindow(hop, "db", log=lines.append, run=t, max_workers=2) as w:
        assert sorted(w.dropped) == ['"public"."tbl_idx1"', '"public"."tbl_idx2"']
        assert 'DROP INDEX CONCURRENTLY IF EXISTS "public"."tbl_idx1"' in t.sent
        assert 'DROP INDEX CONCURRENTLY IF EXISTS "public"."tbl_idx2"' in t.sent
        records = list(tmp_path.glob(f"{INDEXES_SET_ASIDE}.*.json"))
        assert len(records) == 1
        assert set(json.loads(records[0].read_text())) == set(w.dropped)

    assert 'CREATE INDEX CONCURRENTLY tbl_idx1 ON tbl (col1)' in t.sent
    assert 'CREATE INDEX CONCURRENTLY tbl_idx2 ON tbl (col2)' in t.sent
    assert not list(tmp_path.glob(f"{INDEXES_SET_ASIDE}.*.json"))
    assert lines[-1] == "2 secondary indexes rebuilt"


def test_index_window_blocked_when_cannot_save(tmp_path):
    unwritable = tmp_path / "nowrite"
    unwritable.write_text("not a dir")
    bad_hop = Hop(unwritable / "sub")
    raw_pg = chr(31).join(["public", "tbl", "idx", "CREATE INDEX idx ON tbl (a)", "f", "f", "f"])
    t = ScriptedTarget(indexes_raw=raw_pg)

    with pytest.raises(decide.Blocked) as got:
        with IndexWindow(bad_hop, "db", run=t):
            pass
    assert "could not be saved" in str(got.value)
    assert not any("DROP" in s for s in t.sent)


def test_index_window_rebuild_failure_stops_move(tmp_path):
    raw_pg = chr(31).join(["public", "tbl", "idx_fail", "CREATE INDEX idx_fail ON tbl (a)", "f", "f", "f"])
    t = ScriptedTarget(indexes_raw=raw_pg, refuse=["CREATE INDEX CONCURRENTLY idx_fail"])
    hop = Hop(tmp_path)

    with pytest.raises(NotRebuilt) as got:
        with IndexWindow(hop, "db", run=t):
            pass
    assert "1 indexes did not rebuild on the target" in str(got.value)
    assert "CREATE INDEX CONCURRENTLY idx_fail ON tbl (a)" in str(got.value)
    assert list(tmp_path.glob(f"{INDEXES_SET_ASIDE}.*.json"))


def test_topological_fk_sort_hierarchy():
    fks = {
        "orders_cust_fk": {
            "on": '"public"."orders"',
            "name": '"orders_cust_fk"',
            "def": "FOREIGN KEY (cust_id) REFERENCES customers(id)",
            "valid": True,
        },
        "items_order_fk": {
            "on": '"public"."items"',
            "name": '"items_order_fk"',
            "def": "FOREIGN KEY (order_id) REFERENCES orders(id)",
            "valid": True,
        },
        "delivery_item_fk": {
            "on": '"public"."deliveries"',
            "name": '"delivery_item_fk"',
            "def": "FOREIGN KEY (item_id) REFERENCES items(id)",
            "valid": True,
        },
        "payments_order_fk": {
            "on": '"public"."payments"',
            "name": '"payments_order_fk"',
            "def": "FOREIGN KEY (order_id) REFERENCES orders(id)",
            "valid": True,
        },
    }
    gens = topological_fk_sort(fks)
    assert len(gens) == 3
    assert gens[0] == ["orders_cust_fk"]
    assert set(gens[1]) == {"items_order_fk", "payments_order_fk"}
    assert gens[2] == ["delivery_item_fk"]


def test_topological_fk_sort_cycles_and_independent_tables():
    fks = {
        "cycle_a": {
            "on": '"public"."table_a"',
            "name": '"cycle_a"',
            "def": "FOREIGN KEY (b_id) REFERENCES table_b(id)",
            "valid": True,
        },
        "cycle_b": {
            "on": '"public"."table_b"',
            "name": '"cycle_b"',
            "def": "FOREIGN KEY (a_id) REFERENCES table_a(id)",
            "valid": True,
        },
        "independent": {
            "on": '"public"."indep"',
            "name": '"indep_fk"',
            "def": "FOREIGN KEY (x) REFERENCES other(x)",
            "valid": True,
        },
    }
    gens = topological_fk_sort(fks)
    assert len(gens) == 2
    assert gens[0] == ["independent"]
    assert set(gens[1]) == {"cycle_a", "cycle_b"}


def test_restore_foreign_keys_non_blocking_and_topological():
    fks = {
        "orders_cust_fk": {
            "on": '"public"."orders"',
            "name": '"orders_cust_fk"',
            "def": "FOREIGN KEY (cust_id) REFERENCES customers(id)",
            "valid": True,
        },
        "items_order_fk": {
            "on": '"public"."items"',
            "name": '"items_order_fk"',
            "def": "FOREIGN KEY (order_id) REFERENCES orders(id)",
            "valid": True,
        },
    }
    t = ScriptedTarget()
    added_and_validated, failed = restore_foreign_keys_non_blocking(fks, t, max_workers=2)
    assert failed == []
    assert len(added_and_validated) == 4

    add_orders_idx = t.sent.index(
        'alter table "public"."orders" add constraint "orders_cust_fk" FOREIGN KEY (cust_id) REFERENCES customers(id) not valid'
    )
    add_items_idx = t.sent.index(
        'alter table "public"."items" add constraint "items_order_fk" FOREIGN KEY (order_id) REFERENCES orders(id) not valid'
    )
    val_orders_idx = t.sent.index(
        'alter table "public"."orders" validate constraint "orders_cust_fk"'
    )
    val_items_idx = t.sent.index(
        'alter table "public"."items" validate constraint "items_order_fk"'
    )

    assert add_orders_idx < val_orders_idx
    assert add_items_idx < val_items_idx
    assert val_orders_idx < val_items_idx


def test_deferred_index_lifecycle_end_to_end(tmp_path):
    raw_indexes = (
        chr(31).join(["public", "orders", "orders_pkey", "CREATE UNIQUE INDEX orders_pkey ON orders (id)", "t", "t", "t"])
        + chr(30)
        + chr(31).join(["public", "orders", "orders_status_idx", "CREATE INDEX orders_status_idx ON orders (status)", "f", "f", "f"])
    )
    raw_fks = (
        chr(31).join(["public", "orders", "orders_cust_fk", "t", "FOREIGN KEY (cust_id) REFERENCES customers(id)", "public", "customers"])
    )
    t = ScriptedTarget(indexes_raw=raw_indexes, fks_raw=raw_fks)
    hop = Hop(tmp_path)
    lines = []

    with DeferredIndexLifecycle(hop, "db", log=lines.append, run=t, max_workers=2) as lc:
        assert 'alter table "public"."orders" drop constraint "orders_cust_fk"' in t.sent
        assert 'DROP INDEX CONCURRENTLY IF EXISTS "public"."orders_status_idx"' in t.sent

    assert 'CREATE INDEX CONCURRENTLY orders_status_idx ON orders (status)' in t.sent
    assert 'alter table "public"."orders" add constraint "orders_cust_fk" FOREIGN KEY (cust_id) REFERENCES customers(id) not valid' in t.sent
    assert 'alter table "public"."orders" validate constraint "orders_cust_fk"' in t.sent

    # Index rebuild should finish before foreign key validation starts
    idx_rebuild_pos = t.sent.index('CREATE INDEX CONCURRENTLY orders_status_idx ON orders (status)')
    fk_val_pos = t.sent.index('alter table "public"."orders" validate constraint "orders_cust_fk"')
    assert idx_rebuild_pos < fk_val_pos


def test_schema_module_exports():
    assert schema.DeferredIndexLifecycle is DeferredIndexLifecycle
    assert schema.IndexWindow is IndexWindow
    assert schema.ForeignKeyWindow is ForeignKeyWindow
    assert schema.rebuild_indexes_parallel is rebuild_indexes_parallel
    assert schema.topological_fk_sort is topological_fk_sort
    assert schema.concurrent_index_sql is concurrent_index_sql
