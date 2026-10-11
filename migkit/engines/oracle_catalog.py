"""What Oracle says about itself and its schema beyond its rows: its version
and product, the rules and indexes of its columns and tables, the room it
has, the writers beside migkit, and the work a move leaves to hands
(backlog P0-PARITY).

Read from the catalogue every account can read (`all_*`), the privileged
views (`dba_*`, `v$*`) only where the account is granted them - and a
question the account cannot ask is left out of the answer, never answered
as none: `capacity` and `free_bytes` leave a key out, `_handwork` records
the probe as unknown (`handwork.Inventory.unknown`).

None of the statements here has run against a server yet; each is a
question of a view Oracle documents, listed in `capabilities.UNPROVED`
until a sandbox run proves it.
"""
from .dbapi import MARK


class OracleCatalog:
    """The parts of the Oracle engine that read the catalogue and make
    schema objects; mixed into `OracleEngine`."""

    # ---- the servers' versions and products -------------------------------

    def _product(self, side, db):
        """(product, version) of the server, as it names itself: the
        banner's product row (`Oracle Database 23ai Free`) and the full
        release number (`23.26.0.0.0`; before 18c the four-part one)."""
        try:
            got = self._rows(side, db, "select product, version_full from"
                                       " product_component_version where"
                                       " rownum = 1")
        except Exception:  # noqa: BLE001 - before 18c: the older column
            got = None
        if got and got[0][1] is not None:
            return str(got[0][0] or ""), str(got[0][1])
        got = self._rows(side, db, "select product, version from"
                                   " product_component_version where"
                                   " rownum = 1")
        return (str(got[0][0] or ""), str(got[0][1])) if got else ("", "")

    def _server_versions(self):
        """(source, target) release, from the first database each side
        reads; a side that will not answer is None, said as unknown."""
        db = (self.databases() or [""])[0]
        out = []
        for side in ("src", "dst"):
            try:
                out.append(self._product(side, db)[1] or None)
            except Exception:  # noqa: BLE001 - unknown, said by assess
                out.append(None)
        return tuple(out)

    def _brand_probes(self):
        """({source fields}, {target fields}): the product row and the
        release, which name the software that answers
        (`variants.SIGNATURES['oracle']`)."""
        db = (self.databases() or [""])[0]
        out = []
        for side in ("src", "dst"):
            try:
                product, version = self._product(side, db)
            except Exception:  # noqa: BLE001 - an unidentified brand
                out.append({})
                continue
            out.append({"product": product, "version": version})
        return tuple(out)

    # ---- the rules of columns, indexes ------------------------------------

    def neutral_column_rules(self, side, db, table):
        """{column: {"null", "default", "identity"}} of the columns the
        server does not compute (a virtual column is its expression's).
        A default is the expression as the catalogue keeps it; an identity
        column's own sequence is not a default."""
        out = {}
        for name, null, default, virtual, identity in self._rows(
                side, db, "select column_name, nullable, data_default,"
                          " virtual_column, identity_column from"
                          f" all_tab_cols where owner = {MARK} and"
                          f" table_name = {MARK} and hidden_column = 'NO'"
                          " order by column_id",
                (self._owner(side, db), self._q(table)[1:-1])):
            if str(virtual) == "YES":
                continue
            ident = str(identity) == "YES"
            text = " ".join(str(default or "").split())
            out[self._said(str(name))] = {
                "null": str(null) == "Y",
                "default": None if ident or not text
                or text.upper() == "NULL" else text,
                "identity": ident}
        return out

    def neutral_indexes(self, side, db, table):
        """[(name, unique, [columns], plain)] of a table's indexes but its
        key's and its large values'; `plain` where the columns say it all
        - an ordinary B-tree over ascending columns, not an expression, a
        bitmap, a reverse key or a descending column."""
        got = {}
        for name, unique, kind, col, desc in self._rows(
                side, db, "select i.index_name, i.uniqueness, i.index_type,"
                          " c.column_name, c.descend from all_indexes i"
                          " join all_ind_columns c on c.index_owner ="
                          " i.owner and c.index_name = i.index_name where"
                          f" i.owner = {MARK} and i.table_name = {MARK} and"
                          " i.index_type not in ('LOB', 'IOT - TOP') and"
                          " not exists (select 1 from all_constraints k"
                          " where k.owner = i.owner and k.table_name ="
                          " i.table_name and k.constraint_type = 'P' and"
                          " k.index_name = i.index_name) order by"
                          " i.index_name, c.column_position",
                (self._owner(side, db), self._q(table)[1:-1])):
            one = got.setdefault(str(name), [str(unique) == "UNIQUE", [],
                                             str(kind) == "NORMAL"])
            one[1].append(self._said(str(col)))
            if str(desc) != "ASC" or str(col).startswith("SYS_NC"):
                one[2] = False
        return [(self._said(n), u, c, plain)
                for n, (u, c, plain) in sorted(got.items())]

    def neutral_create_index_sql(self, side, db, table, name, unique,
                                 columns):
        return (f"create {'unique ' if unique else ''}index"
                f" {self._q(self._owner(side, db))}.{self._q(name)} on"
                f" {self._qualified(side, db, table)} ("
                + ", ".join(self._q(c) for c in columns) + ")")

    def execute_ddl(self, side, db, sql):
        self._target_only(side, "change a schema")
        self.neutral_create_code(side, db, sql)

    # ---- tables the target lacks ------------------------------------------

    def create_missing(self, db, log=None):
        """The tables in scope the target lacks, made from the source's
        columns and rules before any is filled - by the pair every engine
        copies through, which names what it made and leaves a table already
        there as it is."""
        return self._as_pair().create_missing(db, log)

    def finish_created(self, db, log=None):
        """The source's indexes on the tables `create_missing` made, once
        their rows are in."""
        return self._as_pair().finish_created(db, log)

    # ---- the room a side has ----------------------------------------------

    def _one(self, side, db, sql, args=None):
        """The first value of a statement, or None when it answers nothing
        or the account may not ask."""
        try:
            got = self._rows(side, db, sql, args)
        except Exception:  # noqa: BLE001 - left out of the answer
            return None
        return got[0][0] if got and got[0] else None

    def capacity(self, side, db):
        """What the server gives a move (`sizing`): its CPUs, the sessions
        it has free under `sessions`, the user sessions busy now, and
        whether it is a standby. A key the account may not read is left
        out."""
        out = {}
        cpus = self._one(side, db, "select value from v$osstat where"
                                   " stat_name = 'NUM_CPUS'")
        if cpus is not None:
            out["cpus"] = int(cpus)
        limit = self._one(side, db, "select value from v$parameter where"
                                    " name = 'sessions'")
        used = self._one(side, db, "select count(*) from v$session where"
                                   " type = 'USER'")
        if limit is not None and used is not None:
            out["free_connections"] = max(0, int(limit) - int(used))
        busy = self._one(side, db, "select count(*) from v$session where"
                                   " type = 'USER' and status = 'ACTIVE'"
                                   " and sid <> sys_context('userenv',"
                                   " 'sid')")
        if busy is not None:
            out["running"] = int(busy)
        role = self._one(side, db, "select database_role from v$database")
        if role is not None:
            out["replica"] = "STANDBY" in str(role).upper()
        return out

    def free_bytes(self, side, db):
        """The room the schema's own tablespace has, as the server reports
        it: the bytes its data files can still grow to or hold free, held
        to what the owner's quota allows - and None where neither is
        readable, never a guess. Disk beneath the data files is the
        operator's to count."""
        owner = self._owner(side, db)
        space = self._one(
            side, db, "select nvl(sum(f.bytes), 0) + (select"
                      " nvl(sum(greatest(d.maxbytes - d.bytes, 0)), 0) from"
                      " dba_data_files d where d.tablespace_name ="
                      " u.default_tablespace and d.autoextensible = 'YES')"
                      " from dba_users u left join dba_free_space f on"
                      " f.tablespace_name = u.default_tablespace where"
                      f" u.username = {MARK} group by u.default_tablespace",
            (owner,))
        quota = self._one(
            side, db, "select case when max_bytes = -1 then null else"
                      " max_bytes - bytes end from dba_ts_quotas q where"
                      f" q.username = {MARK} and q.tablespace_name ="
                      " (select default_tablespace from dba_users where"
                      f" username = {MARK})", (owner, owner))
        if space is None:
            return None
        return int(space) if quota is None else min(int(space), int(quota))

    # ---- other writers ----------------------------------------------------

    def stream_writers(self, db):
        """[(what, pausable)] writing into the target besides a repair:
        migkit's own tail, which can be paused, and what the server shows
        of others - a Streams or GoldenGate apply process, a scheduler job
        of the schema, a session of the schema's owner that is not
        migkit's. Those cannot be paused from here; each is named."""
        out = list(super().stream_writers(db))
        owner = self._owner("dst", db)
        for sql, what in (
                ("select apply_name from dba_apply where status ="
                 " 'ENABLED'", "an apply process, %s"),
                (f"select job_name from dba_scheduler_jobs where owner ="
                 f" {MARK} and enabled = 'TRUE'", "a scheduler job, %s"),
                (f"select distinct nvl(program, module) from v$session"
                 f" where username = {MARK} and type = 'USER' and sid <>"
                 f" sys_context('userenv', 'sid')",
                 "a session of the schema's owner, %s")):
            try:
                got = self._rows("dst", db, sql,
                                 (owner,) if MARK in sql else None)
            except Exception:  # noqa: BLE001 - not readable: not said
                continue
            out += [(what % str(r[0]), False) for r in got]
        return out

    # ---- what the move leaves to hands ------------------------------------

    def _keyless(self, side, db):
        """The schema's tables the hop takes that no primary key or unique
        constraint, and no unique index, names a row of."""
        return [self._said(str(t)) for (t,) in self._rows(
            side, db, "select t.table_name from all_tables t where t.owner ="
                      f" {MARK} and t.nested = 'NO' and t.secondary = 'N'"
                      " and t.temporary = 'N' and t.table_name not like"
                      f" '{self.OWN_PREFIX}%' escape '\\' and not exists"
                      " (select 1 from all_constraints c where c.owner ="
                      " t.owner and c.table_name = t.table_name and"
                      " c.constraint_type in ('P', 'U') and c.status ="
                      " 'ENABLED') and not exists (select 1 from"
                      " all_indexes i where i.table_owner = t.owner and"
                      " i.table_name = t.table_name and i.uniqueness ="
                      " 'UNIQUE') order by 1", (self._owner(side, db),))
            if self.in_scope(db, self._said(str(t)))]

    def _handwork(self):
        """The work the move will not do, counted and named
        (`migkit.handwork`): tables nothing names a row of, objects no data
        mover carries (database links, synonyms, queues, external tables,
        the contents of temporary tables), and what runs by itself and
        wants a decision (scheduler jobs, which run on one side at a time).
        A probe the account cannot run is unknown, never zero."""
        from .. import handwork
        inv = handwork.Inventory()
        for db in self.databases():
            owner = self._owner("src", db)
            try:
                inv.add("no-row-key", db, "tables", self._keyless("src", db))
            except Exception as e:  # noqa: BLE001 - unknown, not zero
                inv.unknown("no-row-key", db,
                            f"catalogue query failed: {_first(e)}")
            for kind, what, sql in (
                    ("not-carried", "database links",
                     f"select db_link from all_db_links where owner ="
                     f" {MARK}"),
                    ("not-carried", "synonyms",
                     f"select synonym_name from all_synonyms where owner ="
                     f" {MARK}"),
                    ("not-carried", "external tables (their files)",
                     f"select table_name from all_external_tables where"
                     f" owner = {MARK}"),
                    ("not-carried", "temporary tables (their contents)",
                     f"select table_name from all_tables where owner ="
                     f" {MARK} and temporary = 'Y'"),
                    ("not-carried", "queues",
                     f"select name from all_queues where owner = {MARK}"),
                    ("decide-then-apply",
                     "scheduler jobs to switch on at cutover",
                     f"select job_name from all_scheduler_jobs where owner"
                     f" = {MARK} and enabled = 'TRUE'")):
                try:
                    found = [self._said(str(r[0])) for r in self._rows(
                        "src", db, sql, (owner,))]
                except Exception as e:  # noqa: BLE001 - unknown, not zero
                    inv.unknown(kind, db, f"cannot read {what}:"
                                          f" {_first(e)}")
                    continue
                inv.add(kind, db, what, found)
        return inv


def _first(error):
    """An error's first line, short enough for a report."""
    return (str(error).strip().splitlines() or [""])[0][:90]
