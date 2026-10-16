"""SAP ASE (Sybase) as one side of a pair (backlog 34).

Through FreeTDS's ODBC driver at protocol 5.0, which ASE speaks: the TDS
driver that installs with pip stops at SQL Server's 7.x (measured,
`unrecognized tds version: 5.0`). The endpoint's `odbc_driver` names the
driver's library where it is not in the usual places. A hop's database is
an ASE database, and its tables are named by their owner (`dbo.orders`).
Rows are compared through the in-process renderer every engine without a
canonical SQL rendering shares.

Written 2026-09-25 without a server to run it against: SAP's ASE image
runs on x86 only, and this machine is arm64.
"""
from .. import pieces
from . import hooks
from .base import Engine, Result
from .dbapi import MARK, DbapiRows

#: the ways built here and run against no server - settings, identity
#: counters, the server's own insert-select, accounts, statistics, the
#: rollback point, fence, confirm and delta (R13-ASE), proved on fakes
#: answering the ASE 16 reference's catalogue rows; the whole list, the
#: matrix reads these two names from this source (`capabilities`)
UNPROVED_ON = "built; not run on a real SAP ASE server"
UNPROVED_ITEMS = (
    "cutover.counters cutover.freeze cutover.rollback move.bulk "
    "scope.copy scope.counters scope.deep scope.listings scope.names "
    "scope.row-filter verify.confirm verify.delta verify.fence "
    "verify.table verify.users")

#: where FreeTDS's ODBC driver is installed by Homebrew and by apt
DRIVERS = ("/opt/homebrew/lib/libtdsodbc.so", "/usr/local/lib/libtdsodbc.so",
           "/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so",
           "/usr/lib/aarch64-linux-gnu/odbc/libtdsodbc.so")

#: the declared type, with the numbers a counterpart needs
DECLARED = ("t.name + case when t.name in ('numeric', 'decimal') then '('"
            " + convert(varchar(4), c.prec) + ',' + convert(varchar(4),"
            " c.scale) + ')' when t.name in ('char', 'varchar', 'binary',"
            " 'varbinary', 'nchar', 'nvarchar', 'unichar', 'univarchar')"
            " then '(' + convert(varchar(6), c.length) + ')' else '' end")


class AseEngine(DbapiRows, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "ase"
    CHARSETS = True
    SQL_DIALECT = None
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " an ASE one nobody has run")
    OVER_NETWORK = True
    PARAMSTYLE = "qmark"
    LIMIT = "top"
    #: a `char`, `nchar` or `unichar` that takes no NULL is stored padded
    #: to its length, by the ASE reference's own account; read without the
    #: padding, as every other engine's fixed-length text is compared
    PADDED = ("char", "nchar", "unichar")
    #: large values: sized by the server (`datalength`), and every session
    #: takes a text or image value whole (`_connect`) - read whole, bound
    #: whole, and hashed here as it is folded
    LARGE_VALUES = pieces.Sql(
        "sized by the server, text and image values read whole, never cut"
        " at the session's text size",
        length="datalength({c})",
        types=("text", "unitext", "image", "varchar", "varbinary",
               "univarchar", "nvarchar"))
    #: what a session takes of a text or image value: all of it. ASE hands a
    #: session 32,768 bytes of one unless the session says otherwise, and
    #: cuts the rest off without a word - a copy and a check read through
    #: the same cut would each see the same first 32 KB and agree
    TEXT_SIZE = 2147483647

    def _driver(self, side):
        import os
        ep = self.hop.source if side == "src" else self.hop.target
        named = (ep.options or {}).get("odbc_driver")
        if named:
            return named
        for path in DRIVERS:
            if os.path.exists(path):
                return path
        raise SystemExit("FreeTDS's ODBC driver is not on this machine, and"
                         " SAP ASE is reached through it: brew install"
                         " freetds, or apt-get install tdsodbc - or say"
                         " where it is with odbc_driver in the endpoint's"
                         " options")

    def _connection_string(self, side, db):
        ep = self.hop.source if side == "src" else self.hop.target
        # a value in braces is taken whole; a brace in it is doubled
        secret = "{" + (ep.password or "").replace("}", "}}") + "}"
        return (f"DRIVER={{{self._driver(side)}}};SERVER={ep.host};"
                f"PORT={ep.port or 5000};TDS_Version=5.0;"
                f"UID={ep.user};PWD={secret};"
                f"DATABASE={self._d(side, db)};ClientCharset=UTF-8")

    def _connect(self, side, db):
        try:
            import pyodbc
        except ImportError as e:
            # measured: the module installs, and then cannot load without
            # the ODBC manager (`libodbc.2.dylib ... no such file`)
            raise SystemExit("ODBC is not usable on this machine"
                             f" ({str(e).splitlines()[0][:120]}): brew"
                             " install unixodbc freetds, or apt-get install"
                             " unixodbc tdsodbc")
        conn = pyodbc.connect(self._connection_string(side, db),
                              timeout=15)
        # names in double quotes are names, not strings
        conn.cursor().execute("set quoted_identifier on")
        conn.cursor().execute(f"set textsize {int(self.TEXT_SIZE)}")
        return conn

    def databases(self):
        return list(self.hop.databases)

    def check_deep(self, db):
        """Text stored twice as UTF-8 on the source (`_mojibake`)."""
        return [self._mojibake(db)]

    def neutral_tables(self, side, db):
        return [t for (t,) in self._rows(
            side, db, "select user_name(uid) + '.' + name from sysobjects"
                      " where type = 'U' order by 1")
            if self.in_scope(db, t)]

    def neutral_columns(self, side, db, table):
        return [(n, t) for n, t in self._rows(
            side, db, f"select c.name, {DECLARED} from syscolumns c join"
                      " systypes t on t.usertype = c.usertype where c.id ="
                      f" object_id({MARK}) order by c.colid",
            (table,))]

    def neutral_key(self, side, db, table):
        """The columns of the index ASE marks as the primary key's."""
        return [n for (n,) in self._rows(
            side, db, "select index_col(object_name(i.id), i.indid,"
                      " n.number) from sysindexes i, master..spt_values n"
                      f" where i.id = object_id({MARK}) and i.status & 2048"
                      " = 2048 and n.type = 'P' and n.number between 1 and"
                      " i.keycnt and index_col(object_name(i.id), i.indid,"
                      " n.number) is not null order by n.number",
            (table,))]

    def _before_insert(self, cur, side, db, table):
        """Explicit values go into an identity column only with identity
        insert on for the table."""
        self._run(cur, "select count(*) from syscolumns where id ="
                       f" object_id({MARK}) and status & 128 = 128", (table,))
        if not cur.fetchone()[0]:
            return None
        quoted = self._qualified(side, db, table)
        self._run(cur, f"set identity_insert {quoted} on")
        return f"set identity_insert {quoted} off"

    def check_schema(self, db):
        verifiers = hooks.workflow("verifiers")
        return verifiers.compare_schema(self._as_pair(), db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    # ---- the ASE 16 reference, not run on a server (`UNPROVED_ON`) ---------

    #: the settings that change what a value is or how it compares: the
    #: character set and sort orders, Unicode's handling, and the cut of a
    #: binary value's trailing zeros
    CRITICAL_PARAMS = ("default character set id", "default sortorder id",
                       "default unicode sortorder",
                       "enable unicode normalization",
                       "enable unicode conversions",
                       "enable surrogate processing",
                       "disable varbinary truncation", "default language id")
    #: every setting the server runs with, as `sp_configure` reports it
    SETTINGS = ("select c.name, isnull(rtrim(cc.value2), convert(varchar(32),"
                " cc.value)) from master..sysconfigures c,"
                " master..syscurconfigs cc where cc.config = c.config"
                " union all select '@@version_number', convert(varchar(32),"
                " @@version_number)")

    def _ase_settings(self, side, db):
        """{setting: value} of `side`, or what stopped the reading."""
        try:
            return {str(n).strip(): None if v is None else str(v).strip()
                    for n, v in self._rows(side, db, self.SETTINGS)}
        except Exception as e:  # noqa: BLE001 - said, as the reading's
            return {self.UNREADABLE: str(e).splitlines()[0][:160]
                    if str(e) else type(e).__name__}

    def check_params(self, db):
        """Both servers' settings through the report every engine's go
        through: one that changes what a value is or how it compares
        (`CRITICAL_PARAMS`) fails it."""
        from . import same_engine
        if not same_engine(self):
            return [Result("params", f"{db} params", "skip",
                           "the target is another engine: its settings are"
                           " not ASE's, and check compares the values")]
        return self._param_result(
            db, self._ase_settings("src", db), self._ase_settings("dst", db),
            self.CRITICAL_PARAMS, "set the target server's configuration as"
            " the source's (its character set and sort order are changed"
            " only with the server's own reconfiguration and a restart)")

    # identity columns: ASE has no sequences; a table's identity column
    # gives the next number the server says (`next_identity`), and a
    # target behind it is moved on with `identity_burn_max`
    #: (table, column, next number) of each table with an identity column
    IDENTITIES = ("select user_name(o.uid) + '.' + o.name, c.name,"
                  " next_identity(user_name(o.uid) + '.' + o.name) from"
                  " sysobjects o, syscolumns c where o.type = 'U' and c.id ="
                  " o.id and c.status & 128 = 128")

    def _identities(self, side, db, rows=None):
        rows = self._rows(side, db, self.IDENTITIES) if rows is None else rows
        return {str(t).strip(): (str(c).strip(), None if n is None
                                 else int(str(n).strip()))
                for t, c, n in rows if self.in_scope(db, str(t).strip())}

    @staticmethod
    def _burn_sql(table, last):
        name = str(table).replace("'", "''")
        return (f"exec sp_chgattribute '{name}', 'identity_burn_max', 0,"
                f" '{int(last)}'")

    def check_autoinc(self, db):
        """Each identity column's next number on the target at or past the
        source's: one behind would give a number the source already gave.
        A target of another engine keeps its own counters (said)."""
        from . import same_engine
        if not same_engine(self):
            return [Result("autoinc", db, "skip",
                           "the target is another engine: its counters are"
                           " carried by the move")]
        src, dst = self._identities("src", db), self._identities("dst", db)
        behind = [(t, c, n, dst[t][1]) for t, (c, n) in sorted(src.items())
                  if t in dst and n is not None and (dst[t][1] or 0) < n]
        if not src:
            return [Result("autoinc", db, "ok", "no identity columns")]
        if behind:
            shown = "; ".join(f"{t}.{c} next {d} < source {n}"
                              for t, c, n, d in behind[:6])
            return [Result("autoinc", db, "diff",
                           f"{len(behind)} identity columns behind the"
                           f" source: {shown}", "",
                           "; ".join(self._burn_sql(t, n - 1)
                                     for t, _, n, _ in behind[:6]))]
        return [Result("autoinc", db, "ok",
                       f"{len(src)} identity columns, each next number at"
                       " or past the source's")]

    # accounts: logins are the server's (master), users and groups the
    # database's, server roles granted to logins; a login's password is
    # kept as a hash `create login` does not take, so create takes the
    # passwords file's
    ACCOUNT_PASSWORDS = ("ASE keeps a login's password as a hash it does"
                         " not take back: create sets the passwords file's")
    ACCOUNT_ATTRS = ("kind",)
    #: (database user, login, login locked) of the database
    USERS = ("select u.name, l.name, l.status & 2 from sysusers u,"
             " master..syslogins l where u.suid = l.suid and u.uid <> u.gid")
    #: (user, group) of each user in a group other than public
    GROUPS_HELD = ("select u.name, g.name from sysusers u, sysusers g where"
                   " u.gid = g.uid and u.uid <> u.gid and g.uid <> 0")
    GROUPS = "select name from sysusers where uid = gid and uid <> 0"
    #: (login, server role) of the roles granted to logins
    ROLES_HELD = ("select l.name, r.name from master..sysloginroles lr,"
                  " master..syssrvroles r, master..syslogins l where"
                  " lr.srid = r.srid and lr.suid = l.suid")
    #: (grantee, owner.table, action) of the grants on the database's tables
    GRANTS = ("select user_name(p.uid), user_name(o.uid) + '.' + o.name,"
              " p.action from sysprotects p, sysobjects o where p.id = o.id"
              " and o.type = 'U' and p.protecttype in (0, 1)")
    #: `sysprotects.action` by the ASE reference's numbers
    ACTIONS = {151: "REFERENCES", 193: "SELECT", 195: "INSERT",
               196: "DELETE", 197: "UPDATE", 280: "TRUNCATE TABLE",
               282: "DELETE STATISTICS", 320: "UPDATE STATISTICS"}
    #: the names the server makes for itself
    SERVER_OWN = {"sa", "dbo", "guest", "public", "probe", "sa_role",
                  "sso_role", "oper_role", "sybase_ts_role",
                  "navigator_role", "replication_role", "dtm_tm_role",
                  "ha_role", "mon_role", "js_admin_role", "js_user_role",
                  "js_client_role", "messaging_role", "web_role",
                  "webservices_role", "keycustodian_role", "sap_role"}

    def accounts(self, side):
        """The accounts of `side` in `Engine.accounts`' shape: each
        database user (its login's lock said by `login`), each group and
        server role as a role, the groups and roles each holds, and the
        hop's tables' grants; no password (`ACCOUNT_PASSWORDS`)."""
        dbs = list(self.hop.databases or [])
        if not dbs:
            return {}
        out = {}

        def acct(key, kind, login=False):
            return out.setdefault(key, {"login": login, "roles": [],
                                        "grants": [], "hash": None,
                                        "attrs": {"kind": kind}})

        def own(name):
            return name is None or str(name).strip().lower() in \
                self.SERVER_OWN
        logins = {}
        for db in dbs:
            for user, login, locked in self._rows(side, db, self.USERS):
                if not own(user):
                    logins[str(login).strip()] = str(user).strip()
                    acct(f"user:{str(user).strip()}", "user",
                         not int(locked or 0))
            for (g,) in self._rows(side, db, self.GROUPS):
                if not own(g):
                    acct(f"role:{str(g).strip()}", "group")
            for user, g in self._rows(side, db, self.GROUPS_HELD):
                if not own(user) and not own(g):
                    acct(f"user:{str(user).strip()}", "user")["roles"].append(
                        str(g).strip())
            for who, table, action in self._rows(side, db, self.GRANTS):
                priv = self.ACTIONS.get(int(action))
                table = str(table).strip()
                if own(who) or priv is None or not self.in_scope(db, table):
                    continue
                key = (f"user:{str(who).strip()}" if f"user:{str(who).strip()}"
                       in out else f"role:{str(who).strip()}")
                acct(key, "user" if key.startswith("user:") else "group")[
                    "grants"].append(f"{priv} ON {db}.{table}")
        for login, role in self._rows(side, dbs[0], self.ROLES_HELD):
            user = logins.get(str(login).strip())
            if user is None or own(role):
                continue
            acct(f"role:{str(role).strip()}", "server role")
            out[f"user:{user}"]["roles"].append(str(role).strip())
        for a in out.values():
            a["roles"], a["grants"] = sorted(set(a["roles"])), sorted(
                set(a["grants"]))
        return out

    def account_steps(self, src, dst, passwords):
        """What makes the target's logins, users, groups, server roles and
        grants the source's. A login is made only with the passwords
        file's password (skipped, said, without one)."""
        steps, skipped = [], []
        passwords = passwords or {}
        db = (self.hop.databases or [""])[0]

        def step(key, run, undo, made=False, given=False):
            shown = run if not given else run.split(" with passwd")[0] + \
                " with passwd ***"
            steps.append({"key": key, "shown": shown, "run": run,
                          "undo": undo, "made": made, "given": given})
        order = sorted(src.items(), key=lambda kv: (
            kv[0].startswith("user:"), kv[0]))
        for key, a in order:
            kind, _, name = key.partition(":")
            have = dst.get(key) or {"roles": [], "grants": []}
            sort = (a.get("attrs") or {}).get("kind")
            q = self._q(name)
            if key not in dst:
                if sort == "server role":
                    step(key, f"create role {q}", f"drop role {q}", made=True)
                elif sort == "group":
                    step(key, f"exec sp_addgroup '{name}'",
                         f"exec sp_dropgroup '{name}'", made=True)
                else:
                    secret = passwords.get(name) or passwords.get(key)
                    if not secret:
                        skipped.append((key, "ASE does not take a password's"
                                             " hash back: give its password"
                                             " in the passwords file"))
                        continue
                    lit = str(secret).replace('"', '""')
                    step(key, f"create login {q} with passwd \"{lit}\""
                              f" default database {self._q(db)}",
                         f"drop login {q}", made=True, given=True)
                    step(key, f"exec sp_adduser '{name}'",
                         f"exec sp_dropuser '{name}'", made=True)
            for r in a.get("roles", []):
                if r in have["roles"]:
                    continue
                kind_r = (src.get(f"role:{r}") or {}).get("attrs", {}).get(
                    "kind")
                if kind_r == "group":
                    step(key, f"exec sp_changegroup '{r}', '{name}'",
                         f"exec sp_changegroup 'public', '{name}'")
                else:
                    step(key, f"grant role {self._q(r)} to {q}",
                         f"revoke role {self._q(r)} from {q}")
            for g in a.get("grants", []):
                if g in have["grants"]:
                    continue
                priv, _, on = g.partition(" ON ")
                where, _, table = on.partition(".")
                obj = self._qualified("dst", where, table)
                step(key, f"grant {priv.lower()} on {obj} to {q}",
                     f"revoke {priv.lower()} on {obj} from {q}")
        return steps, skipped

    def account_run(self, what):
        conn = self._connect("dst", (self.hop.databases or [""])[0])
        try:
            self._run(conn.cursor(), what)
            conn.commit()
        finally:
            conn.close()

    # statistics: how much of each table changed since its statistics
    # (`datachange`), read on the target, decide each
    # (`decide.ase_statistics`)
    STATS_FACTS = ("select user_name(o.uid) + '.' + o.name,"
                   " datachange(user_name(o.uid) + '.' + o.name, null,"
                   " null), (select count(*) from sysindexes i where i.id ="
                   " o.id and i.indid between 1 and 254), (select count(*)"
                   " from sysstatistics s where s.id = o.id) from sysobjects"
                   " o where o.type = 'U'")

    def settle_target(self, db, from_source=True):
        """Each table the move wrote whose statistics stand further from
        its rows than `STALE_STATS_RATIO` updated again: on every column
        of each index where it has indexes (`decide.ase_statistics`)."""
        from .. import decide
        tables = self.neutral_tables("dst", db)
        if not tables:
            return None
        facts = {str(r[0]).strip(): r for r in self._rows(
            "dst", db, self.STATS_FACTS)}
        done = {}
        conn = self._connect("dst", db)
        try:
            cur = conn.cursor()
            for t in tables:
                row = facts.get(t) or (t, None, 0, 0)
                got = decide.ase_statistics({
                    "changed": None if row[1] is None else float(row[1]),
                    "gathered": int(row[3] or 0),
                    "indexes": int(row[2] or 0),
                    "ratio": self.STALE_STATS_RATIO})
                if got.path == "current":
                    continue
                how = "update index statistics" if got.path == "index" \
                    else "update statistics"
                self._run(cur, f"{how} {self._qualified('dst', db, t)}")
                done[got.path] = done.get(got.path, 0) + 1
            conn.commit()
        finally:
            conn.close()
        if not done:
            return f"statistics current on {len(tables)} tables"
        words = {"index": "every index's columns", "table": "the table"}
        return "statistics updated on " + ", ".join(
            f"{n} tables ({words[p]})" for p, n in sorted(done.items()))

    # the rollback point: the identity counters read in one transaction
    # at isolation level 3, with the database timestamp where it answers
    def snapshot_state(self, db, state_dir, kind="all"):
        """The target's identity counters where they stand, which a
        rollback sets back (`restore_sequences`), and the database
        timestamp kept with them where it answers
        (`decide.ase_snapshot_mark`); the rows a repair writes are kept by
        the repair's own record."""
        from .. import decide
        conn = self._connect("dst", db)
        try:
            cur = conn.cursor()
            self._run(cur, "set transaction isolation level 3")
            self._run(cur, self.IDENTITIES)
            got_ids = self._identities("dst", db, cur.fetchall())
            try:
                self._run(cur, "select @@dbts")
                raw = cur.fetchall()[0][0]
                dbts = raw.hex() if isinstance(raw, (bytes, bytearray)) \
                    else str(raw).strip()
                why = None if dbts else "the database said nothing"
            except Exception as e:  # noqa: BLE001 - said in the mark
                dbts, why = None, str(e).splitlines()[0][:120] or "refused"
            got = decide.ase_snapshot_mark({"dbts": why})
            conn.commit()
        finally:
            conn.close()
        lines = [f"{t}|{n - 1}" for t, (_, n) in sorted(got_ids.items())
                 if n is not None]
        (state_dir / "dst-ase-counters.txt").write_text(
            "\n".join(lines) + ("\n" if lines else ""))
        (state_dir / "dst-ase-mark.txt").write_text(
            f"{got.path}|{dbts if got.path == 'timestamp mark' else ''}|"
            f"{got.reason}\n")

    def restore_sequences(self, db, state_dir):
        path = state_dir / "dst-ase-counters.txt"
        if not path.exists():
            return []
        return [self._burn_sql(name, int(value)) + ";"
                for name, _, value in (line.rpartition("|") for line in
                                       path.read_text().splitlines() if line)]

    # the server's own insert-select (`decide.ase_bulk_way`), else the copier
    #: (table, rows, data pages) of the database, as `sp_spaceused` counts
    BULK_FACTS = ("select user_name(uid) + '.' + name, row_count(db_id(),"
                  " id), data_pages(db_id(), id) from sysobjects where type"
                  " = 'U'")
    #: the page size, and the pages free on the log segment
    LOG_FACTS = ("select @@maxpagesize, lct_admin('logsegment_freepages',"
                 " db_id())")

    def _same_place(self):
        s, t = self.hop.source, self.hop.target
        return (s.host, s.port) == (t.host, t.port)

    def _empty(self, db, table):
        try:
            return not self._rows("dst", db, "select top 1 1 from"
                                  f" {self._qualified('dst', db, table)}")
        except Exception:  # noqa: BLE001 - no table yet: the copier's
            return False

    def _insert_select(self, db, table, log):
        """One table inserted by the server from the source database,
        identity values kept, its count checked on both sides: a count
        that differs stops the move for the table."""
        cols = ", ".join(self._q(n) for n, _ in self.neutral_columns(
            "src", db, table))
        dst = self._qualified("dst", db, table)
        # the source by its database's name: the statement runs in the
        # target's
        src = f"{self._q(self._d('src', db))}.{self._qualified('src', db, table)}"
        conn = self._connect("dst", db)
        try:
            cur = conn.cursor()
            after = self._before_insert(cur, "dst", db, table)
            self._run(cur, f"insert into {dst} ({cols}) select {cols} from"
                           f" {src}")
            if after:
                self._run(cur, after)
            self._run(cur, f"select count(*) from {dst}")
            moved = int(cur.fetchall()[0][0])
            self._run(cur, f"select count(*) from {src}")
            read = int(cur.fetchall()[0][0])
            if moved != read:
                conn.rollback()
                raise SystemExit(
                    f"{table}: the server inserted {moved} rows of {read}."
                    " The table is not taken as moved; move it with the"
                    " table copier")
            conn.commit()
        finally:
            conn.close()
        if log:
            log(f"{table}: {moved} rows inserted by the server")

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each table carried by the server's own insert-select where the
        decision finds it safe (`decide.ase_bulk_way`, from the server's
        size of it and the target's log), else by the table copier."""
        from .. import decide
        said = log or (lambda m: None)
        facts = {str(t).strip(): (r, p) for t, r, p in self._rows(
            "src", db, self.BULK_FACTS)}
        place, steps, pair = self._same_place(), [], []
        page, free = None, None
        try:
            got = self._rows("dst", db, self.LOG_FACTS)
            page, free = (int(got[0][0]), int(got[0][1])) if got else \
                (None, None)
        except Exception:  # noqa: BLE001 - not read: the copier's
            pass
        for t in list(tables):
            rows, pages = facts.get(t, (None, None))
            got = decide.ase_bulk_way({
                "rows": None if rows is None else int(rows),
                "pages": None if pages is None else int(pages),
                "page_size": page, "log_free": free,
                "empty": self._empty(db, t),
                "mapped": bool(self.through_pair(db, t)),
                "same_place": place})
            steps.append(f"{t}: {got.rung.said} - {got.reason}")
            if not go:
                continue
            if got.path == "copier":
                said(f"{t}: copied table by table - {got.reason}")
                if not pair:
                    pair.append(self._as_pair())
                from .mssql import _Unkept
                sch, _, tbl = str(t).rpartition(".")
                pair[0].move_table(db, sch, tbl, pair[0].READ_ROWS,
                                   _Unkept(), said)
            else:
                self._insert_select(db, t, log)
                if free is not None and pages is not None:
                    free -= int(pages)
        return steps

    # fence and confirm through the pair; delta by each table's timestamp
    # column (`decide.ase_delta_way`)
    #: (table, column) of each `timestamp` column of the database
    STAMPS = ("select user_name(o.uid) + '.' + o.name, c.name from"
              " sysobjects o, syscolumns c, systypes t where o.type = 'U'"
              " and c.id = o.id and t.usertype = c.usertype and t.name ="
              " 'timestamp'")
    #: whether the replication agent runs on the database (`sp_config_rep_
    #: agent`'s own reading: the database's status bit for it)
    AGENT = "select count(*) from master..sysdatabases where dbid = db_id()" \
            " and status3 & 1048576 = 1048576"

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        """The pair's fence: its marker row waited for on the target;
        nothing to wait on with no follow running."""
        return self._as_pair().fence_wait(db, lsn, timeout)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def delta_verify(self, db, limit=20000, log=None):
        """Only the rows changed since the last clean pass: the rows whose
        timestamp column is past the database timestamp the pass began
        at, each asked of both sides by key (`decide.ase_delta_way`). A
        table's mark moves only when it is clean; deletes are the full
        check's."""
        from .. import decide, durable
        from . import same_engine
        tables = self.neutral_tables("src", db)
        stamps = {str(t).strip(): str(c).strip() for t, c in
                  self._rows("src", db, self.STAMPS)}
        stamps = {t: c for t, c in stamps.items() if t in tables}
        try:
            agent = bool(self._rows("src", db, self.AGENT)[0][0])
        except Exception:  # noqa: BLE001 - not read: said as not running
            agent = False
        got = decide.ase_delta_way({"stamped": len(stamps),
                                    "tables": len(tables),
                                    "same": same_engine(self),
                                    "agent": agent})
        if got.path == "none":
            return [Result("delta", db, "skip",
                           "not verified by change: " + got.reason)]
        state = self.hop.report_dir(db) / "ase-delta.json"
        held = durable.read_state(state, "position")
        now, res = dict(held or {}), []
        for t in [t for t in tables if t not in stamps]:
            res.append(Result("delta", f"{db}.{t}", "skip",
                              "not verified by change: no timestamp column"))
        for t, col in sorted(stamps.items()):
            raw = self._rows("src", db, "select @@dbts")[0][0]
            mark = raw.hex() if isinstance(raw, (bytes, bytearray)) \
                else str(raw)
            if held is None or t not in held:
                now[t] = mark
                res.append(Result("delta", f"{db}.{t}", "ok",
                                  "baseline recorded, changes are tracked"
                                  " from this point on"))
                continue
            res.append(self._stamped_pass(db, t, col, held[t], limit, now,
                                          mark))
        durable.write_json(state, now)
        return res

    def _stamped_pass(self, db, table, col, since, limit, now, mark):
        """One table's rows stamped past `since`, compared by key."""
        key = self.neutral_key("src", db, table)
        if not key:
            return Result("delta", f"{db}.{table}", "skip",
                          "not verified by change: the table has no key")
        cols = [n for n, _ in self.neutral_columns("src", db, table)
                if n != col]
        sel = ", ".join(self._q(c) for c in cols)
        src = self._rows("src", db, f"select top {int(limit) + 1} {sel} from"
                         f" {self._qualified('src', db, table)} where"
                         f" {self._q(col)} > {MARK} order by"
                         f" {', '.join(self._q(k) for k in key)}",
                         (bytes.fromhex(since),))
        if len(src) > limit:
            return Result("delta", f"{db}.{table}", "warn",
                          f"more than {limit} rows changed since the last"
                          " clean pass: run check, which compares them all")
        at = [cols.index(k) for k in key]
        where = " and ".join(f"{self._q(k)} = {MARK}" for k in key)
        missing, changed = [], []
        for row in src:
            ks = [row[i] for i in at]
            got = self._rows("dst", db, f"select {sel} from"
                             f" {self._qualified('dst', db, table)} where"
                             f" {where}", ks)
            if not got:
                missing.append(ks)
            elif list(got[0]) != list(row):
                changed.append(ks)
        if missing or changed:
            return Result("delta", f"{db}.{table}", "diff",
                          f"{len(src)} changed rows: {len(missing)} missing"
                          f" on the target, {len(changed)} different", "",
                          "run check on the table, and repair")
        now[table] = mark
        return Result("delta", f"{db}.{table}", "ok",
                      f"{len(src)} rows changed since the last clean pass,"
                      " each the same on the target")
