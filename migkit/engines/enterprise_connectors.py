"""What a server refused for want of a grant, said as the grant it lacks;
and the large-value statements of TiDB, CockroachDB and OceanBase until
their engines take them (backlog P0-PARITY.lv.new).

A refused question is never answered for the server: no list of databases
made up, no count of 0, no empty change stream. It stops as a blocker that
names the grant, so "0 rows, ok" can never stand for "not allowed to look".
"""

import re

from .. import pieces
from ..decide import Blocked

#: server error codes that mean "not allowed", per family
GRANT_CODES = {"mongodb": {13}, "postgres": {"42501"}, "mysql": {1142, 1143,
                                                                 1227, 1044}}

#: what a refusal reads like where the driver gives no code
GRANT_WORDS = ("not authorized", "unauthorized", "permission denied",
               "insufficient privilege", "must be superuser",
               "access denied", "command denied")

#: action -> the grant it needs, per family
GRANTS = {
    "mongodb": {
        "listDatabases": "the listDatabases action on the cluster (the"
                         " readAnyDatabase or clusterMonitor role)",
        "serverStatus": "the serverStatus action on the cluster (the"
                        " clusterMonitor role)",
        "replSetGetStatus": "the replSetGetStatus action on the cluster"
                            " (the clusterMonitor role)",
        "collStats": "the collStats action on the collection (the read"
                     " role on its database)",
        "changeStream": "the changeStream and find actions on the database"
                        " (the read role), on a replica set or sharded"
                        " cluster",
        "find": "the find action on the collection (the read role on its"
                " database)",
    },
    "postgres": {
        "disable_triggers": "superuser to set session_replication_role, or"
                            " ownership of the table to disable its"
                            " triggers",
        "information_schema": "SELECT on the tables, which is all"
                              " information_schema shows of them",
        "replication": "the REPLICATION attribute on the role",
    },
    "mysql": {
        "disable_triggers": "SUPER or SYSTEM_VARIABLES_ADMIN, to set the"
                            " session's foreign_key_checks and sql_log_bin",
        "information_schema": "SELECT on the tables, which is all"
                              " information_schema shows of them",
        "replication": "REPLICATION SLAVE and REPLICATION CLIENT",
    },
    "mssql": {
        "snapshot_isolation": "ALTER DATABASE by its owner, to set"
                              " ALLOW_SNAPSHOT_ISOLATION ON",
    },
}

_FAMILY = {"mariadb": "mysql", "tidb": "mysql", "oceanbase": "mysql",
           "aurora-mysql": "mysql", "cockroachdb": "postgres",
           "aurora-postgres": "postgres", "documentdb": "mongodb",
           "cosmosdb-mongo": "mongodb"}


class MissingGrant(Blocked):
    """A server refused `action` for want of `grant`."""

    def __init__(self, engine, action, grant, error=None):
        self.engine, self.action, self.grant = engine, action, grant
        said = (str(error).splitlines() or [""])[0][:160] if error else ""
        super().__init__(f"{engine}: {action} was refused - the user needs"
                         f" {grant}" + (f" (the server said: {said})"
                                        if said else ""))


def family(engine):
    engine = str(engine or "").lower()
    return _FAMILY.get(engine, engine)


def is_grant_error(engine, exc):
    if exc is None:
        return False
    code = getattr(exc, "code", None)
    if code is None:
        code = getattr(exc, "pgcode", None) or getattr(exc, "sqlstate", None)
    if code is None and getattr(exc, "args", None) \
            and isinstance(exc.args[0], int):
        code = exc.args[0]
    if code is not None and code in GRANT_CODES.get(family(engine), ()):
        return True
    msg = str(exc).lower()
    return any(w in msg for w in GRANT_WORDS)


def _action_of(exc):
    """The command a MongoDB refusal names (`to execute command
    { listDatabases: 1 ...`), or None."""
    got = re.search(r"execute command \{\s*(\w+)", str(exc))
    return got.group(1) if got else None


def lacking(engine, exc, action=None):
    """The grant `exc` says the user lacks, in words, or None where `exc`
    is no refusal for want of a grant."""
    if not is_grant_error(engine, exc):
        return None
    action = action or _action_of(exc)
    known = GRANTS.get(family(engine), {})
    if action in known:
        return known[action]
    return (f"the privilege the server names for {action}" if action
            else "the privilege the server names")


def missing_grant(engine, action, exc=None):
    """Stop on a refused `action` as a blocker naming its grant. Never
    returns: there is no answer to stand in for one the server refused."""
    raise MissingGrant(engine, action,
                       lacking(engine, exc, action)
                       or GRANTS.get(family(engine), {}).get(action)
                       or f"the privilege the server names for {action}",
                       exc)


# The statements below are for ticket LVA to move into the engines' own
# LARGE_VALUES (tidb.py, cockroachdb.py, oceanbase.py); nothing reads them
# from here.
TiDBLargeValues = pieces.Sql(
    "TiDB large values: sized by length(), read in pieces via substring(), appended via concat(), hashed via md5()",
    length={t: "length({c})" for t in (
        "blob", "longblob", "mediumblob", "tinyblob", "text", "longtext",
        "mediumtext", "tinytext", "varchar", "char", "binary", "varbinary",
        "json")},
    types=(
        "blob", "longblob", "mediumblob", "tinyblob", "text", "longtext",
        "mediumtext", "tinytext", "varchar", "char", "binary", "varbinary", "json"
    ),
    piece="substring({c}, {at}, {n})",
    append="{c} = concat({c}, {v})",
    appends=("longtext", "mediumtext", "text", "tinytext", "longblob", "mediumblob", "blob", "tinyblob"),
    md5="md5({c})",
    md5_classes=("bytes", "text"),
)

CockroachDBLargeValues = pieces.Sql(
    "CockroachDB large values: sized by octet_length/length, read via substr, appended via ||, hashed via md5",
    length={
        "bytes": "octet_length({c})",
        "bytea": "octet_length({c})",
        "string": "length({c})",
        "text": "length({c})",
        "varchar": "length({c})",
        "jsonb": "octet_length({c}::text)",
    },
    types=("bytes", "bytea", "string", "text", "varchar", "jsonb"),
    piece="substr({c}, {at}, {n})",
    append="{c} = {c} || {v}",
    appends=("string", "text", "varchar", "bytes", "bytea"),
    md5="md5({c})",
    md5_classes=("bytes", "text"),
)

OceanBaseLargeValues = pieces.Sql(
    "OceanBase large values: MySQL-mode LOB handling with length, substring, concat, and md5",
    length={t: "length({c})" for t in (
        "blob", "longblob", "mediumblob", "text", "longtext", "varchar",
        "varbinary")},
    types=("blob", "longblob", "mediumblob", "text", "longtext", "varchar", "varbinary"),
    piece="substring({c}, {at}, {n})",
    append="{c} = concat({c}, {v})",
    appends=("longtext", "text", "longblob", "blob"),
    md5="md5({c})",
    md5_classes=("bytes", "text"),
)
