"""Db2 for z/OS as one side of a pair (backlog 34, W5), through `ibm_db`.

The endpoint's `database` is the subsystem's location name, and a hop's
databases are its schemas (creators). z/OS answers DRDA only from a client
licensed for Db2 Connect - the operator's licence file beside the IBM data
server driver, which `migkit doctor --install` puts in place once the terms
are accepted, or the subsystem activated for Db2 Connect by its
administrator; without either the connection is refused (SQL1598N) and
migkit says which of the two is missing.

What is reached, and what is not: tables are listed, described, copied and
compared through `SYSIBM`'s catalogue and the same keyed reads as every
DB-API engine, and rows compared through the renderer every engine
without a canonical SQL rendering shares. Changes are not followed: z/OS
hands its log out only through the IFI READS of IFCID 306, called by a
load module installed on the host in an APF-authorised library - vendor
code under licence - or through change tables IBM's replication product
fills. No open reader of either exists, so a z/OS hop moves by copies and
proves each by `check` (`reach_items` says so in `assess`).

Written 2026-09-30 without a z/OS to run it against; IBM Z Xplore gives a
free shared z/OS with Db2 for a manual proof.
"""
from .base import Result
from .db2 import Db2Engine, granted, large_values
from .dbapi import MARK

#: the declared type from `SYSIBM.SYSCOLUMNS`, with the numbers a
#: counterpart needs; a character column `FOR BIT DATA` (FOREIGNKEY 'B')
#: is bytes
DECLARED = ("case when coltype = 'DECIMAL' then 'DECIMAL(' ||"
            " rtrim(char(length)) || ',' || rtrim(char(scale)) || ')'"
            " when coltype in ('CHAR', 'VARCHAR', 'LONGVAR') and"
            " foreignkey = 'B' then 'VARBINARY(' || rtrim(char(length)) ||"
            " ')'"
            " when coltype in ('CHAR', 'VARCHAR', 'GRAPHIC', 'VARG',"
            " 'BINARY', 'VARBIN') then rtrim(coltype) || '(' ||"
            " rtrim(char(length)) || ')'"
            " when coltype = 'TIMESTMP' then 'TIMESTAMP(' ||"
            " rtrim(char(scale)) || ')'"
            " when coltype = 'FLOAT' and length = 4 then 'REAL'"
            " when coltype = 'FLOAT' then 'DOUBLE'"
            " else rtrim(coltype) end")

#: why a z/OS source is not followed, in the words `assess` and the
#: capability table say it
NO_OPEN_LOG = ("Db2 for z/OS hands its log out only to licensed code: a"
               " load module on the host reading it through IFCID 306, or"
               " the change tables IBM's replication product fills - no"
               " open reader exists, so a z/OS source moves by copies, each"
               " proved by check")

#: the codes of `SYSCOLUMNS.DEFAULT` a column the server fills itself
#: carries - a ROWID, a row change timestamp, a period's row begin or
#: end, a transaction start id - beside a plain default (Y, B, N, ...)
#: and an identity (I, J), which the counters read
_GENERATED = ("'A'", "'D'", "'E'", "'F'", "'K'", "'M'", "'Q'", "'R'",
              "'X'")

#: Db2 for z/OS's catalogue (`SYSIBM`), as IBM documents it, answering
#: `sqlcatalog.QUESTIONS`
CATALOGUE = {
    "keyless": (
        "select t.name from sysibm.systables t where t.creator = {schema}"
        " and t.type = 'T' and not exists (select 1 from sysibm.sysindexes"
        " i where i.tbcreator = t.creator and i.tbname = t.name and"
        " i.uniquerule in ('P', 'U', 'C', 'R', 'N')) order by 1"),
    "fks": (
        "select r.tbname, r.relname, f.colname, r.reftbname, k.colname,"
        " r.enforced from sysibm.sysrels r join sysibm.sysforeignkeys f on"
        " f.creator = r.creator and f.tbname = r.tbname and f.relname ="
        " r.relname join sysibm.syskeys k on k.ixcreator = r.ixowner and"
        " k.ixname = r.ixname and k.colseq = f.colseq where r.creator ="
        " {schema} order by r.tbname, r.relname, f.colseq"),
    # a trigger on z/OS cannot be switched off: one there fires
    "triggers": (
        "select tbname, name, 'Y' from sysibm.systriggers where tbowner ="
        " {schema} order by 1, 2"),
    # an index left rebuild pending is in the database's exception table,
    # which only a command shows: the catalogue keeps no state to read
    "indexes": (
        "select i.tbname, i.name, case when i.uniquerule = 'D' then 'N'"
        " else 'Y' end, k.colname, '' from sysibm.sysindexes i join"
        " sysibm.syskeys k on k.ixcreator = i.creator and k.ixname ="
        " i.name where i.tbcreator = {schema} order by i.tbname, i.name,"
        " k.colseq"),
    "checks": (
        "select c.tbname, c.checkname, 'check', case when t.checkflag ="
        " 'C' then 'check pending' else '' end from sysibm.syschecks c"
        " join sysibm.systables t on t.creator = c.tbowner and t.name ="
        " c.tbname where c.tbowner = {schema} union all select r.tbname,"
        " r.relname, 'foreign key', case when r.enforced = 'N' then 'not"
        " enforced' when t.checkflag = 'C' then 'check pending' else ''"
        " end from sysibm.sysrels r join sysibm.systables t on t.creator ="
        " r.creator and t.name = r.tbname where r.creator = {schema}"),
    # a partition's rows as the real-time statistics keep them, which the
    # server updates as it writes and externalizes every few minutes
    "partitions": (
        "select t.name, rtrim(char(p.partition)), p.limitkey,"
        " s.totalrows from sysibm.systables t join sysibm.systablepart p on"
        " p.dbname = t.dbname and p.tsname = t.tsname left join"
        " sysibm.systablespacestats s on s.dbname = p.dbname and s.name ="
        " p.tsname and s.partition = p.partition where t.creator = {schema}"
        " and t.type = 'T' and p.partition > 0 order by 1, p.partition"),
    "matviews": (
        "select v.name, case when v.refresh_time is null or"
        " year(v.refresh_time) = 1 then 'not populated' else '' end, ''"
        " from sysibm.sysviews v join sysibm.systables t on t.creator ="
        " v.creator and t.name = v.name where v.creator = {schema} and"
        " t.type = 'M' order by 1"),
    "rls": (
        "select c.tbname, c.name || case c.control_type when 'R' then ' (row"
        " permission)' else ' (column mask)' end, case when c.enable = 'Y'"
        " and ((c.control_type = 'R' and t.control in ('R', 'B')) or"
        " (c.control_type = 'M' and t.control in ('C', 'B'))) then 'Y' else"
        " 'N' end from sysibm.syscontrols c join sysibm.systables t on"
        " t.creator = c.tbschema and t.name = c.tbname where c.tbschema ="
        " {schema} order by 1, 2"),
    "generated": (
        "select tbname, name, \"DEFAULT\" from sysibm.syscolumns where"
        f" tbcreator = {{schema}} and \"DEFAULT\" in ({', '.join(_GENERATED)})"
        " order by 1, 2"),
    # z/OS keeps no collation of a column: text sorts by the table's
    # encoding scheme (EBCDIC, ASCII or Unicode), which orders it otherwise
    "collation": (
        "select name, '', encoding_scheme from sysibm.systables where"
        " creator = {schema} and type = 'T' order by 1"),
    "charset": (
        "select tbname, name, rtrim(char(ccsid)) from sysibm.syscolumns"
        " where tbcreator = {schema} and coltype in ('CHAR', 'VARCHAR',"
        " 'LONGVAR', 'CLOB', 'GRAPHIC', 'VARG', 'DBCLOB') order by 1, 2"),
    "owners": (
        "select case type when 'V' then 'view' else 'table' end, name,"
        " owner from sysibm.systables where creator = {schema} and type in"
        " ('T', 'V', 'M') union all select case routinetype when 'P' then"
        " 'procedure' else 'function' end, name, owner from"
        " sysibm.sysroutines where schema = {schema} union all select"
        " 'sequence', name, owner from sysibm.syssequences where schema ="
        " {schema} and seqtype = 'S'"),
    "grants": granted("sysibm.systabauth", "tcreator", "ttname", (
        ("alterauth", "ALTER"), ("deleteauth", "DELETE"),
        ("indexauth", "INDEX"), ("insertauth", "INSERT"),
        ("selectauth", "SELECT"), ("updateauth", "UPDATE"),
        ("refauth", "REFERENCES"), ("triggerauth", "TRIGGER"))),
    "seq_grants": granted("sysibm.syssequenceauth", "schema", "name", (
        ("useauth", "USAGE"), ("alterauth", "ALTER"))),
    # the largest value the server has handed out, and the step after it;
    # a counter that has handed out none starts where it was made to
    "sequences": (
        "select name, coalesce(maxassignedval + increment, start) from"
        " sysibm.syssequences where schema = {schema} and seqtype = 'S'"),
    "identity": (
        "select d.dname, d.dcolname, coalesce(s.maxassignedval +"
        " s.increment, s.start) from sysibm.syssequences s join"
        " sysibm.syssequencesdep d on d.bsequenceid = s.sequenceid where"
        " d.dcreator = {schema} and s.seqtype = 'I'"),
    "objects": (
        "select case type when 'V' then 'view' else 'alias' end, name, ''"
        " from sysibm.systables where creator = {schema} and type in ('V',"
        " 'A') union all select case routinetype when 'P' then 'procedure'"
        " else 'function' end, specificname, '' from sysibm.sysroutines"
        " where schema = {schema} union all select 'trigger', name, tbname"
        " from sysibm.systriggers where tbowner = {schema} union all select"
        " 'sequence', name, '' from sysibm.syssequences where schema ="
        " {schema} and seqtype = 'S'"),
    "options": " union all ".join(
        f"select name, '{word}', {col} from sysibm.systables where creator"
        " = {schema} and type = 'T'" for word, col in (
            ("data capture", "datacapture"), ("append", "append"),
            ("encoding scheme", "encoding_scheme"),
            ("audit", "audittype"))),
    "ids": (
        "select name, rtrim(char(dbid)) || '.' || rtrim(char(obid)) from"
        " sysibm.systables where creator = {schema} and type = 'T'"),
}


class Db2ZosEngine(Db2Engine):
    CANON_ENGINE = "db2zos"
    PLATFORM = "zos"
    #: DRDA's own port, which z/OS subsystems listen on by default
    DRDA_PORT = 446
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " one of z/OS's")
    PADDED = ("CHAR", "GRAPHIC")
    SCALED = ("DECIMAL",)
    #: large values as Db2's, a large table sized over every row: a
    #: sample of its pages is asked of Linux, UNIX and Windows only
    LARGE_VALUES = large_values()

    def neutral_tables(self, side, db):
        return [t for t in (self._said(n.strip()) for (n,) in self._rows(
            side, db, "select name from sysibm.systables where creator ="
                      f" {MARK} and type = 'T' order by 1",
            (self._owner(side, db),)))
            if not self.hop.excluded(db, t)]

    def neutral_columns(self, side, db, table):
        return [(self._said(n.strip()), t.strip()) for n, t in self._rows(
            side, db, f"select name, {DECLARED} from sysibm.syscolumns"
                      f" where tbcreator = {MARK} and tbname = {MARK}"
                      " order by colno",
            (self._owner(side, db), self._q(table)[1:-1]))]

    def neutral_key(self, side, db, table):
        """The primary key's columns, from the index that enforces it
        (`UNIQUERULE = 'P'`)."""
        return [self._said(n.strip()) for (n,) in self._rows(
            side, db, "select k.colname from sysibm.sysindexes i join"
                      " sysibm.syskeys k on k.ixcreator = i.creator and"
                      " k.ixname = i.name where i.tbcreator = "
                      f"{MARK} and i.tbname = {MARK} and i.uniquerule ="
                      " 'P' order by k.colseq",
            (self._owner(side, db), self._q(table)[1:-1]))]

    def table_facts(self, side, db):
        """Rows and bytes as RUNSTATS last left them (`CARDF`, `SPACEF` in
        kilobytes); -1, never gathered, is unknown."""
        out = {}
        for name, card, space in self._rows(
                side, db, "select name, cardf, spacef from sysibm.systables"
                          f" where creator = {MARK} and type = 'T'",
                (self._owner(side, db),)):
            out[self._said(name.strip())] = {
                "rows": None if card is None or card < 0 else int(card),
                "bytes": None if space is None or space < 0
                else int(space) * 1024, "key": None}
        return out

    def _pending(self, db):
        """Tables on the target whose definition is incomplete (`STATUS
        'I'`: a primary or unique key with no index to enforce it yet):
        z/OS refuses every write to them."""
        try:
            held = self._rows("dst", db, "select name from sysibm.systables"
                                         f" where creator = {MARK} and type"
                                         " = 'T' and status = 'I'",
                              (self._owner("dst", db),))
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} target tables", "error",
                           "could not read the target's tables:"
                           f" {str(e).splitlines()[0][:90]}")]
        held = self._held_in_scope(db, held)
        if held:
            return [Result("deep", f"{db} target tables", "diff",
                           f"{len(held)} tables whose definition is"
                           " incomplete: "
                           + ", ".join(r[0].strip() for r in held[:6]), "",
                           "create the unique indexes their keys need"
                           " before they are written")]
        return [Result("deep", f"{db} target tables", "ok",
                       "every table's definition complete")]

    def server_identity(self, side, db):
        """The subsystem by its location name and id, and its Db2
        version, as its special registers and built-in variables give
        them."""
        loc, ssid, version = self._rows(
            side, db, "select current server, getvariable('SYSIBM.SSID'),"
                      " getvariable('SYSIBM.VERSION') from"
                      " sysibm.sysdummy1")[0]
        return {"id": f"{str(loc).strip()}/{str(ssid or '').strip()}",
                "name": str(loc).strip(),
                "version": str(version or "").strip()}

    def reach_items(self, side):
        """How deep migkit reaches a z/OS side, for `assess`."""
        from .db2 import db2_connect_licence
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "tables are listed, copied by key ranges and"
                            " compared row by row"}]
        if side == "src":
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes are not followed",
                          "detail": NO_OPEN_LOG})
        if not db2_connect_licence():
            items.append({"level": "warn", "scope": "reach",
                          "item": "no Db2 Connect licence on this machine",
                          "detail": "reached only where the subsystem is"
                                    " activated for Db2 Connect; otherwise"
                                    " the operator's licence is put in"
                                    " place by migkit doctor --install"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")
