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
from .db2 import Db2Engine
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


class Db2ZosEngine(Db2Engine):
    CANON_ENGINE = "db2zos"
    PLATFORM = "zos"
    #: DRDA's own port, which z/OS subsystems listen on by default
    DRDA_PORT = 446
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " one of z/OS's")
    PADDED = ("CHAR", "GRAPHIC")
    SCALED = ("DECIMAL",)

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

    def check_deep(self, db):
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
