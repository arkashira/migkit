"""SCIM 2.0 (RFC 7643, 7644): the identity provider provisions the people
and groups who may sign in, and takes them away (backlog W2).

Served at `/scim/v2/` where `MIGKIT_WEB_SCIM_TOKEN` is set, to a caller
presenting it as a bearer token - the provider's provisioning
connector, not a person. Users and Groups: list with the filters
providers send (`userName eq "ann@example.com"`, `externalId eq ...`,
`members[value eq "..."]`, `and`/`or`/`not`, `co`/`sw`/`pr`), read,
create, replace, patch (the operations Entra ID, Okta and the rest send,
`Replace` and `"False"` included), delete.

What it changes takes effect at once: a person deactivated or deleted is
signed out of every session and loses every API token in the same
request, and their groups - which the hops file's `access` names - are
read from here on each request (`identity.admitted`). A filter on an
attribute this directory does not keep is refused (`invalidFilter`)
rather than read as no filter, which would list everyone.

Every change is written to the view's own record, with the provider's
connector as the one who made it.
"""
import json
import time
import uuid

USER = "urn:ietf:params:scim:schemas:core:2.0:User"
GROUP = "urn:ietf:params:scim:schemas:core:2.0:Group"
LIST = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
ERROR = "urn:ietf:params:scim:api:messages:2.0:Error"
PATCH = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
MEDIA = "application/scim+json"
#: the most a list returns at once
MOST = 500


class ScimError(Exception):
    def __init__(self, status, detail, kind=None):
        super().__init__(detail)
        self.status, self.detail, self.kind = int(status), detail, kind

    def body(self):
        out = {"schemas": [ERROR], "status": str(self.status),
               "detail": self.detail}
        if self.kind:
            out["scimType"] = self.kind
        return out


class _NoCase(dict):
    """An attribute map read in any case: SCIM's attribute names are
    case-insensitive (RFC 7643 2.1)."""

    @staticmethod
    def _key(key):
        return tuple(k.lower() if isinstance(k, str) else k for k in key)

    def __init__(self, items):
        super().__init__({self._key(k): v for k, v in items.items()})

    def get(self, key, default=None):
        return super().get(self._key(key), default)

    def __contains__(self, key):
        return super().__contains__(self._key(key))


USER_ATTRS = _NoCase({
    ("id", None, None): "users.id",
    ("userName", None, None): "users.user_name",
    ("externalId", None, None): "users.external_id",
    ("displayName", None, None): "users.display_name",
    ("emails", None, None): "users.email",
    ("emails", "value", None): "users.email",
    ("active", None, None): "users.active",
    ("meta", "lastModified", None): "users.modified",
    ("meta", "created", None): "users.created",
})
GROUP_ATTRS = _NoCase({
    ("id", None, None): "groups.id",
    ("displayName", None, None): "groups.display_name",
    ("externalId", None, None): "groups.external_id",
    ("members", None, None): "members.user_id",
    ("members", "value", None): "members.user_id",
    ("meta", "lastModified", None): "groups.modified",
    ("meta", "created", None): "groups.created",
})


def _stamp():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _truth(value):
    """A boolean as providers send it - Entra ID sends `"False"`."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise ScimError(400, f"{value!r} is not true or false", "invalidValue")


def where(filter_, table, attrs):
    """(SQL WHERE, parameters) for a SCIM filter over `table`; ScimError
    for a filter that does not parse or names an attribute not kept."""
    from scim2_filter_parser.queries import SQLQuery
    joins = ("LEFT JOIN members ON members.group_id = groups.id",) \
        if table == "groups" else ()
    try:
        q = SQLQuery(filter_, table, attrs, joins)
    except Exception as e:  # noqa: BLE001 - the library's parse errors
        raise ScimError(400, f"the filter does not parse: {e}",
                        "invalidFilter")
    for path in q.transpiler.attr_paths:
        if path not in attrs:
            name = ".".join(str(p) for p in (path[0], path[1]) if p)
            raise ScimError(400, f"{name} is not an attribute this"
                                 " directory filters on", "invalidFilter")
    q.placeholder = "?"
    params = [1 if p == "TRUE" else 0 if p == "FALSE" else p
              for p in q.params]
    return q.sql.rstrip().rstrip(";"), params


class Directory:
    """The SCIM resources over the view's store."""

    def __init__(self, store, base, record=None):
        self.store, self.base = store, base.rstrip("/")
        self.record = record or (lambda entry: None)

    # ---- resources as SCIM writes them ------------------------------------

    def _user(self, row):
        groups = self.store.rows(
            "select g.id, g.display_name from groups g join members m on"
            " m.group_id = g.id where m.user_id = ? order by 2", (row["id"],))
        raw = json.loads(row["raw"] or "{}")
        out = {"schemas": [USER], "id": row["id"],
               "userName": row["user_name"],
               "active": bool(row["active"]),
               "groups": [{"value": g["id"], "display": g["display_name"],
                           "$ref": f"{self.base}/Groups/{g['id']}"}
                          for g in groups],
               "meta": {"resourceType": "User", "created": row["created"],
                        "lastModified": row["modified"],
                        "version": f'W/"{row["version"]}"',
                        "location": f"{self.base}/Users/{row['id']}"}}
        if row["external_id"]:
            out["externalId"] = row["external_id"]
        if row["display_name"]:
            out["displayName"] = row["display_name"]
        if row["email"]:
            out["emails"] = [{"value": row["email"], "primary": True}]
        if isinstance(raw.get("name"), dict):
            out["name"] = raw["name"]
        return out

    def _group(self, row):
        members = self.store.rows(
            "select u.id, u.user_name from users u join members m on"
            " m.user_id = u.id where m.group_id = ? order by 2", (row["id"],))
        out = {"schemas": [GROUP], "id": row["id"],
               "displayName": row["display_name"],
               "members": [{"value": m["id"], "display": m["user_name"],
                            "$ref": f"{self.base}/Users/{m['id']}"}
                           for m in members],
               "meta": {"resourceType": "Group", "created": row["created"],
                        "lastModified": row["modified"],
                        "version": f'W/"{row["version"]}"',
                        "location": f"{self.base}/Groups/{row['id']}"}}
        if row["external_id"]:
            out["externalId"] = row["external_id"]
        return out

    def _one(self, table, rid):
        got = self.store.rows(f"select * from {table} where id = ?",
                              (str(rid),))
        if not got:
            raise ScimError(404, f"no {table[:-1]} {rid}")
        return got[0]

    # ---- the protocol -----------------------------------------------------

    def listing(self, table, args):
        attrs, shape = (USER_ATTRS, self._user) if table == "users" \
            else (GROUP_ATTRS, self._group)
        flt = (args.get("filter") or "").strip()
        if flt:
            sql, params = where(flt, table, attrs)
        else:
            sql, params = f"select distinct {table}.* from {table}", []
        try:
            start = max(1, int(args.get("startIndex") or 1))
            count = min(MOST, max(0, int(args.get("count") or 100)))
        except ValueError:
            raise ScimError(400, "startIndex and count are numbers",
                            "invalidValue")
        rows = self.store.rows(sql + f" order by {table}.created,"
                                     f" {table}.id", params)
        page = rows[start - 1:start - 1 + count]
        return 200, {"schemas": [LIST], "totalResults": len(rows),
                     "startIndex": start, "itemsPerPage": len(page),
                     "Resources": [shape(r) for r in page]}

    def get(self, table, rid):
        row = self._one(table, rid)
        return 200, (self._user(row) if table == "users"
                     else self._group(row))

    def _user_fields(self, body):
        name = str(body.get("userName") or "").strip()
        if not name:
            raise ScimError(400, "userName is required", "invalidValue")
        emails = body.get("emails") or []
        email = ""
        for e in emails if isinstance(emails, list) else []:
            if isinstance(e, dict) and e.get("value") and (
                    e.get("primary") or not email):
                email = str(e["value"])
        active = _truth(body["active"]) if "active" in body else True
        return {"user_name": name,
                "external_id": str(body.get("externalId") or "") or None,
                "display_name": str(body.get("displayName") or "") or None,
                "email": email or None, "active": 1 if active else 0,
                "raw": json.dumps({"name": body.get("name")}
                                  if isinstance(body.get("name"), dict)
                                  else {})}

    def create_user(self, body):
        f = self._user_fields(body)
        rid, now = uuid.uuid4().hex, _stamp()
        import sqlite3
        try:
            with self.store.tx() as db:
                db.execute("insert into users (id, user_name, external_id,"
                           " display_name, email, active, created, modified,"
                           " version, raw) values (?,?,?,?,?,?,?,?,1,?)",
                           (rid, f["user_name"], f["external_id"],
                            f["display_name"], f["email"], f["active"], now,
                            now, f["raw"]))
        except sqlite3.IntegrityError:
            raise ScimError(409, f"{f['user_name']} is provisioned already",
                            "uniqueness")
        self.record({"op": "scim-user-create", "user": f["user_name"],
                     "active": bool(f["active"])})
        return 201, self._user(self._one("users", rid))

    def replace_user(self, rid, body):
        old = self._one("users", rid)
        f = self._user_fields(body)
        self._write_user(old, f)
        return 200, self._user(self._one("users", rid))

    def _write_user(self, old, f):
        import sqlite3
        try:
            with self.store.tx() as db:
                db.execute("update users set user_name = ?, external_id = ?,"
                           " display_name = ?, email = ?, active = ?,"
                           " modified = ?, version = version + 1, raw = ?"
                           " where id = ?",
                           (f["user_name"], f["external_id"],
                            f["display_name"], f["email"], f["active"],
                            _stamp(), f["raw"], old["id"]))
        except sqlite3.IntegrityError:
            raise ScimError(409, f"{f['user_name']} is provisioned already",
                            "uniqueness")
        gone = old["active"] and not f["active"]
        renamed = f["user_name"].lower() != str(old["user_name"]).lower()
        if gone or renamed:
            self._sign_out(old)
        self.record({"op": "scim-user-change", "user": f["user_name"],
                     "was": old["user_name"], "active": bool(f["active"])})

    def _sign_out(self, row):
        n = self.store.close_sessions_of(row["user_name"])
        if row["email"] and str(row["email"]).lower() != str(
                row["user_name"]).lower():
            n += self.store.close_sessions_of(row["email"])
        if n:
            self.record({"op": "signed-out", "user": row["user_name"],
                         "sessions": n, "why": "deprovisioned"})

    def patch_user(self, rid, body):
        old = self._one("users", rid)
        cur = self._user(old)
        for op in _operations(body):
            kind, path, value = op
            if path is None:
                if kind == "remove" or not isinstance(value, dict):
                    raise ScimError(400, "an operation without a path"
                                         " carries attributes", "noTarget")
                for k, v in value.items():
                    _set_user(cur, k, v, kind)
                continue
            _set_user(cur, path, value, kind)
        self._write_user(old, self._user_fields(cur))
        return 200, self._user(self._one("users", rid))

    def delete(self, table, rid):
        row = self._one(table, rid)
        with self.store.tx() as db:
            db.execute(f"delete from {table} where id = ?", (row["id"],))
        if table == "users":
            self._sign_out(row)
            self.record({"op": "scim-user-delete", "user": row["user_name"]})
        else:
            self.record({"op": "scim-group-delete",
                         "group": row["display_name"]})
        return 204, None

    def _member_ids(self, members):
        out = []
        for m in members or []:
            uid = m.get("value") if isinstance(m, dict) else m
            if uid is None:
                continue
            self._one("users", uid)
            out.append(str(uid))
        return out

    def create_group(self, body):
        import sqlite3
        name = str(body.get("displayName") or "").strip()
        if not name:
            raise ScimError(400, "displayName is required", "invalidValue")
        members = self._member_ids(body.get("members"))
        rid, now = uuid.uuid4().hex, _stamp()
        try:
            with self.store.tx() as db:
                db.execute("insert into groups values (?,?,?,?,?,1)",
                           (rid, name, str(body.get("externalId") or "")
                            or None, now, now))
                db.executemany("insert or ignore into members values (?,?)",
                               [(rid, u) for u in members])
        except sqlite3.IntegrityError:
            raise ScimError(409, f"a group {name} is provisioned already",
                            "uniqueness")
        self.record({"op": "scim-group-create", "group": name,
                     "members": len(members)})
        return 201, self._group(self._one("groups", rid))

    def replace_group(self, rid, body):
        import sqlite3
        old = self._one("groups", rid)
        name = str(body.get("displayName") or "").strip()
        if not name:
            raise ScimError(400, "displayName is required", "invalidValue")
        members = self._member_ids(body.get("members"))
        try:
            with self.store.tx() as db:
                db.execute("update groups set display_name = ?,"
                           " external_id = ?, modified = ?, version ="
                           " version + 1 where id = ?",
                           (name, str(body.get("externalId") or "") or None,
                            _stamp(), old["id"]))
                db.execute("delete from members where group_id = ?",
                           (old["id"],))
                db.executemany("insert or ignore into members values (?,?)",
                               [(old["id"], u) for u in members])
        except sqlite3.IntegrityError:
            raise ScimError(409, f"a group {name} is provisioned already",
                            "uniqueness")
        self.record({"op": "scim-group-change", "group": name,
                     "members": len(members)})
        return 200, self._group(self._one("groups", rid))

    def patch_group(self, rid, body):
        old = self._one("groups", rid)
        name = old["display_name"]
        add, drop, replace = [], [], None
        for kind, path, value in _operations(body):
            low = (path or "").lower()
            if path is None and isinstance(value, dict):
                if "displayName" in value:
                    name = str(value["displayName"])
                if "members" in value:
                    replace = self._member_ids(value["members"])
                continue
            if low == "displayname":
                name = str(value)
            elif low == "members":
                ids = self._member_ids(value if isinstance(value, list)
                                       else [value] if value else [])
                if kind == "add":
                    add += ids
                elif kind == "remove":
                    if value in (None, [], ""):
                        replace = []
                    else:
                        drop += ids
                else:
                    replace = ids
            elif low.startswith("members[") and kind == "remove":
                uid = _member_value(path)
                if uid is None:
                    raise ScimError(400, f"{path}: a member is named by"
                                         ' members[value eq "id"]',
                                    "invalidFilter")
                drop.append(uid)
            else:
                raise ScimError(400, f"{path} is not an attribute of a group"
                                     " this directory changes", "invalidPath")
        with self.store.tx() as db:
            db.execute("update groups set display_name = ?, modified = ?,"
                       " version = version + 1 where id = ?",
                       (name, _stamp(), old["id"]))
            if replace is not None:
                db.execute("delete from members where group_id = ?",
                           (old["id"],))
                db.executemany("insert or ignore into members values (?,?)",
                               [(old["id"], u) for u in replace])
            db.executemany("insert or ignore into members values (?,?)",
                           [(old["id"], u) for u in add])
            db.executemany("delete from members where group_id = ? and"
                           " user_id = ?", [(old["id"], u) for u in drop])
        self.record({"op": "scim-group-change", "group": name,
                     "added": len(add), "removed": len(drop),
                     "replaced": replace is not None})
        return 200, self._group(self._one("groups", rid))

    def config(self):
        return {"schemas": ["urn:ietf:params:scim:schemas:core:2.0:"
                            "ServiceProviderConfig"],
                "patch": {"supported": True},
                "bulk": {"supported": False, "maxOperations": 0,
                         "maxPayloadSize": 0},
                "filter": {"supported": True, "maxResults": MOST},
                "changePassword": {"supported": False},
                "sort": {"supported": False}, "etag": {"supported": False},
                "authenticationSchemes": [{
                    "type": "oauthbearertoken", "name": "Bearer token",
                    "description": "the token set as MIGKIT_WEB_SCIM_TOKEN",
                    "primary": True}]}

    def resource_types(self):
        return {"schemas": [LIST], "totalResults": 2, "Resources": [
            {"schemas": ["urn:ietf:params:scim:schemas:core:2.0:"
                         "ResourceType"], "id": "User", "name": "User",
             "endpoint": "/Users", "schema": USER},
            {"schemas": ["urn:ietf:params:scim:schemas:core:2.0:"
                         "ResourceType"], "id": "Group", "name": "Group",
             "endpoint": "/Groups", "schema": GROUP}]}

    def handle(self, method, parts, args, body):
        """(status, body) of one request under `/scim/v2/`."""
        if not parts:
            raise ScimError(404, "nothing here")
        head = parts[0]
        if head == "ServiceProviderConfig" and method == "GET":
            return 200, self.config()
        if head == "ResourceTypes" and method == "GET":
            return 200, self.resource_types()
        if head == "Schemas" and method == "GET":
            return 200, {"schemas": [LIST], "totalResults": 2, "Resources": [
                {"id": USER, "name": "User"}, {"id": GROUP, "name": "Group"}]}
        table = {"Users": "users", "Groups": "groups"}.get(head)
        if table is None or len(parts) > 2:
            raise ScimError(404, "nothing here")
        if len(parts) == 1:
            if method == "GET":
                return self.listing(table, args)
            if method == "POST":
                return (self.create_user if table == "users"
                        else self.create_group)(_body(body))
            raise ScimError(405, f"{method} is not taken here")
        rid = parts[1]
        if method == "GET":
            return self.get(table, rid)
        if method == "DELETE":
            return self.delete(table, rid)
        if method == "PUT":
            return (self.replace_user if table == "users"
                    else self.replace_group)(rid, _body(body))
        if method == "PATCH":
            return (self.patch_user if table == "users"
                    else self.patch_group)(rid, _body(body))
        raise ScimError(405, f"{method} is not taken here")


def _body(body):
    if not isinstance(body, dict):
        raise ScimError(400, "the body is not a JSON object", "invalidSyntax")
    return body


def _operations(body):
    """[(add|replace|remove, path or None, value)] of a PATCH body."""
    ops = body.get("Operations")
    if not isinstance(ops, list) or not ops:
        raise ScimError(400, "a PATCH carries Operations", "invalidSyntax")
    out = []
    for op in ops:
        if not isinstance(op, dict):
            raise ScimError(400, "an operation is an object", "invalidSyntax")
        kind = str(op.get("op", "")).strip().lower()
        if kind not in ("add", "replace", "remove"):
            raise ScimError(400, f"{op.get('op')!r} is not add, replace or"
                                 " remove", "invalidSyntax")
        path = op.get("path")
        out.append((kind, str(path) if path is not None else None,
                    op.get("value")))
    return out


def _member_value(path):
    """The member a `members[value eq "x"]` path names, or None."""
    import re
    got = re.fullmatch(r'\s*members\[\s*value\s+eq\s+"([^"]*)"\s*\]\s*',
                       path, re.I)
    return got.group(1) if got else None


def _set_user(cur, path, value, kind):
    """One attribute of a user as a PATCH sets it, on the SCIM shape."""
    low = path.lower()
    if low.startswith(USER.lower() + ":"):
        low = low[len(USER) + 1:]
    if low == "active":
        cur["active"] = False if kind == "remove" else _truth(value)
    elif low == "username":
        if kind == "remove":
            raise ScimError(400, "userName cannot be removed", "mutability")
        cur["userName"] = str(value)
    elif low in ("displayname", "externalid"):
        key = "displayName" if low == "displayname" else "externalId"
        cur[key] = None if kind == "remove" else str(value)
    elif low.startswith("name"):
        name = dict(cur.get("name") or {})
        if low == "name":
            name = {} if kind == "remove" else dict(value or {})
        else:
            sub = path.split(".", 1)[1]
            if kind == "remove":
                name.pop(sub, None)
            else:
                name[sub] = value
        cur["name"] = name
    elif low.startswith("emails"):
        if kind == "remove":
            cur["emails"] = []
        elif isinstance(value, list):
            cur["emails"] = value
        else:
            cur["emails"] = [{"value": str(value), "primary": True}]
    elif low in ("groups",):
        raise ScimError(400, "a user's groups change through the group",
                        "mutability")
    else:
        # an attribute this directory does not keep (a title, a phone
        # number): taken, and not kept - providers send the whole person
        return
