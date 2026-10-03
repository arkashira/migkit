"""The SQL written in the application's own code, found, carried into the
target's SQL and proved (leapfrog research P11, backlog CV.P11, W3's
application-embedded SQL).

The hop option `application_sql` names where the application's code is
on this machine (a path, or a list of them). migkit reads it and never
writes to it: changing the application is its owners' work (decided
2026-10-01); reading its SQL to convert the dialect is conversion work.
What is found is written beside the reports - each statement in the
target's SQL with where it was found and what the proof said of it
(`application-sql.sql`, `application-sql.json`) - for the people who
change the application.

Where SQL is: every string literal of the code, read by a parser of the
code's own language (ast-grep's, over tree-sitter: Python, Java, Kotlin,
Scala, Go, JavaScript, TypeScript, C#, PHP, Ruby, Rust, Swift), and the
statements of `.sql` files and of XML mapper files. A literal is SQL when
it begins as a statement does and the translator reads it in the source's
dialect with its placeholders (`statements.Holes`) - a message that
begins with "Update" is not. Literals joined by `+` (or PHP's `.`), and
Python's adjacent ones, are read as the one string they make; a string
put together around a variable, an interpolated one (`f"..."`, `${x}`,
`$"..."`, `#{x}` in Ruby), one formatted as the application runs
(`%d`, `{}`), and a mapper statement built by its tags are found and said
as built at run time - found, never run in a guessed form. A query in an
object mapper's own language (a `@Query` that is not native) is the
mapper's to carry, and left out.

Every statement found is proved like a routine (`statements`): run on
both sandboxes with its placeholders filled from the fixture, answers
and rows changed compared.
"""
import re
from pathlib import Path

from .statements import Statement, squash, starts_like_sql, tree_of

#: the code's languages, by the file's extension, as ast-grep names them
LANGUAGES = {".py": "python", ".java": "java", ".kt": "kotlin",
             ".kts": "kotlin", ".scala": "scala", ".go": "go",
             ".js": "javascript", ".mjs": "javascript",
             ".cjs": "javascript", ".jsx": "javascript",
             ".ts": "typescript", ".tsx": "tsx", ".cs": "csharp",
             ".php": "php", ".rb": "ruby", ".rs": "rust",
             ".swift": "swift"}
#: directories never the application's own code
SKIP = {".git", ".hg", ".svn", "node_modules", "vendor", "target", "build",
        "dist", "out", ".venv", "venv", "env", "__pycache__", ".idea",
        ".gradle", ".next", "bin", "obj", ".tox", "site-packages",
        ".mypy_cache", ".pytest_cache", "coverage"}
#: files read at most, the largest file read, statements kept at most
MOST_FILES = 20000
LARGEST = 1_000_000
MOST_FOUND = 2000

#: nodes that are a string literal, in the languages above
STRINGS = {"string", "string_literal", "interpreted_string_literal",
           "raw_string_literal", "template_string",
           "verbatim_string_literal", "encapsed_string",
           "line_string_literal", "multi_line_string_literal",
           "interpolated_string_expression", "heredoc", "text_block"}
#: nodes that join two values, and the operators that join strings
JOINS = {"binary_expression", "binary_operator", "additive_expression",
         "binary", "infix_expression"}
JOINERS = {"+", "."}
#: the pieces of a literal that are its text, and its escapes
CONTENT = {"string_content", "string_fragment", "multiline_string_fragment",
           "interpreted_string_literal_content",
           "raw_string_literal_content", "string_literal_content",
           "raw_string_content", "line_str_text", "multi_line_str_text"}
ESCAPES = {"\\n": "\n", "\\t": "\t", "\\r": "\r", "\\\\": "\\",
           '\\"': '"', "\\'": "'", "\\0": "\0", "\\`": "`", "\\$": "$"}
#: a text formatted by the application as it runs
FORMATTED = re.compile(r"%[-+ 0#]*\d*(\.\d+)?[dvfiqx]|(?<![#$]){\w*}")


def paths(hop):
    """The application's directories the hop names, as paths."""
    got = (hop.options or {}).get("application_sql")
    if not got:
        return []
    got = [got] if isinstance(got, (str, Path)) else list(got)
    return [Path(str(p)).expanduser() for p in got]


def turned_on(hop):
    return bool(paths(hop))


def _files(roots):
    """(root, file) of the application's code, .sql and mapper files, in
    a steady order, at most MOST_FILES."""
    out = []
    for root in roots:
        if root.is_file():
            out.append((root.parent, root))
            continue
        if not root.is_dir():
            continue
        for p in sorted(root.rglob("*")):
            if any(part in SKIP for part in p.relative_to(root).parts[:-1]):
                continue
            if p.is_file() and (p.suffix in LANGUAGES or p.suffix in
                                (".sql", ".xml")):
                out.append((root, p))
                if len(out) >= MOST_FILES:
                    return out
    return out


def _decode(text):
    return re.sub(r"\\(u[0-9a-fA-F]{4}|.)", lambda m: ESCAPES.get(
        m.group(0), chr(int(m.group(1)[1:], 16)) if m.group(1)[0] == "u"
        else m.group(0)), text)


def _dynamic_inside(node):
    """Whether a literal holds a piece the application puts in as it
    runs (an interpolation)."""
    for d in _descend(node):
        k = d.kind()
        if "interpolat" in k or "substitution" in k:
            return True
        if k == "variable_name" and node.kind() in ("encapsed_string",
                                                    "heredoc"):
            return True
    if node.kind() == "string" and node.children():
        start = node.children()[0]
        if start.kind() == "string_start" and "f" in start.text().lower():
            return True
    return False


def _descend(node):
    for c in node.children():
        yield c
        yield from _descend(c)


def _stripped(text):
    """A literal's text without its quotes, where its nodes do not give
    its content apart."""
    m = re.match(r'(?s)^r(#*)"(.*)"\1$', text)
    if m:
        return m.group(2)
    m = re.match(r'(?s)^@"(.*)"$', text)
    if m:
        return m.group(1).replace('""', '"')
    m = re.match(r'(?s)^[A-Za-z$]*("""|\'\'\'|"|\'|`)(.*)\1$', text)
    if m:
        return _decode(m.group(2))
    return text


def value(node):
    """(the literal's text, or None where it is put together as it
    runs)."""
    if _dynamic_inside(node):
        return None
    parts = []
    for d in _descend(node):
        k = d.kind()
        if k in CONTENT and not d.children():
            parts.append(d.text())
        elif k == "escape_sequence":
            parts.append(_decode(d.text()))
    if parts:
        return "".join(parts)
    kids = [c for c in node.children() if c.kind() in STRINGS]
    if node.kind() == "concatenated_string" or (kids and len(kids) ==
                                                len(node.children())):
        got = [value(c) for c in kids]
        return None if None in got else "".join(got)
    return _stripped(node.text())


def _joined(node, seen):
    """(the text a join of literals makes, or None where a piece of it is
    not a literal; the literals' pieces as found)."""
    pieces, constant = [], True

    def walk(n):
        nonlocal constant
        k = n.kind()
        if k in STRINGS or k == "concatenated_string":
            seen.add(_key(n))
            got = value(n)
            if got is None:
                constant = False
                pieces.append(None)
            else:
                pieces.append(got)
            return
        if k in ("parenthesized_expression",):
            for c in n.children():
                if c.is_named():
                    walk(c)
            return
        if _is_join(n):
            for c in n.children():
                if c.is_named() and c.kind() not in ("operator_identifier",):
                    walk(c)
            return
        constant = False
        pieces.append(None)
    walk(node)
    return ("".join(pieces) if constant else None), pieces


def _is_join(node):
    if node.kind() not in JOINS:
        return False
    ops = [c.text() for c in node.children() if not c.is_named()
           or c.kind() == "operator_identifier"]
    return bool(ops) and all(o in JOINERS for o in ops)


def _key(node):
    r = node.range()
    return (r.start.index, r.end.index)


def _in_mapper_query(node):
    """Whether a literal is an object mapper's own query language: inside
    a `@Query` or `@NamedQuery` that does not say it is native."""
    for a in node.ancestors():
        if a.kind() in ("annotation", "marker_annotation"):
            text = a.text()
            return bool(re.match(r"@\s*(\w+\.)*(Named)?Query\b", text)) \
                and not re.search(r"nativeQuery\s*=\s*true", text)
    return False


def in_code(text, language):
    """[(line, text or None, the literal's pieces)] of every string - or
    join of strings - in one file of code that begins as a statement."""
    from ast_grep_py import SgRoot
    root = SgRoot(text, language).root()
    out, seen = [], set()
    # one walk of the tree: a kind a language's grammar lacks is refused
    # by the parser's own search, so its nodes are picked out here
    joins, strings = [], []
    for n in _descend(root):
        k = n.kind()
        if k in JOINS and _is_join(n) and not (
                n.parent() and _is_join(n.parent())):
            joins.append(n)
        elif k in STRINGS or k == "concatenated_string":
            strings.append(n)
    for n in sorted(joins, key=_key):
        got, pieces = _joined(n, seen)
        first = next((p for p in pieces if p is not None), None)
        if first is None or not starts_like_sql(first):
            continue
        if _in_mapper_query(n):
            continue
        out.append((n.range().start.line + 1, got, pieces))
    for n in strings:
        if _key(n) in seen:
            continue
        p = n.parent()
        if p is not None and p.kind() in STRINGS | {"concatenated_string"}:
            continue
        seen.add(_key(n))
        got = value(n)
        probe = got if got is not None else _stripped(n.text())
        if not starts_like_sql(probe) or _in_mapper_query(n):
            continue
        if got is not None and language == "python" and p is not None \
                and p.kind() == "binary_operator" and any(
                    c.text() == "%" for c in p.children()):
            # formatted with `%` as it runs, not handed to the driver
            got = None
        out.append((n.range().start.line + 1, got,
                    [got if got is not None else probe]))
    return sorted(out, key=lambda x: x[0])


def in_sql_file(text):
    """[(line, statement)] of a .sql file: split at the semicolons
    outside quotes and comments, each from its first line that is not a
    comment."""
    out, line, i, n = [], 1, 0, len(text)
    start, begun_line, quote = None, 1, None
    while i < n:
        ch = text[i]
        if quote:
            if ch == quote:
                quote = None
        elif text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
            continue
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            line += text[i:(n if j < 0 else j)].count("\n")
            i = n if j < 0 else j + 2
            continue
        elif ch == ";":
            if start is not None:
                piece = text[start:i].strip()
                if piece:
                    out.append((begun_line, piece))
            start = None
            i += 1
            continue
        elif ch in "'\"`":
            quote = ch
        if start is None and not ch.isspace():
            start, begun_line = i, line
        if ch == "\n":
            line += 1
        i += 1
    if start is not None and text[start:].strip():
        out.append((begun_line, text[start:].strip()))
    return out


_MAPPER = re.compile(r"(?is)<(select|insert|update|delete)\b[^>]*>(.*?)</\1>")
_XML = {"&lt;": "<", "&gt;": ">", "&amp;": "&", "&quot;": '"',
        "&apos;": "'"}


def in_mapper(text):
    """[(line, statement or None, its pieces)] of an XML mapper's
    statements: one built by its tags (`<if>`, `<where>`, `<foreach>`...)
    is None."""
    if "<mapper" not in text:
        return []
    out = []
    for m in _MAPPER.finditer(text):
        body = re.sub(r"(?s)<!\[CDATA\[(.*?)\]\]>", lambda c: c.group(1),
                      m.group(2))
        line = text[:m.start()].count("\n") + 1
        if re.search(r"<\w", body):
            out.append((line, None, [re.sub(r"<[^>]*>", " ", body)]))
            continue
        for k, v in _XML.items():
            body = body.replace(k, v)
        out.append((line, body.strip(), [body.strip()]))
    return out


def found(hop, dialect):
    """([Statement] found in the application's code, [what could not be
    read, in words]). A text that begins as a statement is one only where
    it reads as one in the source's `dialect`; one put together as it
    runs is kept where its first piece does."""
    roots = paths(hop)
    missing = [str(r) for r in roots if not r.exists()]
    by, said = {}, []
    if missing:
        said.append("the application's code named is not on this machine: "
                    + ", ".join(missing[:3]))
    for root, f in _files(roots):
        try:
            if f.stat().st_size > LARGEST:
                continue
            text = f.read_text(errors="replace")
        except OSError:
            continue
        rel = str(f.relative_to(root))
        try:
            if f.suffix == ".sql":
                items = [(ln, s, [s]) for ln, s in in_sql_file(text)
                         if starts_like_sql(s)]
            elif f.suffix == ".xml":
                items = in_mapper(text)
            else:
                items = in_code(text, LANGUAGES[f.suffix])
                if LANGUAGES[f.suffix] != "python":
                    # `%s` is a placeholder of Python's drivers; in any
                    # other language it is text formatted as it runs
                    items = [(ln, None if s is not None and "%s" in
                              _outside_quotes(s) else s, pieces)
                             for ln, s, pieces in items]
        except BaseException as e:  # noqa: BLE001 - the parser's own
            # failure (a panic crosses as BaseException): said, the rest read
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            said.append(f"{rel} could not be read: {squash(e, 60)}")
            continue
        for line, text_, pieces in items:
            if text_ is not None and not FORMATTED.search(
                    _outside_quotes(text_)) and \
                    tree_of(text_, dialect) is None:
                continue
            _add(by, f"{rel}:{line}", text_, pieces)
            if len(by) >= MOST_FOUND:
                break
        if len(by) >= MOST_FOUND:
            said.append(f"only the first {MOST_FOUND} statements found were"
                        " kept")
            break
    return list(by.values()), said


def _add(by, where, text, pieces):
    """One statement found, merged with the same one found elsewhere."""
    if text is None:
        shown = " ... ".join(squash(p, 40) for p in pieces if p)
        why = "it is put together by the application as it runs"
        key = f"dynamic:{where}"
        by[key] = Statement(origin=where, text=shown or "", dynamic=why,
                            label=squash(shown, 100), shown=False)
        return
    if FORMATTED.search(_outside_quotes(text)):
        by[f"dynamic:{where}"] = Statement(
            origin=where, text=text, label=squash(text, 100), shown=False,
            dynamic="it is formatted by the application as it runs")
        return
    key = " ".join(text.split()).lower()
    one = by.get(key)
    if one is None:
        by[key] = Statement(origin=where, text=text, label=squash(text, 100),
                            shown=True)
    else:
        one.runs += 1
        if "(and" not in one.origin:
            one.origin += " (and elsewhere)"


def _outside_quotes(text):
    return re.sub(r"'(?:[^']|'')*'", "''", text)


#: what the code parser's library offers, as its release documents it,
#: and the token in this file that shows each used
_PARSER_API = {
    "SgRoot": "SgRoot", "root": "root", "kind": "kind", "text": "text",
    "range": "range", "children": "children", "parent": "parent",
    "ancestors": "ancestors", "is_named": "is_named", "filename": "filename",
    "find": "find", "find_all": "find_all", "matches": "matches",
    "inside": "inside", "has": "has", "precedes": "precedes",
    "follows": "follows", "get_match": "get_match",
    "get_multiple_matches": "get_multiple_matches",
    "get_transformed": "get_transformed", "get_root": "get_root",
    "field": "field", "field_children": "field_children", "child": "child",
    "named_children": "named_children", "next": "SgNode.next",
    "next_all": "next_all", "prev": "prev", "prev_all": "prev_all",
    "is_leaf": "is_leaf", "is_named_leaf": "is_named_leaf",
    "replace": "replace", "commit_edits": "commit_edits",
    "register_dynamic_language": "register_dynamic_language"}


def _register():
    """The code parser's account: what is used is read off this file;
    what is not says why."""
    from .. import tooluse
    if "ast-grep-py" in tooluse.REGISTRY:
        return
    unused = tooluse.UNUSED
    search = ("a literal is picked by one walk of the tree: the library's"
              " search refuses a kind a language's grammar lacks, and what"
              " makes a literal SQL is its text, read by the translator")
    tooluse.register(
        "ast-grep-py", licence="MIT", release="0.45.3",
        declared={"": frozenset(_PARSER_API)},
        reads=(tuple(f"storedcode/appscan.py::{fn}" for fn in (
            "in_code", "value", "_descend", "_dynamic_inside", "_joined",
            "_is_join", "_key", "_in_mapper_query")),
            {("", o): t for o, t in _PARSER_API.items()}),
        options={
            ("", "filename"): (unused, "the files are opened and named by"
                                       " migkit, which says where each"
                                       " statement was found"),
            ("", "find"): (unused, search),
            ("", "find_all"): (unused, "see find"),
            ("", "matches"): (unused, "see find"),
            ("", "inside"): (unused, "see find"),
            ("", "has"): (unused, "see find"),
            ("", "precedes"): (unused, "see find"),
            ("", "follows"): (unused, "see find"),
            ("", "get_match"): (unused, "no pattern is matched, so no"
                                        " metavariable is read"),
            ("", "get_multiple_matches"): (unused, "see get_match"),
            ("", "get_transformed"): (unused, "no rule rewrites a match"),
            ("", "get_root"): (unused, "the root is kept from the parse"
                                       " that made it"),
            ("", "field"): (unused, "a literal's pieces are read in order"
                                    " by the walk, whatever field holds"
                                    " them"),
            ("", "field_children"): (unused, "see field"),
            ("", "child"): (unused, "see field"),
            ("", "named_children"): (unused, "see field"),
            ("", "next"): (unused, "a literal is read whole where it"
                                   " stands; its neighbours say nothing"
                                   " of it"),
            ("", "next_all"): (unused, "see next"),
            ("", "prev"): (unused, "see next"),
            ("", "prev_all"): (unused, "see next"),
            ("", "is_leaf"): (unused, "a piece's text is read where it has"
                                      " no children, asked of the walk"
                                      " itself"),
            ("", "is_named_leaf"): (unused, "see is_leaf"),
            ("", "replace"): (unused, "migkit never changes the"
                                      " application's code: its owners"
                                      " do, from what migkit writes"),
            ("", "commit_edits"): (unused, "see replace"),
            ("", "register_dynamic_language"): (
                unused, "the built-in grammars cover the languages an"
                        " application's SQL is written in; another is"
                        " named as not read")})


_register()

