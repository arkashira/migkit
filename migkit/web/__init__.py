"""The web view and the API: several people, each signed in by the
organisation's identity provider, each with a role on each hop, and a
REST API over the command line's own functions (backlog W2, the owner's
scope of 2026-10-01: "a web interface for people who do not use the
command line - the same outcomes the console shows, the approvals and
the source-change offer answered there").

* `identity` - OpenID Connect and SAML sign-in, the local account of an
  install without a provider; `scim` - the people and groups the
  provider provisions; `store` - sessions, tokens and the directory.
* `roles` - viewer, operator, approver, admin, per hop or hop group,
  written in the hops file's `access`.
* `api` - the routes and the OpenAPI document written from them;
  `runs`/`runner` - the command line's commands, run whole;
  `views` - what a page and the API show, read once for both;
  `approve` - an approval given in the view, counted by `approvals`.
* `app` - the WSGI application, pages and API; `server` - served by
  `migkit report --serve`.
"""


def serve(port):
    """The web view, until ctrl-c."""
    from .server import serve as _serve
    return _serve(port)
