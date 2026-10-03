"""The web view and the API as one WSGI application (backlog W2).

Pages are rendered on the server (autoescaped templates) with one small
script of the view's own for the buttons, the live progress and a run's
output; every button calls the API, so a page does nothing the API does
not. What every answer carries, and every request passes:

* **Only this view's names**: a Host the view is not reached by is
  refused - a page elsewhere rebinding a name it controls to this
  machine gets nothing. Behind reverse proxies (`MIGKIT_WEB_PROXIES`),
  only their connections are answered and only their forwarded headers
  read.
* **Sessions**: a random value in an HttpOnly, SameSite=Lax cookie
  (`__Host-` prefixed and Secure over https), kept on the server as its
  digest; a new one at every sign-in; ended by signing out, by idling
  (`MIGKIT_WEB_IDLE_MINUTES`), by its term (`MIGKIT_WEB_SESSION_HOURS`,
  or sooner where the provider says), and by the provider deprovisioning
  the person.
* **CSRF**: every request that changes anything and rides on the cookie
  carries the session's token (a header from the script, a field from a
  form) and comes from this view's own origin; the sign-in forms carry a
  token of their own, so nobody is signed in to someone else's account.
  API tokens are not cookies and need neither.
* **Headers**: a content security policy allowing only the view's own
  script and style and no frames, no sniffing, no referrer, no caching of
  anything signed in, HSTS over https.
* **The record**: every call that changes something, every evidence
  download, every sign-in and every refusal of one is written to the
  hop's record or the view's own (`audit`), with who and how.
"""
import json
import secrets
import time
from importlib import resources
from urllib.parse import quote, urlsplit

from werkzeug.exceptions import HTTPException
from werkzeug.routing import Map, Rule
from werkzeug.wrappers import Request, Response

from . import api, identity
from .identity import Principal, Refused

#: the most of a body read from a page or the API; the provider's
#: provisioning connector may send a large group
BODY_MOST = 1024 * 1024
SCIM_MOST = 8 * 1024 * 1024
#: the files the view serves as they are
STATIC = {"web.css": "text/css; charset=utf-8",
          "web.js": "text/javascript; charset=utf-8"}
UNSAFE = ("POST", "PUT", "PATCH", "DELETE")


def _safe_next(value):
    """A place to go on to after signing in: a path of this view only."""
    value = str(value or "/")
    parts = urlsplit(value)
    if parts.scheme or parts.netloc or not value.startswith("/") \
            or value.startswith("//") or "\\" in value:
        return "/"
    return value


class App:
    def __init__(self, settings, store=None, http=None):
        from jinja2 import Environment, PackageLoader, select_autoescape

        from .store import Store
        self.settings = settings
        self.store = store or Store(settings.state / "web.db")
        self.record_path = settings.state / "record.jsonl"
        self.oidc = identity.Oidc(settings, self.store, http) \
            if settings.oidc_issuer else None
        self.saml = identity.Saml(settings, self.store) \
            if settings.saml_metadata else None
        self.local = identity.Local(self.store) if not settings.sso else None
        #: the dashboard's own snapshot of every hop's metrics, served by
        #: `/metrics` - started by `server` beside the view, read once a
        #: scrape where it is not running (`ui.Collector`)
        self.metrics = _metrics()
        https = settings.https
        self.cookie = "__Host-migkit_session" if https else "migkit_session"
        self.signin_cookie = "__Host-migkit_signin" if https \
            else "migkit_signin"
        self.form_cookie = "__Host-migkit_form" if https else "migkit_form"
        self.jinja = Environment(
            loader=PackageLoader("migkit.web", "templates"),
            autoescape=select_autoescape(["html"]), trim_blocks=True,
            lstrip_blocks=True)
        self.jinja.filters["when"] = _when
        self.jinja.filters["took"] = _took
        self.jinja.filters["num"] = _num
        rules = [
            Rule("/", endpoint="home"), Rule("/signin", endpoint="signin"),
            Rule("/signout", endpoint="signout", methods=["POST"]),
            Rule("/auth/oidc/start", endpoint="oidc_start"),
            Rule("/auth/oidc/callback", endpoint="oidc_callback"),
            Rule("/auth/saml/start", endpoint="saml_start"),
            Rule("/auth/saml/acs", endpoint="saml_acs", methods=["POST"]),
            Rule("/auth/saml/metadata", endpoint="saml_metadata"),
            Rule("/auth/local", endpoint="local_signin", methods=["POST"]),
            Rule("/setup", endpoint="setup", methods=["GET", "POST"]),
            Rule("/hops/<hop>", endpoint="hop_page"),
            Rule("/hops/<hop>/runs/<run>", endpoint="run_page"),
            Rule("/hops/<hop>/evidence/<run>", endpoint="evidence_page"),
            Rule("/hops/<hop>/decisions", endpoint="decisions_page"),
            Rule("/account", endpoint="account"),
            Rule("/account/prefs", endpoint="prefs", methods=["POST"]),
            Rule("/admin", endpoint="admin"),
            Rule("/api", endpoint="api_page"),
            Rule("/static/<name>", endpoint="static"),
            Rule("/metrics", endpoint="metrics"),
            Rule("/healthz", endpoint="healthz"),
            Rule("/scim/v2/", endpoint="scim",
                 methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
                 defaults={"rest": ""}),
            Rule("/scim/v2/<path:rest>", endpoint="scim",
                 methods=["GET", "POST", "PUT", "PATCH", "DELETE"]),
        ]
        for n, r in enumerate(api.ROUTES):
            rules.append(Rule(r.rule, endpoint=f"api:{n}",
                              methods=[r.method]))
        self.urls = Map(rules, strict_slashes=False)

    # ---- the record -------------------------------------------------------

    def record(self, entry):
        """An entry in the view's own record (sign-ins, provisioning,
        tokens, refusals), chained like a hop's."""
        from .. import audit
        audit.append(self.record_path, entry)

    # ---- WSGI ------------------------------------------------------------

    def __call__(self, environ, start_response):
        s = self.settings
        if s.proxies:
            if environ.get("REMOTE_ADDR", "") not in s.proxies:
                return Response("only the reverse proxy is answered here",
                                403)(environ, start_response)
            from werkzeug.middleware.proxy_fix import ProxyFix
            return ProxyFix(self._wsgi, x_for=1, x_proto=1, x_host=1,
                            x_port=1)(environ, start_response)
        return self._wsgi(environ, start_response)

    def _wsgi(self, environ, start_response):
        request = Request(environ)
        request.max_content_length = SCIM_MOST if request.path.startswith(
            "/scim/") else BODY_MOST
        request.max_form_memory_size = BODY_MOST
        try:
            response = self.dispatch(request)
        except HTTPException as e:
            response = self._error(request, e.code, e.description)
        self._harden(request, response)
        return response(environ, start_response)

    def _harden(self, request, response):
        h = response.headers
        h.setdefault("Content-Security-Policy",
                     "default-src 'none'; script-src 'self'; style-src"
                     " 'self'; img-src 'self' data:; connect-src 'self';"
                     " form-action 'self'; frame-ancestors 'none';"
                     " base-uri 'none'")
        h["X-Content-Type-Options"] = "nosniff"
        h["Referrer-Policy"] = "no-referrer"
        h["X-Frame-Options"] = "DENY"
        h["Cross-Origin-Opener-Policy"] = "same-origin"
        h["Cross-Origin-Resource-Policy"] = "same-origin"
        h["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        if not request.path.startswith("/static/"):
            h["Cache-Control"] = "no-store"
        if self.settings.https:
            h["Strict-Transport-Security"] = "max-age=31536000"

    def _host_ok(self, request):
        return request.host.lower() in self.settings.allowed_hosts()

    # ---- who is asking ----------------------------------------------------

    def who(self, request):
        """(principal, csrf token or None, prefs, how) of the caller, or
        None: a bearer API token (how "api"), or a session's cookie (how
        "web")."""
        s = self.settings
        auth = request.headers.get("Authorization", "")
        if auth.lower().startswith("bearer "):
            got = self.store.token(auth[7:].strip())
            if got is None:
                return None
            try:
                p = identity.admitted(Principal.from_dict(got), self.store, s)
            except Refused:
                return None
            return p, None, {}, "api"
        got = self.store.session(request.cookies.get(self.cookie),
                                 s.idle_minutes)
        if got is None:
            return None
        principal, csrf, prefs = got
        try:
            p = identity.admitted(Principal.from_dict(principal),
                                  self.store, s)
        except Refused:
            self.store.close_session(request.cookies.get(self.cookie))
            return None
        return p, csrf, prefs, "web"

    def _same_origin(self, request):
        """A request that changes something from a page came from this
        view's own pages: the browser's own word for it where it gives
        one, the Origin it sends otherwise."""
        site = request.headers.get("Sec-Fetch-Site")
        if site is not None and site not in ("same-origin", "none"):
            return False
        origin = request.headers.get("Origin")
        if origin is None or origin == "null":
            return origin is None
        return urlsplit(origin).netloc.lower() in \
            self.settings.allowed_hosts()

    def _csrf_ok(self, request, csrf):
        import hmac
        given = request.headers.get("X-CSRF-Token") \
            or request.form.get("csrf", "")
        return bool(csrf) and hmac.compare_digest(str(given), csrf) \
            and self._same_origin(request)

    # ---- dispatch ---------------------------------------------------------

    def dispatch(self, request):
        if not self._host_ok(request):
            return Response("not this host", 403, mimetype="text/plain")
        adapter = self.urls.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
        except HTTPException as e:
            if request.path.startswith("/api/"):
                return _json({"error": "not found"}, e.code or 404)
            raise
        if endpoint.startswith("api:"):
            return self._api(request, api.ROUTES[int(endpoint[4:])], values)
        return getattr(self, f"_p_{endpoint}")(request, **values)

    def _api(self, request, route, values):
        if route.need == "anyone":
            return _json(api.HANDLERS[route.handler](None))
        got = self.who(request)
        if got is None:
            return _json({"error": "sign in, or present an API token"}, 401)
        principal, csrf, prefs, through = got
        if request.method in UNSAFE and through == "web" \
                and not self._csrf_ok(request, csrf):
            self.record({"op": "refused", "why": "not from this view's page",
                         "path": request.path, "by": principal.id})
            return _json({"error": "not from this view's page"}, 403)
        body = {}
        if request.method in UNSAFE and request.content_length:
            body = request.get_json(silent=True)
            if not isinstance(body, dict):
                return _json({"error": "the body is a JSON object"}, 400)
        ctx = api.Ctx(self, principal, through, body, dict(request.args),
                      prefs=prefs, token=through == "api")
        from . import roles, views
        if route.need == "global admin":
            if not roles.is_admin(principal):
                return self._refuse(ctx, request, "this needs a global"
                                                  " admin")
        elif "hop" in values:
            hop, role = views.one(principal, values.pop("hop"))
            if hop is None:
                return _json({"error": "no such hop"}, 404)
            ctx.hop, ctx.role = hop, role
            if roles.RANK[role] < roles.RANK[route.need]:
                return self._refuse(ctx, request, f"your role on"
                                                  f" {hop.name} is {role};"
                                                  f" this needs"
                                                  f" {route.need}")
        kwargs = {api.PARAMS.get(k, k): v for k, v in values.items()}
        try:
            got = api.HANDLERS[route.handler](ctx, **kwargs)
        except api.Answer as e:
            if route.recorded:
                self.record({"op": "refused", "path": request.path,
                             "by": principal.id, "status": e.status,
                             "why": e.detail, "through": through})
            return _json({"error": e.detail}, e.status)
        if route.recorded:
            self.record({"op": route.handler, "path": request.path,
                         "by": principal.id, "through": through})
        if isinstance(got, tuple) and len(got) == 4:
            status, data, name, kind = got
            r = Response(data, status, mimetype=kind)
            r.headers["Content-Disposition"] = \
                f"attachment; filename=\"{name}\""
            # an evidence page is a download, never a page of this origin
            r.headers["Content-Security-Policy"] = "sandbox; default-src" \
                                                   " 'none'"
            return r
        if isinstance(got, tuple):
            return _json(got[1], got[0])
        return _json(got)

    def _refuse(self, ctx, request, why):
        self.record({"op": "refused", "path": request.path,
                     "by": ctx.principal.id, "status": 403, "why": why,
                     "through": ctx.through})
        if ctx.hop is not None:
            ctx.record({"op": "refused", "path": request.path, "why": why})
        return _json({"error": why}, 403)

    def _error(self, request, status, detail):
        if request.path.startswith(("/api/", "/scim/")):
            return _json({"error": detail or "refused"}, status)
        try:
            me = self._signed_in(request)
        except Exception:  # noqa: BLE001 - the error page shows anyway
            me = None
        return self._page(request, "error.html", me, status=status,
                          code=status, detail=detail)

    # ---- pages ------------------------------------------------------------

    def _page(self, request, template, me, status=200, **context):
        csrf = me[1] if me else None
        html = self.jinja.get_template(template).render(
            me=me[0] if me else None, csrf=csrf,
            prefs=me[2] if me else {}, s=self.settings,
            admin=bool(me) and _is_admin(me[0]), **context)
        return Response(html, status, mimetype="text/html")

    def _signed_in(self, request):
        got = self.who(request)
        if got is None or got[3] != "web":
            return None
        return got

    def _to_signin(self, request):
        return _redirect("/signin?next=" + quote(request.full_path.rstrip(
            "?"), safe=""))

    def _p_healthz(self, request):
        return Response("ok", mimetype="text/plain")

    def _p_static(self, request, name):
        if name not in STATIC:
            return Response("not found", 404)
        data = resources.files("migkit.web").joinpath("static").joinpath(
            name).read_bytes()
        r = Response(data, mimetype=STATIC[name].split(";")[0])
        r.headers["Content-Type"] = STATIC[name]
        r.headers["Cache-Control"] = "public, max-age=3600"
        return r

    def _p_home(self, request):
        me = self._signed_in(request)
        if me is None:
            return self._to_signin(request)
        from . import views
        _, error = views.loaded()
        cards = [views.card(n, h, r) for n, h, r in views.visible(me[0])]
        return self._page(request, "home.html", me, cards=cards,
                          error=error)

    def _form_token(self, request, response=None):
        token = request.cookies.get(self.form_cookie)
        if not token or len(token) < 20:
            token = secrets.token_urlsafe(24)
        return token

    def _set_form_cookie(self, response, token):
        response.set_cookie(self.form_cookie, token, httponly=True,
                            samesite="Strict", secure=self.settings.https,
                            path="/", max_age=3600)

    def _form_ok(self, request):
        import hmac
        cookie = request.cookies.get(self.form_cookie, "")
        return bool(cookie) and hmac.compare_digest(
            cookie, request.form.get("form", "")) \
            and self._same_origin(request)

    def _p_signin(self, request, error="", status=200):
        if self._signed_in(request) is not None and not error:
            return _redirect(_safe_next(request.args.get("next")))
        token = self._form_token(request)
        needs_setup = self.local is not None and not self.local.exists()
        r = self._page(request, "signin.html", None, status=status,
                       error=error, form=token, oidc=self.oidc is not None,
                       saml=self.saml is not None,
                       local=self.local is not None, setup=needs_setup,
                       next=_safe_next(request.args.get("next")))
        self._set_form_cookie(r, token)
        return r

    def _sign_in(self, request, principal, until=None, then="/"):
        """A session for the person, a new one, and the record of it."""
        principal = identity.admitted(principal, self.store, self.settings)
        value, _ = self.store.open_session(principal.as_dict(),
                                           self.settings.session_hours,
                                           until)
        self.record({"op": "signed-in", "by": principal.id,
                     "via": principal.via, "issuer": principal.issuer,
                     "from": request.remote_addr})
        r = _redirect(_safe_next(then))
        r.set_cookie(self.cookie, value, httponly=True, samesite="Lax",
                     secure=self.settings.https, path="/",
                     max_age=int(self.settings.session_hours * 3600))
        r.delete_cookie(self.signin_cookie, path="/",
                        secure=self.settings.https)
        return r

    def _refused_signin(self, request, e, how):
        self.record({"op": "sign-in-refused", "via": how,
                     "why": str(e), "from": request.remote_addr})
        return self._p_signin(request, error=str(e), status=403)

    def _signin_cookie(self, response, value, cross_site_post=False):
        https = self.settings.https
        response.set_cookie(
            self.signin_cookie, value, httponly=True, path="/",
            secure=https, max_age=identity.PENDING,
            # the provider posts its SAML answer from its own site: the
            # cookie binding the sign-in to this browser has to go with
            # it, which only SameSite=None (and so Secure) allows
            samesite="None" if cross_site_post and https else "Lax")

    def _p_oidc_start(self, request):
        if self.oidc is None:
            return Response("not found", 404)
        try:
            url, browser = self.oidc.begin(_safe_next(
                request.args.get("next")))
        except Refused as e:
            return self._refused_signin(request, e, "oidc")
        r = _redirect(url)
        self._signin_cookie(r, browser)
        return r

    def _p_oidc_callback(self, request):
        if self.oidc is None:
            return Response("not found", 404)
        try:
            person, then, until = self.oidc.finish(
                request.args, request.cookies.get(self.signin_cookie))
            return self._sign_in(request, person, until, then)
        except Refused as e:
            return self._refused_signin(request, e, "oidc")

    def _p_saml_start(self, request):
        if self.saml is None:
            return Response("not found", 404)
        url, browser = self.saml.begin(_safe_next(request.args.get("next")))
        r = _redirect(url)
        self._signin_cookie(r, browser, cross_site_post=True)
        return r

    def _p_saml_acs(self, request):
        if self.saml is None:
            return Response("not found", 404)
        try:
            person, then, until = self.saml.finish(
                request.form.get("SAMLResponse", ""),
                request.form.get("RelayState", ""),
                request.cookies.get(self.signin_cookie))
            return self._sign_in(request, person, until, then)
        except Refused as e:
            return self._refused_signin(request, e, "saml")

    def _p_saml_metadata(self, request):
        if self.saml is None:
            return Response("not found", 404)
        return Response(self.saml.metadata_xml(),
                        mimetype="application/samlmetadata+xml")

    def _p_local_signin(self, request):
        if self.local is None:
            return Response("not found", 404)
        if not self._form_ok(request):
            return self._refused_signin(request, Refused(
                "that form was not this view's - sign in again"), "local")
        try:
            person = self.local.sign_in(request.form.get("name"),
                                        request.form.get("password"),
                                        request.remote_addr)
        except Refused as e:
            return self._refused_signin(request, e, "local")
        return self._sign_in(request, person,
                             then=request.form.get("next") or "/")

    def _p_setup(self, request):
        if self.local is None:
            return Response("not found", 404)
        token = self._form_token(request)
        error = ""
        if request.method == "POST":
            if not self._form_ok(request):
                error = "that form was not this view's - open the printed" \
                        " address again"
            else:
                try:
                    again = self.local.exists()
                    person = self.local.set_up(
                        request.form.get("code", ""),
                        request.form.get("name", ""),
                        request.form.get("password", ""),
                        request.form.get("again", ""))
                    self.record({"op": "local-account-set",
                                 "by": person.id, "again": again,
                                 "from": request.remote_addr})
                    return self._sign_in(request, person)
                except Refused as e:
                    error = str(e)
                    self.record({"op": "local-account-refused",
                                 "why": error, "from": request.remote_addr})
        r = self._page(request, "setup.html", None,
                       status=403 if error else 200, error=error,
                       form=token, again=self.local.exists())
        self._set_form_cookie(r, token)
        return r

    def _p_signout(self, request):
        me = self._signed_in(request)
        if me is not None:
            if not self._csrf_ok(request, me[1]):
                return self._error(request, 403, "not from this view's page")
            self.store.close_session(request.cookies.get(self.cookie))
            self.record({"op": "signed-out", "by": me[0].id})
        r = _redirect("/signin")
        r.delete_cookie(self.cookie, path="/", secure=self.settings.https)
        return r

    def _hop(self, request, name):
        me = self._signed_in(request)
        if me is None:
            return None, None, self._to_signin(request)
        from . import views
        hop, role = views.one(me[0], name)
        if hop is None:
            return me, None, self._error(request, 404, "no such hop")
        return me, (hop, role), None

    def _p_hop_page(self, request, hop):
        me, got, stop = self._hop(request, hop)
        if stop is not None:
            return stop
        from . import approve, runs, views
        hop, role = got
        explain = bool(me[2].get("explain", self.settings.explain))
        return self._page(
            request, "hop.html", me, hop=hop, role=role,
            card=views.card(hop.name, hop, role),
            outcome=views.outcome(hop), progress=views.progress(hop),
            approvals=views.approvals(hop, me[0]),
            counts=approve.accepts(hop), offers=views.offers(hop),
            runs=runs.listing(hop, 20), evidence=views.evidence(hop),
            notifications=views.notifications(hop),
            config=views.configuration(hop), actions=runs.ACTIONS,
            decisions=views.decisions(hop, 30) if explain else None,
            explain=explain, may=_may(role))

    def _p_run_page(self, request, hop, run):
        me, got, stop = self._hop(request, hop)
        if stop is not None:
            return stop
        from . import runs
        hop, role = got
        rec = runs.read(hop, run)
        if rec is None:
            return self._error(request, 404, "no such run")
        text, offset = runs.output(hop, run, 0)
        return self._page(request, "run.html", me, hop=hop, role=role,
                          run=rec, text=text, offset=offset,
                          verdict=runs.verdict_of(hop, rec),
                          evidence=runs.evidence_of(hop, rec),
                          may=_may(role))

    def _p_evidence_page(self, request, hop, run):
        me, got, stop = self._hop(request, hop)
        if stop is not None:
            return stop
        from . import views
        hop, role = got
        which = "" if run == "all" else run
        doc = views.summary(hop, which)
        if doc is None:
            return self._error(request, 404, "no bundle this view can read -"
                                             " it may be sealed for its"
                                             " recipients; download it")
        return self._page(request, "evidence.html", me, hop=hop, role=role,
                          run=which, doc=doc)

    def _p_decisions_page(self, request, hop):
        me, got, stop = self._hop(request, hop)
        if stop is not None:
            return stop
        from . import views
        hop, role = got
        return self._page(request, "decisions.html", me, hop=hop,
                          role=role, decisions=views.decisions(hop))

    def _p_account(self, request):
        me = self._signed_in(request)
        if me is None:
            return self._to_signin(request)
        from . import roles, views
        return self._page(request, "account.html", me,
                          tokens=self.store.tokens_of(me[0].id),
                          hops=views.visible(me[0]), roles=roles,
                          days=self.settings.token_days)

    def _p_prefs(self, request):
        me = self._signed_in(request)
        if me is None:
            return self._to_signin(request)
        if not self._csrf_ok(request, me[1]):
            return self._error(request, 403, "not from this view's page")
        prefs = dict(me[2])
        prefs["explain"] = request.form.get("explain") in ("1", "on", "true")
        self.store.set_prefs(request.cookies.get(self.cookie), prefs)
        return _redirect(_safe_next(request.form.get("next") or "/account"))

    def _p_admin(self, request):
        me = self._signed_in(request)
        if me is None:
            return self._to_signin(request)
        if not _is_admin(me[0]):
            return self._error(request, 403, "this needs a global admin")
        ctx = api.Ctx(self, me[0], "web", {}, {})
        return self._page(request, "admin.html", me,
                          people=api.people(ctx), sessions=api.sessions(ctx),
                          record=api.record(ctx))

    def _p_api_page(self, request):
        me = self._signed_in(request)
        return self._page(request, "api.html", me, doc=api.document(),
                          routes=api.ROUTES, base=api.BASE)

    def _p_metrics(self, request):
        import hmac
        s = self.settings
        given = request.headers.get("Authorization", "")
        if s.metrics_token and given.startswith("Bearer ") and \
                hmac.compare_digest(given[7:].strip(), s.metrics_token):
            # the scraper: every hop
            user = None
        else:
            got = self.who(request)
            if got is None:
                r = Response("a scrape carries the bearer"
                             " MIGKIT_METRICS_TOKEN; a person signs in", 401,
                             mimetype="text/plain")
                r.headers["WWW-Authenticate"] = "Bearer"
                return r
            # a person: the hops theirs to see
            user = got[0]
        return Response(self.metrics.text(user),
                        mimetype="text/plain; version=0.0.4")

    def _p_scim(self, request, rest=""):
        import hmac

        from .scim import MEDIA, Directory, ScimError
        s = self.settings
        if not s.scim:
            return Response("not found", 404)
        given = request.headers.get("Authorization", "")
        if not hmac.compare_digest(given.encode(),
                                   f"Bearer {s.scim_token}".encode()):
            self.record({"op": "refused", "path": request.path,
                         "why": "provisioning without its token",
                         "from": request.remote_addr})
            return Response(json.dumps(ScimError(
                401, "the provisioning token is needed").body()), 401,
                mimetype=MEDIA)
        body = None
        if request.method in ("POST", "PUT", "PATCH"):
            try:
                body = json.loads(request.get_data(as_text=True) or "null")
            except ValueError:
                body = None
        directory = Directory(self.store, f"{s.url}/scim/v2",
                              lambda e: self.record({**e, "by": "the"
                                                     " provisioning"
                                                     " connector"}))
        try:
            status, out = directory.handle(
                request.method, [p for p in rest.split("/") if p],
                request.args, body)
        except ScimError as e:
            return Response(json.dumps(e.body()), e.status, mimetype=MEDIA)
        if out is None:
            return Response(status=status)
        r = Response(json.dumps(out), status, mimetype=MEDIA)
        if isinstance(out, dict) and (out.get("meta") or {}).get("location"):
            r.headers["Location"] = out["meta"]["location"]
        return r


def _metrics():
    """`ui.Collector`, with the hops a person may see given by the view's
    roles: a scraper with the metrics token reads every hop, a person
    signed in only theirs."""
    from .. import ui
    from . import views

    def hops():
        got, error = views.loaded()
        if error:
            raise SystemExit(error)
        return got

    class Metrics(ui.Collector):
        def visible(self, user):
            if user is None:
                return None
            return {name for name, _, _ in views.visible(user)}
    return Metrics(loader=hops)


def _is_admin(principal):
    from . import roles
    return roles.is_admin(principal)


def _may(role):
    from . import roles
    return {r: roles.RANK[role] >= roles.RANK[r] for r in roles.ROLES}


def _json(data, status=200):
    return Response(json.dumps(data, default=str), status,
                    mimetype="application/json")


def _redirect(where):
    r = Response(status=303)
    r.headers["Location"] = where
    return r


def _when(epoch):
    if not epoch:
        return "-"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(epoch)))
    except (TypeError, ValueError):
        return str(epoch)


def _took(secs):
    from .. import sourcechange
    if secs is None:
        return "-"
    return sourcechange.took(secs)


def _num(n):
    try:
        return f"{int(n or 0):,}"
    except (TypeError, ValueError):
        return str(n)
