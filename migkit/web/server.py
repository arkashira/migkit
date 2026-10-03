"""The web view served: `migkit report --serve` (no new command), on the
address `MIGKIT_UI_BIND` and the port `--port` - this machine's loopback
by default - over TLS where it is given a certificate, or behind a
reverse proxy that does TLS (`settings`).

The same process fires the hops that run on a schedule of their own
(`schedule.loop`), as the dashboard it serves in place of did: it is the
one process of migkit's that runs anyway.

Served by a threaded production server (the one CherryPy ships), with
TLS 1.2 at the least where it holds the certificate itself.
"""
import ssl
import threading

#: requests served at once
THREADS = 16


def serve(port):
    from cheroot import wsgi

    from .. import config, schedule
    from . import settings as _settings
    from .app import App
    s = _settings.load(port)
    app = App(s)
    if app.local is not None and (not app.local.exists() or s.reset):
        code = app.local.setup_code()
        print(f"migkit: set the operator's password at {s.url}/setup#{code}"
              " (the address works once, for 30 minutes)")
    if s.plain and not s.bound_locally and not s.cert:
        print(f"migkit: the web view listens on {s.bind} without TLS"
              " (MIGKIT_WEB_PLAIN): keep it published on this host's"
              " loopback only")
    who = ("an identity provider" if s.sso else
           "the operator's own account")
    print(f"migkit: the web view at {s.url}, signed in through {who}"
          + (", people provisioned at /scim/v2" if s.scim else "")
          + " (ctrl-c to stop)")
    server = wsgi.Server((s.bind, s.port), app, numthreads=THREADS,
                         server_name="migkit")
    if s.cert:
        from cheroot.ssl.builtin import BuiltinSSLAdapter
        adapter = BuiltinSSLAdapter(s.cert, s.key)
        adapter.context.minimum_version = ssl.TLSVersion.TLSv1_2
        server.ssl_adapter = adapter
    stop = threading.Event()
    threading.Thread(target=schedule.loop, args=(config.load_hops, stop),
                     daemon=True).start()
    # `/metrics` served from a snapshot taken beside the view, as the
    # dashboard serves it
    app.metrics.start()
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        app.metrics.stop()
        server.stop()
        app.store.close()
