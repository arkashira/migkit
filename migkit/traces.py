"""A run as OpenTelemetry spans, beside the Prometheus metrics (backlog
R23.8).

Off until the operator's environment says where spans go, as every
OpenTelemetry program is told: `OTEL_EXPORTER_OTLP_ENDPOINT` or
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (with the rest of the standard
`OTEL_*` variables - headers, protocol, `OTEL_SERVICE_NAME`, and
`OTEL_SDK_DISABLED=true` to turn it off again). Off, every call here is
the API's own no-op and costs nothing measurable.

On, each command is a span (`command`) - its hop, its engines, the run's
id (`session`) - and under it:
* each check of each database, with its status (`answered`)
* each table copied, with the rows it carried
* each phase of a cutover, with the seconds it took (`past`)
* each way the engine chose, as an event with its reason (`event`)

in migkit's words and names, whatever ran underneath: a span never names
a program migkit drives, only what migkit did with it. Spans begun on a
worker thread, where the command's span is not current, are put under
it, so a check's databases side by side stay one trace.
"""
import contextlib
import os
import threading

from opentelemetry import context as otel_context
from opentelemetry import trace

_PROVIDER = None
_TRACER = None
#: the context of the command's span, for spans begun on other threads
_ROOT = None
_LOCK = threading.Lock()

#: the attributes' prefix
NS = "migkit."


def configure(exporter=None):
    """The tracer, with spans going to `exporter` - an OTLP exporter
    built from the environment where it names an endpoint, else none and
    the no-op tracer. A test hands its own in-memory exporter."""
    global _PROVIDER, _TRACER
    with _LOCK:
        if _PROVIDER is not None:
            _PROVIDER.shutdown()
        _PROVIDER = None
        if exporter is None and _wanted():
            from opentelemetry.exporter.otlp.proto.http.trace_exporter \
                import OTLPSpanExporter
            exporter = OTLPSpanExporter()
        if exporter is None:
            _TRACER = trace.NoOpTracer()
            return _TRACER
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import (BatchSpanProcessor,
                                                    SimpleSpanProcessor)

        from . import __version__
        _PROVIDER = TracerProvider(resource=Resource.create({
            "service.name": os.environ.get("OTEL_SERVICE_NAME", "migkit"),
            "service.version": __version__}))
        # batched where they cross a network, at once where a test reads
        # them back in the same breath
        processor = (BatchSpanProcessor if type(exporter).__name__
                     .startswith("OTLP") else SimpleSpanProcessor)
        _PROVIDER.add_span_processor(processor(exporter))
        _TRACER = _PROVIDER.get_tracer("migkit", __version__)
        return _TRACER


def _wanted():
    if os.environ.get("OTEL_SDK_DISABLED", "").lower() == "true":
        return False
    return bool(os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
                or os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))


def tracer():
    if _TRACER is None:
        configure()
    return _TRACER


def enabled():
    return not isinstance(tracer(), trace.NoOpTracer)


def _attrs(values):
    """Attributes as OpenTelemetry takes them: under migkit's prefix,
    plain values only, None left out."""
    out = {}
    for k, v in values.items():
        if v is None:
            continue
        if not isinstance(v, (str, bool, int, float)):
            v = str(v)
        out[k if "." in k else NS + k] = v
    return out


def _failed(s, e):
    """The span's error, said by the kind of the exception only: its
    words can quote a row."""
    from opentelemetry.trace import Status, StatusCode
    s.set_status(Status(StatusCode.ERROR, type(e).__name__))


@contextlib.contextmanager
def span(name, **attrs):
    """A span under the one current here, or under the command's where
    none is (a worker thread)."""
    if not enabled():
        yield trace.INVALID_SPAN
        return
    parent = None
    if not trace.get_current_span().get_span_context().is_valid:
        parent = _ROOT
    with tracer().start_as_current_span(
            name, context=parent, attributes=_attrs(attrs),
            record_exception=False, set_status_on_exception=False) as s:
        try:
            yield s
        except Exception as e:
            _failed(s, e)
            raise


def attributes(s, **attrs):
    if s is not None and s.is_recording():
        s.set_attributes(_attrs(attrs))


#: a status's weight: the worst of a check's answers is the span's status
_WORST = {"ok": 0, "skip": 0, "warn": 1, "diff": 2, "error": 3}


def answered(s, results):
    """A check's span given the worst of its answers; an error is the
    span's error."""
    if s is None or not s.is_recording():
        return
    worst = max((r.get("status", "ok") for r in results),
                key=lambda x: _WORST.get(x, 0), default="ok")
    s.set_attribute(NS + "status", worst)
    s.set_attribute(NS + "answers", len(results))
    if worst == "error":
        from opentelemetry.trace import Status, StatusCode
        s.set_status(Status(StatusCode.ERROR, "the check could not answer"))


def event(name, **attrs):
    """An event on the span current here, or on the command's."""
    if not enabled():
        return
    s = trace.get_current_span()
    if not s.get_span_context().is_valid and _ROOT is not None:
        s = trace.get_current_span(_ROOT)
    if s.is_recording():
        s.add_event(name, _attrs(attrs))


def past(name, began, ended, **attrs):
    """A span for something that already happened, at the times it did
    (epoch seconds): a cutover's phase, timed as it ran."""
    if not enabled() or not began:
        return
    ctx = None if trace.get_current_span().get_span_context().is_valid \
        else _ROOT
    s = tracer().start_span(name, context=ctx, attributes=_attrs(attrs),
                            start_time=int(float(began) * 1e9))
    s.end(end_time=int(float(ended or began) * 1e9))


#: a command's parameters carried on its span: never a password, never a
#: file of one
_SAID = ("hop_name", "db", "table", "mode", "kind", "only", "go",
         "do_apply", "consistent", "verify", "delta", "count", "interval")


@contextlib.contextmanager
def command(name, hop, params, run):
    """The command's span, the root of the run's trace."""
    global _ROOT
    if not enabled():
        yield None
        return
    attrs = {"command": name, "session": run}
    attrs.update({k.replace("hop_name", "hop").replace("do_apply", "apply"):
                  v for k, v in (params or {}).items()
                  if k in _SAID and v not in (None, "", False)})
    if hop is not None:
        attrs["engine"] = hop.engine
        opts = hop.options or {}
        if hop.engine == "hetero":
            attrs["source_engine"] = opts.get("source_engine")
            attrs["target_engine"] = opts.get("target_engine")
    with tracer().start_as_current_span(
            f"migkit {name}", attributes=_attrs(attrs),
            record_exception=False, set_status_on_exception=False) as s:
        _ROOT = otel_context.get_current()
        try:
            yield s
        except SystemExit as e:
            # a number is a verdict's exit (a difference found), not an
            # error; words are migkit stopping
            if isinstance(e.code, str) and e.code.strip():
                from opentelemetry.trace import Status, StatusCode
                s.set_status(Status(StatusCode.ERROR, "stopped"))
            elif e.code not in (None, 0):
                s.set_attribute(NS + "exit", str(e.code))
            raise
        except KeyboardInterrupt:
            s.set_attribute(NS + "interrupted", True)
            raise
        except Exception as e:
            _failed(s, e)
            raise
        finally:
            _ROOT = None


def flush():
    """Every span sent before the process ends."""
    if _PROVIDER is not None:
        try:
            _PROVIDER.force_flush(timeout_millis=5000)
        except Exception:  # noqa: BLE001 - a trace lost is said nowhere
            pass


# ---- the library's account (`tooluse`, backlog 0f) ---------------------------
# what of it is used is read off the functions above; only the reasons for
# the rest are written

_FEATURES = {"TracerProvider": "TracerProvider", "Resource": "Resource",
             "BatchSpanProcessor": "BatchSpanProcessor",
             "SimpleSpanProcessor": "SimpleSpanProcessor",
             "OTLPSpanExporter": "OTLPSpanExporter",
             "start_as_current_span": "start_as_current_span",
             "start_span": "start_span", "set_attributes": "set_attributes",
             "set_status": "set_status", "add_event": "add_event",
             "force_flush": "force_flush", "context": "otel_context",
             "metrics": "MeterProvider", "logs": "LoggerProvider",
             "propagation": "inject", "sampling": "TraceIdRatioBased"}


def _account():
    from . import tooluse
    unused = {
        "metrics": ("the metrics are migkit's Prometheus endpoint, which"
                    " the team's scraper already reads"),
        "logs": ("the run's words stay in its own output and its evidence"
                 " export, where the hop's masking holds"),
        "propagation": ("no request migkit makes goes to a service that"
                        " reads a trace's headers"),
        "sampling": ("a run is a few hundred spans at most, all kept; the"
                     " environment's own sampler setting still applies"),
    }
    tooluse.register(
        "opentelemetry", licence="Apache-2.0", release="1.45.0",
        declared={"": frozenset(_FEATURES)},
        reads=(tuple(f"traces.py::{fn}" for fn in (
            "configure", "span", "attributes", "answered", "event", "past",
            "command", "flush", "_failed")),
            {("", f): t for f, t in _FEATURES.items()}),
        options={("", f): (tooluse.UNUSED, why)
                 for f, why in unused.items()})


_account()
