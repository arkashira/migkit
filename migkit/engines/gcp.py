"""Google's SDK clients, made one way for every Google service a side is:
the project from the side's host (or its `project`), a service account's
key file where the side names one (`credentials_file`) and the machine's
own Application Default Credentials otherwise, and an emulator where the
side names one (`emulator: host:port`).

Bigtable's, Firestore's and Datastore's SDKs read their emulator from an
environment variable when a client is made, and keep it: the variable is
set for the one client being made, under a lock, and put back - so two
sides, one on an emulator and one not, each get their own.
"""
import os
import threading

_ENV = threading.Lock()


def project(ep):
    return str(ep.options.get("project") or ep.host or "") or None


def credentials(ep):
    """A service account's credentials where the side names its key file;
    None for the SDK's own default."""
    if not ep.options.get("credentials_file") or ep.options.get("emulator"):
        return None
    from google.oauth2 import service_account
    return service_account.Credentials.from_service_account_file(
        str(ep.options["credentials_file"]))


def client(make, variable, ep, **kw):
    """`make(**kw)` with the SDK's emulator variable set to the side's
    emulator, or unset where it names none, while the client is made."""
    emulator = ep.options.get("emulator")
    creds = credentials(ep)
    if creds is not None:
        kw["credentials"] = creds
    with _ENV:
        was = os.environ.get(variable)
        if emulator:
            os.environ[variable] = str(emulator)
        else:
            os.environ.pop(variable, None)
        try:
            return make(**kw)
        finally:
            if was is None:
                os.environ.pop(variable, None)
            else:
                os.environ[variable] = was
