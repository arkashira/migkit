"""The vendors' own emulators, started for a test module on its lane
(`servers.private`): the Google emulators of Bigtable, Firestore and
Datastore from one image, Spanner's from its own, and DynamoDB Local. Each is a block that yields
`host:port` or skips the module, naming why, where the emulator cannot run
here (no docker, the image absent and not pulled in time, or the emulator
not answering). A module proves a cell only where the emulator answered."""
import contextlib
import time

import pytest

from tests import servers
from tests.conftest import _wait_tcp

HOST = servers.HOST
#: one image holds the Bigtable, Firestore, Datastore and Spanner emulators
GOOGLE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"
SPANNER = "gcr.io/cloud-spanner-emulator/emulator:latest"
DYNAMO = "amazon/dynamodb-local:latest"
#: seconds to wait for an emulator to answer
READY = 90
PULL = 400

#: kind -> (image, the command, the inner port); each kind maps its port
#: to its own private port of the lane (its place here), so two run at once
KINDS = {
    "bigtable": (GOOGLE, ("gcloud", "beta", "emulators", "bigtable", "start",
                          "--host-port=0.0.0.0:8086"), 8086),
    "firestore": (GOOGLE, ("gcloud", "emulators", "firestore", "start",
                           "--host-port=0.0.0.0:8080"), 8080),
    # Datastore is the Firestore emulator in its Datastore mode
    "datastore": (GOOGLE, ("gcloud", "emulators", "firestore", "start",
                           "--database-mode=datastore-mode",
                           "--host-port=0.0.0.0:8080"), 8080),
    "spanner": (SPANNER, (), 9010),
    "dynamodb": (DYNAMO, ("-jar", "DynamoDBLocal.jar", "-inMemory",
                          "-sharedDb"), 8000),
}


def have(image):
    return servers.docker("image", "inspect", image,
                          timeout=30).returncode == 0


def image_or_skip(image):
    """The image here, pulled within the budget if it is not; skips the
    module naming why where it cannot be had."""
    if not servers.docker_up():
        pytest.skip("no docker here: the emulator was not run")
    if have(image):
        return
    try:
        got = servers.docker("pull", image, timeout=PULL)
    except Exception as e:  # noqa: BLE001 - a slow or absent registry
        pytest.skip(f"{image} was not pulled in {PULL}s ({type(e).__name__})")
    if got.returncode != 0:
        pytest.skip(f"{image} cannot be pulled: {got.stderr.strip()[-200:]}")


@contextlib.contextmanager
def emulator(kind, module, ready=None):
    """Start the `kind` emulator for `module` and yield its `host:port`;
    `ready(address)` is called until it stops raising (the emulator
    answering, not only its port open). Removed when the block ends."""
    image, argv, inner = KINDS[kind]
    image_or_skip(image)
    with servers.private(module, kind, image, *argv, ports=(inner,),
                         first=list(KINDS).index(kind)) as (
            container, mapped):
        address = f"{HOST}:{mapped[inner]}"
        stop = time.time() + READY
        if not _wait_tcp(HOST, mapped[inner], timeout=READY):
            why = servers.logs(container, 8)
            pytest.skip(f"the {kind} emulator did not open its port: {why}")
        last = None
        while ready is not None and time.time() < stop:
            try:
                ready(address)
                break
            except Exception as e:  # noqa: BLE001 - not answering yet
                last = e
                time.sleep(1)
        else:
            if ready is not None:
                pytest.skip(f"the {kind} emulator did not answer: {last!r}")
        yield address
