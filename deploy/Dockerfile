# migkit as a self-hosted migration service (stands in for DMS/DTS on a VM).
#
# Built the same twice: the base image by digest, Debian's packages as they
# stood on one day, every Python package by the digests of its files
# (migkit/data/requirements.txt), and every program by migkit's own
# installer, which holds each file to its lock before it is used
# (migkit/data/programs.lock.json). A program whose terms are not open is
# put in only where the build names it as accepted:
#   docker build --build-arg MIGKIT_ACCEPT_TERMS=atlas,liquibase ...
# What never enters the image - hops.yaml and its passwords, reports, the
# checkout's own files - is in .dockerignore beside the repository root.
FROM python:3.12-slim-bookworm@sha256:392307d22300de8b5986851a12d9176dfc0fc073e65bf6523ebd7dcbeb23564e
ARG DEBIAN_SNAPSHOT=20261001T000000Z
ARG MIGKIT_ACCEPT_TERMS=
ENV SOURCE_DATE_EPOCH=1790812800 PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 MIGKIT_TOOLS=/opt/migkit/tools
RUN if [ -n "$DEBIAN_SNAPSHOT" ]; then \
      printf 'Types: deb\nURIs: https://snapshot.debian.org/archive/debian/%s\nSuites: bookworm bookworm-updates\nComponents: main\nCheck-Valid-Until: no\n\nTypes: deb\nURIs: https://snapshot.debian.org/archive/debian-security/%s\nSuites: bookworm-security\nComponents: main\nCheck-Valid-Until: no\n' \
        "$DEBIAN_SNAPSHOT" "$DEBIAN_SNAPSHOT" > /etc/apt/sources.list.d/debian.sources; \
    fi \
 && apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml README.md LICENSE NOTICE /app/
COPY migkit /app/migkit
RUN pip install --require-hashes -r migkit/data/requirements.txt \
 && pip install --no-deps --no-build-isolation .
RUN apt-get update \
 && MIGKIT_ACCEPT_TERMS="$MIGKIT_ACCEPT_TERMS" migkit doctor --install \
 && rm -rf /var/lib/apt/lists/*
ENV MIGKIT_CONF=/conf/hops.yaml MIGKIT_REPORTS=/state/reports
VOLUME ["/conf", "/state"]
ENTRYPOINT ["migkit"]
CMD ["doctor"]
