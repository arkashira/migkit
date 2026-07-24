# migkit as a self-hosted migration service (stands in for DMS/DTS on a VM)
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      postgresql-client default-mysql-client mydumper pgloader \
      ca-certificates curl gnupg && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -e ".[mysql,mongo]" && \
    pip install --no-cache-dir reladiff datacompy 2>/dev/null || true
# atlas (schema apply) — optional, pulled at build if reachable
RUN curl -sSf https://release.ariga.io/atlas/atlas-linux-amd64-latest \
      -o /usr/local/bin/atlas && chmod +x /usr/local/bin/atlas || true
ENV MIGKIT_CONF=/conf/hops.yaml MIGKIT_REPORTS=/state/reports
VOLUME ["/conf", "/state"]
ENTRYPOINT ["migkit"]
CMD ["doctor"]
