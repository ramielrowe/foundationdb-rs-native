# syntax=docker/dockerfile:1
# Upstream bindingtester + Python binding + libfdb_c, for one FDB release. Built by run.sh from a
# context it assembles (see README.md); do not `docker build` this by hand.
ARG FDB_IMAGE
ARG BASE_IMAGE

FROM ${FDB_IMAGE} AS fdb

FROM ${BASE_IMAGE}
ARG FDB_VERSION
ARG LIBFDB_C_SHA256
LABEL fdb-native-bindingtester="1" fdb-native-bindingtester.fdb-version="${FDB_VERSION}"

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 \
 && rm -rf /var/lib/apt/lists/*

# libfdb_c from the official server image of the same release (pinned by digest).
COPY --from=fdb /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so
RUN echo "${LIBFDB_C_SHA256}  /usr/lib/libfdb_c.so" | sha256sum -c - \
 && chmod 755 /usr/lib/libfdb_c.so && ldconfig

# bindings/bindingtester and bindings/python/tests at the release commit (git archive). The
# bindingtester looks for the Python binding at <root>/bindings/python.
COPY src/ /opt/fdb/
# The `fdb` Python package: the official wheel, verified by run.sh before the build.
COPY fdb.whl /tmp/fdb.whl
RUN python3 -c "import zipfile; z = zipfile.ZipFile('/tmp/fdb.whl'); z.extractall('/opt/fdb/bindings/python', [n for n in z.namelist() if n.startswith('fdb/')])" \
 && rm /tmp/fdb.whl

# bindingtester/__init__.py imports fdb before it extends sys.path, so the package must be importable.
ENV PYTHONPATH=/opt/fdb/bindings/python

COPY overlay/run_bindingtester.py /opt/bt/run_bindingtester.py

WORKDIR /opt/fdb/bindings/bindingtester
CMD ["sleep", "infinity"]
