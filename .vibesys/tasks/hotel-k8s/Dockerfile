# The hotel-k8s task image: every job of the task runs in it, and so do its pods in the
# cluster (the benchmark's seed, probe and phase pods; the check's seed, ready and oracle
# Jobs), which run this image rebuilt in-cluster from the task directory.
#
# It holds vibesys-evaluator (the Python package, loadgen, kubectl, buildctl), built here
# from a pinned vibesys commit as sdk/vibesys-evaluator/image/Dockerfile does until the
# image is published (a released task writes `FROM vibesys-evaluator:<ver>@sha256:...`),
# pymongo for the seed, and the task's Go programs from its evaluator/ module:
# hotel-generator (the benchmark's request generator and checks, on the vibesys-evaluator
# Go SDK of VIBESYS_COMMIT, the same commit as loadgen), hotel-correctness (the accuracy
# check's semantic oracle) and podctl (pod restarts and readiness). The oracle builds
# against the servicebench accuracy framework of an older vibesys commit.

ARG VIBESYS_COMMIT=505f38e556dabe86a2855708e5cde6081880b763
ARG ORACLE_VIBESYS_COMMIT=de037943cb6a8e6e73d00e8bc19fa53570558583
ARG KUBECTL_VERSION=v1.34.0
ARG KUBECTL_SHA256=cfda68cba5848bc3b6c6135ae2f20ba2c78de20059f68789c090166d6abc3e2c

FROM golang:1.25.9-bookworm@sha256:298734aec230b5f3e8cee450ce6d7eccc39f1797ba548ee90d57e9803030c6c3 AS evaluator
ARG VIBESYS_COMMIT
ARG KUBECTL_VERSION
ARG KUBECTL_SHA256
RUN git init -q /src \
 && git -C /src fetch -q --depth 1 https://github.com/uw-syfi/vibesys.git "${VIBESYS_COMMIT}" \
 && git -C /src checkout -q FETCH_HEAD \
 && test "$(git -C /src rev-parse HEAD)" = "${VIBESYS_COMMIT}"
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
RUN cd /src/sdk/vibesys-evaluator/loadgen && go build -o /out/ ./cmd/loadgen ./cmd/otelcapture
RUN curl -fsSL -o /out/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
 && echo "${KUBECTL_SHA256}  /out/kubectl" | sha256sum -c - \
 && chmod 0755 /out/kubectl

FROM golang:1.25.9-bookworm@sha256:298734aec230b5f3e8cee450ce6d7eccc39f1797ba548ee90d57e9803030c6c3 AS oracle
ARG ORACLE_VIBESYS_COMMIT
RUN git init -q /src \
 && git -C /src fetch -q --depth 1 https://github.com/uw-syfi/vibesys.git "${ORACLE_VIBESYS_COMMIT}" \
 && git -C /src checkout -q FETCH_HEAD \
 && test "$(git -C /src rev-parse HEAD)" = "${ORACLE_VIBESYS_COMMIT}"
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
# evaluator/go.mod replaces the servicebench modules by the clone at /src and the Go SDK by
# the VIBESYS_COMMIT clone at /vse.
COPY --from=evaluator /src/sdk/vibesys-evaluator/loadgen/ /vse/sdk/vibesys-evaluator/loadgen/
COPY evaluator/ /build/evaluator/
RUN cd /build/evaluator \
 && go build -o /out/ ./cmd/hotel-correctness ./cmd/podctl \
 && go build -o /out/hotel-generator ./cmd/generator

FROM moby/buildkit:v0.22.0@sha256:832fa7aa1eb3deb56fa62ae933bfa42dad9a83ff4824dbbaf173b49c722b59d0 AS buildkit

FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
COPY --from=evaluator /src/sdk/vibesys-evaluator/python/ /tmp/vibesys-evaluator/
RUN pip install --no-cache-dir /tmp/vibesys-evaluator pymongo==4.10.1 && rm -rf /tmp/vibesys-evaluator
COPY --from=evaluator /out/loadgen /out/otelcapture /out/kubectl /usr/local/bin/
COPY --from=oracle /out/hotel-generator /out/hotel-correctness /out/podctl /usr/local/bin/
COPY --from=buildkit /usr/bin/buildctl /usr/local/bin/buildctl
# A job runs as the invoking user, whose HOME may not exist: kubectl and buildctl keep
# their caches under /tmp.
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp KUBECACHEDIR=/tmp/.kube/cache
COPY . /task
# bench.py writes the composed manifest (k8s/deployed.yaml) next to itself before the
# package reads it; vibesys-cli runs the benchmark container as the host user, not root.
RUN chmod 1777 /task/k8s
