# The hotel-k8s task image: every job of the task runs in it, and so do its pods in the cluster
# (the seed, probe and phase pods of a benchmark sample or an accuracy check), which run this
# image rebuilt in-cluster from the task directory.
#
# It holds vibesys-evaluator (the Python package from PyPI, with its bundled core, loadgen and
# otelcapture, and PyYAML), kubectl and buildctl, and the task's Go programs from its evaluator/
# module, built against the vibesys-evaluator Go module that evaluator/go.mod requires:
# hotel-generator (the benchmark's request generator), hotel-accuracy-generator (the accuracy
# check's), hotel-oracle (the accuracy check's oracle) and hotel-data (the data snapshot's
# builder and checker).
#
# The `data` stage is the data snapshot's image (snapshot.py builds it with
# `--opt target=data`): the data tier's own mongod, which writes the snapshot's files, and
# hotel-data.

# The one place the vibesys-evaluator release is pinned in this file: the PyPI version of the
# package (0.3.0rc2; its platform wheel bundles the core, loadgen and otelcapture). evaluator/go.mod
# requires the Go SDK at the same release, v0.3.0-rc.2 (tests/test_accuracy.py checks they
# agree). The evaluator's own image (ghcr.io/uw-syfi/vibesys-evaluator-microservices) holds the
# same; it is not the base here because it is private, and the in-cluster rebuild of this image
# pulls without credentials.
ARG VIBESYS_EVALUATOR_VERSION=0.3.0rc2
ARG KUBECTL_VERSION=v1.34.0
ARG KUBECTL_SHA256=cfda68cba5848bc3b6c6135ae2f20ba2c78de20059f68789c090166d6abc3e2c

FROM golang:1.25.9-bookworm@sha256:298734aec230b5f3e8cee450ce6d7eccc39f1797ba548ee90d57e9803030c6c3 AS evaluator
ARG KUBECTL_VERSION
ARG KUBECTL_SHA256
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
RUN mkdir /out && curl -fsSL -o /out/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
 && echo "${KUBECTL_SHA256}  /out/kubectl" | sha256sum -c - \
 && chmod 0755 /out/kubectl
COPY evaluator/go.mod evaluator/go.sum /build/evaluator/
RUN cd /build/evaluator && go mod download
COPY evaluator/ /build/evaluator/
RUN cd /build/evaluator \
 && go build -o /out/hotel-data ./cmd/hotel-data \
 && go build -o /out/hotel-generator ./cmd/generator \
 && go build -o /out/hotel-accuracy-generator ./cmd/accuracy-generator \
 && go build -o /out/hotel-oracle ./cmd/oracle

# The data tier's MongoDB image (k8s/manifest.yaml, evaluator/internal/snapshot.MongoImage).
FROM mongo:5.0@sha256:41108d183e972dcbf98d09ed83f6cfc89a471a3f15d06f9d64a95e45d9db8dd2 AS data
COPY --from=evaluator /out/hotel-data /usr/local/bin/

FROM moby/buildkit:v0.22.0@sha256:832fa7aa1eb3deb56fa62ae933bfa42dad9a83ff4824dbbaf173b49c722b59d0 AS buildkit

FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
ARG VIBESYS_EVALUATOR_VERSION
RUN pip install --no-cache-dir "vibesys-evaluator==${VIBESYS_EVALUATOR_VERSION}"
COPY --from=buildkit /usr/bin/buildctl /usr/local/bin/buildctl
# A job runs as the invoking user, whose HOME may not exist: kubectl and buildctl keep
# their caches under /tmp.
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp KUBECACHEDIR=/tmp/.kube/cache
COPY --from=evaluator /out/kubectl /out/hotel-generator /out/hotel-accuracy-generator /out/hotel-oracle /out/hotel-data /usr/local/bin/
COPY . /task
# bench.py writes the composed manifest (k8s/deployed.yaml) next to itself before the package
# reads it; vibesys-cli runs the job's container as the host user, not root.
RUN chmod 1777 /task/k8s
