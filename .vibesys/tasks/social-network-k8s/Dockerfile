# The social-network-k8s task image: every job of the task runs in it, and so do its pods in
# the cluster (the seed, probe and phase pods of a benchmark sample or an accuracy check), which
# run this image rebuilt in-cluster from the task directory.
#
# It holds vibesys-evaluator (the Python package, whose platform wheel bundles the core,
# loadgen and otelcapture), kubectl and buildctl, and the task's Go programs from its evaluator/
# module, built against the vibesys-evaluator Go module that evaluator/go.mod requires:
# sn-generator (the benchmark's request generator), sn-accuracy-generator (the accuracy
# check's), sn-oracle (the accuracy check's oracle) and seed (writes the task's dataset into the
# data tier: the seed Job runs it from this image).

# The one place the vibesys-evaluator release is pinned in this file (the Go module's tag; the
# Python package's PyPI version is derived from it: v0.3.0-rc.1 is 0.3.0rc1). evaluator/go.mod
# requires the same Go version; change both together.
ARG VIBESYS_EVALUATOR_VERSION=v0.3.0-rc.1
ARG KUBECTL_VERSION=v1.34.0
ARG KUBECTL_SHA256=cfda68cba5848bc3b6c6135ae2f20ba2c78de20059f68789c090166d6abc3e2c

FROM golang:1.25.9-bookworm@sha256:298734aec230b5f3e8cee450ce6d7eccc39f1797ba548ee90d57e9803030c6c3 AS evaluator
ARG KUBECTL_VERSION
ARG KUBECTL_SHA256
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
RUN mkdir /out && curl -fsSL -o /out/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
 && echo "${KUBECTL_SHA256}  /out/kubectl" | sha256sum -c - \
 && chmod 0755 /out/kubectl
COPY evaluator/ /build/evaluator/
RUN cd /build/evaluator \
 && go build -o /out/seed ./cmd/seed \
 && go build -o /out/sn-generator ./cmd/generator \
 && go build -o /out/sn-accuracy-generator ./cmd/accuracy-generator \
 && go build -o /out/sn-oracle ./cmd/oracle

FROM moby/buildkit:v0.22.0@sha256:832fa7aa1eb3deb56fa62ae933bfa42dad9a83ff4824dbbaf173b49c722b59d0 AS buildkit

FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
ARG VIBESYS_EVALUATOR_VERSION
RUN version="${VIBESYS_EVALUATOR_VERSION#v}" \
 && pip install --no-cache-dir "vibesys-evaluator==$(echo "$version" | sed 's/-rc\./rc/')"
# The wheel bundles loadgen and otelcapture next to the core (vibesys_evaluator/_bin); the
# readiness probe pods run `loadgen` from PATH.
RUN bin="$(python -c 'import pathlib, vibesys_evaluator; print(pathlib.Path(vibesys_evaluator.__file__).parent / "_bin")')" \
 && ln -s "$bin/loadgen" "$bin/otelcapture" /usr/local/bin/ \
 && loadgen --capabilities > /dev/null
COPY --from=evaluator /out/kubectl /out/seed /out/sn-generator /out/sn-accuracy-generator /out/sn-oracle /usr/local/bin/
COPY --from=buildkit /usr/bin/buildctl /usr/local/bin/buildctl
# A job runs as the invoking user, whose HOME may not exist: kubectl and buildctl keep
# their caches under /tmp. Nothing writes into /task.
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp KUBECACHEDIR=/tmp/.kube/cache
COPY . /task
