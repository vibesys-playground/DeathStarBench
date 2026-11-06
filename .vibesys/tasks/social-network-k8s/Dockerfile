# The social-network-k8s task image: every job of the task runs in it, and so do the
# benchmark's pods in the cluster (its seed, probe and phase pods), which run this image
# rebuilt in-cluster from the task directory.
#
# It holds vibesys-evaluator (the Python package, loadgen, otelcapture), installed from the
# vibesys-evaluator release (PyPI and the public vibesys repository's module tags; the
# evaluator's own image is private), kubectl and buildctl, and the task's Go
# programs from its evaluator/ module, built against the vibesys-evaluator Go SDK that
# evaluator/go.mod requires: sn-generator (the benchmark's request generator and checks),
# checker (the accuracy check, run by run.py) and seed
# (writes the task's dataset into the data tier: the benchmark's seed Job runs it from this
# image, the check's from a small image run.py builds around it).

# The one place the vibesys-evaluator release is pinned in this file: the Python package's
# version (PyPI) and the Go SDK's tag (the same release, spelled as Go does). evaluator/go.mod
# requires the same Go version; change all three together.
ARG EVALUATOR_VERSION=0.2.0rc1
ARG EVALUATOR_GO_VERSION=v0.2.0-rc.1
ARG KUBECTL_VERSION=v1.34.0
ARG KUBECTL_SHA256=cfda68cba5848bc3b6c6135ae2f20ba2c78de20059f68789c090166d6abc3e2c

FROM golang:1.25.9-bookworm@sha256:298734aec230b5f3e8cee450ce6d7eccc39f1797ba548ee90d57e9803030c6c3 AS evaluator
ARG EVALUATOR_VERSION
ARG EVALUATOR_GO_VERSION
ARG KUBECTL_VERSION
ARG KUBECTL_SHA256
ENV CGO_ENABLED=0 GOFLAGS=-trimpath GOBIN=/out
RUN go install \
      "github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/cmd/loadgen@${EVALUATOR_GO_VERSION}" \
      "github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/cmd/otelcapture@${EVALUATOR_GO_VERSION}"
RUN curl -fsSL -o /out/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
 && echo "${KUBECTL_SHA256}  /out/kubectl" | sha256sum -c - \
 && chmod 0755 /out/kubectl

FROM golang:1.25.9-bookworm@sha256:298734aec230b5f3e8cee450ce6d7eccc39f1797ba548ee90d57e9803030c6c3 AS task
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY evaluator/ /build/evaluator/
RUN cd /build/evaluator \
 && go build -o /out/ ./cmd/checker ./cmd/seed \
 && go build -o /out/sn-generator ./cmd/generator

FROM moby/buildkit:v0.22.0@sha256:832fa7aa1eb3deb56fa62ae933bfa42dad9a83ff4824dbbaf173b49c722b59d0 AS buildkit

FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
ARG EVALUATOR_VERSION
RUN pip install --no-cache-dir "vibesys-evaluator==${EVALUATOR_VERSION}"
COPY --from=evaluator /out/loadgen /out/otelcapture /out/kubectl /usr/local/bin/
COPY --from=task /out/sn-generator /out/checker /out/seed /usr/local/bin/
COPY --from=buildkit /usr/bin/buildctl /usr/local/bin/buildctl
# A job runs as the invoking user, whose HOME may not exist: kubectl and buildctl keep
# their caches under /tmp. Nothing writes into /task: run.py works in a temporary directory.
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp KUBECACHEDIR=/tmp/.kube/cache
COPY . /task
