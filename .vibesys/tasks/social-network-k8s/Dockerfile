# The social-network-k8s task image: every job of the task runs in it, and so do the
# benchmark's pods in the cluster (its seed, probe and phase pods), which run this image
# rebuilt in-cluster from the task directory.
#
# It holds vibesys-evaluator (the Python package, loadgen, kubectl, buildctl), built here
# from a pinned vibesys commit as sdk/vibesys-evaluator/image/Dockerfile does until the
# image is published, and the task's Go programs from its evaluator/ module, built against
# the vibesys-evaluator Go SDK of the same commit as loadgen: sn-generator (the benchmark's
# request generator and checks), checker (the accuracy check, run by run.py) and seed
# (writes the task's dataset into the data tier: the benchmark's seed Job runs it from this
# image, the check's from a small image run.py builds around it).

ARG VIBESYS_COMMIT=505f38e556dabe86a2855708e5cde6081880b763
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

FROM golang:1.25.9-bookworm@sha256:298734aec230b5f3e8cee450ce6d7eccc39f1797ba548ee90d57e9803030c6c3 AS task
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
# evaluator/go.mod replaces the Go SDK by the VIBESYS_COMMIT clone at /vse.
COPY --from=evaluator /src/sdk/vibesys-evaluator/loadgen/ /vse/sdk/vibesys-evaluator/loadgen/
COPY evaluator/ /build/evaluator/
RUN cd /build/evaluator \
 && go build -o /out/ ./cmd/checker ./cmd/seed \
 && go build -o /out/sn-generator ./cmd/generator

FROM moby/buildkit:v0.22.0@sha256:832fa7aa1eb3deb56fa62ae933bfa42dad9a83ff4824dbbaf173b49c722b59d0 AS buildkit

FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
COPY --from=evaluator /src/sdk/vibesys-evaluator/python/ /tmp/vibesys-evaluator/
RUN pip install --no-cache-dir /tmp/vibesys-evaluator && rm -rf /tmp/vibesys-evaluator
COPY --from=evaluator /out/loadgen /out/otelcapture /out/kubectl /usr/local/bin/
COPY --from=task /out/sn-generator /out/checker /out/seed /usr/local/bin/
COPY --from=buildkit /usr/bin/buildctl /usr/local/bin/buildctl
# A job runs as the invoking user, whose HOME may not exist: kubectl and buildctl keep
# their caches under /tmp. Nothing writes into /task: run.py works in a temporary directory.
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp KUBECACHEDIR=/tmp/.kube/cache
COPY . /task
