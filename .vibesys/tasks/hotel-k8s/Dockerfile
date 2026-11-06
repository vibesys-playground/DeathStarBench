# The hotel-k8s task image: every job of the task runs in it, and so do its pods in the
# cluster (the benchmark's seed, probe and phase pods; the check's seed, ready and oracle
# Jobs), which run this image rebuilt in-cluster from the task directory.
#
# It holds vibesys-evaluator (the Python package, loadgen, otelcapture), installed from the
# public vibesys repository at EVALUATOR_REF (the evaluator's own image is private; a
# released task writes the release here instead), kubectl and buildctl, and the task's Go
# programs from its evaluator/ module: hotel-generator (the benchmark's request generator
# and checks, on the vibesys-evaluator Go SDK that evaluator/go.mod requires), hotel-correctness
# (the accuracy check's semantic oracle), podctl (pod restarts and readiness) and hotel-data
# (the data snapshot's builder and checker). The oracle builds against the servicebench
# accuracy framework of an older vibesys commit (ORACLE_VIBESYS_COMMIT).
#
# The `data` stage is the data snapshot's image (snapshot.py builds it with
# `--opt target=data`): the data tier's own mongod, which writes the snapshot's files, and
# hotel-data.

# The one place the vibesys-evaluator version is pinned in this file: a vibesys commit (or,
# for a release, its tag). evaluator/go.mod requires the same version of the Go SDK, as a
# pseudo-version (`go list -m <module>@<ref>` prints it); change both together.
ARG EVALUATOR_REF=5b13578e0d29daf7b2eb3c5f9a888a36863e476b
ARG ORACLE_VIBESYS_COMMIT=de037943cb6a8e6e73d00e8bc19fa53570558583
ARG KUBECTL_VERSION=v1.34.0
ARG KUBECTL_SHA256=cfda68cba5848bc3b6c6135ae2f20ba2c78de20059f68789c090166d6abc3e2c

FROM golang:1.25.9-bookworm@sha256:298734aec230b5f3e8cee450ce6d7eccc39f1797ba548ee90d57e9803030c6c3 AS evaluator
ARG EVALUATOR_REF
ARG KUBECTL_VERSION
ARG KUBECTL_SHA256
ENV CGO_ENABLED=0 GOFLAGS=-trimpath GOBIN=/out
RUN go install \
      "github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/cmd/loadgen@${EVALUATOR_REF}" \
      "github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/cmd/otelcapture@${EVALUATOR_REF}"
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
# evaluator/go.mod replaces the servicebench modules by the clone at /src; the Go SDK
# (loadgen/workload) is the module it requires.
COPY evaluator/ /build/evaluator/
RUN cd /build/evaluator \
 && go build -o /out/ ./cmd/hotel-correctness ./cmd/podctl ./cmd/hotel-data \
 && go build -o /out/hotel-generator ./cmd/generator

# The data tier's MongoDB image (k8s/manifest.yaml, evaluator/internal/snapshot.MongoImage).
FROM mongo:5.0@sha256:41108d183e972dcbf98d09ed83f6cfc89a471a3f15d06f9d64a95e45d9db8dd2 AS data
COPY --from=oracle /out/hotel-data /usr/local/bin/

FROM moby/buildkit:v0.22.0@sha256:832fa7aa1eb3deb56fa62ae933bfa42dad9a83ff4824dbbaf173b49c722b59d0 AS buildkit

FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
# From the repository's source archive, not `git+https://`: pip would clone the repository's
# submodules too, some of them private.
ARG EVALUATOR_REF
RUN pip install --no-cache-dir \
      "vibesys-evaluator @ https://github.com/uw-syfi/vibesys/archive/${EVALUATOR_REF}.tar.gz#subdirectory=sdk/vibesys-evaluator/python"
COPY --from=evaluator /out/loadgen /out/otelcapture /out/kubectl /usr/local/bin/
COPY --from=oracle /out/hotel-generator /out/hotel-correctness /out/podctl /out/hotel-data /usr/local/bin/
COPY --from=buildkit /usr/bin/buildctl /usr/local/bin/buildctl
# A job runs as the invoking user, whose HOME may not exist: kubectl and buildctl keep
# their caches under /tmp.
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp KUBECACHEDIR=/tmp/.kube/cache
COPY . /task
# bench.py writes the composed manifest (k8s/deployed.yaml) next to itself before the
# package reads it; vibesys-cli runs the benchmark container as the host user, not root.
RUN chmod 1777 /task/k8s
