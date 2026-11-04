# Image every social-network-k8s job runs in. Built with the task directory as context.
#
# Holds kubectl, buildctl, Python 3 and three prebuilt binaries made from the
# task's evaluator/ module: loadgen (the load generator), seed (writes the task's
# dataset into the data tier) and checker (the accuracy check). run.py bakes
# loadgen and seed into the in-cluster evaluator image. The module builds against
# the generic servicebench engine from a clone of the vibesys repository at a
# pinned commit (evaluator/go.mod replaces it by the clone at /src).

ARG VIBESYS_COMMIT=de037943cb6a8e6e73d00e8bc19fa53570558583

FROM golang:1.24-bookworm@sha256:1a6d4452c65dea36aac2e2d606b01b4a029ec90cc1ae53890540ce6173ea77ac AS build
ARG VIBESYS_COMMIT
RUN git init -q /src \
 && git -C /src fetch -q --depth 1 https://github.com/uw-syfi/vibesys.git "${VIBESYS_COMMIT}" \
 && git -C /src checkout -q FETCH_HEAD \
 && test "$(git -C /src rev-parse HEAD)" = "${VIBESYS_COMMIT}"
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY evaluator/ /build/evaluator/
RUN cd /build/evaluator \
 && go build -o /out/loadgen ./cmd/loadgen \
 && go build -o /out/seed ./cmd/seed \
 && go build -o /out/checker ./cmd/checker

FROM moby/buildkit:v0.22.0@sha256:832fa7aa1eb3deb56fa62ae933bfa42dad9a83ff4824dbbaf173b49c722b59d0 AS buildkit

FROM golang:1.24-bookworm@sha256:1a6d4452c65dea36aac2e2d606b01b4a029ec90cc1ae53890540ce6173ea77ac
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
# kubectl v1.34.0 (matches the kind node), checked against its published sha256.
RUN curl -fsSL -o /usr/local/bin/kubectl https://dl.k8s.io/release/v1.34.0/bin/linux/amd64/kubectl \
 && echo "cfda68cba5848bc3b6c6135ae2f20ba2c78de20059f68789c090166d6abc3e2c  /usr/local/bin/kubectl" | sha256sum -c - \
 && chmod 0755 /usr/local/bin/kubectl
COPY --from=buildkit /usr/bin/buildctl /usr/local/bin/buildctl
COPY --from=build /out/ /opt/vibesys/bin/
ENV PATH=/opt/vibesys/bin:$PATH PYTHONDONTWRITEBYTECODE=1
