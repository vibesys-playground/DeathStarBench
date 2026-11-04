# Image every hotel-k8s job runs in. Built with the task directory as context.
#
# Holds kubectl, buildctl, Go, Python 3, and two prebuilt binaries made from a
# clone of the vibesys repository at a pinned commit: servicebench (the load
# generator; also baked into the in-cluster load-generator image) and
# hotel-correctness (the Go semantic oracle in oracle/).

ARG VIBESYS_COMMIT=de037943cb6a8e6e73d00e8bc19fa53570558583

FROM golang:1.24-bookworm@sha256:1a6d4452c65dea36aac2e2d606b01b4a029ec90cc1ae53890540ce6173ea77ac AS build
ARG VIBESYS_COMMIT
RUN git init -q /src \
 && git -C /src fetch -q --depth 1 https://github.com/uw-syfi/vibesys.git "${VIBESYS_COMMIT}" \
 && git -C /src checkout -q FETCH_HEAD \
 && test "$(git -C /src rev-parse HEAD)" = "${VIBESYS_COMMIT}"
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
# The evaluator module requires vseval v0.2.0; build against the pinned clone's copy.
RUN cd /src/resources/evaluators/microservice \
 && go mod edit -replace=github.com/uw-syfi/vibesys/sdk/vs-evaluator/vseval=/src/sdk/vs-evaluator/vseval \
 && go build -o /out/servicebench ./cmd/servicebench
COPY oracle/ /build/oracle/
RUN cd /build/oracle && go build -o /out/hotel-correctness ./cmd/hotel-correctness

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
COPY --from=build /out/servicebench /out/hotel-correctness /opt/vibesys/bin/
# See image/bwrap: the oracle's lifecycle needs a bwrap; a job container cannot run the real one.
COPY --chmod=0755 image/bwrap /opt/vibesys/bin/bwrap
ENV PATH=/opt/vibesys/bin:$PATH PYTHONDONTWRITEBYTECODE=1
