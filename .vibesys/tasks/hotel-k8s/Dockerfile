# The hotel-k8s task image: every job of the task runs in it, and so do its pods in the cluster
# (the seed, probe and phase pods of a benchmark sample or an accuracy check), which run this
# image rebuilt in-cluster from the task directory.
#
# It is vibesys-evaluator's microservices image at the release 0.3.0rc2 (the one pin of the
# evaluator in this file, by tag and digest): Python with the vibesys_evaluator package 0.3.0rc2
# from PyPI, its bundled Go binaries (loadgen, otelcapture and the core's vibesys-evaluator) on
# the PATH, PyYAML, kubectl and buildctl. On it go the task's Go programs, built from its
# evaluator/ module, whose go.mod requires the Go SDK at the same release, v0.3.0-rc.2
# (tests/test_accuracy.py checks they agree): hotel-generator (the benchmark's request
# generator), hotel-accuracy-generator (the accuracy check's), hotel-oracle (the accuracy check's
# oracle) and hotel-data (the data snapshot's builder and checker).
#
# The `data` stage is the data snapshot's image (snapshot.py builds it with
# `--opt target=data`): the data tier's own mongod, which writes the snapshot's files, and
# hotel-data.

FROM golang:1.25.9-bookworm@sha256:298734aec230b5f3e8cee450ce6d7eccc39f1797ba548ee90d57e9803030c6c3 AS evaluator
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
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

FROM ghcr.io/uw-syfi/vibesys-evaluator-microservices:0.3.0rc2@sha256:64dc4a0dfe63507da08080c6e8925e9e7622d268bf200b71c817821f00f1140e
COPY --from=evaluator /out/hotel-generator /out/hotel-accuracy-generator /out/hotel-oracle /out/hotel-data /usr/local/bin/
COPY . /task
# bench.py writes the composed manifest (k8s/deployed.yaml) next to itself before the package
# reads it; vibesys-cli runs the job's container as the host user, not root.
RUN chmod 1777 /task/k8s
