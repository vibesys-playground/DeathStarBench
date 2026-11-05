module vibesys/hotel-k8s-evaluator

go 1.25.0

require (
	github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen v0.0.0
	vibesys/microservice-evaluator v0.0.0
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.9-20250912141014-52f32327d4b0.1 // indirect
	github.com/BurntSushi/toml v1.4.0 // indirect
	github.com/uw-syfi/vibesys/sdk/vs-evaluator/vseval v0.2.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

// The Dockerfile clones the vibesys repository at the pinned commit to /src and
// builds against its sources, so the build needs no published module version.
replace vibesys/microservice-evaluator => /src/resources/evaluators/microservice

replace github.com/uw-syfi/vibesys/sdk/vs-evaluator/vseval => /src/sdk/vs-evaluator/vseval

// The vibesys-evaluator Go SDK (the generator's), from the clone of the vibesys commit whose
// loadgen the task image holds (the Dockerfile's VIBESYS_COMMIT): one pin for both.
replace github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen => /vse/sdk/vibesys-evaluator/loadgen
