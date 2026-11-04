module vibesys/hotel-k8s-evaluator

go 1.21

require vibesys/microservice-evaluator v0.0.0

require (
	github.com/BurntSushi/toml v1.4.0 // indirect
	github.com/uw-syfi/vibesys/sdk/vs-evaluator/vseval v0.2.0 // indirect
)

// The Dockerfile clones the vibesys repository at the pinned commit to /src and
// builds against its sources, so the build needs no published module version.
replace vibesys/microservice-evaluator => /src/resources/evaluators/microservice

replace github.com/uw-syfi/vibesys/sdk/vs-evaluator/vseval => /src/sdk/vs-evaluator/vseval
