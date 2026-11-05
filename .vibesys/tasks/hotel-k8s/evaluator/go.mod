module vibesys/hotel-k8s-evaluator

go 1.25.0

require (
	github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen v0.0.0
	go.mongodb.org/mongo-driver v1.16.0
	google.golang.org/protobuf v1.36.12
	vibesys/microservice-evaluator v0.0.0
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.9-20250912141014-52f32327d4b0.1 // indirect
	github.com/BurntSushi/toml v1.4.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/klauspost/compress v1.13.6 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/uw-syfi/vibesys/sdk/vs-evaluator/vseval v0.2.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	golang.org/x/crypto v0.22.0 // indirect
	golang.org/x/sync v0.15.0 // indirect
	golang.org/x/text v0.26.0 // indirect
)

// The Dockerfile clones the vibesys repository at the pinned commit to /src and
// builds against its sources, so the build needs no published module version.
replace vibesys/microservice-evaluator => /src/resources/evaluators/microservice

replace github.com/uw-syfi/vibesys/sdk/vs-evaluator/vseval => /src/sdk/vs-evaluator/vseval

// The vibesys-evaluator Go SDK (the generator's), from the clone of the vibesys commit whose
// loadgen the task image holds (the Dockerfile's VIBESYS_COMMIT): one pin for both.
replace github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen => /vse/sdk/vibesys-evaluator/loadgen
