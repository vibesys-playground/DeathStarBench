module vibesys/social-network-k8s-evaluator

go 1.25.0

require (
	// The oracle's checker (cmd/oracle): a pseudo-version of vibesys main (cdb35bd1f, PR 3961), unreleased;
	// the release tag replaces it. Its protobuf files clash with loadgen's: no binary links both.
	github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go v0.0.0-20261106083534-cdb35bd1f4f6
	github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen v0.2.0-rc.1
	go.mongodb.org/mongo-driver v1.12.2
	google.golang.org/protobuf v1.36.12
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.9-20250912141014-52f32327d4b0.1 // indirect
	github.com/anishathalye/porcupine v1.3.1 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/klauspost/compress v1.13.6 // indirect
	github.com/montanaflynn/stats v0.0.0-20171201202039-1bf9dbcd8cbe // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	golang.org/x/crypto v0.0.0-20220622213112-05595931fe9d // indirect
	golang.org/x/sync v0.15.0 // indirect
	golang.org/x/text v0.26.0 // indirect
)
