package hotel

import (
	"time"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// operationState carries a plan's expected answer to ValidateOperation. Every
// expected answer is computed from the task's dataset through
// hotelsupport.Reference and the writes this load generator made
// (writeModel).
type operationState struct {
	kind        string
	expectedIDs []string
	release     func()
	// built is when the plan was built, no later than its first request was
	// sent; reads validate against the values allowed between it and the
	// validation.
	built time.Time
	// messages are the expected message of each step of a write.
	messages []string
	// finish records a write's outcome in the model (once).
	finish func(acked time.Time, ok bool)
	// hotel is the hotel whose rates hotel_rates reads.
	hotel *dataset.Hotel
	// sandbox is the trial whose sandbox a search or distance
	// recommendation queries (nil for the cities).
	sandbox *fixture
}
