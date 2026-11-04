package hotel

// operationState carries a plan's expected answer to ValidateOperation. Every
// expected answer is computed from the task's dataset through
// hotelsupport.Reference when the plan is built.
type operationState struct {
	kind        string
	expectedIDs []string
	release     func()
}
