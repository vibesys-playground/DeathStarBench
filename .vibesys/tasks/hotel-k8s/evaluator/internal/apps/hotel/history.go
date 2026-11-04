package hotel

import "time"

// StalenessBound is the task's visibility contract: a read that starts more
// than StalenessBound after a write was acknowledged must reflect it.
const StalenessBound = time.Second

// version is one value written to an item by this load generator.
type version[T any] struct {
	value T
	// built is when the write's plan was built, no later than the request
	// was sent; acked is when its response was validated, no earlier than
	// it arrived (zero while in flight); ok is an acknowledged write.
	built, acked time.Time
	ok           bool
}

// history is what this load generator wrote to one item (a profile, a rate
// plan, a capacity). Writes to one item are serialized (busy), so versions
// are in the order the application applied them.
type history[T any] struct {
	base     T
	versions []version[T]
	busy     bool
}

// allowed returns the values a read may observe whose plan was built at
// start and whose response was validated at end: the latest value
// acknowledged more than StalenessBound before start (the seeded value if
// none), and every value written after it whose request may have been sent
// before the response arrived. start is no later than the read was sent and
// end no earlier than its response, so the window only widens: the check
// never rejects an answer the contract allows.
func (h *history[T]) allowed(start, end time.Time) []T {
	required := -1
	for index, v := range h.versions {
		if v.ok && v.acked.Before(start.Add(-StalenessBound)) {
			required = index
		}
	}
	values := make([]T, 0, 2)
	if required < 0 {
		values = append(values, h.base)
	} else {
		values = append(values, h.versions[required].value)
	}
	for _, v := range h.versions[required+1:] {
		if v.built.Before(end) {
			values = append(values, v.value)
		}
	}
	return values
}

// latest is the value of the last write, acknowledged or not.
func (h *history[T]) latest() T {
	if len(h.versions) == 0 {
		return h.base
	}
	return h.versions[len(h.versions)-1].value
}

// begin records a write about to be sent and marks the item busy.
func (h *history[T]) begin(value T, built time.Time) {
	h.busy = true
	h.versions = append(h.versions, version[T]{value: value, built: built})
}

// finish records the outcome of the write in flight (the last version).
func (h *history[T]) finish(acked time.Time, ok bool) {
	if !h.busy {
		return
	}
	last := &h.versions[len(h.versions)-1]
	last.acked, last.ok = acked, ok
	h.busy = false
}
