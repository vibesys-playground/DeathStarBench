package hotel

import "vibesys/hotel-k8s-evaluator/internal/dataset"

// User returns seeded account index (modulo the number of users) of the
// task's dataset: its username and plaintext password.
func User(index int) (username string, password string) {
	return DefaultReference().User(index)
}

// UserCount is the number of seeded accounts.
func UserCount() int { return len(dataset.Default().Users) }

// Capacity returns the seeded room capacity of one hotel.
func Capacity(hotelID string) (int, error) {
	return DefaultReference().Capacity(hotelID)
}
