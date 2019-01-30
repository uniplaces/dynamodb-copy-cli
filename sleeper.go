package dynamodbcopy

import (
	"math/rand"
	"time"
)

const maxRandomFactor = 100

// Sleeper abstracts out sleep side effects to allow for better testing
// Sleeper receive the time it should sleep, returning the total time it spend sleeping
type Sleeper func(ms int) int

// RandomSleeper will sleep for the provided ms plust and aditional factor (ranging between [0, 100[)
// Returns the total sleep time
func RandomSleeper(ms int) int {
	elapsed := ms + rand.Intn(maxRandomFactor)

	time.Sleep(time.Duration(elapsed) * time.Millisecond)

	return elapsed
}
