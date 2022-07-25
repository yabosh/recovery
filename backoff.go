package recovery

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/yabosh/logger"
)

// BackoffS will pause (sleep) for a period of time determined by an exponential backoff algorithm.
// attempt is the number of unsuccessful attempts to perform a task that have occurred.  The
// algorithm uses the number of attempts to determine the length of time to pause.
func BackoffS(attempt int) {
	time.Sleep(time.Duration(GetNextBackOffMilliseconds(attempt)) * time.Millisecond)
}

// Backoff will pause the current goroutine for a period of time
// using an exponential backoff algorithm.
func Backoff(attempts int, jitterMS int, maxMS int) {
	backoff := ExponentialBackoffMS(attempts, jitterMS, maxMS)
	time.Sleep(time.Duration(backoff) * time.Millisecond)
}

// ExponentialBackoffMS returns the number of milliseconds to wait before
// retrying an operation using an exponential formula.
//
// attempts is the number of times in a row an operation has been attempted and failed.
// jitterMS is the number of milliseconds of 'jitter' (randomness) to inject into the formula
// maxMS is the maximum time returned (in milliseconds)
func ExponentialBackoffMS(attempts int, jitterMS int, maxMS int) int {

	randomMs := float64(rand.Intn(jitterMS))
	return int(math.Min((math.Pow(2, float64(attempts))*1000 + randomMs), float64(maxMS)))

}

// GetNextBackOffMilliseconds calculates an exponential value used for 'exponential backoff' scenarios.
func GetNextBackOffMilliseconds(attempts int) int {
	return ExponentialBackoffMS(attempts, 5000, 64000)
}

func FailOnError(err error, msg string, a ...interface{}) {
	if err != nil {
		fmtString := fmt.Sprintf("%s: %s", msg, err)
		logger.Error(fmtString, a)
		os.Exit(10)
	}
}
