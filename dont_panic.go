package recovery

/*
  Don't Panic provides a set of routines that can be useful to create fault-tolerant go-routines
*/

import (
	"fmt"
	"runtime/debug"
	"time"

	"github.com/yabosh/logger"
)

// Restartable is a function that can be used in conjunction with WithRestart() that
// will be restarted if it terminates with an error of any sort.  If the function
// terminates without an error then it will not be restarted
type Restartable func() error

// DontPanic wraps a function and traps any panic conditions that arise. DontPanic
// is intended to be used for goroutines that should run without failure.
//
// Sample usage
// 	err := DontPanic(func() {
//		panic("FAILURE")
//	})
//
// This will trap the "FAILURE" panic and return it as a standard err.  It also
// prints the stack trace to assist in debugging the panic.
//
// opName is a string value that is logged if a panic occurs to help identify
// the goroutine affected.
func DontPanic(opName string, f Restartable) (err error) {
	defer func() {
		if panicErr := recover(); panicErr != nil {
			logger.Error("PANIC: OPNAME=%s ERR=%#v", opName, panicErr)
			err = fmt.Errorf("%#v", panicErr)
			debug.PrintStack()
		}
	}()

	return f()
}

// WithRestart is a failsafe mechanism used to ensure that long running tasks do not terminate
// prematurely.  In the event of a panic the error is trapped and logged and then the goroutine function is restarted.
// If the function returns an error then it will be restarted.  If the function causes a panic then it will be restarted
// If the function does not return an error then it will be allowed to terminate normally.
// Sample usage:
//
// // Create a long-running goroutine that should not terminate
// go WithRestart("mytask", func() {
//    for {
// 	    select {
//	    case work := <- workQueue:
//          // Process 'work'
//      }
//    }
// })
//
// If a panic while processing 'work' causes this routine to fail
// WithRestart() will log the panic and stack trace and then restart the
// function.
//
// f() is expected to be long-running but assume that the code processing 'work'
// fails immediately.  This would essentially put WithRestart into an infinite
// loop and spam the log with stack traces. In order to prevent this an exponential
// backoff is used to restart the job if it has run for less than 60 seconds.
//
// Since f() is expected to be a long running function then any instance
// that runs less than 10 seconds will be subject to the backoff function
func WithRestart(opName string, f Restartable) {
	var attempt int
	const jitter = 100
	const maxBackoff = 64000
	const minFunctionRuntimeSecs = 60

	for {
		start := time.Now()
		err := DontPanic(opName, f)

		if err == nil {
			break
		}

		if time.Since(start) < time.Duration(minFunctionRuntimeSecs)*time.Second {
			// Only backoff if f() terminates very quickly
			Backoff(attempt, jitter, maxBackoff)
			attempt++
		} else {
			// f() ran longer than the threshold so don't use any backoff
			// if it fails and must be restarted.
			attempt = 0
		}
		logger.Warn("Restarting service %s", opName)
	}
}

// Retry a function until it completes without returning an error.  This is useful when
// an application relies on external services to be available on startup.
func UntilSuccessful(opName string, f func() error) {
	var attempt int
	const jitter = 100
	const maxBackoff = 64000

	for {
		err := DontPanic(opName, f)

		if err == nil {
			break
		}

		logger.Warn("Operation %s failed.  The operation will be retried.", opName)

		Backoff(attempt, jitter, maxBackoff)
		attempt++

		logger.Warn("Retrying operation %s", opName)
	}
}
