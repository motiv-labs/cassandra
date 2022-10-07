package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	impulse_ctx "github.com/motiv-labs/impulse-ctx"
	log "github.com/motiv-labs/logwrapper"
	"strconv"
	"time"
)

const (
	defaultCassandraRetryAttempts           = "3"
	defaultCassandraSecondsToSleepIncrement = "1"

	envCassandraAttempts                = "CASSANDRA_RETRY_ATTEMPTS"
	envCassandraSecondsToSleepIncrement = "CASSANDRA_SECONDS_SLEEP_INCREMENT"
)

var cassandraRetryAttempts = 3
var cassandraSecondsToSleepIncrement = 1

// Package level initialization.
//
// init functions are automatically executed when the programs starts
func init() {
	ictx := impulse_ctx.ImpulseCtx{}
	var err error
	cassandraRetryAttempts, err = strconv.Atoi(getenv(envCassandraAttempts, defaultCassandraRetryAttempts, ictx))
	if err != nil {
		log.Errorf(ictx, "error trying to get CASSANDRA_RETRY_ATTEMPTS value: %s",
			getenv(envCassandraAttempts, defaultCassandraRetryAttempts, ictx))
		cassandraRetryAttempts = 3
	}

	cassandraSecondsToSleepIncrement, err = strconv.Atoi(getenv(envCassandraSecondsToSleepIncrement, defaultCassandraSecondsToSleepIncrement, ictx))
	if err != nil {
		log.Errorf(ictx, "error trying to get CASSANDRA_SECONDS_SLEEP value: %s",
			getenv(envCassandraSecondsToSleepIncrement, defaultCassandraSecondsToSleepIncrement, ictx))
		cassandraSecondsToSleepIncrement = 1
	}

	log.Debugf(ictx, "got cassandraRetryAttempts: %d", cassandraRetryAttempts)
	log.Debugf(ictx, "got cassandraSecondsToSleepIncrement: %d", cassandraSecondsToSleepIncrement)
}

// queryRetry is an implementation of QueryInterface
type queryRetry struct {
	goCqlQuery *gocql.Query
}

// iterRetry is an implementation of IterInterface
type iterRetry struct {
	goCqlIter *gocql.Iter
}

// Exec wrapper to retry around gocql Exec(). We have a retry approach in place + incremental approach used. For example:
// First time it will wait 1 second, second time 2 seconds, ... It will depend on the values for retries and seconds to wait.
func (q queryRetry) Exec(ctx context.Context) error {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running queryRetry Exec() method")

	retryAttempts := cassandraRetryAttempts
	secondsToSleep := 0

	var err error

	attempts := 1
	for attempts <= retryAttempts {
		//we will try to run the method several times until attempts is met
		err = q.goCqlQuery.Exec()
		if err != nil {
			log.Warnf(impulseCtx, "error when running Exec(): %v, attempt: %d / %d", err, attempts, retryAttempts)

			// incremental sleep
			secondsToSleep = secondsToSleep + cassandraSecondsToSleepIncrement

			log.Warnf(impulseCtx, "sleeping for %d second", secondsToSleep)

			time.Sleep(time.Duration(secondsToSleep) * time.Second)
		} else {
			// in case the error is nil, we stop and return
			return err
		}

		attempts = attempts + 1
	}

	return err
}

// Scan wrapper to retry around gocql Scan(). We have a retry approach in place + incremental approach used. For example:
// First time it will wait 1 second, second time 2 seconds, ... It will depend on the values for retries and seconds to wait.
func (q queryRetry) Scan(ctx context.Context, dest ...interface{}) error {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Error(impulseCtx, "ImpulseCtx isn't correct type")
		return impulse_ctx.NewInvalidImpulseCtx("ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running queryRetry Scan() method")

	retries := cassandraRetryAttempts
	secondsToSleep := 0

	var err error

	attempts := 1
	for attempts <= retries {
		//we will try to run the method several times until attempts is met
		err = q.goCqlQuery.Scan(dest...)
		if err != nil && err != gocql.ErrNotFound { // don't retry not found errors
			log.Warnf(impulseCtx, "error when running Scan(): %v, attempt: %d / %d", err, attempts, retries)

			// incremental sleep
			secondsToSleep = secondsToSleep + cassandraSecondsToSleepIncrement

			log.Warnf(impulseCtx, "sleeping for %d second", secondsToSleep)

			log.Warnf(impulseCtx, "sleeping for %d second", secondsToSleep)
			time.Sleep(time.Duration(secondsToSleep) * time.Second)
		} else {
			// in case the error is nil, we stop and return
			return err
		}

		attempts = attempts + 1
	}

	return err
}

// Iter just a wrapper to be able to call this method
func (q queryRetry) Iter(ctx context.Context) IterInterface {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running queryRetry Iter() method")

	return iterRetry{goCqlIter: q.goCqlQuery.Iter()}
}

// PageState just a wrapper to be able to call this method
func (q queryRetry) PageState(state []byte, ctx context.Context) QueryInterface {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running queryRetry PageState() method")

	return queryRetry{goCqlQuery: q.goCqlQuery.PageState(state)}
}

// PageSize just a wrapper to be able to call this method
func (q queryRetry) PageSize(n int, ctx context.Context) QueryInterface {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running queryRetry PageSize() method")

	return queryRetry{goCqlQuery: q.goCqlQuery.PageSize(n)}
}

//
func (i iterRetry) Scan(ctx context.Context, dest ...interface{}) bool {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running iterRetry Scan() method")

	return i.goCqlIter.Scan(dest...)
}

// WillSwitchPage is just a wrapper to be able to call this method
func (i iterRetry) WillSwitchPage(ctx context.Context) bool {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running iterRetry Close() method")

	return i.goCqlIter.WillSwitchPage()
}

// PageState is just a wrapper to be able to call this method
func (i iterRetry) PageState(ctx context.Context) []byte {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running iterRetry PageState() method")

	return i.goCqlIter.PageState()
}

// MapScan is just a wrapper to be able to call this method
func (i iterRetry) MapScan(m map[string]interface{}, ctx context.Context) bool {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running iterRetry PageState() method")

	return i.goCqlIter.MapScan(m)
}

/*
SliceMapAndClose runs gocql.Iter.SliceMap and gocql.Iter.Close
*/
func (i iterRetry) SliceMapAndClose(ctx context.Context) ([]map[string]interface{}, error) {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}
	ctx = context.WithValue(ctx, impulse_ctx.ImpulseCtxKey, impulseCtx)

	retries := cassandraRetryAttempts
	secondsToSleep := 0

	var sliceMap []map[string]interface{}
	var err error

	attempts := 1
	for attempts <= retries {

		// Scan consumes the next row of the iterator and copies the columns of the
		// current row into the values pointed at by dest.
		sliceMap, err = i.goCqlIter.SliceMap()

		// we will try to run the method several times until attempts is met
		if err = i.goCqlIter.Close(); err != nil {

			log.Warnf(impulseCtx, "error when running Close(): %v, attempt: %d / %d", err, attempts, retries)

			// incremental sleep
			secondsToSleep += cassandraSecondsToSleepIncrement

			log.Warnf(impulseCtx, "sleeping for %d second", secondsToSleep)

			time.Sleep(time.Duration(secondsToSleep) * time.Second)
		} else {
			// in case the error is nil, we stop and return
			return sliceMap, err
		}

		attempts++
	}

	return sliceMap, err
}

// Close is just a wrapper to be able to call this method
func (i iterRetry) Close(ctx context.Context) error {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Error(impulseCtx, "ImpulseCtx isn't correct type")
		return impulse_ctx.NewInvalidImpulseCtx("ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running iterRetry Close() method")

	return i.goCqlIter.Close()
}

// ScanAndClose is a wrapper to retry around the gocql Scan() and Close().
// We have a retry approach in place + incremental approach used. For example:
// First time it will wait 1 second, second time 2 seconds, ... It will depend on the values for retries
// and seconds to wait.
func (i iterRetry) ScanAndClose(ctx context.Context, handle func() bool, dest ...interface{}) error {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Error(impulseCtx, "ImpulseCtx isn't correct type")
		return impulse_ctx.NewInvalidImpulseCtx("ImpulseCtx isn't correct type")
	}

	retries := cassandraRetryAttempts
	secondsToSleep := 0

	var err error

	attempts := 1
	for attempts <= retries {

		// Scan consumes the next row of the iterator and copies the columns of the
		// current row into the values pointed at by dest.
		for i.goCqlIter.Scan(dest...) {
			if !handle() {
				break
			}
		}

		// we will try to run the method several times until attempts is met
		if err = i.goCqlIter.Close(); err != nil {

			log.Warnf(impulseCtx, "error when running Close(): %v, attempt: %d / %d", err, attempts, retries)

			// incremental sleep
			secondsToSleep += cassandraSecondsToSleepIncrement

			log.Warnf(impulseCtx, "sleeping for %d second", secondsToSleep)

			time.Sleep(time.Duration(secondsToSleep) * time.Second)
		} else {
			// in case the error is nil, we stop and return
			return err
		}

		attempts++
	}

	return err
}
