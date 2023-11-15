package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	impulse_ctx "github.com/motiv-labs/impulse-ctx"
	log "github.com/motiv-labs/logwrapper"
	"github.com/motiv-labs/workerpool"
	"strconv"
	"time"
)

const (
	defaultCassandraRetryAttempts           = "3"
	defaultCassandraSecondsToSleepIncrement = "1"
	defaultWorkerPoolSize                   = "50"

	envCassandraAttempts                = "CASSANDRA_RETRY_ATTEMPTS"
	envCassandraSecondsToSleepIncrement = "CASSANDRA_SECONDS_SLEEP_INCREMENT"
	envWorkerPoolSize                   = "CASSANDRA_WORKER_POOL_SIZE"
)

var cassandraRetryAttempts = 3
var cassandraSecondsToSleepIncrement = 1
var workerPoolSize = 50

// default to 50
// allow each service to set the workerpool size individually
var workerPool *workerpool.WorkerPool

func initWorkerPool() {
	ictx := impulse_ctx.ImpulseCtx{}
	ctx := impulse_ctx.NewContext(context.Background(), "", nil, "")
	eWorkerPoolSize, err := strconv.Atoi(getenv(envWorkerPoolSize, defaultWorkerPoolSize, ictx))
	if err != nil {
		log.Errorf(ictx, "error trying to get %s value: %s",
			envWorkerPoolSize, getenv(envWorkerPoolSize, defaultWorkerPoolSize, ictx))
		eWorkerPoolSize = workerPoolSize
	}

	workerPool = workerpool.NewWorkerPool(ctx, eWorkerPoolSize)
}

// Package level initialization.
//
// init functions are automatically executed when the programs starts
func init() {
	ictx := impulse_ctx.ImpulseCtx{}
	cassandraRetryAttempts, err := strconv.Atoi(getenv(envCassandraAttempts, defaultCassandraRetryAttempts, ictx))
	if err != nil {
		log.Errorf(ictx, "error trying to get CASSANDRA_RETRY_ATTEMPTS value: %s",
			getenv(envCassandraAttempts, defaultCassandraRetryAttempts, ictx))
		cassandraRetryAttempts = 3
	}

	cassandraSecondsToSleep, err := strconv.Atoi(getenv(envCassandraSecondsToSleepIncrement, defaultCassandraSecondsToSleepIncrement, ictx))
	if err != nil {
		log.Errorf(ictx, "error trying to get CASSANDRA_SECONDS_SLEEP value: %s",
			getenv(envCassandraSecondsToSleepIncrement, defaultCassandraSecondsToSleepIncrement, ictx))
		cassandraSecondsToSleepIncrement = 1
	}

	initWorkerPool()

	log.Debugf(ictx, "got cassandraRetryAttempts: %d", cassandraRetryAttempts)
	log.Debugf(ictx, "got cassandraSecondsToSleepIncrement: %d", cassandraSecondsToSleep)
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
		queryExecuted := make(chan error)
		workerPool.Submit(ctx, func() {
			err = q.goCqlQuery.Exec()
			queryExecuted <- err
		})
		err = <-queryExecuted

		if err != nil {
			log.Warnf(impulseCtx, "error when running Exec(): %v, attempt: %d / %d, query: %s", err, attempts, retryAttempts, q.goCqlQuery.String())

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
		queryExecuted := make(chan bool)
		workerPool.Submit(ctx, func() {
			err = q.goCqlQuery.Scan(dest...)
			queryExecuted <- true
		})
		<-queryExecuted

		if err != nil {
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

func (i iterRetry) Scan(ctx context.Context, dest ...interface{}) bool {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running iterRetry Scan() method")

	queryExecuted := make(chan bool)
	var returnValue bool
	workerPool.Submit(ctx, func() {
		returnValue = i.goCqlIter.Scan(dest...)
		queryExecuted <- true
	})
	<-queryExecuted

	return returnValue
}

// WillSwitchPage is just a wrapper to be able to call this method
func (i iterRetry) WillSwitchPage(ctx context.Context) bool {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running iterRetry Close() method")

	queryExecuted := make(chan bool)
	var returnValue bool
	workerPool.Submit(ctx, func() {
		returnValue = i.goCqlIter.WillSwitchPage()
		queryExecuted <- true
	})
	<-queryExecuted

	return returnValue
}

// PageState is just a wrapper to be able to call this method
func (i iterRetry) PageState(ctx context.Context) []byte {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running iterRetry PageState() method")

	queryExecuted := make(chan bool)
	var returnValue []byte
	workerPool.Submit(ctx, func() {
		returnValue = i.goCqlIter.PageState()
		queryExecuted <- true
	})
	<-queryExecuted

	return returnValue
}

// MapScan is just a wrapper to be able to call this method
func (i iterRetry) MapScan(m map[string]interface{}, ctx context.Context) bool {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running iterRetry PageState() method")

	queryExecuted := make(chan bool)
	var returnValue bool
	workerPool.Submit(ctx, func() {
		returnValue = i.goCqlIter.MapScan(m)
		queryExecuted <- true
	})
	<-queryExecuted

	return returnValue
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
		queryExecuted := make(chan bool)
		workerPool.Submit(ctx, func() {
			sliceMap, err = i.goCqlIter.SliceMap()
			queryExecuted <- true
		})
		<-queryExecuted

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

	err := i.goCqlIter.Close()

	return err
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
		for i.Scan(ctx, dest...) {
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

// todo test this function
// MapScanAndClose is a wrapper to retry around the gocql MapScan() and Close().
// We have a retry approach in place + incremental approach used. For example:
// First time it will wait 1 second, second time 2 seconds, ... It will depend on the values for retries
// and seconds to wait.
func (i iterRetry) MapScanAndClose(m map[string]interface{}, handle func() bool, ctx context.Context) error {
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
		for i.MapScan(m, ctx) {
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
