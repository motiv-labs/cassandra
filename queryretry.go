package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
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

//todo add worker pool
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
		queryUUID := uuid.New().String()

		startTime := time.Now().UnixNano()
		queryExecuted := false
		workerPool.Submit(ctx, func() {
			startTime = time.Now().UnixNano()
			err = q.goCqlQuery.Exec()
			log.Infof(impulseCtx, "queryUUID: %s`timestamp: %d`startTime: %d`query: %s`messageToGrep: before exec", queryUUID, time.Now().UnixNano(), startTime, q.goCqlQuery.Statement())
			queryExecuted = true
		})
		for !queryExecuted {
			// small sleep time is required for cpu burn
			//time.Sleep(1 * time.Millisecond)
		}

		if err != nil {
			log.Infof(impulseCtx, "queryUUID: %s`timestamp: %d`startTime: %d`query: %s`messageToGrep: error - %v", queryUUID, time.Now().UnixNano(), startTime, q.goCqlQuery.Statement(), err)
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
		queryUUID := uuid.New().String()
		startTime := time.Now().UnixNano()

		queryExecuted := false
		workerPool.Submit(ctx, func() {
			startTime = time.Now().UnixNano()
			err = q.goCqlQuery.Scan(dest...)
			log.Infof(impulseCtx, "queryUUID: %s`timestamp: %d`startTime %d`query: %s`messageToGrep: before exec", queryUUID, time.Now().UnixNano(), startTime, q.goCqlQuery.Statement())
			queryExecuted = true
		})
		for !queryExecuted {
			// small sleep time is required for cpu burn
			//time.Sleep(1 * time.Millisecond)
		}

		if err != nil {
			log.Infof(impulseCtx, "queryUUID: %s`timestamp: %d`startTime %d`query: %s`messageToGrep: error - %v", queryUUID, time.Now().UnixNano(), startTime, q.goCqlQuery.Statement(), err)
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

	startTime := time.Now().UnixNano()
	queryExecuted := false
	var returnValue bool
	workerPool.Submit(ctx, func() {
		startTime = time.Now().UnixNano()
		returnValue = i.goCqlIter.Scan(dest...)
		log.Infof(impulseCtx, "queryUUID: %s`timestamp: %d`startTime: %d`query: %v`messageToGrep: before exec", "", time.Now().UnixNano(), startTime, i.goCqlIter.Columns())
		queryExecuted = true
	})
	for !queryExecuted {
		// small sleep time is required for cpu burn
		//time.Sleep(1 * time.Millisecond)
	}

	return returnValue
}

// WillSwitchPage is just a wrapper to be able to call this method
func (i iterRetry) WillSwitchPage(ctx context.Context) bool {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running iterRetry Close() method")

	queryExecuted := false
	var returnValue bool
	workerPool.Submit(ctx, func() {
		returnValue = i.goCqlIter.WillSwitchPage()
		queryExecuted = true
	})
	for !queryExecuted {
		// small sleep time is required for cpu burn
		//time.Sleep(1 * time.Millisecond)
	}

	return returnValue
}

// PageState is just a wrapper to be able to call this method
func (i iterRetry) PageState(ctx context.Context) []byte {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running iterRetry PageState() method")

	queryExecuted := false
	var returnValue []byte
	workerPool.Submit(ctx, func() {
		returnValue = i.goCqlIter.PageState()
		queryExecuted = true
	})
	for !queryExecuted {
		// small sleep time is required for cpu burn
		//time.Sleep(1 * time.Millisecond)
	}

	return returnValue
}

// MapScan is just a wrapper to be able to call this method
func (i iterRetry) MapScan(m map[string]interface{}, ctx context.Context) bool {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running iterRetry PageState() method")

	queryExecuted := false
	var returnValue bool
	workerPool.Submit(ctx, func() {
		returnValue = i.goCqlIter.MapScan(m)
		queryExecuted = true
	})
	for !queryExecuted {
		// small sleep time is required for cpu burn
		//time.Sleep(1 * time.Millisecond)
	}

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
		queryUUID := uuid.New().String()
		startTime := time.Now().UnixNano()

		queryExecuted := false
		workerPool.Submit(ctx, func() {
			startTime = time.Now().UnixNano()
			sliceMap, err = i.goCqlIter.SliceMap()
			log.Infof(impulseCtx, "queryUUID: %s`timestamp: %d`startTime: %d`query: %v`messageToGrep: before exec", queryUUID, time.Now().UnixNano(), startTime, i.goCqlIter.Columns())
			queryExecuted = true
		})
		for !queryExecuted {
			// small sleep time is required for cpu burn
			//time.Sleep(1 * time.Millisecond)
		}

		// we will try to run the method several times until attempts is met
		if err = i.goCqlIter.Close(); err != nil {
			log.Infof(impulseCtx, "queryUUID: %s`timestamp: %d`startTime: %d`query: %s`messageToGrep: error - %v", queryUUID, time.Now().UnixNano(), startTime, i.goCqlIter.Columns(), err)
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

	startTime := time.Now().UnixNano()
	err := i.goCqlIter.Close()
	if err != nil {
		log.Infof(impulseCtx, "queryUUID: %s`timestamp: %d`startTime: %d`query: %s`messageToGrep: error - %v", "", time.Now().UnixNano(), startTime, i.goCqlIter.Columns(), err)
	}

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
		queryUUID := uuid.New().String()
		startTime := time.Now().UnixNano()
		//log.Infof(impulseCtx, "queryUUID: %s`timestamp: %d`query: %v`messageToGrep: before exec", queryUUID, time.Now().UnixNano(), i.goCqlIter.Columns())
		for i.Scan(ctx, dest...) {
			startTime = time.Now().UnixNano()
			if !handle() {
				break
			}
		}

		// we will try to run the method several times until attempts is met
		if err = i.goCqlIter.Close(); err != nil {
			log.Infof(impulseCtx, "queryUUID: %s`timestamp: %d`approxStartTime: %d`query: %s`messageToGrep: error - %v", queryUUID, time.Now().UnixNano(), startTime, i.goCqlIter.Columns(), err)
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
func (i iterRetry) MapScanAndClose(m map[string]interface{}, handle func(), ctx context.Context) error {
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
		queryUUID := uuid.New().String()
		startTime := time.Now().UnixNano()
		//log.Infof(impulseCtx, "queryUUID: %s`timestamp: %d`query: %v`messageToGrep: before exec", queryUUID, time.Now().UnixNano(), i.goCqlIter.Columns())
		for i.MapScan(m, ctx) {
			startTime = time.Now().UnixNano()
			handle()
		}

		// we will try to run the method several times until attempts is met
		if err = i.goCqlIter.Close(); err != nil {
			log.Infof(impulseCtx, "queryUUID: %s`timestamp: %d`approxStartTime: %d`query: %s`messageToGrep: error - %v", queryUUID, time.Now().UnixNano(), startTime, i.goCqlIter.Columns(), err)
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
