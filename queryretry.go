package cassandra

import (
	"github.com/gocql/gocql"
	log "github.com/motiv-labs/logwrapper"
	"github.com/opentracing/opentracing-go"
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
	cassandraRetryAttempts, err := strconv.Atoi(getenv(envCassandraAttempts, defaultCassandraRetryAttempts))
	if err != nil {
		log.Errorf("error trying to get CASSANDRA_RETRY_ATTEMPTS value: %s",
			getenv(envCassandraAttempts, defaultCassandraRetryAttempts))
		cassandraRetryAttempts = 3
	}

	cassandraSecondsToSleep, err := strconv.Atoi(getenv(envCassandraSecondsToSleepIncrement, defaultCassandraSecondsToSleepIncrement))
	if err != nil {
		log.Errorf("error trying to get CASSANDRA_SECONDS_SLEEP value: %s",
			getenv(envCassandraSecondsToSleepIncrement, defaultCassandraSecondsToSleepIncrement))
		cassandraSecondsToSleepIncrement = 1
	}

	log.Debugf("got cassandraRetryAttempts: %d", cassandraRetryAttempts)
	log.Debugf("got cassandraSecondsToSleepIncrement: %d", cassandraSecondsToSleep)
}

// queryRetry is an implementation of QueryInterface
type queryRetry struct {
	goCqlQuery *gocql.Query
	goCqlIter *gocql.Iter
}

// iterRetry is an implementation of IterInterface
type iterRetry struct {
	goCqlIter *gocql.Iter
}

// Exec wrapper to retry around gocql Exec(). We have a retry approach in place + incremental approach used. For example:
// First time it will wait 1 second, second time 2 seconds, ... It will depend on the values for retries and seconds to wait.
func (q queryRetry) Exec(parentSpan opentracing.Span) error {
	span := opentracing.StartSpan("Exec", opentracing.ChildOf(parentSpan.Context()))
	defer span.Finish()
	span.SetTag("Module", "cassandra")
	span.SetTag("Interface", "queryRetry")

	log.Debug("running queryRetry Exec() method")

	retryAttempts := cassandraRetryAttempts
	secondsToSleep := 0

	var err error

	attempts := 1
	for attempts <= retryAttempts {
		//we will try to run the method several times until attempts is met
		err = q.goCqlQuery.Exec()
		if err != nil {
			log.Warnf("error when running Exec(): %v, attempt: %d / %d", err, attempts, retryAttempts)

			// incremental sleep
			secondsToSleep = secondsToSleep + cassandraSecondsToSleepIncrement

			log.Warnf("sleeping for %d second", secondsToSleep)

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
func (q queryRetry) Scan(parentSpan opentracing.Span, dest ...interface{}) error {
	span := opentracing.StartSpan("Scan", opentracing.ChildOf(parentSpan.Context()))
	defer span.Finish()
	span.SetTag("Module", "cassandra")
	span.SetTag("Interface", "queryRetry")

	log.Debug("running queryRetry Scan() method")

	retries := cassandraRetryAttempts
	secondsToSleep := 0

	var err error

	attempts := 1
	for attempts <= retries {
		//we will try to run the method several times until attempts is met
		err = q.goCqlQuery.Scan(dest...)
		if err != nil {
			log.Warnf("error when running Scan(): %v, attempt: %d / %d", err, attempts, retries)

			// incremental sleep
			secondsToSleep = secondsToSleep + cassandraSecondsToSleepIncrement

			log.Warnf("sleeping for %d second", secondsToSleep)

			log.Warnf("sleeping for %d second", secondsToSleep)
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
func (q queryRetry) Iter(parentSpan opentracing.Span) QueryInterface {
	span := opentracing.StartSpan("Iter", opentracing.ChildOf(parentSpan.Context()))
	defer span.Finish()
	span.SetTag("Module", "cassandra")
	span.SetTag("Interface", "queryRetry")

	log.Debug("running queryRetry Iter() method")

	return queryRetry{goCqlIter: q.goCqlQuery.Iter()}
}

// PageState just a wrapper to be able to call this method
func (q queryRetry) PageState(state []byte, parentSpan opentracing.Span) QueryInterface {
	span := opentracing.StartSpan("PageState", opentracing.ChildOf(parentSpan.Context()))
	defer span.Finish()
	span.SetTag("Module", "cassandra")
	span.SetTag("Interface", "queryRetry")

	log.Debug("running queryRetry PageState() method")

	return queryRetry{goCqlQuery: q.goCqlQuery.PageState(state)}
}

// PageSize just a wrapper to be able to call this method
func (q queryRetry) PageSize(n int, parentSpan opentracing.Span) QueryInterface {
	span := opentracing.StartSpan("PageSize", opentracing.ChildOf(parentSpan.Context()))
	defer span.Finish()
	span.SetTag("Module", "cassandra")
	span.SetTag("Interface", "queryRetry")

	log.Debug("running queryRetry PageSize() method")

	return queryRetry{goCqlQuery: q.goCqlQuery.PageSize(n)}
}

//
func (i iterRetry) Scan(parentSpan opentracing.Span, dest ...interface{}) bool {
	span := opentracing.StartSpan("Scan", opentracing.ChildOf(parentSpan.Context()))
	defer span.Finish()
	span.SetTag("Module", "cassandra")
	span.SetTag("Interface", "iterRetry")

	log.Debug("running iterRetry Scan() method")

	retries := cassandraRetryAttempts
	secondsToSleep := 0

	var result bool

	attempts := 1
	for attempts <= retries {
		//we will try to run the method several times until attempts is met
		result = i.goCqlIter.Scan(dest...)
		if result == false {
			log.Warnf("Iter.Scan() failed: %v, attempt: %d / %d", result, attempts, retries)

			// incremental sleep
			secondsToSleep = secondsToSleep + cassandraSecondsToSleepIncrement

			log.Warnf("sleeping for %d second", secondsToSleep)

			log.Warnf("sleeping for %d second", secondsToSleep)
			time.Sleep(time.Duration(secondsToSleep) * time.Second)
		} else {
			// in case the error is nil, we stop and return
			return result
		}

		attempts = attempts + 1
	}

	return result
}

// WillSwitchPage is just a wrapper to be able to call this method
func (i iterRetry) WillSwitchPage(parentSpan opentracing.Span) bool {
	span := opentracing.StartSpan("WillSwitchPage", opentracing.ChildOf(parentSpan.Context()))
	defer span.Finish()
	span.SetTag("Module", "cassandra")
	span.SetTag("Interface", "iterRetry")

	log.Debug("running iterRetry Close() method")

	return i.goCqlIter.WillSwitchPage()
}

// Close is just a wrapper to be able to call this method
func (i iterRetry) Close(parentSpan opentracing.Span) error {
	span := opentracing.StartSpan("Close", opentracing.ChildOf(parentSpan.Context()))
	defer span.Finish()
	span.SetTag("Module", "cassandra")
	span.SetTag("Interface", "iterRetry")

	log.Debug("running iterRetry Close() method")

	return i.goCqlIter.Close()
}