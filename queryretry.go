package cassandra

import (
	"github.com/gocql/gocql"
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

	log.Debugf("got cassandraRetryAttempts: %s", cassandraRetryAttempts)
	log.Debugf("got cassandraSecondsToSleepIncrement: %s", cassandraSecondsToSleep)
}

// queryRetry is an implementation of QueryInterface
type queryRetry struct {
	goCqlQuery *gocql.Query
}

// Exec wrapper to retry around gocql Exec(). We have a retry approach in place + incremental approach used. For example:
// First time it will wait 1 second, second time 2 seconds, ... It will depend on the values for retries and seconds to wait.
func (q queryRetry) Exec() error {
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
func (q queryRetry) Scan(dest ...interface{}) error {
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
func (q queryRetry) Iter() *gocql.Iter {
	log.Debug("running queryRetry Iter() method")

	return q.goCqlQuery.Iter()
}
