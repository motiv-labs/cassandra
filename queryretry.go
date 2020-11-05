package cassandra

import (
	"github.com/gocql/gocql"
	log "github.com/motiv-labs/logwrapper"
	"strconv"
)

const (
	defaultCassandraRetryAttempts = "3"

	envCassandraAttempts = "CASSANDRA_RETRY_ATTEMPTS"
)

var cassandraRetryAttempts = 3

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

	log.Debugf("got cassandraRetryAttempts: %s", cassandraRetryAttempts)
}

// queryRetry is an implementation of QueryInterface
type queryRetry struct {
	goCqlQuery *gocql.Query
}

// Exec wrapper to retry around gocql Exec()
func (q queryRetry) Exec() error {
	log.Debug("running queryRetry Exec() method")

	attempts := 1
	retries := cassandraRetryAttempts

	var err error

	for attempts <= retries {
		//we will try to run the method several times until attempts is met
		err = q.goCqlQuery.Exec()
		if err != nil {
			log.Warnf("error when running Exec(): %v, attempt: %d / %d", err, attempts, retries)
		} else {
			// in case the error is nil, we stop and return
			return err
		}

		attempts = attempts + 1
	}

	return err
}

// Scan wrapper to retry around gocql Scan()
func (q queryRetry) Scan(dest ...interface{}) error {
	log.Debug("running queryRetry Scan() method")

	attempts := 1
	retries := cassandraRetryAttempts

	var err error

	for attempts <= retries {
		//we will try to run the method several times until attempts is met
		err = q.goCqlQuery.Scan(dest...)
		if err != nil {
			log.Warnf("error when running Scan(): %v, attempt: %d / %d", err, attempts, retries)
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
