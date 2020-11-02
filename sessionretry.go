package cassandra

import (
	"github.com/gocql/gocql"
	log "github.com/motiv-labs/logwrapper"
	"time"
)

// sessionRetry is an implementation of SessionInterface
type sessionRetry struct {
	goCqlSession *gocql.Session
}

func (s sessionRetry) Query(stmt string, values ...interface{}) *gocql.Query {
	log.Debug("running SessionRetry Query() method")

	sp := &gocql.SimpleSpeculativeExecution{
		NumAttempts:  3,
		TimeoutDelay: 1 * time.Second,
	}

	return s.goCqlSession.Query(stmt, values...).SetSpeculativeExecutionPolicy(sp).Idempotent(true)
}

func (s sessionRetry) Close() {
	log.Debug("running SessionRetry Close() method")

	s.goCqlSession.Close()
}
