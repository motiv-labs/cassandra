package cassandra

import (
	"github.com/gocql/gocql"
	log "github.com/motiv-labs/logwrapper"
)

// sessionRetry is an implementation of SessionInterface
type sessionRetry struct {
	goCqlSession *gocql.Session
}

func (s sessionRetry) Query(stmt string, values ...interface{}) *gocql.Query {
	log.Info("running SessionRetry Query() method")
	return s.goCqlSession.Query(stmt, values...)
}

func (s sessionRetry) Close() {
	log.Info("running SessionRetry Close() method")
	s.goCqlSession.Close()
}
