package cassandra

import (
	"github.com/gocql/gocql"
	log "github.com/motiv-labs/logwrapper"
)

// Initializer is a common interface for functionality to start a new session
type Initializer interface {
	NewSession() (Holder, error)
}

// Holder allows to store a close sessions
type Holder interface {
	GetSession() SessionInterface
	CloseSession()
}

type SessionInterface interface {
	Query(stmt string, values ...interface{}) *gocql.Query
	Close()
}

type SessionRetry struct {
	GoCqlSession *gocql.Session
}

func (s SessionRetry) Query(stmt string, values ...interface{}) *gocql.Query {
	log.Info("running SessionRetry Query() method")
	return s.GoCqlSession.Query(stmt, values...)
}

func (s SessionRetry) Close() {
	log.Info("running SessionRetry Close() method")
	s.GoCqlSession.Close()
}
