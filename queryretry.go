package cassandra

import (
	"github.com/gocql/gocql"
	log "github.com/motiv-labs/logwrapper"
)

// queryRetry is an implementation of QueryInterface
type queryRetry struct {
	goCqlQuery *gocql.Query
}

func (q queryRetry) Exec() error {
	log.Info("running queryRetry Exec() method")

	return q.goCqlQuery.Exec()
}
