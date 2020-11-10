package cassandra

import (
	"github.com/gocql/gocql"
	log "github.com/motiv-labs/logwrapper"
	"github.com/opentracing/opentracing-go"
)

// sessionRetry is an implementation of SessionInterface
type sessionRetry struct {
	goCqlSession *gocql.Session
}

// Query wrapper to be able to return our own QueryInterface
func (s sessionRetry) Query(stmt string, parentSpan opentracing.Span, values ...interface{}) QueryInterface {
	span := opentracing.StartSpan("Query", opentracing.ChildOf(parentSpan.Context()))
	defer span.Finish()
	span.SetTag("Module", "cassandra")
	span.SetTag("Interface", "sessionRetry")

	log.Debug("running SessionRetry Query() method")

	return queryRetry{goCqlQuery: s.goCqlSession.Query(stmt, values...)}
}

// Close wrapper to be able to run goCql method
func (s sessionRetry) Close(parentSpan opentracing.Span) {
	span := opentracing.StartSpan("Close", opentracing.ChildOf(parentSpan.Context()))
	defer span.Finish()
	span.SetTag("Module", "cassandra")
	span.SetTag("Interface", "sessionRetry")

	log.Debug("running SessionRetry Close() method")

	s.goCqlSession.Close()
}
