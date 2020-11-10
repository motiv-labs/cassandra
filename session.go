package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/opentracing/opentracing-go"
)

// Initializer is a common interface for functionality to start a new session
type Initializer interface {
	NewSession(parentSpan opentracing.Span) (Holder, error)
}

// Holder allows to store a close sessions
type Holder interface {
	GetSession(parentSpan opentracing.Span) SessionInterface
	CloseSession(parentSpan opentracing.Span)
}

// SessionInterface is an interface to wrap gocql methods used in Motiv
type SessionInterface interface {
	Query(stmt string, parentSpan opentracing.Span, values ...interface{}) QueryInterface
	Close(parentSpan opentracing.Span)
}

type QueryInterface interface {
	Exec(parentSpan opentracing.Span) error
	Scan(parentSpan opentracing.Span, dest ...interface{}) error
	Iter(parentSpan opentracing.Span) *gocql.Iter
}
