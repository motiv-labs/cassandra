package cassandra

import (
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
	Query(parentSpan opentracing.Span, stmt string, values ...interface{}) QueryInterface
	Close(parentSpan opentracing.Span)
}

type QueryInterface interface {
	Exec(parentSpan opentracing.Span) error
	Scan(parentSpan opentracing.Span, dest ...interface{}) error
	Iter(parentSpan opentracing.Span) IterInterface
	PageState(state []byte, parentSpan opentracing.Span) QueryInterface
	PageSize(n int, parentSpan opentracing.Span) QueryInterface
}

type IterInterface interface {
	Scan(parentSpan opentracing.Span, dest ...interface{}) bool
	WillSwitchPage(parentSpan opentracing.Span) bool
	PageState(parentSpan opentracing.Span) []byte
	Close(parentSpan opentracing.Span) error
	ScanAndClose(parentSpan opentracing.Span, object interface{},
		handle func(object interface{}) bool, dest ...interface{}) error
}
