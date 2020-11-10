package cassandra

import "github.com/gocql/gocql"

// Initializer is a common interface for functionality to start a new session
type Initializer interface {
	NewSession() (Holder, error)
}

// Holder allows to store a close sessions
type Holder interface {
	GetSession() SessionInterface
	CloseSession()
}

// SessionInterface is an interface to wrap gocql methods used in Motiv
type SessionInterface interface {
	Query(stmt string, values ...interface{}) QueryInterface
	Close()
}

type QueryInterface interface {
	Exec() error
	Scan(dest ...interface{}) error
	Iter() *gocql.Iter
	PageState(state []byte) *gocql.Query
	PageSize(n int) *gocql.Query
}
