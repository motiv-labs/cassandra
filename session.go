package cassandra

import "github.com/gocql/gocql"

// Initializer is a common interface for functionality to start a new session
type Initializer interface {
	NewSession() (Holder, error)
}

// Holder allows to store a close connections
type Holder interface {
	GetSession() *gocql.Session
	CloseSession()
}
