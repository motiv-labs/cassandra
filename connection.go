package cassandra

import "github.com/gocql/gocql"

// Initializer is a common interface for functionality to start a new connection
type Initializer interface {
	StartConnection() (Holder, error)
}

// Holder allows to store a close connections
type Holder interface {
	GetConnection() *gocql.Session
	CloseConnection()
}
