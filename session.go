package cassandra

import (
	"context"
)

// Initializer is a common interface for functionality to start a new session
type Initializer interface {
	NewSession(ctx context.Context) (Holder, error)
}

// Holder allows to store a close sessions
type Holder interface {
	GetSession(ctx context.Context) SessionInterface
	CloseSession(ctx context.Context)
}

// SessionInterface is an interface to wrap gocql methods used in Motiv
type SessionInterface interface {
	Query(ctx context.Context, stmt string, values ...interface{}) QueryInterface
	Close(ctx context.Context)
}

type QueryInterface interface {
	Exec(ctx context.Context) error
	Scan(ctx context.Context, dest ...interface{}) error
	Iter(ctx context.Context) IterInterface
	PageState(state []byte, ctx context.Context) QueryInterface
	PageSize(n int, ctx context.Context) QueryInterface
}

type IterInterface interface {
	Scan(ctx context.Context, dest ...interface{}) bool
	WillSwitchPage(ctx context.Context) bool
	PageState(ctx context.Context) []byte
	MapScan(m map[string]interface{}, ctx context.Context) bool
	//MapScanAndClose(m *map[string]interface{}, handle func() bool, ctx context.Context) error
	Close(ctx context.Context) error
	ScanAndClose(ctx context.Context, handle func() bool, dest ...interface{}) error
	SliceMapAndClose(ctx context.Context) ([]map[string]interface{}, error)
}
