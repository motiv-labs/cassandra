package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	impulse_ctx "github.com/motiv-labs/impulse-ctx"
	log "github.com/motiv-labs/logwrapper"
)

// sessionRetry is an implementation of SessionInterface
type sessionRetry struct {
	goCqlSession *gocql.Session
}

// Query wrapper to be able to return our own QueryInterface
func (s sessionRetry) Query(ctx context.Context, stmt string, values ...interface{}) QueryInterface {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running SessionRetry Query() method")

	return queryRetry{goCqlQuery: s.goCqlSession.Query(stmt, values...)}
}

// Close wrapper to be able to run goCql method
func (s sessionRetry) Close(ctx context.Context) {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	log.Debug(impulseCtx, "running SessionRetry Close() method")

	s.goCqlSession.Close()
}
