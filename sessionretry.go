package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	impulse_ctx "github.com/motiv-labs/impulse-ctx"
	log "github.com/motiv-labs/logwrapper"
	"github.com/opentracing/opentracing-go"
)

// sessionRetry is an implementation of SessionInterface
type sessionRetry struct {
	goCqlSession *gocql.Session
}

// Query wrapper to be able to return our own QueryInterface
func (s sessionRetry) Query(ctx context.Context, stmt string, values ...interface{}) QueryInterface {
	var span opentracing.Span
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
		span = opentracing.StartSpan("Query")
		defer span.Finish()
		span.SetTag("Module", "cassandra")
		span.SetTag("Interface", "sessionRetry")
		impulseCtx.Span = span
	} else {
		span = opentracing.StartSpan("Query", opentracing.ChildOf(impulseCtx.Span.Context()))
		defer span.Finish()
		span.SetTag("Module", "cassandra")
		span.SetTag("Interface", "sessionRetry")
		impulseCtx.Span = span
	}
	ctx = context.WithValue(ctx, impulse_ctx.ImpulseCtxKey, impulseCtx)

	log.Debug(impulseCtx, "running SessionRetry Query() method")

	return queryRetry{goCqlQuery: s.goCqlSession.Query(stmt, values...)}
}

// Close wrapper to be able to run goCql method
func (s sessionRetry) Close(ctx context.Context) {
	var span opentracing.Span
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
		span = opentracing.StartSpan("Close")
		defer span.Finish()
		span.SetTag("Module", "cassandra")
		span.SetTag("Interface", "sessionRetry")
		impulseCtx.Span = span
	} else {
		span = opentracing.StartSpan("Close", opentracing.ChildOf(impulseCtx.Span.Context()))
		defer span.Finish()
		span.SetTag("Module", "cassandra")
		span.SetTag("Interface", "sessionRetry")
		impulseCtx.Span = span
	}
	ctx = context.WithValue(ctx, impulse_ctx.ImpulseCtxKey, impulseCtx)

	log.Debug(impulseCtx, "running SessionRetry Close() method")

	s.goCqlSession.Close()
}
