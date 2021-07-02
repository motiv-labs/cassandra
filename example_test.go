package cassandra

import (
	"context"
	impulse_ctx "github.com/motiv-labs/impulse-ctx"
	jaegermod "github.com/motiv-labs/jaeger"
	log "github.com/motiv-labs/logwrapper"
	"github.com/opentracing/opentracing-go"
	"time"
)

func NewMockCtx() context.Context {
	span := getSpan()
	return context.WithValue(context.Background(), impulse_ctx.ImpulseCtxKey, impulse_ctx.ImpulseCtx{
		Organization: "",
		Span:         span,
		TraceId:      "",
	})
}

// getSpan simply retuns a real span to show passed in the Kafka examples.
func getSpan() opentracing.Span {
	tracer, closer := jaegermod.Initialize("sync-manager")
	defer closer.Close()
	return tracer.StartSpan("example span")
}

func Example() {
	ctx := NewMockCtx()
	impulseCtx := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)

	// Cassandra initialization - initializes Cassandra keyspace and creates tables if required
	// Needs to be called only on the app startup
	Initialize("system_keyspace", "application_keyspace", 120*time.Second, ctx)

	// Now that Cassandra is initialized we can start new connections

	// Getting a cassandra connection initializer
	sessionInitializer := New("application_keyspace", ctx)

	// Starting a new cassandra session
	sessionHolder, err := sessionInitializer.NewSession(ctx)
	if err != nil {
		log.Errorf(impulseCtx, "error initializing cassandra session - %v", err)
		return
	}

	defer sessionHolder.CloseSession(ctx)

	// Getting the cassandra session
	session := sessionHolder.GetSession(ctx)
	// And have fun with the session, example:
	session.Query(ctx, "SELECT * FROM my_table")
}

func ExampleNew() {
	ctx := NewMockCtx()
	impulseCtx := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)

	// Getting a cassandra connection initializer
	sessionInitializer := New("application_keyspace", ctx)

	// Starting a new cassandra session
	sessionHolder, err := sessionInitializer.NewSession(ctx)
	if err != nil {
		log.Errorf(impulseCtx, "error initializing cassandra session - %v", err)
		return
	}

	defer sessionHolder.CloseSession(ctx)

	// Getting the cassandra session
	session := sessionHolder.GetSession(ctx)
	// And have fun with the session, example:
	session.Query(ctx, "SELECT * FROM my_table").Exec(ctx)
}

func Example_iterRetry_Scan() {

	type Data struct {
		Data1 string
		Data2 string
		Data3 string
		Data4 string
	}

	ctx := NewMockCtx()
	impulseCtx := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)

	// Getting a cassandra connection initializer
	sessionInitializer := New( "application_keyspace", ctx)

	// Starting a new cassandra session
	sessionHolder, err := sessionInitializer.NewSession(ctx)
	if err != nil {
		log.Errorf(impulseCtx, "error initializing cassandra session - %v", err)
		return
	}

	defer sessionHolder.CloseSession(ctx)

	// Getting the cassandra session
	session := sessionHolder.GetSession(ctx)

	// And have fun with the session, example:
	iter := session.Query(ctx, "SELECT * FROM my_table").Iter(ctx)

	var data Data
	var dataList []Data

	for iter.Scan(ctx, &data.Data1, &data.Data2, &data.Data3, &data.Data4) {
		dataList = append(dataList, data)
	}

	if err := iter.Close(ctx); err != nil {
		log.Error(impulseCtx, "error while querying table")
		// return ...
	}

	// return ...
}

func Example_iterRetry_ScanAndClose() {

	type Data struct {
		Data1 string
		Data2 string
		Data3 string
		Data4 string
	}

	ctx := NewMockCtx()
	impulseCtx := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)

	// Getting a cassandra connection initializer
	sessionInitializer := New("application_keyspace", ctx)

	// Starting a new cassandra session
	sessionHolder, err := sessionInitializer.NewSession(ctx)
	if err != nil {
		log.Errorf(impulseCtx,"error initializing cassandra session - %v", err)
		return
	}

	defer sessionHolder.CloseSession(ctx)

	// Getting the cassandra session
	session := sessionHolder.GetSession(ctx)

	// And have fun with the session, example:
	iter := session.Query(ctx, "SELECT * FROM my_table").Iter(ctx)

	var data Data
	var dataList []Data

	if err := iter.ScanAndClose(ctx, func() bool {
		dataList = append(dataList, data)
		return true
	}, &data.Data1, &data.Data2, &data.Data3, &data.Data4); err != nil {
		log.Error(impulseCtx,"error while querying table")
		// return ...
	}

	// return ...
}
