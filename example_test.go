package cassandra

import (
	jaegermod "github.com/motiv-labs/jaeger"
	log "github.com/motiv-labs/logwrapper"
	"github.com/opentracing/opentracing-go"
	"time"
)

// getSpan simply retuns a real span to show passed in the Kafka examples.
func getSpan() opentracing.Span {
	tracer, closer := jaegermod.Initialize("sync-manager")
	defer closer.Close()
	return tracer.StartSpan("example span")
}

func Example() {
	span := getSpan()

	// Cassandra initialization - initializes Cassandra keyspace and creates tables if required
	// Needs to be called only on the app startup
	Initialize("cluster_hostname", "system_keyspace", "application_keyspace", 120*time.Second, span)

	// Now that Cassandra is initialized we can start new connections

	// Getting a cassandra connection initializer
	sessionInitializer := New("db", "application_keyspace", span)

	// Starting a new cassandra session
	sessionHolder, err := sessionInitializer.NewSession(span)
	if err != nil {
		log.Errorf("error initializing cassandra session - %v", err)
		return
	}

	defer sessionHolder.CloseSession(span)

	// Getting the cassandra session
	session := sessionHolder.GetSession(span)
	// And have fun with the session, example:
	session.Query(span, "SELECT * FROM my_table")
}

func ExampleNew() {
	span := getSpan()

	// Getting a cassandra connection initializer
	sessionInitializer := New("db", "application_keyspace", span)

	// Starting a new cassandra session
	sessionHolder, err := sessionInitializer.NewSession(span)
	if err != nil {
		log.Errorf("error initializing cassandra session - %v", err)
		return
	}

	defer sessionHolder.CloseSession(span)

	// Getting the cassandra session
	session := sessionHolder.GetSession(span)
	// And have fun with the session, example:
	session.Query(span, "SELECT * FROM my_table").Exec(span)
}

func Example_iterRetry_Scan() {

	type Data struct {
		Data1 string
		Data2 string
		Data3 string
		Data4 string
	}

	span := getSpan()

	// Getting a cassandra connection initializer
	sessionInitializer := New("db", "application_keyspace", span)

	// Starting a new cassandra session
	sessionHolder, err := sessionInitializer.NewSession(span)
	if err != nil {
		log.Errorf("error initializing cassandra session - %v", err)
		return
	}

	defer sessionHolder.CloseSession(span)

	// Getting the cassandra session
	session := sessionHolder.GetSession(span)

	// And have fun with the session, example:
	iter := session.Query(span, "SELECT * FROM my_table").Iter(span)

	var data Data
	var dataList []Data

	for iter.Scan(span, &data.Data1, &data.Data2, &data.Data3, &data.Data4) {
		dataList = append(dataList, data)
	}

	if err := iter.Close(span); err != nil {
		log.Error("error while querying table")
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

	span := getSpan()

	// Getting a cassandra connection initializer
	sessionInitializer := New("db", "application_keyspace", span)

	// Starting a new cassandra session
	sessionHolder, err := sessionInitializer.NewSession(span)
	if err != nil {
		log.Errorf("error initializing cassandra session - %v", err)
		return
	}

	defer sessionHolder.CloseSession(span)

	// Getting the cassandra session
	session := sessionHolder.GetSession(span)

	// And have fun with the session, example:
	iter := session.Query(span, "SELECT * FROM my_table").Iter(span)

	var data Data
	var dataList []Data

	if err := iter.ScanAndClose(span, data, func(object interface{}) {
		dataList = append(dataList, object.(Data))
	}, &data.Data1, &data.Data2, &data.Data3, &data.Data4); err != nil {
		log.Error("error while querying table")
		// return ...
	}

	// return ...
}
