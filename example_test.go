package cassandra

import (
	log "github.com/motiv-labs/logwrapper"
	"time"
)

func Example() {

	// Cassandra initialization - initializes Cassandra keyspace and creates tables if required
	// Needs to be called only on the app startup
	Initialize("cluster_hostname", "system_keyspace", "application_keyspace", 120*time.Second)

	// Now that Cassandra is initialized we can start new connections

	// Getting a cassandra connection initializer
	sessionInitializer := New("db", "application_keyspace")

	// Starting a new cassandra session
	sessionHolder, err := sessionInitializer.NewSession()
	if err != nil {
		log.Errorf("error initializing cassandra session - %v", err)
		return
	}

	defer sessionHolder.CloseSession()

	// Getting the cassandra session
	session := sessionHolder.GetSession()
	// And have fun with the session, example:
	session.Query("SELECT * FROM my_table")
}

func ExampleNew() {

	// Getting a cassandra connection initializer
	sessionInitializer := New("db", "application_keyspace")

	// Starting a new cassandra session
	sessionHolder, err := sessionInitializer.NewSession()
	if err != nil {
		log.Errorf("error initializing cassandra session - %v", err)
		return
	}

	defer sessionHolder.CloseSession()

	// Getting the cassandra session
	session := sessionHolder.GetSession()
	// And have fun with the session, example:
	session.Query("SELECT * FROM my_table").Exec()
}
