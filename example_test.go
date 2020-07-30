package cassandra

import (
	"fmt"
	"time"
)

func Example() {

	// First Cassandra initialization - initializes Cassandra keyspace and creates tables if required
	// Needs to be called only on the app startup
	Initialize("cluster_hostname", "system_keyspace", "application_keyspace", 120*time.Second)

	// Now that Cassandra is initialized we can start new connections

	// Getting a cassandra session initializer
	sessionInitializer := New("db", "application_keyspace")

	// Starting a new cassandra connection
	sessionHolder, err := sessionInitializer.StartConnection()
	if err != nil {
		fmt.Println(err)
	}

	defer sessionHolder.CloseSession()

	// Getting the cassandra session
	session := sessionHolder.GetSession()
	// And have fun with the session, example:
	session.Query("SELECT * FROM my_table")
}

func ExampleNew() {

	// Getting a cassandra session initializer
	sessionInitializer := New("db", "application_keyspace")

	// Starting a new cassandra connection
	sessionHolder, err := sessionInitializer.StartConnection()
	if err != nil {
		fmt.Println(err)
	}

	defer sessionHolder.CloseSession()

	// Getting the cassandra session
	session := sessionHolder.GetSession()
	// And have fun with the session, example:
	session.Query("SELECT * FROM my_table")
}
