# Cassandra Module

Cassandra module to expose simple ways to interact with a Cassandra database.
This module allows connecting with a Cassandra database for specific keyspaces and hosts, also allows creating new 
keyspaces and tables if required, for that porpuse is required to have a `schema.sql` file with the stataments to run 
under the `/usr/local/bin/` folder (`/usr/local/bin/schema.sql`).

## Example of use:

``` go
// Cassandra initialization - initializes Cassandra keyspace and creates tables if required
// Needs to be called only on the app startup
cassandra.Initialize("cluster_hostname", "system_keyspace", "application_keyspace", 120*time.Second)

// Now that Cassandra is initialized we can start new connections

// Getting a cassandra connection initializer
sessionInitializer := cassandra.New("db", "application_keyspace")

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
```