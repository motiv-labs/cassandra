# Cassandra Module

Cassandra module to expose simple ways to interact with a Cassandra database.
This module allows connecting with a Cassandra database for specific keyspaces and hosts, also allows creating new 
keyspaces and tables if required, for that porpuse is required to have a `schema.sql` file with the statements to run 
under the `/usr/local/bin/` folder (`/usr/local/bin/schema.sql`).

### Overriding schema location
The schema location can be override using the `CASSANDRA_SCHEMA_PATH` and `CASSANDRA_SCHEMA_FILE_NAME` environment variables.

### Retries / Attempt
Cassandra module will handle errors internally and will retry the same call that failed for several times, while sleeping before the next attempt.
We have implemented retry attempt approach in place + incremental approach used. For example: 

If you have this
```
CASSANDRA_RETRY_ATTEMPTS=3
CASSANDRA_SECONDS_SLEEP_INCREMENT=1
```
First time it will wait 1 second, second time 2 seconds, third time 3 seconds, then fail... 

### Overriding Retries / Attempt
`CASSANDRA_RETRY_ATTEMPTS` 3 by default

`CASSANDRA_SECONDS_SLEEP` 1 by default 

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