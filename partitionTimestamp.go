package cassandra

import (
	"context"
	"time"
)

type Timestamp interface {
}

type timestamp struct {
	sessionRetry sessionRetry
	unixTime     int64
}

func NewTimestamp(s sessionRetry, unixTime int64) Timestamp {
	return timestamp{
		sessionRetry: s,
		unixTime:     unixTime,
	}
}

/*
CreatePartitionTimestampValue will create a value based on passed in unix time
This value can be used as the value for a partition key
Params:
	unixTime: the unix time to use
*/
func (t timestamp) CreatePartitionTimestampValue() int64 {
	// todo create the partiion value based on the timestamp and unix value
	return 0
}

/*
PartitionTimestampQuery will query across timestamp based partitions for as many records as match the limit
Params:
	ctx: context object with ImpulseCtx struct for logging and query functions
	table: the table to query
	where: where clause and additional options to pass the query
	start: start time to query by
	end: end time to query by
	limit: the number of records to look for and return
*/
func (t timestamp) PartitionTimestampQuery(ctx context.Context, table, where string, start, end time.Time, limit int) []interface{} {
	// todo build statement
	// todo perform query
	// todo repeat query to next partition if limit is not met
	return nil
}

/*
buildWhereClause will build the where clause for a partition timestamp query
*/
func (t timestamp) buildCassStmt(table, where string, start, end time.Time) string {
	// todo build cassandra statement to be used in timestamp query
	return ""
}
