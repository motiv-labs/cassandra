package cassandra

import (
	"context"
	"fmt"
	impulse_ctx "github.com/motiv-labs/impulse-ctx"
	log "github.com/motiv-labs/logwrapper"
	"strings"
	"time"
)

type Timestamp interface {
}

type timestamp struct {
	sessionRetry sessionRetry
	duration     time.Duration
}

func NewTimestamp(s sessionRetry, duration time.Duration) Timestamp {
	return timestamp{
		sessionRetry: s,
		duration:     duration,
	}
}

/*
CreatePartitionTimestampValue will create a value based on passed in unix time
This value can be used as the value for a partition key
Params:
	unixTime: the unix time to use
*/
func (t timestamp) CreatePartitionTimestampValue() int64 {
	// todo create the partition value based on the timestamp and unix value
	var unixTime int64
	switch t.duration {
	case time.Millisecond:
		unixTime = time.Now().UnixMilli()
	case time.Microsecond:
		unixTime = time.Now().UnixMicro()
	case time.Nanosecond:
		unixTime = time.Now().UnixNano()
	default: // default to seconds
		unixTime = time.Now().Unix()
	}

	return unixTime
}

/*
PartitionTimestampQuery will query across timestamp based partitions for as many records as match the limit
Params:
	ctx: context object with ImpulseCtx struct for logging and query functions
	table: the table to query
	where: where clause and additional options to pass the query
	timeRangeColumn: the name of the column to use a time range on
	start: start time to query by
	end: end time to query by
	limit: the number of records to look for and return
*/
func (t timestamp) PartitionTimestampQuery(ctx context.Context, table, where, timeRangeColumn string, start, end time.Time, limit int) []interface{} {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}
	// todo build statement
	query := t.buildCassQuery(table, where, timeRangeColumn, start, end, limit)
	// todo perform query
	// todo repeat query to next partition if limit is not met
	return nil
}

/*
buildCassQuery will build the cassandra statement for a partition timestamp query
Params:
	table: the table to query
	where: where clause and additional options to pass the query
	timeRangeColumn: the name of the column to use a time range on
	start: start time to query by
	end: end time to query by
	limit: the number of records to look for and return
*/
func (t timestamp) buildCassQuery(table, where, timeRangeColumn string, start, end time.Time, limit int) string {
	// todo build cassandra statement to be used in timestamp query
	// build initial select clause
	selectClause := fmt.Sprintf("SELECT * FROM %s", table)

	// build where clause
	// build in section
	var startTime int64
	var endTime int64
	switch t.duration {
	case time.Millisecond:
		startTime = start.UnixMilli()
		endTime = end.UnixMilli()
	case time.Microsecond:
		startTime = start.UnixMicro()
		endTime = end.UnixMilli()
	case time.Nanosecond:
		startTime = start.UnixNano()
		endTime = end.UnixMilli()
	default: // default to seconds
		startTime = start.Unix()
		endTime = end.UnixMilli()
	}

	// open in clause
	inClause := "IN ("

	// todo understand and fix this int64 conversion logic based on https://github.com/hailocab/gocassa/blob/master/timeseries_table.go#L51
	for i := startTime; ; i += int64(t.duration/t.duration) * 1000 {
		if i >= endTime*1000 {
			break
		}
		// todo fix this append
		if i+int64(t.duration/t.duration)*1000 >= endTime*1000 {
			inClause = fmt.Sprintf("%s")
		} else {
			inClause = fmt.Sprintf("%s")
		}
	}
	// close in clause
	inClause = fmt.Sprintf("%s)", inClause)

	whereClause := fmt.Sprintf("WHERE %s %s", timeRangeColumn, inClause)
	if where != "" {
		whereClause = fmt.Sprintf("%s AND %s", whereClause, where)
	}

	// build limit clause
	limitClause := fmt.Sprintf("LIMIT %d", limit)

	// combine all clauses to create the query
	query := strings.Join([]string{selectClause, whereClause, limitClause}, " ")

	return query
}
