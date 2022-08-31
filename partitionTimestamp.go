package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gocql/gocql"
	impulse_ctx "github.com/motiv-labs/impulse-ctx"
	log "github.com/motiv-labs/logwrapper"
	"strings"
	"time"
)

const (
	inClauseLimit = 300
)

type Timestamp interface {
	CreatePartitionTimestampValue() int64
	PartitionTimestampQuery(ctx context.Context, table, where, timeRangeColumn string, timeRangeIsUUID bool, start, end time.Time, limit int) ([]map[string]interface{}, error)
}

type timestamp struct {
	session         SessionInterface
	duration        time.Duration
	partitionColumn string
}

func NewTimestamp(s SessionInterface, duration time.Duration, partitionColumn string) Timestamp {
	return timestamp{
		session:         s,
		duration:        duration,
		partitionColumn: partitionColumn,
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
	// todo upgrade go versions everywhere to use unix milli or micro
	var unixTime int64
	// for now, only use seconds. future updates can allow for options.
	unixTime = time.Now().Unix()
	return unixTime
}

/*
PartitionTimestampQuery will query across timestamp based partitions for as many records as match the limit
Table is assumed to be in ascending order
Params:
	ctx: context object with ImpulseCtx struct for logging and query functions
	table: the table to query
	where: where clause and additional options to pass the query
	timeRangeColumn: the name of the column to use a time range on
	start: start time to query by
	end: end time to query by
	limit: the number of records to look for and return
*/
func (t timestamp) PartitionTimestampQuery(ctx context.Context, table, where, timeRangeColumn string, timeRangeIsUUID bool, start, end time.Time, limit int) ([]map[string]interface{}, error) {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	// todo check limit and record list length and loop accordingly.
	var recordList []map[string]interface{}
	startTime := start

	for (len(recordList) < limit || limit < 0) && startTime.Before(end) {
		innerRecordList, err := t.performQuery(ctx, table, where, timeRangeColumn, timeRangeIsUUID, startTime, end, limit)
		if err != nil {
			log.Errorf(impulseCtx, "error performing query %v", err)
			return recordList, err
		}
		// add newly returned records to larger list
		recordList = append(recordList, innerRecordList...)

		// update start time based on in limit
		// atm everything is only ever based in seconds.
		startTime = startTime.Add(time.Duration(inClauseLimit) * time.Second)
	}

	return recordList, nil
}

func (t timestamp) performQuery(ctx context.Context, table, where, timeRangeColumn string, timeRangeIsUUID bool, start, end time.Time, limit int) ([]map[string]interface{}, error) {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	query := t.buildCassQuery(table, where, timeRangeColumn, timeRangeIsUUID, start, end, limit)

	iter := t.session.Query(ctx, query).Iter(ctx)

	var recordList []map[string]interface{}
	var err error

	recordList, err = iter.SliceMapAndClose(ctx)

	if err != nil {
		log.Errorf(impulseCtx, "error while querying table %s", table)
	} else {
		log.Debugf(impulseCtx, "successfully returning record list from table %s", table)
	}

	return recordList, err
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
func (t timestamp) buildCassQuery(table, where, timeRangeColumn string, timeRangeIsUUID bool, start, end time.Time, limit int) string {
	funcTime := time.Now()
	// build initial select clause
	selectClause := fmt.Sprintf("SELECT * FROM %s", table)

	// build where clause
	// build in section
	// todo upgrade go versions everywhere to use unix milli or micro
	var startTime int64
	var endTime int64
	// for now, only use seconds. future updates can allow variability
	startTime = start.Unix()
	endTime = end.Unix()
	// open in clause
	inClause := "IN ("

	// based on https://github.com/hailocab/gocassa/blob/master/timeseries_table.go#L51
	// note: t.duration/t.duration will always be 1 micro second * 1000.
	// increment by 1 because we can save ever second

	for i, iterations := startTime, 0; ; i, iterations = i+1, iterations+1 { // increment each second
		if i > endTime || iterations > inClauseLimit { // either we've reached the time range or the max amount of values for the in clause was reached
			break
		}
		if i == startTime {
			// first option, don't add comma
			inClause = fmt.Sprintf("%s%d", inClause, i)
		} else {
			// add next partition key with comma
			inClause = fmt.Sprintf("%s, %d", inClause, i)
		}
	}
	// close in clause
	inClause = fmt.Sprintf("%s)", inClause)

	// build time range clause
	var timeRangeClause string
	if timeRangeIsUUID {
		startUUID := gocql.UUIDFromTime(start)
		endUUID := gocql.UUIDFromTime(end)
		timeRangeClause = fmt.Sprintf("%s > %s AND %s < %s", timeRangeColumn, startUUID.String(), timeRangeColumn, endUUID.String())
	} else {
		timeRangeClause = fmt.Sprintf("%s > '%s' AND %s < '%s'", timeRangeColumn, start.String(), timeRangeColumn, end.String())
	}

	whereClause := fmt.Sprintf("WHERE %s %s AND %s", t.partitionColumn, inClause, timeRangeClause)
	if where != "" {
		whereClause = fmt.Sprintf("%s AND %s", whereClause, where)
	}

	// build limit clause
	var limitClause string
	// if the limit is -1, that means we don't need a limit, that's the reason of the blank statement
	if limit > 0 {
		// if the limit is > 0, it means that we need a valid number to pass to cassandra
		limitClause = fmt.Sprintf("LIMIT %d", limit)
	}

	// combine all clauses to create the query
	query := strings.Join([]string{selectClause, whereClause, limitClause}, " ")

	println("function total time is ", time.Since(funcTime).String())
	return query
}

/*
ConvertSliceMap is used to convert a slice map returned from gocql into the passed in struct.
This can be used in tandem with PartitionTimestampQuery to convert teh record list into a specific slice structure.
*/
func ConvertSliceMap(ctx context.Context, sliceMap []map[string]interface{}, v interface{}) error {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	jsonStr, err := json.Marshal(sliceMap)
	if err != nil {
		log.Errorf(impulseCtx, "error marshaling slice map %v", err)
		return err
	}

	err = json.Unmarshal(jsonStr, &v)
	if err != nil {
		log.Errorf(impulseCtx, "error unmarshaling slice map %v", err)
		return err
	}

	return nil
}

func ConvertSliceMapWithArg(ctx context.Context, sliceMap []map[string]interface{}, v interface{}, keyConvertMap map[string]string) error {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	//keyConvertedSliceMap := make([]map[string]interface{}, len(sliceMap))
	//for k, v := range keyConvertMap {
	// todo write this key conversion logic
	//}

	jsonStr, err := json.Marshal(sliceMap)
	if err != nil {
		log.Errorf(impulseCtx, "error marshaling slice map %v", err)
		return err
	}

	err = json.Unmarshal(jsonStr, &v)
	if err != nil {
		log.Errorf(impulseCtx, "error unmarshaling slice map %v", err)
		return err
	}

	return nil
}
