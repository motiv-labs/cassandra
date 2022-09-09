package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/mitchellh/mapstructure"
	impulse_ctx "github.com/motiv-labs/impulse-ctx"
	log "github.com/motiv-labs/logwrapper"
	"reflect"
	"strings"
	"time"
)

const (
	inClauseLimit = 300
)

type Timestamp interface {
	CreatePartitionTimestampValue() int64
	CreatePartitionTimestampValueFromTime(timestamp time.Time) int64
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
CreatePartitionTimestampValue will create a unix timestamp value based for the current second
This value can be used as the value for a partition key
*/
func (t timestamp) CreatePartitionTimestampValue() int64 {
	// todo create the partition value based on the unix value
	// todo upgrade go versions everywhere to use unix milli or micro
	var unixTime int64
	// for now, only use seconds. future updates can allow for options.
	unixTime = time.Now().Unix()
	return unixTime
}

/*
CreatePartitionTimestampValueFromTime will create a unix timestamp value based on the passed in timestamp
This value can be used as the value for a partition key
Params:
	timestamp: the timestamp to create a unix time from
*/
func (t timestamp) CreatePartitionTimestampValueFromTime(timestamp time.Time) int64 {
	// todo create the partition value based on the timestamp and unix value
	// todo upgrade go versions everywhere to use unix milli or micro
	var unixTime int64
	// for now, only use seconds. future updates can allow for options.
	unixTime = timestamp.Unix()
	return unixTime
}

/*
PartitionTimestampQuery will query across timestamp based partitions for as many records as match the limit
Table is assumed to be in ascending order
Params:
	ctx: context object with ImpulseCtx struct for logging and query functions
	table: the table to query
	where: partial where clause and additional options to pass the query. do not include keywords WHERE, ORDER BY, or LIMIT
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

	var recordList []map[string]interface{}
	startTime := start

	for (len(recordList) < limit || limit < 0) && startTime.Before(end) {
		innerLimit := limit - len(recordList)
		innerRecordList, err := t.performQuery(ctx, table, where, timeRangeColumn, timeRangeIsUUID, startTime, end, innerLimit)
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

	funcTime := time.Now()

	partitions := t.getPartitionShards(ctx, start, end)

	var recordList []map[string]interface{}

	for _, partition := range partitions {
		if len(recordList) < limit || limit < 0 {
			break
		}
		innerLimit := limit - len(recordList)
		query := t.buildCassQuery(ctx, table, where, timeRangeColumn, timeRangeIsUUID, start, end, innerLimit, partition)

		iter := t.session.Query(ctx, query).Iter(ctx)

		var innerRecordList []map[string]interface{}
		var err error

		innerRecordList, err = iter.SliceMapAndClose(ctx)

		if err != nil {
			log.Errorf(impulseCtx, "error while querying table %s", table)
			return nil, err
		} else {
			log.Debugf(impulseCtx, "successfully returning record list from table %s", table)
		}

		recordList = append(recordList, innerRecordList...)
	}

	log.Infof(impulseCtx, "function total time is ", time.Since(funcTime).String())
	return recordList, nil
}

func (t timestamp) getPartitionShards(ctx context.Context, start, end time.Time) []string {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	var startTime int64
	var endTime int64
	// for now, only use seconds. future updates can allow variability
	startTime = start.Unix()
	endTime = end.Unix()
	// make slice
	partitions := make([]string, inClauseLimit)

	// based on https://github.com/hailocab/gocassa/blob/master/timeseries_table.go#L51
	// note: t.duration/t.duration will always be 1 micro second * 1000.
	// increment by 1 because we can save ever second

	for i, iterations := startTime, 0; ; i, iterations = i+1, iterations+1 { // increment each second
		if i > endTime || iterations > inClauseLimit { // either we've reached the time range or the max amount of values for the in clause was reached
			break
		}
		partitions[iterations] = fmt.Sprintf("%d", i)
	}
	return partitions
}

/*
buildCassQuery will build the cassandra statement for a partition timestamp query
Params:
	table: the table to query
	where: partial where clause and additional options to pass the query. do not include keywords WHERE, ORDER BY, or LIMIT
	timeRangeColumn: the name of the column to use a time range on
	start: start time to query by
	end: end time to query by
	limit: the number of records to look for and return
*/
func (t timestamp) buildCassQuery(ctx context.Context, table, where, timeRangeColumn string, timeRangeIsUUID bool, start, end time.Time, limit int, partition string) string {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}
	// build initial select clause
	selectClause := fmt.Sprintf("SELECT * FROM %s", table)

	// build where clause
	var whereClause string
	// build in section
	// todo upgrade go versions everywhere to use unix milli or micro

	// partition shard equal
	partitionEq := fmt.Sprintf("%s=%s", t.partitionColumn, partition)

	// build time range clause
	var timeRangeClause string
	if timeRangeColumn != "" { // only build the clause if a time range column was passed in
		// difference in clauses are based on the logger service's pattern prior to 9/1/2022
		if timeRangeIsUUID {
			startUUID := gocql.UUIDFromTime(start)
			endUUID := gocql.UUIDFromTime(end)
			timeRangeClause = fmt.Sprintf("%s >= %s AND %s <= %s", timeRangeColumn, startUUID.String(), timeRangeColumn, endUUID.String())
		} else {
			timeRangeClause = fmt.Sprintf("%s >= '%s' AND %s <= '%s'", timeRangeColumn, start.String(), timeRangeColumn, end.String())
		}
		whereClause = fmt.Sprintf("WHERE %s AND %s", partitionEq, timeRangeClause)
	} else {
		whereClause = fmt.Sprintf("WHERE %s", partitionEq)
	}

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

/*
ConvertSliceMapWithMapStructure is used to convert a slice map returned from gocql into the passed in struct using the tag "mapstructure" to convert column names to each attribute
This can be used in tandem with PartitionTimestampQuery to convert teh record list into a specific slice structure.
*/
func ConvertSliceMapWithMapStructure(ctx context.Context, sliceMap []map[string]interface{}, v interface{}) error {
	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
	if !ok {
		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
	}

	err := CustomDecode(sliceMap, &v) // note: I don't think this is actually a decoder, just an unmarshaller.
	if err != nil {
		log.Errorf(impulseCtx, "error decoding slice map %v", err)
		return err
	}

	return nil
}

//func ConvertSliceMapWithArg(ctx context.Context, sliceMap []map[string]interface{}, v interface{}, keyConvertMap map[string]string) error {
//	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
//	if !ok {
//		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
//	}
//
//	//keyConvertedSliceMap := make([]map[string]interface{}, len(sliceMap))
//	//for k, v := range keyConvertMap {
//	// todo write this key conversion logic
//	//}
//
//	jsonStr, err := json.Marshal(sliceMap)
//	if err != nil {
//		log.Errorf(impulseCtx, "error marshaling slice map %v", err)
//		return err
//	}
//
//	err = json.Unmarshal(jsonStr, &v)
//	if err != nil {
//		log.Errorf(impulseCtx, "error unmarshaling slice map %v", err)
//		return err
//	}
//
//	return nil
//}

func CustomDecode(input, output interface{}) error {
	config := &mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			UUIDToStringHookFunc(),
		),
		Result: &output,
	}

	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(input)
}

func UUIDToStringHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f != reflect.TypeOf(gocql.UUID{}) {
			return data, nil
		}
		if t.Kind() != reflect.String {
			return data, nil
		}

		return data.(gocql.UUID).String(), nil
	}
}
