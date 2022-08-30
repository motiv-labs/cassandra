package cassandra

/*
ShardTimeUUID is used to query data across multiple cassandra partitions (shards).
It will query the table using the where clause to get data across all shards and order them by timeUUID.
Params:
	table: the table to query
	where: additional options to pass the query
	shards: the number of shards to iterate queries over
*/
//func (s sessionRetry) ShardTimeUUID(ctx context.Context, table, where, timeUUIDColumn string, shards int) ([]interface{}, error) {
//	impulseCtx, ok := ctx.Value(impulse_ctx.ImpulseCtxKey).(impulse_ctx.ImpulseCtx)
//	if !ok {
//		log.Warnf(impulseCtx, "ImpulseCtx isn't correct type")
//	}
//
//	var lastValues []gocql.UUID
//
//	selectStmt := fmt.Sprintf("SELECT * FROM %s", table)
//
//	var allFoundValues []interface{}
//
//	for i := 1; i < shards; i++ {
//		// build initial where statement with initial shard 1
//		whereStmt := buildWhereStmt(1, where)
//
//		// build query
//		query := fmt.Sprintf("%s %s", selectStmt, whereStmt)
//
//		// get iter
//		iter := s.Query(ctx, query).Iter(ctx)
//
//		// create variables to capture data
//		var foundValues []interface{}
//		var value interface{}
//		// run query
//		err := iter.ScanAndClose(ctx, func() bool {
//			foundValues = append(foundValues, value)
//			return true
//		}, &value)
//
//		if err != nil {
//			log.Errorf(impulseCtx, "error while querying %s table", table)
//			return nil, err
//		}
//
//		// get last value for next set of requests
//
//	}
//
//	log.Debugf(impulseCtx, "successfully returning data for %s table", table)
//
//	return allFoundValues, nil
//}

//func buildWhereStmt(shard int, where string) string {
//	whereStmt := fmt.Sprintf("WHERE shards=1")
//	if where != "" {
//		whereStmt = fmt.Sprintf("%s AND %s", whereStmt, where)
//	}
//	return whereStmt
//}
