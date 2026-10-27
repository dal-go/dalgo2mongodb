# dalgo2mongodb

Read-only MongoDB adapter for [DALgo](https://github.com/dal-go/dalgo), using the official MongoDB Go driver.

```go
ctx := context.Background()
db, err := dalgo2mongodb.NewDB(ctx, os.Getenv("MONGODB_URI"))
// Check err; close db via io.Closer when finished.
q := dal.From(dal.NewRootCollectionRef("products", "")).NewQuery().
    WhereField("price", dal.GreaterOrEqual, 10).
    OrderBy(dal.DescendingField("price")).Limit(20).SelectIntoRecord(nil)
reader, err := db.ExecuteQueryToRecordsReader(ctx, q)
// Check err; consume and close reader.
```

The URI must select a database: `mongodb://host/database` or `mongodb+srv://host/database`. Use a dedicated MongoDB account with the `read` role for that database; credentials stay in environment variables or a secret store. TLS and authentication options are handled by the official driver. The adapter never returns driver error messages that may contain credentials.

Supported queries use one root collection, top-level fields, scalar `==`, `<`, `<=`, `>`, `>=` comparisons, AND/OR groups, field projection, ordering, offset (up to 100000), and a positive limit (at most 1000). Omitted limits default to 1000. Results are additionally bounded to 16 MiB of BSON. MongoDB comparison, missing-field and null semantics apply. `_id` is returned unless a projection omits it. Queried record keys and Get/Exists preserve native comparable BSON scalar IDs (including ObjectID and numbers), even when the projection omits `_id`. Null and non-comparable IDs (such as embedded documents and binary slices) are explicitly unsupported; a string ID is different from an ObjectID. Computed expressions, aliases, qualified/nested fields, joins, grouping, cursors, raw commands and aggregation pipelines are rejected. No query can submit `$where`, `$out` or `$merge`.

Get, GetMulti and Exists are supported. Writes return `dal.ErrNotSupported`. Readonly callbacks are ordinary reads and do not provide snapshot isolation across calls. Cancellation and driver timeouts bound network operations; default server selection is 10 seconds and operation timeout is 30 seconds.

## Tests

`go test ./...` runs unit tests. Real-server tests require a disposable database URI ending in `/dalgo_mongo_test`:

```sh
MONGODB_TEST_URI=mongodb://127.0.0.1:27017/dalgo_mongo_test go test -race ./...
```

The integration test creates and drops `dalgo_mongo_test.products`. Never point it at production. CI runs it against MongoDB 8.0.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
