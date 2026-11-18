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

Caller cancellation and deadlines return the standard `context.Canceled` or
`context.DeadlineExceeded` sentinel, including wrapped driver context failures.
The adapter never returns a driver error or `context.Cause` payload that can
contain credentials or query data. Other driver failures keep fixed redacted
messages. An expired/canceled operation cannot publish a newly opened handle,
successful point-read state, a not-found success, or an empty/materialized query
reader. The query's final check follows cursor cleanup; cursor cleanup uses the
caller context with at most five seconds, not fresh background authority.

Point-read failures set the supplied record's error state. A later successful
read can reuse that record. Mapping into an existing mutable data target is not
an atomic rollback contract: callers must respect the returned/record error.
Driver-local cancellation and cursor cleanup do not establish that the server
independently killed remote work. The existing limits do not claim a total
multi-operation or paged-journey budget, native cursors, metadata discovery or
callback transaction isolation.

## Tests

`go test ./...` runs unit tests. Real-server tests require a disposable database URI ending in `/dalgo_mongo_test`:

```sh
MONGODB_TEST_URI=mongodb://127.0.0.1:27017/dalgo_mongo_test go test -race ./...
```

The integration test creates and drops `dalgo_mongo_test.products`. Never point it at production. CI pins its server version in `.github/workflows/ci.yml`; the separately supplied owned VM fixture targets MongoDB 8.0.17.

The context integration fixtures require a loopback URI selecting that same
disposable database and create/drop only their uniquely named collections.
`MONGODB_FIXTURE_ADMIN_URI` can supply the separate same-endpoint administrative
fixture principal while `MONGODB_TEST_URI` selects a read-only principal. The
original `TestMongoIntegration` still needs its seed/drop permissions; run it
with the administrative fixture URI, and run the context fixtures separately
with the read-only URI. No production account or database is inferred.

The additional server-observed dispatch check runs only when the coordinator
sets `MONGODB_OWNED_FAILPOINTS=1` and starts its owned ephemeral `mongod` with
`enableTestCommands=1`. It uses the existing MongoDB `failCommand` and
`waitForFailPoint` test APIs, then checks caller abort, deadline and a fresh read
on the same handle. In MongoDB 8.0.17 these test APIs bypass authorization;
using a fixture administrator is an ownership rule, not a provider privilege
proof. Do not enable them on an external service. Official driver completion
monitors also exercise late Ping/find success, and a mapping callback exercises
late point-read cancellation and reuse without relying on timer races.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
