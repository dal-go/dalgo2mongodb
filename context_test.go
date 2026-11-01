package dalgo2mongodb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func memoryCursor(t *testing.T, documents []any, resultErr error) bsonCursor {
	t.Helper()
	cursor, err := mongo.NewCursorFromDocuments(documents, resultErr, nil)
	if err != nil {
		t.Fatal(err)
	}
	return bsonCursor{cursor}
}

type closeCallbackCursor struct {
	bsonCursor
	afterClose func(context.Context)
}

func (c closeCallbackCursor) Close(ctx context.Context) error {
	err := c.bsonCursor.Close(ctx)
	c.afterClose(ctx)
	return err
}

func TestCanceledOperationsDoNotConnectOrExposeCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("synthetic-password-and-query-data"))
	db, err := NewDB(ctx, "mongodb://user:secret@127.0.0.1:1/dalgo_mongo_test")
	if db != nil || err != context.Canceled {
		t.Fatalf("canceled open: db=%v error=%v", db, err)
	}
	// A nil driver makes an accidental network path fail immediately. The
	// already-canceled public operations must return without touching it.
	backend := &database{}
	r := record.NewRecordWithData(record.NewKeyWithID("products", "key"), map[string]any{"old": "data"})
	if err := backend.Get(ctx, r); err != context.Canceled || r.Error() != context.Canceled {
		t.Fatalf("canceled Get state: error=%v recordError=%v", err, r.Error())
	}
	if exists, err := backend.Exists(ctx, r.Key()); exists || err != context.Canceled {
		t.Fatalf("canceled Exists: %v %v", exists, err)
	}
	if err := backend.GetMulti(ctx, nil); err != context.Canceled {
		t.Fatalf("canceled empty GetMulti: %v", err)
	}
	if reader, err := backend.ExecuteQueryToRecordsReader(ctx, query().Limit(1).SelectIntoRecord(nil)); reader != nil || err != context.Canceled {
		t.Fatalf("canceled query: reader=%v error=%v", reader, err)
	}
}

func TestQueryFinalGuardRunsAfterCursorCleanup(t *testing.T) {
	for _, empty := range []bool{false, true} {
		for _, deadline := range []bool{false, true} {
			t.Run(strings.Join([]string{map[bool]string{true: "empty", false: "row"}[empty], map[bool]string{true: "deadline", false: "caller"}[deadline]}, "/"), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				want := context.Canceled
				if deadline {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
					want = context.DeadlineExceeded
				}
				defer cancel()
				documents := []any{}
				if !empty {
					documents = append(documents, bson.D{{Key: "_id", Value: "key"}, {Key: "name", Value: "existing"}})
				}
				closed := false
				cursor := closeCallbackCursor{bsonCursor: memoryCursor(t, documents, nil), afterClose: func(context.Context) {
					closed = true
					if deadline {
						<-ctx.Done()
					} else {
						cancel()
					}
				}}
				reader, err := collectRecords(ctx, plan{collection: "products"}, cursor)
				if !closed || reader != nil || err != want {
					t.Fatalf("late cleanup success: closed=%v reader=%v error=%v", closed, reader, err)
				}
			})
		}
	}
}

func TestQueryRetainsOnlySafeContextSentinelsFromDriverErrors(t *testing.T) {
	for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
		driverErr := errors.Join(errors.New("synthetic-mongodb-secret-uri"), want)
		cursor := memoryCursor(t, nil, driverErr)
		reader, err := collectRecords(context.Background(), plan{collection: "products"}, cursor)
		if reader != nil || err != want {
			t.Fatalf("driver cancellation classification: reader=%v error=%v", reader, err)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatal("driver error leaked")
		}
	}
	cursor := memoryCursor(t, nil, errors.New("synthetic-mongodb-secret-uri"))
	reader, err := collectRecords(context.Background(), plan{collection: "products"}, cursor)
	if reader != nil || err == nil || err.Error() != "mongodb: query interrupted or failed" {
		t.Fatalf("unsealed driver error: reader=%v error=%v", reader, err)
	}
}

func TestQueryCursorRefactorPreservesNativeIdentityAndProjection(t *testing.T) {
	id := bson.NewObjectID()
	cursor := memoryCursor(t, []any{bson.D{{Key: "_id", Value: id}, {Key: "count", Value: int64(9007199254740993)}}}, nil)
	reader, err := collectRecords(context.Background(), plan{collection: "products", hideID: true}, cursor)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	row, err := reader.Next()
	if err != nil || row.Key().ID != id {
		t.Fatalf("native ID changed: %v %v", row, err)
	}
	data, ok := row.Data().(map[string]any)
	if !ok || data["count"] != int64(9007199254740993) {
		t.Fatalf("native BSON changed: %#v", row.Data())
	}
	if _, exists := data["_id"]; exists {
		t.Fatal("projection exposed omitted identity field")
	}
	if _, err := reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
		t.Fatalf("reader end changed: %v", err)
	}
}
