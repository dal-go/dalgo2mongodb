package dalgo2mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type contextFixture struct {
	uri        string
	admin      *mongo.Client
	database   string
	collection string
}

// Only the explicitly supplied disposable loopback fixture is admitted. The
// coordinator starts pinned mongod inside the capped namespace, never this test.
func ownedContextFixture(t *testing.T) contextFixture {
	t.Helper()
	return ownedContextFixtureWithMonitor(t, nil)
}

func ownedContextFixtureWithMonitor(t *testing.T, monitor *event.CommandMonitor) contextFixture {
	t.Helper()
	raw := os.Getenv("MONGODB_TEST_URI")
	if raw == "" {
		t.Skip("set MONGODB_TEST_URI to disposable loopback dalgo_mongo_test")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Path != "/dalgo_mongo_test" || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" && parsed.Hostname() != "::1") {
		t.Fatal("context fixture requires loopback /dalgo_mongo_test")
	}
	query := parsed.Query()
	query.Set("appName", "dalgo-go-j4-owned-fixture")
	parsed.RawQuery = query.Encode()
	adminURI := os.Getenv("MONGODB_FIXTURE_ADMIN_URI")
	if adminURI == "" {
		adminURI = raw
	}
	adminURL, err := url.Parse(adminURI)
	if err != nil || adminURL.Path != "/dalgo_mongo_test" || adminURL.Host != parsed.Host {
		t.Fatal("fixture admin must select the same owned endpoint/database")
	}
	admin, err := mongo.Connect(options.Client().ApplyURI(adminURI).SetMonitor(monitor).SetMaxPoolSize(1).SetMaxConnecting(1).SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatal("fixture admin connection configuration failed")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := admin.Disconnect(ctx); err != nil {
			t.Errorf("fixture admin cleanup: %v", err)
		}
	})
	collection := fmt.Sprintf("j4_context_%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	coll := admin.Database("dalgo_mongo_test").Collection(collection)
	if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "existing"}, {Key: "name", Value: "existing application document"}}); err != nil {
		t.Fatal("fixture seed failed")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := coll.Drop(ctx); err != nil {
			t.Errorf("owned collection cleanup: %v", err)
		}
	})
	return contextFixture{uri: parsed.String(), admin: admin, database: "dalgo_mongo_test", collection: collection}
}

func fixtureQuery(collection string, empty bool) dal.Query {
	id := "existing"
	if empty {
		id = "missing"
	}
	return dal.From(dal.NewRootCollectionRef(collection, "")).NewQuery().WhereField("_id", dal.Equal, id).Limit(1).SelectIntoRecord(nil)
}

func closeFixtureHandle(t *testing.T, db dal.DB) {
	t.Helper()
	if err := db.(*handle).Close(); err != nil {
		t.Errorf("fixture provider close: %v", err)
	}
}

// Driver messages can contain commands, addresses, or user cancellation causes.
// Fixture diagnostics need only a safe type label and a server error code.
func fixtureCommandFailure(err error) string {
	var command mongo.CommandError
	if errors.As(err, &command) {
		return fmt.Sprintf("command-error(code=%d)", command.Code)
	}
	return fmt.Sprintf("%T", err)
}

func TestMongoLateOpenSuccessIntegration(t *testing.T) {
	fixture := ownedContextFixture(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	var completed, poolClosed atomic.Bool
	monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, command *event.CommandSucceededEvent) {
		if command.CommandName == "ping" {
			completed.Store(true)
			cancel(errors.New("synthetic-secret-cause"))
		}
	}}
	pool := &event.PoolMonitor{Event: func(value *event.PoolEvent) {
		if value.Type == event.ConnectionPoolClosed {
			poolClosed.Store(true)
		}
	}}
	db, err := newDB(ctx, fixture.uri, options.Client().SetMonitor(monitor).SetPoolMonitor(pool).SetMaxPoolSize(1).SetMaxConnecting(1))
	if db != nil {
		closeFixtureHandle(t, db)
	}
	if !completed.Load() || db != nil || err != context.Canceled || !poolClosed.Load() {
		t.Fatalf("late Ping success leaked a handle: completed=%v db=%v error=%v closed=%v", completed.Load(), db, err, poolClosed.Load())
	}
}

func TestMongoLateFindCompletionIntegration(t *testing.T) {
	fixture := ownedContextFixture(t)
	for _, point := range []bool{false, true} {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("point=%v/empty=%v", point, empty), func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				var completed atomic.Bool
				monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, command *event.CommandSucceededEvent) {
					if command.CommandName == "find" {
						completed.Store(true)
						cancel(errors.New("synthetic-private-query-and-password"))
					}
				}}
				db, err := newDB(context.Background(), fixture.uri, options.Client().SetMonitor(monitor).SetMaxPoolSize(1).SetMaxConnecting(1))
				if err != nil {
					t.Fatal(err)
				}
				defer closeFixtureHandle(t, db)
				if point {
					id := "existing"
					if empty {
						id = "missing"
					}
					r := record.NewRecordWithData(record.NewKeyWithID(fixture.collection, id), map[string]any{})
					err = db.Get(ctx, r)
					if r.Error() != context.Canceled {
						t.Fatalf("point read published stale success/not-found state: %v", r.Error())
					}
				} else {
					reader, failure := db.ExecuteQueryToRecordsReader(ctx, fixtureQuery(fixture.collection, empty))
					if reader != nil {
						_ = reader.Close()
						t.Fatal("canceled query exposed a reader")
					}
					err = failure
				}
				if !completed.Load() || err != context.Canceled {
					t.Fatalf("late find completion not canceled: completed=%v error=%v", completed.Load(), err)
				}
			})
		}
	}
}

type mappingCallbackTarget struct {
	document map[string]any
	after    func()
}

type completionCallbackRecord struct {
	record.Record
	onSuccess func()
}

func (r completionCallbackRecord) SetError(err error) record.Record {
	r.Record.SetError(err)
	if err == nil {
		r.onSuccess()
	}
	return r
}

func (v *mappingCallbackTarget) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &v.document)
	if v.after != nil {
		v.after()
	}
	return err
}

func TestMongoPointMappingCancellationAndRetryIntegration(t *testing.T) {
	fixture := ownedContextFixture(t)
	db, err := NewDB(context.Background(), fixture.uri)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFixtureHandle(t, db)
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%v", deadline), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			want := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
				want = context.DeadlineExceeded
			}
			defer cancel()
			mapped := false
			target := &mappingCallbackTarget{after: func() {
				mapped = true
				if deadline {
					<-ctx.Done()
				} else {
					cancel()
				}
			}}
			r := record.NewRecordWithData(record.NewKeyWithID(fixture.collection, "existing"), target)
			err := db.Get(ctx, r)
			if !mapped || err != want || r.Error() != want {
				t.Fatalf("late mapping success: mapped=%v error=%v recordError=%v", mapped, err, r.Error())
			}
			target.after = nil
			if err := db.Get(context.Background(), r); err != nil || r.Error() != nil || !r.Exists() {
				t.Fatalf("same record retry failed: %v recordError=%v", err, r.Error())
			}
			if target.document["name"] != "existing application document" {
				t.Fatal("retry changed native application data")
			}
		})
	}
	t.Run("record completion callback", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		mapped := false
		target := &mappingCallbackTarget{after: func() { mapped = true }}
		r := completionCallbackRecord{
			Record: record.NewRecordWithData(record.NewKeyWithID(fixture.collection, "existing"), target),
			onSuccess: func() {
				if mapped {
					cancel(errors.New("synthetic-secret-completion-cause"))
				}
			},
		}
		if err := db.Get(ctx, r); !mapped || err != context.Canceled || r.Error() != context.Canceled {
			t.Fatalf("record completion exposed late success: mapped=%v error=%v recordError=%v", mapped, err, r.Error())
		}
	})
}

func TestMongoOwnedAfterDispatchCancellationAndReopen(t *testing.T) {
	if os.Getenv("MONGODB_OWNED_FAILPOINTS") != "1" {
		t.Skip("requires root-owned mongod with enableTestCommands=1; no external service assumed")
	}
	var waitMaxTimeFields atomic.Int32
	waitMaxTimeFields.Store(-1)
	monitor := &event.CommandMonitor{Started: func(_ context.Context, command *event.CommandStartedEvent) {
		if command.CommandName != "waitForFailPoint" {
			return
		}
		elements, err := command.Command.Elements()
		if err != nil {
			waitMaxTimeFields.Store(-2)
			return
		}
		var count int32
		for _, element := range elements {
			if element.Key() == "maxTimeMS" {
				count++
			}
		}
		waitMaxTimeFields.Store(count)
	}}
	fixture := ownedContextFixtureWithMonitor(t, monitor)
	db, err := NewDB(context.Background(), fixture.uri)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFixtureHandle(t, db)
	admin := fixture.admin.Database("admin")
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%v", deadline), func(t *testing.T) {
			var configured struct {
				Count int64 `bson:"count"`
			}
			control, stopControl := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopControl()
			err := admin.RunCommand(control, bson.D{
				{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: bson.D{{Key: "times", Value: 1}}},
				{Key: "data", Value: bson.D{{Key: "failCommands", Value: bson.A{"find"}}, {Key: "appName", Value: "dalgo-go-j4-owned-fixture"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 1000}}},
			}).Decode(&configured)
			if err != nil {
				t.Fatalf("owned failpoint configuration failed: %s", fixtureCommandFailure(err))
			}
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := admin.RunCommand(cleanup, bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"}}).Err(); err != nil {
					t.Errorf("owned failpoint cleanup: %s", fixtureCommandFailure(err))
				}
			})
			ctx, cancel := context.WithCancelCause(context.Background())
			want := context.Canceled
			var stopDeadline context.CancelFunc
			if deadline {
				cancel(nil)
				var timed context.Context
				timed, stopDeadline = context.WithTimeout(context.Background(), 500*time.Millisecond)
				ctx, cancel = context.WithCancelCause(timed)
				defer stopDeadline()
				want = context.DeadlineExceeded
			}
			defer cancel(nil)
			done := make(chan error, 1)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				reader, err := db.ExecuteQueryToRecordsReader(ctx, fixtureQuery(fixture.collection, false))
				if reader != nil {
					_ = reader.Close()
				}
				done <- err
			}()
			t.Cleanup(func() {
				cancel(nil)
				select {
				case <-finished:
				case <-time.After(2 * time.Second):
					t.Error("owned read did not finish during cleanup")
				}
			})
			// RunCommand appends maxTimeMS from this deadline. Supplying it in
			// the command too produces duplicate fields, so let the driver emit
			// exactly one server-side limit within the same two-second budget.
			wait, stopWait := context.WithTimeout(control, 2*time.Second)
			waitMaxTimeFields.Store(-1)
			err = admin.RunCommand(wait, bson.D{{Key: "waitForFailPoint", Value: "failCommand"}, {Key: "timesEntered", Value: configured.Count + 1}}).Err()
			stopWait()
			if count := waitMaxTimeFields.Load(); count != 1 {
				cancel(nil)
				t.Fatalf("owned wait wire command must contain one maxTimeMS field, got %d", count)
			}
			if err != nil {
				cancel(nil)
				// The registered cleanup already joins finished with a two-second
				// bound. Do not wait unboundedly on the cancellation under test.
				t.Fatalf("read did not enter the owned MongoDB failpoint: %s", fixtureCommandFailure(err))
			}
			if !deadline {
				cancel(errors.New("synthetic-secret-caller-cause"))
			}
			select {
			case err := <-done:
				if err != want || strings.Contains(err.Error(), "synthetic-secret") {
					t.Fatalf("wrong safe cancellation result: %v", err)
				}
			case <-time.After(750 * time.Millisecond):
				t.Fatal("dispatched read did not promptly cancel")
			}
			// A new read on the same handle succeeds without waiting for or
			// claiming an independently observed remote kill of the blocked read.
			r := record.NewRecordWithData(record.NewKeyWithID(fixture.collection, "existing"), map[string]any{})
			if err := db.Get(context.Background(), r); err != nil {
				t.Fatalf("read after cancellation failed: %v", err)
			}
		})
	}
}
