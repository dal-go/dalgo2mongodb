package dalgo2mongodb

import (
	"context"
	"errors"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func query() *dal.QueryBuilder { return dal.From(dal.NewRootCollectionRef("products", "")).NewQuery() }
func TestPlan(t *testing.T) {
	p, err := makePlan(query().WhereField("price", dal.GreaterOrEqual, 10).OrderBy(dal.DescendingField("price")).Limit(2).SelectColumns(dal.Column{Expression: dal.Field("name")}))
	if err != nil {
		t.Fatal(err)
	}
	if p.limit != 2 || p.sort[0].Value != -1 || !reflect.DeepEqual(p.filter, bson.D{{Key: "price", Value: bson.D{{Key: "$gte", Value: 10}}}}) {
		t.Fatalf("bad plan %#v", p)
	}
	for _, q := range []dal.Query{
		query().WhereField("$where", dal.Equal, "script").SelectIntoRecord(nil),
		query().WhereField("a.b", dal.Equal, "nested").SelectIntoRecord(nil),
		query().Where(dal.Comparison{Left: dal.Field("x"), Operator: dal.Equal, Right: dal.Constant{Value: bson.M{"$gt": 0}}}).SelectIntoRecord(nil),
		query().Limit(1001).SelectIntoRecord(nil),
		query().Offset(-1).SelectIntoRecord(nil),
		query().SelectColumns(dal.Column{Alias: "alias", Expression: dal.Field("name")}),
	} {
		if _, err := makePlan(q); !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("unsafe plan accepted: %v", q)
		}
	}
}
func TestInvalidURINeverLeaks(t *testing.T) {
	for _, uri := range []string{"mongodb://secret:password@host/", "mongodb://host/db/extra", "http://secret:password@host/db"} {
		_, err := NewDB(context.Background(), uri)
		if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "password") {
			t.Fatal(err)
		}
	}
}
func TestMongoIntegration(t *testing.T) {
	uri := os.Getenv("MONGODB_TEST_URI")
	if uri == "" {
		t.Skip("set MONGODB_TEST_URI to run against a real MongoDB server")
	}
	parsed, err := url.Parse(uri)
	if err != nil || parsed.Path != "/dalgo_mongo_test" {
		t.Fatal("integration URI must select disposable dalgo_mongo_test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()
	// The dedicated test database is disposable and never shared with user data.
	coll := client.Database("dalgo_mongo_test").Collection("products")
	defer func() { _ = coll.Drop(context.Background()) }()
	_, err = coll.InsertMany(ctx, []any{bson.M{"_id": "a", "name": "tea", "price": 10}, bson.M{"_id": "b", "name": "coffee", "price": 20}, bson.M{"_id": "c", "name": "water", "price": 5}})
	if err != nil {
		t.Fatal(err)
	}
	db, err := NewDB(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.(*handle).Close() }()
	q := query().WhereField("price", dal.GreaterOrEqual, 10).OrderBy(dal.DescendingField("price")).Limit(1).SelectColumns(dal.Column{Expression: dal.Field("name")})
	reader, err := db.ExecuteQueryToRecordsReader(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	row, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(row.Data(), map[string]any{"name": "coffee"}) {
		t.Fatalf("unexpected row %#v", row.Data())
	}
	if _, err = reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
		t.Fatalf("limit not enforced: %v", err)
	}
	found, err := db.Exists(ctx, record.NewKeyWithID("products", "a"))
	if err != nil || !found {
		t.Fatal(found, err)
	}
	found, err = db.Exists(ctx, record.NewKeyWithID("products", "missing"))
	if err != nil || found {
		t.Fatal(found, err)
	}
	if err = db.RunReadwriteTransaction(ctx, func(context.Context, dal.ReadwriteTransaction) error { t.Fatal("write callback invoked"); return nil }); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatal(err)
	}
	count, err := coll.CountDocuments(ctx, bson.D{})
	if err != nil || count != 3 {
		t.Fatal(count, err)
	}
}
