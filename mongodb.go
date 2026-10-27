// Package dalgo2mongodb provides bounded, read-only MongoDB structured queries.
package dalgo2mongodb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const MaxResults = 1000

type database struct {
	dal.NoConcurrency
	client *mongo.Client
	db     *mongo.Database
}
type handle struct {
	dal.DB
	client *mongo.Client
}

// NewDB opens the database named in uri. Use a MongoDB user with the read role.
// No raw commands, scripts, aggregation pipelines or write operations are exposed.
// Driver errors are deliberately not returned: they can include connection secrets.
func NewDB(ctx context.Context, uri string) (dal.DB, error) {
	u, err := url.Parse(uri)
	if err != nil || (u.Scheme != "mongodb" && u.Scheme != "mongodb+srv") || u.Host == "" || u.Fragment != "" || len(strings.TrimPrefix(u.Path, "/")) == 0 || strings.Contains(strings.TrimPrefix(u.Path, "/"), "/") {
		return nil, errors.New("mongodb: expected mongodb://host/database or mongodb+srv://host/database")
	}
	name := strings.TrimPrefix(u.Path, "/")
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(10 * time.Second).SetTimeout(30 * time.Second))
	if err != nil {
		return nil, errors.New("mongodb: invalid connection settings")
	}
	if err = client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, errors.New("mongodb: connection or authentication failed")
	}
	backend := &database{client: client, db: client.Database(name)}
	return &handle{DB: dal.NewDB(backend), client: client}, nil
}
func (h *handle) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return h.client.Disconnect(ctx)
}
func (d *database) ID() string           { return d.db.Name() }
func (d *database) Adapter() dal.Adapter { return dal.NewAdapter("mongodb", "0.1.0") }
func (*database) Schema() dal.Schema     { return nil }
func (d *database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, opts ...dal.TransactionOption) error {
	return f(ctx, readTx{database: d, options: dal.NewTransactionOptions(opts...)})
}
func (*database) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return fmt.Errorf("%w: mongodb adapter is read-only", dal.ErrNotSupported)
}

type readTx struct {
	*database
	options dal.TransactionOptions
}

func (t readTx) Options() dal.TransactionOptions { return t.options }
func (d *database) Get(ctx context.Context, r record.Record) error {
	if r.Key().Parent() != nil || !validName(r.Key().Collection()) {
		return dal.ErrNotSupported
	}
	var data bson.M
	err := d.db.Collection(r.Key().Collection()).FindOne(ctx, bson.D{{Key: "_id", Value: r.Key().ID}}).Decode(&data)
	if errors.Is(err, mongo.ErrNoDocuments) {
		err = dal.NewErrNotFoundByKey(r.Key(), nil)
		r.SetError(err)
		return err
	}
	if err != nil {
		return errors.New("mongodb: document read failed")
	}
	if err = record.MapToData(r.Data(), map[string]any(data)); err != nil {
		return err
	}
	r.SetError(nil)
	return nil
}
func (d *database) Exists(ctx context.Context, k *record.Key) (bool, error) {
	r := record.NewRecordWithData(k, &map[string]any{})
	err := d.Get(ctx, r)
	if errors.Is(err, record.ErrRecordNotFound) {
		return false, nil
	}
	return err == nil, err
}
func (d *database) GetMulti(ctx context.Context, records []record.Record) error {
	for _, r := range records {
		if err := d.Get(ctx, r); err != nil {
			return err
		}
	}
	return nil
}
func (*database) ExecuteQueryToRecordsetReader(context.Context, dal.Query, ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, dal.ErrNotSupported
}
