// Package dalgo2mongodb provides bounded, read-only MongoDB structured queries.
package dalgo2mongodb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
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
	return newDB(ctx, uri, options.Client())
}

// The private options seam permits official driver monitoring in owned fixtures;
// the public constructor retains the same URI and timeout contract.
func newDB(ctx context.Context, uri string, clientOptions *options.ClientOptions) (dal.DB, error) {
	u, err := url.Parse(uri)
	if err != nil || (u.Scheme != "mongodb" && u.Scheme != "mongodb+srv") || u.Host == "" || u.Fragment != "" || len(strings.TrimPrefix(u.Path, "/")) == 0 || strings.Contains(strings.TrimPrefix(u.Path, "/"), "/") {
		return nil, errors.New("mongodb: expected mongodb://host/database or mongodb+srv://host/database")
	}
	if err := contextFailure(ctx, nil); err != nil {
		return nil, err
	}
	name := strings.TrimPrefix(u.Path, "/")
	client, err := mongo.Connect(clientOptions.ApplyURI(uri).SetServerSelectionTimeout(10 * time.Second).SetTimeout(30 * time.Second))
	if err != nil {
		return nil, sealedReadError(ctx, err, "mongodb: invalid connection settings")
	}
	if err = client.Ping(ctx, nil); err != nil {
		_ = (&handle{client: client}).Close()
		return nil, sealedReadError(ctx, err, "mongodb: connection or authentication failed")
	}
	backend := &database{client: client, db: client.Database(name)}
	opened := &handle{DB: dal.NewDB(backend), client: client}
	if err := contextFailure(ctx, nil); err != nil {
		_ = opened.Close()
		return nil, err
	}
	return opened, nil
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
func (d *database) Get(ctx context.Context, r record.Record) (resultErr error) {
	defer func() {
		if err := contextFailure(ctx, resultErr); err != nil {
			resultErr = err
		}
		r.SetError(resultErr)
		// SetError belongs to the supplied record interface. Do not report a
		// success if its callback cancels while publishing retrieval state.
		if err := contextFailure(ctx, nil); err != nil {
			resultErr = err
			r.SetError(err)
		}
	}()
	if err := contextFailure(ctx, nil); err != nil {
		return err
	}
	if r.Key().Parent() != nil || !validName(r.Key().Collection()) || r.Key().ID == nil || !reflect.TypeOf(r.Key().ID).Comparable() {
		return dal.ErrNotSupported
	}
	var data bson.M
	err := d.db.Collection(r.Key().Collection()).FindOne(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$eq", Value: r.Key().ID}}}}).Decode(&data)
	if cancelled := contextFailure(ctx, err); cancelled != nil {
		return cancelled
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return dal.NewErrNotFoundByKey(r.Key(), nil)
	}
	if err != nil {
		return errors.New("mongodb: document read failed")
	}
	// A record from an earlier failed attempt refuses Data() until its error is
	// cleared. Reset only after this read succeeded; the finalizer publishes the
	// actual result even if mapping or a caller callback cancels afterward.
	r.SetError(nil)
	if err := contextFailure(ctx, nil); err != nil {
		return err
	}
	if err = record.MapToData(r.Data(), map[string]any(data)); err != nil {
		return err
	}
	return nil
}
func (d *database) Exists(ctx context.Context, k *record.Key) (bool, error) {
	r := record.NewRecordWithData(k, &map[string]any{})
	err := d.Get(ctx, r)
	if errors.Is(err, record.ErrRecordNotFound) {
		return false, contextFailure(ctx, nil)
	}
	if cancelled := contextFailure(ctx, err); cancelled != nil {
		return false, cancelled
	}
	return err == nil, err
}
func (d *database) GetMulti(ctx context.Context, records []record.Record) error {
	for _, r := range records {
		if err := d.Get(ctx, r); err != nil {
			return err
		}
	}
	return contextFailure(ctx, nil)
}
func (*database) ExecuteQueryToRecordsetReader(context.Context, dal.Query, ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, dal.ErrNotSupported
}
