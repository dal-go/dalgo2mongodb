package dalgo2mongodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func validName(name string) bool {
	return name != "" && len(name) <= 120 && !strings.ContainsAny(name, ".$\x00")
}
func unsupported() error {
	return fmt.Errorf("%w: mongodb supports top-level fields, scalar comparisons, AND/OR, sorting, projection and limits; no joins, grouping, cursors or native commands", dal.ErrNotSupported)
}

type plan struct {
	collection string
	filter     bson.D
	projection bson.D
	sort       bson.D
	limit      int64
	offset     int64
	hideID     bool
}

func makePlan(query dal.Query) (plan, error) {
	q, ok := query.(dal.StructuredQuery)
	if !ok || q.From() == nil || q.From().Base() == nil || len(q.From().Joins()) != 0 || len(q.GroupBy()) != 0 || q.Having() != nil || q.StartFrom() != "" || q.StartAfter() != "" {
		return plan{}, unsupported()
	}
	collection := q.From().Base()
	ref, ok := collection.(dal.CollectionRef)
	if !ok || ref.Parent() != nil || ref.Schema() != "" || ref.Database() != "" || collection.Alias() != "" {
		return plan{}, unsupported()
	}
	if !validName(collection.Name()) || q.Offset() < 0 || q.Offset() > 100000 || q.Limit() < 0 || q.Limit() > MaxResults {
		return plan{}, unsupported()
	}
	p := plan{collection: collection.Name(), limit: int64(q.Limit()), offset: int64(q.Offset())}
	if p.limit == 0 {
		p.limit = MaxResults
	}
	var err error
	budget := 0
	p.filter, err = condition(q.Where(), 0, &budget)
	if err != nil {
		return plan{}, err
	}
	for _, c := range q.Columns() {
		f, ok := c.Expression.(dal.FieldRef)
		if !ok || f.Source() != "" || !validName(f.Name()) || (c.Alias != "" && c.Alias != f.Name()) {
			return plan{}, unsupported()
		}
		p.projection = append(p.projection, bson.E{Key: f.Name(), Value: 1})
	}
	if len(p.projection) > 0 {
		hasID := false
		for _, e := range p.projection {
			if e.Key == "_id" {
				hasID = true
			}
		}
		if !hasID {
			p.hideID = true
		}
	}
	for _, o := range q.OrderBy() {
		f, ok := o.Expression().(dal.FieldRef)
		if !ok || f.Source() != "" || !validName(f.Name()) {
			return plan{}, unsupported()
		}
		direction := 1
		if o.Descending() {
			direction = -1
		}
		p.sort = append(p.sort, bson.E{Key: f.Name(), Value: direction})
	}
	return p, nil
}
func condition(c dal.Condition, depth int, budget *int) (bson.D, error) {
	*budget++
	if depth > 16 || *budget > 128 {
		return nil, unsupported()
	}
	if c == nil {
		return bson.D{}, nil
	}
	switch c := c.(type) {
	case dal.GroupCondition:
		op := ""
		switch c.Operator() {
		case dal.And:
			op = "$and"
		case dal.Or:
			op = "$or"
		default:
			return nil, unsupported()
		}
		if len(c.Conditions()) == 0 {
			return nil, unsupported()
		}
		children := bson.A{}
		for _, child := range c.Conditions() {
			if child == nil {
				return nil, unsupported()
			}
			f, err := condition(child, depth+1, budget)
			if err != nil {
				return nil, err
			}
			children = append(children, f)
		}
		return bson.D{{Key: op, Value: children}}, nil
	case dal.Comparison:
		field, ok := c.Left.(dal.FieldRef)
		if !ok || field.Source() != "" || !validName(field.Name()) {
			return nil, unsupported()
		}
		constant, ok := c.Right.(dal.Constant)
		if !ok {
			return nil, unsupported()
		}
		switch constant.Value.(type) {
		case nil, bool, string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, time.Time:
		default:
			return nil, unsupported()
		}
		op := ""
		switch c.Operator {
		case dal.Equal:
			op = "$eq"
		case dal.GreaterThen:
			op = "$gt"
		case dal.GreaterOrEqual:
			op = "$gte"
		case dal.LessThen:
			op = "$lt"
		case dal.LessOrEqual:
			op = "$lte"
		default:
			return nil, unsupported()
		}
		return bson.D{{Key: field.Name(), Value: bson.D{{Key: op, Value: constant.Value}}}}, nil
	default:
		return nil, unsupported()
	}
}
func (d *database) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	p, err := makePlan(query)
	if err != nil {
		return nil, err
	}
	opts := options.Find().SetLimit(p.limit).SetSkip(p.offset)
	if len(p.projection) > 0 {
		opts.SetProjection(p.projection)
	}
	if len(p.sort) > 0 {
		opts.SetSort(p.sort)
	}
	cursor, err := d.db.Collection(p.collection).Find(ctx, p.filter, opts)
	if err != nil {
		return nil, errors.New("mongodb: query failed")
	}
	defer func() { _ = cursor.Close(ctx) }()
	records := make([]record.Record, 0)
	var bytes int
	for cursor.Next(ctx) {
		bytes += len(cursor.Current)
		if bytes > 16<<20 {
			return nil, errors.New("mongodb: query result exceeds 16 MiB")
		}
		var data bson.M
		if err = cursor.Decode(&data); err != nil {
			return nil, errors.New("mongodb: document decoding failed")
		}
		id, exists := data["_id"]
		if !exists || id == nil || !reflect.TypeOf(id).Comparable() {
			return nil, fmt.Errorf("%w: MongoDB record IDs must be non-null comparable BSON scalar values", dal.ErrNotSupported)
		}
		if p.hideID {
			delete(data, "_id")
		}
		records = append(records, record.NewRecordWithData(record.NewKeyWithID(p.collection, id), map[string]any(data)))
	}
	if err = cursor.Err(); err != nil {
		return nil, errors.New("mongodb: query interrupted or failed")
	}
	return dal.NewRecordsReader(records), nil
}
