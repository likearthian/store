package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongoOptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type mongoRepository[K comparable, T Model] struct {
	repository
	db         *mongo.Database
	collection *mongo.Collection
}

func CreateMongoRepository[K comparable, T Model](db *mongo.Database, collName string, options ...RepositoryOption) (Repository[K, T], error) {
	opt := &option{}
	for _, op := range options {
		op(opt)
	}

	if opt.name == "" {
		opt.name = collName
	}

	var model T
	m := reflect.TypeOf(model)
	if m.Kind() == reflect.Ptr {
		m = m.Elem()
	}

	modelTags := createModelTags(m, "bson")

	collection := db.Collection(collName)

	repo := &mongoRepository[K, T]{
		repository: repository{
			Name:      opt.name,
			model:     model,
			modelType: m,
			modelTags: modelTags,
		},
		db:         db,
		collection: collection,
	}

	if opt.initValues != nil {
		if err := repo.init(opt.initValues); err != nil {
			return nil, err
		}
	}

	return repo, nil
}

func (m *mongoRepository[K, T]) init(values any) error {
	dataVal := reflect.ValueOf(values)
	if dataVal.Kind() != reflect.Slice {
		return fmt.Errorf("value to init should be a slice")
	}

	if dataVal.Type().Elem().Kind() != m.modelType.Kind() {
		return fmt.Errorf("values to init should be []%s, got %t", m.modelType.Name(), values)
	}

	for i := 0; i < dataVal.Len(); i++ {
		sval := dataVal.Index(i)
		if _, err := m.Insert(context.Background(), sval.Interface().(T)); err != nil {
			if !errors.Is(err, ErrKeyAlreadyExists) {
				return err
			}
		}
	}

	return nil
}

func (m *mongoRepository[K, T]) GetTableDef() TabledDef {
	return m.model.GetTableDef()
}

func (m *mongoRepository[K, T]) Get(ctx context.Context, id K, dest *T, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	ctx = m.setTransactionContext(ctx, opt)
	tabledef := m.model.GetTableDef()
	filter := bson.D{{Key: tabledef.KeyField, Value: id}}
	err := m.collection.FindOne(ctx, filter).Decode(dest)
	if err != nil {
		return wrapMongoError(err)
	}

	return nil
}

func (m *mongoRepository[K, T]) Select(ctx context.Context, filterMap map[string]any, dest *[]T, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	ctx = m.setTransactionContext(ctx, opt)

	filter := m.parseFilterMapIntoFilter(filterMap)

	cur, err := m.collection.Find(ctx, filter)
	if err != nil {
		return wrapMongoError(err)
	}
	defer cur.Close(ctx)

	if err := cur.All(ctx, dest); err != nil {
		return wrapMongoError(err)
	}

	return nil
}

func (m *mongoRepository[K, T]) SQLQuery(ctx context.Context, dest any, sqlStr string, args []interface{}, options ...QueryOption) error {
	return fmt.Errorf("the database does not support SQL Query")
}

func (m *mongoRepository[K, T]) SQLExec(ctx context.Context, sqlStr string, args []interface{}, options ...QueryOption) error {
	return fmt.Errorf("the database does not support SQL Query")
}

func (m *mongoRepository[K, T]) Insert(ctx context.Context, value T, options ...QueryOption) (K, error) {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	ctx = m.setTransactionContext(ctx, opt)

	var zeroKey K
	res, err := m.collection.InsertOne(ctx, value)
	if err != nil {
		return zeroKey, wrapMongoError(err)
	}

	return res.InsertedID.(K), nil
}

func (m *mongoRepository[K, T]) InsertAll(ctx context.Context, values []T, options ...QueryOption) ([]K, error) {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	ctx = m.setTransactionContext(ctx, opt)

	insertValues := Map(values, func(val T) interface{} {
		return val
	})

	res, err := m.collection.InsertMany(ctx, insertValues)
	if err != nil {
		return nil, wrapMongoError(err)
	}

	ids := Map(res.InsertedIDs, func(val interface{}) K {
		return val.(K)
	})

	return ids, nil
}

func (m *mongoRepository[K, T]) Replace(ctx context.Context, id K, value T, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	ctx = m.setTransactionContext(ctx, opt)

	tabledef := m.model.GetTableDef()
	up, err := m.collection.ReplaceOne(ctx, bson.D{{Key: tabledef.KeyField, Value: id}}, value)
	if err != nil {
		return wrapMongoError(err)
	}

	if up.MatchedCount == 0 {
		return ErrKeynotFound
	}

	return nil
}

func (m *mongoRepository[K, T]) Update(ctx context.Context, id K, keyvals map[string]any, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	ctx = m.setTransactionContext(ctx, opt)

	up, err := m.collection.UpdateByID(ctx, id, m.createUpdateParam(keyvals))
	if err != nil {
		return wrapMongoError(err)
	}

	if up.MatchedCount == 0 {
		return ErrKeynotFound
	}

	return nil
}

func (m *mongoRepository[K, T]) Upsert(ctx context.Context, id K, value T, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	ctx = m.setTransactionContext(ctx, opt)

	tabledef := m.model.GetTableDef()
	_, err := m.collection.ReplaceOne(ctx, bson.D{{Key: tabledef.KeyField, Value: id}}, value, mongoOptions.Replace().SetUpsert(true))
	if err != nil {
		return wrapMongoError(err)
	}

	return nil
}

func (m *mongoRepository[K, T]) Delete(ctx context.Context, id []K, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	ctx = m.setTransactionContext(ctx, opt)

	tabledef := m.model.GetTableDef()
	res, err := m.collection.DeleteMany(ctx, bson.D{{tabledef.KeyField, bson.M{"$in": id}}})
	if err != nil {
		return wrapMongoError(err)
	}

	fmt.Println("Deleted:", res.DeletedCount)
	return nil
}

func (m *mongoRepository[K, T]) Begin(ctx context.Context) (Transaction, error) {
	session, err := m.db.Client().StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create mongodb session. %s", err.Error())
	}

	sctx := mongo.NewSessionContext(ctx, session)

	wc := writeconcern.New(writeconcern.WMajority())
	rc := readconcern.Snapshot()
	txnOpts := mongoOptions.Transaction().SetWriteConcern(wc).SetReadConcern(rc)
	opts := []*mongoOptions.TransactionOptions{txnOpts}

	if err := session.StartTransaction(opts...); err != nil {
		return nil, err
	}

	return &mongoTransaction{
		session: session,
		sctx:    sctx,
	}, nil
}

func (m *mongoRepository[K, T]) createUpdateParam(keyvals map[string]interface{}) interface{} {
	update := bson.M{}
	for k, v := range keyvals {
		update[k] = v
	}

	return bson.M{"$set": update}
}

func (m *mongoRepository[K, T]) ParseRequestQueryIntoFilter(req interface{}) (interface{}, error) {
	reqType := reflect.TypeOf(req)
	reqVal := reflect.ValueOf(req)
	if reqType.Kind() == reflect.Ptr {
		reqType = reqType.Elem()
		reqVal = reqVal.Elem()
	}

	var filter = bson.D{}

	for i := 0; i < reqType.NumField(); i++ {
		field := reqType.Field(i)
		tag := field.Tag.Get("db")
		if tag == "" || !m.modelTagExists(tag) {
			continue
		}

		val := reqVal.Field(i).Interface()
		if reqVal.Field(i).Kind() != reflect.Slice {
			filter = append(filter, bson.E{Key: tag, Value: val})
			continue
		}

		if reqVal.Field(i).Len() > 0 {
			if f, err := m.parameterizedFilterCriteriaSlice(tag, val); err == nil {
				filter = append(filter, f)
			}
		}
	}

	return filter, nil
}

func (m *mongoRepository[K, T]) parseFilterMapIntoFilter(filterMap map[string]any) bson.D {
	var filter = bson.D{}

	for k, v := range filterMap {
		vval := reflect.ValueOf(v)
		val := vval.Interface()
		if vval.Kind() != reflect.Slice {
			filter = append(filter, bson.E{Key: k, Value: val})
			continue
		}

		if vval.Len() > 0 {
			if f, err := m.parameterizedFilterCriteriaSlice(k, val); err == nil {
				filter = append(filter, f)
			}
		}
	}

	return filter
}

func (m *mongoRepository[K, T]) parameterizedFilterCriteriaSlice(fieldname string, values any) (bson.E, error) {
	vtype := reflect.TypeOf(values)
	if vtype.Kind() == reflect.Ptr {
		vtype = vtype.Elem()
	}

	if vtype.Kind() != reflect.Slice {
		return bson.E{}, fmt.Errorf("expecting slice as values, got %s", vtype.Kind().String())
	}

	s := reflect.ValueOf(values)
	if s.Len() == 0 {
		return bson.E{}, fmt.Errorf("cannot use empty slice to parameterized")
	}

	var filter bson.E
	if s.Len() > 1 {
		filter = bson.E{Key: fieldname, Value: bson.M{"$in": values}}
	} else {
		filter = bson.E{Key: fieldname, Value: s.Index(0).Interface()}
	}

	return filter, nil
}

func (m *mongoRepository[K, T]) setTransactionContext(ctx context.Context, opt *queryOption) context.Context {
	if opt.Tx != nil {
		tx, ok := opt.Tx.(*mongoTransaction)
		if ok {
			return tx.sctx
		}
	}

	return ctx
}

func wrapMongoError(err error) error {
	if mongo.IsDuplicateKeyError(err) {
		return ErrKeyAlreadyExists
	}

	errMap := map[error]error{
		mongo.ErrNoDocuments: ErrKeynotFound,
	}

	for g, e := range errMap {
		if errors.Is(err, g) {
			err = fmt.Errorf("%w. %s", e, err.Error())
		}
	}

	return err
}

type mongoTransaction struct {
	session mongo.Session
	sctx    mongo.SessionContext
}

func (tx *mongoTransaction) Rollback(ctx context.Context) error {
	tx.sctx.EndSession(ctx)
	return nil
}

func (tx *mongoTransaction) Commit(ctx context.Context) error {
	return tx.sctx.CommitTransaction(tx.sctx)
}
