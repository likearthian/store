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

type mongoRepository struct {
	repository
	db         *mongo.Database
	collection *mongo.Collection
}

func CreateMongoRepository(db *mongo.Database, collName string, model Model, options ...RepositoryOption) (Repository, error) {
	opt := &option{}
	for _, op := range options {
		op(opt)
	}

	if opt.name == "" {
		opt.name = collName
	}

	m := reflect.TypeOf(model)
	if m.Kind() == reflect.Ptr {
		m = m.Elem()
	}

	modelTags := createModelTags(m, "bson")

	collection := db.Collection(collName)

	repo := &mongoRepository{
		repository: repository{
			Name:      opt.name,
			model:     model,
			tabledef:  model.GetTableDef(),
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

func (m *mongoRepository) init(values interface{}) error {
	dataVal := reflect.ValueOf(values)
	if dataVal.Kind() != reflect.Slice {
		return fmt.Errorf("value to init should be a slice")
	}

	if dataVal.Elem().Kind() != m.modelType.Kind() {
		return fmt.Errorf("values to init should be []%s, got %t", m.modelType.Name(), values)
	}

	for i := 0; i < dataVal.Len(); i++ {
		sval := dataVal.Index(i)
		if _, err := m.Put(context.Background(), sval.Interface()); err != nil {
			return err
		}
	}

	return nil
}

func (m *mongoRepository) Get(ctx context.Context, id interface{}, dest interface{}, options ...DataOption) error {
	opt := &dataOption{}
	for _, op := range options {
		op(opt)
	}

	destType := reflect.TypeOf(dest)

    if destType.Kind() != reflect.Ptr {
        return fmt.Errorf("dest must be a pointer to a %s", m.modelType.Name())
    }

    if destType.Elem() != m.modelType {
        return fmt.Errorf("dest must be a pointer to a %s", m.modelType.Name())
    }

	ctx = m.setTransactionContext(ctx, opt)

	filter := bson.D{{Key: "_id", Value: id}}
	err := m.collection.FindOne(ctx, filter).Decode(dest)
	if err != nil {
		return wrapMongoError(err)
	}

	return nil
}

func (m *mongoRepository) Select(ctx context.Context, filterMap map[string]interface{}, dest interface{}, options ...DataOption) error {
	opt := &dataOption{}
	for _, op := range options {
		op(opt)
	}

	destType := reflect.TypeOf(dest)

    if destType.Kind() != reflect.Ptr {
        return fmt.Errorf("dest must be a pointer to a slice of %s or *%s", m.modelType.Name(), m.modelType.Name())
    }

    if destType.Elem().Kind() != reflect.Slice {
        return fmt.Errorf("dest must be a pointer to a slice of %s or *%s", m.modelType.Name(), m.modelType.Name())
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

func (m *mongoRepository) SQLQuery(ctx context.Context, strSql string, dest interface{}, options ...DataOption) error {
	return fmt.Errorf("the database does not support SQL Query")
}

func (m *mongoRepository) SQLRun(ctx context.Context, strSql string, options ...DataOption) error {
	return fmt.Errorf("the database does not support SQL Query")
}

func (m *mongoRepository) Put(ctx context.Context, value interface{}, options ...DataOption) (interface{}, error) {
	opt := &dataOption{}
	for _, op := range options {
		op(opt)
	}

	ctx = m.setTransactionContext(ctx, opt)

	valTyp := reflect.TypeOf(value)
	if valTyp != m.modelType {
		return nil, fmt.Errorf("cannot put value. value is not a type of %s", m.modelType.Name())
	}

	res, err := m.collection.InsertOne(ctx, value)
	if err != nil {
		return nil, wrapMongoError(err)
	}

	return res.InsertedID, nil
}

func (m *mongoRepository) Replace(ctx context.Context, id interface{}, value interface{}, options ...DataOption) error {
	opt := &dataOption{}
	for _, op := range options {
		op(opt)
	}

	ctx = m.setTransactionContext(ctx, opt)

	valTyp := reflect.TypeOf(value)
	if valTyp.Kind() == reflect.Ptr {
		valTyp = valTyp.Elem()
	}

	if valTyp != m.modelType {
		return fmt.Errorf("cannot update value. value is not a type of %s", m.modelType.Name())
	}

	up, err := m.collection.ReplaceOne(ctx, bson.D{{Key:"_id", Value: id}}, value)
	if err != nil {
		return wrapMongoError(err)
	}

	if up.MatchedCount == 0 {
		return ErrKeynotFound
	}

	return nil
}

func (m *mongoRepository) Update(ctx context.Context, id interface{}, keyvals map[string]interface{}, options ...DataOption) error {
	opt := &dataOption{}
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

func (m *mongoRepository) Upsert(ctx context.Context, id interface{}, value interface{}, options ...DataOption) error {
	opt := &dataOption{}
	for _, op := range options {
		op(opt)
	}

	ctx = m.setTransactionContext(ctx, opt)

	valTyp := reflect.TypeOf(value)
	if valTyp.Kind() == reflect.Ptr {
		valTyp = valTyp.Elem()
	}

	if valTyp != m.modelType {
		return fmt.Errorf("cannot update value. value is not a type of %s", m.modelType.Name())
	}

	_, err := m.collection.ReplaceOne(ctx, bson.D{{Key: "_id", Value: id}}, value, mongoOptions.Replace().SetUpsert(true))
	if err != nil {
		return wrapMongoError(err)
	}

	return nil
}

func (m *mongoRepository) Begin(ctx context.Context) (Transaction, error) {
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

func (m *mongoRepository) createUpdateParam(keyvals map[string]interface{}) interface{} {
	update := bson.M{}
	for k, v := range keyvals {
		update[k] = v
	}

	return bson.M{"$set": update}
}

func (m *mongoRepository) ParseRequestQueryIntoFilter(req interface{}) (interface{}, error) {
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

func (m *mongoRepository) parseFilterMapIntoFilter(filterMap map[string]interface{}) bson.D {
	var filter = bson.D{}

	for k, v := range filterMap {
		if !m.modelTagExists(k) {
			continue
		}

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

func (m *mongoRepository) parameterizedFilterCriteriaSlice(fieldname string, values interface{}) (bson.E, error) {
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

func (m *mongoRepository) setTransactionContext(ctx context.Context, opt *dataOption) context.Context {
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
	sctx mongo.SessionContext
}

func (tx *mongoTransaction) Rollback(ctx context.Context) error {
	tx.session.EndSession(ctx)
	return nil
}

func (tx *mongoTransaction) Commit(ctx context.Context) error {
	return tx.session.CommitTransaction(tx.sctx)
}
