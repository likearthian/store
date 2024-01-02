package store

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type firestoreRepository[T any] struct {
	docRepository
	db            *firestore.Client
	docIDFieldPos int
}

func CreateFirestoreRepository[T any](db *firestore.Client, options ...RepositoryOption[T]) (DocRepository[string, T], error) {
	opt := &option[T]{}
	for _, op := range options {
		op(opt)
	}

	var entity T

	mtyp := reflect.TypeOf(entity)
	if mtyp.Kind() == reflect.Ptr {
		mtyp = mtyp.Elem()
	}

	var docIDFieldPos int = -1
	var collectionPath string
	var modelTags = make(map[string]int)
	for i := 0; i < mtyp.NumField(); i++ {
		field := mtyp.Field(i)
		tag := field.Tag.Get("firestore")
		if tag == "__doc_id__" {
			docIDFieldPos = i
			continue
		}

		if field.Name == "DBTable" {
			collectionPath = field.Tag.Get("name")
			continue
		}

		modelTags[strings.ToLower(strings.TrimSpace(tag))] = i
	}

	return &firestoreRepository[T]{
		db:            db,
		docIDFieldPos: docIDFieldPos,
		docRepository: docRepository{
			Name:      collectionPath,
			modelTags: modelTags,
			modelType: mtyp,
			tableDef:  DocTableDef{collectionPath},
		},
	}, nil
}

func (r *firestoreRepository[T]) Get(ctx context.Context, id string, dest *T, opts ...QueryOption) error {
	snap, err := r.db.Collection(r.tableDef.Collection).Doc(id).Get(ctx)
	if err != nil && status.Code(err) != codes.NotFound {
		return err
	}

	if !snap.Exists() {
		return ErrKeynotFound
	}

	return parseFirestoreDoc(dest, snap, r.docIDFieldPos)
}

func (r *firestoreRepository[T]) Select(ctx context.Context, filter map[string]any, dest *[]T, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	fsFilter, err := filterMapToFirestoreFilter(filter, r.modelTags)
	if err != nil {
		return err
	}

	qry := r.db.Collection(r.tableDef.FullTableName()).Query
	for _, f := range fsFilter {
		qry = qry.WhereEntity(f)
	}

	iter := qry.Documents(ctx)
	defer iter.Stop()
	for {
		doc, err := iter.Next()
		if err != nil {
			break
		}

		var data T
		if err := parseFirestoreDoc(&data, doc, r.docIDFieldPos); err != nil {
			return err
		}
		*dest = append(*dest, data)
	}

	return nil
}

func (r *firestoreRepository[T]) Iterator(ctx context.Context, filterMap map[string]any, options ...QueryOption) (RowIterator[T], error) {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	fsFilter, err := filterMapToFirestoreFilter(filterMap, r.modelTags)
	if err != nil {
		return nil, err
	}

	qry := r.db.Collection(r.tableDef.FullTableName()).Query
	for _, f := range fsFilter {
		qry = qry.WhereEntity(f)
	}

	iter := qry.Documents(ctx)
	return &firestoreIterator[T]{iter: iter, docIDFieldPos: r.docIDFieldPos}, nil
}

func (r *firestoreRepository[T]) SQLQuery(ctx context.Context, dest any, sqlStr string, args []interface{}, options ...QueryOption) error {
	return fmt.Errorf("database does not support SQLQuery")
}

func (r *firestoreRepository[T]) SQLExec(ctx context.Context, sqlStr string, args []interface{}, options ...QueryOption) error {
	return fmt.Errorf("database does not support SQLExec")
}

func (r *firestoreRepository[T]) Insert(ctx context.Context, value T, options ...QueryOption) (string, error) {
	var key string
	if r.docIDFieldPos < 0 {
		return key, fmt.Errorf("failed to insert document: document ID field not found")
	}

	mval := reflect.ValueOf(value)
	docIDField := mval.Type().Field(r.docIDFieldPos)
	if docIDField.Type.Kind() != reflect.String {
		return key, fmt.Errorf("failed to insert document: document ID field is not string")
	}

	key = mval.Field(r.docIDFieldPos).String()

	if _, err := r.db.Collection(r.tableDef.FullTableName()).Doc(key).Create(ctx, value); err != nil {
		return key, err
	}

	return key, nil
}

func (r *firestoreRepository[T]) InsertAll(ctx context.Context, values []T, options ...QueryOption) ([]string, error) {
	return nil, fmt.Errorf("not implemented yet")
}

func (r *firestoreRepository[T]) Replace(ctx context.Context, id string, value T, options ...QueryOption) error {
	return fmt.Errorf("not implemented yet")
}

func (r *firestoreRepository[T]) Update(ctx context.Context, id string, keyvals map[string]any, options ...QueryOption) error {
	return fmt.Errorf("not implemented yet")
}

func (r *firestoreRepository[T]) Upsert(ctx context.Context, id string, value T, options ...QueryOption) error {
	return fmt.Errorf("not implemented yet")
}

func (r *firestoreRepository[T]) UpsertAll(ctx context.Context, values []T, options ...QueryOption) error {
	return fmt.Errorf("not implemented yet")
}

func (r *firestoreRepository[T]) Delete(ctx context.Context, id []string, options ...QueryOption) error {
	return fmt.Errorf("not implemented yet")
}

func (r *firestoreRepository[T]) Begin(ctx context.Context) (Transaction, error) {
	return nil, fmt.Errorf("database does not support shareable transaction")
}

func filterMapToFirestoreFilter(filterMap map[string]any, modelTags ...map[string]int) ([]firestore.EntityFilter, error) {
	var filters []firestore.EntityFilter
	for k, v := range filterMap {
		if len(modelTags) > 0 {
			mtags := modelTags[0]
			if _, ok := mtags[k]; !ok {
				return nil, fmt.Errorf("invalid filter key: %s", k)
			}
		}

		filters = append(filters, firestore.PropertyFilter{Path: k, Operator: "==", Value: v})
	}

	return filters, nil
}
