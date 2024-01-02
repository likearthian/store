package store

import (
	"context"
	"reflect"
)

type DocRepository[K comparable, T any] interface {
	Get(ctx context.Context, id K, dest *T, options ...QueryOption) error
	Select(ctx context.Context, filter map[string]any, dest *[]T, options ...QueryOption) error
	Insert(ctx context.Context, value T, options ...QueryOption) (K, error)
	InsertAll(ctx context.Context, values []T, options ...QueryOption) ([]K, error)
	Update(ctx context.Context, id K, keyvals map[string]any, options ...QueryOption) error
	Upsert(ctx context.Context, id K, value T, options ...QueryOption) error
	Delete(ctx context.Context, id []K, options ...QueryOption) error
	Iterator(ctx context.Context, filter map[string]any, options ...QueryOption) (RowIterator[T], error)
}

type docRepository struct {
	Name      string
	tableDef  DocTableDef
	modelType reflect.Type
	modelTags map[string]int
}
