package store

type RowIterator[T any] interface {
	Next() (*T, error)
	Close() error
}
