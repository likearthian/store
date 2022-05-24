package store

type RepositoryOption[T any] func(o *option[T])

type option[T any] struct {
	initValues []T
}

func InitWith[T any](values []T) RepositoryOption[T] {
	return func(o *option[T]) {
		o.initValues = values
	}
}

type QueryOption func(o *queryOption)

type queryOption struct {
	Tx     Transaction
	Limit  int
	Offset int64
}

func WithTransaction(tx Transaction) QueryOption {
	return func(o *queryOption) {
		o.Tx = tx
	}
}

func WithLimit(limit int) QueryOption {
	return func(o *queryOption) {
		o.Limit = limit
	}
}

func WithOffset(offset int64) QueryOption {
	return func(o *queryOption) {
		o.Offset = offset
	}
}
