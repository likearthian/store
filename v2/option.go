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
	Tx              Transaction
	Limit           int
	Offset          int64
	Sorter          []string
	IgnoreDuplicate bool
}

// WithTransaction returns a QueryOption that sets the transaction
// to use for the query.
func WithTransaction(tx Transaction) QueryOption {
	return func(o *queryOption) {
		o.Tx = tx
	}
}

// WithLimit returns a QueryOption that sets the limit for the
// number of rows to return.
func WithLimit(limit int) QueryOption {
	return func(o *queryOption) {
		o.Limit = limit
	}
}

// WithOffset returns a QueryOption that sets the offset for the
// rows returned.
func WithOffset(offset int64) QueryOption {
	return func(o *queryOption) {
		o.Offset = offset
	}
}

// WithSorter returns a QueryOption that sets the sorting order for the query.
// The sorter parameter is a variadic slice of field names to sort by, prefixed by "-" for descending order, and prefixed by "+" for ascending order.
//
// example:
//
//	WithSorter("-name", "+age")
func WithSorter(sorter ...string) QueryOption {
	return func(o *queryOption) {
		o.Sorter = sorter
	}
}

// WithIgnoreDuplicate returns a QueryOption that sets IgnoreDuplicate
// to true. To be used with Insert operation. When set to true, duplicate rows will be discarded
func WithIgnoreDuplicate() QueryOption {
	return func(o *queryOption) {
		o.IgnoreDuplicate = true
	}
}
