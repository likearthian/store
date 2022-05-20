package store

type RepositoryOption func(o *option)

type option struct {
	initValues interface{}
	name       string
}

func InitWith(values interface{}) RepositoryOption {
	return func(o *option) {
		o.initValues = values
	}
}

func WithName(name string) RepositoryOption {
	return func(o *option) {
		o.name = name
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
