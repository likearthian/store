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
	Tx Transaction
}

func WithTransaction(tx Transaction) QueryOption {
	return func(o *queryOption) {
		o.Tx = tx
	}
}
