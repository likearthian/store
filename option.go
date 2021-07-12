package store

type RepositoryOption func(o *option)

type option struct {
	initValues interface{}
	name string
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

type DataOption func(o *dataOption)

type dataOption struct {
	Tx Transaction
}

func WithTransaction(tx Transaction) DataOption {
	return func(o *dataOption) {
		o.Tx = tx
	}
}