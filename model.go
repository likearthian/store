package store

type Model interface {
	GetTableDef() TabledDef
}

type SQLModelHelper interface {
	GetInsertColumnNames() []string
	GetInsertPlaceholders() []string
	GetInsertArgs() []any
}
