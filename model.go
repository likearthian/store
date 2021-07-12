package store

type Model interface{
	GetTableDef() TabledDef
}
