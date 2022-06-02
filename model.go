package store

type Model interface {
	GetTableDef() TabledDef
}

// type SQLModelHelper interface {
// 	GetInsertColumnNames() []string
// 	GetInsertPlaceholders() []string
// 	GetInsertArgs() []any
// }
type SQLInsertGenerator interface {
	GenerateInsertParts() (columns []string, placeholder []string, args []any)
}

type SQLTableCreator interface {
	DDL() string
}

type genericModel struct {
	tableDef TabledDef
}

func (g genericModel) GetTableDef() TabledDef {
	return g.tableDef
}

func CreateGenericModel(tb TabledDef) genericModel {
	return genericModel{
		tableDef: tb,
	}
}
