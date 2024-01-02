package store

type Model interface {
	GetTableDef() TableDef
	SetID(id any)
}

type IDSetter[K comparable] interface {
	SetID(id K)
}

//	type SQLModelHelper interface {
//		GetInsertColumnNames() []string
//		GetInsertPlaceholders() []string
//		GetInsertArgs() []any
//	}
type SQLInsertGenerator interface {
	GenerateInsertParts() (columns []string, placeholder []string, args []any)
}

type SQLTableCreator interface {
	DDL() string
}

type genericModel struct {
	tableDef SQLTableDef
}

func (g genericModel) GetTableDef() SQLTableDef {
	return g.tableDef
}

func CreateGenericModel(tb SQLTableDef) genericModel {
	return genericModel{
		tableDef: tb,
	}
}
