package store

type TabledDef struct {
	Schema    string
	Name      string
	KeyField  string
	Columns   []string
	CreateDDL string
}

type DBTable struct {
}

type Column struct {
	ColumnName string `db:"column_name"`
	DataType   string `db:"data_type"`
}
