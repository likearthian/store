package store

type TabledDef struct {
	Schema   string
	Name     string
	KeyField string
	Primary  []string
	Index    []TableIndex
}

type TableIndex struct {
	Name   string
	Fields []string
}
