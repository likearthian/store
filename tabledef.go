package store

type TabledDef struct {
	Schema       string
	Name         string
	KeyField     string
	PrimaryField []string
	Index        []TableIndex
}

type TableIndex struct {
	Name   string
	Fields []string
}
