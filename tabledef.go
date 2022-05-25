package store

import (
	"fmt"
	"reflect"

	"github.com/iancoleman/strcase"
)

type TabledDef struct {
	Schema    string
	Name      string
	KeyField  string
	Columns   []Column
	CreateDDL string
}

func (td TabledDef) ColumnNames() []string {
	return Map(td.Columns, func(val Column) string {
		return val.ColumnName
	})
}

func (td TabledDef) FullTableName() string {
	return fmt.Sprintf("%s.%s", td.Schema, td.Name)
}

type DBTable struct {
}

type Column struct {
	ColumnName string `db:"column_name"`
	DataType   string `db:"data_type"`
}

type ColumnInfo struct {
	Name      string
	Type      reflect.Type
	Size      int
	IsAuto    bool
	IsKey     bool
	AllowNull bool
}

func ParseModel(model reflect.Type) (schema, table string, cols []ColumnInfo, err error) {
	keyFound := false
	for i := 0; i < model.NumField(); i++ {
		field := model.Field(i)
		if field.Name == "DBTable" {
			schema = field.Tag.Get("schema")
			table = field.Tag.Get("name")
			continue
		}

		name, size, isAuto, isKey, allowNull := ParseDBTag(field.Tag.Get("db"))
		if isKey && keyFound {
			err = fmt.Errorf("cannot have more than 1 key")
			return
		}

		keyFound = isKey

		if name == "" {
			name = strcase.ToScreamingSnake(field.Name)
		}

		cols = append(cols, ColumnInfo{
			Name:      name,
			Type:      field.Type,
			Size:      size,
			AllowNull: allowNull,
			IsAuto:    isAuto,
			IsKey:     isKey,
		})
	}

	return
}
