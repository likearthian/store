package store

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/iancoleman/strcase"
)

type SQLTableDef struct {
	Schema    string
	Name      string
	KeyField  string
	Columns   []ColumnInfo
	CreateDDL string
}

func (td SQLTableDef) ColumnNames() []string {
	return sliceMap(td.Columns, func(val ColumnInfo) string {
		return val.Name
	})
}

func (td SQLTableDef) FullTableName() string {
	name := td.Name
	if td.Schema != "" {
		name = fmt.Sprintf("%s.%s", td.Schema, td.Name)
	}
	return name
}

type DocTableDef struct {
	Collection string
}

func (td DocTableDef) FullTableName() string {
	return td.Collection
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

func createSqlTableDef(mval reflect.Value, ddlGen func(schema, table string, colInfos []ColumnInfo) (string, error)) (SQLTableDef, error) {
	if mval.Type().Kind() == reflect.Ptr {
		mval = mval.Elem()
	}

	if model, isModel := mval.Interface().(Model); isModel {
		return model.GetTableDef(), nil
	}

	var tbdef SQLTableDef
	schema, table, key, colInfos, err := parseModel(mval.Type())
	if err != nil {
		return tbdef, err
	}

	ddl, err := ddlGen(schema, table, colInfos)
	if err != nil {
		return tbdef, err
	}

	return SQLTableDef{
		Schema:    schema,
		Name:      table,
		KeyField:  key,
		Columns:   colInfos,
		CreateDDL: ddl,
	}, nil
}

func parseModel(model reflect.Type) (schema, table, key string, cols []ColumnInfo, err error) {
	keyFound := false
	for i := 0; i < model.NumField(); i++ {
		field := model.Field(i)
		if field.Name == "DBTable" {
			schema = field.Tag.Get("schema")
			table = field.Tag.Get("name")
			continue
		}

		name, size, isAuto, isKey, allowNull := parseDBTag(field.Tag.Get("db"))
		if name == "" {
			name = strcase.ToScreamingSnake(field.Name)
		}

		if isKey {
			key = name
			if keyFound {
				err = fmt.Errorf("cannot have more than 1 key")
				return
			}
		}

		keyFound = isKey

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

func parseDBTag(value string) (name string, size int, isAuto bool, isKey bool, allowNull bool) {
	tagArr := strings.Split(value, ",")
	if len(tagArr) == 0 {
		return
	}

	checkBool := func(key string, tagarr []string) bool {
		bval := false
		skey := strings.TrimSpace(tagarr[0])
		if strings.EqualFold(skey, key) {
			bval = true
		}

		if len(tagarr) > 1 {
			sval := strings.TrimSpace(tagarr[1])
			if strings.EqualFold(sval, "true") {
				bval = true
			}

			if strings.EqualFold(sval, "false") {
				bval = false
			}
		}

		return bval
	}

	name = strings.TrimSpace(tagArr[0])
	if len(tagArr) > 1 {
		det := strings.Split(tagArr[1], " ")
		for _, v := range det {
			varr := strings.Split(v, "=")
			key := strings.TrimSpace(varr[0])

			if checkBool("auto", varr) {
				isAuto = true
				continue
			}

			if checkBool("key", varr) {
				isKey = true
				allowNull = false
				continue
			}

			if checkBool("allownull", varr) {
				allowNull = true && !isKey
				continue
			}

			if len(varr) > 1 {
				if strings.EqualFold(key, "size") {
					size, _ = strconv.Atoi(varr[1])
				}
			}
		}
	}

	return
}
