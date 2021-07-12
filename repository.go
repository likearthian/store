package store

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

type Repository interface {
	Get(ctx context.Context, id interface{}, dest interface{}, options ...DataOption) error
	Select(ctx context.Context, filter map[string]interface{}, dest interface{}, options ...DataOption) error
	Put(ctx context.Context, value interface{}, options ...DataOption) (interface{}, error)
	Replace(ctx context.Context, id interface{}, value interface{}, options ...DataOption) error
	Update(ctx context.Context, id interface{}, keyvals map[string]interface{}, options ...DataOption) error
	Upsert(ctx context.Context, id interface{}, value interface{}, options ...DataOption) error
	//ParseRequestQueryIntoFilter(req interface{}) (interface{}, error)
	SQLQuery(ctx context.Context, sqlStr string, dest interface{}, options ...DataOption) error
	SQLRun(ctx context.Context, sqlStr string, options ...DataOption) error
	Begin(ctx context.Context) (Transaction, error)
}

type repository struct {
	Name      string
	modelType reflect.Type
	model     Model
	tabledef  TabledDef
	modelTags map[string]int
}

func (r repository) modelTagExists(name string) bool {
	_, ok := r.modelTags[name]
	return ok
}

func (r *repository) createInsertSQLQueryFromModelType(value interface{}, fieldTag string) (query string, args []interface{}, err error) {
	dataVal := reflect.ValueOf(value)
	valIsMap := false
	var valKeys []reflect.Value

	if dataVal.Kind() == reflect.Ptr {
		dataVal = dataVal.Elem()
	}

	if dataVal.Kind() == reflect.Map {
		valIsMap = true
		valKeys = dataVal.MapKeys()
		for _, key := range dataVal.MapKeys() {
			if key.Type().Kind() != reflect.String {
				return "", nil, fmt.Errorf("value as map should have string key")
			}
		}
	}

	valType := dataVal.Type()
	if valType.Kind() == reflect.Ptr {
		valType = valType.Elem()
	}

	if !valIsMap && r.modelType.Name() != valType.Name() {
		return "", nil, fmt.Errorf("value must be of %s type or a map, got %s type", r.modelType.Name(), valType.Name())
	}

	var columns []string
	var values []interface{}
	for i := 0; i < r.modelType.NumField(); i++ {
		var val interface{}
		field := r.modelType.Field(i)
		fieldType := field.Type
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}

		col := ""
		tagValue, ok := field.Tag.Lookup(fieldTag)
		if ok {
			name, _, _ := ParseDBTag(tagValue)
			col = name
		}

		if col == "" {
			continue
		}

		if valIsMap {
			key := reflect.ValueOf(col)
			keyFound := false
			for _, mk := range valKeys {
				if mk.Interface() == col {
					keyFound = true
					break
				}
			}

			if !keyFound {
				continue
			}

			//mapValType := mapVal.Elem().Type()
			//valKind := column.Type.ToKind()
			mapVal := dataVal.MapIndex(key)

			//if mapValType.Kind() != valKind && mapValType.Kind() != reflect.Struct {
			//	return nil, nil, fmt.Errorf("cannot assign %s into %s for %s", mapValType.Kind(), valKind.String(), column.Name)
			//}
			val = mapVal.Interface()
		} else {
			val = dataVal.Field(i).Interface()
		}

		if v, ok := val.(driver.Valuer); ok {
			buffVal, err := v.Value()
			if err != nil {
				fmt.Println("Failed to get value from struct, field type :", valType.Field(i).Type)
				continue
			}

			//skip for nil value
			if buffVal == nil {
				continue
			}

			val = buffVal
		}

		columns = append(columns, col)
		values = append(values, val)
	}

	tableDef := r.model.GetTableDef()
	qry := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (?)", tableDef.Schema, tableDef.Name, strings.Join(columns, ","))
	return sqlx.In(qry, values)
}

func createModelTags(model reflect.Type, tag string) map[string]int {
	modelTags := make(map[string]int)
	for i := 0; i < model.NumField(); i++ {
		field := model.Field(i)
		tagName := field.Tag.Get(tag)
		if tagName == "" {
			//tagName = ToDelimited(field.Name, '_')
			continue
		}

		modelTags[tagName] = i
	}

	return modelTags
}
