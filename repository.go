package store

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
)

type Repository[K comparable, T any] interface {
	Get(ctx context.Context, id K, dest *T, options ...QueryOption) error
	Select(ctx context.Context, filter map[string]any, dest *[]T, options ...QueryOption) error
	Insert(ctx context.Context, value T, options ...QueryOption) (K, error)
	InsertAll(ctx context.Context, values []T, options ...QueryOption) ([]K, error)
	Update(ctx context.Context, id K, keyvals map[string]any, options ...QueryOption) error
	Upsert(ctx context.Context, id K, value T, options ...QueryOption) error
	// UpsertAll(ctx context.Context, values []T, options ...QueryOption) error
	Delete(ctx context.Context, id []K, options ...QueryOption) error
	//ParseRequestQueryIntoFilter(req interface{}) (interface{}, error)
	SQLQuery(ctx context.Context, dest any, sqlStr string, args []interface{}, options ...QueryOption) error
	SQLExec(ctx context.Context, sqlStr string, args []interface{}, options ...QueryOption) error
	Begin(ctx context.Context) (Transaction, error)
	GetTableDef() TabledDef
}

type repository struct {
	Name     string
	Schema   string
	tableDef TabledDef
	// model       Model
	modelType   reflect.Type
	modelTags   map[string]int
	columns     []Column
	columnNames []string
}

func (r repository) modelTagExists(name string) bool {
	_, ok := r.modelTags[strings.ToUpper(name)]
	return ok
}

func (r *repository) createFieldsAndValuesMapFromModelType(value any, fieldTag string) (map[string]any, error) {
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
				return nil, fmt.Errorf("value as map should have string key")
			}
		}
	}

	valType := dataVal.Type()
	if valType.Kind() == reflect.Ptr {
		valType = valType.Elem()
	}

	// if !valIsMap && r.modelType.Name() != valType.Name() {
	// 	return nil, fmt.Errorf("value must be of %s type or a map, got %s type", r.modelType.Name(), valType.Name())
	// }

	var result = make(map[string]interface{})
	for i := 0; i < valType.NumField(); i++ {
		var val interface{}
		field := valType.Field(i)
		fieldType := field.Type
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}

		col := ""
		tagValue, ok := field.Tag.Lookup(fieldTag)
		if ok {
			name, _, isAuto, _, _ := ParseDBTag(tagValue)
			if !isAuto {
				col = name
			}
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

		result[col] = val
	}

	return result, nil
}

func createModelTags(model reflect.Type, tag string) map[string]int {
	modelTags := make(map[string]int)
	for i := 0; i < model.NumField(); i++ {
		field := model.Field(i)
		tagArr := strings.Split(field.Tag.Get(tag), ",")
		if len(tagArr) == 0 {
			continue
		}

		tagName := strings.TrimSpace(tagArr[0])
		modelTags[strings.ToUpper(tagName)] = i
	}

	return modelTags
}

func CreateFieldsAndValuesMap[T any](value T, modelTags map[string]int) (map[string]any, error) {
	dataVal := reflect.ValueOf(value)

	valType := dataVal.Type()
	var result = make(map[string]interface{})
	for k, i := range modelTags {
		field := valType.Field(i)
		name, _, isAuto, _, _ := ParseDBTag(field.Tag.Get("db"))
		if name == "" || isAuto {
			continue
		}

		val := dataVal.Field(i).Interface()
		if v, ok := val.(driver.Valuer); ok {
			buffVal, err := v.Value()
			if err != nil {
				return nil, err
			}

			//skip for nil value
			if buffVal == nil {
				continue
			}

			val = buffVal
		}

		result[k] = val
	}

	return result, nil
}
