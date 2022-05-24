package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

type oracleRepository[K comparable, T Model] struct {
	repository
	db *sqlx.DB
}

func CreateOracleRepository[K comparable, T Model](db *sqlx.DB, options ...RepositoryOption) (Repository[K, T], error) {
	opt := &option{}
	for _, op := range options {
		op(opt)
	}

	var model T

	m := reflect.TypeOf(model)
	if m.Kind() == reflect.Ptr {
		m = m.Elem()
	}

	modelTags := createModelTags(m, "db")
	tb := model.GetTableDef()

	if opt.name == "" {
		opt.name = tb.Name
	}

	cols, err := oraGetColumns(db, tb.Schema, tb.Name)
	if err != nil {
		return nil, err
	}

	cols = Filter(cols, func(val Column) bool {
		_, ok := modelTags[strings.ToUpper(val.ColumnName)]
		return ok
	})

	colNames := Map(cols, func(val Column) string {
		return val.ColumnName
	})

	repo := &oracleRepository[K, T]{
		repository: repository{
			Name:        opt.name,
			model:       model,
			modelType:   m,
			modelTags:   modelTags,
			columns:     cols,
			columnNames: colNames,
		},
		db: db,
	}

	// if opt.initValues != nil {
	// 	if err := repo.init(opt.initValues); err != nil {
	// 		return nil, err
	// 	}
	// }

	return repo, nil
}

func (p *oracleRepository[K, T]) Init(values []T) error {
	dataVal := reflect.ValueOf(values)
	if dataVal.Kind() != reflect.Slice {
		return fmt.Errorf("value to init should be a slice")
	}

	if dataVal.Elem().Type() != p.modelType {
		return fmt.Errorf("values to init should be []%s, got %t", p.modelType.Name(), values)
	}

	for i := 0; i < dataVal.Len(); i++ {
		_ = dataVal.Index(i)

	}

	return nil
}

func (p *oracleRepository[K, T]) GetTableDef() TabledDef {
	return p.model.GetTableDef()
}

func (p *oracleRepository[K, T]) Get(ctx context.Context, id K, dest *T, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	columns := strings.Join(p.columnNames, ",")
	var argParam []interface{}
	tableDef := p.model.GetTableDef()
	qry := fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s = ?", columns, tableDef.Schema, tableDef.Name, tableDef.KeyField)
	argParam = append(argParam, id)

	tx, err := p.createTransaction(opt)
	if err != nil {
		return wrapPostgresError(err)
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	qry = tx.Rebind(qry)

	if err := tx.GetContext(ctx, dest, qry, argParam...); err != nil {
		return wrapPostgresError(err)
	}

	if opt.Tx == nil {
		return tx.Commit()
	}

	return nil
}

func (p *oracleRepository[K, T]) Select(ctx context.Context, filterMap map[string]any, dest *[]T, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	filter, argParam, err := p.parseFilterMapIntoWhereClause(filterMap)
	if err != nil {
		return err
	}

	if filter != "" {
		filter = "WHERE " + filter
	}

	tx, err := p.createTransaction(opt)
	if err != nil {
		return err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
		defer tx.Commit()
	}

	columns := strings.Join(p.columnNames, ",")
	tableDef := p.model.GetTableDef()
	qry := fmt.Sprintf("SELECT %s FROM %s.%s %s", columns, tableDef.Schema, tableDef.Name, filter)
	qry = tx.Rebind(qry)

	if err := tx.SelectContext(ctx, dest, qry, argParam...); err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

func (p *oracleRepository[K, T]) SQLQuery(ctx context.Context, dest any, sqlStr string, args []interface{}, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	tx, err := p.createTransaction(opt)
	if err != nil {
		return err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	sqlStr = tx.Rebind(sqlStr)
	if err := tx.SelectContext(ctx, dest, sqlStr, args...); err != nil {
		return err
	}

	if opt.Tx == nil {
		return tx.Commit()
	}

	return nil
}

func (p *oracleRepository[K, T]) SQLExec(ctx context.Context, sqlStr string, args []interface{}, options ...QueryOption) error {
	return fmt.Errorf("not implemented yet")
}

func (p *oracleRepository[K, T]) Insert(ctx context.Context, value T, options ...QueryOption) (K, error) {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	var zeroKey K

	fieldMap, err := p.createFieldsAndValuesMapFromModelType(value, "db")
	if err != nil {
		return zeroKey, err
	}

	var columns []string
	var values []interface{}
	for k, _ := range fieldMap {
		columns = append(columns, k)
		values = append(values, fieldMap[k])
	}

	tx, err := p.createTransaction(opt)
	if err != nil {
		return zeroKey, err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	tb := p.model.GetTableDef()
	var args []any
	mtype := "plain"
	valval := reflect.ValueOf(value)
	smodel, ok := valval.Interface().(SQLModelHelper)
	if ok {
		mtype = "smodel"
	}

	sgen, ok := valval.Interface().(SQLInsertGenerator)
	if ok {
		mtype = "sgen"
	}

	var valph string
	switch mtype {
	case "smodel":
		columns = smodel.GetInsertColumnNames()
		args = smodel.GetInsertArgs()
		ph := smodel.GetInsertPlaceholders()
		valph = strings.Join(ph, ",")
	case "sgen":
		var ph []string
		columns, ph, args = sgen.GenerateInsertParts()
		valph = strings.Join(ph, ",")
	default:
		valph, args, _ = sqlx.In("?", values)
	}

	qry := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES(%s)", tb.Schema, tb.Name, strings.Join(columns, ","), valph)
	qry += fmt.Sprintf(" RETURNING %s INTO ?", tb.KeyField)
	var id K
	args = append(args, sql.Out{Dest: &id})

	qry = tx.Rebind(qry)
	_, err = tx.ExecContext(ctx, qry, args...)
	if err != nil {
		return id, wrapPostgresError(err)
	}

	return id, tx.Commit()
}

func (p *oracleRepository[K, T]) InsertAll(ctx context.Context, values []T, options ...QueryOption) ([]K, error) {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	qry, args, err := p.createMultiInsertQuery(values)
	if err != nil {
		return nil, err
	}

	tx, err := p.createTransaction(opt)
	if err != nil {
		return nil, err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	qry = tx.Rebind(qry)
	_, err = tx.ExecContext(ctx, qry, args...)
	if err != nil {
		return nil, wrapPostgresError(err)
	}

	if opt.Tx == nil {
		return nil, tx.Commit()
	}

	return nil, nil
}

func (p *oracleRepository[K, T]) Replace(ctx context.Context, id K, value T, options ...QueryOption) error {
	return fmt.Errorf("this database doesn't support Replace")
}

func (p *oracleRepository[K, T]) Update(ctx context.Context, id K, keyvals map[string]any, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	qry, args := p.createUpdateQuery(id, keyvals)

	tx, err := p.createTransaction(opt)
	if err != nil {
		return err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	qry = tx.Rebind(qry)

	_, err = tx.ExecContext(ctx, qry, args...)
	if err != nil {
		return wrapPostgresError(err)
	}

	if opt.Tx == nil {
		return tx.Commit()
	}

	return nil
}

func (p *oracleRepository[K, T]) Upsert(ctx context.Context, id K, value T, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	tx, err := p.createTransaction(opt)
	if err != nil {
		return err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	var existing bool = true
	var placeholder T
	if err := p.Get(ctx, id, &placeholder, WithTransaction(opt.Tx)); err != nil {
		existing = false
	}

	if existing {
		if err := p.Delete(ctx, []K{id}, WithTransaction(opt.Tx)); err != nil {
			return err
		}
	}

	if _, err := p.Insert(ctx, value, WithTransaction(opt.Tx)); err != nil {
		return err
	}

	if opt.Tx == nil {
		return tx.Commit()
	}

	return nil
}

func (p *oracleRepository[K, T]) Delete(ctx context.Context, id []K, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	tx, err := p.createTransaction(opt)
	if err != nil {
		return err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	tb := p.model.GetTableDef()
	qry := fmt.Sprintf("DELETE FROM %s.%s WHERE %s in (?)", tb.Schema, tb.Name, tb.KeyField)
	var args []any
	qry, args, err = sqlx.In(qry, id)
	if err != nil {
		return fmt.Errorf("failed to expand delete query. %s", err)
	}

	qry = tx.Rebind(qry)
	if _, err := tx.ExecContext(ctx, qry, args...); err != nil {
		return err
	}

	if opt.Tx == nil {
		return tx.Commit()
	}

	return nil
}

func (p *oracleRepository[K, T]) Begin(ctx context.Context) (Transaction, error) {
	tx, err := p.db.Beginx()
	if err != nil {
		return nil, err
	}

	return &sqlTransaction{Tx: tx}, nil
}

func (p *oracleRepository[K, T]) createUpdateQuery(id K, keyvals map[string]interface{}) (qry string, args []any) {
	tb := p.model.GetTableDef()
	var sets []string
	for k, v := range keyvals {
		sets = append(sets, fmt.Sprintf("%s = ?", k))
		args = append(args, v)
	}

	qry = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s = ?", tb.Schema, tb.Name, strings.Join(sets, ","), tb.KeyField)
	args = append(args, id)
	return
}

func (p *oracleRepository[K, T]) parseFilterMapIntoWhereClause(filterMap map[string]interface{}) (string, []interface{}, error) {
	where := ""
	var args []interface{}
	for k, v := range filterMap {
		if !p.modelTagExists(k) {
			continue
		}

		vval := reflect.ValueOf(v)
		val := vval.Interface()
		if vval.Kind() != reflect.Slice {
			if len(where) > 0 {
				where += " AND "
			}
			where += k + " = ?"
			args = append(args, val)
			continue
		}

		if vval.Len() > 0 {
			if f, arg, err := p.parameterizedFilterCriteriaSlice(k, val); err == nil {
				if len(where) > 0 {
					where += " AND "
				}
				where += f
				args = append(args, arg)
			}
		}
	}

	return sqlx.In(where, args...)
}

func (p *oracleRepository[K, T]) parameterizedFilterCriteriaSlice(fieldname string, values interface{}) (string, interface{}, error) {
	where := fieldname
	vtype := reflect.TypeOf(values)
	if vtype.Kind() == reflect.Ptr {
		vtype = vtype.Elem()
	}

	if vtype.Kind() != reflect.Slice {
		return "", nil, fmt.Errorf("expecting slice as values, got %s", vtype.Kind().String())
	}

	s := reflect.ValueOf(values)
	if s.Len() == 0 {
		return "", nil, fmt.Errorf("cannot use empty slice to parameterized")
	}

	var value interface{}
	if s.Len() > 1 {
		where += " IN(?)"
		value = values
	} else {
		where += " = ?"
		value = s.Index(0).Interface()
	}

	return where, value, nil
}

func oraGetColumns(db *sqlx.DB, schema, table string) ([]Column, error) {
	qry := `
		SELECT 
			column_name "column_name"
			,CASE WHEN InStr(data_type, 'TIMESTAMP') > 0 THEN 'TIMESTAMP' ELSE data_type END "data_type" 
		FROM all_tab_cols WHERE owner = ? AND table_name = ?`
	qry = db.Rebind(qry)
	var cols []Column
	if err := db.Select(&cols, qry, strings.ToUpper(schema), strings.ToUpper(table)); err != nil {
		return nil, err
	}
	return cols, nil
}

func oraGetKeyColumnName(db *sqlx.DB, schema, table string) (string, error) {
	qry := `
		select
			b.COLUMN_NAME
		from
			all_constraints a
			inner join
			all_cons_columns b
			on
				a.constraint_name = b.constraint_name
		where
			a.owner = ?
			and a.table_name = ?
			and a.constraint_type = 'P';
	`

	type ColName struct {
		Name string `db:"COLUMN_NAME"`
	}

	qry = db.Rebind(qry)
	var cols []ColName
	if err := db.Select(&cols, qry, strings.ToUpper(schema), strings.ToUpper(table)); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
	}

	if len(cols) > 1 {
		return "", fmt.Errorf("store doesn't support multiple key field")
	}

	return cols[0].Name, nil
}

func (p *oracleRepository[K, T]) createTransaction(opt *queryOption) (*sqlx.Tx, error) {
	if opt.Tx != nil {
		if tx, ok := opt.Tx.(*sqlTransaction); ok {
			return tx.Tx, nil
		}
	}

	return p.db.Beginx()
}

func (p *oracleRepository[K, T]) createMultiInsertQuery(values []T) (strSql string, args []any, err error) {
	if len(values) == 0 {
		err = fmt.Errorf("values is zero length slice")
		return
	}

	tb := p.model.GetTableDef()
	var columnMap = make(map[string]Column)
	for _, col := range p.columns {
		key := strings.ToUpper(col.ColumnName)
		columnMap[key] = col
	}

	var fieldMaps = make([]map[string]any, len(values))
	for i, val := range values {
		fieldMap, err := p.createFieldsAndValuesMapFromModelType(val, "db")
		if err != nil {
			return strSql, args, err
		}

		fieldMaps[i] = fieldMap
	}

	insertColumnNames := p.columnNames
	mtype := "plain"

	smodel, isSmodel := p.model.(SQLModelHelper)
	if isSmodel {
		insertColumnNames = smodel.GetInsertColumnNames()
		mtype = "smodel"
	}

	sgen, isSgen := p.model.(SQLInsertGenerator)
	if isSgen {
		insertColumnNames, _, _ = sgen.GenerateInsertParts()
		mtype = "sgen"
	}

	var insertFields []Column
	for _, c := range insertColumnNames {
		cname := strings.ToUpper(c)
		if _, ok := columnMap[cname]; ok {
			insertFields = append(insertFields, columnMap[cname])
		}
	}

	var insertRows = make([][]any, len(insertColumnNames))
	for i, val := range values {
		switch mtype {
		case "smodel":
			smodel = reflect.ValueOf(val).Interface().(SQLModelHelper)
			sval := smodel.GetInsertArgs()
			for iv := range sval {
				insertRows[iv] = append(insertRows[iv], sval[iv])
			}
		case "sgen":
			sgen = reflect.ValueOf(val).Interface().(SQLInsertGenerator)
			_, _, sval := sgen.GenerateInsertParts()
			for iv := range sval {
				insertRows[iv] = append(insertRows[iv], sval[iv])
			}
		default:
			fm := fieldMaps[i]
			var row = make([]any, len(insertColumnNames))
			for cx, c := range insertColumnNames {
				cname := strings.ToUpper(c)
				if cval, ok := fm[cname]; ok {
					row[cx] = cval
				}
			}

			for iv := range row {
				insertRows[iv] = append(insertRows[iv], row[iv])
			}
		}
	}

	args = makeOraValueSlice(insertFields, insertRows)
	strSql = fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES(?)", tb.Schema, tb.Name, strings.Join(insertColumnNames, ","))

	strSql, _, err = sqlx.In(strSql, insertColumnNames)

	return
}

// func createOraInsertQuery(schema, tablename string, value any) (qry string, args []any, err error) {
// 	vtype := "struct"
// 	var cols []string
// 	var placeholders []string
// 	var vVal reflect.Value

// 	sqlInserter, isSQLInserter := value.(SQLInserter)
// 	if isSQLInserter {
// 		vtype = "SQLInserter"
// 	} else {
// 		vVal = reflect.ValueOf(value)
// 		switch vVal.Type().Kind() {
// 		case reflect.Struct:
// 			vtype = "struct"
// 		case reflect.Map:
// 			vtype = "map"
// 		default:
// 			return qry, args, fmt.Errorf("value of type %T is not supported", value)
// 		}
// 	}

// 	switch vtype {
// 	case "SQLInserter":
// 		cols, placeholders, args = sqlInserter.GenerateInsertParts()
// 	case "struct":

// 	}
// }

func makeOraValueSlice(insertFields []Column, insertValues [][]any) (argValues []any) {
	argValues = make([]any, len(insertValues))
	for i, value := range insertValues {
		switch insertFields[i].DataType {
		case "VARCHAR2":
			sl := make([]string, len(value))
			for ix, v := range value {
				var val string
				switch v := v.(type) {
				case string:
					val = v
					if len(val) > 4000 {
						val = val[:4000]
					}
				case time.Time:
					val = v.Format("2006-01-02 3:04:05.000000 PM")
				case sql.NullTime:
					val = ""
					if v.Valid {
						val = v.Time.Format("2006-01-02 3:04:05.000000 PM")
					}
				default:
					val = fmt.Sprintf("%v", v)
				}

				sl[ix] = val
			}
			argValues[i] = sl
		case "TIMESTAMP", "DATE":
			sl := make([]sql.NullTime, len(value))
			for ix, v := range value {
				var val sql.NullTime
				switch v := v.(type) {
				case time.Time:
					val = sql.NullTime{Valid: true, Time: v}
				case sql.NullTime:
					val = v
				default:
					val = sql.NullTime{Valid: false}
				}

				sl[ix] = val
			}
			argValues[i] = sl
		case "NUMBER":
			sl := make([]sql.NullFloat64, len(value))
			for ix, v := range value {
				var val sql.NullFloat64
				switch v := v.(type) {
				case int:
					val = sql.NullFloat64{Valid: true, Float64: float64(v)}
				case int64:
					val = sql.NullFloat64{Valid: true, Float64: float64(v)}
				case sql.NullInt32:
					val = sql.NullFloat64{Valid: false}
					if v.Valid {
						val.Float64 = float64(v.Int32)
						val.Valid = true
					}
				case sql.NullInt64:
					val = sql.NullFloat64{Valid: false}
					if v.Valid {
						val.Float64 = float64(v.Int64)
						val.Valid = true
					}
				case sql.NullFloat64:
					val = v
				default:
					val = sql.NullFloat64{Valid: false}
				}

				sl[ix] = val
			}
			argValues[i] = sl
		default:
			sl := make([]string, len(value))
			for ix, v := range value {
				sl[ix] = fmt.Sprintf("%v", v)
			}
			argValues[i] = sl
		}
	}

	return argValues
}

func (p *oracleRepository[K, T]) createOracleMultiInsertQueryAll(values []T) (valueSql string, args []any, err error) {
	if len(values) == 0 {
		err = fmt.Errorf("values is zero length slice")
		return
	}

	tb := p.model.GetTableDef()
	var fieldMaps = make([]map[string]any, len(values))
	for i, val := range values {
		fieldMap, err := p.createFieldsAndValuesMapFromModelType(val, "db")
		if err != nil {
			return valueSql, args, err
		}

		fieldMaps[i] = fieldMap
	}

	var columns []string
	for k, _ := range fieldMaps[0] {
		columns = append(columns, k)
	}

	valueSql = "INSERT ALL"

	smodel, ok := p.model.(SQLModelHelper)
	if ok {
		columns = smodel.GetInsertColumnNames()
	}

	for i, fm := range fieldMaps {
		subins := ""
		if ok {
			smodel = reflect.ValueOf(values[i]).Interface().(SQLModelHelper)
			args = append(args, smodel.GetInsertArgs()...)
			ph := smodel.GetInsertPlaceholders()
			subins = strings.Join(ph, ",")
		} else {
			var argValues []any
			for k, _ := range fm {
				argValues = append(argValues, fm[k])
			}
			subins, _, _ = sqlx.In("?", argValues...)
			args = append(args, argValues...)
		}

		valueSql += fmt.Sprintf("\nINTO %s.%s (%s) VALUES (%s)", tb.Schema, tb.Name, strings.Join(columns, ","), subins)
	}

	valueSql += "\nSELECT 1 FROM DUAL"
	return
}
