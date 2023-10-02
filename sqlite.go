package store

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	"gopkg.in/guregu/null.v4"
)

type sqliteRepository[K comparable, T any] struct {
	repository
	db *sqlx.DB
}

func CreateSqliteRepository[K comparable, T any](db *sqlx.DB, options ...RepositoryOption[T]) (Repository[K, T], error) {
	opt := &option[T]{}
	for _, op := range options {
		op(opt)
	}

	var entity T

	mval := reflect.ValueOf(entity)
	if mval.Type().Kind() == reflect.Ptr {
		mval = mval.Elem()
	}

	modelTags := createModelTags(mval.Type(), "db")
	tb, err := sqliteCreateTableDef(mval)
	if err != nil {
		return nil, err
	}

	cols, err := sqliteGetColumns(db, tb.Schema, tb.Name)
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

	repo := &sqliteRepository[K, T]{
		repository: repository{
			Name:        tb.Name,
			tableDef:    tb,
			modelType:   mval.Type(),
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

func (sl *sqliteRepository[K, T]) init(values []T) error {
	return nil
}

func (sl *sqliteRepository[K, T]) GetTableDef() TabledDef {
	return sl.tableDef
}

func (sl *sqliteRepository[K, T]) Get(ctx context.Context, id K, dest *T, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	columns := strings.Join(sl.columnNames, ",")
	var argParam []any
	tableDef := sl.tableDef
	qry := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", columns, tableDef.FullTableName(), tableDef.KeyField)
	argParam = append(argParam, id)

	tx, err := sl.createTransaction(opt)
	if err != nil {
		return wrapSqliteError(err)
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	qry = tx.Rebind(qry)

	if err := tx.GetContext(ctx, dest, qry, argParam...); err != nil {
		return wrapSqliteError(err)
	}

	return tx.Commit()
}

func (sl *sqliteRepository[K, T]) Select(ctx context.Context, filterMap map[string]any, dest *[]T, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	filter, argParam, err := sl.parseFilterMapIntoWhereClause(filterMap)
	if err != nil {
		return err
	}

	if filter != "" {
		filter = "WHERE " + filter
	}

	tx, err := sl.createTransaction(opt)
	if err != nil {
		return err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	paging := CreateSqliteLimitOffsetSql(opt.Limit, opt.Offset)

	columns := strings.Join(sl.columnNames, ",")
	tableDef := sl.tableDef
	qry := fmt.Sprintf("SELECT %s FROM %s %s%s", columns, tableDef.FullTableName(), filter, paging)
	qry = tx.Rebind(qry)

	if err := tx.SelectContext(ctx, dest, qry, argParam...); err != nil {
		return wrapSqliteError(err)
	}

	return tx.Commit()
}

func (sl *sqliteRepository[K, T]) SQLQuery(ctx context.Context, dest any, sqlStr string, args []interface{}, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	tx, err := sl.createTransaction(opt)
	if err != nil {
		return err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	sqlStr = tx.Rebind(sqlStr)
	//fmt.Println("qry:", sqlStr)
	//fmt.Println("args:", args)
	if err := tx.SelectContext(ctx, dest, sqlStr, args...); err != nil {
		return err
	}

	if opt.Tx == nil {
		return tx.Commit()
	}

	return nil
}

func (sl *sqliteRepository[K, T]) SQLExec(ctx context.Context, sqlStr string, args []interface{}, options ...QueryOption) error {
	return fmt.Errorf("not implemented yet")
}

func (sl *sqliteRepository[K, T]) Insert(ctx context.Context, value T, options ...QueryOption) (K, error) {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	var zeroKey K

	fieldMap, err := sl.createFieldsAndValuesMapFromModelType(value, "db")
	if err != nil {
		return zeroKey, err
	}

	var columns []string
	var values []interface{}
	for k := range fieldMap {
		columns = append(columns, k)
		values = append(values, fieldMap[k])
	}

	tx, err := sl.createTransaction(opt)
	if err != nil {
		return zeroKey, err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	tabledef := sl.tableDef
	qry := fmt.Sprintf("INSERT INTO %s (%s) VALUES (?)", tabledef.FullTableName(), strings.Join(columns, ","))
	qry, args, _ := sqlx.In(qry, values)
	qry = tx.Rebind(qry)

	// fmt.Printf("qry: %s\n", qry)
	// fmt.Printf("args: %+v\n", args)
	_, err = tx.ExecContext(ctx, qry, args...)
	if err != nil {
		return zeroKey, wrapSqliteError(err)
	}

	return zeroKey, tx.Commit()
}

func (sl *sqliteRepository[K, T]) InsertAll(ctx context.Context, values []T, options ...QueryOption) ([]K, error) {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	qry, args, err := sl.createMultiInsertQuery(values)
	if err != nil {
		return nil, err
	}

	tx, err := sl.createTransaction(opt)
	if err != nil {
		return nil, err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	qry = tx.Rebind(qry)
	_, err = tx.ExecContext(ctx, qry, args...)
	if err != nil {
		return nil, wrapSqliteError(err)
	}

	if opt.Tx == nil {
		return nil, tx.Commit()
	}

	return nil, nil
}

func (sl *sqliteRepository[K, T]) Replace(ctx context.Context, id K, value T, options ...QueryOption) error {
	return fmt.Errorf("this database doesn't support Replace")
}

func (sl *sqliteRepository[K, T]) Update(ctx context.Context, id K, keyvals map[string]any, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	qry, args := sl.createUpdateQuery(id, keyvals)

	tx, err := sl.createTransaction(opt)
	if err != nil {
		return err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	qry = tx.Rebind(qry)

	_, err = tx.ExecContext(ctx, qry, args...)
	if err != nil {
		return wrapSqliteError(err)
	}

	if opt.Tx == nil {
		return tx.Commit()
	}

	return nil
}

func (sl *sqliteRepository[K, T]) Upsert(ctx context.Context, id K, value T, options ...QueryOption) error {
	return fmt.Errorf("Upsert not implemented")
}

func (sl *sqliteRepository[K, T]) UpsertAll(ctx context.Context, values []T, options ...QueryOption) error {
	return fmt.Errorf("UpsertAll not implemented")
}

func (sl *sqliteRepository[K, T]) Delete(ctx context.Context, id []K, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	tx, err := sl.createTransaction(opt)
	if err != nil {
		return err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	tb := sl.tableDef
	batches := SplitBatch(id, 125)

	for i := range batches {
		qry := fmt.Sprintf("DELETE FROM %s WHERE %s in (?)", tb.FullTableName(), tb.KeyField)
		var args []any
		qry, args, err = sqlx.In(qry, batches[i])
		if err != nil {
			return fmt.Errorf("failed to expand delete query. %s", err)
		}

		qry = tx.Rebind(qry)
		if _, err := tx.ExecContext(ctx, qry, args...); err != nil {
			return err
		}
	}

	if opt.Tx == nil {
		return tx.Commit()
	}

	return nil
}

func (sl *sqliteRepository[K, T]) Begin(ctx context.Context) (Transaction, error) {
	tx, err := sl.db.Beginx()
	if err != nil {
		return nil, err
	}

	return &sqlTransaction{Tx: tx}, nil
}

func (sl *sqliteRepository[K, T]) createUpdateQuery(id K, keyvals map[string]interface{}) (qry string, args []any) {
	tb := sl.tableDef
	var sets []string
	for k, v := range keyvals {
		sets = append(sets, fmt.Sprintf("%s = ?", k))
		args = append(args, v)
	}

	qry = fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?", tb.FullTableName(), strings.Join(sets, ","), tb.KeyField)
	args = append(args, id)
	return
}

func (sl *sqliteRepository[K, T]) parseFilterMapIntoWhereClause(filterMap map[string]any) (string, []interface{}, error) {
	where := ""
	var args []interface{}
	for k, v := range filterMap {
		if !sl.modelTagExists(k) {
			continue
		}

		vval := reflect.ValueOf(v)
		val := vval.Interface()

		if fnull, ok := val.(FilterNull); ok {
			if len(where) > 0 {
				where += " AND "
			}
			isNot := ""
			if !fnull.IsNull() {
				isNot = "NOT "
			}
			where += fmt.Sprintf("%s IS %sNULL", k, isNot)
			continue
		}

		if vval.Kind() != reflect.Slice {
			if len(where) > 0 {
				where += " AND "
			}
			where += k + " = ?"
			args = append(args, val)
			continue
		}

		if vval.Len() > 0 {
			if f, arg, err := sl.parameterizedFilterCriteriaSlice(k, val); err == nil {
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

func (sl *sqliteRepository[K, T]) parameterizedFilterCriteriaSlice(fieldname string, values interface{}) (string, interface{}, error) {
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

func (sl *sqliteRepository[K, T]) createTransaction(opt *queryOption) (*sqlx.Tx, error) {
	if opt.Tx != nil {
		if tx, ok := opt.Tx.(*sqlTransaction); ok {
			return tx.Tx, nil
		}
	}

	return sl.db.Beginx()
}

func (sl *sqliteRepository[K, T]) createMultiInsertQuery(values []T) (strSql string, args []any, err error) {
	if len(values) == 0 {
		err = fmt.Errorf("values is zero length slice")
		return
	}

	tb := sl.tableDef
	var columnMap = make(map[string]Column)
	for _, col := range sl.columns {
		key := strings.ToUpper(col.ColumnName)
		columnMap[key] = col
	}

	var fieldMaps = make([]map[string]any, len(values))
	for i, val := range values {
		fieldMap, err := sl.createFieldsAndValuesMapFromModelType(val, "db")
		if err != nil {
			return strSql, args, err
		}

		fieldMaps[i] = fieldMap
	}

	insertColumnNames := sl.columnNames

	var insertFields []Column
	for _, c := range insertColumnNames {
		cname := strings.ToUpper(c)
		if _, ok := columnMap[cname]; ok {
			insertFields = append(insertFields, columnMap[cname])
		}
	}

	var insertValues []string
	for i := range values {
		fm := fieldMaps[i]
		var row = make([]any, len(insertColumnNames))
		for cx, c := range insertColumnNames {
			cname := strings.ToUpper(c)
			if cval, ok := fm[cname]; ok {
				row[cx] = cval
			}
		}

		args = append(args, row)
		insertValues = append(insertValues, "(?)")
	}

	strSql = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tb.FullTableName(), strings.Join(insertColumnNames, ","), strings.Join(insertValues, ","))

	strSql, args, err = sqlx.In(strSql, args...)

	return
}

func sqliteCreateTableDef(mval reflect.Value) (TabledDef, error) {
	if mval.Type().Kind() == reflect.Ptr {
		mval = mval.Elem()
	}

	if model, isModel := mval.Interface().(Model); isModel {
		return model.GetTableDef(), nil
	}

	var tbdef TabledDef
	schema, table, colInfos, err := ParseModel(mval.Type())
	if err != nil {
		return tbdef, err
	}

	var cols []Column
	var ddlCols []string
	var keyField string
	for _, col := range colInfos {
		dtype := convertTypeToSqliteType(col.Type)
		if dtype == "UNKNOWN" {
			return tbdef, fmt.Errorf("unknown datatype for Go type %s in %s", col.Type.Name(), mval.Type().Name())
		}

		cols = append(cols, Column{
			ColumnName: col.Name,
			DataType:   dtype,
		})

		coltype := dtype

		var ddlCol strings.Builder
		ddlCol.WriteString(fmt.Sprintf("%s %s", col.Name, coltype))

		if !col.AllowNull {
			ddlCol.WriteString(" NOT NULL")
		}

		if col.IsKey {
			keyField = col.Name
			ddlCol.WriteString(" PRIMARY KEY")
		}

		ddlCols = append(ddlCols, ddlCol.String())
	}

	createDDL := fmt.Sprintf("CREATE TABLE %s (%s)", table, strings.Join(ddlCols, ","))

	return TabledDef{
		Name:      table,
		Schema:    schema,
		KeyField:  keyField,
		Columns:   cols,
		CreateDDL: createDDL,
	}, nil
}

func convertTypeToSqliteType(model reflect.Type) string {
	kind := model.Kind()
	switch kind {
	case reflect.String:
		return "TEXT"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint,
		reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "INTEGER"
	case reflect.Int64, reflect.Uint64:
		return "INTEGER"
	case reflect.Float32:
		return "REAL"
	case reflect.Float64:
		return "REAL"
	case reflect.Bool:
		return "INTEGER"
	case reflect.Struct:
		switch model.Name() {
		case "Time":
			return "TIMESTAMP"
		case "NullString", "String":
			return "TEXT"
		case "NullFloat64", "Float":
			return "REAL"
		case "NullInt32", "NullInt64", "Int":
			return "INTEGER"
		default:
			return "UNKNOWN"
		}
	default:
		return "UNKNOWN"
	}
}

func sqliteGetColumns(db *sqlx.DB, schema, table string) ([]Column, error) {
	qry := fmt.Sprintf("PRAGMA table_info(%s)", table)

	qry = db.Rebind(qry)
	type columnInfo struct {
		CID       int        `db:"cid"`
		Name      string     `db:"name"`
		Type      string     `db:"type"`
		NotNull   int        `db:"notnull"`
		DfltValue null.Float `db:"dflt_value"`
		Pk        int        `db:"pk"`
	}

	var cols []columnInfo
	if err := db.Select(&cols, qry); err != nil {
		return nil, err
	}

	return Map(cols, func(col columnInfo) Column {
		return Column{
			ColumnName: col.Name,
			DataType:   col.Type,
		}
	}), nil
}

func CreateSqliteLimitOffsetSql(limit int, offset int64) string {
	if limit < 0 {
		limit = 0
	}

	qry := strings.Builder{}

	if limit > 0 {
		qry.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	}

	if offset > 0 {
		qry.WriteString(fmt.Sprintf(" OFFSET %d", offset))
	}

	return qry.String()
}
