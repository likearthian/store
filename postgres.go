package store

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

type postgresRepository[K comparable, T any] struct {
	repository
	db *sqlx.DB
}

func CreatePostgresRepository[K comparable, T any](db *sqlx.DB, options ...RepositoryOption[T]) (Repository[K, T], error) {
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
	tb, err := pgCreateTableDef(mval)
	if err != nil {
		return nil, err
	}

	cols, err := pgGetColumns(db, tb.Schema, tb.Name)
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

	repo := &postgresRepository[K, T]{
		repository: repository{
			Name:        tb.Name,
			Schema:      tb.Schema,
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

func (p *postgresRepository[K, T]) init(values []T) error {
	return nil
}

func (p *postgresRepository[K, T]) GetTableDef() TabledDef {
	return p.tableDef
}

func (p *postgresRepository[K, T]) Get(ctx context.Context, id K, dest *T, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	columns := strings.Join(p.columnNames, ",")
	var argParam []interface{}
	tableDef := p.tableDef
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

	return tx.Commit()
}

func (p *postgresRepository[K, T]) Select(ctx context.Context, filterMap map[string]any, dest *[]T, options ...QueryOption) error {
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
	}

	paging := CreatePgLimitOffsetSql(opt.Limit, opt.Offset)

	columns := strings.Join(p.columnNames, ",")
	tableDef := p.tableDef
	qry := fmt.Sprintf("SELECT %s FROM %s.%s %s%s", columns, tableDef.Schema, tableDef.Name, filter, paging)
	qry = tx.Rebind(qry)

	if err := tx.SelectContext(ctx, dest, qry, argParam...); err != nil {
		return wrapPostgresError(err)
	}

	return tx.Commit()
}

func (p *postgresRepository[K, T]) SQLQuery(ctx context.Context, dest any, sqlStr string, args []interface{}, options ...QueryOption) error {
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

func (p *postgresRepository[K, T]) SQLExec(ctx context.Context, sqlStr string, args []interface{}, options ...QueryOption) error {
	return fmt.Errorf("not implemented yet")
}

func (p *postgresRepository[K, T]) Insert(ctx context.Context, value T, options ...QueryOption) (K, error) {
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

	tabledef := p.tableDef
	qry := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (?)", tabledef.Schema, tabledef.Name, strings.Join(columns, ","))
	qry, args, err := sqlx.In(qry, values)
	qry = tx.Rebind(qry)

	_, err = tx.ExecContext(ctx, qry, args...)
	if err != nil {
		return zeroKey, wrapPostgresError(err)
	}

	if opt.Tx == nil {
		return zeroKey, tx.Commit()
	}

	return zeroKey, nil
}

func (p *postgresRepository[K, T]) InsertAll(ctx context.Context, values []T, options ...QueryOption) ([]K, error) {
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
	fmt.Println("qry:", qry)
	fmt.Println("args:", args)
	_, err = tx.ExecContext(ctx, qry, args...)
	if err != nil {
		return nil, wrapPostgresError(err)
	}

	if opt.Tx == nil {
		return nil, tx.Commit()
	}

	return nil, nil
}

func (p *postgresRepository[K, T]) Replace(ctx context.Context, id K, value T, options ...QueryOption) error {
	return fmt.Errorf("this database doesn't support Replace")
}

func (p *postgresRepository[K, T]) Update(ctx context.Context, id K, keyvals map[string]any, options ...QueryOption) error {
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
	fmt.Println("qry:", qry)
	fmt.Println("args:", args)

	_, err = tx.ExecContext(ctx, qry, args...)
	if err != nil {
		return wrapPostgresError(err)
	}

	if opt.Tx == nil {
		return tx.Commit()
	}

	return nil
}

func (p *postgresRepository[K, T]) Upsert(ctx context.Context, id K, value T, options ...QueryOption) error {

	return nil
}

func (p *postgresRepository[K, T]) UpsertAll(ctx context.Context, values []T, options ...QueryOption) error {

	return nil
}

func (p *postgresRepository[K, T]) Delete(ctx context.Context, id []K, options ...QueryOption) error {
	return fmt.Errorf("delete operation not supported yet")
}

func (p *postgresRepository[K, T]) Begin(ctx context.Context) (Transaction, error) {
	tx, err := p.db.Beginx()
	if err != nil {
		return nil, err
	}

	return &sqlTransaction{Tx: tx}, nil
}

func (p *postgresRepository[K, T]) createUpdateQuery(id K, keyvals map[string]interface{}) (qry string, args []any) {
	tb := p.tableDef
	var sets []string
	for k, v := range keyvals {
		sets = append(sets, fmt.Sprintf("%s = ?", k))
		args = append(args, v)
	}

	qry = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s = ?", tb.Schema, tb.Name, strings.Join(sets, ","), tb.KeyField)
	args = append(args, id)
	return
}

func (p *postgresRepository[K, T]) parseFilterMapIntoWhereClause(filterMap map[string]any) (string, []interface{}, error) {
	where := ""
	var args []interface{}
	for k, v := range filterMap {
		if !p.modelTagExists(k) {
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

func (p *postgresRepository[K, T]) parameterizedFilterCriteriaSlice(fieldname string, values interface{}) (string, interface{}, error) {
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

func (p *postgresRepository[K, T]) createTransaction(opt *queryOption) (*sqlx.Tx, error) {
	if opt.Tx != nil {
		if tx, ok := opt.Tx.(*sqlTransaction); ok {
			return tx.Tx, nil
		}
	}

	return p.db.Beginx()
}

func (p *postgresRepository[K, T]) createMultiInsertQuery(values []T) (strSql string, args []any, err error) {
	if len(values) == 0 {
		err = fmt.Errorf("values is zero length slice")
		return
	}

	tb := p.tableDef
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

	var insertFields []Column
	for _, c := range insertColumnNames {
		cname := strings.ToUpper(c)
		if _, ok := columnMap[cname]; ok {
			insertFields = append(insertFields, columnMap[cname])
		}
	}

	var insertValues []string
	for i, _ := range values {
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

	strSql = fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES %s", tb.Schema, tb.Name, strings.Join(insertColumnNames, ","), strings.Join(insertValues, ","))

	strSql, args, err = sqlx.In(strSql, args...)

	return
}

func pgCreateTableDef(mval reflect.Value) (TabledDef, error) {
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
		dtype := convertTypeToPgType(col.Type)
		if dtype == "UNKNOWN" {
			return tbdef, fmt.Errorf("unknown datatype for Go type %s in %s", col.Type.Name(), mval.Type().Name())
		}

		cols = append(cols, Column{
			ColumnName: col.Name,
			DataType:   dtype,
		})

		coltype := dtype
		isArray := false
		if strings.HasPrefix(dtype, "ARRAY_") {
			coltype = dtype[6:]
			isArray = true
		}

		if coltype == "VARCHAR2" && col.Size == 0 {
			return tbdef, fmt.Errorf("VARCHAR definition requires size more than 0")
		}

		var ddlCol strings.Builder
		ddlCol.WriteString(fmt.Sprintf("%s %s", col.Name, coltype))
		if col.Size > 0 {
			ddlCol.WriteString(fmt.Sprintf("(%d)", col.Size))
		}
		if isArray {
			ddlCol.WriteString("[]")
		}

		if !col.AllowNull {
			ddlCol.WriteString(" NOT NULL")
		}

		if col.IsKey {
			keyField = col.Name
			ddlCol.WriteString(" PRIMARY KEY")
		}

		ddlCols = append(ddlCols, ddlCol.String())
	}

	createDDL := fmt.Sprintf("CREATE TABLE %s.%s (%s)", schema, table, strings.Join(ddlCols, ","))

	return TabledDef{
		Name:      table,
		Schema:    schema,
		KeyField:  keyField,
		Columns:   cols,
		CreateDDL: createDDL,
	}, nil
}

func convertTypeToPgType(model reflect.Type) string {
	kind := model.Kind()
	switch kind {
	case reflect.String:
		return "VARCHAR2"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint,
		reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "INTEGER"
	case reflect.Int64, reflect.Uint64:
		return "BIGINT"
	case reflect.Float32:
		return "REAL"
	case reflect.Float64:
		return "DOUBLE PRECISION"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Slice:
		subkind := convertTypeToPgType(model.Elem())
		return "ARRAY_" + subkind
	case reflect.Struct:
		switch model.Name() {
		case "Time":
			return "TIMESTAMP"
		case "NullString", "String":
			return "VARCHAR2"
		case "NullFloat64", "Float":
			return "DOUBLE PRECISION"
		case "NullInt32", "NullInt64", "Int":
			return "INTEGER"
		default:
			return "UNKNOWN"
		}
	default:
		return "UNKNOWN"
	}
}

func pgGetColumns(db *sqlx.DB, schema, table string) ([]Column, error) {
	qry := `
		SELECT
			column_name "column_name"
			,CASE WHEN strpos(data_type, 'timestamp') > 0 THEN 'timestamp'
				WHEN strpos(data_type, 'character varying') > 0 THEN 'varchar'
				ELSE data_type END "data_type"
		FROM information_schema.columns
		WHERE
			table_schema = ?
			and table_name = ?`

	qry = db.Rebind(qry)
	var cols []Column
	if err := db.Select(&cols, qry, schema, table); err != nil {
		return nil, err
	}

	return cols, nil
}

func CreatePgLimitOffsetSql(limit int, offset int64) string {
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

func PostgreLoader(db *sqlx.DB, schema, name string, columns []string, rows <-chan PgSourceRow) error {
	ctx := context.Background()
	conn, err := db.Connx(ctx)
	if err != nil {
		return err
	}

	cerr := conn.Raw(func(dcon any) error {
		pgconn := dcon.(*stdlib.Conn).Conn()
		tx, err := pgconn.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		n, err := tx.CopyFrom(ctx, pgx.Identifier{schema, name}, columns, ChanToCopyFromSource(rows))
		if err != nil {
			fmt.Println("rolling back import")
			return err
		}
		fmt.Println(n, "rows imported")
		fmt.Println("committing import")
		return tx.Commit(ctx)
	})
	if cerr != nil {
		fmt.Println("ERROR CONN:", cerr)
	}

	return cerr
}

func PGXPostgreLoader(cfg PGConfig, schema, name string, columns []string, rows <-chan PgSourceRow) error {
	ctx := context.Background()
	conn, err := PGXConnectPostgresql(cfg)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	n, err := tx.CopyFrom(ctx, pgx.Identifier{schema, name}, columns, ChanToCopyFromSource(rows))
	if err != nil {
		return err
	}

	fmt.Printf("\nimported %d rows\n", n)
	return tx.Commit(ctx)
}

type PgSourceRow struct {
	value []any
	err   error
}

func NewPgSourceRow(value []any, err ...error) PgSourceRow {
	src := PgSourceRow{value: value}
	if len(err) > 0 {
		src.err = err[0]
	}

	return src
}

func ChanToCopyFromSource(ch <-chan PgSourceRow) pgx.CopyFromSource {
	return &chanCopyFromSource{
		ch:     ch,
		err:    nil,
		values: nil,
	}
}

type chanCopyFromSource struct {
	ch     <-chan PgSourceRow
	err    error
	values []any
}

func (chs *chanCopyFromSource) Next() bool {
	v := <-chs.ch
	if v.err != nil {
		chs.err = v.err
		return false
	}

	if v.value == nil {
		fmt.Println("no more rows received")
		return false
	}

	chs.values = v.value
	return true
}

func (chs *chanCopyFromSource) Values() ([]any, error) {
	return chs.values, chs.err
}

func (chs *chanCopyFromSource) Err() error {
	return chs.err
}
