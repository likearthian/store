package store

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

type postgresRepository[K comparable, T Model] struct {
	repository
	db        *sqlx.DB
	tableName string
}

func CreatePostgresRepository[K comparable, T Model](db *sqlx.DB, tableName string, options ...RepositoryOption[T]) (Repository[K, T], error) {
	opt := &option[T]{}
	for _, op := range options {
		op(opt)
	}

	var model T
	m := reflect.TypeOf(model)
	if m.Kind() == reflect.Ptr {
		m = m.Elem()
	}

	modelTags := createModelTags(m, "db")

	repo := &postgresRepository[K, T]{
		repository: repository{
			Name:      model.GetTableDef().Name,
			modelTags: modelTags,
			tableDef:  model.GetTableDef(),
			modelType: m,
		},
		db:        db,
		tableName: tableName,
	}

	if opt.initValues != nil {
		if err := repo.init(opt.initValues); err != nil {
			return nil, err
		}
	}

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

	columns := strings.Join(p.columnNames, ",")
	tableDef := p.tableDef
	qry := fmt.Sprintf("SELECT %s FROM %s.%s %s", columns, tableDef.Schema, tableDef.Name, filter)
	qry = tx.Rebind(qry)

	if err := tx.SelectContext(ctx, dest, qry, argParam...); err != nil {
		return wrapPostgresError(err)
	}

	return tx.Commit()
}

func (p *postgresRepository[K, T]) SQLQuery(ctx context.Context, dest any, sqlStr string, args []interface{}, options ...QueryOption) error {
	return fmt.Errorf("not implemented yet")
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
	qry := fmt.Sprintf("INSERT INTO %s.%s VALUES (?)", tabledef.Schema, tabledef.Name)
	qry, args, err := sqlx.In(qry, values)
	qry = tx.Rebind(qry)

	_, err = tx.ExecContext(ctx, qry, args...)
	if err != nil {
		return zeroKey, wrapPostgresError(err)
	}

	return zeroKey, tx.Commit()
}

func (p *postgresRepository[K, T]) InsertAll(ctx context.Context, values []T, options ...QueryOption) ([]K, error) {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	fieldMap, err := p.createFieldsAndValuesMapFromModelType(values, "db")
	if err != nil {
		return nil, err
	}

	var columns []string
	var insertValues []interface{}
	for k, _ := range fieldMap {
		columns = append(columns, k)
		insertValues = append(insertValues, fieldMap[k])
	}

	tx, err := p.createTransaction(opt)
	if err != nil {
		return nil, err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	tabledef := p.tableDef
	qry := fmt.Sprintf("INSERT INTO %s.%s VALUES (?)", tabledef.Schema, tabledef.Name)
	qry, args, err := sqlx.In(qry, values)
	qry = tx.Rebind(qry)

	_, err = tx.ExecContext(ctx, qry, args...)
	if err != nil {
		return nil, wrapPostgresError(err)
	}

	return nil, tx.Commit()
}

func (p *postgresRepository[K, T]) Replace(ctx context.Context, id K, value T, options ...QueryOption) error {
	return fmt.Errorf("this database doesn't support Replace")
}

func (p *postgresRepository[K, T]) Update(ctx context.Context, id K, keyvals map[string]any, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	var columns []string
	var values []interface{}
	for k, _ := range keyvals {
		columns = append(columns, k)
		values = append(values, keyvals[k])
	}

	tx, err := p.createTransaction(opt)
	if err != nil {
		return err
	}

	if opt.Tx == nil {
		defer tx.Rollback()
	}

	tabledef := p.tableDef
	qry := fmt.Sprintf("INSERT INTO %s.%s VALUES (?)", tabledef.Schema, tabledef.Name)

	qry, args, err := sqlx.In(qry, values)
	qry = tx.Rebind(qry)

	_, err = tx.ExecContext(ctx, qry, args...)
	if err != nil {
		return wrapPostgresError(err)
	}

	return tx.Commit()
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

func (p *postgresRepository[K, T]) createUpdateParam(keyvals map[string]interface{}) interface{} {

	return nil
}

func (p *postgresRepository[K, T]) parseFilterMapIntoWhereClause(filterMap map[string]interface{}) (string, []interface{}, error) {
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
		dtype := convertTypeToOraType(col.Type)
		if dtype == "UNKNOWN" {
			return tbdef, fmt.Errorf("unknown datatype for Go type %s", mval.Type().Name())
		}

		cols = append(cols, Column{
			ColumnName: col.Name,
			DataType:   dtype,
		})

		if dtype == "VARCHAR2" && col.Size == 0 {
			return tbdef, fmt.Errorf("VARCHAR definition requires size more than 0")
		}

		var ddlCol strings.Builder
		ddlCol.WriteString(fmt.Sprintf("%s %s", col.Name, dtype))
		if col.Size > 0 {
			ddlCol.WriteString(fmt.Sprintf("(%d) ", col.Size))
		}

		if !col.AllowNull {
			ddlCol.WriteString("NOT NULL ")
		}

		if col.IsKey {
			keyField = col.Name
			ddlCol.WriteString("PRIMARY KEY")
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
