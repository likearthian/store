package store

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

type postgresRepository struct {
	repository
	db        *sqlx.DB
	tableName string
}

func CreatePostgresRepository(db *sqlx.DB, tableName string, model Model, options ...RepositoryOption) (Repository, error) {
	opt := &option{}
	for _, op := range options {
		op(opt)
	}

	if opt.name == "" {
		opt.name = tableName
	}

	m := reflect.TypeOf(model)
	if m.Kind() == reflect.Ptr {
		m = m.Elem()
	}

	modelTags := createModelTags(m, "db")

	repo := &postgresRepository{
		repository: repository{
			Name:      opt.name,
			model:     model,
			modelType: m,
			modelTags: modelTags,
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

func (p *postgresRepository) init(values interface{}) error {
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

func (p *postgresRepository) Get(ctx context.Context, id interface{}, dest interface{}, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	destType := reflect.TypeOf(dest)
    if destType.Kind() != reflect.Ptr {
        return fmt.Errorf("dest need to be a pointer to %s, gets %T", p.modelType.Name(), dest)
    }

    if destType.Elem() != p.modelType {
    	return fmt.Errorf("dest need to be a pointer to %s, gets %T", p.modelType.Name(), dest)
	}

    columns := strings.Join(GetColumnsFromModelType(p.modelType, "db"), ",")
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

	return tx.Commit()
}

func (p *postgresRepository) Select(ctx context.Context, filterMap map[string]interface{}, dest interface{}, options ...QueryOption) error {
	opt := &queryOption{}
	for _, op := range options {
		op(opt)
	}

	destType := reflect.TypeOf(dest)
    if destType.Kind() != reflect.Ptr {
        return fmt.Errorf("dest need to be a pointer to Slice of %s or *%s, gets %T", p.modelType.Name(), p.modelType.Name(), dest)
    }

    if destType.Elem() != p.modelType {
    	return fmt.Errorf("dest need to be a pointer to Slice of %s or *%s, gets %T", p.modelType.Name(), p.modelType.Name(), dest)
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

	columns := strings.Join(GetColumnsFromModelType(p.modelType, "db"), ",")
    tableDef := p.model.GetTableDef()
    qry := fmt.Sprintf("SELECT %s FROM %s.%s %s", columns, tableDef.Schema, tableDef.Name, filter)
    qry = tx.Rebind(qry)

    if err := tx.SelectContext(ctx, dest, qry, argParam...); err != nil {
    	return wrapPostgresError(err)
	}

	return nil
}

func (p *postgresRepository) SQLQuery(ctx context.Context, dest interface{}, sqlStr string, args []interface{}, options ...QueryOption) error {
	return fmt.Errorf("not implemented yet")
}

func (p *postgresRepository) SQLExec(ctx context.Context, sqlStr string, args []interface{}, options ...QueryOption) error {
	return fmt.Errorf("not implemented yet")
}

func (p *postgresRepository) Put(ctx context.Context, value interface{}, options ...QueryOption) (interface{}, error) {
	valTyp := reflect.TypeOf(value)
	if valTyp != p.modelType {
		return nil, fmt.Errorf("cannot put value. value is not a type of %s", p.modelType.Name())
	}

	return nil, nil
}

func (p *postgresRepository) Replace(ctx context.Context, id interface{}, value interface{}, options ...QueryOption) error {
	valTyp := reflect.TypeOf(value)
	if valTyp.Kind() == reflect.Ptr {
		valTyp = valTyp.Elem()
	}

	if valTyp != p.modelType {
		return fmt.Errorf("cannot update value. value is not a type of %s", p.modelType.Name())
	}

	return nil
}

func (p *postgresRepository) Update(ctx context.Context, id interface{}, keyvals map[string]interface{}, options ...QueryOption) error {
	return nil
}

func (p *postgresRepository) Upsert(ctx context.Context, id interface{}, value interface{}, options ...QueryOption) error {
	valTyp := reflect.TypeOf(value)
	if valTyp.Kind() == reflect.Ptr {
		valTyp = valTyp.Elem()
	}

	if valTyp != p.modelType {
		return fmt.Errorf("cannot update value. value is not a type of %s", p.modelType.Name())
	}

	return nil
}

func (p *postgresRepository) Begin(ctx context.Context) (Transaction, error) {
	tx, err := p.db.Beginx()
	if err != nil {
		return nil, err
	}

	return &sqlTransaction{Tx: tx}, nil
}

func (p *postgresRepository) createUpdateParam(keyvals map[string]interface{}) interface{} {



	return nil
}

func (p *postgresRepository) parseFilterMapIntoWhereClause(filterMap map[string]interface{}) (string, []interface{}, error) {
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

func (p *postgresRepository) parameterizedFilterCriteriaSlice(fieldname string, values interface{}) (string, interface{}, error) {
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

func (p *postgresRepository) createTransaction(opt *queryOption) (*sqlx.Tx, error) {
	if opt.Tx != nil {
		if tx, ok := opt.Tx.(*sqlTransaction); ok {
			return tx.Tx, nil
		}
	}

	return p.db.Beginx()
}
