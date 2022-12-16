package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

type sqlTransaction struct {
	Tx *sqlx.Tx
}

func (st *sqlTransaction) Rollback(_ context.Context) error {
	return st.Tx.Rollback()
}

func (st *sqlTransaction) Commit(_ context.Context) error {
	return st.Tx.Commit()
}

func wrapPostgresError(err error) error {
	errMap := map[error]error{
		sql.ErrNoRows: ErrKeynotFound,
	}

	for g, e := range errMap {
		if errors.Is(err, g) {
			err = fmt.Errorf("%w. %s", e, err.Error())
		}
	}

	return err
}

func MakeSortClause(sorter []string, sortFieldMap map[string]string) string {
	if len(sorter) == 0 {
		return ""
	}

	var srt []string
	for _, s := range sorter {
		op := ""
		field := strings.ToLower(s)
		if s[:1] == "-" || s[:1] == "+" {
			op = s[:1]
			field = strings.ToLower(s[1:])
		}

		if op == "-" {
			op = "DESC"
		} else {
			op = "ASC"
		}

		if sortFieldMap != nil {
			if mf, ok := sortFieldMap[field]; ok {
				field = mf
			}
		}

		srt = append(srt, fmt.Sprintf("%s %s", field, op))
	}

	return strings.Join(srt, ",")
}

type FilterNull interface {
	IsNull() bool
}

type filterNull bool

func (fn filterNull) IsNull() bool {
	return bool(fn)
}

func FilterNullFrom(isNull bool) FilterNull {
	return filterNull(isNull)
}

type FilterStringContains interface {
	Contains() string
}

type filterStringContains string

func (fs filterStringContains) Contains() string {
	return fmt.Sprintf("%%%s%%", fs)
}

func FilterStringContainsFrom(str string) FilterStringContains {
	return filterStringContains(str)
}

func ParseFilterMapIntoWhereClause(filterMap map[string]any) (whereClause string, args []any, err error) {
	where := ""
	for k, v := range filterMap {
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

		if fcontain, ok := val.(FilterStringContains); ok {
			if len(where) > 0 {
				where += " AND "
			}
			where += fmt.Sprintf("%s like ?", k)
			args = append(args, fcontain.Contains())
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
			if f, arg, err := parameterizedFilterCriteriaSlice(k, val); err == nil {
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

func parameterizedFilterCriteriaSlice(fieldname string, values interface{}) (string, any, error) {
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
