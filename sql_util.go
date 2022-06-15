package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
