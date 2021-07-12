package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

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
