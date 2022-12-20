package store

import (
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

type PGConfig struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
}

func ConnectPostgresql(config PGConfig) (*sqlx.DB, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", config.User, config.Password, config.Host, config.Port, config.Database)
	return sqlx.Open("pgx", connStr)
}
