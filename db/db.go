package db

import (
	"database/sql"
	_ "github.com/jackc/pgx"
	_ "github.com/lib/pq"
)

var defaultPgUrl = "postgres://postgres@127.0.0.1:5432/photos?sslmode=disable"

func Connect(connStr *string) (*sql.DB, error) {
	if connStr == nil {
		connStr = &defaultPgUrl
	}

	db, err := sql.Open("postgres", *connStr)
	return db, err
}

type DataRow interface {
	GetId() string
}

type Storage interface {
	CreateTable() error
	DropTable() error
	Insert(DataRow) error
	CheckExisting(DataRow) bool
}
