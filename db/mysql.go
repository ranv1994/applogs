package db

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQL struct {
	DB *sql.DB
}

// NewMySQL creates a new MySQL connection using the existing DB_URL format
func NewMySQL(dsn string) (*MySQL, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// Set connection pool parameters
	db.SetMaxOpenConns(100)          // Maximum number of open connections
	db.SetMaxIdleConns(10)           // Maximum number of idle connections
	db.SetConnMaxLifetime(time.Hour) // Maximum lifetime of a connection

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &MySQL{DB: db}, nil
}

// Close closes the MySQL connection
func (m *MySQL) Close() error {
	return m.DB.Close()
}

// GetDB returns the underlying sql.DB instance
func (m *MySQL) GetDB() *sql.DB {
	return m.DB
}

// Transaction executes a function within a database transaction
func (m *MySQL) Transaction(fn func(*sql.Tx) error) error {
	tx, err := m.DB.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
