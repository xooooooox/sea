package sea

import (
	"database/sql"
)

// Select
func Select(export interface{}, query string, args ...interface{}) error {
	return Query(db,export,query,args...)
}

// Query exec query sql, return *sql.Rows data to specific interface
func Query(db *sql.DB, export interface{}, query string, args ...interface{}) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	return SqlRowsExport(rows, export)
}
