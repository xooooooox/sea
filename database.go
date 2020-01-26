// Copyright (C) xooooooox

package sea

import (
	"database/sql"
)

// DB Database connect instance
var DB *sql.DB

// Exec Execute a sql statement return affected rows and an error
func Exec(query string, args ...interface{}) (int64, error) {
	result, err := DB.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
