// Copyright (C) xooooooox

package sea

import (
	"database/sql"
	"fmt"
	"log"
)

// Execs Execute sql and args
type Execs struct {
	Sql  string
	Args []interface{}
}

// DB Database connect instance
var DB *sql.DB

// LogSql Whether to print sql
var LogSql bool

// Exec Execute a sql statement return affected rows
func Exec(query string, args ...interface{}) (int64, error) {
	if LogSql {
		fmt.Println(DatetimeUnixNano(), query, args)
	}
	result, err := DB.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Atom Atom execute many sql
func Atom(query []Execs) error {
	length := len(query)
	if length == 0 {
		return nil
	}
	if length == 1 {
		_, err := Exec(query[0].Sql, query[1].Args...)
		return err
	}
	tx, err := DB.Begin()
	if err != nil {
		return err
	}
	for i := 0; i < length; i++ {
		result, err := tx.Exec(query[i].Sql, query[i].Args...)
		if err != nil {
			txErr := tx.Rollback()
			if txErr != nil {
				err = txErr
			}
			return err
		}
		_, err = result.RowsAffected()
		if err != nil {
			txErr := tx.Rollback()
			if txErr != nil {
				err = txErr
			}
			return err
		}
	}
	txErr := tx.Commit()
	if txErr != nil {
		err = txErr
	}
	if LogSql {
		defer func() {
			for _, v := range query {
				log.Println(v.Sql, v.Args)
			}
		}()
	}
	return nil
}
