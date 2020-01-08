// Copyright (C) xooooooox

package sea

import (
	"database/sql"
	"fmt"
	"log"
)

// Execute Execute sql and args
type Execute struct {
	Sql  string
	Args []interface{}
}

// DB Database connect instance
var DB *sql.DB

// LogSql Whether to print sql
var LogSql bool

// FlutterSql Beautify sql
var FlutterSql bool

// init
func init() {
	FlutterSql = true
}

// Exec Execute a sql statement return affected rows
func Exec(execute string, args ...interface{}) (int64, error) {
	if LogSql {
		fmt.Println(DatetimeUnixNano(), execute, args)
	}
	result, err := DB.Exec(execute, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Atom Atom execute many sql
func Atom(executes []Execute) error {
	length := len(executes)
	if length == 0 {
		return nil
	}
	if length == 1 {
		_, err := Exec(executes[0].Sql, executes[1].Args...)
		return err
	}
	tx, err := DB.Begin()
	if err != nil {
		return err
	}
	for i := 0; i < length; i++ {
		result, err := tx.Exec(executes[i].Sql, executes[i].Args...)
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
			for _, v := range executes {
				log.Println(v.Sql, v.Args)
			}
		}()
	}
	return nil
}
