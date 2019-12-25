// Copyright (C) xooooooox

package sea

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// SqlArgs execute sql and args
type SqlArgs struct {
	Sql  string
	Args []interface{}
}

// db database connect instance
var db *sql.DB

// Instance db instance
func Instance(instance *sql.DB) error {
	if instance == nil {
		return errors.New("nil value")
	}
	err := instance.Ping()
	if err != nil {
		return err
	}
	db = instance
	return nil
}

// Add insert one or more rows, auto_increment id / the number of affected rows, error
func Add(adds ...interface{}) (int64, error) {
	length := len(adds)
	if length == 0 {
		return 0, nil
	} else if length == 1 {
		return AddRaw(adds[0])
	} else {
		return AddRaws(adds...)
	}
}

// Del delete record from table, return affected rows
func Del(table interface{}, where string, args ...interface{}) (int64, error) {
	err := errors.New("unsupported data type")
	tab := ""
	t, v := reflect.TypeOf(table), reflect.ValueOf(table)
	kind := t.Kind()
	switch kind {
	case reflect.String:
		tab = table.(string)
	case reflect.Struct:
		tab = PascalToUnderline(t.Name())
	case reflect.Ptr:
		t, v = t.Elem(), v.Elem()
		kind = t.Kind()
		switch kind {
		case reflect.String:
			tab = v.String()
		case reflect.Struct:
			tab = PascalToUnderline(t.Name())
		default:
			return 0, err
		}
	default:
		return 0, err
	}
	return Exec(fmt.Sprintf("DELETE FROM `%s` WHERE (%s)", tab, where), args...)
}

// Mod update database.table values
func Mod(table interface{}, update string, where string, args ...interface{}) (int64, error) {
	err := errors.New("unsupported data type")
	tab := ""
	t, v := reflect.TypeOf(table), reflect.ValueOf(table)
	kind := t.Kind()
	switch kind {
	case reflect.String:
		tab = table.(string)
	case reflect.Struct:
		tab = PascalToUnderline(t.Name())
	case reflect.Ptr:
		t, v = t.Elem(), v.Elem()
		kind = t.Kind()
		switch kind {
		case reflect.String:
			tab = v.String()
		case reflect.Struct:
			tab = PascalToUnderline(t.Name())
		default:
			return 0, err
		}
	default:
		return 0, err
	}
	return Exec(fmt.Sprintf("UPDATE `%s` SET %s WHERE (%s)", tab, update, where), args...)
}

// Get exec query sql, return *sql.Rows data to specific interface
func Get(checkout interface{}, query string, args ...interface{}) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	return GetRows(rows, checkout)
}

// AddRaw return the auto_increment id value and an error
func AddRaw(raw interface{}) (int64, error) {
	err := errors.New("neither *AnyStruct nor AnyStruct")
	t, v := reflect.TypeOf(raw), reflect.ValueOf(raw)
	kind := t.Kind()
	if kind == reflect.Ptr {
		t, v = t.Elem(), v.Elem()
		kind = t.Kind()
	}
	if kind != reflect.Struct {
		return 0, err
	}
	table := PascalToUnderline(t.Name())
	column := ""
	values := ""
	args := []interface{}{}
	length := t.NumField()
	for i := 0; i < length; i++ {
		column += fmt.Sprintf("`%s`,", PascalToUnderline(t.Field(i).Name))
		values += "?,"
		args = append(args, v.Field(i).Interface())
	}
	column = strings.TrimRight(column, ",")
	values = strings.TrimRight(values, ",")
	return ExecId(fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", table, column, values), args...)
}

// AddRaws return the number of affected rows and an error
func AddRaws(raws ...interface{}) (int64, error) {
	length := len(raws)
	if length == 0 {
		return 0, nil
	}
	err := errors.New("some members are neither *AnyStruct nor AnyStruct")
	adds := make([]struct {
		Table  string
		Column string
		Values string
		Args   []interface{}
	}, length, length)
	for i := 0; i < length; i++ {
		t, v := reflect.TypeOf(raws[i]), reflect.ValueOf(raws[i])
		kind := t.Kind()
		if kind == reflect.Ptr {
			t, v = t.Elem(), v.Elem()
			kind = t.Kind()
		}
		if kind != reflect.Struct {
			return 0, err
		}
		for j := 0; j < v.NumField(); j++ {
			adds[i].Table = PascalToUnderline(t.Name())
			adds[i].Column = fmt.Sprintf("%s`%s`,", adds[i].Column, PascalToUnderline(t.Field(j).Name))
			adds[i].Values = fmt.Sprintf("%s?,", adds[i].Values)
			adds[i].Args = append(adds[i].Args, v.Field(j).Interface())
		}
		adds[i].Column = strings.TrimRight(adds[i].Column, ",")
		adds[i].Values = strings.TrimRight(adds[i].Values, ",")
	}
	addRows := map[string]struct {
		Sql  string
		Args []interface{}
	}{}
	for i := 0; i < length; i++ {
		table := adds[i].Table
		if addRows[table].Sql == "" {
			addRows[table] = struct {
				Sql  string
				Args []interface{}
			}{
				Sql:  fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", adds[i].Table, adds[i].Column, adds[i].Values),
				Args: adds[i].Args,
			}
			continue
		}
		addRows[table] = struct {
			Sql  string
			Args []interface{}
		}{
			Sql:  fmt.Sprintf("%s,(%s)", addRows[table].Sql, adds[i].Values),
			Args: append(addRows[table].Args, adds[i].Args...),
		}
	}
	var rows int64 = 0
	for _, val := range addRows {
		row, err := Exec(val.Sql, val.Args...)
		if err != nil {
			return 0, err
		}
		rows += row
	}
	return rows, nil
}

// check must be &[]AnyStruct, &[]*AnyStruct,&AnyStruct
// database table column value cannot be null, database allow filed is null value, and rows.Scan(...) will panic
// named columns with uppercase letters will result in null values ​​of the corresponding type, for example: TABLE_NAME, Table_name, table_Name
// when the column value is null, take string type as an example:
// Tip: Although there are ways to deal with the issue of allowing null values, it is not recommended to use allow null
// 1:structure field type string => *string
// 2:structure field type string => sql.NullString
// 3:sql  SELECT IFNULL(`age`,0) AS `age`,IFNULL(`name`,'Bob') AS `name`,IFNULL(`email`,'') AS `email` FROM ...
// 4:sql  SELECT COALESCE(`age`,0) AS `age`,COALESCE(`name`,'Bob') AS `name`,COALESCE(`email`,'') AS `email` FROM ...
// GetRows checkout *sql.rows data to interface
func GetRows(rows *sql.Rows, check interface{}) error {
	err := errors.New("is not *[]AnyStruct, *[]*AnyStruct or *AnyStruct type")
	ct, cv := reflect.TypeOf(check), reflect.ValueOf(check)
	if ct.Kind() != reflect.Ptr {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	ctElemKind := ct.Elem().Kind()
	switch ctElemKind {
	// *[]interface{} type
	case reflect.Slice:
		children := cv.Elem()
		reflectZeroValue := reflect.Value{}
		// *[]AnyStruct type
		if ct.Elem().Elem().Kind() == reflect.Struct {
			for rows.Next() {
				childValue := reflect.New(ct.Elem().Elem())
				childVal := reflect.Indirect(childValue)
				fields := []interface{}{}
				for _, column := range columns {
					// force all field names to lowercase to prevent rows.Scan panic
					columnLower := strings.ToLower(column)
					field := childVal.FieldByName(UnderlineToPascal(columnLower))
					if field == reflectZeroValue || !field.CanSet() {
						bytesTypePtrValue := reflect.New(reflect.TypeOf([]byte{}))
						bytesTypePtrValue.Elem().Set(reflect.ValueOf([]byte{}))
						fields = append(fields, bytesTypePtrValue.Interface())
						continue
					}
					fields = append(fields, field.Addr().Interface())
				}
				err := rows.Scan(fields...)
				if err != nil {
					return err
				}
				children = reflect.Append(children, childValue.Elem())
			}
			reflect.ValueOf(check).Elem().Set(children)
			return nil
		}
		// *[]*AnyStruct type
		if ct.Elem().Elem().Kind() == reflect.Ptr && ct.Elem().Elem().Elem().Kind() == reflect.Struct {
			for rows.Next() {
				childValue := reflect.New(ct.Elem().Elem().Elem())
				childVal := reflect.Indirect(childValue)
				fields := []interface{}{}
				for _, column := range columns {
					// force all field names to lowercase to prevent rows.Scan panic
					columnLower := strings.ToLower(column)
					field := childVal.FieldByName(UnderlineToPascal(columnLower))
					if field == reflectZeroValue || !field.CanSet() {
						bytesTypePtrValue := reflect.New(reflect.TypeOf([]byte{}))
						bytesTypePtrValue.Elem().Set(reflect.ValueOf([]byte{}))
						fields = append(fields, bytesTypePtrValue.Interface())
						continue
					}
					fields = append(fields, field.Addr().Interface())
				}
				err := rows.Scan(fields...)
				if err != nil {
					return err
				}
				children = reflect.Append(children, childValue)
			}
			reflect.ValueOf(check).Elem().Set(children)
			return nil
		}
		return err
	// *AnyStruct type
	case reflect.Struct:
		reflectZeroValue := reflect.Value{}
		childValue := reflect.New(ct.Elem())
		childVal := reflect.Indirect(childValue)
		fields := []interface{}{}
		rows.Next()
		for _, column := range columns {
			// force all field names to lowercase to prevent rows.Scan panic
			columnLower := strings.ToLower(column)
			field := childVal.FieldByName(UnderlineToPascal(columnLower))
			if field == reflectZeroValue || !field.CanSet() {
				bytesTypePtrValue := reflect.New(reflect.TypeOf([]byte{}))
				bytesTypePtrValue.Elem().Set(reflect.ValueOf([]byte{}))
				fields = append(fields, bytesTypePtrValue.Interface())
				continue
			}
			fields = append(fields, field.Addr().Interface())
		}
		err := rows.Scan(fields...)
		if err != nil {
			return err
		}
		reflect.ValueOf(check).Elem().Set(childValue.Elem())
		return nil
	// unknown type
	default:
		return err
	}
}

// Exec execute sql statement return affected rows
func Exec(execute string, args ...interface{}) (int64, error) {
	result, err := db.Exec(execute, args...)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

// ExecId execute insert sql statement return insert record id
func ExecId(execute string, args ...interface{}) (int64, error) {
	result, err := db.Exec(execute, args...)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

// Atom atom execute more sql
func Atom(execute []SqlArgs) error {
	length := len(execute)
	if length == 0 {
		return nil
	}
	if length == 1 {
		_, err := Exec(execute[0].Sql, execute[1].Args...)
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	for i := 0; i < length; i++ {
		result, err := tx.Exec(execute[i].Sql, execute[i].Args...)
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
	return nil
}

// Batch execute more sql
func Batch(execute []SqlArgs) error {
	length := len(execute)
	for i := 0; i < length; i++ {
		_, err := Exec(execute[i].Sql, execute[i].Args...)
		if err != nil {
			return err
		}
	}
	return nil
}
