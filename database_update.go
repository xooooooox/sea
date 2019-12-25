package sea

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Update update database.table values
func Update(table interface{}, update map[string]interface{}, args ...interface{}) (int64, error) {
	t, v := reflect.TypeOf(table), reflect.ValueOf(table)
	kind := t.Kind()
	if kind == reflect.Ptr {
		t, v = t.Elem(), v.Elem()
		kind = t.Kind()
	}
	if kind != reflect.Struct {
		return 0, errors.New("neither *AnyStruct nor AnyStruct")
	}
	tableName := PascalToUnderline(t.Name())
	setColumn := ""
	bindArgs := []interface{}{}
	for k, v := range update {
		setColumn += fmt.Sprintf("`%s`=?,", k)
		bindArgs = append(bindArgs, v)
	}
	// delete at the end ","
	setColumn = strings.TrimRight(setColumn, ",")
	sql := fmt.Sprintf("UPDATE `%s` SET %s ", tableName, setColumn)
	lengthArgs := len(args)
	// no where conditions
	if lengthArgs == 0 {
		// need with set args
		return Execute(db, sql, bindArgs...)
	}
	// set fixed where conditions
	sql += fmt.Sprintf(" WHERE (%s)", args[0])
	if lengthArgs == 1 {
		return Execute(db, sql,bindArgs...)
	}
	// set conditions and carry parameters, where:args[0] args:args[1:]...
	bindArgs = append(bindArgs, args[1:]...)
	return Execute(db, sql, bindArgs...)
}

// UpdateById update database.table values by id
func UpdateById(table interface{}, update map[string]interface{}, id int64) (int64, error) {
	t, v := reflect.TypeOf(table), reflect.ValueOf(table)
	kind := t.Kind()
	if kind == reflect.Ptr {
		t, v = t.Elem(), v.Elem()
		kind = t.Kind()
	}
	if kind != reflect.Struct {
		return 0, errors.New("neither *AnyStruct nor AnyStruct")
	}
	tableName := PascalToUnderline(t.Name())
	setColumn := ""
	bindArgs := []interface{}{}
	for k, v := range update {
		setColumn += fmt.Sprintf("`%s`=?,", k)
		bindArgs = append(bindArgs, v)
	}
	// delete at the end ","
	setColumn = strings.TrimRight(setColumn, ",")
	sql := fmt.Sprintf("UPDATE `%s` SET %s WHERE(`id`=?)", tableName, setColumn)
	bindArgs = append(bindArgs, id)
	return Execute(db, sql, bindArgs...)
}
