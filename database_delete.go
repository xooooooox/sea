package sea

import (
	"errors"
	"fmt"
	"reflect"
)

// Delete delete record from table, return affected rows
func Delete(table interface{}, args ...interface{}) (int64, error) {
	t, v := reflect.TypeOf(table), reflect.ValueOf(table)
	kind := t.Kind()
	if kind == reflect.Ptr {
		t, v = t.Elem(), v.Elem()
		kind = t.Kind()
	}
	if kind != reflect.Struct {
		return 0, errors.New("neither *AnyStruct nor AnyStruct")
	}
	sql := fmt.Sprintf("DELETE FROM `%s` ", PascalToUnderline(t.Name()))
	lengthArgs := len(args)
	// set fixed where conditions
	if lengthArgs == 0 {
		return Execute(db, sql)
	}
	// set conditions and carry parameters, where:args[0] args:args[1:]...
	sql += fmt.Sprintf(" WHERE (%s)", args[0])
	if lengthArgs == 1 {
		return Execute(db, sql)
	}
	return Execute(db, sql, args[1:]...)
}

// DeleteById delete record by id from table, return affected rows
func DeleteById(table interface{}, id int64) (int64, error) {
	t, v := reflect.TypeOf(table), reflect.ValueOf(table)
	kind := t.Kind()
	if kind == reflect.Ptr {
		t, v = t.Elem(), v.Elem()
		kind = t.Kind()
	}
	if kind != reflect.Struct {
		return 0, errors.New("neither *AnyStruct nor AnyStruct")
	}
	return Execute(db, fmt.Sprintf("DELETE FROM `%s` WHERE (`id`=?)", PascalToUnderline(t.Name())), id)
}
