package sea

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// DatabaseInsert the same table "INSERT INTO `table` (`col1`,`col2`,`col3`) VALUES (1,2,3),(1,2,3),(1,2,3)"
type DatabaseInsert struct {
	// insert table name
	Table string
	// insert columns
	Column string
	// insert values
	Values string
	// insert args
	Args []interface{}
}

// InsertMoreInSameTable the same table, insert more rows
type InsertMoreInSameTable struct {
	Sql  string
	Args []interface{}
}

// Insert return the auto_increment id value or the number of affected rows and an error
func Insert(x ...interface{}) (int64, error) {
	xl := len(x)
	if xl == 0 {
		return 0, errors.New("please insert at least one record")
	}
	if xl == 1 {
		x0t := reflect.TypeOf(x[0])
		x0tk := x0t.Kind()
		if x0tk != reflect.Ptr && x0tk != reflect.Struct {
			return 0, errors.New("data types other than unstructured or unstructured pointers are not supported")
		}
		return InsertOne(x[0])
	}
	add := []interface{}{}
	for i := 0; i < xl; i++ {
		xit := reflect.TypeOf(x[i])
		xitk := xit.Kind()
		if xitk != reflect.Ptr && xitk != reflect.Struct {
			return 0, errors.New("data types other than unstructured or unstructured pointers are not supported")
		}
		add = append(add, x[i])
	}
	return InsertMore(add...)
}

// InsertOne return the auto_increment id value and an error
func InsertOne(x interface{}) (id int64, err error) {
	t, v := reflect.TypeOf(x), reflect.ValueOf(x)
	kind := t.Kind()
	if kind == reflect.Ptr {
		t, v = t.Elem(), v.Elem()
		kind = t.Kind()
	}
	if kind != reflect.Struct {
		return 0, errors.New("neither *AnyStruct nor AnyStruct")
	}
	table := DatabaseInsert{
		Table:  PascalToUnderline(t.Name()),
		Column: "",
		Values: "",
		Args:   []interface{}{},
	}
	length := t.NumField()
	for i := 0; i < length; i++ {
		table.Column += fmt.Sprintf("`%s`,", PascalToUnderline(t.Field(i).Name))
		table.Values += "?,"
		table.Args = append(table.Args, v.Field(i).Interface())
	}
	table.Column = strings.TrimRight(table.Column, ",")
	table.Values = strings.TrimRight(table.Values, ",")
	return ExecuteInsertOne(db, fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", table.Table, table.Column, table.Values), table.Args...)
}

// InsertMore return the number of affected rows and an error
func InsertMore(x ...interface{}) (int64, error) {
	err := errors.New("some members are neither *AnyStruct nor AnyStruct")
	length := len(x)
	if length == 0 {
		return 0, err
	}
	add := make([]DatabaseInsert, length, length)
	for i := 0; i < length; i++ {
		t, v := reflect.TypeOf(x[i]), reflect.ValueOf(x[i])
		kind := t.Kind()
		if kind == reflect.Ptr {
			t, v = t.Elem(), v.Elem()
			kind = t.Kind()
		}
		if kind != reflect.Struct {
			return 0, err
		}
		add[i].Table = PascalToUnderline(t.Name())
		for j := 0; j < v.NumField(); j++ {
			add[i].Column += fmt.Sprintf("`%s`,", PascalToUnderline(t.Field(j).Name))
			add[i].Values += "?,"
			add[i].Args = append(add[i].Args, v.Field(j).Interface())
		}
	}
	tables := map[string]InsertMoreInSameTable{}
	for i := 0; i < length; i++ {
		add[i].Column = strings.TrimRight(add[i].Column, ",")
		add[i].Values = strings.TrimRight(add[i].Values, ",")
		if tables[add[i].Table].Sql == "" {
			tables[add[i].Table] = InsertMoreInSameTable{
				Sql:  fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", add[i].Table, add[i].Column, add[i].Values),
				Args: add[i].Args,
			}
			continue
		}
		appends := InsertMoreInSameTable{
			Sql:  fmt.Sprintf("%s,(%s)", tables[add[i].Table].Sql, add[i].Values),
			Args: append(tables[add[i].Table].Args, add[i].Args...),
		}
		tables[add[i].Table] = appends
	}
	var rows int64 = 0
	for _, v := range tables {
		row, err := Execute(db, v.Sql, v.Args...)
		if err != nil {
			return 0, err
		}
		rows += row
	}
	return rows, nil
}
