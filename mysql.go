// Copyright (C) xooooooox

package sea

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// InformationSchemaTables information_schema.TABLES
type InformationSchemaTables struct {
	TableCatalog   string  `json:"table_catalog"`
	TableSchema    string  `json:"table_schema"`
	TableName      string  `json:"table_name"`
	TableType      string  `json:"table_type"`
	Engine         *string `json:"engine"`
	Version        *int64  `json:"version"`
	RowFormat      *string `json:"row_format"`
	TableRows      *int64  `json:"table_rows"`
	AvgRowLength   *int64  `json:"avg_row_length"`
	DataLength     *int64  `json:"data_length"`
	MaxDataLength  *int64  `json:"max_data_length"`
	IndexLength    *int64  `json:"index_length"`
	DataFree       *int64  `json:"data_free"`
	AutoIncrement  *int64  `json:"auto_increment"`
	CreateTime     *string `json:"create_time"`
	UpdateTime     *string `json:"update_time"`
	CheckTime      *string `json:"check_time"`
	TableCollation *string `json:"table_collation"`
	Checksum       *int64  `json:"checksum"`
	CreateOptions  *string `json:"create_options"`
	TableComment   string  `json:"table_comment"`
	MaxIndexLength *int64  `json:"max_index_length"`
	Temporary      *string `json:"temporary"`
}

// InformationSchemaColumns information_schema.COLUMNS
type InformationSchemaColumns struct {
	TableCatalog           string  `json:"table_catalog"`
	TableSchema            string  `json:"table_schema"`
	TableName              string  `json:"table_name"`
	ColumnName             string  `json:"column_name"`
	OrdinalPosition        int64   `json:"ordinal_position"`
	ColumnDefault          *string `json:"column_default"`
	IsNullable             string  `json:"is_nullable"`
	DataType               string  `json:"data_type"`
	CharacterMaximumLength *int64  `json:"character_maximum_length"`
	CharacterOctetLength   *int64  `json:"character_octet_length"`
	NumericPrecision       *int64  `json:"numeric_precision"`
	NumericScale           *int64  `json:"numeric_scale"`
	DatetimePrecision      *int64  `json:"datetime_precision"`
	CharacterSetName       *string `json:"character_set_name"`
	CollationName          *string `json:"collation_name"`
	ColumnType             string  `json:"column_type"`
	ColumnKey              string  `json:"column_key"`
	Extra                  string  `json:"extra"`
	Privileges             string  `json:"privileges"`
	ColumnComment          string  `json:"column_comment"`
	IsGenerated            string  `json:"is_generated"`
	GenerationExpression   *string `json:"generation_expression"`
}

var (
	// InformationSchemaSystemAllDatabases System database
	InformationSchemaSystemAllDatabases []string = []string{"information_schema", "mysql", "performance_schema"}
)

//func fr(s string) string {
//	s = strings.TrimSpace(s)
//	s = strings.ReplaceAll(s, " ", "")
//	switch s {
//	case "", ",", "?", "0", "''", "\"\"", "<>", "!=", ">=", "<=", ">", "<", "=", "(", ")":
//		return s
//	case "LEFT", "RIGHT", "OUT", "JOIN", "AND", "ON", "NOT", "BETWEEN", "OR", "IN", "LIKE", "AS", "ASC", "DESC":
//		return s
//	case "left", "right", "out", "join", "and", "on", "not", "between", "or", "in", "like", "as", "asc", "desc":
//		return strings.ToUpper(s)
//	default:
//	}
//	// number value
//	if StrIsNumber(s) {
//		return s
//	}
//	// string value
//	if strings.Index(s, `'`) == 0 || strings.Index(s, `"`) == 0 {
//		return s
//	}
//	// prefix and suffix WHERE ( (user = ?) AND (email = ?) )
//	if strings.HasPrefix(s, "(") {
//		return fmt.Sprintf("( %s", fr(strings.TrimPrefix(s, "(")))
//	}
//	if strings.HasSuffix(s, ")") {
//		return fmt.Sprintf("%s )", fr(strings.TrimSuffix(s, ")")))
//	}
//	// columns
//	if strings.Index(s, ",") >= 0 {
//		str := ""
//		nodes := strings.Split(s, ",")
//		for _, v := range nodes {
//			v = fr(v)
//			if str == "" {
//				str = v
//				continue
//			}
//			str = fmt.Sprintf("%s, %s", str, v)
//		}
//		return str
//	}
//	// user; id; email; user.id; u.name; u.`group`
//	s = strings.ReplaceAll(s, "`", "")
//	return fmt.Sprintf("`%s`", strings.ReplaceAll(s, ".", "`.`"))
//}

// fn Format name
func fn(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "`", "")
	switch s {
	case "", ",", "?", "0", "''", "\"\"", "<>", "!=", ">=", "<=", ">", "<", "=", "(", ")":
		return s
	default:
	}
	if strings.Index(s, ",") >= 0 {
		str := ""
		nodes := strings.Split(s, ",")
		for _, v := range nodes {
			v = fn(v)
			if str == "" {
				str = v
				continue
			}
			str = fmt.Sprintf("%s, %s", str, v)
		}
		return str
	}
	// user; id; email; user.id; u.name; u.`group`
	return fmt.Sprintf("`%s`", strings.ReplaceAll(s, ".", "`.`"))
}

// fw Format where
func fw(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "")
	switch s {
	case "", ",", "?", "0", "''", "\"\"", "<>", "!=", ">=", "<=", ">", "<", "=", "(", ")":
		return s
	case "LEFT", "RIGHT", "OUT", "JOIN", "AND", "ON", "NOT", "BETWEEN", "OR", "IN", "LIKE", "AS", "ASC", "DESC":
		return s
	case "left", "right", "out", "join", "and", "on", "not", "between", "or", "in", "like", "as", "asc", "desc":
		return strings.ToUpper(s)
	default:
	}
	// age = 18 AND balance = -0.05
	if StrIsNumber(s) {
		return s
	}
	// username = 'xooooooox' OR username = "XOOOOOOOX"
	if strings.Index(s, `'`) == 0 || strings.Index(s, `"`) == 0 {
		return s
	}
	// WHERE ( (user = ?) AND (email = ?) )
	if strings.HasPrefix(s, "(") {
		return fmt.Sprintf("( %s", fw(strings.TrimPrefix(s, "(")))
	}
	if strings.HasSuffix(s, ")") {
		return fmt.Sprintf("%s )", fw(strings.TrimSuffix(s, ")")))
	}
	return fn(s)
}

// fws Format wheres
func fws(s string) string {
	result := ""
	node := strings.Fields(s)
	for _, v := range node {
		v = fw(v)
		if result == "" {
			result = v
			continue
		}
		result = fmt.Sprintf("%s %s", result, v)
	}
	return result
}

// Add The auto_increment id value/the number of affected rows, error
func Add(adds ...interface{}) (int64, error) {
	switch len(adds) {
	case 0:
		return 0, nil
	case 1:
		return addRow(adds[0])
	default:
		return addRows(adds...)
	}
}

// addRow The auto_increment id value and an error
func addRow(raw interface{}) (int64, error) {
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
	column := ""
	values := ""
	args := []interface{}{}
	length := t.NumField()
	for i := 0; i < length; i++ {
		if column == "" {
			column = fmt.Sprintf("%s", fn(PascalToUnderline(t.Field(i).Name)))
			values = "?"
		} else {
			column = fmt.Sprintf("%s, %s", column, fn(PascalToUnderline(t.Field(i).Name)))
			values = fmt.Sprintf("%s, %s", values, "?")
		}
		args = append(args, v.Field(i).Interface())
	}
	sql := fmt.Sprintf("INSERT INTO %s ( %s ) VALUES ( %s )", fn(PascalToUnderline(t.Name())), column, values)
	if LogSql {
		fmt.Println(DatetimeUnixNano(), sql, args)
	}
	result, err := DB.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// addRows The number of affected rows and an error
func addRows(raws ...interface{}) (int64, error) {
	length := len(raws)
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
			if adds[i].Column == "" {
				adds[i].Column = fmt.Sprintf("%s", fn(PascalToUnderline(t.Field(j).Name)))
				adds[i].Values = fmt.Sprintf("%s", "?")
			} else {
				adds[i].Column = fmt.Sprintf("%s, %s", adds[i].Column, fn(PascalToUnderline(t.Field(j).Name)))
				adds[i].Values = fmt.Sprintf("%s, %s", adds[i].Values, "?")
			}
			adds[i].Args = append(adds[i].Args, v.Field(j).Interface())
		}
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
				Sql:  fmt.Sprintf("INSERT INTO %s ( %s ) VALUES ( %s )", fn(adds[i].Table), adds[i].Column, adds[i].Values),
				Args: adds[i].Args,
			}
			continue
		}
		addRows[table] = struct {
			Sql  string
			Args []interface{}
		}{
			Sql:  fmt.Sprintf("%s, ( %s )", addRows[table].Sql, adds[i].Values),
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

// Del Delete
func Del(table string, where string, args ...interface{}) (int64, error) {
	table = fn(table)
	return Exec(fmt.Sprintf("DELETE FROM %s WHERE ( %s )", table, fws(where)), args...)
}

// Mod Update
func Mod(table string, cols []string, where string, args ...interface{}) (int64, error) {
	table = fn(table)
	columns := ""
	for _, v := range cols {
		v = fn(v)
		if columns == "" {
			columns = fmt.Sprintf("%s = ?", v)
			continue
		}
		columns = fmt.Sprintf("%s, %s = ?", columns, v)
	}
	return Exec(fmt.Sprintf("UPDATE %s SET %s WHERE ( %s )", table, columns, fws(where)), args...)
}

// result must be &[]AnyStruct, &[]*AnyStruct,&AnyStruct
// database table column value cannot be null, database allow filed is null value, and rows.Scan(...) will panic
// when the column value is null, take string type as an example:
// Tip: Although there are ways to deal with the issue of allowing null values, and it is not recommended to use allow null
// 1:structure field type string => *string
// 2:structure field type string => sql.NullString
// 3:sql  SELECT IFNULL(`age`,0) AS `age`,IFNULL(`name`,'Bob') AS `name`,IFNULL(`email`,'') AS `email` FROM ...
// 4:sql  SELECT COALESCE(`age`,0) AS `age`,COALESCE(`name`,'Bob') AS `name`,COALESCE(`email`,'') AS `email` FROM ...

// Get Exec query sql, return error
func Get(result interface{}, query string, args ...interface{}) error {
	if LogSql {
		fmt.Println(DatetimeUnixNano(), query, args)
	}
	rows, err := DB.Query(query, args...)
	if err != nil {
		return err
	}
	err = errors.New("is not *[]AnyStruct, *[]*AnyStruct or *AnyStruct type")
	ct, cv := reflect.TypeOf(result), reflect.ValueOf(result)
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
			reflect.ValueOf(result).Elem().Set(children)
			return nil
		}
		// *[]*AnyStruct type
		if ct.Elem().Elem().Kind() == reflect.Ptr && ct.Elem().Elem().Elem().Kind() == reflect.Struct {
			for rows.Next() {
				childValue := reflect.New(ct.Elem().Elem().Elem())
				childVal := reflect.Indirect(childValue)
				fields := []interface{}{}
				for _, column := range columns {
					// force all field names to lowercase to prevent rows.Scan panic; when you query system databases
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
			reflect.ValueOf(result).Elem().Set(children)
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
		reflect.ValueOf(result).Elem().Set(childValue.Elem())
		return nil
	// unknown type
	default:
		return err
	}
}

// InformationSchemaAllDatabases All databases
func InformationSchemaAllDatabases() ([]string, error) {
	dbs := []string{}
	rows, err := DB.Query("SHOW DATABASES")
	if err != nil {
		return dbs, err
	}
	for rows.Next() {
		name := ""
		err = rows.Scan(&name)
		if err != nil {
			return dbs, err
		}
		dbs = append(dbs, name)
	}
	return dbs, nil
}

// InformationSchemaAllDatabases All tables
func InformationSchemaAllTables(database string) ([]InformationSchemaTables, error) {
	tables := []InformationSchemaTables{}
	query := "SELECT * FROM `information_schema`.`TABLES` WHERE(`TABLE_SCHEMA`=? AND `TABLE_TYPE`='BASE TABLE')"
	err := Get(&tables, query, database)
	return tables, err
}

// InformationSchemaAllColumns All columns
func InformationSchemaAllColumns(database, table string) ([]InformationSchemaColumns, error) {
	columns := []InformationSchemaColumns{}
	query := "SELECT * FROM `information_schema`.`COLUMNS` WHERE(`TABLE_SCHEMA`=? AND `TABLE_NAME`=?)"
	err := Get(&columns, query, database, table)
	return columns, err
}
