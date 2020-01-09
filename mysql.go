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
	// InformationSchemaSystemAllDatabases system database
	InformationSchemaSystemAllDatabases []string = []string{"information_schema", "mysql", "performance_schema"}
)

// Flutter `flutter`
func Flutter(s string) string {
	if !FlutterSql {
		return s
	}
	// user; u.name; `u`.`name`; u.`name`; name,email; u.name,u.email; ""; ,; ?; >100; >-100; <>100; =20.99; name='jack'; id=0
	// remove all spaces before and after the string
	s = strings.TrimSpace(s)
	// replace spaces in the string with ""
	s = strings.ReplaceAll(s, " ", "")
	switch s {
	case "", ",", "?", "0", "''", "\"\"", "<>", "!=", ">=", "<=", ">", "<", "=", "(", ")", "LEFT", "RIGHT", "OUT", "JOIN", "AND", "ON", "NOT", "BETWEEN", "OR", "IN", "LIKE", "AS", "ASC", "DESC":
		return s
	case "left", "right", "out", "join", "and", "on", "not", "between", "or", "in", "like", "as", "asc", "desc":
		return strings.ToUpper(s)
	default:
	}
	// number value
	if StrIsNumber(s) {
		return s
	}
	// string value
	if strings.Index(s, `'`) == 0 || strings.Index(s, `"`) == 0 {
		return s
	}
	// prefix and suffix
	if strings.HasPrefix(s, "(") {
		return fmt.Sprintf("( %s", Flutter(strings.TrimPrefix(s, "(")))
	}
	if strings.HasSuffix(s, ")") {
		return fmt.Sprintf("%s )", Flutter(strings.TrimSuffix(s, ")")))
	}
	// columns
	comma := ","
	if strings.Index(s, comma) >= 0 {
		str := ""
		nodes := strings.Split(s, comma)
		for _, v := range nodes {
			v = Flutter(v)
			if str == "" {
				str = v
				continue
			}
			str = fmt.Sprintf("%s, %s", str, v)
		}
		return str
	}
	// `name`; `user`.`id`; u.`group`
	flutter := "`"
	if strings.Index(s, flutter) >= 0 {
		s = strings.ReplaceAll(s, flutter, "")
		return Flutter(s)
	}
	// ,"(",")"
	symbol := []string{"<>", "!=", ">=", "<=", ">", "<", "="} // these symbols exist in the string(vn)
	for _, v := range symbol {
		index := strings.Index(s, v)
		if index < 0 {
			continue
		}
		result := ""
		lv := len(v)
		// prefix
		if strings.HasPrefix(s, v) {
			s = v + " " + s[index+lv:]
		} else if strings.HasSuffix(s, v) { // suffix
			s = s[:index] + " " + v
		} else { // middle
			s = s[:index] + " " + v + " " + s[index+lv:]
		}
		node := strings.Fields(s)
		for _, v := range node {
			v = Flutter(v)
			if result == "" {
				result = v
				continue
			}
			result = fmt.Sprintf("%s %s", result, v)
		}
		return result
	}

	// user; id; email; user.id; u.name
	point := "."
	fpf := fmt.Sprintf("%s%s%s", flutter, point, flutter)
	return fmt.Sprintf("%s%s%s", flutter, strings.ReplaceAll(s, point, fpf), flutter)
}

// Flutters
func Flutters(s string) string {
	if !FlutterSql {
		return s
	}
	result := ""
	node := strings.Fields(s)
	for _, v := range node {
		v = Flutter(v)
		if result == "" {
			result = v
			continue
		}
		result = fmt.Sprintf("%s %s", result, v)
	}
	return result
}

// Add auto_increment id value/the number of affected rows, error
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

// addRow the auto_increment id value and an error
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
			column = fmt.Sprintf("%s", Flutter(PascalToUnderline(t.Field(i).Name)))
			values = "?"
		} else {
			column = fmt.Sprintf("%s, %s", column, Flutter(PascalToUnderline(t.Field(i).Name)))
			values = fmt.Sprintf("%s, %s", values, "?")
		}
		args = append(args, v.Field(i).Interface())
	}
	sql := fmt.Sprintf("INSERT INTO %s ( %s ) VALUES ( %s )", Flutter(PascalToUnderline(t.Name())), column, values)
	if LogSql {
		fmt.Println(DatetimeUnixNano(), sql, args)
	}
	result, err := DB.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// addRows the number of affected rows and an error
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
				adds[i].Column = fmt.Sprintf("%s", Flutter(PascalToUnderline(t.Field(j).Name)))
				adds[i].Values = fmt.Sprintf("%s", "?")
			} else {
				adds[i].Column = fmt.Sprintf("%s, %s", adds[i].Column, Flutter(PascalToUnderline(t.Field(j).Name)))
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
				Sql:  fmt.Sprintf("INSERT INTO %s ( %s ) VALUES ( %s )", Flutter(adds[i].Table), adds[i].Column, adds[i].Values),
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
	table = Flutter(table)
	return Exec(fmt.Sprintf("DELETE FROM %s WHERE ( %s )", table, where), args...)
}

// Mod Update
func Mod(table string, cols []string, where string, args ...interface{}) (int64, error) {
	table = Flutter(table)
	columns := ""
	for _, v := range cols {
		v = Flutter(v)
		if columns == "" {
			columns = fmt.Sprintf("%s = ?", v)
			continue
		}
		columns = fmt.Sprintf("%s, %s = ?", columns, v)
	}
	return Exec(fmt.Sprintf("UPDATE %s SET %s WHERE ( %s )", table, columns, where), args...)
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

// InformationSchemaAllDatabases all databases
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

// InformationSchemaAllDatabases all tables
func InformationSchemaAllTables(database string) ([]InformationSchemaTables, error) {
	tables := []InformationSchemaTables{}
	query := "SELECT * FROM `information_schema`.`TABLES` WHERE(`TABLE_SCHEMA`=? AND `TABLE_TYPE`='BASE TABLE')"
	err := Get(&tables, query, database)
	return tables, err
}

// InformationSchemaAllColumns all columns
func InformationSchemaAllColumns(database, table string) ([]InformationSchemaColumns, error) {
	columns := []InformationSchemaColumns{}
	query := "SELECT * FROM `information_schema`.`COLUMNS` WHERE(`TABLE_SCHEMA`=? AND `TABLE_NAME`=?)"
	err := Get(&columns, query, database, table)
	return columns, err
}
