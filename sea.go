package sea

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	// DB Database connect instance
	DB *sql.DB
	// InformationSchemaSystemAllDatabases System database
	InformationSchemaSystemAllDatabases []string
)

func init() {
	InformationSchemaSystemAllDatabases = []string{"information_schema", "mysql", "performance_schema"}
}

// Exec Execute a sql statement return affected rows and an error
func Exec(query string, args ...interface{}) (int64, error) {
	result, err := DB.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

//////////////////////
// mysql or mariadb //
//////////////////////

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

// Add Insert one or more rows
func Add(insert ...interface{}) (int64, error) {
	if len(insert) == 1 {
		return add(insert[0])
	}
	return adds(insert...)
}

// add Insert one row
func add(row interface{}) (id int64, err error) {
	err = errors.New("please pass in a structure parameter")
	if row == nil {
		return id, err
	}
	t, v := reflect.TypeOf(row), reflect.ValueOf(row)
	if t.Kind() != reflect.Ptr {
		return id, err
	}
	t, v = t.Elem(), v.Elem()
	column, values, args := "", "", []interface{}{}
	for i := 0; i < t.NumField(); i++ {
		args = append(args, v.Field(i).Interface())
		if column == "" {
			column = fmt.Sprintf("`%s`", PascalToUnderline(t.Field(i).Name))
			values = "?"
			continue
		}
		column = fmt.Sprintf("%s, `%s`", column, PascalToUnderline(t.Field(i).Name))
		values = fmt.Sprintf("%s, %s", values, "?")
	}
	result, err := DB.Exec(fmt.Sprintf("INSERT INTO `%s` ( %s ) VALUES ( %s )", PascalToUnderline(t.Name()), column, values), args...)
	if err != nil {
		return id, err
	}
	return result.LastInsertId()
}

// adds Insert more rows
func adds(rows ...interface{}) (affectedRows int64, err error) {
	type insert struct {
		Table  string
		Column string
		Values string
		Args   []interface{}
	}
	type exec struct {
		Sql  string
		Args []interface{}
	}
	length := len(rows)
	err = errors.New("please pass in the structure parameters")
	inserts := make([]insert, length, length)
	for i := 0; i < length; i++ {
		if rows[i] == nil {
			return affectedRows, err
		}
		t, v := reflect.TypeOf(rows[i]), reflect.ValueOf(rows[i])
		if t.Kind() != reflect.Ptr {
			return affectedRows, err
		}
		t, v = t.Elem(), v.Elem()
		for j := 0; j < v.NumField(); j++ {
			inserts[i].Table = PascalToUnderline(t.Name())
			inserts[i].Args = append(inserts[i].Args, v.Field(j).Interface())
			if inserts[i].Column == "" {
				inserts[i].Column = fmt.Sprintf("`%s`", PascalToUnderline(t.Field(j).Name))
				inserts[i].Values = "?"
				continue
			}
			inserts[i].Column = fmt.Sprintf("%s, `%s`", inserts[i].Column, PascalToUnderline(t.Field(j).Name))
			inserts[i].Values = fmt.Sprintf("%s, %s", inserts[i].Values, "?")
		}
	}
	execs := map[string]exec{}
	for i := 0; i < length; i++ {
		table := inserts[i].Table
		if execs[table].Sql == "" {
			execs[table] = exec{
				Sql:  fmt.Sprintf("INSERT INTO `%s` ( %s ) VALUES ( %s )", inserts[i].Table, inserts[i].Column, inserts[i].Values),
				Args: inserts[i].Args,
			}
			continue
		}
		execs[table] = exec{
			Sql:  fmt.Sprintf("%s, ( %s )", execs[table].Sql, inserts[i].Values),
			Args: append(execs[table].Args, inserts[i].Args...),
		}
	}
	for _, val := range execs {
		ar, err := Exec(val.Sql, val.Args...)
		if err != nil {
			return affectedRows, err
		}
		affectedRows += ar
	}
	return affectedRows, nil
}

// Del Delete records from a table
func Del(table string, where string, args ...interface{}) (int64, error) {
	return Exec(fmt.Sprintf("DELETE FROM `%s` WHERE ( %s )", table, where), args...)
}

// Mod Update records from a table
func Mod(table string, cols map[string]interface{}, where string, args ...interface{}) (int64, error) {
	columns := ""
	values := []interface{}{}
	for col, val := range cols {
		values = append(values, val)
		if columns == "" {
			columns = fmt.Sprintf("`%s` = ?", col)
			continue
		}
		columns = fmt.Sprintf("%s, `%s` = ?", columns, col)
	}
	return Exec(fmt.Sprintf("UPDATE `%s` SET %s WHERE ( %s )", table, columns, where), append(values, args...)...)
}

// Get result must be &[]AnyStruct, &[]*AnyStruct,&AnyStruct
// when the column value is null, the solution is as follows:
// Tip: Although there are ways to deal with the issue of allowing null values, and it is not recommended to use allow null
// 1:structure field type string => *string
// 2:structure field type string => sql.NullString
// 3:sql  SELECT IFNULL(`age`,0) AS `age`,IFNULL(`name`,'Bob') AS `name`,IFNULL(`email`,'') AS `email` FROM ...
// 4:sql  SELECT COALESCE(`age`,0) AS `age`,COALESCE(`name`,'Bob') AS `name`,COALESCE(`email`,'') AS `email` FROM ...

// Get Exec query sql, return an error
func Get(result interface{}, query string, args ...interface{}) error {
	err := errors.New("only supports *[]AnyStruct, *[]*AnyStruct, *AnyStruct types")
	t, v := reflect.TypeOf(result), reflect.ValueOf(result)
	if t.Kind() != reflect.Ptr {
		return err
	}
	switch t.Elem().Kind() {
	case reflect.Slice, reflect.Struct:
		rows, err := DB.Query(query, args...)
		if err != nil {
			return err
		}
		columns, err := rows.Columns()
		if err != nil {
			return err
		}
		// *AnyStruct
		if t.Elem().Kind() == reflect.Struct {
			reflectZeroValue := reflect.Value{}
			data := reflect.New(t.Elem())
			dataVal := reflect.Indirect(data)
			fields := []interface{}{}
			rows.Next()
			for _, column := range columns {
				// force all field names to lowercase to prevent rows.Scan panic
				field := dataVal.FieldByName(UnderlineToPascal(strings.ToLower(column)))
				if field == reflectZeroValue || !field.CanSet() {
					bytesTypePtrValue := reflect.New(reflect.TypeOf([]byte{}))
					bytesTypePtrValue.Elem().Set(reflect.ValueOf([]byte{}))
					fields = append(fields, bytesTypePtrValue.Interface())
					continue
				}
				fields = append(fields, field.Addr().Interface())
			}
			err = rows.Scan(fields...)
			if err != nil {
				return err
			}
			reflect.ValueOf(result).Elem().Set(data.Elem())
			return nil
		}
		// slice => *[]AnyStruct or *[]*AnyStruct
		children := v.Elem()
		reflectZeroValue := reflect.Value{}
		// *[]AnyStruct
		if t.Elem().Elem().Kind() == reflect.Struct {
			for rows.Next() {
				data := reflect.New(t.Elem().Elem())
				dataVal := reflect.Indirect(data)
				fields := []interface{}{}
				for _, column := range columns {
					// force all field names to lowercase to prevent rows.Scan panic
					field := dataVal.FieldByName(UnderlineToPascal(strings.ToLower(column)))
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
				children = reflect.Append(children, data.Elem())
			}
			reflect.ValueOf(result).Elem().Set(children)
			return nil
		}
		// *[]*AnyStruct
		if t.Elem().Elem().Kind() == reflect.Ptr && t.Elem().Elem().Elem().Kind() == reflect.Struct {
			for rows.Next() {
				data := reflect.New(t.Elem().Elem().Elem())
				dataVal := reflect.Indirect(data)
				fields := []interface{}{}
				for _, column := range columns {
					field := dataVal.FieldByName(UnderlineToPascal(strings.ToLower(column)))
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
				children = reflect.Append(children, data)
			}
			reflect.ValueOf(result).Elem().Set(children)
			return nil
		}
		return err
	// unknown type
	default:
		return err
	}
}

// InformationSchemaAllDatabases All databases
func InformationSchemaAllDatabases() (database []string, err error) {
	rows, err := DB.Query("SHOW DATABASES")
	if err != nil {
		return
	}
	name := ""
	for rows.Next() {
		err = rows.Scan(&name)
		if err != nil {
			return
		}
		database = append(database, name)
	}
	return
}

// InformationSchemaAllDatabases All tables
func InformationSchemaAllTables(database string) (tables []InformationSchemaTables, err error) {
	return tables, Get(&tables, "SELECT * FROM `information_schema`.`TABLES` WHERE ( `TABLE_SCHEMA` = ? AND `TABLE_TYPE` = 'BASE TABLE' )", database)
}

// InformationSchemaAllColumns All columns
func InformationSchemaAllColumns(database, table string) (columns []InformationSchemaColumns, err error) {
	return columns, Get(&columns, "SELECT * FROM `information_schema`.`COLUMNS` WHERE ( `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ? )", database, table)
}

// Transaction Transaction
type Transaction struct {
	Tx *sql.Tx
}

// Begin Begin
func Begin() (*Transaction, error) {
	ts := &Transaction{}
	tx, err := DB.Begin()
	if err != nil {
		return ts, err
	}
	ts.Tx = tx
	return ts, nil
}

// Rollback Rollback
func (ts *Transaction) Rollback() error {
	return ts.Tx.Rollback()
}

// Commit Commit
func (ts *Transaction) Commit() error {
	return ts.Tx.Commit()
}

// Exec Execute a sql statement return affected rows and an error
func (ts *Transaction) Exec(query string, args ...interface{}) (int64, error) {
	result, err := ts.Tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Add Add
func (ts *Transaction) Add(insert ...interface{}) (int64, error) {
	if len(insert) == 1 {
		return ts.add(insert[0])
	}
	return ts.adds(insert...)
}

// add Insert one row
func (ts *Transaction) add(row interface{}) (id int64, err error) {
	err = errors.New("please pass in a structure parameter")
	if row == nil {
		return id, err
	}
	t, v := reflect.TypeOf(row), reflect.ValueOf(row)
	if t.Kind() != reflect.Ptr {
		return id, err
	}
	t, v = t.Elem(), v.Elem()
	column, values, args := "", "", []interface{}{}
	for i := 0; i < t.NumField(); i++ {
		args = append(args, v.Field(i).Interface())
		if column == "" {
			column = fmt.Sprintf("`%s`", PascalToUnderline(t.Field(i).Name))
			values = "?"
			continue
		}
		column = fmt.Sprintf("%s, `%s`", column, PascalToUnderline(t.Field(i).Name))
		values = fmt.Sprintf("%s, %s", values, "?")
	}
	result, err := ts.Tx.Exec(fmt.Sprintf("INSERT INTO `%s` ( %s ) VALUES ( %s )", PascalToUnderline(t.Name()), column, values), args...)
	if err != nil {
		return id, err
	}
	return result.LastInsertId()
}

// adds Insert more rows
func (ts *Transaction) adds(rows ...interface{}) (affectedRows int64, err error) {
	type insert struct {
		Table  string
		Column string
		Values string
		Args   []interface{}
	}
	type exec struct {
		Sql  string
		Args []interface{}
	}
	length := len(rows)
	err = errors.New("please pass in the structure parameters")
	inserts := make([]insert, length, length)
	for i := 0; i < length; i++ {
		if rows[i] == nil {
			return affectedRows, err
		}
		t, v := reflect.TypeOf(rows[i]), reflect.ValueOf(rows[i])
		if t.Kind() != reflect.Ptr {
			return affectedRows, err
		}
		t, v = t.Elem(), v.Elem()
		for j := 0; j < v.NumField(); j++ {
			inserts[i].Table = PascalToUnderline(t.Name())
			inserts[i].Args = append(inserts[i].Args, v.Field(j).Interface())
			if inserts[i].Column == "" {
				inserts[i].Column = fmt.Sprintf("`%s`", PascalToUnderline(t.Field(j).Name))
				inserts[i].Values = "?"
				continue
			}
			inserts[i].Column = fmt.Sprintf("%s, `%s`", inserts[i].Column, PascalToUnderline(t.Field(j).Name))
			inserts[i].Values = fmt.Sprintf("%s, %s", inserts[i].Values, "?")
		}
	}
	execs := map[string]exec{}
	for i := 0; i < length; i++ {
		table := inserts[i].Table
		if execs[table].Sql == "" {
			execs[table] = exec{
				Sql:  fmt.Sprintf("INSERT INTO `%s` ( %s ) VALUES ( %s )", inserts[i].Table, inserts[i].Column, inserts[i].Values),
				Args: inserts[i].Args,
			}
			continue
		}
		execs[table] = exec{
			Sql:  fmt.Sprintf("%s, ( %s )", execs[table].Sql, inserts[i].Values),
			Args: append(execs[table].Args, inserts[i].Args...),
		}
	}
	for _, val := range execs {
		ar, err := ts.Exec(val.Sql, val.Args...)
		if err != nil {
			return affectedRows, err
		}
		affectedRows += ar
	}
	return affectedRows, nil
}

// Del Delete records from a table
func (ts *Transaction) Del(table string, where string, args ...interface{}) (int64, error) {
	return ts.Exec(fmt.Sprintf("DELETE FROM `%s` WHERE ( %s )", table, where), args...)
}

// Mod Update records from a table
func (ts *Transaction) Mod(table string, cols map[string]interface{}, where string, args ...interface{}) (int64, error) {
	columns := ""
	values := []interface{}{}
	for col, val := range cols {
		values = append(values, val)
		if columns == "" {
			columns = fmt.Sprintf("`%s` = ?", col)
			continue
		}
		columns = fmt.Sprintf("%s, `%s` = ?", columns, col)
	}
	return ts.Exec(fmt.Sprintf("UPDATE `%s` SET %s WHERE ( %s )", table, columns, where), append(values, args...)...)
}

// Inquirer Inquirer
type Inquirer interface {
	Cols(...string) Inquirer
	Table(string) Inquirer
	Alias(string) Inquirer
	Join(...string) Inquirer
	Where(string, ...interface{}) Inquirer
	Group(...string) Inquirer
	Having(string, ...interface{}) Inquirer
	Asc(string) Inquirer
	Desc(string) Inquirer
	Page(uint64) Inquirer
	Limit(uint64) Inquirer
	Get(interface{}) error
}

// Inquiry Inquiry
type Inquiry struct {
	// column name
	cols []string
	// table name
	table string
	// alias name
	alias string
	// join name
	join string
	// where name
	where string
	// group name
	group string
	// having
	having string
	// order name
	order string
	// page page
	page uint64
	// limit limit
	limit uint64
	// sql
	sql string
	// args
	args []interface{}
}

// Query Query
func Query(table ...string) Inquirer {
	q := &Inquiry{}
	for _, v := range table {
		if q.table == "" {
			q.table = v
			continue
		}
		q.table = fmt.Sprintf("%s, %s", q.table, v)
	}
	return q
}

// Cols Columns
func (q *Inquiry) Cols(cols ...string) Inquirer {
	q.cols = append(q.cols, cols...)
	return q
}

// Table Table Name
func (q *Inquiry) Table(table string) Inquirer {
	q.table = table
	return q
}

// Alias Alias
func (q *Inquiry) Alias(alias string) Inquirer {
	q.alias = alias
	return q
}

// Join Join
func (q *Inquiry) Join(join ...string) Inquirer {
	for _, v := range join {
		if q.join == "" {
			q.join = v
			continue
		}
		q.join = fmt.Sprintf("%s %s", q.join, v)
	}
	return q
}

// Where Where
func (q *Inquiry) Where(where string, args ...interface{}) Inquirer {
	q.args = append(q.args, args...)
	if q.where == "" {
		q.where = where
		return q
	}
	q.where = fmt.Sprintf("%s AND %s", q.where, where)
	return q
}

// Group Group
func (q *Inquiry) Group(group ...string) Inquirer {
	for _, v := range group {
		if q.group == "" {
			q.group = v
			continue
		}
		q.group = fmt.Sprintf("%s, %s", q.group, v)
	}
	return q
}

// Having Having
func (q *Inquiry) Having(having string, args ...interface{}) Inquirer {
	q.args = append(q.args, args...)
	if q.having == "" {
		q.having = having
		return q
	}
	q.having = fmt.Sprintf("%s AND %s", q.having, having)
	return q
}

// Asc Order By ASC
func (q *Inquiry) Asc(order string) Inquirer {
	if q.order == "" {
		q.order = fmt.Sprintf("%s ASC", order)
		return q
	}
	q.order = fmt.Sprintf("%s, %s ASC", q.order, order)
	return q
}

// Desc Order By DESC
func (q *Inquiry) Desc(order string) Inquirer {
	if q.order == "" {
		q.order = fmt.Sprintf("%s DESC", order)
		return q
	}
	q.order = fmt.Sprintf("%s, %s DESC", q.order, order)
	return q
}

// Page Page
func (q *Inquiry) Page(page uint64) Inquirer {
	q.page = page
	return q
}

// Limit Limit
func (q *Inquiry) Limit(limit uint64) Inquirer {
	q.limit = limit
	return q
}

// Get Get query result
func (q *Inquiry) Get(get interface{}) error {
	// check columns first
	cols := ""
	if len(q.cols) == 0 {
		cols = "*"
	} else {
		for _, v := range q.cols {
			if cols == "" {
				cols = v
				continue
			}
			cols = fmt.Sprintf("%s, %s", cols, v)
		}
	}
	// table name is not set
	if q.table == "" {
		t := reflect.TypeOf(get)
		kind := t.Kind()
		if kind != reflect.Ptr {
			return errors.New("require pointer parameter")
		}
		t = t.Elem()
		kind = t.Kind()
		switch kind {
		case reflect.Struct:
			q.table = PascalToUnderline(t.Name())
		case reflect.Slice:
			t = t.Elem()
			kind = t.Kind()
			if kind == reflect.Ptr {
				t = t.Elem()
			}
			q.table = PascalToUnderline(t.Name())
		default:
			return errors.New("unsupported data type")
		}
	}
	q.sql = fmt.Sprintf("SELECT %s FROM %s", cols, q.table)
	if q.alias != "" {
		q.sql = fmt.Sprintf("%s %s", q.sql, q.alias)
	}
	if q.join != "" {
		q.sql = fmt.Sprintf("%s %s", q.sql, q.join)
	}
	if q.where != "" {
		q.sql = fmt.Sprintf("%s WHERE ( %s )", q.sql, q.where)
	}
	if q.group != "" {
		q.sql = fmt.Sprintf("%s GROUP BY %s", q.sql, q.group)
		if q.having != "" {
			q.sql = fmt.Sprintf("%s HAVING ( %s )", q.sql, q.having)
		}
	}
	if q.order != "" {
		q.sql = fmt.Sprintf("%s ORDER BY %s", q.sql, q.order)
	}
	// set limit x,y; if not set limit, use 1000 as default value
	if q.limit == 0 {
		q.limit = 1000
	}
	if q.page == 0 {
		q.sql = fmt.Sprintf("%s LIMIT %d", q.sql, q.limit)
	} else {
		q.sql = fmt.Sprintf("%s LIMIT %d,%d", q.sql, (q.page-1)*q.limit, q.limit)
	}
	return Get(get, q.sql, q.args...)
}

// PascalToUnderline XxxYyy to xxx_yyy
func PascalToUnderline(s string) string {
	tmp := []byte{}
	j := false
	num := len(s)
	for i := 0; i < num; i++ {
		d := s[i]
		if i > 0 && d >= 'A' && d <= 'Z' && j {
			tmp = append(tmp, '_')
		}
		if d != '_' {
			j = true
		}
		tmp = append(tmp, d)
	}
	return strings.ToLower(string(tmp[:]))
}

// UnderlineToPascal xxx_yyy to XxxYyy
func UnderlineToPascal(s string) string {
	tmp := []byte{}
	bytes := []byte(s)
	length := len(bytes)
	nextLetterNeedToUpper := true
	for i := 0; i < length; i++ {
		if bytes[i] == '_' {
			nextLetterNeedToUpper = true
			continue
		}
		if nextLetterNeedToUpper && bytes[i] >= 'a' && bytes[i] <= 'z' {
			tmp = append(tmp, bytes[i]-32)
		} else {
			tmp = append(tmp, bytes[i])
		}
		nextLetterNeedToUpper = false
	}
	return string(tmp[:])
}
