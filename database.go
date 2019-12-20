package sea

import (
	"database/sql"
	"errors"
	"reflect"
)

// db database connect instance
var db *sql.DB

// SetDbInstance
func SetDbInstance(instance *sql.DB) bool {
	if instance == nil {
		return false
	}
	err := instance.Ping()
	if err != nil {
		return false
	}
	db = instance
	return true
}

// export must be &[]AnyStruct, &[]*AnyStruct,&AnyStruct
// database table column value cannot be null, database allow filed is null value, and rows.Scan(...) will panic
// when the column value is null, take string type as an example:
// Tip: Although there are ways to deal with the issue of allowing null values, it is not recommended to use allow null
// 1:structure field type string => *string
// 2:structure field type string => sql.NullString
// 3:sql  SELECT IFNULL(`age`,0) AS `age`,IFNULL(`name`,'Bob') AS `name`,IFNULL(`email`,'') AS `email` FROM ...
// 4:sql  SELECT COALESCE(`age`,0) AS `age`,COALESCE(`name`,'Bob') AS `name`,COALESCE(`email`,'') AS `email` FROM ...
// SqlRowsExport export *sql.rows data to interface
func SqlRowsExport(rows *sql.Rows, export interface{}) error {
	err := errors.New("is not *[]AnyStruct, *[]*AnyStruct or *AnyStruct type")
	xt, xv := reflect.TypeOf(export), reflect.ValueOf(export)
	if xt.Kind() != reflect.Ptr {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	xtElemKind := xt.Elem().Kind()
	// *[]interface{} type
	if xtElemKind == reflect.Slice {
		children := xv.Elem()
		reflectZeroValue := reflect.Value{}
		// *[]AnyStruct type
		if xt.Elem().Elem().Kind() == reflect.Struct {
			for rows.Next() {
				childValue := reflect.New(xt.Elem().Elem())
				childVal := reflect.Indirect(childValue)
				fields := []interface{}{}
				for _, column := range columns {
					field := childVal.FieldByName(UnderlineToPascal(column))
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
			reflect.ValueOf(export).Elem().Set(children)
			return nil
		}
		// *[]*AnyStruct type
		if xt.Elem().Elem().Kind() == reflect.Ptr && xt.Elem().Elem().Elem().Kind() == reflect.Struct {
			for rows.Next() {
				childValue := reflect.New(xt.Elem().Elem().Elem())
				childVal := reflect.Indirect(childValue)
				fields := []interface{}{}
				for _, column := range columns {
					field := childVal.FieldByName(UnderlineToPascal(column))
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
			reflect.ValueOf(export).Elem().Set(children)
			return nil
		}
		return err
	}
	// *AnyStruct type
	if xtElemKind == reflect.Struct {
		reflectZeroValue := reflect.Value{}
		childValue := reflect.New(xt.Elem())
		childVal := reflect.Indirect(childValue)
		fields := []interface{}{}
		for rows.Next() {
			for _, column := range columns {
				field := childVal.FieldByName(UnderlineToPascal(column))
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
			reflect.ValueOf(export).Elem().Set(childValue.Elem())
			return nil
		}

	}
	// unknown type
	return err
}

// Execute execute sql statement return affected rows
func Execute(db *sql.DB, query string, args ...interface{}) (int64, error) {
	result, err := db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

// ExecuteInsertOne execute insert sql statement return insert record id
func ExecuteInsertOne(db *sql.DB, query string, args ...interface{}) (int64, error) {
	result, err := db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}
