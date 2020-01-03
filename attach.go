package sea

import "reflect"

// GetTableName get table name
func GetTableName(table interface{}) string {
	t := reflect.TypeOf(table)
	kind := t.Kind()
	switch kind {
	case reflect.String:
		return PascalToUnderline(table.(string))
	case reflect.Struct:
		return PascalToUnderline(t.Name())
	case reflect.Ptr:
		kind := t.Elem().Kind()
		if kind == reflect.Struct {
			return PascalToUnderline(t.Elem().Name())
		}
		if kind == reflect.String {
			return PascalToUnderline(table.(string))
		}
	default:

	}
	return ""
}