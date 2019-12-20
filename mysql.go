package sea

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
