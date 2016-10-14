package fixtures

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	onInsertNow         = "ON_INSERT_NOW()"
	onUpdateNow         = "ON_UPDATE_NOW()"
	onPKGeneratePrefix  = "PK_GENERATE("
	onPKGenerateSuffix  = ")"
	onPKReferencePrefix = "PK_REFERENCE("
	onPKReferenceSuffix = ")"

	postgresDriver = "postgres"
)

// Row represents a single database row
type Row struct {
	Table  string
	PK     map[string]interface{}
	Fields map[string]interface{}

	pkColumns     []string
	pkValues      []interface{}
	insertColumns []string
	updateColumns []string
	insertValues  []interface{}
	updateValues  []interface{}
}

type PrimaryKeyGenerator struct {
	name string
}

func (pk *PrimaryKeyGenerator) Set(values map[string]interface{}, key interface{}) {
	values[pk.name] = key
}

type PrimaryKeyReference struct {
	name string
}

func (pk *PrimaryKeyReference) Get(values map[string]interface{}) interface{} {
	return values[pk.name]
}

// Init loads internal struct variables
func (row *Row) Init() {
	// Init
	row.insertColumns = make([]string, 0, len(row.Fields))
	row.insertValues = make([]interface{}, 0, len(row.Fields))
	row.updateColumns = make([]string, 0, len(row.Fields))
	row.updateValues = make([]interface{}, 0, len(row.Fields))

	// Primary keys
	row.pkColumns = make([]string, 0, len(row.PK))
	row.pkValues = make([]interface{}, 0, len(row.PK))
	for pkKey := range row.PK {
		row.pkColumns = append(row.pkColumns, pkKey)
	}
	sort.Strings(row.pkColumns)

	for _, pkKey := range row.pkColumns {
		sv, ok := row.PK[pkKey].(string)
		if ok {
			if strings.HasPrefix(sv, onPKGeneratePrefix) &&
				strings.HasSuffix(sv, onPKGenerateSuffix) {
				keyName := strings.TrimPrefix(strings.TrimSuffix(sv, onPKGenerateSuffix), onPKGeneratePrefix)
				row.pkValues = append(row.pkValues, &PrimaryKeyGenerator{name: strings.TrimSpace(keyName)})
				continue
			} else if strings.HasPrefix(sv, onPKReferencePrefix) &&
				strings.HasSuffix(sv, onPKReferenceSuffix) {
				keyName := strings.TrimPrefix(strings.TrimSuffix(sv, onPKReferenceSuffix), onPKReferencePrefix)
				row.pkValues = append(row.pkValues, &PrimaryKeyReference{name: strings.TrimSpace(keyName)})
				continue
			}
		}
		row.pkValues = append(row.pkValues, row.PK[pkKey])
	}

	// Field Values
	fieldKeys := make([]string, 0, len(row.Fields))
	for fieldKey := range row.Fields {
		fieldKeys = append(fieldKeys, fieldKey)
	}
	sort.Strings(fieldKeys)
	for _, fieldKey := range fieldKeys {
		fieldValue := row.Fields[fieldKey]

		sv, ok := fieldValue.(string)
		if ok && sv == onInsertNow {
			row.insertColumns = append(row.insertColumns, fieldKey)
			row.insertValues = append(row.insertValues, time.Now())
			continue
		}
		if ok && sv == onUpdateNow {
			row.updateColumns = append(row.updateColumns, fieldKey)
			row.updateValues = append(row.updateValues, time.Now())

			if SetUpdatedAtOnInsert {
				row.insertColumns = append(row.insertColumns, fieldKey)
				row.insertValues = append(row.insertValues, time.Now())
			}
			continue
		}

		if ok && strings.HasPrefix(sv, onPKReferencePrefix) &&
			strings.HasSuffix(sv, onPKReferenceSuffix) {
			keyName := strings.TrimPrefix(strings.TrimSuffix(sv, onPKReferenceSuffix), onPKReferencePrefix)
			fieldValue = &PrimaryKeyReference{name: strings.TrimSpace(keyName)}
		}

		row.insertColumns = append(row.insertColumns, fieldKey)
		row.insertValues = append(row.insertValues, fieldValue)

		row.updateColumns = append(row.updateColumns, fieldKey)
		row.updateValues = append(row.updateValues, fieldValue)
	}
}

// GetInsertColumns returns a slice of column names for INSERT query
func (row *Row) GetInsertColumns() []string {
	escapedColumns := make([]string, len(row.insertColumns))
	for i, insertColumn := range row.insertColumns {
		escapedColumns[i] = fmt.Sprintf("\"%s\"", insertColumn)
	}
	return escapedColumns
}

// GetInsertValues returns a slice of values for INSERT query
func (row *Row) GetInsertValues(primaryKeys map[string]interface{}) []interface{} {
	insertValues := make([]interface{}, len(row.insertValues))
	for idx, value := range row.insertValues {
		if generator, ok := value.(*PrimaryKeyReference); ok {
			insertValues[idx] = generator.Get(primaryKeys)
		} else {
			insertValues[idx] = value
		}
	}
	return insertValues
}

// GetInsertPlaceholders returns a slice of placeholders for INSERT query
func (row *Row) GetInsertPlaceholders(driver string) []string {
	placeholders := make([]string, len(row.insertValues))
	for i := 0; i < len(placeholders); i++ {
		if driver == postgresDriver {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		} else {
			placeholders[i] = "?"
		}
	}
	return placeholders
}

// GetPKAndInsertColumns returns a slice of column names for INSERT query
func (row *Row) GetPKAndInsertColumns() []string {
	escapedColumns := make([]string, 0, len(row.pkColumns)+len(row.insertColumns))

	for _, insertColumn := range row.pkColumns {
		escapedColumns = append(escapedColumns, fmt.Sprintf("\"%s\"", insertColumn))
	}
	for _, insertColumn := range row.insertColumns {
		escapedColumns = append(escapedColumns, fmt.Sprintf("\"%s\"", insertColumn))
	}
	return escapedColumns
}

// GetPKAndInsertValues returns a slice of values for INSERT query
func (row *Row) GetPKAndInsertValues(primaryKeys map[string]interface{}) []interface{} {
	insertValues := make([]interface{}, 0, len(row.pkValues)+len(row.insertValues))
	for _, values := range [][]interface{}{row.pkValues, row.insertValues} {
		for _, value := range values {
			if generator, ok := value.(*PrimaryKeyReference); ok {
				insertValues = append(insertValues, generator.Get(primaryKeys))
			} else {
				insertValues = append(insertValues, value)
			}
		}
	}
	return insertValues
}

// GetPKAndInsertPlaceholders returns a slice of placeholders for INSERT query
func (row *Row) GetPKAndInsertPlaceholders(driver string) []string {
	placeholders := make([]string, len(row.pkValues)+len(row.insertValues))
	for i := 0; i < len(placeholders); i++ {
		if driver == postgresDriver {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		} else {
			placeholders[i] = "?"
		}
	}
	return placeholders
}

// GetUpdateColumns returns a slice of column names for UPDATE query
func (row *Row) GetUpdateColumns() []string {
	escapedColumns := make([]string, len(row.updateColumns))
	for i, updateColumn := range row.updateColumns {
		escapedColumns[i] = fmt.Sprintf("\"%s\"", updateColumn)
	}
	return escapedColumns
}

// GetUpdateColumnsLength returns number of columns for UDPATE query
func (row *Row) GetUpdateColumnsLength() int {
	return len(row.GetUpdateColumns())
}

// GetUpdateValues returns a slice of values for UPDATE query
func (row *Row) GetUpdateValues(primaryKeys map[string]interface{}) []interface{} {
	updateValues := make([]interface{}, len(row.updateValues))
	for idx, value := range row.updateValues {
		if generator, ok := value.(*PrimaryKeyReference); ok {
			updateValues[idx] = generator.Get(primaryKeys)
		} else {
			updateValues[idx] = value
		}
	}
	return updateValues
}

// GetUpdatePlaceholders returns a slice of placeholders for UPDATE query
func (row *Row) GetUpdatePlaceholders(driver string) []string {
	placeholders := make([]string, row.GetUpdateColumnsLength())
	for i, c := range row.GetUpdateColumns() {
		if driver == postgresDriver {
			placeholders[i] = fmt.Sprintf("%s = $%d", c, i+1)
		} else {
			placeholders[i] = fmt.Sprintf("%s = ?", c)
		}
	}
	return placeholders
}

// GetWhere returns a where condition based on primary key with placeholders
func (row *Row) GetWhere(driver string, i int) string {
	wheres := make([]string, len(row.PK))
	j := i
	for _, c := range row.pkColumns {
		if driver == postgresDriver {
			wheres[i-j] = fmt.Sprintf("%s = $%d", c, i+1)
		} else {
			wheres[i-j] = fmt.Sprintf("%s = ?", c)
		}
		i++
	}
	return strings.Join(wheres, " AND ")
}

// GetPKValues returns a slice of primary key values
func (row *Row) GetPKValues(primaryKeys map[string]interface{}) []interface{} {
	pkValues := make([]interface{}, len(row.pkValues))
	for idx, value := range row.pkValues {
		if generator, ok := value.(*PrimaryKeyReference); ok {
			pkValues[idx] = generator.Get(primaryKeys)
		} else {
			pkValues[idx] = value
		}
	}
	return pkValues
}

// GetPKColumns returns a slice of primary key names
func (row *Row) GetPKColumns() []string {
	return row.pkColumns
}
