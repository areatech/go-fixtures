package fixtures

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

var DumpSQL = true
var SetUpdatedAtOnInsert = false

// NewProcessingError ...
func NewProcessingError(row int, cause error) error {
	return fmt.Errorf("Error loading row %d: %s", row, cause.Error())
}

// NewFileError ...
func NewFileError(filename string, cause error) error {
	return fmt.Errorf("Error loading file %s: %s", filename, cause.Error())
}

// Load processes a YAML fixture and inserts/updates the database accordingly
func Load(data []byte, db *sql.DB, driver string) error {
	// Unmarshal the YAML data into a []Row slice
	var rows []Row
	if err := yaml.Unmarshal(data, &rows); err != nil {
		return err
	}

	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // rollback the transaction

	primaryKeys := map[string]interface{}{}

	// Iterate over rows define in the fixture
	for i, row := range rows {
		// Load internat struct variables
		row.Init()

		if pkValues := row.GetPKValues(primaryKeys); len(pkValues) == 1 {
			if generator, ok := pkValues[0].(*PrimaryKeyGenerator); ok {
				insertQuery := fmt.Sprintf(
					`INSERT INTO "%s"(%s) VALUES(%s)`,
					row.Table,
					strings.Join(row.GetInsertColumns(), ", "),
					strings.Join(row.GetInsertPlaceholders(driver), ", "),
				)
				if "postgres" == driver {
					insertQuery = insertQuery + " RETURNING " + row.GetPKColumns()[0]
					if DumpSQL {
						log.Println("SQL:", insertQuery, row.GetInsertValues(primaryKeys))
					}

					var pk int64
					err := tx.QueryRow(insertQuery, row.GetInsertValues(primaryKeys)...).Scan(&pk)
					if err != nil {
						return NewProcessingError(i+1, err)
					}
					generator.Set(primaryKeys, pk)
				} else {
					if DumpSQL {
						log.Println("SQL:", insertQuery, row.GetInsertValues(primaryKeys))
					}

					res, err := tx.Exec(insertQuery, row.GetInsertValues(primaryKeys)...)
					if err != nil {
						return NewProcessingError(i+1, err)
					}
					pk, err := res.LastInsertId()
					if err != nil {
						return NewProcessingError(i+1, err)
					}
					generator.Set(primaryKeys, pk)
				}

				continue
			}
		}

		// Run a SELECT query to find out if we need to insert or UPDATE
		selectQuery := fmt.Sprintf(
			`SELECT COUNT(*) FROM "%s" WHERE %s`,
			row.Table,
			row.GetWhere(driver, 0),
		)

		if DumpSQL {
			log.Println("SQL:", selectQuery, row.GetPKValues(primaryKeys))
		}

		var count int
		err = tx.QueryRow(selectQuery, row.GetPKValues(primaryKeys)...).Scan(&count)
		if err != nil {
			return NewProcessingError(i+1, err)
		}

		if count == 0 {
			// Primary key not found, let's run an INSERT query
			insertQuery := fmt.Sprintf(
				`INSERT INTO "%s"(%s) VALUES(%s)`,
				row.Table,
				strings.Join(row.GetPKAndInsertColumns(), ", "),
				strings.Join(row.GetPKAndInsertPlaceholders(driver), ", "),
			)
			if DumpSQL {
				log.Println("SQL:", insertQuery, row.GetPKAndInsertValues(primaryKeys))
			}
			_, err := tx.Exec(insertQuery, row.GetPKAndInsertValues(primaryKeys)...)
			if err != nil {
				return NewProcessingError(i+1, err)
			}
			if driver == postgresDriver && row.GetPKAndInsertColumns()[0] == "\"id\"" {
				err = fixPostgresPKSequence(tx, row.Table, "id")
				if err != nil {
					return NewProcessingError(i+1, err)
				}
			}
		} else if row.GetUpdateColumnsLength() > 0 {
			// Primary key found, let's run UPDATE query
			updateQuery := fmt.Sprintf(
				`UPDATE "%s" SET %s WHERE %s`,
				row.Table,
				strings.Join(row.GetUpdatePlaceholders(driver), ", "),
				row.GetWhere(driver, row.GetUpdateColumnsLength()),
			)
			values := append(row.GetUpdateValues(primaryKeys), row.GetPKValues(primaryKeys)...)
			if DumpSQL {
				log.Println("SQL:", updateQuery, values)
			}
			_, err := tx.Exec(updateQuery, values...)
			if err != nil {
				return NewProcessingError(i+1, err)
			}
			if driver == postgresDriver && row.GetUpdateColumns()[0] == "\"id\"" {
				err = fixPostgresPKSequence(tx, row.Table, "id")
				if err != nil {
					return NewProcessingError(i+1, err)
				}
			}
		}
	}

	// Commit the transaction
	return tx.Commit()
}

// LoadFile ...
func LoadFile(filename string, db *sql.DB, driver string) error {
	// Read fixture data from the file
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return NewFileError(filename, err)
	}

	// Insert the fixture data
	return Load(data, db, driver)
}

// LoadFiles ...
func LoadFiles(filenames []string, db *sql.DB, driver string) error {
	for _, filename := range filenames {
		if err := LoadFile(filename, db, driver); err != nil {
			return err
		}
	}
	return nil
}

// fixPostgresPKSequence
func fixPostgresPKSequence(tx *sql.Tx, table string, column string) error {
	// Query for the qualified sequence name
	var seqName *string
	err := tx.QueryRow(`
		SELECT pg_get_serial_sequence($1, $2)
	`, table, column).Scan(&seqName)

	if err != nil {
		return err
	}

	if seqName == nil {
		// No sequence to fix
		return nil
	}

	// Set the sequence
	_, err = tx.Exec(fmt.Sprintf(`
		SELECT pg_catalog.setval($1, (SELECT MAX("%s") FROM "%s"))
	`, column, table), *seqName)

	return err
}
