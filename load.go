package fixtures

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

var DefaultDumpSQL = true
var DefaultSetUpdatedAtOnInsert = false

// NewProcessingError ...
func NewProcessingError(row int, cause error) error {
	return fmt.Errorf("Error loading row %d: %s", row, cause.Error())
}

// NewFileError ...
func NewFileError(filename string, cause error) error {
	return fmt.Errorf("Error loading file %s: %s", filename, cause.Error())
}

type Context struct {
	primaryKeys map[string]interface{}
	Db          interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
		QueryRow(query string, args ...interface{}) *sql.Row
	}
	Driver string

	DumpSQL              bool
	SetUpdatedAtOnInsert bool
}

func LoadWithContext(ctx *Context, data []byte) error {
	// Unmarshal the YAML data into a []Row slice
	var rows []Row
	if err := yaml.Unmarshal(data, &rows); err != nil {
		return err
	}

	// Iterate over rows define in the fixture
	for i, row := range rows {
		// Load internat struct variables
		row.Init(ctx)

		if pkValues := row.GetPKValues(ctx.primaryKeys); len(pkValues) == 1 {
			if generator, ok := pkValues[0].(*PrimaryKeyGenerator); ok {
				insertQuery := fmt.Sprintf(
					`INSERT INTO "%s"(%s) VALUES(%s)`,
					row.Table,
					strings.Join(row.GetInsertColumns(), ", "),
					strings.Join(row.GetInsertPlaceholders(ctx.Driver), ", "),
				)
				if "postgres" == ctx.Driver {
					insertQuery = insertQuery + " RETURNING " + row.GetPKColumns()[0]
					if ctx.DumpSQL {
						log.Println("SQL:", insertQuery, row.GetInsertValues(ctx.primaryKeys))
					}

					var pk int64
					err := ctx.Db.QueryRow(insertQuery, row.GetInsertValues(ctx.primaryKeys)...).Scan(&pk)
					if err != nil {
						return NewProcessingError(i+1, err)
					}
					generator.Set(ctx.primaryKeys, pk)
				} else {
					if ctx.DumpSQL {
						log.Println("SQL:", insertQuery, row.GetInsertValues(ctx.primaryKeys))
					}

					res, err := ctx.Db.Exec(insertQuery, row.GetInsertValues(ctx.primaryKeys)...)
					if err != nil {
						return NewProcessingError(i+1, err)
					}
					pk, err := res.LastInsertId()
					if err != nil {
						return NewProcessingError(i+1, err)
					}
					generator.Set(ctx.primaryKeys, pk)
				}

				continue
			}
		}

		// Run a SELECT query to find out if we need to insert or UPDATE
		selectQuery := fmt.Sprintf(
			`SELECT COUNT(*) FROM "%s" WHERE %s`,
			row.Table,
			row.GetWhere(ctx.Driver, 0),
		)

		if ctx.DumpSQL {
			log.Println("SQL:", selectQuery, row.GetPKValues(ctx.primaryKeys))
		}

		var count int

		if err := ctx.Db.QueryRow(selectQuery, row.GetPKValues(ctx.primaryKeys)...).Scan(&count); err != nil {
			return NewProcessingError(i+1, err)
		}

		if count == 0 {
			// Primary key not found, let's run an INSERT query
			insertQuery := fmt.Sprintf(
				`INSERT INTO "%s"(%s) VALUES(%s)`,
				row.Table,
				strings.Join(row.GetPKAndInsertColumns(), ", "),
				strings.Join(row.GetPKAndInsertPlaceholders(ctx.Driver), ", "),
			)
			if ctx.DumpSQL {
				log.Println("SQL:", insertQuery, row.GetPKAndInsertValues(ctx.primaryKeys))
			}
			_, err := ctx.Db.Exec(insertQuery, row.GetPKAndInsertValues(ctx.primaryKeys)...)
			if err != nil {
				return NewProcessingError(i+1, err)
			}
			if ctx.Driver == postgresDriver && row.GetPKAndInsertColumns()[0] == "\"id\"" {
				err = fixPostgresPKSequence(ctx, row.Table, "id")
				if err != nil {
					return NewProcessingError(i+1, err)
				}
			}
		} else if row.GetUpdateColumnsLength() > 0 {
			// Primary key found, let's run UPDATE query
			updateQuery := fmt.Sprintf(
				`UPDATE "%s" SET %s WHERE %s`,
				row.Table,
				strings.Join(row.GetUpdatePlaceholders(ctx.Driver), ", "),
				row.GetWhere(ctx.Driver, row.GetUpdateColumnsLength()),
			)
			values := append(row.GetUpdateValues(ctx.primaryKeys), row.GetPKValues(ctx.primaryKeys)...)
			if ctx.DumpSQL {
				log.Println("SQL:", updateQuery, values)
			}
			_, err := ctx.Db.Exec(updateQuery, values...)
			if err != nil {
				return NewProcessingError(i+1, err)
			}
			if ctx.Driver == postgresDriver && row.GetUpdateColumns()[0] == "\"id\"" {
				err = fixPostgresPKSequence(ctx, row.Table, "id")
				if err != nil {
					return NewProcessingError(i+1, err)
				}
			}
		}
	}
	return nil
}

// Load processes a YAML fixture and inserts/updates the database accordingly
func Load(data []byte, db *sql.DB, driver string) error {
	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // rollback the transaction

	err = LoadWithContext(&Context{
		primaryKeys: map[string]interface{}{},
		Db:          tx,
		Driver:      driver,

		DumpSQL:              DefaultDumpSQL,
		SetUpdatedAtOnInsert: DefaultSetUpdatedAtOnInsert,
	}, data)
	if err != nil {
		return err
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

	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // rollback the transaction

	err = LoadWithContext(&Context{
		primaryKeys: map[string]interface{}{},
		Db:          tx,
		Driver:      driver,

		DumpSQL:              DefaultDumpSQL,
		SetUpdatedAtOnInsert: DefaultSetUpdatedAtOnInsert,
	}, data)

	if err != nil {
		return err
	}
	// Commit the transaction
	return tx.Commit()
}

// LoadFiles ...
func LoadFiles(filenames []string, db *sql.DB, driver string) error {
	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // rollback the transaction

	ctx := &Context{
		primaryKeys: map[string]interface{}{},
		Db:          tx,
		Driver:      driver,

		DumpSQL:              DefaultDumpSQL,
		SetUpdatedAtOnInsert: DefaultSetUpdatedAtOnInsert,
	}

	for _, filename := range filenames {
		// Read fixture data from the file
		data, err := ioutil.ReadFile(filename)
		if err != nil {
			return NewFileError(filename, err)
		}
		err = LoadWithContext(ctx, data)
		if err != nil {
			return err
		}
	}
	// Commit the transaction
	return tx.Commit()
}

// fixPostgresPKSequence
func fixPostgresPKSequence(ctx *Context, table string, column string) error {
	// Query for the qualified sequence name
	var seqName *string
	err := ctx.Db.QueryRow(`
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
	_, err = ctx.Db.Exec(fmt.Sprintf(`
		SELECT pg_catalog.setval($1, (SELECT MAX("%s") FROM "%s"))
	`, column, table), *seqName)

	return err
}
