package pgutil

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Column represents a SQL table column.
type Column struct {
	// Name is the name of the column.
	Name string

	// Null specifies whether the column accepts null values.
	Null bool

	// Unique is either an error or `nil`. Any non-nil value indicates that the
	// column is to be unique--if the unique constraint is violated for this
	// column, this error will be returned.
	Unique error

	// Type contains the name of the column's type, e.g., `VARCHAR(255)` or
	// `TIMESTAMPTZ`.
	Type string
}

func (c *Column) createSQL(sb *strings.Builder, pkey string) {
	// name
	sb.WriteByte('"')
	sb.WriteString(c.Name)
	sb.WriteByte('"')
	sb.WriteByte(' ')

	// type
	sb.WriteString(c.Type)

	// (not) null
	if !c.Null {
		sb.WriteString(" NOT NULL")
	}

	// unique
	if c.Unique != nil {
		sb.WriteString(" UNIQUE")
	}

	// primary key
	if pkey == c.Name {
		sb.WriteString(" PRIMARY KEY")
	}
}

// Table represents a SQL table.
type Table struct {
	// Name is the name of the table.
	Name string

	// Columns is the list of columns in the table. There must always be at
	// at least one column, and the first column is assumed to be the primary
	// key column.
	Columns []Column

	// ExistsErr is returned when there is a primary key conflict error.
	ExistsErr error

	// NotFoundErr is returned when there a primary key can't be found.
	NotFoundErr error
}

// List lists the records in the table.
func (t *Table) List(db *sql.DB) (*Result, error) {
	var sb strings.Builder
	sb.WriteByte('"')
	sb.WriteString(t.Columns[0].Name)
	sb.WriteByte('"')

	for i := range t.Columns[1:] {
		sb.WriteByte(',')
		sb.WriteByte(' ')
		sb.WriteByte('"')
		sb.WriteString(t.Columns[i+1].Name)
		sb.WriteByte('"')
	}

	rows, err := db.Query(fmt.Sprintf(
		"SELECT %s FROM \"%s\"",
		sb.String(),
		t.Name,
	))
	if err != nil {
		return nil, fmt.Errorf("listing rows from table `%s`: %w", t.Name, err)
	}

	return &Result{
		rows:     rows,
		pointers: make([]interface{}, len(t.Columns)),
	}, nil
}

// IDColumn returns the tables primary key column.
func (t *Table) IDColumn() *Column { return &t.Columns[idColumnPosition] }

const idColumnPosition = 0

// Get retrieves a single item by ID and scans it into the provided `out` item.
// If the item isn't found, the table's `NotFoundErr` field will be returned.
func (t *Table) Get(db *sql.DB, id interface{}, out Item) error {
	var columnNames strings.Builder
	columnNames.WriteByte('"')
	columnNames.WriteString(t.Columns[0].Name)
	columnNames.WriteByte('"')

	for _, column := range t.Columns[1:] {
		columnNames.WriteByte(',')
		columnNames.WriteByte(' ')
		columnNames.WriteByte('"')
		columnNames.WriteString(column.Name)
		columnNames.WriteByte('"')
	}

	pointers := make([]interface{}, len(t.Columns))
	out.Scan(pointers)

	if err := db.QueryRow(
		fmt.Sprintf(
			"SELECT %s FROM \"%s\" WHERE \"%s\" = $1",
			columnNames.String(),
			t.Name,
			t.IDColumn().Name,
		),
		id,
	).Scan(pointers...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t.NotFoundErr
		}
		return fmt.Errorf(
			"getting record from `%s` postgres table: %w",
			t.Name,
			err,
		)
	}

	return nil
}

// Exists returns `nil` if a record exists for the provided ID, otherwise it
// returns the Table's `NotFoundErr` field.
func (t *Table) Exists(db *sql.DB, id interface{}) error {
	var dummy string
	if err := db.QueryRow(
		fmt.Sprintf("SELECT true FROM \"%s\" WHERE \"%s\" = $1",
			t.Name,
			t.IDColumn().Name,
		),
		id,
	).Scan(&dummy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t.NotFoundErr
		}
		return fmt.Errorf("checking for row in table `%s`: %w", t.Name, err)
	}
	return nil
}

// Delete deletes the record with the provided ID, otherwise it returns the
// Table's `NotFoundErr` field if no record exists with the provided ID.
func (t *Table) Delete(db *sql.DB, id interface{}) error {
	var dummy string
	if err := db.QueryRow(
		fmt.Sprintf(
			"DELETE FROM \"%s\" WHERE \"%s\" = $1 RETURNING \"%s\"",
			t.Name,
			t.IDColumn().Name,
			t.IDColumn().Name,
		),
		id,
	).Scan(&dummy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t.NotFoundErr
		}
		return fmt.Errorf("deleting row from table `%s`: %w", t.Name, err)
	}
	return nil
}

// Insert puts the provided item into the table. If a record already exists
// with the same ID, the table's `ExistsErr` field will be returned. For UNIQUE
// columns, if the provided item has a value which already exists, the column's
// `Unique` field will be returned.
func (t *Table) Insert(db *sql.DB, item Item) error {
	return t.inserter().insert(db, item)
}

// Upsert puts the provided item into the table. If a record already exists
// with the same ID, the existing record will be updated provided there are no
// other constraint violations. For UNIQUE columns, if the provided item has a
// value which already exists, the column's `Unique` field will be returned.
func (t *Table) Upsert(db *sql.DB, item Item) error {
	return t.upserter().insert(db, item)
}

// Ensure creates the table if it doesn't already exist. If the table already
// exists but has a different schema, it will not be changed.
func (t *Table) Ensure(db *sql.DB) error {
	if _, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS \"%s\" (%s)",
		t.Name,
		createColumnsSQL(t.Columns, t.IDColumn().Name),
	)); err != nil {
		return fmt.Errorf("creating `%s` postgres table: %w", t.Name, err)
	}
	return nil
}

func createColumnsSQL(columns []Column, pkey string) string {
	if len(columns) < 1 {
		return ""
	}
	var sb strings.Builder
	columns[0].createSQL(&sb, pkey)
	for i := range columns[1:] {
		sb.WriteByte(',')
		sb.WriteByte(' ')
		columns[i+1].createSQL(&sb, pkey)
	}
	return sb.String()
}

// Drop drops the table.
func (t *Table) Drop(db *sql.DB) error {
	if _, err := db.Exec(fmt.Sprintf(
		"DROP TABLE IF EXISTS \"%s\"",
		t.Name,
	)); err != nil {
		return fmt.Errorf("dropping table `%s`: %w", t.Name, err)
	}
	return nil
}

// Clear truncates the table.
func (t *Table) Clear(db *sql.DB) error {
	if _, err := db.Exec(fmt.Sprintf(
		"DELETE FROM \"%s\"",
		t.Name,
	)); err != nil {
		return fmt.Errorf("clearing `%s` postgres table: %w", t.Name, err)
	}
	return nil
}

// Reset drops the table if it exists and recreates it.
func (t *Table) Reset(db *sql.DB) error {
	if err := t.Drop(db); err != nil {
		return err
	}
	return t.Ensure(db)
}

// Item represents a record in the table. It facilitates conversion between Go
// types and SQL records.
type Item interface {
	// Values takes a buffer with one slot per column in the table and
	// populates it *with values*. This is used for Insert and Upsert
	// operations.
	Values([]interface{})

	// Scan takes a buffer with one slot per column in the table and populates
	// it *with pointers* to data in the item. This is used for operations
	// which retrieve data from the database.
	Scan([]interface{})

	// ID returns the value which corresponds to the table's primary key
	// column.
	ID() interface{}
}
