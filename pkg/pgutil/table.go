package pgutil

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Column represents a Postgres table column.
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

type Result struct {
	pointers []interface{}
	rows     *sql.Rows
}

func (r *Result) Scan(item Item) error {
	finish := item.Scan(r.pointers)
	if err := r.rows.Scan(r.pointers...); err != nil {
		return err
	}
	return finish()
}

func (r *Result) Next() bool { return r.rows.Next() }

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

const idColumnPosition = 0

func (t *Table) IDColumn() *Column { return &t.Columns[idColumnPosition] }

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
	finish := out.Scan(pointers)

	if err := db.QueryRow(
		fmt.Sprintf(
			"SELECT %s FROM \"%s\" WHERE \"%s\" = $1",
			columnNames.String(),
			t.Name,
			t.IDColumn().Name,
		),
	).Scan(pointers...); err != nil {
		return fmt.Errorf(
			"getting record from `%s` postgres table: %w",
			t.Name,
			err,
		)
	}
	if err := finish(); err != nil {
		return fmt.Errorf(
			"getting record from `%s` postgres table: %w",
			t.Name,
			err,
		)
	}

	return nil
}

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

func (t *Table) Insert(db *sql.DB, item Item) error {
	return t.inserter().insert(db, item)
}

func (t *Table) Upsert(db *sql.DB, item Item) error {
	return t.upserter().insert(db, item)
}

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

func (t *Table) Drop(db *sql.DB) error {
	if _, err := db.Exec(fmt.Sprintf(
		"DROP TABLE IF EXISTS \"%s\"",
		t.Name,
	)); err != nil {
		return fmt.Errorf("dropping table `%s`: %w", t.Name, err)
	}
	return nil
}

func (t *Table) Clear(db *sql.DB) error {
	if _, err := db.Exec(fmt.Sprintf(
		"DELETE FROM \"%s\"",
		t.Name,
	)); err != nil {
		return fmt.Errorf("clearing `%s` postgres table: %w", t.Name, err)
	}
	return nil
}

func (t *Table) Reset(db *sql.DB) error {
	if err := t.Drop(db); err != nil {
		return err
	}
	return t.Ensure(db)
}

type Item interface {
	Values([]interface{})
	Scan([]interface{}) func() error
	ID() interface{}
}
