package pgutil

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/lib/pq"
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
	Columns []*Column
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

func (t *Table) IDColumn() *Column { return t.Columns[0] }

func (t *Table) Exists(db *sql.DB, id interface{}, notFoundErr error) error {
	var dummy string
	if err := db.QueryRow(
		fmt.Sprintf("SELECT true FROM \"%s\" WHERE \"%s\" = $1",
			t.Name,
			t.IDColumn().Name,
		),
		id,
	).Scan(&dummy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return notFoundErr
		}
		return fmt.Errorf("checking for row in table `%s`: %w", t.Name, err)
	}
	return nil
}

func (t *Table) Delete(db *sql.DB, id interface{}, notFoundErr error) error {
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
			return notFoundErr
		}
		return fmt.Errorf("deleting row from table `%s`: %w", t.Name, err)
	}
	return nil
}

func (t *Table) Insert(db *sql.DB, item Item, pkeyConstraintErr error) error {
	return t.Inserter(pkeyConstraintErr).Insert(db, item)
}

type Inserter struct {
	table             *Table
	insertSQL         string
	valuesBuffer      []interface{}
	pkeyConstraintErr error
}

func (i *Inserter) Upsert(db *sql.DB, item Item) error {
	var sb strings.Builder

	// There's always at least 1 column.
	if len(i.table.Columns) > 1 {
		sb.WriteByte('"')
		sb.WriteString(i.table.Columns[1].Name)
		sb.WriteByte('"')
		sb.WriteString("=$2")

		for j, column := range i.table.Columns[2:] {
			sb.WriteByte(',')
			sb.WriteByte(' ')
			sb.WriteByte('"')
			sb.WriteString(column.Name)
			sb.WriteByte('"')
			sb.WriteByte('=')
			sb.WriteByte('$')
			sb.WriteString(strconv.Itoa(j + 3))
		}
	}

	const idColumnPosition = 0
	return i.helper(
		db,
		item,
		fmt.Sprintf(
			"%s ON CONFLICT (\"%s\") DO UPDATE SET %s WHERE \"%s\".\"%s\" = $%d",
			i.insertSQL,
			i.table.IDColumn().Name,
			sb.String(),
			i.table.Name,
			i.table.IDColumn().Name,
			idColumnPosition+1, // postgres placeholders are 1-indexed
		),
	)
}

func (i *Inserter) Insert(db *sql.DB, item Item) error {
	return i.helper(db, item, i.insertSQL)
}

func (i *Inserter) helper(db *sql.DB, item Item, sql string) error {
	log.Println("SQL:", sql)
	item.Values(i.valuesBuffer)
	if _, err := db.Exec(sql, i.valuesBuffer...); err != nil {
		if err, ok := err.(*pq.Error); ok && err.Code == "23505" {
			if fmt.Sprintf("%s_pkey", i.table.Name) == err.Constraint {
				return i.pkeyConstraintErr
			}
			prefix := i.table.Name + "_"
			suffix := "_key"
			if strings.HasPrefix(err.Constraint, prefix) &&
				strings.HasSuffix(err.Constraint, suffix) {
				column := err.Constraint[len(prefix) : len(err.Constraint)-len(suffix)]
				for _, c := range i.table.Columns {
					if c.Name == column {
						return c.Unique
					}
				}
			}
			return err
		}
		return fmt.Errorf(
			"inserting row into postgres table `%s`: %w",
			i.table.Name,
			err,
		)
	}
	return nil
}

func (t *Table) Inserter(pkeyConstraintErr error) *Inserter {
	var columnNames, placeholders strings.Builder
	columnNames.WriteByte('"')
	columnNames.WriteString(t.Columns[0].Name)
	columnNames.WriteByte('"')
	placeholders.WriteString("$1")

	for i := range t.Columns[1:] {
		columnNames.WriteByte(',')
		columnNames.WriteByte(' ')
		columnNames.WriteByte('"')
		columnNames.WriteString(t.Columns[i+1].Name)
		columnNames.WriteByte('"')

		placeholders.WriteByte(',')
		placeholders.WriteByte(' ')
		placeholders.WriteString(fmt.Sprintf("$%d", i+2))
	}
	return &Inserter{
		table: t,
		insertSQL: fmt.Sprintf(
			"INSERT INTO \"%s\" (%s) VALUES(%s)",
			t.Name,
			columnNames.String(),
			placeholders.String(),
		),
		valuesBuffer:      make([]interface{}, len(t.Columns)),
		pkeyConstraintErr: pkeyConstraintErr,
	}
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

func createColumnsSQL(columns []*Column, pkey string) string {
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

func createColumnSQL(sb *strings.Builder, c *Column) {
	sb.WriteByte('"')
	sb.WriteString(c.Name)
	sb.WriteByte('"')
	sb.WriteByte(' ')
	sb.WriteString(c.Type)
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
