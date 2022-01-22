package pgutil

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
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

func (c *Column) createSQL(sb *strings.Builder) {
	// name
	c.nameSQL(sb)
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
}

func (c *Column) nameSQL(sb *strings.Builder) {
	sb.WriteByte('"')
	sb.WriteString(c.Name)
	sb.WriteByte('"')
}

func columnPredicate(
	sb *strings.Builder,
	table string,
	c *Column,
	placeholder int,
) {
	// write table name
	sb.WriteByte('"')
	sb.WriteString(table)
	sb.WriteByte('"')
	sb.WriteByte('.')

	c.nameSQL(sb)
	sb.WriteByte('=')
	sb.WriteByte('$')
	sb.WriteString(strconv.Itoa(placeholder))
}

func columnsPredicate(
	sb *strings.Builder,
	table string,
	head *Column,
	tail ...Column,
) {
	columnPredicate(sb, table, head, 1)
	for i := range tail {
		sb.WriteString(" AND ")
		columnPredicate(sb, table, &tail[i], i+1)
	}
}

// Table represents a SQL table.
type Table struct {
	// Name is the name of the table.
	Name string

	// PrimaryKeys are the primary key columns. These should not overlap with
	// columns defined in the `Columns` field. If there is more than one column
	// defined in this field, then the table's primary key is a composite key.
	PrimaryKeys []Column

	// OtherColumns is the list of non-primary-key columns in the table. None
	// of the columns defined in this field should be defined in the
	// `PrimaryKeys` field or vice-versa.
	OtherColumns []Column

	// ExistsErr is returned when there is a primary key conflict error.
	ExistsErr error

	// NotFoundErr is returned when there a primary key can't be found.
	NotFoundErr error
}

// List lists the records in the table.
func (t *Table) List(db *sql.DB) (*Result, error) {
	var sb strings.Builder
	t.columnNames(&sb)

	rows, err := db.Query(fmt.Sprintf(
		"SELECT %s FROM \"%s\"",
		sb.String(),
		t.Name,
	))
	if err != nil {
		return nil, fmt.Errorf("listing rows from table `%s`: %w", t.Name, err)
	}

	return &Result{rows: rows, pointers: t.buffer()}, nil
}

// Get retrieves a single item by ID and scans it into the provided `out` item.
// If the item isn't found, the table's `NotFoundErr` field will be returned.
func (t *Table) Get(db *sql.DB, id, out Item) error {
	var columnNames, predicate strings.Builder
	t.columnNames(&columnNames)
	t.primaryKeysPredicate(&predicate)

	if err := db.QueryRow(
		fmt.Sprintf(
			"SELECT %s FROM \"%s\" WHERE %s",
			columnNames.String(),
			t.Name,
			predicate.String(),
		),
		t.primaryKeys(id)...,
	).Scan(t.pointers(out)...); err != nil {
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

func (t *Table) pointers(item Item) []interface{} {
	buf := t.buffer()
	item.Scan(buf)
	return buf
}

func (t *Table) Columns() []Column {
	return append(t.PrimaryKeys, t.OtherColumns...)
}

func (t *Table) buffer() []interface{} {
	return make([]interface{}, len(t.PrimaryKeys)+len(t.OtherColumns))
}

func (t *Table) primaryKeys(item Item) []interface{} {
	buf := t.buffer()
	item.Values(buf)
	return buf[:len(t.PrimaryKeys)]
}

// Exists returns `nil` if a record exists for the provided ID, otherwise it
// returns the Table's `NotFoundErr` field.
func (t *Table) Exists(db *sql.DB, item Item) error {
	var dummy string
	var sb strings.Builder
	t.primaryKeysPredicate(&sb)
	if err := db.QueryRow(
		fmt.Sprintf("SELECT true FROM \"%s\" WHERE %s", t.Name, sb.String()),
		t.primaryKeys(item)...,
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
func (t *Table) Delete(db *sql.DB, id Item) error {
	var names, predicate strings.Builder
	t.primaryKeysPredicate(&predicate)
	t.primaryKeysNames(&names)
	var dummy string
	if err := db.QueryRow(
		fmt.Sprintf(
			"DELETE FROM \"%s\" WHERE %s RETURNING %s",
			t.Name,
			predicate.String(),
			names.String(),
		),
		t.primaryKeys(id)...,
	).Scan(&dummy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t.NotFoundErr
		}
		return fmt.Errorf("deleting row from table `%s`: %w", t.Name, err)
	}
	return nil
}

func (t *Table) primaryKeysNames(sb *strings.Builder) {
	columnsNames(sb, &t.PrimaryKeys[0], t.PrimaryKeys[1:]...)
}

func (t *Table) primaryKeysPredicate(sb *strings.Builder) {
	columnsPredicate(sb, t.Name, &t.PrimaryKeys[0], t.PrimaryKeys[1:]...)
}

func (t *Table) columnNames(sb *strings.Builder) {
	columnsNames(
		sb,
		&t.PrimaryKeys[0],
		append(t.PrimaryKeys[1:], t.OtherColumns...)...,
	)
}

func columnsNames(sb *strings.Builder, head *Column, tail ...Column) {
	head.nameSQL(sb)

	for i := range tail {
		sb.WriteByte(',')
		sb.WriteByte(' ')
		tail[i].nameSQL(sb)
	}
}

// Insert puts the provided item into the table. If a record already exists
// with the same ID, the table's `ExistsErr` field will be returned. For UNIQUE
// columns, if the provided item has a value which already exists, the column's
// `Unique` field will be returned.
func (t *Table) Insert(db *sql.DB, item Item) error {
	return insert(db, t, (*Table).insertSQL, item)
}

// Upsert puts the provided item into the table. If a record already exists
// with the same ID, the existing record will be updated provided there are no
// other constraint violations. For UNIQUE columns, if the provided item has a
// value which already exists, the column's `Unique` field will be returned.
func (t *Table) Upsert(db *sql.DB, item Item) error {
	return insert(db, t, (*Table).upsertSQL, item)
}

// Update updates a row in a table. If the row isn't found, the table's
// `NotFoundErr` field is returned.
func (t *Table) Update(db *sql.DB, item Item) error {
	return update(db, t, item)
}

func (t *Table) updateSQL(columns []Column) string {
	if len(columns) < 1 {
		return ""
	}

	// There must be an ID column in position 0 and at least one other column
	// to set because neither `UPDATE <table> SET WHERE <idColumn>=<id>` nor
	// `UPDATE <table> WHERE <idColumn>=<id>` are valid SQL.
	var predicate, names strings.Builder
	t.primaryKeysPredicate(&predicate)
	t.primaryKeysNames(&names)
	return fmt.Sprintf(
		"UPDATE \"%s\" SET %s WHERE %s RETURNING %s",
		t.Name,
		t.setListSQL(columns),
		predicate.String(),
		names.String(),
	)
}

func (t *Table) insertSQL() string {
	var columnNames, placeholders strings.Builder
	t.columnNames(&columnNames)
	t.placeholders(&placeholders)

	return fmt.Sprintf(
		"INSERT INTO \"%s\" (%s) VALUES(%s)",
		t.Name,
		columnNames.String(),
		placeholders.String(),
	)
}

func (t *Table) placeholders(sb *strings.Builder) {
	sb.WriteString("$1")

	for i := 1; i < len(t.PrimaryKeys)+len(t.OtherColumns); i++ {
		sb.WriteByte(',')
		sb.WriteByte(' ')
		sb.WriteByte('$')
		sb.WriteString(strconv.Itoa(i + 1))
	}
}

func update(
	db *sql.DB,
	table *Table,
	item Item,
) error {
	columns, values, err := table.columnsAndValues(item)
	if err != nil {
		return fmt.Errorf("building `update` SQL: %w", err)
	}
	rows, err := db.Query(table.updateSQL(columns), values...)
	if err != nil {
		return fmt.Errorf(
			"inserting row into postgres table `%s`: %w",
			table.Name,
			handleErr(table, err),
		)
	}
	if !rows.Next() {
		return fmt.Errorf(
			"inserting row into postgres table `%s`: %w",
			table.Name,
			table.NotFoundErr,
		)
	}
	return nil
}

func insert(
	db *sql.DB,
	table *Table,
	sqlFunc func(*Table) string,
	item Item,
) error {
	_, values, err := table.columnsAndValues(item)
	if err != nil {
		return fmt.Errorf("building `insert` SQL: %w", err)
	}
	if _, err := db.Exec(sqlFunc(table), values...); err != nil {
		return fmt.Errorf(
			"inserting row into postgres table `%s`: %w",
			table.Name,
			handleErr(table, err),
		)
	}
	return nil
}

func handleErr(table *Table, err error) error {
	if err, ok := err.(*pq.Error); ok && err.Code == "23505" {
		if fmt.Sprintf("%s_pkey", table.Name) == err.Constraint {
			return table.ExistsErr
		}
		prefix := table.Name + "_"
		suffix := "_key"
		if strings.HasPrefix(err.Constraint, prefix) &&
			strings.HasSuffix(err.Constraint, suffix) {
			column := err.Constraint[len(prefix) : len(err.Constraint)-len(suffix)]
			for _, c := range table.OtherColumns {
				if c.Name == column {
					return c.Unique
				}
			}
		}
	}
	return err
}

func (t *Table) columnsAndValues(item Item) ([]Column, []interface{}, error) {
	buf := t.buffer()
	item.Values(buf)
	var (
		columns      []Column
		nonNilValues = buf[:len(t.PrimaryKeys)]
	)

	for i, v := range nonNilValues {
		if v == nil {
			return nil, nil, fmt.Errorf(
				"nil value found for primary key column `%s`",
				t.PrimaryKeys[i].Name,
			)
		}
	}

	optional := buf[len(t.PrimaryKeys):]
	for i := range optional {
		if optional[i] != nil {
			columns = append(columns, t.OtherColumns[i])
			nonNilValues = append(nonNilValues, optional[i])
			continue
		}
		if !t.OtherColumns[i].Null {
			return nil, nil, fmt.Errorf(
				"nil value found for NOT NULL column `%s`",
				t.OtherColumns[i].Name,
			)
		}
	}
	return columns, nonNilValues, nil
}

func (t *Table) upsertSQL() string {
	// build the SET list (the list of "<COLUMN>"=<value> pairs following the
	// SET keyword)
	var pkeys, predicate strings.Builder
	t.primaryKeysNames(&pkeys)
	t.primaryKeysPredicate(&predicate)
	return fmt.Sprintf(
		"%s ON CONFLICT (%s) DO UPDATE SET %s WHERE %s",
		t.insertSQL(),
		pkeys.String(),
		t.setListSQL(t.OtherColumns),
		predicate.String(),
	)
}

// build the SET list (the list of "<COLUMN>"=<value> pairs following the SET
// keyword)
func (t *Table) setListSQL(columns []Column) string {
	var sb strings.Builder
	columns[0].nameSQL(&sb)
	sb.WriteByte('=')
	sb.WriteByte('$')
	sb.WriteString(strconv.Itoa(len(t.PrimaryKeys) + 1))

	tail := columns[1:]
	for i := range tail {
		sb.WriteByte(',')
		sb.WriteByte(' ')
		tail[i].nameSQL(&sb)
		sb.WriteByte('=')
		sb.WriteByte('$')
		sb.WriteString(strconv.Itoa(len(t.PrimaryKeys) + i + 2))
	}
	return sb.String()
}

// Ensure creates the table if it doesn't already exist. If the table already
// exists but has a different schema, it will not be changed.
func (t *Table) Ensure(db *sql.DB) error {
	if _, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS \"%s\" %s",
		t.Name,
		t.createColumnsSQL(),
	)); err != nil {
		return fmt.Errorf("creating `%s` postgres table: %w", t.Name, err)
	}
	return nil
}

func (t *Table) createColumnsSQL() string {
	var sb strings.Builder
	sb.WriteByte('(')
	t.PrimaryKeys[0].createSQL(&sb)
	tail := append(t.PrimaryKeys[1:], t.OtherColumns...)
	for i := range tail {
		sb.WriteByte(',')
		sb.WriteByte(' ')
		tail[i].createSQL(&sb)
	}
	sb.WriteString(", PRIMARY KEY (")
	t.primaryKeysNames(&sb)
	sb.WriteByte(')') // close `PRIMARY KEY (`
	sb.WriteByte(')')
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
}
