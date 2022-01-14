package pgutil

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

func (t *Table) inserter() *inserter {
	return &inserter{
		table:        t,
		sql:          t.insertSQL(),
		valuesBuffer: make([]interface{}, len(t.Columns)),
	}
}

func (t *Table) upserter() *inserter {
	return &inserter{
		table:        t,
		sql:          t.upsertSQL(),
		valuesBuffer: make([]interface{}, len(t.Columns)),
	}
}

func (t *Table) upsertSQL() string {
	var sb strings.Builder

	// There's always at least 1 column.
	if len(t.Columns) > 1 {
		sb.WriteByte('"')
		sb.WriteString(t.Columns[1].Name)
		sb.WriteByte('"')
		sb.WriteString("=$2")

		for j, column := range t.Columns[2:] {
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

	return fmt.Sprintf(
		"%s ON CONFLICT (\"%s\") DO UPDATE SET %s WHERE \"%s\".\"%s\" = $%d",
		t.insertSQL(),
		t.IDColumn().Name,
		sb.String(),
		t.Name,
		t.IDColumn().Name,
		idColumnPosition+1, // postgres placeholders are 1-indexed
	)
}

func (t *Table) insertSQL() string {
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

	return fmt.Sprintf(
		"INSERT INTO \"%s\" (%s) VALUES(%s)",
		t.Name,
		columnNames.String(),
		placeholders.String(),
	)
}

type inserter struct {
	table        *Table
	sql          string
	valuesBuffer []interface{}
}

func (i *inserter) insert(db *sql.DB, item Item) error {
	item.Values(i.valuesBuffer)
	if _, err := db.Exec(i.sql, i.valuesBuffer...); err != nil {
		if err, ok := err.(*pq.Error); ok && err.Code == "23505" {
			if fmt.Sprintf("%s_pkey", i.table.Name) == err.Constraint {
				return i.table.ExistsErr
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
