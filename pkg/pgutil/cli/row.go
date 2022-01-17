package cli

import (
	"fmt"
	"time"

	"github.com/weberc2/auth/pkg/pgutil"
)

type Row []interface{}

func RowFuncFromColumns(columns ...pgutil.Column) (func() Row, error) {
	cellFuncs := make([]func(Row, int), len(columns))
	for i := range columns {
		switch columns[i].Type {
		case "TIMESTAMP", "TIMESTAMPTZ":
			cellFuncs[i] = func(row Row, i int) { row[i] = new(time.Time) }
		case "INTEGER":
			cellFuncs[i] = func(row Row, i int) { row[i] = new(int) }
		case "TEXT":
			cellFuncs[i] = func(row Row, i int) { row[i] = new(string) }
		default:
			if _, err := parseVarChar(columns[i].Type); err == nil {
				cellFuncs[i] = func(row Row, i int) { row[i] = new(string) }
			} else {
				return nil, fmt.Errorf(
					"column `%s` has unsupported type: %s",
					columns[i].Name,
					columns[i].Type,
				)
			}
		}
	}
	return func() Row {
		row := make(Row, len(cellFuncs))
		for i := range cellFuncs {
			cellFuncs[i](row, i)
		}
		return row
	}, nil
}

func RowFromColumns(columns ...pgutil.Column) (Row, error) {
	row := make(Row, len(columns))
	for i := range columns {
		switch columns[i].Type {
		case "TIMESTAMP", "TIMESTAMPTZ":
			row[i] = new(time.Time)
		case "INTEGER":
			row[i] = new(int)
		case "TEXT":
			row[i] = new(string)
		default:
			if _, err := parseVarChar(columns[i].Type); err == nil {
				row[i] = new(string)
			} else {
				return nil, fmt.Errorf(
					"column `%s` has unsupported type: %s",
					columns[i].Name,
					columns[i].Type,
				)
			}
		}
	}
	return row, nil
}

func (r Row) Values(values []interface{}) { copy(values, r) }
func (r Row) Scan(pointers []interface{}) { copy(pointers, r) }
func (r Row) ID() interface{}             { return r[0] }
