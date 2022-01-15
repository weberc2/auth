package pgutil

import "database/sql"

// Result is an iterator over SQL query result rows.
type Result struct {
	pointers []interface{}
	rows     *sql.Rows
}

// Scan scans the current result record into an item.
func (r *Result) Scan(item Item) error {
	item.Scan(r.pointers)
	return r.rows.Scan(r.pointers...)
}

// Next advances the iterator to the next record in the result set.
func (r *Result) Next() bool { return r.rows.Next() }
