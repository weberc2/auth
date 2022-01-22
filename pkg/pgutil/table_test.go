package pgutil

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/weberc2/auth/pkg/types"
	pz "github.com/weberc2/httpeasy"
)

func TestTable_Update(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		table       Table
		state       []DynamicItem
		input       DynamicItem
		wantedState []DynamicItem
		wantedErr   types.WantedError
	}{
		{
			name: "simple",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
			},
			input: DynamicItem{
				NewInteger(0),
				NewString("somethingelse@example.org"),
			},
			wantedState: []DynamicItem{
				{NewInteger(0), NewString("somethingelse@example.org")},
			},
		},
		{
			name: "some fields unchanged",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name: "email",
					Type: "VARCHAR(255)",
					Null: true,
				}, {
					Name: "age",
					Type: "INTEGER",
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state: []DynamicItem{
				{NewInteger(0), NewString("user@example.org"), NewInteger(10)},
			},
			input: DynamicItem{NewInteger(0), nil, NewInteger(25)},
			wantedState: []DynamicItem{
				{NewInteger(0), NewString("user@example.org"), NewInteger(25)},
			},
		},
		{
			name: "not found",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			input: DynamicItem{
				NewInteger(0),
				NewString("user@example.org"),
			},
			wantedErr: errRowNotFound,
		},
		{
			// given two distinct rows, if we try to update the value of a
			// UNIQUE column on one row to the same value as the other row,
			// expect the column's `Unique` constraint violation error to be
			// returned.
			name: "unique constraint violation",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
				{NewInteger(1), NewString("user-1@example.org")},
			},
			input: DynamicItem{NewInteger(1), NewString("user@example.org")},
			wantedState: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
				{NewInteger(1), NewString("user-1@example.org")},
			},
			wantedErr: types.ErrEmailExists,
		},
		{
			name: "missing primary key column",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
			},
			input: DynamicItem{nil, NewString("user@example.org")},
			wantedState: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
			},
			wantedErr: types.WantedErrFunc(func(found error) error {
				wanted := "building `update` SQL: nil value found for " +
					"primary key column `id`"
				if found.Error() != wanted {
					t.Fatalf("wanted `%s`; found `%v`", wanted, found.Error())
				}
				return nil
			}),
		},
		{
			name: "missing not-null column",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
			},
			input: DynamicItem{NewInteger(0), nil},
			wantedState: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
			},
			wantedErr: types.WantedErrFunc(func(found error) error {
				wanted := "building `update` SQL: nil value found for NOT " +
					"NULL column `email`"
				if found.Error() != wanted {
					t.Fatalf("wanted `%s`; found `%v`", wanted, found.Error())
				}
				return nil
			}),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.table.Reset(db); err != nil {
				t.Fatalf("unexpected error resetting test table: %v", err)
			}
			defer func() {
				if err := testCase.table.Drop(db); err != nil {
					t.Fatalf("failed to clean up after test case: %v", err)
				}
			}()

			for i, row := range testCase.state {
				if err := testCase.table.Insert(db, &row); err != nil {
					t.Fatalf(
						"preparing test state: inserting row %d: %v",
						i,
						err,
					)
				}
			}

			if testCase.wantedErr == nil {
				testCase.wantedErr = types.NilError{}
			}
			if err := testCase.wantedErr.CompareErr(
				testCase.table.Update(db, testCase.input),
			); err != nil {
				t.Fatal(err)
			}

			result, err := testCase.table.List(db)
			if err != nil {
				t.Fatalf("unexpected error listing table rows: %v", err)
			}
			newItem, err := DynamicItemFactoryFromTable(&testCase.table)
			if err != nil {
				t.Fatalf(
					"unexpected error building DynamicItemFactory: %v",
					err,
				)
			}
			items, err := result.ToDynamicItems(newItem)
			if err != nil {
				t.Fatal(err)
			}
			if err := CompareDynamicItems(testCase.wantedState, items); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTable_Upsert(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		table       Table
		state       []DynamicItem
		input       DynamicItem
		wantedState []DynamicItem
		wantedErr   types.WantedError
	}{
		{
			name: "simple",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			input: DynamicItem{
				NewInteger(0),
				NewString("user@example.org"),
			},
			wantedState: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
			},
		},
		{
			name: "exists",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
			},
			input: DynamicItem{
				NewInteger(0),
				NewString("somethingelse@example.org"),
			},
			wantedState: []DynamicItem{
				{NewInteger(0), NewString("somethingelse@example.org")},
			},
		},
		{
			// given two distinct rows, if we try to update the value of a
			// UNIQUE column on one row to the same value as the other row,
			// expect the column's `Unique` constraint violation error to be
			// returned.
			name: "unique constraint violation",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
				{NewInteger(1), NewString("user-1@example.org")},
			},
			input: DynamicItem{NewInteger(1), NewString("user@example.org")},
			wantedState: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
				{NewInteger(1), NewString("user-1@example.org")},
			},
			wantedErr: types.ErrEmailExists,
		},
		{
			name: "missing primary key column",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
			},
			input: DynamicItem{nil, NewString("user@example.org")},
			wantedState: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
			},
			wantedErr: types.WantedErrFunc(func(found error) error {
				wanted := "building `insert` SQL: nil value found for " +
					"primary key column `id`"
				if found.Error() != wanted {
					return fmt.Errorf("wanted `%s`; found `%s`", wanted, found.Error())
				}
				return nil
			}),
		},
		{
			name: "missing not-null column",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
			},
			input: DynamicItem{NewInteger(0), nil},
			wantedState: []DynamicItem{
				{NewInteger(0), NewString("user@example.org")},
			},
			wantedErr: types.WantedErrFunc(func(found error) error {
				wanted := "building `insert` SQL: nil value found for NOT " +
					"NULL column `email`"
				if found.Error() != wanted {
					return fmt.Errorf("wanted `%s`; found `%s`", wanted, found.Error())
				}
				return nil
			}),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.table.Reset(db); err != nil {
				t.Fatalf("unexpected error resetting test table: %v", err)
			}
			defer func() {
				if err := testCase.table.Drop(db); err != nil {
					t.Fatalf("failed to clean up after test case: %v", err)
				}
			}()

			for i, row := range testCase.state {
				if err := testCase.table.Insert(db, &row); err != nil {
					t.Fatalf(
						"preparing test state: inserting row %d: %v",
						i,
						err,
					)
				}
			}

			if testCase.wantedErr == nil {
				testCase.wantedErr = types.NilError{}
			}
			if err := testCase.wantedErr.CompareErr(
				testCase.table.Upsert(db, testCase.input),
			); err != nil {
				t.Fatal(err)
			}

			result, err := testCase.table.List(db)
			if err != nil {
				t.Fatalf("unexpected error listing table rows: %v", err)
			}
			items, err := result.ToDynamicItems(
				DynamicItemFactory(NilInteger, NilString),
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := CompareDynamicItems(testCase.wantedState, items); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTable_Insert(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		table       Table
		state       []row
		input       *row
		wantedState []row
		wantedErr   types.WantedError
	}{
		{
			name: "simple",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			input:       &row{0, "user@example.org"},
			wantedState: []row{{0, "user@example.org"}},
		},
		{
			name: "exists",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state:       []row{{0, "user@example.org"}},
			input:       &row{0, "user@example.org"},
			wantedState: []row{{0, "user@example.org"}},
			wantedErr:   errRowExists,
		},
		{
			name: "unique constraint violation",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state:       []row{{0, "user@example.org"}},
			input:       &row{1, "user@example.org"},
			wantedState: []row{{0, "user@example.org"}},
			wantedErr:   types.ErrEmailExists,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.table.Reset(db); err != nil {
				t.Fatalf("unexpected error resetting test table: %v", err)
			}
			defer func() {
				if err := testCase.table.Drop(db); err != nil {
					t.Fatalf("failed to clean up after test case: %v", err)
				}
			}()

			for i, row := range testCase.state {
				if err := testCase.table.Insert(db, &row); err != nil {
					t.Fatalf(
						"preparing test state: inserting row %d: %v",
						i,
						err,
					)
				}
			}

			if testCase.wantedErr == nil {
				testCase.wantedErr = types.NilError{}
			}
			if err := testCase.wantedErr.CompareErr(
				testCase.table.Insert(db, testCase.input),
			); err != nil {
				t.Fatal(err)
			}

			result, err := testCase.table.List(db)
			if err != nil {
				t.Fatalf("unexpected error listing table rows: %v", err)
			}
			rows, err := resultToRows(result)
			if err != nil {
				t.Fatal(err)
			}
			if err := compareRows(testCase.wantedState, rows); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTable_Get(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		table     Table
		state     []*row
		input     int
		wanted    row
		wantedErr types.WantedError
	}{
		{
			name: "exists",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state:  []*row{{0, "user@example.org"}},
			input:  0,
			wanted: row{0, "user@example.org"},
		},
		{
			name: "not found",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			wantedErr: errRowNotFound,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.table.Reset(db); err != nil {
				t.Fatalf("unexpected error resetting test table: %v", err)
			}

			for i, row := range testCase.state {
				if err := testCase.table.Insert(db, row); err != nil {
					t.Fatalf(
						"preparing test state: inserting row %d: %v",
						i,
						err,
					)
				}
			}

			if testCase.wantedErr == nil {
				testCase.wantedErr = types.NilError{}
			}
			var found row
			if err := testCase.wantedErr.CompareErr(
				testCase.table.Get(db, &row{id: testCase.input}, &found),
			); err != nil {
				t.Fatal(err)
			}

			if err := testCase.wanted.compare(&found); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTable_Delete(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		table       Table
		state       []row
		input       int
		wantedState []row
		wantedErr   types.WantedError
	}{
		{
			name: "simple",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			state: []row{{0, "user@example.org"}},
			input: 0,
		},
		{
			name: "not found",
			table: Table{
				Name:        "rows",
				PrimaryKeys: []Column{{Name: "id", Type: "INTEGER"}},
				OtherColumns: []Column{{
					Name:   "email",
					Type:   "VARCHAR(255)",
					Unique: types.ErrEmailExists,
				}},
				ExistsErr:   errRowExists,
				NotFoundErr: errRowNotFound,
			},
			input:     0,
			wantedErr: errRowNotFound,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.table.Reset(db); err != nil {
				t.Fatalf("unexpected error resetting test table: %v", err)
			}
			defer func() {
				if err := testCase.table.Drop(db); err != nil {
					t.Fatalf("failed to clean up after test case: %v", err)
				}
			}()

			for i, row := range testCase.state {
				if err := testCase.table.Insert(db, &row); err != nil {
					t.Fatalf(
						"preparing test state: inserting row %d: %v",
						i,
						err,
					)
				}
			}

			if testCase.wantedErr == nil {
				testCase.wantedErr = types.NilError{}
			}
			if err := testCase.wantedErr.CompareErr(
				testCase.table.Delete(db, &row{id: testCase.input}),
			); err != nil {
				t.Fatal(err)
			}

			result, err := testCase.table.List(db)
			if err != nil {
				t.Fatalf("unexpected error listing table rows: %v", err)
			}
			rows, err := resultToRows(result)
			if err != nil {
				t.Fatal(err)
			}
			if err := compareRows(testCase.wantedState, rows); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type row struct {
	id    int
	email string
}

func (r *row) Values(values []interface{}) {
	values[0] = r.id
	values[1] = r.email
}

func (r *row) Scan(pointers []interface{}) {
	pointers[0] = &r.id
	pointers[1] = &r.email
}

func (r *row) ID() interface{} { return r.id }

func (r *row) compare(found *row) error {
	if r == found {
		return nil
	}
	if r == nil && found != nil {
		return fmt.Errorf("wanted `nil`; found `%v`", found)
	}
	if r != nil && found == nil {
		return fmt.Errorf("wanted `%v`; found `nil`", r)
	}
	if r.id != found.id {
		return fmt.Errorf("id: wanted `%d`; found `%d`", r.id, found.id)
	}
	if r.email != found.email {
		return fmt.Errorf(
			"email: wanted `%s`; found `%s`",
			r.email,
			found.email,
		)
	}
	return nil
}

func compareRows(wanted, found []row) error {
	if len(wanted) != len(found) {
		return fmt.Errorf("wanted %d rows; found %d", len(wanted), len(found))
	}

	for i := range wanted {
		if err := wanted[i].compare(&found[i]); err != nil {
			return fmt.Errorf("mismatch on row %d: %w", i, err)
		}
	}

	return nil
}

func resultToRows(result *Result) ([]row, error) {
	var rows []row
	for result.Next() {
		var row row
		if err := result.Scan(&row); err != nil {
			return nil, fmt.Errorf("unexpected error scanning result: %w", err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

var (
	db = func() *sql.DB {
		db, err := OpenEnvPing()
		if err != nil {
			panic(fmt.Sprintf("unexpected error opening db conn: %v", err))
		}
		return db
	}()

	errRowNotFound = &pz.HTTPError{Status: 404, Message: "row not found"}
	errRowExists   = &pz.HTTPError{Status: 409, Message: "row exists"}
)
