package pguserstore

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/weberc2/auth/pkg/pgutil"
	"github.com/weberc2/auth/pkg/types"
)

type PGUserStore sql.DB

func OpenEnv() (*PGUserStore, error) {
	db, err := pgutil.OpenEnvPing()
	return (*PGUserStore)(db), err
}

func (pgus *PGUserStore) EnsureTable() error {
	return table.Ensure((*sql.DB)(pgus))
}

func (pgus *PGUserStore) DropTable() error {
	return table.Drop((*sql.DB)(pgus))
}

func (pgus *PGUserStore) ClearTable() error {
	return table.Clear((*sql.DB)(pgus))
}

func (pgus *PGUserStore) ResetTable() error {
	return table.Reset((*sql.DB)(pgus))
}

func (pgus *PGUserStore) Insert(user *types.UserEntry) error {
	return table.Insert((*sql.DB)(pgus), (*userEntry)(user))
}

func (pgus *PGUserStore) Upsert(user *types.UserEntry) error {
	return table.Upsert((*sql.DB)(pgus), (*userEntry)(user))
}

func (pgus *PGUserStore) Get(user types.UserID) (*types.UserEntry, error) {
	var entry userEntry
	if err := table.Get((*sql.DB)(pgus), user, &entry); err != nil {
		return nil, err
	}
	return (*types.UserEntry)(&entry), nil
}

func (pgus *PGUserStore) List() ([]*types.UserEntry, error) {
	result, err := table.List((*sql.DB)(pgus))
	if err != nil {
		return nil, fmt.Errorf("listing users: %w", err)
	}
	var values []userEntry
	var entries []*types.UserEntry
	for result.Next() {
		values = append(values, userEntry{})
		entry := &values[len(values)-1]
		if err := result.Scan(entry); err != nil {
			return nil, fmt.Errorf("scanning user entry: %w", err)
		}
		entries = append(entries, (*types.UserEntry)(entry))
	}
	return entries, nil
}

func (pgus *PGUserStore) Delete(user types.UserID) error {
	return table.Delete((*sql.DB)(pgus), user)
}

// Implement `pgutil.Item` for `types.UserEntry`.
//
// Since the implementation for a `pgutil.Item` is tightly coupled to the table
// (specifically the number and quantity of columns), we're going to collocate
// the implementation with the column definition/specification rather than
// implementing the interface on `types.UserEntry` directly.
type userEntry types.UserEntry

func (entry *userEntry) Values(values []interface{}) {
	values[0] = entry.User
	values[1] = entry.Email
	values[2] = entry.PasswordHash
	values[3] = &entry.Created
}

func (entry *userEntry) Scan(pointers []interface{}) func() error {
	pointers[0] = &entry.User
	pointers[1] = &entry.Email
	pointers[2] = &entry.PasswordHash
	pointers[3] = &entry.Created
	return func() error { return nil }
}

func (entry *userEntry) ID() interface{} { return entry.User }

var (
	table = pgutil.Table{
		Name: "users",
		Columns: []*pgutil.Column{
			{
				Name: "user",
				Type: "VARCHAR(32)",
				Null: false,
			},
			{
				Name:   "email",
				Type:   "VARCHAR(128)",
				Unique: types.ErrEmailExists,
				Null:   false,
			},
			{
				Name: "pwhash",
				Type: "VARCHAR(255)",
				Null: false,
			},
			{
				Name: "created",
				Type: "TIMESTAMPTZ",
				Null: false,
			},
		},
		ExistsErr:   types.ErrUserExists,
		NotFoundErr: types.ErrUserNotFound,
	}

	// make sure this satisfies the `types.UserStore` interface
	_ types.UserStore = (*PGUserStore)(nil)
)
