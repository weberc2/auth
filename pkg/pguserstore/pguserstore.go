package pguserstore

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/weberc2/auth/pkg/auth/types"
	"github.com/weberc2/auth/pkg/pgutil"
)

// PGUserStore is a postgres implementation of `types.UserStore`.
type PGUserStore sql.DB

// OpenEnv creates a connection with a postgres database instance and validates
// the connection via ping.
func OpenEnv() (*PGUserStore, error) {
	db, err := pgutil.OpenEnvPing()
	return (*PGUserStore)(db), err
}

// EnsureTable creates the Postgres `users` table if it doesn't already exist.
// If any `users` table exists, this will return nil even if the schemas
// mismatch.
func (pgus *PGUserStore) EnsureTable() error {
	return Table.Ensure((*sql.DB)(pgus))
}

// DropTable drops the `users` Postgres table.
func (pgus *PGUserStore) DropTable() error {
	return Table.Drop((*sql.DB)(pgus))
}

// DropTable truncates the `users` Postgres table.
func (pgus *PGUserStore) ClearTable() error {
	return Table.Clear((*sql.DB)(pgus))
}

// ResetTable drops the `users` Postgres table if it exists and creates a new
// one from scratch.
func (pgus *PGUserStore) ResetTable() error {
	return Table.Reset((*sql.DB)(pgus))
}

// Insert adds a record to the `users` Postgres table. If a record already
// exists with the same ID, `types.ErrUserExists` is returned. If the provided
// ID is novel, but the provided email already exists, `types.ErrEmailExists`
// is returned.
func (pgus *PGUserStore) Insert(user *types.UserEntry) error {
	return Table.Insert((*sql.DB)(pgus), (*userEntry)(user))
}

// Insert adds a record to the `users` Postgres table. If a record already
// exists with the same ID, the record is updated provided there are no other
// constraint violations. If the provided ID is novel, but the provided email
// already exists, `types.ErrEmailExists` is returned.
func (pgus *PGUserStore) Upsert(user *types.UserEntry) error {
	return Table.Upsert((*sql.DB)(pgus), (*userEntry)(user))
}

// Get returns the record corresponding to the provided user ID. If no such
// user ID exists, `types.ErrUserNotFound` is returned.
func (pgus *PGUserStore) Get(user types.UserID) (*types.UserEntry, error) {
	var entry userEntry
	if err := Table.Get(
		(*sql.DB)(pgus),
		&userEntry{User: user},
		&entry,
	); err != nil {
		return nil, err
	}
	return (*types.UserEntry)(&entry), nil
}

// List returns all records in the table.
func (pgus *PGUserStore) List() ([]*types.UserEntry, error) {
	result, err := Table.List((*sql.DB)(pgus))
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

// Delete deletes a user from the table. If no user is found for the provided
// user ID, then `types.ErrUserNotFound` is returned.
func (pgus *PGUserStore) Delete(user types.UserID) error {
	return Table.Delete((*sql.DB)(pgus), &userEntry{User: user})
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

func (entry *userEntry) Scan(pointers []interface{}) {
	pointers[0] = &entry.User
	pointers[1] = &entry.Email
	pointers[2] = &entry.PasswordHash
	pointers[3] = &entry.Created
}

var (
	// fail compilation if `userEntry` doesn't implement the `pgutil.Item`
	// interface.
	_ pgutil.Item = &userEntry{}

	Table = pgutil.Table{
		Name: "users",
		PrimaryKeys: []pgutil.Column{
			{
				Name: "user",
				Type: "VARCHAR(32)",
				Null: false,
			},
		},
		OtherColumns: []pgutil.Column{
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
