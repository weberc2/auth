package pgtokenstore

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/weberc2/auth/pkg/pgutil"
	"github.com/weberc2/auth/pkg/types"
)

type PGTokenStore sql.DB

func OpenEnv() (*PGTokenStore, error) {
	db, err := pgutil.OpenEnvPing()
	return (*PGTokenStore)(db), err
}

func (pgts *PGTokenStore) EnsureTable() error {
	return table.Ensure((*sql.DB)(pgts))
}

func (pgts *PGTokenStore) DropTable() error {
	return table.Drop((*sql.DB)(pgts))
}

func (pgts *PGTokenStore) ClearTable() error {
	return table.Clear((*sql.DB)(pgts))
}

func (pgts *PGTokenStore) ResetTable() error {
	return table.Reset((*sql.DB)(pgts))
}

func (pgts *PGTokenStore) Put(token string, expires time.Time) error {
	return table.Insert((*sql.DB)(pgts), &tokenEntry{token, expires})
}

func (pgts *PGTokenStore) Exists(token string) error {
	return table.Exists((*sql.DB)(pgts), token)
}

func (pgts *PGTokenStore) Delete(token string) error {
	return table.Delete((*sql.DB)(pgts), token)
}

// DeleteExpired deletes all tokens that expired before `now`.
func (pgts *PGTokenStore) DeleteExpired(now time.Time) error {
	if _, err := (*sql.DB)(pgts).Exec(
		fmt.Sprintf(
			"DELETE FROM \"%s\" WHERE \"%s\" < $1",
			table.Name,
			columnExpires.Name,
		),
		now,
	); err != nil {
		return fmt.Errorf("deleting expired tokens from postgres: %w", err)
	}
	return nil
}

func (pgts *PGTokenStore) List() ([]types.Token, error) {
	// we don't want to return a `nil` slice because that gets JSON-marshaled
	// to `null` instead of `[]`.
	entries := []types.Token{}

	result, err := table.List((*sql.DB)(pgts))
	if err != nil {
		return nil, fmt.Errorf("listing tokens: %w", err)
	}

	for result.Next() {
		entries = append(entries, types.Token{})
		if err := result.Scan(
			(*tokenEntry)(&entries[len(entries)-1]),
		); err != nil {
			return nil, fmt.Errorf("listing tokens: %w", err)
		}
	}

	return entries, err
}

type tokenEntry types.Token

func (entry *tokenEntry) ID() interface{} { return entry.Token }

func (entry *tokenEntry) Scan(pointers []interface{}) func() error {
	var expires string
	pointers[0] = &entry.Token
	pointers[1] = &expires
	return func() error {
		exp, err := time.Parse(time.RFC3339, expires)
		if err != nil {
			return fmt.Errorf("parsing `expires` field: %w", err)
		}
		entry.Expires = exp
		return nil
	}
}

func (entry *tokenEntry) Values(values []interface{}) {
	values[0] = entry.Token
	values[1] = entry.Expires
}

var (
	_ types.TokenStore = &PGTokenStore{}

	columnExpires = pgutil.Column{
		Name: "expires",
		Type: "TIMESTAMP",
	}
	table = pgutil.Table{
		Name: "tokens",
		Columns: []*pgutil.Column{
			{Name: "token", Type: "VARCHAR(9000)"},
			&columnExpires,
		},
		ExistsErr:   types.ErrTokenExists,
		NotFoundErr: types.ErrTokenNotFound,
	}
)
