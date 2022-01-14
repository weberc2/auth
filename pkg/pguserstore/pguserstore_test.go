package pguserstore

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/weberc2/auth/pkg/types"
)

func TestPGUserStore_Upsert(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		state       []types.UserEntry
		input       *types.UserEntry
		wantedState []*types.UserEntry
		wantedErr   types.WantedError
	}{
		{
			name: "create",
			input: &types.UserEntry{
				User:         "user",
				Email:        "user@example.org",
				PasswordHash: []byte("passwordhash"),
				Created:      now,
			},
			wantedState: []*types.UserEntry{{
				User:         "user",
				Email:        "user@example.org",
				PasswordHash: []byte("passwordhash"),
				Created:      now,
			}},
		},
		{
			name: "username exists",
			state: []types.UserEntry{{
				User:         "user",
				Email:        "somethingelse@example.org",
				PasswordHash: []byte("anything"),
				Created:      now.Add(-1 * time.Hour),
			}},
			input: &types.UserEntry{
				User:         "user",
				Email:        "user@example.org",
				PasswordHash: []byte("passwordhash"),
				Created:      now,
			},
			wantedState: []*types.UserEntry{{
				User:         "user",
				Email:        "user@example.org",
				PasswordHash: []byte("passwordhash"),
				Created:      now,
			}},
		},
		{
			name: "email exists",
			state: []types.UserEntry{{
				User:         "somethingelse",
				Email:        "user@example.org",
				PasswordHash: []byte("anything"),
				Created:      now,
			}},
			input: &types.UserEntry{
				User:         "user",
				Email:        "user@example.org",
				PasswordHash: []byte("passwordhash"),
				Created:      now,
			},
			wantedState: []*types.UserEntry{{
				User:         "somethingelse",
				Email:        "user@example.org",
				PasswordHash: []byte("anything"),
				Created:      now,
			}},
			wantedErr: types.ErrEmailExists,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := prepare(testCase.state); err != nil {
				t.Fatalf("unexpected error preparing test case: %v", err)
			}

			if testCase.wantedErr == nil {
				testCase.wantedErr = types.NilError{}
			}
			if err := testCase.wantedErr.CompareErr(
				store.Upsert(testCase.input),
			); err != nil {
				t.Fatal(err)
			}

			entries, err := store.List()
			if err != nil {
				t.Fatalf("unexpected error listing users: %v", err)
			}

			if err := types.CompareUserEntries(
				testCase.wantedState,
				entries,
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPGUserStore_Insert(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		state       []types.UserEntry
		input       *types.UserEntry
		wantedState []*types.UserEntry
		wantedErr   types.WantedError
	}{
		{
			name: "simple",
			input: &types.UserEntry{
				User:         "user",
				Email:        "user@example.org",
				PasswordHash: []byte("passwordhash"),
				Created:      now,
			},
			wantedState: []*types.UserEntry{{
				User:         "user",
				Email:        "user@example.org",
				PasswordHash: []byte("passwordhash"),
				Created:      now,
			}},
		},
		{
			name: "username exists",
			state: []types.UserEntry{{
				User:         "user",
				Email:        "somethingelse@example.org",
				PasswordHash: []byte("anything"),
				Created:      now,
			}},
			input: &types.UserEntry{
				User:         "user",
				Email:        "user@example.org",
				PasswordHash: []byte("passwordhash"),
				Created:      now,
			},
			wantedState: []*types.UserEntry{{
				User:         "user",
				Email:        "somethingelse@example.org",
				PasswordHash: []byte("anything"),
				Created:      now,
			}},
			wantedErr: types.ErrUserExists,
		},
		{
			name: "email exists",
			state: []types.UserEntry{{
				User:         "somethingelse",
				Email:        "user@example.org",
				PasswordHash: []byte("anything"),
				Created:      now,
			}},
			input: &types.UserEntry{
				User:         "user",
				Email:        "user@example.org",
				PasswordHash: []byte("passwordhash"),
				Created:      now,
			},
			wantedState: []*types.UserEntry{{
				User:         "somethingelse",
				Email:        "user@example.org",
				PasswordHash: []byte("anything"),
				Created:      now,
			}},
			wantedErr: types.ErrEmailExists,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := prepare(testCase.state); err != nil {
				t.Fatalf("unexpected error preparing test case: %v", err)
			}

			if testCase.wantedErr == nil {
				testCase.wantedErr = types.NilError{}
			}
			if err := testCase.wantedErr.CompareErr(
				store.Insert(testCase.input),
			); err != nil {
				t.Fatal(err)
			}

			entries, err := store.List()
			if err != nil {
				t.Fatalf("unexpected error listing users: %v", err)
			}

			if err := types.CompareUserEntries(
				testCase.wantedState,
				entries,
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}

var (
	now   = time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	store = func() *PGUserStore {
		s, err := OpenEnv()
		if err != nil {
			log.Fatalf(
				"unexpected error opening token store database: %v",
				err,
			)
		}
		if err := s.ResetTable(); err != nil {
			log.Fatalf(
				"unexpected error resetting token store postgres table: %v",
				err,
			)
		}
		return s
	}()
)

func prepare(state []types.UserEntry) error {
	if err := store.ClearTable(); err != nil {
		return fmt.Errorf("preparing postgres table: %w", err)
	}

	for i := range state {
		if err := store.Insert(&state[i]); err != nil {
			return fmt.Errorf(
				"preparing postgres table: "+
					"unexpected error inserting state item at index `%d`: %w",
				i,
				err,
			)
		}
	}

	return nil
}
