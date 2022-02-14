package pgtokenstore

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/weberc2/auth/pkg/auth/types"
)

func TestPGTokenStore_DeleteExpired(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		state       []types.Token
		token       string
		wantedState []types.Token
		wantedErr   types.WantedError
	}{
		{
			name: "simple",
			state: []types.Token{
				{Token: "token0", Expires: beforeNow},
				{Token: "token1", Expires: afterNow},
			},
			token: "token",
			wantedState: []types.Token{{
				Token:   "token1",
				Expires: afterNow,
			}},
		},
	} {
		if err := prepare(testCase.state); err != nil {
			t.Fatal(err)
		}

		if testCase.wantedErr == nil {
			testCase.wantedErr = types.NilError{}
		}
		if err := testCase.wantedErr.CompareErr(
			store.DeleteExpired(now),
		); err != nil {
			t.Fatal(err)
		}

		found, err := store.List()
		if err != nil {
			t.Fatalf("unexpected error listing entries: %v", err)
		}
		if err := types.CompareTokens(
			testCase.wantedState,
			found,
		); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPGTokenStore_Delete(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		state       []types.Token
		token       string
		wantedState []types.Token
		wantedErr   types.WantedError
	}{
		{
			name:  "simple",
			state: []types.Token{{Token: "token", Expires: beforeNow}},
			token: "token",
		},
		{
			name:      "not found",
			token:     "token",
			wantedErr: types.ErrTokenNotFound,
		},
	} {
		if err := prepare(testCase.state); err != nil {
			t.Fatal(err)
		}

		if testCase.wantedErr == nil {
			testCase.wantedErr = types.NilError{}
		}
		if err := testCase.wantedErr.CompareErr(
			store.Delete(testCase.token),
		); err != nil {
			t.Fatal(err)
		}

		found, err := store.List()
		if err != nil {
			t.Fatalf("unexpected error listing entries: %v", err)
		}
		if err := types.CompareTokens(
			testCase.wantedState,
			found,
		); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPGTokenStore_Exists(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		state     []types.Token
		token     string
		wantedErr types.WantedError
	}{
		{
			name:  "simple",
			state: []types.Token{{Token: "token", Expires: beforeNow}},
			token: "token",
		},
		{
			name:      "not found",
			token:     "token",
			wantedErr: types.ErrTokenNotFound,
		},
	} {
		if err := prepare(testCase.state); err != nil {
			t.Fatal(err)
		}

		if testCase.wantedErr == nil {
			testCase.wantedErr = types.NilError{}
		}
		if err := testCase.wantedErr.CompareErr(
			store.Exists(testCase.token),
		); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPGTokenStore_Put(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		state       []types.Token
		token       string
		expires     time.Time
		wantedErr   types.WantedError
		wantedState []types.Token
	}{
		{
			name:    "simple",
			token:   "token",
			expires: afterNow,
			wantedState: []types.Token{{
				Token:   "token",
				Expires: afterNow,
			}},
		},
		{
			name: "token exists",
			state: []types.Token{{
				Token:   "token",
				Expires: beforeNow,
			}},
			token:   "token",
			expires: now,
			wantedState: []types.Token{{
				Token:   "token",
				Expires: beforeNow,
			}},
			wantedErr: types.ErrTokenExists,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := prepare(testCase.state); err != nil {
				t.Fatal(err)
			}

			if testCase.wantedErr == nil {
				testCase.wantedErr = types.NilError{}
			}
			if err := testCase.wantedErr.CompareErr(
				store.Put(testCase.token, testCase.expires),
			); err != nil {
				t.Fatal(err)
			}

			found, err := store.List()
			if err != nil {
				t.Fatalf("unexpected error listing entries: %v", err)
			}
			if err := types.CompareTokens(
				testCase.wantedState,
				found,
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}

var (
	now       = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	beforeNow = time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC)
	afterNow  = time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC)
	store     = func() *PGTokenStore {
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

func prepare(state []types.Token) error {
	if err := store.ClearTable(); err != nil {
		return fmt.Errorf("preparing postgres table: %w", err)
	}

	for i, entry := range state {
		if err := store.Put(entry.Token, entry.Expires); err != nil {
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
