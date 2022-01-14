package auth

import (
	"testing"

	"github.com/weberc2/auth/pkg/types"
	"golang.org/x/crypto/bcrypt"
)

func TestCreate(t *testing.T) {
	const password = "oiusdpafohwerkljsfkljads;fweqr"

	var entry *types.UserEntry
	if err := (&CredStore{&userStoreMock{
		insert: func(e *types.UserEntry) error { entry = e; return nil },
	}}).Create(&types.Credentials{
		User:     "user",
		Email:    "user@example.org",
		Password: password,
	}); err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}

	if entry == nil {
		t.Fatalf("UserStore.Create() not called or called with nil value")
	}

	if entry.User != "user" {
		t.Fatalf("UserStore.User: wanted 'user'; found '%s'", entry.User)
	}

	if entry.Email != "user@example.org" {
		t.Fatalf(
			"UserStore.Email: wanted 'user@example.org'; found '%s'",
			entry.Email,
		)
	}

	if err := bcrypt.CompareHashAndPassword(
		entry.PasswordHash,
		[]byte(password),
	); err != nil {
		t.Fatalf(
			"UserStore.PasswordHash: not generated from password '%s'",
			password,
		)
	}
}

func TestUpsert(t *testing.T) {
	const password = "oiusdpafohwerkljsfkljads;fweqr"

	var entry *types.UserEntry
	if err := (&CredStore{&userStoreMock{
		upsert: func(e *types.UserEntry) error { entry = e; return nil },
	}}).Upsert(&types.Credentials{
		User:     "user",
		Email:    "user@example.org",
		Password: password,
	}); err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}

	if entry == nil {
		t.Fatalf("UserStore.Upsert() not called or called with nil value")
	}

	if entry.User != "user" {
		t.Fatalf("UserStore.User: wanted 'user'; found '%s'", entry.User)
	}

	if entry.Email != "user@example.org" {
		t.Fatalf(
			"UserStore.Email: wanted 'user@example.org'; found '%s'",
			entry.Email,
		)
	}

	if err := bcrypt.CompareHashAndPassword(
		entry.PasswordHash,
		[]byte(password),
	); err != nil {
		t.Fatalf(
			"UserStore.PasswordHash: not generated from password '%s'",
			password,
		)
	}
}
