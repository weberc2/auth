package auth

import (
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/nbutton23/zxcvbn-go"
	"github.com/weberc2/auth/pkg/types"
	pz "github.com/weberc2/httpeasy"

	"golang.org/x/crypto/bcrypt"
)

var ErrPasswordTooSimple = &pz.HTTPError{
	Status:  http.StatusBadRequest,
	Message: "password is too simple",
}

type CredStore struct {
	Users types.UserStore
}

func (cs *CredStore) Validate(creds *types.Credentials) error {
	entry, err := cs.Users.Get(creds.User)
	if err != nil {
		log.Printf("error fetching user `%s`: %v", creds.User, err)
		// If the user doesn't exist, we want to return ErrCredentials in order
		// to minimize the information we give to potential attackers.
		if errors.Is(err, types.ErrUserNotFound) {
			return ErrCredentials
		}
		return fmt.Errorf("validating credentials: %w", err)
	}

	if err := bcrypt.CompareHashAndPassword(
		entry.PasswordHash,
		[]byte(creds.Password),
	); err != nil {
		return ErrCredentials
	}
	return nil
}

func validatePassword(creds *types.Credentials) error {
	minEntropyMatch := zxcvbn.PasswordStrength(
		creds.Password,
		[]string{string(creds.User), creds.Email},
	)
	if minEntropyMatch.Score < 3 {
		return fmt.Errorf("validating password: %w", ErrPasswordTooSimple)
	}
	return nil
}

func makeUserEntry(creds *types.Credentials) (*types.UserEntry, error) {
	if err := validatePassword(creds); err != nil {
		return nil, err
	}
	hashedPassword, err := bcrypt.GenerateFromPassword(
		[]byte(creds.Password),
		bcrypt.DefaultCost,
	)
	if err != nil {
		return nil, err
	}
	return &types.UserEntry{
		User:         creds.User,
		Email:        creds.Email,
		PasswordHash: hashedPassword,
	}, nil
}

func (cs *CredStore) Create(creds *types.Credentials) error {
	entry, err := makeUserEntry(creds)
	if err != nil {
		return fmt.Errorf("creating credentials: %w", err)
	}

	if err := cs.Users.Insert(entry); err != nil {
		return fmt.Errorf("creating credentials: %w", err)
	}
	return nil
}

func (cs *CredStore) Upsert(creds *types.Credentials) error {
	entry, err := makeUserEntry(creds)
	if err != nil {
		return fmt.Errorf("creating user entry: %w", err)
	}

	if err := cs.Users.Upsert(entry); err != nil {
		return fmt.Errorf("upserting user store: %w", err)
	}

	return nil
}
