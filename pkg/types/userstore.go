package types

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	pz "github.com/weberc2/httpeasy"
)

type UserID string

type UserEntry struct {
	User         UserID    `json:"user"`
	Email        string    `json:"email"`
	Created      time.Time `json:"created"`
	PasswordHash []byte    `json:"-"`
}

func (wanted *UserEntry) Compare(found *UserEntry) error {
	if wanted == found {
		return nil
	}
	if wanted == nil && found != nil {
		return fmt.Errorf("UserEntry: wanted `nil`; found not-nil")
	}
	if wanted != nil && found == nil {
		return fmt.Errorf("UserEntry: wanted not-nil; found `nil`")
	}
	if wanted.Email != found.Email {
		return fmt.Errorf(
			"UserEntry.Email: wanted `%s`; found `%s`",
			wanted.Email,
			found.Email,
		)
	}
	if !wanted.Created.Equal(found.Created) {
		return fmt.Errorf(
			"UserEntry.Created: wanted `%s`; found `%s`",
			wanted.Created,
			found.Created,
		)
	}
	if !bytes.Equal(wanted.PasswordHash, found.PasswordHash) {
		return fmt.Errorf(
			"UserEntry.PasswordHash: wanted `%s`; found `%s`",
			wanted.PasswordHash,
			found.PasswordHash,
		)
	}
	return nil
}

func CompareUserEntries(wanted, found []*UserEntry) error {
	if len(wanted) != len(found) {
		return fmt.Errorf(
			"len([]UserEntry): wanted `%d`; found `%d`",
			len(wanted),
			len(found),
		)
	}

	for i := range wanted {
		if err := wanted[i].Compare(found[i]); err != nil {
			return fmt.Errorf("[]UserEntry[%d]: %w", i, err)
		}
	}

	return nil
}

type UserStore interface {
	Get(UserID) (*UserEntry, error)
	Insert(*UserEntry) error
	Upsert(*UserEntry) error
}

var (
	ErrUserNotFound = &pz.HTTPError{
		Status:  http.StatusNotFound,
		Message: "user not found",
	}
	ErrUserExists = &pz.HTTPError{
		Status:  http.StatusConflict,
		Message: "user exists",
	}
	ErrEmailExists = &pz.HTTPError{
		Status:  http.StatusConflict,
		Message: "email address exists",
	}
)
