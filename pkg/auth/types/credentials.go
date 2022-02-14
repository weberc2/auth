package types

import (
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

type Credentials struct {
	User     UserID `json:"user"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (wanted *Credentials) CompareUserEntry(found *UserEntry) error {
	if wanted == nil && found == nil {
		return nil
	}

	if wanted != nil && found == nil {
		return fmt.Errorf("unexpected `nil`")
	}

	if wanted == nil && found != nil {
		return fmt.Errorf("wanted `nil`; found not `nil`")
	}

	if wanted.User != found.User {
		return fmt.Errorf(
			"types.UserEntry.User: wanted `%s`; found `%s`",
			wanted.User,
			found.User,
		)
	}

	if wanted.Email != found.Email {
		return fmt.Errorf(
			"types.UserEntry.Email: wanted `%s`; found `%s`",
			wanted.Email,
			found.Email,
		)
	}

	if bcrypt.CompareHashAndPassword(
		found.PasswordHash,
		[]byte(wanted.Password),
	) != nil {
		return fmt.Errorf(
			"types.UserEntry.PasswordHash: hash doesn't match password `%s`",
			wanted.Password,
		)
	}

	return nil
}
