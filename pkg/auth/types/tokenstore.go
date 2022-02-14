package types

import (
	"fmt"
	"net/http"
	"time"

	pz "github.com/weberc2/httpeasy"
)

var (
	ErrTokenNotFound = &pz.HTTPError{
		Status:  http.StatusUnauthorized,
		Message: "unauthorized",
	}
	ErrTokenExists = &pz.HTTPError{
		Status:  http.StatusConflict,
		Message: "token exists",
	}
)

type Token struct {
	Token   string    `json:"token"`
	Expires time.Time `json:"expires"`
}

func (wanted *Token) Compare(found *Token) error {
	if wanted == found {
		return nil
	}
	if (wanted == nil && found != nil) || (wanted != nil && found == nil) {
		return fmt.Errorf("TokenEntry: wanted `%v`; found `%v`", wanted, found)
	}

	if wanted.Token != found.Token {
		return fmt.Errorf(
			"TokenEntry.Token: wanted `%s`; found `%s`",
			wanted.Token,
			found.Token,
		)
	}

	if !wanted.Expires.Equal(found.Expires) {
		return fmt.Errorf(
			"TokenEntry.Expires: wanted `%s`; found `%s`",
			wanted.Expires,
			found.Expires,
		)
	}

	return nil
}

func CompareTokens(wanted, found []Token) error {
	if len(wanted) != len(found) {
		return fmt.Errorf(
			"len([]TokenEntry): wanted `%d`; found `%d`",
			len(wanted),
			len(found),
		)
	}

	for i := range wanted {
		if err := wanted[i].Compare(&found[i]); err != nil {
			return fmt.Errorf("[]TokenEntry[%d]: %w", i, err)
		}
	}

	return nil
}

type TokenStore interface {
	// Put stores a token. Returns `ErrTokenexists` if the token already
	// exists. Other errors (e.g., I/O errors) may also be returned.
	Put(token string, expires time.Time) error

	// Exists returns `nil` if the token exists or `ErrTokenNotFound` if not.
	// Other errors (e.g., I/O errors) may also be returned.
	Exists(token string) error

	// Delete deletes a token. If the token doesn't exist, `ErrTokenNotFound`
	// will be returned. Other errors (e.g., I/O errors) may also be returned.
	Delete(token string) error

	// Delete expired will delete all tokens which expire before the provieded
	// time.
	DeleteExpired(time.Time) error

	// List all token entries.
	List() ([]Token, error)
}
