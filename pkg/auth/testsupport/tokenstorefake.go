package testsupport

import (
	"time"

	"github.com/weberc2/auth/pkg/auth/types"
)

type TokenStoreFake map[string]time.Time

func (tsf TokenStoreFake) Put(token string, expires time.Time) error {
	if _, found := tsf[token]; found {
		return types.ErrTokenExists
	}
	tsf[token] = expires
	return nil
}

func (tsf TokenStoreFake) Exists(token string) error {
	if _, found := tsf[token]; found {
		return nil
	}
	return types.ErrTokenNotFound
}

func (tsf TokenStoreFake) Delete(token string) error {
	if _, found := tsf[token]; found {
		delete(tsf, token)
		return nil
	}
	return types.ErrTokenNotFound
}

func (tsf TokenStoreFake) DeleteExpired(now time.Time) error {
	for token, expires := range tsf {
		if expires.Before(now) {
			delete(tsf, token)
		}
	}
	return nil
}

func (tsf TokenStoreFake) List() ([]types.Token, error) {
	out := make([]types.Token, 0, len(tsf))
	for token, expires := range tsf {
		out = append(out, types.Token{Token: token, Expires: expires})
	}
	return out, nil
}
