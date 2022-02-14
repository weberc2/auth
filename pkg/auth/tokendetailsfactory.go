package auth

import (
	"fmt"
	"time"

	"github.com/weberc2/auth/pkg/auth/types"
)

type TokenDetails struct {
	AccessToken  types.Token `json:"accessToken"`
	RefreshToken types.Token `json:"refreshToken"`
}

type TokenDetailsFactory struct {
	AccessTokens  TokenFactory
	RefreshTokens TokenFactory
	TimeFunc      func() time.Time
}

func (tdf *TokenDetailsFactory) Create(subject string) (*TokenDetails, error) {
	now := tdf.TimeFunc()
	accessToken, err := tdf.AccessTokens.Create(now, subject)
	if err != nil {
		return nil, fmt.Errorf("creating access token: %w", err)
	}

	refreshToken, err := tdf.RefreshTokens.Create(now, subject)
	if err != nil {
		return nil, fmt.Errorf("creating refresh token: %w", err)
	}

	return &TokenDetails{
		AccessToken:  *accessToken,
		RefreshToken: *refreshToken,
	}, nil
}

func (tdf *TokenDetailsFactory) AccessToken(subject string) (string, error) {
	tok, err := tdf.AccessTokens.Create(tdf.TimeFunc(), subject)
	if err != nil {
		return "", fmt.Errorf("creating access token: %w", err)
	}
	return tok.Token, nil
}
