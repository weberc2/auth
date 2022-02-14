package auth

import (
	"fmt"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/weberc2/auth/pkg/auth/types"
)

type Claims struct {
	User  types.UserID
	Email string
	jwt.StandardClaims
}

type ResetTokenFactory TokenFactory

func (rtf *ResetTokenFactory) Create(
	now time.Time,
	user types.UserID,
	email string,
) (string, error) {
	token := jwt.NewWithClaims(
		jwt.SigningMethodES512,
		Claims{
			User:  user,
			Email: email,
			StandardClaims: jwt.StandardClaims{
				Subject:   string(user),
				Audience:  rtf.Audience,
				Issuer:    rtf.Issuer,
				IssuedAt:  now.Unix(),
				ExpiresAt: now.Add(rtf.TokenValidity).Unix(),
				NotBefore: now.Unix(),
			},
		},
	)
	return token.SignedString(rtf.SigningKey)
}

func (rtf *ResetTokenFactory) Claims(token string) (*Claims, error) {
	var claims Claims
	if _, err := jwt.ParseWithClaims(
		token,
		&claims,
		func(*jwt.Token) (interface{}, error) {
			return &rtf.SigningKey.PublicKey, nil
		},
	); err != nil {
		return nil, fmt.Errorf("parsing claims from token: %w", err)
	}
	return &claims, nil
}
