package client

import (
	"crypto/ecdsa"
	"fmt"
	"strings"

	"github.com/dgrijalva/jwt-go"
	pz "github.com/weberc2/httpeasy"
)

type Authenticator struct {
	Key *ecdsa.PublicKey
}

func (a *Authenticator) Auth(authType AuthType, h pz.Handler) pz.Handler {
	return func(r pz.Request) pz.Response {
		result := authType.validate(a.Key, r)
		if result.User == "" {
			return pz.Unauthorized(nil, result)
		}
		r.Headers.Add("User", result.User)
		return h(r).WithLogging(result)
	}
}

func (a *Authenticator) Optional(authType AuthType, h pz.Handler) pz.Handler {
	return func(r pz.Request) pz.Response {
		result := authType.validate(a.Key, r)
		if result.User != "" {
			r.Headers.Add("User", result.User)
		}
		return h(r).WithLogging(result)
	}
}

type AuthType interface {
	validate(key *ecdsa.PublicKey, r pz.Request) *Result
}

type AuthTypeClientProgram struct{}

func (atcp AuthTypeClientProgram) validate(
	key *ecdsa.PublicKey,
	r pz.Request,
) *Result {
	authorization := r.Headers.Get("Authorization")
	if !strings.HasPrefix(authorization, "Bearer ") {
		return ResultErr(
			"invalid access token",
			fmt.Errorf("missing `Bearer` prefix"),
		)
	}

	user, err := validateAccessToken(authorization[len("Bearer "):], key)
	if err != nil {
		return ResultErr("invalid access token", err)
	}

	return ResultOK("successfully validated access token", user)
}

type Result struct {
	Message string `json:"message"`
	Error   string `json:"error,omitempty"`
	User    string `json:"user,omitempty"`
}

func ResultErr(message string, err error) *Result {
	return &Result{Message: message, Error: err.Error()}
}

func ResultOK(message string, user string) *Result {
	return &Result{Message: message, User: user}
}

func ConstantAuthType(r *Result) AuthType {
	return AuthTypeFunc(
		func(*ecdsa.PublicKey, pz.Request) *Result { return r },
	)
}

type AuthTypeFunc func(*ecdsa.PublicKey, pz.Request) *Result

func (atf AuthTypeFunc) validate(k *ecdsa.PublicKey, r pz.Request) *Result {
	return atf(k, r)
}

type AuthTypeWebServer struct {
	WebServerApp
}

func (atws *AuthTypeWebServer) validate(
	key *ecdsa.PublicKey,
	r pz.Request,
) *Result {
	accessCookie, err := r.Cookie("Access-Token")
	if err != nil {
		return ResultErr("missing `Access-Token` cookie", err)
	}

	refreshCookie, err := r.Cookie("Refresh-Token")
	if err != nil {
		return ResultErr("missing `Refresh-Token` cookie", err)
	}

	accessToken, err := atws.decryptCookie(accessCookie)
	if err != nil {
		return ResultErr("decrypting `Access-Token` cookie", err)
	}

	refreshToken, err := atws.decryptCookie(refreshCookie)
	if err != nil {
		return ResultErr("decrypting `Refresh-Token` cookie", err)
	}

	user, err := validateAccessToken(accessToken, key)
	if err != nil {
		if err, ok := err.(*jwt.ValidationError); ok {
			masked := err.Errors & jwt.ValidationErrorExpired
			if masked == jwt.ValidationErrorExpired {
				rsp, err := atws.Client.Refresh(refreshToken)
				if err != nil {
					return ResultErr("refreshing access token", err)
				}

				// We can probably trust that the token itself is good since
				// it's coming directly from the auth service, but we need its
				// user. If we got here, the previous access token's user
				// failed to parse because the token was expired.
				user, err := validateAccessToken(rsp.AccessToken, key)
				if err != nil {
					return ResultErr("parsing `sub` (user) claim", err)
				}

				encrypted, err := atws.Encrypt(rsp.AccessToken)
				if err != nil {
					return ResultErr("encrypting access token", err)
				}
				accessCookie.Value = encrypted
				return ResultOK("successfully refreshed access token", user)
			}
			return ResultErr("validating access token", err)
		}
		return ResultErr("parsing access token", err)
	}

	return ResultOK("successfully validated access token", user)
}

func validateAccessToken(token string, key *ecdsa.PublicKey) (string, error) {
	var claims jwt.StandardClaims
	if _, err := jwt.ParseWithClaims(
		token,
		&claims,
		func(*jwt.Token) (interface{}, error) {
			return key, nil
		},
	); err != nil {
		return "", err
	}
	return claims.Subject, nil
}
