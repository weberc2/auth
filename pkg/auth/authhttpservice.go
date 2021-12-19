package auth

import (
	"errors"
	"fmt"

	"github.com/dgrijalva/jwt-go"
	"github.com/weberc2/auth/pkg/types"
	pz "github.com/weberc2/httpeasy"
)

var (
	ErrInvalidRefreshToken = &pz.HTTPError{
		Status:  401,
		Message: "invalid refresh token",
	}
)

type AuthHTTPService struct {
	AuthService
}

func (ahs *AuthHTTPService) LoginRoute() pz.Route {
	return pz.Route{
		Path:   "/api/login",
		Method: "POST",
		Handler: func(r pz.Request) pz.Response {
			var creds types.Credentials
			if err := r.JSON(&creds); err != nil {
				return pz.BadRequest(nil, &logging{
					Message: "failed to parse login JSON",
					Error:   err.Error(),
				})
			}

			tokens, err := ahs.Login(&creds)
			if err != nil {
				if errors.Is(err, ErrCredentials) {
					return pz.Unauthorized(
						pz.String("Invalid username or password"),
						&logging{
							Message: "invalid username or password",
							User:    creds.User,
							Error:   err.Error(),
						},
					)
				}

				return pz.InternalServerError(
					&logging{
						Message:   "logging in",
						Error:     err.Error(),
						ErrorType: fmt.Sprintf("%T", err),
						User:      creds.User,
					},
				)
			}

			return pz.Ok(
				pz.JSON(tokens),
				&logging{
					Message: "authentication succeeded",
					User:    creds.User,
				},
			)
		},
	}
}

func (ahs *AuthHTTPService) RefreshRoute() pz.Route {
	return pz.Route{
		Path:   "/api/refresh",
		Method: "POST",
		Handler: func(r pz.Request) pz.Response {
			var payload struct {
				RefreshToken string `json:"refreshToken"`
			}
			if err := r.JSON(&payload); err != nil {
				return pz.BadRequest(nil, &logging{
					Message: "failed to parse refresh JSON",
					Error:   err.Error(),
				})
			}

			accessToken, err := ahs.Refresh(payload.RefreshToken)
			if err != nil {
				var verr *jwt.ValidationError
				if errors.As(err, &verr) {
					return pz.Unauthorized(
						pz.JSON(ErrInvalidRefreshToken),
						&logging{
							Message: "invalid refresh token",
							Error:   err.Error(),
						},
					)
				}
				return pz.InternalServerError(&logging{
					Message:   "refreshing access token",
					ErrorType: fmt.Sprintf("%T", err),
					Error:     err.Error(),
				})
			}

			return pz.Ok(pz.JSON(&refresh{accessToken}))
		},
	}
}

func (ahs *AuthHTTPService) ForgotPasswordRoute() pz.Route {
	return pz.Route{
		Path:   "/api/password/forgot",
		Method: "POST",
		Handler: func(r pz.Request) pz.Response {
			var payload struct {
				User types.UserID `json:"user"`
			}
			if err := r.JSON(&payload); err != nil {
				return pz.BadRequest(nil, struct{ Message, Error string }{
					Message: "failed to parse forgot-password JSON",
					Error:   err.Error(),
				})
			}

			if err := ahs.ForgotPassword(payload.User); err != nil {
				// If the user doesn't exist, we still report success so as to
				// not give away information to potential attackers.
				if errors.Is(err, types.ErrUserNotFound) {
					return pz.Ok(nil, struct{ Message, User, Error string }{
						Message: "user not found; silently succeeding",
						User:    string(payload.User),
						Error:   err.Error(),
					})
				}

				return pz.InternalServerError(&logging{
					Message:   "triggering forget-password notification",
					User:      payload.User,
					ErrorType: fmt.Sprintf("%T", err),
					Error:     err.Error(),
				})
			}

			return pz.Ok(nil, &logging{
				Message: "password reset notification sent",
				User:    payload.User,
			})
		},
	}
}

func (ahs *AuthHTTPService) RegisterRoute() pz.Route {
	return pz.Route{
		Path:   "/api/register",
		Method: "POST",
		Handler: func(r pz.Request) pz.Response {
			var payload struct {
				User  types.UserID `json:"user"`
				Email string       `json:"email"`
			}
			if err := r.JSON(&payload); err != nil {
				return pz.BadRequest(nil, struct{ Message, Error string }{
					Message: "failed to parse register JSON",
					Error:   err.Error(),
				})
			}

			if err := ahs.Register(payload.User, payload.Email); err != nil {
				if errors.Is(err, ErrInvalidEmail) {
					return pz.BadRequest(
						pz.String("Invalid email address"),
						struct {
							Error string
						}{
							Error: err.Error(),
						},
					)
				}
				if errors.Is(err, ErrUserExists) {
					return pz.Conflict(
						pz.String("User already exists"),
						struct {
							Message, Error string
							User           types.UserID
						}{
							Message: "registering user",
							Error:   err.Error(),
							User:    payload.User,
						},
					)
				}
				return pz.InternalServerError(&logging{
					Message:   "registering user",
					ErrorType: fmt.Sprintf("%T", err),
					Error:     err.Error(),
					User:      payload.User,
				})
			}

			return pz.Created(pz.String("Created user"), struct {
				Message string
				User    types.UserID
			}{
				Message: "created user",
				User:    payload.User,
			})
		},
	}
}

func (ahs *AuthHTTPService) UpdatePasswordRoute() pz.Route {
	return pz.Route{
		Path:   "/api/password",
		Method: "PATCH",
		Handler: func(r pz.Request) pz.Response {
			var payload UpdatePassword
			if err := r.JSON(&payload); err != nil {
				return pz.BadRequest(nil, &logging{
					Message: "updating password",
					Error:   err.Error(),
					User:    payload.User,
				})
			}

			if err := ahs.UpdatePassword(&payload); err != nil {
				l := logging{
					Message:   "updating password",
					Error:     err.Error(),
					ErrorType: fmt.Sprintf("%T", err),
					User:      payload.User,
				}
				if errors.Is(err, ErrInvalidResetToken) {
					return pz.NotFound(
						pz.String(ErrInvalidResetToken.Error()),
						&l,
					)
				}
				return pz.InternalServerError(&l)
			}

			return pz.Ok(pz.String("Password updated"), &logging{
				Message: "updated password",
				User:    payload.User,
			})
		},
	}
}

func (ahs *AuthHTTPService) ExchangeRoute() pz.Route {
	return pz.Route{
		Path:   "/api/exchange",
		Method: "POST",
		Handler: func(r pz.Request) pz.Response {
			var code Code
			if err := r.JSON(&code); err != nil {
				return pz.BadRequest(nil, &logging{
					Message: "parsing auth code token payload",
					Error:   err.Error(),
				})
			}

			tokens, err := ahs.Exchange(code.Code)
			if err != nil {
				return pz.HandleError("processing auth code exchange", err)
			}

			return pz.Ok(
				pz.JSON(tokens),
				&logging{Message: "auth code valid; returning tokens"},
			)
		},
	}
}

func (ahs *AuthHTTPService) Routes() []pz.Route {
	return []pz.Route{
		ahs.LoginRoute(),
		ahs.RefreshRoute(),
		ahs.RegisterRoute(),
		ahs.ForgotPasswordRoute(),
		ahs.UpdatePasswordRoute(),
		ahs.ExchangeRoute(),
	}
}

type logging struct {
	Message   string       `json:"message"`
	User      types.UserID `json:"user,omitempty"`
	ErrorType string       `json:"errorType,omitempty"`
	Error     string       `json:"error,omitempty"`
}

type refresh struct {
	AccessToken string `json:"accessToken"`
}