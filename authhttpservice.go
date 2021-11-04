package main

import (
	"errors"

	pz "github.com/weberc2/httpeasy"
)

type AuthHTTPService struct {
	AuthService
}

func (ahs *AuthHTTPService) LoginRoute() pz.Route {
	return pz.Route{
		Path:   "/login",
		Method: "POST",
		Handler: func(r pz.Request) pz.Response {
			var creds Credentials
			if err := r.JSON(&creds); err != nil {
				return pz.BadRequest(nil, struct {
					Message, Error string
				}{
					Message: "failed to parse login JSON",
					Error:   err.Error(),
				})
			}

			tokens, err := ahs.Login(&creds)
			if err != nil {
				if errors.Is(err, ErrCredentials) {
					return pz.Unauthorized(pz.String(
						"Invalid username or password",
					))
				}
			}

			return pz.Ok(
				pz.JSON(tokens),
				struct {
					Message string
					User    UserID
				}{
					Message: "authentication succeeded",
					User:    creds.Username,
				},
			)
		},
	}
}

func (ahs *AuthHTTPService) RefreshRoute() pz.Route {
	return pz.Route{
		Path:   "/refresh",
		Method: "POST",
		Handler: func(r pz.Request) pz.Response {
			var payload struct {
				RefreshToken string `json:"refreshToken"`
			}
			if err := r.JSON(&payload); err != nil {
				return pz.BadRequest(nil, struct{ Message, Error string }{
					Message: "failed to parse refresh JSON",
					Error:   err.Error(),
				})
			}

			accessToken, err := ahs.Refresh(payload.RefreshToken)
			if err != nil {
				return pz.InternalServerError(struct{ Message, Error string }{
					Message: "refreshing access token",
					Error:   err.Error(),
				})
			}

			return pz.Ok(pz.JSON(struct {
				AccessToken string `json:"accessToken"`
			}{accessToken}))
		},
	}
}

func (ahs *AuthHTTPService) RegisterRoute() pz.Route {
	return pz.Route{
		Path:   "/register",
		Method: "POST",
		Handler: func(r pz.Request) pz.Response {
			var payload struct {
				User UserID `json:"user"`
			}
			if err := r.JSON(&payload); err != nil {
				return pz.BadRequest(nil, struct{ Message, Error string }{
					Message: "failed to parse register JSON",
					Error:   err.Error(),
				})
			}

			if err := ahs.Register(payload.User); err != nil {
				if errors.Is(err, ErrUserExists) {
					return pz.Conflict(
						pz.String("User already exists"),
						struct {
							Message, Error string
							User           UserID
						}{
							Message: "registering user",
							Error:   err.Error(),
							User:    payload.User,
						},
					)
				}
			}

			return pz.Created(pz.String("Created user"), struct {
				Message string
				User    UserID
			}{
				Message: "created user",
				User:    payload.User,
			})
		},
	}
}

func (ahs *AuthHTTPService) UpdatePasswordRoute() pz.Route {
	return pz.Route{
		Path:   "/password",
		Method: "PATCH",
		Handler: func(r pz.Request) pz.Response {
			var payload UpdatePassword
			if err := r.JSON(&payload); err != nil {
				return pz.BadRequest(nil, struct {
					Message, Error string
					User           UserID
				}{
					Message: "updating password",
					Error:   err.Error(),
					User:    payload.User,
				})
			}

			if err := ahs.UpdatePassword(&payload); err != nil {
				if errors.Is(err, ErrResetTokenNotFound) {
					return pz.NotFound(
						pz.String(ErrResetTokenNotFound.Error()),
						struct {
							Message, Error string
							User           UserID
						}{
							Message: "updating password",
							Error:   err.Error(),
							User:    payload.User,
						},
					)
				}
			}

			return pz.Ok(pz.String("Password updated"), struct {
				Message string
				User    UserID
			}{
				Message: "updated password",
				User:    payload.User,
			})
		},
	}
}

func (ahs *AuthHTTPService) Routes() []pz.Route {
	return []pz.Route{
		ahs.LoginRoute(),
		ahs.RefreshRoute(),
		ahs.RegisterRoute(),
		ahs.UpdatePasswordRoute(),
	}
}
