package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/weberc2/auth/pkg/auth/testsupport"
	"github.com/weberc2/auth/pkg/auth/types"
	"golang.org/x/crypto/bcrypt"
)

type userStoreMock struct {
	get    func(types.UserID) (*types.UserEntry, error)
	upsert func(*types.UserEntry) error
	insert func(*types.UserEntry) error
}

func (usm *userStoreMock) Get(u types.UserID) (*types.UserEntry, error) {
	if usm.get == nil {
		panic("userStoreMock: missing `get` hook")
	}
	return usm.get(u)
}

func (usm *userStoreMock) Upsert(entry *types.UserEntry) error {
	if usm.upsert == nil {
		panic("userStoreMock: missing `upsert` hook")
	}
	return usm.upsert(entry)
}

func (usm *userStoreMock) Insert(entry *types.UserEntry) error {
	if usm.insert == nil {
		panic("userStoreMock: missing `create` hook")
	}
	return usm.insert(entry)
}

func TestAuthService_ConfirmRegistration(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		subject   types.UserID
		token     string
		email     string
		password  string
		wanted    *types.Credentials
		wantedErr types.WantedError
	}{
		{
			name:     "simple",
			subject:  "subject",
			token:    mustResetToken(now, "subject", "subject@example.org"),
			email:    "subject@example.org",
			password: goodPassword,
			wanted: &types.Credentials{
				User:     "subject",
				Email:    "subject@example.org",
				Password: goodPassword,
			},
		},
		{
			name:     "token parse err",
			subject:  "user",
			token:    "",
			email:    "user@example.org",
			password: goodPassword,
			wanted:   nil,
			wantedErr: TokenClaimsParseErr(fmt.Errorf(
				"parsing claims from token: %v",
				jwt.NewValidationError(
					"token contains an invalid number of segments",
					jwt.ValidationErrorMalformed,
				),
			)),
		},
		{
			name:      "password validation err",
			subject:   "user",
			token:     mustResetToken(now, "user", "user@example.org"),
			email:     "user@example.org",
			password:  "", // invalid
			wanted:    nil,
			wantedErr: ErrPasswordTooSimple,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			jwt.TimeFunc = func() time.Time { return now }
			defer func() { jwt.TimeFunc = time.Now }()

			userStore := testsupport.UserStoreFake{}
			authService := AuthService{
				Creds:  CredStore{userStore},
				Tokens: testsupport.TokenStoreFake{},
				TokenDetails: TokenDetailsFactory{
					AccessTokens:  accessTokenFactory,
					RefreshTokens: refreshTokenFactory,
					TimeFunc:      func() time.Time { return now },
				},
				Codes:         codesTokenFactory,
				ResetTokens:   resetTokenFactory,
				TimeFunc:      func() time.Time { return now },
				Notifications: &testsupport.NotificationServiceFake{},
			}
			if testCase.wantedErr == nil {
				testCase.wantedErr = types.NilError{}
			}
			if err := testCase.wantedErr.CompareErr(
				authService.ConfirmRegistration(
					testCase.token,
					testCase.password,
				),
			); err != nil {
				t.Fatal(err)
			}

			entry, err := userStore.Get(testCase.subject)
			if testCase.wanted == nil && errors.Is(
				err,
				types.ErrUserNotFound,
			) {
				return
			}
			if err != nil {
				t.Fatalf(
					"unexpected error getting user from user store: %v",
					err,
				)
			}

			if err := testCase.wanted.CompareUserEntry(entry); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func mustResetToken(now time.Time, user types.UserID, email string) string {
	t, err := resetTokenFactory.Create(now, user, email)
	if err != nil {
		panic(fmt.Sprintf("creating reset token: %v", err))
	}
	return t
}

func TestAuthService_Login(t *testing.T) {
	accessKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}
	refreshKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}
	const password = "pass"
	hashed := hashBcrypt(password)
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}

	now := time.Date(2022, 01, 01, 0, 0, 0, 0, time.UTC)
	jwt.TimeFunc = func() time.Time { return now.Add(1 * time.Second) }
	tokenStore := testsupport.TokenStoreFake{}
	authService := AuthService{
		Creds: CredStore{&userStoreMock{
			get: func(u types.UserID) (*types.UserEntry, error) {
				if u != "user" {
					return nil, types.ErrUserNotFound
				}
				return &types.UserEntry{
					User:         "user",
					PasswordHash: hashed,
				}, nil
			},
		}},
		Tokens: tokenStore,
		TokenDetails: TokenDetailsFactory{
			AccessTokens: TokenFactory{
				Issuer:        "issuer",
				Audience:      "*.example.org",
				TokenValidity: 15 * time.Minute,
				SigningKey:    accessKey,
			},
			RefreshTokens: TokenFactory{
				Issuer:        "issuer",
				Audience:      "*.example.org",
				TokenValidity: 7 * 24 * time.Hour,
				SigningKey:    refreshKey,
			},
			TimeFunc: func() time.Time { return now },
		},
	}

	tokens, err := authService.Login(&types.Credentials{
		User:     "user",
		Password: password,
	})

	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}

	claims, err := parseToken(tokens.AccessToken.Token, accessKey)
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}

	if wanted := (jwt.StandardClaims{
		ExpiresAt: now.Add(15 * time.Minute).Unix(),
		IssuedAt:  now.Unix(),
		NotBefore: now.Unix(),
		Issuer:    "issuer",
		Subject:   "user",
		Audience:  "*.example.org",
	}); wanted != *claims {
		t.Fatalf("Wanted:\n%# v\n\nFound:\n%# v", wanted, claims)
	}

	claims, err = parseToken(tokens.RefreshToken.Token, refreshKey)
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}

	if wanted := (jwt.StandardClaims{
		ExpiresAt: now.Add(7 * 24 * time.Hour).Unix(),
		IssuedAt:  now.Unix(),
		NotBefore: now.Unix(),
		Issuer:    "issuer",
		Subject:   "user",
		Audience:  "*.example.org",
	}); wanted != *claims {
		t.Fatalf("Wanted:\n%# v\n\nFound:\n%# v", wanted, claims)
	}

	// make sure the token was persisted
	entries, _ := tokenStore.List()
	if err := types.CompareTokens(
		[]types.Token{{
			Token:   tokens.RefreshToken.Token,
			Expires: tokens.RefreshToken.Expires,
		}},
		entries,
	); err != nil {
		t.Fatal(err)
	}

}

func parseToken(
	token string,
	key *ecdsa.PrivateKey,
) (*jwt.StandardClaims, error) {
	tok, err := jwt.ParseWithClaims(
		token,
		&jwt.StandardClaims{},
		func(*jwt.Token) (interface{}, error) { return &key.PublicKey, nil },
	)
	if err != nil {
		return nil, fmt.Errorf("parsing token: %w", err)
	}

	if tok.Method != jwt.SigningMethodES512 {
		return nil, fmt.Errorf(
			"wanted method '%v'; found '%v'",
			jwt.SigningMethodES512,
			tok.Method,
		)
	}

	if c, ok := tok.Claims.(*jwt.StandardClaims); ok {
		return c, nil
	}

	return nil, fmt.Errorf("invalid claims type: %T", tok.Claims)
}

type notificationServiceMock struct {
	notify func(*types.Notification) error
}

func (nsm *notificationServiceMock) Notify(rt *types.Notification) error {
	if nsm.notify == nil {
		panic("notificationServiceMock: missing `notify` hook")
	}
	return nsm.notify(rt)
}

func TestAuthService_Register(t *testing.T) {
	var (
		notifyCalledWithToken *types.Notification
		now                   = time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	)
	resetKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}
	authService := AuthService{
		Creds: CredStore{&userStoreMock{
			get: func(u types.UserID) (*types.UserEntry, error) {
				return nil, types.ErrUserNotFound
			},
		}},
		ResetTokens: ResetTokenFactory{
			Issuer:        "issuer",
			Audience:      "audience",
			TokenValidity: 5 * time.Minute,
			SigningKey:    resetKey,
		},
		Notifications: &notificationServiceMock{
			notify: func(n *types.Notification) error {
				notifyCalledWithToken = n
				return nil
			},
		},
		TimeFunc: func() time.Time { return now },
	}

	if err := authService.Register("user", "user@example.org"); err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}

	wantedToken := &types.Notification{
		Type:  types.NotificationTypeRegister,
		User:  "user",
		Email: "user@example.org",
		Token: notifyCalledWithToken.Token,
	}

	if err := wantedToken.Compare(notifyCalledWithToken); err != nil {
		t.Fatalf("NotificationService.Notify(*ResetToken): %v", err)
	}
}

func TestAuthService_Register_UserNameExists(t *testing.T) {
	authService := AuthService{
		Creds: CredStore{&userStoreMock{
			get: func(u types.UserID) (*types.UserEntry, error) {
				return &types.UserEntry{
					User:         u,
					Email:        "user@example.org",
					PasswordHash: hashBcrypt("password"),
				}, nil
			},
		}},
	}

	if err := authService.Register("user", "user@example.org"); err != nil {
		if !errors.Is(err, ErrUserExists) {
			t.Fatalf(
				"Wanted error '%s'; found '%s'",
				ErrUserExists.Error(),
				err.Error(),
			)
		}
		return
	}
	t.Fatal("Wanted `ErrUserExists`; found `<nil>`")
}

func TestAuthService_Register_InvalidEmailAddress(t *testing.T) {
	authService := AuthService{}
	for _, email := range []string{"", "nodomain@", "noatsign"} {
		if err := authService.Register("user", email); err != nil {
			if errors.Is(err, ErrInvalidEmail) {
				continue
			}
			t.Fatalf("Unexpected err: %v", err)
		}
		t.Fatal("Wanted `ErrInvalidEmail`; found `<nil>`")
	}
}

func TestAuthService_ForgotPassword(t *testing.T) {
	var (
		getCalledWithUser     types.UserID
		notifyCalledWithToken *types.Notification
		now                   = time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
		validity              = 5 * time.Minute
	)
	resetKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}
	authService := AuthService{
		Creds: CredStore{
			Users: &userStoreMock{
				get: func(u types.UserID) (*types.UserEntry, error) {
					getCalledWithUser = u
					return &types.UserEntry{
						User:  u,
						Email: "user@example.org",
					}, nil
				},
			},
		},
		Notifications: &notificationServiceMock{
			notify: func(n *types.Notification) error {
				notifyCalledWithToken = n
				return nil
			},
		},
		ResetTokens: ResetTokenFactory{
			Issuer:        "issuer",
			Audience:      "audience",
			TokenValidity: validity,
			SigningKey:    resetKey,
		},
		TimeFunc: func() time.Time { return now },
	}

	if err := authService.ForgotPassword("user"); err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}

	if getCalledWithUser != "user" {
		t.Fatalf(
			"UserStore.Get(): wanted 'user'; found '%s'",
			getCalledWithUser,
		)
	}

	wantedToken := &types.Notification{
		Type:  types.NotificationTypeForgotPassword,
		User:  "user",
		Email: "user@example.org",
		Token: notifyCalledWithToken.Token, // force a match on this field
	}

	if err := wantedToken.Compare(notifyCalledWithToken); err != nil {
		t.Fatalf("NotificationService.Notify(*ResetToken): %v", err)
	}
}

func TestAuthService_UpdatePassword(t *testing.T) {
	var (
		now      = time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
		password = "osakldflhkjewadfkjsfduIHUHKJGFU"
		entry    *types.UserEntry
	)
	resetKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}
	authService := AuthService{
		Creds: CredStore{
			Users: &userStoreMock{
				get: func(types.UserID) (*types.UserEntry, error) {
					return &types.UserEntry{
						User:         "user",
						Email:        "user@example.org",
						PasswordHash: hashBcrypt(password),
					}, nil
				},
				upsert: func(e *types.UserEntry) error { entry = e; return nil },
			},
		},
		Notifications: &notificationServiceMock{
			notify: func(n *types.Notification) error { return nil },
		},
		ResetTokens: ResetTokenFactory{
			Issuer:        "issuer",
			Audience:      "audience",
			TokenValidity: 5 * time.Minute,
			SigningKey:    resetKey,
		},
		TimeFunc: func() time.Time { return now },
	}

	tok, err := authService.ResetTokens.Create(now, "user", "user@example.org")
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}

	if err := authService.UpdatePassword(&UpdatePassword{
		User:     "user",
		Password: password,
		Token:    tok,
	}); err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}

	wantedCredentials := &types.Credentials{
		User:     "user",
		Email:    "user@example.org",
		Password: password,
	}

	if err := wantedCredentials.CompareUserEntry(entry); err != nil {
		t.Fatalf("UserStore.Upsert(*Credentials): %v", err)
	}
}

func TestAuthService_Logout(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		state        testsupport.TokenStoreFake
		refreshToken string
		wantedErr    types.WantedError
		wantedState  []types.Token
	}{
		{
			name:         "simple",
			state:        testsupport.TokenStoreFake{"token": now},
			refreshToken: "token",
		},
		{
			name:         "not found",
			state:        testsupport.TokenStoreFake{},
			refreshToken: "token",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.wantedErr == nil {
				testCase.wantedErr = types.NilError{}
			}
			if err := testCase.wantedErr.CompareErr(
				(*AuthService).Logout(
					&AuthService{Tokens: testCase.state},
					testCase.refreshToken,
				),
			); err != nil {
				t.Fatal(err)
			}

			found, _ := testCase.state.List()
			if err := types.CompareTokens(
				testCase.wantedState,
				found,
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func hashBcrypt(password string) []byte {
	hash, err := bcrypt.GenerateFromPassword(
		[]byte(password),
		bcrypt.DefaultCost,
	)
	if err != nil {
		panic(fmt.Sprintf("bcrypt-hashing password '%s': %v", password, err))
	}
	return hash
}

var goodPassword = ";oasdfipas#@#$OPYODF:;asdf"
