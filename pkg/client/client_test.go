package client

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/weberc2/auth/pkg/auth"
	"github.com/weberc2/auth/pkg/testsupport"
	"github.com/weberc2/auth/pkg/types"
	pz "github.com/weberc2/httpeasy"
	pztest "github.com/weberc2/httpeasy/testsupport"
	"golang.org/x/crypto/bcrypt"
)

func TestClient_Logout(t *testing.T) {
	jwt.TimeFunc = func() time.Time { return now }
	defer func() { jwt.TimeFunc = time.Now }()
	users := testsupport.UserStoreFake{
		"user": &types.UserEntry{
			User:  "user",
			Email: "user@example.org",
			PasswordHash: func() []byte {
				hash, err := bcrypt.GenerateFromPassword(
					[]byte("password"),
					bcrypt.DefaultCost,
				)
				if err != nil {
					t.Fatalf("unexpected error bcrypt-hashing password: %v", err)
				}
				return hash
			}(),
		},
	}
	authService, err := testAuthService(users)
	if err != nil {
		t.Fatalf("creating test `auth.AuthService`: %v", err)
	}
	tokens, err := authService.Login(&types.Credentials{
		User:     "user",
		Email:    "user@example.org",
		Password: "password",
	})
	if err != nil {
		t.Fatalf("unexpected error logging in: %v", err)
	}

	api := auth.AuthHTTPService{AuthService: authService}
	srv := httptest.NewServer(pz.Register(
		pztest.TestLog(t),
		api.RefreshRoute(),
		api.LogoutRoute(),
	))
	defer srv.Close()

	t.Logf("URL: %s", srv.URL)

	client := testClient(srv)

	// make sure we can refresh
	_, err = client.Refresh(tokens.RefreshToken.Token)
	if err != nil {
		t.Fatalf("unexpected error refreshing token: %v", err)
	}

	// logout; make sure there's no error
	if err := client.Logout(tokens.RefreshToken.Token); err != nil {
		t.Fatalf("logout error: expected `nil`; found `%v`", err)
	}

	// make sure we CANNOT refresh
	_, err = client.Refresh(tokens.RefreshToken.Token)
	if err := auth.ErrUnauthorized.CompareErr(err); err != nil {
		t.Fatal(err)
	}
}

func TestClient_Exchange(t *testing.T) {
	jwt.TimeFunc = func() time.Time { return now }
	defer func() { jwt.TimeFunc = time.Now }()

	authService, err := testAuthService(nil)
	if err != nil {
		t.Fatalf("creating test `auth.AuthService`: %v", err)
	}

	api := auth.AuthHTTPService{AuthService: authService}
	srv := httptest.NewServer(pz.Register(
		pztest.TestLog(t),
		api.ExchangeRoute(),
	))
	defer srv.Close()

	t.Logf("URL: %s", srv.URL)

	client := testClient(srv)

	code, err := authService.Codes.Create(now, "adam")
	if err != nil {
		t.Fatalf("unexpected error creating auth code: %v", err)
	}

	tokens, err := client.Exchange(code.Token)
	if err != nil {
		t.Fatalf("unexpected error exchanging auth code: %v", err)
	}

	if tokens.AccessToken.Token == "" {
		t.Fatal("missing access token")
	}

	if tokens.RefreshToken.Token == "" {
		t.Fatal("missing refresh token")
	}
}

func testClient(srv *httptest.Server) Client {
	httpClient := srv.Client()
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return Client{HTTP: *httpClient, BaseURL: srv.URL}
}

func testAuthService(
	users testsupport.UserStoreFake,
) (auth.AuthService, error) {
	authCodeKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		return auth.AuthService{}, fmt.Errorf(
			"unexpected error generating auth code key: %w",
			err,
		)
	}
	accessKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		return auth.AuthService{}, fmt.Errorf(
			"unexpected error generating access key: %w",
			err,
		)
	}
	refreshKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		return auth.AuthService{}, fmt.Errorf(
			"unexpected error generating refresh key: %w",
			err,
		)
	}

	codes := auth.TokenFactory{
		Issuer:        "issuer",
		Audience:      "audience",
		TokenValidity: time.Minute,
		SigningKey:    authCodeKey,
	}

	if users == nil {
		users = testsupport.UserStoreFake{}
	}

	return auth.AuthService{
		Tokens:        testsupport.TokenStoreFake{},
		Creds:         auth.CredStore{Users: users},
		Notifications: testsupport.NotificationServiceFake{},
		Codes:         codes,
		TokenDetails: auth.TokenDetailsFactory{
			AccessTokens: auth.TokenFactory{
				Issuer:        "issuer",
				Audience:      "audience",
				TokenValidity: 15 * time.Minute,
				SigningKey:    accessKey,
			},
			RefreshTokens: auth.TokenFactory{
				Issuer:        "issuer",
				Audience:      "audience",
				TokenValidity: 15 * time.Minute,
				SigningKey:    refreshKey,
			},
			TimeFunc: func() time.Time { return now },
		},
	}, nil
}

func nowFunc() time.Time { return now }

var (
	now = time.Date(1988, 8, 3, 0, 0, 0, 0, time.UTC)
)
