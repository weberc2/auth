package auth

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	html "html/template"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/weberc2/auth/pkg/auth/testsupport"
	"github.com/weberc2/auth/pkg/auth/types"
	pz "github.com/weberc2/httpeasy"
	pztest "github.com/weberc2/httpeasy/testsupport"
)

func TestWebServer_RegistrationConfirmationHandlerRoute(t *testing.T) {
	const defaultRedirectLocation = "https://app.example.org/index.html"
	for _, testCase := range []struct {
		name           string
		existingUsers  testsupport.UserStoreFake
		body           string
		wantedStatus   int
		wantedLocation string
		wantedUsers    []types.Credentials
	}{
		{
			name: "simple",
			body: confirmationForm(
				mustResetToken(now, "user", "user@example.org"),
				goodPassword,
			),
			wantedStatus:   http.StatusSeeOther,
			wantedLocation: defaultRedirectLocation,
			wantedUsers: []types.Credentials{{
				User:     "user",
				Email:    "user@example.org",
				Password: goodPassword,
			}},
		},
		{
			name:         "missing token",
			body:         confirmationForm("", goodPassword),
			wantedStatus: http.StatusBadRequest,
		},
		{
			name: "missing password",
			body: confirmationForm(
				mustResetToken(now, "user", "user@example.org"),
				"", // password
			),
			wantedStatus: http.StatusBadRequest,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.existingUsers == nil {
				testCase.existingUsers = testsupport.UserStoreFake{}
			}
			webServer := WebServer{
				AuthService: AuthService{
					Creds:       CredStore{testCase.existingUsers},
					Tokens:      testsupport.TokenStoreFake{},
					ResetTokens: resetTokenFactory,
					TokenDetails: TokenDetailsFactory{
						AccessTokens:  accessTokenFactory,
						RefreshTokens: refreshTokenFactory,
						TimeFunc:      func() time.Time { return now },
					},
					Codes:    codesTokenFactory,
					TimeFunc: func() time.Time { return now },
				},
				BaseURL:                 "https://auth.example.org",
				RedirectDomain:          "https://app.example.org",
				DefaultRedirectLocation: defaultRedirectLocation,
			}

			rsp := webServer.RegistrationConfirmationHandlerRoute().Handler(
				pz.Request{
					Body: strings.NewReader(testCase.body),
				},
			)

			if rsp.Status != testCase.wantedStatus {
				data, err := json.Marshal(rsp.Logging)
				if err != nil {
					t.Logf("marshaling response logging: %v", err)
				}
				t.Logf("LOGS: %s", data)
				t.Fatalf(
					"Response.Status: wanted `%d`; found `%d`",
					testCase.wantedStatus,
					rsp.Status,
				)
			}

			if l := rsp.Headers.Get("Location"); l != testCase.wantedLocation {
				t.Fatalf(
					"Response.Headers[\"Location\"]: wanted `%s`; found `%s`",
					testCase.wantedLocation,
					l,
				)
			}

			found := testCase.existingUsers.List()
			if len(found) != len(testCase.wantedUsers) {
				t.Fatalf(
					"len(UserStore): wanted `%d`; found `%d`",
					len(testCase.wantedUsers),
					len(found),
				)
			}

			for i, creds := range testCase.wantedUsers {
				if err := creds.CompareUserEntry(found[i]); err != nil {
					t.Fatalf("UserStore[%d]: %v", i, err)
				}
			}
		})
	}
}

func TestWebServer_RegistrationHandlerRoute(t *testing.T) {
	for _, testCase := range []struct {
		name                string
		existingUsers       testsupport.UserStoreFake
		body                string
		wantedStatus        int
		wantedData          pztest.WantedData
		wantedNotifications []*types.Notification
	}{
		{
			name:         "simple",
			body:         regForm("user", "user@example.org"),
			wantedStatus: http.StatusCreated,
			wantedData:   wantedString(registrationSuccessPage),
			wantedNotifications: []*types.Notification{{
				Type:  types.NotificationTypeRegister,
				User:  "user",
				Email: "user@example.org",
				Token: func() string {
					tok, err := resetTokenFactory.Create(
						now,
						"user",
						"user@example.org",
					)
					if err != nil {
						t.Fatalf(
							"unexpected error creating reset token: %v",
							err,
						)
					}
					return tok
				}(),
			}},
		},
		{
			name:         "invalid form data",
			body:         ";", // invalid form data
			wantedStatus: http.StatusBadRequest,
			wantedData:   ErrParsingFormData,
		},
		{
			name: "username exists",
			existingUsers: testsupport.UserStoreFake{
				"user": {
					User:  "user",
					Email: "user@example.org",
				},
			},
			body:         regForm("user", "user@example.org"),
			wantedStatus: http.StatusConflict,
			wantedData: &wantedTemplate{
				tmpl: registrationForm,
				values: registrationFormContext{
					FormAction:   pathRegistrationForm,
					ErrorMessage: ErrUserExists.Message,
				},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			notificationService := testsupport.NotificationServiceFake{}
			if testCase.existingUsers == nil {
				testCase.existingUsers = testsupport.UserStoreFake{}
			}
			webServer := WebServer{
				AuthService: AuthService{
					Creds:       CredStore{testCase.existingUsers},
					Tokens:      testsupport.TokenStoreFake{},
					ResetTokens: resetTokenFactory,
					TokenDetails: TokenDetailsFactory{
						AccessTokens:  accessTokenFactory,
						RefreshTokens: refreshTokenFactory,
						TimeFunc:      func() time.Time { return now },
					},
					Codes:         codesTokenFactory,
					TimeFunc:      func() time.Time { return now },
					Notifications: &notificationService,
				},
				BaseURL:                 "https://auth.example.org",
				RedirectDomain:          "https://app.example.org",
				DefaultRedirectLocation: "https://app.example.org/index.html",
			}
			rsp := webServer.RegistrationHandlerRoute().Handler(pz.Request{
				Body: strings.NewReader(testCase.body),
			})
			if rsp.Status != testCase.wantedStatus {
				t.Fatalf(
					"Response.Status: wanted `%d`; found `%d`",
					testCase.wantedStatus,
					rsp.Status,
				)
			}

			if err := pztest.CompareSerializer(
				testCase.wantedData,
				rsp.Data,
			); err != nil {
				t.Fatal(err)
			}

			wanted := testCase.wantedNotifications
			found := notificationService.Notifications
			if len(wanted) != len(found) {
				t.Fatalf(
					"sent notifications: length: wanted `%d`; found `%d`",
					len(wanted),
					len(found),
				)
			}

			for i := range wanted {
				if wanted[i].Email != found[i].Email {
					t.Fatalf(
						"Notifications[%d].Email: wanted `%s`; found `%s`",
						i,
						wanted[i].Email,
						found[i].Email,
					)
				}

				if wanted[i].Type != found[i].Type {
					t.Fatalf(
						"Notifications[%d].Type: wanted `%s`; found `%s`",
						i,
						wanted[i].Type,
						found[i].Type,
					)
				}

				if wanted[i].User != found[i].User {
					t.Fatalf(
						"Notifications[%d].User: wanted `%s`; found `%s`",
						i,
						wanted[i].User,
						found[i].User,
					)
				}

				wantedClaims, err := parseClaims(wanted[i].Token)
				if err != nil {
					t.Fatalf(
						"Notifications[%d].Token: parsing wanted token: %v",
						i,
						err,
					)
				}

				foundClaims, err := parseClaims(found[i].Token)
				if err != nil {
					t.Fatalf(
						"Notifications[%d].Token: parsing found token: %v",
						i,
						err,
					)
				}

				if *wantedClaims != *foundClaims {
					wanted, err := json.Marshal(wantedClaims)
					if err != nil {
						t.Fatalf(
							"marshaling wanted[%d]'s token claims: %v",
							i,
							err,
						)
					}
					found, err := json.Marshal(foundClaims)
					if err != nil {
						t.Fatalf(
							"marshaling found[%d]'s token claims: %v",
							i,
							err,
						)
					}
					t.Fatalf(
						"Notifications[%d].Token: wanted `%s`; found `%s`",
						i,
						wanted,
						found,
					)
				}
			}
		})
	}
}

func regForm(username, email string) string {
	return url.Values{
		"username": []string{username},
		"email":    []string{email},
	}.Encode()
}

func confirmationForm(token, password string) string {
	return url.Values{
		"token":    []string{token},
		"password": []string{password},
	}.Encode()
}

func parseClaims(tok string) (*Claims, error) {
	var claims Claims
	if _, err := jwt.ParseWithClaims(
		tok,
		&claims,
		func(*jwt.Token) (interface{}, error) {
			return &resetTokenFactory.SigningKey.PublicKey, nil
		},
	); err != nil {
		return nil, fmt.Errorf("parsing token: %w", err)
	}
	return &claims, nil
}

func TestWebServer_LoginHandler(t *testing.T) {
	codes := TokenFactory{
		Issuer:        "issuer",
		Audience:      "audience",
		TokenValidity: time.Minute,
		SigningKey:    codesSigningKey,
	}

	for _, testCase := range []struct {
		name           string
		username       string
		password       string
		callback       string
		redirect       string
		stateUsers     testsupport.UserStoreFake
		wantedStatus   int
		wantedLocation wantedLocation
	}{{
		name:     "redirects with auth code",
		username: "adam",
		password: "password",
		callback: "https://app.example.org/auth/callback",
		redirect: "https://app.example.org/users/adam/settings",
		stateUsers: testsupport.UserStoreFake{
			"adam": {
				User:         "adam",
				Email:        "adam@example.org",
				PasswordHash: hashBcrypt("password"),
			},
		},
		wantedStatus: http.StatusSeeOther,
		wantedLocation: wantedLocation{
			key:      &codesSigningKey.PublicKey,
			scheme:   "https",
			host:     "app.example.org",
			path:     "/auth/callback",
			callback: "https://app.example.org/auth/callback",
			redirect: "https://app.example.org/users/adam/settings",
		},
	}} {
		jwt.TimeFunc = nowTimeFunc
		defer func() { jwt.TimeFunc = time.Now }()
		webServer := WebServer{
			AuthService: AuthService{
				Creds: CredStore{
					Users: testCase.stateUsers,
				},
				Notifications: &testsupport.NotificationServiceFake{},
				Codes:         codes,
				TimeFunc:      nowTimeFunc,
			},
			BaseURL:                 "https://auth.example.org",
			RedirectDomain:          "app.example.org",
			DefaultRedirectLocation: "https://app.example.org/default/",
		}

		rsp := webServer.LoginHandler(pz.Request{
			Body: strings.NewReader(
				url.Values{
					"username": []string{testCase.username},
					"password": []string{testCase.password},
				}.Encode(),
			),
			URL: &url.URL{
				RawQuery: url.Values{
					"redirect": []string{testCase.redirect},
					"callback": []string{testCase.callback},
				}.Encode(),
			},
		})

		if rsp.Status != testCase.wantedStatus {
			data, err := json.Marshal(rsp.Logging)
			if err != nil {
				t.Logf("marshaling response logs: %v", err)
			}
			t.Logf("response logs: %s", data)
			t.Fatalf(
				"Response.Status: wanted `%d`; found `%d`",
				testCase.wantedStatus,
				rsp.Status,
			)
		}

		if err := testCase.wantedLocation.compare(
			rsp.Headers.Get("Location"),
		); err != nil {
			t.Fatalf("Response.Headers[\"Location\"]: %v", err)
		}
	}
}

type wantedLocation struct {
	key      *ecdsa.PublicKey
	scheme   string
	host     string
	path     string
	callback string
	redirect string
}

func (wanted *wantedLocation) compare(found string) error {
	url, err := url.Parse(found)
	if err != nil {
		return fmt.Errorf("parsing `found`: %w", err)
	}

	if wanted.scheme != url.Scheme {
		return fmt.Errorf(
			"URL.Scheme: wanted `%s`; found `%s`",
			wanted.scheme,
			url.Scheme,
		)
	}

	if wanted.host != url.Host {
		return fmt.Errorf(
			"URL.Host: wanted `%s`; found `%s`",
			wanted.host,
			url.Host,
		)
	}

	if wanted.path != url.Path {
		return fmt.Errorf(
			"URL.Path: wanted `%s`; found `%s`",
			wanted.path,
			url.Path,
		)
	}

	query := url.Query()
	if len(query) != 3 {
		return fmt.Errorf("URL.Query: wanted `3` keys; found `%d`", len(query))
	}

	if wanted.callback != query.Get("callback") {
		return fmt.Errorf(
			"URL.Query[\"callback\"]: wanted `%s`; found `%s`",
			wanted.callback,
			query.Get("callback"),
		)
	}

	if wanted.redirect != query.Get("redirect") {
		return fmt.Errorf(
			"URL.Query[\"redirect\"]: wanted `%s`; found `%s`",
			wanted.redirect,
			query.Get("redirect"),
		)
	}

	if _, err := jwt.Parse(
		query.Get("code"),
		func(*jwt.Token) (interface{}, error) {
			return wanted.key, nil
		},
	); err != nil {
		return fmt.Errorf("URL.Query[\"code\"]: parsing JWT: %w", err)
	}

	return nil
}

type wantedString string

func (ws wantedString) CompareData(data []byte) error {
	if ws != wantedString(data) {
		return fmt.Errorf("wanted `%s`; found `%s`", ws, data)
	}
	return nil
}

func (ws wantedString) CompareErr(err error) error {
	if err == nil || err.Error() != string(ws) {
		return fmt.Errorf("wanted `%s`; found `%v`", ws, err)
	}
	return nil
}

type wantedTemplate struct {
	tmpl   *html.Template
	values interface{}
}

func (wt *wantedTemplate) CompareData(data []byte) error {
	var buf bytes.Buffer
	if err := wt.tmpl.Execute(&buf, wt.values); err != nil {
		return fmt.Errorf("executing HTML template: %w", err)
	}

	if wanted, found := buf.String(), string(data); wanted != found {
		return fmt.Errorf("wanted `%s`; found `%s`", wanted, found)
	}

	return nil
}
