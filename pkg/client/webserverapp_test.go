package client

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/weberc2/auth/pkg/auth"
	"github.com/weberc2/auth/pkg/testsupport"
	"github.com/weberc2/auth/pkg/types"
	pz "github.com/weberc2/httpeasy"
	pztest "github.com/weberc2/httpeasy/testsupport"
)

func TestWebServerApp_Logout(t *testing.T) {
	for _, testCase := range []struct {
		name            string
		tokenStore      testsupport.TokenStoreFake
		redirectDefault string
		referer         string
		cookies         map[string]string
		wantedStatus    int
		wantedLocation  string
		wantedTokens    bool
	}{
		{
			name:            "simple",
			tokenStore:      testsupport.TokenStoreFake{"refresh-token": now},
			redirectDefault: "default",
			cookies: map[string]string{
				"Refresh-Token": "refresh-token",
			},
			wantedStatus:   http.StatusSeeOther,
			wantedLocation: "default",
			wantedTokens:   false,
		},
		{
			// If there's no token cookie, then there's nothing to do, so just
			// redirect.
			name:            "token cookie not found",
			tokenStore:      testsupport.TokenStoreFake{},
			redirectDefault: "default",
			cookies:         map[string]string{},
			wantedStatus:    http.StatusSeeOther,
			wantedLocation:  "default",
			wantedTokens:    false,
		},
		{
			// If the provided token isn't found in the token store, then
			// there's nothing to do, so unset the token cookie and redirect.
			name:            "token not found",
			tokenStore:      testsupport.TokenStoreFake{},
			redirectDefault: "default",
			cookies: map[string]string{
				"Refresh-Token": "refresh-token",
			},
			wantedStatus:   http.StatusSeeOther,
			wantedLocation: "default",
			wantedTokens:   false,
		},
		{
			name:            "redirect",
			tokenStore:      testsupport.TokenStoreFake{"redirect-token": now},
			redirectDefault: "default",
			referer:         "redirect",
			cookies: map[string]string{
				"Refresh-Token": "refresh-token",
			},
			wantedStatus:   http.StatusSeeOther,
			wantedLocation: "redirect",
			wantedTokens:   false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			app := WebServerApp{
				Client: testClient(testAuthServer(
					t,
					&authServiceOptions{
						tokenStore: testCase.tokenStore,
					},
				)),
				DefaultRedirect: testCase.redirectDefault,
				Key:             "cookie-encryption-key",
			}
			var (
				err    error
				appSrv = testServer(t, app.LogoutRoute("/auth/logout"))
			)
			app.BaseURL, err = url.Parse(appSrv.URL)
			if err != nil {
				t.Fatalf("unexpected error parsing webserver app url: %v", err)
			}
			appClient := testHTTPClient(appSrv)

			url := fmt.Sprintf("%s/auth/logout", appSrv.URL)
			t.Logf("GET %s", url)

			req, err := http.NewRequest("GET", url, strings.NewReader(""))
			if err != nil {
				t.Fatalf("unexpected error building request: %v", err)
			}
			if testCase.referer != "" {
				referer := fmt.Sprintf("%s/%s", appSrv.URL, testCase.referer)
				t.Logf("Adding `Referer` header: %s", referer)
				req.Header.Add("Referer", referer)
			}

			cookieDomain := join(app.BaseURL, "auth/logout")
			for cookieName, cookieValue := range testCase.cookies {
				req.AddCookie(cookie(cookieName, cookieDomain, cookieValue))
			}

			rsp, err := appClient.Do(req)
			if err != nil {
				t.Fatalf(
					"unexpected error communicating with app server: %v",
					err,
				)
			}

			if rsp.StatusCode != testCase.wantedStatus {
				t.Fatalf(
					"Response.StatusCode: wanted `%d`; found `%d`",
					testCase.wantedStatus,
					rsp.StatusCode,
				)
			}

			var wanted string
			if testCase.wantedLocation != "" {
				wanted = fmt.Sprintf(
					"%s/%s",
					appSrv.URL,
					testCase.wantedLocation,
				)
			}
			found := rsp.Header.Get("Location")
			if wanted != found {
				t.Fatalf(
					"Response.Header[\"Location\"]: wanted `%s`; found `%s`",
					wanted,
					found,
				)
			}

			cookies := rsp.Cookies()
			var accessToken, refreshToken string
			for _, cookie := range cookies {
				if cookie.Name == "Access-Token" {
					accessToken = cookie.Value
				} else if cookie.Name == "Refresh-Token" {
					refreshToken = cookie.Value
				}
			}

			if testCase.wantedTokens &&
				(accessToken == "" || refreshToken == "") {
				t.Logf("Access-Token: %s", accessToken)
				t.Logf("Refresh-Token: %s", refreshToken)
				t.Fatal("wanted tokens, but at least one is empty")
			}
			if !testCase.wantedTokens &&
				(accessToken != "" || refreshToken != "") {
				t.Logf("Access-Token: %s", accessToken)
				t.Logf("Refresh-Token: %s", refreshToken)
				t.Fatal("didn't want tokens, but at least one is set")
			}
		})
	}
}

func TestWebServerApp_AuthCodeCallback(t *testing.T) {
	var (
		now             = time.Date(2000, 9, 15, 0, 0, 0, 0, time.UTC)
		issuer          = "issuer"
		subject         = "user"
		audience        = "audience"
		authCodeKey     = mustP521Key()
		authCodeFactory = auth.TokenFactory{
			Issuer:        issuer,
			Audience:      audience,
			TokenValidity: time.Minute,
			SigningKey:    authCodeKey,
		}
		codeToken = func() *types.Token {
			tok, err := authCodeFactory.Create(now, subject)
			if err != nil {
				t.Fatalf("creating auth code token: %v", err)
			}
			return tok
		}()
	)

	jwt.TimeFunc = func() time.Time { return now }
	defer func() { jwt.TimeFunc = time.Now }()

	for _, testCase := range []struct {
		name            string
		params          map[string]string
		redirectDefault string
		baseURL         string
		wantedStatus    int
		wantedLocation  string
		wantedTokens    bool
	}{
		{
			name: "simple",
			params: map[string]string{
				"redirect": "intended",
				"code":     codeToken.Token,
			},
			redirectDefault: "default",
			wantedStatus:    http.StatusSeeOther,
			wantedTokens:    true,
			wantedLocation:  "intended",
		},
		{
			name:            "empty redirect param",
			params:          map[string]string{"code": codeToken.Token},
			redirectDefault: "default",
			wantedStatus:    http.StatusSeeOther,
			wantedTokens:    true,
			wantedLocation:  "default",
		},
		{
			name: "invalid redirect param",
			params: map[string]string{
				"redirect": "\n",
				"code":     codeToken.Token,
			},
			redirectDefault: "default",
			wantedStatus:    http.StatusSeeOther,
			wantedTokens:    true,
			wantedLocation:  "default",
		},
		{
			name:            "missing code param",
			params:          map[string]string{"redirect": "intended"},
			redirectDefault: "default",
			wantedStatus:    http.StatusBadRequest,
			wantedTokens:    false,
			wantedLocation:  "",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			app := WebServerApp{
				Client: testClient(testAuthServer(
					t,
					&authServiceOptions{
						authCodeFactory: &authCodeFactory,
					},
				)),
				DefaultRedirect: testCase.redirectDefault,
				Key:             "cookie-encryption-key",
			}

			var (
				err    error
				appSrv = testServer(
					t,
					app.AuthCodeCallbackRoute("/api/auth/code"),
				)
			)
			app.BaseURL, err = url.Parse(appSrv.URL)
			if err != nil {
				t.Fatalf("unexpected error parsing webserver app url: %v", err)
			}

			appClient := testHTTPClient(appSrv)

			values := url.Values{}
			for key, value := range testCase.params {
				values.Add(key, value)
			}

			url := fmt.Sprintf(
				"%s/api/auth/code?%s",
				appSrv.URL,
				values.Encode(),
			)
			t.Logf("GET %s", url)

			rsp, err := appClient.Get(url)
			if err != nil {
				t.Fatalf(
					"unexpected error communicating with app server: %v",
					err,
				)
			}

			if rsp.StatusCode != testCase.wantedStatus {
				t.Fatalf(
					"Response.StatusCode: wanted `%d`; found `%d`",
					testCase.wantedStatus,
					rsp.StatusCode,
				)
			}

			var wanted string
			if testCase.wantedLocation != "" {
				wanted = fmt.Sprintf(
					"%s/%s",
					appSrv.URL,
					testCase.wantedLocation,
				)
			}
			found := rsp.Header.Get("Location")
			if wanted != found {
				t.Fatalf(
					"Response.Header[\"Location\"]: wanted `%s`; found `%s`",
					wanted,
					found,
				)
			}

			cookies := rsp.Cookies()
			var accessToken, refreshToken string
			for _, cookie := range cookies {
				if cookie.Name == "Access-Token" {
					accessToken = cookie.Value
				} else if cookie.Name == "Refresh-Token" {
					refreshToken = cookie.Value
				}
			}

			if testCase.wantedTokens &&
				(accessToken == "" || refreshToken == "") {
				t.Logf("Access-Token: %s", accessToken)
				t.Logf("Refresh-Token: %s", refreshToken)
				t.Fatal("wanted tokens, but at least one is empty")
			}
			if !testCase.wantedTokens &&
				(accessToken != "" || refreshToken != "") {
				t.Logf("Access-Token: %s", accessToken)
				t.Logf("Refresh-Token: %s", refreshToken)
				t.Fatal("didn't want tokens, but at least one is set")
			}
		})
	}
}

func testAuthServer(
	t *testing.T,
	options *authServiceOptions,
) *httptest.Server {
	authService, err := testAuthService(options)
	if err != nil {
		t.Fatalf("unexpected error creating test auth service: %v", err)
	}

	authHTTPService := auth.AuthHTTPService{AuthService: authService}

	return testServer(
		t,
		authHTTPService.ExchangeRoute(),
		authHTTPService.LogoutRoute(),
	)
}

func testServer(t *testing.T, routes ...pz.Route) *httptest.Server {
	return httptest.NewServer(pz.Register(
		pztest.TestLog(t),
		routes...,
	))
}

func testHTTPClient(s *httptest.Server) *http.Client {
	client := s.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return client
}
