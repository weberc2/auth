package client

import (
	"crypto/ecdsa"
	"errors"
	"net/http"
	"testing"

	pz "github.com/weberc2/httpeasy"
)

func TestAuthenticator(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		method        func(*Authenticator, AuthType, pz.Handler) pz.Handler
		authResult    *Result
		wantedUser    string
		wantedInvoked bool
	}{
		{
			name:          "auth success",
			method:        (*Authenticator).Auth,
			authResult:    ResultOK("success", "user"),
			wantedUser:    "user",
			wantedInvoked: true,
		},
		{
			name:          "auth failure",
			method:        (*Authenticator).Auth,
			authResult:    ResultErr("ERR", errors.New("an error occurred")),
			wantedUser:    "",
			wantedInvoked: false,
		},
		{
			name:          "optional success",
			method:        (*Authenticator).Optional,
			authResult:    ResultOK("success", "user"),
			wantedUser:    "user",
			wantedInvoked: true,
		},
		{
			name:          "optional failure",
			method:        (*Authenticator).Optional,
			authResult:    ResultErr("ERR", errors.New("an error occurred")),
			wantedUser:    "",
			wantedInvoked: true,
		},
	} {
		var user string
		var invoked bool
		testCase.method(
			new(Authenticator),
			AuthTypeFunc(func(_ *ecdsa.PublicKey, r pz.Request) *Result {
				return testCase.authResult
			}),
			func(r pz.Request) pz.Response {
				user = r.Headers.Get("User")
				invoked = true
				return pz.Ok(nil, nil)
			},
		)(pz.Request{Headers: http.Header{}})
		if user != testCase.wantedUser {
			t.Fatalf("wanted user `%s`; found `%s`", testCase.wantedUser, user)
		}
		if invoked != testCase.wantedInvoked {
			t.Fatalf(
				"wanted user `%t`; found `%t`",
				testCase.wantedInvoked,
				invoked,
			)
		}
	}
}
