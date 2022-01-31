package client

import (
	"crypto/ecdsa"
	"errors"
	"net/http"
	"testing"

	pz "github.com/weberc2/httpeasy"
)

func TestAuthenticator_Auth(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		authType   AuthType
		wantedUser string
	}{
		{
			name: "success",
			authType: authTypeMock(
				func(_ *ecdsa.PublicKey, r pz.Request) *result {
					return resultOK("success", "user")
				},
			),
			wantedUser: "user",
		},
		{
			name: "failure",
			authType: authTypeMock(
				func(_ *ecdsa.PublicKey, r pz.Request) *result {
					return resultErr("ERR", errors.New("an error occurred"))
				},
			),
			wantedUser: "",
		},
	} {
		var user string
		new(Authenticator).Auth(
			testCase.authType,
			func(r pz.Request) pz.Response {
				user = r.Headers.Get("User")
				return pz.Ok(nil, nil)
			},
		)(pz.Request{Headers: http.Header{}})
		if user != testCase.wantedUser {
			t.Fatalf("wanted user `%s`; found `%s`", testCase.wantedUser, user)
		}
	}
}

type authTypeMock func(key *ecdsa.PublicKey, r pz.Request) *result

func (f authTypeMock) validate(key *ecdsa.PublicKey, r pz.Request) *result {
	return f(key, r)
}
