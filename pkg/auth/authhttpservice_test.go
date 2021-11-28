package auth

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/weberc2/auth/pkg/types"
	pz "github.com/weberc2/httpeasy"
	pztest "github.com/weberc2/httpeasy/testsupport"
)

func TestAuthHTTPService(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		input          string
		route          func(*AuthHTTPService) pz.Route
		validationTime time.Time
		existingUsers  []types.UserEntry
		wantedStatus   int
		wantedPayload  pztest.WantedData
	}{
		{
			name:  "forgot password",
			input: `{"user": "user"}`,
			route: (*AuthHTTPService).ForgotPasswordRoute,
			existingUsers: []types.UserEntry{{
				User:         "user",
				Email:        "user@example.org",
				PasswordHash: hashBcrypt("password"),
			}},
			wantedStatus:  200,
			wantedPayload: Any{},
		},
		{
			// Still want to return 200 when user isn't found to avoid leaking
			// details to potential attackers.
			name:          "forgot password: user not found",
			input:         `{"user": "user"}`,
			route:         (*AuthHTTPService).ForgotPasswordRoute,
			existingUsers: nil,
			wantedStatus:  200,
			wantedPayload: Any{},
		},
		{
			// Expect tokens are returned when a valid refresh token is
			// provided.
			name:           "refresh",
			input:          fmt.Sprintf(`{"refreshToken": "%s"}`, refreshToken),
			route:          (*AuthHTTPService).RefreshRoute,
			validationTime: now.Add(2 * time.Second),
			wantedStatus:   200,
			wantedPayload:  &refresh{AccessToken: accessToken},
		},
		{
			// Expect an error when an invalid refresh token is provided. The
			// same generic `invalid token` error is used regardless of the
			// nature of the error to avoid leaking information to potential
			// attackers.
			name:           "refresh: invalid token",
			input:          `{"refreshToken": "foobar"}`,
			route:          (*AuthHTTPService).RefreshRoute,
			validationTime: now.Add(2 * time.Second),
			wantedStatus:   401,
			wantedPayload:  ErrInvalidRefreshToken,
		},
		{
			// Expect an error when an expired refresh token is provided. The
			// same generic `invalid token` error is used regardless of the
			// nature of the error to avoid leaking information to potential
			// attackers.
			name:           "refresh: expired token",
			input:          fmt.Sprintf(`{"refreshToken": "%s"}`, refreshToken),
			route:          (*AuthHTTPService).RefreshRoute,
			validationTime: now.Add(30 * 24 * time.Hour),
			wantedStatus:   401,
			wantedPayload:  ErrInvalidRefreshToken,
		},
		{
			// Expect tokens returned in exchange for a valid auth code.
			name:           "exchange auth code",
			input:          fmt.Sprintf(`{"code": "%s"}`, authCode),
			route:          (*AuthHTTPService).ExchangeRoute,
			validationTime: now,
			wantedStatus:   200,
			wantedPayload: &TokenDetails{
				AccessToken:  accessToken,
				RefreshToken: refreshToken,
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			vtime := now
			if testCase.validationTime != (time.Time{}) {
				vtime = testCase.validationTime
			}
			jwt.TimeFunc = func() time.Time { return vtime }
			defer func() { jwt.TimeFunc = time.Now }()
			var notifications []*types.Notification
			service := AuthHTTPService{
				AuthService: AuthService{
					Creds: CredStore{
						Users: &userStoreMock{
							get: func(
								user types.UserID,
							) (*types.UserEntry, error) {
								for i, entry := range testCase.existingUsers {
									if entry.User == user {
										return &testCase.existingUsers[i], nil
									}
								}
								return nil, types.ErrUserNotFound
							},
						},
					},
					Notifications: &notificationServiceMock{
						notify: func(n *types.Notification) error {
							notifications = append(notifications, n)
							return nil
						},
					},
					Codes:       codesTokenFactory,
					ResetTokens: resetTokenFactory,
					TokenDetails: TokenDetailsFactory{
						AccessTokens:  accessTokenFactory,
						RefreshTokens: refreshTokenFactory,
						TimeFunc:      nowTimeFunc,
					},
					TimeFunc: nowTimeFunc,
				},
			}

			rsp := testCase.route(&service).Handler(pz.Request{
				Body: strings.NewReader(testCase.input),
			})

			if rsp.Status != testCase.wantedStatus {
				data, err := json.Marshal(rsp.Logging)
				if err != nil {
					t.Logf("failed to marshal handler logs: %v", err)
				}
				t.Logf("request logs: %s", data)
				t.Fatalf(
					"wanted `%d`; found `%d`",
					testCase.wantedStatus,
					rsp.Status,
				)
			}

			if err := pztest.CompareSerializer(
				testCase.wantedPayload,
				rsp.Data,
			); err != nil {
				if err, ok := err.(*pztest.CompareSerializerError); ok {
					t.Logf("response data: %s", err.Data)
				}
				t.Fatal(err)
			}
		})
	}
}

const (
	accessSigningKeyString = `-----BEGIN PRIVATE KEY-----
MIHcAgEBBEIBb4gjfi9dZnm6jypDJ1/44jUYYPaAizXv7QQPG14aj9W1pwoULDuM
ni71Zi68U8NJhB/dfHgvviK8a8289lysux+gBwYFK4EEACOhgYkDgYYABACD5lbL
9RtF/WKFyUpn8FBJ1QZHvsxcfgpSlvGPyJa3pP9NbofkFL5Xuh9Yd5oFp40xQhJv
f9MBqFs4XHv363V+egB5HQFk0oQeiwl8kNfCgTsZzM4CMytyVQZty2zM9CKXG5m7
EjWmjtDDCSEnLodzVVtL89VNxPI97T4P5QFolAMezg==
-----END PRIVATE KEY-----`
	refreshSigningKeyString = `-----BEGIN PRIVATE KEY-----
MIHcAgEBBEIANg/VI7PQKRnNeBz4WKfQWFrQUOfuelQeNMTh9ItWpCKqHB5yb5ba
DMJo4lEXjtduf/vvjPNqWurHGuEAW3aM3n+gBwYFK4EEACOhgYkDgYYABAGidC1I
tlhV5Xgs4xb+co5TI2YIA2huX47u18zZNs8wCmGxwPZ6fQlZW5SCekdNS4K6rocr
TkOM9C1EWEA18dyYngDcIurK/D5Pia3FaorX14KMxduUafX/hhOmWChBrIcK3FWW
gpjZ21DFCBpFh83l3tCrfD+yDXElY9EAg8Xur3vSfg==
-----END PRIVATE KEY-----`
	resetSigningKeyString = `-----BEGIN PRIVATE KEY-----
MIHcAgEBBEIBteoGMRxbAQSI2z9nhD/GBcMVfecuyG58swlqZZDRQ8aUTcmaL371
+9cSBTI6AFNRWl6Fh0/kD4Kyg8UR+4R8fdWgBwYFK4EEACOhgYkDgYYABAEuc5pj
bi3AWn/XJ8xxVn8cDuvnqXEWec+/oiFkJkvlqe0YTA/mz/lmoIgQget6nMVAXUa0
C0Gwvg5hxJ6EF7+ZWwFLFgcyCWW2tezZyNqi7BBW6dAlRGOun6VrldPAJFW96cl8
i5q05kD3gwd3T6OmOv0gCoVYvDhHwZLNuVOUHYVUjg==
-----END PRIVATE KEY-----`
)

func nowTimeFunc() time.Time { return now }

var (
	now                = time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	user               = types.UserID("user")
	accessSigningKey   = mustParseKey(accessSigningKeyString)
	refreshSigningKey  = mustParseKey(refreshSigningKeyString)
	resetSigningKey    = mustParseKey(resetSigningKeyString)
	codesSigningKey    = []byte("codes-signing-key")
	accessTokenFactory = TokenFactory{
		Issuer:        "issuer",
		Audience:      "audience",
		TokenValidity: 15 * time.Minute,
		ParseKey:      &accessSigningKey.PublicKey,
		SigningKey:    accessSigningKey,
		SigningMethod: jwt.SigningMethodES512,
	}
	refreshTokenFactory = TokenFactory{
		Issuer:        "issuer",
		Audience:      "audience",
		TokenValidity: 7 * 24 * time.Hour,
		ParseKey:      &refreshSigningKey.PublicKey,
		SigningKey:    refreshSigningKey,
		SigningMethod: jwt.SigningMethodES512,
	}
	resetTokenFactory = ResetTokenFactory{
		Issuer:        "issuer",
		Audience:      "audience",
		TokenValidity: 1 * time.Hour,
		ParseKey:      &resetSigningKey.PublicKey,
		SigningKey:    resetSigningKey,
		SigningMethod: jwt.SigningMethodES512,
	}
	codesTokenFactory = TokenFactory{
		Issuer:        "issuer",
		Audience:      "audience",
		TokenValidity: time.Minute,
		ParseKey:      codesSigningKey,
		SigningKey:    codesSigningKey,
		SigningMethod: jwt.SigningMethodHS512,
	}
	accessToken  = must(accessTokenFactory.Create(now, string(user)))
	refreshToken = must(refreshTokenFactory.Create(now, string(user)))
	authCode     = must(codesTokenFactory.Create(now, string(user)))
)

func mustParseKey(keyString string) *ecdsa.PrivateKey {
	block, _ := pem.Decode([]byte(keyString))

	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		log.Fatalf("parsing x509 EC private key: %v", err)
	}

	return key
}

func must(s string, err error) string {
	if err != nil {
		panic(err)
	}
	return s
}

type Wanted interface {
	Compare(data []byte) error
}

func compareTokens(key *ecdsa.PublicKey, wanted, found string) error {
	var wantedClaims Claims
	if _, err := jwt.ParseWithClaims(
		wanted,
		&wantedClaims,
		func(*jwt.Token) (interface{}, error) { return key, nil },
	); err != nil {
		return fmt.Errorf("parsing 'wanted' token: %w", err)
	}

	var foundClaims Claims
	if _, err := jwt.ParseWithClaims(
		found,
		&foundClaims,
		func(*jwt.Token) (interface{}, error) { return key, nil },
	); err != nil {
		return fmt.Errorf("parsing 'found' token: %w", err)
	}

	if wantedClaims != foundClaims {
		wanted, err := json.Marshal(wantedClaims)
		if err != nil {
			return fmt.Errorf("marshaling 'wanted' claims: %w", err)
		}
		found, err := json.Marshal(foundClaims)
		if err != nil {
			return fmt.Errorf("marshaling 'found' claims: %w", err)
		}
		return fmt.Errorf("wanted `%s`; found `%s`", wanted, found)
	}

	return nil
}

func (wanted *refresh) CompareData(data []byte) error {
	var found refresh
	if err := json.Unmarshal(data, &found); err != nil {
		return fmt.Errorf("unmarshaling `refresh`: %w", err)
	}

	if err := compareTokens(
		&accessSigningKey.PublicKey,
		wanted.AccessToken,
		found.AccessToken,
	); err != nil {
		return fmt.Errorf("comparing access tokens: %w", err)
	}

	return nil
}

func (wanted *TokenDetails) CompareData(data []byte) error {
	var found TokenDetails
	if err := json.Unmarshal(data, &found); err != nil {
		return fmt.Errorf(
			"TokenDetails.Compare(): unmarshaling `TokenDetails`: %w",
			err,
		)
	}

	if err := compareTokens(
		&accessSigningKey.PublicKey,
		wanted.AccessToken,
		found.AccessToken,
	); err != nil {
		log.Printf(
			"access token: wanted `%s`; found `%s`",
			wanted.AccessToken,
			found.AccessToken,
		)
		return fmt.Errorf("TokenDetails.Compare(): AccessToken: %w", err)
	}

	if err := compareTokens(
		&refreshSigningKey.PublicKey,
		wanted.RefreshToken,
		found.RefreshToken,
	); err != nil {
		log.Printf(
			"refresh token: wanted `%s`; found `%s`",
			wanted.RefreshToken,
			found.RefreshToken,
		)
		return fmt.Errorf("TokenDetails.Compare(): RefreshToken: %w", err)
	}
	return nil
}

type Any struct{}

func (Any) CompareData(data []byte) error { return nil }

func readAll(s pz.Serializer) ([]byte, error) {
	writerTo, err := s()
	if err != nil {
		return nil, fmt.Errorf("executing serializer: %w", err)
	}

	var buf bytes.Buffer
	if _, err := writerTo.WriteTo(&buf); err != nil {
		return nil, fmt.Errorf("buffering response body: %w", err)
	}

	return buf.Bytes(), nil
}
