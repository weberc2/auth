package client

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/weberc2/auth/pkg/auth"
	pz "github.com/weberc2/httpeasy"
)

type codeCallbackParams struct {
	baseURL         *url.URL
	codeParam       string
	redirectParam   string
	redirectDefault string
	encryptionKey   string
}

type codeCallbackResult struct {
	Message            string `json:"message,omitempty"`
	RedirectSpecified  string `json:"redirectSpecified"`
	RedirectActual     string `json:"redirectActual"`
	AccessToken        string `json:"accessToken,omitempty"`
	RefreshToken       string `json:"refreshToken,omitempty"`
	CookieDomain       string `json:"cookieDomain,omitempty"`
	RedirectParseError error  `json:"redirectParseError,omitempty"`
	CodeParseError     error  `json:"codeParseError,omitempty"`
	EncryptionError    error  `json:"encryptionError,omitempty"`
	ExchangeError      error  `json:"exchangeError,omitempty"`
}

func (result *codeCallbackResult) toResponse() pz.Response {
	if result.CodeParseError != nil {
		return pz.BadRequest(nil, result)
	}
	if result.ExchangeError != nil {
		return pz.HandleError(
			"exchanging auth code",
			result.ExchangeError,
			result,
		)
	}
	if result.EncryptionError != nil {
		return pz.InternalServerError(result)
	}
	return pz.SeeOther(result.RedirectActual, result).WithCookies(
		cookie("Access-Token", result.CookieDomain, result.AccessToken),
		cookie("Refresh-Token", result.CookieDomain, result.RefreshToken),
	)
}

func codeCallback(
	client *Client,
	params *codeCallbackParams,
) *codeCallbackResult {
	result := codeCallbackResult{
		RedirectSpecified: join(params.baseURL, params.redirectParam),
	}
	if params.redirectParam == "" {
		result.RedirectParseError = paramNotFoundErr("redirect")
		result.RedirectActual = join(params.baseURL, params.redirectDefault)
	} else if _, err := url.Parse(result.RedirectSpecified); err != nil {
		result.RedirectParseError = err
		result.RedirectActual = join(params.baseURL, params.redirectDefault)
	} else {
		result.RedirectActual = result.RedirectSpecified
	}

	if params.codeParam == "" {
		result.CodeParseError = paramNotFoundErr("code")
		return &result
	}

	tokens, err := client.Exchange(params.codeParam)
	if err != nil {
		result.ExchangeError = err
		return &result
	}

	if err := encryptTokens(tokens, params.encryptionKey); err != nil {
		result.EncryptionError = err
		return &result
	}

	portStart := strings.Index(params.baseURL.Host, ":")
	if portStart < 0 {
		portStart = len(params.baseURL.Host)
	}

	result.Message = "successfully exchanged auth code"
	result.AccessToken = tokens.AccessToken.Token
	result.RefreshToken = tokens.RefreshToken.Token
	result.CookieDomain = params.baseURL.Host[:portStart]
	return &result
}

type paramNotFoundErr string

func (err paramNotFoundErr) Error() string {
	return "required query string parameter missing or empty: " + string(err)
}

func cookie(name, domain, value string) *http.Cookie {
	return &http.Cookie{
		Name: name,
		// without this it will use the route's path (specifically the dirname
		// of the route's path)
		Path:     "/",
		Domain:   domain,
		Value:    value,
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	}
}

func join(base *url.URL, path string) string {
	return fmt.Sprintf(
		"%s/%s",
		strings.TrimRight(base.String(), "/"),
		strings.TrimLeft(path, "/"),
	)
}

func encryptTokens(tokens *auth.TokenDetails, key string) error {
	access, err := encrypt(tokens.AccessToken.Token, key)
	if err != nil {
		return fmt.Errorf("encrypting access token: %w", err)
	}
	refresh, err := encrypt(tokens.RefreshToken.Token, key)
	if err != nil {
		return fmt.Errorf("encrypting refresh token: %w", err)
	}
	tokens.AccessToken.Token, tokens.RefreshToken.Token = access, refresh
	return nil
}

func encrypt(input, key string) (string, error) {
	plaintext := []byte(input)

	k := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(k[:])
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("populating nonce: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(gcm.Seal(
		nonce,
		nonce,
		plaintext,
		nil,
	)), nil
}
