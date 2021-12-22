package client

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"

	pz "github.com/weberc2/httpeasy"
)

type WebServerApp struct {
	Client          Client
	BaseURL         *url.URL
	DefaultRedirect string
	Key             string
}

func (app *WebServerApp) DecryptAccessToken(r pz.Request) (string, error) {
	return app.DecryptCookie(r, "Access-Token")
}

func (app *WebServerApp) DecryptRefreshToken(r pz.Request) (string, error) {
	return app.DecryptCookie(r, "Refresh-Token")
}

func (app *WebServerApp) DecryptCookie(
	r pz.Request,
	cookie string,
) (string, error) {
	c, err := r.Cookie(cookie)
	if err != nil {
		return "", fmt.Errorf("finding cookie `%s`: %w", cookie, err)
	}
	plaintext, err := app.decryptCookie(c)
	if err != nil {
		return "", fmt.Errorf("decrypting `%s` cookie: %w", cookie, err)
	}
	return plaintext, nil
}

func (app *WebServerApp) decryptCookie(cookie *http.Cookie) (string, error) {
	plaintext, err := decrypt(cookie.Value, app.Key)
	if err != nil {
		return "", fmt.Errorf("decrypting cookie: %w", err)
	}
	return plaintext, nil
}

func (app *WebServerApp) Encrypt(data string) (string, error) {
	return encrypt(data, app.Key)
}

func (app *WebServerApp) AuthCodeCallbackRoute(path string) pz.Route {
	return pz.Route{
		Path:   path,
		Method: "GET",
		Handler: func(r pz.Request) pz.Response {
			query := r.URL.Query()
			return codeCallback(
				&app.Client,
				&codeCallbackParams{
					baseURL:         app.BaseURL,
					codeParam:       query.Get("code"),
					redirectParam:   query.Get("redirect"),
					redirectDefault: app.DefaultRedirect,
					encryptionKey:   app.Key,
				},
			).toResponse()
		},
	}
}

func decrypt(input, key string) (string, error) {
	encrypted, err := base64.RawURLEncoding.DecodeString(input)
	if err != nil {
		return "", fmt.Errorf("base64-decoding: %w", err)
	}
	k := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(k[:])
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nsz := gcm.NonceSize()
	data, err := gcm.Open(nil, encrypted[:nsz], encrypted[nsz:], nil)
	return string(data), err
}
