package client

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

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

func (app *WebServerApp) LogoutRoute(path string) pz.Route {
	return pz.Route{
		Path:   path,
		Method: "GET",
		Handler: func(r pz.Request) pz.Response {
			type logging struct {
				Message            string `json:"message"`
				Redirect           string `json:"redirect"`
				RedirectSpecified  string `json:"redirectSpecified"`
				RedirectParseError string `json:"redirectParseError,omitempty"`
				Error              string `json:"error,omitempty"`
			}

			context := logging{RedirectSpecified: r.Headers.Get("Referer")}

			if err := validateURL(context.RedirectSpecified); err != nil {
				context.RedirectParseError = err.Error()
				context.Redirect = join(app.BaseURL, app.DefaultRedirect)
			} else {
				context.Redirect = context.RedirectSpecified
			}

			refreshCookie, err := r.Cookie("Refresh-Token")
			if err != nil {
				context.Message = "missing refresh token cookie; nothing to " +
					"do; redirecting"
				context.Error = err.Error()
				return pz.SeeOther(context.Redirect, &context)
			}
			if err := app.Client.Logout(refreshCookie.Value); err != nil {
				context.Message = "issuing logout request to auth server"
				context.Error = err.Error()
				return pz.InternalServerError(&context)
			}

			portStart := strings.Index(app.BaseURL.Host, ":")
			if portStart < 0 {
				portStart = len(app.BaseURL.Host)
			}
			cookieDomain := app.BaseURL.Host[:portStart]
			context.Message = "successfully logged out"
			return pz.SeeOther(context.Redirect, &context).WithCookies(
				expireCookie(cookie("Access-Token", cookieDomain, "")),
				expireCookie(cookie("Refresh-Token", cookieDomain, "")),
			)
		},
	}
}

func expireCookie(c *http.Cookie) *http.Cookie {
	c.MaxAge = -1
	c.Expires = time.Unix(0, 0)
	return c
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
	if len(encrypted) < nsz {
		return "", fmt.Errorf(
			"encrypted data is smaller than the nonce (%d vs %d respectively)",
			len(encrypted),
			nsz,
		)
	}
	data, err := gcm.Open(nil, encrypted[:nsz], encrypted[nsz:], nil)
	return string(data), err
}

func validateURL(input string) error {
	if input == "" {
		return fmt.Errorf("url is empty")
	}
	_, err := url.Parse(input)
	return err
}
