package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/weberc2/auth/pkg/auth"
)

type Client struct {
	HTTP    http.Client
	BaseURL string
}

func DefaultClient(baseURL string) Client {
	return Client{
		HTTP:    http.Client{Timeout: 10 * time.Second},
		BaseURL: baseURL,
	}
}

func (c *Client) Logout(refreshToken string) error {
	data, err := (&refresh{refreshToken}).marshal()
	if err != nil {
		return fmt.Errorf("logging out: %w", err)
	}

	rsp, err := c.HTTP.Post(
		c.BaseURL+"/api/logout",
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return fmt.Errorf("logging out: %w", err)
	}
	if rsp.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"logging out: status code: wanted `200`; found `%d`",
			rsp.StatusCode,
		)
	}
	return nil
}

func (c *Client) Refresh(refreshToken string) (*auth.RefreshResponse, error) {
	data, err := (&refresh{refreshToken}).marshal()
	if err != nil {
		return nil, fmt.Errorf("marshaling refresh token: %w", err)
	}
	rsp, err := c.HTTP.Post(
		c.BaseURL+"/api/refresh",
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return nil, fmt.Errorf("refreshing access token: %w", err)
	}
	if rsp.StatusCode == http.StatusUnauthorized {
		return nil, auth.ErrUnauthorized
	}
	if rsp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"token refresh response status: wanted `200`; found `%d`",
			rsp.StatusCode,
		)
	}
	defer rsp.Body.Close()

	data, err = ioutil.ReadAll(rsp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading token refresh response body: %w", err)
	}
	var refresh auth.RefreshResponse
	if err := json.Unmarshal(data, &refresh); err != nil {
		return nil, fmt.Errorf("invalid token refresh payload: %w", err)
	}
	return &refresh, nil
}

type refresh struct {
	RefreshToken string `json:"refreshToken"`
}

func (r *refresh) marshal() ([]byte, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("marshaling refresh token: %w", err)
	}
	return data, nil
}

func (c *Client) Exchange(code string) (*auth.TokenDetails, error) {
	data, err := json.Marshal(&auth.Code{Code: code})
	if err != nil {
		return nil, fmt.Errorf("marshaling code: %w", err)
	}

	rsp, err := c.HTTP.Post(
		c.BaseURL+"/api/exchange",
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return nil, fmt.Errorf("exchanging auth code: %w", err)
	}
	if rsp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"auth code exchange status: wanted `200`; found `%d`",
			rsp.StatusCode,
		)
	}
	defer rsp.Body.Close()

	data, err = ioutil.ReadAll(rsp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading exchange response body: %w", err)
	}

	var tokens auth.TokenDetails
	if err := json.Unmarshal(data, &tokens); err != nil {
		return nil, fmt.Errorf("unmarshaling exchange response: %w", err)
	}

	return &tokens, nil
}
