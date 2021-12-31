package auth

import (
	"errors"
	"fmt"
	html "html/template"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/weberc2/auth/pkg/types"
	pz "github.com/weberc2/httpeasy"
)

type Code struct {
	Code string `json:"code"`
}

// WebServer serves the authentication pages for websites (as opposed to
// single-page apps).
type WebServer struct {
	// AuthService is the authentication service backend.
	AuthService AuthService

	// BaseURL is the base URL for the UIService. This **must** end with a
	// trailing slash.
	BaseURL string

	// RedirectDomain specifies the set of subdomains that we will redirect to.
	// So for example, if the `RedirectDomain` field is `google.com`, then we
	// will redirect to `foo.google.com` but not `attacker.com`.
	RedirectDomain string

	// DefaultRedirectLocation is the destination we send users to on success
	// if no redirect location was specified in the query string.
	DefaultRedirectLocation string
}

const (
	pathRegistrationConfirmationHandler = "/confirm"
	pathRegistrationConfirmationForm    = "/confirm"
	pathRegistrationHandler             = "/register"
	pathRegistrationForm                = "/register"
)

func (ws *WebServer) RegistrationFormRoute() pz.Route {
	return pz.Route{
		Path:   pathRegistrationForm,
		Method: "GET",
		Handler: func(r pz.Request) pz.Response {
			context := registrationFormContext{
				FormAction: pathRegistrationHandler,
			}
			return pz.Ok(
				pz.HTMLTemplate(registrationForm, &context),
				&context,
			)
		},
	}
}

var registrationForm = html.Must(html.New("").Parse(`<html>
<head>
	<title>Register</title>
</head>
<body>
<h1>Register</h1>
{{ if .ErrorMessage }}<p id="error-message">{{ .ErrorMessage }}</p>{{ end }}
<form action="{{ .FormAction }}" method="POST">
	<label for="username">Username</label>
	<input type="text" id="username" name="username"><br><br>
	<label for="email">Email</label>
	<input type="text" id="email" name="email"><br><br>
	<input type="submit" value="Submit">
</form>
</body>
</html>`))

var ErrParsingFormData = &pz.HTTPError{
	Status:  http.StatusBadRequest,
	Message: "error parsing form data",
}

type registrationFormContext struct {
	FormAction   string `json:"formAction"`
	ErrorMessage string `json:"errorMessage,omitempty"` // for html template
	PrivateError string `json:"privateError,omitempty"` // logging only
}

func (ws *WebServer) RegistrationHandlerRoute() pz.Route {
	return pz.Route{
		Path:   pathRegistrationHandler,
		Method: "POST",
		Handler: func(r pz.Request) pz.Response {
			form, err := parseForm(r)
			if err != nil {
				return pz.HandleError(
					"error parsing form data",
					ErrParsingFormData,
					&logging{
						Message:   "parsing registration form data",
						ErrorType: fmt.Sprintf("%T", err),
						Error:     err.Error(),
					},
				)
			}

			username := types.UserID(form.Get("username"))
			if err := ws.AuthService.Register(
				username,
				form.Get("email"),
			); err != nil {
				httpErr := &pz.HTTPError{
					Status:  http.StatusInternalServerError,
					Message: "internal server error",
				}
				errors.As(err, &httpErr)
				context := registrationFormContext{
					FormAction:   pathRegistrationHandler,
					ErrorMessage: httpErr.Message,
					PrivateError: err.Error(),
				}
				return pz.Response{
					Status: httpErr.Status,
					Data:   pz.HTMLTemplate(registrationForm, &context),
				}.WithLogging(&context)
			}
			return pz.Created(
				pz.String(registrationSuccessPage),
				&logging{
					Message: "kicked off user registration",
					User:    username,
				},
			)
		},
	}
}

const registrationSuccessPage = `<html>
<head>
	<title>Registration Accepted</title>
<body>
<h1>
Registration Accepted
</h1>
<p>An email has been sent to the email address provided. Please check your
email for a confirmation link.</p>
</body>
</head>
</html>`

func (ws *WebServer) RegistrationConfirmationFormRoute() pz.Route {
	return pz.Route{
		Path:   pathRegistrationConfirmationForm,
		Method: "GET",
		Handler: func(r pz.Request) pz.Response {
			context := registrationConfirmationContext{
				FormAction: pathRegistrationConfirmationHandler,
				Token:      r.URL.Query().Get("t"),
			}
			return pz.Ok(
				pz.HTMLTemplate(registrationConfirmationForm, &context),
				&context,
			)
		},
	}
}

var registrationConfirmationForm = html.Must(html.New("").Parse(`<html>
<head>
	<title>Confirm Registration</title>
</head>
<body>
<h1>Confirm Registration<h1>
{{ if .ErrorMessage }}<p id="error-message">{{ .ErrorMessage }}</p>{{ end }}
<form action="{{ .FormAction }}" method="POST">
	<label for="password">Password</label>
	<input type="password" id="password" name="password"><br><br>
	<input type="hidden" id="token" name="token" value="{{.Token}}">
	<input type="submit" value="Submit">
</form>
</body>
</html>`))

type registrationConfirmationContext struct {
	FormAction   string `json:"formAction"`
	Token        string `json:"token"`                  // hidden form field
	ErrorMessage string `json:"errorMessage,omitempty"` // for html template
	PrivateError string `json:"privateError,omitempty"` // logging only
	ErrorType    string `json:"errorType,omitempty"`    // type of PrivateError
}

func (ws *WebServer) RegistrationConfirmationHandlerRoute() pz.Route {
	return pz.Route{
		Path:   pathRegistrationConfirmationHandler,
		Method: "POST",
		Handler: func(r pz.Request) pz.Response {
			form, err := parseForm(r)
			if err != nil {
				return pz.BadRequest(pz.String("error parsing form"), &logging{
					Message: "parsing registration confirmation form",
					Error:   err.Error(),
				})
			}
			token, password := form.Get("token"), form.Get("password")
			if err := ws.AuthService.ConfirmRegistration(
				token,
				password,
			); err != nil {
				httpErr := &pz.HTTPError{
					Status:  http.StatusInternalServerError,
					Message: "internal server error",
				}
				_ = errors.As(err, &httpErr)
				context := registrationConfirmationContext{
					FormAction:   pathRegistrationConfirmationForm,
					Token:        form.Get("token"),
					ErrorMessage: httpErr.Message,
					PrivateError: err.Error(),
					ErrorType:    fmt.Sprintf("%T", err),
				}
				return pz.Response{
					Status: httpErr.Status,
					Data: pz.HTMLTemplate(
						registrationConfirmationForm,
						&context,
					),
				}.WithLogging(&context)
			}
			return pz.SeeOther(ws.DefaultRedirectLocation, &struct {
				Message string `json:"message"`
			}{"successfully registered user"})
		},
	}
}

func (ws *WebServer) LoginFormPage(r pz.Request) pz.Response {
	query := r.URL.Query()

	// create a struct for templating and logging
	context := struct {
		FormAction string `json:"formAction"`
	}{
		FormAction: ws.BaseURL + "login?" + url.Values{
			"callback": []string{query.Get("callback")},
			"redirect": []string{query.Get("redirect")},
		}.Encode(),
	}

	return pz.Ok(pz.HTMLTemplate(loginForm, &context), &context)
}

var loginForm = html.Must(html.New("").Parse(`<html>
<head>
	<title>Login</title>
</head>
<body>
<h1>Login</h1>
{{ if .ErrorMessage }}<p id="error-message">{{ .ErrorMessage }}</p>{{ end }}
<form action="{{ .FormAction }}" method="POST">
	<label for="username">Username</label>
	<input type="text" id="username" name="username"><br><br>
	<label for="password">Password</label>
	<input type="password" id="password" name="password"><br><br>
	<input type="submit" value="Submit">
</form>
</body>
</html>`))

func (ws *WebServer) LoginHandler(r pz.Request) pz.Response {
	form, err := parseForm(r)
	if err != nil {
		return pz.BadRequest(
			pz.Stringf("parsing credentials: %v", err),
			&logging{
				Message:   "parsing credentials from multi-part form",
				ErrorType: fmt.Sprintf("%T", err),
				Error:     err.Error(),
			},
		)
	}

	username := types.UserID(form.Get("username"))

	code, err := ws.AuthService.LoginAuthCode(&types.Credentials{
		User:     username,
		Password: form.Get("password"),
	})
	if err != nil {
		if errors.Is(err, ErrCredentials) {
			return pz.Unauthorized(
				pz.HTMLTemplate(loginForm, &struct {
					Location     html.HTML
					FormAction   string
					ErrorMessage string
				}{
					FormAction:   ws.BaseURL + "login",
					ErrorMessage: "Invalid credentials",
				}),
				&logging{
					User:    username,
					Message: "login failed",
					Error:   err.Error(),
				},
			)
		}
		return pz.InternalServerError(&logging{
			User:      username,
			Message:   "logging in",
			ErrorType: fmt.Sprintf("%T", err),
			Error:     err.Error(),
		})
	}

	query := r.URL.Query()
	context := struct {
		Message  string `json:"message,omitempty"`
		Target   string `json:"target,omitempty"`
		Redirect redirectResult
		Callback redirectResult
	}{
		Callback: redirectResult{
			Specified: query.Get("callback"),
			Default:   ws.DefaultRedirectLocation,
			Domain:    ws.RedirectDomain,
		},
		Redirect: redirectResult{
			Specified: query.Get("redirect"),
			Default:   ws.DefaultRedirectLocation,
			Domain:    ws.RedirectDomain,
		},
	}

	validateRedirect(&context.Callback)
	if context.Callback.ParseError != "" {
		context.Message = "`callback` parameter contains invalid URL"
		return pz.BadRequest(nil, &context)
	}
	validateRedirect(&context.Redirect)
	if context.Redirect.ParseError != "" {
		context.Message = "`redirect` parameter contains invalid URL"
		return pz.BadRequest(nil, &context)
	}

	context.Target = context.Callback.Actual + "?" + url.Values{
		"code":     []string{code},
		"redirect": []string{context.Redirect.Actual},
		"callback": []string{context.Callback.Actual},
	}.Encode()

	// Previously we used 307 Temporary Redirect, but since we're handling a
	// POST request, the redirect also issued a POST request instead of a GET
	// request. It seems like 303 See Other does what we want.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Redirections#temporary_redirections
	return pz.SeeOther(context.Target, &context)
}

func validateRedirect(context *redirectResult) {
	if context.Specified != "" {
		u, err := url.Parse(context.Specified)
		if err != nil {
			context.ParseError = err.Error()
			return
		}
		context.Parsed = u

		// Make sure the `Host` is either an exact match for the
		// `RedirectDomain` or a valid subdomain. If it's not, then redirect to
		// the default redirect domain.
		if u.Host != context.Domain && !strings.HasSuffix(
			u.Host,
			// Note that we have to prepend a `.` onto the `RedirectDomain`
			// before checking if it is a suffix match to be sure we're only
			// matching subdomains. For example, if `RedirectDomain` is
			// `google.com`, an attacker could register `evilgoogle.com` which
			// would match if we didn't prepend the `.` (causing us to send the
			// attacker our tokens).
			fmt.Sprintf(".%s", context.Domain),
		) {
			context.ValidationError = "" +
				"`location` query string parameter host is neither the " +
				"redirect domain itself nor a subdomain thereof. Falling " +
				"back to default URL."
			context.Actual = context.Default
		}
	} else {
		context.ValidationError = "`location` query string " +
			"parameter is empty or unset. Falling back to " +
			"default URL."
		context.Actual = context.Default
	}
	context.Actual = context.Specified
}

type redirectResult struct {
	Domain          string   `json:"domain"`
	Specified       string   `json:"specified"`
	Default         string   `json:"default"`
	Parsed          *url.URL `json:"parsed,omitempty"`
	Actual          string   `json:"actual,omitempty"`
	ParseError      string   `json:"parseError,omitempty"`
	ValidationError string   `json:"validationError,omitempty"`
}

func parseForm(r pz.Request) (url.Values, error) {
	// Read at most 2kb data to avoid DOS attack. That should be plenty for our
	// form.
	data, err := ioutil.ReadAll(io.LimitReader(r.Body, 2056))
	if err != nil {
		return nil, fmt.Errorf("reading request body: %w", err)
	}
	form, err := url.ParseQuery(string(data))
	if err != nil {
		return nil, fmt.Errorf("parsing form data: %w", err)
	}
	return form, nil
}
