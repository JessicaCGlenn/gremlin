package gremlin

import "errors"

const (
	StatusSuccess                  = 200
	StatusNoContent                = 204
	StatusPartialContent           = 206
	StatusUnauthorized             = 401
	StatusAuthenticate             = 407
	StatusMalformedRequest         = 498
	StatusInvalidRequestArguments  = 499
	StatusServerError              = 500
	StatusScriptEvaluationError    = 597
	StatusServerTimeout            = 598
	StatusServerSerializationError = 599
)

var ErrorMsg = map[int]string{
	StatusUnauthorized:             "Unauthorized",
	StatusAuthenticate:             "Authenticate",
	StatusMalformedRequest:         "Malformed Request",
	StatusInvalidRequestArguments:  "Invalid Request Arguments",
	StatusServerError:              "Server Error",
	StatusScriptEvaluationError:    "Script Evaluation Error",
	StatusServerTimeout:            "Server Timeout",
	StatusServerSerializationError: "Server Serialization Error",
}

var ConnectionErrors = map[int]error {
	StatusUnauthorized:             errors.New("unauthorized"),
	StatusAuthenticate:             errors.New("authenticate"),
	StatusMalformedRequest:         errors.New("malformed request"),
	StatusInvalidRequestArguments:  errors.New("invalid request arguments"),
	StatusServerError:              errors.New("server error"),
	StatusScriptEvaluationError:    errors.New("script evaluation error"),
	StatusServerTimeout:            errors.New("server timeout"),
	StatusServerSerializationError: errors.New("server serialization error"),
}