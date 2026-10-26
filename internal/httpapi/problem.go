package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/votiakov/airq/internal/telemetry"
)

const problemContentType = "application/problem+json"

// Problem is an RFC 9457 problem details body. Type is always "about:blank" (so Title
// is the HTTP status text); Code is the stable, machine-readable error identifier
// clients should switch on.
type Problem struct {
	Type      string       `json:"type"`
	Title     string       `json:"title"`
	Status    int          `json:"status"`
	Code      string       `json:"code"`
	Detail    string       `json:"detail,omitempty"`
	Instance  string       `json:"instance,omitempty"`
	RequestID string       `json:"request_id,omitempty"`
	Errors    []FieldError `json:"errors,omitempty"`
}

// FieldError describes one invalid request parameter.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func newProblem(r *http.Request, status int, code, detail string, fieldErrs []FieldError) Problem {
	return Problem{
		Type:      "about:blank",
		Title:     http.StatusText(status),
		Status:    status,
		Code:      code,
		Detail:    detail,
		Instance:  r.URL.Path,
		RequestID: telemetry.RequestID(r.Context()),
		Errors:    fieldErrs,
	}
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, detail string, fieldErrs ...FieldError) {
	body, _ := json.Marshal(newProblem(r, status, code, detail, fieldErrs))
	h := w.Header()
	h.Set("Content-Type", problemContentType)
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}
