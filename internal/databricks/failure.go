package databricks

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/databricks/databricks-sdk-go/apierr"
)

// FailureKind says what went wrong, in terms this package can be sure of.
//
// It stops short of saying what to do about it. Whether a kind means "report it
// and wait for someone to fix the declaration" or "come back and try again" is a
// controller's policy, and controllers change that policy for reasons that have
// nothing to do with Databricks.
type FailureKind string

const (
	// Unavailable is the default: Databricks could not answer, and the same
	// request later may well succeed.
	Unavailable FailureKind = "Unavailable"

	// NotFound means Databricks looked and the thing is not there.
	NotFound FailureKind = "NotFound"

	// Rejected means Databricks refused the request as invalid -- an id of the
	// wrong shape, a field it will not accept. Databricks answers some of these
	// with 400 rather than 404.
	Rejected FailureKind = "Rejected"

	// Malformed means a value in the spec cannot be a Databricks coordinate at
	// all, so the request was never made. The CRD's own validation is a pattern
	// and does not catch everything.
	Malformed FailureKind = "Malformed"

	// NotConfigured means the operator does not yet know which Databricks
	// account to act in. Nothing was asked of Databricks, because there is
	// nobody to ask as.
	NotConfigured FailureKind = "NotConfigured"

	// Denied means Databricks refused on privilege. It says nothing about the
	// declaration: the operator's own service principal is short of something,
	// and no retry supplies it -- somebody has to grant it in Databricks.
	Denied FailureKind = "Denied"
)

// ErrMalformedCoordinate reports a value this operator recorded that it can no
// longer use.
//
// Every producer parses an id Databricks returned and the operator wrote into
// status, so nothing anybody typed reaches here and there is nothing in a spec
// to correct. The id is carried as a string and the federation policy API wants
// an int64; nothing between the two checks that it fits, because the only writer
// is Databricks.
type ErrMalformedCoordinate struct {
	Field string
	Value string
	Cause error
}

func (e *ErrMalformedCoordinate) Error() string {
	return fmt.Sprintf("%s %q is not a Databricks coordinate: %v", e.Field, e.Value, e.Cause)
}

func (e *ErrMalformedCoordinate) Unwrap() error { return e.Cause }

// ErrNoIssuer is refusing to make or look for a service principal without the
// one thing that says which cluster it belongs to.
//
// The marker is set at creation and cannot be added afterwards, so one made
// without it is permanently outside the set whoever governs the account can ask
// about. It is a mistake on this side rather than anything Databricks refuses.
var ErrNoIssuer = errors.New("no issuer to mark the service principal with")

// KindOf classifies an error. Anything unrecognised is Unavailable, so a failure
// this package has not learned about is retried rather than reported as the
// caller's mistake.
func KindOf(err error) FailureKind {
	if err == nil {
		return ""
	}
	var malformed *ErrMalformedCoordinate
	switch {
	case notConfigured(err):
		// Ahead of everything: no request was made, so no classification below
		// this can describe what happened.
		return NotConfigured
	case errors.As(err, &malformed):
		// The request was never made, so nothing below it can apply.
		return Malformed
	// A refusal is tested before the 404 and 400 checks, and the asymmetry is
	// deliberate. A refusal is about the operator's own standing, which is the
	// same fact whatever question was being asked, so it survives being wrapped.
	// A 404 is about the thing asked for.
	case errors.Is(err, apierr.ErrPermissionDenied), errors.Is(err, apierr.ErrUnauthenticated):
		// Separated from Unavailable so it is not retried on the workqueue's
		// exponential backoff, which would report a misconfigured operator as
		// though Databricks were down and hide it behind an ever-longer wait.
		return Denied
	case errors.Is(err, apierr.ErrResourceDoesNotExist), errors.Is(err, apierr.ErrNotFound):
		return NotFound
	case isStatus(err, http.StatusBadRequest):
		return Rejected
	default:
		return Unavailable
	}
}

func isStatus(err error, code int) bool {
	var apiErr *apierr.APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == code
}

// Reason extracts the message Databricks itself gave, for putting into a
// condition. It is not rewritten: a paraphrased message is one that cannot be
// searched for.
func Reason(err error) string {
	if err == nil {
		return ""
	}
	var apiErr *apierr.APIError
	if errors.As(err, &apiErr) && apiErr.Message != "" {
		if apiErr.ErrorCode != "" {
			return apiErr.ErrorCode + ": " + apiErr.Message
		}
		return apiErr.Message
	}
	return err.Error()
}
