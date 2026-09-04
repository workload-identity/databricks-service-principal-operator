package databricks

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
)

// call is one request the operator made.
type call struct {
	method string
	path   string
	body   string
}

// stubAccount is a Databricks account that answers from a table and remembers
// what it was asked. Its live counterpart is live_test.go.
//
// What reaches Databricks is what these tests are about, and it lives in the
// request body: which field a value goes in, whether a call is made at all,
// what a 404 means. A fake standing in for this package cannot cover any of
// that -- it would assert that a call happened, not what was in it.
type stubAccount struct {
	t      *testing.T
	answer map[string]string

	// status is the code for a route that should fail; anything absent is 200.
	// It is a route rather than a flag because half of what these calls promise
	// is what they do with a 404: a service principal that is already gone is
	// deleted, and one that never existed is absent rather than unreachable.
	//
	// The body that goes with it is refusals[code]. The SDK classifies on the
	// body's error_code first and falls back to the status only when that names
	// nothing it knows, so one body for every code decides the answer for all of
	// them -- and the calls whose whole promise is telling a 404 from everything
	// else would be tested against a server that only ever says one thing.
	status map[string]int

	calls []call
}

// refusals is what Databricks answers each failing status with, so that a route
// set to one classifies as that one.
//
// 429 and 503 are absent on purpose. The SDK retries both, so a route serving
// one is asked until the retry budget runs out, and what fails then is a
// timeout rather than the property under test.
var refusals = map[int]string{
	403: `{"error_code":"PERMISSION_DENIED","message":"the caller is not permitted to do this"}`,
	404: `{"error_code":"RESOURCE_DOES_NOT_EXIST","message":"not there"}`,
	500: `{"error_code":"INTERNAL_ERROR","message":"an invariant on our side was broken"}`,
}

func (r *stubAccount) clients() *clients {
	r.t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		if strings.Contains(req.URL.Path, "well-known") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{}"))
			return
		}
		route := req.Method + " " + req.URL.Path
		r.calls = append(r.calls, call{method: req.Method, path: req.URL.Path, body: string(body)})
		w.Header().Set("Content-Type", "application/json")

		// A paginated list works out the next page from what the last one said
		// about itself, so a fixture that leaves those fields out is asked for
		// page one forever. That hangs the test rather than failing it, which is
		// the worst way for a fixture to be wrong -- so the same route being
		// asked absurdly often is itself the failure.
		if r.made(req.Method, req.URL.Path) > 20 {
			r.t.Errorf("asked %s %s more than 20 times; a paginated fixture has to say "+
				"totalResults, startIndex and itemsPerPage or the list never ends",
				req.Method, req.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if code, failing := r.status[route]; failing {
			body, known := refusals[code]
			if !known {
				r.t.Errorf("nothing says what Databricks answers %d with; a status served with "+
					"another one's error_code classifies as the other one's failure", code)
			}
			w.WriteHeader(code)
			_, _ = w.Write([]byte(body))
			return
		}
		answer, ok := r.answer[route]
		if start := req.URL.Query().Get("startIndex"); ok && start != "" && start != "1" {
			ok = false
		}
		if !ok {
			answer = "{}"
		}
		if _, err := w.Write([]byte(answer)); err != nil {
			r.t.Error(err)
		}
	}))
	r.t.Cleanup(server.Close)

	ac, err := databricks.NewAccountClient(&databricks.Config{
		Host: server.URL, AccountID: testAccountID,
		Token: "not-a-credential", Credentials: config.PatCredentials{},
		// The SDK retries what it takes for transient against a five-minute
		// budget by default. Nothing served here is transient -- a route
		// answers the same however often it is asked -- so a test serving a
		// retried status would spend the whole budget and then fail as a
		// timeout, naming neither the call nor the status it was about.
		RetryTimeoutSeconds: 1,
	})
	if err != nil {
		r.t.Fatal(err)
	}
	return &clients{cfg: Config{AccountID: testAccountID}, account: ac}
}

// Account paths, spelled once. Each was taken from what the SDK actually
// requested rather than from the reference, because a path that is close but
// wrong makes a test pass against a server that answers everything.
const (
	accountRoot          = "/api/2.0/accounts/" + testAccountID
	servicePrincipalsAPI = accountRoot + "/scim/v2/ServicePrincipals"
)

func federationPoliciesAPI(servicePrincipalID string) string {
	return accountRoot + "/servicePrincipals/" + servicePrincipalID + "/federationPolicies"
}

// made counts the calls to one path.
func (r *stubAccount) made(method, path string) int {
	n := 0
	for _, c := range r.calls {
		if c.method == method && c.path == path {
			n++
		}
	}
	return n
}

// wrote returns the body of the one write to path, and fails if there was not
// exactly one.
func (r *stubAccount) wrote(path string) string {
	r.t.Helper()
	var bodies []string
	for _, c := range r.calls {
		if c.path == path && c.method != "GET" {
			bodies = append(bodies, c.body)
		}
	}
	if len(bodies) != 1 {
		r.t.Fatalf("made %d writes to %s, want 1: %+v", len(bodies), path, r.calls)
	}
	return bodies[0]
}
