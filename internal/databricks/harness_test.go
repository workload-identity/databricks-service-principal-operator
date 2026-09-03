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

// recording is a Databricks account that answers from a table and remembers
// what it was asked.
//
// What reaches Databricks is what these tests are about, and it lives in the
// request body: which field a value goes in, whether a call is made at all,
// what a 404 means. A fake standing in for this package cannot cover any of
// that -- it would assert that a call happened, not what was in it.
type recording struct {
	t      *testing.T
	answer map[string]string

	// status is the code for a route that should fail; anything absent is 200.
	// It is a route rather than a flag because half of what these calls promise
	// is what they do with a 404: a service principal that is already gone is
	// deleted, and one that never existed is absent rather than unreachable.
	status map[string]int

	calls []call
}

func (r *recording) clients() *clients {
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
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"error_code":"NOT_FOUND","message":"not there"}`))
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
func (r *recording) made(method, path string) int {
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
func (r *recording) wrote(path string) string {
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
