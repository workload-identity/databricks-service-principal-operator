/*
Copyright 2026 Weidao Lee.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package databricks

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/oauth2"
)

// call is one request the operator made.
type call struct {
	method string
	path   string
	body   string

	// filter is the SCIM filter the request carried, empty when it carried
	// none. It is kept apart from the rest of the query because it is the one
	// part that changes what the answer is allowed to be: a listing narrowed to
	// a name and a listing of the whole account are two questions, and a test
	// asserting that the cheap one sufficed has to be able to say which was
	// asked.
	filter string
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

	// policies is the federation policies on the service principals, served
	// from a store rather than from the table above. Nil unless a test sets
	// one, and everything else goes on being answered from the table.
	//
	// A table cannot answer what these calls are about. What they promise is
	// that two of them leave one policy, and a route answering the same string
	// however often it is asked cannot tell the promise kept from the promise
	// broken -- it says one policy is there whether the calls made one or three.
	policies *policyAccount

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
	409: `{"error_code":"ALREADY_EXISTS","message":"a policy with this id is already there"}`,
	500: `{"error_code":"INTERNAL_ERROR","message":"an invariant on our side was broken"}`,
}

func (r *stubAccount) clients() *clients {
	r.t.Helper()
	if r.policies != nil {
		if r.policies.held == nil {
			r.policies.held = map[string]oauth2.FederationPolicy{}
		}
		r.policies.visible = maps.Clone(r.policies.held)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		if strings.Contains(req.URL.Path, "well-known") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{}"))
			return
		}
		route := req.Method + " " + req.URL.Path
		filter := req.URL.Query().Get("filter")
		r.calls = append(r.calls, call{
			method: req.Method, path: req.URL.Path, body: string(body), filter: filter,
		})
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
		if r.policies != nil && strings.Contains(req.URL.Path, "/federationPolicies") {
			r.policies.serve(r.t, w, req, string(body))
			return
		}
		answer, ok := r.answer[narrowedTo(req.URL.Path, filter)]
		if !ok {
			answer, ok = r.answer[route]
		}
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
	return &clients{cfg: Config{AccountID: testAccountID}, accountClient: ac}
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

// narrowedTo is the answer key for a listing carrying this filter, which is a
// finer route than the collection alone. A path with no entry under it answers
// whatever the filter, so a fixture stays one line until a test needs the
// narrowed listing and the whole one to say different things.
func narrowedTo(path, filter string) string { return "GET " + path + "?filter=" + filter }

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

// listed counts the GETs of path that carried exactly this filter, so a test can
// assert that the whole account was never asked for.
//
// One listing is more than one request whenever it pages -- the last one is the
// empty page that ends it -- so what this number answers is whether the question
// was put, not how many times.
func (r *stubAccount) listed(path, filter string) int {
	n := 0
	for _, c := range r.calls {
		if c.method == http.MethodGet && c.path == path && c.filter == filter {
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

// assignedPolicyPrefix begins the names this account gives a policy whose create
// asked for none, which is what every policy written before this operator named
// its own carries.
const assignedPolicyPrefix = "databricks-assigned-"

// policyAccount is the federation policies on a service principal, held the way
// an account holds them: under the name the create gave, one policy to a name.
//
// Keyed by name rather than kept in a list, because the name is the whole of
// what these calls rest on. A fixture that appended would hold two policies
// after one name was written twice and could not tell that from the account
// refusing the second, which is the difference every test here is about.
type policyAccount struct {
	// held is what the account has. Every write lands here at once.
	held map[string]oauth2.FederationPolicy

	// visible is what a read answers from: what was held before the test wrote
	// anything. Reads being behind writes is the account the controller's second
	// pass meets, milliseconds after its first, and the reason a policy is named
	// rather than looked for.
	//
	// caughtUp puts the two back together, for the tests whose subject is not
	// that window.
	visible  map[string]oauth2.FederationPolicy
	caughtUp bool

	// namesPoliciesItself makes the account ignore the name a create asks for
	// and assign one, as it does for a create that asks for none. It is the one
	// thing this design rests on that cannot be checked from here, so it is
	// arranged instead: what the operator does when Databricks does not take the
	// name is something a test can say.
	namesPoliciesItself bool

	assigned int
}

// serve answers the federation policy calls against the store.
//
// The whole request body replaces what is held, where Databricks would apply the
// update mask. The two agree here because this operator states every field a
// policy has whenever it writes one; a caller that sent part of a policy would
// be testing against an account that does not exist.
func (p *policyAccount) serve(t *testing.T, w http.ResponseWriter, req *http.Request, body string) {
	_, name, addressed := strings.Cut(req.URL.Path, "/federationPolicies/")
	switch {
	case req.Method == http.MethodPost:
		asked := req.URL.Query().Get("policy_id")
		if asked == "" || p.namesPoliciesItself {
			p.assigned++
			asked = fmt.Sprintf("%s%d", assignedPolicyPrefix, p.assigned)
		}
		if _, taken := p.held[asked]; taken {
			refuse(w, http.StatusConflict)
			return
		}
		p.held[asked] = policyFrom(t, body, asked)
		answer(t, w, p.held[asked])
	case addressed && req.Method == http.MethodGet:
		policy, there := p.readable()[name]
		if !there {
			refuse(w, http.StatusNotFound)
			return
		}
		answer(t, w, policy)
	case addressed && req.Method == http.MethodPatch:
		if _, there := p.held[name]; !there {
			refuse(w, http.StatusNotFound)
			return
		}
		p.held[name] = policyFrom(t, body, name)
		answer(t, w, p.held[name])
	case addressed && req.Method == http.MethodDelete:
		if _, there := p.held[name]; !there {
			refuse(w, http.StatusNotFound)
			return
		}
		delete(p.held, name)
		answer(t, w, struct{}{})
	case req.Method == http.MethodGet:
		// Sorted, so that a test naming what was deleted first is naming
		// something the fixture decides rather than something the map does.
		listing := oauth2.ListFederationPoliciesResponse{}
		for _, held := range slices.Sorted(maps.Keys(p.readable())) {
			listing.Policies = append(listing.Policies, p.readable()[held])
		}
		answer(t, w, listing)
	default:
		t.Errorf("nothing here answers %s %s", req.Method, req.URL.Path)
	}
}

// readable is what a read sees, which is not what the account holds until it has
// caught up.
func (p *policyAccount) readable() map[string]oauth2.FederationPolicy {
	if p.caughtUp {
		return p.held
	}
	return p.visible
}

// names is what the account holds, in order, for a test to compare against.
func (p *policyAccount) names() []string { return slices.Sorted(maps.Keys(p.held)) }

// trusting is one policy as the account holds it, built through the same
// function the operator writes one with so that a fixture cannot say a policy is
// there in a shape the operator would never have written.
func trusting(name, issuer, subject, audience string) oauth2.FederationPolicy {
	policy := federationPolicyFor(issuer, subject, audience)
	policy.PolicyId = name
	return policy
}

// policiesHeld keys policies by their name, which is how an account holds them.
func policiesHeld(policies ...oauth2.FederationPolicy) map[string]oauth2.FederationPolicy {
	held := make(map[string]oauth2.FederationPolicy, len(policies))
	for _, policy := range policies {
		held[policy.PolicyId] = policy
	}
	return held
}

func policyFrom(t *testing.T, body, name string) oauth2.FederationPolicy {
	var policy oauth2.FederationPolicy
	if err := json.Unmarshal([]byte(body), &policy); err != nil {
		t.Errorf("the request body is not a federation policy: %v: %s", err, body)
	}
	policy.PolicyId = name
	return policy
}

func answer(t *testing.T, w http.ResponseWriter, body any) {
	written, err := json.Marshal(body)
	if err != nil {
		t.Errorf("the fixture cannot say %+v: %v", body, err)
		return
	}
	_, _ = w.Write(written)
}

// refuse answers with the body that goes with the status, for the reason
// refusals gives: the SDK reads the error code before the status.
func refuse(w http.ResponseWriter, code int) {
	w.WriteHeader(code)
	_, _ = w.Write([]byte(refusals[code]))
}
