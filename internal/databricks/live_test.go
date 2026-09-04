//go:build databricks
// +build databricks

package databricks

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/databricks-sdk-go/service/oauth2"
	"k8s.io/apimachinery/pkg/util/uuid"
)

// Coordinates for a live Databricks account, supplied by whoever runs these.
//
// Both are the SDK's own names, so an environment already set up for the CLI or
// the SDK runs these without translation. Nothing else is needed: with the host
// set, the SDK finds the matching profile and authenticates as whoever is
// logged in.
const (
	envHost      = "DATABRICKS_HOST"
	envAccountID = "DATABRICKS_ACCOUNT_ID"
)

// liveIssuer stands in for a cluster's issuer.
//
// It has to be a real one. Databricks fetches the issuer's OpenID configuration
// when the federation policy is written, not when a token is first exchanged, and
// refuses the write outright: "Unable to load valid OpenID configuration for
// issuer". So an invented hostname cannot be used here, and a cluster whose
// issuer is not published cannot be served by this operator at all -- it fails at
// the policy, one reconcile in, rather than later at exchange.
//
// GitHub Actions is used because it is public, stable, and unmistakably not a
// Kubernetes cluster. The subjects these tests trust on it look like
// system:serviceaccount:..., which GitHub never issues, so no token anywhere can
// satisfy a policy left behind by a failed run.
const liveIssuer = "https://token.actions.githubusercontent.com"

// live returns clients for a real account.
//
// Nothing about the account is in this repository. These tests answer questions
// only Databricks can answer, and the coordinates of the account they are
// answered against name somebody's account and the data in it, so they are
// injected.
//
// A contributor with no Databricks account has to be able to run the suite, and
// a test that fails for them teaches them to ignore failures. The build tag on
// this file is what arranges that: without it these are not compiled, so there
// is nothing to skip and nothing to explain.
//
// With it, a missing coordinate is a failure. Reaching here means somebody asked
// for these by name, and the answer to "you asked for the live tests and got
// none" cannot be a run that says ok.
func live(t *testing.T) Clients {
	t.Helper()
	cfg := RuntimeFromEnv().ForAccount(
		os.Getenv(envHost),
		os.Getenv(envAccountID),
		"", // no clientId: these run as whoever is logged in, not as the operator
	)

	// The operator's own auth is a projected token exchanged for the service
	// principal named by clientId. Running these as a person means neither is
	// present, and leaving OIDCTokenFilepath set from an inherited environment
	// would send the SDK looking for a file that is not there.
	cfg.OIDCTokenFilepath = ""
	cfg.TokenAudience = ""

	for name, value := range map[string]string{
		envHost:      cfg.AccountHost,
		envAccountID: cfg.AccountID,
	} {
		if value == "" {
			t.Fatalf("%s is not set, so these tests would answer nothing. They are built only "+
				"when asked for, and asking for them is how somebody says they have an account "+
				"to answer against: set %s and %s, or run `make test-databricks`, which refuses "+
				"before it compiles anything", name, envHost, envAccountID)
		}
	}

	clients, err := New(cfg)
	if err != nil {
		t.Fatalf("building clients for the live account: %v", err)
	}
	return clients
}

// scratch creates a service principal and arranges for it to be removed.
//
// A leftover here is not litter in a temporary directory -- it is an identity in
// somebody's Databricks account that nothing will ever clean up, because the
// operator only removes what a Kubernetes object still points at.
// liveIssuing is one issuing against the live account, with a uid no other test
// uses.
func liveIssuing(namespace, name string) Issuing {
	return Issuing{
		Issuer:            liveIssuer,
		Namespace:         namespace,
		Name:              name,
		ServiceAccountUID: string(uuid.NewUUID()),
	}
}

func scratch(t *testing.T, clients Clients, namespace, name string) (id, clientID string) {
	t.Helper()
	ctx := context.Background()

	// Registered before the call, and by display name rather than by id.
	//
	// A create that returns an error has still created the service principal:
	// Databricks validates the request after the record exists, so a refused
	// create leaves an identity behind and returns no id to remove it by, so
	// cleaning up only on success leaves them there.
	t.Cleanup(func() { removeByDisplayName(t, clients, DisplayNameFor(namespace, name, "")) })

	id, clientID, err := clients.CreateServicePrincipal(ctx, liveIssuing(namespace, name))
	if err != nil {
		t.Fatalf("creating a service principal for %s/%s: %v", namespace, name, err)
	}
	return id, clientID
}

// removeByDisplayName deletes whatever a test created, whether or not the create
// reported success.
//
// The display name is the only handle available when no id came back. It is not
// unique in Databricks, which is why every test here builds one nothing else
// would produce.
func removeByDisplayName(t *testing.T, clients Clients, displayName string) {
	t.Helper()
	ctx := context.Background()

	// Retried, because a create is not listed the moment it returns. A cleanup
	// that lists once and finds nothing does not fail anything -- it leaves a
	// service principal in somebody's real account with nothing to say where it
	// came from.
	var all []iam.AccountServicePrincipal
	var err error
	for attempt := range 20 {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		all, err = clients.AccountClient().ServicePrincipalsV2.ListAll(ctx, iam.ListAccountServicePrincipalsRequest{})
		if err != nil {
			t.Errorf("listing service principals to clean up %q: %v -- "+
				"anything this test created is still in the account", displayName, err)
			return
		}
		if slices.ContainsFunc(all, func(p iam.AccountServicePrincipal) bool {
			return p.DisplayName == displayName
		}) {
			break
		}
	}
	for _, principal := range all {
		if principal.DisplayName != displayName {
			continue
		}
		if err := clients.DeleteServicePrincipal(ctx, principal.Id); err != nil {
			t.Errorf("the service principal this test created is still in the account: "+
				"id %s, deleting it failed with %v -- it has to be removed by hand",
				principal.Id, err)
		}
	}
}

// unique keeps two runs, or two tests, from colliding on a name. Databricks does
// not enforce uniqueness on display names, so a collision would not fail -- it
// would leave two identities that no assertion can tell apart.
// liveSequence keeps two names apart when the tests run at once. A timestamp
// alone is not enough: two goroutines can read one clock tick.
var liveSequence atomic.Uint64

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), liveSequence.Add(1))
}

// TestLiveCreateReturnsBothIdentifiers is the claim CreateServicePrincipal's
// signature makes, checked against the thing that decides it.
//
// The numeric id is what federation policies hang off and what deletion takes;
// the applicationId is what the workload presents at token exchange. They are
// different values, both assigned by Databricks, and the operator records both
// because neither can be derived from the other. If a create ever returned one
// and not the other, every identity after it would be recorded as usable while
// missing the half its workload needs.
func TestLiveCreateReturnsBothIdentifiers(t *testing.T) {
	// Waiting is what these do: a write becomes visible when it does, and the
	// longest single wait measured here is seconds. They wait together.
	t.Parallel()

	clients := live(t)
	id, clientID := scratch(t, clients, "live-test", unique("both-identifiers"))

	if id == "" {
		t.Error("Databricks returned no numeric id, so nothing could hang a federation policy off it")
	}
	if clientID == "" {
		t.Error("Databricks returned no applicationId, so the workload would have nothing to present")
	}
	if id == clientID {
		t.Errorf("the numeric id and the applicationId came back equal (%s); "+
			"they are different identifiers and the operator records both", id)
	}
}

// TestLiveTheFederationPolicyIsTheRecord covers the only thing on the Databricks
// side that says which cluster an identity belongs to and whose it is.
//
// There is nowhere on a service principal to record it: externalId is the SCIM
// field meant for exactly this and Databricks caps it at 36 characters, which is
// shorter than a Kubernetes subject by itself. So the policy carries the whole
// of it. If Databricks ever normalised either value, whoever governs the account
// would lose the ability to say which identities belong to a cluster they want
// to cut off -- and nothing on this side would notice, because every other test
// asserts what was sent rather than what was stored.
func TestLiveTheFederationPolicyIsTheRecord(t *testing.T) {
	// Waiting is what these do: a write becomes visible when it does, and the
	// longest single wait measured here is seconds. They wait together.
	t.Parallel()

	clients := live(t)
	ctx := context.Background()

	namespace, name := "live-test", unique("the-record")
	id, _ := scratch(t, clients, namespace, name)
	subject := SubjectFor(namespace, name)

	if err := clients.EnsureFederationPolicy(ctx, id, liveIssuer, subject, "databricks"); err != nil {
		t.Fatalf("writing the federation policy: %v", err)
	}

	var policies []oauth2.FederationPolicy
	until(t, "the federation policy that was just written is listed", func() bool {
		policies = policiesOn(t, clients, id)
		return len(policies) == 1
	})
	policy := policies[0].OidcPolicy
	if policy == nil {
		t.Fatal("the policy came back with no oidc policy, so it records neither issuer nor subject")
	}
	if policy.Issuer != liveIssuer {
		t.Errorf("issuer came back as %q and %q was written; this is what says which cluster",
			policy.Issuer, liveIssuer)
	}
	if policy.Subject != subject {
		t.Errorf("subject came back as %q and %q was written; this is what says whose it is",
			policy.Subject, subject)
	}
}

// TestLiveAFederationPolicyIsAcceptedTwice covers what EnsureFederationPolicy
// promises: that reconciling an identity which already has its policy is neither
// a second policy nor an error.
//
// The controller makes this call on every pass, which is at least once a minute
// per identity forever. If a repeat were an error, every converged identity
// would report itself broken; if it were a second policy, the account would
// accumulate one per reconcile.
func TestLiveAFederationPolicyIsAcceptedTwice(t *testing.T) {
	// Waiting is what these do: a write becomes visible when it does, and the
	// longest single wait measured here is seconds. They wait together.
	t.Parallel()

	clients := live(t)
	ctx := context.Background()

	namespace, name := "live-test", unique("federation-policy")
	id, _ := scratch(t, clients, namespace, name)
	subject := SubjectFor(namespace, name)

	if err := clients.EnsureFederationPolicy(ctx, id, liveIssuer, subject, "databricks"); err != nil {
		t.Fatalf("writing the federation policy: %v", err)
	}
	// The second call has to be able to see the first, or it is not the call the
	// controller makes: those are a minute apart, and this is asserting what
	// happens when the policy is already there rather than what happens inside
	// the window where it is not yet listed.
	until(t, "the first policy is listed", func() bool {
		return len(policiesOn(t, clients, id)) == 1
	})

	if err := clients.EnsureFederationPolicy(ctx, id, liveIssuer, subject, "databricks"); err != nil {
		t.Fatalf("writing the same federation policy again: %v -- "+
			"the controller does this on every pass, so every converged identity "+
			"would report itself broken", err)
	}

	// A second policy, if that call made one, is listed a moment later rather
	// than at once -- so this is asserted by going on reading rather than by
	// reading once.
	var count int
	stable(t, "the service principal carries one federation policy after two identical calls "+
		"(the controller makes this call once a minute per identity)", func() bool {
		count = len(policiesOn(t, clients, id))
		return count == 1
	})
	if count != 1 {
		t.Errorf("the service principal carries %d federation policies, want 1", count)
	}
}

// TestLiveExistsFollowsDeletion covers the read the operator uses to notice that
// somebody in Databricks removed an identity it created.
//
// The operator does not recreate one that was removed -- that is the governance
// side's decision to respect -- but it has to see it, because a recorded id that
// no longer names anything would go on being reported as usable while every call
// the workload makes is refused.
func TestLiveExistsFollowsDeletion(t *testing.T) {
	// Waiting is what these do: a write becomes visible when it does, and the
	// longest single wait measured here is seconds. They wait together.
	t.Parallel()

	clients := live(t)
	ctx := context.Background()

	namespace, name := "live-test", unique("exists")
	id, _ := scratch(t, clients, namespace, name)

	found, err := clients.ServicePrincipalExists(ctx, id)
	if err != nil {
		t.Fatalf("asking whether the service principal exists: %v", err)
	}
	if !found {
		t.Fatal("a service principal that was just created reports as absent")
	}

	if err := clients.DeleteServicePrincipal(ctx, id); err != nil {
		t.Fatalf("deleting the service principal: %v", err)
	}

	// Polled rather than asked once. A delete that has returned is not yet
	// visible to a read: the first Get after it still answers, and the operator
	// asks this question on a schedule rather than immediately after its own
	// delete, so a brief disagreement is the API being eventually consistent
	// rather than the answer being wrong. The bound is what makes it a test --
	// the read has to converge, and how long it took is logged so that a change
	// in it is visible rather than absorbed.
	const bound = 30 * time.Second
	started := time.Now()
	for {
		found, err = clients.ServicePrincipalExists(ctx, id)
		if err != nil {
			t.Fatalf("asking whether the deleted service principal exists: %v", err)
		}
		if !found {
			t.Logf("the delete became visible to a read after %s", time.Since(started).Round(time.Millisecond))
			return
		}
		if time.Since(started) > bound {
			t.Fatalf("a service principal deleted %s ago still reports as present; "+
				"an identity removed in Databricks would go on being reported as usable",
				bound)
		}
		time.Sleep(time.Second)
	}
}

// until polls fn until it answers true, and fails the test naming what was
// waited for if it never does.
//
// Every read-back in this file needs it, and until one of them failed none of
// them looked like it did. A write becomes visible to a read when it does:
// measured on one account at 235ms, 245ms, 273ms, 1.5s and 2.2s, the longest and
// one of the shortest on the same day. A test that reads once passes for as long
// as the account is quick and then reports something it never measured -- one
// here did exactly that, and what it had been reporting was true, which is the
// worst version of it.
func until(t *testing.T, what string, fn func() bool) {
	t.Helper()
	began := time.Now()
	for attempt := range 60 {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		if fn() {
			if attempt > 0 {
				t.Logf("%s after %s", what, time.Since(began).Round(time.Millisecond))
			}
			return
		}
	}
	t.Fatalf("waited %s and never saw: %s", time.Since(began).Round(time.Millisecond), what)
}

// stable is until's opposite: it asserts that something goes on being true, by
// reading it several times over and failing on the first read that disagrees.
//
// An absence cannot be polled for the way a presence can. until stops at the
// first read that agrees, which for "no second policy was created" would be the
// first read taken before the account caught up -- and that read agrees for the
// wrong reason. So this keeps asking, and says which round it stopped being
// true on.
//
// The span still has to be longer than the account is slow; consecutive
// agreement proves nothing on its own. What the rounds buy is a failure that
// says when it flipped instead of one that says only that it had by the end,
// and a span written as how often, times how many, rather than as one number
// somebody liked.
//
// The lag it has to beat was measured on the account these run against: 235ms
// to 2.2s with nothing else happening, and 7.6s for the same call with these ten
// tests running at once. The busy figure is the one that matters and the one
// nobody takes.
const (
	stableInterval = 3 * time.Second
	stableRounds   = 5
)

func stable(t *testing.T, what string, fn func() bool) {
	t.Helper()
	began := time.Now()
	for round := range stableRounds {
		if round > 0 {
			time.Sleep(stableInterval)
		}
		if !fn() {
			t.Fatalf("%s stopped being true on round %d of %d, %s in",
				what, round+1, stableRounds, time.Since(began).Round(time.Millisecond))
		}
	}
}

func policiesOn(t *testing.T, clients Clients, id string) []oauth2.FederationPolicy {
	t.Helper()
	numeric, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		t.Fatalf("the id Databricks returned is not numeric: %q", id)
	}
	stored, err := clients.AccountClient().ServicePrincipalFederationPolicy.ListByServicePrincipalId(
		context.Background(), numeric)
	if err != nil {
		t.Fatalf("listing the federation policies on service principal %s: %v", id, err)
	}
	return stored.Policies
}

// TestLiveASecondPolicyDoesNotDisturbTheFirst covers two identities that end up
// on one service principal.
//
// The operator never does this itself, but it does not own the account. Somebody
// governing it can point a second cluster, or a person's own workload, at a
// service principal this operator made. If adding the second policy replaced the
// first rather than joining it, the workload this operator serves would stop
// being able to exchange, with nothing on this side changed and nothing saying
// why.
func TestLiveASecondPolicyDoesNotDisturbTheFirst(t *testing.T) {
	// Waiting is what these do: a write becomes visible when it does, and the
	// longest single wait measured here is seconds. They wait together.
	t.Parallel()

	clients := live(t)
	ctx := context.Background()

	namespace, name := "live-test", unique("two-policies")
	id, _ := scratch(t, clients, namespace, name)

	first := SubjectFor(namespace, name)
	second := SubjectFor(namespace, name+"-other")
	if err := clients.EnsureFederationPolicy(ctx, id, liveIssuer, first, "databricks"); err != nil {
		t.Fatalf("writing the first policy: %v", err)
	}
	if err := clients.EnsureFederationPolicy(ctx, id, liveIssuer, second, "databricks"); err != nil {
		t.Fatalf("writing a second policy for a different subject: %v", err)
	}

	subjects := map[string]bool{}
	until(t, "both policies are listed", func() bool {
		subjects = map[string]bool{}
		for _, policy := range policiesOn(t, clients, id) {
			if policy.OidcPolicy != nil {
				subjects[policy.OidcPolicy.Subject] = true
			}
		}
		return len(subjects) == 2
	})
	if !subjects[first] {
		t.Errorf("the first subject is gone after a second policy was added; subjects are %v", subjects)
	}
	if !subjects[second] {
		t.Errorf("the second subject was not stored; subjects are %v", subjects)
	}
}

// TestLiveAPolicyIsAcceptedForASubjectNothingHasEverIssued covers what Databricks
// checks about a subject, which is nothing.
//
// It matters twice. It is why the operator can write the policy before any pod
// exists -- there is no ordering to get right. And it is why a service principal
// whose ServiceAccount has been deleted goes on trusting that name: Databricks
// will not notice, so the operator deleting the service principal is the whole
// of revocation, and a ServiceAccount recreated with the same name would inherit
// the old identity if it ever did not.
func TestLiveAPolicyIsAcceptedForASubjectNothingHasEverIssued(t *testing.T) {
	// Waiting is what these do: a write becomes visible when it does, and the
	// longest single wait measured here is seconds. They wait together.
	t.Parallel()

	clients := live(t)
	ctx := context.Background()

	namespace, name := "live-test", unique("no-such-subject")
	id, _ := scratch(t, clients, namespace, name)

	subject := SubjectFor("a-namespace-that-does-not-exist", "nor-does-this")
	if err := clients.EnsureFederationPolicy(ctx, id, liveIssuer, subject, "databricks"); err != nil {
		t.Fatalf("a policy for a subject nothing has issued was refused: %v -- "+
			"the operator writes the policy before any pod exists, so an ordering "+
			"requirement here would be a real one", err)
	}
}

// TestLiveTwoServicePrincipalsMayShareADisplayName covers the reason nothing
// finds a service principal by its name.
//
// DisplayNameFor truncates at 100 characters, so two long subjects produce the
// same name, and the display name is chosen on this side rather than assigned.
// If Databricks refused a duplicate, that truncation would be a failure a
// workload could trigger by being named inconveniently. It does not, which is
// also why the display name can never be used to identify one.
func TestLiveTwoServicePrincipalsMayShareADisplayName(t *testing.T) {
	// Waiting is what these do: a write becomes visible when it does, and the
	// longest single wait measured here is seconds. They wait together.
	t.Parallel()

	clients := live(t)
	ctx := context.Background()

	namespace, name := "live-test", unique("shared-name")
	first, _ := scratch(t, clients, namespace, name)

	// The same namespace and name, so the same display name, deliberately.
	t.Cleanup(func() {
		// Both are removed by display name, and the first is registered by
		// scratch already -- this runs before it and takes whichever it finds.
		removeByDisplayName(t, clients, DisplayNameFor(namespace, name, ""))
	})
	// A different uid, because this is two issuings that collide on the display
	// name rather than one issuing made twice.
	second, _, err := clients.CreateServicePrincipal(ctx, Issuing{
		Issuer: liveIssuer, Namespace: namespace, Name: name,
		ServiceAccountUID: string(uuid.NewUUID()),
	})
	if err != nil {
		t.Fatalf("a second service principal with the same display name was refused: %v -- "+
			"display names are truncated to fit, so two long subjects collide and this "+
			"would make that a failure", err)
	}
	if second == first {
		t.Errorf("both creates returned id %s; they are meant to be two identities", first)
	}
}

// TestLiveDeletingAServicePrincipalTakesItsPoliciesWithIt covers what revoke
// relies on.
//
// Deleting the service principal is the whole of ending a workload's access:
// the operator removes nothing else and reads no lists. A federation policy that
// outlived its service principal would be a trust left standing for a subject
// this side has stopped recording.
func TestLiveDeletingAServicePrincipalTakesItsPoliciesWithIt(t *testing.T) {
	// Waiting is what these do: a write becomes visible when it does, and the
	// longest single wait measured here is seconds. They wait together.
	t.Parallel()

	clients := live(t)
	ctx := context.Background()

	namespace, name := "live-test", unique("policies-go-too")
	id, _ := scratch(t, clients, namespace, name)
	if err := clients.EnsureFederationPolicy(ctx, id, liveIssuer,
		SubjectFor(namespace, name), "databricks"); err != nil {
		t.Fatalf("writing the federation policy: %v", err)
	}
	if len(policiesOn(t, clients, id)) != 1 {
		t.Fatal("the policy this test needs was not written")
	}

	if err := clients.DeleteServicePrincipal(ctx, id); err != nil {
		t.Fatalf("deleting the service principal: %v", err)
	}

	// Either answer proves it: the policies are gone, or the service principal
	// they hung off is, which is the same thing from here.
	//
	// Polled rather than read once. A delete becomes visible to a read when it
	// does -- measured at 245ms on one day and 1.5s on another -- and reading
	// immediately measures that rather than what this test is about.
	numeric, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		t.Fatalf("the id Databricks returned is not numeric: %q", id)
	}
	began := time.Now()
	var left int
	for attempt := range 60 {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		stored, err := clients.AccountClient().ServicePrincipalFederationPolicy.ListByServicePrincipalId(ctx, numeric)
		if err != nil {
			t.Logf("listing the policies of a deleted service principal fails after %s, "+
				"which is the answer: %v", time.Since(began).Round(time.Millisecond), err)
			return
		}
		if left = len(stored.Policies); left == 0 {
			t.Logf("the policies went with it after %s", time.Since(began).Round(time.Millisecond))
			return
		}
	}
	t.Errorf("%d federation policies outlived the service principal they were written on by %s; "+
		"deleting it is the whole of revocation and would not be",
		left, time.Since(began).Round(time.Millisecond))
}

// TestLiveTheMarkerComesBackOnAListing covers the lookup that now stands between
// a crash and an identity nothing records.
//
// The record is written before the service principal is created, so a pass that
// creates one and then stops leaves a record naming no id. Finding it again is
// the only way that service principal is ever deleted, and the whole of it rests
// on two things about Databricks that the SDK's types do not say: that a listing
// filtered by display name answers, and that the externalId written at create
// comes back on the items of that listing rather than only on a Get.
//
// If either is not so, every recovery silently finds nothing -- and then makes a
// second service principal, leaving the first behind forever.
func TestLiveTheMarkerComesBackOnAListing(t *testing.T) {
	// Waiting is what these do: a write becomes visible when it does, and the
	// longest single wait measured here is seconds. They wait together.
	t.Parallel()

	clients := live(t)
	ctx := context.Background()

	namespace, name := "live-test", unique("marker-round-trip")
	// Registered before either create, and by display name, because a create
	// that returns an error has still created the service principal.
	t.Cleanup(func() { removeByDisplayName(t, clients, DisplayNameFor(namespace, name, "")) })

	// Two ServiceAccounts of the same name, one after the other: the namespace
	// was recreated, or the ServiceAccount was. Their display names are one
	// string and their subjects are one string, so the marker is the only thing
	// that can tell them apart.
	mine := Issuing{Issuer: liveIssuer, Namespace: namespace, Name: name,
		ServiceAccountUID: string(uuid.NewUUID())}
	theirs := Issuing{Issuer: liveIssuer, Namespace: namespace, Name: name,
		ServiceAccountUID: string(uuid.NewUUID())}

	mineID, mineClientID, err := clients.CreateServicePrincipal(ctx, mine)
	if err != nil {
		t.Fatalf("creating the first: %v", err)
	}
	theirsID, _, err := clients.CreateServicePrincipal(ctx, theirs)
	if err != nil {
		t.Fatalf("creating the second: %v", err)
	}

	var id, clientID string
	var found bool
	until(t, "the service principal just created is found by its marker -- if this never "+
		"happens, either the displayName filter does not answer or the externalId written at "+
		"create does not come back on the listing, and an identity created by a pass that then "+
		"stopped is never found again", func() bool {
		if id, clientID, found, err = clients.FindServicePrincipal(ctx, mine); err != nil {
			t.Fatalf("looking for what was just created: %v", err)
		}
		return found
	})

	if id != mineID {
		t.Errorf("found %s, want %s -- two service principals share this display name and the "+
			"marker is what says which is which; handing over the wrong one gives a workload "+
			"an identity that belongs to whoever held these names before it", id, mineID)
	}
	if clientID != mineClientID {
		t.Errorf("found client id %s, want %s", clientID, mineClientID)
	}

	// The other one, for the same reason in the other direction. Waited for
	// separately: the two creates are two writes and become visible separately.
	var theirsFound string
	until(t, "the second service principal is found by its own marker", func() bool {
		found, _, ok, err := clients.FindServicePrincipal(ctx, theirs)
		if err != nil {
			t.Fatalf("looking for the second: %v", err)
		}
		theirsFound = found
		return ok
	})
	if theirsFound != theirsID {
		t.Errorf("found %s for the second, want %s -- one marker is being matched for both",
			theirsFound, theirsID)
	}

	// And a third ServiceAccount of the same names, which this operator has
	// issued nothing to. The names match two service principals in the account
	// and the marker matches neither.
	//
	// Asserted after both of those are listed, deliberately: before that, "not
	// found" is also what a listing that has not caught up answers, and this
	// would pass for the wrong reason.
	never := Issuing{Issuer: liveIssuer, Namespace: namespace, Name: name,
		ServiceAccountUID: string(uuid.NewUUID())}
	switch _, _, found, err := clients.FindServicePrincipal(ctx, never); {
	case err != nil:
		t.Fatalf("looking for one that was never created: %v", err)
	case found:
		t.Error("a ServiceAccount that was issued nothing was handed one of these; the lookup " +
			"is matching on the display name alone, which two different subjects can share")
	}
}

// TestLiveTwoServicePrincipalsMayTrustOneSubject covers a state this design
// creates on purpose and the one before it never could.
//
// A ServiceAccount deleted and recreated under its own name is a different
// ServiceAccount, so it is issued its own service principal -- and its subject
// is character for character the one before it, because a subject is
// system:serviceaccount:<namespace>:<name> and both names came back. For at
// least one pass the account holds two service principals whose federation
// policies name the same subject: the new one is created and its policy written
// before, or while, the old one is being deleted.
//
// The shape before this one could not reach that state. One subject meant one
// service principal, always, because the object was named after the
// ServiceAccount and a new one adopted what the old one had -- which is exactly
// the inheritance this design exists to end.
//
// If Databricks held (issuer, subject) unique across an account, every recreated
// ServiceAccount would be issued a service principal whose policy is refused,
// and the workload would carry a token nothing accepts. Measured 2026-09-02: it
// does not, and an exchange names the service principal it wants with
// client_id, so there is nothing to disambiguate. This is what keeps that true.
func TestLiveTwoServicePrincipalsMayTrustOneSubject(t *testing.T) {
	// Waiting is what these do: a write becomes visible when it does, and the
	// longest single wait measured here is seconds. They wait together.
	t.Parallel()

	clients := live(t)
	ctx := context.Background()

	namespace, name := "live-test", unique("one-subject")
	subject := SubjectFor(namespace, name)

	before, _ := scratch(t, clients, namespace, name)
	if err := clients.EnsureFederationPolicy(ctx, before, liveIssuer, subject, "databricks"); err != nil {
		t.Fatalf("writing the first policy: %v", err)
	}

	// The same names and a different uid: the ServiceAccount was recreated.
	t.Cleanup(func() { removeByDisplayName(t, clients, DisplayNameFor(namespace, name, "")) })
	after, _, err := clients.CreateServicePrincipal(ctx, Issuing{
		Issuer: liveIssuer, Namespace: namespace, Name: name,
		ServiceAccountUID: string(uuid.NewUUID()),
	})
	if err != nil {
		t.Fatalf("creating the identity of the ServiceAccount that replaced it: %v", err)
	}

	if err := clients.EnsureFederationPolicy(ctx, after, liveIssuer, subject, "databricks"); err != nil {
		t.Fatalf("a second service principal was refused a policy for a subject another one "+
			"already trusts: %v -- every recreated ServiceAccount would then be issued an "+
			"identity no token can be exchanged for, until whatever it replaced is deleted",
			err)
	}

	// And the old one's deletion, which is what happens next, leaves the new
	// one's policy alone. Deleting a service principal takes its own policies
	// with it; this is the assertion that it takes only its own.
	if err := clients.DeleteServicePrincipal(ctx, before); err != nil {
		t.Fatalf("deleting the identity of the ServiceAccount that is gone: %v", err)
	}

	// Asserted by going on reading. What is being claimed is that something did
	// not happen to the second policy, and a single read taken before the delete
	// is visible would say so whatever the delete did.
	stable(t, fmt.Sprintf("service principal %s goes on trusting %s while %s is deleted -- if it "+
		"stops, the recreated ServiceAccount's identity is destroyed by the cleanup of the one "+
		"it replaced, and nothing reports it", after, subject, before), func() bool {
		for _, policy := range policiesOn(t, clients, after) {
			if policy.OidcPolicy != nil && policy.OidcPolicy.Subject == subject {
				return true
			}
		}
		return false
	})
}
