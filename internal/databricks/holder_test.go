package databricks

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/databricks/databricks-sdk-go"
)

// namingClients answers every call by recording which one it was and what it
// was given.
//
// Both are recorded because both can be wrong with nothing to say so. The Holder
// is a method per call, each asking for the clients and passing the call on, and
// EnsureFederationPolicy holds a service principal id and returns a plain error
// -- which is exactly what DeleteServicePrincipal takes and returns, so a
// delegate that reached the neighbour compiles, destroys the identity it was
// asked to make exchangeable, and reports the pass as done.
//
// EnsureFederationPolicy also takes four strings in a row, so two of them handed
// on transposed compiles as well, reaches the right call, and writes a policy
// that trusts something nobody asked to trust.
type namingClients struct {
	called string
	given  []any
}

func (n *namingClients) name(what string, given ...any) { n.called, n.given = what, given }

func (n *namingClients) Account() *databricks.AccountClient { n.name("Account"); return nil }

func (n *namingClients) Snapshot() Clients { n.name("Snapshot"); return n }

func (n *namingClients) AccountID() string { n.name("AccountID"); return "" }

func (n *namingClients) CreateServicePrincipal(_ context.Context, issuing Issuing) (
	string, string, error) {
	n.name("CreateServicePrincipal", issuing)
	return "", "", nil
}

func (n *namingClients) FindServicePrincipal(_ context.Context, issuing Issuing) (
	string, string, bool, error) {
	n.name("FindServicePrincipal", issuing)
	return "", "", false, nil
}

func (n *namingClients) ServicePrincipalExists(_ context.Context, id string) (bool, error) {
	n.name("ServicePrincipalExists", id)
	return false, nil
}

func (n *namingClients) DeleteServicePrincipal(_ context.Context, id string) error {
	n.name("DeleteServicePrincipal", id)
	return nil
}

func (n *namingClients) EnsureFederationPolicy(
	_ context.Context, servicePrincipalID, issuer, subject, audience string) error {
	n.name("EnsureFederationPolicy", servicePrincipalID, issuer, subject, audience)
	return nil
}

var _ Clients = (*namingClients)(nil)

// testIssuing is one issuing, for calls whose arguments are not what is being
// tested.
var testIssuing = Issuing{
	Issuer:            "https://oidc.example",
	Namespace:         "team-a",
	Name:              "etl",
	ServiceAccountUID: "6a5f0d1e-0b2c-4c3d-9e8f-1a2b3c4d5e6f",
}

// delegate is one call the Holder passes on: how to make it, and what has to
// arrive on the other side unchanged and in the order it was given.
type delegate struct {
	invoke func(Clients) error
	given  []any
}

// delegates is every call the Holder passes on, with the name it must reach.
//
// It is built by reflection over the Clients interface so that a method added
// later and mis-delegated fails here rather than in production. A hand-written
// table is complete only on the day it is written.
//
// Every argument is a value no other position could hold, so that a delegate
// handing two of them on the other way round is a mismatch rather than two
// strings that happen to look alike.
func delegates(t *testing.T) map[string]delegate {
	t.Helper()
	ctx := context.Background()
	table := map[string]delegate{
		"CreateServicePrincipal": {
			invoke: func(c Clients) error {
				_, _, err := c.CreateServicePrincipal(ctx, testIssuing)
				return err
			},
			given: []any{testIssuing},
		},
		"FindServicePrincipal": {
			invoke: func(c Clients) error {
				_, _, _, err := c.FindServicePrincipal(ctx, testIssuing)
				return err
			},
			given: []any{testIssuing},
		},
		"ServicePrincipalExists": {
			invoke: func(c Clients) error {
				_, err := c.ServicePrincipalExists(ctx, "7788")
				return err
			},
			given: []any{"7788"},
		},
		"DeleteServicePrincipal": {
			invoke: func(c Clients) error {
				return c.DeleteServicePrincipal(ctx, "7788")
			},
			given: []any{"7788"},
		},
		"EnsureFederationPolicy": {
			invoke: func(c Clients) error {
				return c.EnsureFederationPolicy(ctx, "7788", testIssuer, testSubject, testAudience)
			},
			given: []any{"7788", testIssuer, testSubject, testAudience},
		},
	}

	// Account and AccountID return no error, so neither can be driven by a table
	// keyed on one. Snapshot is not a call that is passed on at all: it answers
	// about the Holder itself, and with nothing installed it returns a stand-in
	// rather than an error, which is the whole of what makes a caller able to
	// hold one thing. All three are covered separately below; everything else
	// has to be here.
	iface := reflect.TypeFor[Clients]()
	for method := range iface.Methods() {
		name := method.Name
		if name == "Account" || name == "AccountID" || name == "Snapshot" {
			continue
		}
		if _, ok := table[name]; !ok {
			t.Fatalf("Clients has a method %s that this table does not exercise; "+
				"a delegate that reaches the wrong call still compiles", name)
		}
	}
	return table
}

// TestEveryHolderCallReachesItsNamesakeWithWhatItWasGiven covers the delegation
// itself, which is a call and a list of arguments and nothing else.
//
// A delegate reaching the neighbouring call and one handing the arguments on in
// another order both compile and both return the right types, so nothing but
// this says either is wrong. What each of them costs is in namingClients.
func TestEveryHolderCallReachesItsNamesakeWithWhatItWasGiven(t *testing.T) {
	t.Parallel()
	for want, call := range delegates(t) {
		t.Run(want, func(t *testing.T) {
			inner := &namingClients{}
			holder := NewHolder("databricks", "account")
			holder.Set(Config{AccountID: "an-account"}, inner)

			if err := call.invoke(holder); err != nil {
				t.Fatal(err)
			}
			if inner.called != want {
				t.Errorf("reached %s, want %s", inner.called, want)
			}
			if !reflect.DeepEqual(inner.given, call.given) {
				t.Errorf("%s was given %#v, want %#v", want, inner.given, call.given)
			}
		})
	}
}

// TestNothingConfiguredIsSaidRatherThanCrashed covers what every call does
// before the account exists.
//
// The operator starts before the DatabricksAccount is reconciled, and
// everything else reconciles in the meantime. Each call has to come back with an
// error naming the object somebody has to create -- not a nil dereference, and
// not an error that reads as though Databricks refused something.
func TestNothingConfiguredIsSaidRatherThanCrashed(t *testing.T) {
	t.Parallel()
	holder := NewHolder("databricks", "account")
	if holder.Configured() {
		t.Error("reports configured while holding nothing")
	}
	if holder.Account() != nil {
		t.Error("handed out an account client while holding nothing")
	}

	for name, call := range delegates(t) {
		t.Run(name, func(t *testing.T) {
			var unconfigured *ErrNotConfigured
			if err := call.invoke(holder); !errors.As(err, &unconfigured) {
				t.Fatalf("error is %v, want ErrNotConfigured", err)
			}
			if unconfigured.Error() == "" {
				t.Error("the error says nothing about what to create")
			}
		})
	}
}

// TestClearingSendsEverythingBackToNotConfigured covers the account being
// deleted.
//
// Clear is for that and only that. It is deliberately not called when a
// reconcile of the account fails, because the clients already installed may
// still work and withdrawing them would take every workload's identity out of
// reach over one bad call.
func TestClearingSendsEverythingBackToNotConfigured(t *testing.T) {
	t.Parallel()
	holder := NewHolder("databricks", "account")
	holder.Set(Config{AccountID: "an-account"}, &namingClients{})
	if !holder.Configured() {
		t.Fatal("reports nothing configured after Set")
	}

	holder.Clear("the account was deleted")
	if holder.Configured() {
		t.Error("still reports configured after Clear")
	}
	var unconfigured *ErrNotConfigured
	if err := holder.DeleteServicePrincipal(context.Background(), "7788"); !errors.As(err, &unconfigured) {
		t.Errorf("error is %v, want ErrNotConfigured", err)
	}

	// And it says why, rather than telling somebody to create an object they
	// are looking at. The default message names the DatabricksAccount as
	// something to make; that reading is wrong for every reason it stops being
	// usable after having been one.
	if !strings.Contains(unconfigured.Error(), "the account was deleted") {
		t.Errorf("the error is %q; whoever withdrew the clients knew why and this does not say",
			unconfigured.Error())
	}
	if _, ok := holder.BuiltFrom(); ok {
		t.Error("still reports a declaration it was built from after Clear")
	}
}

// TestWhatIsInstalledRemembersWhatItWasBuiltFrom covers the comparison that
// decides whether installed clients are still an answer to anything.
//
// Nothing else can make it. The clients cannot say whether the object they came
// from has since been edited, and without it an operator repointed at an account
// that fails verification goes on acting in the old one -- while the object on
// screen names the new one and reports the failure, so that a reader concludes
// nothing is happening.
func TestWhatIsInstalledRemembersWhatItWasBuiltFrom(t *testing.T) {
	t.Parallel()
	holder := NewHolder("databricks", "account")
	if _, ok := holder.BuiltFrom(); ok {
		t.Error("reports a declaration while holding nothing")
	}

	first := Config{AccountHost: "https://accounts.example", AccountID: "one", ClientID: "app"}
	holder.Set(first, &namingClients{})

	installed, ok := holder.BuiltFrom()
	if !ok {
		t.Fatal("reports nothing installed after Set")
	}
	if installed != first {
		t.Errorf("built from %+v, want %+v", installed, first)
	}

	// Every field is part of the answer. The account is the obvious one; the
	// client id decides which service principal the operator acts as, and the
	// host decides where it goes.
	for _, changed := range []Config{
		{AccountHost: "https://accounts.example", AccountID: "two", ClientID: "app"},
		{AccountHost: "https://accounts.example", AccountID: "one", ClientID: "another-app"},
		{AccountHost: "https://elsewhere.example", AccountID: "one", ClientID: "app"},
	} {
		if installed == changed {
			t.Errorf("%+v compares equal to %+v; the operator would go on acting in what it "+
				"already had", changed, installed)
		}
	}
}

// TestAccountIDIsEmptyRatherThanWrongWhenNothingIsConfigured covers the one
// delegate that cannot report a failure.
//
// Every other call answers ErrNotConfigured, and the caller reports it. This one
// returns a string, and the string is compared against what an identity recorded
// to decide whether a 404 means the identity was deleted. An unconfigured Holder
// answering with anything but empty would make that comparison say "different
// account" or "same account" on no evidence at all; empty is the value that
// means neither, and the comparison declines to conclude.
func TestAccountIDIsEmptyRatherThanWrongWhenNothingIsConfigured(t *testing.T) {
	t.Parallel()
	holder := NewHolder("operators", "databricks-account")

	if got := holder.AccountID(); got != "" {
		t.Errorf("AccountID is %q with no clients installed, want empty", got)
	}
}

// TestAccountIDReachesTheClients is the other half: when there are clients, it
// is theirs and not something the Holder made up.
func TestAccountIDReachesTheClients(t *testing.T) {
	t.Parallel()
	naming := &namingClients{}
	holder := NewHolder("operators", "databricks-account")
	holder.Set(Config{AccountID: "an-account"}, naming)

	holder.AccountID()
	if naming.called != "AccountID" {
		t.Errorf("the clients were asked %q, want AccountID", naming.called)
	}
}

// TestASnapshotDoesNotChangeUnderItsCaller covers the reason Snapshot exists.
//
// The Holder is replaced by the account controller while every other controller
// is running. A pass that asks the Holder twice can be answered about two
// different accounts, and the two answers it then acts on are not merely wrong:
// a lookup in the wrong account returns the same 404 as an identity somebody
// deleted, which latches a record that never rebuilds, and a create in one
// account stamped with the other's id is an identity nothing can find again.
func TestASnapshotDoesNotChangeUnderItsCaller(t *testing.T) {
	t.Parallel()
	before := &namingClients{}
	holder := NewHolder("databricks", "account")
	holder.Set(Config{AccountID: "an-account"}, before)

	taken := holder.Snapshot()

	// What a pass does next: the account controller installs another account's
	// clients, or withdraws them entirely.
	after := &namingClients{}
	holder.Set(Config{AccountID: "an-account"}, after)

	if err := taken.DeleteServicePrincipal(context.Background(), "7788"); err != nil {
		t.Fatal(err)
	}
	if before.called != "DeleteServicePrincipal" {
		t.Errorf("the call went to the clients installed afterwards; the pass checked one "+
			"account and acted in another (before=%q after=%q)", before.called, after.called)
	}

	holder.Clear("the account was deleted")
	before.called = ""
	if err := taken.DeleteServicePrincipal(context.Background(), "7788"); err != nil {
		t.Fatalf("a snapshot stopped working when the Holder was cleared: %v", err)
	}
	if before.called != "DeleteServicePrincipal" {
		t.Error("clearing the Holder reached into a snapshot already taken")
	}
}

// TestASnapshotOfNothingReportsWhatToCreate covers the other half: a pass that
// starts before there is an account still has one thing to hold.
//
// Returning nil would put a check at every call site, each free to answer it
// differently. Returning a stand-in that reports ErrNotConfigured everywhere
// keeps that question in one place.
func TestASnapshotOfNothingReportsWhatToCreate(t *testing.T) {
	t.Parallel()
	taken := NewHolder("databricks", "account").Snapshot()
	if taken == nil {
		t.Fatal("a snapshot of nothing is nil; every call site has to check")
	}
	if taken.AccountID() != "" {
		t.Errorf("account id is %q while nothing is installed; something would compare against it",
			taken.AccountID())
	}

	var unconfigured *ErrNotConfigured
	if err := taken.DeleteServicePrincipal(context.Background(), "7788"); !errors.As(err, &unconfigured) {
		t.Fatalf("error is %v, want ErrNotConfigured", err)
	}
	if unconfigured.Error() == "" {
		t.Error("the error says nothing about what to create")
	}
}
