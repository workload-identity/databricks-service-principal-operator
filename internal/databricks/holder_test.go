package databricks

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/databricks/databricks-sdk-go"
)

// namingClients answers every call by recording which one it was.
//
// The Holder is twenty-nine methods that each do the same two things: ask for
// the clients, and pass the call on. That shape is where a copy-paste goes
// unnoticed -- a delegate that reaches the neighbouring call still compiles,
// still returns the right types, and is wrong in a way no signature catches.
// LeaveAccountGroup calling JoinAccountGroup would put a workload back into the
// group it was being taken out of, and report success.
type namingClients struct {
	called string
}

func (n *namingClients) name(what string) { n.called = what }

func (n *namingClients) Account() *databricks.AccountClient { n.name("Account"); return nil }

func (n *namingClients) Snapshot() Clients { n.name("Snapshot"); return n }

func (n *namingClients) AccountID() string { n.name("AccountID"); return "" }

func (n *namingClients) CreateServicePrincipal(context.Context, Issuing) (string, string, error) {
	n.name("CreateServicePrincipal")
	return "", "", nil
}

func (n *namingClients) FindServicePrincipal(context.Context, Issuing) (
	string, string, bool, error) {
	n.name("FindServicePrincipal")
	return "", "", false, nil
}

func (n *namingClients) ServicePrincipalExists(context.Context, string) (bool, error) {
	n.name("ServicePrincipalExists")
	return false, nil
}

func (n *namingClients) DeleteServicePrincipal(context.Context, string) error {
	n.name("DeleteServicePrincipal")
	return nil
}

func (n *namingClients) EnsureFederationPolicy(context.Context, string, string, string, string) error {
	n.name("EnsureFederationPolicy")
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

// delegates is every call the Holder passes on, with the name it must reach.
//
// It is built by reflection over the Clients interface so that a method added
// later and mis-delegated fails here rather than in production. A hand-written
// table is complete only on the day it is written.
func delegates(t *testing.T) map[string]func(Clients) error {
	t.Helper()
	ctx := context.Background()
	table := map[string]func(Clients) error{
		"CreateServicePrincipal": func(c Clients) error {
			_, _, err := c.CreateServicePrincipal(ctx, testIssuing)
			return err
		},
		"FindServicePrincipal": func(c Clients) error {
			_, _, _, err := c.FindServicePrincipal(ctx, testIssuing)
			return err
		},
		"ServicePrincipalExists": func(c Clients) error {
			_, err := c.ServicePrincipalExists(ctx, "7788")
			return err
		},
		"DeleteServicePrincipal": func(c Clients) error {
			return c.DeleteServicePrincipal(ctx, "7788")
		},
		"EnsureFederationPolicy": func(c Clients) error {
			return c.EnsureFederationPolicy(ctx, "7788", "iss", "sub", "aud")
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

// TestEveryHolderCallReachesItsNamesake covers the delegation itself.
//
// The Holder is a method per call, each doing the same two things: ask for the
// clients, and pass the call on. That shape is where a copy-paste goes
// unnoticed -- a delegate reaching the neighbouring call still compiles, still
// returns the right types, and is wrong in a way no signature catches.
func TestEveryHolderCallReachesItsNamesake(t *testing.T) {
	t.Parallel()
	for want, invoke := range delegates(t) {
		t.Run(want, func(t *testing.T) {
			inner := &namingClients{}
			holder := NewHolder("databricks", "account")
			holder.Set(Config{AccountID: "an-account"}, inner)

			if err := invoke(holder); err != nil {
				t.Fatal(err)
			}
			if inner.called != want {
				t.Errorf("reached %s, want %s", inner.called, want)
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

	for name, invoke := range delegates(t) {
		t.Run(name, func(t *testing.T) {
			var unconfigured *ErrNotConfigured
			if err := invoke(holder); !errors.As(err, &unconfigured) {
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
