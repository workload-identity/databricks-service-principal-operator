package databricks

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/databricks/databricks-sdk-go"
)

// ErrNotConfigured reports that no usable DatabricksAccount has been read yet.
//
// It is not a failure of whatever was being asked. Nothing has been asked:
// there is nobody to ask as.
type ErrNotConfigured struct {
	// Name and Namespace are the DatabricksAccount the operator is looking for,
	// so the message names the object to create rather than describing a state.
	Name      string
	Namespace string

	// Reason is why there is nothing usable, when something is known.
	//
	// Empty means nothing has been installed yet, and the message tells the
	// reader to create the object. That reading is wrong whenever the object
	// exists and stopped being usable -- it was deleted, or it was repointed at
	// an account that does not verify -- and sending somebody to create an
	// object they are looking at is worse than saying nothing. Whoever withdrew
	// the clients knows which of those it was, so it says.
	Reason string
}

func (e *ErrNotConfigured) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("DatabricksAccount %s/%s is not usable: %s", e.Namespace, e.Name, e.Reason)
	}
	return fmt.Sprintf("no usable DatabricksAccount %s/%s: the operator does not know which Databricks account to act in",
		e.Namespace, e.Name)
}

// notConfigured reports whether err is the holder having nothing yet.
func notConfigured(err error) bool {
	var e *ErrNotConfigured
	return errors.As(err, &e)
}

// Holder is the Clients the controllers hold, standing in for the ones built
// from a DatabricksAccount that may not exist yet, or may have changed.
//
// The controllers take a Clients and do not know configuration can move
// underneath them. That is deliberate: which account the operator acts in is one
// controller's concern, and threading "is it configured yet" through every call
// site would put that question in several more places, each free to answer it
// differently.
//
// Before anything is set, every method fails with ErrNotConfigured, which
// classifies as NotConfigured and so is reported as a wait rather than as a
// wrong declaration. Nothing crashes and nothing is retried into backoff.
type Holder struct {
	// name and namespace are fixed at construction: they identify the
	// DatabricksAccount this operator was told to use, and appear in the error
	// so that whoever reads a condition is told what to create.
	name      string
	namespace string

	mu sync.RWMutex
	// clients is what is installed, and builtFrom is the declaration it was
	// built from. They move together and are only ever read together.
	clients   Clients
	builtFrom Config
	// reason is why clients is nil, when somebody withdrew them for a reason.
	reason string
}

// NewHolder returns a Holder for the named DatabricksAccount, holding nothing.
func NewHolder(namespace, name string) *Holder {
	return &Holder{name: name, namespace: namespace}
}

// Set installs the clients built from one declaration, replacing any earlier
// ones, and remembers which declaration they came from.
//
// The declaration is kept so that BuiltFrom can answer whether what is installed
// is still what the spec says. Nothing else compares them, and nothing else can:
// the clients themselves cannot say whether the object they were built from has
// since been edited.
func (h *Holder) Set(builtFrom Config, clients Clients) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients = clients
	h.builtFrom = builtFrom
	h.reason = ""
}

// BuiltFrom is the declaration the installed clients were built from, and
// whether anything is installed at all.
func (h *Holder) BuiltFrom() (Config, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.builtFrom, h.clients != nil
}

// Clear drops the clients, so everything reports NotConfigured again, saying
// why.
//
// Two things call it, and the difference between them is the whole of when it is
// right to call it at all.
//
// The account being deleted: there is nothing to act in.
//
// The declaration changing: what is installed was built from a declaration
// nobody makes any more, and keeping it means acting in an account the spec does
// not name -- creating identities there while the object on screen names another
// and reports that it could not be verified. Nothing says so, and a restart
// changes the answer, because what is installed lives in memory.
//
// It is deliberately not called when a reconcile of the *same* declaration
// fails. The clients already installed may still work, and withdrawing them
// would take every workload's identity out of reach over one bad call.
func (h *Holder) Clear(reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients = nil
	h.builtFrom = Config{}
	h.reason = reason
}

// Configured reports whether any clients have been installed. It exists for the
// account controller's own status, not for the others, which learn the same
// thing from the error they get back.
func (h *Holder) Configured() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.clients != nil
}

// Snapshot is one caller's view of the clients, taken once and not taken again.
//
// It exists because the Holder can be replaced between two of its own methods.
// A pass that asks "which account am I in", is answered A, and then makes a call
// that goes to B has checked one thing and done another -- and the answers to
// that are not merely wrong: a lookup in the wrong account returns the same 404
// as an identity that was deleted, which latches a record that never rebuilds,
// and a create in one account stamped with the other's id is an identity nothing
// can ever find. Both are unrecoverable and neither says anything at the time.
//
// So a pass takes this once at the top and uses it throughout. Whichever account
// it holds, the whole pass happens in that one, and a declaration that changes
// takes effect from the next pass.
//
// It is total: with nothing installed it returns a stand-in whose every call
// reports ErrNotConfigured, so a caller has one thing to hold and no case to
// handle.
func (h *Holder) Snapshot() Clients {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.clients == nil {
		return unconfigured{name: h.name, namespace: h.namespace, reason: h.reason}
	}
	return h.clients
}

var _ Clients = (*Holder)(nil)

// The methods below are the Holder standing in for whatever is installed right
// now, for callers that make a single call. Anything making more than one in a
// row takes a Snapshot instead: these each read the Holder again, and what they
// read can change between them.

func (h *Holder) AccountClient() *databricks.AccountClient { return h.Snapshot().AccountClient() }
func (h *Holder) AccountID() string                        { return h.Snapshot().AccountID() }

func (h *Holder) FindServicePrincipal(ctx context.Context, issuing Issuing) (string, string, bool, error) {
	return h.Snapshot().FindServicePrincipal(ctx, issuing)
}

func (h *Holder) CreateServicePrincipal(ctx context.Context, issuing Issuing) (string, string, error) {
	return h.Snapshot().CreateServicePrincipal(ctx, issuing)
}

func (h *Holder) ServicePrincipalExists(ctx context.Context, servicePrincipalID string) (bool, error) {
	return h.Snapshot().ServicePrincipalExists(ctx, servicePrincipalID)
}

func (h *Holder) DeleteServicePrincipal(ctx context.Context, servicePrincipalID string) error {
	return h.Snapshot().DeleteServicePrincipal(ctx, servicePrincipalID)
}

func (h *Holder) EnsureFederationPolicy(ctx context.Context, servicePrincipalID, issuer, subject, audience string) error {
	return h.Snapshot().EnsureFederationPolicy(ctx, servicePrincipalID, issuer, subject, audience)
}

func (h *Holder) RemoveFederationPolicies(ctx context.Context, servicePrincipalID, issuer, subject string) error {
	return h.Snapshot().RemoveFederationPolicies(ctx, servicePrincipalID, issuer, subject)
}

// unconfigured is what a Snapshot holds when nothing has been installed.
//
// Every call reports which DatabricksAccount the operator is looking for, so a
// condition written from it names the object rather than describing a state.
// AccountID is empty, which reads the same as an identity that has recorded
// none: nothing to compare, so nothing concluded.
type unconfigured struct {
	name      string
	namespace string
	reason    string
}

var _ Clients = unconfigured{}

func (u unconfigured) err() error {
	return &ErrNotConfigured{Name: u.name, Namespace: u.namespace, Reason: u.reason}
}

func (u unconfigured) Snapshot() Clients                        { return u }
func (u unconfigured) AccountClient() *databricks.AccountClient { return nil }
func (u unconfigured) AccountID() string                        { return "" }

func (u unconfigured) FindServicePrincipal(context.Context, Issuing) (string, string, bool, error) {
	return "", "", false, u.err()
}

func (u unconfigured) CreateServicePrincipal(context.Context, Issuing) (string, string, error) {
	return "", "", u.err()
}

func (u unconfigured) ServicePrincipalExists(context.Context, string) (bool, error) {
	return false, u.err()
}

func (u unconfigured) DeleteServicePrincipal(context.Context, string) error { return u.err() }

func (u unconfigured) EnsureFederationPolicy(context.Context, string, string, string, string) error {
	return u.err()
}

func (u unconfigured) RemoveFederationPolicies(context.Context, string, string, string) error {
	return u.err()
}
