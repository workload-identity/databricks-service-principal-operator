package databricks

import (
	"context"
	"fmt"

	"github.com/databricks/databricks-sdk-go"
)

// Clients is everything this operator asks of Databricks.
//
// It is short, and that is the point rather than an accident. This operator
// creates an identity and registers the trust that lets one Kubernetes
// ServiceAccount prove it is that identity. It grants nothing, reads no
// catalog, and touches no workspace -- so the credential it needs on the
// Databricks side is small enough that whoever governs that account can read the
// permission set in a minute and decide.
//
// Every call here is account-level. There is no workspace client and no
// workspace resolution, because nothing this operator does happens inside a
// workspace.
type Clients interface {
	// Account is the underlying account client. It exists for the controller
	// that has just built the clients and wants to say whether they work; every
	// other caller goes through the methods below, which report their own
	// failures.
	Account() *databricks.AccountClient

	// AccountID is the Databricks account these clients act in.
	//
	// It exists so that an id recorded against one account is not looked up in
	// another and its absence read as a deletion. Nothing else about a service
	// principal says where it lives, and Databricks answers a lookup in the
	// wrong account with the same 404 it gives for one that is really gone.
	AccountID() string

	// CreateServicePrincipal makes the service principal one Kubernetes
	// ServiceAccount exchanges its token for. It returns the numeric id that
	// federation policies hang off, and the applicationId the workload presents
	// at token exchange. Neither can be chosen; Databricks assigns both.
	//
	// The issuing is taken to mark it as this cluster's and as this
	// ServiceAccount's; see MarkerFor.
	CreateServicePrincipal(ctx context.Context, issuing Issuing) (id, clientID string, err error)

	// FindServicePrincipal looks for one already made for this issuing, which is
	// how a service principal created by a pass that then crashed is matched to
	// the record that was written before it.
	FindServicePrincipal(ctx context.Context, issuing Issuing) (
		id, clientID string, found bool, err error)

	// ServicePrincipalExists reports whether a recorded service principal is
	// still there. Nothing in Kubernetes hears about one being deleted in
	// Databricks, so a recorded id that no longer names anything would go on
	// being reported as usable.
	ServicePrincipalExists(ctx context.Context, id string) (bool, error)

	// DeleteServicePrincipal removes one, and with it everything Databricks
	// recorded against it -- Databricks doing the removing, with no list read
	// and nothing belonging to anybody else written.
	DeleteServicePrincipal(ctx context.Context, id string) error

	// EnsureFederationPolicy makes a subject's token exchangeable for that
	// service principal's, adding nothing if it already is.
	EnsureFederationPolicy(ctx context.Context, servicePrincipalID, issuer, subject, audience string) error

	// RemoveFederationPolicies makes it unexchangeable again, and is the only
	// thing this operator can withdraw without destroying anything: the service
	// principal and every grant on it stay, and a token already in a pod is
	// unaffected, because Databricks alone decides what it accepts.
	RemoveFederationPolicies(ctx context.Context, servicePrincipalID, issuer, subject string) error

	// Snapshot is one view of these clients, which does not change under the
	// caller.
	//
	// What the controllers hold is a Holder, and the account controller can
	// replace what is inside it at any moment. A pass that asks which account it
	// is in and then makes a call has taken two views, and the two can differ --
	// so the guard passes on one account while the call goes to another. What
	// that produces is not merely wrong: a lookup in the wrong account answers
	// with the same 404 as an identity that was deleted, which latches a record
	// that never rebuilds, and a create in one account stamped with the other's
	// id is an identity nothing can find again. Both are silent and neither can
	// be undone.
	//
	// Anything already fixed returns itself, so this is only ever a question for
	// the Holder to answer.
	Snapshot() Clients
}

type clients struct {
	cfg     Config
	account *databricks.AccountClient
}

var _ Clients = (*clients)(nil)

// New builds the clients from one account's configuration.
//
// It contacts nothing. Constructing an account client resolves configuration
// and nothing more, so a wrong host or a missing federation policy is not
// discovered here -- it surfaces on the first call, as a condition on whichever
// object happened to reconcile first. Only what Config.Validate can see is
// caught at startup.
func New(cfg Config) (Clients, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ac, err := databricks.NewAccountClient(cfg.SDKConfig())
	if err != nil {
		return nil, fmt.Errorf("building the Databricks account client: %w", err)
	}
	return &clients{cfg: cfg, account: ac}, nil
}

func (c *clients) Account() *databricks.AccountClient { return c.account }

// Snapshot returns these clients: they were built from one account's
// configuration and nothing replaces them.
func (c *clients) Snapshot() Clients { return c }

func (c *clients) AccountID() string { return c.cfg.AccountID }
