package databricks

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"strconv"
	"strings"

	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/databricks-sdk-go/service/oauth2"
)

// displayNameLimit is what Databricks accepts for a service principal's
// displayName. Measured against a live account, because it is not documented:
// 100 is accepted, 101 is refused with "exceeds maximum limit of 100
// characters".
//
// Unlike externalId, this one is checked before anything is created: a refused
// name leaves nothing behind. The two fields on the same call are validated in
// different orders, so neither answers for the other.
const displayNameLimit = 100

// displayNamePrefix marks a service principal as this operator's, for a person
// reading the Databricks console. It is not how anything finds one.
//
// Two things record what a service principal is, and neither is here.
//
// On this side it is status.servicePrincipalId, on the object that claimed it:
// that is how the operator finds the one it made.
//
// On the Databricks side it is the federation policy, which stores the issuer
// and the subject exactly as given and can be listed for any service principal.
// That is what whoever governs the account reads to say which cluster an
// identity belongs to and whose it is.
//
// externalId would be the SCIM place for a correlation id and is unusable.
// Measured against a live account: 36 characters are accepted and 37 are
// refused with "Azure object id cannot be over 36 characters" -- shorter than a
// Kubernetes subject alone, let alone one joined to an issuer. Databricks also
// enforces no uniqueness on it and refuses to filter on it. Worse, a create
// carrying an oversized one still creates the service principal and then
// returns the error, so a caller that trusts the error leaves identities behind.
const displayNamePrefix = "k8s-"

// DisplayNameFor names a service principal after the subject it stands for, and
// after what the asker called this one.
//
// It is for people. A subject can reach 149 characters -- "system:serviceaccount:"
// and two DNS labels -- so the tail is dropped when it does not fit, which makes
// this neither unique nor reversible. Nothing depends on it being either:
// Databricks does not enforce uniqueness on displayName, and the operator finds
// its service principals by the id it recorded when it made them.
//
// The identity's name is here for the same reason the rest is: without it, a
// ServiceAccount holding two identities in one account shows two rows in the
// Databricks console with identical names, and nobody reading that page can say
// which is the one to grant what.
func DisplayNameFor(namespace, name, identity string) string {
	full := displayNamePrefix + namespace + "-" + name
	if identity != "" {
		full += "-" + identity
	}
	if len(full) <= displayNameLimit {
		return full
	}
	return full[:displayNameLimit]
}

// SubjectFor is the sub claim a projected ServiceAccount token carries, which is
// what a federation policy matches.
func SubjectFor(namespace, name string) string {
	return "system:serviceaccount:" + namespace + ":" + name
}

// markerLimit is what externalId holds. Measured against a live account: 36 is
// accepted and 37 is refused with "Azure object id cannot be over 36
// characters". A create carrying an oversized one still creates the service
// principal and then returns the error, so nothing may be sent that could
// exceed it.
const markerLimit = 36

// Issuing is one act of creating a service principal for one ServiceAccount:
// everything Databricks is told, and everything needed to find it again.
type Issuing struct {
	// Issuer is the cluster's OIDC issuer, taken from the operator's own
	// projected token. It is what the federation policy names and what says
	// which cluster a service principal came from.
	Issuer string

	// Namespace and Name are the ServiceAccount's. They assemble the subject a
	// federation policy matches, and the display name a person reads.
	Namespace, Name string

	// ServiceAccountUID is what tells one issuing from another.
	//
	// Names are what Databricks matches and names get reused: a namespace
	// deleted and recreated can hold a ServiceAccount with the same name, whose
	// subject is character for character the one before it. A UID is never
	// reused, so it is what says whether two things naming the same subject are
	// the same thing.
	ServiceAccountUID string

	// Operator names the operator that issued this, as its DatabricksAccount's
	// <namespace>/<name>. Two operators may act in one Databricks account, and
	// without this each would find the other's service principals by marker and
	// take them for its own.
	Operator string

	// Identity is what the ServiceAccount called this one, empty when it named
	// none. It is what tells two service principals issued to one ServiceAccount
	// by one operator apart -- one to read with, one to write with, both in the
	// same account, matching the same subject.
	Identity string
}

// Subject is the sub claim a projected token for this ServiceAccount carries.
func (i Issuing) Subject() string { return SubjectFor(i.Namespace, i.Name) }

// MarkerFor says which cluster a service principal came from, which operator
// issued it, and which identity it is.
//
// Three parts, because three different questions are asked of it and none is
// answerable from another.
//
// The first twelve are the issuer. Every identity this operator makes in one
// cluster carries the same value there, which is what lets whoever governs the
// Databricks account ask for the set: list the account's service principals and
// take the ones whose marker starts with it. Databricks refuses to filter on
// externalId at all -- measured -- so a set is asked for by reading the list
// either way, and a shared prefix is what makes the reading possible. It stays
// first, and alone, for that reason: folding anything else in would take the
// shared prefix with it, and "everything this cluster issued" would stop being
// a question anybody could ask.
//
// The second twelve are the operator. Two operators may be pointed at one
// Databricks account -- two platform teams sharing an account is the case this
// was written for -- and without this each would find the other's service
// principals and take them for its own.
//
// The last twelve are the identity: the ServiceAccount's uid and the name the
// asker gave this one. A uid rather than the subject, for the reason the subject
// cannot be trusted to identify: it is a pair of names, and names come back. A
// service principal left behind by a deleted namespace carries a subject a later
// namespace of the same name reproduces exactly, so a marker built from the
// subject would have the operator adopt the dead one as the live one's. And the
// name alongside it, because one ServiceAccount holding several identities in
// one account is the ordinary case -- one to read with, one to write with -- and
// their uid is the same.
//
// Every part is hashed because the whole of an issuer runs to about a hundred
// characters and externalId holds 36. Truncating the issuer instead would give
// every cluster on one provider the same first part.
//
// It is written in the create request, so a service principal carries it from
// the moment it exists. That is what makes it usable to find one again after a
// pass that created it and then crashed: there is no window in which the thing
// is there and the marker is not.
func MarkerFor(issuing Issuing) string {
	return part(issuing.Issuer) + part(issuing.Operator) +
		part(issuing.ServiceAccountUID+"/"+issuing.Identity)
}

// ClusterMarker is the first part on its own: what every identity this cluster
// made carries, and what a set of them is asked for by.
func ClusterMarker(issuer string) string { return part(issuer) }

// part is 12 characters of a hash, which is 60 bits.
func part(of string) string {
	sum := sha256.Sum256([]byte(of))
	return base32.StdEncoding.WithPadding(base32.NoPadding).
		EncodeToString(sum[:])[:markerLimit/3]
}

// CreateServicePrincipal makes one for a subject and returns its numeric id and
// its applicationId.
//
// Both come back from Databricks; neither can be chosen. applicationId in
// particular is refused on create -- "Attribute applicationID cannot be
// specified for the SCIM Object" -- which is why the workload has to be told the
// client id rather than deriving it.
func (c *clients) CreateServicePrincipal(ctx context.Context, issuing Issuing) (id, clientID string, err error) {
	if strings.TrimSpace(issuing.Issuer) == "" {
		return "", "", fmt.Errorf("%w for %s", ErrNoIssuer, issuing.Subject())
	}
	created, err := c.accountClient.ServicePrincipalsV2.Create(ctx, iam.CreateAccountServicePrincipalRequest{
		DisplayName: DisplayNameFor(issuing.Namespace, issuing.Name, issuing.Identity),
		ExternalId:  MarkerFor(issuing),
		Active:      true,
	})
	if err != nil {
		return "", "", fmt.Errorf("creating a service principal for %s: %w", issuing.Subject(), err)
	}
	return created.Id, created.ApplicationId, nil
}

// FindServicePrincipal looks for one this operator already made for an issuing.
//
// It exists for one window. The record is written before the create is made, so
// a pass that creates a service principal and then stops -- a crash, a lost
// connection, a pod evicted -- leaves a record naming no id and a service
// principal nothing names. This is how the second is matched to the first
// instead of being left behind.
//
// Two fields, because neither is enough alone. The display name can be filtered
// on and identifies nothing: it is truncated at 100 characters and its hyphens
// are ambiguous, so two different subjects can share one. The marker names the
// cluster and the issuing exactly and cannot be filtered on at all -- Databricks
// answers a filter naming externalId with a 400. So the name narrows the list
// and the marker decides.
func (c *clients) FindServicePrincipal(ctx context.Context, issuing Issuing) (
	id, clientID string, found bool, err error) {
	if strings.TrimSpace(issuing.Issuer) == "" {
		return "", "", false, fmt.Errorf("%w for %s", ErrNoIssuer, issuing.Subject())
	}

	display := DisplayNameFor(issuing.Namespace, issuing.Name, issuing.Identity)
	matches, err := c.accountClient.ServicePrincipalsV2.ListAll(ctx, iam.ListAccountServicePrincipalsRequest{
		Filter: fmt.Sprintf("displayName eq %q", display),
	})
	if err != nil {
		return "", "", false, fmt.Errorf("looking for a service principal named %s: %w", display, err)
	}

	marker := MarkerFor(issuing)
	var candidates []iam.AccountServicePrincipal
	for _, principal := range matches {
		if principal.ExternalId == marker {
			candidates = append(candidates, principal)
		}
	}

	switch len(candidates) {
	case 0:
		return "", "", false, nil
	case 1:
		return candidates[0].Id, candidates[0].ApplicationId, true, nil
	default:
		// Two carrying one subject's marker. Nothing this operator does makes a
		// second, so this is a duplicate somebody else made or one left by a
		// create whose record was lost twice over; adopting either would be a
		// guess.
		return "", "", false, fmt.Errorf(
			"%d service principals in this account are named %s and carry this cluster's marker; "+
				"which of them belongs to %s cannot be told from here",
			len(candidates), display, issuing.Subject())
	}
}

// ServicePrincipalExists reports whether the service principal with this id is
// still there.
//
// Asked on every pass rather than assumed. Nothing in Kubernetes hears about one
// being deleted in Databricks, and a recorded id that no longer names anything
// makes this object a record of something that is not there -- while whatever
// still reaches it goes on being reported as usable.
func (c *clients) ServicePrincipalExists(ctx context.Context, servicePrincipalID string) (bool, error) {
	// Refused rather than asked. An empty id makes the URL the collection's own
	// -- ".../ServicePrincipals/" -- which Databricks answers with 200 and every
	// service principal in the account, so the call succeeds and this reports
	// that a service principal naming nothing is there. A recorded identity
	// always has an id; an empty one is a caller that has not got one yet, and
	// the answer to "is this there" is not "yes".
	if servicePrincipalID == "" {
		return false, fmt.Errorf("no service principal id to look up: an empty one asks Databricks "+
			"for the whole collection, which answers %s", "200")
	}
	if _, err := c.accountClient.ServicePrincipalsV2.Get(ctx,
		iam.GetAccountServicePrincipalRequest{Id: servicePrincipalID}); err != nil {
		if KindOf(err) == NotFound {
			return false, nil
		}
		return false, fmt.Errorf("looking up service principal %s: %w", servicePrincipalID, err)
	}
	return true, nil
}

// DeleteServicePrincipal removes one.
//
// This is the whole of what the operator does to end a workload's access: one
// call, no list read, and nothing belonging to anybody else written. What it
// leaves behind is nothing that can be presented -- the federation policies go
// with it, measured against a live account, so no token can be exchanged for it
// again.
func (c *clients) DeleteServicePrincipal(ctx context.Context, servicePrincipalID string) error {
	if err := c.accountClient.ServicePrincipalsV2.Delete(ctx,
		iam.DeleteAccountServicePrincipalRequest{Id: servicePrincipalID}); err != nil {
		if KindOf(err) == NotFound {
			return nil
		}
		return fmt.Errorf("deleting service principal %s: %w", servicePrincipalID, err)
	}
	return nil
}

// EnsureFederationPolicy makes the token exchange work for one subject.
//
// Policies live under a service principal and there is no account-level lookup
// by subject, so this lists the ones on that principal and adds nothing if the
// subject is already trusted. Several service principals may trust the same
// subject -- verified -- so this says nothing about any other.
func (c *clients) EnsureFederationPolicy(ctx context.Context, servicePrincipalID, issuer, subject, audience string) error {
	numeric, err := strconv.ParseInt(servicePrincipalID, 10, 64)
	if err != nil {
		return &ErrMalformedCoordinate{Field: "servicePrincipalId", Value: servicePrincipalID, Cause: err}
	}

	existing, err := c.accountClient.ServicePrincipalFederationPolicy.ListByServicePrincipalId(ctx, numeric)
	if err != nil {
		return fmt.Errorf("listing the federation policies on service principal %s: %w", servicePrincipalID, err)
	}
	for _, policy := range existing.Policies {
		if matches(policy.OidcPolicy, issuer, subject, audience) {
			return nil
		}
	}

	_, err = c.accountClient.ServicePrincipalFederationPolicy.Create(ctx,
		oauth2.CreateServicePrincipalFederationPolicyRequest{
			ServicePrincipalId: numeric,
			Policy: oauth2.FederationPolicy{
				Description: "Kubernetes workload " + subject,
				OidcPolicy: &oauth2.OidcFederationPolicy{
					Issuer:    issuer,
					Subject:   subject,
					Audiences: []string{audience},
				},
			},
		})
	if err != nil {
		return fmt.Errorf("trusting %s on service principal %s: %w", subject, servicePrincipalID, err)
	}
	return nil
}

// RemoveFederationPolicies takes back the one thing a cluster can take back.
//
// A running pod holds a token that was already minted, and Databricks alone
// decides whether to accept it, so nothing here reaches the workload. What goes
// is the account-side statement that a token from this issuer, for this subject,
// may be exchanged at all -- and with it, the exchange. The service principal
// stays, and so does everything granted to it.
//
// Matched on issuer and subject, where the create beside it also compares the
// audience. A policy naming an audience this operator no longer hands out is
// still one that a token minted for that audience satisfies, so matching the
// current audience would leave the exchange open under an older name.
//
// Every page is read, where EnsureFederationPolicy takes the first one.
// Returning nil here claims that nothing of this cluster's is left on that
// service principal, and a policy past a page boundary would make the claim
// false while the call reported success. Ensure's worst case at the same
// boundary is a second policy saying what the first says, which this removes.
//
// A policy already gone is not a failure. A withdrawal that lost its namespace
// partway through and is retried finds some of its policies already deleted, and
// that is the answer it wanted, not an error to report over work that is done.
func (c *clients) RemoveFederationPolicies(ctx context.Context, servicePrincipalID, issuer, subject string) error {
	numeric, err := strconv.ParseInt(servicePrincipalID, 10, 64)
	if err != nil {
		return &ErrMalformedCoordinate{Field: "servicePrincipalId", Value: servicePrincipalID, Cause: err}
	}

	existing, err := c.accountClient.ServicePrincipalFederationPolicy.ListAll(ctx,
		oauth2.ListServicePrincipalFederationPoliciesRequest{ServicePrincipalId: numeric})
	if err != nil {
		return fmt.Errorf("listing the federation policies on service principal %s: %w",
			servicePrincipalID, err)
	}

	for _, policy := range existing {
		if policy.OidcPolicy == nil ||
			policy.OidcPolicy.Issuer != issuer || policy.OidcPolicy.Subject != subject {
			continue
		}
		err := c.accountClient.ServicePrincipalFederationPolicy.Delete(ctx,
			oauth2.DeleteServicePrincipalFederationPolicyRequest{
				ServicePrincipalId: numeric,
				PolicyId:           policy.PolicyId,
			})
		if err != nil && KindOf(err) != NotFound {
			// Stopped at the first one that did not go, rather than carried on
			// and summarised. The caller reports this as trust still in place,
			// which is true of the one that failed and of everything after it,
			// and the next pass starts again from what is actually there.
			return fmt.Errorf("removing the trust in %s on service principal %s: %w",
				subject, servicePrincipalID, err)
		}
	}
	return nil
}

func matches(policy *oauth2.OidcFederationPolicy, issuer, subject, audience string) bool {
	if policy == nil {
		return false
	}
	return policy.Issuer == issuer && policy.Subject == subject &&
		len(policy.Audiences) == 1 && policy.Audiences[0] == audience
}

// IssuerOf is the iss claim of a token, which a federation policy has to name.
//
// The operator takes it from its own projected token rather than being told:
// every ServiceAccount in a cluster gets tokens from the same issuer, so the one
// on the operator's own token is the one a workload's will carry. A value read
// from what is actually presented cannot disagree with it; a configured one can.
func IssuerOf(claims TokenClaims) (string, error) {
	if strings.TrimSpace(claims.Issuer) == "" {
		return "", fmt.Errorf("the operator's own token carries no iss claim, so the issuer to trust is unknown")
	}
	return claims.Issuer, nil
}
