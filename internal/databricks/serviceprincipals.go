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
	"context"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/databricks/databricks-sdk-go/apierr"
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
func part(of string) string { return truncatedHash(of, markerLimit/3) }

// truncatedHash is the first characters of a hash, in base32 because externalId
// and a policy id both hold text and neither holds every byte.
//
// The width is the caller's, and each caller has its own reason for the one it
// asks for. A value written into Databricks and looked for by a later pass stops
// being findable the day its width changes, so no two of them may be made to
// move together.
func truncatedHash(of string, width int) string {
	sum := sha256.Sum256([]byte(of))
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])[:width]
}

// federationPolicyIDPrefix says a policy id was chosen here rather than assigned
// by Databricks, for a person reading an account's policies. Databricks assigns
// something opaque, so a policy carrying this is one that something on this side
// can name again.
const federationPolicyIDPrefix = "k8s-"

// federationPolicyIDPart is 12 characters of a hash, which is 60 bits. It is
// spelled here rather than taken from part's width: that one is a third of what
// externalId holds, a Databricks limit this has nothing to do with, and the day
// that limit were measured again would be the day every policy already written
// stopped answering to its name.
const federationPolicyIDPart = 12

// FederationPolicyIDFor names the policy that says this cluster trusts one
// subject on one service principal.
//
// A name is what makes writing the policy twice writing one policy. A create
// carrying it either makes that policy or is refused because it is already
// there, and both answers are the one the caller wanted -- where a create that
// first asks a listing whether to create makes a second policy every time the
// listing has not caught up, which is every first convergence, because the
// controller writes status twice and the second pass arrives in milliseconds.
//
// Keyed on the issuer and the subject and deliberately not on the audience,
// which is the pair RemoveFederationPolicies matches on and for its reason: a
// policy naming an audience this operator no longer hands out is still one that
// a token minted for that audience satisfies, so the two are one policy and not
// two. Keying the name on the audience as well would make a reissued operator
// token write a second policy beside the first rather than restate the one that
// is there -- the duplicate this exists to end, arriving by another road.
//
// Hashed, because neither part can be spelled in an id: Databricks takes
// lowercase alphanumerics, hyphens and slashes, and a subject is
// "system:serviceaccount:ns:name" while an issuer is a URL. Lower cased for the
// same reason, base32 being upper. The issuer's part comes first and alone, for
// the reason it does in MarkerFor: what every policy one cluster wrote shares is
// then a prefix somebody reading a service principal's policies can pick out.
func FederationPolicyIDFor(issuer, subject string) string {
	return federationPolicyIDPrefix + strings.ToLower(
		truncatedHash(issuer, federationPolicyIDPart)+truncatedHash(subject, federationPolicyIDPart))
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
// The marker decides and the display name only narrows what has to be read. The
// name identifies nothing: it is truncated at 100 characters and its hyphens are
// ambiguous, so two different subjects can share one. It is also editable by
// anybody with access to the account, and the operator never writes it again
// after the create, so a service principal somebody renamed answers no filter
// naming it.
//
// Hence two listings rather than one. The filtered listing is what an ordinary
// lookup costs, and it answers for as long as the name is the one created. When
// nothing it returns carries the marker, the account is listed whole and the
// marker compared over all of it -- the only way left to ask, because Databricks
// answers a filter naming externalId with a 400. That listing is the expensive
// one and it runs in exactly the case where stopping at the first would report
// "not found" about something that exists: one caller acts on that answer by
// creating a second service principal for the same subject, and the other by
// releasing the finalizer on the only record of the first.
func (c *clients) FindServicePrincipal(ctx context.Context, issuing Issuing) (
	id, clientID string, found bool, err error) {
	if strings.TrimSpace(issuing.Issuer) == "" {
		return "", "", false, fmt.Errorf("%w for %s", ErrNoIssuer, issuing.Subject())
	}

	display := DisplayNameFor(issuing.Namespace, issuing.Name, issuing.Identity)
	marker := MarkerFor(issuing)

	namesakes, err := c.accountClient.ServicePrincipalsV2.ListAll(ctx, iam.ListAccountServicePrincipalsRequest{
		Filter: fmt.Sprintf("displayName eq %q", display),
	})
	if err != nil {
		return "", "", false, fmt.Errorf("looking for a service principal named %s: %w", display, err)
	}

	candidates := carrying(namesakes, marker)
	if len(candidates) == 0 {
		everything, listErr := c.accountClient.ServicePrincipalsV2.ListAll(ctx,
			iam.ListAccountServicePrincipalsRequest{})
		if listErr != nil {
			// Named after the account rather than after a name, which is the
			// difference between the two failures: above, Databricks would not
			// answer about a name; here, it would not say what is in the account
			// at all.
			return "", "", false, fmt.Errorf(
				"listing this account's service principals to find the one marked %s for %s: %w",
				marker, issuing.Subject(), listErr)
		}
		candidates = carrying(everything, marker)
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
		//
		// The display name is named above and not here. Through the second
		// listing these two need not share one, and naming what they were
		// created with would send a reader looking for rows the console no
		// longer has.
		return "", "", false, fmt.Errorf(
			"%d service principals in this account carry %s, which is %s's marker; "+
				"which of them belongs to it cannot be told from here",
			len(candidates), marker, issuing.Subject())
	}
}

// carrying is the whole of what says a service principal is one issuing's. Both
// listings are read through it, because which of them turned one up says nothing
// about whether it is the one: the filter is a way of reading less, not a second
// test of identity.
func carrying(principals []iam.AccountServicePrincipal, marker string) []iam.AccountServicePrincipal {
	var carried []iam.AccountServicePrincipal
	for _, principal := range principals {
		if principal.ExternalId == marker {
			carried = append(carried, principal)
		}
	}
	return carried
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
// The policy is named rather than looked for. FederationPolicyIDFor derives its
// id from the issuer and the subject, so the trust in one subject on one service
// principal has one name and a second call writes that name again instead of
// making a second policy. No listing decides whether to create: the controller
// writes status twice on a first convergence, so the second pass arrives
// milliseconds after the first and reads a listing that does not yet carry what
// the first one wrote -- and a create decided from that listing is a duplicate
// on every identity this operator issues.
//
// Three answers, and each of them is this call's promise kept. The named policy
// is there and says what it should, so nothing is written. It is not there, so
// it is created. It is there naming another audience -- the operator's own token
// was reissued for one -- so it is restated to name this one. A create refused
// because the name is taken is the third of those reached by another road, and
// answered the same way.
//
// Several service principals may trust the same subject -- verified -- so this
// says nothing about any other. A policy id names a policy under one service
// principal, so two of them carrying the same name is two policies.
func (c *clients) EnsureFederationPolicy(ctx context.Context, servicePrincipalID, issuer, subject, audience string) error {
	numeric, err := strconv.ParseInt(servicePrincipalID, 10, 64)
	if err != nil {
		return &ErrMalformedCoordinate{Field: "servicePrincipalId", Value: servicePrincipalID, Cause: err}
	}
	policyID := FederationPolicyIDFor(issuer, subject)

	standing, err := c.accountClient.ServicePrincipalFederationPolicy.
		GetByServicePrincipalIdAndPolicyId(ctx, numeric, policyID)
	switch {
	case err == nil:
		if trusts(standing.OidcPolicy, issuer, subject, audience) {
			return nil
		}
		return c.restateFederationPolicy(ctx, numeric, servicePrincipalID, policyID, issuer, subject, audience)
	case KindOf(err) != NotFound:
		return fmt.Errorf("looking for policy %s, which is what trusts %s on service principal %s: %w",
			policyID, subject, servicePrincipalID, err)
	}

	created, err := c.accountClient.ServicePrincipalFederationPolicy.Create(ctx,
		oauth2.CreateServicePrincipalFederationPolicyRequest{
			ServicePrincipalId: numeric,
			PolicyId:           policyID,
			Policy:             federationPolicyFor(issuer, subject, audience),
		})
	switch {
	case err == nil:
		// Databricks named the policy something else, so it did not take the
		// name it was given and nothing here can address what it made. Reported
		// rather than carried on with: every pass after this one would look for
		// a name that is not there and create again, which is a policy per pass
		// per identity with nothing saying so.
		//
		// Read only when Databricks says something. An answer carrying no
		// policy_id says nothing about the name, and refusing on that would
		// refuse every create.
		if created.PolicyId != "" && created.PolicyId != policyID {
			return fmt.Errorf(
				"the policy trusting %s on service principal %s was asked to be named %s "+
					"and Databricks named it %s, so nothing here can find it again",
				subject, servicePrincipalID, policyID, created.PolicyId)
		}
	case alreadyExists(err):
		if err := c.restateFederationPolicy(
			ctx, numeric, servicePrincipalID, policyID, issuer, subject, audience); err != nil {
			return err
		}
	default:
		return fmt.Errorf("trusting %s on service principal %s: %w", subject, servicePrincipalID, err)
	}

	// Whatever else this cluster wrote for this subject goes with the naming of
	// it. A service principal can carry a policy Databricks named -- an account
	// holds what earlier operators put in it -- and that one answers to no name
	// this can ask for, so the listing is the only thing that finds it. Left
	// beside the named one it is a second policy saying what the first says, on
	// that identity, for as long as the identity lives.
	//
	// Only on the road where the name was not already standing, so the listing
	// is read on the pass that first writes the name and on no pass after it.
	// The alternative is one listing per identity per pass, for ever.
	//
	// Its failure is not this call's. The trust this promises is in place by the
	// time this runs, and reporting a cleanup that could not be finished as a
	// failed write would take an identity that works and report it broken. What
	// is left instead is a policy saying what the named one says, which the
	// removal takes with the rest.
	_ = c.removeListedPolicies(ctx, numeric, servicePrincipalID, issuer, subject, policyID)
	return nil
}

// restateFederationPolicy writes what the named policy should say over what it
// says now.
//
// The audience is the only thing that can have drifted. The issuer and the
// subject are what the name is derived from and the description is derived from
// the subject, so a policy found under this name and differing from what would
// be written for it differs in the audience or in nothing -- which is what the
// mask names, and why it names nothing else.
func (c *clients) restateFederationPolicy(ctx context.Context, numeric int64,
	servicePrincipalID, policyID, issuer, subject, audience string) error {
	_, err := c.accountClient.ServicePrincipalFederationPolicy.Update(ctx,
		oauth2.UpdateServicePrincipalFederationPolicyRequest{
			ServicePrincipalId: numeric,
			PolicyId:           policyID,
			UpdateMask:         "oidc_policy",
			Policy:             federationPolicyFor(issuer, subject, audience),
		})
	if err != nil {
		return fmt.Errorf("restating policy %s, which is what trusts %s on service principal %s, "+
			"to name audience %q: %w", policyID, subject, servicePrincipalID, audience, err)
	}
	return nil
}

// federationPolicyFor is the whole of what this operator says a policy is,
// spelled once so that a create and a restatement cannot come to say different
// things about the same name.
func federationPolicyFor(issuer, subject, audience string) oauth2.FederationPolicy {
	return oauth2.FederationPolicy{
		Description: "Kubernetes workload " + subject,
		OidcPolicy: &oauth2.OidcFederationPolicy{
			Issuer:    issuer,
			Subject:   subject,
			Audiences: []string{audience},
		},
	}
}

// alreadyExists is a create refused because the name it asked for is taken.
//
// It is not a failure of the call that made it. The name is derived from the
// issuer and the subject, so whatever stands under it is what this was asking
// for, and a create is refused this way exactly when the pass before it got
// there first. The SDK maps ALREADY_EXISTS and RESOURCE_ALREADY_EXISTS onto the
// same sentinel it maps a bare 409 onto, so one test answers for the error code
// and for the status alike.
//
// Not a FailureKind. A kind is what a controller branches on, and no controller
// has anything to do about this that is not done where it is caught.
func alreadyExists(err error) bool { return errors.Is(err, apierr.ErrResourceConflict) }

// RemoveFederationPolicies takes back the one thing a cluster can take back.
//
// A running pod holds a token that was already minted, and Databricks alone
// decides whether to accept it, so nothing here reaches the workload. What goes
// is the account-side statement that a token from this issuer, for this subject,
// may be exchanged at all -- and with it, the exchange. The service principal
// stays, and so does everything granted to it.
//
// The named policy goes first and without being looked for, because a name can
// be deleted whether or not anything has listed it yet. A namespace that stops
// being served moments after an identity was issued is a removal reading a
// listing that has not caught up, and a removal taking its whole answer from one
// reports success over trust still standing.
//
// Then the listing, for everything else this cluster wrote for the subject. A
// policy Databricks named answers to no name this can ask for, and an account
// holds what earlier operators put in it, so nothing but a listing finds one.
// Returning nil claims that nothing of this cluster's is left on that service
// principal, and it takes both reads to make the claim true.
func (c *clients) RemoveFederationPolicies(ctx context.Context, servicePrincipalID, issuer, subject string) error {
	numeric, err := strconv.ParseInt(servicePrincipalID, 10, 64)
	if err != nil {
		return &ErrMalformedCoordinate{Field: "servicePrincipalId", Value: servicePrincipalID, Cause: err}
	}

	named := FederationPolicyIDFor(issuer, subject)
	if err := c.forgetPolicy(ctx, numeric, named); err != nil {
		return fmt.Errorf("removing the trust in %s on service principal %s: %w",
			subject, servicePrincipalID, err)
	}
	return c.removeListedPolicies(ctx, numeric, servicePrincipalID, issuer, subject, named)
}

// removeListedPolicies deletes every policy on this service principal that names
// this issuer and this subject, except the one already dealt with by name.
//
// What counts as one of this cluster's policies for a subject is decided here
// and nowhere else, and the call that writes a policy and the call that takes
// one back read it the same way. The two disagreeing would mean one of them
// reaching a policy the other would not.
//
// Matched on issuer and subject, and not on the audience. A policy naming an
// audience this operator no longer hands out is still one that a token minted
// for that audience satisfies, so matching the current audience would leave the
// exchange open under an older name. Another cluster's issuer and another
// workload's subject are not this operator's to touch -- somebody put them there
// deliberately, and the service principal is still theirs to reach.
//
// Every page is read. Returning nil claims that nothing of this cluster's is
// left, and a policy past a page boundary would make the claim false while the
// call reported success.
func (c *clients) removeListedPolicies(ctx context.Context, numeric int64,
	servicePrincipalID, issuer, subject, except string) error {
	listed, err := c.accountClient.ServicePrincipalFederationPolicy.ListAll(ctx,
		oauth2.ListServicePrincipalFederationPoliciesRequest{ServicePrincipalId: numeric})
	if err != nil {
		return fmt.Errorf("listing the federation policies on service principal %s: %w",
			servicePrincipalID, err)
	}

	for _, policy := range listed {
		if policy.PolicyId == except || policy.OidcPolicy == nil ||
			policy.OidcPolicy.Issuer != issuer || policy.OidcPolicy.Subject != subject {
			continue
		}
		if err := c.forgetPolicy(ctx, numeric, policy.PolicyId); err != nil {
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

// forgetPolicy deletes one policy by name.
//
// A policy already gone is not a failure. A removal that lost its namespace
// partway through and is retried finds some of its policies already deleted, and
// a removal by a name nothing ever wrote finds nothing at all. Both are the
// answer that was wanted, not an error to report over work that is done or was
// never there to do.
func (c *clients) forgetPolicy(ctx context.Context, numeric int64, policyID string) error {
	if err := c.accountClient.ServicePrincipalFederationPolicy.
		DeleteByServicePrincipalIdAndPolicyId(ctx, numeric, policyID); err != nil &&
		KindOf(err) != NotFound {
		return err
	}
	return nil
}

func trusts(policy *oauth2.OidcFederationPolicy, issuer, subject, audience string) bool {
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
