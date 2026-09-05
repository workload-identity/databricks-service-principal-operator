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
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// TestCreateServicePrincipalTakesBothIdsFromDatabricks covers two ids that look
// alike and are not interchangeable.
//
// The numeric id is what federation policies hang off and what this operator
// records; the applicationId is what a workload presents when it exchanges its
// token. Neither can be chosen --
// applicationId is refused on create -- so both are read back from the answer,
// and a workload has to be told its client id rather than deriving it.
func TestCreateServicePrincipalTakesBothIdsFromDatabricks(t *testing.T) {
	t.Parallel()
	server := &stubAccount{t: t, answer: map[string]string{
		"POST " + servicePrincipalsAPI: `{"id":"71630475020656","applicationId":"11111111-1111-1111-1111-111111111111"}`,
	}}
	c := server.clients()

	id, clientID, err := c.CreateServicePrincipal(context.Background(),
		Issuing{Issuer: testIssuer, Namespace: "team-a", Name: "etl", ServiceAccountUID: "uid-etl"})
	if err != nil {
		t.Fatal(err)
	}
	if id != "71630475020656" || clientID != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("read id=%q clientId=%q, want both as Databricks gave them", id, clientID)
	}

	sent := server.wrote(servicePrincipalsAPI)
	if strings.Contains(sent, "applicationId") {
		t.Errorf("sent %s; applicationId is refused on create and cannot be chosen here", sent)
	}
	if !strings.Contains(sent, DisplayNameFor("team-a", "etl", "")) {
		t.Errorf("sent %s, want the display name for team-a/etl", sent)
	}
}

// TestDisplayNameIsForPeopleAndMayBeCut covers a name that does not fit.
//
// A subject reaches 149 characters -- "system:serviceaccount:" and two DNS
// labels -- and Databricks caps the display name well below that, so the tail
// goes. Nothing depends on this being unique or reversible: Databricks does not
// enforce uniqueness on it, and this operator finds its service principals by
// the id it wrote down.
func TestDisplayNameIsForPeopleAndMayBeCut(t *testing.T) {
	t.Parallel()
	short := DisplayNameFor("team-a", "etl", "")
	if !strings.Contains(short, "team-a") || !strings.Contains(short, "etl") {
		t.Errorf("display name is %q, want it to name the ServiceAccount", short)
	}
	long := DisplayNameFor(strings.Repeat("n", 63), strings.Repeat("m", 63), "")
	if len(long) > displayNameLimit {
		t.Errorf("display name is %d characters, want no more than %d", len(long), displayNameLimit)
	}
	if !strings.HasPrefix(long, displayNamePrefix) {
		t.Errorf("display name is %q; the prefix is what marks it as this operator's", long)
	}
}

// TestSubjectIsTheClaimKubernetesPuts covers the one string that has to match
// exactly, because a federation policy compares it literally.
func TestSubjectIsTheClaimKubernetesPuts(t *testing.T) {
	t.Parallel()
	if got := SubjectFor("team-a", "etl"); got != "system:serviceaccount:team-a:etl" {
		t.Errorf("subject is %q, want the sub claim a projected token carries", got)
	}
}

// TestAServicePrincipalThatIsGoneIsAbsentRatherThanAFailure covers the answer
// this call exists to give.
//
// Nothing in Kubernetes hears about a service principal being deleted in
// Databricks. Asked on every pass, a 404 has to mean "not there" rather than
// "the call failed" -- reading it as a failure would leave the object naming a
// dead id while reporting that tokens can still be exchanged for it.
func TestAServicePrincipalThatIsGoneIsAbsentRatherThanAFailure(t *testing.T) {
	t.Parallel()
	server := &stubAccount{t: t, status: map[string]int{
		"GET " + servicePrincipalsAPI + "/7788": 404,
	}}
	c := server.clients()

	there, err := c.ServicePrincipalExists(context.Background(), "7788")
	if err != nil {
		t.Fatalf("a 404 came back as a failure: %v", err)
	}
	if there {
		t.Error("reported as there; Databricks looked and it is not")
	}
}

// TestOnlyDatabricksHavingLookedMakesItAbsent covers the other half of the same
// answer, and the more expensive half to get wrong.
//
// The caller reads false as "this service principal was deleted in Databricks",
// latches it into the record, and never rebuilds the identity. A 500 or a
// refusal reported as absence would therefore destroy a live identity
// permanently, off one bad minute. Only Databricks having looked and found
// nothing is an absence; everything else is an error the caller waits on.
//
// The two statuses are the two things that are not a 404 and mean different
// things to a controller: Denied is the operator's own standing, which no retry
// supplies, and Unavailable is Databricks not having answered. Neither is
// retried by the SDK, so both come back in one call.
func TestOnlyDatabricksHavingLookedMakesItAbsent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		code int
		want FailureKind
	}{
		{"refused on privilege", 403, Denied},
		{"Databricks could not answer", 500, Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &stubAccount{t: t, status: map[string]int{
				"GET " + servicePrincipalsAPI + "/7788": tc.code,
			}}
			c := server.clients()

			there, err := c.ServicePrincipalExists(context.Background(), "7788")
			if err == nil {
				t.Fatalf("a %d came back as an answer; the caller latches absence and never "+
					"rebuilds the identity", tc.code)
			}
			if got := KindOf(err); got != tc.want {
				t.Errorf("a %d classifies as %s, want %s", tc.code, got, tc.want)
			}
			if there {
				t.Error("reported as there as well")
			}
		})
	}
}

// TestDeletingOneThatIsAlreadyGoneIsDone covers the finalizer being able to end.
//
// Revocation is deleting the service principal, and the finalizer is not dropped
// until that succeeds. If "already deleted" were a failure, an object whose
// service principal somebody removed by hand could never finish deleting, and
// would sit there until a person took the finalizer off.
func TestDeletingOneThatIsAlreadyGoneIsDone(t *testing.T) {
	t.Parallel()
	server := &stubAccount{t: t, status: map[string]int{
		"DELETE " + servicePrincipalsAPI + "/7788": 404,
	}}
	c := server.clients()

	if err := c.DeleteServicePrincipal(context.Background(), "7788"); err != nil {
		t.Errorf("error is %v; it is gone, which is what was asked for", err)
	}
}

// TestOnlyDatabricksAgreeingItIsGoneEndsTheDelete covers what the finalizer
// rests on.
//
// The caller reads a nil error as "the delete is done" and drops the finalizer.
// A delete Databricks refused, reported as done, takes the record out of
// Kubernetes while the service principal is still live in the account -- and
// nothing left in the cluster then names it, so nobody is going to find it. Only
// a 404 means it is gone.
func TestOnlyDatabricksAgreeingItIsGoneEndsTheDelete(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		code int
		want FailureKind
	}{
		{"refused on privilege", 403, Denied},
		{"Databricks could not answer", 500, Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &stubAccount{t: t, status: map[string]int{
				"DELETE " + servicePrincipalsAPI + "/7788": tc.code,
			}}
			c := server.clients()

			err := c.DeleteServicePrincipal(context.Background(), "7788")
			if err == nil {
				t.Fatalf("a %d came back as a completed delete; the finalizer goes and the "+
					"service principal stays alive with nothing naming it", tc.code)
			}
			if got := KindOf(err); got != tc.want {
				t.Errorf("a %d classifies as %s, want %s", tc.code, got, tc.want)
			}
		})
	}
}

const (
	testIssuer   = "https://oidc.example/cluster"
	testSubject  = "system:serviceaccount:team-a:etl"
	testAudience = "databricks"
)

// TestAFederationPolicyIsNotAddedTwice covers the read that makes this
// idempotent.
//
// Policies live under a service principal and there is no lookup by subject, so
// the ones on that principal are listed and an exact match ends it. Adding a
// second policy for a subject already trusted is a duplicate nobody would ever
// clean up, on every pass, forever.
func TestAFederationPolicyIsNotAddedTwice(t *testing.T) {
	t.Parallel()
	server := &stubAccount{t: t, answer: map[string]string{
		"GET " + federationPoliciesAPI("7788"): `{"policies":[{"oidc_policy":{
			"issuer":"` + testIssuer + `","subject":"` + testSubject + `","audiences":["` + testAudience + `"]}}]}`,
	}}
	c := server.clients()

	if err := c.EnsureFederationPolicy(context.Background(), "7788", testIssuer, testSubject, testAudience); err != nil {
		t.Fatal(err)
	}
	if n := server.made("POST", federationPoliciesAPI("7788")); n != 0 {
		t.Errorf("added %d policies; this subject is already trusted", n)
	}
}

// TestAPolicyThatIsNotThisOneIsNotAMatch covers the three claims all having to
// agree, and the audiences having to be the one.
//
// A policy is what makes a token exchangeable, so a near-match that is treated
// as a match leaves the workload unable to exchange anything, with this operator
// reporting it as ready. The audience list is compared as a list of one: a
// policy trusting two audiences trusts something this operator did not ask for.
func TestAPolicyThatIsNotThisOneIsNotAMatch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		policy string
	}{
		{"another subject", `{"issuer":"` + testIssuer + `","subject":"system:serviceaccount:team-b:loader","audiences":["` + testAudience + `"]}`},
		{"another issuer", `{"issuer":"https://oidc.example/other","subject":"` + testSubject + `","audiences":["` + testAudience + `"]}`},
		{"another audience", `{"issuer":"` + testIssuer + `","subject":"` + testSubject + `","audiences":["something-else"]}`},
		{"one audience too many", `{"issuer":"` + testIssuer + `","subject":"` + testSubject + `","audiences":["` + testAudience + `","other"]}`},
		{"no oidc policy at all", `null`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &stubAccount{t: t, answer: map[string]string{
				"GET " + federationPoliciesAPI("7788"): `{"policies":[{"oidc_policy":` + tc.policy + `}]}`,
			}}
			c := server.clients()

			if err := c.EnsureFederationPolicy(context.Background(), "7788",
				testIssuer, testSubject, testAudience); err != nil {
				t.Fatal(err)
			}
			sent := server.wrote(federationPoliciesAPI("7788"))
			for _, want := range []string{testIssuer, testSubject, testAudience} {
				if !strings.Contains(sent, want) {
					t.Errorf("added %s, want it to carry %s", sent, want)
				}
			}
		})
	}
}

// TestRemovingPoliciesTakesEveryPolicyThisClusterWroteAndLeavesTheRest covers
// what a removal is allowed to reach.
//
// Matched on issuer and subject and not on the audience, which is the one way
// this differs from the create beside it: a policy naming an audience this
// operator no longer hands out is still one a token minted for that audience
// satisfies, so leaving it would leave the exchange open under an older name.
// Another cluster's issuer and another workload's subject are not this
// removal's to touch -- somebody put them there deliberately, and the service
// principal is still theirs to reach.
func TestRemovingPoliciesTakesEveryPolicyThisClusterWroteAndLeavesTheRest(t *testing.T) {
	t.Parallel()
	server := &stubAccount{t: t, answer: map[string]string{
		"GET " + federationPoliciesAPI("7788"): `{"policies":[
			{"policy_id":"current","oidc_policy":{"issuer":"` + testIssuer + `","subject":"` + testSubject + `","audiences":["` + testAudience + `"]}},
			{"policy_id":"older-audience","oidc_policy":{"issuer":"` + testIssuer + `","subject":"` + testSubject + `","audiences":["was-the-audience"]}},
			{"policy_id":"another-cluster","oidc_policy":{"issuer":"https://oidc.example/other","subject":"` + testSubject + `","audiences":["` + testAudience + `"]}},
			{"policy_id":"another-workload","oidc_policy":{"issuer":"` + testIssuer + `","subject":"system:serviceaccount:team-b:loader","audiences":["` + testAudience + `"]}}
		]}`,
	}}
	c := server.clients()

	if err := c.RemoveFederationPolicies(context.Background(), "7788", testIssuer, testSubject); err != nil {
		t.Fatal(err)
	}

	var deleted []string
	for _, made := range server.calls {
		if made.method == "DELETE" {
			deleted = append(deleted, strings.TrimPrefix(made.path, federationPoliciesAPI("7788")+"/"))
		}
	}
	slices.Sort(deleted)
	want := []string{"current", "older-audience"}
	if !slices.Equal(deleted, want) {
		t.Errorf("deleted %v, want %v -- anything missing is trust this cluster still has, and "+
			"anything extra is trust somebody else put there", deleted, want)
	}
}

// TestAPolicyThatIsAlreadyGoneIsRemoved covers the answer a retried removal is
// asked for.
//
// A removal that failed partway, or one made twice because the namespace changed
// hands, finds policies it already deleted. If that were a failure the record
// would report the removal as unfinished for ever, and the account it left
// would never say it had let go.
func TestAPolicyThatIsAlreadyGoneIsRemoved(t *testing.T) {
	t.Parallel()
	server := &stubAccount{
		t: t,
		answer: map[string]string{
			"GET " + federationPoliciesAPI("7788"): `{"policies":[{"policy_id":"p1","oidc_policy":{
				"issuer":"` + testIssuer + `","subject":"` + testSubject + `","audiences":["` + testAudience + `"]}}]}`,
		},
		status: map[string]int{
			"DELETE " + federationPoliciesAPI("7788") + "/p1": 404,
		},
	}
	c := server.clients()

	if err := c.RemoveFederationPolicies(context.Background(), "7788", testIssuer, testSubject); err != nil {
		t.Errorf("error is %v; the trust is gone, which is what was asked for", err)
	}
}

// TestRemovingPoliciesRefusesAnIdThatIsNotANumber covers the same coordinate
// the create refuses, on the call that would otherwise report a removal it
// never attempted.
func TestRemovingPoliciesRefusesAnIdThatIsNotANumber(t *testing.T) {
	t.Parallel()
	server := &stubAccount{t: t}
	c := server.clients()

	var malformed *ErrMalformedCoordinate
	err := c.RemoveFederationPolicies(context.Background(), "not-a-number", testIssuer, testSubject)
	if !errors.As(err, &malformed) {
		t.Errorf("error is %v, want a malformed coordinate; nil would be this reporting a "+
			"removal it never made", err)
	}
	if len(server.calls) != 0 {
		t.Errorf("made %+v for an id that could not be read", server.calls)
	}
}

// TestAFederationPolicyRefusesAnIdThatIsNotANumber covers a coordinate that is
// a string here and a number on the wire.
func TestAFederationPolicyRefusesAnIdThatIsNotANumber(t *testing.T) {
	t.Parallel()
	server := &stubAccount{t: t}
	c := server.clients()

	var malformed *ErrMalformedCoordinate
	err := c.EnsureFederationPolicy(context.Background(), "not-a-number", testIssuer, testSubject, testAudience)
	if !errors.As(err, &malformed) {
		t.Errorf("error is %v, want a malformed coordinate", err)
	}
	if len(server.calls) != 0 {
		t.Errorf("made %+v for an id that could not be read", server.calls)
	}
}

// TestTheIssuerComesFromTheOperatorsOwnToken covers where the one value a
// federation policy cannot be wrong about comes from.
//
// Every ServiceAccount in a cluster gets tokens from the same issuer, so the one
// on the operator's own token is the one a workload's will carry. A value read
// from what is actually presented cannot disagree with what is presented; a
// configured one can, and the disagreement shows up as a token exchange that
// fails for a reason nothing here reports.
func TestTheIssuerComesFromTheOperatorsOwnToken(t *testing.T) {
	t.Parallel()
	got, err := IssuerOf(TokenClaims{Issuer: "https://oidc.example/cluster"})
	if err != nil || got != "https://oidc.example/cluster" {
		t.Errorf("read %q, %v; want the iss claim as it was presented", got, err)
	}

	// Blank is refused rather than sent. A policy trusting an empty issuer is
	// one nothing can satisfy, written without a word about why.
	for _, blank := range []string{"", "   "} {
		if _, err := IssuerOf(TokenClaims{Issuer: blank}); err == nil {
			t.Errorf("accepted %q as an issuer", blank)
		}
	}
}

// TestTheMarkerFitsWhatDatabricksWillStore covers the one limit that cannot be
// found out by trying.
//
// externalId holds 36 characters, measured: 36 is accepted and 37 is refused
// with "Azure object id cannot be over 36 characters". What makes it worth a
// test rather than a comment is how it fails -- a create carrying an oversized
// one returns the error and creates the service principal anyway, so a caller
// that trusts the error leaves an identity behind that nothing recorded.
func TestTheMarkerFitsWhatDatabricksWillStore(t *testing.T) {
	t.Parallel()
	for _, issuing := range []Issuing{
		{Issuer: "https://oidc.example/cluster", Namespace: "team-a", Name: "etl",
			ServiceAccountUID: "6a5f0d1e-0b2c-4c3d-9e8f-1a2b3c4d5e6f"},
		{Issuer: "https://oidc.eks.ap-northeast-1.amazonaws.com/id/" + strings.Repeat("A", 200),
			Namespace: strings.Repeat("n", 63), Name: strings.Repeat("s", 253),
			ServiceAccountUID: strings.Repeat("u", 200)},
		{},
	} {
		if got := len(MarkerFor(issuing)); got != markerLimit {
			t.Errorf("MarkerFor(%d, %d, %d, %d characters) is %d characters, want %d",
				len(issuing.Issuer), len(issuing.Namespace), len(issuing.Name),
				len(issuing.ServiceAccountUID), got, markerLimit)
		}
	}
}

// TestTheMarkerTellsOneServiceAccountFromAnother covers what finding a service
// principal again rests on.
//
// The display name identifies nothing: its hyphens are ambiguous, so
// DisplayNameFor("team-a", "x", "") and DisplayNameFor("team", "a-x", "") are one
// string, and it is truncated at 100 characters besides. A lookup that narrows
// by name and then confirms on the marker is only sound if the marker tells them
// apart -- otherwise the holder of one namespace is handed another's identity,
// gains everything granted to it, and destroys it on the way out.
func TestTheMarkerTellsOneServiceAccountFromAnother(t *testing.T) {
	t.Parallel()
	const issuer = "https://oidc.example/cluster"

	if DisplayNameFor("team-a", "x", "") != DisplayNameFor("team", "a-x", "") {
		t.Fatal("the display names no longer collide; this test is about what happens when they do")
	}
	one := Issuing{Issuer: issuer, Namespace: "team-a", Name: "x", ServiceAccountUID: "uid-one"}
	two := Issuing{Issuer: issuer, Namespace: "team", Name: "a-x", ServiceAccountUID: "uid-two"}
	if MarkerFor(one) == MarkerFor(two) {
		t.Error("two ServiceAccounts sharing a display name share a marker; one namespace can " +
			"be handed the other's identity, and destroy it by removing its own annotation")
	}
}

// TestARecreatedServiceAccountIsNotTheOneBefore covers the property the whole
// ownership design now rests on, and the one a marker built from the subject
// could not have.
//
// A namespace deleted and recreated under the same name can hold a
// ServiceAccount with the same name, whose subject -- which is all Databricks
// matches -- is character for character the one before it. If the marker were
// the subject, the operator would find the dead one's service principal and hand
// it to the new occupant along with everything granted to it. A uid is issued
// once and never again, so it is what the marker is made of.
func TestARecreatedServiceAccountIsNotTheOneBefore(t *testing.T) {
	t.Parallel()
	const issuer = "https://oidc.example/cluster"
	before := Issuing{Issuer: issuer, Namespace: "team-a", Name: "etl", ServiceAccountUID: "uid-before"}
	after := Issuing{Issuer: issuer, Namespace: "team-a", Name: "etl", ServiceAccountUID: "uid-after"}

	if before.Subject() != after.Subject() {
		t.Fatal("the subjects differ; this test is about what happens when they cannot")
	}
	if MarkerFor(before) == MarkerFor(after) {
		t.Error("a ServiceAccount recreated under the same names carries the marker of the one " +
			"before it; the new occupant of a namespace inherits the old identity")
	}
}

// TestEveryIdentityOfOneClusterSharesItsFirstHalf covers the half that is not
// about telling them apart.
//
// Whoever governs the Databricks account has to be able to ask which of its
// service principals came from a given cluster before they can act on any of
// them, and externalId cannot be filtered on -- so the question is answered by
// reading the list and matching a prefix. That only works if the prefix is
// shared, which is the opposite of what the second half does.
func TestEveryIdentityOfOneClusterSharesItsFirstHalf(t *testing.T) {
	t.Parallel()
	const a = "https://oidc.example/cluster-a"
	const b = "https://oidc.example/cluster-b"

	for _, uid := range []string{"uid-one", "uid-two"} {
		got := MarkerFor(Issuing{Issuer: a, Namespace: "team-a", Name: "etl", ServiceAccountUID: uid})
		if got[:len(ClusterMarker(a))] != ClusterMarker(a) {
			t.Errorf("the identity of %s does not carry its cluster's marker", uid)
		}
	}
	if ClusterMarker(a) == ClusterMarker(b) {
		t.Error("two clusters share a marker; the set of identities from one cannot be asked for")
	}
}

// TestWhatIsSentIsTheNameAndTheMarker covers the two fields a create carries,
// and why it carries exactly those.
func TestWhatIsSentIsTheNameAndTheMarker(t *testing.T) {
	t.Parallel()
	server := &stubAccount{t: t, answer: map[string]string{
		"POST " + servicePrincipalsAPI: `{"id":"7788","applicationId":"app-uuid"}`,
	}}
	c := server.clients()

	issuing := Issuing{Issuer: testIssuer, Namespace: "team-a", Name: "etl", ServiceAccountUID: "uid-etl"}
	if _, _, err := c.CreateServicePrincipal(context.Background(), issuing); err != nil {
		t.Fatal(err)
	}
	sent := server.wrote(servicePrincipalsAPI)

	// The name can be filtered on and is not unique. The marker is unique to a
	// cluster and cannot be filtered on. Finding one again needs both.
	if !strings.Contains(sent, DisplayNameFor("team-a", "etl", "")) {
		t.Errorf("sent %s, want the display name -- it is what a lookup filters on", sent)
	}
	if !strings.Contains(sent, MarkerFor(issuing)) {
		t.Errorf("sent %s, want this cluster's marker -- it is what tells one of its "+
			"identities from another cluster's of the same name", sent)
	}
}

// TestTwoIdentitiesOfOneServiceAccountCarryDifferentMarkers is what the third
// part of the marker is for.
//
// Both are issued to one ServiceAccount by one operator, so their issuer, their
// operator and their uid are identical, and their subject is identical too --
// Databricks matches the subject, and both federation policies name the same
// one. The marker is what tells them apart, and it is how a crashed pass finds
// the service principal it made rather than its neighbour.
func TestTwoIdentitiesOfOneServiceAccountCarryDifferentMarkers(t *testing.T) {
	t.Parallel()
	base := Issuing{
		Issuer: testIssuer, Namespace: "team-a", Name: "etl",
		ServiceAccountUID: "uid-etl", Operator: "ops-a/databricks-account",
	}
	reader, writer, only := base, base, base
	reader.Identity, writer.Identity = "reader", "writer"

	for _, pair := range [][2]Issuing{{reader, writer}, {reader, only}, {writer, only}} {
		if MarkerFor(pair[0]) == MarkerFor(pair[1]) {
			t.Errorf("%q marks both %q and %q; a pass that crashed after creating one would "+
				"find the other and record it as its own",
				MarkerFor(pair[0]), pair[0].Identity, pair[1].Identity)
		}
	}
}

// TestTwoOperatorsInOneAccountDoNotFindEachOthers is what the second part is
// for.
//
// Two platform teams may point their operators at one Databricks account. Every
// other input is then identical -- one cluster, one ServiceAccount, one identity
// name -- so without the operator in the marker each would list the account,
// find the other's service principal, and record it as the one it made.
func TestTwoOperatorsInOneAccountDoNotFindEachOthers(t *testing.T) {
	t.Parallel()
	mine := Issuing{
		Issuer: testIssuer, Namespace: "team-a", Name: "etl", ServiceAccountUID: "uid-etl",
		Operator: "ops-a/databricks-account",
	}
	theirs := mine
	theirs.Operator = "ops-b/databricks-account"

	if MarkerFor(mine) == MarkerFor(theirs) {
		t.Error("two operators sharing one Databricks account produce one marker; each adopts " +
			"the other's identities, and both then believe they own it")
	}
}

// TestEverythingThisClusterIssuedIsStillOneQuestion covers what the first part
// has to keep doing.
//
// Databricks refuses to filter on externalId, so the only way to ask for a set
// is to list and match a prefix. That is the whole of the answer to "how does an
// account admin find everything one cluster put here", and adding the operator
// and the identity may not take it away.
func TestEverythingThisClusterIssuedIsStillOneQuestion(t *testing.T) {
	t.Parallel()
	prefix := ClusterMarker(testIssuer)
	for _, issuing := range []Issuing{
		{Issuer: testIssuer, Namespace: "team-a", Name: "etl",
			ServiceAccountUID: "uid-etl", Operator: "ops-a/databricks-account"},
		{Issuer: testIssuer, Namespace: "team-a", Name: "etl", Identity: "reader",
			ServiceAccountUID: "uid-etl", Operator: "ops-a/databricks-account"},
		{Issuer: testIssuer, Namespace: "team-b", Name: "loader",
			ServiceAccountUID: "uid-loader", Operator: "ops-b/databricks-account"},
	} {
		if !strings.HasPrefix(MarkerFor(issuing), prefix) {
			t.Errorf("%+v carries %q, which does not start with this cluster's %q",
				issuing, MarkerFor(issuing), prefix)
		}
	}
	if MarkerFor(Issuing{Issuer: "https://oidc.example/another", Namespace: "team-a",
		Name: "etl", ServiceAccountUID: "uid-etl"})[:len(prefix)] == prefix {
		t.Error("another cluster carries this one's prefix; the set is not one cluster's")
	}
}

// TestTheConsoleCanTellTwoIdentitiesApart covers the display name, which is for
// people and nothing else.
//
// Two identities of one ServiceAccount would otherwise show as two rows with the
// same name in the Databricks console, and whoever is granting permissions there
// has no way to say which is the one meant to read.
func TestTheConsoleCanTellTwoIdentitiesApart(t *testing.T) {
	t.Parallel()
	reader := DisplayNameFor("team-a", "etl", "reader")
	writer := DisplayNameFor("team-a", "etl", "writer")
	if reader == writer {
		t.Fatalf("both are shown as %q", reader)
	}
	if !strings.Contains(reader, "reader") {
		t.Errorf("%q does not carry the name its asker gave it", reader)
	}
	if only := DisplayNameFor("team-a", "etl", ""); !strings.HasSuffix(only, "etl") {
		t.Errorf("%q gained something for an identity with no name", only)
	}
}

// found is one service principal as Databricks returns it in a list, still under
// the name it was created with.
func found(id, marker string) string {
	return namedAs(id, marker, DisplayNameFor("team-a", "etl", ""))
}

// namedAs is the same with whatever name the console shows now. Anybody with
// access to the account can edit a display name and the operator never writes it
// again, so the name a service principal answers to is not evidence of what it
// is.
func namedAs(id, marker, display string) string {
	return fmt.Sprintf(`{"id":%q,"applicationId":"app-%s","displayName":%q,"externalId":%q}`,
		id, id, display, marker)
}

// page is a SCIM page holding all of them. The paging fields are what end the
// iteration: a page that leaves them out is followed by a request for the same
// page, forever.
func page(principals ...string) string {
	return fmt.Sprintf(`{"totalResults":%d,"startIndex":1,"itemsPerPage":%d,"Resources":[%s]}`,
		len(principals), len(principals), strings.Join(principals, ","))
}

// lookedFor is the issuing every lookup test below is about.
var lookedFor = Issuing{
	Issuer: testIssuer, Namespace: "team-a", Name: "etl",
	ServiceAccountUID: "uid-etl", Operator: "ops-a/databricks-account",
}

// onTheName is the filter the narrowed listing carries, spelled here the way the
// lookup spells it. A lookup that stopped filtering, or filtered on something
// else, would stop matching this key and answer from the account-wide entry
// instead, which is what the assertions below would then be reading.
var onTheName = fmt.Sprintf("displayName eq %q", DisplayNameFor("team-a", "etl", ""))

// byName and wholeAccount are the two answer keys a lookup can reach: the
// listing narrowed to the name, and the listing of everything.
var (
	byName       = narrowedTo(servicePrincipalsAPI, onTheName)
	wholeAccount = "GET " + servicePrincipalsAPI
)

// TestTheMarkerDecidesWhichOfTheNamesakesIsTaken covers the second half of the
// lookup, which is the half that identifies anything.
//
// The filter is on the display name, which is not unique and is truncated
// besides, so what comes back is this operator's service principal alongside
// every namesake in the account -- another cluster's, another operator's,
// another ServiceAccount's whose name collided. Taking one of those records it
// as this workload's identity, which hands the workload everything granted to
// somebody else and destroys it when the annotation is removed.
func TestTheMarkerDecidesWhichOfTheNamesakesIsTaken(t *testing.T) {
	t.Parallel()
	elsewhere := Issuing{Issuer: "https://oidc.example/another", Namespace: "team-a", Name: "etl",
		ServiceAccountUID: "uid-etl", Operator: "ops-a/databricks-account"}
	otherOperator := lookedFor
	otherOperator.Operator = "ops-b/databricks-account"

	server := &stubAccount{t: t, answer: map[string]string{
		"GET " + servicePrincipalsAPI: page(
			found("1100", MarkerFor(elsewhere)),
			found("7788", MarkerFor(lookedFor)),
			found("9900", MarkerFor(otherOperator)),
		),
	}}
	c := server.clients()

	id, clientID, ok, err := c.FindServicePrincipal(context.Background(), lookedFor)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("found nothing; the one carrying this issuing's marker is in the list")
	}
	if id != "7788" || clientID != "app-7788" {
		t.Errorf("adopted id=%q clientId=%q, want the one carrying %q", id, clientID, MarkerFor(lookedFor))
	}
}

// TestTwoCarryingOneMarkerIsRefusedRatherThanGuessed covers the answer that is
// neither of the two obvious ones.
//
// Nothing this operator does makes a second, so a pair is a duplicate somebody
// else made or one left behind by a create whose record was lost twice over.
// Taking either is a guess, and a guess that goes wrong hands this workload an
// identity somebody else's grants hang off -- and deletes that one when the
// object goes. There is nothing here that says which, so it says so instead.
func TestTwoCarryingOneMarkerIsRefusedRatherThanGuessed(t *testing.T) {
	t.Parallel()
	server := &stubAccount{t: t, answer: map[string]string{
		"GET " + servicePrincipalsAPI: page(
			found("7788", MarkerFor(lookedFor)),
			found("9900", MarkerFor(lookedFor)),
		),
	}}
	c := server.clients()

	id, clientID, ok, err := c.FindServicePrincipal(context.Background(), lookedFor)
	if err == nil {
		t.Fatalf("adopted id=%q of two carrying one marker; which of them belongs to %s cannot "+
			"be told from here", id, lookedFor.Subject())
	}
	if ok || id != "" || clientID != "" {
		t.Errorf("reported found=%v id=%q clientId=%q alongside the refusal", ok, id, clientID)
	}
}

// TestARenamedServicePrincipalIsStillFound is the reason there are two
// listings.
//
// A display name is editable by anybody with access to the account and the
// operator never writes it again after the create, so the filter can answer
// nothing about a service principal that is sitting right there. Answering "not
// found" then is not a missed optimisation: one caller creates a second service
// principal for a subject that already has one, and the other releases the
// finalizer on the only record naming the first, which leaves an identity in the
// account that nothing in the cluster can reach or delete.
func TestARenamedServicePrincipalIsStillFound(t *testing.T) {
	t.Parallel()
	server := &stubAccount{t: t, answer: map[string]string{
		byName: page(),
		wholeAccount: page(
			namedAs("7788", MarkerFor(lookedFor), "renamed-by-somebody-in-the-console"),
		),
	}}
	c := server.clients()

	id, clientID, ok, err := c.FindServicePrincipal(context.Background(), lookedFor)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("found nothing; %q is in the account carrying this issuing's marker, under a "+
			"name somebody changed", "7788")
	}
	if id != "7788" || clientID != "app-7788" {
		t.Errorf("adopted id=%q clientId=%q, want the one carrying %q", id, clientID, MarkerFor(lookedFor))
	}
}

// TestTheNarrowedListingAnswersOnItsOwn is the other half of the same claim.
//
// The account-wide listing is a page of every service principal there is, read
// on every reconcile of every object. It is affordable because it happens only
// when the narrowed listing turned up nothing, and nothing but this says so: a
// later change that dropped the filter, or moved the fallback out of its guard,
// would leave every test above passing while every lookup read the account.
func TestTheNarrowedListingAnswersOnItsOwn(t *testing.T) {
	t.Parallel()
	server := &stubAccount{t: t, answer: map[string]string{
		byName: page(found("7788", MarkerFor(lookedFor))),
	}}
	c := server.clients()

	if _, _, ok, err := c.FindServicePrincipal(context.Background(), lookedFor); err != nil || !ok {
		t.Fatalf("found=%v err=%v; the narrowed listing holds the one carrying the marker", ok, err)
	}
	if server.listed(servicePrincipalsAPI, onTheName) == 0 {
		t.Errorf("no listing carried %s, so the lookup narrows by something else now and the "+
			"assertion below is no longer about the cheap path", onTheName)
	}
	if n := server.listed(servicePrincipalsAPI, ""); n != 0 {
		t.Errorf("listed the whole account %d times after the narrowed listing had answered; "+
			"every lookup now costs a page of every service principal in it", n)
	}
}

// TestTheFallbackDoesNotInventAMatch covers the answer the second listing must
// still be able to give.
//
// It reads every service principal in the account, which is every namesake and
// every stranger, and the only thing separating them from this issuing's is the
// marker. Loosening that -- matching the name again, or taking the sole namesake
// -- would make the fallback hand back somebody else's identity in precisely the
// case the first listing already refused to.
func TestTheFallbackDoesNotInventAMatch(t *testing.T) {
	t.Parallel()
	elsewhere := Issuing{Issuer: "https://oidc.example/another", Namespace: "team-a", Name: "etl",
		ServiceAccountUID: "uid-etl", Operator: "ops-a/databricks-account"}
	otherOperator := lookedFor
	otherOperator.Operator = "ops-b/databricks-account"

	server := &stubAccount{t: t, answer: map[string]string{
		byName: page(found("1100", MarkerFor(elsewhere))),
		wholeAccount: page(
			found("1100", MarkerFor(elsewhere)),
			namedAs("9900", MarkerFor(otherOperator), "k8s-team-b-loader"),
		),
	}}
	c := server.clients()

	id, clientID, ok, err := c.FindServicePrincipal(context.Background(), lookedFor)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Errorf("adopted id=%q clientId=%q; nothing in the account carries %q, and a namesake "+
			"carrying somebody else's marker is somebody else's", id, clientID, MarkerFor(lookedFor))
	}
	if server.listed(servicePrincipalsAPI, "") == 0 {
		t.Error("the whole account was never listed; the narrowed listing turned up a namesake " +
			"carrying somebody else's marker, which is not an answer to look no further on")
	}
}

// TestTwoCarryingOneMarkerIsRefusedThroughTheFallbackToo is the refusal above,
// reached the other way.
//
// Two carrying one marker is the same fact whichever listing turned them up, and
// through this one they need not even share a name. The judgement is one place
// for that reason: a fallback that decided for itself could adopt what the
// narrowed listing refuses to.
func TestTwoCarryingOneMarkerIsRefusedThroughTheFallbackToo(t *testing.T) {
	t.Parallel()
	server := &stubAccount{t: t, answer: map[string]string{
		byName: page(),
		wholeAccount: page(
			namedAs("7788", MarkerFor(lookedFor), "renamed-once"),
			namedAs("9900", MarkerFor(lookedFor), "renamed-twice"),
		),
	}}
	c := server.clients()

	id, clientID, ok, err := c.FindServicePrincipal(context.Background(), lookedFor)
	if err == nil {
		t.Fatalf("adopted id=%q of two carrying one marker; which of them belongs to %s cannot "+
			"be told from here", id, lookedFor.Subject())
	}
	if ok || id != "" || clientID != "" {
		t.Errorf("reported found=%v id=%q clientId=%q alongside the refusal", ok, id, clientID)
	}
}

// TestExistsRefusesAnEmptyID pins what was measured against a live account on
// 2026-09-03: an empty id makes the request URL the collection's own, and
// Databricks answers it with 200 and every service principal there is. So the
// call succeeds, and "does this exist" answers yes about nothing.
//
// The one caller in this repository guards against it, which is why nothing was
// wrong. The guard is on the method because it belongs to whoever can see the
// URL being built, not to everyone who ever calls it.
//
// The client is a zero value, and that is the assertion: it holds no account, so
// any call at all panics. Reaching the end of this means the refusal happened
// before anything was asked.
func TestExistsRefusesAnEmptyID(t *testing.T) {
	t.Parallel()
	c := &clients{}
	there, err := c.ServicePrincipalExists(context.Background(), "")
	if err == nil {
		t.Fatal("an empty service principal id was looked up; it asks Databricks for the whole " +
			"collection, which answers 200, and the answer comes back as 'it exists'")
	}
	if there {
		t.Error("it reported the service principal as present as well")
	}
}
