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

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	dbx "github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

// otherNamespace is a second namespace this operator serves, so that a test
// about one being taken out of scope can say what happened to the one that was
// not.
const otherNamespace = "team-b"

// otherOperator is a second operator on the same cluster: its own
// DatabricksAccount, in its own namespace, serving a namespace this one also
// serves. It is what makes the difference between suspending minting and
// removing the cluster's permission visible at all -- with one operator the two
// look the same.
var otherOperator = types.NamespacedName{Namespace: "other-operators", Name: "elsewhere"}

// databricksAccounts wires the account controller onto the same fake cluster.
//
// All three controllers over one fake client, because the removal is not in
// any one of them: the account controller claims the namespace and the record's
// controller takes the trust back, and a test that ran either alone would be
// asserting about half of it.
func (c *controllers) databricksAccounts(t *testing.T) *DatabricksAccountReconciler {
	t.Helper()
	return &DatabricksAccountReconciler{
		Client:                          c.Client,
		Scheme:                          c.DatabricksServiceAccounts.Scheme,
		DatabricksAccountNamespacedName: testOperatorRef,
		AccountInUse:                    dbx.NewAccountInUse(operatorNamespace, databricksAccountName),
		OwnToken:                        dbx.Config{OIDCTokenFilepath: c.Issued.TokenPath, TokenAudience: testAudience},
		build:                           func(dbx.Config) (dbx.Clients, error) { return c.Stub, nil },
		verify:                          func(context.Context, dbx.Clients) error { return nil },
	}
}

// serving is a DatabricksAccount for another operator, in that operator's own
// namespace, naming the namespaces it serves.
func serving(operator types.NamespacedName, namespaces ...string) *dbxv1alpha1.DatabricksAccount {
	object := databricksAccountNamed(operator.Name)
	object.Namespace = operator.Namespace
	object.Spec.Namespaces = namespaces
	return object
}

// databricksServiceAccountsFor is another operator's DatabricksServiceAccount
// controller over the same cluster. It is a different operator by the only
// thing that makes one: the DatabricksAccount it acts on, which is also its
// field manager.
func databricksServiceAccountsFor(scheme *runtime.Scheme, c client.Client,
	operator types.NamespacedName) *DatabricksServiceAccountReconciler {
	return &DatabricksServiceAccountReconciler{
		Client:                          c,
		Scheme:                          scheme,
		DatabricksAccountNamespacedName: operator,
	}
}

// nowServing rewrites which namespaces this operator's account names, the way a
// platform team editing spec.namespaces does.
func nowServing(t *testing.T, c client.Client, namespaces ...string) {
	t.Helper()
	var databricksAccount dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(), testOperatorRef, &databricksAccount); err != nil {
		t.Fatal(err)
	}
	databricksAccount.Spec.Namespaces = namespaces
	if err := c.Update(context.Background(), &databricksAccount); err != nil {
		t.Fatal(err)
	}
}

// claims a namespace for one operator, exactly as that operator's own account
// controller claims it: server-side apply under its own field manager. Written
// that way rather than by editing the object, because what makes the claim a
// claim is who owns the field, and an edit owns nothing.
func claims(t *testing.T, c client.Client, namespace string,
	operator types.NamespacedName, since time.Time, opts ...client.ApplyOption) {
	t.Helper()
	opts = append(opts, fieldOwnerFor(operator))
	if err := c.Apply(context.Background(), removingPoliciesBy(namespace, operator, since), opts...); err != nil {
		t.Fatal(err)
	}
}

// claimOn is the operator named on a namespace, or "" for one nobody has
// claimed.
func claimOn(t *testing.T, c client.Client, name string) string {
	t.Helper()
	return labelsOf(t, c, name)[dbxv1alpha1.RemovingPoliciesLabel]
}

// claimedSince is when the holder of a namespace last said it was still there.
func claimedSince(t *testing.T, c client.Client, name string) time.Time {
	t.Helper()
	var namespace corev1.Namespace
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, &namespace); err != nil {
		t.Fatal(err)
	}
	said := namespace.Annotations[dbxv1alpha1.RemovingPoliciesSinceAnnotation]
	at, err := time.Parse(time.RFC3339, said)
	if err != nil {
		t.Fatalf("%s carries %q, which is not a time: %v",
			dbxv1alpha1.RemovingPoliciesSinceAnnotation, said, err)
	}
	return at
}

// labelsOf is what a namespace carries now.
func labelsOf(t *testing.T, c client.Client, name string) map[string]string {
	t.Helper()
	var namespace corev1.Namespace
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, &namespace); err != nil {
		t.Fatal(err)
	}
	return namespace.Labels
}

// readyOf is the Ready condition on a record.
func readyOf(t *testing.T, c client.Client,
	issued *dbxv1alpha1.IssuedDatabricksServicePrincipal) *metav1.Condition {
	t.Helper()
	var got dbxv1alpha1.IssuedDatabricksServicePrincipal
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(issued), &got); err != nil {
		t.Fatal(err)
	}
	return meta.FindStatusCondition(got.Status.Conditions, conditionReady)
}

// policiesRemovedCondition is what the account says about the namespaces it
// stopped serving.
func policiesRemovedCondition(t *testing.T, c client.Client) *metav1.Condition {
	t.Helper()
	var got dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(), testOperatorRef, &got); err != nil {
		t.Fatal(err)
	}
	return meta.FindStatusCondition(got.Status.Conditions, conditionFederationPoliciesRemoved)
}

// requestFor is the reconcile request for one ServiceAccount, which is what a
// DatabricksServiceAccount controller is asked about.
func requestFor(serviceAccount *corev1.ServiceAccount) reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKeyFromObject(serviceAccount)}
}

// recordsIn is how many records one operator has written in its own namespace,
// which is how a test says whether that operator minted anything.
func recordsIn(t *testing.T, c client.Client, namespace string) int {
	t.Helper()
	var list dbxv1alpha1.IssuedDatabricksServicePrincipalList
	if err := c.List(context.Background(), &list, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	return len(list.Items)
}

// TestANamespaceTakenOutOfScopeIsClaimedAndKeepsItsPermission covers the half of
// a removal that happens in the cluster.
//
// The claim names this operator, so that a second one serving the same namespace
// can tell whose removal it is waiting for. The cluster's own labels are left
// exactly as they were: they are the permission given to every operator here at
// once, and taking one off would be this operator revoking somebody else's, for
// good, since no operator may write one back. A namespace still named is
// untouched, because a removal that reached one is a removal that took every
// team on the account with it.
func TestANamespaceTakenOutOfScopeIsClaimedAndKeepsItsPermission(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub,
		databricksAccountServing(otherNamespace),
		mintingNamespace(testNamespace),
		mintingNamespace(otherNamespace),
		asker,
		recordFor(asker),
	)

	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

	if got := claimOn(t, c.Client, testNamespace); got != removedPoliciesBy(testOperatorRef) {
		t.Errorf("%s is claimed by %q, want %q; identities can still be minted there while this "+
			"operator works through the ones it has", testNamespace, got, removedPoliciesBy(testOperatorRef))
	}
	minting, err := namespaceMints(context.Background(), c.Client, testNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if minting {
		t.Errorf("%s still mints; whatever is made next is not in the set the policies are being "+
			"removed from", testNamespace)
	}

	left := labelsOf(t, c.Client, testNamespace)
	for _, label := range []string{dbxv1alpha1.MintLabel, dbxv1alpha1.InjectLabel} {
		if left[label] != dbxv1alpha1.Enabled {
			t.Errorf("%s lost %s; that is the cluster's permission to every operator serving it, "+
				"and nothing but a person may give it back", testNamespace, label)
		}
	}

	if got := claimOn(t, c.Client, otherNamespace); got != "" {
		t.Errorf("%s is claimed by %q, and it is still one this account names; one team leaving "+
			"stopped another team's minting", otherNamespace, got)
	}
}

// TestANamespaceThisOperatorNeverEnteredIsLeftAlone covers the one write this
// operator makes outside its own namespace not being made anywhere it has no
// standing.
//
// The set is derived from the records, so a namespace this operator issued
// nothing in is not one it has anything to remove there. Another operator on
// the same cluster serves namespaces this one does not name, and a claim there
// would suspend that operator's minting for a removal with nothing in it.
func TestANamespaceThisOperatorNeverEnteredIsLeftAlone(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		databricksAccountServing(testNamespace),
		mintingNamespace(testNamespace),
		mintingNamespace(otherNamespace),
	)

	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

	if got := claimOn(t, c.Client, otherNamespace); got != "" {
		t.Errorf("%s is claimed by %q, and this operator has never issued anything there; "+
			"whoever mints there does so for somebody else", otherNamespace, got)
	}
}

// TestTakingANamespaceBackDestroysNothingAndEndsTheExchange covers what a
// removal is, in one assertion each way.
//
// The federation policy is the only thing a cluster can take back: a running pod
// already holds its token and Databricks alone decides whether to accept it. So
// the trust goes and nothing else does -- the service principal stays with every
// grant made on it, and the record stays with the id that finds it again.
func TestTakingANamespaceBackDestroysNothingAndEndsTheExchange(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)

	issued := c.issuedOf(t, asker)
	if issued == nil || issued.Status.ServicePrincipalID == "" {
		t.Fatal("nothing was issued to start from")
	}
	before := issued.Status

	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)

	if len(stub.policies) != 0 {
		t.Errorf("federation policies are %v; this cluster can still be exchanged for an "+
			"identity in a namespace the account stopped naming", stub.policies)
	}
	if len(stub.deleted) != 0 {
		t.Errorf("deleted %v; taking a namespace out of scope destroyed a service principal, and "+
			"with it every grant somebody made on it", stub.deleted)
	}

	after := c.issuedOf(t, asker)
	if after == nil {
		t.Fatal("the record is gone; the identity can never be restored without one")
	}
	if after.Status.ServicePrincipalID != before.ServicePrincipalID ||
		after.Status.ClientID != before.ClientID {
		t.Errorf("the record now says %s/%s and said %s/%s; the ids are what find the identity "+
			"again, and losing them is losing the identity",
			after.Status.ServicePrincipalID, after.Status.ClientID,
			before.ServicePrincipalID, before.ClientID)
	}

	ready := readyOf(t, c.Client, issued)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != reasonNotServed {
		t.Errorf("Ready is %v, want False/%s -- Ready promises the exchange, and the exchange is "+
			"what stopped", ready, reasonNotServed)
	}
	if !strings.Contains(ready.Message, testNamespace) {
		t.Errorf("Ready says %q, and does not name the namespace whose removal caused it",
			ready.Message)
	}
}

// TestNothingIsWrittenBackWhileTheNamespaceIsUnclaimed covers the order the two
// halves of a removal are not in.
//
// Nothing sequences the account controller against the record's, and the
// wake-up reaches both at once. So the record's controller is driven first with
// the namespace still unclaimed, where it must ask Databricks for nothing at
// all, and then again after the claim lands and the trust goes -- and then a
// DatabricksServiceAccount pass and another record pass, which is the pair that
// would re-assert the trust on a record that already exists and put back what
// was just taken away.
func TestNothingIsWrittenBackWhileTheNamespaceIsUnclaimed(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asker, asking(testNamespace, "loader"))
	c.settle(t)

	nowServing(t, c.Client)

	// The account controller has not run, so nothing has claimed the namespace
	// and it still mints.
	c.records(t)
	if claimOn(t, c.Client, testNamespace) != "" {
		t.Fatal("the namespace was claimed before this ran; the ordering this test is about " +
			"was not the one it exercised")
	}
	if len(stub.policiesRemoved) != 0 {
		t.Errorf("asked Databricks to take the trust off %v while identities could still be "+
			"made here; whatever is made next is not in the set that was just worked through",
			stub.policiesRemoved)
	}
	waiting := readyOf(t, c.Client, c.issuedOf(t, asker))
	if waiting == nil || waiting.Reason != reasonMintingNotSuspended {
		t.Errorf("Ready is %v, want %s -- somebody waiting for this removal has to be able "+
			"to see that it has not started and why", waiting, reasonMintingNotSuspended)
	}

	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)
	if len(stub.policies) != 0 {
		t.Fatalf("federation policies are %v; the namespace is claimed and the trust is still "+
			"there", stub.policies)
	}

	c.project(t, testNamespace, testName)
	c.project(t, testNamespace, "loader")
	c.records(t)

	if len(stub.policies) != 0 {
		t.Errorf("federation policies are %v; the trust was written back over the removal, "+
			"and both passes will go on doing it to each other", stub.policies)
	}
	if len(stub.created) != 1 {
		t.Errorf("created %v, want the one made while the namespace was still served; an "+
			"identity was minted in a namespace this account had already stopped naming",
			stub.created)
	}
}

// TestNoServicePrincipalIsMintedForANamespaceOnItsWayOut covers the one
// interleaving a removal cannot converge its way out of.
//
// Between the edit and the claim landing, a ServiceAccount here still produces
// records: a pass of the DatabricksServiceAccount controller that read the
// account before the edit landed writes its record after it. A removal begun
// in that window enumerates the records it can see, and this one is not among
// them -- so it mints a service principal nobody asked for, with an
// applicationId no workload was ever given, and a later pass takes its trust
// away again. What is left is an object in the Databricks account that nothing
// wants and nothing explains, and it cannot be undone by converging harder: it
// is already there.
//
// The removal therefore waits until this operator has claimed the namespace
// before it asks Databricks anything, so that the set it acts on has stopped
// growing.
func TestNoServicePrincipalIsMintedForANamespaceOnItsWayOut(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asker, asking(testNamespace, "loader"))
	c.settle(t)

	var before dbxv1alpha1.DatabricksAccount
	if err := c.Client.Get(context.Background(), testOperatorRef, &before); err != nil {
		t.Fatal(err)
	}
	nowServing(t, c.Client)

	// The DatabricksServiceAccount controller in the middle of a pass it started
	// before the edit: it reads the account as it was, and everything else as it
	// is. That is the whole of the window, and no watch closes it -- the read
	// happened before the write landed and the record is written after.
	c.DatabricksServiceAccounts.Client = interceptor.NewClient(c.Client.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
			object client.Object, opts ...client.GetOption) error {
			if databricksAccount, is := object.(*dbxv1alpha1.DatabricksAccount); is {
				before.DeepCopyInto(databricksAccount)
				return nil
			}
			return c.Get(ctx, key, object, opts...)
		},
	})

	c.project(t, testNamespace, "loader")
	c.records(t)

	minted := c.issuedOf(t, asking(testNamespace, "loader"))
	if minted == nil {
		t.Fatal("no record was made for the ServiceAccount that asked before the edit was " +
			"visible; the window this test is about was not open")
	}
	if len(stub.created) != 1 {
		t.Errorf("created %v; a namespace on its way out was given a new service principal, "+
			"with an applicationId no workload was ever handed, and nothing in the Databricks "+
			"account says what it is for", stub.created)
	}
	if minted.Status.ServicePrincipalID != "" || minted.Status.ClientID != "" {
		t.Errorf("the record names %s/%s in Databricks; nothing may be made for a namespace this "+
			"account has stopped naming", minted.Status.ServicePrincipalID, minted.Status.ClientID)
	}
	if len(stub.policies) != 1 {
		t.Errorf("federation policies are %v, want only the one from before the edit; a policy "+
			"was written for an identity that should never have reached Databricks",
			stub.policies)
	}
	if len(stub.policiesRemoved) != 0 {
		t.Errorf("began taking the trust off %v while a record for this namespace was still "+
			"being made; a removal that starts here acts on a set that is still growing",
			stub.policiesRemoved)
	}
	ready := readyOf(t, c.Client, minted)
	if ready == nil || ready.Reason != reasonNotServed {
		t.Fatalf("Ready is %v, want %s -- the record names nothing in Databricks and never "+
			"will, and somebody looking at it has to be told that rather than left waiting",
			ready, reasonNotServed)
	}
	if !strings.Contains(ready.Message, testNamespace) {
		t.Errorf("Ready says %q and does not name the namespace that stopped being served",
			ready.Message)
	}
}

// TestNamingANamespaceAgainRestoresTheSameIdentity covers the way back, which is
// the reason nothing is destroyed on the way out.
//
// The record kept its service principal id and its client id, so converging it
// again writes the policy back onto the same service principal. The workload
// gets the client id it always had and every grant made on it still applies --
// no new identity, and nothing to grant again.
func TestNamingANamespaceAgainRestoresTheSameIdentity(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)
	before := c.issuedOf(t, asker).Status

	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)
	if len(stub.policies) != 0 {
		t.Fatal("the trust was not taken back, so there is nothing for this to restore")
	}

	nowServing(t, c.Client, testNamespace)
	c.records(t)

	if len(stub.policies) != 1 ||
		!strings.HasPrefix(stub.policies[0], before.ServicePrincipalID+" ") {
		t.Errorf("federation policies are %v, want one on service principal %s -- a namespace "+
			"coming back has to reach the identity it left with", stub.policies,
			before.ServicePrincipalID)
	}
	if len(stub.created) != 1 {
		t.Errorf("created %v; coming back made a second service principal, so the workload has a "+
			"new client id and every grant made on the first one points at nothing",
			stub.created)
	}

	after := c.issuedOf(t, asker)
	if after.Status.ClientID != before.ClientID {
		t.Errorf("the client id is now %s and was %s; every pod and every grant names the one it "+
			"was given", after.Status.ClientID, before.ClientID)
	}
	ready := readyOf(t, c.Client, after)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready is %v, want the exchange working again", ready)
	}
}

// TestAnAbsentAccountRemovesNothing covers the difference between an edit and
// an absence, which is the same input to the same question.
//
// A DatabricksAccount that has been deleted names no namespace, and neither does
// one this pass has not read yet. Read as "this namespace is out of scope" that
// would take the trust off every identity in the cluster on the strength of
// nobody having said anything -- and the way back needs the object somebody just
// deleted. Not knowing refuses to mint; it never removes.
func TestAnAbsentAccountRemovesNothing(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)

	// Claimed by this operator first, so that the only thing left between these
	// records and a removal is the account object being gone.
	claims(t, c.Client, testNamespace, testOperatorRef, time.Now())

	var declared dbxv1alpha1.DatabricksAccount
	if err := c.Client.Get(context.Background(), testOperatorRef, &declared); err != nil {
		t.Fatal(err)
	}
	if err := c.Client.Delete(context.Background(), &declared); err != nil {
		t.Fatal(err)
	}
	c.records(t)

	if len(stub.policiesRemoved) != 0 {
		t.Errorf("took the trust off %v; the account object is gone, which says nothing about "+
			"which namespaces it served, and the edit that would say so cannot be made on an "+
			"object that is not there", stub.policiesRemoved)
	}
	if len(stub.policies) != 1 {
		t.Errorf("federation policies are %v, want the one that was there; every identity in the "+
			"cluster stopped being exchangeable because one object was deleted", stub.policies)
	}
}

// TestARemovalThatDidNotLandIsNotReportedAsFinished covers the one thing this
// operator must never do about a call it could not make.
//
// The trust is still in place, so this cluster can still be exchanged for the
// identity -- the opposite of what the edit asked for. Reported as removed it
// would be a namespace that looks let go of and is not, and nobody would look at
// it again.
func TestARemovalThatDidNotLandIsNotReportedAsFinished(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)
	issued := c.issuedOf(t, asker)

	stub.removeErr = errors.New("the service is temporarily unavailable")
	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)

	if len(stub.policies) != 1 {
		t.Fatalf("federation policies are %v; the call failed, so the trust it would have "+
			"removed has to still be there for this test to mean anything", stub.policies)
	}
	ready := readyOf(t, c.Client, issued)
	if ready == nil || ready.Reason == reasonNotServed {
		t.Fatalf("Ready is %v; the removal did not go through and the record says it did", ready)
	}
	if ready.Reason != reasonRemovePoliciesFailed {
		t.Errorf("Ready is %v, want %s -- what went wrong is a call to Databricks, and the reason "+
			"is what somebody alerts on", ready, reasonRemovePoliciesFailed)
	}

	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	removed := policiesRemovedCondition(t, c.Client)
	if removed == nil || removed.Status != metav1.ConditionFalse {
		t.Fatalf("%s is %v, want False -- the object whose edit started the removal is the "+
			"only place somebody would look to see it finish", conditionFederationPoliciesRemoved,
			removed)
	}
	if !strings.Contains(removed.Message, testNamespace) {
		t.Errorf("%s says %q and does not name the namespace still holding trust, so finding it "+
			"means reading every record one at a time", conditionFederationPoliciesRemoved,
			removed.Message)
	}
	if claimOn(t, c.Client, testNamespace) != removedPoliciesBy(testOperatorRef) {
		t.Errorf("%s is no longer claimed by this operator while its trust is still in place; "+
			"minting would resume into a set this removal has not finished with",
			testNamespace)
	}
}

// TestANamespaceThisOperatorCannotClaimSaysSo covers the other half of the same
// promise, on the half of the removal that happens in the cluster.
//
// A refused write leaves a namespace this operator does not hold, so it removes
// nothing. Nothing else reports it -- the Namespace looks like every other
// enrolled one -- so the account it left says so rather than reporting a
// removal it did not make.
func TestANamespaceThisOperatorCannotClaimSaysSo(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	refusing := interceptor.Funcs{
		Apply: func(ctx context.Context, c client.WithWatch, object runtime.ApplyConfiguration,
			opts ...client.ApplyOption) error {
			if _, ok := object.(*corev1ac.NamespaceApplyConfiguration); ok {
				return errors.New("namespaces is forbidden")
			}
			return c.Apply(ctx, object, opts...)
		},
	}
	c := newControllersWith(t, stub, refusing,
		databricksAccountServing(otherNamespace),
		mintingNamespace(testNamespace),
		asker,
		recordFor(asker),
	)

	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

	if claimOn(t, c.Client, testNamespace) != "" {
		t.Fatal("the write was not refused, so this test asserts nothing")
	}
	removed := policiesRemovedCondition(t, c.Client)
	if removed == nil || removed.Status != metav1.ConditionFalse {
		t.Fatalf("%s is %v, want False -- the namespace is unclaimed and still mints",
			conditionFederationPoliciesRemoved, removed)
	}
	for _, want := range []string{testNamespace, dbxv1alpha1.RemovingPoliciesLabel} {
		if !strings.Contains(removed.Message, want) {
			t.Errorf("%s says %q, want it to name %s so that whoever can write a Namespace knows "+
				"what is missing", conditionFederationPoliciesRemoved, removed.Message, want)
		}
	}
}

// TestASecondOperatorWaitsWhileTheFirstHoldsTheNamespace covers the exclusion
// itself, which is the only reason there is one key rather than one per
// operator.
//
// Two operators removing policies in one namespace at the same time are each
// working through a set the other is still adding to. The loser is told so by
// the API server -- the label key already belongs to a different field manager
// -- and what it does about it is nothing: it names the holder and asks
// Databricks for nothing at all, so the winner's set stops moving and its own
// is untouched.
func TestASecondOperatorWaitsWhileTheFirstHoldsTheNamespace(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)
	issued := c.issuedOf(t, asker)

	claims(t, c.Client, testNamespace, otherOperator, time.Now())
	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)

	if got := claimOn(t, c.Client, testNamespace); got != removedPoliciesBy(otherOperator) {
		t.Errorf("%s is claimed by %q, want %q; the second operator took a claim the first was "+
			"still using, and both are now removing from a set the other is changing",
			testNamespace, got, removedPoliciesBy(otherOperator))
	}
	if len(stub.policiesRemoved) != 0 {
		t.Errorf("asked Databricks to take the trust off %v without holding %s; losing the "+
			"claim is the whole of what stops a second removal", stub.policiesRemoved,
			dbxv1alpha1.RemovingPoliciesLabel)
	}
	if len(stub.policies) != 1 {
		t.Errorf("federation policies are %v, want the one that was there", stub.policies)
	}

	ready := readyOf(t, c.Client, issued)
	if ready == nil || ready.Reason != reasonAnotherRemoval {
		t.Fatalf("Ready is %v, want %s -- waiting on another operator is a thing somebody has to "+
			"be able to see", ready, reasonAnotherRemoval)
	}
	if !strings.Contains(ready.Message, removedPoliciesBy(otherOperator)) {
		t.Errorf("Ready says %q and does not name who it is waiting for, so finding out means "+
			"reading a Namespace by hand", ready.Message)
	}

	removed := policiesRemovedCondition(t, c.Client)
	if removed == nil || removed.Status != metav1.ConditionFalse {
		t.Errorf("%s is %v, want False -- nothing has been removed and the account says it has",
			conditionFederationPoliciesRemoved, removed)
	}
}

// TestAnOperatorWhoseClaimWasTakenStopsRemovingPolicies covers the half of the
// exclusion that has no code of its own.
//
// A holder that was taken over finds out on its next write, because the field
// belongs to somebody else now and the API server refuses it. There is no
// liveness check anywhere: the refusal is the check, and what it produces is an
// operator that stops removing and goes back to asking for the namespace.
func TestAnOperatorWhoseClaimWasTakenStopsRemovingPolicies(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker, asking(testNamespace, "loader"))
	c.settle(t)

	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	if claimOn(t, c.Client, testNamespace) != removedPoliciesBy(testOperatorRef) {
		t.Fatal("this operator never held the namespace, so there was nothing for the other " +
			"one to take")
	}

	claims(t, c.Client, testNamespace, otherOperator, time.Now(), client.ForceOwnership)

	stub.policiesRemoved = nil
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)

	if got := claimOn(t, c.Client, testNamespace); got != removedPoliciesBy(otherOperator) {
		t.Errorf("%s is claimed by %q, want %q; the operator that lost the namespace wrote its "+
			"own name back over the one that took it", testNamespace, got,
			removedPoliciesBy(otherOperator))
	}
	if len(stub.policiesRemoved) != 0 {
		t.Errorf("went on taking the trust off %v after losing the namespace; the set it is "+
			"working through is now the other operator's to change", stub.policiesRemoved)
	}
	removed := policiesRemovedCondition(t, c.Client)
	if removed == nil || removed.Status != metav1.ConditionFalse {
		t.Errorf("%s is %v, want False -- this operator holds nothing and has removed nothing",
			conditionFederationPoliciesRemoved, removed)
	}
}

// TestAClaimNobodyHasRefreshedCanBeTakenAndAFreshOneCannot covers the lease, and
// both directions of it in one place because the threshold is only a decision if
// each side of it does something different.
//
// An operator killed halfway through a removal would otherwise leave a
// namespace nobody can mint in and nobody can remove anything in, until a
// person noticed and edited a Namespace. The timestamp is rewritten on every
// pass, so one that has not moved is a holder that is not running -- and taking
// it is the one place a claim is written over somebody else's.
func TestAClaimNobodyHasRefreshedCanBeTakenAndAFreshOneCannot(t *testing.T) {
	t.Parallel()
	for _, held := range []struct {
		name  string
		since time.Duration
		want  types.NamespacedName
	}{
		{"stale", policyRemovalClaimStale + time.Minute, testOperatorRef},
		{"fresh", policyRemovalClaimStale - time.Minute, otherOperator},
	} {
		t.Run(held.name, func(t *testing.T) {
			t.Parallel()
			stub := &stubClients{}
			asker := asking(testNamespace, testName)
			c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
			c.settle(t)

			claims(t, c.Client, testNamespace, otherOperator, time.Now().Add(-held.since))
			nowServing(t, c.Client)
			reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

			if got := claimOn(t, c.Client, testNamespace); got != removedPoliciesBy(held.want) {
				t.Errorf("%s is claimed by %q, want %q after %s without a word from its holder",
					testNamespace, got, removedPoliciesBy(held.want), held.since)
			}
		})
	}
}

// TestAHolderSaysAgainOnEveryPassThatItIsStillThere covers the half of the lease
// the threshold cannot check by itself.
//
// A claim is not written once and left. managedFields[].time does not move when
// the applied content is identical -- measured against a live cluster -- so the
// claim carries a time of its own and its holder writes it again on every pass.
// Without that, a removal with more identities in it than five minutes of work
// would age past the threshold while it was still going, and the next operator
// along would take the namespace out from under it: two removals at once, each
// working through a set the other is changing, which is the whole of what the
// claim exists to prevent.
func TestAHolderSaysAgainOnEveryPassThatItIsStillThere(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)

	// Old enough that a claim left alone would be a minute from being taken, and
	// this operator's own, so what the pass does to it is a refresh and not a
	// conflict.
	stopped := time.Now().Add(time.Minute - policyRemovalClaimStale).UTC().Truncate(time.Second)
	claims(t, c.Client, testNamespace, testOperatorRef, stopped)
	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

	if got := claimOn(t, c.Client, testNamespace); got != removedPoliciesBy(testOperatorRef) {
		t.Fatalf("%s is claimed by %q, want %q -- there is no claim of this operator's to refresh",
			testNamespace, got, removedPoliciesBy(testOperatorRef))
	}
	if said := claimedSince(t, c.Client, testNamespace); !said.After(stopped) {
		t.Errorf("the claim still says %s, which is what it said before this pass. One that is "+
			"not written again ages out while its holder is still removing, and the next "+
			"operator along takes the namespace from under it", said)
	}
}

// TestReleasingLetsTheOtherOperatorMintAgain covers what the claim being a claim
// and not a revocation is for.
//
// The failure this replaced: one operator took the cluster's mint label off, and
// every other operator serving that namespace stopped minting for good, because
// no operator may write that label back and only a person could. Here minting is
// suspended for exactly as long as one removal takes, the permission
// underneath is never touched, and the other operator resumes with nobody doing
// anything.
func TestReleasingLetsTheOtherOperatorMintAgain(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	theirs := askingOf(testNamespace, "loader", otherOperator.String())
	c := newControllers(t, stub,
		databricksAccountServing(testNamespace),
		mintingNamespace(testNamespace),
		asker,
		theirs,
		serving(otherOperator, testNamespace),
	)
	c.settle(t)
	other := databricksServiceAccountsFor(c.DatabricksServiceAccounts.Scheme, c.Client, otherOperator)

	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

	if _, err := other.Reconcile(context.Background(), requestFor(theirs)); err != nil {
		t.Fatal(err)
	}
	if made := recordsIn(t, c.Client, otherOperator.Namespace); made != 0 {
		t.Errorf("operator %s issued %d identities while %s was having its policies removed; the "+
			"set the removal is working through is still growing", otherOperator, made,
			testNamespace)
	}

	c.records(t)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

	if got := claimOn(t, c.Client, testNamespace); got != "" {
		t.Fatalf("%s is still claimed by %q after the removal finished; every other operator "+
			"serving it waits for a person", testNamespace, got)
	}
	if labelsOf(t, c.Client, testNamespace)[dbxv1alpha1.MintLabel] != dbxv1alpha1.Enabled {
		t.Fatalf("%s lost %s; that is the cluster's permission, and nothing this operator does "+
			"gives it back", testNamespace, dbxv1alpha1.MintLabel)
	}

	if _, err := other.Reconcile(context.Background(), requestFor(theirs)); err != nil {
		t.Fatal(err)
	}
	if made := recordsIn(t, c.Client, otherOperator.Namespace); made != 1 {
		t.Errorf("operator %s issued %d identities after the removal finished, want 1; its "+
			"minting stopped because another operator removed policies in a namespace they "+
			"share", otherOperator, made)
	}
}

// TestAnIdentityAlreadyLetGoOfDoesNotTakeTheNamespaceBack covers what a record
// already let go of does for the rest of its life, which is where giving the
// namespace back either holds or comes undone.
//
// Every record is looked at again on its own interval, and this one is looked at
// for as long as it exists. Asking for the namespace each time would take
// minting away from every other operator serving it once an interval, for ever,
// to remove a policy that is not there -- and the account beside it would flip
// between finished and not finished at the same rate.
func TestAnIdentityAlreadyLetGoOfDoesNotTakeTheNamespaceBack(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)

	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	if claimOn(t, c.Client, testNamespace) != "" {
		t.Fatal("the namespace was not given back, so there is nothing here to take again")
	}
	asked := len(stub.policiesRemoved)

	c.records(t)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

	if got := claimOn(t, c.Client, testNamespace); got != "" {
		t.Errorf("%s is claimed by %q again with nothing left to remove; every other operator "+
			"serving it stops minting once an interval, for ever", testNamespace, got)
	}
	if len(stub.policiesRemoved) != asked {
		t.Errorf("asked Databricks to take the trust off %v again; the trust it names went on "+
			"the pass before", stub.policiesRemoved[asked:])
	}
	ready := readyOf(t, c.Client, c.issuedOf(t, asker))
	if ready == nil || ready.Reason != reasonNotServed {
		t.Errorf("Ready is %v, want %s -- the identity was let go of and nothing since has "+
			"changed that", ready, reasonNotServed)
	}
	removed := policiesRemovedCondition(t, c.Client)
	if removed == nil || removed.Status != metav1.ConditionTrue {
		t.Errorf("%s is %v, want True -- the removal finished, and an account that says so "+
			"and then says otherwise is one nobody can alert on", conditionFederationPoliciesRemoved,
			removed)
	}
}
