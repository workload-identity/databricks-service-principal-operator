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
	"slices"
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
// All three controllers over one fake client, because the destruction is not in
// any one of them: the account controller claims the namespace and the record's
// controller destroys the identities in it, and a test that ran either alone
// would be asserting about half of it.
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
	if err := c.Apply(context.Background(), destructionClaimFor(namespace, operator, since), opts...); err != nil {
		t.Fatal(err)
	}
}

// claimOn is the operator named on a namespace, or "" for one nobody has
// claimed.
func claimOn(t *testing.T, c client.Client, name string) string {
	t.Helper()
	return labelsOf(t, c, name)[dbxv1alpha1.DestroyingIdentitiesLabel]
}

// claimedSince is when the holder of a namespace last said it was still there.
func claimedSince(t *testing.T, c client.Client, name string) time.Time {
	t.Helper()
	var namespace corev1.Namespace
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, &namespace); err != nil {
		t.Fatal(err)
	}
	said := namespace.Annotations[dbxv1alpha1.DestroyingIdentitiesSinceAnnotation]
	at, err := time.Parse(time.RFC3339, said)
	if err != nil {
		t.Fatalf("%s carries %q, which is not a time: %v",
			dbxv1alpha1.DestroyingIdentitiesSinceAnnotation, said, err)
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

// identitiesDestroyedCondition is what the account says about the namespaces it
// stopped serving.
func identitiesDestroyedCondition(t *testing.T, c client.Client) *metav1.Condition {
	t.Helper()
	var got dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(), testOperatorRef, &got); err != nil {
		t.Fatal(err)
	}
	return meta.FindStatusCondition(got.Status.Conditions, conditionIdentitiesDestroyed)
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
// a destruction that happens in the cluster.
//
// The claim names this operator, so that a second one serving the same namespace
// can tell whose destruction it is waiting for. The cluster's own labels are left
// exactly as they were: they are the permission given to every operator here at
// once, and taking one off would be this operator revoking somebody else's, for
// good, since no operator may write one back. A namespace still named is
// untouched, because a destruction that reached one is a destruction that took
// every team on the account with it.
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

	if got := claimOn(t, c.Client, testNamespace); got != destructionClaimedBy(testOperatorRef) {
		t.Errorf("%s is claimed by %q, want %q; identities can still be minted there while this "+
			"operator destroys the ones it has", testNamespace, got, destructionClaimedBy(testOperatorRef))
	}
	minting, err := namespaceMints(context.Background(), c.Client, testNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if minting {
		t.Errorf("%s still mints; whatever is made next is not in the set being destroyed",
			testNamespace)
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
// nothing in is not one it has anything to destroy in. Another operator on the
// same cluster serves namespaces this one does not name, and a claim there would
// suspend that operator's minting for a destruction with nothing in it.
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

// TestTakingANamespaceOffTheListDestroysTheIdentitiesInIt covers what the edit
// means, which is the whole of this behaviour.
//
// Not a pause: the record goes and the service principal it named is deleted in
// Databricks, so every grant anybody made on it goes too. That is what makes the
// invariant checkable -- every service principal carrying this operator's marker
// is in a namespace on the list -- and a state that reads as "not reachable" but
// comes back on one edit could never have been.
func TestTakingANamespaceOffTheListDestroysTheIdentitiesInIt(t *testing.T) {
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
	c.records(t)

	if len(stub.deleted) != 1 || stub.deleted[0] != before.ServicePrincipalID {
		t.Errorf("deleted %v, want service principal %s; a namespace off the list still has an "+
			"identity in Databricks that this account issued, and every grant on it stands",
			stub.deleted, before.ServicePrincipalID)
	}
	if len(stub.policies) != 0 {
		t.Errorf("federation policies are %v; the service principal was deleted and its trust "+
			"outlived it", stub.policies)
	}
	if len(stub.policiesRemoved) != 0 {
		t.Errorf("took the trust off %v instead of destroying the identity; that leaves a service "+
			"principal in the account with every grant on it, which one edit puts back in reach",
			stub.policiesRemoved)
	}
	if after := c.issuedOf(t, asker); after != nil {
		t.Errorf("the record is still there, saying %v; a record that stays is an identity this "+
			"account has not finished destroying", after.Status)
	}
}

// TestNoPodIsEquippedWithAnIdentityThatWasDestroyed covers the copy in the
// tenant's namespace, which is the only one of these objects a webhook reads.
//
// The DatabricksServiceAccount carries the client id every new pod here is
// given. Left standing after the identity behind it is destroyed, it would go on
// equipping pods with a client id that resolves to nothing -- a workload that
// starts, authenticates and fails on its first call, with nothing saying why. So
// the entries go when the records do.
func TestNoPodIsEquippedWithAnIdentityThatWasDestroyed(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)
	if principalOf(t, c.Client) == nil {
		t.Fatal("no DatabricksServiceAccount was written, so there is nothing here to take away")
	}

	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)
	c.records(t)
	c.project(t, testNamespace, testName)

	if got := principalOf(t, c.Client); got != nil {
		t.Errorf("the DatabricksServiceAccount still carries %v; every pod created here is given "+
			"a client id whose service principal was destroyed", got.Status.Identities)
	}
}

// TestANamespaceStillOnTheListIsUntouchedWhileAnotherIsEmptied covers the reach
// of one edit.
//
// Taking one namespace off the list is not the account withdrawing from the
// cluster. The other team's identity is not deleted, its trust is not taken off,
// and its namespace is not claimed -- so nothing about their workloads changes
// while somebody else's are being destroyed.
func TestANamespaceStillOnTheListIsUntouchedWhileAnotherIsEmptied(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	leaving := asking(testNamespace, testName)
	staying := asking(otherNamespace, "loader")
	c := newControllers(t, stub,
		databricksAccountServing(testNamespace, otherNamespace),
		mintingNamespace(testNamespace),
		mintingNamespace(otherNamespace),
		leaving, staying,
	)
	for range 4 {
		c.project(t, testNamespace, testName)
		c.project(t, otherNamespace, "loader")
		c.records(t)
	}
	kept := c.issuedOf(t, staying)
	if kept == nil || kept.Status.ServicePrincipalID == "" {
		t.Fatal("the namespace that stays was issued nothing, so there is nothing here to keep")
	}

	nowServing(t, c.Client, otherNamespace)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)
	c.records(t)

	if slices.Contains(stub.deleted, kept.Status.ServicePrincipalID) {
		t.Errorf("deleted %v, which includes %s in %s -- a namespace this account still names",
			stub.deleted, kept.Status.ServicePrincipalID, otherNamespace)
	}
	if got := c.issuedOf(t, staying); got == nil {
		t.Errorf("the record in %s is gone; one team leaving the list destroyed another team's "+
			"identity", otherNamespace)
	}
	if got := claimOn(t, c.Client, otherNamespace); got != "" {
		t.Errorf("%s is claimed by %q and it is still on the list; its minting is suspended for "+
			"somebody else's destruction", otherNamespace, got)
	}
	ready := readyOf(t, c.Client, kept)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready in %s is %v, want the exchange still working", otherNamespace, ready)
	}
}

// TestADestructionInterruptedPartWayIsContinued covers the reason the claim is a
// lease on a Namespace rather than anything this operator remembers.
//
// An operator killed between deleting a record and deleting the service
// principal it names has left a record in Databricks' way out and nothing in
// memory saying so. The next pass reads the deletion timestamp that is already
// there and carries on from it: the finalizer is what holds the record, and it
// holds it whatever the account now says about the namespace.
func TestADestructionInterruptedPartWayIsContinued(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)
	issued := c.issuedOf(t, asker)
	if issued == nil || issued.Status.ServicePrincipalID == "" {
		t.Fatal("nothing was issued to start from")
	}

	// The pass that deletes the record and dies before the service principal
	// goes: Databricks refuses, so the finalizer stays on and the object is left
	// terminating.
	stub.deleteErr = errors.New("the service is temporarily unavailable")
	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)
	c.records(t)

	halfway := c.issuedOf(t, asker)
	if halfway == nil || halfway.DeletionTimestamp.IsZero() {
		t.Fatalf("the record is %v; the destruction never began, so there is nothing here that "+
			"was interrupted", halfway)
	}
	if len(stub.deleted) != 0 {
		t.Fatalf("deleted %v; the call was supposed to fail, so this asserts nothing", stub.deleted)
	}

	// Another operator takes the namespace while this one is halfway through.
	// Nothing about that may stop a record already on its way out: the service
	// principal it names is one nothing else records.
	claims(t, c.Client, testNamespace, otherOperator, time.Now(), client.ForceOwnership)
	stub.deleteErr = nil
	c.records(t)

	if len(stub.deleted) != 1 || stub.deleted[0] != issued.Status.ServicePrincipalID {
		t.Errorf("deleted %v, want service principal %s; the destruction stopped where it was "+
			"interrupted and the service principal it had already let go of stayed",
			stub.deleted, issued.Status.ServicePrincipalID)
	}
	if after := c.issuedOf(t, asker); after != nil {
		t.Errorf("the record is still there; its service principal is gone and nothing releases it")
	}
}

// TestARecordWhoseServicePrincipalIsAlreadyGoneStillGoes covers the one state
// that converges no further, in the one place that has to reach every state.
//
// A record latched as removed in Databricks is one nothing rebuilds, and left
// standing in a namespace off the list it would be counted for ever as an
// identity the destruction has not finished with -- holding the claim, so that
// nobody mints there again and the account never says it is done. There is
// nothing in Databricks to delete for it, and that is not a reason to keep it.
func TestARecordWhoseServicePrincipalIsAlreadyGoneStillGoes(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)

	stub.gone = c.issuedOf(t, asker).Status.ServicePrincipalID
	c.records(t)
	if got := readyOf(t, c.Client, c.issuedOf(t, asker)); got == nil || got.Reason != reasonRemovedInDatabricks {
		t.Fatalf("Ready is %v, want %s -- the record is not in the state this test is about",
			got, reasonRemovedInDatabricks)
	}

	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)
	c.records(t)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

	if got := c.issuedOf(t, asker); got != nil {
		t.Errorf("the record is still there, saying %v; it names nothing in Databricks and is "+
			"counted as something left to destroy for as long as it exists", got.Status)
	}
	if got := claimOn(t, c.Client, testNamespace); got != "" {
		t.Errorf("%s is still claimed by %q with nothing left in it; no operator mints there "+
			"until a person acts", testNamespace, got)
	}
	destroyed := identitiesDestroyedCondition(t, c.Client)
	if destroyed == nil || destroyed.Status != metav1.ConditionTrue {
		t.Errorf("%s is %v, want True -- nothing this account issued is left in %s",
			conditionIdentitiesDestroyed, destroyed, testNamespace)
	}
}

// TestNothingIsDestroyedWhileTheNamespaceIsUnclaimed covers the order the two
// halves of a destruction are not in.
//
// Nothing sequences the account controller against the record's, and the
// wake-up reaches both at once. So the record's controller is driven first with
// the namespace still unclaimed, where it must destroy nothing at all, and then
// again after the claim lands.
func TestNothingIsDestroyedWhileTheNamespaceIsUnclaimed(t *testing.T) {
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
	if len(stub.deleted) != 0 {
		t.Errorf("deleted %v while identities could still be made here; whatever is made next is "+
			"not in the set that was just worked through", stub.deleted)
	}
	waiting := c.issuedOf(t, asker)
	if waiting == nil || !waiting.DeletionTimestamp.IsZero() {
		t.Fatalf("the record is %v; it was put on its way out before anything suspended minting "+
			"in the namespace it names", waiting)
	}
	ready := readyOf(t, c.Client, waiting)
	if ready == nil || ready.Reason != reasonMintingNotSuspended {
		t.Errorf("Ready is %v, want %s -- somebody waiting for this destruction has to be able "+
			"to see that it has not started and why", ready, reasonMintingNotSuspended)
	}

	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)
	c.records(t)

	if len(stub.deleted) != 1 {
		t.Errorf("deleted %v, want the one identity there was; the namespace is claimed and the "+
			"destruction has not happened", stub.deleted)
	}
}

// TestNoServicePrincipalIsMintedForANamespaceOnItsWayOut covers the one
// interleaving a destruction cannot converge its way out of.
//
// Between the edit and the claim landing, a ServiceAccount here still produces
// records: a pass of the DatabricksServiceAccount controller that read the
// account before the edit landed writes its record after it. If that record then
// minted, this operator would create a service principal in a namespace nothing
// of its account's belongs in and destroy it again on a later pass -- an
// applicationId no workload was ever given, made and unmade for nothing.
//
// So a record for a namespace this account no longer names reaches Databricks
// for nothing at all: it is destroyed from the state it is in, which is one where
// nothing can exist.
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
	minted := c.issuedOf(t, asking(testNamespace, "loader"))
	if minted == nil {
		t.Fatal("no record was made for the ServiceAccount that asked before the edit was " +
			"visible; the window this test is about was not open")
	}

	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)
	c.records(t)

	if len(stub.created) != 1 {
		t.Errorf("created %v; a namespace on its way out was given a new service principal, "+
			"with an applicationId no workload was ever handed, and nothing in the Databricks "+
			"account says what it is for", stub.created)
	}
	if got := c.issuedOf(t, asking(testNamespace, "loader")); got != nil {
		t.Errorf("the record made in the window is still there, saying %v; it names a namespace "+
			"this account does not serve and nothing will ever make it name anything",
			got.Status)
	}
	if len(stub.deleted) != 1 {
		t.Errorf("deleted %v, want only the identity that existed before the edit; the record "+
			"made in the window had nothing in Databricks to delete", stub.deleted)
	}
}

// TestNamingANamespaceAgainIssuesANewIdentity covers what coming back is, which
// is the price of the way out being a destruction.
//
// It is not a restore. The service principal that was there is gone with every
// grant on it, so what a returning namespace gets is a new one, with a client id
// no pod was ever given and nothing granted to it. Whoever granted anything
// grants it again.
func TestNamingANamespaceAgainIssuesANewIdentity(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)
	before := c.issuedOf(t, asker).Status

	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)
	c.records(t)
	if c.issuedOf(t, asker) != nil {
		t.Fatal("the identity was not destroyed, so there is nothing here to come back from")
	}
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

	nowServing(t, c.Client, testNamespace)
	c.settle(t)

	after := c.issuedOf(t, asker)
	if after == nil || after.Status.ServicePrincipalID == "" {
		t.Fatalf("the namespace is served again and %v was issued; a namespace that comes back "+
			"gets identities like any other", after)
	}
	if after.Status.ServicePrincipalID == before.ServicePrincipalID ||
		after.Status.ClientID == before.ClientID {
		t.Errorf("the identity is %s/%s again, and %s/%s was destroyed in Databricks; this "+
			"record names something that is not there",
			after.Status.ServicePrincipalID, after.Status.ClientID,
			before.ServicePrincipalID, before.ClientID)
	}
	if len(stub.created) != 2 {
		t.Errorf("created %v, want one before the edit and one after it", stub.created)
	}
	ready := readyOf(t, c.Client, after)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready is %v, want the new identity exchangeable", ready)
	}
}

// TestAnAbsentAccountDestroysNothing covers the difference between an edit and
// an absence, which is the same input to the same question.
//
// A DatabricksAccount that has been deleted names no namespace, and neither does
// one this pass has not read yet. Read as "this namespace is out of scope" that
// would destroy every identity in the cluster on the strength of nobody having
// said anything. Not knowing refuses to mint; it never destroys.
func TestAnAbsentAccountDestroysNothing(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)

	// Claimed by this operator first, so that the only thing left between these
	// records and a destruction is the account object being gone.
	claims(t, c.Client, testNamespace, testOperatorRef, time.Now())

	var declared dbxv1alpha1.DatabricksAccount
	if err := c.Client.Get(context.Background(), testOperatorRef, &declared); err != nil {
		t.Fatal(err)
	}
	if err := c.Client.Delete(context.Background(), &declared); err != nil {
		t.Fatal(err)
	}
	c.records(t)
	c.records(t)

	if len(stub.deleted) != 0 {
		t.Errorf("deleted %v; the account object is gone, which says nothing about which "+
			"namespaces it served, and the edit that would say so cannot be made on an object "+
			"that is not there", stub.deleted)
	}
	if c.issuedOf(t, asker) == nil {
		t.Error("the record is gone; every identity in the cluster was destroyed because one " +
			"object was deleted")
	}
}

// TestAnUnreachableDatabricksLeavesTheIdentitiesCountedAndNothingReleased
// covers the promise being eventual, which is the one place this design admits
// its invariant is false.
//
// The namespace is off the list and the identities are still in the account,
// because destroying them is a call to Databricks and Databricks did not answer.
// Everything then rests on two things: the count says how many are left, and the
// namespace stays claimed -- because minting resuming into a namespace this
// operator has not finished emptying would put new identities in the set it is
// still working through.
func TestAnUnreachableDatabricksLeavesTheIdentitiesCountedAndNothingReleased(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)

	stub.deleteErr = errors.New("the service is temporarily unavailable")
	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)
	c.records(t)

	if len(stub.deleted) != 0 {
		t.Fatalf("deleted %v; the call failed, so the identity it names has to still be there "+
			"for this test to mean anything", stub.deleted)
	}
	held := c.issuedOf(t, asker)
	if held == nil {
		t.Fatal("the record is gone and its service principal is not; nothing in the cluster " +
			"names it now, so nothing will ever remove it")
	}
	ready := readyOf(t, c.Client, held)
	if ready == nil || ready.Reason != reasonDeleteFailed {
		t.Errorf("Ready is %v, want %s -- what went wrong is a call to Databricks, and the reason "+
			"is what somebody alerts on", ready, reasonDeleteFailed)
	}

	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	destroyed := identitiesDestroyedCondition(t, c.Client)
	if destroyed == nil || destroyed.Status != metav1.ConditionFalse {
		t.Fatalf("%s is %v, want False -- the object whose edit started the destruction is the "+
			"only place somebody would look to see it finish", conditionIdentitiesDestroyed,
			destroyed)
	}
	if !strings.Contains(destroyed.Message, testNamespace+" (1)") {
		t.Errorf("%s says %q, and does not say how many identities are left in %s. That count is "+
			"the only place an identity stranded by an account nobody can reach is visible",
			conditionIdentitiesDestroyed, destroyed.Message, testNamespace)
	}
	if claimOn(t, c.Client, testNamespace) != destructionClaimedBy(testOperatorRef) {
		t.Errorf("%s is no longer claimed by this operator while identities of its account are "+
			"still there; minting would resume into a set this destruction has not finished "+
			"with", testNamespace)
	}
}

// TestANamespaceThisOperatorCannotClaimSaysSo covers the other half of the same
// promise, on the half of the destruction that happens in the cluster.
//
// A refused write leaves a namespace this operator does not hold, so it destroys
// nothing. Nothing else reports it -- the Namespace looks like every other
// enrolled one -- so the account it left says so rather than reporting a
// destruction it did not make.
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
	destroyed := identitiesDestroyedCondition(t, c.Client)
	if destroyed == nil || destroyed.Status != metav1.ConditionFalse {
		t.Fatalf("%s is %v, want False -- the namespace is unclaimed and still mints",
			conditionIdentitiesDestroyed, destroyed)
	}
	for _, want := range []string{testNamespace, dbxv1alpha1.DestroyingIdentitiesLabel} {
		if !strings.Contains(destroyed.Message, want) {
			t.Errorf("%s says %q, want it to name %s so that whoever can write a Namespace knows "+
				"what is missing", conditionIdentitiesDestroyed, destroyed.Message, want)
		}
	}
}

// TestASecondOperatorWaitsWhileTheFirstHoldsTheNamespace covers the exclusion
// itself, which is the only reason there is one key rather than one per
// operator.
//
// Two operators destroying identities in one namespace at the same time are each
// working through a set the other is still adding to. The loser is told so by
// the API server -- the label key already belongs to a different field manager
// -- and what it does about it is nothing: it names the holder and destroys
// nothing, so the winner's set stops moving and its own is untouched.
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
	c.records(t)

	if got := claimOn(t, c.Client, testNamespace); got != destructionClaimedBy(otherOperator) {
		t.Errorf("%s is claimed by %q, want %q; the second operator took a claim the first was "+
			"still using, and both are now destroying from a set the other is changing",
			testNamespace, got, destructionClaimedBy(otherOperator))
	}
	if len(stub.deleted) != 0 {
		t.Errorf("deleted %v without holding %s; losing the claim is the whole of what stops a "+
			"second destruction", stub.deleted, dbxv1alpha1.DestroyingIdentitiesLabel)
	}
	if got := c.issuedOf(t, asker); got == nil || !got.DeletionTimestamp.IsZero() {
		t.Errorf("the record is %v; it was put on its way out while another operator holds the "+
			"namespace", got)
	}

	ready := readyOf(t, c.Client, issued)
	if ready == nil || ready.Reason != reasonAnotherDestruction {
		t.Fatalf("Ready is %v, want %s -- waiting on another operator is a thing somebody has to "+
			"be able to see", ready, reasonAnotherDestruction)
	}
	if !strings.Contains(ready.Message, destructionClaimedBy(otherOperator)) {
		t.Errorf("Ready says %q and does not name who it is waiting for, so finding out means "+
			"reading a Namespace by hand", ready.Message)
	}

	destroyed := identitiesDestroyedCondition(t, c.Client)
	if destroyed == nil || destroyed.Status != metav1.ConditionFalse {
		t.Errorf("%s is %v, want False -- nothing has been destroyed and the account says it has",
			conditionIdentitiesDestroyed, destroyed)
	}
}

// TestAnOperatorWhoseClaimWasTakenStopsDestroying covers the half of the
// exclusion that has no code of its own.
//
// A holder that was taken over finds out on its next write, because the field
// belongs to somebody else now and the API server refuses it. There is no
// liveness check anywhere: the refusal is the check, and what it produces is an
// operator that stops and goes back to asking for the namespace.
func TestAnOperatorWhoseClaimWasTakenStopsDestroying(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)

	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	if claimOn(t, c.Client, testNamespace) != destructionClaimedBy(testOperatorRef) {
		t.Fatal("this operator never held the namespace, so there was nothing for the other " +
			"one to take")
	}

	claims(t, c.Client, testNamespace, otherOperator, time.Now(), client.ForceOwnership)

	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	c.records(t)

	if got := claimOn(t, c.Client, testNamespace); got != destructionClaimedBy(otherOperator) {
		t.Errorf("%s is claimed by %q, want %q; the operator that lost the namespace wrote its "+
			"own name back over the one that took it", testNamespace, got,
			destructionClaimedBy(otherOperator))
	}
	if got := c.issuedOf(t, asker); got == nil || !got.DeletionTimestamp.IsZero() {
		t.Errorf("the record is %v; this operator went on destroying after losing the namespace, "+
			"and the set it is working through is now the other operator's to change", got)
	}
	destroyed := identitiesDestroyedCondition(t, c.Client)
	if destroyed == nil || destroyed.Status != metav1.ConditionFalse {
		t.Errorf("%s is %v, want False -- this operator holds nothing and has destroyed nothing",
			conditionIdentitiesDestroyed, destroyed)
	}
}

// TestAClaimNobodyHasRefreshedCanBeTakenAndAFreshOneCannot covers the lease, and
// both directions of it in one place because the threshold is only a decision if
// each side of it does something different.
//
// An operator killed halfway through a destruction would otherwise leave a
// namespace nobody can mint in and nobody can destroy anything in, until a
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
		{"stale", destructionClaimStale + time.Minute, testOperatorRef},
		{"fresh", destructionClaimStale - time.Minute, otherOperator},
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

			if got := claimOn(t, c.Client, testNamespace); got != destructionClaimedBy(held.want) {
				t.Errorf("%s is claimed by %q, want %q after %s without a word from its holder",
					testNamespace, got, destructionClaimedBy(held.want), held.since)
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
// Without that, a destruction with more identities in it than five minutes of
// work would age past the threshold while it was still going, and the next
// operator along would take the namespace out from under it: two destructions at
// once, each working through a set the other is changing, which is the whole of
// what the claim exists to prevent.
func TestAHolderSaysAgainOnEveryPassThatItIsStillThere(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)

	// Old enough that a claim left alone would be a minute from being taken, and
	// this operator's own, so what the pass does to it is a refresh and not a
	// conflict.
	stopped := time.Now().Add(time.Minute - destructionClaimStale).UTC().Truncate(time.Second)
	claims(t, c.Client, testNamespace, testOperatorRef, stopped)
	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

	if got := claimOn(t, c.Client, testNamespace); got != destructionClaimedBy(testOperatorRef) {
		t.Fatalf("%s is claimed by %q, want %q -- there is no claim of this operator's to refresh",
			testNamespace, got, destructionClaimedBy(testOperatorRef))
	}
	if said := claimedSince(t, c.Client, testNamespace); !said.After(stopped) {
		t.Errorf("the claim still says %s, which is what it said before this pass. One that is "+
			"not written again ages out while its holder is still destroying, and the next "+
			"operator along takes the namespace from under it", said)
	}
}

// TestReleasingLetsTheOtherOperatorMintAgain covers what the claim being a claim
// and not a revocation is for.
//
// The failure this replaced: one operator took the cluster's mint label off, and
// every other operator serving that namespace stopped minting for good, because
// no operator may write that label back and only a person could. Here minting is
// suspended for exactly as long as one destruction takes, the permission
// underneath is never touched, and the other operator resumes with nobody doing
// anything.
//
// The namespace this operator gives back is still one its own account does not
// name, and that is the point: what stops this operator minting there is the
// list, so holding the key any longer would stop only everybody else.
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
		t.Errorf("operator %s issued %d identities while %s was being emptied; the set the "+
			"destruction is working through is still growing", otherOperator, made, testNamespace)
	}

	c.records(t)
	c.records(t)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

	if got := claimOn(t, c.Client, testNamespace); got != "" {
		t.Fatalf("%s is still claimed by %q after the destruction finished; every other operator "+
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
		t.Errorf("operator %s issued %d identities after the destruction finished, want 1; its "+
			"minting stopped because another operator emptied a namespace they share",
			otherOperator, made)
	}
}

// TestANamespaceNamedAgainMidDestructionIsGivenBack covers the one way out of
// the claim that the records cannot say anything about.
//
// The claim is taken while records are being destroyed, and the records are what
// name the namespaces to claim. A namespace put back on the list before the
// destruction finished has no such record left to name it, so the key would stay
// on it for ever with only this operator able to lift it -- and minting there
// would be suspended for everybody, including this operator, which the list has
// just said may act there again.
func TestANamespaceNamedAgainMidDestructionIsGivenBack(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	asker := asking(testNamespace, testName)
	c := newControllers(t, stub, mintingNamespace(testNamespace), asker)
	c.settle(t)

	nowServing(t, c.Client)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)
	if claimOn(t, c.Client, testNamespace) != destructionClaimedBy(testOperatorRef) {
		t.Fatal("the namespace was never claimed, so there is nothing here to give back")
	}

	nowServing(t, c.Client, testNamespace)
	reconcileDatabricksAccount(t, c.databricksAccounts(t), databricksAccountName)

	if got := claimOn(t, c.Client, testNamespace); got != "" {
		t.Errorf("%s is claimed by %q and this account names it again; nothing mints there and "+
			"only this operator can end that", testNamespace, got)
	}
	minting, err := namespaceMints(context.Background(), c.Client, testNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if !minting {
		t.Errorf("%s still does not mint after being named again", testNamespace)
	}
	destroyed := identitiesDestroyedCondition(t, c.Client)
	if destroyed == nil || destroyed.Status != metav1.ConditionTrue {
		t.Errorf("%s is %v, want True -- every namespace this operator issued in is one this "+
			"account names", conditionIdentitiesDestroyed, destroyed)
	}
}
