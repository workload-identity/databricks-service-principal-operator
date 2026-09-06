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
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/apierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	dbx "github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
	dbxwebhook "github.com/workload-identity/databricks-service-principal-operator/internal/webhook"
)

// TestANamespaceTeardownDestroysTheIdentityInEitherOrder covers the failure this
// whole shape was built for.
//
// A namespace controller deletes the kinds in a namespace in an order nothing
// specifies. When the record of an identity lived in that namespace, the order
// decided the outcome: deleted before the ServiceAccount, the operator woke
// holding a deletion it had to interpret while the ServiceAccount still asked,
// and whatever it guessed, one of the two orders left a live service principal
// in Databricks that nothing in the cluster recorded -- inherited by the next
// occupant of that namespace name, because a federation policy names a subject
// and a subject is a pair of names.
//
// The record is not in that namespace, so there is no order to get right.
func TestANamespaceTeardownDestroysTheIdentityInEitherOrder(t *testing.T) {
	t.Parallel()
	for _, order := range []struct {
		name  string
		first func(t *testing.T, c *controllers, serviceAccount *corev1.ServiceAccount)
	}{
		{"the DatabricksServiceAccount is deleted first", func(t *testing.T, c *controllers, _ *corev1.ServiceAccount) {
			principal := principalOf(t, c.Client)
			if principal == nil {
				t.Fatal("nothing was projected")
			}
			if err := c.Client.Delete(context.Background(), principal); err != nil {
				t.Fatal(err)
			}
		}},
		{"the ServiceAccount is deleted first", func(t *testing.T, c *controllers, serviceAccount *corev1.ServiceAccount) {
			if err := c.Client.Delete(context.Background(), serviceAccount); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(order.name, func(t *testing.T) {
			serviceAccount := asking(testNamespace, testName)
			stub := &stubClients{}
			c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount)
			c.settle(t)

			created := c.issuedOf(t, serviceAccount)
			if created == nil || created.Status.ServicePrincipalID == "" {
				t.Fatal("nothing was issued to tear down")
			}

			// Whichever went first, everything in the namespace is gone by the
			// end of a teardown.
			order.first(t, c, serviceAccount)
			deleteEverythingIn(t, c.Client, testNamespace)
			c.settle(t)

			if len(stub.deleted) != 1 {
				t.Errorf("deleted %v in Databricks, want the one identity whose ServiceAccount "+
					"is gone; what is left is a service principal nothing records, and the next "+
					"namespace of this name inherits it", stub.deleted)
			}
			if c.issuedOf(t, serviceAccount) != nil {
				t.Error("the record is still there after its service principal was deleted")
			}
		})
	}
}

// TestARecreatedServiceAccountDoesNotInheritTheOldIdentity covers what the
// subject cannot do.
//
// Databricks matches the subject in a federation policy, and the subject is
// system:serviceaccount:<namespace>:<name> -- two names, both reusable. A
// namespace recreated under its old name, holding a ServiceAccount of its old
// name, presents a token that satisfies the old policy exactly. The only thing
// that can tell them apart is the uid, so the uid is what the record is named
// for, what the marker is made of, and what is compared here.
func TestARecreatedServiceAccountDoesNotInheritTheOldIdentity(t *testing.T) {
	t.Parallel()
	before := asking(testNamespace, testName)
	stub := &stubClients{}
	c := newControllers(t, stub, mintingNamespace(testNamespace), before)
	c.settle(t)

	inherited := c.issuedOf(t, before)
	if inherited == nil || inherited.Status.ServicePrincipalID == "" {
		t.Fatal("nothing was issued to inherit")
	}

	// The same names, a different ServiceAccount. This is a namespace deleted
	// and recreated, or simply a ServiceAccount deleted and recreated: from
	// Databricks' side the two are indistinguishable.
	if err := c.Client.Delete(context.Background(), before); err != nil {
		t.Fatal(err)
	}
	after := asking(testNamespace, testName)
	after.UID = types.UID("a-different-uid")
	after.ResourceVersion = ""
	if err := c.Client.Create(context.Background(), after); err != nil {
		t.Fatal(err)
	}
	stub.newServicePrincipalID, stub.newClientID = "9900", "app-uuid-2"
	c.settle(t)

	if len(stub.deleted) != 1 || stub.deleted[0] != inherited.Status.ServicePrincipalID {
		t.Errorf("deleted %v, want the identity of the ServiceAccount that is gone; leaving it "+
			"gives its permissions to whoever holds this namespace next", stub.deleted)
	}
	if c.issuedOf(t, before) != nil {
		t.Error("the old record survived the ServiceAccount it was issued to")
	}

	issued := c.issuedOf(t, after)
	if issued == nil {
		t.Fatal("the new ServiceAccount was issued nothing")
	}
	if issued.Status.ServicePrincipalID == inherited.Status.ServicePrincipalID {
		t.Errorf("the new ServiceAccount was handed %s, which belonged to the one before it",
			issued.Status.ServicePrincipalID)
	}
}

// TestARecordWithNoIdStillDestroysWhatItMade covers the one window the ordering
// cannot close.
//
// The mark is written before the create, so a pass that creates a service
// principal and then stops leaves a record in Sent: a create went out and no id
// came back. Deleting that record by its id would delete nothing and leave the
// service principal behind, which is exactly the outcome the record exists to
// prevent -- so it is looked for by the marker it was created carrying.
func TestARecordWithNoIdStillDestroysWhatItMade(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	issued := sentRecordFor(serviceAccount)
	// The mark down, no id recorded, and something out there: what a crash
	// between the two calls leaves behind.
	stub := &stubClients{foundID: "7788", foundClientID: "app-uuid"}
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount, issued)

	stopsAsking(t, c.Client)
	c.settle(t)

	if len(stub.deleted) != 1 || stub.deleted[0] != "7788" {
		t.Errorf("deleted %v, want the service principal this record made and never named; "+
			"nothing else in the cluster knows it exists", stub.deleted)
	}
	if c.issuedOf(t, serviceAccount) != nil {
		t.Error("the record is still there after what it made was deleted")
	}
}

// TestADeletionIsNotHeldOverAValueItDiscards covers a deletion that could not
// happen because of a claim nothing on that path reads.
//
// The audience is what a federation policy names, and destroying an identity
// writes no policy: it finds the service principal this record made and removes
// it. Asking for the audience anyway held the deletion, on an object whose
// finalizer keeps it in the cluster until the call lands, with a message about
// federation policies on the one path that writes none.
func TestADeletionIsNotHeldOverAValueItDiscards(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	issued := sentRecordFor(serviceAccount)
	stub := &stubClients{foundID: "7788", foundClientID: "app-uuid"}
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount, issued)
	// A token with an issuer and no aud claim. The issuer is what the search
	// needs and it is there; the audience is what is missing and it is not.
	c.Issued.TokenPath = writeTokenAs(t, testIssuer,
		"system:serviceaccount:operators:controller-manager", "")

	stopsAsking(t, c.Client)
	c.settle(t)

	if len(stub.deleted) != 1 || stub.deleted[0] != "7788" {
		t.Errorf("deleted %v, want the service principal this record made; it is still there, "+
			"and nothing outside this record knows it exists", stub.deleted)
	}
	if c.issuedOf(t, serviceAccount) != nil {
		t.Error("the record is still held, over a claim its remaining work never reads")
	}
}

// TestASettledIdentityIsNotAskedAboutAsOftenAsAFailingOne covers what coming
// back costs.
//
// Every record that comes back re-runs an existence check and a federation
// policy listing, and there is one record per identity in the account. An
// identity somebody is waiting on is worth that once a minute -- what it waits
// for is a person acting in Databricks, which raises no event here. A converged
// one has nobody waiting: it is only being watched for a deletion made in
// Databricks, and paying the same rate for that is paying it for ever.
func TestASettledIdentityIsNotAskedAboutAsOftenAsAFailingOne(t *testing.T) {
	t.Parallel()
	comeBackIn := func(t *testing.T, stub *stubClients) (time.Duration, *metav1.Condition) {
		t.Helper()
		serviceAccount := asking(testNamespace, testName)
		c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount)
		c.settle(t)
		record := c.issuedOf(t, serviceAccount)
		if record == nil {
			t.Fatal("nothing was issued to ask about")
		}
		result, err := c.Issued.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(record),
		})
		if err != nil {
			t.Fatal(err)
		}
		return result.RequeueAfter, meta.FindStatusCondition(
			c.issuedOf(t, serviceAccount).Status.Conditions, conditionReady)
	}

	settled, ready := comeBackIn(t, &stubClients{})
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("Ready is %v; this half of the comparison has to be a converged identity", ready)
	}
	// A refusal rather than an outage, because those two come back different
	// ways and only this one comes back on an interval: an unreachable Databricks
	// is handed to the workqueue as an error and backs off exponentially, which
	// is what leaves it nothing to compare here.
	awaited, waiting := comeBackIn(t, &stubClients{
		policyErr: fmt.Errorf("the operator's service principal may not write policies: %w",
			apierr.ErrPermissionDenied),
	})
	if waiting == nil || waiting.Status == metav1.ConditionTrue {
		t.Fatalf("Ready is %v; this half has to be an identity that is still owed something", waiting)
	}

	if settled <= awaited {
		t.Errorf("a converged identity comes back in %v and one that is not in %v; every "+
			"identity in the account then pays a lookup and a policy listing at the rate meant "+
			"for the ones somebody is waiting on", settled, awaited)
	}
}

// TestTheDestroyingReadIsNotTakenFromTheCache covers which reader answers the
// one question that destroys things.
//
// A cache that has not caught up reports a ServiceAccount that exists as absent,
// and absent is the answer that deletes a service principal and everything
// granted to it. Being a pass late costs nothing; being wrong is not
// recoverable, because Databricks assigns the applicationId of whatever is made
// in its place.
func TestTheDestroyingReadIsNotTakenFromTheCache(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	issued := recordFor(serviceAccount)
	issued.Status.ServicePrincipalID = "7788"
	issued.Status.ClientID = "app-uuid"

	// The cached client does not have the ServiceAccount; the live one does.
	stub := &stubClients{}
	c := newControllers(t, stub, mintingNamespace(testNamespace), issued)
	live, _ := newFakeClient(t, mintingNamespace(testNamespace), serviceAccount)
	c.Issued.Live = live

	c.records(t)
	c.records(t)

	if len(stub.deleted) != 0 {
		t.Errorf("deleted %v on a cache that had not caught up; the ServiceAccount is there and "+
			"still asking", stub.deleted)
	}
	held := c.issuedOf(t, serviceAccount)
	if held == nil {
		t.Fatal("the record was dropped on a stale read")
	}
	// Marked for deletion is already the whole of it: the finalizer is what runs
	// next, and there is no way back from a deletionTimestamp.
	if !held.DeletionTimestamp.IsZero() {
		t.Error("the record was marked for deletion on a stale read; the finalizer deletes the " +
			"service principal from here and nothing can call that back")
	}
}

// deleteEverythingIn removes what a namespace teardown removes, which is
// everything namespaced in it.
func deleteEverythingIn(t *testing.T, c client.Client, namespace string) {
	t.Helper()
	ctx := context.Background()

	var serviceAccounts corev1.ServiceAccountList
	if err := c.List(ctx, &serviceAccounts, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	for i := range serviceAccounts.Items {
		if err := client.IgnoreNotFound(c.Delete(ctx, &serviceAccounts.Items[i])); err != nil {
			t.Fatal(err)
		}
	}

	var principals dbxv1alpha1.DatabricksServiceAccountList
	if err := c.List(ctx, &principals, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	for i := range principals.Items {
		if err := client.IgnoreNotFound(c.Delete(ctx, &principals.Items[i])); err != nil {
			t.Fatal(err)
		}
	}

	var namespaceObject corev1.Namespace
	if err := c.Get(ctx, types.NamespacedName{Name: namespace}, &namespaceObject); err == nil {
		namespaceObject.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
		if err := client.IgnoreNotFound(c.Delete(ctx, &namespaceObject)); err != nil {
			t.Fatal(err)
		}
	}
}

// TestARecordWithNoFinalizerGetsOneBeforeAnythingIsCreated covers an orphan
// reachable by an object missing one string.
//
// The finalizer is written when the record is made, and nothing put it back. A
// record without one converges like any other -- it creates a real service
// principal and records the id -- and is then let go without deleting anything,
// which is the exact outcome this object exists to prevent. Whatever manages the
// operator's namespace from a manifest writes records without finalizers.
func TestARecordWithNoFinalizerGetsOneBeforeAnythingIsCreated(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	bare := recordFor(serviceAccount)
	bare.Finalizers = nil

	stub := &stubClients{}
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount, bare)
	c.records(t)

	issued := c.issuedOf(t, serviceAccount)
	if issued == nil {
		t.Fatal("the record is gone")
	}
	if !slices.Contains(issued.Finalizers, dbxv1alpha1.ServicePrincipalFinalizer) {
		t.Fatalf("finalizers are %v; whatever this record makes in Databricks, nothing will "+
			"delete it", issued.Finalizers)
	}
	if len(stub.created) != 0 {
		t.Errorf("created %v on the same pass; the finalizer has to be there before there is "+
			"anything for it to hold", stub.created)
	}

	// And the next pass builds, so this costs a pass and not the identity.
	c.records(t)
	if len(stub.created) != 1 {
		t.Errorf("created %v on the pass after; adding the finalizer stopped it building at all",
			stub.created)
	}
}

// TestRemovingTheFinalizerByHandIsNotArguedWith covers the documented way out.
//
// A record can be held by something that will not resolve -- a Databricks that
// stays unreachable, an account it was not made in -- and the answer is a person
// who can see what is left behind removing the finalizer. Putting it back every
// pass would be the operator arguing with them on a timer, and the object would
// never go.
func TestRemovingTheFinalizerByHandIsNotArguedWith(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	held := recordFor(serviceAccount)
	held.Status.ServicePrincipalID = "7788"
	held.Status.AccountID = testAccountID

	// A second finalizer, so the record survives ours being removed. Without one
	// the object goes the instant the last finalizer does, and the operator
	// never gets a pass in which it could put ours back -- which is the whole of
	// what this is about. Something else holding an object of ours is ordinary:
	// whatever manages the operator's namespace often adds its own.
	held.Finalizers = append(held.Finalizers, "example.com/held-by-something-else")

	stub := &stubClients{deleteErr: errors.New("the service is temporarily unavailable")}
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount, held)

	stopsAsking(t, c.Client)
	c.settle(t)

	stuck := c.issuedOf(t, serviceAccount)
	if stuck == nil || stuck.DeletionTimestamp.IsZero() {
		t.Fatal("the record is not being held on its way out; this test is about one that is")
	}

	// The person who can see what is left behind decides.
	controllerutil.RemoveFinalizer(stuck, dbxv1alpha1.ServicePrincipalFinalizer)
	if err := c.Client.Update(context.Background(), stuck); err != nil {
		t.Fatal(err)
	}
	c.settle(t)

	got := c.issuedOf(t, serviceAccount)
	if got == nil {
		t.Fatal("the record went; something else is still holding it")
	}
	if slices.Contains(got.Finalizers, dbxv1alpha1.ServicePrincipalFinalizer) {
		t.Errorf("finalizers are %v; the operator put back what somebody removed, so the way "+
			"out documented for a record that will not resolve does not work", got.Finalizers)
	}
	if len(stub.deleted) != 0 {
		t.Errorf("deleted %v after being let go by hand; letting go is the person saying they "+
			"will deal with what is left", stub.deleted)
	}
}

// TestNotHavingLookedIsSaidAsUnknown covers the difference between "this is
// wrong" and "nothing has been checked", which this project has a rule about and
// broke in one of the two places it applies.
//
// Unknown and pointedly not False: nothing has been asked of Databricks, so
// False reports a declaration as wrong on the strength of never having looked.
// destroy said Unknown and converge said False for the same cause -- the
// operator being unable to read its own token -- so the same condition meant two
// things depending on which way the object was going.
//
// The reason is the same both ways for the same reason. One root cause reported
// under two names is one an alert on either misses half the time, and the token
// this operator cannot read is nothing to do with which Databricks account it
// was told to act in.
func TestNotHavingLookedIsSaidAsUnknown(t *testing.T) {
	t.Parallel()
	for _, going := range []struct {
		name string
		run  func(t *testing.T, c *controllers, serviceAccount *corev1.ServiceAccount)
	}{
		{"converging", func(*testing.T, *controllers, *corev1.ServiceAccount) {}},
		{"on its way out", func(t *testing.T, c *controllers, _ *corev1.ServiceAccount) {
			stopsAsking(t, c.Client)
		}},
	} {
		t.Run(going.name, func(t *testing.T) {
			serviceAccount := asking(testNamespace, testName)
			// In Sent, which is what makes both directions need the token:
			// converging reads it to write a policy, and destroying reads it to
			// build the marker for what this record's create may have made and
			// never named.
			existing := sentRecordFor(serviceAccount)
			existing.Status.AccountID = testAccountID

			stub := &stubClients{}
			c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount, existing)
			c.Issued.TokenPath = filepath.Join(t.TempDir(), "not-there")
			going.run(t, c, serviceAccount)
			c.settle(t)

			issued := c.issuedOf(t, serviceAccount)
			if issued == nil {
				t.Fatal("the record is gone")
			}
			ready := meta.FindStatusCondition(issued.Status.Conditions, conditionReady)
			if ready == nil || ready.Reason != reasonUnprepared {
				t.Fatalf("Ready is %v, want %s -- NotConfigured sends whoever reads it to a "+
					"DatabricksAccount with nothing wrong in it, and leaves the file that "+
					"could not be read unnamed", ready, reasonUnprepared)
			}
			if ready.Status != metav1.ConditionUnknown {
				t.Errorf("Ready is %s/%s; nothing has been asked of Databricks, and False says "+
					"this identity is wrong on the strength of never having looked",
					ready.Status, ready.Reason)
			}
		})
	}
}

// TestARecordThatSentNoCreateIsLetGoWithoutAsking covers the state that is both
// the common one and the only one where letting go costs nothing.
//
// A record with no mark sent no create, so nothing exists for it in Databricks
// and there is nothing to look for. The lookup that used to run here asked
// Databricks about work this operator knows it never began, and paid a listing
// for every record deleted before it ever converged -- which is most of them.
func TestARecordThatSentNoCreateIsLetGoWithoutAsking(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	unsent := recordFor(serviceAccount)

	// Something findable, so that a lookup happening at all is visible rather
	// than merely unasserted: against a stub that finds nothing, a lookup that
	// ran would leave no trace in what this asserts.
	stub := &stubClients{foundID: "7788", foundClientID: "app-uuid"}
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount, unsent)

	stopsAsking(t, c.Client)
	c.settle(t)

	if len(stub.looked) != 0 {
		t.Errorf("looked for %v; this record sent no create, so nothing it could name exists "+
			"and the answer cannot change what happens", stub.looked)
	}
	if len(stub.deleted) != 0 {
		t.Errorf("deleted %v on behalf of a record that never asked Databricks for anything",
			stub.deleted)
	}
	if c.issuedOf(t, serviceAccount) != nil {
		t.Error("the record is held; nothing can exist for it, so holding it is waiting for " +
			"something that will never arrive")
	}
}

// TestARecordHeldInSentIsNotLetGoOfOnAListing covers the one state in which no
// irreversible act is available.
//
// A create was sent and its answer never arrived, so a service principal
// carrying this record's marker may exist. The listing that would find it is
// eventually consistent, so "not there" is not an answer: releasing the
// finalizer on it leaves a live service principal in Databricks that nothing in
// the cluster records, which is the one outcome this object exists to prevent.
//
// The message has to end the situation, because nothing else will. No interval
// resolves this, and the person who can is the one who searched the account.
func TestARecordHeldInSentIsNotLetGoOfOnAListing(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	sent := sentRecordFor(serviceAccount)

	// Nothing findable, which is both "the create never ran" and "it ran and the
	// listing has not caught up", and nothing here can tell them apart.
	stub := &stubClients{}
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount, sent)

	stopsAsking(t, c.Client)
	c.settle(t)

	held := c.issuedOf(t, serviceAccount)
	if held == nil {
		t.Fatal("the record was let go of on a listing that found nothing; whatever its create " +
			"made is now in Databricks with nothing anywhere naming it")
	}
	if !slices.Contains(held.Finalizers, dbxv1alpha1.ServicePrincipalFinalizer) {
		t.Fatalf("finalizers are %v; the object goes as soon as the last one does",
			held.Finalizers)
	}
	if len(stub.looked) == 0 {
		t.Error("nothing was looked for; a create was sent, and this is the state whose only " +
			"legal move is to keep asking")
	}

	ready := meta.FindStatusCondition(held.Status.Conditions, conditionReady)
	if ready == nil || ready.Reason != reasonCreateUnconfirmed {
		t.Fatalf("Ready is %v, want %s", ready, reasonCreateUnconfirmed)
	}
	if ready.Status != metav1.ConditionUnknown {
		t.Errorf("Ready is %s/%s; a listing that has not caught up is not evidence that "+
			"anything is wrong", ready.Status, ready.Reason)
	}
	// The two halves a person needs: what to search Databricks with, and that
	// removing the finalizer is theirs to do once they have.
	if marker := dbx.MarkerFor(c.Issued.issuing(held, testIssuer)); !strings.Contains(ready.Message, marker) {
		t.Errorf("message is %q and does not carry %q; without the marker there is no way to "+
			"search the account for what this record may have made", ready.Message, marker)
	}
	if !strings.Contains(ready.Message, "finalizer") {
		t.Errorf("message is %q; nothing releases this on its own, so it has to say who does",
			ready.Message)
	}
}

// TestARecordInSentDoesNotCreateASecond covers what a second create would cost.
//
// Both service principals would carry one marker, and FindServicePrincipal
// refuses to adopt either when it finds two -- so the record would be stuck for
// ever, with two identities in the account and no way to tell which is its.
// Waiting costs a pass; this costs the account.
func TestARecordInSentDoesNotCreateASecond(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	sent := sentRecordFor(serviceAccount)

	stub := &stubClients{}
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount, sent)
	c.settle(t)

	if len(stub.created) != 0 {
		t.Errorf("created %v for a record whose create was already sent; two carrying one "+
			"marker cannot be told apart, and neither is ever adopted again", stub.created)
	}
	issued := c.issuedOf(t, serviceAccount)
	if issued == nil {
		t.Fatal("the record is gone")
	}
	if issued.Status.ServicePrincipalID != "" {
		t.Errorf("servicePrincipalId is %q; nothing answered, and writing an id nothing "+
			"returned is inventing the evidence every later pass reads",
			issued.Status.ServicePrincipalID)
	}
	ready := meta.FindStatusCondition(issued.Status.Conditions, conditionReady)
	if ready == nil || ready.Reason != reasonCreateUnconfirmed {
		t.Fatalf("Ready is %v, want %s", ready, reasonCreateUnconfirmed)
	}
}

// TestTheMarkIsWrittenBeforeTheCreate covers the ordering the whole state
// machine rests on.
//
// Whether a create was already sent is the one fact this operator cannot work
// out again; everything else about a record is derivable from its spec. Written
// after the call, the mark says nothing about a pass that did not survive the
// call -- and that pass is exactly the one that leaves a service principal
// behind.
//
// The process is stopped inside the create rather than made to fail, because a
// failure still reaches the status write at the end of the pass: it cannot tell
// "written down before the call" from "written down eventually".
func TestTheMarkIsWrittenBeforeTheCreate(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	unsent := recordFor(serviceAccount)

	stub := &stubClients{createPanics: true}
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount, unsent)

	if issued := c.issuedOf(t, serviceAccount); issued.Status.ServicePrincipalCreateSentAt != nil {
		t.Fatalf("the record is already marked at %v; this test is about one that has asked "+
			"for nothing", issued.Status.ServicePrincipalCreateSentAt)
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the stub was arranged to stop the pass and did not")
			}
		}()
		c.records(t)
	}()

	issued := c.issuedOf(t, serviceAccount)
	if len(stub.created) != 1 {
		t.Fatalf("created %v; this test says nothing unless the create was reached", stub.created)
	}
	if issued.Status.ServicePrincipalCreateSentAt == nil {
		t.Error("a create was sent and nothing records that it was; the next pass reads this " +
			"record as one that never asked, creates a second, and leaves the first behind")
	}
	if got := issued.Status.ServicePrincipalState(); got != dbxv1alpha1.ServicePrincipalSent {
		t.Errorf("the record is %s, want %s -- it is what makes the next pass look before it "+
			"builds", got, dbxv1alpha1.ServicePrincipalSent)
	}
}

// TestALatchedRemovalDestroysNoEvidence covers what the record has to still say
// after the one decision it never takes back.
//
// The id is what somebody searches Databricks' audit log with to find out who
// deleted it and when, and the client id is what every grant anybody made names.
// Clearing either to stop pods being equipped would be the record of a removal
// destroying the record it was drawn from -- and the equipping is stopped by the
// identity no longer being usable, which needs nothing destroyed.
func TestALatchedRemovalDestroysNoEvidence(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	stub := &stubClients{}
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount,
		equippedPod(testName))
	c.settle(t)

	built := c.issuedOf(t, serviceAccount)
	if built == nil || built.Status.ServicePrincipalID == "" || built.Status.ClientID == "" {
		t.Fatal("nothing was built for somebody to delete in Databricks")
	}
	id, clientID := built.Status.ServicePrincipalID, built.Status.ClientID

	stub.gone = id
	c.settle(t)

	issued := c.issuedOf(t, serviceAccount)
	if issued == nil {
		t.Fatal("the record is gone")
	}
	if issued.Status.ServicePrincipalID != id {
		t.Errorf("servicePrincipalId is %q, want %q -- it is the only value that searches the "+
			"audit log for who deleted it", issued.Status.ServicePrincipalID, id)
	}
	if issued.Status.ClientID != clientID {
		t.Errorf("clientId is %q, want %q -- it is what every grant anybody made names, and a "+
			"person auditing them has nothing else to match on",
			issued.Status.ClientID, clientID)
	}
	if got := issued.Status.ServicePrincipalState(); got != dbxv1alpha1.ServicePrincipalGone {
		t.Fatalf("the record is %s, want %s", got, dbxv1alpha1.ServicePrincipalGone)
	}

	// And no pod is equipped with it, which is what clearing the client id used
	// to buy and now costs nothing.
	if dbxwebhook.Profiles([]dbxv1alpha1.ProjectedIdentity{
		identityIn(t, principalOf(t, c.Client)),
	}) != "" {
		t.Error("the webhook still writes a profile for a service principal that is not there; " +
			"a pod admitted now exchanges its token for nothing")
	}
	if len(stub.created) != 1 {
		t.Errorf("created %v; a record in Gone builds no replacement, whatever else it still "+
			"says", stub.created)
	}
}
