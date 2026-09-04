package controller

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
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
// The record is written before the create, so a pass that creates a service
// principal and then stops leaves a record naming no id. Deleting that record by
// its id would delete nothing and leave the service principal behind, which is
// exactly the outcome the record exists to prevent -- so it is looked for by the
// marker it was created carrying.
func TestARecordWithNoIdStillDestroysWhatItMade(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	issued := recordFor(serviceAccount)
	// Nothing recorded, and something out there: what a crash between the two
	// calls leaves behind.
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
	issued := recordFor(serviceAccount)
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
	awaited, waiting := comeBackIn(t, &stubClients{
		policyErr: errors.New("the service is temporarily unavailable"),
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

// TestARecordIsHeldRatherThanActedOnInTheWrongAccount covers switching the
// DatabricksAccount and switching it back.
//
// An id means nothing in another account: deleting it there removes something
// this operator never made, or answers 404 and reads as already gone. So a
// record whose account is not the one being acted in waits, and says so. It
// costs nothing to wait -- the record is in the operator's own namespace, so
// nothing anybody else owns is held up by it.
func TestARecordIsHeldRatherThanActedOnInTheWrongAccount(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	issued := recordFor(serviceAccount)
	issued.Status.ServicePrincipalID = "7788"
	issued.Status.AccountID = "the-account-it-was-made-in"

	// Already on its way out, and stuck: the delete has not landed yet. That is
	// what makes the account able to change underneath a revoke, which is the
	// case this is about.
	stub := &stubClients{
		accountID: "the-account-it-was-made-in",
		deleteErr: errors.New("the service is temporarily unavailable"),
	}
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount, issued)
	stopsAsking(t, c.Client)
	c.settle(t)

	if c.issuedOf(t, serviceAccount).DeletionTimestamp.IsZero() {
		t.Fatal("the record is not on its way out; this test is about one that is")
	}

	// And now the operator is pointed somewhere else.
	stub.accountID = "somewhere-else"
	stub.deleteErr = nil
	c.settle(t)

	held := c.issuedOf(t, serviceAccount)
	if held == nil {
		t.Fatal("the record went while its service principal is alive in another account")
	}
	if len(stub.deleted) != 0 {
		t.Errorf("deleted %v while acting in another account; that id names something this "+
			"operator never made, or nothing at all", stub.deleted)
	}
	ready := meta.FindStatusCondition(held.Status.Conditions, conditionReady)
	if ready == nil || ready.Reason != reasonAccountMismatch {
		t.Fatalf("Ready is %v, want it to name the account to point back at", ready)
	}

	// It resumes when the operator comes back, without anybody having to
	// remember that it was waiting.
	stub.accountID = "the-account-it-was-made-in"
	c.settle(t)

	if len(stub.deleted) != 1 || stub.deleted[0] != "7788" {
		t.Errorf("deleted %v after the operator came back to the account it was made in, "+
			"want the identity it was holding", stub.deleted)
	}
	if c.issuedOf(t, serviceAccount) != nil {
		t.Error("the record is still there after what it held was deleted")
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

// TestAnIdRecordedWithNoAccountIsNotConcludedFrom covers the inference that is
// only available when both are known.
//
// A 404 from Databricks means "gone" or "not in this account", and the code
// treats it as the first: it records the removal and never rebuilds. That is
// unrecoverable. With no account recorded there is nothing to tell the two
// apart, and the comment saying an empty value is not evidence of anything sat
// above code that proceeded as though it were evidence of sameness.
func TestAnIdRecordedWithNoAccountIsNotConcludedFrom(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	carried := recordFor(serviceAccount)
	carried.Status.ServicePrincipalID = "7788"
	carried.Status.ClientID = "app-uuid"
	// No account: a record written before this was kept, or edited by hand.

	stub := &stubClients{gone: "7788"}
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount, carried)
	c.settle(t)

	issued := c.issuedOf(t, serviceAccount)
	if issued == nil {
		t.Fatal("the record is gone")
	}
	if issued.Status.RemovedServicePrincipalID != "" {
		t.Errorf("recorded %s as deleted in Databricks on the strength of a 404 that could as "+
			"readily have meant 'not in this account'", issued.Status.RemovedServicePrincipalID)
	}
	if issued.Status.ServicePrincipalID != "7788" {
		t.Errorf("servicePrincipalId is %q, want it untouched", issued.Status.ServicePrincipalID)
	}
	ready := meta.FindStatusCondition(issued.Status.Conditions, conditionReady)
	if ready == nil || ready.Reason != reasonAccountUnknown {
		t.Fatalf("Ready is %v, want it to say the account is unknown", ready)
	}
	if !strings.Contains(ready.Message, "accountId") {
		t.Errorf("message is %q; it has to say what would unstick it", ready.Message)
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
			existing := recordFor(serviceAccount)
			// No id recorded, which is what makes both directions need the
			// token: converging reads it to write a policy, and destroying
			// reads it to find what this record may have made and never named.
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
