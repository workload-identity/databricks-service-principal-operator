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
	"path/filepath"
	"strings"
	"testing"

	"github.com/databricks/databricks-sdk-go/apierr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	dbx "github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

const (
	testSubject = "system:serviceaccount:databricks-operator-system:databricks-operator-controller-manager"
)

// newDatabricksAccountReconciler wires a reconciler whose client building and account call
// are both under the test's control, so nothing here reaches Databricks.
func newDatabricksAccountReconciler(t *testing.T, tokenPath string, verify func() error,
	objects ...client.Object) (*DatabricksAccountReconciler, client.Client, *dbx.AccountInUse) {
	t.Helper()
	c, _ := newFakeClient(t, objects...)
	r, accountInUse := restartedDatabricksAccountReconciler(t, c, tokenPath, verify)
	return r, c, accountInUse
}

// restartedDatabricksAccountReconciler is this operator coming up again over a
// cluster already in some state.
//
// A new process, so a new AccountInUse holding nothing: what is installed lives
// in memory and no restart inherits it. That is the difference a test about a
// half-finished deletion has to be able to make, since the pass that installed
// the clients happened in the process that died.
func restartedDatabricksAccountReconciler(t *testing.T, c client.Client, tokenPath string,
	verify func() error) (*DatabricksAccountReconciler, *dbx.AccountInUse) {
	t.Helper()
	accountInUse := dbx.NewAccountInUse(operatorNamespace, databricksAccountName)
	built := &stubClients{}
	return &DatabricksAccountReconciler{
		Client:                          c,
		Scheme:                          c.Scheme(),
		DatabricksAccountNamespacedName: types.NamespacedName{Namespace: operatorNamespace, Name: databricksAccountName},
		AccountInUse:                    accountInUse,
		OwnToken:                        dbx.Config{OIDCTokenFilepath: tokenPath, TokenAudience: "databricks"},
		build:                           func(dbx.Config) (dbx.Clients, error) { return built, nil },
		verify:                          func(context.Context, dbx.Clients) error { return verify() },
	}, accountInUse
}

func databricksAccountCondition(t *testing.T, c client.Client, name string) *metav1.Condition {
	t.Helper()
	var got dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: operatorNamespace, Name: name,
	}, &got); err != nil {
		t.Fatalf("getting %s: %v", name, err)
	}
	return meta.FindStatusCondition(got.Status.Conditions, conditionReady)
}

// databricksAccountOrNil is the object as the cluster holds it, or nil when it
// is gone -- which is what a finalizer that has been released looks like.
func databricksAccountOrNil(t *testing.T, c client.Client, name string) *dbxv1alpha1.DatabricksAccount {
	t.Helper()
	var got dbxv1alpha1.DatabricksAccount
	err := c.Get(context.Background(), types.NamespacedName{Namespace: operatorNamespace, Name: name}, &got)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("getting %s: %v", name, err)
	}
	return &got
}

func deleteDatabricksAccount(t *testing.T, c client.Client, name string) {
	t.Helper()
	live := databricksAccountOrNil(t, c, name)
	if live == nil {
		t.Fatalf("%s is already gone", name)
	}
	if err := c.Delete(context.Background(), live); err != nil {
		t.Fatal(err)
	}
}

func databricksAccountStatus(t *testing.T, c client.Client, name string) dbxv1alpha1.DatabricksAccountStatus {
	t.Helper()
	var got dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: operatorNamespace, Name: name,
	}, &got); err != nil {
		t.Fatalf("getting %s: %v", name, err)
	}
	return got.Status
}

// TestAccountReportsTheSubjectItPresents is why this kind exists at all.
//
// The federation policy in Databricks has to name the operator's subject
// exactly, and that string is assembled from the operator namespace and the
// ServiceAccount name -- both of which kustomize rewrites, neither of which is
// derivable from anything the installer holds. Getting it wrong is refused with
// the policy's own description echoed back, which sends the reader to fix a
// policy that is correct.
//
// So the operator states who it is. Anything less and this type is a ConfigMap
// with extra steps.
func TestAccountReportsTheSubjectItPresents(t *testing.T) {
	t.Parallel()
	r, c, accountInUse := newDatabricksAccountReconciler(t,
		writeToken(t, testSubject, "databricks"),
		func() error { return nil },
		databricksAccountNamed(databricksAccountName))
	reconcileDatabricksAccount(t, r, databricksAccountName)

	status := databricksAccountStatus(t, c, databricksAccountName)
	if status.Subject != testSubject {
		t.Errorf("subject is %q, want %q", status.Subject, testSubject)
	}
	if status.Audience != "databricks" {
		t.Errorf("audience is %q, want the aud the token carries", status.Audience)
	}
	if ready := databricksAccountCondition(t, c, databricksAccountName); ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("Ready is %v, want True", ready)
	}
	if !accountInUse.Configured() {
		t.Error("the account verified but no clients were installed")
	}
}

// TestAccountReportsTheSubjectEvenWhenRefused covers the case the field is for.
//
// A working account is the one nobody needs to look at. The subject has to be
// reported when the exchange fails, because that is the moment somebody is
// comparing it against a federation policy.
func TestAccountReportsTheSubjectEvenWhenRefused(t *testing.T) {
	t.Parallel()
	refused := &apierr.APIError{
		ErrorCode:  "TOKEN_INVALID",
		StatusCode: 401,
		Message:    "the subject does not match any federation policy",
	}
	r, c, accountInUse := newDatabricksAccountReconciler(t,
		writeToken(t, testSubject, "databricks"),
		func() error { return refused },
		databricksAccountNamed(databricksAccountName))
	reconcileDatabricksAccount(t, r, databricksAccountName)

	status := databricksAccountStatus(t, c, databricksAccountName)
	if status.Subject != testSubject {
		t.Errorf("subject is %q, want it reported even though the exchange failed", status.Subject)
	}
	ready := meta.FindStatusCondition(status.Conditions, conditionReady)
	if ready == nil || ready.Status == metav1.ConditionTrue {
		t.Fatalf("Ready is %v, want it not True", ready)
	}
	// A refusal is Denied, not a failure to answer: it is retried on a fixed
	// delay rather than backed off, because what fixes it is a policy written in
	// Databricks and nothing here hears about that.
	if ready.Reason != reasonDenied {
		t.Errorf("reason is %q, want %q", ready.Reason, reasonDenied)
	}
	if accountInUse.Configured() {
		t.Error("clients were installed from an account that was refused")
	}
}

// TestAccountFailureKeepsWorkingClients is the safety property.
//
// Everything the operator does resolves through these clients. Dropping them
// because one account-level call failed would take every identity with it -- a
// consequence far larger than the failure that caused it,
// and one that fixes nothing. The account says it is unhappy; the workloads keep
// their access.
func TestAccountFailureKeepsWorkingClients(t *testing.T) {
	t.Parallel()
	fail := false
	r, c, accountInUse := newDatabricksAccountReconciler(t,
		writeToken(t, testSubject, "databricks"),
		func() error {
			if fail {
				return errors.New("the service is temporarily unavailable")
			}
			return nil
		},
		databricksAccountNamed(databricksAccountName))

	reconcileDatabricksAccount(t, r, databricksAccountName)
	if !accountInUse.Configured() {
		t.Fatal("the first reconcile installed no clients")
	}

	fail = true
	reconcileDatabricksAccount(t, r, databricksAccountName)

	if !accountInUse.Configured() {
		t.Error("one failed account call withdrew the clients every other object depends on")
	}
	if ready := databricksAccountCondition(t, c, databricksAccountName); ready == nil || ready.Status == metav1.ConditionTrue {
		t.Errorf("Ready is %v, want it not True: the failure still has to be reported", ready)
	}
}

// TestAccountDeletionClearsTheClients covers the one case that does clear them.
// Nobody has declared an account any more, and continuing to act in one that
// was deleted is worse than reporting that there is none.
func TestAccountDeletionClearsTheClients(t *testing.T) {
	t.Parallel()
	r, c, accountInUse := newDatabricksAccountReconciler(t,
		writeToken(t, testSubject, "databricks"),
		func() error { return nil },
		databricksAccountNamed(databricksAccountName))
	reconcileDatabricksAccount(t, r, databricksAccountName)
	if !accountInUse.Configured() {
		t.Fatal("the first reconcile installed no clients")
	}

	var live dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: operatorNamespace, Name: databricksAccountName,
	}, &live); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	reconcileDatabricksAccount(t, r, databricksAccountName)

	if accountInUse.Configured() {
		t.Error("the account was deleted and the operator kept acting in it")
	}
}

// TestAccountNotSelectedSaysSo covers a DatabricksAccount this operator was not
// pointed at.
//
// Leaving it silent would be indistinguishable from an operator that is not
// running, which is the wrong thing for somebody to conclude while staging a
// change. It also must not be adopted: which account is live is decided by the
// flag on the Deployment, not by whoever can create an object in this namespace.
func TestAccountNotSelectedSaysSo(t *testing.T) {
	t.Parallel()
	const other = "staging-account"
	r, c, accountInUse := newDatabricksAccountReconciler(t,
		writeToken(t, testSubject, "databricks"),
		func() error { return nil },
		databricksAccountNamed(other))
	reconcileDatabricksAccount(t, r, other)

	ready := databricksAccountCondition(t, c, other)
	if ready == nil || ready.Status != metav1.ConditionFalse {
		t.Fatalf("Ready is %v, want False", ready)
	}
	if ready.Reason != reasonNotSelected {
		t.Errorf("reason is %q, want %q", ready.Reason, reasonNotSelected)
	}
	// The one in use has to be named, or the reader is told they are wrong
	// without being told what is right.
	if !strings.Contains(ready.Message, databricksAccountName) {
		t.Errorf("message is %q, want it to name the account this operator uses", ready.Message)
	}
	if accountInUse.Configured() {
		t.Error("an account this operator was not pointed at was adopted anyway")
	}
}

// TestUnconfiguredAccountInUseIsNotAFailedLookup covers what the identity
// controller sees before any account is usable.
//
// Nothing has been asked of Databricks, so reporting a declaration as wrong
// would be blaming it for never having been checked. It must classify as
// NotConfigured, which is reported as a wait.
func TestUnconfiguredAccountInUseIsNotAFailedLookup(t *testing.T) {
	t.Parallel()
	accountInUse := dbx.NewAccountInUse(operatorNamespace, databricksAccountName)

	_, err := accountInUse.ServicePrincipalExists(context.Background(), "7788")
	if err == nil {
		t.Fatal("an unconfigured AccountInUse answered a lookup")
	}
	if got := dbx.KindOf(err); got != dbx.NotConfigured {
		t.Errorf("KindOf is %q, want %q", got, dbx.NotConfigured)
	}
	// The message has to name the object to create. "not configured" on its own
	// tells somebody they are stuck without telling them what unsticks them.
	if !strings.Contains(err.Error(), databricksAccountName) || !strings.Contains(err.Error(), operatorNamespace) {
		t.Errorf("error is %q, want it to name the DatabricksAccount to create", err)
	}

	result := outcomeFor(err)
	if result.Status != metav1.ConditionUnknown {
		t.Errorf("status is %s, want Unknown: nothing has been checked", result.Status)
	}
	if result.Err != nil {
		t.Error("returned to the workqueue: an operator with no account would back off exponentially " +
			"while waiting for somebody to create one")
	}
}

// identityMadeIn is one recorded against a particular Databricks account.
// identityMadeIn is a record of an identity this operator issued in some account.
//
// A record and not a DatabricksServiceAccount, and in the operator's own
// namespace and not the tenant's, because that is where the count is taken
// from: the DatabricksServiceAccount is a copy, and a copy is missing exactly
// when this question is most worth asking -- an identity whose namespace was
// torn down still has its record and no longer has its copy.
func identityMadeIn(name, accountID string) *dbxv1alpha1.IssuedDatabricksServicePrincipal {
	issued := &dbxv1alpha1.IssuedDatabricksServicePrincipal{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: operatorNamespace},
		Spec: dbxv1alpha1.IssuedDatabricksServicePrincipalSpec{
			Subject: "system:serviceaccount:team-a:" + name,
			ServiceAccount: dbxv1alpha1.IssuedServiceAccount{
				Namespace: "team-a", Name: name, UID: types.UID("uid-" + name),
			},
		},
	}
	issued.Status.ServicePrincipalID = "7788"
	issued.Status.AccountID = accountID
	return issued
}

// identityOfAnotherOperator is a record in another operator's namespace. Nothing
// this operator does may hold on it: it was issued by another operator, holding
// another account admin credential, and only that one can destroy what it names.
func identityOfAnotherOperator(name, accountID string) *dbxv1alpha1.IssuedDatabricksServicePrincipal {
	issued := identityMadeIn(name, accountID)
	issued.Namespace = "another-operator"
	return issued
}

// TestTheAccountIsHeldWhileARecordNamesIt is the second of the three doors that
// make the account this operator acts in fixed for its lifetime.
//
// spec.accountId being immutable is worth nothing on its own: deleting the
// object and writing another one naming a different account is the same edit
// with a longer handle, and every id this operator wrote down would then mean
// nothing. So the object waits for the records, and says on itself how many.
func TestTheAccountIsHeldWhileARecordNamesIt(t *testing.T) {
	t.Parallel()
	r, c, _ := newDatabricksAccountReconciler(t, writeToken(t, testIssuer, testAudience),
		func() error { return nil },
		databricksAccountNamed(databricksAccountName),
		identityMadeIn("etl", testAccountID),
		identityMadeIn("loader", testAccountID))
	reconcileDatabricksAccount(t, r, databricksAccountName)

	deleteDatabricksAccount(t, c, databricksAccountName)
	reconcileDatabricksAccount(t, r, databricksAccountName)

	held := databricksAccountOrNil(t, c, databricksAccountName)
	if held == nil {
		t.Fatal("the DatabricksAccount went while two records name the Databricks account it " +
			"declares; the next one written here can name any account it likes")
	}
	remain := meta.FindStatusCondition(held.Status.Conditions, conditionRecordsReleased)
	if remain == nil || remain.Status != metav1.ConditionFalse {
		t.Fatalf("RecordsReleased is %v; a deletion that does not happen and says nothing reads "+
			"as one nobody has got to yet", remain)
	}
	if !strings.Contains(remain.Message, "2") {
		t.Errorf("message is %q, want the count of what is holding it", remain.Message)
	}
	if !strings.Contains(remain.Message, testAccountID) {
		t.Errorf("message is %q, want the Databricks account being held", remain.Message)
	}
	// The way out, for an account that can never be drained because it can no
	// longer be reached. Without it this finalizer is a trap.
	if !strings.Contains(remain.Message, "finalizer") {
		t.Errorf("message is %q, want the way out written where somebody reads it", remain.Message)
	}

	// The operator still installs its clients while it is held, and that is what
	// makes the hold escapable at all: those clients are what deletes the
	// service principals the records name. Asked of a restart, because that is
	// where it fails -- an operator that gave up on the deletion would come up
	// holding nothing, and nothing would then be able to drain it.
	restarted, itsClients := restartedDatabricksAccountReconciler(t, c,
		writeToken(t, testIssuer, testAudience), func() error { return nil })
	reconcileDatabricksAccount(t, restarted, databricksAccountName)
	if !itsClients.Configured() {
		t.Error("an operator restarted while its account is held installed nothing, so nothing " +
			"can destroy what is holding it and the only way out is by hand")
	}

	// And it goes once nothing names it. The records are what a person deletes;
	// each destroys its own service principal on the way out.
	for _, name := range []string{"etl", "loader"} {
		record := &dbxv1alpha1.IssuedDatabricksServicePrincipal{
			ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: name},
		}
		if err := c.Delete(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
	reconcileDatabricksAccount(t, r, databricksAccountName)

	if databricksAccountOrNil(t, c, databricksAccountName) != nil {
		t.Error("the DatabricksAccount is still held with no record naming its account; the " +
			"finalizer would have to be removed by hand to uninstall a drained operator")
	}
}

// TestAnOperatorThatNeverReachedDatabricksLetsItsAccountGo is what keeps a typo
// at install from bricking anything.
//
// A record names an account only once a create has been sent, so an operator
// whose host or account id was wrong from the start has records naming nothing
// and nothing in Databricks. Holding its object would mean the first thing a
// person got wrong is the one thing they cannot undo.
func TestAnOperatorThatNeverReachedDatabricksLetsItsAccountGo(t *testing.T) {
	t.Parallel()
	never := identityMadeIn("etl", "")
	never.Status.ServicePrincipalID = ""
	r, c, _ := newDatabricksAccountReconciler(t, writeToken(t, testIssuer, testAudience),
		func() error { return nil },
		databricksAccountNamed(databricksAccountName), never)
	reconcileDatabricksAccount(t, r, databricksAccountName)

	deleteDatabricksAccount(t, c, databricksAccountName)
	reconcileDatabricksAccount(t, r, databricksAccountName)

	if databricksAccountOrNil(t, c, databricksAccountName) != nil {
		t.Error("the DatabricksAccount is held by a record that never sent a create, so nothing " +
			"can exist for it anywhere; a mistyped account id would be permanent")
	}
}

// TestAnotherOperatorsRecordsDoNotHoldThisAccount covers the boundary, on the
// one question this controller answers by reading another kind.
//
// Two operators in a cluster hold two account admin credentials, each in its own
// namespace, and neither is the other's to drain. A record somewhere else
// holding this object would make uninstalling this operator wait on identities
// it never issued and cannot delete.
func TestAnotherOperatorsRecordsDoNotHoldThisAccount(t *testing.T) {
	t.Parallel()
	r, c, _ := newDatabricksAccountReconciler(t, writeToken(t, testIssuer, testAudience),
		func() error { return nil },
		databricksAccountNamed(databricksAccountName),
		identityOfAnotherOperator("theirs", testAccountID))
	reconcileDatabricksAccount(t, r, databricksAccountName)

	deleteDatabricksAccount(t, c, databricksAccountName)
	reconcileDatabricksAccount(t, r, databricksAccountName)

	if databricksAccountOrNil(t, c, databricksAccountName) != nil {
		t.Error("a record in another operator's namespace is holding this object; nothing this " +
			"operator can do would ever release it")
	}
}

// TestAnAccountThisOperatorDoesNotUseIsNotHeld covers the way the flag is meant
// to be moved: write the new object, see it accepted, then move the flag.
//
// The one it moved off is nothing this operator uses, so it holds nothing of
// this operator's. A finalizer left there would make deleting it hang on a
// controller that does nothing but report the object as not selected.
func TestAnAccountThisOperatorDoesNotUseIsNotHeld(t *testing.T) {
	t.Parallel()
	const other = "staging-account"
	stale := databricksAccountNamed(other)
	stale.Finalizers = []string{dbxv1alpha1.AccountFinalizer}
	r, c, _ := newDatabricksAccountReconciler(t, writeToken(t, testIssuer, testAudience),
		func() error { return nil },
		databricksAccountNamed(databricksAccountName), stale)

	deleteDatabricksAccount(t, c, other)
	reconcileDatabricksAccount(t, r, other)

	if databricksAccountOrNil(t, c, other) != nil {
		t.Error("a DatabricksAccount this operator was not told to use is held by it, and nothing " +
			"it does will ever let go")
	}
}

// TestAnOperatorToldToUseAnotherAccountRefusesToServe is the third door, and the
// only one nothing in the API server can hold.
//
// spec.accountId is immutable and the object cannot be deleted while records
// name its account, but --databricks-account is a flag on a Deployment: pointing
// it at a second object naming a second account walks past both. Admission never
// sees that edit, so it is asked once, at startup, and the answer is not to
// serve.
func TestAnOperatorToldToUseAnotherAccountRefusesToServe(t *testing.T) {
	t.Parallel()
	c, _ := newFakeClient(t,
		databricksAccountNamed(databricksAccountName),
		identityMadeIn("etl", "the-account-it-was-made-in"))

	err := CheckSelectedAccount(context.Background(), c, testOperatorRef)
	if err == nil {
		t.Fatal("the operator started against an account none of its records was made in; every " +
			"identity in the cluster is one 404 away from being latched as deleted")
	}
	// Which record, which account it names, and which this operator was told to
	// use. Without all three the person reading it cannot tell whether the flag
	// or the object is the thing that is wrong.
	for _, want := range []string{"etl", "the-account-it-was-made-in", testAccountID, "databricks-account"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal is %q, want it to name %q", err, want)
		}
	}
}

// TestAnOperatorWhoseRecordsAgreeServes is the other half. The check is made at
// startup, where refusing wrongly is an operator that will not come up at all.
func TestAnOperatorWhoseRecordsAgreeServes(t *testing.T) {
	t.Parallel()
	// A record made here, one that never sent a create and so names no account,
	// and another operator's, in its own namespace.
	c, _ := newFakeClient(t,
		databricksAccountNamed(databricksAccountName),
		identityMadeIn("etl", testAccountID),
		identityMadeIn("unsent", ""),
		identityOfAnotherOperator("theirs", "an-account-this-operator-never-saw"))

	if err := CheckSelectedAccount(context.Background(), c, testOperatorRef); err != nil {
		t.Errorf("refused to serve: %v", err)
	}
}

// TestAnOperatorWithNoDatabricksAccountServes covers the ordinary first install.
//
// The operator is deployed before the account is declared, and it runs and says
// so on every object rather than crashlooping with the reason only in its log.
// Refusing here would make that impossible.
func TestAnOperatorWithNoDatabricksAccountServes(t *testing.T) {
	t.Parallel()
	c, _ := newFakeClient(t, identityMadeIn("etl", "some-account"))

	if err := CheckSelectedAccount(context.Background(), c, testOperatorRef); err != nil {
		t.Errorf("refused to serve with no DatabricksAccount to disagree with: %v", err)
	}
}

// TestRepointingAtAnAccountThatFailsClearsTheOldOne covers the operator
// acting in an account its own declaration no longer names.
//
// Verification failing used to return before anything was cleared, so the
// clients built from the previous declaration stayed installed. The operator
// went on creating service principals in that account while the object on
// screen named another and reported that it could not be verified -- so a
// reader's only reasonable conclusion, that nothing was happening, was wrong.
//
// And a restart changed the answer. What is installed lives in memory, so the
// same cluster with the same spec behaved differently depending on whether the
// operator had happened to restart -- which nothing on either object records.
//
// The spec is edited here because that is the cheap way to reach the state. In a
// cluster it is reached by deleting the object and writing another one, which is
// what an install that never got its account id right does: accountId itself
// cannot be edited, and while any record names the account the deletion is
// refused.
func TestRepointingAtAnAccountThatFailsClearsTheOldOne(t *testing.T) {
	t.Parallel()
	failing := errors.New("this account cannot be verified")
	var verifies error
	r, c, accountInUse := newDatabricksAccountReconciler(t, writeToken(t, testIssuer, testAudience),
		func() error { return verifies },
		databricksAccountNamed(databricksAccountName))

	reconcileDatabricksAccount(t, r, databricksAccountName)
	if !accountInUse.Configured() {
		t.Fatal("nothing was installed for an account that verified")
	}

	// Repointed, and the new one does not verify.
	var repointed dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: operatorNamespace, Name: databricksAccountName}, &repointed); err != nil {
		t.Fatal(err)
	}
	repointed.Spec.AccountID = "an-account-that-does-not-work"
	if err := c.Update(context.Background(), &repointed); err != nil {
		t.Fatal(err)
	}
	verifies = failing
	reconcileDatabricksAccount(t, r, databricksAccountName)

	if accountInUse.Configured() {
		t.Error("the clients built from the old declaration are still installed; every " +
			"identity minted from here goes into an account this object no longer names")
	}
	if id := accountInUse.AccountID(); id != "" {
		t.Errorf("still acting in account %s", id)
	}

	// And what everything else now reports names the object rather than telling
	// somebody to create one they are looking at.
	err := accountInUse.DeleteServicePrincipal(context.Background(), "7788")
	if err == nil {
		t.Fatal("a call went through with nothing installed")
	}
	if !strings.Contains(err.Error(), "an-account-that-does-not-work") {
		t.Errorf("the error is %q; it has to say which account was named and that it was not "+
			"verified, because the object exists and the default message says to create it",
			err.Error())
	}
}

// TestOneBadCallDoesNotClearAWorkingAccount is the other half, and the reason
// clearing is conditional at all.
//
// Databricks being briefly unreachable is not a declaration that changed. Taking
// the clients away over it would put every workload's identity out of reach
// until it answered again, on an operator that had nothing wrong with it.
func TestOneBadCallDoesNotClearAWorkingAccount(t *testing.T) {
	t.Parallel()
	var verifies error
	r, _, accountInUse := newDatabricksAccountReconciler(t, writeToken(t, testIssuer, testAudience),
		func() error { return verifies },
		databricksAccountNamed(databricksAccountName))

	reconcileDatabricksAccount(t, r, databricksAccountName)
	if !accountInUse.Configured() {
		t.Fatal("nothing was installed for an account that verified")
	}

	verifies = errors.New("the service is temporarily unavailable")
	reconcileDatabricksAccount(t, r, databricksAccountName)

	if !accountInUse.Configured() {
		t.Error("one failed check took the account away; every identity in the cluster is out " +
			"of reach until Databricks answers, and nothing about the declaration changed")
	}
}

// TestAnOperatorThatCannotReadItsOwnTokenSaysSo covers an hour of looking
// healthy while nothing can be issued.
//
// Ready stays true, and truthfully: the account was reached and read. It goes on
// being reached, too, because the SDK holds the access token it already
// exchanged -- so for as long as that lasts, roughly an hour, the object reports
// health while no identity anywhere can be issued or converged. The only tell
// used to be a status line that stopped halfway, which nothing can be alerted on.
func TestAnOperatorThatCannotReadItsOwnTokenSaysSo(t *testing.T) {
	t.Parallel()
	r, c, _ := newDatabricksAccountReconciler(t, filepath.Join(t.TempDir(), "not-there"),
		func() error { return nil },
		databricksAccountNamed(databricksAccountName))
	reconcileDatabricksAccount(t, r, databricksAccountName)

	var got dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: operatorNamespace, Name: databricksAccountName}, &got); err != nil {
		t.Fatal(err)
	}

	prepared := meta.FindStatusCondition(got.Status.Conditions, conditionPrepared)
	if prepared == nil || prepared.Status != metav1.ConditionFalse {
		t.Fatalf("Prepared is %v; the operator cannot read its own token and nothing it issues "+
			"can name an issuer", prepared)
	}
	if prepared.Reason != reasonUnprepared {
		t.Errorf("reason is %s, want %s", prepared.Reason, reasonUnprepared)
	}

	// And Ready is not overloaded with it. What broke is not the account: it was
	// reached and read, and saying otherwise would report an account as wrong
	// for something that is not about it.
	ready := databricksAccountCondition(t, c, databricksAccountName)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready is %v; the account was reached and read, which is what it reports", ready)
	}
}

// TestNothingIsBlankedByAReadThatFailed covers the half of the same failure that
// was not a decision.
//
// The subject was overwritten unconditionally and the audience only when
// non-empty, so one failed read blanked one and left the other stale -- the same
// object saying two different things about the same moment.
func TestNothingIsBlankedByAReadThatFailed(t *testing.T) {
	t.Parallel()
	r, c, _ := newDatabricksAccountReconciler(t, writeToken(t, testSubject, testAudience),
		func() error { return nil },
		databricksAccountNamed(databricksAccountName))
	reconcileDatabricksAccount(t, r, databricksAccountName)

	var reported dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: operatorNamespace, Name: databricksAccountName}, &reported); err != nil {
		t.Fatal(err)
	}
	if reported.Status.Subject == "" || reported.Status.Audience == "" {
		t.Fatalf("status is %+v, want both read from the token", reported.Status)
	}

	// The token goes away.
	r.OwnToken.OIDCTokenFilepath = filepath.Join(t.TempDir(), "not-there")
	reconcileDatabricksAccount(t, r, databricksAccountName)

	var after dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: operatorNamespace, Name: databricksAccountName}, &after); err != nil {
		t.Fatal(err)
	}
	if after.Status.Subject != reported.Status.Subject {
		t.Errorf("subject went from %q to %q on a read that failed; the value is not wrong, it "+
			"was not read", reported.Status.Subject, after.Status.Subject)
	}
	if after.Status.Audience != reported.Status.Audience {
		t.Errorf("audience went from %q to %q", reported.Status.Audience, after.Status.Audience)
	}

	// Including the sentence Ready is made of. The account is still reachable on
	// the access token the SDK already exchanged, so this reads "acting in
	// account X as" and then stops -- on a condition that says True, at the one
	// moment somebody is comparing that subject against a federation policy.
	ready := databricksAccountCondition(t, c, databricksAccountName)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("Ready is %v; the account was reached and read, which is what it reports", ready)
	}
	if !strings.Contains(ready.Message, reported.Status.Subject) {
		t.Errorf("Ready says %q, want it to name %q -- the same read that failed already left "+
			"the value on the object, deliberately",
			ready.Message, reported.Status.Subject)
	}
}

// TestNotKnowingIsNotAnAnswerAboutANamespace covers the difference
// databricksAccountServes exists to carry: an operator that was told nothing
// and an operator that was told no.
//
// Both name no namespace, and only one of them is a refusal. The absent case is
// the one a caller must never act on -- a DatabricksAccount somebody deleted, or
// a first pass whose cache has not filled, would otherwise read as "this
// namespace is out of scope" and take the trust off every identity in the
// cluster on the strength of nobody having said anything.
func TestNotKnowingIsNotAnAnswerAboutANamespace(t *testing.T) {
	t.Parallel()
	elsewhere := databricksAccountNamed(databricksAccountName)
	elsewhere.Spec.Namespaces = []string{"somebody-elses-namespace"}
	here := databricksAccountNamed(databricksAccountName)
	here.Spec.Namespaces = []string{testNamespace}

	for _, asked := range []struct {
		what             string
		objects          []client.Object
		declared, served bool
	}{
		{
			what:     "no DatabricksAccount at all",
			declared: false,
			served:   false,
		},
		{
			what:     "a DatabricksAccount naming this namespace",
			objects:  []client.Object{here},
			declared: true,
			served:   true,
		},
		{
			what:     "a DatabricksAccount naming another namespace",
			objects:  []client.Object{elsewhere},
			declared: true,
			served:   false,
		},
	} {
		t.Run(asked.what, func(t *testing.T) {
			t.Parallel()
			c, _ := newFakeClient(t, asked.objects...)

			declared, served, err := databricksAccountServes(
				context.Background(), c, testOperatorRef, testNamespace)
			if err != nil {
				t.Fatal(err)
			}
			if declared != asked.declared {
				t.Errorf("declared is %v, want %v, given %s -- a caller that cannot tell "+
					"silence from a refusal acts on both, and one of them must never be "+
					"acted on", declared, asked.declared, asked.what)
			}
			if served != asked.served {
				t.Errorf("served is %v, want %v, given %s", served, asked.served, asked.what)
			}
		})
	}
}
