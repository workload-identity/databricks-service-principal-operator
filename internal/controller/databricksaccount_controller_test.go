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
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/databricks/databricks-sdk-go/apierr"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	dbx "github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

const (
	operatorNamespace = "databricks-operator-system"
	accountObject     = "databricks-account"
	testAccountID     = "aaaaaaaa-0000-0000-0000-000000000000"
	testClientID      = "bbbbbbbb-0000-0000-0000-000000000000"
	testSubject       = "system:serviceaccount:databricks-operator-system:databricks-operator-controller-manager"
)

// writeToken writes a projected token carrying the given claims, so that the
// controller reads them the way it will in a cluster rather than being handed
// them.
func writeToken(t *testing.T, subject, audience string) string {
	return writeTokenAs(t, "https://oidc.example/id/X", subject, audience)
}

// writeTokenAs is the same, with the issuer said out loud. The issuer is what
// every federation policy names, so a test about one has to choose it.
func writeTokenAs(t *testing.T, issuer, subject, audience string) string {
	t.Helper()
	payload := `{"iss":"` + issuer + `","sub":"` + subject + `","aud":["` + audience + `"]}`
	token := "header." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".signature"
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func databricksAccountNamed(name string) *dbxv1alpha1.DatabricksAccount {
	return &dbxv1alpha1.DatabricksAccount{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: operatorNamespace},
		Spec: dbxv1alpha1.DatabricksAccountSpec{
			Host:      "https://accounts.cloud.databricks.com",
			AccountID: testAccountID,
			ClientID:  testClientID,
		},
	}
}

// newAccountReconciler wires a reconciler whose client building and account call
// are both under the test's control, so nothing here reaches Databricks.
func newAccountReconciler(t *testing.T, tokenPath string, verify func() error,
	objects ...client.Object) (*DatabricksAccountReconciler, client.Client, *dbx.AccountInUse) {
	t.Helper()
	c, scheme := newFakeClient(t, objects...)
	accountInUse := dbx.NewAccountInUse(operatorNamespace, accountObject)
	built := &stubClients{}
	return &DatabricksAccountReconciler{
		Client:                          c,
		Scheme:                          scheme,
		DatabricksAccountNamespacedName: types.NamespacedName{Namespace: operatorNamespace, Name: accountObject},
		AccountInUse:                    accountInUse,
		OwnToken:                        dbx.Config{OIDCTokenFilepath: tokenPath, TokenAudience: "databricks"},
		build:                           func(dbx.Config) (dbx.Clients, error) { return built, nil },
		verify:                          func(context.Context, dbx.Clients) error { return verify() },
	}, c, accountInUse
}

func reconcileAccount(t *testing.T, r *DatabricksAccountReconciler, name string) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: operatorNamespace, Name: name},
	}); err != nil {
		t.Fatalf("reconciling %s: %v", name, err)
	}
}

func accountCondition(t *testing.T, c client.Client, name string) *metav1.Condition {
	t.Helper()
	var got dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: operatorNamespace, Name: name,
	}, &got); err != nil {
		t.Fatalf("getting %s: %v", name, err)
	}
	return meta.FindStatusCondition(got.Status.Conditions, conditionReady)
}

func accountStatus(t *testing.T, c client.Client, name string) dbxv1alpha1.DatabricksAccountStatus {
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
	r, c, accountInUse := newAccountReconciler(t,
		writeToken(t, testSubject, "databricks"),
		func() error { return nil },
		databricksAccountNamed(accountObject))
	reconcileAccount(t, r, accountObject)

	status := accountStatus(t, c, accountObject)
	if status.Subject != testSubject {
		t.Errorf("subject is %q, want %q", status.Subject, testSubject)
	}
	if status.Audience != "databricks" {
		t.Errorf("audience is %q, want the aud the token carries", status.Audience)
	}
	if ready := accountCondition(t, c, accountObject); ready == nil || ready.Status != metav1.ConditionTrue {
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
	r, c, accountInUse := newAccountReconciler(t,
		writeToken(t, testSubject, "databricks"),
		func() error { return refused },
		databricksAccountNamed(accountObject))
	reconcileAccount(t, r, accountObject)

	status := accountStatus(t, c, accountObject)
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
	r, c, accountInUse := newAccountReconciler(t,
		writeToken(t, testSubject, "databricks"),
		func() error {
			if fail {
				return errors.New("the service is temporarily unavailable")
			}
			return nil
		},
		databricksAccountNamed(accountObject))

	reconcileAccount(t, r, accountObject)
	if !accountInUse.Configured() {
		t.Fatal("the first reconcile installed no clients")
	}

	fail = true
	reconcileAccount(t, r, accountObject)

	if !accountInUse.Configured() {
		t.Error("one failed account call withdrew the clients every other object depends on")
	}
	if ready := accountCondition(t, c, accountObject); ready == nil || ready.Status == metav1.ConditionTrue {
		t.Errorf("Ready is %v, want it not True: the failure still has to be reported", ready)
	}
}

// TestAccountDeletionClearsTheClients covers the one case that does clear them.
// Nobody has declared an account any more, and continuing to act in one that
// was deleted is worse than reporting that there is none.
func TestAccountDeletionClearsTheClients(t *testing.T) {
	t.Parallel()
	r, c, accountInUse := newAccountReconciler(t,
		writeToken(t, testSubject, "databricks"),
		func() error { return nil },
		databricksAccountNamed(accountObject))
	reconcileAccount(t, r, accountObject)
	if !accountInUse.Configured() {
		t.Fatal("the first reconcile installed no clients")
	}

	var live dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: operatorNamespace, Name: accountObject,
	}, &live); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	reconcileAccount(t, r, accountObject)

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
	r, c, accountInUse := newAccountReconciler(t,
		writeToken(t, testSubject, "databricks"),
		func() error { return nil },
		databricksAccountNamed(other))
	reconcileAccount(t, r, other)

	ready := accountCondition(t, c, other)
	if ready == nil || ready.Status != metav1.ConditionFalse {
		t.Fatalf("Ready is %v, want False", ready)
	}
	if ready.Reason != reasonNotSelected {
		t.Errorf("reason is %q, want %q", ready.Reason, reasonNotSelected)
	}
	// The one in use has to be named, or the reader is told they are wrong
	// without being told what is right.
	if !strings.Contains(ready.Message, accountObject) {
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
	accountInUse := dbx.NewAccountInUse(operatorNamespace, accountObject)

	_, err := accountInUse.ServicePrincipalExists(context.Background(), "7788")
	if err == nil {
		t.Fatal("an unconfigured AccountInUse answered a lookup")
	}
	if got := dbx.KindOf(err); got != dbx.NotConfigured {
		t.Errorf("KindOf is %q, want %q", got, dbx.NotConfigured)
	}
	// The message has to name the object to create. "not configured" on its own
	// tells somebody they are stuck without telling them what unsticks them.
	if !strings.Contains(err.Error(), accountObject) || !strings.Contains(err.Error(), operatorNamespace) {
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
// this operator reports may count it: it was issued by another operator, holding
// another account admin credential, and saying this operator stranded it is
// saying something untrue about somebody else's work.
func identityOfAnotherOperator(name, accountID string) *dbxv1alpha1.IssuedDatabricksServicePrincipal {
	issued := identityMadeIn(name, accountID)
	issued.Namespace = "another-operator"
	return issued
}

// TestIdentitiesMadeElsewhereAreCountedHere covers the gap between the object
// somebody edits and the objects that carry the consequence.
//
// Pointing this object at a different account leaves every identity made in the
// old one reporting AccountMismatch, and none of them fails: their workloads
// keep working, because exchanging a token does not go through this operator.
// Nothing alerts and nothing resolves on its own, so the state is permanent and
// invisible, and finding it means reading every identity's status one at a time.
func TestIdentitiesMadeElsewhereAreCountedHere(t *testing.T) {
	t.Parallel()
	r, c, _ := newAccountReconciler(t, writeToken(t, testIssuer, testAudience),
		func() error { return nil },
		databricksAccountNamed(accountObject),
		identityMadeIn("etl", "an-older-account"),
		identityMadeIn("loader", "an-older-account"),
		identityMadeIn("current", testAccountID))
	reconcileAccount(t, r, accountObject)

	var got dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: operatorNamespace, Name: accountObject}, &got); err != nil {
		t.Fatal(err)
	}

	agree := meta.FindStatusCondition(got.Status.Conditions, conditionAccountsAgree)
	if agree == nil || agree.Status != metav1.ConditionFalse {
		t.Fatalf("AccountsAgree is %v, want it to say some identities were made elsewhere", agree)
	}
	if !strings.Contains(agree.Message, "2") {
		t.Errorf("message is %q, want the count -- one at a time is the thing this replaces",
			agree.Message)
	}
	if !strings.Contains(agree.Message, "an-older-account") {
		t.Errorf("message is %q, want the account to point back at", agree.Message)
	}

	// Ready is unaffected: the operator can act in this account perfectly well.
	ready := accountCondition(t, c, accountObject)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready is %v; holding identities from another account does not stop the "+
			"operator acting in this one", ready)
	}
}

// TestAnotherOperatorsIdentitiesAreNotCounted covers the boundary, on the one
// report that reads more than one object.
//
// Two operators in a cluster hold two account admin credentials for two Databricks
// accounts, and each is the trust boundary of its own. An operator that counted
// the other's records would say, on the object somebody reads when they are
// worried, that it has stranded identities it never issued -- and it would say it
// permanently, since nothing it does can resolve them.
func TestAnotherOperatorsIdentitiesAreNotCounted(t *testing.T) {
	t.Parallel()
	r, c, _ := newAccountReconciler(t, writeToken(t, testIssuer, testAudience),
		func() error { return nil },
		databricksAccountNamed(accountObject),
		identityMadeIn("current", testAccountID),
		identityOfAnotherOperator("theirs", "an-account-this-operator-never-saw"))
	reconcileAccount(t, r, accountObject)

	var got dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: operatorNamespace, Name: accountObject}, &got); err != nil {
		t.Fatal(err)
	}

	agree := meta.FindStatusCondition(got.Status.Conditions, conditionAccountsAgree)
	if agree == nil || agree.Status != metav1.ConditionTrue {
		t.Fatalf("AccountsAgree is %v; the only identity this operator issued was made in this "+
			"account, and the other one is not this operator's to count", agree)
	}
	if strings.Contains(agree.Message, "an-account-this-operator-never-saw") {
		t.Errorf("message is %q; it names another operator's Databricks account", agree.Message)
	}
}

// TestAllIdentitiesHereIsSaidRatherThanLeftBlank covers the ordinary case.
//
// An absent condition reads the same as an operator that has not looked, and the
// question it answers -- are the identities in this cluster this account's -- is
// one somebody asks precisely when they are unsure.
func TestAllIdentitiesHereIsSaidRatherThanLeftBlank(t *testing.T) {
	t.Parallel()
	r, c, _ := newAccountReconciler(t, writeToken(t, testIssuer, testAudience),
		func() error { return nil },
		databricksAccountNamed(accountObject),
		identityMadeIn("etl", testAccountID))
	reconcileAccount(t, r, accountObject)

	var got dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: operatorNamespace, Name: accountObject}, &got); err != nil {
		t.Fatal(err)
	}
	agree := meta.FindStatusCondition(got.Status.Conditions, conditionAccountsAgree)
	if agree == nil || agree.Status != metav1.ConditionTrue {
		t.Errorf("AccountsAgree is %v, want it said rather than left to be inferred", agree)
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
// The guard added for identities recorded elsewhere could not see it either: it
// asks which account the operator is acting in, and the answer was still the old
// one, so it compared that account against itself and passed.
//
// And a restart changed the answer. What is installed lives in memory, so the
// same cluster with the same spec behaved differently depending on whether the
// operator had happened to restart -- which nothing on either object records.
func TestRepointingAtAnAccountThatFailsClearsTheOldOne(t *testing.T) {
	t.Parallel()
	failing := errors.New("this account cannot be verified")
	var verifies error
	r, c, accountInUse := newAccountReconciler(t, writeToken(t, testIssuer, testAudience),
		func() error { return verifies },
		databricksAccountNamed(accountObject))

	reconcileAccount(t, r, accountObject)
	if !accountInUse.Configured() {
		t.Fatal("nothing was installed for an account that verified")
	}

	// Repointed, and the new one does not verify.
	var repointed dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: operatorNamespace, Name: accountObject}, &repointed); err != nil {
		t.Fatal(err)
	}
	repointed.Spec.AccountID = "an-account-that-does-not-work"
	if err := c.Update(context.Background(), &repointed); err != nil {
		t.Fatal(err)
	}
	verifies = failing
	reconcileAccount(t, r, accountObject)

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
	r, _, accountInUse := newAccountReconciler(t, writeToken(t, testIssuer, testAudience),
		func() error { return verifies },
		databricksAccountNamed(accountObject))

	reconcileAccount(t, r, accountObject)
	if !accountInUse.Configured() {
		t.Fatal("nothing was installed for an account that verified")
	}

	verifies = errors.New("the service is temporarily unavailable")
	reconcileAccount(t, r, accountObject)

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
	r, c, _ := newAccountReconciler(t, filepath.Join(t.TempDir(), "not-there"),
		func() error { return nil },
		databricksAccountNamed(accountObject))
	reconcileAccount(t, r, accountObject)

	var got dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: operatorNamespace, Name: accountObject}, &got); err != nil {
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
	ready := accountCondition(t, c, accountObject)
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
	r, c, _ := newAccountReconciler(t, writeToken(t, testSubject, testAudience),
		func() error { return nil },
		databricksAccountNamed(accountObject))
	reconcileAccount(t, r, accountObject)

	var reported dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: operatorNamespace, Name: accountObject}, &reported); err != nil {
		t.Fatal(err)
	}
	if reported.Status.Subject == "" || reported.Status.Audience == "" {
		t.Fatalf("status is %+v, want both read from the token", reported.Status)
	}

	// The token goes away.
	r.OwnToken.OIDCTokenFilepath = filepath.Join(t.TempDir(), "not-there")
	reconcileAccount(t, r, accountObject)

	var after dbxv1alpha1.DatabricksAccount
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: operatorNamespace, Name: accountObject}, &after); err != nil {
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
	ready := accountCondition(t, c, accountObject)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("Ready is %v; the account was reached and read, which is what it reports", ready)
	}
	if !strings.Contains(ready.Message, reported.Status.Subject) {
		t.Errorf("Ready says %q, want it to name %q -- the same read that failed already left "+
			"the value on the object, deliberately",
			ready.Message, reported.Status.Subject)
	}
}
