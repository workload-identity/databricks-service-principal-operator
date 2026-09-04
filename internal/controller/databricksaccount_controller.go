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
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	"github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

// EnvPodNamespace carries the operator's own namespace, set from the downward
// API in the Deployment. Kubernetes fills it in, so it says nothing about how
// this repository is organised -- renaming the operator namespace cannot leave it
// pointing somewhere stale.
const EnvPodNamespace = "POD_NAMESPACE"

const (
	// accountRetryAfterAwaited is how long an unusable account waits before
	// trying again. What fixes it -- the operator's service principal, its
	// federation policy, what it is allowed to do -- is done in Databricks,
	// which raises no event here.
	accountRetryAfterAwaited = time.Minute

	// accountRetryAfterSettled is the interval on a working account. It is a
	// liveness check on one account-level call: a federation policy removed in
	// Databricks would otherwise show up as every identity failing at once,
	// with nothing saying why.
	accountRetryAfterSettled = 10 * time.Minute

	// withdrawalClaimStale is how long a namespace may go without its holder
	// saying it is still there before another operator may take it. Five
	// accountRetryAfterAwaited intervals: the holder rewrites the timestamp on
	// every pass, so one missed pass is a slow one and five is an operator that
	// is not coming back.
	//
	// It does not have to dodge the SDK's own five-minute retry budget, which is
	// the obvious objection to a threshold this short. For a misjudgement to cost
	// anything, the holder would have to be blocked while the operator taking the
	// namespace off it can still reach Databricks -- and both talk to the same
	// account, so an outage that stops one stops the other, and the handover
	// costs nothing because neither can act.
	withdrawalClaimStale = 5 * time.Minute
)

// DatabricksAccountReconciler turns one DatabricksAccount into the clients the
// record's controller uses. It is the only other one that reaches Databricks at
// all: the DatabricksServiceAccount controller talks to nothing outside the
// cluster.
type DatabricksAccountReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// The DatabricksAccount this operator was told to use, by the
	// --databricks-account flag and its own namespace. Anything else is left
	// alone rather than adopted: which account the operator acts in is a
	// decision made when it is deployed, not by whoever can create an object.
	DatabricksAccountNamespacedName types.NamespacedName

	// Holder is what the other controllers hold. This controller is the only
	// writer.
	Holder *databricks.Holder

	// Runtime is the half of the config the Deployment decides.
	Runtime databricks.Config

	// build makes clients from a config. A field so a test can supply clients
	// that answer without a network; SetupWithManager leaves it as databricks.New.
	build func(databricks.Config) (databricks.Clients, error)

	// verify makes one cheap account-level call, to find out whether the token
	// exchange and the account coordinates actually work. Constructing a client
	// contacts nothing, so without this the first news of a wrong host would be
	// every identity failing at once.
	verify func(context.Context, databricks.Clients) error
}

// The identities are read to count the ones made in another account, and to find
// the namespaces this account has stopped naming. Read only: this controller
// writes none of them.
// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=databricksserviceaccounts,verbs=get;list;watch

// The only object this operator writes that it does not own -- the
// DatabricksServiceAccounts beside it are its own kind, which it makes and
// deletes. What it writes is one label key of its own, saying it is withdrawing
// from a namespace this account has stopped naming, and it takes that key off
// again when it has finished. MintLabel and InjectLabel are the cluster's,
// given to every operator serving the namespace at once, and nothing here reads
// or moves them.
//
// patch and not update, which is what confines the write to that one key: an
// update sends a whole Namespace, including a quota controller's annotations and
// another operator's labels, none of which this operator has any business
// sending. No create either, which is why every write here is made only after a
// read found the Namespace -- so a namespace deleted mid-pass is refused rather
// than recreated with nothing in it.
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=databricksaccounts,verbs=get;list;watch,namespace=system
// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=databricksaccounts/status,verbs=get;update;patch,namespace=system

// Reconcile builds the clients and reports whether they work.
//
// It never clears working clients on a failure. An account that stops answering
// is reported, and the previous clients stay in place: withdrawing them would
// stop every identity converging, which is a much larger consequence than the
// failure that caused it, and it would not fix anything.
// Only the object being deleted clears them, because then there is nothing left
// to act as.
func (r *DatabricksAccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if req.NamespacedName != r.DatabricksAccountNamespacedName {
		// Not the one this operator was told to use. Its own status says so;
		// see setNotSelected below.
		return ctrl.Result{}, r.setNotSelected(ctx, req.NamespacedName)
	}

	var databricksAccount dbxv1alpha1.DatabricksAccount
	if err := r.Get(ctx, req.NamespacedName, &databricksAccount); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		// Deleted. There is no account to act in, and saying so is better than
		// acting in one nobody has declared.
		r.Holder.Clear(fmt.Sprintf("DatabricksAccount %s was deleted", r.DatabricksAccountNamespacedName))
		return ctrl.Result{}, nil
	}

	// Read before anything else, so the subject and audience are reported even
	// when the exchange fails -- that is exactly when somebody needs to see them.
	//
	// Nothing is overwritten when the read fails. It used to blank the subject
	// and keep the audience, so half of what the object said went stale and the
	// other half went missing, for the same one failure.
	claims, claimsErr := databricks.ReadTokenClaims(r.Runtime.OIDCTokenFilepath)
	if claimsErr == nil {
		databricksAccount.Status.Subject = claims.Subject
		if len(claims.Audience) > 0 {
			databricksAccount.Status.Audience = claims.Audience[0]
		}
	}
	r.reportPrepared(&databricksAccount, claims, claimsErr)

	records, err := r.records(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Written before anything is attempted, because it is knowable without
	// attempting anything: it compares what this operator recorded against what
	// this object names. Computed only after a successful verification, a failed
	// switch left the previous pass's "every identity was made in this account"
	// standing under a spec naming another -- a present-tense claim, about the
	// wrong account, on the object whose edit caused it.
	r.reportAgreement(&databricksAccount, records)

	// Also before the account is contacted, and for a stronger reason than
	// knowability: suspending minting needs nothing from Databricks, and a
	// namespace this account has stopped naming must not go on minting into it
	// for as long as Databricks happens to be unreachable.
	withdrawn := r.withdraw(ctx, &databricksAccount, records)

	cfg := r.Runtime.ForAccount(databricksAccount.Spec.Host,
		databricksAccount.Spec.AccountID, databricksAccount.Spec.ClientID)

	// Withdrawn before the new declaration is tried, not after it succeeds.
	//
	// What is installed was built from a declaration that has been edited, so it
	// is not the answer to anything any more. Keeping it until the replacement
	// verifies means an operator whose spec names one account and which goes on
	// creating identities in another -- with the object on screen saying the
	// verification failed, so that a reader concludes nothing is happening.
	// Nothing anywhere compares the two, and a restart changes the answer,
	// because what is installed lives in memory.
	//
	// Only when the declaration itself changed. The same declaration failing is
	// one bad call, and withdrawing over that would take every workload's
	// identity out of reach until Databricks answered again.
	if installed, ok := r.Holder.BuiltFrom(); ok && installed != cfg {
		r.Holder.Clear(fmt.Sprintf(
			"DatabricksAccount %s was changed to name Databricks account %s, and that has not "+
				"been verified yet; read its status", r.DatabricksAccountNamespacedName, databricksAccount.Spec.AccountID))
	}

	clients, err := r.build(cfg)
	if err != nil {
		return r.reportReady(ctx, &databricksAccount, metav1.ConditionFalse, reasonInvalidSpec, err.Error())
	}

	if err := r.verify(ctx, clients); err != nil {
		// The operator's own claims being unreadable is worth saying here rather
		// than on their own: a refusal whose cause is a missing token file reads
		// as a policy problem otherwise.
		message := databricks.Reason(err)
		if claimsErr != nil {
			message = fmt.Sprintf("%s (and the operator's own token could not be read: %v)", message, claimsErr)
		}
		result := outcomeFor(err)
		return r.reportReady(ctx, &databricksAccount, result.Status, result.Reason, message)
	}

	r.Holder.Set(cfg, clients)

	// The subject as the status holds it, not as this pass read it. Verification
	// goes on passing for about an hour after the token file becomes unreadable
	// -- the SDK holds the access token it already exchanged -- so this line is
	// reached with the claims at their zero value, and rendering from them puts
	// "as " on a Ready account. The status keeps the last one read for the same
	// reason it is not blanked above: the value is not wrong, it was not read.
	result, err := r.reportReady(ctx, &databricksAccount, metav1.ConditionTrue, reasonAccountReady,
		fmt.Sprintf("acting in account %s as %s", databricksAccount.Spec.AccountID, databricksAccount.Status.Subject))
	if !withdrawn {
		// The settled interval is long because a working account has nobody
		// waiting on it. A withdrawal that has not finished has somebody waiting
		// on it, and what would finish it -- a namespace this operator could not
		// claim, a record whose trust is still in place -- raises no event here
		// either.
		result.RequeueAfter = accountRetryAfterAwaited
	}
	return result, err
}

// records is every identity this operator issued, read from the cache in the
// operator's own namespace.
//
// Both halves of that matter. The records are where an account id is written;
// the DatabricksServiceAccounts in tenants' namespaces are copies of them, and
// a copy is missing exactly when these questions are most worth asking -- an
// identity whose namespace was torn down still has its record and no longer has
// its copy. And another operator's identities are not this one's to read:
// they are in another namespace, in another account, and counting them here
// would say this operator has stranded something it never issued.
func (r *DatabricksAccountReconciler) records(ctx context.Context) (
	[]dbxv1alpha1.IssuedDatabricksServicePrincipal, error) {
	var identities dbxv1alpha1.IssuedDatabricksServicePrincipalList
	if err := r.List(ctx, &identities, client.InNamespace(r.DatabricksAccountNamespacedName.Namespace)); err != nil {
		return nil, err
	}
	return identities.Items, nil
}

// reportAgreement counts the identities this operator issued that were made in
// some other Databricks account.
//
// Read from what this operator recorded rather than from Databricks, and the
// count is only a report -- a failure to produce it makes the line wrong, never
// the operator unusable.
func (r *DatabricksAccountReconciler) reportAgreement(databricksAccount *dbxv1alpha1.DatabricksAccount,
	records []dbxv1alpha1.IssuedDatabricksServicePrincipal) {
	elsewhere := map[string]int{}
	for i := range records {
		made := records[i].Status.AccountID
		if made == "" || made == databricksAccount.Spec.AccountID {
			continue
		}
		elsewhere[made]++
	}

	if len(elsewhere) == 0 {
		setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionAccountsAgree,
			metav1.ConditionTrue, reasonHere,
			"every identity this operator issued was made in this account")
		return
	}

	accounts := make([]string, 0, len(elsewhere))
	for made := range elsewhere {
		accounts = append(accounts, made)
	}
	slices.Sort(accounts)

	var total int
	parts := make([]string, 0, len(accounts))
	for _, made := range accounts {
		total += elsewhere[made]
		parts = append(parts, fmt.Sprintf("%d in %s", elsewhere[made], made))
	}

	setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionAccountsAgree,
		metav1.ConditionFalse, reasonElsewhere,
		fmt.Sprintf("%d identit(ies) this operator issued were made in another Databricks account "+
			"(%s). Nothing is done for them and nothing about them is known from here; their "+
			"workloads are unaffected. Pointing this object back at the other account resumes "+
			"them, and does the same to the ones made here.",
			total, strings.Join(parts, ", ")))
}

// accountServes reports whether one namespace is this operator's to act in, and
// whether this operator has been told anything at all.
//
// Read from this operator's own DatabricksAccount, which lives in its own
// namespace and which only the team holding that account admin credential can
// write. It is their refusal, and it is the only one of the three answers that
// is: the cluster's mint label says a namespace may be served by somebody, and a
// ServiceAccount's annotation says which operator it asks, but neither can
// commit an operator to spending its credential.
//
// The two answers are separate because they are read in opposite directions.
// Not served refuses to make anything -- no DatabricksAccount, or one naming no
// namespaces, mints nothing, since acting on an absent answer would be this
// operator spending an account admin credential somewhere nobody said it could.
// Not served is also what withdraws the trust from an identity that exists, and
// there an absent answer must not be acted on at all: an object that has been
// deleted, or that this pass has not read yet, is this operator knowing nothing,
// and knowing nothing is never a reason to take something back.
//
// One function rather than a method on each controller that asks. Both the
// DatabricksServiceAccount controller and the record's decide what to do from
// this answer, and two spellings of it are two chances for one of them to read
// a namespace as served that the other does not -- which is an identity minted
// on one side and withdrawn on the other, at the same time, for ever.
func accountServes(ctx context.Context, reader client.Reader,
	name types.NamespacedName, namespace string) (declared, served bool, err error) {
	var databricksAccount dbxv1alpha1.DatabricksAccount
	switch err := reader.Get(ctx, name, &databricksAccount); {
	case apierrors.IsNotFound(err):
		return false, false, nil
	case err != nil:
		return false, false, err
	}
	return true, databricksAccount.Spec.Serves(namespace), nil
}

// withdraw suspends minting in every namespace this operator issued something in
// and this account no longer names, and reports whether the withdrawal has
// finished.
//
// The set is derived from the records rather than remembered from a previous
// spec. Nothing here holds the list as it was before the edit, and it does not
// need to: a namespace this operator issued an identity in and this account does
// not name is one it is no longer entitled to spend on, whether the edit was a
// moment ago or before this process started. A remembered list would also have
// to survive a restart, and the one thing that must not depend on this
// operator's memory is a withdrawal.
//
// Nothing is destroyed. The service principals stay, with every grant made on
// them, and so do the records and their ids -- what goes is, on the records
// themselves, the trust that lets a token from this cluster be exchanged. That
// is the only thing a cluster can take back: a running pod already holds its
// token, and Databricks alone decides whether to accept it.
//
// The suspension goes too, as soon as there is nothing left to take back. It is
// this operator saying "not while I am working", not a verdict on the namespace,
// and holding it any longer would leave every other operator serving that
// namespace unable to mint for a withdrawal that had finished.
func (r *DatabricksAccountReconciler) withdraw(ctx context.Context,
	databricksAccount *dbxv1alpha1.DatabricksAccount,
	records []dbxv1alpha1.IssuedDatabricksServicePrincipal) bool {
	// Every namespace out of scope, and how many identities in it this cluster
	// can still be exchanged for. The namespace is entered whether or not any of
	// them is outstanding, because a count of zero is what says the suspension
	// can be lifted -- a namespace missing from this map is one nothing is known
	// about, which is not the same answer.
	holding := map[string]int{}
	for i := range records {
		namespace := records[i].Spec.ServiceAccount.Namespace
		if databricksAccount.Spec.Serves(namespace) {
			continue
		}
		if _, seen := holding[namespace]; !seen {
			holding[namespace] = 0
		}
		if !withdrawnFromDatabricks(&records[i]) {
			holding[namespace]++
		}
	}

	if len(holding) == 0 {
		setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionNamespacesWithdrawn,
			metav1.ConditionTrue, reasonWithdrawn,
			"every namespace this operator issued an identity in is one this account names")
		return true
	}

	namespaces := make([]string, 0, len(holding))
	for namespace := range holding {
		namespaces = append(namespaces, namespace)
	}
	slices.Sort(namespaces)

	// Taken before anything is removed and given back once nothing is left to
	// remove, one namespace at a time. A namespace whose count is zero is one
	// every record has already been let go of in, so nothing there is waiting on
	// this operator any more.
	var unclaimed, stillHeld, trusted []string
	for _, namespace := range namespaces {
		if holding[namespace] > 0 {
			trusted = append(trusted, fmt.Sprintf("%s (%d)", namespace, holding[namespace]))
			if err := r.acquireWithdrawal(ctx, namespace); err != nil {
				unclaimed = append(unclaimed, fmt.Sprintf("%s (%v)", namespace, err))
			}
			continue
		}
		if err := r.releaseWithdrawal(ctx, namespace); err != nil {
			stillHeld = append(stillHeld, fmt.Sprintf("%s (%v)", namespace, err))
		}
	}

	if len(unclaimed) == 0 && len(stillHeld) == 0 && len(trusted) == 0 {
		setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionNamespacesWithdrawn,
			metav1.ConditionTrue, reasonWithdrawn,
			fmt.Sprintf("this account no longer serves %s: no token from this cluster can be "+
				"exchanged for the identities it issued there, and minting is no longer "+
				"suspended, so any other operator serving those namespaces goes on as before. "+
				"Their service principals and every grant on them are untouched, so naming a "+
				"namespace here again restores the exchange on the same client ids.",
				strings.Join(namespaces, ", ")))
		return true
	}

	var said []string
	if len(unclaimed) > 0 {
		// Named separately from the trust below because they are fixed by
		// different people: this one by whoever can write a Namespace, or by
		// whoever runs the operator named in the message, and neither is
		// necessarily whoever holds this account.
		said = append(said, fmt.Sprintf(
			"this operator does not hold %s on %s, so it has removed nothing there: it will "+
				"not take the trust off identities in a namespace that can still mint them",
			dbxv1alpha1.WithdrawingLabel, strings.Join(unclaimed, ", ")))
	}
	if len(stillHeld) > 0 {
		// The trust is gone and only the suspension is left, which is why this is
		// said apart from everything above: nothing is waiting on Databricks, and
		// what it costs is every other operator serving these namespaces, whose
		// minting does not resume until the key comes off.
		said = append(said, fmt.Sprintf(
			"there is nothing left to withdraw from %s and this operator could not take %s off "+
				"them again, so no operator is minting there",
			strings.Join(stillHeld, ", "), dbxv1alpha1.WithdrawingLabel))
	}
	if len(trusted) > 0 {
		said = append(said, fmt.Sprintf(
			"identities in %s can still be exchanged for from this cluster; their records say "+
				"why under Ready, in namespace %s",
			strings.Join(trusted, ", "), r.DatabricksAccountNamespacedName.Namespace))
	}
	setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionNamespacesWithdrawn,
		metav1.ConditionFalse, reasonWithdrawing,
		fmt.Sprintf("%s. Nothing has been destroyed by this and nothing is destroyed by its not "+
			"finishing: every service principal and every grant on it is where it was.",
			strings.Join(said, ". ")))
	return false
}

// withdrawnFromDatabricks reports whether this cluster can still be exchanged
// for the identity this record names.
//
// Read from the condition the record already writes rather than from a field
// beside it. A second place saying the same thing is a second place for the two
// to disagree, and the disagreement that matters is this account reporting a
// withdrawal finished while the record it counted says the trust is still there.
//
// RemovedInDatabricks counts as withdrawn because the service principal itself
// is gone, and its federation policies went with it -- measured against a live
// account. There is nothing left to take back.
func withdrawnFromDatabricks(issued *dbxv1alpha1.IssuedDatabricksServicePrincipal) bool {
	ready := meta.FindStatusCondition(issued.Status.Conditions, conditionReady)
	if ready == nil {
		return false
	}
	return ready.Reason == reasonNotServed || ready.Reason == reasonRemovedInDatabricks
}

// acquireWithdrawal claims a namespace for this operator's withdrawal, and says
// again on every pass that the claim is still wanted.
//
// The claim is one label key, and server-side apply is the whole of the mutual
// exclusion. metadata.labels is a map whose keys are owned one at a time -- the
// same mechanism status.identities already relies on -- so a second operator
// applying this key over somebody else's value is refused a conflict. Being
// refused is what it means to lose, and there is no register to keep and nothing
// to unwind after a crash.
//
// Never forced except over a claim nobody has refreshed. Forcing takes the field
// whatever it says, which would leave nothing here excluding anything.
//
// The timestamp is rewritten every pass because managedFields[].time does not
// move when the applied content is identical -- measured against a live cluster
// -- so an unchanging annotation would say when the claim was made and never
// that its holder is still running.
func (r *DatabricksAccountReconciler) acquireWithdrawal(ctx context.Context, name string) error {
	// Read before the apply because this operator holds no create on Namespaces:
	// an apply naming one that is not there is a create, and a namespace that is
	// gone is one nothing can be minted into and so one with nothing to claim.
	// Its records stay, and their trust is taken back on their own controller's
	// pass.
	switch _, exists, err := withdrawalHeldBy(ctx, r.Client, name); {
	case err != nil:
		return err
	case !exists:
		return nil
	}

	err := r.Apply(ctx, withdrawingBy(name, r.DatabricksAccountNamespacedName, time.Now()), r.owner())
	if !apierrors.IsConflict(err) {
		return err
	}

	// Read after the refusal rather than before it, because what is wanted is
	// who holds it now: a read taken first would name whoever held it when this
	// pass started, which is exactly the operator this one has just been told it
	// is not.
	var namespace corev1.Namespace
	if err := r.Get(ctx, types.NamespacedName{Name: name}, &namespace); err != nil {
		return client.IgnoreNotFound(err)
	}
	holder := namespace.Labels[dbxv1alpha1.WithdrawingLabel]
	said := namespace.Annotations[dbxv1alpha1.WithdrawingSinceAnnotation]

	since, unreadable := time.Parse(time.RFC3339, said)
	if unreadable != nil {
		// A claim whose age cannot be read is not evidence that its holder is
		// gone, and taking it on that would be this operator inventing the one
		// fact that says it may. Reported instead, every pass, which is what
		// puts it in front of somebody.
		return fmt.Errorf("held by %s, which has not said when: %q", holder, said)
	}
	if age := time.Since(since); age < withdrawalClaimStale {
		return fmt.Errorf("held by %s, last seen %s ago", holder, age.Truncate(time.Second))
	}

	// The one force there is. Its holder stopped saying it was there long enough
	// ago that waiting for it is waiting for nothing, and the operator it is
	// taken from finds out the same way anybody else does: its next apply is
	// refused, because the field is no longer its own.
	return r.Apply(ctx, withdrawingBy(name, r.DatabricksAccountNamespacedName, time.Now()),
		r.owner(), client.ForceOwnership)
}

// releaseWithdrawal gives a namespace back, by applying the same object without
// the key: an apply keeps only what it sends.
//
// Only by the operator that holds it. An operator that was never the holder, or
// that was taken over by another, has nothing here to give back, and sending
// this anyway would be one operator ending another's withdrawal halfway through.
func (r *DatabricksAccountReconciler) releaseWithdrawal(ctx context.Context, name string) error {
	var namespace corev1.Namespace
	switch err := r.Get(ctx, types.NamespacedName{Name: name}, &namespace); {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return err
	}
	if namespace.Labels[dbxv1alpha1.WithdrawingLabel] != withdrawnBy(r.DatabricksAccountNamespacedName) {
		return nil
	}
	return r.Apply(ctx, corev1ac.Namespace(name), r.owner())
}

// withdrawalHeldBy is the operator that has suspended minting in this namespace,
// and whether there is a namespace at all.
//
// The two are separate answers because a namespace that is gone is not a
// namespace nobody holds. Nothing can be minted into one that does not exist, so
// there is nothing for a claim to protect there and a withdrawal has nothing to
// wait for -- while a namespace that is there and unclaimed is one this operator
// must claim before it removes anything.
func withdrawalHeldBy(ctx context.Context, reader client.Reader, name string) (
	holder string, exists bool, err error) {
	var namespace corev1.Namespace
	switch err := reader.Get(ctx, types.NamespacedName{Name: name}, &namespace); {
	case apierrors.IsNotFound(err):
		return "", false, nil
	case err != nil:
		return "", false, err
	}
	return namespace.Labels[dbxv1alpha1.WithdrawingLabel], true, nil
}

// withdrawingBy is the claim as this operator sends it: the key, its own name,
// and the time it is saying so.
func withdrawingBy(name string, account types.NamespacedName,
	at time.Time) *corev1ac.NamespaceApplyConfiguration {
	return corev1ac.Namespace(name).
		WithLabels(map[string]string{dbxv1alpha1.WithdrawingLabel: withdrawnBy(account)}).
		WithAnnotations(map[string]string{
			dbxv1alpha1.WithdrawingSinceAnnotation: at.UTC().Format(time.RFC3339),
		})
}

// withdrawnBy is how one operator names itself to another in a label value.
//
// A "." where every other reference to an operator in this API uses a "/",
// because a label value may not hold one. Both halves are read back by people,
// not parsed: what a loser does with the name is print it.
func withdrawnBy(account types.NamespacedName) string {
	return account.Namespace + "." + account.Name
}

func (r *DatabricksAccountReconciler) owner() client.FieldOwner {
	return fieldOwnerFor(r.DatabricksAccountNamespacedName)
}

// setNotSelected marks an account this operator was not told to use.
//
// Silence would be worse than anything it costs: somebody who wrote a second
// object and sees no status has no way to tell it from an operator that is not
// running, and the object that is in use is named here so the difference is one
// kubectl away.
func (r *DatabricksAccountReconciler) setNotSelected(ctx context.Context, name types.NamespacedName) error {
	var databricksAccount dbxv1alpha1.DatabricksAccount
	if err := r.Get(ctx, name, &databricksAccount); err != nil {
		return client.IgnoreNotFound(err)
	}
	setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionReady,
		metav1.ConditionFalse, reasonNotSelected,
		fmt.Sprintf("this operator uses %s; change --databricks-account to select this one instead",
			r.DatabricksAccountNamespacedName))
	return client.IgnoreNotFound(r.Status().Update(ctx, &databricksAccount))
}

// reportReady ends the pass: it writes Ready, comes back at the interval that
// status asks for, and does the status update.
//
// The update is what tells it from reportAgreement and reportPrepared, which set
// their condition on the object in memory and return nothing. This is what
// carries them to the API server.
func (r *DatabricksAccountReconciler) reportReady(ctx context.Context,
	databricksAccount *dbxv1alpha1.DatabricksAccount,
	status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {
	setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionReady,
		status, reason, message)
	return ctrl.Result{
		RequeueAfter: retryAfterFor(status, accountRetryAfterAwaited, accountRetryAfterSettled),
	}, client.IgnoreNotFound(r.Status().Update(ctx, databricksAccount))
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabricksAccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.build == nil {
		r.build = databricks.New
	}
	if r.verify == nil {
		r.verify = verifyAccount
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&dbxv1alpha1.DatabricksAccount{}).
		Named("databricksaccount").
		Complete(r)
}

// verifyAccount makes the cheapest account-level call there is.
//
// Listing workspaces costs no compute and produces no DBU -- the operator must
// never issue SQL, which would wake a serverless warehouse and keep pushing its
// auto-stop out on every reconcile. What this proves is the whole chain: the
// projected token was accepted, the federation policy matched, the account id is
// right, and the service principal can read something.
func verifyAccount(ctx context.Context, clients databricks.Clients) error {
	accountClient := clients.AccountClient()
	if accountClient == nil {
		return fmt.Errorf("no account client was built")
	}
	if _, err := accountClient.Workspaces.List(ctx); err != nil {
		return fmt.Errorf("listing the account's workspaces: %w", err)
	}
	return nil
}

// accountChangedForRecords selects the writes on this object worth waking every
// record for.
//
// Two things on it decide what a record's pass does, and a record can see
// neither from where it sits.
//
// Whether the account is usable: on the way in, everything waiting on
// NotConfigured can now be resolved, and waiting out each record's own retry
// interval would leave the operator looking broken for a minute after it was
// fixed.
//
// Which namespaces it serves: taking one off that list is a withdrawal, and the
// records in it are where the withdrawal is carried out. Nothing else says so --
// the edit changes no record, no ServiceAccount and no Namespace -- so without
// this the trust stays in place until each record comes round on its own
// interval, which for a settled identity is ten minutes. Putting a namespace
// back has the same shape and the same wait.
//
// Everything else is skipped, and that is what keeps this from being a loop:
// every reconcile of an account writes its status, and waking every record on
// each of those would put the whole catalogue back in the queue once a minute
// for no reason.
func accountChangedForRecords(selected types.NamespacedName) predicate.Predicate {
	ready := func(o client.Object) bool {
		databricksAccount, ok := o.(*dbxv1alpha1.DatabricksAccount)
		if !ok || client.ObjectKeyFromObject(databricksAccount) != selected {
			return false
		}
		for _, c := range databricksAccount.Status.Conditions {
			if c.Type == conditionReady {
				return c.Status == metav1.ConditionTrue
			}
		}
		return false
	}
	// Sorted, because the field is a set: two spellings of the same set are the
	// same declaration, and reordering it is not a reason to wake every record
	// in the cluster. Another account's answer is nil on both sides, so nothing
	// it does compares as a change here.
	served := func(o client.Object) []string {
		databricksAccount, ok := o.(*dbxv1alpha1.DatabricksAccount)
		if !ok || client.ObjectKeyFromObject(databricksAccount) != selected {
			return nil
		}
		return slices.Sorted(slices.Values(databricksAccount.Spec.Namespaces))
	}
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return ready(e.Object) },
		DeleteFunc: func(e event.DeleteEvent) bool { return client.ObjectKeyFromObject(e.Object) == selected },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return ready(e.ObjectOld) != ready(e.ObjectNew) ||
				!slices.Equal(served(e.ObjectOld), served(e.ObjectNew))
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// enqueueOnAccountChange wakes every recorded identity when the selected
// DatabricksAccount says something new about them: that it can be acted in, or
// that it no longer serves the namespace one of them is in.
//
// Without it, an operator whose account was just fixed stays visibly broken, and
// a namespace taken out of scope keeps its trust, for as long as each object's
// own retry interval -- ten minutes for a settled identity. Nothing else
// reports either change: one happened in Databricks and the other is an edit to
// a field no record watches.
//
// Every record is woken and not only the ones affected, because which ones those
// are cannot be worked out from the event: the list before the edit is not
// available here, and a record whose namespace was just removed is exactly the
// one the new list does not name.
func enqueueOnAccountChange(mgr ctrl.Manager, account types.NamespacedName, list client.ObjectList,
	keys func(client.ObjectList) []reconcile.Request) (client.Object, handler.EventHandler, builder.WatchesOption) {
	mapFunc := func(ctx context.Context, _ client.Object) []reconcile.Request {
		items := list.DeepCopyObject().(client.ObjectList)
		if err := mgr.GetClient().List(ctx, items); err != nil {
			// Dropping the wake-up costs a wait, not correctness: every object
			// comes back on its own interval regardless.
			return nil
		}
		return keys(items)
	}
	return &dbxv1alpha1.DatabricksAccount{},
		handler.EnqueueRequestsFromMapFunc(mapFunc),
		builder.WithPredicates(accountChangedForRecords(account))
}

// issuedRequests turns a listing of records into requests. It exists because
// ObjectList carries no way to reach its items generically.
func issuedRequests(list client.ObjectList) []reconcile.Request {
	items := list.(*dbxv1alpha1.IssuedDatabricksServicePrincipalList).Items
	requests := make([]reconcile.Request, 0, len(items))
	for i := range items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&items[i]),
		})
	}
	return requests
}

// reportPrepared says whether the operator knows what it will write into every
// federation policy and give to every pod.
//
// It is written on every pass and independently of everything else, because what
// it reports fails independently: the account can be reached and read while this
// is false, and it stays that way for as long as the SDK holds the access token
// it already exchanged.
func (r *DatabricksAccountReconciler) reportPrepared(databricksAccount *dbxv1alpha1.DatabricksAccount,
	claims databricks.TokenClaims, err error) {
	switch {
	case err != nil:
		setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionPrepared,
			metav1.ConditionFalse, reasonUnprepared,
			fmt.Sprintf("the operator's own projected token could not be read (%v), so the "+
				"issuer every federation policy names and the audience every pod is given are "+
				"unknown. No identity can be issued or converged until it can be. Existing "+
				"workloads are unaffected: exchanging a token does not go through this operator.",
				err))
	case strings.TrimSpace(claims.Issuer) == "":
		setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionPrepared,
			metav1.ConditionFalse, reasonUnprepared,
			"the operator's own projected token carries no iss claim, so the issuer every "+
				"federation policy names is unknown. No identity can be issued or converged.")
	case len(claims.Audience) == 0 || claims.Audience[0] == "":
		setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionPrepared,
			metav1.ConditionFalse, reasonUnprepared,
			"the operator's own projected token carries no aud claim, so the audience every "+
				"pod is given is unknown. A federation policy naming none is one no token can "+
				"satisfy, so nothing is written.")
	default:
		setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionPrepared,
			metav1.ConditionTrue, reasonPrepared,
			fmt.Sprintf("writing %s into every federation policy and giving %s to every pod",
				claims.Issuer, claims.Audience[0]))
	}
}
