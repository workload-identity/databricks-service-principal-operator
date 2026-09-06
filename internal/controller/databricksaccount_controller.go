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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
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
	// databricksAccountRetryAfterAwaited is how long an unusable account waits
	// before trying again. What fixes it -- the operator's service principal,
	// its federation policy, what it is allowed to do -- is done in Databricks,
	// which raises no event here.
	databricksAccountRetryAfterAwaited = time.Minute

	// databricksAccountRetryAfterSettled is the interval on a working account.
	// It is a liveness check on one account-level call: a federation policy
	// removed in Databricks would otherwise show up as every identity failing at
	// once, with nothing saying why.
	databricksAccountRetryAfterSettled = 10 * time.Minute

	// destructionClaimStale is how long a namespace may go without its holder
	// saying it is still there before another operator may take it. Five
	// databricksAccountRetryAfterAwaited intervals: the holder rewrites the
	// timestamp on every pass, so one missed pass is a slow one and five is an
	// operator that is not coming back.
	//
	// It does not have to dodge the SDK's own five-minute retry budget, which is
	// the obvious objection to a threshold this short. For a misjudgement to cost
	// anything, the holder would have to be blocked while the operator taking the
	// namespace off it can still reach Databricks -- and both talk to the same
	// account, so an outage that stops one stops the other, and the handover
	// costs nothing because neither can act.
	destructionClaimStale = 5 * time.Minute
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

	// AccountInUse is what the other controllers hold. This controller is the only
	// writer.
	AccountInUse *databricks.AccountInUse

	// OwnToken is where the operator's own projected token is mounted and what
	// audience it carries: the half of the config the Deployment decides. The
	// other half -- host, account and client -- is on the DatabricksAccount.
	OwnToken databricks.Config

	// build makes clients from a config. A field so a test can supply clients
	// that answer without a network; SetupWithManager leaves it as databricks.New.
	build func(databricks.Config) (databricks.Clients, error)

	// verify makes one cheap account-level call, to find out whether the token
	// exchange and the account coordinates actually work. Constructing a client
	// contacts nothing, so without this the first news of a wrong host would be
	// every identity failing at once.
	verify func(context.Context, databricks.Clients) error
}

// The identities are read to count the ones that still name this Databricks
// account, and to find the namespaces this account has stopped naming. Read
// only: this controller writes none of them.
// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=databricksserviceaccounts,verbs=get;list;watch

// Every ServiceAccount in the cluster, to count the ones asking this operator in
// a namespace this account does not name. Cluster-wide because that set is
// exactly the namespaces outside this account's reach. Read only, and the same
// rule the DatabricksServiceAccount controller already holds -- stated here
// anyway, so that narrowing that controller's permission cannot silently take
// this one's away.
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch

// The only object this operator writes that it does not own -- the
// DatabricksServiceAccounts beside it are its own kind, which it makes and
// deletes. What it writes is one label key of its own, saying it is destroying
// the identities it issued in a namespace this account has stopped naming, and
// it takes that key off again when it has finished. MintLabel and InjectLabel are
// the cluster's, given to every operator serving the namespace at once, and
// nothing here reads or moves them.
//
// patch and not update, which is what confines the write to that one key: an
// update sends a whole Namespace, including a quota controller's annotations and
// another operator's labels, none of which this operator has any business
// sending. No create either, which is why every write here is made only after a
// read found the Namespace -- so a namespace deleted mid-pass is refused rather
// than recreated with nothing in it.
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;patch
// update on the object itself is for its finalizer and nothing else. A
// finalizer is metadata, so there is no narrower verb for it, and the spec this
// permission also reaches is one nobody but the team holding this namespace can
// write anyway.
// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=databricksaccounts,verbs=get;list;watch;update,namespace=system
// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=databricksaccounts/status,verbs=get;update;patch,namespace=system
// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=databricksaccounts/finalizers,verbs=update,namespace=system

// Reconcile builds the clients and reports whether they work.
//
// It never clears working clients on a failure. An account that stops answering
// is reported, and the previous clients stay in place: withdrawing them would
// stop every identity converging, which is a much larger consequence than the
// failure that caused it, and it would not fix anything.
// Only the object being deleted clears them, because then there is nothing left
// to act as.
func (r *DatabricksAccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if req.NamespacedName != r.DatabricksAccountNamespacedName {
		// Not the one this operator was told to use. Its own status says so;
		// see setNotSelected below.
		logger.V(1).Info("Not the DatabricksAccount this operator was told to use",
			"inUse", r.DatabricksAccountNamespacedName)
		return ctrl.Result{}, r.setNotSelected(ctx, req.NamespacedName)
	}

	var databricksAccount dbxv1alpha1.DatabricksAccount
	if err := r.Get(ctx, req.NamespacedName, &databricksAccount); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		// Deleted. There is no account to act in, and saying so is better than
		// acting in one nobody has declared.
		r.AccountInUse.Clear(fmt.Sprintf("DatabricksAccount %s was deleted", r.DatabricksAccountNamespacedName))
		logger.Info("Cleared the Databricks clients: the DatabricksAccount was deleted")
		return ctrl.Result{}, nil
	}

	// Put on before this pass can install any clients, which is what makes it
	// hold everything this operator ever creates: nothing is made in Databricks
	// until the clients are installed, and they are installed below.
	//
	// Not while the object is terminating. Putting it back then would be the
	// operator arguing on a timer with the person who removed it by hand, and
	// the object would never go.
	if databricksAccount.DeletionTimestamp.IsZero() &&
		!controllerutil.ContainsFinalizer(&databricksAccount, dbxv1alpha1.AccountFinalizer) {
		controllerutil.AddFinalizer(&databricksAccount, dbxv1alpha1.AccountFinalizer)
		if err := r.Update(ctx, &databricksAccount); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Read before anything else, so the subject and audience are reported even
	// when the exchange fails -- that is exactly when somebody needs to see them.
	//
	// Nothing is overwritten when the read fails. It used to blank the subject
	// and keep the audience, so half of what the object said went stale and the
	// other half went missing, for the same one failure.
	claims, claimsErr := databricks.ReadTokenClaims(r.OwnToken.OIDCTokenFilepath)
	if claimsErr == nil {
		databricksAccount.Status.Subject = claims.Subject
		if len(claims.Audience) > 0 {
			databricksAccount.Status.Audience = claims.Audience[0]
		}
		// The issuer and what it hashes to, so that somebody looking at a service
		// principal in Databricks can tell whether this cluster made it. Both are
		// derived here rather than left to be derived by hand: the marker is a
		// hash, so the only way to use it is to compare against one somebody
		// computed the same way.
		databricksAccount.Status.Issuer = claims.Issuer
		databricksAccount.Status.ClusterMarker = databricks.ClusterMarker(claims.Issuer)
	} else {
		logger.Error(claimsErr, "Could not read the operator's own projected token, so no identity can be "+
			"issued or converged", "tokenPath", r.OwnToken.OIDCTokenFilepath)
	}
	r.reportPrepared(&databricksAccount, claims, claimsErr)

	records, err := r.records(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	// The deletion, decided from the records and nothing else. A held object
	// carries on through the rest of this pass rather than returning here: the
	// clients installed below are what deletes the service principals those
	// records name, so an operator that stopped at this line would be holding an
	// object nothing could ever drain.
	if !databricksAccount.DeletionTimestamp.IsZero() {
		naming := recordsNaming(records, databricksAccount.Spec.AccountID)
		if naming == 0 {
			return ctrl.Result{}, r.release(ctx, &databricksAccount)
		}
		r.reportHeld(&databricksAccount, naming)
	}

	// Also before the account is contacted, and for a stronger reason than
	// knowability: suspending minting needs nothing from Databricks, and a
	// namespace this account has stopped naming must not go on minting into it
	// for as long as Databricks happens to be unreachable.
	destroyed, err := r.destroyIdentitiesIn(ctx, &databricksAccount, records)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Before the account is contacted for the same reason: it is read entirely
	// out of the cluster, and an unreachable Databricks must not be able to stop
	// this account's holder from seeing who is asking. That is at its most
	// valuable exactly when something is wrong.
	if err := r.reportUnservedRequests(ctx, &databricksAccount); err != nil {
		return ctrl.Result{}, err
	}

	cfg := r.OwnToken.ForAccount(databricksAccount.Spec.Host,
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
	//
	// Read once and asked again at the install, because both are the same
	// question: whether this pass is the one that changes what the operator acts
	// as. Set runs on every pass, whether or not anything moved.
	installed, holding := r.AccountInUse.BuiltFrom()
	installing := !holding || installed != cfg

	if holding && installed != cfg {
		r.AccountInUse.Clear(fmt.Sprintf(
			"DatabricksAccount %s was changed to name Databricks account %s, and that has not "+
				"been verified yet; read its status", r.DatabricksAccountNamespacedName, databricksAccount.Spec.AccountID))
		logger.Info("Cleared the Databricks clients: the DatabricksAccount now names another Databricks "+
			"account, which has not been verified yet", "accountId", databricksAccount.Spec.AccountID)
	}

	clients, err := r.build(cfg)
	if err != nil {
		logger.Error(err, "Could not build the Databricks clients", "accountId", databricksAccount.Spec.AccountID)
		return r.reportReady(ctx, &databricksAccount, metav1.ConditionFalse, reasonInvalidSpec, err.Error())
	}

	if err := r.verify(ctx, clients); err != nil {
		logger.Error(err, "Could not verify the Databricks account",
			"accountId", databricksAccount.Spec.AccountID, "host", databricksAccount.Spec.Host)
		// The operator's own claims being unreadable is worth saying here rather
		// than on their own: a refusal whose cause is a missing token file reads
		// as a policy problem otherwise.
		message := databricks.Reason(err)
		if claimsErr != nil {
			message = fmt.Sprintf("%s (and the operator's own token could not be read: %v)", message, claimsErr)
		}
		failure := outcomeFor(err)
		failure.Message = message
		return r.reportFailure(ctx, &databricksAccount, failure)
	}

	r.AccountInUse.Set(cfg, clients)
	if installing {
		logger.Info("Installed the Databricks clients",
			"accountId", databricksAccount.Spec.AccountID, "host", databricksAccount.Spec.Host)
	}

	// The subject as the status holds it, not as this pass read it. Verification
	// goes on passing for about an hour after the token file becomes unreadable
	// -- the SDK holds the access token it already exchanged -- so this line is
	// reached with the claims at their zero value, and rendering from them puts
	// "as " on a Ready account. The status keeps the last one read for the same
	// reason it is not blanked above: the value is not wrong, it was not read.
	result, err := r.reportReady(ctx, &databricksAccount, metav1.ConditionTrue, reasonAccountReady,
		fmt.Sprintf("acting in account %s as %s", databricksAccount.Spec.AccountID, databricksAccount.Status.Subject))
	if !destroyed {
		// The settled interval is long because a working account has nobody
		// waiting on it. A destruction that has not finished has somebody waiting
		// on it, and what would finish it -- a namespace this operator could not
		// claim, a record whose service principal is still there -- raises no
		// event here either.
		result.RequeueAfter = databricksAccountRetryAfterAwaited
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
// its copy. And another operator's identities are not this one's to read: they
// are in another namespace, in another account, and counting them here would
// hold this object over something this operator never issued.
func (r *DatabricksAccountReconciler) records(ctx context.Context) (
	[]dbxv1alpha1.IssuedDatabricksServicePrincipal, error) {
	var identities dbxv1alpha1.IssuedDatabricksServicePrincipalList
	if err := r.List(ctx, &identities, client.InNamespace(r.DatabricksAccountNamespacedName.Namespace)); err != nil {
		return nil, err
	}
	return identities.Items, nil
}

// recordsNaming counts the records that were acted on in one Databricks
// account.
//
// A record that has never sent a create names no account and is not counted:
// nothing can exist for it, so nothing is held by it. That is what makes a typo
// at install harmless -- an operator that never reached Databricks wrote no
// account anywhere, and the object it was declared in deletes freely.
func recordsNaming(records []dbxv1alpha1.IssuedDatabricksServicePrincipal, accountID string) int {
	var naming int
	for i := range records {
		if records[i].Status.AccountID == accountID {
			naming++
		}
	}
	return naming
}

// CheckSelectedAccount refuses to let an operator serve records that were acted
// on in a Databricks account other than the one it was told to use.
//
// It is the third of the three doors that make the account fixed for an
// operator's lifetime, and the only one admission cannot hold: spec.accountId
// is immutable and AccountFinalizer keeps the object from being replaced, but
// --databricks-account is edited on a Deployment, where no CEL rule and no
// webhook can see it. So it is asked once, at startup, before anything is
// reconciled.
//
// Refusing is not serving. A partly-serving operator in the wrong account is
// exactly what the three doors exist to prevent, and every path downstream of
// here is written on the assumption that an id it reads is one it can act on.
//
// No DatabricksAccount is not a refusal: an operator installed before its
// account is declared runs and says so on every object. Neither is a record
// that names no account, which has sent no create and so has nothing anywhere
// to be wrong about.
func CheckSelectedAccount(ctx context.Context, reader client.Reader, selected types.NamespacedName) error {
	var databricksAccount dbxv1alpha1.DatabricksAccount
	if err := reader.Get(ctx, selected, &databricksAccount); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("reading DatabricksAccount %s: %w", selected, err)
	}

	var records dbxv1alpha1.IssuedDatabricksServicePrincipalList
	if err := reader.List(ctx, &records, client.InNamespace(selected.Namespace)); err != nil {
		return fmt.Errorf("reading the records in %s: %w", selected.Namespace, err)
	}

	var named []dbxv1alpha1.IssuedDatabricksServicePrincipal
	for i := range records.Items {
		made := records.Items[i].Status.AccountID
		if made != "" && made != databricksAccount.Spec.AccountID {
			named = append(named, records.Items[i])
		}
	}
	if len(named) == 0 {
		return nil
	}

	slices.SortFunc(named, func(a, b dbxv1alpha1.IssuedDatabricksServicePrincipal) int {
		return strings.Compare(a.Name, b.Name)
	})
	others := ""
	if len(named) > 1 {
		others = fmt.Sprintf(", and %d other record(s) in %s do the same", len(named)-1, selected.Namespace)
	}
	return fmt.Errorf("record %s/%s names Databricks account %s%s, and this operator was told to act in "+
		"%s by --databricks-account naming %s. A service principal id means nothing outside the account "+
		"it was made in, so serving these from here would delete something this operator never made, or "+
		"read the 404 that means \"not here\" as the one that means \"gone\". Point --databricks-account "+
		"at the DatabricksAccount naming %s, or install a second operator, in a namespace of its own, for "+
		"%s",
		named[0].Namespace, named[0].Name, named[0].Status.AccountID, others,
		databricksAccount.Spec.AccountID, selected,
		named[0].Status.AccountID, databricksAccount.Spec.AccountID)
}

// release lets a DatabricksAccount go, and stops acting in the account it
// named.
//
// The clients are withdrawn here rather than on the pass that finds the object
// gone, because between those two this operator would be creating identities in
// an account nobody declares any more.
func (r *DatabricksAccountReconciler) release(ctx context.Context,
	databricksAccount *dbxv1alpha1.DatabricksAccount) error {
	r.AccountInUse.Clear(fmt.Sprintf("DatabricksAccount %s was deleted", r.DatabricksAccountNamespacedName))
	log.FromContext(ctx).Info("Cleared the Databricks clients: the DatabricksAccount is being deleted and no "+
		"record names its Databricks account", "accountId", databricksAccount.Spec.AccountID)
	controllerutil.RemoveFinalizer(databricksAccount, dbxv1alpha1.AccountFinalizer)
	return client.IgnoreNotFound(r.Update(ctx, databricksAccount))
}

// reportHeld says why this object is not going, on the object somebody deleted.
//
// Only ever False. The moment it would be true the finalizer is released and
// the object is gone, and a condition on a deleted object is one nobody reads.
func (r *DatabricksAccountReconciler) reportHeld(databricksAccount *dbxv1alpha1.DatabricksAccount, naming int) {
	setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionRecordsReleased,
		metav1.ConditionFalse, reasonRecordsRemain,
		fmt.Sprintf("%d record(s) in %s still name Databricks account %s, so this object is held: "+
			"letting it go and creating another one naming a different account is the edit "+
			"accountId's immutability refuses, and an id means nothing outside the account it "+
			"was made in. Deleting a record destroys the service principal it names. An account "+
			"that can no longer be reached never drains, so whoever has looked at what is left "+
			"in Databricks removes the finalizer.",
			naming, r.DatabricksAccountNamespacedName.Namespace, databricksAccount.Spec.AccountID))
}

// databricksAccountServes reports whether one namespace is this operator's to
// act in, and whether this operator has been told anything at all.
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
// Not served is also what destroys an identity that exists, and there an absent
// answer must not be acted on at all: an object that has been deleted, or that
// this pass has not read yet, is this operator knowing nothing, and knowing
// nothing is never a reason to destroy anything.
//
// One function rather than a method on each controller that asks. Both the
// DatabricksServiceAccount controller and the record's decide what to do from
// this answer, and two spellings of it are two chances for one of them to read
// a namespace as served that the other does not -- which is an identity minted
// on one side and stripped of its trust on the other, at the same time, for
// ever.
func databricksAccountServes(ctx context.Context, reader client.Reader,
	databricksAccountNamespacedName types.NamespacedName, namespace string) (
	declared, served bool, err error) {
	var databricksAccount dbxv1alpha1.DatabricksAccount
	switch err := reader.Get(ctx, databricksAccountNamespacedName, &databricksAccount); {
	case apierrors.IsNotFound(err):
		return false, false, nil
	case err != nil:
		return false, false, err
	}
	return true, databricksAccount.Spec.Serves(namespace), nil
}

// destroyIdentitiesIn suspends minting in every namespace this operator issued
// something in and this account no longer names, and reports whether the
// destruction has finished.
//
// The set is derived from the records rather than remembered from a previous
// spec. Nothing here holds the list as it was before the edit, and it does not
// need to: a namespace this operator issued an identity in and this account does
// not name is one it is no longer entitled to spend on, whether the edit was a
// moment ago or before this process started. A remembered list would also have
// to survive a restart, and the one thing that must not depend on this
// operator's memory is which identities are still trusted.
//
// Nothing is destroyed here. What this does is claim the namespace and count
// what is left in it; the destroying is on the records, whose own controller
// deletes each one and whose finalizer deletes the service principal. So this
// suspends minting first and reports afterwards, and between those two it does
// nothing at all.
//
// The suspension goes as soon as there is nothing left to destroy. It is this
// operator saying "not while I am working", not a verdict on the namespace: the
// namespace stays off this account's list, so this operator mints nothing there
// whatever the key says, and holding it any longer would leave every other
// operator serving that namespace unable to mint for a destruction that had
// finished.
func (r *DatabricksAccountReconciler) destroyIdentitiesIn(ctx context.Context,
	databricksAccount *dbxv1alpha1.DatabricksAccount,
	records []dbxv1alpha1.IssuedDatabricksServicePrincipal) (bool, error) {
	// Every namespace out of scope, and how many identities this account still
	// has in it. A record that exists is an identity that exists: a record on its
	// way out is held by its finalizer until Databricks has confirmed the service
	// principal is gone, so counting records is counting what is left in the
	// account rather than what has been asked for.
	holding := map[string]int{}
	for i := range records {
		namespace := records[i].Spec.ServiceAccount.Namespace
		if databricksAccount.Spec.Serves(namespace) {
			continue
		}
		holding[namespace]++
	}

	// Read from the cluster, because the records no longer say it. A namespace
	// whose last record is gone has left the map above, and it is exactly the one
	// whose claim has to come off -- so the claims this operator holds are found
	// where they are written. Entered with a count of zero, which is what the
	// loop below reads as "nothing left here".
	claimed, err := r.namespacesClaimedForDestruction(ctx)
	if err != nil {
		return false, err
	}
	for _, namespace := range claimed {
		if _, seen := holding[namespace]; !seen {
			holding[namespace] = 0
		}
	}

	if len(holding) == 0 {
		setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionIdentitiesDestroyed,
			metav1.ConditionTrue, reasonIdentitiesDestroyed,
			"every namespace this operator issued an identity in is one this account names")
		return true, nil
	}

	namespaces := make([]string, 0, len(holding))
	for namespace := range holding {
		namespaces = append(namespaces, namespace)
	}
	slices.Sort(namespaces)

	// Taken before anything is destroyed and given back once nothing is left,
	// one namespace at a time.
	var unclaimed, stillHeld, remaining []string
	for _, namespace := range namespaces {
		if holding[namespace] > 0 {
			remaining = append(remaining, fmt.Sprintf("%s (%d)", namespace, holding[namespace]))
			if err := r.acquireDestructionClaim(ctx, namespace); err != nil {
				unclaimed = append(unclaimed, fmt.Sprintf("%s (%v)", namespace, err))
			}
			continue
		}
		if err := r.releaseDestructionClaim(ctx, namespace); err != nil {
			stillHeld = append(stillHeld, fmt.Sprintf("%s (%v)", namespace, err))
		}
	}

	if len(unclaimed) == 0 && len(stillHeld) == 0 && len(remaining) == 0 {
		setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionIdentitiesDestroyed,
			metav1.ConditionTrue, reasonIdentitiesDestroyed,
			fmt.Sprintf("this account no longer serves %s: every identity it issued there is "+
				"destroyed, and minting is no longer suspended, so any other operator serving "+
				"those namespaces goes on as before. Naming a namespace here again issues new "+
				"service principals, with new client ids and no grants.",
				strings.Join(namespaces, ", ")))
		return true, nil
	}

	var said []string
	if len(unclaimed) > 0 {
		// Named separately from the count below because they are fixed by
		// different people: this one by whoever can write a Namespace, or by
		// whoever runs the operator named in the message, and neither is
		// necessarily whoever holds this account.
		said = append(said, fmt.Sprintf(
			"this operator does not hold %s on %s, so it has destroyed nothing there: it will "+
				"not destroy identities in a namespace that can still mint them",
			dbxv1alpha1.DestroyingIdentitiesLabel, strings.Join(unclaimed, ", ")))
	}
	if len(stillHeld) > 0 {
		// Nothing is left in these and only the suspension remains, which is why
		// this is said apart from everything above: nothing is waiting on
		// Databricks, and what it costs is every other operator serving these
		// namespaces, whose minting does not resume until the key comes off.
		said = append(said, fmt.Sprintf(
			"there is nothing left to destroy in %s and this operator could not take %s off "+
				"them again, so no operator is minting there",
			strings.Join(stillHeld, ", "), dbxv1alpha1.DestroyingIdentitiesLabel))
	}
	if len(remaining) > 0 {
		said = append(said, fmt.Sprintf(
			"identities issued in %s are still in Databricks; their records say why under "+
				"Ready, in namespace %s",
			strings.Join(remaining, ", "), r.DatabricksAccountNamespacedName.Namespace))
	}
	setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionIdentitiesDestroyed,
		metav1.ConditionFalse, reasonDestroyingIdentities,
		fmt.Sprintf("%s. Every count here is an identity this account still has in a namespace "+
			"it no longer names, which is what this list promises there are none of.",
			strings.Join(said, ". ")))
	return false, nil
}

// reportUnservedRequests says which requests this operator can see and is not
// serving: a ServiceAccount asking it for an identity, in a namespace this
// account does not name.
//
// It reports and does nothing. Serving one is an edit to spec.namespaces, made
// by a person who has decided to spend an account admin credential there, and an
// operator that took an annotation as that decision would be letting anybody who
// can write a ServiceAccount make it.
//
// Cluster-wide, and it has to be. The namespaces worth reporting are exactly the
// ones this account does not name, so a listing narrowed to what it serves would
// answer only the question nobody asks.
//
// Read through RequestsFor rather than by matching keys, because a cluster holds
// more than one operator and a request addressed to another one is not a request
// this account is refusing. Its Refused keys are not counted either: a value that
// does not parse names no operator, so counting one here would put a typo
// somebody else has to fix on this account's status as a namespace it could
// serve.
func (r *DatabricksAccountReconciler) reportUnservedRequests(ctx context.Context,
	databricksAccount *dbxv1alpha1.DatabricksAccount) error {
	var serviceAccounts corev1.ServiceAccountList
	if err := r.List(ctx, &serviceAccounts); err != nil {
		return err
	}

	// Counted per namespace rather than listed one by one, because the edit this
	// informs is per namespace: spec.namespaces takes a namespace or does not,
	// and naming forty ServiceAccounts would bury the four names somebody acts
	// on.
	asking := map[string]int{}
	for i := range serviceAccounts.Items {
		serviceAccount := &serviceAccounts.Items[i]
		if databricksAccount.Spec.Serves(serviceAccount.Namespace) {
			continue
		}
		requested := dbxv1alpha1.RequestsFor(serviceAccount.Annotations, r.DatabricksAccountNamespacedName)
		if len(requested.Understood) == 0 {
			continue
		}
		asking[serviceAccount.Namespace]++
	}

	if len(asking) == 0 {
		setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionRequestsServed,
			metav1.ConditionTrue, reasonRequestsServed,
			"every ServiceAccount asking this operator for an identity is in a namespace "+
				"spec.namespaces names. One in a namespace it does not name is refused here and "+
				"nowhere else: nothing is written in the namespace it came from, so this is where "+
				"such a request would be counted.")
		return nil
	}

	namespaces := make([]string, 0, len(asking))
	for namespace := range asking {
		namespaces = append(namespaces, namespace)
	}
	slices.Sort(namespaces)

	counted := make([]string, 0, len(namespaces))
	for _, namespace := range namespaces {
		counted = append(counted, fmt.Sprintf("%s (%d)", namespace, asking[namespace]))
	}

	setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionRequestsServed,
		metav1.ConditionFalse, reasonRequestsNotServed,
		fmt.Sprintf("ServiceAccounts in %s ask this operator for an identity and are in namespaces "+
			"spec.namespaces does not name. Nothing "+
			"is being made for them and nothing in their own namespace says so -- there is no "+
			"DatabricksServiceAccount to carry a condition and no event on anything -- so this line "+
			"is the whole of what anybody can read about it. Either name the namespace in "+
			"spec.namespaces, which issues new service principals with new client ids and no "+
			"grants, or have whoever wrote the %s annotation take it off. None of these was ever "+
			"issued, which is what tells them from the ones %s counts: those exist and are being "+
			"destroyed.",
			strings.Join(counted, ", "), dbxv1alpha1.ServicePrincipalAnnotation,
			conditionIdentitiesDestroyed))
	return nil
}

// namespacesClaimedForDestruction is every namespace this operator has suspended
// minting in, read from the cluster.
//
// It is what makes the suspension liftable at all. The claim is taken while
// records are being destroyed and the records are what say which namespaces
// those are -- so the moment the last one goes, nothing left anywhere says this
// operator is holding a namespace, and the key would stay on it for ever with
// only this operator able to lift it. A namespace named again after the edit
// leaves the same way: it is served, so no record puts it in the set, and the
// claim taken while it was not served is still there.
//
// Selected on the operator's namespace and then narrowed on the account, because
// only the first half of the holder's name is in a label. Two operators sharing
// one namespace both match the selector, and what tells them apart is a
// comparison on what came back -- so the account name never has to be short
// enough for a label value.
func (r *DatabricksAccountReconciler) namespacesClaimedForDestruction(ctx context.Context) ([]string, error) {
	var namespaces corev1.NamespaceList
	if err := r.List(ctx, &namespaces, client.MatchingLabels{
		dbxv1alpha1.DestroyingIdentitiesLabel: r.DatabricksAccountNamespacedName.Namespace,
	}); err != nil {
		return nil, err
	}
	claimed := make([]string, 0, len(namespaces.Items))
	for i := range namespaces.Items {
		if destructionHolderOf(&namespaces.Items[i]) != r.DatabricksAccountNamespacedName {
			continue
		}
		claimed = append(claimed, namespaces.Items[i].Name)
	}
	return claimed, nil
}

// acquireDestructionClaim claims a namespace for this operator's destruction,
// and says again on every pass that the claim is still wanted.
//
// The claim is a label key and an annotation key, and server-side apply is the
// whole of the mutual exclusion. Both maps have their keys owned one at a time
// -- the same mechanism status.identities already relies on -- so an operator
// applying either of them over somebody else's value is refused a conflict, and
// an apply refused on one key writes neither. Being refused is what it means to
// lose, and there is no register to keep and nothing to unwind after a crash.
//
// Two operators in one namespace agree on the label and differ on the
// annotation, which is what the annotation is doing in the claim rather than
// only in the message: without it their claims would be indistinguishable and
// each would read the other's as its own. Measured against a real API server,
// the second one is refused one conflict and it is on the annotation -- a label
// value applied identically is co-owned rather than refused, so the label alone
// would exclude nobody there.
//
// Never forced except over a claim nobody has refreshed. Forcing takes the field
// whatever it says, which would leave nothing here excluding anything.
//
// The timestamp is rewritten every pass because managedFields[].time does not
// move when the applied content is identical -- measured against a live cluster
// -- so an unchanging annotation would say when the claim was made and never
// that its holder is still running.
func (r *DatabricksAccountReconciler) acquireDestructionClaim(ctx context.Context, name string) error {
	logger := log.FromContext(ctx)

	// Read before the apply because this operator holds no create on Namespaces:
	// an apply naming one that is not there is a create, and a namespace that is
	// gone is one nothing can be minted into and so one with nothing to claim.
	// Its records stay, and each is destroyed on its own controller's pass.
	holder, exists, err := destructionHeldBy(ctx, r.Client, name)
	switch {
	case err != nil:
		return err
	case !exists:
		return nil
	}

	// Whether this pass is taking the claim or saying again that it still wants
	// one it already holds. The apply is the same either way, so without this the
	// line below would repeat for as long as the removal takes.
	taking := holder != r.DatabricksAccountNamespacedName

	err = r.Apply(ctx, destructionClaimFor(name, r.DatabricksAccountNamespacedName, time.Now()), r.owner())
	if !apierrors.IsConflict(err) {
		switch {
		case err != nil:
			logger.Error(err, "Could not claim a namespace to destroy the identities in it",
				"tenantNamespace", name)
		case taking:
			logger.Info("Claimed a namespace to destroy the identities in it", "tenantNamespace", name)
		}
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
	holder = destructionHolderOf(&namespace)
	said := namespace.Annotations[dbxv1alpha1.DestroyingIdentitiesSinceAnnotation]
	logger.V(1).Info("Another operator holds the destruction claim",
		"tenantNamespace", name, "holder", holder, "since", said)

	since, unreadable := time.Parse(time.RFC3339, said)
	if unreadable != nil {
		// A claim whose age cannot be read is not evidence that its holder is
		// gone, and taking it on that would be this operator inventing the one
		// fact that says it may. Reported instead, every pass, which is what
		// puts it in front of somebody.
		return fmt.Errorf("held by %s, which has not said when: %q", holder, said)
	}
	age := time.Since(since)
	if age < destructionClaimStale {
		return fmt.Errorf("held by %s, last seen %s ago", holder, age.Truncate(time.Second))
	}

	// The one force there is. Its holder stopped saying it was there long enough
	// ago that waiting for it is waiting for nothing, and the operator it is
	// taken from finds out the same way anybody else does: its next apply is
	// refused, because the field is no longer its own.
	logger.Info("Taking the destruction claim from an operator that stopped refreshing it",
		"tenantNamespace", name, "holder", holder, "lastSeen", age.Truncate(time.Second))
	return r.Apply(ctx, destructionClaimFor(name, r.DatabricksAccountNamespacedName, time.Now()),
		r.owner(), client.ForceOwnership)
}

// releaseDestructionClaim gives a namespace back, by applying the same object
// without the key: an apply keeps only what it sends.
//
// Only by the operator that holds it. An operator that was never the holder, or
// that was taken over by another, has nothing here to give back, and sending
// this anyway would be one operator ending another's destruction halfway
// through.
func (r *DatabricksAccountReconciler) releaseDestructionClaim(ctx context.Context, name string) error {
	var namespace corev1.Namespace
	switch err := r.Get(ctx, types.NamespacedName{Name: name}, &namespace); {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return err
	}
	if destructionHolderOf(&namespace) != r.DatabricksAccountNamespacedName {
		return nil
	}

	logger := log.FromContext(ctx)
	if err := r.Apply(ctx, corev1ac.Namespace(name), r.owner()); err != nil {
		logger.Error(err, "Could not release the claim on a namespace whose identities are all destroyed",
			"tenantNamespace", name)
		return err
	}
	logger.Info("Released the claim on a namespace: nothing of this account's is left there",
		"tenantNamespace", name)
	return nil
}

// destructionHeldBy is the operator that has suspended minting in this
// namespace, and whether there is a namespace at all.
//
// The two are separate answers because a namespace that is gone is not a
// namespace nobody holds. Nothing can be minted into one that does not exist, so
// there is nothing for a claim to protect there and a destruction has nothing to
// wait for -- while a namespace that is there and unclaimed is one this operator
// must claim before it destroys anything.
func destructionHeldBy(ctx context.Context, reader client.Reader, name string) (
	holder types.NamespacedName, exists bool, err error) {
	var namespace corev1.Namespace
	switch err := reader.Get(ctx, types.NamespacedName{Name: name}, &namespace); {
	case apierrors.IsNotFound(err):
		return types.NamespacedName{}, false, nil
	case err != nil:
		return types.NamespacedName{}, false, err
	}
	return destructionHolderOf(&namespace), true, nil
}

// destructionHolderOf reads the holder off a Namespace already in hand, and is
// the zero value for one nobody holds.
//
// The label decides whether there is a holder at all, because it is the half
// namespaceMints reads: an account annotation on a namespace carrying no label
// suspends nothing, and naming a holder for it would name somebody nothing is
// waiting on. The annotation only completes the name.
func destructionHolderOf(namespace *corev1.Namespace) types.NamespacedName {
	held := namespace.Labels[dbxv1alpha1.DestroyingIdentitiesLabel]
	if held == "" {
		return types.NamespacedName{}
	}
	return types.NamespacedName{
		Namespace: held,
		Name:      namespace.Annotations[dbxv1alpha1.DestroyingIdentitiesAccountAnnotation],
	}
}

// destructionClaimFor is the claim as this operator sends it: its own name split
// across a label and an annotation, and the time it is saying so.
//
// One apply carries all three, which is what makes the halves arrive and leave
// together. Sent separately there would be a pass in which a namespace carried a
// label naming a namespace and no account, and every reader of it would have to
// have an answer for that.
func destructionClaimFor(namespace string, databricksAccountNamespacedName types.NamespacedName,
	at time.Time) *corev1ac.NamespaceApplyConfiguration {
	return corev1ac.Namespace(namespace).
		WithLabels(map[string]string{
			dbxv1alpha1.DestroyingIdentitiesLabel: databricksAccountNamespacedName.Namespace,
		}).
		WithAnnotations(map[string]string{
			dbxv1alpha1.DestroyingIdentitiesAccountAnnotation: databricksAccountNamespacedName.Name,
			dbxv1alpha1.DestroyingIdentitiesSinceAnnotation:   at.UTC().Format(time.RFC3339),
		})
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
func (r *DatabricksAccountReconciler) setNotSelected(ctx context.Context,
	databricksAccountNamespacedName types.NamespacedName) error {
	var databricksAccount dbxv1alpha1.DatabricksAccount
	if err := r.Get(ctx, databricksAccountNamespacedName, &databricksAccount); err != nil {
		return client.IgnoreNotFound(err)
	}

	// An object this operator does not use carries nothing of this operator's.
	// Moving --databricks-account is meant to be staged -- write the new object,
	// see it accepted, then move the flag -- and a finalizer left on the one it
	// moved off would make deleting that one hang on a controller that no longer
	// looks at it.
	if controllerutil.ContainsFinalizer(&databricksAccount, dbxv1alpha1.AccountFinalizer) {
		controllerutil.RemoveFinalizer(&databricksAccount, dbxv1alpha1.AccountFinalizer)
		if err := client.IgnoreNotFound(r.Update(ctx, &databricksAccount)); err != nil {
			return err
		}
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
// The update is what tells it from reportHeld and reportPrepared, which set
// their condition on the object in memory and return nothing. This is what
// carries them to the API server.
func (r *DatabricksAccountReconciler) reportReady(ctx context.Context,
	databricksAccount *dbxv1alpha1.DatabricksAccount,
	status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {
	setCondition(&databricksAccount.Status.Conditions, databricksAccount.Generation, conditionReady,
		status, reason, message)
	return ctrl.Result{
		RequeueAfter: retryAfterFor(status, databricksAccountRetryAfterAwaited, databricksAccountRetryAfterSettled),
	}, client.IgnoreNotFound(r.Status().Update(ctx, databricksAccount))
}

// reportFailure ends the pass on something Databricks said: reportReady, plus
// the one thing outcomeFor decides that a condition cannot carry -- whether this
// failure goes back to the workqueue as an error, which is what outcome.Err is
// about.
//
// The condition is written either way, and first, so what a person reads does
// not depend on which kind of failure this was.
func (r *DatabricksAccountReconciler) reportFailure(ctx context.Context,
	databricksAccount *dbxv1alpha1.DatabricksAccount, failure outcome) (ctrl.Result, error) {
	result, err := r.reportReady(ctx, databricksAccount, failure.Status, failure.Reason, failure.Message)
	if err != nil {
		return ctrl.Result{}, err
	}
	if failure.Err != nil {
		return ctrl.Result{}, failure.Err
	}
	return result, nil
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
		// Every ServiceAccount event, mapped onto this one object, because an
		// annotation written in a namespace this account does not name changes
		// no record, no DatabricksServiceAccount and no Namespace -- so nothing
		// else here would ever wake for it, and the count would be as old as the
		// last pass on the account's own interval.
		//
		// That staleness is the failure worth spending a watch on: the count is
		// read to decide whether to put a namespace back on spec.namespaces, and
		// a count minutes behind is one somebody edits the list wrongly on.
		//
		// The manager's cache already holds ServiceAccounts for the
		// DatabricksServiceAccount controller, so this is an event handler and
		// not a second informer, and the workqueue collapses a cluster's worth of
		// events onto the single key this maps to.
		Watches(&corev1.ServiceAccount{}, handler.EnqueueRequestsFromMapFunc(
			func(context.Context, client.Object) []reconcile.Request {
				return []reconcile.Request{{NamespacedName: r.DatabricksAccountNamespacedName}}
			})).
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

// databricksAccountChanged selects the writes on this object worth waking every
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
// Which namespaces it serves: taking one off that list destroys the identities
// there, and the records in it are where that is carried out. Nothing else says
// so -- the edit changes no record, no ServiceAccount and no Namespace -- so
// without this they stay until each record comes round on its own interval,
// which for a settled identity is ten minutes. Putting a namespace back has the
// same shape and the same wait.
//
// Everything else is skipped, and that is what keeps this from being a loop:
// every reconcile of an account writes its status, and waking every record on
// each of those would put the whole catalogue back in the queue once a minute
// for no reason.
func databricksAccountChanged(databricksAccountNamespacedName types.NamespacedName) predicate.Predicate {
	ready := func(o client.Object) bool {
		databricksAccount, ok := o.(*dbxv1alpha1.DatabricksAccount)
		if !ok || client.ObjectKeyFromObject(databricksAccount) != databricksAccountNamespacedName {
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
		if !ok || client.ObjectKeyFromObject(databricksAccount) != databricksAccountNamespacedName {
			return nil
		}
		return slices.Sorted(slices.Values(databricksAccount.Spec.Namespaces))
	}
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return ready(e.Object) },
		DeleteFunc: func(e event.DeleteEvent) bool {
			return client.ObjectKeyFromObject(e.Object) == databricksAccountNamespacedName
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return ready(e.ObjectOld) != ready(e.ObjectNew) ||
				!slices.Equal(served(e.ObjectOld), served(e.ObjectNew))
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// enqueueOnDatabricksAccountChange wakes every recorded identity when the
// selected DatabricksAccount says something new about them: that it can be
// acted in, or that it no longer serves the namespace one of them is in.
//
// Without it, an operator whose account was just fixed stays visibly broken, and
// the identities in a namespace taken out of scope go on existing, for as long
// as each object's own retry interval -- ten minutes for a settled identity.
// Nothing else reports either change: one happened in Databricks and the other
// is an edit to a field no record watches.
//
// Every record is woken and not only the ones affected, because which ones those
// are cannot be worked out from the event: the list before the edit is not
// available here, and a record whose namespace was just removed is exactly the
// one the new list does not name.
func enqueueOnDatabricksAccountChange(mgr ctrl.Manager,
	databricksAccountNamespacedName types.NamespacedName, list client.ObjectList,
	keys func(client.ObjectList) []reconcile.Request) (
	client.Object, handler.EventHandler, builder.WatchesOption) {
	mapFunc := func(ctx context.Context, _ client.Object) []reconcile.Request {
		items := list.DeepCopyObject().(client.ObjectList)
		if err := mgr.GetClient().List(ctx, items); err != nil {
			// Dropping the wake-up costs a wait, not correctness: every object
			// comes back on its own interval regardless.
			log.FromContext(ctx).Error(err, "Could not wake the records this DatabricksAccount changed the answer for")
			return nil
		}
		return keys(items)
	}
	return &dbxv1alpha1.DatabricksAccount{},
		handler.EnqueueRequestsFromMapFunc(mapFunc),
		builder.WithPredicates(databricksAccountChanged(databricksAccountNamespacedName))
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
