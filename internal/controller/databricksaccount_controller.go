/*
Copyright 2026.

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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
)

// DatabricksAccountReconciler turns one DatabricksAccount into the clients the
// record's controller uses. It is the only other one that reaches Databricks at
// all: the projection's controller talks to nothing outside the cluster.
type DatabricksAccountReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Account is the object this operator was told to use, by the
	// --databricks-account flag and its own namespace. Anything else is left
	// alone rather than adopted: which account the operator acts in is a
	// decision made when it is deployed, not by whoever can create an object.
	Account types.NamespacedName

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

// The identities are read to count the ones made in another account. Read only:
// this controller writes none of them.
// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=databricksserviceprincipals,verbs=get;list;watch
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
	if req.NamespacedName != r.Account {
		// Not the one this operator was told to use. Its own status says so;
		// see setNotSelected below.
		return ctrl.Result{}, r.setNotSelected(ctx, req.NamespacedName)
	}

	var account dbxv1alpha1.DatabricksAccount
	if err := r.Get(ctx, req.NamespacedName, &account); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		// Deleted. There is no account to act in, and saying so is better than
		// acting in one nobody has declared.
		r.Holder.Clear(fmt.Sprintf("DatabricksAccount %s was deleted", r.Account))
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
		account.Status.Subject = claims.Subject
		if len(claims.Audience) > 0 {
			account.Status.Audience = claims.Audience[0]
		}
	}
	r.reportPrepared(&account, claims, claimsErr)

	// Written before anything is attempted, because it is knowable without
	// attempting anything: it compares what this operator recorded against what
	// this object names. Computed only after a successful verification, a failed
	// switch left the previous pass's "every identity was made in this account"
	// standing under a spec naming another -- a present-tense claim, about the
	// wrong account, on the object whose edit caused it.
	if err := r.reportAgreement(ctx, &account); err != nil {
		return ctrl.Result{}, err
	}

	cfg := r.Runtime.ForAccount(account.Spec.Host, account.Spec.AccountID, account.Spec.ClientID)

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
				"been verified yet; read its status", r.Account, account.Spec.AccountID))
	}

	clients, err := r.build(cfg)
	if err != nil {
		return r.report(ctx, &account, metav1.ConditionFalse, reasonInvalidSpec, err.Error())
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
		return r.report(ctx, &account, result.Status, result.Reason, message)
	}

	r.Holder.Set(cfg, clients)

	return r.report(ctx, &account, metav1.ConditionTrue, reasonAccountReady,
		fmt.Sprintf("acting in account %s as %s", account.Spec.AccountID, claims.Subject))
}

// reportAgreement counts the identities this operator issued that were made in
// some other Databricks account.
//
// Read from the cache rather than from Databricks: what is compared is what this
// operator recorded, and the count is only a report -- a failure to produce it
// makes the line wrong, never the operator unusable.
//
// Counted from the records, in this operator's own namespace, and both halves of
// that matter. The records are where an account id is written; the projections
// in tenants' namespaces are copies of them, and a copy is missing exactly when
// this question is most worth asking -- an identity whose namespace was torn
// down, or whose namespace this operator no longer serves, still has its record
// and no longer has its projection. And another operator's identities are not
// this one's to count: they are in another namespace, in another account, and
// reporting them here would say this operator has stranded something it never
// issued.
func (r *DatabricksAccountReconciler) reportAgreement(ctx context.Context,
	account *dbxv1alpha1.DatabricksAccount) error {
	var identities dbxv1alpha1.IssuedDatabricksServicePrincipalList
	if err := r.List(ctx, &identities, client.InNamespace(r.Account.Namespace)); err != nil {
		return err
	}

	elsewhere := map[string]int{}
	for i := range identities.Items {
		made := identities.Items[i].Status.AccountID
		if made == "" || made == account.Spec.AccountID {
			continue
		}
		elsewhere[made]++
	}

	if len(elsewhere) == 0 {
		setCondition(&account.Status.Conditions, account.Generation, conditionAccountsAgree,
			metav1.ConditionTrue, reasonHere,
			"every identity this operator issued was made in this account")
		return nil
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

	setCondition(&account.Status.Conditions, account.Generation, conditionAccountsAgree,
		metav1.ConditionFalse, reasonElsewhere,
		fmt.Sprintf("%d identit(ies) this operator issued were made in another Databricks account "+
			"(%s). Nothing is done for them and nothing about them is known from here; their "+
			"workloads are unaffected. Pointing this object back at the other account resumes "+
			"them, and does the same to the ones made here.",
			total, strings.Join(parts, ", ")))
	return nil
}

// setNotSelected marks an account this operator was not told to use.
//
// Silence would be worse than anything it costs: somebody who wrote a second
// object and sees no status has no way to tell it from an operator that is not
// running, and the object that is in use is named here so the difference is one
// kubectl away.
func (r *DatabricksAccountReconciler) setNotSelected(ctx context.Context, name types.NamespacedName) error {
	var account dbxv1alpha1.DatabricksAccount
	if err := r.Get(ctx, name, &account); err != nil {
		return client.IgnoreNotFound(err)
	}
	setCondition(&account.Status.Conditions, account.Generation, conditionReady,
		metav1.ConditionFalse, reasonNotSelected,
		fmt.Sprintf("this operator uses %s; change --databricks-account to select this one instead", r.Account))
	return client.IgnoreNotFound(r.Status().Update(ctx, &account))
}

func (r *DatabricksAccountReconciler) report(ctx context.Context, account *dbxv1alpha1.DatabricksAccount,
	status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {
	setCondition(&account.Status.Conditions, account.Generation, conditionReady, status, reason, message)
	return ctrl.Result{
		RequeueAfter: retryAfterFor(status, accountRetryAfterAwaited, accountRetryAfterSettled),
	}, client.IgnoreNotFound(r.Status().Update(ctx, account))
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
	account := clients.Account()
	if account == nil {
		return fmt.Errorf("no account client was built")
	}
	if _, err := account.Workspaces.List(ctx); err != nil {
		return fmt.Errorf("listing the account's workspaces: %w", err)
	}
	return nil
}

// accountBecameUsable selects the transitions worth waking the other
// controller for.
//
// Every reconcile of an account writes its status, and most writes change
// nothing that matters to an identity. Only the crossing into or out of Ready
// does: on the way in, everything waiting on NotConfigured can now be resolved,
// and waiting out each object's own retry interval would leave the operator
// looking broken for a minute after it was fixed.
func accountBecameUsable(selected types.NamespacedName) predicate.Predicate {
	ready := func(o client.Object) bool {
		account, ok := o.(*dbxv1alpha1.DatabricksAccount)
		if !ok || client.ObjectKeyFromObject(account) != selected {
			return false
		}
		for _, c := range account.Status.Conditions {
			if c.Type == conditionReady {
				return c.Status == metav1.ConditionTrue
			}
		}
		return false
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return ready(e.Object) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return client.ObjectKeyFromObject(e.Object) == selected },
		UpdateFunc:  func(e event.UpdateEvent) bool { return ready(e.ObjectOld) != ready(e.ObjectNew) },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// enqueueOnAccountReady wakes every recorded identity when the selected
// DatabricksAccount crosses into or out of usable.
//
// Without it, an operator whose account was just fixed stays visibly broken for
// as long as each object's own retry interval, because nothing else reports the
// change: what was repaired happened in Databricks, which raises no event here.
//
// The predicate is what keeps this from being a loop. Every reconcile of an
// account writes its status, and waking every object on every one of those
// writes would put the whole catalogue back in the queue once a minute for no
// reason.
func enqueueOnAccountReady(mgr ctrl.Manager, account types.NamespacedName, list client.ObjectList,
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
		builder.WithPredicates(accountBecameUsable(account))
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
func (r *DatabricksAccountReconciler) reportPrepared(account *dbxv1alpha1.DatabricksAccount,
	claims databricks.TokenClaims, err error) {
	switch {
	case err != nil:
		setCondition(&account.Status.Conditions, account.Generation, conditionPrepared,
			metav1.ConditionFalse, reasonUnprepared,
			fmt.Sprintf("the operator's own projected token could not be read (%v), so the "+
				"issuer every federation policy names and the audience every pod is given are "+
				"unknown. No identity can be issued or converged until it can be. Existing "+
				"workloads are unaffected: exchanging a token does not go through this operator.",
				err))
	case strings.TrimSpace(claims.Issuer) == "":
		setCondition(&account.Status.Conditions, account.Generation, conditionPrepared,
			metav1.ConditionFalse, reasonUnprepared,
			"the operator's own projected token carries no iss claim, so the issuer every "+
				"federation policy names is unknown. No identity can be issued or converged.")
	case len(claims.Audience) == 0 || claims.Audience[0] == "":
		setCondition(&account.Status.Conditions, account.Generation, conditionPrepared,
			metav1.ConditionFalse, reasonUnprepared,
			"the operator's own projected token carries no aud claim, so the audience every "+
				"pod is given is unknown. A federation policy naming none is one no token can "+
				"satisfy, so nothing is written.")
	default:
		setCondition(&account.Status.Conditions, account.Generation, conditionPrepared,
			metav1.ConditionTrue, reasonPrepared,
			fmt.Sprintf("writing %s into every federation policy and giving %s to every pod",
				claims.Issuer, claims.Audience[0]))
	}
}
