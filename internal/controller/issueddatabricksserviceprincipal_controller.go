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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	"github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

const (
	issuedRetryAfterAwaited = time.Minute
	issuedRetryAfterSettled = time.Minute
)

// IssuedDatabricksServicePrincipalReconciler owns everything this operator does
// in Databricks. Nothing else creates a service principal and nothing else
// deletes one.
//
// It reconciles the operator's own record of what it issued, which lives in the
// operator's own namespace and outlives the namespace that asked for it. That is
// what makes this the only controller here with no ordering to reason about: the
// record is its own work item, sitting where nothing a tenant does can reach it,
// and it is asked the same question every pass whether or not anybody saw
// anything happen.
type IssuedDatabricksServicePrincipalReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Databricks databricks.Clients

	// Live reads straight from the API server, bypassing the cache.
	//
	// It is used for the one read that decides a deletion. A cache that has not
	// caught up reports a ServiceAccount that exists as absent, and absent is
	// the answer that destroys an identity and everything granted to it. Being
	// a pass late is free; being wrong is not.
	Live client.Reader

	// Account is the DatabricksAccount this operator acts in. Records here are
	// answerable only once it is usable, and nothing on a record reports that --
	// so this controller watches it and wakes on the crossing.
	//
	// It is also how this operator is named: a ServiceAccount asks one operator
	// rather than another by naming its account's namespace and name, which two
	// operators cannot share.
	Account types.NamespacedName

	// TokenPath is where the operator's own projected token is mounted.
	//
	// The path and not the claims read from it. The issuer every federation
	// policy names and the audience every pod is given both come out of that
	// file, and reading them once at startup made a failure that lasted a moment
	// last for the life of the process: no identity could converge again, while
	// the account controller re-read the same file every pass and reported
	// everything healthy. The only thing that fixed it was a restart, and
	// nothing said so.
	//
	// So what is held is the thing that can be read, not a reading of it. A
	// failure is then a failure of this pass, and it heals when the file does.
	TokenPath string
}

// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=issueddatabricksserviceprincipals,verbs=get;list;watch;create;update;delete,namespace=system
// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=issueddatabricksserviceprincipals/status,verbs=get;update;patch,namespace=system
// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=issueddatabricksserviceprincipals/finalizers,verbs=update,namespace=system

// Reconcile asks one question of one record, and it is not "what changed".
//
// Is the ServiceAccount this was issued to still there, still asking, and still
// the same one? No is the answer that destroys the identity, and it is reached
// the same way whether the ServiceAccount was deleted, its annotation was
// withdrawn, its namespace was torn down, or all of that happened while this
// operator was not running. Nothing here waits to be told.
func (r *IssuedDatabricksServicePrincipalReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var issued dbxv1alpha1.IssuedDatabricksServicePrincipal
	switch err := r.Get(ctx, req.NamespacedName, &issued); {
	case apierrors.IsNotFound(err):
		return ctrl.Result{}, nil
	case err != nil:
		return ctrl.Result{}, err
	}

	// One view of the clients for the whole pass. Asking the Holder again
	// between the guard and the call it guards is what lets a pass check one
	// account and act in another.
	clients := r.Databricks.Snapshot()

	if !issued.DeletionTimestamp.IsZero() {
		return r.destroy(ctx, clients, &issued)
	}

	// Put back before anything is created, and only while this is not on its way
	// out.
	//
	// It is written when the record is made, and a record without it converges
	// like any other: it creates a real service principal, records the id, and
	// then, when it is deleted, is let go without deleting anything -- the exact
	// orphan this object exists to prevent, reachable by an object missing one
	// string. A record written by hand, or by whatever manages the operator's
	// namespace from a manifest, is missing it.
	//
	// Not while it is terminating, and that is what keeps the documented way out
	// working. A person who can see what is left behind removes this by hand;
	// putting it back then would be the operator arguing with them on a timer,
	// and the object would never go.
	if !controllerutil.ContainsFinalizer(&issued, dbxv1alpha1.ServicePrincipalFinalizer) {
		controllerutil.AddFinalizer(&issued, dbxv1alpha1.ServicePrincipalFinalizer)
		if err := r.Update(ctx, &issued); err != nil {
			return ctrl.Result{}, err
		}
		// Returned rather than carried on with, so that "the finalizer is there
		// before anything exists to hold" is something a reader can see rather
		// than trace. The next pass builds.
		return ctrl.Result{Requeue: true}, nil
	}

	// Ahead of everything else. An id means nothing outside the account it was
	// made in: acting on this record from another one would delete something
	// this operator never made, or read the 404 that means "not here" as the one
	// that means "gone".
	if elsewhere, message := r.elsewhere(clients, &issued); elsewhere {
		return r.report(ctx, &issued, metav1.ConditionUnknown, reasonAccountMismatch, message)
	}

	switch wanted, err := r.stillWanted(ctx, &issued); {
	case err != nil:
		return ctrl.Result{}, err
	case !wanted:
		// Deleting this record is how the identity is destroyed. The finalizer
		// does the destroying, so this is the whole of it here.
		return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, &issued))
	}

	return r.converge(ctx, clients, &issued)
}

// stillWanted reports whether the ServiceAccount this was issued to is still
// there, still asking, and still the same one.
//
// The uid is what makes the third clause mean anything. A namespace deleted and
// recreated under the same name can hold a ServiceAccount with the same name,
// whose subject is character for character the one this record was issued for --
// and Databricks matches that subject, so without this the new occupant would
// inherit the old identity and everything granted to it. A uid is issued once
// and never again.
//
// Read live rather than from the cache. This is the read that destroys things.
func (r *IssuedDatabricksServicePrincipalReconciler) stillWanted(ctx context.Context,
	issued *dbxv1alpha1.IssuedDatabricksServicePrincipal) (bool, error) {
	var account corev1.ServiceAccount
	switch err := r.Live.Get(ctx, types.NamespacedName{
		Namespace: issued.Spec.ServiceAccount.Namespace,
		Name:      issued.Spec.ServiceAccount.Name,
	}, &account); {
	case apierrors.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, err
	}
	// This identity, not merely this ServiceAccount. A workload that had two and
	// dropped one still asks this operator for the other, so asking whether it
	// asks at all would keep both -- and asking whether it asks for none would
	// destroy both.
	//
	// Withdrawn rather than "not asked for", and the two differ on exactly one
	// input: a key that is there and whose value will not parse. It asks for
	// nothing and it withdraws nothing, and reading the second as the first is
	// what deleted a service principal over a missing "/". This is the read that
	// destroys, so it is the one place where uncertainty must not.
	if dbxv1alpha1.RequestsFor(account.Annotations, r.Account).Withdrawn(issued.Spec.Identity) {
		return false, nil
	}
	return account.UID == issued.Spec.ServiceAccount.UID, nil
}

// destroy deletes the service principal, and holds the record until it has.
//
// The finalizer is never dropped on a timeout. Doing so would be this operator
// deciding that an identity nothing records is acceptable, on the evidence that
// one call did not go through. A person can decide that; they remove the
// finalizer by hand, having seen what is left behind.
func (r *IssuedDatabricksServicePrincipalReconciler) destroy(ctx context.Context,
	clients databricks.Clients,
	issued *dbxv1alpha1.IssuedDatabricksServicePrincipal) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(issued, dbxv1alpha1.ServicePrincipalFinalizer) {
		return ctrl.Result{}, nil
	}

	// Held rather than released. Releasing here would leave a live service
	// principal in another account with nothing anywhere recording it -- which
	// is the one outcome this whole shape exists to prevent. It waits instead,
	// and it costs nothing to wait: the record is in the operator's own
	// namespace, so nothing else is blocked by it.
	if elsewhere, message := r.elsewhere(clients, issued); elsewhere {
		return r.report(ctx, issued, metav1.ConditionUnknown, reasonAccountMismatch, message)
	}

	// The same refusal, for the same reason and a worse outcome. Deleting an id
	// in the wrong account answers 404, which reads as already gone, releases the
	// finalizer, and leaves a live service principal in the other account with
	// nothing anywhere recording it.
	if issued.Status.ServicePrincipalID != "" && issued.Status.AccountID == "" &&
		clients.AccountID() != "" {
		return r.report(ctx, issued, metav1.ConditionUnknown, reasonAccountUnknown,
			fmt.Sprintf("service principal %s is recorded with no Databricks account, so it is "+
				"not deleted from here: an answer of 'not found' would mean 'not in this "+
				"account' as readily as 'gone'. This record is held until somebody sets "+
				"status.accountId, or removes the finalizer having seen what is left behind.",
				issued.Status.ServicePrincipalID))
	}

	id := issued.Status.ServicePrincipalID
	if id == "" && issued.Status.RemovedServicePrincipalID == "" {
		// This record names no service principal, which is either a record whose
		// create never ran or one whose create ran and whose answer was never
		// written down. The second leaves a service principal nothing names, so
		// it is looked for rather than assumed away.
		issuer, _, err := r.issued(ctx)
		if err != nil {
			return r.report(ctx, issued, metav1.ConditionUnknown, reasonNotConfigured, err.Error())
		}
		found, _, ok, err := clients.FindServicePrincipal(ctx, r.issuing(issued, issuer))
		if err != nil {
			result := outcomeFor(err)
			return r.report(ctx, issued, result.Status, result.Reason, result.Message)
		}
		if ok {
			id = found
		}
	}

	if id != "" {
		if err := clients.DeleteServicePrincipal(ctx, id); err != nil {
			result := outcomeFor(err)
			return r.report(ctx, issued, result.Status, reasonRevokeFailed,
				fmt.Sprintf("%s; the service principal is still there, and this record is held until it is not",
					result.Message))
		}
	}

	controllerutil.RemoveFinalizer(issued, dbxv1alpha1.ServicePrincipalFinalizer)
	return ctrl.Result{}, client.IgnoreNotFound(r.Update(ctx, issued))
}

// converge builds the service principal and the federation policy that makes
// this subject's token exchangeable for it.
func (r *IssuedDatabricksServicePrincipalReconciler) converge(ctx context.Context,
	clients databricks.Clients,
	issued *dbxv1alpha1.IssuedDatabricksServicePrincipal) (ctrl.Result, error) {
	// Ahead of everything, because it is a decision rather than a state to
	// converge towards. Once a service principal this operator created has been
	// deleted in Databricks, nothing here builds another: that deletion was made
	// by somebody entitled to, and replacing it every minute would be this
	// operator overruling them on a timer, and winning.
	if issued.Status.RemovedServicePrincipalID != "" {
		return r.report(ctx, issued, metav1.ConditionFalse, reasonRemovedInDatabricks,
			fmt.Sprintf("service principal %s was deleted in Databricks and is not replaced. To "+
				"issue a new identity, remove %s from ServiceAccount %s/%s and add it again, "+
				"which produces a new client id",
				issued.Status.RemovedServicePrincipalID,
				dbxv1alpha1.ServicePrincipalAnnotationFor(issued.Spec.Identity),
				issued.Spec.ServiceAccount.Namespace, issued.Spec.ServiceAccount.Name))
	}

	// Read before anything is created. The federation policy is the only thing
	// on the Databricks side naming the cluster a service principal belongs to,
	// so one created before the issuer is known, whose policy then fails to be
	// written, is an identity with no account-side record at all.
	// Unknown and not False, which is this project's own rule for it and was
	// broken here while destroy a few lines up kept it. Nothing has been checked
	// -- the operator could not read its own token, so it has not asked
	// Databricks anything -- and False would report this identity as wrong on
	// the strength of never having looked at it.
	issuer, audience, err := r.issued(ctx)
	if err != nil {
		return r.report(ctx, issued, metav1.ConditionUnknown, reasonNotConfigured, err.Error())
	}

	// Written before the first call and not with its answer. A pass that creates
	// a service principal and then stops has to leave behind something saying
	// which account to look in, or an operator later pointed elsewhere finds
	// nothing, concludes nothing was made, and makes a second.
	//
	// Only when nothing has been made yet. A record that already names a service
	// principal and no account was made somewhere this operator cannot know, and
	// stamping it with wherever the operator happens to be now is inventing the
	// evidence the guard below is meant to weigh.
	if here := clients.AccountID(); here != "" &&
		issued.Status.AccountID == "" && issued.Status.ServicePrincipalID == "" {
		issued.Status.AccountID = here
		if err := r.Status().Update(ctx, issued); err != nil {
			return ctrl.Result{}, err
		}
	}

	// A recorded id with no account is not something to conclude from.
	//
	// Everything below reads a 404 as "somebody deleted it in Databricks", which
	// is recorded and never rebuilt. That reading is only available when the
	// account this was made in is known and is the one being asked -- otherwise
	// the same 404 means "not here", and taking it as a deletion erases the only
	// record of where a live identity is.
	//
	// The account is written before the first call, so this is a record carried
	// across an upgrade or edited by hand. It waits rather than guesses, and
	// somebody has to say which account it was made in.
	if issued.Status.ServicePrincipalID != "" && issued.Status.AccountID == "" &&
		clients.AccountID() != "" {
		return r.report(ctx, issued, metav1.ConditionUnknown, reasonAccountUnknown,
			fmt.Sprintf("service principal %s is recorded with no Databricks account, so an "+
				"answer of 'not found' cannot be told from a lookup in the wrong place. Nothing "+
				"is done for it and nothing about it is concluded. Set status.accountId to the "+
				"account it was made in, or delete this record if you have removed it there.",
				issued.Status.ServicePrincipalID))
	}

	if id := issued.Status.ServicePrincipalID; id != "" {
		switch there, err := clients.ServicePrincipalExists(ctx, id); {
		case err != nil:
			result := outcomeFor(err)
			return r.report(ctx, issued, result.Status, result.Reason, result.Message)
		case !there:
			// The id is kept, in its own field, because it is the value to
			// search Databricks' audit log with: that says who deleted it and
			// when. A flag would say something is wrong; an id says whom to ask.
			issued.Status.RemovedServicePrincipalID = id
			issued.Status.ServicePrincipalID = ""
			// Cleared so that no pod is equipped with a client id resolving to
			// nothing: the projection carries no client id, so the webhook
			// injects none.
			issued.Status.ClientID = ""
			return r.report(ctx, issued, metav1.ConditionFalse, reasonRemovedInDatabricks,
				fmt.Sprintf("service principal %s was deleted in Databricks and is not replaced. To "+
					"issue a new identity, remove %s from ServiceAccount %s/%s and add it again, "+
					"which produces a new client id",
					issued.Status.RemovedServicePrincipalID,
					dbxv1alpha1.ServicePrincipalAnnotationFor(issued.Spec.Identity),
					issued.Spec.ServiceAccount.Namespace, issued.Spec.ServiceAccount.Name))
		}
	}

	if issued.Status.ServicePrincipalID == "" {
		// Looked for before it is made. An empty id here is a record written
		// before a create that may or may not have run; making a second service
		// principal would leave the first behind and hand the workload a new
		// applicationId, so that everything granted to the first points at
		// nothing.
		issuing := r.issuing(issued, issuer)
		id, clientID, found, err := clients.FindServicePrincipal(ctx, issuing)
		if err != nil {
			result := outcomeFor(err)
			return r.report(ctx, issued, result.Status, result.Reason, result.Message)
		}
		if !found {
			id, clientID, err = clients.CreateServicePrincipal(ctx, issuing)
		}
		if err != nil {
			result := outcomeFor(err)
			return r.report(ctx, issued, result.Status, result.Reason, result.Message)
		}

		issued.Status.ServicePrincipalID = id
		issued.Status.ClientID = clientID
		issued.Status.AccountID = clients.AccountID()
		issued.Status.Issuer = issuer
		// Assigned here rather than after the write below, with the client id it
		// belongs to. The projection copies both into the tenant's namespace and
		// the webhook injects both, and a pod given a client id with no audience
		// gets a token minted for the audience kubelet defaults to -- which is
		// the one value that can never be exchanged. Half of a pair is worse
		// than neither.
		issued.Status.Audience = audience

		// Written before anything else is attempted. A service principal exists
		// in Databricks now, and an id this record never held is one nothing
		// deletes until the marker is read.
		//
		// Status().Update rather than the forgiving one: a NotFound here means
		// this record was deleted mid-pass, and carrying on would build a
		// federation policy onto an id nothing records.
		if err := r.Status().Update(ctx, issued); err != nil {
			return ctrl.Result{}, err
		}
	}

	issued.Status.Issuer = issuer
	issued.Status.Audience = audience
	if err := clients.EnsureFederationPolicy(ctx,
		issued.Status.ServicePrincipalID, issuer,
		issued.Spec.Subject, issued.Status.Audience); err != nil {
		result := outcomeFor(err)
		return r.report(ctx, issued, result.Status, result.Reason, result.Message)
	}

	return r.report(ctx, issued, metav1.ConditionTrue, reasonExchangeable,
		fmt.Sprintf("exchanging tokens for %s as client %s", issued.Spec.Subject, issued.Status.ClientID))
}

// issuing is what Databricks is told, assembled from what was recorded rather
// than from what is true now. A record that has outlived its ServiceAccount
// still has to name the same service principal.
func (r *IssuedDatabricksServicePrincipalReconciler) issuing(
	issued *dbxv1alpha1.IssuedDatabricksServicePrincipal, issuer string) databricks.Issuing {
	return databricks.Issuing{
		Issuer:            issuer,
		Namespace:         issued.Spec.ServiceAccount.Namespace,
		Name:              issued.Spec.ServiceAccount.Name,
		ServiceAccountUID: string(issued.Spec.ServiceAccount.UID),
		Operator:          r.Account.String(),
		Identity:          issued.Spec.Identity,
	}
}

// elsewhere reports whether this record was made in a different Databricks
// account than the one the operator is acting in, and says so in the words
// whoever reads it needs.
//
// Only when both are known. An unconfigured Holder reports no account, and that
// is not evidence of anything.
func (r *IssuedDatabricksServicePrincipalReconciler) elsewhere(clients databricks.Clients,
	issued *dbxv1alpha1.IssuedDatabricksServicePrincipal) (bool, string) {
	here := clients.AccountID()
	if here == "" || issued.Status.AccountID == "" || issued.Status.AccountID == here {
		return false, ""
	}
	return true, fmt.Sprintf(
		"this identity was made in Databricks account %s and this operator is acting in %s. "+
			"Nothing is done for it and nothing about it is concluded: it is not known to be "+
			"gone, only unreachable from where this is looking. Point the DatabricksAccount "+
			"back at %s to resume.",
		issued.Status.AccountID, here, issued.Status.AccountID)
}

// issued is the issuer and audience this operator's own token carries, read now.
//
// Both are taken from what is actually presented rather than from anything
// configured: every ServiceAccount in a cluster is issued tokens by the same
// issuer for the audience its volume asks for, so the operator's own token is
// the one to trust. A value read from what is presented cannot disagree with
// what is presented; a configured one can, and the disagreement surfaces as a
// token exchange that fails for a reason nothing reports.
//
// An empty audience is refused here rather than sent. A federation policy naming
// none is one no token can satisfy -- measured against a live account: any
// audience but the one named is refused, with the audience presented echoed back
// -- and writing it would leave an identity that looks built and can never be
// exchanged for.
func (r *IssuedDatabricksServicePrincipalReconciler) issued(context.Context) (issuer, audience string, err error) {
	claims, err := databricks.ReadTokenClaims(r.TokenPath)
	if err != nil {
		return "", "", err
	}
	issuer, err = databricks.IssuerOf(claims)
	if err != nil {
		return "", "", err
	}
	if len(claims.Audience) == 0 || claims.Audience[0] == "" {
		return "", "", fmt.Errorf(
			"the operator's own token carries no aud claim, so the audience to trust is unknown; " +
				"a federation policy naming none is one no token can satisfy")
	}
	return issuer, claims.Audience[0], nil
}

func (r *IssuedDatabricksServicePrincipalReconciler) report(ctx context.Context,
	issued *dbxv1alpha1.IssuedDatabricksServicePrincipal,
	status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {
	setCondition(&issued.Status.Conditions, issued.Generation, conditionReady, status, reason, message)
	return ctrl.Result{
		RequeueAfter: retryAfterFor(status, issuedRetryAfterAwaited, issuedRetryAfterSettled),
	}, client.IgnoreNotFound(r.Status().Update(ctx, issued))
}

// issuedByServiceAccount indexes records by the uid of the ServiceAccount they
// were issued to, so that an event about one wakes every record it holds without
// reading the whole namespace.
const issuedByServiceAccount = "spec.serviceAccount.uid"

// SetupWithManager sets up the controller with the Manager.
//
// ServiceAccounts are watched because they are what the question is about, and
// the record's name is derived from the ServiceAccount's uid alone, so an event
// names the record without anything having to be read to find out which one.
// The watch is the fast path; correctness rests on the periodic pass, which asks
// the same question whether or not any event was seen.
func (r *IssuedDatabricksServicePrincipalReconciler) SetupWithManager(mgr ctrl.Manager, namespace string) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(),
		&dbxv1alpha1.IssuedDatabricksServicePrincipal{}, issuedByServiceAccount,
		func(object client.Object) []string {
			issued, ok := object.(*dbxv1alpha1.IssuedDatabricksServicePrincipal)
			if !ok {
				return nil
			}
			return []string{string(issued.Spec.ServiceAccount.UID)}
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&dbxv1alpha1.IssuedDatabricksServicePrincipal{}).
		Watches(&corev1.ServiceAccount{}, handler.EnqueueRequestsFromMapFunc(
			func(ctx context.Context, object client.Object) []reconcile.Request {
				// Listed rather than derived from the ServiceAccount's name.
				//
				// One ServiceAccount now has as many records as it asked for
				// identities, and the names are derived from names this event
				// cannot see: dropping an identity from the annotation removes
				// the only place its name was written, so a name derived from
				// what the annotation says now would never wake the record that
				// has to be destroyed. What has to be woken is what exists.
				var records dbxv1alpha1.IssuedDatabricksServicePrincipalList
				if err := mgr.GetClient().List(ctx, &records,
					client.InNamespace(namespace),
					client.MatchingFields{
						issuedByServiceAccount: string(object.GetUID()),
					}); err != nil {
					return nil
				}
				return issuedRequests(&records)
			})).
		// An account that has just become usable makes every record here
		// answerable, and nothing on a record reports that.
		Watches(enqueueOnAccountReady(mgr, r.Account,
			&dbxv1alpha1.IssuedDatabricksServicePrincipalList{}, issuedRequests)).
		Named("issueddatabricksserviceprincipal").
		Complete(r)
}
