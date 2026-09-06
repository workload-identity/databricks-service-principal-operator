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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	"github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

const (
	// issuedRetryAfterAwaited is how long a record that is not Ready waits. What
	// it is waiting on is a person acting in Databricks, which raises no event
	// here.
	issuedRetryAfterAwaited = time.Minute

	// issuedRetryAfterSettled is the interval on a converged identity, and it is
	// ten times the other because nobody is waiting on it. It exists to notice a
	// service principal deleted in Databricks, which raises no event either; a
	// minute of it costs every identity in the account an existence check and a
	// federation policy listing, once a minute, for as long as the operator
	// runs. Ten minutes is what the account's own liveness check settled on for
	// the same question.
	issuedRetryAfterSettled = 10 * time.Minute
)

// IssuedDatabricksServicePrincipalReconciler owns everything this operator does
// in Databricks. Nothing else creates a service principal and nothing else
// deletes one.
//
// It reconciles the operator's own record of what it issued, which lives in the
// operator's own namespace and outlives the namespace that asked for it. The
// record is its own work item, sitting where nothing a tenant does can reach it,
// and it is asked the same question every pass whether or not anybody saw
// anything happen.
//
// One thing here waits on another controller, and only one: destroying an
// identity because its namespace left the account's list waits until this
// operator has claimed that namespace, so that the set being destroyed has
// stopped growing. It waits on the state rather than on the pass -- it reads the
// Namespace -- so nothing has to run in an order.
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

	// The DatabricksAccount this operator acts in. Records here are answerable
	// only once it is usable, and nothing on a record reports that -- so this
	// controller watches it and wakes on the crossing.
	//
	// It is also how this operator is named: a ServiceAccount asks one operator
	// rather than another by naming its account's namespace and name, which two
	// operators cannot share.
	DatabricksAccountNamespacedName types.NamespacedName

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

// Reconcile asks two questions of one record before it converges anything, and
// neither is "what changed".
//
// Is the ServiceAccount this was issued to still there, still asking, and still
// the same one? And is its namespace still one this account names? No to either
// destroys the identity, and either is reached the same way whether the
// ServiceAccount was deleted, its annotation was removed, its namespace was torn
// down, the account stopped naming it, or all of that happened while this
// operator was not running. Nothing here waits to be told.
//
// The two are different people saying it -- whoever holds the ServiceAccount,
// and whoever holds the Databricks account -- and they destroy the same way,
// because there is one way an identity ends: the record is deleted and the
// finalizer takes the service principal with it.
func (r *IssuedDatabricksServicePrincipalReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var issued dbxv1alpha1.IssuedDatabricksServicePrincipal
	switch err := r.Get(ctx, req.NamespacedName, &issued); {
	case apierrors.IsNotFound(err):
		return ctrl.Result{}, nil
	case err != nil:
		return ctrl.Result{}, err
	}

	// One view of the clients for the whole pass. What is installed can be
	// withdrawn between two calls -- see Snapshot -- and a pass that looks up in
	// one account and deletes in another has the two answers that are
	// unrecoverable.
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

	switch wanted, err := r.stillWanted(ctx, &issued); {
	case err != nil:
		return ctrl.Result{}, err
	case !wanted:
		// Deleting this record is how the identity is destroyed. The finalizer
		// does the destroying, so this is the whole of it here.
		log.FromContext(ctx).Info("The ServiceAccount no longer asks for this identity, so the record is "+
			"deleted and its service principal with it",
			"tenantNamespace", issued.Spec.ServiceAccount.Namespace,
			"serviceAccount", issued.Spec.ServiceAccount.Name)
		return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, &issued))
	}

	// The second question of the same shape, and it is asked here rather than
	// inside converge so that it is reached whatever state the record is in. A
	// record whose service principal was already deleted in Databricks converges
	// no further, and one in a namespace this account no longer names still has
	// to go -- otherwise it sits there for ever, counted as an identity the
	// destruction has not finished with, holding the claim on its namespace.
	//
	// The account having to be there is the difference between an edit and an
	// absence. A DatabricksAccount that was deleted, or that this pass has not
	// read yet, names no namespace either -- and reading that as "every identity
	// is out of scope" would have a deleted object destroy every identity in the
	// cluster. Nothing said that; nothing was read.
	switch declared, served, err := databricksAccountServes(ctx, r.Client, r.DatabricksAccountNamespacedName,
		issued.Spec.ServiceAccount.Namespace); {
	case err != nil:
		return ctrl.Result{}, err
	case declared && !served:
		return r.destroyBecauseNotServed(ctx, &issued)
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
	var serviceAccount corev1.ServiceAccount
	switch err := r.Live.Get(ctx, types.NamespacedName{
		Namespace: issued.Spec.ServiceAccount.Namespace,
		Name:      issued.Spec.ServiceAccount.Name,
	}, &serviceAccount); {
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
	// NoLongerAsked rather than "not asked for", and the two differ on exactly
	// one input: a key that is there and whose value will not parse. Nothing is
	// asked for through it, and nothing is taken back through it either, and
	// reading the second as the first is what deleted a service principal over a
	// missing "/". This is the read that destroys, so it is the one place where
	// uncertainty must not.
	requested := dbxv1alpha1.RequestsFor(serviceAccount.Annotations, r.DatabricksAccountNamespacedName)
	if requested.NoLongerAsked(issued.Spec.Identity) {
		return false, nil
	}
	return serviceAccount.UID == issued.Spec.ServiceAccount.UID, nil
}

// destroy deletes the service principal, and holds the record until it has.
//
// Which of those happens is what the record's state says, and nothing else:
// letting the finalizer go is irreversible, so it is taken only where nothing
// can be left behind by it -- Unsent, where nothing can exist; Gone, where
// Databricks has answered; and after this operator's own delete has confirmed.
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

	// What there is to delete, which each state answers differently and only one
	// of them by asking Databricks.
	var id string
	switch issued.Status.ServicePrincipalState() {
	case dbxv1alpha1.ServicePrincipalUnsent:
		// Nothing was ever asked of Databricks for this record, so nothing can
		// be out there and there is nothing to look for. This is the ordinary
		// case -- a record deleted before it ever converged -- and a lookup here
		// would be the operator asking about work it knows it never began.

	case dbxv1alpha1.ServicePrincipalGone:
		// A get by id already answered that it is not there.

	case dbxv1alpha1.ServicePrincipalSent:
		// A create was sent and its answer never arrived, so something may exist
		// carrying this record's marker. It is looked for rather than assumed
		// away, and when the listing does not show it the record is held: a
		// listing is eventually consistent, and releasing on one is how a live
		// service principal ends up with nothing recording it.
		//
		// The issuer alone. Asking for the pair would hold this deletion over a
		// token carrying no aud claim -- a value only a federation policy needs,
		// and this path writes none.
		issuer, err := r.issuer(ctx)
		if err != nil {
			return r.reportReady(ctx, issued, metav1.ConditionUnknown, reasonUnprepared, err.Error())
		}
		issuing := r.issuing(issued, issuer)
		found, _, ok, err := clients.FindServicePrincipal(ctx, issuing)
		if err != nil {
			return r.reportFailure(ctx, issued, outcomeFor(err))
		}
		if !ok {
			return r.reportReady(ctx, issued, metav1.ConditionUnknown, reasonCreateUnconfirmed,
				fmt.Sprintf("a create was sent for this identity at %s and no service principal "+
					"carrying its marker has appeared, so this record is held rather than let go "+
					"of: releasing it now would leave one in Databricks that nothing records. "+
					"Search account %s for a service principal whose externalId is %s. Whoever "+
					"has looked and found none removes the finalizer; whoever finds one deletes "+
					"it there first.",
					issued.Status.ServicePrincipalCreateSentAt.UTC().Format(time.RFC3339),
					clients.AccountID(), databricks.MarkerFor(issuing)))
		}
		id = found

	case dbxv1alpha1.ServicePrincipalKnown:
		// The id is authoritative, and it is meaningful because the account this
		// operator acts in cannot change under it: spec.accountId is immutable,
		// the DatabricksAccount cannot be deleted while records name its
		// account, and an operator whose records name another one refuses to
		// start.
		id = issued.Status.ServicePrincipalID
	}

	logger := log.FromContext(ctx)
	if id != "" {
		if err := clients.DeleteServicePrincipal(ctx, id); err != nil {
			logger.Error(err, "Could not delete the service principal in Databricks, so this record is held",
				"servicePrincipalId", id)
			failure := outcomeFor(err)
			failure.Reason = reasonDeleteFailed
			failure.Message = fmt.Sprintf(
				"%s; the service principal is still there, and this record is held until it is not",
				failure.Message)
			return r.reportFailure(ctx, issued, failure)
		}
		logger.Info("Deleted the service principal in Databricks", "servicePrincipalId", id)
	}

	controllerutil.RemoveFinalizer(issued, dbxv1alpha1.ServicePrincipalFinalizer)
	logger.Info("Releasing the record: nothing it names is left in Databricks")
	return ctrl.Result{}, client.IgnoreNotFound(r.Update(ctx, issued))
}

// converge builds the service principal and the federation policy that makes
// this subject's token exchangeable for it.
func (r *IssuedDatabricksServicePrincipalReconciler) converge(ctx context.Context,
	clients databricks.Clients,
	issued *dbxv1alpha1.IssuedDatabricksServicePrincipal) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Ahead of everything, because it is a decision rather than a state to
	// converge towards. Once a service principal this operator created has been
	// deleted in Databricks, nothing here builds another: that deletion was made
	// by somebody entitled to, and replacing it every minute would be this
	// operator overruling them on a timer, and winning.
	if issued.Status.ServicePrincipalState() == dbxv1alpha1.ServicePrincipalGone {
		return r.reportReady(ctx, issued, metav1.ConditionFalse, reasonRemovedInDatabricks,
			removedInDatabricksMessage(issued))
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
	issuer, audience, err := r.issuerAndAudience(ctx)
	if err != nil {
		return r.reportReady(ctx, issued, metav1.ConditionUnknown, reasonUnprepared, err.Error())
	}

	issuing := r.issuing(issued, issuer)
	switch issued.Status.ServicePrincipalState() {
	case dbxv1alpha1.ServicePrincipalKnown:
		// The authoritative question, and the only state that can ask it. A get
		// by id answers for certain, so a 404 here is a deletion rather than a
		// listing that has not caught up.
		switch there, err := clients.ServicePrincipalExists(ctx, issued.Status.ServicePrincipalID); {
		case err != nil:
			return r.reportFailure(ctx, issued, outcomeFor(err))
		case !there:
			// The moment, and nothing cleared. The id stays where it was
			// written, which is where somebody searching the audit log for who
			// deleted it and when goes to read it; what stops a pod being
			// equipped is the identity no longer being usable, not the client id
			// being destroyed.
			removedAt := metav1.Now()
			issued.Status.ServicePrincipalRemovedAt = &removedAt
			logger.Info("Latching the service principal as removed in Databricks: nothing here builds "+
				"another", "servicePrincipalId", issued.Status.ServicePrincipalID)
			return r.reportReady(ctx, issued, metav1.ConditionFalse, reasonRemovedInDatabricks,
				removedInDatabricksMessage(issued))
		}

	case dbxv1alpha1.ServicePrincipalUnsent:
		// No lookup, because nothing can exist for this record to adopt. The
		// lookup that used to run here is a listing, and a listing that has not
		// caught up answers "nothing" about a service principal that is there --
		// which is only a wrong answer once a create has been sent.
		//
		// The mark goes down first and is persisted before the call, so a pass
		// that creates and then stops leaves behind the one fact it cannot work
		// out again. The account goes with it, in the same write: a record that
		// named a service principal and no account would be one nothing could
		// safely look up, and both are what a later pass needs to find what this
		// one made.
		if here := clients.AccountID(); here != "" && issued.Status.AccountID == "" {
			issued.Status.AccountID = here
		}
		sentAt := metav1.Now()
		issued.Status.ServicePrincipalCreateSentAt = &sentAt
		if err := r.Status().Update(ctx, issued); err != nil {
			return ctrl.Result{}, err
		}

		id, clientID, err := clients.CreateServicePrincipal(ctx, issuing)
		if err != nil {
			logger.Error(err, "Could not create the service principal in Databricks")
			return r.reportFailure(ctx, issued, outcomeFor(err))
		}
		logger.Info("Created a service principal in Databricks",
			"servicePrincipalId", id, "clientId", clientID)
		if err := r.adopt(ctx, clients, issued, id, clientID, issuer, audience); err != nil {
			return ctrl.Result{}, err
		}

	case dbxv1alpha1.ServicePrincipalSent:
		// A create was sent and its answer never arrived, so a service principal
		// carrying this record's marker may already exist. It is looked for, and
		// nothing else is done: creating here is what makes two of them carrying
		// one marker, which FindServicePrincipal then refuses to adopt either of
		// for ever.
		id, clientID, found, err := clients.FindServicePrincipal(ctx, issuing)
		if err != nil {
			return r.reportFailure(ctx, issued, outcomeFor(err))
		}
		if !found {
			return r.reportReady(ctx, issued, metav1.ConditionUnknown, reasonCreateUnconfirmed,
				fmt.Sprintf("a create was sent for this identity at %s and no service principal "+
					"carrying its marker has appeared, so nothing is created here: a second one "+
					"carrying the same marker could never be told from the first. Search account "+
					"%s for a service principal whose externalId is %s. Deleting it there, or "+
					"deleting this record once nothing is left, is what ends this.",
					issued.Status.ServicePrincipalCreateSentAt.UTC().Format(time.RFC3339),
					clients.AccountID(), databricks.MarkerFor(issuing)))
		}
		logger.Info("Adopted the service principal this record's create made",
			"servicePrincipalId", id, "clientId", clientID)
		if err := r.adopt(ctx, clients, issued, id, clientID, issuer, audience); err != nil {
			return ctrl.Result{}, err
		}
	}

	issued.Status.Issuer = issuer
	issued.Status.Audience = audience

	// Read before reportReady writes it again. The policy is written on every
	// pass, so a record already reporting the exchange as working is one this
	// pass changed nothing about -- and a line for each of those would be one per
	// identity per interval, for ever.
	exchangeable := meta.IsStatusConditionTrue(issued.Status.Conditions, conditionReady)
	if err := clients.EnsureFederationPolicy(ctx,
		issued.Status.ServicePrincipalID, issuer,
		issued.Spec.Subject, issued.Status.Audience); err != nil {
		logger.Error(err, "Could not write the federation policy, so no token from this cluster can be "+
			"exchanged for this identity", "servicePrincipalId", issued.Status.ServicePrincipalID,
			"subject", issued.Spec.Subject)
		return r.reportFailure(ctx, issued, outcomeFor(err))
	}
	if exchangeable {
		logger.V(1).Info("The federation policy is still in place",
			"servicePrincipalId", issued.Status.ServicePrincipalID)
	} else {
		logger.Info("Wrote the federation policy: this subject's tokens can be exchanged",
			"servicePrincipalId", issued.Status.ServicePrincipalID, "subject", issued.Spec.Subject,
			"issuer", issuer)
	}

	return r.reportReady(ctx, issued, metav1.ConditionTrue, reasonExchangeable,
		fmt.Sprintf("exchanging tokens for %s as client %s", issued.Spec.Subject, issued.Status.ClientID))
}

// adopt writes down what Databricks answered, before anything else is
// attempted. It is what takes a record from Sent to Known, whether the answer
// came from the create or from the lookup that found what an earlier create
// made.
//
// Status().Update rather than the forgiving one: a NotFound here means this
// record was deleted mid-pass, and carrying on would build a federation policy
// onto an id nothing records.
func (r *IssuedDatabricksServicePrincipalReconciler) adopt(ctx context.Context,
	clients databricks.Clients, issued *dbxv1alpha1.IssuedDatabricksServicePrincipal,
	id, clientID, issuer, audience string) error {
	issued.Status.ServicePrincipalID = id
	issued.Status.ClientID = clientID
	if here := clients.AccountID(); here != "" {
		issued.Status.AccountID = here
	}
	issued.Status.Issuer = issuer
	// Assigned with the client id it belongs to. The DatabricksServiceAccount
	// controller copies both into the tenant's namespace and the webhook injects
	// both, and a pod given a client id with no audience gets a token minted for
	// the audience kubelet defaults to -- which is the one value that can never
	// be exchanged. Half of a pair is worse than neither.
	issued.Status.Audience = audience
	return r.Status().Update(ctx, issued)
}

// removedInDatabricksMessage says what happened and names the one act that
// starts a new identity.
//
// It names the id, which is still on the record: that is the value somebody
// searches Databricks' audit log with, and it says who deleted it and when. A
// flag would say something is wrong; an id says whom to ask.
func removedInDatabricksMessage(issued *dbxv1alpha1.IssuedDatabricksServicePrincipal) string {
	return fmt.Sprintf("service principal %s was deleted in Databricks and is not replaced. To "+
		"issue a new identity, remove %s from ServiceAccount %s/%s and add it again, "+
		"which produces a new client id",
		issued.Status.ServicePrincipalID,
		dbxv1alpha1.ServicePrincipalAnnotationFor(issued.Spec.Identity),
		issued.Spec.ServiceAccount.Namespace, issued.Spec.ServiceAccount.Name)
}

// destroyBecauseNotServed destroys an identity whose namespace this account no
// longer names, by deleting the record: the finalizer deletes the service
// principal, and everything Databricks recorded against it goes with it.
//
// That is what taking a namespace off spec.namespaces means, and it is not
// reversible from here. Naming the namespace again issues a new service
// principal with a new client id and no grants; nothing looks for what was
// destroyed, because there is nothing left to look for.
//
// Nothing is destroyed until this operator holds the namespace, read from the
// Namespace rather than taken as done because the account was edited. Claiming
// it and destroying are two controllers with nothing sequencing them, and in
// between the two a ServiceAccount here still produces records: a pass of the
// DatabricksServiceAccount controller that read the account before the edit
// landed writes its record after it, and that record is one more thing to
// destroy. The claim is what closes that window, because minting is read fresh
// from the Namespace while the account was read stale.
//
// What it buys is a set that stops moving. Nothing is left behind either way --
// a record written in that window is routed here on its own pass and destroyed
// from the state it is in, having asked Databricks for nothing -- but the count
// the account reports would be chasing a set still growing while it counts, so
// "finished" would mean nothing.
//
// Both controllers read one cache, so a claim this one sees is one the
// DatabricksServiceAccount controller sees too.
//
// The wait is only on starting. Once the record is deleted, destroy runs on
// every pass whatever the namespace says: retreat needs no permission, and a
// destruction that stopped halfway because a claim changed hands would leave a
// service principal that nothing records.
func (r *IssuedDatabricksServicePrincipalReconciler) destroyBecauseNotServed(ctx context.Context,
	issued *dbxv1alpha1.IssuedDatabricksServicePrincipal) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	namespace := issued.Spec.ServiceAccount.Namespace

	holder, exists, err := destructionHeldBy(ctx, r.Client, namespace)
	switch {
	case err != nil:
		return ctrl.Result{}, err
	case exists && holder == (types.NamespacedName{}):
		return r.reportReady(ctx, issued, metav1.ConditionUnknown, reasonMintingNotSuspended,
			fmt.Sprintf("namespace %s is not one DatabricksAccount %s names, so this identity is "+
				"to be destroyed, and nothing carries %s there. Nothing has been destroyed yet: a "+
				"destruction begun while identities can still be made here would leave behind the "+
				"ones made after it. This operator writes that label on its account's own pass, "+
				"and the destruction follows.",
				namespace, r.DatabricksAccountNamespacedName, dbxv1alpha1.DestroyingIdentitiesLabel))
	case exists && holder != r.DatabricksAccountNamespacedName:
		logger.V(1).Info("Waiting for another operator to finish destroying its identities here",
			"tenantNamespace", namespace, "holder", holder)
		return r.reportReady(ctx, issued, metav1.ConditionUnknown, reasonAnotherDestruction,
			fmt.Sprintf("namespace %s carries %s=%s and %s=%s, so operator %s is destroying its "+
				"identities there and this one waits: one at a time is what keeps either from "+
				"working through a set the other is still adding to. Nothing has been destroyed "+
				"for this identity.",
				namespace, dbxv1alpha1.DestroyingIdentitiesLabel, holder.Namespace,
				dbxv1alpha1.DestroyingIdentitiesAccountAnnotation, holder.Name, holder))
	}

	logger.Info("Namespace is not one this account names, so the record is deleted and its service "+
		"principal with it", "tenantNamespace", namespace,
		"serviceAccount", issued.Spec.ServiceAccount.Name)
	return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, issued))
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
		Operator:          r.DatabricksAccountNamespacedName.String(),
		Identity:          issued.Spec.Identity,
	}
}

// issuerAndAudience is the issuer and audience this operator's own token
// carries, read now.
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
func (r *IssuedDatabricksServicePrincipalReconciler) issuerAndAudience(context.Context) (
	issuer, audience string, err error) {
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

// issuer is the half of that a deletion needs, and the only half it may be held
// over. A record on its way out has a service principal to find and remove and
// no policy to write, so the audience is not its business.
func (r *IssuedDatabricksServicePrincipalReconciler) issuer(context.Context) (string, error) {
	claims, err := databricks.ReadTokenClaims(r.TokenPath)
	if err != nil {
		return "", err
	}
	return databricks.IssuerOf(claims)
}

// reportReady ends the pass: it writes Ready, comes back at the interval that
// status asks for, and does the status update.
//
// It is how a pass returns, which is what tells it from the status updates
// above: those record what Databricks answered so that the next pass can find
// it, and the pass goes on.
func (r *IssuedDatabricksServicePrincipalReconciler) reportReady(ctx context.Context,
	issued *dbxv1alpha1.IssuedDatabricksServicePrincipal,
	status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {
	setCondition(&issued.Status.Conditions, issued.Generation, conditionReady, status, reason, message)
	return ctrl.Result{
		RequeueAfter: retryAfterFor(status, issuedRetryAfterAwaited, issuedRetryAfterSettled),
	}, client.IgnoreNotFound(r.Status().Update(ctx, issued))
}

// reportFailure ends the pass on something Databricks said: reportReady, plus
// the one thing outcomeFor decides that a condition cannot carry -- whether this
// failure goes back to the workqueue as an error, which is what outcome.Err is
// about.
//
// The condition is written either way, and first, so what a person reads does
// not depend on which kind of failure this was.
func (r *IssuedDatabricksServicePrincipalReconciler) reportFailure(ctx context.Context,
	issued *dbxv1alpha1.IssuedDatabricksServicePrincipal, failure outcome) (ctrl.Result, error) {
	result, err := r.reportReady(ctx, issued, failure.Status, failure.Reason, failure.Message)
	if err != nil {
		return ctrl.Result{}, err
	}
	if failure.Err != nil {
		return ctrl.Result{}, failure.Err
	}
	return result, nil
}

// issuedByServiceAccount indexes records by the uid of the ServiceAccount they
// were issued to, so that an event about one wakes every record it holds without
// reading the whole namespace.
const issuedByServiceAccount = "spec.serviceAccount.uid"

// SetupWithManager sets up the controller with the Manager.
//
// ServiceAccounts are watched because they are what the question is about. An
// event on one names none of its records: there is a record per identity and
// the name carries a hash of the identity, so the map function below lists
// them. The watch is the fast path; correctness rests on the periodic pass,
// which asks the same question whether or not any event was seen.
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
		// answerable, and one that has stopped serving a namespace makes the
		// records in it ones to remove the policies from. Nothing on a record
		// reports either.
		Watches(enqueueOnDatabricksAccountChange(mgr, r.DatabricksAccountNamespacedName,
			&dbxv1alpha1.IssuedDatabricksServicePrincipalList{}, issuedRequests)).
		Named("issueddatabricksserviceprincipal").
		Complete(r)
}
