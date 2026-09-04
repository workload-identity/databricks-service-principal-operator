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

	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	acv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/applyconfiguration/api/v1alpha1"
	"github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
	dbxwebhook "github.com/workload-identity/databricks-service-principal-operator/internal/webhook"
)

const (
	// Two names and one value, because this controller has no second question to
	// ask. Everything it reports is read from objects it watches -- the record,
	// the pods, the ServiceAccount, the namespace, the DatabricksAccount -- so an
	// object that is not Ready learns nothing by asking again that the event it
	// waits for would not have brought sooner. What the interval is for is a
	// missed event, and that risk is the same either side of Ready.
	//
	// The awaited one is also what outcomeFor hands back, so it is how long a
	// failure against Databricks waits in every controller here.
	servicePrincipalRetryAfterAwaited = time.Minute
	servicePrincipalRetryAfterSettled = time.Minute
)

// DatabricksServiceAccountReconciler keeps one object in a tenant's namespace
// showing what this operator issued to one ServiceAccount.
//
// It talks to nothing outside the cluster. Every fact it reports was read from
// an IssuedDatabricksServicePrincipal in the operator's own namespace, which is
// the record; this is a copy of it put where its owner can read it. The copy is
// deleted and rebuilt freely, and neither is an event in the identity's life.
//
// The one thing it decides is whether a record should exist at all: a
// ServiceAccount asking, in a namespace this operator serves, where minting is
// open. Once the record exists nothing here reads MintLabel again, which is why
// closing minting cannot destroy an identity -- not as a rule obeyed, but as a
// question this controller no longer asks.
type DatabricksServiceAccountReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// The DatabricksAccount naming this operator, which is how a ServiceAccount
	// asks this one rather than another: the annotation names an operator by its
	// account's namespace and name, which two of them cannot share.
	//
	// Its namespace is the operator's own, so it is also where the records are
	// written and looked for.
	DatabricksAccountNamespacedName types.NamespacedName
}

// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=databricksserviceaccounts,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=databricksserviceaccounts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=databricks.workload-identity.io,resources=issueddatabricksserviceprincipals,verbs=get;list;watch;create,namespace=system
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile keeps the DatabricksServiceAccount for one ServiceAccount in step
// with the record.
//
// One request name is one ServiceAccount: the object carries that
// ServiceAccount's name in its namespace, and so does the request. Both
// directions are handled here -- an annotated ServiceAccount with no
// DatabricksServiceAccount yet, and a DatabricksServiceAccount whose
// ServiceAccount no longer asks.
func (r *DatabricksServiceAccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var serviceAccount corev1.ServiceAccount
	switch err := r.Get(ctx, req.NamespacedName, &serviceAccount); {
	case apierrors.IsNotFound(err):
		// The ServiceAccount is gone. Its DatabricksServiceAccount is owned by it and
		// is collected; the record is not, and its own controller is what notices.
		return ctrl.Result{}, nil
	case err != nil:
		return ctrl.Result{}, err
	}

	// Read once for the whole pass. It decides whether a record may be made here,
	// and nothing else -- in particular it does not end the pass.
	//
	// What this object holds is a copy of records that go on existing after a
	// namespace is taken out of scope -- their service principals are not
	// destroyed and their ids are intact -- and their trust is withdrawn, which
	// makes every one of them stop being exchangeable. Stopping here would
	// freeze this copy at the last thing that was true, so the team whose
	// workloads have just stopped reaching Databricks would read Ready on the
	// one object in their own namespace that is supposed to tell them.
	declared, served, err := accountServes(ctx, r.Client, r.DatabricksAccountNamespacedName, serviceAccount.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	requested := dbxv1alpha1.RequestsFor(serviceAccount.Annotations, r.DatabricksAccountNamespacedName)

	resent, err := r.resentIdentities(ctx, &serviceAccount, requested)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(requested.Understood) == 0 && len(resent) == 0 {
		// Nothing asked and nothing sent again, which is the only reading under
		// which every entry this operator owns was withdrawn. The condition used
		// to be "no requests", and a value that would not parse produced none --
		// so a lost "/" arrived here as a withdrawal and destroyed a service
		// principal along with every grant made on it.
		//
		// This operator's entries go and the others stay. Withdrawing is also
		// what ends the identity, and that is the record's controller's to act
		// on -- nothing here destroys anything.
		return ctrl.Result{}, r.removeIdentities(ctx, &serviceAccount)
	}

	// Whether new records may be made here, asked once for the whole
	// ServiceAccount: minting is a property of the namespace, and asking it per
	// identity would read the same Namespace several times to get the same
	// answer.
	minting, err := namespaceMints(ctx, r.Client, serviceAccount.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Every answer, not any of them. The label is the cluster letting a team in;
	// the DatabricksAccount is the team that holds the account admin credential
	// saying where it may be spent, and an operator that has not read one yet has
	// not been told anything. A record made on the strength of the label alone
	// would be this operator spending that credential in a namespace its own
	// account does not name.
	minting = minting && declared && served

	entries := make([]dbxv1alpha1.ProjectedIdentity, 0, len(requested.Understood)+len(resent))
	for _, request := range requested.Understood {
		entry, made, err := r.entryFor(ctx, &serviceAccount, request, minting)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !made {
			continue
		}
		entries = append(entries, entry)
	}
	entries = append(entries, resent...)

	// Ordered by the name the asker gave each identity, unnamed first, which is
	// the order First reads and so decides what a pod naming no profile is
	// given. It has to be imposed here: what was asked for and what is being
	// sent again arrive as two groups, and neither the annotations nor the
	// DatabricksServiceAccount carries an order to take instead.
	slices.SortFunc(entries, func(a, b dbxv1alpha1.ProjectedIdentity) int {
		return strings.Compare(r.identityNameOf(a), r.identityNameOf(b))
	})

	if len(entries) == 0 {
		// Every identity asked for is in a namespace nobody opened. Nothing, and
		// nothing said: that is the ordinary state of every namespace in the
		// cluster, and an object in each of them saying so would be this operator
		// answering a question nobody asked.
		return ctrl.Result{}, nil
	}

	// All of them in one apply. Applying one at a time would have each write
	// remove the ones not sent -- this operator owns them all, and what it sends
	// is what it keeps -- so the object would end each pass holding whichever
	// identity happened to go last.
	if err := r.apply(ctx, &serviceAccount, entries...); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.comeBackIn(entries)}, nil
}

// comeBackIn is the soonest any of these identities wants to be looked at again.
//
// One object, one requeue, so the interval is the shortest of them: an identity
// still waiting on Databricks is checked at the awaited interval whatever its
// neighbours are doing.
func (r *DatabricksServiceAccountReconciler) comeBackIn(
	entries []dbxv1alpha1.ProjectedIdentity) time.Duration {
	soonest := servicePrincipalRetryAfterSettled
	for i := range entries {
		after := retryAfterFor(readyStatus(entries[i].Conditions),
			servicePrincipalRetryAfterAwaited, servicePrincipalRetryAfterSettled)
		if after < soonest {
			soonest = after
		}
	}
	return soonest
}

// entryFor is one identity's entry, and whether there is one to write.
//
// There is not when the record does not exist and the namespace is not open to
// minting. That is the ordinary state of every namespace nobody enabled, and it
// is not an error and not a condition: nothing is said about it anywhere.
//
// A namespace where minting was open and has been closed is not that case. Its
// records exist, so this returns their entries and the identities go on being
// shown and go on working.
func (r *DatabricksServiceAccountReconciler) entryFor(ctx context.Context,
	serviceAccount *corev1.ServiceAccount, request dbxv1alpha1.Request,
	minting bool) (dbxv1alpha1.ProjectedIdentity, bool, error) {
	issued, err := r.issued(ctx, serviceAccount, request.Name)
	if err != nil {
		return dbxv1alpha1.ProjectedIdentity{}, false, err
	}
	if issued == nil {
		if !minting {
			return dbxv1alpha1.ProjectedIdentity{}, false, nil
		}
		if issued, err = r.issue(ctx, serviceAccount, request); err != nil {
			return dbxv1alpha1.ProjectedIdentity{}, false, err
		}
	}

	entry := dbxv1alpha1.ProjectedIdentity{
		Profile:                   request.String(),
		Operator:                  request.Operator.String(),
		Issued:                    r.DatabricksAccountNamespacedName.Namespace + "/" + issued.Name,
		ServicePrincipalID:        issued.Status.ServicePrincipalID,
		RemovedServicePrincipalID: issued.Status.RemovedServicePrincipalID,
		ClientID:                  issued.Status.ClientID,
		AccountID:                 issued.Status.AccountID,
		Subject:                   issued.Spec.Subject,
		Audience:                  issued.Status.Audience,
	}

	// Reported whatever the record says, because it is a fact about pods rather
	// than about Databricks, and the two fail independently.
	equipped, err := r.equipment(ctx, serviceAccount, entry)
	if err != nil {
		return dbxv1alpha1.ProjectedIdentity{}, false, err
	}
	setCondition(&entry.Conditions, 0, conditionEquipped,
		equipped.Status, equipped.Reason, equipped.Message)

	ready := meta.FindStatusCondition(issued.Status.Conditions, conditionReady)
	if ready == nil {
		setCondition(&entry.Conditions, 0, conditionReady,
			metav1.ConditionUnknown, reasonAwaitingRecord,
			fmt.Sprintf("IssuedDatabricksServicePrincipal %s was made and has not been acted "+
				"on yet", issued.Name))
	} else {
		setCondition(&entry.Conditions, 0, conditionReady, ready.Status, ready.Reason, ready.Message)
	}
	return entry, true, nil
}

// readyStatus is the Ready of one entry, for deciding when to come back.
func readyStatus(conditions []metav1.Condition) metav1.ConditionStatus {
	if ready := meta.FindStatusCondition(conditions, conditionReady); ready != nil {
		return ready.Status
	}
	return metav1.ConditionUnknown
}

func (r *DatabricksServiceAccountReconciler) owner() client.FieldOwner {
	return fieldOwnerFor(r.DatabricksAccountNamespacedName)
}

// fieldOwnerFor is the field manager one operator writes as.
//
// It is the operator's own name, so the API server records which fields belong
// to it and refuses to let another operator take them. That is what makes
// several operators able to write one object without undoing each other -- the
// failure two of them writing one DatabricksAccount's status produced, where
// each undid the other forever -- and it is also what makes one label key on a
// Namespace an exclusive claim, since the second operator to send it is
// refused.
func fieldOwnerFor(account types.NamespacedName) client.FieldOwner {
	return client.FieldOwner("databricks.workload-identity.io/" + account.String())
}

// apply writes this operator's entry and nothing else.
//
// Server-side apply, on a list keyed by the request: what is sent is one entry,
// and the API server merges it with whatever other operators own. Read, change,
// write would take the whole list -- including entries this operator did not
// write and may have read before another operator changed them.
func (r *DatabricksServiceAccountReconciler) apply(ctx context.Context,
	serviceAccount *corev1.ServiceAccount, entries ...dbxv1alpha1.ProjectedIdentity) error {
	if _, err := r.project(ctx, serviceAccount); err != nil {
		return err
	}

	status := acv1alpha1.DatabricksServiceAccountStatus()
	for i := range entries {
		status = status.WithIdentities(projectedIdentityFor(entries[i]))
	}
	databricksServiceAccount := acv1alpha1.DatabricksServiceAccount(serviceAccount.Name, serviceAccount.Namespace).
		WithStatus(status)

	return client.IgnoreNotFound(r.Status().Apply(ctx, databricksServiceAccount, r.owner()))
}

// projectedIdentityFor is the entry as something that can say "unset".
//
// An apply configuration exists for one reason and it is this: under
// server-side apply an operator owns everything it sends, and a Go struct has no
// way to send nothing. A string with no value marshals as "", so sending the
// struct says "host is empty" where the truth is "this operator has no opinion
// about host" -- and the next operator to set it is refused with a conflict over
// a value nobody meant to claim.
//
// So each field is set only when it has one, and nothing else is sent. That
// rule is what TestTheDatabricksServiceAccountSendsOnlyWhatIsSet holds: it also
// fails when a field is added to ProjectedIdentity and not carried here, which
// is the silent half -- the DatabricksServiceAccount simply stops reporting it.
func projectedIdentityFor(entry dbxv1alpha1.ProjectedIdentity) *acv1alpha1.ProjectedIdentityApplyConfiguration {
	// The key, always, including when it is the only thing there is: an entry
	// without one is not an entry, and it is what the merge is keyed on.
	identity := acv1alpha1.ProjectedIdentity().WithProfile(entry.Profile)

	for _, field := range []struct {
		value string
		set   func(string) *acv1alpha1.ProjectedIdentityApplyConfiguration
	}{
		{entry.Operator, identity.WithOperator},
		{entry.Issued, identity.WithIssued},
		{entry.ServicePrincipalID, identity.WithServicePrincipalID},
		{entry.AccountID, identity.WithAccountID},
		{entry.ClientID, identity.WithClientID},
		{entry.RemovedServicePrincipalID, identity.WithRemovedServicePrincipalID},
		{entry.Subject, identity.WithSubject},
		{entry.Audience, identity.WithAudience},
	} {
		if field.value != "" {
			identity = field.set(field.value)
		}
	}

	for i := range entry.Conditions {
		condition := entry.Conditions[i]
		identity = identity.WithConditions(metav1ac.Condition().
			WithType(condition.Type).
			WithStatus(condition.Status).
			WithReason(condition.Reason).
			WithMessage(condition.Message).
			WithObservedGeneration(condition.ObservedGeneration).
			WithLastTransitionTime(condition.LastTransitionTime))
	}

	return identity
}

// removeIdentities takes this operator's entries out of the
// DatabricksServiceAccount, and the object with them when they were the last
// ones. Nothing outside the cluster is reached: the service principals these
// entries describe are the record controller's to end, and it ends none of them
// because of this.
//
// Applying no entries removes what this operator owns and leaves what it does
// not. An object left holding none says a ServiceAccount was issued nothing,
// which is what every ServiceAccount in the cluster says by having no object at
// all, so it goes.
func (r *DatabricksServiceAccountReconciler) removeIdentities(ctx context.Context,
	serviceAccount *corev1.ServiceAccount) error {
	var databricksServiceAccount dbxv1alpha1.DatabricksServiceAccount
	switch err := r.Get(ctx, client.ObjectKeyFromObject(serviceAccount), &databricksServiceAccount); {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return err
	}

	// Every entry this operator owns, said by the entry rather than worked out
	// from its key. A named identity's key is the name its asker chose and holds
	// nothing about who issued it, so an operator looking for its own reference
	// there finds its unnamed entry and none of its named ones -- and every named
	// one is left behind reporting Ready for a service principal that no longer
	// exists.
	var mine, others int
	for _, entry := range databricksServiceAccount.Status.Identities {
		if entry.AskedOf(r.DatabricksAccountNamespacedName) {
			mine++
			continue
		}
		others++
	}

	// Nothing of anybody's. The object says a ServiceAccount was issued nothing,
	// which is what every ServiceAccount in the cluster says by having no object
	// at all.
	if others == 0 {
		// Deleted rather than emptied, and that is not a shortcut.
		//
		// Applying no entries is how this operator releases what it owns, and it
		// works only while somebody else's remain. identities is the whole of this
		// status, so removing the last of them leaves the merge with an object that
		// has no fields -- which structured merge writes as null, and the API server
		// refuses: `status: Invalid value: "null": in body must be of type object`.
		// The apply then fails on every pass, forever, and the
		// DatabricksServiceAccount it was supposed to withdraw stays exactly as it
		// was.
		//
		// The case where that happens is the case where the object goes anyway.
		//
		// Two operators withdrawing at once both see none left and both delete.
		// The second is told it is already gone, which is the answer it wanted.
		return client.IgnoreNotFound(r.Delete(ctx, &databricksServiceAccount))
	}
	if mine == 0 {
		return nil
	}

	// Somebody else's entries remain, so releasing this operator's leaves an
	// object with something in it and the merge has fields to write.
	return r.apply(ctx, serviceAccount)
}

// issued reads the record for one identity of this ServiceAccount, or nil when
// there is none.
//
// By name rather than by listing: the name is derived from the ServiceAccount's
// uid and the identity's name, so one identity has exactly one record and a
// ServiceAccount recreated under the same name has different records.
func (r *DatabricksServiceAccountReconciler) issued(ctx context.Context,
	serviceAccount *corev1.ServiceAccount,
	identity string) (*dbxv1alpha1.IssuedDatabricksServicePrincipal, error) {
	var issued dbxv1alpha1.IssuedDatabricksServicePrincipal
	switch err := r.Get(ctx, types.NamespacedName{
		Namespace: r.DatabricksAccountNamespacedName.Namespace,
		Name:      dbxv1alpha1.IssuedNameFor(serviceAccount.Namespace, serviceAccount.Name, identity, serviceAccount.UID),
	}, &issued); {
	case apierrors.IsNotFound(err):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return &issued, nil
}

// issue writes the record. It is the operator promising to remember something
// before there is anything to remember.
//
// Nothing has been created in Databricks at this point, and that order is the
// whole guarantee: a pass that writes this and then fails leaves a record naming
// no service principal, which the marker resolves. The reverse order would leave
// a service principal nothing anywhere names.
func (r *DatabricksServiceAccountReconciler) issue(ctx context.Context,
	serviceAccount *corev1.ServiceAccount,
	request dbxv1alpha1.Request) (*dbxv1alpha1.IssuedDatabricksServicePrincipal, error) {
	var namespace corev1.Namespace
	if err := r.Get(ctx, types.NamespacedName{Name: serviceAccount.Namespace}, &namespace); err != nil {
		return nil, err
	}

	issued := &dbxv1alpha1.IssuedDatabricksServicePrincipal{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.DatabricksAccountNamespacedName.Namespace,
			Name: dbxv1alpha1.IssuedNameFor(
				serviceAccount.Namespace, serviceAccount.Name, request.Name, serviceAccount.UID),
			// Set at creation rather than on the first pass that builds
			// something. A record that could be deleted before its finalizer was
			// added is one whose service principal nothing would remove.
			Finalizers: []string{dbxv1alpha1.ServicePrincipalFinalizer},
		},
		Spec: dbxv1alpha1.IssuedDatabricksServicePrincipalSpec{
			Subject:  databricks.SubjectFor(serviceAccount.Namespace, serviceAccount.Name),
			Identity: request.Name,
			ServiceAccount: dbxv1alpha1.IssuedServiceAccount{
				Namespace:    serviceAccount.Namespace,
				Name:         serviceAccount.Name,
				UID:          serviceAccount.UID,
				NamespaceUID: namespace.UID,
			},
		},
	}
	// AlreadyExists is another pass having got there first, or this one having
	// already written it. Either way the record is there and it is the same one:
	// the name is derived, not generated.
	switch err := r.Create(ctx, issued); {
	case err == nil:
		// What Create filled in, not what a read gives back. Reads here go
		// through the informer's cache, which lags its own writes, so reading
		// the record just written returns "not found" often enough -- and
		// "not found" from that read is nil with no error, which the caller
		// dereferences.
		return issued, nil
	case !apierrors.IsAlreadyExists(err):
		return nil, err
	}

	existing, err := r.issued(ctx, serviceAccount, request.Name)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		// The API server says this record exists and the cache does not have it
		// yet. Neither is wrong and neither is the answer, so this pass makes no
		// claim about the identity and comes back for it.
		return nil, fmt.Errorf(
			"record %s for %s/%s exists and has not reached this operator's cache yet",
			issued.Name, serviceAccount.Namespace, serviceAccount.Name)
	}
	return existing, nil
}

// project makes sure there is an object in the tenant's namespace to write on.
//
// Owned by the ServiceAccount, so it is collected when the ServiceAccount goes.
// That is all its ownership does: nothing about any identity depends on this
// object existing, and nothing happens when it stops.
//
// Created rather than applied, because an apply to the status subresource needs
// the object to be there, and because the owner reference is written once and
// belongs to whichever operator got here first -- it says the same thing whoever
// wrote it.
func (r *DatabricksServiceAccountReconciler) project(ctx context.Context,
	serviceAccount *corev1.ServiceAccount) (*dbxv1alpha1.DatabricksServiceAccount, error) {
	var principal dbxv1alpha1.DatabricksServiceAccount
	err := r.Get(ctx, client.ObjectKeyFromObject(serviceAccount), &principal)
	if err == nil {
		return &principal, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	principal = dbxv1alpha1.DatabricksServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: serviceAccount.Namespace, Name: serviceAccount.Name},
	}
	if err := controllerutil.SetControllerReference(serviceAccount, &principal, r.Scheme); err != nil {
		return nil, err
	}
	switch err := r.Create(ctx, &principal); {
	case err == nil:
		// What Create filled in. Reading it back would go through the cache,
		// which lags its own writes, and the answer to a miss there is neither
		// "there is none" nor an error worth reporting -- it is a read that did
		// not need making.
		return &principal, nil
	case !apierrors.IsAlreadyExists(err):
		return nil, err
	}
	return &principal, r.Get(ctx, client.ObjectKeyFromObject(serviceAccount), &principal)
}

// equipment says whether the pods running under this ServiceAccount carry
// what they need to reach Databricks: the token, and somewhere to send it.
//
// The webhook admits a pod it could not equip, deliberately: an outage of this
// operator must never stop workloads from starting. What that leaves is a pod
// that looks entirely normal and cannot reach Databricks, failing with an SDK
// error that points at Databricks -- and it does not heal when the webhook comes
// back, because nothing rewrites a running pod. This is what makes that visible,
// on the object somebody already looks at when Databricks access is not working.
//
// Only pods that are still going to run: one that is completed or on its way out
// is history, and reporting it would keep an alarm on for something nobody can
// or should fix.
func (r *DatabricksServiceAccountReconciler) equipment(ctx context.Context,
	serviceAccount *corev1.ServiceAccount, entry dbxv1alpha1.ProjectedIdentity) (outcome, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(serviceAccount.Namespace)); err != nil {
		return outcome{}, err
	}

	// Three lists, because the three failures are fixed by different people
	// doing different things, and a message that merged them would tell each of
	// them to do the others'.
	var noToken, wrongAudience, stale []string
	for i := range pods.Items {
		pod := &pods.Items[i]
		if serviceAccountOf(pod) != serviceAccount.Name || !pod.DeletionTimestamp.IsZero() {
			continue
		}
		switch pod.Status.Phase {
		case corev1.PodSucceeded, corev1.PodFailed:
			continue
		}
		switch minted, carries := tokenAudienceOf(pod, entry.Profile); {
		case !carries:
			noToken = append(noToken, pod.Name)
		case minted != entry.Audience:
			wrongAudience = append(wrongAudience, pod.Name)
		case !describes(pod, entry):
			// The pod carries this identity's token and a configuration written
			// before the identity said something the workload needs -- a host it
			// has since named, or a client id it did not have then. The pod is
			// not wrong about anything it holds; it is holding an older answer.
			stale = append(stale, pod.Name)
		}
	}

	if len(noToken) == 0 && len(wrongAudience) == 0 && len(stale) == 0 {
		return outcome{
			Status: metav1.ConditionTrue, Reason: reasonEquipped,
			Message: "every pod under this ServiceAccount carries what it needs to reach Databricks",
		}, nil
	}

	slices.Sort(noToken)
	slices.Sort(wrongAudience)
	slices.Sort(stale)
	var said []string
	if len(noToken) > 0 {
		// What to do about it depends on something this pass can find out and the
		// message used not to ask: a namespace that does not carry the inject
		// label is one where no pod is ever equipped, and recreating them for
		// ever changes nothing.
		injecting, err := r.injecting(ctx, serviceAccount.Namespace)
		if err != nil {
			return outcome{}, err
		}
		if injecting {
			// Named rather than chosen between. The webhook being unable to
			// answer is one of three ways here, and the other two are it working
			// as intended: it drops an identity with no client id rather than
			// write a profile nothing can be exchanged for, and it leaves a pod
			// that already carries a volume of that name alone. Nothing in this
			// pass tells them apart -- a pod equipped for one identity and not
			// another carries the volume too -- and the one that recreating does
			// not fix is the one whose owner would be left recreating for ever.
			said = append(said, fmt.Sprintf(
				"%d pod(s) are running without this identity's Databricks token: %s. A pod is "+
					"admitted without one when the webhook could not answer, when this identity "+
					"had no client id yet, or when the pod already carried a volume named %s, "+
					"which this operator does not overwrite; recreating the pod answers the "+
					"first two and not the third",
				len(noToken), strings.Join(noToken, ", "), dbxwebhook.TokenVolume))
		} else {
			said = append(said, fmt.Sprintf(
				"%d pod(s) are running without the Databricks token, and namespace %s does not "+
					"carry %s=%s, so no pod created here is given one. Recreating them changes "+
					"nothing until that label is set: %s",
				len(noToken), serviceAccount.Namespace,
				dbxv1alpha1.InjectLabel, dbxv1alpha1.Enabled, strings.Join(noToken, ", ")))
		}
	}
	if len(wrongAudience) > 0 {
		said = append(said, fmt.Sprintf(
			"%d pod(s) carry a token minted for another audience, which Databricks will not "+
				"accept whatever else is right; they were created before this identity knew its "+
				"own, and recreating them is what fixes it: %s",
			len(wrongAudience), strings.Join(wrongAudience, ", ")))
	}
	if len(stale) > 0 {
		if entry.ClientID == "" {
			// The identity has no client id to write, so a pod created now would
			// carry no profile at all and the advice above is one nobody can
			// act on. What to do instead is on Ready, which is where the reason
			// the client id is gone is written.
			said = append(said, fmt.Sprintf(
				"%d pod(s) carry a profile naming a client id this identity no longer has, so "+
					"nothing they hold can be exchanged; Ready says why and what starts a new "+
					"identity: %s",
				len(stale), strings.Join(stale, ", ")))
		} else {
			said = append(said, fmt.Sprintf(
				"%d pod(s) carry this identity's token and a configuration written before it said "+
					"what it says now; they were created earlier, and recreating them is what fixes "+
					"it: %s",
				len(stale), strings.Join(stale, ", ")))
		}
	}
	return outcome{
		Status: metav1.ConditionFalse, Reason: reasonNotEquipped,
		Message: strings.Join(said, ". "),
	}, nil
}

// describes reports whether the configuration this pod carries says about this
// identity what the identity says now.
//
// Compared rather than inspected key by key: what the webhook writes for one
// identity is one block, so the whole block either appears in the pod or does
// not, and every way it can be out of date -- a host named since, a client id
// assigned since -- is the same answer without a check for each.
//
// A pod whose configuration was written by hand is left alone by the webhook and
// will not match. That is the right report: this operator did not equip it, and
// saying so is not the same as saying it is broken.
func describes(pod *corev1.Pod, entry dbxv1alpha1.ProjectedIdentity) bool {
	written := dbxwebhook.Configuration([]dbxv1alpha1.ProjectedIdentity{entry})
	if written == "" {
		// An identity with no client id is one the webhook writes nothing for,
		// and every string contains the empty one -- so without this the check
		// answers yes for every pod in the namespace and Equipped reports True
		// for pods carrying a profile for a service principal that is not there.
		// Recording one as removed in Databricks clears the client id, which is
		// exactly when those pods are worth reporting.
		return false
	}
	return strings.Contains(pod.Annotations[dbxwebhook.ConfigAnnotation], written)
}

// serviceAccountOf is the ServiceAccount a pod actually runs as. An empty name
// means "default", which Kubernetes substitutes and which can hold an identity
// like any other.
func serviceAccountOf(pod *corev1.Pod) string {
	if pod.Spec.ServiceAccountName == "" {
		return "default"
	}
	return pod.Spec.ServiceAccountName
}

// tokenAudienceOf is the audience the token in this pod was minted for, and
// whether there is one at all.
//
// The audience is asked for rather than the volume's name alone, because the
// name cannot answer the question. A pod carrying a volume of the right name
// whose token was minted for another audience is one Databricks will refuse
// whatever else is right, and it looks equipped from the outside. Two ways in:
// a pod admitted before this identity had recorded its own audience, and a
// volume of that name this operator did not write.
func tokenAudienceOf(pod *corev1.Pod, request string) (audience string, carries bool) {
	wanted := dbxwebhook.TokenProjectionPathFor(request)
	for _, volume := range pod.Spec.Volumes {
		if volume.Name != dbxwebhook.TokenVolume || volume.Projected == nil {
			continue
		}
		for _, source := range volume.Projected.Sources {
			// By path, because one volume now carries one token per identity.
			// Taking the first would report every identity by whichever happened
			// to be projected first, so a pod equipped for one and not another
			// would read as equipped for both.
			if source.ServiceAccountToken != nil &&
				source.ServiceAccountToken.Path == wanted {
				return source.ServiceAccountToken.Audience, true
			}
		}
	}
	return "", false
}

// resentIdentities is last pass's entry for every identity whose annotation key
// is there and could not be read, to be sent again exactly as it stands.
//
// Sent again because an apply keeps only what it sends: an entry left out is an
// entry taken away, so the identity would come off the object its owner reads
// and the webhook would stop equipping new pods for it -- over a typo, while the
// service principal itself is untouched and still in use. Nothing about it is
// recomputed either, because the request that would say what to compute is the
// thing that could not be read.
func (r *DatabricksServiceAccountReconciler) resentIdentities(ctx context.Context,
	serviceAccount *corev1.ServiceAccount,
	requested dbxv1alpha1.Requested) ([]dbxv1alpha1.ProjectedIdentity, error) {
	if len(requested.Refused) == 0 {
		return nil, nil
	}

	var databricksServiceAccount dbxv1alpha1.DatabricksServiceAccount
	switch err := r.Get(ctx, client.ObjectKeyFromObject(serviceAccount), &databricksServiceAccount); {
	case apierrors.IsNotFound(err):
		return nil, nil
	case err != nil:
		return nil, err
	}

	var resent []dbxv1alpha1.ProjectedIdentity
	for _, entry := range databricksServiceAccount.Status.Identities {
		if entry.AskedOf(r.DatabricksAccountNamespacedName) && requested.Unreadable(r.identityNameOf(entry)) {
			resent = append(resent, entry)
		}
	}
	return resent, nil
}

// identityNameOf is the name this entry's asker gave the identity, which is what
// its annotation key holds and what the entry itself is not keyed by.
//
// An entry is keyed by the profile name, and for the unnamed identity that is
// this operator's own reference rather than anything a person wrote. Reading it
// back is this operator's to do, because only it knows its own reference.
func (r *DatabricksServiceAccountReconciler) identityNameOf(
	entry dbxv1alpha1.ProjectedIdentity) string {
	if entry.Profile == r.DatabricksAccountNamespacedName.String() {
		return ""
	}
	return entry.Profile
}

// injecting reports whether pods created in this namespace are given their
// token. It is asked only when there is something to say about a pod, because
// the answer only changes what is said.
func (r *DatabricksServiceAccountReconciler) injecting(ctx context.Context, name string) (bool, error) {
	var namespace corev1.Namespace
	switch err := r.Get(ctx, types.NamespacedName{Name: name}, &namespace); {
	case apierrors.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, err
	}
	return namespace.Labels[dbxv1alpha1.InjectLabel] == dbxv1alpha1.Enabled, nil
}

// namespaceMints reports whether new identities may be made in this namespace.
//
// Separate from the annotation, and answered separately, because the two are
// different people saying different things and withdrawing them means different
// things. A namespace where minting was never opened gets no identities; one
// where it is closed keeps the identities it has. See MintLabel.
//
// No while any operator is withdrawing here, whichever operator that is. What it
// is doing is taking the trust off a set of identities, and a set that can still
// grow while it works is one it finishes without having covered -- so minting
// stops for everybody until the withdrawal is over and the key comes off. The
// permission underneath is untouched and needs nobody to give it again.
//
// A namespace that is not there mints nothing.
func namespaceMints(ctx context.Context, reader client.Reader, name string) (bool, error) {
	var namespace corev1.Namespace
	switch err := reader.Get(ctx, types.NamespacedName{Name: name}, &namespace); {
	case apierrors.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, err
	}
	if _, withdrawing := namespace.Labels[dbxv1alpha1.WithdrawingLabel]; withdrawing {
		return false, nil
	}
	return namespace.Labels[dbxv1alpha1.MintLabel] == dbxv1alpha1.Enabled, nil
}

// identitiesInNamespace wakes every ServiceAccount in a namespace whose label
// just changed.
//
// Without it, enabling a namespace does nothing until something else touches
// each ServiceAccount in it. That is not the rare case: a team is onboarded by
// annotating its ServiceAccounts and then having somebody with cluster-wide
// access enable the namespace, and in that order nothing happens and nothing
// says so. Not visible to a unit test, because every one of them calls Reconcile
// directly and a namespace label is not an event on anything this watched.
func (r *DatabricksServiceAccountReconciler) identitiesInNamespace(ctx context.Context,
	object client.Object) []reconcile.Request {
	var accounts corev1.ServiceAccountList
	if err := r.List(ctx, &accounts, client.InNamespace(object.GetName())); err != nil {
		// Dropping the wake-up costs a wait, not correctness: each identity
		// comes back on its own interval, and an annotation written after this
		// wakes it directly.
		return nil
	}
	requests := make([]reconcile.Request, 0, len(accounts.Items))
	for i := range accounts.Items {
		// Anything this operator owes something for: an identity it is asked for,
		// or a key it could not read and has to say so about. Reading one value
		// no longer answers that -- a ServiceAccount asking only through named
		// keys carries nothing at all under the bare one -- so the annotations
		// are read the way every other caller reads them.
		requested := dbxv1alpha1.RequestsFor(accounts.Items[i].Annotations, r.DatabricksAccountNamespacedName)
		if len(requested.Understood) == 0 && len(requested.Refused) == 0 {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&accounts.Items[i]),
		})
	}
	return requests
}

// identitiesThisOperatorReaches is every ServiceAccount in every namespace this
// operator's DatabricksAccount names.
//
// A team adding a namespace to that list changes the answer for every
// ServiceAccount in it, and is an event on none of them. Without this they stay
// unserved until something unrelated happens to touch one.
//
// The list before the edit is not read, and does not need to be. A namespace
// taken off it holds identities whose trust the record's controller withdraws,
// and every one of those records writes its status when it does -- which is an
// event this controller already watches, on the ServiceAccount it belongs to.
// So the namespaces that left the list are woken by the work being done in them
// rather than by being remembered here.
func (r *DatabricksServiceAccountReconciler) identitiesThisOperatorReaches(ctx context.Context,
	object client.Object) []reconcile.Request {
	databricksAccount, ok := object.(*dbxv1alpha1.DatabricksAccount)
	if !ok || client.ObjectKeyFromObject(databricksAccount) != r.DatabricksAccountNamespacedName {
		return nil
	}
	var requests []reconcile.Request
	for _, namespace := range databricksAccount.Spec.Namespaces {
		requests = append(requests,
			r.identitiesInNamespace(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: namespace},
			})...)
	}
	return requests
}

// SetupWithManager sets up the controller with the Manager.
//
// Everything is keyed on the ServiceAccount, because the ServiceAccount is what
// the request means. Owns alone would not do it -- an annotated ServiceAccount
// with no DatabricksServiceAccount yet owns nothing, so nothing would wake
// this.
func (r *DatabricksServiceAccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// What this operator may act in is written here, and a namespace added
		// to it is an event on no ServiceAccount.
		Watches(&dbxv1alpha1.DatabricksAccount{},
			handler.EnqueueRequestsFromMapFunc(r.identitiesThisOperatorReaches)).
		For(&dbxv1alpha1.DatabricksServiceAccount{}).
		// Pods are watched so that one admitted without a token is reported
		// rather than left looking normal. The map is to the ServiceAccount it
		// runs as, which is one request per pod event and nothing when there is
		// no such identity.
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(
			func(_ context.Context, object client.Object) []reconcile.Request {
				pod, ok := object.(*corev1.Pod)
				if !ok {
					return nil
				}
				return []reconcile.Request{{
					NamespacedName: types.NamespacedName{
						Namespace: pod.Namespace,
						Name:      serviceAccountOf(pod),
					},
				}}
			})).
		// A namespace being enabled, or losing its label, changes the answer for
		// every ServiceAccount in it, and is an event on none of them.
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.identitiesInNamespace)).
		Watches(&corev1.ServiceAccount{}, handler.EnqueueRequestsFromMapFunc(
			func(_ context.Context, object client.Object) []reconcile.Request {
				return []reconcile.Request{{
					NamespacedName: types.NamespacedName{
						Namespace: object.GetNamespace(),
						Name:      object.GetName(),
					},
				}}
			})).
		// The record is what this projects, so a change to one is the event that
		// makes this object wrong.
		Watches(&dbxv1alpha1.IssuedDatabricksServicePrincipal{}, handler.EnqueueRequestsFromMapFunc(
			func(_ context.Context, object client.Object) []reconcile.Request {
				issued, ok := object.(*dbxv1alpha1.IssuedDatabricksServicePrincipal)
				if !ok {
					return nil
				}
				return []reconcile.Request{{
					NamespacedName: types.NamespacedName{
						Namespace: issued.Spec.ServiceAccount.Namespace,
						Name:      issued.Spec.ServiceAccount.Name,
					},
				}}
			})).
		Named("databricksserviceaccount").
		Complete(r)
}
