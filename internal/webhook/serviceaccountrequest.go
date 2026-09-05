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

package webhook

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

// RequestPath is where the refusal below is served, and like Path it appears
// twice: here, and in config/webhook/manifests.yaml.
//
// The same hand-written manifest, so the same drift, and the same silence when
// it happens: failurePolicy is Ignore, so a ServiceAccount sent to a path with
// no handler is admitted with the error swallowed, and this operator is back to
// saying nothing at all about a request it will never answer. That is exactly
// the state this webhook exists to end, so losing the path would look like
// nothing had been built.
const RequestPath = "/validate-v1-serviceaccount"

// ServiceAccountRequestRefuser refuses an annotation asking this operator for an
// identity in a namespace its DatabricksAccount does not name.
//
// The refusal is not new: the controller already declines to mint there, because
// minting outside spec.namespaces would be this operator spending an account
// admin credential where its own account says it may not. What is new is that
// somebody hears about it. Declined in the controller, the request produces no
// entry, so no DatabricksServiceAccount, so no condition and no event -- nothing
// anywhere says the annotation was seen and refused, and whoever wrote it waits
// for an identity that is never coming with nowhere to look.
//
// So this moves the refusal to the moment of the write, where there is a person
// attached to it and a message can reach them.
type ServiceAccountRequestRefuser struct {
	client.Client
	Decoder admission.Decoder

	// DatabricksAccountNamespacedName is the account this operator acts in, and
	// it decides both halves: which annotations are this operator's to refuse,
	// and which namespaces it serves.
	DatabricksAccountNamespacedName types.NamespacedName
}

// Handle refuses the transition that introduces a request this operator cannot
// answer, and admits everything else.
//
// The transition and never the state, which is the whole shape of this. A rule
// that refused the state would make an already-annotated ServiceAccount in a
// namespace since taken off the list permanently unwritable -- no label, no
// secret, no ownerReference, and no way to take the annotation off either, since
// that write carries the state too. That is a worse failure than the one this
// prevents, and it lands on people who did nothing.
func (v *ServiceAccountRequestRefuser) Handle(ctx context.Context, req admission.Request) admission.Response {
	serviceAccount := &corev1.ServiceAccount{}
	if err := v.Decoder.Decode(req, serviceAccount); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Only what RequestsFor understood as addressed to this operator. A key
	// naming another operator is that operator's to answer, and a key that will
	// not parse names nobody -- refusing either would be this operator standing
	// in front of a decision that is not its own.
	asking := dbxv1alpha1.RequestsFor(serviceAccount.Annotations, v.DatabricksAccountNamespacedName)
	if len(asking.Understood) == 0 {
		return admission.Allowed("nothing here asks this operator for an identity")
	}

	introduced := v.introducedBy(req, asking)
	if len(introduced) == 0 {
		return admission.Allowed("this write introduces no request to this operator")
	}

	// The namespace on the request rather than on the object: a ServiceAccount
	// created through a template may not carry one yet, and the namespace is the
	// entire question here.
	namespace := req.Namespace
	if namespace == "" {
		namespace = serviceAccount.Namespace
	}

	var databricksAccount dbxv1alpha1.DatabricksAccount
	switch err := v.Get(ctx, v.DatabricksAccountNamespacedName, &databricksAccount); {
	case apierrors.IsNotFound(err):
		// Admitted. This operator has not been told anything about which
		// namespaces it serves, and knowing nothing is not a reason to refuse
		// somebody else's write. The controller is what enforces; all this does
		// is move a refusal earlier, and a refusal it cannot justify is one it
		// should not make.
		return admission.Allowed("this operator has no DatabricksAccount to answer from")
	case err != nil:
		// Said in the log because nothing else keeps it: an admission response
		// that refused nothing is stored nowhere, so a spell of unreadable
		// DatabricksAccounts would otherwise be a stretch of requests admitted
		// for a reason that left no trace.
		log.FromContext(ctx).Error(err, "Could not read the DatabricksAccount, so this request is "+
			"admitted without being checked", "namespace", namespace,
			"serviceAccount", serviceAccount.Name)
		return admission.Allowed(fmt.Sprintf("could not read the DatabricksAccount: %v", err))
	}
	if databricksAccount.Spec.Serves(namespace) {
		return admission.Allowed("this account serves this namespace")
	}

	return admission.Denied(notServedMessage(
		introduced, namespace, serviceAccount.Name, v.DatabricksAccountNamespacedName))
}

// introducedBy is what this write asks of this operator that the object was not
// already asking.
//
// Read off oldObject rather than off anything remembered, because the API server
// is the only thing that knows what was there a moment ago and it sends it. On a
// CREATE there is no oldObject, so everything asked for is introduced.
//
// A removal cannot appear here at all: it takes a key away, so what is left is a
// subset of what was there. That is what makes taking the annotation off always
// work, in every namespace, including one that was refused -- and it is the only
// way out of a namespace this operator no longer serves.
func (v *ServiceAccountRequestRefuser) introducedBy(
	req admission.Request, asking dbxv1alpha1.Requested) []dbxv1alpha1.Request {
	var already dbxv1alpha1.Requested
	if len(req.OldObject.Raw) > 0 {
		old := &corev1.ServiceAccount{}
		if err := v.Decoder.DecodeRaw(req.OldObject, old); err != nil {
			// Nothing is known about what was there, so nothing is known about
			// what changed. Read as everything having been introduced, this
			// would refuse the removal of an annotation, which is the one write
			// that must always go through.
			return nil
		}
		already = dbxv1alpha1.RequestsFor(old.Annotations, v.DatabricksAccountNamespacedName)
	}

	var introduced []dbxv1alpha1.Request
	for _, request := range asking.Understood {
		if !slices.Contains(already.Understood, request) {
			introduced = append(introduced, request)
		}
	}
	return introduced
}

// notServedMessage is the whole value of this webhook: it is read by somebody
// who has just been stopped and has no idea why, and it has to leave them
// knowing which object to go and edit and in which order.
//
// The order is the part that cannot be left out. Adding the namespace and
// writing the annotation are not two halves of one act that may be done either
// way round: taking a namespace off spec.namespaces destroys every identity in
// it, so the list is this account's statement of where it has identities, and an
// annotation written before the namespace is on it is a request addressed to an
// operator that has not been told it works here.
func notServedMessage(introduced []dbxv1alpha1.Request, namespace, serviceAccount string,
	databricksAccount types.NamespacedName) string {
	keys := make([]string, 0, len(introduced))
	for _, request := range introduced {
		keys = append(keys, dbxv1alpha1.ServicePrincipalAnnotationFor(request.Name))
	}

	return fmt.Sprintf("ServiceAccount %s/%s asks DatabricksAccount %s for a Databricks identity "+
		"through %s, and %s is not one of the namespaces that account names. Add %s to "+
		"spec.namespaces of DatabricksAccount %s first, and write the annotation after that. "+
		"In that order, because the list is what says this account has identities in a namespace: "+
		"taking a namespace off it destroys every identity there. Written now, the annotation "+
		"would be answered by nothing at all -- no DatabricksServiceAccount, no condition and no "+
		"event -- and the workloads here would go on failing every call to Databricks with an "+
		"error naming Databricks.",
		namespace, serviceAccount, databricksAccount, strings.Join(keys, ", "), namespace,
		namespace, databricksAccount)
}
