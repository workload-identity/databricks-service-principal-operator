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
	"encoding/json"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

// thisOperator is the DatabricksAccount this operator acts on, split into the
// two halves the annotation value joins with a "/".
var thisOperator = types.NamespacedName{Namespace: "operators", Name: "databricks-account"}

// newRefuser builds the handler over an account naming exactly the namespaces
// given. No account at all is what nil asks for, which is the case where this
// operator has been told nothing.
func newRefuser(t *testing.T, serves []string) *ServiceAccountRequestRefuser {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := dbxv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	var objects []client.Object
	if serves != nil {
		account := &dbxv1alpha1.DatabricksAccount{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: thisOperator.Namespace,
				Name:      thisOperator.Name,
			},
		}
		account.Spec.Namespaces = serves
		objects = append(objects, account)
	}

	return &ServiceAccountRequestRefuser{
		Client:                          fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
		Decoder:                         admission.NewDecoder(scheme),
		DatabricksAccountNamespacedName: thisOperator,
	}
}

// serviceAccountWith is a ServiceAccount in the test namespace carrying exactly
// these annotations.
func serviceAccountWith(annotations map[string]string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   testNamespace,
			Name:        testAccount,
			Annotations: annotations,
		},
	}
}

// asksThisOperator is the bare key: the request for this operator's unnamed
// identity, which is the whole of the common case.
func asksThisOperator() map[string]string {
	return map[string]string{dbxv1alpha1.ServicePrincipalAnnotation: thisOperator.String()}
}

// creating is what the API server sends for a ServiceAccount being created:
// there is no oldObject, so everything it asks for is being introduced.
func creating(t *testing.T, serviceAccount *corev1.ServiceAccount) admission.Request {
	t.Helper()
	return admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Namespace: serviceAccount.Namespace,
			Object:    runtime.RawExtension{Raw: marshal(t, serviceAccount)},
		},
	}
}

// updating is what the API server sends for an edit, which is the only shape in
// which the difference between a state and a transition is visible.
func updating(t *testing.T, old, updated *corev1.ServiceAccount) admission.Request {
	t.Helper()
	return admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Update,
			Namespace: updated.Namespace,
			Object:    runtime.RawExtension{Raw: marshal(t, updated)},
			OldObject: runtime.RawExtension{Raw: marshal(t, old)},
		},
	}
}

func marshal(t *testing.T, serviceAccount *corev1.ServiceAccount) []byte {
	t.Helper()
	raw, err := json.Marshal(serviceAccount)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestAskingInANamespaceThisAccountDoesNotNameIsRefusedWhereSomebodyIsReading
// covers the whole reason this webhook exists.
//
// The controller already refuses to mint here, and refusing there says nothing:
// no entry, so no DatabricksServiceAccount, so no condition and no event. The
// person who wrote the annotation waits for an identity that is never coming and
// has nowhere to look for the reason.
//
// So the message is the thing under test, not the boolean. It has to leave
// somebody who has never read this repository knowing which object to edit and
// in which order.
func TestAskingInANamespaceThisAccountDoesNotNameIsRefusedWhereSomebodyIsReading(t *testing.T) {
	t.Parallel()
	refuser := newRefuser(t, []string{"somewhere-else"})

	response := refuser.Handle(context.Background(), creating(t, serviceAccountWith(asksThisOperator())))
	if response.Allowed {
		t.Fatal("the request was admitted; nothing will answer it and nothing will say so")
	}

	message := response.Result.Message
	for what, want := range map[string]string{
		"the namespace it was written in":  testNamespace,
		"the DatabricksAccount to edit":    thisOperator.String(),
		"the field on it":                  "spec.namespaces",
		"the annotation key that asked":    dbxv1alpha1.ServicePrincipalAnnotation,
		"what to do before the annotation": "first",
	} {
		if !strings.Contains(message, want) {
			t.Errorf("the refusal names no %s: it has to contain %q, and it is %q",
				what, want, message)
		}
	}
}

// TestAskingInANamespaceThisAccountNamesIsAdmitted is the case the operator was
// built for, and the one this webhook must not touch.
func TestAskingInANamespaceThisAccountNamesIsAdmitted(t *testing.T) {
	t.Parallel()
	refuser := newRefuser(t, []string{"elsewhere", testNamespace})

	response := refuser.Handle(context.Background(), creating(t, serviceAccountWith(asksThisOperator())))
	if !response.Allowed {
		t.Fatalf("a request in a namespace this account names was refused: %q",
			response.Result.Message)
	}
}

// TestTakingTheAnnotationOffIsNeverRefused covers the one write that has to work
// in every namespace, including one that was refused a moment ago. It is the
// only way out of a namespace this operator no longer serves.
//
// Both shapes, because only one of them is held by the transition check. Taking
// the last key off leaves an object asking for nothing, which is admitted by
// anything -- the API server does not even send it, since matchConditions reads
// object and not oldObject. Taking one key off a ServiceAccount that asked for
// two leaves an object still asking, and that one is admitted only because what
// is refused is the transition: a rule reading the state would refuse a person
// for giving an identity up.
func TestTakingTheAnnotationOffIsNeverRefused(t *testing.T) {
	t.Parallel()

	both := asksThisOperator()
	both[dbxv1alpha1.ServicePrincipalAnnotationFor("reader")] = thisOperator.String()

	for what, removal := range map[string][2]map[string]string{
		"the only identity asked for":     {asksThisOperator(), {"team": "platform"}},
		"one identity out of the two":     {both, asksThisOperator()},
		"the unnamed one, keeping a name": {both, {dbxv1alpha1.ServicePrincipalAnnotationFor("reader"): thisOperator.String()}},
	} {
		t.Run(what, func(t *testing.T) {
			t.Parallel()
			refuser := newRefuser(t, []string{"somewhere-else"})

			response := refuser.Handle(context.Background(), updating(t,
				serviceAccountWith(removal[0]), serviceAccountWith(removal[1])))
			if !response.Allowed {
				t.Fatalf("giving up %s was refused, so the ServiceAccount can never be put "+
					"right: %q", what, response.Result.Message)
			}
		})
	}
}

// TestAnEditBesideAnAnnotationThatWasAlreadyThereIsAdmitted covers the same
// distinction from the other side, and it is where refusing the state does real
// damage.
//
// A namespace taken off the account's list leaves every ServiceAccount in it
// still annotated. Refusing the state would make each of them permanently
// unwritable -- no label, no secret, no ownerReference, no controller able to
// touch it -- for a request that was made before the list changed.
func TestAnEditBesideAnAnnotationThatWasAlreadyThereIsAdmitted(t *testing.T) {
	t.Parallel()
	refuser := newRefuser(t, []string{"somewhere-else"})

	old := serviceAccountWith(asksThisOperator())
	edited := serviceAccountWith(asksThisOperator())
	edited.Secrets = []corev1.ObjectReference{{Name: "pull-token"}}

	response := refuser.Handle(context.Background(), updating(t, old, edited))
	if !response.Allowed {
		t.Fatalf("an edit that left the request untouched was refused, so every annotated "+
			"ServiceAccount in a namespace since taken off the list is now frozen: %q",
			response.Result.Message)
	}
}

// TestAnUpdateThatIntroducesTheAnnotationIsRefused covers how the annotation is
// actually written: onto a ServiceAccount that already exists.
//
// A rule that only looked at CREATE would refuse almost nothing.
func TestAnUpdateThatIntroducesTheAnnotationIsRefused(t *testing.T) {
	t.Parallel()
	refuser := newRefuser(t, []string{"somewhere-else"})

	request := updating(t,
		serviceAccountWith(map[string]string{"team": "platform"}),
		serviceAccountWith(asksThisOperator()))

	response := refuser.Handle(context.Background(), request)
	if response.Allowed {
		t.Fatal("annotating an existing ServiceAccount was admitted, which is how the annotation " +
			"is written nearly every time")
	}
}

// TestASecondIdentityAskedForBesideOneAlreadyThereIsRefused covers a transition
// hidden inside an object that was already asking.
//
// Both keys are this operator's and one of them is new, so what is introduced is
// the second identity alone -- and it is as unanswerable as the first would have
// been. Comparing the two sets by whether either is empty would admit it.
func TestASecondIdentityAskedForBesideOneAlreadyThereIsRefused(t *testing.T) {
	t.Parallel()
	refuser := newRefuser(t, []string{"somewhere-else"})

	both := asksThisOperator()
	both[dbxv1alpha1.ServicePrincipalAnnotationFor("reader")] = thisOperator.String()

	response := refuser.Handle(context.Background(),
		updating(t, serviceAccountWith(asksThisOperator()), serviceAccountWith(both)))
	if response.Allowed {
		t.Fatal("a second identity asked for beside one already there was admitted; it will be " +
			"answered by nothing, exactly like the first")
	}
	if key := dbxv1alpha1.ServicePrincipalAnnotationFor("reader"); !strings.Contains(
		response.Result.Message, key) {
		t.Errorf("the refusal does not name %q, which is the key that was just written: %q",
			key, response.Result.Message)
	}
}

// TestARequestNamingAnotherOperatorIsNotThisOnesToRefuse covers the limit on
// what this operator may speak for.
//
// Several operators serve one cluster, each with its own account and its own
// namespace list. A key naming another operator says nothing about whether this
// one serves the namespace, and refusing it would be this operator standing in
// front of somebody else's decision -- in a namespace that other operator may
// well name.
func TestARequestNamingAnotherOperatorIsNotThisOnesToRefuse(t *testing.T) {
	t.Parallel()
	refuser := newRefuser(t, []string{"somewhere-else"})

	response := refuser.Handle(context.Background(), creating(t, serviceAccountWith(
		map[string]string{dbxv1alpha1.ServicePrincipalAnnotation: "other-operators/databricks-account"})))
	if !response.Allowed {
		t.Fatalf("a request addressed to another operator was refused by this one: %q",
			response.Result.Message)
	}
}

// TestWithNoDatabricksAccountNothingIsRefused covers what this operator does
// before it has been told anything.
//
// Knowing nothing is not a reason to refuse somebody else's write. The
// controller is what enforces the namespace list; this only moves the refusal
// earlier, and a refusal it cannot justify would put this operator on the write
// path of every ServiceAccount in the cluster saying no on no evidence.
func TestWithNoDatabricksAccountNothingIsRefused(t *testing.T) {
	t.Parallel()
	refuser := newRefuser(t, nil)

	response := refuser.Handle(context.Background(), creating(t, serviceAccountWith(asksThisOperator())))
	if !response.Allowed {
		t.Fatalf("a request was refused by an operator with no DatabricksAccount to answer from: %q",
			response.Result.Message)
	}
}
