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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

// TestOnlyThisOperatorsOwnKindsAreScoped covers the one setting that keeps two
// operators apart, and the reason it cannot be checked anywhere else.
//
// Every other test in this package calls Reconcile directly, which answers what
// the controller does with what it is handed and says nothing about what it is
// handed. Scoping is what decides that, it is set once in the assembly, and an
// entry dropped from here is an operator that works and quietly watches another
// operator's objects -- while holding an account admin credential for a different
// Databricks account.
func TestOnlyThisOperatorsOwnKindsAreScoped(t *testing.T) {
	t.Parallel()
	const ours = "operators"
	options := CacheOptions(ours)

	for _, kind := range []client.Object{
		&dbxv1alpha1.DatabricksAccount{},
		&dbxv1alpha1.IssuedDatabricksServicePrincipal{},
	} {
		by, ok := scoping(options.ByObject, kind)
		if !ok {
			t.Errorf("%T is not scoped; this operator watches every namespace's", kind)
			continue
		}
		if len(by.Namespaces) != 1 {
			t.Errorf("%T is scoped to %d namespaces, want only this operator's", kind, len(by.Namespaces))
			continue
		}
		if _, ok := by.Namespaces[ours]; !ok {
			t.Errorf("%T is scoped to %v, want %q", kind, by.Namespaces, ours)
		}
	}

	// Pods are the other reason this exists, and they are not scoped: a pod
	// running under an identity is in the tenant's namespace, not here. What is
	// done to them instead is dropping everything that is not read.
	pods, ok := scoping(options.ByObject, &corev1.Pod{})
	if !ok || pods.Transform == nil {
		t.Error("pods are held whole; they are the most numerous thing in a cluster and almost " +
			"nothing they carry is read here")
	}
	if len(pods.Namespaces) != 0 {
		t.Errorf("pods are scoped to %v; they run in the namespaces this operator serves, "+
			"not in its own", pods.Namespaces)
	}
}

// scoping finds what was said about one kind.
//
// By type rather than by the key itself: the map is keyed on an operator, and
// the operator a test writes is a different pointer from the one the assembly
// wrote. Looking it up directly answers "not present" for everything, which is
// the shape of a test that passes whatever the code does.
func scoping(byObject map[client.Object]cache.ByObject, kind client.Object) (cache.ByObject, bool) {
	want := fmt.Sprintf("%T", kind)
	for key, value := range byObject {
		if fmt.Sprintf("%T", key) == want {
			return value, true
		}
	}
	return cache.ByObject{}, false
}
