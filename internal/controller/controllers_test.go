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
	"errors"
	"fmt"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

// controllers is both controllers over one fake cluster, answered by one stub
// client.
//
// They are run together because neither is the feature on its own: one decides
// that an identity should exist and shows what exists, the other makes it exist
// and destroys it. A test that ran only one would be asserting about half of a
// sentence.
type controllers struct {
	DatabricksServiceAccounts *DatabricksServiceAccountReconciler
	Issued                    *IssuedDatabricksServicePrincipalReconciler
	Client                    client.Client
	Stub                      *stubClients
}

func newControllers(t *testing.T, stub *stubClients, objects ...client.Object) *controllers {
	t.Helper()
	return newControllersWith(t, stub, interceptor.Funcs{}, objects...)
}

// newControllersWith is the same over a cluster that answers some calls itself.
func newControllersWith(t *testing.T, stub *stubClients, funcs interceptor.Funcs,
	objects ...client.Object) *controllers {
	t.Helper()
	if !slices.ContainsFunc(objects, func(object client.Object) bool {
		_, is := object.(*dbxv1alpha1.DatabricksAccount)
		return is
	}) {
		objects = append(objects, databricksAccountServing(testNamespace))
	}
	c, scheme := newFakeClientWith(t, funcs, objects...)
	// A real file, because the reconciler holds the path and reads it. Holding
	// the reading instead is what made one bad moment at startup permanent.
	token := writeTokenAs(t, testIssuer, "system:serviceaccount:operators:controller-manager", testAudience)
	return &controllers{
		DatabricksServiceAccounts: &DatabricksServiceAccountReconciler{
			Client:                          c,
			Scheme:                          scheme,
			DatabricksAccountNamespacedName: testOperatorRef,
		},
		Issued: &IssuedDatabricksServicePrincipalReconciler{
			Client:                          c,
			Scheme:                          scheme,
			DatabricksAccountNamespacedName: testOperatorRef,
			// The fake client is both, because it has no cache to be behind.
			// What the live reader is for is asserted where it matters, not
			// here.
			Live:       c,
			Databricks: stub,
			TokenPath:  token,
		},
		Client: c,
		Stub:   stub,
	}
}

// settle runs both controllers until what they do stops changing.
//
// Several passes rather than one, because that is what happens in a cluster: a
// record is written before anything is created, the create is a second pass, and
// the DatabricksServiceAccount controller copying the result is a third. Hiding
// that behind one call would be hiding the ordering the whole design rests on.
func (c *controllers) settle(t *testing.T) {
	t.Helper()
	for range 4 {
		c.project(t, testNamespace, testName)
		c.records(t)
	}
}

// project runs the DatabricksServiceAccount controller for one ServiceAccount.
func (c *controllers) project(t *testing.T, namespace, name string) {
	t.Helper()
	if _, err := c.DatabricksServiceAccounts.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
	}); err != nil {
		t.Fatalf("projecting %s/%s: %v", namespace, name, err)
	}
}

// records runs the record controller over every record there is.
//
// A pass that returns a failure this test put in the stub is what is supposed to
// happen: an unreachable Databricks is handed back as an error so the workqueue
// backs the record off and counts it. Anything else ends the test, which is what
// this is for -- an API error from the fake cluster, or a failure nobody
// arranged, would otherwise be read as the outage the test is about.
func (c *controllers) records(t *testing.T) {
	t.Helper()
	for _, failed := range c.recordPasses(t) {
		if !c.arranged(failed) {
			t.Fatalf("reconciling a record: %v", failed)
		}
	}
}

// arranged reports whether a failure is one this test put in the stub.
func (c *controllers) arranged(err error) bool {
	for _, injected := range []error{c.Stub.err, c.Stub.deleteErr, c.Stub.policyErr} {
		if injected != nil && errors.Is(err, injected) {
			return true
		}
	}
	return false
}

// recordPasses runs the same passes and hands back what each one returned, for
// the test that is about the returning rather than about what it reports.
func (c *controllers) recordPasses(t *testing.T) []error {
	t.Helper()
	var list dbxv1alpha1.IssuedDatabricksServicePrincipalList
	if err := c.Client.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	var failed []error
	for i := range list.Items {
		if _, err := c.Issued.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&list.Items[i]),
		}); err != nil {
			failed = append(failed, fmt.Errorf("record %s: %w", list.Items[i].Name, err))
		}
	}
	return failed
}

func reconcileDatabricksAccount(t *testing.T, r *DatabricksAccountReconciler, name string) {
	t.Helper()
	if err := databricksAccountPass(t, r, name); err != nil {
		t.Fatalf("reconciling %s: %v", name, err)
	}
}

// databricksAccountPass is the same pass for a test that has arranged a failure
// Databricks cannot answer: those are handed back as errors so the workqueue
// backs the account off and counts them, and a test about one is a test where
// the pass returns something.
func databricksAccountPass(t *testing.T, r *DatabricksAccountReconciler, name string) error {
	t.Helper()
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: operatorNamespace, Name: name},
	})
	return err
}

// issuedOf returns the record for one ServiceAccount, or nil if there is none.
func (c *controllers) issuedOf(t *testing.T,
	serviceAccount *corev1.ServiceAccount) *dbxv1alpha1.IssuedDatabricksServicePrincipal {
	t.Helper()
	var issued dbxv1alpha1.IssuedDatabricksServicePrincipal
	err := c.Client.Get(context.Background(), types.NamespacedName{
		Namespace: testRecords,
		Name:      dbxv1alpha1.IssuedNameFor(serviceAccount.Namespace, serviceAccount.Name, "", serviceAccount.UID),
	}, &issued)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return &issued
}

// principalOf returns the DatabricksServiceAccount, or nil if there is none.
func principalOf(t *testing.T, c client.Client) *dbxv1alpha1.DatabricksServiceAccount {
	t.Helper()
	var got dbxv1alpha1.DatabricksServiceAccount
	err := c.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testName}, &got)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return &got
}

// identityIn is the first entry of a DatabricksServiceAccount.
//
// Tests written when a DatabricksServiceAccount held one identity read its
// fields off the object. They now read them off an entry, and reading the first
// is what those tests always meant: they ask through one key and get one entry.
//
// Not "the identity its owner named first" -- there is no such thing any more,
// the request being a set of annotation keys. With one entry there is nothing
// for an order to decide, which is the only case this helper is used in.
func identityIn(t *testing.T, principal *dbxv1alpha1.DatabricksServiceAccount) dbxv1alpha1.ProjectedIdentity {
	t.Helper()
	if principal == nil {
		t.Fatal("no DatabricksServiceAccount to read an identity from")
	}
	identity, issued := principal.Status.First()
	if !issued {
		t.Fatalf("%s/%s carries no identity", principal.Namespace, principal.Name)
	}
	return identity
}
