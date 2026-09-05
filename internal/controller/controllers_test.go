package controller

import (
	"context"
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
func (c *controllers) records(t *testing.T) {
	t.Helper()
	var list dbxv1alpha1.IssuedDatabricksServicePrincipalList
	if err := c.Client.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	for i := range list.Items {
		if _, err := c.Issued.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&list.Items[i]),
		}); err != nil {
			t.Fatalf("reconciling record %s: %v", list.Items[i].Name, err)
		}
	}
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
