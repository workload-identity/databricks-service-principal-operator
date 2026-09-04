package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"testing"

	"github.com/databricks/databricks-sdk-go"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	dbx "github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

// stubClients answers from fields and records what it was asked, so a test can
// say what the operator should have asked Databricks for without a stub having
// to model Databricks.
type stubClients struct {
	// What the operator asked Databricks for, in the order the calls were made.
	// That is the operator's call order and not an order anybody wrote: the
	// request is a set of annotation keys, and a map has none.
	created  []string
	deleted  []string
	policies []string

	// accountID is the Databricks account this stub acts in.
	accountID string

	// What a lookup finds, and what was looked for.
	foundID       string
	foundClientID string
	looked        []string

	// err fails every call.
	err error

	// deleteErr fails only DeleteServicePrincipal, so a test can see what an
	// object on its way out does when the one call it owes does not land.
	deleteErr error

	// policyErr fails only EnsureFederationPolicy, so a test can arrange the one
	// order that matters: a service principal made, and something after it not.
	policyErr error

	// removeErr fails only RemoveFederationPolicies, so a test can see what a
	// withdrawal that did not land says about itself, and that it does not say
	// it is finished.
	removeErr error

	// withdrawn is every service principal this operator asked to have the trust
	// taken off, in the order it asked.
	withdrawn []string

	// policyPanics stands in for the process dying inside that call. Nothing
	// after it runs -- which is the only way "written down before the next call"
	// differs from "written down by the end of the pass".
	policyPanics bool

	// gone is an id Databricks no longer has, so a test can see what happens
	// when what is recorded is not there any more.
	gone string

	// What the next create answers with.
	newServicePrincipalID string
	newClientID           string
}

func (s *stubClients) AccountClient() *databricks.AccountClient { panic("not used") }

// Snapshot returns this stub. What the Holder does here is the thing being
// stood in for.
func (s *stubClients) Snapshot() dbx.Clients { return s }

// accountID is where this stub pretends to be acting. Empty means the stub was
// not told, and testAccountID is what it answers then -- a configured operator
// always knows which account it is in, and tests that leave this out are not
// about that.
func (s *stubClients) AccountID() string {
	if s.accountID == "" {
		return testAccountID
	}
	return s.accountID
}

// found is what the next lookup answers with: an id and applicationId to adopt,
// or empty for nothing there.
func (s *stubClients) FindServicePrincipal(_ context.Context, issuing dbx.Issuing) (
	string, string, bool, error) {
	if s.err != nil {
		return "", "", false, s.err
	}
	s.looked = append(s.looked, issuing.Namespace+"/"+issuing.Name)
	if s.foundID == "" {
		return "", "", false, nil
	}
	return s.foundID, s.foundClientID, true, nil
}

func (s *stubClients) CreateServicePrincipal(_ context.Context, issuing dbx.Issuing) (string, string, error) {
	if s.err != nil {
		return "", "", s.err
	}
	// Recorded per identity rather than per ServiceAccount, so a failure names
	// which one. Two identities of one ServiceAccount are two creates, and
	// "created [team-a/etl team-a/etl]" would say nothing about which.
	asked := issuing.Namespace + "/" + issuing.Name
	if issuing.Identity != "" {
		asked += "." + issuing.Identity
	}
	s.created = append(s.created, asked)

	if s.newServicePrincipalID != "" {
		return s.newServicePrincipalID, s.newClientID, nil
	}
	// The first is 7788 because that is the id every single-identity test in
	// this package reads. The ones after it differ, because two identities
	// sharing an id would let a test pass that had only made one.
	if len(s.created) == 1 {
		return "7788", "app-uuid", nil
	}
	return fmt.Sprintf("7788%d", len(s.created)), fmt.Sprintf("app-uuid-%d", len(s.created)), nil
}

func (s *stubClients) ServicePrincipalExists(_ context.Context, id string) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	if s.gone == id {
		return false, nil
	}
	return true, nil
}

func (s *stubClients) DeleteServicePrincipal(_ context.Context, id string) error {
	if s.err != nil {
		return s.err
	}
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.deleted = append(s.deleted, id)
	return nil
}

func (s *stubClients) EnsureFederationPolicy(_ context.Context, servicePrincipalID, issuer, subject, audience string) error {
	if s.err != nil {
		return s.err
	}
	if s.policyErr != nil {
		return s.policyErr
	}
	if s.policyPanics {
		panic("the operator stopped here")
	}
	// Recorded once however often it is asked for. The real call lists the
	// policies on that service principal and adds nothing when the subject is
	// already trusted, so a stub that counted repeats would have tests asserting
	// against a behaviour Databricks does not have.
	policy := servicePrincipalID + " " + issuer + " " + subject + " " + audience
	if !slices.Contains(s.policies, policy) {
		s.policies = append(s.policies, policy)
	}
	return nil
}

// RemoveFederationPolicies drops what EnsureFederationPolicy recorded, matched
// the way the real one matches: on the service principal, the issuer and the
// subject, and not on the audience -- a policy naming an audience this operator
// no longer hands out is still one a token minted for it satisfies.
func (s *stubClients) RemoveFederationPolicies(_ context.Context, servicePrincipalID, issuer, subject string) error {
	if s.err != nil {
		return s.err
	}
	if s.removeErr != nil {
		return s.removeErr
	}
	s.withdrawn = append(s.withdrawn, servicePrincipalID)
	prefix := servicePrincipalID + " " + issuer + " " + subject + " "
	s.policies = slices.DeleteFunc(s.policies, func(policy string) bool {
		return strings.HasPrefix(policy, prefix)
	})
	return nil
}

var _ dbx.Clients = (*stubClients)(nil)

func newFakeClient(t *testing.T, objects ...client.Object) (client.Client, *runtime.Scheme) {
	t.Helper()
	return newFakeClientWith(t, interceptor.Funcs{}, objects...)
}

// newFakeClientWith is the same cluster with some calls answered by the test.
// It is how a write the API server refuses is arranged, which is the only way to
// see what this operator says about work it could not do.
func newFakeClientWith(t *testing.T, funcs interceptor.Funcs,
	objects ...client.Object) (client.Client, *runtime.Scheme) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := dbxv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(
			&dbxv1alpha1.DatabricksServiceAccount{},
			&dbxv1alpha1.IssuedDatabricksServicePrincipal{},
			&dbxv1alpha1.DatabricksAccount{},
		).
		WithInterceptorFuncs(funcs).
		Build(), scheme
}

// namespaceNamed is a plain Namespace, which is what a ServiceAccount in it
// asking for an identity is refused against.
func namespaceNamed(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

// mintingNamespace is a namespace as the operator instructions produce one: it
// may mint identities, and pods created in it are given their token. Both
// labels, because that is what a namespace in use carries -- one without inject
// mints identities no pod is ever equipped with.
//
// Only somebody with cluster-wide access can open either, which is what stops
// holding a namespace from being the power to mint identities without limit.
// Neither moves without a person: an operator withdrawing from the namespace
// suspends minting under a key of its own and gives it back afterwards.
func mintingNamespace(name string) *corev1.Namespace {
	namespace := namespaceNamed(name)
	namespace.Labels = map[string]string{
		dbxv1alpha1.MintLabel:   dbxv1alpha1.Enabled,
		dbxv1alpha1.InjectLabel: dbxv1alpha1.Enabled,
	}
	return namespace
}

// mintingWithoutInjecting is the half-labelled namespace: identities are made
// here and no pod created here is ever given one.
func mintingWithoutInjecting(name string) *corev1.Namespace {
	namespace := namespaceNamed(name)
	namespace.Labels = map[string]string{dbxv1alpha1.MintLabel: dbxv1alpha1.Enabled}
	return namespace
}

// serviceAccountNamed is one that has not asked for anything.
func serviceAccountNamed(namespace, name string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace,
			// A UID, because ownership is compared by UID and the fake client
			// assigns none. Two objects with no UID read as the same owner.
			UID: types.UID(namespace + "/" + name),
		},
	}
}

// testOperator is the operator these tests are, named the way a ServiceAccount
// names one: by the DatabricksAccount it acts on.
var testOperator = types.NamespacedName{Namespace: operatorNamespace, Name: accountObject}

// asking is a ServiceAccount whose bare key names this operator, which is the
// request for its unnamed identity.
func asking(namespace, name string) *corev1.ServiceAccount {
	return askingFor(namespace, name, "")
}

// askingOf is the same with the bare key's value written out, for tests about
// what it says rather than about what follows from it.
func askingOf(namespace, name, value string) *corev1.ServiceAccount {
	serviceAccount := serviceAccountNamed(namespace, name)
	serviceAccount.Annotations = map[string]string{dbxv1alpha1.ServicePrincipalAnnotation: value}
	return serviceAccount
}

// testRecords is the operator's own namespace, where the records of what it
// issued are kept. It is not a tenant's, and that is the whole reason a record
// survives a tenant's namespace being torn down.
//
// The same namespace the DatabricksAccount is in, because that is what the
// operator is given: both come from its own POD_NAMESPACE. A harness that told
// them apart would let the account controller list records it would find in a
// cluster and find none here.
const testRecords = operatorNamespace

// harness is both controllers over one fake cluster.
//
// They are run together because neither is the feature on its own: one decides
// that an identity should exist and shows what exists, the other makes it exist
// and destroys it. A test that ran only one would be asserting about half of a
// sentence.
type harness struct {
	Projection *DatabricksServiceAccountReconciler
	Issued     *IssuedDatabricksServicePrincipalReconciler
	Client     client.Client
	Stub       *stubClients
}

// accountServing is this operator's DatabricksAccount, declaring which
// namespaces it will act in.
//
// Supplied by a test that is about the reach itself. Every other test gets the
// one newHarness seeds, which serves testNamespace: an operator that serves
// nothing does nothing, so without a default every test in this package would be
// asserting about a closed door.
func accountServing(namespaces ...string) *dbxv1alpha1.DatabricksAccount {
	served := databricksAccountNamed(accountObject)
	served.Spec.Namespaces = namespaces
	return served
}

func newHarness(t *testing.T, stub *stubClients, objects ...client.Object) *harness {
	t.Helper()
	return newHarnessWith(t, stub, interceptor.Funcs{}, objects...)
}

// newHarnessWith is the same over a cluster that answers some calls itself.
func newHarnessWith(t *testing.T, stub *stubClients, funcs interceptor.Funcs,
	objects ...client.Object) *harness {
	t.Helper()
	if !slices.ContainsFunc(objects, func(object client.Object) bool {
		_, is := object.(*dbxv1alpha1.DatabricksAccount)
		return is
	}) {
		objects = append(objects, accountServing(testNamespace))
	}
	c, scheme := newFakeClientWith(t, funcs, objects...)
	// A real file, because the reconciler holds the path and reads it. Holding
	// the reading instead is what made one bad moment at startup permanent.
	token := writeTokenAs(t, testIssuer, "system:serviceaccount:operators:controller-manager", testAudience)
	return &harness{
		Projection: &DatabricksServiceAccountReconciler{
			Client:                          c,
			Scheme:                          scheme,
			DatabricksAccountNamespacedName: testOperator,
		},
		Issued: &IssuedDatabricksServicePrincipalReconciler{
			Client:                          c,
			Scheme:                          scheme,
			DatabricksAccountNamespacedName: testOperator,
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
// the projection copying the result is a third. Hiding that behind one call
// would be hiding the ordering the whole design rests on.
func (h *harness) settle(t *testing.T) {
	t.Helper()
	for range 4 {
		h.project(t, testNamespace, testName)
		h.records(t)
	}
}

// project runs the projection for one ServiceAccount.
func (h *harness) project(t *testing.T, namespace, name string) {
	t.Helper()
	if _, err := h.Projection.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
	}); err != nil {
		t.Fatalf("projecting %s/%s: %v", namespace, name, err)
	}
}

// records runs the record controller over every record there is.
func (h *harness) records(t *testing.T) {
	t.Helper()
	var list dbxv1alpha1.IssuedDatabricksServicePrincipalList
	if err := h.Client.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	for i := range list.Items {
		if _, err := h.Issued.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&list.Items[i]),
		}); err != nil {
			t.Fatalf("reconciling record %s: %v", list.Items[i].Name, err)
		}
	}
}

// issuedOf returns the record for one ServiceAccount, or nil if there is none.
func (h *harness) issuedOf(t *testing.T,
	serviceAccount *corev1.ServiceAccount) *dbxv1alpha1.IssuedDatabricksServicePrincipal {
	t.Helper()
	var issued dbxv1alpha1.IssuedDatabricksServicePrincipal
	err := h.Client.Get(context.Background(), types.NamespacedName{
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

// recordFor is a record as the operator would have written it, for tests that
// start from an identity that already exists.
func recordFor(serviceAccount *corev1.ServiceAccount) *dbxv1alpha1.IssuedDatabricksServicePrincipal {
	return &dbxv1alpha1.IssuedDatabricksServicePrincipal{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  testRecords,
			Name:       dbxv1alpha1.IssuedNameFor(serviceAccount.Namespace, serviceAccount.Name, "", serviceAccount.UID),
			Finalizers: []string{dbxv1alpha1.ServicePrincipalFinalizer},
		},
		Spec: dbxv1alpha1.IssuedDatabricksServicePrincipalSpec{
			Subject: dbx.SubjectFor(serviceAccount.Namespace, serviceAccount.Name),
			ServiceAccount: dbxv1alpha1.IssuedServiceAccount{
				Namespace: serviceAccount.Namespace,
				Name:      serviceAccount.Name,
				UID:       serviceAccount.UID,
			},
		},
	}
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
