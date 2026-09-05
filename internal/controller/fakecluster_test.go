package controller

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	dbx "github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

const (
	operatorNamespace     = "databricks-operator-system"
	databricksAccountName = "databricks-account"
	testAccountID         = "aaaaaaaa-0000-0000-0000-000000000000"
	testClientID          = "bbbbbbbb-0000-0000-0000-000000000000"

	testNamespace = "team-a"
	testName      = "etl"
	testIssuer    = "https://oidc.example/cluster"
	testAudience  = "databricks"
)

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
// Neither moves without a person: an operator removing federation policies in
// the namespace suspends minting under a key of its own and gives it back
// afterwards.
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

// testOperatorRef names the operator these tests are, the way a ServiceAccount
// names one: by the DatabricksAccount it acts on.
var testOperatorRef = types.NamespacedName{Namespace: operatorNamespace, Name: databricksAccountName}

// asking is a ServiceAccount whose bare key names this operator, which is the
// request for its unnamed identity.
func asking(namespace, name string) *corev1.ServiceAccount {
	return askingFor(namespace, name, "")
}

// askingFor is a ServiceAccount naming this operator once per identity: one
// annotation key each, which is what makes no longer asking for one and
// mistyping one different edits.
func askingFor(namespace, name string, identities ...string) *corev1.ServiceAccount {
	serviceAccount := serviceAccountNamed(namespace, name)
	serviceAccount.Annotations = make(map[string]string, len(identities))
	for _, identity := range identities {
		serviceAccount.Annotations[dbxv1alpha1.ServicePrincipalAnnotationFor(identity)] =
			testOperatorRef.String()
	}
	return serviceAccount
}

// askingOf is the same with the bare key's value written out, for tests about
// what it says rather than about what follows from it.
func askingOf(namespace, name, value string) *corev1.ServiceAccount {
	serviceAccount := serviceAccountNamed(namespace, name)
	serviceAccount.Annotations = map[string]string{dbxv1alpha1.ServicePrincipalAnnotation: value}
	return serviceAccount
}

// stopsAsking takes the annotation off, which is the only thing that revokes.
// The opposite of asking, and named for it: what a ServiceAccount takes back is
// its request, which is a different act from the account removing the
// federation policies in a namespace.
func stopsAsking(t *testing.T, c client.Client) {
	t.Helper()
	serviceAccount := serviceAccountNamed(testNamespace, testName)
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testName}, serviceAccount); err != nil {
		t.Fatal(err)
	}
	serviceAccount.Annotations = nil
	if err := c.Update(context.Background(), serviceAccount); err != nil {
		t.Fatal(err)
	}
}

// testRecords is the operator's own namespace, where the records of what it
// issued are kept. It is not a tenant's, and that is the whole reason a record
// survives a tenant's namespace being torn down.
//
// The same namespace the DatabricksAccount is in, because that is what the
// operator is given: both come from its own POD_NAMESPACE. A fixture that told
// them apart would let the account controller list records it would find in a
// cluster and find none here.
const testRecords = operatorNamespace

func databricksAccountNamed(name string) *dbxv1alpha1.DatabricksAccount {
	return &dbxv1alpha1.DatabricksAccount{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: operatorNamespace},
		Spec: dbxv1alpha1.DatabricksAccountSpec{
			Host:      "https://accounts.cloud.databricks.com",
			AccountID: testAccountID,
			ClientID:  testClientID,
		},
	}
}

// databricksAccountServing is this operator's DatabricksAccount, declaring which
// namespaces it will act in.
//
// Supplied by a test that is about the reach itself. Every other test gets the
// one newControllers seeds, which serves testNamespace: an operator that serves
// nothing does nothing, so without a default every test in this package would be
// asserting about a closed door.
func databricksAccountServing(namespaces ...string) *dbxv1alpha1.DatabricksAccount {
	served := databricksAccountNamed(databricksAccountName)
	served.Spec.Namespaces = namespaces
	return served
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

func rawPodRunningAs(serviceAccount string, volumes ...corev1.Volume) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "runner-" + serviceAccount, Namespace: testNamespace,
		},
		Spec:   corev1.PodSpec{ServiceAccountName: serviceAccount, Volumes: volumes},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// cached is a pod as the reconciler actually receives it.
//
// Production reads pods from an informer that has put every one of them through
// TrimPod; a test that hands the reconciler a whole pod is testing a shape that
// never reaches it. A check that reads a field the trim drops passes here and
// reports every pod in a real cluster wrongly.
//
// So every pod fixture goes through here. It is not a convenience -- it is the
// only thing that makes the trim and the checks that depend on it fail together.
func cached(pod *corev1.Pod) *corev1.Pod {
	trimmed, err := TrimPod(pod)
	if err != nil {
		panic(err) // TrimPod cannot fail for a *corev1.Pod
	}
	return trimmed.(*corev1.Pod)
}

// writeToken writes a projected token carrying the given claims, so that the
// controller reads them the way it will in a cluster rather than being handed
// them.
func writeToken(t *testing.T, subject, audience string) string {
	return writeTokenAs(t, "https://oidc.example/id/X", subject, audience)
}

// writeTokenAs is the same, with the issuer said out loud. The issuer is what
// every federation policy names, so a test about one has to choose it.
func writeTokenAs(t *testing.T, issuer, subject, audience string) string {
	t.Helper()
	payload := `{"iss":"` + issuer + `","sub":"` + subject + `","aud":["` + audience + `"]}`
	token := "header." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".signature"
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
