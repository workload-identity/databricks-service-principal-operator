package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	jsonpatch "github.com/evanphx/json-patch/v5"
	admissionv1 "k8s.io/api/admission/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	acv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/applyconfiguration/api/v1alpha1"
	dbx "github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
	dbxwebhook "github.com/workload-identity/databricks-service-principal-operator/internal/webhook"
)

// These run the controller under a real manager, against a real API server, and
// then do nothing but write objects and wait.
//
// Every other test in this package calls Reconcile itself, which answers "what
// does it do once woken" and says nothing about what wakes it. That gap is not
// theoretical: a namespace being enabled changed the answer for every
// ServiceAccount in it, was an event on none of them, and nothing was watching
// namespaces. Sixty-four tests passed and the operator did nothing on a real
// cluster until an unrelated pod happened to be created.
//
// So these assert only through the API server: the trigger goes in, and the
// object either appears or does not.
var _ = Describe("what wakes the controller", Ordered, func() {
	var (
		gate    *CacheSynced
		running ctrl.Manager
	)

	const (
		wiredNamespace = "wiring"
		wiredAccount   = "etl"
		wiredRecords   = "operators"
	)

	BeforeAll(func() {
		manager, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme: k8sClient.Scheme(),
			// The same scoping the binary runs with. Without it these specs
			// would pass against a cache no operator ever has.
			Cache: CacheOptions(wiredRecords),
			// Nothing is served: a metrics listener would collide with anything
			// else bound in this test binary, and none of these tests read it.
			Metrics: metricsserver.Options{BindAddress: "0"},
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: wiredRecords},
		})).To(Succeed())

		// Serving nothing to begin with. What this suite is about is which edit
		// wakes what, so every answer starts closed and is opened by its own
		// owner in its own step.
		Expect(k8sClient.Create(ctx, &dbxv1alpha1.DatabricksAccount{
			ObjectMeta: metav1.ObjectMeta{Namespace: wiredRecords, Name: "databricks-account"},
			Spec: dbxv1alpha1.DatabricksAccountSpec{
				Host:      "https://accounts.cloud.databricks.com",
				AccountID: "the-account",
				ClientID:  "11111111-1111-1111-1111-111111111111",
			},
		})).To(Succeed())

		// Written here rather than through the testing.T helper: these specs do
		// not have one, and what matters is that the reconciler is given a path
		// it can read, the way the binary is.
		payload := `{"iss":"https://oidc.example/cluster",` +
			`"sub":"system:serviceaccount:operators:controller-manager","aud":["databricks"]}`
		token := "header." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".signature"
		tokenFile := filepath.Join(GinkgoT().TempDir(), "token")
		Expect(os.WriteFile(tokenFile, []byte(token), 0o600)).To(Succeed())

		projection := &DatabricksServiceAccountReconciler{
			Client:                          manager.GetClient(),
			Scheme:                          manager.GetScheme(),
			Records:                         wiredRecords,
			DatabricksAccountNamespacedName: types.NamespacedName{Namespace: wiredRecords, Name: "databricks-account"},
		}
		Expect(projection.SetupWithManager(manager)).To(Succeed())

		// The real Holder, with no DatabricksAccount to build clients from, so
		// nothing here reaches Databricks. What these specs are about is what
		// wakes a controller, which does not depend on the far side answering.
		issued := &IssuedDatabricksServicePrincipalReconciler{
			Client:                          manager.GetClient(),
			Scheme:                          manager.GetScheme(),
			Live:                            manager.GetAPIReader(),
			Databricks:                      dbx.NewHolder(wiredRecords, "databricks-account"),
			DatabricksAccountNamespacedName: types.NamespacedName{Namespace: wiredRecords, Name: "databricks-account"},
			TokenPath:                       tokenFile,
		}
		Expect(issued.SetupWithManager(manager, wiredRecords)).To(Succeed())

		gate = &CacheSynced{}
		Expect(manager.Add(gate)).To(Succeed())
		Expect(gate.Check(nil)).To(HaveOccurred(),
			"ready before the manager was started; the pod would join the webhook Service "+
				"with an empty cache and admit every pod unchanged")

		running = manager
		go func() {
			defer GinkgoRecover()
			Expect(manager.Start(ctx)).To(Succeed())
		}()
		Expect(manager.GetCache().WaitForCacheSync(ctx)).To(BeTrue())
	})

	It("reports ready once the manager has started it, and not before", func() {
		// Weaker than it looks and worth saying so: what this catches is the
		// gate never being started at all -- added to the wrong group, or an
		// Add that failed. That the manager starts it only after the caches have
		// synced is controller-runtime's ordering, and this cannot see the
		// difference between "after" and "alongside".
		Eventually(func() error {
			return gate.Check(nil)
		}, 20*time.Second, 100*time.Millisecond).Should(Succeed(),
			"the readiness gate never opened; this pod would never join the webhook Service")
		Expect(running.GetCache().WaitForCacheSync(ctx)).To(BeTrue())
	})

	It("does nothing for a ServiceAccount in a namespace nobody enabled", func() {
		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: wiredNamespace},
		})).To(Succeed())

		Expect(k8sClient.Create(ctx, &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name: wiredAccount, Namespace: wiredNamespace,
				Annotations: map[string]string{
					dbxv1alpha1.ServicePrincipalAnnotation: wiredRecords + "/databricks-account",
				},
			},
		})).To(Succeed())

		// Consistently rather than Eventually: the assertion is that nothing
		// appears, and a single look would pass before the controller had a
		// chance to act.
		//
		// One second, not three. It has to outlast a controller that would mint
		// immediately, which is the shape of the code it guards -- the namespace
		// is read on the same pass as the request. It would not catch minting
		// deferred past a second, and nothing here defers anything. The other
		// two seconds were a third of this suite's running time.
		Consistently(func() bool {
			return recordedIdentity(wiredNamespace, wiredAccount) == nil
		}, time.Second, 100*time.Millisecond).Should(BeTrue(),
			"an identity was minted in a namespace nobody allowed to ask")
	})

	It("does nothing once the namespace is enabled but this operator serves it not", func() {
		// This is the order a team is onboarded in: the ServiceAccounts are
		// annotated first, and somebody with cluster-wide access enables the
		// namespace afterwards. Nothing touches the ServiceAccount again.
		namespace := &corev1.Namespace{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: wiredNamespace}, namespace)).To(Succeed())
		namespace.Labels = map[string]string{dbxv1alpha1.MintLabel: dbxv1alpha1.Enabled}
		Expect(k8sClient.Update(ctx, namespace)).To(Succeed())

		// The cluster has said this namespace may be served by somebody. It has
		// not said by whom, and it cannot: only the team holding the account
		// admin credential can commit it.
		Consistently(func() bool {
			return recordedIdentity(wiredNamespace, wiredAccount) == nil
		}, time.Second, 100*time.Millisecond).Should(BeTrue(),
			"an identity was minted in a namespace this operator was never told to serve")
	})

	It("wakes when this operator's reach is widened, without anything else changing", func() {
		// The last of the three answers, given by the team that owns the
		// Databricks account, on their own object. It is an event on no
		// ServiceAccount and on no Namespace.
		databricksAccount := &dbxv1alpha1.DatabricksAccount{}
		Expect(k8sClient.Get(ctx,
			types.NamespacedName{Namespace: wiredRecords, Name: "databricks-account"},
			databricksAccount)).To(Succeed())
		databricksAccount.Spec.Namespaces = []string{wiredNamespace}
		Expect(k8sClient.Update(ctx, databricksAccount)).To(Succeed())

		Eventually(func() *dbxv1alpha1.DatabricksServiceAccount {
			return recordedIdentity(wiredNamespace, wiredAccount)
		}, 20*time.Second, 250*time.Millisecond).ShouldNot(BeNil(),
			"widening this operator's reach woke nothing; every ServiceAccount in the namespace "+
				"stays unserved until something unrelated happens to touch one")
	})

	// A ServiceAccount asking only through named keys carries nothing under the
	// bare annotation, and the wake-up for a namespace label used to skip on
	// exactly that. What it costs is the onboarding order this mapper exists for
	// -- annotate the ServiceAccounts, then have somebody with cluster-wide
	// access enable the namespace -- in which nothing else touches them again.
	It("wakes a ServiceAccount that asks only by name when its namespace is enabled", func() {
		const named = "named-only"

		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: named},
		})).To(Succeed())

		databricksAccount := &dbxv1alpha1.DatabricksAccount{}
		Expect(k8sClient.Get(ctx,
			types.NamespacedName{Namespace: wiredRecords, Name: "databricks-account"},
			databricksAccount)).To(Succeed())
		databricksAccount.Spec.Namespaces = append(databricksAccount.Spec.Namespaces, named)
		Expect(k8sClient.Update(ctx, databricksAccount)).To(Succeed())

		Expect(k8sClient.Create(ctx, &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name: wiredAccount, Namespace: named,
				Annotations: map[string]string{
					dbxv1alpha1.ServicePrincipalAnnotationFor("reader"): wiredRecords +
						"/databricks-account",
				},
			},
		})).To(Succeed())

		namespace := &corev1.Namespace{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: named}, namespace)).To(Succeed())
		namespace.Labels = map[string]string{dbxv1alpha1.MintLabel: dbxv1alpha1.Enabled}
		Expect(k8sClient.Update(ctx, namespace)).To(Succeed())

		Eventually(func() bool {
			projected := recordedIdentity(named, wiredAccount)
			if projected == nil {
				return false
			}
			_, minted := projected.Status.Identity("reader")
			return minted
		}, 20*time.Second, 250*time.Millisecond).Should(BeTrue(),
			"enabling the namespace woke nothing. This ServiceAccount stays unserved until "+
				"something unrelated happens to touch it, and nothing says so")
	})

	It("owns the DatabricksServiceAccount by the ServiceAccount and holds nothing with it", func() {
		var recorded *dbxv1alpha1.DatabricksServiceAccount
		Eventually(func() *dbxv1alpha1.DatabricksServiceAccount {
			recorded = recordedIdentity(wiredNamespace, wiredAccount)
			return recorded
		}, 20*time.Second, 250*time.Millisecond).ShouldNot(BeNil())

		Expect(recorded.OwnerReferences).To(HaveLen(1))
		Expect(recorded.OwnerReferences[0].Kind).To(Equal("ServiceAccount"))
		Expect(recorded.OwnerReferences[0].Name).To(Equal(wiredAccount))

		// No finalizer, and that is the point rather than an omission. This
		// object is a copy; deleting it means nothing, and an identity whose
		// destruction hung off a copy in a tenant's namespace is one a namespace
		// teardown destroys or abandons depending on which kind it deleted
		// first.
		Expect(recorded.Finalizers).To(BeEmpty())
	})

	It("writes the record in the operator's own namespace, and holds that", func() {
		serviceAccount := &corev1.ServiceAccount{}
		Expect(k8sClient.Get(ctx,
			types.NamespacedName{Namespace: wiredNamespace, Name: wiredAccount}, serviceAccount)).To(Succeed())

		var issued dbxv1alpha1.IssuedDatabricksServicePrincipal
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{
				Namespace: wiredRecords,
				Name:      dbxv1alpha1.IssuedNameFor(wiredNamespace, wiredAccount, "", serviceAccount.UID),
			}, &issued)
		}, 20*time.Second, 250*time.Millisecond).Should(Succeed(),
			"nothing recorded what was issued; the record is what outlives this namespace")

		Expect(issued.Finalizers).To(ContainElement(dbxv1alpha1.ServicePrincipalFinalizer))
		Expect(issued.Spec.ServiceAccount.UID).To(Equal(serviceAccount.UID))
		Expect(issued.OwnerReferences).To(BeEmpty(),
			"the record is owned by nothing; being collected with the ServiceAccount is what it exists not to do")
	})

	It("says why it can do nothing further, naming the object to create", func() {
		Eventually(func() string {
			recorded := recordedIdentity(wiredNamespace, wiredAccount)
			if recorded == nil {
				return ""
			}
			identity, issued := recorded.Status.First()
			if !issued {
				return ""
			}
			for _, condition := range identity.Conditions {
				if condition.Type == conditionReady {
					return condition.Reason + " " + condition.Message
				}
			}
			return ""
		}, 20*time.Second, 250*time.Millisecond).To(ContainSubstring("databricks-account"),
			"nothing told anybody which object to create")
	})

	// This is the whole of the several-operators design, and the only place it can
	// be shown: the merge is the API server's, and a fake client's is not it.
	//
	// Two operators write one DatabricksServiceAccount. Neither reads what the
	// other wrote and neither can be made to wait for it, so the only thing
	// keeping one from undoing the other is that each owns its own entry and sends
	// only that. Read, change, write passes every test that looks at one operator.
	It("leaves another operator's entry alone while writing its own", func() {
		const stranger = "ops-b/databricks-account"

		Expect(k8sClient.Create(ctx, &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name: "shared", Namespace: wiredNamespace,
				Annotations: map[string]string{
					dbxv1alpha1.ServicePrincipalAnnotation: wiredRecords + "/databricks-account",
				},
			},
		})).To(Succeed())

		Eventually(func() bool {
			projected := recordedIdentity(wiredNamespace, "shared")
			if projected == nil {
				return false
			}
			_, mine := projected.Status.Identity(wiredRecords + "/databricks-account")
			return mine
		}, 20*time.Second, 250*time.Millisecond).Should(BeTrue(),
			"this operator never wrote its own entry, so there is nothing to test the merge with")

		// The other operator, writing as itself. Nothing about it exists in this
		// binary: what makes it a different operator is the field manager, which
		// is what the API server records ownership against.
		theirs := acv1alpha1.DatabricksServiceAccount("shared", wiredNamespace).
			WithStatus(acv1alpha1.DatabricksServiceAccountStatus().
				WithIdentities(acv1alpha1.ProjectedIdentity().
					WithProfile(stranger).
					WithOperator(stranger).
					WithClientID("belongs-to-somebody-else")))
		Expect(k8sClient.Status().Apply(ctx, theirs,
			client.FieldOwner("databricks.workload-identity.io/"+stranger))).To(Succeed())

		// Make this operator write again, and write something observably new, so
		// that seeing the new value is proof its write landed after theirs. A
		// bare "both are present" would pass by looking too early.
		serviceAccount := &corev1.ServiceAccount{}
		Expect(k8sClient.Get(ctx,
			types.NamespacedName{Namespace: wiredNamespace, Name: "shared"}, serviceAccount)).To(Succeed())
		serviceAccount.Annotations[dbxv1alpha1.ServicePrincipalAnnotationFor("later")] =
			wiredRecords + "/databricks-account"
		Expect(k8sClient.Update(ctx, serviceAccount)).To(Succeed())

		Eventually(func() bool {
			projected := recordedIdentity(wiredNamespace, "shared")
			if projected == nil {
				return false
			}
			_, appeared := projected.Status.Identity("later")
			return appeared
		}, 20*time.Second, 250*time.Millisecond).Should(BeTrue(),
			"this operator never wrote after the other one did, so the merge was never exercised")

		projected := recordedIdentity(wiredNamespace, "shared")
		Expect(projected).NotTo(BeNil())
		other, kept := projected.Status.Identity(stranger)
		Expect(kept).To(BeTrue(),
			"this operator's write removed another operator's entry; the workload loses an "+
				"identity it was issued, and the other operator cannot tell -- it reads its own "+
				"entry, finds it gone, and writes it back, forever")
		Expect(other.ClientID).To(Equal("belongs-to-somebody-else"),
			"another operator's entry was overwritten rather than left")
	})

	// Withdrawing is an apply that removes the last entry, and what that produces
	// is only visible to a real API server: identities is the whole of this
	// status, so the merge is left with an object that has no fields, writes
	// null, and is refused -- `status: Invalid value: "null": in body must be of
	// type object`. A fake client validates nothing and reports success.
	//
	// The apply then fails on every pass and the DatabricksServiceAccount stays
	// exactly as it was: reporting Ready, for identities whose records are deleted
	// and whose service principals are destroyed.
	It("withdraws named identities without leaving the DatabricksServiceAccount behind", func() {
		Expect(k8sClient.Create(ctx, &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name: "named", Namespace: wiredNamespace,
				Annotations: map[string]string{
					dbxv1alpha1.ServicePrincipalAnnotationFor("reader"): wiredRecords +
						"/databricks-account",
					dbxv1alpha1.ServicePrincipalAnnotationFor("writer"): wiredRecords +
						"/databricks-account",
				},
			},
		})).To(Succeed())

		Eventually(func() int {
			projected := recordedIdentity(wiredNamespace, "named")
			if projected == nil {
				return 0
			}
			return len(projected.Status.Identities)
		}, 20*time.Second, 250*time.Millisecond).Should(Equal(2),
			"nothing was projected, so there is nothing to withdraw")

		serviceAccount := &corev1.ServiceAccount{}
		Expect(k8sClient.Get(ctx,
			types.NamespacedName{Namespace: wiredNamespace, Name: "named"}, serviceAccount)).To(Succeed())
		serviceAccount.Annotations = nil
		Expect(k8sClient.Update(ctx, serviceAccount)).To(Succeed())

		Eventually(func() bool {
			return recordedIdentity(wiredNamespace, "named") == nil
		}, 20*time.Second, 250*time.Millisecond).Should(BeTrue(),
			"the DatabricksServiceAccount outlived the request. Its records are gone and its service "+
				"principals are destroyed, so every word of it is false -- and it still reports "+
				"Ready")
	})

	It("wakes when the annotation is withdrawn, and takes the identity back", func() {
		serviceAccount := &corev1.ServiceAccount{}
		Expect(k8sClient.Get(ctx,
			types.NamespacedName{Namespace: wiredNamespace, Name: wiredAccount}, serviceAccount)).To(Succeed())
		serviceAccount.Annotations = nil
		Expect(k8sClient.Update(ctx, serviceAccount)).To(Succeed())

		Eventually(func() bool {
			return recordedIdentity(wiredNamespace, wiredAccount) == nil
		}, 20*time.Second, 250*time.Millisecond).Should(BeTrue(),
			"withdrawing the annotation left the identity in place")
	})
})

// recordedIdentity returns the object, or nil if there is none or it is on its
// way out. A object with a deletionTimestamp is gone as far as anything here is
// concerned: envtest runs no garbage collector, so it lingers after its
// finalizer is dropped.
func recordedIdentity(namespace, name string) *dbxv1alpha1.DatabricksServiceAccount {
	var recorded dbxv1alpha1.DatabricksServiceAccount
	err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &recorded)
	if apierrors.IsNotFound(err) || err != nil {
		return nil
	}
	if !recorded.DeletionTimestamp.IsZero() {
		return nil
	}
	return &recorded
}

// A pod that gates readiness on winning an election never becomes ready while
// another pod holds the lease -- and with one replica, the pod holding it is the
// one the rolling update is waiting to replace. Nothing times out and nothing
// fails: the Deployment sits at "old ready, new not" indefinitely.
//
// It is here rather than in a unit test because the thing that decides is
// controller-runtime's own classification of a Runnable, and a unit test would
// assert the classification we believe in rather than the one it makes.
var _ = Describe("readiness under leader election", func() {
	It("opens while another pod holds the lease", func() {
		const (
			electing = "electing"
			election = "b1c0ffee.databricks.workload-identity.io"
		)

		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: electing},
		})).To(Succeed())

		// Held by somebody else, and held for long enough that nothing in this
		// spec can take it: that is the state a rolling update begins in.
		held := metav1.NewMicroTime(time.Now())
		Expect(k8sClient.Create(ctx, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Namespace: electing, Name: election},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       ptr.To("the-pod-being-replaced"),
				LeaseDurationSeconds: ptr.To(int32(3600)),
				AcquireTime:          &held,
				RenewTime:            &held,
			},
		})).To(Succeed())

		manager, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                  k8sClient.Scheme(),
			Metrics:                 metricsserver.Options{BindAddress: "0"},
			LeaderElection:          true,
			LeaderElectionID:        election,
			LeaderElectionNamespace: electing,
		})
		Expect(err).NotTo(HaveOccurred())

		gate := &CacheSynced{}
		Expect(manager.Add(gate)).To(Succeed())

		running, stop := context.WithCancel(ctx)
		DeferCleanup(stop)
		go func() {
			defer GinkgoRecover()
			_ = manager.Start(running)
		}()

		Eventually(func() error {
			return gate.Check(nil)
		}, 30*time.Second, 250*time.Millisecond).Should(Succeed(),
			"the gate never opened while another pod held the lease. This pod would never join "+
				"the webhook Service, the Deployment would never remove the pod holding the "+
				"lease, and the rollout would sit there forever")
	})
})

// What the webhook produces has to be a pod the API server will accept, and
// nothing but a real one can say so.
//
// A mutating webhook runs before validation, so anything invalid it produces is
// this operator refusing to let a workload be created -- with an error naming
// neither this operator nor anything its owner recognises, and failurePolicy:
// Ignore is no protection, because the call succeeded. The unit tests build the
// same pod against a fake client, which validates nothing and reports success.
//
// Two things here are only legal by measurement rather than by reading the API:
// a downwardAPI fieldPath with a quoted annotation key in it, and a projection
// path carrying a name the workload chose.
var _ = Describe("what the webhook produces", func() {
	const admitting = "admitting"

	BeforeEach(func() {
		_ = k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: admitting},
		})
	})

	It("makes a pod the API server accepts, carrying every identity", func() {
		const reader, writer = "reader", "writer"

		databricksServiceAccount := &dbxv1alpha1.DatabricksServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Namespace: admitting, Name: "etl"},
		}
		Expect(k8sClient.Create(ctx, databricksServiceAccount)).To(Succeed())
		databricksServiceAccount.Status.Identities = []dbxv1alpha1.ProjectedIdentity{
			{
				Profile: reader, Operator: "ops-a/databricks-account",
				ClientID: "reader-client", Audience: "databricks",
			},
			{
				Profile: writer, Operator: "ops-b/databricks-account",
				ClientID: "writer-client", Audience: "some-other-aud",
			},
		}
		Expect(k8sClient.Status().Update(ctx, databricksServiceAccount)).To(Succeed())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: admitting},
			Spec: corev1.PodSpec{
				ServiceAccountName: "etl",
				Containers:         []corev1.Container{{Name: "app", Image: "busybox"}},
			},
		}
		raw, err := json.Marshal(pod)
		Expect(err).NotTo(HaveOccurred())

		injector := &dbxwebhook.PodTokenInjector{
			Client:  k8sClient,
			Decoder: admission.NewDecoder(k8sClient.Scheme()),
		}
		response := injector.Handle(ctx, admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Namespace: admitting,
				Object:    runtime.RawExtension{Raw: raw},
			},
		})
		Expect(response.Allowed).To(BeTrue())
		Expect(response.Patches).NotTo(BeEmpty(), "the webhook changed nothing")

		patchJSON, err := json.Marshal(response.Patches)
		Expect(err).NotTo(HaveOccurred())
		decoded, err := jsonpatch.DecodePatch(patchJSON)
		Expect(err).NotTo(HaveOccurred())
		applied, err := decoded.Apply(raw)
		Expect(err).NotTo(HaveOccurred())

		admitted := &corev1.Pod{}
		Expect(json.Unmarshal(applied, admitted)).To(Succeed())

		// The whole point: a real API server, applying its own validation to
		// what this webhook wrote.
		Expect(k8sClient.Create(ctx, admitted)).To(Succeed(),
			"the API server refused the pod this webhook produced. A mutating webhook runs "+
				"before validation, so this is the operator making a workload uncreatable")

		var projected *corev1.ProjectedVolumeSource
		for i := range admitted.Spec.Volumes {
			if admitted.Spec.Volumes[i].Name == dbxwebhook.TokenVolume {
				projected = admitted.Spec.Volumes[i].Projected
			}
		}
		Expect(projected).NotTo(BeNil())

		paths := map[string]string{}
		var copies int
		for _, source := range projected.Sources {
			if token := source.ServiceAccountToken; token != nil {
				paths[token.Path] = token.Audience
			}
			if source.DownwardAPI != nil {
				copies += len(source.DownwardAPI.Items)
			}
		}
		Expect(paths).To(SatisfyAll(
			HaveKeyWithValue(dbxwebhook.TokenProjectionPathFor(reader), "databricks"),
			HaveKeyWithValue(dbxwebhook.TokenProjectionPathFor(writer), "some-other-aud"),
		), "a token per identity, each minted for its own audience")
		Expect(copies).To(Equal(1), "nothing copies the configuration into the container")
		Expect(admitted.Annotations).To(HaveKey(dbxwebhook.ConfigAnnotation))
	})
})
