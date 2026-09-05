package controller

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/databricks/databricks-sdk-go/apierr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	dbx "github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
	dbxwebhook "github.com/workload-identity/databricks-service-principal-operator/internal/webhook"
)

// TestAnAskingServiceAccountGetsAnIdentity covers the whole of the ordinary
// path, and the two things about the object that are not obvious from it
// working.
func TestAnAskingServiceAccountGetsAnIdentity(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))

	// Twice: the first pass creates the object, the second converges it. That is
	// not an implementation detail worth hiding -- an object that had to exist
	// before anything was built in Databricks is what makes the id recordable.
	c.settle(t)

	principal := principalOf(t, c.Client)
	if principal == nil {
		t.Fatal("no identity was recorded for a ServiceAccount that asked for one")
	}
	if identityIn(t, principal).ClientID == "" || identityIn(t, principal).ServicePrincipalID == "" {
		t.Errorf("status is %+v, want both ids Databricks assigned", principal.Status)
	}
	if identityIn(t, principal).Subject != "system:serviceaccount:"+testNamespace+":"+testName {
		t.Errorf("subject is %q, want the sub claim a projected token carries", identityIn(t, principal).Subject)
	}
	if identityIn(t, principal).Audience != testAudience {
		t.Errorf("audience is %q; a workload's projected volume has to ask for this one, and the "+
			"default ServiceAccount token does not carry it", identityIn(t, principal).Audience)
	}
	ready := meta.FindStatusCondition(identityIn(t, principal).Conditions, conditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready is %v, want the exchange reported as working", ready)
	}

	// Owned by the ServiceAccount. That is what makes "the ServiceAccount
	// exists" true by construction rather than something to keep checking: an
	// identity whose ServiceAccount is gone is one nobody owns, and a later
	// ServiceAccount of the same name would inherit it silently.
	owners := principal.OwnerReferences
	if len(owners) != 1 || owners[0].Kind != "ServiceAccount" || owners[0].Name != testName {
		t.Errorf("owners are %+v, want the ServiceAccount that asked", owners)
	}

	// And it holds nothing. This object is a copy; what holds the identity is
	// the record, in the operator's own namespace, where a namespace teardown
	// cannot reach it.
	if len(principal.Finalizers) != 0 {
		t.Errorf("finalizers are %v; nothing about the identity may hang off an object in the "+
			"tenant's namespace, because that object is deleted with the namespace in an "+
			"order nothing specifies", principal.Finalizers)
	}

	issued := c.issuedOf(t, asking(testNamespace, testName))
	if issued == nil {
		t.Fatal("nothing recorded what was issued; the record is the only memory there is")
	}
	if !slices.Contains(issued.Finalizers, dbxv1alpha1.ServicePrincipalFinalizer) {
		t.Errorf("the record's finalizers are %v, want it held until Databricks has let go",
			issued.Finalizers)
	}
	if issued.Status.ServicePrincipalID != identityIn(t, principal).ServicePrincipalID {
		t.Errorf("the DatabricksServiceAccount says %q and the record says %q; it is a copy and "+
			"there is nothing for it to disagree with the record about",
			identityIn(t, principal).ServicePrincipalID, issued.Status.ServicePrincipalID)
	}
	if want := testRecords + "/" + issued.Name; identityIn(t, principal).Issued != want {
		t.Errorf("the DatabricksServiceAccount names record %q, want %q -- the record lives in a namespace the "+
			"reader cannot list, so naming it without the namespace names nothing they can find",
			identityIn(t, principal).Issued, want)
	}

	if len(stub.created) != 1 {
		t.Errorf("created %v, want exactly one service principal", stub.created)
	}
	if len(stub.policies) != 1 || !strings.Contains(stub.policies[0], testIssuer) {
		t.Errorf("policies are %v, want one naming the cluster's issuer", stub.policies)
	}
}

// TestANamespaceThatWasNotAllowedMintsNothing covers the gate, and it is the one
// that keeps this operator from being a way to mint Databricks identities
// without limit.
//
// Annotating a ServiceAccount is something anybody who holds a namespace can do,
// so the request cannot also be the permission. The label is on the Namespace
// because that is an object only somebody with cluster-wide access can write.
func TestANamespaceThatWasNotAllowedMintsNothing(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		namespaceNamed(testNamespace), asking(testNamespace, testName))
	c.settle(t)

	if principalOf(t, c.Client) != nil {
		t.Error("an identity was recorded in a namespace nobody allowed to ask")
	}
	if len(stub.created) != 0 {
		t.Errorf("created %v in Databricks for a namespace that was never allowed", stub.created)
	}
}

// TestAServiceAccountThatDidNotAskGetsNothing is the other half of the gate: the
// namespace allows asking, and this one did not ask.
func TestAServiceAccountThatDidNotAskGetsNothing(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), serviceAccountNamed(testNamespace, testName))
	c.settle(t)

	if principalOf(t, c.Client) != nil {
		t.Error("an identity was recorded for a ServiceAccount that never asked for one")
	}
	if len(stub.created) != 0 {
		t.Errorf("created %v for a ServiceAccount that never asked", stub.created)
	}
}

// TestRemovingTheAnnotationTakesTheIdentityBack covers the only way to say
// "this workload no longer needs an identity" while keeping the ServiceAccount.
//
// Deleting the ServiceAccount says it too, and that path is garbage collection's
// rather than this operator's -- the object is owned by it. This one is the case
// where the ServiceAccount stays and the request goes.
func TestRemovingTheAnnotationTakesTheIdentityBack(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.settle(t)
	if principalOf(t, c.Client) == nil {
		t.Fatal("nothing was built to take back")
	}

	var serviceAccount = serviceAccountNamed(testNamespace, testName)
	if err := c.Client.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testName}, serviceAccount); err != nil {
		t.Fatal(err)
	}
	serviceAccount.Annotations = nil
	if err := c.Client.Update(context.Background(), serviceAccount); err != nil {
		t.Fatal(err)
	}

	// Twice: the first pass marks the object for deletion, and the finalizer
	// makes the second one the pass that pays what is owed.
	c.settle(t)

	if principalOf(t, c.Client) != nil {
		t.Error("the identity is still recorded after the request was taken back")
	}
	if len(stub.deleted) != 1 {
		t.Errorf("deleted %v, want the service principal removed in Databricks", stub.deleted)
	}
}

// TestTheIdIsRecordedBeforeAnythingElseIsAttempted covers the one order that can
// leave something behind.
//
// The service principal exists in Databricks the moment it is made. An id this
// object never wrote down is one nothing here will ever delete -- so the write
// has to land before the next call is attempted, not at the end of the pass.
func TestTheIdIsRecordedBeforeAnythingElseIsAttempted(t *testing.T) {
	t.Parallel()
	// The pass does not finish. A returned error would still reach the status
	// write at the end of the pass, so it cannot tell "written down before the
	// next call" from "written down eventually" -- and the case this is about is
	// the process not surviving the next call.
	stub := &stubClients{policyPanics: true}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))

	// The record is written first, and nothing has been created yet. That order
	// is the outer guarantee: a pass that stops here leaves a record naming no
	// service principal, which the marker resolves, rather than a service
	// principal nothing names.
	c.project(t, testNamespace, testName)
	issued := c.issuedOf(t, asking(testNamespace, testName))
	if issued == nil {
		t.Fatal("nothing was recorded before Databricks was asked for anything")
	}
	if issued.Status.ServicePrincipalID != "" || len(stub.created) != 0 {
		t.Fatalf("record says %q and created %v; nothing should exist in Databricks yet",
			issued.Status.ServicePrincipalID, stub.created)
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the stub was arranged to stop the pass and did not")
			}
		}()
		c.records(t)
	}()

	issued = c.issuedOf(t, asking(testNamespace, testName))
	if issued.Status.ServicePrincipalID == "" {
		t.Error("a service principal exists in Databricks and its id was never written down; " +
			"nothing here will ever delete it by id")
	}
	if len(stub.created) != 1 {
		t.Errorf("created %v, want the one service principal that is now unrecorded", stub.created)
	}
}

// TestAFailedPolicyIsNotReportedAsWorking is the other half of the same call:
// the service principal is there and no token reaches it, which is not ready.
func TestAFailedPolicyIsNotReportedAsWorking(t *testing.T) {
	t.Parallel()
	stub := &stubClients{policyErr: errors.New("the service is temporarily unavailable")}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.settle(t)

	principal := principalOf(t, c.Client)
	ready := meta.FindStatusCondition(identityIn(t, principal).Conditions, conditionReady)
	if ready == nil || ready.Status == metav1.ConditionTrue {
		t.Errorf("Ready is %v; the policy was not written, so no token reaches this principal", ready)
	}
}

// TestDatabricksNotAnsweringIsNotTheDeclarationBeingWrong covers the branch
// everything unclassified falls into.
//
// It is outcomeFor's default, so it is what a create, a delete and a federation
// policy write report when Databricks did not answer -- not only a lookup.
// Unknown rather than False, because False is the answer somebody edits their
// spec over, and nothing about the spec has been shown to be wrong: the same
// declaration will work when Databricks answers again.
func TestDatabricksNotAnsweringIsNotTheDeclarationBeingWrong(t *testing.T) {
	t.Parallel()
	unreachable := errors.New("dial tcp: lookup accounts.cloud.databricks.com: no such host")
	c := newControllers(t, &stubClients{policyErr: unreachable},
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.settle(t)

	ready := meta.FindStatusCondition(
		identityIn(t, principalOf(t, c.Client)).Conditions, conditionReady)
	if ready == nil || ready.Reason != reasonDatabricksUnavailable {
		t.Fatalf("Ready is %v, want %s -- this is the reason a tenant reads while Databricks "+
			"is unreachable, and it is the only one that goes back to the workqueue",
			ready, reasonDatabricksUnavailable)
	}
	if ready.Status != metav1.ConditionUnknown {
		t.Errorf("Ready is %s, want Unknown: False sends whoever reads it to a declaration that "+
			"has nothing wrong with it", ready.Status)
	}
	if !strings.Contains(ready.Message, unreachable.Error()) {
		t.Errorf("Ready says %q and does not carry what Databricks answered, which is the only "+
			"place it is written down", ready.Message)
	}
}

// TestARefusedRequestIsNotReportedAsSomethingMissing covers two answers that
// call for different people.
//
// Databricks answers some refusals 400 rather than 404, and the two mean
// opposite things: what was named is not there, or the request itself is not
// acceptable. Under one reason, whoever matched on it goes looking for a service
// principal that was never missing, and nothing they can do in Databricks makes
// the refused request land.
func TestARefusedRequestIsNotReportedAsSomethingMissing(t *testing.T) {
	t.Parallel()
	for _, answer := range []struct {
		name string
		gave error
		want string
	}{
		{"refused", &apierr.APIError{
			ErrorCode:  "INVALID_PARAMETER_VALUE",
			StatusCode: 400,
			Message:    "the subject is not acceptable",
		}, reasonRejected},
		{"not there", apierr.ErrResourceDoesNotExist, reasonNotFound},
	} {
		t.Run(answer.name, func(t *testing.T) {
			c := newControllers(t, &stubClients{policyErr: answer.gave},
				mintingNamespace(testNamespace), asking(testNamespace, testName))
			c.settle(t)

			ready := meta.FindStatusCondition(
				identityIn(t, principalOf(t, c.Client)).Conditions, conditionReady)
			if ready == nil || ready.Reason != answer.want {
				t.Errorf("Ready is %v, want %s; the reason is what people match and alert on, "+
					"and these two are ended by different acts", ready, answer.want)
			}
		})
	}
}

// TestAnIdThisOperatorWroteDownIsNotReportedAsABadSpec covers where a reason
// sends the person who reads it.
//
// The only value that can fail to parse as a coordinate is the service principal
// id on the record, which this operator wrote from what Databricks answered.
// Nobody declared it. Reported as a bad spec, whoever reads it opens the
// ServiceAccount and the DatabricksAccount, finds nothing wrong with either, and
// still has an identity that cannot converge.
func TestAnIdThisOperatorWroteDownIsNotReportedAsABadSpec(t *testing.T) {
	t.Parallel()
	c := newControllers(t, &stubClients{policyErr: &dbx.ErrMalformedCoordinate{
		Field: "servicePrincipalId",
		Value: "99999999999999999999",
		Cause: errors.New("value out of range"),
	}}, mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.settle(t)

	ready := meta.FindStatusCondition(
		identityIn(t, principalOf(t, c.Client)).Conditions, conditionReady)
	if ready == nil || ready.Reason != reasonMalformedRecord {
		t.Errorf("Ready is %v, want %s; nothing anybody declared is wrong here",
			ready, reasonMalformedRecord)
	}
	if ready != nil && !strings.Contains(ready.Message, "servicePrincipalId") {
		t.Errorf("message is %q; it has to name the value that will not parse, which is the "+
			"only way to find it on an object holding several ids", ready.Message)
	}
}

// TestOneDeletedInDatabricksIsNotReplaced is the whole of what makes this a
// bridge into somebody else's governance rather than a second one.
//
// Databricks is where identity is governed. A service principal deleted there
// was deleted by somebody entitled to, and making a new one because the
// annotation still stands would be this operator overruling that decision every
// minute -- and winning, since it never tires.
func TestOneDeletedInDatabricksIsNotReplaced(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.settle(t)

	first := identityIn(t, principalOf(t, c.Client)).ServicePrincipalID
	stub.gone = first

	// More than once. The decision has to survive the pass that records it: if
	// the record is what holds it, and that pass erases the live id, the next
	// pass reads "never built" and builds -- which is overruling the other side
	// on a timer, one minute at a time.
	c.settle(t)

	if len(stub.created) != 1 {
		t.Errorf("created %v; the one that was deleted in Databricks was replaced", stub.created)
	}

	principal := principalOf(t, c.Client)
	if identityIn(t, principal).RemovedServicePrincipalID != first {
		t.Errorf("removedServicePrincipalId is %q, want %q -- it is the value that searches "+
			"Databricks' audit log for who deleted it",
			identityIn(t, principal).RemovedServicePrincipalID, first)
	}
	if identityIn(t, principal).ServicePrincipalID != "" {
		t.Errorf("servicePrincipalId is %q; nothing is there, and naming it would mislead "+
			"everything that reads it", identityIn(t, principal).ServicePrincipalID)
	}
	if identityIn(t, principal).ClientID != "" {
		t.Errorf("clientId is %q; a pod equipped with it would present a client id that "+
			"resolves to nothing", identityIn(t, principal).ClientID)
	}
	ready := meta.FindStatusCondition(identityIn(t, principal).Conditions, conditionReady)
	if ready == nil || ready.Reason != reasonRemovedInDatabricks {
		t.Errorf("Ready is %v, want it to say the governance side removed it", ready)
	}
	if ready != nil && !strings.Contains(ready.Message, dbxv1alpha1.ServicePrincipalAnnotation) {
		t.Errorf("message is %q; it has to name the one act that starts again, because nothing "+
			"else does and it is not guessable", ready.Message)
	}
}

// TestAskingAgainStartsAgain is the recovery, and the only one.
//
// Removing the annotation destroys the identity; writing it again asks for a
// new one. It is a rotation rather than a repair: Databricks assigns the
// applicationId and will not accept one, so everything granted to the old
// identity has to be granted again by whoever governs that.
//
// Deleting the DatabricksServiceAccount is not this. That object is a copy and
// deleting it does nothing, which is the whole reason the recovery has to be an
// act on the ServiceAccount.
func TestAskingAgainStartsAgain(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.settle(t)

	stub.gone = identityIn(t, principalOf(t, c.Client)).ServicePrincipalID
	c.settle(t)

	stopsAsking(t, c.Client)
	c.settle(t)
	if c.issuedOf(t, asking(testNamespace, testName)) != nil {
		t.Fatal("the record outlived the request it was made for")
	}

	stub.gone = ""
	stub.newServicePrincipalID, stub.newClientID = "9900", "app-uuid-2"
	serviceAccount := serviceAccountNamed(testNamespace, testName)
	if err := c.Client.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testName}, serviceAccount); err != nil {
		t.Fatal(err)
	}
	serviceAccount.Annotations = map[string]string{
		dbxv1alpha1.ServicePrincipalAnnotation: testOperatorRef.String(),
	}
	if err := c.Client.Update(context.Background(), serviceAccount); err != nil {
		t.Fatal(err)
	}
	c.settle(t)

	principal := principalOf(t, c.Client)
	if principal == nil {
		t.Fatal("nothing was issued after the annotation was written again")
	}
	if identityIn(t, principal).ClientID != "app-uuid-2" {
		t.Errorf("clientId is %q, want the new one -- Databricks assigns it and it cannot be "+
			"the same, which is why every permission has to be granted again",
			identityIn(t, principal).ClientID)
	}
	if identityIn(t, principal).RemovedServicePrincipalID != "" {
		t.Errorf("removedServicePrincipalId is %q on a fresh identity",
			identityIn(t, principal).RemovedServicePrincipalID)
	}
}

// TestOneThatIsThereIsNotBuiltAgain is the other half: a settled object makes no
// create call, however often it is reconciled.
func TestOneThatIsThereIsNotBuiltAgain(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.settle(t)

	if len(stub.created) != 1 {
		t.Errorf("created %v; the service principal was already there on every pass after the first",
			stub.created)
	}
}

// TestAFailedDeleteHoldsTheRecord covers the finalizer never being dropped on a
// failure.
//
// Dropping it would be the operator deciding, on the evidence of one call that
// did not go through, that an identity outliving the object recording it is
// acceptable. A person can decide that; they remove the finalizer by hand,
// having seen what is left.

func TestAFailedDeleteHoldsTheRecord(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.settle(t)

	stopsAsking(t, c.Client)
	stub.deleteErr = errors.New("the service is temporarily unavailable")
	c.settle(t)

	held := c.issuedOf(t, asking(testNamespace, testName))
	if held == nil {
		t.Fatal("the record went while its service principal is still in Databricks")
	}
	ready := meta.FindStatusCondition(held.Status.Conditions, conditionReady)
	if ready == nil || ready.Reason != reasonDeleteFailed {
		t.Errorf("Ready is %v, want it to say the service principal is still there", ready)
	}

	// The DatabricksServiceAccount is gone either way, and that is right: nothing
	// is asking any more, so there is nothing to show. What is owed is owed on the
	// record, in the operator's own namespace, where waiting blocks nobody's
	// namespace from being deleted.
	if principalOf(t, c.Client) != nil {
		t.Error("the DatabricksServiceAccount outlived the request; it shows what is asked for and nothing asks")
	}

	// And it is owed until it is paid.
	stub.deleteErr = nil
	c.settle(t)
	if len(stub.deleted) != 1 {
		t.Errorf("deleted %v, want the retry to have removed it", stub.deleted)
	}
	if c.issuedOf(t, asking(testNamespace, testName)) != nil {
		t.Error("the record is still there after the call landed")
	}
}

// TestAnEmptyIssuerIsReportedRatherThanSent covers a value that cannot be right
// and that Databricks blames the wrong thing for.
//
// The issuer is read from the operator's own projected token. A token file that
// could not be read leaves it empty, and Databricks refuses an empty issuer with
// a message about the policy's own description -- which sends whoever reads it
// to look at the policy.
func TestAnEmptyIssuerIsReportedRatherThanSent(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.Issued.TokenPath = writeTokenAs(t, "", "system:serviceaccount:operators:controller-manager", testAudience)
	c.settle(t)

	if len(stub.policies) != 0 {
		t.Errorf("wrote %v; an empty issuer is a policy nothing can satisfy", stub.policies)
	}
	principal := principalOf(t, c.Client)
	ready := meta.FindStatusCondition(identityIn(t, principal).Conditions, conditionReady)
	if ready == nil || ready.Reason != reasonUnprepared {
		t.Errorf("Ready is %v, want %s -- what could not be read is the operator's own token, "+
			"and NotConfigured names the account instead", ready, reasonUnprepared)
	}
}

// podRunningAs is a pod using one ServiceAccount, with no Databricks token —
// what the webhook admits when it cannot answer.
func podRunningAs(serviceAccount string, volumes ...corev1.Volume) *corev1.Pod {
	return cached(rawPodRunningAs(serviceAccount, volumes...))
}

// tokenVolume is what the webhook injects for this operator's identity, as the
// pod cache keeps it: the name, the path that says which identity the token is
// for, and the audience it is minted for.
func tokenVolume() corev1.Volume {
	return tokenVolumeFor(testAudience)
}

func tokenVolumeFor(audience string) corev1.Volume {
	return corev1.Volume{
		Name: dbxwebhook.TokenVolume,
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{{
					ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
						Path:     dbxwebhook.TokenProjectionPathFor(testOperatorRef.String()),
						Audience: audience,
					},
				}},
			},
		},
	}
}

// equippedPod carries what the webhook injects: the token, and the configuration
// describing the identity it belongs to.
//
// The configuration is rendered rather than written out, because that is what
// being equipped means -- the pod says about this identity what the identity
// says now. A fixture with the string typed in would drift from the renderer
// instead of from the identity, and would then be testing itself.
func equippedPod(serviceAccount string, identities ...dbxv1alpha1.ProjectedIdentity) *corev1.Pod {
	if len(identities) == 0 {
		identities = []dbxv1alpha1.ProjectedIdentity{{
			Profile: testOperatorRef.String(), ClientID: "app-uuid", Audience: testAudience,
		}}
	}
	pod := rawPodRunningAs(serviceAccount, tokenVolume())
	pod.Annotations = map[string]string{
		dbxwebhook.ConfigAnnotation: dbxwebhook.Profiles(identities),
	}
	return cached(pod)
}

func equipment(t *testing.T, c client.Client) *metav1.Condition {
	t.Helper()
	principal := principalOf(t, c)
	if principal == nil {
		t.Fatal("no identity is recorded")
	}
	return meta.FindStatusCondition(identityIn(t, principal).Conditions, conditionEquipped)
}

// TestAPodRunningWithoutTheTokenIsReported covers the cost of admitting pods the
// webhook could not equip.
//
// That admission is deliberate: an outage of this operator must never stop
// workloads from starting. What it leaves behind is a pod that looks entirely
// normal, cannot reach Databricks, and fails with an SDK error pointing at
// Databricks -- and it does not heal when the webhook returns, because nothing
// rewrites a running pod. Reporting it here is what makes that finding possible
// at all.
func TestAPodRunningWithoutTheTokenIsReported(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName),
		podRunningAs(testName))
	c.settle(t)

	condition := equipment(t, c.Client)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("Equipped is %v, want it to say a pod is running without the token", condition)
	}
	if !strings.Contains(condition.Message, "runner-"+testName) {
		t.Errorf("message is %q; it has to name the pod, because finding it by hand means "+
			"reading every pod spec in the namespace", condition.Message)
	}
	if !strings.Contains(condition.Message, "recreating") {
		t.Errorf("message is %q; it has to say what fixes it, because waiting does not and "+
			"nothing else says so", condition.Message)
	}
	// And it has to stop at what is known. A pod admitted while this identity
	// had no client id, and a pod that already carried a volume of that name,
	// arrive here identically -- and recreating the third comes back the same,
	// so a message naming only the webhook sends its reader to look at a webhook
	// that was up and to recreate a pod for ever.
	for _, cause := range []string{"client id", dbxwebhook.TokenVolume} {
		if !strings.Contains(condition.Message, cause) {
			t.Errorf("message is %q, want it to name %q as well; what is established is that "+
				"this pod has no token, and three things produce that",
				condition.Message, cause)
		}
	}
}

// TestAFullyEquippedPodIsNotReported is the other half. Reporting a pod that is
// fine would put an alarm on for every workload in the cluster.
func TestAFullyEquippedPodIsNotReported(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName),
		equippedPod(testName))
	c.settle(t)

	condition := equipment(t, c.Client)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		t.Errorf("Equipped is %v for a pod that has everything it needs", condition)
	}
}

// TestAPodCarryingAnOlderConfigurationIsReported covers the half of being
// equipped that is easy to leave out, and only the half that is a fault.
//
// A pod is given its configuration once, when it is created, and nothing
// rewrites a running pod. One admitted before Databricks had assigned a client
// id carries a profile that names none, which cannot be exchanged for anything
// -- and everything else about that pod looks right: the service principal
// exists, the policy is written, the token is in the volume.
func TestAPodCarryingAnOlderConfigurationIsReported(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	// The token is there, and the configuration is from before the client id
	// was known.
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName),
		podRunningAs(testName, tokenVolume()))
	c.settle(t)

	condition := equipment(t, c.Client)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("Equipped is %v, want it to say a pod carries an older answer than the "+
			"identity gives now", condition)
	}
	if !strings.Contains(condition.Message, "configuration") {
		t.Errorf("message is %q, want it to say the pod carries an older answer than the "+
			"identity gives now", condition.Message)
	}
	if !strings.Contains(condition.Message, "recreating") {
		t.Errorf("message is %q, want it to say what fixes it", condition.Message)
	}
}

// TestPodsHoldingAProfileForAServicePrincipalThatIsGoneAreNotCalledEquipped
// covers the state where being equipped stops meaning anything.
//
// A service principal deleted in Databricks clears the client id and leaves the
// audience, so the webhook would now write nothing for this identity -- and what
// is written for nothing is the empty string, which every pod in the namespace
// contains. Equipped then reports True for pods carrying a profile naming a
// client that resolves to nothing, which is the one moment their owner needs to
// be told otherwise.
func TestPodsHoldingAProfileForAServicePrincipalThatIsGoneAreNotCalledEquipped(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName),
		equippedPod(testName))
	c.settle(t)

	if condition := equipment(t, c.Client); condition == nil ||
		condition.Status != metav1.ConditionTrue {
		t.Fatalf("Equipped is %v before anything was deleted; the rest of this test says "+
			"nothing unless it starts from a pod that was equipped", condition)
	}

	stub.gone = identityIn(t, principalOf(t, c.Client)).ServicePrincipalID
	c.settle(t)

	if got := identityIn(t, principalOf(t, c.Client)).ClientID; got != "" {
		t.Fatalf("clientId is %q; this test is about what is reported once it is cleared", got)
	}
	condition := equipment(t, c.Client)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("Equipped is %v for a pod carrying a profile for a service principal that is "+
			"not there; that pod cannot reach Databricks and the object says it can", condition)
	}
	if !strings.Contains(condition.Message, "runner-"+testName) {
		t.Errorf("message is %q; it has to name the pod, because finding it by hand means "+
			"reading every pod spec in the namespace", condition.Message)
	}
}

// TestPodsInANamespaceThatInjectsNothingAreToldSo covers a message that used to
// prescribe something that could never work.
//
// A namespace carrying mint and not inject makes identities and equips no pod,
// so every pod under one is reported without a token and told to be recreated --
// for ever, because recreating them produces the same pod. The pass reads the
// Namespace already and did not look.
func TestPodsInANamespaceThatInjectsNothingAreToldSo(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingWithoutInjecting(testNamespace), asking(testNamespace, testName),
		podRunningAs(testName))
	c.settle(t)

	condition := equipment(t, c.Client)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("Equipped is %v, want it to say the pod has no token", condition)
	}
	if !strings.Contains(condition.Message, dbxv1alpha1.InjectLabel) {
		t.Errorf("message is %q; it has to name the label, because recreating the pod is what "+
			"it otherwise asks for and that changes nothing", condition.Message)
	}
}

// TestPodsNobodyCanFixAreNotReported covers what would otherwise be a permanent
// alarm.
//
// A completed Job pod is history: it ran, it is over, and recreating it is not a
// thing anybody wants. A pod on its way out is the same. Reporting either would
// leave a condition False that no action clears.
func TestPodsNobodyCanFixAreNotReported(t *testing.T) {
	t.Parallel()
	finished := podRunningAs(testName)
	finished.Name = "finished"
	finished.Status.Phase = corev1.PodSucceeded

	going := podRunningAs(testName)
	going.Name = "going"
	now := metav1.Now()
	going.DeletionTimestamp = &now
	going.Finalizers = []string{"keeps.it/around"}

	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName), finished, going)
	c.settle(t)

	if condition := equipment(t, c.Client); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Errorf("Equipped is %v; neither of those pods is one anybody can or should fix", condition)
	}
}

// TestAnotherServiceAccountsPodsAreNotThisOnesProblem covers the mapping from a
// pod to the identity it runs under. Reporting somebody else's pod here would
// send whoever reads it to the wrong workload.
func TestAnotherServiceAccountsPodsAreNotThisOnesProblem(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName),
		podRunningAs("somebody-else"))
	c.settle(t)

	if condition := equipment(t, c.Client); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Errorf("Equipped is %v; that pod runs as another ServiceAccount", condition)
	}
}

// TestWithNoDatabricksAccountNothingBreaksAndEverythingSaysWhy covers forgetting
// the one object the operator needs.
//
// The Databricks side may be entirely ready -- a service principal for the
// operator, a federation policy naming its subject -- and the operator still
// cannot act, because nothing has told it which account or as whom. That is not
// a failure of any workload's identity, and reporting it as one would send
// somebody to look at the wrong thing.
//
// So: nothing crashes, nothing is created in Databricks, and every identity
// carries a message naming the object to create. An operator that crashlooped
// here would put the same reason in a log nobody is reading yet.
func TestWithNoDatabricksAccountNothingBreaksAndEverythingSaysWhy(t *testing.T) {
	t.Parallel()
	c := newControllers(t, nil,
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	// The real AccountInUse rather than the stub: with no DatabricksAccount it
	// has no clients, and answering NotConfigured is the whole of what is being
	// tested.
	c.Issued.Databricks = dbx.NewAccountInUse(operatorNamespace, databricksAccountName)

	c.settle(t)

	// The object is made either way: creating it is a Kubernetes act, and having
	// it is what carries the explanation.
	principal := principalOf(t, c.Client)
	if principal == nil {
		t.Fatal("no object was created; there is then nowhere to say why nothing works")
	}
	if identityIn(t, principal).ServicePrincipalID != "" {
		t.Errorf("servicePrincipalId is %q; nothing could have been created",
			identityIn(t, principal).ServicePrincipalID)
	}

	ready := meta.FindStatusCondition(identityIn(t, principal).Conditions, conditionReady)
	if ready == nil {
		t.Fatal("Ready is unset")
	}
	// Unknown rather than False. Nothing has been asked of Databricks, so
	// reporting this identity as wrong would blame it for never having been
	// looked at.
	if ready.Status != metav1.ConditionUnknown || ready.Reason != reasonNotConfigured {
		t.Errorf("Ready is %v, want Unknown/NotConfigured", ready)
	}
	if !strings.Contains(ready.Message, databricksAccountName) ||
		!strings.Contains(ready.Message, operatorNamespace) {
		t.Errorf("message is %q; it has to name the DatabricksAccount to create, or it tells "+
			"somebody they are stuck without telling them what unsticks them", ready.Message)
	}
}

// TestEnablingANamespaceWakesTheServiceAccountsInIt covers the order a team is
// actually onboarded in.
//
// The ServiceAccounts are annotated first, by the team; the namespace is enabled
// afterwards, by somebody with cluster-wide access. A namespace label is an
// event on no ServiceAccount, so without this nothing happens and nothing says
// so — the identities simply never appear.
//
// This was found by deploying, not by testing: every other test here calls
// Reconcile directly, which is exactly the step that was missing.
func TestEnablingANamespaceWakesTheServiceAccountsInIt(t *testing.T) {
	t.Parallel()
	asked := asking(testNamespace, testName)
	silent := serviceAccountNamed(testNamespace, "no-databricks")
	elsewhere := asking("team-b", "loader")

	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asked, silent,
		namespaceNamed("team-b"), elsewhere)

	requests := c.DatabricksServiceAccounts.identitiesInNamespace(
		context.Background(), mintingNamespace(testNamespace))

	if len(requests) != 1 {
		t.Fatalf("woke %+v, want only the ServiceAccount that asked", requests)
	}
	if requests[0].Namespace != testNamespace || requests[0].Name != testName {
		t.Errorf("woke %v, want %s/%s", requests[0], testNamespace, testName)
	}
}

// TestEnablingANamespaceWakesNothingElse is the other half. This runs on every
// namespace event in the cluster, and waking a ServiceAccount that never asked
// costs a reconcile that can only decide to do nothing.
func TestEnablingANamespaceWakesNothingElse(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		namespaceNamed("team-b"),
		serviceAccountNamed("team-b", "one"),
		serviceAccountNamed("team-b", "two"))

	if requests := c.DatabricksServiceAccounts.identitiesInNamespace(
		context.Background(), namespaceNamed("team-b")); len(requests) != 0 {
		t.Errorf("woke %+v; none of those ServiceAccounts asked for anything", requests)
	}
}

// TestEnablingANamespaceWakesAServiceAccountItCanOnlyRefuse is the third answer
// this mapper has to give, and the only one where waking produces a sentence
// rather than an identity.
//
// A ServiceAccount whose keys cannot be read asks for nothing that can be acted
// on, so it looks like one to skip. The Warning naming the offending key is
// written in Reconcile and nowhere else, and in the order a team is onboarded --
// annotate, then have somebody enable the namespace -- this mapper is the only
// thing that gets Reconcile there. Skipping it means whoever mistyped a value
// waits on an identity that will never arrive and is told nothing about why.
func TestEnablingANamespaceWakesAServiceAccountItCanOnlyRefuse(t *testing.T) {
	t.Parallel()
	mistyped := serviceAccountNamed(testNamespace, "mistyped")
	mistyped.Annotations = map[string]string{
		dbxv1alpha1.ServicePrincipalAnnotationFor(""): strings.ReplaceAll(testOperatorRef.String(), "/", ""),
	}

	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), mistyped, serviceAccountNamed(testNamespace, "no-databricks"))

	requests := c.DatabricksServiceAccounts.identitiesInNamespace(
		context.Background(), mintingNamespace(testNamespace))

	if len(requests) != 1 {
		t.Fatalf("woke %+v, want the ServiceAccount whose key cannot be read", requests)
	}
	if requests[0].Namespace != testNamespace || requests[0].Name != "mistyped" {
		t.Errorf("woke %v, want %s/mistyped", requests[0], testNamespace)
	}
}

// TestANamespaceWithMintingClosedKeepsWhatItHas covers the difference between
// the two things that can be taken away.
//
// The annotation is a workload saying it no longer needs an identity, and
// taking it off revokes. The namespace label is the cluster saying which teams
// are in, and taking it off is not a statement about any workload -- so what
// exists is left alone, still working, and no longer managed.
//
// Read the other way: without this, one `kubectl label namespace ... -` deletes
// every Databricks service principal in that namespace and everything anybody
// granted them, on an edit that reads like turning a feature off.
func TestANamespaceWithMintingClosedKeepsWhatItHas(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.settle(t)

	before := principalOf(t, c.Client)
	if before == nil || identityIn(t, before).ServicePrincipalID == "" {
		t.Fatal("no identity to close minting on")
	}

	namespace := namespaceNamed(testNamespace)
	if err := c.Client.Update(context.Background(), namespace); err != nil {
		t.Fatal(err)
	}
	c.settle(t)

	after := principalOf(t, c.Client)
	if after == nil {
		t.Fatal("the identity was deleted; closing minting on a namespace is not a workload " +
			"asking to be revoked, and rebuilding produces a new applicationId")
	}
	if identityIn(t, after).ServicePrincipalID != identityIn(t, before).ServicePrincipalID {
		t.Errorf("the recorded id changed from %s to %s",
			identityIn(t, before).ServicePrincipalID, identityIn(t, after).ServicePrincipalID)
	}
	if len(stub.deleted) != 0 {
		t.Errorf("deleted %v in Databricks; the identity was meant to be left alone", stub.deleted)
	}

	// Still managed, and reported as working. The old shape stopped managing an
	// identity when its namespace stopped carrying the label, which left it
	// half-owned: alive, unwatched, and reported as something nobody could
	// explain. Minting now gates one thing only -- whether a record is made --
	// and nothing asks about it again afterwards.
	ready := meta.FindStatusCondition(identityIn(t, after).Conditions, conditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("Ready is %v; closing minting is not a statement about an identity that "+
			"already exists, and its workloads are still using it", ready)
	}
}

// TestANamespaceWithMintingClosedMintsNothingNew is the other half: what is left
// alone is what already exists. A ServiceAccount that asks after minting has
// been closed gets nothing, which is the whole point of the gate.
func TestANamespaceWithMintingClosedMintsNothingNew(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		namespaceNamed(testNamespace), asking(testNamespace, testName))
	c.settle(t)

	if principalOf(t, c.Client) != nil {
		t.Error("an identity was minted in a namespace where minting was never opened")
	}
	if len(stub.created) != 0 {
		t.Errorf("created %v in Databricks", stub.created)
	}
}

// TestANamespaceThisOperatorDoesNotServeIsLeftAlone covers what makes two
// operators able to share a cluster.
//
// A cluster serving two teams with two Databricks accounts needs two operators,
// because one operator holds one account's credential and that credential is an
// account admin. The CRDs are shared -- a CRD is named for its group, so no
// prefix separates them -- and both operators therefore see every identity in the
// cluster. Without this, both mint one for the same workload, in two accounts,
// each reporting success, and the pod is told whichever client id was written
// last.
func TestANamespaceThisOperatorDoesNotServeIsLeftAlone(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		databricksAccountServing("somebody-elses-namespace"),
		mintingNamespace(testNamespace), asking(testNamespace, testName))

	c.settle(t)

	if principalOf(t, c.Client) != nil {
		t.Error("an identity was made in a namespace this operator was not told to serve")
	}
	if len(stub.created) != 0 {
		t.Errorf("created %v in Databricks for a namespace nobody named", stub.created)
	}
}

// TestANamespaceThisOperatorServesIsServed is the positive case.
func TestANamespaceThisOperatorServesIsServed(t *testing.T) {
	t.Parallel()
	c := newControllers(t, &stubClients{},
		databricksAccountServing(testNamespace),
		mintingNamespace(testNamespace), asking(testNamespace, testName))

	c.settle(t)

	if principalOf(t, c.Client) == nil {
		t.Error("no identity was made in a namespace this operator serves")
	}
}

// TestNamingNoNamespacesServesNothing covers the default, which has to be the
// closed one.
//
// An operator is installed by one team and holds their account admin credential.
// A permissive default would have it answering every ServiceAccount in the
// cluster that happened to name it, before that team had said a word about where
// their credential may be spent.
func TestNamingNoNamespacesServesNothing(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		databricksAccountServing(), mintingNamespace(testNamespace), asking(testNamespace, testName))

	c.settle(t)

	if principalOf(t, c.Client) != nil {
		t.Error("an identity was made where nobody had said this operator may act")
	}
	if len(stub.created) != 0 {
		t.Errorf("created %v in Databricks against an account naming no namespaces", stub.created)
	}
}

// TestAnIdentityFromAnotherAccountIsNotDeclaredDeleted covers the inference this
// operator must not make.
//
// Databricks answers a lookup in the wrong account with the same 404 it gives
// for an identity that is really gone. Reading the second as the first moves the
// id to removedServicePrincipalId, clears it, and never rebuilds -- and what was
// cleared is the only record of where the live identity is. One edit to the
// DatabricksAccount did that to every identity in a cluster in one reconcile
// interval, with nothing anywhere reporting an error.
func TestAnIdentityFromAnotherAccountIsNotDeclaredDeleted(t *testing.T) {
	t.Parallel()
	serviceAccount := asking(testNamespace, testName)
	existing := recordFor(serviceAccount)
	existing.Status.ServicePrincipalID = "7788"
	existing.Status.ClientID = "app-uuid"
	existing.Status.AccountID = "the-account-it-was-made-in"

	// Absent from this account, which is what a lookup in the wrong one answers.
	stub := &stubClients{accountID: "somewhere-else", gone: "7788"}
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount, existing)

	c.settle(t)

	got := c.issuedOf(t, serviceAccount)
	if got == nil {
		t.Fatal("the record is gone")
	}
	if got.Status.RemovedServicePrincipalID != "" {
		t.Errorf("recorded %s as deleted in Databricks on a lookup made in another account",
			got.Status.RemovedServicePrincipalID)
	}
	if got.Status.ServicePrincipalID != "7788" {
		t.Errorf("servicePrincipalId is %q, want it untouched -- clearing it erases the only "+
			"record of where the live identity is", got.Status.ServicePrincipalID)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if ready == nil || ready.Reason != reasonAccountMismatch {
		t.Fatalf("Ready is %v, want it to say which account it was made in", ready)
	}
	if !strings.Contains(ready.Message, "the-account-it-was-made-in") {
		t.Errorf("message is %q; it has to name the account to point back at", ready.Message)
	}

	// And the owner of the namespace can read all of that without being able to
	// read the operator's own.
	databricksServiceAccount := principalOf(t, c.Client)
	if databricksServiceAccount == nil {
		t.Fatal("nothing was databricksServiceAccount")
	}
	shown := meta.FindStatusCondition(
		identityIn(t, databricksServiceAccount).Conditions, conditionReady)
	if shown == nil || shown.Reason != reasonAccountMismatch {
		t.Errorf("the DatabricksServiceAccount says %v; it carries the record's words or it is no use", shown)
	}
}

// TestTheAccountIsRecordedWithTheId covers where the comparison's other half
// comes from. An id recorded with no account cannot be compared, so the two are
// written together or the check above is inert for everything made after it.
func TestTheAccountIsRecordedWithTheId(t *testing.T) {
	t.Parallel()
	stub := &stubClients{accountID: "the-account"}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.settle(t)

	got := principalOf(t, c.Client)
	if got == nil {
		t.Fatal("no identity was recorded")
	}
	if identityIn(t, got).AccountID != "the-account" {
		t.Errorf("accountId is %q, want the account it was made in; without it nothing can "+
			"tell a lookup in the wrong account from a deletion", identityIn(t, got).AccountID)
	}
}

// TestDeletingTheDatabricksServiceAccountChangesNothing covers the difference
// between a record and a copy of one.
//
// Somebody deleting this object has not said the workload should lose its
// identity -- the ServiceAccount is still asking, and that is the only thing
// that says it wants one. It is not a request to rotate either: it is deleting a
// view, and a view comes back.
func TestDeletingTheDatabricksServiceAccountChangesNothing(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.settle(t)

	before := principalOf(t, c.Client)
	if before == nil || identityIn(t, before).ServicePrincipalID == "" {
		t.Fatal("nothing was built to delete")
	}
	if err := c.Client.Delete(context.Background(), before); err != nil {
		t.Fatal(err)
	}
	c.settle(t)

	if len(stub.deleted) != 0 {
		t.Errorf("deleted %v in Databricks; the ServiceAccount is still asking for it",
			stub.deleted)
	}
	if len(stub.created) != 1 {
		t.Errorf("created %v; deleting a copy is not a reason to make a second identity",
			stub.created)
	}
	after := principalOf(t, c.Client)
	if after == nil {
		t.Fatal("the DatabricksServiceAccount did not come back")
	}
	if identityIn(t, after).ServicePrincipalID != identityIn(t, before).ServicePrincipalID {
		t.Errorf("it came back naming %q, want the same identity %q",
			identityIn(t, after).ServicePrincipalID, identityIn(t, before).ServicePrincipalID)
	}
}

// TestTheObjectComesBackAdoptingWhatIsThere is the other half. The record is
// rebuilt from Databricks rather than by making a second service principal, so
// the applicationId a workload was given does not change under it.
func TestTheObjectComesBackAdoptingWhatIsThere(t *testing.T) {
	t.Parallel()
	stub := &stubClients{foundID: "7788", foundClientID: "app-uuid"}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))

	c.settle(t)

	got := principalOf(t, c.Client)
	if got == nil {
		t.Fatal("no identity was recorded")
	}
	if len(stub.created) != 0 {
		t.Errorf("created %v; there was already one to adopt", stub.created)
	}
	if identityIn(t, got).ServicePrincipalID != "7788" || identityIn(t, got).ClientID != "app-uuid" {
		t.Errorf("status is %+v, want the one that was already there", got.Status)
	}
	if len(stub.looked) == 0 {
		t.Error("nothing was looked for; a lost record would be a lost identity")
	}
}

// TestAPodCarryingATokenForAnotherAudienceIsReported covers the pod that looks
// equipped from every angle and can never reach Databricks.
//
// The audience is one of the three things a federation policy matches, and a
// token minted for another one is refused whatever else is right. Two ways in:
// a pod admitted in the window before this identity had recorded its own
// audience, and a volume of that name this operator did not write. Both leave a
// pod carrying the volume, the variables and a token, so a report that asked
// only whether the volume is there calls it equipped -- and it stays wrong,
// because nothing rewrites a running pod.
func TestAPodCarryingATokenForAnotherAudienceIsReported(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	// Everything else in place -- the volume, the path, the configuration -- so
	// the audience is the only thing that can produce the report.
	pod := rawPodRunningAs(testName, tokenVolumeFor(""))
	pod.Annotations = map[string]string{
		dbxwebhook.ConfigAnnotation: dbxwebhook.Profiles(
			[]dbxv1alpha1.ProjectedIdentity{{
				Profile: testOperatorRef.String(), ClientID: "app-uuid", Audience: testAudience,
			}}),
	}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName), cached(pod))
	c.settle(t)

	condition := equipment(t, c.Client)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("Equipped is %v; the token in that pod was minted for the audience kubelet "+
			"defaults to, which no federation policy names", condition)
	}
	if !strings.Contains(condition.Message, "another audience") {
		t.Errorf("message is %q; it has to say which of the three things is wrong, because "+
			"the pod carries everything else and looks right", condition.Message)
	}
	if !strings.Contains(condition.Message, "runner-"+testName) {
		t.Errorf("message is %q; it has to name the pod", condition.Message)
	}
}

// TestATokenThatCouldNotBeReadRecoversWhenItCan covers a failure that used to be
// permanent for the life of the process.
//
// The issuer every federation policy names and the audience every pod is given
// are read out of the operator's own projected token. Reading them once, at
// startup, meant a file that was not there for that moment was not there for
// ever: no identity could converge again, while the account controller re-read
// the same file every pass and reported everything healthy. The only fix was a
// restart, and the message -- which talks about the token -- sends whoever reads
// it to look at the volume, which is fine.
func TestATokenThatCouldNotBeReadRecoversWhenItCan(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))

	good := c.Issued.TokenPath
	c.Issued.TokenPath = filepath.Join(t.TempDir(), "not-there-yet")
	c.settle(t)

	if len(stub.created) != 0 {
		t.Errorf("created %v while the operator could not read its own token; the issuer that "+
			"went into those policies came from nowhere", stub.created)
	}
	principal := principalOf(t, c.Client)
	if principal == nil {
		t.Fatal("nothing was projected, so nothing says why this is stuck")
	}
	ready := meta.FindStatusCondition(identityIn(t, principal).Conditions, conditionReady)
	if ready == nil || ready.Reason != reasonUnprepared {
		t.Fatalf("Ready is %v, want %s -- the DatabricksAccount is fine and NotConfigured is "+
			"what sends its reader there", ready, reasonUnprepared)
	}

	// And the file appears, as it does a moment after a pod starts.
	c.Issued.TokenPath = good
	c.settle(t)

	if len(stub.created) != 1 {
		t.Errorf("created %v after the token could be read; one bad moment is still permanent",
			stub.created)
	}
	if got := principalOf(t, c.Client); identityIn(t, got).ClientID == "" {
		t.Errorf("status is %+v, want the identity to have converged", got.Status)
	}
}

// TestATokenCarryingNoAudienceIsRefusedRatherThanSent covers what would be
// written if the answer were taken as it came.
//
// Measured against a live account: a federation policy matches the audience, and
// a token minted for any other is refused with the audience it presented echoed
// back. So a policy naming no audience is one nothing can satisfy -- and the
// identity would look built, report Ready, equip its pods, and never exchange.
func TestATokenCarryingNoAudienceIsRefusedRatherThanSent(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace), asking(testNamespace, testName))
	c.Issued.TokenPath = writeTokenAs(t, testIssuer,
		"system:serviceaccount:operators:controller-manager", "")
	c.settle(t)

	if len(stub.policies) != 0 {
		t.Errorf("wrote %v; a policy naming no audience is one no token can satisfy", stub.policies)
	}
	principal := principalOf(t, c.Client)
	if principal == nil {
		t.Fatal("nothing was projected")
	}
	ready := meta.FindStatusCondition(identityIn(t, principal).Conditions, conditionReady)
	if ready == nil || ready.Status == metav1.ConditionTrue {
		t.Errorf("Ready is %v; nothing usable was built", ready)
	}
}

// TestAnAnnotationNamingAnotherOperatorIsNotThisOnesRequest covers the whole of
// how one operator tells its requests from another's.
//
// The annotation names the operator it asks, by its DatabricksAccount's namespace
// and name. Two operators cannot share that -- the API server will not hold two
// objects of one kind at one address -- which a chosen name could not promise.
func TestAnAnnotationNamingAnotherOperatorIsNotThisOnesRequest(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	c := newControllers(t, stub,
		mintingNamespace(testNamespace),
		askingOf(testNamespace, testName, "somebody-else/databricks-account"))
	c.settle(t)

	if len(stub.created) != 0 {
		t.Errorf("created %v for a request naming another operator", stub.created)
	}
	if principalOf(t, c.Client) != nil {
		t.Error("projected an identity this operator was not asked for")
	}
}

// TestAServiceAccountAskingSeveralOperatorsIsAskingThisOne is the other half:
// annotations naming several are a request to each of them, and this operator
// answers its own keys and leaves the rest where they are.
func TestAServiceAccountAskingSeveralOperatorsIsAskingThisOne(t *testing.T) {
	t.Parallel()
	stub := &stubClients{}
	serviceAccount := asking(testNamespace, testName)
	serviceAccount.Annotations[dbxv1alpha1.ServicePrincipalAnnotationFor("elsewhere")] =
		"somebody-else/databricks-account"
	c := newControllers(t, stub, mintingNamespace(testNamespace), serviceAccount)
	c.settle(t)

	principal := principalOf(t, c.Client)
	if principal == nil {
		t.Fatal("an operator named in the annotations did not answer")
	}
	if len(principal.Status.Identities) != 1 {
		t.Fatalf("the DatabricksServiceAccount carries %+v; this operator was asked for one identity and the "+
			"other key is another operator's to answer", principal.Status.Identities)
	}
	if identityIn(t, principal).Profile != testOperatorRef.String() {
		t.Errorf("the entry is keyed %q, want this operator's own key",
			identityIn(t, principal).Profile)
	}
	if len(stub.created) != 1 {
		t.Errorf("created %v, want the one this operator was asked for", stub.created)
	}
}

// TestARecordThatHasNotReachedTheCacheIsNotDereferenced is the panic this
// operator took on a real cluster, on 2026-09-03, the first time the end-to-end
// suite ran: a nil pointer in entryFor, one reconcile after the record was made.
//
// Reads go through the informer's cache and the cache lags its own writes, so a
// record read back immediately after being created comes back "not found" --
// which this operator's own reader reports as nil with no error. Nothing said
// that could happen, so the value was used.
//
// What the fix has to be is not "check for nil": it is that a pass which cannot
// see what it just made must say so and come back, because an entry built from a
// record it could not read would report an identity nobody issued.
func TestARecordThatHasNotReachedTheCacheIsNotDereferenced(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := dbxv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	// A cache that never catches up, which is the same shape as one that has not
	// caught up yet and is what makes the failure a test rather than a race.
	lagging := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(mintingNamespace(testNamespace), asking(testNamespace, testName)).
		WithStatusSubresource(
			&dbxv1alpha1.DatabricksServiceAccount{},
			&dbxv1alpha1.IssuedDatabricksServicePrincipal{},
			&dbxv1alpha1.DatabricksAccount{},
		).
		WithInterceptorFuncs(interceptor.Funcs{
			// The record exists -- another pass wrote it -- and this pass's
			// cache does not have it. Both halves are needed: a Create that
			// succeeds no longer reads anything back, so only the pass that
			// finds one already there can be left holding nothing.
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.CreateOption) error {
				if _, ok := obj.(*dbxv1alpha1.IssuedDatabricksServicePrincipal); ok {
					return apierrors.NewAlreadyExists(
						schema.GroupResource{Group: dbxv1alpha1.GroupVersion.Group,
							Resource: "issueddatabricksserviceprincipals"}, obj.GetName())
				}
				return c.Create(ctx, obj, opts...)
			},
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*dbxv1alpha1.IssuedDatabricksServicePrincipal); ok {
					return apierrors.NewNotFound(
						schema.GroupResource{Group: dbxv1alpha1.GroupVersion.Group,
							Resource: "issueddatabricksserviceprincipals"}, key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	reconciler := &DatabricksServiceAccountReconciler{
		Client:                          lagging,
		Scheme:                          scheme,
		DatabricksAccountNamespacedName: types.NamespacedName{Namespace: operatorNamespace, Name: "databricks-account"},
	}

	// The assertion is that this returns at all. Before the fix it panicked, and
	// a panicking reconcile is not an error the workqueue retries -- it is the
	// controller's worker going down.
	_, _, err := reconciler.entryFor(context.Background(),
		asking(testNamespace, testName),
		dbxv1alpha1.Request{Operator: reconciler.DatabricksAccountNamespacedName}, true)
	if err == nil {
		t.Fatal("a record that could not be read back produced no error; the pass built an entry " +
			"from a record it never saw")
	}
	if !strings.Contains(err.Error(), "cache") {
		t.Errorf("the error is %q; it should say the record has not reached the cache, because "+
			"the answer is to come back rather than to look somewhere else", err)
	}
}

// TestAnIdentityWithNoClientIdDescribesNoPod covers the guard describes needs in
// order to answer a question about a string at all.
//
// The webhook writes nothing for an identity with no client id, and every string
// contains the empty one. Without the guard the containment check answers yes
// for every pod in the namespace, so Equipped reports True for pods carrying a
// profile for a service principal that is not there -- and clearing the client
// id is exactly what recording one as removed in Databricks does, which is the
// moment those pods are worth reporting.
func TestAnIdentityWithNoClientIdDescribesNoPod(t *testing.T) {
	t.Parallel()
	removed := dbxv1alpha1.ProjectedIdentity{
		Profile: testOperatorRef.String(), Audience: testAudience,
	}

	for _, pod := range []*corev1.Pod{
		equippedPod(testName),
		rawPodRunningAs(testName),
	} {
		if describes(pod, removed) {
			t.Errorf("pod %s is reported as describing an identity with no client id; every "+
				"pod in the namespace would be, and Equipped would say True for a service "+
				"principal that is not there", pod.Name)
		}
	}
}

// TestAPodDescribesTheIdentityItWasAdmittedWith is the pair of answers Equipped
// is built out of: the pod carries what this identity says now, or it carries an
// older answer.
//
// The stale case is the one nothing else notices. A pod admitted before
// Databricks assigned the client id is holding a profile that was right when it
// was written, and nothing rewrites a running pod -- so the only way it is ever
// reported is this comparison.
func TestAPodDescribesTheIdentityItWasAdmittedWith(t *testing.T) {
	t.Parallel()
	converged := dbxv1alpha1.ProjectedIdentity{
		Profile: testOperatorRef.String(), ClientID: "app-uuid", Audience: testAudience,
	}
	pod := equippedPod(testName, converged)

	if !describes(pod, converged) {
		t.Error("a pod admitted with this identity's own profile is not reported as carrying " +
			"it; every equipped pod would be asked to be recreated for ever")
	}

	reassigned := converged
	reassigned.ClientID = "another-app-uuid"
	if describes(pod, reassigned) {
		t.Error("a pod carrying a profile written before the client id changed is reported as " +
			"up to date; nothing rewrites a running pod, so nothing else would ever say so")
	}
}

// TestAPodNamingNoServiceAccountRunsAsDefault covers the substitution Kubernetes
// makes and the API object does not show.
//
// An empty spec.serviceAccountName is a pod running as "default", which can hold
// an identity like any other. Read literally it matches no ServiceAccount, so
// every such pod drops out of the count of what this DatabricksServiceAccount is
// answerable for -- silently, and in the direction of reporting Equipped True
// for pods that were never equipped.
func TestAPodNamingNoServiceAccountRunsAsDefault(t *testing.T) {
	t.Parallel()
	named := rawPodRunningAs(testName)
	if got := serviceAccountOf(named); got != testName {
		t.Errorf("serviceAccountOf is %q, want %q", got, testName)
	}

	unnamed := rawPodRunningAs("")
	if got := serviceAccountOf(unnamed); got != "default" {
		t.Errorf("serviceAccountOf is %q, want %q -- the pod runs as default and would be "+
			"counted against no DatabricksServiceAccount at all", got, "default")
	}
}
