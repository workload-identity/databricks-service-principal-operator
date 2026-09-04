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
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	dbxwebhook "github.com/workload-identity/databricks-service-principal-operator/internal/webhook"
)

// askingFor is a ServiceAccount naming this operator once per identity: one
// annotation key each, which is what makes withdrawing one and mistyping one
// different edits.
func askingFor(namespace, name string, identities ...string) *corev1.ServiceAccount {
	serviceAccount := serviceAccountNamed(namespace, name)
	serviceAccount.Annotations = make(map[string]string, len(identities))
	for _, identity := range identities {
		serviceAccount.Annotations[dbxv1alpha1.ServicePrincipalAnnotationFor(identity)] =
			testOperator.String()
	}
	return serviceAccount
}

// recordsOf is every record this operator holds for one ServiceAccount.
func recordsOf(t *testing.T, c client.Client, namespace, name string) []dbxv1alpha1.IssuedDatabricksServicePrincipal {
	t.Helper()
	var all dbxv1alpha1.IssuedDatabricksServicePrincipalList
	if err := c.List(context.Background(), &all, client.InNamespace(testRecords)); err != nil {
		t.Fatal(err)
	}
	var mine []dbxv1alpha1.IssuedDatabricksServicePrincipal
	for _, record := range all.Items {
		if record.Spec.ServiceAccount.Namespace == namespace &&
			record.Spec.ServiceAccount.Name == name && record.DeletionTimestamp.IsZero() {
			mine = append(mine, record)
		}
	}
	return mine
}

// TestOneServiceAccountGetsTwoIdentitiesFromOneOperator is the requirement this
// whole step exists for.
//
// A workload reading with one service principal and writing with another is
// ordinary practice in Databricks, and both live in one account. The design
// before this had the number of identities bounded by the number of operators
// serving the cluster, which had "two identities in one account" answered with
// "run a second operator" -- a conclusion nothing about Databricks asks for.
func TestOneServiceAccountGetsTwoIdentitiesFromOneOperator(t *testing.T) {
	t.Parallel()
	stub := &stubClients{accountID: "the-account"}
	h := newHarness(t, stub,
		mintingNamespace(testNamespace), askingFor(testNamespace, testName, "reader", "writer"))

	h.settle(t)

	records := recordsOf(t, h.Client, testNamespace, testName)
	if len(records) != 2 {
		t.Fatalf("wrote %d records, want one per identity: %+v", len(records), records)
	}
	named := []string{records[0].Spec.Identity, records[1].Spec.Identity}
	slices.Sort(named)
	if named[0] != "reader" || named[1] != "writer" {
		t.Errorf("records name identities %v, want reader and writer", named)
	}

	if len(stub.created) != 2 {
		t.Fatalf("created %v in Databricks, want one service principal per identity", stub.created)
	}
	if records[0].Status.ServicePrincipalID == records[1].Status.ServicePrincipalID {
		t.Errorf("both records name service principal %q; one identity was made and recorded "+
			"twice", records[0].Status.ServicePrincipalID)
	}

	databricksServiceAccount := principalOf(t, h.Client)
	if databricksServiceAccount == nil {
		t.Fatal("nothing was projected")
	}
	if len(databricksServiceAccount.Status.Identities) != 2 {
		t.Fatalf("the DatabricksServiceAccount carries %d entries, want both: %+v",
			len(databricksServiceAccount.Status.Identities),
			databricksServiceAccount.Status.Identities)
	}
	for _, want := range []string{"reader", "writer"} {
		entry, carried := databricksServiceAccount.Status.Identity(want)
		if !carried {
			t.Fatalf("the DatabricksServiceAccount has no entry for %q; the workload cannot name a profile "+
				"it cannot see", want)
		}
		if entry.ClientID == "" {
			t.Errorf("%q carries no client id, so nothing can exchange for it", want)
		}
		if entry.Operator != testOperator.String() {
			t.Errorf("%q says it was issued by %q; every operator reads ownership off that "+
				"field, so an entry naming nobody is one every one of them leaves behind",
				want, entry.Operator)
		}
	}
}

// TestDroppingOneIdentityKeepsTheOther is the half that does real damage.
//
// The question asked every pass used to be "does this ServiceAccount still ask
// this operator for anything". With several identities that question destroys
// all of them or none: a workload that had two and dropped one would either keep
// a service principal nothing asks for, or lose the one it still uses.
func TestDroppingOneIdentityKeepsTheOther(t *testing.T) {
	t.Parallel()
	stub := &stubClients{accountID: "the-account"}
	h := newHarness(t, stub,
		mintingNamespace(testNamespace), askingFor(testNamespace, testName, "reader", "writer"))
	h.settle(t)

	before := recordsOf(t, h.Client, testNamespace, testName)
	if len(before) != 2 {
		t.Fatalf("this test is about dropping one of two; there are %d", len(before))
	}
	var dropped string
	for _, record := range before {
		if record.Spec.Identity == "reader" {
			dropped = record.Status.ServicePrincipalID
		}
	}

	// The annotations are the request and nothing else is: one key leaves them.
	serviceAccount := &corev1.ServiceAccount{}
	if err := h.Client.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testName}, serviceAccount); err != nil {
		t.Fatal(err)
	}
	delete(serviceAccount.Annotations, dbxv1alpha1.ServicePrincipalAnnotationFor("reader"))
	if err := h.Client.Update(context.Background(), serviceAccount); err != nil {
		t.Fatal(err)
	}

	h.settle(t)

	after := recordsOf(t, h.Client, testNamespace, testName)
	if len(after) != 1 {
		t.Fatalf("%d records remain, want only the one still asked for: %+v", len(after), after)
	}
	if after[0].Spec.Identity != "writer" {
		t.Errorf("the record that remains is %q; the one still asked for was destroyed",
			after[0].Spec.Identity)
	}
	if !slices.Contains(stub.deleted, dropped) {
		t.Errorf("deleted %v in Databricks, want the dropped identity %q -- a service principal "+
			"nothing asks for and nothing records is what this operator exists to prevent",
			stub.deleted, dropped)
	}
	if slices.Contains(stub.deleted, after[0].Status.ServicePrincipalID) {
		t.Errorf("deleted %q as well; that is the identity the workload still uses",
			after[0].Status.ServicePrincipalID)
	}

	databricksServiceAccount := principalOf(t, h.Client)
	if databricksServiceAccount == nil {
		t.Fatal("the DatabricksServiceAccount is gone; one identity remains")
	}
	if _, still := databricksServiceAccount.Status.Identity("reader"); still {
		t.Error("the DatabricksServiceAccount still shows the dropped identity, so a pod would go on being " +
			"equipped for one that no longer exists")
	}
	if _, kept := databricksServiceAccount.Status.Identity("writer"); !kept {
		t.Error("the DatabricksServiceAccount lost the identity that is still asked for")
	}
}

// TestAddingASecondIdentityLeavesTheFirstAlone covers the direction a workload
// actually grows in: it has one, and asks for another.
//
// The first must not be re-made, because a new service principal has a new
// client id and everything granted to the old one then points at nothing.
func TestAddingASecondIdentityLeavesTheFirstAlone(t *testing.T) {
	t.Parallel()
	stub := &stubClients{accountID: "the-account"}
	h := newHarness(t, stub, mintingNamespace(testNamespace), asking(testNamespace, testName))
	h.settle(t)

	first := recordsOf(t, h.Client, testNamespace, testName)
	if len(first) != 1 {
		t.Fatalf("this test starts from one identity; there are %d", len(first))
	}
	was := first[0].Status.ServicePrincipalID

	serviceAccount := &corev1.ServiceAccount{}
	if err := h.Client.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testName}, serviceAccount); err != nil {
		t.Fatal(err)
	}
	serviceAccount.Annotations[dbxv1alpha1.ServicePrincipalAnnotationFor("writer")] =
		testOperator.String()
	if err := h.Client.Update(context.Background(), serviceAccount); err != nil {
		t.Fatal(err)
	}

	h.settle(t)

	after := recordsOf(t, h.Client, testNamespace, testName)
	if len(after) != 2 {
		t.Fatalf("%d records, want the original and the new one: %+v", len(after), after)
	}
	kept := false
	for _, record := range after {
		if record.Spec.Identity == "" && record.Status.ServicePrincipalID == was {
			kept = true
		}
	}
	if !kept {
		t.Errorf("the first identity is no longer %q; its client id changed, so everything "+
			"granted to it in Databricks now points at nothing", was)
	}
	if len(stub.deleted) != 0 {
		t.Errorf("deleted %v while adding an identity", stub.deleted)
	}
}

// TestWithdrawingNamedIdentitiesTakesTheDatabricksServiceAccountWithThem covers
// the whole of a ServiceAccount's request going away when the identities were
// named.
//
// Withdrawing removes the entries this operator owns, and a named identity's key
// is the name its asker chose, which says nothing about who issued it. An
// operator working ownership out of the key finds none of its named entries, so
// nothing is withdrawn and the object is not deleted either, because entries
// remain.
//
// What that leaves is a DatabricksServiceAccount reporting Ready for identities
// whose records are deleted and whose service principals are destroyed. It is
// the one state this object is supposed to be incapable of: it decides nothing,
// so the only thing it can be is wrong.
func TestWithdrawingNamedIdentitiesTakesTheDatabricksServiceAccountWithThem(t *testing.T) {
	t.Parallel()
	stub := &stubClients{accountID: "the-account"}
	h := newHarness(t, stub,
		mintingNamespace(testNamespace), askingFor(testNamespace, testName, "reader", "writer"))
	h.settle(t)

	if databricksServiceAccount := principalOf(t, h.Client); databricksServiceAccount == nil ||
		len(databricksServiceAccount.Status.Identities) != 2 {
		t.Fatalf("this test is about withdrawing two named identities; there are %v",
			databricksServiceAccount)
	}

	serviceAccount := &corev1.ServiceAccount{}
	if err := h.Client.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testName}, serviceAccount); err != nil {
		t.Fatal(err)
	}
	serviceAccount.Annotations = nil
	if err := h.Client.Update(context.Background(), serviceAccount); err != nil {
		t.Fatal(err)
	}

	h.settle(t)

	if left := recordsOf(t, h.Client, testNamespace, testName); len(left) != 0 {
		t.Errorf("%d records remain for a ServiceAccount that asks for nothing: %+v", len(left), left)
	}
	if len(stub.deleted) != 2 {
		t.Errorf("deleted %v in Databricks, want both identities", stub.deleted)
	}
	if databricksServiceAccount := principalOf(t, h.Client); databricksServiceAccount != nil {
		t.Errorf("the DatabricksServiceAccount is still there, carrying %+v. Its records are gone and its "+
			"service principals are destroyed, so every word of it is false -- and it reports "+
			"Ready, which is the one thing a copy cannot be trusted to be wrong about",
			databricksServiceAccount.Status.Identities)
	}
}

// TestEquipmentIsReportedPerNamedIdentity covers the equipment check under the
// shape everything else in this file is about.
//
// The check finds a pod's token by the path its identity's key produces, and
// compares the configuration the pod carries against what that identity says
// now. Both are keyed by the name the asker gave -- so a pod equipped for one
// identity and not another has to read as equipped for one and not another,
// rather than for both or neither.
func TestEquipmentIsReportedPerNamedIdentity(t *testing.T) {
	t.Parallel()
	const reader, writer = "reader", "writer"

	// A pod carrying the reader's token and the reader's configuration, and
	// nothing of the writer's -- which is what a pod admitted before the second
	// identity converged looks like.
	pod := rawPodRunningAs(testName, corev1.Volume{
		Name: dbxwebhook.TokenVolume,
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{{
					ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
						Path:     dbxwebhook.TokenProjectionPathFor(reader),
						Audience: testAudience,
					},
				}},
			},
		},
	})
	pod.Annotations = map[string]string{
		dbxwebhook.ConfigAnnotation: dbxwebhook.Configuration(
			[]dbxv1alpha1.ProjectedIdentity{{
				Profile: reader, ClientID: "app-uuid", Audience: testAudience,
			}}),
	}

	h := newHarness(t, &stubClients{accountID: "the-account"},
		mintingNamespace(testNamespace),
		askingFor(testNamespace, testName, "reader", "writer"), cached(pod))
	h.settle(t)

	databricksServiceAccount := principalOf(t, h.Client)
	if databricksServiceAccount == nil {
		t.Fatal("nothing was projected")
	}
	for _, want := range []struct {
		profile string
		status  metav1.ConditionStatus
		says    string
		why     string
	}{
		{reader, metav1.ConditionTrue, "carries what it needs",
			"the pod has this identity's token and its configuration"},
		// And the reason has to be this one. A pod holding another identity's
		// token is not a pod holding a stale configuration, and the two send
		// whoever reads it to fix different things -- recreate the pod versus
		// wait for Databricks. Asserting only True or False lets a lookup that
		// finds any token at all still read as correct here.
		{writer, metav1.ConditionFalse, "running without this identity's Databricks token",
			"the pod has no token for this identity, whatever it holds for another"},
	} {
		entry, carried := databricksServiceAccount.Status.Identity(want.profile)
		if !carried {
			t.Fatalf("no entry for %q", want.profile)
		}
		got := meta.FindStatusCondition(entry.Conditions, conditionEquipped)
		if got == nil || got.Status != want.status {
			t.Errorf("Equipped for %q is %v, want %v -- %s", want.profile, got, want.status, want.why)
			continue
		}
		if !strings.Contains(got.Message, want.says) {
			t.Errorf("Equipped for %q says %q, want it to say %q -- %s",
				want.profile, got.Message, want.says, want.why)
		}
	}
}

// TestAValueThatCannotBeReadDestroysNothing is the bug this whole shape exists
// to remove, and it was measured rather than imagined: a single "/" typed out of
// the annotation deleted the service principal and every grant anybody had made
// on it.
//
// A key that is there and cannot be read asks for nothing and withdraws nothing.
// Read as a withdrawal it is the most expensive possible answer to a typo, and
// the person who made it gets no chance to fix it -- by the time they look, the
// identity is gone and a new one has a new client id.
func TestAValueThatCannotBeReadDestroysNothing(t *testing.T) {
	t.Parallel()
	stub := &stubClients{accountID: "the-account"}
	h := newHarness(t, stub,
		mintingNamespace(testNamespace), askingFor(testNamespace, testName, "reader", "writer"))
	h.settle(t)

	if before := recordsOf(t, h.Client, testNamespace, testName); len(before) != 2 {
		t.Fatalf("this test mistypes one of two identities; there are %d", len(before))
	}
	was, kept := principalOf(t, h.Client).Status.Identity("reader")
	if !kept {
		t.Fatal("the identity this test is about was never projected")
	}

	// The edit that did the damage: the "/" between the operator's namespace and
	// its DatabricksAccount, gone.
	mistyped := dbxv1alpha1.ServicePrincipalAnnotationFor("reader")
	serviceAccount := &corev1.ServiceAccount{}
	if err := h.Client.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testName}, serviceAccount); err != nil {
		t.Fatal(err)
	}
	serviceAccount.Annotations[mistyped] = strings.ReplaceAll(testOperator.String(), "/", "")
	if err := h.Client.Update(context.Background(), serviceAccount); err != nil {
		t.Fatal(err)
	}

	h.settle(t)

	if after := recordsOf(t, h.Client, testNamespace, testName); len(after) != 2 {
		t.Fatalf("%d records remain, want both: %+v. A mistyped value destroyed an identity, "+
			"which takes the service principal and everything granted to it", len(after), after)
	}
	if len(stub.deleted) != 0 {
		t.Errorf("deleted %v in Databricks over a typo somebody could have fixed in a second",
			stub.deleted)
	}

	databricksServiceAccount := principalOf(t, h.Client)
	if databricksServiceAccount == nil {
		t.Fatal("the DatabricksServiceAccount is gone; both identities are still there")
	}
	still, carried := databricksServiceAccount.Status.Identity("reader")
	if !carried {
		t.Fatal("the DatabricksServiceAccount dropped the mistyped identity. Its owner can no longer see the " +
			"identity they still hold, and the webhook equips no new pod for it")
	}
	if still.ClientID != was.ClientID || still.ServicePrincipalID != was.ServicePrincipalID {
		t.Errorf("the entry now says %+v and said %+v; nothing was asked for, so nothing about "+
			"it should have moved", still, was)
	}
	if _, other := databricksServiceAccount.Status.Identity("writer"); !other {
		t.Error("the identity whose key was never touched went with it")
	}
}

// TestAServiceAccountWhoseOnlyKeyIsMistypedKeepsItsIdentity is the worst case of
// the rule, and the only fixture that reaches it.
//
// The other two tests of this rule each leave something understood --
// TestAValueThatCannotBeReadDestroysNothing has a second identity whose key is
// still good, and TestAKeyThatIsGoneIsWithdrawnWhileAKeyThatWillNotParseIsHeld a
// key that was deleted rather than mistyped -- so in both the withdraw branch is
// held open by a request this operator can still read, and neither notices if
// that branch stops consulting what is being held. A ServiceAccount asking for
// one identity and mistyping it has nothing else: what stands between a missing
// "/" and a destroyed service principal is that one check, and this is what goes
// red when it goes.
func TestAServiceAccountWhoseOnlyKeyIsMistypedKeepsItsIdentity(t *testing.T) {
	t.Parallel()
	stub := &stubClients{accountID: "the-account"}
	h := newHarness(t, stub,
		mintingNamespace(testNamespace), askingFor(testNamespace, testName, ""))
	h.settle(t)

	if before := recordsOf(t, h.Client, testNamespace, testName); len(before) != 1 {
		t.Fatalf("this test asks for one identity and mistypes it; there are %d", len(before))
	}
	was, issued := principalOf(t, h.Client).Status.Identity(testOperator.String())
	if !issued {
		t.Fatal("the identity this test is about was never projected")
	}

	serviceAccount := &corev1.ServiceAccount{}
	if err := h.Client.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testName}, serviceAccount); err != nil {
		t.Fatal(err)
	}
	serviceAccount.Annotations[dbxv1alpha1.ServicePrincipalAnnotationFor("")] =
		strings.ReplaceAll(testOperator.String(), "/", "")
	if err := h.Client.Update(context.Background(), serviceAccount); err != nil {
		t.Fatal(err)
	}

	h.settle(t)

	if after := recordsOf(t, h.Client, testNamespace, testName); len(after) != 1 {
		t.Fatalf("%d records remain, want the one nobody withdrew: %+v. The key is still there "+
			"and still names this identity, badly", len(after), after)
	}
	if len(stub.deleted) != 0 {
		t.Fatalf("deleted %v. A workload lost its service principal, its federation policy and "+
			"every grant made on it because somebody left out a %q", stub.deleted, "/")
	}

	databricksServiceAccount := principalOf(t, h.Client)
	if databricksServiceAccount == nil {
		t.Fatal("the DatabricksServiceAccount is gone, so its owner cannot see the identity they still hold " +
			"and the webhook equips no new pod for it")
	}
	still, carried := databricksServiceAccount.Status.Identity(testOperator.String())
	if !carried {
		t.Fatal("the DatabricksServiceAccount dropped the only identity on it")
	}
	if still.ClientID != was.ClientID {
		t.Errorf("the entry names client %s and named %s; nothing was asked for, so nothing "+
			"about it should have moved", still.ClientID, was.ClientID)
	}
}

// TestAKeyThatIsGoneIsWithdrawnWhileAKeyThatWillNotParseIsHeld is the sentence
// the whole change exists to make true, in one edit.
//
// Written as entries of one comma-separated value the two were the same input --
// an entry that is not in the string any more -- so the operator had to guess,
// and guessing withdrawal destroys an identity over a typo. As separate keys
// they are a key that is gone and a key that is there, and this asserts they are
// answered differently on a ServiceAccount where both happened at once.
func TestAKeyThatIsGoneIsWithdrawnWhileAKeyThatWillNotParseIsHeld(t *testing.T) {
	t.Parallel()
	stub := &stubClients{accountID: "the-account"}
	h := newHarness(t, stub,
		mintingNamespace(testNamespace), askingFor(testNamespace, testName, "", "reader"))
	h.settle(t)

	before := recordsOf(t, h.Client, testNamespace, testName)
	if len(before) != 2 {
		t.Fatalf("this test starts from an unnamed identity and a named one; there are %d",
			len(before))
	}
	var withdrawn string
	for _, record := range before {
		if record.Spec.Identity == "" {
			withdrawn = record.Status.ServicePrincipalID
		}
	}

	serviceAccount := &corev1.ServiceAccount{}
	if err := h.Client.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testName}, serviceAccount); err != nil {
		t.Fatal(err)
	}
	delete(serviceAccount.Annotations, dbxv1alpha1.ServicePrincipalAnnotation)
	serviceAccount.Annotations[dbxv1alpha1.ServicePrincipalAnnotationFor("reader")] = "ops-a"
	if err := h.Client.Update(context.Background(), serviceAccount); err != nil {
		t.Fatal(err)
	}

	h.settle(t)

	after := recordsOf(t, h.Client, testNamespace, testName)
	if len(after) != 1 || after[0].Spec.Identity != "reader" {
		t.Fatalf("the records left are %+v, want the one whose key is still there. A key that is "+
			"gone is a withdrawal and a key that will not parse is not, and this operator "+
			"answered both the same way", after)
	}
	if !slices.Contains(stub.deleted, withdrawn) {
		t.Errorf("deleted %v, want the identity whose key was removed (%q) -- a service principal "+
			"nothing asks for and nothing records is what this operator exists to prevent",
			stub.deleted, withdrawn)
	}
	if len(stub.deleted) != 1 {
		t.Errorf("deleted %v; the mistyped key's identity went with the withdrawn one",
			stub.deleted)
	}

	databricksServiceAccount := principalOf(t, h.Client)
	if databricksServiceAccount == nil {
		t.Fatal("the DatabricksServiceAccount is gone; the mistyped identity is still there and still working")
	}
	if _, gone := databricksServiceAccount.Status.Identity(testOperator.String()); gone {
		t.Error("the DatabricksServiceAccount still shows the withdrawn identity, so a pod would go on being " +
			"equipped for one whose service principal is destroyed")
	}
	if _, held := databricksServiceAccount.Status.Identity("reader"); !held {
		t.Error("the DatabricksServiceAccount dropped the mistyped identity, which nobody withdrew")
	}
}
