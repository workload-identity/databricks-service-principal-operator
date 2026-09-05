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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

func databricksAccountWith(name string, ready metav1.ConditionStatus) *dbxv1alpha1.DatabricksAccount {
	object := databricksAccountNamed(name)
	setCondition(&object.Status.Conditions, 0, conditionReady, ready, reasonAccountReady, "checked")
	return object
}

// TestOnlyAChangeARecordDependsOnWakesTheIdentities covers the predicate that
// keeps a wake-up from being a loop.
//
// Every reconcile of the account writes its status, and most of those writes
// change nothing an identity cares about. Waking every identity in the cluster
// on each of them would put the whole set back in the queue once a minute,
// forever, for no reason.
func TestOnlyAChangeARecordDependsOnWakesTheIdentities(t *testing.T) {
	t.Parallel()
	selected := types.NamespacedName{Namespace: operatorNamespace, Name: databricksAccountName}
	p := databricksAccountChanged(selected)

	notReady := databricksAccountWith(databricksAccountName, metav1.ConditionFalse)
	ready := databricksAccountWith(databricksAccountName, metav1.ConditionTrue)

	if !p.Update(event.UpdateEvent{ObjectOld: notReady, ObjectNew: ready}) {
		t.Error("becoming usable did not wake anything; every identity would wait out its own interval")
	}
	if !p.Update(event.UpdateEvent{ObjectOld: ready, ObjectNew: notReady}) {
		t.Error("ceasing to be usable did not wake anything")
	}
	if p.Update(event.UpdateEvent{ObjectOld: ready, ObjectNew: databricksAccountWith(databricksAccountName, metav1.ConditionTrue)}) {
		t.Error("a status write that changed nothing woke every identity in the cluster; " +
			"that happens once a minute and never stops")
	}
	if !p.Create(event.CreateEvent{Object: ready}) {
		t.Error("an account that arrives already usable did not wake anything")
	}
	if p.Create(event.CreateEvent{Object: notReady}) {
		t.Error("an account that arrives unusable woke everything, and there is nothing to answer with")
	}
}

// databricksAccountServingWith is a usable account naming some namespaces, which is the
// pair of facts the predicate reads.
func databricksAccountServingWith(name string, namespaces ...string) *dbxv1alpha1.DatabricksAccount {
	object := databricksAccountWith(name, metav1.ConditionTrue)
	object.Spec.Namespaces = namespaces
	return object
}

// TestChangingWhichNamespacesAreServedWakesEveryRecord covers the edit that
// starts and ends a removal reaching the records that carry it out.
//
// Taking a namespace off spec.namespaces changes no record, no ServiceAccount
// and no Namespace, so this watch is the only thing that hears it. Without it
// the trust stays in place, and the identities go on being exchangeable, until
// each record comes round on its own interval -- ten minutes for a settled one.
// Putting the namespace back has the same shape and the same wait.
func TestChangingWhichNamespacesAreServedWakesEveryRecord(t *testing.T) {
	t.Parallel()
	selected := types.NamespacedName{Namespace: operatorNamespace, Name: databricksAccountName}
	p := databricksAccountChanged(selected)

	both := databricksAccountServingWith(databricksAccountName, "team-a", "team-b")
	one := databricksAccountServingWith(databricksAccountName, "team-a")

	if !p.Update(event.UpdateEvent{ObjectOld: both, ObjectNew: one}) {
		t.Error("a namespace taken out of scope woke no record; the trust it was supposed to " +
			"take off stays in place until every record's own ten-minute interval comes round")
	}
	if !p.Update(event.UpdateEvent{ObjectOld: one, ObjectNew: both}) {
		t.Error("a namespace named again woke no record; the identities there are exchangeable " +
			"again only after the same wait")
	}
	if p.Update(event.UpdateEvent{
		ObjectOld: both,
		ObjectNew: databricksAccountServingWith(databricksAccountName, "team-b", "team-a"),
	}) {
		t.Error("reordering the list woke every record in the cluster; it is a set, and the two " +
			"spellings are one declaration")
	}
	if p.Update(event.UpdateEvent{
		ObjectOld: databricksAccountServingWith("someone-elses", "team-a"),
		ObjectNew: databricksAccountServingWith("someone-elses"),
	}) {
		t.Error("another operator's account changing what it serves woke this operator's records")
	}
}

// TestAnotherAccountWakesNothing covers the operator being told which account to
// act in. Any other one is somebody else's object in the same cluster, and its
// condition says nothing about whether this operator can act.
func TestAnotherAccountWakesNothing(t *testing.T) {
	t.Parallel()
	selected := types.NamespacedName{Namespace: operatorNamespace, Name: databricksAccountName}
	p := databricksAccountChanged(selected)

	other := databricksAccountWith("someone-elses", metav1.ConditionTrue)
	if p.Create(event.CreateEvent{Object: other}) {
		t.Error("an account this operator was not told to use woke everything")
	}
	if p.Update(event.UpdateEvent{
		ObjectOld: databricksAccountWith("someone-elses", metav1.ConditionFalse),
		ObjectNew: other,
	}) {
		t.Error("an account this operator was not told to use woke everything on its transition")
	}

	// Deletion is the exception, and only for the selected one: the clients it
	// installed are about to be withdrawn, and every identity has to hear that.
	if !p.Delete(event.DeleteEvent{Object: databricksAccountWith(databricksAccountName, metav1.ConditionTrue)}) {
		t.Error("the selected account being deleted woke nothing")
	}
	if p.Delete(event.DeleteEvent{Object: other}) {
		t.Error("another account being deleted woke everything")
	}
}

// TestEveryRecordIsWokenOnce covers the listing that turns one account event
// into the requests it stands for.
//
// It is the records that are woken, not the copies in tenants' namespaces.
// Making a copy reaches nothing outside the cluster, so an account becoming
// usable changes nothing about one; what was waiting on the account is the
// controller that acts in it.
func TestEveryRecordIsWokenOnce(t *testing.T) {
	t.Parallel()
	list := &dbxv1alpha1.IssuedDatabricksServicePrincipalList{
		Items: []dbxv1alpha1.IssuedDatabricksServicePrincipal{
			{ObjectMeta: metav1.ObjectMeta{Namespace: "operators", Name: "team-a.etl-aaaa"}},
			{ObjectMeta: metav1.ObjectMeta{Namespace: "operators", Name: "team-b.loader-bbbb"}},
		},
	}
	requests := issuedRequests(list)
	if len(requests) != 2 {
		t.Fatalf("made %d requests for two records: %+v", len(requests), requests)
	}
	seen := map[string]bool{}
	for _, request := range requests {
		seen[request.String()] = true
	}
	for _, want := range []string{"operators/team-a.etl-aaaa", "operators/team-b.loader-bbbb"} {
		if !seen[want] {
			t.Errorf("requests are %+v, want one for %s", requests, want)
		}
	}
}

var _ client.Object = (*dbxv1alpha1.IssuedDatabricksServicePrincipal)(nil)
