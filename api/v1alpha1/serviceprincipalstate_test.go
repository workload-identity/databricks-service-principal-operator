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

package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// moment is a value for the two timestamps. Nothing compares them, so which
// moment it is says nothing; that they are set is the whole of what is read.
func moment() *metav1.Time {
	at := metav1.Now()
	return &at
}

// TestEveryCombinationOfTheThreeFieldsNamesOneState is what makes the state
// something a caller can ask for rather than derive.
//
// Three fields spell eight combinations and four states, so five of the rows
// below are combinations no single comparison answers correctly. The order is
// most-decided first -- a 404 answered about the id outranks the id, and the id
// outranks the mark -- because each is a later fact about the same service
// principal than the one under it, and reading them in any other order makes a
// record that has been through the whole of a life report an earlier part of it.
func TestEveryCombinationOfTheThreeFieldsNamesOneState(t *testing.T) {
	t.Parallel()
	for _, row := range []struct {
		name   string
		status IssuedDatabricksServicePrincipalStatus
		want   ServicePrincipalState
	}{
		{"nothing written", IssuedDatabricksServicePrincipalStatus{}, ServicePrincipalUnsent},
		{"the mark alone", IssuedDatabricksServicePrincipalStatus{
			ServicePrincipalCreateSentAt: moment(),
		}, ServicePrincipalSent},
		{"the mark and the id", IssuedDatabricksServicePrincipalStatus{
			ServicePrincipalCreateSentAt: moment(), ServicePrincipalID: "7788",
		}, ServicePrincipalKnown},
		{"the mark, the id and a removal", IssuedDatabricksServicePrincipalStatus{
			ServicePrincipalCreateSentAt: moment(), ServicePrincipalID: "7788",
			ServicePrincipalRemovedAt: moment(),
		}, ServicePrincipalGone},

		// A record written before the mark existed: it has an id and no mark,
		// and nothing about it is in doubt. Resolving the mark first would call
		// it Unsent and create a second service principal for it.
		{"an id and no mark", IssuedDatabricksServicePrincipalStatus{
			ServicePrincipalID: "7788",
		}, ServicePrincipalKnown},
		{"a removal and no mark", IssuedDatabricksServicePrincipalStatus{
			ServicePrincipalID: "7788", ServicePrincipalRemovedAt: moment(),
		}, ServicePrincipalGone},

		// Neither of these is reachable: a removal is only ever written about an
		// id this record holds. They are here because the resolution has to be
		// total -- a state machine with a combination it does not answer is one
		// whose callers each invent an answer, which is the defect it replaces.
		{"a removal and nothing else", IssuedDatabricksServicePrincipalStatus{
			ServicePrincipalRemovedAt: moment(),
		}, ServicePrincipalGone},
		{"a removal and the mark", IssuedDatabricksServicePrincipalStatus{
			ServicePrincipalCreateSentAt: moment(), ServicePrincipalRemovedAt: moment(),
		}, ServicePrincipalGone},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			if got := row.status.ServicePrincipalState(); got != row.want {
				t.Errorf("the state is %s, want %s", got, row.want)
			}
		})
	}
}

// TestOnlyAKnownIdentityIsUsable holds the one question the webhook and the
// equipment check ask.
//
// A service principal that is gone keeps its id and its client id, because both
// are what somebody auditing the account matches on. So a test of emptiness
// answers this wrongly in exactly the case it exists for, and the identity has
// to be asked whether it may be used rather than whether a field is filled in.
func TestOnlyAKnownIdentityIsUsable(t *testing.T) {
	t.Parallel()
	for _, row := range []struct {
		name  string
		entry ProjectedIdentity
		want  bool
	}{
		{"nothing was made for it", ProjectedIdentity{}, false},
		{"a service principal it can be exchanged for", ProjectedIdentity{
			ServicePrincipalID: "7788", ClientID: "app-uuid",
		}, true},
		{"one that was deleted in Databricks", ProjectedIdentity{
			ServicePrincipalID: "7788", ClientID: "app-uuid",
			ServicePrincipalRemovedAt: moment(),
		}, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			if got := row.entry.Usable(); got != row.want {
				t.Errorf("Usable is %v, want %v", got, row.want)
			}
		})
	}
}
