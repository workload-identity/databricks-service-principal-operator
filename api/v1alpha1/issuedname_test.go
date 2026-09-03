/*
Copyright 2026.

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
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

// TestTwoIdentitiesOfOneServiceAccountGetTwoRecords is the failure this name
// exists to prevent.
//
// Before the identity's name was in it, both of a ServiceAccount's identities
// derived the same record name. The second create would answer AlreadyExists,
// be read as "another pass got there first", and the workload would hold one
// identity while its annotation asked for two -- with nothing anywhere saying
// so.
func TestTwoIdentitiesOfOneServiceAccountGetTwoRecords(t *testing.T) {
	t.Parallel()
	const uid = types.UID("11111111-1111-1111-1111-111111111111")

	reader := IssuedNameFor("team-a", "etl", "reader", uid)
	writer := IssuedNameFor("team-a", "etl", "writer", uid)
	only := IssuedNameFor("team-a", "etl", "", uid)

	for _, pair := range [][2]string{{reader, writer}, {reader, only}, {writer, only}} {
		if pair[0] == pair[1] {
			t.Errorf("%q names two identities; the second create is answered AlreadyExists and "+
				"read as success, so one of them is never made", pair[0])
		}
	}
}

// TestTheIdentityIsInTheSuffixAndNotOnlyTheReadableHalf covers the pair the
// readable half cannot keep apart.
//
// The readable half is <namespace>.<serviceaccount>.<identity> truncated to fit,
// and a ServiceAccount name may be a whole DNS subdomain. Long enough, the cut
// lands before the identity is ever reached, and both identities read the same.
// The suffix is then the only thing left, so the suffix is what has to carry the
// difference -- showing the identity in the readable half is for people and
// proves nothing.
func TestTheIdentityIsInTheSuffixAndNotOnlyTheReadableHalf(t *testing.T) {
	t.Parallel()
	const uid = types.UID("22222222-2222-2222-2222-222222222222")

	// Both legal: a namespace is a DNS label and a ServiceAccount name is a DNS
	// subdomain, so the two together already exceed what the record's name has
	// room for.
	namespace, name := strings.Repeat("n", 63), strings.Repeat("s", 253)

	reader := IssuedNameFor(namespace, name, "reader", uid)
	writer := IssuedNameFor(namespace, name, "writer", uid)

	if before, _ := cutSuffix(reader); before != mustCut(t, writer) {
		t.Fatalf("the readable halves differ (%q and %q), so this pair does not reach the case "+
			"it is about; lengthen the names", before, mustCut(t, writer))
	}
	if reader == writer {
		t.Errorf("both identities are recorded as %q. The readable half cannot tell them apart "+
			"at this length, so nothing does: the second create is answered AlreadyExists, read "+
			"as success, and one identity is never made", reader)
	}
}

func cutSuffix(name string) (string, string) {
	i := strings.LastIndex(name, "-")
	if i < 0 {
		return name, ""
	}
	return name[:i], name[i+1:]
}

func mustCut(t *testing.T, name string) string {
	t.Helper()
	before, suffix := cutSuffix(name)
	if suffix == "" {
		t.Fatalf("%q carries no suffix", name)
	}
	return before
}

// TestOneWorkloadsIdentitiesSortTogether covers where the identity's name sits.
//
// After the ServiceAccount, not before it. Before it, `kubectl get isdbxsp`
// would gather every "reader" in the cluster and scatter each workload's own
// identities -- and "what does this workload have" is the question people ask
// of this list, while "who has a reader" is not.
func TestOneWorkloadsIdentitiesSortTogether(t *testing.T) {
	t.Parallel()
	names := []string{
		IssuedNameFor("team-b", "loader", "", types.UID("d")),
		IssuedNameFor("team-a", "etl", "writer", types.UID("a")),
		IssuedNameFor("team-a", "etl", "", types.UID("a")),
		IssuedNameFor("team-a", "etl", "reader", types.UID("a")),
	}
	slices.Sort(names)

	for i, name := range names[:3] {
		if !strings.HasPrefix(name, "team-a.etl") {
			t.Fatalf("sorted[%d] is %q; one workload's identities are not together: %v",
				i, name, names)
		}
	}

	// And each says which identity it is. The suffix is what keeps them apart,
	// but a person reading this list has to be able to say which row is the
	// reader without hashing anything -- that is the whole of what the readable
	// half is for.
	for i, want := range []string{"team-a.etl-", "team-a.etl.reader-", "team-a.etl.writer-"} {
		if !strings.HasPrefix(names[i], want) {
			t.Errorf("sorted[%d] is %q, want it to begin %q: sorted together and unreadable is "+
				"three rows nobody can tell apart", i, names[i], want)
		}
	}
}

// TestTheNameIsAlwaysUsable covers what the API server will accept, including
// the case the readable half has to be cut back for.
func TestTheNameIsAlwaysUsable(t *testing.T) {
	t.Parallel()
	// Every one of the three at its own limit: a namespace is a DNS label, a
	// ServiceAccount name is a DNS subdomain, and an identity's name is what an
	// annotation key holds after "service-principal.". Together they run past what
	// a Kubernetes object name takes, which is the case the readable half is cut
	// back for.
	for _, name := range []string{
		IssuedNameFor("team-a", "etl", "", types.UID("a")),
		IssuedNameFor("team-a", "etl", "reader", types.UID("a")),
		IssuedNameFor(strings.Repeat("n", 63), strings.Repeat("s", 253),
			strings.Repeat("i", identityNameLimit), types.UID("a")),
	} {
		if problems := validation.IsDNS1123Subdomain(name); len(problems) != 0 {
			t.Errorf("%q is not a usable name: %v -- the create is refused inside a reconcile, "+
				"where the message reaches nobody who asked", name, problems)
		}
	}
}

// TestTheSameIdentityAsksForTheSameName covers why the name is derived rather
// than generated: a pass that creates this record and then fails has to ask for
// the same name next time and be told it already exists.
func TestTheSameIdentityAsksForTheSameName(t *testing.T) {
	t.Parallel()
	const uid = types.UID("33333333-3333-3333-3333-333333333333")
	for _, identity := range []string{"", "reader", "writer"} {
		first := IssuedNameFor("team-a", "etl", identity, uid)
		if again := IssuedNameFor("team-a", "etl", identity, uid); again != first {
			t.Errorf("%q asks for %q and then %q; a retry would make a second record for one "+
				"service principal, and the first would be left with nothing naming it",
				identity, first, again)
		}
	}
}

// TestTheReadableHalfIsNeverCutOnItsSeparator covers where the truncation lands.
//
// The readable half is cut wherever the room runs out, and one that ends on its
// own "." meets the suffix as ".-" -- a label beginning with "-", which the API
// server refuses. Measured on these inputs, both of which are legal: a namespace
// is a DNS label and a ServiceAccount name is a DNS subdomain.
//
// What it costs is quiet. The create is refused inside a reconcile, where the
// message reaches nobody who asked, so the ServiceAccount is left annotated with
// no identity and nothing on it saying why.
func TestTheReadableHalfIsNeverCutOnItsSeparator(t *testing.T) {
	t.Parallel()
	const uid = types.UID("44444444-4444-4444-4444-444444444444")
	name := IssuedNameFor(strings.Repeat("n", 10), strings.Repeat("s", 230), "reader", uid)

	if problems := validation.IsDNS1123Subdomain(name); len(problems) != 0 {
		t.Errorf("this ServiceAccount's record would be called %q, which the API server refuses "+
			"(%v): its identity is never issued and nothing says so", name, problems)
	}
}
