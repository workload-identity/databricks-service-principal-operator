package v1alpha1

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

const opsA = "ops-a/databricks-account"

func operator(namespace, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: name}
}

// TestAMistypedValueLeavesTheIdentityItNamesAlone is the bug this whole shape
// exists to remove.
//
// A value that cannot be read is a key that is still there. Reading it as a key
// nobody wrote deletes the service principal it names and every grant anybody
// made on it -- verified against a real account, where losing one "/" was
// enough.
func TestAMistypedValueLeavesTheIdentityItNamesAlone(t *testing.T) {
	t.Parallel()
	asked := RequestsFor(map[string]string{
		ServicePrincipalAnnotationFor("reader"): "ops-a", // the "/" lost
	}, operator("ops-a", "databricks-account"))

	if len(asked.Understood) != 0 {
		t.Errorf("%q was understood as %+v; nothing in it names an operator",
			"ops-a", asked.Understood)
	}
	if len(asked.Refused) != 1 {
		t.Fatalf("refused %v, want the one key that could not be read", asked.Refused)
	}
	if key := asked.Refused[0].Key; key != ServicePrincipalAnnotationFor("reader") {
		t.Errorf("the refusal is filed under %q; nothing can then tell which identity to hold, "+
			"and the one it names is destroyed on this pass", key)
	}
	if !asked.Unreadable("reader") {
		t.Errorf("%q reads as an identity nobody wrote a key for", "reader")
	}
	if asked.NoLongerAsked("reader") {
		t.Errorf("a lost %q reads as nobody asking any more, so the service principal and every "+
			"grant made on it are deleted over a typo", "/")
	}
}

// TestAKeyThatIsGoneIsNoLongerAsked covers the other half of the same split.
//
// Refusing safely is only worth having if letting go still works: an identity
// whose key a person deleted has to be destroyed, or a workload keeps access its
// owner revoked.
func TestAKeyThatIsGoneIsNoLongerAsked(t *testing.T) {
	t.Parallel()
	asked := RequestsFor(map[string]string{
		ServicePrincipalAnnotationFor("writer"): opsA,
	}, operator("ops-a", "databricks-account"))

	if !asked.NoLongerAsked("reader") {
		t.Errorf("%q is asked for by nothing and still reads as asked for; a workload keeps an "+
			"identity its owner deleted the key for", "reader")
	}
	if !asked.NoLongerAsked("") {
		t.Errorf("the unnamed identity is asked for by nothing and still reads as asked for; " +
			"deleting the bare key would revoke nothing")
	}
	if asked.NoLongerAsked("writer") {
		t.Errorf("%q is asked for by a key that reads cleanly and reads as let go of anyway", "writer")
	}
}

// TestOneUnreadableKeyDoesNotTakeTheOthersWithIt covers what per-key refusal is
// for.
//
// A ServiceAccount asking for three identities and mistyping one has asked for
// two. Refusing the whole annotation would take two working identities away over
// a mistake in a third.
func TestOneUnreadableKeyDoesNotTakeTheOthersWithIt(t *testing.T) {
	t.Parallel()
	asked := RequestsFor(map[string]string{
		ServicePrincipalAnnotation:              opsA,
		ServicePrincipalAnnotationFor("reader"): "not-an-operator",
		ServicePrincipalAnnotationFor("writer"): opsA,
	}, operator("ops-a", "databricks-account"))

	if len(asked.Understood) != 2 {
		t.Fatalf("understood %+v; the two keys that read cleanly are unaffected by the third",
			asked.Understood)
	}
	if asked.Understood[0].Name != "" || asked.Understood[1].Name != "writer" {
		t.Errorf("understood %+v, want the unnamed identity and %q", asked.Understood, "writer")
	}
	if len(asked.Refused) != 1 {
		t.Errorf("refused %v, want only the key that could not be read", asked.Refused)
	}
	if asked.NoLongerAsked("") || asked.NoLongerAsked("writer") {
		t.Errorf("an identity whose own key reads cleanly is let go of over a mistake in another " +
			"key, which destroys it")
	}
}

// TestTheProfileNameIsTheStringThePersonWrote covers the promise the whole
// naming scheme is built to keep.
//
// The profile a workload passes to the Databricks SDK is the string its owner
// wrote in the annotation key. Derived any other way, they would have to learn
// the derivation to name their own profile, which is a rule this project decided
// not to invent.
func TestTheProfileNameIsTheStringThePersonWrote(t *testing.T) {
	t.Parallel()
	asked := RequestsFor(map[string]string{
		ServicePrincipalAnnotation:              opsA,
		ServicePrincipalAnnotationFor("reader"): opsA,
	}, operator("ops-a", "databricks-account"))

	if len(asked.Understood) != 2 {
		t.Fatalf("understood %+v, want both", asked.Understood)
	}
	if profile := asked.Understood[1].String(); profile != "reader" {
		t.Errorf("the key named %q and the workload has to write %q in its own configuration; "+
			"nothing may change the name on the way", "reader", profile)
	}
	// The unnamed identity has no name of its own, so it is named by what its key
	// held. It is a legal INI section name and cannot collide with any identity
	// name, because a name may hold no "/".
	if profile := asked.Understood[0].String(); profile != opsA {
		t.Errorf("the unnamed identity's profile is %q; it has no name of its own, so the only "+
			"string its owner wrote for it is %q", profile, opsA)
	}
}

// TestWhatIsNotAnIdentityName covers each way a key's own name is refused.
//
// The key being a map entry validates none of this. Measured: the API server
// accepts "service-principal.Reader", "service-principal.read_er" and
// "service-principal.read.er" without complaint, and all three would be carried
// into the name of a Kubernetes object that takes none of them.
func TestWhatIsNotAnIdentityName(t *testing.T) {
	t.Parallel()
	for _, key := range []string{
		ServicePrincipalAnnotationFor("Reader"),
		ServicePrincipalAnnotationFor("read_er"),
		ServicePrincipalAnnotationFor("read.er"),
		ServicePrincipalAnnotationFor("-reader"),
		ServicePrincipalAnnotationFor("reader-"),
		ServicePrincipalAnnotationFor(strings.Repeat("r", identityNameLimit+1)),
	} {
		asked := RequestsFor(map[string]string{key: opsA}, operator("ops-a", "databricks-account"))

		if len(asked.Understood) != 0 {
			t.Errorf("%q was understood as %+v, and its name goes into a Kubernetes object name "+
				"that the API server then refuses, inside a reconcile where nobody sees it",
				key, asked.Understood)
		}
		if len(asked.Refused) != 1 {
			t.Fatalf("%q produced %v, want one refusal saying what to write", key, asked.Refused)
		}
		if !strings.Contains(asked.Refused[0].Error(), key) {
			t.Errorf("%q is refused with %q, which does not name the key: a ServiceAccount now "+
				"carries a key per identity, and a refusal that does not say which one leaves "+
				"every line it wrote equally suspect", key, asked.Refused[0])
		}
	}
}

// TestAKeyEndingInADotIsNotTheUnnamedIdentity covers the one name whose refusal
// must not be mistaken for a request.
//
// Its suffix is empty, which is what the unnamed identity's name is. Read as
// that identity it would be a second bare key, and whichever of the two the map
// happened to yield last would win -- silently, and differently on each pass.
func TestAKeyEndingInADotIsNotTheUnnamedIdentity(t *testing.T) {
	t.Parallel()
	key := ServicePrincipalAnnotation + "."
	asked := RequestsFor(map[string]string{key: opsA}, operator("ops-a", "databricks-account"))

	if len(asked.Understood) != 0 {
		t.Errorf("%q was understood as %+v", key, asked.Understood)
	}
	if len(asked.Refused) != 1 || !strings.Contains(asked.Refused[0].Error(), key) {
		t.Fatalf("%q produced %v, want one refusal naming the key", key, asked.Refused)
	}
	if asked.Unreadable("") {
		t.Errorf("%q is held against the unnamed identity, whose key is %q and was never "+
			"written: an identity nobody asked about stops being maintained", key,
			ServicePrincipalAnnotation)
	}
}

// TestTheLongestNameAnAnnotationKeyHoldsIsAccepted pins the limit to what was
// measured rather than to a round number.
//
// The name part of an annotation key caps at 63 bytes and "service-principal."
// is 18 of them, so 45 is what is left. A shorter limit would refuse names the
// cluster accepts; a longer one would be refused by the API server on the write,
// where the message is about bytes and not about identities.
func TestTheLongestNameAnAnnotationKeyHoldsIsAccepted(t *testing.T) {
	t.Parallel()
	name := strings.Repeat("r", identityNameLimit)
	asked := RequestsFor(map[string]string{
		ServicePrincipalAnnotationFor(name): opsA,
	}, operator("ops-a", "databricks-account"))

	if len(asked.Understood) != 1 || asked.Understood[0].Name != name {
		t.Errorf("a %d-character name is refused (%v) though the API server carries the key it "+
			"makes", identityNameLimit, asked.Refused)
	}
	_, part, _ := strings.Cut(ServicePrincipalAnnotationFor(name), "/")
	if len(part) != 63 {
		t.Errorf("the longest accepted name makes a key whose name part is %d bytes, not the 63 "+
			"the API server caps it at, so %d is either turning away names the cluster carries "+
			"or letting through keys it refuses", len(part), identityNameLimit)
	}
}

// TestWhatIsNotAnOperator covers each way a key's value is refused.
func TestWhatIsNotAnOperator(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		"databricks-account",   // no namespace
		"ops-a/two/deep",       // more than one separator
		"/databricks-account",  // empty namespace
		"ops-a/",               // empty name
		"Ops-A/DatabricksAcct", // not names Kubernetes accepts
		"",                     // an annotation left blank
	} {
		asked := RequestsFor(map[string]string{
			ServicePrincipalAnnotation: value,
		}, operator("ops-a", "databricks-account"))

		if len(asked.Understood) != 0 {
			t.Errorf("%q was understood as %+v", value, asked.Understood)
		}
		if len(asked.Refused) != 1 {
			t.Fatalf("%q produced %v, want one refusal", value, asked.Refused)
		}
		if !strings.Contains(asked.Refused[0].Error(), ServicePrincipalAnnotation) ||
			!strings.Contains(asked.Refused[0].Error(), value) {
			t.Errorf("%q is refused with %q, which has to quote both the key and what was "+
				"written in it", value, asked.Refused[0])
		}
	}
}

// TestAValueThatCutsCleanlyAndNamesNobodyIsRefused covers the values that reach
// the end of parsing without failing it.
//
// Both of these cut on a "/" into two halves, so nothing about the shape says
// they are wrong. "reader@ops-a/databricks-account" gives a namespace of
// "reader@ops-a" and a comma gives a name holding one, and either read as an
// operator is one that cannot exist -- so the key is understood as somebody
// else's and falls out of every operator's request, which is what a key somebody
// deleted does and ends the identity. The value has to be refused rather than
// left to name a stranger, and both halves checked against what Kubernetes
// accepts is what does it.
func TestAValueThatCutsCleanlyAndNamesNobodyIsRefused(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		"reader@" + opsA,
		opsA + ",ops-b/databricks-account",
	} {
		asked := RequestsFor(map[string]string{
			ServicePrincipalAnnotation: value,
		}, operator("ops-a", "databricks-account"))

		if len(asked.Understood) != 0 {
			t.Errorf("%q was understood as %+v", value, asked.Understood)
		}
		if len(asked.Refused) != 1 {
			t.Fatalf("%q produced %v; unrefused it names a stranger, and an identity every "+
				"operator reads as somebody else's is one every operator reads as let go of",
				value, asked.Refused)
		}
		if !strings.Contains(asked.Refused[0].Error(), "/") {
			t.Errorf("%q is refused with %q, which does not show the shape to write instead",
				value, asked.Refused[0])
		}
	}
}

// TestOneOperatorAnswersSeveralNames is the whole of what an identity's name is
// for.
//
// A workload reading with one service principal and writing with another is
// ordinary practice in Databricks, and both live in one account, so both are
// asked of one operator. Without the name, the number of identities a
// ServiceAccount could hold would be the number of operators serving its
// cluster -- a limit nothing about Databricks asks for, and one that answered
// "two identities in one account" with "run a second operator".
func TestOneOperatorAnswersSeveralNames(t *testing.T) {
	t.Parallel()
	asked := RequestsFor(map[string]string{
		ServicePrincipalAnnotationFor("reader"): opsA,
		ServicePrincipalAnnotationFor("writer"): opsA,
	}, operator("ops-a", "databricks-account"))

	if len(asked.Understood) != 2 {
		t.Fatalf("understood %d identities from one operator, want two: %+v",
			len(asked.Understood), asked.Understood)
	}
	if asked.Understood[0].String() == asked.Understood[1].String() {
		t.Fatalf("both identities are keyed %q; one entry overwrites the other in the DatabricksServiceAccount "+
			"and the workload ends up holding one", asked.Understood[0])
	}
}

// TestTheUnnamedIdentityAndANamedOneAreTwo covers the pair easiest to read as
// one.
//
// They name one operator and one ServiceAccount and differ only in a name one of
// them does not have. Treating the empty name as "no identity in particular"
// rather than as an identity would collapse them, and a workload adding a second
// identity would silently lose its first.
func TestTheUnnamedIdentityAndANamedOneAreTwo(t *testing.T) {
	t.Parallel()
	asked := RequestsFor(map[string]string{
		ServicePrincipalAnnotation:              opsA,
		ServicePrincipalAnnotationFor("writer"): opsA,
	}, operator("ops-a", "databricks-account"))

	if len(asked.Understood) != 2 {
		t.Fatalf("understood %d, want two: %+v", len(asked.Understood), asked.Understood)
	}
	if asked.Understood[0].String() == asked.Understood[1].String() {
		t.Errorf("both are keyed %q, so one of the two identities has nowhere to be reported",
			asked.Understood[0])
	}
}

// TestAnotherOperatorsKeysAreNotThisOnesButItsRefusalsAre covers what one
// operator takes out of a ServiceAccount that asks several.
//
// Refusals are not narrowed, and must not be: a value that does not parse names
// no operator, so no operator can tell whether the key was addressed to it.
// Narrowed, the operator the key was meant for would not see it among its own,
// and a key it does not see is a key that is gone.
func TestAnotherOperatorsKeysAreNotThisOnesButItsRefusalsAre(t *testing.T) {
	t.Parallel()
	annotations := map[string]string{
		ServicePrincipalAnnotation:              opsA,
		ServicePrincipalAnnotationFor("writer"): "ops-b/databricks-account",
		ServicePrincipalAnnotationFor("reader"): "ops-b",
	}

	asked := RequestsFor(annotations, operator("ops-a", "databricks-account"))
	if len(asked.Understood) != 1 || asked.Understood[0].Name != "" {
		t.Fatalf("understood %+v, want only the key naming this operator", asked.Understood)
	}
	if len(asked.Refused) != 1 {
		t.Errorf("refused %v; a key whose value names no operator is held by whoever is reading, "+
			"or the one it was meant for reads it as let go of", asked.Refused)
	}

	if other := RequestsFor(annotations, operator("ops-c", "databricks-account")); len(other.Understood) != 0 {
		t.Errorf("an operator nobody named understood %+v as asked of it", other.Understood)
	}
}

// TestAnIdentityMovedToAnotherOperatorIsNoLongerAskedOfIt covers a readable
// key that now says somebody else.
//
// That is a person moving an identity, not a mistake to hold back on: the
// operator that used to answer for it must destroy what it made, or the
// ServiceAccount ends up holding two service principals under one name.
func TestAnIdentityMovedToAnotherOperatorIsNoLongerAskedOfIt(t *testing.T) {
	t.Parallel()
	asked := RequestsFor(map[string]string{
		ServicePrincipalAnnotationFor("reader"): "ops-b/databricks-account",
	}, operator("ops-a", "databricks-account"))

	if !asked.NoLongerAsked("reader") {
		t.Errorf("%q now names another operator and this one keeps maintaining it, so the "+
			"ServiceAccount holds two service principals under one profile name", "reader")
	}
	if asked.Unreadable("reader") {
		t.Errorf("%q reads cleanly and is being held as unreadable, so nothing ever destroys "+
			"what this operator made for it", "reader")
	}
}

// TestTheOrderIsTheSameOnEveryPass covers what replaces the order a person used
// to write.
//
// Annotations are a map, and Go randomises the order a map yields. Unordered,
// the first entry -- which is the identity a workload is given when it names no
// profile -- would change from pass to pass, and so would the pod spec the
// webhook writes.
func TestTheOrderIsTheSameOnEveryPass(t *testing.T) {
	t.Parallel()
	annotations := map[string]string{
		ServicePrincipalAnnotationFor("writer"): opsA,
		ServicePrincipalAnnotationFor("reader"): opsA,
		ServicePrincipalAnnotationFor("admin"):  opsA,
		ServicePrincipalAnnotation:              opsA,
	}

	want := []string{"", "admin", "reader", "writer"}
	for pass := range 16 {
		asked := RequestsFor(annotations, operator("ops-a", "databricks-account"))
		for i, name := range want {
			if asked.Understood[i].Name != name {
				t.Fatalf("pass %d read %+v; the identity a pod is given by default changes from "+
					"pass to pass, and so does the pod spec the webhook writes",
					pass, asked.Understood)
			}
		}
	}
}

// TestAnnotationsThatAreNotRequestsAreNotRead covers a ServiceAccount carrying
// everything else a cluster puts on one.
func TestAnnotationsThatAreNotRequestsAreNotRead(t *testing.T) {
	t.Parallel()
	asked := RequestsFor(map[string]string{
		"kubectl.kubernetes.io/last-applied-configuration": "{}",
		ServicePrincipalAnnotation + "-owner":              "platform",
		"databricks.workload-identity.io/mint":             Enabled,
	}, operator("ops-a", "databricks-account"))

	if len(asked.Understood) != 0 || len(asked.Refused) != 0 {
		t.Errorf("understood %+v and refused %v; a ServiceAccount that asked for nothing gets an "+
			"identity, or a warning about an annotation that is none of this operator's business",
			asked.Understood, asked.Refused)
	}
	if !asked.NoLongerAsked("") {
		t.Errorf("a ServiceAccount that never asked reads as still asking, but nothing here " +
			"holds an identity for it to keep")
	}
}
