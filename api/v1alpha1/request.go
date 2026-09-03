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
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Request is one identity a ServiceAccount asked for: which operator to ask, and
// what to call the identity.
//
// One annotation key asks for one identity, and that is what makes withdrawing
// an identity and mistyping one different inputs. Written as entries of one
// comma-separated value they were the same input -- an entry that is not there
// any more -- so the operator had to guess which had happened, and guessing
// "withdrawn" destroys a service principal and every grant anybody made on it.
// Verified by running it: one missing "/" was enough. As map entries, the two
// are a key that is gone and a key that is there.
//
// An operator is named by the DatabricksAccount it acts on -- its namespace and
// its name -- because that is unique in a way a chosen name is not. Two operators
// cannot share it: the API server will not allow two objects of one kind at one
// address. A nickname can be shared, and a shared nickname is how two operators
// come to believe they hold one identity.
type Request struct {
	// Operator is the DatabricksAccount naming the operator this asks, which is
	// what the key's value holds.
	Operator types.NamespacedName

	// Name is what the asker calls this identity, which is what the key's own
	// name holds after "service-principal.".
	//
	// Empty is the bare key, and is not a missing value: it names the one
	// identity this operator issues to this ServiceAccount, which is the whole of
	// the common case.
	//
	// It is chosen by the asker rather than derived, because nothing else knows
	// what the two identities are for. The name a person wrote is the name their
	// code reads back.
	Name string
}

// String is the profile name: what a workload passes to the Databricks SDK to
// use this identity, and the key of the entry in the projection.
//
// A named identity is its name, verbatim, so the string a person wrote in an
// annotation key is the string their code names -- "reader" in the annotation is
// [reader] in the config file, with no rule to learn in between. The unnamed
// identity has no such string of its own, so it is named by what its key held:
// the operator's reference.
//
// Unique per ServiceAccount without anything having to check. Names are
// annotation map keys, so no two are equal; and a name may hold no "/" while
// every operator reference holds exactly one, so a named identity cannot collide
// with an unnamed one either.
func (r Request) String() string {
	if r.Name == "" {
		return r.Operator.String()
	}
	return r.Name
}

// Refusal is one service-principal annotation key that could not be acted on.
//
// Out of deepcopy generation, which refuses an interface field: this is what
// reading an annotation produced and never a field of an object, so nothing
// copies it.
//
// +kubebuilder:object:generate=false
type Refusal struct {
	// Key is the annotation key as it was written. It is what says which identity
	// is being held rather than withdrawn, so it is kept as a value and not only
	// spelled into the message.
	Key string

	// Err is why the key would not read, naming the key and the shape to write
	// instead.
	//
	// Nothing surfaces it, and there is nowhere for it to go. A refused key gets
	// no entry in the projection, so there is no condition to carry it, and the
	// object as a whole has none to offer: a value that does not parse names
	// nobody, so every operator serving the cluster refuses that key, while every
	// entry of the object is owned by one.
	//
	// An Event on the ServiceAccount is not that place either. The spam filter
	// keys on source and involvedObject and holds neither reason nor message
	// (client-go v0.36.0, tools/record/events_cache.go: getSpamKey, burst 25,
	// refill 1 per 300s), so every Event on one ServiceAccount shares a budget: a
	// refusal that stands is re-emitted unchanged on every reconcile until the
	// budget is gone, and the next refusal -- a different key, a different
	// message -- is dropped. Measured on a cluster: a second refused key produced
	// no Event at all. Silence then reads as nothing being wrong to exactly the
	// person who knows refusals are reported, which is worse than never reporting
	// them.
	Err error
}

func (r Refusal) Error() string { return r.Err.Error() }

// Requested is what one ServiceAccount's annotations asked of one operator: the
// keys that were understood, and the keys that were not.
//
// One value holding both, rather than two returned side by side, because the
// refusals decide something. A key that cannot be read is a key that is still
// there, and the identity it names has to be left exactly as it stands. Returned
// as a second value they could be dropped with "_" -- which is what RequestsFor
// used to do, and dropping them is precisely how a mistyped key became a key
// that had disappeared.
//
// Out of deepcopy generation along with the Refusals it holds.
//
// +kubebuilder:object:generate=false
type Requested struct {
	// Understood is one entry per key this operator was asked through, ordered by
	// identity name with the unnamed one first.
	//
	// Ordered rather than as written: annotations are a map and a map has no
	// order, so an order has to be chosen or the identity a pod is given by
	// default would change from pass to pass. The unnamed one first because it is
	// the only identity a ServiceAccount asking for exactly one has.
	Understood []Request

	// Refused is one entry per service-principal key that could not be read,
	// ordered by key.
	//
	// Not narrowed to this operator, and it must not be: a value that does not
	// parse names no operator, so no operator can tell whether the key was
	// addressed to it. Narrowed by guessing, the operator the key was meant for
	// would not see it among its own -- and a key it does not see is a key that is
	// gone, which is a withdrawal, which ends the service principal and every
	// grant made on it. So every operator serving the cluster holds the identity
	// that key names, and the one it was meant for is among them.
	Refused []Refusal
}

// Unreadable reports whether one identity's key is there and could not be read.
//
// Nothing is created and nothing is destroyed for such an identity. The key is
// there, so its absence cannot be read as a withdrawal; its value cannot be
// read, so it asks for nothing. Whatever the identity already is, it stays --
// which is what makes refusing a key safe, and refusing safely is the whole of
// this change.
func (q Requested) Unreadable(identity string) bool {
	key := ServicePrincipalAnnotationFor(identity)
	for _, refusal := range q.Refused {
		if refusal.Key == key {
			return true
		}
	}
	return false
}

// Withdrawn reports whether an identity this operator holds is no longer asked
// of it. It is the only thing that destroys a service principal.
//
// It is one method rather than a negation each caller writes, because the two
// ways an identity leaves Understood are not the same: gone from the annotations
// is a withdrawal, and present but unreadable is a typo. Getting that negation
// wrong deletes an identity, so it is written once.
//
// An identity whose key now names another operator is withdrawn from this one.
// The key is readable and it says somebody else, which is a person moving an
// identity and not a mistake to hold back on.
func (q Requested) Withdrawn(identity string) bool {
	for _, request := range q.Understood {
		if request.Name == identity {
			return false
		}
	}
	return !q.Unreadable(identity)
}

// RequestsFor reads every service-principal annotation on a ServiceAccount as
// one operator.
//
// The whole annotations map, not one value: the bare key asks for this
// operator's unnamed identity, "<the key>.<name>" asks for one called <name>,
// and the value of each is the operator being asked.
//
// It hands back what it refused along with what it understood, and there is no
// way to ask for only the first. The refusals say which identities must be left
// alone this pass, so an operator that dropped them would read a typo as a
// withdrawal and destroy a service principal over it.
func RequestsFor(annotations map[string]string, operator types.NamespacedName) Requested {
	var requested Requested
	for key, value := range annotations {
		name, named := strings.CutPrefix(key, servicePrincipalKeyPrefix)
		switch {
		case named:
			if err := validIdentityName(key, name); err != nil {
				requested.Refused = append(requested.Refused, Refusal{Key: key, Err: err})
				continue
			}
		case key == ServicePrincipalAnnotation:
			// CutPrefix hands the whole key back when the prefix is not there.
			// Left as it came, the unnamed identity would be called after its own
			// annotation, and that name is its profile name -- so a workload would
			// have to write "databricks.workload-identity.io/service-principal" in
			// its configuration to use the identity it asked for by name at all.
			name = ""
		default:
			continue
		}
		asked, err := parseOperator(key, value)
		if err != nil {
			requested.Refused = append(requested.Refused, Refusal{Key: key, Err: err})
			continue
		}
		if asked != operator {
			continue
		}
		requested.Understood = append(requested.Understood, Request{Operator: asked, Name: name})
	}

	slices.SortFunc(requested.Understood, func(a, b Request) int {
		return strings.Compare(a.Name, b.Name)
	})
	slices.SortFunc(requested.Refused, func(a, b Refusal) int {
		return strings.Compare(a.Key, b.Key)
	})
	return requested
}

// ServicePrincipalAnnotationFor is the annotation key that asks for one
// identity, and the bare key for the unnamed one.
//
// Composed here rather than at each caller, so that "the key for this identity"
// and "the identity for this key" cannot drift apart -- Unreadable compares one
// against the other.
func ServicePrincipalAnnotationFor(identity string) string {
	if identity == "" {
		return ServicePrincipalAnnotation
	}
	return servicePrincipalKeyPrefix + identity
}

// servicePrincipalKeyPrefix is what a key naming an identity begins with.
const servicePrincipalKeyPrefix = ServicePrincipalAnnotation + "."

// identityNameLimit is what one identity's name may run to.
//
// Measured, not chosen: the name part of an annotation key is capped at 63 bytes
// by the API server, and "service-principal." is 18 of them. 45 is accepted and
// 46 is refused with "name part must be no more than 63 bytes".
const identityNameLimit = 45

// validIdentityName refuses a key whose name cannot be an identity's.
//
// The key being a map entry validates nothing that matters here. Measured: the
// API server accepts "service-principal.Reader", "service-principal.read_er" and
// "service-principal.read.er" without complaint, and uppercase and "_" would then
// be carried into the record's own name, which is a Kubernetes object name and
// takes neither.
//
// The "." is refused for a second reason. The record's readable half is
// <namespace>.<serviceaccount>.<identity>, and a dot inside the identity leaves
// nothing to say where the ServiceAccount's name ends and the identity's begins,
// in either direction.
//
// Refused at the parse rather than where a record is created: a key refused here
// is a key that could not be read, and the identity it names is held exactly as
// it stands. Understood instead, it would be a request to create a record under
// a name no Kubernetes object can carry, inside a reconcile that nobody who
// wrote the annotation is reading.
func validIdentityName(key, name string) error {
	switch {
	case name == "":
		return fmt.Errorf(
			"%s names no identity after the %q: leave the %q off to ask for this operator's only "+
				"identity, or write a name after it", key, ".", ".")
	case len(name) > identityNameLimit:
		return fmt.Errorf(
			"%s names an identity of %d characters, and %d is the most an annotation key holds "+
				"after %q", key, len(name), identityNameLimit, "service-principal.")
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case r == '-' && i > 0 && i < len(name)-1:
		default:
			return fmt.Errorf(
				"%s names an identity holding %q. A name may hold lowercase letters, digits and "+
					"%q between them: it becomes part of the name of a Kubernetes object",
				key, string(r), "-")
		}
	}
	return nil
}

// parseOperator reads one key's value: the operator being asked, written as
// <its namespace>/<its DatabricksAccount>.
//
// Both halves are checked against what Kubernetes accepts for them, because a
// value can cut cleanly on the "/" and still name nothing that could exist.
// "reader@ops-a/databricks-account" gives a namespace of "reader@ops-a", and
// unchecked it is read as an operator by that name: no operator can be called
// that, so the key is understood as somebody else's and falls out of every
// operator's request. Falling out is exactly what a key somebody deleted does,
// and that ends the identity -- so a value nothing could claim has to be refused
// rather than left to name a stranger.
func parseOperator(key, value string) (types.NamespacedName, error) {
	reference := strings.TrimSpace(value)
	namespace, name, cut := strings.Cut(reference, "/")
	if !cut || len(validation.IsDNS1123Label(namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(name)) != 0 {
		return types.NamespacedName{}, fmt.Errorf(
			"%s is %q, which is not an operator: write <its namespace>/<its DatabricksAccount>, "+
				"as in %q", key, value, "ops-a/databricks-account")
	}
	return types.NamespacedName{Namespace: namespace, Name: name}, nil
}
