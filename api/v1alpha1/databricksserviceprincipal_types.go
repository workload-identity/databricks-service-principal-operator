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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// ServicePrincipalAnnotation on a ServiceAccount asks for a Databricks
	// service principal for it, and is one key per identity: this one asks for
	// the unnamed identity, and "<this>.<name>" asks for one called <name>. The
	// value of each is the operator being asked, as
	// <its namespace>/<its DatabricksAccount>. See ServicePrincipalAnnotationFor
	// and RequestsFor.
	//
	// One key each rather than one comma-separated list, because a list makes
	// "this identity was withdrawn" and "this identity was misspelled" the same
	// input: an entry that is no longer in the string. The operator then has to
	// guess, and guessing withdrawal destroys a service principal along with
	// every grant anybody made on it -- verified by running it, where losing a
	// single "/" was enough. As map entries the two are a key that is gone and a
	// key that is there, so nothing is guessed and a refusal costs nothing.
	//
	// It names what it asks for. The cloud providers put the identifier of an
	// identity that already exists here -- iam.gke.io/gcp-service-account,
	// eks.amazonaws.com/role-arn, azure.workload.identity/client-id -- and this
	// operator has nothing like it to name, because the identity does not exist
	// until this asks for it. So what the value names is the operator to ask,
	// and the key carries the rest of the meaning: what appears in Databricks is
	// a service principal, what appears in the cluster is a
	// DatabricksServicePrincipal, and this is what asks for one.
	//
	// The request is made on the ServiceAccount rather than in an object of its
	// own, and that is the whole reason a name typed here cannot be wrong:
	// nothing can be annotated that does not exist. An object naming a
	// ServiceAccount could name one that never existed, or one deleted since --
	// and a service principal for a ServiceAccount that is not there is an
	// identity nobody owns, which a later ServiceAccount of the same name would
	// silently inherit along with anything granted to it in the meantime.
	//
	// It is also the shape the cloud providers converged on for the same job,
	// so it is the one a reader already knows.
	ServicePrincipalAnnotation = "databricks.workload-identity.io/service-principal"

	// There is deliberately no annotation for the workspace a workload reaches.
	//
	// A host is a destination and this operator issues identities. It cannot
	// verify one -- it knows the account it acts in, and a workload wants a
	// workspace -- so carrying a host would be repeating a string nobody checks,
	// in an object that is not where the workload's other configuration lives.
	//
	// Nothing is lost by leaving it out. Measured against the SDK: a profile with
	// no host resolves cleanly and takes DATABRICKS_HOST from the environment,
	// keeping auth_type, client_id and the token path from the profile. So a
	// workload names its own workspace in its own Deployment, under the SDK's own
	// name, and learns nothing of this operator's to do it.

	// MintLabel on a Namespace is whether new identities may be made in it. Only
	// somebody with cluster-wide access can write a Namespace, so it is the
	// cluster's answer to a team's request rather than the team's own.
	//
	// It is not a security boundary, and that decides what withdrawing it does.
	// The identity a ServiceAccount can ask for is
	// system:serviceaccount:<its own namespace>:<itself>, bounded by what the
	// asker already controls, and it is made holding nothing -- so this stops
	// nobody reaching anything they could not reach anyway. It decides which
	// teams are in.
	//
	// Withdrawing it therefore stops new identities and leaves existing ones
	// alone, still working and no longer managed, saying so. Revoking one is
	// removing the annotation, by whoever asked, which is the split these two
	// keys exist for.
	//
	// It is not what the webhook selects on. See InjectLabel.
	MintLabel = "databricks.workload-identity.io/mint"

	// InjectLabel on a Namespace is the webhook's namespaceSelector: whether
	// pods created here have their token injected.
	//
	// It names the action, which is what the labels doing this job elsewhere do
	// -- istio-injection, linkerd.io/inject, azure.workload.identity/use -- so
	// that removing it says what stops.
	//
	// Separate from MintLabel because the two end at different times.
	// Enrolment is an event -- open it, mint what is needed, close it -- and
	// closing it must not break the identities that were minted while it was
	// open. Injection is continuous: a namespace holding any identity needs its
	// new pods equipped for as long as that identity is used, which is long
	// after minting has been closed. One label for both means closing the
	// minting switch quietly stops equipping, and every workload there loses
	// access on its next rollout.
	//
	// It is an optimisation, not a boundary. A pod the webhook is asked about
	// whose ServiceAccount has no identity is admitted unchanged, so leaving
	// this label on a namespace that no longer uses Databricks costs one
	// admission call per pod and nothing else. Forgetting it is cheap; that is
	// the direction this should fail in.
	//
	// The operator's own namespace does not carry it, which is what stops
	// admitting the operator's own pod from depending on the operator running.
	InjectLabel = "databricks.workload-identity.io/inject"

	// Enabled is the value the two Namespace labels must carry. The labels
	// switch an action on -- minting, injecting -- and are written by somebody
	// with cluster-wide access.
	//
	// Anything else means the same as absent: "false", "no", an empty value, and
	// a typo are not permission to mint and not an instruction to inject.
	Enabled = "enabled"

	// ServicePrincipalFinalizer holds an IssuedDatabricksServicePrincipal until
	// the service principal it records has been deleted in Databricks.
	//
	// It is on the record and not on the projection, which is the difference
	// between an identity that is destroyed when its record says so and one
	// destroyed by whichever kind a namespace teardown happened to delete first.
	//
	// It is never dropped on a timeout. Removing it while the service principal
	// is still there would be the operator deciding that an identity outliving
	// the object recording it is acceptable, on the evidence that one call did
	// not go through. A person can remove it by hand, which is the same decision
	// made by somebody who can see what is left behind.
	ServicePrincipalFinalizer = "databricks.workload-identity.io/delete-service-principal"
)

// ProjectedIdentity is one identity this ServiceAccount was issued, copied from
// the record that holds it.
//
// Everything here was written by the operator that issued it, and only by that
// one. A ServiceAccount asking two operators gets two of these on one object,
// each owned by its own writer -- which is what stops the two from undoing each
// other, the way two operators writing one DatabricksAccount's status once did.
type ProjectedIdentity struct {
	// request is the profile name: what a workload passes to the Databricks SDK
	// to use this identity. It is the identity's own name, or the operator's
	// reference for the unnamed one. See Request.String.
	//
	// It is the key, so one operator writes one entry and cannot touch another's.
	// Unique across operators without anything checking: a name is an annotation
	// map key on this one ServiceAccount, and no name holds the "/" that every
	// operator reference holds.
	Request string `json:"request"`

	// operator is the DatabricksAccount naming the operator that issued this, as
	// <its namespace>/<its name> -- the value the ServiceAccount wrote in the
	// annotation key that asked for it, echoed back.
	//
	// Stored rather than read out of the key, and that is what the field is for.
	// The key is the profile name, which for a named identity is the name alone,
	// so an operator looking for its own reference inside the key would find its
	// unnamed entries and none of its named ones -- and every named one it had
	// stopped maintaining would be left on the object still reporting Ready.
	// Whose an entry is is a fact, so it is kept as one. See
	// ProjectedIdentity.AskedOf.
	Operator string `json:"operator"`

	// issued names the record this was copied from, as
	// <the operator's namespace>/<the record's name>, which is enough to reach
	// it: kubectl -n <that namespace> get isdbxsp <that name>.
	//
	// Reported because that record is what this operator acts on and this object
	// is not, and its name is derived from a uid, so it cannot be guessed.
	// +optional
	Issued string `json:"issued,omitempty"`

	// servicePrincipalId is the numeric id Databricks assigned. Federation
	// policies hang off it.
	//
	// A string rather than an integer because these are int64 and already
	// approach the largest integer JSON carries exactly.
	// +optional
	ServicePrincipalID string `json:"servicePrincipalId,omitempty"`

	// accountId is the Databricks account this identity was made in.
	//
	// Without it, "Databricks says this id does not exist" cannot be told from
	// "I asked the wrong account": both answer 404, and reading the second as
	// the first erases the only record of where a live identity is.
	// +optional
	AccountID string `json:"accountId,omitempty"`

	// clientId is the applicationId Databricks assigned, which the workload
	// presents when it exchanges its token, and which anything granting
	// permissions to this workload names.
	//
	// It cannot be derived: Databricks generates it and refuses to accept one.
	// +optional
	ClientID string `json:"clientId,omitempty"`

	// removedServicePrincipalId is an id this operator created and that is no
	// longer in Databricks.
	//
	// It is kept rather than a flag because it is the value to search Databricks'
	// audit log with: that says who deleted it and when. A flag would say
	// something is wrong; an id says whom to ask.
	// +optional
	RemovedServicePrincipalID string `json:"removedServicePrincipalId,omitempty"`

	// subject is the sub claim the federation policy matches, assembled from the
	// ServiceAccount's namespace and name.
	//
	// Reported rather than left to be reconstructed: it is what a person compares
	// against a policy in Databricks, and assembling it by hand is where the
	// mistake happens.
	// +optional
	Subject string `json:"subject,omitempty"`

	// audience is the aud claim on the token this identity is proved with, which
	// its federation policy also names. A token minted for any other audience is
	// refused -- measured -- including the one a pod gets when none is asked for.
	// +optional
	Audience string `json:"audience,omitempty"`

	// conditions report this identity: whether a token can be exchanged for it,
	// and whether the pods under this ServiceAccount carry what it gave them.
	//
	// Both belong here rather than on the object, and Equipped moved here for a
	// reason worth keeping: with two operators, both would compute it and both
	// would write it, and conditions are keyed by type -- so they would contend
	// for one entry. Asked per identity it also says more, because a pod may
	// carry one operator's token and not another's.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// DatabricksServicePrincipalStatus is every identity this ServiceAccount was
// issued, as its owner can see them.
type DatabricksServicePrincipalStatus struct {
	// identities is one entry per identity, keyed by what was asked for.
	//
	// A list rather than an object per identity, so that the name of this object
	// stays the ServiceAccount's and needs no composition rule -- and so that one
	// object answers "what identities does this workload have" rather than
	// leaving somebody to find the others.
	// +listType=map
	// +listMapKey=request
	// +optional
	Identities []ProjectedIdentity `json:"identities,omitempty"`
}

// First is the identity a workload here is given when it names no profile.
//
// Entries are ordered by identity name with the unnamed one first, so this is
// the unnamed identity wherever there is one -- which is the only identity a
// ServiceAccount asking for exactly one has. There is no order its owner wrote
// to honour instead: the request is a set of annotation keys, and a map has no
// order.
func (s *DatabricksServicePrincipalStatus) First() (ProjectedIdentity, bool) {
	if len(s.Identities) == 0 {
		return ProjectedIdentity{}, false
	}
	return s.Identities[0], true
}

// Identity returns the entry for one profile name, and whether there is one.
func (s *DatabricksServicePrincipalStatus) Identity(request string) (*ProjectedIdentity, bool) {
	for i := range s.Identities {
		if s.Identities[i].Request == request {
			return &s.Identities[i], true
		}
	}
	return nil, false
}

// AskedOf reports whether this entry was issued by one operator.
//
// A comparison of a field that says so, not a parse of the key. It used to be
// read back out of the key, and "parse a string to find out who owns this" is
// the shape that produced the worst bug in this codebase.
func (i ProjectedIdentity) AskedOf(operator types.NamespacedName) bool {
	return i.Operator == operator.String()
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dbxsp
// +kubebuilder:printcolumn:name="Client ID",type=string,JSONPath=".status.identities[0].clientId"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.identities[0].conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// DatabricksServicePrincipal shows the owner of one ServiceAccount what this
// operator issued to it. It is a projection and it decides nothing.
//
// Every field on it was copied from an IssuedDatabricksServicePrincipal in the
// operator's own namespace, which is the record and the only one. This object
// carries no finalizer, and deleting it means nothing: it is rebuilt on the next
// pass, the way a status that was scribbled on is overwritten. Nothing about the
// identity in Databricks depends on it existing.
//
// It is here because the record is not, and cannot be: the record has to outlive
// the namespace that asked for the identity, and a person holding that namespace
// cannot be given the operator's own. So the facts are copied to where they can
// be read -- including why an identity is stuck, which is the thing its owner
// would otherwise have to ask the platform team for.
//
// The operator never reads this object to decide anything. The moment it did,
// this would stop being a projection and become a second record, and two records
// disagree eventually without anybody noticing.
//
// It has no spec. Everything is already addressable: the name and namespace say
// which ServiceAccount, and the annotation on that ServiceAccount is the
// request. A copy here would be new only in the sense of being able to disagree
// with its source.
//
// The name is load-bearing. One object per ServiceAccount, named after it, is
// what makes "one identity per ServiceAccount" a rule the API server enforces
// rather than something this operator has to keep true.
type DatabricksServicePrincipal struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Status DatabricksServicePrincipalStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DatabricksServicePrincipalList contains a list of DatabricksServicePrincipal.
type DatabricksServicePrincipalList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatabricksServicePrincipal `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &DatabricksServicePrincipal{}, &DatabricksServicePrincipalList{})
		return nil
	})
}
