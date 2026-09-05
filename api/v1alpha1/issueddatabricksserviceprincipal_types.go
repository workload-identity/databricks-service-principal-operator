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
	"crypto/sha256"
	"encoding/base32"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// IssuedServiceAccount is the ServiceAccount an identity was issued to, named
// so that a later one of the same name is not mistaken for it.
//
// The names are what Databricks matches: the subject in a federation policy is
// system:serviceaccount:<namespace>:<name>, and names get reused. The UIDs are
// what says whether the ServiceAccount answering to those names today is the
// one this was issued to. A namespace deleted and recreated under the same name
// is a different namespace, and every identity issued to the old one has to be
// destroyed rather than inherited.
type IssuedServiceAccount struct {
	// namespace and name are the ServiceAccount's, and are what the subject in
	// Databricks is assembled from.
	Namespace string `json:"namespace"`
	Name      string `json:"name"`

	// uid is the ServiceAccount's. It is what every comparison is made on: the
	// name of this record, the marker written into Databricks, and the question
	// asked every pass of whether the ServiceAccount answering to these names
	// today is the one this was issued to.
	//
	// One value is enough. A uid is unique across the cluster and across time,
	// so a namespace recreated under its old name holds ServiceAccounts whose
	// uids are new whatever their names are.
	UID types.UID `json:"uid"`

	// namespaceUid is the namespace's, and nothing decides on it.
	//
	// It is recorded because it answers a question a person asks of a record
	// that has outlived what it names -- was this namespace recreated, or was it
	// only this ServiceAccount -- and because it cannot drift: nothing
	// recomputes it and nothing writes it twice.
	NamespaceUID types.UID `json:"namespaceUid"`
}

// IssuedDatabricksServicePrincipalSpec is what was issued and to whom. It is
// written once, when the record is made, and never changed.
//
// It is a spec rather than a status because it is not an observation of
// anything. Nothing recomputes these values and nothing reconciles towards
// them: they are what was true at the moment this operator created a service
// principal, and their whole use is to be compared against what is true now.
type IssuedDatabricksServicePrincipalSpec struct {
	// accountId is the Databricks account the service principal was created in.
	//
	// Recorded because an id means nothing anywhere else. An operator pointed at
	// another account that acted on this record would delete something it never
	// made, or read a 404 that means "not here" as "gone".
	AccountID string `json:"accountId"`

	// subject is the sub claim written into the federation policy, verbatim.
	//
	// Stored rather than reassembled from the names above. It is what Databricks
	// holds, and comparing the two sides means comparing what was actually sent,
	// not what would be sent if it were sent today.
	Subject string `json:"subject"`

	// serviceAccount is who this was issued to.
	ServiceAccount IssuedServiceAccount `json:"serviceAccount"`

	// identity is what the ServiceAccount called this one, and is empty when it
	// named none.
	//
	// It is what tells this record from the others issued to the same
	// ServiceAccount by the same operator. Every pass asks whether the
	// annotation still names it, and without this that question could only be
	// asked of the ServiceAccount as a whole -- so dropping one identity would
	// destroy them all, or none.
	//
	// Empty is a value here, not a missing one: it is the identity asked for by
	// naming no name, which is what nearly every ServiceAccount does.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="identity is immutable: a record names one identity for its whole life"
	Identity string `json:"identity,omitempty"`
}

// IssuedDatabricksServicePrincipalStatus is what Databricks holds for this
// record now.
type IssuedDatabricksServicePrincipalStatus struct {
	// accountId is the Databricks account this record was acted on in.
	//
	// It is recorded because an id means nothing anywhere else: an operator
	// pointed at another account would delete something it never made, or read
	// the 404 that means "not here" as the one that means "gone".
	//
	// It is in the status rather than the spec because the account is not known
	// to whoever writes the record -- the DatabricksServiceAccount controller
	// knows a ServiceAccount is asking and nothing about Databricks. It is
	// written before the first call is made, not with the answer, so that a pass
	// which creates a service principal and then stops has already said where to
	// look for it.
	// +optional
	AccountID string `json:"accountId,omitempty"`

	// servicePrincipalCreateSentAt is when this operator was about to ask
	// Databricks for the service principal below. It is written before the call
	// is made and never cleared.
	//
	// It is the one fact about this record that cannot be worked out again. The
	// marker, the subject and the display name are all derivable from the spec
	// at any moment; whether a create was already sent is not. Without it, a
	// record naming no id cannot be told from one whose create ran and whose
	// answer never arrived -- and those two permit opposite acts, since the
	// first may create and may be let go of, and the second may do neither.
	//
	// A moment rather than a flag, because a record waiting for that answer has
	// to be able to say how long it has been waiting.
	// +optional
	ServicePrincipalCreateSentAt *metav1.Time `json:"servicePrincipalCreateSentAt,omitempty"`

	// servicePrincipalId is the numeric id Databricks assigned. Federation
	// policies hang off it, and it is what deletes it.
	//
	// Empty for one window only: between the create being sent and its answer
	// being written down. What closes that window is the marker -- the service
	// principal carries it from the moment it exists, so one created by a pass
	// that then crashed is found again rather than left behind.
	//
	// Written once and never cleared, including when Databricks answers that it
	// is gone. It is the value somebody searches the audit log with to find out
	// who deleted it, so clearing it would be the evidence of a removal
	// destroying the evidence it was drawn from.
	//
	// A string rather than an integer because these are int64 and already
	// approach the largest integer JSON carries exactly.
	// +optional
	ServicePrincipalID string `json:"servicePrincipalId,omitempty"`

	// clientId is the applicationId Databricks assigned, which the workload
	// presents when it exchanges its token, and which anything granting
	// permissions to this workload names.
	//
	// It cannot be derived: Databricks generates it and refuses to accept one.
	// +optional
	ClientID string `json:"clientId,omitempty"`

	// issuer and audience are the other two things the federation policy
	// matches, recorded as they were sent.
	// +optional
	Issuer string `json:"issuer,omitempty"`
	// +optional
	Audience string `json:"audience,omitempty"`

	// servicePrincipalRemovedAt is when a get by id answered that the service
	// principal above is not there.
	//
	// Databricks is where identity is governed. One deleted there was deleted by
	// somebody entitled to, and making a new one while the annotation still
	// stands would be this operator overruling them every minute, and winning,
	// since it never tires. So it is recorded and left.
	//
	// Only the moment, because the id it is about is still in servicePrincipalId
	// and stays there: that is the value to search Databricks' audit log with,
	// and it says who deleted it and when. The two together bracket a service
	// principal's life, which is what makes what this record knows readable off
	// the status rather than guessed at from which field happens to be empty.
	// +optional
	ServicePrincipalRemovedAt *metav1.Time `json:"servicePrincipalRemovedAt,omitempty"`

	// conditions report whether the service principal is there and whether a
	// federation policy trusts this subject.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ServicePrincipalState is what a record knows about the service principal it
// is the memory of.
//
// It is not stored anywhere. It is read off the three fields above, so that
// every caller asks one question and gets one answer.
type ServicePrincipalState string

const (
	// ServicePrincipalUnsent is no create having been sent, so nothing can
	// exist. It is the only state in which one may be created, and the only one
	// in which this record may be let go of without asking Databricks anything.
	ServicePrincipalUnsent ServicePrincipalState = "Unsent"

	// ServicePrincipalSent is a create having been sent whose answer never
	// arrived. It is the state "I may have made something I cannot see", and it
	// permits no irreversible act: creating here would make a second service
	// principal carrying one marker, which nothing afterwards can tell apart,
	// and releasing the finalizer here would leave one behind that nothing
	// records. The only move it allows is to keep looking by the marker.
	ServicePrincipalSent ServicePrincipalState = "Sent"

	// ServicePrincipalKnown is there being an id, which is what makes an
	// authoritative question askable: a get by id answers for certain, where a
	// list by marker is a listing and may be behind. It is the only state in
	// which a workload is equipped, and the only one in which a 404 is a
	// deletion.
	ServicePrincipalKnown ServicePrincipalState = "Known"

	// ServicePrincipalGone is such a get having answered that the id is not
	// there. Nothing here builds another.
	ServicePrincipalGone ServicePrincipalState = "Gone"
)

// ServicePrincipalState is what this record knows, resolved most-decided first:
// a 404 answered about the id outranks the id, and the id outranks the mark,
// because each is a later fact about the same service principal than the one
// under it.
//
// One function rather than a comparison at each site. Three fields spell four
// states, and an emptiness compared in isolation means "never knew" on one path,
// "deliberately cleared" on another and "gone" on a third -- so the paths
// disagree about the same record. What each state permits is the whole of what
// this operator does, and it is decided here.
func (s *IssuedDatabricksServicePrincipalStatus) ServicePrincipalState() ServicePrincipalState {
	switch {
	case s.ServicePrincipalRemovedAt != nil:
		return ServicePrincipalGone
	case s.ServicePrincipalID != "":
		return ServicePrincipalKnown
	case s.ServicePrincipalCreateSentAt != nil:
		return ServicePrincipalSent
	default:
		return ServicePrincipalUnsent
	}
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=isdbxsp
// +kubebuilder:printcolumn:name="Subject",type=string,JSONPath=".spec.subject"
// +kubebuilder:printcolumn:name="Client ID",type=string,JSONPath=".status.clientId"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// IssuedDatabricksServicePrincipal is this operator's record that it created a
// service principal in Databricks. It is the only one.
//
// It lives in the operator's own namespace, and that is the whole point. What it
// records outlives the namespace that asked for it: a service principal is in
// Databricks, and deleting a Kubernetes namespace does not reach it. A record
// kept inside that namespace is deleted with it, in an order nothing specifies,
// and what survives is an identity nothing in the cluster remembers -- which the
// next occupant of that namespace name inherits, because a federation policy
// names a subject and a subject is a name that gets reused.
//
// So this record is not deleted by anything happening in a tenant's namespace.
// It is asked, every pass, whether the ServiceAccount it was issued to is still
// there, still asking, and still the same one. When the answer is no it deletes
// itself, and its finalizer deletes the service principal on the way out.
// Nothing about that depends on an event having been seen, on which kind a
// namespace teardown deleted first, or on the operator having been running at
// the time.
//
// Deleting this object destroys the identity. That is what it means, and it is
// the only object here that means it. The DatabricksServiceAccount in the
// tenant's namespace is a copy of this one: it can be deleted, and it comes
// back.
//
// Nobody writes one of these. The name is derived and the spec is immutable.
type IssuedDatabricksServicePrincipal struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="this records what was issued and cannot be changed; delete the object to destroy the identity"
	Spec IssuedDatabricksServicePrincipalSpec `json:"spec"`

	// +optional
	Status IssuedDatabricksServicePrincipalStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// IssuedDatabricksServicePrincipalList contains a list of
// IssuedDatabricksServicePrincipal.
type IssuedDatabricksServicePrincipalList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []IssuedDatabricksServicePrincipal `json:"items"`
}

// issuedNameLimit is what a Kubernetes object name holds, and issuedSuffix is
// how much of that this reserves for what makes the name unique.
const (
	issuedNameLimit = 253
	issuedSuffix    = 10
)

// IssuedNameFor names the record for one issuing.
//
// Readable first and unique second: <namespace>.<serviceaccount>[.<identity>]
// says whose it is at a glance -- none of the three may contain a dot, so the
// dots are unambiguously the separators -- and a suffix derived from the
// ServiceAccount's uid and the identity's name is what keeps two issuings apart.
//
// Both parts are needed. Without the readable part nobody can find anything by
// eye. Without the suffix, a namespace recreated under the same name would want
// a record whose name is already taken by the old one, which is still there
// waiting to destroy the identity it holds.
//
// The identity's name goes after the ServiceAccount rather than before it, so
// that a listing sorts one workload's identities together:
//
//	team-a.etl-h7x3k9qp
//	team-a.etl.reader-p2m8w4tq
//	team-a.etl.writer-k9f3n7bx
//	team-b.loader-x4c1r6ds
//
// Before it, the same listing would gather every "reader" in the cluster and
// scatter each workload's own, which is the question nobody asks.
//
// It is derived rather than generated, so a pass that creates this record and
// then fails asks for the same name next time and is told it already exists,
// rather than making a second record for one service principal. It is derived
// from the ServiceAccount and the identity's name alone, so that an event about
// a ServiceAccount names its records without anything having to be read.
func IssuedNameFor(namespace, name, identity string, uid types.UID) string {
	// The identity's name is hashed along with the uid, not only shown in the
	// readable half: that half is truncated to fit, and two long ServiceAccount
	// names could be cut back to the same string. The suffix is what has to keep
	// them apart, so it is what has to carry the difference.
	sum := sha256.Sum256([]byte(string(uid) + "/" + identity))
	suffix := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).
		EncodeToString(sum[:])[:issuedSuffix])

	readable := namespace + "." + name
	if identity != "" {
		readable += "." + identity
	}
	if room := issuedNameLimit - len(suffix) - 1; len(readable) > room {
		// Trimmed of a trailing ".", because the cut lands wherever the room runs
		// out and a readable half ending in one meets the suffix as ".-", which is
		// a label beginning with "-". Measured: a 10-character namespace, a
		// 230-character ServiceAccount name and the identity "reader" produce
		// exactly that, and validation.IsDNS1123Subdomain refuses it -- inside a
		// reconcile, where the message reaches nobody who asked and the identity is
		// never issued. Both inputs are legal: a namespace is a DNS label and a
		// ServiceAccount name is a DNS subdomain.
		readable = strings.TrimSuffix(readable[:room], ".")
	}
	return readable + "-" + suffix
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion,
			&IssuedDatabricksServicePrincipal{}, &IssuedDatabricksServicePrincipalList{})
		return nil
	})
}
