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
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// DatabricksAccountSpec says which Databricks account the operator acts in, and
// as whom.
//
// It is the operator's own configuration rather than anything a workload
// declares, and it is namespaced so that changing it needs write access to the
// operator's own namespace -- which is already the access that could replace the
// operator's image or its ServiceAccount. Expressing it as a declaration rather
// than as startup arguments is what lets the operator answer back: whether the
// exchange works, and, when it does not, which subject and audience it was
// presenting.
//
// Nothing here is a secret, and it is deliberately not stored as one. A host is
// a URL. An account id is an identifier. clientId is the client_id of an OAuth
// client that has no client secret -- what gates the exchange is the federation
// policy in Databricks, naming an issuer, a subject and an audience. Holding
// these three values without the ability to mint a token from that issuer, for
// that subject, obtains nothing. Storing them as a Secret would assert they need
// protecting, and this project's claim is that nothing here does.
//
// Two values that belong to the operator rather than to the account are not
// here: where its projected token is mounted, and the audience that token
// carries. Both are set in the Deployment, because the audience has to agree
// with the projected volume beside it, and a value that must match its neighbour
// belongs next to it rather than in a separate object where nothing can compare
// them.
type DatabricksAccountSpec struct {
	// host is the Databricks account console host, such as
	// https://accounts.cloud.databricks.com.
	//
	// On unified deployments the account and its workspaces share one host, so
	// this is not always an "accounts." name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^https://`
	// +required
	Host string `json:"host"`

	// accountId identifies the Databricks account.
	//
	//	databricks account users list -o json | jq -r '.[0].id'  # not this
	//
	// It is in the account console URL, and in the CLI's own profile:
	//
	//	databricks auth describe -p <account profile>
	// +kubebuilder:validation:MinLength=1
	// +required
	AccountID string `json:"accountId"`

	// clientId is the applicationId of the service principal the operator acts
	// as -- not its numeric id, and not its display name.
	//
	// Token exchange without it fails as TOKEN_INVALID with the federation
	// policy's own description echoed back, which reads as though the policy
	// were wrong. It is not.
	// +kubebuilder:validation:MinLength=1
	// +required
	ClientID string `json:"clientId"`

	// namespaces are the namespaces this operator will serve, by name.
	//
	// This is where a platform team says what their account admin credential may
	// be spent on. It is theirs and nobody else's: the object lives in their own
	// namespace, and no namespace can add itself to it.
	//
	// Empty serves nothing, and that is the default rather than an oversight. A
	// permissive default would have an operator installed for one team quietly
	// answering every ServiceAccount in the cluster that happened to name it.
	//
	// Taking a namespace off this list means this account no longer serves it,
	// and it destroys nothing. The operator suspends minting there -- it labels
	// the Namespace with its own name, which is a claim no second operator can
	// take while it is held -- and then removes every federation policy it wrote
	// for identities in it, in that order, because a removal made while
	// identities can still be minted there would leave behind the ones minted
	// after it. Then it lifts the label, and whoever else serves the namespace
	// goes on minting with nothing for a person to do. The service principals
	// stay, with every grant made on them, and so do the records and their ids.
	// What goes is the one thing a cluster can take back: a token from here can
	// no longer be exchanged. A token already in a running pod is not recalled
	// by any of this -- the pod holds it, and Databricks alone decides whether to
	// accept it.
	//
	// Naming a namespace here again puts the trust back on the same service
	// principals, with the same client ids and every grant intact.
	//
	// Names rather than a label selector. Neither is safer -- a namespace coming
	// into scope still needs the cluster's mint label open and a ServiceAccount
	// naming this operator -- so the choice is auditability against flexibility.
	// A list answers "which namespaces does this operator serve" by being read. A
	// selector answers it only by being evaluated against every namespace in the
	// cluster, at the moment somebody asks, and the answer changes when somebody
	// who is not on that team relabels a namespace.
	//
	// +listType=set
	// +optional
	Namespaces []string `json:"namespaces,omitempty"`
}

// Serves reports whether this operator will act in a namespace.
func (s *DatabricksAccountSpec) Serves(namespace string) bool {
	return slices.Contains(s.Namespaces, namespace)
}

// DatabricksAccountStatus reports whether the operator can act in this account,
// and as whom.
type DatabricksAccountStatus struct {
	// subject is the sub claim the operator presents when it exchanges its
	// token. The federation policy in Databricks has to name this exact string.
	//
	// It is reported because it cannot be derived by reading anything the
	// installer has: it is assembled from the operator's namespace and
	// ServiceAccount name, both of which kustomize rewrites. Getting it wrong
	// produces a refusal that quotes the federation policy back, sending whoever
	// reads it to fix a policy that is correct. So the operator states who it is
	// rather than leaving it to be reconstructed.
	// +optional
	Subject string `json:"subject,omitempty"`

	// audience is the aud claim on the token the operator presents, which the
	// federation policy must also name. It comes from the Deployment, not from
	// the spec above.
	// +optional
	Audience string `json:"audience,omitempty"`

	// conditions report whether this account is usable.
	//
	// Ready is true once the operator has exchanged its token and made one
	// account-level call. Until then no identity can be created, and each one
	// says so rather than failing in a way that looks like the ServiceAccount
	// having asked wrongly.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dbxacc
// +kubebuilder:printcolumn:name="Host",type=string,JSONPath=".spec.host"
// +kubebuilder:printcolumn:name="Subject",type=string,JSONPath=".status.subject"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// DatabricksAccount is the account this operator acts in.
//
// The operator uses the one named by its --databricks-account flag, in its own
// namespace. Which one that is, is therefore a decision made when the operator
// is deployed rather than by whoever can create an object here, and there is
// never a question of which of several is live. Any other DatabricksAccount is
// left alone and says on its own status that it was not selected -- silence
// would be indistinguishable from an operator that is not running.
//
// That also makes a change something you can stage: write the new account, see
// it accepted, then move the flag. Adopting whichever one happened to be present
// would mean deleting the old one first and being broken in between.
type DatabricksAccount struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec DatabricksAccountSpec `json:"spec"`

	// +optional
	Status DatabricksAccountStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DatabricksAccountList contains a list of DatabricksAccount.
type DatabricksAccountList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatabricksAccount `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &DatabricksAccount{}, &DatabricksAccountList{})
		return nil
	})
}
