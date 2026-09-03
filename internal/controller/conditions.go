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

package controller

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// conditionReady says whether a token presented by this subject can be
	// exchanged for the service principal this object records. Anything less
	// than both halves -- the service principal and a federation policy naming
	// this subject -- is not ready, because a service principal nothing reaches
	// is an identity nobody can use.
	conditionReady = "Ready"

	// conditionEquipped says whether every pod running under this ServiceAccount
	// carries what it was supposed to be given.
	//
	// Stated as intent rather than as a list, deliberately. What a workload needs
	// is a token and somewhere to send it, and naming only the token was this
	// condition claiming more than it checked -- a pod carrying half of what it
	// needs was reported as equipped, and failed on its first call with an SDK
	// error naming neither this operator nor what was missing. The list belongs
	// in the message, which says which pods and what to do; the name says the
	// promise, so that adding to what is injected does not require renaming a
	// condition people alert on.
	//
	// Separate from Ready because they are different facts and fail
	// independently: Ready is whether the identity can be exchanged for at all,
	// and this is whether the pods that would exchange it were given what they
	// need. A pod created while the webhook was unavailable is admitted
	// unchanged -- that is deliberate, so that an outage here never stops
	// workloads from starting -- and it stays wrong after the webhook comes
	// back, because nothing rewrites a running pod.
	conditionEquipped = "Equipped"

	// conditionPrepared, on a DatabricksAccount, says whether the operator can
	// do anything at all -- whether it knows what it will write into every
	// federation policy and give to every pod.
	//
	// Both values come out of the operator's own projected token: the issuer
	// every policy names, and the audience every pod's volume asks for. Neither
	// is configured, because a configured value can disagree with what is
	// actually presented and a read one cannot.
	//
	// It is its own condition and not part of Ready, because what breaks is not
	// the account. Ready says a Databricks account was reached and read, which
	// stays true while this is false: the SDK holds the access token it already
	// exchanged, so verification goes on passing for as long as that lasts --
	// about an hour -- while no identity anywhere can be issued or converged.
	//
	// That hour is the whole reason this is a condition rather than a sentence
	// in a message. A message cannot be alerted on, and the state it describes
	// looks like health: the operator is running, the account is Ready, and the
	// only tell is a status line that stops halfway.
	//
	// Named for the promise rather than for what it reads today. Naming the
	// issuer, or the token file, would be this claiming more than it checks --
	// the mistake conditionEquipped was renamed to stop making.
	conditionPrepared = "Prepared"

	// conditionAccountsAgree, on a DatabricksAccount, says whether the
	// identities in this cluster were made in the account it names.
	//
	// It is here because the object whose edit causes them to disagree is the
	// one that says nothing about it. Every affected identity reports
	// AccountMismatch, and none of them fails: their workloads keep working,
	// because exchanging a token does not go through this operator. So nothing
	// alerts, nothing resolves on its own, and finding out means reading every
	// identity's status one at a time. This is the count, on the object that
	// was edited.
	conditionAccountsAgree = "AccountsAgree"
)

// Condition reasons. They are part of the status API -- people match on them and
// alert on them -- so they live together rather than as literals at the point of
// use, where a second spelling would go unnoticed.
const (
	reasonNotFound      = "NotFound"
	reasonInvalidSpec   = "InvalidSpec"
	reasonDenied        = "Denied"
	reasonNotConfigured = "NotConfigured"
	reasonLookupFailed  = "LookupFailed"

	// reasonPrepared is the operator knowing what it will write; reasonUnprepared
	// is it not, with the message saying which value is missing and where it is
	// read from.
	reasonPrepared   = "Prepared"
	reasonUnprepared = "TokenUnreadable"

	// reasonAccountReady and reasonNotSelected belong to the DatabricksAccount:
	// whether the operator can act in it, and whether this one is the account it
	// was told to use.
	reasonAccountReady = "AccountReady"
	reasonNotSelected  = "NotSelected"

	// reasonExchangeable is the one that matters to a workload: a service
	// principal exists and a federation policy trusts this subject, so the
	// token this ServiceAccount is issued can be exchanged for it. It says
	// nothing about what that service principal may do.
	reasonExchangeable = "Exchangeable"

	// reasonRemovedInDatabricks is a service principal this operator created and
	// that somebody entitled to has since deleted there. It is not an error and
	// not a lookup that failed: it is a decision on the governance side, and
	// this operator's part in it is to report it and leave it alone.
	reasonRemovedInDatabricks = "RemovedInDatabricks"

	// reasonNotEquipped is one or more pods running without the token. It is not
	// a fault of the identity, and it does not heal on its own: the pods have to
	// be recreated.
	reasonNotEquipped = "NotEquipped"

	// reasonEquipped is every pod under this ServiceAccount carrying the token.
	reasonEquipped = "Equipped"

	// reasonElsewhere is identities recorded against a different Databricks
	// account than the one this DatabricksAccount names. reasonHere is none.
	reasonElsewhere = "IdentitiesElsewhere"
	reasonHere      = "AllHere"

	// reasonAccountMismatch is an identity recorded against one Databricks
	// account while the operator is acting in another.
	//
	// Unknown rather than False: the identity has not failed and is not known to
	// be wrong. Nothing about it can be established from here, which is the
	// whole point of saying so instead of concluding.
	reasonAccountMismatch = "AccountMismatch"

	// reasonAccountUnknown is a record naming a service principal and not the
	// Databricks account it was made in. It is not AccountMismatch: nothing is
	// known to differ. Nothing is known at all, which is why the operator
	// neither concludes nor acts.
	reasonAccountUnknown = "AccountUnknown"

	// reasonRevokeFailed is the service principal still being there after its
	// record was asked to go. The finalizer is held until it is not.
	reasonRevokeFailed = "RevokeFailed"

	// reasonAwaitingRecord is a projection whose record exists and has not been
	// acted on yet. It is the ordinary first moment of an identity's life, and
	// it is reported rather than left blank so that a workload starting into it
	// sees something other than silence.
	reasonAwaitingRecord = "AwaitingRecord"
)

func setCondition(conditions *[]metav1.Condition, generation int64, conditionType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}
