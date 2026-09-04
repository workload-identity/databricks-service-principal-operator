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

	// conditionNamespacesWithdrawn, on a DatabricksAccount, says whether every
	// namespace this operator issued in and this account no longer names has
	// actually been let go of: this cluster's trust removed from every identity
	// it issued there, and the namespace released again afterwards.
	//
	// It is on this object for the reason conditionAccountsAgree is. The edit
	// that starts a withdrawal is made here, the work of it happens on records
	// in another kind and on Namespaces in another scope, and none of them is
	// somewhere the person who made the edit would think to look. Without this,
	// a withdrawal that stalled on one namespace out of five would be found by
	// reading every record's status one at a time.
	//
	// False is not a failure of the account: nothing it names is wrong, and
	// nothing anywhere has been destroyed. It is a withdrawal that has not
	// finished, which is a thing to wait for or to act on, and it is written
	// here so that waiting is a choice somebody makes rather than one they make
	// by not knowing.
	conditionNamespacesWithdrawn = "NamespacesWithdrawn"
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

	// reasonRejected is Databricks refusing the request itself as invalid, which
	// it answers 400 rather than 404. NotFound next to it is answered by
	// restoring what was named or pointing this at something else; this one is
	// answered only by changing what is sent, and no act in Databricks makes the
	// same request acceptable.
	reasonRejected = "Rejected"

	// reasonMalformedRecord is a value this operator wrote down that it can no
	// longer act on: the service principal id on the record will not parse as
	// the coordinate the call takes, so nothing is asked of Databricks at all.
	// Not InvalidSpec, which is next to it for a declaration that is wrong --
	// nobody declared this value, and sending whoever reads it to the spec sends
	// them to a file with nothing wrong in it.
	reasonMalformedRecord = "MalformedRecord"

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

	// reasonNotEquipped is one or more pods running without what they were to be
	// given. It is not a fault of the identity, and it does not heal on its own:
	// nothing rewrites a running pod. What ends it is in the message, which is
	// not always recreating the pods -- a namespace that injects nothing, and a
	// pod whose volume name was already taken, both come back the same.
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

	// reasonNotServed is an identity in a namespace this operator's account no
	// longer names. Its trust has been removed, so no token from this cluster
	// can be exchanged for it -- which is False rather than anything else,
	// because the one thing Ready promises is exactly that exchange.
	//
	// It is not RemovedInDatabricks next to it, and the difference is what
	// happens on the way back: nothing was destroyed here, so naming the
	// namespace again puts the trust back on the same service principal, with
	// the same client id and every grant on it.
	reasonNotServed = "NotServed"

	// reasonMintingNotSuspended is a withdrawal that has not started, because
	// nothing has stopped the namespace it is for from minting. Unknown rather
	// than False: nothing has been asked of Databricks, so there is nothing to
	// conclude, and False would report an exchange as ended on the strength of an
	// intention.
	//
	// It is its own reason and not silence, because waiting is what somebody
	// waiting for a withdrawal has to be able to see. What ends it is this
	// operator claiming the namespace, which it does on its account's own pass --
	// so a record sitting here is either a moment old or a Namespace this
	// operator could not write, and the account it left says which.
	reasonMintingNotSuspended = "MintingNotSuspended"

	// reasonAnotherWithdrawal is a namespace two operators serve, where the other
	// one is withdrawing from it and holds the key that says so. One withdrawal
	// at a time is the point of there being one key: each takes the trust off its
	// own identities, and the set either is working through must not grow under
	// it while the other reopens minting.
	//
	// Unknown, and nothing was asked of Databricks. Waiting costs nothing here --
	// the other operator lets go when it has finished, and if it dies instead its
	// claim goes stale and this one takes it.
	reasonAnotherWithdrawal = "AnotherWithdrawal"

	// reasonWithdrawFailed is that removal not having gone through. The trust is
	// still in place and this cluster can still exchange for the identity, which
	// is the opposite of what the edit asked for, so it is reported rather than
	// left to look like the withdrawal above.
	reasonWithdrawFailed = "WithdrawFailed"

	// reasonWithdrawn is every namespace this account stopped serving having
	// been let go of; reasonWithdrawing is at least one that has not, with the
	// message naming which and what is outstanding.
	reasonWithdrawn   = "Withdrawn"
	reasonWithdrawing = "Withdrawing"

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
