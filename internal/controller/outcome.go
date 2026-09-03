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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

// outcome is what one lookup against Databricks means: how to say it in a
// condition, and when to come back.
//
// Both fields put the object back in the queue; they differ in how. Result asks
// for it after a fixed delay. Err hands it to the rate limiter instead, which
// counts a failure, backs off exponentially, and records it as a reconcile
// error -- right when Databricks may recover on its own, wrong when the answer
// will not change until a person acts.
type outcome struct {
	Status  metav1.ConditionStatus
	Reason  string
	Message string
	Result  ctrl.Result
	Err     error
}

// outcomeFor turns a failure into that decision. It is the only place the
// mapping lives: a new kind of failure is handled by extending this switch, and
// no controller has to be told about it.
func outcomeFor(err error) outcome {
	const retryAfter = servicePrincipalRetryAfterAwaited
	message := databricks.Reason(err)
	switch databricks.KindOf(err) {
	case databricks.NotFound:
		// Databricks looked and the thing is not there. What answers it is
		// somebody restoring it, or pointing this at something else, and either
		// raises an event of its own.
		return outcome{
			Status: metav1.ConditionFalse, Reason: reasonNotFound, Message: message,
			Result: ctrl.Result{RequeueAfter: retryAfter},
		}
	case databricks.Rejected:
		// Databricks refused the request itself rather than what it named, and
		// it is kept apart from NotFound because the two are answered by
		// different people doing different things: this one by whoever can
		// change what is sent, and nothing done in Databricks makes the same
		// request acceptable.
		return outcome{
			Status: metav1.ConditionFalse, Reason: reasonRejected, Message: message,
			Result: ctrl.Result{RequeueAfter: retryAfter},
		}
	case databricks.Malformed:
		// The request was never made: a value will not parse as the coordinate
		// the call takes. The call that produces this reads the service
		// principal id off the record, which this operator wrote from what
		// Databricks answered -- so nothing anybody declared is wrong, and a
		// reason naming the spec sends its reader to a file with nothing wrong
		// in it.
		return outcome{
			Status: metav1.ConditionFalse, Reason: reasonMalformedRecord, Message: message,
			Result: ctrl.Result{RequeueAfter: retryAfter},
		}
	case databricks.NotConfigured:
		// Unknown, and pointedly not False: nothing has been checked, so saying
		// False would report a declaration as wrong on the strength of never
		// having looked at it.
		//
		// The delay is a safety net rather than the way this is meant to
		// recover. The account controller enqueues everything once it has usable
		// clients, so the normal path is an event, not a wait.
		return outcome{
			Status: metav1.ConditionUnknown, Reason: reasonNotConfigured, Message: message,
			Result: ctrl.Result{RequeueAfter: retryAfter},
		}
	case databricks.Denied:
		// Not this object's fault, so it is not reported as one. What fixes it
		// is a permission given to the operator's own service principal, made in
		// Databricks, which raises no event here -- so a fixed delay is the only
		// way to notice it landing.
		return outcome{
			Status: metav1.ConditionUnknown, Reason: reasonDenied, Message: message,
			Result: ctrl.Result{RequeueAfter: retryAfter},
		}
	default:
		// Unknown rather than False: the thing may be perfectly fine, and saying
		// False would blame a declaration that has not been shown to be wrong.
		// This is the one branch that goes back to the workqueue, where the
		// backoff belongs.
		return outcome{
			Status: metav1.ConditionUnknown, Reason: reasonLookupFailed, Message: message,
			Err: err,
		}
	}
}

// retryAfterFor picks how long to wait before looking again.
//
// An object that is not Ready has somebody waiting on it: they declared
// something, or fixed something in Databricks, and nothing in Kubernetes will
// tell us it worked. An object that is Ready has nobody waiting -- the check is
// only there to notice it being dropped in Databricks, which raises no event
// either but which no one is watching for.
//
// external-dns faces the same shape, an external system with no watch, and polls
// every minute by default.
func retryAfterFor(status metav1.ConditionStatus, awaited, settled time.Duration) time.Duration {
	if status == metav1.ConditionTrue {
		return settled
	}
	return awaited
}
