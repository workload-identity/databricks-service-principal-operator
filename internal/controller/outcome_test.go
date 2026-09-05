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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/apierr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dbx "github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

// TestEveryFailureIsAnsweredByTheOneItCallsFor pins the whole of outcomeFor,
// which is where a Databricks failure becomes something a person reads and
// something the operator does next.
//
// Three separate decisions per branch, and each is wrong in its own way. The
// status decides whether a declaration is reported as wrong: NotConfigured and
// Denied are Unknown because nothing was checked, and False there would blame a
// spec nobody has looked at. The reason is what people match and alert on. And
// Err decides how the object comes back -- its controller's fixed interval for
// anything a person has to fix in Databricks, the workqueue's exponential
// backoff for the one case where trying again sooner is right.
//
// Swapping any of the three leaves every other test in this package green: the
// controllers pass whatever this returns straight through.
func TestEveryFailureIsAnsweredByTheOneItCallsFor(t *testing.T) {
	t.Parallel()
	for _, failure := range []struct {
		what     string
		gave     error
		status   metav1.ConditionStatus
		reason   string
		backsOff bool
	}{
		{
			what:   "Databricks looked and it is not there",
			gave:   apierr.ErrResourceDoesNotExist,
			status: metav1.ConditionFalse,
			reason: reasonNotFound,
		},
		{
			what: "Databricks refused the request itself",
			gave: &apierr.APIError{
				ErrorCode:  "INVALID_PARAMETER_VALUE",
				StatusCode: 400,
				Message:    "the subject is not acceptable",
			},
			status: metav1.ConditionFalse,
			reason: reasonRejected,
		},
		{
			what: "a recorded value that is not a Databricks coordinate",
			gave: &dbx.ErrMalformedCoordinate{
				Field: "servicePrincipalId", Value: "not-a-number", Cause: errors.New("invalid syntax"),
			},
			status: metav1.ConditionFalse,
			reason: reasonMalformedRecord,
		},
		{
			what:   "no DatabricksAccount the operator can act in",
			gave:   &dbx.ErrNotConfigured{Namespace: operatorNamespace, Name: databricksAccountName},
			status: metav1.ConditionUnknown,
			reason: reasonNotConfigured,
		},
		{
			what:   "Databricks refusing the operator on privilege",
			gave:   apierr.ErrPermissionDenied,
			status: metav1.ConditionUnknown,
			reason: reasonDenied,
		},
		{
			what:     "anything else Databricks did not answer",
			gave:     errors.New("dial tcp: lookup accounts.cloud.databricks.com: no such host"),
			status:   metav1.ConditionUnknown,
			reason:   reasonDatabricksUnavailable,
			backsOff: true,
		},
	} {
		t.Run(failure.what, func(t *testing.T) {
			t.Parallel()
			got := outcomeFor(failure.gave)

			if got.Status != failure.status {
				t.Errorf("status is %s, want %s -- %s", got.Status, failure.status, failure.what)
			}
			if got.Reason != failure.reason {
				t.Errorf("reason is %q, want %q; it is what people match and alert on",
					got.Reason, failure.reason)
			}
			if got.Message != dbx.Reason(failure.gave) {
				t.Errorf("message is %q, want %q -- a paraphrased message is one nobody can "+
					"search for", got.Message, dbx.Reason(failure.gave))
			}

			if failure.backsOff {
				if got.Err == nil {
					t.Error("no error was returned, so this never reaches the workqueue's " +
						"backoff and never counts as a reconcile failure")
				}
				return
			}
			if got.Err != nil {
				t.Errorf("returned %v; this goes back on the workqueue's exponential backoff, "+
					"which reports a thing only a person can fix as though Databricks were "+
					"down and hides it behind an ever-longer wait", got.Err)
			}
		})
	}
}

// TestAnObjectNobodyIsWaitingOnIsAskedAboutLessOften covers retryAfterFor, which
// is the whole of how often this operator talks to Databricks.
//
// Not Ready means somebody declared something, or fixed something in Databricks,
// and nothing in Kubernetes will tell us it worked. Ready means nobody is
// waiting, and the only reason to look again is to notice the identity being
// dropped in Databricks. Answering the settled interval to something that is not
// Ready leaves that person waiting ten minutes for a fix they already made.
func TestAnObjectNobodyIsWaitingOnIsAskedAboutLessOften(t *testing.T) {
	t.Parallel()
	// Neither is an interval this operator actually uses. The function only picks
	// between the two it is handed, and naming the real ones would leave a test
	// that still passed after they were swapped at every call site.
	const awaited, settled = 3 * time.Second, 7 * time.Hour

	for _, status := range []metav1.ConditionStatus{metav1.ConditionFalse, metav1.ConditionUnknown} {
		if got := retryAfterFor(status, awaited, settled); got != awaited {
			t.Errorf("%s waits %v, want %v -- somebody is waiting on this one",
				status, got, awaited)
		}
	}
	if got := retryAfterFor(metav1.ConditionTrue, awaited, settled); got != settled {
		t.Errorf("True waits %v, want %v -- a working identity would be asked about at the "+
			"rate meant for a broken one", got, settled)
	}
}

// TestOnlyAnUnreachableDatabricksIsHandedBackToTheWorkqueue covers what a
// controller does with the decision outcomeFor made.
//
// The condition is the whole of what a person sees, and it is written whichever
// kind of failure this was. What the two kinds do not share is the return: an
// unreachable Databricks goes back as an error, so the record is backed off
// exponentially and counted as a reconcile failure -- which is the only thing
// that puts an outage in the manager's log or in a metric somebody alerts on. A
// refusal is returned as no failure at all, because it is fixed by a person in
// Databricks and backing off from it delays noticing the moment they do.
func TestOnlyAnUnreachableDatabricksIsHandedBackToTheWorkqueue(t *testing.T) {
	t.Parallel()
	for _, failure := range []struct {
		what     string
		gave     error
		returned bool
		reason   string
	}{
		{
			what:     "Databricks did not answer",
			gave:     errors.New("dial tcp: lookup accounts.cloud.databricks.com: no such host"),
			returned: true,
			reason:   reasonDatabricksUnavailable,
		},
		{
			what:     "Databricks refused the operator",
			gave:     fmt.Errorf("writing the policy: %w", apierr.ErrPermissionDenied),
			returned: false,
			reason:   reasonDenied,
		},
	} {
		t.Run(failure.what, func(t *testing.T) {
			t.Parallel()
			serviceAccount := asking(testNamespace, testName)
			c := newControllers(t, &stubClients{policyErr: failure.gave},
				mintingNamespace(testNamespace), serviceAccount)
			c.settle(t)

			issued := c.issuedOf(t, serviceAccount)
			if issued == nil {
				t.Fatal("nothing was issued, so no pass reached Databricks")
			}
			ready := readyOf(t, c.Client, issued)
			if ready == nil || ready.Status == metav1.ConditionTrue || ready.Reason != failure.reason {
				t.Fatalf("Ready is %v, want %s and not True; whatever else a failure does, the "+
					"person reading the object has to be told about it", ready, failure.reason)
			}

			passes := c.recordPasses(t)
			if failure.returned && len(passes) == 0 {
				t.Error("the pass returned nothing, so nothing counts a reconcile error, nothing " +
					"backs the record off, and an outage that lasts hours is a condition on an " +
					"object and silence everywhere else")
			}
			if !failure.returned && len(passes) != 0 {
				t.Errorf("the pass returned %v; a refusal only a person can lift is then retried "+
					"on an ever-longer backoff and counted against an operator that is working "+
					"correctly", passes)
			}
		})
	}
}
