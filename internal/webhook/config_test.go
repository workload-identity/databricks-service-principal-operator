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

package webhook

import (
	"strings"
	"testing"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

// TestNothingAtAllIsRenderedWhenNoIdentityHasAClientId is a contract of
// Configuration itself, and the caller that depends on it is not the webhook.
//
// The webhook drops unconverged identities before it renders, so it never asks
// this question. The controller's describes does: it renders one identity's
// block and asks whether the pod's annotation contains it, and it reads the
// empty string as "there is nothing to look for". A block with client_id and
// nothing after it is not empty, every annotation contains a substring of some
// pod, and Equipped would report True for pods carrying no such profile --
// which matters exactly when an identity is recorded as removed in Databricks,
// because that clears the client id.
//
// So the empty answer has to be exactly empty. Not a newline, not a section
// header for a profile naming nobody.
func TestNothingAtAllIsRenderedWhenNoIdentityHasAClientId(t *testing.T) {
	t.Parallel()
	rendered := Configuration([]dbxv1alpha1.ProjectedIdentity{
		{Profile: testOperator, Operator: testOperator, Audience: testAudience},
		{Profile: "reader", Operator: testOperator, Audience: testAudience},
	})
	if rendered != "" {
		t.Errorf("Configuration rendered %q for identities Databricks has not answered for. "+
			"describes reads a non-empty answer as a block to look for in the pod, and reports "+
			"every pod in the namespace as equipped with it", rendered)
	}
}

// TestAnIdentityDatabricksHasNotAnsweredForContributesNoProfile is the same
// property where the answer is not empty.
//
// A profile naming no client cannot be exchanged for anything -- it fails as
// TOKEN_INVALID with the federation policy echoed back, which reads as though
// the policy were wrong -- so an identity still without one has to leave the
// file exactly as it would be had that identity not been in the list, including
// the blank lines between the profiles that survive it.
func TestAnIdentityDatabricksHasNotAnsweredForContributesNoProfile(t *testing.T) {
	t.Parallel()
	converged := []dbxv1alpha1.ProjectedIdentity{
		{Profile: "reader", Operator: testOperator, ClientID: "reader-client", Audience: testAudience},
		{Profile: "writer", Operator: testOperator, ClientID: "writer-client", Audience: testAudience},
	}
	// One in every position, because the separator is written per profile and a
	// leading, trailing or interior gap are three different mistakes.
	mixed := []dbxv1alpha1.ProjectedIdentity{
		{Profile: "pending-first", Operator: testOperator, Audience: testAudience},
		converged[0],
		{Profile: "pending-middle", Operator: testOperator, Audience: testAudience},
		converged[1],
		{Profile: "pending-last", Operator: testOperator, Audience: testAudience},
	}

	rendered := Configuration(mixed)
	if strings.Contains(rendered, "pending-") {
		t.Errorf("the configuration is %q and names an identity with no client id; the workload "+
			"gets a profile whose only outcome is TOKEN_INVALID", rendered)
	}
	if want := Configuration(converged); rendered != want {
		t.Errorf("the configuration is %q, want %q -- byte for byte what the converged "+
			"identities render on their own", rendered, want)
	}

	// And the neighbours are readable, so the comparison above is between two
	// files the SDK resolves rather than two that are equally wrong.
	if got := resolvedProfile(t, rendered, "reader").ClientID; got != "reader-client" {
		t.Errorf("profile %q resolves client_id %q", "reader", got)
	}
	if got := resolvedProfile(t, rendered, "writer").ClientID; got != "writer-client" {
		t.Errorf("profile %q resolves client_id %q", "writer", got)
	}
}

// TestEitherSdkFindsANameItReads is what makes the profile usable by a workload
// written in either language.
//
// One value, two names. The Go SDK declares the token path as
// databricks_id_token_filepath and the audience as audience; the Python SDK
// declares them as oidc_token_filepath and token_audience. A reader drops the
// name it does not declare without saying so, so a profile written for one is
// not something the other partly understands: it is one that fails as "default
// auth: cannot configure default credentials" for naming no path it knows, or
// resolves cleanly having never been told what its token was minted for.
//
// Asserted against the text because there is no Python here to resolve it with.
// What the Go SDK makes of this same profile, the names meant for the other one
// included, is TestWhatThePodIsGivenIsWhatTheSdkReads.
func TestEitherSdkFindsANameItReads(t *testing.T) {
	t.Parallel()
	rendered := Configuration([]dbxv1alpha1.ProjectedIdentity{{
		Profile:  testOperator,
		Operator: testOperator,
		ClientID: testClientID,
		Audience: testAudience,
	}})

	for _, line := range []string{
		"databricks_id_token_filepath = " + TokenPathFor(testOperator),
		"oidc_token_filepath = " + TokenPathFor(testOperator),
		"audience = " + testAudience,
		"token_audience = " + testAudience,
	} {
		// Anchored on both sides, because "audience = databricks" is a substring
		// of the token_audience line and each of these has to be its own.
		if !strings.Contains(rendered, "\n"+line+"\n") {
			t.Errorf("the configuration is %q and does not carry %q on a line of its own; the "+
				"SDK that reads that name is left holding a profile it cannot complete",
				rendered, line)
		}
	}
}
