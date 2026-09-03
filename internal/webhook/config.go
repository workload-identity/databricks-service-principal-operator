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

package webhook

import (
	"path"
	"strings"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

// Configuration renders the identities as the Databricks SDK's own config file,
// one profile per identity.
//
// The SDK's own format rather than anything of this project's, because the
// workload already has a way to read it and this operator has no business
// inventing a second one. Every SDK and the CLI take a config file and a profile
// name; what is written here is what they already know how to consume.
//
// The profile name is the string the ServiceAccount wrote, unchanged: the
// identity's own name where it has one, and the operator's reference for the
// unnamed identity. Measured: a section name may hold a slash, which is what
// lets the unnamed one be the reference itself rather than something derived
// from it -- and a derivation would be a rule the workload had to learn in order
// to name its own profile.
func Configuration(identities []dbxv1alpha1.ProjectedIdentity) string {
	var rendered strings.Builder
	for _, identity := range identities {
		if identity.ClientID == "" {
			// Nothing to write. Databricks assigns the client id on create, so an
			// identity without one has not converged, and a profile naming no
			// client cannot be exchanged for anything -- it would fail as
			// TOKEN_INVALID with the federation policy echoed back, which reads
			// as though the policy were wrong.
			continue
		}
		if rendered.Len() > 0 {
			rendered.WriteString("\n")
		}
		rendered.WriteString("[" + identity.Request + "]\n")

		// No host, and that is the whole of what this operator does not decide.
		// A host is a destination and this issues identities; measured against
		// the SDK, a profile without one resolves cleanly and takes
		// DATABRICKS_HOST from the environment, keeping everything below.
		//
		// auth_type is what resolution actually depends on. Without it the SDK
		// stops at "validate: more than one authorization method configured:
		// file-oidc and oauth" -- client_id marks one authorization family and
		// the token path marks another, and it will not choose between them.
		rendered.WriteString("auth_type = file-oidc\n")
		rendered.WriteString("client_id = " + identity.ClientID + "\n")
		rendered.WriteString("databricks_id_token_filepath = " + TokenPathFor(identity.Request) + "\n")

		// "audience", not "token_audience". The SDK looks a profile's keys up by
		// the name in its own struct tag, and Config.TokenAudience is tagged
		// name:"audience"; a key it does not know is dropped without a word.
		// Measured against v0.176.0 with the profile above: as token_audience it
		// resolved TokenAudience="", as audience it resolves "databricks".
		// Nothing said so for as long as the wrong name was written, because a
		// profile missing the audience resolves cleanly -- the audience was
		// simply not in what the workload got.
		rendered.WriteString("audience = " + identity.Audience + "\n")
	}
	return rendered.String()
}

// TokenPathFor is where one identity's token appears in the pod.
//
// Under a directory named for the profile, so that several identities are
// several files. The name is used whole -- a named identity is one segment, and
// the unnamed one's reference holds the one slash that makes it two -- so
// nothing has to be escaped or shortened, and the path a workload reads in its
// profile is the name it asked under.
func TokenPathFor(request string) string {
	return path.Join(TokenMountPath, request, TokenFile)
}

// TokenProjectionPathFor is the same path relative to the volume, which is what
// a projected source names.
func TokenProjectionPathFor(request string) string {
	return path.Join(request, TokenFile)
}
