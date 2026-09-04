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
	"path"
	"strings"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

// Profiles renders the identities as the Databricks SDK's own config file, one
// profile per identity.
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
func Profiles(identities []dbxv1alpha1.ProjectedIdentity) string {
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
		rendered.WriteString("[" + identity.Profile + "]\n")

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

		// The two values below are each written twice, because the SDKs do not
		// agree on what to call them in a profile and this operator cannot see
		// which one the workload uses. Go reads databricks_id_token_filepath and
		// audience; Python reads oidc_token_filepath and token_audience. Their
		// environment variables do agree -- DATABRICKS_OIDC_TOKEN_FILEPATH and
		// DATABRICKS_TOKEN_AUDIENCE on both -- so the profile is the only place
		// the split shows, which is why it is easy to miss.
		//
		// Writing all four is safe in both directions, and that is a measured
		// fact rather than a general one: Go looks up each field it declares by
		// the name in its struct tag and never sees the rest, and Python assigns
		// every key in the section and reads only the ones it declared. Neither
		// says which ones it dropped.
		//
		// That silence is the reason to write both rather than pick. A profile
		// carrying auth_type = file-oidc and no path its reader knows fails as
		// "default auth: cannot configure default credentials", which names
		// nothing that is wrong; and one carrying no audience its reader knows
		// resolves cleanly, having simply never been told what the token it
		// names was minted for.
		rendered.WriteString("databricks_id_token_filepath = " + TokenPathFor(identity.Profile) + "\n")
		rendered.WriteString("oidc_token_filepath = " + TokenPathFor(identity.Profile) + "\n")
		rendered.WriteString("audience = " + identity.Audience + "\n")
		rendered.WriteString("token_audience = " + identity.Audience + "\n")
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
func TokenPathFor(profile string) string {
	return path.Join(TokenMountPath, profile, TokenFile)
}

// TokenProjectionPathFor is the same path relative to the volume, which is what
// a projected source names.
func TokenProjectionPathFor(profile string) string {
	return path.Join(profile, TokenFile)
}
