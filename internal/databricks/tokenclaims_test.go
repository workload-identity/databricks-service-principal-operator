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

package databricks

import (
	"encoding/base64"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func jwtWith(payload string) string {
	return "header." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".signature"
}

// TestParseTokenClaimsReadsWhatKubernetesWrites covers the shape Kubernetes actually
// writes: aud as an array, and a sub naming the ServiceAccount. This is the
// string the federation policy in Databricks has to match.
func TestParseTokenClaimsReadsWhatKubernetesWrites(t *testing.T) {
	t.Parallel()
	const subject = "system:serviceaccount:probe:budget-puller"
	const issuer = "https://oidc.eks.ap-northeast-1.amazonaws.com/id/4A36"

	got, err := parseTokenClaims(jwtWith(
		`{"iss":"` + issuer + `","sub":"` + subject + `","aud":["databricks"],"exp":1788098161}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Subject != subject {
		t.Errorf("subject is %q, want %q", got.Subject, subject)
	}
	if got.Issuer != issuer {
		t.Errorf("issuer is %q, want %q", got.Issuer, issuer)
	}
	if want := []string{"databricks"}; !slices.Equal(got.Audience, want) {
		t.Errorf("audience is %v, want %v", got.Audience, want)
	}
}

// TestParseTokenClaimsAcceptsASingleAudience covers the other form RFC 7519 allows.
// Kubernetes writes the array, but a token from elsewhere must not read as
// malformed just for being spelled the legal other way.
func TestParseTokenClaimsAcceptsASingleAudience(t *testing.T) {
	t.Parallel()
	got, err := parseTokenClaims(jwtWith(`{"sub":"someone","aud":"databricks"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []string{"databricks"}; !slices.Equal(got.Audience, want) {
		t.Errorf("audience is %v, want %v", got.Audience, want)
	}
}

// TestParseTokenClaimsRejectsWhatIsNotAToken covers a mounted file that is not a
// JWT at all. Returning zero claims silently would report an empty subject,
// which reads as "the operator presents nothing" rather than "the file is
// wrong" -- and somebody would go looking at their federation policy.
func TestParseTokenClaimsRejectsWhatIsNotAToken(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, token string }{
		{"not a jwt", "hello"},
		// A payload that would parse cleanly, so only the segment count can
		// reject this. With "header.payload" the JSON parse failed first and
		// the count check was never reached.
		{"two segments", "header." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"someone"}`))},
		{"four segments", jwtWith(`{"sub":"someone"}`) + ".extra"},
		{"payload is not base64url", "header.!!!.signature"},
		{"payload is not json", jwtWith("not json")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseTokenClaims(tc.token); err == nil {
				t.Error("want an error, got nil")
			}
		})
	}
}

// TestReadTokenClaimsNamesTheFile covers the token not being there at all, which is
// what a Deployment missing its projected volume looks like. The path has to be
// in the message: without it the reader is told a file is missing but not which.
func TestReadTokenClaimsNamesTheFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "token")
	_, err := ReadTokenClaims(path)
	if err == nil {
		t.Fatal("want an error for a token that is not there")
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("error is %q, want it to name %q", err, path)
	}

	if _, err := ReadTokenClaims(""); err == nil {
		t.Error("want an error when no token file is configured at all")
	}
}
