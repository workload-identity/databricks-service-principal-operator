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
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// TokenClaims is the iss, sub and aud claims the operator presents when it
// exchanges its token.
//
// The federation policy in Databricks has to name exactly these values. Getting
// one wrong produces a refusal that quotes the policy's own description back,
// which reads as though the policy were at fault.
type TokenClaims struct {
	Issuer   string
	Subject  string
	Audience []string
}

// ReadTokenClaims reads the operator's own claims out of its projected token.
//
// It reads them rather than assembling them. The subject could be built from the
// namespace and the ServiceAccount name, and the audience is written in the
// Deployment beside the volume -- but a value reconstructed from the inputs
// cannot disagree with itself, so it would report what the operator believes
// instead of what it holds. Reading the file reports what will actually be
// presented, which is the thing the federation policy has to match.
//
// The signature is not checked, and there is nothing here to check it against:
// these are the operator's own claims, read to be displayed. Databricks verifies
// them against the cluster's public keys, which is where that check belongs.
func ReadTokenClaims(path string) (TokenClaims, error) {
	if path == "" {
		return TokenClaims{}, fmt.Errorf("no projected token file is configured")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return TokenClaims{}, fmt.Errorf("reading the projected token at %s: %w", path, err)
	}
	return parseTokenClaims(string(raw))
}

func parseTokenClaims(token string) (TokenClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return TokenClaims{}, fmt.Errorf("the projected token is not a JWT: %d segments, want 3", len(parts))
	}
	// Projected tokens are compact-serialized, so the payload is base64url with
	// the padding stripped.
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return TokenClaims{}, fmt.Errorf("decoding the projected token's payload: %w", err)
	}

	// aud is "a StringOrURI value or an array of them" in RFC 7519, and
	// Kubernetes writes the array form. Both are accepted here so that a token
	// from somewhere else does not read as malformed.
	var raw struct {
		Issuer   string          `json:"iss"`
		Subject  string          `json:"sub"`
		Audience json.RawMessage `json:"aud"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return TokenClaims{}, fmt.Errorf("parsing the projected token's payload: %w", err)
	}

	claims := TokenClaims{Issuer: raw.Issuer, Subject: raw.Subject}
	if len(raw.Audience) > 0 {
		var many []string
		if err := json.Unmarshal(raw.Audience, &many); err == nil {
			claims.Audience = many
		} else {
			var one string
			if err := json.Unmarshal(raw.Audience, &one); err != nil {
				return TokenClaims{}, fmt.Errorf("the projected token's aud claim is neither a string nor an array")
			}
			claims.Audience = []string{one}
		}
	}
	return claims, nil
}
