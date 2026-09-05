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
	"reflect"
	"testing"
)

const (
	awsAccountHost     = "https://accounts.cloud.databricks.com"
	testAccountID      = "00000000-0000-0000-0000-000000000000"
	unifiedAccountHost = "https://example.databricks.com"
	deploymentName     = "dbc-XXXXXXXX-YYYY"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	workloadIdentity := Config{
		AccountHost:       awsAccountHost,
		AccountID:         testAccountID,
		ClientID:          "11111111-1111-1111-1111-111111111111",
		OIDCTokenFilepath: "/var/run/secrets/databricks/token",
	}

	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{{
		name: "workload identity",
		cfg:  workloadIdentity,
	}, {
		name: "sdk resolution",
		cfg: Config{
			AccountHost: awsAccountHost,
			AccountID:   testAccountID,
		},
	}, {
		// A unified host must not be rejected for not looking like an account
		// console.
		name: "unified host",
		cfg: func() Config {
			c := workloadIdentity
			c.AccountHost = unifiedAccountHost
			return c
		}(),
	}, {
		name: "token file without client id",
		cfg: func() Config {
			c := workloadIdentity
			c.ClientID = ""
			return c
		}(),
		wantErr: true,
	}, {
		name: "no account id",
		cfg: func() Config {
			c := workloadIdentity
			c.AccountID = ""
			return c
		}(),
		wantErr: true,
	}, {
		name: "no account host",
		cfg: func() Config {
			c := workloadIdentity
			c.AccountHost = ""
			return c
		}(),
		wantErr: true,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr && err == nil {
				t.Fatal("want error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestRuntimeForAccount covers the two halves coming together: the Deployment
// decides where the operator's own token is, a DatabricksAccount decides which
// account it acts in, and neither half is complete on its own.
func TestRuntimeForAccount(t *testing.T) {
	t.Setenv(EnvOIDCTokenFilepath, "/var/run/secrets/databricks/token")
	t.Setenv(EnvTokenAudience, "databricks")

	runtime := RuntimeFromEnv()
	if err := runtime.Validate(); err == nil {
		t.Fatal("the Deployment half alone is not a usable config; Validate should say so")
	}

	cfg := runtime.ForAccount(awsAccountHost, testAccountID, "11111111-1111-1111-1111-111111111111")
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ForAccount must not disturb what the Deployment decided: the audience has
	// to keep matching the projected volume, which no account can see.
	if cfg.OIDCTokenFilepath != runtime.OIDCTokenFilepath || cfg.TokenAudience != runtime.TokenAudience {
		t.Errorf("ForAccount changed the Deployment's half: %+v", cfg)
	}
	if !cfg.UsesWorkloadIdentity() {
		t.Fatal("a token file is set, so this should be the workload identity path")
	}

	sdk := cfg.SDKConfig()
	if sdk.AuthType != "file-oidc" {
		t.Errorf("auth type is %q, want file-oidc", sdk.AuthType)
	}
	if sdk.ClientID != cfg.ClientID {
		t.Errorf("client id is %q, want %q", sdk.ClientID, cfg.ClientID)
	}
	if sdk.OIDCTokenFilepath != cfg.OIDCTokenFilepath {
		t.Errorf("token file is %q, want %q", sdk.OIDCTokenFilepath, cfg.OIDCTokenFilepath)
	}
	if sdk.AccountID != cfg.AccountID {
		t.Errorf("account id is %q, want %q", sdk.AccountID, cfg.AccountID)
	}
}

// TestRuntimeWithoutTokenFile covers running outside a cluster, where there is
// no projected token and the SDK resolves authentication its own way.
func TestRuntimeWithoutTokenFile(t *testing.T) {
	t.Setenv(EnvOIDCTokenFilepath, "")
	t.Setenv(EnvTokenAudience, "")

	cfg := RuntimeFromEnv().ForAccount(awsAccountHost, testAccountID, "")
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.UsesWorkloadIdentity() {
		t.Fatal("no token file is set, so this should not be the workload identity path")
	}
	if got := cfg.SDKConfig().AuthType; got != "" {
		t.Errorf("auth type is %q, want it left empty for the SDK to resolve", got)
	}
}

// TestConfigIsOnlyWhatWasDeclared covers a constraint on this struct that
// nothing else can express.
//
// The account controller compares the whole of a Config against the one the
// installed clients were built from, and treats any difference as "the
// declaration changed, so what is installed is no longer an answer to anything".
// That is only true while every field here comes from the DatabricksAccount or
// the environment the operator started with -- values that change when somebody
// changes them and at no other time.
//
// A field that is computed, cached, or carries state would make that comparison
// true for reasons nobody declared, and the symptom is an operator that
// withdraws its own clients every minute and reports NotConfigured on every
// identity in the cluster. Nobody would trace that back to a struct field.
//
// So this fails when a field is added, which is the only moment at which reading
// the paragraph above is any use.
func TestConfigIsOnlyWhatWasDeclared(t *testing.T) {
	t.Parallel()
	declared := map[string]bool{
		"AccountHost":       true,
		"AccountID":         true,
		"ClientID":          true,
		"OIDCTokenFilepath": true,
		"TokenAudience":     true,
	}

	got := map[string]bool{}
	for field := range reflect.TypeFor[Config]().Fields() {
		got[field.Name] = true
	}

	for name := range got {
		if !declared[name] {
			t.Errorf("Config has a field %s that this test does not know about. It is compared "+
				"whole against the declaration the installed clients were built from, and any "+
				"difference withdraws them -- so a field that changes for any reason other than "+
				"somebody editing the DatabricksAccount makes the operator take its own clients "+
				"away, over and over, reporting NotConfigured on every identity in the cluster. "+
				"If %s is declared, add it here.", name, name)
		}
	}
	for name := range declared {
		if !got[name] {
			t.Errorf("Config no longer has %s; this test is out of date and the comparison it "+
				"guards may no longer mean what it says", name)
		}
	}
}
