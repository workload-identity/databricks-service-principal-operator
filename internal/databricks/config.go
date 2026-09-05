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
	"fmt"
	"os"
	"strings"

	"github.com/databricks/databricks-sdk-go"
)

// Environment variable names, matching the SDK's own.
//
// Only these two are read. The account's coordinates arrive in a
// DatabricksAccount, not in the environment: these two are what the operator
// decides about itself -- where its own token is mounted, and the audience that
// token carries -- and both have to agree with the projected volume declared
// beside them in the Deployment, which nothing could check if they lived in an
// object written elsewhere.
const (
	EnvOIDCTokenFilepath = "DATABRICKS_OIDC_TOKEN_FILEPATH"
	EnvTokenAudience     = "DATABRICKS_TOKEN_AUDIENCE"
)

// Config is what the operator needs in order to reach Databricks as itself.
//
// It comes from two places, and the split is the point. The three account
// coordinates are read from a DatabricksAccount and can change while the
// operator runs; the two below them come from the Deployment and cannot.
type Config struct {
	// AccountHost is the account console host, e.g.
	// https://accounts.cloud.databricks.com. On unified deployments the account
	// and its workspaces share one host, so it is not always an "accounts." name.
	AccountHost string

	// AccountID identifies the Databricks account.
	AccountID string

	// ClientID is the applicationId of the service principal the operator acts
	// as -- not its numeric id and not its display name.
	//
	// Token exchange without it fails as TOKEN_INVALID with the federation
	// policy echoed back, which reads as though the policy were wrong.
	ClientID string

	// OIDCTokenFilepath is where the operator's projected ServiceAccount token
	// is mounted. Setting it selects the file-oidc auth type; leaving it empty
	// hands authentication to the SDK's own resolution, for running outside a
	// cluster.
	OIDCTokenFilepath string

	// TokenAudience must equal the audience of the operator's federation
	// policy. Empty means the SDK asks the host for its default.
	TokenAudience string
}

// RuntimeFromEnv reads the half of the config the Deployment decides. It cannot
// fail: what it reads is either set or not, and Validate is what judges the
// whole once an account has supplied the rest.
func RuntimeFromEnv() Config {
	return Config{
		OIDCTokenFilepath: strings.TrimSpace(os.Getenv(EnvOIDCTokenFilepath)),
		TokenAudience:     strings.TrimSpace(os.Getenv(EnvTokenAudience)),
	}
}

// ForAccount returns a copy carrying an account's coordinates. The receiver
// keeps what the Deployment decided.
func (c Config) ForAccount(host, accountID, clientID string) Config {
	c.AccountHost = host
	c.AccountID = accountID
	c.ClientID = clientID
	return c
}

// Validate reports what is missing.
//
// The CRD already requires all three account values, so reaching a failure here
// means something bypassed it. It stays because the message is what lands in the
// account's condition, and because Config is also built in tests.
func (c Config) Validate() error {
	if c.AccountHost == "" {
		return fmt.Errorf("the account has no host")
	}
	if c.AccountID == "" {
		return fmt.Errorf("the account has no accountId")
	}
	if c.OIDCTokenFilepath != "" && c.ClientID == "" {
		return fmt.Errorf("the operator has a projected token but the account has no clientId: token exchange without it fails as TOKEN_INVALID with a message that blames the federation policy instead")
	}
	return nil
}

// UsesWorkloadIdentity reports whether the operator authenticates by exchanging
// a projected ServiceAccount token.
func (c Config) UsesWorkloadIdentity() bool {
	return c.OIDCTokenFilepath != ""
}

// SDKConfig builds the account-level SDK config, which is the only kind there
// is here: every call this package makes is account-level, and there is no
// workspace client to derive. See the note on clients.
func (c Config) SDKConfig() *databricks.Config {
	cfg := &databricks.Config{
		Host:      c.AccountHost,
		AccountID: c.AccountID,
	}
	if !c.UsesWorkloadIdentity() {
		return cfg
	}
	cfg.AuthType = "file-oidc"
	cfg.ClientID = c.ClientID
	cfg.OIDCTokenFilepath = c.OIDCTokenFilepath
	cfg.TokenAudience = c.TokenAudience
	return cfg
}
