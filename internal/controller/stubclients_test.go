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
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/databricks/databricks-sdk-go"

	dbx "github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

// stubClients answers from fields and records what it was asked, so a test can
// say what the operator should have asked Databricks for without a stub having
// to model Databricks.
type stubClients struct {
	// What the operator asked Databricks for, in the order the calls were made.
	// That is the operator's call order and not an order anybody wrote: the
	// request is a set of annotation keys, and a map has none.
	created  []string
	deleted  []string
	policies []string

	// accountID is the Databricks account this stub acts in.
	accountID string

	// What a lookup finds, and what was looked for.
	foundID       string
	foundClientID string
	looked        []string

	// err fails every call.
	err error

	// deleteErr fails only DeleteServicePrincipal, so a test can see what an
	// object on its way out does when the one call it owes does not land.
	deleteErr error

	// policyErr fails only EnsureFederationPolicy, so a test can arrange the one
	// order that matters: a service principal made, and something after it not.
	policyErr error

	// removeErr fails only RemoveFederationPolicies, so a test can see what a
	// removal that did not land says about itself, and that it does not say
	// it is finished.
	removeErr error

	// policiesRemoved is every service principal this operator asked to have the
	// trust taken off, in the order it asked.
	policiesRemoved []string

	// policyPanics and createPanics stand in for the process dying inside that
	// call. Nothing after it runs -- which is the only way "written down before
	// the next call" differs from "written down by the end of the pass".
	policyPanics bool
	createPanics bool

	// gone is an id Databricks no longer has, so a test can see what happens
	// when what is recorded is not there any more.
	gone string

	// What the next create answers with.
	newServicePrincipalID string
	newClientID           string
}

func (s *stubClients) AccountClient() *databricks.AccountClient { panic("not used") }

// Snapshot returns this stub. What the AccountInUse does here is the thing
// being stood in for.
func (s *stubClients) Snapshot() dbx.Clients { return s }

// accountID is where this stub pretends to be acting. Empty means the stub was
// not told, and testAccountID is what it answers then -- a configured operator
// always knows which account it is in, and tests that leave this out are not
// about that.
func (s *stubClients) AccountID() string {
	if s.accountID == "" {
		return testAccountID
	}
	return s.accountID
}

// found is what the next lookup answers with: an id and applicationId to adopt,
// or empty for nothing there.
func (s *stubClients) FindServicePrincipal(_ context.Context, issuing dbx.Issuing) (
	string, string, bool, error) {
	if s.err != nil {
		return "", "", false, s.err
	}
	s.looked = append(s.looked, issuing.Namespace+"/"+issuing.Name)
	if s.foundID == "" {
		return "", "", false, nil
	}
	return s.foundID, s.foundClientID, true, nil
}

func (s *stubClients) CreateServicePrincipal(_ context.Context, issuing dbx.Issuing) (string, string, error) {
	if s.err != nil {
		return "", "", s.err
	}
	if s.createPanics {
		// Recorded first. A service principal made by a call the operator never
		// saw return is exactly what the mark exists for, so the stub has to
		// leave the same evidence behind that Databricks would.
		s.created = append(s.created, issuing.Namespace+"/"+issuing.Name)
		panic("the operator stopped here")
	}
	// Recorded per identity rather than per ServiceAccount, so a failure names
	// which one. Two identities of one ServiceAccount are two creates, and
	// "created [team-a/etl team-a/etl]" would say nothing about which.
	asked := issuing.Namespace + "/" + issuing.Name
	if issuing.Identity != "" {
		asked += "." + issuing.Identity
	}
	s.created = append(s.created, asked)

	if s.newServicePrincipalID != "" {
		return s.newServicePrincipalID, s.newClientID, nil
	}
	// The first is 7788 because that is the id every single-identity test in
	// this package reads. The ones after it differ, because two identities
	// sharing an id would let a test pass that had only made one.
	if len(s.created) == 1 {
		return "7788", "app-uuid", nil
	}
	return fmt.Sprintf("7788%d", len(s.created)), fmt.Sprintf("app-uuid-%d", len(s.created)), nil
}

func (s *stubClients) ServicePrincipalExists(_ context.Context, servicePrincipalID string) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	if s.gone == servicePrincipalID {
		return false, nil
	}
	return true, nil
}

func (s *stubClients) DeleteServicePrincipal(_ context.Context, servicePrincipalID string) error {
	if s.err != nil {
		return s.err
	}
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.deleted = append(s.deleted, servicePrincipalID)
	return nil
}

func (s *stubClients) EnsureFederationPolicy(_ context.Context, servicePrincipalID, issuer, subject, audience string) error {
	if s.err != nil {
		return s.err
	}
	if s.policyErr != nil {
		return s.policyErr
	}
	if s.policyPanics {
		panic("the operator stopped here")
	}
	// Recorded once however often it is asked for. The real call lists the
	// policies on that service principal and adds nothing when the subject is
	// already trusted, so a stub that counted repeats would have tests asserting
	// against a behaviour Databricks does not have.
	policy := servicePrincipalID + " " + issuer + " " + subject + " " + audience
	if !slices.Contains(s.policies, policy) {
		s.policies = append(s.policies, policy)
	}
	return nil
}

// RemoveFederationPolicies drops what EnsureFederationPolicy recorded, matched
// the way the real one matches: on the service principal, the issuer and the
// subject, and not on the audience -- a policy naming an audience this operator
// no longer hands out is still one a token minted for it satisfies.
func (s *stubClients) RemoveFederationPolicies(_ context.Context, servicePrincipalID, issuer, subject string) error {
	if s.err != nil {
		return s.err
	}
	if s.removeErr != nil {
		return s.removeErr
	}
	s.policiesRemoved = append(s.policiesRemoved, servicePrincipalID)
	prefix := servicePrincipalID + " " + issuer + " " + subject + " "
	s.policies = slices.DeleteFunc(s.policies, func(policy string) bool {
		return strings.HasPrefix(policy, prefix)
	})
	return nil
}

var _ dbx.Clients = (*stubClients)(nil)
