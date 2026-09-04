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
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// The condition types and reasons, spelled out a second time.
//
// Not a check that a constant equals itself. These strings leave the cluster:
// they are what a jsonpath selects on, what an alert matches, and what is
// already written into the status of every object this operator has ever
// touched. A condition type is never garbage collected off an existing object,
// so renaming one leaves the old type sitting there for ever alongside the new;
// renaming a reason silently stops somebody else's selector matching, with
// nothing failing anywhere to say so.
//
// What this makes impossible is the accidental edit -- a rename that swept a
// string literal along with the identifier, a typo fixed in passing. What it
// makes visible is the deliberate one: it cannot be made without editing this
// table too, and that edit is a line in a review that says the API changed.
var conditionTypes = map[string]string{
	"conditionReady":                     "Ready",
	"conditionEquipped":                  "Equipped",
	"conditionPrepared":                  "Prepared",
	"conditionAccountsAgree":             "AccountsAgree",
	"conditionFederationPoliciesRemoved": "FederationPoliciesRemoved",
}

var conditionReasons = map[string]string{
	"reasonNotFound":                 "NotFound",
	"reasonInvalidSpec":              "InvalidSpec",
	"reasonDenied":                   "Denied",
	"reasonNotConfigured":            "NotConfigured",
	"reasonDatabricksUnavailable":    "DatabricksUnavailable",
	"reasonRejected":                 "Rejected",
	"reasonMalformedRecord":          "MalformedRecord",
	"reasonPrepared":                 "Prepared",
	"reasonUnprepared":               "TokenUnreadable",
	"reasonAccountReady":             "AccountReady",
	"reasonNotSelected":              "NotSelected",
	"reasonExchangeable":             "Exchangeable",
	"reasonRemovedInDatabricks":      "RemovedInDatabricks",
	"reasonNotEquipped":              "NotEquipped",
	"reasonEquipped":                 "Equipped",
	"reasonElsewhere":                "IdentitiesElsewhere",
	"reasonHere":                     "AllHere",
	"reasonAccountMismatch":          "AccountMismatch",
	"reasonAccountUnknown":           "AccountUnknown",
	"reasonDeleteFailed":             "DeleteFailed",
	"reasonNotServed":                "NotServed",
	"reasonMintingNotSuspended":      "MintingNotSuspended",
	"reasonAnotherRemoval":           "AnotherRemoval",
	"reasonRemovePoliciesFailed":     "RemovePoliciesFailed",
	"reasonPoliciesRemoved":          "PoliciesRemoved",
	"reasonRemovingPolicies":         "RemovingPolicies",
	"reasonAwaitingServicePrincipal": "AwaitingServicePrincipal",
}

// TestEveryConditionTypeAndReasonIsTheStringItWas holds the published spelling
// of all 32 constants against an accidental edit.
func TestEveryConditionTypeAndReasonIsTheStringItWas(t *testing.T) {
	t.Parallel()
	declared := conditionConstantsIn(t, "conditions.go")

	for name, want := range conditionTypes {
		if got, ok := declared[name]; !ok || got != want {
			t.Errorf("%s is %q, want %q -- a renamed condition type is never garbage collected "+
				"off the objects that already carry it, so both spellings live there for ever",
				name, got, want)
		}
	}
	for name, want := range conditionReasons {
		if got, ok := declared[name]; !ok || got != want {
			t.Errorf("%s is %q, want %q -- a renamed reason stops somebody's selector matching "+
				"and nothing anywhere fails to say so", name, got, want)
		}
	}
}

// TestEveryConditionTypeAndReasonIsInTheTable is what keeps the table above
// from quietly becoming a subset.
//
// A table checked only in the direction of what it lists is one a new reason
// never joins: the constant is added, nothing mentions it here, and everything
// stays green. So conditions.go is read back and the two sets are compared both
// ways, which makes adding a constant without publishing its string a failure
// rather than an omission.
func TestEveryConditionTypeAndReasonIsInTheTable(t *testing.T) {
	t.Parallel()
	declared := conditionConstantsIn(t, "conditions.go")

	if len(conditionTypes) != 5 || len(conditionReasons) != 27 {
		t.Errorf("the table holds %d condition types and %d reasons, want 5 and 27",
			len(conditionTypes), len(conditionReasons))
	}
	if len(declared) != len(conditionTypes)+len(conditionReasons) {
		t.Errorf("conditions.go declares %d of these constants and the table holds %d",
			len(declared), len(conditionTypes)+len(conditionReasons))
	}
	for name := range declared {
		_, inTypes := conditionTypes[name]
		_, inReasons := conditionReasons[name]
		if !inTypes && !inReasons {
			t.Errorf("%s is declared in conditions.go and named nowhere in this file; its "+
				"string is API surface and nothing is holding it", name)
		}
	}
}

// conditionConstantsIn reads the condition type and reason constants out of the
// source, by name and value.
//
// Read from the file rather than referred to, because a Go test cannot ask what
// constants a package declares and the whole point of the check above is to
// notice one that nothing refers to.
func conditionConstantsIn(t *testing.T, path string) map[string]string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	found := map[string]string{}
	for _, decl := range file.Decls {
		general, is := decl.(*ast.GenDecl)
		if !is || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value, is := spec.(*ast.ValueSpec)
			if !is {
				continue
			}
			for i, name := range value.Names {
				if !strings.HasPrefix(name.Name, "condition") && !strings.HasPrefix(name.Name, "reason") {
					continue
				}
				literal, is := value.Values[i].(*ast.BasicLit)
				if !is || literal.Kind != token.STRING {
					t.Fatalf("%s is not a string literal; this file is where the published "+
						"spelling lives and it has to be readable as one", name.Name)
				}
				unquoted, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatal(err)
				}
				found[name.Name] = unquoted
			}
		}
	}
	return found
}
