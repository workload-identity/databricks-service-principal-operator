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

package main

import (
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

// TestEverySettingReachesAReconciler covers the stretch between a flag being
// parsed and a controller reading it.
//
// Both ends of that stretch are already tested and neither covers it. The
// reconcilers' own tests set their fields directly, so they pass whatever
// assembly does; the rendered-manifest test checks only that a flag is still in
// the container's args, so it passes whatever the binary does with it. An
// assignment dropped between the two is silent, and what it breaks is specific:
// losing DatabricksAccountNamespacedName makes the operator act in whichever
// DatabricksAccount somebody created, and write the records of what was issued
// where nothing looks for them.
//
// So this asserts the one thing neither end can: that each value arrives.
func TestEverySettingReachesAReconciler(t *testing.T) {
	t.Parallel()
	want := settings{
		DatabricksAccountNamespacedName: types.NamespacedName{Namespace: "operators", Name: "the-account"},
		AccountInUse:                    databricks.NewAccountInUse("operators", "the-account"),
		OwnToken: databricks.Config{
			OIDCTokenFilepath: "/var/run/secrets/databricks/token",
			TokenAudience:     "databricks",
		},
	}

	databricksAccountReconciler, databricksServiceAccountReconciler, issued := reconcilers(want, nil, nil, nil)

	wanted := want.DatabricksAccountNamespacedName
	if issued.DatabricksAccountNamespacedName != wanted ||
		databricksAccountReconciler.DatabricksAccountNamespacedName != wanted ||
		databricksServiceAccountReconciler.DatabricksAccountNamespacedName != wanted {
		t.Errorf("DatabricksAccountNamespacedName is %v, %v and %v, want %v -- the operator would "+
			"act in whichever DatabricksAccount somebody created",
			issued.DatabricksAccountNamespacedName, databricksAccountReconciler.DatabricksAccountNamespacedName,
			databricksServiceAccountReconciler.DatabricksAccountNamespacedName, wanted)
	}
	if issued.Databricks != want.AccountInUse || databricksAccountReconciler.AccountInUse != want.AccountInUse {
		t.Error("the two controllers were not given the same AccountInUse; the account controller " +
			"is the only writer and the record controller reads what it wrote")
	}
	if databricksAccountReconciler.OwnToken != want.OwnToken {
		t.Errorf("OwnToken is %+v, want %+v -- the operator would look for its token somewhere "+
			"other than where the Deployment mounts it", databricksAccountReconciler.OwnToken, want.OwnToken)
	}
	if issued.TokenPath != want.OwnToken.OIDCTokenFilepath {
		t.Errorf("TokenPath is %q, want %q -- the issuer every federation policy names and the "+
			"audience every pod is given are read from that file",
			issued.TokenPath, want.OwnToken.OIDCTokenFilepath)
	}
}
