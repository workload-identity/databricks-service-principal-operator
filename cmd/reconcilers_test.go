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
		Holder:                          databricks.NewHolder("operators", "the-account"),
		Runtime: databricks.Config{
			OIDCTokenFilepath: "/var/run/secrets/databricks/token",
			TokenAudience:     "databricks",
		},
	}

	account, databricksServiceAccountReconciler, issued := reconcilers(want, nil, nil, nil)

	wanted := want.DatabricksAccountNamespacedName
	if issued.DatabricksAccountNamespacedName != wanted ||
		account.DatabricksAccountNamespacedName != wanted ||
		databricksServiceAccountReconciler.DatabricksAccountNamespacedName != wanted {
		t.Errorf("DatabricksAccountNamespacedName is %v, %v and %v, want %v -- the operator would "+
			"act in whichever DatabricksAccount somebody created",
			issued.DatabricksAccountNamespacedName, account.DatabricksAccountNamespacedName,
			databricksServiceAccountReconciler.DatabricksAccountNamespacedName, wanted)
	}
	if issued.Databricks != want.Holder || account.Holder != want.Holder {
		t.Error("the two controllers were not given the same Holder; the account controller " +
			"is the only writer and the record controller reads what it wrote")
	}
	if account.Runtime != want.Runtime {
		t.Errorf("Runtime is %+v, want %+v -- the operator would look for its token somewhere "+
			"other than where the Deployment mounts it", account.Runtime, want.Runtime)
	}
	if issued.TokenPath != want.Runtime.OIDCTokenFilepath {
		t.Errorf("TokenPath is %q, want %q -- the issuer every federation policy names and the "+
			"audience every pod is given are read from that file",
			issued.TokenPath, want.Runtime.OIDCTokenFilepath)
	}
}
