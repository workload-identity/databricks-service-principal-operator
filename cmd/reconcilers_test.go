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
// losing Records has the records of what was issued written to one namespace and
// looked for in another, and losing DatabricksAccountNamespacedName makes the
// operator act in whichever DatabricksAccount somebody created.
//
// So this asserts the one thing neither end can: that each value arrives.
func TestEverySettingReachesAReconciler(t *testing.T) {
	t.Parallel()
	want := settings{
		Namespace:                       "operators",
		DatabricksAccountNamespacedName: types.NamespacedName{Namespace: "operators", Name: "the-account"},
		Holder:                          databricks.NewHolder("operators", "the-account"),
		Runtime: databricks.Config{
			OIDCTokenFilepath: "/var/run/secrets/databricks/token",
			TokenAudience:     "databricks",
		},
	}

	account, projection, issued := reconcilers(want, nil, nil, nil)

	if projection.Records != want.Namespace {
		t.Errorf("Records is %q, want %q -- the records of what was issued would be written "+
			"to, and looked for in, different namespaces", projection.Records, want.Namespace)
	}
	wanted := want.DatabricksAccountNamespacedName
	if issued.DatabricksAccountNamespacedName != wanted || account.DatabricksAccountNamespacedName != wanted {
		t.Errorf("DatabricksAccountNamespacedName is %v and %v, want %v -- the operator would "+
			"act in whichever DatabricksAccount somebody created",
			issued.DatabricksAccountNamespacedName, account.DatabricksAccountNamespacedName, wanted)
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
