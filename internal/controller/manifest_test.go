/*
Copyright 2026.

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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	"github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
	dbxwebhook "github.com/workload-identity/databricks-service-principal-operator/internal/webhook"
)

// managerContainer returns the manager container as the shipped Deployment
// declares it. The Deployment is the second document in manager.yaml; the first
// is the Namespace.
func managerContainer(t *testing.T) (corev1.Container, corev1.PodSpec) {
	t.Helper()
	path := filepath.Join("..", "..", "config", "manager", "manager.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var deployment appsv1.Deployment
	for doc := range strings.SplitSeq(string(data), "\n---\n") {
		var probe struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal([]byte(doc), &probe); err != nil || probe.Kind != "Deployment" {
			continue
		}
		if err := yaml.Unmarshal([]byte(doc), &deployment); err != nil {
			t.Fatalf("parsing the Deployment in %s: %v", path, err)
		}
	}
	spec := deployment.Spec.Template.Spec
	for _, c := range spec.Containers {
		if c.Name == "manager" {
			return c, spec
		}
	}
	t.Fatalf("no container named manager in %s", path)
	return corev1.Container{}, corev1.PodSpec{}
}

// TestManagerStartsWithWhatTheManifestSupplies covers what the Deployment has to
// carry for the operator to be able to do anything at all: where its own token
// is, what audience that token has, and which namespace to look in.
//
// Nothing else belongs here. The account's coordinates are a DatabricksAccount,
// so that they can change without editing a Deployment and so that the operator
// can answer back on them.
func TestManagerStartsWithWhatTheManifestSupplies(t *testing.T) {
	t.Parallel()
	container, _ := managerContainer(t)

	literal := map[string]string{}
	for _, e := range container.Env {
		literal[e.Name] = e.Value
	}

	// Set in the manifest, because the operator itself decides them: where it
	// mounts its own token, and the audience that token must carry.
	for _, name := range []string{databricks.EnvOIDCTokenFilepath, databricks.EnvTokenAudience} {
		if literal[name] == "" {
			t.Errorf("%s is not set in the manager Deployment", name)
		}
	}

	// The token's audience and the audience the SDK asks for are the same value
	// in two places. Drift between them fails token exchange with an error that
	// blames the federation policy.
	_, spec := managerContainer(t)
	for _, v := range spec.Volumes {
		if v.Projected == nil {
			continue
		}
		for i := range v.Projected.Sources {
			s := v.Projected.Sources[i].ServiceAccountToken
			if s == nil {
				continue
			}
			if s.Audience != literal[databricks.EnvTokenAudience] {
				t.Errorf("the projected token's audience is %q but %s is %q; token exchange will fail "+
					"with an error that reads as though the federation policy were wrong",
					s.Audience, databricks.EnvTokenAudience, literal[databricks.EnvTokenAudience])
			}
		}
	}

	// The mount has to reach where the operator is told to look.
	var mounted bool
	for _, m := range container.VolumeMounts {
		if strings.HasPrefix(literal[databricks.EnvOIDCTokenFilepath], m.MountPath) {
			mounted = true
		}
	}
	if !mounted {
		t.Errorf("%s is %q, which no volumeMount covers: the file will not be there",
			databricks.EnvOIDCTokenFilepath, literal[databricks.EnvOIDCTokenFilepath])
	}

	// The operator's own namespace, which is where it looks for its
	// DatabricksAccount. It has to come from the downward API: a literal would
	// be this repository's namespace written into a manifest anyone may rename,
	// and it would be wrong the moment they did.
	var fromDownwardAPI bool
	for _, e := range container.Env {
		if e.Name != EnvPodNamespace {
			continue
		}
		if e.Value != "" {
			t.Errorf("%s is a literal %q; it must come from the downward API, "+
				"or renaming the operator namespace silently points the operator at the wrong one",
				EnvPodNamespace, e.Value)
		}
		if e.ValueFrom != nil && e.ValueFrom.FieldRef != nil &&
			e.ValueFrom.FieldRef.FieldPath == "metadata.namespace" {
			fromDownwardAPI = true
		}
	}
	if !fromDownwardAPI {
		t.Errorf("%s does not come from metadata.namespace: the operator cannot find its DatabricksAccount",
			EnvPodNamespace)
	}

	// The account's coordinates are a DatabricksAccount, not environment. An
	// env or an envFrom carrying them would be a second source of truth, and
	// the one the operator does not read.
	for _, e := range container.Env {
		if strings.HasPrefix(e.Name, "DATABRICKS_") &&
			e.Name != databricks.EnvOIDCTokenFilepath && e.Name != databricks.EnvTokenAudience {
			t.Errorf("%s is set in the manager Deployment; the account's coordinates belong in a "+
				"DatabricksAccount, which is what the operator reads", e.Name)
		}
	}
	if len(container.EnvFrom) > 0 {
		t.Error("the manager Deployment reads envFrom; the account's coordinates belong in a DatabricksAccount")
	}
}

// TestWhatThisOperatorMayReachIsBoundedByItsNamespace covers the permission that
// makes two operators separate, which is the only thing that does.
//
// The credential each operator holds is an account admin -- the floor Databricks
// sets for writing a federation policy on a service principal -- so an operator
// is the trust boundary. Two kinds belong inside it: the DatabricksAccount that
// says which account to act in, and the records of what was issued. Cluster-wide
// permission on either lets one operator read and write another's, and it did
// exactly that: both stamped Ready on the other's account object, each undoing
// the other, forever, waking every identity in the cluster on every flip.
//
// A predicate in the code could have stopped that reconcile. Only the permission
// stops the next one somebody writes.
func TestWhatThisOperatorMayReachIsBoundedByItsNamespace(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "config", "rbac", "role.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	type rule struct {
		Resources []string `json:"resources"`
	}
	var cluster, namespaced []string
	for doc := range strings.SplitSeq(string(data), "\n---\n") {
		var role struct {
			Kind  string `json:"kind"`
			Rules []rule `json:"rules"`
		}
		if err := yaml.Unmarshal([]byte(doc), &role); err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		for _, r := range role.Rules {
			switch role.Kind {
			case "ClusterRole":
				cluster = append(cluster, r.Resources...)
			case "Role":
				namespaced = append(namespaced, r.Resources...)
			}
		}
	}

	// Inside the boundary. Cluster-wide on either of these is the whole bug.
	for _, inside := range []string{"databricksaccounts", "issueddatabricksserviceprincipals"} {
		if slices.Contains(cluster, inside) {
			t.Errorf("the ClusterRole grants %s across the cluster; one operator can then read "+
				"and write another operator's, and both hold an account admin credential", inside)
		}
		if !slices.Contains(namespaced, inside) {
			t.Errorf("nothing in the namespaced Role grants %s; the operator cannot reach its "+
				"own", inside)
		}
	}

	// Outside it, and genuinely so: these live in namespaces this operator does
	// not own and cannot be narrowed.
	for _, outside := range []string{"serviceaccounts", "namespaces", "pods", "databricksserviceprincipals"} {
		if !slices.Contains(cluster, outside) {
			t.Errorf("the ClusterRole no longer grants %s; it is read or written in every "+
				"namespace this operator serves, so it cannot be narrowed to one", outside)
		}
	}
}

// TestTheWebhookIsAskedAgainWhenThePodChanges covers a default that leaves
// containers unequipped in a pod that reports as equipped.
//
// reinvocationPolicy defaults to Never, so a container added by a webhook that
// runs after this one -- a mesh sidecar, a policy engine -- gets no token, no
// mount and no variables. The pod-level volume is there, so nothing reports it,
// and the workload in that container fails on its first call to Databricks with
// an error naming Databricks.
//
// Asking twice is only safe because the second pass does nothing to what the
// first wrote, which is the behaviour a pod already carrying this volume gets.
func TestTheWebhookIsAskedAgainWhenThePodChanges(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "config", "webhook", "manifests.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var configuration admissionv1.MutatingWebhookConfiguration
	if err := yaml.Unmarshal(data, &configuration); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	if len(configuration.Webhooks) != 1 {
		t.Fatalf("%d webhooks in %s, want one", len(configuration.Webhooks), path)
	}

	policy := configuration.Webhooks[0].ReinvocationPolicy
	if policy == nil || *policy != admissionv1.IfNeededReinvocationPolicy {
		t.Errorf("reinvocationPolicy is %v, want IfNeeded -- containers added after this webhook "+
			"ran get nothing, in a pod that reports as equipped because the volume is there",
			policy)
	}
}

// TestTheWebhookIsServedWhereTheManifestSendsPods covers the one drift this
// manifest being hand-written makes possible.
//
// It is hand-written because the +kubebuilder:webhook marker cannot express a
// namespaceSelector, and this webhook's whole reach is one. Nothing therefore
// regenerates the manifest from the code, so the path in it and the path the
// handler is registered on are two strings that have to agree and nothing makes
// them.
//
// Disagreeing is silent in the worst way. failurePolicy is Ignore, so the API
// server's call to a path with no handler is swallowed: every pod in every
// enrolled namespace starts with no token, no mount and no variables, looks
// entirely normal, and fails on its first call to Databricks. Nothing rewrites a
// running pod, so they stay that way.
func TestTheWebhookIsServedWhereTheManifestSendsPods(t *testing.T) {
	t.Parallel()
	configuration := mutatingWebhook(t)

	sent := configuration.Webhooks[0].ClientConfig.Service
	if sent == nil {
		t.Fatal("the manifest names no Service; nothing reaches this operator at all")
	}
	if sent.Path == nil || *sent.Path != dbxwebhook.Path {
		t.Errorf("pods are sent to %q and the handler is registered on %q. The call lands on no "+
			"handler, failurePolicy Ignore swallows it, and every pod in every enrolled "+
			"namespace starts unequipped and looks normal",
			ptr.Deref(sent.Path, ""), dbxwebhook.Path)
	}
}

// TestTheWebhookIsAskedOnlyAboutEnrolledNamespaces covers the selector that is
// the reason this manifest is written by hand.
//
// Without it this webhook stands on the creation path of every pod in the
// cluster. It admits them unchanged, so nothing breaks -- which is exactly why
// losing it would go unnoticed, while the blast radius of the failurePolicy
// decision beside it quietly became the whole cluster.
func TestTheWebhookIsAskedOnlyAboutEnrolledNamespaces(t *testing.T) {
	t.Parallel()
	selector := mutatingWebhook(t).Webhooks[0].NamespaceSelector

	if selector == nil || len(selector.MatchLabels) == 0 {
		t.Fatalf("namespaceSelector is %v; this webhook is now on the creation path of every "+
			"pod in the cluster", selector)
	}
	if got := selector.MatchLabels[dbxv1alpha1.InjectLabel]; got != dbxv1alpha1.Enabled {
		t.Errorf("the selector asks for %s=%q, want %q -- it no longer matches the label this "+
			"operator tells people to write, so either every namespace is consulted or none is",
			dbxv1alpha1.InjectLabel, got, dbxv1alpha1.Enabled)
	}
}

// mutatingWebhook reads the hand-written configuration these tests are about.
func mutatingWebhook(t *testing.T) admissionv1.MutatingWebhookConfiguration {
	t.Helper()
	path := filepath.Join("..", "..", "config", "webhook", "manifests.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var configuration admissionv1.MutatingWebhookConfiguration
	if err := yaml.Unmarshal(data, &configuration); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	if len(configuration.Webhooks) != 1 {
		t.Fatalf("%d webhooks in %s, want one", len(configuration.Webhooks), path)
	}
	return configuration
}

// TestTheShippedManifestNamesNobodysRegistry covers a leak a deploy writes by
// itself.
//
// `make deploy IMG=...` runs `kustomize edit set image`, which writes the
// registry into config/manager/kustomization.yaml and leaves it there. A
// registry hostname names an account -- an AWS account id is part of an ECR
// one -- so deploying to a cluster and then committing publishes whose
// infrastructure this was built against, with nothing in the diff that looks
// like a secret.
//
// The placeholder is what a clone should find: whoever deploys passes IMG.
func TestTheShippedManifestNamesNobodysRegistry(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "config", "manager", "kustomization.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var kustomization struct {
		Images []struct {
			Name    string `json:"name"`
			NewName string `json:"newName"`
			NewTag  string `json:"newTag"`
		} `json:"images"`
	}
	if err := yaml.Unmarshal(data, &kustomization); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}

	for _, image := range kustomization.Images {
		// A registry is what a dot or a port in the first segment says: the
		// placeholder is a bare name and cannot have one.
		host, _, _ := strings.Cut(image.NewName, "/")
		if strings.ContainsAny(host, ".:") {
			t.Errorf("%s names registry %q. A deploy wrote it and it was committed; the "+
				"hostname says whose account this was built against", path, image.NewName)
		}
	}
}
