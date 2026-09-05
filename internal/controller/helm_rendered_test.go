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
	"bytes"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// chartDir is the second description of the objects config/default describes.
//
// config/ is the source of truth: `make deploy` and the released
// dist/install.yaml are built from it. The chart says the same thing again, in
// another language, and a second description drifts -- so the tests in this file
// render both and compare them. Without that, a chart is an install that looks
// like this operator and is not.
const chartDir = "charts/databricks-service-principal-operator"

// chartImageRegistry is where this project publishes, which is the only registry
// a default in the chart may name: anywhere else is an account somebody else
// pays for and can take away.
const chartImageRegistry = "ghcr.io/workload-identity/"

// What the chart is rendered under here, and the choice is what makes the
// comparison readable rather than what makes it pass.
//
// Three things the two renderers are allowed to disagree on, and this is where
// each is settled rather than asserted.
//
// Names. config/default prepends one fixed string to everything; the chart
// builds every name from the release, because that is how two operators for two
// Databricks accounts install side by side without anybody editing a file.
// Rendering under a release named after that fixed string makes every namespaced
// name come out the same, and leaves the cluster-scoped ones differing by the
// namespace the chart puts in them -- which they must, and which
// TestTwoReleasesCollideOnNothingClusterScoped is about.
//
// Namespace. It is the release's, and `helm install --namespace` sets it. So it
// is passed in here rather than compared, and set to the one config/default
// installs into.
//
// Replicas. A value, and the only number in the Deployment this chart exists to
// let an installer change. Both sides default to one.
const (
	helmRelease   = "databricks-service-principal-operator"
	helmNamespace = "dbxsp-operator-system"
)

// templated renders the chart the way `helm install` would.
//
// --include-crds because the CRDs live in crds/ rather than templates/, and
// helm template leaves those out unless asked. --api-versions because the chart
// refuses to install into a cluster with no cert-manager, and `helm template`
// talks to no cluster at all: declaring the API version is what a cluster
// carrying cert-manager presents.
func templated(t *testing.T, release, namespace string) []map[string]any {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not on PATH")
	}
	root := filepath.Join("..", "..")

	command := exec.Command(helm, "template", release, filepath.Join(root, chartDir),
		"--namespace", namespace,
		"--include-crds",
		"--api-versions", "cert-manager.io/v1",
	)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	out, err := command.Output()
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, stderr.String())
	}
	return documents(t, out)
}

// documents splits a multi-document manifest into parsed objects.
func documents(t *testing.T, out []byte) []map[string]any {
	t.Helper()
	var objects []map[string]any
	for doc := range strings.SplitSeq(string(out), "\n---\n") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		object := map[string]any{}
		if err := yaml.Unmarshal([]byte(doc), &object); err != nil {
			t.Fatalf("parsing a rendered document: %v", err)
		}
		if len(object) > 0 {
			objects = append(objects, object)
		}
	}
	return objects
}

func byKind(objects []map[string]any, kind string) []map[string]any {
	var found []map[string]any
	for _, object := range objects {
		if object["kind"] == kind {
			found = append(found, object)
		}
	}
	return found
}

// decode reads a rendered object back as the type the API server would see it
// as, so that a comparison is over fields rather than over whatever shape the
// YAML happened to take.
func decode[T any](t *testing.T, object map[string]any) T {
	t.Helper()
	body, err := yaml.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	var into T
	if err := yaml.Unmarshal(body, &into); err != nil {
		t.Fatalf("decoding the rendered %v: %v", object["kind"], err)
	}
	return into
}

func managerOf(t *testing.T, objects []map[string]any) corev1.Container {
	t.Helper()
	deployment := decode[appsv1.Deployment](t, only(t, objects, "Deployment"))
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name == "manager" {
			return container
		}
	}
	t.Fatal("no container named manager in the rendered Deployment")
	return corev1.Container{}
}

// clusterScoped are the kinds two operators would collide on, minus the one
// they are meant to share.
//
// CustomResourceDefinitions are shared deliberately. They are one set per
// cluster whichever operator installed them, which is why the chart keeps them
// in crds/ where Helm installs them once and then never touches them -- and why
// an uninstall must not take them, since deleting a CRD deletes the records and
// destroying those destroys service principals.
var clusterScoped = map[string]bool{
	"ClusterRole":                    true,
	"ClusterRoleBinding":             true,
	"MutatingWebhookConfiguration":   true,
	"ValidatingWebhookConfiguration": true,
}

// TestTheChartAndTheOperatorShareAVersion covers the pair that is released
// together.
//
// The chart names the operator it installs, and the two are versioned as one
// thing: `helm install --version 0.15.0` and the v0.15.0 release are the same
// release. Letting them drift means a table somewhere of which chart goes with
// which operator, and the first person to need that table is the one who already
// installed the wrong pair.
//
// They differ by the `v`, which Helm refuses in a chart version and this project
// uses in every other version it writes.
func TestTheChartAndTheOperatorShareAVersion(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join("..", "..", chartDir, "Chart.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var chart struct {
		Version    string `json:"version"`
		AppVersion string `json:"appVersion"`
	}
	if err := yaml.Unmarshal(data, &chart); err != nil {
		t.Fatal(err)
	}
	if want := "v" + chart.Version; chart.AppVersion != want {
		t.Errorf("the chart is version %q and installs appVersion %q, want %q. They are released "+
			"together, so a reader who has the chart version knows the operator version and needs "+
			"nothing else", chart.Version, chart.AppVersion, want)
	}
}

// TestTheChartShipsTheGeneratedCRDs covers the copy nothing regenerates.
//
// config/crd/bases is written by `make manifests` from the Go types. The chart's
// crds/ is a hand-made copy of it, and Helm installs a chart's crds/ once and
// then never touches it -- so a schema change that reaches config/ and not here
// is not a broken install, it is an install that quietly serves last release's
// schema, prunes the field nobody declared, and reports nothing.
func TestTheChartShipsTheGeneratedCRDs(t *testing.T) {
	t.Parallel()
	root := filepath.Join("..", "..")
	generated := filepath.Join(root, "config", "crd", "bases")
	shipped := filepath.Join(root, chartDir, "crds")

	want, err := yamlNames(generated)
	if err != nil {
		t.Fatal(err)
	}
	got, err := yamlNames(shipped)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(want, got) {
		t.Fatalf("%s holds %v and %s holds %v; the chart installs the CRDs it carries and no others",
			generated, want, shipped, got)
	}

	for _, file := range want {
		a, err := os.ReadFile(filepath.Join(generated, file))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(shipped, file))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a, b) {
			t.Errorf("the chart's copy of %s is not what `make manifests` generated, so a Helm "+
				"install serves a schema this repository no longer describes -- and Helm never "+
				"updates a CRD it installed, so nothing corrects it later: "+
				"cp config/crd/bases/%s charts/databricks-service-principal-operator/crds/",
				file, file)
		}
	}
}

func yamlNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".yaml") {
			names = append(names, entry.Name())
		}
	}
	return names, nil
}

// TestTheChartInstallsTheSameCRDs covers the same drift from the other side:
// what each renderer actually hands the API server.
//
// The names have to match exactly, unlike everything else here -- a CRD name is
// <plural>.<group> and the API server derives it, so there is no room for a
// release to rename one and no second set for a second operator.
func TestTheChartInstallsTheSameCRDs(t *testing.T) {
	t.Parallel()
	chart := byKind(templated(t, helmRelease, helmNamespace), "CustomResourceDefinition")
	kustomized := byKind(rendered(t), "CustomResourceDefinition")

	specs := func(objects []map[string]any) map[string]string {
		out := map[string]string{}
		for _, object := range objects {
			body, err := yaml.Marshal(object["spec"])
			if err != nil {
				t.Fatal(err)
			}
			out[name(object)] = string(body)
		}
		return out
	}
	fromChart, fromKustomize := specs(chart), specs(kustomized)

	if len(fromKustomize) != 3 {
		t.Fatalf("config/default renders %d CRDs, want the three this operator owns", len(fromKustomize))
	}
	if !slices.Equal(slices.Sorted(maps.Keys(fromChart)), slices.Sorted(maps.Keys(fromKustomize))) {
		t.Fatalf("the chart installs CRDs %v and config/default installs %v",
			slices.Sorted(maps.Keys(fromChart)), slices.Sorted(maps.Keys(fromKustomize)))
	}
	for crd, want := range fromKustomize {
		if fromChart[crd] != want {
			t.Errorf("the chart's %s has a different spec from the generated one; an install "+
				"through Helm would validate custom resources against a schema this repository "+
				"does not describe", crd)
		}
	}
}

// TestTheChartStartsTheManagerTheSameWay covers what the process is handed.
//
// Every flag here has a default, which is what makes a missing one silent: the
// operator starts, passes its probes, and acts on values nobody wrote. The
// environment is compared whole rather than by the two DATABRICKS_ names alone,
// because a third one appearing would be a second source of truth for the
// account's coordinates, and the one the operator does not read.
//
// The image is compared too, and deliberately. It is a value, but its default is
// config/manager's placeholder -- a bare name with no registry in it, so that
// what is checked in names nobody's account -- and a chart that quietly defaulted
// to somebody's registry instead would be publishing that fact.
//
// replicaCount is not compared, for the reason given above the constants.
func TestTheChartStartsTheManagerTheSameWay(t *testing.T) {
	t.Parallel()
	fromChart := managerOf(t, templated(t, helmRelease, helmNamespace))
	fromKustomize := managerOf(t, rendered(t))

	chartArgs := slices.Sorted(slices.Values(fromChart.Args))
	kustomizeArgs := slices.Sorted(slices.Values(fromKustomize.Args))
	if !slices.Equal(chartArgs, kustomizeArgs) {
		t.Errorf("the chart starts the manager with %v and config/default with %v; every flag "+
			"here has a default, so the operator starts either way and acts on values nobody wrote",
			chartArgs, kustomizeArgs)
	}

	if got, want := envOf(fromChart), envOf(fromKustomize); !maps.Equal(got, want) {
		t.Errorf("the chart sets %v and config/default sets %v. DATABRICKS_OIDC_TOKEN_FILEPATH is "+
			"what selects the SDK's file-oidc auth type, DATABRICKS_TOKEN_AUDIENCE has to equal the "+
			"aud of the operator's federation policy, and POD_NAMESPACE has to come from the "+
			"downward API or the operator looks for its DatabricksAccount in the wrong namespace",
			got, want)
	}

	// The two name different images on purpose, and each default is right for
	// how that path is used. config/default is rewritten by `make deploy` with
	// whoever is deploying, so what a clone finds there has to name nobody -- a
	// registry hostname names an account, which is what
	// TestTheShippedManifestNamesNobodysRegistry keeps out. The chart is the
	// published artifact: `helm install` with no values is how somebody installs
	// this, so its default is the image this project publishes, and a placeholder
	// there would mean a chart whose ordinary install pulls nothing.
	//
	// What both must be is this project's own, so neither is checked in naming a
	// registry somebody else pays for.
	if !strings.HasPrefix(fromChart.Image, chartImageRegistry) {
		t.Errorf("the chart's default image is %q, want it under %q -- the chart is what people "+
			"install, so its default has to be an image this project publishes",
			fromChart.Image, chartImageRegistry)
	}
	if strings.Contains(fromKustomize.Image, "/") {
		t.Errorf("config/default's image is %q; a clone has to find a placeholder there, because "+
			"`make deploy` rewrites it and a registry hostname names an account",
			fromKustomize.Image)
	}

	if !slices.Equal(fromChart.Command, fromKustomize.Command) {
		t.Errorf("the chart runs %v and config/default runs %v", fromChart.Command, fromKustomize.Command)
	}
}

// envOf flattens the container's environment, including where a value that is
// not a literal comes from -- POD_NAMESPACE has to be the downward API, and a
// literal that happened to read the same would not be the same thing.
func envOf(container corev1.Container) map[string]string {
	out := map[string]string{}
	for _, variable := range container.Env {
		switch {
		case variable.ValueFrom != nil && variable.ValueFrom.FieldRef != nil:
			out[variable.Name] = "fieldRef:" + variable.ValueFrom.FieldRef.FieldPath
		case variable.ValueFrom != nil:
			out[variable.Name] = "valueFrom"
		default:
			out[variable.Name] = variable.Value
		}
	}
	return out
}

// TestTheChartRegistersTheSameWebhooks covers both admission configurations,
// field for field.
//
// Everything a webhook configuration carries decides either what reaches the
// operator or what happens when it cannot be reached, and none of it fails
// loudly. failurePolicy Ignore means a misconfiguration is swallowed: pods start
// with no token, requests that should be refused are admitted, and both look
// exactly like a cluster where nothing is wrong. So the comparison is over the
// whole object rather than a list of fields somebody has to keep up to date.
//
// The Service the API server dials is blanked before comparing, and only that.
// Its name and namespace are the chart's to choose -- that is how two operators
// each reach their own webhook server -- while the path is not, because the path
// is where a handler is registered in the code.
func TestTheChartRegistersTheSameWebhooks(t *testing.T) {
	t.Parallel()
	chart := templated(t, helmRelease, helmNamespace)
	kustomized := rendered(t)

	mutating := func(objects []map[string]any) admissionv1.MutatingWebhook {
		configuration := decode[admissionv1.MutatingWebhookConfiguration](t, only(t, objects, "MutatingWebhookConfiguration"))
		if len(configuration.Webhooks) != 1 {
			t.Fatalf("%d webhooks in the MutatingWebhookConfiguration, want one", len(configuration.Webhooks))
		}
		hook := configuration.Webhooks[0]
		hook.ClientConfig = admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{Path: hook.ClientConfig.Service.Path}}
		return hook
	}
	compare(t, "MutatingWebhookConfiguration", mutating(chart), mutating(kustomized))

	validating := func(objects []map[string]any) admissionv1.ValidatingWebhook {
		configuration := decode[admissionv1.ValidatingWebhookConfiguration](t, only(t, objects, "ValidatingWebhookConfiguration"))
		if len(configuration.Webhooks) != 1 {
			t.Fatalf("%d webhooks in the ValidatingWebhookConfiguration, want one", len(configuration.Webhooks))
		}
		hook := configuration.Webhooks[0]
		hook.ClientConfig = admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{Path: hook.ClientConfig.Service.Path}}
		return hook
	}
	compare(t, "ValidatingWebhookConfiguration", validating(chart), validating(kustomized))
}

// compare reports the difference between two objects as YAML, because the field
// that has drifted is what the reader needs and a struct printed on one line is
// not that.
func compare[T any](t *testing.T, what string, got, want T) {
	t.Helper()
	a, err := yaml.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	b, err := yaml.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Errorf("the chart's %s and config/default's disagree.\n\nchart:\n%s\nconfig/default:\n%s",
			what, a, b)
	}
}

// TestTheChartGrantsExactlyWhatKustomizeGrants covers the drift that matters
// most, and it is checked in both directions because the two failures are
// different and both are quiet.
//
// A chart that grants more is a permission nobody reviewed, held by a process
// that already holds a Databricks account admin credential. A chart that grants
// less is an operator that cannot reach something -- and nothing in this package
// would see it, because no unit test runs against an API server that enforces
// RBAC. It surfaces as a reconcile failing in somebody's cluster.
//
// Grants are compared rather than rules: a rule is a grouping and the same
// permissions can be written many ways, so what is compared is the set of
// (group, resource, verb) triples each role hands out. The roles are matched by
// what is left of their names after each renderer's prefix -- kustomize's fixed
// namePrefix, and the chart's release and namespace -- which is also the
// assertion that the chart renames roles and does not invent or drop them.
func TestTheChartGrantsExactlyWhatKustomizeGrants(t *testing.T) {
	t.Parallel()
	fromChart := grantsByRole(t, templated(t, helmRelease, helmNamespace), chartSuffix)
	fromKustomize := grantsByRole(t, rendered(t), kustomizeSuffix)

	for role := range fromChart {
		if _, known := fromKustomize[role]; !known {
			t.Errorf("the chart installs %s, which config/default does not. Whatever it grants is "+
				"a permission nobody reviewed against the operator this repository describes", role)
		}
	}
	for role := range fromKustomize {
		if _, known := fromChart[role]; !known {
			t.Errorf("config/default installs %s and the chart does not. If the operator is bound "+
				"to it, a Helm install is an operator that cannot do something, and it fails at "+
				"reconcile time in somebody's cluster", role)
		}
	}

	for role, want := range fromKustomize {
		got, known := fromChart[role]
		if !known {
			continue
		}
		for grant := range got {
			if !want[grant] {
				t.Errorf("the chart's %s grants %s and config/default's does not. The operator "+
					"already holds a Databricks account admin credential; a permission it holds "+
					"in the cluster on top of that is one nobody agreed to", role, grant)
			}
		}
		for grant := range want {
			if !got[grant] {
				t.Errorf("config/default's %s grants %s and the chart's does not. Nothing here "+
					"enforces RBAC, so this is green until an install fails a reconcile",
					role, grant)
			}
		}
	}
}

// kustomizeSuffix and chartSuffix strip what each renderer puts in front of a
// name. config/default prepends one fixed string; the chart prepends the release
// and, for a cluster-scoped object, the namespace as well.
func kustomizeSuffix(n string) string {
	return strings.TrimPrefix(n, helmRelease+"-")
}

func chartSuffix(n string) string {
	return strings.TrimPrefix(strings.TrimPrefix(n, helmRelease+"-"), helmNamespace+"-")
}

// grantsByRole reads every role in a render as the set of permissions it hands
// out, keyed by kind and by the part of its name neither renderer chose.
func grantsByRole(t *testing.T, objects []map[string]any, suffix func(string) string) map[string]map[string]bool {
	t.Helper()
	out := map[string]map[string]bool{}
	for _, kind := range []string{"ClusterRole", "Role"} {
		for _, object := range byKind(objects, kind) {
			role := decode[rbacv1.Role](t, object)
			key := kind + " " + suffix(name(object))
			if _, seen := out[key]; seen {
				t.Fatalf("two %ss are named %q once their prefix is taken off; the comparison "+
					"cannot tell them apart", kind, key)
			}
			out[key] = grantsOf(role.Rules)
		}
	}
	return out
}

func grantsOf(rules []rbacv1.PolicyRule) map[string]bool {
	grants := map[string]bool{}
	for _, rule := range rules {
		for _, verb := range rule.Verbs {
			for _, group := range rule.APIGroups {
				for _, resource := range rule.Resources {
					for _, named := range orAny(rule.ResourceNames) {
						grants[fmt.Sprintf("%s/%s/%s:%s", group, resource, named, verb)] = true
					}
				}
			}
			for _, url := range rule.NonResourceURLs {
				grants[fmt.Sprintf("url %s:%s", url, verb)] = true
			}
		}
	}
	return grants
}

// orAny stands in for the empty resourceNames list, which means every object of
// that resource rather than none of them.
func orAny(names []string) []string {
	if len(names) == 0 {
		return []string{"*"}
	}
	return names
}

// TestTwoReleasesCollideOnNothingClusterScoped covers the thing the chart is
// better at than config/default, and the only way to find out is to render two.
//
// One operator per Databricks account is a supported shape
// (docs/several-operators.md), and config/default cannot do it without somebody
// editing its fixed namePrefix and namespace. So every cluster-scoped name the
// chart produces has to carry the release -- and the namespace as well, because
// Helm releases are namespaced and two people may each hold a release called
// "operator".
//
// A collision here does not fail an install. Helm 3 refuses to adopt an object
// another release owns, so the second install stops halfway; and where it does
// not, the first uninstall takes the second operator's ClusterRoles and webhook
// configurations with it.
func TestTwoReleasesCollideOnNothingClusterScoped(t *testing.T) {
	t.Parallel()

	names := func(release, namespace string) map[string]bool {
		out := map[string]bool{}
		for _, object := range templated(t, release, namespace) {
			kind, _ := object["kind"].(string)
			if !clusterScoped[kind] {
				continue
			}
			if want := release + "-" + namespace + "-"; !strings.HasPrefix(name(object), want) {
				t.Errorf("%s %q does not start with %q; a cluster-scoped name that does not carry "+
					"both is a name a second operator can land on", kind, name(object), want)
			}
			out[kind+" "+name(object)] = true
		}
		if len(out) == 0 {
			t.Fatal("the chart rendered no cluster-scoped objects")
		}
		return out
	}

	// Two accounts, two teams, nothing in common.
	finance := names("finance", "finance-operator")
	risk := names("risk", "risk-operator")
	for object := range finance {
		if risk[object] {
			t.Errorf("two releases both install %s", object)
		}
	}

	// And the case the namespace in the name exists for: the same release name,
	// which Helm permits because a release belongs to a namespace.
	here := names("operator", "team-a-operator")
	there := names("operator", "team-b-operator")
	for object := range here {
		if there[object] {
			t.Errorf("two releases called \"operator\" in different namespaces both install %s; "+
				"whichever is installed second adopts or is refused, and whichever is uninstalled "+
				"first takes it from the other", object)
		}
	}
}

// TestTheChartsNamesReachEachOther covers the class of failure
// kustomize_rendered_test.go exists for, in the renderer that has just as much
// of it: a reference left pointing at a name something else renamed.
//
// Helm has no equivalent of kustomize's nameReference at all -- every one of
// these is a string written twice -- and the consequence is the same and just as
// quiet. cert-manager cannot find an Issuer, or the API server is told to trust
// a Certificate that does not exist, and failurePolicy Ignore turns all of it
// into pods that start without a token.
func TestTheChartsNamesReachEachOther(t *testing.T) {
	t.Parallel()
	objects := templated(t, helmRelease, helmNamespace)

	issuer := only(t, objects, "Issuer")
	certificate := only(t, objects, "Certificate")
	spec, _ := certificate["spec"].(map[string]any)
	ref, _ := spec["issuerRef"].(map[string]any)
	if got, _ := ref["name"].(string); got != name(issuer) {
		t.Errorf("the Certificate asks for Issuer %q and the Issuer is named %q", got, name(issuer))
	}
	if namespace(certificate) != namespace(issuer) {
		t.Errorf("the Certificate is in %q and the Issuer in %q; an Issuer is namespaced",
			namespace(certificate), namespace(issuer))
	}

	wantCA := namespace(certificate) + "/" + name(certificate)
	for _, kind := range []string{"MutatingWebhookConfiguration", "ValidatingWebhookConfiguration"} {
		webhook := only(t, objects, kind)
		meta, _ := webhook["metadata"].(map[string]any)
		annotations, _ := meta["annotations"].(map[string]any)
		if got, _ := annotations["cert-manager.io/inject-ca-from"].(string); got != wantCA {
			t.Errorf("inject-ca-from on the %s is %q, want %q -- without it the API server has no "+
				"CA for this webhook, every call fails the handshake, and failurePolicy Ignore "+
				"swallows that", kind, got, wantCA)
		}
	}

	// The Service the API server dials, the certificate that has to cover that
	// name, and the Secret the manager mounts to serve it.
	hook := decode[admissionv1.MutatingWebhookConfiguration](t, only(t, objects, "MutatingWebhookConfiguration")).Webhooks[0]
	service := hook.ClientConfig.Service
	var reached bool
	for _, candidate := range byKind(objects, "Service") {
		if name(candidate) == service.Name && namespace(candidate) == service.Namespace {
			reached = true
		}
	}
	if !reached {
		t.Errorf("the webhook calls Service %s/%s, which the chart does not create",
			service.Namespace, service.Name)
	}

	dialled := service.Name + "." + service.Namespace + ".svc"
	names, _ := spec["dnsNames"].([]any)
	var covered bool
	for _, candidate := range names {
		if value, _ := candidate.(string); value == dialled {
			covered = true
		}
	}
	if !covered {
		t.Errorf("dnsNames are %v, want one covering %q -- the API server dials that name and "+
			"verifies what it is given against it", names, dialled)
	}

	secret, _ := spec["secretName"].(string)
	deployment := decode[appsv1.Deployment](t, only(t, objects, "Deployment"))
	var mounted bool
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Secret != nil && volume.Secret.SecretName == secret {
			mounted = true
		}
	}
	if !mounted {
		t.Errorf("the Deployment does not mount %q; cert-manager writes the certificate there and "+
			"the webhook server has nothing to serve without it", secret)
	}
}

// TestTheChartsBindingsNameWhatItInstalls covers the other half of the same
// problem, on the objects that decide what the operator may do.
//
// A binding whose roleRef names nothing is not refused -- RBAC has no referential
// integrity -- so the operator simply has none of those permissions, and the
// first thing anybody sees is a reconcile failing on a Forbidden it should never
// have met.
func TestTheChartsBindingsNameWhatItInstalls(t *testing.T) {
	t.Parallel()
	objects := templated(t, helmRelease, helmNamespace)

	roles := map[string]bool{}
	for _, kind := range []string{"ClusterRole", "Role"} {
		for _, object := range byKind(objects, kind) {
			roles[kind+" "+namespace(object)+"/"+name(object)] = true
		}
	}
	accounts := map[string]bool{}
	for _, object := range byKind(objects, "ServiceAccount") {
		accounts[namespace(object)+"/"+name(object)] = true
	}

	for _, kind := range []string{"ClusterRoleBinding", "RoleBinding"} {
		for _, object := range byKind(objects, kind) {
			binding := decode[rbacv1.RoleBinding](t, object)

			// A ClusterRoleBinding names a ClusterRole; a RoleBinding may name
			// either, and the role it names lives in the binding's namespace
			// when it is a Role.
			in := ""
			if binding.RoleRef.Kind == "Role" {
				in = namespace(object)
			}
			if key := binding.RoleRef.Kind + " " + in + "/" + binding.RoleRef.Name; !roles[key] {
				t.Errorf("%s %q names %s, which the chart does not install. RBAC does not refuse a "+
					"binding to nothing, so the operator just has none of those permissions",
					kind, name(object), key)
			}

			for _, subject := range binding.Subjects {
				if subject.Kind != "ServiceAccount" {
					continue
				}
				if key := subject.Namespace + "/" + subject.Name; !accounts[key] {
					t.Errorf("%s %q is granted to ServiceAccount %s, which the chart does not "+
						"install", kind, name(object), key)
				}
			}
		}
	}
}

// TestTheChartsServicesReachItsPods covers the labels, which are the only thing
// joining a Service to the process behind it.
//
// A selector matching nothing is an endpointless Service: the API server's call
// to the webhook is refused at connect, failurePolicy Ignore swallows it, and
// every pod in every enrolled namespace starts unequipped. A selector matching
// too much is worse in a cluster with two operators, because the API server then
// reaches whichever pod it likes and one account's webhook answers about the
// other account's identities.
func TestTheChartsServicesReachItsPods(t *testing.T) {
	t.Parallel()
	objects := templated(t, helmRelease, helmNamespace)
	deployment := decode[appsv1.Deployment](t, only(t, objects, "Deployment"))
	pod := deployment.Spec.Template.Labels

	if deployment.Spec.Selector == nil || len(deployment.Spec.Selector.MatchLabels) == 0 {
		t.Fatal("the Deployment has no selector")
	}
	for key, want := range deployment.Spec.Selector.MatchLabels {
		if pod[key] != want {
			t.Errorf("the Deployment selects %s=%q and its pods carry %q; the API server refuses a "+
				"Deployment whose selector does not match its own template", key, want, pod[key])
		}
	}

	// The instance is what keeps one operator's Service off another's pods. Two
	// operators run the same image under the same name in different namespaces,
	// and a Service is namespaced -- but a selector that named neither the
	// release nor anything release-specific would be one label away from
	// reaching across if either ever moved.
	if pod["app.kubernetes.io/instance"] != helmRelease {
		t.Errorf("the manager's pods carry instance %q, want %q",
			pod["app.kubernetes.io/instance"], helmRelease)
	}

	for _, object := range byKind(objects, "Service") {
		service := decode[corev1.Service](t, object)
		if len(service.Spec.Selector) == 0 {
			t.Errorf("Service %q selects on nothing", name(object))
			continue
		}
		for key, want := range service.Spec.Selector {
			if pod[key] != want {
				t.Errorf("Service %q selects %s=%q and the manager's pods carry %q, so it has no "+
					"endpoints at all", name(object), key, want, pod[key])
			}
		}
	}
}
