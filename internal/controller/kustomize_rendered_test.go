package controller

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

// rendered builds config/default the way `make deploy` does, and returns every
// object in it.
//
// These tests read the rendered output rather than the source files because
// what goes wrong lives in the rendering: kustomize renames things, and a
// reference it does not know about is left behind pointing at a name that no
// longer exists. Reading the sources would show two files that each look right.
func rendered(t *testing.T) []map[string]any {
	t.Helper()
	root := filepath.Join("..", "..")
	kustomize := filepath.Join(root, "bin", "kustomize")
	if _, err := os.Stat(kustomize); err != nil {
		t.Skip("bin/kustomize is not built; run `make kustomize`")
	}

	out, err := exec.Command(kustomize, "build", filepath.Join(root, "config", "default")).Output()
	if err != nil {
		t.Fatalf("kustomize build: %v", err)
	}

	var objects []map[string]any
	for doc := range strings.SplitSeq(string(out), "\n---\n") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		object := map[string]any{}
		if err := yaml.Unmarshal([]byte(doc), &object); err != nil {
			t.Fatalf("parsing the rendered manifest: %v", err)
		}
		if len(object) > 0 {
			objects = append(objects, object)
		}
	}
	return objects
}

func only(t *testing.T, objects []map[string]any, kind string) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, object := range objects {
		if object["kind"] == kind {
			found = append(found, object)
		}
	}
	if len(found) != 1 {
		t.Fatalf("found %d %s in the rendered manifest, want 1", len(found), kind)
	}
	return found[0]
}

func name(object map[string]any) string {
	meta, _ := object["metadata"].(map[string]any)
	value, _ := meta["name"].(string)
	return value
}

func namespace(object map[string]any) string {
	meta, _ := object["metadata"].(map[string]any)
	value, _ := meta["namespace"].(string)
	return value
}

// TestTheCertificateReachesItsIssuer covers a reference kustomize does not
// know about.
//
// A Certificate's issuerRef names an Issuer, and nothing built into kustomize
// says so — so the namePrefix renames the Issuer and leaves the reference
// pointing at a name that no longer exists. Both source files still look
// correct, and cert-manager fails to issue at deploy time.
//
// What follows from that failure is quiet rather than loud: the webhook has no
// serving certificate, its failurePolicy is Ignore, and pods start without a
// token. So this is worth a test even though it is one line of configuration.
func TestTheCertificateReachesItsIssuer(t *testing.T) {
	t.Parallel()
	objects := rendered(t)
	issuer := only(t, objects, "Issuer")
	certificate := only(t, objects, "Certificate")

	spec, _ := certificate["spec"].(map[string]any)
	ref, _ := spec["issuerRef"].(map[string]any)
	got, _ := ref["name"].(string)

	if got != name(issuer) {
		t.Errorf("the Certificate asks for Issuer %q and the Issuer is named %q; "+
			"cert-manager will not issue, the webhook will have no certificate, and "+
			"pods will start with no token and no complaint", got, name(issuer))
	}
	if namespace(certificate) != namespace(issuer) {
		t.Errorf("the Certificate is in %q and the Issuer in %q; an Issuer is namespaced",
			namespace(certificate), namespace(issuer))
	}
}

// TestTheWebhookIsToldWhichCertificateToTrust covers the other half of the same
// wiring.
//
// The API server is the only client of this webhook, and it trusts it only
// because cert-manager writes a caBundle in — which it does when this annotation
// names the Certificate. A stale name here fails exactly as above: silently.
func TestTheWebhookIsToldWhichCertificateToTrust(t *testing.T) {
	t.Parallel()
	objects := rendered(t)
	certificate := only(t, objects, "Certificate")
	webhook := only(t, objects, "MutatingWebhookConfiguration")

	meta, _ := webhook["metadata"].(map[string]any)
	annotations, _ := meta["annotations"].(map[string]any)
	got, _ := annotations["cert-manager.io/inject-ca-from"].(string)

	want := namespace(certificate) + "/" + name(certificate)
	if got != want {
		t.Errorf("inject-ca-from is %q, want %q", got, want)
	}
}

// TestTheWebhookPointsAtAServiceThatExists covers the third name in the same
// chain, and the certificate covering it.
func TestTheWebhookPointsAtAServiceThatExists(t *testing.T) {
	t.Parallel()
	objects := rendered(t)
	webhook := only(t, objects, "MutatingWebhookConfiguration")
	certificate := only(t, objects, "Certificate")

	var services []map[string]any
	for _, object := range objects {
		if object["kind"] == "Service" {
			services = append(services, object)
		}
	}

	hooks, _ := webhook["webhooks"].([]any)
	if len(hooks) != 1 {
		t.Fatalf("the manifest registers %d webhooks, want 1", len(hooks))
	}
	hook, _ := hooks[0].(map[string]any)
	config, _ := hook["clientConfig"].(map[string]any)
	service, _ := config["service"].(map[string]any)
	serviceName, _ := service["name"].(string)
	serviceNamespace, _ := service["namespace"].(string)

	var matched bool
	for _, s := range services {
		if name(s) == serviceName && namespace(s) == serviceNamespace {
			matched = true
		}
	}
	if !matched {
		t.Fatalf("the webhook calls Service %s/%s, which the manifest does not create",
			serviceNamespace, serviceName)
	}

	// And the certificate has to cover the name the API server will dial, or the
	// call fails verification rather than failing to connect.
	spec, _ := certificate["spec"].(map[string]any)
	names, _ := spec["dnsNames"].([]any)
	want := serviceName + "." + serviceNamespace + ".svc"
	var covered bool
	for _, n := range names {
		if value, _ := n.(string); value == want {
			covered = true
		}
	}
	if !covered {
		t.Errorf("dnsNames are %v, want one covering %q -- the API server dials that name",
			names, want)
	}
}

// TestTheManagerIsGivenTheCertificateItServes covers the last link: the
// Deployment mounting the Secret cert-manager writes, and being told where.
func TestTheManagerIsGivenTheCertificateItServes(t *testing.T) {
	t.Parallel()
	objects := rendered(t)
	certificate := only(t, objects, "Certificate")
	deployment := only(t, objects, "Deployment")

	spec, _ := certificate["spec"].(map[string]any)
	secret, _ := spec["secretName"].(string)

	body, err := yaml.Marshal(deployment)
	if err != nil {
		t.Fatal(err)
	}
	manifest := string(body)

	if !strings.Contains(manifest, secret) {
		t.Errorf("the Deployment does not mount %q; cert-manager writes the certificate there "+
			"and the webhook server has nothing to serve without it", secret)
	}
	if !strings.Contains(manifest, "--webhook-cert-path") {
		t.Error("the manager is not told where its certificate is, so it will not find one")
	}
}

// TestTheWebhookOnlySeesNamespacesThatWereAllowed covers the selector that keeps
// this off the critical path of everything else.
//
// It is also what prevents the deadlock this class of webhook is known for: the
// operator's own namespace does not carry the label, so admitting the operator's
// own pod never depends on the operator running.
func TestTheWebhookOnlySeesNamespacesThatWereAllowed(t *testing.T) {
	t.Parallel()
	objects := rendered(t)
	webhook := only(t, objects, "MutatingWebhookConfiguration")

	hooks, _ := webhook["webhooks"].([]any)
	hook, _ := hooks[0].(map[string]any)
	selector, _ := hook["namespaceSelector"].(map[string]any)
	labels, _ := selector["matchLabels"].(map[string]any)

	if labels[dbxv1alpha1.InjectLabel] != dbxv1alpha1.Enabled {
		t.Errorf("namespaceSelector is %v, want %s -- without it this webhook is asked about "+
			"every pod in the cluster, including the operator's own",
			selector, dbxv1alpha1.InjectLabel)
	}
	// And specifically not the minting label. They end at different times:
	// closing minting must not stop equipping the pods of identities that were
	// minted while it was open, which is every workload's next rollout.
	if _, wrong := labels[dbxv1alpha1.MintLabel]; wrong {
		t.Errorf("namespaceSelector is %v; selecting on the minting label means closing "+
			"minting silently stops injection, and every identity in that namespace loses "+
			"its token as pods are recreated", selector)
	}
}

// TestTheManagerKeepsEveryFlagTheManifestSets covers a patch quietly dropping
// what it did not mean to touch.
//
// A strategic merge that sets `args` replaces the whole list — args is a list of
// scalars with no merge key, so kustomize has nothing to match on and takes the
// patch's version entire. Every flag the base Deployment sets disappears, and
// every one of them has a default, so the operator starts, passes its probes,
// and looks correct while acting on values nobody wrote.
//
// Nothing else notices: a patch about webhook certificates can switch off the
// metrics server and leave a manifest that still applies cleanly.
func TestTheManagerKeepsEveryFlagTheManifestSets(t *testing.T) {
	t.Parallel()
	objects := rendered(t)
	deployment := only(t, objects, "Deployment")

	spec, _ := deployment["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	podSpec, _ := template["spec"].(map[string]any)
	containers, _ := podSpec["containers"].([]any)
	if len(containers) != 1 {
		t.Fatalf("the Deployment has %d containers, want 1", len(containers))
	}
	container, _ := containers[0].(map[string]any)
	raw, _ := container["args"].([]any)

	args := make([]string, 0, len(raw))
	for _, a := range raw {
		value, _ := a.(string)
		args = append(args, value)
	}

	for _, want := range []string{
		// Two or more replicas would each act on every object.
		"--leader-elect",
		// Without it the probes in this same manifest have nothing to reach.
		"--health-probe-bind-address=",
		// Which DatabricksAccount to act in.
		"--databricks-account=",
		// Where cert-manager writes the certificate the webhook serves.
		"--webhook-cert-path=",
		// Added by a separate patch.
		"--metrics-bind-address=",
	} {
		var found bool
		for _, arg := range args {
			if strings.HasPrefix(arg, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("args are %v, want one starting %q -- a patch has replaced the list rather "+
				"than appending to it, and every flag here has a default that hides it", args, want)
		}
	}
}

// TestTheManagerCarriesItsOwnToken covers the volume the operator authenticates
// with, which the same class of patch would drop.
func TestTheManagerCarriesItsOwnToken(t *testing.T) {
	t.Parallel()
	objects := rendered(t)
	deployment := only(t, objects, "Deployment")

	body, err := yaml.Marshal(deployment)
	if err != nil {
		t.Fatal(err)
	}
	manifest := string(body)

	for _, want := range []string{
		// The projected token the operator exchanges. Nothing else authenticates
		// it, and there is no credential in this manifest to fall back to.
		"serviceAccountToken",
		"databricks-token",
		// The certificate the webhook serves, added by a different patch. Both
		// have to survive, and a patch that replaces a list drops the other.
		"webhook-certs",
	} {
		if !strings.Contains(manifest, want) {
			t.Errorf("the Deployment does not carry %q", want)
		}
	}
}

// dnsLabel is the limit on one DNS label -- 63 octets, RFC 1035. A name that
// becomes a single label of a hostname is held to it.
const dnsLabel = 63

// dnsSubdomain is the limit on a whole name, and what most kinds get.
const dnsSubdomain = 253

// singleLabel lists the kinds whose names have to fit in one label. A Service
// name is the first label of <service>.<namespace>.svc and a Namespace name the
// second, so those two get 63 where everything else in this manifest gets 253.
var singleLabel = map[string]bool{
	"Service":   true,
	"Namespace": true,
}

func limitFor(kind string) int {
	if singleLabel[kind] {
		return dnsLabel
	}
	return dnsSubdomain
}

// TestEveryNameFitsWhatItsKindAllows covers the prefix outgrowing one of the
// names it is prepended to.
//
// namePrefix here is the repo name, and the repo name is long because it says
// exactly what this operator does. That is worth keeping and it costs headroom:
// a Service gets 63 characters in total, so whatever is appended to the prefix
// has what is left of that.
//
// The scaffold does not know about the prefix. Its own metrics Service is called
// controller-manager-metrics-service, which lands on 72, and nothing catches it
// until apply -- where the API server complains about a DNS-1035 label and names
// neither the prefix nor the file the rest of the name came from. Every test
// passes first.
//
// So this asserts the budget rather than any particular name: whatever a future
// scaffold generates has to fit before it can be deployed.
func TestEveryNameFitsWhatItsKindAllows(t *testing.T) {
	t.Parallel()
	objects := rendered(t)
	prefix := namePrefix(t)

	if len(objects) == 0 {
		t.Fatal("the manifest rendered no objects")
	}

	// Namespaces are collected rather than reported in place. Kustomize writes
	// the same one onto every namespaced object from a single setting, so one
	// mistake would otherwise be reported a dozen times and bury the line that
	// names the Namespace itself.
	namespaces := map[string]bool{}

	for _, object := range objects {
		kind, _ := object["kind"].(string)
		limit := limitFor(kind)

		if got := name(object); len(got) > limit {
			t.Errorf("%s %q is %d characters and a %s may be %d; "+
				"namePrefix is %q (%d), leaving %d for the rest of the name",
				kind, got, len(got), kind, limit, prefix, len(prefix), limit-len(prefix))
		}

		if got := namespace(object); got != "" {
			namespaces[got] = true
		}
	}

	// A namespace is a Namespace name wherever it appears. A manifest that
	// names one too long to exist fails at apply for a reason none of the
	// objects carrying it reports.
	for got := range namespaces {
		if len(got) > dnsLabel {
			t.Errorf("the manifest places objects in namespace %q, which is %d "+
				"characters and a Namespace may be %d", got, len(got), dnsLabel)
		}
	}
}

// namePrefix reports what config/default prepends to every name, so that a
// failure above can say where the length went rather than only that there was
// too much of it. An unreadable file is not itself the failure: the test still
// has the rendered names, and reports without the attribution.
func namePrefix(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "config", "default", "kustomization.yaml"))
	if err != nil {
		return ""
	}
	var config struct {
		NamePrefix string `json:"namePrefix"`
	}
	if err := yaml.Unmarshal(body, &config); err != nil {
		return ""
	}
	return config.NamePrefix
}
