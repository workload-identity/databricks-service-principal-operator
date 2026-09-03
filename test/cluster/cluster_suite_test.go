//go:build cluster
// +build cluster

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

package cluster

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/workload-identity/databricks-service-principal-operator/test/utils"
)

var (
	// managerImage is the manager image to be built and loaded for testing.
	managerImage = "example.com/databricks-service-principal-operator:v0.0.1"
	// managerNamespace is where the deployment lands. The suite creates it and
	// removes it, because every spec here runs against that one deployment.
	managerNamespace = "dbxsp-operator-system"
	// shouldCleanupCertManager tracks whether CertManager was installed by this suite.
	shouldCleanupCertManager = false
)

// TestCluster runs everything that needs a real Kubernetes cluster and nothing
// else. It reaches as far as the first call to Databricks and stops there: a
// kind cluster publishes an issuer Databricks cannot fetch, so nothing here
// mints. What is on the far side of that line is test/e2e's, which needs an
// account as well as a cluster.
//
// The default setup requires Kind and CertManager.
//
// To enable kubectl kuberc (use custom kubectl configurations), set: KUBECTL_KUBERC=true
// By default, kuberc is disabled to ensure consistent test behavior across different environments.
// To skip CertManager installation, set: CERT_MANAGER_INSTALL_SKIP=true
func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting databricks-service-principal-operator cluster test suite\n")
	RunSpecs(t, "cluster suite")
}

var _ = BeforeSuite(func() {
	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager image")

	// TODO(user): If you want to change the test vendor from Kind,
	// ensure the image is built and available, then remove the following block.
	By("loading the manager image on Kind")
	err = utils.LoadImageToKindClusterWithName(managerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager image into Kind")

	configureKubectlKubeRC()
	pinCluster()
	setupCertManager()
	awaitCertManager()

	By("removing what an earlier run left in the manager namespace")
	// Before creating it as well as after using it. A run that was interrupted
	// never reaches its AfterSuite, and what it leaves is a namespace that
	// cannot finish terminating -- so the next run fails on "namespace is being
	// terminated", which says nothing about anything being tested.
	clearManagerNamespace()

	By("creating the manager namespace")
	cmd = exec.Command("kubectl", "create", "ns", managerNamespace)
	_, _ = utils.Run(cmd)

	By("labeling the namespace to enforce the restricted security policy")
	cmd = exec.Command("kubectl", "label", "--overwrite", "ns", managerNamespace,
		"pod-security.kubernetes.io/enforce=restricted")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to label the manager namespace")

	By("installing CRDs")
	cmd = exec.Command("make", "install")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to install CRDs")

	By("deploying the controller-manager")
	cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
})

var _ = AfterSuite(func() {
	By("removing cluster-scoped objects the specs created")
	// Named here rather than left to the spec that made it: a spec that failed
	// partway never reaches its own cleanup, and this is cluster-scoped, so what
	// it breaks is the next run rather than this one.
	cmd := exec.Command("kubectl", "delete", "clusterrolebinding",
		metricsRoleBindingName, "--ignore-not-found")
	_, _ = utils.Run(cmd)

	By("removing the manager namespace")
	// Ahead of undeploying, which is the opposite of the order it reads in.
	// Undeploying deletes the manager namespace along with everything in it, and
	// waits -- so run first it waits on records that it has, in the same breath,
	// deleted the only thing that could release. Ahead of uninstalling for the
	// same kind of reason: a record cannot be reached once its CRD is gone.
	clearManagerNamespace()

	By("undeploying the controller-manager")
	cmd = exec.Command("make", "undeploy")
	_, _ = utils.Run(cmd)

	By("uninstalling CRDs")
	cmd = exec.Command("make", "uninstall")
	_, _ = utils.Run(cmd)

	teardownCertManager()

	By("removing the manager image built for this run")
	// A host-side artifact of this suite, under a placeholder registry nothing
	// else writes to, loaded into a cluster that is about to be deleted. Left
	// behind it is a hundred megabytes per run that nobody goes looking for,
	// on a machine that was not asked.
	cmd = exec.Command("docker", "rmi", managerImage)
	_, _ = utils.Run(cmd)
})

// clearManagerNamespace stops the operator and takes its namespace away, records
// and all, and does not return until the namespace is gone.
//
// The records are the whole of the work. Each is held by a finalizer until its
// service principal is confirmed deleted in Databricks, and against a kind
// cluster that confirmation can never arrive: Databricks cannot reach the issuer
// this cluster publishes, so nothing was ever created, and the operator will not
// read a call it could not make as "gone". Held, they keep the namespace
// terminating for as long as the cluster lives, and every later run fails on
// that rather than on anything it meant to test.
//
// So this does by hand what the operator's own comment says a person does:
// removes the finalizer having seen what is left behind. What is left behind
// here is nothing, and that is a fact about a cluster Databricks cannot reach
// rather than a judgement about the record.
//
// The operator is stopped first so that nothing puts the finalizer back. A
// record on its way out never gets it again -- that is what keeps this way out
// open for a person -- but a record still wanted does, and one of those is
// enough to wedge the namespace all over again.
func clearManagerNamespace() {
	cmd := exec.Command("kubectl", "delete", "deployment", "-n", managerNamespace,
		"-l", "control-plane=controller-manager", "--ignore-not-found", "--wait=true")
	_, _ = utils.Run(cmd)

	cmd = exec.Command("kubectl", "get", "issueddatabricksserviceprincipal",
		"-n", managerNamespace, "--ignore-not-found", "-o", "name")
	records, err := utils.Run(cmd)
	if err == nil {
		for _, record := range utils.GetNonEmptyLines(records) {
			cmd = exec.Command("kubectl", "patch", record, "-n", managerNamespace,
				"--type", "merge", "-p", `{"metadata":{"finalizers":null}}`)
			_, _ = utils.Run(cmd)
		}
	}

	cmd = exec.Command("kubectl", "delete", "namespace", managerNamespace,
		"--ignore-not-found", "--wait=true", "--timeout=2m")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(),
		"the manager namespace would not go; a run against this cluster cannot deploy until it has")
}

// pinCluster gives this run a kubeconfig of its own, naming the one cluster it
// is allowed to touch.
//
// Every kubectl below is a subprocess that inherits this process's environment
// and carries no --context, so without this they all follow whichever context
// the shared kubeconfig currently points at. Nothing holds that still: "kind
// create cluster" moves it, and so does anybody working on the machine.
//
// What the suite does on a cluster it reached by accident is not small. It
// deletes namespaces, uninstalls CRDs, removes cluster-scoped roles, and
// uninstalls cert-manager -- and every one of those succeeds quietly on the
// wrong cluster, because they are the same commands it meant to run.
func pinCluster() {
	By("pinning this run to its own cluster")
	kubeconfig := filepath.Join(os.TempDir(), "kubeconfig-"+utils.KindCluster())
	cmd := exec.Command(utils.KindBinary(), "export", "kubeconfig",
		"--name", utils.KindCluster(), "--kubeconfig", kubeconfig)
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(),
		"Failed to write a kubeconfig for the cluster this run was given")
	ExpectWithOffset(1, os.Setenv("KUBECONFIG", kubeconfig)).To(Succeed())
}

// Disable kubectl kuberc by default for test isolation.
// This prevents local kubectl configurations from affecting test behavior.
// To enable kuberc, set: KUBECTL_KUBERC=true
func configureKubectlKubeRC() {
	if os.Getenv("KUBECTL_KUBERC") != "true" {
		By("disabling kubectl kuberc for test isolation")
		err := os.Setenv("KUBECTL_KUBERC", "false")
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to disable kubectl kuberc")
		_, _ = fmt.Fprintf(GinkgoWriter,
			"kubectl kuberc disabled for consistent test behavior (override with KUBECTL_KUBERC=true)\n")
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "kubectl kuberc enabled (KUBECTL_KUBERC=true)\n")
	}
}

// setupCertManager installs CertManager if needed for webhook tests.
// Skips installation if CERT_MANAGER_INSTALL_SKIP=true or if already present.
func setupCertManager() {
	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true" {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager installation (CERT_MANAGER_INSTALL_SKIP=true)\n")
		return
	}

	By("checking if CertManager is already installed")
	if utils.IsCertManagerCRDsInstalled() {
		_, _ = fmt.Fprintf(GinkgoWriter, "CertManager is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupCertManager = true

	By("installing CertManager")
	Expect(utils.InstallCertManager()).To(Succeed(), "Failed to install CertManager")
}

// awaitCertManager waits until cert-manager will answer for its own kinds.
//
// Its deployment being available is not that, and the gap between the two is
// where deploying lands. A Certificate or an Issuer goes through cert-manager's
// own validating webhook, which the API server reaches over a certificate the
// cainjector writes into that webhook's configuration some time after the
// deployment comes up. Before it does, creating either fails with "certificate
// signed by unknown authority" -- which names a certificate, and reads as this
// operator's being wrong rather than as cert-manager not being ready.
//
// Asked by trying, because there is no condition anywhere that says it. The
// server-side dry run goes through admission and creates nothing, so this is
// the question itself rather than something that stands in for it.
func awaitCertManager() {
	By("waiting for cert-manager to admit its own kinds")
	probe := `apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: readiness-probe
  namespace: cert-manager
spec:
  selfSigned: {}
`
	Eventually(func() error {
		cmd := exec.Command("kubectl", "apply", "--dry-run=server", "-f", "-")
		cmd.Stdin = strings.NewReader(probe)
		_, err := utils.Run(cmd)
		return err
	}, 3*time.Minute, 5*time.Second).Should(Succeed(),
		"cert-manager never became able to admit an Issuer, so nothing this deploys can have a "+
			"certificate")
}

// teardownCertManager uninstalls CertManager if it was installed by setupCertManager.
// This ensures we only remove what we installed.
func teardownCertManager() {
	if !shouldCleanupCertManager {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager cleanup (not installed by this suite)\n")
		return
	}

	By("uninstalling CertManager")
	utils.UninstallCertManager()
}
