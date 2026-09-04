//go:build cluster
// +build cluster

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

package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	dbxwebhook "github.com/workload-identity/databricks-service-principal-operator/internal/webhook"
	"github.com/workload-identity/databricks-service-principal-operator/test/utils"
)

// namespace where the project is deployed in
const namespace = "dbxsp-operator-system"

// serviceAccountName created for the project
const serviceAccountName = "databricks-service-principal-operator-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "databricks-service-principal-operator-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "databricks-service-principal-operator-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	// The deployment is torn down by the suite, not here: it is what every spec
	// in this package runs against, and undeploying it after one container left
	// the others with no operator and no CRDs at all.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				By("getting the name of the controller-manager pod")
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				By("validating the pod's status")
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			// Removed first. This is cluster-scoped, so a run that failed before
			// its AfterAll leaves it behind and every later run fails here on a
			// leftover rather than on anything real.
			cmd := exec.Command("kubectl", "delete", "clusterrolebinding", metricsRoleBindingName,
				"--ignore-not-found")
			_, _ = utils.Run(cmd)

			cmd = exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=databricks-service-principal-operator-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": [
								"for i in $(seq 1 30); do curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics && exit 0 || sleep 2; done; exit 1"
							],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		// TODO: Customize the e2e test suite with scenarios specific to your project.
		// Consider applying sample/CR(s) and check their status and/or verifying
		// the reconciliation by using the metrics, i.e.:
		// metricsOutput, err := getMetricsOutput()
		// Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
		// Expect(metricsOutput).To(ContainSubstring(
		//    fmt.Sprintf(`controller_runtime_reconcile_total{controller="%s",result="success"} 1`,
		//    strings.ToLower(<Kind>),
		// ))
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		By("executing kubectl command to create the token")
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		By("parsing the JSON output to extract the token")
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}

// The identity path, on a real cluster, for everything that does not need
// Databricks.
//
// What this covers and the envtest suite cannot: garbage collection. envtest
// runs no collector, so "deleting the ServiceAccount takes the identity with
// it" is only true where a real control plane is running -- and that is the
// whole reason the object is owned by the ServiceAccount rather than checked
// against it.
var _ = Describe("Identities", Ordered, func() {
	const (
		team    = "cluster-team"
		account = "etl"
	)

	// Nothing here mints. A kind cluster publishes an OIDC issuer Databricks
	// cannot reach, so the first call always fails -- which is exactly the line
	// these tests stop at. Everything before it is this operator's own: who may
	// ask, who agreed to serve them, what is written down before anything is
	// created, and what happens when the request goes away.
	//
	// What is on the far side of that line is covered by the live tests, against
	// a real account, and cannot be covered here at any price.

	// Removed before as well as after. AfterAll does not run when a run is
	// interrupted, so a suite that only cleans up afterwards fails on its own
	// leftovers the next time -- and the failure is about a stale namespace
	// rather than about anything this is testing.
	BeforeAll(func() {
		removeTeam(team)
		applyAccount()
		serveNamespaces()
	})
	AfterAll(func() {
		removeTeam(team)
		serveNamespaces()
	})

	It("mints nothing in a namespace nobody enabled", func() {
		applyTeam(team, account, unnamed)

		Consistently(func() string {
			return identityNames(team)
		}, 10*time.Second, time.Second).Should(BeEmpty(),
			"an identity was minted in a namespace nobody allowed to ask")
	})

	It("mints nothing once enabled while this operator serves it not", func() {
		// The order a team is onboarded in: annotated first by the team, enabled
		// afterwards by somebody with cluster-wide access. Nothing touches the
		// ServiceAccount again, and a namespace label is an event on no
		// ServiceAccount.
		cmd := exec.Command("kubectl", "label", "namespace", team,
			"databricks.workload-identity.io/inject=enabled",
			"databricks.workload-identity.io/mint=enabled")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		// The cluster has said this namespace may be served by somebody. It has
		// not said by whom, and it cannot: only the team holding the account
		// admin credential can commit it.
		Consistently(func() string {
			return identityNames(team)
		}, 10*time.Second, time.Second).Should(BeEmpty(),
			"an identity was minted for an operator that was never told to serve this namespace")
	})

	It("wakes when this operator is told to serve it, without anything else changing", func() {
		serveNamespaces(team)

		Eventually(func() string {
			return identityNames(team)
		}, time.Minute, time.Second).Should(ContainSubstring(account),
			"widening this operator's reach woke nothing; every ServiceAccount in the "+
				"namespace stays unserved until something unrelated happens to touch one")
	})

	It("writes the record before anything is created, in its own namespace", func() {
		// The order is the whole guarantee. A pass that writes this and then
		// fails leaves a record naming no service principal, which the marker
		// resolves; the reverse order leaves a service principal nothing
		// records. Here the first call cannot succeed at all, so what this
		// asserts is that the record exists anyway.
		Eventually(func() string {
			return recordNames()
		}, time.Minute, time.Second).Should(ContainSubstring(team+"."+account),
			"nothing recorded what was about to be issued; the record is what outlives "+
				"the namespace that asked")
	})

	It("says which object to create rather than failing silently", func() {
		Eventually(func() string {
			cmd := exec.Command("kubectl", "get", "databricksserviceaccount", account, "-n", team,
				"-o", "jsonpath={.status.identities[0].conditions[?(@.type=='Ready')].message}")
			out, err := utils.Run(cmd)
			if err != nil {
				return ""
			}
			return out
		}, time.Minute, time.Second).Should(Not(BeEmpty()),
			"nothing said why this identity is not ready")
	})

	It("takes the identity with the ServiceAccount", func() {
		// Ownership rather than a check, which is what makes this true without
		// anything having to notice. Only a real control plane collects.
		cmd := exec.Command("kubectl", "delete", "serviceaccount", account, "-n", team)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() string {
			return identityNames(team)
		}, time.Minute, time.Second).Should(BeEmpty(),
			"the identity outlived the ServiceAccount it stands for")
	})

	It("stops wanting the record too, which is the half nothing collects", func() {
		// The DatabricksServiceAccount is owned by the ServiceAccount and is
		// collected by the control plane. The record is owned by nothing --
		// deliberately, so that a namespace teardown cannot take it -- so its going
		// is this operator's own work, and nothing else in the cluster would notice
		// if it stopped.
		Eventually(func() []string {
			return recordsStillWanted()
		}, time.Minute, time.Second).ShouldNot(ContainElement(ContainSubstring(team+"."+account)),
			"the record outlived the ServiceAccount it was issued to; nothing else collects it")
	})

	It("holds the record it cannot confirm, and says what is holding it", func() {
		// Where this cluster stops. Letting go means having deleted the service
		// principal, and nothing here can reach Databricks to delete one or to
		// ask whether one is there -- so the record stays, which is the refusal
		// the design is built on rather than a failure of it. A pass that let go
		// on a call it could not make would leave a live service principal with
		// nothing anywhere recording it, and that is the one outcome none of
		// this may produce.
		Consistently(func() []string {
			return recordsBeingDeleted()
		}, 20*time.Second, 2*time.Second).Should(ContainElement(ContainSubstring(team+"."+account)),
			"a record was let go without its service principal having been deleted")

		Expect(heldBecause(team+"."+account)).NotTo(BeEmpty(),
			"the record is held and nothing on it says why, so the only way to learn what is "+
				"left behind is to read the operator's logs")
	})
})

var _ = Describe("Several identities", Ordered, func() {
	const (
		team    = "cluster-several"
		account = "etl"
	)

	BeforeAll(func() {
		removeTeam(team)
		applyAccount()
		applyTeam(team, account, "reader", "writer")
		cmd := exec.Command("kubectl", "label", "namespace", team,
			"databricks.workload-identity.io/inject=enabled",
			"databricks.workload-identity.io/mint=enabled")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		serveNamespaces(team)
	})
	AfterAll(func() {
		removeTeam(team)
		serveNamespaces()
	})

	It("records one identity per name, keyed by what the asker wrote", func() {
		Eventually(func() string {
			return recordNames()
		}, time.Minute, time.Second).Should(SatisfyAll(
			ContainSubstring(team+"."+account+".reader"),
			ContainSubstring(team+"."+account+".writer"),
		), "one ServiceAccount asked for two identities and did not get two records")

		Eventually(func() []string {
			return requestsOn(team, account)
		}, time.Minute, time.Second).Should(ConsistOf("reader", "writer"),
			"the DatabricksServiceAccount does not carry both identities under the names they were asked by, "+
				"which are the names a workload will pass to the SDK")
	})

	It("says which operator issued each entry", func() {
		// One object may carry entries from several operators, and a named
		// identity's key is its name alone -- so this field is the only thing on
		// the object that says whose an entry is. An operator that cannot find
		// its own entries either destroys a neighbour's identity or stops
		// maintaining one of its own while it still reports Ready.
		Eventually(func() []string {
			return operatorsOn(team, account)
		}, time.Minute, time.Second).Should(ConsistOf(operatorRef, operatorRef),
			"an entry does not name the operator that issued it")
	})

	It("takes only the identity whose key was deleted", func() {
		withdraw(team, account, "reader")

		Eventually(func() []string {
			return recordsStillWanted()
		}, time.Minute, time.Second).ShouldNot(ContainElement(ContainSubstring(team+"."+account+".reader")),
			"the identity whose key was deleted is still wanted")

		// The half that says the first half meant anything. Both records name
		// one ServiceAccount, one operator and one uid, and differ only in the
		// name the asker gave -- so a pass that reads one key's deletion as the
		// whole request having changed destroys both here and looks correct
		// above.
		Expect(recordsStillWanted()).To(ContainElement(ContainSubstring(team+"."+account+".writer")),
			"deleting one identity's key took the one still asked for")
	})

	It("takes the DatabricksServiceAccount with the last of them", func() {
		// Withdrawing the last entry is an apply that removes the only field this
		// status has, and what a real API server does with that is the thing a fake
		// client cannot show: the merge writes null and the write is refused, leaving
		// a DatabricksServiceAccount that reports Ready for identities that no longer
		// exist.
		withdraw(team, account, "writer")

		Eventually(func() string {
			return identityNames(team)
		}, time.Minute, time.Second).Should(BeEmpty(),
			"the DatabricksServiceAccount outlived the request that made it")
		Eventually(func() []string {
			return recordsStillWanted()
		}, time.Minute, time.Second).ShouldNot(ContainElement(ContainSubstring(team+"."+account+".writer")),
			"the record outlived the request that made it")
	})
})

// A key this operator will not act on, which is the half of a request it used to
// have to guess at.
//
// Every spec here is about what does not happen. A value that cannot be read and
// a name this operator cannot use are both refusals, and a refusal has to leave
// the identity its key names exactly as it stands: that is what makes refusing
// safe, and it is the only reason a mistyped annotation is no longer a destroyed
// service principal and every grant anybody made on it.
//
// Kind is enough for all of it. Nothing here mints, but a refusal is decided
// before the first call to Databricks, and the record it must not take is
// written before that call too.
var _ = Describe("A key this operator will not act on", Ordered, func() {
	const (
		team    = "cluster-refused"
		account = "etl"

		// The identity asked for correctly, which every spec below leaves
		// standing. Without one there is nothing a refusal could destroy, and
		// "destroyed nothing" would hold just as well of an operator that does
		// nothing at all.
		kept = "reader"
	)
	held := team + "." + account + "." + kept

	BeforeAll(func() {
		removeTeam(team)
		applyAccount()
		applyTeam(team, account, kept)
		cmd := exec.Command("kubectl", "label", "namespace", team,
			"databricks.workload-identity.io/inject=enabled",
			"databricks.workload-identity.io/mint=enabled")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		serveNamespaces(team)

		Eventually(func() []string {
			return recordsStillWanted()
		}, time.Minute, time.Second).Should(ContainElement(ContainSubstring(held)),
			"nothing was recorded for the identity that was asked for correctly, so there is "+
				"nothing here that a refusal could be observed to leave alone")
	})
	AfterAll(func() {
		removeTeam(team)
		serveNamespaces()
	})

	It("holds the identity whose value it could not read, rather than taking a typo for a withdrawal", func() {
		// One "/" short of an operator reference, which is the input this whole
		// change was made for. As one entry of a comma-separated value it was
		// indistinguishable from an entry somebody had removed, and it destroyed
		// the service principal it named.
		Expect(ask(team, account, kept, namespace)).To(Succeed())
		// Put back inside the spec that broke it, so that what the specs below
		// find is a ServiceAccount asking correctly for one identity.
		DeferCleanup(func() { Expect(ask(team, account, kept, operatorRef)).To(Succeed()) })

		// Polled rather than read once. The edit wakes a reconcile at once, so a
		// single read straight after it passes before the operator has looked at
		// the annotation at all -- which is the reading under which the destroy
		// this is about went unnoticed for as long as it did.
		Consistently(func() []string {
			return recordsStillWanted()
		}, 20*time.Second, 2*time.Second).Should(ContainElement(ContainSubstring(held)),
			"a value that could not be read took the identity it names away. Nothing asked for "+
				"that: the key is still there, and what it says could not be read")
	})

	It("refuses a name the cluster took and it cannot use, and destroys nothing in doing it", func() {
		// The key being a map entry validates nothing that matters. The API
		// server takes uppercase, "_" and "." in the name part of a key, and all
		// three would be carried into the name of a Kubernetes object: a record
		// is <namespace>.<serviceaccount>.<identity>-<suffix>, which takes
		// neither of the first two, and a "." in the identity leaves nothing to
		// say where the ServiceAccount's name ends and the identity's begins.
		refused := []string{"Reader", "read_er", "read.er"}
		for _, name := range refused {
			key := dbxv1alpha1.ServicePrincipalAnnotationFor(name)
			Expect(ask(team, account, name, operatorRef)).To(Succeed(),
				"the cluster would not carry %s, so this spec is not about what it says it is: "+
					"what it is for is a name the API server accepts and this operator must not",
				key)
			Expect(annotationsOn(team, account)).To(HaveKey(key),
				"%s is not on the ServiceAccount, so nothing has been asked of this operator yet",
				key)
		}

		Consistently(func(g Gomega) {
			wanted := recordsStillWanted()
			g.Expect(wanted).To(ContainElement(ContainSubstring(held)),
				"a name this operator refused took the identity that was asked for correctly")
			for _, name := range refused {
				g.Expect(wanted).NotTo(ContainElement(ContainSubstring(team+"."+account+"."+name)),
					"an identity was issued under the name %q, which no Kubernetes object can "+
						"be called and no record could ever be created for", name)
			}
		}, 20*time.Second, 2*time.Second).Should(Succeed())
	})

	It("cannot be asked for a longer name than a key holds, which is where the limit comes from", func() {
		// Why the name limit is 45 rather than 63: an annotation key's name part
		// is capped at 63 and "service-principal." is 18 of them. A 46-character
		// name is therefore never a request this operator gets to refuse -- the
		// cluster refuses the key first -- and both halves of that are asked of
		// a real API server here rather than counted a second time in a comment.
		const longest = 45
		Expect(ask(team, account, strings.Repeat("a", longest+1), operatorRef)).NotTo(Succeed(),
			"the cluster carried an identity name of %d characters. The limit this operator "+
				"enforces was measured against what an annotation key holds, and a key that "+
				"holds more makes it an arbitrary limit rather than a protective one",
			longest+1)

		name := strings.Repeat("a", longest)
		Expect(ask(team, account, name, operatorRef)).To(Succeed(),
			"the cluster would not carry a %d-character identity name, so this operator's limit "+
				"is one character too generous and every name at it is unusable", longest)
		Eventually(func() string {
			return recordNames()
		}, time.Minute, time.Second).Should(ContainSubstring(team+"."+account+"."+name),
			"the longest name a key can hold was accepted and nothing was recorded for it. The "+
				"record's name is derived from this one, and a derived name that is not a legal "+
				"object name fails inside a reconcile where nobody who asked will see it")
	})
})

// The pod-facing half, which is nearly all only true on a real cluster.
//
// An envtest spec covers what the webhook writes, by calling it. What calling it
// cannot show is whether anything reaches it: its registration, the certificate
// cert-manager issued for it, the Service in front of it, and the selector
// deciding which namespaces' pods it is asked about. A webhook that is never
// consulted passes every test that consults it directly.
//
// Nor can it show that what the webhook wrote can be carried out. The unnamed
// identity's profile is the operator's own reference, so a projection path
// holding "/" is legal to write and has to be turned into nested directories by
// a kubelet; a configuration handed over through downwardAPI has to arrive as it
// was written, and it is written once and read by an SDK that will not say which
// character it choked on.
//
// Both shapes of profile are here for that reason: the unnamed one, whose path
// is two segments, and a named one, whose path is the name and nothing else.
//
// The identities are put in by hand because nothing here can mint them, and
// what is being tested starts after minting. Every other Describe leaves the
// DatabricksServiceAccount to the operator; this one owns it, so the namespace
// is enabled for injection and for nothing else, and this operator is never
// told to serve it.
var _ = Describe("What a pod is equipped with", Ordered, func() {
	const (
		team    = "cluster-pod"
		account = "etl"
		runner  = "runner"
		reader  = "reader"

		// sole is the unnamed identity's profile name, which is this operator's
		// own reference because that identity has no name of its own.
		sole = operatorRef
	)

	BeforeAll(func() {
		removeTeam(team)
		equipTeam(team, account, sole, reader)
		awaitOperator()
	})
	AfterAll(func() {
		removeTeam(team)
	})

	It("is asked about at all, which nothing else here proves", func() {
		// The pod is created and comes back changed. Between those two is every
		// part of being a webhook that is not the handler: the configuration
		// naming it, the certificate it is reached over, and the selector that
		// decided this namespace's pods are its business.
		//
		// Made again and again until it does, because failurePolicy is Ignore
		// and there is nothing else to wait for. A pod created while the webhook
		// is not yet answering comes back exactly as a pod does when the webhook
		// is broken: unchanged, admitted, and with no error anywhere. Waiting for
		// the deployment above narrows that window and does not close it -- the
		// API server has still to have been given the certificate and to have
		// found the service -- so asking once would make this spec pass or fail
		// on the order the suite happened to shuffle into.
		Eventually(func() []string {
			runPod(team, runner, account)
			return volumeNames(team, runner)
		}, 2*time.Minute, 5*time.Second).Should(ContainElement(dbxwebhook.TokenVolume),
			"the pod was admitted unchanged, so nothing consulted the webhook: it is not "+
				"registered, or not reachable, or not asked about this namespace")
	})

	It("can be started with what the webhook wrote, which is a kubelet's opinion", func() {
		// Admission and startup are different judges. The API server accepted
		// these paths as strings; the kubelet has to make directories out of
		// them, and the "/" inside the unnamed identity's is the whole reason to
		// ask.
		Eventually(func() string {
			return podPhase(team, runner)
		}, time.Minute, time.Second).Should(Equal("Running"),
			"the pod was admitted and would not start, which is this operator having made a "+
				"workload uncreatable in the one way admission does not catch")
	})

	It("holds one token per identity, at the path its profile names", func() {
		for _, identity := range []string{sole, reader} {
			path := fmt.Sprintf("%s/%s/%s",
				dbxwebhook.TokenMountPath, identity, dbxwebhook.TokenFile)
			token, err := inPod(team, runner, "cat "+shellQuote(path))
			Expect(err).NotTo(HaveOccurred(),
				"no token at the path the profile for %q names, so an SDK reading that "+
					"profile has nothing to present", identity)
			// Three parts, which is as far as this can go: whether Databricks
			// would accept it is a question for a cluster it can reach.
			Expect(strings.Split(strings.TrimSpace(token), ".")).To(HaveLen(3),
				"the file at the path for %q is not a token", identity)
		}
	})

	It("hands over the configuration unchanged, which nothing downstream can repair", func() {
		// Byte for byte. It leaves as an annotation, is carried by downwardAPI,
		// and arrives as a file an SDK parses -- and a configuration that is
		// nearly right fails at the exchange, a long way from here, saying
		// nothing about a newline.
		written := podAnnotation(team, runner, dbxwebhook.ConfigAnnotation)
		Expect(written).NotTo(BeEmpty(), "the webhook wrote no configuration to carry")

		read, err := inPod(team, runner,
			"cat "+shellQuote(dbxwebhook.TokenMountPath+"/"+dbxwebhook.ConfigFile))
		Expect(err).NotTo(HaveOccurred(), "there is no configuration in the pod to read")
		Expect(read).To(Equal(written),
			"the configuration the pod reads is not the one the webhook wrote")

		for _, identity := range []string{sole, reader} {
			Expect(read).To(ContainSubstring("[" + identity + "]"))
		}
	})
})

// equipTeam builds a namespace whose pods are equipped but whose identities are
// nobody's work but this test's.
//
// The DatabricksServiceAccount is written whole, status included, because the
// operator is never told to serve this namespace and so never writes one. A
// client id is what the webhook waits for -- an identity without one cannot be
// exchanged for anything -- so these carry one, and it stands for a minted
// identity in the only way this cluster allows.
//
// The identities are profile names, which is what the webhook reads and what the
// entries are keyed by. Each is written as this operator's because an entry has
// to name one: operator is required in the CRD, so an entry without it is not an
// entry this can write.
func equipTeam(team, serviceAccount string, identities ...string) {
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %s
  labels:
    databricks.workload-identity.io/inject: enabled
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: %s
  namespace: %s
---
apiVersion: databricks.workload-identity.io/v1alpha1
kind: DatabricksServiceAccount
metadata:
  name: %s
  namespace: %s
`, team, serviceAccount, team, serviceAccount, team)

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to create the namespace pods are equipped in")

	entries := make([]string, 0, len(identities))
	for i, identity := range identities {
		entries = append(entries, fmt.Sprintf(
			`{"profile":%q,"operator":%q,`+
				`"clientId":"0000000%d-0000-0000-0000-000000000000","audience":"databricks"}`,
			identity, operatorRef, i+1))
	}
	cmd = exec.Command("kubectl", "-n", team, "patch", "databricksserviceaccount", serviceAccount,
		"--subresource", "status", "--type", "merge",
		"-p", fmt.Sprintf(`{"status":{"identities":[%s]}}`, strings.Join(entries, ",")))
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to write the identities the pod is equipped with")
}

// awaitOperator waits for the operator to be up, which is as much of the
// webhook being ready as anything here can observe.
//
// It is not the whole of it. What has to be true is that the API server can
// reach the webhook and has the certificate to do it over, and nothing reports
// that -- an unequipped pod is the only symptom, and it is the same symptom as
// the failure this suite is looking for.
func awaitOperator() {
	cmd := exec.Command("kubectl", "wait", "--for=condition=Available",
		"deployment", "-l", "control-plane=controller-manager",
		"-n", namespace, "--timeout=3m")
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "the operator never became available")
}

// runPod creates a pod that stays up long enough to be looked inside, replacing
// one left by an earlier attempt.
func runPod(team, name, serviceAccount string) {
	cmd := exec.Command("kubectl", "delete", "pod", name, "-n", team,
		"--ignore-not-found", "--force", "--grace-period=0")
	_, _ = utils.Run(cmd)

	manifest := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  serviceAccountName: %s
  restartPolicy: Never
  containers:
  - name: app
    image: busybox:1.37
    command: ["sleep", "600"]
`, name, team, serviceAccount)

	cmd = exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to create the pod")
}

// volumeNames lists the volumes a pod came back with.
func volumeNames(team, name string) []string {
	cmd := exec.Command("kubectl", "get", "pod", name, "-n", team,
		"-o", "jsonpath={.spec.volumes[*].name}")
	out, err := utils.Run(cmd)
	if err != nil {
		return nil
	}
	return strings.Fields(out)
}

// podPhase reads where a pod has got to.
func podPhase(team, name string) string {
	cmd := exec.Command("kubectl", "get", "pod", name, "-n", team, "-o", "jsonpath={.status.phase}")
	out, err := utils.Run(cmd)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// podAnnotation reads one annotation off a pod.
//
// Read as JSON rather than by jsonpath: the keys here hold dots and slashes,
// which jsonpath reads as structure.
func podAnnotation(team, name, key string) string {
	cmd := exec.Command("kubectl", "get", "pod", name, "-n", team, "-o", "json")
	out, err := utils.Run(cmd)
	if err != nil {
		return ""
	}
	var pod struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal([]byte(out), &pod); err != nil {
		return ""
	}
	return pod.Metadata.Annotations[key]
}

// inPod runs a shell command inside the pod and returns what it printed.
//
// This is the only reader that sees what the workload sees. Everything else
// here reads the pod's own description, which says what was asked for rather
// than what arrived.
func inPod(team, name, command string) (string, error) {
	cmd := exec.Command("kubectl", "exec", "-n", team, name, "--", "sh", "-c", command)
	return utils.Run(cmd)
}

// shellQuote wraps a path for the shell inside the pod, where these paths hold
// characters a shell would otherwise read.
func shellQuote(path string) string {
	return "'" + strings.ReplaceAll(path, "'", `'\''`) + "'"
}

// removeTeam deletes a team namespace and waits for it to be gone, so that a
// create straight afterwards does not race the terminating one.
func removeTeam(team string) {
	cmd := exec.Command("kubectl", "delete", "namespace", team, "--ignore-not-found", "--wait=true")
	_, _ = utils.Run(cmd)
}

// operatorRef is how a ServiceAccount names this operator: the namespace it runs
// in and the name of its DatabricksAccount. It is the string a tenant writes,
// so the tests write it the same way rather than assembling it differently.
const operatorRef = namespace + "/databricks-account"

// unnamed is what a ServiceAccount asks for by writing the bare key: this
// operator's only identity, which is what nearly every ServiceAccount wants. Its
// profile name, having none of its own, is operatorRef.
const unnamed = ""

// applyTeam creates the namespace and a ServiceAccount asking this operator for
// the identities named, one annotation key each, written as apply so that it
// says the same thing whether or not anything is there.
func applyTeam(team, serviceAccount string, identities ...string) {
	var annotations strings.Builder
	for _, identity := range identities {
		fmt.Fprintf(&annotations, "    %q: %q\n",
			dbxv1alpha1.ServicePrincipalAnnotationFor(identity), operatorRef)
	}

	manifest := fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %s
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: %s
  namespace: %s
  annotations:
%s`, team, serviceAccount, team, annotations.String())

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to create the team namespace and ServiceAccount")
}

// ask writes one identity's key on a ServiceAccount that is already there, and
// reports what the cluster said rather than insisting it succeeded: some of the
// keys written here are ones the API server itself will not take, and that
// refusal is what the spec writing them is about.
func ask(team, account, identity, operator string) error {
	cmd := exec.Command("kubectl", "-n", team, "annotate", "serviceaccount", account,
		"--overwrite", dbxv1alpha1.ServicePrincipalAnnotationFor(identity)+"="+operator)
	_, err := utils.Run(cmd)
	return err
}

// withdraw takes one identity's key away, which is the only edit that destroys
// an identity.
//
// A key that is gone says the identity is no longer wanted; a key that is there
// and cannot be read says nothing at all. Written as two different edits because
// they are two different requests -- which is the whole of what these suites were
// rewritten for.
func withdraw(team, account, identity string) {
	cmd := exec.Command("kubectl", "-n", team, "annotate", "serviceaccount", account,
		dbxv1alpha1.ServicePrincipalAnnotationFor(identity)+"-")
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to withdraw identity %q", identity)
}

// requestsOn lists the identities the DatabricksServiceAccount carries, by the
// profile name each is keyed under.
func requestsOn(team, serviceAccount string) []string {
	cmd := exec.Command("kubectl", "get", "databricksserviceaccount", serviceAccount, "-n", team,
		"-o", "jsonpath={.status.identities[*].request}")
	out, err := utils.Run(cmd)
	if err != nil {
		return nil
	}
	return strings.Fields(out)
}

// operatorsOn lists what those entries say about who issued them.
func operatorsOn(team, serviceAccount string) []string {
	cmd := exec.Command("kubectl", "get", "databricksserviceaccount", serviceAccount, "-n", team,
		"-o", "jsonpath={.status.identities[*].operator}")
	out, err := utils.Run(cmd)
	if err != nil {
		return nil
	}
	return strings.Fields(out)
}

// annotationsOn reads what the ServiceAccount is actually carrying.
//
// Read as JSON rather than by jsonpath: these keys hold dots and slashes, which
// jsonpath reads as structure. Asked at all because half of what these specs
// write is a key the API server may refuse, and a spec about this operator
// refusing something has to know the cluster took it first.
func annotationsOn(team, serviceAccount string) map[string]string {
	cmd := exec.Command("kubectl", "get", "serviceaccount", serviceAccount, "-n", team, "-o", "json")
	out, err := utils.Run(cmd)
	if err != nil {
		return nil
	}
	var fetched struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal([]byte(out), &fetched); err != nil {
		return nil
	}
	return fetched.Metadata.Annotations
}

// serveNamespaces is the platform team's half of the answer, on the operator's
// own DatabricksAccount. Naming nothing serves nothing, so a suite that never
// writes this is a suite where the operator correctly does nothing.
func serveNamespaces(names ...string) {
	list := "[]"
	if len(names) > 0 {
		list = `["` + strings.Join(names, `","`) + `"]`
	}
	cmd := exec.Command("kubectl", "-n", namespace, "patch",
		"databricksaccount", "databricks-account", "--type", "merge",
		"-p", fmt.Sprintf(`{"spec":{"namespaces":%s}}`, list))
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to declare which namespaces the operator serves")
}

// applyAccount creates the DatabricksAccount the operator acts on.
//
// Its coordinates are deliberately not real. A kind cluster publishes an issuer
// Databricks cannot reach, so nothing here can mint: what these tests cover is
// everything up to the first call, which is where all of this operator's own
// decisions are made.
func applyAccount() {
	manifest := fmt.Sprintf(`apiVersion: databricks.workload-identity.io/v1alpha1
kind: DatabricksAccount
metadata:
  name: databricks-account
  namespace: %s
spec:
  host: https://accounts.cloud.databricks.com
  accountId: 00000000-0000-0000-0000-000000000000
  clientId: 00000000-0000-0000-0000-000000000000
`, namespace)

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to create the DatabricksAccount")
}

// recordNames lists the records in the operator's own namespace, which is where
// what was issued is remembered.
func recordNames() string {
	cmd := exec.Command("kubectl", "get", "issueddatabricksserviceprincipal",
		"-n", namespace, "-o", "jsonpath={.items[*].metadata.name}")
	out, err := utils.Run(cmd)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// records is enough of an IssuedDatabricksServicePrincipal list to answer the
// two questions these tests ask of one: is it on its way out, and what does it
// say about itself while it will not go.
type records struct {
	Items []struct {
		Metadata struct {
			Name              string `json:"name"`
			DeletionTimestamp string `json:"deletionTimestamp"`
		} `json:"metadata"`
		Status struct {
			Conditions []struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"conditions"`
		} `json:"status"`
	} `json:"items"`
}

// recordsInNamespace reads every record in the operator's own namespace.
func recordsInNamespace() records {
	cmd := exec.Command("kubectl", "get", "issueddatabricksserviceprincipal",
		"-n", namespace, "-o", "json")
	out, err := utils.Run(cmd)
	if err != nil {
		return records{}
	}
	var listed records
	if err := json.Unmarshal([]byte(out), &listed); err != nil {
		return records{}
	}
	return listed
}

// recordsStillWanted names the records the operator has not decided against:
// there, and not on their way out.
//
// The decision is what these tests are about, and it is entirely this
// operator's -- whether the ServiceAccount is still there, still asking for this
// identity, and still the same one. Everything after it is a call to Databricks
// that no cluster running here can make.
//
// Asked this way round on purpose. "Gone" and "marked for deletion" are one
// answer to the question being asked, and which of the two a record reaches
// depends on whether Databricks can be reached at all -- so a test that named
// one of them would be asserting the state of the network. Asserting the
// decision leaves how far the deletion got to the spec that is about that.
func recordsStillWanted() []string {
	var wanted []string
	for _, record := range recordsInNamespace().Items {
		if record.Metadata.DeletionTimestamp == "" {
			wanted = append(wanted, record.Metadata.Name)
		}
	}
	return wanted
}

// recordsBeingDeleted names the records that are on their way out and have not
// got there: deleted as far as this operator is concerned, and still present
// because the finalizer will not let go.
//
// The distinction from recordsStillWanted is the whole point of having both. A
// record that vanishes outright is one nobody wanted -- correct on a cluster
// that can reach Databricks, and on this one it means the finalizer was dropped
// on a call that never went through.
func recordsBeingDeleted() []string {
	var deleting []string
	for _, record := range recordsInNamespace().Items {
		if record.Metadata.DeletionTimestamp != "" {
			deleting = append(deleting, record.Metadata.Name)
		}
	}
	return deleting
}

// heldBecause returns what one record says about why it is still here, found by
// the readable half of its name.
func heldBecause(name string) string {
	for _, record := range recordsInNamespace().Items {
		if !strings.Contains(record.Metadata.Name, name) {
			continue
		}
		for _, condition := range record.Status.Conditions {
			if condition.Type == "Ready" {
				return condition.Message
			}
		}
	}
	return ""
}

// identityNames lists the recorded identities in a namespace.
func identityNames(namespace string) string {
	cmd := exec.Command("kubectl", "get", "databricksserviceaccount", "-n", namespace,
		"-o", "jsonpath={.items[*].metadata.name}")
	out, err := utils.Run(cmd)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}
