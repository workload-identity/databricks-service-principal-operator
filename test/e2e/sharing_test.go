//go:build sharing
// +build sharing

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

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/uuid"

	"github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

// One ServiceAccount asking two operators, which is the case the whole shape was
// rebuilt around: a pod binds one ServiceAccount, and a workload may need an
// identity in more than one Databricks account.
//
// One namespace, one ServiceAccount, both operators serving it, and one
// annotation key each. Everything Databricks matches on is then shared -- the
// same subject, the same uid, the same issuer -- so nothing in the account keeps
// the two apart except what each operator writes about itself. Two teams in two
// namespaces would not show that: their uids differ, so they would stay apart
// however either operator behaved.
//
// The key asking the other operator is a named one, which is what makes this the
// case an operator gets wrong. Its entry on the projection is keyed "other" and
// names no operator anywhere in that key, so an operator looking for its own
// reference inside the key finds nothing of its own -- and what it does with the
// entries it does not recognise is either destroying a neighbour's identity or
// leaving its own behind.
var _ = Describe("One ServiceAccount asking two operators", Ordered, Serial, func() {
	const (
		team = "e2e-sharing"
		name = "etl"

		// What the other operator's identity is called. The operator this suite
		// was pointed at is asked by the bare key, so its identity is the unnamed
		// one and there is no name to choose for it.
		theirIdentity = "other"
	)
	other := &otherOperator{namespace: "dbxsp-two-system", prefix: "dbxsp-two-"}
	var mine, theirs, myProfile, myID, theirID string

	BeforeAll(func() {
		other.uninstall()
		removeTeam(team)

		other.install()
		mine = operatorRef()
		theirs = other.namespace + "/" + accountName
		myProfile = unnamedProfile()

		serve(team)
		serveOn(other.namespace, team)
		applyAsking(team, name, map[string]string{unnamed: mine, theirIdentity: theirs})
	})

	AfterAll(func() {
		removeTeam(team)
		destroyIfLeft(myID)
		destroyIfLeft(theirID)
		other.uninstall()
	})

	It("gets one identity from each, and they are two", func() {
		Eventually(func() string {
			return servicePrincipalIDOf(team, name, myProfile)
		}, 5*time.Minute, 5*time.Second).ShouldNot(BeEmpty(), "this operator issued nothing")
		Eventually(func() string {
			return servicePrincipalIDOf(team, name, theirIdentity)
		}, 5*time.Minute, 5*time.Second).ShouldNot(BeEmpty(), "the other operator issued nothing")

		myID = servicePrincipalIDOf(team, name, myProfile)
		theirID = servicePrincipalIDOf(team, name, theirIdentity)

		Expect(myID).NotTo(Equal(theirID),
			"one ServiceAccount asked two operators and both point at the same service "+
				"principal: one of them looked its own work up, found the other's, and took it. "+
				"The two were issued to one subject, in one account, from one cluster, so the "+
				"marker is what has to tell them apart")
		Expect(exists(myID)).To(BeTrue())
		Expect(exists(theirID)).To(BeTrue())

		// The property, not a consequence of it. Both operators start at once
		// and both look before they create, so with one marker between them they
		// each find nothing and each make one -- two service principals a later
		// lookup cannot tell apart, rather than one adopting the other. Which of
		// those happens is a race; that the markers differ is not.
		Expect(markerOf(myID)).NotTo(Equal(markerOf(theirID)),
			"both service principals carry the same marker, so neither operator can tell its "+
				"own work from the other's: they share the subject, the uid and the issuer that "+
				"Databricks matches on")
	})

	It("says which operator issued each entry, since the key no longer does", func() {
		// The projection is one object two operators write, and a named
		// identity's key is its name alone. So "whose is this entry" is answered
		// by this field or by nothing: an operator answering it from the key
		// would find its own reference in its unnamed entries and in none of its
		// named ones.
		Expect(operatorOf(team, name, myProfile)).To(Equal(mine),
			"the entry this operator issued does not say so")
		Expect(operatorOf(team, name, theirIdentity)).To(Equal(theirs),
			"the entry the operator in %s issued does not name it, so this operator has no way "+
				"to tell it from one of its own", other.namespace)
	})

	It("keeps the other's when one of the two keys is deleted", func() {
		// The same subject stays, and so does the other operator's identity for
		// it. An operator that cannot tell its own work from its neighbour's
		// destroys both here, and a key is still asking for one of them.
		withdraw(team, name, unnamed)

		Eventually(func() bool {
			return exists(myID)
		}, 5*time.Minute, 5*time.Second).Should(BeFalse(),
			"this operator did not destroy the identity whose key was deleted")

		Consistently(func() bool {
			return exists(theirID)
		}, 60*time.Second, 10*time.Second).Should(BeTrue(),
			"the other operator's identity went when this one's key was deleted, and the "+
				"ServiceAccount still asks for it")

		Eventually(func() []string {
			return requestsOn(team, name)
		}, 2*time.Minute, 5*time.Second).Should(ConsistOf(theirIdentity),
			"the projection does not carry exactly the entry that is still asked for: an "+
				"operator either took the other's entry off it, or left its own on it after "+
				"destroying what the entry describes")
	})
})

// serveOn declares a namespace served by an operator other than the one this
// suite was pointed at.
func serveOn(operator, name string) {
	_, err := kubectl("-n", operator, "patch", "databricksaccount", accountName,
		"--type", "json", "-p",
		fmt.Sprintf(`[{"op":"add","path":"/spec/namespaces/-","value":%q}]`, name))
	Expect(err).NotTo(HaveOccurred(), "declaring that the operator in %s serves %s", operator, name)
}

// otherOperator is another platform team's install of this operator: its own
// namespace, its own DatabricksAccount, the same Databricks account underneath.
//
// Built as an overlay written when the suite runs rather than one kept in the
// repository. The overlay is config/default plus exactly three overrides -- the
// namespace, the prefix every cluster-scoped name is built from, and the image --
// so it is by construction the same install as the one already there. A
// checked-in overlay would drift from config/default the first time that changed,
// and then a failure here could mean "two operators interfere" or "the other one
// was installed differently", which is one meaning too many.
type otherOperator struct {
	namespace          string
	prefix             string
	servicePrincipalID string
}

// install writes the overlay, applies it, gives the operator an identity in
// Databricks, and does not return until it is reconciling.
func (o *otherOperator) install() {
	root, err := filepath.Abs("../..")
	Expect(err).NotTo(HaveOccurred())

	// Under the repository rather than in the system temp directory: kustomize
	// refuses an absolute path in `resources`, so the overlay has to be able to
	// name config/default relatively. bin/ is where this repository already puts
	// what it does not track.
	dir, err := os.MkdirTemp(filepath.Join(root, "bin"), "other-operator-")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = os.RemoveAll(dir) })

	image := kubectlOut("-n", operatorNamespace, "get", "deployment",
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].spec.template.spec.containers[0].image}")
	Expect(image).NotTo(BeEmpty(), "the installed operator has no image to copy")

	overlay := fmt.Sprintf(`resources:
- ../../config/default
namespace: %s
namePrefix: %s
images:
- name: controller
  newName: %s
  newTag: %s
`, o.namespace, o.prefix,
		strings.Split(image, ":")[0], strings.Split(image, ":")[1])
	Expect(os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(overlay), 0o600)).To(Succeed())

	By("installing another operator in " + o.namespace)
	built, err := exec.Command(filepath.Join(root, "bin", "kustomize"), "build", dir).Output()
	Expect(err).NotTo(HaveOccurred(), "building the other operator's manifests")

	apply := exec.Command("kubectl", "--context="+kubeContext, "apply", "-f", "-")
	apply.Stdin = strings.NewReader(string(built))
	out, err := apply.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "applying the other operator: %s", out)

	// Read off the deployment, not computed. The prefix in the overlay is
	// prepended to the one config/default already carries, so the name is
	// <this prefix><that prefix>controller-manager -- and a guess at it produces
	// a federation policy for a subject no token will ever have, which surfaces
	// as the operator being unable to reach Databricks at all.
	account := kubectlOut("-n", o.namespace, "get", "deployment",
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].spec.template.spec.serviceAccountName}")
	Expect(account).NotTo(BeEmpty(), "the other operator's deployment names no ServiceAccount")

	o.grantItself(account)

	_, err = kubectl("-n", o.namespace, "wait", "--for=condition=Available",
		"deployment", "-l", "control-plane=controller-manager", "--timeout=5m")
	Expect(err).NotTo(HaveOccurred(), "the other operator never became available")

	Eventually(func() string {
		return kubectlOut("-n", o.namespace, "get", "databricksaccount", accountName,
			"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
	}, 3*time.Minute, 5*time.Second).Should(Equal("True"),
		"the other operator never reached the account it was pointed at")
}

// grantItself gives it what the operator already there was given by hand: a
// service principal, a federation policy naming its own subject, and a
// DatabricksAccount pointing at both.
func (o *otherOperator) grantItself(account string) {
	subject := databricks.SubjectFor(o.namespace, account)
	issuing := databricks.Issuing{
		Issuer:            clusterIssuer(),
		Namespace:         o.namespace,
		Name:              account,
		ServiceAccountUID: string(uuid.NewUUID()),
		Operator:          o.namespace + "/" + accountName,
	}
	id, clientID, err := clients.CreateServicePrincipal(context.Background(), issuing)
	Expect(err).NotTo(HaveOccurred(), "creating the other operator's own service principal")
	o.servicePrincipalID = id
	Expect(clients.EnsureFederationPolicy(context.Background(), id,
		issuing.Issuer, subject, orphanAudience)).To(Succeed())

	// The same grant the first operator was given by hand. Without it this one
	// authenticates and can create nothing, and every spec below would fail
	// saying the operator was denied rather than saying anything about two
	// operators.
	grantAccountAdmin(id)

	apply(fmt.Sprintf(`apiVersion: databricks.workload-identity.io/v1alpha1
kind: DatabricksAccount
metadata:
  name: %s
  namespace: %s
spec:
  host: %s
  accountId: %s
  clientId: %s
  namespaces: []
`, accountName, o.namespace, host, accountID, clientID))
}

// uninstall removes everything this install put on the cluster, including
// the twelve cluster-scoped objects that a namespace deletion does not reach,
// and the identity it was given.
func (o *otherOperator) uninstall() {
	_, _ = kubectl("delete", "namespace", o.namespace, "--ignore-not-found",
		"--wait=true", "--timeout=3m")
	for _, kind := range []string{"clusterrole", "clusterrolebinding",
		"mutatingwebhookconfiguration", "validatingwebhookconfiguration"} {
		names := strings.Fields(kubectlOut("get", kind, "-o", "jsonpath={.items[*].metadata.name}"))
		for _, name := range names {
			if strings.HasPrefix(name, o.prefix) {
				_, _ = kubectl("delete", kind, name, "--ignore-not-found")
			}
		}
	}
	destroyIfLeft(o.servicePrincipalID)
}

// grantAccountAdmin gives a service principal the role the operator needs.
//
// Read off the installed operator rather than assumed: its own service principal
// carries roles: [{type: direct, value: account_admin}], and that is the whole of
// what this operator is entitled to.
func grantAccountAdmin(servicePrincipalID string) {
	body := `{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],` +
		`"Operations":[{"op":"add","path":"roles","value":[{"value":"account_admin"}]}]}`
	request, err := http.NewRequest(http.MethodPatch,
		fmt.Sprintf("%s/api/2.0/accounts/%s/scim/v2/ServicePrincipals/%s",
			host, accountID, servicePrincipalID), strings.NewReader(body))
	Expect(err).NotTo(HaveOccurred())
	// Signed by the same client the rest of this suite acts through, which is
	// the operator. A bearer token read from the environment was a second
	// credential to get right, and unset it sends "Bearer " and the account
	// answers 401 -- a message about credentials, in a suite about two operators.
	Expect(clients.Account().Config.Authenticate(request)).To(Succeed(),
		"signing the request as the operator")
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	Expect(err).NotTo(HaveOccurred(), "granting account_admin to %s", servicePrincipalID)
	defer func() { _ = response.Body.Close() }()
	said, _ := io.ReadAll(response.Body)
	Expect(response.StatusCode).To(BeNumerically("<", 300),
		"granting account_admin to %s: %d %s", servicePrincipalID, response.StatusCode, said)
}

// markerOf is what an operator wrote on a service principal to recognise its own
// work, read back off the account.
//
// Read over HTTP because the SDK's account service principal does not carry
// externalId, and this is the field the whole "everything from this cluster"
// question rests on.
func markerOf(servicePrincipalID string) string {
	request, err := http.NewRequest(http.MethodGet,
		fmt.Sprintf("%s/api/2.0/accounts/%s/scim/v2/ServicePrincipals/%s",
			host, accountID, servicePrincipalID), nil)
	Expect(err).NotTo(HaveOccurred())
	// Signed by the same client the rest of this suite acts through, which is
	// the operator. A bearer token read from the environment was a second
	// credential to get right, and unset it sends "Bearer " and the account
	// answers 401 -- a message about credentials, in a suite about two operators.
	Expect(clients.Account().Config.Authenticate(request)).To(Succeed(),
		"signing the request as the operator")

	response, err := http.DefaultClient.Do(request)
	Expect(err).NotTo(HaveOccurred(), "reading the marker on %s", servicePrincipalID)
	defer func() { _ = response.Body.Close() }()
	said, err := io.ReadAll(response.Body)
	Expect(err).NotTo(HaveOccurred())
	Expect(response.StatusCode).To(BeNumerically("<", 300),
		"reading the marker on %s: %d %s", servicePrincipalID, response.StatusCode, said)

	var principal struct {
		ExternalID  string `json:"externalId"`
		DisplayName string `json:"displayName"`
	}
	Expect(json.Unmarshal(said, &principal)).To(Succeed())
	// The whole of what the account says about it. A service principal with no
	// marker is not a case this operator can produce -- every create sets one --
	// so if it happens the useful thing is what else is true of it.
	Expect(principal.ExternalID).NotTo(BeEmpty(),
		"service principal %s carries no marker at all. Databricks says: %s",
		servicePrincipalID, said)
	return principal.ExternalID
}
