//go:build e2e || sharing
// +build e2e sharing

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

// Package e2e is the only suite that can say the thing this operator claims.
//
// Every other layer stops at a boundary. The cluster suite runs on kind, whose
// issuer Databricks cannot fetch, so it reaches the first call to Databricks and
// goes no further. The Databricks suite has a real account and no cluster, so it
// asks what Databricks does with a policy nobody can present a token for -- its
// issuer is GitHub's, chosen precisely so that no token anywhere satisfies it.
//
// The claim is one sentence and it spans both: a ServiceAccount's own token is
// how a Databricks service principal proves itself. Nothing that holds one side
// can check it. This suite holds both, and the assertion that matters is made
// from inside a pod, with the token a kubelet put there, against the account.
package e2e

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/uuid"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	"github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
)

// What this needs, and it needs all of it. There is no default for any of them:
// three name somebody's Databricks account and somebody's cluster, and the
// fourth is where an operator was installed by hand.
const (
	envHost      = "DATABRICKS_HOST"
	envAccountID = "DATABRICKS_ACCOUNT_ID"
	envContext   = "E2E_KUBE_CONTEXT"
	envOperator  = "E2E_OPERATOR_NAMESPACE"
)

var (
	// host and accountID name the account, and are read once so that every use
	// of them is the same string the clients were built from.
	host, accountID string

	// kubeContext is named rather than taken from the current one.
	//
	// This suite creates namespaces, annotates ServiceAccounts and deletes both,
	// and every one of those succeeds quietly on a cluster it reached by
	// accident. A kubeconfig's current context is moved by anything on the
	// machine, so it is not an answer to "which cluster"; being told is.
	kubeContext string

	// operatorNamespace is where somebody already installed the operator. This
	// suite does not install one: the operator authenticates as a service
	// principal whose federation policy names its own subject, and creating that
	// is the bootstrap a person does once. What is tested here is the installed
	// thing.
	operatorNamespace string

	// clients reads the account directly, to check what the operator says it did
	// against what is there. Without it this suite could only ask the operator
	// whether it had done its job.
	clients databricks.Clients
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting databricks-service-principal-operator e2e suite\n")
	RunSpecs(t, "e2e suite")
}

// Synchronized because this suite runs its containers in parallel, and the two
// halves of getting ready are different questions. Emptying the list of served
// namespaces is one act for the whole run and belongs to one process; reading
// the inputs and building clients is per-process state that each one needs.
var _ = SynchronizedBeforeSuite(func() []byte {
	readInputs()
	By("starting from a served-namespace list that is empty and exists")
	// Exists, because what each container adds to it is a JSON patch append,
	// and an append has nowhere to go when the field is absent.
	_, err := kubectl("-n", operatorNamespace, "patch", "databricksaccount", accountName,
		"--type", "merge", "-p", `{"spec":{"namespaces":[]}}`)
	Expect(err).NotTo(HaveOccurred(), "clearing what the operator serves")
	return nil
}, func(_ []byte) {
	readInputs()

	By("reaching the Databricks account as the operator")
	// As the operator, not as whoever ran this. The identity is already in the
	// cluster and already trusted by the account: minting a token for the
	// operator's ServiceAccount with the audience its projected volume asks for
	// produces the same string kubelet writes into its pod, and the federation
	// policy that lets the operator exchange it does not care which of the two
	// presented it.
	//
	// Two things follow, and both are why this is not merely tidier. The suite
	// needs no human logged in, so it runs the same from a laptop and from CI,
	// where there is nobody to open a browser -- authenticating as whoever ran
	// it meant an expired refresh token in somebody's keyring stopped the suite
	// with an error about credentials rather than about this operator. And what
	// it can reach is exactly what the operator can reach, so a spec cannot pass
	// here on an admin's permissions and fail in a cluster that holds only the
	// operator's.
	config := databricks.Config{
		AccountHost:       host,
		AccountID:         accountID,
		ClientID:          operatorClientID(),
		OIDCTokenFilepath: operatorTokenFile(),
		TokenAudience:     orphanAudience,
	}
	var err error
	clients, err = databricks.New(config)
	Expect(err).NotTo(HaveOccurred(), "building clients for the account")

	By("checking the operator is installed and reaching Databricks")
	// Asserted here rather than discovered spec by spec. Every failure below
	// would otherwise have this as its first candidate cause, and it is one
	// question with one answer.
	Eventually(func() string {
		return kubectlOut("-n", operatorNamespace, "get", "databricksaccount", accountName,
			"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
	}, 2*time.Minute, 5*time.Second).Should(Equal("True"),
		"the DatabricksAccount in %s is not Ready, so nothing here can be issued. This suite "+
			"tests an installed operator; it does not install one", operatorNamespace)
})

// Left as it was found, once, after every process has finished with it.
var _ = SynchronizedAfterSuite(func() {}, func() {
	_, _ = kubectl("-n", operatorNamespace, "patch", "databricksaccount", accountName,
		"--type", "merge", "-p", `{"spec":{"namespaces":[]}}`)
})

// readInputs takes what this run was given, and refuses without any of it.
//
// Failing rather than skipping. This suite is built behind a tag and run by a
// target that refuses without these, so reaching here means somebody asked for
// it by name -- and "you asked for the end-to-end tests and got none" cannot be
// a run that says ok.
func readInputs() {
	for name, into := range map[string]*string{
		envContext:   &kubeContext,
		envOperator:  &operatorNamespace,
		envHost:      &host,
		envAccountID: &accountID,
	} {
		*into = strings.TrimSpace(os.Getenv(name))
		Expect(*into).NotTo(BeEmpty(), "%s is not set: this suite needs a real cluster with the "+
			"operator installed on it and a Databricks account, and will not guess either", name)
	}
}

// accountName is what the operator's DatabricksAccount is called. It is half of
// how a ServiceAccount names an operator, so the suite writes it the same way a
// tenant does rather than assembling the reference differently.
const accountName = "databricks-account"

// operatorRef is how a ServiceAccount names this operator: the value every
// service-principal annotation key here carries, and the profile name of the
// identity asked for by the bare key.
func operatorRef() string { return operatorNamespace + "/" + accountName }

// unnamed is what a ServiceAccount asks for by writing the bare key: this
// operator's only identity, which is what nearly every ServiceAccount wants.
const unnamed = ""

// unnamedProfile is what that identity is called everywhere afterwards -- on
// the DatabricksServiceAccount, in the pod's configuration, and in the path its
// token is at. Having no name of its own it is named by what its key held, so
// this is operatorRef by rule rather than by coincidence.
func unnamedProfile() string { return operatorRef() }

// kubectl runs one command against the cluster this run was given.
//
// The context is passed on every call rather than selected once. Selecting it
// would change the kubeconfig for everything else on the machine, and this suite
// is not entitled to do that to whoever ran it.
func kubectl(args ...string) (string, error) {
	cmd := exec.Command("kubectl", append([]string{"--context=" + kubeContext}, args...)...)
	out, err := cmd.CombinedOutput()
	_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s\n", strings.Join(args, " "))
	if err != nil {
		return string(out), fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, out)
	}
	return string(out), nil
}

// kubectlOut is kubectl for a read whose failure is "not yet", which is what
// every Eventually here is waiting through.
func kubectlOut(args ...string) string {
	out, err := kubectl(args...)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// apply writes a manifest, so that a spec says the same thing whether or not
// what it describes is already there.
func apply(manifest string) {
	cmd := exec.Command("kubectl", "--context="+kubeContext, "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "applying:\n%s\n%s", manifest, out)
}

// databricksHost and databricksAccountID are what the pod is told to talk to.
//
// Passed in rather than read inside the pod. A workload knows its own
// destination and this operator deliberately does not tell it -- so a suite that
// found the host in the pod would be testing something the design says is not
// there.
func databricksHost() string      { return host }
func databricksAccountID() string { return accountID }

// exists asks Databricks whether a service principal is there.
func exists(servicePrincipalID string) bool {
	// Named rather than assumed. Every caller here got this id from a
	// DatabricksServiceAccount that may not carry one yet, and "is it there" asked
	// of nothing is a question with no true answer -- so a spec that asked it
	// would pass or fail on something it never checked.
	ExpectWithOffset(1, servicePrincipalID).NotTo(BeEmpty(),
		"asked whether a service principal is in Databricks without having one to ask about")
	there, err := clients.ServicePrincipalExists(context.Background(), servicePrincipalID)
	Expect(err).NotTo(HaveOccurred(), "asking Databricks about service principal %s", servicePrincipalID)
	return there
}

// exchangeInPod does inside the pod what a workload's SDK does: reads the
// configuration, finds the profile, reads the token file it names, exchanges it,
// and reports who Databricks says it now is.
//
// Done with a shell and curl rather than an SDK because what is being tested is
// the file and the token, not a client library. Everything it needs it reads out
// of the configuration the webhook wrote -- the client id and the token's path
// are not passed in, so a configuration naming the wrong path or the wrong
// client fails here rather than being worked around.
//
// The token path and the audience are read twice, under the name Go declares for
// each and the name Python declares, and the pair has to agree. Only one of the
// two is exercised by the exchange below, so without this a profile that served
// one language and not the other would pass here on a real cluster.
//
// Who it became is read from the access token's own `sub` claim rather than
// asked. There is no "who am I" at account level: SCIM's Me is 405 there, and
// the workspace one answers "Unable to load OAuth Config" for an account token.
// The claim is the better answer anyway -- it is Databricks stating whose token
// this is, in the token, rather than a second call that could be authorised
// differently.
func exchangeInPod(team, pod, profile string) string {
	script := `set -e
cfg="$WORKLOAD_IDENTITY_DATABRICKS_CONFIG_FILE"
section=$(awk -v p="[$1]" '$0==p{f=1;next} /^\[/{f=0} f' "$cfg")
client=$(echo "$section" | sed -n 's/^client_id *= *//p')
tokenfile=$(echo "$section" | sed -n 's/^databricks_id_token_filepath *= *//p')
audience=$(echo "$section" | sed -n 's/^audience *= *//p')
pytokenfile=$(echo "$section" | sed -n 's/^oidc_token_filepath *= *//p')
pyaudience=$(echo "$section" | sed -n 's/^token_audience *= *//p')
test -n "$client"    || { echo "no client_id in profile $1"; exit 1; }
test -n "$tokenfile" || { echo "no databricks_id_token_filepath in profile $1"; exit 1; }
test -r "$tokenfile" || { echo "the profile names $tokenfile and there is no such file"; exit 1; }
test -n "$audience"  || { echo "no audience in profile $1"; exit 1; }
test "$pytokenfile" = "$tokenfile" || { echo "the token path is $tokenfile under the name Go reads and $pytokenfile under the name Python reads"; exit 1; }
test "$pyaudience"  = "$audience"  || { echo "the audience is $audience under the name Go reads and $pyaudience under the name Python reads"; exit 1; }
access=$(curl -sS -X POST "$2/oidc/accounts/$3/v1/token" \
  -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \
  -d subject_token_type=urn:ietf:params:oauth:token-type:jwt \
  -d scope=all-apis \
  -d "client_id=$client" \
  --data-urlencode "subject_token@$tokenfile" \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
test -n "$access" || { echo "the exchange returned no access token for audience $audience"; exit 1; }
payload=$(echo "$access" | cut -d. -f2)
while [ $(( ${#payload} % 4 )) -ne 0 ]; do payload="$payload="; done
echo "$payload" | tr '_-' '/+' | base64 -d 2>/dev/null \
  | sed -n 's/.*"sub":"\([^"]*\)".*/\1/p'`

	out, err := kubectl("-n", team, "exec", pod, "--", "sh", "-c", script, "sh",
		profile, databricksHost(), databricksAccountID())
	Expect(err).NotTo(HaveOccurred(),
		"exchanging the token for profile %q inside the pod", profile)
	return strings.TrimSpace(out)
}

// removeTeam takes the tenant namespace away and waits, so that a create
// straight afterwards does not race a terminating one.
func removeTeam(team string) {
	_, _ = kubectl("delete", "namespace", team, "--ignore-not-found", "--wait=true", "--timeout=3m")
}

// serve is the platform team's half: the namespaces this operator will act in.
//
// Added rather than assigned. The list is one field on one object that every
// container in this suite shares, and the containers run in parallel -- so an
// assignment computed from a read loses whatever another container added between
// the read and the write, and what is lost reads as that container's operator
// having refused to serve it.
//
// A JSON patch append is one operation at the API server, so there is nothing to
// lose. The field is a set, so a duplicate is refused rather than silently
// accumulated. Nothing removes an entry: a container that has finished has
// deleted its namespace, and serving a namespace that is not there costs the
// operator nothing. The list is emptied once, after every process is done.
func serve(name string) {
	_, err := kubectl("-n", operatorNamespace, "patch", "databricksaccount", accountName,
		"--type", "json", "-p",
		fmt.Sprintf(`[{"op":"add","path":"/spec/namespaces/-","value":%q}]`, name))
	Expect(err).NotTo(HaveOccurred(), "declaring that this operator serves %s", name)
}

// applyTeam creates the tenant's half: a namespace enabled for both minting and
// injection, and a ServiceAccount asking this operator for the named identities,
// one annotation key each.
func applyTeam(team, serviceAccount string, identities ...string) {
	asks := make(map[string]string, len(identities))
	for _, identity := range identities {
		asks[identity] = operatorRef()
	}
	applyAsking(team, serviceAccount, asks)
}

// applyAsking is applyTeam for a ServiceAccount whose identities do not all ask
// the same operator: each key carries its own, so one ServiceAccount can ask
// two.
//
// Written in sorted order because a map has none. A helper that produced a
// different manifest on each call would leave what a spec applied depending on a
// map iteration.
func applyAsking(team, serviceAccount string, asks map[string]string) {
	var annotations strings.Builder
	for _, identity := range slices.Sorted(maps.Keys(asks)) {
		fmt.Fprintf(&annotations, "    %q: %q\n",
			dbxv1alpha1.ServicePrincipalAnnotationFor(identity), asks[identity])
	}

	apply(fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %s
  labels:
    databricks.workload-identity.io/mint: enabled
    databricks.workload-identity.io/inject: enabled
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: %s
  namespace: %s
  annotations:
%s`, team, serviceAccount, team, annotations.String()))
}

// ask writes one identity's key on a ServiceAccount that is already there.
//
// Overwriting, because correcting a value that could not be read is the case
// this exists for, and it is an edit to one key and not to the others.
func ask(team, account, identity, operator string) {
	_, err := kubectl("-n", team, "annotate", "serviceaccount", account, "--overwrite",
		dbxv1alpha1.ServicePrincipalAnnotationFor(identity)+"="+operator)
	Expect(err).NotTo(HaveOccurred(),
		"asking %s for identity %q of %s", account, identity, operator)
}

// withdraw takes one identity's key away, which is the only edit that destroys
// an identity.
//
// Deleting a key rather than rewriting a list is the whole of what this suite
// was rewritten for. A key that is gone says the identity is no longer wanted; a
// key that is there and cannot be read says nothing at all, and the two used to
// be the same input.
func withdraw(team, account, identity string) {
	_, err := kubectl("-n", team, "annotate", "serviceaccount", account,
		dbxv1alpha1.ServicePrincipalAnnotationFor(identity)+"-")
	Expect(err).NotTo(HaveOccurred(), "withdrawing identity %q from %s", identity, account)
}

// runPod creates a pod that stays up long enough to be looked inside, and that
// carries a shell and curl so the exchange can be made from where the token is.
func runPod(team, name, serviceAccount string) {
	_, _ = kubectl("delete", "pod", name, "-n", team, "--ignore-not-found",
		"--force", "--grace-period=0")
	apply(fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  serviceAccountName: %s
  restartPolicy: Never
  containers:
  - name: app
    image: curlimages/curl:latest
    command: ["sleep", "900"]
`, name, team, serviceAccount))
}

// Enough of the object to answer what the operator says it issued. The suite
// reads it back as JSON, so a field nothing here asserts on is left out.
type databricksServiceAccount struct {
	Status struct {
		Identities []struct {
			Profile                   string `json:"profile"`
			Operator                  string `json:"operator"`
			ClientID                  string `json:"clientId"`
			ServicePrincipalID        string `json:"servicePrincipalId"`
			RemovedServicePrincipalID string `json:"removedServicePrincipalId"`
		} `json:"identities"`
	} `json:"status"`
}

func read(team, serviceAccount string) databricksServiceAccount {
	out := kubectlOut("-n", team, "get", "databricksserviceaccount", serviceAccount, "-o", "json")
	var p databricksServiceAccount
	if out == "" || json.Unmarshal([]byte(out), &p) != nil {
		return databricksServiceAccount{}
	}
	return p
}

// requestsOn lists the identities the DatabricksServiceAccount carries, by the
// profile name each is keyed under.
//
// Not "as the asker wrote them": a named identity is keyed by the name in its
// key, but the unnamed one is keyed by the operator's reference, which nobody
// wrote anywhere.
func requestsOn(team, serviceAccount string) []string {
	var requests []string
	for _, identity := range read(team, serviceAccount).Status.Identities {
		requests = append(requests, identity.Profile)
	}
	return requests
}

// removedIDOf is the id of a service principal this operator created and that is
// no longer in Databricks. It is kept rather than a flag because it is what an
// audit log is searched with: a flag says something is wrong, an id says whom to
// ask.
func removedIDOf(team, account, profile string) string {
	for _, identity := range read(team, account).Status.Identities {
		if identity.Profile == profile {
			return identity.RemovedServicePrincipalID
		}
	}
	return ""
}

// operatorOf is the operator an entry says issued it.
//
// Worth asking now that a named identity's key is its name alone: the entry for
// "reader" carries no operator anywhere in its key, so this field is the only
// thing on the object that says whose the entry is.
func operatorOf(team, account, profile string) string {
	for _, identity := range read(team, account).Status.Identities {
		if identity.Profile == profile {
			return identity.Operator
		}
	}
	return ""
}

// conditionOn is the reason one identity gives for the state it is in.
func conditionOn(team, account, request, condition string) string {
	return kubectlOut("-n", team, "get", "databricksserviceaccount", account, "-o",
		fmt.Sprintf("jsonpath={.status.identities[?(@.request==%q)].conditions[?(@.type==%q)].reason}",
			request, condition))
}

// clientIDOf is what the workload presents, and what a grant to this identity
// records.
func clientIDOf(team, account, profile string) string {
	for _, identity := range read(team, account).Status.Identities {
		if identity.Profile == profile {
			return identity.ClientID
		}
	}
	return ""
}

// servicePrincipalIDOf is what federation policies hang off, and what the
// account is asked about.
func servicePrincipalIDOf(team, account, profile string) string {
	for _, identity := range read(team, account).Status.Identities {
		if identity.Profile == profile {
			return identity.ServicePrincipalID
		}
	}
	return ""
}

// destroyIfLeft removes a service principal the suite made and the operator did
// not, which is what a failed run leaves behind: one in a real account that
// nothing in the cluster names.
func destroyIfLeft(servicePrincipalID string) {
	if servicePrincipalID == "" {
		return
	}
	// Both errors are reported rather than swallowed. This ran once with an
	// expired token, read the error as "nothing to clean up", and left a real
	// identity in a real account with nothing anywhere naming it -- the outcome
	// this exists to prevent, arrived at by this.
	there, err := clients.ServicePrincipalExists(context.Background(), servicePrincipalID)
	Expect(err).NotTo(HaveOccurred(),
		"could not find out whether service principal %s is still there, so it may be left behind",
		servicePrincipalID)
	if !there {
		return
	}
	_, _ = fmt.Fprintf(GinkgoWriter,
		"removing service principal %s, which this run made and left behind\n", servicePrincipalID)
	Expect(clients.DeleteServicePrincipal(context.Background(), servicePrincipalID)).To(Succeed(),
		"service principal %s was made by this run and is still in the account", servicePrincipalID)
}

// makeOrphan puts an identity in the account that trusts this namespace and name
// and that nothing in the cluster records.
//
// Built through the operator's own Issuing so that the marker, the display name
// and the subject are the ones the operator writes. The uid is invented: it is
// what makes this an identity issued to a ServiceAccount that no longer exists,
// which is the whole of what an orphan is.
func makeOrphan(team, serviceAccount string) (id, clientID string) {
	issuing := databricks.Issuing{
		Issuer:            clusterIssuer(),
		Namespace:         team,
		Name:              serviceAccount,
		ServiceAccountUID: string(uuid.NewUUID()),
		Operator:          operatorRef(),
	}

	id, clientID, err := clients.CreateServicePrincipal(context.Background(), issuing)
	Expect(err).NotTo(HaveOccurred(), "creating the orphan")
	Expect(clients.EnsureFederationPolicy(context.Background(), id,
		issuing.Issuer, databricks.SubjectFor(team, serviceAccount), orphanAudience)).To(Succeed(),
		"the orphan has no federation policy, so it trusts nothing and is not the hazard")
	_, _ = fmt.Fprintf(GinkgoWriter,
		"orphan %s (%s) now trusts %s\n", id, clientID, databricks.SubjectFor(team, serviceAccount))
	return id, clientID
}

// orphanAudience is what a projected token in this cluster carries. The operator
// reads it off its own token rather than being told, and the orphan has to name
// the same one or it is trusted by nothing.
const orphanAudience = "databricks"

// operatorClientID is the applicationId the operator presents, read from the
// account it was installed against rather than passed in.
//
// Read rather than configured because it is already written down: the operator
// cannot work without it, so a copy given to this suite could only ever be a
// second place for it to be wrong.
func operatorClientID() string {
	id := kubectlOut("-n", operatorNamespace, "get", "databricksaccount", accountName,
		"-o", "jsonpath={.spec.clientId}")
	Expect(id).NotTo(BeEmpty(),
		"the DatabricksAccount in %s names no clientId, so there is no identity for this suite "+
			"to act as", operatorNamespace)
	return id
}

// operatorTokenFile writes a token for the operator's ServiceAccount and returns
// the path, which is what the SDK's file-oidc source reads on every exchange.
//
// One token for the run, minted for longer than the run may take. kubelet
// rewrites the operator's own file before it expires; nothing rewrites this one,
// so its lifetime has to cover the suite instead. The API server caps what it
// will issue, and a cap shorter than the run would surface as an exchange
// failing partway through -- so the ceiling is stated here rather than found.
func operatorTokenFile() string {
	token, err := kubectl("-n", operatorNamespace, "create", "token",
		"databricks-service-principal-operator-controller-manager",
		"--audience="+orphanAudience, "--duration=3600s")
	Expect(err).NotTo(HaveOccurred(), "minting a token to act as the operator with")

	path := filepath.Join(GinkgoT().TempDir(), "operator-token")
	Expect(os.WriteFile(path, []byte(strings.TrimSpace(token)), 0o600)).To(Succeed(),
		"writing the token the SDK reads")
	return path
}

// clusterIssuer is what this cluster signs ServiceAccount tokens with, read from
// a token rather than configured.
//
// The same value the operator uses, arrived at the same way: a value read from
// what is actually presented cannot disagree with what is presented.
func clusterIssuer() string {
	token, err := kubectl("-n", operatorNamespace, "create", "token",
		"databricks-service-principal-operator-controller-manager",
		"--audience="+orphanAudience, "--duration=600s")
	Expect(err).NotTo(HaveOccurred(), "minting a token to read this cluster's issuer from")

	parts := strings.Split(strings.TrimSpace(token), ".")
	Expect(parts).To(HaveLen(3), "that is not a token")
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	Expect(err).NotTo(HaveOccurred(), "decoding the token's claims")

	var claims struct {
		Issuer string `json:"iss"`
	}
	Expect(json.Unmarshal(payload, &claims)).To(Succeed())
	Expect(claims.Issuer).NotTo(BeEmpty(), "the token names no issuer")
	return claims.Issuer
}
