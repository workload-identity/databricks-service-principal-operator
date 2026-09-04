//go:build e2e
// +build e2e

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

package e2e

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The whole chain, once, on the two identities a workload asks for.
//
// Two rather than one, because two is the shape the design was rebuilt around
// and one would pass without saying whether the second identity is a second
// identity. Everything downstream -- the record's name, the marker, the profile
// in the pod, the withdrawal -- differs between them only in the name the asker
// gave, so a pass that reads the annotation without reading that name gets both
// wrong in a way one identity cannot show.
var _ = Describe("A workload reaching Databricks with no credential", Ordered, func() {
	const (
		team           = "e2e-workload"
		serviceAccount = "etl"
		runner         = "runner"

		// The names the asker gave, which are the annotation key suffixes, the
		// profile names a workload passes to the SDK and the keys of the entries on
		// the DatabricksServiceAccount. One string throughout, which is the promise
		// the naming rule makes.
		reader = "reader"
		writer = "writer"
	)
	var (
		// What Databricks said, kept so the last spec can ask about it after the
		// records that named it are gone. That is the point of the last spec:
		// the identity has to be gone from Databricks, and after withdrawal
		// nothing in the cluster remembers its id.
		issuedIDs = map[string]string{}
	)

	BeforeAll(func() {
		removeTeam(team)
		serve(team)
		applyTeam(team, serviceAccount, reader, writer)

		// Waited for and recorded here rather than in the spec that first checks
		// them. Every spec below needs these ids, and taking them from a sibling
		// makes each one pass only in the order the file is written -- which is
		// also what stops any of them being run on its own to find out whether
		// it stands up.
		for _, identity := range []string{reader, writer} {
			Eventually(func() string {
				return servicePrincipalIDOf(team, serviceAccount, identity)
			}, 5*time.Minute, 5*time.Second).ShouldNot(BeEmpty(),
				"nothing was issued for %q", identity)
			issuedIDs[identity] = servicePrincipalIDOf(team, serviceAccount, identity)
		}
	})

	AfterAll(func() {
		// Withdrawal is what destroys the identities, and the last spec does it
		// deliberately. This is for a run that failed before reaching it: a
		// leftover here is a service principal in a real account that nothing
		// will ever collect, because the operator only removes what a Kubernetes
		// object still points at.
		removeTeam(team)
	})

	It("issues one identity per name the asker wrote", func() {
		Eventually(func() []string {
			return profilesOn(team, serviceAccount)
		}, 3*time.Minute, 5*time.Second).Should(ConsistOf(reader, writer),
			"the DatabricksServiceAccount does not carry both identities under the names they were asked by")

		Eventually(func() bool {
			for _, identity := range []string{reader, writer} {
				if clientIDOf(team, serviceAccount, identity) == "" {
					return false
				}
			}
			return true
		}, 5*time.Minute, 5*time.Second).Should(BeTrue(),
			"an identity never got a client id, so nothing was created in Databricks for it")
	})

	It("created them in Databricks, and they are the ones it recorded", func() {
		// The operator's own account of what it did, checked against the
		// account. Everything else in this suite reads the operator; this is the
		// one place the far side answers for itself.
		for _, identity := range []string{reader, writer} {
			Expect(exists(issuedIDs[identity])).To(BeTrue(),
				"the operator recorded service principal %s for %q and Databricks does not have it",
				issuedIDs[identity], identity)
		}
		Expect(issuedIDs[reader]).NotTo(Equal(issuedIDs[writer]),
			"one ServiceAccount asked for two identities and got one service principal twice")
	})

	It("equips a pod with a token per identity", func() {
		runPod(team, runner, serviceAccount)

		Eventually(func() string {
			return kubectlOut("-n", team, "get", "pod", runner, "-o", "jsonpath={.status.phase}")
		}, 3*time.Minute, 5*time.Second).Should(Equal("Running"),
			"the pod never started with what the webhook wrote")
	})

	// The spec this suite exists for.
	//
	// Every layer below this one stops somewhere. This is the sentence itself:
	// the token a kubelet projected into this pod, presented from inside it, is
	// accepted by Databricks as the service principal the operator created --
	// and no credential was written anywhere for that to happen.
	It("exchanges each token for the identity it names, from inside the pod", func() {
		for _, identity := range []string{reader, writer} {
			By("exchanging as " + identity)
			got := exchangeInPod(team, runner, identity)
			Expect(got).To(Equal(clientIDOf(team, serviceAccount, identity)),
				"a token from this pod's %q profile was exchanged and Databricks answered as a "+
					"different service principal", identity)
		}
	})

	It("answers as two different service principals, from one ServiceAccount", func() {
		// The same exchange, read for what one pod holding two identities means:
		// one ServiceAccount, one subject, one token issuer, and two service
		// principals that the request tells apart by client id alone.
		Expect(exchangeInPod(team, runner, reader)).NotTo(Equal(exchangeInPod(team, runner, writer)),
			"both profiles exchanged for the same service principal, so the second identity is "+
				"not a second identity")
	})

	// The spec the whole shape was changed for, made where the cost is real.
	//
	// A value that cannot be read used to be indistinguishable from an entry
	// somebody had removed, and the operator read it as a removal: this exact
	// input, one "/" short of an operator reference, destroyed a service
	// principal and every grant anybody had made on it. What must happen now is
	// nothing whatever -- not to the identity the key names, and not to its
	// neighbour.
	It("changes nothing at all when a key's value cannot be read", func() {
		client := clientIDOf(team, serviceAccount, reader)
		Expect(client).NotTo(BeEmpty(), "there is no working identity here to leave alone")

		askForIdentity(team, serviceAccount, reader, operatorNamespace)
		// Put back inside the spec that broke it, so that the withdrawal below
		// is a withdrawal of an identity that is there rather than of one this
		// spec left in an unreadable state.
		DeferCleanup(func() { askForIdentity(team, serviceAccount, reader, operatorRef()) })

		// Polled rather than read once. The edit wakes a reconcile immediately,
		// so a single read straight after it passes before the operator has
		// looked at the annotation at all -- which is the reading under which
		// the destroy this is about went unnoticed.
		Consistently(func() string {
			return clientIDOf(team, serviceAccount, reader)
		}, 90*time.Second, 10*time.Second).Should(Equal(client),
			"a value that could not be read took %q off the DatabricksServiceAccount. A pod admitted now "+
				"would be equipped for an identity the operator has stopped standing behind",
			reader)

		Expect(exists(issuedIDs[reader])).To(BeTrue(),
			"service principal %s was destroyed over a mistyped value, along with every grant "+
				"anybody had made on it. Nothing asked for it to be destroyed: the key is still "+
				"there and it could not be read", issuedIDs[reader])
		Expect(exchangeInPod(team, runner, reader)).To(Equal(client),
			"the running workload can no longer authenticate as %q, so a mistyped annotation "+
				"took down something that was already working", reader)

		Expect(profilesOn(team, serviceAccount)).To(ConsistOf(reader, writer),
			"a key that could not be read changed what the other keys were given")
		Expect(exists(issuedIDs[writer])).To(BeTrue(),
			"one key's value could not be read and another identity's service principal went")
	})

	It("leaves the other usable when one of the two is withdrawn", func() {
		// A workload reads one configuration and half of it is taken away. What
		// has to survive is not the entry -- that is the cluster suite's, and it
		// has no Databricks to ask -- but the exchange: the identity still asked
		// for still authenticates, from the same pod, after its neighbour was
		// destroyed in the same account.
		stopAskingForIdentity(team, serviceAccount, reader)

		Eventually(func() bool {
			return exists(issuedIDs[reader])
		}, 3*time.Minute, 5*time.Second).Should(BeFalse(),
			"the identity whose key was deleted is still in Databricks")

		// The half that says the first half meant anything. The two identities
		// differ only in the name the asker gave, so an operator that took one
		// key's deletion for the ServiceAccount's whole request having changed
		// destroys both and passes the assertion above.
		Expect(exists(issuedIDs[writer])).To(BeTrue(),
			"deleting one identity's key destroyed service principal %s, which another key is "+
				"still asking for", issuedIDs[writer])

		// A pod of its own. The one above holds the configuration it was admitted
		// with, and a workload that starts now is what a workload that restarts
		// after such an edit is.
		const after = "after-drop"
		runPod(team, after, serviceAccount)
		Eventually(func() string {
			return kubectlOut("-n", team, "get", "pod", after, "-o", "jsonpath={.status.phase}")
		}, 3*time.Minute, 5*time.Second).Should(Equal("Running"))

		Expect(exchangeInPod(team, after, writer)).To(Equal(clientIDOf(team, serviceAccount, writer)),
			"the identity that was kept stopped authenticating when its neighbour was destroyed")
	})

	It("destroys them in Databricks when the last key is deleted too", func() {
		stopAskingForIdentity(team, serviceAccount, writer)

		// Both, though one went a spec ago. Asserting the pair says that a
		// ServiceAccount left asking for nothing keeps nothing, not only that
		// the key edited last was acted on.
		for _, identity := range []string{reader, writer} {
			id := issuedIDs[identity]
			Expect(id).NotTo(BeEmpty(), "the earlier spec did not record an id for %q", identity)
			// Polled rather than read once. A delete becomes visible to a read
			// somewhere between a quarter of a second and eight, and a read that
			// happened to be late has passed for a day and then failed.
			Eventually(func() bool {
				return exists(id)
			}, 3*time.Minute, 5*time.Second).Should(BeFalse(),
				"service principal %s for %q is still in Databricks after the request for it was "+
					"withdrawn", id, identity)
		}
	})
})

// Tearing the namespace down is the case the record was moved out of it for.
//
// Everything a tenant holds goes at once and in an order nothing specifies: the
// ServiceAccount, the DatabricksServiceAccount, the namespace itself. What must
// survive that is the operator's own record, because it is the only thing that
// knows a service principal exists -- and what must not survive it is the
// service principal. Neither half can be checked anywhere else: on kind there
// is nothing in Databricks to look for, and against an account alone there is
// no namespace to tear down.
var _ = Describe("A namespace torn down with an identity in it", Ordered, func() {
	const (
		team           = "e2e-teardown"
		serviceAccount = "etl"
	)
	var profile, issuedID string

	BeforeAll(func() {
		profile = unnamedProfile()
		removeTeam(team)
		serve(team)
		applyTeam(team, serviceAccount, unnamed)

		Eventually(func() string {
			return servicePrincipalIDOf(team, serviceAccount, profile)
		}, 5*time.Minute, 5*time.Second).ShouldNot(BeEmpty(),
			"nothing was issued, so there is nothing for a teardown to lose")
		issuedID = servicePrincipalIDOf(team, serviceAccount, profile)
		Expect(exists(issuedID)).To(BeTrue())
	})

	AfterAll(func() {
		removeTeam(team)
		// A run that failed before the assertion leaves an identity in a real
		// account that nothing in the cluster records any more, which is exactly
		// the outcome this spec is about. The suite made it, so the suite
		// removes it.
		destroyIfLeft(issuedID)
	})

	It("destroys the service principal when the whole namespace goes", func() {
		removeTeam(team)

		Eventually(func() bool {
			return exists(issuedID)
		}, 5*time.Minute, 5*time.Second).Should(BeFalse(),
			"the namespace is gone and service principal %s is still in Databricks. Nothing in "+
				"the cluster names it now, so nothing will ever remove it", issuedID)
	})

	It("keeps no record of it afterwards", func() {
		Eventually(func() string {
			return kubectlOut("-n", operatorNamespace, "get", "issueddatabricksserviceprincipal",
				"-o", "jsonpath={.items[*].metadata.name}")
		}, 2*time.Minute, 5*time.Second).ShouldNot(ContainSubstring(team+"."+serviceAccount),
			"the record outlived the service principal it was holding")
	})
})

// A name is not an identity, and this is where that stops being a slogan.
//
// A federation policy matches a subject, and a subject is
// system:serviceaccount:<namespace>:<name> -- two names, both reusable. Delete a
// ServiceAccount and make another with the same name and the new one presents a
// token whose subject is character for character the old one's. If the old
// service principal is still there and still trusts that subject, the new
// occupant inherits it, and everything anybody granted it.
//
// What stops that is that the old one is destroyed, and that the record is named
// after a uid rather than a name. Both are only observable here.
var _ = Describe("A ServiceAccount recreated under the same name", Ordered, func() {
	const (
		team           = "e2e-reuse"
		serviceAccount = "etl"
	)
	var profile, firstID, firstClient string

	BeforeAll(func() {
		profile = unnamedProfile()
		removeTeam(team)
		serve(team)
		applyTeam(team, serviceAccount, unnamed)

		Eventually(func() string {
			return clientIDOf(team, serviceAccount, profile)
		}, 5*time.Minute, 5*time.Second).ShouldNot(BeEmpty(), "nothing was issued to reuse the name of")
		firstID = servicePrincipalIDOf(team, serviceAccount, profile)
		firstClient = clientIDOf(team, serviceAccount, profile)
	})

	AfterAll(func() {
		removeTeam(team)
		destroyIfLeft(firstID)
		destroyIfLeft(servicePrincipalIDOf(team, serviceAccount, profile))
	})

	It("destroys the identity the first one had", func() {
		_, err := kubectl("-n", team, "delete", "serviceaccount", serviceAccount)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			return exists(firstID)
		}, 5*time.Minute, 5*time.Second).Should(BeFalse(),
			"service principal %s outlived the ServiceAccount it was issued to. The next holder "+
				"of that name presents the same subject, so it would inherit this", firstID)
	})

	// Named for what it asserts, which is less than the Describe above it.
	//
	// It cannot catch inheritance. By the time it runs, the spec above has
	// waited until the first service principal was gone, so there is nothing
	// left to inherit and any operator passes. Catching inheritance means the
	// old service principal being alive while the new ServiceAccount exists,
	// which happens when a destroy is slow or refused -- and reaching that state
	// deliberately needs somewhere to hold the destroy, which this operator does
	// not have. Recorded in draft.md rather than implied by a name here.
	//
	// What it does hold: the ServiceAccount that took the name is served like any
	// other, and gets an identity of its own that Databricks has.
	It("issues an identity of its own to the ServiceAccount that takes the name", func() {
		applyTeam(team, serviceAccount, unnamed)

		var second string
		Eventually(func() string {
			second = clientIDOf(team, serviceAccount, profile)
			return second
		}, 5*time.Minute, 5*time.Second).ShouldNot(BeEmpty(),
			"the ServiceAccount that took the name was never issued anything")

		Expect(second).NotTo(Equal(firstClient),
			"the new ServiceAccount reports the client id of the one that had the name before it")
		Expect(exists(servicePrincipalIDOf(team, serviceAccount, profile))).To(BeTrue(),
			"the new identity was recorded and Databricks does not have it")
	})
})

// The decision the operator makes about somebody else's platform, checked where
// it is actually made.
//
// A service principal deleted in Databricks was deleted by somebody entitled to.
// The annotation still stands, so the operator could build another -- and it
// refuses, because building one a minute later is overruling that person on a
// timer, and it never tires. That refusal is a decision, not a mechanism, and
// nothing anywhere had exercised it: on kind there is nothing to delete, and
// against an account alone there is no operator to watch.
//
// What it must do instead is say so, name the id somebody can search an audit
// log with, and stop equipping pods with a client that resolves to nothing.
var _ = Describe("An identity deleted in Databricks", Ordered, func() {
	const (
		team           = "e2e-removed"
		serviceAccount = "etl"
	)
	var profile, deletedID string

	BeforeAll(func() {
		profile = unnamedProfile()
		removeTeam(team)
		serve(team)
		applyTeam(team, serviceAccount, unnamed)

		Eventually(func() string {
			return servicePrincipalIDOf(team, serviceAccount, profile)
		}, 5*time.Minute, 5*time.Second).ShouldNot(BeEmpty(), "nothing was issued to delete")
		deletedID = servicePrincipalIDOf(team, serviceAccount, profile)
		Expect(exists(deletedID)).To(BeTrue())

		By("deleting it in Databricks, the way whoever governs the account would")
		Expect(clients.DeleteServicePrincipal(context.Background(), deletedID)).To(Succeed())
	})

	AfterAll(func() {
		removeTeam(team)
		destroyIfLeft(servicePrincipalIDOf(team, serviceAccount, profile))
	})

	It("reports it, naming the id somebody can go and ask about", func() {
		Eventually(func() string {
			return removedIDOf(team, serviceAccount, profile)
		}, 5*time.Minute, 5*time.Second).Should(Equal(deletedID),
			"nothing on the DatabricksServiceAccount names the service principal that was deleted, so the only "+
				"way to find out which one it was is to read the operator's logs")

		Expect(conditionOn(team, serviceAccount, profile, "Ready")).To(Equal("RemovedInDatabricks"),
			"the identity does not say why it is not ready")
	})

	It("stops naming a client id, so no pod is equipped with one that resolves to nothing", func() {
		Expect(clientIDOf(team, serviceAccount, profile)).To(BeEmpty(),
			"the DatabricksServiceAccount still carries a client id for a service principal that is gone. A pod "+
				"admitted now would be given it, exchange for nothing, and fail at the far end")
	})

	It("does not build another while the annotation still stands", func() {
		// Waited for before anything is held, and waited for on the right thing.
		//
		// Straight after the deletion the DatabricksServiceAccount still carries the
		// client id from before it, so holding "no client id" from there reports a
		// replacement that has not happened -- a failure that is untrue, and one that
		// only the specs above happening to run first would hide.
		//
		// And the wait is for the removal being recorded, not for the client id
		// going: an operator that noticed and then replaced also has a client
		// id, so waiting on that one cannot tell "has not looked yet" from "has
		// looked and built another", and would report the first while meaning
		// the other.
		Eventually(func() string {
			return removedIDOf(team, serviceAccount, profile)
		}, 5*time.Minute, 5*time.Second).Should(Equal(deletedID),
			"the operator never noticed the service principal was deleted, so there is nothing "+
				"yet to say about what it did next")

		// The whole of the decision. The request has not been withdrawn, so an
		// operator reading the annotation as a standing order would make a new
		// one -- with a new client id, holding none of what was granted to the
		// old, and overruling whoever did the deleting.
		Consistently(func() string {
			return clientIDOf(team, serviceAccount, profile)
		}, 90*time.Second, 10*time.Second).Should(BeEmpty(),
			"the operator issued a replacement for an identity somebody deleted in Databricks")
	})
})

// The orphan, made on purpose, because it is the only way to reach the state the
// design's worst case describes.
//
// A federation policy matches a subject and a subject is a pair of reusable
// names. So an identity that outlives the ServiceAccount it was issued to is
// trusted by whoever takes that name next. What normally prevents it is that the
// identity is destroyed first -- which the specs above assert, and which is why
// they can never see this: by the time they look, there is nothing to inherit.
//
// So the orphan is built directly in the account, with the operator running and
// untouched. That is also what an orphan is: something in Databricks that
// nothing in the cluster records. It is built through the operator's own
// Issuing, so the marker and the display name are the ones the operator would
// have written rather than a second implementation of them, and its uid is one
// no ServiceAccount will ever have.
//
// What this can assert is that the operator does not hand the orphan to the new
// occupant. It cannot assert the orphan is unreachable, and nothing can: an
// applicationId is not a secret -- it is the value handed to whoever grants
// permissions -- so anyone who has seen it and holds these two names can present
// a token for it. Deleting the orphan is the whole of that answer, which is why
// this deletes it.
var _ = Describe("A ServiceAccount taking a name an orphaned identity still trusts", Ordered, func() {
	const (
		team           = "e2e-orphan"
		serviceAccount = "etl"
		runner         = "runner"
	)
	var profile, orphanID, orphanClient string

	BeforeAll(func() {
		profile = unnamedProfile()
		removeTeam(team)
		serve(team)
		orphanID, orphanClient = makeOrphan(team, serviceAccount)
		applyTeam(team, serviceAccount, unnamed)
	})

	AfterAll(func() {
		removeTeam(team)
		// This made an identity nothing will collect. Deleting it is not tidying
		// up after a failure; it is the answer the design gives for an orphan,
		// carried out by whoever made one.
		destroyIfLeft(orphanID)
		destroyIfLeft(servicePrincipalIDOf(team, serviceAccount, profile))
	})

	It("is a real orphan: the account holds it and the cluster does not", func() {
		// Asserted rather than assumed. A setup that quietly failed to make one
		// would leave every spec below passing for the wrong reason.
		Expect(exists(orphanID)).To(BeTrue(), "the orphan was not created")
		Expect(kubectlOut("-n", operatorNamespace, "get", "issueddatabricksserviceprincipal",
			"-o", "jsonpath={.items[*].status.servicePrincipalId}")).NotTo(ContainSubstring(orphanID),
			"the operator has a record of the orphan, so it is not one")
	})

	It("is issued an identity of its own, not the one left behind", func() {
		var taken string
		Eventually(func() string {
			taken = clientIDOf(team, serviceAccount, profile)
			return taken
		}, 5*time.Minute, 5*time.Second).ShouldNot(BeEmpty(),
			"the ServiceAccount that took the name was never issued anything")

		Expect(taken).NotTo(Equal(orphanClient),
			"the ServiceAccount that took the name was handed %s, the identity that already "+
				"trusted this namespace and name. Its pods would authenticate as that service "+
				"principal and hold everything anybody granted it", orphanClient)
	})

	It("hands the pod the new identity, which is what a workload actually reads", func() {
		// The DatabricksServiceAccount is the operator's claim; this is what a
		// workload gets. They are written by different code and the one that decides
		// is this.
		runPod(team, runner, serviceAccount)
		Eventually(func() string {
			return kubectlOut("-n", team, "get", "pod", runner, "-o", "jsonpath={.status.phase}")
		}, 3*time.Minute, 5*time.Second).Should(Equal("Running"))

		got := exchangeInPod(team, runner, profile)
		Expect(got).To(Equal(clientIDOf(team, serviceAccount, profile)),
			"the pod exchanged for something other than the identity this ServiceAccount was issued")
		Expect(got).NotTo(Equal(orphanClient), "the pod exchanged for the orphan")
	})
})
