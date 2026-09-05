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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// The claim as an API server stores it, which is the only place the limits on it
// are real. A fake client takes a label value of any length, so every unit test
// in destroyidentities_test.go would go on passing with the account name back in
// the label -- while every cluster refused the claim, and the namespaces this
// operator stopped serving kept their identities for ever.
var _ = Describe("the claim one operator writes on a namespace", Ordered, func() {
	const claimed = "claimed-for-destruction"

	// The namespace this operator is installed in by config/, so that what is
	// measured here is the budget a real deployment has rather than one this
	// test chose.
	const installedIn = "dbxsp-operator-system"

	// The longest account name that deployment can carry, and it is the field
	// manager that says so rather than anything in the claim: 128 bytes, shared
	// with the operator's namespace and a fixed prefix. Under the label value
	// this claim used to be, the budget was 63 shared with the same namespace,
	// which left 41 -- so this name is one no cluster would have accepted the
	// claim for.
	longName := strings.Repeat("a",
		128-len("databricks.workload-identity.io/")-len(installedIn)-len("/"))
	holder := types.NamespacedName{Namespace: installedIn, Name: longName}

	BeforeAll(func() {
		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: claimed},
		})).To(Succeed())
	})

	It("is written for an operator whose account name no label value would hold", func() {
		Expect(len(longName)).To(BeNumerically(">", 63),
			"the name is short enough to have been a label value, so this proves nothing")

		Expect(k8sClient.Apply(ctx, destructionClaimFor(claimed, holder, time.Now()),
			fieldOwnerFor(holder))).To(Succeed(),
			"the claim was refused, so a namespace taken off spec.namespaces is never claimed "+
				"and nothing in it is ever destroyed -- while the account reports the same "+
				"IdentitiesDestroyed=False it reports for a destruction that is merely slow")
	})

	It("is read back as the operator that wrote it", func() {
		namespace := &corev1.Namespace{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: claimed}, namespace)).To(Succeed())

		Expect(destructionHolderOf(namespace)).To(Equal(holder),
			"the holder does not read back whole. Its own operator would take the claim again "+
				"every pass, and every other one would name the wrong operator to whoever is "+
				"waiting")
		Expect(namespaceMints(ctx, k8sClient, claimed)).To(BeFalse(),
			"minting is not suspended while a destruction runs, so the set being destroyed can "+
				"still grow")
	})

	It("excludes a second operator sharing that namespace", func() {
		// The one case a label value alone could not decide, and the reason the
		// account name is in the claim at all rather than only in the message:
		// these two write the same label value, so what refuses the second one
		// is the annotation.
		sibling := types.NamespacedName{Namespace: installedIn, Name: "second-account"}

		err := k8sClient.Apply(ctx, destructionClaimFor(claimed, sibling, time.Now()),
			fieldOwnerFor(sibling))
		Expect(apierrors.IsConflict(err)).To(BeTrue(),
			"a second operator in this one's own namespace was not refused (%v), so both are "+
				"destroying identities in a namespace each is still adding to", err)
	})
})
