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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

// The first of the three doors, and the only one the API server holds on its
// own. It is a CEL rule on the CRD, so nothing but a real API server can be
// asked whether it works -- a fake client validates nothing, and a controller
// test that edited the field would pass while every cluster refused the same
// edit.
var _ = Describe("the account a DatabricksAccount names", Ordered, func() {
	const fixedNamespace = "account-fixed"

	var declared *dbxv1alpha1.DatabricksAccount

	BeforeAll(func() {
		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: fixedNamespace},
		})).To(Succeed())

		declared = &dbxv1alpha1.DatabricksAccount{
			ObjectMeta: metav1.ObjectMeta{Namespace: fixedNamespace, Name: "databricks-account"},
			Spec: dbxv1alpha1.DatabricksAccountSpec{
				Host:      "https://accounts.cloud.databricks.com",
				AccountID: "the-account",
				ClientID:  "11111111-1111-1111-1111-111111111111",
			},
		}
		Expect(k8sClient.Create(ctx, declared)).To(Succeed())
	})

	// Read back before each edit, so one spec's rejection is not the next one's
	// stale resourceVersion.
	live := func() *dbxv1alpha1.DatabricksAccount {
		got := &dbxv1alpha1.DatabricksAccount{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Namespace: fixedNamespace, Name: "databricks-account",
		}, got)).To(Succeed())
		return got
	}

	It("refuses an accountId that changes", func() {
		moved := live()
		moved.Spec.AccountID = "another-account"

		err := k8sClient.Update(ctx, moved)
		Expect(err).To(HaveOccurred(),
			"the account was repointed; every id this operator recorded now names something in a "+
				"place it will not be looking")
		Expect(err.Error()).To(ContainSubstring("accountId is immutable"))
		// What to do instead, on the refusal itself. A rule that only says no
		// sends whoever hit it to guess.
		Expect(err.Error()).To(ContainSubstring("install a second operator"))
	})

	It("refuses a host that changes", func() {
		moved := live()
		moved.Spec.Host = "https://accounts.gcp.databricks.com"

		err := k8sClient.Update(ctx, moved)
		Expect(err).To(HaveOccurred(),
			"the host was changed under an unchanged account id; one account has one host, so this "+
				"is a move by another route")
		Expect(err.Error()).To(ContainSubstring("host is immutable"))
	})

	It("accepts everything else about it changing", func() {
		edited := live()
		edited.Spec.ClientID = "22222222-2222-2222-2222-222222222222"
		edited.Spec.Namespaces = []string{"team-a"}

		Expect(k8sClient.Update(ctx, edited)).To(Succeed(),
			"an unchanged accountId was rejected; the operator this belongs to could never be "+
				"given a new client or a new namespace")
	})
})
