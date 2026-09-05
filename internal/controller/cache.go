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
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

// CacheOptions is what this operator watches, and how much of it.
//
// It is a function rather than a literal in main so that what it decides can be
// asserted. Two of the three things here are load-bearing in a way nothing else
// would report: a kind scoped to the wrong namespace, or to none, is an operator
// that works and quietly reaches into another operator's.
//
// Everything watched cluster-wide here is watched to be read. The one write this
// operator makes on an object it does not own is worth naming beside them: a
// Namespace gains one label of this operator's own while it destroys what it
// issued there, and loses it again when it has finished. The cluster's own
// labels are read and never written, so nothing here lets a team in or shuts one
// out, and the permission behind it is patch on Namespaces alone for that
// reason.
func CacheOptions(namespace string) cache.Options {
	own := map[string]cache.Config{namespace: {}}
	return cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			// Pods are watched so that one admitted without a token is reported
			// rather than left looking normal, and pods are the most numerous
			// thing in a cluster. Almost none of what they carry matters here:
			// the ServiceAccount they run as, the names of their volumes, and
			// whether they are still going to run. Everything else is dropped
			// before it reaches the cache, which is the difference between
			// holding a few hundred bytes per pod and several kilobytes.
			&corev1.Pod{}: {Transform: TrimPod},

			// The two kinds that belong to this operator, scoped to its own
			// namespace. That is the whole of how two operators are kept apart.
			//
			// The credential each holds is an account admin -- the floor
			// Databricks sets for writing a federation policy -- so an operator
			// is the trust boundary, and one that could see another's objects
			// would widen that boundary while buying nothing. It also cost
			// something: both wrote Ready on the other's DatabricksAccount, each
			// undoing the other, forever, waking every identity in the cluster
			// on every flip.
			//
			// Scoped rather than filtered in a predicate, so that the permission
			// and the watch say the same thing. Both kinds are then a Role in
			// this namespace rather than a ClusterRole.
			&dbxv1alpha1.DatabricksAccount{}:                {Namespaces: own},
			&dbxv1alpha1.IssuedDatabricksServicePrincipal{}: {Namespaces: own},
		},
	}
}
