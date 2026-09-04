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
	"context"
	"errors"
	"net/http"
	"sync/atomic"

	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// CacheSynced answers whether this operator can be asked anything yet.
//
// Readiness decides when this pod joins the webhook Service's endpoints, and the
// webhook reads the cluster through the manager's cache. A pod that reports
// ready while its cache is still filling is one the API server sends pods to
// while every read it makes fails or blocks -- and both of those land in the
// branch that admits the pod unchanged, deliberately, so that an outage here
// never stops a workload from starting.
//
// The cost of answering too early is therefore silent and it does not heal.
// Every pod created in every enrolled namespace during that window starts with
// no token, no mount and no variables, looks entirely normal, and fails on its
// first call to Databricks with an error naming Databricks. Nothing rewrites a
// running pod, so they stay that way until somebody recreates them.
//
// It is not a rare window. controller-runtime starts the webhook server before
// the caches, this operator watches every Pod, ServiceAccount and Namespace in
// the cluster, and with one replica a rolling update means the pod that is
// filling its cache is the only one answering.
//
// Liveness is deliberately not this. A cache that is slow to fill is not a
// process to kill, and gating liveness on it would restart the pod into filling
// the cache again.
type CacheSynced struct {
	synced atomic.Bool
}

// Start marks this operator answerable and blocks until it is asked to stop.
//
// It is a manager.Runnable, and that is the whole of how it knows: the manager
// starts caches first and waits for them to sync before it starts anything else,
// so being started at all is the fact being reported.
func (c *CacheSynced) Start(ctx context.Context) error {
	c.synced.Store(true)
	<-ctx.Done()
	return nil
}

// NeedLeaderElection is false, and answering it at all is load-bearing.
//
// controller-runtime puts a Runnable that stays silent here into the group it
// starts only after winning the election. This one gates readiness, and readiness
// decides when the pod joins the webhook Service -- which every replica serves
// from, leader or not. The question it answers is about this pod's own cache,
// and a cache fills whether or not the pod is the leader.
//
// Silent, a rolling update of a single replica deadlocks and stays deadlocked:
// the old pod holds the lease, so the new pod never becomes leader, so this never
// starts, so the new pod is never ready, so the Deployment never removes the old
// pod holding the lease.
func (c *CacheSynced) NeedLeaderElection() bool { return false }

// Check is the readiness check.
func (c *CacheSynced) Check(*http.Request) error {
	if c.synced.Load() {
		return nil
	}
	return errors.New("the cache has not finished filling; admitting pods now would equip none of them")
}

// Being a Runnable is not decoration: the manager starts caches first and waits
// for them to sync before it starts anything else, and that ordering is the
// whole of what Start reports.
//
// Being a LeaderElectionRunnable is what keeps that ordering from also meaning
// "and this pod won the election", which readiness must not wait for.
var (
	_ manager.Runnable               = (*CacheSynced)(nil)
	_ manager.LeaderElectionRunnable = (*CacheSynced)(nil)
)
