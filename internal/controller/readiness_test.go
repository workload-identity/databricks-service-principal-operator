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
	"testing"
	"time"
)

// TestNothingIsReadyBeforeTheCacheIs covers the answer that decides when this
// pod starts being asked to admit pods.
//
// Answering ready too early is not a slow start. Every read the webhook makes
// through an unfilled cache fails or blocks, and both land in the branch that
// admits the pod unchanged -- so every pod created in every enrolled namespace
// during that window gets no token, no mount and no variables, looks entirely
// normal, and stays that way, because nothing rewrites a running pod.
func TestNothingIsReadyBeforeTheCacheIs(t *testing.T) {
	t.Parallel()
	var synced CacheSynced

	if err := synced.Check(nil); err == nil {
		t.Fatal("ready before anything started; the pod joins the webhook Service while its " +
			"cache is empty and admits every pod unchanged")
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- synced.Start(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for synced.Check(nil) != nil {
		if time.Now().After(deadline) {
			t.Fatal("still not ready after being started; the pod never joins the Service")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// And it keeps running until it is told to stop, which is what makes the
	// manager start it after the caches rather than alongside them.
	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("stopping returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("did not stop when the manager did")
	}
}
