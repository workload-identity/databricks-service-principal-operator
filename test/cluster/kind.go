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
	"os"
	"os/exec"
)

const (
	defaultKindBinary  = "kind"
	defaultKindCluster = "kind"
)

// kindCluster is the cluster this run works on, which the Makefile passes so
// that the name is decided in one place.
func kindCluster() string {
	if v, ok := os.LookupEnv("KIND_CLUSTER"); ok {
		return v
	}
	return defaultKindCluster
}

// kindBinary is the kind this run calls, which the Makefile passes so that it is
// the pinned one rather than whatever is on the PATH.
func kindBinary() string {
	if v, ok := os.LookupEnv("KIND"); ok {
		return v
	}
	return defaultKindBinary
}

// loadImageToKindCluster loads a local docker image to the kind cluster
func loadImageToKindCluster(name string) error {
	cmd := exec.Command(kindBinary(), "load", "docker-image", name, "--name", kindCluster())
	_, err := run(cmd)
	return err
}
