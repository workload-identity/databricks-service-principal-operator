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
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// The two ways to name a release of this project in an instruction somebody
// types. Both have a form that names no release at all -- releases/latest for
// the manifest, no --version for the chart -- and those are what the pages use.
var (
	pinnedManifest = regexp.MustCompile(`releases/download/v[0-9]+\.[0-9]+\.[0-9]+/`)
	pinnedChart    = regexp.MustCompile(`--version [0-9]+\.[0-9]+\.[0-9]+`)
)

// TestNoPageNamesARelease covers the version a reader would otherwise be given.
//
// A version written into a page is one somebody has to remember to change, and
// nothing about a released repository changes when they forget: the page goes on
// sending readers to an install one or several releases behind the project they
// came to read about. So no page names one. `releases/latest/download` is the
// newest manifest and `helm install` with no --version is the newest chart, and
// both stay right without anybody touching them.
//
// The version itself lives in Chart.yaml, which is the one file a release edits.
func TestNoPageNamesARelease(t *testing.T) {
	t.Parallel()
	for _, path := range markdownFiles(t) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		name := relativeToRoot(path)
		for _, found := range pinnedManifest.FindAllString(string(data), -1) {
			t.Errorf("%s sends a reader to %s. Every release after this one leaves that page "+
				"installing an older operator than the page describes -- releases/latest/download "+
				"is the same URL with nothing to keep up to date", name, found)
		}
		for _, found := range pinnedChart.FindAllString(string(data), -1) {
			t.Errorf("%s installs the chart with %q. Leave it off and Helm takes the newest "+
				"release, which is what the page means", name, found)
		}
	}
}

// markdownFiles is every page in the repository.
func markdownFiles(t *testing.T) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(repositoryRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// bin/ is vendored tools and .claude/worktrees holds whole checkouts of
		// this repository at other versions, where a page naming an old release
		// is correct.
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "bin" || d.Name() == ".claude" ||
			d.Name() == "dist") {
			return fs.SkipDir
		}
		if !d.IsDir() && filepath.Ext(path) == ".md" {
			found = append(found, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) == 0 {
		t.Fatal("no pages were found, so nothing here was checked")
	}
	return found
}

const repositoryRoot = "../.."

// relativeToRoot names a file the way a person would.
func relativeToRoot(path string) string {
	if cleaned, err := filepath.Rel(repositoryRoot, path); err == nil {
		return cleaned
	}
	return path
}
