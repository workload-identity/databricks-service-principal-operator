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
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

// TestTheDatabricksServiceAccountSendsOnlyWhatIsSet holds the whole reason
// this operator builds an apply configuration rather than filling in a struct.
//
// Under server-side apply an operator owns everything it sends. Sending a value
// nobody set is not a cosmetic waste: it is this operator claiming "clientId is
// the empty string" where the truth is that it has no opinion, and the next
// operator to set clientId is refused with a conflict over a claim nobody meant
// to make.
//
// The loop is over the type's own fields rather than a written-out list, so a
// field added to ProjectedIdentity is covered the day it is added. It fails two
// ways, and the second is the quiet one:
//
//   - the conversion sends a field that was not set -- the ownership bug above;
//   - the conversion drops a field that was set -- the DatabricksServiceAccount
//     silently stops reporting it, and nothing else notices, because every
//     reader of this object is a person.
func TestTheDatabricksServiceAccountSendsOnlyWhatIsSet(t *testing.T) {
	t.Parallel()

	const key = "profile"

	for field := range reflect.TypeFor[dbxv1alpha1.ProjectedIdentity]().Fields() {
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "" {
			t.Errorf("%s carries no json tag, so what it is sent as is the Go name", field.Name)
			continue
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Only this field, and the key, which is always sent: an entry
			// without one is not an entry.
			entry := dbxv1alpha1.ProjectedIdentity{Profile: "ops-a/databricks-account"}
			switch value := reflect.ValueOf(&entry).Elem().FieldByName(field.Name); {
			case value.Kind() == reflect.String:
				value.SetString("set")
			case field.Name == "Conditions":
				entry.Conditions = []metav1.Condition{{
					Type: "Ready", Status: metav1.ConditionTrue, Reason: "Made", Message: "made",
				}}
			case field.Name == "ServicePrincipalRemovedAt":
				// Truncated to the second, because the API's own marshalling is
				// RFC 3339 and a nanosecond that does not survive the round trip
				// would look like a field that was dropped.
				removedAt := metav1.NewTime(time.Now().Truncate(time.Second))
				entry.ServicePrincipalRemovedAt = &removedAt
			default:
				t.Fatalf("%s is a %s, which this test does not know how to set; "+
					"teach it rather than skipping, or the field goes uncovered",
					field.Name, value.Kind())
			}

			raw, err := json.Marshal(projectedIdentityFor(entry))
			if err != nil {
				t.Fatal(err)
			}
			var sent map[string]any
			if err := json.Unmarshal(raw, &sent); err != nil {
				t.Fatal(err)
			}
			delete(sent, key)

			if name == key {
				if len(sent) != 0 {
					t.Errorf("an entry with only the key sends %v as well; this operator owns "+
						"every one of those, and another operator setting one is refused", sent)
				}
				return
			}
			if _, carried := sent[name]; !carried {
				t.Errorf("%s was set and is not sent; the DatabricksServiceAccount stops reporting it and "+
					"nothing fails -- everything that reads this object is a person", name)
			}
			delete(sent, name)
			if len(sent) != 0 {
				t.Errorf("setting %s also sent %v; this operator now owns values nobody set",
					name, sent)
			}
		})
	}
}
