package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	dbxwebhook "github.com/workload-identity/databricks-service-principal-operator/internal/webhook"
)

// TestTrimPodKeepsExactlyWhatIsRead covers the cache entry being smaller than
// the object without being wrong.
//
// Pods are the most numerous thing in a cluster and this operator watches them
// all, so what is kept per pod decides whether the watch is cheap. What it must
// not do is drop something the equipment check reads: that would report every
// pod as unequipped, or none, and the test that would catch it is this one.
func TestTrimPodKeepsExactlyWhatIsRead(t *testing.T) {
	t.Parallel()
	deleted := metav1.Now()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "runner",
			Namespace:         "team-a",
			UID:               "a-uid",
			ResourceVersion:   "42",
			DeletionTimestamp: &deleted,
			Annotations: map[string]string{
				"large":                     "dropped",
				dbxwebhook.ConfigAnnotation: "[ops-a/databricks-account]",
			},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: "etl",
			Volumes: []corev1.Volume{{
				Name: "databricks-token",
				VolumeSource: corev1.VolumeSource{
					Projected: &corev1.ProjectedVolumeSource{
						Sources: []corev1.VolumeProjection{{
							ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
								Path:              "ops-a/databricks-account/token",
								Audience:          "databricks",
								ExpirationSeconds: ptr.To(int64(3600)),
							},
						}},
					},
				},
			}, {
				Name:         "somebody-elses",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
			Containers: []corev1.Container{{
				Name: "app", Image: "busybox",
				Env: []corev1.EnvVar{
					{Name: "DATABRICKS_HOST", Value: "https://dbc-example.cloud.databricks.com"},
					{Name: "SOMETHING_ELSE", Value: "dropped"},
				},
			}},
		},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "app"}},
		},
	}

	trimmed, err := TrimPod(pod)
	if err != nil {
		t.Fatal(err)
	}
	kept, ok := trimmed.(*corev1.Pod)
	if !ok {
		t.Fatalf("TrimPod returned %T", trimmed)
	}

	// Everything the equipment check reads.
	if kept.Name != "runner" || kept.Namespace != "team-a" {
		t.Errorf("identity is %s/%s", kept.Namespace, kept.Name)
	}
	if kept.Spec.ServiceAccountName != "etl" {
		t.Errorf("serviceAccountName is %q; without it no pod can be attributed to an identity",
			kept.Spec.ServiceAccountName)
	}
	if kept.Status.Phase != corev1.PodRunning {
		t.Errorf("phase is %q; without it a finished pod is reported as one somebody should fix",
			kept.Status.Phase)
	}
	if kept.DeletionTimestamp == nil {
		t.Error("deletionTimestamp was dropped; a pod on its way out would be reported as fixable")
	}
	if len(kept.Spec.Volumes) != 2 || kept.Spec.Volumes[0].Name != "databricks-token" {
		t.Errorf("volumes are %+v; a volume this operator did not write is kept by name so that "+
			"a collision on the mount path is still visible", kept.Spec.Volumes)
	}

	// Containers are dropped whole. Nothing about them is read any more: what a
	// pod was given is written in one annotation, not spread across every
	// container's environment.
	if len(kept.Spec.Containers) != 0 || len(kept.Spec.InitContainers) != 0 {
		t.Errorf("containers are %+v and %+v; nothing here reads them, and they are most of the "+
			"size of a pod", kept.Spec.Containers, kept.Spec.InitContainers)
	}
	if len(kept.Status.ContainerStatuses) != 0 {
		t.Error("container statuses were kept; nothing here reads them")
	}

	// One annotation, and only that one. A pod's annotations are unbounded --
	// every tool in a cluster writes there -- so keeping the map would put all of
	// it in this operator's memory for every pod in the cluster.
	if kept.Annotations["large"] != "" {
		t.Error("an annotation nothing reads was kept")
	}
	if kept.Annotations[dbxwebhook.ConfigAnnotation] != "[ops-a/databricks-account]" {
		t.Errorf("annotations are %v; without the configuration every pod is reported as "+
			"carrying an older answer than the identity gives now", kept.Annotations)
	}

	// The projected token keeps its path and its audience and nothing else. The
	// path says which identity it is for, and without it one token answers for
	// every identity the pod holds.
	source := kept.Spec.Volumes[0].Projected.Sources[0].ServiceAccountToken
	if source.Path != "ops-a/databricks-account/token" || source.Audience != "databricks" {
		t.Errorf("the projected token is %+v, want its path and audience", source)
	}
	if source.ExpirationSeconds != nil {
		t.Error("expirationSeconds was kept; nothing here reads it")
	}
	if kept.Spec.Volumes[1].Projected != nil || kept.Spec.Volumes[1].EmptyDir != nil {
		t.Error("a source was kept for a volume that projects no token")
	}
}

// TestTrimPodLeavesAnythingElseAlone covers the cache handing it something that
// is not a pod. Returning it unchanged is what keeps this from being a way to
// silently empty another kind.
func TestTrimPodLeavesAnythingElseAlone(t *testing.T) {
	t.Parallel()
	other := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "etl"}}
	got, err := TrimPod(other)
	if err != nil {
		t.Fatal(err)
	}
	if got != any(other) {
		t.Errorf("returned %+v, want the object unchanged", got)
	}
}

// TestTrimPodKeepsNoConfigurationWhenThereIsNone covers a pod this operator
// never equipped, which is nearly every pod in the cluster.
//
// It has to come out of the trim saying nothing rather than saying the empty
// string: an annotation that is absent and one that is present and blank are the
// same to the check, and keeping the map for the second would put an entry on
// every pod in the cluster to say nothing.
func TestTrimPodKeepsNoConfigurationWhenThereIsNone(t *testing.T) {
	t.Parallel()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "runner", Namespace: "team-a",
			Annotations: map[string]string{"somebody-elses": "kept out"},
		},
		Spec: corev1.PodSpec{ServiceAccountName: "etl"},
	}

	trimmed, err := TrimPod(pod)
	if err != nil {
		t.Fatal(err)
	}
	if kept := trimmed.(*corev1.Pod); kept.Annotations != nil {
		t.Errorf("annotations are %v, want none at all", kept.Annotations)
	}
}
