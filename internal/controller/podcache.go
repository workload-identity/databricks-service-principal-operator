package controller

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dbxwebhook "github.com/workload-identity/databricks-service-principal-operator/internal/webhook"
)

// This lives beside equipment rather than beside the manager it is
// configured on, and that placement is the point.
//
// It exists only to serve that check: it drops everything the check does not
// read, so that watching every pod in a cluster stays cheap. The two therefore
// have to agree about what is read, and nothing in the language makes them --
// a field added to the check and not to this one is silently always absent, so
// every pod is reported wrong and every test still passes, because the tests
// hand the reconciler whole pods and production hands it these.
//
// Being in this package is what lets the tests close that: they push their
// fixtures through TrimPod before the reconciler sees them, the way the cache
// does.

// TrimPod keeps only what the equipment check reads: which ServiceAccount a pod
// runs as, whether it is still going to run, its volumes' names and the tokens
// they project, and the configuration it was given. A cache entry is not a copy
// of the object for other people to use: nothing here reaches for a field this
// drops, and a future reader who needs one will find it missing rather than
// stale, which is the failure that can be noticed.
func TrimPod(object any) (any, error) {
	pod, ok := object.(*corev1.Pod)
	if !ok {
		return object, nil
	}
	trimmed := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              pod.Name,
			Namespace:         pod.Namespace,
			UID:               pod.UID,
			ResourceVersion:   pod.ResourceVersion,
			DeletionTimestamp: pod.DeletionTimestamp,
		},
		Spec:   corev1.PodSpec{ServiceAccountName: pod.Spec.ServiceAccountName},
		Status: corev1.PodStatus{Phase: pod.Status.Phase},
	}

	// One annotation of the pod's, and not the rest. It is what the pod was
	// given, and comparing it against what the identity says now is how a pod
	// holding an older answer is found. A pod's annotations are otherwise
	// unbounded -- every tool in a cluster writes there -- so keeping the map
	// would put all of it in this operator's memory for every pod in the cluster.
	if config, given := pod.Annotations[dbxwebhook.ConfigAnnotation]; given {
		trimmed.Annotations = map[string]string{dbxwebhook.ConfigAnnotation: config}
	}

	for _, volume := range pod.Spec.Volumes {
		trimmed.Spec.Volumes = append(trimmed.Spec.Volumes, keepAudience(volume))
	}
	return trimmed, nil
}

// keepAudience keeps a volume's name and, for every ServiceAccount token it
// projects, the path that says which identity the token is for and the audience
// it is minted for.
//
// The audience is what decides whether a token can be exchanged at all, and a
// name alone cannot answer that. A pod carrying a volume of the right name whose
// token was minted for some other audience -- the API server's, which is what
// kubelet uses when none is asked for -- looks equipped from the name and can
// never reach Databricks.
//
// The path is what says which of them is which. One volume carries one token per
// identity, so keeping only the first would have every identity answered by
// whichever happened to be projected first, and a pod equipped for one and not
// another would read as equipped for both.
//
// Nothing else of a source survives: not the expiry, not any other kind of
// volume's contents.
func keepAudience(volume corev1.Volume) corev1.Volume {
	kept := corev1.Volume{Name: volume.Name}
	if volume.Projected == nil {
		return kept
	}
	var sources []corev1.VolumeProjection
	for _, source := range volume.Projected.Sources {
		if source.ServiceAccountToken == nil {
			continue
		}
		sources = append(sources, corev1.VolumeProjection{
			ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
				Path:     source.ServiceAccountToken.Path,
				Audience: source.ServiceAccountToken.Audience,
			},
		})
	}
	if len(sources) > 0 {
		kept.Projected = &corev1.ProjectedVolumeSource{Sources: sources}
	}
	return kept
}
