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

// Package webhook puts a Databricks-audienced token into the pods of
// ServiceAccounts that asked for an identity.
//
// This is not a convenience. The token every pod already has, at
// /var/run/secrets/kubernetes.io/serviceaccount/token, carries the API server's
// audience -- and a Databricks federation policy checks aud, so that token can
// never be exchanged. A second, explicitly projected token is required work; the
// only question is whether each workload's author writes it or this does.
package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

const (
	// Path is where this webhook is served, and it appears twice: here, where
	// the handler is registered, and in config/webhook/manifests.yaml, where the
	// API server is told to send pods.
	//
	// That manifest is written by hand rather than generated, because the
	// +kubebuilder:webhook marker cannot express a namespaceSelector and this
	// webhook's whole reach is one. So nothing regenerates the two together, and
	// a change here that does not reach the manifest is silent in the worst way:
	// failurePolicy is Ignore, so pods are sent to a path with no handler, the
	// error is swallowed, and every pod in every enrolled namespace starts
	// unequipped and looks normal.
	Path = "/mutate-v1-pod"

	// TokenVolume is the name of the volume this adds. It is also how the
	// webhook recognises its own work, so that a pod written by somebody who
	// wanted control gets left alone.
	TokenVolume = "databricks-token"

	// TokenMountPath is where everything this webhook supplies appears: one token
	// per identity, and the configuration naming them.
	TokenMountPath = "/var/run/secrets/databricks"

	// TokenFile is the name of each token file. It is the last segment of a path
	// whose earlier segments are the identity it belongs to, so several identities
	// are several files rather than one that has to be shared.
	TokenFile = "token"

	// ConfigFile is the rendered configuration, in the directory above.
	ConfigFile = "config"

	// ConfigAnnotation carries that configuration on the pod, which is how it
	// reaches the container: a downwardAPI projection copies this annotation into
	// a file, so nothing is created in the workload's namespace to hold it.
	ConfigAnnotation = "databricks.workload-identity.io/config"

	// EnvConfigFile and EnvConfigProfile publish what the pod was given, under
	// names no Databricks SDK reads.
	//
	// Nothing the SDK looks at is set, and that is the whole of this design.
	// Setting DATABRICKS_CONFIG_FILE would not add a value -- it would take over
	// the SDK's entire resolution, so a workload carrying its own .databrickscfg
	// would find that file still present, untouched, and never read again. An
	// environment variable beats a profile silently, measured, so the intrusion
	// would be undetectable from inside the workload and invisible to this
	// operator, which cannot see what an image contains.
	//
	// The tail of each name is the SDK's own, so the relationship needs no
	// lookup. The prefix is this project's domain, so nothing Databricks adds
	// later can collide with it -- which a name inside DATABRICKS_ could not
	// promise.
	//
	// What this costs is one line in every workload that wants what it was given:
	//
	//	export DATABRICKS_CONFIG_FILE=$WORKLOAD_IDENTITY_DATABRICKS_CONFIG_FILE
	//	export DATABRICKS_CONFIG_PROFILE=$WORKLOAD_IDENTITY_DATABRICKS_CONFIG_PROFILE
	//
	// That is the price of never overriding anything, and it is the right price:
	// the alternative charges it to whoever brought their own configuration and
	// cannot find out why it stopped mattering.
	EnvConfigFile    = "WORKLOAD_IDENTITY_DATABRICKS_CONFIG_FILE"
	EnvConfigProfile = "WORKLOAD_IDENTITY_DATABRICKS_CONFIG_PROFILE"

	// tokenExpirationSeconds is how long kubelet mints each token for. An hour is
	// the API server's own default rather than a floor -- ServiceAccountTokenProjection
	// takes anything from 600 seconds up -- and it is kept because nothing here
	// gains from a shorter one: kubelet rewrites the file well before it expires,
	// so the lifetime is invisible to the workload either way.
	tokenExpirationSeconds int64 = 3600
)

// PodTokenInjector adds the projected token to pods whose ServiceAccount has an
// identity.
type PodTokenInjector struct {
	client.Client
	Decoder admission.Decoder
}

// Handle injects the volume, the mount, and the two environment variables.
//
// It admits the pod either way. A pod refused because this webhook could not
// answer is a workload that does not start for a reason unrelated to what it
// does, and the failure this prevents is visible and specific: the workload's
// first call to Databricks is refused, with the reason on its
// DatabricksServiceAccount. Refusing here trades a legible failure for an
// illegible one.
func (i *PodTokenInjector) Handle(ctx context.Context, req admission.Request) admission.Response {
	pod := &corev1.Pod{}
	if err := i.Decoder.Decode(req, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	// The namespace on the request rather than on the pod: a pod created by a
	// controller may not carry one yet.
	namespace := req.Namespace
	if namespace == "" {
		namespace = pod.Namespace
	}

	account := pod.Spec.ServiceAccountName
	if account == "" {
		account = "default"
	}

	// The identity is what says this pod's ServiceAccount asked and was allowed.
	// Reading it rather than re-deriving the answer means the webhook and the
	// controller cannot disagree about who gets a token.
	var principal dbxv1alpha1.DatabricksServiceAccount
	switch err := i.Get(ctx, types.NamespacedName{Namespace: namespace, Name: account}, &principal); {
	case apierrors.IsNotFound(err):
		return admission.Allowed("no Databricks identity for this ServiceAccount")
	case err != nil:
		// Admitted, not refused. See the note on Handle.
		return admission.Allowed(fmt.Sprintf("could not read the Databricks identity: %v", err))
	}

	// Every identity that has converged, in the order the status carries them.
	// All of them, because a workload reading with one service principal and
	// writing with another holds both at once and a pod carries whatever it was
	// issued.
	//
	// The client id is assigned by Databricks on create, so one still without it
	// is dropped rather than written: a profile naming no client cannot be
	// exchanged for anything, and its neighbours are usable now. The pod is
	// admitted with what there is, and a later pod gets the rest.
	var ready []dbxv1alpha1.ProjectedIdentity
	for _, identity := range principal.Status.Identities {
		if identity.ClientID != "" {
			ready = append(ready, identity)
		}
	}
	if len(ready) == 0 {
		return admission.Allowed("no Databricks identity for this ServiceAccount yet")
	}

	inject(pod, ready)

	patched, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, patched)
}

// inject adds what is missing and changes nothing that is already there.
//
// A pod that already carries a volume of this name was written by somebody who
// wanted control of it, and overwriting it would be this webhook overruling
// them silently. The same goes for a container that already sets the
// environment.
//
// It also has to leave a pod it cannot equip in a state the API server accepts.
// A mutating webhook runs before validation, so anything it produces that is
// invalid is this operator refusing to let a workload be created, with an error
// naming neither this operator nor anything its owner recognises -- and
// failurePolicy: Ignore is no protection, because the call succeeded.
func inject(pod *corev1.Pod, identities []dbxv1alpha1.ProjectedIdentity) {
	// Nothing at all when the name is taken. Either this pod is already equipped
	// -- being seen twice -- or that volume belongs to somebody else, and
	// mounting theirs where this operator's configuration says the tokens are
	// would hand the workload files that are not tokens and an error that says
	// nothing about why.
	if hasVolume(pod.Spec.Volumes, TokenVolume) {
		return
	}

	// One source per identity, plus the configuration naming them. A projected
	// volume takes several ServiceAccountToken sources, each with its own
	// audience and its own path, so several identities are one volume and one
	// mount rather than one of each per identity.
	//
	// Each audience is that identity's own. Two operators serving one workload
	// need not agree on one, and nothing here asks them to: an audience is
	// whatever the operator's own token carries, and asking two platform teams to
	// align theirs would be this operator making its own convenience somebody
	// else's coordination problem.
	sources := make([]corev1.VolumeProjection, 0, len(identities)+1)
	for _, identity := range identities {
		sources = append(sources, corev1.VolumeProjection{
			ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
				Path:              TokenProjectionPathFor(identity.Request),
				Audience:          identity.Audience,
				ExpirationSeconds: ptr(tokenExpirationSeconds),
			},
		})
	}

	// The configuration reaches the container through an annotation on the pod
	// and a downwardAPI projection of it. Nothing is created in the workload's
	// namespace to hold it: a ConfigMap would be an object this operator writes
	// into somebody else's namespace, outliving the pod it was for and needing
	// its own collection.
	pod.Annotations = withAnnotation(pod.Annotations, ConfigAnnotation, Configuration(identities))
	sources = append(sources, corev1.VolumeProjection{
		DownwardAPI: &corev1.DownwardAPIProjection{
			Items: []corev1.DownwardAPIVolumeFile{{
				Path:     ConfigFile,
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: configFieldPath},
			}},
		},
	})

	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: TokenVolume,
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{Sources: sources},
		},
	})

	// Init containers too. A workload that reaches Databricks to prepare
	// something before it starts needs its identity as much as the one that
	// reaches it while running.
	for i := range pod.Spec.InitContainers {
		equip(&pod.Spec.InitContainers[i], identities[0].Request)
	}
	for i := range pod.Spec.Containers {
		equip(&pod.Spec.Containers[i], identities[0].Request)
	}
}

// configFieldPath is the annotation the downwardAPI source copies. Measured:
// what goes in comes out byte for byte, which is what lets the file be the SDK's
// own format rather than something this operator has to re-encode.
const configFieldPath = "metadata.annotations['" + ConfigAnnotation + "']"

func equip(container *corev1.Container, defaultProfile string) {
	// Left alone entirely when something else is already at this path.
	//
	// Two mounts on one path is not a pod the API server accepts -- "mountPath:
	// Invalid value: \"/var/run/secrets/databricks\": must be unique" -- so
	// adding the second is this webhook making the pod uncreatable rather than
	// equipping it. The way in is a team that hand-rolled the projected token at
	// the same path before being enrolled, which is the path this operator's own
	// Deployment uses.
	//
	// The variables are withheld too, not just the mount. They name a file this
	// container would not have, and a workload told where its configuration is
	// when it is not there fails further from the cause than one told nothing.
	// The identity reports the pod as not equipped, which is what says so.
	if hasMountPath(container.VolumeMounts, TokenMountPath) {
		return
	}

	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      TokenVolume,
		MountPath: TokenMountPath,
		ReadOnly:  true,
	})

	// Published, not configured. Neither name is one any Databricks SDK reads,
	// so a workload that wants what it was given says so itself and one that
	// brought its own configuration keeps it.
	setEnv(container, EnvConfigFile, path.Join(TokenMountPath, ConfigFile))

	// The identity a workload here gets when it names no profile. The status
	// orders its entries with the unnamed identity first, so this is that one
	// wherever it has converged -- the only identity a ServiceAccount asking for
	// exactly one has. There is no order its owner wrote to honour instead: the
	// request is a set of annotation keys, and a map has none. A workload wanting
	// another names that profile itself; they are all in the file.
	setEnv(container, EnvConfigProfile, defaultProfile)
}

// setEnv adds a variable the container does not already set. One it does set was
// written deliberately, and this is not the place to argue with it.
func setEnv(container *corev1.Container, name, value string) {
	for _, existing := range container.Env {
		if existing.Name == name {
			return
		}
	}
	container.Env = append(container.Env, corev1.EnvVar{Name: name, Value: value})
}

func hasVolume(volumes []corev1.Volume, name string) bool {
	for _, v := range volumes {
		if v.Name == name {
			return true
		}
	}
	return false
}

// hasMountPath asks the question the API server asks. Mount paths are what have
// to be unique within a container; the volume's name is not, and comparing it
// answers a different question -- one that says "no" for every container that
// mounts something else here, which is exactly the case that makes the pod
// invalid.
func hasMountPath(mounts []corev1.VolumeMount, at string) bool {
	for _, m := range mounts {
		if m.MountPath == at {
			return true
		}
	}
	return false
}

func ptr[T any](v T) *T { return &v }

// withAnnotation sets one annotation, leaving any others and never replacing one
// somebody wrote themselves.
func withAnnotation(annotations map[string]string, name, value string) map[string]string {
	if annotations == nil {
		annotations = map[string]string{}
	}
	if _, written := annotations[name]; !written {
		annotations[name] = value
	}
	return annotations
}
