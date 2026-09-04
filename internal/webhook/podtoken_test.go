package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sdkconfig "github.com/databricks/databricks-sdk-go/config"
	jsonpatch "github.com/evanphx/json-patch/v5"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	dbxv1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
)

const (
	testNamespace = "team-a"
	testAccount   = "etl"
	testAudience  = "databricks"
	testClientID  = "11111111-1111-1111-1111-111111111111"

	// testOperator is the operator that issued the identity and, because that
	// identity is the unnamed one, its profile name too.
	testOperator = "operators/databricks-account"
)

func newInjector(t *testing.T, objects ...client.Object) *PodTokenInjector {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := dbxv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return &PodTokenInjector{
		Client:  fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
		Decoder: admission.NewDecoder(scheme),
	}
}

// identity is a converged DatabricksServicePrincipal holding the unnamed
// identity: Databricks has assigned both ids and the audience is recorded.
func identity(namespace, name string) *dbxv1alpha1.DatabricksServicePrincipal {
	principal := &dbxv1alpha1.DatabricksServicePrincipal{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	principal.Status.Identities = []dbxv1alpha1.ProjectedIdentity{{
		Request:            testOperator,
		Operator:           testOperator,
		ClientID:           testClientID,
		Audience:           testAudience,
		ServicePrincipalID: "7788",
	}}
	return principal
}

// resolvedProfile is what the Databricks SDK makes of one profile in what this
// webhook rendered, read by the SDK's own config package.
//
// Asserting on the text is the wrong question, and this file used to ask it. The
// SDK matches a profile's keys against the names in its own struct tags and
// drops one it does not recognise without a word, so a line being present says
// nothing about whether anything read it -- which is how "token_audience"
// survived: written into every pod, in no resolved configuration, with the
// profile resolving cleanly either way.
//
// Only the file is loaded. The SDK's default loaders read the environment first,
// so a developer with DATABRICKS_* set would be testing their own shell.
func resolvedProfile(t *testing.T, rendered, profile string) *sdkconfig.Config {
	t.Helper()
	written := filepath.Join(t.TempDir(), "databrickscfg")
	if err := os.WriteFile(written, []byte(rendered), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &sdkconfig.Config{
		ConfigFile: written,
		Profile:    profile,
		Loaders:    []sdkconfig.Loader{sdkconfig.ConfigFile},
	}
	if err := cfg.EnsureResolved(); err != nil {
		t.Fatalf("the SDK will not resolve profile %q out of what the pod was given: %v\n%s",
			profile, err, rendered)
	}
	return cfg
}

func podUsing(account string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: testNamespace},
		Spec: corev1.PodSpec{
			ServiceAccountName: account,
			Containers:         []corev1.Container{{Name: "app", Image: "busybox"}},
		},
	}
}

// requestFor is what the API server sends for this pod.
func requestFor(t *testing.T, pod *corev1.Pod) admission.Request {
	t.Helper()
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	return admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Namespace: pod.Namespace,
			Object:    runtime.RawExtension{Raw: raw},
		},
	}
}

// podFrom is the pod the API server ends up storing: what went in, with the
// response's patches applied. Asserting on the operations instead would be
// asserting on a shape nothing in the cluster ever sees.
func podFrom(t *testing.T, request admission.Request, response admission.Response) *corev1.Pod {
	t.Helper()
	raw := request.Object.Raw
	if len(response.Patches) > 0 {
		patchJSON, err := json.Marshal(response.Patches)
		if err != nil {
			t.Fatal(err)
		}
		if raw, err = applyPatch(raw, patchJSON); err != nil {
			t.Fatal(err)
		}
	}
	patched := &corev1.Pod{}
	if err := json.Unmarshal(raw, patched); err != nil {
		t.Fatal(err)
	}
	return patched
}

// admit runs the pod through the webhook and returns what it becomes.
func admit(t *testing.T, i *PodTokenInjector, pod *corev1.Pod) *corev1.Pod {
	t.Helper()
	request := requestFor(t, pod)
	response := i.Handle(context.Background(), request)
	if !response.Allowed {
		t.Fatalf("the pod was refused: %+v", response.Result)
	}
	return podFrom(t, request, response)
}

// TestAPodGetsATokenItsFederationPolicyWillAccept covers the whole reason this
// webhook exists.
//
// Every pod already has a ServiceAccount token, and it is useless here: it
// carries the API server's audience, and a Databricks federation policy checks
// aud. So a second token has to be projected asking for the audience the policy
// names -- required work, not a convenience.
func TestAPodGetsATokenItsFederationPolicyWillAccept(t *testing.T) {
	t.Parallel()
	i := newInjector(t, identity(testNamespace, testAccount))
	pod := admit(t, i, podUsing(testAccount))

	projected := tokenFor(t, pod, testOperator)
	if projected.Audience != testAudience {
		t.Errorf("audience is %q, want the one the federation policy names -- the default "+
			"ServiceAccount token carries the API server's and can never be exchanged", projected.Audience)
	}
	if projected.ExpirationSeconds == nil || *projected.ExpirationSeconds <= 0 {
		t.Errorf("expirationSeconds is %v; a token that does not expire is the thing this project removes",
			projected.ExpirationSeconds)
	}

	container := pod.Spec.Containers[0]
	if !hasMountPath(container.VolumeMounts, TokenMountPath) {
		t.Errorf("mounts are %+v; the token was projected and the container cannot read it",
			container.VolumeMounts)
	}
	// What the pod is told, and none of it is what the SDK reads.
	env := map[string]string{}
	for _, e := range container.Env {
		env[e.Name] = e.Value
	}
	if env[EnvConfigFile] != TokenMountPath+"/"+ConfigFile {
		t.Errorf("%s is %q, want the file the configuration was projected into",
			EnvConfigFile, env[EnvConfigFile])
	}
	if env[EnvConfigProfile] != testOperator {
		t.Errorf("%s is %q, want the identity this ServiceAccount named first",
			EnvConfigProfile, env[EnvConfigProfile])
	}
	for _, name := range []string{
		"DATABRICKS_CONFIG_FILE", "DATABRICKS_CONFIG_PROFILE", "DATABRICKS_HOST",
		"DATABRICKS_CLIENT_ID", "DATABRICKS_OIDC_TOKEN_FILEPATH", "DATABRICKS_TOKEN_AUDIENCE",
	} {
		if value, set := env[name]; set {
			t.Errorf("%s was set to %q. Anything the SDK reads takes over its whole resolution, "+
				"so a workload carrying its own configuration would find that file still there, "+
				"untouched, and never read again", name, value)
		}
	}

	// And the identity itself, written in the file rather than in the
	// environment. The client id is assigned by Databricks and refused on create,
	// so nothing about the ServiceAccount predicts it.
	if config := configurationIn(t, pod); !strings.Contains(config, testClientID) {
		t.Errorf("the configuration is %q and names no client id; nothing in this pod can be "+
			"exchanged for anything", config)
	}
}

// tokenFor is the token projected for one identity, found by the path that says
// which identity it belongs to.
func tokenFor(t *testing.T, pod *corev1.Pod, request string) *corev1.ServiceAccountTokenProjection {
	t.Helper()
	wanted := TokenProjectionPathFor(request)
	for _, volume := range pod.Spec.Volumes {
		if volume.Name != TokenVolume || volume.Projected == nil {
			continue
		}
		for _, source := range volume.Projected.Sources {
			if source.ServiceAccountToken != nil && source.ServiceAccountToken.Path == wanted {
				return source.ServiceAccountToken
			}
		}
	}
	t.Fatalf("no token projected at %q; volumes are %+v", wanted, pod.Spec.Volumes)
	return nil
}

// configurationIn is what the pod carries for the container to read, and it
// fails unless something actually projects it: an annotation nothing copies
// reaches nobody, and the pod would look configured from the outside.
func configurationIn(t *testing.T, pod *corev1.Pod) string {
	t.Helper()
	var projects bool
	for _, volume := range pod.Spec.Volumes {
		if volume.Name != TokenVolume || volume.Projected == nil {
			continue
		}
		for _, source := range volume.Projected.Sources {
			if source.DownwardAPI == nil {
				continue
			}
			for _, item := range source.DownwardAPI.Items {
				if item.Path == ConfigFile && item.FieldRef != nil &&
					item.FieldRef.FieldPath == configFieldPath {
					projects = true
				}
			}
		}
	}
	if !projects {
		t.Fatalf("nothing projects %s into the container; volumes are %+v",
			ConfigAnnotation, pod.Spec.Volumes)
	}
	return pod.Annotations[ConfigAnnotation]
}

// TestAPodWithNoIdentityIsLeftAlone covers every other pod in the cluster.
//
// This webhook stands on the pod creation path. A pod whose ServiceAccount never
// asked for a Databricks identity must come out exactly as it went in.
func TestAPodWithNoIdentityIsLeftAlone(t *testing.T) {
	t.Parallel()
	i := newInjector(t)
	before := podUsing(testAccount)
	after := admit(t, i, before.DeepCopy())

	if len(after.Spec.Volumes) != 0 {
		t.Errorf("volumes are %+v for a pod whose ServiceAccount never asked", after.Spec.Volumes)
	}
	if len(after.Spec.Containers[0].Env) != 0 {
		t.Errorf("env is %+v for a pod whose ServiceAccount never asked", after.Spec.Containers[0].Env)
	}
}

// TestAPodIsAdmittedWhenTheIdentityCannotBeRead covers the read failing for a
// reason that is not the object being absent -- apiserver unreachable, cache not
// started, request timed out.
//
// The pod is admitted unequipped rather than refused. Refusing trades a legible
// failure for an illegible one: a workload that does not start, with an error
// naming this operator to somebody whose Deployment says nothing about
// Databricks, instead of a workload that starts and whose first Databricks call
// is refused with the reason on its own DatabricksServicePrincipal.
//
// Admitted and unequipped are one answer, not two. Equipping from a read that
// never arrived would mount a token for an identity nobody confirmed exists.
func TestAPodIsAdmittedWhenTheIdentityCannotBeRead(t *testing.T) {
	t.Parallel()
	unreachable := errors.New("etcdserver: request timed out")

	i := newInjector(t, identity(testNamespace, testAccount))
	i.Client = interceptor.NewClient(i.Client.(client.WithWatch), interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return unreachable
		},
	})

	request := requestFor(t, podUsing(testAccount))
	response := i.Handle(context.Background(), request)

	if !response.Allowed {
		t.Fatalf("the pod was refused because this webhook could not read: %+v. The workload "+
			"then does not start, for a reason unrelated to what it does and named nowhere its "+
			"owner would look", response.Result)
	}
	if response.Result == nil || !strings.Contains(response.Result.Message, unreachable.Error()) {
		t.Errorf("the response says %+v and does not carry %q. Nothing in internal/ logs, so "+
			"this string is the only place anybody learns the read failed",
			response.Result, unreachable)
	}

	after := podFrom(t, request, response)
	if len(after.Spec.Volumes) != 0 {
		t.Errorf("volumes are %+v; the identity these were built from was never read",
			after.Spec.Volumes)
	}
	if len(after.Spec.Containers[0].VolumeMounts) != 0 {
		t.Errorf("mounts are %+v on a pod nothing was projected into",
			after.Spec.Containers[0].VolumeMounts)
	}
	if len(after.Spec.Containers[0].Env) != 0 {
		t.Errorf("env is %+v; the container is told where a configuration is that no read "+
			"produced", after.Spec.Containers[0].Env)
	}
}

// TestAnIdentityWithNoClientIdYetChangesNothing covers the window between the
// object being created and Databricks having answered.
//
// The client id is assigned on create and cannot be derived, so there is nothing
// to tell the workload yet. Injecting a volume without it would produce a pod
// that looks equipped and is not.
func TestAnIdentityWithNoClientIdYetChangesNothing(t *testing.T) {
	t.Parallel()
	principal := identity(testNamespace, testAccount)
	principal.Status.Identities[0].ClientID = ""
	i := newInjector(t, principal)

	pod := admit(t, i, podUsing(testAccount))
	if len(pod.Spec.Volumes) != 0 {
		t.Errorf("volumes are %+v before Databricks assigned a client id", pod.Spec.Volumes)
	}
}

// TestWhatSomebodyWroteIsNotOverwritten covers a pod whose author wanted control
// of this.
//
// A volume of this name, or an environment variable already set, was written
// deliberately. Replacing it would be the webhook overruling them silently,
// which is worse than not helping.
func TestWhatSomebodyWroteIsNotOverwritten(t *testing.T) {
	t.Parallel()
	i := newInjector(t, identity(testNamespace, testAccount))

	pod := podUsing(testAccount)
	pod.Spec.Volumes = []corev1.Volume{{
		Name:         TokenVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}
	pod.Spec.Containers[0].Env = []corev1.EnvVar{
		{Name: EnvConfigProfile, Value: "somebody-elses-choice"},
	}

	after := admit(t, i, pod)
	if len(after.Spec.Volumes) != 1 || after.Spec.Volumes[0].EmptyDir == nil {
		t.Errorf("volumes are %+v; the one already there was written on purpose", after.Spec.Volumes)
	}
	for _, e := range after.Spec.Containers[0].Env {
		if e.Name == EnvConfigProfile && e.Value != "somebody-elses-choice" {
			t.Errorf("%s was rewritten to %q", EnvConfigProfile, e.Value)
		}
	}

	// What was added, which is the half this test used to leave out. Asserting
	// only that nothing was overwritten certified a promise the code did not
	// keep: it skipped the volume and the mount and then set the file path
	// anyway, pointing the workload at a path that may hold nothing, or at
	// somebody else's volume mounted where they did not put it. A workload told
	// where its token is when it is not there fails further from the cause than
	// one told nothing.
	for _, e := range after.Spec.Containers[0].Env {
		if e.Name == EnvConfigFile {
			t.Errorf("%s was set to %q on a pod this webhook decided not to equip",
				EnvConfigFile, e.Value)
		}
	}
	if len(after.Spec.Containers[0].VolumeMounts) != 0 {
		t.Errorf("mounts are %+v; nothing was to be added to a pod somebody wrote themselves",
			after.Spec.Containers[0].VolumeMounts)
	}
}

// TestInitContainersAreEquippedToo covers a workload that reaches Databricks
// before it starts. It needs the token as much as the one that reaches it while
// running, and forgetting init containers fails only for those workloads.
func TestInitContainersAreEquippedToo(t *testing.T) {
	t.Parallel()
	i := newInjector(t, identity(testNamespace, testAccount))
	pod := podUsing(testAccount)
	pod.Spec.InitContainers = []corev1.Container{{Name: "prepare", Image: "busybox"}}

	after := admit(t, i, pod)
	init := after.Spec.InitContainers[0]
	if !hasMountPath(init.VolumeMounts, TokenMountPath) {
		t.Errorf("init container mounts are %+v, want the token", init.VolumeMounts)
	}
	if _, set := envOf(init, EnvConfigProfile); !set {
		t.Errorf("init container env is %+v, want what says which identity it was given", init.Env)
	}
}

// TestAPodWithNoServiceAccountNamedUsesDefault covers the pod spec's own
// default. Kubernetes runs such a pod as "default", so that is the identity to
// look for -- and a "default" ServiceAccount that asked is as entitled as any
// other.
func TestAPodWithNoServiceAccountNamedUsesDefault(t *testing.T) {
	t.Parallel()
	i := newInjector(t, identity(testNamespace, "default"))
	pod := admit(t, i, podUsing(""))

	if !hasVolume(pod.Spec.Volumes, TokenVolume) {
		t.Errorf("volumes are %+v; the pod runs as default, which has an identity", pod.Spec.Volumes)
	}
}

// applyPatch applies a JSON Patch, so a test can assert on the pod that comes
// out rather than on the operations that make it.
func applyPatch(original, patch []byte) ([]byte, error) {
	decoded, err := jsonpatch.DecodePatch(patch)
	if err != nil {
		return nil, err
	}
	return decoded.Apply(original)
}

// envOf returns what a container sets for one name, and whether it sets it.
func envOf(container corev1.Container, name string) (string, bool) {
	for _, e := range container.Env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// TestTheProfileNamesNoWorkspace is what this operator decided not to decide.
//
// A host is a destination and this issues identities. It cannot verify one --
// it knows the account it acts in, and a workload wants a workspace -- so
// writing one would be repeating a string nobody checks.
//
// Nothing is lost by leaving it out, and this is where that is measured rather
// than assumed: the SDK resolves a profile with no host at all, and takes
// DATABRICKS_HOST from the environment. So the workload names its own workspace
// in its own Deployment, under the SDK's own name.
func TestTheProfileNamesNoWorkspace(t *testing.T) {
	t.Parallel()
	i := newInjector(t, identity(testNamespace, testAccount))

	rendered := configurationIn(t, admit(t, i, podUsing(testAccount)))
	if strings.Contains(rendered, "host") {
		t.Errorf("the configuration is %q and names a host. This operator cannot check one, so "+
			"writing it would put a value nobody verifies in an object that is not where the "+
			"workload's other configuration lives", rendered)
	}
	if host := resolvedProfile(t, rendered, testOperator).Host; host != "" {
		t.Errorf("the SDK resolved host %q out of a profile that names none", host)
	}
}

// TestWhatThePodIsGivenIsWhatTheSdkReads is the assertion this file used to make
// against the text of the configuration, and text was the wrong thing to ask.
//
// It asserted that "token_audience = " appeared, saying the profile would not
// resolve without it. Both halves were false: the SDK's own name for that field
// is "audience" -- config.Config declares TokenAudience with name:"audience" --
// so what this webhook wrote was dropped without a word, and the profile
// resolved cleanly all the same, carrying no audience at all. Measured against
// v0.176.0 with the profile below: as token_audience the SDK resolves
// TokenAudience="", as audience it resolves "databricks".
func TestWhatThePodIsGivenIsWhatTheSdkReads(t *testing.T) {
	t.Parallel()
	i := newInjector(t, identity(testNamespace, testAccount))

	rendered := configurationIn(t, admit(t, i, podUsing(testAccount)))
	cfg := resolvedProfile(t, rendered, testOperator)

	if cfg.TokenAudience != testAudience {
		t.Errorf("the SDK resolved audience %q out of a profile written for %q; a key it does "+
			"not know is dropped in silence, so the pod carries a profile that never says what "+
			"its token was minted for and nothing about it looks wrong",
			cfg.TokenAudience, testAudience)
	}
	if cfg.ClientID != testClientID {
		t.Errorf("the SDK resolved client_id %q; a profile naming no client cannot be exchanged "+
			"for anything", cfg.ClientID)
	}
	if cfg.OIDCTokenFilepath != TokenPathFor(testOperator) {
		t.Errorf("the SDK reads the token from %q and kubelet projects it at %q",
			cfg.OIDCTokenFilepath, TokenPathFor(testOperator))
	}
	if cfg.AuthType != "file-oidc" {
		t.Errorf("the SDK resolved auth_type %q. Without it resolution stops at \"more than one "+
			"authorization method configured: file-oidc and oauth\", because client_id marks one "+
			"family and the token path marks another", cfg.AuthType)
	}
}

// TestAWorkloadsOwnConfigurationIsNotDisplaced is the property the whole shape
// exists for, and the reason nothing here sets a name the SDK reads.
//
// A workload may carry its own .databrickscfg, baked into its image or mounted.
// Setting DATABRICKS_CONFIG_FILE would not add a value to that workload -- it
// would take over the SDK's entire resolution, and the file it brought would
// still be there, untouched, and never read again. Measured: an environment
// variable beats a profile silently, so the workload could not find out, and
// this operator cannot see what an image contains.
func TestAWorkloadsOwnConfigurationIsNotDisplaced(t *testing.T) {
	t.Parallel()
	const theirs = "/etc/databricks/their-own.cfg"

	i := newInjector(t, identity(testNamespace, testAccount))

	pod := podUsing(testAccount)
	pod.Spec.Containers[0].Env = []corev1.EnvVar{
		{Name: "DATABRICKS_CONFIG_FILE", Value: theirs},
		{Name: "DATABRICKS_CONFIG_PROFILE", Value: "the-one-they-meant"},
	}

	after := admit(t, i, pod)
	if got, _ := envOf(after.Spec.Containers[0], "DATABRICKS_CONFIG_FILE"); got != theirs {
		t.Errorf("DATABRICKS_CONFIG_FILE is %q, want the workload's own %q", got, theirs)
	}
	if got, _ := envOf(after.Spec.Containers[0], "DATABRICKS_CONFIG_PROFILE"); got != "the-one-they-meant" {
		t.Errorf("DATABRICKS_CONFIG_PROFILE is %q, want the workload's own", got)
	}

	// And it was still equipped. Leaving the workload's own configuration alone
	// is not the same as refusing to give it anything: what this operator
	// provides is published beside it, under names nothing reads by itself.
	if _, set := envOf(after.Spec.Containers[0], EnvConfigFile); !set {
		t.Errorf("%s was not set; a workload that brought its own configuration is still "+
			"entitled to be told what it was issued", EnvConfigFile)
	}
}

// TestAContainerAlreadySettingTheseNamesKeepsItsOwnValues covers a workload that
// publishes this operator's own names itself.
//
// A container that sets one of them was written by somebody who decided what it
// should say -- pointing at a configuration they assembled, or at a profile
// other than the one the status happens to carry first. Two entries of one name
// in a container's env is a pod the API server accepts and kubelet resolves to
// the last, so appending would take that decision away with nothing anywhere
// saying it happened.
func TestAContainerAlreadySettingTheseNamesKeepsItsOwnValues(t *testing.T) {
	t.Parallel()
	const (
		theirFile    = "/etc/databricks/assembled-by-them"
		theirProfile = "the-one-they-meant"
	)

	i := newInjector(t, identity(testNamespace, testAccount))
	pod := podUsing(testAccount)
	pod.Spec.Containers[0].Env = []corev1.EnvVar{
		{Name: EnvConfigFile, Value: theirFile},
		{Name: EnvConfigProfile, Value: theirProfile},
	}

	container := admit(t, i, pod).Spec.Containers[0]

	// The count, not just the value: a container holding both entries would read
	// as correct here whenever the workload's happened to come second.
	set := map[string][]string{}
	for _, e := range container.Env {
		set[e.Name] = append(set[e.Name], e.Value)
	}
	for name, want := range map[string]string{EnvConfigFile: theirFile, EnvConfigProfile: theirProfile} {
		if got := set[name]; len(got) != 1 || got[0] != want {
			t.Errorf("%s is %q, want exactly one entry holding %q. kubelet resolves the last of "+
				"several, so a second entry is this webhook overruling the container silently",
				name, got, want)
		}
	}

	// And it was equipped all the same. Nothing about this container collides
	// with the volume or the mount, so withholding those would punish it for
	// having said where its configuration is.
	if len(container.VolumeMounts) != 1 || container.VolumeMounts[0].Name != TokenVolume {
		t.Errorf("mounts are %+v; the token was projected and this container cannot read it",
			container.VolumeMounts)
	}
}

// TestAPodAlreadyUsingThisPathIsLeftAlone covers the webhook making a pod the
// API server refuses.
//
// A mutating webhook runs before validation, so a second mount on one path is
// not a mistake the API server tidies up -- it is "mountPath: Invalid value:
// \"/var/run/secrets/databricks\": must be unique", and the pod is never
// created. failurePolicy: Ignore does not help: the call succeeded. The workload
// stops being created, with an error naming neither this operator nor anything
// its owner recognises, and nothing the owner can do while the namespace carries
// the inject label.
//
// The way in is a team that hand-rolled the projected token at this path before
// being enrolled -- the path this operator's own Deployment uses.
func TestAPodAlreadyUsingThisPathIsLeftAlone(t *testing.T) {
	t.Parallel()
	i := newInjector(t, identity(testNamespace, testAccount))
	pod := podUsing(testAccount)
	pod.Spec.Volumes = []corev1.Volume{{Name: "their-own-token"}}
	pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
		Name: "their-own-token", MountPath: TokenMountPath,
	}}

	after := admit(t, i, pod)

	var atPath []string
	for _, m := range after.Spec.Containers[0].VolumeMounts {
		if m.MountPath == TokenMountPath {
			atPath = append(atPath, m.Name)
		}
	}
	if len(atPath) != 1 {
		t.Errorf("%d mounts on %s (%v); the API server refuses that pod, and this webhook is "+
			"what made it", len(atPath), TokenMountPath, atPath)
	}
	if atPath[0] != "their-own-token" {
		t.Errorf("the mount at %s is %q; what was already there was replaced", TokenMountPath, atPath[0])
	}

	// The variables are withheld with the mount. They would name a file this
	// container does not have, and a workload told where its configuration is
	// when it is not there fails further from the cause than one told nothing.
	for _, e := range after.Spec.Containers[0].Env {
		if e.Name == EnvConfigFile || e.Name == EnvConfigProfile {
			t.Errorf("%s is set to %q on a container that was given nothing",
				e.Name, e.Value)
		}
	}
}

// TestAPodCarryingThisVolumeNameIsLeftAlone covers the other half of the same
// collision.
//
// A volume of this name that this webhook did not add belongs to somebody else,
// and the old code skipped creating its own and then mounted theirs under this
// operator's variables -- handing the workload a file that is not a token and an
// error that says nothing about why. Seeing a pod this webhook already equipped
// is the same case and the same answer.
func TestAPodCarryingThisVolumeNameIsLeftAlone(t *testing.T) {
	t.Parallel()
	i := newInjector(t, identity(testNamespace, testAccount))
	pod := podUsing(testAccount)
	theirs := corev1.Volume{Name: TokenVolume, VolumeSource: corev1.VolumeSource{
		EmptyDir: &corev1.EmptyDirVolumeSource{},
	}}
	pod.Spec.Volumes = []corev1.Volume{theirs}

	after := admit(t, i, pod)

	if len(after.Spec.Volumes) != 1 || after.Spec.Volumes[0].EmptyDir == nil {
		t.Errorf("volumes are %+v; what was already named %s was replaced or added to",
			after.Spec.Volumes, TokenVolume)
	}
	if len(after.Spec.Containers[0].VolumeMounts) != 0 {
		t.Errorf("mounts are %+v; the container was pointed at somebody else's volume",
			after.Spec.Containers[0].VolumeMounts)
	}
}

// TestAPodCarriesEveryIdentityItWasIssued is the whole reason the pod-facing
// shape changed.
//
// A workload reading with one service principal and writing with another holds
// both at once, and both are real: two tokens with their own audiences, two
// profiles with their own client ids. Before this, a pod was given the first and
// the second existed in Databricks with nothing in the cluster able to use it.
func TestAPodCarriesEveryIdentityItWasIssued(t *testing.T) {
	t.Parallel()
	const (
		reader = "reader"
		writer = "ops-b/databricks-account"
	)

	principal := identity(testNamespace, testAccount)
	principal.Status.Identities = []dbxv1alpha1.ProjectedIdentity{
		{Request: reader, Operator: testOperator, ClientID: "reader-client", Audience: "databricks"},
		// Another operator's unnamed identity, which is why its profile is that
		// operator's reference. Its audience is its own: nothing asks two platform
		// teams to agree on one.
		{Request: writer, Operator: writer, ClientID: "writer-client", Audience: "some-other-aud"},
	}
	i := newInjector(t, principal)
	pod := admit(t, i, podUsing(testAccount))

	if got := tokenFor(t, pod, reader).Audience; got != "databricks" {
		t.Errorf("the reader's token is minted for %q", got)
	}
	if got := tokenFor(t, pod, writer).Audience; got != "some-other-aud" {
		t.Errorf("the writer's token is minted for %q; two operators need not share an "+
			"audience, and this one carries whichever its own token does", got)
	}

	rendered := configurationIn(t, pod)
	if got := resolvedProfile(t, rendered, reader).ClientID; got != "reader-client" {
		t.Errorf("profile %q resolves client_id %q; the two identities were rendered into one "+
			"file and one of them did not survive it", reader, got)
	}
	if got := resolvedProfile(t, rendered, writer).ClientID; got != "writer-client" {
		t.Errorf("profile %q resolves client_id %q; before this the second identity existed in "+
			"Databricks with nothing in the cluster able to use it", writer, got)
	}

	// The default is whichever the status carries first, and the status puts the
	// unnamed identity there. Nothing here is an order its owner wrote: the
	// request is a set of annotation keys, and a map has none.
	if got, _ := envOf(pod.Spec.Containers[0], EnvConfigProfile); got != reader {
		t.Errorf("%s is %q, want the identity the status carries first", EnvConfigProfile, got)
	}
}

// TestAnIdentityIsCalledWhatItsAnnotationKeyCalledIt covers the promise the
// annotation shape exists to keep.
//
// A named identity's profile is the name in its own annotation key, verbatim, so
// the string a person wrote is the string their code passes to the SDK and the
// directory its token appears in. Deriving one would be a rule the workload's
// author has to learn before they can name what they asked for.
func TestAnIdentityIsCalledWhatItsAnnotationKeyCalledIt(t *testing.T) {
	t.Parallel()
	principal := identity(testNamespace, testAccount)
	principal.Status.Identities = []dbxv1alpha1.ProjectedIdentity{
		{Request: "reader", Operator: testOperator, ClientID: "reader-client", Audience: testAudience},
	}
	i := newInjector(t, principal)
	pod := admit(t, i, podUsing(testAccount))

	rendered := configurationIn(t, pod)
	if !strings.Contains(rendered, "[reader]\n") {
		t.Errorf("the configuration is %q; a ServiceAccount that asked under \"reader\" has to "+
			"pass the SDK something else to reach the identity it asked for", rendered)
	}
	if got, _ := envOf(pod.Spec.Containers[0], EnvConfigProfile); got != "reader" {
		t.Errorf("%s is %q, want the name in the annotation key", EnvConfigProfile, got)
	}

	// One segment, and the whole of it is the name. The unnamed identity is the
	// only profile holding a slash, so it is the only one whose token sits a
	// directory deeper.
	if got := TokenProjectionPathFor("reader"); got != "reader/token" {
		t.Errorf("kubelet is told to project the token at %q inside the volume", got)
	}
	at := TokenMountPath + "/reader/token"
	if got := resolvedProfile(t, rendered, "reader").OIDCTokenFilepath; got != at {
		t.Errorf("the profile sends the SDK to %q and the token is mounted at %q", got, at)
	}
	if got := tokenFor(t, pod, "reader").Audience; got != testAudience {
		t.Errorf("the token projected where that profile reads is minted for %q", got)
	}
}

// TestAnIdentityWithoutAClientIdYetDoesNotHoldBackItsNeighbours covers a pod
// created while one of two identities is still converging.
//
// Databricks assigns the client id on create, so one identity can be usable
// while another is not. Writing a profile with no client id would give the
// workload something that fails as TOKEN_INVALID with the federation policy
// echoed back, and withholding the whole pod would take away an identity that
// works because a different one does not yet.
func TestAnIdentityWithoutAClientIdYetDoesNotHoldBackItsNeighbours(t *testing.T) {
	t.Parallel()
	const ready = "reader"

	principal := identity(testNamespace, testAccount)
	principal.Status.Identities = []dbxv1alpha1.ProjectedIdentity{
		{Request: ready, Operator: testOperator, ClientID: "reader-client", Audience: testAudience},
		{Request: "writer", Operator: testOperator, Audience: testAudience},
	}
	i := newInjector(t, principal)
	pod := admit(t, i, podUsing(testAccount))

	rendered := configurationIn(t, pod)
	if !strings.Contains(rendered, "["+ready+"]") {
		t.Errorf("the configuration is %q; the identity that had converged was withheld "+
			"because another had not", rendered)
	}
	if strings.Contains(rendered, "[writer]") {
		t.Errorf("the configuration is %q; a profile with no client id cannot be exchanged for "+
			"anything and fails as though the federation policy were wrong", rendered)
	}
	if len(pod.Spec.Volumes[0].Projected.Sources) != 2 {
		t.Errorf("sources are %+v, want one token and the configuration",
			pod.Spec.Volumes[0].Projected.Sources)
	}
}
