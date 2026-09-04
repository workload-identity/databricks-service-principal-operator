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

package main

import (
	"crypto/tls"
	"flag"
	"os"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	databricksworkloadgrantsiov1alpha1 "github.com/workload-identity/databricks-service-principal-operator/api/v1alpha1"
	"github.com/workload-identity/databricks-service-principal-operator/internal/controller"
	"github.com/workload-identity/databricks-service-principal-operator/internal/databricks"
	dbxwebhook "github.com/workload-identity/databricks-service-principal-operator/internal/webhook"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(databricksworkloadgrantsiov1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// settings is everything the flags and the environment decided, in one place.
//
// It exists so that the assembly below can be tested. Every one of these reaches
// a reconciler field and nothing else does, and a value that stops being passed
// is a failure nothing else notices: the reconcilers' own tests set their fields
// directly, and the manifest test only checks that a flag is still in the args.
// Between those two is where a dropped assignment lives, and this is what makes
// that stretch reachable.
type settings struct {
	// Namespace is the operator's own, which is where the DatabricksAccount is
	// read from and where the records of what was issued are kept.
	Namespace string
	Account   types.NamespacedName
	Holder    *databricks.Holder
	Runtime   databricks.Config
}

// reconcilers builds all three controllers from what was decided. It starts
// nothing and contacts nothing.
//
// live is a reader that goes to the API server rather than to the cache. Only
// one place uses it, and it is the read that decides whether an identity is
// destroyed.
func reconcilers(s settings, c client.Client, live client.Reader, scheme *runtime.Scheme) (
	*controller.DatabricksAccountReconciler,
	*controller.DatabricksServicePrincipalReconciler,
	*controller.IssuedDatabricksServicePrincipalReconciler) {
	return &controller.DatabricksAccountReconciler{
			Client:  c,
			Scheme:  scheme,
			Account: s.Account,
			Holder:  s.Holder,
			Runtime: s.Runtime,
		}, &controller.DatabricksServicePrincipalReconciler{
			Client:  c,
			Scheme:  scheme,
			Records: s.Namespace,
			Account: s.Account,
		}, &controller.IssuedDatabricksServicePrincipalReconciler{
			Client:     c,
			Scheme:     scheme,
			Databricks: s.Holder,
			Live:       live,
			Account:    s.Account,
			// The path, not what was read from it once. See TokenPath.
			TokenPath: s.Runtime.OIDCTokenFilepath,
		}
}

func main() {
	// The flags that are only a value write into this directly, so there is no
	// copy between the flag and the field for anybody to drop. What is computed
	// or built rather than parsed is filled in below.
	var decided settings

	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var accountName string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&accountName, "databricks-account", "databricks-account",
		"The name of the DatabricksAccount, in the operator's own namespace, that says which Databricks "+
			"account to act in. Others are reported as not selected rather than acted on.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	// The operator's own namespace, from the downward API. It is where the
	// DatabricksAccount lives and where the records of what was issued are kept,
	// and taking it from Kubernetes rather than from a value somebody wrote
	// means the operator instructions never have to mention how this repository
	// names things.
	//
	// Read before the manager is built, because the manager's cache is scoped by
	// it: an operator that cannot say which namespace is its own has nothing to
	// scope to, and starting anyway would mean watching every namespace's.
	namespace := os.Getenv(controller.EnvPodNamespace)
	if namespace == "" {
		setupLog.Error(nil, "Failed to determine the operator's own namespace",
			"variable", controller.EnvPodNamespace)
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Cache:                  controller.CacheOptions(namespace),
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "f6626f90.databricks.workload-identity.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	account := types.NamespacedName{Namespace: namespace, Name: accountName}

	runtimeCfg := databricks.RuntimeFromEnv()
	if !runtimeCfg.UsesWorkloadIdentity() {
		// In a cluster this is always set, so reaching here means the Deployment
		// lost it and authentication has quietly fallen back to whatever the SDK
		// can find. Say so rather than letting it look normal.
		setupLog.Info("Databricks authentication is left to the SDK: no projected token file is configured",
			"variable", databricks.EnvOIDCTokenFilepath)
	}

	// Nothing is built here, and startup does not depend on Databricks being
	// reachable or even declared. The holder answers NotConfigured until the
	// account controller has clients that work, so an operator with no
	// DatabricksAccount runs and says so on every object, rather than
	// crashlooping with the reason only in its log.
	dbClients := databricks.NewHolder(namespace, accountName)

	decided.Namespace = namespace
	decided.Account = account
	decided.Holder = dbClients
	decided.Runtime = runtimeCfg

	accountReconciler, projectionReconciler, issuedReconciler := reconcilers(
		decided, mgr.GetClient(), mgr.GetAPIReader(), mgr.GetScheme())

	if err := accountReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "databricksaccount")
		os.Exit(1)
	}
	if err := issuedReconciler.SetupWithManager(mgr, namespace); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "issueddatabricksserviceprincipal")
		os.Exit(1)
	}
	if err := projectionReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "databricksserviceprincipal")
		os.Exit(1)
	}
	// The webhook that puts a Databricks-audienced token into the pods of
	// ServiceAccounts that have an identity. Without it every workload's author
	// writes the same fifteen lines of projected volume into their own
	// Deployment -- and the token every pod already has carries the API server's
	// audience, so it can never be exchanged.
	mgr.GetWebhookServer().Register(dbxwebhook.Path, &webhook.Admission{
		Handler: &dbxwebhook.PodTokenInjector{
			Client:  mgr.GetClient(),
			Decoder: admission.NewDecoder(mgr.GetScheme()),
		},
	})

	// +kubebuilder:scaffold:builder

	// Liveness is a ping: a process that is running is not one to kill.
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	// Readiness is not, and this is what decides when this pod starts being sent
	// pods to admit. Answering before the cache has filled admits every one of
	// them unchanged -- no token, no variables -- and nothing rewrites a running
	// pod afterwards.
	synced := &controller.CacheSynced{}
	if err := mgr.Add(synced); err != nil {
		setupLog.Error(err, "Failed to set up the readiness gate")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("cache", synced.Check); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
