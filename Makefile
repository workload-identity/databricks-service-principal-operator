# Image URL to use all building/pushing image targets
IMG ?= controller:latest
# YEAR defines the year value used for substituting the YEAR placeholder in the boilerplate header.
YEAR ?= $(shell date +%Y)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" applyconfiguration:headerFile="hack/boilerplate.go.txt" paths="./api/..."
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt",year=$(YEAR) paths="./..."

# The apply configurations are how the DatabricksServiceAccount is written. They
# are the only form that can send "this instance has no opinion about host"
# rather than "host is empty", and an instance owns whatever it sends -- so a
# type edited without this rerun is an instance claiming values it never set.
# Kept in `generate` rather than a target of its own for that reason: there is
# no state in which regenerating deepcopy and not these is correct.
#
# First, and that is not cosmetic. The deepcopy line loads ./... , which includes
# the controller that imports these, so on a clean checkout -- or after this
# directory is deleted -- running it first fails to load the package and no
# generator runs at all.

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

# Four ways to run tests, and what tells them apart is what each one needs. An
# entry says what it needs and refuses without it, so a run that could not
# answer anything never looks like a run that passed:
#
#   make test             nothing
#   make test-cluster     a Kubernetes cluster (kind, made and destroyed here)
#   make test-databricks  a Databricks account
#   make test-e2e         both, and it is the only one that proves the whole chain
#   make test-sharing     both, and it installs a second operator to do it
#
# Each layer is built behind its own tag, so the ones you did not ask for are
# not compiled and there is nothing to skip.
#
# kustomize is a prerequisite because the tests that read the rendered manifests
# -- the cert-manager wiring, the flags a patch can drop, the token volume, the
# DNS name budget -- shell out to bin/kustomize and skip themselves when it is
# not there. go test still prints ok for the package, so without this line a
# fresh clone's first green run is eight assertions short and says so nowhere.
.PHONY: test
test: manifests generate fmt vet kustomize setup-envtest ## Run the tests that need nothing.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /test/) -coverprofile cover.out

# These answer questions only Databricks can answer, against somebody's
# real account, and nothing about that account is in this repository. They carry
# a build tag, so `make test` does not compile them and a contributor without an
# account sees no skipped tests to learn to ignore.
#
# The variables are checked here rather than only inside the tests, because a
# `go test` that reaches no test still prints ok. Refusing before anything
# compiles is the only place the refusal is visible.
#
# These are the SDK's own names. An environment already set up for the CLI needs
# nothing else: with the host set, the SDK finds the matching profile and
# authenticates as whoever is logged in.
.PHONY: test-databricks
test-databricks: fmt vet ## Run the tests that need a Databricks account. Needs DATABRICKS_HOST and DATABRICKS_ACCOUNT_ID.
	@test -n "$(DATABRICKS_HOST)" || { echo "DATABRICKS_HOST is not set. These tests are run against a real Databricks account; nothing about one is in this repository."; exit 1; }
	@test -n "$(DATABRICKS_ACCOUNT_ID)" || { echo "DATABRICKS_ACCOUNT_ID is not set. These tests are run against a real Databricks account; nothing about one is in this repository."; exit 1; }
	go test -tags=databricks -count=1 -v -timeout $(DATABRICKS_TIMEOUT) ./internal/databricks/ -run TestLive

# DATABRICKS_TIMEOUT is generous because every assertion about an absence polls. A
# delete becomes visible to a read somewhere between 235ms and 7.6s on one
# measured account, and the long end was under load -- so the tests wait, and
# waiting is what they are for.
DATABRICKS_TIMEOUT ?= 15m

# TODO(user): To use a different vendor, modify the setup under 'test/cluster'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# kubectl kuberc is disabled by default for test isolation; enable with:
# - KUBECTL_KUBERC=true
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true
# Short, and it has to stay short. Kind names the node <cluster>-control-plane,
# the API server stamps that name as a label value on its own identity Lease, and
# a label value holds 63 characters. At 64 the Lease is refused, the API server
# retries it forever, the node is never registered, and kubeadm gives up with
# "nodes ... not found" -- which reads as a broken machine rather than a name one
# character too long. Measured: this was
# databricks-service-principal-operator-test-cluster, 50 characters, and every
# fresh create failed. So the ceiling here is 49.
KIND_CLUSTER ?= dbxsp-operator-test-cluster
# Pinned, and not the default. The node image decides the Kubernetes version,
# and cert-manager's CRDs need 1.30 or newer -- on an older one the suite fails
# while installing cert-manager, which reads as a cert-manager problem.
KIND_NODE_IMAGE ?= kindest/node:v1.34.0

.PHONY: setup-test-cluster
setup-test-cluster: kind ## Set up a Kind cluster for the cluster tests if it does not exist
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) --image $(KIND_NODE_IMAGE) ;; \
	esac

# CLUSTER_TIMEOUT replaces go test's ten-minute default, which this suite has to be
# out from under whatever it is set to. Hitting that default does not read as
# running out of time: the suite is killed mid-spec, the panic that surfaces is
# in whatever AfterSuite was doing, and what gets reported is a stack trace
# inside cleanup.
#
# Passing, the suite takes under two minutes. Failing, it takes longer but not
# without bound: each Describe is Ordered, so one failure skips the rest of its
# container, and the worst case is one timed-out spec per container -- around
# ten minutes with the timeouts the specs carry. This is twice that.
CLUSTER_TIMEOUT ?= 20m

# CLUSTER_ARGS is what makes the seed printed at the top of every run worth
# printing. The order the containers run in is shuffled, and it has already
# hidden one spec that only passed when it ran late; running the seed back is
# how such a failure is looked at twice:
#
#   make test-cluster CLUSTER_ARGS=-ginkgo.seed=1788376140
CLUSTER_ARGS ?=

.PHONY: test-cluster
test-cluster: setup-test-cluster manifests generate fmt vet ## Run the tests that need a cluster. Uses an isolated Kind cluster.
	KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) go test -tags=cluster ./test/cluster/ -v -ginkgo.v -timeout $(CLUSTER_TIMEOUT) $(CLUSTER_ARGS)
	$(MAKE) cleanup-test-cluster

.PHONY: cleanup-test-cluster
cleanup-test-cluster: ## Tear down the Kind cluster used by the cluster tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

# The end-to-end tests need both sides, and they are the only ones that can say
# the sentence this operator is for: a ServiceAccount's own token, presented from
# inside a pod, is accepted by Databricks as the service principal the operator
# created, with no credential written anywhere.
#
# They run against an operator somebody already installed. This target does not
# install one: the operator authenticates as a service principal whose federation
# policy names its own subject, and creating that is a bootstrap a person does
# once. The cluster is named rather than taken from the current context, because
# this suite creates and deletes namespaces and every one of those succeeds
# quietly on a cluster it reached by accident.
#
# The cluster's OIDC issuer has to be reachable from Databricks, which is what
# rules out kind and is why this is a layer of its own.
.PHONY: test-e2e
test-e2e: ginkgo fmt vet ## Run the tests that need a cluster AND a Databricks account. See the variables below.
	@test -n "$(DATABRICKS_HOST)" || { echo "DATABRICKS_HOST is not set."; exit 1; }
	@test -n "$(DATABRICKS_ACCOUNT_ID)" || { echo "DATABRICKS_ACCOUNT_ID is not set."; exit 1; }
	@test -n "$(E2E_KUBE_CONTEXT)" || { echo "E2E_KUBE_CONTEXT is not set. Name the cluster the operator is installed on; this suite will not guess from the current context."; exit 1; }
	@test -n "$(E2E_OPERATOR_NAMESPACE)" || { echo "E2E_OPERATOR_NAMESPACE is not set. Name the namespace the operator runs in; it is half of how a ServiceAccount names it."; exit 1; }
	$(GINKGO) --tags=e2e --procs=$(E2E_PROCS) -v --timeout=$(E2E_TIMEOUT) ./test/e2e/

# E2E_PROCS is how many of the suite's containers run at once.
#
# They can, because each one holds its own namespace and its own identities, and
# the one thing they share -- the list of namespaces this operator serves -- is
# appended to rather than assigned. Serially the suite is the sum of its
# containers; in parallel it is the longest one.
#
# The number is small on purpose. Every container is minting and destroying real
# identities in one Databricks account, and what that account does under load is
# measured rather than assumed: a read that follows a delete took 235ms quiet and
# 7.6s with ten of these running at once.
E2E_PROCS ?= 4

.PHONY: ginkgo
ginkgo: $(LOCALBIN)
	$(call go-install-tool,$(GINKGO),github.com/onsi/ginkgo/v2/ginkgo,$(GINKGO_VERSION))

# E2E_TIMEOUT: minting is several calls to Databricks per identity, and every
# assertion about an absence polls rather than reading once.
E2E_TIMEOUT ?= 30m

# Two operators on one cluster, sharing one Databricks account. Its own entry
# because of what it needs beyond the layer below: it installs a whole second
# operator, which means twelve more cluster-scoped objects and a second identity
# in the account. Nothing else here writes anything cluster-wide, and a suite
# that does should be the one you asked for rather than one that came along.
#
# The same file holds both suites, behind different tags, so that how a suite
# reaches the cluster is written once. Two copies of that become two answers to
# it, and then a failure can mean the suite is wrong rather than the operator.
.PHONY: test-sharing
test-sharing: ginkgo fmt vet ## Run the tests that need two operators on one cluster. Same variables as test-e2e.
	@test -n "$(DATABRICKS_HOST)" || { echo "DATABRICKS_HOST is not set."; exit 1; }
	@test -n "$(DATABRICKS_ACCOUNT_ID)" || { echo "DATABRICKS_ACCOUNT_ID is not set."; exit 1; }
	@test -n "$(E2E_KUBE_CONTEXT)" || { echo "E2E_KUBE_CONTEXT is not set. Name the cluster the operator is installed on; this suite will not guess from the current context."; exit 1; }
	@test -n "$(E2E_OPERATOR_NAMESPACE)" || { echo "E2E_OPERATOR_NAMESPACE is not set. Name the namespace the operator runs in; it is half of how a ServiceAccount names it."; exit 1; }
	$(GINKGO) --tags=sharing --procs=1 -v --timeout=$(SHARING_TIMEOUT) ./test/e2e/

# One process, not four: this suite installs and removes an operator that every
# other spec on the cluster would be reconciled by.
SHARING_TIMEOUT ?= 30m

.PHONY: kind
kind: $(LOCALBIN)
	$(call go-install-tool,$(KIND),sigs.k8s.io/kind,$(KIND_VERSION))

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name databricks-service-principal-operator-builder
	$(CONTAINER_TOOL) buildx use databricks-service-principal-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm databricks-service-principal-operator-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: check-cert-manager
check-cert-manager: ## Refuse to deploy into a cluster that has no cert-manager.
	@if ! "$(KUBECTL)" cluster-info >/dev/null 2>&1; then \
		echo ""; \
		echo "kubectl cannot reach a cluster, so there is nothing to check or deploy."; \
		echo "Check which context is selected: kubectl config current-context"; \
		echo ""; \
		exit 1; \
	fi
	@if ! "$(KUBECTL)" get crd certificates.cert-manager.io >/dev/null 2>&1; then \
		echo ""; \
		echo "cert-manager is not installed in this cluster, so this deploy is stopping."; \
		echo ""; \
		echo "The webhook serves TLS and cert-manager is what issues its certificate."; \
		echo "Applying anyway would not fail cleanly: everything lands except the"; \
		echo "Certificate and the Issuer, leaving the webhook installed with nothing"; \
		echo "to serve. Its failurePolicy is Ignore, so from then on pods start with"; \
		echo "no token and nothing reports it."; \
		echo ""; \
		echo "Install cert-manager -- any version serving cert-manager.io/v1 -- and"; \
		echo "run this again."; \
		echo ""; \
		exit 1; \
	fi

.PHONY: deploy
deploy: manifests kustomize check-cert-manager ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	@cp config/manager/kustomization.yaml config/manager/kustomization.yaml.deploying
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply -f -
	@# Put back, always. `kustomize edit` writes IMG into a tracked file and
	@# leaves it there, so a deploy followed by a commit publishes the registry
	@# hostname -- which names an account. TestTheShippedManifestNamesNobodysRegistry
	@# guards the file, so without this every run of the cluster suite, which
	@# deploys, turns that guard red until somebody notices and reverts by hand.
	@mv config/manager/kustomization.yaml.deploying config/manager/kustomization.yaml

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= $(LOCALBIN)/kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint
GINKGO ?= $(LOCALBIN)/ginkgo

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.21.0

#ENVTEST_VERSION is the controller-runtime version to use for setup-envtest, derived from go.mod
KIND_VERSION ?= v0.30.0
# Pinned to the ginkgo the suites are written against: the CLI and the library
# have to agree, and a mismatch fails at the parallel handshake rather than at
# compile time.
GINKGO_VERSION ?= v2.27.4
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v")

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.12.2
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		$(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
