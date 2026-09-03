# Issues

To be opened as GitHub issues when the repository exists. One heading per issue.

## The end-to-end suite depends on kubectl

`test/e2e` talks to the cluster by running `kubectl` as a subprocess. That is not
part of what the suite tests, and `kubectl` is the only tool this repository does
not pin -- `KUBECTL ?= kubectl` takes whatever is on `PATH`.

`k8s.io/client-go` is already a direct dependency and has everything needed:
`plugin/pkg/client/auth/exec` for the kubeconfig's exec credentials, and
`tools/remotecommand` for the one `exec` into a pod. Replacing kubectl also
distinguishes "not there yet" from "the call failed", which `kubectlOut` cannot:
both come back as an empty string.

## Removing a namespace from `spec.namespaces` does not stop what it has issued

`Serves` is read in exactly one place: `Reconcile` on
`DatabricksServicePrincipalReconciler`
(`internal/controller/databricksserviceprincipal_controller.go:118`, via
`serves()` at `:810`). `IssuedDatabricksServicePrincipalReconciler`, which owns
the service principal and the federation policy, never reads it. Removing a
namespace therefore freezes the projection at its last value: still
`Ready=True`, still carrying `clientId`, so the webhook goes on equipping new
pods. Meanwhile the record keeps converging, the service principal stays alive,
and `EnsureFederationPolicy` re-asserts the trust on every pass.
`api/v1alpha1/databricksaccount_types.go` documents the field as where a
platform team says what their account admin credential may be spent on, and the
credential goes on being spent in a namespace that team has declared out of
scope -- two account-level Databricks calls per identity per minute,
indefinitely.

The framing being worked under is this. `spec.namespaces` is consent, the
annotation on a ServiceAccount is a request, and a request without consent has
no effect: what a tenant writes is theirs, whether the account admin honours it
is the admin's.
Consent is a gate and not a trigger -- adding a namespace creates nothing by
itself, since the tenant must still carry the annotation and the namespace must
still hold the `mint` label -- so withdrawing consent should close the gate, and
the token must stop being exchangeable. That much does not say the service
principal has to be destroyed.

What the framing leaves open, and the reason this is an issue rather than a
task, is whether the grants accumulated on an identity should die with the
consent. Keeping the service principal means re-adding one line restores
everything under the same `applicationId`. Destroying it means re-adding
produces a new `applicationId` -- Databricks assigns it and will not let you
choose it (`internal/databricks/servicePrincipals.go:183-196`) -- and every
Unity Catalog grant, cluster policy and workspace permission naming the old one
is stranded.

A trap has to be recorded alongside the decision. `serves()` returns `false`
when the `DatabricksAccount` cannot be read at all (`apierrors.IsNotFound` ->
`return false, nil`), so "declared out of scope" and "could not read the
declaration" are the same answer today. Wiring `!Serves` straight to destruction
would put back, one level up, the shape this project has just spent a change
removing from the annotation -- absence read as intent -- except that here one
unreadable object would destroy every identity in every namespace.

One cost is worth knowing before anyone plans the work: the `Clients` interface
has no federation-policy delete. It has only `EnsureFederationPolicy`, which
lists and creates (`internal/databricks/client.go:65`,
`internal/databricks/servicePrincipals.go:295-332`). Closing the gate needs a
new method on that interface and a live test for it.
