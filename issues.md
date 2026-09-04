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
`DatabricksServiceAccountReconciler`
(`internal/controller/databricksserviceaccount_controller.go:118`, via
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

## No reconcile in this operator has a deadline

`controller-runtime` v0.24.1, the version in `go.mod`, offers
`ReconciliationTimeout` on a controller's `Options` and on the manager's
`config.Controller` as a default every controller inherits.
`pkg/controller/controller.go:114-116` documents it as "used as the timeout
passed to the context of each Reconcile call. By default, there is no timeout."
This repository sets it in neither place -- nothing constructs a
`config.Controller`, and no builder chain calls `WithOptions` -- so there is no
timeout. A `Reconcile` that does not return holds its worker, and nothing here
bounds how long one may take.

What the option does when set is narrow, and worth knowing before anyone
reaches for it. From `pkg/internal/controller/controller.go:214-219`, it wraps
the call in `context.WithTimeoutCause(ctx, c.ReconciliationTimeout, ...)` with
`errReconciliationTimeout` as the cause. It cancels a context, and Go cannot
interrupt a goroutine, so it reaches only code that reads the context. That is
what an HTTP call or an API server read does, and it is the case that actually
happens here -- but it is not every case.

What decides the value is the whole of the decision, so it belongs in the issue
rather than in the work. The Databricks SDK's own `defaultRetryTimeout` is five
minutes and its `defaultHTTPTimeout` sixty seconds
(`httpclient/api_client.go:107-119` in `databricks-sdk-go` v0.176.0), and
`Config.SDKConfig` (`internal/databricks/config.go:102`) sets neither
`HTTPTimeoutSeconds` nor `RetryTimeout`, so both defaults apply untouched. What
bounds a reconcile today is therefore a decision the SDK made, not one made
here. A reconcile timeout shorter than the SDK's retry budget cancels work the
SDK was going to finish; one longer than it is never reached in the case it was
written for.

## One wedged object stops every other object of its kind

`MaxConcurrentReconciles` is set on none of the three controllers, and
controller-runtime defaults it to one (`pkg/controller/controller.go:244-246`).
Each controller therefore has a single worker, and one `Reconcile` that is slow
or stuck holds it while every other object of that kind waits in the queue
behind it. The three have separate workers and separate queues, so this does
not spread between them: `IssuedDatabricksServicePrincipalReconciler` stalling
leaves `DatabricksServiceAccountReconciler` and `DatabricksAccountReconciler`
running.

The concrete shape is `IssuedDatabricksServicePrincipalReconciler`. It owns
every `IssuedDatabricksServicePrincipal` in the cluster and makes the
per-identity Databricks calls -- `FindServicePrincipal`,
`CreateServicePrincipal`, `ServicePrincipalExists`, `EnsureFederationPolicy`.
One identity whose call is retrying against the SDK's five-minute budget holds
every other identity in the cluster behind it for as long as that takes.
`DatabricksAccountReconciler` calls Databricks too -- `verifyAccount` lists the
account's workspaces on every pass -- but it reconciles the one selected
`DatabricksAccount`, so its single worker has nothing queued behind it. It
stalls in the same way and costs something different: while it is stalled the
`Holder` is not updated, so a spec that has been repointed is neither installed
nor withdrawn.

What the decision turns on is not whether a second worker would help but what
it would be doing at the same time, because these reconcilers write to
Databricks. Read `internal/databricks/holder.go`'s `Snapshot` and the reasoning
it carries first: a pass takes one view of the account at the top precisely
because asking "which account am I in" and then calling into another is
unrecoverable and silent -- a lookup in the wrong account returns the same 404
as a deleted identity, and a create in one account stamped with the other's id
is an identity nothing can find. `Snapshot` settles that for one pass against a
changing declaration. It says nothing about two passes running at once against
the same identity, and that is the question a second worker asks.

## Nothing restarts an operator that has stopped reconciling

`cmd/main.go:313` registers `mgr.AddHealthzCheck("healthz", healthz.Ping)`, and
`healthz.Ping` is `func(_ *http.Request) error { return nil }`. The Deployment
does wire a liveness probe at `/healthz`
(`config/manager/manager.yaml:104-109`), so the shape is there -- it just
cannot fail, and the kubelet never restarts this pod. Readiness is
`CacheSynced.Check` (`internal/controller/readiness.go`), registered at
`cmd/main.go:326`, which reports whether the cache finished filling and stays
true afterwards. Neither probe has anything to do with whether reconciles are
still happening.

Two things make a stall invisible rather than merely unrecovered. Leader
election renews its lease from its own goroutine
(`client-go/tools/leaderelection/leaderelection.go:279`), so a wedged process
keeps the lease and no other replica takes over -- `--leader-elect` is passed in
`config/manager/manager.yaml:64`, and `replicas` is 1, so today there is no
other replica in any case. And there is no logging anywhere in `internal/`:
grep for `FromContext`, `logf.`, `ctrl.Log` or `logr.` across the non-test files
and nothing comes back. So nothing is printed either. The only place a stall
shows is that conditions on the objects stop being updated, which somebody has
to go and look at.

A timeout does not answer this one, which is why it is a separate issue and not
the same one. Cancelling a context reaches only code that reads it; a goroutine
that does not is unreachable, and the sole remaining lever is ending the process
and letting the kubelet start it again. That is what a liveness check that can
fail is for, and this operator does not have one. Note what `readiness.go`
already settled and what it did not: it argues deliberately that liveness must
not be gated on cache sync, because a cache that is slow to fill is not a
process to kill and restarting would only start the filling again. That
reasoning is about startup. It leaves untouched the question of a process that
finished starting and then stopped working.
