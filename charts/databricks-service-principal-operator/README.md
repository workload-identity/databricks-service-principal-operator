# databricks-service-principal-operator (Helm chart)

Installs the operator: the three CRDs, the manager, its RBAC, both admission
webhooks, and the cert-manager `Certificate` and `Issuer` that serve them. The
same objects `config/default` installs, with one difference that is the reason
this chart exists — every cluster-scoped name is derived from the release, so a
second operator for a second Databricks account installs alongside the first
without anybody editing a file.

`config/` stays the source of truth. `make deploy` and the released
`dist/install.yaml` are built from it, and this chart is a second description of
the same objects. What keeps the two from drifting is
`internal/controller/helm_rendered_test.go`, which renders both and compares the
CRDs, the manager's flags and environment, both webhook configurations and every
RBAC rule in both directions. It runs in `make test`.

## Before you install

**cert-manager**, any version serving `cert-manager.io/v1`. It is a requirement
and not a chart dependency: it is cluster-wide, most clusters already have it,
and one operator's install is the wrong place to decide how a whole cluster
issues certificates. The chart refuses rather than installing half of itself —
everything except the `Certificate` and the `Issuer` would apply cleanly, the
webhook's `failurePolicy` is `Ignore`, and the install would look finished while
every pod started with no token.

**A Databricks service principal for the operator, and it has to be an account
admin.** Step 1 of [Installing the operator](../../docs/installing.md) is where
that is created. Its federation policy has to name the subject and audience this
release presents, and `helm install` prints both.

## Install

```sh
helm install dbxsp-operator \
  oci://registry-1.docker.io/weidaolee/databricks-service-principal-operator \
  --version 0.14.0 \
  --namespace dbxsp-operator-system --create-namespace
```

No values are needed. `image.repository` defaults to the image this project
publishes and `image.tag` to the chart's `appVersion`, so the chart and the
operator it installs cannot be given different versions by editing one of them.
`--set image.repository=<your registry>/<image>` runs a build of your own
instead.

The chart version is the operator version without its `v`: chart `0.14.0`
installs `v0.14.0`, and there is no table of which goes with which.

The chart does not create the namespace. Helm's `--create-namespace` does, and
leaves it outside the release, which is what you want: a namespace owned by the
release is a namespace `helm uninstall` deletes, taking everything anybody else
put in it.

Then write the `DatabricksAccount` that says which account to act in
([step 3](../../docs/installing.md#3-point-it-at-the-account)). There is
deliberately no value in this chart that creates one: it carries an account id
and a client id, and those belong to whoever holds the account.

### Rendering it without a cluster

`helm template` and `helm lint` talk to no cluster, so they cannot see
cert-manager either. Tell them it is there:

```sh
helm template dbxsp-operator charts/databricks-service-principal-operator \
  --api-versions cert-manager.io/v1
```

## The CRDs are in `crds/`, and that is a decision

Helm installs a chart's `crds/` once, on first install, and then never touches
them — not on upgrade, not on uninstall. That is exactly the behaviour to want
here:

- Two operators share one set of CRDs. The second install finding them already
  there and leaving them alone is correct.
- **`helm uninstall` must not delete them.** Deleting a CRD deletes every record
  stored under it, and this operator's records are what bind a Databricks
  service principal to a ServiceAccount. Losing them destroys service
  principals, in an account the cluster cannot get them back from.

The cost is that **a CRD change does not reach an existing install through
`helm upgrade`**. After a release that changes a schema, apply it deliberately:

```sh
kubectl apply -f charts/databricks-service-principal-operator/crds/
```

The copies in `crds/` are byte-identical to `config/crd/bases/`, which
`make manifests` generates. `TestTheChartShipsTheGeneratedCRDs` fails when they
are not, because an install that quietly serves last release's schema prunes the
field nobody declared and reports nothing.

## Two operators in one cluster

One operator per Databricks account
([Several operators in one cluster](../../docs/several-operators.md)). Every
cluster-scoped object this chart installs — both `ClusterRole` sets, both
`ClusterRoleBinding`s, both webhook configurations — is named
`<release>-<namespace>-<what it is>`. The namespace is in there as well as the
release because Helm releases are namespaced, so two people may each hold a
release called `operator`.

Namespaced objects need nothing of the sort; their namespace already separates
them.

The release name is what every name is built from, and a release name longer
than 47 characters is refused rather than truncated: a `Service` name is one DNS
label and holds 63, of which this chart appends 16. Truncating to fit is how two
releases end up sharing a webhook Service, which would send one account's API
server calls to the other account's operator.

## Values

| Value                        | Default              | What it is                                                                          |
|------------------------------|----------------------|-------------------------------------------------------------------------------------|
| `image.repository`           | `controller`         | The placeholder. Names no registry; pass your own.                                  |
| `image.tag`                  | `latest`             |                                                                                     |
| `image.pullPolicy`           | `IfNotPresent`       |                                                                                     |
| `replicaCount`               | `1`                  | A second replica is a warm standby; the leader lease means only one acts.           |
| `fullnameOverride`           | `""`                 | Build names from this instead of the release name.                                  |
| `namespace`                  | `""`                 | Where the objects go. Empty means the release's namespace.                          |
| `serviceAccount.name`        | `""`                 | Half of the subject the federation policy names. Empty derives it from the release. |
| `serviceAccount.annotations` | `{}`                 |                                                                                     |
| `databricks.account`         | `databricks-account` | Which `DatabricksAccount` in this namespace to act on. `--databricks-account`.      |
| `databricks.tokenAudience`   | `databricks`         | The `aud` of the operator's own token, and of its federation policy in Databricks.  |
| `leaderElection.enabled`     | `true`               | Off also drops the `Role` and `RoleBinding` that go with it.                        |
| `metrics.bindAddress`        | `:8443`              | The metrics `Service` takes its port from this.                                     |
| `resources`                  | 500m/128Mi, 10m/64Mi |                                                                                     |
| `podAnnotations`             | `{}`                 |                                                                                     |
| `nodeSelector`               | `{}`                 |                                                                                     |
| `tolerations`                | `[]`                 |                                                                                     |
| `affinity`                   | `{}`                 |                                                                                     |

Every default is what `config/default` renders today. That is what makes the
comparison test possible: rendered with no values, the chart and the kustomize
output say the same thing.
