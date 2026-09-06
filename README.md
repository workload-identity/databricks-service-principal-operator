# databricks-service-principal-operator

Your workload reaches Databricks with no credential anywhere — no token in a
Secret, nothing to rotate, nothing to leak.

You annotate a ServiceAccount, naming the operator to ask:

```sh
OPERATOR=dbxsp-operator-system/databricks-account

kubectl -n team-a annotate serviceaccount etl \
  databricks.workload-identity.io/service-principal=$OPERATOR
```

A Databricks service principal is created for `team-a/etl` (`<namespace>/<name>`),
Databricks is told to trust that ServiceAccount's token as that principal, and
every pod running under it comes up holding the identity:

```sh
$ kubectl -n team-a get databricksserviceaccount etl
NAME   CLIENT ID                              READY   AGE
etl    11111111-1111-1111-1111-111111111111   True    4s
```

Your workload uses it by naming what it was given, in one line:

```sh
export DATABRICKS_CONFIG_FILE=$WORKLOAD_IDENTITY_DATABRICKS_CONFIG_FILE
export DATABRICKS_CONFIG_PROFILE=$WORKLOAD_IDENTITY_DATABRICKS_CONFIG_PROFILE
```

One line rather than none, deliberately: nothing this operator sets is a name any
Databricks SDK reads, so a workload that brought its own configuration keeps it.
See [Why you have to write that line](docs/what-the-pod-gets.md#why-you-have-to-write-that-line).

The annotation is a request, not a permission. Three people have to have said
yes and you are the third, the other two being a cluster admin and the team
holding the Databricks account: [Three people have to say yes](docs/who-says-yes.md).
This operator grants nothing either — what that principal may read or run is
decided in Databricks.

## Why this

This is the mechanism Databricks recommends — their own table ranks **OAuth
token federation (Recommended)** above OAuth for service principals and for
users, because it "eliminates the need for managing and rotating Databricks
secrets" ([Authenticate access to Databricks using OAuth token
federation](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation),
read 2026-09-05). Nothing here varies from it: the RFC 8693 exchange their SDKs
make, a per-service-principal federation policy through their API, the stock SDK
on its own `file-oidc` path. What the operator supplies is the part that is
manual work, once per identity.

- **No credential anywhere.** Not better Secret hygiene — nothing to rotate and
  nothing to leak, because there is no secret: the pod is given a token minted for
  Databricks, and kubelet replaces it before it expires
  ([measured](docs/measured.md#the-token-exchange)).
- **One annotation, in the shape people already know.** GKE, EKS and Azure all
  hand a workload a cloud identity this way; Databricks has the OIDC federation
  mechanism and nothing that drives it from Kubernetes, so today each identity is
  made by hand, which is why workloads end up holding a personal access token in
  a Secret instead ([Asking for an identity](docs/asking-for-an-identity.md)).
- **The pod gets everything and the operator takes over nothing.** A workload's
  pod spec says nothing about Databricks — no volume, no audience, no client id —
  and yet no name this operator sets is a name any SDK reads, so a workload that
  brought its own configuration keeps it, and writes one line rather than none
  ([What the pod gets](docs/what-the-pod-gets.md)).
- **One ServiceAccount can hold several identities, chosen by name.** One key per
  identity, so a workload that reads with one service principal and writes with
  another asks for both by name and picks between them in code
  ([Several identities](docs/several-identities.md)).
- **It issues identities and grants them nothing, deliberately.** The operator has
  to be an account admin, so the account cannot prove from permissions alone that
  it does not grant — the only thing left to read is the code, and there is no
  call in it that grants anything
  ([Issuing an identity, not a permission](docs/identity-not-permission.md)).
- **The identity's life is bound to the ServiceAccount's.** Nobody has to remember
  to clean up in Databricks: the annotation goes, or the ServiceAccount goes, or
  the namespace goes, and the identity ends, its federation policies with it. The
  alternative is a hand-made service principal and a token outliving the workload,
  still holding whatever was granted to them, with nobody noticing — orphans are
  the default everywhere else and here they are impossible
  ([Ending an identity](docs/asking-for-an-identity.md#ending-an-identity)).

Every claim this repository makes about Databricks behaviour was measured against
a live account, and the numbers are in [what was measured](docs/measured.md).

## Requirements

- **cert-manager**, any version serving `cert-manager.io/v1`. The install carries
  two admission webhooks — one that equips pods, one that refuses an annotation
  asking for an identity in a namespace this account does not serve — and the one
  certificate cert-manager issues serves both.
- **A cluster whose OIDC issuer Databricks can reach.** EKS and GKE publish a
  managed issuer and are fine.
- **A Databricks service principal for the operator, and it has to be an account
  admin** ([why there is no narrower role](docs/installing.md#why-it-has-to-be-an-account-admin)).

Check the issuer first ([why](docs/installing.md#check-the-issuer-first)):

```sh
kubectl get --raw /.well-known/openid-configuration | jq -r .issuer
```

## Install

1. In Databricks, create the operator's service principal, write a federation
   policy on it naming that issuer, and make it an account admin
   ([step 1](docs/installing.md#1-give-the-operator-an-identity-in-databricks)).

2. Deploy ([step 2](docs/installing.md#2-deploy)):

   ```sh
   kubectl apply -f https://github.com/workload-identity/databricks-service-principal-operator/releases/latest/download/install.yaml
   ```

   One manifest: the three CRDs, the operator, its RBAC, the two webhooks and
   the certificate they are served with.

   Or with Helm, which names everything after the release rather than fixing it
   ([the chart](charts/databricks-service-principal-operator/README.md)):

   ```sh
   helm install dbxsp-operator \
     oci://ghcr.io/workload-identity/charts/databricks-service-principal-operator \
     --namespace dbxsp-operator-system --create-namespace
   ```

3. Point it at the account ([step 3](docs/installing.md#3-point-it-at-the-account)):

   ```yaml
   apiVersion: databricks.workload-identity.io/v1alpha1
   kind: DatabricksAccount
   metadata:
     name: databricks-account
     namespace: dbxsp-operator-system
   spec:
     host: https://accounts.cloud.databricks.com
     accountId: <account id>
     clientId: <applicationId from step 1>
     namespaces:
     - team-a
   ```

4. Check it ([step 4](docs/installing.md#check-it)). `Ready` means the operator
   can act; if it is not, `status.subject` and `status.audience` are what the
   policy from step 1 has to name exactly.

   ```sh
   kubectl -n dbxsp-operator-system get databricksaccount databricks-account -o yaml
   ```

## Give a workload an identity

A cluster admin labels the namespace once
([why two labels](docs/who-says-yes.md#the-cluster-admin-this-namespace-may-be-served)):

```sh
kubectl label namespace team-a \
  databricks.workload-identity.io/inject=enabled \
  databricks.workload-identity.io/mint=enabled
```

Whoever holds the namespace writes the annotation from the top of this page on
the ServiceAccount, its value being the operator installed above, written
`<its namespace>/<its DatabricksAccount>` — here
`dbxsp-operator-system/databricks-account`
([Asking for an identity](docs/asking-for-an-identity.md)). The workload writes
the two `export` lines. That is the whole of it
([What the pod gets](docs/what-the-pod-gets.md)).

## Before you use it

- **Taking a namespace off `spec.namespaces` destroys every identity in it**, and
  naming it again is not a restore — it issues new service principals, with new
  client ids and no grants
  ([what taking one off does](docs/what-the-namespace-list-means.md#taking-a-namespace-off-the-list-destroys-the-identities-in-it)).
- **The operator grants nothing, and will not** — every identity arrives able to
  do nothing until somebody holding a different credential grants it something
  ([Issuing an identity, not a permission](docs/identity-not-permission.md)).

## Docs

| For                  | Read                                                                                 | About                                                                        |
|----------------------|--------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| The account holder   | [Installing the operator](docs/installing.md)                                        | Putting this into a cluster for the first time                               |
|                      | [Three people have to say yes](docs/who-says-yes.md)                                 | Who has to agree before any identity exists, and the cluster admin to ask    |
|                      | [What the namespace list means](docs/what-the-namespace-list-means.md)               | What `spec.namespaces` promises, and why taking one off destroys             |
|                      | [Several operators in one cluster](docs/several-operators.md)                        | A second Databricks account in a cluster that already has one                |
|                      | [The Helm chart](charts/databricks-service-principal-operator/README.md)             | Installing a second operator without editing a file, and what it does not do |
|                      | [Finding out what a service principal is](docs/finding-an-identity-in-databricks.md) | You are looking at one in Databricks and want to know what made it           |
| The namespace holder | [Asking for an identity, and ending one](docs/asking-for-an-identity.md)             | The annotation, and the key you withdraw to end it                           |
|                      | [Several identities for one ServiceAccount](docs/several-identities.md)              | One workload acting as more than one Databricks service principal            |
|                      | [What the pod gets](docs/what-the-pod-gets.md)                                       | What an equipped pod holds, and the one line your code names                 |
| Everybody            | [The three objects](docs/the-three-objects.md)                                       | What each custom resource is, before you touch one                           |
|                      | [When something is wrong](docs/when-something-is-wrong.md)                           | You have a symptom and not yet an object to look at                          |
| Evaluating this      | [Issuing an identity, not a permission](docs/identity-not-permission.md)             | What the boundary is, why it is load-bearing, and what it costs              |
|                      | [What was measured](docs/measured.md)                                                | Every claim this rests on, what was asked, and what came back                |
|                      | [Security](SECURITY.md)                                                              | What is a vulnerability here and what is the design, and how to report one   |
