# databricks-service-principal-operator

Your workload reaches Databricks with no credential anywhere — no token in a
Secret, nothing to rotate, nothing to leak.

You annotate a ServiceAccount, naming the operator to ask:

```sh
kubectl -n team-a annotate serviceaccount etl \
  databricks.workload-identity.io/service-principal=ops-a/databricks-account
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
See [What your workload writes](#what-your-workload-writes).

The annotation is a request, not a permission: two other people have to have
said yes first, and both of them are below. This operator grants nothing either
— what that principal may read or run is decided in Databricks.

> **Not released.** All of this has run — one pod on EKS holding two identities
> the operator created, exchanging each of its two tokens and acting as a
> different Databricks service principal with each — once, on one cluster,
> against one account. Nobody has used it for anything.

---

## Requirements

- **cert-manager**, any version serving `cert-manager.io/v1`. The webhook serves
  TLS and cert-manager issues its certificate. `make deploy` refuses rather than
  installing half of itself when the CRDs are absent.
- **A cluster whose OIDC issuer Databricks can reach.** EKS and GKE publish a
  managed issuer and are fine. A private one does not work until it is published
  somewhere Databricks can read.
- **A Databricks service principal for the operator, and it has to be an account
  admin.** Writing a service principal's federation policy is
  [documented as account admin only][fed-policy], and this operator writes one
  for every identity it creates. Step 1 below.

[fed-policy]: https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation-policy

Check the issuer first; nothing else helps if it fails.

```sh
kubectl get --raw /.well-known/openid-configuration | jq -r .issuer
```

Databricks fetches that issuer's OpenID configuration when a federation policy
is written -- not later when a token is exchanged -- so an unreachable one fails
on the first reconcile with "Unable to load valid OpenID configuration for
issuer".

## 1. Give the operator an identity, in Databricks

The operator authenticates by exchanging its own token, so its identity must
exist before it runs — it cannot create its own. Once, as an account admin,
ideally in Terraform:

1. Create a service principal for the operator. Keep its `applicationId`.
2. Write a federation policy on it:
   - `issuer` — from the command in Requirements
   - `subject` — `system:serviceaccount:<operator namespace>:<operator ServiceAccount>`
   - `audiences` — exactly one, matching `DATABRICKS_TOKEN_AUDIENCE` in
     `config/manager/manager.yaml` (`databricks` by default)
3. Make it an **account admin**.

   This is a large grant and it is not avoidable today. The two halves of what
   the operator does have different requirements, and the wider one decides:

| What it does                | Who may                                 | Source                                      |
|-----------------------------|-----------------------------------------|---------------------------------------------|
| Create a service principal  | Account admins **and workspace admins** | [Manage service principals][manage-sp]      |
| Write its federation policy | **Account admins** only                 | [Configure a federation policy][fed-policy] |

   There is no narrower role. `roles/servicePrincipal.manager` is not it -- it
   [manages the roles on an existing service principal][sp-acl], not the
   creation of new ones and not their federation policies. Granting it changes
   nothing here; that was checked against a live account.

   (Those are the AWS pages. The Azure and GCP doc sets say the same thing.)

   What follows from it is worth stating plainly. An account admin can grant
   Databricks permissions. This operator does not, and holds no code that could
   -- but the account can no longer prove that from the permissions alone, only
   from what the operator is. That is why it does not grant, and will not:
   [What it will not do](#what-it-will-not-do).

[manage-sp]: https://docs.databricks.com/aws/en/admin/users-groups/manage-service-principals
[sp-acl]: https://docs.databricks.com/aws/en/security/auth/access-control/service-principal-acl

## 2. Deploy

```sh
make deploy IMG=<registry>/databricks-service-principal-operator:<tag>
```

## 3. Point it at the account

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

`namespaces` is where this operator will act, by name, and it is the half of the
answer that belongs to whoever holds this account: it says which namespaces their
account admin credential may be spent on. Naming none serves none, and that is
the default — a fresh install is inert until its owner says where it may act, so
a `DatabricksAccount` written without this field is correct and does nothing.

It is the second of the two yeses below, and
[Saying where the account may be spent](#saying-where-the-account-may-be-spent)
is the rest of it.

`host` and `accountId` cannot be changed afterwards, and the API server refuses
the edit. A service principal id means nothing outside the account it was made
in, so an operator acts in one Databricks account for its whole life: correcting
either value means deleting this object and writing the one you meant, and a
second account means a second operator. Deleting is itself refused while any
record in this namespace still names that account — the object says how many, and
how to get out of it when the account can no longer be reached — and an operator
whose records name an account other than the one `--databricks-account` selects
refuses to start, naming the record and both accounts in its log. An install that
never reached Databricks recorded no account anywhere, so a typo here is undone
by deleting the object and writing it again.

Check it:

```sh
kubectl -n dbxsp-operator-system \
  get databricksaccount databricks-account -o yaml
```

`Ready` means the operator can act. If it is not Ready, `status.subject` and
`status.audience` are what the operator actually presents — the federation policy
from step 1 must name those exact values. They are reported even on failure,
which is when you need them.

Installation is done. The rest is per-team and per-workload.

---

## Two people have to say yes first

An annotation does nothing until both of these are already true. The two sections
after this one are those two, in this order:

| Who                                     | Says                                | Where                                                   |
|-----------------------------------------|-------------------------------------|---------------------------------------------------------|
| A cluster admin                         | this namespace may be served at all | `databricks.workload-identity.io/mint` on the Namespace |
| The team holding the Databricks account | I will spend my credential here     | `spec.namespaces` on their DatabricksAccount            |

Neither can say the other's, and neither can say the third: the annotation
itself, written by whoever holds the namespace. Withhold any of the three and
nothing happens, and nothing says so — until an identity exists there is no
object to carry a condition, and an object in every namespace announcing that it
has nothing would be this operator answering a question nobody asked. All three
are read rather than reported: the label on the Namespace, the list on the
DatabricksAccount, and the annotation on the ServiceAccount.

## Saying a namespace may be served

A cluster admin does this, once per namespace, with two labels:

```sh
kubectl label namespace team-a \
  databricks.workload-identity.io/inject=enabled \
  databricks.workload-identity.io/mint=enabled
```

| Label    | What it decides                                 | When to remove it                                                                      |
|----------|-------------------------------------------------|----------------------------------------------------------------------------------------|
| `mint`   | Whether new identities may be minted here       | As soon as you have minted what you meant to. Nothing that already exists is affected. |
| `inject` | Whether pods created here are given their token | Only when this namespace has stopped using Databricks altogether.                      |

They are separate because they end at different times. Minting is an event —
open it, mint, close it. Equipping pods is continuous: an identity minted while
minting was open needs every new pod under it equipped for as long as it is used.
One label for both would mean closing the minting switch quietly broke every
workload there on its next rollout.

`mint` is not a security boundary. The identity a ServiceAccount can ask for
is `system:serviceaccount:<its own namespace>:<itself>` and it is created holding
nothing, so it does not stop anybody reaching something they could not reach
anyway. It decides which teams are in.

`inject` left on a namespace that no longer uses Databricks costs one admission
call per pod and nothing else — a pod whose ServiceAccount has no identity is
admitted unchanged. Forgetting it is cheap.

**Neither label revokes.** To end a workload's access, remove the annotation key
— by whoever asked for it, which is the point of them being two separate things.
See [Destroying an identity](#destroying-an-identity).

## Saying where the account may be spent

The cluster saying a namespace may be served is not the same as a platform team
agreeing to spend their account admin credential on it. Those are two people, and
`spec.namespaces` from step 3 is the second one's answer.

It is theirs and nobody else's, by where it lives: the `DatabricksAccount` is in
the operator's own namespace, so writing to it needs the access that could
already replace the operator's image. A namespace cannot add itself to the list,
and a team that wants in asks the team holding the account rather than editing
anything of their own.

### Taking a namespace off the list destroys the identities in it

**This is destructive and it cannot be undone from here.** Every service
principal this account issued in that namespace is deleted in Databricks, every
grant anybody made on one goes with it, and the records go. Emptying the list
does it for every namespace at once.

**Naming the namespace again is not a restore.** It issues new service principals,
with new client ids and no grants. Everything that was granted has to be granted
again, by whoever granted it.

So the list means one thing in each of the three states a namespace can be in:

| The namespace is   | This account                             |
|--------------------|------------------------------------------|
| on the list        | issues identities there and manages them |
| taken off the list | destroys every identity it issued there  |
| never on the list  | has nothing there                        |

Which buys one thing a person can check: **every service principal carrying this
operator's marker is in a namespace on the list.**

**The promise is eventual.** The destroying is done against Databricks, so while
Databricks cannot be reached the namespace is off the list and the identities are
still there. `IdentitiesDestroyed` on the `DatabricksAccount` says how many are
left and in which namespaces, and it is what says whether it has finished:

```sh
kubectl -n dbxsp-operator-system get databricksaccount databricks-account \
  -o jsonpath='{.status.conditions[?(@.type=="IdentitiesDestroyed")].message}'
```

While it is running, the operator suspends minting in the namespace — it writes
`databricks.workload-identity.io/destroying-identities` on the Namespace, naming
itself — so that the set it is destroying stops growing. It takes that key off
again when it has finished, and any other operator serving that namespace goes on
minting with nothing for a person to do.

### Two Databricks accounts in one cluster

One operator serves one Databricks account, because it holds that account's
credential and that credential is an account admin. Two accounts need two
operators, and each account admin then trusts only their own.

An operator is named after the account it serves, not after a team — several
teams can share one account, and they then share one operator.

Each one runs in its own namespace, and says which namespaces it will serve on
its own DatabricksAccount:

```yaml
# in the finance operator's namespace
spec:
  namespaces:
  - team-a
  - team-b
```

`team-a` and `team-b` use the finance account, so that operator serves both;
`risk` is named on the other operator's account instead.

A namespace that was never on the list is left entirely alone: nothing there
belongs to this account. A namespace **taken off** the list is not the same
thing, and the section below says what it is.

Both operators are asked about the same pods, and they agree without
coordinating: each reads the same `DatabricksServiceAccount`, which carries
every identity the ServiceAccount was issued, so whichever is asked first equips
the pod with all of them and the second finds its work already done.

That is also why they need not agree on an audience. Each identity's token is
minted for whatever its own operator's token carries, and a pod holds one token
per identity — so nobody has to go round asking the other platform teams what
theirs is.

---

## Giving one workload an identity

Whoever holds the namespace does all of this, and none of it needs a cluster admin.

### First, find the operator you are asking

An operator is named by its DatabricksAccount: the namespace it runs in and the
name of that object, joined by `/`. Ask the cluster:

```sh
kubectl get databricksaccounts -A
```

```
NAMESPACE               NAME                 HOST                                    READY
dbxsp-operator-system   databricks-account   https://accounts.cloud.databricks.com   True
```

That operator's reference is the two columns joined:

```
dbxsp-operator-system/databricks-account
```

It is an address rather than a nickname, because the API server will not hold two
objects of one kind at one address — so two operators cannot be confused for each
other, and nobody has to keep a list of who chose which name.

### Then annotate

```sh
OPERATOR=dbxsp-operator-system/databricks-account

kubectl -n team-a annotate serviceaccount etl \
  databricks.workload-identity.io/service-principal=$OPERATOR

kubectl -n team-a get databricksserviceaccount etl
```

```
NAME   CLIENT ID                              READY   AGE
etl    11111111-1111-1111-1111-111111111111   True    4s
```

`CLIENT ID` is the applicationId to grant Databricks permissions to. That is the
whole of the common case.

**Grant to a group rather than to that applicationId** — worth weighing before
you settle on how to grant. If the service principal is ever deleted in
Databricks, nothing rebuilds it: the identity reports `RemovedInDatabricks` and
waits rather than making another, and what you ask for in its place carries a new
applicationId. Anything that named the old one directly has to be redone;
anything that went through a group is adding the new principal to it.

### More than one identity

A workload that reads with one service principal and writes with another asks for
each under its own key, naming it there:

```sh
kubectl -n team-a annotate serviceaccount etl \
  databricks.workload-identity.io/service-principal.reader=$OPERATOR \
  databricks.workload-identity.io/service-principal.writer=$OPERATOR
```

The name after the `.` is the profile name your code asks for, and the value is
the same operator reference as above. That is
[docs/several-identities.md](docs/several-identities.md), and everything above
holds unchanged for the one-identity case.

### Recreate pods that were already running

A pod is equipped when it is created, and nothing rewrites a running one. The
object's `Equipped` condition names the pods that are missing what they should
have:

```sh
kubectl -n team-a get databricksserviceaccount etl \
  -o jsonpath='{.status.conditions[?(@.type=="Equipped")].message}'
```

---

## What your workload writes

One line, and this operator sets nothing else.

```sh
export DATABRICKS_CONFIG_FILE=$WORKLOAD_IDENTITY_DATABRICKS_CONFIG_FILE
export DATABRICKS_CONFIG_PROFILE=$WORKLOAD_IDENTITY_DATABRICKS_CONFIG_PROFILE
```

or in code, naming them yourself:

```python
import os
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(
    config_file=os.environ["WORKLOAD_IDENTITY_DATABRICKS_CONFIG_FILE"],
    profile=os.environ["WORKLOAD_IDENTITY_DATABRICKS_CONFIG_PROFILE"],
)
```

`WORKLOAD_IDENTITY_DATABRICKS_CONFIG_PROFILE` names the identity your
ServiceAccount asked for with the bare key — the one everything above is about. A
workload holding several picks between them by name:
[docs/several-identities.md](docs/several-identities.md).

### Why you have to write that line

**Nothing this operator sets is a name any Databricks SDK reads.** Setting
`DATABRICKS_CONFIG_FILE` would not add a value to your workload — it would take
over the SDK's whole resolution. A workload carrying its own `.databrickscfg`,
baked into its image or mounted, would find that file still there, untouched, and
never read again. An environment variable beats a profile silently, so nothing
inside the workload could find out, and this operator cannot see what an image
contains.

So it publishes instead of configuring, and the act of using what was published
is yours. That is the price of never displacing anything you brought.

### The workspace is yours to name

The annotation names an operator and nothing else — no workspace. Your workload
names its own, in its own Deployment, with the SDK's own variable:

```yaml
env:
- name: DATABRICKS_HOST
  value: https://dbc-example.cloud.databricks.com
```

That composes with what this operator gives you rather than replacing it —
measured against the SDK: the profile supplies `auth_type`, `client_id` and the
token path, and `DATABRICKS_HOST` supplies the host.

It is not here because this operator issues identities, and a host is a
destination. It could not check one if it carried it: it knows the Databricks
account it acts in, and a workload wants a workspace. Carrying it would be
repeating a string nobody verifies, in an object that is not where the rest of
your configuration lives.

### What is actually in the pod

```
/var/run/secrets/databricks/
├── config
└── ops-a/databricks-account/token
```

`config` is the SDK's own format. The one identity asked for with the bare key
has no name of its own, so its profile is named by what that key held — the
operator:

```ini
[ops-a/databricks-account]
auth_type = file-oidc
client_id = 11111111-1111-1111-1111-111111111111
databricks_id_token_filepath = /var/run/secrets/databricks/ops-a/databricks-account/token
oidc_token_filepath = /var/run/secrets/databricks/ops-a/databricks-account/token
audience = databricks
token_audience = databricks
```

**Two values are there twice, and both spellings are yours to ignore.** The SDKs
do not agree on what to call them in a profile — Go reads
`databricks_id_token_filepath` and `audience`, Python reads `oidc_token_filepath`
and `token_audience` — and this operator cannot see which one your image
contains, so it writes both. Whichever SDK you use reads the pair it knows and
drops the other without a word. Their environment variables do agree, which is
why the split only shows here.

An identity asked for under a named key is named by that name instead, and its
profile is `[reader]`: [docs/several-identities.md](docs/several-identities.md).

**The tokens expire and are replaced, and that is not your problem.** kubelet
rewrites each file well before its token expires, and the SDK re-reads the file
on every exchange. Point it at the path and it stays working; nothing has to be
restarted, and nothing has to notice.

---

## Destroying an identity

Remove the key. In `kubectl annotate`, a key with a `-` on the end means "delete
this one" — the same shape as `key=value` for setting it:

```sh
kubectl -n team-a annotate serviceaccount etl \
  databricks.workload-identity.io/service-principal-
#                                                  ^ not a typo: this deletes it
```

That trailing hyphen is easy to miss here, because the key ends in `-principal`
and already has hyphens of its own. Editing the ServiceAccount by hand and
deleting the line does exactly the same thing.

The key is the request, so a key that is gone is the workload saying it no longer
needs that identity. The service principal is deleted in Databricks — everything
Databricks recorded against that principal goes with it, Databricks doing the
removing.

**A key that is gone is the only thing that destroys**, together with a key whose
value now names a different operator, which is the same sentence read from this
operator's side: a person moved that identity elsewhere. A key that is still
there and cannot be read destroys nothing — the identity it names is left exactly
as it stands. Deleting a key and mistyping a value are edits to different entries
of a map, so the operator never has to decide which of the two you meant.

So a typo costs you the edit and nothing else, and nothing anywhere says so —
what you have is an edit that did not take effect. Whether a key reads is decided
by the characters in it and by nothing about the cluster, so the line you wrote is
the whole of what there is to check: an operator's reference is
`<its namespace>/<its DatabricksAccount>`, and an identity's name goes after a
`.` on the key.

Ending one of several is deleting that one key, and the others are untouched:
[docs/several-identities.md](docs/several-identities.md).

**Deleting the ServiceAccount does the same thing, for the same reason. So does
deleting the namespace.**

**One thing else destroys one, and it is not yours to do:** the team holding the
account taking your namespace off `spec.namespaces`. That destroys every identity
this account issued in it, and re-adding the namespace issues new ones rather
than bringing those back — see
[Taking a namespace off the list](#taking-a-namespace-off-the-list-destroys-the-identities-in-it).

**Nothing else destroys one.** Removing the namespace's `mint` label stops new
identities and leaves existing ones alone. Deleting the
`DatabricksServiceAccount` in your namespace does nothing at all: that object
is a copy, and it is rebuilt on the next pass.

---

## What it will not do

It creates identities. It grants them nothing, and it will not — not a group
membership, not a workspace assignment, not a permission on anything in Unity
Catalog. That is a boundary rather than an unfinished half, and two things rest
on it.

**It is the only thing about this operator the account can check.** The
operator's own service principal has to be an account admin, because writing a
federation policy allows nothing narrower; step 1 spends a while on why. An
account admin can grant Databricks permissions, so the account cannot rule out
that this one does by reading its permissions — the only thing left to read is
the code, and today that reading is short, because there is no call in it that
grants anything. The moment one identity gets a group membership from here, the
answer becomes "it grants only what it should", which is a claim about a running
program rather than a fact about a file. The account admin the operator already
paid for then buys nothing.

**It is what makes `mint` safe to hand out.** Annotating a ServiceAccount is a
namespace-level act: whoever can write ServiceAccounts in `team-a` can mint an
identity there. That is acceptable only because a minted identity holds nothing
— it is why "mint is not a security boundary", below, is true. If minting also
granted, write access to a namespace would become a way to acquire Databricks
permissions, by a route nobody in Databricks reviews.

A third, smaller one: every permission in the account has a single origin, so
"who can read this table" is answered in Databricks alone, rather than
reconstructed from annotations anyone holding a namespace can change.

What this costs is real, and it is the cost being chosen. `status.clientID`
names the identity and stops there; putting it in a group is somebody else's
work, done with somebody else's credential — one that needs none of the account
admin this operator does. Whether that somebody is Terraform, a person, or
another controller is a governance decision, and not this operator's to make.

---

## Where the operator remembers what it issued

Every identity has a second object, an `IssuedDatabricksServicePrincipal`, in the
operator's own namespace:

```sh
kubectl -n dbxsp-operator-system get isdbxsp
```

That object is the record. The one in your namespace is a copy of it:
delete it and it comes back, write to it and the next pass overwrites you.

The record is what deletes the service principal in Databricks, and it lives
outside your namespace so that tearing your namespace down cannot lose it: what
is in Databricks outlives a Kubernetes namespace, so what remembers it has to as
well.

It is named after the ServiceAccount's uid rather than its name, so a
ServiceAccount deleted and recreated under the same name gets a new identity
rather than the old one. `status.identities[].issued` on your object names the
record it came from.

Deleting a record destroys the identity it holds. Nothing else should ever write
one.

## When something is wrong

| Look at                               | Answers                                                             |
|---------------------------------------|---------------------------------------------------------------------|
| `DatabricksAccount` `Ready`           | Whether this Databricks account was reached and read                |
| `DatabricksAccount` `Prepared`        | Whether the operator can issue anything at all                      |
| `DatabricksServiceAccount` `Ready`    | Whether the operator is getting this identity to where it should be |
| `DatabricksServiceAccount` `Equipped` | Whether its pods carry what they need to reach Databricks           |
| `IssuedDatabricksServicePrincipal`    | The same answers, on the object the operator actually acts on       |

`DatabricksAccount` Ready does not mean the operator can create anything —
verification is a read. If it is Ready and an identity says `Denied`, the
operator is short of a permission in Databricks, and no retry supplies it.

**`Prepared` is the one to alert on.** It is false when the operator cannot read
its own projected token, and it therefore does not know the issuer to write into
every federation policy or the audience to give every pod. Nothing new can be
issued while it is false, and Ready stays true throughout — the account is still
reachable, on an access token already exchanged, for about an hour. Existing
workloads are unaffected either way: exchanging a token does not go through this
operator.

`Ready` on an identity carries more than one kind of answer, and the reason says
which:

| Reason                     | Means                                                                                                                                                                                                                                                                             |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Exchangeable`             | This subject's token can be exchanged for this service principal                                                                                                                                                                                                                  |
| `RemovedInDatabricks`      | Somebody deleted the service principal there. It is not replaced; `status.servicePrincipalRemovedAt` says when that was noticed and `status.servicePrincipalId` is still the id to search the audit log for. Remove that identity's key and write it again to be issued a new one |
| `CreateUnconfirmed`        | A create was sent for this identity and no service principal carrying its marker has appeared. Nothing is created and nothing is deleted here: the message names the marker to search the account for                                                                             |
| `Denied`                   | The operator lacks a Databricks permission. No retry supplies it                                                                                                                                                                                                                  |
| `AwaitingServicePrincipal` | The record was just written and has not been acted on yet                                                                                                                                                                                                                         |
| `NotConfigured`            | There is no usable `DatabricksAccount` yet                                                                                                                                                                                                                                        |
| `TokenUnreadable`          | The operator cannot read its own projected token, so it does not know the issuer to write or the audience to give. Its Deployment's `serviceAccountToken` projection is where to look, not this object                                                                            |
| `Rejected`                 | Databricks refused the request itself as invalid. Something has to change in what is being sent; restoring or re-pointing anything does not answer it                                                                                                                             |
| `MalformedRecord`          | An id this operator wrote into `status` can no longer be read as one. It is in the status, not in any spec, so there is nothing in the spec to correct                                                                                                                            |
| `NotFound`                 | Databricks looked and the thing named is not there. Something has to be restored or re-pointed                                                                                                                                                                                    |
| `DatabricksUnavailable`    | Databricks did not answer, whatever it was asked. Nothing has been concluded from it and it is retried                                                                                                                                                                            |
| `DeleteFailed`             | Deletion is being held because the service principal is still there. This is reported on the record, in the operator's namespace                                                                                                                                                  |
| `MintingNotSuspended`      | This identity is in a namespace the account no longer names and is to be destroyed, and nothing has stopped that namespace from minting yet. Nothing has been destroyed and nothing has been asked of Databricks                                                                  |
| `AnotherDestruction`       | Another operator serving that namespace is destroying its own identities there and holds the key that says so. One happens at a time, and this one waits for the other to finish                                                                                                  |
