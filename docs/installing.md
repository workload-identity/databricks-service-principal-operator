# Installing the operator

For whoever holds the Databricks account and is putting this operator into a
cluster for the first time.

There are three steps and they are short. What is not short is the second half of
step 1, which asks for an account admin credential — the largest thing this
install asks for, and the one worth reading before you agree to it rather than
after.

## What has to be there already

- **cert-manager**, any version serving `cert-manager.io/v1`. The webhook serves
  TLS and cert-manager issues its certificate. `make deploy` refuses rather than
  installing half of itself when the CRDs are absent.
- **A cluster whose OIDC issuer Databricks can reach.** EKS and GKE publish a
  managed issuer and are fine. A private one does not work until it is published
  somewhere Databricks can read.
- **A Databricks service principal for the operator, and it has to be an account
  admin.** Writing a service principal's federation policy is
  [documented as account admin only][fed-policy], and this operator writes one
  for every identity it creates. Step 1 is where that is created, and
  [Why it has to be an account admin](#why-it-has-to-be-an-account-admin) is why
  there is no smaller version of it.

[fed-policy]: https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation-policy

### Check the issuer first

Nothing else helps if this fails.

```sh
kubectl get --raw /.well-known/openid-configuration | jq -r .issuer
```

Databricks fetches that issuer's OpenID configuration when a federation policy is
written — not later, when a token is exchanged — so an unreachable issuer fails
on the first reconcile, with "Unable to load valid OpenID configuration for
issuer". That is the good case. An issuer checked only at exchange time would
leave you with an install that looked finished, and a failure that first appeared
in somebody else's namespace, in somebody else's workload, long after the person
who could fix it had stopped looking.

## 1. Give the operator an identity, in Databricks

The operator authenticates by exchanging its own token, so its identity must
exist before it runs — it cannot create its own. Once, as an account admin,
ideally in Terraform:

1. Create a service principal for the operator. Keep its `applicationId`.
2. Write a federation policy on it:
   - `issuer` — from the command above
   - `subject` — `system:serviceaccount:<operator namespace>:<operator ServiceAccount>`
   - `audiences` — exactly one, matching `DATABRICKS_TOKEN_AUDIENCE` in
     `config/manager/manager.yaml` (`databricks` by default)
3. Make it an **account admin**.

The subject and the audience are the two values this policy has to name exactly,
and they are also the two the operator reports back once it is running — even
while it is failing, which is when you need them. So writing the policy from what
you expect and then correcting it against what the operator says it presents is a
reasonable order to work in: [Check it](#check-it).

### Why it has to be an account admin

This is a large grant and it is not avoidable today. The two halves of what the
operator does have different requirements, and the wider one decides:

| What it does                | Who may                                 | Source                                      |
|-----------------------------|-----------------------------------------|---------------------------------------------|
| Create a service principal  | Account admins **and workspace admins** | [Manage service principals][manage-sp]      |
| Write its federation policy | **Account admins** only                 | [Configure a federation policy][fed-policy] |

There is no narrower role. `roles/servicePrincipal.manager` is not it — it
[manages the roles on an existing service principal][sp-acl], not the creation of
new ones and not their federation policies. Granting it changes nothing here;
that was checked against a live account.

(Those are the AWS pages. The Azure and GCP doc sets say the same thing.)

[manage-sp]: https://docs.databricks.com/aws/en/admin/users-groups/manage-service-principals
[sp-acl]: https://docs.databricks.com/aws/en/security/auth/access-control/service-principal-acl

What follows from it is worth stating plainly. An account admin can grant
Databricks permissions. This operator does not, and holds no code that could —
but the account can no longer prove that from the permissions alone, only from
what the operator is. That is why it does not grant, and will not:
[Issuing an identity, not a permission](identity-not-permission.md).

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

`namespaces` is where this operator will act, by name: it says which namespaces
your account admin credential may be spent on. Naming none serves none, and that
is the default — a fresh install is inert until its owner says where it may act,
so a `DatabricksAccount` written without this field is correct and does nothing.

It is one of the three separate yeses an identity needs, and the only one that is
yours: [Three people have to say yes](who-says-yes.md). What the list means once
a namespace is on it, and what taking one off does, is
[What the namespace list means](what-the-namespace-list-means.md) — read that
before you ever shorten the list, because shortening it destroys.

### Why `host` and `accountId` cannot be changed

The API server refuses the edit. A service principal id means nothing outside the
account it was made in, so an operator acts in one Databricks account for its
whole life, and a value that could be corrected in place would silently make
every record the operator holds a record of something in a different account.

Correcting either one means deleting this object and writing the one you meant,
and a second account means a second operator:
[Several operators in one cluster](several-operators.md).

Deleting is itself refused while any record in this namespace still names that
account — the object says how many there are, and how to get out of it when the
account can no longer be reached — and an operator whose records name an account
other than the one `--databricks-account` selects refuses to start, naming the
record and both accounts in its log.

An install that never reached Databricks recorded no account anywhere, so a typo
in either value, caught before anything was issued, is undone by deleting the
object and writing it again. That is the ordinary case for a fresh install, and
it is why the refusal above is not something you are likely to meet on day one.

### Check it

```sh
kubectl -n dbxsp-operator-system \
  get databricksaccount databricks-account -o yaml
```

`Ready` means the operator can act. If it is not Ready, `status.subject` and
`status.audience` are what the operator actually presents — the federation policy
from step 1 must name those exact values. They are reported even on failure,
which is when you need them, and comparing them against the policy is the whole
of diagnosing a step 1 that did not quite land.

Installation is done. Everything after this is per-team and per-workload, and
none of it is yours: a namespace holder asks for an identity by annotating a
ServiceAccount ([Asking for an identity](asking-for-an-identity.md)), and what
their pods then carry is [What the pod gets](what-the-pod-gets.md).

## Where to go next

| For                                                    | Read                                                              |
|--------------------------------------------------------|-------------------------------------------------------------------|
| Who else has to agree before an identity exists        | [Three people have to say yes](who-says-yes.md)                   |
| What `spec.namespaces` promises, and what removal does | [What the namespace list means](what-the-namespace-list-means.md) |
| A second Databricks account in the same cluster        | [Several operators in one cluster](several-operators.md)          |
| What each object is and who writes it                  | [The three objects](the-three-objects.md)                         |
| Something is not Ready                                 | [When something is wrong](when-something-is-wrong.md)             |
| What was actually run against a live account           | [What was measured](measured.md)                                  |
