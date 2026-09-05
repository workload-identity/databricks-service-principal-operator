# Three people have to say yes

For whoever holds the Databricks account, and for the cluster admin they will
have to ask, working out who has to agree before any identity exists.

An annotation on a ServiceAccount is a request. It does nothing until three
separate people have each said something, and none of them can say another's:

| Who                                     | Says                                | Where                                                   |
|-----------------------------------------|-------------------------------------|---------------------------------------------------------|
| A cluster admin                         | this namespace may be served at all | `databricks.workload-identity.io/mint` on the Namespace |
| The team holding the Databricks account | I will spend my credential here     | `spec.namespaces` on their DatabricksAccount            |
| The team holding the namespace          | this workload wants an identity     | the annotation on the ServiceAccount                    |

They are in three different places because they belong to three different people,
and each place is one only its owner can write. A Namespace label needs cluster
access. The `DatabricksAccount` is in the operator's own namespace. The
annotation is on an object inside the asking team's namespace.

Withhold any of the three and nothing happens, and nothing says so. That is
deliberate: until an identity exists there is no object to carry a condition, and
an object in every namespace announcing that it has nothing would be this
operator answering a question nobody asked. All three are read rather than
reported — the label on the Namespace, the list on the DatabricksAccount, and the
annotation on the ServiceAccount — so what there is to check is the three places
themselves.

## The cluster admin: this namespace may be served

Once per namespace, with two labels:

```sh
kubectl label namespace team-a \
  databricks.workload-identity.io/inject=enabled \
  databricks.workload-identity.io/mint=enabled
```

| Label    | What it decides                                 | When to remove it                                                                      |
|----------|-------------------------------------------------|----------------------------------------------------------------------------------------|
| `mint`   | Whether new identities may be minted here       | As soon as you have minted what you meant to. Nothing that already exists is affected. |
| `inject` | Whether pods created here are given their token | Only when this namespace has stopped using Databricks altogether.                      |

### Why that is two labels and not one

They are separate because they end at different times. Minting is an event — open
it, mint, close it. Equipping pods is continuous: an identity minted while
minting was open needs every new pod under it equipped for as long as it is used.

One label for both would mean that closing the minting switch quietly broke every
workload in the namespace on its next rollout — not immediately, which would at
least be noticed, but whenever something happened to recreate a pod. The two
labels are what keeps a tidying-up act from being a delayed outage.

`inject` left on a namespace that no longer uses Databricks costs one admission
call per pod and nothing else — a pod whose ServiceAccount has no identity is
admitted unchanged. Forgetting it is cheap, which is the point of it being the
one you are told to leave alone.

### `mint` is not a security boundary

The identity a ServiceAccount can ask for is
`system:serviceaccount:<its own namespace>:<itself>`, and it is created holding
nothing. So `mint` does not stop anybody reaching something they could not reach
anyway. It decides which teams are in.

That it holds nothing is the reason handing `mint` out is safe at all, and it is
a property of this operator that is deliberately kept rather than a stage it has
not got past: [Issuing an identity, not a permission](identity-not-permission.md).

### Neither label revokes

Removing `mint` stops new identities and leaves every existing one alone.
Removing `inject` stops new pods being equipped and destroys nothing.

To end a workload's access, the key is removed by whoever asked for it — which is
the point of the request and the permission being two separate things:
[Asking for an identity](asking-for-an-identity.md).

## The account holder: I will spend my credential here

A cluster saying a namespace may be served is not the same as a platform team
agreeing to spend their account admin credential on it. Those are two people, and
`spec.namespaces` on the `DatabricksAccount` is the second one's answer:
[Installing the operator](installing.md#3-point-it-at-the-account) is where it is
first written.

It is theirs and nobody else's, by where it lives. The `DatabricksAccount` is in
the operator's own namespace, so writing to it needs the access that could
already replace the operator's image — anyone who can add a namespace to the list
could have replaced the operator outright, so the list gives away nothing that
was not already given away. A namespace cannot add itself, and a team that wants
in asks the team holding the account rather than editing anything of their own.

That is the whole of why the list is a decision and not a convenience. What the
list then means, in each of the three states a namespace can be in, and why
taking one off is destructive, is
[What the namespace list means](what-the-namespace-list-means.md).

## The namespace holder: this workload wants an identity

The annotation, written by whoever holds the namespace, on a ServiceAccount in
it — [Asking for an identity](asking-for-an-identity.md).

It is a request rather than a permission, and it is the only one of the three
that can be made by somebody who does not administer anything. That is safe for
exactly one reason: what it asks for arrives holding nothing.
