# Finding out what a service principal is

For whoever holds the Databricks account, looking at a service principal there
and wanting to know what made it and what it answers to.

Everything below is read from Databricks. Nothing needs access to the cluster,
except the one command that prints the marker to compare against — and somebody
else can run that for you and paste the string.

## The short version

| You have            | Read                                              | It tells you                                      |
|---------------------|---------------------------------------------------|---------------------------------------------------|
| A service principal | its federation policy                             | the cluster and the ServiceAccount, both verbatim |
| A service principal | `externalId`                                      | whether it is this cluster's, by comparison       |
| A cluster           | `status.clusterMarker` on its `DatabricksAccount` | what to compare against                           |

The federation policy is the answer. The rest is for narrowing a listing.

## The federation policy says it in plain text

Every identity this operator issues has exactly one federation policy, and that
policy is the record: it names the cluster's OIDC issuer and the ServiceAccount's
subject as literal strings, because that is what Databricks matches a presented
token against.

```
issuer   https://oidc.eks.<region>.amazonaws.com/id/<cluster>
subject  system:serviceaccount:team-a:etl
```

So a service principal with that policy belongs to `team-a/etl` on the cluster
that publishes that issuer. Nothing is hashed, truncated or encoded, and nothing
about it can be edited without changing which token is accepted.

This is also why deleting a service principal is the whole of revoking it:
[what was measured](measured.md#federation-policies) records that its policies go
with it.

## `externalId` is a marker, not a name

Every service principal this operator creates carries an `externalId` of 36
characters, which is three hashes of twelve:

```
5IF3QENN435U  ZQ7X2WPLK4TA  MJ6VD3BHY8CN
issuer        operator      ServiceAccount uid + identity name
```

**None of it can be read backwards.** A hash is not a shortened name; there is
nothing in those twelve characters to decode. What it is for is comparison: take
the marker a cluster publishes and see whether a service principal's `externalId`
starts with it.

The cluster publishes its own:

```sh
kubectl -n dbxsp-operator-system get databricksaccount databricks-account \
  -o jsonpath='{.status.clusterMarker}'
```

That is the first twelve characters every identity that cluster made carries, and
that nothing else does. `status.issuer` beside it is the issuer it was computed
from, which is the value you will also find on the federation policies.

### Everything one cluster issued

There is no server-side filter for this. The account's SCIM listing filters on
`displayName` and refuses a filter on `externalId` with a 400 —
[measured](measured.md#the-scim-api) — so the answer is: list the service
principals and keep the ones whose `externalId` starts with that cluster's
marker. On an account of any size that is one listing and a string comparison,
not a query.

### Which of two operators issued it

The second twelve characters are the operator: its namespace and the name of its
`DatabricksAccount`, hashed together. Two operators serving one Databricks
account produce identities whose markers agree in the first part and differ in
the second ([Several operators in one cluster](several-operators.md)).

**This part of the marker is the only thing in Databricks that tells them
apart.** Both operators can be asked for an identity by the same ServiceAccount,
and then the two service principals carry federation policies naming the same
issuer, the same subject and the same audience — that is a shape Databricks
accepts, and which one a token exchanges for is decided by the `client_id` in
the request rather than by anything on the policies.

Nothing publishes that second part today, so from the Databricks side two such
identities are distinguishable but not identifiable. The way round it is to ask
each operator instead: the `IssuedDatabricksServicePrincipal` objects in an
operator's own namespace record every service principal id it made
([The three objects](the-three-objects.md)).

## `displayName` is a hint

```
k8s-team-a-etl
k8s-team-a-etl-reader
```

It is built from the namespace, the ServiceAccount name and the identity's name
if it has one, prefixed `k8s-` and cut at 100 characters. It is there so that a
person reading a list in the Databricks console can see what they are looking at.

**Do not conclude anything from it.** Databricks enforces no uniqueness on a
display name — two service principals may share one, measured — anybody can edit
it, and a long namespace and ServiceAccount together reach the cut. It is the
first thing to look at and never the thing to act on.

## The service principals that are not identities

Two kinds of service principal in the account are not workload identities and
carry no marker:

- **The operators' own.** Each operator authenticates as one, created by hand at
  install time, named on its `DatabricksAccount` as `spec.clientId`. It has a
  federation policy like any other, naming the operator's own namespace and
  ServiceAccount, and it is an account admin.
- **Anything a person made.** This operator only ever touches a service principal
  carrying its own marker.

So a service principal with no `externalId` was not made here, and neither this
operator nor any other install of it will delete it, adopt it, or write a policy
on it.

## What you cannot find out from Databricks

- **Whether the ServiceAccount still exists.** A federation policy names a
  subject and Databricks checks nothing about it — a policy for a ServiceAccount
  that was deleted keeps working, which is why deleting the service principal is
  the whole of revocation. A service principal whose ServiceAccount is gone looks
  exactly like one whose ServiceAccount is fine.
- **Which namespace it is in now.** The marker holds the ServiceAccount's uid, not
  its namespace, and the display name holds the namespace as it was when the
  identity was issued.
- **Whether the cluster still exists.** The issuer on the policy is a URL that may
  no longer resolve.

Each of those is a question for the cluster, and the object that answers it is
the `IssuedDatabricksServicePrincipal` in the operator's namespace
([The three objects](the-three-objects.md)).
