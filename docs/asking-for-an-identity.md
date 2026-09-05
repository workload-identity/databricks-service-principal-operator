# Asking for an identity, and ending one

For whoever holds a namespace and wants a workload in it to reach Databricks.
None of it needs a cluster admin, and none of it needs anything in Databricks.

You write one annotation key on the ServiceAccount your workload runs as. That
key is the request, and it is also the whole of what you withdraw to end it: an
identity lasts exactly as long as the key naming it. That is why asking and
ending are one page — the second half is the first half read backwards.

The annotation is a request, not a permission. Two other people have to have said
yes before it does anything, neither yes is yours to give, and neither is visible
from your namespace: [Two people have to say yes first](who-says-yes.md). One of
the two is now checked as you write, so the order matters and the cluster will
tell you if you have it wrong — below. What
the identity may then read or run in Databricks is decided in Databricks, by
somebody else again: [Issuing an identity, not a permission](identity-not-permission.md).

## First, find the operator you are asking

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

If more than one is listed, they serve different Databricks accounts, and which
one you want is which account your data is in:
[Two Databricks accounts in one cluster](several-operators.md).

## Then annotate

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

### If that write is refused

The account holder's yes has to already be there, and the API server checks it
while you are standing at the keyboard:

```
Error from server (Forbidden): admission webhook
"serviceaccountrequest.databricks.workload-identity.io" denied the request:
ServiceAccount team-a/etl asks DatabricksAccount
dbxsp-operator-system/databricks-account for a Databricks identity through
databricks.workload-identity.io/service-principal, and team-a is not one of the
namespaces that account names. Add team-a to spec.namespaces of DatabricksAccount
dbxsp-operator-system/databricks-account first, and write the annotation after
that. In that order, because the list is what says this account has identities in
a namespace: taking a namespace off it destroys every identity there. Written
now, the annotation would be answered by nothing at all -- no
DatabricksServiceAccount, no condition and no event -- and the workloads here
would go on failing every call to Databricks with an error naming Databricks.
```

Nothing was stored, so there is nothing to undo. What it asks for is not yours to
do: your namespace has to be named on that `DatabricksAccount`'s
`spec.namespaces`, which is the account holder's own edit
([Three people have to say yes](who-says-yes.md#the-account-holder-i-will-spend-my-credential-here)),
and your annotation goes on after it. Asking in the other order is not slower; it
does not go through at all.

Only that one thing is refused. A key naming an operator that is not there, or
one whose value will not parse, goes in and sits there: it is nobody's to refuse,
so no operator refuses it. That case is [below](#ending-an-identity), and it is
still the quiet one.

## More than one identity

A workload that reads with one service principal and writes with another asks for
each under its own key, naming it there:

```sh
kubectl -n team-a annotate serviceaccount etl \
  databricks.workload-identity.io/service-principal.reader=$OPERATOR \
  databricks.workload-identity.io/service-principal.writer=$OPERATOR
```

The name after the `.` is the profile name your code asks for, and the value is
the same operator reference as above. That is
[Several identities for one ServiceAccount](several-identities.md), and
everything on this page holds unchanged for the one-identity case.

## Recreate pods that were already running

A pod is equipped when it is created, and nothing rewrites a running one. The
object's `Equipped` condition names the pods that are missing what they should
have:

```sh
kubectl -n team-a get databricksserviceaccount etl \
  -o jsonpath='{.status.conditions[?(@.type=="Equipped")].message}'
```

Recreating them is whatever your pod spec's own rollout is. What each pod then
comes up holding, and the one line your workload writes to use it, is
[What the pod gets](what-the-pod-gets.md).

---

## Ending an identity

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

**Removing a key is never refused, in any namespace.** The webhook that refused
you above refuses only the write that *introduces* a request, so taking one off
always goes through — in a namespace that has just refused you, and in one the
account holder has taken off their list since. So does any unrelated edit to a
ServiceAccount that already carries the key. A rule that refused the state
instead would leave such a ServiceAccount permanently unwritable, with no way to
withdraw the request either, which is a worse failure than the one it prevents
and lands on people who did nothing.

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
[Ending one and keeping the rest](several-identities.md#ending-one-and-keeping-the-rest).

### What else destroys one

**Deleting the ServiceAccount does the same thing, for the same reason. So does
deleting the namespace.** The key is gone either way, and nothing is left asking.

**One thing else destroys one, and it is not yours to do:** the team holding the
account taking your namespace off their `spec.namespaces`. That destroys every
identity this account issued in it, and re-adding the namespace issues new ones
rather than bringing those back — see
[What the namespace list means](what-the-namespace-list-means.md).

### What does not destroy one

**Nothing else does.** Removing the namespace's `mint` label stops new identities
from being asked for and leaves existing ones alone, which is why a cluster admin
can close minting without breaking anything already running. Deleting the
`DatabricksServiceAccount` in your namespace does nothing at all: that object is a
copy of a record the operator keeps in its own namespace, and it is rebuilt on the
next pass — [The three objects](the-three-objects.md).

If an identity is not there and you did not end it, the condition on the object
says which of these happened:
[When something is wrong](when-something-is-wrong.md).
