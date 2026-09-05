# Several identities for one ServiceAccount

For whoever holds a namespace and needs one workload to act as more than one
Databricks service principal.

[Asking for an identity](asking-for-an-identity.md) covers one workload, one
identity, which is what nearly every ServiceAccount asks for. This is the rest of
it.

A pod binds exactly one ServiceAccount. A workload that reads from one place and
writes to another needs an identity for each, and one Databricks account holding
several service principals for one workload — one granted read, another granted
write — is ordinary practice there. So the number of identities is the asker's to
decide, and the asker names each one.

## One key per identity

```
databricks.workload-identity.io/service-principal          <operator>
databricks.workload-identity.io/service-principal.<name>   <operator>

    <operator> ::= <operator namespace>/<DatabricksAccount name>
```

Every key's value is the operator being asked, written the way
[First, find the operator you are asking](asking-for-an-identity.md#first-find-the-operator-you-are-asking)
finds it. What differs between the keys is the name after the `.`, and that name
is what the identity is called everywhere you meet it afterwards.

| You write                                                    | You get                                              |
|--------------------------------------------------------------|------------------------------------------------------|
| `service-principal: ops-a/...`                               | that operator's one identity for this ServiceAccount |
| `service-principal.reader: ops-a/...`                        | an identity you call `reader`, from that operator    |
| `service-principal.reader` and `.writer`                     | two identities in the same Databricks account        |
| `service-principal.raw: ops-a/...` and `.curated: ops-b/...` | one identity in each of two Databricks accounts      |

Two identities in one account and one identity in each of two accounts are the
same annotations with different values. Nothing downstream distinguishes them
either.

```sh
OPERATOR=dbxsp-operator-system/databricks-account

kubectl -n team-a annotate serviceaccount etl \
  databricks.workload-identity.io/service-principal.reader=$OPERATOR \
  databricks.workload-identity.io/service-principal.writer=$OPERATOR
```

No `--overwrite`, and that is not an accident of the example: each key is written
on its own, so adding an identity does not touch the ones already there.

```
$ kubectl -n team-a get databricksserviceaccount etl -o yaml
status:
  identities:
  - profile: reader
    operator: dbxsp-operator-system/databricks-account
    clientId: 11111111-1111-1111-1111-111111111111
    ...
  - profile: writer
    operator: dbxsp-operator-system/databricks-account
    clientId: 22222222-2222-2222-2222-222222222222
    ...
```

`profile` is what your code passes to the SDK — here the identity's own name —
and `operator` is the operator that issued it, echoed back from the value you
wrote. They are two fields because they answer two questions: which profile your
code names, and whose entry this is.

## Each key is edited on its own

This is the property everything below rests on, so it is worth being exact about.

| The edit                                     | What happens                                                                  |
|----------------------------------------------|-------------------------------------------------------------------------------|
| A key is added                               | That identity is created. Nothing else is touched                             |
| A key's value is changed to another operator | The old operator destroys its identity; the new one issues its own            |
| A key is deleted                             | That identity is destroyed in Databricks, along with everything granted to it |
| A key's value does not parse                 | That key is refused. Nothing is created for it and **nothing is destroyed**   |
| A key's name does not parse                  | The same, and no identity of that name has ever existed to lose               |

A refusal is not reported anywhere. What you see is the key you edited not taking
effect: no entry appears for it, and the operator says nothing about it. Whether
a key reads is decided by the characters in the key and its value, so the line
you wrote is the whole of what there is to check.

**Refusing is safe because refusing changes nothing.** The identity a refused key
names is left exactly as it stands — still issued, still exchangeable, its pods
still working — until the key is either fixed or deleted. That is why a mistyped
value costs you the edit rather than a service principal.

It reads as an obvious property and it was not available before. A single
comma-separated value made "this identity is no longer asked for" and "this
identity was misspelled" the same input — an entry that is no longer in the
string — so the operator had to guess, and guessing that nobody is asking for it
deletes a service principal and every grant made on it. As map entries, the two
are a key that is gone and a key that is there.

## The name is what your code looks things up by

Lowercase letters, digits, and `-` between them, and at most 45 characters. That
limit is measured rather than chosen: an annotation key's name part caps at 63
bytes and `service-principal.` is 18 of them.

`.` and `_` and uppercase are refused, even though the API server accepts all
three in a key. The name becomes part of the name of a Kubernetes object — the
record is `<namespace>.<serviceaccount>.<identity>-<hash>` — which takes neither
`_` nor uppercase; and a `.` inside the identity leaves nothing to say where the
ServiceAccount's name ends and the identity's begins. So the operator checks the
name itself and says so on the ServiceAccount, rather than letting the API server
refuse the record in the middle of a reconcile where the message reaches nobody.

Nothing derives the name and nothing rewrites it. The string you put after the
`.` is the profile name in the pod, unchanged — so your code names what you
already wrote in the annotation, rather than learning a rule for how this
operator would have spelled it.

**Two identities cannot share a name, and nothing has to check that.** They are
keys of one map on one ServiceAccount, so the API server keeps them apart:
writing `service-principal.reader` twice is not two identities, it is one key
written twice. A named identity cannot collide with the unnamed one either — the
unnamed one's profile is the operator reference, which holds a `/`, and no
identity name may.

**Name them after whatever actually distinguishes them.** Two identities exist
because they differ in some way you care about; that difference is what your code
has to look them up by, and the name is the only place it can be written:

| They differ by                 | Name them                |
|--------------------------------|--------------------------|
| what they are allowed to do    | `reader`, `writer`       |
| which workspace they can reach | `analytics`, `lakehouse` |

Uniqueness is now enforced; meaning is still yours. `a` and `b` are distinct
names, are accepted, are issued, and leave your code with two profiles it cannot
tell apart.

```python
import os
from databricks.sdk import WorkspaceClient

config = os.environ["WORKLOAD_IDENTITY_DATABRICKS_CONFIG_FILE"]

reader = WorkspaceClient(config_file=config, profile="reader")
writer = WorkspaceClient(config_file=config, profile="writer")
```

The profile is the name and nothing else. There is no operator reference in it,
because which operator issued an identity is not something a workload has to know
to use it.

`WORKLOAD_IDENTITY_DATABRICKS_CONFIG_PROFILE` names the identity asked for with
the bare `service-principal` key, so a workload with one default identity and
some named extras still writes
[the one line](what-the-pod-gets.md#what-your-workload-writes) for the default. A
ServiceAccount whose keys are all named has no such default, and its workloads
should name the profile they want rather than take whichever entry happens to be
first.

## Which workspace each one reaches

Two cases, and they need different amounts of work.

### Both identities, one workspace

The common one: two service principals in one account, granted different things,
reaching the same workspace. One host answers for both, so the SDK's own variable
is enough and your code says nothing about it:

```yaml
env:
- name: DATABRICKS_HOST
  value: https://dbc-example.cloud.databricks.com
```

Both clients above pick it up. A ConfigMap holding that one value works the same
way and is worth it when several workloads share it:

```yaml
envFrom:
- configMapRef:
    name: databricks-workspaces
```

### Each identity a different workspace

`DATABRICKS_HOST` is one value and cannot say two things, so each host has to be
named separately and passed per client. A ConfigMap is where those live:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: databricks-workspaces
  namespace: team-a
data:
  ANALYTICS_HOST: https://dbc-a.cloud.databricks.com
  LAKEHOUSE_HOST: https://dbc-b.cloud.databricks.com
```

```python
clients = {
    name: WorkspaceClient(
        config_file=config,
        profile=name,
        host=os.environ[f"{name.upper()}_HOST"],
    )
    for name in ("analytics", "lakehouse")
}
```

The identity's name is the profile name and the ConfigMap key, so the
correspondence is in the names and there is no second table to keep in step. That
is what the naming rule above is for: with `a` and `b` this loop cannot be written
at all.

An explicit `host=` beats `DATABRICKS_HOST`, which beats the profile — so this
works whether or not something else in the pod sets the variable.

### Who knows which identity reaches which workspace

A person, and only a person. That an identity can reach a workspace is a grant
made in Databricks, after this operator created the service principal, by whoever
governs the account. This operator grants nothing and reads no permissions, so it
does not know and cannot report it. Neither does the cluster.

So the ConfigMap above is that knowledge written down, by whoever holds it, in
the environment it is true for. Nothing derives it, and removing the guesswork is
what the naming rule buys you: the names carry the correspondence, and the
ConfigMap carries the values.

Using the wrong one is not silent. The token exchange succeeds — a federation
policy matches an issuer, a subject and an audience, and says nothing about
workspaces — and the call is then refused by Databricks for lack of permission,
which names what was denied.

## What the pod gets

One token per identity, each minted for its own audience, and one configuration
naming all of them:

```
/var/run/secrets/databricks/
├── config
├── reader/token
└── writer/token
```

`config` holds one profile per identity — `[reader]` and `[writer]` here — each
carrying that identity's own `client_id` and its own token path. The format of a
profile, and why the token path and the audience are each written under two
spellings, are the same whether a ServiceAccount asked for one identity or six:
[What the pod gets](what-the-pod-gets.md). So is the one line your workload
writes to read any of it.

One path segment per identity, named the same as the profile. The identity asked
for with the bare key is the one exception, because its profile name is the
operator reference: it keeps the `/` and so lands two segments deep, at
`dbxsp-operator-system/databricks-account/token`.

**Two operators serving one workload need not agree on an audience.** Each
identity's token is minted for whatever its own operator's token carries, and the
pod holds one token per identity — so nobody has to go round asking the other
platform teams what theirs is. Both operators are asked about the same pod and
agree without coordinating: whichever is asked first equips it with every
identity in the DatabricksServiceAccount, and the second finds its work already
done. Why there would be two at all is
[Two Databricks accounts in one cluster](several-operators.md).

## Ending one and keeping the rest

Delete that one key. The trailing `-` is what `kubectl annotate` reads as delete:

```sh
kubectl -n team-a annotate serviceaccount etl \
  databricks.workload-identity.io/service-principal.writer-
```

The writer's service principal is deleted in Databricks — everything Databricks
recorded against that principal goes with it — and the reader's is untouched,
because nothing about the reader's key was part of this edit. There is no whole
string to read back before pressing enter: the identities you did not name are
not in the command.

## What each identity is recorded as

One record per identity, in the operator's own namespace:

```
$ kubectl -n dbxsp-operator-system get isdbxsp
dbx-demo.etl-h7x3k9qp
dbx-demo.etl.reader-p2m8w4tq
dbx-demo.etl.writer-k9f3n7bx
dbx-demo.loader-x4c1r6ds
```

The identity's name comes after the ServiceAccount's so that one workload's
identities sort together; before it, a listing would gather every `reader` in the
cluster and scatter each workload's own.

In Databricks they are told apart by their display names — `k8s-<namespace>-<serviceaccount>-<identity>`
— and, for the operator itself, by an `externalId` that says which cluster, which
operator, and which identity. Two identities of one ServiceAccount share
everything else: one cluster, one operator, one uid, and the same subject, since
a federation policy matches the subject and both name the same one.

### The one place two identities can look alike

A display name is capped at 100 characters and truncated to fit. A namespace and
a ServiceAccount name are each allowed to be long enough that the cut lands
before the identity's name is reached, and the two then show as two rows with
identical names in the Databricks console:

```
namespace: 63 characters, ServiceAccount: 63 characters

k8s-nnnn…nnnn-nnnn…nnnn   ← analytics
k8s-nnnn…nnnn-nnnn…nnnn   ← lakehouse
```

Nothing breaks. The operator finds its service principals by the id it recorded
and by the marker, and both of those still differ — Databricks enforces no
uniqueness on a display name either. The record's own name survives the same cut,
because the identity is hashed into its suffix as well as shown in its readable
half. What is lost is a person reading that console page being able to say which
row is which, and that page is where permissions are granted.

So if your namespace and ServiceAccount names together approach 90 characters,
check the console once after the first grant. Everything else about the identity
survives the cut; only the part a human reads does not.
