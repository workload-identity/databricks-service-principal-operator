# Several operators in one cluster

For whoever holds a Databricks account in a cluster where somebody else already
holds one, or where a second account is about to arrive.

## One operator per Databricks account

An operator serves exactly one Databricks account, because it holds that
account's credential and that credential is an account admin
([why it has to be](installing.md#why-it-has-to-be-an-account-admin)). Two
accounts need two operators, and each account admin then trusts only their own —
which is the property that makes the arrangement acceptable at all. Sharing one
operator between two accounts would mean each account's admin credential sitting
inside a process the other account's holder can also reach.

That is also why `host` and `accountId` cannot be edited on a `DatabricksAccount`
and a second account means a second install rather than a second object:
[why they cannot be changed](installing.md#why-host-and-accountid-cannot-be-changed).

## Installing the second one

What a second install needs is a second set of cluster-scoped names —
ClusterRoles, ClusterRoleBindings, and both admission webhook configurations —
and its own namespace. Two ways to get them.

A kustomize overlay naming both:

```yaml
resources:
- ../../config/default
namespace: finance-operator
namePrefix: finance-
images:
- name: controller
  newName: <registry>/databricks-service-principal-operator
  newTag: <tag>
```

`kustomize build` that directory and apply it. `test/e2e/sharing_test.go`
installs its second operator exactly this way, so the arrangement is exercised
rather than only argued for.

Or the Helm chart, which derives the same names from the release instead, so
there is no overlay to write:

```sh
helm install finance \
  oci://ghcr.io/workload-identity/charts/databricks-service-principal-operator \
  --version 0.15.0 \
  --namespace finance-operator --create-namespace
```

What neither can be is the released `install.yaml` applied twice. Its names are
fixed, so the second apply overwrites the first install's cluster-scoped objects
— including the webhook configurations, which is the half that fails quietly:
`failurePolicy` is `Ignore`, so the operator that lost simply stops being
consulted.

The two installs do not have to match. The prefix, the namespace and the image
are each install's own, and nothing reads another install's names.

## An operator is named after the account, not after a team

Several teams can share one Databricks account, and they then share one operator.
Naming the operator after a team makes the second team's annotation read as a
request to somebody else's operator, and the name goes stale the first time the
team that installed it is not the only one using it.

Each operator runs in its own namespace, and says which namespaces it will serve
on its own `DatabricksAccount`:

```yaml
# in the finance operator's namespace
spec:
  namespaces:
  - team-a
  - team-b
```

`team-a` and `team-b` use the finance account, so that operator serves both;
`risk` is named on the other operator's account instead.

The reference a namespace holder writes in an annotation is that operator's
namespace and the name of its `DatabricksAccount`, joined by `/`. It is an
address rather than a nickname, because the API server will not hold two objects
of one kind at one address — so two operators cannot be confused for each other,
and nobody has to keep a list of who chose which name.
[Asking for an identity](asking-for-an-identity.md) is where that reference is
looked up and used.

## What one operator does about another's namespaces

Nothing, in the ordinary case. A namespace that is not on this account's list is
a namespace this account has nothing in.

The exception is a namespace that was on the list and was taken off, which is not
the same state and is not passive: that account destroys what it issued there,
and while it is doing so it suspends minting in that namespace for everybody,
including the other operator, until it has finished.
[What the namespace list means](what-the-namespace-list-means.md) is that whole
mechanism, including why the suspension is a different key from the cluster
admin's `mint` label.

## Two operators asked about the same ServiceAccount refuse only their own

Each install carries its own `ValidatingWebhookConfiguration` on ServiceAccounts,
so a write that introduces one of these annotations is put to both operators.
They do not fight, and the reason is sharper than agreement: neither is asked
about the other's request at all. Each reads only the keys whose value names its
own `DatabricksAccount`, and a key naming the other operator is admitted without
a thought — it is that operator's to answer, and standing in front of a decision
that is not its own is the one thing a refusal here must not do. A key that will
not parse names nobody, and is admitted by both for the same reason.

So a ServiceAccount asking the finance operator and the risk operator from one
namespace is refused by exactly the one whose account does not name it, and the
message names that account rather than "the operator" — which is what makes it
actionable, since the two are edited by different people.

Neither operator's copy of it can carry a `namespaceSelector`, because the
namespaces it exists for are exactly the ones nobody enrolled: a request in an enrolled
namespace is answered, and it is the request in a namespace an account never
named that goes unanswered. A selector would exempt the only case they exist for.
What keeps that cheap instead is a CEL `matchConditions` evaluated in the API
server, which sends on only a ServiceAccount carrying one of these keys — a
handful of writes in the cluster rather than all of them. Both operators pay for
their own, and neither pays for the other's.

## Two operators asked about the same pod agree without coordinating

A pod can hold identities from more than one account — a workload reading from
one and writing to another — and then both operators' webhooks are asked about
the same pod.

They agree without talking to each other, because neither one is the source of
what the pod should carry. Each reads the same `DatabricksServiceAccount`, which
carries every identity that ServiceAccount was issued, whichever operator issued
it. So whichever is asked first equips the pod with all of them, and the second
finds its work already done. There is no ordering to arrange and no lock to hold:
being second is indistinguishable from there being nothing to do.

### They need not agree on an audience

Each identity's token is minted for whatever its own operator's token carries,
and the pod holds one token per identity rather than one token that has to
satisfy everybody. So the audience is a local decision inside one account's
install, and nobody has to go round asking the other platform teams what theirs
is before choosing one.

That matters more than it looks, because the audience is fixed in step 1 of an
install — it is written into the operator's own federation policy before the
operator has ever run, by whoever holds that account, usually before they know
who else is on the cluster. If audiences had to match, the second account to
arrive would have to either adopt the first one's choice or ask the first to
change a federation policy it had already been running on.

What the pod ends up holding, one token per identity, is
[What the pod gets](what-the-pod-gets.md), and a workload holding several
identities picks between them by name:
[Several identities for one ServiceAccount](several-identities.md).
