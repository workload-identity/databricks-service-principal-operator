# When something is wrong

For whoever has a symptom and does not yet know which object to look at.

The tables at the bottom are ordered for somebody who already knows that. This
page is ordered the other way round, because a symptom is what you actually
arrive with. Find yours below; the reason a symptom has the answer it has is
written next to it, which is what stops the next one being a guess.

- [I annotated a ServiceAccount and nothing happened](#i-annotated-a-serviceaccount-and-nothing-happened)
- [My workload gets 403](#my-workload-gets-403)
- [My workload's token is refused](#my-workloads-token-is-refused)
- [The pod is running and has no token](#the-pod-is-running-and-has-no-token)
- [It worked and then stopped](#it-worked-and-then-stopped)
- [The operator is not doing anything at all](#the-operator-is-not-doing-anything-at-all)

---

## I annotated a ServiceAccount and nothing happened

**There is genuinely nothing to look at, and that is the answer rather than a
gap.** No `DatabricksServiceAccount` appears, no condition is written, and no
event is emitted. Until an identity exists there is no object to carry a
condition, and an object in every namespace announcing that it has nothing would
be this operator answering a question nobody asked.

So the check is not "what does the cluster say". It is the three things that each
have to be true before anything happens, all three of which are **read** rather
than reported:

| Has to be true                                            | Read from                                               | Who writes it                           |
|-----------------------------------------------------------|---------------------------------------------------------|-----------------------------------------|
| This namespace may be served at all                       | `databricks.workload-identity.io/mint` on the Namespace | a cluster admin                         |
| This account will spend its credential here               | `spec.namespaces` on the operator's `DatabricksAccount` | the team holding the Databricks account |
| This ServiceAccount is asking, of an operator that exists | the annotation key and its value on the ServiceAccount  | whoever holds the namespace             |

Three different people, and none of them can say another's. Withhold any one and
nothing happens.

The third is the one to read character by character, because it is the one you
just typed. The value is `<operator namespace>/<DatabricksAccount name>`, and a
value that does not parse — or that names an operator which is not there — is
refused. **A refused key is left alone deliberately**: an identity a refused key
names stays issued and stays exchangeable, because the alternative is an operator
that destroys a service principal, and every grant made on it, over a mistyped
character. That is why the failure is silent, and why the line you wrote is the
whole of what there is to check.

`kubectl get databricksaccounts -A` names the operators that exist, in the form
the value takes. The rest is [docs/who-says-yes.md](who-says-yes.md).

## My workload gets 403

**The identity is right and nobody has granted it anything.** This is the design
working, not a fault.

An identity this operator issues arrives holding nothing at all: no Unity Catalog
privilege, no workspace entitlement, no group. What it may reach is decided in
Databricks, by somebody holding a credential that needs none of the account admin
this operator has, after the service principal exists. So a 403 on the first call
of a brand new identity is the expected first state, and the fix is a grant made
by that somebody rather than anything in the cluster.

`status.identities[].clientId` on the `DatabricksServiceAccount` is what they
grant to. Why the operator will not do it itself, and what that costs, is
[docs/identity-not-permission.md](identity-not-permission.md).

Worth ruling out one nearby case: a token exchange succeeds whatever workspace
you point it at, because a federation policy matches an issuer, a subject and an
audience and says nothing about workspaces. So an identity used against the wrong
workspace also arrives here, as a refusal naming what was denied.

## My workload's token is refused

Two failures land here and **the message tells them apart**, so read it before
changing anything.

| The message                                                            | What it is                                                               |
|------------------------------------------------------------------------|--------------------------------------------------------------------------|
| `TOKEN_AUDIENCE_INVALID`                                               | the token was minted for one audience and the policy will accept another |
| `TOKEN_INVALID (Ensure a valid federation policy has been configured)` | the request carried no `client_id`                                       |

`TOKEN_AUDIENCE_INVALID` names both audiences — the one presented and the one the
policy accepts — so it is self-answering, and the two values it prints are the
whole of the diagnosis. Measured against a live account: a policy matches the
audience exactly, and a token minted for any other is refused with the audience
it presented echoed back — [docs/measured.md](measured.md).

The second message is the trap, because it blames the federation policy and the
policy is correct. A token exchange sent without a `client_id` is refused with
the policy's own description quoted back at you. Nothing in the policy will fix
it; what is missing is on the request.

**The token every pod already carries can never be exchanged.** Its audience is
the API server's, and a federation policy checks `aud`. That is the whole reason
a second token is projected into the pod at all — required work rather than a
convenience — and it is why pointing an SDK at the default
`/var/run/secrets/kubernetes.io/serviceaccount/token` produces
`TOKEN_AUDIENCE_INVALID` every time. What the pod is given instead is
[docs/what-the-pod-gets.md](what-the-pod-gets.md).

## The pod is running and has no token

**A pod is equipped when it is created, and nothing rewrites a running one.** The
webhook acts at admission; a pod admitted while the webhook was unavailable is
admitted unchanged — deliberately, so that an outage here never stops workloads
from starting — and it stays that way after the webhook comes back, because there
is nothing in Kubernetes that goes round rewriting pods that are already running.

So this does not heal on its own, and waiting is not a plan. `Equipped`, on the
identity's entry in the `DatabricksServiceAccount`, names the pods that are
missing what they should have been given:

```sh
kubectl -n team-a get dbxsa etl -o yaml
```

It is a separate condition from `Ready` because they are different facts and fail
independently: `Ready` is whether the identity can be exchanged for at all, and
`Equipped` is whether the pods that would do the exchanging were given what they
need. It is stated as a promise rather than as a list of what is injected, so
that adding to what a pod gets does not require renaming a condition people alert
on — the list is in the message.

Read the message before recreating the pods, because recreating them is not
always the answer. A namespace that is not labelled for injection, and a pod
whose volume name was already taken, both present exactly like this and both come
back the same way after a restart.

## It worked and then stopped

Two things end a working identity, and they are told apart on the record.

**`RemovedInDatabricks`** — somebody deleted the service principal in Databricks.
It is not an error and not a lookup that failed: Databricks is where identity is
governed, whoever deleted it was entitled to, and an operator that made a new one
while the annotation still stood would be overruling them every minute and
winning, since it never tires. So it is recorded and left alone.
`status.servicePrincipalRemovedAt` says when it was noticed and
`status.servicePrincipalId` is still the id to search the audit log with. To be
issued a new identity, remove that key and write it again — and note that the new
one has a new `applicationId`, so every grant made on the old one has to be made
again.

**The account holder took the namespace off `spec.namespaces`.** That destroys
every identity this account issued there — the service principals are deleted in
Databricks, the grants go with them, and the client ids stop resolving. It is not
a pause, and naming the namespace again is not a restore:
[docs/what-the-namespace-list-means.md](what-the-namespace-list-means.md). A
token already in a running pod is not recalled by any of this. The pod holds it,
and it stops working because the service principal it names is gone.

## The operator is not doing anything at all

**`Prepared` on the `DatabricksAccount` is the one to alert on.**

```sh
kubectl -n dbxsp-operator-system get dbxacc -o yaml
```

It is false when the operator cannot read its own projected token. Both values it
needs come out of that token — the issuer it writes into every federation policy,
and the audience it gives every pod — and neither is configured, because a
configured value can disagree with what is actually presented and a read one
cannot. So nothing new can be issued and nothing converges while it is false.

`Ready` stays true throughout, and that is the reason `Prepared` exists as its
own condition rather than as a sentence in a message. `Ready` says a Databricks
account was reached and read; the SDK holds an access token it already exchanged,
so verification goes on passing for about an hour while no identity anywhere can
be issued. For that hour the state looks like health: the operator is running,
the account is `Ready`, and the only tell is a status that stops halfway. A
message cannot be alerted on.

**Existing workloads are unaffected either way.** Exchanging a token does not go
through this operator — the pod holds its own token and talks to Databricks
directly — so this is an outage of issuing, not of working.

---

# Appendix

## Which object answers what

| Look at                               | Answers                                                             |
|---------------------------------------|---------------------------------------------------------------------|
| `DatabricksAccount` `Ready`           | Whether this Databricks account was reached and read                |
| `DatabricksAccount` `Prepared`        | Whether the operator can issue anything at all                      |
| `DatabricksServiceAccount` `Ready`    | Whether the operator is getting this identity to where it should be |
| `DatabricksServiceAccount` `Equipped` | Whether its pods carry what they need to reach Databricks           |
| `IssuedDatabricksServicePrincipal`    | The same answers, on the object the operator actually acts on       |

What each of those objects is, who writes it, and what deleting it costs, is
[docs/the-three-objects.md](the-three-objects.md) — read that before editing any
of them, because one of the three is the only memory of something outside the
cluster.

`DatabricksAccount` `Ready` does not mean the operator can create anything —
verification is a read. If it is `Ready` and an identity says `Denied`, the
operator is short of a permission in Databricks, and no retry supplies it.

## The reasons on `Ready`

`Ready` on an identity carries more than one kind of answer, and the reason says
which. These strings are API surface: people match on them and alert on them, so
they are reproduced here exactly as the operator writes them.

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

Three of those say a state rather than a failure — `AwaitingServicePrincipal`,
`CreateUnconfirmed`, `MintingNotSuspended` — and they are reported rather than
left blank so that somebody waiting sees something other than silence. The
distinction is worth keeping when you write an alert: `CreateUnconfirmed` and
`MintingNotSuspended` have concluded nothing and asked Databricks for nothing,
and neither is an identity that is known to be broken.
