# Security

This operator holds an account admin credential for a Databricks account and
decides which workloads get an identity in it. Both of those make it worth
looking at closely, and this page is for whoever is doing the looking.

## Reporting a vulnerability

**[Open a private advisory][report].** It is GitHub's private form on this
repository: only the maintainers see it, and the thread stays there until it is
resolved. No mail address here, because one that is published is one that is
scraped, and a report that arrives in an inbox has nowhere to be discussed.

Please do not open a public issue for anything you believe is exploitable.
Everything else — a hardening idea, a question about the model below, something
that reads wrong — is an ordinary issue and is welcome as one.

What helps most, in the report:

- The version, from `kubectl -n <operator namespace> get deploy -o jsonpath='{..image}'`.
- What an attacker starts with. This design is mostly about who may write what,
  so which of the three permissions in [Three people have to say yes][says-yes]
  they hold is usually the whole question.
- What they end up with in Databricks, and what stopped it from being nothing.

We will acknowledge a report within a week and say what we think of it — whether
it is a vulnerability, and what we intend to do — rather than leaving it open
without an answer. If it is one, the fix goes out as a release with an advisory
naming what was reachable and from where. Credit as you prefer, including none.

## Supported versions

The latest release, and only that one. This is `0.x` and nothing is backported:
a fix is a new release, and upgrading to it is the way to get it. The chart
version and the operator version are the same number, so there is no pairing to
work out ([the chart][chart]).

That is a statement about what happens, not a stance about what should. It
changes at 1.0.

## The model this operator rests on

Three things below look like findings and are not. They are the design, they are
argued at length in the pages linked from each, and a report that one of them is
possible is one we will close with a link. What we very much want to hear is a
way past one of them.

### The operator's credential is an account admin

It has to be. Databricks documents its federation policy API as account admin
only, so an operator that writes those policies holds a credential that can do
anything in the account ([Installing the operator][installing]).

**So the blast radius of the operator's own pod is the whole Databricks
account.** Anyone who can run code in it, exec into it, read its projected token,
or write objects in its namespace can mint identities there. That is the thing to
protect: the operator's namespace is as sensitive as the account it acts in, and
should be treated like any other namespace holding an admin credential.

What it is *not* is a credential sitting anywhere. The operator authenticates by
exchanging its own projected ServiceAccount token, so there is no secret in the
cluster to steal and none in this repository to leak.

A report that a namespace-admin on the operator's namespace can escalate into
Databricks is this paragraph. A report that somebody *outside* that namespace can
is a vulnerability, and is exactly what we want.

### The webhook fails open, and that is the safe direction

Both admission webhooks are `failurePolicy: Ignore`. Read on its own that looks
like a hole, so here is what fails.

The mutating webhook equips a pod with its token. Ignored, the pod starts holding
nothing — the workload cannot reach Databricks, which is a broken deployment and
not an escalation. The validating webhook refuses a request that names an account
which does not serve that namespace. Ignored, the annotation is written and then
nothing acts on it: **admission is not where the decision is made.** The operator
reads the Namespace label and the account's own `spec.namespaces` on every pass,
and an identity exists only when all three of the permissions in [Three people
have to say yes][says-yes] are there, whether or not any webhook ran.

The alternative — `failurePolicy: Fail` — makes a certificate rotation or a
crashed operator into a cluster-wide outage of pod creation, in namespaces that
have nothing to do with Databricks. That is the larger failure, and it is why
this one is `Ignore`.

### An identity is not a permission

This operator creates service principals and writes federation policies. It
grants them nothing: no group, no workspace assignment, no privilege on anything
in Unity Catalog. A workload that obtains an identity it should not have has
obtained an empty one, and somebody still has to grant it something before it can
read a byte.

That boundary is the load-bearing part of the design rather than an unfinished
half, and [Issuing an identity, not a permission][not-permission] is the whole
argument. A report that ends at "the workload got an identity" should say what
the identity could then do.

### What is in scope

Anything that gets past one of those three gates, or past the boundaries the rest
of the design claims:

- An identity issued to a ServiceAccount whose namespace has no `mint` label, or
  which the account's `spec.namespaces` does not name.
- A token accepted for an identity it was not issued for — one workload
  exchanging for another's service principal, or a recreated ServiceAccount
  inheriting the identity of the one it replaced.
- A service principal this operator created that outlives what asked for it: not
  destroyed when its ServiceAccount is deleted, its annotation removed, or its
  namespace taken off the account's list.
- One operator acting on another operator's identities, or in an account it was
  not told to act in ([Several operators in one cluster][several]).
- Anything that reads the operator's token, or exchanges it, from outside its own
  pod.
- A credential of any kind written to a log, a condition, an event, or an object.

[report]: https://github.com/workload-identity/databricks-service-principal-operator/security/advisories/new
[says-yes]: docs/who-says-yes.md
[not-permission]: docs/identity-not-permission.md
[installing]: docs/installing.md
[several]: docs/several-operators.md
[chart]: charts/databricks-service-principal-operator/README.md
