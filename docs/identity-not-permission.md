# Issuing an identity, not a permission

For somebody deciding whether to adopt this operator: what the boundary is, why
it is the load-bearing part of the design rather than an unfinished half, and
what it costs.

This operator creates identities. It grants them nothing, and it will not — not a
group membership, not a workspace assignment, not a permission on anything in
Unity Catalog. The most natural suggestion anybody makes about this design walks
straight through that boundary, and it is a good suggestion until you look at
what the call actually confers. This records the argument, so that it does not
have to be re-derived every time somebody arrives at it.

## The suggestion

A workload needs a Databricks identity. There are two ways to hand it one.

- Create a service principal for that workload and write the federation policy
  that makes its token exchangeable for it. This is what happens today.
- Write a federation policy on a service principal that already exists — one a
  person created and granted things to.

The second is cheaper on every axis you can see from here. The operator creates
nothing, so it can orphan nothing. The identity works the moment the policy
lands, rather than arriving empty and waiting for somebody. The applicationId
belongs to something that outlives any one workload, so a ServiceAccount
recreated under a new uid does not strand every grant made to its predecessor.
It even declines to decide granularity: whether one service principal serves one
workload or twenty is left to whoever made it.

## Why it is not done

Permissions attach to a service principal — Unity Catalog privileges, workspace
entitlements, cluster policies, group membership. Nothing attaches to a
federation policy. A policy is an admission rule: it names an issuer, a subject
and an audience, and holds nothing else.

So one call means two different things depending on what it is pointed at.
Written on a service principal made a moment ago, it admits a subject to nothing.
Written on one that has been granted things, it admits that subject to all of
them, at once. The request is identical. What it confers is not.

**The second is granting a permission, whatever it is called.** That is precisely
the act this design is built to keep out of the operator, and calling it a
federation policy does not change who ends up able to read which table.

## What rests on that

Three things do. Two of them fail outright under the suggestion, and the third
stops being cheap.

**It is the only thing about this operator the account can check.** The
operator's own service principal has to be an account admin, because writing a
federation policy allows nothing narrower — [who says
yes](who-says-yes.md) spends a while on why. An account admin can grant
Databricks permissions, so the account cannot rule out that this one does by
reading its permissions. The only thing left to read is the code, and today that
reading is short, because there is no call in it that grants anything. The moment
one identity gets a group membership from here, the answer becomes "it grants
only what it should", which is a claim about a running program rather than a fact
about a file — and the account admin the operator already paid for buys nothing.

The suggestion is exactly that moment. Writing policies onto service principals
that already hold permissions is a call that confers permissions, and the short
reading is over.

**It is what makes `mint` safe to hand out.** Annotating a ServiceAccount is a
namespace-level act: whoever can write ServiceAccounts in a namespace can mint an
identity there — see [asking for an identity](asking-for-an-identity.md). That is
acceptable only because a minted identity holds nothing. If minting also granted,
write access to a namespace would become a way to acquire Databricks permissions,
by a route nobody in Databricks reviews.

The suggestion makes minting grant. Asking would attach a subject to a service
principal somebody had already granted things to, and the namespace would be
where that decision was made.

**Every permission in the account has a single origin.** "Who can read this
table" is answered in Databricks, by reading what has been granted there, rather
than reconstructed from annotations anyone holding a namespace can change. Under
the suggestion the grants stop being the whole answer: each one would also have
to be read together with the federation policies hanging off the service
principal that holds it, and each of those policies is written from a namespace.
The question stays answerable and stops being cheap, which in practice means it
stops being asked.

## The empty identity is the gate

An identity that can do nothing on the day it is issued reads like an unfinished
half. It is the opposite: it is what makes the issuing safe to automate at all.
The decision about what a workload may do is deferred to somebody holding a
different credential, and until they act, nothing has been decided and nothing is
at risk.

It is also legible from the workload's side rather than only from the account's.
A workload holding an identity nobody has granted anything is refused with 403
and not 401: it is authenticated, and it holds nothing. So "the token is wrong"
and "nobody has granted this anything yet" are two different failures with two
different status codes, and whoever is debugging reads which one it is instead of
being told — see [what was measured](measured.md).

That deferral does not have to be expensive. Put the permissions on a group and
review them once; from then on each new workload is a membership decision against
a set that has already been reviewed, rather than a permission set designed from
scratch. The operator never touches the group, and the credential that adds a
member needs none of the account admin the operator holds. The account-level
create request carries no group and no entitlement, so this is not a discipline
the operator has to keep — it is a call it cannot make.

## What the alternative would have forced

The suggestion also moves when review can happen, and this is the part that is
easiest to miss.

Today the review is asynchronous because the unreviewed state is inert. A service
principal exists, holds nothing, and waits. Whoever decides what it may do can
take an hour or a week, and nothing is exposed in the meantime.

Attaching subjects to already-permissioned service principals has no inert state.
The subject holds everything the moment the policy is written, so the review has
to have closed before the write. Two shapes follow and neither is good. Either
the annotation is what triggers the write, and review must therefore already be
complete before a tenant writes an annotation the operator does not control — or
the operator holds the request pending until somebody with the account's
authority records that this subject may join that service principal, which puts a
person inside the reconcile loop and makes a queue of pending human decisions
part of the operator's correctness.

The first is a back door. The second is a treadmill on the critical path: one
decision per workload, blocking, which is the cost the suggestion was supposed to
remove.

## What the boundary costs

It is worth stating plainly rather than defending. Every identity this operator
issues arrives useless, and somebody has to act before it is worth anything. That
step is per workload and it does not go away.

What the operator removes is the distribution and rotation of a credential, which
is toil. What it leaves is the decision about how much a workload may reach,
which is a judgement. Automating the first and declining the second is the whole
of the split.

The operator's answer stops at `status.clientID`, which names the identity and
nothing else. Putting that identity in a group is somebody else's work, done with
somebody else's credential — one that needs none of the account admin this
operator holds. Whether that somebody is Terraform, a person, or another
controller is a governance decision, and not this operator's to make.
