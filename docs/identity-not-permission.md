# Issuing an identity, not a permission

The README states the boundary: this operator creates identities, grants them
nothing, and will not — [What it will not do](../README.md#what-it-will-not-do).
The most natural suggestion anybody makes about this design walks straight
through that boundary, and it is a good suggestion until you look at what the
call actually confers. This records the argument, so that it does not have to be
re-derived every time somebody arrives at it.

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

The README gives the two reasons the boundary is load-bearing rather than tidy:
it is the only claim about this operator the account can check, and it is what
makes handing out `mint` safe. Both fail under the suggestion.

The first fails because the check is a reading of the code. This operator's own
service principal has to be an account admin — writing a federation policy allows
nothing narrower — so an account cannot rule out that it grants things by reading
its permissions. What is left to read is the source, and today that reading is
short: no call in it confers a permission on anything. Writing policies onto
service principals that already hold permissions is a call that confers
permissions, and the short reading is over.

The second fails because annotating a ServiceAccount is a namespace-level act.
Whoever can write ServiceAccounts in a namespace can ask for an identity there.
That is safe only while a newly issued identity holds nothing. If asking could
attach a subject to a service principal somebody had already granted things to,
then write access to a namespace would be a way to acquire Databricks
permissions, by a route nobody in Databricks reviews.

## The empty identity is the gate

An identity that can do nothing on the day it is issued reads like an unfinished
half. It is the opposite: it is what makes the issuing safe to automate at all.
The decision about what a workload may do is deferred to somebody holding a
different credential, and until they act, nothing has been decided and nothing is
at risk.

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
