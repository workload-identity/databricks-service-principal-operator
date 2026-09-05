# What the namespace list means

For whoever holds the Databricks account and is about to add a namespace to
`spec.namespaces`, or — and this is the one to read first — about to take one
off.

`spec.namespaces` on the `DatabricksAccount` is the account holder's answer to
where their account admin credential may be spent, and it is theirs by where it
lives rather than by convention:
[Three people have to say yes](who-says-yes.md#the-account-holder-i-will-spend-my-credential-here).
This is what the list actually promises once it is written.

## The three states, and what the account does in each

A namespace is in exactly one of three states with respect to one account, and
the third is not the first:

| The namespace is   | This account                             |
|--------------------|------------------------------------------|
| on the list        | issues identities there and manages them |
| taken off the list | destroys every identity it issued there  |
| never on the list  | has nothing there                        |

"Never on the list" and "taken off the list" look the same from inside the
namespace — no identities either way — and they are entirely different acts. One
is an account that was never asked; the other is an account withdrawing, which
means deleting what it made.

The list is short and each line of it is a decision, which buys one thing a
person can check: **every service principal carrying this operator's marker is in
a namespace on the list.** That is a sentence you can go and verify in the
Databricks console against a list you can read in one screen, and it is the
reason the list is names rather than a selector.

## Taking a namespace off the list destroys the identities in it

**This is destructive and it cannot be undone from here.** Every service
principal this account issued in that namespace is deleted in Databricks, every
grant anybody made on one goes with it, and the records go. Emptying the list
does it for every namespace at once.

**Naming the namespace again is not a restore.** It issues new service
principals, with new client ids and no grants. Everything that was granted has to
be granted again, by whoever granted it — and that is somebody else, holding a
different credential, who did not make this edit and will not know it happened
until something of theirs stops working.

So the list is not a routing table to be tidied. Removing a line is the same kind
of act as deleting the namespace, and the person making it is usually not the
person who will pay for it.

It is also not the only way an identity is destroyed, only the one that is not
the asking team's to do. The ordinary way is the team that asked removing the key
that asked: [Asking for an identity](asking-for-an-identity.md).

## The promise is eventual

The destroying is done against Databricks, so while Databricks cannot be reached,
the namespace is off the list and the identities are still there. The list says
what will be true; it does not by itself say that it already is.

`IdentitiesDestroyed` on the `DatabricksAccount` is what says how far from kept
the promise currently is — how many identities are left and in which namespaces:

```sh
kubectl -n dbxsp-operator-system get databricksaccount databricks-account \
  -o jsonpath='{.status.conditions[?(@.type=="IdentitiesDestroyed")].message}'
```

That condition, rather than the absence of the namespace from the list, is what
to read before telling anybody the account no longer reaches that namespace.

## While it is destroying, minting there is suspended

The set being destroyed has to stop growing, or a namespace that was removed
could go on minting into the account it was removed from for as long as the
destruction took.

So the operator writes `databricks.workload-identity.io/destroying-identities` on
the Namespace, naming itself in the value, and takes the key off again when it
has finished. It is the operator's own bookkeeping, held for the length of one
job — not a label a person sets, and not one a person has to clear.

It is a different key from `mint` on purpose, and the reason is what makes it
safe where two operators serve one namespace.
[`mint` is the cluster's permission](who-says-yes.md#the-cluster-admin-this-namespace-may-be-served),
given to every operator there at once, so an operator that removed it to stop its
own minting would be revoking somebody else's — permanently, since no operator
may write it back. This key is suspension instead: while it is held, minting in
that namespace stops for everybody, and when the operator that took it lifts it,
everybody's minting resumes with nothing for a person to do. That the operator
lifts it even though the namespace is still off its own list is the point — what
stops it minting there is the list, and holding the key any longer would only
stop everybody else.

Naming the holder in the value is what lets a second operator say who has it, and
it is one key rather than one per operator so that only one destruction runs in a
namespace at a time. The value spells the holder
`<the operator's namespace>.<its DatabricksAccount's name>`, with a `.` where
every other reference to an operator uses a `/`, because a label value cannot
hold `/`. A companion annotation records when the holder last said it was still
there, which makes the key a lease: an operator killed partway through does not
leave a namespace unable to mint for ever.

[Several operators in one cluster](several-operators.md) is the rest of what two
operators in one cluster do to each other.
