# The three objects

For anybody who has met one of these three custom resources and wants to know
what it is before touching it.

They are not three views of one thing. One is configuration, one is a record,
and one is a copy, and what deleting each of them costs differs by orders of
magnitude. So this page asks the same questions of all three, in the same order,
and the differences are read off rather than inferred.

| Kind                               | Short     | Lives in                 | Written by                              |
|------------------------------------|-----------|--------------------------|-----------------------------------------|
| `DatabricksAccount`                | `dbxacc`  | the operator's namespace | the team holding the Databricks account |
| `IssuedDatabricksServicePrincipal` | `isdbxsp` | the operator's namespace | the operator, and nothing else          |
| `DatabricksServiceAccount`         | `dbxsa`   | the workload's namespace | the operator, and nothing else          |

## The sentence the rest of this page hangs on

The service principal in Databricks cannot be rebuilt. Asking for another one
produces a different `applicationId`, so every grant anybody made has to be made
again. The record is the only thing that remembers it — which is why the record's
lifetime has to contain the service principal's, and why deleting a record
destroys what it names.

That is one object out of the three. The opposite of it is what makes `dbxsa`
safe to touch: it is a projection and never evidence. Delete it and the next pass
rebuilds it; write to it and the next pass overwrites you. Nothing in Databricks
depends on it existing, because nothing on it was ever the source of anything.

So the question to ask before editing any of these is not what a field means. It
is whether this object is the memory of something outside the cluster. For
exactly one of the three the answer is yes.

### Two things about the record that nothing else tells you

**It is named after the ServiceAccount's uid, not its name.** A federation policy
in Databricks matches `system:serviceaccount:<namespace>:<name>`, and names get
reused: a ServiceAccount deleted and recreated under the same name answers to the
same subject while being a different thing. Naming the record after the uid is
what makes those two different records, so the new ServiceAccount is issued a new
identity rather than inheriting one somebody granted things to while the old one
held it. The same holds a level up — a namespace deleted and recreated under its
old name holds ServiceAccounts whose uids are all new.

**It lives in the operator's namespace so that tearing down a team's namespace
cannot lose it.** What is in Databricks outlives a Kubernetes namespace, so what
remembers it has to as well. A record kept beside the workload would be deleted
along with the namespace, in an order nothing specifies, and what survived would
be a service principal nothing in the cluster remembers — inherited in silence by
whoever next takes that namespace name.

---

## `DatabricksAccount` (`dbxacc`)

**What it is.** The operator's own configuration: which Databricks account it
acts in, as whom, and in which namespaces. Nothing on it is a secret and it is
deliberately not stored as one — a host is a URL, an account id is an identifier,
and `clientId` is the client id of an OAuth client that has no secret. What gates
the exchange is the federation policy in Databricks, so holding these three
values without being able to mint a token for that subject obtains nothing.

**Where it lives.** In the operator's own namespace, because editing it should
need the access that could already replace the operator's image.

**Who writes it.** The team that holds the Databricks account, once, at install
time — [docs/installing.md](installing.md). The operator serves the one named by
its `--databricks-account` flag; any other `DatabricksAccount` is left alone and
says on its own status that it was not selected, because silence there would be
indistinguishable from an operator that is not running.

**Deleting it.** Refused while any record in that namespace still names the
Databricks account it declares. Deleting this object and writing another naming a
different account is how `spec.accountId` would be changed after being made
immutable, and it is the same change with a longer handle. The refusal acts on
nothing: what would have to go is somebody else's identities in Databricks, and
this object is not where that decision is made. A person can remove the finalizer
by hand, and the object says so while it is held — an account that has become
unreachable can never drain, so a finalizer with no way out would be a trap
rather than a door.

**Editing it.** `spec.host` and `spec.accountId` are immutable and the API server
refuses the edit; a service principal id means nothing outside the account it was
made in, so one operator acts in one account for its whole life. `spec.namespaces`
is the field that is edited after install, and taking a namespace off it destroys
every identity this account issued there —
[docs/what-the-namespace-list-means.md](what-the-namespace-list-means.md).

**How long it lives.** As long as the operator does. A second Databricks account
is a second operator, not a second object here —
[docs/several-operators.md](several-operators.md).

**Fields somebody decides from.** `spec.namespaces` is the one anybody edits after
install. `status.subject` and `status.audience` are what the operator actually
presents when it exchanges its own token, and the federation policy written at
install must name those exact strings — they are reported precisely because they
cannot be derived from anything the installer holds, being assembled from a
namespace and a ServiceAccount name that kustomize rewrites. `Ready` and
`Prepared` are separate answers and both matter;
[docs/when-something-is-wrong.md](when-something-is-wrong.md) says why `Prepared`
is the one to alert on. `RequestsServed` is the third, and it is what the
`spec.namespaces` edit is made against: it names each namespace where a
ServiceAccount is asking this operator and this account does not serve, and how
many are asking there. It is the only place such a request is visible at all,
because nothing is written in the namespace it came from.

## `IssuedDatabricksServicePrincipal` (`isdbxsp`)

**What it is.** The operator's record that it created a service principal in
Databricks. It is the only one. Its spec is written once, when the record is
made, and never changed — a spec rather than a status because nothing recomputes
it and nothing reconciles towards it. It is what was true at the moment the
service principal was created, kept so that it can be compared against what is
true now.

**Where it lives.** In the operator's namespace, for the reason above: what it
records outlives the namespace that asked for it.

**Who writes it.** The operator, and nobody else. The name is derived and the
spec is immutable, so there is nothing here for a person to author.

**Deleting it.** This destroys the identity. That is what it means, and it is the
only object here that means it: the record carries a finalizer, and the finalizer
deletes the service principal in Databricks on the way out, with every grant
anybody made on it. The record is also not deleted by anything happening in a
team's namespace. It is asked, every pass, whether the ServiceAccount it was
issued to is still there, still asking, and still the same one; when the answer is
no it deletes itself. None of that depends on an event having been seen, on which
kind a namespace teardown deleted first, or on the operator having been running at
the time.

**Editing it.** The API server refuses. The spec records what was issued, and
there is no edit to it that could be true.

**How long it lives.** Longer than the service principal, always. That
containment is the design: a service principal that exists while nothing records
it is the one state this operator has no way back from, and every rule on this
object is there to keep the record's life wrapped around the service principal's
at both ends. The create is recorded before it is sent, so a pass that made one
and then died has already said where to look; the finalizer is held until the
delete is confirmed, and it is never dropped on a timeout, because releasing it
would be the operator deciding on the evidence of one failed call that an
unrecorded identity is acceptable.

**Fields somebody decides from.** `status.servicePrincipalId` is what you search
the Databricks audit log with, and it is never cleared — not even when Databricks
answers that the service principal is gone, because clearing it would let the
evidence of a removal destroy the evidence it was drawn from.
`status.servicePrincipalRemovedAt` is when that answer came, and the two together
bracket a service principal's life. `status.accountId` says which account to
search it in. `spec.serviceAccount.uid` is what every comparison is made on.

## `DatabricksServiceAccount` (`dbxsa`)

**What it is.** A copy, in the workload's own namespace, of what the operator
issued to one ServiceAccount. It decides nothing. It has no spec at all —
everything is already addressable, since the name and namespace say which
ServiceAccount and the annotation on that ServiceAccount is the request, so a
spec here would be new only in being able to disagree with its source.

**Where it lives.** Beside the workload, named after the ServiceAccount, one per
ServiceAccount however many identities it asked for. It exists because the record
cannot be here and the facts still have to be readable by whoever holds the
namespace — including why an identity is stuck, which is otherwise a question for
the platform team.

**Who writes it.** The operator. The operator never reads it back to decide
anything: the moment it did, this would stop being a copy and become a second
record, and two records disagree eventually with nobody noticing.

**Deleting it.** Nothing. It carries no finalizer and the next pass rebuilds it.

**Editing it.** Nothing. The next pass overwrites you, the way a status that was
scribbled on is overwritten.

**How long it lives.** As long as the ServiceAccount is asking for something. It
is derived, so its lifetime is a consequence rather than a decision anybody makes.

**Fields somebody decides from.** `status.identities` is one entry per identity,
keyed on `profile` — the name your code passes to the SDK. `clientId` is what
anything granting permissions to this workload names, and it cannot be derived,
because Databricks generates it and refuses to accept one. `issued` names the
record this entry was copied from, as `<operator namespace>/<record name>`, which
is how you reach a record whose name was derived from a uid and so cannot be
guessed. `operator` says whose entry it is, which is what keeps two operators from
undoing each other's work on one object —
[docs/several-identities.md](several-identities.md).

---

## Where the exhaustive list is

`kubectl explain`, and the CRDs under `config/crd/bases/`. Both are generated
from the types and both are complete:

```sh
kubectl explain isdbxsp.status
```

This page names only the fields somebody decides something from. Listing the rest
here would make a second copy of a generated file, and a second copy goes stale
with nothing failing to say so — the same failure this operator refuses when it
declines to read a `DatabricksServiceAccount` back.

## What is next to this

- [docs/who-says-yes.md](who-says-yes.md) — the three things that have to be true
  before any of these objects appears
- [docs/asking-for-an-identity.md](asking-for-an-identity.md) — how an identity is
  asked for, and what answers
- [docs/what-the-pod-gets.md](what-the-pod-gets.md) — what ends up in the pod
- [docs/identity-not-permission.md](identity-not-permission.md) — why an identity
  arrives holding nothing
- [docs/when-something-is-wrong.md](when-something-is-wrong.md) — start there when
  you have a symptom rather than an object
