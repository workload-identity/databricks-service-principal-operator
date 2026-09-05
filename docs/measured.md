# What was measured

For somebody deciding whether to adopt this operator: every claim it rests on,
what was asked, what came back, and what follows from it.

None of this is read off the SDK's type definitions. Each entry was established
against a live Databricks account, because the questions are ones only Databricks
can answer. Several have an executable form in `internal/databricks/live_test.go`
and `test/e2e/e2e_suite_test.go`; the rest were measured by hand and are recorded
here because they are recorded nowhere else.

## How to repeat any of this

No cluster deployment and no pod is needed. A ServiceAccount token minted by

```sh
kubectl create token <sa> -n <ns> --audience <the audience>
```

is exactly what a projected volume carries into a pod — same issuer, same
subject, and the audience you name. The exchange is a plain POST to
`<host>/oidc/accounts/<account id>/v1/token` from anywhere with network access,
so a shell and `curl` are the whole instrument. The endpoint is the one named by
`token_endpoint` at
`<host>/oidc/accounts/<account id>/.well-known/oauth-authorization-server`.

The live Go tests carry the `databricks` build tag and are not compiled without
it, so a contributor with no Databricks account never sees them fail — there is
nothing to skip and nothing to explain. With the tag, a missing coordinate is a
failure rather than a silent pass:

```sh
DATABRICKS_HOST=... DATABRICKS_ACCOUNT_ID=... make test-databricks
```

Both are the SDK's own names, so an environment already set up for the CLI or the
SDK runs them without translation. Nothing about any account is in this
repository.

## The token exchange

**It is RFC 8693 token exchange, not `private_key_jwt`.** The shape the docs for
OAuth client authentication lead you to — `grant_type=client_credentials` with a
`client_assertion` — is refused with *"Invalid private key JWT authentication:
The client identifier doesn't match the client assertion subject"*. That message
reads as though the client id were wrong, and it is not: the grant type is. What
works:

```
grant_type         = urn:ietf:params:oauth:grant-type:token-exchange
subject_token_type = urn:ietf:params:oauth:token-type:jwt
subject_token      = <the Kubernetes token>
client_id          = <applicationId>
scope              = all-apis
```

Anybody debugging a workload's first exchange will lose a day to that error
message otherwise, because everything it names is correct.

**An unset audience is the account id, not "no audience".** The SDK falls back to
the account id when `DATABRICKS_TOKEN_AUDIENCE` is empty. So a federation policy
written for the audience `databricks` cannot be satisfied by a workload that
never set one — the two sides disagree and neither says so. The operator sets the
policy's audience and the workload's from one place, so they agree by
construction; anybody writing a policy by hand has to decide which of the two
they are matching. See [what the pod gets](what-the-pod-gets.md).

**The audience is enforced by name, and the one a pod gets by default is refused
by name.** One ServiceAccount's token was exchanged three times, changing only
the audience it was minted for. The audience the policy names is exchanged; any
other is refused with `invalid_grant` and `TOKEN_AUDIENCE_INVALID (Token audience
'<what was sent>' invalid)`, and the message goes on to name the issuer and
audience the policy will accept. `https://kubernetes.default.svc` — what kubelet
mints when a projected volume asks for no audience — is refused like any other.
That is the measurement behind the claim that the token every pod in the cluster
already carries can never be exchanged for anything.

**The two ways a workload fails here give different messages.** A missing
`client_id` gives `TOKEN_INVALID (Ensure a valid federation policy has been
configured)`; a wrong audience gives `TOKEN_AUDIENCE_INVALID`. Worth knowing
before writing anything that reads them, and worth knowing before reading one at
3am — see [when something is wrong](when-something-is-wrong.md).

**An exchange with no `client_id` is refused even when a policy matching the
token exists.** The per-service-principal policies are not searched by subject.
The request says which service principal it wants, and the policy on that one is
what is checked. Everything under [federation policies](#federation-policies)
about two service principals trusting one subject follows from this: there is
nothing for Databricks to disambiguate.

**A workload that holds nothing is refused with 403, not 401.** The exchanged
token is a real, authenticated identity from the first moment; it simply has no
permissions. This is the operator's promise made observable — whoever debugs a
workload tells "the token is wrong" from "nobody granted this anything" by the
status code alone, without asking anyone. It is also the reason
[issuing an identity is not granting a permission](identity-not-permission.md) is
a claim you can check rather than one you have to believe.

## Federation policies

**Databricks fetches the issuer's OpenID configuration when the policy is
written, not when a token is first exchanged.** An unreachable issuer is refused
outright: *"Unable to load valid OpenID configuration for issuer"*. So a cluster
whose issuer is not published fails on its first reconcile, one pass in, rather
than silently later at the first exchange. For anybody evaluating this, that is a
prerequisite to check before adopting and not a failure mode to discover: the
cluster's issuer has to be publicly resolvable from Databricks. See
[installing](installing.md).

**Writing the same policy twice leaves one policy and does not error.** The
controller makes this call on every pass, at least once a minute per identity
forever. If a repeat were an error, every converged identity would report itself
broken; if it were a second policy, the account would accumulate one per
reconcile.

**A second, different policy joins the first rather than replacing it.** The
operator never writes two, but it does not own the account — somebody can point
another cluster at a service principal this one made. A replace would have
silently ended the first workload's access.

**Two service principals may each hold a policy for one issuer, subject and
audience.** The second write is accepted, one token exchanges for either, and
which one comes back is decided entirely by the `client_id` in the request.
Deleting one takes only its own policy; the other goes on exchanging. This is
what makes recreating a ServiceAccount safe: the new pods are injected with the
new client id, the old identity being alive for a moment changes nothing, and the
two records need no ordering between them.

It also sharpens what an orphan is. Inheritance is not passive — a new occupant
does not fall into an old identity, they have to name it. But naming it is all it
takes, and the applicationId is not a secret: it is the value handed to whoever
grants permissions, so it lives in grant statements, group memberships and the
console. Deleting the orphan is the whole of the answer.

**A policy naming a subject nothing has ever created is accepted.** Databricks
checks nothing about the subject — a policy naming a namespace and a
ServiceAccount that have never existed is written and read back intact. Two
things follow. There is no ordering to get right, so a policy can be written
before any pod exists. And a policy goes on trusting a name after the
ServiceAccount is gone, which is why deleting the service principal has to be the
whole of revocation.

**Deleting a service principal takes its federation policies with it.** Asking
for them afterwards fails with *"Invalid service principal id"*, which is the
same answer from here. That is what makes deletion the whole of revocation, and
it is why [the three objects](the-three-objects.md) treats deleting the identity
as the only complete withdrawal.

**`policy_id` is chosen by the caller.** Documented as "if unspecified, the id
will be assigned by Databricks", constrained to lowercase alphanumeric, hyphens
and slashes, **2 to 63 characters** — and verified: seven creates under
caller-derived ids, none refused, every later read finding the policy under the
name it was given. A policy is scoped to its service principal — the resource is
`accounts/<a>/servicePrincipals/<sp>/federationPolicies/<id>` — which is what
lets two service principals each carry one for the same subject.

Account-level federation policies are a separate collection and cannot replace
these. They match the token's subject claim against a Databricks username, so
they assume the identity provider already knows Databricks names. A Kubernetes
token's subject is `system:serviceaccount:...`, fixed by Kubernetes, and a
service principal's identifier is assigned by Databricks. Neither side can be
chosen, so nothing matches. Only a per-service-principal policy can express an
arbitrary external subject mapping to a chosen identity.

## The SCIM API

**`externalId` is capped at 36 characters, and an oversized one creates the
service principal and then returns an error.** 36 accepted, 37 refused with
*"Azure object id cannot be over 36 characters"*. Six service principals were
left in a real account learning that, because the call reported failure after the
create had already happened. Anything that cleans up after a failed create has to
clean up by display name, not by the id it never received. It is also why the
federation policy rather than `externalId` carries the record of which cluster
and which subject an identity belongs to: a Kubernetes subject does not fit in 36
characters, and there is no other writable field.

**The listing filters on `displayName`; a filter on `externalId` is a 400.**
`filter=displayName eq "..."` answers on `/api/2.0`; the same filter on
`externalId` is refused outright, and `/api/2.1` refuses any parameter at all. So
a service principal can be found by the name this operator gives it and cannot be
found by anything written in `externalId`. The lookup is therefore filter, then
fall back: the display-name filter as the fast path, and when it matches nothing,
an unfiltered listing compared client-side. The expensive path runs only in the
case that would otherwise answer "not found" wrongly.

**The marker written at create comes back on a read and on a listing.** The
resource returns `active,applicationId,displayName,externalId,id,schemas`. An
identity created by a pass that then crashed is found again, which is the only
way it is ever deleted.

**`externalId` can be cleared, but only by a full PUT.**

| attempt                            | result                                  |
|------------------------------------|-----------------------------------------|
| `PATCH` replace `externalId`       | HTTP 400, refused                       |
| `PATCH` remove `externalId`        | **HTTP 200 and the value is unchanged** |
| `PUT` the resource back without it | HTTP 200, and `externalId` is gone      |

The middle row is the one to write down: the call reports success and does
nothing. Anything built on PATCH here would believe it had released something it
had not. A PUT replaces the whole resource, so everything meant to survive has to
be sent back with it — `applicationId` is server-owned and survived; `displayName`
and `active` were sent and survived.

**A display name may be 100 characters; 101 is refused before anything is
created.** Nothing is left behind — the opposite of `externalId` on the same
call, which creates and then refuses. Two fields, one request, validated in
different orders, so measuring one says nothing about the other. Assume nothing
about a third.

**Two service principals may share a display name.** Databricks enforces no
uniqueness on it. That is what makes truncating to 100 characters safe, and it is
also why a display name can never identify one — the marker is what tells two
apart when a namespace or a ServiceAccount has been recreated under the same
name.

**A create returns both the numeric id and the applicationId, and they are
different values.** The numeric id is what federation policies hang off and what
deletion takes; the applicationId is what the workload presents at token
exchange. Neither can be derived from the other, so the operator records both.

## Timing

**A write or a delete becomes visible to a read when it does, and how long that
takes varies by an order of magnitude.** Measured across two days on one account:
235ms, 245ms, 273ms, 1.5s and 2.2s — the longest and one of the shortest on the
same day — and **7.6s** for the same call with ten of these tests running at
once.

Two things follow, and the second is the one nobody measures.

An assertion about a presence polls rather than reading once. A test here read
once, passed for a day, then failed; what it had been measuring was the account's
mood rather than the thing it named, and for that day it had been reporting
something true, which is the worst version of it.

An assertion about an *absence* cannot be polled the same way — polling stops at
the first read that agrees, and for "no second policy was created" that is the
first read taken before the account caught up, which agrees for the wrong reason.
So an absence is asserted by reading it several times over and failing on the
first read that disagrees, which also says which round it flipped on. **A number
taken from a quiet run is three times too small for a busy one**, so the figure
that decides how long such an assertion has to hold for is the loaded one.

**A create is visible on the first lookup; a delete is not.** Three rounds
against the account: the new service principal was found on the first attempt,
868ms to 1.29s after the create began. The lag runs in one direction only, so an
argument that assumes a create might not be visible yet is arguing from a
measurement that was never taken.

## The SDK

All of this was measured with one Kubernetes token, one config file holding two
profiles, and one process authenticating twice. Each profile produced an access
token whose `sub` was its own applicationId, and both profiles named the same
token file. **One Kubernetes token is as many Databricks identities as there are
profiles** — see [several identities](several-identities.md).

Four things came out of doing it that the SDK's types do not say.

**A profile needs `auth_type = file-oidc`.** Without it the SDK refuses to
resolve at all, reporting *"more than one authorization method configured"*:
`client_id` alone marks the oauth family and `databricks_id_token_filepath` marks
file-oidc. It is a required line, not a redundant one.

**The ini names are not the environment names.** The token path is
`databricks_id_token_filepath` in a profile and `DATABRICKS_OIDC_TOKEN_FILEPATH`
in the environment. Nothing warns you when you use the wrong one.

**A profile name may contain a slash.** `[ops-a/databricks-account]` resolves,
which is what lets a profile be the operator's reference verbatim rather than
something transformed into it and back.

**Naming no profile is not a default.** With a config file present and no profile
named, the SDK falls through to its default credential search and fails; a
profile that does not exist fails by name. Neither falls back silently, so the
variable naming the profile is not optional.

And the one the whole injection design rests on:

**An environment variable beats a profile, silently.** With a profile selected
and `DATABRICKS_CLIENT_ID` set to another identity's, the SDK authenticated as
the environment's. Nothing reports a conflict.

That is why nothing this operator writes into a pod is a name the SDK reads.
Setting any such variable is not adding a value — it is overruling whatever the
image brought, silently, with no way for the workload to find out that it
happened. The reverse stays true and is worth checking in your own images: a
stray `DATABRICKS_CLIENT_ID` left by a base image, an old pod spec or another
operator silently decides which identity every profile in that pod resolves to.
See [what the pod gets](what-the-pod-gets.md).

**A pod annotation is projected into a file verbatim.** 417 bytes written, 417
bytes read, newlines and blank lines intact, through a downwardAPI volume
selecting `metadata.annotations['...']`. That is the delivery mechanism for a
config file the operator renders, and it needs no object in the tenant's
namespace to hold it.

## Still not established

Written down as unverified rather than left out, because an unasked question
looks exactly like an answered one once it is missing from the page.

**What Databricks answers for a deactivated service principal.** Not measured.
Whether a deactivated one is absent, present-and-inactive, or something else
decides what the operator concludes when it reads one back, and nothing here
tells you.

**What a create carrying a `policy_id` that already exists returns.** Not
documented and not measured. The code assumes a 409 mapped to the SDK's conflict
error and fails loudly if that is wrong. A green suite is not evidence: across
the runs made so far, not one conflict occurred, because the read always answered
first. Only a deliberate probe of two creates under one id settles it, and until
somebody makes one it will first fire on a race.

## Where these live in the repository

- `internal/databricks/live_test.go` — the live Go tests, behind the `databricks`
  build tag. Federation policy behaviour, the marker round-trip, deletion, and
  shared display names.
- `test/e2e/e2e_suite_test.go` — the exchange itself, run from inside a pod
  against a real account, including the request shape above.
- `internal/webhook/profiles.go` and its tests — the profile rendering that the
  SDK findings above are the reason for.
