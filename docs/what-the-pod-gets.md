# What the pod gets, and the one line to use it

For whoever writes the workload — what an equipped pod comes up holding, and the
one line of it your code has to name.

Asking for the identity is the ServiceAccount annotation, and it is
[Asking for an identity](asking-for-an-identity.md). This page starts after that:
the identity exists, the pod is equipped, and something in the pod has to use it.

## What your workload writes

One line, and this operator sets nothing else.

```sh
export DATABRICKS_CONFIG_FILE=$WORKLOAD_IDENTITY_DATABRICKS_CONFIG_FILE
export DATABRICKS_CONFIG_PROFILE=$WORKLOAD_IDENTITY_DATABRICKS_CONFIG_PROFILE
```

or in code, naming them yourself:

```python
import os
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(
    config_file=os.environ["WORKLOAD_IDENTITY_DATABRICKS_CONFIG_FILE"],
    profile=os.environ["WORKLOAD_IDENTITY_DATABRICKS_CONFIG_PROFILE"],
)
```

`WORKLOAD_IDENTITY_DATABRICKS_CONFIG_PROFILE` names the identity your
ServiceAccount asked for with the bare key — the ordinary case, one workload and
one identity. A workload holding several picks between them by name:
[Several identities for one ServiceAccount](several-identities.md).

## Why you have to write that line

**Nothing this operator sets is a name any Databricks SDK reads.** Setting
`DATABRICKS_CONFIG_FILE` would not add a value to your workload — it would take
over the SDK's whole resolution. A workload carrying its own `.databrickscfg`,
baked into its image or mounted, would find that file still there, untouched, and
never read again. An environment variable beats a profile silently, so nothing
inside the workload could find out, and this operator cannot see what an image
contains.

So it publishes instead of configuring, and the act of using what was published
is yours. That is the price of never displacing anything you brought.

## The workspace is yours to name

The annotation names an operator and nothing else — no workspace. Your workload
names its own, in its own pod spec, with the SDK's own variable:

```yaml
env:
- name: DATABRICKS_HOST
  value: https://dbc-example.cloud.databricks.com
```

That composes with what this operator gives you rather than replacing it —
measured against the SDK: the profile supplies `auth_type`, `client_id` and the
token path, and `DATABRICKS_HOST` supplies the host.

It is not here because this operator issues identities, and a host is a
destination. It could not check one if it carried it: it knows the Databricks
account it acts in, and a workload wants a workspace. Carrying it would be
repeating a string nobody verifies, in an object that is not where the rest of
your configuration lives.

A workload holding several identities that reach different workspaces has one
more thing to arrange, because `DATABRICKS_HOST` is one value:
[Which workspace each one reaches](several-identities.md#which-workspace-each-one-reaches).

## What is actually in the pod

```
/var/run/secrets/databricks/
├── config
└── dbxsp-operator-system/databricks-account/token
```

`config` is the SDK's own format. The one identity asked for with the bare key
has no name of its own, so its profile is named by what that key held — the
operator:

```ini
[dbxsp-operator-system/databricks-account]
auth_type = file-oidc
client_id = 11111111-1111-1111-1111-111111111111
databricks_id_token_filepath = /var/run/secrets/databricks/dbxsp-operator-system/databricks-account/token
oidc_token_filepath = /var/run/secrets/databricks/dbxsp-operator-system/databricks-account/token
audience = databricks
token_audience = databricks
```

**Two values are there twice, and both spellings are yours to ignore.** The SDKs
do not agree on what to call them in a profile — Go reads
`databricks_id_token_filepath` and `audience`, Python reads `oidc_token_filepath`
and `token_audience` — and this operator cannot see which one your image
contains, so it writes both. Whichever SDK you use reads the pair it knows and
drops the other without a word. Their environment variables do agree, which is
why the split only shows here.

An identity asked for under a named key is named by that name instead, and its
profile is `[reader]`:
[Several identities for one ServiceAccount](several-identities.md).

**The tokens expire and are replaced, and that is not your problem.** kubelet
rewrites each file well before its token expires, and the SDK re-reads the file
on every exchange. Point it at the path and it stays working; nothing has to be
restarted, and nothing has to notice.

## If a pod comes up with none of this

A pod is equipped when it is created, so a pod that was already running when the
identity was asked for holds nothing until it is recreated — the `Equipped`
condition names those pods:
[Recreate pods that were already running](asking-for-an-identity.md#recreate-pods-that-were-already-running).
Anything else is [When something is wrong](when-something-is-wrong.md).
