{{/*
The name the namespaced objects of one release are built from.

The release name alone, and not Helm's usual "<release>-<chart>": this chart's
name is 37 characters, a Service name has 63, and the longest suffix appended
below is "-webhook-service" -- so the conventional helper produces a Service the
API server refuses for every release name longer than ten characters.

Refused rather than truncated for the same reason. Truncating to fit is how two
releases whose names agree in their first 47 characters end up sharing a
webhook Service, and what follows is one operator's API server calls landing on
the other's pods.
*/}}
{{- define "dbxsp.fullname" -}}
{{- $name := default .Release.Name .Values.fullnameOverride -}}
{{- if gt (len $name) 47 -}}
{{- fail (printf "the release name %q is %d characters; a Service name is one DNS label and holds 63, of which this chart appends 16 (\"-webhook-service\"), so 47 is what is left. Install under a shorter release name, or set fullnameOverride." $name (len $name)) -}}
{{- end -}}
{{- $name -}}
{{- end -}}

{{/*
Where every object goes. The release's namespace unless something says
otherwise, so that `helm install --namespace` and what gets installed cannot
disagree.
*/}}
{{- define "dbxsp.namespace" -}}
{{- default .Release.Namespace .Values.namespace -}}
{{- end -}}

{{/*
What every cluster-scoped name in this release starts with.

Two operators on one cluster is a supported shape -- one per Databricks account
-- and cluster-scoped objects are the only ones nothing else keeps apart. So the
namespace is in the name as well as the release: Helm releases are namespaced,
so two people may each hold a release called "operator", and their ClusterRoles
would then be one object that the second install adopts and the first uninstall
deletes.

The 47-character budget above does not apply here. A ClusterRole, a
ClusterRoleBinding and a webhook configuration each get a full DNS subdomain,
which is 253.
*/}}
{{- define "dbxsp.clusterPrefix" -}}
{{- printf "%s-%s" (include "dbxsp.fullname" .) (include "dbxsp.namespace" .) -}}
{{- end -}}

{{- define "dbxsp.serviceAccountName" -}}
{{- default (printf "%s-controller-manager" (include "dbxsp.fullname" .)) .Values.serviceAccount.name -}}
{{- end -}}

{{/*
The labels a Service selects on and the Deployment matches on. The instance is
in there because two operators in one cluster run the same image under the same
name, and a Service that reached the wrong one would send the API server's
webhook calls to an operator serving a different Databricks account.
*/}}
{{- define "dbxsp.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
control-plane: controller-manager
{{- end -}}

{{/*
What every object carries. control-plane is not in here: it says "this is the
manager's pod", and a ClusterRole wearing it would answer a selector nobody
meant it to answer.
*/}}
{{- define "dbxsp.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
