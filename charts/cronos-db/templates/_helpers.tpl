{{/*
Expand the name of the chart.
*/}}
{{- define "cronos-db.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "cronos-db.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "cronos-db.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "cronos-db.labels" -}}
helm.sh/chart: {{ include "cronos-db.chart" . }}
{{ include "cronos-db.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "cronos-db.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cronos-db.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "cronos-db.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "cronos-db.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Headless service name used for stable pod DNS.
*/}}
{{- define "cronos-db.headlessServiceName" -}}
{{- printf "%s-headless" (include "cronos-db.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Cluster seeds based on StatefulSet pod names and headless service.
*/}}
{{- define "cronos-db.clusterSeeds" -}}
{{- $fullname := include "cronos-db.fullname" . }}
{{- $headless := include "cronos-db.headlessServiceName" . }}
{{- $namespace := .Release.Namespace }}
{{- $count := int .Values.replicaCount }}
{{- $seeds := list }}
{{- range $i := until $count }}
{{- $seeds = append $seeds (printf "%s-%d.%s.%s.svc.cluster.local:7946" $fullname $i $headless $namespace) }}
{{- end }}
{{- join "," $seeds }}
{{- end }}
