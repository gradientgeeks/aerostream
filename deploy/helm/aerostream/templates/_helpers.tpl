{{/* Chart name, truncated for DNS labels. */}}
{{- define "aerostream.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Fully qualified app name. */}}
{{- define "aerostream.fullname" -}}
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

{{- define "aerostream.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Labels shared by every object. */}}
{{- define "aerostream.labels" -}}
helm.sh/chart: {{ include "aerostream.chart" . }}
app.kubernetes.io/name: {{ include "aerostream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: aerostream
{{- end }}

{{/* Selector labels for a component: include "aerostream.selectorLabels" (dict "ctx" . "component" "broker") */}}
{{- define "aerostream.selectorLabels" -}}
app.kubernetes.io/name: {{ include "aerostream.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "aerostream.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}

{{- define "aerostream.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "aerostream.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Names of the Services. */}}
{{- define "aerostream.controller.headless" -}}{{ include "aerostream.fullname" . }}-controller-headless{{- end }}
{{- define "aerostream.controller.service" -}}{{ include "aerostream.fullname" . }}-controller{{- end }}
{{- define "aerostream.broker.headless" -}}{{ include "aerostream.fullname" . }}-broker-headless{{- end }}
{{- define "aerostream.broker.service" -}}{{ include "aerostream.fullname" . }}-broker{{- end }}
{{- define "aerostream.configSecret" -}}{{ include "aerostream.fullname" . }}-config{{- end }}

{{/* Preset pod anti-affinity for a component (soft = prefer spreading, hard = require). */}}
{{- define "aerostream.podAntiAffinity" -}}
{{- $preset := .preset -}}
{{- if eq $preset "hard" }}
podAntiAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    - topologyKey: kubernetes.io/hostname
      labelSelector:
        matchLabels:
          {{- include "aerostream.selectorLabels" (dict "ctx" .ctx "component" .component) | nindent 10 }}
{{- else if eq $preset "soft" }}
podAntiAffinity:
  preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels:
            {{- include "aerostream.selectorLabels" (dict "ctx" .ctx "component" .component) | nindent 12 }}
{{- end }}
{{- end }}

{{/* Integer rendering that survives YAML float64 parsing of large numbers. */}}
{{- define "aerostream.int" -}}{{ printf "%d" (int64 .) }}{{- end }}
