{{- define "riquet.name" -}}{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}{{- end }}
{{- define "riquet.fullname" -}}{{- if .Values.fullnameOverride }}{{ .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}{{- else }}{{ printf "%s-%s" .Release.Name (include "riquet.name" .) | trunc 63 | trimSuffix "-" }}{{- end }}{{- end }}
{{- define "riquet.labels" -}}
app.kubernetes.io/name: {{ include "riquet.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end }}
{{- define "riquet.selectorLabels" -}}
app.kubernetes.io/name: {{ include "riquet.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
{{- define "riquet.internalTokenSecretName" -}}
{{- if .Values.auth.internalTokenSecret.name -}}
{{- .Values.auth.internalTokenSecret.name -}}
{{- else -}}
{{- printf "%s-internal" (include "riquet.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}
