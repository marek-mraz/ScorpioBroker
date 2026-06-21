# Helm Chart Patterns Reference

## Standard Chart Structure

### Full Production Chart (Always Use This)

```
mychart/
├── Chart.yaml
├── README.md               # Chart documentation
├── values.yaml
├── values.schema.json      # JSON Schema validation
├── .helmignore
├── templates/
│   ├── _helpers.tpl
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── ingress.yaml
│   ├── serviceaccount.yaml
│   ├── hpa.yaml
│   ├── pdb.yaml
│   ├── networkpolicy.yaml
│   ├── configmap.yaml
│   ├── secret.yaml
│   ├── pvc.yaml
│   ├── NOTES.txt
│   └── tests/
│       └── test-connection.yaml
└── charts/                  # Managed by helm dependency update
```

---

## _helpers.tpl — Standard Helpers

Every chart needs these. Copy and adapt.

```yaml
{{/*
Expand the name of the chart.
*/}}
{{- define "mychart.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated at 63 chars because some Kubernetes name fields are limited.
*/}}
{{- define "mychart.fullname" -}}
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
{{- define "mychart.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "mychart.labels" -}}
helm.sh/chart: {{ include "mychart.chart" . }}
{{ include "mychart.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels (immutable — used in matchLabels).
*/}}
{{- define "mychart.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mychart.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use.
*/}}
{{- define "mychart.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "mychart.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
```

### Why These Helpers Matter
- **Name truncation** — Kubernetes names max at 63 characters. Always trunc.
- **Selector labels separate from common labels** — selectors are immutable after creation. Adding `app.kubernetes.io/version` to selectors breaks upgrades.
- **nameOverride vs fullnameOverride** — `nameOverride` replaces the chart name portion, `fullnameOverride` replaces everything.

---

## Deployment Patterns

### Standard Web Service

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "mychart.fullname" . }}
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
spec:
  {{- if not .Values.autoscaling.enabled }}
  replicas: {{ .Values.replicaCount }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "mychart.selectorLabels" . | nindent 6 }}
  template:
    metadata:
          annotations:
            {{- if .Values.backup.enabled }}
            {{- if .Values.backup.volumesToBackup }}
            backup.velero.io/backup-volumes: {{ .Values.backup.volumesToBackup | quote }}
            {{- end }}
            {{- if .Values.backup.volumesToExclude }}
            backup.velero.io/backup-volumes-excludes: {{ .Values.backup.volumesToExclude | quote }}
            {{- end }}
            {{- if .Values.backup.hooks.enabled }}
            {{- if .Values.backup.hooks.preCommand }}
            pre.hook.backup.velero.io/command: {{ .Values.backup.hooks.preCommand | quote }}
            pre.hook.backup.velero.io/container: {{ .Chart.Name }}
            {{- end }}
            {{- if .Values.backup.hooks.postCommand }}
            post.hook.backup.velero.io/command: {{ .Values.backup.hooks.postCommand | quote }}
            post.hook.backup.velero.io/container: {{ .Chart.Name }}
            {{- end }}
            {{- end }}
            {{- end }}
            {{- with .Values.podAnnotations }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      labels:
        {{- include "mychart.labels" . | nindent 8 }}
        {{- with .Values.podLabels }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
    spec:
      serviceAccountName: {{ include "mychart.serviceAccountName" . }}
      automountServiceAccountToken: false
      securityContext:
        {{- toYaml .Values.podSecurityContext | nindent 8 }}
      containers:
        - name: {{ .Chart.Name }}
          securityContext:
            {{- toYaml .Values.securityContext | nindent 12 }}
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          ports:
            - name: http
              containerPort: {{ .Values.service.port }}
              protocol: TCP
          startupProbe:
            {{- toYaml .Values.startupProbe | nindent 12 }}
          livenessProbe:
            {{- toYaml .Values.livenessProbe | nindent 12 }}
          readinessProbe:
            {{- toYaml .Values.readinessProbe | nindent 12 }}
          resources:
            {{- toYaml .Values.resources | nindent 12 }}
          {{- with .Values.volumeMounts }}
          volumeMounts:
            {{- toYaml . | nindent 12 }}
          {{- end }}
      {{- with .Values.volumes }}
      volumes:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
```

### Service Dependency Flow (InitContainers)

Always use `initContainers` to ensure dependencies (like databases or APIs) are ready before the main application starts. This prevents crashloop backoffs and race conditions.

```yaml
# In deployment.yaml (spec.template.spec)
      initContainers:
        - name: wait-for-dependency
          image: busybox:1.36
          command: 
            - 'sh'
            - '-c'
            - 'until nc -z {{ .Values.dependencies.database.host }} {{ .Values.dependencies.database.port }}; do echo "Waiting for database..."; sleep 2; done;'
          resources:
            requests:
              cpu: 50m
              memory: 32Mi
            limits:
              cpu: 100m
              memory: 64Mi
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 1000
```

### Worker (No Service)

```yaml
# Same as above but without ports, probes, or Service resource
# Use for background workers, queue consumers, cron jobs
spec:
  containers:
    - name: {{ .Chart.Name }}
      image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
      command: {{ toYaml .Values.command | nindent 8 }}
      resources:
        {{- toYaml .Values.resources | nindent 12 }}
```

---

## Conditional Resource Patterns

### Optional Ingress

```yaml
### Optional IngressRoute (Traefik)

```yaml
{{- if .Values.ingressRoute.enabled }}
{{- $host := .Values.ingressRoute.host }}
{{- $hasWww := hasPrefix "www." $host }}
{{- $nakedHost := trimPrefix "www." $host }}

# Redirect all plain HTTP traffic to HTTPS
apiVersion: traefik.io/v1alpha1
kind: Middleware
metadata:
  name: {{ include "mychart.fullname" . }}-redirect-https
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
spec:
  redirectScheme:
    scheme: https
    permanent: true
---
{{- if $hasWww }}
apiVersion: traefik.io/v1alpha1
kind: Middleware
metadata:
  name: {{ include "mychart.fullname" . }}-redirect-www
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
spec:
  redirectRegex:
    regex: "^https?://{{ $nakedHost }}/(.*)"
    replacement: "https://{{ $host }}/${1}"
    permanent: true
---
{{- end }}
apiVersion: traefik.io/v1alpha1
kind: IngressRoute
metadata:
  name: {{ include "mychart.fullname" . }}-http
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
    app.kubernetes.io/component: web
spec:
  entryPoints:
    - web
  routes:
    - match: Host(`{{ $host }}`){{ if $hasWww }} || Host(`{{ $nakedHost }}`){{ end }}
      kind: Rule
      middlewares:
        - name: {{ include "mychart.fullname" . }}-redirect-https
      services:
        - name: {{ include "mychart.fullname" . }}
          port: {{ .Values.service.port }}
---
apiVersion: traefik.io/v1alpha1
kind: IngressRoute
metadata:
  name: {{ include "mychart.fullname" . }}-https
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
    app.kubernetes.io/component: web
spec:
  entryPoints:
    - websecure
  routes:
    - match: Host(`{{ $host }}`)
      kind: Rule
      services:
        - name: {{ include "mychart.fullname" . }}
          port: {{ .Values.service.port }}
{{- if $hasWww }}
    - match: Host(`{{ $nakedHost }}`)
      kind: Rule
      middlewares:
        - name: {{ include "mychart.fullname" . }}-redirect-www
      services:
        - name: {{ include "mychart.fullname" . }}
          port: {{ .Values.service.port }}
{{- end }}
  tls:
    {{- if .Values.ingressRoute.certResolver }}
    certResolver: {{ .Values.ingressRoute.certResolver }}
    {{- else if .Values.ingressRoute.tlsSecretName }}
    secretName: {{ .Values.ingressRoute.tlsSecretName }}
    {{- end }}
{{- end }}
```

### Optional Ingress

```yaml
{{- if .Values.ingress.enabled -}}
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: {{ include "mychart.fullname" . }}
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
  {{- with .Values.ingress.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  ingressClassName: {{ .Values.ingress.className }}
  {{- if .Values.ingress.tls }}
  tls:
    {{- range .Values.ingress.tls }}
    - hosts:
        {{- range .hosts }}
        - {{ . | quote }}
        {{- end }}
      secretName: {{ .secretName }}
    {{- end }}
  {{- end }}
  rules:
    {{- range .Values.ingress.hosts }}
    - host: {{ .host | quote }}
      http:
        paths:
          {{- range .paths }}
          - path: {{ .path }}
            pathType: {{ .pathType }}
            backend:
              service:
                name: {{ include "mychart.fullname" $ }}
                port:
                  number: {{ $.Values.service.port }}
          {{- end }}
    {{- end }}
{{- end }}
```

### Optional HPA

```yaml
{{- if .Values.autoscaling.enabled }}
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ include "mychart.fullname" . }}
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{ include "mychart.fullname" . }}
  minReplicas: {{ .Values.autoscaling.minReplicas }}
  maxReplicas: {{ .Values.autoscaling.maxReplicas }}
  metrics:
    {{- if .Values.autoscaling.targetCPUUtilizationPercentage }}
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {{ .Values.autoscaling.targetCPUUtilizationPercentage }}
    {{- end }}
    {{- if .Values.autoscaling.targetMemoryUtilizationPercentage }}
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: {{ .Values.autoscaling.targetMemoryUtilizationPercentage }}
    {{- end }}
{{- end }}
```

---

## Helm Hooks & Lifecycle

Hooks allow executing Jobs at specific points in the release lifecycle.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: "{{ include "mychart.fullname" . }}-db-migrate"
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
  annotations:
    # Execute before install and upgrade
    "helm.sh/hook": pre-install,pre-upgrade
    # Execute before other hooks (lower weight runs first)
    "helm.sh/hook-weight": "-5"
    # Delete the job after successful execution (can also be before-hook-creation, hook-failed)
    "helm.sh/hook-delete-policy": hook-succeeded
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          command: ["/bin/sh", "-c", "run-migrations.sh"]
```

---

## Custom Resource Definitions (CRDs)

In Helm 3, CRDs must be placed in the `crds/` directory, NOT in `templates/`.
- Files in `crds/` **cannot be templated** (no `{{ }}`).
- CRDs are installed before templates are rendered.
- Helm does not upgrade or delete CRDs automatically to prevent data loss.

```text
mychart/
├── crds/
│   └── my-custom-resource.yaml   # Plain YAML, no templates
├── templates/
│   └── instance.yaml             # Can be templated
```

---

## SealedSecret Pattern

```yaml
{{- if .Values.sealedSecret.enabled }}
apiVersion: bitnami.com/v1alpha1
kind: SealedSecret
metadata:
  name: {{ include "mychart.fullname" . }}
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
spec:
  encryptedData:
    {{- toYaml .Values.sealedSecret.encryptedData | nindent 4 }}
  template:
    metadata:
      name: {{ include "mychart.fullname" . }}
      labels:
        {{- include "mychart.labels" . | nindent 8 }}
    type: Opaque
{{- end }}
```

---

## Velero Backup Schedule Pattern

```yaml
{{- if .Values.backup.enabled }}
apiVersion: velero.io/v1
kind: Schedule
metadata:
  name: {{ include "mychart.fullname" . }}
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
  annotations:
    # Ensure ArgoCD does not prune the schedule or generated backups
    argocd.argoproj.io/compare-options: IgnoreExtraneous
    argocd.argoproj.io/sync-options: Delete=false,Prune=false
spec:
  schedule: {{ .Values.backup.schedule | quote }}
  {{- if .Values.backup.timeZone }}
  timezone: {{ .Values.backup.timeZone | quote }}
  {{- end }}
  # Do not use useOwnerReferencesInBackup: true with GitOps/ArgoCD
  template:
    includedNamespaces:
      - {{ .Release.Namespace }}
    labelSelector:
      matchLabels:
        {{- include "mychart.selectorLabels" . | nindent 8 }}
    ttl: {{ .Values.backup.ttl | default "720h" }}
    includeClusterResources: false
        {{- if .Values.backup.snapshotMoveData }}
        snapshotMoveData: true
        {{- end }}
        {{- if .Values.backup.hooks.enabled }}
        hooks:
          resources:
            - name: pre-post-freeze
              includedNamespaces:
                - {{ .Release.Namespace }}
              includedResources:
                - pods
              labelSelector:
                matchLabels:
                  {{- include "mychart.selectorLabels" . | nindent 18 }}
              {{- if .Values.backup.hooks.preCommand }}
              preHooks:
                - exec:
                    container: {{ .Chart.Name }}
                    # Note: Using hooks in schedule requires a JSON array format (e.g.,["/sbin/fsfreeze", "--freeze", "/data"])
                    command: {{ .Values.backup.hooks.preCommand }}
                    onError: Fail
                    timeout: 30s
              {{- end }}
              {{- if .Values.backup.hooks.postCommand }}
              postHooks:
                - exec:
                    container: {{ .Chart.Name }}
                    command: {{ .Values.backup.hooks.postCommand }}
                    onError: Continue
                    timeout: 30s
              {{- end }}
        {{- end }}
    {{- end }}
```

---

## PersistentVolumeClaim Pattern

```yaml
{{- if .Values.persistence.enabled -}}
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: {{ include "mychart.fullname" . }}
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
spec:
  accessModes:
    - {{ .Values.persistence.accessMode | quote }}
  resources:
    requests:
      storage: {{ .Values.persistence.size | quote }}
  {{- if .Values.persistence.storageClass }}
  {{- if (eq "-" .Values.persistence.storageClass) }}
  storageClassName: ""
  {{- else }}
  storageClassName: "{{ .Values.persistence.storageClass }}"
  {{- end }}
  {{- end }}
{{- end }}
```

---

## PodDisruptionBudget

```yaml
{{- if .Values.pdb.enabled }}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "mychart.fullname" . }}
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
spec:
  {{- if .Values.pdb.minAvailable }}
  minAvailable: {{ .Values.pdb.minAvailable }}
  {{- end }}
  {{- if .Values.pdb.maxUnavailable }}
  maxUnavailable: {{ .Values.pdb.maxUnavailable }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "mychart.selectorLabels" . | nindent 6 }}
{{- end }}
```

---

## Test Connection Template

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: "{{ include "mychart.fullname" . }}-test-connection"
  labels:
    {{- include "mychart.labels" . | nindent 4 }}
  annotations:
    "helm.sh/hook": test
spec:
  containers:
    - name: wget
      image: busybox
      command: ['wget']
      args: ['{{ include "mychart.fullname" . }}:{{ .Values.service.port }}']
  restartPolicy: Never
```

---

## NOTES.txt Pattern

```
1. Get the application URL by running these commands:
{{- if .Values.ingress.enabled }}
{{- range $host := .Values.ingress.hosts }}
  http{{ if $.Values.ingress.tls }}s{{ end }}://{{ $host.host }}
{{- end }}
{{- else if contains "NodePort" .Values.service.type }}
  export NODE_PORT=$(kubectl get --namespace {{ .Release.Namespace }} -o jsonpath="{.spec.ports[0].nodePort}" services {{ include "mychart.fullname" . }})
  export NODE_IP=$(kubectl get nodes --namespace {{ .Release.Namespace }} -o jsonpath="{.items[0].status.addresses[0].address}")
  echo http://$NODE_IP:$NODE_PORT
{{- else if contains "LoadBalancer" .Values.service.type }}
  NOTE: It may take a few minutes for the LoadBalancer IP to be available.
  kubectl get --namespace {{ .Release.Namespace }} svc {{ include "mychart.fullname" . }} -w
{{- else if contains "ClusterIP" .Values.service.type }}
  kubectl --namespace {{ .Release.Namespace }} port-forward svc/{{ include "mychart.fullname" . }} {{ .Values.service.port }}:{{ .Values.service.port }}
  echo "Visit http://127.0.0.1:{{ .Values.service.port }}"
{{- end }}
```

---

## Dependency Management

### Chart.yaml with Dependencies

```yaml
apiVersion: v2
name: myapp
version: 1.0.0
appVersion: "2.5.0"
dependencies:
  - name: postgresql
    version: ~15.5.0
    repository: https://charts.bitnami.com/bitnami
    condition: postgresql.enabled
  - name: redis
    version: ~19.0.0
    repository: https://charts.bitnami.com/bitnami
    condition: redis.enabled
  - name: common
    version: ~2.0.0
    repository: https://charts.bitnami.com/bitnami
    tags:
      - bitnami-common
```

### Overriding Subchart Values

```yaml
# values.yaml — subchart values go under the dependency name
postgresql:
  enabled: true
  auth:
    database: myapp
    username: myapp
  primary:
    resources:
      requests:
        cpu: 250m
        memory: 256Mi

redis:
  enabled: false
```

### Commands

```bash
# Download dependencies
helm dependency update mychart/

# List dependencies
helm dependency list mychart/

# Build (same as update but doesn't update Chart.lock)
helm dependency build mychart/
```

---

## Troubleshooting Checklist

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Template renders empty | Missing `{{- if }}` or wrong value path | `helm template --debug` to see rendered output |
| Upgrade fails on selector change | Selector labels changed between versions | Never change selectorLabels — they're immutable |
| Values not applying | Wrong nesting in values override | Check indentation and key paths |
| Subchart not rendering | Missing `condition:` or dependency not updated | Run `helm dependency update` |
| Name too long | Kubernetes 63-char limit | Ensure `trunc 63` in _helpers.tpl |
| RBAC permission denied | ServiceAccount missing or wrong Role | Check SA exists and RoleBinding is correct |
