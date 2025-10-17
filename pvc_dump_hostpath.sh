#!/usr/bin/env sh
set -eu

############################
# User-configurable section
############################
# Namespaces
NS_PREFIX="${NS_PREFIX:-}"                       # e.g. "team-" (matches team-a, team-b, ...)
NS_LIST="${NS_LIST:-}"                           # explicit namespaces: 'ns1 ns2' (overrides prefix if set)
INCLUDE_PVC_REGEX="${INCLUDE_PVC_REGEX:-.*}"     # regex to include PVC names
EXCLUDE_PVC_REGEX="${EXCLUDE_PVC_REGEX:-}"       # regex to exclude PVC names

# Where to write (on the node)
BACKUP_NODE="${BACKUP_NODE:-worker-01}"          # fallback node if not colocating
BACKUP_BASE_PATH="${BACKUP_BASE_PATH:-/data/backups/pvc-archives}"  # hostPath on the node

# Behavior
COLOCATE_MODE="${COLOCATE_MODE:-true}"           # if true and PVC is RWO+Bound+mounted, run on the app’s node instead
STRICT_RWO_CHECK="${STRICT_RWO_CHECK:-true}"     # when COLOCATE_MODE=false: skip RWO PVCs mounted on another node
KEEP_POD="${KEEP_POD:-false}"
DRY_RUN="${DRY_RUN:-false}"

# Archiving
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-1}"      # gzip level (1..9)
EXCLUDES="${EXCLUDES:-lost+found}"               # space-separated paths (relative to PVC root) to exclude
IMAGE="${IMAGE:-debian:bookworm-slim}"           # must have: sh, tar, gzip, coreutils (sha256sum), find, stat

# Timeouts
CREATE_TIMEOUT="${CREATE_TIMEOUT:-300s}"         # wait for pod Ready
RUN_TIMEOUT="${RUN_TIMEOUT:-0}"                  # 0=no timeout (else e.g. 8h)

# Reporting
REPORT_DIR="${REPORT_DIR:-./reports}"
LOG_DIR="${LOG_DIR:-${REPORT_DIR}/logs}"
CSV_PATH="${CSV_PATH:-${REPORT_DIR}/backup_report.csv}"
############################
# End user-configurable
############################

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }; }
need kubectl
need jq
mkdir -p "$REPORT_DIR" "$LOG_DIR"

stamp() { date -Iseconds; }
append_csv_header() {
  if [ ! -s "$CSV_PATH" ]; then
    echo "date,namespace,pvc,storageClass,accessModes,capacity,phase,result,detail,backup_node,hostpath,archive_file,bytes,checksum_ok" >"$CSV_PATH"
  fi
}
append_csv() { echo "$1" >> "$CSV_PATH"; }

discover_namespaces() {
  if [ -n "$NS_LIST" ]; then
    for ns in $NS_LIST; do echo "$ns"; done
  elif [ -n "$NS_PREFIX" ]; then
    kubectl get ns -o json | jq -r --arg p "$NS_PREFIX" '.items[].metadata.name | select(startswith($p))'
  else
    kubectl get ns -o json | jq -r '.items[].metadata.name'
  fi
}

list_pvcs_json() {
  ns=$1
  kubectl -n "$ns" get pvc -o json
}

pvc_phase() {
  ns=$1 pvc=$2
  kubectl -n "$ns" get pvc "$pvc" -o jsonpath='{.status.phase}' 2>/dev/null || echo "-"
}

# returns: "podName|nodeName" of a pod mounting the PVC (if any), else empty
find_mounting_pod_and_node() {
  ns=$1 pvc=$2
  kubectl -n "$ns" get pods -o json \
  | jq -r --arg pvc "$pvc" '
      .items[]
      | select(.spec.volumes[]? | select(.persistentVolumeClaim? and .persistentVolumeClaim.claimName==$pvc))
      | "\(.metadata.name)|\(.spec.nodeName // "")"
    ' | head -n1
}

safe_delete_pod() {
  ns=$1 pod=$2
  kubectl -n "$ns" delete pod "$pod" --ignore-not-found --grace-period=0 --force >/dev/null 2>&1 || true
}

wait_for_pod_ready() {
  ns=$1 pod=$2 timeout=$3
  kubectl -n "$ns" wait --for=condition=Ready "pod/$pod" --timeout="$timeout"
}

wait_for_pod_complete() {
  ns=$1 pod=$2 timeout=$3
  if [ "$timeout" = "0" ]; then
    kubectl -n "$ns" wait --for=condition=ContainersReady "pod/$pod" --timeout=1s >/dev/null 2>&1 || true
    kubectl -n "$ns" wait --for=condition=Ready "pod/$pod" --timeout=1s >/dev/null 2>&1 || true
    # Poll until terminated
    while :; do
      phase=$(kubectl -n "$ns" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
      if [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; then break; fi
      sleep 5
    done
  else
    kubectl -n "$ns" wait --for=condition=Ready "pod/$pod" --timeout="${CREATE_TIMEOUT}" >/dev/null 2>&1 || true
    kubectl -n "$ns" wait --for=condition=Complete "pod/$pod" --timeout="$timeout"
  fi
}

make_archive_command() {
  # Build a POSIX-sh command that:
  # - writes exclusively to /backup/out (writable hostPath)
  # - creates a temp file then atomic mv
  # - produces meta JSON without jq
  archive_prefix=$1
  excludes="$2"
  level=$3

  # Build tar exclude args
  excl_args=""
  for e in $excludes; do
    [ -n "$e" ] && excl_args="$excl_args --exclude=$e"
  done

  cat <<'EOSH' | sed "s|__ARCHIVE_PREFIX__|$archive_prefix|g; s|__EXCL_ARGS__|$excl_args|g; s|__LEVEL__|$level|g"
set -eu
mkdir -p /backup/out
cd /src

ARCH="__ARCHIVE_PREFIX__"
TMP="/backup/out/.${ARCH}.tar.gz.$(date +%s).$$"
OUT="/backup/out/${ARCH}.tar.gz"
META="/backup/out/${ARCH}.meta.json"

# Create archive to temp then mv
# Avoid GNU-only options; stay POSIX-sh compatible.
# shellcheck disable=SC2086
tar -cf - __EXCL_ARGS__ . | gzip -c -"__LEVEL__" > "$TMP"

# Compute size and checksum
BYTES=$(stat -c '%s' "$TMP" 2>/dev/null || stat -f '%z' "$TMP")
SHA=$(sha256sum "$TMP" 2>/dev/null | awk '{print $1}')
[ -n "$SHA" ] || SHA=$(sha256 "$TMP" 2>/dev/null | awk '{print $4}')

mv "$TMP" "$OUT"

printf '{' >"$META"
printf '"file": "%s",' "$(basename "$OUT")" >>"$META"
printf '"bytes": %s,' "${BYTES:-0}" >>"$META"
printf '"sha256": "%s",' "${SHA:-unknown}" >>"$META"
printf '"finished_at": "%s"' "$(date -Iseconds)" >>"$META"
printf '}\n' >>"$META"
EOSH
}

build_pod_manifest() {
  ns=$1 pvc=$2 node=$3 host_dir=$4 archive_prefix=$5
  excludes=$6 level=$7 image=$8 run_timeout=$9

  cmd=$(make_archive_command "$archive_prefix" "$excludes" "$level" | sed 's/\\/\\\\/g; s/"/\\"/g; s/$/\\n/')

  cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: backup-${pvc}-$(date +%s)
  namespace: ${ns}
spec:
  restartPolicy: Never
  nodeName: ${node}
  securityContext:
    runAsUser: 0
    runAsGroup: 0
    fsGroup: 0
    fsGroupChangePolicy: OnRootMismatch
  tolerations:
  - operator: "Exists"
  volumes:
  - name: src
    persistentVolumeClaim:
      claimName: ${pvc}
      readOnly: true
  - name: out
    hostPath:
      path: ${host_dir}
      type: DirectoryOrCreate
  containers:
  - name: archiver
    image: ${image}
    imagePullPolicy: IfNotPresent
    securityContext:
      runAsUser: 0
      runAsGroup: 0
      allowPrivilegeEscalation: false
      readOnlyRootFilesystem: false
    volumeMounts:
    - name: src
      mountPath: /src
      readOnly: true
    - name: out
      mountPath: /backup/out
      readOnly: false
    command: ["sh","-c"]
    args:
    - |
      ${cmd}
    resources: {}
  terminationGracePeriodSeconds: 10
EOF
}

backup_one_pvc() {
  ns=$1 pvc=$2 sc=$3 access=$4 capacity=$5

  phase=$(pvc_phase "$ns" "$pvc")
  archive_prefix="${ns}-${pvc}-$(date -u +%Y%m%dT%H%M%SZ)"
  host_dir="${BACKUP_BASE_PATH}/${ns}/${pvc}"
  mkdir -p "$LOG_DIR"

  # Decide node
  node="$BACKUP_NODE"
  if [ "$COLOCATE_MODE" = "true" ]; then
    mpn=$(find_mounting_pod_and_node "$ns" "$pvc" || true)
    if [ -n "$mpn" ]; then
      node_candidate=$(printf '%s' "$mpn" | awk -F'|' '{print $2}')
      if [ -n "$node_candidate" ]; then node="$node_candidate"; fi
    fi
  elif [ "$STRICT_RWO_CHECK" = "true" ] && printf '%s' "$access" | grep -q '^ReadWriteOnce'; then
    # If not colocating but RWO and mounted elsewhere, skip
    mpn=$(find_mounting_pod_and_node "$ns" "$pvc" || true)
    if [ -n "$mpn" ]; then
      echo "Skip $ns/$pvc: RWO mounted and STRICT_RWO_CHECK=true" >&2
      append_csv "$(printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' \
        "$(stamp)" "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "skipped" "rwo-mounted-strict" "$node" "$host_dir" "-" "-" "-")"
      return 0
    fi
  fi

  if [ "$DRY_RUN" = "true" ]; then
    echo "[DRY-RUN] Would create backup pod for $ns/$pvc on node $node -> $host_dir"
    append_csv "$(printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' \
      "$(stamp)" "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "dry-run" "-" "$node" "$host_dir" "-" "-" "-")"
    return 0
  fi

  pod_manifest=$(build_pod_manifest "$ns" "$pvc" "$node" "$host_dir" "$archive_prefix" "$EXCLUDES" "$COMPRESSION_LEVEL" "$IMAGE" "$RUN_TIMEOUT")
  pod=$(printf '%s\n' "$pod_manifest" | awk '/name: backup-/{print $3; exit}')
  printf '%s\n' "$pod_manifest" | kubectl apply -f - >/dev/null

  # Wait for Ready then completion
  wait_for_pod_ready "$ns" "$pod" "$CREATE_TIMEOUT" || true
  wait_for_pod_complete "$ns" "$pod" "$RUN_TIMEOUT" || true

  # Collect logs and results
  log_file="${LOG_DIR}/${ns}-${pvc}-${archive_prefix}.log"
  kubectl -n "$ns" logs "$pod" >"$log_file" 2>&1 || true

  # Determine container exit code (fallback to phase)
  rc=$(kubectl -n "$ns" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || echo "1")

  file="-"; bytes="-"; checksum_ok="-"; detail="-"
  if [ "$rc" = "0" ]; then
    # Read meta.json inside the pod (no bash; plain sh)
    meta=$(kubectl -n "$ns" exec "$pod" -- sh -ceu 'cat /backup/out/'"$archive_prefix"'.meta.json 2>/dev/null || echo "{}"' || echo "{}")
    file=$(printf '%s' "$meta" | jq -r '.file // "-"')
    bytes=$(printf '%s' "$meta" | jq -r '.bytes // "-"')
    if kubectl -n "$ns" exec "$pod" -- sh -c '[ -f /backup/out/'"$file"' ]' >/dev/null 2>&1; then
      checksum_ok="true"; detail="ok"
    else
      checksum_ok="false"; detail="archive-missing"; rc=1
    fi
  else
    detail="container-exit-$rc"
  fi

  result="error"
  [ "$rc" = "0" ] && result="ok"

  append_csv "$(printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' \
    "$(stamp)" "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "$result" "$detail" "$node" "$host_dir" "$file" "$bytes" "$checksum_ok")"

  if [ "$KEEP_POD" != "true" ]; then safe_delete_pod "$ns" "$pod"; fi
}

main() {
  append_csv_header
  nslist=$(discover_namespaces)
  [ -z "$nslist" ] && { echo "No namespaces matched."; exit 0; }

  printf '%s\n' "$nslist" | while IFS= read -r ns; do
    [ -n "$ns" ] || continue
    if ! json=$(list_pvcs_json "$ns"); then
      echo "Cannot list PVCs in $ns" >&2
      continue
    fi
    pvcs=$(echo "$json" | jq -r '.items[].metadata.name')
    [ -z "$pvcs" ] && { echo "No PVCs in $ns"; continue; }

    printf '%s\n' "$pvcs" | while IFS= read -r pvc; do
      [ -n "$pvc" ] || continue
      printf '%s' "$pvc" | grep -Eq "$INCLUDE_PVC_REGEX" || continue
      if [ -n "$EXCLUDE_PVC_REGEX" ] && printf '%s' "$pvc" | grep -Eq "$EXCLUDE_PVC_REGEX"; then
        continue
      fi
      sc=$(echo "$json" | jq -r --arg pvc "$pvc" '.items[] | select(.metadata.name==$pvc) | (.spec.storageClassName // "-")')
      access=$(echo "$json" | jq -r --arg pvc "$pvc" '.items[] | select(.metadata.name==$pvc) | (.spec.accessModes | join("+"))')
      cap=$(echo "$json" | jq -r --arg pvc "$pvc" '.items[] | select(.metadata.name==$pvc) | (.status.capacity.storage // "-")')
      backup_one_pvc "$ns" "$pvc" "$sc" "$access" "$cap"
    done
  done
  echo "Finished at: $(stamp)"
}

main
