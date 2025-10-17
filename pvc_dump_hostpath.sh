#!/usr/bin/env bash
set -euo pipefail

########################################
# User-configurable section (host side)
########################################
# Namespaces to process
NS_PREFIX="${NS_PREFIX:-}"                     # e.g. "team-" (matches team-a, team-b, ...)
NS_LIST="${NS_LIST:-}"                         # explicit namespaces: 'ns1 ns2' (overrides prefix if set)
INCLUDE_PVC_REGEX="${INCLUDE_PVC_REGEX:-.*}"   # regex to include PVC names
EXCLUDE_PVC_REGEX="${EXCLUDE_PVC_REGEX:-}"     # regex to exclude PVC names

# Where to write (on the node)
BACKUP_NODE="${BACKUP_NODE:-worker-01}"        # fallback node if not co-locating or unknown
BACKUP_BASE_PATH="${BACKUP_BASE_PATH:-/data/backups/pvc-archives}"  # hostPath directory on that node

# Behavior
COLOCATE_MODE="${COLOCATE_MODE:-true}"         # if true and PVC is RWO and mounted, run on that node
STRICT_RWO_CHECK="${STRICT_RWO_CHECK:-true}"   # if true, abort if RWO PVC is mounted elsewhere
KEEP_POD="${KEEP_POD:-false}"                  # keep pod after run (for inspection)
DRY_RUN="${DRY_RUN:-false}"

# Archiving
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-1}"    # gzip level 1..9 (1 = fastest, 9 = smallest)
EXCLUDES="${EXCLUDES:-}"                       # space-separated relative paths to exclude inside the PVC, e.g. "tmp cache .git"

# Image to run inside the backup pod
IMAGE="${IMAGE:-debian:bookworm-slim}"         # has /bin/sh, tar, gzip; coreutils provides sha256sum

# Timeouts (seconds)
POD_READY_TIMEOUT="${POD_READY_TIMEOUT:-600}"
JOB_TIMEOUT="${JOB_TIMEOUT:-7200}"

# Local host output (CSV + logs)
LOCAL_OUT_DIR="${LOCAL_OUT_DIR:-./backup-logs}"
CSV_FILE="${CSV_FILE:-$LOCAL_OUT_DIR/summary.csv}"

########################################
# Helpers (host side)
########################################
stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
die() { echo "ERROR: $*" >&2; exit 1; }
require() { command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"; }
k() { kubectl "$@"; }

require kubectl
require jq

mkdir -p "$LOCAL_OUT_DIR"
if [ ! -f "$CSV_FILE" ]; then
  echo "timestamp,namespace,pvc,node,archive_file,bytes,checksum_ok,status,message" > "$CSV_FILE"
fi

########################################
# Kubernetes discovery
########################################
list_namespaces() {
  if [ -n "$NS_LIST" ]; then
    printf "%s\n" $NS_LIST
  elif [ -n "$NS_PREFIX" ]; then
    k get ns -o json | jq -r --arg pfx "$NS_PREFIX" '.items[].metadata.name | select(startswith($pfx))'
  else
    k get ns -o json | jq -r '.items[].metadata.name'
  fi
}

list_pvcs_json() {
  local ns="$1"
  k -n "$ns" get pvc -o json
}

pvc_access_modes() {
  local ns="$1" pvc="$2" json="$3"
  echo "$json" | jq -r --arg pvc "$pvc" '.items[] | select(.metadata.name==$pvc) | (.spec.accessModes | join("+"))'
}

pvc_capacity() {
  local ns="$1" pvc="$2" json="$3"
  echo "$json" | jq -r --arg pvc "$pvc" '.items[] | select(.metadata.name==$pvc) | (.status.capacity.storage // "-")'
}

pvc_sc() {
  local ns="$1" pvc="$2" json="$3"
  echo "$json" | jq -r --arg pvc "$pvc" '.items[] | select(.metadata.name==$pvc) | (.spec.storageClassName // "-")'
}

# Find a running pod that mounts the PVC and return its nodeName (first match)
find_pod_and_node_for_pvc() {
  local ns="$1" pvc="$2"
  k -n "$ns" get pod -o json \
    | jq -r --arg pvc "$pvc" '
      .items[]
      | select(.status.phase=="Running")
      | select(.spec.volumes[]? | select(.persistentVolumeClaim? and .persistentVolumeClaim.claimName==$pvc))
      | .spec.nodeName
    ' | head -n1
}

########################################
# YAML generator for the backup Pod
########################################
# Emits on stdout a Pod manifest that:
# - mounts the target PVC at /src (read-only)
# - mounts hostPath BACKUP_BASE_PATH/out/<ns>/<pvc>/<timestamp> at /backup/out
# - runs a POSIX /bin/sh script to archive, checksum, and print META line
gen_pod_yaml() {
  local ns="$1" pvc="$2" node="$3" archive_prefix="$4" out_dir="$5"

  # Build excludes into a single string for sh expansion
  local excludes_args=""
  if [ -n "$EXCLUDES" ]; then
    for e in $EXCLUDES; do
      excludes_args="$excludes_args --exclude='./$e'"
    done
  fi

  # Security context and node pinning
  cat <<'COMMON' > /dev/null
COMMON
  # Now emit actual YAML; use single-quoted heredoc to avoid host-side $ expansion
  cat <<'YAML'
apiVersion: v1
kind: Pod
metadata:
  name: pvc-backup-__PVC__
  namespace: __NS__
  labels:
    app: pvc-backup
    pvc: "__PVC__"
spec:
  restartPolicy: Never
  nodeName: __NODE__
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    fsGroup: 1000
  containers:
  - name: archiver
    image: __IMAGE__
    imagePullPolicy: IfNotPresent
    securityContext:
      readOnlyRootFilesystem: true
    env:
    - name: ARCHIVE_PREFIX
      value: "__ARCHIVE_PREFIX__"
    - name: COMPRESSION_LEVEL
      value: "__COMPRESSION_LEVEL__"
    - name: EXCLUDES_ARGS
      value: "__EXCLUDES_ARGS__"
    volumeMounts:
    - name: src
      mountPath: /src
      readOnly: true
    - name: out
      mountPath: /backup/out
    command: ["/bin/sh","-eu","-c"]
    args:
    - |
      # POSIX sh script (no pipefail)
      umask 022
      mkdir -p /backup/out

      TMP_FILE="/backup/out/.${ARCHIVE_PREFIX}.tar.gz"
      FINAL_FILE="/backup/out/${ARCHIVE_PREFIX}.tar.gz"
      TMP_SHA="/backup/out/.${ARCHIVE_PREFIX}.tar.gz.sha256.tmp"
      FINAL_SHA="/backup/out/${ARCHIVE_PREFIX}.tar.gz.sha256"
      META="/backup/out/${ARCHIVE_PREFIX}.meta.json"

      # Create archive to a hidden temp file, then atomically rename
      # Note: EXCLUDES_ARGS comes from env and is expanded by sh here.
      tar -C /src ${EXCLUDES_ARGS} -cf - . | gzip -${COMPRESSION_LEVEL} > "${TMP_FILE}"

      ( cd /backup/out && sha256sum "$(basename "${TMP_FILE}")" > "${TMP_SHA}" )
      mv "${TMP_FILE}" "${FINAL_FILE}"
      mv "${TMP_SHA}"  "${FINAL_SHA}"

      # Integrity checks
      gzip -t "${FINAL_FILE}"
      ( cd /backup/out && sha256sum -c "$(basename "${FINAL_SHA}")" )

      # Portable byte count (GNU/BSD)
      COUNT_BYTES=$(stat -c %s "${FINAL_FILE}" 2>/dev/null || stat -f %z "${FINAL_FILE}")
      printf '{"parts":1,"bytes":%s,"file":"%s","checksum_ok":true}\n' "$COUNT_BYTES" "$(basename "${FINAL_FILE}")" > "${META}"

      echo "META: $(cat "${META}")"
      echo "OK"
  volumes:
  - name: src
    persistentVolumeClaim:
      claimName: "__PVC__"
      readOnly: true
  - name: out
    hostPath:
      path: "__NODE_OUT_DIR__"
      type: DirectoryOrCreate
YAML
  # Replace placeholders
  sed -e "s|__NS__|$ns|g" \
      -e "s|__PVC__|$pvc|g" \
      -e "s|__NODE__|$node|g" \
      -e "s|__IMAGE__|$IMAGE|g" \
      -e "s|__ARCHIVE_PREFIX__|$archive_prefix|g" \
      -e "s|__COMPRESSION_LEVEL__|$COMPRESSION_LEVEL|g" \
      -e "s|__EXCLUDES_ARGS__|$excludes_args|g" \
      -e "s|__NODE_OUT_DIR__|$out_dir|g"
}

########################################
# Pod runner and log collector
########################################
wait_for_pod_completion() {
  local ns="$1" pod="$2" timeout="$3"
  local start=$(date +%s)
  while true; do
    local phase
    phase=$(k -n "$ns" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Pending")
    case "$phase" in
      Succeeded|Failed) echo "$phase"; return 0;;
      *)
        local now=$(date +%s)
        if [ $((now - start)) -ge "$timeout" ]; then
          echo "Timeout"
          return 0
        fi
        sleep 3
      ;;
    esac
  done
}

collect_meta_from_logs() {
  local ns="$1" pod="$2"
  # META line has JSON after "META: "
  local logs
  logs=$(k -n "$ns" logs "$pod" 2>&1 || true)
  local meta_line
  meta_line=$(printf "%s\n" "$logs" | grep -E '^[[:space:]]*META: ' | tail -n1 || true)
  local meta_json="${meta_line#META: }"
  echo "$meta_json"
}

########################################
# Backup one PVC
########################################
backup_one_pvc() {
  local ns="$1" pvc="$2" sc="$3" access_modes="$4" capacity="$5"

  # Decide node
  local target_node="$BACKUP_NODE"
  local mounted_node=""
  if [ "$COLOCATE_MODE" = "true" ]; then
    mounted_node="$(find_pod_and_node_for_pvc "$ns" "$pvc" || true)"
    if [ -n "$mounted_node" ]; then
      target_node="$mounted_node"
    fi
  fi

  # RWO safety
  if echo "$access_modes" | grep -q "ReadWriteOnce"; then
    if [ -n "$mounted_node" ] && [ "$mounted_node" != "$target_node" ] && [ "$STRICT_RWO_CHECK" = "true" ]; then
      echo "WARN: $ns/$pvc is RWO and appears mounted on $mounted_node; refusing to run on $target_node due to STRICT_RWO_CHECK=true"
      echo "$(stamp),$ns,$pvc,$target_node,,0,,SKIPPED,RWO mounted on $mounted_node" >> "$CSV_FILE"
      return 0
    fi
  fi

  local ts_short
  ts_short="$(date -u +%Y%m%dT%H%M%SZ)"
  local archive_prefix="${ts_short}-${pvc}"
  local node_out_dir="${BACKUP_BASE_PATH}/out/${ns}/${pvc}/${ts_short}"

  echo "-> $ns/$pvc on node $target_node (SC=$sc, AM=$access_modes, CAP=$capacity)"

  local pod="pvc-backup-${pvc}"
  local yaml
  yaml="$(gen_pod_yaml "$ns" "$pvc" "$target_node" "$archive_prefix" "$node_out_dir")"

  if [ "$DRY_RUN" = "true" ]; then
    echo "[DRY-RUN] Would create pod $ns/$pod on $target_node and write to $node_out_dir"
    return 0
  fi

  # Ensure hostPath target exists on the node (created by kubelet DirectoryOrCreate)

  # Apply pod
  echo "$yaml" | k apply -f -

  # Wait for completion
  local phase
  phase="$(wait_for_pod_completion "$ns" "$pod" "$JOB_TIMEOUT")"

  # Collect logs
  local log_file="$LOCAL_OUT_DIR/${ns}__${pvc}__${ts_short}.log"
  k -n "$ns" logs "$pod" > "$log_file" 2>&1 || true

  local meta_json
  meta_json="$(collect_meta_from_logs "$ns" "$pod")"

  # Parse meta if present
  local archive_file="" bytes="" checksum_ok=""
  if [ -n "$meta_json" ] && echo "$meta_json" | jq -e . >/dev/null 2>&1; then
    archive_file=$(echo "$meta_json" | jq -r '.file // ""')
    bytes=$(echo "$meta_json" | jq -r '.bytes // ""')
    checksum_ok=$(echo "$meta_json" | jq -r '.checksum_ok // ""')
  fi

  # CSV line
  local status="$phase"
  local msg=""
  if [ "$phase" = "Timeout" ]; then
    status="Failed"
    msg="timeout ${JOB_TIMEOUT}s"
  fi

  echo "$(stamp),$ns,$pvc,$target_node,$archive_file,$bytes,$checksum_ok,$status,$msg" >> "$CSV_FILE"

  # Cleanup pod
  if [ "$KEEP_POD" != "true" ]; then
    k -n "$ns" delete pod "$pod" --ignore-not-found >/dev/null 2>&1 || true
  else
    echo "KEEP_POD=true: left $ns/$pod for inspection"
  fi
}

########################################
# Main
########################################
main() {
  echo "Started at: $(stamp)"
  echo "Namespaces source: $([ -n "$NS_LIST" ] && echo "NS_LIST" || ([ -n "$NS_PREFIX" ] && echo "NS_PREFIX=$NS_PREFIX" || echo "all namespaces"))"
  echo "Target node (fallback): $BACKUP_NODE"
  echo "Co-locate mode: $COLOCATE_MODE | Strict RWO: $STRICT_RWO_CHECK"
  echo "Image: $IMAGE | Compression: -${COMPRESSION_LEVEL}"
  echo "Excludes: ${EXCLUDES:-<none>}"
  echo "Node hostPath base: $BACKUP_BASE_PATH"
  echo "Local logs: $LOCAL_OUT_DIR"

  mapfile -t nslist < <(list_namespaces)
  ((${#nslist[@]})) || die "No namespaces found."

  echo "Namespaces: ${nslist[*]}"

  for ns in "${nslist[@]}"; do
    echo "==> Namespace: $ns"
    json="$(list_pvcs_json "$ns")" || { echo "Cannot list PVCs in $ns" >&2; continue; }
    mapfile -t pvcs < <(echo "$json" | jq -r '.items[].metadata.name')
    ((${#pvcs[@]})) || { echo "No PVCs in $ns"; continue; }

    for pvc in "${pvcs[@]}"; do
      [[ "$pvc" =~ $INCLUDE_PVC_REGEX ]] || continue
      [[ -n "$EXCLUDE_PVC_REGEX" && "$pvc" =~ $EXCLUDE_PVC_REGEX ]] && continue

      sc="$(pvc_sc "$ns" "$pvc" "$json")"
      access="$(pvc_access_modes "$ns" "$pvc" "$json")"
      cap="$(pvc_capacity "$ns" "$pvc" "$json")"

      backup_one_pvc "$ns" "$pvc" "$sc" "$access" "$cap"
    done
  done
  echo "Finished at: $(stamp)"
}

main
