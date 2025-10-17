#!/usr/bin/env sh
set -eu

############################
# User-configurable section
############################
# Namespaces
NS_PREFIX="${NS_PREFIX:-}"                 # e.g. "team-" (matches team-a, team-b, ...)
NS_LIST="${NS_LIST:-}"                     # explicit namespaces: 'ns1 ns2' (overrides prefix if set)
INCLUDE_PVC_REGEX="${INCLUDE_PVC_REGEX:-.*}"   # regex to include PVC names
EXCLUDE_PVC_REGEX="${EXCLUDE_PVC_REGEX:-}"     # regex to exclude PVC names

# Where to write (on the node)
BACKUP_NODE="${BACKUP_NODE:-worker-01}"    # node to pin backup pod (ignored if COLOCATE_MODE=true for a given PVC)
BACKUP_BASE_PATH="${BACKUP_BASE_PATH:-/data/backups/pvc-archives}"  # hostPath directory on that node

# Behavior
COLOCATE_MODE="${COLOCATE_MODE:-true}"     # if true and PVC is RWO+Bound+mounted, run on the app’s node instead
STRICT_RWO_CHECK="${STRICT_RWO_CHECK:-true}"  # if true, skip RWO PVCs that are mounted on another node (when COLOCATE_MODE=false)
KEEP_POD="${KEEP_POD:-false}"
DRY_RUN="${DRY_RUN:-false}"

# Archiving
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-1}"   # gzip level (1..9)
EXCLUDES="${EXCLUDES:-lost+found}"            # space-separated paths relative to the PVC root
IMAGE="${IMAGE:-debian:bookworm-slim}"        # must have tar, gzip, coreutils, sha256sum, jq

# Timeouts
CREATE_TIMEOUT="${CREATE_TIMEOUT:-300s}"   # wait for pod Ready
RUN_TIMEOUT="${RUN_TIMEOUT:-0}"            # 0=no timeout (else e.g. 8h)

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
append_csv() {
  echo "$1" >> "$CSV_PATH"
}

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

pvc_mount_info() {
  # prints: phase mounted_nodes(joined by + or - if none)
  ns=$1 pvc=$2
  pods=$(kubectl -n "$ns" get pod -o json \
    | jq -r --arg pvc "$pvc" '.items[]
      | select(.spec.volumes[]? | (.persistentVolumeClaim? // empty) | .claimName==$pvc)
      | .metadata.name')
  if [ -n "$pods" ]; then
    nodes=$(kubectl -n "$ns" get pod -o json \
      | jq -r --arg pvc "$pvc" '.items[]
        | select(.spec.volumes[]? | (.persistentVolumeClaim? // empty) | .claimName==$pvc)
        | (.spec.nodeName // "-")' | sort -u | paste -sd+ -)
  else
    nodes="-"
  fi
  phase=$(kubectl -n "$ns" get pvc "$pvc" -o jsonpath='{.status.phase}')
  echo "$phase $nodes"
}

pod_manifest() {
  ns=$1 pvc=$2 node=$3 host_dir=$4 archive_prefix=$5
  # Build excludes array for tar
  excludes_arr=""
  for e in $EXCLUDES; do excludes_arr="$excludes_arr --exclude='./$e'"; done

  # sanitize pvc for resource name
  pvc_sanitized=$(printf '%s' "$pvc" | sed 's/[^a-z0-9-]/-/g')

  cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: "pvc-backup-${pvc_sanitized}"
  namespace: "$ns"
  labels:
    app: pvc-backup
spec:
  restartPolicy: Never
  nodeName: "$node"
  tolerations:
  - operator: "Exists"
  volumes:
  - name: src
    persistentVolumeClaim:
      claimName: "$pvc"
  - name: out
    hostPath:
      path: "$host_dir"
      type: DirectoryOrCreate
  containers:
  - name: archiver
    image: "$IMAGE"
    imagePullPolicy: IfNotPresent
    securityContext:
      readOnlyRootFilesystem: true
    env:
    - name: ARCHIVE_PREFIX
      value: "$archive_prefix"
    - name: COMPRESSION_LEVEL
      value: "$COMPRESSION_LEVEL"
    - name: EXCLUDES_ARGS
      value: "$excludes_arr"
    volumeMounts:
    - name: src
      mountPath: /src
      readOnly: true
    - name: out
      mountPath: /backup/out
    command: ["sh","-ceu","--"]
    args:
    - |
      set -euo pipefail
      : "Run timeout (seconds) handled by outer kubectl if set"

      mkdir -p /backup/out
      TMP_FILE="/backup/out/.\${ARCHIVE_PREFIX}.tar.gz"
      FINAL_FILE="/backup/out/\${ARCHIVE_PREFIX}.tar.gz"
      TMP_SHA="/backup/out/.\${ARCHIVE_PREFIX}.tar.gz.sha256.tmp"
      FINAL_SHA="/backup/out/\${ARCHIVE_PREFIX}.tar.gz.sha256"
      META="/backup/out/\${ARCHIVE_PREFIX}.meta.json"

      # Create archive to a hidden temp file, then atomically rename
      tar -C /src \${EXCLUDES_ARGS} -cf - . | gzip -\${COMPRESSION_LEVEL} > "\${TMP_FILE}"

      ( cd /backup/out && sha256sum "\$(basename "\${TMP_FILE}")" > "\${TMP_SHA}" )
      mv "\${TMP_FILE}" "\${FINAL_FILE}"
      mv "\${TMP_SHA}"  "\${FINAL_SHA}"

      # Integrity checks
      gzip -t "\${FINAL_FILE}"
      ( cd /backup/out && sha256sum -c "\$(basename "\${FINAL_SHA}")" )

      COUNT_BYTES=\$(stat -c %s "\${FINAL_FILE}")
      printf '{\"parts\":1,\"bytes\":%s,\"file\":\"%s\"}\n' "\$COUNT_BYTES" "\$(basename "\${FINAL_FILE}")" > "\${META}"

      echo "OK"
EOF
}

safe_delete_pod() {
  kubectl -n "$1" delete pod "$2" --ignore-not-found --grace-period=0 --force >/dev/null 2>&1 || true
}

backup_one_pvc() {
  ns=$1 pvc=$2 sc=$3 access=$4 capacity=$5

  info=$(pvc_mount_info "$ns" "$pvc")
  phase=$(printf '%s\n' "$info" | awk '{print $1}')
  mounted_nodes=$(printf '%s\n' "$info" | cut -d' ' -f2-)

  # Decide node
  node="$BACKUP_NODE"
  if [ "$COLOCATE_MODE" = "true" ] && [ "$phase" = "Bound" ] && [ "$mounted_nodes" != "-" ]; then
    node=$(printf '%s' "$mounted_nodes" | cut -d'+' -f1)
  elif [ "$STRICT_RWO_CHECK" = "true" ] && [ "$access" = "ReadWriteOnce" ] && [ "$mounted_nodes" != "-" ] && [ "$COLOCATE_MODE" != "true" ]; then
    echo "Skip RWO PVC mounted elsewhere: $ns/$pvc on $mounted_nodes"
    append_csv "$(printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' \
      "$(stamp)" "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "skipped" "mounted:$mounted_nodes" "-" "-" "-" "-" "-")"
    return
  fi

  date_tag=$(date +%Y%m%dT%H%M%S)
  archive_prefix="${date_tag}-${pvc}"
  host_dir="${BACKUP_BASE_PATH}/out/${ns}/${pvc}/${date_tag}"
  pod="pvc-backup-$(printf '%s' "$pvc" | sed 's/[^a-z0-9-]/-/g')"
  log_file="${LOG_DIR}/${ns}__${pvc}__${date_tag}.log"

  echo "Backing up $ns/$pvc -> $node:$host_dir"
  if [ "$DRY_RUN" = "true" ]; then
    echo "[DRY RUN] Would create pod on $node and write to $host_dir" | tee -a "$log_file"
    append_csv "$(printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' \
      "$(stamp)" "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "dry-run" "-" "$node" "$host_dir" "-" "-" "-")"
    return
  fi

  safe_delete_pod "$ns" "$pod"
  pod_manifest "$ns" "$pvc" "$node" "$host_dir" "$archive_prefix" | kubectl apply -f - >/dev/null

  # Wait for Ready
  if ! kubectl -n "$ns" wait --for=condition=Ready pod/"$pod" --timeout="$CREATE_TIMEOUT" >/dev/null 2>&1; then
    echo "Pod failed to become Ready for $ns/$pvc" | tee -a "$log_file"
    append_csv "$(printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' \
      "$(stamp)" "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "error" "pod-not-ready" "$node" "$host_dir" "-" "-" "-")"
    safe_delete_pod "$ns" "$pod"
    return
  fi

  # Let the container do the work (command runs on start). Optionally enforce run timeout.
  set +e
  if [ "$RUN_TIMEOUT" != "0" ]; then
    kubectl -n "$ns" wait --for=condition=Ready=false --timeout="$RUN_TIMEOUT" pod/"$pod" >/dev/null 2>&1
  else
    kubectl -n "$ns" wait --for=condition=Ready=false --timeout=0s pod/"$pod" >/dev/null 2>&1 || true
  fi
  kubectl -n "$ns" logs "$pod" >"$log_file" 2>&1
  rc=$(kubectl -n "$ns" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || echo "1")
  set -e

  file="-"; bytes="-"; checksum_ok="-"; detail="-"
  if [ "$rc" = "0" ]; then
    meta=$(kubectl -n "$ns" exec "$pod" -- sh -ceu 'cat /backup/out/'"$archive_prefix"'.meta.json 2>/dev/null || echo "{}"')
    file=$(echo "$meta" | jq -r '.file // "-"')
    bytes=$(echo "$meta" | jq -r '.bytes // "-"')
    if kubectl -n "$ns" exec "$pod" -- sh -c '[ -f /backup/out/'"$file"' ]' 2>/dev/null; then
      checksum_ok="true"; detail="ok"
    else
      checksum_ok="false"; detail="archive-missing"
      rc=1
    fi
  else
    detail="container-exit-$rc"
  fi

  if [ "$rc" = "0" ]; then result="ok"; else result="error"; fi

  append_csv "$(printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' \
    "$(stamp)" "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "$result" "$detail" "$node" "$host_dir" "$file" "$bytes" "$checksum_ok")"

  if [ "$KEEP_POD" != "true" ]; then safe_delete_pod "$ns" "$pod"; fi
}

main() {
  append_csv_header
  nslist=$(discover_namespaces)
  if [ -z "$nslist" ]; then echo "No namespaces matched."; exit 0; fi

  echo "Namespaces: $(printf '%s' "$nslist" | tr '\n' ' ')"
  printf '%s\n' "$nslist" | while IFS= read -r ns; do
    [ -n "$ns" ] || continue
    echo "==> Namespace: $ns"
    if ! json=$(list_pvcs_json "$ns"); then
      echo "Cannot list PVCs in $ns" >&2
      continue
    fi
    pvcs=$(echo "$json" | jq -r '.items[].metadata.name')
    if [ -z "$pvcs" ]; then
      echo "No PVCs in $ns"
      continue
    fi

    printf '%s\n' "$pvcs" | while IFS= read -r pvc; do
      [ -n "$pvc" ] || continue
      if ! printf '%s' "$pvc" | grep -Eq "$INCLUDE_PVC_REGEX"; then
        continue
      fi
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
