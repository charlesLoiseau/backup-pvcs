#!/usr/bin/env bash
set -euo pipefail

############################
# User-configurable section
############################
# Namespaces
NS_PREFIX="${NS_PREFIX:-}"                 # e.g. "team-" (matches team-a, team-b, ...)
NS_LIST="${NS_LIST:-}"                     # explicit namespaces: 'ns1 ns2' (overrides prefix if set)
INCLUDE_PVC_REGEX="${INCLUDE_PVC_REGEX:-.*}"   # regex to include PVC names
EXCLUDE_PVC_REGEX="${EXCLUDE_PVC_REGEX:-}"     # regex to exclude PVC names

# Where to write (on the node)
BACKUP_NODE="${BACKUP_NODE:-worker-01}"    # node to pin backup pod (ignored if COLOCATE_MODE=true and a mount is found)
BACKUP_BASE_PATH="${BACKUP_BASE_PATH:-/data/backups/pvc-archives}"  # hostPath directory on that node

# Behavior
COLOCATE_MODE="${COLOCATE_MODE:-true}"         # if true and PVC is RWO and mounted, run on that node
STRICT_RWO_CHECK="${STRICT_RWO_CHECK:-true}"   # if false, allow backing up elsewhere (read-only risk if mounted)
KEEP_POD="${KEEP_POD:-false}"                  # keep pod after run (for inspection)
DRY_RUN="${DRY_RUN:-false}"

# Archiving
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-1}"    # gzip level (1..9)
EXCLUDES="${EXCLUDES:-lost+found}"             # space-separated paths relative to PVC root
IMAGE="${IMAGE:-debian:bookworm-slim}"         # needs tar, gzip, coreutils, sha256sum

# Timeouts
CREATE_TIMEOUT="${CREATE_TIMEOUT:-300s}"   # wait for pod Ready
RUN_TIMEOUT="${RUN_TIMEOUT:-0}"            # 0 = no host-side timeout, else e.g. 8h, requires coreutils `timeout`

# Reporting
REPORT_DIR="${REPORT_DIR:-./reports}"
LOG_DIR="${LOG_DIR:-${REPORT_DIR}/logs}"
CSV_PATH="${CSV_PATH:-${REPORT_DIR}/backup_report.csv}"
############################
# End user-configurable
############################

# --- helpers ---
need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }; }
need kubectl
need jq
mkdir -p "$REPORT_DIR" "$LOG_DIR"

stamp() { date -Iseconds; }
append_csv_header() {
  if [[ ! -s "$CSV_PATH" ]]; then
    echo "date,namespace,pvc,storageClass,accessModes,capacity,phase,result,detail,backup_node,hostpath,archive_file,bytes,checksum_ok" >"$CSV_PATH"
  fi
}
append_csv() { echo "$1" >> "$CSV_PATH"; }

discover_namespaces() {
  if [[ -n "$NS_LIST" ]]; then
    tr ' ' '\n' <<<"$NS_LIST" | sed '/^$/d'
  elif [[ -n "$NS_PREFIX" ]]; then
    kubectl get ns -o json | jq -r --arg pfx "$NS_PREFIX" '.items[].metadata.name | select(startswith($pfx))'
  else
    kubectl get ns -o json | jq -r '.items[].metadata.name'
  fi
}

list_pvcs_json() { # $1=ns
  kubectl -n "$1" get pvc -o json
}

# Find a node that mounts the given PVC (by inspecting pods in the same namespace)
find_mount_node_for_pvc() { # ns pvc -> node or "-"
  local ns="$1" pvc="$2"
  kubectl -n "$ns" get pods -o json \
  | jq -r --arg pvc "$pvc" '
      .items[]
      | select(.status.phase != "Succeeded" and .status.phase != "Failed")
      | select(.spec.volumes[]? | select(.persistentVolumeClaim? and .persistentVolumeClaim.claimName == $pvc))
      | .spec.nodeName // empty
    ' \
  | head -n1 || true
}

# Pick node: co-locate if allowed and possible for RWO; else BACKUP_NODE
pick_node_for_pvc() { # ns pvc accessModes -> node chosen and a detail msg
  local ns="$1" pvc="$2" access="$3"
  local node="" detail=""
  local is_rwo="false"
  [[ "$access" == *"ReadWriteOnce"* ]] && is_rwo="true"

  if [[ "$COLOCATE_MODE" == "true" && "$is_rwo" == "true" ]]; then
    node="$(find_mount_node_for_pvc "$ns" "$pvc" || true)"
    if [[ -n "$node" ]]; then
      detail="colocated"
      echo "$node" "$detail"
      return 0
    fi
  fi

  if [[ "$STRICT_RWO_CHECK" == "true" && "$is_rwo" == "true" && "$COLOCATE_MODE" != "true" ]]; then
    # If STRICT and not colocating, ensure not mounted elsewhere
    local mounted_elsewhere
    mounted_elsewhere="$(find_mount_node_for_pvc "$ns" "$pvc" || true)"
    if [[ -n "$mounted_elsewhere" ]]; then
      echo "-" "rwo-mounted-elsewhere"
      return 0
    fi
  fi

  node="$BACKUP_NODE"; detail="pinned"
  echo "$node" "$detail"
}

build_host_dir() { # ns pvc timestamp -> host path dir
  local ns="$1" pvc="$2" ts="$3"
  echo "${BACKUP_BASE_PATH%/}/out/$ns/$pvc/$ts"
}

pod_name_for() { # pvc -> safe pod name
  local pvc="$1"
  pvc="${pvc//[^a-z0-9-]/-}"
  echo "pvc-backup-$pvc"
}

# YAML generator: IMPORTANT — variables in script are escaped ($$) so the container expands them, not bash/kubectl
pod_manifest() {
  local ns="$1" pvc="$2" node="$3" host_dir="$4" archive_prefix="$5"
  local excludes_arr=""
  for e in $EXCLUDES; do excludes_arr="$excludes_arr --exclude='./$e'"; done

  cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: "$(pod_name_for "$pvc")"
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
    command: ["bash","-ceu","--"]
    args:
    - |
      set -euo pipefail
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
      printf '{\"parts\":1,\"bytes\":%s,\"file\":\"%s\",\"checksum_ok\":true}\n' "\$COUNT_BYTES" "\$(basename "\${FINAL_FILE}")" > "\${META}"

      # Emit a single line that the host can parse from logs
      echo "META: \$(cat "\${META}")"

      # Done
      echo "OK"
EOF
}

apply_and_wait_ready() { # ns pvc
  local ns="$1" pvc="$2"
  local pod="$(pod_name_for "$pvc")"
  kubectl -n "$ns" apply -f - >/dev/null
  kubectl -n "$ns" wait --for=condition=Ready "pod/$pod" --timeout="$CREATE_TIMEOUT" >/dev/null
}

stream_logs_until_exit() { # ns pvc logfile
  local ns="$1" pvc="$2" logfile="$3"
  local pod="$(pod_name_for "$pvc")"

  if [[ "$RUN_TIMEOUT" != "0" ]]; then
    timeout "$RUN_TIMEOUT" kubectl -n "$ns" logs -f "$pod" | tee "$logfile"
    return $?
  else
    kubectl -n "$ns" logs -f "$pod" | tee "$logfile"
    return 0
  fi
}

get_pod_exit_code() { # ns pvc -> exitCode or 0/unknown
  local ns="$1" pvc="$2" pod exitCode
  pod="$(pod_name_for "$pvc")"
  # give the kubelet a moment to write status
  sleep 1
  exitCode="$(kubectl -n "$ns" get pod "$pod" -o json \
    | jq -r '.status.containerStatuses[0].state.terminated.exitCode // empty' || true)"
  if [[ -z "$exitCode" ]]; then echo "0"; else echo "$exitCode"; fi
}

safe_delete_pod() { # ns pvc
  local ns="$1" pvc="$2" pod
  pod="$(pod_name_for "$pvc")"
  kubectl -n "$ns" delete pod "$pod" --ignore-not-found --grace-period=0 --force >/dev/null 2>&1 || true
}

backup_one_pvc() { # ns pvc sc access capacity
  local ns="$1" pvc="$2" sc="$3" access="$4" capacity="$5"

  # Decide node
  read -r node node_detail < <(pick_node_for_pvc "$ns" "$pvc" "$access")
  if [[ "$node" == "-" || -z "$node" ]]; then
    echo "Skip $ns/$pvc: $node_detail"
    append_csv "$(printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' \
      "$(stamp)" "$ns" "$pvc" "$sc" "$access" "$capacity" "-" "skipped" "$node_detail" "-" "-" "-" "-" "-")"
    return 0
  fi

  local ts="$(date +%Y%m%dT%H%M%S)"
  local host_dir; host_dir="$(build_host_dir "$ns" "$pvc" "$ts")"
  local archive_prefix="${ts}-${pvc}"

  echo "Backing up $ns/$pvc -> node=$node path=$host_dir (mode=$node_detail)"

  if [[ "$DRY_RUN" == "true" ]]; then
    append_csv "$(printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' \
      "$(stamp)" "$ns" "$pvc" "$sc" "$access" "$capacity" "-" "dry-run" "$node_detail" "$node" "$host_dir" "-" "-" "-")"
    return 0
  fi

  # Build YAML and run
  local logf="${LOG_DIR}/${ns}__${pvc}__${ts}.log"
  pod_manifest "$ns" "$pvc" "$node" "$host_dir" "$archive_prefix" | apply_and_wait_ready "$ns" "$pvc"
  set +e
  stream_logs_until_exit "$ns" "$pvc" "$logf"
  local logs_rc=$?
  local exit_code; exit_code="$(get_pod_exit_code "$ns" "$pvc")"
  set -e

  # Parse META from logs
  local meta_json file bytes checksum_ok
  meta_json="$(grep -a '^META: ' "$logf" | sed 's/^META: //;t;d' | tail -n1 || true)"
  if [[ -n "$meta_json" ]]; then
    file="$(jq -r '.file // "-"' <<<"$meta_json" 2>/dev/null || echo "-")"
    bytes="$(jq -r '.bytes // "-"' <<<"$meta_json" 2>/dev/null || echo "-")"
    checksum_ok="$(jq -r '.checksum_ok // false' <<<"$meta_json" 2>/dev/null || echo "false")"
  else
    file="-"; bytes="-"; checksum_ok="false"
  fi

  local phase="Terminated"
  local result detail
  if [[ "$logs_rc" -ne 0 ]]; then
    result="error"; detail="logs-stream-$logs_rc"
  elif [[ "$exit_code" != "0" ]]; then
    result="error"; detail="container-exit-$exit_code"
  else
    result="ok"; detail="ok"
  fi

  append_csv "$(printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' \
    "$(stamp)" "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "$result" "$detail" "$node" "$host_dir" "$file" "$bytes" "$checksum_ok")"

  [[ "$KEEP_POD" == "true" ]] || safe_delete_pod "$ns" "$pvc"
}

main() {
  append_csv_header
  mapfile -t nslist < <(discover_namespaces)
  if ((${#nslist[@]}==0)); then echo "No namespaces matched."; exit 0; fi
  echo "Namespaces: ${nslist[*]}"

  for ns in "${nslist[@]}"; do
    echo "==> Namespace: $ns"
    json="$(list_pvcs_json "$ns")" || { echo "Cannot list PVCs in $ns" >&2; continue; }
    mapfile -t pvcs < <(echo "$json" | jq -r '.items[].metadata.name')
    ((${#pvcs[@]})) || { echo "No PVCs in $ns"; continue; }

    for pvc in "${pvcs[@]}"; do
      [[ "$pvc" =~ $INCLUDE_PVC_REGEX ]] || continue
      [[ -n "$EXCLUDE_PVC_REGEX" && "$pvc" =~ $EXCLUDE_PVC_REGEX ]] && continue

      sc="$(echo "$json" | jq -r --arg pvc "$pvc" '.items[] | select(.metadata.name==$pvc) | (.spec.storageClassName // "-")')"
      access="$(echo "$json" | jq -r --arg pvc "$pvc" '.items[] | select(.metadata.name==$pvc) | (.spec.accessModes | join("+"))')"
      cap="$(echo "$json" | jq -r --arg pvc "$pvc" '.items[] | select(.metadata.name==$pvc) | (.status.capacity.storage // "-")')"

      backup_one_pvc "$ns" "$pvc" "$sc" "$access" "$cap"
    done
  done
  echo "Finished at: $(stamp)"
}

main
