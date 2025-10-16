#!/usr/bin/env bash
set -euo pipefail

############################
# User-configurable section
############################

# What to back up
NS_PREFIX="${NS_PREFIX:-}"                 # e.g., "team-" to match team-a, team-b, ...
NS_LIST="${NS_LIST:-}"                     # space-separated explicit namespaces (overrides prefix if set)
INCLUDE_PVC_REGEX="${INCLUDE_PVC_REGEX:-.*}" # only PVC names matching this regex
EXCLUDE_PVC_REGEX="${EXCLUDE_PVC_REGEX:-}"   # skip PVC names matching this regex

# Where to write on the chosen node
BACKUP_NODE="${BACKUP_NODE:-worker-01}"    # the node name to pin the dumper pod
BACKUP_BASE_PATH="${BACKUP_BASE_PATH:-/data/backups/pvc-archives}"  # base path on that node

# Archive options
PART_SIZE="${PART_SIZE:-1G}"               # e.g., 1G, 2G, 5G
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-1}"# gzip 1..9
EXCLUDES="${EXCLUDES:-lost+found}"         # space-separated paths relative to PVC root to exclude

# Image containing tar/gzip/split/sha256sum
IMAGE="${IMAGE:-debian:bookworm-slim}"

# Timeouts
CREATE_TIMEOUT="${CREATE_TIMEOUT:-300s}"   # wait for pod Ready
RUN_TIMEOUT="${RUN_TIMEOUT:-0}"            # 0=no timeout, else e.g., 8h

# Reporting
REPORT_DIR="${REPORT_DIR:-./reports}"
LOG_DIR="${LOG_DIR:-${REPORT_DIR}/logs}"
CSV_PATH="${CSV_PATH:-${REPORT_DIR}/backup_report.csv}"

# Behavior knobs
# - strict RWO: if true, refuse to attempt backup when PVC is RWO and currently mounted by another pod.
STRICT_RWO_CHECK="${STRICT_RWO_CHECK:-true}"

# - colocate mode: for RWO PVCs currently used by a pod, attempt to run dumper on that pod's node.
#   This overrides BACKUP_NODE per-PVC when possible. Requires list/watch permission on pods.
COLOCATE_MODE="${COLOCATE_MODE:-true}"

# Keep dumper pods around for re-copying?
KEEP_POD="${KEEP_POD:-false}"

# dry run: list what would be backed up and where, but do not run pods
DRY_RUN="${DRY_RUN:-false}"

############################
# End user-configurable
############################

mkdir -p "$REPORT_DIR" "$LOG_DIR"
DATE_TAG="$(date +%Y%m%d-%H%M%S)"
echo "Started: $(date -Iseconds)"
echo "Reports: $REPORT_DIR"
echo "Logs:    $LOG_DIR"
echo "CSV:     $CSV_PATH"
echo

# CSV header
if [[ ! -f "$CSV_PATH" ]]; then
  echo "date,namespace,pvc,storageClass,accessModes,capacity,phase,result,detail,backup_node,hostpath,archive_prefix,parts,count_bytes,checksum_ok" > "$CSV_PATH"
fi

kget() { kubectl get "$@"; }
kjson() { kubectl get "$1" "$2" -n "$3" -o json; }

# Discover namespaces
discover_namespaces() {
  if [[ -n "$NS_LIST" ]]; then
    echo "$NS_LIST"
    return
  fi
  if [[ -z "$NS_PREFIX" ]]; then
    echo "ERROR: Set NS_PREFIX or NS_LIST" >&2
    exit 1
  fi
  kget ns -o json | jq -r '.items[].metadata.name' | grep -E "^${NS_PREFIX}" || true
}

# For each PVC, we’ll gather metadata
list_pvcs_json() {
  local ns="$1"
  kget pvc -n "$ns" -o json
}

# Determine node to run on:
# - If COLOCATE_MODE=true and PVC is RWO and mounted by a pod, return that pod's node.
# - Else return BACKUP_NODE.
decide_node_for_pvc() {
  local ns="$1" pvc="$2" access="$3"
  local node="$BACKUP_NODE"
  if [[ "$COLOCATE_MODE" == "true" && "$access" =~ ReadWriteOnce ]]; then
    # Find a pod mounting this PVC
    local pod_and_node
    pod_and_node="$(kubectl -n "$ns" get pod -o json \
      | jq -r --arg pvc "$pvc" '
        .items[]
        | {name: .metadata.name, node: .spec.nodeName, vols: (.spec.volumes // [])}
        | select(.vols[]? | .persistentVolumeClaim? .claimName == $pvc)
        | "\(.name)|\(.node)"' | head -n1 || true)"
    if [[ -n "$pod_and_node" && "$pod_and_node" == *"|"* ]]; then
      node="${pod_and_node##*|}"
    fi
  fi
  echo "$node"
}

# Build exclude args
build_exclude_args() {
  local arr=()
  if [[ -n "${EXCLUDES:-}" ]]; then
    # shellcheck disable=SC2206
    local ex=($EXCLUDES)
    for e in "${ex[@]}"; do arr+=( "--exclude=$e" ); done
  fi
  printf '%s\n' "${arr[@]}"
}

# Create and run dumper pod for one PVC
backup_one_pvc() {
  local ns="$1" pvc="$2" sc="$3" access="$4" capacity="$5"

  # Filters
  if [[ -n "$INCLUDE_PVC_REGEX" && ! "$pvc" =~ $INCLUDE_PVC_REGEX ]]; then
    echo "[$ns/$pvc] skip (does not match INCLUDE_PVC_REGEX)" >&2
    return 0
  fi
  if [[ -n "$EXCLUDE_PVC_REGEX" && "$pvc" =~ $EXCLUDE_PVC_REGEX ]]; then
    echo "[$ns/$pvc] skip (matches EXCLUDE_PVC_REGEX)" >&2
    return 0
  fi

  # Check if PVC is Bound and if mounted elsewhere when RWO
  local phase
  phase="$(kjson pvc "$pvc" "$ns" | jq -r '.status.phase')"
  if [[ "$phase" != "Bound" ]]; then
    append_csv "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "skip" "PVC not Bound" "-" "-" "-" "-" "-" "-"
    return 0
  fi

  # For RWO, check active mounts
  if [[ "$STRICT_RWO_CHECK" == "true" && "$access" =~ ReadWriteOnce ]]; then
    local mounts
    mounts="$(kubectl -n "$ns" get pod -o json \
      | jq -r --arg pvc "$pvc" '
        .items[]
        | select((.status.phase!="Failed") and (.status.phase!="Succeeded"))
        | select(.spec.volumes[]? .persistentVolumeClaim? .claimName == $pvc)
        | .metadata.name' || true)"
    if [[ -n "$mounts" ]]; then
      append_csv "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "skipped-RWO-in-use" "Mounted by pod(s): $(echo "$mounts" | tr '\n' ',')" "-" "-" "-" "-" "-" "-"
      echo "[$ns/$pvc] RWO and mounted by: $(echo "$mounts" | tr '\n' ' '), skipping (STRICT_RWO_CHECK=true)" >&2
      return 0
    fi
  fi

  local node chosen_node host_dir archive_prefix log_file manifest tmpfile
  chosen_node="$(decide_node_for_pvc "$ns" "$pvc" "$access")"
  node="$chosen_node"
  host_dir="${BACKUP_BASE_PATH}/out/${ns}/${pvc}/${DATE_TAG}"
  archive_prefix="${DATE_TAG}-${pvc}"
  log_file="${LOG_DIR}/${ns}__${pvc}__${DATE_TAG}.log"
  manifest="$(mktemp)"; tmpfile="$(mktemp)"

  echo "[$ns/$pvc] node=$node hostPath=$host_dir" | tee -a "$log_file"

  if [[ "$DRY_RUN" == "true" ]]; then
    append_csv "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "planned" "dry-run" "$node" "$host_dir" "$archive_prefix" "-" "-" "-"
    rm -f "$manifest" "$tmpfile"
    return 0
  fi

  # Build dumper pod manifest
  local excludes_arr; mapfile -t excludes_arr < <(build_exclude_args)
  {
    cat <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: dump-${ns//[^a-z0-9-]/-}-${pvc//[^a-z0-9-]/-}-${DATE_TAG}
  namespace: ${ns}
  labels:
    app: pvc-backup
    pvc: ${pvc}
    run: ${DATE_TAG}
spec:
  nodeName: ${node}
  restartPolicy: Never
  containers:
    - name: dumper
      image: ${IMAGE}
      securityContext:
        runAsUser: 0
      env:
        - { name: PART_SIZE, value: "${PART_SIZE}" }
        - { name: ARCHIVE_PREFIX, value: "${archive_prefix}" }
        - { name: COMPRESSION_LEVEL, value: "${COMPRESSION_LEVEL}" }
      command:
        - bash
        - -ceu
        - |
          mkdir -p /backup/out
          TMP_PREFIX="/backup/out/.${archive_prefix}.part"
          FINAL_PREFIX="/backup/out/${archive_prefix}.part"
          # prepare excludes
          declare -a EXCLUDES_ARR
YAML
    if ((${#excludes_arr[@]})); then
      for ex in "${excludes_arr[@]}"; do
        printf '          EXCLUDES_ARR+=( "%s" )\n' "$ex"
      done
    else
      echo '          EXCLUDES_ARR=()'
    fi
    cat <<'YAML'
          # Stream, compress, split
          tar -C /src "${EXCLUDES_ARR[@]}" -cf - . \
          | gzip -"${COMPRESSION_LEVEL}" \
          | split -b "${PART_SIZE}" -d -a 4 - "${TMP_PREFIX}."

          # Checksums
          ( cd /backup/out && sha256sum "$(basename "${TMP_PREFIX}")".* > ".parts.sha256.tmp" )

          # Move to final names atomically
          for f in /backup/out/."${ARCHIVE_PREFIX}".part.*; do
            mv "$f" "/backup/out/${ARCHIVE_PREFIX}.part.${f##*.}"
          done
          mv "/backup/out/.parts.sha256.tmp" "/backup/out/${ARCHIVE_PREFIX}.parts.sha256"

          # Integrity test of gzip stream (concatenate parts)
          cat /backup/out/${ARCHIVE_PREFIX}.part.* | gzip -t

          # Collect simple stats
          COUNT_PARTS=$(ls -1 /backup/out/${ARCHIVE_PREFIX}.part.* | wc -l)
          COUNT_BYTES=$(du -cb /backup/out/${ARCHIVE_PREFIX}.part.* | tail -1 | awk '{print $1}')
          echo "{\"parts\":${COUNT_PARTS},\"bytes\":${COUNT_BYTES}}" > /backup/out/${ARCHIVE_PREFIX}.meta.json

          echo "OK"
      volumeMounts:
        - name: src
          mountPath: /src
          readOnly: true
        - name: backup
          mountPath: /backup
  volumes:
    - name: src
      persistentVolumeClaim:
        claimName: ${pvc}
        readOnly: true
    - name: backup
      hostPath:
        path: ${host_dir}
        type: DirectoryOrCreate
YAML
  } > "$manifest"

  local pod="dump-${ns//[^a-z0-9-]/-}-${pvc//[^a-z0-9-]/-}-${DATE_TAG}"

  # Apply and wait
  if ! kubectl -n "$ns" apply -f "$manifest" >/dev/null 2>&1; then
    append_csv "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "error" "kubectl apply failed" "$node" "$host_dir" "$archive_prefix" "-" "-" "-"
    echo "[$ns/$pvc] ERROR applying manifest" | tee -a "$log_file"
    rm -f "$manifest" "$tmpfile"; return 0
  fi

  if ! kubectl -n "$ns" wait --for=condition=Ready "pod/${pod}" --timeout="$CREATE_TIMEOUT" >/dev/null 2>&1; then
    local desc; desc="$(kubectl -n "$ns" describe pod "$pod" 2>&1 || true)"
    append_csv "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "error" "Pod not Ready: $(firstline "$desc")" "$node" "$host_dir" "$archive_prefix" "-" "-" "-"
    echo "$desc" >> "$log_file"
    safe_delete_pod "$ns" "$pod"
    rm -f "$manifest" "$tmpfile"; return 0
  fi

  # Stream logs to file
  if [[ "$RUN_TIMEOUT" != "0" ]]; then
    set +e
    timeout "$RUN_TIMEOUT" kubectl -n "$ns" logs -f "pod/${pod}" | tee -a "$log_file"
    local log_rc=$?
    set -e
    if [[ $log_rc -ne 0 ]]; then
      local desc; desc="$(kubectl -n "$ns" describe pod "$pod" 2>&1 || true)"
      append_csv "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "error" "Logs/timeout rc=$log_rc: $(firstline "$desc")" "$node" "$host_dir" "$archive_prefix" "-" "-" "-"
      echo "$desc" >> "$log_file"
      safe_delete_pod "$ns" "$pod"
      rm -f "$manifest" "$tmpfile"; return 0
    fi
  else
    kubectl -n "$ns" logs -f "pod/${pod}" | tee -a "$log_file"
  fi

  # Check phase
  local pod_phase; pod_phase="$(kubectl -n "$ns" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")"
  if [[ "$pod_phase" != "Succeeded" ]]; then
    local desc; desc="$(kubectl -n "$ns" describe pod "$pod" 2>&1 || true)"
    append_csv "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "error" "Pod phase=$pod_phase: $(firstline "$desc")" "$node" "$host_dir" "$archive_prefix" "-" "-" "-"
    echo "$desc" >> "$log_file"
    safe_delete_pod "$ns" "$pod"
    rm -f "$manifest" "$tmpfile"; return 0
  fi

  # Post-run: read stats and verify checksum on the node directory (best-effort)
  local meta parts bytes checksum_ok detail
  meta="$(kubectl -n "$ns" exec "$pod" -- bash -ceu 'cat /backup/out/'"$archive_prefix"'.meta.json 2>/dev/null || echo "{}"')"
  parts="$(echo "$meta" | jq -r '.parts // "-"')"
  bytes="$(echo "$meta" | jq -r '.bytes // "-"')"

  # Ask the pod to verify checksums once (uses same mount)
  set +e
  kubectl -n "$ns" exec "$pod" -- bash -ceu 'cd /backup/out && sha256sum -c '"$archive_prefix"'.parts.sha256' >"$tmpfile" 2>&1
  local sum_rc=$?
  set -e
  if [[ $sum_rc -eq 0 ]]; then
    checksum_ok="true"; detail="ok"
  else
    checksum_ok="false"; detail="sha256 verify failed (see log)"
    cat "$tmpfile" >> "$log_file"
  fi

  append_csv "$ns" "$pvc" "$sc" "$access" "$capacity" "$phase" "ok" "$detail" "$node" "$host_dir" "$archive_prefix" "$parts" "$bytes" "$checksum_ok"

  if [[ "$KEEP_POD" != "true" ]]; then
    safe_delete_pod "$ns" "$pod"
  fi

  rm -f "$manifest" "$tmpfile"
}

append_csv() {
  local date ns pvc sc access cap phase result detail node host path parts bytes csum
  date="$(date -Iseconds)"
  ns="$1"; pvc="$2"; sc="$3"; access="$4"; cap="$5"; phase="$6"; result="$7"; detail="$8"
  node="$9"; host="${10}"; path="${11}"; parts="${12}"; bytes="${13}"; csum="${14}"
  printf '%s,%s,%s,%s,"%s",%s,%s,%s,"%s",%s,%s,%s,%s,%s,%s\n' \
    "$date" "$ns" "$pvc" "$sc" "$access" "$cap" "$phase" "$result" "$detail" "$node" "$host" "$path" "$parts" "$bytes" "$csum" \
    >> "$CSV_PATH"
}

firstline() { echo "$1" | head -n1; }

safe_delete_pod() {
  local ns="$1" pod="$2"
  kubectl -n "$ns" delete pod "$pod" --wait=false >/dev/null 2>&1 || true
}

main() {
  local nslist; mapfile -t nslist < <(discover_namespaces)
  if ((${#nslist[@]}==0)); then
    echo "No namespaces matched." >&2; exit 0
  fi
  echo "Namespaces: ${nslist[*]}"
  echo

  for ns in "${nslist[@]}"; do
    echo "=== Namespace: $ns ==="
    local json; json="$(list_pvcs_json "$ns")" || { echo "Cannot list PVCs in $ns" >&2; continue; }
    # Iterate PVCs
    mapfile -t pvcs < <(echo "$json" | jq -r '.items[].metadata.name' )
    if ((${#pvcs[@]}==0)); then
      echo "No PVCs in $ns"; continue
    fi
    for pvc in "${pvcs[@]}"; do
      local sc access cap
      sc="$(echo "$json" | jq -r --arg pvc "$pvc" '.items[] | select(.metadata.name==$pvc) | (.spec.storageClassName // "-")')"
      access="$(echo "$json" | jq -r --arg pvc "$pvc" '.items[] | select(.metadata.name==$pvc) | (.spec.accessModes | join("+"))')"
      cap="$(echo "$json" | jq -r --arg pvc "$pvc" '.items[] | select(.metadata.name==$pvc) | (.status.capacity.storage // "-")')"
      backup_one_pvc "$ns" "$pvc" "$sc" "$access" "$cap"
    done
  endtime="$(date -Iseconds)"
  echo
  echo "Finished at: $endtime"
}

main
