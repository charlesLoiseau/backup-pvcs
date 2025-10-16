#!/usr/bin/env bash
set -Eeuo pipefail

# ------------- Parameters -------------
DEST_DIR="${1:-}"                         # destination directory for archives
NS_PREFIX="${NS_PREFIX:-test)}"            # namespace prefix to scan (env var)
LOG_DIR="${LOG_DIR:-./logs}"              # logs directory (env var)
MAX_PARALLEL="${MAX_PARALLEL:-1}"         # 1 = sequential; increase to parallelize
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
IMAGE="${IMAGE:-alpine:3.20}"             # dumper pod image
READY_TIMEOUT="${READY_TIMEOUT:-180s}"    # pod Ready timeout
RETRIES="${RETRIES:-3}"                   # retry count for sensitive ops
SLEEP_BASE="${SLEEP_BASE:-2}"             # exponential backoff base (seconds)

if [[ -z "$DEST_DIR" ]]; then
  echo "Usage: $0 /path/to/destination-dir"
  echo "Useful env vars: NS_PREFIX , LOG_DIR, MAX_PARALLEL, IMAGE"
  exit 1
fi

mkdir -p "$DEST_DIR" "$LOG_DIR"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
ts_compact() { date '+%Y%m%d-%H%M%S'; }

LOG_FILE="$LOG_DIR/dump-rwx-$(ts_compact).log"
REPORT_CSV="$DEST_DIR/report-$(ts_compact).csv"
CHECKSUMS_FILE="$DEST_DIR/checksums-$(ts_compact).sha256"

# Mirror stdout/stderr to console and to the log file
exec > >(tee -a "$LOG_FILE") 2>&1

# ------------- Logging helpers -------------
log()   { echo "[$(ts)] [$1] $2"; }
info()  { log INFO "$1"; }
warn()  { log WARN "$1"; }
err()   { log ERROR "$1"; }

abort() {
  err "$1"
  err "Aborting."
  exit 1
}

# Global error and exit traps
trap 'err "An error occurred at line $LINENO";' ERR
trap 'info "Script end (trap EXIT). See $LOG_FILE, CSV: $REPORT_CSV, SHA256: $CHECKSUMS_FILE"' EXIT

# ------------- Pre-flight checks -------------
command -v "$KUBECTL" >/dev/null || abort "kubectl not found"
command -v "$JQ" >/dev/null 2>&1 || abort "jq not found"
command -v gzip >/dev/null || abort "gzip not found"
command -v sha256sum >/dev/null || abort "sha256sum not found"

# ------------- Generic helpers -------------
# retry N cmd... : run a command with retries and exponential backoff
retry() {
  local tries=$1; shift
  local n=1; local delay=$SLEEP_BASE
  until "$@"; do
    if (( n >= tries )); then return 1; fi
    warn "Retry $n/$tries for: $* (sleep ${delay}s)"
    sleep "$delay"; delay=$((delay*2)); n=$((n+1))
  done
}

# Create a temporary pod mounting the target PVC read-only
create_pod() {
  local ns="$1" pvc="$2" pod="$3"
  cat <<EOF | "$KUBECTL" apply -n "$ns" -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: $pod
  labels: { app: pvc-dumper, pvc: "$pvc" }
spec:
  restartPolicy: Never
  containers:
  - name: dumper
    image: $IMAGE
    command: ["sh","-c","sleep infinity"]
    securityContext:
      readOnlyRootFilesystem: true
    volumeMounts:
    - name: data
      mountPath: /mnt
      readOnly: true
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: $pvc
      readOnly: true
EOF
}

# Wait for pod to be Ready
wait_ready() {
  "$KUBECTL" wait --for=condition=Ready "pod/$2" -n "$1" --timeout="$READY_TIMEOUT" >/dev/null
}

# Delete the temporary pod
delete_pod() {
  "$KUBECTL" delete pod "$2" -n "$1" --ignore-not-found >/dev/null 2>&1 || true
}

# Dump one RWX PVC (Filesystem mode) into a local gzipped tar
dump_one() {
  local ns="$1" pvc="$2"
  local outdir="$DEST_DIR/$ns"; mkdir -p "$outdir"
  local pod="dump-${pvc//_/--}-$(ts_compact)"; pod="${pod,,}"; pod="${pod//./-}"
  local outfile="$outdir/${pvc}-$(ts_compact).tar.gz"

  info "Start dump ns=$ns pvc=$pvc pod=$pod -> $outfile"

  if ! retry "$RETRIES" create_pod "$ns" "$pvc" "$pod"; then
    err "Pod creation failed ($ns/$pvc)"; echo "$ns,$pvc,FAILED,pod_create" >> "$REPORT_CSV"; return 1
  fi

  # Ensure pod cleanup even if errors occur
  local cleanup_called=0
  cleanup() { ((cleanup_called)) || { cleanup_called=1; delete_pod "$ns" "$pod"; }; }
  trap cleanup RETURN

  if ! retry "$RETRIES" wait_ready "$ns" "$pod"; then
    err "Pod did not become Ready ($ns/$pvc)"; echo "$ns,$pvc,FAILED,pod_not_ready" >> "$REPORT_CSV"; return 1
  fi

  # Stream tar from /mnt
  if ! retry "$RETRIES" bash -c \
    "$KUBECTL exec -n '$ns' '$pod' -- sh -c 'tar -C /mnt -czvf - . 2>/dev/null' > '$outfile'"; then
    err "Dump failed ($ns/$pvc)"; echo "$ns,$pvc,FAILED,exec_tar" >> "$REPORT_CSV"; return 1
  fi

  # Validate archive integrity
  if ! gzip -t "$outfile" 2>/dev/null; then
    err "Corrupted archive: $outfile"; echo "$ns,$pvc,FAILED,corrupt_archive" >> "$REPORT_CSV"; return 1
  fi

  # Record checksum
  sha256sum "$outfile" >> "$CHECKSUMS_FILE"

  info "OK $outfile"
  echo "$ns,$pvc,OK,$outfile" >> "$REPORT_CSV"

  cleanup
  trap - RETURN
  return 0
}

# ------------- Discover namespaces & PVCs -------------
info "Destination directory: $DEST_DIR"
info "Namespace prefix: $NS_PREFIX"
info "Log file: $LOG_FILE"

mapfile -t NS < <("$KUBECTL" get ns -o json | "$JQ" -r '.items[].metadata.name' | grep -E "^${NS_PREFIX}" || true)
if [[ ${#NS[@]} -eq 0 ]]; then
  warn "No namespaces starting with '${NS_PREFIX}'."
  exit 0
fi

# Build worklist of (ns;pvc) for RWX Filesystem PVCs only
WORK=()
for ns in "${NS[@]}"; do
  info "Scan namespace: $ns"
  mapfile -t lines < <(
    "$KUBECTL" get pvc -n "$ns" -o json \
      | "$JQ" -r '
        .items[]
        | select(.spec.accessModes[]? == "ReadWriteMany")
        | "\(.metadata.name)\t\(.spec.volumeMode // "Filesystem")"
      ' || true
  )
  if [[ ${#lines[@]} -eq 0 ]]; then
    info "  no RWX PVCs"
    continue
  fi
  for L in "${lines[@]}"; do
    pvc="${L%%$'\t'*}"; vmode="${L#*$'\t'}"
    if [[ "$vmode" == "Block" ]]; then
      warn "  skip $ns/$pvc (volumeMode=Block)"
      echo "$ns,$pvc,SKIPPED,block_volume" >> "$REPORT_CSV"
      continue
    fi
    WORK+=("$ns;$pvc")
  done
done

if [[ ${#WORK[@]} -eq 0 ]]; then
  warn "No RWX PVCs to process."
  exit 0
fi

info "Total PVCs to dump: ${#WORK[@]}"
echo "namespace,pvc,status,detail" > "$REPORT_CSV"
: > "$CHECKSUMS_FILE"

# ------------- Execution (sequential or parallel) -------------
if (( MAX_PARALLEL <= 1 )); then
  for item in "${WORK[@]}"; do
    IFS=';' read -r ns pvc <<< "$item"
    dump_one "$ns" "$pvc" || true
  done
else
  # Simple parallelization using xargs -P
  export -f dump_one ts ts_compact log info warn err retry create_pod wait_ready delete_pod
  export DEST_DIR KUBECTL JQ IMAGE READY_TIMEOUT RETRIES SLEEP_BASE REPORT_CSV CHECKSUMS_FILE
  printf "%s\n" "${WORK[@]}" \
    | xargs -I{} -P "$MAX_PARALLEL" bash -c '
        IFS=";"; read -r ns pvc <<< "{}";
        dump_one "$ns" "$pvc" || true
      '
fi

info "Done. Report: $REPORT_CSV ; Checksums: $CHECKSUMS_FILE ; Log: $LOG_FILE"
