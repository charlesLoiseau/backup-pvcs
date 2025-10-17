#!/usr/bin/env sh
# pvc_backup_all.sh
# Back up ALL PVCs in ALL namespaces to a specific node+path using BusyBox.
# - One pod per PVC (runs in parallel).
# - Archives land on the chosen node under: ${BACKUP_BASE}/${namespace}/${pvc}/
# - Script (not pods) writes a CSV summary and per-PVC logs locally.
# Requirements: kubectl, jq

set -eu

# ---------------------- Config ----------------------
BACKUP_NODE="${BACKUP_NODE:-worker-01}"           # Node where backup pods must run
BACKUP_BASE="${BACKUP_BASE:-/var/backups/pvc}"    # Node path (hostPath) where archives are stored
IMAGE="${IMAGE:-busybox:1.36}"                    # BusyBox image
NS_LIST="${NS_LIST:-}"                            # Optional: space-separated namespaces (default: all)
GLOBAL_TIMEOUT_SEC="${GLOBAL_TIMEOUT_SEC:-21600}" # 6h: max overall wait
REPORT_DIR="${REPORT_DIR:-./reports}"             # Local folder for logs & csv (on the machine running this script)
# ----------------------------------------------------

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }; }
need kubectl
need jq

mkdir -p "$REPORT_DIR/logs"
CSV="$REPORT_DIR/backup_report.csv"
LOG="$REPORT_DIR/actions.log"
STAMP() { date -u +%Y-%m-%dT%H:%M:%SZ; }
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"   # stable id used in archive filenames

# CSV header
[ -s "$CSV" ] || echo "date,namespace,pvc,result,detail,backup_node,hostpath,archive_file" >"$CSV"

log() { echo "[$(STAMP)] $*" | tee -a "$LOG" >&2; }

# Discover namespaces
discover_namespaces() {
  if [ -n "$NS_LIST" ]; then
    for ns in $NS_LIST; do echo "$ns"; done
  else
    kubectl get ns -o json | jq -r '.items[].metadata.name'
  fi
}

dns() { printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]/-/g; s/^-*//; s/-*$//; s/--*/-/g' | cut -c1-58; }

# Create one backup pod manifest to stdout
# The pod:
# - mounts PVC read-only at /src
# - mounts hostPath at /backup/out
# - creates a single tar.gz named: ${NS}-${PVC}-${RUN_ID}.tar.gz
make_pod() {
  ns="$1"; pvc="$2"
  pod="pvc-bak-$(dns "$ns")-$(dns "$pvc")-$RUN_ID"
  cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${ns}
  labels:
    app: pvc-backup
    pvc: "${pvc}"
spec:
  restartPolicy: Never
  nodeName: ${BACKUP_NODE}
  volumes:
  - name: src
    persistentVolumeClaim:
      claimName: ${pvc}
      readOnly: true
  - name: out
    hostPath:
      path: ${BACKUP_BASE}/${ns}/${pvc}
      type: DirectoryOrCreate
  containers:
  - name: archiver
    image: ${IMAGE}
    imagePullPolicy: IfNotPresent
    env:
    - name: NS
      value: "${ns}"
    - name: PVC
      value: "${pvc}"
    - name: RUN_ID
      value: "${RUN_ID}"
    volumeMounts:
    - name: src
      mountPath: /src
      readOnly: true
    - name: out
      mountPath: /backup/out
    command: ["sh","-c"]
    args:
      - |
        set -eu
        OUT=/backup/out
        SRC=/src
        ARCH="\${NS}-\${PVC}-\${RUN_ID}.tar.gz"
        TMP="\$OUT/.\$ARCH.\$\$"
        mkdir -p "\$OUT"
        cd "\$SRC" 2>/dev/null || { mkdir -p /tmp/empty && cd /tmp/empty; }
        # BusyBox tar with -z
        tar -czf "\$TMP" .
        mv "\$TMP" "\$OUT/\$ARCH"
        echo "Wrote /backup/out/\$ARCH"
  terminationGracePeriodSeconds: 5
EOF
}

launch_all() {
  ns_list=$(discover_namespaces)
  [ -n "$ns_list" ] || { log "No namespaces found"; return; }

  echo "$ns_list" | while IFS= read -r ns; do
    [ -n "$ns" ] || continue
    pvcs_json=$(kubectl -n "$ns" get pvc -o json 2>/dev/null || echo '{"items":[]}')
    echo "$pvcs_json" | jq -r '.items[].metadata.name' | while IFS= read -r pvc; do
      [ -n "$pvc" ] || continue
      pod="pvc-bak-$(dns "$ns")-$(dns "$pvc")-$RUN_ID"
      log "Creating pod $ns/$pod for PVC $pvc"
      make_pod "$ns" "$pvc" | kubectl apply -f - >/dev/null
    done
  done
}

wait_all_done_or_timeout() {
  start=$(date +%s)
  while :; do
    pods_json=$(kubectl get pods --all-namespaces -l app=pvc-backup -o json 2>/dev/null || echo '{"items":[]}')
    total=$(echo "$pods_json" | jq '.items | length')
    [ "$total" -gt 0 ] || { log "No backup pods found to wait for."; break; }

    done_cnt=$(echo "$pods_json" | jq '[.items[] | select(.status.phase=="Succeeded" or .status.phase=="Failed")] | length')
    if [ "$done_cnt" -lt "$total" ]; then
      now=$(date +%s)
      elapsed=$(( now - start ))
      [ "$elapsed" -gt "$GLOBAL_TIMEOUT_SEC" ] && { log "Global timeout reached (${GLOBAL_TIMEOUT_SEC}s)"; break; }
      sleep 5
      continue
    fi
    break
  done
}

collect_logs_and_csv_and_cleanup() {
  pods_json=$(kubectl get pods --all-namespaces -l app=pvc-backup -o json 2>/dev/null || echo '{"items":[]}')
  echo "$pods_json" | jq -r '.items[] | [.metadata.namespace, .metadata.name, .metadata.labels.pvc] | @tsv' | \
  while IFS=$'\t' read -r ns pod pvc; do
    phase=$(kubectl -n "$ns" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
    log_file="$REPORT_DIR/logs/${ns}__${pvc}__${pod}.log"
    kubectl -n "$ns" logs "$pod" >"$log_file" 2>&1 || true

    # We know the exact target filename thanks to RUN_ID
    archive="${ns}-${pvc}-${RUN_ID}.tar.gz"

    result="error"; detail="$phase"
    [ "$phase" = "Succeeded" ] && { result="ok"; detail="archived"; }

    echo "$(STAMP),$ns,$pvc,$result,$detail,$BACKUP_NODE,$BACKUP_BASE/$ns/$pvc,$archive" >> "$CSV"

    # delete pod
    kubectl -n "$ns" delete pod "$pod" --ignore-not-found >/dev/null 2>&1 || true
  done
}

log "Backup run started at $(STAMP)"
log "Node=${BACKUP_NODE}, Node path=${BACKUP_BASE}, RunID=${RUN_ID}"

launch_all
wait_all_done_or_timeout
collect_logs_and_csv_and_cleanup

log "Backup run finished at $(STAMP)"
log "CSV: $CSV"
log "Logs directory: $REPORT_DIR/logs"
