#!/usr/bin/env sh
# Simple PVC -> tar.gz backup runner (single PVC)
# Fix applied: write the archive to a writable hostPath (/backup/out) mounted from the node.

set -eu

# --- Config (edit these) ---
NS="${NS:-default}"                   # Kubernetes namespace
PVC="${PVC:-my-pvc}"                  # PVC name to back up
NODE="${NODE:-worker-01}"             # Node where the backup pod should run
HOSTPATH="${HOSTPATH:-/data/backups}" # Directory on the node to store archives
IMAGE="${IMAGE:-debian:bookworm-slim}"# Image with sh, tar, gzip, sha256sum
GZIP_LEVEL="${GZIP_LEVEL:-1}"         # gzip compression level 1..9
EXCLUDES="${EXCLUDES:-lost+found}"    # space-separated relative paths to exclude
TIMEOUT_CREATE="${TIMEOUT_CREATE:-300s}" # Pod ready timeout
# ---------------------------

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing: $1" >&2; exit 1; }; }
need kubectl
stamp_utc() { date -u +%Y%m%dT%H%M%SZ; }

ARCHIVE_PREFIX="${NS}-${PVC}-$(stamp_utc)"
POD="pvc-backup-$(date +%s)"

# Build exclude args for tar
EXCL_ARGS=""
for e in $EXCLUDES; do [ -n "$e" ] && EXCL_ARGS="$EXCL_ARGS --exclude=$e"; done

# Command that runs inside the pod:
POD_CMD=$(cat <<'EOSH'
set -eu
mkdir -p /backup/out
cd /src
ARCH="${ARCHIVE_PREFIX}"
TMP="/backup/out/.${ARCH}.tar.gz.$(date +%s).$$"
OUT="/backup/out/${ARCH}.tar.gz"
META="/backup/out/${ARCH}.meta.json"

# Create archive to temp, then atomic move to final path
# shellcheck disable=SC2086
tar -cf - __EXCL_ARGS__ . | gzip -c -"__LEVEL__" > "$TMP"

# Size + checksum (support GNU/BSD)
BYTES=$(stat -c '%s' "$TMP" 2>/dev/null || stat -f '%z' "$TMP")
SHA=$(sha256sum "$TMP" 2>/dev/null | awk '{print $1}')
[ -n "${SHA:-}" ] || SHA=$(sha256 "$TMP" 2>/dev/null | awk '{print $4}')

mv "$TMP" "$OUT"

# Write a tiny meta file
{
  printf '{'
  printf '"file":"%s",' "$(basename "$OUT")"
  printf '"bytes":%s,' "${BYTES:-0}"
  printf '"sha256":"%s",' "${SHA:-unknown}"
  printf '"finished_at":"%s"' "$(date -Iseconds)"
  printf '}\n'
} > "$META"

echo "Archive: $OUT"
echo "SHA256: $SHA"
EOSH
)

# Inject variables
POD_CMD=${POD_CMD//__LEVEL__/$GZIP_LEVEL}
POD_CMD=${POD_CMD//__EXCL_ARGS__/$EXCL_ARGS}
POD_CMD=${POD_CMD//ARCHIVE_PREFIX/$ARCHIVE_PREFIX}

# Create a simple Pod that:
# - mounts the PVC read-only at /src
# - mounts a writable hostPath at /backup/out
# - runs as root to avoid permission issues on the mount
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${NS}
spec:
  restartPolicy: Never
  nodeName: ${NODE}
  securityContext:
    runAsUser: 0
    runAsGroup: 0
    fsGroup: 0
    fsGroupChangePolicy: OnRootMismatch
  volumes:
  - name: src
    persistentVolumeClaim:
      claimName: ${PVC}
      readOnly: true
  - name: out
    hostPath:
      path: ${HOSTPATH}/${NS}/${PVC}
      type: DirectoryOrCreate
  containers:
  - name: archiver
    image: ${IMAGE}
    imagePullPolicy: IfNotPresent
    securityContext:
      runAsUser: 0
      runAsGroup: 0
      allowPrivilegeEscalation: false
    volumeMounts:
    - name: src
      mountPath: /src
      readOnly: true
    - name: out
      mountPath: /backup/out
    command: ["sh","-c"]
    args:
      - |
        ${POD_CMD}
  terminationGracePeriodSeconds: 5
EOF

# Wait for the pod to be Ready, then for it to finish
kubectl -n "$NS" wait --for=condition=Ready "pod/$POD" --timeout="$TIMEOUT_CREATE" >/dev/null 2>&1 || true

# Poll for completion (Succeeded/Failed)
echo "Waiting for backup to complete..."
while :; do
  phase=$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
  [ "$phase" = "Succeeded" ] && break
  [ "$phase" = "Failed" ] && break
  sleep 3
done

echo "Pod phase: $phase"
kubectl -n "$NS" logs "$POD" || true

# Print result path (on the node)
ARCHIVE_FILE="${ARCHIVE_PREFIX}.tar.gz"
META_FILE="${ARCHIVE_PREFIX}.meta.json"
OUT_DIR="${HOSTPATH}/${NS}/${PVC}"
echo "Node path: ${OUT_DIR}/${ARCHIVE_FILE}"
echo "Meta path: ${OUT_DIR}/${META_FILE}"

# Clean up the pod
kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true
