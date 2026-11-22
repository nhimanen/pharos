#!/usr/bin/env bash
# End-to-end gate of the pharos Kubernetes operator on minikube (spec "Testing", steps 1-8).
#
#   1. builds the pharos and pharos-operator images inside minikube's Docker (no registry);
#   2. installs the CRD, the RBAC and the operator (deploy/), with those images;
#   3. runs scripts/operator_e2e.py: a PharosCluster with MinIO and two tiny generated git projects
#      (served from a pod inside the cluster), Ready, search against a single-node reference, scale
#      up under load, scale down, writer takeover after `delete --force`, writer scale-down, the
#      parentGeneration chain, the upgrade pre-flight (refused / failed) and an ordered upgrade, a
#      secret scan, and the PharosCluster's teardown;
#   4. uninstalls the operator, the RBAC, the CRD and the namespace (unless --keep).
#
# Needs minikube, kubectl, docker and python3 on PATH. The minikube profile must run a CNI that
# enforces NetworkPolicy (calico): `--start` creates it as
#   minikube start -p $E2E_PROFILE --driver=docker --cpus=4 --memory=7g --cni=calico
# Nothing outside the profile is touched: no registry, no ~/.pharos, no model download (embeddings
# are "none"; see docs/superpowers/gates/2026-11-22-operator.md).
#
# Env:
#   E2E_PROFILE   minikube profile (default pharos-e2e)
#   E2E_NS        namespace of the operator and the cluster (default pharos; deploy/ hardcodes it)
#   E2E_OUT       output dir: operator logs, timeline (default ./e2e-out)
#   E2E_PHASES    driver phases (default: all; see scripts/operator_e2e.py)
# Flags:
#   --start          start (create) the minikube profile when it is not running
#   --no-build       reuse the images already in minikube
#   --keep           leave the operator, CRD and namespace installed afterwards
#   --delete-profile delete the minikube profile at the end (even on failure)
#   -h, --help
set -euo pipefail

usage() { sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }
START=0; BUILD=1; KEEP=0; DELETE_PROFILE=0
for a in "$@"; do
  case "$a" in
    --start) START=1 ;;
    --no-build) BUILD=0 ;;
    --keep) KEEP=1 ;;
    --delete-profile) DELETE_PROFILE=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "operator-e2e: unknown argument $a" >&2; exit 2 ;;
  esac
done

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${E2E_PROFILE:-pharos-e2e}"
NS="${E2E_NS:-pharos}"
VERSION="1.4.1-e2e"
EXTRA_TAGS=(1.4.2-e2e 1.4.3-e2e 1.4.4-e2e)   # same image: pre-flight refused / failed, upgrade
OPERATOR_IMAGE="pharos-operator:e2e"
export E2E_OUT="${E2E_OUT:-$PWD/e2e-out}"
export E2E_PROFILE="$PROFILE" E2E_NS="$NS"

for tool in minikube kubectl docker python3; do
  command -v "$tool" >/dev/null || { echo "operator-e2e: $tool is not on PATH" >&2; exit 2; }
done

cleanup_profile() {
  if [ "$DELETE_PROFILE" = 1 ]; then
    echo "== deleting minikube profile $PROFILE"
    minikube delete -p "$PROFILE" >/dev/null 2>&1 || true
  fi
}
trap cleanup_profile EXIT

if ! minikube -p "$PROFILE" status >/dev/null 2>&1; then
  if [ "$START" = 1 ]; then
    echo "== starting minikube profile $PROFILE (docker driver, calico)"
    minikube start -p "$PROFILE" --driver=docker --cpus=4 --memory=7g --cni=calico
  else
    echo "operator-e2e: minikube profile $PROFILE is not running (use --start)" >&2; exit 2
  fi
fi
kubectl config use-context "$PROFILE" >/dev/null
if ! kubectl get pods -n kube-system -l k8s-app=calico-node -o name | grep -q .; then
  echo "operator-e2e: profile $PROFILE has no calico: NetworkPolicy would not be enforced" \
       "(recreate it with --cni=calico)" >&2
  exit 2
fi

if [ "$BUILD" = 1 ]; then
  echo "== building images in minikube's docker"
  (
    eval "$(minikube -p "$PROFILE" docker-env)"
    cd "$REPO"
    # the legacy builder: minikube's docker client may have no buildx; the Dockerfiles need no BuildKit
    DOCKER_BUILDKIT=0 docker build -q -f docker/pharos/Dockerfile -t "pharos:$VERSION" .
    for t in "${EXTRA_TAGS[@]}"; do docker tag "pharos:$VERSION" "pharos:$t"; done
    DOCKER_BUILDKIT=0 docker build -q -f docker/operator/Dockerfile -t "$OPERATOR_IMAGE" .
  )
fi

echo "== installing the CRD, RBAC and operator"
kubectl apply -f "$REPO/deploy/crd/" -f "$REPO/deploy/rbac.yaml" >/dev/null
sed -e "s#ghcr.io/OWNER/pharos-operator:VERSION#$OPERATOR_IMAGE#" \
    -e "s#value: ghcr.io/OWNER/pharos\$#value: pharos#" \
    -e "s#imagePullPolicy: IfNotPresent#imagePullPolicy: Never#" \
    "$REPO/deploy/operator.yaml" | kubectl apply -f - >/dev/null
# a rebuilt image under the same tag: restart the pod so it runs the new one
[ "$BUILD" = 1 ] && kubectl -n "$NS" rollout restart deploy/pharos-operator >/dev/null
kubectl -n "$NS" rollout status deploy/pharos-operator --timeout=180s

rc=0
python3 "$REPO/scripts/operator_e2e.py" || rc=$?

if [ "$KEEP" = 0 ]; then
  echo "== uninstalling the operator, RBAC, CRD and namespace"
  kubectl delete -f "$REPO/deploy/operator.yaml" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl delete -f "$REPO/deploy/rbac.yaml" --ignore-not-found --wait=true --timeout=180s >/dev/null 2>&1 || true
  kubectl delete -f "$REPO/deploy/crd/" --ignore-not-found >/dev/null 2>&1 || true
fi
exit $rc
