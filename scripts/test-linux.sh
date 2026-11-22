#!/usr/bin/env bash
# Native Linux socket and race validation in Docker's architecture.
# Source and the host module cache are mounted read-only; build output is ephemeral.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
module_cache="$(go env GOMODCACHE)"
# Check the common Docker Desktop disk location before asking Docker to pull or
# create anything. Custom/remote engine storage needs its own capacity check.
if [[ "$(uname -s)" == Darwin ]]; then
  docker_storage="$HOME/Library/Containers/com.docker.docker/Data"
  if [[ -d "$docker_storage" ]]; then
    if [[ -e "$docker_storage/vms/0/data/Docker.raw" ]]; then
      docker_storage="$docker_storage/vms/0/data/Docker.raw"
    fi
    available_kib="$(df -Pk "$docker_storage" | awk 'NR == 2 {print $4}')"
    if [[ ! "$available_kib" =~ ^[0-9]+$ ]] || (( available_kib < 4194304 )); then
      echo "Linux validation requires at least 4 GiB free on Docker's host storage volume." >&2
      exit 1
    fi
  fi
fi
docker run --rm --read-only --cpus=2 --memory=4g \
  --tmpfs /tmp:rw,exec,size=2g --tmpfs /root/.cache:rw,size=1g \
  --mount "type=bind,src=$repo_root,dst=/src,readonly" \
  --mount "type=bind,src=$module_cache,dst=/go/pkg/mod,readonly" \
  --env GOMAXPROCS=2 --workdir /src golang:1.26.2@sha256:b54cbf583d390341599d7bcbc062425c081105cc5ef6d170ced98ef9d047c716 \
  sh -ec 'go version
uname -srmo
go test -race ./...
go vet ./...
cd interop
go test -race ./...
go vet ./...'
