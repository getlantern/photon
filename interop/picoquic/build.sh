#!/bin/sh
# Build an external, pinned interoperability peer. No production Go dependency.
set -eu
if [ "$#" -ne 1 ]; then
    echo "usage: $0 EXTERNAL_BUILD_DIRECTORY" >&2
    exit 2
fi
peer_root=$1
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
peer_revision=8c6bbf82b6b6d98a413a69e6bf2265e7f6a10161
tls_revision=bfa67875982afc4c24f21e146cef4747fa189c2f
mkdir -p "$peer_root"
if [ ! -d "$peer_root/picoquic/.git" ]; then
    git clone --no-checkout https://github.com/private-octopus/picoquic.git "$peer_root/picoquic"
    git -C "$peer_root/picoquic" checkout --detach "$peer_revision"
fi
if [ "$(git -C "$peer_root/picoquic" rev-parse HEAD)" != "$peer_revision" ]; then
    echo "picoquic checkout does not match the pinned revision" >&2
    exit 1
fi
cmake -S "$script_dir" -B "$peer_root/interop-build" \
    -DPICOQUIC_SOURCE="$(CDPATH= cd -- "$peer_root/picoquic" && pwd)" \
    -DPICOQUIC_FETCH_PTLS=ON -DPICOQUIC_FETCH_PTLS_TAG="$tls_revision" \
    -Dpicoquic_BUILD_TESTS=OFF -DBUILD_PICO_SIM=OFF \
    -DBUILD_PQBENCH=OFF -DBUILD_LOGREADER=OFF -DCMAKE_BUILD_TYPE=Release
cmake --build "$peer_root/interop-build" --target picoquicdemo picoquic-client --parallel 4
printf 'PICOQUIC_DEMO=%s/interop-build/picoquic/picoquicdemo\n' "$peer_root"
printf 'PICOQUIC_CLIENT=%s/interop-build/picoquic-client\n' "$peer_root"
