#!/bin/sh
set -eu
# External test-only peer; no source is copied into the implementation.
revision=91567706c41c0d97ab8dc576873ecd472d7869fa
peer_dir=${1:?usage: build.sh EMPTY_EXTERNAL_DIRECTORY}
if [ -e "$peer_dir" ]; then
    echo "destination must not exist: $peer_dir" >&2
    exit 1
fi
git clone https://github.com/litespeedtech/ls-qpack.git "$peer_dir"
git -C "$peer_dir" checkout --detach "$revision"
cmake -S "$peer_dir" -B "$peer_dir/build" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DLSQPACK_BIN=ON -DLSQPACK_TESTS=OFF -DCMAKE_BUILD_TYPE=Release
cmake --build "$peer_dir/build" --parallel 4
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
"${CC:-cc}" -std=c99 -O2 -I "$peer_dir" "$script_dir/encoder-peer.c" "$peer_dir/build/libls-qpack.a" -lm -o "$peer_dir/build/bin/encoder-peer"
