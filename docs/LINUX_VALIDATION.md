# Linux runtime validation

Run `scripts/test-linux.sh` from a host with Go and a running Docker engine.
The script runs race tests and vet for the root and independent-peer modules.
It mounts source and the host Go module cache read-only and removes the container
on exit. Dependencies must already be present in the host module cache. Build
output now uses bounded tmpfs mounts under /tmp and /root/.cache, and the
container root filesystem is read-only. The script checks for at least 4 GiB
free at the standard macOS Docker Desktop storage location before starting.
Custom or remote engine storage requires a separate capacity check.

The container is limited to two CPUs and 4 GiB, with GOMAXPROCS=2. Its pinned
image is Go 1.26.2:

`golang:1.26.2@sha256:b54cbf583d390341599d7bcbc062425c081105cc5ef6d170ced98ef9d047c716`

The first run used Docker Desktop on macOS arm64, executing native Linux arm64
binaries under Linux 6.12.76-linuxkit. This exercises Linux sockets and ancillary
ECN data, unlike cross-compilation. It does not establish behavior on Linux
amd64, physical NICs, other kernels, or production network conditions.

The default independent-peer suite excludes documented strict known failures;
see `interop/KNOWN_FAILURES.md`. A default pass cannot establish complete v2
key-update interoperability or completion of the overall implementation.

## First-run evidence, 2026-09-20

The root module's complete race suite passed, including real Linux transport
socket tests (transport package: 26.697 seconds). The rest of the run failed at
the infrastructure level: Docker reported disk I/O errors and its backend log
reported no space left on the host volume. The attached command ultimately
exited 125 with an unexpected EOF. No Linux independent-peer or vet pass was
observed.

The Docker instance started for this run was stopped after those errors were
confirmed. Two unopened updater cache files created during this startup were
moved to `/Volumes/ExtremeSSD/macdev/quic-docker-update-recovery-qhct7vaq`,
recovering approximately 450 MiB. Existing Docker images, volumes, and repository
files were not deleted. The internal volume still had only 615 MiB available.

The revised read-only/tmpfs script passed shell syntax validation, and its
low-disk guard correctly refused a new run before contacting Docker. The revised
container configuration has not yet completed runtime validation. Do not restart
validation until Docker storage has adequate space and its filesystem is usable.
