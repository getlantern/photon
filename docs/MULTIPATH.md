# Multipath evidence and remaining work

The extension was published as [RFC 10069](https://www.rfc-editor.org/rfc/rfc10069.html)
in November 2026. Production code was written from draft-21; its assigned values were
compared with the RFC on 2026-11-22 and match (see [standards freshness](REMAINING.md#standards-freshness)).
The RFC's normative text has not been diffed against draft-21, and the dated
sections below describe checks made against that draft.
This is an evidence inventory, not a completed conformance audit.

| Draft area | Implementation and focused evidence | Limits / remaining gates |
| --- | --- | --- |
| 2.1–2.4: negotiation, parameters, nonce, ACK spaces | `internal/transport/multipath.go`, `path_recovery.go`, `internal/packet`; `TestPathPacketNumberIsolation`, multipath parameter and protection tests | 0-RTT composition and exhaustive parameter combinations require further audit |
| 2.5–2.6: key updates and closure | `path_keyphase_test.go`, `path_pto_test.go`, `path_close_receive_test.go`; shared keys, per-path numbers, largest retained PTO for key/closing retention | Independent adverse-reordering/key-update matrix remains incomplete |
| 3.1: initiation and validation | `path_initiate.go`, `path_validation_multi_test.go`; role restrictions, response affinity, minimum 1200-byte validation, bounded retry | Address-family changes and wildcard packet-info support remain open |
| 3.2–3.4 and 4: CID, status, abandonment, frames | `path_cids.go`, `path_ids_test.go`, `path_status_send_test.go`, `path_close_test.go`, wire codecs | Public configuration is intentionally bounded; no exhaustive section-by-section conformance claim |
| 5.3–5.7: congestion, RTT, scheduling, recovery | `path_recovery.go`, `path_scheduler.go`, `path_pto_test.go`; per-path recovery and congestion control (BBR by default, or NewReno) and available/backup scheduling | ECN integration and comparative multipath performance remain open; scheduling is an implementation policy |
| 5.8: per-path MTU | Initial minimum-size validation exists | Linux/macOS nonfragmentation socket setup exists; [full DPLPMTUD](MTU.md) and unequal-PMTU retransmission gates are incomplete |
| 5.9: connection idle behavior | `idle.go`, `path_packet.go`, `path_idle_test.go`, `TestMultipathIdlePTOFloor`; authenticated activity is connection-wide, PTO floor uses retained application paths | Per-path keepalive policy is not supplied by this draft; broader middlebox tests remain open |

The idle PTO policy conservatively uses the largest retained path PTO without
exponential backoff. This prevents a stale primary-path RTT from shortening the
connection timeout while other paths have longer feedback times. The negotiated
idle timeout still takes precedence when larger, and zero on both endpoints
continues to disable idle expiry.

[Picoquic fixtures](../interop/picoquic/README.md) document pinned independent
v1/v2 negotiation, path opening, abandonment, stream transfer and selected loss
cases in both roles, plus migration in our-client role. Those are historical
results; the default external binary paths were absent during this review, so
no fresh Picoquic run is claimed. Reverse-role migration, arbitrary loss/reordering,
and performance superiority are unverified.

The older `PLAN.md` milestone prose is historical. Use this inventory, the
endpoint guides and `PROGRESS.md` for newer implementation evidence, and inspect
the code/tests before relying on any status statement.


## Connection keepalive after path-zero abandonment

`TestPublicKeepAlive` now includes real-UDP v1/v2 cases with negotiated multipath,
a validated path on a distinct client socket, and public abandonment of path zero.
Only one endpoint enables keepalive (client and server roles are both covered).
With MTU discovery disabled, each pair survives 3.5 seconds without application
traffic despite a one-second advertised idle timeout, then transfers exact bytes
and FIN in both directions. All four multipath cases and the four base-path
controls pass under race instrumentation; root vet passes.

These cases use two copies of this implementation. They do not independently
validate draft interoperability or prove maintenance of every idle path's NAT
binding. Independent-peer multipath keepalive and injected keepalive loss remain
outstanding.

Keepalive admission also has a v1/v2 regression for queued frames on a path held
by local migration or not yet validated. Such queues no longer suppress PING
admission on another eligible empty path. The fixture preserves the blocked
queue and authenticates output on the selected path; it is a synthetic selection
test. Full root race tests and the independent basic/migration keepalive gates
pass after the change.

## Draft-21 control-frame audit: 2026-09-30

The [current IETF record](https://datatracker.ietf.org/doc/draft-ietf-quic-multipath/)
still identifies revision 21, and the [RFC Editor queue](https://queue.rfc-editor.org/)
shows first-editor work. No newer revision or published RFC was found in this
check. The code points in `internal/wire/multipath_frames.go` match draft-21
section 4; final publication still requires revalidation.

| Clause | Reviewed implementation | Additional evidence |
| --- | --- | --- |
| §4: reject out-of-limit path IDs | `path_cids.go`, `path_control.go`, `path_lifecycle.go`, `path_recovery.go` | `TestMultipathControlLimitsBothVersions`: protected v1/v2 packets covering ACK/ACK_ECN, abandon, both status types, new/retire CID, blocked paths/CIDs |
| §4.6: invalid MAX_PATH_ID; non-increasing frames ignored | `updatePeerMaximum` compares the initial peer maximum and the 32-bit bound | Same new test checks below-initial and 2^32 values; existing CID/control ordering tests cover reordering and the maximum valid value |
| §§3.4.3/4: ignore frames for disposed paths | Retained tombstones checked before per-path allocation/ACK dispatch | `TestDisposedPathControlIgnored`: both versions, ACK/ACK_ECN (including unsent numbers), new/retire CID, both status types, blocked CID and repeated abandonment; no resurrected path/CID history/output charge |
| §§3.4/3.4.3: retain state before disposal | `receiveAbandon`, `collectAbandoned`, `receiveACK` | Existing retention tests; the new test explicitly disposes after both abandonment directions and three supplied PTOs |

Abandoned-but-retained paths can still process late ACKs; silently ignoring ACKs
is required once that path state is disposed. This distinction avoids incorrectly
changing working retained-state recovery during the audit. New protected-frame
gates pass under race (1.445s), and transport vet passes. No production fix was
required. These are synthetic own-engine packet tests, not independent multipath
interop, and they do not complete the draft's normative audit.
