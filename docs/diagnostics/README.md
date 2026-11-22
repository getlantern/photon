# Handshake stall diagnostics, 2026-09-28

`shared-endpoint-retry-loop.txt` captures one failing shared endpoint test,
including the unmodified v0.63.0 reference client's debug logs. One client
completed; the client bound to `[::]:60381` kept sending tokenless Initials,
while the engine replied with Retry to `127.0.0.1:60381`. No Retry acceptance
appeared for that client.

A read-only socket inspection immediately after the failing run found:

```
cloudflar 821 afisk 12u IPv4 UDP *:60381
cloudflar 821 afisk 13u IPv4 UDP *:53850
```

Port 53850 also appears in the earlier v2 loss-fixture failure trace. These
observations support an address-family port collision for those stalls; they
do not explain every historical handshake timeout.

The standalone probe in `darwin-udp-bind-overlap.go.txt` reproduced this OS
behavior using only newly created test sockets. Copy it to a temporary `.go`
file and run it with Go. On this Darwin arm64 host it printed:

```
IPv4 listener: 0.0.0.0:61870 second wildcard listener: [::]:61870
IPv4 received "probe" err=<nil>
second wildcard received 0 bytes err=read udp [::]:61870: i/o timeout
second explicit IPv4 bind: listen udp4 0.0.0.0:61870: bind: address already in use
```

The probe neither uses nor modifies the unrelated process's sockets. The
reference `DialAddr` helper creates its socket with `net.ListenUDP("udp",
&net.UDPAddr{IP: net.IPv4zero})`; on this host that is an IPv6 wildcard socket.
The affected IPv4 loss/shared-endpoint fixtures now use an explicit `udp4`
socket with upstream `Dial`, preserving its single-use transport and zero-CID
behavior. Socket cleanup is explicit. Original deadlines and packet-loss
assertions remain. The independent production dialer already chooses `udp4`
or `udp6` from its destination, so no production socket change was necessary.

A subsequent full run timed out in the initial-packet-size fixture, which
still used upstream DialAddr (75.439s); its cause was not separately traced.
All direct reference DialAddr calls now use a shared fixture helper that chooses
udp4/udp6 from the resolved destination, preserves ServerName inference and
uses the unchanged upstream Dial protocol implementation. This also changes the
stream-echo benchmark's client socket setup; historical measurements predate it.
A passing suite cannot classify every historical timeout or rule out unrelated
future failures.

After the explicit IPv4 change, 50 repetitions of the complete shared-endpoint
and single-datagram-loss matrices passed under the race detector (30.866s).
The earlier debug run reproduced the shared-endpoint collision (36.202s);
its relevant test segment is retained here. This is fixture/environment
validation, not proof that all historical timeouts share this cause.

With all direct DialAddr fixture calls moved to the shared destination-family
helper, the full default interop race suite passed (71.261s); vet also passed.
