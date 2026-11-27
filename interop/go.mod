module github.com/getlantern/photon/interop

go 1.26.0

require (
	github.com/apernet/quic-go v0.65.1-0.20261109005002-2895f039137a
	github.com/getlantern/photon v0.0.0
	github.com/quic-go/qpack v0.6.0
	github.com/quic-go/quic-go v0.65.0
	golang.org/x/exp v0.0.0-20261124000051-d6646c509b6d
	golang.org/x/net v0.58.0
	golang.org/x/sys v0.48.0
)

require (
	github.com/andybalholm/brotli v1.0.6 // indirect
	github.com/klauspost/compress v1.17.4 // indirect
	github.com/refraction-networking/utls v1.8.2 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)

replace github.com/getlantern/photon => ..
