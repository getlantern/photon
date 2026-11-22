//go:build darwin

package transport

import "golang.org/x/sys/unix"

const udpECNTOS = unix.IP_RECVTOS
